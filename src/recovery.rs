use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use alloy::primitives::U256;

use dashmap::DashMap;
use polymarket_client_sdk_v2::clob::types::request::{OrdersRequest, TradesRequest};
use polymarket_client_sdk_v2::clob::types::response::OpenOrderResponse;
use polymarket_client_sdk_v2::error::{Kind as PmErrorKind, Status as PmStatus, StatusCode};
use polymarket_client_sdk_v2::types::Decimal;

use crate::clob_client::{AuthenticatedClobClient, build_authenticated_clob_client};
use crate::config::AuthConfig;
use crate::storage::{OrderStore, StoredOrder};
use crate::strategy::OrderCorrelationMap;

const END_CURSOR: &str = "LTE=";

// 启动恢复负责把 orders.db 的 active 订单与远端 CLOB 状态重新对齐。
pub struct RecoveryCoordinator {
    order_store: OrderStore,
    auth: AuthConfig,
    simulation_enabled: bool,
}

// 恢复产物会交给订单执行器，作为重启后继续管理存量订单的入口。
pub struct RecoveryArtifacts {
    pub order_correlations: OrderCorrelationMap,
}

impl RecoveryCoordinator {
    pub fn new(order_store: OrderStore, auth: AuthConfig, simulation_enabled: bool) -> Self {
        Self {
            order_store,
            auth,
            simulation_enabled,
        }
    }

    pub async fn recover(&self) -> anyhow::Result<RecoveryArtifacts> {
        // 先读取本地 active 订单，再用远端状态筛掉不可恢复订单，最后重建订单关联索引。
        let local_projection = self.load_local_projection()?;
        let reconciled_active_orders = self
            .reconcile_orders_against_exchange(local_projection.active_orders)
            .await?;
        let order_correlations = self.build_order_correlations(&reconciled_active_orders)?;

        Ok(RecoveryArtifacts { order_correlations })
    }

    fn load_local_projection(&self) -> anyhow::Result<LocalProjection> {
        Ok(LocalProjection {
            active_orders: self.order_store.load_active_orders()?,
        })
    }

    async fn reconcile_orders_against_exchange(
        &self,
        stored_orders: Vec<StoredOrder>,
    ) -> anyhow::Result<Vec<StoredOrder>> {
        // 模拟模式没有远端订单，直接信任本地 active 记录。
        if self.simulation_enabled {
            return Ok(stored_orders);
        }

        let client = build_authenticated_clob_client(&self.auth)
            .await
            .map_err(|e| anyhow::anyhow!("启动恢复：构建 Polymarket CLOB 客户端失败: {e:#}"))?;
        // 先批量拉 open orders，只有本地 active 但不在 open 列表里的订单才逐个查询。
        let remote_open_orders = fetch_remote_open_orders(&client)
            .await
            .map_err(|e| anyhow::anyhow!("启动恢复：拉取 Polymarket open orders 失败: {e:#}"))?;
        let mut reconciled = Vec::new();

        for stored_order in stored_orders {
            if let Some(updated) = self
                .reconcile_stored_order(&client, &remote_open_orders, stored_order)
                .await?
            {
                reconciled.push(updated);
            }
        }

        Ok(reconciled)
    }

    async fn reconcile_stored_order(
        &self,
        client: &AuthenticatedClobClient,
        remote_open_orders: &HashMap<String, OpenOrderResponse>,
        stored_order: StoredOrder,
    ) -> anyhow::Result<Option<StoredOrder>> {
        let Some(remote_order_id) = stored_order.remote_order_id.as_deref() else {
            // 没有远端 id 的订单不能继续被 order_ws 或撤单逻辑可靠关联。
            self.mark_missing_remote_order_id_unknown(&stored_order)?;
            return Ok(None);
        };

        if let Some(open_order) = remote_open_orders.get(remote_order_id) {
            return self
                .reconcile_present_open_order(&stored_order, remote_order_id, open_order)
                .map(Some);
        }

        match client.order(remote_order_id).await {
            Ok(order) => {
                self.reconcile_resolved_order_lookup(&stored_order, remote_order_id, &order)
            }
            Err(error) if is_not_found_status(&error) => {
                self.reconcile_missing_remote_order(client, &stored_order, remote_order_id)
                    .await?;
                Ok(None)
            }
            Err(error) => Err(anyhow::anyhow!(
                "启动恢复：查询单个远端订单 {remote_order_id} 失败: {error:#}"
            )),
        }
    }

    fn mark_missing_remote_order_id_unknown(
        &self,
        stored_order: &StoredOrder,
    ) -> anyhow::Result<()> {
        self.order_store
            .update_order_status_by_local(&stored_order.local_order_id, "unknown")?;
        self.order_store.append_order_event(
            Some(&stored_order.local_order_id),
            None,
            "startup_reconciled",
            serde_json::json!({
                "result": "missing_remote_order_id",
                "status": "unknown",
            }),
        )?;
        Ok(())
    }

    fn reconcile_present_open_order(
        &self,
        stored_order: &StoredOrder,
        remote_order_id: &str,
        open_order: &OpenOrderResponse,
    ) -> anyhow::Result<StoredOrder> {
        let status = map_open_order_status(open_order);
        self.order_store
            .update_order_status_by_remote(remote_order_id, status)?;
        self.order_store.append_order_event(
            Some(&stored_order.local_order_id),
            Some(remote_order_id),
            "startup_reconciled",
            serde_json::json!({
                "result": "present_in_open_orders",
                "status": status,
                "original_size": open_order.original_size.to_string(),
                "size_matched": open_order.size_matched.to_string(),
                "price": open_order.price.to_string(),
            }),
        )?;

        let mut updated = stored_order.clone();
        updated.status = status.to_string();
        Ok(updated)
    }

    fn reconcile_resolved_order_lookup(
        &self,
        stored_order: &StoredOrder,
        remote_order_id: &str,
        order: &OpenOrderResponse,
    ) -> anyhow::Result<Option<StoredOrder>> {
        let remote_status = map_single_order_status(order);
        let status = reconciled_status(&stored_order.status, remote_status);
        self.order_store
            .update_order_status_by_remote(remote_order_id, status)?;
        self.order_store.append_order_event(
            Some(&stored_order.local_order_id),
            Some(remote_order_id),
            "startup_reconciled",
            serde_json::json!({
                "result": "resolved_by_order_lookup",
                "status": status,
                "remote_status": remote_status,
                "original_size": order.original_size.to_string(),
                "size_matched": order.size_matched.to_string(),
                "price": order.price.to_string(),
            }),
        )?;

        if is_terminal_or_unrecoverable_status(status) {
            return Ok(None);
        }

        let mut updated = stored_order.clone();
        updated.status = status.to_string();
        Ok(Some(updated))
    }

    async fn reconcile_missing_remote_order(
        &self,
        client: &AuthenticatedClobClient,
        stored_order: &StoredOrder,
        remote_order_id: &str,
    ) -> anyhow::Result<()> {
        let inferred_status = infer_missing_remote_terminal_status(client, stored_order).await?;
        let status = reconciled_status(&stored_order.status, inferred_status);
        self.order_store
            .update_order_status_by_local(&stored_order.local_order_id, status)?;
        self.order_store.append_order_event(
            Some(&stored_order.local_order_id),
            Some(remote_order_id),
            "startup_reconciled",
            serde_json::json!({
                "result": "missing_from_remote",
                "status": status,
                "inferred_status": inferred_status,
            }),
        )?;
        Ok(())
    }

    fn build_order_correlations(
        &self,
        reconciled_active_orders: &[StoredOrder],
    ) -> anyhow::Result<OrderCorrelationMap> {
        // 同一份 meta 同时按 local id 和 remote id 索引，方便策略与私有 WS 双向回查。
        let order_correlations: OrderCorrelationMap = Arc::new(DashMap::new());

        for stored_order in reconciled_active_orders {
            let local_meta = stored_order.to_local_order_meta();
            order_correlations.insert(local_meta.local_order_id.clone(), local_meta.clone());
            if let Some(remote_order_id) = &local_meta.remote_order_id {
                order_correlations.insert(remote_order_id.clone(), local_meta.clone());
            }
            self.order_store.append_order_event(
                Some(&local_meta.local_order_id),
                local_meta.remote_order_id.as_deref(),
                "recovered_on_startup",
                serde_json::json!({
                    "strategy": local_meta.strategy.as_ref(),
                    "topic": local_meta.topic.as_ref().map(|topic| topic.as_ref()),
                    "token": local_meta.token,
                    "side": format!("{:?}", local_meta.side),
                    "price": local_meta.price.to_string(),
                    "order_size": local_meta.order_size.to_string(),
                    "status": stored_order.status,
                    "last_mid": stored_order.last_mid.map(|value| value.to_string()),
                }),
            )?;
        }

        Ok(order_correlations)
    }
}

struct LocalProjection {
    active_orders: Vec<StoredOrder>,
}

async fn fetch_remote_open_orders(
    client: &AuthenticatedClobClient,
) -> anyhow::Result<HashMap<String, OpenOrderResponse>> {
    // Polymarket 分页以固定 END_CURSOR 表示结束，不能只看 cursor 是否为空。
    let mut remote_open_orders = HashMap::new();
    let mut cursor: Option<String> = None;

    loop {
        let page = client
            .orders(&OrdersRequest::default(), cursor.clone())
            .await
            .map_err(|e| anyhow::anyhow!("拉取 open orders 页失败，cursor={cursor:?}: {e:#}"))?;
        for order in page.data {
            remote_open_orders.insert(order.id.clone(), order);
        }
        if page.next_cursor == END_CURSOR {
            break;
        }
        cursor = Some(page.next_cursor);
    }

    Ok(remote_open_orders)
}

fn map_open_order_status(order: &OpenOrderResponse) -> &'static str {
    if order.size_matched == Decimal::ZERO {
        "open"
    } else {
        "partially_filled"
    }
}

fn map_single_order_status(order: &OpenOrderResponse) -> &'static str {
    use polymarket_client_sdk_v2::clob::types::OrderStatusType;

    match order.status {
        OrderStatusType::Canceled => "canceled",
        OrderStatusType::Matched => {
            if order.size_matched == order.original_size {
                "filled"
            } else {
                "partially_filled"
            }
        }
        OrderStatusType::Live | OrderStatusType::Delayed | OrderStatusType::Unmatched => {
            map_open_order_status(order)
        }
        OrderStatusType::Unknown(_) => "unknown",
        _ => "unknown",
    }
}

fn is_terminal_or_unrecoverable_status(status: &str) -> bool {
    matches!(
        status,
        "filled" | "canceled" | "rejected" | "failed" | "unknown"
    )
}

fn reconciled_status<'a>(current_status: &'a str, remote_status: &'a str) -> &'a str {
    if is_terminal_or_unrecoverable_status(current_status) && remote_status == "unknown" {
        current_status
    } else {
        remote_status
    }
}

async fn infer_missing_remote_terminal_status(
    client: &AuthenticatedClobClient,
    stored_order: &StoredOrder,
) -> anyhow::Result<&'static str> {
    // 订单查不到时再查 token 成交，用 maker order id 判断它是否已成交。
    let request = TradesRequest::builder()
        .asset_id(U256::from_str(&stored_order.token)?)
        .build();
    let page = match client.trades(&request, None).await {
        Ok(page) => page,
        Err(error) if is_empty_decimal_decode_error(&error) => {
            return Ok("unknown");
        }
        Err(error) => {
            return Err(anyhow::anyhow!(
                "查询 token {} 的成交记录失败: {error:#}",
                stored_order.token
            ));
        }
    };
    let has_trade_for_order = page.data.iter().any(|trade| {
        trade.maker_orders.iter().any(|maker_order| {
            maker_order.order_id == stored_order.remote_order_id.clone().unwrap_or_default()
        })
    });
    Ok(if has_trade_for_order {
        "filled"
    } else {
        "unknown"
    })
}

fn is_not_found_status(error: &polymarket_client_sdk_v2::error::Error) -> bool {
    error.kind() == PmErrorKind::Status
        && error
            .downcast_ref::<PmStatus>()
            .is_some_and(|status| status.status_code == StatusCode::NOT_FOUND)
}

fn is_empty_decimal_decode_error(error: &polymarket_client_sdk_v2::error::Error) -> bool {
    let message = error.to_string();
    message.contains("invalid value: string \"\"") && message.contains("Decimal")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconciled_status_preserves_terminal_status_when_remote_is_unknown() {
        assert_eq!(reconciled_status("filled", "unknown"), "filled");
        assert_eq!(reconciled_status("canceled", "unknown"), "canceled");
        assert_eq!(reconciled_status("rejected", "unknown"), "rejected");
        assert_eq!(reconciled_status("failed", "unknown"), "failed");
    }
}
