use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::auth::{LocalSigner, Signer as _};
use polymarket_client_sdk_v2::clob::ws::Client;
use polymarket_client_sdk_v2::types::{Address, Decimal};
use serde_json::json;
use tracing::{info, warn};

use crate::{
    config::AuthConfig,
    notification::{LiquidityRewardFillNotification, NotificationEvent, Notifier},
    positions::PositionRefreshTrigger,
    storage::OrderStore,
    strategy::{OrderCorrelationMap, OrderFillEvent, OrderStatusEvent, StrategyEvent},
};

pub async fn run(
    auth: AuthConfig,
    correlations: OrderCorrelationMap,
    order_store: OrderStore,
    positions_refresh_tx: tokio::sync::mpsc::Sender<PositionRefreshTrigger>,
    strategy_tx: tokio::sync::mpsc::Sender<StrategyEvent>,
    notifier: Option<Notifier>,
) {
    loop {
        match subscribe_orders(
            &auth,
            &correlations,
            &order_store,
            &positions_refresh_tx,
            &strategy_tx,
            notifier.as_ref(),
        )
        .await
        {
            Ok(()) => warn!(target: "order", "订单 websocket 已断开，5 秒后重连"),
            Err(error) => {
                warn!(target: "order", error = %error, "订单 websocket 监听失败，5 秒后重连")
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn subscribe_orders(
    auth: &AuthConfig,
    correlations: &OrderCorrelationMap,
    order_store: &OrderStore,
    positions_refresh_tx: &tokio::sync::mpsc::Sender<PositionRefreshTrigger>,
    strategy_tx: &tokio::sync::mpsc::Sender<StrategyEvent>,
    notifier: Option<&Notifier>,
) -> anyhow::Result<()> {
    let signer = LocalSigner::from_str(&auth.private_key)?.with_chain_id(Some(POLYGON));
    let address = Address::from_str(&auth.funder)?;
    let rest_client = polymarket_client_sdk_v2::clob::Client::new(
        "https://clob.polymarket.com",
        polymarket_client_sdk_v2::clob::Config::builder()
            .use_server_time(true)
            .build(),
    )?;
    let credentials = rest_client.create_or_derive_api_key(&signer, None).await?;

    let client = Client::default().authenticate(credentials, address)?;
    let mut stream = Box::pin(client.subscribe_orders(Vec::new())?);

    info!(target: "order", funder = %auth.funder, "已连接订单 websocket 并开始监听订单变化");

    while let Some(message) = stream.next().await {
        match message {
            Ok(order) => {
                let local_meta = correlations.get(&order.id).map(|entry| entry.clone());
                let local_meta = match local_meta {
                    Some(meta) => Some(meta),
                    None => match order_store.find_order_by_remote(&order.id) {
                        Ok(Some(stored_order)) => {
                            let meta = stored_order.to_local_order_meta();
                            correlations.insert(meta.local_order_id.clone(), meta.clone());
                            if let Some(remote_order_id) = meta.remote_order_id.clone() {
                                correlations.insert(remote_order_id, meta.clone());
                            }
                            info!(
                                target: "order",
                                order_id = %order.id,
                                local_order_id = %meta.local_order_id,
                                strategy = %meta.strategy,
                                token = %meta.token,
                                "订单 websocket 从数据库恢复本地订单关联"
                            );
                            Some(meta)
                        }
                        Ok(None) => None,
                        Err(error) => {
                            warn!(target: "order", order_id = %order.id, error = %error, "订单 websocket 从数据库恢复本地订单关联失败");
                            None
                        }
                    },
                };
                let local_order_id = local_meta.as_ref().map(|meta| meta.local_order_id.clone());
                let status = classify_ws_status(
                    &format!("{:?}", order.msg_type),
                    order
                        .original_size
                        .as_ref()
                        .map(|value| value.to_string())
                        .as_deref(),
                    order
                        .size_matched
                        .as_ref()
                        .map(|value| value.to_string())
                        .as_deref(),
                );
                let previous_size_matched = order_store
                    .last_ws_size_matched_by_remote(&order.id)
                    .ok()
                    .flatten();
                let current_size_matched = order.size_matched;

                let _ = order_store.append_order_event(
                    local_order_id.as_deref(),
                    Some(&order.id),
                    "ws_update",
                    json!({
                        "strategy": local_meta.as_ref().map(|meta| meta.strategy.as_ref()),
                        "topic": local_meta.as_ref().and_then(|meta| meta.topic.as_ref().map(|topic| topic.as_ref())),
                        "market": order.market,
                        "asset_id": order.asset_id,
                        "side": format!("{:?}", order.side),
                        "price": order.price.to_string(),
                        "msg_type": format!("{:?}", order.msg_type),
                        "original_size": order.original_size.map(|value| value.to_string()),
                        "size_matched": order.size_matched.map(|value| value.to_string()),
                        "timestamp": order.timestamp,
                        "status": status,
                    }),
                );
                let _ = order_store.update_order_status_by_remote(&order.id, status);

                if let Some(local_meta) = local_meta {
                    info!(
                        target: "order",
                        order_id = %order.id,
                        local_order_id = %local_meta.local_order_id,
                        remote_order_id = ?local_meta.remote_order_id,
                        strategy = %local_meta.strategy,
                        topic = ?local_meta.topic,
                        token = %local_meta.token,
                        local_side = ?local_meta.side,
                        local_price = %local_meta.price,
                        local_order_size = %local_meta.order_size,
                        market = %order.market,
                        asset_id = %order.asset_id,
                        side = ?order.side,
                        price = %order.price,
                        msg_type = ?order.msg_type,
                        original_size = ?order.original_size,
                        size_matched = ?order.size_matched,
                        timestamp = ?order.timestamp,
                        status = status,
                        "收到订单 websocket 更新，并成功关联本地订单"
                    );
                    if let Some(delta_size) =
                        fill_delta(previous_size_matched, current_size_matched)
                    {
                        let total_matched_size = current_size_matched.unwrap_or(Decimal::ZERO);
                        let _ = strategy_tx.try_send(StrategyEvent::OrderFill(OrderFillEvent {
                            token: local_meta.token.clone(),
                            local_order_id: local_meta.local_order_id.clone(),
                            side: local_meta.side,
                            delta_size,
                            total_matched_size,
                        }));
                        if local_meta.strategy.as_ref() == "liquidity_reward" {
                            if let Some(notifier) = notifier {
                                notifier.try_notify(NotificationEvent::LiquidityRewardFill(
                                    LiquidityRewardFillNotification {
                                        strategy: local_meta.strategy.to_string(),
                                        topic: local_meta
                                            .topic
                                            .as_ref()
                                            .map(|topic| topic.to_string()),
                                        token: local_meta.token.clone(),
                                        local_order_id: local_meta.local_order_id.clone(),
                                        remote_order_id: order.id.clone(),
                                        side: local_meta.side,
                                        order_price: local_meta.price,
                                        order_size: local_meta.order_size,
                                        delta_size,
                                        total_matched_size,
                                        market: order.market.to_string(),
                                        asset_id: order.asset_id.to_string(),
                                        ws_price: order.price.to_string(),
                                        ws_original_size: order
                                            .original_size
                                            .map(|value| value.to_string()),
                                        ws_size_matched: order
                                            .size_matched
                                            .map(|value| value.to_string()),
                                        ws_status: status.to_string(),
                                        ws_msg_type: format!("{:?}", order.msg_type),
                                        ws_timestamp: order.timestamp,
                                    },
                                ));
                            }
                        }
                        info!(
                            target: "order",
                            order_id = %order.id,
                            local_order_id = %local_meta.local_order_id,
                            token = %local_meta.token,
                            side = ?local_meta.side,
                            delta_size = %delta_size,
                            total_matched_size = %total_matched_size,
                            "根据订单 websocket 成交增量触发策略库存更新"
                        );
                    }

                    let is_terminal = matches!(status, "canceled" | "filled" | "rejected");
                    if is_terminal {
                        correlations.remove(&local_meta.local_order_id);
                        if let Some(remote_id) = &local_meta.remote_order_id {
                            correlations.remove(remote_id.as_str());
                        }
                        let _ =
                            strategy_tx.try_send(StrategyEvent::OrderStatus(OrderStatusEvent {
                                token: local_meta.token.clone(),
                                local_order_id: local_meta.local_order_id.clone(),
                                status: Arc::from(status),
                                reason: None,
                            }));
                    }
                } else {
                    info!(
                        target: "order",
                        order_id = %order.id,
                        local_order_id = ?local_order_id,
                        market = %order.market,
                        asset_id = %order.asset_id,
                        side = ?order.side,
                        price = %order.price,
                        msg_type = ?order.msg_type,
                        original_size = ?order.original_size,
                        size_matched = ?order.size_matched,
                        timestamp = ?order.timestamp,
                        status = status,
                        "收到订单 websocket 更新，但未匹配到本地订单"
                    );
                }

                let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderUpdate);
                if matches!(status, "partially_filled" | "filled") {
                    let positions_refresh_tx = positions_refresh_tx.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderUpdate);
                        tokio::time::sleep(Duration::from_secs(12)).await;
                        let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderUpdate);
                    });
                }
            }
            Err(error) => {
                return Err(error.into());
            }
        }
    }

    Ok(())
}

fn fill_delta(
    previous_size_matched: Option<Decimal>,
    current_size_matched: Option<Decimal>,
) -> Option<Decimal> {
    let current_size_matched = current_size_matched?;
    let previous_size_matched = previous_size_matched.unwrap_or(Decimal::ZERO);
    let delta = current_size_matched - previous_size_matched;
    (delta > Decimal::ZERO).then_some(delta)
}

fn classify_ws_status(
    msg_type: &str,
    original_size: Option<&str>,
    size_matched: Option<&str>,
) -> &'static str {
    let msg_type = msg_type.to_ascii_lowercase();
    if msg_type.contains("cancel") {
        return "canceled";
    }
    if msg_type.contains("reject") {
        return "rejected";
    }
    if let (Some(original_size), Some(size_matched)) = (original_size, size_matched) {
        if original_size == size_matched {
            return "filled";
        }
        if size_matched != "0" {
            return "partially_filled";
        }
    }
    "open"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(value: f64) -> Decimal {
        Decimal::try_from(value).unwrap()
    }

    #[test]
    fn fill_delta_ignores_missing_current_size() {
        assert_eq!(fill_delta(None, None), None);
    }

    #[test]
    fn fill_delta_ignores_first_zero_size() {
        assert_eq!(fill_delta(None, Some(Decimal::ZERO)), None);
    }

    #[test]
    fn fill_delta_detects_first_positive_size() {
        assert_eq!(fill_delta(None, Some(Decimal::ONE)), Some(Decimal::ONE));
    }

    #[test]
    fn fill_delta_ignores_unchanged_size() {
        assert_eq!(fill_delta(Some(Decimal::ONE), Some(Decimal::ONE)), None);
    }

    #[test]
    fn fill_delta_detects_incremental_size() {
        assert_eq!(
            fill_delta(Some(Decimal::ONE), Some(dec(1.5))),
            Some(dec(0.5))
        );
    }

    #[test]
    fn fill_delta_ignores_size_regression() {
        assert_eq!(fill_delta(Some(dec(2.0)), Some(Decimal::ONE)), None);
    }
}
