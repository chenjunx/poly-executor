use std::str::FromStr;
use std::sync::Arc;

use polymarket_client_sdk::auth::{LocalSigner, Signer as _};
use polymarket_client_sdk::clob::types::{OrderType, Side, SignatureType};
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::types::{Address, Decimal};
use polymarket_client_sdk::POLYGON;
use serde_json::json;
use tracing::{info, warn};

use crate::{
    AuthConfig, OrderConfig,
    positions::{PositionRefreshTrigger, SimulatedFillEvent},
    storage::OrderStore,
    strategy::{LocalOrderMeta, OrderCorrelationMap, OrderSignal, QuoteSide, UnifiedOrder},
};

const CLOB_HOST: &str = "https://clob.polymarket.com";

pub async fn run(
    mut order_rx: tokio::sync::mpsc::Receiver<OrderSignal>,
    auth: AuthConfig,
    order_cfg: OrderConfig,
    simulation_enabled: bool,
    correlations: OrderCorrelationMap,
    order_store: OrderStore,
    positions_refresh_tx: tokio::sync::mpsc::Sender<PositionRefreshTrigger>,
    sim_fill_tx: tokio::sync::mpsc::Sender<SimulatedFillEvent>,
) {
    while let Some(signal) = order_rx.recv().await {
        let order = UnifiedOrder::from(signal);
        match order {
            UnifiedOrder::PairArbitrage {
                token0,
                token1,
                ask0,
                ask1,
                gap,
            } => {
                if !order_cfg.enabled {
                    continue;
                }
                simulate_pair_order(&auth, &order_cfg, token0, token1, ask0, ask1, gap).await;
                if !simulation_enabled {
                    let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderPlacement);
                }
            }
            UnifiedOrder::MidRequote {
                topic,
                token,
                mid,
                side,
                price,
                min_order_size,
                cancelled_order_ids,
                new_order_ids,
            } => {
                let base_meta = LocalOrderMeta {
                    local_order_id: String::new(),
                    remote_order_id: None,
                    strategy: Arc::from("mid_requote"),
                    topic: Some(topic.clone()),
                    token: token.clone(),
                    side,
                    price,
                    min_order_size,
                };

                for local_order_id in cancelled_order_ids.iter() {
                    let _ = order_store.update_order_status_by_local(local_order_id, "cancel_requested");
                    let _ = order_store.append_order_event(
                        Some(local_order_id),
                        correlations
                            .get(local_order_id)
                            .and_then(|entry| entry.remote_order_id.clone())
                            .as_deref(),
                        "cancel_requested",
                        json!({
                            "topic": topic.as_ref(),
                            "token": token,
                            "mid": mid.to_string(),
                        }),
                    );
                }

                for local_order_id in new_order_ids.iter() {
                    let mut meta = base_meta.clone();
                    meta.local_order_id = local_order_id.clone();
                    correlations.insert(local_order_id.clone(), meta.clone());
                    persist_new_order(&order_store, &meta, mid, simulation_enabled);
                }

                if simulation_enabled {
                    info!(
                        target: "order",
                        topic = %topic,
                        token = %token,
                        mid = %mid,
                        side = ?side,
                        price = %price,
                        min_order_size = %min_order_size,
                        cancelled_orders = ?cancelled_order_ids,
                        new_orders = ?new_order_ids,
                        "mid_requote 模拟挂单成功，已记录本地订单元数据"
                    );

                    for local_order_id in new_order_ids.iter() {
                        let _ = order_store.update_order_status_by_local(local_order_id, "open");
                        let _ = order_store.append_order_event(
                            Some(local_order_id),
                            None,
                            "submit_succeeded",
                            json!({
                                "mode": "simulation",
                                "topic": topic.as_ref(),
                                "token": token,
                                "side": format!("{:?}", side),
                                "price": price.to_string(),
                                "min_order_size": min_order_size.to_string(),
                            }),
                        );
                        let sim_fill_tx = sim_fill_tx.clone();
                        let fill_event = SimulatedFillEvent {
                            local_order_id: local_order_id.clone(),
                            token: token.clone(),
                            side,
                            price,
                            size: min_order_size,
                        };
                        tokio::spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                            let _ = sim_fill_tx.send(fill_event).await;
                        });
                    }
                    continue;
                }

                if !order_cfg.enabled {
                    continue;
                }

                match place_mid_requote_orders(
                    &auth,
                    &correlations,
                    &order_store,
                    &topic,
                    &token,
                    mid,
                    side,
                    price,
                    min_order_size,
                    cancelled_order_ids.as_ref(),
                    new_order_ids.as_ref(),
                )
                .await
                {
                    Ok(placed_any) => {
                        if placed_any {
                            let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderPlacement);
                        }
                    }
                    Err(error) => {
                        for local_order_id in new_order_ids.iter() {
                            let _ = order_store.update_order_status_by_local(local_order_id, "failed");
                            let _ = order_store.append_order_event(
                                Some(local_order_id),
                                None,
                                "submit_failed",
                                json!({
                                    "topic": topic.as_ref(),
                                    "token": token,
                                    "side": format!("{:?}", side),
                                    "price": price.to_string(),
                                    "min_order_size": min_order_size.to_string(),
                                    "error": error.to_string(),
                                }),
                            );
                        }
                        warn!(
                            target: "order",
                            topic = %topic,
                            token = %token,
                            side = ?side,
                            price = %price,
                            min_order_size = %min_order_size,
                            error = %error,
                            "mid_requote 真实下单失败"
                        );
                    }
                }
            }
        }
    }
}

async fn place_mid_requote_orders(
    auth: &AuthConfig,
    correlations: &OrderCorrelationMap,
    order_store: &OrderStore,
    topic: &Arc<str>,
    token: &str,
    mid: Decimal,
    side: QuoteSide,
    price: Decimal,
    min_order_size: Decimal,
    cancelled_order_ids: &[String],
    new_order_ids: &[String],
) -> anyhow::Result<bool> {
    let signer = LocalSigner::from_str(&auth.private_key)?.with_chain_id(Some(POLYGON));
    let funder = Address::from_str(&auth.funder)?;
    let client = Client::new(CLOB_HOST, Config::builder().use_server_time(true).build())?
        .authentication_builder(&signer)
        .funder(funder)
        .signature_type(SignatureType::Proxy)
        .authenticate()
        .await?;

    for local_order_id in cancelled_order_ids {
        let Some(meta) = correlations.get(local_order_id).map(|entry| entry.clone()) else {
            warn!(
                target: "order",
                local_order_id = %local_order_id,
                token = %token,
                "mid_requote 取消旧单时未找到本地订单元数据"
            );
            continue;
        };

        let Some(remote_order_id) = meta.remote_order_id.clone() else {
            warn!(
                target: "order",
                local_order_id = %local_order_id,
                token = %token,
                "mid_requote 取消旧单时尚未关联远端订单 ID"
            );
            continue;
        };

        let result = client.cancel_order(&remote_order_id).await?;
        let _ = order_store.append_order_event(
            Some(&local_order_id),
            Some(&remote_order_id),
            "cancel_response",
            json!({
                "canceled": result.canceled,
                "not_canceled": result.not_canceled,
            }),
        );
        info!(
            target: "order",
            local_order_id = %local_order_id,
            remote_order_id = %remote_order_id,
            canceled = ?result.canceled,
            not_canceled = ?result.not_canceled,
            "mid_requote 已向交易所发送撤单请求"
        );
    }

    let sdk_side = to_sdk_side(side);
    let mut placed_any = false;

    for local_order_id in new_order_ids {
        let signable = client
            .limit_order()
            .token_id(token)
            .order_type(OrderType::GTC)
            .price(price)
            .size(min_order_size)
            .side(sdk_side)
            .build()
            .await?;
        let signed = client.sign(&signer, signable).await?;
        let response = client.post_order(signed).await?;

        let Some(mut meta) = correlations.get(local_order_id).map(|entry| entry.clone()) else {
            warn!(
                target: "order",
                local_order_id = %local_order_id,
                remote_order_id = %response.order_id,
                token = %token,
                "mid_requote 下单成功后未找到本地订单元数据，跳过关联写回"
            );
            continue;
        };

        meta.remote_order_id = Some(response.order_id.clone());
        correlations.insert(local_order_id.clone(), meta.clone());
        correlations.insert(response.order_id.clone(), meta.clone());
        let status = if response.success { "open" } else { "failed" };
        let _ = order_store.update_order_remote_and_status(local_order_id, &response.order_id, status, Some(mid));
        let _ = order_store.append_order_event(
            Some(local_order_id),
            Some(&response.order_id),
            if response.success { "submit_succeeded" } else { "submit_failed" },
            json!({
                "topic": topic.as_ref(),
                "token": token,
                "mid": mid.to_string(),
                "side": format!("{:?}", side),
                "price": price.to_string(),
                "min_order_size": min_order_size.to_string(),
                "status": format!("{:?}", response.status),
                "success": response.success,
                "error_msg": response.error_msg,
            }),
        );

        info!(
            target: "order",
            topic = %topic,
            token = %token,
            mid = %mid,
            local_order_id = %local_order_id,
            remote_order_id = %response.order_id,
            side = ?side,
            price = %price,
            min_order_size = %min_order_size,
            status = ?response.status,
            success = response.success,
            error_msg = ?response.error_msg,
            cancelled_orders = ?cancelled_order_ids,
            new_orders = ?new_order_ids,
            "mid_requote 真实挂单成功，已写入本地与远端订单关联"
        );

        placed_any = true;
    }

    Ok(placed_any)
}

fn persist_new_order(order_store: &OrderStore, meta: &LocalOrderMeta, mid: Decimal, simulation_enabled: bool) {
    let _ = order_store.upsert_order(meta, "pending_submit", Some(mid));
    let _ = order_store.append_order_event(
        Some(&meta.local_order_id),
        meta.remote_order_id.as_deref(),
        "signal_generated",
        json!({
            "strategy": meta.strategy.as_ref(),
            "topic": meta.topic.as_ref().map(|topic| topic.as_ref()),
            "token": meta.token,
            "side": format!("{:?}", meta.side),
            "price": meta.price.to_string(),
            "min_order_size": meta.min_order_size.to_string(),
            "mode": if simulation_enabled { "simulation" } else { "real" },
        }),
    );
}

fn to_sdk_side(side: QuoteSide) -> Side {
    match side {
        QuoteSide::Buy => Side::Buy,
        QuoteSide::Sell => Side::Sell,
    }
}

async fn simulate_pair_order(
    auth: &AuthConfig,
    cfg: &OrderConfig,
    token0: String,
    token1: String,
    ask0: Decimal,
    ask1: Decimal,
    gap: Decimal,
) {
    let _ = auth;

    info!(
        target: "order",
        gap = %gap,
        token0 = %token0,
        ask0 = %ask0,
        token1 = %token1,
        ask1 = %ask1,
        size_usdc = cfg.size_usdc,
        "pair_arbitrage 模拟交易成功"
    );
}
