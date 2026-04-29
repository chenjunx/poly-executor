use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::U256;
use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::auth::{LocalSigner, Signer as _};
use polymarket_client_sdk_v2::clob::types::{OrderType, Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::{Address, Decimal};
use serde_json::json;
use tracing::{info, warn};

use crate::{
    config::{AuthConfig, OrderConfig},
    positions::{PositionRefreshTrigger, SimulatedFillEvent},
    storage::OrderStore,
    strategy::{
        LocalOrderMeta, OrderCorrelationMap, OrderSignal, OrderStatusEvent, QuoteSide,
        StrategyEvent, UnifiedOrder,
    },
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
    strategy_tx: tokio::sync::mpsc::Sender<StrategyEvent>,
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
                simulate_pair_order(&auth, &order_cfg, token0, token1, ask0, ask1, gap).await;
                if !simulation_enabled {
                    let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderPlacement);
                }
            }
            UnifiedOrder::LiquidityRewardPlace {
                strategy,
                topic,
                token,
                mid,
                side,
                price,
                order_size,
                local_order_id,
                simulated,
            } => {
                let meta = LocalOrderMeta {
                    local_order_id: local_order_id.clone(),
                    remote_order_id: None,
                    strategy: strategy.clone(),
                    topic: Some(topic.clone()),
                    token: token.clone(),
                    side,
                    price,
                    order_size,
                };
                correlations.insert(local_order_id.clone(), meta.clone());
                persist_new_order(&order_store, &meta, mid, simulated || simulation_enabled);

                if simulated || simulation_enabled {
                    info!(
                        target: "order",
                        topic = %topic,
                        token = %token,
                        mid = %mid,
                        side = ?side,
                        price = %price,
                        order_size = %order_size,
                        local_order_id = %local_order_id,
                        simulated,
                        "liquidity_reward 模拟挂单成功，已记录本地订单元数据"
                    );
                    let _ = order_store.update_order_status_by_local(&local_order_id, "open");
                    let _ = order_store.append_order_event(
                        Some(&local_order_id),
                        None,
                        "submit_succeeded",
                        json!({
                            "mode": "simulation",
                            "strategy": strategy.as_ref(),
                            "topic": topic.as_ref(),
                            "token": token,
                            "side": format!("{:?}", side),
                            "price": price.to_string(),
                            "order_size": order_size.to_string(),
                        }),
                    );
                    continue;
                }

                match place_liquidity_reward_order(
                    &auth,
                    &correlations,
                    &order_store,
                    &strategy,
                    &topic,
                    &token,
                    mid,
                    side,
                    price,
                    order_size,
                    &local_order_id,
                )
                .await
                {
                    Ok(placed) => {
                        if placed {
                            let _ = positions_refresh_tx
                                .try_send(PositionRefreshTrigger::OrderPlacement);
                        }
                    }
                    Err(error) => {
                        let error = error.to_string();
                        let _ = order_store.update_order_status_by_local(&local_order_id, "failed");
                        let _ = order_store.append_order_event(
                            Some(&local_order_id),
                            None,
                            "submit_failed",
                            json!({
                                "strategy": strategy.as_ref(),
                                "topic": topic.as_ref(),
                                "token": token,
                                "side": format!("{:?}", side),
                                "price": price.to_string(),
                                "order_size": order_size.to_string(),
                                "error": error,
                            }),
                        );
                        let _ =
                            strategy_tx.try_send(StrategyEvent::OrderStatus(OrderStatusEvent {
                                token: token.clone(),
                                local_order_id: local_order_id.clone(),
                                status: Arc::from("failed"),
                            }));
                        schedule_balance_retry_if_needed(&error, &positions_refresh_tx);
                        warn!(
                            target: "order",
                            topic = %topic,
                            token = %token,
                            side = ?side,
                            price = %price,
                            order_size = %order_size,
                            error = %error,
                            "liquidity_reward 真实下单失败"
                        );
                    }
                }
            }
            UnifiedOrder::LiquidityRewardStageReplacement {
                strategy,
                topic,
                token,
                mid,
                side,
                price,
                order_size,
                active_local_order_id,
                pending_local_order_id,
                request_cancel,
                simulated,
            } => {
                let meta = LocalOrderMeta {
                    local_order_id: pending_local_order_id.clone(),
                    remote_order_id: None,
                    strategy: strategy.clone(),
                    topic: Some(topic.clone()),
                    token: token.clone(),
                    side,
                    price,
                    order_size,
                };
                correlations.insert(pending_local_order_id.clone(), meta.clone());
                let _ = order_store.upsert_order(&meta, "pending_cancel_confirm", Some(mid));
                let _ = order_store.append_order_event(
                    Some(&pending_local_order_id),
                    None,
                    "replacement_staged",
                    json!({
                        "strategy": strategy.as_ref(),
                        "topic": topic.as_ref(),
                        "token": token,
                        "active_local_order_id": active_local_order_id,
                        "side": format!("{:?}", side),
                        "price": price.to_string(),
                        "order_size": order_size.to_string(),
                        "simulated": simulated,
                    }),
                );

                if simulated || simulation_enabled {
                    if request_cancel {
                        let _ = order_store
                            .update_order_status_by_local(&active_local_order_id, "canceled");
                        correlations.remove(&active_local_order_id);
                        let _ = strategy_tx.try_send(StrategyEvent::OrderStatus(
                            OrderStatusEvent {
                                token: token.clone(),
                                local_order_id: active_local_order_id.clone(),
                                status: Arc::from("canceled"),
                            },
                        ));
                    }
                } else if request_cancel {
                    request_liquidity_reward_cancel(
                        &auth,
                        &correlations,
                        &order_store,
                        &token,
                        &active_local_order_id,
                    )
                    .await;
                }
            }
            UnifiedOrder::LiquidityRewardCancel {
                strategy,
                topic,
                token,
                side,
                active_local_order_id,
                simulated,
            } => {
                let remote_order_id = correlations
                    .get(&active_local_order_id)
                    .and_then(|entry| entry.remote_order_id.clone());
                let _ = order_store.append_order_event(
                    Some(&active_local_order_id),
                    remote_order_id.as_deref(),
                    "cancel_only_requested",
                    json!({
                        "strategy": strategy.as_ref(),
                        "topic": topic.as_ref(),
                        "token": token,
                        "side": format!("{:?}", side),
                        "simulated": simulated,
                    }),
                );
                if simulated || simulation_enabled {
                    let _ = order_store
                        .update_order_status_by_local(&active_local_order_id, "canceled");
                    correlations.remove(&active_local_order_id);
                    let _ = strategy_tx.try_send(StrategyEvent::OrderStatus(OrderStatusEvent {
                        token: token.clone(),
                        local_order_id: active_local_order_id.clone(),
                        status: Arc::from("canceled"),
                    }));
                } else {
                    request_liquidity_reward_cancel(
                        &auth,
                        &correlations,
                        &order_store,
                        &token,
                        &active_local_order_id,
                    )
                    .await;
                }
            }
        }
    }
}

async fn request_liquidity_reward_cancel(
    auth: &AuthConfig,
    correlations: &OrderCorrelationMap,
    order_store: &OrderStore,
    token: &str,
    local_order_id: &str,
) {
    let meta = correlations.get(local_order_id).map(|entry| entry.clone());
    let _ = order_store.update_order_status_by_local(local_order_id, "cancel_requested");
    let _ = order_store.append_order_event(
        Some(local_order_id),
        meta.as_ref().and_then(|entry| entry.remote_order_id.as_deref()),
        "cancel_requested",
        json!({
            "strategy": meta.as_ref().map(|entry| entry.strategy.as_ref()),
            "topic": meta.as_ref().and_then(|entry| entry.topic.as_ref().map(|topic| topic.as_ref())),
            "token": token,
        }),
    );

    let signer = match LocalSigner::from_str(&auth.private_key) {
        Ok(signer) => signer.with_chain_id(Some(POLYGON)),
        Err(error) => {
            warn!(target: "order", local_order_id = %local_order_id, error = %error, "liquidity_reward 构造 signer 失败，无法发送撤单");
            return;
        }
    };
    let funder = match Address::from_str(&auth.funder) {
        Ok(funder) => funder,
        Err(error) => {
            warn!(target: "order", local_order_id = %local_order_id, error = %error, "liquidity_reward 解析 funder 失败，无法发送撤单");
            return;
        }
    };
    let client = match Client::new(CLOB_HOST, Config::builder().use_server_time(true).build()) {
        Ok(client) => client,
        Err(error) => {
            warn!(target: "order", local_order_id = %local_order_id, error = %error, "liquidity_reward 构造客户端失败，无法发送撤单");
            return;
        }
    };
    let client = match client
        .authentication_builder(&signer)
        .funder(funder)
        .signature_type(SignatureType::Proxy)
        .authenticate()
        .await
    {
        Ok(client) => client,
        Err(error) => {
            warn!(target: "order", local_order_id = %local_order_id, error = %error, "liquidity_reward 鉴权失败，无法发送撤单");
            return;
        }
    };

    let Some(meta) = meta else {
        warn!(target: "order", local_order_id = %local_order_id, token = %token, "liquidity_reward 取消旧单时未找到本地订单元数据");
        return;
    };
    let Some(remote_order_id) = meta.remote_order_id.clone() else {
        warn!(target: "order", local_order_id = %local_order_id, token = %token, "liquidity_reward 取消旧单时尚未关联远端订单 ID");
        return;
    };

    let t_cancel = Instant::now();
    match client.cancel_order(&remote_order_id).await {
        Ok(result) => {
            let cancel_ms = t_cancel.elapsed().as_millis();
            let _ = order_store.append_order_event(
                Some(local_order_id),
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
                cancel_rtt_ms = cancel_ms,
                "liquidity_reward 已向交易所发送撤单请求"
            );
        }
        Err(error) => {
            let cancel_ms = t_cancel.elapsed().as_millis();
            warn!(
                target: "order",
                local_order_id = %local_order_id,
                remote_order_id = %remote_order_id,
                error = %error,
                cancel_rtt_ms = cancel_ms,
                "liquidity_reward 撤单请求失败"
            );
        }
    }
}

async fn place_liquidity_reward_order(
    auth: &AuthConfig,
    correlations: &OrderCorrelationMap,
    order_store: &OrderStore,
    strategy: &Arc<str>,
    topic: &Arc<str>,
    token: &str,
    mid: Decimal,
    side: QuoteSide,
    price: Decimal,
    order_size: Decimal,
    local_order_id: &str,
) -> anyhow::Result<bool> {
    let signer = LocalSigner::from_str(&auth.private_key)?.with_chain_id(Some(POLYGON));
    let funder = Address::from_str(&auth.funder)?;
    let client = Client::new(CLOB_HOST, Config::builder().use_server_time(true).build())?
        .authentication_builder(&signer)
        .funder(funder)
        .signature_type(SignatureType::Proxy)
        .authenticate()
        .await?;

    let sdk_side = to_sdk_side(side);
    let signable = client
        .limit_order()
        .token_id(U256::from_str(token)?)
        .order_type(OrderType::GTC)
        .price(price)
        .size(order_size)
        .side(sdk_side)
        .build()
        .await?;
    let signed = client.sign(&signer, signable).await?;
    let t_submit = Instant::now();
    let response = client.post_order(signed).await?;
    let submit_ms = t_submit.elapsed().as_millis();

    let Some(mut meta) = correlations.get(local_order_id).map(|entry| entry.clone()) else {
        warn!(
            target: "order",
            local_order_id = %local_order_id,
            remote_order_id = %response.order_id,
            token = %token,
            "liquidity_reward 下单成功后未找到本地订单元数据，跳过关联写回"
        );
        return Ok(false);
    };

    meta.remote_order_id = Some(response.order_id.clone());
    correlations.insert(local_order_id.to_string(), meta.clone());
    correlations.insert(response.order_id.clone(), meta.clone());
    let status = if response.success { "open" } else { "failed" };
    let _ = order_store.update_order_remote_and_status(
        local_order_id,
        &response.order_id,
        status,
        Some(mid),
    );
    let _ = order_store.append_order_event(
        Some(local_order_id),
        Some(&response.order_id),
        if response.success {
            "submit_succeeded"
        } else {
            "submit_failed"
        },
        json!({
            "strategy": strategy.as_ref(),
            "topic": topic.as_ref(),
            "token": token,
            "mid": mid.to_string(),
            "side": format!("{:?}", side),
            "price": price.to_string(),
            "order_size": order_size.to_string(),
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
        order_size = %order_size,
        status = ?response.status,
        success = response.success,
        error_msg = ?response.error_msg,
        submit_rtt_ms = submit_ms,
        "liquidity_reward 真实挂单成功，已写入本地与远端订单关联"
    );

    Ok(response.success)
}

fn schedule_balance_retry_if_needed(
    error: &str,
    positions_refresh_tx: &tokio::sync::mpsc::Sender<PositionRefreshTrigger>,
) {
    if !error.contains("not enough balance / allowance") {
        return;
    }
    let positions_refresh_tx = positions_refresh_tx.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderUpdate);
        tokio::time::sleep(Duration::from_secs(12)).await;
        let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderUpdate);
    });
}

fn persist_new_order(
    order_store: &OrderStore,
    meta: &LocalOrderMeta,
    mid: Decimal,
    simulation_enabled: bool,
) {
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
            "order_size": meta.order_size.to_string(),
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
