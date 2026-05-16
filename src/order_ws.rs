use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::auth::{LocalSigner, Signer as _};
use polymarket_client_sdk_v2::clob::ws::Client;
use polymarket_client_sdk_v2::clob::ws::types::response::{TradeMessage, WsMessage};
use polymarket_client_sdk_v2::types::{Address, Decimal};
use serde_json::json;
use tracing::{info, warn};

use crate::{
    config::AuthConfig,
    notification::{LiquidityRewardFillNotification, NotificationEvent, Notifier},
    positions::PositionRefreshTrigger,
    storage::OrderStore,
    strategy::{OrderCorrelationMap, QuoteSide},
};

pub async fn run(
    auth: AuthConfig,
    correlations: OrderCorrelationMap,
    order_store: OrderStore,
    positions_refresh_tx: tokio::sync::mpsc::Sender<PositionRefreshTrigger>,
    observation_tx: tokio::sync::mpsc::Sender<crate::order_gateway::GatewayObservation>,
    notifier: Option<Notifier>,
) {
    loop {
        match subscribe_orders(
            &auth,
            &correlations,
            &order_store,
            &positions_refresh_tx,
            &observation_tx,
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
    observation_tx: &tokio::sync::mpsc::Sender<crate::order_gateway::GatewayObservation>,
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
    let mut stream = Box::pin(client.subscribe_user_events(Vec::new())?);

    info!(target: "order", funder = %auth.funder, "已连接订单 websocket 并开始监听订单变化");

    while let Some(message) = stream.next().await {
        match message {
            Ok(WsMessage::Order(order)) => {
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
                        if let Err(error) = observation_tx.try_send(gateway_fill_observation(
                            Some(order.id.clone()),
                            Some(local_meta.local_order_id.clone()),
                            local_meta.token.clone(),
                            local_meta.side,
                            delta_size,
                            order.price,
                            order.id.clone(),
                        )) {
                            warn!(
                                target: "order",
                                strategy = %local_meta.strategy,
                                token = %local_meta.token,
                                local_order_id = %local_meta.local_order_id,
                                delta_size = %delta_size,
                                error = %error,
                                "订单成交 observation 投递 Gateway 失败"
                            );
                        }
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
                        for delay in post_fill_position_refresh_delays() {
                            tokio::time::sleep(*delay).await;
                            let _ =
                                positions_refresh_tx.try_send(PositionRefreshTrigger::OrderUpdate);
                        }
                    });
                }
            }
            Ok(WsMessage::Trade(trade)) => {
                let maker_order_ids = trade_maker_order_ids(&trade);
                info!(
                    target: "order",
                    trade_id = %trade.id,
                    market = %trade.market,
                    asset_id = %trade.asset_id,
                    side = ?trade.side,
                    status = ?trade.status,
                    size = %trade.size,
                    price = %trade.price,
                    taker_order_id = ?trade.taker_order_id,
                    maker_order_ids = ?maker_order_ids,
                    timestamp = ?trade.timestamp,
                    last_update = ?trade.last_update,
                    matchtime = ?trade.matchtime,
                    "收到 trade websocket 更新"
                );
            }
            Ok(_) => {}
            Err(error) => {
                return Err(error.into());
            }
        }
    }

    Ok(())
}

fn gateway_fill_observation(
    exch_id: Option<String>,
    local_id: Option<String>,
    token_id: String,
    side: QuoteSide,
    fill_delta: Decimal,
    fill_price: Decimal,
    trade_id: String,
) -> crate::order_gateway::GatewayObservation {
    crate::order_gateway::GatewayObservation::WsFill {
        exch_id: exch_id.map(crate::order_gateway::ExchangeOrderId::from),
        local_id: local_id.map(crate::order_gateway::LocalOrderId::from),
        token_id: crate::order_gateway::TokenId::from(token_id),
        side: match side {
            QuoteSide::Buy => crate::order_gateway::OrderSide::Buy,
            QuoteSide::Sell => crate::order_gateway::OrderSide::Sell,
        },
        fill_delta,
        fill_price,
        trade_id: Arc::from(trade_id),
    }
}

const POST_FILL_POSITION_REFRESH_DELAYS: [Duration; 4] = [
    Duration::from_secs(3),
    Duration::from_secs(12),
    Duration::from_secs(15),
    Duration::from_secs(30),
];

fn post_fill_position_refresh_delays() -> &'static [Duration] {
    &POST_FILL_POSITION_REFRESH_DELAYS
}

fn trade_maker_order_ids(trade: &TradeMessage) -> Vec<String> {
    trade
        .maker_orders
        .iter()
        .map(|maker| maker.order_id.clone())
        .collect()
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
mod gateway_observation_tests {
    use super::*;

    #[test]
    fn ws_fill_delta_maps_to_gateway_observation() {
        let observation = gateway_fill_observation(
            Some("exch-1".to_string()),
            None,
            "token-1".to_string(),
            QuoteSide::Buy,
            Decimal::try_from(2_f64).unwrap(),
            Decimal::try_from(0.42_f64).unwrap(),
            "trade-1".to_string(),
        );

        assert!(matches!(
            observation,
            crate::order_gateway::GatewayObservation::WsFill { .. }
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_client_sdk_v2::clob::ws::types::response::TradeMessageStatus;

    fn dec(value: f64) -> Decimal {
        Decimal::try_from(value).unwrap()
    }

    #[test]
    fn post_fill_position_refreshes_extend_to_one_minute() {
        let cumulative = post_fill_position_refresh_delays()
            .iter()
            .scan(Duration::ZERO, |elapsed, delay| {
                *elapsed += *delay;
                Some(*elapsed)
            })
            .collect::<Vec<_>>();

        assert_eq!(
            cumulative,
            vec![
                Duration::from_secs(3),
                Duration::from_secs(15),
                Duration::from_secs(30),
                Duration::from_secs(60),
            ]
        );
    }

    #[test]
    fn trade_maker_order_ids_extracts_order_ids() {
        let trade = TradeMessage::builder()
            .id("trade-1".to_string())
            .market(
                "0xfbc0c760359fe3f73b833535186c9592deda90f373d79b10c0af6ea6a1f947f1"
                    .parse()
                    .unwrap(),
            )
            .asset_id(
                "31266632690440281732493182712982317452788219157475457369452413915821186184190"
                    .parse()
                    .unwrap(),
            )
            .side(polymarket_client_sdk_v2::clob::types::Side::Buy)
            .size(dec(1.0))
            .price(dec(0.56))
            .status(TradeMessageStatus::Matched)
            .taker_order_id("taker-1".to_string())
            .maker_orders(vec![
                polymarket_client_sdk_v2::clob::ws::types::response::MakerOrder::builder()
                    .asset_id(
                        "31266632690440281732493182712982317452788219157475457369452413915821186184190"
                            .parse()
                            .unwrap(),
                    )
                    .matched_amount(dec(1.0))
                    .order_id("maker-1".to_string())
                    .outcome("YES".to_string())
                    .owner("00000000-0000-0000-0000-000000000001".parse().unwrap())
                    .price(dec(0.56))
                    .build(),
            ])
            .build();

        assert_eq!(trade_maker_order_ids(&trade), vec!["maker-1".to_string()]);
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
