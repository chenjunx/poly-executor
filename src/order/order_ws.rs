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

use crate::{config::AuthConfig, storage::OrderStore};

pub async fn run(
    auth: AuthConfig,
    order_store: OrderStore,
    observation_tx: tokio::sync::mpsc::Sender<crate::order_gateway::GatewayObservation>,
) {
    loop {
        match subscribe_orders(&auth, &order_store, &observation_tx).await {
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
    order_store: &OrderStore,
    observation_tx: &tokio::sync::mpsc::Sender<crate::order_gateway::GatewayObservation>,
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
                let local_meta = match order_store.find_order_by_remote(&order.id) {
                    Ok(Some(stored_order)) => {
                        let meta = stored_order.to_local_order_meta();
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
                    if let Err(error) =
                        observation_tx.try_send(gateway_private_ws_order_update_observation(
                            order.id.clone(),
                            local_meta.token.clone(),
                            order.market.to_string(),
                            order.price,
                            previous_size_matched,
                            current_size_matched,
                            order.original_size,
                            Some(status),
                        ))
                    {
                        warn!(
                            target: "order",
                            strategy = %local_meta.strategy,
                            token = %local_meta.token,
                            local_order_id = %local_meta.local_order_id,
                            error = %error,
                            "订单 websocket observation 投递 Gateway 失败"
                        );
                    }
                    info!(
                        target: "order",
                        order_id = %order.id,
                        local_order_id = %local_meta.local_order_id,
                        token = %local_meta.token,
                        side = ?local_meta.side,
                        current_size_matched = ?current_size_matched,
                        original_size = ?order.original_size,
                        "订单 websocket 匹配状态已投递 Gateway，等待 settlement 确认"
                    );
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
                for observation in trade_settlement_observations(&trade) {
                    if let Err(error) = observation_tx.try_send(observation) {
                        warn!(target: "order", trade_id = %trade.id, error = %error, "trade settlement observation 投递 Gateway 失败");
                    }
                }
            }
            Ok(_) => {}
            Err(error) => {
                return Err(error.into());
            }
        }
    }

    Ok(())
}

fn gateway_private_ws_order_update_observation(
    exch_id: String,
    token_id: String,
    market_id: String,
    fill_price: Decimal,
    previous_size_matched: Option<Decimal>,
    current_size_matched: Option<Decimal>,
    original_size: Option<Decimal>,
    remote_status_code: Option<&str>,
) -> crate::order_gateway::GatewayObservation {
    crate::order_gateway::GatewayObservation::PrivateWsOrderUpdate(
        crate::order_gateway::PrivateWsOrderUpdate {
            exch_id: crate::order_gateway::ExchangeOrderId::from(exch_id),
            token_id: crate::order_gateway::TokenId::from(token_id),
            market_id: crate::order_gateway::MarketId::from(market_id),
            fill_price,
            previous_size_matched,
            current_size_matched,
            original_size,
            remote_status_code: remote_status_code.map(Arc::from),
            ts_ns: 0,
            recovery: false,
        },
    )
}

fn trade_maker_order_ids(trade: &TradeMessage) -> Vec<String> {
    trade
        .maker_orders
        .iter()
        .map(|maker| maker.order_id.clone())
        .collect()
}

fn trade_settlement_observations(
    trade: &TradeMessage,
) -> Vec<crate::order_gateway::GatewayObservation> {
    let Some(transaction_hash) = trade.transaction_hash else {
        return Vec::new();
    };
    let transaction_hash = Arc::<str>::from(format!("{transaction_hash:#x}"));
    let ts_ns = trade.timestamp.unwrap_or_default() as u64 * 1_000_000;
    let mut observations = Vec::new();
    if let Some(taker_order_id) = trade.taker_order_id.as_ref() {
        observations.push(
            crate::order_gateway::GatewayObservation::SettlementTradeObserved {
                exch_id: crate::order_gateway::ExchangeOrderId::from(taker_order_id.as_str()),
                transaction_hash: transaction_hash.clone(),
                fill_qty: trade.size,
                fill_price: trade.price,
                ts_ns,
                recovery: false,
            },
        );
    }
    for maker in &trade.maker_orders {
        observations.push(
            crate::order_gateway::GatewayObservation::SettlementTradeObserved {
                exch_id: crate::order_gateway::ExchangeOrderId::from(maker.order_id.as_str()),
                transaction_hash: transaction_hash.clone(),
                fill_qty: maker.matched_amount,
                fill_price: maker.price,
                ts_ns,
                recovery: false,
            },
        );
    }
    observations
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
    if let (Some(_original_size), Some(size_matched)) = (original_size, size_matched) {
        if size_matched != "0" {
            return "matched";
        }
    }
    "open"
}

#[cfg(test)]
mod gateway_observation_tests {
    use super::*;
    use polymarket_client_sdk_v2::clob::ws::types::response::TradeMessageStatus;

    fn dec(value: f64) -> Decimal {
        Decimal::try_from(value).unwrap()
    }

    #[test]
    fn trade_message_maps_taker_and_maker_orders_to_settlement_observations() {
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
            .size(dec(2.0))
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
                    .matched_amount(dec(1.25))
                    .order_id("maker-1".to_string())
                    .outcome("YES".to_string())
                    .owner("00000000-0000-0000-0000-000000000001".parse().unwrap())
                    .price(dec(0.56))
                    .build(),
            ])
            .transaction_hash(
                "0x0000000000000000000000000000000000000000000000000000000000000abc"
                    .parse()
                    .unwrap(),
            )
            .build();

        let observations = trade_settlement_observations(&trade);

        assert_eq!(observations.len(), 2);
        assert!(observations.iter().any(|observation| matches!(
            observation,
            crate::order_gateway::GatewayObservation::SettlementTradeObserved {
                exch_id,
                transaction_hash,
                fill_qty,
                fill_price,
                ..
            } if exch_id.as_str() == "taker-1"
                && transaction_hash.as_ref() == "0x0000000000000000000000000000000000000000000000000000000000000abc"
                && *fill_qty == dec(2.0)
                && *fill_price == dec(0.56)
        )));
        assert!(observations.iter().any(|observation| matches!(
            observation,
            crate::order_gateway::GatewayObservation::SettlementTradeObserved {
                exch_id,
                transaction_hash,
                fill_qty,
                fill_price,
                ..
            } if exch_id.as_str() == "maker-1"
                && transaction_hash.as_ref() == "0x0000000000000000000000000000000000000000000000000000000000000abc"
                && *fill_qty == dec(1.25)
                && *fill_price == dec(0.56)
        )));
    }

    #[test]
    fn ws_order_update_with_matched_size_maps_to_open_not_fill_observation() {
        let observation = gateway_private_ws_order_update_observation(
            "exch-1".to_string(),
            "token-1".to_string(),
            "market-1".to_string(),
            Decimal::try_from(0.42_f64).unwrap(),
            Some(Decimal::ZERO),
            Some(Decimal::try_from(10_f64).unwrap()),
            Some(Decimal::try_from(10_f64).unwrap()),
            Some("matched"),
        );

        let crate::order_gateway::GatewayObservation::PrivateWsOrderUpdate(update) = observation
        else {
            panic!("order update should remain a private ws order update");
        };
        assert_eq!(
            update.current_size_matched,
            Some(Decimal::try_from(10_f64).unwrap())
        );
    }

    #[test]
    fn ws_order_update_maps_to_gateway_private_ws_observation() {
        let observation = gateway_private_ws_order_update_observation(
            "exch-1".to_string(),
            "token-1".to_string(),
            "market-1".to_string(),
            Decimal::try_from(0.42_f64).unwrap(),
            Some(Decimal::ONE),
            Some(Decimal::try_from(3_f64).unwrap()),
            Some(Decimal::try_from(10_f64).unwrap()),
            Some("matched"),
        );

        assert!(matches!(
            observation,
            crate::order_gateway::GatewayObservation::PrivateWsOrderUpdate { .. }
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
    fn classify_ws_status_treats_matched_size_as_matched_not_filled() {
        assert_eq!(
            classify_ws_status("matched", Some("10"), Some("10")),
            "matched"
        );
    }
}
