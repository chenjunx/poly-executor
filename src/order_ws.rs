use std::str::FromStr;
use std::time::Duration;

use futures::StreamExt as _;
use polymarket_client_sdk::auth::{Credentials, Uuid};
use polymarket_client_sdk::clob::ws::Client;
use polymarket_client_sdk::types::Address;
use tracing::{info, warn};

use crate::{
    AuthConfig,
    positions::PositionRefreshTrigger,
    strategy::OrderCorrelationMap,
};

pub async fn run(
    auth: AuthConfig,
    correlations: OrderCorrelationMap,
    positions_refresh_tx: tokio::sync::mpsc::Sender<PositionRefreshTrigger>,
) {
    loop {
        match subscribe_orders(&auth, &correlations, &positions_refresh_tx).await {
            Ok(()) => warn!(target: "order", "订单 websocket 已断开，5 秒后重连"),
            Err(error) => warn!(target: "order", error = %error, "订单 websocket 监听失败，5 秒后重连"),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn subscribe_orders(
    auth: &AuthConfig,
    correlations: &OrderCorrelationMap,
    positions_refresh_tx: &tokio::sync::mpsc::Sender<PositionRefreshTrigger>,
) -> anyhow::Result<()> {
    let api_key = Uuid::parse_str(&auth.api_key)?;
    let address = Address::from_str(&auth.funder)?;
    let credentials = Credentials::new(
        api_key,
        auth.api_secret.clone(),
        auth.passphrase.clone(),
    );

    let client = Client::default().authenticate(credentials, address)?;
    let mut stream = Box::pin(client.subscribe_orders(Vec::new())?);

    info!(target: "order", funder = %auth.funder, "已连接订单 websocket 并开始监听订单变化");

    while let Some(message) = stream.next().await {
        match message {
            Ok(order) => {
                if let Some(local_meta) = correlations.get(&order.id) {
                    info!(
                        target: "order",
                        order_id = %order.id,
                        local_order_id = %local_meta.local_order_id,
                        strategy = %local_meta.strategy,
                        topic = ?local_meta.topic,
                        token = %local_meta.token,
                        local_side = ?local_meta.side,
                        local_price = %local_meta.price,
                        market = %order.market,
                        asset_id = %order.asset_id,
                        side = ?order.side,
                        price = %order.price,
                        msg_type = ?order.msg_type,
                        original_size = ?order.original_size,
                        size_matched = ?order.size_matched,
                        timestamp = ?order.timestamp,
                        "收到订单 websocket 更新，并成功关联本地订单"
                    );
                } else {
                    info!(
                        target: "order",
                        order_id = %order.id,
                        market = %order.market,
                        asset_id = %order.asset_id,
                        side = ?order.side,
                        price = %order.price,
                        msg_type = ?order.msg_type,
                        original_size = ?order.original_size,
                        size_matched = ?order.size_matched,
                        timestamp = ?order.timestamp,
                        "收到订单 websocket 更新，但未匹配到本地订单"
                    );
                }

                let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderUpdate);
            }
            Err(error) => {
                return Err(error.into());
            }
        }
    }

    Ok(())
}
