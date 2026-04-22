use std::sync::Arc;

use polymarket_client_sdk::types::Decimal;
use tracing::info;

use crate::{
    AuthConfig, OrderConfig,
    positions::PositionRefreshTrigger,
    strategy::{LocalOrderMeta, OrderCorrelationMap, OrderSignal, UnifiedOrder},
};

pub async fn run(
    mut order_rx: tokio::sync::mpsc::Receiver<OrderSignal>,
    auth: AuthConfig,
    order_cfg: OrderConfig,
    correlations: OrderCorrelationMap,
    positions_refresh_tx: tokio::sync::mpsc::Sender<PositionRefreshTrigger>,
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
                let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderPlacement);
            }
            UnifiedOrder::MidRequote {
                topic,
                token,
                mid,
                side,
                price,
                cancelled_order_ids,
                new_order_ids,
            } => {
                for local_order_id in new_order_ids.iter() {
                    correlations.insert(
                        local_order_id.clone(),
                        LocalOrderMeta {
                            local_order_id: local_order_id.clone(),
                            strategy: Arc::from("mid_requote"),
                            topic: Some(topic.clone()),
                            token: token.clone(),
                            side,
                            price,
                        },
                    );
                }

                info!(
                    target: "order",
                    topic = %topic,
                    token = %token,
                    mid = %mid,
                    side = ?side,
                    price = %price,
                    cancelled_orders = ?cancelled_order_ids,
                    new_orders = ?new_order_ids,
                    "mid_requote 模拟挂单成功，已记录本地订单元数据"
                );

                let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderPlacement);
            }
        }
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
