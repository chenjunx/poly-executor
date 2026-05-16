use std::collections::HashMap;
use std::sync::Arc;

use tracing::warn;

use crate::strategy::{StrategyEvent, StrategyHandle};

pub struct Dispatcher {
    routes: HashMap<Arc<str>, Vec<StrategyHandle>>,
    position_routes: HashMap<String, Vec<StrategyHandle>>,
}

impl Dispatcher {
    pub fn new(strategies: Vec<StrategyHandle>) -> Self {
        let mut routes: HashMap<Arc<str>, Vec<StrategyHandle>> = HashMap::new();
        let mut position_routes: HashMap<String, Vec<StrategyHandle>> = HashMap::new();
        for strategy in &strategies {
            for topic in strategy.topics.iter() {
                routes
                    .entry(topic.clone())
                    .or_default()
                    .push(strategy.clone());
            }
            for token in strategy.related_tokens.iter() {
                position_routes
                    .entry(token.clone())
                    .or_default()
                    .push(strategy.clone());
            }
        }
        Self {
            routes,
            position_routes,
        }
    }

    pub async fn run(self, mut rx: tokio::sync::mpsc::Receiver<StrategyEvent>) {
        while let Some(event) = rx.recv().await {
            match &event {
                StrategyEvent::Market(market_event) => {
                    if let Some(strategies) = self.routes.get(&market_event.topic) {
                        for strategy in strategies {
                            if let Err(err) = strategy.tx.try_send(event.clone()) {
                                warn!(
                                    strategy = %strategy.name,
                                    topic = %market_event.topic,
                                    error = %err,
                                    "dispatcher 投递策略事件失败"
                                );
                            }
                        }
                    }
                }
                StrategyEvent::Positions(positions_event) => {
                    let mut notified: std::collections::HashSet<Arc<str>> =
                        std::collections::HashSet::new();
                    for asset_id in positions_event.changed_assets.iter() {
                        let Some(strategies) = self.position_routes.get(asset_id) else {
                            continue;
                        };
                        for strategy in strategies {
                            if !notified.insert(strategy.name.clone()) {
                                continue;
                            }
                            if let Err(err) = strategy.tx.try_send(event.clone()) {
                                warn!(
                                    strategy = %strategy.name,
                                    asset_id = %asset_id,
                                    error = %err,
                                    "dispatcher 投递仓位事件失败"
                                );
                            }
                        }
                    }
                }
                StrategyEvent::RewardPoolRemoval(removal_event) => {
                    let mut notified: std::collections::HashSet<Arc<str>> =
                        std::collections::HashSet::new();
                    for token in [&removal_event.token1, &removal_event.token2] {
                        let Some(strategies) = self.position_routes.get(token) else {
                            continue;
                        };
                        for strategy in strategies {
                            if !notified.insert(strategy.name.clone()) {
                                continue;
                            }
                            if let Err(err) = strategy.tx.try_send(event.clone()) {
                                warn!(
                                    strategy = %strategy.name,
                                    condition_id = %removal_event.condition_id,
                                    error = %err,
                                    "dispatcher 投递奖励池剔除事件失败"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap};

    use polymarket_client_sdk_v2::types::Decimal;

    use crate::strategy::{
        CleanOrderbook, MarketEvent, PositionSnapshot, PositionView, PositionsUpdateEvent,
    };

    fn test_market_event(topic: &str, token: &str) -> StrategyEvent {
        StrategyEvent::Market(MarketEvent {
            topic: Arc::from(topic),
            asset_id: Arc::from(token),
            book: CleanOrderbook {
                best_bid_price: 4000,
                best_bid_size: 100,
                best_ask_price: 4100,
                best_ask_size: 100,
                timestamp_ms: 1,
                bids: Arc::new(BTreeMap::new()),
                asks: Arc::new(BTreeMap::new()),
            },
        })
    }

    fn test_positions_event(token: &str) -> StrategyEvent {
        let mut by_asset = HashMap::new();
        by_asset.insert(
            token.to_string(),
            PositionView {
                asset_id: token.to_string(),
                size: Decimal::ONE,
                avg_price: Decimal::ONE,
                cur_price: Decimal::ONE,
                current_value: Decimal::ONE,
                cash_pnl: Decimal::ZERO,
                title: Arc::from("title"),
                outcome: Arc::from("outcome"),
            },
        );
        StrategyEvent::Positions(PositionsUpdateEvent {
            snapshot: Arc::new(PositionSnapshot {
                by_asset: Arc::new(by_asset),
            }),
            changed_assets: Arc::from([token.to_string()]),
        })
    }

    #[tokio::test]
    async fn dispatcher_keeps_market_routing_without_order_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let strategy = StrategyHandle {
            name: Arc::from("test"),
            topics: Arc::from([Arc::from("topic")]),
            related_tokens: Arc::from(["token-1".to_string()]),
            tx,
        };
        let dispatcher = Dispatcher::new(vec![strategy]);
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(4);
        let task = tokio::spawn(dispatcher.run(input_rx));

        input_tx
            .send(test_market_event("topic", "token-1"))
            .await
            .unwrap();
        drop(input_tx);

        let event = rx.recv().await.expect("market event routed");
        assert!(matches!(event, StrategyEvent::Market(_)));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn dispatcher_keeps_position_routing_without_order_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let strategy = StrategyHandle {
            name: Arc::from("test"),
            topics: Arc::from([Arc::from("topic")]),
            related_tokens: Arc::from(["token-1".to_string()]),
            tx,
        };
        let dispatcher = Dispatcher::new(vec![strategy]);
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(4);
        let task = tokio::spawn(dispatcher.run(input_rx));

        input_tx
            .send(test_positions_event("token-1"))
            .await
            .unwrap();
        drop(input_tx);

        let event = rx.recv().await.expect("position event routed");
        assert!(matches!(event, StrategyEvent::Positions(_)));
        task.await.unwrap();
    }
}
