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
                routes.entry(topic.clone()).or_default().push(strategy.clone());
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
                StrategyEvent::OrderStatus(order_event) => {
                    if let Some(strategies) = self.position_routes.get(&order_event.token) {
                        for strategy in strategies {
                            if let Err(err) = strategy.tx.try_send(event.clone()) {
                                warn!(
                                    strategy = %strategy.name,
                                    asset_id = %order_event.token,
                                    error = %err,
                                    "dispatcher 投递订单状态事件失败"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
