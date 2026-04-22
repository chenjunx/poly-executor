use std::collections::HashMap;
use std::sync::Arc;

use tracing::warn;

use crate::strategy::{MarketEvent, StrategyHandle};

pub struct Dispatcher {
    routes: HashMap<Arc<str>, Vec<StrategyHandle>>,
}

impl Dispatcher {
    pub fn new(strategies: Vec<StrategyHandle>) -> Self {
        let mut routes: HashMap<Arc<str>, Vec<StrategyHandle>> = HashMap::new();
        for strategy in strategies {
            for topic in strategy.topics.iter() {
                routes.entry(topic.clone()).or_default().push(strategy.clone());
            }
        }
        Self { routes }
    }

    pub async fn run(self, mut rx: tokio::sync::mpsc::Receiver<MarketEvent>) {
        while let Some(event) = rx.recv().await {
            if let Some(strategies) = self.routes.get(&event.topic) {
                for strategy in strategies {
                    if let Err(err) = strategy.tx.try_send(event.clone()) {
                        warn!(
                            strategy = %strategy.name,
                            topic = %event.topic,
                            error = %err,
                            "dispatcher 投递策略事件失败"
                        );
                    }
                }
            }
        }
    }
}
