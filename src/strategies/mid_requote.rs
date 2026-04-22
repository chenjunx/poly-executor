use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use polymarket_client_sdk::types::Decimal;
use tracing::warn;

use crate::strategy::{MarketEvent, OrderSignal, Strategy, StrategyRegistration, TopicRegistration};

const DEFAULT_TOPIC: &str = "mid";
const PRICE_SCALE: u32 = 10_000;

#[derive(Debug, Clone)]
pub struct MidRequoteRule {
    pub topic: Arc<str>,
    pub token: String,
    pub lower_offset: Decimal,
    pub upper_offset: Decimal,
}

#[derive(Debug, Clone)]
struct ActiveOrders {
    buy_order_id: String,
    sell_order_id: String,
}

#[derive(Debug, Clone)]
struct RequoteState {
    last_mid: Option<Decimal>,
    active_orders: Option<ActiveOrders>,
}

pub struct MidRequoteStrategy {
    rules: Arc<HashMap<String, MidRequoteRule>>,
}

impl MidRequoteStrategy {
    pub fn from_csv(csv_file: &str) -> anyhow::Result<Option<(Self, StrategyRegistration)>> {
        let csv_path = resolve_csv_path(csv_file);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(&csv_path)
            .map_err(|e| anyhow::anyhow!("无法打开 {}: {}", csv_path, e))?;

        let mut rules = Vec::new();
        for result in reader.records() {
            let record = result?;
            if record.len() < 2 {
                continue;
            }

            let token = record[0].trim();
            let offset = record[1].trim();
            if token.is_empty() || offset.is_empty() {
                continue;
            }

            let offset = Decimal::try_from(offset.parse::<f64>()?)?;
            let topic: Arc<str> = if record.len() >= 3 && !record[2].trim().is_empty() {
                Arc::from(record[2].trim())
            } else {
                Arc::from(DEFAULT_TOPIC)
            };

            rules.push(MidRequoteRule {
                topic,
                token: token.to_string(),
                lower_offset: offset,
                upper_offset: offset,
            });
        }

        Self::from_rules(rules)
    }

    pub fn from_rules(
        rules: Vec<MidRequoteRule>,
    ) -> anyhow::Result<Option<(Self, StrategyRegistration)>> {
        if rules.is_empty() {
            return Ok(None);
        }

        let mut rule_map = HashMap::new();
        let mut topic_tokens: HashMap<Arc<str>, Vec<String>> = HashMap::new();

        for rule in rules {
            topic_tokens
                .entry(rule.topic.clone())
                .or_default()
                .push(rule.token.clone());
            rule_map.insert(rule.token.clone(), rule);
        }

        let mut topics: Vec<Arc<str>> = topic_tokens.keys().cloned().collect();
        topics.sort();

        let topic_tokens = topic_tokens
            .into_iter()
            .map(|(topic, mut tokens)| {
                tokens.sort();
                tokens.dedup();
                TopicRegistration {
                    topic,
                    tokens: Arc::<[String]>::from(tokens),
                }
            })
            .collect::<Vec<_>>();

        let strategy = Self {
            rules: Arc::new(rule_map),
        };
        let registration = StrategyRegistration {
            name: Arc::from(strategy.name()),
            topics: Arc::<[Arc<str>]>::from(topics),
            topic_tokens: Arc::<[TopicRegistration]>::from(topic_tokens),
        };

        Ok(Some((strategy, registration)))
    }
}

impl Strategy for MidRequoteStrategy {
    fn name(&self) -> &str {
        "mid_requote"
    }

    fn spawn(
        self,
        mut rx: tokio::sync::mpsc::Receiver<MarketEvent>,
        order_tx: tokio::sync::mpsc::Sender<OrderSignal>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut states: HashMap<String, RequoteState> = HashMap::new();

            while let Some(event) = rx.recv().await {
                let Some(rule) = self.rules.get(event.asset_id.as_ref()) else {
                    continue;
                };

                let bid = Decimal::from(event.book.best_bid_price) / Decimal::from(PRICE_SCALE);
                let ask = Decimal::from(event.book.best_ask_price) / Decimal::from(PRICE_SCALE);
                let mid = (bid + ask) / Decimal::TWO;

                let state = states
                    .entry(rule.token.clone())
                    .or_insert_with(|| RequoteState {
                        last_mid: None,
                        active_orders: None,
                    });

                if state.last_mid == Some(mid) {
                    continue;
                }

                let cancelled_order_ids: Arc<[String]> = state
                    .active_orders
                    .take()
                    .map(|orders| vec![orders.buy_order_id, orders.sell_order_id])
                    .unwrap_or_default()
                    .into();

                let buy_price = mid - rule.lower_offset;
                let sell_price = mid + rule.upper_offset;
                if buy_price <= Decimal::ZERO || sell_price <= Decimal::ZERO {
                    warn!(token = %rule.token, mid = %mid, buy_price = %buy_price, sell_price = %sell_price, "mid_requote 计算出的挂单价格无效");
                    state.last_mid = Some(mid);
                    continue;
                }

                let base = format!("{}-{}", rule.token, event.book.timestamp_ms);
                let buy_order_id = format!("{}-buy", base);
                let sell_order_id = format!("{}-sell", base);
                let new_order_ids: Arc<[String]> =
                    vec![buy_order_id.clone(), sell_order_id.clone()].into();

                state.last_mid = Some(mid);
                state.active_orders = Some(ActiveOrders {
                    buy_order_id,
                    sell_order_id,
                });

                if let Err(err) = order_tx.try_send(OrderSignal::MidRequote {
                    topic: rule.topic.clone(),
                    token: rule.token.clone(),
                    mid,
                    buy_price,
                    sell_price,
                    cancelled_order_ids,
                    new_order_ids,
                }) {
                    warn!(token = %rule.token, error = %err, "mid_requote 发送模拟挂单事件失败");
                }
            }
        })
    }
}

fn resolve_csv_path(csv_file: &str) -> String {
    let csv_path = Path::new(csv_file);
    if csv_path.is_absolute() {
        csv_file.to_string()
    } else if let Ok(mut exe_path) = std::env::current_exe() {
        exe_path.pop();
        exe_path.push(csv_file);
        exe_path.to_string_lossy().to_string()
    } else {
        csv_file.to_string()
    }
}
