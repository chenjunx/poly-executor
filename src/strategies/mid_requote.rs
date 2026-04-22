use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use polymarket_client_sdk::types::Decimal;
use tracing::{info, warn};

use crate::strategy::{
    OrderSignal, QuoteSide, Strategy, StrategyEvent, StrategyRegistration, TopicRegistration,
};

const DEFAULT_TOPIC: &str = "mid";
const PRICE_SCALE: u32 = 10_000;

#[derive(Debug, Clone)]
pub struct MidRequoteRule {
    pub topic: Arc<str>,
    pub token: String,
    pub buy_offset: Decimal,
    pub sell_offset: Decimal,
}

#[derive(Debug, Clone)]
struct ActiveOrder {
    order_id: String,
    side: QuoteSide,
}

#[derive(Debug, Clone)]
struct RequoteState {
    last_mid: Option<Decimal>,
    last_position_size: Decimal,
    active_order: Option<ActiveOrder>,
}

pub struct MidRequoteStrategy {
    rules: Arc<HashMap<String, MidRequoteRule>>,
    registration: Arc<StrategyRegistration>,
}

impl MidRequoteStrategy {
    pub fn from_csv(csv_file: &str) -> anyhow::Result<Option<Self>> {
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
            let buy_offset = record[1].trim();
            if token.is_empty() || buy_offset.is_empty() {
                continue;
            }

            let buy_offset = Decimal::try_from(buy_offset.parse::<f64>()?)?;
            let sell_offset = if record.len() >= 3 && !record[2].trim().is_empty() {
                Decimal::try_from(record[2].trim().parse::<f64>()?)?
            } else {
                buy_offset
            };
            let topic: Arc<str> = if record.len() >= 4 && !record[3].trim().is_empty() {
                Arc::from(record[3].trim())
            } else {
                Arc::from(DEFAULT_TOPIC)
            };

            rules.push(MidRequoteRule {
                topic,
                token: token.to_string(),
                buy_offset,
                sell_offset,
            });
        }

        Self::from_rules(rules)
    }

    pub fn from_rules(rules: Vec<MidRequoteRule>) -> anyhow::Result<Option<Self>> {
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

        let mut related_tokens: Vec<String> = rule_map.keys().cloned().collect();
        related_tokens.sort();

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

        let registration = Arc::new(StrategyRegistration {
            name: Arc::from("mid_requote"),
            topics: Arc::<[Arc<str>]>::from(topics),
            topic_tokens: Arc::<[TopicRegistration]>::from(topic_tokens),
            related_tokens: Arc::<[String]>::from(related_tokens),
        });

        Ok(Some(Self {
            rules: Arc::new(rule_map),
            registration,
        }))
    }
}

impl Strategy for MidRequoteStrategy {
    fn name(&self) -> &str {
        "mid_requote"
    }

    fn registration(&self) -> &StrategyRegistration {
        self.registration.as_ref()
    }

    fn spawn(
        self,
        mut rx: tokio::sync::mpsc::Receiver<StrategyEvent>,
        order_tx: tokio::sync::mpsc::Sender<OrderSignal>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut states: HashMap<String, RequoteState> = HashMap::new();

            while let Some(event) = rx.recv().await {
                match event {
                    StrategyEvent::Market(event) => {
                        let Some(rule) = self.rules.get(event.asset_id.as_ref()) else {
                            continue;
                        };

                        let bid = Decimal::from(event.book.best_bid_price) / Decimal::from(PRICE_SCALE);
                        let ask = Decimal::from(event.book.best_ask_price) / Decimal::from(PRICE_SCALE);
                        let mid = (bid + ask) / Decimal::TWO;

                        info!(
                            token = %rule.token,
                            topic = %rule.topic,
                            bid = %bid,
                            ask = %ask,
                            mid = %mid,
                            ts = event.book.timestamp_ms,
                            "mid_requote 行情快照"
                        );

                        let state = states
                            .entry(rule.token.clone())
                            .or_insert_with(|| RequoteState {
                                last_mid: None,
                                last_position_size: Decimal::ZERO,
                                active_order: None,
                            });

                        if state.last_mid == Some(mid) {
                            continue;
                        }

                        state.last_mid = Some(mid);

                        let side = state
                            .active_order
                            .as_ref()
                            .map(|order| order.side)
                            .unwrap_or(QuoteSide::Buy);
                        if let Err(err) = submit_quote(
                            rule,
                            state,
                            side,
                            mid,
                            event.book.timestamp_ms,
                            &order_tx,
                        ) {
                            warn!(token = %rule.token, error = %err, "mid_requote 发送模拟挂单事件失败");
                        }
                    }
                    StrategyEvent::Positions(update) => {
                        let Some(update) = self.relevant_positions(&update) else {
                            continue;
                        };

                        for token in update.changed_assets.iter() {
                            let Some(rule) = self.rules.get(token.as_str()) else {
                                continue;
                            };
                            let state = states
                                .entry(rule.token.clone())
                                .or_insert_with(|| RequoteState {
                                    last_mid: None,
                                    last_position_size: Decimal::ZERO,
                                    active_order: None,
                                });

                            let new_position_size = update
                                .snapshot
                                .by_asset
                                .get(token.as_str())
                                .map(|position| position.size)
                                .unwrap_or(Decimal::ZERO);
                            let old_position_size = state.last_position_size;
                            state.last_position_size = new_position_size;

                            let side = if new_position_size > old_position_size {
                                QuoteSide::Sell
                            } else if new_position_size < old_position_size {
                                QuoteSide::Buy
                            } else {
                                continue;
                            };

                            let Some(mid) = state.last_mid else {
                                continue;
                            };

                            if let Err(err) = submit_quote(
                                rule,
                                state,
                                side,
                                mid,
                                0,
                                &order_tx,
                            ) {
                                warn!(token = %rule.token, error = %err, "mid_requote 仓位变化后发送反手挂单失败");
                            }
                        }
                    }
                }
            }
        })
    }
}

fn submit_quote(
    rule: &MidRequoteRule,
    state: &mut RequoteState,
    side: QuoteSide,
    mid: Decimal,
    ts: u64,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
) -> Result<(), tokio::sync::mpsc::error::TrySendError<OrderSignal>> {
    let price = match side {
        QuoteSide::Buy => mid - rule.buy_offset,
        QuoteSide::Sell => mid + rule.sell_offset,
    };
    if price <= Decimal::ZERO {
        warn!(token = %rule.token, side = ?side, mid = %mid, price = %price, "mid_requote 计算出的挂单价格无效");
        return Ok(());
    }

    let cancelled_order_ids: Arc<[String]> = state
        .active_order
        .take()
        .map(|order| vec![order.order_id])
        .unwrap_or_default()
        .into();
    let order_side = match side {
        QuoteSide::Buy => "buy",
        QuoteSide::Sell => "sell",
    };
    let order_id = format!("{}-{}-{}", rule.token, ts, order_side);
    let new_order_ids: Arc<[String]> = vec![order_id.clone()].into();

    state.active_order = Some(ActiveOrder { order_id, side });

    order_tx.try_send(OrderSignal::MidRequote {
        topic: rule.topic.clone(),
        token: rule.token.clone(),
        mid,
        side,
        price,
        cancelled_order_ids,
        new_order_ids,
    })
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
