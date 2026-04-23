use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use polymarket_client_sdk::types::Decimal;
use tracing::{info, warn};

use crate::{
    storage::OrderStore,
    strategy::{
        OrderSignal, QuoteSide, Strategy, StrategyEvent, StrategyRegistration, TopicRegistration,
    },
};

const DEFAULT_TOPIC: &str = "mid";
const PRICE_SCALE: u32 = 10_000;
static ORDER_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct MidRequoteRule {
    pub topic: Arc<str>,
    pub token: String,
    pub offset: Decimal,
    pub min_order_size: Decimal,
}

#[derive(Debug, Clone)]
struct ActiveOrder {
    order_id: String,
    side: QuoteSide,
}

#[derive(Debug, Clone)]
struct RequoteState {
    topic: Arc<str>,
    last_mid: Option<Decimal>,
    last_best_bid: Option<Decimal>,
    last_best_ask: Option<Decimal>,
    last_position_size: Decimal,
    active_order: Option<ActiveOrder>,
}

#[derive(Debug, Clone)]
pub struct MidRequoteRestoreState {
    pub topic: Arc<str>,
    pub active_local_order_id: Option<String>,
    pub last_mid: Option<Decimal>,
    pub last_best_bid: Option<Decimal>,
    pub last_best_ask: Option<Decimal>,
    pub last_position_size: Decimal,
}

pub struct MidRequoteStrategy {
    rules: Arc<HashMap<String, MidRequoteRule>>,
    registration: Arc<StrategyRegistration>,
    restored_states: HashMap<String, MidRequoteRestoreState>,
    order_store: Option<OrderStore>,
}

impl MidRequoteStrategy {
    pub fn with_restore_state(
        mut self,
        restored_states: HashMap<String, MidRequoteRestoreState>,
        order_store: Option<OrderStore>,
    ) -> Self {
        self.restored_states = restored_states;
        self.order_store = order_store;
        self
    }

    pub fn from_csv(csv_file: &str) -> anyhow::Result<Option<Self>> {
        let csv_path = resolve_csv_path(csv_file);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_path(&csv_path)
            .map_err(|e| anyhow::anyhow!("无法打开 {}: {}", csv_path, e))?;

        let mut rules = Vec::new();
        for result in reader.records() {
            let record = result?;
            if record.len() < 3 {
                continue;
            }

            let token = record[0].trim();
            let offset = record[1].trim();
            let min_order_size = record[2].trim();
            if token.is_empty() || offset.is_empty() || min_order_size.is_empty() {
                continue;
            }

            let offset = Decimal::try_from(offset.parse::<f64>()?)?;
            let min_order_size = Decimal::try_from(min_order_size.parse::<f64>()?)?;
            let topic: Arc<str> = if record.len() >= 4 && !record[3].trim().is_empty() {
                Arc::from(record[3].trim())
            } else {
                Arc::from(DEFAULT_TOPIC)
            };

            rules.push(MidRequoteRule {
                topic,
                token: token.to_string(),
                offset,
                min_order_size,
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
            restored_states: HashMap::new(),
            order_store: None,
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
        let rules = self.rules.clone();
        let related_tokens = self.registration.related_tokens.clone();
        let order_store = self.order_store.clone();
        let restored_states = self.restored_states;

        tokio::spawn(async move {
            let mut states: HashMap<String, RequoteState> = restored_states
                .into_iter()
                .map(|(token, restored)| {
                    (
                        token,
                        RequoteState {
                            topic: restored.topic,
                            last_mid: restored.last_mid,
                            last_best_bid: restored.last_best_bid,
                            last_best_ask: restored.last_best_ask,
                            last_position_size: restored.last_position_size,
                            active_order: restored.active_local_order_id.map(|order_id| ActiveOrder {
                                order_id,
                                side: QuoteSide::Buy,
                            }),
                        },
                    )
                })
                .collect();

            while let Some(event) = rx.recv().await {
                match event {
                    StrategyEvent::Market(event) => {
                        let Some(rule) = rules.get(event.asset_id.as_ref()) else {
                            continue;
                        };

                        let bid = Decimal::from(event.book.best_bid_price) / Decimal::from(PRICE_SCALE);
                        let ask = Decimal::from(event.book.best_ask_price) / Decimal::from(PRICE_SCALE);
                        let mid = (bid + ask) / Decimal::TWO;

                        let state = states
                            .entry(rule.token.clone())
                            .or_insert_with(|| RequoteState {
                                topic: rule.topic.clone(),
                                last_mid: None,
                                last_best_bid: None,
                                last_best_ask: None,
                                last_position_size: Decimal::ZERO,
                                active_order: None,
                            });
                        let is_first_snapshot = state.last_mid.is_none();
                        let mid_changed = state.last_mid != Some(mid);

                        state.topic = rule.topic.clone();
                        state.last_best_bid = Some(bid);
                        state.last_best_ask = Some(ask);

                        if is_first_snapshot {
                            info!(
                                target = "order",
                                token = %rule.token,
                                topic = %rule.topic,
                                bid = %bid,
                                ask = %ask,
                                mid = %mid,
                                ts = event.book.timestamp_ms,
                                "mid_requote 首次收到行情"
                            );
                        } else if mid_changed {
                            info!(
                                target = "order",
                                token = %rule.token,
                                topic = %rule.topic,
                                bid = %bid,
                                ask = %ask,
                                mid = %mid,
                                prev_mid = ?state.last_mid,
                                ts = event.book.timestamp_ms,
                                "mid_requote mid 发生变化"
                            );
                        }

                        if !mid_changed {
                            persist_state(order_store.as_ref(), &rule.token, state);
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
                            bid,
                            ask,
                            mid,
                            event.book.timestamp_ms,
                            &order_tx,
                        ) {
                            warn!(token = %rule.token, error = %err, "mid_requote 发送模拟挂单事件失败");
                        }
                        persist_state(order_store.as_ref(), &rule.token, state);
                    }
                    StrategyEvent::Positions(update) => {
                        let changed_assets: Vec<String> = update
                            .changed_assets
                            .iter()
                            .filter(|asset| related_tokens.iter().any(|token| token == *asset))
                            .cloned()
                            .collect();
                        if changed_assets.is_empty() {
                            continue;
                        }

                        for token in changed_assets.iter() {
                            let Some(rule) = rules.get(token.as_str()) else {
                                continue;
                            };
                            let state = states
                                .entry(rule.token.clone())
                                .or_insert_with(|| RequoteState {
                                    topic: rule.topic.clone(),
                                    last_mid: None,
                                    last_best_bid: None,
                                    last_best_ask: None,
                                    last_position_size: Decimal::ZERO,
                                    active_order: None,
                                });

                            state.topic = rule.topic.clone();

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
                            let Some(best_bid) = state.last_best_bid else {
                                continue;
                            };
                            let Some(best_ask) = state.last_best_ask else {
                                continue;
                            };

                            info!(
                                target = "order",
                                token = %rule.token,
                                topic = %rule.topic,
                                old_position_size = %old_position_size,
                                new_position_size = %new_position_size,
                                next_side = ?side,
                                best_bid = %best_bid,
                                best_ask = %best_ask,
                                mid = %mid,
                                "mid_requote 根据仓位变化切换挂单方向"
                            );

                            if let Err(err) = submit_quote(
                                rule,
                                state,
                                side,
                                best_bid,
                                best_ask,
                                mid,
                                0,
                                &order_tx,
                            ) {
                                warn!(token = %rule.token, error = %err, "mid_requote 仓位变化后发送反手挂单失败");
                            }
                            persist_state(order_store.as_ref(), &rule.token, state);
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
    best_bid: Decimal,
    best_ask: Decimal,
    mid: Decimal,
    ts: u64,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
) -> Result<(), tokio::sync::mpsc::error::TrySendError<OrderSignal>> {
    let price = match side {
        QuoteSide::Buy => best_bid - rule.offset,
        QuoteSide::Sell => best_ask + rule.offset,
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
    let seq = ORDER_SEQ.fetch_add(1, Ordering::Relaxed);
    let order_id = format!("{}-{}-{}-{}", rule.token, ts, order_side, seq);
    let new_order_ids: Arc<[String]> = vec![order_id.clone()].into();

    state.active_order = Some(ActiveOrder { order_id, side });

    order_tx.try_send(OrderSignal::MidRequote {
        topic: rule.topic.clone(),
        token: rule.token.clone(),
        mid,
        side,
        price,
        min_order_size: rule.min_order_size,
        cancelled_order_ids,
        new_order_ids,
    })
}

fn persist_state(order_store: Option<&OrderStore>, token: &str, state: &RequoteState) {
    let Some(order_store) = order_store else {
        return;
    };

    if let Err(error) = order_store.upsert_mid_requote_state(
        token,
        state.topic.as_ref(),
        state.active_order.as_ref().map(|order| order.order_id.as_str()),
        state.last_mid,
        state.last_best_bid,
        state.last_best_ask,
        state.last_position_size,
    ) {
        warn!(token = %token, error = %error, "mid_requote 持久化策略状态失败");
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
