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
    pub mid_change_threshold: Decimal,
    pub min_order_size: Decimal,
}

#[derive(Debug, Clone)]
struct ActiveOrder {
    order_id: String,
    side: QuoteSide,
}

#[derive(Debug, Clone)]
struct PendingReplacement {
    order_id: String,
    side: QuoteSide,
    price: Decimal,
    order_size: Decimal,
    mid: Decimal,
}

#[derive(Debug, Clone)]
struct RequoteState {
    topic: Arc<str>,
    last_mid: Option<Decimal>,
    last_quoted_mid: Option<Decimal>,
    last_best_bid: Option<Decimal>,
    last_best_ask: Option<Decimal>,
    last_position_size: Decimal,
    active_order: Option<ActiveOrder>,
    pending_replacement: Option<PendingReplacement>,
}

#[derive(Debug, Clone)]
pub struct MidRequoteRestoreState {
    pub topic: Arc<str>,
    pub active_local_order_id: Option<String>,
    pub active_side: Option<QuoteSide>,
    pub pending_local_order_id: Option<String>,
    pub pending_side: Option<QuoteSide>,
    pub pending_price: Option<Decimal>,
    pub pending_order_size: Option<Decimal>,
    pub pending_mid: Option<Decimal>,
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
            let mid_change_threshold = record[2].trim();
            let min_order_size = record[3].trim();
            if token.is_empty()
                || offset.is_empty()
                || mid_change_threshold.is_empty()
                || min_order_size.is_empty()
            {
                continue;
            }

            let offset = Decimal::try_from(offset.parse::<f64>()?)?;
            let mid_change_threshold = Decimal::try_from(mid_change_threshold.parse::<f64>()?)?;
            let min_order_size = Decimal::try_from(min_order_size.parse::<f64>()?)?;
            let topic: Arc<str> = if record.len() >= 5 && !record[4].trim().is_empty() {
                Arc::from(record[4].trim())
            } else {
                Arc::from(DEFAULT_TOPIC)
            };

            rules.push(MidRequoteRule {
                topic,
                token: token.to_string(),
                offset,
                mid_change_threshold,
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
                            last_quoted_mid: restored.last_mid,
                            last_best_bid: restored.last_best_bid,
                            last_best_ask: restored.last_best_ask,
                            last_position_size: restored.last_position_size,
                            active_order: restored.active_local_order_id.and_then(|order_id| {
                                restored.active_side.map(|side| ActiveOrder { order_id, side })
                            }),
                            pending_replacement: match (
                                restored.pending_local_order_id,
                                restored.pending_side,
                                restored.pending_price,
                                restored.pending_order_size,
                                restored.pending_mid,
                            ) {
                                (
                                    Some(order_id),
                                    Some(side),
                                    Some(price),
                                    Some(order_size),
                                    Some(mid),
                                ) => Some(PendingReplacement {
                                    order_id,
                                    side,
                                    price,
                                    order_size,
                                    mid,
                                }),
                                _ => None,
                            },
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
                                last_quoted_mid: None,
                                last_best_bid: None,
                                last_best_ask: None,
                                last_position_size: Decimal::ZERO,
                                active_order: None,
                                pending_replacement: None,
                            });
                        let is_first_snapshot = state.last_mid.is_none();
                        let prev_mid = state.last_mid;
                        let mid_changed = prev_mid != Some(mid);
                        let quoted_mid_delta = state
                            .last_quoted_mid
                            .map(|quoted_mid| (mid - quoted_mid).abs());
                        let should_requote = state.last_quoted_mid.is_none()
                            || quoted_mid_delta.is_some_and(|delta| delta >= rule.mid_change_threshold);

                        state.topic = rule.topic.clone();
                        state.last_best_bid = Some(bid);
                        state.last_best_ask = Some(ask);
                        state.last_mid = Some(mid);

                        if is_first_snapshot {
                            info!(
                                target = "order",
                                token = %rule.token,
                                topic = %rule.topic,
                                bid = %bid,
                                ask = %ask,
                                mid = %mid,
                                mid_change_threshold = %rule.mid_change_threshold,
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
                                prev_mid = ?prev_mid,
                                last_quoted_mid = ?state.last_quoted_mid,
                                quoted_mid_delta = ?quoted_mid_delta,
                                mid_change_threshold = %rule.mid_change_threshold,
                                will_requote = should_requote,
                                ts = event.book.timestamp_ms,
                                "mid_requote mid 发生变化"
                            );
                        }

                        if !mid_changed || !should_requote {
                            persist_state(order_store.as_ref(), &rule.token, state);
                            continue;
                        }

                        state.last_quoted_mid = Some(mid);

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
                            rule.min_order_size,
                            event.book.timestamp_ms,
                            &order_tx,
                        ) {
                            warn!(token = %rule.token, error = %err, "mid_requote 发送模拟挂单事件失败");
                        }
                        persist_state(order_store.as_ref(), &rule.token, state);
                    }
                    StrategyEvent::OrderStatus(status_event) => {
                        let Some(state) = states.get_mut(status_event.token.as_str()) else {
                            continue;
                        };
                        let is_terminal = matches!(status_event.status.as_ref(), "canceled" | "filled" | "rejected");
                        if !is_terminal {
                            continue;
                        }
                        let Some(active_order) = state.active_order.as_ref() else {
                            continue;
                        };
                        if active_order.order_id != status_event.local_order_id {
                            continue;
                        }
                        let Some(pending) = state.pending_replacement.clone() else {
                            state.active_order = None;
                            persist_state(order_store.as_ref(), &status_event.token, state);
                            continue;
                        };
                        if let Err(err) = order_tx.try_send(OrderSignal::MidRequotePlace {
                            strategy: Arc::from("mid_requote"),
                            topic: state.topic.clone(),
                            token: status_event.token.clone(),
                            mid: pending.mid,
                            side: pending.side,
                            price: pending.price,
                            order_size: pending.order_size,
                            local_order_id: pending.order_id.clone(),
                        }) {
                            warn!(token = %status_event.token, error = %err, "mid_requote 收到撤单确认后发送 replacement 失败");
                            persist_state(order_store.as_ref(), &status_event.token, state);
                            continue;
                        }
                        state.active_order = Some(ActiveOrder {
                            order_id: pending.order_id,
                            side: pending.side,
                        });
                        state.pending_replacement = None;
                        persist_state(order_store.as_ref(), &status_event.token, state);
                    }
                    StrategyEvent::Positions(update) => {
                        let changed_assets: Vec<String> = update
                            .changed_assets
                            .iter()
                            .filter(|asset| related_tokens.iter().any(|token| token == *asset))
                            .cloned()
                            .collect();

                        let mut restored_pending_tokens: Vec<String> = states
                            .iter()
                            .filter(|(_, state)| {
                                state.active_order.is_none() && state.pending_replacement.is_some()
                            })
                            .map(|(token, _)| token.clone())
                            .collect();
                        restored_pending_tokens.sort();
                        restored_pending_tokens.dedup();

                        let mut tokens_to_process = changed_assets;
                        tokens_to_process.extend(restored_pending_tokens);
                        tokens_to_process.sort();
                        tokens_to_process.dedup();
                        if tokens_to_process.is_empty() {
                            continue;
                        }

                        for token in tokens_to_process.iter() {
                            let Some(rule) = rules.get(token.as_str()) else {
                                continue;
                            };
                            let state = states
                                .entry(rule.token.clone())
                                .or_insert_with(|| RequoteState {
                                    topic: rule.topic.clone(),
                                    last_mid: None,
                                    last_quoted_mid: None,
                                    last_best_bid: None,
                                    last_best_ask: None,
                                    last_position_size: Decimal::ZERO,
                                    active_order: None,
                                    pending_replacement: None,
                                });

                            state.topic = rule.topic.clone();

                            if state.active_order.is_none() {
                                if let Some(pending) = state.pending_replacement.clone() {
                                    if let Err(err) = order_tx.try_send(OrderSignal::MidRequotePlace {
                                        strategy: Arc::from("mid_requote"),
                                        topic: state.topic.clone(),
                                        token: token.clone(),
                                        mid: pending.mid,
                                        side: pending.side,
                                        price: pending.price,
                                        order_size: pending.order_size,
                                        local_order_id: pending.order_id.clone(),
                                    }) {
                                        warn!(token = %rule.token, error = %err, "mid_requote 恢复 pending replacement 时发送挂单失败");
                                        persist_state(order_store.as_ref(), &rule.token, state);
                                        continue;
                                    }
                                    state.active_order = Some(ActiveOrder {
                                        order_id: pending.order_id,
                                        side: pending.side,
                                    });
                                    state.pending_replacement = None;
                                    persist_state(order_store.as_ref(), &rule.token, state);
                                    continue;
                                }
                            }

                            let new_position_size = update
                                .snapshot
                                .by_asset
                                .get(token.as_str())
                                .map(|position| position.size)
                                .unwrap_or(Decimal::ZERO);
                            let old_position_size = state.last_position_size;
                            state.last_position_size = new_position_size;

                            let position_delta = (new_position_size - old_position_size).abs();
                            let side = if new_position_size > old_position_size {
                                QuoteSide::Sell
                            } else if new_position_size < old_position_size {
                                QuoteSide::Buy
                            } else {
                                persist_state(order_store.as_ref(), &rule.token, state);
                                continue;
                            };

                            let Some(mid) = state.last_mid else {
                                persist_state(order_store.as_ref(), &rule.token, state);
                                continue;
                            };
                            let Some(best_bid) = state.last_best_bid else {
                                persist_state(order_store.as_ref(), &rule.token, state);
                                continue;
                            };
                            let Some(best_ask) = state.last_best_ask else {
                                persist_state(order_store.as_ref(), &rule.token, state);
                                continue;
                            };

                            info!(
                                target = "order",
                                token = %rule.token,
                                topic = %rule.topic,
                                old_position_size = %old_position_size,
                                new_position_size = %new_position_size,
                                position_delta = %position_delta,
                                next_side = ?side,
                                best_bid = %best_bid,
                                best_ask = %best_ask,
                                mid = %mid,
                                "mid_requote 根据仓位变化切换挂单方向"
                            );

                            if position_delta.is_zero() {
                                persist_state(order_store.as_ref(), &rule.token, state);
                                continue;
                            }

                            state.last_quoted_mid = Some(mid);

                            if let Err(err) = submit_quote(
                                rule,
                                state,
                                side,
                                best_bid,
                                best_ask,
                                mid,
                                position_delta,
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
    order_size: Decimal,
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
    if order_size <= Decimal::ZERO {
        warn!(token = %rule.token, side = ?side, mid = %mid, order_size = %order_size, "mid_requote 计算出的挂单数量无效");
        return Ok(());
    }

    let order_side = match side {
        QuoteSide::Buy => "buy",
        QuoteSide::Sell => "sell",
    };
    let seq = ORDER_SEQ.fetch_add(1, Ordering::Relaxed);
    let order_id = format!("{}-{}-{}-{}", rule.token, ts, order_side, seq);

    if let Some(active_order) = state.active_order.as_ref() {
        state.pending_replacement = Some(PendingReplacement {
            order_id: order_id.clone(),
            side,
            price,
            order_size,
            mid,
        });
        return order_tx.try_send(OrderSignal::MidRequoteStageReplacement {
            strategy: Arc::from("mid_requote"),
            topic: rule.topic.clone(),
            token: rule.token.clone(),
            mid,
            side,
            price,
            order_size,
            active_local_order_id: active_order.order_id.clone(),
            pending_local_order_id: order_id,
            request_cancel: state.pending_replacement.is_some(),
        });
    }

    state.active_order = Some(ActiveOrder {
        order_id: order_id.clone(),
        side,
    });

    order_tx.try_send(OrderSignal::MidRequotePlace {
        strategy: Arc::from("mid_requote"),
        topic: rule.topic.clone(),
        token: rule.token.clone(),
        mid,
        side,
        price,
        order_size,
        local_order_id: order_id,
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
        state.pending_replacement.as_ref().map(|pending| pending.order_id.as_str()),
        state.pending_replacement.as_ref().map(|pending| pending.side),
        state.pending_replacement.as_ref().map(|pending| pending.price),
        state.pending_replacement.as_ref().map(|pending| pending.order_size),
        state.pending_replacement.as_ref().map(|pending| pending.mid),
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
    if csv_path.is_absolute() || csv_path.exists() {
        csv_file.to_string()
    } else if let Ok(mut exe_path) = std::env::current_exe() {
        exe_path.pop();
        exe_path.push(csv_file);
        exe_path.to_string_lossy().to_string()
    } else {
        csv_file.to_string()
    }
}
