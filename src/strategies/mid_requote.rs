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
    pub target_order_size: Decimal,
    pub reward_min_orders: Option<u32>,
    pub reward_max_spread_cents: Option<f64>,
    pub reward_min_size: Option<f64>,
    pub reward_daily_pool: Option<f64>,
}

#[derive(Debug, Clone)]
struct ActiveOrder {
    order_id: String,
    order_size: Decimal,
}

#[derive(Debug, Clone)]
struct PendingReplacement {
    order_id: String,
    price: Decimal,
    order_size: Decimal,
    mid: Decimal,
}

#[derive(Debug, Clone, Default)]
struct SideQuoteState {
    last_quoted_mid: Option<Decimal>,
    active_order: Option<ActiveOrder>,
    pending_replacement: Option<PendingReplacement>,
    cancel_requested: bool,
}

#[derive(Debug, Clone)]
struct RequoteState {
    topic: Arc<str>,
    last_mid: Option<Decimal>,
    last_best_bid: Option<Decimal>,
    last_best_ask: Option<Decimal>,
    last_position_size: Decimal,
    buy: SideQuoteState,
    sell: SideQuoteState,
}

#[derive(Debug, Clone, Default)]
pub struct MidRequoteRestoreSideState {
    pub active_local_order_id: Option<String>,
    pub active_order_size: Option<Decimal>,
    pub pending_local_order_id: Option<String>,
    pub pending_price: Option<Decimal>,
    pub pending_order_size: Option<Decimal>,
    pub pending_mid: Option<Decimal>,
    pub last_quoted_mid: Option<Decimal>,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone)]
pub struct MidRequoteRestoreState {
    pub topic: Arc<str>,
    pub buy: MidRequoteRestoreSideState,
    pub sell: MidRequoteRestoreSideState,
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
    pub fn rules(&self) -> impl Iterator<Item = (&String, &MidRequoteRule)> {
        self.rules.iter()
    }

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
            .has_headers(true)
            .from_path(&csv_path)
            .map_err(|e| anyhow::anyhow!("无法打开 {}: {}", csv_path, e))?;

        let mut rules = Vec::new();
        for result in reader.records() {
            let record = result?;
            if record.len() < 4 {
                continue;
            }

            let token = record[0].trim();
            let offset = record[1].trim();
            let mid_change_threshold = record[2].trim();
            let target_order_size = record[3].trim();
            if token.is_empty()
                || offset.is_empty()
                || mid_change_threshold.is_empty()
                || target_order_size.is_empty()
            {
                continue;
            }

            let offset = Decimal::try_from(offset.parse::<f64>()?)?;
            let mid_change_threshold = Decimal::try_from(mid_change_threshold.parse::<f64>()?)?;
            let target_order_size = Decimal::try_from(target_order_size.parse::<f64>()?)?;
            let topic: Arc<str> = if record.len() >= 5 && !record[4].trim().is_empty() {
                Arc::from(record[4].trim())
            } else {
                Arc::from(DEFAULT_TOPIC)
            };

            let reward_min_orders = record.get(5).and_then(|v| v.trim().parse::<u32>().ok());
            let reward_max_spread_cents = record.get(6).and_then(|v| v.trim().parse::<f64>().ok());
            let reward_min_size = record.get(7).and_then(|v| v.trim().parse::<f64>().ok());
            let reward_daily_pool = record.get(8).and_then(|v| v.trim().parse::<f64>().ok());

            rules.push(MidRequoteRule {
                topic,
                token: token.to_string(),
                offset,
                mid_change_threshold,
                target_order_size,
                reward_min_orders,
                reward_max_spread_cents,
                reward_min_size,
                reward_daily_pool,
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
                .map(|(token, restored)| (token, state_from_restore(restored)))
                .collect();

            while let Some(event) = rx.recv().await {
                match event {
                    StrategyEvent::Market(event) => {
                        let Some(rule) = rules.get(event.asset_id.as_ref()) else {
                            continue;
                        };

                        let bid =
                            Decimal::from(event.book.best_bid_price) / Decimal::from(PRICE_SCALE);
                        let ask =
                            Decimal::from(event.book.best_ask_price) / Decimal::from(PRICE_SCALE);
                        let mid = (bid + ask) / Decimal::TWO;

                        let state = states
                            .entry(rule.token.clone())
                            .or_insert_with(|| empty_state(rule));
                        let is_first_snapshot = state.last_mid.is_none();
                        let prev_mid = state.last_mid;
                        let mid_changed = prev_mid != Some(mid);
                        let buy_mid_delta = quoted_mid_delta(&state.buy, mid);
                        let sell_mid_delta = quoted_mid_delta(&state.sell, mid);

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
                                buy_last_quoted_mid = ?state.buy.last_quoted_mid,
                                sell_last_quoted_mid = ?state.sell.last_quoted_mid,
                                buy_mid_delta = ?buy_mid_delta,
                                sell_mid_delta = ?sell_mid_delta,
                                mid_change_threshold = %rule.mid_change_threshold,
                                buy_will_requote = side_needs_quote(&state.buy, mid, rule.mid_change_threshold),
                                sell_will_requote = state.last_position_size > Decimal::ZERO
                                    && side_needs_quote(&state.sell, mid, rule.mid_change_threshold),
                                ts = event.book.timestamp_ms,
                                "mid_requote mid 发生变化"
                            );
                        }

                        let buy_target = buy_target_size(rule, state.last_position_size);
                        if buy_target > Decimal::ZERO {
                            if side_needs_quote(&state.buy, mid, rule.mid_change_threshold) {
                                if let Err(err) = submit_side_quote(
                                    rule,
                                    state,
                                    QuoteSide::Buy,
                                    bid,
                                    ask,
                                    mid,
                                    buy_target,
                                    event.book.timestamp_ms,
                                    &order_tx,
                                ) {
                                    warn!(token = %rule.token, error = %err, "mid_requote 发送买侧挂单事件失败");
                                }
                            }
                        } else if let Err(err) =
                            cancel_side_quote(rule, state, QuoteSide::Buy, &order_tx)
                        {
                            warn!(token = %rule.token, error = %err, "mid_requote 发送买侧撤单事件失败");
                        }

                        if state.last_position_size > Decimal::ZERO {
                            if side_needs_quote(&state.sell, mid, rule.mid_change_threshold) {
                                if let Err(err) = submit_side_quote(
                                    rule,
                                    state,
                                    QuoteSide::Sell,
                                    bid,
                                    ask,
                                    mid,
                                    state.last_position_size,
                                    event.book.timestamp_ms,
                                    &order_tx,
                                ) {
                                    warn!(token = %rule.token, error = %err, "mid_requote 发送卖侧挂单事件失败");
                                }
                            }
                        } else if let Err(err) =
                            cancel_side_quote(rule, state, QuoteSide::Sell, &order_tx)
                        {
                            warn!(token = %rule.token, error = %err, "mid_requote 发送卖侧撤单事件失败");
                        }

                        persist_state(order_store.as_ref(), &rule.token, state);
                    }
                    StrategyEvent::OrderStatus(status_event) => {
                        let Some(state) = states.get_mut(status_event.token.as_str()) else {
                            continue;
                        };
                        let is_terminal = matches!(
                            status_event.status.as_ref(),
                            "canceled" | "filled" | "rejected" | "failed"
                        );
                        if !is_terminal {
                            continue;
                        }
                        let Some(side) = active_order_side(state, &status_event.local_order_id)
                        else {
                            continue;
                        };
                        let topic = state.topic.clone();
                        let lane = side_state_mut(state, side);
                        lane.cancel_requested = false;
                        let Some(pending) = lane.pending_replacement.clone() else {
                            lane.active_order = None;
                            persist_state(order_store.as_ref(), &status_event.token, state);
                            continue;
                        };
                        if side == QuoteSide::Sell && status_event.status.as_ref() == "filled" {
                            lane.active_order = None;
                            lane.pending_replacement = None;
                            persist_state(order_store.as_ref(), &status_event.token, state);
                            continue;
                        }

                        if let Err(err) = order_tx.try_send(OrderSignal::MidRequotePlace {
                            strategy: Arc::from("mid_requote"),
                            topic,
                            token: status_event.token.clone(),
                            mid: pending.mid,
                            side,
                            price: pending.price,
                            order_size: pending.order_size,
                            local_order_id: pending.order_id.clone(),
                        }) {
                            warn!(token = %status_event.token, side = ?side, error = %err, "mid_requote 收到撤单确认后发送 replacement 失败");
                            persist_state(order_store.as_ref(), &status_event.token, state);
                            continue;
                        }
                        lane.active_order = Some(ActiveOrder {
                            order_id: pending.order_id,
                            order_size: pending.order_size,
                        });
                        lane.last_quoted_mid = Some(pending.mid);
                        lane.pending_replacement = None;
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
                            .filter(|(_, state)| has_unblocked_pending(state))
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
                                .or_insert_with(|| empty_state(rule));

                            state.topic = rule.topic.clone();

                            let new_position_size = update
                                .snapshot
                                .by_asset
                                .get(token.as_str())
                                .map(|position| position.size)
                                .unwrap_or(Decimal::ZERO);
                            let old_position_size = state.last_position_size;
                            state.last_position_size = new_position_size;

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

                            promote_unblocked_pending(rule, state, QuoteSide::Buy, &order_tx);

                            let buy_target = buy_target_size(rule, new_position_size);
                            if buy_target > Decimal::ZERO {
                                let buy_target_changed =
                                    side_target_size(&state.buy) != Some(buy_target);
                                if side_is_empty(&state.buy) || buy_target_changed {
                                    if let Err(err) = submit_side_quote(
                                        rule,
                                        state,
                                        QuoteSide::Buy,
                                        best_bid,
                                        best_ask,
                                        mid,
                                        buy_target,
                                        0,
                                        &order_tx,
                                    ) {
                                        warn!(token = %rule.token, error = %err, "mid_requote 仓位刷新后同步买侧失败");
                                    }
                                }
                            } else if let Err(err) =
                                cancel_side_quote(rule, state, QuoteSide::Buy, &order_tx)
                            {
                                warn!(token = %rule.token, error = %err, "mid_requote 仓位达到目标后撤买侧失败");
                            }

                            if new_position_size > Decimal::ZERO {
                                promote_unblocked_pending(rule, state, QuoteSide::Sell, &order_tx);
                                let sell_target_changed =
                                    side_target_size(&state.sell) != Some(new_position_size);
                                if side_is_empty(&state.sell) || sell_target_changed {
                                    if let Err(err) = submit_side_quote(
                                        rule,
                                        state,
                                        QuoteSide::Sell,
                                        best_bid,
                                        best_ask,
                                        mid,
                                        new_position_size,
                                        0,
                                        &order_tx,
                                    ) {
                                        warn!(token = %rule.token, error = %err, "mid_requote 仓位刷新后同步卖侧失败");
                                    }
                                }
                            } else if let Err(err) =
                                cancel_side_quote(rule, state, QuoteSide::Sell, &order_tx)
                            {
                                warn!(token = %rule.token, error = %err, "mid_requote 仓位清零后撤卖侧失败");
                            }

                            info!(
                                target = "order",
                                token = %rule.token,
                                topic = %rule.topic,
                                old_position_size = %old_position_size,
                                new_position_size = %new_position_size,
                                buy_active = state.buy.active_order.as_ref().map(|order| order.order_id.as_str()),
                                sell_active = state.sell.active_order.as_ref().map(|order| order.order_id.as_str()),
                                sell_pending = state.sell.pending_replacement.as_ref().map(|pending| pending.order_id.as_str()),
                                best_bid = %best_bid,
                                best_ask = %best_ask,
                                mid = %mid,
                                "mid_requote 根据仓位同步双边库存报价"
                            );

                            persist_state(order_store.as_ref(), &rule.token, state);
                        }
                    }
                }
            }
        })
    }
}

fn state_from_restore(restored: MidRequoteRestoreState) -> RequoteState {
    RequoteState {
        topic: restored.topic,
        last_mid: None,
        last_best_bid: None,
        last_best_ask: None,
        last_position_size: restored.last_position_size,
        buy: side_from_restore(restored.buy),
        sell: side_from_restore(restored.sell),
    }
}

fn side_from_restore(restored: MidRequoteRestoreSideState) -> SideQuoteState {
    SideQuoteState {
        last_quoted_mid: restored.last_quoted_mid,
        active_order: restored.active_local_order_id.and_then(|order_id| {
            restored.active_order_size.map(|order_size| ActiveOrder {
                order_id,
                order_size,
            })
        }),
        pending_replacement: match (
            restored.pending_local_order_id,
            restored.pending_price,
            restored.pending_order_size,
            restored.pending_mid,
        ) {
            (Some(order_id), Some(price), Some(order_size), Some(mid)) => {
                Some(PendingReplacement {
                    order_id,
                    price,
                    order_size,
                    mid,
                })
            }
            _ => None,
        },
        cancel_requested: restored.cancel_requested,
    }
}

fn empty_state(rule: &MidRequoteRule) -> RequoteState {
    RequoteState {
        topic: rule.topic.clone(),
        last_mid: None,
        last_best_bid: None,
        last_best_ask: None,
        last_position_size: Decimal::ZERO,
        buy: SideQuoteState::default(),
        sell: SideQuoteState::default(),
    }
}

fn side_state_mut(state: &mut RequoteState, side: QuoteSide) -> &mut SideQuoteState {
    match side {
        QuoteSide::Buy => &mut state.buy,
        QuoteSide::Sell => &mut state.sell,
    }
}

fn buy_target_size(rule: &MidRequoteRule, position_size: Decimal) -> Decimal {
    rule.target_order_size - position_size
}

fn quoted_mid_delta(side_state: &SideQuoteState, mid: Decimal) -> Option<Decimal> {
    side_state
        .last_quoted_mid
        .map(|quoted_mid| (mid - quoted_mid).abs())
}

fn side_needs_quote(side_state: &SideQuoteState, mid: Decimal, threshold: Decimal) -> bool {
    side_is_empty(side_state)
        || quoted_mid_delta(side_state, mid).is_some_and(|delta| delta >= threshold)
}

fn side_is_empty(side_state: &SideQuoteState) -> bool {
    side_state.active_order.is_none() && side_state.pending_replacement.is_none()
}

fn side_target_size(side_state: &SideQuoteState) -> Option<Decimal> {
    side_state
        .pending_replacement
        .as_ref()
        .map(|pending| pending.order_size)
        .or_else(|| {
            side_state
                .active_order
                .as_ref()
                .map(|order| order.order_size)
        })
}

fn active_order_side(state: &RequoteState, local_order_id: &str) -> Option<QuoteSide> {
    if state
        .buy
        .active_order
        .as_ref()
        .is_some_and(|order| order.order_id == local_order_id)
    {
        Some(QuoteSide::Buy)
    } else if state
        .sell
        .active_order
        .as_ref()
        .is_some_and(|order| order.order_id == local_order_id)
    {
        Some(QuoteSide::Sell)
    } else {
        None
    }
}

fn has_unblocked_pending(state: &RequoteState) -> bool {
    (state.buy.active_order.is_none() && state.buy.pending_replacement.is_some())
        || (state.sell.active_order.is_none() && state.sell.pending_replacement.is_some())
}

fn promote_unblocked_pending(
    rule: &MidRequoteRule,
    state: &mut RequoteState,
    side: QuoteSide,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
) {
    let topic = state.topic.clone();
    let lane = side_state_mut(state, side);
    if lane.active_order.is_some() {
        return;
    }
    let Some(pending) = lane.pending_replacement.clone() else {
        return;
    };
    if let Err(err) = order_tx.try_send(OrderSignal::MidRequotePlace {
        strategy: Arc::from("mid_requote"),
        topic,
        token: rule.token.clone(),
        mid: pending.mid,
        side,
        price: pending.price,
        order_size: pending.order_size,
        local_order_id: pending.order_id.clone(),
    }) {
        warn!(token = %rule.token, side = ?side, error = %err, "mid_requote 恢复 pending replacement 时发送挂单失败");
        return;
    }
    lane.active_order = Some(ActiveOrder {
        order_id: pending.order_id,
        order_size: pending.order_size,
    });
    lane.last_quoted_mid = Some(pending.mid);
    lane.pending_replacement = None;
    lane.cancel_requested = false;
}

fn submit_side_quote(
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
    let topic = state.topic.clone();
    let lane = side_state_mut(state, side);

    if let Some(active_order) = lane.active_order.as_ref() {
        let request_cancel = !lane.cancel_requested;
        lane.pending_replacement = Some(PendingReplacement {
            order_id: order_id.clone(),
            price,
            order_size,
            mid,
        });
        lane.last_quoted_mid = Some(mid);
        if request_cancel {
            lane.cancel_requested = true;
        }
        return order_tx.try_send(OrderSignal::MidRequoteStageReplacement {
            strategy: Arc::from("mid_requote"),
            topic,
            token: rule.token.clone(),
            mid,
            side,
            price,
            order_size,
            active_local_order_id: active_order.order_id.clone(),
            pending_local_order_id: order_id,
            request_cancel,
        });
    }

    lane.active_order = Some(ActiveOrder {
        order_id: order_id.clone(),
        order_size,
    });
    lane.last_quoted_mid = Some(mid);
    lane.cancel_requested = false;

    order_tx.try_send(OrderSignal::MidRequotePlace {
        strategy: Arc::from("mid_requote"),
        topic,
        token: rule.token.clone(),
        mid,
        side,
        price,
        order_size,
        local_order_id: order_id,
    })
}

fn cancel_side_quote(
    rule: &MidRequoteRule,
    state: &mut RequoteState,
    side: QuoteSide,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
) -> Result<(), tokio::sync::mpsc::error::TrySendError<OrderSignal>> {
    let topic = state.topic.clone();
    let lane = side_state_mut(state, side);
    lane.pending_replacement = None;
    let Some(active_order) = lane.active_order.as_ref() else {
        lane.cancel_requested = false;
        return Ok(());
    };
    if lane.cancel_requested {
        return Ok(());
    }
    lane.cancel_requested = true;
    order_tx.try_send(OrderSignal::MidRequoteCancel {
        strategy: Arc::from("mid_requote"),
        topic,
        token: rule.token.clone(),
        side,
        active_local_order_id: active_order.order_id.clone(),
    })
}

fn persist_state(order_store: Option<&OrderStore>, token: &str, state: &RequoteState) {
    let Some(order_store) = order_store else {
        return;
    };

    if let Err(error) = order_store.upsert_mid_requote_shared_state(
        token,
        state.topic.as_ref(),
        state.last_mid,
        state.last_best_bid,
        state.last_best_ask,
        state.last_position_size,
    ) {
        warn!(token = %token, error = %error, "mid_requote 持久化共享策略状态失败");
    }

    persist_side_state(order_store, token, QuoteSide::Buy, &state.buy);
    persist_side_state(order_store, token, QuoteSide::Sell, &state.sell);
}

fn persist_side_state(
    order_store: &OrderStore,
    token: &str,
    side: QuoteSide,
    state: &SideQuoteState,
) {
    if let Err(error) = order_store.upsert_mid_requote_side_state(
        token,
        side,
        state
            .active_order
            .as_ref()
            .map(|order| order.order_id.as_str()),
        state
            .pending_replacement
            .as_ref()
            .map(|pending| pending.order_id.as_str()),
        state
            .pending_replacement
            .as_ref()
            .map(|pending| pending.price),
        state
            .pending_replacement
            .as_ref()
            .map(|pending| pending.order_size),
        state
            .pending_replacement
            .as_ref()
            .map(|pending| pending.mid),
        state.last_quoted_mid,
        state.cancel_requested,
    ) {
        warn!(token = %token, side = ?side, error = %error, "mid_requote 持久化单侧策略状态失败");
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
