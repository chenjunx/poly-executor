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

const DEFAULT_TOPIC: &str = "liquidity_reward";
const PRICE_SCALE: u32 = 10_000;
static ORDER_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct LiquidityRewardRule {
    pub topic: Arc<str>,
    pub token1: String,
    pub token2: Option<String>,
    pub reward_min_orders: Option<u32>,
    pub reward_max_spread_cents: Option<f64>,
    pub reward_min_size: Option<f64>,
    pub reward_daily_pool: Option<f64>,
}

#[derive(Debug, Clone)]
struct ActiveOrder {
    order_id: String,
    price: Decimal,
    order_size: Decimal,
}

#[derive(Debug, Clone)]
struct PendingReplacement {
    order_id: String,
    price: Decimal,
    order_size: Decimal,
    mid: Decimal,
}

#[derive(Debug, Clone)]
struct TokenQuoteState {
    topic: Arc<str>,
    active_order: Option<ActiveOrder>,
    pending_replacement: Option<PendingReplacement>,
    cancel_requested: bool,
    last_mid: Option<Decimal>,
    last_best_bid: Option<Decimal>,
    last_best_ask: Option<Decimal>,
}

#[derive(Debug, Clone, Default)]
pub struct LiquidityRewardRestoreSideState {
    pub active_local_order_id: Option<String>,
    pub active_price: Option<Decimal>,
    pub active_order_size: Option<Decimal>,
    pub pending_local_order_id: Option<String>,
    pub pending_price: Option<Decimal>,
    pub pending_order_size: Option<Decimal>,
    pub pending_mid: Option<Decimal>,
    pub last_quoted_mid: Option<Decimal>,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone)]
pub struct LiquidityRewardRestoreState {
    pub topic: Arc<str>,
    pub buy: LiquidityRewardRestoreSideState,
    pub sell: LiquidityRewardRestoreSideState,
    pub last_mid: Option<Decimal>,
    pub last_best_bid: Option<Decimal>,
    pub last_best_ask: Option<Decimal>,
    pub last_position_size: Decimal,
}

pub struct LiquidityRewardStrategy {
    rules: Arc<HashMap<String, LiquidityRewardRule>>,
    registration: Arc<StrategyRegistration>,
    restored_states: HashMap<String, LiquidityRewardRestoreState>,
    order_store: Option<OrderStore>,
    simulation_enabled: bool,
}

impl LiquidityRewardStrategy {
    pub fn rules(&self) -> impl Iterator<Item = (&String, &LiquidityRewardRule)> {
        self.rules.iter()
    }

    pub fn with_restore_state(
        mut self,
        restored_states: HashMap<String, LiquidityRewardRestoreState>,
        order_store: Option<OrderStore>,
        simulation_enabled: bool,
    ) -> Self {
        self.restored_states = restored_states;
        self.order_store = order_store;
        self.simulation_enabled = simulation_enabled;
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
            if record.len() < 3 {
                continue;
            }

            let token1 = record[0].trim();
            let token2_raw = record[1].trim();
            if token1.is_empty() {
                continue;
            }

            let token2 = if token2_raw.is_empty() {
                None
            } else {
                Some(token2_raw.to_string())
            };
            let topic: Arc<str> = record
                .get(2)
                .filter(|s| !s.trim().is_empty())
                .map(|s| Arc::from(s.trim()))
                .unwrap_or_else(|| Arc::from(DEFAULT_TOPIC));

            let reward_min_orders = record.get(3).and_then(|v| v.trim().parse::<u32>().ok());
            let reward_max_spread_cents = record.get(4).and_then(|v| v.trim().parse::<f64>().ok());
            let reward_min_size = record.get(5).and_then(|v| v.trim().parse::<f64>().ok());
            let reward_daily_pool = record.get(6).and_then(|v| v.trim().parse::<f64>().ok());

            rules.push(LiquidityRewardRule {
                topic,
                token1: token1.to_string(),
                token2,
                reward_min_orders,
                reward_max_spread_cents,
                reward_min_size,
                reward_daily_pool,
            });
        }

        Self::from_rules(rules)
    }

    pub fn from_rules(rules: Vec<LiquidityRewardRule>) -> anyhow::Result<Option<Self>> {
        if rules.is_empty() {
            return Ok(None);
        }

        let mut rule_map: HashMap<String, LiquidityRewardRule> = HashMap::new();
        let mut topic_tokens: HashMap<Arc<str>, Vec<String>> = HashMap::new();

        for rule in rules {
            topic_tokens
                .entry(rule.topic.clone())
                .or_default()
                .push(rule.token1.clone());
            if let Some(t2) = &rule.token2 {
                topic_tokens
                    .entry(rule.topic.clone())
                    .or_default()
                    .push(t2.clone());
                rule_map.insert(t2.clone(), rule.clone());
            }
            rule_map.insert(rule.token1.clone(), rule);
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
            name: Arc::from("liquidity_reward"),
            topics: Arc::<[Arc<str>]>::from(topics),
            topic_tokens: Arc::<[TopicRegistration]>::from(topic_tokens),
            related_tokens: Arc::<[String]>::from(related_tokens),
        });

        Ok(Some(Self {
            rules: Arc::new(rule_map),
            registration,
            restored_states: HashMap::new(),
            order_store: None,
            simulation_enabled: false,
        }))
    }
}

impl Strategy for LiquidityRewardStrategy {
    fn name(&self) -> &str {
        "liquidity_reward"
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
        let order_store = self.order_store.clone();
        let restored_states = self.restored_states;
        let simulation_enabled = self.simulation_enabled;

        tokio::spawn(async move {
            let mut states: HashMap<String, TokenQuoteState> = restored_states
                .into_iter()
                .map(|(token, restored)| (token, state_from_restore(restored)))
                .collect();

            while let Some(event) = rx.recv().await {
                match event {
                    StrategyEvent::Market(event) => {
                        let Some(rule) = rules.get(event.asset_id.as_ref()) else {
                            continue;
                        };
                        let token = event.asset_id.as_ref().to_string();

                        let bid = Decimal::from(event.book.best_bid_price)
                            / Decimal::from(PRICE_SCALE);
                        let ask = Decimal::from(event.book.best_ask_price)
                            / Decimal::from(PRICE_SCALE);
                        let mid = (bid + ask) / Decimal::TWO;

                        let state = states
                            .entry(token.clone())
                            .or_insert_with(|| empty_state(rule));
                        state.topic = rule.topic.clone();
                        state.last_mid = Some(mid);
                        state.last_best_bid = Some(bid);
                        state.last_best_ask = Some(ask);

                        promote_pending_if_unblocked(&token, state, simulation_enabled, &order_tx);

                        let Some(order_size) = rule
                            .reward_min_size
                            .and_then(|s| Decimal::try_from(s).ok())
                            .filter(|s| *s > Decimal::ZERO)
                        else {
                            warn!(token = %token, "liquidity_reward reward_min_size 未配置或无效，跳过挂单");
                            persist_state(order_store.as_ref(), &token, state);
                            continue;
                        };

                        let spread = rule
                            .reward_max_spread_cents
                            .and_then(|c| Decimal::try_from(c / 100.0).ok())
                            .unwrap_or(Decimal::ZERO);

                        if needs_requote(state, mid, bid, spread) {
                            let reason = requote_reason(state, mid, spread);
                            info!(
                                target = "order",
                                token = %token,
                                topic = %rule.topic,
                                reason,
                                mid = %mid,
                                bid = %bid,
                                spread_cents = ?rule.reward_max_spread_cents,
                                active_price = ?state.active_order.as_ref().map(|o| o.price),
                                "liquidity_reward 触发报价"
                            );
                            if let Err(err) = submit_quote(
                                rule,
                                &token,
                                state,
                                mid,
                                order_size,
                                spread,
                                simulation_enabled,
                                event.book.timestamp_ms,
                                &order_tx,
                            ) {
                                warn!(token = %token, error = %err, "liquidity_reward 发送挂单事件失败");
                            }
                        }

                        persist_state(order_store.as_ref(), &token, state);
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

                        let is_active = state
                            .active_order
                            .as_ref()
                            .is_some_and(|o| o.order_id == status_event.local_order_id);
                        let is_pending = state
                            .pending_replacement
                            .as_ref()
                            .is_some_and(|p| p.order_id == status_event.local_order_id);

                        if !is_active && !is_pending {
                            continue;
                        }

                        let token = status_event.token.clone();

                        if is_pending {
                            state.pending_replacement = None;
                            state.cancel_requested = false;
                            persist_state(order_store.as_ref(), &token, state);
                            continue;
                        }

                        state.cancel_requested = false;
                        let Some(pending) = state.pending_replacement.clone() else {
                            state.active_order = None;
                            persist_state(order_store.as_ref(), &token, state);
                            continue;
                        };

                        let topic = state.topic.clone();
                        if let Err(err) = order_tx.try_send(OrderSignal::LiquidityRewardPlace {
                            strategy: Arc::from("liquidity_reward"),
                            topic,
                            token: token.clone(),
                            mid: pending.mid,
                            side: QuoteSide::Buy,
                            price: pending.price,
                            order_size: pending.order_size,
                            local_order_id: pending.order_id.clone(),
                            simulated: simulation_enabled,
                        }) {
                            warn!(token = %token, error = %err, "liquidity_reward 收到撤单确认后发送 replacement 失败");
                            persist_state(order_store.as_ref(), &token, state);
                            continue;
                        }
                        state.active_order = Some(ActiveOrder {
                            order_id: pending.order_id,
                            price: pending.price,
                            order_size: pending.order_size,
                        });
                        state.pending_replacement = None;
                        persist_state(order_store.as_ref(), &token, state);
                    }
                    StrategyEvent::OrderFill(_) | StrategyEvent::Positions(_) => {}
                }
            }
        })
    }
}

fn promote_pending_if_unblocked(
    token: &str,
    state: &mut TokenQuoteState,
    simulated: bool,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
) {
    if state.active_order.is_some() {
        return;
    }
    let Some(pending) = state.pending_replacement.clone() else {
        return;
    };
    let topic = state.topic.clone();
    if let Err(err) = order_tx.try_send(OrderSignal::LiquidityRewardPlace {
        strategy: Arc::from("liquidity_reward"),
        topic,
        token: token.to_string(),
        mid: pending.mid,
        side: QuoteSide::Buy,
        price: pending.price,
        order_size: pending.order_size,
        local_order_id: pending.order_id.clone(),
        simulated,
    }) {
        warn!(token = %token, error = %err, "liquidity_reward 恢复 pending 时发送挂单失败");
        return;
    }
    state.active_order = Some(ActiveOrder {
        order_id: pending.order_id,
        price: pending.price,
        order_size: pending.order_size,
    });
    state.pending_replacement = None;
    state.cancel_requested = false;
}

fn needs_requote(
    state: &TokenQuoteState,
    mid: Decimal,
    best_bid: Decimal,
    offset: Decimal,
) -> bool {
    let Some(active) = &state.active_order else {
        return state.pending_replacement.is_none();
    };
    active.price < mid - offset || active.price > best_bid
}

fn requote_reason(state: &TokenQuoteState, mid: Decimal, offset: Decimal) -> &'static str {
    match &state.active_order {
        None => "no_order",
        Some(active) => {
            if active.price < mid - offset {
                "outside_zone"
            } else {
                "best_bid"
            }
        }
    }
}

fn submit_quote(
    rule: &LiquidityRewardRule,
    token: &str,
    state: &mut TokenQuoteState,
    mid: Decimal,
    order_size: Decimal,
    spread: Decimal,
    simulated: bool,
    ts: u64,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
) -> Result<(), tokio::sync::mpsc::error::TrySendError<OrderSignal>> {
    let price = mid - spread / Decimal::TWO;
    if price <= Decimal::ZERO {
        warn!(token = %token, mid = %mid, spread_cents = ?rule.reward_max_spread_cents, price = %price, "liquidity_reward 计算出的挂单价格无效");
        return Ok(());
    }

    let seq = ORDER_SEQ.fetch_add(1, Ordering::Relaxed);
    let order_id = format!("{}-{}-buy-{}", token, ts, seq);
    let topic = state.topic.clone();

    if let Some(active) = &state.active_order {
        let request_cancel = !state.cancel_requested;
        state.pending_replacement = Some(PendingReplacement {
            order_id: order_id.clone(),
            price,
            order_size,
            mid,
        });
        if request_cancel {
            state.cancel_requested = true;
        }
        return order_tx.try_send(OrderSignal::LiquidityRewardStageReplacement {
            strategy: Arc::from("liquidity_reward"),
            topic,
            token: token.to_string(),
            mid,
            side: QuoteSide::Buy,
            price,
            order_size,
            active_local_order_id: active.order_id.clone(),
            pending_local_order_id: order_id,
            request_cancel,
            simulated,
        });
    }

    state.active_order = Some(ActiveOrder {
        order_id: order_id.clone(),
        price,
        order_size,
    });
    state.cancel_requested = false;
    order_tx.try_send(OrderSignal::LiquidityRewardPlace {
        strategy: Arc::from("liquidity_reward"),
        topic,
        token: token.to_string(),
        mid,
        side: QuoteSide::Buy,
        price,
        order_size,
        local_order_id: order_id,
        simulated,
    })
}

fn state_from_restore(restored: LiquidityRewardRestoreState) -> TokenQuoteState {
    TokenQuoteState {
        topic: restored.topic,
        active_order: restored.buy.active_local_order_id.and_then(|order_id| {
            let price = restored.buy.active_price?;
            let order_size = restored.buy.active_order_size?;
            Some(ActiveOrder {
                order_id,
                price,
                order_size,
            })
        }),
        pending_replacement: match (
            restored.buy.pending_local_order_id,
            restored.buy.pending_price,
            restored.buy.pending_order_size,
            restored.buy.pending_mid,
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
        cancel_requested: restored.buy.cancel_requested,
        last_mid: restored.last_mid,
        last_best_bid: restored.last_best_bid,
        last_best_ask: restored.last_best_ask,
    }
}

fn empty_state(rule: &LiquidityRewardRule) -> TokenQuoteState {
    TokenQuoteState {
        topic: rule.topic.clone(),
        active_order: None,
        pending_replacement: None,
        cancel_requested: false,
        last_mid: None,
        last_best_bid: None,
        last_best_ask: None,
    }
}

fn persist_state(order_store: Option<&OrderStore>, token: &str, state: &TokenQuoteState) {
    let Some(store) = order_store else {
        return;
    };

    if let Err(error) = store.upsert_liquidity_reward_shared_state(
        token,
        state.topic.as_ref(),
        state.last_mid,
        state.last_best_bid,
        state.last_best_ask,
        Decimal::ZERO,
    ) {
        warn!(token = %token, error = %error, "liquidity_reward 持久化共享策略状态失败");
    }

    let active = state.active_order.as_ref();
    let pending = state.pending_replacement.as_ref();
    if let Err(error) = store.upsert_liquidity_reward_side_state(
        token,
        QuoteSide::Buy,
        active.map(|o| o.order_id.as_str()),
        pending.map(|p| p.order_id.as_str()),
        pending.map(|p| p.price),
        pending.map(|p| p.order_size),
        pending.map(|p| p.mid),
        None,
        state.cancel_requested,
    ) {
        warn!(token = %token, error = %error, "liquidity_reward 持久化策略状态失败");
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
