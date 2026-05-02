use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use polymarket_client_sdk_v2::types::Decimal;
use tracing::{info, warn};

use crate::{
    notification::{
        LiquidityRewardFillNotification, LiquidityRewardUnwindActionNotification,
        NotificationEvent, Notifier,
    },
    storage::OrderStore,
    strategy::{
        OrderSignal, QuoteSide, Strategy, StrategyEvent, StrategyRegistration, TopicRegistration,
    },
    tick_size::{TickSizeMap, snap_price_to_tick},
};

const DEFAULT_TOPIC: &str = "liquidity_reward";
const PRICE_SCALE: u32 = 10_000;
const SIZE_SCALE: u32 = 10_000;
const UNWIND_RETRY_MAX_ATTEMPTS: u8 = 5;
const UNWIND_RETRY_DELAY: Duration = Duration::from_secs(3);
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
    /// true = FixedOffset 模式：直接挂 target_price，不依赖竞价结构
    pub fixed_price: bool,
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
struct PendingUnwind {
    local_order_id: String,
    price: Decimal,
    order_size: Decimal,
    matched_size: Decimal,
    attempts: u8,
    size_adjusted: bool,
}

#[derive(Debug, Clone)]
struct TokenQuoteState {
    topic: Arc<str>,
    active_order: Option<ActiveOrder>,
    pending_replacement: Option<PendingReplacement>,
    pending_unwinds: HashMap<String, PendingUnwind>,
    cancel_requested: bool,
    last_mid: Option<Decimal>,
    last_best_bid: Option<Decimal>,
    last_best_ask: Option<Decimal>,
    last_bids: Option<Arc<BTreeMap<u16, u32>>>,
    /// 本对内任一订单成交后置 true，此后不再挂新单
    halted: bool,
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
    tick_size_map: TickSizeMap,
    notifier: Option<Notifier>,
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
        tick_size_map: TickSizeMap,
    ) -> Self {
        self.restored_states = restored_states;
        self.order_store = order_store;
        self.simulation_enabled = simulation_enabled;
        self.tick_size_map = tick_size_map;
        self
    }

    pub fn with_notifier(mut self, notifier: Option<Notifier>) -> Self {
        self.notifier = notifier;
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
            let fixed_price = record
                .get(7)
                .map(|v| matches!(v.trim(), "true" | "1" | "yes"))
                .unwrap_or(false);

            rules.push(LiquidityRewardRule {
                topic,
                token1: token1.to_string(),
                token2,
                reward_min_orders,
                reward_max_spread_cents,
                reward_min_size,
                reward_daily_pool,
                fixed_price,
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
            tick_size_map: Arc::new(dashmap::DashMap::new()),
            notifier: None,
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
        let tick_size_map = self.tick_size_map.clone();
        let notifier = self.notifier.clone();

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

                        let bid =
                            Decimal::from(event.book.best_bid_price) / Decimal::from(PRICE_SCALE);
                        let ask =
                            Decimal::from(event.book.best_ask_price) / Decimal::from(PRICE_SCALE);
                        let mid = (bid + ask) / Decimal::TWO;

                        {
                            let state = states
                                .entry(token.clone())
                                .or_insert_with(|| empty_state(rule));
                            state.topic = rule.topic.clone();
                            state.last_mid = Some(mid);
                            state.last_best_bid = Some(bid);
                            state.last_best_ask = Some(ask);
                            state.last_bids = Some(event.book.bids.clone());
                        }

                        let fixed_external_ask = if rule.fixed_price {
                            paired_external_ask(&token, rule, &states)
                        } else {
                            None
                        };

                        let state = states
                            .get_mut(token.as_str())
                            .expect("state exists after market update");

                        if state.halted {
                            continue;
                        }

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

                        let decision = quote_decision(
                            rule,
                            &token,
                            state,
                            mid,
                            ask,
                            fixed_external_ask,
                            spread,
                            &event.book.bids,
                            &tick_size_map,
                        );

                        match decision.action {
                            QuoteAction::PlaceOrReplace { price, reason } => {
                                info!(
                                    target = "order",
                                    token = %token,
                                    topic = %rule.topic,
                                    reason,
                                    mid = %mid,
                                    bid = %bid,
                                    target_price = ?decision.target_price,
                                    min_reward_price = ?decision.min_reward_price,
                                    competitor_best_bid = ?decision.competitor_best_bid,
                                    non_best_cap = ?decision.non_best_cap,
                                    fixed_external_ask = ?decision.fixed_external_ask,
                                    desired_price = %price,
                                    active_price = ?state.active_order.as_ref().map(|o| o.price),
                                    spread_cents = ?rule.reward_max_spread_cents,
                                    "liquidity_reward 触发报价"
                                );
                                if let Err(err) = submit_quote(
                                    &token,
                                    state,
                                    mid,
                                    order_size,
                                    price,
                                    simulation_enabled,
                                    event.book.timestamp_ms,
                                    &order_tx,
                                ) {
                                    warn!(token = %token, error = %err, "liquidity_reward 发送挂单事件失败");
                                }
                            }
                            QuoteAction::CancelOnly { reason } => {
                                info!(
                                    target = "order",
                                    token = %token,
                                    topic = %rule.topic,
                                    reason,
                                    mid = %mid,
                                    bid = %bid,
                                    target_price = ?decision.target_price,
                                    min_reward_price = ?decision.min_reward_price,
                                    competitor_best_bid = ?decision.competitor_best_bid,
                                    non_best_cap = ?decision.non_best_cap,
                                    fixed_external_ask = ?decision.fixed_external_ask,
                                    active_price = ?state.active_order.as_ref().map(|o| o.price),
                                    "liquidity_reward 当前盘口不适合挂单，撤销 active 后等待"
                                );
                                if let Err(err) = cancel_active_order(
                                    &token,
                                    state,
                                    simulation_enabled,
                                    &order_tx,
                                ) {
                                    warn!(token = %token, error = %err, "liquidity_reward 发送撤单等待事件失败");
                                }
                            }
                            QuoteAction::Wait { reason } => {
                                info!(
                                    target = "order",
                                    token = %token,
                                    topic = %rule.topic,
                                    reason,
                                    mid = %mid,
                                    bid = %bid,
                                    target_price = ?decision.target_price,
                                    min_reward_price = ?decision.min_reward_price,
                                    competitor_best_bid = ?decision.competitor_best_bid,
                                    non_best_cap = ?decision.non_best_cap,
                                    fixed_external_ask = ?decision.fixed_external_ask,
                                    active_price = ?state.active_order.as_ref().map(|o| o.price),
                                    "liquidity_reward 报价保持不变或等待"
                                );
                            }
                        }

                        persist_state(order_store.as_ref(), &token, state);
                    }
                    StrategyEvent::OrderStatus(status_event) => {
                        let Some(state) = states.get_mut(status_event.token.as_str()) else {
                            continue;
                        };

                        let token = status_event.token.clone();
                        let status = status_event.status.as_ref();

                        if state
                            .pending_unwinds
                            .contains_key(&status_event.local_order_id)
                        {
                            match status {
                                "open" => {
                                    if let Some(unwind) =
                                        state.pending_unwinds.remove(&status_event.local_order_id)
                                    {
                                        warn!(
                                            target: "order",
                                            token = %token,
                                            order_id = %status_event.local_order_id,
                                            attempts = unwind.attempts,
                                            "liquidity_reward FAK unwind 卖单返回 open，需要手动处理"
                                        );
                                        let topic = state.topic.clone();
                                        notify_unwind_action(
                                            notifier.as_ref(),
                                            Some(topic.to_string()),
                                            token.clone(),
                                            unwind.local_order_id,
                                            unwind.price,
                                            unwind.order_size,
                                            unwind.attempts,
                                            "unwind-manual-required",
                                            simulation_enabled,
                                        );
                                    }
                                }
                                "failed" => {
                                    let reason = status_event.reason.as_ref().map(|r| r.as_ref());
                                    schedule_unwind_retry_if_needed(
                                        &token,
                                        state,
                                        &status_event.local_order_id,
                                        reason,
                                        simulation_enabled,
                                        &order_tx,
                                        notifier.as_ref(),
                                    );
                                }
                                "filled" => {
                                    if let Some(unwind) =
                                        state.pending_unwinds.remove(&status_event.local_order_id)
                                    {
                                        let topic = state.topic.clone();
                                        notify_unwind_action(
                                            notifier.as_ref(),
                                            Some(topic.to_string()),
                                            token.clone(),
                                            unwind.local_order_id,
                                            unwind.price,
                                            unwind.order_size,
                                            unwind.attempts,
                                            "unwind-filled",
                                            simulation_enabled,
                                        );
                                    }
                                }
                                "canceled" | "rejected" => {
                                    if let Some(unwind) =
                                        state.pending_unwinds.remove(&status_event.local_order_id)
                                    {
                                        let remaining = unwind.order_size - unwind.matched_size;
                                        let action = if remaining > Decimal::ZERO {
                                            "unwind-canceled-partial"
                                        } else {
                                            "unwind-canceled"
                                        };
                                        let topic = state.topic.clone();
                                        notify_unwind_action(
                                            notifier.as_ref(),
                                            Some(topic.to_string()),
                                            token.clone(),
                                            unwind.local_order_id.clone(),
                                            unwind.price,
                                            unwind.order_size,
                                            unwind.attempts,
                                            action,
                                            simulation_enabled,
                                        );
                                        if remaining > Decimal::ZERO {
                                            submit_remaining_unwind(
                                                &token,
                                                state,
                                                remaining,
                                                simulation_enabled,
                                                &order_tx,
                                                notifier.as_ref(),
                                                &tick_size_map,
                                            );
                                        }
                                    }
                                }
                                _ => {}
                            }
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

                        // 成交（全部或部分）→ 终止整对做市
                        if matches!(status, "filled" | "partially_filled") {
                            let fill_size = if is_active {
                                state.active_order.as_ref().map(|o| o.order_size)
                            } else {
                                state.pending_replacement.as_ref().map(|p| p.order_size)
                            }
                            .unwrap_or(Decimal::ZERO);
                            let order_price = if is_active {
                                state.active_order.as_ref().map(|o| o.price)
                            } else {
                                state.pending_replacement.as_ref().map(|p| p.price)
                            }
                            .unwrap_or(Decimal::ZERO);
                            let topic_str = state.topic.to_string();

                            info!(
                                target: "order",
                                token = %token,
                                status,
                                order_id = %status_event.local_order_id,
                                fill_size = %fill_size,
                                "liquidity_reward 检测到成交，终止整对做市并撤销所有订单"
                            );

                            // 即时成交（Matched）WS 不推送 fill 事件，此处补发成交通知
                            if let Some(n) = notifier.as_ref() {
                                n.try_notify(NotificationEvent::LiquidityRewardFill(
                                    LiquidityRewardFillNotification {
                                        strategy: "liquidity_reward".to_string(),
                                        topic: Some(topic_str),
                                        token: token.to_string(),
                                        local_order_id: status_event.local_order_id.to_string(),
                                        remote_order_id: String::new(),
                                        side: QuoteSide::Buy,
                                        order_price,
                                        order_size: fill_size,
                                        delta_size: fill_size,
                                        total_matched_size: fill_size,
                                        market: String::new(),
                                        asset_id: token.to_string(),
                                        ws_price: order_price.to_string(),
                                        ws_original_size: Some(fill_size.to_string()),
                                        ws_size_matched: Some(fill_size.to_string()),
                                        ws_status: "Matched".to_string(),
                                        ws_msg_type: "matched".to_string(),
                                        ws_timestamp: None,
                                    },
                                ));
                            }

                            // state 借用在此结束，下方 halt_pair 可重新借用 states
                            let _ = state;
                            halt_pair(
                                &token,
                                &rules,
                                &mut states,
                                simulation_enabled,
                                &order_tx,
                                notifier.as_ref(),
                                order_store.as_ref(),
                                Some(fill_size),
                                &tick_size_map,
                            );
                            continue;
                        }

                        // 非成交终结状态：正常清理
                        if !matches!(status, "canceled" | "rejected" | "failed") {
                            continue;
                        }

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
                    StrategyEvent::OrderFill(fill_event) => {
                        if let Some(state) = states.get_mut(fill_event.token.as_str()) {
                            if let Some(unwind) =
                                state.pending_unwinds.get_mut(&fill_event.local_order_id)
                            {
                                unwind.matched_size = fill_event.total_matched_size;
                                continue;
                            }
                        }
                        let is_ours = states.get(fill_event.token.as_str()).is_some_and(|s| {
                            s.active_order
                                .as_ref()
                                .is_some_and(|o| o.order_id == fill_event.local_order_id)
                                || s.pending_replacement
                                    .as_ref()
                                    .is_some_and(|p| p.order_id == fill_event.local_order_id)
                        });
                        if !is_ours {
                            continue;
                        }
                        info!(
                            target: "order",
                            token = %fill_event.token,
                            order_id = %fill_event.local_order_id,
                            delta_size = %fill_event.delta_size,
                            total_matched = %fill_event.total_matched_size,
                            "liquidity_reward 检测到成交事件，终止整对做市并撤销所有订单"
                        );
                        halt_pair(
                            &fill_event.token,
                            &rules,
                            &mut states,
                            simulation_enabled,
                            &order_tx,
                            notifier.as_ref(),
                            order_store.as_ref(),
                            Some(fill_event.delta_size),
                            &tick_size_map,
                        );
                    }
                    StrategyEvent::Positions(_) => {}
                }
            }
        })
    }
}

fn schedule_unwind_retry_if_needed(
    token: &str,
    state: &mut TokenQuoteState,
    local_order_id: &str,
    reason: Option<&str>,
    simulated: bool,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
    notifier: Option<&Notifier>,
) {
    let Some(mut unwind) = state.pending_unwinds.remove(local_order_id) else {
        return;
    };

    if is_size_precision_error(reason) {
        if unwind.size_adjusted {
            warn!(
                target: "order",
                token = %token,
                order_id = %local_order_id,
                attempts = unwind.attempts,
                reason = ?reason,
                "liquidity_reward unwind 规整数量后仍失败，需要手动处理"
            );
            notify_unwind_action(
                notifier,
                Some(state.topic.to_string()),
                token.to_string(),
                unwind.local_order_id,
                unwind.price,
                unwind.order_size,
                unwind.attempts,
                "unwind-manual-required",
                simulated,
            );
            return;
        }

        let adjusted_size = snap_unwind_size_to_lot(unwind.order_size);
        if adjusted_size <= Decimal::ZERO {
            warn!(
                target: "order",
                token = %token,
                order_id = %local_order_id,
                original_size = %unwind.order_size,
                adjusted_size = %adjusted_size,
                reason = ?reason,
                "liquidity_reward unwind 数量规整后无有效数量，需要手动处理"
            );
            notify_unwind_action(
                notifier,
                Some(state.topic.to_string()),
                token.to_string(),
                unwind.local_order_id,
                unwind.price,
                unwind.order_size,
                unwind.attempts,
                "unwind-manual-required",
                simulated,
            );
            return;
        }

        unwind.attempts += 1;
        unwind.order_size = adjusted_size;
        unwind.size_adjusted = true;
        schedule_unwind_retry(
            token,
            state,
            unwind,
            simulated,
            order_tx,
            notifier,
            "unwind-size-adjust-retry",
            Duration::ZERO,
        );
        return;
    }

    if !is_not_enough_balance_error(reason) {
        warn!(
            target: "order",
            token = %token,
            order_id = %local_order_id,
            reason = ?reason,
            attempts = unwind.attempts,
            "liquidity_reward unwind 卖单失败，原因不可重试，需要手动处理"
        );
        notify_unwind_action(
            notifier,
            Some(state.topic.to_string()),
            token.to_string(),
            unwind.local_order_id,
            unwind.price,
            unwind.order_size,
            unwind.attempts,
            "unwind-manual-required",
            simulated,
        );
        return;
    }

    if unwind.attempts >= UNWIND_RETRY_MAX_ATTEMPTS {
        warn!(
            target: "order",
            token = %token,
            order_id = %local_order_id,
            attempts = unwind.attempts,
            reason = ?reason,
            "liquidity_reward unwind 卖单余额延迟重试达到上限，需要手动处理"
        );
        notify_unwind_action(
            notifier,
            Some(state.topic.to_string()),
            token.to_string(),
            unwind.local_order_id,
            unwind.price,
            unwind.order_size,
            unwind.attempts,
            "unwind-manual-required",
            simulated,
        );
        return;
    }

    unwind.attempts += 1;
    schedule_unwind_retry(
        token,
        state,
        unwind,
        simulated,
        order_tx,
        notifier,
        "unwind-retry",
        UNWIND_RETRY_DELAY,
    );
}

fn schedule_unwind_retry(
    token: &str,
    state: &mut TokenQuoteState,
    mut unwind: PendingUnwind,
    simulated: bool,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
    notifier: Option<&Notifier>,
    action: &'static str,
    delay: Duration,
) {
    let retry_order_id = next_unwind_retry_order_id(token, unwind.attempts);
    let topic = state.topic.clone();
    let topic_for_notify = Some(topic.to_string());
    let price = unwind.price;
    let order_size = unwind.order_size;
    let attempts = unwind.attempts;
    unwind.local_order_id = retry_order_id.clone();
    state.pending_unwinds.insert(retry_order_id.clone(), unwind);

    let order_tx = order_tx.clone();
    let notifier = notifier.cloned();
    let token = token.to_string();
    tokio::spawn(async move {
        if delay > Duration::ZERO {
            tokio::time::sleep(delay).await;
        }
        if let Err(error) = order_tx.try_send(OrderSignal::LiquidityRewardMarketSell {
            strategy: Arc::from("liquidity_reward"),
            topic,
            token: token.clone(),
            price,
            order_size,
            local_order_id: retry_order_id.clone(),
            simulated,
        }) {
            warn!(
                target: "order",
                token = %token,
                order_id = %retry_order_id,
                attempts,
                error = %error,
                "liquidity_reward 发送 unwind 重试卖单失败"
            );
        } else {
            info!(
                target: "order",
                token = %token,
                order_id = %retry_order_id,
                attempts,
                price = %price,
                order_size = %order_size,
                action,
                "liquidity_reward 已发送 unwind 重试卖单"
            );
            notify_unwind_action(
                notifier.as_ref(),
                topic_for_notify,
                token,
                retry_order_id,
                price,
                order_size,
                attempts,
                action,
                simulated,
            );
        }
    });
}

fn notify_unwind_action(
    notifier: Option<&Notifier>,
    topic: Option<String>,
    token: String,
    local_order_id: String,
    price: Decimal,
    order_size: Decimal,
    attempts: u8,
    action: &str,
    simulated: bool,
) {
    if let Some(notifier) = notifier {
        notifier.try_notify(NotificationEvent::LiquidityRewardUnwindAction(
            LiquidityRewardUnwindActionNotification {
                strategy: "liquidity_reward".to_string(),
                topic,
                token,
                local_order_id,
                side: QuoteSide::Sell,
                price,
                order_size,
                attempts,
                action: action.to_string(),
                simulated,
            },
        ));
    }
}

fn is_not_enough_balance_error(reason: Option<&str>) -> bool {
    reason.is_some_and(|reason| {
        reason
            .to_ascii_lowercase()
            .contains("not enough balance / allowance")
    })
}

fn is_size_precision_error(reason: Option<&str>) -> bool {
    reason.is_some_and(|reason| {
        let reason = reason.to_ascii_lowercase();
        (reason.contains("decimal places") && reason.contains("maximum lot size is 2"))
            || (reason.contains("unable to build amount")
                && reason.contains("decimal points")
                && reason.contains("must be <= 2"))
    })
}

fn snap_unwind_size_to_lot(size: Decimal) -> Decimal {
    let scale = Decimal::from(100);
    (size * scale).floor() / scale
}

fn next_unwind_retry_order_id(token: &str, attempts: u8) -> String {
    let seq = ORDER_SEQ.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    format!("{}-{}-unwind-retry-{}-{}", token, ts, attempts, seq)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_not_enough_balance_error() {
        assert!(is_not_enough_balance_error(Some(
            "not enough balance / allowance: the balance is not enough"
        )));
        assert!(is_not_enough_balance_error(Some(
            "NOT ENOUGH BALANCE / ALLOWANCE"
        )));
        assert!(!is_not_enough_balance_error(Some("invalid price")));
        assert!(!is_not_enough_balance_error(None));
    }

    #[test]
    fn detects_size_precision_error() {
        assert!(is_size_precision_error(Some(
            "Validation: invalid: Unable to build Order: Size 4.050846 has 6 decimal places. Maximum lot size is 2"
        )));
        assert!(is_size_precision_error(Some(
            "Validation: invalid: Unable to build Amount with 6 decimal points, must be <= 2"
        )));
        assert!(!is_size_precision_error(Some(
            "not enough balance / allowance"
        )));
        assert!(!is_size_precision_error(None));
    }

    #[test]
    fn snaps_unwind_size_to_two_decimal_places() {
        let size = Decimal::try_from(4.050846_f64).unwrap();
        assert_eq!(
            snap_unwind_size_to_lot(size),
            Decimal::try_from(4.05_f64).unwrap()
        );
    }

    #[test]
    fn creates_retry_order_id_with_attempt() {
        let order_id = next_unwind_retry_order_id("token", 2);
        assert!(order_id.starts_with("token-"));
        assert!(order_id.contains("-unwind-retry-2-"));
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

enum QuoteAction {
    PlaceOrReplace {
        price: Decimal,
        reason: &'static str,
    },
    CancelOnly {
        reason: &'static str,
    },
    Wait {
        reason: &'static str,
    },
}

struct QuoteDecision {
    action: QuoteAction,
    target_price: Option<Decimal>,
    min_reward_price: Decimal,
    competitor_best_bid: Option<Decimal>,
    non_best_cap: Option<Decimal>,
    fixed_external_ask: Option<Decimal>,
}

fn quote_decision(
    rule: &LiquidityRewardRule,
    token: &str,
    state: &TokenQuoteState,
    mid: Decimal,
    best_ask: Decimal,
    fixed_external_ask: Option<Decimal>,
    spread: Decimal,
    bids: &BTreeMap<u16, u32>,
    tick_size_map: &TickSizeMap,
) -> QuoteDecision {
    let default_tick = Decimal::try_from(0.01_f64).unwrap_or(Decimal::ONE);
    let tick = tick_size_map.get(token).map(|v| *v).unwrap_or(default_tick);
    let competitor_best_bid = competitor_best_bid(bids, state);
    let fixed_mid = if rule.fixed_price {
        match (competitor_best_bid, fixed_external_ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::TWO),
            (Some(bid), None) if rule.token2.is_none() => Some((bid + best_ask) / Decimal::TWO),
            _ => None,
        }
    } else {
        None
    };

    if rule.fixed_price && fixed_mid.is_none() && rule.token2.is_some() {
        return QuoteDecision {
            action: if state.active_order.is_some() {
                QuoteAction::CancelOnly {
                    reason: "no_external_fixed_mid",
                }
            } else {
                QuoteAction::Wait {
                    reason: "no_external_fixed_mid",
                }
            },
            target_price: None,
            min_reward_price: Decimal::ZERO,
            competitor_best_bid,
            non_best_cap: None,
            fixed_external_ask,
        };
    }

    let pricing_mid = if rule.fixed_price {
        fixed_mid.unwrap_or(mid)
    } else {
        mid
    };
    let target_price = snap_price_to_tick(pricing_mid - spread / Decimal::TWO, tick, true);
    let min_reward_price = pricing_mid - spread;

    // FixedOffset 模式：直接挂 target_price，不依赖竞价结构
    if rule.fixed_price {
        if target_price <= Decimal::ZERO {
            warn!(token = %token, mid = %mid, spread_cents = ?rule.reward_max_spread_cents, price = %target_price, "liquidity_reward(fixed) 计算出的挂单价格无效");
            return QuoteDecision {
                action: if state.active_order.is_some() {
                    QuoteAction::CancelOnly {
                        reason: "invalid_target_price",
                    }
                } else {
                    QuoteAction::Wait {
                        reason: "invalid_target_price",
                    }
                },
                target_price: Some(target_price),
                min_reward_price,
                competitor_best_bid: None,
                non_best_cap: None,
                fixed_external_ask,
            };
        }
        let action = match &state.active_order {
            None => {
                if fixed_mid.is_none() {
                    QuoteAction::Wait {
                        reason: "only_own_bid",
                    }
                } else if state.pending_replacement.is_none() {
                    QuoteAction::PlaceOrReplace {
                        price: target_price,
                        reason: "no_order",
                    }
                } else {
                    QuoteAction::Wait {
                        reason: "pending_replacement",
                    }
                }
            }
            Some(active) => {
                if active.price != target_price {
                    if state
                        .pending_replacement
                        .as_ref()
                        .is_some_and(|p| p.price == target_price)
                    {
                        QuoteAction::Wait {
                            reason: "pending_replacement_same_price",
                        }
                    } else {
                        QuoteAction::PlaceOrReplace {
                            price: target_price,
                            reason: "price_drifted",
                        }
                    }
                } else {
                    QuoteAction::Wait {
                        reason: "unchanged",
                    }
                }
            }
        };
        return QuoteDecision {
            action,
            target_price: Some(target_price),
            min_reward_price,
            competitor_best_bid,
            non_best_cap: None,
            fixed_external_ask,
        };
    }

    // CompetitorBased 模式（原有逻辑）
    let non_best_cap = competitor_best_bid.map(|price| price - tick);

    if target_price <= Decimal::ZERO {
        warn!(token = %token, mid = %mid, spread_cents = ?rule.reward_max_spread_cents, tick = %tick, price = %target_price, "liquidity_reward 计算出的挂单价格无效");
        return QuoteDecision {
            action: if state.active_order.is_some() {
                QuoteAction::CancelOnly {
                    reason: "invalid_target_price",
                }
            } else {
                QuoteAction::Wait {
                    reason: "invalid_target_price",
                }
            },
            target_price: Some(target_price),
            min_reward_price,
            competitor_best_bid,
            non_best_cap,
            fixed_external_ask,
        };
    }

    let Some(non_best_cap) = non_best_cap else {
        return QuoteDecision {
            action: if state.active_order.is_some() {
                QuoteAction::CancelOnly {
                    reason: "no_competitor_bid",
                }
            } else {
                QuoteAction::Wait {
                    reason: "no_competitor_bid",
                }
            },
            target_price: Some(target_price),
            min_reward_price,
            competitor_best_bid,
            non_best_cap,
            fixed_external_ask,
        };
    };

    let desired_price = if non_best_cap >= target_price {
        target_price
    } else if non_best_cap >= min_reward_price {
        snap_price_to_tick(non_best_cap, tick, true)
    } else {
        return QuoteDecision {
            action: if state.active_order.is_some() {
                QuoteAction::CancelOnly {
                    reason: "outside_reward_zone_wait",
                }
            } else {
                QuoteAction::Wait {
                    reason: "outside_reward_zone_wait",
                }
            },
            target_price: Some(target_price),
            min_reward_price,
            competitor_best_bid,
            non_best_cap: Some(non_best_cap),
            fixed_external_ask,
        };
    };

    let action = match &state.active_order {
        None => {
            if state.pending_replacement.is_none() {
                QuoteAction::PlaceOrReplace {
                    price: desired_price,
                    reason: "no_order",
                }
            } else {
                QuoteAction::Wait {
                    reason: "pending_replacement",
                }
            }
        }
        Some(active) => {
            if active.price != desired_price {
                if state
                    .pending_replacement
                    .as_ref()
                    .is_some_and(|p| p.price == desired_price)
                {
                    QuoteAction::Wait {
                        reason: "pending_replacement_same_price",
                    }
                } else {
                    QuoteAction::PlaceOrReplace {
                        price: desired_price,
                        reason: "target_price_changed",
                    }
                }
            } else {
                QuoteAction::Wait {
                    reason: "unchanged",
                }
            }
        }
    };

    QuoteDecision {
        action,
        target_price: Some(target_price),
        min_reward_price,
        competitor_best_bid,
        non_best_cap: Some(non_best_cap),
        fixed_external_ask,
    }
}

fn paired_token<'a>(token: &str, rule: &'a LiquidityRewardRule) -> Option<&'a str> {
    if token == rule.token1 {
        rule.token2.as_deref()
    } else {
        Some(rule.token1.as_str())
    }
}

fn paired_external_ask(
    token: &str,
    rule: &LiquidityRewardRule,
    states: &HashMap<String, TokenQuoteState>,
) -> Option<Decimal> {
    let paired = paired_token(token, rule)?;
    let paired_state = states.get(paired)?;
    let paired_bids = paired_state.last_bids.as_deref()?;
    let paired_external_bid = competitor_best_bid(paired_bids, paired_state)?;
    let external_ask = Decimal::ONE - paired_external_bid;
    (external_ask > Decimal::ZERO).then_some(external_ask)
}

fn competitor_best_bid(bids: &BTreeMap<u16, u32>, state: &TokenQuoteState) -> Option<Decimal> {
    for (&price, &size) in bids.iter().rev() {
        let mut remaining = size as i64;
        if let Some(order) = state.active_order.as_ref() {
            if scaled_price(order.price) == price {
                remaining -= scaled_size(order.order_size);
            }
        }
        if let Some(order) = state.pending_replacement.as_ref() {
            if scaled_price(order.price) == price {
                remaining -= scaled_size(order.order_size);
            }
        }
        if remaining > 0 {
            return Some(Decimal::from(price) / Decimal::from(PRICE_SCALE));
        }
    }
    None
}

fn scaled_price(price: Decimal) -> u16 {
    (price * Decimal::from(PRICE_SCALE))
        .round()
        .to_string()
        .parse()
        .unwrap_or_default()
}

fn scaled_size(size: Decimal) -> i64 {
    (size * Decimal::from(SIZE_SCALE))
        .round()
        .to_string()
        .parse()
        .unwrap_or_default()
}

fn cancel_active_order(
    token: &str,
    state: &mut TokenQuoteState,
    simulated: bool,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
) -> Result<(), tokio::sync::mpsc::error::TrySendError<OrderSignal>> {
    let Some(active) = state.active_order.as_ref() else {
        return Ok(());
    };
    state.pending_replacement = None;
    if state.cancel_requested {
        return Ok(());
    }
    order_tx.try_send(OrderSignal::LiquidityRewardCancel {
        strategy: Arc::from("liquidity_reward"),
        topic: state.topic.clone(),
        token: token.to_string(),
        side: QuoteSide::Buy,
        active_local_order_id: active.order_id.clone(),
        simulated,
    })?;
    state.cancel_requested = true;
    Ok(())
}

fn submit_quote(
    token: &str,
    state: &mut TokenQuoteState,
    mid: Decimal,
    order_size: Decimal,
    price: Decimal,
    simulated: bool,
    ts: u64,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
) -> Result<(), tokio::sync::mpsc::error::TrySendError<OrderSignal>> {
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
        pending_unwinds: HashMap::new(),
        cancel_requested: restored.buy.cancel_requested,
        last_mid: restored.last_mid,
        last_best_bid: restored.last_best_bid,
        last_best_ask: restored.last_best_ask,
        last_bids: None,
        halted: false,
    }
}

fn empty_state(rule: &LiquidityRewardRule) -> TokenQuoteState {
    TokenQuoteState {
        topic: rule.topic.clone(),
        active_order: None,
        pending_replacement: None,
        pending_unwinds: HashMap::new(),
        cancel_requested: false,
        last_mid: None,
        last_best_bid: None,
        last_best_ask: None,
        last_bids: None,
        halted: false,
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

/// 终止整个做市对：取消两个 token 的所有挂单并标记 halted。
/// 只要该对中任一订单发生成交（全部或部分），就调用此函数。
/// 若 `unwind_size` 非 None，则在撤单后立即以市价卖出该数量的持仓。
fn halt_pair(
    token: &str,
    rules: &std::collections::HashMap<String, LiquidityRewardRule>,
    states: &mut std::collections::HashMap<String, TokenQuoteState>,
    simulated: bool,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
    notifier: Option<&Notifier>,
    order_store: Option<&OrderStore>,
    unwind_size: Option<Decimal>,
    tick_size_map: &TickSizeMap,
) {
    let Some(rule) = rules.get(token) else { return };
    let paired = paired_token(token, rule);

    cancel_and_halt(token, states, simulated, order_tx, order_store);
    if let Some(p) = paired {
        cancel_and_halt(p, states, simulated, order_tx, order_store);
    }

    let Some(size) = unwind_size else { return };
    let state = states.get(token);
    let topic = state
        .map(|s| s.topic.clone())
        .unwrap_or_else(|| Arc::from("liquidity_reward"));
    let sell_ref_price = state
        .and_then(|s| s.last_best_bid)
        .or_else(|| state.and_then(|s| s.last_mid));
    let Some(ref_price) = sell_ref_price else {
        warn!(token = %token, "liquidity_reward 无法获取最新买价，跳过市价卖出");
        return;
    };
    let default_tick = Decimal::try_from(0.01_f64).unwrap_or(Decimal::ONE);
    let tick = tick_size_map.get(token).map(|v| *v).unwrap_or(default_tick);
    let sell_price = snap_price_to_tick(ref_price, tick, true);
    if sell_price <= Decimal::ZERO {
        warn!(token = %token, sell_price = %sell_price, "liquidity_reward 市价卖出价格无效，跳过");
        return;
    }
    let seq = ORDER_SEQ.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let order_id = format!("{}-{}-unwind-{}", token, ts, seq);
    let topic_for_notify = Some(topic.to_string());
    info!(
        target: "order",
        token = %token,
        sell_price = %sell_price,
        size = %size,
        order_id = %order_id,
        "liquidity_reward 成交后立即市价卖出持仓"
    );
    match order_tx.try_send(OrderSignal::LiquidityRewardMarketSell {
        strategy: Arc::from("liquidity_reward"),
        topic,
        token: token.to_string(),
        price: sell_price,
        order_size: size,
        local_order_id: order_id.clone(),
        simulated,
    }) {
        Ok(()) => {
            if let Some(state) = states.get_mut(token) {
                state.pending_unwinds.insert(
                    order_id.clone(),
                    PendingUnwind {
                        local_order_id: order_id.clone(),
                        price: sell_price,
                        order_size: size,
                        matched_size: Decimal::ZERO,
                        attempts: 0,
                        size_adjusted: false,
                    },
                );
            }
            notify_unwind_action(
                notifier,
                topic_for_notify,
                token.to_string(),
                order_id,
                sell_price,
                size,
                0,
                "unwind",
                simulated,
            );
        }
        Err(e) => {
            warn!(token = %token, error = %e, "liquidity_reward 发送市价卖出失败");
        }
    }
}

/// 撤销单个 token 的挂单并置 halted = true。
fn cancel_and_halt(
    token: &str,
    states: &mut std::collections::HashMap<String, TokenQuoteState>,
    simulated: bool,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
    order_store: Option<&OrderStore>,
) {
    let Some(state) = states.get_mut(token) else {
        return;
    };
    if state.halted {
        return;
    }
    state.halted = true;
    state.pending_replacement = None;
    state.cancel_requested = false;

    if let Some(active) = state.active_order.take() {
        let topic = state.topic.clone();
        if let Err(e) = order_tx.try_send(OrderSignal::LiquidityRewardCancel {
            strategy: Arc::from("liquidity_reward"),
            topic,
            token: token.to_string(),
            side: QuoteSide::Buy,
            active_local_order_id: active.order_id,
            simulated,
        }) {
            warn!(token = %token, error = %e, "liquidity_reward halt 发送撤单失败");
        }
    }
    persist_state(order_store, token, state);
}

fn submit_remaining_unwind(
    token: &str,
    state: &mut TokenQuoteState,
    remaining_size: Decimal,
    simulated: bool,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
    notifier: Option<&Notifier>,
    tick_size_map: &TickSizeMap,
) {
    let sell_ref_price = state.last_best_bid.or(state.last_mid);
    let Some(ref_price) = sell_ref_price else {
        warn!(target: "order", token = %token, "liquidity_reward unwind 部分取消后无法获取买价，跳过剩余卖出");
        return;
    };
    let default_tick = Decimal::try_from(0.01_f64).unwrap_or(Decimal::ONE);
    let tick = tick_size_map.get(token).map(|v| *v).unwrap_or(default_tick);
    let sell_price = snap_price_to_tick(ref_price, tick, true);
    if sell_price <= Decimal::ZERO {
        warn!(target: "order", token = %token, sell_price = %sell_price, "liquidity_reward unwind 剩余卖出价格无效，跳过");
        return;
    }
    let seq = ORDER_SEQ.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let order_id = format!("{}-{}-unwind-remaining-{}", token, ts, seq);
    let topic = state.topic.clone();
    info!(
        target: "order",
        token = %token,
        sell_price = %sell_price,
        size = %remaining_size,
        order_id = %order_id,
        "liquidity_reward unwind 部分取消，重新提交剩余持仓卖单"
    );
    match order_tx.try_send(OrderSignal::LiquidityRewardMarketSell {
        strategy: Arc::from("liquidity_reward"),
        topic: topic.clone(),
        token: token.to_string(),
        price: sell_price,
        order_size: remaining_size,
        local_order_id: order_id.clone(),
        simulated,
    }) {
        Ok(()) => {
            state.pending_unwinds.insert(
                order_id.clone(),
                PendingUnwind {
                    local_order_id: order_id.clone(),
                    price: sell_price,
                    order_size: remaining_size,
                    matched_size: Decimal::ZERO,
                    attempts: 0,
                    size_adjusted: false,
                },
            );
            notify_unwind_action(
                notifier,
                Some(topic.to_string()),
                token.to_string(),
                order_id,
                sell_price,
                remaining_size,
                0,
                "unwind-remaining",
                simulated,
            );
        }
        Err(e) => {
            warn!(target: "order", token = %token, error = %e, "liquidity_reward 发送 unwind 剩余卖单失败");
        }
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
