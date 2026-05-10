use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use polymarket_client_sdk_v2::types::Decimal;
use tracing::warn;

use crate::{
    notification::{LiquidityRewardManualAttentionNotification, NotificationEvent, Notifier},
    storage::{ActiveRewardMarketPoolEntry, MarketStore, OrderStore},
    strategies::liquidity_reward::{LiquidityRewardRestoreState, LiquidityRewardRule},
    strategy::{OrderSignal, Strategy, StrategyEvent, StrategyRegistration, TopicRegistration},
    tick_size::{TickSizeMap, snap_price_to_tick, snap_unwind_size_to_lot},
};

const DEFAULT_TOPIC: &str = "liquidity_reward";
const PRICE_SCALE: u32 = 10_000;
const SIZE_SCALE: u32 = 10_000;
const FILL_UNWIND_MAX_ATTEMPTS: u8 = 5;
static ORDER_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq)]
struct ActiveOrder {
    order_id: String,
    price: Decimal,
    order_size: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
struct PendingReplacement {
    order_id: String,
    price: Decimal,
    order_size: Decimal,
    mid: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
enum UnwindKind {
    Fill,
    PoolRemoval,
    Remaining,
}

#[derive(Debug, Clone, PartialEq)]
struct PendingUnwind {
    local_order_id: String,
    price: Decimal,
    order_size: Decimal,
    matched_size: Decimal,
    attempts: u8,
    kind: UnwindKind,
}

#[derive(Debug, Clone, PartialEq)]
struct FillUnwindIntent {
    trigger_local_order_id: String,
    trigger_remote_order_id: Option<String>,
    remaining_size: Decimal,
    trade_confirmed: bool,
    position_visible_size: Option<Decimal>,
    attempts: u8,
    created_at_ms: u64,
    next_retry_after_ms: Option<u64>,
    last_error: Option<String>,
    manual_notified: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum CancelNext {
    Wait,
    Replace(PendingReplacement),
}

#[derive(Debug, Clone, PartialEq)]
enum HaltReason {
    Fill,
    PoolRemoval,
}

#[derive(Debug, Clone, PartialEq)]
enum QuoteState {
    Idle,
    Active {
        order: ActiveOrder,
    },
    Canceling {
        order: ActiveOrder,
        next: CancelNext,
    },
    Halted {
        active: Option<ActiveOrder>,
        cancel_requested: bool,
        reason: HaltReason,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum RiskState {
    Normal,
    PoolRemovalPending {
        position_size: Option<Decimal>,
        unwind_in_flight: bool,
    },
}

#[derive(Debug, Clone, Default)]
struct MarketSnapshot {
    mid: Option<Decimal>,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
    bids: Option<Arc<BTreeMap<u16, u32>>>,
}

#[derive(Debug, Clone)]
struct TokenFsm {
    token: String,
    topic: Arc<str>,
    quote: QuoteState,
    risk: RiskState,
    market: MarketSnapshot,
    pending_unwinds: HashMap<String, PendingUnwind>,
    pending_fill_unwind: Option<FillUnwindIntent>,
}

#[derive(Debug, Clone, PartialEq)]
enum Effect {
    PlaceBuy {
        token: String,
        topic: Arc<str>,
        local_order_id: String,
        mid: Decimal,
        price: Decimal,
        order_size: Decimal,
    },
    CancelBuy {
        token: String,
        topic: Arc<str>,
        local_order_id: String,
    },
    MarketSell {
        token: String,
        topic: Arc<str>,
        local_order_id: String,
        price: Decimal,
        order_size: Decimal,
    },
    PersistPoolHalt {
        condition_id: String,
        pool_version: u64,
        reason: &'static str,
    },
}

// TokenFsm 只维护内存状态并返回待执行 effect，不直接访问订单通道或数据库。
impl TokenFsm {
    // 创建单 token 的初始 FSM：未挂单、风险正常、行情快照为空。
    fn empty(token: String, topic: Arc<str>) -> Self {
        Self {
            token,
            topic,
            quote: QuoteState::Idle,
            risk: RiskState::Normal,
            market: MarketSnapshot::default(),
            pending_unwinds: HashMap::new(),
            pending_fill_unwind: None,
        }
    }

    fn mark_fill_unwind_intent(
        &mut self,
        trigger_local_order_id: &str,
        trigger_remote_order_id: Option<&str>,
        remaining_size: Decimal,
    ) {
        if remaining_size <= Decimal::ZERO {
            return;
        }
        if let Some(intent) = self.pending_fill_unwind.as_mut() {
            if remaining_size > intent.remaining_size {
                intent.remaining_size = remaining_size;
            }
            if intent.trigger_remote_order_id.is_none() {
                intent.trigger_remote_order_id = trigger_remote_order_id.map(str::to_string);
            }
            return;
        }
        let created_at_ms = now_ms();
        self.pending_fill_unwind = Some(FillUnwindIntent {
            trigger_local_order_id: trigger_local_order_id.to_string(),
            trigger_remote_order_id: trigger_remote_order_id.map(str::to_string),
            remaining_size,
            trade_confirmed: false,
            position_visible_size: None,
            attempts: 0,
            created_at_ms,
            next_retry_after_ms: None,
            last_error: None,
            manual_notified: false,
        });
    }

    fn mark_fill_trade_confirmed(&mut self, trade: &crate::strategy::TradeConfirmedEvent) -> bool {
        let Some(intent) = self.pending_fill_unwind.as_mut() else {
            return false;
        };
        if trade.token != self.token {
            return false;
        }
        let order_id = intent
            .trigger_remote_order_id
            .as_deref()
            .unwrap_or(intent.trigger_local_order_id.as_str());
        let matched = trade.taker_order_id.as_deref() == Some(order_id)
            || trade
                .maker_order_ids
                .iter()
                .any(|maker_order_id| maker_order_id == order_id);
        if matched {
            intent.trade_confirmed = true;
        }
        matched
    }

    fn update_fill_unwind_position(&mut self, size: Decimal) {
        let Some(intent) = self.pending_fill_unwind.as_mut() else {
            return;
        };
        intent.position_visible_size = (size > Decimal::ZERO).then_some(size);
    }

    fn retain_fill_unwind_after_balance_failure(&mut self, reason: &str, retry_after: Duration) {
        let Some(intent) = self.pending_fill_unwind.as_mut() else {
            return;
        };
        intent.last_error = Some(reason.to_string());
        intent.next_retry_after_ms = Some(now_ms().saturating_add(retry_after.as_millis() as u64));
    }

    fn clear_fill_unwind_intent(&mut self) {
        self.pending_fill_unwind = None;
    }

    fn manual_attention_notification(
        &mut self,
        reason: &str,
    ) -> Option<LiquidityRewardManualAttentionNotification> {
        let intent = self.pending_fill_unwind.as_mut()?;
        if intent.manual_notified {
            return None;
        }
        intent.manual_notified = true;
        intent.last_error = Some(reason.to_string());
        Some(LiquidityRewardManualAttentionNotification {
            strategy: "liquidity_reward".to_string(),
            topic: Some(self.topic.to_string()),
            token: self.token.clone(),
            trigger_local_order_id: intent.trigger_local_order_id.clone(),
            trigger_remote_order_id: intent.trigger_remote_order_id.clone(),
            remaining_size: intent.remaining_size,
            visible_position_size: intent.position_visible_size,
            attempts: intent.attempts,
            last_error: reason.to_string(),
            waited_secs: now_ms().saturating_sub(intent.created_at_ms) / 1000,
        })
    }

    fn submit_fill_unwind_if_ready(&mut self, tick_size_map: &TickSizeMap) -> Vec<Effect> {
        let Some(intent) = self.pending_fill_unwind.clone() else {
            return Vec::new();
        };
        if !intent.trade_confirmed || intent.attempts >= FILL_UNWIND_MAX_ATTEMPTS {
            return Vec::new();
        }
        if self
            .pending_unwinds
            .values()
            .any(|unwind| unwind.kind == UnwindKind::Fill)
        {
            return Vec::new();
        }
        if intent
            .next_retry_after_ms
            .is_some_and(|next_retry_after_ms| now_ms() < next_retry_after_ms)
        {
            return Vec::new();
        }
        let Some(visible_size) = intent
            .position_visible_size
            .filter(|size| *size > Decimal::ZERO)
        else {
            return Vec::new();
        };
        let size = if visible_size < intent.remaining_size {
            visible_size
        } else {
            intent.remaining_size
        };
        let effects = self.submit_unwind(size, tick_size_map, "unwind", UnwindKind::Fill);
        if !effects.is_empty() {
            if let Some(intent) = self.pending_fill_unwind.as_mut() {
                intent.attempts = intent.attempts.saturating_add(1);
                intent.position_visible_size = Some(visible_size);
                intent.next_retry_after_ms = None;
            }
        }
        effects
    }

    // 初次或替换后挂买单：状态进入 Active，并返回 PlaceBuy effect 交给外层执行。
    fn place_buy(
        &mut self,
        local_order_id: String,
        mid: Decimal,
        price: Decimal,
        order_size: Decimal,
    ) -> Vec<Effect> {
        self.quote = QuoteState::Active {
            order: ActiveOrder {
                order_id: local_order_id.clone(),
                price,
                order_size,
            },
        };
        vec![Effect::PlaceBuy {
            token: self.token.clone(),
            topic: self.topic.clone(),
            local_order_id,
            mid,
            price,
            order_size,
        }]
    }

    // 仅撤当前 active 买单且不补新单：Active -> Canceling(Wait)，等待撤单确认后回到 Idle。
    fn cancel_wait(&mut self) -> Vec<Effect> {
        let QuoteState::Active { order } = self.quote.clone() else {
            return Vec::new();
        };
        self.quote = QuoteState::Canceling {
            order: order.clone(),
            next: CancelNext::Wait,
        };
        vec![Effect::CancelBuy {
            token: self.token.clone(),
            topic: self.topic.clone(),
            local_order_id: order.order_id,
        }]
    }

    // 报价需要替换时先撤旧单：Active -> Canceling(Replace)，撤单确认后再提交 pending 新单。
    fn stage_replacement(&mut self, pending: PendingReplacement) -> Vec<Effect> {
        let QuoteState::Active { order } = self.quote.clone() else {
            return Vec::new();
        };
        self.quote = QuoteState::Canceling {
            order: order.clone(),
            next: CancelNext::Replace(pending),
        };
        vec![Effect::CancelBuy {
            token: self.token.clone(),
            topic: self.topic.clone(),
            local_order_id: order.order_id,
        }]
    }

    // 收到撤单成功确认：Canceling(Wait) 回 Idle，Canceling(Replace) 立即产生新 PlaceBuy effect。
    fn on_cancel_confirmed(&mut self) -> Vec<Effect> {
        let QuoteState::Canceling { next, .. } = self.quote.clone() else {
            return Vec::new();
        };
        match next {
            CancelNext::Wait => {
                self.quote = QuoteState::Idle;
                Vec::new()
            }
            CancelNext::Replace(pending) => self.place_buy(
                pending.order_id,
                pending.mid,
                pending.price,
                pending.order_size,
            ),
        }
    }

    // 正常替换/撤单流程里撤单失败：Canceling 回 Active，后续行情可重新判断是否继续撤换。
    fn on_cancel_failed(&mut self) {
        if let QuoteState::Canceling { order, .. } = self.quote.clone() {
            self.quote = QuoteState::Active { order };
        }
    }

    // 风控停机入口：保留仍可能在交易所的 active，除触发成交单自身外都生成 CancelBuy effect。
    fn halt(&mut self, reason: HaltReason, filled_local_order_id: Option<&str>) -> Vec<Effect> {
        let (active, cancel_requested) = match self.quote.clone() {
            QuoteState::Active { order } => (Some(order), false),
            QuoteState::Canceling { order, .. } => (Some(order), true),
            QuoteState::Halted {
                active,
                cancel_requested,
                ..
            } => (active, cancel_requested),
            QuoteState::Idle => (None, false),
        };
        let active = active.filter(|order| filled_local_order_id != Some(order.order_id.as_str()));

        let cancel_effect = active.as_ref().and_then(|order| {
            (!cancel_requested).then(|| Effect::CancelBuy {
                token: self.token.clone(),
                topic: self.topic.clone(),
                local_order_id: order.order_id.clone(),
            })
        });

        self.quote = QuoteState::Halted {
            active,
            cancel_requested: cancel_requested || cancel_effect.is_some(),
            reason,
        };

        cancel_effect.into_iter().collect()
    }

    // Halted 状态下补发撤单：只有 active 存在且当前没有在途 cancel 时才返回 CancelBuy effect。
    fn retry_halted_cancel(&mut self) -> Vec<Effect> {
        let QuoteState::Halted {
            active: Some(order),
            cancel_requested,
            reason,
        } = self.quote.clone()
        else {
            return Vec::new();
        };
        if cancel_requested {
            return Vec::new();
        }
        self.quote = QuoteState::Halted {
            active: Some(order.clone()),
            cancel_requested: true,
            reason,
        };
        vec![Effect::CancelBuy {
            token: self.token.clone(),
            topic: self.topic.clone(),
            local_order_id: order.order_id,
        }]
    }

    // Halted 撤单发送或执行失败：仍保持停机，只重开 cancel retry 窗口，不能回到 Active。
    fn on_halted_cancel_failed(&mut self) {
        if let QuoteState::Halted { active, reason, .. } = self.quote.clone() {
            self.quote = QuoteState::Halted {
                active,
                cancel_requested: false,
                reason,
            };
        }
    }

    // 记录池子剔除后的清仓意图：未提交时等待仓位快照，已提交时避免重复重置。
    fn mark_pool_removal_unwind(&mut self) {
        if !matches!(
            self.risk,
            RiskState::PoolRemovalPending {
                unwind_in_flight: true,
                ..
            }
        ) {
            self.risk = RiskState::PoolRemovalPending {
                position_size: None,
                unwind_in_flight: false,
            };
        }
    }

    // 用最新仓位更新池子剔除清仓意图：仓位清零则风险恢复 Normal，否则记录待清仓数量。
    fn update_pool_removal_position(&mut self, size: Decimal) {
        if size <= Decimal::ZERO {
            self.risk = RiskState::Normal;
            return;
        }
        if let RiskState::PoolRemovalPending {
            unwind_in_flight, ..
        } = self.risk.clone()
        {
            self.risk = RiskState::PoolRemovalPending {
                position_size: Some(size),
                unwind_in_flight,
            };
        }
    }

    // 根据最新 best bid/mid 生成 FAK 卖单 effect，并登记 pending unwind 供后续状态事件更新。
    fn submit_unwind(
        &mut self,
        size: Decimal,
        tick_size_map: &TickSizeMap,
        order_id_suffix: &str,
        kind: UnwindKind,
    ) -> Vec<Effect> {
        let Some(ref_price) = self.market.best_bid.or(self.market.mid) else {
            warn!(token = %self.token, "liquidity_reward_fsm 无法获取最新买价，暂缓市价卖出");
            return Vec::new();
        };
        let default_tick = Decimal::try_from(0.01_f64).unwrap_or(Decimal::ONE);
        let tick = tick_size_map
            .get(self.token.as_str())
            .map(|value| *value)
            .unwrap_or(default_tick);
        let price = snap_price_to_tick(ref_price, tick, true);
        let order_size = snap_unwind_size_to_lot(size);
        if price <= Decimal::ZERO || order_size <= Decimal::ZERO {
            warn!(token = %self.token, price = %price, size = %size, order_size = %order_size, "liquidity_reward_fsm 清仓卖单价格或数量无效，暂缓");
            return Vec::new();
        }
        let seq = ORDER_SEQ.fetch_add(1, Ordering::Relaxed);
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let local_order_id = format!("{}-{}-{}-{}", self.token, ts, order_id_suffix, seq);
        self.pending_unwinds.insert(
            local_order_id.clone(),
            PendingUnwind {
                local_order_id: local_order_id.clone(),
                price,
                order_size,
                matched_size: Decimal::ZERO,
                attempts: 0,
                kind,
            },
        );
        vec![Effect::MarketSell {
            token: self.token.clone(),
            topic: self.topic.clone(),
            local_order_id,
            price,
            order_size,
        }]
    }

    // pending unwind 的成交只更新已成交量；它是风险回退卖单，不能反向触发 pair halt。
    fn on_unwind_fill(&mut self, local_order_id: &str, matched_size: Decimal) -> bool {
        let Some(unwind) = self.pending_unwinds.get_mut(local_order_id) else {
            return false;
        };
        unwind.matched_size = matched_size;
        true
    }

    // unwind 终态会清理 pending；pool removal 来源的卖单终态会释放 in-flight，让后续 Positions 可继续补卖。
    fn on_unwind_terminal(&mut self, local_order_id: &str) -> Option<PendingUnwind> {
        let unwind = self.pending_unwinds.remove(local_order_id)?;
        if unwind.kind == UnwindKind::PoolRemoval {
            if let RiskState::PoolRemovalPending { position_size, .. } = self.risk.clone() {
                self.risk = RiskState::PoolRemovalPending {
                    position_size,
                    unwind_in_flight: false,
                };
            }
        }
        Some(unwind)
    }

    // 池子剔除清仓有在途卖单时不重复提交；终态后由 Positions 再确认是否继续补卖。
    fn submit_pool_removal_unwind_if_ready(&mut self, tick_size_map: &TickSizeMap) -> Vec<Effect> {
        let RiskState::PoolRemovalPending {
            position_size: Some(size),
            unwind_in_flight: false,
        } = self.risk.clone()
        else {
            return Vec::new();
        };
        let effects =
            self.submit_unwind(size, tick_size_map, "pool-unwind", UnwindKind::PoolRemoval);
        if !effects.is_empty() {
            self.risk = RiskState::PoolRemovalPending {
                position_size: None,
                unwind_in_flight: true,
            };
        }
        effects
    }
}

pub struct LiquidityRewardFsmStrategy {
    rules: Arc<HashMap<String, LiquidityRewardRule>>,
    registration: Arc<StrategyRegistration>,
    restored_states: HashMap<String, LiquidityRewardRestoreState>,
    order_store: Option<OrderStore>,
    market_store: Option<MarketStore>,
    simulation_enabled: bool,
    tick_size_map: TickSizeMap,
    notifier: Option<Notifier>,
    balance_cooldown: Duration,
}

impl LiquidityRewardFsmStrategy {
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
        let restored_count = restored_states.len();
        self.restored_states = restored_states
            .into_iter()
            .filter(|(token, _)| self.rules.contains_key(token))
            .collect();
        let skipped_count = restored_count.saturating_sub(self.restored_states.len());
        if skipped_count > 0 {
            warn!(
                restored_count,
                skipped_count,
                active_rule_count = self.rules.len(),
                "liquidity_reward_fsm 跳过当前规则外的历史恢复状态"
            );
        }
        self.order_store = order_store;
        self.simulation_enabled = simulation_enabled;
        self.tick_size_map = tick_size_map;
        self
    }

    pub fn with_market_store(mut self, market_store: MarketStore) -> Self {
        self.market_store = Some(market_store);
        self
    }

    pub fn with_notifier(mut self, notifier: Option<Notifier>) -> Self {
        self.notifier = notifier;
        self
    }

    pub fn with_balance_cooldown(mut self, cooldown: Duration) -> Self {
        self.balance_cooldown = cooldown;
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
                condition_id: None,
                pool_version: None,
            });
        }

        Self::from_rules(rules)
    }

    pub fn from_pool_entries(
        entries: Vec<ActiveRewardMarketPoolEntry>,
    ) -> anyhow::Result<Option<Self>> {
        let mut rules = Vec::new();
        for entry in entries {
            if entry.liquidity_reward_halted
                && entry.liquidity_reward_halted_pool_version.is_some()
                && entry.liquidity_reward_halted_pool_version == entry.pool_version
            {
                continue;
            }
            let Some(reward_max_spread_cents) = parse_pool_f64(
                entry.rewards_max_spread.as_deref(),
                &entry.condition_id,
                "rewards_max_spread",
            ) else {
                continue;
            };
            let Some(reward_min_size) = parse_pool_f64(
                entry.rewards_min_size.as_deref(),
                &entry.condition_id,
                "rewards_min_size",
            ) else {
                continue;
            };
            let Some(reward_daily_pool) = parse_pool_f64(
                entry.market_daily_reward.as_deref(),
                &entry.condition_id,
                "market_daily_reward",
            ) else {
                continue;
            };

            rules.push(LiquidityRewardRule {
                topic: Arc::from(DEFAULT_TOPIC),
                token1: entry.token1,
                token2: Some(entry.token2),
                reward_min_orders: None,
                reward_max_spread_cents: Some(reward_max_spread_cents),
                reward_min_size: Some(reward_min_size),
                reward_daily_pool: Some(reward_daily_pool),
                fixed_price: false,
                condition_id: Some(entry.condition_id),
                pool_version: entry.pool_version,
            });
        }

        Self::from_rules(rules)
    }

    pub fn from_rules(rules: Vec<LiquidityRewardRule>) -> anyhow::Result<Option<Self>> {
        if rules.is_empty() {
            return Ok(None);
        }
        let (rules, registration) = build_rules_and_registration(rules)?;
        Ok(Some(Self {
            rules: Arc::new(rules),
            registration: Arc::new(registration),
            restored_states: HashMap::new(),
            order_store: None,
            market_store: None,
            simulation_enabled: false,
            tick_size_map: TickSizeMap::default(),
            notifier: None,
            balance_cooldown: Duration::from_secs(60),
        }))
    }
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
    fsm: &TokenFsm,
    mid: Decimal,
    best_ask: Decimal,
    fixed_external_ask: Option<Decimal>,
    spread: Decimal,
    bids: &BTreeMap<u16, u32>,
    tick_size_map: &TickSizeMap,
) -> QuoteDecision {
    let default_tick = Decimal::try_from(0.01_f64).unwrap_or(Decimal::ONE);
    let tick = tick_size_map
        .get(token)
        .map(|value| *value)
        .unwrap_or(default_tick);
    let competitor_best_bid = competitor_best_bid(bids, fsm);
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
            action: if active_order(fsm).is_some() {
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

    if rule.fixed_price {
        if target_price <= Decimal::ZERO {
            warn!(token = %token, mid = %mid, spread_cents = ?rule.reward_max_spread_cents, price = %target_price, "liquidity_reward_fsm(fixed) 计算出的挂单价格无效");
            return QuoteDecision {
                action: if active_order(fsm).is_some() {
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
        let action = match active_order(fsm) {
            None => QuoteAction::PlaceOrReplace {
                price: target_price,
                reason: "no_order",
            },
            Some(active) => {
                if active.price != target_price {
                    if pending_replacement(fsm).is_some_and(|pending| pending.price == target_price)
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

    let non_best_cap = competitor_best_bid.map(|price| price - tick);

    if target_price <= Decimal::ZERO {
        warn!(token = %token, mid = %mid, spread_cents = ?rule.reward_max_spread_cents, tick = %tick, price = %target_price, "liquidity_reward_fsm 计算出的挂单价格无效");
        return QuoteDecision {
            action: if active_order(fsm).is_some() {
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
            action: if active_order(fsm).is_some() {
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
            action: if active_order(fsm).is_some() {
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

    let action = match active_order(fsm) {
        None => QuoteAction::PlaceOrReplace {
            price: desired_price,
            reason: "no_order",
        },
        Some(active) => {
            if active.price != desired_price {
                if pending_replacement(fsm).is_some_and(|pending| pending.price == desired_price) {
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

fn active_order(fsm: &TokenFsm) -> Option<&ActiveOrder> {
    match &fsm.quote {
        QuoteState::Active { order } | QuoteState::Canceling { order, .. } => Some(order),
        QuoteState::Halted { active, .. } => active.as_ref(),
        QuoteState::Idle => None,
    }
}

fn pending_replacement(fsm: &TokenFsm) -> Option<&PendingReplacement> {
    match &fsm.quote {
        QuoteState::Canceling {
            next: CancelNext::Replace(pending),
            ..
        } => Some(pending),
        _ => None,
    }
}

fn paired_token<'a>(token: &str, rule: &'a LiquidityRewardRule) -> Option<&'a str> {
    if token == rule.token1 {
        rule.token2.as_deref()
    } else {
        Some(rule.token1.as_str())
    }
}

fn removal_pair_token<'a>(
    token1: &'a str,
    token2: &'a str,
    rules: &HashMap<String, LiquidityRewardRule>,
) -> Option<&'a str> {
    if rules.contains_key(token1) {
        Some(token1)
    } else if rules.contains_key(token2) {
        Some(token2)
    } else {
        None
    }
}

fn halt_pair_fsm(
    trigger_token: &str,
    rules: &HashMap<String, LiquidityRewardRule>,
    fsms: &mut HashMap<String, TokenFsm>,
    reason: HaltReason,
    fill_unwind_size: Option<Decimal>,
    filled_local_order_id: Option<&str>,
    filled_remote_order_id: Option<&str>,
) -> Vec<Effect> {
    let Some(rule) = rules.get(trigger_token) else {
        return Vec::new();
    };
    let mut tokens = vec![trigger_token.to_string()];
    if let Some(paired) = paired_token(trigger_token, rule) {
        tokens.push(paired.to_string());
    }
    tokens.sort();
    tokens.dedup();

    let mut effects = Vec::new();
    if let (Some(condition_id), Some(pool_version)) = (&rule.condition_id, rule.pool_version) {
        effects.push(Effect::PersistPoolHalt {
            condition_id: condition_id.clone(),
            pool_version,
            reason: "liquidity_reward_halt",
        });
    }

    for token in tokens {
        let fsm = fsms
            .entry(token.clone())
            .or_insert_with(|| TokenFsm::empty(token.clone(), rule.topic.clone()));
        fsm.topic = rule.topic.clone();
        effects.extend(fsm.halt(reason.clone(), filled_local_order_id));
    }

    if let (Some(size), Some(local_order_id)) = (fill_unwind_size, filled_local_order_id) {
        if let Some(fsm) = fsms.get_mut(trigger_token) {
            fsm.mark_fill_unwind_intent(local_order_id, filled_remote_order_id, size);
        }
    }

    effects
}

fn apply_pool_removal_positions(
    snapshot: &crate::strategy::PositionSnapshot,
    fsms: &mut HashMap<String, TokenFsm>,
    tick_size_map: &TickSizeMap,
) -> Vec<Effect> {
    let mut effects = Vec::new();
    let tokens = fsms.keys().cloned().collect::<Vec<_>>();
    for token in tokens {
        let Some(fsm) = fsms.get_mut(token.as_str()) else {
            continue;
        };
        if !matches!(fsm.risk, RiskState::PoolRemovalPending { .. }) {
            continue;
        }
        let size = snapshot
            .by_asset
            .get(token.as_str())
            .map(|position| position.size)
            .unwrap_or(Decimal::ZERO);
        fsm.update_pool_removal_position(size);
        effects.extend(fsm.submit_pool_removal_unwind_if_ready(tick_size_map));
    }
    effects
}

fn apply_fill_unwind_positions(
    snapshot: &crate::strategy::PositionSnapshot,
    fsms: &mut HashMap<String, TokenFsm>,
    tick_size_map: &TickSizeMap,
) -> Vec<Effect> {
    let mut effects = Vec::new();
    let tokens = fsms.keys().cloned().collect::<Vec<_>>();
    for token in tokens {
        let Some(fsm) = fsms.get_mut(token.as_str()) else {
            continue;
        };
        if fsm.pending_fill_unwind.is_none() {
            continue;
        }
        let size = snapshot
            .by_asset
            .get(token.as_str())
            .map(|position| position.size)
            .unwrap_or(Decimal::ZERO);
        fsm.update_fill_unwind_position(size);
        effects.extend(fsm.submit_fill_unwind_if_ready(tick_size_map));
    }
    effects
}

fn paired_external_ask(
    token: &str,
    rule: &LiquidityRewardRule,
    fsms: &HashMap<String, TokenFsm>,
) -> Option<Decimal> {
    let paired = paired_token(token, rule)?;
    let paired_fsm = fsms.get(paired)?;
    let paired_bids = paired_fsm.market.bids.as_deref()?;
    let paired_external_bid = competitor_best_bid(paired_bids, paired_fsm)?;
    let external_ask = Decimal::ONE - paired_external_bid;
    (external_ask > Decimal::ZERO).then_some(external_ask)
}

fn competitor_best_bid(bids: &BTreeMap<u16, u32>, fsm: &TokenFsm) -> Option<Decimal> {
    for (&price, &size) in bids.iter().rev() {
        let mut remaining = size as i64;
        if let Some(order) = active_order(fsm) {
            if scaled_price(order.price) == price {
                remaining -= scaled_size(order.order_size);
            }
        }
        if let Some(order) = pending_replacement(fsm) {
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

fn next_buy_order_id(token: &str, ts: u64) -> String {
    let seq = ORDER_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{}-{}-buy-{}", token, ts, seq)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn is_not_enough_balance_error(reason: Option<&str>) -> bool {
    reason.is_some_and(|reason| {
        reason
            .to_ascii_lowercase()
            .contains("not enough balance / allowance")
    })
}

fn fsm_from_restore(token: String, restored: LiquidityRewardRestoreState) -> TokenFsm {
    let quote = if let Some(order_id) = restored.buy.active_local_order_id.clone() {
        let order = ActiveOrder {
            order_id,
            price: restored.buy.active_price.unwrap_or(Decimal::ZERO),
            order_size: restored.buy.active_order_size.unwrap_or(Decimal::ZERO),
        };
        if let Some(pending_order_id) = restored.buy.pending_local_order_id.clone() {
            QuoteState::Canceling {
                order,
                next: CancelNext::Replace(PendingReplacement {
                    order_id: pending_order_id,
                    price: restored.buy.pending_price.unwrap_or(Decimal::ZERO),
                    order_size: restored.buy.pending_order_size.unwrap_or(Decimal::ZERO),
                    mid: restored.buy.pending_mid.unwrap_or(Decimal::ZERO),
                }),
            }
        } else if restored.buy.cancel_requested {
            QuoteState::Canceling {
                order,
                next: CancelNext::Wait,
            }
        } else {
            QuoteState::Active { order }
        }
    } else {
        QuoteState::Idle
    };

    TokenFsm {
        token,
        topic: restored.topic,
        quote,
        risk: RiskState::Normal,
        market: MarketSnapshot {
            mid: restored.last_mid,
            best_bid: restored.last_best_bid,
            best_ask: restored.last_best_ask,
            bids: None,
        },
        pending_unwinds: HashMap::new(),
        pending_fill_unwind: None,
    }
}

fn order_belongs_to_quote(fsm: &TokenFsm, local_order_id: &str) -> bool {
    match &fsm.quote {
        QuoteState::Active { order } => order.order_id == local_order_id,
        QuoteState::Canceling { order, next } => {
            order.order_id == local_order_id
                || matches!(
                    next,
                    CancelNext::Replace(pending) if pending.order_id == local_order_id
                )
        }
        QuoteState::Halted { active, .. } => active
            .as_ref()
            .is_some_and(|order| order.order_id == local_order_id),
        QuoteState::Idle => false,
    }
}

fn execute_effects(
    effects: Vec<Effect>,
    simulated: bool,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
    market_store: Option<&MarketStore>,
) {
    for effect in effects {
        match effect {
            Effect::PlaceBuy {
                token,
                topic,
                local_order_id,
                mid,
                price,
                order_size,
            } => {
                if let Err(error) = order_tx.try_send(OrderSignal::LiquidityRewardPlace {
                    strategy: Arc::from("liquidity_reward"),
                    topic,
                    token,
                    mid,
                    side: crate::strategy::QuoteSide::Buy,
                    price,
                    order_size,
                    local_order_id,
                    simulated,
                }) {
                    warn!(error = %error, "liquidity_reward_fsm 发送挂单信号失败");
                }
            }
            Effect::CancelBuy {
                token,
                topic,
                local_order_id,
            } => {
                if let Err(error) = order_tx.try_send(OrderSignal::LiquidityRewardCancel {
                    strategy: Arc::from("liquidity_reward"),
                    topic,
                    token,
                    side: crate::strategy::QuoteSide::Buy,
                    active_local_order_id: local_order_id,
                    simulated,
                }) {
                    warn!(error = %error, "liquidity_reward_fsm 发送撤单信号失败");
                }
            }
            Effect::MarketSell {
                token,
                topic,
                local_order_id,
                price,
                order_size,
            } => {
                if let Err(error) = order_tx.try_send(OrderSignal::LiquidityRewardMarketSell {
                    strategy: Arc::from("liquidity_reward"),
                    topic,
                    token,
                    price,
                    order_size,
                    local_order_id,
                    simulated,
                }) {
                    warn!(error = %error, "liquidity_reward_fsm 发送清仓卖单信号失败");
                }
            }
            Effect::PersistPoolHalt {
                condition_id,
                pool_version,
                reason,
            } => {
                let Some(store) = market_store else { continue };
                let halted_at_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                if let Err(error) = store.halt_liquidity_reward_pool_entry(
                    &condition_id,
                    pool_version,
                    reason,
                    halted_at_ms,
                ) {
                    warn!(condition_id = %condition_id, pool_version, error = %error, "liquidity_reward_fsm 写入池子 halt 状态失败");
                }
            }
        }
    }
}

fn persist_token_state(order_store: Option<&OrderStore>, fsm: &TokenFsm) {
    let Some(store) = order_store else { return };

    if let Err(error) = store.upsert_liquidity_reward_shared_state(
        &fsm.token,
        fsm.topic.as_ref(),
        fsm.market.mid,
        fsm.market.best_bid,
        fsm.market.best_ask,
        Decimal::ZERO,
    ) {
        warn!(token = %fsm.token, error = %error, "liquidity_reward_fsm 持久化共享策略状态失败");
    }

    let (active, pending, cancel_requested) = match &fsm.quote {
        QuoteState::Idle => (None, None, false),
        QuoteState::Active { order } => (Some(order), None, false),
        QuoteState::Canceling { order, next } => {
            let pending = match next {
                CancelNext::Wait => None,
                CancelNext::Replace(pending) => Some(pending),
            };
            (Some(order), pending, true)
        }
        QuoteState::Halted {
            active,
            cancel_requested,
            ..
        } => (active.as_ref(), None, *cancel_requested),
    };

    if let Err(error) = store.upsert_liquidity_reward_side_state(
        &fsm.token,
        crate::strategy::QuoteSide::Buy,
        active.map(|order| order.order_id.as_str()),
        pending.map(|pending| pending.order_id.as_str()),
        pending.map(|pending| pending.price),
        pending.map(|pending| pending.order_size),
        pending.map(|pending| pending.mid),
        None,
        cancel_requested,
    ) {
        warn!(token = %fsm.token, error = %error, "liquidity_reward_fsm 持久化策略状态失败");
    }
}

fn parse_pool_f64(value: Option<&str>, condition_id: &str, field: &str) -> Option<f64> {
    let Some(value) = value else {
        warn!(condition_id = %condition_id, field, "liquidity_reward_fsm DB 池字段缺失，跳过市场");
        return None;
    };
    match value.parse::<f64>() {
        Ok(value) => Some(value),
        Err(error) => {
            warn!(condition_id = %condition_id, field, value, error = %error, "liquidity_reward_fsm DB 池字段无效，跳过市场");
            None
        }
    }
}

// 一条市场规则需要同时注册 token1/token2，确保任一侧事件都能路由回本策略。
fn build_rules_and_registration(
    rules: Vec<LiquidityRewardRule>,
) -> anyhow::Result<(HashMap<String, LiquidityRewardRule>, StrategyRegistration)> {
    let mut by_token = HashMap::new();
    let mut topic_tokens: HashMap<Arc<str>, Vec<String>> = HashMap::new();
    let mut related_tokens = HashSet::new();

    for rule in rules {
        if rule.token1.is_empty() {
            continue;
        }
        let topic = rule.topic.clone();
        related_tokens.insert(rule.token1.clone());
        topic_tokens
            .entry(topic.clone())
            .or_default()
            .push(rule.token1.clone());
        by_token.insert(rule.token1.clone(), rule.clone());

        if let Some(token2) = rule.token2.as_ref().filter(|token| !token.is_empty()) {
            related_tokens.insert(token2.clone());
            topic_tokens.entry(topic).or_default().push(token2.clone());
            by_token.insert(token2.clone(), rule);
        }
    }

    if by_token.is_empty() {
        return Ok((by_token, empty_registration()));
    }

    let mut topics: Vec<Arc<str>> = topic_tokens.keys().cloned().collect();
    topics.sort();

    let topic_token_regs = topic_tokens
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

    let mut related_tokens = related_tokens.into_iter().collect::<Vec<_>>();
    related_tokens.sort();

    Ok((
        by_token,
        StrategyRegistration {
            name: Arc::from("liquidity_reward"),
            topics: Arc::<[Arc<str>]>::from(topics),
            topic_tokens: Arc::<[TopicRegistration]>::from(topic_token_regs),
            related_tokens: Arc::<[String]>::from(related_tokens),
        },
    ))
}

fn empty_registration() -> StrategyRegistration {
    StrategyRegistration {
        name: Arc::from("liquidity_reward"),
        topics: Arc::<[Arc<str>]>::from(Vec::<Arc<str>>::new()),
        topic_tokens: Arc::<[TopicRegistration]>::from(Vec::<TopicRegistration>::new()),
        related_tokens: Arc::<[String]>::from(Vec::<String>::new()),
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

impl Strategy for LiquidityRewardFsmStrategy {
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
        let market_store = self.market_store.clone();
        let restored_states = self.restored_states;
        let simulation_enabled = self.simulation_enabled;
        let tick_size_map = self.tick_size_map.clone();
        let notifier = self.notifier.clone();
        let balance_cooldown = self.balance_cooldown;

        tokio::spawn(async move {
            let mut fsms: HashMap<String, TokenFsm> = restored_states
                .into_iter()
                .map(|(token, restored)| (token.clone(), fsm_from_restore(token, restored)))
                .collect();
            let mut latest_positions: Option<Arc<crate::strategy::PositionSnapshot>> = None;

            while let Some(event) = rx.recv().await {
                match event {
                    StrategyEvent::Market(event) => {
                        let Some(rule) = rules.get(event.asset_id.as_ref()) else {
                            continue;
                        };
                        let token = event.asset_id.as_ref().to_string();
                        let fixed_external_ask = if rule.fixed_price {
                            paired_external_ask(&token, rule, &fsms)
                        } else {
                            None
                        };
                        let fsm = fsms
                            .entry(token.clone())
                            .or_insert_with(|| TokenFsm::empty(token.clone(), rule.topic.clone()));

                        let bid =
                            Decimal::from(event.book.best_bid_price) / Decimal::from(PRICE_SCALE);
                        let ask =
                            Decimal::from(event.book.best_ask_price) / Decimal::from(PRICE_SCALE);
                        let mid = (bid + ask) / Decimal::TWO;
                        fsm.topic = rule.topic.clone();
                        fsm.market.mid = Some(mid);
                        fsm.market.best_bid = Some(bid);
                        fsm.market.best_ask = Some(ask);
                        fsm.market.bids = Some(event.book.bids.clone());

                        let mut effects = fsm.retry_halted_cancel();
                        effects.extend(fsm.submit_pool_removal_unwind_if_ready(&tick_size_map));
                        effects.extend(fsm.submit_fill_unwind_if_ready(&tick_size_map));
                        if matches!(fsm.quote, QuoteState::Halted { .. }) {
                            execute_effects(
                                effects,
                                simulation_enabled,
                                &order_tx,
                                market_store.as_ref(),
                            );
                            persist_token_state(order_store.as_ref(), fsm);
                            continue;
                        }

                        let Some(order_size) = rule
                            .reward_min_size
                            .and_then(|size| Decimal::try_from(size).ok())
                            .filter(|size| *size > Decimal::ZERO)
                        else {
                            warn!(token = %token, "liquidity_reward_fsm reward_min_size 未配置或无效，跳过挂单");
                            persist_token_state(order_store.as_ref(), fsm);
                            continue;
                        };
                        let spread = rule
                            .reward_max_spread_cents
                            .and_then(|cents| Decimal::try_from(cents / 100.0).ok())
                            .unwrap_or(Decimal::ZERO);
                        let decision = quote_decision(
                            rule,
                            &token,
                            fsm,
                            mid,
                            ask,
                            fixed_external_ask,
                            spread,
                            &event.book.bids,
                            &tick_size_map,
                        );

                        match decision.action {
                            QuoteAction::PlaceOrReplace { price, .. } => {
                                let order_id = next_buy_order_id(&token, event.book.timestamp_ms);
                                effects.extend(match &fsm.quote {
                                    QuoteState::Active { .. } => {
                                        fsm.stage_replacement(PendingReplacement {
                                            order_id,
                                            price,
                                            order_size,
                                            mid,
                                        })
                                    }
                                    QuoteState::Idle => {
                                        fsm.place_buy(order_id, mid, price, order_size)
                                    }
                                    _ => Vec::new(),
                                });
                            }
                            QuoteAction::CancelOnly { .. } => {
                                effects.extend(fsm.cancel_wait());
                            }
                            QuoteAction::Wait { .. } => {}
                        }

                        execute_effects(
                            effects,
                            simulation_enabled,
                            &order_tx,
                            market_store.as_ref(),
                        );
                        persist_token_state(order_store.as_ref(), fsm);
                    }
                    StrategyEvent::OrderStatus(status_event) => {
                        let Some(fsm) = fsms.get_mut(status_event.token.as_str()) else {
                            continue;
                        };
                        let status = status_event.status.as_ref();
                        if fsm
                            .pending_unwinds
                            .contains_key(&status_event.local_order_id)
                        {
                            let effects = match status {
                                "canceled" | "rejected" => fsm
                                    .on_unwind_terminal(&status_event.local_order_id)
                                    .into_iter()
                                    .filter_map(|unwind| {
                                        let remaining = unwind.order_size - unwind.matched_size;
                                        (remaining > Decimal::ZERO).then_some(remaining)
                                    })
                                    .flat_map(|remaining| {
                                        fsm.submit_unwind(
                                            remaining,
                                            &tick_size_map,
                                            "unwind-remaining",
                                            UnwindKind::Remaining,
                                        )
                                    })
                                    .collect(),
                                "open" => Vec::new(),
                                "filled" => {
                                    if let Some(unwind) =
                                        fsm.on_unwind_terminal(&status_event.local_order_id)
                                    {
                                        if unwind.kind == UnwindKind::Fill {
                                            fsm.clear_fill_unwind_intent();
                                        }
                                    }
                                    Vec::new()
                                }
                                "failed" => {
                                    let unwind =
                                        fsm.on_unwind_terminal(&status_event.local_order_id);
                                    if unwind.is_some_and(|unwind| unwind.kind == UnwindKind::Fill)
                                    {
                                        let reason = status_event.reason.as_deref().unwrap_or("-");
                                        if is_not_enough_balance_error(
                                            status_event.reason.as_deref(),
                                        ) && fsm.pending_fill_unwind.as_ref().is_some_and(
                                            |intent| intent.attempts < FILL_UNWIND_MAX_ATTEMPTS,
                                        ) {
                                            fsm.retain_fill_unwind_after_balance_failure(
                                                reason,
                                                balance_cooldown,
                                            );
                                        } else if let Some(notification) =
                                            fsm.manual_attention_notification(reason)
                                        {
                                            if let Some(notifier) = notifier.as_ref() {
                                                notifier.try_notify(
                                                    NotificationEvent::LiquidityRewardManualAttention(
                                                        notification,
                                                    ),
                                                );
                                            }
                                        }
                                    }
                                    Vec::new()
                                }
                                _ => Vec::new(),
                            };
                            execute_effects(
                                effects,
                                simulation_enabled,
                                &order_tx,
                                market_store.as_ref(),
                            );
                            persist_token_state(order_store.as_ref(), fsm);
                            continue;
                        }
                        if !order_belongs_to_quote(fsm, &status_event.local_order_id) {
                            continue;
                        }
                        match status {
                            "filled" => {
                                let size = active_order(fsm)
                                    .map(|order| order.order_size)
                                    .unwrap_or(Decimal::ZERO);
                                let token = status_event.token.clone();
                                let local_order_id = status_event.local_order_id.clone();
                                let _ = fsm;
                                let effects = halt_pair_fsm(
                                    &token,
                                    &rules,
                                    &mut fsms,
                                    HaltReason::Fill,
                                    Some(size),
                                    Some(local_order_id.as_str()),
                                    None,
                                );
                                execute_effects(
                                    effects,
                                    simulation_enabled,
                                    &order_tx,
                                    market_store.as_ref(),
                                );
                                if let Some(fsm) = fsms.get(token.as_str()) {
                                    persist_token_state(order_store.as_ref(), fsm);
                                }
                            }
                            "canceled" | "rejected" => {
                                let effects = fsm.on_cancel_confirmed();
                                execute_effects(
                                    effects,
                                    simulation_enabled,
                                    &order_tx,
                                    market_store.as_ref(),
                                );
                                persist_token_state(order_store.as_ref(), fsm);
                            }
                            "failed" => {
                                fsm.on_cancel_failed();
                                persist_token_state(order_store.as_ref(), fsm);
                            }
                            "cancel_failed" => {
                                fsm.on_halted_cancel_failed();
                                persist_token_state(order_store.as_ref(), fsm);
                            }
                            _ => {}
                        }
                    }
                    StrategyEvent::OrderFill(fill_event) => {
                        if let Some(fsm) = fsms.get_mut(fill_event.token.as_str()) {
                            if fsm.on_unwind_fill(
                                &fill_event.local_order_id,
                                fill_event.total_matched_size,
                            ) {
                                continue;
                            }
                        }
                        if fill_event.strategy.as_ref() != "liquidity_reward"
                            || fill_event.side != crate::strategy::QuoteSide::Buy
                        {
                            continue;
                        }
                        if fsms.get(fill_event.token.as_str()).is_some_and(|fsm| {
                            matches!(fsm.risk, RiskState::PoolRemovalPending { .. })
                        }) {
                            continue;
                        }
                        let effects = halt_pair_fsm(
                            &fill_event.token,
                            &rules,
                            &mut fsms,
                            HaltReason::Fill,
                            Some(fill_event.total_matched_size),
                            Some(fill_event.local_order_id.as_str()),
                            fill_event.remote_order_id.as_deref(),
                        );
                        execute_effects(
                            effects,
                            simulation_enabled,
                            &order_tx,
                            market_store.as_ref(),
                        );
                        if let Some(fsm) = fsms.get(fill_event.token.as_str()) {
                            persist_token_state(order_store.as_ref(), fsm);
                        }
                    }
                    StrategyEvent::TradeConfirmed(trade_event) => {
                        let Some(fsm) = fsms.get_mut(trade_event.token.as_str()) else {
                            continue;
                        };
                        if !fsm.mark_fill_trade_confirmed(&trade_event) {
                            continue;
                        }
                        if let Some(snapshot) = latest_positions.as_ref() {
                            let size = snapshot
                                .by_asset
                                .get(trade_event.token.as_str())
                                .map(|position| position.size)
                                .unwrap_or(Decimal::ZERO);
                            fsm.update_fill_unwind_position(size);
                        }
                        let effects = fsm.submit_fill_unwind_if_ready(&tick_size_map);
                        execute_effects(
                            effects,
                            simulation_enabled,
                            &order_tx,
                            market_store.as_ref(),
                        );
                        persist_token_state(order_store.as_ref(), fsm);
                    }
                    StrategyEvent::RewardPoolRemoval(removal_event) => {
                        let Some(token) = removal_pair_token(
                            &removal_event.token1,
                            &removal_event.token2,
                            &rules,
                        ) else {
                            continue;
                        };
                        let mut effects = halt_pair_fsm(
                            token,
                            &rules,
                            &mut fsms,
                            HaltReason::PoolRemoval,
                            None,
                            None,
                            None,
                        );
                        for pool_token in [&removal_event.token1, &removal_event.token2] {
                            if let Some(rule) = rules.get(pool_token.as_str()) {
                                let fsm = fsms.entry(pool_token.clone()).or_insert_with(|| {
                                    TokenFsm::empty(pool_token.clone(), rule.topic.clone())
                                });
                                fsm.mark_pool_removal_unwind();
                            }
                        }
                        if let Some(snapshot) = latest_positions.as_ref() {
                            effects.extend(apply_pool_removal_positions(
                                snapshot,
                                &mut fsms,
                                &tick_size_map,
                            ));
                        }
                        execute_effects(
                            effects,
                            simulation_enabled,
                            &order_tx,
                            market_store.as_ref(),
                        );
                    }
                    StrategyEvent::Positions(positions_event) => {
                        latest_positions = Some(positions_event.snapshot);
                        let snapshot = latest_positions
                            .as_ref()
                            .expect("latest positions just set");
                        let mut effects =
                            apply_pool_removal_positions(snapshot, &mut fsms, &tick_size_map);
                        effects.extend(apply_fill_unwind_positions(
                            snapshot,
                            &mut fsms,
                            &tick_size_map,
                        ));
                        execute_effects(
                            effects,
                            simulation_enabled,
                            &order_tx,
                            market_store.as_ref(),
                        );
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(value: f64) -> Decimal {
        Decimal::try_from(value).unwrap()
    }

    fn token_fsm() -> TokenFsm {
        TokenFsm::empty("token1".to_string(), Arc::from("liquidity_reward"))
    }

    #[test]
    fn idle_place_buy_moves_to_active_and_emits_place() {
        let mut fsm = token_fsm();
        let effects = fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));

        assert_eq!(
            fsm.quote,
            QuoteState::Active {
                order: ActiveOrder {
                    order_id: "buy-1".to_string(),
                    price: dec(0.49),
                    order_size: dec(100.0),
                }
            }
        );
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::PlaceBuy { token, local_order_id, price, order_size, .. }
                if token == "token1"
                    && local_order_id == "buy-1"
                    && *price == dec(0.49)
                    && *order_size == dec(100.0)
        ));
    }

    #[test]
    fn active_cancel_wait_moves_to_canceling_and_emits_cancel() {
        let mut fsm = token_fsm();
        fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));

        let effects = fsm.cancel_wait();

        assert!(matches!(
            fsm.quote,
            QuoteState::Canceling {
                next: CancelNext::Wait,
                ..
            }
        ));
        assert!(matches!(
            &effects[0],
            Effect::CancelBuy { token, local_order_id, .. }
                if token == "token1" && local_order_id == "buy-1"
        ));
    }

    #[test]
    fn cancel_confirmed_with_replacement_places_pending_order() {
        let mut fsm = token_fsm();
        fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));
        fsm.stage_replacement(PendingReplacement {
            order_id: "buy-2".to_string(),
            price: dec(0.48),
            order_size: dec(100.0),
            mid: dec(0.5),
        });

        let effects = fsm.on_cancel_confirmed();

        assert_eq!(
            fsm.quote,
            QuoteState::Active {
                order: ActiveOrder {
                    order_id: "buy-2".to_string(),
                    price: dec(0.48),
                    order_size: dec(100.0),
                }
            }
        );
        assert!(matches!(
            &effects[0],
            Effect::PlaceBuy { local_order_id, price, .. }
                if local_order_id == "buy-2" && *price == dec(0.48)
        ));
    }

    #[test]
    fn cancel_failed_restores_active_order() {
        let mut fsm = token_fsm();
        fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));
        fsm.cancel_wait();

        fsm.on_cancel_failed();

        assert_eq!(
            fsm.quote,
            QuoteState::Active {
                order: ActiveOrder {
                    order_id: "buy-1".to_string(),
                    price: dec(0.49),
                    order_size: dec(100.0),
                }
            }
        );
    }

    #[test]
    fn halt_active_keeps_active_and_emits_cancel() {
        let mut fsm = token_fsm();
        fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));

        let effects = fsm.halt(HaltReason::PoolRemoval, None);

        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::CancelBuy { local_order_id, .. } if local_order_id == "buy-1"
        ));
        assert!(matches!(
            fsm.quote,
            QuoteState::Halted {
                active: Some(_),
                cancel_requested: true,
                reason: HaltReason::PoolRemoval
            }
        ));
    }

    #[test]
    fn halt_filled_order_does_not_emit_cancel_but_remembers_halted() {
        let mut fsm = token_fsm();
        fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));

        let effects = fsm.halt(HaltReason::Fill, Some("buy-1"));

        assert!(effects.is_empty());
        assert!(matches!(
            fsm.quote,
            QuoteState::Halted {
                active: None,
                cancel_requested: false,
                reason: HaltReason::Fill
            }
        ));
    }

    #[test]
    fn fill_unwind_intent_records_trigger_order_and_size() {
        let mut fsm = token_fsm();

        fsm.mark_fill_unwind_intent("buy-1", Some("remote-buy-1"), dec(20.0));
        fsm.mark_fill_unwind_intent("buy-1", None, dec(10.0));

        let created_at_ms = fsm
            .pending_fill_unwind
            .as_ref()
            .expect("intent should be recorded")
            .created_at_ms;
        assert_eq!(
            fsm.pending_fill_unwind,
            Some(FillUnwindIntent {
                trigger_local_order_id: "buy-1".to_string(),
                trigger_remote_order_id: Some("remote-buy-1".to_string()),
                remaining_size: dec(20.0),
                trade_confirmed: false,
                position_visible_size: None,
                attempts: 0,
                created_at_ms,
                next_retry_after_ms: None,
                last_error: None,
                manual_notified: false,
            })
        );
    }

    #[test]
    fn fill_unwind_balance_failure_keeps_intent_for_retry() {
        let mut fsm = token_fsm();
        fsm.market.best_bid = Some(dec(0.49));
        fsm.mark_fill_unwind_intent("buy-1", Some("remote-buy-1"), dec(20.0));
        fsm.mark_fill_trade_confirmed(&confirmed_trade("token1", "remote-buy-1"));
        fsm.update_fill_unwind_position(dec(20.0));

        let first = fsm.submit_fill_unwind_if_ready(&Arc::new(dashmap::DashMap::new()));
        assert_eq!(first.len(), 1);
        let first_id = match &first[0] {
            Effect::MarketSell { local_order_id, .. } => local_order_id.clone(),
            other => panic!("expected market sell, got {other:?}"),
        };
        let unwind = fsm
            .on_unwind_terminal(&first_id)
            .expect("pending unwind should exist");
        assert_eq!(unwind.kind, UnwindKind::Fill);
        fsm.retain_fill_unwind_after_balance_failure(
            "not enough balance / allowance",
            Duration::ZERO,
        );

        let retry = fsm.submit_fill_unwind_if_ready(&Arc::new(dashmap::DashMap::new()));
        assert_eq!(retry.len(), 1);
        assert!(matches!(
            &retry[0],
            Effect::MarketSell { order_size, .. } if *order_size == Decimal::from(20)
        ));
        assert_eq!(
            fsm.pending_fill_unwind
                .as_ref()
                .expect("intent should remain")
                .attempts,
            2
        );
    }

    #[test]
    fn fill_unwind_manual_attention_notifies_once() {
        let mut fsm = token_fsm();
        fsm.mark_fill_unwind_intent("buy-1", Some("remote-buy-1"), dec(20.0));
        let intent = fsm
            .pending_fill_unwind
            .as_mut()
            .expect("intent should be recorded");
        intent.position_visible_size = Some(dec(15.0));
        intent.attempts = FILL_UNWIND_MAX_ATTEMPTS;

        let notification = fsm
            .manual_attention_notification("not enough balance / allowance")
            .expect("first manual attention should notify");
        assert_eq!(notification.token, "token1");
        assert_eq!(notification.trigger_local_order_id, "buy-1");
        assert_eq!(
            notification.trigger_remote_order_id.as_deref(),
            Some("remote-buy-1")
        );
        assert_eq!(notification.remaining_size, dec(20.0));
        assert_eq!(notification.visible_position_size, Some(dec(15.0)));
        assert_eq!(notification.attempts, FILL_UNWIND_MAX_ATTEMPTS);
        assert!(
            fsm.manual_attention_notification("not enough balance / allowance")
                .is_none()
        );
    }

    #[test]
    fn halted_cancel_failed_reopens_retry_window() {
        let mut fsm = token_fsm();
        fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));
        fsm.halt(HaltReason::PoolRemoval, None);

        fsm.on_halted_cancel_failed();
        let retry = fsm.retry_halted_cancel();

        assert_eq!(retry.len(), 1);
        assert!(matches!(
            &retry[0],
            Effect::CancelBuy { local_order_id, .. } if local_order_id == "buy-1"
        ));
    }

    #[test]
    fn pool_removal_position_size_is_recorded_until_unwind_in_flight() {
        let mut fsm = token_fsm();
        fsm.mark_pool_removal_unwind();
        fsm.update_pool_removal_position(dec(7.5));

        assert_eq!(
            fsm.risk,
            RiskState::PoolRemovalPending {
                position_size: Some(dec(7.5)),
                unwind_in_flight: false,
            }
        );
    }

    #[test]
    fn snaps_unwind_size_to_two_decimal_places() {
        assert_eq!(
            snap_unwind_size_to_lot("140.836205".parse::<Decimal>().unwrap()),
            "140.83".parse::<Decimal>().unwrap()
        );
    }

    #[test]
    fn submit_unwind_uses_lot_adjusted_size() {
        let mut fsm = token_fsm();
        fsm.market.best_bid = Some(dec(0.71));

        let effects = fsm.submit_unwind(
            "140.836205".parse::<Decimal>().unwrap(),
            &Arc::new(dashmap::DashMap::new()),
            "unwind",
            UnwindKind::Fill,
        );

        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::MarketSell { order_size, .. }
                if *order_size == "140.83".parse::<Decimal>().unwrap()
        ));
        let pending = fsm
            .pending_unwinds
            .values()
            .next()
            .expect("pending unwind should be recorded");
        assert_eq!(pending.order_size, "140.83".parse::<Decimal>().unwrap());
    }

    #[tokio::test]
    async fn fsm_strategy_places_initial_quote_on_market() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![test_rule()])
            .expect("strategy should build")
            .expect("strategy should exist");
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        event_tx
            .send(StrategyEvent::Market(crate::strategy::MarketEvent {
                topic: Arc::from(DEFAULT_TOPIC),
                asset_id: Arc::from("token1"),
                book: quoteable_book(),
            }))
            .await
            .expect("market event should send");

        let signal = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected quote signal")
            .expect("order channel should remain open");
        assert!(matches!(
            signal,
            OrderSignal::LiquidityRewardPlace { token, price, order_size, .. }
                if token == "token1" && price == dec(0.48) && order_size == dec(100.0)
        ));

        handle.abort();
    }

    #[tokio::test]
    async fn fsm_strategy_does_not_quote_when_halted() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![test_rule()])
            .expect("strategy should build")
            .expect("strategy should exist");
        let mut restored_states = HashMap::new();
        restored_states.insert("token1".to_string(), restore_state("buy-1"));
        let strategy = strategy.with_restore_state(
            restored_states,
            None,
            false,
            Arc::new(dashmap::DashMap::new()),
        );
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        event_tx
            .send(StrategyEvent::OrderStatus(
                crate::strategy::OrderStatusEvent {
                    token: "token1".to_string(),
                    local_order_id: "buy-1".to_string(),
                    status: Arc::from("filled"),
                    reason: None,
                },
            ))
            .await
            .expect("filled status should send");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), order_rx.recv())
                .await
                .is_err(),
            "filled status must not submit unwind before confirmation"
        );
        event_tx
            .send(StrategyEvent::Market(crate::strategy::MarketEvent {
                topic: Arc::from(DEFAULT_TOPIC),
                asset_id: Arc::from("token1"),
                book: quoteable_book(),
            }))
            .await
            .expect("market event should send");

        assert!(
            tokio::time::timeout(Duration::from_millis(100), order_rx.recv())
                .await
                .is_err(),
            "halted fsm must not submit new quote"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn pending_unwind_open_status_keeps_remaining_retry_available() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![test_rule()])
            .expect("strategy should build")
            .expect("strategy should exist");
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        event_tx
            .send(StrategyEvent::Market(crate::strategy::MarketEvent {
                topic: Arc::from(DEFAULT_TOPIC),
                asset_id: Arc::from("token1"),
                book: quoteable_book(),
            }))
            .await
            .expect("market event should send");
        tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected initial quote")
            .expect("order channel should remain open");
        event_tx
            .send(StrategyEvent::Positions(
                crate::strategy::PositionsUpdateEvent {
                    snapshot: Arc::new(crate::strategy::PositionSnapshot {
                        by_asset: Arc::new(HashMap::from([(
                            "token1".to_string(),
                            position("token1", 100.0, 0.49),
                        )])),
                    }),
                    changed_assets: Arc::from(["token1".to_string()]),
                },
            ))
            .await
            .expect("positions event should send");
        event_tx
            .send(StrategyEvent::RewardPoolRemoval(
                crate::strategy::RewardPoolRemovalEvent {
                    condition_id: "0xabc".to_string(),
                    token1: "token1".to_string(),
                    token2: "token2".to_string(),
                    reason: "token1_spread_gt_threshold".to_string(),
                },
            ))
            .await
            .expect("pool removal event should send");

        let mut unwind_id = None;
        for _ in 0..2 {
            let signal = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
                .await
                .expect("expected pool removal signal")
                .expect("order channel should remain open");
            if let OrderSignal::LiquidityRewardMarketSell {
                local_order_id,
                order_size,
                ..
            } = signal
            {
                assert_eq!(order_size, dec(100.0));
                unwind_id = Some(local_order_id);
            }
        }
        let unwind_id = unwind_id.expect("pool removal unwind should submit");

        event_tx
            .send(StrategyEvent::OrderStatus(
                crate::strategy::OrderStatusEvent {
                    token: "token1".to_string(),
                    local_order_id: unwind_id.clone(),
                    status: Arc::from("open"),
                    reason: None,
                },
            ))
            .await
            .expect("open status should send");
        event_tx
            .send(StrategyEvent::OrderFill(crate::strategy::OrderFillEvent {
                strategy: Arc::from("liquidity_reward"),
                topic: Some(Arc::from(DEFAULT_TOPIC)),
                token: "token1".to_string(),
                local_order_id: unwind_id.clone(),
                remote_order_id: None,
                side: crate::strategy::QuoteSide::Sell,
                delta_size: dec(40.0),
                total_matched_size: dec(40.0),
            }))
            .await
            .expect("unwind fill should send");
        event_tx
            .send(StrategyEvent::OrderStatus(
                crate::strategy::OrderStatusEvent {
                    token: "token1".to_string(),
                    local_order_id: unwind_id,
                    status: Arc::from("canceled"),
                    reason: None,
                },
            ))
            .await
            .expect("canceled status should send");

        let retry = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected retry for remaining unwind")
            .expect("order channel should remain open");
        assert!(matches!(
            retry,
            OrderSignal::LiquidityRewardMarketSell { order_size, .. } if order_size == dec(60.0)
        ));

        handle.abort();
    }

    #[tokio::test]
    async fn reward_pool_removal_halts_pair_cancels_orders_and_unwinds_positions() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![paired_rule(Some("token2"))])
            .expect("strategy should build")
            .expect("strategy should exist");
        let mut restored_states = HashMap::new();
        restored_states.insert("token1".to_string(), restore_state("token1-active"));
        restored_states.insert("token2".to_string(), restore_state("token2-active"));
        let strategy = strategy.with_restore_state(
            restored_states,
            None,
            false,
            Arc::new(dashmap::DashMap::new()),
        );
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        event_tx
            .send(StrategyEvent::Positions(
                crate::strategy::PositionsUpdateEvent {
                    snapshot: Arc::new(crate::strategy::PositionSnapshot {
                        by_asset: Arc::new(HashMap::from([(
                            "token1".to_string(),
                            position("token1", 12.5, 0.44),
                        )])),
                    }),
                    changed_assets: Arc::from(["token1".to_string()]),
                },
            ))
            .await
            .expect("positions event should send");
        event_tx
            .send(StrategyEvent::RewardPoolRemoval(
                crate::strategy::RewardPoolRemovalEvent {
                    condition_id: "0xabc".to_string(),
                    token1: "token1".to_string(),
                    token2: "token2".to_string(),
                    reason: "token1_spread_gt_threshold".to_string(),
                },
            ))
            .await
            .expect("pool removal event should send");

        let mut signals = Vec::new();
        for _ in 0..3 {
            signals.push(
                tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
                    .await
                    .expect("expected order signal")
                    .expect("order channel should remain open"),
            );
        }
        assert!(signals.iter().any(|signal| matches!(
            signal,
            OrderSignal::LiquidityRewardCancel { token, active_local_order_id, .. }
                if token == "token1" && active_local_order_id == "token1-active"
        )));
        assert!(signals.iter().any(|signal| matches!(
            signal,
            OrderSignal::LiquidityRewardCancel { token, active_local_order_id, .. }
                if token == "token2" && active_local_order_id == "token2-active"
        )));
        assert!(signals.iter().any(|signal| matches!(
            signal,
            OrderSignal::LiquidityRewardMarketSell { token, price, order_size, .. }
                if token == "token1" && *price == dec(0.49) && *order_size == dec(12.5)
        )));

        handle.abort();
    }

    #[tokio::test]
    async fn reward_pool_removal_unwinds_late_position_with_late_market() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![paired_rule(Some("token2"))])
            .expect("strategy should build")
            .expect("strategy should exist");
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        event_tx
            .send(StrategyEvent::RewardPoolRemoval(
                crate::strategy::RewardPoolRemovalEvent {
                    condition_id: "0xabc".to_string(),
                    token1: "token1".to_string(),
                    token2: "token2".to_string(),
                    reason: "token1_spread_gt_threshold".to_string(),
                },
            ))
            .await
            .expect("pool removal event should send");
        event_tx
            .send(StrategyEvent::Positions(
                crate::strategy::PositionsUpdateEvent {
                    snapshot: Arc::new(crate::strategy::PositionSnapshot {
                        by_asset: Arc::new(HashMap::from([(
                            "token2".to_string(),
                            position("token2", 7.5, 0.49),
                        )])),
                    }),
                    changed_assets: Arc::from(["token2".to_string()]),
                },
            ))
            .await
            .expect("positions event should send");
        event_tx
            .send(StrategyEvent::Market(crate::strategy::MarketEvent {
                topic: Arc::from(DEFAULT_TOPIC),
                asset_id: Arc::from("token2"),
                book: quoteable_book(),
            }))
            .await
            .expect("market event should send");

        let signal = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected late unwind signal")
            .expect("order channel should remain open");
        assert!(matches!(
            signal,
            OrderSignal::LiquidityRewardMarketSell { token, order_size, .. }
                if token == "token2" && order_size == dec(7.5)
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), order_rx.recv())
                .await
                .is_err(),
            "halted market must only submit pending unwind, not a new quote"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn reward_pool_removal_unwinds_both_positive_positions() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![paired_rule(Some("token2"))])
            .expect("strategy should build")
            .expect("strategy should exist");
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        for token in ["token1", "token2"] {
            event_tx
                .send(StrategyEvent::Market(crate::strategy::MarketEvent {
                    topic: Arc::from(DEFAULT_TOPIC),
                    asset_id: Arc::from(token),
                    book: quoteable_book(),
                }))
                .await
                .expect("market event should send");
            tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
                .await
                .expect("expected initial quote")
                .expect("order channel should remain open");
        }
        event_tx
            .send(StrategyEvent::Positions(
                crate::strategy::PositionsUpdateEvent {
                    snapshot: Arc::new(crate::strategy::PositionSnapshot {
                        by_asset: Arc::new(HashMap::from([
                            ("token1".to_string(), position("token1", 3.0, 0.49)),
                            ("token2".to_string(), position("token2", 4.0, 0.49)),
                        ])),
                    }),
                    changed_assets: Arc::from(["token1".to_string(), "token2".to_string()]),
                },
            ))
            .await
            .expect("positions event should send");
        event_tx
            .send(StrategyEvent::RewardPoolRemoval(
                crate::strategy::RewardPoolRemovalEvent {
                    condition_id: "0xabc".to_string(),
                    token1: "token1".to_string(),
                    token2: "token2".to_string(),
                    reason: "token1_spread_gt_threshold".to_string(),
                },
            ))
            .await
            .expect("pool removal event should send");

        let mut signals = Vec::new();
        for _ in 0..4 {
            signals.push(
                tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
                    .await
                    .expect("expected halt signal")
                    .expect("order channel should remain open"),
            );
        }
        assert!(signals.iter().any(|signal| matches!(
            signal,
            OrderSignal::LiquidityRewardMarketSell { token, order_size, .. }
                if token == "token1" && *order_size == Decimal::from(3)
        )));
        assert!(signals.iter().any(|signal| matches!(
            signal,
            OrderSignal::LiquidityRewardMarketSell { token, order_size, .. }
                if token == "token2" && *order_size == Decimal::from(4)
        )));

        handle.abort();
    }

    #[tokio::test]
    async fn repeated_reward_pool_removal_does_not_duplicate_unwind() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![paired_rule(Some("token2"))])
            .expect("strategy should build")
            .expect("strategy should exist");
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        event_tx
            .send(StrategyEvent::Market(crate::strategy::MarketEvent {
                topic: Arc::from(DEFAULT_TOPIC),
                asset_id: Arc::from("token1"),
                book: quoteable_book(),
            }))
            .await
            .expect("market event should send");
        tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected initial quote")
            .expect("order channel should remain open");
        event_tx
            .send(StrategyEvent::Positions(
                crate::strategy::PositionsUpdateEvent {
                    snapshot: Arc::new(crate::strategy::PositionSnapshot {
                        by_asset: Arc::new(HashMap::from([(
                            "token1".to_string(),
                            position("token1", 3.0, 0.49),
                        )])),
                    }),
                    changed_assets: Arc::from(["token1".to_string()]),
                },
            ))
            .await
            .expect("positions event should send");
        for _ in 0..2 {
            event_tx
                .send(StrategyEvent::RewardPoolRemoval(
                    crate::strategy::RewardPoolRemovalEvent {
                        condition_id: "0xabc".to_string(),
                        token1: "token1".to_string(),
                        token2: "token2".to_string(),
                        reason: "token1_spread_gt_threshold".to_string(),
                    },
                ))
                .await
                .expect("pool removal event should send");
        }

        let mut sell_count = 0;
        for _ in 0..2 {
            let signal = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
                .await
                .expect("expected halt signal")
                .expect("order channel should remain open");
            if matches!(
                signal,
                OrderSignal::LiquidityRewardMarketSell { ref token, order_size, .. }
                    if token == "token1" && order_size == Decimal::from(3)
            ) {
                sell_count += 1;
            }
        }
        assert_eq!(sell_count, 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), order_rx.recv())
                .await
                .is_err(),
            "repeated removal must not submit duplicate unwind"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn pool_removal_can_unwind_again_after_first_unwind_terminal() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![paired_rule(Some("token2"))])
            .expect("strategy should build")
            .expect("strategy should exist");
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        event_tx
            .send(StrategyEvent::Market(crate::strategy::MarketEvent {
                topic: Arc::from(DEFAULT_TOPIC),
                asset_id: Arc::from("token1"),
                book: quoteable_book(),
            }))
            .await
            .expect("market event should send");
        tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected initial quote")
            .expect("order channel should remain open");
        event_tx
            .send(StrategyEvent::Positions(
                crate::strategy::PositionsUpdateEvent {
                    snapshot: Arc::new(crate::strategy::PositionSnapshot {
                        by_asset: Arc::new(HashMap::from([(
                            "token1".to_string(),
                            position("token1", 5.0, 0.49),
                        )])),
                    }),
                    changed_assets: Arc::from(["token1".to_string()]),
                },
            ))
            .await
            .expect("positions event should send");
        event_tx
            .send(StrategyEvent::RewardPoolRemoval(
                crate::strategy::RewardPoolRemovalEvent {
                    condition_id: "0xabc".to_string(),
                    token1: "token1".to_string(),
                    token2: "token2".to_string(),
                    reason: "token1_spread_gt_threshold".to_string(),
                },
            ))
            .await
            .expect("pool removal event should send");

        let mut first_unwind_id = None;
        for _ in 0..2 {
            let signal = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
                .await
                .expect("expected pool removal signal")
                .expect("order channel should remain open");
            if let OrderSignal::LiquidityRewardMarketSell {
                local_order_id,
                order_size,
                ..
            } = signal
            {
                assert_eq!(order_size, Decimal::from(5));
                first_unwind_id = Some(local_order_id);
            }
        }
        let first_unwind_id = first_unwind_id.expect("pool removal unwind should submit");
        event_tx
            .send(StrategyEvent::OrderStatus(
                crate::strategy::OrderStatusEvent {
                    token: "token1".to_string(),
                    local_order_id: first_unwind_id,
                    status: Arc::from("filled"),
                    reason: None,
                },
            ))
            .await
            .expect("unwind status should send");
        event_tx
            .send(StrategyEvent::Positions(
                crate::strategy::PositionsUpdateEvent {
                    snapshot: Arc::new(crate::strategy::PositionSnapshot {
                        by_asset: Arc::new(HashMap::from([(
                            "token1".to_string(),
                            position("token1", 3.0, 0.49),
                        )])),
                    }),
                    changed_assets: Arc::from(["token1".to_string()]),
                },
            ))
            .await
            .expect("positions event should send");

        let second_unwind = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected second pool removal unwind")
            .expect("order channel should remain open");
        assert!(matches!(
            second_unwind,
            OrderSignal::LiquidityRewardMarketSell { token, order_size, .. }
                if token == "token1" && order_size == Decimal::from(3)
        ));

        handle.abort();
    }

    #[tokio::test]
    async fn buy_fill_halts_pair_and_cancels_other_side() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![paired_rule(Some("token2"))])
            .expect("strategy should build")
            .expect("strategy should exist");
        let mut restored_states = HashMap::new();
        restored_states.insert("token2".to_string(), restore_state("token2-active"));
        let strategy = strategy.with_restore_state(
            restored_states,
            None,
            false,
            Arc::new(dashmap::DashMap::new()),
        );
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        event_tx
            .send(StrategyEvent::Market(crate::strategy::MarketEvent {
                topic: Arc::from(DEFAULT_TOPIC),
                asset_id: Arc::from("token1"),
                book: quoteable_book(),
            }))
            .await
            .expect("market event should send");
        tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected initial quote")
            .expect("order channel should remain open");
        event_tx
            .send(StrategyEvent::OrderFill(crate::strategy::OrderFillEvent {
                strategy: Arc::from("liquidity_reward"),
                topic: Some(Arc::from(DEFAULT_TOPIC)),
                token: "token1".to_string(),
                local_order_id: "historical-buy".to_string(),
                remote_order_id: Some("remote-buy".to_string()),
                side: crate::strategy::QuoteSide::Buy,
                delta_size: Decimal::from(20),
                total_matched_size: Decimal::from(20),
            }))
            .await
            .expect("fill event should send");

        let mut signals = Vec::new();
        for _ in 0..2 {
            signals.push(
                tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
                    .await
                    .expect("expected cancel signal")
                    .expect("order channel should remain open"),
            );
        }
        assert!(signals.iter().any(|signal| matches!(
            signal,
            OrderSignal::LiquidityRewardCancel { token, active_local_order_id, .. }
                if token == "token2" && active_local_order_id == "token2-active"
        )));
        assert!(
            !signals
                .iter()
                .any(|signal| matches!(signal, OrderSignal::LiquidityRewardMarketSell { .. }))
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), order_rx.recv())
                .await
                .is_err(),
            "buy fill must not submit unwind before confirmation"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn fill_unwind_waits_for_trade_and_position() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![test_rule()])
            .expect("strategy should build")
            .expect("strategy should exist");
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        event_tx
            .send(StrategyEvent::Market(crate::strategy::MarketEvent {
                topic: Arc::from(DEFAULT_TOPIC),
                asset_id: Arc::from("token1"),
                book: quoteable_book(),
            }))
            .await
            .expect("market event should send");
        let quote = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected initial quote")
            .expect("order channel should remain open");
        let buy_id = match quote {
            OrderSignal::LiquidityRewardPlace { local_order_id, .. } => local_order_id,
            other => panic!("expected buy quote, got {other:?}"),
        };
        event_tx
            .send(StrategyEvent::OrderFill(crate::strategy::OrderFillEvent {
                strategy: Arc::from("liquidity_reward"),
                topic: Some(Arc::from(DEFAULT_TOPIC)),
                token: "token1".to_string(),
                local_order_id: buy_id.clone(),
                remote_order_id: Some("remote-buy".to_string()),
                side: crate::strategy::QuoteSide::Buy,
                delta_size: Decimal::from(20),
                total_matched_size: Decimal::from(20),
            }))
            .await
            .expect("fill event should send");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), order_rx.recv())
                .await
                .is_err()
        );

        event_tx
            .send(StrategyEvent::TradeConfirmed(confirmed_trade(
                "token1",
                "remote-buy",
            )))
            .await
            .expect("trade event should send");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), order_rx.recv())
                .await
                .is_err()
        );

        event_tx
            .send(StrategyEvent::Positions(
                crate::strategy::PositionsUpdateEvent {
                    snapshot: Arc::new(crate::strategy::PositionSnapshot {
                        by_asset: Arc::new(HashMap::from([(
                            "token1".to_string(),
                            position("token1", 15.0, 0.49),
                        )])),
                    }),
                    changed_assets: Arc::from(["token1".to_string()]),
                },
            ))
            .await
            .expect("positions event should send");

        let signal = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected fill unwind")
            .expect("order channel should remain open");
        assert!(matches!(
            signal,
            OrderSignal::LiquidityRewardMarketSell { token, price, order_size, .. }
                if token == "token1" && price == dec(0.49) && order_size == Decimal::from(15)
        ));

        handle.abort();
    }

    #[tokio::test]
    async fn fill_unwind_uses_cached_position_after_trade_confirmed() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![test_rule()])
            .expect("strategy should build")
            .expect("strategy should exist");
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        event_tx
            .send(StrategyEvent::Market(crate::strategy::MarketEvent {
                topic: Arc::from(DEFAULT_TOPIC),
                asset_id: Arc::from("token1"),
                book: quoteable_book(),
            }))
            .await
            .expect("market event should send");
        let quote = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected initial quote")
            .expect("order channel should remain open");
        let buy_id = match quote {
            OrderSignal::LiquidityRewardPlace { local_order_id, .. } => local_order_id,
            other => panic!("expected buy quote, got {other:?}"),
        };
        event_tx
            .send(StrategyEvent::Positions(
                crate::strategy::PositionsUpdateEvent {
                    snapshot: Arc::new(crate::strategy::PositionSnapshot {
                        by_asset: Arc::new(HashMap::from([(
                            "token1".to_string(),
                            position("token1", 30.0, 0.49),
                        )])),
                    }),
                    changed_assets: Arc::from(["token1".to_string()]),
                },
            ))
            .await
            .expect("positions event should send");
        event_tx
            .send(StrategyEvent::OrderFill(crate::strategy::OrderFillEvent {
                strategy: Arc::from("liquidity_reward"),
                topic: Some(Arc::from(DEFAULT_TOPIC)),
                token: "token1".to_string(),
                local_order_id: buy_id.clone(),
                remote_order_id: Some("remote-buy".to_string()),
                side: crate::strategy::QuoteSide::Buy,
                delta_size: Decimal::from(20),
                total_matched_size: Decimal::from(20),
            }))
            .await
            .expect("fill event should send");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), order_rx.recv())
                .await
                .is_err()
        );

        event_tx
            .send(StrategyEvent::TradeConfirmed(confirmed_trade(
                "token1",
                "remote-buy",
            )))
            .await
            .expect("trade event should send");

        let signal = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected fill unwind")
            .expect("order channel should remain open");
        assert!(matches!(
            signal,
            OrderSignal::LiquidityRewardMarketSell { token, order_size, .. }
                if token == "token1" && order_size == Decimal::from(20)
        ));

        handle.abort();
    }

    #[tokio::test]
    async fn fill_unwind_ignores_unrelated_trade_confirmation() {
        let strategy = LiquidityRewardFsmStrategy::from_rules(vec![test_rule()])
            .expect("strategy should build")
            .expect("strategy should exist");
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(16);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(16);
        let handle = strategy.spawn(event_rx, order_tx);

        event_tx
            .send(StrategyEvent::Market(crate::strategy::MarketEvent {
                topic: Arc::from(DEFAULT_TOPIC),
                asset_id: Arc::from("token1"),
                book: quoteable_book(),
            }))
            .await
            .expect("market event should send");
        let quote = tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
            .await
            .expect("expected initial quote")
            .expect("order channel should remain open");
        let buy_id = match quote {
            OrderSignal::LiquidityRewardPlace { local_order_id, .. } => local_order_id,
            other => panic!("expected buy quote, got {other:?}"),
        };
        event_tx
            .send(StrategyEvent::OrderFill(crate::strategy::OrderFillEvent {
                strategy: Arc::from("liquidity_reward"),
                topic: Some(Arc::from(DEFAULT_TOPIC)),
                token: "token1".to_string(),
                local_order_id: buy_id.clone(),
                remote_order_id: Some("remote-buy".to_string()),
                side: crate::strategy::QuoteSide::Buy,
                delta_size: Decimal::from(20),
                total_matched_size: Decimal::from(20),
            }))
            .await
            .expect("fill event should send");
        event_tx
            .send(StrategyEvent::Positions(
                crate::strategy::PositionsUpdateEvent {
                    snapshot: Arc::new(crate::strategy::PositionSnapshot {
                        by_asset: Arc::new(HashMap::from([(
                            "token1".to_string(),
                            position("token1", 20.0, 0.49),
                        )])),
                    }),
                    changed_assets: Arc::from(["token1".to_string()]),
                },
            ))
            .await
            .expect("positions event should send");
        event_tx
            .send(StrategyEvent::TradeConfirmed(confirmed_trade(
                "token1",
                "other-order",
            )))
            .await
            .expect("trade event should send");

        assert!(
            tokio::time::timeout(Duration::from_millis(100), order_rx.recv())
                .await
                .is_err(),
            "unrelated trade must not submit fill unwind"
        );

        handle.abort();
    }

    fn test_rule() -> LiquidityRewardRule {
        paired_rule(None)
    }

    fn paired_rule(token2: Option<&str>) -> LiquidityRewardRule {
        LiquidityRewardRule {
            topic: Arc::from(DEFAULT_TOPIC),
            token1: "token1".to_string(),
            token2: token2.map(str::to_string),
            reward_min_orders: None,
            reward_max_spread_cents: Some(4.0),
            reward_min_size: Some(100.0),
            reward_daily_pool: Some(50.0),
            fixed_price: false,
            condition_id: None,
            pool_version: None,
        }
    }

    fn quoteable_book() -> crate::strategy::CleanOrderbook {
        crate::strategy::CleanOrderbook {
            best_bid_price: 4900,
            best_bid_size: 100,
            best_ask_price: 5100,
            best_ask_size: 100,
            timestamp_ms: 1,
            bids: Arc::new(BTreeMap::from([(4900, 100)])),
            asks: Arc::new(BTreeMap::new()),
        }
    }

    fn confirmed_trade(token: &str, order_id: &str) -> crate::strategy::TradeConfirmedEvent {
        crate::strategy::TradeConfirmedEvent {
            token: token.to_string(),
            market: "market".to_string(),
            trade_id: "trade".to_string(),
            size: Decimal::from(1),
            price: dec(0.49),
            side: crate::strategy::QuoteSide::Buy,
            taker_order_id: Some(order_id.to_string()),
            maker_order_ids: Arc::from(Vec::<String>::new()),
            timestamp_ms: None,
        }
    }

    fn position(asset_id: &str, size: f64, cur_price: f64) -> crate::strategy::PositionView {
        crate::strategy::PositionView {
            asset_id: asset_id.to_string(),
            size: Decimal::try_from(size).unwrap(),
            avg_price: Decimal::try_from(cur_price).unwrap(),
            cur_price: Decimal::try_from(cur_price).unwrap(),
            current_value: Decimal::ZERO,
            cash_pnl: Decimal::ZERO,
            title: Arc::from("test"),
            outcome: Arc::from("Yes"),
        }
    }

    fn restore_state(active_order_id: &str) -> LiquidityRewardRestoreState {
        LiquidityRewardRestoreState {
            topic: Arc::from(DEFAULT_TOPIC),
            buy: crate::strategies::liquidity_reward::LiquidityRewardRestoreSideState {
                active_local_order_id: Some(active_order_id.to_string()),
                active_price: Some(dec(0.5)),
                active_order_size: Some(dec(100.0)),
                ..Default::default()
            },
            sell: crate::strategies::liquidity_reward::LiquidityRewardRestoreSideState::default(),
            last_mid: Some(dec(0.5)),
            last_best_bid: Some(dec(0.49)),
            last_best_ask: Some(dec(0.51)),
            last_position_size: Decimal::ZERO,
        }
    }
}
