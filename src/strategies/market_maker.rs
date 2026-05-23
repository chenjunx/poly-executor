use std::collections::{BTreeSet, HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;

use tracing::{info, warn};

use polymarket_client_sdk_v2::types::Decimal;

use crate::notification::{NotificationEvent, Notifier, RiskEventNotification};
use crate::order_gateway::{
    CancelOrderRequest, CancelScope, GatewayOrderType, LocalOrderId, LocalOrderState, MarketId,
    OrderRecord, OrderRequest, OrderSide, PlaceOrderRequest, StrategyId, TimeInForce, TokenId,
};
use crate::storage::ActiveRewardMarketPoolEntry;
use crate::strategy::{
    CleanOrderbook, MarketEvent, Strategy, StrategyKind, StrategyMarketSubscriptions,
    StrategyRegistration, TopicRegistration, spawn_market_subscription_mux,
};
use crate::tick_size::snap_unwind_size_to_lot;

const MARKET_MAKER_NAME: &str = "market_maker";
const PRICE_SCALE: u32 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketMakerRule {
    pub condition_id: String,
    pub market_slug: Option<String>,
    pub token1: String,
    pub token2: String,
    pub rewards_max_spread: Option<String>,
    pub rewards_min_size: Option<String>,
}

pub struct MarketMakerStrategy {
    rules: Arc<[MarketMakerRule]>,
    registration: Arc<StrategyRegistration>,
    config: Arc<MarketMakerStrategyConfig>,
    notifier: Option<Notifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketMakerStrategyConfig {
    pub max_inventory_usd: Decimal,
    pub overweight_ratio: Decimal,
    pub default_max_spread: Decimal,
    pub tick_size: Decimal,
    pub min_size: Decimal,
    pub max_skew: Decimal,
    pub volatility_window_ms: u64,
    pub volatility_min_samples: usize,
    pub volatility_threshold: Decimal,
    pub spread_cooldown_ms: u64,
    pub volatility_cooldown_ms: u64,
    pub fair_midpoint_cooldown_ms: u64,
    pub fair_midpoint_min: Decimal,
    pub fair_midpoint_max: Decimal,
    pub abnormal_market_spread_multiplier: Decimal,
    pub normal_quote_levels: usize,
    pub overweight_quote_levels: usize,
    pub level_ratios: Vec<Decimal>,
    pub level_sizes_usd: Vec<Decimal>,
    pub reconcile_size_tolerance: Decimal,
}

impl Default for MarketMakerStrategyConfig {
    fn default() -> Self {
        Self {
            max_inventory_usd: Decimal::from(100u32),
            overweight_ratio: Decimal::from(7u32) / Decimal::from(10u32),
            default_max_spread: parse_decimal("0.03"),
            tick_size: parse_decimal("0.01"),
            min_size: Decimal::from(5u32),
            max_skew: parse_decimal("0.01"),
            volatility_window_ms: 5 * 60 * 1000,
            volatility_min_samples: 5,
            volatility_threshold: parse_decimal("0.02"),
            spread_cooldown_ms: 60 * 1000,
            volatility_cooldown_ms: 5 * 60 * 1000,
            fair_midpoint_cooldown_ms: 10 * 60 * 1000,
            fair_midpoint_min: parse_decimal("0.15"),
            fair_midpoint_max: parse_decimal("0.85"),
            abnormal_market_spread_multiplier: Decimal::from(2u32),
            normal_quote_levels: 3,
            overweight_quote_levels: 2,
            level_ratios: vec![
                parse_decimal("0.4"),
                parse_decimal("0.55"),
                parse_decimal("0.7"),
            ],
            level_sizes_usd: vec![
                Decimal::from(50u32),
                Decimal::from(75u32),
                Decimal::from(100u32),
            ],
            reconcile_size_tolerance: parse_decimal("0.2"),
        }
    }
}

impl TryFrom<&crate::config::MarketMakerConfig> for MarketMakerStrategyConfig {
    type Error = anyhow::Error;

    fn try_from(config: &crate::config::MarketMakerConfig) -> anyhow::Result<Self> {
        anyhow::ensure!(
            config.max_inventory_usd > 0.0,
            "market_maker.max_inventory_usd must be > 0"
        );
        anyhow::ensure!(
            (0.0..=1.0).contains(&config.overweight_ratio),
            "market_maker.overweight_ratio must be between 0 and 1"
        );
        anyhow::ensure!(
            config.default_max_spread > 0.0,
            "market_maker.default_max_spread must be > 0"
        );
        anyhow::ensure!(config.tick_size > 0.0, "market_maker.tick_size must be > 0");
        anyhow::ensure!(config.min_size > 0.0, "market_maker.min_size must be > 0");
        anyhow::ensure!(config.max_skew >= 0.0, "market_maker.max_skew must be >= 0");
        anyhow::ensure!(
            config.volatility_min_samples > 0,
            "market_maker.volatility_min_samples must be > 0"
        );
        anyhow::ensure!(
            config.volatility_threshold >= 0.0,
            "market_maker.volatility_threshold must be >= 0"
        );
        anyhow::ensure!(
            config.fair_midpoint_min >= 0.0
                && config.fair_midpoint_min < config.fair_midpoint_max
                && config.fair_midpoint_max <= 1.0,
            "market_maker fair midpoint range must satisfy 0 <= min < max <= 1"
        );
        anyhow::ensure!(
            config.abnormal_market_spread_multiplier > 0.0,
            "market_maker.abnormal_market_spread_multiplier must be > 0"
        );
        anyhow::ensure!(
            config.normal_quote_levels > 0,
            "market_maker.normal_quote_levels must be > 0"
        );
        anyhow::ensure!(
            config.overweight_quote_levels > 0,
            "market_maker.overweight_quote_levels must be > 0"
        );
        anyhow::ensure!(
            !config.level_ratios.is_empty(),
            "market_maker.level_ratios must not be empty"
        );
        anyhow::ensure!(
            config.level_ratios.len() == config.level_sizes_usd.len(),
            "market_maker.level_ratios and level_sizes_usd must have the same length"
        );
        anyhow::ensure!(
            config.level_ratios.iter().all(|value| *value > 0.0),
            "market_maker.level_ratios entries must be > 0"
        );
        anyhow::ensure!(
            config.level_sizes_usd.iter().all(|value| *value > 0.0),
            "market_maker.level_sizes_usd entries must be > 0"
        );
        anyhow::ensure!(
            (0.0..=1.0).contains(&config.reconcile_size_tolerance),
            "market_maker.reconcile_size_tolerance must be between 0 and 1"
        );

        Ok(Self {
            max_inventory_usd: decimal_from_f64(config.max_inventory_usd)?,
            overweight_ratio: decimal_from_f64(config.overweight_ratio)?,
            default_max_spread: decimal_from_f64(config.default_max_spread)?,
            tick_size: decimal_from_f64(config.tick_size)?,
            min_size: decimal_from_f64(config.min_size)?,
            max_skew: decimal_from_f64(config.max_skew)?,
            volatility_window_ms: config.volatility_window_ms,
            volatility_min_samples: config.volatility_min_samples,
            volatility_threshold: decimal_from_f64(config.volatility_threshold)?,
            spread_cooldown_ms: config.spread_cooldown_ms,
            volatility_cooldown_ms: config.volatility_cooldown_ms,
            fair_midpoint_cooldown_ms: config.fair_midpoint_cooldown_ms,
            fair_midpoint_min: decimal_from_f64(config.fair_midpoint_min)?,
            fair_midpoint_max: decimal_from_f64(config.fair_midpoint_max)?,
            abnormal_market_spread_multiplier: decimal_from_f64(
                config.abnormal_market_spread_multiplier,
            )?,
            normal_quote_levels: config.normal_quote_levels,
            overweight_quote_levels: config.overweight_quote_levels,
            level_ratios: config
                .level_ratios
                .iter()
                .map(|value| decimal_from_f64(*value))
                .collect::<anyhow::Result<Vec<_>>>()?,
            level_sizes_usd: config
                .level_sizes_usd
                .iter()
                .map(|value| decimal_from_f64(*value))
                .collect::<anyhow::Result<Vec<_>>>()?,
            reconcile_size_tolerance: decimal_from_f64(config.reconcile_size_tolerance)?,
        })
    }
}

fn decimal_from_f64(value: f64) -> anyhow::Result<Decimal> {
    Decimal::try_from(value)
        .map_err(|error| anyhow::anyhow!("invalid decimal config value {value}: {error}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InventorySide {
    LongYes,
    LongNo,
    Flat,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InventoryState {
    pub yes_value_usd: Decimal,
    pub no_value_usd: Decimal,
    pub value_usd: Decimal,
    pub ratio: Decimal,
    pub side: InventorySide,
    pub is_overweight: bool,
}

pub fn compute_inventory_state(
    yes_token_balance: Decimal,
    no_token_balance: Decimal,
    yes_fair_mid: Decimal,
    no_fair_mid: Decimal,
    max_inventory_usd: Decimal,
    overweight_ratio: Decimal,
) -> InventoryState {
    let yes_value_usd = yes_token_balance * yes_fair_mid;
    let no_value_usd = no_token_balance * no_fair_mid;
    let value_usd = yes_value_usd - no_value_usd;
    let ratio = if max_inventory_usd > Decimal::ZERO {
        clamp_decimal(value_usd / max_inventory_usd, -Decimal::ONE, Decimal::ONE)
    } else {
        Decimal::ZERO
    };
    let side = if value_usd > Decimal::ZERO {
        InventorySide::LongYes
    } else if value_usd < Decimal::ZERO {
        InventorySide::LongNo
    } else {
        InventorySide::Flat
    };

    InventoryState {
        yes_value_usd,
        no_value_usd,
        value_usd,
        ratio,
        side,
        is_overweight: decimal_abs(ratio) > overweight_ratio,
    }
}

fn clamp_decimal(value: Decimal, min: Decimal, max: Decimal) -> Decimal {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}

fn decimal_abs(value: Decimal) -> Decimal {
    if value < Decimal::ZERO { -value } else { value }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuoteSkewState {
    pub yes_skew: Decimal,
    pub no_skew: Decimal,
}

pub fn compute_quote_skew(inventory_ratio: Decimal, max_skew: Decimal) -> QuoteSkewState {
    let ratio = clamp_decimal(inventory_ratio, -Decimal::ONE, Decimal::ONE);
    let magnitude = ratio * ratio * max_skew;
    if ratio > Decimal::ZERO {
        QuoteSkewState {
            yes_skew: Decimal::ZERO,
            no_skew: magnitude,
        }
    } else if ratio < Decimal::ZERO {
        QuoteSkewState {
            yes_skew: magnitude,
            no_skew: Decimal::ZERO,
        }
    } else {
        QuoteSkewState {
            yes_skew: Decimal::ZERO,
            no_skew: Decimal::ZERO,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetTokenSide {
    Yes,
    No,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TargetQuoteParams {
    pub max_spread: Decimal,
    pub tick_size: Decimal,
    pub min_size: Decimal,
    pub normal_quote_levels: usize,
    pub overweight_quote_levels: usize,
    pub level_ratios: Vec<Decimal>,
    pub level_sizes_usd: Vec<Decimal>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TargetQuote {
    pub token_side: TargetTokenSide,
    pub level: usize,
    pub price: Decimal,
    pub size: Decimal,
    pub size_usd: Decimal,
    pub adjusted_mid: Decimal,
    pub distance: Decimal,
    pub raw_bid: Decimal,
}

pub fn compute_target_buy_quotes_for_token(
    token_side: TargetTokenSide,
    fair_mid: Decimal,
    skew: Decimal,
    is_overweight: bool,
    params: &TargetQuoteParams,
) -> Vec<TargetQuote> {
    let adjusted_mid = fair_mid + skew;
    let num_levels = if is_overweight {
        params.overweight_quote_levels
    } else {
        params.normal_quote_levels
    };
    params
        .level_ratios
        .iter()
        .zip(params.level_sizes_usd.iter())
        .take(num_levels)
        .enumerate()
        .filter_map(|(index, (&level_ratio, &size_usd))| {
            let distance = params.max_spread * level_ratio;
            let raw_bid = adjusted_mid - distance;
            let price = floor_to_tick(raw_bid, params.tick_size);
            if price <= Decimal::ZERO {
                return None;
            }
            let size = snap_unwind_size_to_lot(decimal_max(size_usd / price, params.min_size));
            Some(TargetQuote {
                token_side,
                level: index + 1,
                price,
                size,
                size_usd,
                adjusted_mid,
                distance,
                raw_bid,
            })
        })
        .collect()
}

fn floor_to_tick(price: Decimal, tick_size: Decimal) -> Decimal {
    if tick_size <= Decimal::ZERO {
        return price;
    }
    (price / tick_size).floor() * tick_size
}

fn decimal_max(left: Decimal, right: Decimal) -> Decimal {
    if left > right { left } else { right }
}

fn price_to_decimal(price: u16) -> Decimal {
    Decimal::from(price) / Decimal::from(PRICE_SCALE)
}

fn size_to_decimal(size: u32) -> Decimal {
    Decimal::from(size) / Decimal::from(PRICE_SCALE)
}

fn record_fair_midpoint_history(
    history: &mut VecDeque<(u64, Decimal)>,
    timestamp_ms: u64,
    fair_mid: Decimal,
    window_ms: u64,
) {
    history.push_back((timestamp_ms, fair_mid));
    while let Some((oldest_timestamp_ms, _)) = history.front() {
        if timestamp_ms.saturating_sub(*oldest_timestamp_ms) > window_ms {
            history.pop_front();
        } else {
            break;
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct VolatilityState {
    sample_count: usize,
    min_fair_mid: Decimal,
    max_fair_mid: Decimal,
    range: Decimal,
    threshold: Decimal,
    is_volatile: bool,
}

fn price_history_volatility_state(
    history: &VecDeque<(u64, Decimal)>,
    threshold: Decimal,
    min_samples: usize,
) -> Option<VolatilityState> {
    let (_, first_price) = history.front()?;
    let mut min_price = *first_price;
    let mut max_price = *first_price;
    for (_, price) in history.iter().skip(1) {
        if *price < min_price {
            min_price = *price;
        }
        if *price > max_price {
            max_price = *price;
        }
    }
    let range = max_price - min_price;

    Some(VolatilityState {
        sample_count: history.len(),
        min_fair_mid: min_price,
        max_fair_mid: max_price,
        range,
        threshold,
        is_volatile: history.len() >= min_samples && range > threshold,
    })
}

fn price_history_is_volatile(
    history: &VecDeque<(u64, Decimal)>,
    threshold: Decimal,
    min_samples: usize,
) -> bool {
    price_history_volatility_state(history, threshold, min_samples)
        .is_some_and(|state| state.is_volatile)
}

fn token_price_is_volatile(
    price_history: &HashMap<String, VecDeque<(u64, Decimal)>>,
    token_id: &str,
    config: &MarketMakerStrategyConfig,
) -> bool {
    price_history.get(token_id).is_some_and(|history| {
        price_history_is_volatile(
            history,
            config.volatility_threshold,
            config.volatility_min_samples,
        )
    })
}

fn log_volatility_state(
    asset_id: &str,
    history: &VecDeque<(u64, Decimal)>,
    config: &MarketMakerStrategyConfig,
) {
    if let Some(state) = price_history_volatility_state(
        history,
        config.volatility_threshold,
        config.volatility_min_samples,
    ) {
        info!(
            target: "order",
            asset_id = %asset_id,
            window_ms = config.volatility_window_ms,
            sample_count = state.sample_count,
            min_samples = config.volatility_min_samples,
            min_fair_mid = %state.min_fair_mid,
            max_fair_mid = %state.max_fair_mid,
            range = %state.range,
            threshold = %state.threshold,
            is_volatile = state.is_volatile,
            "market_maker 5m volatility state"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MarketMakerRiskDecision {
    Allow,
    Skip { code: &'static str, reason: String },
}

impl MarketMakerRiskDecision {
    fn is_allow(&self) -> bool {
        matches!(self, Self::Allow)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CooldownState {
    until_ms: u64,
    code: &'static str,
    reason: String,
    triggered_at_ms: u64,
}

fn cooldown_key(rule: &MarketMakerRule) -> String {
    if rule.condition_id.is_empty() {
        format!("{}:{}", rule.token1, rule.token2)
    } else {
        rule.condition_id.clone()
    }
}

fn cooldown_duration_ms(code: &str, config: &MarketMakerStrategyConfig) -> u64 {
    match code {
        "price_volatility" => config.volatility_cooldown_ms,
        "fair_midpoint_out_of_range" => config.fair_midpoint_cooldown_ms,
        "abnormal_market_spread" => config.spread_cooldown_ms,
        _ => config.spread_cooldown_ms,
    }
}

fn active_cooldown<'a>(
    cooldowns: &'a mut HashMap<String, CooldownState>,
    rule: &MarketMakerRule,
    now_ms: u64,
) -> Option<&'a CooldownState> {
    let key = cooldown_key(rule);
    let expired = cooldowns
        .get(&key)
        .is_some_and(|cooldown| now_ms >= cooldown.until_ms);
    if expired {
        cooldowns.remove(&key);
    }
    cooldowns.get(&key)
}

fn enter_cooldown(
    cooldowns: &mut HashMap<String, CooldownState>,
    rule: &MarketMakerRule,
    now_ms: u64,
    code: &'static str,
    reason: String,
    config: &MarketMakerStrategyConfig,
) -> CooldownState {
    let cooldown = CooldownState {
        until_ms: now_ms + cooldown_duration_ms(code, config),
        code,
        reason,
        triggered_at_ms: now_ms,
    };
    cooldowns.insert(cooldown_key(rule), cooldown.clone());
    cooldown
}

fn cooldown_risk_notification(
    rule: &MarketMakerRule,
    intent: &TargetBuyQuoteIntent,
    cooldown: &CooldownState,
) -> NotificationEvent {
    NotificationEvent::RiskEvent(RiskEventNotification {
        source: "market_maker_cooldown".to_string(),
        strategy_id: Some(MARKET_MAKER_NAME.to_string()),
        local_order_id: None,
        market_id: (!rule.condition_id.is_empty()).then(|| rule.condition_id.clone()),
        token_id: Some(intent.token_id.clone()),
        risk_code: cooldown.code.to_string(),
        reason: cooldown.reason.clone(),
    })
}

fn cooldown_cancel_requests(rule: &MarketMakerRule) -> Vec<CancelOrderRequest> {
    let reason = Some(Arc::from("market_maker_cooldown"));
    if !rule.condition_id.is_empty() {
        vec![CancelOrderRequest {
            strategy_id: StrategyId::from(MARKET_MAKER_NAME),
            scope: CancelScope::Market {
                market_id: MarketId::from(rule.condition_id.clone()),
            },
            reason,
        }]
    } else {
        vec![
            CancelOrderRequest {
                strategy_id: StrategyId::from(MARKET_MAKER_NAME),
                scope: CancelScope::Token {
                    token_id: TokenId::from(rule.token1.clone()),
                },
                reason: reason.clone(),
            },
            CancelOrderRequest {
                strategy_id: StrategyId::from(MARKET_MAKER_NAME),
                scope: CancelScope::Token {
                    token_id: TokenId::from(rule.token2.clone()),
                },
                reason,
            },
        ]
    }
}

struct MarketMakerQuoteRiskContext<'a> {
    rule: &'a MarketMakerRule,
    intent: &'a TargetBuyQuoteIntent,
    books: &'a HashMap<String, Arc<CleanOrderbook>>,
    price_history: &'a HashMap<String, VecDeque<(u64, Decimal)>>,
    config: &'a MarketMakerStrategyConfig,
}

fn check_market_maker_quote_risk(ctx: &MarketMakerQuoteRiskContext<'_>) -> MarketMakerRiskDecision {
    let checks: [fn(&MarketMakerQuoteRiskContext<'_>) -> MarketMakerRiskDecision; 3] = [
        check_abnormal_market_spread,
        check_fair_midpoint_safe_range,
        check_quote_volatility,
    ];
    for check in checks {
        let decision = check(ctx);
        if !decision.is_allow() {
            return decision;
        }
    }

    MarketMakerRiskDecision::Allow
}

fn check_abnormal_market_spread(ctx: &MarketMakerQuoteRiskContext<'_>) -> MarketMakerRiskDecision {
    let Some(book) = ctx.books.get(&ctx.intent.token_id) else {
        return MarketMakerRiskDecision::Allow;
    };
    let market_spread =
        price_to_decimal(book.best_ask_price) - price_to_decimal(book.best_bid_price);
    if market_spread
        > reward_max_spread(
            ctx.rule.rewards_max_spread.as_deref(),
            ctx.config.default_max_spread,
        ) * ctx.config.abnormal_market_spread_multiplier
    {
        MarketMakerRiskDecision::Skip {
            code: "abnormal_market_spread",
            reason: "abnormal market spread".to_string(),
        }
    } else {
        MarketMakerRiskDecision::Allow
    }
}

fn check_fair_midpoint_safe_range(
    ctx: &MarketMakerQuoteRiskContext<'_>,
) -> MarketMakerRiskDecision {
    let Some(book) = ctx.books.get(&ctx.intent.token_id) else {
        return MarketMakerRiskDecision::Allow;
    };
    let fair_mid = price_to_decimal(compute_fair_midpoint(book));
    if fair_mid < ctx.config.fair_midpoint_min || fair_mid > ctx.config.fair_midpoint_max {
        MarketMakerRiskDecision::Skip {
            code: "fair_midpoint_out_of_range",
            reason: "fair midpoint out of safe range".to_string(),
        }
    } else {
        MarketMakerRiskDecision::Allow
    }
}

fn check_quote_volatility(ctx: &MarketMakerQuoteRiskContext<'_>) -> MarketMakerRiskDecision {
    if token_price_is_volatile(ctx.price_history, &ctx.intent.token_id, ctx.config) {
        MarketMakerRiskDecision::Skip {
            code: "price_volatility",
            reason: "price volatility too high".to_string(),
        }
    } else {
        MarketMakerRiskDecision::Allow
    }
}

pub fn compute_fair_midpoint(book: &CleanOrderbook) -> u16 {
    let bid = u64::from(book.best_bid_price);
    let ask = u64::from(book.best_ask_price);
    let bid_size = u64::from(book.best_bid_size);
    let ask_size = u64::from(book.best_ask_size);
    let total_size = bid_size + ask_size;

    let rounded = if total_size > 0 {
        let numerator = ask * bid_size + bid * ask_size;
        (numerator + total_size / 2) / total_size
    } else {
        (bid + ask + 1) / 2
    };

    let clamped = rounded.clamp(bid, ask);
    clamped as u16
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FairMidpointLogEvent {
    pub topic: Arc<str>,
    pub asset_id: Arc<str>,
    pub best_bid_price: u16,
    pub best_ask_price: u16,
    pub best_bid_size: u32,
    pub best_ask_size: u32,
    pub fair_midpoint: u16,
    pub timestamp_ms: u64,
}

pub fn fair_midpoint_log_event(event: &MarketEvent) -> FairMidpointLogEvent {
    FairMidpointLogEvent {
        topic: event.topic.clone(),
        asset_id: event.asset_id.clone(),
        best_bid_price: event.book.best_bid_price,
        best_ask_price: event.book.best_ask_price,
        best_bid_size: event.book.best_bid_size,
        best_ask_size: event.book.best_ask_size,
        fair_midpoint: compute_fair_midpoint(&event.book),
        timestamp_ms: event.book.timestamp_ms,
    }
}

fn log_fair_midpoint(event: &MarketEvent) {
    let log_event = fair_midpoint_log_event(event);
    info!(
        target: "order",
        topic = %log_event.topic,
        asset_id = %log_event.asset_id,
        best_bid_price = %price_to_decimal(log_event.best_bid_price),
        best_ask_price = %price_to_decimal(log_event.best_ask_price),
        best_bid_size = %size_to_decimal(log_event.best_bid_size),
        best_ask_size = %size_to_decimal(log_event.best_ask_size),
        fair_midpoint = %price_to_decimal(log_event.fair_midpoint),
        raw_best_bid_price = log_event.best_bid_price,
        raw_best_ask_price = log_event.best_ask_price,
        raw_best_bid_size = log_event.best_bid_size,
        raw_best_ask_size = log_event.best_ask_size,
        raw_fair_midpoint = log_event.fair_midpoint,
        timestamp_ms = log_event.timestamp_ms,
        "market_maker fair midpoint"
    );
}

fn current_inventory_state(
    rule: &MarketMakerRule,
    yes_book: &CleanOrderbook,
    no_book: &CleanOrderbook,
    position_read: &crate::position_engine::PositionReadHandle,
    config: &MarketMakerStrategyConfig,
) -> (Decimal, Decimal, Decimal, Decimal, InventoryState) {
    let yes_balance = position_read
        .get_entry(MARKET_MAKER_NAME, &rule.token1)
        .map(|entry| entry.filled_position)
        .unwrap_or(Decimal::ZERO);
    let no_balance = position_read
        .get_entry(MARKET_MAKER_NAME, &rule.token2)
        .map(|entry| entry.filled_position)
        .unwrap_or(Decimal::ZERO);
    let yes_fair_mid = price_to_decimal(compute_fair_midpoint(yes_book));
    let no_fair_mid = price_to_decimal(compute_fair_midpoint(no_book));
    let inventory = compute_inventory_state(
        yes_balance,
        no_balance,
        yes_fair_mid,
        no_fair_mid,
        config.max_inventory_usd,
        config.overweight_ratio,
    );

    (
        yes_balance,
        no_balance,
        yes_fair_mid,
        no_fair_mid,
        inventory,
    )
}

fn log_inventory_state(
    rule: &MarketMakerRule,
    yes_balance: Decimal,
    no_balance: Decimal,
    yes_fair_mid: Decimal,
    no_fair_mid: Decimal,
    inventory: &InventoryState,
) {
    info!(
        target: "order",
        condition_id = %rule.condition_id,
        market_slug = rule.market_slug.as_deref().unwrap_or(""),
        yes_token = %rule.token1,
        no_token = %rule.token2,
        yes_balance = %yes_balance,
        no_balance = %no_balance,
        yes_fair_mid = %yes_fair_mid,
        no_fair_mid = %no_fair_mid,
        yes_value_usd = %inventory.yes_value_usd,
        no_value_usd = %inventory.no_value_usd,
        inventory_value_usd = %inventory.value_usd,
        inventory_ratio = %inventory.ratio,
        inventory_side = ?inventory.side,
        is_overweight = inventory.is_overweight,
        "market_maker inventory state"
    );
}

#[derive(Debug, Clone, PartialEq)]
struct TargetBuyQuoteIntent {
    token_id: String,
    quote: TargetQuote,
}

#[derive(Debug, Clone, PartialEq)]
struct ReconcileResult {
    to_cancel: Vec<OrderRecord>,
    to_place: Vec<TargetBuyQuoteIntent>,
    to_keep: Vec<OrderRecord>,
}

fn reconcile_market_maker_orders(
    targets: &[TargetBuyQuoteIntent],
    current_orders: Vec<OrderRecord>,
    tick_size: Decimal,
    size_tolerance_ratio: Decimal,
) -> ReconcileResult {
    let mut unmatched_current = current_orders;
    let mut to_place = Vec::new();
    let mut to_keep = Vec::new();

    for target in targets {
        let matched_index = unmatched_current.iter().position(|current| {
            if current.token_id.as_str() != target.token_id || current.side != OrderSide::Buy {
                return false;
            }
            if matches!(
                current.local_state,
                LocalOrderState::Filled
                    | LocalOrderState::Cancelled
                    | LocalOrderState::Rejected
                    | LocalOrderState::Failed
                    | LocalOrderState::UnknownTerminal
            ) {
                return false;
            }
            let Some(current_price) = current.price else {
                return false;
            };
            let price_diff = decimal_abs(current_price - target.quote.price);
            let size_diff_ratio = if target.quote.size > Decimal::ZERO {
                decimal_abs(current.remaining_size - target.quote.size) / target.quote.size
            } else {
                Decimal::ONE
            };
            price_diff < tick_size && size_diff_ratio < size_tolerance_ratio
        });

        if let Some(index) = matched_index {
            to_keep.push(unmatched_current.remove(index));
        } else {
            to_place.push(target.clone());
        }
    }

    ReconcileResult {
        to_cancel: unmatched_current,
        to_place,
        to_keep,
    }
}

fn filter_current_orders_for_rule(
    rule: &MarketMakerRule,
    orders: Vec<OrderRecord>,
) -> Vec<OrderRecord> {
    orders
        .into_iter()
        .filter(|order| {
            if !rule.condition_id.is_empty() {
                order.market_id.as_str() == rule.condition_id
            } else {
                order.token_id.as_str() == rule.token1 || order.token_id.as_str() == rule.token2
            }
        })
        .filter(|order| order.side == OrderSide::Buy && order.price.is_some())
        .filter(|order| order.remaining_size > Decimal::ZERO)
        .collect()
}

fn cancel_request_for_order(order: &OrderRecord) -> CancelOrderRequest {
    CancelOrderRequest {
        strategy_id: StrategyId::from(MARKET_MAKER_NAME),
        scope: CancelScope::LocalOrderId {
            local_id: order.local_id.clone(),
            exch_id: order.exch_id.clone(),
            token_id: Some(order.token_id.clone()),
        },
        reason: Some(Arc::from("market_maker_reconcile")),
    }
}

fn target_buy_quote_intents(
    rule: &MarketMakerRule,
    yes_fair_mid: Decimal,
    no_fair_mid: Decimal,
    inventory: &InventoryState,
    config: &MarketMakerStrategyConfig,
) -> Vec<TargetBuyQuoteIntent> {
    let params = target_quote_params(rule, config);
    let skew = compute_quote_skew(inventory.ratio, config.max_skew);
    let mut intents = Vec::new();

    if inventory.ratio <= Decimal::ZERO {
        intents.extend(
            compute_target_buy_quotes_for_token(
                TargetTokenSide::Yes,
                yes_fair_mid,
                skew.yes_skew,
                inventory.is_overweight,
                &params,
            )
            .into_iter()
            .map(|quote| TargetBuyQuoteIntent {
                token_id: rule.token1.clone(),
                quote,
            }),
        );
    }

    if inventory.ratio >= Decimal::ZERO {
        intents.extend(
            compute_target_buy_quotes_for_token(
                TargetTokenSide::No,
                no_fair_mid,
                skew.no_skew,
                inventory.is_overweight,
                &params,
            )
            .into_iter()
            .map(|quote| TargetBuyQuoteIntent {
                token_id: rule.token2.clone(),
                quote,
            }),
        );
    }

    intents
}

fn target_quote_params(
    rule: &MarketMakerRule,
    config: &MarketMakerStrategyConfig,
) -> TargetQuoteParams {
    TargetQuoteParams {
        max_spread: reward_max_spread(
            rule.rewards_max_spread.as_deref(),
            config.default_max_spread,
        ),
        tick_size: config.tick_size,
        min_size: rule
            .rewards_min_size
            .as_deref()
            .map(parse_decimal)
            .unwrap_or(config.min_size),
        normal_quote_levels: config.normal_quote_levels,
        overweight_quote_levels: config.overweight_quote_levels,
        level_ratios: config.level_ratios.clone(),
        level_sizes_usd: config.level_sizes_usd.clone(),
    }
}

fn reward_max_spread(value: Option<&str>, default_max_spread: Decimal) -> Decimal {
    let spread = value.map(parse_decimal).unwrap_or(default_max_spread);
    if spread > Decimal::ONE {
        spread / Decimal::from(100u32)
    } else {
        spread
    }
}

fn parse_decimal(value: &str) -> Decimal {
    Decimal::from_str(value).expect("market maker decimal parameter should parse")
}

fn dec_percent(value: u32) -> Decimal {
    Decimal::from(value) / Decimal::from(100u32)
}

fn send_cooldown_cancel_requests(
    order_gateway: &crate::order_gateway::OrderGatewayHandle,
    rule: &MarketMakerRule,
) {
    for request in cooldown_cancel_requests(rule) {
        if let Err(error) = order_gateway.try_send(OrderRequest::Cancel(request)) {
            warn!(
                target: "order",
                condition_id = %rule.condition_id,
                token1 = %rule.token1,
                token2 = %rule.token2,
                error = ?error,
                "market_maker 冷静期撤单请求投递失败"
            );
        }
    }
}

fn build_place_order_request(
    rule: &MarketMakerRule,
    intent: &TargetBuyQuoteIntent,
    timestamp_ms: u64,
) -> PlaceOrderRequest {
    PlaceOrderRequest {
        strategy_id: StrategyId::from(MARKET_MAKER_NAME),
        market_id: (!rule.condition_id.is_empty())
            .then(|| MarketId::from(rule.condition_id.clone())),
        token_id: TokenId::from(intent.token_id.clone()),
        local_id: LocalOrderId::from(local_order_id(rule, intent, timestamp_ms)),
        side: OrderSide::Buy,
        order_type: GatewayOrderType::Limit {
            time_in_force: TimeInForce::Gtc,
        },
        price: Some(intent.quote.price),
        size: intent.quote.size,
        reason: Some(Arc::from("market_maker_target_buy_quote")),
    }
}

fn local_order_id(
    rule: &MarketMakerRule,
    intent: &TargetBuyQuoteIntent,
    timestamp_ms: u64,
) -> String {
    format!(
        "{MARKET_MAKER_NAME}:{}:{}:L{}:{timestamp_ms}",
        rule.condition_id, intent.token_id, intent.quote.level
    )
}

impl MarketMakerStrategy {
    pub fn from_csv(csv_file: &str) -> anyhow::Result<Option<Self>> {
        Self::from_csv_with_config(csv_file, MarketMakerStrategyConfig::default())
    }

    pub fn from_csv_with_config(
        csv_file: &str,
        config: MarketMakerStrategyConfig,
    ) -> anyhow::Result<Option<Self>> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(csv_file)
            .map_err(|e| anyhow::anyhow!("无法打开 {}: {}", csv_file, e))?;
        let headers = reader.headers()?.clone();
        let header_index = |name: &str| {
            headers
                .iter()
                .position(|header| header.trim().eq_ignore_ascii_case(name))
        };
        let condition_id_index = header_index("condition_id");
        let rewards_max_spread_index = header_index("rewards_max_spread");
        let rewards_min_size_index =
            header_index("rewards_min_size").or_else(|| header_index("reward_min_size"));
        let token1_index = header_index("token1").unwrap_or(0);
        let token2_index = header_index("token2").unwrap_or(1);

        let mut rules = Vec::new();
        for result in reader.records() {
            let record = result?;
            let token1 = record.get(token1_index).map(str::trim).unwrap_or_default();
            let token2 = record.get(token2_index).map(str::trim).unwrap_or_default();
            if token1.is_empty() || token2.is_empty() {
                continue;
            }
            let condition_id = condition_id_index
                .and_then(|index| record.get(index))
                .map(str::trim)
                .unwrap_or_default();
            let rewards_max_spread = rewards_max_spread_index
                .and_then(|index| record.get(index))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            let rewards_min_size = rewards_min_size_index
                .and_then(|index| record.get(index))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);

            rules.push(MarketMakerRule {
                condition_id: condition_id.to_string(),
                market_slug: None,
                token1: token1.to_string(),
                token2: token2.to_string(),
                rewards_max_spread,
                rewards_min_size,
            });
        }

        Self::from_rules_with_config(rules, config)
    }

    pub fn from_pool_entries(
        entries: Vec<ActiveRewardMarketPoolEntry>,
    ) -> anyhow::Result<Option<Self>> {
        Self::from_pool_entries_with_config(entries, MarketMakerStrategyConfig::default())
    }

    pub fn from_pool_entries_with_config(
        entries: Vec<ActiveRewardMarketPoolEntry>,
        config: MarketMakerStrategyConfig,
    ) -> anyhow::Result<Option<Self>> {
        let rules = entries
            .into_iter()
            .map(|entry| MarketMakerRule {
                condition_id: entry.condition_id,
                market_slug: entry.market_slug,
                token1: entry.token1,
                token2: entry.token2,
                rewards_max_spread: entry.rewards_max_spread,
                rewards_min_size: entry.rewards_min_size,
            })
            .collect::<Vec<_>>();
        Self::from_rules_with_config(rules, config)
    }

    pub fn from_rules(rules: Vec<MarketMakerRule>) -> anyhow::Result<Option<Self>> {
        Self::from_rules_with_config(rules, MarketMakerStrategyConfig::default())
    }

    pub fn from_rules_with_config(
        rules: Vec<MarketMakerRule>,
        config: MarketMakerStrategyConfig,
    ) -> anyhow::Result<Option<Self>> {
        if rules.is_empty() {
            return Ok(None);
        }

        let mut related_tokens = BTreeSet::new();
        for rule in &rules {
            related_tokens.insert(rule.token1.clone());
            related_tokens.insert(rule.token2.clone());
        }

        let related_tokens = related_tokens.into_iter().collect::<Vec<_>>();
        let topics = related_tokens
            .iter()
            .map(|token| Arc::<str>::from(token.as_str()))
            .collect::<Vec<_>>();
        let topic_token_regs = related_tokens
            .iter()
            .map(|token| TopicRegistration {
                topic: Arc::<str>::from(token.as_str()),
                tokens: Arc::<[String]>::from(vec![token.clone()]),
            })
            .collect::<Vec<_>>();
        let registration = Arc::new(StrategyRegistration {
            name: Arc::from(MARKET_MAKER_NAME),
            kind: StrategyKind::MarketMaker,
            topics: Arc::<[Arc<str>]>::from(topics),
            topic_tokens: Arc::<[TopicRegistration]>::from(topic_token_regs),
            related_tokens: Arc::<[String]>::from(related_tokens),
        });

        Ok(Some(Self {
            rules: Arc::<[MarketMakerRule]>::from(rules),
            registration,
            config: Arc::new(config),
            notifier: None,
        }))
    }

    pub fn with_notifier(mut self, notifier: Option<Notifier>) -> Self {
        self.notifier = notifier;
        self
    }

    pub fn rules(&self) -> &[MarketMakerRule] {
        self.rules.as_ref()
    }
}

impl Strategy for MarketMakerStrategy {
    fn name(&self) -> &str {
        MARKET_MAKER_NAME
    }

    fn registration(&self) -> &StrategyRegistration {
        self.registration.as_ref()
    }

    fn spawn(
        self,
        market_subscriptions: StrategyMarketSubscriptions,
        order_gateway: crate::order_gateway::OrderGatewayHandle,
        position_read: crate::position_engine::PositionReadHandle,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let config = self.config.clone();
            let notifier = self.notifier.clone();
            let mut books: HashMap<String, Arc<CleanOrderbook>> = HashMap::new();
            let mut price_history: HashMap<String, VecDeque<(u64, Decimal)>> = HashMap::new();
            let mut cooldowns: HashMap<String, CooldownState> = HashMap::new();
            let mut rx = spawn_market_subscription_mux(market_subscriptions, 256);
            while let Some(event) = rx.recv().await {
                log_fair_midpoint(&event);
                let asset_id = event.asset_id.to_string();
                let history = price_history.entry(asset_id.clone()).or_default();
                record_fair_midpoint_history(
                    history,
                    event.book.timestamp_ms,
                    price_to_decimal(compute_fair_midpoint(&event.book)),
                    config.volatility_window_ms,
                );
                log_volatility_state(&asset_id, history, &config);
                books.insert(asset_id, event.book.clone());
                for rule in self.rules.iter() {
                    if let Some(cooldown) =
                        active_cooldown(&mut cooldowns, rule, event.book.timestamp_ms)
                    {
                        info!(
                            target: "order",
                            condition_id = %rule.condition_id,
                            token1 = %rule.token1,
                            token2 = %rule.token2,
                            risk_code = cooldown.code,
                            reason = %cooldown.reason,
                            cooldown_until_ms = cooldown.until_ms,
                            "market_maker 冷静期中，跳过报价"
                        );
                        continue;
                    }
                    let (Some(yes_book), Some(no_book)) =
                        (books.get(&rule.token1), books.get(&rule.token2))
                    else {
                        continue;
                    };
                    let (yes_balance, no_balance, yes_fair_mid, no_fair_mid, inventory) =
                        current_inventory_state(rule, yes_book, no_book, &position_read, &config);
                    log_inventory_state(
                        rule,
                        yes_balance,
                        no_balance,
                        yes_fair_mid,
                        no_fair_mid,
                        &inventory,
                    );
                    let mut target_intents = Vec::new();
                    let mut entered_cooldown = false;
                    for intent in target_buy_quote_intents(
                        rule,
                        yes_fair_mid,
                        no_fair_mid,
                        &inventory,
                        &config,
                    ) {
                        let risk_ctx = MarketMakerQuoteRiskContext {
                            rule,
                            intent: &intent,
                            books: &books,
                            price_history: &price_history,
                            config: &config,
                        };
                        match check_market_maker_quote_risk(&risk_ctx) {
                            MarketMakerRiskDecision::Allow => target_intents.push(intent),
                            MarketMakerRiskDecision::Skip { code, reason } => {
                                let cooldown = enter_cooldown(
                                    &mut cooldowns,
                                    rule,
                                    event.book.timestamp_ms,
                                    code,
                                    reason,
                                    &config,
                                );
                                warn!(
                                    target: "order",
                                    condition_id = %risk_ctx.rule.condition_id,
                                    token_id = %risk_ctx.intent.token_id,
                                    risk_code = cooldown.code,
                                    reason = %cooldown.reason,
                                    cooldown_until_ms = cooldown.until_ms,
                                    cooldown_ms = cooldown.until_ms.saturating_sub(cooldown.triggered_at_ms),
                                    "market_maker 进入冷静期并撤单"
                                );
                                if let Some(notifier) = notifier.as_ref() {
                                    notifier.try_notify(cooldown_risk_notification(
                                        risk_ctx.rule,
                                        risk_ctx.intent,
                                        &cooldown,
                                    ));
                                }
                                send_cooldown_cancel_requests(&order_gateway, rule);
                                entered_cooldown = true;
                                break;
                            }
                        }
                    }
                    if entered_cooldown {
                        continue;
                    }
                    let current_orders = match order_gateway
                        .query_active_orders(StrategyId::from(MARKET_MAKER_NAME))
                        .await
                    {
                        Ok(orders) => filter_current_orders_for_rule(rule, orders),
                        Err(error) => {
                            warn!(
                                target: "order",
                                condition_id = %rule.condition_id,
                                error = ?error,
                                "market_maker 查询当前挂单失败，跳过改单"
                            );
                            continue;
                        }
                    };
                    let quote_params = target_quote_params(rule, &config);
                    let reconcile = reconcile_market_maker_orders(
                        &target_intents,
                        current_orders,
                        quote_params.tick_size,
                        config.reconcile_size_tolerance,
                    );
                    info!(
                        target: "order",
                        condition_id = %rule.condition_id,
                        token1 = %rule.token1,
                        token2 = %rule.token2,
                        target_count = target_intents.len(),
                        keep_count = reconcile.to_keep.len(),
                        cancel_count = reconcile.to_cancel.len(),
                        place_count = reconcile.to_place.len(),
                        "market_maker reconcile orders"
                    );
                    for order in reconcile.to_cancel {
                        let request = cancel_request_for_order(&order);
                        match order_gateway.try_send(OrderRequest::Cancel(request)) {
                            Ok(()) => info!(
                                target: "order",
                                condition_id = %rule.condition_id,
                                local_id = %order.local_id.as_str(),
                                token_id = %order.token_id.as_str(),
                                price = ?order.price,
                                remaining_size = %order.remaining_size,
                                "market_maker reconcile 撤单请求已投递"
                            ),
                            Err(error) => warn!(
                                target: "order",
                                condition_id = %rule.condition_id,
                                local_id = %order.local_id.as_str(),
                                error = ?error,
                                "market_maker reconcile 撤单请求投递失败"
                            ),
                        }
                    }
                    for intent in reconcile.to_place {
                        let request =
                            build_place_order_request(rule, &intent, event.book.timestamp_ms);
                        match order_gateway.try_send(OrderRequest::Place(request)) {
                            Ok(()) => info!(
                                target: "order",
                                condition_id = %rule.condition_id,
                                token_id = %intent.token_id,
                                level = intent.quote.level,
                                price = %intent.quote.price,
                                size = %intent.quote.size,
                                "market_maker 模拟发单请求已投递"
                            ),
                            Err(error) => warn!(
                                target: "order",
                                condition_id = %rule.condition_id,
                                token_id = %intent.token_id,
                                level = intent.quote.level,
                                error = ?error,
                                "market_maker 模拟发单请求投递失败"
                            ),
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};

    use super::*;
    use crate::strategy::{CleanOrderbook, MarketEvent};

    fn clean_book(
        best_bid_price: u16,
        best_ask_price: u16,
        best_bid_size: u32,
        best_ask_size: u32,
    ) -> CleanOrderbook {
        clean_book_at(
            best_bid_price,
            best_ask_price,
            best_bid_size,
            best_ask_size,
            100,
        )
    }

    fn clean_book_at(
        best_bid_price: u16,
        best_ask_price: u16,
        best_bid_size: u32,
        best_ask_size: u32,
        timestamp_ms: u64,
    ) -> CleanOrderbook {
        CleanOrderbook {
            best_bid_price,
            best_bid_size,
            best_ask_price,
            best_ask_size,
            timestamp_ms,
            bids: Arc::new(BTreeMap::new()),
            asks: Arc::new(BTreeMap::new()),
        }
    }

    fn active_entry(condition_id: &str, token1: &str, token2: &str) -> ActiveRewardMarketPoolEntry {
        ActiveRewardMarketPoolEntry {
            condition_id: condition_id.to_string(),
            market_slug: Some(format!("slug-{condition_id}")),
            question: None,
            token1: token1.to_string(),
            token2: token2.to_string(),
            tokens_json: "[]".to_string(),
            market_competitiveness: None,
            rewards_min_size: Some("5".to_string()),
            rewards_max_spread: Some("3".to_string()),
            market_daily_reward: None,
            volume_24hr_clob: None,
            volume_24hr: None,
            liquidity_reward_roi: None,
            build_date_utc: None,
            pool_version: Some(1),
            liquidity_reward_selected: true,
            liquidity_reward_selected_at_ms: Some(100),
            liquidity_reward_select_reason: Some("roi_descending_top_n".to_string()),
            liquidity_reward_select_rank: Some(1),
            liquidity_reward_halted: false,
            liquidity_reward_halted_at_ms: None,
            liquidity_reward_halt_reason: None,
            liquidity_reward_halted_pool_version: None,
        }
    }

    fn dec(numerator: u32, denominator: u32) -> Decimal {
        Decimal::from(numerator) / Decimal::from(denominator)
    }

    fn target_intent(token_id: &str, price: Decimal, size: Decimal) -> TargetBuyQuoteIntent {
        TargetBuyQuoteIntent {
            token_id: token_id.to_string(),
            quote: TargetQuote {
                token_side: TargetTokenSide::Yes,
                level: 1,
                price,
                size,
                size_usd: price * size,
                adjusted_mid: price,
                distance: Decimal::ZERO,
                raw_bid: price,
            },
        }
    }

    fn active_order(local_id: &str, token_id: &str, price: Decimal, size: Decimal) -> OrderRecord {
        active_order_for_market(local_id, "market-a", token_id, price, size)
    }

    fn active_order_for_market(
        local_id: &str,
        market_id: &str,
        token_id: &str,
        price: Decimal,
        size: Decimal,
    ) -> OrderRecord {
        OrderRecord {
            strategy_id: StrategyId::from(MARKET_MAKER_NAME),
            market_id: MarketId::from(market_id),
            token_id: TokenId::from(token_id),
            local_id: LocalOrderId::from(local_id),
            exch_id: None,
            side: OrderSide::Buy,
            order_type: GatewayOrderType::Limit {
                time_in_force: TimeInForce::Gtc,
            },
            price: Some(price),
            original_size: size,
            local_state: LocalOrderState::Open,
            filled_size_total: Decimal::ZERO,
            remaining_size: size,
            avg_fill_price: None,
        }
    }

    async fn collect_gateway_requests(
        mut gateway_rx: tokio::sync::mpsc::Receiver<OrderRequest>,
        mut active_order_replies: VecDeque<Vec<OrderRecord>>,
    ) -> Vec<OrderRequest> {
        let mut requests = Vec::new();
        while let Some(request) = gateway_rx.recv().await {
            match request {
                OrderRequest::Query(crate::order_gateway::OrderQueryRequest::ActiveOrders {
                    reply_tx,
                    ..
                }) => {
                    let _ = reply_tx.send(active_order_replies.pop_front().unwrap_or_default());
                }
                other => requests.push(other),
            }
        }
        requests
    }

    #[test]
    fn reconcile_orders_keeps_current_order_when_price_and_size_are_close() {
        let target = target_intent("yes-token", dec(58, 100), Decimal::from(100u32));
        let current = active_order(
            "current-1",
            "yes-token",
            dec(575, 1000),
            Decimal::from(110u32),
        );

        let result = reconcile_market_maker_orders(
            &[target],
            vec![current.clone()],
            dec(1, 100),
            dec(20, 100),
        );

        assert_eq!(result.to_keep, vec![current]);
        assert!(result.to_cancel.is_empty());
        assert!(result.to_place.is_empty());
    }

    #[test]
    fn reconcile_orders_replaces_current_order_when_price_or_size_drift() {
        let target = target_intent("yes-token", dec(58, 100), Decimal::from(100u32));
        let far_price = active_order(
            "far-price",
            "yes-token",
            dec(56, 100),
            Decimal::from(100u32),
        );
        let far_size = active_order(
            "far-size",
            "yes-token",
            dec(575, 1000),
            Decimal::from(130u32),
        );

        let price_result = reconcile_market_maker_orders(
            &[target.clone()],
            vec![far_price.clone()],
            dec(1, 100),
            dec(20, 100),
        );
        let size_result = reconcile_market_maker_orders(
            &[target.clone()],
            vec![far_size.clone()],
            dec(1, 100),
            dec(20, 100),
        );

        assert_eq!(price_result.to_cancel, vec![far_price]);
        assert_eq!(price_result.to_place, vec![target.clone()]);
        assert!(price_result.to_keep.is_empty());
        assert_eq!(size_result.to_cancel, vec![far_size]);
        assert_eq!(size_result.to_place, vec![target]);
        assert!(size_result.to_keep.is_empty());
    }

    #[test]
    fn reconcile_orders_does_not_match_different_token() {
        let target = target_intent("yes-token", dec(58, 100), Decimal::from(100u32));
        let current = active_order(
            "no-order",
            "no-token",
            dec(575, 1000),
            Decimal::from(100u32),
        );

        let result = reconcile_market_maker_orders(
            &[target.clone()],
            vec![current.clone()],
            dec(1, 100),
            dec(20, 100),
        );

        assert_eq!(result.to_cancel, vec![current]);
        assert_eq!(result.to_place, vec![target]);
        assert!(result.to_keep.is_empty());
    }

    #[test]
    fn default_market_maker_config_matches_existing_quote_parameters() {
        let rule = MarketMakerRule {
            condition_id: "0xabc".to_string(),
            market_slug: None,
            token1: "yes-token".to_string(),
            token2: "no-token".to_string(),
            rewards_max_spread: None,
            rewards_min_size: None,
        };
        let config = MarketMakerStrategyConfig::default();

        let params = target_quote_params(&rule, &config);

        assert_eq!(params.max_spread, dec(3, 100));
        assert_eq!(params.tick_size, dec(1, 100));
        assert_eq!(params.min_size, Decimal::from(5u32));
        assert_eq!(params.normal_quote_levels, 3);
        assert_eq!(params.overweight_quote_levels, 2);
        assert_eq!(
            params.level_ratios,
            vec![dec(40, 100), dec(55, 100), dec(70, 100)]
        );
        assert_eq!(
            params.level_sizes_usd,
            vec![
                Decimal::from(50u32),
                Decimal::from(75u32),
                Decimal::from(100u32)
            ]
        );
    }

    #[test]
    fn custom_market_maker_config_controls_quote_levels_and_sizes() {
        let params = TargetQuoteParams {
            max_spread: dec(4, 100),
            tick_size: dec(1, 100),
            min_size: Decimal::from(5u32),
            normal_quote_levels: 2,
            overweight_quote_levels: 1,
            level_ratios: vec![dec(50, 100), Decimal::ONE],
            level_sizes_usd: vec![Decimal::from(10u32), Decimal::from(20u32)],
        };

        let normal_quotes = compute_target_buy_quotes_for_token(
            TargetTokenSide::Yes,
            dec(50, 100),
            Decimal::ZERO,
            false,
            &params,
        );
        let overweight_quotes = compute_target_buy_quotes_for_token(
            TargetTokenSide::Yes,
            dec(50, 100),
            Decimal::ZERO,
            true,
            &params,
        );

        assert_eq!(normal_quotes.len(), 2);
        assert_eq!(normal_quotes[0].size_usd, Decimal::from(10u32));
        assert_eq!(normal_quotes[1].size_usd, Decimal::from(20u32));
        assert_eq!(overweight_quotes.len(), 1);
        assert_eq!(overweight_quotes[0].size_usd, Decimal::from(10u32));
    }

    #[test]
    fn target_quote_size_is_truncated_to_lot_precision() {
        let params = TargetQuoteParams {
            max_spread: Decimal::ZERO,
            tick_size: dec(1, 100),
            min_size: Decimal::from(5u32),
            normal_quote_levels: 1,
            overweight_quote_levels: 1,
            level_ratios: vec![Decimal::ONE],
            level_sizes_usd: vec![Decimal::from(10u32)],
        };

        let quotes = compute_target_buy_quotes_for_token(
            TargetTokenSide::Yes,
            dec(57, 100),
            Decimal::ZERO,
            false,
            &params,
        );

        assert_eq!(quotes[0].price, dec(57, 100));
        assert_eq!(quotes[0].size, dec(1754, 100));
    }

    #[test]
    fn market_maker_config_rejects_mismatched_level_arrays() {
        let config = crate::config::MarketMakerConfig {
            level_ratios: vec![0.4, 0.7],
            level_sizes_usd: vec![50.0],
            ..Default::default()
        };

        let error = MarketMakerStrategyConfig::try_from(&config)
            .expect_err("mismatched quote level arrays should be rejected");

        assert!(
            error
                .to_string()
                .contains("level_ratios and level_sizes_usd")
        );
    }

    #[test]
    fn inventory_state_values_yes_and_no_with_separate_fair_midpoints() {
        let state = compute_inventory_state(
            Decimal::from(100u32),
            Decimal::from(50u32),
            dec(4, 10),
            dec(6, 10),
            Decimal::from(100u32),
            dec(7, 10),
        );

        assert_eq!(state.yes_value_usd, Decimal::from(40u32));
        assert_eq!(state.no_value_usd, Decimal::from(30u32));
        assert_eq!(state.value_usd, Decimal::from(10u32));
        assert_eq!(state.ratio, dec(1, 10));
        assert_eq!(state.side, InventorySide::LongYes);
        assert!(!state.is_overweight);
    }

    #[test]
    fn inventory_state_uses_no_fair_midpoint_for_no_inventory() {
        let state = compute_inventory_state(
            Decimal::ZERO,
            Decimal::from(200u32),
            dec(4, 10),
            dec(6, 10),
            Decimal::from(100u32),
            dec(7, 10),
        );

        assert_eq!(state.yes_value_usd, Decimal::ZERO);
        assert_eq!(state.no_value_usd, Decimal::from(120u32));
        assert_eq!(state.value_usd, Decimal::from(-120));
        assert_eq!(state.ratio, -Decimal::ONE);
        assert_eq!(state.side, InventorySide::LongNo);
        assert!(state.is_overweight);
    }

    #[test]
    fn inventory_state_is_flat_when_yes_and_no_values_match() {
        let state = compute_inventory_state(
            Decimal::from(100u32),
            Decimal::from(50u32),
            dec(3, 10),
            dec(6, 10),
            Decimal::from(100u32),
            dec(7, 10),
        );

        assert_eq!(state.yes_value_usd, Decimal::from(30u32));
        assert_eq!(state.no_value_usd, Decimal::from(30u32));
        assert_eq!(state.value_usd, Decimal::ZERO);
        assert_eq!(state.ratio, Decimal::ZERO);
        assert_eq!(state.side, InventorySide::Flat);
        assert!(!state.is_overweight);
    }

    #[test]
    fn quote_skew_only_moves_no_up_when_inventory_ratio_is_positive() {
        let skew = compute_quote_skew(dec(4, 10), dec(1, 100));

        assert_eq!(skew.yes_skew, Decimal::ZERO);
        assert_eq!(skew.no_skew, dec(16, 10000));
    }

    #[test]
    fn quote_skew_only_moves_yes_up_when_inventory_ratio_is_negative() {
        let skew = compute_quote_skew(-dec(4, 10), dec(1, 100));

        assert_eq!(skew.yes_skew, dec(16, 10000));
        assert_eq!(skew.no_skew, Decimal::ZERO);
    }

    #[test]
    fn quote_skew_clamps_inventory_ratio_before_squaring() {
        let long_yes = compute_quote_skew(Decimal::from(2u32), dec(1, 100));
        let long_no = compute_quote_skew(Decimal::from(-2), dec(1, 100));

        assert_eq!(long_yes.yes_skew, Decimal::ZERO);
        assert_eq!(long_yes.no_skew, dec(1, 100));
        assert_eq!(long_no.yes_skew, dec(1, 100));
        assert_eq!(long_no.no_skew, Decimal::ZERO);
    }

    #[test]
    fn simulate_quote_skew_cases() {
        let max_skew = dec(1, 100);
        let yes_fair_mid = dec(5, 10);
        let no_fair_mid = dec(5, 10);
        let ratios = [
            -Decimal::ONE,
            -dec(7, 10),
            -dec(4, 10),
            Decimal::ZERO,
            dec(4, 10),
            dec(7, 10),
            Decimal::ONE,
        ];

        for ratio in ratios {
            let ratio_abs = decimal_abs(ratio);
            let ratio_squared = ratio * ratio;
            let skew = compute_quote_skew(ratio, max_skew);
            let yes_adjusted_mid = yes_fair_mid + skew.yes_skew;
            let no_adjusted_mid = no_fair_mid + skew.no_skew;
            let inventory_side = if ratio > Decimal::ZERO {
                "YES 偏多：只让 NO 更容易买到"
            } else if ratio < Decimal::ZERO {
                "NO 偏多：只让 YES 更容易买到"
            } else {
                "库存平衡"
            };

            println!(
                "\n库存偏斜 ratio = {}\n  {}\n  abs(ratio) = {}\n  ratio^2 = {} * {} = {}\n  max_skew = {}\n  YES skew = {}\n  NO skew = {}\n  YES fair_mid 示例 = {}\n  YES adjusted_mid = {} + {} = {}\n  NO fair_mid 示例 = {}\n  NO adjusted_mid = {} + {} = {}",
                ratio,
                inventory_side,
                ratio_abs,
                ratio,
                ratio,
                ratio_squared,
                max_skew,
                skew.yes_skew,
                skew.no_skew,
                yes_fair_mid,
                yes_fair_mid,
                skew.yes_skew,
                yes_adjusted_mid,
                no_fair_mid,
                no_fair_mid,
                skew.no_skew,
                no_adjusted_mid,
            );
        }
    }

    #[test]
    fn simulate_target_buy_quote_cases() {
        let params = TargetQuoteParams {
            max_spread: dec(3, 100),
            tick_size: dec(1, 100),
            min_size: Decimal::from(5u32),
            normal_quote_levels: 3,
            overweight_quote_levels: 2,
            level_ratios: vec![dec(40, 100), dec(55, 100), dec(70, 100)],
            level_sizes_usd: vec![
                Decimal::from(50u32),
                Decimal::from(75u32),
                Decimal::from(100u32),
            ],
        };
        let max_skew = dec(1, 100);
        let yes_fair_mid = dec(5, 10);
        let no_fair_mid = dec(5, 10);
        let cases = [
            ("NO 满仓：只买 YES", -Decimal::ONE, true),
            ("NO 偏多：只买 YES", -dec(7, 10), false),
            ("库存平衡：YES/NO 都正常买", Decimal::ZERO, false),
            ("YES 偏多：只买 NO", dec(7, 10), false),
            ("YES 满仓：只买 NO", Decimal::ONE, true),
        ];

        for (name, ratio, is_overweight) in cases {
            let skew = compute_quote_skew(ratio, max_skew);
            let mut quote_groups = Vec::new();
            if ratio <= Decimal::ZERO {
                quote_groups.push((
                    TargetTokenSide::Yes,
                    yes_fair_mid,
                    skew.yes_skew,
                    compute_target_buy_quotes_for_token(
                        TargetTokenSide::Yes,
                        yes_fair_mid,
                        skew.yes_skew,
                        is_overweight,
                        &params,
                    ),
                ));
            }
            if ratio >= Decimal::ZERO {
                quote_groups.push((
                    TargetTokenSide::No,
                    no_fair_mid,
                    skew.no_skew,
                    compute_target_buy_quotes_for_token(
                        TargetTokenSide::No,
                        no_fair_mid,
                        skew.no_skew,
                        is_overweight,
                        &params,
                    ),
                ));
            }

            println!(
                "\n场景: {}\n  inventory_ratio = {}\n  is_overweight = {}\n  max_spread(单边奖励距离) = {}\n  max_skew = {}\n  yes_skew = {}\n  no_skew = {}",
                name,
                ratio,
                is_overweight,
                params.max_spread,
                max_skew,
                skew.yes_skew,
                skew.no_skew,
            );

            for (token_side, fair_mid, token_skew, quotes) in quote_groups {
                println!(
                    "  {:?}: fair_mid={} adjusted_mid={} + {} = {}",
                    token_side,
                    fair_mid,
                    fair_mid,
                    token_skew,
                    fair_mid + token_skew,
                );
                for quote in quotes {
                    println!(
                        "    L{} BUY: distance=max_spread*ratio={} raw_bid={} price=floor_tick({})={} size=max({}/{}, min {})={}",
                        quote.level,
                        quote.distance,
                        quote.raw_bid,
                        quote.raw_bid,
                        quote.price,
                        quote.size_usd,
                        quote.price,
                        params.min_size,
                        quote.size,
                    );
                }
            }
        }
    }

    #[test]
    fn simulate_inventory_skew_cases() {
        struct Case {
            name: &'static str,
            yes_balance: Decimal,
            no_balance: Decimal,
            yes_fair_mid: Decimal,
            no_fair_mid: Decimal,
        }

        let max_inventory_usd = Decimal::from(100u32);
        let overweight_ratio = dec(7, 10);
        let cases = [
            Case {
                name: "空仓",
                yes_balance: Decimal::ZERO,
                no_balance: Decimal::ZERO,
                yes_fair_mid: dec(4, 10),
                no_fair_mid: dec(6, 10),
            },
            Case {
                name: "YES 偏多但未超限",
                yes_balance: Decimal::from(100u32),
                no_balance: Decimal::ZERO,
                yes_fair_mid: dec(4, 10),
                no_fair_mid: dec(6, 10),
            },
            Case {
                name: "NO 偏多但未超限",
                yes_balance: Decimal::ZERO,
                no_balance: Decimal::from(80u32),
                yes_fair_mid: dec(4, 10),
                no_fair_mid: dec(5, 10),
            },
            Case {
                name: "YES 超重",
                yes_balance: Decimal::from(250u32),
                no_balance: Decimal::ZERO,
                yes_fair_mid: dec(4, 10),
                no_fair_mid: dec(6, 10),
            },
            Case {
                name: "NO 超重",
                yes_balance: Decimal::ZERO,
                no_balance: Decimal::from(250u32),
                yes_fair_mid: dec(6, 10),
                no_fair_mid: dec(4, 10),
            },
            Case {
                name: "YES/NO 价值抵消",
                yes_balance: Decimal::from(100u32),
                no_balance: Decimal::from(50u32),
                yes_fair_mid: dec(3, 10),
                no_fair_mid: dec(6, 10),
            },
        ];

        for case in cases {
            let state = compute_inventory_state(
                case.yes_balance,
                case.no_balance,
                case.yes_fair_mid,
                case.no_fair_mid,
                max_inventory_usd,
                overweight_ratio,
            );
            let raw_ratio = if max_inventory_usd > Decimal::ZERO {
                state.value_usd / max_inventory_usd
            } else {
                Decimal::ZERO
            };

            println!(
                "\n场景: {}\n  YES价值 = {} * {} = {} USD\n  NO价值  = {} * {} = {} USD\n  净库存价值 = {} - {} = {} USD\n  原始偏斜比例 = {} / {} = {}\n  截断后偏斜比例 = {}\n  方向 = {:?}\n  是否超重 = {} (阈值: {})",
                case.name,
                case.yes_balance,
                case.yes_fair_mid,
                state.yes_value_usd,
                case.no_balance,
                case.no_fair_mid,
                state.no_value_usd,
                state.yes_value_usd,
                state.no_value_usd,
                state.value_usd,
                state.value_usd,
                max_inventory_usd,
                raw_ratio,
                state.ratio,
                state.side,
                state.is_overweight,
                overweight_ratio,
            );
        }
    }

    #[test]
    fn volatility_guard_requires_minimum_samples() {
        let history = VecDeque::from([
            (1, dec(50, 100)),
            (2, dec(51, 100)),
            (3, dec(52, 100)),
            (4, dec(53, 100)),
        ]);

        assert!(!price_history_is_volatile(&history, dec(2, 100), 5));
    }

    #[test]
    fn volatility_guard_rejects_when_price_range_exceeds_threshold() {
        let history = VecDeque::from([
            (1, dec(50, 100)),
            (2, dec(51, 100)),
            (3, dec(52, 100)),
            (4, dec(53, 100)),
            (5, dec(521, 1000)),
        ]);

        assert!(price_history_is_volatile(&history, dec(2, 100), 5));
    }

    #[test]
    fn volatility_state_reports_window_range_and_threshold() {
        let history = VecDeque::from([
            (1, dec(50, 100)),
            (2, dec(51, 100)),
            (3, dec(49, 100)),
            (4, dec(52, 100)),
            (5, dec(515, 1000)),
        ]);

        let state = price_history_volatility_state(&history, dec(2, 100), 5)
            .expect("history should produce volatility state");

        assert_eq!(state.sample_count, 5);
        assert_eq!(state.min_fair_mid, dec(49, 100));
        assert_eq!(state.max_fair_mid, dec(52, 100));
        assert_eq!(state.range, dec(3, 100));
        assert_eq!(state.threshold, dec(2, 100));
        assert!(state.is_volatile);
    }

    #[test]
    fn fair_midpoint_history_prunes_prices_outside_window() {
        let mut history = VecDeque::new();
        record_fair_midpoint_history(&mut history, 1, dec(40, 100), 100);
        record_fair_midpoint_history(&mut history, 50, dec(50, 100), 100);
        record_fair_midpoint_history(&mut history, 102, dec(60, 100), 100);

        assert_eq!(
            history,
            VecDeque::from([(50, dec(50, 100)), (102, dec(60, 100))])
        );
    }

    #[test]
    fn quote_risk_chain_skips_volatile_token_with_reason_code() {
        let rule = MarketMakerRule {
            condition_id: "0xabc".to_string(),
            market_slug: None,
            token1: "maker-token-1".to_string(),
            token2: "maker-token-2".to_string(),
            rewards_max_spread: None,
            rewards_min_size: None,
        };
        let intent = TargetBuyQuoteIntent {
            token_id: "maker-token-1".to_string(),
            quote: TargetQuote {
                token_side: TargetTokenSide::Yes,
                level: 1,
                price: dec(49, 100),
                size: Decimal::from(100u32),
                size_usd: Decimal::from(49u32),
                adjusted_mid: dec(50, 100),
                distance: dec(1, 100),
                raw_bid: dec(49, 100),
            },
        };
        let price_history = HashMap::from([(
            "maker-token-1".to_string(),
            VecDeque::from([
                (1, dec(50, 100)),
                (2, dec(51, 100)),
                (3, dec(52, 100)),
                (4, dec(53, 100)),
                (5, dec(521, 1000)),
            ]),
        )]);
        let books = HashMap::from([(
            "maker-token-1".to_string(),
            Arc::new(clean_book(4_900, 5_100, 100, 100)),
        )]);
        let config = MarketMakerStrategyConfig::default();
        let ctx = MarketMakerQuoteRiskContext {
            rule: &rule,
            intent: &intent,
            books: &books,
            price_history: &price_history,
            config: &config,
        };

        let decision = check_market_maker_quote_risk(&ctx);

        assert_eq!(
            decision,
            MarketMakerRiskDecision::Skip {
                code: "price_volatility",
                reason: "price volatility too high".to_string(),
            }
        );
    }

    #[test]
    fn quote_risk_chain_skips_when_market_spread_is_abnormally_wide() {
        let rule = MarketMakerRule {
            condition_id: "0xabc".to_string(),
            market_slug: None,
            token1: "maker-token-1".to_string(),
            token2: "maker-token-2".to_string(),
            rewards_max_spread: Some("0.03".to_string()),
            rewards_min_size: None,
        };
        let intent = TargetBuyQuoteIntent {
            token_id: "maker-token-1".to_string(),
            quote: TargetQuote {
                token_side: TargetTokenSide::Yes,
                level: 1,
                price: dec(44, 100),
                size: Decimal::from(100u32),
                size_usd: Decimal::from(44u32),
                adjusted_mid: dec(45, 100),
                distance: dec(1, 100),
                raw_bid: dec(44, 100),
            },
        };
        let price_history = HashMap::new();
        let books = HashMap::from([(
            "maker-token-1".to_string(),
            Arc::new(clean_book(4_000, 5_000, 100, 100)),
        )]);
        let config = MarketMakerStrategyConfig::default();
        let ctx = MarketMakerQuoteRiskContext {
            rule: &rule,
            intent: &intent,
            books: &books,
            price_history: &price_history,
            config: &config,
        };

        let decision = check_market_maker_quote_risk(&ctx);

        assert_eq!(
            decision,
            MarketMakerRiskDecision::Skip {
                code: "abnormal_market_spread",
                reason: "abnormal market spread".to_string(),
            }
        );
    }

    #[test]
    fn quote_risk_chain_skips_when_fair_midpoint_is_out_of_safe_range() {
        let rule = MarketMakerRule {
            condition_id: "0xabc".to_string(),
            market_slug: None,
            token1: "maker-token-1".to_string(),
            token2: "maker-token-2".to_string(),
            rewards_max_spread: Some("0.03".to_string()),
            rewards_min_size: None,
        };
        let intent = TargetBuyQuoteIntent {
            token_id: "maker-token-1".to_string(),
            quote: TargetQuote {
                token_side: TargetTokenSide::Yes,
                level: 1,
                price: dec(1, 100),
                size: Decimal::from(100u32),
                size_usd: Decimal::from(1u32),
                adjusted_mid: dec(2, 100),
                distance: dec(1, 100),
                raw_bid: dec(1, 100),
            },
        };
        let price_history = HashMap::new();
        let books = HashMap::from([(
            "maker-token-1".to_string(),
            Arc::new(clean_book(900, 1_100, 100, 100)),
        )]);
        let config = MarketMakerStrategyConfig::default();
        let ctx = MarketMakerQuoteRiskContext {
            rule: &rule,
            intent: &intent,
            books: &books,
            price_history: &price_history,
            config: &config,
        };

        let decision = check_market_maker_quote_risk(&ctx);

        assert_eq!(
            decision,
            MarketMakerRiskDecision::Skip {
                code: "fair_midpoint_out_of_range",
                reason: "fair midpoint out of safe range".to_string(),
            }
        );
    }

    #[test]
    fn cooldown_risk_notification_contains_market_maker_context() {
        let rule = MarketMakerRule {
            condition_id: "0xabc".to_string(),
            market_slug: None,
            token1: "maker-token-1".to_string(),
            token2: "maker-token-2".to_string(),
            rewards_max_spread: None,
            rewards_min_size: None,
        };
        let intent = TargetBuyQuoteIntent {
            token_id: "maker-token-1".to_string(),
            quote: TargetQuote {
                token_side: TargetTokenSide::Yes,
                level: 1,
                price: dec(49, 100),
                size: Decimal::from(100u32),
                size_usd: Decimal::from(49u32),
                adjusted_mid: dec(50, 100),
                distance: dec(1, 100),
                raw_bid: dec(49, 100),
            },
        };
        let cooldown = CooldownState {
            until_ms: 1100,
            code: "price_volatility",
            reason: "price volatility too high".to_string(),
            triggered_at_ms: 100,
        };

        let event = cooldown_risk_notification(&rule, &intent, &cooldown);

        match event {
            crate::notification::NotificationEvent::RiskEvent(risk) => {
                assert_eq!(risk.source, "market_maker_cooldown");
                assert_eq!(risk.strategy_id.as_deref(), Some("market_maker"));
                assert_eq!(risk.local_order_id, None);
                assert_eq!(risk.market_id.as_deref(), Some("0xabc"));
                assert_eq!(risk.token_id.as_deref(), Some("maker-token-1"));
                assert_eq!(risk.risk_code, "price_volatility");
                assert_eq!(risk.reason, "price volatility too high");
            }
            other => panic!("unexpected notification: {other:?}"),
        }
    }

    #[test]
    fn fair_midpoint_returns_mid_when_best_sizes_are_equal() {
        let book = clean_book(40, 60, 100, 100);

        let fair_mid = compute_fair_midpoint(&book);

        assert_eq!(fair_mid, 50);
    }

    #[test]
    fn fair_midpoint_moves_toward_ask_when_bid_size_is_larger() {
        let book = clean_book(40, 60, 300, 100);

        let fair_mid = compute_fair_midpoint(&book);

        assert_eq!(fair_mid, 55);
    }

    #[test]
    fn fair_midpoint_moves_toward_bid_when_ask_size_is_larger() {
        let book = clean_book(40, 60, 100, 300);

        let fair_mid = compute_fair_midpoint(&book);

        assert_eq!(fair_mid, 45);
    }

    #[test]
    fn fair_midpoint_falls_back_to_mid_when_best_sizes_are_zero() {
        let book = clean_book(41, 60, 0, 0);

        let fair_mid = compute_fair_midpoint(&book);

        assert_eq!(fair_mid, 51);
    }

    #[test]
    fn fair_midpoint_is_clamped_to_best_bid_and_ask() {
        let bid_clamped = compute_fair_midpoint(&clean_book(40, 40, 0, 100));
        let ask_clamped = compute_fair_midpoint(&clean_book(60, 60, 100, 0));

        assert_eq!(bid_clamped, 40);
        assert_eq!(ask_clamped, 60);
    }

    #[test]
    fn from_csv_registers_csv_tokens() {
        let csv_path = std::env::temp_dir().join(format!(
            "market_maker_from_csv_{}_{}.csv",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time should move forward")
                .as_nanos()
        ));
        std::fs::write(
            &csv_path,
            "token1,token2,topic,reward_min_orders,reward_max_spread_cents,reward_min_size,reward_daily_pool,fixed_price\ncsv-token-1,csv-token-2,market_maker,,4,100,50,false\n",
        )
        .expect("csv should write");

        let strategy = MarketMakerStrategy::from_csv(csv_path.to_str().expect("utf8 path"))
            .expect("csv should load")
            .expect("csv row should build strategy");

        assert_eq!(strategy.rules().len(), 1);
        assert_eq!(strategy.rules()[0].condition_id, "");
        assert_eq!(strategy.rules()[0].token1, "csv-token-1");
        assert_eq!(strategy.rules()[0].token2, "csv-token-2");
        assert_eq!(
            strategy.registration().related_tokens.as_ref(),
            &["csv-token-1".to_string(), "csv-token-2".to_string()]
        );
    }

    #[test]
    fn from_csv_accepts_optional_condition_id_column() {
        let csv_path = std::env::temp_dir().join(format!(
            "market_maker_from_csv_condition_{}_{}.csv",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time should move forward")
                .as_nanos()
        ));
        std::fs::write(
            &csv_path,
            "condition_id,token1,token2\n0xabc,csv-token-1,csv-token-2\n",
        )
        .expect("csv should write");

        let strategy = MarketMakerStrategy::from_csv(csv_path.to_str().expect("utf8 path"))
            .expect("csv should load")
            .expect("csv row should build strategy");

        assert_eq!(strategy.rules().len(), 1);
        assert_eq!(strategy.rules()[0].condition_id, "0xabc");
        assert_eq!(strategy.rules()[0].token1, "csv-token-1");
        assert_eq!(strategy.rules()[0].token2, "csv-token-2");
    }

    #[test]
    fn from_csv_loads_reward_spread_and_min_size_columns() {
        let csv_path = std::env::temp_dir().join(format!(
            "market_maker_from_csv_rewards_{}_{}.csv",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time should move forward")
                .as_nanos()
        ));
        std::fs::write(
            &csv_path,
            "token1,token2,topic,reward_min_orders,rewards_max_spread,reward_min_size,reward_daily_pool,fixed_price\ncsv-token-1,csv-token-2,market_maker,,0.05,200,50,false\n",
        )
        .expect("csv should write");

        let strategy = MarketMakerStrategy::from_csv(csv_path.to_str().expect("utf8 path"))
            .expect("csv should load")
            .expect("csv row should build strategy");

        assert_eq!(strategy.rules().len(), 1);
        assert_eq!(
            strategy.rules()[0].rewards_max_spread.as_deref(),
            Some("0.05")
        );
        assert_eq!(strategy.rules()[0].rewards_min_size.as_deref(), Some("200"));
    }

    #[test]
    fn cooldown_cancel_requests_use_market_when_condition_id_exists() {
        let rule = MarketMakerRule {
            condition_id: "0xabc".to_string(),
            market_slug: None,
            token1: "yes-token".to_string(),
            token2: "no-token".to_string(),
            rewards_max_spread: None,
            rewards_min_size: None,
        };

        let requests = cooldown_cancel_requests(&rule);

        assert_eq!(requests.len(), 1);
        assert!(matches!(
            &requests[0].scope,
            CancelScope::Market { market_id } if market_id.as_str() == "0xabc"
        ));
    }

    #[test]
    fn cooldown_cancel_requests_fall_back_to_both_tokens_without_condition_id() {
        let rule = MarketMakerRule {
            condition_id: String::new(),
            market_slug: None,
            token1: "yes-token".to_string(),
            token2: "no-token".to_string(),
            rewards_max_spread: None,
            rewards_min_size: None,
        };

        let requests = cooldown_cancel_requests(&rule);

        assert_eq!(requests.len(), 2);
        assert!(matches!(
            &requests[0].scope,
            CancelScope::Token { token_id } if token_id.as_str() == "yes-token"
        ));
        assert!(matches!(
            &requests[1].scope,
            CancelScope::Token { token_id } if token_id.as_str() == "no-token"
        ));
    }

    #[test]
    fn cooldown_expires_before_market_can_quote_again() {
        let rule = MarketMakerRule {
            condition_id: "0xabc".to_string(),
            market_slug: None,
            token1: "yes-token".to_string(),
            token2: "no-token".to_string(),
            rewards_max_spread: None,
            rewards_min_size: None,
        };
        let config = MarketMakerStrategyConfig::default();
        let mut cooldowns = HashMap::new();
        enter_cooldown(
            &mut cooldowns,
            &rule,
            1_000,
            "abnormal_market_spread",
            "wide".to_string(),
            &config,
        );

        assert!(active_cooldown(&mut cooldowns, &rule, 1_001).is_some());
        assert!(active_cooldown(&mut cooldowns, &rule, 61_000).is_none());
        assert!(cooldowns.is_empty());
    }

    #[test]
    fn from_pool_entries_returns_none_for_empty_pool() {
        let strategy = MarketMakerStrategy::from_pool_entries(Vec::new())
            .expect("empty pool should not error");

        assert!(strategy.is_none());
    }

    #[test]
    fn from_pool_entries_registers_selected_pool_tokens() {
        let strategy = MarketMakerStrategy::from_pool_entries(vec![
            active_entry("0xbbb", "token-b2", "token-b1"),
            active_entry("0xaaa", "token-a1", "token-b1"),
        ])
        .expect("selected pool should build strategy")
        .expect("non-empty pool should create strategy");

        let registration = strategy.registration();
        assert_eq!(strategy.name(), "market_maker");
        assert_eq!(registration.name.as_ref(), "market_maker");
        assert_eq!(
            registration.kind,
            crate::strategy::StrategyKind::MarketMaker
        );
        assert_eq!(
            registration.topics.as_ref(),
            &[
                Arc::<str>::from("token-a1"),
                Arc::<str>::from("token-b1"),
                Arc::<str>::from("token-b2"),
            ]
        );
        assert_eq!(
            registration.related_tokens.as_ref(),
            &[
                "token-a1".to_string(),
                "token-b1".to_string(),
                "token-b2".to_string(),
            ]
        );
        let regs = registration.topic_tokens.as_ref();
        assert_eq!(regs.len(), 3);
        for token in registration.related_tokens.iter() {
            assert!(regs.iter().any(|reg| {
                reg.topic.as_ref() == token.as_str() && reg.tokens.as_ref() == &[token.clone()]
            }));
        }
    }

    #[test]
    fn fair_midpoint_log_event_uses_market_event_book() {
        let event = MarketEvent {
            topic: Arc::from("maker-token-1"),
            asset_id: Arc::from("maker-token-1"),
            book: Arc::new(clean_book(40, 60, 300, 100)),
        };

        let log_event = fair_midpoint_log_event(&event);

        assert_eq!(log_event.asset_id.as_ref(), "maker-token-1");
        assert_eq!(log_event.topic.as_ref(), "maker-token-1");
        assert_eq!(log_event.best_bid_price, 40);
        assert_eq!(log_event.best_ask_price, 60);
        assert_eq!(log_event.best_bid_size, 300);
        assert_eq!(log_event.best_ask_size, 100);
        assert_eq!(log_event.fair_midpoint, 55);
        assert_eq!(log_event.timestamp_ms, 100);
    }

    #[tokio::test]
    async fn spawn_skips_quotes_for_volatile_token() {
        let strategy = MarketMakerStrategy::from_pool_entries(vec![active_entry(
            "0xabc",
            "maker-token-1",
            "maker-token-2",
        )])
        .expect("market maker should build")
        .expect("non-empty pool should create strategy");
        let (topic1_tx, topic1_rx) = tokio::sync::broadcast::channel(16);
        let (topic2_tx, topic2_rx) = tokio::sync::broadcast::channel(16);
        let subscriptions = StrategyMarketSubscriptions {
            topics: vec![
                (Arc::from("maker-token-1"), topic1_rx),
                (Arc::from("maker-token-2"), topic2_rx),
            ],
        };
        let (gateway_handle, mut gateway_rx) =
            crate::order_gateway::OrderGatewayHandle::new_for_test(
                8,
                crate::order_gateway::GatewayPhase::Live,
            );
        let (_ingestor, ingest_handle, _persist_rx) =
            crate::position_engine::PositionIngestor::new_for_test(8, 8);

        let handle = strategy.spawn(subscriptions, gateway_handle, ingest_handle.read_handle());
        for (timestamp_ms, bid, ask) in [
            (1_000, 4_900, 5_100),
            (2_000, 4_950, 5_150),
            (3_000, 5_000, 5_200),
            (4_000, 5_050, 5_250),
            (5_000, 5_200, 5_400),
        ] {
            topic1_tx
                .send(MarketEvent {
                    topic: Arc::from("maker-token-1"),
                    asset_id: Arc::from("maker-token-1"),
                    book: Arc::new(clean_book_at(bid, ask, 100, 100, timestamp_ms)),
                })
                .expect("yes market event should send");
        }
        topic2_tx
            .send(MarketEvent {
                topic: Arc::from("maker-token-2"),
                asset_id: Arc::from("maker-token-2"),
                book: Arc::new(clean_book_at(4_900, 5_100, 100, 300, 6_000)),
            })
            .expect("no market event should send");
        drop(topic1_tx);
        drop(topic2_tx);
        handle
            .await
            .expect("market maker task should exit when event channel closes");

        let mut orders = Vec::new();
        while let Ok(request) = gateway_rx.try_recv() {
            orders.push(request);
        }

        assert_eq!(orders.len(), 1);
        let crate::order_gateway::OrderRequest::Cancel(request) = orders.remove(0) else {
            panic!("market maker should cancel orders after volatility risk");
        };
        assert_eq!(request.strategy_id.as_str(), "market_maker");
        assert!(matches!(
            request.scope,
            CancelScope::Market { market_id } if market_id.as_str() == "0xabc"
        ));
    }

    #[tokio::test]
    async fn spawn_uses_token_cancels_for_csv_rule_without_condition_id() {
        let strategy = MarketMakerStrategy::from_rules(vec![MarketMakerRule {
            condition_id: String::new(),
            market_slug: None,
            token1: "maker-token-1".to_string(),
            token2: "maker-token-2".to_string(),
            rewards_max_spread: None,
            rewards_min_size: None,
        }])
        .expect("market maker should build")
        .expect("non-empty rules should create strategy");
        let (topic1_tx, topic1_rx) = tokio::sync::broadcast::channel(16);
        let (topic2_tx, topic2_rx) = tokio::sync::broadcast::channel(16);
        let subscriptions = StrategyMarketSubscriptions {
            topics: vec![
                (Arc::from("maker-token-1"), topic1_rx),
                (Arc::from("maker-token-2"), topic2_rx),
            ],
        };
        let (gateway_handle, mut gateway_rx) =
            crate::order_gateway::OrderGatewayHandle::new_for_test(
                8,
                crate::order_gateway::GatewayPhase::Live,
            );
        let (_ingestor, ingest_handle, _persist_rx) =
            crate::position_engine::PositionIngestor::new_for_test(8, 8);

        let handle = strategy.spawn(subscriptions, gateway_handle, ingest_handle.read_handle());
        for (timestamp_ms, bid, ask) in [
            (1_000, 4_900, 5_100),
            (2_000, 4_950, 5_150),
            (3_000, 5_000, 5_200),
            (4_000, 5_050, 5_250),
            (5_000, 5_200, 5_400),
        ] {
            topic1_tx
                .send(MarketEvent {
                    topic: Arc::from("maker-token-1"),
                    asset_id: Arc::from("maker-token-1"),
                    book: Arc::new(clean_book_at(bid, ask, 100, 100, timestamp_ms)),
                })
                .expect("yes market event should send");
        }
        topic2_tx
            .send(MarketEvent {
                topic: Arc::from("maker-token-2"),
                asset_id: Arc::from("maker-token-2"),
                book: Arc::new(clean_book_at(4_900, 5_100, 100, 300, 6_000)),
            })
            .expect("no market event should send");
        drop(topic1_tx);
        drop(topic2_tx);
        handle
            .await
            .expect("market maker task should exit when event channel closes");

        let mut cancels = Vec::new();
        while let Ok(request) = gateway_rx.try_recv() {
            let crate::order_gateway::OrderRequest::Cancel(request) = request else {
                panic!("market maker should only cancel orders after volatility risk");
            };
            cancels.push(request);
        }

        assert_eq!(cancels.len(), 2);
        assert!(matches!(
            &cancels[0].scope,
            CancelScope::Token { token_id } if token_id.as_str() == "maker-token-1"
        ));
        assert!(matches!(
            &cancels[1].scope,
            CancelScope::Token { token_id } if token_id.as_str() == "maker-token-2"
        ));
    }

    #[tokio::test]
    async fn spawn_keeps_existing_close_orders_instead_of_placing_duplicates() {
        let strategy = MarketMakerStrategy::from_pool_entries(vec![active_entry(
            "0xabc",
            "maker-token-1",
            "maker-token-2",
        )])
        .expect("market maker should build")
        .expect("non-empty pool should create strategy");
        let (topic1_tx, topic1_rx) = tokio::sync::broadcast::channel(8);
        let (topic2_tx, topic2_rx) = tokio::sync::broadcast::channel(8);
        let subscriptions = StrategyMarketSubscriptions {
            topics: vec![
                (Arc::from("maker-token-1"), topic1_rx),
                (Arc::from("maker-token-2"), topic2_rx),
            ],
        };
        let (gateway_handle, gateway_rx) = crate::order_gateway::OrderGatewayHandle::new_for_test(
            16,
            crate::order_gateway::GatewayPhase::Live,
        );
        let (_ingestor, ingest_handle, _persist_rx) =
            crate::position_engine::PositionIngestor::new_for_test(8, 8);

        let request_collector = tokio::spawn(collect_gateway_requests(
            gateway_rx,
            VecDeque::from([vec![active_order_for_market(
                "existing-yes-l1",
                "0xabc",
                "maker-token-1",
                dec(49, 100),
                dec(1040, 10),
            )]]),
        ));
        let handle = strategy.spawn(subscriptions, gateway_handle, ingest_handle.read_handle());
        topic1_tx
            .send(MarketEvent {
                topic: Arc::from("maker-token-1"),
                asset_id: Arc::from("maker-token-1"),
                book: Arc::new(clean_book(4_900, 5_100, 300, 100)),
            })
            .expect("yes market event should send");
        topic2_tx
            .send(MarketEvent {
                topic: Arc::from("maker-token-2"),
                asset_id: Arc::from("maker-token-2"),
                book: Arc::new(clean_book(4_900, 5_100, 100, 300)),
            })
            .expect("no market event should send");
        drop(topic1_tx);
        drop(topic2_tx);
        handle
            .await
            .expect("market maker task should exit when event channel closes");
        let requests = request_collector
            .await
            .expect("request collector should exit after handle drops");

        assert_eq!(
            requests
                .iter()
                .filter(|request| matches!(request, OrderRequest::Place(request) if request.local_id.as_str().contains("maker-token-1:L1")))
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn spawn_cancels_drifted_order_and_places_target_quote() {
        let strategy = MarketMakerStrategy::from_pool_entries(vec![active_entry(
            "0xabc",
            "maker-token-1",
            "maker-token-2",
        )])
        .expect("market maker should build")
        .expect("non-empty pool should create strategy");
        let (topic1_tx, topic1_rx) = tokio::sync::broadcast::channel(8);
        let (topic2_tx, topic2_rx) = tokio::sync::broadcast::channel(8);
        let subscriptions = StrategyMarketSubscriptions {
            topics: vec![
                (Arc::from("maker-token-1"), topic1_rx),
                (Arc::from("maker-token-2"), topic2_rx),
            ],
        };
        let (gateway_handle, gateway_rx) = crate::order_gateway::OrderGatewayHandle::new_for_test(
            16,
            crate::order_gateway::GatewayPhase::Live,
        );
        let (_ingestor, ingest_handle, _persist_rx) =
            crate::position_engine::PositionIngestor::new_for_test(8, 8);

        let request_collector = tokio::spawn(collect_gateway_requests(
            gateway_rx,
            VecDeque::from([vec![active_order_for_market(
                "old-yes-l1",
                "0xabc",
                "maker-token-1",
                dec(45, 100),
                Decimal::from(100u32),
            )]]),
        ));
        let handle = strategy.spawn(subscriptions, gateway_handle, ingest_handle.read_handle());
        topic1_tx
            .send(MarketEvent {
                topic: Arc::from("maker-token-1"),
                asset_id: Arc::from("maker-token-1"),
                book: Arc::new(clean_book(4_900, 5_100, 300, 100)),
            })
            .expect("yes market event should send");
        topic2_tx
            .send(MarketEvent {
                topic: Arc::from("maker-token-2"),
                asset_id: Arc::from("maker-token-2"),
                book: Arc::new(clean_book(4_900, 5_100, 100, 300)),
            })
            .expect("no market event should send");
        drop(topic1_tx);
        drop(topic2_tx);
        handle
            .await
            .expect("market maker task should exit when event channel closes");
        let requests = request_collector
            .await
            .expect("request collector should exit after handle drops");

        assert!(requests.iter().any(|request| matches!(
            request,
            OrderRequest::Cancel(request)
                if matches!(&request.scope, CancelScope::LocalOrderId { local_id, token_id, .. }
                    if local_id.as_str() == "old-yes-l1"
                        && token_id.as_ref().is_some_and(|token_id| token_id.as_str() == "maker-token-1"))
        )));
        assert!(requests.iter().any(|request| matches!(
            request,
            OrderRequest::Place(request)
                if request.local_id.as_str().contains("maker-token-1:L1")
        )));
    }

    #[tokio::test]
    async fn spawn_emits_buy_orders_to_gateway() {
        let strategy = MarketMakerStrategy::from_pool_entries(vec![active_entry(
            "0xabc",
            "maker-token-1",
            "maker-token-2",
        )])
        .expect("market maker should build")
        .expect("non-empty pool should create strategy");
        let (topic1_tx, topic1_rx) = tokio::sync::broadcast::channel(8);
        let (topic2_tx, topic2_rx) = tokio::sync::broadcast::channel(8);
        let subscriptions = StrategyMarketSubscriptions {
            topics: vec![
                (Arc::from("maker-token-1"), topic1_rx),
                (Arc::from("maker-token-2"), topic2_rx),
            ],
        };
        let (gateway_handle, gateway_rx) = crate::order_gateway::OrderGatewayHandle::new_for_test(
            16,
            crate::order_gateway::GatewayPhase::Live,
        );
        let (_ingestor, ingest_handle, _persist_rx) =
            crate::position_engine::PositionIngestor::new_for_test(8, 8);

        let request_collector = tokio::spawn(collect_gateway_requests(
            gateway_rx,
            VecDeque::from([Vec::new()]),
        ));
        let handle = strategy.spawn(subscriptions, gateway_handle, ingest_handle.read_handle());
        topic1_tx
            .send(MarketEvent {
                topic: Arc::from("maker-token-1"),
                asset_id: Arc::from("maker-token-1"),
                book: Arc::new(clean_book(4_900, 5_100, 300, 100)),
            })
            .expect("yes market event should send");
        topic2_tx
            .send(MarketEvent {
                topic: Arc::from("maker-token-2"),
                asset_id: Arc::from("maker-token-2"),
                book: Arc::new(clean_book(4_900, 5_100, 100, 300)),
            })
            .expect("no market event should send");
        drop(topic1_tx);
        drop(topic2_tx);
        handle
            .await
            .expect("market maker task should exit when event channel closes");
        let orders = request_collector
            .await
            .expect("request collector should exit after handle drops");

        assert_eq!(orders.len(), 6);
        for request in orders {
            let crate::order_gateway::OrderRequest::Place(request) = request else {
                panic!("market maker should only place orders");
            };
            assert_eq!(request.strategy_id.as_str(), "market_maker");
            assert_eq!(request.market_id.as_ref().unwrap().as_str(), "0xabc");
            assert!(
                request.token_id.as_str() == "maker-token-1"
                    || request.token_id.as_str() == "maker-token-2"
            );
            assert_eq!(request.side, crate::order_gateway::OrderSide::Buy);
            assert!(matches!(
                request.order_type,
                crate::order_gateway::GatewayOrderType::Limit {
                    time_in_force: crate::order_gateway::TimeInForce::Gtc
                }
            ));
            assert!(request.price.unwrap() > Decimal::ZERO);
            assert!(request.size >= Decimal::from(5u32));
            assert_eq!(
                request.reason.as_deref(),
                Some("market_maker_target_buy_quote")
            );
        }
    }
}
