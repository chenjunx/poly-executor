use std::collections::{BTreeSet, HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;

use tracing::{info, warn};

use polymarket_client_sdk_v2::types::Decimal;

use crate::order_gateway::{
    GatewayOrderType, LocalOrderId, MarketId, OrderRequest, OrderSide, PlaceOrderRequest,
    StrategyId, TimeInForce, TokenId,
};
use crate::storage::ActiveRewardMarketPoolEntry;
use crate::strategy::{
    CleanOrderbook, MarketEvent, Strategy, StrategyKind, StrategyMarketSubscriptions,
    StrategyRegistration, TopicRegistration, spawn_market_subscription_mux,
};

const MARKET_MAKER_NAME: &str = "market_maker";
const PRICE_SCALE: u32 = 10_000;
const MAX_INVENTORY_USD: u32 = 100;
const OVERWEIGHT_RATIO_NUMERATOR: u32 = 7;
const OVERWEIGHT_RATIO_DENOMINATOR: u32 = 10;
const DEFAULT_MAX_SPREAD: &str = "0.03";
const DEFAULT_TICK_SIZE: &str = "0.01";
const DEFAULT_MIN_SIZE: u32 = 5;
const DEFAULT_MAX_SKEW: &str = "0.01";
const VOLATILITY_WINDOW_MS: u64 = 5 * 60 * 1000;
const VOLATILITY_MIN_SAMPLES: usize = 5;

fn volatility_threshold() -> Decimal {
    Decimal::from(2u32) / Decimal::from(100u32)
}

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
    let num_levels = if is_overweight { 2 } else { 3 };
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
            let size = decimal_max(size_usd / price, params.min_size);
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

fn price_history_is_volatile(
    history: &VecDeque<(u64, Decimal)>,
    threshold: Decimal,
    min_samples: usize,
) -> bool {
    if history.len() < min_samples {
        return false;
    }

    let mut min_price = history[0].1;
    let mut max_price = history[0].1;
    for (_, price) in history.iter().skip(1) {
        if *price < min_price {
            min_price = *price;
        }
        if *price > max_price {
            max_price = *price;
        }
    }

    max_price - min_price > threshold
}

fn token_price_is_volatile(
    price_history: &HashMap<String, VecDeque<(u64, Decimal)>>,
    token_id: &str,
) -> bool {
    price_history.get(token_id).is_some_and(|history| {
        price_history_is_volatile(history, volatility_threshold(), VOLATILITY_MIN_SAMPLES)
    })
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

struct MarketMakerQuoteRiskContext<'a> {
    rule: &'a MarketMakerRule,
    intent: &'a TargetBuyQuoteIntent,
    books: &'a HashMap<String, Arc<CleanOrderbook>>,
    price_history: &'a HashMap<String, VecDeque<(u64, Decimal)>>,
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
        > reward_max_spread(ctx.rule.rewards_max_spread.as_deref()) * Decimal::from(2u32)
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
    if fair_mid < dec_percent(15) || fair_mid > dec_percent(85) {
        MarketMakerRiskDecision::Skip {
            code: "fair_midpoint_out_of_range",
            reason: "fair midpoint out of safe range".to_string(),
        }
    } else {
        MarketMakerRiskDecision::Allow
    }
}

fn check_quote_volatility(ctx: &MarketMakerQuoteRiskContext<'_>) -> MarketMakerRiskDecision {
    if token_price_is_volatile(ctx.price_history, &ctx.intent.token_id) {
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
        Decimal::from(MAX_INVENTORY_USD),
        Decimal::from(OVERWEIGHT_RATIO_NUMERATOR) / Decimal::from(OVERWEIGHT_RATIO_DENOMINATOR),
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

fn target_buy_quote_intents(
    rule: &MarketMakerRule,
    yes_fair_mid: Decimal,
    no_fair_mid: Decimal,
    inventory: &InventoryState,
) -> Vec<TargetBuyQuoteIntent> {
    let params = target_quote_params(rule);
    let skew = compute_quote_skew(inventory.ratio, parse_decimal(DEFAULT_MAX_SKEW));
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

fn target_quote_params(rule: &MarketMakerRule) -> TargetQuoteParams {
    TargetQuoteParams {
        max_spread: reward_max_spread(rule.rewards_max_spread.as_deref()),
        tick_size: parse_decimal(DEFAULT_TICK_SIZE),
        min_size: rule
            .rewards_min_size
            .as_deref()
            .map(parse_decimal)
            .unwrap_or_else(|| Decimal::from(DEFAULT_MIN_SIZE)),
        level_ratios: vec![dec_percent(40), dec_percent(55), dec_percent(70)],
        level_sizes_usd: vec![
            Decimal::from(50u32),
            Decimal::from(75u32),
            Decimal::from(100u32),
        ],
    }
}

fn reward_max_spread(value: Option<&str>) -> Decimal {
    let spread = value
        .map(parse_decimal)
        .unwrap_or_else(|| parse_decimal(DEFAULT_MAX_SPREAD));
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

fn quote_dedupe_key(rule: &MarketMakerRule, intent: &TargetBuyQuoteIntent) -> String {
    format!(
        "{}:{}:L{}:{}:{}",
        rule.condition_id,
        intent.token_id,
        intent.quote.level,
        intent.quote.price,
        intent.quote.size
    )
}

impl MarketMakerStrategy {
    pub fn from_csv(csv_file: &str) -> anyhow::Result<Option<Self>> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(csv_file)
            .map_err(|e| anyhow::anyhow!("无法打开 {}: {}", csv_file, e))?;

        let mut rules = Vec::new();
        for result in reader.records() {
            let record = result?;
            if record.len() < 2 {
                continue;
            }

            let token1 = record[0].trim();
            let token2 = record[1].trim();
            if token1.is_empty() || token2.is_empty() {
                continue;
            }

            rules.push(MarketMakerRule {
                condition_id: String::new(),
                market_slug: None,
                token1: token1.to_string(),
                token2: token2.to_string(),
                rewards_max_spread: None,
                rewards_min_size: None,
            });
        }

        Self::from_rules(rules)
    }

    pub fn from_pool_entries(
        entries: Vec<ActiveRewardMarketPoolEntry>,
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
        Self::from_rules(rules)
    }

    pub fn from_rules(rules: Vec<MarketMakerRule>) -> anyhow::Result<Option<Self>> {
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
        }))
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
            let mut books: HashMap<String, Arc<CleanOrderbook>> = HashMap::new();
            let mut price_history: HashMap<String, VecDeque<(u64, Decimal)>> = HashMap::new();
            let mut submitted_quotes: BTreeSet<String> = BTreeSet::new();
            let mut rx = spawn_market_subscription_mux(market_subscriptions, 256);
            while let Some(event) = rx.recv().await {
                log_fair_midpoint(&event);
                let asset_id = event.asset_id.to_string();
                record_fair_midpoint_history(
                    price_history.entry(asset_id.clone()).or_default(),
                    event.book.timestamp_ms,
                    price_to_decimal(compute_fair_midpoint(&event.book)),
                    VOLATILITY_WINDOW_MS,
                );
                books.insert(asset_id, event.book.clone());
                for rule in self.rules.iter() {
                    let (Some(yes_book), Some(no_book)) =
                        (books.get(&rule.token1), books.get(&rule.token2))
                    else {
                        continue;
                    };
                    let (yes_balance, no_balance, yes_fair_mid, no_fair_mid, inventory) =
                        current_inventory_state(rule, yes_book, no_book, &position_read);
                    log_inventory_state(
                        rule,
                        yes_balance,
                        no_balance,
                        yes_fair_mid,
                        no_fair_mid,
                        &inventory,
                    );
                    for intent in
                        target_buy_quote_intents(rule, yes_fair_mid, no_fair_mid, &inventory)
                    {
                        let risk_ctx = MarketMakerQuoteRiskContext {
                            rule,
                            intent: &intent,
                            books: &books,
                            price_history: &price_history,
                        };
                        match check_market_maker_quote_risk(&risk_ctx) {
                            MarketMakerRiskDecision::Allow => {}
                            MarketMakerRiskDecision::Skip { code, reason } => {
                                warn!(
                                    target: "order",
                                    condition_id = %risk_ctx.rule.condition_id,
                                    token_id = %risk_ctx.intent.token_id,
                                    risk_code = code,
                                    reason = %reason,
                                    "market_maker 跳过报价"
                                );
                                continue;
                            }
                        }
                        let dedupe_key = quote_dedupe_key(rule, &intent);
                        if submitted_quotes.contains(&dedupe_key) {
                            continue;
                        }
                        let request =
                            build_place_order_request(rule, &intent, event.book.timestamp_ms);
                        match order_gateway.try_send(OrderRequest::Place(request)) {
                            Ok(()) => {
                                submitted_quotes.insert(dedupe_key);
                                info!(
                                    target: "order",
                                    condition_id = %rule.condition_id,
                                    token_id = %intent.token_id,
                                    level = intent.quote.level,
                                    price = %intent.quote.price,
                                    size = %intent.quote.size,
                                    "market_maker 模拟发单请求已投递"
                                );
                            }
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
        let ctx = MarketMakerQuoteRiskContext {
            rule: &rule,
            intent: &intent,
            books: &books,
            price_history: &price_history,
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
        let ctx = MarketMakerQuoteRiskContext {
            rule: &rule,
            intent: &intent,
            books: &books,
            price_history: &price_history,
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
        let ctx = MarketMakerQuoteRiskContext {
            rule: &rule,
            intent: &intent,
            books: &books,
            price_history: &price_history,
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
        assert_eq!(strategy.rules()[0].token1, "csv-token-1");
        assert_eq!(strategy.rules()[0].token2, "csv-token-2");
        assert_eq!(
            strategy.registration().related_tokens.as_ref(),
            &["csv-token-1".to_string(), "csv-token-2".to_string()]
        );
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

        assert_eq!(orders.len(), 3);
        for request in orders {
            let crate::order_gateway::OrderRequest::Place(request) = request else {
                panic!("market maker should only place orders");
            };
            assert_eq!(request.token_id.as_str(), "maker-token-2");
        }
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
        let (gateway_handle, mut gateway_rx) =
            crate::order_gateway::OrderGatewayHandle::new_for_test(
                8,
                crate::order_gateway::GatewayPhase::Live,
            );
        let (_ingestor, ingest_handle, _persist_rx) =
            crate::position_engine::PositionIngestor::new_for_test(8, 8);

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

        let mut orders = Vec::new();
        while let Ok(request) = gateway_rx.try_recv() {
            orders.push(request);
        }

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
