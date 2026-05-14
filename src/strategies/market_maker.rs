use std::collections::BTreeSet;
use std::sync::Arc;

use tracing::info;

use crate::storage::ActiveRewardMarketPoolEntry;
use crate::strategy::{
    CleanOrderbook, MarketEvent, OrderSignal, Strategy, StrategyEvent, StrategyRegistration,
    TopicRegistration,
};

const MARKET_MAKER_TOPIC: &str = "market_maker";
const MARKET_MAKER_NAME: &str = "market_maker";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketMakerRule {
    pub condition_id: String,
    pub market_slug: Option<String>,
    pub token1: String,
    pub token2: String,
}

pub struct MarketMakerStrategy {
    rules: Arc<[MarketMakerRule]>,
    registration: Arc<StrategyRegistration>,
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
        best_bid_price = log_event.best_bid_price,
        best_ask_price = log_event.best_ask_price,
        best_bid_size = log_event.best_bid_size,
        best_ask_size = log_event.best_ask_size,
        fair_midpoint = log_event.fair_midpoint,
        timestamp_ms = log_event.timestamp_ms,
        "market_maker fair midpoint"
    );
}

impl MarketMakerStrategy {
    pub fn from_pool_entries(
        entries: Vec<ActiveRewardMarketPoolEntry>,
    ) -> anyhow::Result<Option<Self>> {
        if entries.is_empty() {
            return Ok(None);
        }

        let mut rules = Vec::with_capacity(entries.len());
        let mut related_tokens = BTreeSet::new();
        for entry in entries {
            related_tokens.insert(entry.token1.clone());
            related_tokens.insert(entry.token2.clone());
            rules.push(MarketMakerRule {
                condition_id: entry.condition_id,
                market_slug: entry.market_slug,
                token1: entry.token1,
                token2: entry.token2,
            });
        }

        let related_tokens = related_tokens.into_iter().collect::<Vec<_>>();
        let topic = Arc::<str>::from(MARKET_MAKER_TOPIC);
        let registration = Arc::new(StrategyRegistration {
            name: Arc::from(MARKET_MAKER_NAME),
            topics: Arc::<[Arc<str>]>::from(vec![topic.clone()]),
            topic_tokens: Arc::<[TopicRegistration]>::from(vec![TopicRegistration {
                topic,
                tokens: Arc::<[String]>::from(related_tokens.clone()),
            }]),
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
        mut rx: tokio::sync::mpsc::Receiver<StrategyEvent>,
        _order_tx: tokio::sync::mpsc::Sender<OrderSignal>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                match event {
                    StrategyEvent::Market(event) => log_fair_midpoint(&event),
                    StrategyEvent::Positions(_)
                    | StrategyEvent::OrderStatus(_)
                    | StrategyEvent::OrderFill(_)
                    | StrategyEvent::TradeConfirmed(_)
                    | StrategyEvent::RewardPoolRemoval(_) => {}
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::strategy::{CleanOrderbook, MarketEvent};

    fn clean_book(
        best_bid_price: u16,
        best_ask_price: u16,
        best_bid_size: u32,
        best_ask_size: u32,
    ) -> CleanOrderbook {
        CleanOrderbook {
            best_bid_price,
            best_bid_size,
            best_ask_price,
            best_ask_size,
            timestamp_ms: 100,
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
            rewards_min_size: None,
            rewards_max_spread: None,
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
            registration.topics.as_ref(),
            &[Arc::<str>::from("market_maker")]
        );
        assert_eq!(
            registration.related_tokens.as_ref(),
            &[
                "token-a1".to_string(),
                "token-b1".to_string(),
                "token-b2".to_string(),
            ]
        );
        assert_eq!(registration.topic_tokens.len(), 1);
        assert_eq!(registration.topic_tokens[0].topic.as_ref(), "market_maker");
        assert_eq!(
            registration.topic_tokens[0].tokens.as_ref(),
            registration.related_tokens.as_ref()
        );
    }

    #[test]
    fn fair_midpoint_log_event_uses_market_event_book() {
        let event = MarketEvent {
            topic: Arc::from("market_maker"),
            asset_id: Arc::from("maker-token-1"),
            book: clean_book(40, 60, 300, 100),
        };

        let log_event = fair_midpoint_log_event(&event);

        assert_eq!(log_event.asset_id.as_ref(), "maker-token-1");
        assert_eq!(log_event.topic.as_ref(), "market_maker");
        assert_eq!(log_event.best_bid_price, 40);
        assert_eq!(log_event.best_ask_price, 60);
        assert_eq!(log_event.best_bid_size, 300);
        assert_eq!(log_event.best_ask_size, 100);
        assert_eq!(log_event.fair_midpoint, 55);
        assert_eq!(log_event.timestamp_ms, 100);
    }

    #[tokio::test]
    async fn spawn_consumes_events_without_emitting_order_signals() {
        let strategy = MarketMakerStrategy::from_pool_entries(vec![active_entry(
            "0xabc",
            "maker-token-1",
            "maker-token-2",
        )])
        .expect("market maker should build")
        .expect("non-empty pool should create strategy");
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(8);
        let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(8);

        let handle = strategy.spawn(event_rx, order_tx);
        event_tx
            .send(StrategyEvent::Market(MarketEvent {
                topic: Arc::from("market_maker"),
                asset_id: Arc::from("maker-token-1"),
                book: clean_book(40, 60, 300, 100),
            }))
            .await
            .expect("market event should send");
        drop(event_tx);
        handle
            .await
            .expect("market maker task should exit when event channel closes");

        assert!(order_rx.try_recv().is_err());
    }
}
