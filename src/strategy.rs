use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;

use polymarket_client_sdk::types::Decimal;

#[derive(Clone)]
pub struct Filters {
    pub min_diff: Decimal,
    pub max_spread: Decimal,
    pub min_price: Decimal,
    pub max_price: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub enum OrderSignal {
    PairArbitrage {
        token0: String,
        token1: String,
        ask0: Decimal,
        ask1: Decimal,
        gap: Decimal,
    },
    MidRequote {
        topic: Arc<str>,
        token: String,
        mid: Decimal,
        side: QuoteSide,
        price: Decimal,
        cancelled_order_ids: Arc<[String]>,
        new_order_ids: Arc<[String]>,
    },
}

#[derive(Debug, Clone)]
pub enum UnifiedOrder {
    PairArbitrage {
        token0: String,
        token1: String,
        ask0: Decimal,
        ask1: Decimal,
        gap: Decimal,
    },
    MidRequote {
        topic: Arc<str>,
        token: String,
        mid: Decimal,
        side: QuoteSide,
        price: Decimal,
        cancelled_order_ids: Arc<[String]>,
        new_order_ids: Arc<[String]>,
    },
}

impl From<OrderSignal> for UnifiedOrder {
    fn from(signal: OrderSignal) -> Self {
        match signal {
            OrderSignal::PairArbitrage {
                token0,
                token1,
                ask0,
                ask1,
                gap,
            } => Self::PairArbitrage {
                token0,
                token1,
                ask0,
                ask1,
                gap,
            },
            OrderSignal::MidRequote {
                topic,
                token,
                mid,
                side,
                price,
                cancelled_order_ids,
                new_order_ids,
            } => Self::MidRequote {
                topic,
                token,
                mid,
                side,
                price,
                cancelled_order_ids,
                new_order_ids,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct PairEntry {
    pub tokens: [String; 2],
    pub topic: Arc<str>,
}

#[derive(Debug, Clone, Copy)]
pub struct CleanOrderbook {
    pub best_bid_price: u16,
    pub best_bid_size: u32,
    pub best_ask_price: u16,
    pub best_ask_size: u32,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone)]
pub struct MarketEvent {
    pub topic: Arc<str>,
    pub asset_id: Arc<str>,
    pub book: CleanOrderbook,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PositionView {
    pub asset_id: String,
    pub size: Decimal,
    pub avg_price: Decimal,
    pub cur_price: Decimal,
    pub current_value: Decimal,
    pub cash_pnl: Decimal,
    pub title: Arc<str>,
    pub outcome: Arc<str>,
}

#[derive(Debug, Clone)]
pub struct PositionSnapshot {
    pub by_asset: Arc<HashMap<String, PositionView>>,
}

#[derive(Debug, Clone)]
pub struct PositionsUpdateEvent {
    pub snapshot: Arc<PositionSnapshot>,
    pub changed_assets: Arc<[String]>,
}

#[derive(Debug, Clone)]
pub struct RelevantPositionsUpdate {
    pub snapshot: Arc<PositionSnapshot>,
    pub changed_assets: Arc<[String]>,
}

#[derive(Debug, Clone)]
pub enum StrategyEvent {
    Market(MarketEvent),
    Positions(PositionsUpdateEvent),
}

#[derive(Debug, Clone)]
pub struct TopicRegistration {
    pub topic: Arc<str>,
    pub tokens: Arc<[String]>,
}

#[derive(Debug, Clone)]
pub struct StrategyRegistration {
    pub name: Arc<str>,
    pub topics: Arc<[Arc<str>]>,
    pub topic_tokens: Arc<[TopicRegistration]>,
    pub related_tokens: Arc<[String]>,
}

#[derive(Clone)]
pub struct StrategyHandle {
    pub name: Arc<str>,
    pub topics: Arc<[Arc<str>]>,
    pub related_tokens: Arc<[String]>,
    pub tx: tokio::sync::mpsc::Sender<StrategyEvent>,
}

pub fn merge_topic_tokens(
    registrations: &[StrategyRegistration],
) -> HashMap<Arc<str>, Vec<String>> {
    let mut topic_tokens: HashMap<Arc<str>, Vec<String>> = HashMap::new();
    for registration in registrations {
        for topic_registration in registration.topic_tokens.iter() {
            let tokens = topic_tokens
                .entry(topic_registration.topic.clone())
                .or_default();
            tokens.extend(topic_registration.tokens.iter().cloned());
        }
    }
    for tokens in topic_tokens.values_mut() {
        tokens.sort();
        tokens.dedup();
    }
    topic_tokens
}

pub fn build_token_topics(
    topic_tokens: &HashMap<Arc<str>, Vec<String>>,
) -> HashMap<String, Arc<[Arc<str>]>> {
    let mut token_topics: HashMap<String, Vec<Arc<str>>> = HashMap::new();
    for (topic, tokens) in topic_tokens {
        for token in tokens {
            let topics = token_topics.entry(token.clone()).or_default();
            if !topics.iter().any(|existing| existing == topic) {
                topics.push(topic.clone());
            }
        }
    }
    token_topics
        .into_iter()
        .map(|(token, topics)| (token, Arc::<[Arc<str>]>::from(topics)))
        .collect()
}

#[derive(Debug, Clone)]
pub struct LocalOrderMeta {
    pub local_order_id: String,
    pub strategy: Arc<str>,
    pub topic: Option<Arc<str>>,
    pub token: String,
    pub side: QuoteSide,
    pub price: Decimal,
}

pub type OrderCorrelationMap = Arc<DashMap<String, LocalOrderMeta>>;

pub trait Strategy: Send + 'static {
    fn name(&self) -> &str;
    fn registration(&self) -> &StrategyRegistration;

    fn relevant_positions(
        &self,
        event: &PositionsUpdateEvent,
    ) -> Option<RelevantPositionsUpdate> {
        let related_tokens = &self.registration().related_tokens;
        let changed_assets: Vec<String> = event
            .changed_assets
            .iter()
            .filter(|asset| related_tokens.iter().any(|token| token == *asset))
            .cloned()
            .collect();
        if changed_assets.is_empty() {
            return None;
        }

        Some(RelevantPositionsUpdate {
            snapshot: event.snapshot.clone(),
            changed_assets: changed_assets.into(),
        })
    }

    fn spawn(
        self,
        rx: tokio::sync::mpsc::Receiver<StrategyEvent>,
        order_tx: tokio::sync::mpsc::Sender<OrderSignal>,
    ) -> tokio::task::JoinHandle<()>;
}
