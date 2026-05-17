use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;

use crate::order_gateway::OrderGatewayHandle;

use polymarket_client_sdk_v2::types::Decimal;

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
pub struct PairEntry {
    pub tokens: [String; 2],
    pub topic: Arc<str>,
}

#[derive(Debug, Clone)]
pub struct CleanOrderbook {
    pub best_bid_price: u16,
    pub best_bid_size: u32,
    pub best_ask_price: u16,
    pub best_ask_size: u32,
    pub timestamp_ms: u64,
    pub bids: Arc<std::collections::BTreeMap<u16, u32>>,
    pub asks: Arc<std::collections::BTreeMap<u16, u32>>,
}

#[derive(Debug, Clone)]
pub struct MarketEvent {
    pub topic: Arc<str>,
    pub asset_id: Arc<str>,
    pub book: CleanOrderbook,
}

#[derive(Debug, Clone)]
pub struct PositionChangedEvent {
    pub changed_assets: Arc<[String]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivateRiskDecision {
    Allow,
    Reject { code: Arc<str>, reason: Arc<str> },
}

pub trait PrivateRiskCheck<I, S>: Send + Sync {
    fn check_place(
        &self,
        intent: &I,
        state: &S,
        position: Option<&crate::position_engine::PositionEntrySnapshot>,
    ) -> PrivateRiskDecision;
}

pub struct AllowAllPrivateRisk;

impl<I, S> PrivateRiskCheck<I, S> for AllowAllPrivateRisk {
    fn check_place(
        &self,
        _intent: &I,
        _state: &S,
        _position: Option<&crate::position_engine::PositionEntrySnapshot>,
    ) -> PrivateRiskDecision {
        PrivateRiskDecision::Allow
    }
}

#[derive(Debug, Clone)]
pub enum StrategyEvent {
    Market(MarketEvent),
    PositionChanged(PositionChangedEvent),
}

#[derive(Debug, Clone)]
pub struct TopicRegistration {
    pub topic: Arc<str>,
    pub tokens: Arc<[String]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyKind {
    MarketMaker,
    PairArbitrage,
    TrendFollowing,
    MeanReversion,
    Arbitrage,
    Hedging,
    Liquidation,
    Monitoring,
}

#[derive(Debug, Clone)]
pub struct StrategyRegistration {
    pub name: Arc<str>,
    pub kind: StrategyKind,
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
    pub remote_order_id: Option<String>,
    pub strategy: Arc<str>,
    pub topic: Option<Arc<str>>,
    pub token: String,
    pub side: QuoteSide,
    pub price: Decimal,
    pub order_size: Decimal,
}

pub type OrderCorrelationMap = Arc<DashMap<String, LocalOrderMeta>>;

pub trait Strategy: Send + 'static {
    fn name(&self) -> &str;
    fn registration(&self) -> &StrategyRegistration;

    fn spawn(
        self,
        rx: tokio::sync::mpsc::Receiver<StrategyEvent>,
        order_gateway: OrderGatewayHandle,
    ) -> tokio::task::JoinHandle<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestIntent;

    #[derive(Debug)]
    struct TestState;

    #[test]
    fn allow_all_private_risk_allows_strategy_intent() {
        let risk = AllowAllPrivateRisk;

        let decision = risk.check_place(&TestIntent, &TestState, None);

        assert_eq!(decision, PrivateRiskDecision::Allow);
    }
}
