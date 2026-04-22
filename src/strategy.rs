use std::collections::HashMap;
use std::sync::Arc;

use polymarket_client_sdk::types::Decimal;

#[derive(Clone)]
pub struct Filters {
    pub min_diff: Decimal,
    pub max_spread: Decimal,
    pub min_price: Decimal,
    pub max_price: Decimal,
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
        buy_price: Decimal,
        sell_price: Decimal,
        cancelled_order_ids: Arc<[String]>,
        new_order_ids: Arc<[String]>,
    },
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
}

#[derive(Clone)]
pub struct StrategyHandle {
    pub name: Arc<str>,
    pub topics: Arc<[Arc<str>]>,
    pub tx: tokio::sync::mpsc::Sender<MarketEvent>,
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

pub trait Strategy: Send + 'static {
    fn name(&self) -> &str;
    fn spawn(
        self,
        rx: tokio::sync::mpsc::Receiver<MarketEvent>,
        order_tx: tokio::sync::mpsc::Sender<OrderSignal>,
    ) -> tokio::task::JoinHandle<()>;
}
