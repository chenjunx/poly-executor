use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use tracing::warn;

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
    pub bids: Arc<BTreeMap<u16, u32>>,
    pub asks: Arc<BTreeMap<u16, u32>>,
}

#[derive(Debug, Clone)]
pub struct MarketEvent {
    pub topic: Arc<str>,
    pub asset_id: Arc<str>,
    pub book: Arc<CleanOrderbook>,
}

#[derive(Debug, Clone)]
pub struct MarketAssetEvent {
    pub asset_id: Arc<str>,
    pub topics: Arc<[Arc<str>]>,
    pub book: Arc<CleanOrderbook>,
}

#[derive(Debug)]
pub struct StrategyMarketSubscriptions {
    pub topics: Vec<(Arc<str>, tokio::sync::broadcast::Receiver<MarketEvent>)>,
}

pub fn build_topic_broadcasts(
    topic_tokens: &HashMap<Arc<str>, Vec<String>>,
    capacity: usize,
) -> HashMap<Arc<str>, tokio::sync::broadcast::Sender<MarketEvent>> {
    topic_tokens
        .keys()
        .cloned()
        .map(|topic| {
            let (tx, _rx) = tokio::sync::broadcast::channel(capacity.max(1));
            (topic, tx)
        })
        .collect()
}

pub fn subscribe_strategy_topics(
    registration: &StrategyRegistration,
    topic_txs: &HashMap<Arc<str>, tokio::sync::broadcast::Sender<MarketEvent>>,
) -> anyhow::Result<StrategyMarketSubscriptions> {
    let mut topics = Vec::with_capacity(registration.topics.len());
    for topic in registration.topics.iter() {
        let tx = topic_txs.get(topic).ok_or_else(|| {
            anyhow::anyhow!("策略 {} 缺少行情 topic 订阅: {}", registration.name, topic)
        })?;
        topics.push((topic.clone(), tx.subscribe()));
    }
    Ok(StrategyMarketSubscriptions { topics })
}

pub fn spawn_market_subscription_mux(
    subscriptions: StrategyMarketSubscriptions,
    capacity: usize,
) -> tokio::sync::mpsc::Receiver<MarketEvent> {
    let (tx, rx) = tokio::sync::mpsc::channel(capacity.max(1));
    for (topic, mut topic_rx) in subscriptions.topics {
        let tx = tx.clone();
        tokio::spawn(async move {
            loop {
                match topic_rx.recv().await {
                    Ok(event) => {
                        if tx.try_send(event).is_err() {
                            warn!(topic = %topic, "策略本地行情队列已满，丢弃 topic event");
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(topic = %topic, skipped, "策略 topic 行情订阅落后，跳过旧事件");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
    rx
}

#[derive(Debug, Clone)]
pub struct RewardPoolRemovalEvent {
    pub condition_id: String,
    pub token1: String,
    pub token2: String,
    pub reason: String,
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
    RewardPoolRemoval(RewardPoolRemovalEvent),
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

pub fn merge_topic_tokens(
    registrations: &[StrategyRegistration],
) -> HashMap<Arc<str>, Vec<String>> {
    let mut tokens: Vec<String> = registrations
        .iter()
        .flat_map(|registration| registration.related_tokens.iter().cloned())
        .collect();
    tokens.sort();
    tokens.dedup();

    tokens
        .into_iter()
        .map(|token| {
            let topic = Arc::<str>::from(token.as_str());
            (topic, vec![token])
        })
        .collect()
}

pub fn build_token_topics(
    topic_tokens: &HashMap<Arc<str>, Vec<String>>,
) -> HashMap<String, Arc<[Arc<str>]>> {
    topic_tokens
        .values()
        .flatten()
        .cloned()
        .map(|token| {
            let topic = Arc::<str>::from(token.as_str());
            (token, Arc::<[Arc<str>]>::from(vec![topic]))
        })
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

pub trait Strategy: Send + 'static {
    fn name(&self) -> &str;
    fn registration(&self) -> &StrategyRegistration;

    fn spawn(
        self,
        market_subscriptions: StrategyMarketSubscriptions,
        order_gateway: OrderGatewayHandle,
        position_read: crate::position_engine::PositionReadHandle,
    ) -> tokio::task::JoinHandle<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestIntent;

    #[derive(Debug)]
    struct TestState;

    fn test_book(timestamp_ms: u64) -> Arc<CleanOrderbook> {
        Arc::new(CleanOrderbook {
            best_bid_price: 40,
            best_bid_size: 100,
            best_ask_price: 60,
            best_ask_size: 100,
            timestamp_ms,
            bids: Arc::new(std::collections::BTreeMap::new()),
            asks: Arc::new(std::collections::BTreeMap::new()),
        })
    }

    fn test_registration(topics: Vec<Arc<str>>) -> StrategyRegistration {
        StrategyRegistration {
            name: Arc::from("test_strategy"),
            kind: StrategyKind::Monitoring,
            topics: Arc::<[Arc<str>]>::from(topics),
            topic_tokens: Arc::<[TopicRegistration]>::from(Vec::new()),
            related_tokens: Arc::<[String]>::from(Vec::new()),
        }
    }

    #[test]
    fn merge_topic_tokens_uses_each_related_token_as_its_own_topic() {
        let registration = StrategyRegistration {
            name: Arc::from("test"),
            kind: StrategyKind::Monitoring,
            topics: Arc::<[Arc<str>]>::from(vec![Arc::from("token-a"), Arc::from("token-b")]),
            topic_tokens: Arc::<[TopicRegistration]>::from(vec![TopicRegistration {
                topic: Arc::from("legacy-topic"),
                tokens: Arc::<[String]>::from(vec!["token-b".to_string(), "token-a".to_string()]),
            }]),
            related_tokens: Arc::<[String]>::from(vec![
                "token-b".to_string(),
                "token-a".to_string(),
            ]),
        };

        let routes = merge_topic_tokens(&[registration]);

        assert_eq!(routes.len(), 2);
        assert_eq!(routes.get("token-a").unwrap(), &vec!["token-a".to_string()]);
        assert_eq!(routes.get("token-b").unwrap(), &vec!["token-b".to_string()]);
    }

    #[test]
    fn build_token_topics_returns_identity_topics() {
        let topic_tokens = HashMap::from([
            (Arc::<str>::from("token-a"), vec!["token-a".to_string()]),
            (Arc::<str>::from("token-b"), vec!["token-b".to_string()]),
        ]);

        let token_topics = build_token_topics(&topic_tokens);

        assert_eq!(
            token_topics.get("token-a").unwrap().as_ref(),
            &[Arc::<str>::from("token-a")]
        );
        assert_eq!(
            token_topics.get("token-b").unwrap().as_ref(),
            &[Arc::<str>::from("token-b")]
        );
    }

    #[test]
    fn build_topic_broadcasts_creates_sender_per_token_topic() {
        let mut topic_tokens = HashMap::new();
        topic_tokens.insert(Arc::<str>::from("token-a"), vec!["token-a".to_string()]);
        topic_tokens.insert(Arc::<str>::from("token-b"), vec!["token-b".to_string()]);

        let topic_txs = build_topic_broadcasts(&topic_tokens, 8);

        assert_eq!(topic_txs.len(), 2);
        assert!(topic_txs.contains_key(&Arc::<str>::from("token-a")));
        assert!(topic_txs.contains_key(&Arc::<str>::from("token-b")));
    }

    #[test]
    fn subscribe_strategy_topics_subscribes_all_registered_token_topics() {
        let mut topic_tokens = HashMap::new();
        topic_tokens.insert(Arc::<str>::from("token-a"), vec!["token-a".to_string()]);
        topic_tokens.insert(Arc::<str>::from("token-b"), vec!["token-b".to_string()]);
        let topic_txs = build_topic_broadcasts(&topic_tokens, 8);
        let registration = test_registration(vec![Arc::from("token-a"), Arc::from("token-b")]);

        let subscriptions = subscribe_strategy_topics(&registration, &topic_txs)
            .expect("registered token topics should subscribe");

        assert_eq!(subscriptions.topics.len(), 2);
        assert_eq!(subscriptions.topics[0].0.as_ref(), "token-a");
        assert_eq!(subscriptions.topics[1].0.as_ref(), "token-b");
    }

    #[test]
    fn subscribe_strategy_topics_fails_for_missing_token_topic() {
        let topic_txs = HashMap::new();
        let registration = test_registration(vec![Arc::from("missing-token")]);

        let err = subscribe_strategy_topics(&registration, &topic_txs)
            .expect_err("missing token topic should fail fast");

        assert!(err.to_string().contains("missing-token"));
    }

    #[tokio::test]
    async fn strategy_market_subscription_mux_continues_after_lagged_receiver() {
        let (tx, rx) = tokio::sync::broadcast::channel(1);
        let subscriptions = StrategyMarketSubscriptions {
            topics: vec![(Arc::from("topic-a"), rx)],
        };
        tx.send(MarketEvent {
            topic: Arc::from("topic-a"),
            asset_id: Arc::from("token-a"),
            book: test_book(100),
        })
        .expect("first event should send");
        tx.send(MarketEvent {
            topic: Arc::from("topic-a"),
            asset_id: Arc::from("token-a"),
            book: test_book(200),
        })
        .expect("second event should send");

        let mut mux_rx = spawn_market_subscription_mux(subscriptions, 8);
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), mux_rx.recv())
            .await
            .expect("mux should continue after lag")
            .expect("mux should forward retained event");

        assert_eq!(event.topic.as_ref(), "topic-a");
        assert_eq!(event.book.timestamp_ms, 200);
    }

    #[test]
    fn allow_all_private_risk_allows_strategy_intent() {
        let risk = AllowAllPrivateRisk;

        let decision = risk.check_place(&TestIntent, &TestState, None);

        assert_eq!(decision, PrivateRiskDecision::Allow);
    }
}
