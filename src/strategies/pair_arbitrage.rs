use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use chrono::Local;
use dashmap::DashMap;
use polymarket_client_sdk_v2::types::Decimal;
use tracing::info;

use crate::strategy::{
    CleanOrderbook, Filters, PairEntry, Strategy, StrategyKind, StrategyMarketSubscriptions,
    StrategyRegistration, TopicRegistration, spawn_market_subscription_mux,
};

const PRICE_SCALE: u32 = 10_000;

#[derive(Debug, Clone)]
struct TokenPrice {
    asset_id: String,
    best_bid_price: Option<u16>,
    best_bid_size: Option<u32>,
    best_ask_price: Option<u16>,
    best_ask_size: Option<u32>,
    updated_at_ms: u64,
}

impl TokenPrice {
    fn new(asset_id: String) -> Self {
        Self {
            asset_id,
            best_bid_price: None,
            best_bid_size: None,
            best_ask_price: None,
            best_ask_size: None,
            updated_at_ms: 0,
        }
    }
}

#[derive(Clone, Default)]
struct PriceStore {
    inner: Arc<DashMap<String, TokenPrice>>,
}

impl PriceStore {
    fn new() -> Self {
        Self::default()
    }

    fn register(&self, asset_ids: &[String]) {
        for id in asset_ids {
            self.inner
                .entry(id.clone())
                .or_insert_with(|| TokenPrice::new(id.clone()));
        }
    }

    fn apply(&self, asset_id: &str, book: CleanOrderbook) -> Vec<String> {
        let mut entry = self
            .inner
            .entry(asset_id.to_string())
            .or_insert_with(|| TokenPrice::new(asset_id.to_string()));

        if book.timestamp_ms >= entry.updated_at_ms {
            entry.best_bid_price = Some(book.best_bid_price);
            entry.best_bid_size = Some(book.best_bid_size);
            entry.best_ask_price = Some(book.best_ask_price);
            entry.best_ask_size = Some(book.best_ask_size);
            entry.updated_at_ms = book.timestamp_ms;
            return vec![asset_id.to_string()];
        }

        vec![]
    }

    fn get(&self, asset_id: &str) -> Option<TokenPrice> {
        self.inner.get(asset_id).map(|r| r.clone())
    }
}

pub struct PairArbitrageStrategy {
    filters: Arc<Filters>,
    pairs_by_token: Arc<HashMap<Arc<str>, Arc<[PairEntry]>>>,
    registration: Arc<StrategyRegistration>,
}

impl PairArbitrageStrategy {
    pub fn from_config(filters: Arc<Filters>, assets_file: &str) -> anyhow::Result<Self> {
        let assets_path = Path::new(assets_file);
        let csv_path = if assets_path.is_absolute() {
            assets_file.to_string()
        } else if let Ok(mut exe_path) = std::env::current_exe() {
            exe_path.pop();
            exe_path.push(assets_file);
            exe_path.to_string_lossy().to_string()
        } else {
            assets_file.to_string()
        };

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(&csv_path)
            .map_err(|e| anyhow::anyhow!("无法打开 {}: {}", csv_path, e))?;

        let mut pair_entries: Vec<PairEntry> = Vec::new();
        for result in reader.records() {
            let record = result?;
            if record.len() < 2 {
                continue;
            }
            let topic: Arc<str> = if record.len() >= 3 {
                Arc::from(record[2].trim())
            } else {
                Arc::from("default")
            };
            pair_entries.push(PairEntry {
                tokens: [record[0].trim().to_string(), record[1].trim().to_string()],
                topic,
            });
        }

        if pair_entries.is_empty() {
            anyhow::bail!("资产文件中没有有效的 token 配对: {}", csv_path);
        }

        let mut token_pairs: HashMap<Arc<str>, Vec<PairEntry>> = HashMap::new();
        let mut asset_ids = HashSet::new();

        for entry in pair_entries {
            for token in &entry.tokens {
                asset_ids.insert(token.clone());
                token_pairs
                    .entry(Arc::<str>::from(token.as_str()))
                    .or_default()
                    .push(entry.clone());
            }
        }

        let mut asset_ids: Vec<String> = asset_ids.into_iter().collect();
        asset_ids.sort();

        let subscriptions: Vec<Arc<str>> = asset_ids
            .iter()
            .map(|token| Arc::<str>::from(token.as_str()))
            .collect();

        let topic_token_regs: Vec<TopicRegistration> = asset_ids
            .iter()
            .map(|token| TopicRegistration {
                topic: Arc::<str>::from(token.as_str()),
                tokens: Arc::<[String]>::from(vec![token.clone()]),
            })
            .collect();

        let pairs_by_token: Arc<HashMap<Arc<str>, Arc<[PairEntry]>>> = Arc::new(
            token_pairs
                .into_iter()
                .map(|(token, pairs)| (token, Arc::<[PairEntry]>::from(pairs)))
                .collect(),
        );

        let registration = Arc::new(StrategyRegistration {
            name: Arc::from("pair_arbitrage"),
            kind: StrategyKind::PairArbitrage,
            topics: Arc::<[Arc<str>]>::from(subscriptions),
            topic_tokens: Arc::<[TopicRegistration]>::from(topic_token_regs),
            related_tokens: Arc::<[String]>::from(asset_ids),
        });

        Ok(Self {
            filters,
            pairs_by_token,
            registration,
        })
    }
}

impl Strategy for PairArbitrageStrategy {
    fn name(&self) -> &str {
        "pair_arbitrage"
    }

    fn registration(&self) -> &StrategyRegistration {
        self.registration.as_ref()
    }

    fn spawn(
        self,
        market_subscriptions: StrategyMarketSubscriptions,
        order_gateway: crate::order_gateway::OrderGatewayHandle,
        _position_read: crate::position_engine::PositionReadHandle,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let store = PriceStore::new();
            store.register(self.registration.related_tokens.as_ref());
            let mut rx = spawn_market_subscription_mux(market_subscriptions, 256);

            while let Some(event) = rx.recv().await {
                let updated = store.apply(event.asset_id.as_ref(), event.book.as_ref().clone());
                if updated.is_empty() {
                    continue;
                }

                check_pairs(
                    &store,
                    &self.pairs_by_token,
                    &self.filters,
                    &event.asset_id,
                    &updated,
                    &order_gateway,
                );
            }
        })
    }
}

fn price_to_decimal(price: u16) -> Decimal {
    Decimal::from(price) / Decimal::from(PRICE_SCALE)
}

fn check_pairs(
    store: &PriceStore,
    pairs_by_token: &HashMap<Arc<str>, Arc<[PairEntry]>>,
    filters: &Filters,
    token: &Arc<str>,
    updated_assets: &[String],
    _order_gateway: &crate::order_gateway::OrderGatewayHandle,
) {
    let Some(pairs) = pairs_by_token.get(token) else {
        return;
    };

    let updated_set: HashSet<&str> = updated_assets.iter().map(|s| s.as_str()).collect();

    for pair in pairs.iter() {
        if !updated_set.contains(pair.tokens[0].as_str())
            && !updated_set.contains(pair.tokens[1].as_str())
        {
            continue;
        }

        let (Some(p0), Some(p1)) = (store.get(&pair.tokens[0]), store.get(&pair.tokens[1])) else {
            continue;
        };
        let (Some(bid0), Some(ask0), Some(bid1), Some(ask1)) = (
            p0.best_bid_price,
            p0.best_ask_price,
            p1.best_bid_price,
            p1.best_ask_price,
        ) else {
            continue;
        };

        let bid0 = price_to_decimal(bid0);
        let ask0 = price_to_decimal(ask0);
        let bid1 = price_to_decimal(bid1);
        let ask1 = price_to_decimal(ask1);

        if ask0 - bid0 > filters.max_spread || ask1 - bid1 > filters.max_spread {
            continue;
        }
        if bid0 < filters.min_price
            || bid0 > filters.max_price
            || ask0 < filters.min_price
            || ask0 > filters.max_price
            || bid1 < filters.min_price
            || bid1 > filters.max_price
            || ask1 < filters.min_price
            || ask1 > filters.max_price
        {
            continue;
        }

        let gap = Decimal::ONE - (ask0 + ask1);
        if gap <= filters.min_diff {
            continue;
        }

        let event_ts = p0.updated_at_ms.max(p1.updated_at_ms);
        let line = format!(
            "[ALERT] {} | topic={} | event_ts={} | 1-(ask0+ask1)={:.4}\n  token0={} bid={} ask={} ts={}\n  token1={} bid={} ask={} ts={}",
            Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            pair.topic,
            event_ts,
            gap,
            &p0.asset_id[..12],
            bid0,
            ask0,
            p0.updated_at_ms,
            &p1.asset_id[..12],
            bid1,
            ask1,
            p1.updated_at_ms,
        );
        info!(target: "alerts", "{}", line.trim_end());
    }
}

#[cfg(test)]
mod gateway_migration_tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;
    use crate::strategy::{CleanOrderbook, PairEntry, StrategyKind};

    fn filters() -> Filters {
        Filters {
            min_diff: Decimal::try_from(0.01_f64).expect("decimal should build"),
            max_spread: Decimal::try_from(0.10_f64).expect("decimal should build"),
            min_price: Decimal::try_from(0.01_f64).expect("decimal should build"),
            max_price: Decimal::try_from(0.99_f64).expect("decimal should build"),
        }
    }

    fn clean_book(best_bid_price: u16, best_ask_price: u16) -> CleanOrderbook {
        CleanOrderbook {
            best_bid_price,
            best_bid_size: 100,
            best_ask_price,
            best_ask_size: 100,
            timestamp_ms: 100,
            bids: Arc::new(BTreeMap::new()),
            asks: Arc::new(BTreeMap::new()),
        }
    }

    #[test]
    fn pair_arbitrage_keeps_alert_only_without_gateway_order() {
        let store = PriceStore::new();
        store.register(&["token-000000".to_string(), "token-111111".to_string()]);
        store.apply("token-000000", clean_book(3_900, 4_000));
        store.apply("token-111111", clean_book(4_900, 5_000));

        let token = Arc::<str>::from("token-000000");
        let pair_topic = Arc::<str>::from("topic");
        let pairs = HashMap::from([(
            token.clone(),
            Arc::<[PairEntry]>::from(vec![PairEntry {
                tokens: ["token-000000".to_string(), "token-111111".to_string()],
                topic: pair_topic.clone(),
            }]),
        )]);
        let (gateway, mut gateway_rx) = crate::order_gateway::OrderGatewayHandle::new_for_test(
            8,
            crate::order_gateway::GatewayPhase::Live,
        );

        let csv_path = std::env::temp_dir().join(format!(
            "pair_arbitrage_kind_{}_{}.csv",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time should move forward")
                .as_nanos()
        ));
        std::fs::write(
            &csv_path,
            "token1,token2,topic\ntoken-000000,token-111111,topic\n",
        )
        .expect("csv should write");
        let strategy = PairArbitrageStrategy::from_config(
            Arc::new(filters()),
            csv_path.to_str().expect("path should be utf-8"),
        )
        .expect("strategy should build");
        std::fs::remove_file(&csv_path).expect("csv should be removed");

        check_pairs(
            &store,
            &pairs,
            &filters(),
            &token,
            &["token-000000".to_string()],
            &gateway,
        );

        assert_eq!(strategy.registration().kind, StrategyKind::PairArbitrage);
        assert_eq!(
            strategy.registration().topics.as_ref(),
            &[
                Arc::<str>::from("token-000000"),
                Arc::<str>::from("token-111111"),
            ]
        );
        assert!(gateway_rx.try_recv().is_err());
    }
}
