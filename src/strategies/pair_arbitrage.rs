use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use chrono::Local;
use polymarket_client_sdk::types::Decimal;
use tracing::{info, warn};

const PRICE_SCALE: u32 = 10_000;

use crate::price_store::PriceStore;
use crate::strategy::{
    Filters, OrderSignal, PairEntry, Strategy, StrategyEvent, StrategyRegistration,
    TopicRegistration,
};

pub struct PairArbitrageStrategy {
    filters: Arc<Filters>,
    pairs_by_topic: Arc<HashMap<Arc<str>, Arc<[PairEntry]>>>,
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
            .has_headers(false)
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

        let mut topic_pairs: HashMap<Arc<str>, Vec<PairEntry>> = HashMap::new();
        let mut topic_tokens: HashMap<Arc<str>, Vec<String>> = HashMap::new();
        let mut asset_ids = HashSet::new();

        for entry in pair_entries {
            topic_tokens
                .entry(entry.topic.clone())
                .or_default()
                .extend(entry.tokens.iter().cloned());
            for token in &entry.tokens {
                asset_ids.insert(token.clone());
            }
            topic_pairs
                .entry(entry.topic.clone())
                .or_default()
                .push(entry);
        }

        let mut subscriptions: Vec<Arc<str>> = topic_tokens.keys().cloned().collect();
        subscriptions.sort();

        let topic_token_regs: Vec<TopicRegistration> = topic_tokens
            .into_iter()
            .map(|(topic, mut tokens)| {
                tokens.sort();
                tokens.dedup();
                TopicRegistration {
                    topic,
                    tokens: Arc::<[String]>::from(tokens),
                }
            })
            .collect();

        let pairs_by_topic: Arc<HashMap<Arc<str>, Arc<[PairEntry]>>> = Arc::new(
            topic_pairs
                .into_iter()
                .map(|(topic, pairs)| (topic, Arc::<[PairEntry]>::from(pairs)))
                .collect(),
        );

        let mut asset_ids: Vec<String> = asset_ids.into_iter().collect();
        asset_ids.sort();

        let registration = Arc::new(StrategyRegistration {
            name: Arc::from("pair_arbitrage"),
            topics: Arc::<[Arc<str>]>::from(subscriptions),
            topic_tokens: Arc::<[TopicRegistration]>::from(topic_token_regs),
            related_tokens: Arc::<[String]>::from(asset_ids),
        });

        Ok(Self {
            filters,
            pairs_by_topic,
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
        mut rx: tokio::sync::mpsc::Receiver<StrategyEvent>,
        order_tx: tokio::sync::mpsc::Sender<OrderSignal>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let store = PriceStore::new();
            store.register(self.registration.related_tokens.as_ref());

            while let Some(event) = rx.recv().await {
                match event {
                    StrategyEvent::Market(event) => {
                        let updated = store.apply(event.asset_id.as_ref(), event.book);
                        if updated.is_empty() {
                            continue;
                        }

                        check_pairs(
                            &store,
                            &self.pairs_by_topic,
                            &self.filters,
                            &event.topic,
                            &updated,
                            &order_tx,
                        );
                    }
                    StrategyEvent::Positions(update) => {
                        let Some(_update) = self.relevant_positions(&update) else {
                            continue;
                        };
                    }
                }
            }
        })
    }
}

fn price_to_decimal(price: u16) -> Decimal {
    Decimal::from(price) / Decimal::from(PRICE_SCALE)
}

fn check_pairs(
    store: &PriceStore,
    pairs_by_topic: &HashMap<Arc<str>, Arc<[PairEntry]>>,
    filters: &Filters,
    topic: &Arc<str>,
    updated_assets: &[String],
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
) {
    let Some(pairs) = pairs_by_topic.get(topic) else {
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
        if bid0 < filters.min_price || bid0 > filters.max_price
            || ask0 < filters.min_price || ask0 > filters.max_price
            || bid1 < filters.min_price || bid1 > filters.max_price
            || ask1 < filters.min_price || ask1 > filters.max_price
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
            topic,
            event_ts,
            gap,
            &p0.asset_id[..12], bid0, ask0, p0.updated_at_ms,
            &p1.asset_id[..12], bid1, ask1, p1.updated_at_ms,
        );
        info!(target: "alerts", "{}", line.trim_end());

        if let Err(err) = order_tx.try_send(OrderSignal::PairArbitrage {
            token0: p0.asset_id.clone(),
            token1: p1.asset_id.clone(),
            ask0,
            ask1,
            gap,
        }) {
            warn!(topic = %topic, error = %err, "pair_arbitrage 发送下单信号失败");
        }
    }
}
