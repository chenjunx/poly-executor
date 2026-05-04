use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{info, warn};

use crate::market::MarketBookCache;
use crate::proxy_ws;
use crate::storage::{ActiveRewardMarketPoolEntry, MarketStore};
use crate::strategy::CleanOrderbook;

const PRICE_SCALE: f64 = 10_000.0;

pub struct RewardMarketPoolMonitorConfig {
    pub refresh_interval: Duration,
    pub token1_spread_threshold: f64,
    pub token1_min_bid: f64,
    pub token1_max_bid: f64,
    pub max_tokens_per_connection: usize,
}

#[derive(Debug, Clone)]
struct RewardMarketPoolPair {
    condition_id: String,
    market_slug: Option<String>,
    question: Option<String>,
    token1: String,
    token2: String,
}

pub async fn run_reward_market_pool_monitor(
    store: MarketStore,
    proxy: Option<proxy_ws::Proxy>,
    config: RewardMarketPoolMonitorConfig,
) {
    let mut subscribed_tokens = BTreeSet::new();
    let mut ws_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::channel(4096);
    let mut book_cache = MarketBookCache::new();
    let mut pairs_by_condition: HashMap<String, RewardMarketPoolPair> = HashMap::new();
    let mut conditions_by_token: HashMap<String, Vec<String>> = HashMap::new();
    let mut ticker = tokio::time::interval(config.refresh_interval.max(Duration::from_secs(60)));
    ticker.tick().await;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let entries = match store.load_active_reward_market_pool_entries() {
                    Ok(entries) => entries,
                    Err(error) => {
                        warn!(target: "order", error = %error, "reward_market_pool_monitor 读取奖励市场池失败");
                        continue;
                    }
                };
                let pairs = pool_pairs_from_active_entries(entries);

                pairs_by_condition = pairs
                    .into_iter()
                    .map(|pair| (pair.condition_id.clone(), pair))
                    .collect();
                conditions_by_token = build_conditions_by_token(&pairs_by_condition);
                let next_tokens = pairs_by_condition
                    .values()
                    .flat_map(|pair| [pair.token1.clone(), pair.token2.clone()])
                    .collect::<BTreeSet<_>>();

                if next_tokens != subscribed_tokens {
                    for task in ws_tasks.drain(..) {
                        task.abort();
                    }
                    ws_tasks = spawn_reward_pool_subscriptions(
                        next_tokens.iter().cloned().collect(),
                        config.max_tokens_per_connection.max(1),
                        proxy.clone(),
                        ws_tx.clone(),
                    );
                    info!(
                        target: "order",
                        market_count = pairs_by_condition.len(),
                        token_count = next_tokens.len(),
                        connection_count = ws_tasks.len(),
                        "reward_market_pool_monitor 奖励市场池监听已更新"
                    );
                    subscribed_tokens = next_tokens;
                }
            }

            msg = ws_rx.recv() => {
                let Some(msg) = msg else { break };
                for (asset_id, book) in book_cache.apply(&msg) {
                    let Some(condition_ids) = conditions_by_token.get(asset_id.as_ref()) else {
                        continue;
                    };
                    for condition_id in condition_ids {
                        let Some(pair) = pairs_by_condition.get(condition_id) else {
                            continue;
                        };
                        if pair.token1 != asset_id.as_ref() {
                            continue;
                        }
                        if let Err(error) = evaluate_token1_book(&store, pair, &book, &config) {
                            warn!(target: "order", condition_id = %pair.condition_id, error = %error, "reward_market_pool_monitor 检查奖励市场池行情失败");
                        }
                    }
                }
            }
        }
    }
}

fn pool_pairs_from_active_entries(
    entries: Vec<ActiveRewardMarketPoolEntry>,
) -> Vec<RewardMarketPoolPair> {
    entries
        .into_iter()
        .map(|entry| RewardMarketPoolPair {
            condition_id: normalize_condition_id(&entry.condition_id),
            market_slug: entry.market_slug,
            question: entry.question,
            token1: entry.token1,
            token2: entry.token2,
        })
        .collect()
}

fn build_conditions_by_token(
    pairs_by_condition: &HashMap<String, RewardMarketPoolPair>,
) -> HashMap<String, Vec<String>> {
    let mut result: HashMap<String, Vec<String>> = HashMap::new();
    for pair in pairs_by_condition.values() {
        result
            .entry(pair.token1.clone())
            .or_default()
            .push(pair.condition_id.clone());
        result
            .entry(pair.token2.clone())
            .or_default()
            .push(pair.condition_id.clone());
    }
    result
}

fn spawn_reward_pool_subscriptions(
    tokens: Vec<String>,
    max_tokens_per_connection: usize,
    proxy: Option<proxy_ws::Proxy>,
    tx: tokio::sync::mpsc::Sender<polymarket_client_sdk_v2::clob::ws::types::response::WsMessage>,
) -> Vec<tokio::task::JoinHandle<()>> {
    tokens
        .chunks(max_tokens_per_connection)
        .map(|chunk| {
            let tokens = chunk.to_vec();
            let proxy = proxy.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                loop {
                    if let Err(error) = proxy_ws::run(proxy.clone(), tokens.clone(), tx.clone()).await {
                        warn!(target: "order", error = %error, token_count = tokens.len(), "reward_market_pool_monitor WS 连接断开，5 秒后重连");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            })
        })
        .collect()
}

fn evaluate_token1_book(
    store: &MarketStore,
    pair: &RewardMarketPoolPair,
    book: &CleanOrderbook,
    config: &RewardMarketPoolMonitorConfig,
) -> anyhow::Result<()> {
    let bid = scaled_price_to_f64(book.best_bid_price);
    let ask = scaled_price_to_f64(book.best_ask_price);
    let spread = token1_spread(book);
    let now = now_ms()?;

    store.update_reward_market_pool_token1_check(&pair.condition_id, bid, ask, spread, now)?;

    if spread > config.token1_spread_threshold {
        let reason = format!(
            "token1_spread_gt_threshold: spread={spread} threshold={}",
            config.token1_spread_threshold
        );
        if store.kick_reward_market_pool_entry(&pair.condition_id, &reason, now)? {
            info!(target: "order", condition_id = %pair.condition_id, token1 = %pair.token1, spread, "reward_market_pool_monitor 奖励市场踢出");
        }
        return Ok(());
    }

    if bid < config.token1_min_bid || bid > config.token1_max_bid {
        let reason = format!(
            "token1_bid_out_of_range: bid={bid} min={} max={}",
            config.token1_min_bid, config.token1_max_bid
        );
        if store.kick_reward_market_pool_entry(&pair.condition_id, &reason, now)? {
            info!(target: "order", condition_id = %pair.condition_id, token1 = %pair.token1, bid, "reward_market_pool_monitor 奖励市场踢出");
        }
    }

    Ok(())
}

fn scaled_price_to_f64(price: u16) -> f64 {
    price as f64 / PRICE_SCALE
}

fn token1_spread(book: &CleanOrderbook) -> f64 {
    scaled_price_to_f64(book.best_ask_price) - scaled_price_to_f64(book.best_bid_price)
}

fn normalize_condition_id(condition_id: &str) -> String {
    if condition_id.starts_with("0x") {
        condition_id.to_string()
    } else {
        format!("0x{condition_id}")
    }
}

fn now_ms() -> anyhow::Result<u64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(bid: u16, ask: u16) -> CleanOrderbook {
        CleanOrderbook {
            best_bid_price: bid,
            best_bid_size: 100,
            best_ask_price: ask,
            best_ask_size: 100,
            timestamp_ms: 1,
            bids: Arc::new(BTreeMap::new()),
            asks: Arc::new(BTreeMap::new()),
        }
    }

    #[test]
    fn computes_token1_spread_from_scaled_prices() {
        let book = book(4500, 5600);
        assert_eq!(scaled_price_to_f64(book.best_bid_price), 0.45);
        assert_eq!(scaled_price_to_f64(book.best_ask_price), 0.56);
        assert!((token1_spread(&book) - 0.11).abs() < f64::EPSILON);
    }

    #[test]
    fn builds_pool_pairs_from_active_entries() {
        let entries = vec![ActiveRewardMarketPoolEntry {
            condition_id: "abc".to_string(),
            market_slug: Some("slug".to_string()),
            question: Some("question".to_string()),
            token1: "token1".to_string(),
            token2: "token2".to_string(),
            tokens_json: "[]".to_string(),
            market_competitiveness: Some("1.23".to_string()),
            rewards_min_size: Some("100".to_string()),
            rewards_max_spread: Some("4".to_string()),
            market_daily_reward: Some("50".to_string()),
            build_date_utc: Some("2026-05-04".to_string()),
            pool_version: Some(123),
            liquidity_reward_selected: false,
            liquidity_reward_selected_at_ms: None,
            liquidity_reward_select_reason: None,
            liquidity_reward_select_rank: None,
        }];

        let pairs = pool_pairs_from_active_entries(entries);
        let pairs_by_condition = pairs
            .into_iter()
            .map(|pair| (pair.condition_id.clone(), pair))
            .collect::<HashMap<_, _>>();
        let conditions_by_token = build_conditions_by_token(&pairs_by_condition);

        assert!(pairs_by_condition.contains_key("0xabc"));
        assert_eq!(
            conditions_by_token.get("token1").unwrap(),
            &vec!["0xabc".to_string()]
        );
        assert_eq!(
            conditions_by_token.get("token2").unwrap(),
            &vec!["0xabc".to_string()]
        );
    }
}
