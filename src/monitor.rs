use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{TimeDelta, Utc};
use polymarket_client_sdk_v2::clob::types::request::UserRewardsEarningRequest;
use polymarket_client_sdk_v2::types::Decimal;
use tracing::{info, warn};

use crate::clob_client::build_authenticated_clob_client;
use crate::config::AuthConfig;
use crate::polymarket_rewards::{
    Market as RewardMarket, MarketParams, Order as RewardOrder, Side as RewardSide, estimate_reward,
};
use crate::storage::MarketStore;
use crate::strategy::{OrderCorrelationMap, QuoteSide};

const PRICE_SCALE: f64 = 10_000.0;
const SIZE_SCALE: f64 = 10_000.0;
const LOG_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct FullBookSnapshot {
    pub asset_id: Arc<str>,
    pub bids: Arc<BTreeMap<u16, u32>>,
    pub asks: Arc<BTreeMap<u16, u32>>,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone)]
pub struct RewardMonitorConfig {
    pub min_orders: u32,
    pub max_spread_cents: f64,
    pub min_size: f64,
    pub daily_reward_pool: f64,
    pub simulation: bool,
    /// 配对的另一个 token（YES↔NO），用于合并计算奖励
    pub paired_token: Option<String>,
    /// true = 这是 YES token（token1），false = NO token（token2）
    pub is_yes_token: bool,
}

pub async fn run_reward_estimator(
    mut rx: tokio::sync::mpsc::Receiver<FullBookSnapshot>,
    configs: HashMap<String, RewardMonitorConfig>,
    correlations: OrderCorrelationMap,
    market_store: MarketStore,
) {
    // 速率限制键：YES token ID
    let mut last_log: HashMap<String, Instant> = HashMap::new();
    // 每个 token 的最新订单薄快照缓存
    let mut book_cache: HashMap<String, FullBookSnapshot> = HashMap::new();

    while let Some(snapshot) = rx.recv().await {
        let token = snapshot.asset_id.as_ref().to_string();
        let Some(config) = configs.get(&token) else {
            book_cache.insert(token, snapshot);
            continue;
        };

        book_cache.insert(token.clone(), snapshot);

        // 统一以 YES token 作为速率限制 key，避免同一对市场重复计算
        let yes_token = if config.is_yes_token {
            token.clone()
        } else {
            config.paired_token.clone().unwrap_or_else(|| token.clone())
        };

        let now = Instant::now();
        let due = last_log
            .get(&yes_token)
            .map_or(true, |t| now.duration_since(*t) >= LOG_INTERVAL);
        if !due {
            continue;
        }

        let yes_cfg = if config.is_yes_token {
            config
        } else {
            match configs.get(&yes_token) {
                Some(c) => c,
                None => config,
            }
        };

        let no_token = if config.is_yes_token {
            config.paired_token.as_deref()
        } else {
            Some(token.as_str())
        };

        let Some(yes_snap) = book_cache.get(&yes_token) else {
            continue;
        };
        let Some(&bid_scaled) = yes_snap.bids.keys().next_back() else {
            continue;
        };
        let Some(&ask_scaled) = yes_snap.asks.keys().next() else {
            continue;
        };
        let mid = (bid_scaled as f64 + ask_scaled as f64) / 2.0 / PRICE_SCALE;

        // 收集我的订单：YES token 标记 Market::Yes，NO token 标记 Market::No
        let mut my_orders: Vec<RewardOrder> = Vec::new();
        collect_my_orders(&correlations, &yes_token, RewardMarket::Yes, &mut my_orders);
        if let Some(no_tok) = no_token {
            collect_my_orders(&correlations, no_tok, RewardMarket::No, &mut my_orders);
        }

        let no_snap = no_token.and_then(|t| book_cache.get(t));

        let mut competitor_orders: Vec<RewardOrder> = Vec::new();
        if yes_cfg.simulation {
            // 模拟模式：竞争者 = 两个 token 的全量订单薄
            add_book_orders(yes_snap, RewardMarket::Yes, &mut competitor_orders);
            if let Some(snap) = no_snap {
                add_book_orders(snap, RewardMarket::No, &mut competitor_orders);
            }
        } else {
            // 真实模式：从订单薄中减去我的单子
            let (my_yes_bids, my_yes_asks) = my_side_map(&my_orders, RewardMarket::Yes);
            add_book_minus_mine(
                yes_snap,
                RewardMarket::Yes,
                &my_yes_bids,
                &my_yes_asks,
                &mut competitor_orders,
            );
            if let Some(snap) = no_snap {
                let (my_no_bids, my_no_asks) = my_side_map(&my_orders, RewardMarket::No);
                add_book_minus_mine(
                    snap,
                    RewardMarket::No,
                    &my_no_bids,
                    &my_no_asks,
                    &mut competitor_orders,
                );
            }
        }

        let params = MarketParams {
            mid_price: mid,
            max_spread_cents: yes_cfg.max_spread_cents,
            scale_factor: 3.0,
            reward_pool: yes_cfg.daily_reward_pool,
        };
        let est = estimate_reward(&my_orders, &competitor_orders, &params);

        info!(
            target: "order",
            token = %yes_token,
            mid = format!("{:.4}", mid),
            my_orders = my_orders.len(),
            my_qone = format!("{:.4}", est.my_qone),
            my_qtwo = format!("{:.4}", est.my_qtwo),
            my_qmin = format!("{:.4}", est.my_qmin),
            competitors_qmin = format!("{:.4}", est.competitors_qmin),
            my_share_pct = format!("{:.2}%", est.share * 100.0),
            estimated_daily_reward = format!("{:.4}", est.estimated_reward),
            below_min_payout = est.estimated_reward > 0.0 && est.estimated_reward < 1.0,
            simulation = yes_cfg.simulation,
            "liquidity_reward 流动性奖励实时估算"
        );

        if let Err(error) = market_store.insert_liquidity_reward_score(
            &yes_token,
            mid,
            my_orders.len(),
            est.my_qone,
            est.my_qtwo,
            est.my_qmin,
            est.competitors_qmin,
            est.share,
            est.estimated_reward,
            yes_cfg.simulation,
        ) {
            warn!(target: "order", token = %yes_token, error = %error, "liquidity_reward 写入分数记录失败");
        }

        last_log.insert(yes_token, now);
    }
}

fn collect_my_orders(
    correlations: &OrderCorrelationMap,
    token: &str,
    market: RewardMarket,
    out: &mut Vec<RewardOrder>,
) {
    for entry in correlations
        .iter()
        .filter(|e| e.value().token == token && e.value().strategy.as_ref() == "liquidity_reward")
    {
        let m = entry.value();
        let price: f64 = m.price.to_string().parse().unwrap_or(0.0);
        let size: f64 = m.order_size.to_string().parse().unwrap_or(0.0);
        out.push(RewardOrder {
            side: match m.side {
                QuoteSide::Buy => RewardSide::Buy,
                QuoteSide::Sell => RewardSide::Sell,
            },
            market: market.clone(),
            price,
            size,
        });
    }
}

fn add_book_orders(snap: &FullBookSnapshot, market: RewardMarket, out: &mut Vec<RewardOrder>) {
    for (&p, &s) in snap.bids.iter() {
        let size = s as f64 / SIZE_SCALE;
        if size > 0.0 {
            out.push(RewardOrder {
                side: RewardSide::Buy,
                market: market.clone(),
                price: p as f64 / PRICE_SCALE,
                size,
            });
        }
    }
    for (&p, &s) in snap.asks.iter() {
        let size = s as f64 / SIZE_SCALE;
        if size > 0.0 {
            out.push(RewardOrder {
                side: RewardSide::Sell,
                market: market.clone(),
                price: p as f64 / PRICE_SCALE,
                size,
            });
        }
    }
}

fn my_side_map(
    my_orders: &[RewardOrder],
    market: RewardMarket,
) -> (HashMap<u16, f64>, HashMap<u16, f64>) {
    let mut bids: HashMap<u16, f64> = HashMap::new();
    let mut asks: HashMap<u16, f64> = HashMap::new();
    for order in my_orders.iter().filter(|o| o.market == market) {
        let p = (order.price * PRICE_SCALE).round() as u16;
        match order.side {
            RewardSide::Buy => *bids.entry(p).or_default() += order.size,
            RewardSide::Sell => *asks.entry(p).or_default() += order.size,
        }
    }
    (bids, asks)
}

fn add_book_minus_mine(
    snap: &FullBookSnapshot,
    market: RewardMarket,
    my_bids: &HashMap<u16, f64>,
    my_asks: &HashMap<u16, f64>,
    out: &mut Vec<RewardOrder>,
) {
    for (&p, &s) in snap.bids.iter() {
        let comp = (s as f64 / SIZE_SCALE - my_bids.get(&p).copied().unwrap_or(0.0)).max(0.0);
        if comp > 0.0 {
            out.push(RewardOrder {
                side: RewardSide::Buy,
                market: market.clone(),
                price: p as f64 / PRICE_SCALE,
                size: comp,
            });
        }
    }
    for (&p, &s) in snap.asks.iter() {
        let comp = (s as f64 / SIZE_SCALE - my_asks.get(&p).copied().unwrap_or(0.0)).max(0.0);
        if comp > 0.0 {
            out.push(RewardOrder {
                side: RewardSide::Sell,
                market: market.clone(),
                price: p as f64 / PRICE_SCALE,
                size: comp,
            });
        }
    }
}

pub async fn run_liquidity_reward_monitor(auth: AuthConfig, interval_secs: u64) {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));

    loop {
        ticker.tick().await;
        if let Err(error) = poll_user_rewards(&auth).await {
            warn!(target: "order", error = %error, "user_reward_monitor 拉取当前用户奖励信息失败");
        }
    }
}

async fn poll_user_rewards(auth: &AuthConfig) -> anyhow::Result<()> {
    let client = build_authenticated_clob_client(auth).await?;
    let date = Utc::now().date_naive() - TimeDelta::days(30);
    let request = UserRewardsEarningRequest::builder().date(date).build();
    let rewards = client
        .user_earnings_and_markets_config(&request, None)
        .await?;
    let reward_percentages = client.reward_percentages().await?;

    let total_earnings = rewards
        .data
        .iter()
        .flat_map(|reward| reward.earnings.iter())
        .fold(Decimal::ZERO, |acc, earning| acc + earning.earnings);

    info!(
        target = "order",
        since = %date,
        market_count = rewards.data.len(),
        reward_percentage_count = reward_percentages.len(),
        total_earnings = %total_earnings,
        "user_reward_monitor 当前用户奖励汇总"
    );

    for reward in rewards.data {
        let market_earnings = reward
            .earnings
            .iter()
            .fold(Decimal::ZERO, |acc, earning| acc + earning.earnings);
        info!(
            target = "order",
            condition_id = %reward.condition_id,
            question = %reward.question,
            market_slug = %reward.market_slug,
            event_slug = %reward.event_slug,
            maker_address = %reward.maker_address,
            earning_percentage = %reward.earning_percentage,
            market_earnings = %market_earnings,
            rewards_max_spread = %reward.rewards_max_spread,
            rewards_min_size = %reward.rewards_min_size,
            market_competitiveness = %reward.market_competitiveness,
            reward_configs = reward.rewards_config.len(),
            "user_reward_monitor 当前用户奖励市场"
        );
    }

    Ok(())
}
