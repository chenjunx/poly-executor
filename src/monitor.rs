use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{TimeDelta, Utc};
use polymarket_client_sdk::clob::types::request::UserRewardsEarningRequest;
use polymarket_client_sdk::types::Decimal;
use tracing::{info, warn};

use crate::clob_client::build_authenticated_clob_client;
use crate::config::AuthConfig;
use crate::polymarket_rewards::{
    Market as RewardMarket, MarketParams, Order as RewardOrder, Side as RewardSide, estimate_reward,
};
use crate::strategy::{OrderCorrelationMap, QuoteSide};

const PRICE_SCALE: f64 = 10_000.0;
const SIZE_SCALE: f64 = 10_000.0;
const LOG_INTERVAL: Duration = Duration::from_secs(60);

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
}

pub async fn run_reward_estimator(
    mut rx: tokio::sync::mpsc::Receiver<FullBookSnapshot>,
    configs: HashMap<String, RewardMonitorConfig>,
    correlations: OrderCorrelationMap,
) {
    let mut last_log: HashMap<String, Instant> = HashMap::new();

    while let Some(snapshot) = rx.recv().await {
        let token = snapshot.asset_id.as_ref();
        let Some(config) = configs.get(token) else {
            continue;
        };

        let now = Instant::now();
        let due = last_log
            .get(token)
            .map_or(true, |t| now.duration_since(*t) >= LOG_INTERVAL);
        if !due {
            continue;
        }

        let Some(&bid_scaled) = snapshot.bids.keys().next_back() else {
            continue;
        };
        let Some(&ask_scaled) = snapshot.asks.keys().next() else {
            continue;
        };
        let mid = (bid_scaled as f64 + ask_scaled as f64) / 2.0 / PRICE_SCALE;

        let my_orders: Vec<RewardOrder> = correlations
            .iter()
            .filter(|e| {
                let m = e.value();
                m.token == token && m.strategy.as_ref() == "mid_requote"
            })
            .map(|e| {
                let m = e.value();
                let price: f64 = m.price.to_string().parse().unwrap_or(0.0);
                let size: f64 = m.order_size.to_string().parse().unwrap_or(0.0);
                RewardOrder {
                    side: match m.side {
                        QuoteSide::Buy => RewardSide::Buy,
                        QuoteSide::Sell => RewardSide::Sell,
                    },
                    market: RewardMarket::Yes,
                    price,
                    size,
                }
            })
            .collect();

        let mut my_bid_at: HashMap<u16, f64> = HashMap::new();
        let mut my_ask_at: HashMap<u16, f64> = HashMap::new();
        for order in &my_orders {
            let p = (order.price * PRICE_SCALE).round() as u16;
            match order.side {
                RewardSide::Buy => *my_bid_at.entry(p).or_default() += order.size,
                RewardSide::Sell => *my_ask_at.entry(p).or_default() += order.size,
            }
        }

        let mut competitor_orders: Vec<RewardOrder> = Vec::new();
        for (&p, &s) in snapshot.bids.iter() {
            let comp = (s as f64 / SIZE_SCALE - my_bid_at.get(&p).copied().unwrap_or(0.0)).max(0.0);
            if comp > 0.0 {
                competitor_orders.push(RewardOrder {
                    side: RewardSide::Buy,
                    market: RewardMarket::Yes,
                    price: p as f64 / PRICE_SCALE,
                    size: comp,
                });
            }
        }
        for (&p, &s) in snapshot.asks.iter() {
            let comp = (s as f64 / SIZE_SCALE - my_ask_at.get(&p).copied().unwrap_or(0.0)).max(0.0);
            if comp > 0.0 {
                competitor_orders.push(RewardOrder {
                    side: RewardSide::Sell,
                    market: RewardMarket::Yes,
                    price: p as f64 / PRICE_SCALE,
                    size: comp,
                });
            }
        }

        let params = MarketParams {
            mid_price: mid,
            max_spread_cents: config.max_spread_cents,
            scale_factor: 3.0,
            reward_pool: config.daily_reward_pool,
        };
        let est = estimate_reward(&my_orders, &competitor_orders, &params);

        info!(
            target: "order",
            token = %token,
            mid = format!("{:.4}", mid),
            my_orders = my_orders.len(),
            my_qone = format!("{:.4}", est.my_qone),
            my_qtwo = format!("{:.4}", est.my_qtwo),
            my_qmin = format!("{:.4}", est.my_qmin),
            competitors_qmin = format!("{:.4}", est.competitors_qmin),
            my_share_pct = format!("{:.2}%", est.share * 100.0),
            estimated_daily_reward = format!("{:.4}", est.estimated_reward),
            below_min_payout = est.estimated_reward > 0.0 && est.estimated_reward < 1.0,
            "mid_requote 流动性奖励实时估算"
        );

        last_log.insert(token.to_string(), now);
    }
}

pub async fn run_mid_reward_monitor(auth: AuthConfig, interval_secs: u64) {
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
        .iter()
        .flat_map(|reward| reward.earnings.iter())
        .fold(Decimal::ZERO, |acc, earning| acc + earning.earnings);

    info!(
        target = "order",
        since = %date,
        market_count = rewards.len(),
        reward_percentage_count = reward_percentages.len(),
        total_earnings = %total_earnings,
        "user_reward_monitor 当前用户奖励汇总"
    );

    for reward in rewards {
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
