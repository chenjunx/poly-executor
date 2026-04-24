use std::time::Duration;

use chrono::{TimeDelta, Utc};
use polymarket_client_sdk::clob::types::request::UserRewardsEarningRequest;
use polymarket_client_sdk::types::Decimal;
use tracing::{info, warn};

use crate::clob_client::build_authenticated_clob_client;
use crate::config::AuthConfig;

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

