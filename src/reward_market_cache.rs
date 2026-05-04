use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, NaiveDateTime, Utc};
use polymarket_client_sdk_v2::clob::types::response::{
    CurrentRewardResponse, MarketRewardResponse, Token,
};
use polymarket_client_sdk_v2::types::Decimal;
use tracing::{info, warn};

use crate::clob_client::{AuthenticatedClobClient, build_authenticated_clob_client};
use crate::config::AuthConfig;
use crate::storage::{MarketStore, RewardMarketPoolStorageEntry};

#[derive(Clone)]
pub struct RewardMarketPoolEntry {
    pub market: Arc<CurrentRewardResponse>,
    pub detail: Arc<MarketRewardResponse>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RewardMarketPoolMeta {
    pub build_date_utc: NaiveDate,
    pub version: u64,
    pub built_at_ms: u64,
}

#[derive(Debug, Clone)]
struct OwnedRewardMarketPoolStorageEntry {
    condition_id: String,
    market_slug: Option<String>,
    question: Option<String>,
    token1: String,
    token2: String,
    tokens_json: String,
    market_competitiveness: Option<String>,
    rewards_min_size: Option<String>,
    rewards_max_spread: Option<String>,
    market_daily_reward: Option<String>,
}

impl RewardMarketPoolEntry {
    pub fn market_competitiveness(&self) -> Decimal {
        self.detail.market_competitiveness
    }
}

#[derive(Clone)]
pub struct RewardMarketPoolRules {
    rules: Arc<[RewardMarketRule]>,
}

#[derive(Clone)]
pub enum RewardMarketRule {
    MinDailyReward(Decimal),
    MinHoursBeforeEnd(i64),
    ExcludeCompetitivenessTails {
        lower_percent: u32,
        upper_percent: u32,
    },
}

impl RewardMarketPoolRules {
    pub fn new(rules: impl Into<Arc<[RewardMarketRule]>>) -> Self {
        Self {
            rules: rules.into(),
        }
    }

    pub fn default_active_pool() -> Self {
        Self::new([
            RewardMarketRule::MinDailyReward(Decimal::from(50u32)),
            RewardMarketRule::MinHoursBeforeEnd(48),
            RewardMarketRule::ExcludeCompetitivenessTails {
                lower_percent: 20,
                upper_percent: 20,
            },
        ])
    }

    fn basic_matches(&self, market: &CurrentRewardResponse, now: NaiveDateTime) -> bool {
        self.rules
            .iter()
            .all(|rule| rule.basic_matches(market, now))
    }

    fn apply_pool_rules(&self, entries: Vec<RewardMarketPoolEntry>) -> Vec<RewardMarketPoolEntry> {
        self.rules
            .iter()
            .fold(entries, |entries, rule| rule.apply_pool_rule(entries))
    }
}

impl RewardMarketRule {
    fn basic_matches(&self, market: &CurrentRewardResponse, now: NaiveDateTime) -> bool {
        match self {
            Self::MinDailyReward(min_daily_reward) => {
                market_daily_reward(market) > *min_daily_reward
            }
            Self::MinHoursBeforeEnd(min_hours_before_end) => {
                market_end_time(market).is_some_and(|end_time| {
                    end_time - now > ChronoDuration::hours(*min_hours_before_end)
                })
            }
            Self::ExcludeCompetitivenessTails { .. } => true,
        }
    }

    fn apply_pool_rule(&self, entries: Vec<RewardMarketPoolEntry>) -> Vec<RewardMarketPoolEntry> {
        match self {
            Self::ExcludeCompetitivenessTails {
                lower_percent,
                upper_percent,
            } => exclude_competitiveness_tails(entries, *lower_percent, *upper_percent),
            _ => entries,
        }
    }
}

pub async fn run_reward_market_loader(
    auth: AuthConfig,
    store: MarketStore,
    refresh_interval: Duration,
    liquidity_reward_market_count: usize,
) {
    let pool_rules = RewardMarketPoolRules::default_active_pool();
    let retry_interval = refresh_interval.max(Duration::from_secs(60));
    let mut last_success_meta: Option<RewardMarketPoolMeta> = None;
    loop {
        let now = Utc::now();
        let build_date_utc = now.date_naive();
        if last_success_meta.map(|meta| meta.build_date_utc) == Some(build_date_utc) {
            tokio::time::sleep(duration_until_next_utc_midnight(now)).await;
            continue;
        }

        match load_reward_markets_once(
            &auth,
            &store,
            &pool_rules,
            build_date_utc,
            liquidity_reward_market_count,
        )
        .await
        {
            Ok(meta) => {
                last_success_meta = Some(meta);
                info!(
                    target: "order",
                    build_date_utc = %meta.build_date_utc,
                    pool_version = meta.version,
                    "reward_market_loader 当日奖励市场池构建完成，等待下一个 UTC 零点"
                );
                tokio::time::sleep(duration_until_next_utc_midnight(Utc::now())).await;
            }
            Err(error) => {
                warn!(
                    target: "order",
                    build_date_utc = %build_date_utc,
                    retry_secs = retry_interval.as_secs(),
                    error = %error,
                    "reward_market_loader 当日奖励市场池构建失败，保留旧缓存并稍后重试"
                );
                tokio::time::sleep(retry_interval).await;
            }
        }
    }
}

async fn load_reward_markets_once(
    auth: &AuthConfig,
    store: &MarketStore,
    pool_rules: &RewardMarketPoolRules,
    build_date_utc: NaiveDate,
    liquidity_reward_market_count: usize,
) -> anyhow::Result<RewardMarketPoolMeta> {
    let client = build_authenticated_clob_client(auth).await?;
    let mut markets = Vec::new();
    let mut next_cursor = None;
    let now = Utc::now().naive_utc();

    loop {
        let page = client.current_rewards(next_cursor.clone()).await?;
        markets.extend(page.data);

        let cursor = page.next_cursor.trim();
        if cursor.is_empty() || cursor == "LTE=" {
            break;
        }
        next_cursor = Some(cursor.to_string());
    }

    let loaded_market_count = markets.len();
    let basic_pool_candidates = markets
        .iter()
        .filter(|market| pool_rules.basic_matches(market, now))
        .cloned()
        .collect::<Vec<_>>();
    let basic_pool_candidate_count = basic_pool_candidates.len();
    let pool_entries = load_pool_entries(&client, basic_pool_candidates).await?;
    let detailed_pool_candidate_count = pool_entries.len();
    let pool_entries = pool_rules.apply_pool_rules(pool_entries);
    let reward_market_pool_count = pool_entries.len();
    let owned_entries = pool_entries_to_storage_entries(&pool_entries);
    let stored_reward_market_pool_count = owned_entries.len();
    let built_at_ms = now_ms()?;
    let borrowed_entries = owned_entries
        .iter()
        .map(|entry| RewardMarketPoolStorageEntry {
            condition_id: &entry.condition_id,
            market_slug: entry.market_slug.as_deref(),
            question: entry.question.as_deref(),
            token1: &entry.token1,
            token2: &entry.token2,
            tokens_json: &entry.tokens_json,
            market_competitiveness: entry.market_competitiveness.as_deref(),
            rewards_min_size: entry.rewards_min_size.as_deref(),
            rewards_max_spread: entry.rewards_max_spread.as_deref(),
            market_daily_reward: entry.market_daily_reward.as_deref(),
        })
        .collect::<Vec<_>>();
    let selected_liquidity_reward_market_count = store.replace_reward_market_pool_entries(
        build_date_utc,
        built_at_ms,
        &borrowed_entries,
        built_at_ms,
        liquidity_reward_market_count,
    )?;
    let meta = RewardMarketPoolMeta {
        build_date_utc,
        version: built_at_ms,
        built_at_ms,
    };

    info!(
        target: "order",
        build_date_utc = %meta.build_date_utc,
        pool_version = meta.version,
        loaded_market_count,
        basic_pool_candidate_count,
        detailed_pool_candidate_count,
        reward_market_pool_count,
        stored_reward_market_pool_count,
        selected_liquidity_reward_market_count,
        "reward_market_loader 当前奖励市场加载完成并已写入数据库"
    );

    Ok(meta)
}

async fn load_pool_entries(
    client: &AuthenticatedClobClient,
    markets: Vec<CurrentRewardResponse>,
) -> anyhow::Result<Vec<RewardMarketPoolEntry>> {
    let mut entries = Vec::new();
    for market in markets {
        let condition_id = normalize_condition_id(&market.condition_id.to_string());
        match load_market_reward_detail(client, &condition_id).await {
            Ok(Some(detail)) => entries.push(RewardMarketPoolEntry {
                market: Arc::new(market),
                detail: Arc::new(detail),
            }),
            Ok(None) => {
                warn!(target: "order", condition_id = %condition_id, "reward_market_loader 当前奖励市场详情为空，跳过市场池候选")
            }
            Err(error) => {
                warn!(target: "order", condition_id = %condition_id, error = %error, "reward_market_loader 查询奖励市场详情失败，跳过市场池候选")
            }
        }
    }
    Ok(entries)
}

fn pool_entries_to_storage_entries(
    entries: &[RewardMarketPoolEntry],
) -> Vec<OwnedRewardMarketPoolStorageEntry> {
    entries
        .iter()
        .filter_map(|entry| {
            let tokens = &entry.detail.tokens;
            if tokens.len() < 2 {
                warn!(target: "order", condition_id = %entry.market.condition_id, token_count = tokens.len(), "reward_market_loader 奖励市场 token 数不足，跳过入库");
                return None;
            }
            Some(OwnedRewardMarketPoolStorageEntry {
                condition_id: normalize_condition_id(&entry.market.condition_id.to_string()),
                market_slug: Some(entry.detail.market_slug.clone()),
                question: Some(entry.detail.question.clone()),
                token1: tokens[0].token_id.to_string(),
                token2: tokens[1].token_id.to_string(),
                tokens_json: tokens_json(tokens),
                market_competitiveness: Some(entry.market_competitiveness().to_string()),
                rewards_min_size: Some(entry.detail.rewards_min_size.to_string()),
                rewards_max_spread: Some(entry.detail.rewards_max_spread.to_string()),
                market_daily_reward: Some(market_daily_reward(&entry.market).to_string()),
            })
        })
        .collect()
}

async fn load_market_reward_detail(
    client: &AuthenticatedClobClient,
    condition_id: &str,
) -> anyhow::Result<Option<MarketRewardResponse>> {
    let mut next_cursor = None;
    loop {
        let page = client
            .raw_rewards_for_market(condition_id, next_cursor.clone())
            .await?;
        if let Some(market) = page.data.into_iter().next() {
            return Ok(Some(market));
        }

        let cursor = page.next_cursor.trim();
        if cursor.is_empty() || cursor == "LTE=" {
            return Ok(None);
        }
        next_cursor = Some(cursor.to_string());
    }
}

fn exclude_competitiveness_tails(
    mut entries: Vec<RewardMarketPoolEntry>,
    lower_percent: u32,
    upper_percent: u32,
) -> Vec<RewardMarketPoolEntry> {
    entries.sort_by(|left, right| {
        left.market_competitiveness()
            .partial_cmp(&right.market_competitiveness())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let len = entries.len();
    let lower_cut = percentile_cut_count(len, lower_percent);
    let upper_cut = percentile_cut_count(len, upper_percent);
    entries
        .into_iter()
        .skip(lower_cut)
        .take(len.saturating_sub(lower_cut + upper_cut))
        .collect()
}

fn percentile_cut_count(len: usize, percent: u32) -> usize {
    (len * percent as usize) / 100
}

fn tokens_json(tokens: &[Token]) -> String {
    let values = tokens
        .iter()
        .map(|token| {
            serde_json::json!({
                "token_id": token.token_id.to_string(),
                "outcome": token.outcome,
                "price": token.price.to_string(),
                "winner": token.winner,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&values).unwrap_or_else(|_| "[]".to_string())
}

fn duration_until_next_utc_midnight(now: DateTime<Utc>) -> Duration {
    let next_day = now.date_naive() + ChronoDuration::days(1);
    let Some(next_midnight) = next_day.and_hms_opt(0, 0, 0).map(|time| time.and_utc()) else {
        return Duration::from_secs(60);
    };
    (next_midnight - now)
        .to_std()
        .unwrap_or_else(|_| Duration::from_secs(60))
}

fn now_ms() -> anyhow::Result<u64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(duration.as_millis() as u64)
}

fn market_daily_reward(market: &CurrentRewardResponse) -> Decimal {
    market
        .rewards_config
        .iter()
        .map(|config| config.rate_per_day)
        .fold(Decimal::ZERO, |total, reward| total + reward)
}

fn market_end_time(market: &CurrentRewardResponse) -> Option<NaiveDateTime> {
    market
        .rewards_config
        .iter()
        .map(|config| config.end_date)
        .max()
        .and_then(|end_date| end_date.and_hms_opt(23, 59, 59))
}

fn normalize_condition_id(condition_id: &str) -> String {
    if condition_id.starts_with("0x") {
        condition_id.to_string()
    } else {
        format!("0x{condition_id}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::B256;
    use serde_json::json;

    fn condition_id(byte: u8) -> B256 {
        B256::repeat_byte(byte)
    }

    fn market(condition_id: B256, rate_per_day: &str, end_date: &str) -> CurrentRewardResponse {
        serde_json::from_value(json!({
            "condition_id": condition_id.to_string(),
            "rewards_config": [{
                "asset_address": "0x0000000000000000000000000000000000000000",
                "start_date": "2026-05-01",
                "end_date": end_date,
                "rate_per_day": rate_per_day,
                "total_rewards": "1000"
            }],
            "rewards_max_spread": "1",
            "rewards_min_size": "1",
        }))
        .expect("test market should deserialize")
    }

    fn detail(condition_id: B256, competitiveness: &str) -> MarketRewardResponse {
        serde_json::from_value(json!({
            "condition_id": condition_id.to_string(),
            "question": "question",
            "market_slug": condition_id.to_string(),
            "event_slug": "event",
            "image": "",
            "rewards_max_spread": "1",
            "rewards_min_size": "1",
            "market_competitiveness": competitiveness,
            "tokens": [{
                "token_id": "1",
                "outcome": "Yes",
                "price": "0",
                "winner": false
            }, {
                "token_id": "2",
                "outcome": "No",
                "price": "0",
                "winner": false
            }],
            "rewards_config": [],
        }))
        .expect("test market detail should deserialize")
    }

    fn pool_entry(condition_id: B256, competitiveness: &str) -> RewardMarketPoolEntry {
        RewardMarketPoolEntry {
            market: Arc::new(market(condition_id, "51", "2026-05-10")),
            detail: Arc::new(detail(condition_id, competitiveness)),
        }
    }

    fn test_now() -> NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2026, 5, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    }

    #[test]
    fn applies_daily_reward_and_end_time_basic_rules() {
        let rules = RewardMarketPoolRules::default_active_pool();
        let eligible = market(condition_id(4), "51", "2026-05-10");
        let low_reward = market(condition_id(5), "50", "2026-05-10");
        let ending_soon = market(condition_id(6), "51", "2026-05-03");

        assert!(rules.basic_matches(&eligible, test_now()));
        assert!(!rules.basic_matches(&low_reward, test_now()));
        assert!(!rules.basic_matches(&ending_soon, test_now()));
    }

    #[test]
    fn excludes_top_and_bottom_competitiveness_tails() {
        let rules = RewardMarketPoolRules::new([RewardMarketRule::ExcludeCompetitivenessTails {
            lower_percent: 20,
            upper_percent: 20,
        }]);
        let entries = (0..10)
            .map(|index| pool_entry(condition_id(index as u8), &index.to_string()))
            .collect::<Vec<_>>();

        let filtered = rules.apply_pool_rules(entries);
        let competitiveness = filtered
            .iter()
            .map(|entry| entry.market_competitiveness().to_string())
            .collect::<Vec<_>>();

        assert_eq!(competitiveness, vec!["2", "3", "4", "5", "6", "7"]);
    }

    #[test]
    fn converts_pool_entries_to_storage_entries() {
        let entries = pool_entries_to_storage_entries(&[pool_entry(condition_id(7), "0.5")]);

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].condition_id, condition_id(7).to_string());
        assert_eq!(entries[0].token1, "1");
        assert_eq!(entries[0].token2, "2");
        assert_eq!(entries[0].market_competitiveness.as_deref(), Some("0.5"));
        assert_eq!(entries[0].rewards_min_size.as_deref(), Some("1"));
        assert_eq!(entries[0].rewards_max_spread.as_deref(), Some("1"));
        assert_eq!(entries[0].market_daily_reward.as_deref(), Some("51"));
        assert!(entries[0].tokens_json.contains("Yes"));
    }

    #[test]
    fn skips_pool_entries_with_less_than_two_tokens() {
        let mut entry = pool_entry(condition_id(8), "0.5");
        Arc::make_mut(&mut entry.detail).tokens.truncate(1);

        let entries = pool_entries_to_storage_entries(&[entry]);

        assert!(entries.is_empty());
    }

    #[test]
    fn computes_duration_until_next_utc_midnight() {
        let parse = |value: &str| value.parse::<DateTime<Utc>>().unwrap();

        assert_eq!(
            duration_until_next_utc_midnight(parse("2026-05-03T00:00:00Z")).as_secs(),
            86_400
        );
        assert_eq!(
            duration_until_next_utc_midnight(parse("2026-05-03T12:00:00Z")).as_secs(),
            43_200
        );
        assert_eq!(
            duration_until_next_utc_midnight(parse("2026-05-03T23:59:30Z")).as_secs(),
            30
        );
    }

    #[test]
    fn normalizes_condition_id_prefix() {
        assert_eq!(normalize_condition_id("abc"), "0xabc");
        assert_eq!(normalize_condition_id("0xabc"), "0xabc");
    }
}
