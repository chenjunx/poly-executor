use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE;
use chrono::{Duration as ChronoDuration, NaiveDate, NaiveDateTime, Utc};
use hmac::{Hmac, Mac as _};
use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::auth::{LocalSigner, Signer as _};
use polymarket_client_sdk_v2::clob::types::response::{
    CurrentRewardResponse, MarketRewardResponse, Page, Token,
};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketBySlugRequest;
use polymarket_client_sdk_v2::types::Decimal;
use reqwest::header::HeaderMap;
use reqwest::{Method, Request};
use serde::Deserialize;
use sha2::Sha256;
use tracing::{info, warn};

use crate::clob_client::{AuthenticatedClobClient, build_authenticated_clob_client};
use crate::config::AuthConfig;
use crate::storage::{
    MarketStore, RemovedLiquidityRewardPoolEntry, RewardMarketPoolStorageEntry,
    StoredRewardMarketPoolMeta,
};
use crate::strategy::{RewardPoolRemovalEvent, StrategyEvent};

#[derive(Debug, Clone, Deserialize)]
struct RewardCurrentMarket {
    #[serde(flatten)]
    market: CurrentRewardResponse,
    #[serde(alias = "totalDailyRate")]
    total_daily_rate: Option<Decimal>,
}

#[derive(Clone)]
struct RewardMarketPoolEntry {
    market: Arc<RewardCurrentMarket>,
    detail: Arc<MarketRewardResponse>,
    gamma: RewardMarketGammaSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct RewardMarketGammaSnapshot {
    end_time: Option<NaiveDateTime>,
    volume_24hr_clob: Option<Decimal>,
    volume_24hr: Option<Decimal>,
}

fn selected_volume_24hr(snapshot: &RewardMarketGammaSnapshot) -> Option<Decimal> {
    snapshot.volume_24hr_clob.or(snapshot.volume_24hr)
}

fn volume_passes_threshold(volume: Option<Decimal>) -> bool {
    volume.is_some_and(|volume| volume >= Decimal::from(50_000u32))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RewardMarketPoolMeta {
    pub build_date_utc: NaiveDate,
    pub version: u64,
    pub built_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewardMarketPoolBuildTrigger {
    Startup,
}

fn should_build_reward_market_pool(
    last_success_meta: Option<RewardMarketPoolMeta>,
    trigger: RewardMarketPoolBuildTrigger,
) -> bool {
    match trigger {
        RewardMarketPoolBuildTrigger::Startup => last_success_meta.is_none(),
    }
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
    volume_24hr_clob: Option<String>,
    volume_24hr: Option<String>,
    liquidity_reward_roi: Option<String>,
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
    MinDaysBeforeEnd(i64),
    PositiveRewardsMinSize,
}

impl RewardMarketPoolRules {
    pub fn new(rules: impl Into<Arc<[RewardMarketRule]>>) -> Self {
        Self {
            rules: rules.into(),
        }
    }

    pub fn default_active_pool(_max_rewards_min_size: Option<Decimal>) -> Self {
        Self::new(vec![
            RewardMarketRule::MinDaysBeforeEnd(60),
            RewardMarketRule::PositiveRewardsMinSize,
        ])
    }

    fn basic_matches(&self, market: &CurrentRewardResponse) -> bool {
        self.rules.iter().all(|rule| rule.basic_matches(market))
    }

    fn gamma_matches(&self, gamma: &RewardMarketGammaSnapshot, now: NaiveDateTime) -> bool {
        self.rules.iter().all(|rule| rule.gamma_matches(gamma, now))
    }
}

impl RewardMarketRule {
    fn basic_matches(&self, market: &CurrentRewardResponse) -> bool {
        match self {
            Self::MinDaysBeforeEnd(_) => true,
            Self::PositiveRewardsMinSize => market.rewards_min_size > Decimal::ZERO,
        }
    }

    fn gamma_matches(&self, gamma: &RewardMarketGammaSnapshot, now: NaiveDateTime) -> bool {
        match self {
            Self::MinDaysBeforeEnd(min_days_before_end) => gamma.end_time.is_some_and(|end_time| {
                end_time - now > ChronoDuration::days(*min_days_before_end)
            }),
            Self::PositiveRewardsMinSize => true,
        }
    }
}

pub async fn run_reward_market_loader(
    auth: AuthConfig,
    store: MarketStore,
    refresh_interval: Duration,
    liquidity_reward_market_count: usize,
    max_rewards_min_size: Option<Decimal>,
    strategy_tx: tokio::sync::mpsc::Sender<StrategyEvent>,
) {
    let pool_rules = RewardMarketPoolRules::default_active_pool(max_rewards_min_size);
    let retry_interval = refresh_interval.max(Duration::from_secs(60));
    let mut last_success_meta = match load_initial_reward_market_pool_meta(&store) {
        Ok(meta) => meta,
        Err(error) => {
            warn!(target: "order", error = %error, "reward_market_loader 读取已有奖励市场池构建状态失败，将按需重建");
            None
        }
    };
    loop {
        let now = Utc::now();
        let build_date_utc = now.date_naive();
        if !should_build_reward_market_pool(
            last_success_meta,
            RewardMarketPoolBuildTrigger::Startup,
        ) {
            info!(
                target: "order",
                pool_version = last_success_meta.map(|meta| meta.version),
                "reward_market_loader 已存在奖励市场池，跳过自动重建"
            );
            tokio::time::sleep(retry_interval).await;
            continue;
        }

        match load_reward_markets_once(
            &auth,
            &store,
            &pool_rules,
            build_date_utc,
            liquidity_reward_market_count,
            &strategy_tx,
        )
        .await
        {
            Ok(meta) => {
                last_success_meta = Some(meta);
                info!(
                    target: "order",
                    build_date_utc = %meta.build_date_utc,
                    pool_version = meta.version,
                    "reward_market_loader 奖励市场池首次构建完成"
                );
                tokio::time::sleep(retry_interval).await;
            }
            Err(error) => {
                warn!(
                    target: "order",
                    build_date_utc = %build_date_utc,
                    retry_secs = retry_interval.as_secs(),
                    error = %error,
                    "reward_market_loader 奖励市场池构建失败，保留旧缓存并稍后重试"
                );
                tokio::time::sleep(retry_interval).await;
            }
        }
    }
}

fn load_initial_reward_market_pool_meta(
    store: &MarketStore,
) -> anyhow::Result<Option<RewardMarketPoolMeta>> {
    Ok(store
        .load_latest_reward_market_pool_meta()?
        .map(reward_market_pool_meta_from_storage))
}

fn reward_market_pool_meta_from_storage(meta: StoredRewardMarketPoolMeta) -> RewardMarketPoolMeta {
    RewardMarketPoolMeta {
        build_date_utc: meta.build_date_utc,
        version: meta.version,
        built_at_ms: meta.built_at_ms,
    }
}

async fn load_reward_markets_once(
    auth: &AuthConfig,
    store: &MarketStore,
    pool_rules: &RewardMarketPoolRules,
    build_date_utc: NaiveDate,
    liquidity_reward_market_count: usize,
    strategy_tx: &tokio::sync::mpsc::Sender<StrategyEvent>,
) -> anyhow::Result<RewardMarketPoolMeta> {
    let client = build_authenticated_clob_client(auth).await?;
    let mut markets = Vec::new();
    let mut next_cursor = None;
    let now = Utc::now().naive_utc();

    loop {
        let page = load_current_rewards_page(auth, next_cursor.clone()).await?;
        markets.extend(
            page.data
                .into_iter()
                .filter(|market| market.total_daily_rate.is_some()),
        );

        let cursor = page.next_cursor.trim();
        if cursor.is_empty() || cursor == "LTE=" {
            break;
        }
        next_cursor = Some(cursor.to_string());
    }

    let loaded_market_count = markets.len();
    let basic_pool_candidates = markets
        .iter()
        .filter(|market| pool_rules.basic_matches(&market.market))
        .cloned()
        .collect::<Vec<_>>();
    let basic_pool_candidate_count = basic_pool_candidates.len();
    let pool_entries = load_pool_entries(&client, basic_pool_candidates, pool_rules, now).await?;
    let detailed_pool_candidate_count = pool_entries.len();
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
            volume_24hr_clob: entry.volume_24hr_clob.as_deref(),
            volume_24hr: entry.volume_24hr.as_deref(),
            liquidity_reward_roi: entry.liquidity_reward_roi.as_deref(),
        })
        .collect::<Vec<_>>();
    let replace_result = store.replace_reward_market_pool_entries(
        build_date_utc,
        built_at_ms,
        &borrowed_entries,
        built_at_ms,
        liquidity_reward_market_count,
    )?;
    let selected_liquidity_reward_market_count = replace_result.selected_count;
    notify_removed_liquidity_reward_pool_entries(
        replace_result.removed_selected_entries,
        strategy_tx,
        build_date_utc,
    );
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

async fn load_current_rewards_page(
    auth: &AuthConfig,
    next_cursor: Option<String>,
) -> anyhow::Result<Page<RewardCurrentMarket>> {
    let cursor = next_cursor.map_or(String::new(), |cursor| format!("?next_cursor={cursor}"));
    let url = format!("https://clob.polymarket.com/rewards/markets/current{cursor}");
    let http_client = reqwest::Client::new();
    let request = http_client.request(Method::GET, url).build()?;
    let headers = create_clob_l2_headers(auth, &request)?;
    let response = http_client
        .execute(request_with_headers(request, headers)?)
        .await?;
    let mut body = response.error_for_status()?.bytes().await?.to_vec();
    Ok(simd_json::from_slice(&mut body)?)
}

fn request_with_headers(mut request: Request, headers: HeaderMap) -> anyhow::Result<Request> {
    request.headers_mut().extend(headers);
    Ok(request)
}

fn create_clob_l2_headers(auth: &AuthConfig, request: &Request) -> anyhow::Result<HeaderMap> {
    let signer = LocalSigner::from_str(&auth.private_key)?.with_chain_id(Some(POLYGON));
    let timestamp = Utc::now().timestamp();
    let message = format!(
        "{timestamp}{}{}{}",
        request.method(),
        request.url().path(),
        request_body_string(request)
    );
    let decoded_secret = URL_SAFE.decode(&auth.api_secret)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&decoded_secret)?;
    mac.update(message.as_bytes());
    let signature = URL_SAFE.encode(mac.finalize().into_bytes());

    let mut headers = HeaderMap::new();
    headers.insert("POLY_ADDRESS", signer.address().to_checksum(None).parse()?);
    headers.insert("POLY_API_KEY", auth.api_key.parse()?);
    headers.insert("POLY_PASSPHRASE", auth.passphrase.parse()?);
    headers.insert("POLY_SIGNATURE", signature.parse()?);
    headers.insert("POLY_TIMESTAMP", timestamp.to_string().parse()?);
    Ok(headers)
}

fn request_body_string(request: &Request) -> String {
    request
        .body()
        .and_then(|body| body.as_bytes())
        .map(String::from_utf8_lossy)
        .map(|body| body.replace('\'', "\""))
        .unwrap_or_default()
}

fn notify_removed_liquidity_reward_pool_entries(
    removed_entries: Vec<RemovedLiquidityRewardPoolEntry>,
    strategy_tx: &tokio::sync::mpsc::Sender<StrategyEvent>,
    build_date_utc: NaiveDate,
) {
    for entry in removed_entries {
        let reason = format!("reward_pool_rebuild_removed: build_date_utc={build_date_utc}");
        if let Err(error) =
            strategy_tx.try_send(StrategyEvent::RewardPoolRemoval(RewardPoolRemovalEvent {
                condition_id: entry.condition_id.clone(),
                token1: entry.token1,
                token2: entry.token2,
                reason,
            }))
        {
            warn!(target: "order", condition_id = %entry.condition_id, error = %error, "reward_market_loader 投递重建移除奖励市场事件失败");
        }
    }
}

async fn load_pool_entries(
    client: &AuthenticatedClobClient,
    markets: Vec<RewardCurrentMarket>,
    pool_rules: &RewardMarketPoolRules,
    now: NaiveDateTime,
) -> anyhow::Result<Vec<RewardMarketPoolEntry>> {
    let gamma_client = GammaClient::default();
    let mut entries = Vec::new();
    for market in markets {
        let condition_id = normalize_condition_id(&market.market.condition_id.to_string());
        match load_market_reward_detail(client, &condition_id).await {
            Ok(Some(detail)) => {
                match load_gamma_market_snapshot(&gamma_client, &detail.market_slug).await {
                    Ok(gamma) => {
                        if !pool_rules.gamma_matches(&gamma, now) {
                            continue;
                        }
                        let volume_24hr = selected_volume_24hr(&gamma);
                        if !volume_passes_threshold(volume_24hr) {
                            continue;
                        }
                        entries.push(RewardMarketPoolEntry {
                            market: Arc::new(market),
                            detail: Arc::new(detail),
                            gamma,
                        });
                    }
                    Err(error) => {
                        warn!(target: "order", condition_id = %condition_id, market_slug = %detail.market_slug, error = %error, "reward_market_loader 查询 Gamma 市场成交量失败，跳过市场池候选")
                    }
                }
            }
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

async fn load_gamma_market_snapshot(
    gamma_client: &GammaClient,
    market_slug: &str,
) -> anyhow::Result<RewardMarketGammaSnapshot> {
    let market = gamma_client
        .market_by_slug(&MarketBySlugRequest::builder().slug(market_slug).build())
        .await?;
    Ok(RewardMarketGammaSnapshot {
        end_time: market.end_date.map(|end_date| end_date.naive_utc()),
        volume_24hr_clob: market.volume_24hr_clob,
        volume_24hr: market.volume_24hr,
    })
}

fn pool_entries_to_storage_entries(
    entries: &[RewardMarketPoolEntry],
) -> Vec<OwnedRewardMarketPoolStorageEntry> {
    entries
        .iter()
        .filter_map(|entry| {
            let Some(daily_reward) = market_daily_reward(&entry.market) else {
                return None;
            };
            let tokens = &entry.detail.tokens;
            if tokens.len() < 2 {
                warn!(target: "order", condition_id = %entry.market.market.condition_id, token_count = tokens.len(), "reward_market_loader 奖励市场 token 数不足，跳过入库");
                return None;
            }
            Some(OwnedRewardMarketPoolStorageEntry {
                condition_id: normalize_condition_id(&entry.market.market.condition_id.to_string()),
                market_slug: Some(entry.detail.market_slug.clone()),
                question: Some(entry.detail.question.clone()),
                token1: tokens[0].token_id.to_string(),
                token2: tokens[1].token_id.to_string(),
                tokens_json: tokens_json(tokens),
                market_competitiveness: Some(entry.market_competitiveness().to_string()),
                rewards_min_size: Some(entry.detail.rewards_min_size.to_string()),
                rewards_max_spread: Some(entry.detail.rewards_max_spread.to_string()),
                market_daily_reward: Some(daily_reward.to_string()),
                volume_24hr_clob: entry.gamma.volume_24hr_clob.map(|volume| volume.to_string()),
                volume_24hr: entry.gamma.volume_24hr.map(|volume| volume.to_string()),
                liquidity_reward_roi: liquidity_reward_roi(entry).map(|value| value.to_string()),
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

fn now_ms() -> anyhow::Result<u64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(duration.as_millis() as u64)
}

fn market_daily_reward(market: &RewardCurrentMarket) -> Option<Decimal> {
    market.total_daily_rate
}

fn liquidity_reward_roi(entry: &RewardMarketPoolEntry) -> Option<Decimal> {
    if entry.detail.rewards_min_size <= Decimal::ZERO {
        return None;
    }
    Some(market_daily_reward(&entry.market)? / entry.detail.rewards_min_size)
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
        market_with_min_size(condition_id, rate_per_day, end_date, "1")
    }

    fn reward_current_market(
        condition_id: B256,
        total_daily_rate: Option<Decimal>,
    ) -> RewardCurrentMarket {
        RewardCurrentMarket {
            market: market(condition_id, "51", "2026-05-10"),
            total_daily_rate,
        }
    }

    fn market_with_min_size(
        condition_id: B256,
        rate_per_day: &str,
        end_date: &str,
        rewards_min_size: &str,
    ) -> CurrentRewardResponse {
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
            "rewards_min_size": rewards_min_size,
        }))
        .expect("test market should deserialize")
    }

    fn detail(condition_id: B256, competitiveness: &str) -> MarketRewardResponse {
        detail_with_min_size(condition_id, competitiveness, "1")
    }

    fn detail_with_min_size(
        condition_id: B256,
        competitiveness: &str,
        rewards_min_size: &str,
    ) -> MarketRewardResponse {
        serde_json::from_value(json!({
            "condition_id": condition_id.to_string(),
            "question": "question",
            "market_slug": condition_id.to_string(),
            "event_slug": "event",
            "image": "",
            "rewards_max_spread": "1",
            "rewards_min_size": rewards_min_size,
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
            market: Arc::new(reward_current_market(
                condition_id,
                Some(Decimal::from(51u32)),
            )),
            detail: Arc::new(detail(condition_id, competitiveness)),
            gamma: RewardMarketGammaSnapshot {
                end_time: Some(
                    NaiveDate::from_ymd_opt(2026, 8, 1)
                        .unwrap()
                        .and_hms_opt(23, 59, 59)
                        .unwrap(),
                ),
                volume_24hr_clob: Some(Decimal::from(60_000u32)),
                volume_24hr: Some(Decimal::from(70_000u32)),
            },
        }
    }

    #[test]
    fn liquidity_reward_roi_uses_total_daily_rate() {
        let entry = RewardMarketPoolEntry {
            market: Arc::new(RewardCurrentMarket {
                market: market(condition_id(1), "50", "2026-08-01"),
                total_daily_rate: Some(Decimal::from(80u32)),
            }),
            detail: Arc::new(detail_with_min_size(condition_id(1), "10", "200")),
            gamma: RewardMarketGammaSnapshot {
                end_time: Some(
                    NaiveDate::from_ymd_opt(2026, 8, 1)
                        .unwrap()
                        .and_hms_opt(23, 59, 59)
                        .unwrap(),
                ),
                volume_24hr_clob: Some(Decimal::from(60_000u32)),
                volume_24hr: None,
            },
        };

        assert_eq!(
            liquidity_reward_roi(&entry),
            Some(Decimal::try_from(0.4_f64).unwrap())
        );
    }

    #[test]
    fn liquidity_reward_roi_is_none_when_rewards_min_size_is_zero() {
        let entry = RewardMarketPoolEntry {
            market: Arc::new(reward_current_market(
                condition_id(1),
                Some(Decimal::from(50u32)),
            )),
            detail: Arc::new(detail_with_min_size(condition_id(1), "10", "0")),
            gamma: RewardMarketGammaSnapshot {
                end_time: Some(
                    NaiveDate::from_ymd_opt(2026, 8, 1)
                        .unwrap()
                        .and_hms_opt(23, 59, 59)
                        .unwrap(),
                ),
                volume_24hr_clob: Some(Decimal::from(60_000u32)),
                volume_24hr: None,
            },
        };

        assert_eq!(liquidity_reward_roi(&entry), None);
    }

    #[test]
    fn selected_volume_prefers_clob_volume_over_total_volume() {
        let snapshot = RewardMarketGammaSnapshot {
            end_time: Some(
                NaiveDate::from_ymd_opt(2026, 8, 1)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
            ),
            volume_24hr_clob: Some(Decimal::from(60_000u32)),
            volume_24hr: Some(Decimal::from(70_000u32)),
        };

        assert_eq!(
            selected_volume_24hr(&snapshot),
            Some(Decimal::from(60_000u32))
        );
    }

    #[test]
    fn selected_volume_falls_back_to_total_volume() {
        let snapshot = RewardMarketGammaSnapshot {
            end_time: Some(
                NaiveDate::from_ymd_opt(2026, 8, 1)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
            ),
            volume_24hr_clob: None,
            volume_24hr: Some(Decimal::from(55_000u32)),
        };

        assert_eq!(
            selected_volume_24hr(&snapshot),
            Some(Decimal::from(55_000u32))
        );
    }

    #[test]
    fn volume_rule_requires_at_least_fifty_thousand() {
        assert!(volume_passes_threshold(Some(Decimal::from(50_000u32))));
        assert!(volume_passes_threshold(Some(Decimal::from(50_001u32))));
        assert!(!volume_passes_threshold(Some(Decimal::from(49_999u32))));
        assert!(!volume_passes_threshold(None));
    }

    #[test]
    fn default_active_pool_uses_gamma_end_date_for_sixty_day_rule() {
        let rules = RewardMarketPoolRules::default_active_pool(None);
        let now = NaiveDate::from_ymd_opt(2026, 5, 13)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let gamma_too_soon = RewardMarketGammaSnapshot {
            end_time: Some(
                NaiveDate::from_ymd_opt(2026, 7, 11)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
            ),
            volume_24hr_clob: Some(Decimal::from(60_000u32)),
            volume_24hr: None,
        };
        let gamma_far_enough = RewardMarketGammaSnapshot {
            end_time: Some(
                NaiveDate::from_ymd_opt(2026, 7, 13)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap(),
            ),
            volume_24hr_clob: Some(Decimal::from(60_000u32)),
            volume_24hr: None,
        };

        assert!(!rules.gamma_matches(&gamma_too_soon, now));
        assert!(rules.gamma_matches(&gamma_far_enough, now));
    }

    #[test]
    fn default_active_pool_requires_positive_rewards_min_size() {
        let rules = RewardMarketPoolRules::default_active_pool(None);
        let zero_min_size = market_with_min_size(condition_id(1), "100", "2026-08-01", "0");
        let positive_min_size = market_with_min_size(condition_id(2), "100", "2026-08-01", "100");

        assert!(!rules.basic_matches(&zero_min_size));
        assert!(rules.basic_matches(&positive_min_size));
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
        assert_eq!(entries[0].volume_24hr_clob.as_deref(), Some("60000"));
        assert_eq!(entries[0].volume_24hr.as_deref(), Some("70000"));
        assert_eq!(entries[0].liquidity_reward_roi.as_deref(), Some("51"));
        assert!(entries[0].tokens_json.contains("Yes"));
    }

    #[test]
    fn skips_pool_entries_without_total_daily_rate() {
        let mut entry = pool_entry(condition_id(8), "0.5");
        Arc::make_mut(&mut entry.market).total_daily_rate = None;

        let entries = pool_entries_to_storage_entries(&[entry]);

        assert!(entries.is_empty());
    }

    #[test]
    fn skips_pool_entries_with_less_than_two_tokens() {
        let mut entry = pool_entry(condition_id(8), "0.5");
        Arc::make_mut(&mut entry.detail).tokens.truncate(1);

        let entries = pool_entries_to_storage_entries(&[entry]);

        assert!(entries.is_empty());
    }

    #[test]
    fn reward_pool_builds_when_no_previous_meta_exists() {
        assert!(should_build_reward_market_pool(
            None,
            RewardMarketPoolBuildTrigger::Startup
        ));
    }

    #[test]
    fn reward_pool_does_not_auto_rebuild_when_previous_meta_exists() {
        let meta = RewardMarketPoolMeta {
            build_date_utc: NaiveDate::from_ymd_opt(2026, 5, 12).unwrap(),
            version: 123,
            built_at_ms: 123,
        };

        assert!(!should_build_reward_market_pool(
            Some(meta),
            RewardMarketPoolBuildTrigger::Startup
        ));
    }

    #[test]
    fn loads_initial_reward_market_pool_meta_from_store() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let build_date_utc = NaiveDate::from_ymd_opt(2026, 5, 5).unwrap();
        let entries = vec![RewardMarketPoolStorageEntry {
            condition_id: "0xabc",
            market_slug: Some("slug"),
            question: Some("question"),
            token1: "token1",
            token2: "token2",
            tokens_json: "[]",
            market_competitiveness: Some("1"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("50"),
            volume_24hr_clob: None,
            volume_24hr: None,
            liquidity_reward_roi: None,
        }];
        store
            .replace_reward_market_pool_entries(build_date_utc, 123, &entries, 123, 1)
            .expect("pool should replace");

        let meta = load_initial_reward_market_pool_meta(&store)
            .expect("initial meta query should work")
            .expect("initial meta should exist");

        assert_eq!(meta.build_date_utc, build_date_utc);
        assert_eq!(meta.version, 123);
        assert_eq!(meta.built_at_ms, 123);
    }

    #[test]
    fn notifies_removed_liquidity_reward_pool_entries() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        notify_removed_liquidity_reward_pool_entries(
            vec![RemovedLiquidityRewardPoolEntry {
                condition_id: "0xremoved".to_string(),
                token1: "token1".to_string(),
                token2: "token2".to_string(),
            }],
            &tx,
            NaiveDate::from_ymd_opt(2026, 5, 5).unwrap(),
        );

        let event = rx.try_recv().expect("removal event should be sent");
        let StrategyEvent::RewardPoolRemoval(removal) = event else {
            panic!("expected reward pool removal event");
        };
        assert_eq!(removal.condition_id, "0xremoved");
        assert_eq!(removal.token1, "token1");
        assert_eq!(removal.token2, "token2");
        assert_eq!(
            removal.reason,
            "reward_pool_rebuild_removed: build_date_utc=2026-05-05"
        );
    }

    #[test]
    fn normalizes_condition_id_prefix() {
        assert_eq!(normalize_condition_id("abc"), "0xabc");
        assert_eq!(normalize_condition_id("0xabc"), "0xabc");
    }
}
