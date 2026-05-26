use std::collections::HashMap;

use config::{Config, File};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct AppConfig {
    pub(crate) proxy: ProxySettings,
    pub(crate) app: AppSettings,
    pub(crate) auth: AuthConfig,
    pub(crate) order: OrderConfig,
    #[serde(default)]
    pub(crate) simulation: SimulationConfig,
    #[serde(default, alias = "mid_requote")]
    pub(crate) liquidity_reward: LiquidityRewardConfig,
    #[serde(default)]
    pub(crate) notification: NotificationConfig,
    #[serde(default)]
    pub(crate) market_maker: MarketMakerConfig,
    #[serde(default)]
    pub(crate) topic_threads: HashMap<String, usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProxySettings {
    pub(crate) url: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AppSettings {
    pub(crate) log_file: String,
    pub(crate) assets_file: String,
    #[serde(default)]
    pub(crate) sqlite_path: String,
    #[serde(default)]
    pub(crate) market_sqlite_path: String,
    pub(crate) min_diff: f64,
    pub(crate) max_spread: f64,
    pub(crate) min_price: f64,
    pub(crate) max_price: f64,
    pub(crate) default_threads: usize,
    #[serde(default)]
    pub(crate) tick_store_enabled: bool,
    #[serde(default)]
    pub(crate) raw_store_enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct AuthConfig {
    pub(crate) api_key: String,
    pub(crate) api_secret: String,
    pub(crate) passphrase: String,
    pub(crate) private_key: String,
    pub(crate) funder: String,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct OrderConfig {
    pub(crate) size_usdc: f64,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub(crate) struct SimulationConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct LiquidityRewardConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) file: String,
    #[serde(default = "default_liquidity_reward_source")]
    pub(crate) source: String,
    #[serde(default = "default_liquidity_reward_pool_market_count")]
    pub(crate) pool_market_count: usize,
    #[serde(default)]
    pub(crate) pool_max_rewards_min_size: Option<f64>,
    #[serde(default)]
    pub(crate) monitor_enabled: bool,
    #[serde(default)]
    pub(crate) simulation: bool,
    #[serde(default = "default_liquidity_reward_balance_cooldown_secs")]
    pub(crate) balance_cooldown_secs: u64,
}

impl Default for LiquidityRewardConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            file: String::new(),
            source: default_liquidity_reward_source(),
            pool_market_count: default_liquidity_reward_pool_market_count(),
            pool_max_rewards_min_size: None,
            monitor_enabled: false,
            simulation: false,
            balance_cooldown_secs: default_liquidity_reward_balance_cooldown_secs(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct MarketMakerConfig {
    #[serde(default = "default_market_maker_enabled")]
    pub(crate) enabled: bool,
    #[serde(default = "default_market_maker_file")]
    pub(crate) file: String,
    #[serde(default = "default_market_maker_max_inventory_usd")]
    pub(crate) max_inventory_usd: f64,
    #[serde(default = "default_market_maker_overweight_ratio")]
    pub(crate) overweight_ratio: f64,
    #[serde(default = "default_market_maker_default_max_spread")]
    pub(crate) default_max_spread: f64,
    #[serde(default = "default_market_maker_tick_size")]
    pub(crate) tick_size: f64,
    #[serde(default = "default_market_maker_min_size")]
    pub(crate) min_size: f64,
    #[serde(default = "default_market_maker_max_skew")]
    pub(crate) max_skew: f64,
    #[serde(default = "default_market_maker_volatility_window_ms")]
    pub(crate) volatility_window_ms: u64,
    #[serde(default = "default_market_maker_volatility_min_samples")]
    pub(crate) volatility_min_samples: usize,
    #[serde(default = "default_market_maker_volatility_threshold")]
    pub(crate) volatility_threshold: f64,
    #[serde(default = "default_market_maker_spread_cooldown_ms")]
    pub(crate) spread_cooldown_ms: u64,
    #[serde(default = "default_market_maker_volatility_cooldown_ms")]
    pub(crate) volatility_cooldown_ms: u64,
    #[serde(default = "default_market_maker_fair_midpoint_cooldown_ms")]
    pub(crate) fair_midpoint_cooldown_ms: u64,
    #[serde(default = "default_market_maker_fair_midpoint_min")]
    pub(crate) fair_midpoint_min: f64,
    #[serde(default = "default_market_maker_fair_midpoint_max")]
    pub(crate) fair_midpoint_max: f64,
    #[serde(default = "default_market_maker_abnormal_market_spread_multiplier")]
    pub(crate) abnormal_market_spread_multiplier: f64,
    #[serde(default = "default_market_maker_normal_quote_levels")]
    pub(crate) normal_quote_levels: usize,
    #[serde(default = "default_market_maker_overweight_quote_levels")]
    pub(crate) overweight_quote_levels: usize,
    #[serde(default = "default_market_maker_level_ratios")]
    pub(crate) level_ratios: Vec<f64>,
    #[serde(default = "default_market_maker_level_sizes_usd")]
    pub(crate) level_sizes_usd: Vec<f64>,
    #[serde(default = "default_market_maker_rebalance_level_share_ratios")]
    pub(crate) rebalance_level_share_ratios: Vec<f64>,
    #[serde(default = "default_market_maker_rebalance_max_usd_per_level")]
    pub(crate) rebalance_max_usd_per_level: Vec<f64>,
    #[serde(default = "default_market_maker_rebalance_max_usd_per_cycle")]
    pub(crate) rebalance_max_usd_per_cycle: f64,
    #[serde(default = "default_market_maker_reconcile_size_tolerance")]
    pub(crate) reconcile_size_tolerance: f64,
}

impl Default for MarketMakerConfig {
    fn default() -> Self {
        Self {
            enabled: default_market_maker_enabled(),
            file: default_market_maker_file(),
            max_inventory_usd: default_market_maker_max_inventory_usd(),
            overweight_ratio: default_market_maker_overweight_ratio(),
            default_max_spread: default_market_maker_default_max_spread(),
            tick_size: default_market_maker_tick_size(),
            min_size: default_market_maker_min_size(),
            max_skew: default_market_maker_max_skew(),
            volatility_window_ms: default_market_maker_volatility_window_ms(),
            volatility_min_samples: default_market_maker_volatility_min_samples(),
            volatility_threshold: default_market_maker_volatility_threshold(),
            spread_cooldown_ms: default_market_maker_spread_cooldown_ms(),
            volatility_cooldown_ms: default_market_maker_volatility_cooldown_ms(),
            fair_midpoint_cooldown_ms: default_market_maker_fair_midpoint_cooldown_ms(),
            fair_midpoint_min: default_market_maker_fair_midpoint_min(),
            fair_midpoint_max: default_market_maker_fair_midpoint_max(),
            abnormal_market_spread_multiplier:
                default_market_maker_abnormal_market_spread_multiplier(),
            normal_quote_levels: default_market_maker_normal_quote_levels(),
            overweight_quote_levels: default_market_maker_overweight_quote_levels(),
            level_ratios: default_market_maker_level_ratios(),
            level_sizes_usd: default_market_maker_level_sizes_usd(),
            rebalance_level_share_ratios: default_market_maker_rebalance_level_share_ratios(),
            rebalance_max_usd_per_level: default_market_maker_rebalance_max_usd_per_level(),
            rebalance_max_usd_per_cycle: default_market_maker_rebalance_max_usd_per_cycle(),
            reconcile_size_tolerance: default_market_maker_reconcile_size_tolerance(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub(crate) struct NotificationConfig {
    #[serde(default)]
    pub(crate) dingtalk: DingtalkConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct DingtalkConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) webhook: String,
    #[serde(default)]
    pub(crate) secret: String,
    #[serde(default = "default_dingtalk_timeout_secs")]
    pub(crate) timeout_secs: u64,
    #[serde(default = "default_dingtalk_queue_size")]
    pub(crate) queue_size: usize,
}

impl Default for DingtalkConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            webhook: String::new(),
            secret: String::new(),
            timeout_secs: default_dingtalk_timeout_secs(),
            queue_size: default_dingtalk_queue_size(),
        }
    }
}

fn default_liquidity_reward_source() -> String {
    "csv".to_string()
}

fn default_liquidity_reward_pool_market_count() -> usize {
    6
}

fn default_liquidity_reward_balance_cooldown_secs() -> u64 {
    60
}

fn default_market_maker_enabled() -> bool {
    true
}

fn default_market_maker_file() -> String {
    "market_maker.csv".to_string()
}

fn default_market_maker_max_inventory_usd() -> f64 {
    100.0
}

fn default_market_maker_overweight_ratio() -> f64 {
    0.7
}

fn default_market_maker_default_max_spread() -> f64 {
    0.03
}

fn default_market_maker_tick_size() -> f64 {
    0.01
}

fn default_market_maker_min_size() -> f64 {
    5.0
}

fn default_market_maker_max_skew() -> f64 {
    0.01
}

fn default_market_maker_volatility_window_ms() -> u64 {
    5 * 60 * 1000
}

fn default_market_maker_volatility_min_samples() -> usize {
    5
}

fn default_market_maker_volatility_threshold() -> f64 {
    0.02
}

fn default_market_maker_spread_cooldown_ms() -> u64 {
    60 * 1000
}

fn default_market_maker_volatility_cooldown_ms() -> u64 {
    5 * 60 * 1000
}

fn default_market_maker_fair_midpoint_cooldown_ms() -> u64 {
    10 * 60 * 1000
}

fn default_market_maker_fair_midpoint_min() -> f64 {
    0.15
}

fn default_market_maker_fair_midpoint_max() -> f64 {
    0.85
}

fn default_market_maker_abnormal_market_spread_multiplier() -> f64 {
    2.0
}

fn default_market_maker_normal_quote_levels() -> usize {
    3
}

fn default_market_maker_overweight_quote_levels() -> usize {
    2
}

fn default_market_maker_level_ratios() -> Vec<f64> {
    vec![0.4, 0.55, 0.7]
}

fn default_market_maker_level_sizes_usd() -> Vec<f64> {
    vec![50.0, 75.0, 100.0]
}

fn default_market_maker_rebalance_level_share_ratios() -> Vec<f64> {
    vec![0.3, 0.15, 0.05]
}

fn default_market_maker_rebalance_max_usd_per_level() -> Vec<f64> {
    vec![200.0, 150.0, 100.0]
}

fn default_market_maker_rebalance_max_usd_per_cycle() -> f64 {
    450.0
}

fn default_market_maker_reconcile_size_tolerance() -> f64 {
    0.2
}

fn default_dingtalk_timeout_secs() -> u64 {
    5
}

fn default_dingtalk_queue_size() -> usize {
    1024
}

pub(crate) fn load_app_config() -> anyhow::Result<AppConfig> {
    let config_path = if std::path::Path::new("config.toml").exists() {
        "config.toml".to_string()
    } else if let Ok(mut exe_path) = std::env::current_exe() {
        exe_path.pop();
        exe_path.push("config.toml");
        exe_path.to_string_lossy().to_string()
    } else {
        "config.toml".to_string()
    };

    let local_config_path = if std::path::Path::new("config.local.toml").exists() {
        "config.local.toml".to_string()
    } else if let Ok(mut exe_path) = std::env::current_exe() {
        exe_path.pop();
        exe_path.push("config.local.toml");
        exe_path.to_string_lossy().to_string()
    } else {
        "config.local.toml".to_string()
    };

    let settings = Config::builder()
        .add_source(File::with_name(&config_path).required(false))
        .add_source(File::with_name(&local_config_path).required(false))
        .build()?;
    Ok(settings.try_deserialize()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::FileFormat;

    #[test]
    fn app_config_does_not_require_account_section() {
        let toml = r#"
[proxy]
url = ""

[app]
log_file = ""
assets_file = ""
min_diff = 0.0
max_spread = 0.0
min_price = 0.0
max_price = 1.0
default_threads = 1

[auth]
api_key = ""
api_secret = ""
passphrase = ""
private_key = ""
funder = ""

[order]
size_usdc = 1.0
"#;

        let config: AppConfig = Config::builder()
            .add_source(config::File::from_str(toml, FileFormat::Toml))
            .build()
            .expect("minimal config source should build")
            .try_deserialize()
            .expect("minimal config should parse without account section");
        assert!(config.app.assets_file.is_empty());
    }

    #[test]
    fn app_config_uses_default_market_maker_config_when_section_missing() {
        let toml = r#"
[proxy]
url = ""

[app]
log_file = ""
assets_file = ""
min_diff = 0.0
max_spread = 0.0
min_price = 0.0
max_price = 1.0
default_threads = 1

[auth]
api_key = ""
api_secret = ""
passphrase = ""
private_key = ""
funder = ""

[order]
size_usdc = 1.0
"#;

        let config: AppConfig = Config::builder()
            .add_source(config::File::from_str(toml, FileFormat::Toml))
            .build()
            .expect("minimal config source should build")
            .try_deserialize()
            .expect("minimal config should parse without market maker section");

        assert!(config.market_maker.enabled);
        assert_eq!(config.market_maker.file, "market_maker.csv");
        assert_eq!(config.market_maker.max_inventory_usd, 100.0);
        assert_eq!(config.market_maker.overweight_ratio, 0.7);
        assert_eq!(config.market_maker.level_ratios, vec![0.4, 0.55, 0.7]);
        assert_eq!(config.market_maker.level_sizes_usd, vec![50.0, 75.0, 100.0]);
        assert_eq!(
            config.market_maker.rebalance_level_share_ratios,
            vec![0.3, 0.15, 0.05]
        );
        assert_eq!(
            config.market_maker.rebalance_max_usd_per_level,
            vec![200.0, 150.0, 100.0]
        );
        assert_eq!(config.market_maker.rebalance_max_usd_per_cycle, 450.0);
    }

    #[test]
    fn app_config_parses_market_maker_section() {
        let toml = r#"
[proxy]
url = ""

[app]
log_file = ""
assets_file = ""
min_diff = 0.0
max_spread = 0.0
min_price = 0.0
max_price = 1.0
default_threads = 1

[auth]
api_key = ""
api_secret = ""
passphrase = ""
private_key = ""
funder = ""

[order]
size_usdc = 1.0

[market_maker]
enabled = false
file = "custom_market_maker.csv"
max_inventory_usd = 250.0
overweight_ratio = 0.6
level_ratios = [0.25, 0.5]
level_sizes_usd = [20.0, 40.0]
rebalance_level_share_ratios = [0.2, 0.1]
rebalance_max_usd_per_level = [120.0, 80.0]
rebalance_max_usd_per_cycle = 180.0
reconcile_size_tolerance = 0.05
"#;

        let config: AppConfig = Config::builder()
            .add_source(config::File::from_str(toml, FileFormat::Toml))
            .build()
            .expect("config source should build")
            .try_deserialize()
            .expect("market maker config should parse");

        assert!(!config.market_maker.enabled);
        assert_eq!(config.market_maker.file, "custom_market_maker.csv");
        assert_eq!(config.market_maker.max_inventory_usd, 250.0);
        assert_eq!(config.market_maker.overweight_ratio, 0.6);
        assert_eq!(config.market_maker.level_ratios, vec![0.25, 0.5]);
        assert_eq!(config.market_maker.level_sizes_usd, vec![20.0, 40.0]);
        assert_eq!(
            config.market_maker.rebalance_level_share_ratios,
            vec![0.2, 0.1]
        );
        assert_eq!(
            config.market_maker.rebalance_max_usd_per_level,
            vec![120.0, 80.0]
        );
        assert_eq!(config.market_maker.rebalance_max_usd_per_cycle, 180.0);
        assert_eq!(config.market_maker.reconcile_size_tolerance, 0.05);
    }
}
