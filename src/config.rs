use std::collections::HashMap;

use ::config::{Config, File};
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
    pub(crate) topic_threads: HashMap<String, usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProxySettings {
    pub(crate) url: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AppSettings {
    pub(crate) log_file: String,
    pub(crate) order_log_file: String,
    pub(crate) assets_file: String,
    #[serde(default)]
    pub(crate) sqlite_path: String,
    pub(crate) min_diff: f64,
    pub(crate) max_spread: f64,
    pub(crate) min_price: f64,
    pub(crate) max_price: f64,
    pub(crate) default_threads: usize,
    #[serde(default = "default_monitor_interval_secs")]
    pub(crate) monitor_interval_secs: u64,
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

#[derive(Debug, Deserialize, Clone, Default)]
pub(crate) struct LiquidityRewardConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) file: String,
    #[serde(default)]
    pub(crate) monitor_enabled: bool,
    #[serde(default)]
    pub(crate) simulation: bool,
    #[serde(default = "default_true")]
    pub(crate) reward_estimator_enabled: bool,
}

fn default_true() -> bool {
    true
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

fn default_monitor_interval_secs() -> u64 {
    30
}
