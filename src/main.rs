mod dispatcher;
mod logging;
mod market;
mod order;
mod order_ws;
mod positions;
mod price_store;
mod proxy_ws;
mod strategies;
mod strategy;

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;

use config::{Config, File};
use polymarket_client_sdk::types::Decimal;
use serde::Deserialize;
use tracing::info;

use dispatcher::Dispatcher;
use positions::PositionRefreshTrigger;
use strategies::mid_requote::MidRequoteStrategy;
use strategies::pair_arbitrage::PairArbitrageStrategy;
use strategy::{
    build_token_topics, merge_topic_tokens, Filters, OrderCorrelationMap, OrderSignal, Strategy,
    StrategyHandle,
};

#[derive(Debug, Deserialize)]
struct AppConfig {
    proxy: ProxySettings,
    app: AppSettings,
    auth: AuthConfig,
    order: OrderConfig,
    #[serde(default)]
    mid_requote: MidRequoteConfig,
    #[serde(default)]
    topic_threads: HashMap<String, usize>,
}

#[derive(Debug, Deserialize)]
struct ProxySettings {
    url: String,
}

#[derive(Debug, Deserialize)]
struct AppSettings {
    log_file: String,
    order_log_file: String,
    assets_file: String,
    min_diff: f64,
    max_spread: f64,
    min_price: f64,
    max_price: f64,
    default_threads: usize,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct AuthConfig {
    api_key: String,
    api_secret: String,
    passphrase: String,
    private_key: String,
    funder: String,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct OrderConfig {
    enabled: bool,
    size_usdc: f64,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct MidRequoteConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    file: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
    let app_config: AppConfig = settings.try_deserialize()?;

    let log_filename = if !app_config.app.log_file.is_empty() {
        &app_config.app.log_file
    } else {
        "alerts.log"
    };
    let order_log_filename = if !app_config.app.order_log_file.is_empty() {
        &app_config.app.order_log_file
    } else {
        "orders.log"
    };

    let resolve_log_path = |filename: &str| {
        let path_obj = std::path::Path::new(filename);
        if path_obj.is_absolute() {
            filename.to_string()
        } else if let Ok(mut exe_path) = std::env::current_exe() {
            exe_path.pop();
            exe_path.push(filename);
            exe_path.to_string_lossy().to_string()
        } else {
            filename.to_string()
        }
    };

    let log_path = resolve_log_path(log_filename);
    let order_log_path = resolve_log_path(order_log_filename);

    let _log_guards = logging::init_logging(&log_path, &order_log_path)?;

    let dec = |v: f64| Decimal::try_from(v).unwrap_or_default();
    let filters = Arc::new(Filters {
        min_diff: dec(app_config.app.min_diff),
        max_spread: dec(app_config.app.max_spread),
        min_price: dec(app_config.app.min_price),
        max_price: dec(app_config.app.max_price),
    });

    let assets_file = if !app_config.app.assets_file.is_empty() {
        app_config.app.assets_file.as_str()
    } else {
        "assets.csv"
    };
    let pair_strategy = PairArbitrageStrategy::from_config(filters.clone(), assets_file)?;
    let pair_registration = pair_strategy.registration().clone();

    let mid_file = if !app_config.mid_requote.file.is_empty() {
        app_config.mid_requote.file.as_str()
    } else {
        "mid.csv"
    };
    let mid_requote = if app_config.mid_requote.enabled {
        MidRequoteStrategy::from_csv(mid_file)?
    } else {
        None
    };

    let mut registrations = vec![pair_registration.clone()];
    if let Some(strategy) = &mid_requote {
        registrations.push(strategy.registration().clone());
    }

    let topic_tokens = Arc::new(merge_topic_tokens(&registrations));
    let token_topics = Arc::new(build_token_topics(topic_tokens.as_ref()));
    let token_count = topic_tokens
        .values()
        .flatten()
        .collect::<std::collections::HashSet<_>>()
        .len();

    info!("正在连接 Polymarket WebSocket...");
    info!(token_count, "开始监听 token 价格变动");

    let proxy = if !app_config.proxy.url.is_empty() {
        proxy_ws::Proxy::from_raw(&app_config.proxy.url)
    } else {
        proxy_ws::Proxy::from_env()
    };
    let default_threads = app_config.app.default_threads.max(1);

    let (ws_tx, ws_rx) = tokio::sync::mpsc::channel(256 * topic_tokens.len().max(1));
    let (strategy_tx, strategy_rx) = tokio::sync::mpsc::channel(1024);
    let (order_tx, order_rx) = tokio::sync::mpsc::channel::<OrderSignal>(64);
    let (positions_refresh_tx, positions_refresh_rx) = tokio::sync::mpsc::channel(64);
    let order_correlations: OrderCorrelationMap = Arc::new(DashMap::new());

    let (pair_tx, pair_rx) = tokio::sync::mpsc::channel(256);
    let mut strategy_handles = vec![StrategyHandle {
        name: pair_registration.name.clone(),
        topics: pair_registration.topics.clone(),
        related_tokens: pair_registration.related_tokens.clone(),
        tx: pair_tx,
    }];

    pair_strategy.spawn(pair_rx, order_tx.clone());

    if let Some(mid_requote_strategy) = mid_requote {
        let mid_requote_registration = mid_requote_strategy.registration().clone();
        let (mid_requote_tx, mid_requote_rx) = tokio::sync::mpsc::channel(256);
        strategy_handles.push(StrategyHandle {
            name: mid_requote_registration.name.clone(),
            topics: mid_requote_registration.topics.clone(),
            related_tokens: mid_requote_registration.related_tokens.clone(),
            tx: mid_requote_tx,
        });
        mid_requote_strategy.spawn(mid_requote_rx, order_tx.clone());
    }
    tokio::spawn(Dispatcher::new(strategy_handles).run(strategy_rx));
    tokio::spawn(market::run(token_topics.clone(), ws_rx, strategy_tx.clone()));
    tokio::spawn(positions::run(
        app_config.auth.clone(),
        positions_refresh_rx,
        strategy_tx.clone(),
    ));

    {
        let topic_tokens = topic_tokens.clone();
        let topic_threads = app_config.topic_threads.clone();
        tokio::spawn(async move {
            market::spawn_subscriptions(
                topic_tokens.as_ref(),
                &topic_threads,
                default_threads,
                proxy,
                ws_tx,
            )
            .await;
        });
    }

    tokio::spawn(order::run(
        order_rx,
        app_config.auth.clone(),
        app_config.order.clone(),
        order_correlations.clone(),
        positions_refresh_tx.clone(),
    ));

    let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::Startup);

    if app_config.order.enabled {
        tokio::spawn(order_ws::run(
            app_config.auth.clone(),
            order_correlations.clone(),
            positions_refresh_tx.clone(),
        ));
    }

    futures::future::pending::<()>().await;
    Ok(())
}
