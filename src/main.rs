mod dispatcher;
mod logging;
mod market;
mod price_store;
mod proxy_ws;
mod strategies;
mod strategy;

use std::collections::HashMap;
use std::sync::Arc;
use serde::Deserialize;
use config::{Config, File};
use polymarket_client_sdk::types::Decimal;
use tracing::{info, warn};

use dispatcher::Dispatcher;
use strategies::mid_requote::MidRequoteStrategy;
use strategies::pair_arbitrage::PairArbitrageStrategy;
use strategy::{build_token_topics, merge_topic_tokens, Filters, OrderSignal, Strategy, StrategyHandle};

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
    assets_file: String,
    min_diff: f64,
    max_spread: f64,
    min_price: f64,
    max_price: f64,
    default_threads: usize,
}

#[derive(Debug, Deserialize, Clone)]
struct AuthConfig {
    api_key: String,
    api_secret: String,
    passphrase: String,
    private_key: String,
    funder: String,
}

#[derive(Debug, Deserialize, Clone)]
struct OrderConfig {
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
    // 1. 加载配置
    let config_path = if std::path::Path::new("config.toml").exists() {
        "config.toml".to_string()
    } else if let Ok(mut exe_path) = std::env::current_exe() {
        exe_path.pop();
        exe_path.push("config.toml");
        exe_path.to_string_lossy().to_string()
    } else {
        "config.toml".to_string()
    };

    let settings = Config::builder()
        .add_source(File::with_name(&config_path).required(false))
        .build()?;
    let app_config: AppConfig = settings.try_deserialize()?;

    // 2. 查找日志文件路径 (确保使用绝对路径)
    let log_filename = if !app_config.app.log_file.is_empty() {
        &app_config.app.log_file
    } else {
        "alerts.log"
    };

    let log_path_obj = std::path::Path::new(log_filename);
    let log_path = if log_path_obj.is_absolute() {
        log_filename.to_string()
    } else if let Ok(mut exe_path) = std::env::current_exe() {
        exe_path.pop();
        exe_path.push(log_filename);
        exe_path.to_string_lossy().to_string()
    } else {
        log_filename.to_string()
    };

    let _log_guards = logging::init_logging(&log_path)?;

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
    let (pair_strategy, pair_registration) =
        PairArbitrageStrategy::from_config(filters.clone(), assets_file)?;

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
    if let Some((_, registration)) = &mid_requote {
        registrations.push(registration.clone());
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
    let (market_tx, market_rx) = tokio::sync::mpsc::channel(1024);
    let (order_tx, mut order_rx) = tokio::sync::mpsc::channel::<OrderSignal>(64);

    let (pair_tx, pair_rx) = tokio::sync::mpsc::channel(256);
    let mut strategy_handles = vec![StrategyHandle {
        name: pair_registration.name.clone(),
        topics: pair_registration.topics.clone(),
        tx: pair_tx,
    }];

    pair_strategy.spawn(pair_rx, order_tx.clone());

    if let Some((mid_requote_strategy, mid_requote_registration)) = mid_requote {
        let (mid_requote_tx, mid_requote_rx) = tokio::sync::mpsc::channel(256);
        strategy_handles.push(StrategyHandle {
            name: mid_requote_registration.name.clone(),
            topics: mid_requote_registration.topics.clone(),
            tx: mid_requote_tx,
        });
        mid_requote_strategy.spawn(mid_requote_rx, order_tx.clone());
    }
    tokio::spawn(Dispatcher::new(strategy_handles).run(market_rx));
    tokio::spawn(market::run(token_topics.clone(), ws_rx, market_tx));

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

    {
        let auth = app_config.auth.clone();
        let order_cfg = app_config.order.clone();
        tokio::spawn(async move {
            while let Some(signal) = order_rx.recv().await {
                match &signal {
                    OrderSignal::PairArbitrage { .. } => {
                        if !order_cfg.enabled {
                            continue;
                        }
                        if let Err(e) = place_order(&auth, &order_cfg, &signal).await {
                            warn!(error = %e, "下单失败");
                        }
                    }
                    OrderSignal::MidRequote {
                        topic,
                        token,
                        mid,
                        buy_price,
                        sell_price,
                        cancelled_order_ids,
                        new_order_ids,
                    } => {
                        info!(
                            target: "alerts",
                            topic = %topic,
                            token = %token,
                            mid = %mid,
                            buy_price = %buy_price,
                            sell_price = %sell_price,
                            cancelled_orders = ?cancelled_order_ids,
                            new_orders = ?new_order_ids,
                            "mid_requote 模拟撤单并重挂"
                        );
                    }
                }
            }
        });
    }

    futures::future::pending::<()>().await;
    Ok(())
}

/// 执行套利下单：同时买入 token0 和 token1。
/// 认证信息从 AuthConfig 读取，下单数量从 OrderConfig 读取。
async fn place_order(
    auth: &AuthConfig,
    cfg: &OrderConfig,
    signal: &OrderSignal,
) -> anyhow::Result<()> {
    let OrderSignal::PairArbitrage {
        token0,
        token1,
        ask0,
        ask1,
        gap,
    } = signal else {
        anyhow::bail!("place_order 只支持 PairArbitrage 信号");
    };

    info!(
        gap = %gap,
        token0 = %&token0[..12],
        ask0 = %ask0,
        token1 = %&token1[..12],
        ask1 = %ask1,
        size_usdc = cfg.size_usdc,
        "提交下单信号"
    );

    // TODO: 调用 Polymarket CLOB REST API 下单
    // 认证字段：auth.api_key / auth.api_secret / auth.passphrase
    //           auth.private_key（用于 EIP-712 签名）/ auth.funder
    // 下单参数：token0 / token1 / ask0 / ask1
    //           cfg.size_usdc（总 USDC 金额，按价格折算成 token 数量）
    let _ = auth;

    Ok(())
}