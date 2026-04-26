mod clob_client;
mod config;
mod dispatcher;
mod logging;
mod market;
mod monitor;
mod order;
mod order_ws;
mod polymarket_rewards;
mod positions;
mod price_store;
mod proxy_ws;
mod recovery;
mod storage;
mod strategies;
mod strategy;

use std::sync::Arc;

use polymarket_client_sdk::types::Decimal;
use tracing::info;

use config::load_app_config;
use dispatcher::Dispatcher;
use monitor::RewardMonitorConfig;
use positions::{PositionRefreshTrigger, SimulatedFillEvent};
use recovery::RecoveryCoordinator;
use storage::OrderStore;
use strategies::liquidity_reward::LiquidityRewardStrategy;
use strategies::pair_arbitrage::PairArbitrageStrategy;
use strategy::{
    Filters, OrderSignal, Strategy, StrategyHandle, build_token_topics, merge_topic_tokens,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app_config = load_app_config()?;

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

    let resolve_path = |filename: &str| {
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

    let log_path = resolve_path(log_filename);
    let order_log_path = resolve_path(order_log_filename);

    let _log_guards = logging::init_logging(&log_path, &order_log_path)?;

    let sqlite_filename = if !app_config.app.sqlite_path.is_empty() {
        app_config.app.sqlite_path.as_str()
    } else {
        "orders.db"
    };
    let order_store = OrderStore::open(&resolve_path(sqlite_filename))?;
    order_store.init_schema()?;

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

    let recovery = RecoveryCoordinator::new(
        order_store.clone(),
        app_config.auth.clone(),
        app_config.simulation.enabled,
    )
    .recover()
    .await?;
    let order_correlations = recovery.order_correlations;
    let restored_liquidity_reward_states = recovery.liquidity_reward_restore_states;
    let liquidity_reward_file = if !app_config.liquidity_reward.file.is_empty() {
        app_config.liquidity_reward.file.as_str()
    } else {
        "liquidity_reward.csv"
    };
    let liquidity_reward_strategy_opt = if app_config.liquidity_reward.enabled {
        LiquidityRewardStrategy::from_csv(liquidity_reward_file)?
    } else {
        None
    };

    let lr_simulation = app_config.liquidity_reward.simulation;
    let reward_monitor_configs: std::collections::HashMap<String, RewardMonitorConfig> =
        liquidity_reward_strategy_opt
            .as_ref()
            .map(|s| {
                s.rules()
                    .filter_map(|(token, rule)| {
                        let pool = rule.reward_daily_pool?;
                        let spread = rule.reward_max_spread_cents?;
                        let is_yes = token == &rule.token1;
                        let paired_token = if is_yes {
                            rule.token2.clone()
                        } else {
                            Some(rule.token1.clone())
                        };
                        Some((
                            token.clone(),
                            RewardMonitorConfig {
                                min_orders: rule.reward_min_orders.unwrap_or(0),
                                max_spread_cents: spread,
                                min_size: rule.reward_min_size.unwrap_or(0.0),
                                daily_reward_pool: pool,
                                simulation: lr_simulation,
                                paired_token,
                                is_yes_token: is_yes,
                            },
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();

    let liquidity_reward = liquidity_reward_strategy_opt.map(|strategy| {
        strategy.with_restore_state(
            restored_liquidity_reward_states.clone(),
            Some(order_store.clone()),
            lr_simulation,
        )
    });

    let liquidity_reward_tokens: Arc<std::collections::HashSet<String>> = Arc::new(
        liquidity_reward
            .as_ref()
            .map(|strategy| {
                strategy
                    .registration()
                    .related_tokens
                    .iter()
                    .cloned()
                    .collect::<std::collections::HashSet<_>>()
            })
            .unwrap_or_default(),
    );

    if app_config.liquidity_reward.enabled
        && app_config.liquidity_reward.monitor_enabled
        && !app_config.simulation.enabled
    {
        tokio::spawn(monitor::run_liquidity_reward_monitor(
            app_config.auth.clone(),
            app_config.app.monitor_interval_secs.max(1),
        ));
    }

    let mut registrations = vec![pair_registration.clone()];
    if let Some(strategy) = &liquidity_reward {
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
    let monitor_tx = if reward_monitor_configs.is_empty() {
        None
    } else {
        let (tx, rx) = tokio::sync::mpsc::channel(512);
        tokio::spawn(monitor::run_reward_estimator(
            rx,
            reward_monitor_configs,
            order_correlations.clone(),
            order_store.clone(),
        ));
        Some(tx)
    };
    let (positions_refresh_tx, positions_refresh_rx) = tokio::sync::mpsc::channel(64);
    let (sim_fill_tx, sim_fill_rx) = tokio::sync::mpsc::channel::<SimulatedFillEvent>(64);

    let (pair_tx, pair_rx) = tokio::sync::mpsc::channel(256);
    let mut strategy_handles = vec![StrategyHandle {
        name: pair_registration.name.clone(),
        topics: pair_registration.topics.clone(),
        related_tokens: pair_registration.related_tokens.clone(),
        tx: pair_tx,
    }];

    pair_strategy.spawn(pair_rx, order_tx.clone());

    if let Some(liquidity_reward_strategy) = liquidity_reward {
        let liquidity_reward_registration = liquidity_reward_strategy.registration().clone();
        let (liquidity_reward_tx, liquidity_reward_rx) = tokio::sync::mpsc::channel(256);
        strategy_handles.push(StrategyHandle {
            name: liquidity_reward_registration.name.clone(),
            topics: liquidity_reward_registration.topics.clone(),
            related_tokens: liquidity_reward_registration.related_tokens.clone(),
            tx: liquidity_reward_tx,
        });
        liquidity_reward_strategy.spawn(liquidity_reward_rx, order_tx.clone());
    }
    tokio::spawn(Dispatcher::new(strategy_handles).run(strategy_rx));
    tokio::spawn(market::run(
        token_topics.clone(),
        liquidity_reward_tokens.clone(),
        ws_rx,
        strategy_tx.clone(),
        monitor_tx,
    ));
    if app_config.simulation.enabled {
        tokio::spawn(positions::run_simulated(sim_fill_rx, strategy_tx.clone()));
    } else {
        tokio::spawn(positions::run(
            app_config.auth.clone(),
            positions_refresh_rx,
            strategy_tx.clone(),
        ));
    }

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
        app_config.simulation.enabled,
        order_correlations.clone(),
        order_store.clone(),
        positions_refresh_tx.clone(),
        sim_fill_tx,
        strategy_tx.clone(),
    ));

    if !app_config.simulation.enabled {
        let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::Startup);
    }

    if app_config.order.enabled && !app_config.simulation.enabled {
        tokio::spawn(order_ws::run(
            app_config.auth.clone(),
            order_correlations.clone(),
            order_store.clone(),
            positions_refresh_tx.clone(),
            strategy_tx.clone(),
        ));
    }

    futures::future::pending::<()>().await;
    Ok(())
}
