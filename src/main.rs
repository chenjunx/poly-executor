mod account;
mod clob_client;
mod config;
mod dispatcher;
mod logging;
mod market;
mod monitor;
mod notification;
mod order;
mod order_ws;
mod polymarket_rewards;
mod positions;
mod price_store;
mod proxy_ws;
mod recovery;
mod reward_market_cache;
mod reward_market_pool_monitor;
mod storage;
mod strategies;
mod strategy;
mod tick_size;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use notification::Notifier;

use polymarket_client_sdk_v2::types::Decimal;
use tracing::info;

use config::{AccountConfig, AppConfig, load_app_config};
use dispatcher::Dispatcher;
use monitor::{FullBookSnapshot, RewardMonitorConfig};
use positions::{PositionRefreshTrigger, SimulatedFillEvent};
use recovery::{RecoveryArtifacts, RecoveryCoordinator};
use storage::{MarketStore, OrderStore};
use strategies::liquidity_reward::{LiquidityRewardRestoreState, LiquidityRewardRule};
use strategies::liquidity_reward_fsm::LiquidityRewardFsmStrategy as LiquidityRewardStrategy;
use strategies::market_maker::MarketMakerStrategy;
use strategies::pair_arbitrage::PairArbitrageStrategy;
use strategy::{
    CleanOrderbook, Filters, OrderCorrelationMap, OrderSignal, Strategy, StrategyEvent,
    StrategyHandle, StrategyRegistration, build_token_topics, merge_topic_tokens,
};

type WsMessage = polymarket_client_sdk_v2::clob::ws::types::response::WsMessage;
type TopicTokens = Arc<HashMap<Arc<str>, Vec<String>>>;
type TokenTopics = Arc<HashMap<String, Arc<[Arc<str>]>>>;
type WsSender = tokio::sync::mpsc::Sender<WsMessage>;
type WsReceiver = tokio::sync::mpsc::Receiver<WsMessage>;
type StrategySender = tokio::sync::mpsc::Sender<StrategyEvent>;
type StrategyReceiver = tokio::sync::mpsc::Receiver<StrategyEvent>;
type OrderSender = tokio::sync::mpsc::Sender<OrderSignal>;
type OrderReceiver = tokio::sync::mpsc::Receiver<OrderSignal>;
type MonitorSender = tokio::sync::mpsc::Sender<FullBookSnapshot>;
type PositionRefreshSender = tokio::sync::mpsc::Sender<PositionRefreshTrigger>;
type PositionRefreshReceiver = tokio::sync::mpsc::Receiver<PositionRefreshTrigger>;
type SimFillSender = tokio::sync::mpsc::Sender<SimulatedFillEvent>;
type SimFillReceiver = tokio::sync::mpsc::Receiver<SimulatedFillEvent>;
type TickRecorderSender = tokio::sync::mpsc::Sender<(Arc<str>, CleanOrderbook)>;
type RawRecorderSender = tokio::sync::mpsc::Sender<market::RawStoreEvent>;

struct StrategyBootstrap {
    pair_strategy: PairArbitrageStrategy,
    pair_registration: StrategyRegistration,
    liquidity_reward: Option<LiquidityRewardStrategy>,
    market_maker: Option<MarketMakerStrategy>,
    liquidity_reward_tokens: Arc<HashSet<String>>,
    reward_monitor_configs: HashMap<String, RewardMonitorConfig>,
    tick_size_map: tick_size::TickSizeMap,
}

struct RoutingBootstrap {
    topic_tokens: TopicTokens,
    token_topics: TokenTopics,
    token_count: usize,
}

struct RecorderSenders {
    tick_tx: Option<TickRecorderSender>,
    raw_store_tx: Option<RawRecorderSender>,
}

struct MarketRuntime {
    topic_tokens: TopicTokens,
    token_topics: TokenTopics,
    liquidity_reward_tokens: Arc<HashSet<String>>,
    ws_tx: WsSender,
    ws_rx: WsReceiver,
    strategy_tx: StrategySender,
    monitor_tx: Option<MonitorSender>,
    recorder_senders: RecorderSenders,
    tick_size_map: tick_size::TickSizeMap,
    positions_refresh_rx: PositionRefreshReceiver,
    sim_fill_rx: SimFillReceiver,
    proxy: Option<proxy_ws::Proxy>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    // 读取 config.toml + config.local.toml，后续所有路径和开关都以这里为准。
    let app_config = load_app_config()?;
    let log_path = init_log_path(&app_config);
    let _log_guards = logging::init_logging(&log_path)?;
    let (order_store, market_store) = init_stores(&app_config)?;

    let recovery = recover_orders(&app_config, order_store.clone()).await?;
    let order_correlations = recovery.order_correlations;
    let notifier = notification::spawn_dingtalk_notifier(app_config.notification.dingtalk.clone());
    spawn_account_monitor(&app_config, market_store.clone());
    let strategy_bootstrap = build_strategies(
        &app_config,
        order_store.clone(),
        market_store.clone(),
        recovery.liquidity_reward_restore_states,
        notifier.clone(),
    )
    .await?;
    let StrategyBootstrap {
        pair_strategy,
        pair_registration,
        liquidity_reward,
        market_maker,
        liquidity_reward_tokens,
        reward_monitor_configs,
        tick_size_map,
    } = strategy_bootstrap;

    let proxy = init_proxy(&app_config);
    let registrations = build_strategy_registrations(
        &pair_registration,
        liquidity_reward.as_ref(),
        market_maker.as_ref(),
    );
    let routing = build_routing(&registrations);

    info!("正在连接 Polymarket WebSocket...");
    info!(routing.token_count, "开始监听 token 价格变动");

    // ws_tx 承载公开行情；strategy_tx 承载行情、持仓、成交和奖励池事件。
    let (ws_tx, ws_rx) = tokio::sync::mpsc::channel(256 * routing.topic_tokens.len().max(1));
    let (strategy_tx, strategy_rx) = tokio::sync::mpsc::channel(1024);
    spawn_reward_market_tasks(
        &app_config,
        market_store.clone(),
        liquidity_reward_tokens.clone(),
        proxy.clone(),
        strategy_tx.clone(),
        notifier.clone(),
    )?;

    // order_tx 是所有策略向订单执行器提交下单/撤单信号的唯一入口。
    let (order_tx, order_rx) = tokio::sync::mpsc::channel::<OrderSignal>(64);
    let monitor_tx = spawn_reward_estimator(
        &app_config,
        reward_monitor_configs,
        order_correlations.clone(),
        market_store.clone(),
    );
    let (positions_refresh_tx, positions_refresh_rx) = tokio::sync::mpsc::channel(64);
    let (sim_fill_tx, sim_fill_rx) = tokio::sync::mpsc::channel::<SimulatedFillEvent>(64);

    spawn_strategy_tasks(
        pair_strategy,
        pair_registration,
        liquidity_reward,
        market_maker,
        order_tx.clone(),
        strategy_rx,
    );
    let recorder_senders = spawn_recorders(&app_config, market_store.clone());
    spawn_market_and_positions(
        &app_config,
        MarketRuntime {
            topic_tokens: routing.topic_tokens,
            token_topics: routing.token_topics,
            liquidity_reward_tokens,
            ws_tx,
            ws_rx,
            strategy_tx: strategy_tx.clone(),
            monitor_tx,
            recorder_senders,
            tick_size_map,
            positions_refresh_rx,
            sim_fill_rx,
            proxy,
        },
    );
    spawn_order_tasks(
        &app_config,
        order_store,
        order_correlations,
        order_rx,
        positions_refresh_tx,
        sim_fill_tx,
        strategy_tx,
        notifier,
    );

    futures::future::pending::<()>().await;
    Ok(())
}

async fn recover_orders(
    app_config: &AppConfig,
    order_store: OrderStore,
) -> anyhow::Result<RecoveryArtifacts> {
    // 启动恢复先对账本地 active 订单和远端状态，再把可恢复状态交给策略。
    RecoveryCoordinator::new(
        order_store,
        app_config.auth.clone(),
        app_config.simulation.enabled,
    )
    .recover()
    .await
    .map_err(|e| anyhow::anyhow!("启动恢复订单失败: {e:#}"))
}

async fn build_strategies(
    app_config: &AppConfig,
    order_store: OrderStore,
    market_store: MarketStore,
    restored_liquidity_reward_states: HashMap<String, LiquidityRewardRestoreState>,
    notifier: Option<Notifier>,
) -> anyhow::Result<StrategyBootstrap> {
    let pair_strategy = build_pair_strategy(app_config)?;
    let pair_registration = pair_strategy.registration().clone();
    let liquidity_reward_strategy_opt = build_liquidity_reward_strategy(app_config, &market_store)?;
    let market_maker = build_market_maker_strategy(&market_store)?;
    let reward_monitor_configs = build_reward_monitor_configs(
        liquidity_reward_strategy_opt.as_ref(),
        app_config.liquidity_reward.simulation,
    );
    let tick_size_map = tick_size::new_tick_size_map();
    // unwind 卖单需要按 token tick size 规整价格。
    let lr_tokens = liquidity_reward_strategy_opt
        .as_ref()
        .map(|s| s.registration().related_tokens.to_vec())
        .unwrap_or_default();
    tick_size::load_for_tokens(&lr_tokens, &app_config.auth, &tick_size_map).await;

    // 注入恢复状态、订单库和行情库后，策略才能在 halt 时撤单、unwind 并持久化 pool halt。
    let liquidity_reward = liquidity_reward_strategy_opt.map(|strategy| {
        strategy
            .with_restore_state(
                restored_liquidity_reward_states,
                Some(order_store),
                app_config.liquidity_reward.simulation,
                tick_size_map.clone(),
            )
            .with_balance_cooldown(std::time::Duration::from_secs(
                app_config.liquidity_reward.balance_cooldown_secs,
            ))
            .with_market_store(market_store)
            .with_notifier(notifier)
    });

    let liquidity_reward_tokens = Arc::new(
        liquidity_reward
            .as_ref()
            .map(|strategy| {
                strategy
                    .registration()
                    .related_tokens
                    .iter()
                    .cloned()
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default(),
    );

    Ok(StrategyBootstrap {
        pair_strategy,
        pair_registration,
        liquidity_reward,
        market_maker,
        liquidity_reward_tokens,
        reward_monitor_configs,
        tick_size_map,
    })
}

fn build_pair_strategy(app_config: &AppConfig) -> anyhow::Result<PairArbitrageStrategy> {
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
    PairArbitrageStrategy::from_config(filters, assets_file)
}

fn build_market_maker_strategy(
    market_store: &MarketStore,
) -> anyhow::Result<Option<MarketMakerStrategy>> {
    let entries = market_store.load_liquidity_reward_pool_entries()?;
    MarketMakerStrategy::from_pool_entries(entries)
}

fn liquidity_reward_runtime_enabled(_app_config: &AppConfig) -> bool {
    false
}

fn build_liquidity_reward_strategy(
    app_config: &AppConfig,
    market_store: &MarketStore,
) -> anyhow::Result<Option<LiquidityRewardStrategy>> {
    if !liquidity_reward_runtime_enabled(app_config) {
        return Ok(None);
    }

    let liquidity_reward_file = if !app_config.liquidity_reward.file.is_empty() {
        app_config.liquidity_reward.file.as_str()
    } else {
        "liquidity_reward.csv"
    };
    // LiquidityReward 可从 CSV 固定规则或 market.db 的 selected pool 构建。
    match app_config.liquidity_reward.source.as_str() {
        "csv" | "" => LiquidityRewardStrategy::from_csv(liquidity_reward_file),
        "db_pool" => {
            let entries = market_store.load_liquidity_reward_pool_entries()?;
            LiquidityRewardStrategy::from_pool_entries(entries)
        }
        other => Err(anyhow::anyhow!("未知 liquidity_reward.source: {other}")),
    }
}

fn build_reward_monitor_configs(
    liquidity_reward_strategy: Option<&LiquidityRewardStrategy>,
    lr_simulation: bool,
) -> HashMap<String, RewardMonitorConfig> {
    liquidity_reward_strategy
        .map(|strategy| {
            strategy
                .rules()
                .filter_map(|(token, rule)| reward_monitor_config(token, rule, lr_simulation))
                .collect()
        })
        .unwrap_or_default()
}

fn reward_monitor_config(
    token: &str,
    rule: &LiquidityRewardRule,
    lr_simulation: bool,
) -> Option<(String, RewardMonitorConfig)> {
    let pool = rule.reward_daily_pool?;
    let spread = rule.reward_max_spread_cents?;
    let is_yes = token == rule.token1;
    let paired_token = if is_yes {
        rule.token2.clone()
    } else {
        Some(rule.token1.clone())
    };
    Some((
        token.to_string(),
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
}

fn init_proxy(app_config: &AppConfig) -> Option<proxy_ws::Proxy> {
    if !app_config.proxy.url.is_empty() {
        proxy_ws::Proxy::from_raw(&app_config.proxy.url)
    } else {
        proxy_ws::Proxy::from_env()
    }
}

fn build_strategy_registrations(
    pair_registration: &StrategyRegistration,
    liquidity_reward: Option<&LiquidityRewardStrategy>,
    market_maker: Option<&MarketMakerStrategy>,
) -> Vec<StrategyRegistration> {
    let mut registrations = vec![pair_registration.clone()];
    if let Some(strategy) = liquidity_reward {
        registrations.push(strategy.registration().clone());
    }
    if let Some(strategy) = market_maker {
        registrations.push(strategy.registration().clone());
    }
    registrations
}

fn build_routing(registrations: &[StrategyRegistration]) -> RoutingBootstrap {
    // 合并所有策略订阅需求，按 topic 分配公开行情 WebSocket 订阅。
    let topic_tokens = Arc::new(merge_topic_tokens(registrations));
    let token_topics = Arc::new(build_token_topics(topic_tokens.as_ref()));
    let token_count = topic_tokens
        .values()
        .flatten()
        .collect::<std::collections::HashSet<_>>()
        .len();

    RoutingBootstrap {
        topic_tokens,
        token_topics,
        token_count,
    }
}

fn account_monitor_enabled(config: &AccountConfig) -> bool {
    config.enabled
}

fn spawn_account_monitor(app_config: &AppConfig, market_store: MarketStore) {
    if !account_monitor_enabled(&app_config.account) {
        return;
    }

    tokio::spawn(account::run(
        app_config.auth.clone(),
        app_config.account.clone(),
        market_store,
    ));
}

fn spawn_reward_market_tasks(
    app_config: &AppConfig,
    market_store: MarketStore,
    liquidity_reward_tokens: Arc<HashSet<String>>,
    proxy: Option<proxy_ws::Proxy>,
    strategy_tx: StrategySender,
    notifier: Option<Notifier>,
) -> anyhow::Result<()> {
    let reward_market_tasks_enabled = liquidity_reward_runtime_enabled(app_config)
        && app_config.liquidity_reward.monitor_enabled
        && !app_config.simulation.enabled;
    if !reward_market_tasks_enabled {
        return Ok(());
    }

    let pool_max_rewards_min_size = app_config
        .liquidity_reward
        .pool_max_rewards_min_size
        .map(|value| {
            if !value.is_finite() || value <= 0.0 {
                anyhow::bail!("liquidity_reward.pool_max_rewards_min_size 必须是正数");
            }
            Decimal::try_from(value).map_err(Into::into)
        })
        .transpose()?;

    // 三个后台任务分别负责奖励估算监控、每日池构建、池内市场健康检查。
    tokio::spawn(monitor::run_liquidity_reward_monitor(
        app_config.auth.clone(),
        app_config.app.monitor_interval_secs.max(1),
        liquidity_reward_tokens,
    ));
    let reward_market_monitor_interval =
        std::time::Duration::from_secs(app_config.app.monitor_interval_secs.max(60));
    tokio::spawn(reward_market_cache::run_reward_market_loader(
        app_config.auth.clone(),
        market_store.clone(),
        reward_market_monitor_interval,
        app_config.liquidity_reward.pool_market_count,
        pool_max_rewards_min_size,
        strategy_tx.clone(),
    ));
    tokio::spawn(reward_market_pool_monitor::run_reward_market_pool_monitor(
        market_store,
        proxy,
        reward_market_pool_monitor::RewardMarketPoolMonitorConfig {
            refresh_interval: reward_market_monitor_interval,
            token1_spread_threshold: 0.1,
            token1_min_bid: 0.1,
            token1_max_bid: 0.9,
            max_tokens_per_connection: 200,
            strategy_tx,
            notifier,
        },
    ));

    Ok(())
}

fn spawn_reward_estimator(
    app_config: &AppConfig,
    reward_monitor_configs: HashMap<String, RewardMonitorConfig>,
    order_correlations: OrderCorrelationMap,
    market_store: MarketStore,
) -> Option<MonitorSender> {
    if reward_monitor_configs.is_empty() || !app_config.liquidity_reward.reward_estimator_enabled {
        return None;
    }

    let (tx, rx) = tokio::sync::mpsc::channel(512);
    tokio::spawn(monitor::run_reward_estimator(
        rx,
        reward_monitor_configs,
        order_correlations,
        market_store,
    ));
    Some(tx)
}

fn market_maker_strategy_handle(
    market_maker: &MarketMakerStrategy,
) -> Option<(StrategyHandle, tokio::sync::mpsc::Receiver<StrategyEvent>)> {
    let registration = market_maker.registration().clone();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    Some((
        StrategyHandle {
            name: registration.name.clone(),
            topics: registration.topics.clone(),
            related_tokens: registration.related_tokens.clone(),
            tx,
        },
        rx,
    ))
}

fn spawn_strategy_tasks(
    pair_strategy: PairArbitrageStrategy,
    pair_registration: StrategyRegistration,
    liquidity_reward: Option<LiquidityRewardStrategy>,
    market_maker: Option<MarketMakerStrategy>,
    order_tx: OrderSender,
    strategy_rx: StrategyReceiver,
) {
    let (pair_tx, pair_rx) = tokio::sync::mpsc::channel(256);
    let mut strategy_handles = vec![StrategyHandle {
        name: pair_registration.name.clone(),
        topics: pair_registration.topics.clone(),
        related_tokens: pair_registration.related_tokens.clone(),
        tx: pair_tx,
    }];

    pair_strategy.spawn(pair_rx, order_tx.clone());

    // StrategyHandle 只保存路由元信息；实际策略任务各自消费自己的事件队列。
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

    if let Some(market_maker_strategy) = market_maker {
        if let Some((handle, market_maker_rx)) =
            market_maker_strategy_handle(&market_maker_strategy)
        {
            strategy_handles.push(handle);
            market_maker_strategy.spawn(market_maker_rx, order_tx.clone());
        }
    }
    tokio::spawn(Dispatcher::new(strategy_handles).run(strategy_rx));
}

fn spawn_recorders(app_config: &AppConfig, market_store: MarketStore) -> RecorderSenders {
    let tick_tx = if app_config.app.tick_store_enabled {
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        tokio::spawn(market::run_tick_recorder(rx, market_store.clone()));
        Some(tx)
    } else {
        None
    };

    let raw_store_tx = if app_config.app.raw_store_enabled {
        let (tx, rx) = tokio::sync::mpsc::channel(4096);
        tokio::spawn(market::run_raw_recorder(rx, market_store));
        Some(tx)
    } else {
        None
    };

    RecorderSenders {
        tick_tx,
        raw_store_tx,
    }
}

fn spawn_market_and_positions(app_config: &AppConfig, runtime: MarketRuntime) {
    let MarketRuntime {
        topic_tokens,
        token_topics,
        liquidity_reward_tokens,
        ws_tx,
        ws_rx,
        strategy_tx,
        monitor_tx,
        recorder_senders,
        tick_size_map,
        positions_refresh_rx,
        sim_fill_rx,
        proxy,
    } = runtime;
    let default_threads = app_config.app.default_threads.max(1);

    // market::run 汇总公开行情并按 token/topic 分发给策略、奖励估算和持久化 recorder。
    tokio::spawn(market::run(
        token_topics,
        liquidity_reward_tokens,
        ws_rx,
        strategy_tx.clone(),
        monitor_tx,
        recorder_senders.tick_tx,
        recorder_senders.raw_store_tx,
        tick_size_map,
    ));

    if app_config.simulation.enabled {
        // 模拟模式用本地成交事件维护仓位；真实模式定期拉取远端持仓。
        tokio::spawn(positions::run_simulated(sim_fill_rx, strategy_tx.clone()));
        keep_channel_open(positions_refresh_rx);
    } else {
        tokio::spawn(positions::run(
            app_config.auth.clone(),
            positions_refresh_rx,
            strategy_tx,
        ));
        keep_channel_open(sim_fill_rx);
    }

    // 公开行情订阅单独启动，断线重连和分线程订阅由 market 模块处理。
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

fn keep_channel_open<T: Send + 'static>(receiver: tokio::sync::mpsc::Receiver<T>) {
    tokio::spawn(async move {
        let _receiver = receiver;
        futures::future::pending::<()>().await;
    });
}

fn spawn_order_tasks(
    app_config: &AppConfig,
    order_store: OrderStore,
    order_correlations: OrderCorrelationMap,
    order_rx: OrderReceiver,
    positions_refresh_tx: PositionRefreshSender,
    sim_fill_tx: SimFillSender,
    strategy_tx: StrategySender,
    notifier: Option<Notifier>,
) {
    // 订单执行器消费 OrderSignal，负责真实 CLOB 下单/撤单或模拟成交。
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
        // 启动后立即刷新一次真实持仓，让策略尽早获得可用于 unwind 的仓位快照。
        let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::Startup);
        // 私有订单 WS 回写订单状态和成交增量，是真实成交通知与 halt 的主要触发源。
        tokio::spawn(order_ws::run(
            app_config.auth.clone(),
            order_correlations,
            order_store,
            positions_refresh_tx,
            strategy_tx,
            notifier,
        ));
    }
}

fn init_log_path(app_config: &AppConfig) -> String {
    let log_filename = if !app_config.app.log_file.is_empty() {
        app_config.app.log_file.as_str()
    } else {
        "alerts.log"
    };
    resolve_path(log_filename)
}

fn init_stores(app_config: &AppConfig) -> anyhow::Result<(OrderStore, MarketStore)> {
    let sqlite_filename = if !app_config.app.sqlite_path.is_empty() {
        app_config.app.sqlite_path.as_str()
    } else {
        "orders.db"
    };
    let sqlite_path = resolve_path(sqlite_filename);
    // orders.db 保存真实订单、订单事件和策略恢复状态。
    let order_store = OrderStore::open(&sqlite_path)?;
    order_store.init_schema()?;

    let market_sqlite_path = if !app_config.app.market_sqlite_path.is_empty() {
        resolve_path(&app_config.app.market_sqlite_path)
    } else {
        derive_market_sqlite_path(&sqlite_path)
    };
    // market.db 保存行情、奖励估算和每日奖励市场池状态。
    let market_store = MarketStore::open(&market_sqlite_path)?;
    market_store.init_schema()?;

    Ok((order_store, market_store))
}

// 相对路径按可执行文件目录解析，避免服务方式启动时落到意外工作目录。
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AppSettings, AuthConfig, LiquidityRewardConfig, NotificationConfig, OrderConfig,
        ProxySettings, SimulationConfig,
    };
    use crate::storage::ActiveRewardMarketPoolEntry;
    use crate::strategy::TopicRegistration;

    fn active_pool_entry(
        condition_id: &str,
        token1: &str,
        token2: &str,
    ) -> ActiveRewardMarketPoolEntry {
        ActiveRewardMarketPoolEntry {
            condition_id: condition_id.to_string(),
            market_slug: Some(format!("slug-{condition_id}")),
            question: None,
            token1: token1.to_string(),
            token2: token2.to_string(),
            tokens_json: "[]".to_string(),
            market_competitiveness: None,
            rewards_min_size: None,
            rewards_max_spread: None,
            market_daily_reward: None,
            volume_24hr_clob: None,
            volume_24hr: None,
            liquidity_reward_roi: None,
            build_date_utc: None,
            pool_version: Some(1),
            liquidity_reward_selected: true,
            liquidity_reward_selected_at_ms: Some(100),
            liquidity_reward_select_reason: Some("roi_descending_top_n".to_string()),
            liquidity_reward_select_rank: Some(1),
            liquidity_reward_halted: false,
            liquidity_reward_halted_at_ms: None,
            liquidity_reward_halt_reason: None,
            liquidity_reward_halted_pool_version: None,
        }
    }

    #[test]
    fn account_monitor_enabled_respects_account_config() {
        let mut config = AccountConfig::default();
        assert!(!account_monitor_enabled(&config));

        config.enabled = true;
        assert!(account_monitor_enabled(&config));
    }

    #[test]
    fn build_strategy_registrations_includes_market_maker_when_present() {
        let pair_registration = StrategyRegistration {
            name: Arc::from("pair_arbitrage"),
            topics: Arc::<[Arc<str>]>::from(vec![Arc::from("pair")]),
            topic_tokens: Arc::<[TopicRegistration]>::from(vec![TopicRegistration {
                topic: Arc::from("pair"),
                tokens: Arc::<[String]>::from(vec!["pair-token".to_string()]),
            }]),
            related_tokens: Arc::<[String]>::from(vec!["pair-token".to_string()]),
        };
        let market_maker = MarketMakerStrategy::from_pool_entries(vec![active_pool_entry(
            "0xabc",
            "maker-token-1",
            "maker-token-2",
        )])
        .expect("market maker should build")
        .expect("non-empty pool should create market maker");

        let registrations =
            build_strategy_registrations(&pair_registration, None, Some(&market_maker));

        assert_eq!(registrations.len(), 2);
        assert_eq!(registrations[0].name.as_ref(), "pair_arbitrage");
        assert_eq!(registrations[1].name.as_ref(), "market_maker");
    }

    fn app_config_with_liquidity_reward_enabled() -> AppConfig {
        AppConfig {
            proxy: ProxySettings { url: String::new() },
            app: AppSettings {
                log_file: "alerts.log".to_string(),
                assets_file: "assets.csv".to_string(),
                sqlite_path: String::new(),
                market_sqlite_path: String::new(),
                min_diff: 0.0,
                max_spread: 0.0,
                min_price: 0.0,
                max_price: 1.0,
                default_threads: 1,
                monitor_interval_secs: 30,
                tick_store_enabled: false,
                raw_store_enabled: false,
            },
            auth: AuthConfig {
                api_key: String::new(),
                api_secret: String::new(),
                passphrase: String::new(),
                private_key: String::new(),
                funder: String::new(),
            },
            order: OrderConfig { size_usdc: 1.0 },
            simulation: SimulationConfig { enabled: false },
            liquidity_reward: LiquidityRewardConfig {
                enabled: true,
                source: "db_pool".to_string(),
                monitor_enabled: true,
                ..LiquidityRewardConfig::default()
            },
            account: AccountConfig::default(),
            notification: NotificationConfig::default(),
            topic_threads: HashMap::new(),
        }
    }

    #[test]
    fn liquidity_reward_runtime_is_forced_disabled_even_when_config_enabled() {
        let config = app_config_with_liquidity_reward_enabled();

        assert!(!liquidity_reward_runtime_enabled(&config));
    }

    #[test]
    fn build_liquidity_reward_strategy_is_forced_disabled_even_when_config_enabled() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let build_date = chrono::NaiveDate::from_ymd_opt(2026, 5, 14).unwrap();
        let entries = vec![storage::RewardMarketPoolStorageEntry {
            condition_id: "0xlr",
            market_slug: Some("lr-market"),
            question: Some("LR market?"),
            token1: "lr-token-1",
            token2: "lr-token-2",
            tokens_json: "[]",
            market_competitiveness: None,
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("50"),
            volume_24hr_clob: Some("60000"),
            volume_24hr: Some("65000"),
            liquidity_reward_roi: Some("0.5"),
        }];
        store
            .replace_reward_market_pool_entries(build_date, 1, &entries, 100, 1)
            .expect("pool entries should save");
        let config = app_config_with_liquidity_reward_enabled();

        let strategy = build_liquidity_reward_strategy(&config, &store)
            .expect("forced disabled liquidity reward should not error");

        assert!(strategy.is_none());
    }

    #[test]
    fn build_market_maker_strategy_uses_selected_pool_entries() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let build_date = chrono::NaiveDate::from_ymd_opt(2026, 5, 14).unwrap();
        let entries = vec![storage::RewardMarketPoolStorageEntry {
            condition_id: "0xmaker",
            market_slug: Some("maker-market"),
            question: Some("Maker market?"),
            token1: "maker-token-1",
            token2: "maker-token-2",
            tokens_json: "[]",
            market_competitiveness: None,
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("50"),
            volume_24hr_clob: Some("60000"),
            volume_24hr: Some("65000"),
            liquidity_reward_roi: Some("0.5"),
        }];
        store
            .replace_reward_market_pool_entries(build_date, 1, &entries, 100, 1)
            .expect("pool entries should save");

        let market_maker = build_market_maker_strategy(&store)
            .expect("market maker build should not error")
            .expect("selected pool should create market maker");

        assert_eq!(market_maker.registration().name.as_ref(), "market_maker");
        assert_eq!(
            market_maker.registration().related_tokens.as_ref(),
            &["maker-token-1".to_string(), "maker-token-2".to_string()]
        );
    }

    #[test]
    fn market_maker_strategy_handle_is_created_when_strategy_exists() {
        let market_maker = MarketMakerStrategy::from_pool_entries(vec![active_pool_entry(
            "0xabc",
            "maker-token-1",
            "maker-token-2",
        )])
        .expect("market maker should build")
        .expect("non-empty pool should create market maker");

        let (handle, _rx) =
            market_maker_strategy_handle(&market_maker).expect("market maker handle should exist");

        assert_eq!(handle.name.as_ref(), "market_maker");
        assert_eq!(
            handle.related_tokens.as_ref(),
            &["maker-token-1".to_string(), "maker-token-2".to_string()]
        );
    }
}

fn resolve_path(filename: &str) -> String {
    let path_obj = Path::new(filename);
    if path_obj.is_absolute() {
        filename.to_string()
    } else if let Ok(mut exe_path) = std::env::current_exe() {
        exe_path.pop();
        exe_path.push(filename);
        exe_path.to_string_lossy().to_string()
    } else {
        filename.to_string()
    }
}

fn derive_market_sqlite_path(order_sqlite_path: &str) -> String {
    let mut path = PathBuf::from(order_sqlite_path);
    path.set_file_name("market.db");
    path.to_string_lossy().to_string()
}
