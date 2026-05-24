mod account;
mod clob_client;
mod config;
mod logging;
mod market;
mod notification;
#[path = "order/order_gateway.rs"]
mod order_gateway;
#[path = "order/order_ws.rs"]
mod order_ws;
#[path = "position/position_engine.rs"]
mod position_engine;
#[path = "position/valuation.rs"]
mod position_valuation;
mod proxy_ws;
#[path = "risk/risk.rs"]
mod risk;
mod storage;
mod strategies;
#[path = "strategies/strategy.rs"]
mod strategy;
mod tick_size;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use log::info;
use polymarket_client_sdk_v2::types::Decimal;

use config::{AppConfig, load_app_config};
use order_gateway::{
    ClobOrderCancelSubmitter, ClobOrderSubmitter, ClobRemoteOrderReader, LocalRejectReason,
    OrderEventKind, OrderEventPayload, OrderEventSubscriber, OrderGateway, OrderGatewayConfig,
    OrderSide, SimulatedOrderCancelSubmitter, SimulatedOrderSubmitter,
};
use risk::{
    AccountHandleEquityReader, GatewayRiskEngine, GlobalPortfolioValuationReader,
    NoopMarketRiskReader, RiskConfig, StrategyKindRegistry,
};
use storage::{MarketStore, OrderStore};
use strategies::market_maker::{MarketMakerStrategy, MarketMakerStrategyConfig};
use strategies::pair_arbitrage::PairArbitrageStrategy;
use strategy::{
    CleanOrderbook, Filters, Strategy, StrategyRegistration, build_token_topics,
    build_topic_broadcasts, merge_topic_tokens, subscribe_strategy_topics,
};

type WsMessage = polymarket_client_sdk_v2::clob::ws::types::response::WsMessage;
type TopicTokens = Arc<HashMap<Arc<str>, Vec<String>>>;
type TokenTopics = Arc<HashMap<String, Arc<[Arc<str>]>>>;
type WsSender = tokio::sync::mpsc::Sender<WsMessage>;
type WsReceiver = tokio::sync::mpsc::Receiver<WsMessage>;
type TopicBroadcasts =
    Arc<HashMap<Arc<str>, tokio::sync::broadcast::Sender<strategy::MarketEvent>>>;
type TickRecorderSender = tokio::sync::mpsc::Sender<(Arc<str>, CleanOrderbook)>;
type RawRecorderSender = tokio::sync::mpsc::Sender<market::RawStoreEvent>;
type MarketFirehoseSender = tokio::sync::mpsc::Sender<strategy::MarketAssetEvent>;

struct StrategyBootstrap {
    pair_strategy: PairArbitrageStrategy,
    pair_registration: StrategyRegistration,
    market_maker: Option<MarketMakerStrategy>,
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
    ws_tx: WsSender,
    ws_rx: WsReceiver,
    firehose_tx: MarketFirehoseSender,
    topic_txs: TopicBroadcasts,
    book_publisher: market::MarketBookPublisher,
    recorder_senders: RecorderSenders,
    tick_size_map: tick_size::TickSizeMap,
    proxy: Option<proxy_ws::Proxy>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    // 读取 config.toml + config.local.toml，后续所有路径和开关都以这里为准。
    let app_config = load_app_config()?;
    let log_path = init_log_path(&app_config);
    logging::init_logging(&log_path)?;
    let notifier = notification::spawn_dingtalk_notifier(app_config.notification.dingtalk.clone());
    let (order_store, market_store) = init_stores(&app_config)?;

    let account_read_handle = account::spawn_account_monitor(app_config.auth.clone());
    let strategy_bootstrap = build_strategies(&app_config, market_store.clone()).await?;
    let StrategyBootstrap {
        pair_strategy,
        pair_registration,
        market_maker,
        tick_size_map,
    } = strategy_bootstrap;

    let proxy = init_proxy(&app_config);
    let registrations = build_strategy_registrations(&pair_registration, market_maker.as_ref());
    let routing = build_routing(&registrations);

    info!("正在连接 Polymarket WebSocket...");
    info!(
        "开始监听 token 价格变动 routing.token_count={:?}",
        routing.token_count
    );

    // ws_tx 承载公开行情 raw message。
    let (ws_tx, ws_rx) = tokio::sync::mpsc::channel(256 * routing.topic_tokens.len().max(1));
    let (firehose_tx, firehose_rx) = tokio::sync::mpsc::channel(16_384);
    let topic_txs = Arc::new(build_topic_broadcasts(routing.topic_tokens.as_ref(), 1024));
    let book_publisher = market::MarketBookPublisher::new();
    let market_book_read_handle = book_publisher.read_handle();
    let position_keeper = position_engine::recover_keeper(&order_store)?;
    let (position_ingestor, position_ingest_handle, position_persist_rx) =
        position_engine::PositionIngestor::new(16_384, 16_384, position_keeper);
    let position_read_handle = position_ingest_handle.read_handle();
    let strategy_position_read_handle = position_read_handle.clone();
    let position_status_handle = position_ingest_handle.status_handle();
    let risk_engine = GatewayRiskEngine::new_with_daily_loss_limit(
        RiskConfig::default(),
        position_read_handle,
        position_status_handle,
        StrategyKindRegistry::from_registrations(&registrations),
        Arc::new(NoopMarketRiskReader),
        order_store.clone(),
        Arc::new(GlobalPortfolioValuationReader::new(
            strategy_position_read_handle.clone(),
            position_valuation::MarketBookMarkPriceReader::new(
                market_book_read_handle,
                position_valuation::MarkPriceKind::BestBid,
            ),
        )),
        Arc::new(AccountHandleEquityReader::new(account_read_handle)),
    );
    let gateway_config = OrderGatewayConfig {
        simulation_enabled: app_config.simulation.enabled,
        request_ring_capacity: 1024,
        event_ring_capacity: 16384,
    };
    let order_submitter: Arc<dyn order_gateway::OrderSubmitter> = if app_config.simulation.enabled {
        Arc::new(SimulatedOrderSubmitter)
    } else {
        Arc::new(ClobOrderSubmitter::new(app_config.auth.clone()))
    };
    let order_cancel_submitter: Arc<dyn order_gateway::OrderCancelSubmitter> =
        if app_config.simulation.enabled {
            Arc::new(SimulatedOrderCancelSubmitter)
        } else {
            Arc::new(ClobOrderCancelSubmitter::new(app_config.auth.clone()))
        };
    let (mut order_gateway, order_gateway_handle, order_event_ring, order_observation_tx) =
        if app_config.simulation.enabled {
            OrderGateway::new_with_submitters(
                gateway_config,
                Arc::new(risk_engine),
                order_store.clone(),
                order_submitter,
                order_cancel_submitter,
            )
        } else {
            OrderGateway::new_with_submitters_and_remote_reader(
                gateway_config,
                Arc::new(risk_engine),
                order_store.clone(),
                order_submitter,
                order_cancel_submitter,
                Arc::new(ClobRemoteOrderReader::new(app_config.auth.clone())),
            )
        };
    let pending_settlement_rx = order_gateway.subscribe_pending_settlements();
    tokio::spawn(position_ingestor.run_until_input_closed());
    tokio::spawn(position_engine::run_persist_task(
        order_store.clone(),
        position_persist_rx,
    ));
    tokio::spawn(position_engine::run_order_event_bridge(
        order_event_ring.subscribe_all(),
        position_ingest_handle,
    ));
    if let Some(notifier) = notifier.clone() {
        tokio::spawn(run_order_notification_bridge(
            order_event_ring.subscribe_all(),
            notifier,
        ));
    }
    order_gateway
        .complete_startup_recovery()
        .await
        .expect("order gateway startup recovery should complete");
    tokio::spawn(order_gateway.run_until_request_channel_closed());

    spawn_strategy_tasks(
        pair_strategy,
        pair_registration,
        market_maker,
        order_gateway_handle,
        strategy_position_read_handle,
        topic_txs.as_ref(),
        notifier.clone(),
    )?;
    tokio::spawn(drain_market_firehose(firehose_rx));
    let recorder_senders = spawn_recorders(&app_config, market_store.clone());
    spawn_market_and_positions(
        &app_config,
        MarketRuntime {
            topic_tokens: routing.topic_tokens,
            token_topics: routing.token_topics,
            ws_tx,
            ws_rx,
            firehose_tx,
            topic_txs,
            book_publisher,
            recorder_senders,
            tick_size_map,
            proxy,
        },
    );
    if !app_config.simulation.enabled {
        OrderGateway::spawn_private_order_ws(
            app_config.auth.clone(),
            order_store,
            order_observation_tx.clone(),
        );
        OrderGateway::spawn_settlement_activity_poller(
            order_gateway::DataApiSettlementActivityReader::new(app_config.auth.funder.parse()?),
            pending_settlement_rx,
            order_observation_tx,
        );
    }

    futures::future::pending::<()>().await;
    Ok(())
}

async fn build_strategies(
    app_config: &AppConfig,
    market_store: MarketStore,
) -> anyhow::Result<StrategyBootstrap> {
    let pair_strategy = build_pair_strategy(app_config)?;
    let pair_registration = pair_strategy.registration().clone();
    let market_maker = build_market_maker_strategy(app_config, &market_store)?;
    let tick_size_map = tick_size::new_tick_size_map();

    Ok(StrategyBootstrap {
        pair_strategy,
        pair_registration,
        market_maker,
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
    app_config: &AppConfig,
    _market_store: &MarketStore,
) -> anyhow::Result<Option<MarketMakerStrategy>> {
    build_market_maker_strategy_from_config(&app_config.market_maker)
}

fn build_market_maker_strategy_from_config(
    config: &config::MarketMakerConfig,
) -> anyhow::Result<Option<MarketMakerStrategy>> {
    if !config.enabled {
        info!(target: "order", "market_maker 配置已禁用，未启动");
        return Ok(None);
    }
    let csv_filename = if config.file.is_empty() {
        "market_maker.csv"
    } else {
        config.file.as_str()
    };
    let csv_file = resolve_path(csv_filename);
    let strategy_config = MarketMakerStrategyConfig::try_from(config)?;
    build_market_maker_strategy_from_csv_file_with_config(&csv_file, strategy_config)
}

fn build_market_maker_strategy_from_csv_file(
    csv_file: &str,
) -> anyhow::Result<Option<MarketMakerStrategy>> {
    build_market_maker_strategy_from_csv_file_with_config(
        csv_file,
        MarketMakerStrategyConfig::default(),
    )
}

fn build_market_maker_strategy_from_csv_file_with_config(
    csv_file: &str,
    config: MarketMakerStrategyConfig,
) -> anyhow::Result<Option<MarketMakerStrategy>> {
    let strategy = MarketMakerStrategy::from_csv_with_config(csv_file, config)?;
    match strategy.as_ref() {
        Some(strategy) => {
            info!(target: "order", "market_maker 已加载 CSV csv_file={:?} token_count={:?} market_count={:?}", csv_file, strategy.registration().related_tokens.len(), strategy.rules().len());
            for rule in strategy.rules() {
                info!(target: "order", "market_maker 加载市场 csv_file={:?} condition_id={} market_slug={} token1={} token2={} rewards_max_spread={} rewards_min_size={}", csv_file, rule.condition_id, rule.market_slug.as_deref().unwrap_or_default(), rule.token1, rule.token2, rule.rewards_max_spread.as_deref().unwrap_or_default(), rule.rewards_min_size.as_deref().unwrap_or_default());
            }
        }
        None => info!(target: "order", "market_maker CSV 为空，未启动 csv_file={:?}", csv_file),
    }
    Ok(strategy)
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
    market_maker: Option<&MarketMakerStrategy>,
) -> Vec<StrategyRegistration> {
    let mut registrations = vec![pair_registration.clone()];
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

fn spawn_strategy_tasks(
    pair_strategy: PairArbitrageStrategy,
    pair_registration: StrategyRegistration,
    market_maker: Option<MarketMakerStrategy>,
    order_gateway_handle: order_gateway::OrderGatewayHandle,
    position_read: position_engine::PositionReadHandle,
    topic_txs: &HashMap<Arc<str>, tokio::sync::broadcast::Sender<strategy::MarketEvent>>,
    notifier: Option<notification::Notifier>,
) -> anyhow::Result<()> {
    let pair_subscriptions = subscribe_strategy_topics(&pair_registration, topic_txs)?;
    pair_strategy.spawn(
        pair_subscriptions,
        order_gateway_handle.clone(),
        position_read.clone(),
    );

    if let Some(market_maker_strategy) = market_maker {
        let market_maker_subscriptions =
            subscribe_strategy_topics(market_maker_strategy.registration(), topic_txs)?;
        market_maker_strategy.with_notifier(notifier).spawn(
            market_maker_subscriptions,
            order_gateway_handle,
            position_read,
        );
    }

    Ok(())
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

async fn drain_market_firehose(mut rx: tokio::sync::mpsc::Receiver<strategy::MarketAssetEvent>) {
    while rx.recv().await.is_some() {}
}

async fn run_order_notification_bridge(
    mut subscriber: OrderEventSubscriber,
    notifier: notification::Notifier,
) {
    loop {
        match subscriber.recv_relevant().await {
            Ok(event) => {
                if let Some(notification) = notification_from_order_event(&event) {
                    notifier.try_notify(notification);
                }
            }
            Err(order_gateway::OrderEventPollError::Closed) => break,
            Err(order_gateway::OrderEventPollError::Lagged { skipped }) => {
                log::warn!(target: "notification", "订单通知订阅落后，部分订单通知可能已跳过 skipped={:?}", skipped);
            }
            Err(order_gateway::OrderEventPollError::Empty) => {}
        }
    }
}

fn notification_from_order_event(
    event: &order_gateway::OrderEventEnvelope,
) -> Option<notification::NotificationEvent> {
    if event.recovery {
        return None;
    }

    match (&event.kind, &event.payload) {
        (
            OrderEventKind::PartialFill,
            OrderEventPayload::PartialFill {
                fill_qty,
                fill_price,
                cum_qty,
                avg_fill_price,
            },
        ) => Some(order_fill_notification(
            event,
            "partial_fill",
            *fill_qty,
            *fill_price,
            *cum_qty,
            *avg_fill_price,
        )),
        (
            OrderEventKind::Fill,
            OrderEventPayload::Fill {
                fill_qty,
                fill_price,
                cum_qty,
                avg_fill_price,
            },
        ) => Some(order_fill_notification(
            event,
            "fill",
            *fill_qty,
            *fill_price,
            *cum_qty,
            *avg_fill_price,
        )),
        (
            OrderEventKind::LocalRejected,
            OrderEventPayload::LocalRejected {
                reason: LocalRejectReason::RiskRejected { code, reason },
            },
        ) => Some(notification::NotificationEvent::RiskEvent(
            notification::RiskEventNotification {
                source: "order_gateway".to_string(),
                strategy_id: Some(event.strategy_id.as_str().to_string()),
                local_order_id: Some(event.local_id.as_str().to_string()),
                market_id: Some(event.market_id.as_str().to_string()),
                token_id: Some(event.token_id.as_str().to_string()),
                risk_code: code.to_string(),
                reason: reason.to_string(),
            },
        )),
        _ => None,
    }
}

fn order_fill_notification(
    event: &order_gateway::OrderEventEnvelope,
    fill_kind: &str,
    fill_qty: Decimal,
    fill_price: Decimal,
    cum_qty: Decimal,
    avg_fill_price: Option<Decimal>,
) -> notification::NotificationEvent {
    let order = event.order.as_ref();
    notification::NotificationEvent::OrderFilled(notification::OrderFilledNotification {
        strategy_id: event.strategy_id.as_str().to_string(),
        local_order_id: event.local_id.as_str().to_string(),
        market_id: event.market_id.as_str().to_string(),
        token_id: event.token_id.as_str().to_string(),
        side: order
            .map(|meta| order_side_label(meta.side).to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        fill_kind: fill_kind.to_string(),
        fill_qty,
        fill_price,
        cum_qty,
        avg_fill_price,
    })
}

fn order_side_label(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    }
}

fn spawn_market_and_positions(app_config: &AppConfig, runtime: MarketRuntime) {
    let MarketRuntime {
        topic_tokens,
        token_topics,
        ws_tx,
        ws_rx,
        firehose_tx,
        topic_txs,
        book_publisher,
        recorder_senders,
        tick_size_map,
        proxy,
    } = runtime;
    let default_threads = app_config.app.default_threads.max(1);

    // market::run 汇总公开行情并按 token/topic 分发给策略和持久化 recorder。
    tokio::spawn(market::run(
        token_topics,
        ws_rx,
        book_publisher,
        firehose_tx,
        topic_txs,
        recorder_senders.tick_tx,
        recorder_senders.raw_store_tx,
        tick_size_map,
    ));

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

fn init_log_path(app_config: &AppConfig) -> String {
    let log_filename = if !app_config.app.log_file.is_empty() {
        app_config.app.log_file.as_str()
    } else {
        "poly-executor.log"
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
    // market.db 保存行情和每日奖励市场池状态。
    let market_store = MarketStore::open(&market_sqlite_path)?;
    market_store.init_schema()?;

    Ok((order_store, market_store))
}

// 相对路径按可执行文件目录解析，避免服务方式启动时落到意外工作目录。
#[cfg(test)]
mod tests {
    use super::*;
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
    fn account_monitor_starts_without_config_gate() {
        let _spawn_fn: fn(config::AuthConfig) -> account::AccountReadHandle =
            account::spawn_account_monitor;
    }

    #[test]
    fn order_gateway_exposes_settlement_activity_poller_entrypoint() {
        let _spawn_fn = OrderGateway::spawn_settlement_activity_poller::<
            order_gateway::DataApiSettlementActivityReader,
        >;
    }

    fn sample_order_meta() -> order_gateway::OrderEventOrderMeta {
        order_gateway::OrderEventOrderMeta {
            side: order_gateway::OrderSide::Buy,
            order_type: order_gateway::GatewayOrderType::Limit {
                time_in_force: order_gateway::TimeInForce::Gtc,
            },
            price: Some(Decimal::try_from(0.42_f64).unwrap()),
            original_size: Decimal::try_from(10.5_f64).unwrap(),
        }
    }

    fn sample_order_event(
        kind: order_gateway::OrderEventKind,
        payload: order_gateway::OrderEventPayload,
    ) -> order_gateway::OrderEventEnvelope {
        order_gateway::OrderEventEnvelope {
            strategy_id: order_gateway::StrategyId::from("market_maker"),
            local_id: order_gateway::LocalOrderId::from("mm-local-1"),
            token_id: order_gateway::TokenId::from("token-1"),
            market_id: order_gateway::MarketId::from("0xmarket"),
            seq: 1,
            ts_ns: 1,
            recovery: false,
            kind,
            payload,
            order: Some(sample_order_meta()),
        }
    }

    #[test]
    fn accepted_order_event_is_ignored_for_dingtalk_notification() {
        let event = sample_order_event(
            order_gateway::OrderEventKind::Accepted,
            order_gateway::OrderEventPayload::Accepted {
                exch_id: Some(order_gateway::ExchangeOrderId::from("0xorder")),
            },
        );

        assert!(notification_from_order_event(&event).is_none());
    }

    #[test]
    fn partial_fill_event_maps_to_order_filled_notification() {
        let event = sample_order_event(
            order_gateway::OrderEventKind::PartialFill,
            order_gateway::OrderEventPayload::PartialFill {
                fill_qty: Decimal::try_from(1.25_f64).unwrap(),
                fill_price: Decimal::try_from(0.41_f64).unwrap(),
                cum_qty: Decimal::try_from(2.5_f64).unwrap(),
                avg_fill_price: Some(Decimal::try_from(0.415_f64).unwrap()),
            },
        );

        let notification = notification_from_order_event(&event).expect("should map fill");

        match notification {
            notification::NotificationEvent::OrderFilled(fill) => {
                assert_eq!(fill.strategy_id, "market_maker");
                assert_eq!(fill.local_order_id, "mm-local-1");
                assert_eq!(fill.fill_kind, "partial_fill");
                assert_eq!(fill.fill_qty, Decimal::try_from(1.25_f64).unwrap());
                assert_eq!(fill.fill_price, Decimal::try_from(0.41_f64).unwrap());
                assert_eq!(fill.cum_qty, Decimal::try_from(2.5_f64).unwrap());
                assert_eq!(
                    fill.avg_fill_price,
                    Some(Decimal::try_from(0.415_f64).unwrap())
                );
            }
            other => panic!("unexpected notification: {other:?}"),
        }
    }

    #[test]
    fn fill_event_maps_to_order_filled_notification() {
        let event = sample_order_event(
            order_gateway::OrderEventKind::Fill,
            order_gateway::OrderEventPayload::Fill {
                fill_qty: Decimal::try_from(8_f64).unwrap(),
                fill_price: Decimal::try_from(0.44_f64).unwrap(),
                cum_qty: Decimal::try_from(10.5_f64).unwrap(),
                avg_fill_price: Some(Decimal::try_from(0.435_f64).unwrap()),
            },
        );

        let notification = notification_from_order_event(&event).expect("should map fill");

        match notification {
            notification::NotificationEvent::OrderFilled(fill) => {
                assert_eq!(fill.strategy_id, "market_maker");
                assert_eq!(fill.local_order_id, "mm-local-1");
                assert_eq!(fill.fill_kind, "fill");
                assert_eq!(fill.fill_qty, Decimal::try_from(8_f64).unwrap());
                assert_eq!(fill.fill_price, Decimal::try_from(0.44_f64).unwrap());
                assert_eq!(fill.cum_qty, Decimal::try_from(10.5_f64).unwrap());
                assert_eq!(
                    fill.avg_fill_price,
                    Some(Decimal::try_from(0.435_f64).unwrap())
                );
            }
            other => panic!("unexpected notification: {other:?}"),
        }
    }

    #[test]
    fn risk_local_rejected_event_maps_to_risk_notification() {
        let event = sample_order_event(
            order_gateway::OrderEventKind::LocalRejected,
            order_gateway::OrderEventPayload::LocalRejected {
                reason: order_gateway::LocalRejectReason::RiskRejected {
                    code: Arc::from("daily_loss_limit"),
                    reason: Arc::from("daily loss limit triggered"),
                },
            },
        );

        let notification = notification_from_order_event(&event).expect("should map risk reject");

        match notification {
            notification::NotificationEvent::RiskEvent(risk) => {
                assert_eq!(risk.source, "order_gateway");
                assert_eq!(risk.strategy_id.as_deref(), Some("market_maker"));
                assert_eq!(risk.local_order_id.as_deref(), Some("mm-local-1"));
                assert_eq!(risk.market_id.as_deref(), Some("0xmarket"));
                assert_eq!(risk.token_id.as_deref(), Some("token-1"));
                assert_eq!(risk.risk_code, "daily_loss_limit");
                assert_eq!(risk.reason, "daily loss limit triggered");
            }
            other => panic!("unexpected notification: {other:?}"),
        }
    }

    #[test]
    fn non_risk_local_rejected_event_is_ignored() {
        let event = sample_order_event(
            order_gateway::OrderEventKind::LocalRejected,
            order_gateway::OrderEventPayload::LocalRejected {
                reason: order_gateway::LocalRejectReason::DuplicateLocalId,
            },
        );

        assert!(notification_from_order_event(&event).is_none());
    }

    #[test]
    fn recovery_order_event_is_ignored() {
        let mut event = sample_order_event(
            order_gateway::OrderEventKind::Accepted,
            order_gateway::OrderEventPayload::Accepted {
                exch_id: Some(order_gateway::ExchangeOrderId::from("0xorder")),
            },
        );
        event.recovery = true;

        assert!(notification_from_order_event(&event).is_none());
    }

    #[test]
    fn build_strategy_registrations_includes_market_maker_when_present() {
        let pair_registration = StrategyRegistration {
            name: Arc::from("pair_arbitrage"),
            kind: strategy::StrategyKind::PairArbitrage,
            topics: Arc::<[Arc<str>]>::from(vec![Arc::from("pair-token")]),
            topic_tokens: Arc::<[TopicRegistration]>::from(vec![TopicRegistration {
                topic: Arc::from("pair-token"),
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

        let registrations = build_strategy_registrations(&pair_registration, Some(&market_maker));

        assert_eq!(registrations.len(), 2);
        assert_eq!(registrations[0].name.as_ref(), "pair_arbitrage");
        assert_eq!(registrations[1].name.as_ref(), "market_maker");
    }

    #[test]
    fn build_market_maker_strategy_loads_csv_file() {
        let csv_path = std::env::temp_dir().join(format!(
            "market_maker_load_csv_{}_{}.csv",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time should move forward")
                .as_nanos()
        ));
        std::fs::write(
            &csv_path,
            "token1,token2,topic,reward_min_orders,reward_max_spread_cents,reward_min_size,reward_daily_pool,fixed_price\ncsv-maker-token-1,csv-maker-token-2,market_maker,,3,100,75,false\n",
        )
        .expect("csv should write");

        let market_maker =
            build_market_maker_strategy_from_csv_file(csv_path.to_str().expect("utf8 path"))
                .expect("market maker build should not error")
                .expect("csv row should create market maker");

        assert_eq!(market_maker.registration().name.as_ref(), "market_maker");
        assert_eq!(
            market_maker.registration().related_tokens.as_ref(),
            &[
                "csv-maker-token-1".to_string(),
                "csv-maker-token-2".to_string()
            ]
        );
    }

    #[test]
    fn build_market_maker_strategy_from_config_skips_csv_when_disabled() {
        let config = config::MarketMakerConfig {
            enabled: false,
            file: "missing-market-maker.csv".to_string(),
            ..Default::default()
        };

        let market_maker = build_market_maker_strategy_from_config(&config)
            .expect("disabled market maker should not try to read csv");

        assert!(market_maker.is_none());
    }

    #[test]
    fn build_market_maker_strategy_from_config_uses_configured_csv_file() {
        let csv_path = std::env::temp_dir().join(format!(
            "market_maker_config_csv_{}_{}.csv",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("time should move forward")
                .as_nanos()
        ));
        std::fs::write(
            &csv_path,
            "token1,token2,topic,reward_min_orders,rewards_max_spread,reward_min_size,reward_daily_pool,fixed_price\nconfigured-token-1,configured-token-2,market_maker,,0.04,25,75,false\n",
        )
        .expect("csv should write");
        let config = config::MarketMakerConfig {
            enabled: true,
            file: csv_path.to_string_lossy().to_string(),
            default_max_spread: 0.02,
            min_size: 10.0,
            ..Default::default()
        };

        let market_maker = build_market_maker_strategy_from_config(&config)
            .expect("configured market maker should build")
            .expect("configured csv row should create strategy");

        assert_eq!(market_maker.rules()[0].token1, "configured-token-1");
        assert_eq!(
            market_maker.rules()[0].rewards_max_spread.as_deref(),
            Some("0.04")
        );
        assert_eq!(
            market_maker.rules()[0].rewards_min_size.as_deref(),
            Some("25")
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
