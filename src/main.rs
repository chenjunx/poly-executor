mod dispatcher;
mod logging;
mod market;
mod monitor;
mod order;
mod order_ws;
mod positions;
mod price_store;
mod proxy_ws;
mod strategies;
mod strategy;
mod storage;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use dashmap::DashMap;

use config::{Config, File};
use polymarket_client_sdk::auth::{LocalSigner, Normal, Signer as _};
use polymarket_client_sdk::clob::types::request::{OrdersRequest, TradesRequest};
use polymarket_client_sdk::clob::types::response::OpenOrderResponse;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::error::{Kind as PmErrorKind, Status as PmStatus, StatusCode};
use polymarket_client_sdk::types::Decimal;
use polymarket_client_sdk::POLYGON;
use serde::Deserialize;
use tracing::info;

use dispatcher::Dispatcher;
use positions::{PositionRefreshTrigger, SimulatedFillEvent};
use storage::OrderStore;
use strategies::mid_requote::{MidRequoteRestoreState, MidRequoteStrategy};
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
    simulation: SimulationConfig,
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
    #[serde(default)]
    sqlite_path: String,
    min_diff: f64,
    max_spread: f64,
    min_price: f64,
    max_price: f64,
    default_threads: usize,
    #[serde(default = "default_monitor_interval_secs")]
    monitor_interval_secs: u64,
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
struct SimulationConfig {
    #[serde(default)]
    enabled: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
struct MidRequoteConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    file: String,
    #[serde(default)]
    monitor_enabled: bool,
}

fn default_monitor_interval_secs() -> u64 {
    30
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

    let order_correlations: OrderCorrelationMap = Arc::new(DashMap::new());

    let stored_active_orders = order_store.load_active_orders()?;
    let reconciled_active_orders = if app_config.simulation.enabled {
        stored_active_orders
    } else {
        reconcile_startup_orders(&app_config.auth, &order_store, stored_active_orders).await?
    };

    for stored_order in reconciled_active_orders {
        let local_meta = stored_order.to_local_order_meta();
        order_correlations.insert(local_meta.local_order_id.clone(), local_meta.clone());
        if let Some(remote_order_id) = &local_meta.remote_order_id {
            order_correlations.insert(remote_order_id.clone(), local_meta.clone());
        }
        order_store.append_order_event(
            Some(&local_meta.local_order_id),
            local_meta.remote_order_id.as_deref(),
            "recovered_on_startup",
            serde_json::json!({
                "strategy": local_meta.strategy.as_ref(),
                "topic": local_meta.topic.as_ref().map(|topic| topic.as_ref()),
                "token": local_meta.token,
                "side": format!("{:?}", local_meta.side),
                "price": local_meta.price.to_string(),
                "order_size": local_meta.order_size.to_string(),
                "status": stored_order.status,
                "last_mid": stored_order.last_mid.map(|value| value.to_string()),
            }),
        )?;
    }

    let restored_mid_requote_states: HashMap<String, MidRequoteRestoreState> = order_store
        .load_mid_requote_states()?
        .into_iter()
        .map(|state| {
            let active_local_order_id = state
                .active_local_order_id
                .filter(|order_id| order_correlations.contains_key(order_id));
            let active_side = active_local_order_id
                .as_ref()
                .and_then(|order_id| order_correlations.get(order_id).map(|entry| entry.side));
            let pending_local_order_id = state
                .pending_local_order_id
                .filter(|order_id| order_correlations.contains_key(order_id));
            let has_pending_replacement = pending_local_order_id.is_some();
            (
                state.token.clone(),
                MidRequoteRestoreState {
                    topic: Arc::from(state.topic),
                    active_local_order_id,
                    active_side,
                    pending_local_order_id,
                    pending_side: if has_pending_replacement {
                        state.pending_side
                    } else {
                        None
                    },
                    pending_price: if has_pending_replacement {
                        state.pending_price
                    } else {
                        None
                    },
                    pending_order_size: if has_pending_replacement {
                        state.pending_order_size
                    } else {
                        None
                    },
                    pending_mid: if has_pending_replacement {
                        state.pending_mid
                    } else {
                        None
                    },
                    last_mid: state.last_mid,
                    last_best_bid: state.last_best_bid,
                    last_best_ask: state.last_best_ask,
                    last_position_size: state.last_position_size,
                },
            )
        })
        .collect();

    let mid_file = if !app_config.mid_requote.file.is_empty() {
        app_config.mid_requote.file.as_str()
    } else {
        "mid.csv"
    };
    let mid_requote = if app_config.mid_requote.enabled {
        MidRequoteStrategy::from_csv(mid_file)?
            .map(|strategy| strategy.with_restore_state(restored_mid_requote_states.clone(), Some(order_store.clone())))
    } else {
        None
    };

    let mid_tokens: Arc<std::collections::HashSet<String>> = Arc::new(
        mid_requote
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

    if app_config.mid_requote.enabled
        && app_config.mid_requote.monitor_enabled
        && !app_config.simulation.enabled
    {
        tokio::spawn(monitor::run_mid_reward_monitor(
            app_config.auth.clone(),
            app_config.app.monitor_interval_secs.max(1),
        ));
    }

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
    let (sim_fill_tx, sim_fill_rx) = tokio::sync::mpsc::channel::<SimulatedFillEvent>(64);

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
    tokio::spawn(market::run(
        token_topics.clone(),
        mid_tokens.clone(),
        ws_rx,
        strategy_tx.clone(),
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

async fn reconcile_startup_orders(
    auth: &AuthConfig,
    order_store: &OrderStore,
    stored_orders: Vec<storage::StoredOrder>,
) -> anyhow::Result<Vec<storage::StoredOrder>> {
    let client = build_authenticated_clob_client(auth).await?;
    let mut remote_open_orders = HashMap::new();
    let mut cursor: Option<String> = None;

    loop {
        let page = client.orders(&OrdersRequest::default(), cursor.clone()).await?;
        for order in page.data {
            remote_open_orders.insert(order.id.clone(), order);
        }
        if page.next_cursor == "LTE=" {
            break;
        }
        cursor = Some(page.next_cursor);
    }

    let mut reconciled = Vec::new();
    for stored_order in stored_orders {
        let Some(remote_order_id) = stored_order.remote_order_id.as_deref() else {
            order_store.update_order_status_by_local(&stored_order.local_order_id, "unknown")?;
            order_store.append_order_event(
                Some(&stored_order.local_order_id),
                None,
                "startup_reconciled",
                serde_json::json!({
                    "result": "missing_remote_order_id",
                    "status": "unknown",
                }),
            )?;
            continue;
        };

        if let Some(open_order) = remote_open_orders.get(remote_order_id) {
            let status = map_open_order_status(open_order);
            order_store.update_order_status_by_remote(remote_order_id, status)?;
            order_store.append_order_event(
                Some(&stored_order.local_order_id),
                Some(remote_order_id),
                "startup_reconciled",
                serde_json::json!({
                    "result": "present_in_open_orders",
                    "status": status,
                    "original_size": open_order.original_size.to_string(),
                    "size_matched": open_order.size_matched.to_string(),
                    "price": open_order.price.to_string(),
                }),
            )?;
            let mut updated = stored_order.clone();
            updated.status = status.to_string();
            reconciled.push(updated);
            continue;
        }

        match client.order(remote_order_id).await {
            Ok(order) => {
                let status = map_single_order_status(&order);
                order_store.update_order_status_by_remote(remote_order_id, status)?;
                order_store.append_order_event(
                    Some(&stored_order.local_order_id),
                    Some(remote_order_id),
                    "startup_reconciled",
                    serde_json::json!({
                        "result": "resolved_by_order_lookup",
                        "status": status,
                        "original_size": order.original_size.to_string(),
                        "size_matched": order.size_matched.to_string(),
                        "price": order.price.to_string(),
                    }),
                )?;
                if !matches!(status, "filled" | "canceled" | "rejected" | "failed" | "unknown") {
                    let mut updated = stored_order.clone();
                    updated.status = status.to_string();
                    reconciled.push(updated);
                }
            }
            Err(error) if is_not_found_status(&error) => {
                let terminal_status = infer_missing_remote_terminal_status(&client, &stored_order).await?;
                order_store.update_order_status_by_local(&stored_order.local_order_id, terminal_status)?;
                order_store.append_order_event(
                    Some(&stored_order.local_order_id),
                    Some(remote_order_id),
                    "startup_reconciled",
                    serde_json::json!({
                        "result": "missing_from_remote",
                        "status": terminal_status,
                    }),
                )?;
            }
            Err(error) => return Err(error.into()),
        }
    }

    Ok(reconciled)
}

async fn build_authenticated_clob_client(
    auth: &AuthConfig,
) -> anyhow::Result<ClobClient<polymarket_client_sdk::auth::state::Authenticated<Normal>>> {
    let signer = LocalSigner::from_str(&auth.private_key)?.with_chain_id(Some(POLYGON));
    Ok(
        ClobClient::new("https://clob.polymarket.com", ClobConfig::builder().use_server_time(true).build())?
            .authentication_builder(&signer)
            .funder(auth.funder.parse()?)
            .signature_type(polymarket_client_sdk::clob::types::SignatureType::Proxy)
            .authenticate()
            .await?,
    )
}

fn map_open_order_status(order: &OpenOrderResponse) -> &'static str {
    if order.size_matched == Decimal::ZERO {
        "open"
    } else {
        "partially_filled"
    }
}

fn map_single_order_status(order: &OpenOrderResponse) -> &'static str {
    use polymarket_client_sdk::clob::types::OrderStatusType;

    match order.status {
        OrderStatusType::Canceled => "canceled",
        OrderStatusType::Matched => {
            if order.size_matched == order.original_size {
                "filled"
            } else {
                "partially_filled"
            }
        }
        OrderStatusType::Live | OrderStatusType::Delayed | OrderStatusType::Unmatched => {
            map_open_order_status(order)
        }
        OrderStatusType::Unknown => "unknown",
        _ => "unknown",
    }
}

async fn infer_missing_remote_terminal_status(
    client: &ClobClient<polymarket_client_sdk::auth::state::Authenticated<Normal>>,
    stored_order: &storage::StoredOrder,
) -> anyhow::Result<&'static str> {
    let request = TradesRequest::builder()
        .asset_id(stored_order.token.clone())
        .build();
    let page = client.trades(&request, None).await?;
    let has_full_fill = page.data.iter().any(|trade| {
        trade.maker_orders.iter().any(|maker_order| {
            maker_order.order_id == stored_order.remote_order_id.clone().unwrap_or_default()
        })
    });
    Ok(if has_full_fill { "filled" } else { "unknown" })
}

fn is_not_found_status(error: &polymarket_client_sdk::error::Error) -> bool {
    error.kind() == PmErrorKind::Status
        && error
            .downcast_ref::<PmStatus>()
            .is_some_and(|status| status.status_code == StatusCode::NOT_FOUND)
}
