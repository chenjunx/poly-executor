use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use futures::StreamExt as _;
use polymarket_client_sdk::clob::types::Side;
use polymarket_client_sdk::clob::ws::Client;
use polymarket_client_sdk::clob::ws::types::response::{
    BookUpdate, PriceChangeBatchEntry, WsMessage,
};
use polymarket_client_sdk::types::Decimal;
use tracing::{info, warn};

use crate::monitor::FullBookSnapshot;
use crate::proxy_ws;
use crate::strategy::{CleanOrderbook, MarketEvent, StrategyEvent};

#[derive(Debug, Clone, Default)]
struct LocalOrderbook {
    bids: BTreeMap<u16, u32>,
    asks: BTreeMap<u16, u32>,
    timestamp_ms: u64,
}

impl LocalOrderbook {
    fn apply_book(&mut self, book: &BookUpdate) -> Option<CleanOrderbook> {
        self.bids.clear();
        self.asks.clear();

        for level in &book.bids {
            let Some(price) = scale_price(level.price) else {
                continue;
            };
            let Some(size) = scale_size(level.size) else {
                continue;
            };
            if size == 0 {
                continue;
            }
            self.bids.insert(price, size);
        }

        for level in &book.asks {
            let Some(price) = scale_price(level.price) else {
                continue;
            };
            let Some(size) = scale_size(level.size) else {
                continue;
            };
            if size == 0 {
                continue;
            }
            self.asks.insert(price, size);
        }

        self.timestamp_ms = u64::try_from(book.timestamp).ok()?;
        self.to_clean_orderbook()
    }

    fn apply_price_change(
        &mut self,
        change: &PriceChangeBatchEntry,
        timestamp_ms: u64,
    ) -> Option<CleanOrderbook> {
        let price = scale_price(change.price)?;
        let levels = match change.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
            Side::Unknown => return None,
            _ => return None,
        };

        match change.size.and_then(scale_size) {
            Some(0) => {
                levels.remove(&price);
            }
            Some(size) => {
                levels.insert(price, size);
            }
            None => {
                levels.remove(&price);
            }
        }

        self.timestamp_ms = self.timestamp_ms.max(timestamp_ms);
        self.to_clean_orderbook()
    }

    fn to_clean_orderbook(&self) -> Option<CleanOrderbook> {
        let (&best_bid_price, &best_bid_size) = self.bids.iter().next_back()?;
        let (&best_ask_price, &best_ask_size) = self.asks.iter().next()?;

        Some(CleanOrderbook {
            best_bid_price,
            best_bid_size,
            best_ask_price,
            best_ask_size,
            timestamp_ms: self.timestamp_ms,
        })
    }
}

pub async fn spawn_subscriptions(
    topic_groups: &HashMap<Arc<str>, Vec<String>>,
    topic_threads: &HashMap<String, usize>,
    default_threads: usize,
    proxy: Option<proxy_ws::Proxy>,
    tx: tokio::sync::mpsc::Sender<WsMessage>,
) {
    let all_chunks: Vec<Vec<String>> = topic_groups
        .iter()
        .flat_map(|(topic, tokens)| {
            let n = topic_threads
                .get(topic.as_ref())
                .copied()
                .unwrap_or(default_threads)
                .max(1);
            let chunk_size = (tokens.len() + n - 1) / n;
            tokens
                .chunks(chunk_size)
                .map(|c| c.to_vec())
                .collect::<Vec<_>>()
        })
        .collect();

    for (topic, tokens) in topic_groups {
        let n = topic_threads
            .get(topic.as_ref())
            .copied()
            .unwrap_or(default_threads)
            .max(1);
        info!(topic = %topic, token_count = tokens.len(), connection_count = n.min(tokens.len()), "topic 订阅连接已分配");
    }

    for chunk in all_chunks {
        let tx = tx.clone();
        if let Some(ref proxy) = proxy {
            let proxy = proxy.clone();
            tokio::spawn(async move {
                loop {
                    if let Err(e) = proxy_ws::run(proxy.clone(), chunk.clone(), tx.clone()).await {
                        warn!(error = %e, "proxy_ws 连接断开，5 秒后重连");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            });
        } else {
            tokio::spawn(async move {
                loop {
                    let client = Client::default();
                    let orderbook_stream = match client.subscribe_orderbook(chunk.clone()) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(error = %e, "direct subscribe_orderbook 失败，5 秒后重连");
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    };
                    let price_stream = match client.subscribe_prices(chunk.clone()) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(error = %e, "direct subscribe_prices 失败，5 秒后重连");
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    };
                    let merged_stream = futures::stream::select(
                        orderbook_stream.map(|result| result.map(WsMessage::Book)),
                        price_stream.map(|result| result.map(WsMessage::PriceChange)),
                    );
                    let mut merged_stream = Box::pin(merged_stream);
                    while let Some(result) = merged_stream.next().await {
                        match result {
                            Ok(msg) => {
                                if tx.send(msg).await.is_err() {
                                    return;
                                }
                            }
                            Err(error) => {
                                warn!(error = %error, "direct market stream 消息处理失败，准备重连");
                                break;
                            }
                        }
                    }
                    warn!("direct market stream 断开，5 秒后重连");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            });
        }
    }
}

pub async fn run(
    token_topics: Arc<HashMap<String, Arc<[Arc<str>]>>>,
    mid_tokens: Arc<std::collections::HashSet<String>>,
    mut ws_rx: tokio::sync::mpsc::Receiver<WsMessage>,
    market_tx: tokio::sync::mpsc::Sender<StrategyEvent>,
    monitor_tx: Option<tokio::sync::mpsc::Sender<FullBookSnapshot>>,
) {
    let mut books: HashMap<String, LocalOrderbook> = HashMap::new();

    while let Some(msg) = ws_rx.recv().await {
        let events = apply_market_message(&mut books, &msg);
        if events.is_empty() {
            continue;
        }

        for (asset_id, book) in events {
            log_mid_orderbook_diagnostics(&msg, &asset_id, &book, mid_tokens.as_ref());

            if let Some(ref tx) = monitor_tx {
                if let Some(state) = books.get(asset_id.as_ref()) {
                    let _ = tx.try_send(FullBookSnapshot {
                        asset_id: asset_id.clone(),
                        bids: Arc::new(state.bids.clone()),
                        asks: Arc::new(state.asks.clone()),
                        timestamp_ms: state.timestamp_ms,
                    });
                }
            }

            let Some(topics) = token_topics.get(asset_id.as_ref()) else {
                continue;
            };

            for topic in topics.iter().cloned() {
                if market_tx
                    .send(StrategyEvent::Market(MarketEvent {
                        topic,
                        asset_id: asset_id.clone(),
                        book,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
}

fn log_mid_orderbook_diagnostics(
    msg: &WsMessage,
    asset_id: &Arc<str>,
    book: &CleanOrderbook,
    mid_tokens: &std::collections::HashSet<String>,
) {
    if !mid_tokens.contains(asset_id.as_ref()) {
        return;
    }

    match msg {
        WsMessage::Book(snapshot) => {
            let first_bid = snapshot.bids.first().map(|level| (level.price, level.size));
            let last_bid = snapshot.bids.last().map(|level| (level.price, level.size));
            let first_ask = snapshot.asks.first().map(|level| (level.price, level.size));
            let last_ask = snapshot.asks.last().map(|level| (level.price, level.size));

            info!(
                target: "order",
                asset_id = %asset_id,
                event_type = "book",
                bids_len = snapshot.bids.len(),
                asks_len = snapshot.asks.len(),
                first_bid_price = ?first_bid.as_ref().map(|level| level.0),
                first_bid_size = ?first_bid.as_ref().map(|level| level.1),
                last_bid_price = ?last_bid.as_ref().map(|level| level.0),
                last_bid_size = ?last_bid.as_ref().map(|level| level.1),
                first_ask_price = ?first_ask.as_ref().map(|level| level.0),
                first_ask_size = ?first_ask.as_ref().map(|level| level.1),
                last_ask_price = ?last_ask.as_ref().map(|level| level.0),
                last_ask_size = ?last_ask.as_ref().map(|level| level.1),
                best_bid_price = book.best_bid_price,
                best_bid_size = book.best_bid_size,
                best_ask_price = book.best_ask_price,
                best_ask_size = book.best_ask_size,
                ts = book.timestamp_ms,
                "mid 监控 token 的订单簿快照诊断"
            );
        }
        WsMessage::PriceChange(price_change) => {
            for change in &price_change.price_changes {
                if change.asset_id != asset_id.as_ref() {
                    continue;
                }
                info!(
                    target: "order",
                    asset_id = %asset_id,
                    event_type = "price_change",
                    side = ?change.side,
                    level_price = %change.price,
                    level_size = ?change.size,
                    delta_best_bid = ?change.best_bid,
                    delta_best_ask = ?change.best_ask,
                    best_bid_price = book.best_bid_price,
                    best_bid_size = book.best_bid_size,
                    best_ask_price = book.best_ask_price,
                    best_ask_size = book.best_ask_size,
                    ts = book.timestamp_ms,
                    "mid 监控 token 的订单簿增量诊断"
                );
            }
        }
        _ => {}
    }
}

fn apply_market_message(
    books: &mut HashMap<String, LocalOrderbook>,
    msg: &WsMessage,
) -> Vec<(Arc<str>, CleanOrderbook)> {
    match msg {
        WsMessage::Book(book) => {
            let asset_id = book.asset_id.clone();
            let state = books.entry(asset_id.clone()).or_default();
            state
                .apply_book(book)
                .into_iter()
                .map(|clean| (Arc::from(asset_id.clone()), clean))
                .collect()
        }
        WsMessage::PriceChange(price_change) => {
            let Some(timestamp_ms) = u64::try_from(price_change.timestamp).ok() else {
                return Vec::new();
            };
            price_change
                .price_changes
                .iter()
                .filter_map(|change| {
                    let state = books.entry(change.asset_id.clone()).or_default();
                    state
                        .apply_price_change(change, timestamp_ms)
                        .map(|clean| (Arc::from(change.asset_id.as_str()), clean))
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

fn scale_price(price: Decimal) -> Option<u16> {
    let scaled = (price * Decimal::from(10_000u32)).round();
    let as_u32 = u32::try_from(scaled).ok()?;
    u16::try_from(as_u32).ok()
}

fn scale_size(size: Decimal) -> Option<u32> {
    let scaled = (size * Decimal::from(10_000u32)).round();
    u32::try_from(scaled).ok()
}
