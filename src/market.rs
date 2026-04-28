use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use polymarket_client_sdk::clob::types::Side;
use polymarket_client_sdk::clob::ws::types::response::{
    BookUpdate, PriceChangeBatchEntry, WsMessage,
};
use polymarket_client_sdk::types::Decimal;
use tracing::{info, warn};

use crate::monitor::FullBookSnapshot;
use crate::proxy_ws;
use crate::storage::OrderStore;
use crate::strategy::{CleanOrderbook, MarketEvent, StrategyEvent};

#[derive(Debug, Clone, Default)]
struct LocalOrderbook {
    bids: BTreeMap<u16, u32>,
    asks: BTreeMap<u16, u32>,
    timestamp_ms: u64,
    market: String,
}

impl LocalOrderbook {
    fn apply_book(&mut self, book: &BookUpdate) -> Option<CleanOrderbook> {
        self.bids.clear();
        self.asks.clear();
        self.market = book.market.clone();

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
        let proxy = proxy.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = proxy_ws::run(proxy.clone(), chunk.clone(), tx.clone()).await {
                    warn!(error = %e, "WS 连接断开，5 秒后重连");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        });
    }
}

pub async fn run_tick_recorder(
    mut rx: tokio::sync::mpsc::Receiver<(Arc<str>, CleanOrderbook)>,
    store: OrderStore,
) {
    const BATCH_SIZE: usize = 500;
    let flush_interval = Duration::from_millis(500);

    let mut buffer: Vec<(String, u16, u32, u16, u32, u64)> = Vec::with_capacity(BATCH_SIZE);
    // token -> (last_bid_price, last_ask_price)：只在 best price 变化时落盘
    let mut last_top: HashMap<String, (u16, u16)> = HashMap::new();
    let mut ticker = tokio::time::interval(flush_interval);
    ticker.tick().await; // 跳过立即触发的第一次

    loop {
        tokio::select! {
            biased;

            msg = rx.recv() => {
                let Some((token, book)) = msg else { break };
                let key = (book.best_bid_price, book.best_ask_price);
                let last = last_top.entry(token.to_string()).or_insert((0, 0));
                if *last != key {
                    *last = key;
                    buffer.push((
                        token.to_string(),
                        book.best_bid_price,
                        book.best_bid_size,
                        book.best_ask_price,
                        book.best_ask_size,
                        book.timestamp_ms,
                    ));
                    if buffer.len() >= BATCH_SIZE {
                        flush_ticks(&store, &mut buffer);
                    }
                }
            }

            _ = ticker.tick() => {
                if !buffer.is_empty() {
                    flush_ticks(&store, &mut buffer);
                }
            }
        }
    }

    if !buffer.is_empty() {
        flush_ticks(&store, &mut buffer);
    }
}

fn flush_ticks(store: &OrderStore, buffer: &mut Vec<(String, u16, u32, u16, u32, u64)>) {
    if let Err(e) = store.insert_market_ticks_batch(buffer) {
        warn!(error = %e, rows = buffer.len(), "market_ticks 批量写入失败");
    }
    buffer.clear();
}

pub(crate) enum RawStoreEvent {
    Book {
        token: String,
        market: String,
        bids: BTreeMap<u16, u32>,
        asks: BTreeMap<u16, u32>,
        ts_ms: u64,
    },
    Trade {
        token: String,
        market: String,
        price: String,
        side: Option<String>,
        size: Option<String>,
        fee_rate: Option<String>,
        ts_ms: i64,
    },
}

pub async fn run_raw_recorder(
    mut rx: tokio::sync::mpsc::Receiver<RawStoreEvent>,
    store: OrderStore,
) {
    while let Some(event) = rx.recv().await {
        match event {
            RawStoreEvent::Book { token, market, bids, asks, ts_ms } => {
                let bids_blob = pack_book(&bids);
                let asks_blob = pack_book(&asks);
                if let Err(e) = store.insert_book_snapshot(
                    &token, &market, &bids_blob, &asks_blob, ts_ms as i64,
                ) {
                    warn!(error = %e, "book_snapshots 写入失败");
                }
            }
            RawStoreEvent::Trade { token, market, price, side, size, fee_rate, ts_ms } => {
                if let Err(e) = store.insert_trade_event(
                    &token,
                    &market,
                    &price,
                    side.as_deref(),
                    size.as_deref(),
                    fee_rate.as_deref(),
                    ts_ms,
                ) {
                    warn!(error = %e, "trade_events 写入失败");
                }
            }
        }
    }
}

fn pack_book(book: &BTreeMap<u16, u32>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(book.len() * 6);
    for (&price, &size) in book {
        buf.extend_from_slice(&price.to_le_bytes());
        buf.extend_from_slice(&size.to_le_bytes());
    }
    buf
}

pub async fn run(
    token_topics: Arc<HashMap<String, Arc<[Arc<str>]>>>,
    liquidity_reward_tokens: Arc<std::collections::HashSet<String>>,
    mut ws_rx: tokio::sync::mpsc::Receiver<WsMessage>,
    market_tx: tokio::sync::mpsc::Sender<StrategyEvent>,
    monitor_tx: Option<tokio::sync::mpsc::Sender<FullBookSnapshot>>,
    tick_tx: Option<tokio::sync::mpsc::Sender<(Arc<str>, CleanOrderbook)>>,
    raw_store_tx: Option<tokio::sync::mpsc::Sender<RawStoreEvent>>,
) {
    let mut books: HashMap<String, LocalOrderbook> = HashMap::new();

    while let Some(msg) = ws_rx.recv().await {
        if let Some(ref tx) = raw_store_tx {
            if let WsMessage::LastTradePrice(ltp) = &msg {
                let _ = tx.try_send(RawStoreEvent::Trade {
                    token: ltp.asset_id.clone(),
                    market: ltp.market.clone(),
                    price: ltp.price.to_string(),
                    side: ltp.side.map(|s| format!("{s:?}")),
                    size: ltp.size.map(|s| s.to_string()),
                    fee_rate: ltp.fee_rate_bps.clone(),
                    ts_ms: ltp.timestamp,
                });
            }
        }

        let events = apply_market_message(&mut books, &msg);
        if events.is_empty() {
            continue;
        }

        for (asset_id, book) in events {
            log_liquidity_reward_orderbook_diagnostics(
                &msg,
                &asset_id,
                &book,
                liquidity_reward_tokens.as_ref(),
            );

            if let Some(ref tx) = tick_tx {
                let _ = tx.try_send((asset_id.clone(), book));
            }

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

            if let Some(ref tx) = raw_store_tx {
                if let Some(state) = books.get(asset_id.as_ref()) {
                    let _ = tx.try_send(RawStoreEvent::Book {
                        token: asset_id.to_string(),
                        market: state.market.clone(),
                        bids: state.bids.clone(),
                        asks: state.asks.clone(),
                        ts_ms: state.timestamp_ms,
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

fn log_liquidity_reward_orderbook_diagnostics(
    msg: &WsMessage,
    asset_id: &Arc<str>,
    book: &CleanOrderbook,
    liquidity_reward_tokens: &std::collections::HashSet<String>,
) {
    if !liquidity_reward_tokens.contains(asset_id.as_ref()) {
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
                "liquidity_reward 监控 token 的订单簿快照诊断"
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
                    "liquidity_reward 监控 token 的订单簿增量诊断"
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
                    if state.market.is_empty() {
                        state.market = price_change.market.clone();
                    }
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
