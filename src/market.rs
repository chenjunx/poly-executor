use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use dashmap::DashMap;

use log::{info, warn};
use polymarket_client_sdk_v2::clob::types::Side;
use polymarket_client_sdk_v2::clob::ws::types::response::{
    BookUpdate, PriceChangeBatchEntry, WsMessage,
};
use polymarket_client_sdk_v2::types::Decimal;

use crate::proxy_ws;
use crate::storage::MarketStore;
use crate::strategy::{CleanOrderbook, MarketAssetEvent, MarketEvent};
use crate::tick_size::TickSizeMap;

#[derive(Debug, Clone, Default)]
struct LocalOrderbook {
    bids: BTreeMap<u16, u32>,
    asks: BTreeMap<u16, u32>,
    timestamp_ms: u64,
    market: String,
}

pub(crate) struct MarketBookCache {
    books: HashMap<String, LocalOrderbook>,
}

#[derive(Debug, Clone)]
pub(crate) struct MarketAssetUpdate {
    pub asset_id: Arc<str>,
    pub book: Arc<CleanOrderbook>,
}

#[derive(Clone)]
pub struct MarketBookPublisher {
    books: Arc<DashMap<String, Arc<ArcSwapOption<CleanOrderbook>>>>,
}

#[derive(Clone)]
pub struct MarketBookReadHandle {
    books: Arc<DashMap<String, Arc<ArcSwapOption<CleanOrderbook>>>>,
}

impl MarketBookPublisher {
    pub fn new() -> Self {
        Self {
            books: Arc::new(DashMap::new()),
        }
    }

    pub fn read_handle(&self) -> MarketBookReadHandle {
        MarketBookReadHandle {
            books: self.books.clone(),
        }
    }

    pub fn publish(&self, asset_id: &str, book: Arc<CleanOrderbook>) {
        let cell = self
            .books
            .entry(asset_id.to_string())
            .or_insert_with(|| Arc::new(ArcSwapOption::empty()))
            .clone();
        cell.store(Some(book));
    }
}

impl MarketBookReadHandle {
    pub fn get(&self, asset_id: &str) -> Option<Arc<CleanOrderbook>> {
        self.books.get(asset_id).and_then(|cell| cell.load_full())
    }
}

impl MarketBookCache {
    pub(crate) fn new() -> Self {
        Self {
            books: HashMap::new(),
        }
    }

    pub(crate) fn apply(&mut self, msg: &WsMessage) -> Vec<(Arc<str>, CleanOrderbook)> {
        apply_market_message(&mut self.books, msg)
    }

    fn local_state(&self, asset_id: &str) -> Option<&LocalOrderbook> {
        self.books.get(asset_id)
    }
}

impl LocalOrderbook {
    fn apply_book(&mut self, book: &BookUpdate) -> Option<CleanOrderbook> {
        self.bids.clear();
        self.asks.clear();
        self.market = book.market.to_string();

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
            bids: Arc::new(self.bids.clone()),
            asks: Arc::new(self.asks.clone()),
        })
    }
}

pub(crate) fn build_unique_subscription_chunks(
    topic_groups: &HashMap<Arc<str>, Vec<String>>,
    default_threads: usize,
) -> Vec<Vec<String>> {
    let mut tokens: Vec<String> = topic_groups
        .values()
        .flat_map(|topic_tokens| topic_tokens.iter().cloned())
        .collect();
    tokens.sort();
    tokens.dedup();

    if tokens.is_empty() {
        return Vec::new();
    }

    let connection_count = default_threads.max(1).min(tokens.len());
    let chunk_size = tokens.len().div_ceil(connection_count);
    tokens
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

pub async fn spawn_subscriptions(
    topic_groups: &HashMap<Arc<str>, Vec<String>>,
    _topic_threads: &HashMap<String, usize>,
    default_threads: usize,
    proxy: Option<proxy_ws::Proxy>,
    tx: tokio::sync::mpsc::Sender<WsMessage>,
) {
    let all_chunks = build_unique_subscription_chunks(topic_groups, default_threads);
    let token_count: usize = all_chunks.iter().map(Vec::len).sum();
    info!(
        "全局唯一 token 订阅连接已分配 token_count={:?} connection_count={:?}",
        token_count,
        all_chunks.len()
    );

    for chunk in all_chunks {
        let tx = tx.clone();
        let proxy = proxy.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = proxy_ws::run(proxy.clone(), chunk.clone(), tx.clone()).await {
                    warn!("WS 连接断开，5 秒后重连 error={}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        });
    }
}

pub async fn run_tick_recorder(
    mut rx: tokio::sync::mpsc::Receiver<(Arc<str>, CleanOrderbook)>,
    store: MarketStore,
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

fn flush_ticks(store: &MarketStore, buffer: &mut Vec<(String, u16, u32, u16, u32, u64)>) {
    if let Err(e) = store.insert_market_ticks_batch(buffer) {
        warn!(
            "market_ticks 批量写入失败 error={} rows={:?}",
            e,
            buffer.len()
        );
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
    store: MarketStore,
) {
    while let Some(event) = rx.recv().await {
        match event {
            RawStoreEvent::Book {
                token,
                market,
                bids,
                asks,
                ts_ms,
            } => {
                let bids_blob = pack_book(&bids);
                let asks_blob = pack_book(&asks);
                if let Err(e) = store.insert_book_snapshot(
                    &token,
                    &market,
                    &bids_blob,
                    &asks_blob,
                    ts_ms as i64,
                ) {
                    warn!("book_snapshots 写入失败 error={}", e);
                }
            }
            RawStoreEvent::Trade {
                token,
                market,
                price,
                side,
                size,
                fee_rate,
                ts_ms,
            } => {
                if let Err(e) = store.insert_trade_event(
                    &token,
                    &market,
                    &price,
                    side.as_deref(),
                    size.as_deref(),
                    fee_rate.as_deref(),
                    ts_ms,
                ) {
                    warn!("trade_events 写入失败 error={}", e);
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

async fn publish_market_asset_update(
    update: MarketAssetUpdate,
    book_publisher: &MarketBookPublisher,
    firehose_tx: &tokio::sync::mpsc::Sender<MarketAssetEvent>,
    topic_txs: &HashMap<Arc<str>, tokio::sync::broadcast::Sender<MarketEvent>>,
) -> Result<(), tokio::sync::mpsc::error::SendError<MarketAssetEvent>> {
    book_publisher.publish(update.asset_id.as_ref(), update.book.clone());
    let topics = Arc::<[Arc<str>]>::from(vec![update.asset_id.clone()]);
    firehose_tx
        .send(MarketAssetEvent {
            asset_id: update.asset_id.clone(),
            topics,
            book: update.book.clone(),
        })
        .await?;

    if let Some(tx) = topic_txs.get(&update.asset_id) {
        let _ = tx.send(MarketEvent {
            topic: update.asset_id.clone(),
            asset_id: update.asset_id,
            book: update.book,
        });
    }

    Ok(())
}

pub async fn run(
    token_topics: Arc<HashMap<String, Arc<[Arc<str>]>>>,
    mut ws_rx: tokio::sync::mpsc::Receiver<WsMessage>,
    book_publisher: MarketBookPublisher,
    firehose_tx: tokio::sync::mpsc::Sender<MarketAssetEvent>,
    topic_txs: Arc<HashMap<Arc<str>, tokio::sync::broadcast::Sender<MarketEvent>>>,
    tick_tx: Option<tokio::sync::mpsc::Sender<(Arc<str>, CleanOrderbook)>>,
    raw_store_tx: Option<tokio::sync::mpsc::Sender<RawStoreEvent>>,
    tick_size_map: TickSizeMap,
) {
    let mut book_cache = MarketBookCache::new();

    while let Some(msg) = ws_rx.recv().await {
        if let WsMessage::TickSizeChange(change) = &msg {
            tick_size_map.insert(change.asset_id.to_string(), change.new_tick_size);
            continue;
        }

        if let Some(ref tx) = raw_store_tx {
            if let WsMessage::LastTradePrice(ltp) = &msg {
                let _ = tx.try_send(RawStoreEvent::Trade {
                    token: ltp.asset_id.to_string(),
                    market: ltp.market.to_string(),
                    price: ltp.price.to_string(),
                    side: ltp.side.map(|s| format!("{s:?}")),
                    size: ltp.size.map(|s| s.to_string()),
                    fee_rate: ltp.fee_rate_bps.map(|f| f.to_string()),
                    ts_ms: ltp.timestamp,
                });
            }
        }

        let events = book_cache.apply(&msg);
        if events.is_empty() {
            continue;
        }

        for (asset_id, book) in events {
            if let Some(ref tx) = tick_tx {
                let _ = tx.try_send((asset_id.clone(), book.clone()));
            }

            if let Some(ref tx) = raw_store_tx {
                if let Some(state) = book_cache.local_state(asset_id.as_ref()) {
                    let _ = tx.try_send(RawStoreEvent::Book {
                        token: asset_id.to_string(),
                        market: state.market.clone(),
                        bids: state.bids.clone(),
                        asks: state.asks.clone(),
                        ts_ms: state.timestamp_ms,
                    });
                }
            }

            if !token_topics.contains_key(asset_id.as_ref()) {
                continue;
            }
            let book = Arc::new(book);
            let update = MarketAssetUpdate {
                asset_id: asset_id.clone(),
                book: book.clone(),
            };
            if publish_market_asset_update(update, &book_publisher, &firehose_tx, &topic_txs)
                .await
                .is_err()
            {
                return;
            }
        }
    }
}

fn apply_market_message(
    books: &mut HashMap<String, LocalOrderbook>,
    msg: &WsMessage,
) -> Vec<(Arc<str>, CleanOrderbook)> {
    match msg {
        WsMessage::Book(book) => {
            let asset_id = book.asset_id.to_string();
            let state = books.entry(asset_id.clone()).or_default();
            state
                .apply_book(book)
                .into_iter()
                .map(|clean| (Arc::from(asset_id.as_str()), clean))
                .collect()
        }
        WsMessage::PriceChange(price_change) => {
            let Some(timestamp_ms) = u64::try_from(price_change.timestamp).ok() else {
                return Vec::new();
            };
            let market_str = price_change.market.to_string();
            price_change
                .price_changes
                .iter()
                .filter_map(|change| {
                    let asset_id = change.asset_id.to_string();
                    let state = books.entry(asset_id.clone()).or_default();
                    if state.market.is_empty() {
                        state.market = market_str.clone();
                    }
                    state
                        .apply_price_change(change, timestamp_ms)
                        .map(|clean| (Arc::from(asset_id.as_str()), clean))
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

const PRICE_SCALE: u32 = 10_000;

fn scale_price(price: Decimal) -> Option<u16> {
    let scaled = (price * Decimal::from(PRICE_SCALE)).round();
    let as_u32 = u32::try_from(scaled).ok()?;
    u16::try_from(as_u32).ok()
}

fn scale_size(size: Decimal) -> Option<u32> {
    let scaled = (size * Decimal::from(PRICE_SCALE)).round();
    u32::try_from(scaled).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic_groups() -> HashMap<Arc<str>, Vec<String>> {
        HashMap::from([
            (
                Arc::from("topic-a"),
                vec!["token-1".to_string(), "token-2".to_string()],
            ),
            (
                Arc::from("topic-b"),
                vec!["token-2".to_string(), "token-3".to_string()],
            ),
        ])
    }

    #[test]
    fn unique_subscription_chunks_deduplicate_tokens_across_topics() {
        let chunks = build_unique_subscription_chunks(&topic_groups(), 2);
        let mut tokens: Vec<String> = chunks.into_iter().flatten().collect();

        tokens.sort();

        assert_eq!(tokens, ["token-1", "token-2", "token-3"]);
    }

    #[test]
    fn unique_subscription_chunks_never_assign_token_to_multiple_connections() {
        let chunks = build_unique_subscription_chunks(&topic_groups(), 2);
        let mut tokens: Vec<String> = chunks.into_iter().flatten().collect();
        let total = tokens.len();

        tokens.sort();
        tokens.dedup();

        assert_eq!(tokens.len(), total);
    }

    #[test]
    fn unique_subscription_chunks_respect_global_connection_count() {
        let chunks = build_unique_subscription_chunks(&topic_groups(), 10);

        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|chunk| !chunk.is_empty()));
    }

    fn test_book(
        best_bid_price: u16,
        best_ask_price: u16,
        timestamp_ms: u64,
    ) -> Arc<CleanOrderbook> {
        Arc::new(CleanOrderbook {
            best_bid_price,
            best_bid_size: 100,
            best_ask_price,
            best_ask_size: 200,
            timestamp_ms,
            bids: Arc::new(BTreeMap::from([(best_bid_price, 100)])),
            asks: Arc::new(BTreeMap::from([(best_ask_price, 200)])),
        })
    }

    #[test]
    fn market_book_read_handle_reads_published_full_book() {
        let publisher = MarketBookPublisher::new();
        let read_handle = publisher.read_handle();
        let book = test_book(4000, 4100, 1);

        publisher.publish("token-1", book.clone());

        let published = read_handle.get("token-1").expect("book should publish");
        assert_eq!(published.best_bid_price, 4000);
        assert_eq!(published.best_ask_price, 4100);
        assert_eq!(published.timestamp_ms, 1);
        assert_eq!(published.bids.get(&4000), Some(&100));
        assert_eq!(published.asks.get(&4100), Some(&200));
    }

    #[test]
    fn market_book_read_handle_returns_latest_book_after_republish() {
        let publisher = MarketBookPublisher::new();
        let read_handle = publisher.read_handle();

        publisher.publish("token-1", test_book(4000, 4100, 1));
        publisher.publish("token-1", test_book(4200, 4300, 2));

        let published = read_handle.get("token-1").expect("book should publish");
        assert_eq!(published.best_bid_price, 4200);
        assert_eq!(published.best_ask_price, 4300);
        assert_eq!(published.timestamp_ms, 2);
    }

    #[test]
    fn market_book_read_handle_returns_none_for_unknown_asset() {
        let publisher = MarketBookPublisher::new();
        let read_handle = publisher.read_handle();

        assert!(read_handle.get("missing-token").is_none());
    }

    #[test]
    fn market_asset_event_carries_asset_token_topic_for_asset_update() {
        let book = test_book(4000, 4100, 1);
        let event = MarketAssetEvent {
            asset_id: Arc::from("token-1"),
            topics: Arc::from([Arc::from("token-1")]),
            book: book.clone(),
        };

        assert_eq!(event.asset_id.as_ref(), "token-1");
        assert_eq!(event.topics.as_ref(), &[Arc::from("token-1")]);
        assert!(Arc::ptr_eq(&event.book, &book));
    }

    #[tokio::test]
    async fn market_run_publishes_book_before_firehose_and_topic_event() {
        let publisher = MarketBookPublisher::new();
        let read_handle = publisher.read_handle();
        let (firehose_tx, mut firehose_rx) = tokio::sync::mpsc::channel(4);
        let (topic_tx, mut topic_rx) = tokio::sync::broadcast::channel(4);
        let topic_txs = HashMap::from([(Arc::from("token-1"), topic_tx)]);
        let book = test_book(4000, 4100, 1);

        publish_market_asset_update(
            MarketAssetUpdate {
                asset_id: Arc::from("token-1"),
                book: book.clone(),
            },
            &publisher,
            &firehose_tx,
            &topic_txs,
        )
        .await
        .expect("publish should succeed");

        let firehose_event = firehose_rx.recv().await.expect("firehose should receive");
        let latest = read_handle
            .get("token-1")
            .expect("book should publish first");
        assert!(Arc::ptr_eq(&latest, &book));
        assert!(Arc::ptr_eq(&firehose_event.book, &book));

        let topic_event = topic_rx.recv().await.expect("topic should receive");
        assert!(Arc::ptr_eq(&topic_event.book, &book));
    }

    #[tokio::test]
    async fn market_run_publishes_firehose_once_and_token_topic_event_once() {
        let publisher = MarketBookPublisher::new();
        let (firehose_tx, mut firehose_rx) = tokio::sync::mpsc::channel(4);
        let (topic_tx, mut topic_rx) = tokio::sync::broadcast::channel(4);
        let topic_txs = HashMap::from([(Arc::from("token-1"), topic_tx)]);
        let book = test_book(4000, 4100, 1);

        publish_market_asset_update(
            MarketAssetUpdate {
                asset_id: Arc::from("token-1"),
                book: book.clone(),
            },
            &publisher,
            &firehose_tx,
            &topic_txs,
        )
        .await
        .expect("publish should succeed");

        let firehose_event = firehose_rx.recv().await.expect("firehose should receive");
        assert_eq!(firehose_event.topics.as_ref(), &[Arc::from("token-1")]);
        assert!(firehose_rx.try_recv().is_err());
        assert_eq!(
            topic_rx
                .recv()
                .await
                .expect("token topic should receive")
                .topic
                .as_ref(),
            "token-1"
        );
        assert!(topic_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn market_run_topic_send_without_subscriber_does_not_stop_reducer() {
        let publisher = MarketBookPublisher::new();
        let (firehose_tx, mut firehose_rx) = tokio::sync::mpsc::channel(4);
        let (topic_tx, _topic_rx) = tokio::sync::broadcast::channel(4);
        let topic_txs = HashMap::from([(Arc::from("token-1"), topic_tx)]);
        drop(_topic_rx);

        publish_market_asset_update(
            MarketAssetUpdate {
                asset_id: Arc::from("token-1"),
                book: test_book(4000, 4100, 1),
            },
            &publisher,
            &firehose_tx,
            &topic_txs,
        )
        .await
        .expect("publish should ignore missing topic subscribers");

        assert_eq!(
            firehose_rx
                .recv()
                .await
                .expect("firehose should receive")
                .asset_id
                .as_ref(),
            "token-1"
        );
    }
}
