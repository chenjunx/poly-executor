use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt as _;
use polymarket_client_sdk::clob::ws::Client;
use polymarket_client_sdk::clob::ws::types::response::WsMessage;
use polymarket_client_sdk::types::Decimal;
use tracing::{info, warn};

use crate::proxy_ws;
use crate::strategy::{CleanOrderbook, MarketEvent, StrategyEvent};

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
            tokens.chunks(chunk_size).map(|c| c.to_vec()).collect::<Vec<_>>()
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
                    let mut orderbook_stream = Box::pin(orderbook_stream);
                    while let Some(result) = orderbook_stream.next().await {
                        if let Ok(msg) = result {
                            if tx.send(WsMessage::Book(msg)).await.is_err() {
                                return;
                            }
                        }
                    }
                    warn!("direct orderbook stream 断开，5 秒后重连");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            });
        }
    }
}

pub async fn run(
    token_topics: Arc<HashMap<String, Arc<[Arc<str>]>>>,
    mut ws_rx: tokio::sync::mpsc::Receiver<WsMessage>,
    market_tx: tokio::sync::mpsc::Sender<StrategyEvent>,
) {
    while let Some(msg) = ws_rx.recv().await {
        let Some((asset_id, book)) = extract_clean_orderbook(&msg) else {
            continue;
        };

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

fn extract_clean_orderbook(msg: &WsMessage) -> Option<(Arc<str>, CleanOrderbook)> {
    let WsMessage::Book(book) = msg else {
        return None;
    };

    let best_bid = book.bids.first()?;
    let best_ask = book.asks.first()?;

    Some((
        Arc::from(book.asset_id.as_str()),
        CleanOrderbook {
            best_bid_price: scale_price(best_bid.price)?,
            best_bid_size: scale_size(best_bid.size)?,
            best_ask_price: scale_price(best_ask.price)?,
            best_ask_size: scale_size(best_ask.size)?,
            timestamp_ms: u64::try_from(book.timestamp).ok()?,
        },
    ))
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
