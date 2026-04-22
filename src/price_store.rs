use std::sync::Arc;

use dashmap::DashMap;

use crate::strategy::CleanOrderbook;

#[derive(Debug, Clone)]
pub struct TokenPrice {
    pub asset_id: String,
    pub best_bid_price: Option<u16>,
    pub best_bid_size: Option<u32>,
    pub best_ask_price: Option<u16>,
    pub best_ask_size: Option<u32>,
    pub updated_at_ms: u64,
}

impl TokenPrice {
    fn new(asset_id: String) -> Self {
        Self {
            asset_id,
            best_bid_price: None,
            best_bid_size: None,
            best_ask_price: None,
            best_ask_size: None,
            updated_at_ms: 0,
        }
    }
}

/// 线程安全的内存价格缓存，可跨任务共享。
#[derive(Clone, Default)]
pub struct PriceStore {
    inner: Arc<DashMap<String, TokenPrice>>,
}

impl PriceStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, asset_ids: &[String]) {
        for id in asset_ids {
            self.inner
                .entry(id.clone())
                .or_insert_with(|| TokenPrice::new(id.clone()));
        }
    }

    pub fn apply(&self, asset_id: &str, book: CleanOrderbook) -> Vec<String> {
        let mut entry = self
            .inner
            .entry(asset_id.to_string())
            .or_insert_with(|| TokenPrice::new(asset_id.to_string()));

        if book.timestamp_ms >= entry.updated_at_ms {
            entry.best_bid_price = Some(book.best_bid_price);
            entry.best_bid_size = Some(book.best_bid_size);
            entry.best_ask_price = Some(book.best_ask_price);
            entry.best_ask_size = Some(book.best_ask_size);
            entry.updated_at_ms = book.timestamp_ms;
            return vec![asset_id.to_string()];
        }

        vec![]
    }

    pub fn get(&self, asset_id: &str) -> Option<TokenPrice> {
        self.inner.get(asset_id).map(|r| r.clone())
    }

    pub fn iter_all(&self) -> Vec<TokenPrice> {
        self.inner.iter().map(|r| r.clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}
