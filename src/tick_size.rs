use std::str::FromStr;
use std::sync::Arc;

use alloy::primitives::U256;
use dashmap::DashMap;
use polymarket_client_sdk_v2::types::Decimal;
use tracing::{info, warn};

use crate::clob_client::build_authenticated_clob_client;
use crate::config::AuthConfig;

pub type TickSizeMap = Arc<DashMap<String, Decimal>>;

pub fn new_tick_size_map() -> TickSizeMap {
    Arc::new(DashMap::new())
}

/// 量化标准价格对齐：
/// - 买单（bid）向下取整：确保挂价不超出意图价，避免以更差价格被撮合
/// - 卖单（ask）向上取整：确保挂价不低于意图价，避免以更差价格被撮合
pub fn snap_price_to_tick(price: Decimal, tick: Decimal, is_buy: bool) -> Decimal {
    if tick <= Decimal::ZERO {
        return price;
    }
    let ratio = price / tick;
    let snapped = if is_buy { ratio.floor() } else { ratio.ceil() };
    snapped * tick
}

/// 启动时批量查询所有 token 的 tick_size，写入共享 map。
/// 若 CLOB 客户端构建失败或单个 token 查询失败，回退到默认值 0.01。
pub async fn load_for_tokens(tokens: &[String], auth: &AuthConfig, map: &TickSizeMap) {
    if tokens.is_empty() {
        return;
    }
    let default_tick = Decimal::try_from(0.01_f64).unwrap_or(Decimal::ONE);
    info!(token_count = tokens.len(), "正在初始化 tick_size 缓存");

    let client = match build_authenticated_clob_client(auth).await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, "tick_size 初始化：CLOB 客户端构建失败，全部使用默认值 0.01");
            for token in tokens {
                map.entry(token.clone()).or_insert(default_tick);
            }
            return;
        }
    };

    // 并发查询所有 token，总耗时 ≈ 单次请求时延（而非 N 倍）
    let futs = tokens.iter().map(|token| {
        let client = &client;
        let token_u256 = U256::from_str(token).unwrap_or_default();
        async move { (token, client.tick_size(token_u256).await) }
    });
    let results = futures::future::join_all(futs).await;

    let mut ok = 0usize;
    let mut failed = 0usize;
    for (token, result) in results {
        match result {
            Ok(resp) => {
                let tick = Decimal::from(resp.minimum_tick_size);
                info!(token = %token, tick = %tick, "tick_size 已加载");
                map.insert(token.clone(), tick);
                ok += 1;
            }
            Err(e) => {
                warn!(token = %token, error = %e, "tick_size 查询失败，使用默认值 0.01");
                map.entry(token.clone()).or_insert(default_tick);
                failed += 1;
            }
        }
    }
    info!(ok, failed, "tick_size 缓存初始化完成");
}
