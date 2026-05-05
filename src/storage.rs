use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use chrono::NaiveDate;
use polymarket_client_sdk_v2::types::Decimal;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use crate::strategy::{LocalOrderMeta, QuoteSide};

#[derive(Clone)]
pub struct OrderStore {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Clone)]
pub struct MarketStore {
    conn: Arc<Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct StoredOrder {
    pub local_order_id: String,
    pub remote_order_id: Option<String>,
    pub strategy: String,
    pub topic: Option<String>,
    pub token: String,
    pub side: QuoteSide,
    pub price: Decimal,
    pub order_size: Decimal,
    pub status: String,
    pub last_mid: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub struct StoredLiquidityRewardSharedState {
    pub token: String,
    pub topic: String,
    pub last_mid: Option<Decimal>,
    pub last_best_bid: Option<Decimal>,
    pub last_best_ask: Option<Decimal>,
    pub last_position_size: Decimal,
}

#[derive(Debug, Clone)]
pub struct StoredLiquidityRewardSideState {
    pub token: String,
    pub side: QuoteSide,
    pub active_local_order_id: Option<String>,
    pub pending_local_order_id: Option<String>,
    pub pending_price: Option<Decimal>,
    pub pending_order_size: Option<Decimal>,
    pub pending_mid: Option<Decimal>,
    pub last_quoted_mid: Option<Decimal>,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone)]
pub struct StoredRewardMarketPoolState {
    pub condition_id: String,
    pub market_slug: Option<String>,
    pub question: Option<String>,
    pub token1: String,
    pub token2: String,
    pub in_pool: bool,
    pub kicked_at_ms: Option<u64>,
    pub kick_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemovedLiquidityRewardPoolEntry {
    pub condition_id: String,
    pub token1: String,
    pub token2: String,
}

#[derive(Debug, Clone)]
pub struct RewardMarketPoolReplaceResult {
    pub selected_count: usize,
    pub removed_selected_entries: Vec<RemovedLiquidityRewardPoolEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredRewardMarketPoolMeta {
    pub build_date_utc: NaiveDate,
    pub version: u64,
    pub built_at_ms: u64,
}

pub struct RewardMarketPoolStorageEntry<'a> {
    pub condition_id: &'a str,
    pub market_slug: Option<&'a str>,
    pub question: Option<&'a str>,
    pub token1: &'a str,
    pub token2: &'a str,
    pub tokens_json: &'a str,
    pub market_competitiveness: Option<&'a str>,
    pub rewards_min_size: Option<&'a str>,
    pub rewards_max_spread: Option<&'a str>,
    pub market_daily_reward: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct ActiveRewardMarketPoolEntry {
    pub condition_id: String,
    pub market_slug: Option<String>,
    pub question: Option<String>,
    pub token1: String,
    pub token2: String,
    pub tokens_json: String,
    pub market_competitiveness: Option<String>,
    pub rewards_min_size: Option<String>,
    pub rewards_max_spread: Option<String>,
    pub market_daily_reward: Option<String>,
    pub build_date_utc: Option<String>,
    pub pool_version: Option<u64>,
    pub liquidity_reward_selected: bool,
    pub liquidity_reward_selected_at_ms: Option<u64>,
    pub liquidity_reward_select_reason: Option<String>,
    pub liquidity_reward_select_rank: Option<u32>,
    pub liquidity_reward_halted: bool,
    pub liquidity_reward_halted_at_ms: Option<u64>,
    pub liquidity_reward_halt_reason: Option<String>,
    pub liquidity_reward_halted_pool_version: Option<u64>,
}

impl StoredOrder {
    pub fn to_local_order_meta(&self) -> LocalOrderMeta {
        LocalOrderMeta {
            local_order_id: self.local_order_id.clone(),
            remote_order_id: self.remote_order_id.clone(),
            strategy: Arc::from(self.strategy.clone()),
            topic: self.topic.as_ref().map(|topic| Arc::from(topic.as_str())),
            token: self.token.clone(),
            side: self.side,
            price: self.price,
            order_size: self.order_size,
        }
    }
}

impl OrderStore {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        Ok(Self {
            conn: Arc::new(Mutex::new(open_sqlite_connection(path)?)),
        })
    }

    pub fn init_schema(&self) -> anyhow::Result<()> {
        self.with_conn(|conn| {
            conn.execute_batch(
                "
                CREATE TABLE IF NOT EXISTS orders (
                    local_order_id TEXT PRIMARY KEY,
                    remote_order_id TEXT UNIQUE,
                    strategy TEXT NOT NULL,
                    topic TEXT,
                    token TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price TEXT NOT NULL,
                    min_order_size TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_mid TEXT,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS order_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    local_order_id TEXT,
                    remote_order_id TEXT,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    event_ts_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS strategy_state_mid_requote (
                    token TEXT PRIMARY KEY,
                    topic TEXT NOT NULL,
                    active_local_order_id TEXT,
                    pending_local_order_id TEXT,
                    pending_side TEXT,
                    pending_price TEXT,
                    pending_order_size TEXT,
                    pending_mid TEXT,
                    last_mid TEXT,
                    last_best_bid TEXT,
                    last_best_ask TEXT,
                    last_position_size TEXT NOT NULL,
                    updated_at_ms INTEGER NOT NULL
                );


                CREATE TABLE IF NOT EXISTS strategy_state_mid_requote_side (
                    token TEXT NOT NULL,
                    side TEXT NOT NULL,
                    active_local_order_id TEXT,
                    pending_local_order_id TEXT,
                    pending_price TEXT,
                    pending_order_size TEXT,
                    pending_mid TEXT,
                    last_quoted_mid TEXT,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (token, side)
                );
                ",
            )?;
            ensure_column(
                conn,
                "strategy_state_mid_requote",
                "pending_local_order_id",
                "TEXT",
            )?;
            ensure_column(conn, "strategy_state_mid_requote", "pending_side", "TEXT")?;
            ensure_column(conn, "strategy_state_mid_requote", "pending_price", "TEXT")?;
            ensure_column(
                conn,
                "strategy_state_mid_requote",
                "pending_order_size",
                "TEXT",
            )?;
            ensure_column(conn, "strategy_state_mid_requote", "pending_mid", "TEXT")?;
            migrate_liquidity_reward_side_state(conn)?;
            Ok(())
        })
    }

    pub fn upsert_order(
        &self,
        meta: &LocalOrderMeta,
        status: &str,
        last_mid: Option<Decimal>,
    ) -> anyhow::Result<()> {
        let now = now_ms()?;
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO orders (
                    local_order_id, remote_order_id, strategy, topic, token, side, price,
                    min_order_size, status, last_mid, created_at_ms, updated_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                ON CONFLICT(local_order_id) DO UPDATE SET
                    remote_order_id = COALESCE(excluded.remote_order_id, orders.remote_order_id),
                    strategy = excluded.strategy,
                    topic = excluded.topic,
                    token = excluded.token,
                    side = excluded.side,
                    price = excluded.price,
                    min_order_size = excluded.min_order_size,
                    status = excluded.status,
                    last_mid = COALESCE(excluded.last_mid, orders.last_mid),
                    updated_at_ms = excluded.updated_at_ms
                ",
                params![
                    meta.local_order_id,
                    meta.remote_order_id,
                    meta.strategy.as_ref(),
                    meta.topic.as_ref().map(|topic| topic.as_ref()),
                    meta.token,
                    side_to_str(meta.side),
                    meta.price.to_string(),
                    meta.order_size.to_string(),
                    status,
                    last_mid.map(|value| value.to_string()),
                    now,
                    now,
                ],
            )?;
            Ok(())
        })
    }

    pub fn update_order_remote_and_status(
        &self,
        local_order_id: &str,
        remote_order_id: &str,
        status: &str,
        last_mid: Option<Decimal>,
    ) -> anyhow::Result<()> {
        let now = now_ms()?;
        self.with_conn(|conn| {
            conn.execute(
                "
                UPDATE orders
                SET remote_order_id = ?2,
                    status = ?3,
                    last_mid = COALESCE(?4, last_mid),
                    updated_at_ms = ?5
                WHERE local_order_id = ?1
                ",
                params![
                    local_order_id,
                    remote_order_id,
                    status,
                    last_mid.map(|value| value.to_string()),
                    now,
                ],
            )?;
            Ok(())
        })
    }

    pub fn update_order_status_by_local(
        &self,
        local_order_id: &str,
        status: &str,
    ) -> anyhow::Result<()> {
        let now = now_ms()?;
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE orders SET status = ?2, updated_at_ms = ?3 WHERE local_order_id = ?1",
                params![local_order_id, status, now],
            )?;
            Ok(())
        })
    }

    pub fn update_order_status_by_remote(
        &self,
        remote_order_id: &str,
        status: &str,
    ) -> anyhow::Result<()> {
        let now = now_ms()?;
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE orders SET status = ?2, updated_at_ms = ?3 WHERE remote_order_id = ?1",
                params![remote_order_id, status, now],
            )?;
            Ok(())
        })
    }

    pub fn append_order_event(
        &self,
        local_order_id: Option<&str>,
        remote_order_id: Option<&str>,
        event_type: &str,
        payload: Value,
    ) -> anyhow::Result<()> {
        let now = now_ms()?;
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO order_events (local_order_id, remote_order_id, event_type, payload_json, event_ts_ms)
                VALUES (?1, ?2, ?3, ?4, ?5)
                ",
                params![local_order_id, remote_order_id, event_type, payload.to_string(), now],
            )?;
            Ok(())
        })
    }

    pub fn upsert_liquidity_reward_state(
        &self,
        token: &str,
        topic: &str,
        active_local_order_id: Option<&str>,
        pending_local_order_id: Option<&str>,
        pending_side: Option<QuoteSide>,
        pending_price: Option<Decimal>,
        pending_order_size: Option<Decimal>,
        pending_mid: Option<Decimal>,
        last_mid: Option<Decimal>,
        last_best_bid: Option<Decimal>,
        last_best_ask: Option<Decimal>,
        last_position_size: Decimal,
    ) -> anyhow::Result<()> {
        let now = now_ms()?;
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO strategy_state_mid_requote (
                    token, topic, active_local_order_id, pending_local_order_id, pending_side,
                    pending_price, pending_order_size, pending_mid, last_mid, last_best_bid,
                    last_best_ask, last_position_size, updated_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                ON CONFLICT(token) DO UPDATE SET
                    topic = excluded.topic,
                    active_local_order_id = excluded.active_local_order_id,
                    pending_local_order_id = excluded.pending_local_order_id,
                    pending_side = excluded.pending_side,
                    pending_price = excluded.pending_price,
                    pending_order_size = excluded.pending_order_size,
                    pending_mid = excluded.pending_mid,
                    last_mid = excluded.last_mid,
                    last_best_bid = excluded.last_best_bid,
                    last_best_ask = excluded.last_best_ask,
                    last_position_size = excluded.last_position_size,
                    updated_at_ms = excluded.updated_at_ms
                ",
                params![
                    token,
                    topic,
                    active_local_order_id,
                    pending_local_order_id,
                    pending_side.map(side_to_str),
                    pending_price.map(|value| value.to_string()),
                    pending_order_size.map(|value| value.to_string()),
                    pending_mid.map(|value| value.to_string()),
                    last_mid.map(|value| value.to_string()),
                    last_best_bid.map(|value| value.to_string()),
                    last_best_ask.map(|value| value.to_string()),
                    last_position_size.to_string(),
                    now,
                ],
            )?;
            Ok(())
        })
    }

    pub fn upsert_liquidity_reward_shared_state(
        &self,
        token: &str,
        topic: &str,
        last_mid: Option<Decimal>,
        last_best_bid: Option<Decimal>,
        last_best_ask: Option<Decimal>,
        last_position_size: Decimal,
    ) -> anyhow::Result<()> {
        self.upsert_liquidity_reward_state(
            token,
            topic,
            None,
            None,
            None,
            None,
            None,
            None,
            last_mid,
            last_best_bid,
            last_best_ask,
            last_position_size,
        )
    }

    pub fn upsert_liquidity_reward_side_state(
        &self,
        token: &str,
        side: QuoteSide,
        active_local_order_id: Option<&str>,
        pending_local_order_id: Option<&str>,
        pending_price: Option<Decimal>,
        pending_order_size: Option<Decimal>,
        pending_mid: Option<Decimal>,
        last_quoted_mid: Option<Decimal>,
        cancel_requested: bool,
    ) -> anyhow::Result<()> {
        let now = now_ms()?;
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO strategy_state_mid_requote_side (
                    token, side, active_local_order_id, pending_local_order_id, pending_price,
                    pending_order_size, pending_mid, last_quoted_mid, cancel_requested, updated_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                ON CONFLICT(token, side) DO UPDATE SET
                    active_local_order_id = excluded.active_local_order_id,
                    pending_local_order_id = excluded.pending_local_order_id,
                    pending_price = excluded.pending_price,
                    pending_order_size = excluded.pending_order_size,
                    pending_mid = excluded.pending_mid,
                    last_quoted_mid = excluded.last_quoted_mid,
                    cancel_requested = excluded.cancel_requested,
                    updated_at_ms = excluded.updated_at_ms
                ",
                params![
                    token,
                    side_to_str(side),
                    active_local_order_id,
                    pending_local_order_id,
                    pending_price.map(|value| value.to_string()),
                    pending_order_size.map(|value| value.to_string()),
                    pending_mid.map(|value| value.to_string()),
                    last_quoted_mid.map(|value| value.to_string()),
                    if cancel_requested { 1_i64 } else { 0_i64 },
                    now,
                ],
            )?;
            Ok(())
        })
    }

    pub fn load_active_orders(&self) -> anyhow::Result<Vec<StoredOrder>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "
                SELECT local_order_id, remote_order_id, strategy, topic, token, side, price,
                       min_order_size, status, last_mid
                FROM orders
                WHERE status NOT IN ('filled', 'canceled', 'rejected', 'failed', 'unknown')
                  AND (remote_order_id IS NOT NULL OR status IN ('open', 'pending_submit'))
",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(StoredOrder {
                    local_order_id: row.get(0)?,
                    remote_order_id: row.get(1)?,
                    strategy: row.get(2)?,
                    topic: row.get(3)?,
                    token: row.get(4)?,
                    side: side_from_str(&row.get::<_, String>(5)?).map_err(to_sql_error)?,
                    price: decimal_from_str(&row.get::<_, String>(6)?).map_err(to_sql_error)?,
                    order_size: decimal_from_str(&row.get::<_, String>(7)?)
                        .map_err(to_sql_error)?,
                    status: row.get(8)?,
                    last_mid: row
                        .get::<_, Option<String>>(9)?
                        .map(|value| decimal_from_str(&value).map_err(to_sql_error))
                        .transpose()?,
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
    }

    pub fn load_liquidity_reward_shared_states(
        &self,
    ) -> anyhow::Result<Vec<StoredLiquidityRewardSharedState>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "
                SELECT token, topic, last_mid, last_best_bid, last_best_ask, last_position_size
                FROM strategy_state_mid_requote
                ",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(StoredLiquidityRewardSharedState {
                    token: row.get(0)?,
                    topic: row.get(1)?,
                    last_mid: row
                        .get::<_, Option<String>>(2)?
                        .map(|value| decimal_from_str(&value).map_err(to_sql_error))
                        .transpose()?,
                    last_best_bid: row
                        .get::<_, Option<String>>(3)?
                        .map(|value| decimal_from_str(&value).map_err(to_sql_error))
                        .transpose()?,
                    last_best_ask: row
                        .get::<_, Option<String>>(4)?
                        .map(|value| decimal_from_str(&value).map_err(to_sql_error))
                        .transpose()?,
                    last_position_size: decimal_from_str(&row.get::<_, String>(5)?)
                        .map_err(to_sql_error)?,
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
    }

    pub fn load_liquidity_reward_side_states(
        &self,
    ) -> anyhow::Result<Vec<StoredLiquidityRewardSideState>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "
                SELECT token, side, active_local_order_id, pending_local_order_id, pending_price,
                       pending_order_size, pending_mid, last_quoted_mid, cancel_requested
                FROM strategy_state_mid_requote_side
                ",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(StoredLiquidityRewardSideState {
                    token: row.get(0)?,
                    side: side_from_str(&row.get::<_, String>(1)?).map_err(to_sql_error)?,
                    active_local_order_id: row.get(2)?,
                    pending_local_order_id: row.get(3)?,
                    pending_price: row
                        .get::<_, Option<String>>(4)?
                        .map(|value| decimal_from_str(&value).map_err(to_sql_error))
                        .transpose()?,
                    pending_order_size: row
                        .get::<_, Option<String>>(5)?
                        .map(|value| decimal_from_str(&value).map_err(to_sql_error))
                        .transpose()?,
                    pending_mid: row
                        .get::<_, Option<String>>(6)?
                        .map(|value| decimal_from_str(&value).map_err(to_sql_error))
                        .transpose()?,
                    last_quoted_mid: row
                        .get::<_, Option<String>>(7)?
                        .map(|value| decimal_from_str(&value).map_err(to_sql_error))
                        .transpose()?,
                    cancel_requested: row.get::<_, i64>(8)? != 0,
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
    }

    pub fn find_order_by_remote(
        &self,
        remote_order_id: &str,
    ) -> anyhow::Result<Option<StoredOrder>> {
        self.with_conn(|conn| {
            conn.query_row(
                "
                SELECT local_order_id, remote_order_id, strategy, topic, token, side, price,
                       min_order_size, status, last_mid
                FROM orders
                WHERE remote_order_id = ?1
                ",
                params![remote_order_id],
                |row| {
                    Ok(StoredOrder {
                        local_order_id: row.get(0)?,
                        remote_order_id: row.get(1)?,
                        strategy: row.get(2)?,
                        topic: row.get(3)?,
                        token: row.get(4)?,
                        side: side_from_str(&row.get::<_, String>(5)?).map_err(to_sql_error)?,
                        price: decimal_from_str(&row.get::<_, String>(6)?).map_err(to_sql_error)?,
                        order_size: decimal_from_str(&row.get::<_, String>(7)?)
                            .map_err(to_sql_error)?,
                        status: row.get(8)?,
                        last_mid: row
                            .get::<_, Option<String>>(9)?
                            .map(|value| decimal_from_str(&value).map_err(to_sql_error))
                            .transpose()?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn last_ws_size_matched_by_remote(
        &self,
        remote_order_id: &str,
    ) -> anyhow::Result<Option<Decimal>> {
        self.with_conn(|conn| {
            let payload = conn
                .query_row(
                    "
                    SELECT payload_json
                    FROM order_events
                    WHERE remote_order_id = ?1 AND event_type = 'ws_update'
                    ORDER BY event_ts_ms DESC, id DESC
                    LIMIT 1
                    ",
                    params![remote_order_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;

            payload
                .and_then(|payload| {
                    serde_json::from_str::<Value>(&payload)
                        .ok()
                        .and_then(|value| value.get("size_matched").cloned())
                        .and_then(|value| match value {
                            Value::String(value) => Some(value),
                            Value::Number(value) => Some(value.to_string()),
                            _ => None,
                        })
                })
                .map(|value| decimal_from_str(&value))
                .transpose()
        })
    }

    fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> anyhow::Result<T>) -> anyhow::Result<T> {
        let guard = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite 连接锁已中毒"))?;
        f(&guard)
    }
}

impl MarketStore {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        Ok(Self {
            conn: Arc::new(Mutex::new(open_sqlite_connection(path)?)),
        })
    }

    pub fn init_schema(&self) -> anyhow::Result<()> {
        self.with_conn(|conn| {
            conn.execute_batch(
                "
                CREATE TABLE IF NOT EXISTS liquidity_reward_scores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token TEXT NOT NULL,
                    mid TEXT NOT NULL,
                    my_orders INTEGER NOT NULL,
                    my_qone TEXT NOT NULL,
                    my_qtwo TEXT NOT NULL,
                    my_qmin TEXT NOT NULL,
                    competitors_qmin TEXT NOT NULL,
                    my_share TEXT NOT NULL,
                    estimated_daily_reward TEXT NOT NULL,
                    simulation INTEGER NOT NULL DEFAULT 0,
                    recorded_at_ms INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_lr_scores_token_ts
                    ON liquidity_reward_scores (token, recorded_at_ms DESC);

                CREATE TABLE IF NOT EXISTS market_ticks (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    token     TEXT    NOT NULL,
                    bid_price INTEGER NOT NULL,
                    bid_size  INTEGER NOT NULL,
                    ask_price INTEGER NOT NULL,
                    ask_size  INTEGER NOT NULL,
                    ts_ms     INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_market_ticks_token_ts
                    ON market_ticks (token, ts_ms DESC);

                CREATE TABLE IF NOT EXISTS book_snapshots (
                    id     INTEGER PRIMARY KEY AUTOINCREMENT,
                    token  TEXT    NOT NULL,
                    market TEXT    NOT NULL,
                    bids   BLOB    NOT NULL,
                    asks   BLOB    NOT NULL,
                    ts_ms  INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_book_snapshots_token_ts
                    ON book_snapshots (token, ts_ms DESC);

                CREATE TABLE IF NOT EXISTS trade_events (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    token    TEXT NOT NULL,
                    market   TEXT NOT NULL,
                    price    TEXT NOT NULL,
                    side     TEXT,
                    size     TEXT,
                    fee_rate TEXT,
                    ts_ms    INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_trade_events_token_ts
                    ON trade_events (token, ts_ms DESC);

                CREATE TABLE IF NOT EXISTS reward_market_pool_state (
                    condition_id TEXT PRIMARY KEY,
                    market_slug TEXT,
                    question TEXT,
                    token1 TEXT NOT NULL,
                    token2 TEXT NOT NULL,
                    tokens_json TEXT NOT NULL,
                    market_competitiveness TEXT,
                    rewards_min_size TEXT,
                    rewards_max_spread TEXT,
                    market_daily_reward TEXT,
                    build_date_utc TEXT,
                    pool_version INTEGER,
                    liquidity_reward_selected INTEGER NOT NULL DEFAULT 0,
                    liquidity_reward_selected_at_ms INTEGER,
                    liquidity_reward_select_reason TEXT,
                    liquidity_reward_select_rank INTEGER,
                    liquidity_reward_halted INTEGER NOT NULL DEFAULT 0,
                    liquidity_reward_halted_at_ms INTEGER,
                    liquidity_reward_halt_reason TEXT,
                    liquidity_reward_halted_pool_version INTEGER,
                    in_pool INTEGER NOT NULL DEFAULT 1,
                    first_seen_at_ms INTEGER NOT NULL,
                    last_seen_at_ms INTEGER NOT NULL,
                    last_token1_best_bid TEXT,
                    last_token1_best_ask TEXT,
                    last_token1_spread TEXT,
                    last_checked_at_ms INTEGER,
                    kicked_at_ms INTEGER,
                    kick_reason TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_reward_market_pool_state_in_pool
                    ON reward_market_pool_state (in_pool, last_seen_at_ms DESC);

                CREATE INDEX IF NOT EXISTS idx_reward_market_pool_state_kicked_at
                    ON reward_market_pool_state (kicked_at_ms DESC);
                ",
            )?;
            ensure_column(conn, "reward_market_pool_state", "build_date_utc", "TEXT")?;
            ensure_column(conn, "reward_market_pool_state", "pool_version", "INTEGER")?;
            ensure_column(conn, "reward_market_pool_state", "rewards_min_size", "TEXT")?;
            ensure_column(
                conn,
                "reward_market_pool_state",
                "rewards_max_spread",
                "TEXT",
            )?;
            ensure_column(
                conn,
                "reward_market_pool_state",
                "market_daily_reward",
                "TEXT",
            )?;
            ensure_column(
                conn,
                "reward_market_pool_state",
                "liquidity_reward_selected",
                "INTEGER NOT NULL DEFAULT 0",
            )?;
            ensure_column(
                conn,
                "reward_market_pool_state",
                "liquidity_reward_selected_at_ms",
                "INTEGER",
            )?;
            ensure_column(
                conn,
                "reward_market_pool_state",
                "liquidity_reward_select_reason",
                "TEXT",
            )?;
            ensure_column(
                conn,
                "reward_market_pool_state",
                "liquidity_reward_select_rank",
                "INTEGER",
            )?;
            ensure_column(
                conn,
                "reward_market_pool_state",
                "liquidity_reward_halted",
                "INTEGER NOT NULL DEFAULT 0",
            )?;
            ensure_column(
                conn,
                "reward_market_pool_state",
                "liquidity_reward_halted_at_ms",
                "INTEGER",
            )?;
            ensure_column(
                conn,
                "reward_market_pool_state",
                "liquidity_reward_halt_reason",
                "TEXT",
            )?;
            ensure_column(
                conn,
                "reward_market_pool_state",
                "liquidity_reward_halted_pool_version",
                "INTEGER",
            )?;
            Ok(())
        })
    }

    pub fn insert_liquidity_reward_score(
        &self,
        token: &str,
        mid: f64,
        my_orders: usize,
        my_qone: f64,
        my_qtwo: f64,
        my_qmin: f64,
        competitors_qmin: f64,
        my_share: f64,
        estimated_daily_reward: f64,
        simulation: bool,
    ) -> anyhow::Result<()> {
        let now = now_ms()?;
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO liquidity_reward_scores (
                    token, mid, my_orders, my_qone, my_qtwo, my_qmin,
                    competitors_qmin, my_share, estimated_daily_reward,
                    simulation, recorded_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                ",
                params![
                    token,
                    mid.to_string(),
                    my_orders as i64,
                    my_qone.to_string(),
                    my_qtwo.to_string(),
                    my_qmin.to_string(),
                    competitors_qmin.to_string(),
                    my_share.to_string(),
                    estimated_daily_reward.to_string(),
                    if simulation { 1_i64 } else { 0_i64 },
                    now,
                ],
            )?;
            Ok(())
        })
    }

    pub fn insert_book_snapshot(
        &self,
        token: &str,
        market: &str,
        bids: &[u8],
        asks: &[u8],
        ts_ms: i64,
    ) -> anyhow::Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO book_snapshots (token, market, bids, asks, ts_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![token, market, bids, asks, ts_ms],
            )?;
            Ok(())
        })
    }

    pub fn insert_trade_event(
        &self,
        token: &str,
        market: &str,
        price: &str,
        side: Option<&str>,
        size: Option<&str>,
        fee_rate: Option<&str>,
        ts_ms: i64,
    ) -> anyhow::Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO trade_events (token, market, price, side, size, fee_rate, ts_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![token, market, price, side, size, fee_rate, ts_ms],
            )?;
            Ok(())
        })
    }

    pub fn insert_market_ticks_batch(
        &self,
        ticks: &[(String, u16, u32, u16, u32, u64)],
    ) -> anyhow::Result<usize> {
        if ticks.is_empty() {
            return Ok(0);
        }
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN")?;
            let mut stmt = conn.prepare_cached(
                "INSERT INTO market_ticks (token, bid_price, bid_size, ask_price, ask_size, ts_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            let mut count = 0usize;
            for (token, bid_price, bid_size, ask_price, ask_size, ts_ms) in ticks {
                stmt.execute(params![
                    token,
                    *bid_price as i64,
                    *bid_size as i64,
                    *ask_price as i64,
                    *ask_size as i64,
                    *ts_ms as i64,
                ])?;
                count += 1;
            }
            conn.execute_batch("COMMIT")?;
            Ok(count)
        })
    }

    pub fn replace_reward_market_pool_entries(
        &self,
        build_date_utc: NaiveDate,
        pool_version: u64,
        entries: &[RewardMarketPoolStorageEntry<'_>],
        now_ms: u64,
        liquidity_reward_market_count: usize,
    ) -> anyhow::Result<RewardMarketPoolReplaceResult> {
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN")?;
            let result = (|| -> anyhow::Result<RewardMarketPoolReplaceResult> {
                let removed_selected_entries =
                    load_selected_entries_missing_from_next_pool(conn, entries)?;
                conn.execute("DELETE FROM reward_market_pool_state", [])?;
                let mut stmt = conn.prepare_cached(
                    "
                    INSERT INTO reward_market_pool_state (
                        condition_id, market_slug, question, token1, token2, tokens_json,
                        market_competitiveness, rewards_min_size, rewards_max_spread,
                        market_daily_reward, build_date_utc, pool_version, in_pool,
                        first_seen_at_ms, last_seen_at_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 1, ?13, ?13)
                    ",
                )?;
                let build_date_utc = build_date_utc.to_string();
                for entry in entries {
                    stmt.execute(params![
                        entry.condition_id,
                        entry.market_slug,
                        entry.question,
                        entry.token1,
                        entry.token2,
                        entry.tokens_json,
                        entry.market_competitiveness,
                        entry.rewards_min_size,
                        entry.rewards_max_spread,
                        entry.market_daily_reward,
                        build_date_utc,
                        pool_version as i64,
                        now_ms as i64,
                    ])?;
                }
                let selected_count = mark_liquidity_reward_pool_selection_in_conn(
                    conn,
                    entries,
                    liquidity_reward_market_count,
                    now_ms,
                )?;
                Ok(RewardMarketPoolReplaceResult {
                    selected_count,
                    removed_selected_entries,
                })
            })();
            match result {
                Ok(replace_result) => {
                    conn.execute_batch("COMMIT")?;
                    Ok(replace_result)
                }
                Err(error) => {
                    let _ = conn.execute_batch("ROLLBACK");
                    Err(error)
                }
            }
        })
    }

    pub fn upsert_reward_market_pool_entry(
        &self,
        condition_id: &str,
        market_slug: Option<&str>,
        question: Option<&str>,
        token1: &str,
        token2: &str,
        tokens_json: &str,
        market_competitiveness: Option<&str>,
        now_ms: u64,
    ) -> anyhow::Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO reward_market_pool_state (
                    condition_id, market_slug, question, token1, token2, tokens_json,
                    market_competitiveness, in_pool, first_seen_at_ms, last_seen_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, ?8)
                ON CONFLICT(condition_id) DO UPDATE SET
                    market_slug = excluded.market_slug,
                    question = excluded.question,
                    token1 = excluded.token1,
                    token2 = excluded.token2,
                    tokens_json = excluded.tokens_json,
                    market_competitiveness = excluded.market_competitiveness,
                    in_pool = CASE
                        WHEN reward_market_pool_state.kicked_at_ms IS NULL THEN 1
                        ELSE reward_market_pool_state.in_pool
                    END,
                    last_seen_at_ms = excluded.last_seen_at_ms
                ",
                params![
                    condition_id,
                    market_slug,
                    question,
                    token1,
                    token2,
                    tokens_json,
                    market_competitiveness,
                    now_ms as i64,
                ],
            )?;
            Ok(())
        })
    }

    pub fn load_latest_reward_market_pool_meta(
        &self,
    ) -> anyhow::Result<Option<StoredRewardMarketPoolMeta>> {
        self.with_conn(|conn| {
            conn.query_row(
                "
                SELECT build_date_utc, pool_version, pool_version
                FROM reward_market_pool_state
                WHERE build_date_utc IS NOT NULL
                  AND pool_version IS NOT NULL
                ORDER BY pool_version DESC
                LIMIT 1
                ",
                [],
                |row| {
                    let build_date_utc: String = row.get(0)?;
                    let version = row.get::<_, i64>(1)? as u64;
                    let built_at_ms = row.get::<_, i64>(2)? as u64;
                    let build_date_utc = NaiveDate::parse_from_str(&build_date_utc, "%Y-%m-%d")
                        .map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                0,
                                rusqlite::types::Type::Text,
                                Box::new(error),
                            )
                        })?;
                    Ok(StoredRewardMarketPoolMeta {
                        build_date_utc,
                        version,
                        built_at_ms,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn load_active_reward_market_pool_entries(
        &self,
    ) -> anyhow::Result<Vec<ActiveRewardMarketPoolEntry>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "
                SELECT condition_id, market_slug, question, token1, token2, tokens_json,
                       market_competitiveness, rewards_min_size, rewards_max_spread,
                       market_daily_reward, build_date_utc, pool_version,
                       liquidity_reward_selected, liquidity_reward_selected_at_ms,
                       liquidity_reward_select_reason, liquidity_reward_select_rank,
                       liquidity_reward_halted, liquidity_reward_halted_at_ms,
                       liquidity_reward_halt_reason, liquidity_reward_halted_pool_version
                FROM reward_market_pool_state
                WHERE in_pool = 1
                ORDER BY condition_id
",
            )?;
            let rows = stmt.query_map([], |row| active_reward_market_pool_entry_from_row(row))?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
    }

    pub fn load_liquidity_reward_pool_entries(
        &self,
    ) -> anyhow::Result<Vec<ActiveRewardMarketPoolEntry>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "
                SELECT condition_id, market_slug, question, token1, token2, tokens_json,
                       market_competitiveness, rewards_min_size, rewards_max_spread,
                       market_daily_reward, build_date_utc, pool_version,
                       liquidity_reward_selected, liquidity_reward_selected_at_ms,
                       liquidity_reward_select_reason, liquidity_reward_select_rank,
                       liquidity_reward_halted, liquidity_reward_halted_at_ms,
                       liquidity_reward_halt_reason, liquidity_reward_halted_pool_version
                FROM reward_market_pool_state
                WHERE in_pool = 1
                  AND liquidity_reward_selected = 1
                  AND (
                      liquidity_reward_halted = 0
                      OR liquidity_reward_halted_pool_version IS NULL
                      OR liquidity_reward_halted_pool_version != pool_version
                  )
                ORDER BY liquidity_reward_select_rank, condition_id
                ",
            )?;
            let rows = stmt.query_map([], |row| active_reward_market_pool_entry_from_row(row))?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
    }

    pub fn halt_liquidity_reward_pool_entry(
        &self,
        condition_id: &str,
        pool_version: u64,
        reason: &str,
        halted_at_ms: u64,
    ) -> anyhow::Result<bool> {
        self.with_conn(|conn| {
            let updated = conn.execute(
                "
                UPDATE reward_market_pool_state
                SET liquidity_reward_halted = 1,
                    liquidity_reward_halted_at_ms = ?3,
                    liquidity_reward_halt_reason = ?4,
                    liquidity_reward_halted_pool_version = ?2
                WHERE condition_id = ?1
                  AND pool_version = ?2
                  AND liquidity_reward_selected = 1
                  AND (
                      liquidity_reward_halted = 0
                      OR liquidity_reward_halted_pool_version IS NULL
                      OR liquidity_reward_halted_pool_version != ?2
                  )
                ",
                params![
                    condition_id,
                    pool_version as i64,
                    halted_at_ms as i64,
                    reason
                ],
            )?;
            Ok(updated > 0)
        })
    }

    pub fn update_reward_market_pool_token1_check(
        &self,
        condition_id: &str,
        token1_best_bid: f64,
        token1_best_ask: f64,
        token1_spread: f64,
        checked_at_ms: u64,
    ) -> anyhow::Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "
                UPDATE reward_market_pool_state
                SET last_token1_best_bid = ?2,
                    last_token1_best_ask = ?3,
                    last_token1_spread = ?4,
                    last_checked_at_ms = ?5
                WHERE condition_id = ?1
                ",
                params![
                    condition_id,
                    token1_best_bid.to_string(),
                    token1_best_ask.to_string(),
                    token1_spread.to_string(),
                    checked_at_ms as i64,
                ],
            )?;
            Ok(())
        })
    }

    pub fn kick_reward_market_pool_entry(
        &self,
        condition_id: &str,
        reason: &str,
        kicked_at_ms: u64,
    ) -> anyhow::Result<bool> {
        self.with_conn(|conn| {
            let changed = conn.execute(
                "
                UPDATE reward_market_pool_state
                SET in_pool = 0,
                    kicked_at_ms = ?2,
                    kick_reason = ?3
                WHERE condition_id = ?1
                  AND in_pool = 1
                ",
                params![condition_id, kicked_at_ms as i64, reason],
            )?;
            Ok(changed > 0)
        })
    }

    pub fn get_reward_market_pool_state(
        &self,
        condition_id: &str,
    ) -> anyhow::Result<Option<StoredRewardMarketPoolState>> {
        self.with_conn(|conn| {
            conn.query_row(
                "
                SELECT condition_id, market_slug, question, token1, token2, in_pool,
                       kicked_at_ms, kick_reason
                FROM reward_market_pool_state
                WHERE condition_id = ?1
                ",
                params![condition_id],
                |row| {
                    Ok(StoredRewardMarketPoolState {
                        condition_id: row.get(0)?,
                        market_slug: row.get(1)?,
                        question: row.get(2)?,
                        token1: row.get(3)?,
                        token2: row.get(4)?,
                        in_pool: row.get::<_, i64>(5)? != 0,
                        kicked_at_ms: row
                            .get::<_, Option<i64>>(6)?
                            .and_then(|value| u64::try_from(value).ok()),
                        kick_reason: row.get(7)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
    }

    fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> anyhow::Result<T>) -> anyhow::Result<T> {
        let guard = self
            .conn
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite 连接锁已中毒"))?;
        f(&guard)
    }
}

struct LiquidityRewardPoolSelection<'a> {
    condition_id: &'a str,
    reason: &'static str,
    rank: u32,
}

fn load_selected_entries_missing_from_next_pool(
    conn: &Connection,
    next_entries: &[RewardMarketPoolStorageEntry<'_>],
) -> anyhow::Result<Vec<RemovedLiquidityRewardPoolEntry>> {
    let next_condition_ids = next_entries
        .iter()
        .map(|entry| entry.condition_id)
        .collect::<BTreeSet<_>>();
    let mut stmt = conn.prepare(
        "
        SELECT condition_id, token1, token2
        FROM reward_market_pool_state
        WHERE in_pool = 1
          AND liquidity_reward_selected = 1
        ORDER BY liquidity_reward_select_rank, condition_id
        ",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(RemovedLiquidityRewardPoolEntry {
            condition_id: row.get(0)?,
            token1: row.get(1)?,
            token2: row.get(2)?,
        })
    })?;
    let selected_entries = rows.collect::<Result<Vec<_>, _>>()?;
    Ok(selected_entries
        .into_iter()
        .filter(|entry| !next_condition_ids.contains(entry.condition_id.as_str()))
        .collect())
}

fn mark_liquidity_reward_pool_selection_in_conn(
    conn: &Connection,
    entries: &[RewardMarketPoolStorageEntry<'_>],
    market_count: usize,
    selected_at_ms: u64,
) -> anyhow::Result<usize> {
    let selections = select_liquidity_reward_pool_entries(entries, market_count);
    if selections.is_empty() {
        return Ok(0);
    }

    let mut stmt = conn.prepare_cached(
        "
        UPDATE reward_market_pool_state
        SET liquidity_reward_selected = 1,
            liquidity_reward_selected_at_ms = ?2,
            liquidity_reward_select_reason = ?3,
            liquidity_reward_select_rank = ?4
        WHERE condition_id = ?1
        ",
    )?;
    for selection in &selections {
        stmt.execute(params![
            selection.condition_id,
            selected_at_ms as i64,
            selection.reason,
            selection.rank as i64,
        ])?;
    }

    Ok(selections.len())
}

fn select_liquidity_reward_pool_entries<'a>(
    entries: &'a [RewardMarketPoolStorageEntry<'a>],
    market_count: usize,
) -> Vec<LiquidityRewardPoolSelection<'a>> {
    if market_count == 0 {
        return Vec::new();
    }

    let mut sorted = entries
        .iter()
        .filter_map(|entry| {
            entry
                .market_competitiveness
                .and_then(|value| value.parse::<f64>().ok())
                .map(|competitiveness| (entry, competitiveness))
        })
        .collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let head_count = market_count / 2;
    let tail_count = market_count.saturating_sub(head_count);
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();

    for (entry, _) in sorted.iter().take(head_count) {
        if seen.insert(entry.condition_id) {
            selected.push(LiquidityRewardPoolSelection {
                condition_id: entry.condition_id,
                reason: "competitiveness_low_tail",
                rank: selected.len() as u32 + 1,
            });
        }
    }
    for (entry, _) in sorted.iter().rev().take(tail_count).rev() {
        if seen.insert(entry.condition_id) {
            selected.push(LiquidityRewardPoolSelection {
                condition_id: entry.condition_id,
                reason: "competitiveness_high_tail",
                rank: selected.len() as u32 + 1,
            });
        }
    }

    selected
}

fn active_reward_market_pool_entry_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ActiveRewardMarketPoolEntry> {
    let pool_version = row
        .get::<_, Option<i64>>(11)?
        .and_then(|value| u64::try_from(value).ok());
    let liquidity_reward_selected_at_ms = row
        .get::<_, Option<i64>>(13)?
        .and_then(|value| u64::try_from(value).ok());
    let liquidity_reward_select_rank = row
        .get::<_, Option<i64>>(15)?
        .and_then(|value| u32::try_from(value).ok());
    let liquidity_reward_halted_at_ms = row
        .get::<_, Option<i64>>(17)?
        .and_then(|value| u64::try_from(value).ok());
    let liquidity_reward_halted_pool_version = row
        .get::<_, Option<i64>>(19)?
        .and_then(|value| u64::try_from(value).ok());
    Ok(ActiveRewardMarketPoolEntry {
        condition_id: row.get(0)?,
        market_slug: row.get(1)?,
        question: row.get(2)?,
        token1: row.get(3)?,
        token2: row.get(4)?,
        tokens_json: row.get(5)?,
        market_competitiveness: row.get(6)?,
        rewards_min_size: row.get(7)?,
        rewards_max_spread: row.get(8)?,
        market_daily_reward: row.get(9)?,
        build_date_utc: row.get(10)?,
        pool_version,
        liquidity_reward_selected: row.get::<_, i64>(12)? != 0,
        liquidity_reward_selected_at_ms,
        liquidity_reward_select_reason: row.get(14)?,
        liquidity_reward_select_rank,
        liquidity_reward_halted: row.get::<_, i64>(16)? != 0,
        liquidity_reward_halted_at_ms,
        liquidity_reward_halt_reason: row.get(18)?,
        liquidity_reward_halted_pool_version,
    })
}

fn open_sqlite_connection(path: &str) -> anyhow::Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("无法打开 SQLite 文件: {path}"))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(conn)
}

fn now_ms() -> anyhow::Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("系统时间早于 Unix epoch")?;
    Ok(duration.as_millis() as u64)
}

fn decimal_from_str(value: &str) -> anyhow::Result<Decimal> {
    Decimal::from_str(value).with_context(|| format!("无法解析 Decimal: {value}"))
}

fn side_to_str(side: QuoteSide) -> &'static str {
    match side {
        QuoteSide::Buy => "buy",
        QuoteSide::Sell => "sell",
    }
}

fn side_from_str(value: &str) -> anyhow::Result<QuoteSide> {
    match value {
        "buy" => Ok(QuoteSide::Buy),
        "sell" => Ok(QuoteSide::Sell),
        other => Err(anyhow::anyhow!("未知 side: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reward_market_pool_state_kick_is_persistent() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");

        store
            .upsert_reward_market_pool_entry(
                "0xabc",
                Some("slug"),
                Some("question"),
                "token1",
                "token2",
                "[]",
                Some("1.23"),
                100,
            )
            .expect("entry should insert");
        let state = store
            .get_reward_market_pool_state("0xabc")
            .expect("state query should work")
            .expect("state should exist");
        assert!(state.in_pool);
        assert_eq!(state.token1, "token1");
        assert_eq!(state.token2, "token2");

        store
            .update_reward_market_pool_token1_check("0xabc", 0.45, 0.56, 0.11, 150)
            .expect("price check should update");
        assert!(
            store
                .kick_reward_market_pool_entry("0xabc", "token1_spread_gt_threshold", 200)
                .expect("kick should update")
        );
        assert!(
            !store
                .kick_reward_market_pool_entry("0xabc", "repeat", 201)
                .expect("second kick should be ignored")
        );

        store
            .upsert_reward_market_pool_entry(
                "0xabc",
                Some("slug2"),
                Some("question2"),
                "token1",
                "token2",
                "[]",
                Some("2.34"),
                300,
            )
            .expect("upsert after kick should not restore");
        let state = store
            .get_reward_market_pool_state("0xabc")
            .expect("state query should work")
            .expect("state should exist");
        assert!(!state.in_pool);
        assert_eq!(state.kicked_at_ms, Some(200));
        assert_eq!(
            state.kick_reason.as_deref(),
            Some("token1_spread_gt_threshold")
        );
        assert_eq!(state.market_slug.as_deref(), Some("slug2"));
    }

    #[test]
    fn replace_reward_market_pool_entries_deletes_old_pool_and_resets_kick() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");

        store
            .upsert_reward_market_pool_entry(
                "0xabc",
                Some("old"),
                Some("old question"),
                "token1",
                "token2",
                "[]",
                Some("1.23"),
                100,
            )
            .expect("entry should insert");
        store
            .kick_reward_market_pool_entry("0xabc", "old kick", 150)
            .expect("kick should update");

        let build_date = NaiveDate::from_ymd_opt(2026, 5, 4).unwrap();
        let entries = vec![RewardMarketPoolStorageEntry {
            condition_id: "0xdef",
            market_slug: Some("new"),
            question: Some("new question"),
            token1: "new token1",
            token2: "new token2",
            tokens_json: "[]",
            market_competitiveness: Some("2.34"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("50"),
        }];
        store
            .replace_reward_market_pool_entries(build_date, 2, &entries, 200, 0)
            .expect("pool should replace");

        assert!(
            store
                .get_reward_market_pool_state("0xabc")
                .expect("state query should work")
                .is_none()
        );
        let state = store
            .get_reward_market_pool_state("0xdef")
            .expect("state query should work")
            .expect("new state should exist");
        assert!(state.in_pool);
        assert_eq!(state.kicked_at_ms, None);
        assert_eq!(state.kick_reason, None);
        assert_eq!(state.market_slug.as_deref(), Some("new"));
    }

    #[test]
    fn replace_reward_market_pool_entries_reports_removed_selected_entries() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let build_date = NaiveDate::from_ymd_opt(2026, 5, 4).unwrap();
        let first_entries = vec![
            RewardMarketPoolStorageEntry {
                condition_id: "0xkeep",
                market_slug: Some("keep"),
                question: Some("keep question"),
                token1: "keep token1",
                token2: "keep token2",
                tokens_json: "[]",
                market_competitiveness: Some("1"),
                rewards_min_size: Some("100"),
                rewards_max_spread: Some("4"),
                market_daily_reward: Some("50"),
            },
            RewardMarketPoolStorageEntry {
                condition_id: "0xremoved",
                market_slug: Some("removed"),
                question: Some("removed question"),
                token1: "removed token1",
                token2: "removed token2",
                tokens_json: "[]",
                market_competitiveness: Some("2"),
                rewards_min_size: Some("100"),
                rewards_max_spread: Some("4"),
                market_daily_reward: Some("50"),
            },
        ];
        store
            .replace_reward_market_pool_entries(build_date, 1, &first_entries, 100, 2)
            .expect("initial pool should replace");

        let second_entries = vec![RewardMarketPoolStorageEntry {
            condition_id: "0xkeep",
            market_slug: Some("keep"),
            question: Some("keep question"),
            token1: "keep token1",
            token2: "keep token2",
            tokens_json: "[]",
            market_competitiveness: Some("1"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("50"),
        }];
        let result = store
            .replace_reward_market_pool_entries(build_date, 2, &second_entries, 200, 1)
            .expect("second pool should replace");

        assert_eq!(result.selected_count, 1);
        assert_eq!(result.removed_selected_entries.len(), 1);
        assert_eq!(result.removed_selected_entries[0].condition_id, "0xremoved");
        assert_eq!(result.removed_selected_entries[0].token1, "removed token1");
        assert_eq!(result.removed_selected_entries[0].token2, "removed token2");
    }

    #[test]
    fn load_latest_reward_market_pool_meta_returns_last_build() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let old_build_date = NaiveDate::from_ymd_opt(2026, 5, 4).unwrap();
        let new_build_date = NaiveDate::from_ymd_opt(2026, 5, 5).unwrap();
        let entries = vec![RewardMarketPoolStorageEntry {
            condition_id: "0xlatest",
            market_slug: Some("latest"),
            question: Some("latest question"),
            token1: "latest token1",
            token2: "latest token2",
            tokens_json: "[]",
            market_competitiveness: Some("1"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("50"),
        }];

        assert!(
            store
                .load_latest_reward_market_pool_meta()
                .expect("empty meta query should work")
                .is_none()
        );
        store
            .replace_reward_market_pool_entries(old_build_date, 100, &entries, 100, 1)
            .expect("old pool should replace");
        store
            .replace_reward_market_pool_entries(new_build_date, 200, &entries, 200, 1)
            .expect("new pool should replace");

        let meta = store
            .load_latest_reward_market_pool_meta()
            .expect("latest meta query should work")
            .expect("latest meta should exist");

        assert_eq!(meta.build_date_utc, new_build_date);
        assert_eq!(meta.version, 200);
        assert_eq!(meta.built_at_ms, 200);
    }

    #[test]
    fn load_liquidity_reward_pool_entries_skips_current_version_halted_entries() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let build_date = NaiveDate::from_ymd_opt(2026, 5, 4).unwrap();
        let entries = vec![RewardMarketPoolStorageEntry {
            condition_id: "0xhalted",
            market_slug: Some("halted"),
            question: Some("halted question"),
            token1: "halted token1",
            token2: "halted token2",
            tokens_json: "[]",
            market_competitiveness: Some("1"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("50"),
        }];
        store
            .replace_reward_market_pool_entries(build_date, 100, &entries, 100, 1)
            .expect("pool should replace");
        store
            .halt_liquidity_reward_pool_entry("0xhalted", 100, "filled", 200)
            .expect("halt should persist");

        let selected = store
            .load_liquidity_reward_pool_entries()
            .expect("selected entries should load");

        assert!(selected.is_empty());
    }

    #[test]
    fn load_liquidity_reward_pool_entries_allows_halted_entry_after_new_pool_version() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let old_build_date = NaiveDate::from_ymd_opt(2026, 5, 4).unwrap();
        let new_build_date = NaiveDate::from_ymd_opt(2026, 5, 5).unwrap();
        let entries = vec![RewardMarketPoolStorageEntry {
            condition_id: "0xhalted",
            market_slug: Some("halted"),
            question: Some("halted question"),
            token1: "halted token1",
            token2: "halted token2",
            tokens_json: "[]",
            market_competitiveness: Some("1"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("50"),
        }];
        store
            .replace_reward_market_pool_entries(old_build_date, 100, &entries, 100, 1)
            .expect("old pool should replace");
        store
            .halt_liquidity_reward_pool_entry("0xhalted", 100, "filled", 200)
            .expect("halt should persist");
        store
            .replace_reward_market_pool_entries(new_build_date, 200, &entries, 300, 1)
            .expect("new pool should replace");

        let selected = store
            .load_liquidity_reward_pool_entries()
            .expect("selected entries should load");

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].condition_id, "0xhalted");
    }

    #[test]
    fn load_active_reward_market_pool_entries_returns_only_in_pool() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let build_date = NaiveDate::from_ymd_opt(2026, 5, 4).unwrap();
        let entries = vec![
            RewardMarketPoolStorageEntry {
                condition_id: "0xaaa",
                market_slug: Some("active"),
                question: Some("active question"),
                token1: "active token1",
                token2: "active token2",
                tokens_json: "[{\"token_id\":\"active token1\"}]",
                market_competitiveness: Some("1.11"),
                rewards_min_size: Some("100"),
                rewards_max_spread: Some("4"),
                market_daily_reward: Some("50"),
            },
            RewardMarketPoolStorageEntry {
                condition_id: "0xbbb",
                market_slug: Some("kicked"),
                question: Some("kicked question"),
                token1: "kicked token1",
                token2: "kicked token2",
                tokens_json: "[]",
                market_competitiveness: Some("2.22"),
                rewards_min_size: Some("200"),
                rewards_max_spread: Some("5"),
                market_daily_reward: Some("60"),
            },
        ];
        store
            .replace_reward_market_pool_entries(build_date, 123, &entries, 200, 0)
            .expect("pool should replace");
        store
            .kick_reward_market_pool_entry("0xbbb", "out", 250)
            .expect("kick should update");

        let active = store
            .load_active_reward_market_pool_entries()
            .expect("active entries should load");

        assert_eq!(active.len(), 1);
        assert_eq!(active[0].condition_id, "0xaaa");
        assert_eq!(active[0].market_slug.as_deref(), Some("active"));
        assert_eq!(active[0].token1, "active token1");
        assert_eq!(active[0].token2, "active token2");
        assert_eq!(active[0].tokens_json, "[{\"token_id\":\"active token1\"}]");
        assert_eq!(active[0].rewards_min_size.as_deref(), Some("100"));
        assert_eq!(active[0].rewards_max_spread.as_deref(), Some("4"));
        assert_eq!(active[0].market_daily_reward.as_deref(), Some("50"));
        assert_eq!(active[0].build_date_utc.as_deref(), Some("2026-05-04"));
        assert_eq!(active[0].pool_version, Some(123));
    }

    #[test]
    fn load_liquidity_reward_pool_entries_selects_competitiveness_tails() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let build_date = NaiveDate::from_ymd_opt(2026, 5, 4).unwrap();
        let entries = (0..8)
            .map(|index| RewardMarketPoolStorageEntry {
                condition_id: match index {
                    0 => "0x0",
                    1 => "0x1",
                    2 => "0x2",
                    3 => "0x3",
                    4 => "0x4",
                    5 => "0x5",
                    6 => "0x6",
                    _ => "0x7",
                },
                market_slug: None,
                question: None,
                token1: match index {
                    0 => "token10",
                    1 => "token11",
                    2 => "token12",
                    3 => "token13",
                    4 => "token14",
                    5 => "token15",
                    6 => "token16",
                    _ => "token17",
                },
                token2: match index {
                    0 => "token20",
                    1 => "token21",
                    2 => "token22",
                    3 => "token23",
                    4 => "token24",
                    5 => "token25",
                    6 => "token26",
                    _ => "token27",
                },
                tokens_json: "[]",
                market_competitiveness: match index {
                    0 => Some("0"),
                    1 => Some("1"),
                    2 => Some("2"),
                    3 => Some("3"),
                    4 => Some("4"),
                    5 => Some("5"),
                    6 => Some("6"),
                    _ => Some("7"),
                },
                rewards_min_size: Some("100"),
                rewards_max_spread: Some("4"),
                market_daily_reward: Some("50"),
            })
            .collect::<Vec<_>>();
        store
            .replace_reward_market_pool_entries(build_date, 123, &entries, 200, 6)
            .expect("pool should replace");
        let selected = store
            .load_liquidity_reward_pool_entries()
            .expect("pool entries should load");
        let condition_ids = selected
            .iter()
            .map(|entry| entry.condition_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            condition_ids,
            vec!["0x0", "0x1", "0x2", "0x5", "0x6", "0x7"]
        );
        assert!(selected.iter().all(|entry| entry.liquidity_reward_selected));
        assert_eq!(
            selected
                .iter()
                .map(|entry| entry.liquidity_reward_select_reason.as_deref())
                .collect::<Vec<_>>(),
            vec![
                Some("competitiveness_low_tail"),
                Some("competitiveness_low_tail"),
                Some("competitiveness_low_tail"),
                Some("competitiveness_high_tail"),
                Some("competitiveness_high_tail"),
                Some("competitiveness_high_tail"),
            ]
        );
        assert_eq!(
            selected
                .iter()
                .map(|entry| entry.liquidity_reward_select_rank)
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)]
        );
        assert_eq!(
            selected
                .iter()
                .map(|entry| entry.liquidity_reward_selected_at_ms)
                .collect::<Vec<_>>(),
            vec![Some(200); 6]
        );

        store
            .kick_reward_market_pool_entry("0x6", "out", 250)
            .expect("kick should update");
        let selected_after_kick = store
            .load_liquidity_reward_pool_entries()
            .expect("pool entries should load");
        let condition_ids_after_kick = selected_after_kick
            .iter()
            .map(|entry| entry.condition_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            condition_ids_after_kick,
            vec!["0x0", "0x1", "0x2", "0x5", "0x7"]
        );
    }
}

fn migrate_liquidity_reward_side_state(conn: &Connection) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(
        "
        SELECT token, active_local_order_id, pending_local_order_id, pending_side,
               pending_price, pending_order_size, pending_mid, last_mid
        FROM strategy_state_mid_requote
        WHERE active_local_order_id IS NOT NULL OR pending_local_order_id IS NOT NULL
        ",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
        ))
    })?;
    let legacy_rows = rows.collect::<Result<Vec<_>, _>>()?;
    let now = now_ms()?;

    for (
        token,
        active_local_order_id,
        pending_local_order_id,
        pending_side,
        pending_price,
        pending_order_size,
        pending_mid,
        last_mid,
    ) in legacy_rows
    {
        if side_state_exists(conn, &token)? {
            continue;
        }

        if let Some(active_local_order_id) = active_local_order_id {
            let active_side = conn
                .query_row(
                    "SELECT side FROM orders WHERE local_order_id = ?1",
                    params![active_local_order_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if let Some(active_side) = active_side {
                conn.execute(
                    "
                    INSERT OR IGNORE INTO strategy_state_mid_requote_side (
                        token, side, active_local_order_id, pending_local_order_id, pending_price,
                        pending_order_size, pending_mid, last_quoted_mid, cancel_requested, updated_at_ms
                    ) VALUES (?1, ?2, ?3, NULL, NULL, NULL, NULL, ?4, 0, ?5)
                    ",
                    params![token, active_side, active_local_order_id, last_mid, now],
                )?;
            }
        }

        if let (
            Some(pending_local_order_id),
            Some(pending_side),
            Some(pending_price),
            Some(pending_order_size),
            Some(pending_mid),
        ) = (
            pending_local_order_id,
            pending_side,
            pending_price,
            pending_order_size,
            pending_mid,
        ) {
            conn.execute(
                "
                INSERT INTO strategy_state_mid_requote_side (
                    token, side, active_local_order_id, pending_local_order_id, pending_price,
                    pending_order_size, pending_mid, last_quoted_mid, cancel_requested, updated_at_ms
                ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, 0, ?8)
                ON CONFLICT(token, side) DO UPDATE SET
                    pending_local_order_id = excluded.pending_local_order_id,
                    pending_price = excluded.pending_price,
                    pending_order_size = excluded.pending_order_size,
                    pending_mid = excluded.pending_mid,
                    last_quoted_mid = COALESCE(strategy_state_mid_requote_side.last_quoted_mid, excluded.last_quoted_mid),
                    updated_at_ms = excluded.updated_at_ms
                ",
                params![
                    token,
                    pending_side,
                    pending_local_order_id,
                    pending_price,
                    pending_order_size,
                    pending_mid,
                    last_mid,
                    now,
                ],
            )?;
        }
    }

    Ok(())
}

fn side_state_exists(conn: &Connection, token: &str) -> anyhow::Result<bool> {
    let count = conn.query_row(
        "SELECT COUNT(*) FROM strategy_state_mid_requote_side WHERE token = ?1",
        params![token],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(count > 0)
}

fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> anyhow::Result<()> {
    let pragma = format!("PRAGMA table_info({table})");
    let mut stmt = conn.prepare(&pragma)?;
    let columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let exists = columns
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .any(|name| name == column);
    if !exists {
        conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
            [],
        )?;
    }
    Ok(())
}

fn to_sql_error(error: anyhow::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::other(error.to_string())),
    )
}
