use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use polymarket_client_sdk::types::Decimal;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

use crate::strategy::{LocalOrderMeta, QuoteSide};

#[derive(Clone)]
pub struct OrderStore {
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
pub struct StoredMidRequoteSharedState {
    pub token: String,
    pub topic: String,
    pub last_mid: Option<Decimal>,
    pub last_best_bid: Option<Decimal>,
    pub last_best_ask: Option<Decimal>,
    pub last_position_size: Decimal,
}

#[derive(Debug, Clone)]
pub struct StoredMidRequoteSideState {
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
        let conn =
            Connection::open(path).with_context(|| format!("无法打开 SQLite 文件: {path}"))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
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
            migrate_mid_requote_side_state(conn)?;
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

    pub fn upsert_mid_requote_state(
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

    pub fn upsert_mid_requote_shared_state(
        &self,
        token: &str,
        topic: &str,
        last_mid: Option<Decimal>,
        last_best_bid: Option<Decimal>,
        last_best_ask: Option<Decimal>,
        last_position_size: Decimal,
    ) -> anyhow::Result<()> {
        self.upsert_mid_requote_state(
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

    pub fn upsert_mid_requote_side_state(
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

    pub fn load_mid_requote_shared_states(
        &self,
    ) -> anyhow::Result<Vec<StoredMidRequoteSharedState>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "
                SELECT token, topic, last_mid, last_best_bid, last_best_ask, last_position_size
                FROM strategy_state_mid_requote
                ",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(StoredMidRequoteSharedState {
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

    pub fn load_mid_requote_side_states(&self) -> anyhow::Result<Vec<StoredMidRequoteSideState>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "
                SELECT token, side, active_local_order_id, pending_local_order_id, pending_price,
                       pending_order_size, pending_mid, last_quoted_mid, cancel_requested
                FROM strategy_state_mid_requote_side
                ",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(StoredMidRequoteSideState {
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

    pub fn find_local_order_id_by_remote(
        &self,
        remote_order_id: &str,
    ) -> anyhow::Result<Option<String>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT local_order_id FROM orders WHERE remote_order_id = ?1",
                params![remote_order_id],
                |row| row.get(0),
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

fn migrate_mid_requote_side_state(conn: &Connection) -> anyhow::Result<()> {
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
