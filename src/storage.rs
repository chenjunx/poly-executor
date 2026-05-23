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

#[derive(Debug, Clone, PartialEq)]
pub struct StoredDailyLossState {
    pub trading_day: String,
    pub day_start_total_pnl: Decimal,
    pub day_start_equity: Decimal,
    pub loss_limit_ratio: Decimal,
    pub loss_limit_amount: Decimal,
    pub halted: bool,
    pub halt_reason: Option<String>,
    pub halted_at_ms: Option<u64>,
    pub updated_at_ms: u64,
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
    pub volume_24hr_clob: Option<&'a str>,
    pub volume_24hr: Option<&'a str>,
    pub liquidity_reward_roi: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderGatewayOrderSnapshot {
    pub strategy_id: String,
    pub market_id: Option<String>,
    pub token_id: String,
    pub local_id: String,
    pub exch_id: Option<String>,
    pub side: String,
    pub order_type: String,
    pub price: Option<String>,
    pub size: String,
    pub local_state: String,
    pub remote_status_code: Option<String>,
    pub filled_size_total: String,
    pub remaining_size: String,
    pub avg_fill_price: Option<String>,
    pub last_submission_attempt: Option<i64>,
    pub last_event_seq: u64,
    pub terminal_at_ms: Option<u64>,
}

pub struct OrderGatewayEventInsert<'a> {
    pub seq: u64,
    pub strategy_id: &'a str,
    pub token_id: &'a str,
    pub market_id: Option<&'a str>,
    pub local_id: Option<&'a str>,
    pub exch_id: Option<&'a str>,
    pub event_kind: &'a str,
    pub local_state: &'a str,
    pub remote_status_code: Option<&'a str>,
    pub remote_reject_code: Option<&'a str>,
    pub remote_reject_reason: Option<&'a str>,
    pub fill_delta: Option<&'a str>,
    pub fill_total: Option<&'a str>,
    pub remaining_size: Option<&'a str>,
    pub avg_fill_price: Option<&'a str>,
    pub error_code: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub raw_json: &'a str,
    pub recovery: bool,
}

pub struct OrderGatewaySubmissionInsert<'a> {
    pub local_id: &'a str,
    pub submit_attempt: i64,
    pub strategy_id: &'a str,
    pub token_id: &'a str,
    pub side: &'a str,
    pub order_type: &'a str,
    pub price: Option<&'a str>,
    pub size: &'a str,
    pub exch_id: Option<&'a str>,
    pub unsigned_payload_json: &'a str,
    pub signed_payload_json: &'a str,
    pub signature: &'a str,
    pub signer_address: &'a str,
    pub nonce_or_salt: Option<&'a str>,
    pub expiration: Option<i64>,
    pub exchange_payload_hash: &'a str,
    pub rest_request_json: &'a str,
    pub rest_response_json: Option<&'a str>,
    pub rest_status_code: Option<i64>,
    pub submit_state: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredOrderGatewaySubmission {
    pub local_id: String,
    pub submit_attempt: i64,
    pub signed_payload_json: String,
    pub exchange_payload_hash: String,
    pub submit_state: String,
}

pub struct PositionJournalInsert<'a> {
    pub seq: u64,
    pub ts_ms: u64,
    pub event_type: &'a str,
    pub strategy_id: Option<&'a str>,
    pub token_id: &'a str,
    pub local_order_id: Option<&'a str>,
    pub exchange_order_id: Option<&'a str>,
    pub side: Option<&'a str>,
    pub qty: Option<&'a str>,
    pub price: Option<&'a str>,
    pub source: &'a str,
    pub recovery: bool,
    pub payload_json: &'a str,
}

pub struct PositionSnapshotRow<'a> {
    pub scope_type: &'a str,
    pub strategy_id: Option<&'a str>,
    pub token_id: &'a str,
    pub filled_position: &'a str,
    pub cost_basis: &'a str,
    pub realized_pnl: &'a str,
    pub working_buy_exposure: &'a str,
    pub working_sell_exposure: &'a str,
}

pub struct PositionOpenOrderSnapshotRow<'a> {
    pub snapshot_id: u64,
    pub seq: u64,
    pub strategy_id: &'a str,
    pub token_id: &'a str,
    pub local_order_id: &'a str,
    pub exchange_order_id: Option<&'a str>,
    pub side: &'a str,
    pub price: &'a str,
    pub original_size: &'a str,
    pub remaining_size: &'a str,
    pub local_state: &'a str,
}

pub struct PositionReconciliationInsert<'a> {
    pub reconciliation_id: &'a str,
    pub started_at_ms: u64,
    pub exchange_data_as_of_ms: u64,
    pub last_local_seq_compared: u64,
    pub status: &'a str,
    pub mismatch_count: u64,
    pub adjustment_journal_seq: Option<u64>,
    pub summary_json: &'a str,
    pub alert_message: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPositionJournalRow {
    pub seq: u64,
    pub event_type: String,
    pub strategy_id: Option<String>,
    pub token_id: String,
    pub local_order_id: Option<String>,
    pub side: Option<String>,
    pub qty: Option<String>,
    pub price: Option<String>,
    pub payload_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPositionSnapshotRow {
    pub scope_type: String,
    pub strategy_id: Option<String>,
    pub token_id: String,
    pub filled_position: String,
    pub cost_basis: String,
    pub realized_pnl: String,
    pub working_buy_exposure: String,
    pub working_sell_exposure: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPositionOpenOrderSnapshotRow {
    pub snapshot_id: u64,
    pub seq: u64,
    pub strategy_id: String,
    pub token_id: String,
    pub local_order_id: String,
    pub exchange_order_id: Option<String>,
    pub side: String,
    pub price: String,
    pub original_size: String,
    pub remaining_size: String,
    pub local_state: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredPositionSnapshotBatch {
    pub snapshot_id: u64,
    pub seq: u64,
    pub ts_ms: u64,
    pub rows: Vec<StoredPositionSnapshotRow>,
    pub open_orders: Vec<StoredPositionOpenOrderSnapshotRow>,
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
    pub volume_24hr_clob: Option<String>,
    pub volume_24hr: Option<String>,
    pub liquidity_reward_roi: Option<String>,
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

                CREATE TABLE IF NOT EXISTS order_gateway_orders (
                    local_id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    market_id TEXT,
                    token_id TEXT NOT NULL,
                    exch_id TEXT UNIQUE,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    price TEXT,
                    size TEXT NOT NULL,
                    local_state TEXT NOT NULL,
                    remote_status_code TEXT,
                    filled_size_total TEXT NOT NULL,
                    remaining_size TEXT NOT NULL,
                    avg_fill_price TEXT,
                    last_submission_attempt INTEGER,
                    last_event_seq INTEGER NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    terminal_at_ms INTEGER
                );

                CREATE TABLE IF NOT EXISTS order_gateway_events (
                    seq INTEGER PRIMARY KEY,
                    created_at_ms INTEGER NOT NULL,
                    strategy_id TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    market_id TEXT,
                    local_id TEXT,
                    exch_id TEXT,
                    event_kind TEXT NOT NULL,
                    local_state TEXT NOT NULL,
                    remote_status_code TEXT,
                    remote_reject_code TEXT,
                    remote_reject_reason TEXT,
                    fill_delta TEXT,
                    fill_total TEXT,
                    remaining_size TEXT,
                    avg_fill_price TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    raw_json TEXT NOT NULL,
                    recovery INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS order_gateway_submissions (
                    local_id TEXT NOT NULL,
                    submit_attempt INTEGER NOT NULL,
                    strategy_id TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    side TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    price TEXT,
                    size TEXT NOT NULL,
                    exch_id TEXT,
                    unsigned_payload_json TEXT NOT NULL,
                    signed_payload_json TEXT NOT NULL,
                    signature TEXT NOT NULL,
                    signer_address TEXT NOT NULL,
                    nonce_or_salt TEXT,
                    expiration INTEGER,
                    exchange_payload_hash TEXT NOT NULL,
                    rest_request_json TEXT NOT NULL,
                    rest_response_json TEXT,
                    rest_status_code INTEGER,
                    submit_state TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (local_id, submit_attempt)
                );

                CREATE TABLE IF NOT EXISTS order_gateway_cancel_attempts (
                    cancel_attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    local_id TEXT,
                    exch_id TEXT,
                    scope TEXT NOT NULL,
                    rest_request_json TEXT NOT NULL,
                    rest_response_json TEXT,
                    rest_status_code INTEGER,
                    cancel_state TEXT NOT NULL,
                    error_code TEXT,
                    created_at_ms INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS position_journal (
                    seq INTEGER PRIMARY KEY,
                    ts_ms INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    strategy_id TEXT,
                    token_id TEXT NOT NULL,
                    local_order_id TEXT,
                    exchange_order_id TEXT,
                    side TEXT,
                    qty TEXT,
                    price TEXT,
                    source TEXT NOT NULL,
                    recovery INTEGER NOT NULL,
                    payload_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS position_snapshots (
                    snapshot_id INTEGER NOT NULL,
                    seq INTEGER NOT NULL,
                    ts_ms INTEGER NOT NULL,
                    scope_type TEXT NOT NULL,
                    strategy_id TEXT,
                    token_id TEXT NOT NULL,
                    filled_position TEXT NOT NULL,
                    cost_basis TEXT NOT NULL,
                    realized_pnl TEXT NOT NULL,
                    working_buy_exposure TEXT NOT NULL,
                    working_sell_exposure TEXT NOT NULL,
                    PRIMARY KEY (snapshot_id, scope_type, strategy_id, token_id)
                );

                CREATE TABLE IF NOT EXISTS position_open_orders_snapshot (
                    snapshot_id INTEGER NOT NULL,
                    seq INTEGER NOT NULL,
                    strategy_id TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    local_order_id TEXT NOT NULL,
                    exchange_order_id TEXT,
                    side TEXT NOT NULL,
                    price TEXT NOT NULL,
                    original_size TEXT NOT NULL,
                    remaining_size TEXT NOT NULL,
                    local_state TEXT NOT NULL,
                    PRIMARY KEY (snapshot_id, local_order_id)
                );

                CREATE TABLE IF NOT EXISTS position_reconciliations (
                    reconciliation_id TEXT PRIMARY KEY,
                    started_at_ms INTEGER NOT NULL,
                    exchange_data_as_of_ms INTEGER NOT NULL,
                    last_local_seq_compared INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    mismatch_count INTEGER NOT NULL,
                    adjustment_journal_seq INTEGER,
                    summary_json TEXT NOT NULL,
                    alert_message TEXT
                );

                CREATE TABLE IF NOT EXISTS risk_daily_loss_state (
                    trading_day TEXT PRIMARY KEY,
                    day_start_total_pnl TEXT NOT NULL,
                    day_start_equity TEXT NOT NULL,
                    loss_limit_ratio TEXT NOT NULL,
                    loss_limit_amount TEXT NOT NULL,
                    halted INTEGER NOT NULL,
                    halt_reason TEXT,
                    halted_at_ms INTEGER,
                    updated_at_ms INTEGER NOT NULL
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

    pub fn load_daily_loss_state(
        &self,
        trading_day: &str,
    ) -> anyhow::Result<Option<StoredDailyLossState>> {
        self.with_conn(|conn| {
            conn.query_row(
                "
                SELECT trading_day, day_start_total_pnl, day_start_equity, loss_limit_ratio,
                       loss_limit_amount, halted, halt_reason, halted_at_ms, updated_at_ms
                FROM risk_daily_loss_state
                WHERE trading_day = ?1
                ",
                params![trading_day],
                |row| {
                    Ok(StoredDailyLossState {
                        trading_day: row.get(0)?,
                        day_start_total_pnl: decimal_from_str(&row.get::<_, String>(1)?)
                            .map_err(to_sql_error)?,
                        day_start_equity: decimal_from_str(&row.get::<_, String>(2)?)
                            .map_err(to_sql_error)?,
                        loss_limit_ratio: decimal_from_str(&row.get::<_, String>(3)?)
                            .map_err(to_sql_error)?,
                        loss_limit_amount: decimal_from_str(&row.get::<_, String>(4)?)
                            .map_err(to_sql_error)?,
                        halted: row.get::<_, i64>(5)? != 0,
                        halt_reason: row.get(6)?,
                        halted_at_ms: row.get::<_, Option<i64>>(7)?.map(|value| value as u64),
                        updated_at_ms: row.get::<_, i64>(8)? as u64,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
    }

    pub fn upsert_daily_loss_state(&self, state: &StoredDailyLossState) -> anyhow::Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO risk_daily_loss_state (
                    trading_day, day_start_total_pnl, day_start_equity, loss_limit_ratio,
                    loss_limit_amount, halted, halt_reason, halted_at_ms, updated_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ON CONFLICT(trading_day) DO UPDATE SET
                    day_start_total_pnl = excluded.day_start_total_pnl,
                    day_start_equity = excluded.day_start_equity,
                    loss_limit_ratio = excluded.loss_limit_ratio,
                    loss_limit_amount = excluded.loss_limit_amount,
                    halted = excluded.halted,
                    halt_reason = excluded.halt_reason,
                    halted_at_ms = excluded.halted_at_ms,
                    updated_at_ms = excluded.updated_at_ms
                ",
                params![
                    state.trading_day.as_str(),
                    state.day_start_total_pnl.to_string(),
                    state.day_start_equity.to_string(),
                    state.loss_limit_ratio.to_string(),
                    state.loss_limit_amount.to_string(),
                    if state.halted { 1_i64 } else { 0_i64 },
                    state.halt_reason.as_deref(),
                    state.halted_at_ms.map(|value| value as i64),
                    state.updated_at_ms as i64,
                ],
            )?;
            Ok(())
        })
    }

    pub fn halt_daily_loss_state(
        &self,
        trading_day: &str,
        reason: &str,
        halted_at_ms: u64,
    ) -> anyhow::Result<()> {
        self.with_conn(|conn| {
            let affected = conn.execute(
                "
                UPDATE risk_daily_loss_state
                SET halted = 1, halt_reason = ?2, halted_at_ms = ?3, updated_at_ms = ?3
                WHERE trading_day = ?1
                ",
                params![trading_day, reason, halted_at_ms as i64],
            )?;
            anyhow::ensure!(
                affected == 1,
                "daily loss state not found for trading day {trading_day}"
            );
            Ok(())
        })
    }

    pub fn upsert_order_gateway_order(
        &self,
        snapshot: &OrderGatewayOrderSnapshot,
    ) -> anyhow::Result<()> {
        let now = now_ms()?;
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO order_gateway_orders (
                    local_id, strategy_id, market_id, token_id, exch_id, side, order_type,
                    price, size, local_state, remote_status_code, filled_size_total,
                    remaining_size, avg_fill_price, last_submission_attempt, last_event_seq,
                    created_at_ms, updated_at_ms, terminal_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)
                ON CONFLICT(local_id) DO UPDATE SET
                    strategy_id = excluded.strategy_id,
                    market_id = excluded.market_id,
                    token_id = excluded.token_id,
                    exch_id = COALESCE(excluded.exch_id, order_gateway_orders.exch_id),
                    side = excluded.side,
                    order_type = excluded.order_type,
                    price = excluded.price,
                    size = excluded.size,
                    local_state = excluded.local_state,
                    remote_status_code = excluded.remote_status_code,
                    filled_size_total = excluded.filled_size_total,
                    remaining_size = excluded.remaining_size,
                    avg_fill_price = excluded.avg_fill_price,
                    last_submission_attempt = excluded.last_submission_attempt,
                    last_event_seq = excluded.last_event_seq,
                    updated_at_ms = excluded.updated_at_ms,
                    terminal_at_ms = excluded.terminal_at_ms
                ",
                params![
                    snapshot.local_id,
                    snapshot.strategy_id,
                    snapshot.market_id,
                    snapshot.token_id,
                    snapshot.exch_id,
                    snapshot.side,
                    snapshot.order_type,
                    snapshot.price,
                    snapshot.size,
                    snapshot.local_state,
                    snapshot.remote_status_code,
                    snapshot.filled_size_total,
                    snapshot.remaining_size,
                    snapshot.avg_fill_price,
                    snapshot.last_submission_attempt,
                    snapshot.last_event_seq as i64,
                    now,
                    now,
                    snapshot.terminal_at_ms.map(|value| value as i64),
                ],
            )?;
            Ok(())
        })
    }

    pub fn append_order_gateway_event(
        &self,
        event: &OrderGatewayEventInsert<'_>,
    ) -> anyhow::Result<()> {
        let now = now_ms()?;
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO order_gateway_events (
                    seq, created_at_ms, strategy_id, token_id, market_id, local_id, exch_id,
                    event_kind, local_state, remote_status_code, remote_reject_code,
                    remote_reject_reason, fill_delta, fill_total, remaining_size, avg_fill_price,
                    error_code, error_message, raw_json, recovery
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20)
                ",
                params![
                    event.seq as i64,
                    now,
                    event.strategy_id,
                    event.token_id,
                    event.market_id,
                    event.local_id,
                    event.exch_id,
                    event.event_kind,
                    event.local_state,
                    event.remote_status_code,
                    event.remote_reject_code,
                    event.remote_reject_reason,
                    event.fill_delta,
                    event.fill_total,
                    event.remaining_size,
                    event.avg_fill_price,
                    event.error_code,
                    event.error_message,
                    event.raw_json,
                    if event.recovery { 1_i64 } else { 0_i64 },
                ],
            )?;
            Ok(())
        })
    }

    pub fn insert_order_gateway_submission(
        &self,
        submission: &OrderGatewaySubmissionInsert<'_>,
    ) -> anyhow::Result<()> {
        let now = now_ms()?;
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO order_gateway_submissions (
                    local_id, submit_attempt, strategy_id, token_id, side, order_type,
                    price, size, exch_id, unsigned_payload_json, signed_payload_json, signature,
                    signer_address, nonce_or_salt, expiration, exchange_payload_hash,
                    rest_request_json, rest_response_json, rest_status_code, submit_state,
                    created_at_ms, updated_at_ms
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)
                ",
                params![
                    submission.local_id,
                    submission.submit_attempt,
                    submission.strategy_id,
                    submission.token_id,
                    submission.side,
                    submission.order_type,
                    submission.price,
                    submission.size,
                    submission.exch_id,
                    submission.unsigned_payload_json,
                    submission.signed_payload_json,
                    submission.signature,
                    submission.signer_address,
                    submission.nonce_or_salt,
                    submission.expiration,
                    submission.exchange_payload_hash,
                    submission.rest_request_json,
                    submission.rest_response_json,
                    submission.rest_status_code,
                    submission.submit_state,
                    now,
                    now,
                ],
            )?;
            Ok(())
        })
    }

    pub fn append_position_journal(&self, event: &PositionJournalInsert<'_>) -> anyhow::Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO position_journal (
                    seq, ts_ms, event_type, strategy_id, token_id, local_order_id,
                    exchange_order_id, side, qty, price, source, recovery, payload_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                ",
                params![
                    event.seq as i64,
                    event.ts_ms as i64,
                    event.event_type,
                    event.strategy_id,
                    event.token_id,
                    event.local_order_id,
                    event.exchange_order_id,
                    event.side,
                    event.qty,
                    event.price,
                    event.source,
                    if event.recovery { 1_i64 } else { 0_i64 },
                    event.payload_json,
                ],
            )?;
            Ok(())
        })
    }

    pub fn insert_position_snapshot_rows(
        &self,
        snapshot_id: u64,
        seq: u64,
        ts_ms: u64,
        rows: &[PositionSnapshotRow<'_>],
        open_orders: &[PositionOpenOrderSnapshotRow<'_>],
    ) -> anyhow::Result<()> {
        self.with_conn(|conn| {
            conn.execute_batch("BEGIN IMMEDIATE")?;
            let result = (|| -> anyhow::Result<()> {
                for row in rows {
                    conn.execute(
                        "
                        INSERT INTO position_snapshots (
                            snapshot_id, seq, ts_ms, scope_type, strategy_id, token_id,
                            filled_position, cost_basis, realized_pnl, working_buy_exposure,
                            working_sell_exposure
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                        ",
                        params![
                            snapshot_id as i64,
                            seq as i64,
                            ts_ms as i64,
                            row.scope_type,
                            row.strategy_id,
                            row.token_id,
                            row.filled_position,
                            row.cost_basis,
                            row.realized_pnl,
                            row.working_buy_exposure,
                            row.working_sell_exposure,
                        ],
                    )?;
                }
                for order in open_orders {
                    conn.execute(
                        "
                        INSERT INTO position_open_orders_snapshot (
                            snapshot_id, seq, strategy_id, token_id, local_order_id,
                            exchange_order_id, side, price, original_size, remaining_size, local_state
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                        ",
                        params![
                            order.snapshot_id as i64,
                            order.seq as i64,
                            order.strategy_id,
                            order.token_id,
                            order.local_order_id,
                            order.exchange_order_id,
                            order.side,
                            order.price,
                            order.original_size,
                            order.remaining_size,
                            order.local_state,
                        ],
                    )?;
                }
                Ok(())
            })();
            if let Err(error) = result {
                conn.execute_batch("ROLLBACK")?;
                return Err(error);
            }
            conn.execute_batch("COMMIT")?;
            Ok(())
        })
    }

    pub fn insert_position_reconciliation(
        &self,
        reconciliation: &PositionReconciliationInsert<'_>,
    ) -> anyhow::Result<()> {
        self.with_conn(|conn| {
            conn.execute(
                "
                INSERT INTO position_reconciliations (
                    reconciliation_id, started_at_ms, exchange_data_as_of_ms,
                    last_local_seq_compared, status, mismatch_count, adjustment_journal_seq,
                    summary_json, alert_message
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ",
                params![
                    reconciliation.reconciliation_id,
                    reconciliation.started_at_ms as i64,
                    reconciliation.exchange_data_as_of_ms as i64,
                    reconciliation.last_local_seq_compared as i64,
                    reconciliation.status,
                    reconciliation.mismatch_count as i64,
                    reconciliation
                        .adjustment_journal_seq
                        .map(|value| value as i64),
                    reconciliation.summary_json,
                    reconciliation.alert_message,
                ],
            )?;
            Ok(())
        })
    }

    pub fn load_position_journal_after(
        &self,
        seq: u64,
    ) -> anyhow::Result<Vec<StoredPositionJournalRow>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "
                SELECT seq, event_type, strategy_id, token_id, local_order_id, side, qty, price, payload_json
                FROM position_journal
                WHERE seq > ?1
                ORDER BY seq ASC
                ",
            )?;
            let rows = stmt.query_map(params![seq as i64], |row| {
                Ok(StoredPositionJournalRow {
                    seq: row.get::<_, i64>(0)? as u64,
                    event_type: row.get(1)?,
                    strategy_id: row.get(2)?,
                    token_id: row.get(3)?,
                    local_order_id: row.get(4)?,
                    side: row.get(5)?,
                    qty: row.get(6)?,
                    price: row.get(7)?,
                    payload_json: row.get(8)?,
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
    }

    pub fn load_latest_position_snapshot(
        &self,
    ) -> anyhow::Result<Option<StoredPositionSnapshotBatch>> {
        self.with_conn(|conn| {
            let snapshot_id = conn
                .query_row(
                    "SELECT snapshot_id FROM position_snapshots ORDER BY snapshot_id DESC LIMIT 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?;
            let Some(snapshot_id) = snapshot_id else {
                return Ok(None);
            };
            let mut row_stmt = conn.prepare(
                "
                SELECT seq, ts_ms, scope_type, strategy_id, token_id, filled_position,
                       cost_basis, realized_pnl, working_buy_exposure, working_sell_exposure
                FROM position_snapshots
                WHERE snapshot_id = ?1
                ORDER BY scope_type, strategy_id, token_id
                ",
            )?;
            let rows = row_stmt
                .query_map(params![snapshot_id], |row| {
                    Ok((
                        row.get::<_, i64>(0)? as u64,
                        row.get::<_, i64>(1)? as u64,
                        StoredPositionSnapshotRow {
                            scope_type: row.get(2)?,
                            strategy_id: row.get(3)?,
                            token_id: row.get(4)?,
                            filled_position: row.get(5)?,
                            cost_basis: row.get(6)?,
                            realized_pnl: row.get(7)?,
                            working_buy_exposure: row.get(8)?,
                            working_sell_exposure: row.get(9)?,
                        },
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let seq = rows.first().map(|row| row.0).unwrap_or(0);
            let ts_ms = rows.first().map(|row| row.1).unwrap_or(0);
            let rows = rows.into_iter().map(|row| row.2).collect::<Vec<_>>();

            let mut order_stmt = conn.prepare(
                "
                SELECT snapshot_id, seq, strategy_id, token_id, local_order_id,
                       exchange_order_id, side, price, original_size, remaining_size, local_state
                FROM position_open_orders_snapshot
                WHERE snapshot_id = ?1
                ORDER BY local_order_id
                ",
            )?;
            let open_orders = order_stmt
                .query_map(params![snapshot_id], |row| {
                    Ok(StoredPositionOpenOrderSnapshotRow {
                        snapshot_id: row.get::<_, i64>(0)? as u64,
                        seq: row.get::<_, i64>(1)? as u64,
                        strategy_id: row.get(2)?,
                        token_id: row.get(3)?,
                        local_order_id: row.get(4)?,
                        exchange_order_id: row.get(5)?,
                        side: row.get(6)?,
                        price: row.get(7)?,
                        original_size: row.get(8)?,
                        remaining_size: row.get(9)?,
                        local_state: row.get(10)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Some(StoredPositionSnapshotBatch {
                snapshot_id: snapshot_id as u64,
                seq,
                ts_ms,
                rows,
                open_orders,
            }))
        })
    }

    pub fn load_max_order_gateway_event_seq(&self) -> anyhow::Result<u64> {
        self.with_conn(|conn| {
            let seq = conn.query_row(
                "SELECT COALESCE(MAX(seq), 0) FROM order_gateway_events",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            Ok(seq as u64)
        })
    }

    pub fn load_order_gateway_recoverable_orders(
        &self,
    ) -> anyhow::Result<Vec<OrderGatewayOrderSnapshot>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "
                SELECT strategy_id, market_id, token_id, local_id, exch_id, side, order_type,
                       price, size, local_state, remote_status_code, filled_size_total,
                       remaining_size, avg_fill_price, last_submission_attempt, last_event_seq,
                       terminal_at_ms
                FROM order_gateway_orders
                WHERE local_state NOT IN ('Filled', 'Cancelled', 'Expired', 'Rejected', 'Failed', 'UnknownTerminal')
                ",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok(OrderGatewayOrderSnapshot {
                    strategy_id: row.get(0)?,
                    market_id: row.get(1)?,
                    token_id: row.get(2)?,
                    local_id: row.get(3)?,
                    exch_id: row.get(4)?,
                    side: row.get(5)?,
                    order_type: row.get(6)?,
                    price: row.get(7)?,
                    size: row.get(8)?,
                    local_state: row.get(9)?,
                    remote_status_code: row.get(10)?,
                    filled_size_total: row.get(11)?,
                    remaining_size: row.get(12)?,
                    avg_fill_price: row.get(13)?,
                    last_submission_attempt: row.get(14)?,
                    last_event_seq: row.get::<_, i64>(15)? as u64,
                    terminal_at_ms: row.get::<_, Option<i64>>(16)?.map(|value| value as u64),
                })
            })?;
            rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
        })
    }

    pub fn load_latest_order_gateway_submission(
        &self,
        local_id: &str,
    ) -> anyhow::Result<Option<StoredOrderGatewaySubmission>> {
        self.with_conn(|conn| {
            conn.query_row(
                "
                SELECT local_id, submit_attempt, signed_payload_json, exchange_payload_hash, submit_state
                FROM order_gateway_submissions
                WHERE local_id = ?1
                ORDER BY submit_attempt DESC
                LIMIT 1
                ",
                params![local_id],
                |row| {
                    Ok(StoredOrderGatewaySubmission {
                        local_id: row.get(0)?,
                        submit_attempt: row.get(1)?,
                        signed_payload_json: row.get(2)?,
                        exchange_payload_hash: row.get(3)?,
                        submit_state: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
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

                CREATE TABLE IF NOT EXISTS account_fund_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    checked_at_ms INTEGER NOT NULL,
                    balance TEXT NOT NULL,
                    allowances_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_account_fund_snapshots_checked_at
                    ON account_fund_snapshots (checked_at_ms DESC);

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
                    volume_24hr_clob TEXT,
                    volume_24hr TEXT,
                    liquidity_reward_roi TEXT,
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
            ensure_column(conn, "reward_market_pool_state", "volume_24hr_clob", "TEXT")?;
            ensure_column(conn, "reward_market_pool_state", "volume_24hr", "TEXT")?;
            ensure_column(
                conn,
                "reward_market_pool_state",
                "liquidity_reward_roi",
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
                        market_daily_reward, volume_24hr_clob, volume_24hr,
                        liquidity_reward_roi, build_date_utc, pool_version, in_pool,
                        first_seen_at_ms, last_seen_at_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, 1, ?16, ?16)
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
                        entry.volume_24hr_clob,
                        entry.volume_24hr,
                        entry.liquidity_reward_roi,
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
                       market_daily_reward, volume_24hr_clob, volume_24hr,
                       liquidity_reward_roi, build_date_utc, pool_version,
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
                       market_daily_reward, volume_24hr_clob, volume_24hr,
                       liquidity_reward_roi, build_date_utc, pool_version,
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
                .liquidity_reward_roi
                .and_then(|value| value.parse::<f64>().ok())
                .map(|roi| (entry, roi))
        })
        .collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.condition_id.cmp(right.0.condition_id))
    });

    sorted
        .into_iter()
        .take(market_count)
        .enumerate()
        .map(|(index, (entry, _))| LiquidityRewardPoolSelection {
            condition_id: entry.condition_id,
            reason: "roi_descending_top_n",
            rank: index as u32 + 1,
        })
        .collect()
}

fn active_reward_market_pool_entry_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ActiveRewardMarketPoolEntry> {
    let pool_version = row
        .get::<_, Option<i64>>(14)?
        .and_then(|value| u64::try_from(value).ok());
    let liquidity_reward_selected_at_ms = row
        .get::<_, Option<i64>>(16)?
        .and_then(|value| u64::try_from(value).ok());
    let liquidity_reward_select_rank = row
        .get::<_, Option<i64>>(18)?
        .and_then(|value| u32::try_from(value).ok());
    let liquidity_reward_halted_at_ms = row
        .get::<_, Option<i64>>(20)?
        .and_then(|value| u64::try_from(value).ok());
    let liquidity_reward_halted_pool_version = row
        .get::<_, Option<i64>>(22)?
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
        volume_24hr_clob: row.get(10)?,
        volume_24hr: row.get(11)?,
        liquidity_reward_roi: row.get(12)?,
        build_date_utc: row.get(13)?,
        pool_version,
        liquidity_reward_selected: row.get::<_, i64>(15)? != 0,
        liquidity_reward_selected_at_ms,
        liquidity_reward_select_reason: row.get(17)?,
        liquidity_reward_select_rank,
        liquidity_reward_halted: row.get::<_, i64>(19)? != 0,
        liquidity_reward_halted_at_ms,
        liquidity_reward_halt_reason: row.get(21)?,
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

    fn dec(value: &str) -> Decimal {
        Decimal::from_str(value).expect("decimal should parse")
    }

    #[test]
    fn daily_loss_state_roundtrips_and_halts() {
        let store = OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let state = StoredDailyLossState {
            trading_day: "2026-05-22".to_string(),
            day_start_total_pnl: dec("1.5"),
            day_start_equity: dec("1000"),
            loss_limit_ratio: dec("0.03"),
            loss_limit_amount: dec("30"),
            halted: false,
            halt_reason: None,
            halted_at_ms: None,
            updated_at_ms: 100,
        };

        store
            .upsert_daily_loss_state(&state)
            .expect("state should upsert");
        let loaded = store
            .load_daily_loss_state("2026-05-22")
            .expect("state should load")
            .expect("state should exist");
        assert_eq!(loaded.day_start_total_pnl, dec("1.5"));
        assert_eq!(loaded.day_start_equity, dec("1000"));
        assert!(!loaded.halted);

        store
            .halt_daily_loss_state("2026-05-22", "loss reached", 200)
            .expect("state should halt");
        let halted = store
            .load_daily_loss_state("2026-05-22")
            .expect("state should load")
            .expect("state should exist");
        assert!(halted.halted);
        assert_eq!(halted.halt_reason, Some("loss reached".to_string()));
        assert_eq!(halted.halted_at_ms, Some(200));
        assert_eq!(halted.updated_at_ms, 200);

        let missing = store
            .halt_daily_loss_state("2099-01-01", "missing", 300)
            .expect_err("missing state should not halt");
        assert!(missing.to_string().contains("daily loss state not found"));
    }

    #[test]
    fn order_gateway_schema_persists_snapshot_event_and_submission() {
        let store = OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");

        let snapshot = OrderGatewayOrderSnapshot {
            strategy_id: "liquidity_reward".to_string(),
            market_id: Some("liquidity_reward".to_string()),
            token_id: "token-1".to_string(),
            local_id: "local-1".to_string(),
            exch_id: Some("exch-1".to_string()),
            side: "Buy".to_string(),
            order_type: "LimitGtc".to_string(),
            price: Some("0.42".to_string()),
            size: "10".to_string(),
            local_state: "Open".to_string(),
            remote_status_code: Some("open".to_string()),
            filled_size_total: "0".to_string(),
            remaining_size: "10".to_string(),
            avg_fill_price: None,
            last_submission_attempt: Some(1),
            last_event_seq: 7,
            terminal_at_ms: None,
        };
        store
            .upsert_order_gateway_order(&snapshot)
            .expect("snapshot should persist");
        store
            .append_order_gateway_event(&OrderGatewayEventInsert {
                seq: 7,
                strategy_id: "liquidity_reward",
                token_id: "token-1",
                market_id: Some("liquidity_reward"),
                local_id: Some("local-1"),
                exch_id: Some("exch-1"),
                event_kind: "Open",
                local_state: "Open",
                remote_status_code: Some("open"),
                remote_reject_code: None,
                remote_reject_reason: None,
                fill_delta: None,
                fill_total: Some("0"),
                remaining_size: Some("10"),
                avg_fill_price: None,
                error_code: None,
                error_message: None,
                raw_json: "{}",
                recovery: false,
            })
            .expect("event should persist");
        store
            .insert_order_gateway_submission(&OrderGatewaySubmissionInsert {
                local_id: "local-1",
                submit_attempt: 1,
                strategy_id: "liquidity_reward",
                token_id: "token-1",
                side: "Buy",
                order_type: "LimitGtc",
                price: Some("0.42"),
                size: "10",
                exch_id: Some("exch-1"),
                unsigned_payload_json: "{\"unsigned\":true}",
                signed_payload_json: "{\"signed\":true}",
                signature: "0xsig",
                signer_address: "0xsigner",
                nonce_or_salt: Some("salt-1"),
                expiration: None,
                exchange_payload_hash: "hash-1",
                rest_request_json: "{\"request\":true}",
                rest_response_json: Some("{\"ok\":true}"),
                rest_status_code: Some(200),
                submit_state: "Submitted",
            })
            .expect("submission should persist");

        let active = store
            .load_order_gateway_recoverable_orders()
            .expect("recoverable orders should load");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].local_id, "local-1");
        assert_eq!(active[0].exch_id.as_deref(), Some("exch-1"));
    }

    #[test]
    fn load_max_order_gateway_event_seq_reads_latest_event_seq() {
        let store = OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");

        assert_eq!(
            store
                .load_max_order_gateway_event_seq()
                .expect("empty max seq should load"),
            0
        );

        store
            .append_order_gateway_event(&OrderGatewayEventInsert {
                seq: 33,
                strategy_id: "market_maker",
                token_id: "token-1",
                market_id: Some("market-1"),
                local_id: Some("local-1"),
                exch_id: Some("exch-1"),
                event_kind: "Open",
                local_state: "Open",
                remote_status_code: Some("open"),
                remote_reject_code: None,
                remote_reject_reason: None,
                fill_delta: None,
                fill_total: Some("0"),
                remaining_size: Some("10"),
                avg_fill_price: None,
                error_code: None,
                error_message: None,
                raw_json: "{}",
                recovery: false,
            })
            .expect("event should persist");

        assert_eq!(
            store
                .load_max_order_gateway_event_seq()
                .expect("max seq should load"),
            33
        );
    }

    #[test]
    fn position_engine_schema_persists_journal_snapshot_open_order_and_reconciliation() {
        let store = OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");

        store
            .append_position_journal(&PositionJournalInsert {
                seq: 10,
                ts_ms: 1000,
                event_type: "OrderFillApplied",
                strategy_id: Some("strategy-a"),
                token_id: "token-1",
                local_order_id: Some("local-1"),
                exchange_order_id: Some("exch-1"),
                side: Some("Buy"),
                qty: Some("4"),
                price: Some("0.4"),
                source: "Live",
                recovery: false,
                payload_json: "{}",
            })
            .expect("journal should persist");

        store
            .insert_position_snapshot_rows(
                3,
                10,
                1000,
                &[PositionSnapshotRow {
                    scope_type: "strategy",
                    strategy_id: Some("strategy-a"),
                    token_id: "token-1",
                    filled_position: "4",
                    cost_basis: "1.6",
                    realized_pnl: "0",
                    working_buy_exposure: "6",
                    working_sell_exposure: "0",
                }],
                &[PositionOpenOrderSnapshotRow {
                    snapshot_id: 3,
                    seq: 10,
                    strategy_id: "strategy-a",
                    token_id: "token-1",
                    local_order_id: "local-1",
                    exchange_order_id: Some("exch-1"),
                    side: "Buy",
                    price: "0.4",
                    original_size: "10",
                    remaining_size: "6",
                    local_state: "Open",
                }],
            )
            .expect("snapshot should persist");

        store
            .insert_position_reconciliation(&PositionReconciliationInsert {
                reconciliation_id: "recon-1",
                started_at_ms: 1000,
                exchange_data_as_of_ms: 1100,
                last_local_seq_compared: 10,
                status: "Adjusted",
                mismatch_count: 1,
                adjustment_journal_seq: Some(11),
                summary_json: "{}",
                alert_message: Some("position mismatch"),
            })
            .expect("reconciliation should persist");

        let latest = store
            .load_latest_position_snapshot()
            .expect("snapshot should load")
            .expect("snapshot should exist");
        assert_eq!(latest.snapshot_id, 3);
        assert_eq!(latest.seq, 10);
        assert_eq!(latest.rows.len(), 1);
        assert_eq!(latest.rows[0].cost_basis, "1.6");
        assert_eq!(latest.open_orders.len(), 1);
        assert_eq!(latest.open_orders[0].remaining_size, "6");

        let journal = store
            .load_position_journal_after(9)
            .expect("journal should load");
        assert_eq!(journal.len(), 1);
        assert_eq!(journal[0].seq, 10);
        assert_eq!(journal[0].event_type, "OrderFillApplied");
    }

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
    fn reward_market_pool_persists_volume_and_roi_fields() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");

        let entries = vec![RewardMarketPoolStorageEntry {
            condition_id: "0xabc",
            market_slug: Some("test-market"),
            question: Some("Test market?"),
            token1: "token1",
            token2: "token2",
            tokens_json: "[]",
            market_competitiveness: Some("12.5"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("25"),
            volume_24hr_clob: Some("60000"),
            volume_24hr: Some("65000"),
            liquidity_reward_roi: Some("0.25"),
        }];

        store
            .replace_reward_market_pool_entries(
                NaiveDate::from_ymd_opt(2026, 5, 13).unwrap(),
                123,
                &entries,
                456,
                1,
            )
            .expect("pool entries should replace");

        let loaded = store
            .load_active_reward_market_pool_entries()
            .expect("active pool entries should load");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].volume_24hr_clob.as_deref(), Some("60000"));
        assert_eq!(loaded[0].volume_24hr.as_deref(), Some("65000"));
        assert_eq!(loaded[0].liquidity_reward_roi.as_deref(), Some("0.25"));
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
            volume_24hr_clob: None,
            volume_24hr: None,
            liquidity_reward_roi: None,
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
                volume_24hr_clob: None,
                volume_24hr: None,
                liquidity_reward_roi: Some("0.5"),
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
                volume_24hr_clob: None,
                volume_24hr: None,
                liquidity_reward_roi: Some("0.4"),
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
            volume_24hr_clob: None,
            volume_24hr: None,
            liquidity_reward_roi: Some("0.5"),
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
            volume_24hr_clob: None,
            volume_24hr: None,
            liquidity_reward_roi: None,
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
            volume_24hr_clob: None,
            volume_24hr: None,
            liquidity_reward_roi: None,
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
            volume_24hr_clob: None,
            volume_24hr: None,
            liquidity_reward_roi: Some("0.5"),
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
                volume_24hr_clob: None,
                volume_24hr: None,
                liquidity_reward_roi: None,
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
                volume_24hr_clob: None,
                volume_24hr: None,
                liquidity_reward_roi: None,
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

    fn reward_pool_entry<'a>(
        condition_id: &'a str,
        token1: &'a str,
        token2: &'a str,
        market_competitiveness: Option<&'a str>,
        liquidity_reward_roi: Option<&'a str>,
    ) -> RewardMarketPoolStorageEntry<'a> {
        RewardMarketPoolStorageEntry {
            condition_id,
            market_slug: None,
            question: None,
            token1,
            token2,
            tokens_json: "[]",
            market_competitiveness,
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("50"),
            volume_24hr_clob: None,
            volume_24hr: None,
            liquidity_reward_roi,
        }
    }

    #[test]
    fn load_liquidity_reward_pool_entries_selects_roi_descending_top_n() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let build_date = NaiveDate::from_ymd_opt(2026, 5, 4).unwrap();
        let entries = vec![
            reward_pool_entry("0x0", "token10", "token20", Some("0"), Some("0.10")),
            reward_pool_entry("0x1", "token11", "token21", Some("1"), Some("0.50")),
            reward_pool_entry("0x2", "token12", "token22", Some("2"), Some("0.30")),
            reward_pool_entry("0x3", "token13", "token23", Some("3"), Some("0.20")),
            reward_pool_entry("0x4", "token14", "token24", Some("4"), Some("0.40")),
        ];
        store
            .replace_reward_market_pool_entries(build_date, 123, &entries, 200, 3)
            .expect("pool should replace");
        let selected = store
            .load_liquidity_reward_pool_entries()
            .expect("pool entries should load");
        let condition_ids = selected
            .iter()
            .map(|entry| entry.condition_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(condition_ids, vec!["0x1", "0x4", "0x2"]);
        assert!(selected.iter().all(|entry| entry.liquidity_reward_selected));
        assert_eq!(
            selected
                .iter()
                .map(|entry| entry.liquidity_reward_select_reason.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("roi_descending_top_n"); 3]
        );
        assert_eq!(
            selected
                .iter()
                .map(|entry| entry.liquidity_reward_select_rank)
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3)]
        );
        assert_eq!(
            selected
                .iter()
                .map(|entry| entry.liquidity_reward_selected_at_ms)
                .collect::<Vec<_>>(),
            vec![Some(200); 3]
        );

        store
            .kick_reward_market_pool_entry("0x4", "out", 250)
            .expect("kick should update");
        let selected_after_kick = store
            .load_liquidity_reward_pool_entries()
            .expect("pool entries should load");
        let condition_ids_after_kick = selected_after_kick
            .iter()
            .map(|entry| entry.condition_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(condition_ids_after_kick, vec!["0x1", "0x2"]);
    }

    #[test]
    fn load_liquidity_reward_pool_entries_skips_entries_without_parseable_roi() {
        let store = MarketStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let build_date = NaiveDate::from_ymd_opt(2026, 5, 4).unwrap();
        let entries = vec![
            reward_pool_entry(
                "0xinvalid",
                "invalid token1",
                "invalid token2",
                Some("10"),
                Some("not-a-number"),
            ),
            reward_pool_entry(
                "0xmissing",
                "missing token1",
                "missing token2",
                Some("20"),
                None,
            ),
            reward_pool_entry(
                "0xlow",
                "low token1",
                "low token2",
                Some("30"),
                Some("0.25"),
            ),
            reward_pool_entry(
                "0xhigh",
                "high token1",
                "high token2",
                Some("40"),
                Some("0.75"),
            ),
        ];
        store
            .replace_reward_market_pool_entries(build_date, 123, &entries, 200, 3)
            .expect("pool should replace");
        let selected = store
            .load_liquidity_reward_pool_entries()
            .expect("pool entries should load");
        let condition_ids = selected
            .iter()
            .map(|entry| entry.condition_id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(condition_ids, vec!["0xhigh", "0xlow"]);
        assert_eq!(
            selected
                .iter()
                .map(|entry| entry.liquidity_reward_select_rank)
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2)]
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
