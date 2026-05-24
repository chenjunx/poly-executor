use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use arc_swap::ArcSwapOption;
use log::warn;
use polymarket_client_sdk_v2::types::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::order_gateway::{
    OrderEventEnvelope, OrderEventPollError, OrderEventSubscriber, OrderSide,
};
use crate::storage::{OrderStore, PositionJournalInsert};

#[derive(Debug, Clone, PartialEq)]
pub struct PositionEntrySnapshot {
    pub filled_position: Decimal,
    pub cost_basis: Decimal,
    pub realized_pnl: Decimal,
    pub working_buy_exposure: Decimal,
    pub working_sell_exposure: Decimal,
    pub last_update_seq: u64,
    pub last_update_ts_ms: u64,
    pub degraded: bool,
}

impl PositionEntrySnapshot {
    pub fn avg_cost(&self) -> Option<Decimal> {
        if self.filled_position == Decimal::ZERO {
            None
        } else {
            Some(self.cost_basis / self.filled_position)
        }
    }

    pub fn theoretical_min(&self) -> Decimal {
        self.filled_position - self.working_sell_exposure
    }

    pub fn theoretical_max(&self) -> Decimal {
        self.filled_position + self.working_buy_exposure
    }

    pub fn theoretical_net(&self) -> Decimal {
        self.filled_position + self.working_buy_exposure - self.working_sell_exposure
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionEventSource {
    Live,
    Recovery,
    Reconciliation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionTerminalReason {
    Cancelled,
    Expired,
    LocalRejected,
    RemoteRejected,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PositionEntryKey {
    Strategy {
        strategy_id: String,
        token_id: String,
    },
    Global {
        token_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PositionEvent {
    OrderWorkingRegistered {
        strategy_id: String,
        token_id: String,
        local_order_id: String,
        exchange_order_id: Option<String>,
        side: PositionSide,
        price: Decimal,
        size: Decimal,
        seq: u64,
        ts_ms: u64,
        source: PositionEventSource,
        recovery: bool,
    },
    OrderFillApplied {
        strategy_id: String,
        token_id: String,
        local_order_id: String,
        exchange_order_id: Option<String>,
        side: PositionSide,
        fill_qty: Decimal,
        fill_price: Decimal,
        cum_qty: Option<Decimal>,
        seq: u64,
        ts_ms: u64,
        source: PositionEventSource,
        recovery: bool,
    },
    OrderTerminalApplied {
        strategy_id: String,
        token_id: String,
        local_order_id: String,
        reason: PositionTerminalReason,
        seq: u64,
        ts_ms: u64,
        source: PositionEventSource,
        recovery: bool,
    },
    OrderStale {
        strategy_id: String,
        token_id: String,
        local_order_id: String,
        seq: u64,
        ts_ms: u64,
        source: PositionEventSource,
        recovery: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
struct PositionEntryState {
    filled_position: Decimal,
    cost_basis: Decimal,
    realized_pnl: Decimal,
    working_buy_exposure: Decimal,
    working_sell_exposure: Decimal,
    last_update_seq: u64,
    last_update_ts_ms: u64,
}

impl Default for PositionEntryState {
    fn default() -> Self {
        Self {
            filled_position: Decimal::ZERO,
            cost_basis: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            working_buy_exposure: Decimal::ZERO,
            working_sell_exposure: Decimal::ZERO,
            last_update_seq: 0,
            last_update_ts_ms: 0,
        }
    }
}

impl PositionEntryState {
    fn snapshot(&self, degraded: bool) -> PositionEntrySnapshot {
        PositionEntrySnapshot {
            filled_position: self.filled_position,
            cost_basis: self.cost_basis,
            realized_pnl: self.realized_pnl,
            working_buy_exposure: self.working_buy_exposure,
            working_sell_exposure: self.working_sell_exposure,
            last_update_seq: self.last_update_seq,
            last_update_ts_ms: self.last_update_ts_ms,
            degraded,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct OpenOrderState {
    strategy_id: String,
    token_id: String,
    local_order_id: String,
    exchange_order_id: Option<String>,
    side: PositionSide,
    price: Decimal,
    original_size: Decimal,
    remaining_size: Decimal,
    terminal: bool,
}

#[derive(Debug, Default)]
pub struct PositionKeeper {
    strategy_entries: HashMap<(String, String), PositionEntryState>,
    global_entries: HashMap<String, PositionEntryState>,
    open_orders: HashMap<String, OpenOrderState>,
    degraded: bool,
}

type PositionReadRegistry =
    std::sync::RwLock<HashMap<PositionEntryKey, Arc<ArcSwapOption<PositionEntrySnapshot>>>>;

#[derive(Debug, Clone, PartialEq)]
pub struct PositionKeeperSnapshot {
    pub seq: u64,
    pub ts_ms: u64,
    pub entries: Vec<(PositionEntryKey, PositionEntrySnapshot)>,
    pub open_orders: Vec<PositionOpenOrderSnapshot>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PositionOpenOrderSnapshot {
    pub strategy_id: String,
    pub token_id: String,
    pub local_order_id: String,
    pub exchange_order_id: Option<String>,
    pub side: PositionSide,
    pub price: Decimal,
    pub original_size: Decimal,
    pub remaining_size: Decimal,
    pub terminal: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PositionReconciliationAdjustment {
    pub reconciliation_id: String,
    pub exchange_data_as_of_ms: u64,
    pub last_local_seq_compared: u64,
    pub strategy_id: String,
    pub token_id: String,
    pub exchange_filled_position: Decimal,
    pub exchange_cost_basis: Decimal,
    pub exchange_realized_pnl: Decimal,
    pub exchange_working_buy_exposure: Decimal,
    pub exchange_working_sell_exposure: Decimal,
    pub reason: String,
    pub seq: u64,
    pub ts_ms: u64,
}

#[derive(Default)]
pub struct PositionSnapshotPublisher {
    entries: Arc<PositionReadRegistry>,
}

#[derive(Clone)]
pub struct PositionReadHandle {
    entries: Arc<PositionReadRegistry>,
}

#[derive(Clone)]
pub struct PositionStatusHandle {
    status: Arc<AtomicU8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionEngineStatus {
    Recovering,
    Live,
    Degraded,
    Stopped,
}

impl PositionEngineStatus {
    fn as_u8(self) -> u8 {
        match self {
            Self::Recovering => 0,
            Self::Live => 1,
            Self::Degraded => 2,
            Self::Stopped => 3,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Live,
            2 => Self::Degraded,
            3 => Self::Stopped,
            _ => Self::Recovering,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PositionPersistRecord {
    Journal(PositionEvent),
}

impl PositionPersistRecord {
    pub fn seq(&self) -> u64 {
        match self {
            Self::Journal(event) => event.seq(),
        }
    }
}

pub fn recover_keeper(store: &OrderStore) -> anyhow::Result<PositionKeeper> {
    let snapshot = store.load_latest_position_snapshot()?;
    let snapshot_seq = snapshot.as_ref().map(|snapshot| snapshot.seq).unwrap_or(0);
    let mut keeper = match snapshot {
        Some(snapshot) => PositionKeeper::from_snapshot(position_snapshot_from_stored(snapshot)?)?,
        None => PositionKeeper::default(),
    };
    let events = store
        .load_position_journal_after(snapshot_seq)?
        .into_iter()
        .filter_map(
            |row| match serde_json::from_str::<PositionEvent>(&row.payload_json) {
                Ok(event) => Some(event),
                Err(error) => {
                    warn!(
                        "position journal payload 解析失败，跳过该记录 seq={:?} error={}",
                        row.seq, error
                    );
                    None
                }
            },
        )
        .collect::<Vec<_>>();
    keeper.apply_replay_events(events)?;
    Ok(keeper)
}

pub async fn run_order_event_bridge(
    mut subscriber: OrderEventSubscriber,
    ingest_handle: PositionIngestHandle,
) {
    loop {
        match subscriber.recv_relevant().await {
            Ok(event) => {
                let Some(position_event) = position_event_from_order_event(&event) else {
                    continue;
                };
                if let Err(error) = ingest_handle.try_ingest(position_event) {
                    ingest_handle.mark_degraded();
                    warn!("position engine 投递订单事件失败 error={:?}", error);
                }
            }
            Err(OrderEventPollError::Lagged { skipped }) => {
                ingest_handle.mark_degraded();
                warn!(
                    "position engine 订单事件订阅落后，仓位状态已标记 degraded skipped={:?}",
                    skipped
                );
            }
            Err(OrderEventPollError::Closed) => {
                warn!("position engine 订单事件订阅已关闭");
                return;
            }
            Err(OrderEventPollError::Empty) => {}
        }
    }
}

pub async fn run_persist_task(
    store: OrderStore,
    mut persist_rx: mpsc::Receiver<PositionPersistRecord>,
) {
    while let Some(record) = persist_rx.recv().await {
        if let Err(error) = persist_record(&store, &record) {
            warn!(
                "position journal 持久化失败 seq={:?} error={}",
                record.seq(),
                error
            );
        }
    }
}

fn persist_record(store: &OrderStore, record: &PositionPersistRecord) -> anyhow::Result<()> {
    match record {
        PositionPersistRecord::Journal(event) => append_position_event(store, event),
    }
}

fn append_position_event(store: &OrderStore, event: &PositionEvent) -> anyhow::Result<()> {
    let payload_json = serde_json::to_string(event)?;
    let qty = position_event_qty(event).map(|value| value.to_string());
    let price = position_event_price(event).map(|value| value.to_string());
    store.append_position_journal(&PositionJournalInsert {
        seq: event.seq(),
        ts_ms: position_event_ts_ms(event),
        event_type: position_event_type(event),
        strategy_id: position_event_strategy_id(event),
        token_id: position_event_token_id(event),
        local_order_id: position_event_local_order_id(event),
        exchange_order_id: position_event_exchange_order_id(event),
        side: position_event_side(event).map(position_side_label),
        qty: qty.as_deref(),
        price: price.as_deref(),
        source: position_event_source(event),
        recovery: position_event_recovery(event),
        payload_json: &payload_json,
    })
}

fn position_snapshot_from_stored(
    stored: crate::storage::StoredPositionSnapshotBatch,
) -> anyhow::Result<PositionKeeperSnapshot> {
    let entries = stored
        .rows
        .into_iter()
        .map(|row| {
            let snapshot = PositionEntrySnapshot {
                filled_position: Decimal::from_str(&row.filled_position)?,
                cost_basis: Decimal::from_str(&row.cost_basis)?,
                realized_pnl: Decimal::from_str(&row.realized_pnl)?,
                working_buy_exposure: Decimal::from_str(&row.working_buy_exposure)?,
                working_sell_exposure: Decimal::from_str(&row.working_sell_exposure)?,
                last_update_seq: stored.seq,
                last_update_ts_ms: stored.ts_ms,
                degraded: false,
            };
            let key = match row.scope_type.as_str() {
                "strategy" => PositionEntryKey::Strategy {
                    strategy_id: row.strategy_id.unwrap_or_default(),
                    token_id: row.token_id,
                },
                _ => PositionEntryKey::Global {
                    token_id: row.token_id,
                },
            };
            Ok((key, snapshot))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let open_orders = stored
        .open_orders
        .into_iter()
        .map(|row| {
            Ok(PositionOpenOrderSnapshot {
                strategy_id: row.strategy_id,
                token_id: row.token_id,
                local_order_id: row.local_order_id,
                exchange_order_id: row.exchange_order_id,
                side: position_side_from_label(&row.side)?,
                price: Decimal::from_str(&row.price)?,
                original_size: Decimal::from_str(&row.original_size)?,
                remaining_size: Decimal::from_str(&row.remaining_size)?,
                terminal: matches!(
                    row.local_state.as_str(),
                    "Filled" | "Cancelled" | "Rejected" | "Failed" | "UnknownTerminal"
                ),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(PositionKeeperSnapshot {
        seq: stored.seq,
        ts_ms: stored.ts_ms,
        entries,
        open_orders,
    })
}

pub fn position_event_from_order_event(event: &OrderEventEnvelope) -> Option<PositionEvent> {
    let order = event.order.as_ref()?;
    position_event_from_gateway_event(
        event,
        position_side_from_order_side(order.side),
        order.original_size,
        order.price.unwrap_or(Decimal::ZERO),
    )
}

fn position_side_from_order_side(side: OrderSide) -> PositionSide {
    match side {
        OrderSide::Buy => PositionSide::Buy,
        OrderSide::Sell => PositionSide::Sell,
    }
}

pub fn position_event_from_gateway_event(
    event: &OrderEventEnvelope,
    side: PositionSide,
    original_size: Decimal,
    price: Decimal,
) -> Option<PositionEvent> {
    let strategy_id = event.strategy_id.as_str().to_string();
    let token_id = event.token_id.as_str().to_string();
    let local_order_id = event.local_id.as_str().to_string();
    let ts_ms = event.ts_ns / 1_000_000;
    let source = if event.recovery {
        PositionEventSource::Recovery
    } else {
        PositionEventSource::Live
    };

    match &event.payload {
        crate::order_gateway::OrderEventPayload::Accepted { exch_id } => {
            Some(PositionEvent::OrderWorkingRegistered {
                strategy_id,
                token_id,
                local_order_id,
                exchange_order_id: exch_id.as_ref().map(|value| value.as_str().to_string()),
                side,
                price,
                size: original_size,
                seq: event.seq,
                ts_ms,
                source,
                recovery: event.recovery,
            })
        }
        crate::order_gateway::OrderEventPayload::Open { exch_id } => {
            Some(PositionEvent::OrderWorkingRegistered {
                strategy_id,
                token_id,
                local_order_id,
                exchange_order_id: Some(exch_id.as_str().to_string()),
                side,
                price,
                size: original_size,
                seq: event.seq,
                ts_ms,
                source,
                recovery: event.recovery,
            })
        }
        crate::order_gateway::OrderEventPayload::PartialFill {
            fill_qty,
            fill_price,
            cum_qty,
            ..
        }
        | crate::order_gateway::OrderEventPayload::Fill {
            fill_qty,
            fill_price,
            cum_qty,
            ..
        } => Some(PositionEvent::OrderFillApplied {
            strategy_id,
            token_id,
            local_order_id,
            exchange_order_id: None,
            side,
            fill_qty: *fill_qty,
            fill_price: *fill_price,
            cum_qty: Some(*cum_qty),
            seq: event.seq,
            ts_ms,
            source,
            recovery: event.recovery,
        }),
        crate::order_gateway::OrderEventPayload::Cancelled { .. }
        | crate::order_gateway::OrderEventPayload::Expired
        | crate::order_gateway::OrderEventPayload::LocalRejected { .. }
        | crate::order_gateway::OrderEventPayload::RemoteRejected { .. }
        | crate::order_gateway::OrderEventPayload::Failed { .. } => {
            Some(PositionEvent::OrderTerminalApplied {
                strategy_id,
                token_id,
                local_order_id,
                reason: match event.kind {
                    crate::order_gateway::OrderEventKind::Expired => {
                        PositionTerminalReason::Expired
                    }
                    crate::order_gateway::OrderEventKind::LocalRejected => {
                        PositionTerminalReason::LocalRejected
                    }
                    crate::order_gateway::OrderEventKind::RemoteRejected => {
                        PositionTerminalReason::RemoteRejected
                    }
                    crate::order_gateway::OrderEventKind::Failed => PositionTerminalReason::Failed,
                    _ => PositionTerminalReason::Cancelled,
                },
                seq: event.seq,
                ts_ms,
                source,
                recovery: event.recovery,
            })
        }
        crate::order_gateway::OrderEventPayload::Stale { .. } => Some(PositionEvent::OrderStale {
            strategy_id,
            token_id,
            local_order_id,
            seq: event.seq,
            ts_ms,
            source,
            recovery: event.recovery,
        }),
        _ => None,
    }
}

fn position_event_type(event: &PositionEvent) -> &'static str {
    match event {
        PositionEvent::OrderWorkingRegistered { .. } => "OrderWorkingRegistered",
        PositionEvent::OrderFillApplied { .. } => "OrderFillApplied",
        PositionEvent::OrderTerminalApplied { .. } => "OrderTerminalApplied",
        PositionEvent::OrderStale { .. } => "OrderStale",
    }
}

fn position_event_strategy_id(event: &PositionEvent) -> Option<&str> {
    match event {
        PositionEvent::OrderWorkingRegistered { strategy_id, .. }
        | PositionEvent::OrderFillApplied { strategy_id, .. }
        | PositionEvent::OrderTerminalApplied { strategy_id, .. }
        | PositionEvent::OrderStale { strategy_id, .. } => Some(strategy_id.as_str()),
    }
}

fn position_event_token_id(event: &PositionEvent) -> &str {
    match event {
        PositionEvent::OrderWorkingRegistered { token_id, .. }
        | PositionEvent::OrderFillApplied { token_id, .. }
        | PositionEvent::OrderTerminalApplied { token_id, .. }
        | PositionEvent::OrderStale { token_id, .. } => token_id.as_str(),
    }
}

fn position_event_local_order_id(event: &PositionEvent) -> Option<&str> {
    match event {
        PositionEvent::OrderWorkingRegistered { local_order_id, .. }
        | PositionEvent::OrderFillApplied { local_order_id, .. }
        | PositionEvent::OrderTerminalApplied { local_order_id, .. }
        | PositionEvent::OrderStale { local_order_id, .. } => Some(local_order_id.as_str()),
    }
}

fn position_event_exchange_order_id(event: &PositionEvent) -> Option<&str> {
    match event {
        PositionEvent::OrderWorkingRegistered {
            exchange_order_id, ..
        }
        | PositionEvent::OrderFillApplied {
            exchange_order_id, ..
        } => exchange_order_id.as_deref(),
        PositionEvent::OrderTerminalApplied { .. } | PositionEvent::OrderStale { .. } => None,
    }
}

fn position_event_side(event: &PositionEvent) -> Option<PositionSide> {
    match event {
        PositionEvent::OrderWorkingRegistered { side, .. }
        | PositionEvent::OrderFillApplied { side, .. } => Some(*side),
        PositionEvent::OrderTerminalApplied { .. } | PositionEvent::OrderStale { .. } => None,
    }
}

fn position_event_qty(event: &PositionEvent) -> Option<Decimal> {
    match event {
        PositionEvent::OrderWorkingRegistered { size, .. } => Some(*size),
        PositionEvent::OrderFillApplied { fill_qty, .. } => Some(*fill_qty),
        PositionEvent::OrderTerminalApplied { .. } | PositionEvent::OrderStale { .. } => None,
    }
}

fn position_event_price(event: &PositionEvent) -> Option<Decimal> {
    match event {
        PositionEvent::OrderWorkingRegistered { price, .. } => Some(*price),
        PositionEvent::OrderFillApplied { fill_price, .. } => Some(*fill_price),
        PositionEvent::OrderTerminalApplied { .. } | PositionEvent::OrderStale { .. } => None,
    }
}

fn position_event_source(event: &PositionEvent) -> &'static str {
    match event {
        PositionEvent::OrderWorkingRegistered { source, .. }
        | PositionEvent::OrderFillApplied { source, .. }
        | PositionEvent::OrderTerminalApplied { source, .. }
        | PositionEvent::OrderStale { source, .. } => position_source_label(*source),
    }
}

fn position_event_recovery(event: &PositionEvent) -> bool {
    match event {
        PositionEvent::OrderWorkingRegistered { recovery, .. }
        | PositionEvent::OrderFillApplied { recovery, .. }
        | PositionEvent::OrderTerminalApplied { recovery, .. }
        | PositionEvent::OrderStale { recovery, .. } => *recovery,
    }
}

fn position_event_ts_ms(event: &PositionEvent) -> u64 {
    match event {
        PositionEvent::OrderWorkingRegistered { ts_ms, .. }
        | PositionEvent::OrderFillApplied { ts_ms, .. }
        | PositionEvent::OrderTerminalApplied { ts_ms, .. }
        | PositionEvent::OrderStale { ts_ms, .. } => *ts_ms,
    }
}

fn position_side_label(side: PositionSide) -> &'static str {
    match side {
        PositionSide::Buy => "Buy",
        PositionSide::Sell => "Sell",
    }
}

fn position_side_from_label(value: &str) -> anyhow::Result<PositionSide> {
    match value {
        "Buy" | "buy" => Ok(PositionSide::Buy),
        "Sell" | "sell" => Ok(PositionSide::Sell),
        other => Err(anyhow::anyhow!("未知 position side: {other}")),
    }
}

fn position_source_label(source: PositionEventSource) -> &'static str {
    match source {
        PositionEventSource::Live => "Live",
        PositionEventSource::Recovery => "Recovery",
        PositionEventSource::Reconciliation => "Reconciliation",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionIngestError {
    RingFull,
    Closed,
}

#[derive(Clone)]
pub struct PositionIngestHandle {
    tx: mpsc::Sender<PositionEvent>,
    read_handle: PositionReadHandle,
    status: Arc<AtomicU8>,
}

pub struct PositionIngestor {
    rx: mpsc::Receiver<PositionEvent>,
    persist_tx: mpsc::Sender<PositionPersistRecord>,
    keeper: PositionKeeper,
    publisher: PositionSnapshotPublisher,
    status: Arc<AtomicU8>,
}

impl PositionEvent {
    pub fn seq(&self) -> u64 {
        match self {
            PositionEvent::OrderWorkingRegistered { seq, .. }
            | PositionEvent::OrderFillApplied { seq, .. }
            | PositionEvent::OrderTerminalApplied { seq, .. }
            | PositionEvent::OrderStale { seq, .. } => *seq,
        }
    }
}

impl PositionIngestor {
    pub fn new(
        input_capacity: usize,
        persist_capacity: usize,
        keeper: PositionKeeper,
    ) -> (
        Self,
        PositionIngestHandle,
        mpsc::Receiver<PositionPersistRecord>,
    ) {
        let (tx, rx) = mpsc::channel(input_capacity.max(1));
        let (persist_tx, persist_rx) = mpsc::channel(persist_capacity.max(1));
        let publisher = PositionSnapshotPublisher::default();
        let initial_keys = keeper
            .export_snapshot(0, 0)
            .entries
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        publisher.publish_changed(&keeper, &initial_keys);
        let read_handle = publisher.read_handle();
        let status = Arc::new(AtomicU8::new(PositionEngineStatus::Live.as_u8()));
        let handle = PositionIngestHandle {
            tx,
            read_handle,
            status: status.clone(),
        };
        let ingestor = Self {
            rx,
            persist_tx,
            keeper,
            publisher,
            status,
        };
        (ingestor, handle, persist_rx)
    }

    pub fn new_for_test(
        input_capacity: usize,
        persist_capacity: usize,
    ) -> (
        Self,
        PositionIngestHandle,
        mpsc::Receiver<PositionPersistRecord>,
    ) {
        Self::new(input_capacity, persist_capacity, PositionKeeper::default())
    }

    pub async fn run_until_input_closed(mut self) {
        while let Some(event) = self.rx.recv().await {
            self.apply_and_publish(event).await;
        }
        self.status
            .store(PositionEngineStatus::Stopped.as_u8(), Ordering::Release);
    }

    async fn apply_and_publish(&mut self, event: PositionEvent) {
        let changed = self.keeper.apply_event(event.clone());
        self.publisher.publish_changed(&self.keeper, &changed);
        if self
            .persist_tx
            .try_send(PositionPersistRecord::Journal(event))
            .is_err()
        {
            self.mark_degraded();
        }
    }

    fn mark_degraded(&self) {
        self.status
            .store(PositionEngineStatus::Degraded.as_u8(), Ordering::Release);
    }
}

impl PositionIngestHandle {
    pub fn try_ingest(&self, event: PositionEvent) -> Result<(), PositionIngestError> {
        self.tx.try_send(event).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => PositionIngestError::RingFull,
            mpsc::error::TrySendError::Closed(_) => PositionIngestError::Closed,
        })
    }

    pub fn read_handle(&self) -> PositionReadHandle {
        self.read_handle.clone()
    }

    pub fn status(&self) -> PositionEngineStatus {
        PositionEngineStatus::from_u8(self.status.load(Ordering::Acquire))
    }

    pub fn status_handle(&self) -> PositionStatusHandle {
        PositionStatusHandle {
            status: self.status.clone(),
        }
    }

    pub fn mark_degraded(&self) {
        self.status
            .store(PositionEngineStatus::Degraded.as_u8(), Ordering::Release);
    }
}

impl PositionStatusHandle {
    pub fn status(&self) -> PositionEngineStatus {
        PositionEngineStatus::from_u8(self.status.load(Ordering::Acquire))
    }
}

impl PositionSnapshotPublisher {
    pub fn read_handle(&self) -> PositionReadHandle {
        PositionReadHandle {
            entries: self.entries.clone(),
        }
    }

    pub fn publish_changed(&self, keeper: &PositionKeeper, keys: &[PositionEntryKey]) {
        let mut registry = self
            .entries
            .write()
            .expect("position publish registry should not be poisoned");
        for key in keys {
            let snapshot = match key {
                PositionEntryKey::Strategy {
                    strategy_id,
                    token_id,
                } => keeper.entry(strategy_id, token_id),
                PositionEntryKey::Global { token_id } => keeper.global_entry(token_id),
            };
            if let Some(snapshot) = snapshot {
                let cell = registry
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(ArcSwapOption::empty()));
                cell.store(Some(Arc::new(snapshot)));
            }
        }
    }
}

impl PositionReadHandle {
    pub fn get_entry(&self, strategy_id: &str, token_id: &str) -> Option<PositionEntrySnapshot> {
        self.get(&PositionEntryKey::Strategy {
            strategy_id: strategy_id.to_string(),
            token_id: token_id.to_string(),
        })
    }

    pub fn get_global_entry(&self, token_id: &str) -> Option<PositionEntrySnapshot> {
        self.get(&PositionEntryKey::Global {
            token_id: token_id.to_string(),
        })
    }

    pub fn scan_strategy(&self, strategy_id: &str) -> Vec<(String, PositionEntrySnapshot)> {
        let registry = self
            .entries
            .read()
            .expect("position read registry should not be poisoned");
        registry
            .iter()
            .filter_map(|(key, cell)| match key {
                PositionEntryKey::Strategy {
                    strategy_id: entry_strategy,
                    token_id,
                } if entry_strategy == strategy_id => cell
                    .load_full()
                    .map(|snapshot| (token_id.clone(), snapshot.as_ref().clone())),
                _ => None,
            })
            .collect()
    }

    pub fn snapshot_all_weak(&self) -> Vec<(PositionEntryKey, PositionEntrySnapshot)> {
        let registry = self
            .entries
            .read()
            .expect("position read registry should not be poisoned");
        registry
            .iter()
            .filter_map(|(key, cell)| {
                cell.load_full()
                    .map(|snapshot| (key.clone(), snapshot.as_ref().clone()))
            })
            .collect()
    }

    fn get(&self, key: &PositionEntryKey) -> Option<PositionEntrySnapshot> {
        let registry = self
            .entries
            .read()
            .expect("position read registry should not be poisoned");
        registry
            .get(key)
            .and_then(|cell| cell.load_full())
            .map(|snapshot| snapshot.as_ref().clone())
    }
}

impl PositionKeeper {
    pub fn export_snapshot(&self, seq: u64, ts_ms: u64) -> PositionKeeperSnapshot {
        let mut entries = Vec::new();
        for ((strategy_id, token_id), entry) in &self.strategy_entries {
            entries.push((
                PositionEntryKey::Strategy {
                    strategy_id: strategy_id.clone(),
                    token_id: token_id.clone(),
                },
                entry.snapshot(self.degraded),
            ));
        }
        for (token_id, entry) in &self.global_entries {
            entries.push((
                PositionEntryKey::Global {
                    token_id: token_id.clone(),
                },
                entry.snapshot(self.degraded),
            ));
        }
        let open_orders = self
            .open_orders
            .values()
            .map(|order| PositionOpenOrderSnapshot {
                strategy_id: order.strategy_id.clone(),
                token_id: order.token_id.clone(),
                local_order_id: order.local_order_id.clone(),
                exchange_order_id: order.exchange_order_id.clone(),
                side: order.side,
                price: order.price,
                original_size: order.original_size,
                remaining_size: order.remaining_size,
                terminal: order.terminal,
            })
            .collect();
        PositionKeeperSnapshot {
            seq,
            ts_ms,
            entries,
            open_orders,
        }
    }

    pub fn from_snapshot(snapshot: PositionKeeperSnapshot) -> anyhow::Result<Self> {
        let mut keeper = PositionKeeper::default();
        for (key, entry) in snapshot.entries {
            let state = PositionEntryState {
                filled_position: entry.filled_position,
                cost_basis: entry.cost_basis,
                realized_pnl: entry.realized_pnl,
                working_buy_exposure: entry.working_buy_exposure,
                working_sell_exposure: entry.working_sell_exposure,
                last_update_seq: entry.last_update_seq,
                last_update_ts_ms: entry.last_update_ts_ms,
            };
            match key {
                PositionEntryKey::Strategy {
                    strategy_id,
                    token_id,
                } => {
                    keeper
                        .strategy_entries
                        .insert((strategy_id, token_id), state);
                }
                PositionEntryKey::Global { token_id } => {
                    keeper.global_entries.insert(token_id, state);
                }
            }
        }
        for order in snapshot.open_orders {
            keeper.open_orders.insert(
                order.local_order_id.clone(),
                OpenOrderState {
                    strategy_id: order.strategy_id,
                    token_id: order.token_id,
                    local_order_id: order.local_order_id,
                    exchange_order_id: order.exchange_order_id,
                    side: order.side,
                    price: order.price,
                    original_size: order.original_size,
                    remaining_size: order.remaining_size,
                    terminal: order.terminal,
                },
            );
        }
        Ok(keeper)
    }

    pub fn apply_replay_events(&mut self, events: Vec<PositionEvent>) -> anyhow::Result<()> {
        let mut last_seq = 0;
        for event in events {
            let seq = event.seq();
            if last_seq != 0 && seq <= last_seq {
                self.degraded = true;
            }
            last_seq = seq;
            self.apply_event(event);
        }
        Ok(())
    }

    pub fn apply_reconciliation_adjustment(
        &mut self,
        adjustment: PositionReconciliationAdjustment,
    ) -> Vec<PositionEntryKey> {
        self.degraded = true;
        let state = PositionEntryState {
            filled_position: adjustment.exchange_filled_position,
            cost_basis: adjustment.exchange_cost_basis,
            realized_pnl: adjustment.exchange_realized_pnl,
            working_buy_exposure: adjustment.exchange_working_buy_exposure,
            working_sell_exposure: adjustment.exchange_working_sell_exposure,
            last_update_seq: adjustment.seq,
            last_update_ts_ms: adjustment.ts_ms,
        };
        self.strategy_entries.insert(
            (adjustment.strategy_id.clone(), adjustment.token_id.clone()),
            state.clone(),
        );
        self.global_entries
            .insert(adjustment.token_id.clone(), state);
        vec![
            PositionEntryKey::Strategy {
                strategy_id: adjustment.strategy_id,
                token_id: adjustment.token_id.clone(),
            },
            PositionEntryKey::Global {
                token_id: adjustment.token_id,
            },
        ]
    }

    pub fn entry(&self, strategy_id: &str, token_id: &str) -> Option<PositionEntrySnapshot> {
        self.strategy_entries
            .get(&(strategy_id.to_string(), token_id.to_string()))
            .map(|entry| entry.snapshot(self.degraded))
    }

    pub fn global_entry(&self, token_id: &str) -> Option<PositionEntrySnapshot> {
        self.global_entries
            .get(token_id)
            .map(|entry| entry.snapshot(self.degraded))
    }

    pub fn apply_event(&mut self, event: PositionEvent) -> Vec<PositionEntryKey> {
        match event {
            PositionEvent::OrderWorkingRegistered {
                strategy_id,
                token_id,
                local_order_id,
                exchange_order_id,
                side,
                price,
                size,
                seq,
                ts_ms,
                ..
            } => self.register_working(
                strategy_id,
                token_id,
                local_order_id,
                exchange_order_id,
                side,
                price,
                size,
                seq,
                ts_ms,
            ),
            PositionEvent::OrderFillApplied {
                strategy_id,
                token_id,
                local_order_id,
                side,
                fill_qty,
                fill_price,
                seq,
                ts_ms,
                ..
            } => self.apply_fill(
                strategy_id,
                token_id,
                local_order_id,
                side,
                fill_qty,
                fill_price,
                seq,
                ts_ms,
            ),
            PositionEvent::OrderTerminalApplied {
                local_order_id,
                seq,
                ts_ms,
                ..
            } => self.apply_terminal(local_order_id, seq, ts_ms),
            PositionEvent::OrderStale { .. } => Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn register_working(
        &mut self,
        strategy_id: String,
        token_id: String,
        local_order_id: String,
        exchange_order_id: Option<String>,
        side: PositionSide,
        price: Decimal,
        size: Decimal,
        seq: u64,
        ts_ms: u64,
    ) -> Vec<PositionEntryKey> {
        if self.open_orders.contains_key(&local_order_id) {
            return Vec::new();
        }
        let order = OpenOrderState {
            strategy_id: strategy_id.clone(),
            token_id: token_id.clone(),
            local_order_id: local_order_id.clone(),
            exchange_order_id,
            side,
            price,
            original_size: size,
            remaining_size: size,
            terminal: false,
        };
        self.open_orders.insert(local_order_id, order);
        self.adjust_working(&strategy_id, &token_id, side, size, seq, ts_ms)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_fill(
        &mut self,
        strategy_id: String,
        token_id: String,
        local_order_id: String,
        side: PositionSide,
        fill_qty: Decimal,
        fill_price: Decimal,
        seq: u64,
        ts_ms: u64,
    ) -> Vec<PositionEntryKey> {
        if let Some(order) = self.open_orders.get_mut(&local_order_id) {
            order.remaining_size -= fill_qty;
            if order.remaining_size < Decimal::ZERO {
                order.remaining_size = Decimal::ZERO;
                self.degraded = true;
            }
        }
        self.adjust_working(&strategy_id, &token_id, side, -fill_qty, seq, ts_ms);
        self.adjust_filled(
            &strategy_id,
            &token_id,
            side,
            fill_qty,
            fill_price,
            seq,
            ts_ms,
        )
    }

    fn apply_terminal(
        &mut self,
        local_order_id: String,
        seq: u64,
        ts_ms: u64,
    ) -> Vec<PositionEntryKey> {
        let Some(order) = self.open_orders.remove(&local_order_id) else {
            return Vec::new();
        };
        self.adjust_working(
            &order.strategy_id,
            &order.token_id,
            order.side,
            -order.remaining_size,
            seq,
            ts_ms,
        )
    }

    fn adjust_working(
        &mut self,
        strategy_id: &str,
        token_id: &str,
        side: PositionSide,
        delta: Decimal,
        seq: u64,
        ts_ms: u64,
    ) -> Vec<PositionEntryKey> {
        let mut keys = Vec::new();
        let strategy_key = (strategy_id.to_string(), token_id.to_string());
        let strategy_entry = self.strategy_entries.entry(strategy_key).or_default();
        apply_working_delta(strategy_entry, side, delta, seq, ts_ms);
        keys.push(PositionEntryKey::Strategy {
            strategy_id: strategy_id.to_string(),
            token_id: token_id.to_string(),
        });

        let global_entry = self.global_entries.entry(token_id.to_string()).or_default();
        apply_working_delta(global_entry, side, delta, seq, ts_ms);
        keys.push(PositionEntryKey::Global {
            token_id: token_id.to_string(),
        });
        keys
    }

    #[allow(clippy::too_many_arguments)]
    fn adjust_filled(
        &mut self,
        strategy_id: &str,
        token_id: &str,
        side: PositionSide,
        qty: Decimal,
        price: Decimal,
        seq: u64,
        ts_ms: u64,
    ) -> Vec<PositionEntryKey> {
        let mut keys = Vec::new();
        let strategy_key = (strategy_id.to_string(), token_id.to_string());
        let strategy_entry = self.strategy_entries.entry(strategy_key).or_default();
        apply_fill_delta(
            strategy_entry,
            side,
            qty,
            price,
            seq,
            ts_ms,
            &mut self.degraded,
        );
        keys.push(PositionEntryKey::Strategy {
            strategy_id: strategy_id.to_string(),
            token_id: token_id.to_string(),
        });

        let global_entry = self.global_entries.entry(token_id.to_string()).or_default();
        apply_fill_delta(
            global_entry,
            side,
            qty,
            price,
            seq,
            ts_ms,
            &mut self.degraded,
        );
        keys.push(PositionEntryKey::Global {
            token_id: token_id.to_string(),
        });
        keys
    }
}

fn apply_working_delta(
    entry: &mut PositionEntryState,
    side: PositionSide,
    delta: Decimal,
    seq: u64,
    ts_ms: u64,
) {
    match side {
        PositionSide::Buy => entry.working_buy_exposure += delta,
        PositionSide::Sell => entry.working_sell_exposure += delta,
    }
    if entry.working_buy_exposure < Decimal::ZERO {
        entry.working_buy_exposure = Decimal::ZERO;
    }
    if entry.working_sell_exposure < Decimal::ZERO {
        entry.working_sell_exposure = Decimal::ZERO;
    }
    entry.last_update_seq = seq;
    entry.last_update_ts_ms = ts_ms;
}

fn apply_fill_delta(
    entry: &mut PositionEntryState,
    side: PositionSide,
    qty: Decimal,
    price: Decimal,
    seq: u64,
    ts_ms: u64,
    degraded: &mut bool,
) {
    match side {
        PositionSide::Buy => {
            entry.filled_position += qty;
            entry.cost_basis += qty * price;
        }
        PositionSide::Sell => {
            if entry.filled_position == Decimal::ZERO {
                *degraded = true;
                entry.filled_position -= qty;
            } else {
                let avg_cost = entry.cost_basis / entry.filled_position;
                entry.realized_pnl += qty * (price - avg_cost);
                entry.filled_position -= qty;
                entry.cost_basis -= qty * avg_cost;
            }
        }
    }
    entry.last_update_seq = seq;
    entry.last_update_ts_ms = ts_ms;
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_client_sdk_v2::types::Decimal;

    fn dec(value: f64) -> Decimal {
        Decimal::try_from(value).expect("decimal should build")
    }

    #[test]
    fn entry_snapshot_computes_avg_cost_and_theoretical_values() {
        let snapshot = PositionEntrySnapshot {
            filled_position: dec(10.0),
            cost_basis: dec(4.0),
            realized_pnl: dec(1.5),
            working_buy_exposure: dec(3.0),
            working_sell_exposure: dec(2.0),
            last_update_seq: 42,
            last_update_ts_ms: 1000,
            degraded: false,
        };

        assert_eq!(snapshot.avg_cost(), Some(dec(0.4)));
        assert_eq!(snapshot.theoretical_min(), dec(8.0));
        assert_eq!(snapshot.theoretical_max(), dec(13.0));
        assert_eq!(snapshot.theoretical_net(), dec(11.0));
        assert_eq!(snapshot.realized_pnl, dec(1.5));
    }

    #[test]
    fn entry_snapshot_has_no_avg_cost_when_flat() {
        let snapshot = PositionEntrySnapshot {
            filled_position: Decimal::ZERO,
            cost_basis: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            working_buy_exposure: dec(2.0),
            working_sell_exposure: dec(1.0),
            last_update_seq: 7,
            last_update_ts_ms: 2000,
            degraded: false,
        };

        assert_eq!(snapshot.avg_cost(), None);
        assert_eq!(snapshot.theoretical_min(), dec(-1.0));
        assert_eq!(snapshot.theoretical_max(), dec(2.0));
        assert_eq!(snapshot.theoretical_net(), dec(1.0));
    }

    fn buy_working(local_id: &str, qty: f64, price: f64) -> PositionEvent {
        PositionEvent::OrderWorkingRegistered {
            strategy_id: "strategy-a".to_string(),
            token_id: "token-1".to_string(),
            local_order_id: local_id.to_string(),
            exchange_order_id: None,
            side: PositionSide::Buy,
            price: dec(price),
            size: dec(qty),
            seq: 1,
            ts_ms: 100,
            source: PositionEventSource::Live,
            recovery: false,
        }
    }

    fn sell_working(local_id: &str, qty: f64, price: f64) -> PositionEvent {
        PositionEvent::OrderWorkingRegistered {
            strategy_id: "strategy-a".to_string(),
            token_id: "token-1".to_string(),
            local_order_id: local_id.to_string(),
            exchange_order_id: None,
            side: PositionSide::Sell,
            price: dec(price),
            size: dec(qty),
            seq: 1,
            ts_ms: 100,
            source: PositionEventSource::Live,
            recovery: false,
        }
    }

    fn fill(local_id: &str, side: PositionSide, qty: f64, price: f64, seq: u64) -> PositionEvent {
        PositionEvent::OrderFillApplied {
            strategy_id: "strategy-a".to_string(),
            token_id: "token-1".to_string(),
            local_order_id: local_id.to_string(),
            exchange_order_id: None,
            side,
            fill_qty: dec(qty),
            fill_price: dec(price),
            cum_qty: None,
            seq,
            ts_ms: 100 + seq,
            source: PositionEventSource::Live,
            recovery: false,
        }
    }

    fn terminal(local_id: &str, seq: u64) -> PositionEvent {
        PositionEvent::OrderTerminalApplied {
            strategy_id: "strategy-a".to_string(),
            token_id: "token-1".to_string(),
            local_order_id: local_id.to_string(),
            reason: PositionTerminalReason::Cancelled,
            seq,
            ts_ms: 100 + seq,
            source: PositionEventSource::Live,
            recovery: false,
        }
    }

    #[test]
    fn reducer_registers_working_buy_for_strategy_and_global_entries() {
        let mut keeper = PositionKeeper::default();

        let changed = keeper.apply_event(buy_working("buy-1", 10.0, 0.4));

        assert_eq!(changed.len(), 2);
        let strategy = keeper
            .entry("strategy-a", "token-1")
            .expect("strategy entry");
        let global = keeper.global_entry("token-1").expect("global entry");
        assert_eq!(strategy.working_buy_exposure, dec(10.0));
        assert_eq!(strategy.working_sell_exposure, Decimal::ZERO);
        assert_eq!(global.working_buy_exposure, dec(10.0));
        assert_eq!(global.working_sell_exposure, Decimal::ZERO);
    }

    #[test]
    fn reducer_does_not_double_count_duplicate_working_registration() {
        let mut keeper = PositionKeeper::default();

        keeper.apply_event(buy_working("buy-1", 10.0, 0.4));
        keeper.apply_event(buy_working("buy-1", 10.0, 0.4));

        let strategy = keeper
            .entry("strategy-a", "token-1")
            .expect("strategy entry");
        assert_eq!(strategy.working_buy_exposure, dec(10.0));
    }

    #[test]
    fn reducer_buy_fill_reduces_working_and_increases_cost_basis() {
        let mut keeper = PositionKeeper::default();

        keeper.apply_event(buy_working("buy-1", 10.0, 0.4));
        keeper.apply_event(fill("buy-1", PositionSide::Buy, 4.0, 0.4, 2));

        let strategy = keeper
            .entry("strategy-a", "token-1")
            .expect("strategy entry");
        assert_eq!(strategy.filled_position, dec(4.0));
        assert_eq!(strategy.cost_basis, dec(1.6));
        assert_eq!(strategy.working_buy_exposure, dec(6.0));
        assert_eq!(strategy.realized_pnl, Decimal::ZERO);
    }

    #[test]
    fn reducer_sell_fill_realizes_pnl_and_reduces_cost_basis() {
        let mut keeper = PositionKeeper::default();

        keeper.apply_event(buy_working("buy-1", 10.0, 0.4));
        keeper.apply_event(fill("buy-1", PositionSide::Buy, 10.0, 0.4, 2));
        keeper.apply_event(sell_working("sell-1", 3.0, 0.6));
        keeper.apply_event(fill("sell-1", PositionSide::Sell, 3.0, 0.6, 3));

        let strategy = keeper
            .entry("strategy-a", "token-1")
            .expect("strategy entry");
        assert_eq!(strategy.filled_position, dec(7.0));
        assert_eq!(strategy.cost_basis, dec(2.8));
        assert_eq!(strategy.realized_pnl, dec(0.6));
        assert_eq!(strategy.working_sell_exposure, Decimal::ZERO);
    }

    #[test]
    fn reducer_terminal_releases_only_remaining_working_exposure() {
        let mut keeper = PositionKeeper::default();

        keeper.apply_event(sell_working("sell-1", 5.0, 0.6));
        keeper.apply_event(fill("sell-1", PositionSide::Sell, 2.0, 0.6, 2));
        keeper.apply_event(terminal("sell-1", 3));

        let strategy = keeper
            .entry("strategy-a", "token-1")
            .expect("strategy entry");
        assert_eq!(strategy.working_sell_exposure, Decimal::ZERO);
    }

    #[test]
    fn reducer_stale_event_does_not_release_working_exposure() {
        let mut keeper = PositionKeeper::default();

        keeper.apply_event(sell_working("sell-1", 5.0, 0.6));
        keeper.apply_event(PositionEvent::OrderStale {
            strategy_id: "strategy-a".to_string(),
            token_id: "token-1".to_string(),
            local_order_id: "sell-1".to_string(),
            seq: 2,
            ts_ms: 102,
            source: PositionEventSource::Live,
            recovery: false,
        });

        let strategy = keeper
            .entry("strategy-a", "token-1")
            .expect("strategy entry");
        assert_eq!(strategy.working_sell_exposure, dec(5.0));
    }

    #[test]
    fn read_handle_returns_latest_strategy_and_global_entry_snapshots() {
        let publisher = PositionSnapshotPublisher::default();
        let handle = publisher.read_handle();
        let mut keeper = PositionKeeper::default();

        let changed = keeper.apply_event(buy_working("buy-1", 10.0, 0.4));
        publisher.publish_changed(&keeper, &changed);

        let strategy = handle
            .get_entry("strategy-a", "token-1")
            .expect("strategy snapshot");
        let global = handle.get_global_entry("token-1").expect("global snapshot");
        assert_eq!(strategy.working_buy_exposure, dec(10.0));
        assert_eq!(global.working_buy_exposure, dec(10.0));
    }

    #[test]
    fn read_handle_scans_strategy_with_entry_consistent_weak_range_snapshot() {
        let publisher = PositionSnapshotPublisher::default();
        let handle = publisher.read_handle();
        let mut keeper = PositionKeeper::default();

        let first = keeper.apply_event(buy_working("buy-1", 10.0, 0.4));
        publisher.publish_changed(&keeper, &first);
        let second = keeper.apply_event(PositionEvent::OrderWorkingRegistered {
            strategy_id: "strategy-a".to_string(),
            token_id: "token-2".to_string(),
            local_order_id: "buy-2".to_string(),
            exchange_order_id: None,
            side: PositionSide::Buy,
            price: dec(0.3),
            size: dec(5.0),
            seq: 2,
            ts_ms: 102,
            source: PositionEventSource::Live,
            recovery: false,
        });
        publisher.publish_changed(&keeper, &second);

        let mut rows = handle.scan_strategy("strategy-a");
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, "token-1");
        assert_eq!(rows[0].1.working_buy_exposure, dec(10.0));
        assert_eq!(rows[1].0, "token-2");
        assert_eq!(rows[1].1.working_buy_exposure, dec(5.0));
    }

    #[tokio::test]
    async fn ingestor_applies_events_and_publishes_read_snapshot() {
        let (ingestor, handle, mut persist_rx) = PositionIngestor::new_for_test(8, 8);
        let task = tokio::spawn(ingestor.run_until_input_closed());

        handle
            .try_ingest(buy_working("buy-1", 10.0, 0.4))
            .expect("event should enqueue");
        let persisted = persist_rx.recv().await.expect("persist record");
        assert_eq!(persisted.seq(), 1);

        let snapshot = handle
            .read_handle()
            .get_entry("strategy-a", "token-1")
            .expect("snapshot");
        assert_eq!(snapshot.working_buy_exposure, dec(10.0));

        drop(handle);
        task.await.expect("ingestor task should finish");
    }

    #[tokio::test]
    async fn ingestor_marks_degraded_when_persistence_queue_is_full() {
        let (ingestor, handle, _persist_rx) = PositionIngestor::new_for_test(8, 1);
        let task = tokio::spawn(ingestor.run_until_input_closed());

        handle
            .try_ingest(buy_working("buy-1", 10.0, 0.4))
            .expect("first event should enqueue");
        handle
            .try_ingest(PositionEvent::OrderWorkingRegistered {
                strategy_id: "strategy-a".to_string(),
                token_id: "token-2".to_string(),
                local_order_id: "buy-2".to_string(),
                exchange_order_id: None,
                side: PositionSide::Buy,
                price: dec(0.3),
                size: dec(5.0),
                seq: 2,
                ts_ms: 102,
                source: PositionEventSource::Live,
                recovery: false,
            })
            .expect("second event should enqueue");

        tokio::task::yield_now().await;
        assert_eq!(handle.status(), PositionEngineStatus::Degraded);

        drop(handle);
        task.await.expect("ingestor task should finish");
    }

    #[test]
    fn recovery_loads_snapshot_and_replays_journal_after_snapshot_seq() {
        let mut original = PositionKeeper::default();
        original.apply_event(buy_working("buy-1", 10.0, 0.4));
        original.apply_event(fill("buy-1", PositionSide::Buy, 4.0, 0.4, 2));
        let snapshot = original.export_snapshot(2, 200);

        let mut recovered = PositionKeeper::from_snapshot(snapshot).expect("snapshot should load");
        recovered
            .apply_replay_events(vec![fill("buy-1", PositionSide::Buy, 3.0, 0.4, 3)])
            .expect("journal replay should apply");

        let entry = recovered.entry("strategy-a", "token-1").expect("entry");
        assert_eq!(entry.filled_position, dec(7.0));
        assert_eq!(entry.cost_basis, dec(2.8));
        assert_eq!(entry.working_buy_exposure, dec(3.0));
    }

    #[test]
    fn reconciliation_adjustment_replaces_local_state_with_exchange_truth_and_marks_degraded() {
        let mut keeper = PositionKeeper::default();
        keeper.apply_event(buy_working("buy-1", 10.0, 0.4));
        keeper.apply_event(fill("buy-1", PositionSide::Buy, 4.0, 0.4, 2));

        let changed = keeper.apply_reconciliation_adjustment(PositionReconciliationAdjustment {
            reconciliation_id: "recon-1".to_string(),
            exchange_data_as_of_ms: 2000,
            last_local_seq_compared: 2,
            strategy_id: "strategy-a".to_string(),
            token_id: "token-1".to_string(),
            exchange_filled_position: dec(5.0),
            exchange_cost_basis: dec(2.0),
            exchange_realized_pnl: dec(0.1),
            exchange_working_buy_exposure: dec(6.0),
            exchange_working_sell_exposure: Decimal::ZERO,
            reason: "exchange position mismatch".to_string(),
            seq: 3,
            ts_ms: 2001,
        });

        assert_eq!(changed.len(), 2);
        let strategy = keeper
            .entry("strategy-a", "token-1")
            .expect("strategy entry");
        let global = keeper.global_entry("token-1").expect("global entry");
        assert_eq!(strategy.filled_position, dec(5.0));
        assert_eq!(strategy.cost_basis, dec(2.0));
        assert_eq!(strategy.realized_pnl, dec(0.1));
        assert_eq!(global.filled_position, dec(5.0));
        assert!(strategy.degraded);
    }

    #[test]
    fn adapter_maps_gateway_open_fill_and_cancel_events_to_position_events() {
        use crate::order_gateway::{
            CancelReason, ExchangeOrderId, LocalOrderId, MarketId, OrderEventEnvelope,
            OrderEventKind, OrderEventPayload, StrategyId, TokenId,
        };

        let open = OrderEventEnvelope {
            strategy_id: StrategyId::from("strategy-a"),
            local_id: LocalOrderId::from("local-1"),
            token_id: TokenId::from("token-1"),
            market_id: MarketId::from("market-1"),
            seq: 1,
            ts_ns: 1_000_000,
            recovery: false,
            kind: OrderEventKind::Open,
            payload: OrderEventPayload::Open {
                exch_id: ExchangeOrderId::from("exch-1"),
            },
            order: None,
        };
        let fill = OrderEventEnvelope {
            strategy_id: StrategyId::from("strategy-a"),
            local_id: LocalOrderId::from("local-1"),
            token_id: TokenId::from("token-1"),
            market_id: MarketId::from("market-1"),
            seq: 2,
            ts_ns: 2_000_000,
            recovery: false,
            kind: OrderEventKind::PartialFill,
            payload: OrderEventPayload::PartialFill {
                fill_qty: dec(3.0),
                fill_price: dec(0.4),
                cum_qty: dec(3.0),
                avg_fill_price: Some(dec(0.4)),
            },
            order: None,
        };
        let cancel = OrderEventEnvelope {
            strategy_id: StrategyId::from("strategy-a"),
            local_id: LocalOrderId::from("local-1"),
            token_id: TokenId::from("token-1"),
            market_id: MarketId::from("market-1"),
            seq: 3,
            ts_ns: 3_000_000,
            recovery: false,
            kind: OrderEventKind::Cancelled,
            payload: OrderEventPayload::Cancelled {
                reason: CancelReason::Requested,
            },
            order: None,
        };

        let open_event =
            position_event_from_gateway_event(&open, PositionSide::Buy, dec(10.0), dec(0.4))
                .expect("open maps");
        let fill_event =
            position_event_from_gateway_event(&fill, PositionSide::Buy, dec(10.0), dec(0.4))
                .expect("fill maps");
        let cancel_event =
            position_event_from_gateway_event(&cancel, PositionSide::Buy, dec(10.0), dec(0.4))
                .expect("cancel maps");

        assert!(matches!(
            open_event,
            PositionEvent::OrderWorkingRegistered { .. }
        ));
        assert!(matches!(fill_event, PositionEvent::OrderFillApplied { .. }));
        assert!(matches!(
            cancel_event,
            PositionEvent::OrderTerminalApplied { .. }
        ));
    }
}
