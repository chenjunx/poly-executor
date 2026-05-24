use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::auth::{LocalSigner, Signer as _};
use polymarket_client_sdk_v2::clob::types::request::OrdersRequest;
use polymarket_client_sdk_v2::clob::types::{OrderStatusType, OrderType, Side as ClobSide};
use polymarket_client_sdk_v2::types::{Decimal, U256};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::config::AuthConfig;
use crate::storage::{
    OrderGatewayCancelAttemptInsert, OrderGatewayEventInsert, OrderGatewayOrderSnapshot,
    OrderGatewaySubmissionInsert, OrderStore,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StrategyId(pub Arc<str>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LocalOrderId(pub Arc<str>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenId(pub Arc<str>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketId(pub Arc<str>);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExchangeOrderId(pub Arc<str>);

macro_rules! impl_id_from_str {
    ($ty:ident) => {
        impl From<&str> for $ty {
            fn from(value: &str) -> Self {
                Self(Arc::from(value))
            }
        }

        impl From<String> for $ty {
            fn from(value: String) -> Self {
                Self(Arc::from(value))
            }
        }

        impl $ty {
            pub fn as_str(&self) -> &str {
                self.0.as_ref()
            }
        }
    };
}

impl_id_from_str!(StrategyId);
impl_id_from_str!(LocalOrderId);
impl_id_from_str!(TokenId);
impl_id_from_str!(MarketId);
impl_id_from_str!(ExchangeOrderId);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeInForce {
    Gtc,
    Gtd { expires_at_ms: u64 },
    Ioc,
    Fok,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayOrderType {
    Limit { time_in_force: TimeInForce },
    Market,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlaceOrderRequest {
    pub strategy_id: StrategyId,
    pub market_id: Option<MarketId>,
    pub token_id: TokenId,
    pub local_id: LocalOrderId,
    pub side: OrderSide,
    pub order_type: GatewayOrderType,
    pub price: Option<Decimal>,
    pub size: Decimal,
    pub reason: Option<Arc<str>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelScope {
    LocalOrderId {
        local_id: LocalOrderId,
        exch_id: Option<ExchangeOrderId>,
        token_id: Option<TokenId>,
    },
    Token {
        token_id: TokenId,
    },
    Market {
        market_id: MarketId,
    },
    AllForStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelOrderRequest {
    pub strategy_id: StrategyId,
    pub scope: CancelScope,
    pub reason: Option<Arc<str>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderQueryLookup {
    LocalId(LocalOrderId),
    ExchangeId(ExchangeOrderId),
}

#[derive(Debug)]
pub enum OrderQueryRequest {
    ActiveOrders {
        strategy_id: StrategyId,
        reply_tx: oneshot::Sender<Vec<OrderRecord>>,
    },
    Order {
        strategy_id: StrategyId,
        lookup: OrderQueryLookup,
        reply_tx: oneshot::Sender<Option<OrderRecord>>,
    },
}

#[derive(Debug)]
pub enum OrderRequest {
    Place(PlaceOrderRequest),
    Cancel(CancelOrderRequest),
    Query(OrderQueryRequest),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayPhase {
    Recovering,
    Live,
}

impl GatewayPhase {
    fn as_u8(self) -> u8 {
        match self {
            Self::Recovering => 0,
            Self::Live => 1,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Live,
            _ => Self::Recovering,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderRequestError {
    RingFull,
    GatewayRecovering,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderQueryError {
    Request(OrderRequestError),
    ResponseDropped,
}

impl From<OrderRequestError> for OrderQueryError {
    fn from(error: OrderRequestError) -> Self {
        Self::Request(error)
    }
}

#[derive(Clone)]
pub struct OrderGatewayHandle {
    tx: mpsc::Sender<OrderRequest>,
    phase: Arc<AtomicU8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OrderEventKind {
    Accepted,
    Open,
    PartialFill,
    Fill,
    Cancelled,
    Expired,
    LocalRejected,
    RemoteRejected,
    Failed,
    Stale,
    Orphan,
    Recovered,
    RecoveryCompleted,
    GatewayHealth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalOrderState {
    Accepted,
    Rejected,
    SubmitPending,
    Submitted,
    Open,
    PartiallyFilled,
    Filled,
    CancelRequested,
    CancelPending,
    Cancelled,
    CancelRejected,
    Failed,
    UnknownPending,
    UnknownTerminal,
}

impl LocalOrderState {
    fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Rejected | Self::Failed | Self::UnknownTerminal
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Venue {
    Polymarket,
    Simulation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteOrderState {
    pub venue: Venue,
    pub status_code: Option<Arc<str>>,
    pub status_text: Option<Arc<str>>,
    pub reject_code: Option<Arc<str>>,
    pub reject_reason: Option<Arc<str>>,
    pub raw_event_type: Option<Arc<str>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelReason {
    Requested,
    Expired,
    RemoteCancelled,
    RemoteCancelledOrMatched,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalRejectReason {
    DuplicateLocalId,
    RiskRejected { code: Arc<str>, reason: Arc<str> },
    InvalidRequest { reason: Arc<str> },
    GatewayRecovering,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayOperation {
    Place,
    Cancel,
    Query,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureKind {
    SigningFailed { message: Arc<str> },
    Transport { message: Arc<str> },
    Timeout { operation: GatewayOperation },
    StateConflict { message: Arc<str> },
    MissingSignedPayloadAfterRestart,
    PersistenceFailed { message: Arc<str> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayStatus {
    Recovering,
    Live,
    Degraded,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderEventPayload {
    Accepted {
        exch_id: Option<ExchangeOrderId>,
    },
    Open {
        exch_id: ExchangeOrderId,
    },
    PartialFill {
        fill_qty: Decimal,
        fill_price: Decimal,
        cum_qty: Decimal,
        avg_fill_price: Option<Decimal>,
    },
    Fill {
        fill_qty: Decimal,
        fill_price: Decimal,
        cum_qty: Decimal,
        avg_fill_price: Option<Decimal>,
    },
    Cancelled {
        reason: CancelReason,
    },
    Expired,
    LocalRejected {
        reason: LocalRejectReason,
    },
    RemoteRejected {
        code: Option<Arc<str>>,
        message: Arc<str>,
        remote_status: Option<RemoteOrderState>,
    },
    Failed {
        kind: FailureKind,
    },
    Stale {
        age_ms: u64,
    },
    Orphan {
        exch_id: ExchangeOrderId,
    },
    Recovered {
        current_state: LocalOrderState,
    },
    RecoveryCompleted {
        recovered_order_count: usize,
        unresolved_order_count: usize,
        failed_unrecoverable_count: usize,
    },
    GatewayHealth {
        status: GatewayStatus,
        ws_lag_ms: u64,
        rest_rtt_ms: u64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderEventOrderMeta {
    pub side: OrderSide,
    pub order_type: GatewayOrderType,
    pub price: Option<Decimal>,
    pub original_size: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderEventEnvelope {
    pub strategy_id: StrategyId,
    pub local_id: LocalOrderId,
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub seq: u64,
    pub ts_ns: u64,
    pub recovery: bool,
    pub kind: OrderEventKind,
    pub payload: OrderEventPayload,
    pub order: Option<OrderEventOrderMeta>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderRecord {
    pub strategy_id: StrategyId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub local_id: LocalOrderId,
    pub exch_id: Option<ExchangeOrderId>,
    pub side: OrderSide,
    pub order_type: GatewayOrderType,
    pub price: Option<Decimal>,
    pub original_size: Decimal,
    pub local_state: LocalOrderState,
    pub filled_size_total: Decimal,
    pub remaining_size: Decimal,
    pub avg_fill_price: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrivateWsOrderUpdate {
    pub exch_id: ExchangeOrderId,
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub fill_price: Decimal,
    pub previous_size_matched: Option<Decimal>,
    pub current_size_matched: Option<Decimal>,
    pub original_size: Option<Decimal>,
    pub remote_status_code: Option<Arc<str>>,
    pub ts_ns: u64,
    pub recovery: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SettlementKey {
    pub transaction_hash: Arc<str>,
    pub exch_id: ExchangeOrderId,
}

pub trait SettlementActivityReader: Send + Sync {
    fn confirmed_trade_transactions<'a>(
        &'a self,
        pending: &'a [SettlementKey],
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = anyhow::Result<std::collections::HashSet<SettlementKey>>,
                > + Send
                + 'a,
        >,
    >;
}

pub struct DataApiSettlementActivityReader {
    client: polymarket_client_sdk_v2::data::Client,
    user: polymarket_client_sdk_v2::types::Address,
}

impl DataApiSettlementActivityReader {
    pub fn new(user: polymarket_client_sdk_v2::types::Address) -> Self {
        Self {
            client: polymarket_client_sdk_v2::data::Client::default(),
            user,
        }
    }

    #[cfg(test)]
    fn from_activities_for_test(
        activities: Vec<polymarket_client_sdk_v2::data::types::response::Activity>,
    ) -> TestDataApiSettlementActivityReader {
        TestDataApiSettlementActivityReader { activities }
    }
}

impl SettlementActivityReader for DataApiSettlementActivityReader {
    fn confirmed_trade_transactions<'a>(
        &'a self,
        pending: &'a [SettlementKey],
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = anyhow::Result<std::collections::HashSet<SettlementKey>>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let target_hashes = pending
                .iter()
                .map(|key| {
                    key.transaction_hash
                        .parse::<polymarket_client_sdk_v2::types::B256>()
                        .map(|hash| (key, hash))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let request =
                polymarket_client_sdk_v2::data::types::request::ActivityRequest::builder()
                    .user(self.user)
                    .activity_types(vec![
                        polymarket_client_sdk_v2::data::types::ActivityType::Trade,
                    ])
                    .limit(500)?
                    .build();
            let activities = self.client.activity(&request).await?;
            Ok(confirmed_settlement_keys_from_activities(
                target_hashes,
                activities.iter(),
            ))
        })
    }
}

#[cfg(test)]
struct TestDataApiSettlementActivityReader {
    activities: Vec<polymarket_client_sdk_v2::data::types::response::Activity>,
}

#[cfg(test)]
impl SettlementActivityReader for TestDataApiSettlementActivityReader {
    fn confirmed_trade_transactions<'a>(
        &'a self,
        pending: &'a [SettlementKey],
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = anyhow::Result<std::collections::HashSet<SettlementKey>>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let target_hashes = pending
                .iter()
                .map(|key| {
                    key.transaction_hash
                        .parse::<polymarket_client_sdk_v2::types::B256>()
                        .map(|hash| (key, hash))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(confirmed_settlement_keys_from_activities(
                target_hashes,
                self.activities.iter(),
            ))
        })
    }
}

fn confirmed_settlement_keys_from_activities<'a, I>(
    target_hashes: Vec<(&'a SettlementKey, polymarket_client_sdk_v2::types::B256)>,
    activities: I,
) -> std::collections::HashSet<SettlementKey>
where
    I: IntoIterator<Item = &'a polymarket_client_sdk_v2::data::types::response::Activity>,
{
    let confirmed_hashes = activities
        .into_iter()
        .filter(|activity| {
            activity.activity_type == polymarket_client_sdk_v2::data::types::ActivityType::Trade
        })
        .map(|activity| activity.transaction_hash)
        .collect::<std::collections::HashSet<_>>();
    target_hashes
        .into_iter()
        .filter_map(|(key, hash)| confirmed_hashes.contains(&hash).then(|| key.clone()))
        .collect()
}

pub struct NoopSettlementActivityReader;

impl SettlementActivityReader for NoopSettlementActivityReader {
    fn confirmed_trade_transactions<'a>(
        &'a self,
        _pending: &'a [SettlementKey],
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = anyhow::Result<std::collections::HashSet<SettlementKey>>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async { Ok(std::collections::HashSet::new()) })
    }
}

pub async fn poll_settlement_activity_once<R>(
    reader: &R,
    pending: Vec<SettlementKey>,
    observation_tx: mpsc::Sender<GatewayObservation>,
) -> anyhow::Result<()>
where
    R: SettlementActivityReader,
{
    let confirmed = reader.confirmed_trade_transactions(&pending).await?;
    for key in pending {
        if confirmed.contains(&key) {
            let _ = observation_tx
                .send(GatewayObservation::SettlementActivityConfirmed {
                    exch_id: key.exch_id,
                    transaction_hash: key.transaction_hash,
                    ts_ns: now_ns(),
                    recovery: false,
                })
                .await;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingSettlementTrade {
    pub fill_qty: Decimal,
    pub fill_price: Decimal,
    pub ts_ns: u64,
    pub recovery: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GatewayObservation {
    PrivateWsOrderUpdate(PrivateWsOrderUpdate),
    SettlementTradeObserved {
        exch_id: ExchangeOrderId,
        transaction_hash: Arc<str>,
        fill_qty: Decimal,
        fill_price: Decimal,
        ts_ns: u64,
        recovery: bool,
    },
    SettlementActivityConfirmed {
        exch_id: ExchangeOrderId,
        transaction_hash: Arc<str>,
        ts_ns: u64,
        recovery: bool,
    },
    RestAccepted {
        local_id: LocalOrderId,
        exch_id: Option<ExchangeOrderId>,
        ts_ns: u64,
        recovery: bool,
    },
    RestCancelAccepted {
        local_id: LocalOrderId,
        reason: CancelReason,
        ts_ns: u64,
        recovery: bool,
    },
    WsOpen {
        exch_id: ExchangeOrderId,
        token_id: TokenId,
        market_id: MarketId,
        remote_status_code: Option<Arc<str>>,
        ts_ns: u64,
        recovery: bool,
    },
    WsPartialFill {
        exch_id: ExchangeOrderId,
        fill_qty: Decimal,
        fill_price: Decimal,
        cum_qty: Decimal,
        avg_fill_price: Option<Decimal>,
        ts_ns: u64,
        recovery: bool,
    },
    Timeout {
        local_id: LocalOrderId,
        operation: GatewayOperation,
        age_ms: u64,
        ts_ns: u64,
        recovery: bool,
    },
}

#[derive(Debug, Default)]
pub struct GatewayState {
    next_seq: u64,
    orders: std::collections::HashMap<LocalOrderId, OrderRecord>,
    local_by_exch: std::collections::HashMap<ExchangeOrderId, LocalOrderId>,
    pending_by_exch: std::collections::HashMap<ExchangeOrderId, Vec<GatewayObservation>>,
    pending_settlements: std::collections::HashMap<SettlementKey, PendingSettlementTrade>,
    applied_settlements: std::collections::HashSet<SettlementKey>,
    confirmed_unapplied_settlements: std::collections::HashSet<SettlementKey>,
}

impl GatewayState {
    pub fn record_submitted(&mut self, request: PlaceOrderRequest) {
        let market_id = request
            .market_id
            .clone()
            .unwrap_or_else(|| MarketId::from(request.token_id.as_str()));
        let record = OrderRecord {
            strategy_id: request.strategy_id,
            market_id,
            token_id: request.token_id,
            local_id: request.local_id.clone(),
            exch_id: None,
            side: request.side,
            order_type: request.order_type,
            price: request.price,
            original_size: request.size,
            local_state: LocalOrderState::SubmitPending,
            filled_size_total: Decimal::try_from(0_f64).expect("zero decimal"),
            remaining_size: request.size,
            avg_fill_price: None,
        };
        self.orders.insert(request.local_id, record);
    }

    pub fn order(&self, local_id: &LocalOrderId) -> Option<&OrderRecord> {
        self.orders.get(local_id)
    }

    fn query_order(
        &self,
        strategy_id: &StrategyId,
        lookup: &OrderQueryLookup,
    ) -> Option<OrderRecord> {
        let record = match lookup {
            OrderQueryLookup::LocalId(local_id) => self.orders.get(local_id),
            OrderQueryLookup::ExchangeId(exch_id) => self
                .local_by_exch
                .get(exch_id)
                .and_then(|local_id| self.orders.get(local_id)),
        }?;
        (record.strategy_id == *strategy_id).then(|| record.clone())
    }

    fn active_orders_for_strategy(&self, strategy_id: &StrategyId) -> Vec<OrderRecord> {
        self.orders
            .values()
            .filter(|record| {
                record.strategy_id == *strategy_id && !record.local_state.is_terminal()
            })
            .cloned()
            .collect()
    }

    fn cancel_targets(&self, request: &CancelOrderRequest) -> Vec<OrderCancelTarget> {
        self.orders
            .values()
            .filter(|record| {
                record.strategy_id == request.strategy_id && !record.local_state.is_terminal()
            })
            .filter(|record| match &request.scope {
                CancelScope::LocalOrderId { local_id, .. } => record.local_id == *local_id,
                CancelScope::Token { token_id } => record.token_id == *token_id,
                CancelScope::Market { market_id } => record.market_id == *market_id,
                CancelScope::AllForStrategy => true,
            })
            .filter_map(|record| {
                record.exch_id.clone().map(|exch_id| OrderCancelTarget {
                    local_id: record.local_id.clone(),
                    exch_id,
                })
            })
            .collect()
    }

    pub fn pending_settlement_keys(&self) -> Vec<SettlementKey> {
        self.pending_settlements.keys().cloned().collect()
    }

    pub fn apply_observation(
        &mut self,
        observation: GatewayObservation,
    ) -> Vec<OrderEventEnvelope> {
        match observation {
            GatewayObservation::RestAccepted {
                local_id,
                exch_id,
                ts_ns,
                recovery,
            } => self.apply_rest_accepted(local_id, exch_id, ts_ns, recovery),
            GatewayObservation::RestCancelAccepted {
                local_id,
                reason,
                ts_ns,
                recovery,
            } => self.apply_rest_cancel_accepted(local_id, reason, ts_ns, recovery),
            GatewayObservation::PrivateWsOrderUpdate(update) => {
                self.apply_private_ws_order_update(update)
            }
            GatewayObservation::SettlementTradeObserved {
                exch_id,
                transaction_hash,
                fill_qty,
                fill_price,
                ts_ns,
                recovery,
            } => self.apply_settlement_trade_observed(
                exch_id,
                transaction_hash,
                fill_qty,
                fill_price,
                ts_ns,
                recovery,
            ),
            GatewayObservation::SettlementActivityConfirmed {
                exch_id,
                transaction_hash,
                ts_ns,
                recovery,
            } => {
                self.apply_settlement_activity_confirmed(exch_id, transaction_hash, ts_ns, recovery)
            }
            GatewayObservation::WsOpen { ref exch_id, .. }
            | GatewayObservation::WsPartialFill { ref exch_id, .. }
                if !self.local_by_exch.contains_key(exch_id) =>
            {
                self.pending_by_exch
                    .entry(exch_id.clone())
                    .or_default()
                    .push(observation);
                Vec::new()
            }
            GatewayObservation::WsOpen {
                exch_id,
                ts_ns,
                recovery,
                ..
            } => self.apply_ws_open(exch_id, ts_ns, recovery),
            GatewayObservation::WsPartialFill {
                exch_id,
                fill_qty,
                fill_price,
                cum_qty,
                avg_fill_price,
                ts_ns,
                recovery,
            } => self.apply_ws_partial_fill(
                exch_id,
                fill_qty,
                fill_price,
                cum_qty,
                avg_fill_price,
                ts_ns,
                recovery,
            ),
            GatewayObservation::Timeout {
                local_id,
                operation,
                age_ms,
                ts_ns,
                recovery,
            } => self.apply_timeout(local_id, operation, age_ms, ts_ns, recovery),
        }
    }

    fn apply_rest_accepted(
        &mut self,
        local_id: LocalOrderId,
        exch_id: Option<ExchangeOrderId>,
        ts_ns: u64,
        recovery: bool,
    ) -> Vec<OrderEventEnvelope> {
        let record = {
            let Some(record) = self.orders.get_mut(&local_id) else {
                return Vec::new();
            };
            record.local_state = LocalOrderState::Accepted;
            if let Some(exch_id) = exch_id.clone() {
                record.exch_id = Some(exch_id.clone());
                self.local_by_exch.insert(exch_id.clone(), local_id.clone());
            }
            record.clone()
        };

        let mut events = vec![self.envelope_from_record(
            record,
            ts_ns,
            recovery,
            OrderEventKind::Accepted,
            OrderEventPayload::Accepted {
                exch_id: exch_id.clone(),
            },
        )];
        if let Some(exch_id) = exch_id {
            let pending = self.pending_by_exch.remove(&exch_id).unwrap_or_default();
            for observation in pending {
                events.extend(self.apply_observation(observation));
            }

            let settlement_keys = self
                .confirmed_unapplied_settlements
                .iter()
                .filter(|key| key.exch_id == exch_id)
                .cloned()
                .collect::<Vec<_>>();
            for key in settlement_keys {
                self.confirmed_unapplied_settlements.remove(&key);
                events.extend(self.apply_settlement_activity_confirmed(
                    key.exch_id.clone(),
                    key.transaction_hash.clone(),
                    ts_ns,
                    recovery,
                ));
            }
        }
        events
    }

    fn apply_rest_cancel_accepted(
        &mut self,
        local_id: LocalOrderId,
        reason: CancelReason,
        ts_ns: u64,
        recovery: bool,
    ) -> Vec<OrderEventEnvelope> {
        let record = {
            let Some(record) = self.orders.get_mut(&local_id) else {
                return Vec::new();
            };
            record.local_state = LocalOrderState::Cancelled;
            record.clone()
        };
        vec![self.envelope_from_record(
            record,
            ts_ns,
            recovery,
            OrderEventKind::Cancelled,
            OrderEventPayload::Cancelled { reason },
        )]
    }

    fn apply_ws_open(
        &mut self,
        exch_id: ExchangeOrderId,
        ts_ns: u64,
        recovery: bool,
    ) -> Vec<OrderEventEnvelope> {
        let Some(local_id) = self.local_by_exch.get(&exch_id) else {
            return Vec::new();
        };
        let record = {
            let Some(record) = self.orders.get_mut(local_id) else {
                return Vec::new();
            };
            record.local_state = LocalOrderState::Open;
            record.clone()
        };
        vec![self.envelope_from_record(
            record,
            ts_ns,
            recovery,
            OrderEventKind::Open,
            OrderEventPayload::Open { exch_id },
        )]
    }

    fn apply_ws_partial_fill(
        &mut self,
        exch_id: ExchangeOrderId,
        fill_qty: Decimal,
        fill_price: Decimal,
        cum_qty: Decimal,
        avg_fill_price: Option<Decimal>,
        ts_ns: u64,
        recovery: bool,
    ) -> Vec<OrderEventEnvelope> {
        let Some(local_id) = self.local_by_exch.get(&exch_id) else {
            return Vec::new();
        };
        let record = {
            let Some(record) = self.orders.get_mut(local_id) else {
                return Vec::new();
            };
            record.local_state = LocalOrderState::PartiallyFilled;
            record.filled_size_total = cum_qty;
            record.remaining_size -= fill_qty;
            record.avg_fill_price = avg_fill_price;
            record.clone()
        };
        vec![self.envelope_from_record(
            record,
            ts_ns,
            recovery,
            OrderEventKind::PartialFill,
            OrderEventPayload::PartialFill {
                fill_qty,
                fill_price,
                cum_qty,
                avg_fill_price,
            },
        )]
    }

    pub fn apply_private_ws_order_update(
        &mut self,
        update: PrivateWsOrderUpdate,
    ) -> Vec<OrderEventEnvelope> {
        self.apply_observation(GatewayObservation::WsOpen {
            exch_id: update.exch_id,
            token_id: update.token_id,
            market_id: update.market_id,
            remote_status_code: update.remote_status_code,
            ts_ns: update.ts_ns,
            recovery: update.recovery,
        })
    }

    fn apply_settlement_trade_observed(
        &mut self,
        exch_id: ExchangeOrderId,
        transaction_hash: Arc<str>,
        fill_qty: Decimal,
        fill_price: Decimal,
        ts_ns: u64,
        recovery: bool,
    ) -> Vec<OrderEventEnvelope> {
        let key = SettlementKey {
            transaction_hash,
            exch_id,
        };
        if self.applied_settlements.contains(&key) {
            return Vec::new();
        }
        self.pending_settlements
            .entry(key)
            .or_insert(PendingSettlementTrade {
                fill_qty,
                fill_price,
                ts_ns,
                recovery,
            });
        Vec::new()
    }

    fn apply_settlement_activity_confirmed(
        &mut self,
        exch_id: ExchangeOrderId,
        transaction_hash: Arc<str>,
        ts_ns: u64,
        recovery: bool,
    ) -> Vec<OrderEventEnvelope> {
        let key = SettlementKey {
            transaction_hash,
            exch_id: exch_id.clone(),
        };
        if self.applied_settlements.contains(&key) {
            return Vec::new();
        }
        let Some(trade) = self.pending_settlements.remove(&key) else {
            return Vec::new();
        };
        let Some(local_id) = self.local_by_exch.get(&exch_id).cloned() else {
            self.confirmed_unapplied_settlements.insert(key.clone());
            self.pending_settlements.insert(key, trade);
            return Vec::new();
        };

        let record = {
            let Some(record) = self.orders.get_mut(&local_id) else {
                return Vec::new();
            };
            let fill_qty = if trade.fill_qty > record.remaining_size {
                record.remaining_size
            } else {
                trade.fill_qty
            };
            if fill_qty <= Decimal::ZERO {
                self.applied_settlements.insert(key);
                return Vec::new();
            }
            record.filled_size_total += fill_qty;
            record.remaining_size -= fill_qty;
            record.avg_fill_price = Some(trade.fill_price);
            let is_terminal = record.remaining_size <= Decimal::ZERO;
            if is_terminal {
                record.remaining_size = Decimal::ZERO;
                record.local_state = LocalOrderState::Filled;
            } else {
                record.local_state = LocalOrderState::PartiallyFilled;
            }
            (record.clone(), fill_qty, is_terminal)
        };

        self.applied_settlements.insert(key);
        let (record, fill_qty, is_terminal) = record;
        let payload = if is_terminal {
            OrderEventPayload::Fill {
                fill_qty,
                fill_price: trade.fill_price,
                cum_qty: record.filled_size_total,
                avg_fill_price: Some(trade.fill_price),
            }
        } else {
            OrderEventPayload::PartialFill {
                fill_qty,
                fill_price: trade.fill_price,
                cum_qty: record.filled_size_total,
                avg_fill_price: Some(trade.fill_price),
            }
        };
        vec![self.envelope_from_record(
            record,
            ts_ns.max(trade.ts_ns),
            recovery || trade.recovery,
            if is_terminal {
                OrderEventKind::Fill
            } else {
                OrderEventKind::PartialFill
            },
            payload,
        )]
    }

    #[cfg(test)]
    fn has_pending_settlement_for_test(&self, transaction_hash: &str, exch_id: &str) -> bool {
        self.pending_settlements.contains_key(&SettlementKey {
            transaction_hash: Arc::from(transaction_hash),
            exch_id: ExchangeOrderId::from(exch_id),
        })
    }

    fn apply_timeout(
        &mut self,
        local_id: LocalOrderId,
        _operation: GatewayOperation,
        age_ms: u64,
        ts_ns: u64,
        recovery: bool,
    ) -> Vec<OrderEventEnvelope> {
        let record = {
            let Some(record) = self.orders.get_mut(&local_id) else {
                return Vec::new();
            };
            record.local_state = LocalOrderState::UnknownPending;
            record.clone()
        };
        vec![self.envelope_from_record(
            record,
            ts_ns,
            recovery,
            OrderEventKind::Stale,
            OrderEventPayload::Stale { age_ms },
        )]
    }

    fn envelope_from_record(
        &mut self,
        record: OrderRecord,
        ts_ns: u64,
        recovery: bool,
        kind: OrderEventKind,
        payload: OrderEventPayload,
    ) -> OrderEventEnvelope {
        self.next_seq += 1;
        let order = Some(OrderEventOrderMeta {
            side: record.side,
            order_type: record.order_type,
            price: record.price,
            original_size: record.original_size,
        });
        OrderEventEnvelope {
            strategy_id: record.strategy_id,
            local_id: record.local_id,
            token_id: record.token_id,
            market_id: record.market_id,
            seq: self.next_seq,
            ts_ns,
            recovery,
            kind,
            payload,
            order,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderSubmitResult {
    pub exch_id: Option<ExchangeOrderId>,
    pub remote_status_code: Option<Arc<str>>,
    pub unsigned_payload_json: String,
    pub signed_payload_json: String,
    pub signature: String,
    pub signer_address: String,
    pub nonce_or_salt: Option<String>,
    pub expiration: Option<i64>,
    pub exchange_payload_hash: String,
    pub rest_request_json: String,
    pub rest_response_json: Option<String>,
    pub rest_status_code: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCancelTarget {
    pub local_id: LocalOrderId,
    pub exch_id: ExchangeOrderId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCancelRejected {
    pub exch_id: ExchangeOrderId,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderCancelResult {
    pub local_ids: Vec<LocalOrderId>,
    pub not_canceled: Vec<OrderCancelRejected>,
    pub rest_request_json: String,
    pub rest_response_json: Option<String>,
    pub rest_status_code: Option<i64>,
}

pub trait OrderSubmitter: Send + Sync {
    fn submit<'a>(
        &'a self,
        request: &'a PlaceOrderRequest,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<OrderSubmitResult>> + Send + 'a>>;
}

pub trait OrderCancelSubmitter: Send + Sync {
    fn cancel<'a>(
        &'a self,
        targets: &'a [OrderCancelTarget],
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<OrderCancelResult>> + Send + 'a>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteOpenOrderSnapshot {
    pub exch_id: ExchangeOrderId,
}

pub trait RemoteOrderReader: Send + Sync {
    fn open_orders<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<RemoteOpenOrderSnapshot>>> + Send + 'a>>;
}

pub struct ClobRemoteOrderReader {
    auth: AuthConfig,
}

impl ClobRemoteOrderReader {
    pub fn new(auth: AuthConfig) -> Self {
        Self { auth }
    }
}

impl RemoteOrderReader for ClobRemoteOrderReader {
    fn open_orders<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<RemoteOpenOrderSnapshot>>> + Send + 'a>>
    {
        Box::pin(async move {
            let client = crate::clob_client::build_authenticated_clob_client(&self.auth).await?;
            let request = OrdersRequest::builder().build();
            let mut cursor = None;
            let mut orders = Vec::new();

            loop {
                let page = client.orders(&request, cursor.take()).await?;
                orders.extend(page.data.into_iter().map(|order| RemoteOpenOrderSnapshot {
                    exch_id: ExchangeOrderId::from(order.id),
                }));
                if page.next_cursor == "LTE=" {
                    break;
                }
                cursor = Some(page.next_cursor);
            }

            Ok(orders)
        })
    }
}

pub struct ClobOrderSubmitter {
    auth: AuthConfig,
}

impl ClobOrderSubmitter {
    pub fn new(auth: AuthConfig) -> Self {
        Self { auth }
    }
}

impl OrderSubmitter for ClobOrderSubmitter {
    fn submit<'a>(
        &'a self,
        request: &'a PlaceOrderRequest,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<OrderSubmitResult>> + Send + 'a>> {
        Box::pin(async move {
            let signer =
                LocalSigner::from_str(&self.auth.private_key)?.with_chain_id(Some(POLYGON));
            let client = crate::clob_client::build_authenticated_clob_client(&self.auth).await?;
            let token_id = U256::from_str(request.token_id.as_str())?;
            let price = request
                .price
                .ok_or_else(|| anyhow::anyhow!("limit order price is required"))?;
            let order_type = clob_order_type(&request.order_type);
            let order_type_label = order_type.to_string();
            let signable = client
                .limit_order()
                .token_id(token_id)
                .side(clob_order_side(request.side))
                .price(price)
                .size(request.size)
                .order_type(order_type)
                .build()
                .await?;
            let unsigned_payload_json = serde_json::json!({
                "payload_debug": format!("{:?}", signable.payload),
                "order_type": order_type_label,
            })
            .to_string();
            let signed = client.sign(&signer, signable).await?;
            let signed_payload_json = serde_json::json!({
                "payload_debug": format!("{:?}", signed.payload),
                "signature": signed.signature.to_string(),
                "order_type": signed.order_type.to_string(),
                "owner": signed.owner.to_string(),
                "post_only": signed.post_only,
                "defer_exec": signed.defer_exec,
            })
            .to_string();
            let signature = signed.signature.to_string();
            let signer_address = signer.address().to_string();
            let exchange_payload_hash = format!("{:?}", signed.payload);
            let rest_request_json = signed_payload_json.clone();
            let response = client.post_order(signed).await?;
            let rest_response_json = serde_json::json!({
                "error_msg": response.error_msg,
                "making_amount": response.making_amount.to_string(),
                "taking_amount": response.taking_amount.to_string(),
                "orderID": response.order_id,
                "status": order_status_label(&response.status),
                "success": response.success,
                "transaction_hashes": response.transaction_hashes.iter().map(|value| value.to_string()).collect::<Vec<_>>(),
                "trade_ids": response.trade_ids,
            })
            .to_string();
            if !response.success {
                return Err(anyhow::anyhow!(
                    "order rejected by CLOB: {}",
                    response
                        .error_msg
                        .unwrap_or_else(|| "unknown error".to_string())
                ));
            }
            Ok(OrderSubmitResult {
                exch_id: Some(ExchangeOrderId::from(response.order_id)),
                remote_status_code: Some(Arc::from(order_status_label(&response.status))),
                unsigned_payload_json,
                signed_payload_json,
                signature,
                signer_address,
                nonce_or_salt: None,
                expiration: None,
                exchange_payload_hash,
                rest_request_json,
                rest_response_json: Some(rest_response_json),
                rest_status_code: Some(200),
            })
        })
    }
}

pub struct ClobOrderCancelSubmitter {
    auth: AuthConfig,
}

impl ClobOrderCancelSubmitter {
    pub fn new(auth: AuthConfig) -> Self {
        Self { auth }
    }
}

impl OrderCancelSubmitter for ClobOrderCancelSubmitter {
    fn cancel<'a>(
        &'a self,
        targets: &'a [OrderCancelTarget],
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<OrderCancelResult>> + Send + 'a>> {
        Box::pin(async move {
            let client = crate::clob_client::build_authenticated_clob_client(&self.auth).await?;
            let order_ids = targets
                .iter()
                .map(|target| target.exch_id.as_str())
                .collect::<Vec<_>>();
            let rest_request_json = serde_json::to_string(&order_ids)?;
            let response = client.cancel_orders(&order_ids).await?;
            let canceled = response.canceled;
            let not_canceled = response.not_canceled;
            let rest_response_json = serde_json::json!({
                "canceled": canceled,
                "not_canceled": not_canceled,
            })
            .to_string();
            Ok(OrderCancelResult {
                local_ids: targets
                    .iter()
                    .filter(|target| {
                        canceled
                            .iter()
                            .any(|exch_id| exch_id == target.exch_id.as_str())
                    })
                    .map(|target| target.local_id.clone())
                    .collect(),
                not_canceled: not_canceled
                    .into_iter()
                    .map(|(exch_id, reason)| OrderCancelRejected {
                        exch_id: ExchangeOrderId::from(exch_id),
                        reason,
                    })
                    .collect(),
                rest_request_json,
                rest_response_json: Some(rest_response_json),
                rest_status_code: Some(200),
            })
        })
    }
}

#[derive(Debug, Default)]
pub struct SimulatedOrderSubmitter;

impl OrderSubmitter for SimulatedOrderSubmitter {
    fn submit<'a>(
        &'a self,
        request: &'a PlaceOrderRequest,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<OrderSubmitResult>> + Send + 'a>> {
        Box::pin(async move {
            let exch_id = ExchangeOrderId::from(format!("sim-{}", request.token_id.as_str()));
            Ok(OrderSubmitResult {
                exch_id: Some(exch_id.clone()),
                remote_status_code: Some(Arc::from("open")),
                unsigned_payload_json: "{}".to_string(),
                signed_payload_json: "{}".to_string(),
                signature: "simulated".to_string(),
                signer_address: "simulated".to_string(),
                nonce_or_salt: None,
                expiration: None,
                exchange_payload_hash: format!("sim-{}", request.local_id.as_str()),
                rest_request_json: "{}".to_string(),
                rest_response_json: Some(format!(
                    "{{\"orderID\":\"{}\",\"status\":\"open\"}}",
                    exch_id.as_str()
                )),
                rest_status_code: Some(200),
            })
        })
    }
}

#[derive(Debug, Default)]
pub struct SimulatedOrderCancelSubmitter;

impl OrderCancelSubmitter for SimulatedOrderCancelSubmitter {
    fn cancel<'a>(
        &'a self,
        targets: &'a [OrderCancelTarget],
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<OrderCancelResult>> + Send + 'a>> {
        Box::pin(async move {
            Ok(OrderCancelResult {
                local_ids: targets
                    .iter()
                    .map(|target| target.local_id.clone())
                    .collect(),
                not_canceled: Vec::new(),
                rest_request_json: "{}".to_string(),
                rest_response_json: Some("{\"simulated\":true}".to_string()),
                rest_status_code: Some(200),
            })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderGatewayConfig {
    pub simulation_enabled: bool,
    pub request_ring_capacity: usize,
    pub event_ring_capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskDecision {
    Allow,
    Reject { code: Arc<str>, reason: Arc<str> },
}

pub trait OrderRiskCheck: Send + Sync {
    fn check_place(&self, request: &PlaceOrderRequest, state: &GatewayState) -> RiskDecision;
    fn check_cancel(&self, request: &CancelOrderRequest, state: &GatewayState) -> RiskDecision;
}

pub struct AllowAllRiskCheck;

impl OrderRiskCheck for AllowAllRiskCheck {
    fn check_place(&self, _request: &PlaceOrderRequest, _state: &GatewayState) -> RiskDecision {
        RiskDecision::Allow
    }

    fn check_cancel(&self, _request: &CancelOrderRequest, _state: &GatewayState) -> RiskDecision {
        RiskDecision::Allow
    }
}

pub struct OrderGateway {
    rx: mpsc::Receiver<OrderRequest>,
    observation_rx: mpsc::Receiver<GatewayObservation>,
    event_ring: OrderEventRing,
    handle: OrderGatewayHandle,
    state: GatewayState,
    risk: Arc<dyn OrderRiskCheck>,
    submitter: Arc<dyn OrderSubmitter>,
    cancel_submitter: Arc<dyn OrderCancelSubmitter>,
    remote_order_reader: Option<Arc<dyn RemoteOrderReader>>,
    config: OrderGatewayConfig,
    order_store: Option<OrderStore>,
    pending_settlement_tx: tokio::sync::watch::Sender<Vec<SettlementKey>>,
}

impl OrderGateway {
    pub fn new(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
        order_store: OrderStore,
    ) -> (
        Self,
        OrderGatewayHandle,
        OrderEventRing,
        mpsc::Sender<GatewayObservation>,
    ) {
        Self::new_with_submitters(
            config,
            risk,
            order_store,
            Arc::new(SimulatedOrderSubmitter),
            Arc::new(SimulatedOrderCancelSubmitter),
        )
    }

    pub fn new_with_submitter(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
        order_store: OrderStore,
        submitter: Arc<dyn OrderSubmitter>,
    ) -> (
        Self,
        OrderGatewayHandle,
        OrderEventRing,
        mpsc::Sender<GatewayObservation>,
    ) {
        Self::new_with_submitters(
            config,
            risk,
            order_store,
            submitter,
            Arc::new(SimulatedOrderCancelSubmitter),
        )
    }

    pub fn new_with_submitters(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
        order_store: OrderStore,
        submitter: Arc<dyn OrderSubmitter>,
        cancel_submitter: Arc<dyn OrderCancelSubmitter>,
    ) -> (
        Self,
        OrderGatewayHandle,
        OrderEventRing,
        mpsc::Sender<GatewayObservation>,
    ) {
        Self::new_for_test_inner(
            config,
            risk,
            Some(order_store),
            submitter,
            cancel_submitter,
            None,
        )
    }

    pub fn new_for_test(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
    ) -> (
        Self,
        OrderGatewayHandle,
        OrderEventRing,
        mpsc::Sender<GatewayObservation>,
    ) {
        Self::new_for_test_inner(
            config,
            risk,
            None,
            Arc::new(SimulatedOrderSubmitter),
            Arc::new(SimulatedOrderCancelSubmitter),
            None,
        )
    }

    pub fn new_for_test_with_store(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
        order_store: OrderStore,
    ) -> (
        Self,
        OrderGatewayHandle,
        OrderEventRing,
        mpsc::Sender<GatewayObservation>,
    ) {
        Self::new_for_test_inner(
            config,
            risk,
            Some(order_store),
            Arc::new(SimulatedOrderSubmitter),
            Arc::new(SimulatedOrderCancelSubmitter),
            None,
        )
    }

    pub fn new_for_test_with_store_and_submitter(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
        order_store: OrderStore,
        submitter: Arc<dyn OrderSubmitter>,
    ) -> (
        Self,
        OrderGatewayHandle,
        OrderEventRing,
        mpsc::Sender<GatewayObservation>,
    ) {
        Self::new_for_test_inner(
            config,
            risk,
            Some(order_store),
            submitter,
            Arc::new(SimulatedOrderCancelSubmitter),
            None,
        )
    }

    pub fn new_for_test_with_store_and_submitters(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
        order_store: OrderStore,
        submitter: Arc<dyn OrderSubmitter>,
        cancel_submitter: Arc<dyn OrderCancelSubmitter>,
    ) -> (
        Self,
        OrderGatewayHandle,
        OrderEventRing,
        mpsc::Sender<GatewayObservation>,
    ) {
        Self::new_for_test_inner(
            config,
            risk,
            Some(order_store),
            submitter,
            cancel_submitter,
            None,
        )
    }

    pub fn new_with_submitters_and_remote_reader(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
        order_store: OrderStore,
        submitter: Arc<dyn OrderSubmitter>,
        cancel_submitter: Arc<dyn OrderCancelSubmitter>,
        remote_order_reader: Arc<dyn RemoteOrderReader>,
    ) -> (
        Self,
        OrderGatewayHandle,
        OrderEventRing,
        mpsc::Sender<GatewayObservation>,
    ) {
        Self::new_for_test_inner(
            config,
            risk,
            Some(order_store),
            submitter,
            cancel_submitter,
            Some(remote_order_reader),
        )
    }

    pub fn new_for_test_with_store_and_remote_reader(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
        order_store: OrderStore,
        remote_order_reader: Arc<dyn RemoteOrderReader>,
    ) -> (
        Self,
        OrderGatewayHandle,
        OrderEventRing,
        mpsc::Sender<GatewayObservation>,
    ) {
        Self::new_for_test_inner(
            config,
            risk,
            Some(order_store),
            Arc::new(SimulatedOrderSubmitter),
            Arc::new(SimulatedOrderCancelSubmitter),
            Some(remote_order_reader),
        )
    }

    fn new_for_test_inner(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
        order_store: Option<OrderStore>,
        submitter: Arc<dyn OrderSubmitter>,
        cancel_submitter: Arc<dyn OrderCancelSubmitter>,
        remote_order_reader: Option<Arc<dyn RemoteOrderReader>>,
    ) -> (
        Self,
        OrderGatewayHandle,
        OrderEventRing,
        mpsc::Sender<GatewayObservation>,
    ) {
        let (handle, rx) = OrderGatewayHandle::new_for_test(
            config.request_ring_capacity,
            GatewayPhase::Recovering,
        );
        let (observation_tx, observation_rx) = mpsc::channel(config.request_ring_capacity);
        let (pending_settlement_tx, _pending_settlement_rx) =
            tokio::sync::watch::channel(Vec::new());
        let event_ring = OrderEventRing::new(config.event_ring_capacity);
        let gateway = Self {
            rx,
            observation_rx,
            event_ring: event_ring.clone(),
            handle: handle.clone(),
            state: GatewayState::default(),
            risk,
            submitter,
            cancel_submitter,
            remote_order_reader,
            config,
            order_store,
            pending_settlement_tx,
        };
        (gateway, handle, event_ring, observation_tx)
    }

    pub fn complete_recovery(
        &mut self,
        recovered_order_count: usize,
        unresolved_order_count: usize,
        failed_unrecoverable_count: usize,
    ) -> Result<(), OrderEventPublishError> {
        self.handle.set_phase(GatewayPhase::Live);
        let event = self.system_event(
            OrderEventKind::RecoveryCompleted,
            OrderEventPayload::RecoveryCompleted {
                recovered_order_count,
                unresolved_order_count,
                failed_unrecoverable_count,
            },
        );
        self.event_ring.publish(event)
    }

    pub async fn complete_startup_recovery(&mut self) -> anyhow::Result<()> {
        let (recovered_order_count, failed_unrecoverable_count) =
            self.recover_from_gateway_store_without_completion()?;
        let unresolved_order_count = self.reconcile_remote_open_orders().await?;
        self.complete_recovery(
            recovered_order_count,
            unresolved_order_count,
            failed_unrecoverable_count,
        )
        .map_err(|error| anyhow::anyhow!("publish recovery completion failed: {error:?}"))
    }

    pub fn recover_from_gateway_store(&mut self) -> anyhow::Result<()> {
        let (recovered_order_count, failed_unrecoverable_count) =
            self.recover_from_gateway_store_without_completion()?;
        self.complete_recovery(recovered_order_count, 0, failed_unrecoverable_count)
            .map_err(|error| anyhow::anyhow!("publish recovery completion failed: {error:?}"))
    }

    fn recover_from_gateway_store_without_completion(&mut self) -> anyhow::Result<(usize, usize)> {
        let store = self
            .order_store
            .clone()
            .ok_or_else(|| anyhow::anyhow!("order store is required for gateway recovery"))?;
        let snapshots = store.load_order_gateway_recoverable_orders()?;
        let mut recovered_order_count = 0;
        let mut failed_unrecoverable_count = 0;

        for snapshot in snapshots {
            let missing_signed_payload = snapshot.exch_id.is_none()
                && store
                    .load_latest_order_gateway_submission(&snapshot.local_id)?
                    .is_none();
            let mut record = order_record_from_gateway_snapshot(snapshot);
            if missing_signed_payload {
                record.local_state = LocalOrderState::Failed;
                record.remaining_size = Decimal::try_from(0_f64).expect("zero decimal");
                failed_unrecoverable_count += 1;
                self.state
                    .orders
                    .insert(record.local_id.clone(), record.clone());
                let event = self.state.envelope_from_record(
                    record,
                    0,
                    true,
                    OrderEventKind::Failed,
                    OrderEventPayload::Failed {
                        kind: FailureKind::MissingSignedPayloadAfterRestart,
                    },
                );
                self.publish_and_persist(event);
                continue;
            }

            let exch_id = record.exch_id.clone();
            if let Some(exch_id) = exch_id.clone() {
                self.state
                    .local_by_exch
                    .insert(exch_id, record.local_id.clone());
            }
            let current_state = record.local_state;
            self.state
                .orders
                .insert(record.local_id.clone(), record.clone());
            recovered_order_count += 1;
            let event = self.state.envelope_from_record(
                record,
                0,
                true,
                OrderEventKind::Recovered,
                OrderEventPayload::Recovered { current_state },
            );
            self.publish_and_persist(event);
            if let Some(exch_id) = exch_id {
                let pending = self
                    .state
                    .pending_by_exch
                    .remove(&exch_id)
                    .unwrap_or_default();
                for observation in pending {
                    for event in self.state.apply_observation(observation) {
                        self.publish_and_persist(event);
                    }
                }
            }
        }

        Ok((recovered_order_count, failed_unrecoverable_count))
    }

    async fn reconcile_remote_open_orders(&mut self) -> anyhow::Result<usize> {
        let Some(reader) = self.remote_order_reader.clone() else {
            return Ok(0);
        };
        let remote_open_order_ids = reader
            .open_orders()
            .await?
            .into_iter()
            .map(|order| order.exch_id)
            .collect::<std::collections::HashSet<_>>();
        let local_missing_remote_orders = self
            .state
            .orders
            .values()
            .filter(|record| !record.local_state.is_terminal())
            .filter_map(|record| {
                let exch_id = record.exch_id.as_ref()?;
                (!remote_open_order_ids.contains(exch_id)).then(|| record.local_id.clone())
            })
            .collect::<Vec<_>>();
        for local_id in local_missing_remote_orders {
            for event in self
                .state
                .apply_observation(GatewayObservation::RestCancelAccepted {
                    local_id,
                    reason: CancelReason::RemoteCancelled,
                    ts_ns: self.state.next_seq + 1,
                    recovery: true,
                })
            {
                self.publish_and_persist(event);
            }
        }

        Ok(0)
    }

    pub fn spawn_private_order_ws(
        auth: AuthConfig,
        order_store: OrderStore,
        observation_tx: mpsc::Sender<GatewayObservation>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(crate::order_ws::run(auth, order_store, observation_tx))
    }

    pub fn spawn_settlement_activity_poller<R>(
        reader: R,
        pending_rx: tokio::sync::watch::Receiver<Vec<SettlementKey>>,
        observation_tx: mpsc::Sender<GatewayObservation>,
    ) -> tokio::task::JoinHandle<()>
    where
        R: SettlementActivityReader + 'static,
    {
        tokio::spawn(async move {
            run_settlement_activity_poller(reader, pending_rx, observation_tx).await;
        })
    }

    pub fn subscribe_pending_settlements(
        &self,
    ) -> tokio::sync::watch::Receiver<Vec<SettlementKey>> {
        self.pending_settlement_tx.subscribe()
    }

    pub async fn run_until_request_channel_closed(mut self) {
        loop {
            tokio::select! {
                request = self.rx.recv() => {
                    match request {
                        Some(request) => self.handle_request(request).await,
                        None => break,
                    }
                }
                observation = self.observation_rx.recv() => {
                    if let Some(observation) = observation {
                        self.handle_observation(observation);
                    }
                }
            }
        }
    }

    pub async fn run_one_request_for_test(&mut self) -> bool {
        let Some(request) = self.rx.recv().await else {
            return false;
        };
        self.handle_request(request).await;
        true
    }

    fn handle_observation(&mut self, observation: GatewayObservation) {
        for event in self.state.apply_observation(observation) {
            self.publish_and_persist(event);
        }
        self.pending_settlement_tx
            .send_replace(self.state.pending_settlement_keys());
    }

    async fn handle_request(&mut self, request: OrderRequest) {
        match request {
            OrderRequest::Place(request) => self.handle_place_request(request).await,
            OrderRequest::Cancel(request) => self.handle_cancel_request(request).await,
            OrderRequest::Query(request) => self.handle_query_request(request),
        }
    }

    fn handle_query_request(&self, request: OrderQueryRequest) {
        match request {
            OrderQueryRequest::ActiveOrders {
                strategy_id,
                reply_tx,
            } => {
                let _ = reply_tx.send(self.state.active_orders_for_strategy(&strategy_id));
            }
            OrderQueryRequest::Order {
                strategy_id,
                lookup,
                reply_tx,
            } => {
                let _ = reply_tx.send(self.state.query_order(&strategy_id, &lookup));
            }
        }
    }

    async fn handle_place_request(&mut self, request: PlaceOrderRequest) {
        match self.risk.check_place(&request, &self.state) {
            RiskDecision::Allow => {
                self.state.record_submitted(request.clone());
                match self.submitter.submit(&request).await {
                    Ok(result) => self.handle_submit_result(request, result),
                    Err(error) => self.publish_submit_failed(request, error),
                }
            }
            RiskDecision::Reject { code, reason } => self.publish_local_rejected(
                request.strategy_id,
                request.local_id,
                request.token_id,
                request.market_id,
                LocalRejectReason::RiskRejected { code, reason },
            ),
        }
    }

    fn handle_submit_result(&mut self, request: PlaceOrderRequest, result: OrderSubmitResult) {
        if let Some(store) = &self.order_store {
            if let Err(error) = persist_order_submission(store, &request, &result) {
                log::warn!(target: "order", "order submission 持久化失败 error={} local_id={:?}", error, request.local_id.as_str());
            }
        }

        let exch_id = result.exch_id.clone();
        for event in self
            .state
            .apply_observation(GatewayObservation::RestAccepted {
                local_id: request.local_id.clone(),
                exch_id: exch_id.clone(),
                ts_ns: self.state.next_seq + 1,
                recovery: false,
            })
        {
            self.publish_and_persist(event);
        }

        if self.config.simulation_enabled {
            if let Some(exch_id) = exch_id {
                let token_id = request.token_id;
                let market_id = request.market_id.unwrap_or_else(|| MarketId::from(""));
                for event in self.state.apply_observation(GatewayObservation::WsOpen {
                    exch_id,
                    token_id,
                    market_id,
                    remote_status_code: result.remote_status_code,
                    ts_ns: self.state.next_seq + 1,
                    recovery: false,
                }) {
                    self.publish_and_persist(event);
                }
            }
        }
    }

    fn publish_submit_failed(&mut self, request: PlaceOrderRequest, error: anyhow::Error) {
        let event = self.strategy_event(
            request.strategy_id,
            request.local_id,
            request.token_id,
            request.market_id.unwrap_or_else(|| MarketId::from("")),
            OrderEventKind::Failed,
            OrderEventPayload::Failed {
                kind: FailureKind::Transport {
                    message: Arc::from(error.to_string()),
                },
            },
        );
        self.publish_and_persist(event);
    }

    async fn handle_cancel_request(&mut self, request: CancelOrderRequest) {
        if let RiskDecision::Reject { code, reason } = self.risk.check_cancel(&request, &self.state)
        {
            let local_id = match request.scope {
                CancelScope::LocalOrderId { local_id, .. } => local_id,
                _ => LocalOrderId::from(""),
            };
            self.publish_local_rejected(
                request.strategy_id,
                local_id,
                TokenId::from(""),
                None,
                LocalRejectReason::RiskRejected { code, reason },
            );
            return;
        }

        let targets = self.state.cancel_targets(&request);
        if targets.is_empty() {
            return;
        }

        match self.cancel_submitter.cancel(&targets).await {
            Ok(result) => {
                self.persist_cancel_attempts(&targets, &request.scope, &result, "Cancelled", None);
                for rejected in &result.not_canceled {
                    if remote_order_is_gone_after_cancel(&rejected.reason) {
                        if let Some(target) = targets
                            .iter()
                            .find(|target| target.exch_id == rejected.exch_id)
                        {
                            log::warn!(target: "order", "order cancel 发现远端订单已不存在，收敛本地活跃状态 exch_id={:?} reason={:?}", rejected.exch_id.as_str(), rejected.reason);
                            for event in self.state.apply_observation(
                                GatewayObservation::RestCancelAccepted {
                                    local_id: target.local_id.clone(),
                                    reason: CancelReason::RemoteCancelledOrMatched,
                                    ts_ns: self.state.next_seq + 1,
                                    recovery: false,
                                },
                            ) {
                                self.publish_and_persist(event);
                            }
                            continue;
                        }
                    }
                    log::warn!(target: "order", "order cancel 未被交易所接受，保留本地活跃状态 exch_id={:?} reason={:?}", rejected.exch_id.as_str(), rejected.reason);
                }
                for local_id in result.local_ids {
                    for event in
                        self.state
                            .apply_observation(GatewayObservation::RestCancelAccepted {
                                local_id,
                                reason: CancelReason::Requested,
                                ts_ns: self.state.next_seq + 1,
                                recovery: false,
                            })
                    {
                        self.publish_and_persist(event);
                    }
                }
            }
            Err(error) => {
                self.persist_failed_cancel_attempts(&targets, &request.scope, &error);
                log::warn!(target: "order", "order cancel 提交失败，保留本地活跃状态 error={}", error);
            }
        }
    }

    fn persist_cancel_attempts(
        &self,
        targets: &[OrderCancelTarget],
        scope: &CancelScope,
        result: &OrderCancelResult,
        cancel_state: &str,
        error_code: Option<&str>,
    ) {
        let Some(store) = &self.order_store else {
            return;
        };
        let scope = cancel_scope_label(scope);
        for target in targets {
            let state = if result
                .local_ids
                .iter()
                .any(|local_id| local_id == &target.local_id)
            {
                cancel_state
            } else {
                "Rejected"
            };
            if let Err(error) =
                store.insert_order_gateway_cancel_attempt(&OrderGatewayCancelAttemptInsert {
                    local_id: Some(target.local_id.as_str()),
                    exch_id: Some(target.exch_id.as_str()),
                    scope,
                    rest_request_json: &result.rest_request_json,
                    rest_response_json: result.rest_response_json.as_deref(),
                    rest_status_code: result.rest_status_code,
                    cancel_state: state,
                    error_code,
                })
            {
                log::warn!(target: "order", "order cancel attempt 持久化失败 error={} local_id={:?}", error, target.local_id.as_str());
            }
        }
    }

    fn persist_failed_cancel_attempts(
        &self,
        targets: &[OrderCancelTarget],
        scope: &CancelScope,
        error: &anyhow::Error,
    ) {
        let result = OrderCancelResult {
            local_ids: Vec::new(),
            not_canceled: targets
                .iter()
                .map(|target| OrderCancelRejected {
                    exch_id: target.exch_id.clone(),
                    reason: error.to_string(),
                })
                .collect(),
            rest_request_json: serde_json::json!(
                targets
                    .iter()
                    .map(|target| target.exch_id.as_str())
                    .collect::<Vec<_>>()
            )
            .to_string(),
            rest_response_json: Some(serde_json::json!({ "error": error.to_string() }).to_string()),
            rest_status_code: None,
        };
        self.persist_cancel_attempts(targets, scope, &result, "Failed", Some("cancel_failed"));
    }

    fn publish_local_rejected(
        &mut self,
        strategy_id: StrategyId,
        local_id: LocalOrderId,
        token_id: TokenId,
        market_id: Option<MarketId>,
        reason: LocalRejectReason,
    ) {
        let event = self.strategy_event(
            strategy_id,
            local_id,
            token_id,
            market_id.unwrap_or_else(|| MarketId::from("")),
            OrderEventKind::LocalRejected,
            OrderEventPayload::LocalRejected { reason },
        );
        self.publish_and_persist(event);
    }

    fn publish_and_persist(&mut self, mut event: OrderEventEnvelope) {
        if let Some(store) = &self.order_store {
            match store.allocate_gateway_seq() {
                Ok(seq) => {
                    self.state.next_seq = self.state.next_seq.max(seq);
                    event.seq = seq;
                }
                Err(error) => {
                    log::warn!(target: "order", "gateway seq 分配失败，跳过事件发布 error={}", error);
                    return;
                }
            }
            let _ = persist_gateway_event(store, &self.state, &event);
        }
        let _ = self.event_ring.publish(event);
    }

    fn system_event(
        &mut self,
        kind: OrderEventKind,
        payload: OrderEventPayload,
    ) -> OrderEventEnvelope {
        self.strategy_event(
            StrategyId::from("SYSTEM"),
            LocalOrderId::from(""),
            TokenId::from(""),
            MarketId::from(""),
            kind,
            payload,
        )
    }

    fn strategy_event(
        &mut self,
        strategy_id: StrategyId,
        local_id: LocalOrderId,
        token_id: TokenId,
        market_id: MarketId,
        kind: OrderEventKind,
        payload: OrderEventPayload,
    ) -> OrderEventEnvelope {
        self.state.next_seq += 1;
        OrderEventEnvelope {
            strategy_id,
            local_id,
            token_id,
            market_id,
            seq: self.state.next_seq,
            ts_ns: self.state.next_seq,
            recovery: false,
            kind,
            payload,
            order: None,
        }
    }
}

#[derive(Clone)]
pub struct OrderEventRing {
    tx: broadcast::Sender<OrderEventEnvelope>,
}

impl OrderEventRing {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity.max(1));
        Self { tx }
    }

    pub fn publish(&self, event: OrderEventEnvelope) -> Result<(), OrderEventPublishError> {
        self.tx
            .send(event)
            .map(|_| ())
            .map_err(|_| OrderEventPublishError::Closed)
    }

    pub fn subscribe_for_strategy(&self, strategy_id: StrategyId) -> OrderEventSubscriber {
        OrderEventSubscriber {
            strategy_id: Some(strategy_id),
            rx: self.tx.subscribe(),
        }
    }

    pub fn subscribe_all(&self) -> OrderEventSubscriber {
        OrderEventSubscriber {
            strategy_id: None,
            rx: self.tx.subscribe(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderEventPublishError {
    Closed,
}

pub struct OrderEventSubscriber {
    strategy_id: Option<StrategyId>,
    rx: broadcast::Receiver<OrderEventEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderEventPollError {
    Empty,
    Closed,
    Lagged { skipped: u64 },
}

impl OrderEventSubscriber {
    pub fn try_recv_relevant(&mut self) -> Result<OrderEventEnvelope, OrderEventPollError> {
        loop {
            match self.rx.try_recv() {
                Ok(event) if self.is_relevant(&event) => return Ok(event),
                Ok(_) => continue,
                Err(broadcast::error::TryRecvError::Empty) => {
                    return Err(OrderEventPollError::Empty);
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    return Err(OrderEventPollError::Closed);
                }
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    return Err(OrderEventPollError::Lagged { skipped });
                }
            }
        }
    }

    pub async fn recv_relevant(&mut self) -> Result<OrderEventEnvelope, OrderEventPollError> {
        loop {
            match self.rx.recv().await {
                Ok(event) if self.is_relevant(&event) => return Ok(event),
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(OrderEventPollError::Closed);
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    return Err(OrderEventPollError::Lagged { skipped });
                }
            }
        }
    }

    fn is_relevant(&self, event: &OrderEventEnvelope) -> bool {
        self.strategy_id
            .as_ref()
            .is_none_or(|strategy_id| event.strategy_id == *strategy_id)
    }
}

fn persist_order_submission(
    store: &OrderStore,
    request: &PlaceOrderRequest,
    result: &OrderSubmitResult,
) -> anyhow::Result<()> {
    let price = request.price.map(|value| value.to_string());
    let size = request.size.to_string();
    let order_type = gateway_order_type_label(&request.order_type).to_string();
    store.insert_order_gateway_submission(&OrderGatewaySubmissionInsert {
        local_id: request.local_id.as_str(),
        submit_attempt: 1,
        strategy_id: request.strategy_id.as_str(),
        token_id: request.token_id.as_str(),
        side: order_side_label(request.side),
        order_type: &order_type,
        price: price.as_deref(),
        size: &size,
        exch_id: result.exch_id.as_ref().map(|value| value.as_str()),
        unsigned_payload_json: &result.unsigned_payload_json,
        signed_payload_json: &result.signed_payload_json,
        signature: &result.signature,
        signer_address: &result.signer_address,
        nonce_or_salt: result.nonce_or_salt.as_deref(),
        expiration: result.expiration,
        exchange_payload_hash: &result.exchange_payload_hash,
        rest_request_json: &result.rest_request_json,
        rest_response_json: result.rest_response_json.as_deref(),
        rest_status_code: result.rest_status_code,
        submit_state: "Submitted",
    })
}

pub fn persist_gateway_event(
    store: &OrderStore,
    state: &GatewayState,
    event: &OrderEventEnvelope,
) -> anyhow::Result<()> {
    if let Some(record) = state.order(&event.local_id) {
        store.upsert_order_gateway_order(&OrderGatewayOrderSnapshot {
            strategy_id: record.strategy_id.as_str().to_string(),
            market_id: Some(record.market_id.as_str().to_string())
                .filter(|value| !value.is_empty()),
            token_id: record.token_id.as_str().to_string(),
            local_id: record.local_id.as_str().to_string(),
            exch_id: record
                .exch_id
                .as_ref()
                .map(|value| value.as_str().to_string()),
            side: format!("{:?}", record.side),
            order_type: gateway_order_type_label(&record.order_type).to_string(),
            price: record.price.map(|value| value.to_string()),
            size: record.original_size.to_string(),
            local_state: format!("{:?}", record.local_state),
            remote_status_code: None,
            filled_size_total: record.filled_size_total.to_string(),
            remaining_size: record.remaining_size.to_string(),
            avg_fill_price: record.avg_fill_price.map(|value| value.to_string()),
            last_submission_attempt: None,
            last_event_seq: event.seq,
            terminal_at_ms: terminal_state(record.local_state).then_some(event.ts_ns / 1_000_000),
        })?;
    }

    let fill_delta = event_fill_delta(event);
    let fill_total = event_fill_total(event);
    store.append_order_gateway_event(&OrderGatewayEventInsert {
        seq: event.seq,
        strategy_id: event.strategy_id.as_str(),
        token_id: event.token_id.as_str(),
        market_id: Some(event.market_id.as_str()).filter(|value| !value.is_empty()),
        local_id: Some(event.local_id.as_str()).filter(|value| !value.is_empty()),
        exch_id: event_exchange_id(event).map(|value| value.as_str()),
        event_kind: event_kind_label(event.kind),
        local_state: event_local_state_label(event),
        remote_status_code: None,
        remote_reject_code: None,
        remote_reject_reason: None,
        fill_delta: fill_delta.as_deref(),
        fill_total: fill_total.as_deref(),
        remaining_size: None,
        avg_fill_price: None,
        error_code: None,
        error_message: None,
        raw_json: "{}",
        recovery: event.recovery,
    })
}

fn terminal_state(state: LocalOrderState) -> bool {
    matches!(
        state,
        LocalOrderState::Filled
            | LocalOrderState::Cancelled
            | LocalOrderState::Rejected
            | LocalOrderState::Failed
            | LocalOrderState::UnknownTerminal
    )
}

fn order_side_label(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "Buy",
        OrderSide::Sell => "Sell",
    }
}

fn cancel_scope_label(scope: &CancelScope) -> &'static str {
    match scope {
        CancelScope::LocalOrderId { .. } => "local_order_id",
        CancelScope::Token { .. } => "token",
        CancelScope::Market { .. } => "market",
        CancelScope::AllForStrategy => "all_for_strategy",
    }
}

fn clob_order_side(side: OrderSide) -> ClobSide {
    match side {
        OrderSide::Buy => ClobSide::Buy,
        OrderSide::Sell => ClobSide::Sell,
    }
}

fn clob_order_type(order_type: &GatewayOrderType) -> OrderType {
    match order_type {
        GatewayOrderType::Limit {
            time_in_force: TimeInForce::Gtc,
        } => OrderType::GTC,
        GatewayOrderType::Limit {
            time_in_force: TimeInForce::Gtd { .. },
        } => OrderType::GTD,
        GatewayOrderType::Limit {
            time_in_force: TimeInForce::Ioc,
        } => OrderType::FAK,
        GatewayOrderType::Limit {
            time_in_force: TimeInForce::Fok,
        }
        | GatewayOrderType::Market => OrderType::FOK,
    }
}

fn order_status_label(status: &OrderStatusType) -> String {
    status.to_string()
}

fn order_side_from_label(value: &str) -> Option<OrderSide> {
    match value {
        "Buy" | "buy" => Some(OrderSide::Buy),
        "Sell" | "sell" => Some(OrderSide::Sell),
        _ => None,
    }
}

fn gateway_order_type_from_label(value: &str) -> GatewayOrderType {
    match value {
        "Market" => GatewayOrderType::Market,
        "LimitGtd" => GatewayOrderType::Limit {
            time_in_force: TimeInForce::Gtd { expires_at_ms: 0 },
        },
        "LimitIoc" => GatewayOrderType::Limit {
            time_in_force: TimeInForce::Ioc,
        },
        "LimitFok" => GatewayOrderType::Limit {
            time_in_force: TimeInForce::Fok,
        },
        _ => GatewayOrderType::Limit {
            time_in_force: TimeInForce::Gtc,
        },
    }
}

fn gateway_order_type_label(order_type: &GatewayOrderType) -> &'static str {
    match order_type {
        GatewayOrderType::Limit {
            time_in_force: TimeInForce::Gtc,
        } => "LimitGtc",
        GatewayOrderType::Limit {
            time_in_force: TimeInForce::Gtd { .. },
        } => "LimitGtd",
        GatewayOrderType::Limit {
            time_in_force: TimeInForce::Ioc,
        } => "LimitIoc",
        GatewayOrderType::Limit {
            time_in_force: TimeInForce::Fok,
        } => "LimitFok",
        GatewayOrderType::Market => "Market",
    }
}

fn event_kind_label(kind: OrderEventKind) -> &'static str {
    match kind {
        OrderEventKind::Accepted => "Accepted",
        OrderEventKind::Open => "Open",
        OrderEventKind::PartialFill => "PartialFill",
        OrderEventKind::Fill => "Fill",
        OrderEventKind::Cancelled => "Cancelled",
        OrderEventKind::Expired => "Expired",
        OrderEventKind::LocalRejected => "LocalRejected",
        OrderEventKind::RemoteRejected => "RemoteRejected",
        OrderEventKind::Failed => "Failed",
        OrderEventKind::Stale => "Stale",
        OrderEventKind::Orphan => "Orphan",
        OrderEventKind::Recovered => "Recovered",
        OrderEventKind::RecoveryCompleted => "RecoveryCompleted",
        OrderEventKind::GatewayHealth => "GatewayHealth",
    }
}

fn event_local_state_label(event: &OrderEventEnvelope) -> &'static str {
    match event.kind {
        OrderEventKind::Accepted => "Accepted",
        OrderEventKind::Open => "Open",
        OrderEventKind::PartialFill => "PartiallyFilled",
        OrderEventKind::Fill => "Filled",
        OrderEventKind::Cancelled => "Cancelled",
        OrderEventKind::Expired => "UnknownTerminal",
        OrderEventKind::LocalRejected | OrderEventKind::RemoteRejected => "Rejected",
        OrderEventKind::Failed => "Failed",
        OrderEventKind::Stale => "UnknownPending",
        OrderEventKind::Recovered => "Recovered",
        _ => "UnknownPending",
    }
}

fn event_exchange_id(event: &OrderEventEnvelope) -> Option<&ExchangeOrderId> {
    match &event.payload {
        OrderEventPayload::Accepted { exch_id } => exch_id.as_ref(),
        OrderEventPayload::Open { exch_id } => Some(exch_id),
        OrderEventPayload::Orphan { exch_id } => Some(exch_id),
        _ => None,
    }
}

fn event_fill_delta(event: &OrderEventEnvelope) -> Option<String> {
    match &event.payload {
        OrderEventPayload::PartialFill { fill_qty, .. }
        | OrderEventPayload::Fill { fill_qty, .. } => Some(fill_qty.to_string()),
        _ => None,
    }
}

fn event_fill_total(event: &OrderEventEnvelope) -> Option<String> {
    match &event.payload {
        OrderEventPayload::PartialFill { cum_qty, .. }
        | OrderEventPayload::Fill { cum_qty, .. } => Some(cum_qty.to_string()),
        _ => None,
    }
}

fn order_record_from_gateway_snapshot(snapshot: OrderGatewayOrderSnapshot) -> OrderRecord {
    let local_id = LocalOrderId::from(snapshot.local_id);
    OrderRecord {
        strategy_id: StrategyId::from(snapshot.strategy_id),
        market_id: snapshot
            .market_id
            .map(MarketId::from)
            .unwrap_or_else(|| MarketId::from("")),
        token_id: TokenId::from(snapshot.token_id),
        local_id,
        exch_id: snapshot.exch_id.map(ExchangeOrderId::from),
        side: order_side_from_label(&snapshot.side).unwrap_or(OrderSide::Buy),
        order_type: gateway_order_type_from_label(&snapshot.order_type),
        price: snapshot
            .price
            .as_deref()
            .and_then(|value| Decimal::from_str(value).ok()),
        original_size: Decimal::from_str(&snapshot.size)
            .unwrap_or_else(|_| Decimal::try_from(0_f64).expect("zero decimal")),
        local_state: local_order_state_from_label(&snapshot.local_state),
        filled_size_total: Decimal::from_str(&snapshot.filled_size_total)
            .unwrap_or_else(|_| Decimal::try_from(0_f64).expect("zero decimal")),
        remaining_size: Decimal::from_str(&snapshot.remaining_size)
            .unwrap_or_else(|_| Decimal::try_from(0_f64).expect("zero decimal")),
        avg_fill_price: snapshot
            .avg_fill_price
            .as_deref()
            .and_then(|value| Decimal::from_str(value).ok()),
    }
}

fn settlement_activity_poll_delay(error: Option<&anyhow::Error>) -> Duration {
    match error {
        Some(error)
            if error.to_string().contains("429 Too Many Requests")
                || error.to_string().contains("error code: 1015") =>
        {
            Duration::from_secs(30)
        }
        _ => Duration::from_secs(5),
    }
}

async fn run_settlement_activity_poller<R>(
    reader: R,
    mut pending_rx: tokio::sync::watch::Receiver<Vec<SettlementKey>>,
    observation_tx: mpsc::Sender<GatewayObservation>,
) where
    R: SettlementActivityReader,
{
    loop {
        let pending = pending_rx.borrow().clone();
        if pending.is_empty() {
            if pending_rx.changed().await.is_err() {
                return;
            }
            continue;
        }

        let delay =
            match poll_settlement_activity_once(&reader, pending, observation_tx.clone()).await {
                Ok(()) => settlement_activity_poll_delay(None),
                Err(error) => {
                    log::warn!(target: "order", "settlement activity poll failed error={}", error);
                    settlement_activity_poll_delay(Some(&error))
                }
            };
        tokio::time::sleep(delay).await;
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn remote_order_is_gone_after_cancel(reason: &str) -> bool {
    reason.contains("order can't be found - already canceled or matched")
}

fn local_order_state_from_label(value: &str) -> LocalOrderState {
    match value {
        "Accepted" => LocalOrderState::Accepted,
        "Rejected" => LocalOrderState::Rejected,
        "SubmitPending" => LocalOrderState::SubmitPending,
        "Submitted" => LocalOrderState::Submitted,
        "Open" => LocalOrderState::Open,
        "PartiallyFilled" => LocalOrderState::PartiallyFilled,
        "Filled" => LocalOrderState::Filled,
        "CancelRequested" => LocalOrderState::CancelRequested,
        "CancelPending" => LocalOrderState::CancelPending,
        "Cancelled" => LocalOrderState::Cancelled,
        "CancelRejected" => LocalOrderState::CancelRejected,
        "Failed" => LocalOrderState::Failed,
        "UnknownTerminal" => LocalOrderState::UnknownTerminal,
        _ => LocalOrderState::UnknownPending,
    }
}

pub fn recover_gateway_orders_for_test(
    store: &OrderStore,
    state: &mut GatewayState,
) -> anyhow::Result<Vec<OrderEventEnvelope>> {
    let snapshots = store.load_order_gateway_recoverable_orders()?;
    let mut events = Vec::new();
    for snapshot in snapshots {
        if snapshot.exch_id.is_none()
            && store
                .load_latest_order_gateway_submission(&snapshot.local_id)?
                .is_none()
        {
            let mut record = order_record_from_gateway_snapshot(snapshot);
            record.local_state = LocalOrderState::Failed;
            record.remaining_size = Decimal::try_from(0_f64).expect("zero decimal");
            state.orders.insert(record.local_id.clone(), record.clone());
            events.push(state.envelope_from_record(
                record,
                0,
                true,
                OrderEventKind::Failed,
                OrderEventPayload::Failed {
                    kind: FailureKind::MissingSignedPayloadAfterRestart,
                },
            ));
        }
    }
    Ok(events)
}

impl OrderGatewayHandle {
    pub fn new_for_test(
        capacity: usize,
        phase: GatewayPhase,
    ) -> (Self, mpsc::Receiver<OrderRequest>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            Self {
                tx,
                phase: Arc::new(AtomicU8::new(phase.as_u8())),
            },
            rx,
        )
    }

    pub fn try_send(&self, request: OrderRequest) -> Result<(), OrderRequestError> {
        if GatewayPhase::from_u8(self.phase.load(Ordering::Acquire)) == GatewayPhase::Recovering {
            return Err(OrderRequestError::GatewayRecovering);
        }

        self.tx.try_send(request).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => OrderRequestError::RingFull,
            mpsc::error::TrySendError::Closed(_) => OrderRequestError::Closed,
        })
    }

    pub async fn query_active_orders(
        &self,
        strategy_id: StrategyId,
    ) -> Result<Vec<OrderRecord>, OrderQueryError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.try_send(OrderRequest::Query(OrderQueryRequest::ActiveOrders {
            strategy_id,
            reply_tx,
        }))?;
        reply_rx.await.map_err(|_| OrderQueryError::ResponseDropped)
    }

    pub async fn query_order(
        &self,
        strategy_id: StrategyId,
        lookup: OrderQueryLookup,
    ) -> Result<Option<OrderRecord>, OrderQueryError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.try_send(OrderRequest::Query(OrderQueryRequest::Order {
            strategy_id,
            lookup,
            reply_tx,
        }))?;
        reply_rx.await.map_err(|_| OrderQueryError::ResponseDropped)
    }

    pub fn set_phase(&self, phase: GatewayPhase) {
        self.phase.store(phase.as_u8(), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use polymarket_client_sdk_v2::types::Decimal;

    fn place_request(local_id: &str) -> OrderRequest {
        place_request_for_strategy("liquidity_reward", "token-1", local_id)
    }

    fn place_request_for_strategy(
        strategy_id: &str,
        token_id: &str,
        local_id: &str,
    ) -> OrderRequest {
        OrderRequest::Place(PlaceOrderRequest {
            strategy_id: StrategyId::from(strategy_id),
            market_id: Some(MarketId::from(strategy_id)),
            token_id: TokenId::from(token_id),
            local_id: LocalOrderId::from(local_id),
            side: OrderSide::Buy,
            order_type: GatewayOrderType::Limit {
                time_in_force: TimeInForce::Gtc,
            },
            price: Some(Decimal::try_from(0.42_f64).expect("decimal")),
            size: Decimal::try_from(10_f64).expect("decimal"),
            reason: Some(Arc::from("test")),
        })
    }

    fn place_request_for_market(
        strategy_id: &str,
        market_id: &str,
        token_id: &str,
        local_id: &str,
    ) -> OrderRequest {
        OrderRequest::Place(PlaceOrderRequest {
            strategy_id: StrategyId::from(strategy_id),
            market_id: Some(MarketId::from(market_id)),
            token_id: TokenId::from(token_id),
            local_id: LocalOrderId::from(local_id),
            side: OrderSide::Buy,
            order_type: GatewayOrderType::Limit {
                time_in_force: TimeInForce::Gtc,
            },
            price: Some(Decimal::try_from(0.42_f64).expect("decimal")),
            size: Decimal::try_from(10_f64).expect("decimal"),
            reason: Some(Arc::from("test")),
        })
    }

    #[test]
    fn request_ring_full_returns_error_without_gateway_event() {
        let (handle, mut rx) = OrderGatewayHandle::new_for_test(1, GatewayPhase::Live);

        handle
            .try_send(place_request("order-1"))
            .expect("first send fits");
        let error = handle
            .try_send(place_request("order-2"))
            .expect_err("second send should see full ring");

        assert_eq!(error, OrderRequestError::RingFull);
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn gateway_recovering_rejects_new_requests() {
        let (handle, _rx) = OrderGatewayHandle::new_for_test(8, GatewayPhase::Recovering);

        let error = handle
            .try_send(place_request("order-1"))
            .expect_err("recovering gateway rejects requests");

        assert_eq!(error, OrderRequestError::GatewayRecovering);
    }

    fn accepted_event(strategy: &str, local_id: &str, seq: u64) -> OrderEventEnvelope {
        OrderEventEnvelope {
            strategy_id: StrategyId::from(strategy),
            local_id: LocalOrderId::from(local_id),
            token_id: TokenId::from("token-1"),
            market_id: MarketId::from("market-1"),
            seq,
            ts_ns: seq * 100,
            recovery: false,
            kind: OrderEventKind::Accepted,
            payload: OrderEventPayload::Accepted { exch_id: None },
            order: None,
        }
    }

    #[test]
    fn event_subscriber_filters_by_strategy_before_payload() {
        let ring = OrderEventRing::new(8);
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("strategy-a"));

        ring.publish(accepted_event("strategy-b", "b-1", 1))
            .expect("publish unrelated event");
        ring.publish(accepted_event("strategy-a", "a-1", 2))
            .expect("publish related event");

        let event = subscriber
            .try_recv_relevant()
            .expect("related event should arrive");
        assert_eq!(event.strategy_id, StrategyId::from("strategy-a"));
        assert_eq!(event.local_id, LocalOrderId::from("a-1"));
        assert_eq!(event.seq, 2);
    }

    #[test]
    fn event_subscriber_reports_lag_when_broadcast_overwrites() {
        let ring = OrderEventRing::new(1);
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("strategy-a"));

        ring.publish(accepted_event("strategy-a", "a-1", 1))
            .unwrap();
        ring.publish(accepted_event("strategy-a", "a-2", 2))
            .unwrap();

        let error = subscriber
            .try_recv_relevant()
            .expect_err("subscriber should observe lag");
        assert!(matches!(error, OrderEventPollError::Lagged { skipped } if skipped > 0));
    }

    fn dec(value: f64) -> Decimal {
        Decimal::try_from(value).expect("decimal")
    }

    fn activity_for_test(
        transaction_hash: &str,
        activity_type: polymarket_client_sdk_v2::data::types::ActivityType,
    ) -> polymarket_client_sdk_v2::data::types::response::Activity {
        serde_json::from_str(&format!(
            r#"{{
                "proxyWallet":"0x0000000000000000000000000000000000000000",
                "timestamp":1,
                "conditionId":"0x0000000000000000000000000000000000000000000000000000000000000001",
                "type":"{}",
                "size":"3",
                "usdcSize":"1.26",
                "transactionHash":"{}",
                "price":"0.42",
                "asset":"1",
                "side":"BUY",
                "outcomeIndex":0
            }}"#,
            activity_type, transaction_hash
        ))
        .expect("activity fixture should parse")
    }

    struct FakeSettlementActivityReader {
        confirmed: Vec<(String, String)>,
    }

    impl SettlementActivityReader for FakeSettlementActivityReader {
        fn confirmed_trade_transactions<'a>(
            &'a self,
            pending: &'a [SettlementKey],
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = anyhow::Result<std::collections::HashSet<SettlementKey>>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                Ok(pending
                    .iter()
                    .filter(|key| {
                        self.confirmed.iter().any(|(tx, order)| {
                            tx == key.transaction_hash.as_ref() && order == key.exch_id.as_str()
                        })
                    })
                    .cloned()
                    .collect())
            })
        }
    }

    struct CountingSettlementActivityReader {
        calls: AtomicUsize,
        confirmed: Vec<(String, String)>,
    }

    impl SettlementActivityReader for CountingSettlementActivityReader {
        fn confirmed_trade_transactions<'a>(
            &'a self,
            pending: &'a [SettlementKey],
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = anyhow::Result<std::collections::HashSet<SettlementKey>>,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(pending
                    .iter()
                    .filter(|key| {
                        self.confirmed.iter().any(|(tx, order)| {
                            tx == key.transaction_hash.as_ref() && order == key.exch_id.as_str()
                        })
                    })
                    .cloned()
                    .collect())
            })
        }
    }

    struct FakeRemoteOrderReader {
        open_orders: Vec<RemoteOpenOrderSnapshot>,
    }

    impl RemoteOrderReader for FakeRemoteOrderReader {
        fn open_orders<'a>(
            &'a self,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = anyhow::Result<Vec<RemoteOpenOrderSnapshot>>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move { Ok(self.open_orders.clone()) })
        }
    }

    #[tokio::test]
    async fn data_api_activity_reader_confirms_trade_by_transaction_hash() {
        let transaction_hash = "0x0000000000000000000000000000000000000000000000000000000000000abc";
        let reader =
            DataApiSettlementActivityReader::from_activities_for_test(vec![activity_for_test(
                transaction_hash,
                polymarket_client_sdk_v2::data::types::ActivityType::Trade,
            )]);

        let confirmed = reader
            .confirmed_trade_transactions(&[SettlementKey {
                transaction_hash: Arc::from(transaction_hash),
                exch_id: ExchangeOrderId::from("exch-1"),
            }])
            .await
            .expect("reader should check fixture");

        assert!(confirmed.contains(&SettlementKey {
            transaction_hash: Arc::from(transaction_hash),
            exch_id: ExchangeOrderId::from("exch-1"),
        }));
    }

    #[tokio::test]
    async fn settlement_activity_poller_sends_confirmation_for_trade_activity() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let reader = FakeSettlementActivityReader {
            confirmed: vec![("0xabc".to_string(), "exch-1".to_string())],
        };
        let pending = vec![SettlementKey {
            transaction_hash: Arc::from("0xabc"),
            exch_id: ExchangeOrderId::from("exch-1"),
        }];

        poll_settlement_activity_once(&reader, pending, tx)
            .await
            .expect("poll should succeed");

        let observation = rx.recv().await.expect("confirmation should be sent");
        assert!(matches!(
            observation,
            GatewayObservation::SettlementActivityConfirmed { ref transaction_hash, ref exch_id, .. }
                if transaction_hash.as_ref() == "0xabc" && exch_id.as_str() == "exch-1"
        ));
    }

    #[tokio::test]
    async fn settlement_activity_poll_batches_pending_transactions_into_one_reader_call() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(8);
        let reader = CountingSettlementActivityReader {
            calls: AtomicUsize::new(0),
            confirmed: vec![
                ("0xabc".to_string(), "exch-1".to_string()),
                ("0xdef".to_string(), "exch-2".to_string()),
            ],
        };
        let pending = vec![
            SettlementKey {
                transaction_hash: Arc::from("0xabc"),
                exch_id: ExchangeOrderId::from("exch-1"),
            },
            SettlementKey {
                transaction_hash: Arc::from("0xdef"),
                exch_id: ExchangeOrderId::from("exch-2"),
            },
            SettlementKey {
                transaction_hash: Arc::from("0x999"),
                exch_id: ExchangeOrderId::from("exch-3"),
            },
        ];

        poll_settlement_activity_once(&reader, pending, tx)
            .await
            .expect("poll should succeed");

        let mut confirmed = Vec::new();
        while let Ok(GatewayObservation::SettlementActivityConfirmed {
            transaction_hash,
            exch_id,
            ..
        }) = rx.try_recv()
        {
            confirmed.push((transaction_hash.to_string(), exch_id.as_str().to_string()));
        }
        confirmed.sort();
        assert_eq!(reader.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            confirmed,
            vec![
                ("0xabc".to_string(), "exch-1".to_string()),
                ("0xdef".to_string(), "exch-2".to_string()),
            ]
        );
    }

    #[test]
    fn settlement_activity_rate_limit_error_uses_backoff_delay() {
        let error = anyhow::anyhow!(
            "Status: error(429 Too Many Requests) making GET call to /activity with error code: 1015"
        );

        assert_eq!(
            settlement_activity_poll_delay(Some(&error)),
            Duration::from_secs(30)
        );
        assert_eq!(settlement_activity_poll_delay(None), Duration::from_secs(5));
    }

    #[tokio::test]
    async fn gateway_publishes_pending_settlement_snapshot_after_observation() {
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, _handle, _ring, _observation_tx) =
            OrderGateway::new_for_test(config, Arc::new(AllowAllRiskCheck));
        let mut pending_rx = gateway.subscribe_pending_settlements();

        gateway.handle_observation(GatewayObservation::SettlementTradeObserved {
            exch_id: ExchangeOrderId::from("exch-1"),
            transaction_hash: Arc::from("0xabc"),
            fill_qty: dec(3.0),
            fill_price: dec(0.42),
            ts_ns: 1,
            recovery: false,
        });

        pending_rx.changed().await.expect("pending snapshot update");
        assert_eq!(pending_rx.borrow().len(), 1);
        assert!(pending_rx.borrow().iter().any(|key| {
            key.transaction_hash.as_ref() == "0xabc" && key.exch_id.as_str() == "exch-1"
        }));
    }

    #[test]
    fn reducer_binds_ws_observation_that_arrives_before_rest_acceptance() {
        let mut state = GatewayState::default();
        let request = match place_request("local-1") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        let exch_id = ExchangeOrderId::from("exch-1");

        state.record_submitted(request);
        let ws_events = state.apply_observation(GatewayObservation::WsOpen {
            exch_id: exch_id.clone(),
            token_id: TokenId::from("token-1"),
            market_id: MarketId::from("market-1"),
            remote_status_code: Some(Arc::from("open")),
            ts_ns: 1,
            recovery: false,
        });
        assert!(ws_events.is_empty());

        let rest_events = state.apply_observation(GatewayObservation::RestAccepted {
            local_id: LocalOrderId::from("local-1"),
            exch_id: Some(exch_id.clone()),
            ts_ns: 2,
            recovery: false,
        });

        assert_eq!(rest_events.len(), 2);
        assert_eq!(rest_events[0].kind, OrderEventKind::Accepted);
        assert_eq!(rest_events[1].kind, OrderEventKind::Open);
        assert_eq!(
            state.order(&LocalOrderId::from("local-1")).unwrap().exch_id,
            Some(exch_id)
        );
    }

    #[test]
    fn reducer_timeout_marks_order_stale_not_rejected() {
        let mut state = GatewayState::default();
        let request = match place_request("local-timeout") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);

        let events = state.apply_observation(GatewayObservation::Timeout {
            local_id: LocalOrderId::from("local-timeout"),
            operation: GatewayOperation::Place,
            age_ms: 5_000,
            ts_ns: 9,
            recovery: false,
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, OrderEventKind::Stale);
        assert_eq!(
            state
                .order(&LocalOrderId::from("local-timeout"))
                .unwrap()
                .local_state,
            LocalOrderState::UnknownPending
        );
    }

    #[test]
    fn reducer_cancel_after_partial_fill_preserves_cumulative_fill() {
        let mut state = GatewayState::default();
        let request = match place_request("local-cancel") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);
        state.apply_observation(GatewayObservation::RestAccepted {
            local_id: LocalOrderId::from("local-cancel"),
            exch_id: Some(ExchangeOrderId::from("exch-cancel")),
            ts_ns: 1,
            recovery: false,
        });
        state.apply_observation(GatewayObservation::WsPartialFill {
            exch_id: ExchangeOrderId::from("exch-cancel"),
            fill_qty: dec(4.0),
            fill_price: dec(0.42),
            cum_qty: dec(4.0),
            avg_fill_price: Some(dec(0.42)),
            ts_ns: 2,
            recovery: false,
        });

        let cancel_events = state.apply_observation(GatewayObservation::RestCancelAccepted {
            local_id: LocalOrderId::from("local-cancel"),
            reason: CancelReason::Requested,
            ts_ns: 3,
            recovery: false,
        });

        assert_eq!(cancel_events.len(), 1);
        assert_eq!(cancel_events[0].kind, OrderEventKind::Cancelled);
        let record = state.order(&LocalOrderId::from("local-cancel")).unwrap();
        assert_eq!(record.local_state, LocalOrderState::Cancelled);
        assert_eq!(record.filled_size_total, dec(4.0));
        assert_eq!(record.remaining_size, dec(6.0));
    }

    #[derive(Default)]
    struct CountingCancelSubmitter {
        calls: AtomicUsize,
        target_count: AtomicUsize,
    }

    impl OrderCancelSubmitter for CountingCancelSubmitter {
        fn cancel<'a>(
            &'a self,
            targets: &'a [OrderCancelTarget],
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<OrderCancelResult>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                self.target_count.fetch_add(targets.len(), Ordering::SeqCst);
                Ok(OrderCancelResult {
                    local_ids: targets
                        .iter()
                        .map(|target| target.local_id.clone())
                        .collect(),
                    not_canceled: Vec::new(),
                    rest_request_json: "{}".to_string(),
                    rest_response_json: Some("{\"cancelled\":true}".to_string()),
                    rest_status_code: Some(200),
                })
            })
        }
    }

    struct RejectAllRiskCheck;

    impl OrderRiskCheck for RejectAllRiskCheck {
        fn check_place(&self, _request: &PlaceOrderRequest, _state: &GatewayState) -> RiskDecision {
            RiskDecision::Reject {
                code: Arc::from("risk_rejected"),
                reason: Arc::from("test rejection"),
            }
        }

        fn check_cancel(
            &self,
            _request: &CancelOrderRequest,
            _state: &GatewayState,
        ) -> RiskDecision {
            RiskDecision::Allow
        }
    }

    #[tokio::test]
    async fn gateway_risk_rejection_publishes_local_rejected_event() {
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test(config, Arc::new(RejectAllRiskCheck));
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("liquidity_reward"));

        handle.set_phase(GatewayPhase::Live);
        handle
            .try_send(place_request("risk-1"))
            .expect("send should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        let event = subscriber
            .try_recv_relevant()
            .expect("rejection event should publish");
        assert_eq!(event.kind, OrderEventKind::LocalRejected);
        assert!(matches!(
            event.payload,
            OrderEventPayload::LocalRejected { .. }
        ));
    }

    #[tokio::test]
    async fn gateway_recovery_completed_switches_handle_to_live() {
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test(config, Arc::new(AllowAllRiskCheck));
        let mut system_subscriber = ring.subscribe_for_strategy(StrategyId::from("SYSTEM"));

        gateway
            .complete_recovery(2, 1, 0)
            .expect("recovery completion should publish");

        let event = system_subscriber
            .try_recv_relevant()
            .expect("system recovery event");
        assert_eq!(event.kind, OrderEventKind::RecoveryCompleted);
        assert!(!event.recovery);
        handle
            .try_send(place_request("live-after-recovery"))
            .expect("live send should work");
    }

    #[test]
    fn persisting_reducer_event_writes_gateway_snapshot_and_event_log() {
        let store = crate::storage::OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let mut state = GatewayState::default();
        let request = match place_request("persist-1") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);
        let accepted = state
            .apply_observation(GatewayObservation::RestAccepted {
                local_id: LocalOrderId::from("persist-1"),
                exch_id: Some(ExchangeOrderId::from("exch-persist-1")),
                ts_ns: 1_000_000,
                recovery: false,
            })
            .into_iter()
            .next()
            .expect("accepted event");

        persist_gateway_event(&store, &state, &accepted).expect("event should persist");

        let active = store
            .load_order_gateway_recoverable_orders()
            .expect("recoverable orders should load");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].local_id, "persist-1");
        assert_eq!(active[0].local_state, "Accepted");
        assert_eq!(active[0].last_event_seq, accepted.seq);
    }

    #[derive(Clone)]
    struct FakeOrderSubmitter {
        result: OrderSubmitResult,
    }

    impl OrderSubmitter for FakeOrderSubmitter {
        fn submit<'a>(
            &'a self,
            _request: &'a PlaceOrderRequest,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = anyhow::Result<OrderSubmitResult>> + Send + 'a>,
        > {
            let result = self.result.clone();
            Box::pin(async move { Ok(result) })
        }
    }

    fn submit_result(exch_id: &str, remote_status_code: &str) -> OrderSubmitResult {
        OrderSubmitResult {
            exch_id: Some(ExchangeOrderId::from(exch_id)),
            remote_status_code: Some(Arc::from(remote_status_code)),
            unsigned_payload_json: "{\"unsigned\":true}".to_string(),
            signed_payload_json: "{\"signed\":true}".to_string(),
            signature: "signature-1".to_string(),
            signer_address: "0x0000000000000000000000000000000000000000".to_string(),
            nonce_or_salt: Some("salt-1".to_string()),
            expiration: None,
            exchange_payload_hash: "hash-1".to_string(),
            rest_request_json: "{\"request\":true}".to_string(),
            rest_response_json: Some(format!(
                "{{\"orderID\":\"{exch_id}\",\"status\":\"{remote_status_code}\"}}"
            )),
            rest_status_code: Some(200),
        }
    }

    #[tokio::test]
    async fn live_gateway_uses_submitter_without_simulated_order_id() {
        let store = crate::storage::OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let config = OrderGatewayConfig {
            simulation_enabled: false,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test_with_store_and_submitter(
                config,
                Arc::new(AllowAllRiskCheck),
                store.clone(),
                Arc::new(FakeOrderSubmitter {
                    result: submit_result("real-order-1", "LIVE"),
                }),
            );
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("liquidity_reward"));

        handle.set_phase(GatewayPhase::Live);
        handle
            .try_send(place_request("live-submit-1"))
            .expect("send should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        let event = subscriber
            .try_recv_relevant()
            .expect("accepted event should publish");
        assert_eq!(event.kind, OrderEventKind::Accepted);
        assert_eq!(
            event_exchange_id(&event),
            Some(&ExchangeOrderId::from("real-order-1"))
        );
        assert_ne!(
            event_exchange_id(&event),
            Some(&ExchangeOrderId::from("sim-token-1"))
        );
        let submission = store
            .load_latest_order_gateway_submission("live-submit-1")
            .expect("submission should load")
            .expect("submission should persist");
        assert_eq!(submission.submit_state, "Submitted");
    }

    #[tokio::test]
    async fn live_gateway_matched_submit_does_not_publish_fill() {
        let store = crate::storage::OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let config = OrderGatewayConfig {
            simulation_enabled: false,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test_with_store_and_submitter(
                config,
                Arc::new(AllowAllRiskCheck),
                store,
                Arc::new(FakeOrderSubmitter {
                    result: submit_result("matched-order-1", "MATCHED"),
                }),
            );
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("liquidity_reward"));

        handle.set_phase(GatewayPhase::Live);
        handle
            .try_send(place_request("matched-submit-1"))
            .expect("send should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        let accepted = subscriber
            .try_recv_relevant()
            .expect("matched submit should publish accepted event");
        assert_eq!(accepted.kind, OrderEventKind::Accepted);
        assert_eq!(
            event_exchange_id(&accepted),
            Some(&ExchangeOrderId::from("matched-order-1"))
        );
        assert!(subscriber.try_recv_relevant().is_err());
    }

    #[tokio::test]
    async fn gateway_runtime_persists_published_acceptance_event() {
        let store = crate::storage::OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, _ring, _observation_tx) = OrderGateway::new_for_test_with_store(
            config,
            Arc::new(AllowAllRiskCheck),
            store.clone(),
        );

        handle.set_phase(GatewayPhase::Live);
        handle
            .try_send(place_request("runtime-persist-1"))
            .expect("send should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        let active = store
            .load_order_gateway_recoverable_orders()
            .expect("recoverable orders should load");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].local_id, "runtime-persist-1");
        assert_eq!(active[0].local_state, "Open");
    }

    struct FailingCancelSubmitter;

    impl OrderCancelSubmitter for FailingCancelSubmitter {
        fn cancel<'a>(
            &'a self,
            _targets: &'a [OrderCancelTarget],
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<OrderCancelResult>> + Send + 'a>> {
            Box::pin(async move { Err(anyhow::anyhow!("cancel transport failed")) })
        }
    }

    #[derive(Default)]
    struct RemoteNotFoundCancelSubmitter {
        calls: AtomicUsize,
    }

    impl OrderCancelSubmitter for RemoteNotFoundCancelSubmitter {
        fn cancel<'a>(
            &'a self,
            targets: &'a [OrderCancelTarget],
        ) -> Pin<Box<dyn Future<Output = anyhow::Result<OrderCancelResult>> + Send + 'a>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(OrderCancelResult {
                    local_ids: Vec::new(),
                    not_canceled: targets
                        .iter()
                        .map(|target| OrderCancelRejected {
                            exch_id: target.exch_id.clone(),
                            reason: "order can't be found - already canceled or matched"
                                .to_string(),
                        })
                        .collect(),
                    rest_request_json: "{}".to_string(),
                    rest_response_json: Some("{\"not_canceled\":true}".to_string()),
                    rest_status_code: Some(200),
                })
            })
        }
    }

    #[tokio::test]
    async fn cancel_not_found_marks_order_remote_cancelled_locally() {
        let config = OrderGatewayConfig {
            simulation_enabled: false,
            request_ring_capacity: 8,
            event_ring_capacity: 16,
        };
        let store = OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let cancel_submitter = Arc::new(RemoteNotFoundCancelSubmitter::default());
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test_with_store_and_submitters(
                config,
                Arc::new(AllowAllRiskCheck),
                store,
                Arc::new(SimulatedOrderSubmitter),
                cancel_submitter.clone(),
            );
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("market_maker"));
        handle.set_phase(GatewayPhase::Live);

        handle
            .try_send(place_request_for_market(
                "market_maker",
                "market-a",
                "token-yes",
                "yes-open",
            ))
            .expect("place should enter gateway");
        assert!(gateway.run_one_request_for_test().await);
        handle
            .try_send(OrderRequest::Cancel(CancelOrderRequest {
                strategy_id: StrategyId::from("market_maker"),
                scope: CancelScope::Market {
                    market_id: MarketId::from("market-a"),
                },
                reason: Some(Arc::from("replace quote")),
            }))
            .expect("cancel should enter gateway");
        assert!(gateway.run_one_request_for_test().await);
        handle
            .try_send(OrderRequest::Cancel(CancelOrderRequest {
                strategy_id: StrategyId::from("market_maker"),
                scope: CancelScope::Market {
                    market_id: MarketId::from("market-a"),
                },
                reason: Some(Arc::from("replace quote")),
            }))
            .expect("second cancel should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        let record = gateway
            .state
            .order(&LocalOrderId::from("yes-open"))
            .expect("order should still exist");
        assert_eq!(record.local_state, LocalOrderState::Cancelled);
        assert_eq!(cancel_submitter.calls.load(Ordering::SeqCst), 1);
        let cancelled = std::iter::from_fn(|| subscriber.try_recv_relevant().ok())
            .find(|event| event.kind == OrderEventKind::Cancelled)
            .expect("remote gone order should publish cancelled event");
        assert!(matches!(
            cancelled.payload,
            OrderEventPayload::Cancelled {
                reason: CancelReason::RemoteCancelledOrMatched
            }
        ));
    }

    #[tokio::test]
    async fn failed_cancel_submit_keeps_order_active_locally() {
        let config = OrderGatewayConfig {
            simulation_enabled: false,
            request_ring_capacity: 8,
            event_ring_capacity: 16,
        };
        let store = OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test_with_store_and_submitters(
                config,
                Arc::new(AllowAllRiskCheck),
                store,
                Arc::new(SimulatedOrderSubmitter),
                Arc::new(FailingCancelSubmitter),
            );
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("market_maker"));
        handle.set_phase(GatewayPhase::Live);

        handle
            .try_send(place_request_for_market(
                "market_maker",
                "market-a",
                "token-yes",
                "yes-open",
            ))
            .expect("place should enter gateway");
        assert!(gateway.run_one_request_for_test().await);
        handle
            .try_send(OrderRequest::Cancel(CancelOrderRequest {
                strategy_id: StrategyId::from("market_maker"),
                scope: CancelScope::Market {
                    market_id: MarketId::from("market-a"),
                },
                reason: Some(Arc::from("replace quote")),
            }))
            .expect("cancel should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        let record = gateway
            .state
            .order(&LocalOrderId::from("yes-open"))
            .expect("order should still exist");
        assert_eq!(record.local_state, LocalOrderState::Accepted);
        assert!(
            !std::iter::from_fn(|| subscriber.try_recv_relevant().ok())
                .any(|event| event.kind == OrderEventKind::Cancelled)
        );
    }

    #[tokio::test]
    async fn cancel_request_calls_cancel_submitter_before_local_cancelled_event() {
        let config = OrderGatewayConfig {
            simulation_enabled: false,
            request_ring_capacity: 8,
            event_ring_capacity: 16,
        };
        let store = OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let cancel_submitter = Arc::new(CountingCancelSubmitter::default());
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test_with_store_and_submitters(
                config,
                Arc::new(AllowAllRiskCheck),
                store,
                Arc::new(SimulatedOrderSubmitter),
                cancel_submitter.clone(),
            );
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("market_maker"));
        handle.set_phase(GatewayPhase::Live);

        handle
            .try_send(place_request_for_market(
                "market_maker",
                "market-a",
                "token-yes",
                "yes-open",
            ))
            .expect("place should enter gateway");
        assert!(gateway.run_one_request_for_test().await);
        handle
            .try_send(OrderRequest::Cancel(CancelOrderRequest {
                strategy_id: StrategyId::from("market_maker"),
                scope: CancelScope::Market {
                    market_id: MarketId::from("market-a"),
                },
                reason: Some(Arc::from("replace quote")),
            }))
            .expect("cancel should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        assert_eq!(cancel_submitter.calls.load(Ordering::SeqCst), 1);
        assert_eq!(cancel_submitter.target_count.load(Ordering::SeqCst), 1);
        assert!(
            std::iter::from_fn(|| subscriber.try_recv_relevant().ok())
                .any(|event| event.kind == OrderEventKind::Cancelled)
        );
    }

    #[tokio::test]
    async fn cancel_market_scope_cancels_matching_active_orders() {
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 16,
        };
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test(config, Arc::new(AllowAllRiskCheck));
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("market_maker"));
        handle.set_phase(GatewayPhase::Live);

        for request in [
            place_request_for_market("market_maker", "market-a", "token-yes", "yes-open"),
            place_request_for_market("market_maker", "market-a", "token-no", "no-open"),
            place_request_for_market("market_maker", "market-b", "token-other", "other-open"),
        ] {
            handle
                .try_send(request)
                .expect("place should enter gateway");
            assert!(gateway.run_one_request_for_test().await);
        }
        handle
            .try_send(OrderRequest::Cancel(CancelOrderRequest {
                strategy_id: StrategyId::from("market_maker"),
                scope: CancelScope::Market {
                    market_id: MarketId::from("market-a"),
                },
                reason: Some(Arc::from("cooldown")),
            }))
            .expect("cancel should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        let mut cancelled = Vec::new();
        while let Ok(event) = subscriber.try_recv_relevant() {
            if event.kind == OrderEventKind::Cancelled {
                cancelled.push(event.local_id.as_str().to_string());
            }
        }
        cancelled.sort();
        assert_eq!(
            cancelled,
            vec!["no-open".to_string(), "yes-open".to_string()]
        );
        assert_eq!(
            gateway
                .state
                .order(&LocalOrderId::from("other-open"))
                .expect("other order should exist")
                .local_state,
            LocalOrderState::Open
        );
    }

    #[tokio::test]
    async fn cancel_token_scope_cancels_matching_active_orders() {
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 16,
        };
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test(config, Arc::new(AllowAllRiskCheck));
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("market_maker"));
        handle.set_phase(GatewayPhase::Live);

        for request in [
            place_request_for_market("market_maker", "market-a", "token-yes", "yes-open"),
            place_request_for_market("market_maker", "market-a", "token-no", "no-open"),
        ] {
            handle
                .try_send(request)
                .expect("place should enter gateway");
            assert!(gateway.run_one_request_for_test().await);
        }
        handle
            .try_send(OrderRequest::Cancel(CancelOrderRequest {
                strategy_id: StrategyId::from("market_maker"),
                scope: CancelScope::Token {
                    token_id: TokenId::from("token-yes"),
                },
                reason: Some(Arc::from("cooldown")),
            }))
            .expect("cancel should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        let cancelled = std::iter::from_fn(|| subscriber.try_recv_relevant().ok())
            .filter(|event| event.kind == OrderEventKind::Cancelled)
            .map(|event| event.local_id.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(cancelled, vec!["yes-open".to_string()]);
        assert_eq!(
            gateway
                .state
                .order(&LocalOrderId::from("no-open"))
                .expect("no order should exist")
                .local_state,
            LocalOrderState::Open
        );
    }

    #[tokio::test]
    async fn query_active_orders_returns_only_matching_strategy_non_terminal_orders() {
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, _ring, _observation_tx) =
            OrderGateway::new_for_test(config, Arc::new(AllowAllRiskCheck));
        handle.set_phase(GatewayPhase::Live);

        handle
            .try_send(place_request_for_strategy(
                "strategy-a",
                "token-a-open",
                "a-open",
            ))
            .expect("send should enter gateway");
        assert!(gateway.run_one_request_for_test().await);
        handle
            .try_send(place_request_for_strategy(
                "strategy-a",
                "token-a-filled",
                "a-filled",
            ))
            .expect("send should enter gateway");
        assert!(gateway.run_one_request_for_test().await);
        handle
            .try_send(place_request_for_strategy(
                "strategy-b",
                "token-b-open",
                "b-open",
            ))
            .expect("send should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        gateway.handle_observation(GatewayObservation::SettlementTradeObserved {
            exch_id: ExchangeOrderId::from("sim-token-a-filled"),
            transaction_hash: Arc::from("0xabc"),
            fill_qty: dec(10.0),
            fill_price: dec(0.42),
            ts_ns: 10,
            recovery: false,
        });
        gateway.handle_observation(GatewayObservation::SettlementActivityConfirmed {
            exch_id: ExchangeOrderId::from("sim-token-a-filled"),
            transaction_hash: Arc::from("0xabc"),
            ts_ns: 11,
            recovery: false,
        });

        let query_handle = handle.clone();
        let query_task = tokio::spawn(async move {
            query_handle
                .query_active_orders(StrategyId::from("strategy-a"))
                .await
        });
        assert!(gateway.run_one_request_for_test().await);
        let active_orders = query_task
            .await
            .expect("query task should finish")
            .expect("query should return records");

        assert_eq!(active_orders.len(), 1);
        assert_eq!(active_orders[0].local_id, LocalOrderId::from("a-open"));
        assert_eq!(active_orders[0].strategy_id, StrategyId::from("strategy-a"));
        assert_eq!(active_orders[0].local_state, LocalOrderState::Open);
    }

    #[tokio::test]
    async fn query_order_is_strategy_scoped_and_supports_local_and_exchange_ids() {
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, _ring, _observation_tx) =
            OrderGateway::new_for_test(config, Arc::new(AllowAllRiskCheck));
        handle.set_phase(GatewayPhase::Live);

        handle
            .try_send(place_request_for_strategy(
                "strategy-a",
                "token-a",
                "a-local",
            ))
            .expect("send should enter gateway");
        assert!(gateway.run_one_request_for_test().await);

        let local_query_handle = handle.clone();
        let local_query = tokio::spawn(async move {
            local_query_handle
                .query_order(
                    StrategyId::from("strategy-a"),
                    OrderQueryLookup::LocalId(LocalOrderId::from("a-local")),
                )
                .await
        });
        assert!(gateway.run_one_request_for_test().await);
        let by_local = local_query
            .await
            .expect("query task should finish")
            .expect("query should return response")
            .expect("matching local id should find order");
        assert_eq!(by_local.local_id, LocalOrderId::from("a-local"));
        assert_eq!(by_local.exch_id, Some(ExchangeOrderId::from("sim-token-a")));

        let exchange_query_handle = handle.clone();
        let exchange_query = tokio::spawn(async move {
            exchange_query_handle
                .query_order(
                    StrategyId::from("strategy-a"),
                    OrderQueryLookup::ExchangeId(ExchangeOrderId::from("sim-token-a")),
                )
                .await
        });
        assert!(gateway.run_one_request_for_test().await);
        let by_exchange = exchange_query
            .await
            .expect("query task should finish")
            .expect("query should return response")
            .expect("matching exchange id should find order");
        assert_eq!(by_exchange.local_id, LocalOrderId::from("a-local"));

        let mismatched_strategy_handle = handle.clone();
        let mismatched_strategy_query = tokio::spawn(async move {
            mismatched_strategy_handle
                .query_order(
                    StrategyId::from("strategy-b"),
                    OrderQueryLookup::LocalId(LocalOrderId::from("a-local")),
                )
                .await
        });
        assert!(gateway.run_one_request_for_test().await);
        let mismatched_strategy_result = mismatched_strategy_query
            .await
            .expect("query task should finish")
            .expect("query should return response");
        assert!(mismatched_strategy_result.is_none());
    }

    fn upsert_recoverable_gateway_order(
        store: &crate::storage::OrderStore,
        local_id: &str,
        exch_id: &str,
    ) {
        store
            .upsert_order_gateway_order(&crate::storage::OrderGatewayOrderSnapshot {
                strategy_id: "liquidity_reward".to_string(),
                market_id: Some("market-1".to_string()),
                token_id: "token-1".to_string(),
                local_id: local_id.to_string(),
                exch_id: Some(exch_id.to_string()),
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
            })
            .expect("snapshot should write");
    }

    #[tokio::test]
    async fn gateway_startup_recovery_restores_orders_and_enters_live_phase() {
        let store = crate::storage::OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        upsert_recoverable_gateway_order(&store, "recover-startup", "exch-recover-startup");
        let config = OrderGatewayConfig {
            simulation_enabled: false,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test_with_store(config, Arc::new(AllowAllRiskCheck), store);
        let mut system_subscriber = ring.subscribe_for_strategy(StrategyId::from("SYSTEM"));

        gateway
            .complete_startup_recovery()
            .await
            .expect("startup recovery should run");

        assert!(
            gateway
                .state
                .order(&LocalOrderId::from("recover-startup"))
                .is_some()
        );
        let completed = system_subscriber
            .try_recv_relevant()
            .expect("recovery completion should publish");
        assert_eq!(completed.kind, OrderEventKind::RecoveryCompleted);
        handle
            .try_send(place_request("live-after-startup-recovery"))
            .expect("gateway should be live after startup recovery");
    }

    #[tokio::test]
    async fn gateway_startup_reconciliation_marks_missing_remote_order_cancelled() {
        let store = crate::storage::OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        upsert_recoverable_gateway_order(&store, "remote-cancelled", "exch-remote-cancelled");
        let config = OrderGatewayConfig {
            simulation_enabled: false,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test_with_store_and_remote_reader(
                config,
                Arc::new(AllowAllRiskCheck),
                store,
                Arc::new(FakeRemoteOrderReader {
                    open_orders: Vec::new(),
                }),
            );
        let mut strategy_subscriber =
            ring.subscribe_for_strategy(StrategyId::from("liquidity_reward"));

        gateway
            .complete_startup_recovery()
            .await
            .expect("startup recovery should reconcile remote orders");

        let record = gateway
            .state
            .order(&LocalOrderId::from("remote-cancelled"))
            .expect("recovered order should remain queryable");
        assert_eq!(record.local_state, LocalOrderState::Cancelled);
        assert!(
            gateway
                .state
                .active_orders_for_strategy(&StrategyId::from("liquidity_reward"))
                .is_empty()
        );

        let recovered = strategy_subscriber
            .try_recv_relevant()
            .expect("recovered event should publish");
        let cancelled = strategy_subscriber
            .try_recv_relevant()
            .expect("remote cancellation should publish");
        assert_eq!(recovered.kind, OrderEventKind::Recovered);
        assert_eq!(cancelled.kind, OrderEventKind::Cancelled);
        assert!(cancelled.recovery);
        assert!(matches!(
            cancelled.payload,
            OrderEventPayload::Cancelled {
                reason: CancelReason::RemoteCancelled
            }
        ));
        handle
            .try_send(place_request("live-after-reconciliation"))
            .expect("gateway should be live after reconciliation");
    }

    #[tokio::test]
    async fn gateway_recovery_allocates_event_seq_from_shared_allocator() {
        let store = crate::storage::OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        store
            .append_position_journal(&crate::storage::PositionJournalInsert {
                seq: 49,
                ts_ms: 1000,
                event_type: "OrderWorkingRegistered",
                strategy_id: Some("market_maker"),
                token_id: "token-1",
                local_order_id: Some("local-1"),
                exchange_order_id: Some("exch-1"),
                side: Some("Buy"),
                qty: Some("10"),
                price: Some("0.57"),
                source: "Live",
                recovery: false,
                payload_json: "{}",
            })
            .expect("position journal should persist");
        upsert_recoverable_gateway_order(&store, "recover-open", "exch-recover-open");
        let config = OrderGatewayConfig {
            simulation_enabled: false,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, _handle, ring, _observation_tx) =
            OrderGateway::new_for_test_with_store(config, Arc::new(AllowAllRiskCheck), store);
        let mut strategy_subscriber =
            ring.subscribe_for_strategy(StrategyId::from("liquidity_reward"));

        gateway
            .recover_from_gateway_store()
            .expect("gateway recovery should run");

        let recovered = strategy_subscriber
            .try_recv_relevant()
            .expect("recovered event should publish");
        assert_eq!(recovered.seq, 50);
    }

    #[tokio::test]
    async fn gateway_recovery_restores_open_order_and_publishes_completion() {
        let store = crate::storage::OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        upsert_recoverable_gateway_order(&store, "recover-open", "exch-recover-open");
        let config = OrderGatewayConfig {
            simulation_enabled: false,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test_with_store(config, Arc::new(AllowAllRiskCheck), store);
        let mut strategy_subscriber =
            ring.subscribe_for_strategy(StrategyId::from("liquidity_reward"));
        let mut system_subscriber = ring.subscribe_for_strategy(StrategyId::from("SYSTEM"));

        gateway
            .recover_from_gateway_store()
            .expect("gateway recovery should run");

        let record = gateway
            .state
            .order(&LocalOrderId::from("recover-open"))
            .expect("recovered order should be in state");
        assert_eq!(record.local_state, LocalOrderState::Open);
        assert_eq!(
            record.exch_id,
            Some(ExchangeOrderId::from("exch-recover-open"))
        );
        assert_eq!(
            gateway
                .state
                .local_by_exch
                .get(&ExchangeOrderId::from("exch-recover-open")),
            Some(&LocalOrderId::from("recover-open"))
        );

        let recovered = strategy_subscriber
            .try_recv_relevant()
            .expect("recovered event should publish");
        assert_eq!(recovered.kind, OrderEventKind::Recovered);
        assert!(recovered.recovery);
        assert!(matches!(
            recovered.payload,
            OrderEventPayload::Recovered {
                current_state: LocalOrderState::Open
            }
        ));

        let completed = system_subscriber
            .try_recv_relevant()
            .expect("recovery completion should publish");
        assert_eq!(completed.kind, OrderEventKind::RecoveryCompleted);
        assert!(matches!(
            completed.payload,
            OrderEventPayload::RecoveryCompleted {
                recovered_order_count: 1,
                unresolved_order_count: 0,
                failed_unrecoverable_count: 0,
            }
        ));
        handle
            .try_send(place_request("live-after-gateway-recovery"))
            .expect("gateway should be live after recovery");
    }

    #[tokio::test]
    async fn gateway_recovery_replays_pending_ws_observation_after_exchange_correlation() {
        let store = crate::storage::OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        upsert_recoverable_gateway_order(&store, "recover-pending", "exch-recover-pending");
        let config = OrderGatewayConfig {
            simulation_enabled: false,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, _handle, ring, _observation_tx) =
            OrderGateway::new_for_test_with_store(config, Arc::new(AllowAllRiskCheck), store);
        let mut strategy_subscriber =
            ring.subscribe_for_strategy(StrategyId::from("liquidity_reward"));

        let pending_events = gateway
            .state
            .apply_observation(GatewayObservation::WsPartialFill {
                exch_id: ExchangeOrderId::from("exch-recover-pending"),
                fill_qty: dec(2.0),
                fill_price: dec(0.42),
                cum_qty: dec(2.0),
                avg_fill_price: Some(dec(0.42)),
                ts_ns: 9,
                recovery: true,
            });
        assert!(pending_events.is_empty());

        gateway
            .recover_from_gateway_store()
            .expect("gateway recovery should run");

        let recovered = strategy_subscriber
            .try_recv_relevant()
            .expect("recovered event should publish");
        let fill = strategy_subscriber
            .try_recv_relevant()
            .expect("pending fill should replay after recovery correlation");
        assert_eq!(recovered.kind, OrderEventKind::Recovered);
        assert_eq!(fill.kind, OrderEventKind::PartialFill);
        assert!(fill.recovery);
        let record = gateway
            .state
            .order(&LocalOrderId::from("recover-pending"))
            .expect("recovered order should be in state");
        assert_eq!(record.local_state, LocalOrderState::PartiallyFilled);
        assert_eq!(record.filled_size_total, dec(2.0));
        assert_eq!(record.remaining_size, dec(8.0));
    }

    #[test]
    fn settlement_trade_observed_is_stored_pending_without_fill_event() {
        let mut state = GatewayState::default();
        let request = match place_request("local-1") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);
        state.apply_observation(GatewayObservation::RestAccepted {
            local_id: LocalOrderId::from("local-1"),
            exch_id: Some(ExchangeOrderId::from("exch-1")),
            ts_ns: 1,
            recovery: false,
        });

        let events = state.apply_observation(GatewayObservation::SettlementTradeObserved {
            exch_id: ExchangeOrderId::from("exch-1"),
            transaction_hash: Arc::from("0xabc"),
            fill_qty: dec(3.0),
            fill_price: dec(0.42),
            ts_ns: 2,
            recovery: false,
        });

        assert!(events.is_empty());
        assert!(state.has_pending_settlement_for_test("0xabc", "exch-1"));
    }

    #[test]
    fn settlement_activity_confirmation_publishes_partial_fill_for_pending_trade() {
        let mut state = GatewayState::default();
        let request = match place_request("settlement-partial") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);
        state.apply_observation(GatewayObservation::RestAccepted {
            local_id: LocalOrderId::from("settlement-partial"),
            exch_id: Some(ExchangeOrderId::from("exch-1")),
            ts_ns: 1,
            recovery: false,
        });
        state.apply_observation(GatewayObservation::SettlementTradeObserved {
            exch_id: ExchangeOrderId::from("exch-1"),
            transaction_hash: Arc::from("0xabc"),
            fill_qty: dec(3.0),
            fill_price: dec(0.42),
            ts_ns: 2,
            recovery: false,
        });

        let events = state.apply_observation(GatewayObservation::SettlementActivityConfirmed {
            exch_id: ExchangeOrderId::from("exch-1"),
            transaction_hash: Arc::from("0xabc"),
            ts_ns: 3,
            recovery: false,
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, OrderEventKind::PartialFill);
        assert!(matches!(
            events[0].payload,
            OrderEventPayload::PartialFill { fill_qty, cum_qty, .. }
                if fill_qty == dec(3.0) && cum_qty == dec(3.0)
        ));
        let order = state
            .order(&LocalOrderId::from("settlement-partial"))
            .unwrap();
        assert_eq!(order.local_state, LocalOrderState::PartiallyFilled);
        assert_eq!(order.filled_size_total, dec(3.0));
        assert_eq!(order.remaining_size, dec(7.0));
    }

    #[test]
    fn settlement_activity_confirmation_publishes_fill_when_order_remaining_is_consumed() {
        let mut state = GatewayState::default();
        let request = match place_request("settlement-fill") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);
        state.apply_observation(GatewayObservation::RestAccepted {
            local_id: LocalOrderId::from("settlement-fill"),
            exch_id: Some(ExchangeOrderId::from("exch-1")),
            ts_ns: 1,
            recovery: false,
        });
        state.apply_observation(GatewayObservation::SettlementTradeObserved {
            exch_id: ExchangeOrderId::from("exch-1"),
            transaction_hash: Arc::from("0xdef"),
            fill_qty: dec(10.0),
            fill_price: dec(0.42),
            ts_ns: 2,
            recovery: false,
        });

        let events = state.apply_observation(GatewayObservation::SettlementActivityConfirmed {
            exch_id: ExchangeOrderId::from("exch-1"),
            transaction_hash: Arc::from("0xdef"),
            ts_ns: 3,
            recovery: false,
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, OrderEventKind::Fill);
        assert!(matches!(
            events[0].payload,
            OrderEventPayload::Fill { fill_qty, cum_qty, .. }
                if fill_qty == dec(10.0) && cum_qty == dec(10.0)
        ));
        let order = state.order(&LocalOrderId::from("settlement-fill")).unwrap();
        assert_eq!(order.local_state, LocalOrderState::Filled);
        assert_eq!(order.filled_size_total, dec(10.0));
        assert_eq!(order.remaining_size, Decimal::ZERO);
    }

    #[test]
    fn settlement_activity_confirmation_is_idempotent_for_same_transaction_and_order() {
        let mut state = GatewayState::default();
        let request = match place_request("settlement-idempotent") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);
        state.apply_observation(GatewayObservation::RestAccepted {
            local_id: LocalOrderId::from("settlement-idempotent"),
            exch_id: Some(ExchangeOrderId::from("exch-1")),
            ts_ns: 1,
            recovery: false,
        });
        state.apply_observation(GatewayObservation::SettlementTradeObserved {
            exch_id: ExchangeOrderId::from("exch-1"),
            transaction_hash: Arc::from("0xabc"),
            fill_qty: dec(3.0),
            fill_price: dec(0.42),
            ts_ns: 2,
            recovery: false,
        });

        let first = state.apply_observation(GatewayObservation::SettlementActivityConfirmed {
            exch_id: ExchangeOrderId::from("exch-1"),
            transaction_hash: Arc::from("0xabc"),
            ts_ns: 3,
            recovery: false,
        });
        let second = state.apply_observation(GatewayObservation::SettlementActivityConfirmed {
            exch_id: ExchangeOrderId::from("exch-1"),
            transaction_hash: Arc::from("0xabc"),
            ts_ns: 4,
            recovery: false,
        });

        assert_eq!(first.len(), 1);
        assert!(second.is_empty());
        let order = state
            .order(&LocalOrderId::from("settlement-idempotent"))
            .unwrap();
        assert_eq!(order.filled_size_total, dec(3.0));
    }

    #[test]
    fn settlement_trade_before_rest_correlation_applies_after_rest_accepted() {
        let mut state = GatewayState::default();
        let request = match place_request("settlement-before-rest") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);
        state.apply_observation(GatewayObservation::SettlementTradeObserved {
            exch_id: ExchangeOrderId::from("exch-1"),
            transaction_hash: Arc::from("0xabc"),
            fill_qty: dec(3.0),
            fill_price: dec(0.42),
            ts_ns: 1,
            recovery: false,
        });
        state.apply_observation(GatewayObservation::SettlementActivityConfirmed {
            exch_id: ExchangeOrderId::from("exch-1"),
            transaction_hash: Arc::from("0xabc"),
            ts_ns: 2,
            recovery: false,
        });

        let events = state.apply_observation(GatewayObservation::RestAccepted {
            local_id: LocalOrderId::from("settlement-before-rest"),
            exch_id: Some(ExchangeOrderId::from("exch-1")),
            ts_ns: 3,
            recovery: false,
        });

        assert!(
            events
                .iter()
                .any(|event| event.kind == OrderEventKind::PartialFill)
        );
        let order = state
            .order(&LocalOrderId::from("settlement-before-rest"))
            .unwrap();
        assert_eq!(order.filled_size_total, dec(3.0));
    }

    #[test]
    fn pending_private_ws_full_match_does_not_replay_fill_after_rest_acceptance() {
        let mut state = GatewayState::default();
        let request = match place_request("ws-pending-fill") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);

        let pending_events = state.apply_private_ws_order_update(PrivateWsOrderUpdate {
            exch_id: ExchangeOrderId::from("exch-ws-pending-fill"),
            token_id: TokenId::from("token-1"),
            market_id: MarketId::from("market-1"),
            fill_price: dec(0.42),
            previous_size_matched: Some(dec(4.0)),
            current_size_matched: Some(dec(10.0)),
            original_size: Some(dec(10.0)),
            remote_status_code: Some(Arc::from("matched")),
            ts_ns: 10,
            recovery: false,
        });
        assert!(pending_events.is_empty());

        let events = state.apply_observation(GatewayObservation::RestAccepted {
            local_id: LocalOrderId::from("ws-pending-fill"),
            exch_id: Some(ExchangeOrderId::from("exch-ws-pending-fill")),
            ts_ns: 11,
            recovery: false,
        });

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, OrderEventKind::Accepted);
        assert_ne!(events[1].kind, OrderEventKind::Fill);
        let record = state.order(&LocalOrderId::from("ws-pending-fill")).unwrap();
        assert_ne!(record.local_state, LocalOrderState::Filled);
        assert_eq!(record.filled_size_total, Decimal::ZERO);
        assert_eq!(record.remaining_size, dec(10.0));
    }

    #[test]
    fn private_ws_full_match_does_not_publish_fill_before_settlement_confirmation() {
        let mut state = GatewayState::default();
        let request = match place_request("local-1") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);
        state.apply_observation(GatewayObservation::RestAccepted {
            local_id: LocalOrderId::from("local-1"),
            exch_id: Some(ExchangeOrderId::from("exch-1")),
            ts_ns: 1,
            recovery: false,
        });

        let events = state.apply_observation(GatewayObservation::PrivateWsOrderUpdate(
            PrivateWsOrderUpdate {
                exch_id: ExchangeOrderId::from("exch-1"),
                token_id: TokenId::from("token-1"),
                market_id: MarketId::from("market-1"),
                fill_price: dec(0.42),
                previous_size_matched: Some(Decimal::ZERO),
                current_size_matched: Some(dec(10.0)),
                original_size: Some(dec(10.0)),
                remote_status_code: Some(Arc::from("matched")),
                ts_ns: 2,
                recovery: false,
            },
        ));

        assert!(
            events
                .iter()
                .all(|event| event.kind != OrderEventKind::Fill)
        );
        let order = state
            .order(&LocalOrderId::from("local-1"))
            .expect("order should stay tracked");
        assert_ne!(order.local_state, LocalOrderState::Filled);
        assert_eq!(order.filled_size_total, Decimal::ZERO);
        assert_eq!(order.remaining_size, dec(10.0));
    }

    #[test]
    fn private_ws_order_update_does_not_publish_partial_fill_before_settlement_confirmation() {
        let mut state = GatewayState::default();
        let request = match place_request("ws-partial") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);
        state.apply_observation(GatewayObservation::RestAccepted {
            local_id: LocalOrderId::from("ws-partial"),
            exch_id: Some(ExchangeOrderId::from("exch-ws-partial")),
            ts_ns: 1,
            recovery: false,
        });

        let events = state.apply_private_ws_order_update(PrivateWsOrderUpdate {
            exch_id: ExchangeOrderId::from("exch-ws-partial"),
            token_id: TokenId::from("token-1"),
            market_id: MarketId::from("market-1"),
            fill_price: dec(0.42),
            previous_size_matched: Some(dec(1.0)),
            current_size_matched: Some(dec(3.0)),
            original_size: Some(dec(10.0)),
            remote_status_code: Some(Arc::from("matched")),
            ts_ns: 9,
            recovery: false,
        });

        assert!(
            events
                .iter()
                .all(|event| event.kind != OrderEventKind::PartialFill
                    && event.kind != OrderEventKind::Fill)
        );
        let record = state.order(&LocalOrderId::from("ws-partial")).unwrap();
        assert_ne!(record.local_state, LocalOrderState::PartiallyFilled);
        assert_ne!(record.local_state, LocalOrderState::Filled);
        assert_eq!(record.filled_size_total, Decimal::ZERO);
        assert_eq!(record.remaining_size, dec(10.0));
    }

    #[test]
    fn private_ws_order_update_does_not_publish_fill_before_settlement_confirmation() {
        let mut state = GatewayState::default();
        let request = match place_request("ws-fill") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.record_submitted(request);
        state.apply_observation(GatewayObservation::RestAccepted {
            local_id: LocalOrderId::from("ws-fill"),
            exch_id: Some(ExchangeOrderId::from("exch-ws-fill")),
            ts_ns: 1,
            recovery: false,
        });

        let events = state.apply_private_ws_order_update(PrivateWsOrderUpdate {
            exch_id: ExchangeOrderId::from("exch-ws-fill"),
            token_id: TokenId::from("token-1"),
            market_id: MarketId::from("market-1"),
            fill_price: dec(0.42),
            previous_size_matched: Some(dec(4.0)),
            current_size_matched: Some(dec(10.0)),
            original_size: Some(dec(10.0)),
            remote_status_code: Some(Arc::from("matched")),
            ts_ns: 10,
            recovery: false,
        });

        assert!(
            events
                .iter()
                .all(|event| event.kind != OrderEventKind::Fill)
        );
        let record = state.order(&LocalOrderId::from("ws-fill")).unwrap();
        assert_ne!(record.local_state, LocalOrderState::Filled);
        assert_eq!(record.filled_size_total, Decimal::ZERO);
        assert_eq!(record.remaining_size, dec(10.0));
    }

    #[test]
    fn recovery_marks_order_without_signed_payload_unrecoverable() {
        let store = crate::storage::OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        store
            .upsert_order_gateway_order(&crate::storage::OrderGatewayOrderSnapshot {
                strategy_id: "liquidity_reward".to_string(),
                market_id: Some("liquidity_reward".to_string()),
                token_id: "token-1".to_string(),
                local_id: "missing-signed".to_string(),
                exch_id: None,
                side: "Buy".to_string(),
                order_type: "LimitGtc".to_string(),
                price: Some("0.42".to_string()),
                size: "10".to_string(),
                local_state: "UnknownPending".to_string(),
                remote_status_code: None,
                filled_size_total: "0".to_string(),
                remaining_size: "10".to_string(),
                avg_fill_price: None,
                last_submission_attempt: Some(1),
                last_event_seq: 1,
                terminal_at_ms: None,
            })
            .expect("snapshot should write");

        let mut state = GatewayState::default();
        let events = recover_gateway_orders_for_test(&store, &mut state).expect("recovery runs");

        assert!(events.iter().any(|event| {
            event.kind == OrderEventKind::Failed
                && matches!(
                    event.payload,
                    OrderEventPayload::Failed {
                        kind: FailureKind::MissingSignedPayloadAfterRestart
                    }
                )
        }));
        assert!(events.iter().all(|event| event.recovery));
    }

    #[tokio::test]
    async fn simulated_gateway_place_emits_accepted_and_open() {
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, ring, _observation_tx) =
            OrderGateway::new_for_test(config, Arc::new(AllowAllRiskCheck));
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("liquidity_reward"));

        handle.set_phase(GatewayPhase::Live);
        handle
            .try_send(place_request("sim-place-1"))
            .expect("send should work");
        assert!(gateway.run_one_request_for_test().await);

        let first = subscriber.try_recv_relevant().expect("accepted event");
        let second = subscriber.try_recv_relevant().expect("open event");
        assert_eq!(first.kind, OrderEventKind::Accepted);
        assert_eq!(second.kind, OrderEventKind::Open);
        assert!(matches!(second.payload, OrderEventPayload::Open { .. }));
    }
}
