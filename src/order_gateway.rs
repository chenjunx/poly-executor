use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use polymarket_client_sdk_v2::types::Decimal;
use tokio::sync::{broadcast, mpsc};

use crate::storage::{OrderGatewayEventInsert, OrderGatewayOrderSnapshot, OrderStore};

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

#[derive(Debug, Clone, PartialEq)]
pub enum OrderRequest {
    Place(PlaceOrderRequest),
    Cancel(CancelOrderRequest),
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
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderRecord {
    pub strategy_id: StrategyId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub local_id: LocalOrderId,
    pub exch_id: Option<ExchangeOrderId>,
    pub local_state: LocalOrderState,
    pub filled_size_total: Decimal,
    pub remaining_size: Decimal,
    pub avg_fill_price: Option<Decimal>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GatewayObservation {
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
    WsFill {
        exch_id: Option<ExchangeOrderId>,
        local_id: Option<LocalOrderId>,
        token_id: TokenId,
        side: OrderSide,
        fill_delta: Decimal,
        fill_price: Decimal,
        trade_id: Arc<str>,
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
            GatewayObservation::WsFill {
                exch_id: Some(ref exch_id),
                ..
            } if !self.local_by_exch.contains_key(exch_id) => {
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
            GatewayObservation::WsFill {
                exch_id,
                local_id,
                fill_delta,
                fill_price,
                ..
            } => self.apply_ws_fill(exch_id, local_id, fill_delta, fill_price),
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

    fn apply_ws_fill(
        &mut self,
        exch_id: Option<ExchangeOrderId>,
        local_id: Option<LocalOrderId>,
        fill_delta: Decimal,
        fill_price: Decimal,
    ) -> Vec<OrderEventEnvelope> {
        let local_id = match (local_id, exch_id) {
            (Some(local_id), _) => local_id,
            (None, Some(exch_id)) => {
                let Some(local_id) = self.local_by_exch.get(&exch_id) else {
                    return Vec::new();
                };
                local_id.clone()
            }
            (None, None) => return Vec::new(),
        };
        let Some(record) = self.orders.get(&local_id) else {
            return Vec::new();
        };
        let cum_qty = record.filled_size_total + fill_delta;
        self.apply_ws_partial_fill(
            record
                .exch_id
                .clone()
                .unwrap_or_else(|| ExchangeOrderId::from("")),
            fill_delta,
            fill_price,
            cum_qty,
            Some(fill_price),
            0,
            false,
        )
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
        }
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
    config: OrderGatewayConfig,
    order_store: Option<OrderStore>,
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
        Self::new_for_test_inner(config, risk, Some(order_store))
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
        Self::new_for_test_inner(config, risk, None)
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
        Self::new_for_test_inner(config, risk, Some(order_store))
    }

    fn new_for_test_inner(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
        order_store: Option<OrderStore>,
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
        let event_ring = OrderEventRing::new(config.event_ring_capacity);
        let gateway = Self {
            rx,
            observation_rx,
            event_ring: event_ring.clone(),
            handle: handle.clone(),
            state: GatewayState::default(),
            risk,
            config,
            order_store,
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
    }

    async fn handle_request(&mut self, request: OrderRequest) {
        match request {
            OrderRequest::Place(request) => self.handle_place_request(request).await,
            OrderRequest::Cancel(request) => self.handle_cancel_request(request).await,
        }
    }

    async fn handle_place_request(&mut self, request: PlaceOrderRequest) {
        match self.risk.check_place(&request, &self.state) {
            RiskDecision::Allow => {
                self.state.record_submitted(request.clone());
                if self.config.simulation_enabled {
                    let exch_id =
                        ExchangeOrderId::from(format!("sim-{}", request.token_id.as_str()));
                    let token_id = request.token_id;
                    let market_id = request.market_id.unwrap_or_else(|| MarketId::from(""));
                    for event in self
                        .state
                        .apply_observation(GatewayObservation::RestAccepted {
                            local_id: request.local_id,
                            exch_id: Some(exch_id.clone()),
                            ts_ns: self.state.next_seq + 1,
                            recovery: false,
                        })
                    {
                        self.publish_and_persist(event);
                    }
                    for event in self.state.apply_observation(GatewayObservation::WsOpen {
                        exch_id,
                        token_id,
                        market_id,
                        remote_status_code: Some(Arc::from("open")),
                        ts_ns: self.state.next_seq + 1,
                        recovery: false,
                    }) {
                        self.publish_and_persist(event);
                    }
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
        }
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

    fn publish_and_persist(&mut self, event: OrderEventEnvelope) {
        if let Some(store) = &self.order_store {
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
            strategy_id,
            rx: self.tx.subscribe(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderEventPublishError {
    Closed,
}

pub struct OrderEventSubscriber {
    strategy_id: StrategyId,
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
                Ok(event) if event.strategy_id == self.strategy_id => return Ok(event),
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
            side: "Buy".to_string(),
            order_type: "LimitGtc".to_string(),
            price: None,
            size: (record.filled_size_total + record.remaining_size).to_string(),
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
            let local_id = LocalOrderId::from(snapshot.local_id.clone());
            let record = OrderRecord {
                strategy_id: StrategyId::from(snapshot.strategy_id),
                market_id: snapshot
                    .market_id
                    .map(MarketId::from)
                    .unwrap_or_else(|| MarketId::from("")),
                token_id: TokenId::from(snapshot.token_id),
                local_id: local_id.clone(),
                exch_id: None,
                local_state: LocalOrderState::Failed,
                filled_size_total: Decimal::try_from(0_f64).expect("zero decimal"),
                remaining_size: Decimal::try_from(0_f64).expect("zero decimal"),
                avg_fill_price: None,
            };
            state.orders.insert(local_id, record.clone());
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

    pub fn set_phase(&self, phase: GatewayPhase) {
        self.phase.store(phase.as_u8(), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use polymarket_client_sdk_v2::types::Decimal;

    fn place_request(local_id: &str) -> OrderRequest {
        OrderRequest::Place(PlaceOrderRequest {
            strategy_id: StrategyId::from("liquidity_reward"),
            market_id: Some(MarketId::from("liquidity_reward")),
            token_id: TokenId::from("token-1"),
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

    #[test]
    fn reducer_binds_ws_observation_that_arrives_before_rest_acceptance() {
        let mut state = GatewayState::default();
        let request = match place_request("local-1") {
            OrderRequest::Place(request) => request,
            OrderRequest::Cancel(_) => unreachable!(),
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
            OrderRequest::Cancel(_) => unreachable!(),
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
            OrderRequest::Cancel(_) => unreachable!(),
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
            OrderRequest::Cancel(_) => unreachable!(),
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
