# Order Gateway Ring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace direct strategy-to-order-executor flow with a single-threaded Order Gateway using an MPSC request ring and SPMC event ring, with recoverable order persistence and fine-grained order events.

**Architecture:** Add a new `order_gateway` module that owns request/event types, ring handles, reducer state, risk hooks, recovery phase, and gateway runtime. Strategies submit only `Place`/`Cancel` requests; the Gateway normalizes REST/WS/timeout observations into fixed-envelope order events. Persistence is extended in `OrderStore` with gateway snapshot/event/submission/cancel-attempt tables, while PositionKeeper and chain position confirmation remain out of scope.

**Tech Stack:** Rust 2024, Tokio MPSC/Broadcast channels, rusqlite, Polymarket SDK, existing `OrderStore`, existing strategy abstraction, TDD with `cargo test` and `cargo fmt`.

---

## Implementation Decisions for First Version

- `request_ring`: `tokio::sync::mpsc::channel<OrderRequest>` wrapped by `OrderGatewayHandle` using `try_send` for non-blocking strategy submissions.
- `event_ring`: `tokio::sync::broadcast::channel<OrderEventEnvelope>` used as a bounded SPMC ring. Broadcast lag maps to subscriber sequence-gap handling and gateway health logging.
- ID types: string-backed newtypes in the first version, so integration with existing `String`/`Arc<str>` IDs is straightforward.
- Payload: strongly typed `OrderEventPayload` in the first version. Keep `OrderEventEnvelope` fixed-key fields in the documented order so a later `#[repr(C, align(64))]` + `[u8; PAYLOAD_SIZE]` payload is possible after replacing the enum payload.
- No git commits during implementation unless the user explicitly asks.
- Stop after each major task with diff and test output for review.

---

## File Structure

- Create: `src/order_gateway.rs`
  - ID newtypes: `StrategyId`, `LocalOrderId`, `TokenId`, `MarketId`, `ExchangeOrderId`.
  - Request types: `OrderRequest`, `PlaceOrderRequest`, `CancelOrderRequest`, `CancelScope`, `GatewayOrderType`, `TimeInForce`, `OrderSide`.
  - Event types: `OrderEventEnvelope`, `OrderEventKind`, `OrderEventPayload`, local/remote state and reason enums.
  - Ring handles: `OrderGatewayHandle`, `OrderEventSubscriber`, `OrderRequestError`, `OrderEventPollError`.
  - Reducer state: `GatewayState`, `OrderRecord`, `GatewayObservation`, transition helpers.
  - Runtime shell: `OrderGateway`, `OrderGatewayConfig`, `OrderRiskCheck`, `AllowAllRiskCheck`.
- Modify: `src/main.rs`
  - Add `mod order_gateway;`.
  - Replace `OrderSender/OrderReceiver` aliases with `OrderGatewayHandle` where strategies submit orders.
  - Spawn `OrderGateway` instead of `order::run` as the strategy order entry point.
  - Keep market data and position dispatcher unchanged.
- Modify: `src/strategy.rs`
  - Replace `OrderSignal` strategy output usage with `order_gateway::OrderGatewayHandle` in the `Strategy::spawn` signature.
  - Keep existing `OrderStatusEvent`/`OrderFillEvent` temporarily only where non-migrated tests need them, then remove order-status routing in a later task.
- Modify: `src/strategies/liquidity_reward_fsm.rs`
  - Convert current `OrderSignal::LiquidityRewardPlace`, `LiquidityRewardMarketSell`, `LiquidityRewardStageReplacement`, and `LiquidityRewardCancel` emissions into `OrderRequest::Place/Cancel` calls.
  - Convert market sell into `Place { side: Sell, order_type: Market, ... }`.
  - Convert replacement into explicit `Cancel` + `Place` requests.
- Modify: `src/strategies/pair_arbitrage.rs`
  - Remove the old `OrderSignal::PairArbitrage` strategy-to-order message. Current pair arbitrage only logs a simulated opportunity in `order.rs`, so preserve alert logging in the strategy and do not create a fake Gateway order.
- Modify: `src/strategies/market_maker.rs`
  - Update spawn signature to accept `OrderGatewayHandle`; no order submissions expected.
- Modify: `src/order.rs`
  - Extract reusable Polymarket execution helpers for place/cancel into functions used by `order_gateway`.
  - Remove direct strategy event output from order execution path once Gateway owns event output.
- Modify: `src/order_ws.rs`
  - Convert WS order/fill messages into `GatewayObservation` sent to Gateway instead of sending `StrategyEvent::OrderStatus/OrderFill`.
- Modify: `src/storage.rs`
  - Add gateway tables and typed persistence methods.
- Test: inline module tests in `src/order_gateway.rs`, `src/storage.rs`, `src/main.rs`, and strategy modules.

---

### Task 1: Add Order Gateway Core Request Types and Non-blocking Request Ring

**Files:**
- Create: `src/order_gateway.rs`
- Modify: `src/main.rs`
- Test: `src/order_gateway.rs`

- [ ] **Step 1: Write failing tests for request ring full and recovering errors**

Create `src/order_gateway.rs` with only the tests first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
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

        handle.try_send(place_request("order-1")).expect("first send fits");
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
}
```

- [ ] **Step 2: Register the module and run the failing tests**

Add this line near the top of `src/main.rs`:

```rust
mod order_gateway;
```

Run:

```powershell
cargo test order_gateway::tests::request_ring_full_returns_error_without_gateway_event; if ($?) { cargo test order_gateway::tests::gateway_recovering_rejects_new_requests }
```

Expected: FAIL with missing types such as `OrderRequest`, `OrderGatewayHandle`, and `GatewayPhase`.

- [ ] **Step 3: Implement minimal request types and request ring handle**

In `src/order_gateway.rs`, add:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use polymarket_client_sdk_v2::types::Decimal;
use tokio::sync::mpsc;

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
    Token { token_id: TokenId },
    Market { market_id: MarketId },
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
```

- [ ] **Step 4: Run the request ring tests**

Run:

```powershell
cargo test order_gateway::tests::request_ring_full_returns_error_without_gateway_event; if ($?) { cargo test order_gateway::tests::gateway_recovering_rejects_new_requests }
```

Expected: PASS.

- [ ] **Step 5: Stop for review**

Show:

```powershell
git diff -- src/order_gateway.rs src/main.rs
```

Do not proceed until reviewed if the user asks for a checkpoint.

---

### Task 2: Add Fixed-key Event Envelope and Broadcast-backed Event Ring

**Files:**
- Modify: `src/order_gateway.rs`
- Test: `src/order_gateway.rs`

- [ ] **Step 1: Write failing tests for event filtering and lag detection**

Append tests to `src/order_gateway.rs`:

```rust
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

        let event = subscriber.try_recv_relevant().expect("related event should arrive");
        assert_eq!(event.strategy_id, StrategyId::from("strategy-a"));
        assert_eq!(event.local_id, LocalOrderId::from("a-1"));
        assert_eq!(event.seq, 2);
    }

    #[test]
    fn event_subscriber_reports_lag_when_broadcast_overwrites() {
        let ring = OrderEventRing::new(1);
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("strategy-a"));

        ring.publish(accepted_event("strategy-a", "a-1", 1)).unwrap();
        ring.publish(accepted_event("strategy-a", "a-2", 2)).unwrap();

        let error = subscriber
            .try_recv_relevant()
            .expect_err("subscriber should observe lag");
        assert!(matches!(error, OrderEventPollError::Lagged { skipped } if skipped > 0));
    }
```

- [ ] **Step 2: Run event tests to verify they fail**

Run:

```powershell
cargo test order_gateway::tests::event_subscriber_filters_by_strategy_before_payload; if ($?) { cargo test order_gateway::tests::event_subscriber_reports_lag_when_broadcast_overwrites }
```

Expected: FAIL because `OrderEventRing`, `OrderEventEnvelope`, `OrderEventKind`, and `OrderEventPayload` are missing.

- [ ] **Step 3: Implement envelope, payload, and subscriber**

Add to `src/order_gateway.rs`:

```rust
use tokio::sync::broadcast;

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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    Accepted { exch_id: Option<ExchangeOrderId> },
    Open { exch_id: ExchangeOrderId },
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
    Cancelled { reason: CancelReason },
    Expired,
    LocalRejected { reason: LocalRejectReason },
    RemoteRejected {
        code: Option<Arc<str>>,
        message: Arc<str>,
        remote_status: Option<RemoteOrderState>,
    },
    Failed { kind: FailureKind },
    Stale { age_ms: u64 },
    Orphan { exch_id: ExchangeOrderId },
    Recovered { current_state: LocalOrderState },
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
                Err(broadcast::error::TryRecvError::Empty) => return Err(OrderEventPollError::Empty),
                Err(broadcast::error::TryRecvError::Closed) => return Err(OrderEventPollError::Closed),
                Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                    return Err(OrderEventPollError::Lagged { skipped });
                }
            }
        }
    }
}
```

- [ ] **Step 4: Run event ring tests**

Run:

```powershell
cargo test order_gateway::tests::event_subscriber_filters_by_strategy_before_payload; if ($?) { cargo test order_gateway::tests::event_subscriber_reports_lag_when_broadcast_overwrites }
```

Expected: PASS.

- [ ] **Step 5: Stop for review**

Show:

```powershell
git diff -- src/order_gateway.rs
```

---

### Task 3: Add Gateway Persistence Tables and Typed Store Methods

**Files:**
- Modify: `src/storage.rs`
- Test: `src/storage.rs`

- [ ] **Step 1: Write failing storage tests for schema and submission recovery materials**

Inside `#[cfg(test)] mod tests` in `src/storage.rs`, add:

```rust
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
```

- [ ] **Step 2: Run storage test to verify it fails**

Run:

```powershell
cargo test storage::tests::order_gateway_schema_persists_snapshot_event_and_submission
```

Expected: FAIL because gateway storage structs and methods are missing.

- [ ] **Step 3: Add structs and schema**

In `src/storage.rs`, near existing stored structs, add:

```rust
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
```

Extend `OrderStore::init_schema()` SQL with:

```sql
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
```

- [ ] **Step 4: Add store methods**

In `impl OrderStore`, add:

```rust
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
```

- [ ] **Step 5: Run storage tests**

Run:

```powershell
cargo test storage::tests::order_gateway_schema_persists_snapshot_event_and_submission
```

Expected: PASS.

- [ ] **Step 6: Stop for review**

Show:

```powershell
git diff -- src/storage.rs
```

---

### Task 4: Implement Reducer State for REST/WS/Timeout Observations

**Files:**
- Modify: `src/order_gateway.rs`
- Test: `src/order_gateway.rs`

- [ ] **Step 1: Write failing reducer tests**

Append tests:

```rust
    #[test]
    fn reducer_binds_ws_observation_that_arrives_before_rest_acceptance() {
        let mut state = GatewayState::new_for_test();
        let request = match place_request("local-1") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.accept_place_request(request).expect("request accepted");

        let early_events = state.reduce(GatewayObservation::WsFill {
            exch_id: Some(ExchangeOrderId::from("exch-1")),
            local_id: None,
            token_id: TokenId::from("token-1"),
            side: OrderSide::Buy,
            fill_delta: Decimal::try_from(3_f64).unwrap(),
            fill_price: Decimal::try_from(0.42_f64).unwrap(),
            trade_id: Arc::from("trade-1"),
        });
        assert_eq!(early_events.len(), 1);
        assert_eq!(early_events[0].kind, OrderEventKind::Orphan);

        let events = state.reduce(GatewayObservation::RestPlaceAccepted {
            local_id: LocalOrderId::from("local-1"),
            exch_id: ExchangeOrderId::from("exch-1"),
            remote_status: Some(RemoteOrderState {
                venue: Venue::Polymarket,
                status_code: Some(Arc::from("open")),
                status_text: None,
                reject_code: None,
                reject_reason: None,
                raw_event_type: Some(Arc::from("rest_place")),
            }),
        });

        assert!(events.iter().any(|event| event.kind == OrderEventKind::Open));
        assert!(events.iter().any(|event| event.kind == OrderEventKind::PartialFill));
    }

    #[test]
    fn reducer_timeout_marks_order_stale_not_rejected() {
        let mut state = GatewayState::new_for_test();
        let request = match place_request("local-timeout") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.accept_place_request(request).expect("request accepted");

        let events = state.reduce(GatewayObservation::Timeout {
            local_id: LocalOrderId::from("local-timeout"),
            operation: GatewayOperation::Place,
        });

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, OrderEventKind::Stale);
        assert!(matches!(
            state.order_state(&LocalOrderId::from("local-timeout")),
            Some(LocalOrderState::UnknownPending)
        ));
    }

    #[test]
    fn reducer_cancel_after_partial_fill_preserves_cumulative_fill() {
        let mut state = GatewayState::new_for_test();
        let request = match place_request("local-cancel") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        state.accept_place_request(request).expect("request accepted");
        state.reduce(GatewayObservation::RestPlaceAccepted {
            local_id: LocalOrderId::from("local-cancel"),
            exch_id: ExchangeOrderId::from("exch-cancel"),
            remote_status: None,
        });
        state.reduce(GatewayObservation::WsFill {
            exch_id: Some(ExchangeOrderId::from("exch-cancel")),
            local_id: None,
            token_id: TokenId::from("token-1"),
            side: OrderSide::Buy,
            fill_delta: Decimal::try_from(4_f64).unwrap(),
            fill_price: Decimal::try_from(0.42_f64).unwrap(),
            trade_id: Arc::from("trade-1"),
        });

        let events = state.reduce(GatewayObservation::RestCancelAccepted {
            local_id: LocalOrderId::from("local-cancel"),
            exch_id: Some(ExchangeOrderId::from("exch-cancel")),
            remote_status: None,
        });

        assert!(events.iter().any(|event| event.kind == OrderEventKind::Cancelled));
        let record = state.order_record(&LocalOrderId::from("local-cancel")).unwrap();
        assert_eq!(record.filled_size_total, Decimal::try_from(4_f64).unwrap());
        assert_eq!(record.local_state, LocalOrderState::Cancelled);
    }
```

- [ ] **Step 2: Run reducer tests to verify failure**

Run:

```powershell
cargo test order_gateway::tests::reducer_binds_ws_observation_that_arrives_before_rest_acceptance; if ($?) { cargo test order_gateway::tests::reducer_timeout_marks_order_stale_not_rejected }; if ($?) { cargo test order_gateway::tests::reducer_cancel_after_partial_fill_preserves_cumulative_fill }
```

Expected: FAIL because reducer types are missing.

- [ ] **Step 3: Add observation and record types**

In `src/order_gateway.rs`, add:

```rust
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteReject {
    pub code: Option<Arc<str>>,
    pub message: Arc<str>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GatewayObservation {
    RestPlaceAccepted {
        local_id: LocalOrderId,
        exch_id: ExchangeOrderId,
        remote_status: Option<RemoteOrderState>,
    },
    RestPlaceRejected {
        local_id: LocalOrderId,
        reason: RemoteReject,
    },
    RestCancelAccepted {
        local_id: LocalOrderId,
        exch_id: Option<ExchangeOrderId>,
        remote_status: Option<RemoteOrderState>,
    },
    RestCancelRejected {
        local_id: LocalOrderId,
        reason: RemoteReject,
    },
    WsOrderStatus {
        exch_id: ExchangeOrderId,
        status: RemoteOrderState,
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
    },
    RestQueryStatus {
        local_id: LocalOrderId,
        exch_id: Option<ExchangeOrderId>,
        status: RemoteOrderState,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderRecord {
    pub strategy_id: StrategyId,
    pub market_id: Option<MarketId>,
    pub token_id: TokenId,
    pub local_id: LocalOrderId,
    pub exch_id: Option<ExchangeOrderId>,
    pub side: OrderSide,
    pub order_type: GatewayOrderType,
    pub price: Option<Decimal>,
    pub size: Decimal,
    pub local_state: LocalOrderState,
    pub filled_size_total: Decimal,
    pub remaining_size: Decimal,
    pub avg_fill_price: Option<Decimal>,
}
```

- [ ] **Step 4: Add GatewayState reducer implementation**

Add:

```rust
pub struct GatewayState {
    records: HashMap<LocalOrderId, OrderRecord>,
    exch_to_local: HashMap<ExchangeOrderId, LocalOrderId>,
    pending_remote: HashMap<ExchangeOrderId, Vec<GatewayObservation>>,
    next_seq: u64,
    recovery: bool,
}

impl GatewayState {
    pub fn new_for_test() -> Self {
        Self {
            records: HashMap::new(),
            exch_to_local: HashMap::new(),
            pending_remote: HashMap::new(),
            next_seq: 1,
            recovery: false,
        }
    }

    pub fn accept_place_request(
        &mut self,
        request: PlaceOrderRequest,
    ) -> Result<OrderEventEnvelope, LocalRejectReason> {
        if self.records.contains_key(&request.local_id) {
            return Err(LocalRejectReason::DuplicateLocalId);
        }
        let record = OrderRecord {
            strategy_id: request.strategy_id.clone(),
            market_id: request.market_id.clone(),
            token_id: request.token_id.clone(),
            local_id: request.local_id.clone(),
            exch_id: None,
            side: request.side,
            order_type: request.order_type,
            price: request.price,
            size: request.size,
            local_state: LocalOrderState::Accepted,
            filled_size_total: Decimal::ZERO,
            remaining_size: request.size,
            avg_fill_price: None,
        };
        self.records.insert(request.local_id.clone(), record);
        Ok(self.event_for_record(
            &request.local_id,
            OrderEventKind::Accepted,
            OrderEventPayload::Accepted { exch_id: None },
        ))
    }

    pub fn reduce(&mut self, observation: GatewayObservation) -> Vec<OrderEventEnvelope> {
        match observation {
            GatewayObservation::RestPlaceAccepted { local_id, exch_id, remote_status } => {
                self.bind_exchange_id(&local_id, exch_id.clone());
                let mut events = vec![self.set_state_event(
                    &local_id,
                    LocalOrderState::Open,
                    OrderEventKind::Open,
                    OrderEventPayload::Open { exch_id: exch_id.clone() },
                )];
                if let Some(status) = remote_status {
                    events.extend(self.apply_remote_status(&local_id, status));
                }
                if let Some(pending) = self.pending_remote.remove(&exch_id) {
                    for pending_observation in pending {
                        events.extend(self.reduce(pending_observation));
                    }
                }
                events
            }
            GatewayObservation::RestPlaceRejected { local_id, reason } => vec![self.set_state_event(
                &local_id,
                LocalOrderState::Rejected,
                OrderEventKind::RemoteRejected,
                OrderEventPayload::RemoteRejected {
                    code: reason.code,
                    message: reason.message,
                    remote_status: None,
                },
            )],
            GatewayObservation::RestCancelAccepted { local_id, exch_id, remote_status } => {
                if let Some(exch_id) = exch_id {
                    self.bind_exchange_id(&local_id, exch_id);
                }
                let mut events = vec![self.set_state_event(
                    &local_id,
                    LocalOrderState::Cancelled,
                    OrderEventKind::Cancelled,
                    OrderEventPayload::Cancelled { reason: CancelReason::RemoteCancelled },
                )];
                if let Some(status) = remote_status {
                    events.extend(self.apply_remote_status(&local_id, status));
                }
                events
            }
            GatewayObservation::RestCancelRejected { local_id, reason } => vec![self.set_state_event(
                &local_id,
                LocalOrderState::CancelRejected,
                OrderEventKind::RemoteRejected,
                OrderEventPayload::RemoteRejected {
                    code: reason.code,
                    message: reason.message,
                    remote_status: None,
                },
            )],
            GatewayObservation::WsOrderStatus { exch_id, status } => {
                if let Some(local_id) = self.exch_to_local.get(&exch_id).cloned() {
                    self.apply_remote_status(&local_id, status)
                } else {
                    self.pending_remote
                        .entry(exch_id.clone())
                        .or_default()
                        .push(GatewayObservation::WsOrderStatus { exch_id: exch_id.clone(), status });
                    vec![self.system_event(OrderEventKind::Orphan, OrderEventPayload::Orphan { exch_id })]
                }
            }
            GatewayObservation::WsFill { exch_id, local_id, token_id, side, fill_delta, fill_price, trade_id: _ } => {
                let resolved_local = local_id.or_else(|| exch_id.as_ref().and_then(|id| self.exch_to_local.get(id).cloned()));
                let Some(local_id) = resolved_local else {
                    if let Some(exch_id) = exch_id {
                        self.pending_remote.entry(exch_id.clone()).or_default().push(GatewayObservation::WsFill {
                            exch_id: Some(exch_id.clone()),
                            local_id: None,
                            token_id,
                            side,
                            fill_delta,
                            fill_price,
                            trade_id: Arc::from("pending"),
                        });
                        return vec![self.system_event(OrderEventKind::Orphan, OrderEventPayload::Orphan { exch_id })];
                    }
                    return Vec::new();
                };
                self.apply_fill(&local_id, fill_delta, fill_price)
            }
            GatewayObservation::Timeout { local_id, operation: _ } => vec![self.set_state_event(
                &local_id,
                LocalOrderState::UnknownPending,
                OrderEventKind::Stale,
                OrderEventPayload::Stale { age_ms: 0 },
            )],
            GatewayObservation::RestQueryStatus { local_id, exch_id, status } => {
                if let Some(exch_id) = exch_id {
                    self.bind_exchange_id(&local_id, exch_id);
                }
                self.apply_remote_status(&local_id, status)
            }
        }
    }

    pub fn order_state(&self, local_id: &LocalOrderId) -> Option<LocalOrderState> {
        self.records.get(local_id).map(|record| record.local_state)
    }

    pub fn order_record(&self, local_id: &LocalOrderId) -> Option<&OrderRecord> {
        self.records.get(local_id)
    }

    fn bind_exchange_id(&mut self, local_id: &LocalOrderId, exch_id: ExchangeOrderId) {
        if let Some(record) = self.records.get_mut(local_id) {
            record.exch_id = Some(exch_id.clone());
            self.exch_to_local.insert(exch_id, local_id.clone());
        }
    }

    fn apply_remote_status(
        &mut self,
        local_id: &LocalOrderId,
        status: RemoteOrderState,
    ) -> Vec<OrderEventEnvelope> {
        match status.status_code.as_deref() {
            Some("filled") | Some("matched") => vec![self.set_state_event(
                local_id,
                LocalOrderState::Filled,
                OrderEventKind::Fill,
                OrderEventPayload::Fill {
                    fill_qty: Decimal::ZERO,
                    fill_price: Decimal::ZERO,
                    cum_qty: self.records.get(local_id).map(|r| r.filled_size_total).unwrap_or(Decimal::ZERO),
                    avg_fill_price: self.records.get(local_id).and_then(|r| r.avg_fill_price),
                },
            )],
            Some("canceled") | Some("cancelled") => vec![self.set_state_event(
                local_id,
                LocalOrderState::Cancelled,
                OrderEventKind::Cancelled,
                OrderEventPayload::Cancelled { reason: CancelReason::RemoteCancelled },
            )],
            Some("rejected") => vec![self.set_state_event(
                local_id,
                LocalOrderState::Rejected,
                OrderEventKind::RemoteRejected,
                OrderEventPayload::RemoteRejected {
                    code: status.reject_code.clone(),
                    message: status.reject_reason.clone().unwrap_or_else(|| Arc::from("remote rejected")),
                    remote_status: Some(status),
                },
            )],
            _ => Vec::new(),
        }
    }

    fn apply_fill(
        &mut self,
        local_id: &LocalOrderId,
        fill_delta: Decimal,
        fill_price: Decimal,
    ) -> Vec<OrderEventEnvelope> {
        if let Some(record) = self.records.get_mut(local_id) {
            record.filled_size_total += fill_delta;
            record.remaining_size = (record.size - record.filled_size_total).max(Decimal::ZERO);
            record.avg_fill_price = Some(fill_price);
            let is_full = record.remaining_size <= Decimal::ZERO;
            record.local_state = if is_full {
                LocalOrderState::Filled
            } else {
                LocalOrderState::PartiallyFilled
            };
            let payload = if is_full {
                OrderEventPayload::Fill {
                    fill_qty: fill_delta,
                    fill_price,
                    cum_qty: record.filled_size_total,
                    avg_fill_price: record.avg_fill_price,
                }
            } else {
                OrderEventPayload::PartialFill {
                    fill_qty: fill_delta,
                    fill_price,
                    cum_qty: record.filled_size_total,
                    avg_fill_price: record.avg_fill_price,
                }
            };
            let kind = if is_full { OrderEventKind::Fill } else { OrderEventKind::PartialFill };
            return vec![self.event_for_record(local_id, kind, payload)];
        }
        Vec::new()
    }

    fn set_state_event(
        &mut self,
        local_id: &LocalOrderId,
        state: LocalOrderState,
        kind: OrderEventKind,
        payload: OrderEventPayload,
    ) -> OrderEventEnvelope {
        if let Some(record) = self.records.get_mut(local_id) {
            record.local_state = state;
        }
        self.event_for_record(local_id, kind, payload)
    }

    fn event_for_record(
        &mut self,
        local_id: &LocalOrderId,
        kind: OrderEventKind,
        payload: OrderEventPayload,
    ) -> OrderEventEnvelope {
        let record = self.records.get(local_id).expect("record should exist");
        let seq = self.next_seq;
        self.next_seq += 1;
        OrderEventEnvelope {
            strategy_id: record.strategy_id.clone(),
            local_id: record.local_id.clone(),
            token_id: record.token_id.clone(),
            market_id: record.market_id.clone().unwrap_or_else(|| MarketId::from("")),
            seq,
            ts_ns: seq,
            recovery: self.recovery,
            kind,
            payload,
        }
    }

    fn system_event(&mut self, kind: OrderEventKind, payload: OrderEventPayload) -> OrderEventEnvelope {
        let seq = self.next_seq;
        self.next_seq += 1;
        OrderEventEnvelope {
            strategy_id: StrategyId::from("SYSTEM"),
            local_id: LocalOrderId::from(""),
            token_id: TokenId::from(""),
            market_id: MarketId::from(""),
            seq,
            ts_ns: seq,
            recovery: self.recovery,
            kind,
            payload,
        }
    }
}
```

- [ ] **Step 5: Run reducer tests**

Run:

```powershell
cargo test order_gateway::tests::reducer_binds_ws_observation_that_arrives_before_rest_acceptance; if ($?) { cargo test order_gateway::tests::reducer_timeout_marks_order_stale_not_rejected }; if ($?) { cargo test order_gateway::tests::reducer_cancel_after_partial_fill_preserves_cumulative_fill }
```

Expected: PASS.

- [ ] **Step 6: Stop for review**

Show:

```powershell
git diff -- src/order_gateway.rs
```

---

### Task 5: Add Gateway Runtime Shell, Risk Hook, and Recovery Completion Event

**Files:**
- Modify: `src/order_gateway.rs`
- Test: `src/order_gateway.rs`

- [ ] **Step 1: Write failing runtime tests**

Append tests:

```rust
    struct RejectAllRiskCheck;

    impl OrderRiskCheck for RejectAllRiskCheck {
        fn check_place(&self, _request: &PlaceOrderRequest, _state: &GatewayState) -> RiskDecision {
            RiskDecision::Reject {
                code: Arc::from("risk_rejected"),
                reason: Arc::from("test rejection"),
            }
        }

        fn check_cancel(&self, _request: &CancelOrderRequest, _state: &GatewayState) -> RiskDecision {
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
        let (gateway, handle, ring) = OrderGateway::new_for_test(config, Arc::new(RejectAllRiskCheck));
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("liquidity_reward"));
        let task = tokio::spawn(gateway.run_until_request_channel_closed());

        handle.set_phase(GatewayPhase::Live);
        handle.try_send(place_request("risk-1")).expect("send should enter gateway");
        drop(handle);
        task.await.expect("gateway task should join");

        let event = subscriber.try_recv_relevant().expect("rejection event should publish");
        assert_eq!(event.kind, OrderEventKind::LocalRejected);
        assert!(matches!(event.payload, OrderEventPayload::LocalRejected { .. }));
    }

    #[tokio::test]
    async fn gateway_recovery_completed_switches_handle_to_live() {
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (mut gateway, handle, ring) = OrderGateway::new_for_test(config, Arc::new(AllowAllRiskCheck));
        let mut system_subscriber = ring.subscribe_for_strategy(StrategyId::from("SYSTEM"));

        gateway.complete_recovery(2, 1, 0).expect("recovery completion should publish");

        let event = system_subscriber.try_recv_relevant().expect("system recovery event");
        assert_eq!(event.kind, OrderEventKind::RecoveryCompleted);
        assert!(!event.recovery);
        handle.try_send(place_request("live-after-recovery")).expect("live send should work");
    }
```

- [ ] **Step 2: Run runtime tests to verify failure**

Run:

```powershell
cargo test order_gateway::tests::gateway_risk_rejection_publishes_local_rejected_event; if ($?) { cargo test order_gateway::tests::gateway_recovery_completed_switches_handle_to_live }
```

Expected: FAIL because runtime shell and risk traits are missing.

- [ ] **Step 3: Add runtime shell and risk traits**

Add:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderGatewayConfig {
    pub simulation_enabled: bool,
    pub request_ring_capacity: usize,
    pub event_ring_capacity: usize,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskDecision {
    Allow,
    Reject { code: Arc<str>, reason: Arc<str> },
}

pub struct OrderGateway {
    rx: mpsc::Receiver<OrderRequest>,
    event_ring: OrderEventRing,
    handle: OrderGatewayHandle,
    state: GatewayState,
    risk: Arc<dyn OrderRiskCheck>,
    config: OrderGatewayConfig,
}

impl OrderGateway {
    pub fn new_for_test(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
    ) -> (Self, OrderGatewayHandle, OrderEventRing) {
        let (handle, rx) = OrderGatewayHandle::new_for_test(
            config.request_ring_capacity,
            GatewayPhase::Recovering,
        );
        let event_ring = OrderEventRing::new(config.event_ring_capacity);
        let gateway = Self {
            rx,
            event_ring: event_ring.clone(),
            handle: handle.clone(),
            state: GatewayState::new_for_test(),
            risk,
            config,
        };
        (gateway, handle, event_ring)
    }

    pub fn complete_recovery(
        &mut self,
        recovered_order_count: usize,
        unresolved_order_count: usize,
        failed_unrecoverable_count: usize,
    ) -> Result<(), OrderEventPublishError> {
        self.state.recovery = false;
        self.handle.set_phase(GatewayPhase::Live);
        let event = self.state.system_event(
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
        while let Some(request) = self.rx.recv().await {
            self.handle_request(request).await;
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
            RiskDecision::Allow => match self.state.accept_place_request(request.clone()) {
                Ok(event) => {
                    let _ = self.event_ring.publish(event);
                    if self.config.simulation_enabled {
                        for event in self.state.reduce(GatewayObservation::RestPlaceAccepted {
                            local_id: request.local_id,
                            exch_id: ExchangeOrderId::from(format!("sim-{}", request.token_id.as_str())),
                            remote_status: Some(RemoteOrderState {
                                venue: Venue::Simulation,
                                status_code: Some(Arc::from("open")),
                                status_text: None,
                                reject_code: None,
                                reject_reason: None,
                                raw_event_type: Some(Arc::from("simulation_place")),
                            }),
                        }) {
                            let _ = self.event_ring.publish(event);
                        }
                    }
                }
                Err(reason) => self.publish_local_rejected(request.strategy_id, request.local_id, request.token_id, request.market_id, reason),
            },
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
        if let RiskDecision::Reject { code, reason } = self.risk.check_cancel(&request, &self.state) {
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
        let seq = self.state.next_seq;
        self.state.next_seq += 1;
        let _ = self.event_ring.publish(OrderEventEnvelope {
            strategy_id,
            local_id,
            token_id,
            market_id: market_id.unwrap_or_else(|| MarketId::from("")),
            seq,
            ts_ns: seq,
            recovery: self.state.recovery,
            kind: OrderEventKind::LocalRejected,
            payload: OrderEventPayload::LocalRejected { reason },
        });
    }
}
```

- [ ] **Step 4: Run runtime tests**

Run:

```powershell
cargo test order_gateway::tests::gateway_risk_rejection_publishes_local_rejected_event; if ($?) { cargo test order_gateway::tests::gateway_recovery_completed_switches_handle_to_live }
```

Expected: PASS.

- [ ] **Step 5: Stop for review**

Show:

```powershell
git diff -- src/order_gateway.rs
```

---

### Task 6: Connect Gateway Persistence to Reducer Events

**Files:**
- Modify: `src/order_gateway.rs`
- Modify: `src/storage.rs`
- Test: `src/order_gateway.rs`

- [ ] **Step 1: Write failing test for event persistence and snapshot upsert**

Append test in `src/order_gateway.rs`:

```rust
    #[test]
    fn persisting_reducer_event_writes_gateway_snapshot_and_event_log() {
        let store = crate::storage::OrderStore::open(":memory:").expect("store should open");
        store.init_schema().expect("schema should initialize");
        let mut state = GatewayState::new_for_test();
        let request = match place_request("persist-1") {
            OrderRequest::Place(request) => request,
            _ => unreachable!(),
        };
        let accepted = state.accept_place_request(request).expect("accepted event");

        persist_gateway_event(&store, &state, &accepted).expect("event should persist");

        let active = store
            .load_order_gateway_recoverable_orders()
            .expect("recoverable orders should load");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].local_id, "persist-1");
        assert_eq!(active[0].local_state, "Accepted");
        assert_eq!(active[0].last_event_seq, accepted.seq);
    }
```

- [ ] **Step 2: Run persistence integration test to verify failure**

Run:

```powershell
cargo test order_gateway::tests::persisting_reducer_event_writes_gateway_snapshot_and_event_log
```

Expected: FAIL because `persist_gateway_event` is missing.

- [ ] **Step 3: Implement event-to-storage mapping**

In `src/order_gateway.rs`, add:

```rust
use crate::storage::{OrderGatewayEventInsert, OrderGatewayOrderSnapshot, OrderStore};

pub fn persist_gateway_event(
    store: &OrderStore,
    state: &GatewayState,
    event: &OrderEventEnvelope,
) -> anyhow::Result<()> {
    if let Some(record) = state.order_record(&event.local_id) {
        store.upsert_order_gateway_order(&OrderGatewayOrderSnapshot {
            strategy_id: record.strategy_id.as_str().to_string(),
            market_id: record.market_id.as_ref().map(|value| value.as_str().to_string()),
            token_id: record.token_id.as_str().to_string(),
            local_id: record.local_id.as_str().to_string(),
            exch_id: record.exch_id.as_ref().map(|value| value.as_str().to_string()),
            side: format!("{:?}", record.side),
            order_type: order_type_label(&record.order_type).to_string(),
            price: record.price.map(|value| value.to_string()),
            size: record.size.to_string(),
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
        fill_delta: event_fill_delta(event).as_deref(),
        fill_total: event_fill_total(event).as_deref(),
        remaining_size: None,
        avg_fill_price: None,
        error_code: None,
        error_message: None,
        raw_json: "{}",
        recovery: event.recovery,
    })
}

fn order_type_label(order_type: &GatewayOrderType) -> &'static str {
    match order_type {
        GatewayOrderType::Limit { time_in_force: TimeInForce::Gtc } => "LimitGtc",
        GatewayOrderType::Limit { time_in_force: TimeInForce::Gtd { .. } } => "LimitGtd",
        GatewayOrderType::Limit { time_in_force: TimeInForce::Ioc } => "LimitIoc",
        GatewayOrderType::Limit { time_in_force: TimeInForce::Fok } => "LimitFok",
        GatewayOrderType::Market => "Market",
    }
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
        OrderEventPayload::PartialFill { fill_qty, .. } | OrderEventPayload::Fill { fill_qty, .. } => {
            Some(fill_qty.to_string())
        }
        _ => None,
    }
}

fn event_fill_total(event: &OrderEventEnvelope) -> Option<String> {
    match &event.payload {
        OrderEventPayload::PartialFill { cum_qty, .. } | OrderEventPayload::Fill { cum_qty, .. } => {
            Some(cum_qty.to_string())
        }
        _ => None,
    }
}
```

- [ ] **Step 4: Call persistence from Gateway runtime**

Add `order_store: Option<OrderStore>` to `OrderGateway`, set it to `None` in `new_for_test`, and create a helper:

```rust
    fn publish_and_persist(&mut self, event: OrderEventEnvelope) {
        if let Some(store) = &self.order_store {
            let _ = persist_gateway_event(store, &self.state, &event);
        }
        let _ = self.event_ring.publish(event);
    }
```

Replace direct `self.event_ring.publish(event)` calls in runtime methods with `self.publish_and_persist(event)`.

- [ ] **Step 5: Run persistence test**

Run:

```powershell
cargo test order_gateway::tests::persisting_reducer_event_writes_gateway_snapshot_and_event_log
```

Expected: PASS.

- [ ] **Step 6: Stop for review**

Show:

```powershell
git diff -- src/order_gateway.rs src/storage.rs
```

---

### Task 7: Add Submission Material Persistence API for Signed Orders

**Files:**
- Modify: `src/order_gateway.rs`
- Modify: `src/order.rs`
- Test: `src/order_gateway.rs`

- [ ] **Step 1: Write failing test for missing signed payload recovery failure**

Append test:

```rust
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

        let mut state = GatewayState::new_for_test();
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
```

- [ ] **Step 2: Run recovery test to verify failure**

Run:

```powershell
cargo test order_gateway::tests::recovery_marks_order_without_signed_payload_unrecoverable
```

Expected: FAIL because recovery helper and submission lookup are missing.

- [ ] **Step 3: Add submission lookup to storage**

In `src/storage.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredOrderGatewaySubmission {
    pub local_id: String,
    pub submit_attempt: i64,
    pub signed_payload_json: String,
    pub exchange_payload_hash: String,
    pub submit_state: String,
}
```

Add method:

```rust
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
```

`OptionalExtension` is already imported in `storage.rs`.

- [ ] **Step 4: Add recovery helper**

In `src/order_gateway.rs`, add:

```rust
pub fn recover_gateway_orders_for_test(
    store: &OrderStore,
    state: &mut GatewayState,
) -> anyhow::Result<Vec<OrderEventEnvelope>> {
    state.recovery = true;
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
                market_id: snapshot.market_id.map(MarketId::from),
                token_id: TokenId::from(snapshot.token_id),
                local_id: local_id.clone(),
                exch_id: None,
                side: if snapshot.side == "Sell" { OrderSide::Sell } else { OrderSide::Buy },
                order_type: GatewayOrderType::Limit { time_in_force: TimeInForce::Gtc },
                price: snapshot.price.and_then(|value| value.parse::<f64>().ok()).and_then(|value| Decimal::try_from(value).ok()),
                size: snapshot.size.parse::<f64>().ok().and_then(|value| Decimal::try_from(value).ok()).unwrap_or(Decimal::ZERO),
                local_state: LocalOrderState::Failed,
                filled_size_total: Decimal::ZERO,
                remaining_size: Decimal::ZERO,
                avg_fill_price: None,
            };
            state.records.insert(local_id.clone(), record);
            events.push(state.event_for_record(
                &local_id,
                OrderEventKind::Failed,
                OrderEventPayload::Failed {
                    kind: FailureKind::MissingSignedPayloadAfterRestart,
                },
            ));
        }
    }
    Ok(events)
}
```

- [ ] **Step 5: Run recovery test**

Run:

```powershell
cargo test order_gateway::tests::recovery_marks_order_without_signed_payload_unrecoverable
```

Expected: PASS.

- [ ] **Step 6: Document signed payload extraction boundary in code**

In `src/order.rs`, add a small public data type near the Polymarket place helpers:

```rust
pub struct SignedOrderSubmissionMaterial {
    pub unsigned_payload_json: String,
    pub signed_payload_json: String,
    pub signature: String,
    pub signer_address: String,
    pub nonce_or_salt: Option<String>,
    pub expiration: Option<i64>,
    pub exchange_payload_hash: String,
    pub rest_request_json: String,
}
```

Do not log any of these fields.

- [ ] **Step 7: Stop for review**

Show:

```powershell
git diff -- src/order_gateway.rs src/storage.rs src/order.rs
```

---

### Task 8: Convert Gateway Runtime to Use Existing Simulation Path

**Files:**
- Modify: `src/order_gateway.rs`
- Modify: `src/main.rs`
- Test: `src/order_gateway.rs`

- [ ] **Step 1: Write failing simulation lifecycle test**

Append test:

```rust
    #[tokio::test]
    async fn simulated_gateway_place_emits_accepted_and_open() {
        let config = OrderGatewayConfig {
            simulation_enabled: true,
            request_ring_capacity: 8,
            event_ring_capacity: 8,
        };
        let (gateway, handle, ring) = OrderGateway::new_for_test(config, Arc::new(AllowAllRiskCheck));
        let mut subscriber = ring.subscribe_for_strategy(StrategyId::from("liquidity_reward"));
        let task = tokio::spawn(gateway.run_until_request_channel_closed());

        handle.set_phase(GatewayPhase::Live);
        handle.try_send(place_request("sim-place-1")).expect("send should work");
        drop(handle);
        task.await.expect("gateway task should finish");

        let first = subscriber.try_recv_relevant().expect("accepted event");
        let second = subscriber.try_recv_relevant().expect("open event");
        assert_eq!(first.kind, OrderEventKind::Accepted);
        assert_eq!(second.kind, OrderEventKind::Open);
        assert!(matches!(second.payload, OrderEventPayload::Open { .. }));
    }
```

- [ ] **Step 2: Run simulation lifecycle test**

Run:

```powershell
cargo test order_gateway::tests::simulated_gateway_place_emits_accepted_and_open
```

Expected: PASS because Task 5 already implemented the simulation acceptance/open path.

- [ ] **Step 3: Add production constructor for gateway rings**

In `src/order_gateway.rs`, add:

```rust
impl OrderGateway {
    pub fn new(
        config: OrderGatewayConfig,
        risk: Arc<dyn OrderRiskCheck>,
        order_store: OrderStore,
    ) -> (Self, OrderGatewayHandle, OrderEventRing) {
        let (handle, rx) = OrderGatewayHandle::new_for_test(
            config.request_ring_capacity,
            GatewayPhase::Recovering,
        );
        let event_ring = OrderEventRing::new(config.event_ring_capacity);
        let gateway = Self {
            rx,
            event_ring: event_ring.clone(),
            handle: handle.clone(),
            state: GatewayState::new_for_test(),
            risk,
            config,
            order_store: Some(order_store),
        };
        (gateway, handle, event_ring)
    }
}
```

- [ ] **Step 4: Wire Gateway creation in `main.rs` without changing strategies yet**

In `src/main.rs`, add imports:

```rust
use order_gateway::{AllowAllRiskCheck, OrderGateway, OrderGatewayConfig};
```

Create the Gateway before strategy spawn:

```rust
let gateway_config = OrderGatewayConfig {
    simulation_enabled: app_config.simulation.enabled,
    request_ring_capacity: 1024,
    event_ring_capacity: 16384,
};
let (mut order_gateway, order_gateway_handle, order_event_ring) = OrderGateway::new(
    gateway_config,
    Arc::new(AllowAllRiskCheck),
    order_store.clone(),
);
order_gateway
    .complete_recovery(0, 0, 0)
    .expect("order gateway recovery completion should publish");
tokio::spawn(order_gateway.run_until_request_channel_closed());
```

Keep old `order_tx/order_rx` path temporarily in this task so the code still compiles before strategy migration.

- [ ] **Step 5: Run compile-focused test**

Run:

```powershell
cargo test order_gateway::tests::simulated_gateway_place_emits_accepted_and_open
```

Expected: PASS and compile succeeds.

- [ ] **Step 6: Stop for review**

Show:

```powershell
git diff -- src/order_gateway.rs src/main.rs
```

---

### Task 9: Migrate Strategy Trait and Market Maker to Gateway Handle

**Files:**
- Modify: `src/strategy.rs`
- Modify: `src/strategies/market_maker.rs`
- Modify: `src/main.rs`
- Test: `src/strategies/market_maker.rs`

- [ ] **Step 1: Write failing compile-oriented test for market maker spawn signature**

Update `src/strategies/market_maker.rs` test `spawn_consumes_events_without_emitting_order_signals` to create a gateway handle instead of `order_tx`:

```rust
let (gateway_handle, _gateway_rx) = crate::order_gateway::OrderGatewayHandle::new_for_test(
    8,
    crate::order_gateway::GatewayPhase::Live,
);

let handle = strategy.spawn(event_rx, gateway_handle);
```

Remove `order_tx/order_rx` assertions from this test and keep only:

```rust
handle
    .await
    .expect("market maker task should exit when event channel closes");
```

- [ ] **Step 2: Run test to verify failure**

Run:

```powershell
cargo test strategies::market_maker::tests::spawn_consumes_events_without_emitting_order_signals
```

Expected: FAIL because `Strategy::spawn` still expects `Sender<OrderSignal>`.

- [ ] **Step 3: Change Strategy trait signature**

In `src/strategy.rs`, import the handle:

```rust
use crate::order_gateway::OrderGatewayHandle;
```

Change the trait method from:

```rust
fn spawn(
    self,
    rx: tokio::sync::mpsc::Receiver<StrategyEvent>,
    order_tx: tokio::sync::mpsc::Sender<OrderSignal>,
) -> tokio::task::JoinHandle<()>;
```

to:

```rust
fn spawn(
    self,
    rx: tokio::sync::mpsc::Receiver<StrategyEvent>,
    order_gateway: OrderGatewayHandle,
) -> tokio::task::JoinHandle<()>;
```

- [ ] **Step 4: Update market maker implementation**

In `src/strategies/market_maker.rs`, change:

```rust
_order_tx: tokio::sync::mpsc::Sender<OrderSignal>,
```

to:

```rust
_order_gateway: crate::order_gateway::OrderGatewayHandle,
```

Remove `OrderSignal` import if it is no longer used.

- [ ] **Step 5: Update main strategy spawn calls for market maker only**

In `src/main.rs`, update `spawn_strategy_tasks` to accept both old `order_tx` and new `order_gateway_handle` temporarily:

```rust
fn spawn_strategy_tasks(
    pair_strategy: PairArbitrageStrategy,
    pair_registration: StrategyRegistration,
    liquidity_reward: Option<LiquidityRewardStrategy>,
    market_maker: Option<MarketMakerStrategy>,
    order_tx: OrderSender,
    order_gateway_handle: order_gateway::OrderGatewayHandle,
    strategy_rx: StrategyReceiver,
) {
```

Use `order_gateway_handle.clone()` for market maker:

```rust
market_maker_strategy.spawn(market_maker_rx, order_gateway_handle.clone());
```

Keep pair/liquidity reward temporarily on the old path by updating them in the next task.

- [ ] **Step 6: Run market maker test**

Run:

```powershell
cargo test strategies::market_maker::tests::spawn_consumes_events_without_emitting_order_signals
```

Expected: PASS after all trait implementors are updated enough to compile. If pair/liquidity reward fail compile due trait mismatch, apply the same signature change there but keep sending logic via temporary adapter in Task 10.

- [ ] **Step 7: Stop for review**

Show:

```powershell
git diff -- src/strategy.rs src/strategies/market_maker.rs src/main.rs
```

---

### Task 10: Migrate Liquidity Reward Order Submissions to Place/Cancel Requests

**Files:**
- Modify: `src/strategies/liquidity_reward_fsm.rs`
- Modify: `src/strategy.rs`
- Test: `src/strategies/liquidity_reward_fsm.rs`

- [ ] **Step 1: Write failing tests for request conversion helpers**

In `src/strategies/liquidity_reward_fsm.rs`, add tests for helper functions near existing tests:

```rust
    #[test]
    fn liquidity_reward_place_request_uses_limit_order_fields() {
        let request = liquidity_reward_place_request(
            Arc::from("liquidity_reward"),
            Arc::from("liquidity_reward"),
            "token-1".to_string(),
            QuoteSide::Buy,
            Decimal::try_from(0.42_f64).unwrap(),
            Decimal::try_from(10_f64).unwrap(),
            "local-1".to_string(),
            "quote".into(),
        );

        let crate::order_gateway::OrderRequest::Place(place) = request else {
            panic!("expected place request");
        };
        assert_eq!(place.strategy_id.as_str(), "liquidity_reward");
        assert_eq!(place.token_id.as_str(), "token-1");
        assert_eq!(place.local_id.as_str(), "local-1");
        assert_eq!(place.side, crate::order_gateway::OrderSide::Buy);
        assert!(matches!(
            place.order_type,
            crate::order_gateway::GatewayOrderType::Limit { .. }
        ));
    }

    #[test]
    fn liquidity_reward_cancel_request_targets_local_order_id() {
        let request = liquidity_reward_cancel_request(
            Arc::from("liquidity_reward"),
            "token-1".to_string(),
            "local-1".to_string(),
            "replace".into(),
        );

        let crate::order_gateway::OrderRequest::Cancel(cancel) = request else {
            panic!("expected cancel request");
        };
        assert_eq!(cancel.strategy_id.as_str(), "liquidity_reward");
        assert!(matches!(
            cancel.scope,
            crate::order_gateway::CancelScope::LocalOrderId { .. }
        ));
    }
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```powershell
cargo test liquidity_reward_fsm::tests::liquidity_reward_place_request_uses_limit_order_fields; if ($?) { cargo test liquidity_reward_fsm::tests::liquidity_reward_cancel_request_targets_local_order_id }
```

Expected: FAIL because helper functions are missing.

- [ ] **Step 3: Add conversion helpers**

In `src/strategies/liquidity_reward_fsm.rs`, add near order emission helpers:

```rust
fn gateway_side(side: QuoteSide) -> crate::order_gateway::OrderSide {
    match side {
        QuoteSide::Buy => crate::order_gateway::OrderSide::Buy,
        QuoteSide::Sell => crate::order_gateway::OrderSide::Sell,
    }
}

fn liquidity_reward_place_request(
    strategy: Arc<str>,
    topic: Arc<str>,
    token: String,
    side: QuoteSide,
    price: Decimal,
    order_size: Decimal,
    local_order_id: String,
    reason: Arc<str>,
) -> crate::order_gateway::OrderRequest {
    crate::order_gateway::OrderRequest::Place(crate::order_gateway::PlaceOrderRequest {
        strategy_id: crate::order_gateway::StrategyId(strategy),
        market_id: Some(crate::order_gateway::MarketId(topic)),
        token_id: crate::order_gateway::TokenId::from(token),
        local_id: crate::order_gateway::LocalOrderId::from(local_order_id),
        side: gateway_side(side),
        order_type: crate::order_gateway::GatewayOrderType::Limit {
            time_in_force: crate::order_gateway::TimeInForce::Gtc,
        },
        price: Some(price),
        size: order_size,
        reason: Some(reason),
    })
}

fn liquidity_reward_market_sell_request(
    strategy: Arc<str>,
    topic: Arc<str>,
    token: String,
    price: Decimal,
    order_size: Decimal,
    local_order_id: String,
    reason: Arc<str>,
) -> crate::order_gateway::OrderRequest {
    crate::order_gateway::OrderRequest::Place(crate::order_gateway::PlaceOrderRequest {
        strategy_id: crate::order_gateway::StrategyId(strategy),
        market_id: Some(crate::order_gateway::MarketId(topic)),
        token_id: crate::order_gateway::TokenId::from(token),
        local_id: crate::order_gateway::LocalOrderId::from(local_order_id),
        side: crate::order_gateway::OrderSide::Sell,
        order_type: crate::order_gateway::GatewayOrderType::Market,
        price: Some(price),
        size: order_size,
        reason: Some(reason),
    })
}

fn liquidity_reward_cancel_request(
    strategy: Arc<str>,
    token: String,
    active_local_order_id: String,
    reason: Arc<str>,
) -> crate::order_gateway::OrderRequest {
    crate::order_gateway::OrderRequest::Cancel(crate::order_gateway::CancelOrderRequest {
        strategy_id: crate::order_gateway::StrategyId(strategy),
        scope: crate::order_gateway::CancelScope::LocalOrderId {
            local_id: crate::order_gateway::LocalOrderId::from(active_local_order_id),
            exch_id: None,
            token_id: Some(crate::order_gateway::TokenId::from(token)),
        },
        reason: Some(reason),
    })
}
```

- [ ] **Step 4: Replace `order_tx.try_send(OrderSignal::...)` calls**

In `LiquidityRewardFsmStrategy::spawn`, rename the parameter to `order_gateway` and replace each `order_tx.try_send(...)` with `order_gateway.try_send(...)` using the helpers above.

For `LiquidityRewardStageReplacement`, emit:

```rust
if request_cancel {
    let _ = order_gateway.try_send(liquidity_reward_cancel_request(
        strategy.clone(),
        token.clone(),
        active_local_order_id.clone(),
        Arc::from("stage_replacement_cancel"),
    ));
}
let _ = order_gateway.try_send(liquidity_reward_place_request(
    strategy.clone(),
    topic.clone(),
    token.clone(),
    side,
    price,
    order_size,
    pending_local_order_id.clone(),
    Arc::from("stage_replacement_place"),
));
```

- [ ] **Step 5: Run liquidity reward conversion tests**

Run:

```powershell
cargo test liquidity_reward_fsm::tests::liquidity_reward_place_request_uses_limit_order_fields; if ($?) { cargo test liquidity_reward_fsm::tests::liquidity_reward_cancel_request_targets_local_order_id }
```

Expected: PASS.

- [ ] **Step 6: Stop for review**

Show:

```powershell
git diff -- src/strategies/liquidity_reward_fsm.rs src/strategy.rs
```

---

### Task 11: Migrate Pair Arbitrage and Remove Old OrderSignal Strategy Output

**Files:**
- Modify: `src/strategies/pair_arbitrage.rs`
- Modify: `src/strategy.rs`
- Modify: `src/main.rs`
- Test: `src/strategies/pair_arbitrage.rs`

- [ ] **Step 1: Write failing test for pair arbitrage alert-only behavior**

Append tests to `src/strategies/pair_arbitrage.rs`:

```rust
#[cfg(test)]
mod gateway_migration_tests {
    use super::*;
    use crate::strategy::PairEntry;

    fn filters() -> Filters {
        Filters {
            min_diff: Decimal::try_from(0.01_f64).unwrap(),
            max_spread: Decimal::try_from(0.10_f64).unwrap(),
            min_price: Decimal::try_from(0.01_f64).unwrap(),
            max_price: Decimal::try_from(0.99_f64).unwrap(),
        }
    }

    #[test]
    fn pair_arbitrage_keeps_alert_only_without_gateway_order() {
        let store = PriceStore::new();
        store.register(&["token-0".to_string(), "token-1".to_string()]);
        store.apply(
            "token-0",
            CleanOrderbook {
                best_bid_price: 3900,
                best_bid_size: 100,
                best_ask_price: 4000,
                best_ask_size: 100,
                timestamp_ms: 1,
                bids: Arc::new(std::collections::BTreeMap::new()),
                asks: Arc::new(std::collections::BTreeMap::new()),
            },
        );
        store.apply(
            "token-1",
            CleanOrderbook {
                best_bid_price: 3900,
                best_bid_size: 100,
                best_ask_price: 4000,
                best_ask_size: 100,
                timestamp_ms: 2,
                bids: Arc::new(std::collections::BTreeMap::new()),
                asks: Arc::new(std::collections::BTreeMap::new()),
            },
        );
        let mut pairs = HashMap::new();
        pairs.insert(
            Arc::from("topic"),
            Arc::<[PairEntry]>::from([PairEntry {
                tokens: ["token-0".to_string(), "token-1".to_string()],
                topic: Arc::from("topic"),
            }]),
        );
        let (gateway, mut gateway_rx) = crate::order_gateway::OrderGatewayHandle::new_for_test(
            8,
            crate::order_gateway::GatewayPhase::Live,
        );

        check_pairs(
            &store,
            &pairs,
            &filters(),
            &Arc::from("topic"),
            &["token-0".to_string()],
            &gateway,
        );

        assert!(gateway_rx.try_recv().is_err());
    }
}
```

- [ ] **Step 2: Run pair arbitrage test to verify failure**

Run:

```powershell
cargo test strategies::pair_arbitrage::gateway_migration_tests::pair_arbitrage_keeps_alert_only_without_gateway_order
```

Expected: FAIL because `check_pairs` still takes an old `OrderSignal` sender.

- [ ] **Step 3: Convert pair arbitrage spawn signature and helper parameter**

In `src/strategies/pair_arbitrage.rs`, update `spawn` to accept:

```rust
_order_gateway: crate::order_gateway::OrderGatewayHandle,
```

Change `check_pairs` to accept the Gateway handle:

```rust
fn check_pairs(
    store: &PriceStore,
    pairs_by_topic: &HashMap<Arc<str>, Arc<[PairEntry]>>,
    filters: &Filters,
    topic: &Arc<str>,
    updated_assets: &[String],
    _order_gateway: &crate::order_gateway::OrderGatewayHandle,
) {
```

Delete the old `order_tx.try_send(OrderSignal::PairArbitrage { ... })` block. Keep the existing alert log, because the old executor only simulated pair arbitrage by logging and did not place real orders.

- [ ] **Step 4: Remove `OrderSignal` and `UnifiedOrder` from strategy API**

In `src/strategy.rs`, delete `OrderSignal`, `UnifiedOrder`, and `impl From<OrderSignal> for UnifiedOrder` after liquidity reward and pair arbitrage no longer use them.

- [ ] **Step 5: Update `main.rs` strategy spawning**

Change `spawn_strategy_tasks` signature to:

```rust
fn spawn_strategy_tasks(
    pair_strategy: PairArbitrageStrategy,
    pair_registration: StrategyRegistration,
    liquidity_reward: Option<LiquidityRewardStrategy>,
    market_maker: Option<MarketMakerStrategy>,
    order_gateway_handle: order_gateway::OrderGatewayHandle,
    strategy_rx: StrategyReceiver,
) {
```

Use `order_gateway_handle.clone()` for every strategy `spawn` call.

Remove old `order_tx/order_rx` creation from `main.rs`:

```rust
let (order_tx, order_rx) = tokio::sync::mpsc::channel::<OrderSignal>(64);
```

Remove the old `spawn_order_tasks(... order_rx ...)` call after Gateway runtime is wired.

- [ ] **Step 6: Run pair arbitrage migration test and compile check**

Run:

```powershell
cargo test strategies::pair_arbitrage::gateway_migration_tests::pair_arbitrage_keeps_alert_only_without_gateway_order; if ($?) { cargo test --no-run }
```

Expected: PASS.

- [ ] **Step 7: Stop for review**

Show:

```powershell
git diff -- src/main.rs src/strategy.rs src/strategies/pair_arbitrage.rs src/strategies/liquidity_reward_fsm.rs src/strategies/market_maker.rs
```

---

### Task 12: Route Order WS Messages into Gateway Observations

**Files:**
- Modify: `src/order_ws.rs`
- Modify: `src/order_gateway.rs`
- Modify: `src/main.rs`
- Test: `src/order_ws.rs` or `src/order_gateway.rs`

- [ ] **Step 1: Write failing helper test for WS fill conversion**

In `src/order_ws.rs`, add a pure helper test around `fill_delta` behavior:

```rust
#[cfg(test)]
mod gateway_observation_tests {
    use super::*;

    #[test]
    fn ws_fill_delta_maps_to_gateway_observation() {
        let observation = gateway_fill_observation(
            Some("exch-1".to_string()),
            None,
            "token-1".to_string(),
            QuoteSide::Buy,
            Decimal::try_from(2_f64).unwrap(),
            Decimal::try_from(0.42_f64).unwrap(),
            "trade-1".to_string(),
        );

        assert!(matches!(
            observation,
            crate::order_gateway::GatewayObservation::WsFill { .. }
        ));
    }
}
```

- [ ] **Step 2: Run test to verify failure**

Run:

```powershell
cargo test order_ws::gateway_observation_tests::ws_fill_delta_maps_to_gateway_observation
```

Expected: FAIL because `gateway_fill_observation` is missing.

- [ ] **Step 3: Add helper and observation sender**

In `src/order_ws.rs`, add:

```rust
fn gateway_fill_observation(
    exch_id: Option<String>,
    local_id: Option<String>,
    token_id: String,
    side: QuoteSide,
    fill_delta: Decimal,
    fill_price: Decimal,
    trade_id: String,
) -> crate::order_gateway::GatewayObservation {
    crate::order_gateway::GatewayObservation::WsFill {
        exch_id: exch_id.map(crate::order_gateway::ExchangeOrderId::from),
        local_id: local_id.map(crate::order_gateway::LocalOrderId::from),
        token_id: crate::order_gateway::TokenId::from(token_id),
        side: match side {
            QuoteSide::Buy => crate::order_gateway::OrderSide::Buy,
            QuoteSide::Sell => crate::order_gateway::OrderSide::Sell,
        },
        fill_delta,
        fill_price,
        trade_id: Arc::from(trade_id),
    }
}
```

Add an observation channel parameter to `order_ws::run`:

```rust
observation_tx: tokio::sync::mpsc::Sender<crate::order_gateway::GatewayObservation>,
```

When current code sends `StrategyEvent::OrderFill`, also or instead send `GatewayObservation::WsFill` to `observation_tx`. Since the spec says direct cut, remove strategy order/fill sends after gateway subscriber tests are in place.

- [ ] **Step 4: Add observation receiver to Gateway runtime**

In `src/order_gateway.rs`, add an `observation_rx` field and run loop branch:

```rust
pub async fn run(mut self) {
    loop {
        tokio::select! {
            request = self.rx.recv() => {
                match request {
                    Some(request) => self.handle_request(request).await,
                    None => break,
                }
            }
            observation = self.observation_rx.recv() => {
                match observation {
                    Some(observation) => {
                        let events = self.state.reduce(observation);
                        for event in events {
                            self.publish_and_persist(event);
                        }
                    }
                    None => {}
                }
            }
        }
    }
}
```

- [ ] **Step 5: Wire `observation_tx` in `main.rs`**

When constructing Gateway, return/store `observation_tx` and pass it to `order_ws::run`.

- [ ] **Step 6: Run WS helper test**

Run:

```powershell
cargo test order_ws::gateway_observation_tests::ws_fill_delta_maps_to_gateway_observation
```

Expected: PASS.

- [ ] **Step 7: Stop for review**

Show:

```powershell
git diff -- src/order_ws.rs src/order_gateway.rs src/main.rs
```

---

### Task 13: Remove Dispatcher OrderStatus/OrderFill Routing After Gateway Migration

**Files:**
- Modify: `src/dispatcher.rs`
- Modify: `src/strategy.rs`
- Test: `src/dispatcher.rs`

- [ ] **Step 1: Write failing dispatcher tests to prove market and position routing remain**

Append this test module to `src/dispatcher.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap};

    use polymarket_client_sdk_v2::types::Decimal;

    use crate::strategy::{
        CleanOrderbook, MarketEvent, PositionSnapshot, PositionView, PositionsUpdateEvent,
    };

    fn test_market_event(topic: &str, token: &str) -> StrategyEvent {
        StrategyEvent::Market(MarketEvent {
            topic: Arc::from(topic),
            asset_id: Arc::from(token),
            book: CleanOrderbook {
                best_bid_price: 4000,
                best_bid_size: 100,
                best_ask_price: 4100,
                best_ask_size: 100,
                timestamp_ms: 1,
                bids: Arc::new(BTreeMap::new()),
                asks: Arc::new(BTreeMap::new()),
            },
        })
    }

    fn test_positions_event(token: &str) -> StrategyEvent {
        let mut by_asset = HashMap::new();
        by_asset.insert(
            token.to_string(),
            PositionView {
                asset_id: token.to_string(),
                size: Decimal::ONE,
                avg_price: Decimal::ONE,
                cur_price: Decimal::ONE,
                current_value: Decimal::ONE,
                cash_pnl: Decimal::ZERO,
                title: Arc::from("title"),
                outcome: Arc::from("outcome"),
            },
        );
        StrategyEvent::Positions(PositionsUpdateEvent {
            snapshot: Arc::new(PositionSnapshot {
                by_asset: Arc::new(by_asset),
            }),
            changed_assets: Arc::from([token.to_string()]),
        })
    }

    #[tokio::test]
    async fn dispatcher_keeps_market_routing_without_order_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let strategy = StrategyHandle {
            name: Arc::from("test"),
            topics: Arc::from([Arc::from("topic")]),
            related_tokens: Arc::from(["token-1".to_string()]),
            tx,
        };
        let dispatcher = Dispatcher::new(vec![strategy]);
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(4);
        let task = tokio::spawn(dispatcher.run(input_rx));

        input_tx.send(test_market_event("topic", "token-1")).await.unwrap();
        drop(input_tx);

        let event = rx.recv().await.expect("market event routed");
        assert!(matches!(event, StrategyEvent::Market(_)));
        task.await.unwrap();
    }

    #[tokio::test]
    async fn dispatcher_keeps_position_routing_without_order_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let strategy = StrategyHandle {
            name: Arc::from("test"),
            topics: Arc::from([Arc::from("topic")]),
            related_tokens: Arc::from(["token-1".to_string()]),
            tx,
        };
        let dispatcher = Dispatcher::new(vec![strategy]);
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(4);
        let task = tokio::spawn(dispatcher.run(input_rx));

        input_tx.send(test_positions_event("token-1")).await.unwrap();
        drop(input_tx);

        let event = rx.recv().await.expect("position event routed");
        assert!(matches!(event, StrategyEvent::Positions(_)));
        task.await.unwrap();
    }
}
```

- [ ] **Step 2: Run dispatcher tests to verify current compile state**

Run:

```powershell
cargo test dispatcher::tests::dispatcher_keeps_market_routing_without_order_events; if ($?) { cargo test dispatcher::tests::dispatcher_keeps_position_routing_without_order_events }
```

Expected: PASS before removing order routing, proving non-order routing behavior is covered.

- [ ] **Step 3: Remove order-status routing from dispatcher**

In `src/dispatcher.rs`, remove match arms for:

```rust
StrategyEvent::OrderStatus(_)
StrategyEvent::OrderFill(_)
StrategyEvent::TradeConfirmed(_)
```

Keep market, positions, and reward pool removal routing unchanged.

- [ ] **Step 4: Remove obsolete strategy order event variants**

In `src/strategy.rs`, remove `OrderStatusEvent`, `OrderFillEvent`, `TradeConfirmedEvent`, and the corresponding `StrategyEvent::OrderStatus`, `StrategyEvent::OrderFill`, and `StrategyEvent::TradeConfirmed` variants. Update any remaining compile errors by replacing direct strategy order events with Gateway observations/events from Tasks 10-12.

- [ ] **Step 5: Run compile test**

Run:

```powershell
cargo test dispatcher::tests::dispatcher_keeps_market_routing_without_order_events; if ($?) { cargo test dispatcher::tests::dispatcher_keeps_position_routing_without_order_events }; if ($?) { cargo test --no-run }
```

Expected: PASS.

- [ ] **Step 6: Stop for review**

Show:

```powershell
git diff -- src/dispatcher.rs src/strategy.rs
```

---

### Task 14: Final Validation

**Files:**
- All modified files

- [ ] **Step 1: Run formatting check**

Run:

```powershell
cargo fmt --check
```

Expected: PASS.

If it fails due only to rustfmt layout, run:

```powershell
cargo fmt; if ($?) { cargo fmt --check }
```

Expected: PASS.

- [ ] **Step 2: Run focused gateway tests**

Run:

```powershell
cargo test order_gateway::tests
```

Expected: PASS.

- [ ] **Step 3: Run storage gateway tests**

Run:

```powershell
cargo test storage::tests::order_gateway
```

Expected: PASS for gateway storage tests. If test names do not share that prefix after implementation, run the exact gateway storage test names added in Task 3.

- [ ] **Step 4: Run order websocket gateway conversion tests**

Run:

```powershell
cargo test order_ws::gateway_observation_tests
```

Expected: PASS.

- [ ] **Step 5: Run strategy tests**

Run:

```powershell
cargo test strategies::market_maker::tests
cargo test liquidity_reward_fsm::tests
```

Expected: PASS.

- [ ] **Step 6: Run full test suite**

Run:

```powershell
cargo test
```

Expected: PASS. Existing warnings may remain; do not fix unrelated warnings unless they are caused by this work.

- [ ] **Step 7: Show final diff and status**

Run:

```powershell
git diff -- src/order_gateway.rs src/storage.rs src/order.rs src/order_ws.rs src/main.rs src/strategy.rs src/dispatcher.rs src/strategies/liquidity_reward_fsm.rs src/strategies/pair_arbitrage.rs src/strategies/market_maker.rs docs/superpowers/specs/2026-05-15-order-gateway-ring-design.md docs/superpowers/plans/2026-05-15-order-gateway-ring.md; git status --short
```

Expected: diff shows Order Gateway implementation plus the spec/plan docs. `.claude/settings.local.json` may remain as unrelated existing local change and should not be committed.

- [ ] **Step 8: Report completion**

Report:

- Gateway request/event rings added.
- Strategies submit `Place/Cancel` through Gateway handle.
- Gateway reducer owns order state and event publication.
- Storage persists snapshots, events, signed submission materials, and cancel attempts.
- Recovery emits `recovery = true` events and `RecoveryCompleted` before live trading.
- Order events no longer route through dispatcher.
- PositionKeeper and chain position confirmation remain out of scope.
- No git commit created unless explicitly requested.
