# Order Gateway Ring Design

## Goal

Refactor order submission and order-status delivery around a single-threaded Order Gateway. Strategies submit order intent through a shared MPSC request ring. The Gateway executes requests, merges REST/WS/timeout observations into order-state events, persists recoverable state, and publishes fixed-layout events to an SPMC event ring. Strategies subscribe to the event ring and filter their own events.

This design only covers order state. It does not implement chain position confirmation or a PositionKeeper.

## Current Context

Today strategies send `OrderSignal` through a Tokio MPSC channel to `order::run`. `order::run` executes simulated or real orders, writes order storage/correlations, and sends order status/fill events back through the strategy dispatcher. The dispatcher routes order events by token to relevant strategies.

The new design removes order-status routing from the dispatcher. Market data and position updates may continue to use the existing dispatcher, but order request/response flow moves to the Gateway rings.

## Architecture

```text
Strategy tasks
  -> request_ring: MPSC<OrderRequest>
  -> OrderGateway single-thread reducer/executor
       -> optional pre-trade risk hook
       -> simulation or venue adapter
       -> REST place/cancel
       -> WS/order observations
       -> timeout/reconciler observations
       -> OrderStore persistence
  -> event_ring: SPMC<OrderEventEnvelope>
  -> Strategy subscribers filter strategy/local/token/market keys
```

The Gateway is the only component that mutates order state. All external signals are normalized into observations, then reduced by the Gateway into ordered events.

## Non-goals

- No PositionKeeper implementation.
- No chain settlement/position-arrival confirmation.
- No strategy-specific trading logic changes beyond request/event API migration.
- No dispatcher double-write for order events once migrated.
- No lock-free hand-written byte payload in the first implementation unless needed; the design keeps the envelope compatible with that later optimization.

## Request Ring

`request_ring` is MPSC: many strategies produce, the Gateway is the only consumer.

The request API exposes only `Place` and `Cancel`:

```rust
pub enum OrderRequest {
    Place(PlaceOrderRequest),
    Cancel(CancelOrderRequest),
}
```

`PlaceOrderRequest` expresses generic order intent. Strategies do not depend on Polymarket-specific order types.

```rust
pub struct PlaceOrderRequest {
    pub strategy_id: StrategyId,
    pub topic: Option<MarketId>,
    pub token_id: TokenId,
    pub local_id: LocalOrderId,
    pub side: OrderSide,
    pub order_type: GatewayOrderType,
    pub price: Option<Decimal>,
    pub size: Decimal,
    pub reason: Option<Arc<str>>,
}

pub enum OrderSide {
    Buy,
    Sell,
}

pub enum GatewayOrderType {
    Limit { time_in_force: TimeInForce },
    Market,
}

pub enum TimeInForce {
    Gtc,
    Gtd { expires_at_ms: u64 },
    Ioc,
    Fok,
}
```

`CancelOrderRequest` supports both single-order and batch cancel scopes:

```rust
pub struct CancelOrderRequest {
    pub strategy_id: StrategyId,
    pub scope: CancelScope,
    pub reason: Option<Arc<str>>,
}

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
```

A batch cancel produces per-order cancel observations/events. Strategies should receive terminal status per affected order, not just a single batch-success event.

## Backpressure and Full Rings

- `request_ring` full: sending returns `Err(OrderRequestError::RingFull)`. The Gateway does not see the request, so no `OrderEvent` is emitted.
- Gateway recovering: sending returns `Err(OrderRequestError::GatewayRecovering)` until recovery completes.
- `event_ring` lag/overwrite: the Gateway does not block. The ring is configured with a large capacity, and lag is logged/observable via health events. Strategy subscribers detect sequence gaps.

Capacities should be configuration-driven. Initial defaults can be conservative, e.g. request ring `1024`, event ring `16384` or larger.

## Simulation Boundary

Simulation is a Gateway config, not a strategy request field.

If `simulation_enabled = true`, every `Place`/`Cancel` follows the simulation path. Strategies still send real trading intent. They should not branch on simulation mode.

## Risk Hook Boundary

The Gateway reserves a pre-trade risk hook:

```rust
pub trait OrderRiskCheck {
    fn check_place(&self, request: &PlaceOrderRequest, state: &GatewayState) -> RiskDecision;
    fn check_cancel(&self, request: &CancelOrderRequest, state: &GatewayState) -> RiskDecision;
}

pub enum RiskDecision {
    Allow,
    Reject { code: Arc<str>, reason: Arc<str> },
}
```

Gateway risk is the final safety layer: duplicate local IDs, disabled trading, price/size bounds, token bans, global kill-switches. It does not replace strategy-specific risk logic.

## Event Ring Envelope

`event_ring` carries a fixed envelope so subscribers can filter without fully matching payloads.

```rust
#[repr(C, align(64))]
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
```

Filtering order:

```text
strategy_id -> local_id/token_id/market_id -> kind
```

Every event has `seq` and `ts_ns`. Subscribers use `seq` to detect gaps and `ts_ns` to measure latency.

System-wide events use reserved IDs:

```text
strategy_id = StrategyId::SYSTEM
token_id = TokenId::ZERO
market_id = MarketId::ZERO
local_id = LocalOrderId::ZERO
```

## Event Kinds

Events are fine-grained. They are not a single broad status enum.

```rust
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
```

Payloads preserve local and remote details:

```rust
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
```

A later low-latency implementation may replace `OrderEventPayload` with a fixed byte payload:

```rust
pub payload: [u8; PAYLOAD_SIZE]
```

The envelope layout should remain stable.

## Local and Remote State

Gateway events should expose both normalized local state and remote venue evidence where relevant.

Local state is for strategy logic:

```rust
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
```

Remote state preserves venue-specific diagnostics:

```rust
pub struct RemoteOrderState {
    pub venue: Venue,
    pub status_code: Option<Arc<str>>,
    pub status_text: Option<Arc<str>>,
    pub reject_code: Option<Arc<str>>,
    pub reject_reason: Option<Arc<str>>,
    pub raw_event_type: Option<Arc<str>>,
}

pub enum Venue {
    Polymarket,
    Simulation,
}
```

Strategies should primarily act on local event kind/state, using remote status/reason for diagnostics or special handling.

## Rejection and Failure Semantics

Local rejection means the order was not sent to the venue. Examples:

- risk hook rejected
- invalid request
- duplicate `local_id`
- disabled trading
- unsupported order type

The Gateway emits `LocalRejected`. The strategy can release its pending state because no remote order exists.

Remote rejection means the venue rejected a submitted request. Examples:

- insufficient balance
- invalid price
- market closed
- venue-specific reject code

The Gateway emits `RemoteRejected` with remote code/message.

Signing/build failures are `Failed`, not remote rejections, because the order may not have reached the venue:

```rust
pub enum FailureKind {
    SigningFailed { message: Arc<str> },
    Transport { message: Arc<str> },
    Timeout { operation: GatewayOperation },
    StateConflict { message: Arc<str> },
    MissingSignedPayloadAfterRestart,
    PersistenceFailed { message: Arc<str> },
}
```

Timeout is not treated as a terminal rejection. Timeout moves the local order into `UnknownPending` or emits `Stale`, and the reconciler must resolve it.

## Three-source Reconciliation

The Gateway normalizes three sources into observations:

1. REST response: synchronous place/cancel result, accepted exchange ID, or reject reason.
2. WS push: fills, status changes, maker/taker reports.
3. Local timeout: REST response missing, WS missing, REST accepted but WS did not confirm.

All sources become `GatewayObservation` and are reduced by the Gateway. External tasks do not mutate order state directly.

```rust
pub enum GatewayObservation {
    RestPlaceAccepted { local_id: LocalOrderId, exch_id: ExchangeOrderId, remote_status: Option<RemoteOrderState> },
    RestPlaceRejected { local_id: LocalOrderId, reason: RemoteReject },
    RestCancelAccepted { local_id: LocalOrderId, exch_id: Option<ExchangeOrderId>, remote_status: Option<RemoteOrderState> },
    RestCancelRejected { local_id: LocalOrderId, reason: RemoteReject },
    WsOrderStatus { exch_id: ExchangeOrderId, status: RemoteOrderState },
    WsFill { exch_id: Option<ExchangeOrderId>, local_id: Option<LocalOrderId>, token_id: TokenId, side: OrderSide, fill_delta: Decimal, fill_price: Decimal, trade_id: Arc<str> },
    Timeout { local_id: LocalOrderId, operation: GatewayOperation },
    RestQueryStatus { local_id: LocalOrderId, exch_id: Option<ExchangeOrderId>, status: RemoteOrderState },
}
```

The Gateway maintains:

```text
local_id -> OrderRecord
exch_id -> local_id
pending_remote_observations[exch_id]
```

Rules:

- REST place accepted binds `exch_id -> local_id`.
- WS with known `exch_id` reduces into the matching order.
- WS before REST binding is stored in pending observations and replayed after binding.
- Unmatched remote events become `Orphan` events/logs.
- Timeout produces unknown/stale state, not terminal failure.

## Monotonic State Rules

State transitions are monotonic. Later observations cannot move an order backwards.

Rules:

- `Fill` is a strong terminal order state.
- `Cancelled` is terminal unless later fill evidence shows a prior partial fill; then filled totals are corrected and a correction event can be emitted.
- `PartialFill` followed by cancel success ends as `Cancelled` with nonzero cumulative fill.
- REST accepted does not prove final open state; WS or REST query may immediately upgrade to fill/cancel/expired.
- Cancel accepted does not prove no fill occurred; WS/query can still show partial/full fill before cancellation.

## Cancel Semantics

Cancel is a request, not a guaranteed result.

Possible outcomes:

- Cancel success with no fill: `Cancelled`.
- Cancel success after partial fill: `PartialFill` then `Cancelled` with cumulative fill preserved.
- Already filled: `Fill` or `RemoteRejected`/`CancelRejected` plus final fill state after reconciliation.
- Still open after cancel failure: `Failed` or `RemoteRejected`, with local state remaining open/cancel-rejected according to reducer result.
- Cancel timeout: `Stale` or `UnknownPending`; strategy must not assume the order is cancelled.

Strategies update order state from `OrderEvent`, not direct cancel REST responses.

## Reconciler

A background reconciler may query remote state for uncertain orders. It must not write order state directly. It sends `GatewayObservation::RestQueryStatus` back to the Gateway reducer.

The reconciler scans states such as:

- `UnknownPending`
- `SubmitPending` too old
- `CancelPending` too old
- `Open`/`PartiallyFilled` needing periodic confirmation

## Persistence

Persistence is owned by the Gateway. It must preserve both current state and replay/recovery materials.

### Snapshot table

`order_gateway_orders` stores one row per `local_id`:

```text
strategy_id
topic/market_id
token_id
local_id
exch_id
side
order_type
price
size
local_state
remote_status_code
filled_size_total
remaining_size
avg_fill_price
last_submission_attempt
last_event_seq
created_at_ms
updated_at_ms
terminal_at_ms
```

### Event log table

`order_gateway_events` appends every reducer-produced event:

```text
seq
created_at_ms
strategy_id
token_id
market_id
local_id
exch_id
event_kind
local_state
remote_status_code
remote_reject_code
remote_reject_reason
fill_delta
fill_total
remaining_size
avg_fill_price
error_code
error_message
raw_json
recovery
```

### Submission materials table

`order_gateway_submissions` preserves signed order data needed after restart:

```text
local_id
submit_attempt
strategy_id
token_id
side
order_type
price
size
exch_id nullable
unsigned_payload_json
signed_payload_json
signature
signer_address
nonce_or_salt
expiration
exchange_payload_hash
rest_request_json
rest_response_json nullable
rest_status_code nullable
submit_state
created_at_ms
updated_at_ms
```

This is required because after a REST timeout or process restart, the Gateway may need to query, resend the same signed payload, or cancel. It must not generate a new signed order for the same unresolved intent.

### Cancel attempts table

`order_gateway_cancel_attempts` records cancel tries:

```text
cancel_attempt_id
local_id
exch_id
scope
rest_request_json
rest_response_json
rest_status_code
cancel_state
error_code
created_at_ms
```

If venue cancel requires signed payloads, those payloads must also be preserved.

### Sensitive-data handling

Signed payloads and signatures are sensitive trading materials.

- Do not print them in logs.
- Do not include them in normal debug output.
- Restrict SQLite file permissions on deployment.
- Consider field encryption later if needed.

## Recovery Protocol

On startup, Gateway enters `GatewayPhase::Recovering`.

During recovery:

- Gateway rejects new requests with `OrderRequestError::GatewayRecovering`.
- All events written to `event_ring` have `recovery = true`.
- Strategies use recovery events to rebuild internal state only. They should not generate new trading decisions from these events.

Recovery reads non-terminal snapshots, rebuilds indexes, and uses persisted submission materials for unresolved orders.

Cases:

1. `exch_id` exists: query remote state and reduce observation.
2. No `exch_id`, signed payload exists: query by available signed payload/hash/client fields; if not found and the previous submit was not confirmed, resend the same signed payload rather than signing a new order.
3. No signed payload: emit unrecoverable failure with `MissingSignedPayloadAfterRestart`.

After recovery, Gateway emits:

```rust
OrderEventKind::RecoveryCompleted
```

with counts:

```text
recovered_order_count
unresolved_order_count
failed_unrecoverable_count
```

Then Gateway enters `GatewayPhase::Live`; new events have `recovery = false`.

## Strategy Rules

Strategies should:

- Submit only `Place`/`Cancel`.
- Treat `request_ring` send errors as no Gateway event will arrive.
- Use `Accepted` to know Gateway took ownership.
- Filter events by fixed envelope keys.
- Ignore unrelated strategy IDs without decoding payload.
- Use `recovery = true` events only to rebuild local state.
- Resume active trading decisions only after `RecoveryCompleted`.
- Treat `Fill`/`PartialFill` as order execution state only, not chain position confirmation.

## PositionKeeper Boundary

This version does not implement PositionKeeper.

Gateway events confirm order status only. `Fill` and `PartialFill` mean the venue reports matching/fill. They do not mean chain position has arrived or is usable. A future PositionKeeper may use order event fields such as `local_id`, `exch_id`, `token_id`, side, fill size, and trade IDs, but that component is out of scope for this design.

## Migration Plan Outline

1. Add gateway request/event types and fixed event envelope.
2. Add request ring and event ring implementations or wrappers.
3. Move `OrderSignal` semantics into `OrderRequest::Place/Cancel`.
4. Extract current `order::run` execution branches into Gateway executor methods.
5. Add Gateway reducer and observation queue.
6. Add persistence tables and recovery protocol.
7. Update strategies to send `OrderRequest` and consume `event_ring` subscribers.
8. Remove order-status/fill routing from dispatcher after strategies are migrated.
9. Keep market data and position dispatching unchanged.

## Test Strategy

Unit tests:

- `request_ring` full returns error and emits no event.
- Gateway recovering rejects requests.
- duplicate `local_id` is locally rejected.
- risk rejection emits `LocalRejected`.
- REST place accepted emits `Accepted/Open` with `exch_id`.
- REST place rejected emits `RemoteRejected` with code/message.
- signing failure emits `Failed(SigningFailed)`.
- place timeout emits `Stale`/unknown, not terminal rejection.
- WS before REST binding is replayed after `exch_id` binding.
- unmatched WS event emits `Orphan`.
- partial fill then cancel preserves cumulative fill.
- cancel timeout does not mark cancelled.
- batch cancel produces per-order events.
- recovery events are flagged `recovery = true`.
- `RecoveryCompleted` switches live events to `recovery = false`.
- missing signed payload after restart emits unrecoverable failure.

Integration tests:

- simulated gateway place/cancel lifecycle.
- strategy subscriber filters fixed envelope keys.
- event sequence gap detection.
- persistence snapshot + event log + submission material recovery.

## Open Implementation Choices

- Exact ring implementation crate or in-house implementation.
- Exact ID wrapper types (`StrategyId`, `LocalOrderId`, `TokenId`, `MarketId`) and whether they are interned numeric IDs or string-backed newtypes in the first version.
- Initial configured capacities for request and event rings.
- Whether first implementation uses typed `OrderEventPayload` or fixed `[u8; PAYLOAD_SIZE]` payload encoding.
