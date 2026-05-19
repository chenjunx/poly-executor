# Order Gateway Settlement Confirmation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `OrderGateway` treat Polymarket private order `MATCHED` updates as pending settlement and publish `PartialFill` / `Fill` only after order-level trade activity is confirmed on-chain.

**Architecture:** `src/order/order_ws.rs` continues to own the authenticated user channel network connection, but it sends trade/order observations into `OrderGateway` instead of converting matched orderbook updates into fills. `src/order/order_gateway.rs` owns settlement state: observed trades keyed by `transaction_hash + order_id`, confirmed activity observations, idempotent fill application, and final `OrderEventRing` publication to `PositionEngine`. Activity polling is introduced behind a small injectable trait/seam so tests never call real Polymarket services; runtime wiring can start the poller in real mode after the reducer and WS trade mapping are tested.

**Tech Stack:** Rust, Tokio mpsc, Polymarket CLOB WebSocket SDK `TradeMessage`, existing `OrderGateway` reducer and `OrderEventRing`, SQLite `OrderStore` only for existing order recovery/logging, cargo test.

---

## Files

- Modify: `src/order/order_gateway.rs`
  - Stop turning private order matched size updates into `PartialFill` / `Fill`.
  - Add `SettlementTradeObserved` and `SettlementActivityConfirmed` observations.
  - Track pending and applied settlement trades in `GatewayState`.
  - Apply fills idempotently only after activity confirmation for the same `transaction_hash + exchange_order_id`.
  - Add test-only observation drain helper if needed for poller tests.
- Modify: `src/order/order_ws.rs`
  - Keep order update handling as order/open/matched logging and gateway order update observation.
  - Extract trade observations from `TradeMessage`, including `transaction_hash`, taker order id, and maker order ids.
  - Send settlement trade observations to gateway for each order id contained in the trade.
  - Do not publish fill observations from order `size_matched` deltas.
- Modify: `src/main.rs`
  - In real mode, keep private order WS startup and add settlement activity poller startup after the activity reader seam exists.
- Modify as needed: `src/clob_client.rs`
  - Add an activity reader function/trait only if the SDK/client exposes a suitable activity API. Keep the implementation thin and mockable.
- No changes to `src/position/position_engine.rs` behavior in this plan; it must continue to consume only gateway events.
- No DB schema migration in this plan.

## Constraints

- Do not start the full main program.
- Do not connect to real CLOB/WS/activity API in tests.
- Do not place or cancel real orders.
- Do not write business DB during verification.
- Do not commit git changes.
- Follow TDD: write each failing test first, run it to verify the expected failure, then implement minimal code.
- Treat Polymarket private order `MATCHED` as not final: it must not release working exposure or increase filled position by itself.
- `PositionEngine` must remain downstream-only and must not poll chain/activity for fills.
- Settlement idempotency key is exactly `(transaction_hash, exchange_order_id)`.

---

### Task 1: Stop converting matched order updates into fills

**Files:**
- Modify: `src/order/order_gateway.rs`

- [ ] **Step 1: Write the failing reducer test for full matched order updates**

Add this test inside `#[cfg(test)] mod tests` in `src/order/order_gateway.rs`:

```rust
#[test]
fn private_ws_full_match_does_not_publish_fill_before_settlement_confirmation() {
    let mut state = GatewayState::default();
    let request = place_request("local-1", "token-1", 10.0);
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

    assert!(events.iter().all(|event| event.kind != OrderEventKind::Fill));
    let order = state
        .order(&LocalOrderId::from("local-1"))
        .expect("order should stay tracked");
    assert_ne!(order.local_state, LocalOrderState::Filled);
    assert_eq!(order.filled_size_total, Decimal::ZERO);
    assert_eq!(order.remaining_size, dec(10.0));
}
```

If the existing test helper is not named `place_request`, use the helper already present in `order_gateway` tests that builds a `PlaceOrderRequest`; otherwise add the minimal helper shown in Task 1 Step 3.

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test order_gateway::tests::private_ws_full_match_does_not_publish_fill_before_settlement_confirmation
```

Expected: fail because `PrivateWsOrderUpdate` currently publishes `OrderEventKind::Fill` when `current_size_matched == original_size`.

- [ ] **Step 3: Ensure test helpers exist**

If not already available in `order_gateway` tests, add these helpers inside the test module:

```rust
fn dec(value: f64) -> Decimal {
    Decimal::try_from(value).expect("decimal should build")
}

fn place_request(local_id: &str, token_id: &str, size: f64) -> PlaceOrderRequest {
    PlaceOrderRequest {
        strategy_id: StrategyId::from("market_maker"),
        market_id: Some(MarketId::from("market-1")),
        token_id: TokenId::from(token_id),
        local_id: LocalOrderId::from(local_id),
        side: OrderSide::Buy,
        order_type: GatewayOrderType::Limit {
            time_in_force: TimeInForce::Gtc,
        },
        price: Some(dec(0.42)),
        size: dec(size),
        reason: None,
    }
}
```

Do not duplicate helpers if equivalent helpers already exist.

- [ ] **Step 4: Implement minimal reducer change**

In `src/order/order_gateway.rs`, replace `apply_private_ws_order_update(...)` with this behavior:

```rust
pub fn apply_private_ws_order_update(
    &mut self,
    update: PrivateWsOrderUpdate,
) -> Vec<OrderEventEnvelope> {
    if update.current_size_matched.is_some() {
        return self.apply_observation(GatewayObservation::WsOpen {
            exch_id: update.exch_id,
            token_id: update.token_id,
            market_id: update.market_id,
            remote_status_code: update.remote_status_code,
            ts_ns: update.ts_ns,
            recovery: update.recovery,
        });
    }
    self.apply_observation(GatewayObservation::WsOpen {
        exch_id: update.exch_id,
        token_id: update.token_id,
        market_id: update.market_id,
        remote_status_code: update.remote_status_code,
        ts_ns: update.ts_ns,
        recovery: update.recovery,
    })
}
```

Then delete `apply_private_ws_full_fill(...)` if it becomes unused.

This intentionally treats all private order matched-size updates as orderbook state only. Settlement fill events will be added in later tasks.

- [ ] **Step 5: Run targeted tests**

Run:

```powershell
cargo test order_gateway::tests::private_ws_full_match_does_not_publish_fill_before_settlement_confirmation
cargo test order_gateway::tests::private_ws_order_update_maps_full_match_to_fill
```

Expected: the new test passes. The old `private_ws_order_update_maps_full_match_to_fill` test should fail because it encodes the old incorrect behavior; update or replace that old test in the next step.

- [ ] **Step 6: Replace old full-fill matched test**

Find the old test named:

```rust
private_ws_order_update_maps_full_match_to_fill
```

Replace its assertion so it verifies no `Fill` is produced before settlement confirmation, or delete it if the new test fully covers the behavior. Do not keep any test that says full `size_matched` equals gateway fill.

- [ ] **Step 7: Run order gateway tests**

Run:

```powershell
cargo test order_gateway::tests
```

Expected: all order gateway tests pass after replacing the old behavior expectation.

---

### Task 2: Add settlement trade observation types and pending state

**Files:**
- Modify: `src/order/order_gateway.rs`

- [ ] **Step 1: Write the failing test for pending settlement trade storage**

Add this test inside `order_gateway` tests:

```rust
#[test]
fn settlement_trade_observed_is_stored_pending_without_fill_event() {
    let mut state = GatewayState::default();
    let request = place_request("local-1", "token-1", 10.0);
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
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test order_gateway::tests::settlement_trade_observed_is_stored_pending_without_fill_event
```

Expected: compile failure because `SettlementTradeObserved` and `has_pending_settlement_for_test` do not exist.

- [ ] **Step 3: Add settlement state structures**

In `src/order/order_gateway.rs`, add near `PrivateWsOrderUpdate`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SettlementKey {
    pub transaction_hash: Arc<str>,
    pub exch_id: ExchangeOrderId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingSettlementTrade {
    pub fill_qty: Decimal,
    pub fill_price: Decimal,
    pub ts_ns: u64,
    pub recovery: bool,
}
```

Add two fields to `GatewayState`:

```rust
pending_settlements: std::collections::HashMap<SettlementKey, PendingSettlementTrade>,
applied_settlements: std::collections::HashSet<SettlementKey>,
```

`#[derive(Default)]` will still work for `HashMap` and `HashSet`.

- [ ] **Step 4: Add observation variant and reducer branch**

Add this variant to `GatewayObservation`:

```rust
SettlementTradeObserved {
    exch_id: ExchangeOrderId,
    transaction_hash: Arc<str>,
    fill_qty: Decimal,
    fill_price: Decimal,
    ts_ns: u64,
    recovery: bool,
},
```

Add this match branch in `GatewayState::apply_observation(...)`:

```rust
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
```

Add this method:

```rust
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
    self.pending_settlements.entry(key).or_insert(PendingSettlementTrade {
        fill_qty,
        fill_price,
        ts_ns,
        recovery,
    });
    Vec::new()
}
```

- [ ] **Step 5: Add test-only pending assertion helper**

Add this method in `impl GatewayState`:

```rust
#[cfg(test)]
fn has_pending_settlement_for_test(&self, transaction_hash: &str, exch_id: &str) -> bool {
    self.pending_settlements.contains_key(&SettlementKey {
        transaction_hash: Arc::from(transaction_hash),
        exch_id: ExchangeOrderId::from(exch_id),
    })
}
```

- [ ] **Step 6: Run test**

Run:

```powershell
cargo test order_gateway::tests::settlement_trade_observed_is_stored_pending_without_fill_event
```

Expected: pass.

---

### Task 3: Confirm pending settlement activity into PartialFill and Fill

**Files:**
- Modify: `src/order/order_gateway.rs`

- [ ] **Step 1: Add failing tests for confirmed partial and full settlement**

Add these tests in `order_gateway` tests:

```rust
#[test]
fn settlement_activity_confirmation_publishes_partial_fill_for_pending_trade() {
    let mut state = GatewayState::default();
    let request = place_request("local-1", "token-1", 10.0);
    state.record_submitted(request);
    state.apply_observation(GatewayObservation::RestAccepted {
        local_id: LocalOrderId::from("local-1"),
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
    let order = state.order(&LocalOrderId::from("local-1")).unwrap();
    assert_eq!(order.local_state, LocalOrderState::PartiallyFilled);
    assert_eq!(order.filled_size_total, dec(3.0));
    assert_eq!(order.remaining_size, dec(7.0));
}

#[test]
fn settlement_activity_confirmation_publishes_fill_when_order_remaining_is_consumed() {
    let mut state = GatewayState::default();
    let request = place_request("local-1", "token-1", 10.0);
    state.record_submitted(request);
    state.apply_observation(GatewayObservation::RestAccepted {
        local_id: LocalOrderId::from("local-1"),
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
    let order = state.order(&LocalOrderId::from("local-1")).unwrap();
    assert_eq!(order.local_state, LocalOrderState::Filled);
    assert_eq!(order.filled_size_total, dec(10.0));
    assert_eq!(order.remaining_size, Decimal::ZERO);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```powershell
cargo test order_gateway::tests::settlement_activity_confirmation_
```

Expected: compile failure because `SettlementActivityConfirmed` does not exist.

- [ ] **Step 3: Add activity confirmation observation and reducer branch**

Add this `GatewayObservation` variant:

```rust
SettlementActivityConfirmed {
    exch_id: ExchangeOrderId,
    transaction_hash: Arc<str>,
    ts_ns: u64,
    recovery: bool,
},
```

Add this match branch in `apply_observation(...)`:

```rust
GatewayObservation::SettlementActivityConfirmed {
    exch_id,
    transaction_hash,
    ts_ns,
    recovery,
} => self.apply_settlement_activity_confirmed(exch_id, transaction_hash, ts_ns, recovery),
```

- [ ] **Step 4: Implement settlement confirmation application**

Add this method in `GatewayState`:

```rust
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
```

- [ ] **Step 5: Run confirmation tests**

Run:

```powershell
cargo test order_gateway::tests::settlement_activity_confirmation_
```

Expected: both tests pass.

---

### Task 4: Enforce settlement idempotency and pending-before-correlation behavior

**Files:**
- Modify: `src/order/order_gateway.rs`

- [ ] **Step 1: Add idempotency and pending correlation tests**

Add these tests:

```rust
#[test]
fn settlement_activity_confirmation_is_idempotent_for_same_transaction_and_order() {
    let mut state = GatewayState::default();
    let request = place_request("local-1", "token-1", 10.0);
    state.record_submitted(request);
    state.apply_observation(GatewayObservation::RestAccepted {
        local_id: LocalOrderId::from("local-1"),
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
    let order = state.order(&LocalOrderId::from("local-1")).unwrap();
    assert_eq!(order.filled_size_total, dec(3.0));
}

#[test]
fn settlement_trade_before_rest_correlation_applies_after_rest_accepted() {
    let mut state = GatewayState::default();
    let request = place_request("local-1", "token-1", 10.0);
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
        local_id: LocalOrderId::from("local-1"),
        exch_id: Some(ExchangeOrderId::from("exch-1")),
        ts_ns: 3,
        recovery: false,
    });

    assert!(events.iter().any(|event| event.kind == OrderEventKind::PartialFill));
    let order = state.order(&LocalOrderId::from("local-1")).unwrap();
    assert_eq!(order.filled_size_total, dec(3.0));
}
```

- [ ] **Step 2: Run tests to verify pending-correlation fails if not handled**

Run:

```powershell
cargo test order_gateway::tests::settlement_activity_confirmation_is_idempotent_for_same_transaction_and_order
cargo test order_gateway::tests::settlement_trade_before_rest_correlation_applies_after_rest_accepted
```

Expected: idempotency may pass after Task 3; pending-before-correlation should fail until confirmation is replayed after `RestAccepted`.

- [ ] **Step 3: Track confirmed pending settlements that lack local correlation**

Add this field to `GatewayState`:

```rust
confirmed_unapplied_settlements: std::collections::HashSet<SettlementKey>,
```

In `apply_settlement_activity_confirmed(...)`, when `local_by_exch` is missing, insert the key into `confirmed_unapplied_settlements`, reinsert the trade into `pending_settlements`, and return no events:

```rust
self.confirmed_unapplied_settlements.insert(key.clone());
self.pending_settlements.insert(key, trade);
return Vec::new();
```

In `apply_rest_accepted(...)`, after replaying `pending_by_exch`, also replay settlement confirmations for this exchange id:

```rust
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
```

- [ ] **Step 4: Run tests**

Run:

```powershell
cargo test order_gateway::tests::settlement_activity_confirmation_is_idempotent_for_same_transaction_and_order
cargo test order_gateway::tests::settlement_trade_before_rest_correlation_applies_after_rest_accepted
```

Expected: both pass.

---

### Task 5: Map user-channel trade messages into settlement observations

**Files:**
- Modify: `src/order/order_ws.rs`

- [ ] **Step 1: Write failing trade mapping test**

Add this test inside `#[cfg(test)] mod gateway_observation_tests` in `src/order/order_ws.rs`:

```rust
#[test]
fn trade_message_maps_taker_and_maker_orders_to_settlement_observations() {
    let trade = TradeMessage::builder()
        .id("trade-1".to_string())
        .market(
            "0xfbc0c760359fe3f73b833535186c9592deda90f373d79b10c0af6ea6a1f947f1"
                .parse()
                .unwrap(),
        )
        .asset_id(
            "31266632690440281732493182712982317452788219157475457369452413915821186184190"
                .parse()
                .unwrap(),
        )
        .side(polymarket_client_sdk_v2::clob::types::Side::Buy)
        .size(dec(2.0))
        .price(dec(0.56))
        .status(TradeMessageStatus::Matched)
        .taker_order_id("taker-1".to_string())
        .maker_orders(vec![
            polymarket_client_sdk_v2::clob::ws::types::response::MakerOrder::builder()
                .asset_id(
                    "31266632690440281732493182712982317452788219157475457369452413915821186184190"
                        .parse()
                        .unwrap(),
                )
                .matched_amount(dec(1.25))
                .order_id("maker-1".to_string())
                .outcome("YES".to_string())
                .owner("00000000-0000-0000-0000-000000000001".parse().unwrap())
                .price(dec(0.56))
                .build(),
        ])
        .transaction_hash(Some(
            "0x0000000000000000000000000000000000000000000000000000000000000abc"
                .parse()
                .unwrap(),
        ))
        .build();

    let observations = trade_settlement_observations(&trade);

    assert_eq!(observations.len(), 2);
    assert!(observations.iter().any(|observation| matches!(
        observation,
        crate::order_gateway::GatewayObservation::SettlementTradeObserved {
            exch_id,
            transaction_hash,
            fill_qty,
            fill_price,
            ..
        } if exch_id.as_str() == "taker-1"
            && transaction_hash.as_ref() == "0x0000000000000000000000000000000000000000000000000000000000000abc"
            && *fill_qty == dec(2.0)
            && *fill_price == dec(0.56)
    )));
    assert!(observations.iter().any(|observation| matches!(
        observation,
        crate::order_gateway::GatewayObservation::SettlementTradeObserved {
            exch_id,
            transaction_hash,
            fill_qty,
            fill_price,
            ..
        } if exch_id.as_str() == "maker-1"
            && transaction_hash.as_ref() == "0x0000000000000000000000000000000000000000000000000000000000000abc"
            && *fill_qty == dec(1.25)
            && *fill_price == dec(0.56)
    )));
}
```

If `TradeMessage::builder().transaction_hash(...)` has a different generated builder setter shape, inspect the compile error and adjust only the builder call, not the expected behavior.

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test order_ws::gateway_observation_tests::trade_message_maps_taker_and_maker_orders_to_settlement_observations
```

Expected: compile failure because `trade_settlement_observations` does not exist.

- [ ] **Step 3: Implement trade mapping helper**

In `src/order/order_ws.rs`, add:

```rust
fn trade_settlement_observations(
    trade: &TradeMessage,
) -> Vec<crate::order_gateway::GatewayObservation> {
    let Some(transaction_hash) = trade.transaction_hash else {
        return Vec::new();
    };
    let transaction_hash = Arc::<str>::from(format!("{transaction_hash:#x}"));
    let mut observations = Vec::new();
    if let Some(taker_order_id) = trade.taker_order_id.as_ref() {
        observations.push(crate::order_gateway::GatewayObservation::SettlementTradeObserved {
            exch_id: crate::order_gateway::ExchangeOrderId::from(taker_order_id.as_str()),
            transaction_hash: transaction_hash.clone(),
            fill_qty: trade.size,
            fill_price: trade.price,
            ts_ns: trade.timestamp.unwrap_or_default() as u64 * 1_000_000,
            recovery: false,
        });
    }
    for maker in &trade.maker_orders {
        observations.push(crate::order_gateway::GatewayObservation::SettlementTradeObserved {
            exch_id: crate::order_gateway::ExchangeOrderId::from(maker.order_id.as_str()),
            transaction_hash: transaction_hash.clone(),
            fill_qty: maker.matched_amount,
            fill_price: maker.price,
            ts_ns: trade.timestamp.unwrap_or_default() as u64 * 1_000_000,
            recovery: false,
        });
    }
    observations
}
```

- [ ] **Step 4: Wire WebSocket trade handling to send observations**

In the `Ok(WsMessage::Trade(trade))` branch, after the existing `info!(...)`, add:

```rust
for observation in trade_settlement_observations(&trade) {
    if let Err(error) = observation_tx.try_send(observation) {
        warn!(target: "order", trade_id = %trade.id, error = %error, "trade settlement observation 投递 Gateway 失败");
    }
}
```

- [ ] **Step 5: Run order ws tests**

Run:

```powershell
cargo test order_ws::gateway_observation_tests
cargo test order_ws::tests
```

Expected: all order ws tests pass.

---

### Task 6: Remove order matched delta fill emission from order_ws

**Files:**
- Modify: `src/order/order_ws.rs`

- [ ] **Step 1: Write failing test for order update observation classification**

Add this test to `gateway_observation_tests`:

```rust
#[test]
fn ws_order_update_with_matched_size_maps_to_open_not_fill_observation() {
    let observation = gateway_private_ws_order_update_observation(
        "exch-1".to_string(),
        "token-1".to_string(),
        "market-1".to_string(),
        Decimal::try_from(0.42_f64).unwrap(),
        Some(Decimal::ZERO),
        Some(Decimal::try_from(10_f64).unwrap()),
        Some(Decimal::try_from(10_f64).unwrap()),
        Some("matched"),
    );

    let crate::order_gateway::GatewayObservation::PrivateWsOrderUpdate(update) = observation else {
        panic!("order update should remain a private ws order update");
    };
    assert_eq!(update.current_size_matched, Some(Decimal::try_from(10_f64).unwrap()));
}
```

This test documents that order update observations carry matched size but must not be interpreted as fill until settlement confirmation.

- [ ] **Step 2: Remove delta-gated send condition**

In `src/order/order_ws.rs`, replace this block:

```rust
if let Some(delta_size) = fill_delta(previous_size_matched, current_size_matched) {
    let total_matched_size = current_size_matched.unwrap_or(Decimal::ZERO);
    if let Err(error) =
        observation_tx.try_send(gateway_private_ws_order_update_observation(...))
    {
        ...
    }
    info!(..., "根据订单 websocket 成交增量触发策略库存更新");
}
```

with unconditional observation sending for any locally matched order update:

```rust
if let Err(error) = observation_tx.try_send(gateway_private_ws_order_update_observation(
    order.id.clone(),
    local_meta.token.clone(),
    order.market.to_string(),
    order.price,
    previous_size_matched,
    current_size_matched,
    order.original_size,
    Some(status),
)) {
    warn!(
        target: "order",
        strategy = %local_meta.strategy,
        token = %local_meta.token,
        local_order_id = %local_meta.local_order_id,
        error = %error,
        "订单 websocket observation 投递 Gateway 失败"
    );
}
```

Also update the log message from:

```rust
"根据订单 websocket 成交增量触发策略库存更新"
```

to:

```rust
"订单 websocket 匹配状态已投递 Gateway，等待 settlement 确认"
```

- [ ] **Step 3: Delete unused fill_delta helper and tests**

Delete the `fill_delta(...)` helper and these tests if they become unused:

```rust
fill_delta_ignores_missing_current_size
fill_delta_ignores_first_zero_size
fill_delta_detects_first_positive_size
fill_delta_ignores_unchanged_size
fill_delta_detects_incremental_size
fill_delta_ignores_size_regression
```

Keep `trade_maker_order_ids_extracts_order_ids`.

- [ ] **Step 4: Fix stale status wording**

Update `classify_ws_status(...)` so full matched size returns `"matched"`, not `"filled"`:

```rust
if let (Some(_original_size), Some(size_matched)) = (original_size, size_matched) {
    if size_matched != "0" {
        return "matched";
    }
}
```

Do not return `"filled"` from order websocket status classification.

- [ ] **Step 5: Run order ws tests**

Run:

```powershell
cargo test order_ws::tests
cargo test order_ws::gateway_observation_tests
```

Expected: all order ws tests pass.

---

### Task 7: Add activity confirmation poller seam without real network in tests

**Files:**
- Modify: `src/order/order_gateway.rs`
- Modify as needed: `src/clob_client.rs`

- [ ] **Step 1: Write a pure poller test with fake activity reader**

Add a test module or tests inside `src/order/order_gateway.rs`:

```rust
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

struct FakeSettlementActivityReader {
    confirmed: Vec<(String, String)>,
}

#[async_trait::async_trait]
impl SettlementActivityReader for FakeSettlementActivityReader {
    async fn is_trade_activity_confirmed(
        &self,
        transaction_hash: &str,
        exch_id: &str,
    ) -> anyhow::Result<bool> {
        Ok(self
            .confirmed
            .iter()
            .any(|(tx, order)| tx == transaction_hash && order == exch_id))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test order_gateway::tests::settlement_activity_poller_sends_confirmation_for_trade_activity
```

Expected: compile failure because `SettlementActivityReader` and `poll_settlement_activity_once` do not exist.

- [ ] **Step 3: Add async-trait dependency only if unavailable**

Check if `async-trait` is already present in `Cargo.toml`. If not, do not add it immediately. Prefer a boxed future trait to avoid dependency churn:

```rust
pub trait SettlementActivityReader: Send + Sync {
    fn is_trade_activity_confirmed<'a>(
        &'a self,
        transaction_hash: &'a str,
        exch_id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + 'a>>;
}
```

If `async-trait` is already present, the async trait form in the test is acceptable. Keep whichever form compiles with the smallest dependency change.

- [ ] **Step 4: Implement one-shot poll function**

Add this function in `src/order/order_gateway.rs`:

```rust
pub async fn poll_settlement_activity_once<R>(
    reader: &R,
    pending: Vec<SettlementKey>,
    observation_tx: mpsc::Sender<GatewayObservation>,
) -> anyhow::Result<()>
where
    R: SettlementActivityReader,
{
    for key in pending {
        if reader
            .is_trade_activity_confirmed(key.transaction_hash.as_ref(), key.exch_id.as_str())
            .await?
        {
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
```

If using the boxed-future trait, the `.await` call still works on the returned future.

- [ ] **Step 5: Add pending settlement snapshot method**

Add this method to `GatewayState`:

```rust
pub fn pending_settlement_keys(&self) -> Vec<SettlementKey> {
    self.pending_settlements.keys().cloned().collect()
}
```

- [ ] **Step 6: Run poller test**

Run:

```powershell
cargo test order_gateway::tests::settlement_activity_poller_sends_confirmation_for_trade_activity
```

Expected: pass.

---

### Task 8: Add runtime poller wiring behind gateway-owned API

**Files:**
- Modify: `src/order/order_gateway.rs`
- Modify: `src/main.rs`
- Modify as needed: `src/clob_client.rs`

- [ ] **Step 1: Add API shape compile test**

Add this test to `src/main.rs` tests:

```rust
#[test]
fn order_gateway_exposes_settlement_activity_poller_entrypoint() {
    let _spawn_fn = OrderGateway::spawn_settlement_activity_poller::<order_gateway::NoopSettlementActivityReader>;
}
```

If generic function item inference is too awkward, replace this with a direct type assertion for the reader type and entrypoint once implemented.

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test tests::order_gateway_exposes_settlement_activity_poller_entrypoint
```

Expected: compile failure because `spawn_settlement_activity_poller` and `NoopSettlementActivityReader` do not exist.

- [ ] **Step 3: Add no-op reader and spawn entrypoint**

In `src/order/order_gateway.rs`, add:

```rust
pub struct NoopSettlementActivityReader;

impl SettlementActivityReader for NoopSettlementActivityReader {
    fn is_trade_activity_confirmed<'a>(
        &'a self,
        _transaction_hash: &'a str,
        _exch_id: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<bool>> + Send + 'a>> {
        Box::pin(async { Ok(false) })
    }
}
```

Add this associated function:

```rust
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
```

Add the loop:

```rust
async fn run_settlement_activity_poller<R>(
    reader: R,
    mut pending_rx: tokio::sync::watch::Receiver<Vec<SettlementKey>>,
    observation_tx: mpsc::Sender<GatewayObservation>,
) where
    R: SettlementActivityReader,
{
    loop {
        let pending = pending_rx.borrow().clone();
        if !pending.is_empty() {
            if let Err(error) = poll_settlement_activity_once(&reader, pending, observation_tx.clone()).await {
                tracing::warn!(target: "order", error = %error, "settlement activity poll failed");
            }
        }
        tokio::select! {
            changed = pending_rx.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
        }
    }
}
```

- [ ] **Step 4: Add pending settlement watch publisher to gateway**

Add to `OrderGateway` fields:

```rust
pending_settlement_tx: tokio::sync::watch::Sender<Vec<SettlementKey>>,
```

Create the channel in `new_for_test_inner(...)`:

```rust
let (pending_settlement_tx, _pending_settlement_rx) = tokio::sync::watch::channel(Vec::new());
```

Store `pending_settlement_tx` in `OrderGateway`.

In `handle_observation(...)`, after applying the observation, publish current pending keys:

```rust
self.pending_settlement_tx
    .send_replace(self.state.pending_settlement_keys());
```

Add this method:

```rust
pub fn subscribe_pending_settlements(&self) -> tokio::sync::watch::Receiver<Vec<SettlementKey>> {
    self.pending_settlement_tx.subscribe()
}
```

- [ ] **Step 5: Wire runtime in main with no-op reader initially**

In `src/main.rs`, before spawning `order_gateway.run_until_request_channel_closed()`, get the receiver:

```rust
let pending_settlement_rx = order_gateway.subscribe_pending_settlements();
```

In the real-mode block that starts private WS, add:

```rust
OrderGateway::spawn_settlement_activity_poller(
    order_gateway::NoopSettlementActivityReader,
    pending_settlement_rx,
    order_observation_tx.clone(),
);
```

This compiles the lifecycle without real activity API calls. Replace `NoopSettlementActivityReader` with the real reader only after activity API details are implemented and tested.

- [ ] **Step 6: Run compile and main tests**

Run:

```powershell
cargo test tests::order_gateway_exposes_settlement_activity_poller_entrypoint
cargo check
cargo test main::tests
```

Expected: pass. The runtime poller exists but uses no-op reader until real activity API implementation is added.

---

### Task 9: Add real activity reader only after API details are verified

**Files:**
- Modify as needed: `src/clob_client.rs`
- Modify as needed: `src/order/order_gateway.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Verify SDK/API shape without adding networked tests**

Inspect the installed Polymarket SDK types or existing client methods for activity query support. Do not call the real API. Confirm the method can query by `transaction_hash` and returns activity entries containing `type` and enough order identity to match `exch_id`.

Expected: one of these outcomes:

```text
A. SDK exposes activity query by transaction hash and activity type.
B. SDK does not expose it; need a thin HTTP client wrapper.
C. API shape is unclear; stop and ask user for the known endpoint/schema.
```

- [ ] **Step 2: If API shape is confirmed, write a parser/unit test from sample JSON**

If the API returns JSON, add a pure parser test with a redacted sample payload:

```rust
#[test]
fn activity_entry_parser_detects_trade_for_transaction_and_order() {
    let json = r#"
    [
      {"transactionHash":"0xabc","type":"TRADE","orderId":"exch-1"}
    ]
    "#;

    let entries = parse_activity_entries(json).expect("activity json should parse");

    assert!(entries.iter().any(|entry| {
        entry.transaction_hash == "0xabc"
            && entry.activity_type == "TRADE"
            && entry.order_id.as_deref() == Some("exch-1")
    }));
}
```

Use the real field names after verifying the API schema. Do not invent field names if they differ from the actual schema.

- [ ] **Step 3: Implement real reader minimally**

Implement a concrete reader that satisfies `SettlementActivityReader` and only answers:

```rust
is_trade_activity_confirmed(transaction_hash, exch_id) -> anyhow::Result<bool>
```

It must return `true` only when an activity entry has:

```text
transaction hash matches
activity type == TRADE
order id or equivalent order identity matches exch_id
```

If the activity endpoint cannot identify the order id, stop and report that runtime wiring cannot safely be completed without attribution.

- [ ] **Step 4: Replace no-op runtime reader only after unit tests pass**

In `main.rs`, replace `NoopSettlementActivityReader` with the real reader construction only if it does not require real network during tests and can be compiled offline.

- [ ] **Step 5: Run targeted tests**

Run:

```powershell
cargo test order_gateway::tests
cargo test order_ws::tests
cargo test order_ws::gateway_observation_tests
cargo check
```

Expected: pass.

---

### Task 10: Final verification and stop for review

**Files:**
- Modify only as required by earlier tasks.

- [ ] **Step 1: Format check**

Run:

```powershell
cargo fmt --check
```

Expected: pass. If formatting fails, run `cargo fmt`, then re-run `cargo fmt --check`.

- [ ] **Step 2: Targeted tests**

Run:

```powershell
cargo test order_gateway::tests
cargo test order_ws::tests
cargo test order_ws::gateway_observation_tests
cargo test position_engine::tests
cargo test main::tests
```

Expected: all pass.

- [ ] **Step 3: Final verification**

Run:

```powershell
cargo check
cargo test
```

Expected: both pass. Existing dead-code warnings may remain; fix only warnings introduced by this change.

- [ ] **Step 4: Stop for user review**

Report:

```text
Order gateway settlement confirmation pipeline complete for review.
Changed: private order MATCHED no longer publishes fills, trade messages create pending settlement observations, activity confirmation applies idempotent PartialFill/Fill events through OrderGateway, and PositionEngine remains downstream-only.
Verified: cargo fmt --check, cargo check, cargo test, and targeted order_gateway/order_ws/position_engine/main tests.
Not done: no git commit, no main program run, no real CLOB/WS connection in tests, no real orders, no business DB writes.
```

---

## Self-Review

- Spec coverage: covers the confirmed Polymarket behavior that order events stop at MATCHED, adds order-level trade observations, adds transaction-hash activity confirmation, gates final `PartialFill` / `Fill` through `OrderGateway`, and keeps `PositionEngine` downstream-only.
- Placeholder scan: no TBD/TODO/fill-in placeholders remain. Task 9 intentionally gates real activity reader implementation on verified API shape and gives exact stop conditions instead of guessing endpoint fields.
- Type consistency: uses existing `GatewayObservation`, `GatewayState`, `OrderEventKind`, `OrderEventPayload`, `ExchangeOrderId`, `TradeMessage`, and `OrderGateway` names consistently.
- Scope check: no DB migration, no real networked tests, no real order side effects, and no direct position-engine polling are included.

## Execution Options

Do not commit git changes during execution unless the user explicitly requests it. Stop for user review after implementation and verification.
