# Account Balance Risk Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the always-on account state service into gateway risk checks so real order placement is rejected when account balance is unavailable or insufficient.

**Architecture:** `src/account.rs` remains the only component that talks to CLOB for balance/allowance polling. `src/risk.rs` receives an `AccountReadHandle` as a read-only dependency and adds one global risk rule that checks the latest in-memory account snapshot during `PlaceOrderRequest` evaluation. `src/main.rs` starts the account monitor once and passes the handle into `GatewayRiskEngine`; no risk code starts polling, subscribes in the hot path, writes DB, or calls CLOB directly.

**Tech Stack:** Rust, Tokio watch channel, Polymarket SDK `Decimal`, existing `OrderRiskCheck` / `RiskRule` infrastructure, cargo test.

---

## Files

- Modify: `src/account.rs`
  - Make the test-only `AccountReadHandle::new_for_test()` helper visible to sibling module tests with `pub(crate)`.
- Modify: `src/risk.rs`
  - Import `AccountReadHandle`.
  - Add account state to `RiskContext` and `GatewayRiskEngine`.
  - Add `AccountBalanceRule` to the default global rules.
  - Add helper functions for order notional and projected account notional.
  - Add unit tests for sufficient balance, insufficient balance, missing snapshot, sell order behavior, and cancel behavior.
- Modify: `src/main.rs`
  - Rename `_account_read_handle` to `account_read_handle`.
  - Pass the account handle into `GatewayRiskEngine::new(...)`.
- No DB schema or runtime storage changes.
- No README change required unless wording about account/risk behavior is added separately after implementation.

## Constraints

- Do not start the full main program.
- Do not connect to real CLOB/WS in tests.
- Do not place or cancel real orders.
- Do not write business DB during verification.
- Do not commit git changes.
- Follow TDD: write each failing test first, run it to verify the expected failure, then implement minimal code.
- Keep risk as a read-only consumer of account state; do not move polling or CLOB client construction into `risk.rs`.
- Use `latest()` for order-path checks; do not use `subscribe()` in the synchronous risk hot path.

---

### Task 1: Expose account test handle for sibling module tests

**Files:**
- Modify: `src/account.rs`

- [ ] **Step 1: Write the failing access expectation in risk tests**

Add this minimal test inside `#[cfg(test)] mod tests` in `src/risk.rs`:

```rust
#[test]
fn risk_tests_can_build_account_read_handle() {
    let (handle, tx) = crate::account::AccountReadHandle::new_for_test();
    assert!(handle.latest().is_none());
    drop(tx);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test risk::tests::risk_tests_can_build_account_read_handle
```

Expected: compile failure because `AccountReadHandle::new_for_test` is private to the `account` module.

- [ ] **Step 3: Implement minimal visibility change**

In `src/account.rs`, change the test helper signature from:

```rust
#[cfg(test)]
fn new_for_test() -> (
    Self,
    tokio::sync::watch::Sender<Option<AccountFundSnapshot>>,
) {
```

to:

```rust
#[cfg(test)]
pub(crate) fn new_for_test() -> (
    Self,
    tokio::sync::watch::Sender<Option<AccountFundSnapshot>>,
) {
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
cargo test risk::tests::risk_tests_can_build_account_read_handle
```

Expected: pass.

---

### Task 2: Add account snapshot dependency to risk context

**Files:**
- Modify: `src/risk.rs`

- [ ] **Step 1: Write the failing construction test**

Replace `risk_tests_can_build_account_read_handle` with this test in `src/risk.rs` tests:

```rust
#[test]
fn risk_context_carries_account_read_handle() {
    let (account_read, _account_tx) = crate::account::AccountReadHandle::new_for_test();
    let (_ingestor, ingest_handle, _persist_rx) = PositionIngestor::new_for_test(8, 8);
    let engine = GatewayRiskEngine::new(
        RiskConfig::default(),
        ingest_handle.read_handle(),
        ingest_handle.status_handle(),
        StrategyKindRegistry::from_registrations(&[registration(
            "market_maker",
            StrategyKind::MarketMaker,
        )]),
        Arc::new(NoopMarketRiskReader),
        account_read.clone(),
    );

    let ctx = engine.context(&GatewayState::default());

    assert_eq!(ctx.account_read.latest(), account_read.latest());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test risk::tests::risk_context_carries_account_read_handle
```

Expected: compile failure because `GatewayRiskEngine::new` does not accept `AccountReadHandle` and `RiskContext` does not expose `account_read`.

- [ ] **Step 3: Add account handle to risk engine and context**

In `src/risk.rs`, add the import near the top:

```rust
use crate::account::AccountReadHandle;
```

Change `RiskContext` from:

```rust
pub struct RiskContext<'a> {
    pub gateway_state: &'a GatewayState,
    pub position_read: &'a PositionReadHandle,
    pub position_status: &'a PositionStatusHandle,
    pub strategy_registry: &'a StrategyKindRegistry,
    pub market_reader: &'a dyn MarketRiskReader,
    pub config: &'a RiskConfig,
}
```

to:

```rust
pub struct RiskContext<'a> {
    pub gateway_state: &'a GatewayState,
    pub position_read: &'a PositionReadHandle,
    pub position_status: &'a PositionStatusHandle,
    pub strategy_registry: &'a StrategyKindRegistry,
    pub market_reader: &'a dyn MarketRiskReader,
    pub account_read: &'a AccountReadHandle,
    pub config: &'a RiskConfig,
}
```

Add the field to `GatewayRiskEngine`:

```rust
account_read: AccountReadHandle,
```

Change `GatewayRiskEngine::new(...)` and `GatewayRiskEngine::with_rules(...)` to accept the final parameter:

```rust
account_read: AccountReadHandle,
```

Pass it through `Self::with_rules(...)`, store it in `Self`, and include it in `context()`:

```rust
account_read: &self.account_read,
```

Do not add any account balance rule in this task.

- [ ] **Step 4: Update existing risk test helpers to pass account handle**

In `src/risk.rs` tests, add:

```rust
fn account_handle_with_snapshot(
    snapshot: Option<crate::account::AccountFundSnapshot>,
) -> crate::account::AccountReadHandle {
    let (handle, tx) = crate::account::AccountReadHandle::new_for_test();
    tx.send_replace(snapshot);
    handle
}

fn empty_account_handle() -> crate::account::AccountReadHandle {
    account_handle_with_snapshot(None)
}
```

Update every existing `GatewayRiskEngine::new(...)` call in `src/risk.rs` tests to pass `empty_account_handle()` as the last argument.

Update `engine_with_config` and `engine_with_position` so they pass `empty_account_handle()` as the last argument.

- [ ] **Step 5: Run targeted risk tests**

Run:

```powershell
cargo test risk::tests
```

Expected: all risk tests pass after construction updates. No account balance behavior exists yet.

---

### Task 3: Wire account handle from main into risk engine

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Write the failing compile expectation**

Run:

```powershell
cargo check
```

Expected after Task 2: compile failure in `src/main.rs` because `GatewayRiskEngine::new(...)` now requires an `AccountReadHandle` argument.

- [ ] **Step 2: Pass the account read handle into risk engine**

In `src/main.rs`, replace:

```rust
let _account_read_handle = account::spawn_account_monitor(app_config.auth.clone());
```

with:

```rust
let account_read_handle = account::spawn_account_monitor(app_config.auth.clone());
```

Then update `GatewayRiskEngine::new(...)` from:

```rust
let risk_engine = GatewayRiskEngine::new(
    RiskConfig::default(),
    position_read_handle,
    position_status_handle,
    StrategyKindRegistry::from_registrations(&registrations),
    Arc::new(NoopMarketRiskReader),
);
```

to:

```rust
let risk_engine = GatewayRiskEngine::new(
    RiskConfig::default(),
    position_read_handle,
    position_status_handle,
    StrategyKindRegistry::from_registrations(&registrations),
    Arc::new(NoopMarketRiskReader),
    account_read_handle,
);
```

- [ ] **Step 3: Run compile check**

Run:

```powershell
cargo check
```

Expected: compile succeeds, allowing existing warnings.

---

### Task 4: Add account balance rule for missing snapshot

**Files:**
- Modify: `src/risk.rs`

- [ ] **Step 1: Write the failing missing-snapshot test**

Add this test in `src/risk.rs` tests:

```rust
#[test]
fn account_balance_rule_rejects_place_when_snapshot_is_unavailable() {
    let engine = engine_with_config(RiskConfig::default());

    let decision = engine.check_place(
        &place("market_maker", "token-1", 1.0),
        &GatewayState::default(),
    );

    assert!(matches!(
        decision,
        RiskDecision::Reject { ref code, .. } if code.as_ref() == "account_snapshot_unavailable"
    ));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test risk::tests::account_balance_rule_rejects_place_when_snapshot_is_unavailable
```

Expected: test fails because the current engine allows otherwise valid orders without account snapshot.

- [ ] **Step 3: Add minimal account balance rule for missing snapshot**

In `src/risk.rs`, add this rule before `GlobalTokenExposureRule`:

```rust
pub struct AccountBalanceRule;

impl RiskRule for AccountBalanceRule {
    fn id(&self) -> &'static str {
        "account_balance"
    }

    fn layer(&self) -> RiskLayer {
        RiskLayer::Global
    }

    fn check_place(&self, _request: &PlaceOrderRequest, ctx: &RiskContext<'_>) -> RiskDecision {
        if ctx.account_read.latest().is_none() {
            return reject("account_snapshot_unavailable", "account snapshot is unavailable");
        }
        RiskDecision::Allow
    }
}
```

Add the rule to the default `GatewayRiskEngine::new(...)` rule list after `BasicOrderSanityRule` and before exposure rules:

```rust
Arc::new(AccountBalanceRule),
```

- [ ] **Step 4: Run the missing-snapshot test**

Run:

```powershell
cargo test risk::tests::account_balance_rule_rejects_place_when_snapshot_is_unavailable
```

Expected: pass.

---

### Task 5: Add account balance rule for sufficient and insufficient buy notional

**Files:**
- Modify: `src/risk.rs`

- [ ] **Step 1: Add tests for sufficient and insufficient balance**

Add these helpers in `src/risk.rs` tests:

```rust
fn account_snapshot(balance: f64) -> crate::account::AccountFundSnapshot {
    crate::account::AccountFundSnapshot {
        checked_at_ms: 42,
        balance: dec(balance),
        allowances_json: "{}".to_string(),
    }
}

fn engine_with_account_balance(balance: f64) -> GatewayRiskEngine {
    let (_ingestor, ingest_handle, _persist_rx) = PositionIngestor::new_for_test(8, 8);
    GatewayRiskEngine::new(
        RiskConfig::default(),
        ingest_handle.read_handle(),
        ingest_handle.status_handle(),
        StrategyKindRegistry::from_registrations(&[registration(
            "market_maker",
            StrategyKind::MarketMaker,
        )]),
        Arc::new(NoopMarketRiskReader),
        account_handle_with_snapshot(Some(account_snapshot(balance))),
    )
}
```

Add these tests:

```rust
#[test]
fn account_balance_rule_allows_buy_when_balance_covers_order_notional() {
    let engine = engine_with_account_balance(10.0);

    let decision = engine.check_place(
        &place("market_maker", "token-1", 5.0),
        &GatewayState::default(),
    );

    assert_eq!(decision, RiskDecision::Allow);
}

#[test]
fn account_balance_rule_rejects_buy_when_order_notional_exceeds_balance() {
    let engine = engine_with_account_balance(1.0);

    let decision = engine.check_place(
        &place("market_maker", "token-1", 5.0),
        &GatewayState::default(),
    );

    assert!(matches!(
        decision,
        RiskDecision::Reject { ref code, .. } if code.as_ref() == "insufficient_account_balance"
    ));
}
```

The existing `place(...)` helper uses price `0.42`, so size `5.0` has order notional `2.10`.

- [ ] **Step 2: Run tests to verify insufficient behavior fails**

Run:

```powershell
cargo test risk::tests::account_balance_rule_
```

Expected: sufficient-balance test may pass if only snapshot availability is checked; insufficient-balance test fails because notional is not compared to balance yet.

- [ ] **Step 3: Implement buy notional comparison**

In `AccountBalanceRule::check_place`, replace the body with:

```rust
let Some(snapshot) = ctx.account_read.latest() else {
    return reject("account_snapshot_unavailable", "account snapshot is unavailable");
};
let projected_notional = projected_account_notional(request, ctx);
if projected_notional > snapshot.balance {
    return reject("insufficient_account_balance", "account balance is insufficient");
}
RiskDecision::Allow
```

Add helper functions near `projected_global_exposure(...)`:

```rust
fn projected_account_notional(request: &PlaceOrderRequest, ctx: &RiskContext<'_>) -> Decimal {
    current_account_notional_for_token(request, ctx) + order_buy_notional(request)
}

fn current_account_notional_for_token(
    request: &PlaceOrderRequest,
    ctx: &RiskContext<'_>,
) -> Decimal {
    ctx.position_read
        .get_global_entry(request.token_id.as_str())
        .map(|entry| entry.working_buy_exposure * request.price.unwrap_or(Decimal::ZERO))
        .unwrap_or(Decimal::ZERO)
}

fn order_buy_notional(request: &PlaceOrderRequest) -> Decimal {
    match request.side {
        OrderSide::Buy => request.price.unwrap_or(Decimal::ZERO) * request.size,
        OrderSide::Sell => Decimal::ZERO,
    }
}
```

- [ ] **Step 4: Run account balance rule tests**

Run:

```powershell
cargo test risk::tests::account_balance_rule_
```

Expected: all account balance rule tests pass.

---

### Task 6: Cover sell orders and cancel requests

**Files:**
- Modify: `src/risk.rs`

- [ ] **Step 1: Add sell-order helper and tests**

Add this helper in `src/risk.rs` tests:

```rust
fn sell_place(strategy_id: &str, token_id: &str, size: f64) -> PlaceOrderRequest {
    let mut request = place(strategy_id, token_id, size);
    request.side = OrderSide::Sell;
    request
}
```

Add these tests:

```rust
#[test]
fn account_balance_rule_does_not_require_balance_for_sell_order_notional() {
    let engine = engine_with_account_balance(0.0);

    let decision = engine.check_place(
        &sell_place("market_maker", "token-1", 5.0),
        &GatewayState::default(),
    );

    assert_eq!(decision, RiskDecision::Allow);
}

#[test]
fn account_balance_rule_does_not_reject_cancel_when_snapshot_is_unavailable() {
    let engine = engine_with_config(RiskConfig::default());

    let decision = engine.check_cancel(&cancel("market_maker"), &GatewayState::default());

    assert_eq!(decision, RiskDecision::Allow);
}
```

- [ ] **Step 2: Run tests**

Run:

```powershell
cargo test risk::tests::account_balance_rule_does_not_
```

Expected: both pass. If sell fails because another exposure rule rejects it, adjust only the test setup to use a token/position/config that isolates account balance behavior; do not weaken the account rule.

- [ ] **Step 3: Run all risk tests**

Run:

```powershell
cargo test risk::tests
```

Expected: all risk tests pass.

---

### Task 7: Add projected working buy notional coverage

**Files:**
- Modify: `src/risk.rs`

- [ ] **Step 1: Add helper for account balance plus existing working exposure**

Add this helper in `src/risk.rs` tests:

```rust
fn engine_with_account_balance_and_position(
    balance: f64,
    working_buy_exposure: f64,
) -> GatewayRiskEngine {
    let (_ingestor, ingest_handle, _persist_rx) = PositionIngestor::new_for_test(8, 8);
    let publisher = PositionSnapshotPublisher::default();
    let read_handle = publisher.read_handle();
    let mut keeper = PositionKeeper::default();
    let changed = keeper.apply_event(PositionEvent::OrderWorkingRegistered {
        strategy_id: "market_maker".to_string(),
        token_id: "token-1".to_string(),
        local_order_id: "working-1".to_string(),
        exchange_order_id: None,
        side: PositionSide::Buy,
        price: dec(0.4),
        size: dec(working_buy_exposure),
        seq: 1,
        ts_ms: 100,
        source: PositionEventSource::Live,
        recovery: false,
    });
    publisher.publish_changed(&keeper, &changed);
    GatewayRiskEngine::new(
        RiskConfig::default(),
        read_handle,
        ingest_handle.status_handle(),
        StrategyKindRegistry::from_registrations(&[registration(
            "market_maker",
            StrategyKind::MarketMaker,
        )]),
        Arc::new(NoopMarketRiskReader),
        account_handle_with_snapshot(Some(account_snapshot(balance))),
    )
}
```

Add this test:

```rust
#[test]
fn account_balance_rule_rejects_buy_when_projected_working_notional_exceeds_balance() {
    let engine = engine_with_account_balance_and_position(2.0, 4.0);

    let decision = engine.check_place(
        &place("market_maker", "token-1", 2.0),
        &GatewayState::default(),
    );

    assert!(matches!(
        decision,
        RiskDecision::Reject { ref code, .. } if code.as_ref() == "insufficient_account_balance"
    ));
}
```

With the existing `place(...)` helper price of `0.42`, current working notional is approximated as `4.0 * 0.42 = 1.68` and new order notional is `2.0 * 0.42 = 0.84`, totaling `2.52`, which exceeds balance `2.0`.

- [ ] **Step 2: Run test**

Run:

```powershell
cargo test risk::tests::account_balance_rule_rejects_buy_when_projected_working_notional_exceeds_balance
```

Expected: pass if Task 5 helper already includes current working buy exposure. If it fails, fix `current_account_notional_for_token(...)` without changing the test expectation.

- [ ] **Step 3: Run all risk tests**

Run:

```powershell
cargo test risk::tests
```

Expected: all risk tests pass.

---

### Task 8: Final verification and stop for review

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
cargo test account::tests
cargo test risk::tests
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
Account balance risk integration complete for review.
Changed: gateway risk now reads AccountReadHandle, rejects place orders when account snapshot is unavailable or projected buy notional exceeds account balance, and main passes the account monitor handle into risk.
Verified: cargo fmt --check, cargo check, cargo test, and targeted account/risk/main tests.
Not done: no git commit, no main program run, no real CLOB/WS connection, no real orders, no business DB writes.
```

---

## Self-Review

- Spec coverage: covers passing account state into risk, missing snapshot rejection, insufficient balance rejection, sufficient balance allow path, sell/cancel non-impact, and main wiring.
- Placeholder scan: no TBD/TODO/fill-in placeholders remain.
- Type consistency: uses existing `AccountReadHandle`, `AccountFundSnapshot`, `GatewayRiskEngine`, `RiskContext`, `RiskRule`, `PlaceOrderRequest`, `RiskDecision`, and `Decimal` names.
- Scope check: risk remains a read-only account state consumer; no DB write, no CLOB calls from risk, no main program execution, and no real order side effects.

## Execution Options

Do not commit git changes during execution unless the user explicitly requests it. Stop for user review after implementation and verification.
