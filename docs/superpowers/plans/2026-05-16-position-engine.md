# PositionEngine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a PositionEngine module that maintains filled position, working exposure, cost basis, realized PnL, low-latency read snapshots, SQLite persistence, recovery, and reconciliation records.

**Architecture:** PositionEngine is a single-writer module. PositionIngestor serializes position events into writer-private PositionKeeper state, publishes immutable per-entry read snapshots for synchronous readers, and sends journal/snapshot/reconciliation records to SQLite persistence. PositionEngine does not implement risk rules, does not consume market midpoint, and does not calculate unrealized or total PnL.

**Tech Stack:** Rust 2024, Tokio MPSC, rusqlite, polymarket_client_sdk_v2 Decimal, serde_json, existing OrderGateway event types, TDD with cargo test.

---

## Execution constraints

- Do not commit git changes.
- Do not start the full trading program.
- Do not place orders.
- Do not write business DB files during tests; storage tests must use SQLite `:memory:`.
- After each major task, stop and show diff plus test results for user review.
- Follow TDD for every behavior change: write failing test, run it and confirm the expected failure, implement minimal code, rerun and confirm pass.

## File structure

- Create `src/position_engine.rs`
  - Owns PositionEngine domain types, PositionKeeper reducer, PositionIngestor, read handle, read snapshots, replay helpers, and tests.
- Modify `src/main.rs`
  - Add `mod position_engine;` only after the module compiles.
  - No runtime startup wiring in this plan unless a later approved plan explicitly wires live trading flow.
- Modify `src/storage.rs`
  - Add SQLite schema and persistence methods for `position_journal`, `position_snapshots`, `position_open_orders_snapshot`, and `position_reconciliations`.
  - Keep Decimal storage as strings, matching existing storage conventions.
- Modify `Cargo.toml`
  - Add `arc-swap = "1"` for sound acquire-load publication of immutable read snapshots.

## Important read-cell implementation choice

The design discussion allowed a single-entry seqlock. In Rust, direct seqlock reads over concurrently mutated `Decimal`, `HashMap`, `String`, or heap-owned state are not sound. The implementation must therefore use this safe equivalent:

- writer updates private mutable `PositionKeeper` state without locks
- writer builds an immutable `Arc<PositionEntrySnapshot>` for each changed entry
- writer publishes it through `arc_swap::ArcSwapOption` using release/acquire pointer publication
- readers acquire-load the latest immutable snapshot and clone/copy its values

This preserves the intended model: writer-private mutation is lock-free, readers are synchronous and non-blocking, and publication is a single atomic pointer update per changed entry.

---

### Task 1: Core PositionEntry state and derived methods

**Files:**
- Create: `src/position_engine.rs`
- Modify: `src/main.rs`
- Test: `src/position_engine.rs`

- [ ] **Step 1: Add module declaration for test visibility**

Modify `src/main.rs` near the other module declarations:

```rust
mod position_engine;
```

- [ ] **Step 2: Write failing tests for derived values**

Create `src/position_engine.rs` with this test-first content:

```rust
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
}
```

- [ ] **Step 3: Run tests to verify red**

Run:

```powershell
cargo test position_engine::tests::entry_snapshot_computes_avg_cost_and_theoretical_values position_engine::tests::entry_snapshot_has_no_avg_cost_when_flat
```

Expected: FAIL because `PositionEntrySnapshot` does not exist.

- [ ] **Step 4: Implement minimal snapshot type and methods**

At the top of `src/position_engine.rs`, above the test module, add:

```rust
use polymarket_client_sdk_v2::types::Decimal;

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
```

- [ ] **Step 5: Run tests to verify green**

Run:

```powershell
cargo test position_engine::tests::entry_snapshot_computes_avg_cost_and_theoretical_values position_engine::tests::entry_snapshot_has_no_avg_cost_when_flat
```

Expected: PASS.

- [ ] **Step 6: Review checkpoint**

Run:

```powershell
git diff -- src/position_engine.rs src/main.rs
```

Stop and show the diff plus test result to the user.

---

### Task 2: PositionKeeper reducer for working exposure, filled position, cost basis, and realized PnL

**Files:**
- Modify: `src/position_engine.rs`
- Test: `src/position_engine.rs`

- [ ] **Step 1: Write failing reducer tests**

Append these tests inside `#[cfg(test)] mod tests` in `src/position_engine.rs`:

```rust
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
        let strategy = keeper.entry("strategy-a", "token-1").expect("strategy entry");
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

        let strategy = keeper.entry("strategy-a", "token-1").expect("strategy entry");
        assert_eq!(strategy.working_buy_exposure, dec(10.0));
    }

    #[test]
    fn reducer_buy_fill_reduces_working_and_increases_cost_basis() {
        let mut keeper = PositionKeeper::default();

        keeper.apply_event(buy_working("buy-1", 10.0, 0.4));
        keeper.apply_event(fill("buy-1", PositionSide::Buy, 4.0, 0.4, 2));

        let strategy = keeper.entry("strategy-a", "token-1").expect("strategy entry");
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

        let strategy = keeper.entry("strategy-a", "token-1").expect("strategy entry");
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

        let strategy = keeper.entry("strategy-a", "token-1").expect("strategy entry");
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

        let strategy = keeper.entry("strategy-a", "token-1").expect("strategy entry");
        assert_eq!(strategy.working_sell_exposure, dec(5.0));
    }
```

- [ ] **Step 2: Run reducer tests to verify red**

Run:

```powershell
cargo test position_engine::tests::reducer_ -- --nocapture
```

Expected: FAIL because `PositionKeeper`, `PositionEvent`, `PositionSide`, and reducer methods do not exist.

- [ ] **Step 3: Add reducer domain types**

Add this code above the test module in `src/position_engine.rs`:

```rust
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionEventSource {
    Live,
    Recovery,
    Reconciliation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionTerminalReason {
    Cancelled,
    Expired,
    LocalRejected,
    RemoteRejected,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PositionEntryKey {
    Strategy { strategy_id: String, token_id: String },
    Global { token_id: String },
}

#[derive(Debug, Clone, PartialEq)]
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
```

- [ ] **Step 4: Add reducer implementation**

Add this implementation above the test module:

```rust
impl PositionKeeper {
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
            } => self.apply_fill(strategy_id, token_id, local_order_id, side, fill_qty, fill_price, seq, ts_ms),
            PositionEvent::OrderTerminalApplied {
                local_order_id,
                seq,
                ts_ms,
                ..
            } => self.apply_terminal(local_order_id, seq, ts_ms),
            PositionEvent::OrderStale { .. } => Vec::new(),
        }
    }

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
        let keys = self.adjust_filled(&strategy_id, &token_id, side, fill_qty, fill_price, seq, ts_ms);
        keys
    }

    fn apply_terminal(&mut self, local_order_id: String, seq: u64, ts_ms: u64) -> Vec<PositionEntryKey> {
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
        apply_fill_delta(strategy_entry, side, qty, price, seq, ts_ms, &mut self.degraded);
        keys.push(PositionEntryKey::Strategy {
            strategy_id: strategy_id.to_string(),
            token_id: token_id.to_string(),
        });

        let global_entry = self.global_entries.entry(token_id.to_string()).or_default();
        apply_fill_delta(global_entry, side, qty, price, seq, ts_ms, &mut self.degraded);
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
```

- [ ] **Step 5: Run reducer tests to verify green**

Run:

```powershell
cargo test position_engine::tests::reducer_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Run focused module tests**

Run:

```powershell
cargo test position_engine::tests
```

Expected: PASS.

- [ ] **Step 7: Review checkpoint**

Run:

```powershell
git diff -- src/position_engine.rs src/main.rs
```

Stop and show the diff plus test result to the user.

---

### Task 3: Read publication and PositionReadHandle

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/position_engine.rs`
- Test: `src/position_engine.rs`

- [ ] **Step 1: Write failing read-handle tests**

Append these tests inside the existing test module:

```rust
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
```

- [ ] **Step 2: Run read-handle tests to verify red**

Run:

```powershell
cargo test position_engine::tests::read_handle_ -- --nocapture
```

Expected: FAIL because `PositionSnapshotPublisher` and `PositionReadHandle` do not exist.

- [ ] **Step 3: Add dependency**

Modify `Cargo.toml` dependencies:

```toml
arc-swap = "1"
```

- [ ] **Step 4: Implement read publication types**

Add imports near the top of `src/position_engine.rs`:

```rust
use std::sync::Arc;

use arc_swap::ArcSwapOption;
```

Add these types above the test module:

```rust
#[derive(Default)]
pub struct PositionSnapshotPublisher {
    entries: std::sync::RwLock<HashMap<PositionEntryKey, Arc<ArcSwapOption<PositionEntrySnapshot>>>>,
}

#[derive(Clone)]
pub struct PositionReadHandle {
    entries: Arc<std::sync::RwLock<HashMap<PositionEntryKey, Arc<ArcSwapOption<PositionEntrySnapshot>>>>>,
}

impl PositionSnapshotPublisher {
    pub fn read_handle(&self) -> PositionReadHandle {
        let cloned = self
            .entries
            .read()
            .expect("position read registry should not be poisoned")
            .clone();
        PositionReadHandle {
            entries: Arc::new(std::sync::RwLock::new(cloned)),
        }
    }

    pub fn publish_changed(&self, keeper: &PositionKeeper, keys: &[PositionEntryKey]) {
        let mut registry = self
            .entries
            .write()
            .expect("position publish registry should not be poisoned");
        for key in keys {
            let snapshot = match key {
                PositionEntryKey::Strategy { strategy_id, token_id } => keeper.entry(strategy_id, token_id),
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
                PositionEntryKey::Strategy { strategy_id: entry_strategy, token_id }
                    if entry_strategy == strategy_id =>
                {
                    cell.load_full().map(|snapshot| (token_id.clone(), snapshot.as_ref().clone()))
                }
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
```

- [ ] **Step 5: Run read-handle tests to verify green**

Run:

```powershell
cargo test position_engine::tests::read_handle_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Run focused module tests**

Run:

```powershell
cargo test position_engine::tests
```

Expected: PASS.

- [ ] **Step 7: Review checkpoint**

Run:

```powershell
git diff -- Cargo.toml src/position_engine.rs
```

Stop and show the diff plus test result to the user.

---

### Task 4: SQLite schema and PositionStore methods

**Files:**
- Modify: `src/storage.rs`
- Test: `src/storage.rs`

- [ ] **Step 1: Write failing storage test**

Append this test inside `#[cfg(test)] mod tests` in `src/storage.rs`:

```rust
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
```

- [ ] **Step 2: Run storage test to verify red**

Run:

```powershell
cargo test storage::tests::position_engine_schema_persists_journal_snapshot_open_order_and_reconciliation
```

Expected: FAIL because the PositionStore structs and methods do not exist.

- [ ] **Step 3: Add storage structs**

Add these structs near the other OrderStore structs in `src/storage.rs`:

```rust
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
```

- [ ] **Step 4: Add SQLite schema**

In `OrderStore::init_schema`, inside the existing `execute_batch` SQL string, add these tables after `order_gateway_cancel_attempts`:

```sql
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
```

- [ ] **Step 5: Add storage methods**

Add these methods inside `impl OrderStore`:

```rust
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
            let tx = conn.transaction()?;
            for row in rows {
                tx.execute(
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
                tx.execute(
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
            tx.commit()?;
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
                    reconciliation.adjustment_journal_seq.map(|value| value as i64),
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

    pub fn load_latest_position_snapshot(&self) -> anyhow::Result<Option<StoredPositionSnapshotBatch>> {
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
```

If `conn.transaction()` fails to compile because `with_conn` passes an immutable `&Connection`, update `with_conn` to pass `&mut Connection` only if the existing call sites still compile. If changing `with_conn` is too broad, replace the transaction block with `conn.execute_batch("BEGIN IMMEDIATE")`, the inserts, and `conn.execute_batch("COMMIT")` while preserving rollback on errors.

- [ ] **Step 6: Run storage test to verify green**

Run:

```powershell
cargo test storage::tests::position_engine_schema_persists_journal_snapshot_open_order_and_reconciliation
```

Expected: PASS.

- [ ] **Step 7: Run storage tests**

Run:

```powershell
cargo test storage::tests
```

Expected: PASS.

- [ ] **Step 8: Review checkpoint**

Run:

```powershell
git diff -- src/storage.rs
```

Stop and show the diff plus test result to the user.

---

### Task 5: PositionIngestor queue, persistence queue, and degraded status

**Files:**
- Modify: `src/position_engine.rs`
- Test: `src/position_engine.rs`

- [ ] **Step 1: Write failing ingestor tests**

Append these tests inside the test module:

```rust
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
```

- [ ] **Step 2: Run ingestor tests to verify red**

Run:

```powershell
cargo test position_engine::tests::ingestor_ -- --nocapture
```

Expected: FAIL because `PositionIngestor`, `PositionEngineStatus`, persistence records, and ingestor handle do not exist.

- [ ] **Step 3: Implement ingestor and status types**

Add imports:

```rust
use std::sync::atomic::{AtomicU8, Ordering};

use tokio::sync::mpsc;
```

Add this code above the test module:

```rust
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionIngestError {
    RingFull,
    Closed,
}

impl PositionIngestor {
    pub fn new_for_test(
        input_capacity: usize,
        persist_capacity: usize,
    ) -> (Self, PositionIngestHandle, mpsc::Receiver<PositionPersistRecord>) {
        let (tx, rx) = mpsc::channel(input_capacity.max(1));
        let (persist_tx, persist_rx) = mpsc::channel(persist_capacity.max(1));
        let publisher = PositionSnapshotPublisher::default();
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
            keeper: PositionKeeper::default(),
            publisher,
            status,
        };
        (ingestor, handle, persist_rx)
    }

    pub async fn run_until_input_closed(mut self) {
        while let Some(event) = self.rx.recv().await {
            let changed = self.keeper.apply_event(event.clone());
            self.publisher.publish_changed(&self.keeper, &changed);
            if self
                .persist_tx
                .try_send(PositionPersistRecord::Journal(event))
                .is_err()
            {
                self.status
                    .store(PositionEngineStatus::Degraded.as_u8(), Ordering::Release);
            }
        }
        self.status
            .store(PositionEngineStatus::Stopped.as_u8(), Ordering::Release);
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
}
```

- [ ] **Step 4: Run ingestor tests to verify green**

Run:

```powershell
cargo test position_engine::tests::ingestor_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Run focused module tests**

Run:

```powershell
cargo test position_engine::tests
```

Expected: PASS.

- [ ] **Step 6: Review checkpoint**

Run:

```powershell
git diff -- src/position_engine.rs
```

Stop and show the diff plus test result to the user.

---

### Task 6: Snapshot export, load, and journal replay recovery

**Files:**
- Modify: `src/position_engine.rs`
- Test: `src/position_engine.rs`

- [ ] **Step 1: Write failing recovery test**

Append this test inside the test module:

```rust
    #[test]
    fn recovery_loads_snapshot_and_replays_journal_after_snapshot_seq() {
        let mut original = PositionKeeper::default();
        original.apply_event(buy_working("buy-1", 10.0, 0.4));
        original.apply_event(fill("buy-1", PositionSide::Buy, 4.0, 0.4, 2));
        let snapshot = original.export_snapshot(2, 200);

        let mut recovered = PositionKeeper::from_snapshot(snapshot).expect("snapshot should load");
        recovered.apply_replay_events(vec![fill("buy-1", PositionSide::Buy, 3.0, 0.4, 3)])
            .expect("journal replay should apply");

        let entry = recovered.entry("strategy-a", "token-1").expect("entry");
        assert_eq!(entry.filled_position, dec(7.0));
        assert_eq!(entry.cost_basis, dec(2.8));
        assert_eq!(entry.working_buy_exposure, dec(3.0));
    }
```

- [ ] **Step 2: Run recovery test to verify red**

Run:

```powershell
cargo test position_engine::tests::recovery_loads_snapshot_and_replays_journal_after_snapshot_seq
```

Expected: FAIL because snapshot export/load/replay methods do not exist.

- [ ] **Step 3: Implement in-memory snapshot types and recovery helpers**

Add this code above the test module:

```rust
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
                PositionEntryKey::Strategy { strategy_id, token_id } => {
                    keeper.strategy_entries.insert((strategy_id, token_id), state);
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
}
```

- [ ] **Step 4: Run recovery test to verify green**

Run:

```powershell
cargo test position_engine::tests::recovery_loads_snapshot_and_replays_journal_after_snapshot_seq
```

Expected: PASS.

- [ ] **Step 5: Run focused module tests**

Run:

```powershell
cargo test position_engine::tests
```

Expected: PASS.

- [ ] **Step 6: Review checkpoint**

Run:

```powershell
git diff -- src/position_engine.rs
```

Stop and show the diff plus test result to the user.

---

### Task 7: Reconciliation adjustment model

**Files:**
- Modify: `src/position_engine.rs`
- Test: `src/position_engine.rs`

- [ ] **Step 1: Write failing reconciliation test**

Append this test inside the test module:

```rust
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
        let strategy = keeper.entry("strategy-a", "token-1").expect("strategy entry");
        let global = keeper.global_entry("token-1").expect("global entry");
        assert_eq!(strategy.filled_position, dec(5.0));
        assert_eq!(strategy.cost_basis, dec(2.0));
        assert_eq!(strategy.realized_pnl, dec(0.1));
        assert_eq!(global.filled_position, dec(5.0));
        assert!(strategy.degraded);
    }
```

- [ ] **Step 2: Run reconciliation test to verify red**

Run:

```powershell
cargo test position_engine::tests::reconciliation_adjustment_replaces_local_state_with_exchange_truth_and_marks_degraded
```

Expected: FAIL because reconciliation adjustment types and methods do not exist.

- [ ] **Step 3: Implement reconciliation adjustment**

Add this code above the test module:

```rust
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

impl PositionKeeper {
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
}
```

- [ ] **Step 4: Run reconciliation test to verify green**

Run:

```powershell
cargo test position_engine::tests::reconciliation_adjustment_replaces_local_state_with_exchange_truth_and_marks_degraded
```

Expected: PASS.

- [ ] **Step 5: Run focused module tests**

Run:

```powershell
cargo test position_engine::tests
```

Expected: PASS.

- [ ] **Step 6: Review checkpoint**

Run:

```powershell
git diff -- src/position_engine.rs
```

Stop and show the diff plus test result to the user.

---

### Task 8: Order Gateway event adapter

**Files:**
- Modify: `src/position_engine.rs`
- Test: `src/position_engine.rs`

- [ ] **Step 1: Write failing adapter tests**

Append this test inside the test module:

```rust
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
        };

        let open_event = position_event_from_gateway_event(&open, PositionSide::Buy, dec(10.0), dec(0.4))
            .expect("open maps");
        let fill_event = position_event_from_gateway_event(&fill, PositionSide::Buy, dec(10.0), dec(0.4))
            .expect("fill maps");
        let cancel_event = position_event_from_gateway_event(&cancel, PositionSide::Buy, dec(10.0), dec(0.4))
            .expect("cancel maps");

        assert!(matches!(open_event, PositionEvent::OrderWorkingRegistered { .. }));
        assert!(matches!(fill_event, PositionEvent::OrderFillApplied { .. }));
        assert!(matches!(cancel_event, PositionEvent::OrderTerminalApplied { .. }));
    }
```

- [ ] **Step 2: Run adapter test to verify red**

Run:

```powershell
cargo test position_engine::tests::adapter_maps_gateway_open_fill_and_cancel_events_to_position_events
```

Expected: FAIL because `position_event_from_gateway_event` does not exist.

- [ ] **Step 3: Implement adapter function**

Add this code above the test module:

```rust
pub fn position_event_from_gateway_event(
    event: &crate::order_gateway::OrderEventEnvelope,
    side: PositionSide,
    original_size: Decimal,
    price: Decimal,
) -> Option<PositionEvent> {
    let strategy_id = event.strategy_id.as_str().to_string();
    let token_id = event.token_id.as_str().to_string();
    let local_order_id = event.local_id.as_str().to_string();
    let ts_ms = event.ts_ns / 1_000_000;
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
                source: if event.recovery {
                    PositionEventSource::Recovery
                } else {
                    PositionEventSource::Live
                },
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
                source: if event.recovery {
                    PositionEventSource::Recovery
                } else {
                    PositionEventSource::Live
                },
                recovery: event.recovery,
            })
        }
        crate::order_gateway::OrderEventPayload::PartialFill { fill_qty, fill_price, cum_qty, .. }
        | crate::order_gateway::OrderEventPayload::Fill { fill_qty, fill_price, cum_qty, .. } => {
            Some(PositionEvent::OrderFillApplied {
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
                source: if event.recovery {
                    PositionEventSource::Recovery
                } else {
                    PositionEventSource::Live
                },
                recovery: event.recovery,
            })
        }
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
                    crate::order_gateway::OrderEventKind::Expired => PositionTerminalReason::Expired,
                    crate::order_gateway::OrderEventKind::LocalRejected => PositionTerminalReason::LocalRejected,
                    crate::order_gateway::OrderEventKind::RemoteRejected => PositionTerminalReason::RemoteRejected,
                    crate::order_gateway::OrderEventKind::Failed => PositionTerminalReason::Failed,
                    _ => PositionTerminalReason::Cancelled,
                },
                seq: event.seq,
                ts_ms,
                source: if event.recovery {
                    PositionEventSource::Recovery
                } else {
                    PositionEventSource::Live
                },
                recovery: event.recovery,
            })
        }
        crate::order_gateway::OrderEventPayload::Stale { .. } => Some(PositionEvent::OrderStale {
            strategy_id,
            token_id,
            local_order_id,
            seq: event.seq,
            ts_ms,
            source: if event.recovery {
                PositionEventSource::Recovery
            } else {
                PositionEventSource::Live
            },
            recovery: event.recovery,
        }),
        _ => None,
    }
}
```

- [ ] **Step 4: Run adapter test to verify green**

Run:

```powershell
cargo test position_engine::tests::adapter_maps_gateway_open_fill_and_cancel_events_to_position_events
```

Expected: PASS.

- [ ] **Step 5: Run focused module tests**

Run:

```powershell
cargo test position_engine::tests
```

Expected: PASS.

- [ ] **Step 6: Review checkpoint**

Run:

```powershell
git diff -- src/position_engine.rs
```

Stop and show the diff plus test result to the user.

---

### Task 9: Final validation

**Files:**
- Modify only files changed by earlier tasks if validation reveals compile or formatting issues.

- [ ] **Step 1: Run format check**

Run:

```powershell
cargo fmt --check
```

Expected: PASS. If it fails, run `cargo fmt`, then rerun `cargo fmt --check`.

- [ ] **Step 2: Run focused tests**

Run:

```powershell
cargo test position_engine::tests; if ($?) { cargo test storage::tests::position_engine_schema_persists_journal_snapshot_open_order_and_reconciliation }
```

Expected: PASS.

- [ ] **Step 3: Run compile validation**

Run:

```powershell
cargo test --no-run
```

Expected: PASS. Existing unrelated warnings may remain; new warnings from PositionEngine should be fixed unless the user approves them.

- [ ] **Step 4: Show final diff and status**

Run:

```powershell
git diff --stat -- Cargo.toml src/main.rs src/position_engine.rs src/storage.rs docs/superpowers/plans/2026-05-16-position-engine.md
git status --short
```

Stop and show the diff summary, test results, and any warnings to the user. Do not commit.

---

## Self-review notes

Spec coverage:

- PositionEngine naming and boundaries: Tasks 1, 3, 5.
- PositionIngestor single writer and queue: Task 5.
- PositionKeeper private reducer state: Task 2.
- Read API single-entry strong snapshot and weak range/table snapshots: Task 3.
- Theoretical values as methods only: Task 1.
- Cost basis stored, avg cost derived: Tasks 1 and 2.
- No market midpoint and no unrealized PnL: preserved by all tasks.
- SQLite journal/snapshot/open-order/reconciliation tables: Task 4.
- Snapshot replay recovery: Task 6.
- Reconciliation adjustment and exchange source-of-truth semantics at reducer level: Task 7.
- Order Gateway event adapter: Task 8.
- Risk rules excluded: no task implements limits or rejection rules.

Type consistency:

- Strategy keys use `strategy_id` and `token_id` strings throughout.
- Global keys use `token_id` throughout.
- Decimal persistence uses strings throughout storage additions.
- `avg_cost`, `theoretical_min`, `theoretical_max`, and `theoretical_net` are methods on `PositionEntrySnapshot`, not stored fields.

Execution reminder:

- Every task must be executed with TDD.
- Stop after every major task review checkpoint.
- Do not commit git changes.
