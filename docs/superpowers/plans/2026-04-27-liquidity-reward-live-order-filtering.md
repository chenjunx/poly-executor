# Liquidity Reward Live Order Filtering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make liquidity reward estimates count only live orders and remove terminal orders from the in-memory order correlation map after they have been processed.

**Architecture:** Keep `OrderCorrelationMap` as the shared in-memory order lookup, but enrich `LocalOrderMeta` with a status string so readers can distinguish live orders from staged or terminal orders. Update status transitions in `order.rs` and `order_ws.rs`, then filter reward monitor collection to `open` orders only. Remove both local and remote correlation keys after terminal websocket/status processing is complete.

**Tech Stack:** Rust 2024, `DashMap`, `tokio`, `rusqlite`, existing inline Rust unit tests via `cargo test`.

---

## File Structure

- Modify `src/strategy.rs`
  - Add `status: Arc<str>` to `LocalOrderMeta`.
  - Add small helper methods on `LocalOrderMeta` for live-order checks and terminal cleanup key access.

- Modify `src/storage.rs`
  - Populate `LocalOrderMeta.status` when converting `StoredOrder` during recovery.

- Modify `src/order.rs`
  - Initialize status for newly submitted and staged liquidity reward orders.
  - Update correlation status when simulated or real submit result changes DB status.
  - Add and test helper functions for setting status and removing both local/remote correlation keys.

- Modify `src/order_ws.rs`
  - After websocket status classification, update correlation status.
  - After fill delta and terminal strategy notification are emitted, remove terminal entries from `OrderCorrelationMap`.

- Modify `src/monitor.rs`
  - Filter `collect_my_orders` to include only `LocalOrderMeta` entries whose status is `open`.
  - Add unit tests proving canceled and pending-cancel orders do not contribute to reward estimation.

---

### Task 1: Add order status to LocalOrderMeta and storage conversion

**Files:**
- Modify: `src/strategy.rs:323-335`
- Modify: `src/storage.rs:55-64`
- Test: `src/strategy.rs` inline `#[cfg(test)]` module

- [ ] **Step 1: Write the failing tests**

Add this test module at the end of `src/strategy.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_client_sdk::types::Decimal;
    use std::sync::Arc;

    fn meta_with_status(status: &str) -> LocalOrderMeta {
        LocalOrderMeta {
            local_order_id: "local-1".to_string(),
            remote_order_id: Some("remote-1".to_string()),
            strategy: Arc::from("liquidity_reward"),
            topic: Some(Arc::from("liquidity_reward")),
            token: "token-1".to_string(),
            side: QuoteSide::Buy,
            price: Decimal::from(50) / Decimal::from(100),
            order_size: Decimal::from(200),
            status: Arc::from(status),
        }
    }

    #[test]
    fn local_order_meta_treats_only_open_orders_as_live_for_rewards() {
        assert!(meta_with_status("open").is_live_for_reward());
        assert!(!meta_with_status("pending_submit").is_live_for_reward());
        assert!(!meta_with_status("pending_cancel_confirm").is_live_for_reward());
        assert!(!meta_with_status("cancel_requested").is_live_for_reward());
        assert!(!meta_with_status("canceled").is_live_for_reward());
        assert!(!meta_with_status("failed").is_live_for_reward());
        assert!(!meta_with_status("filled").is_live_for_reward());
        assert!(!meta_with_status("rejected").is_live_for_reward());
    }

    #[test]
    fn local_order_meta_reports_correlation_keys_for_cleanup() {
        let meta = meta_with_status("open");
        assert_eq!(
            meta.correlation_keys(),
            vec!["local-1".to_string(), "remote-1".to_string()]
        );
    }

    #[test]
    fn local_order_meta_reports_only_local_key_without_remote_id() {
        let mut meta = meta_with_status("open");
        meta.remote_order_id = None;
        assert_eq!(meta.correlation_keys(), vec!["local-1".to_string()]);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test local_order_meta_ -- --nocapture
```

Expected: FAIL to compile because `LocalOrderMeta` has no `status`, `is_live_for_reward`, or `correlation_keys`.

- [ ] **Step 3: Implement status field and helpers**

In `src/strategy.rs`, change `LocalOrderMeta` to:

```rust
#[derive(Debug, Clone)]
pub struct LocalOrderMeta {
    pub local_order_id: String,
    pub remote_order_id: Option<String>,
    pub strategy: Arc<str>,
    pub topic: Option<Arc<str>>,
    pub token: String,
    pub side: QuoteSide,
    pub price: Decimal,
    pub order_size: Decimal,
    pub status: Arc<str>,
}

impl LocalOrderMeta {
    pub fn is_live_for_reward(&self) -> bool {
        self.status.as_ref() == "open"
    }

    pub fn correlation_keys(&self) -> Vec<String> {
        let mut keys = vec![self.local_order_id.clone()];
        if let Some(remote_order_id) = &self.remote_order_id {
            keys.push(remote_order_id.clone());
        }
        keys
    }
}
```

In `src/storage.rs`, update `StoredOrder::to_local_order_meta` to include status:

```rust
    pub fn to_local_order_meta(&self) -> LocalOrderMeta {
        LocalOrderMeta {
            local_order_id: self.local_order_id.clone(),
            remote_order_id: self.remote_order_id.clone(),
            strategy: Arc::from(self.strategy.as_str()),
            topic: self.topic.as_ref().map(|topic| Arc::from(topic.as_str())),
            token: self.token.clone(),
            side: self.side,
            price: self.price,
            order_size: self.order_size,
            status: Arc::from(self.status.as_str()),
        }
    }
```

- [ ] **Step 4: Fix all new `LocalOrderMeta` initializers to compile**

For every production initializer reported by the compiler, add the correct initial status:

In `src/order.rs` `UnifiedOrder::LiquidityRewardPlace`, use:

```rust
status: Arc::from("pending_submit"),
```

In `src/order.rs` `UnifiedOrder::LiquidityRewardStageReplacement`, use:

```rust
status: Arc::from("pending_cancel_confirm"),
```

Do not change behavior elsewhere in this step.

- [ ] **Step 5: Run test to verify it passes**

Run:

```bash
cargo test local_order_meta_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
git add src/strategy.rs src/storage.rs src/order.rs
git commit -m "track local order status in correlations"
```

---

### Task 2: Add correlation status update and cleanup helpers

**Files:**
- Modify: `src/order.rs`
- Test: `src/order.rs` inline `#[cfg(test)]` module

- [ ] **Step 1: Write the failing tests**

Add this test module at the end of `src/order.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use polymarket_client_sdk::types::Decimal;
    use std::sync::Arc;

    fn meta() -> LocalOrderMeta {
        LocalOrderMeta {
            local_order_id: "local-1".to_string(),
            remote_order_id: Some("remote-1".to_string()),
            strategy: Arc::from("liquidity_reward"),
            topic: Some(Arc::from("liquidity_reward")),
            token: "token-1".to_string(),
            side: QuoteSide::Buy,
            price: Decimal::from(56) / Decimal::from(100),
            order_size: Decimal::from(200),
            status: Arc::from("pending_submit"),
        }
    }

    #[test]
    fn update_correlation_status_updates_local_and_remote_entries() {
        let correlations: OrderCorrelationMap = Arc::new(DashMap::new());
        let meta = meta();
        correlations.insert(meta.local_order_id.clone(), meta.clone());
        correlations.insert("remote-1".to_string(), meta);

        update_correlation_status(&correlations, "local-1", "open");

        assert_eq!(correlations.get("local-1").unwrap().status.as_ref(), "open");
        assert_eq!(correlations.get("remote-1").unwrap().status.as_ref(), "open");
    }

    #[test]
    fn remove_correlation_entries_removes_local_and_remote_entries() {
        let correlations: OrderCorrelationMap = Arc::new(DashMap::new());
        let meta = meta();
        correlations.insert(meta.local_order_id.clone(), meta.clone());
        correlations.insert("remote-1".to_string(), meta.clone());

        remove_correlation_entries(&correlations, &meta);

        assert!(correlations.get("local-1").is_none());
        assert!(correlations.get("remote-1").is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test correlation_ -- --nocapture
```

Expected: FAIL to compile because `update_correlation_status` and `remove_correlation_entries` do not exist.

- [ ] **Step 3: Implement helper functions**

Add these private helper functions in `src/order.rs` near `schedule_balance_retry_if_needed`:

```rust
fn update_correlation_status(
    correlations: &OrderCorrelationMap,
    local_order_id: &str,
    status: &str,
) {
    let Some(mut meta) = correlations.get(local_order_id).map(|entry| entry.clone()) else {
        return;
    };

    meta.status = Arc::from(status);
    for key in meta.correlation_keys() {
        correlations.insert(key, meta.clone());
    }
}

fn remove_correlation_entries(correlations: &OrderCorrelationMap, meta: &LocalOrderMeta) {
    for key in meta.correlation_keys() {
        correlations.remove(&key);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cargo test correlation_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/order.rs
git commit -m "add order correlation status helpers"
```

---

### Task 3: Keep correlation status in sync in order submission paths

**Files:**
- Modify: `src/order.rs:78-92`
- Modify: `src/order.rs:128-137`
- Modify: `src/order.rs:212-216`
- Modify: `src/order.rs:258-260`
- Modify: `src/order.rs:430-434`
- Test: existing tests from Task 2 plus targeted `cargo test`

- [ ] **Step 1: Write the failing test**

Extend the `update_correlation_status_updates_local_and_remote_entries` test in `src/order.rs` by adding a second transition:

```rust
        update_correlation_status(&correlations, "local-1", "open");
        update_correlation_status(&correlations, "local-1", "canceled");

        assert_eq!(correlations.get("local-1").unwrap().status.as_ref(), "canceled");
        assert_eq!(correlations.get("remote-1").unwrap().status.as_ref(), "canceled");
```

Keep the existing `open` assertions before the second transition.

- [ ] **Step 2: Run test to verify it currently passes helper behavior**

Run:

```bash
cargo test update_correlation_status_updates_local_and_remote_entries -- --nocapture
```

Expected: PASS. This confirms the helper supports the status transitions before wiring it into production paths.

- [ ] **Step 3: Wire status updates into production paths**

In `src/order.rs` simulated `LiquidityRewardPlace` branch, after DB status update to `open`, add:

```rust
                    update_correlation_status(&correlations, &local_order_id, "open");
```

In `src/order.rs` real submit error branch, after DB status update to `failed`, add:

```rust
                        update_correlation_status(&correlations, &local_order_id, "failed");
```

In `src/order.rs` simulated stage replacement branch, after DB status update of active order to `canceled`, add:

```rust
                        update_correlation_status(&correlations, &active_local_order_id, "canceled");
```

In `src/order.rs` simulated cancel-only branch, after DB status update of active order to `canceled`, add:

```rust
                        update_correlation_status(&correlations, &active_local_order_id, "canceled");
```

In `src/order.rs` `place_liquidity_reward_order`, set the status before reinserting local and remote keys:

```rust
    let status = if response.success { "open" } else { "failed" };
    meta.status = Arc::from(status);
    meta.remote_order_id = Some(response.order_id.clone());
    correlations.insert(local_order_id.to_string(), meta.clone());
    correlations.insert(response.order_id.clone(), meta.clone());
```

Remove the old duplicate `let status = ...` if present below those insertions.

- [ ] **Step 4: Run targeted tests**

Run:

```bash
cargo test correlation_ local_order_meta_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/order.rs
git commit -m "sync correlation status during order submission"
```

---

### Task 4: Filter reward monitor collection to live orders only

**Files:**
- Modify: `src/monitor.rs:185-207`
- Test: `src/monitor.rs` inline `#[cfg(test)]` module

- [ ] **Step 1: Write the failing tests**

Add this test module at the end of `src/monitor.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use polymarket_client_sdk::types::Decimal;
    use std::sync::Arc;

    fn insert_order(
        correlations: &OrderCorrelationMap,
        id: &str,
        token: &str,
        status: &str,
        price: Decimal,
    ) {
        correlations.insert(
            id.to_string(),
            crate::strategy::LocalOrderMeta {
                local_order_id: id.to_string(),
                remote_order_id: None,
                strategy: Arc::from("liquidity_reward"),
                topic: Some(Arc::from("liquidity_reward")),
                token: token.to_string(),
                side: QuoteSide::Buy,
                price,
                order_size: Decimal::from(200),
                status: Arc::from(status),
            },
        );
    }

    #[test]
    fn collect_my_orders_ignores_non_open_liquidity_reward_orders() {
        let correlations: OrderCorrelationMap = Arc::new(DashMap::new());
        insert_order(
            &correlations,
            "open-order",
            "token-1",
            "open",
            Decimal::from(56) / Decimal::from(100),
        );
        insert_order(
            &correlations,
            "pending-cancel-order",
            "token-1",
            "pending_cancel_confirm",
            Decimal::from(57) / Decimal::from(100),
        );
        insert_order(
            &correlations,
            "canceled-order",
            "token-1",
            "canceled",
            Decimal::from(58) / Decimal::from(100),
        );

        let mut orders = Vec::new();
        collect_my_orders(&correlations, "token-1", RewardMarket::Yes, &mut orders);

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].price, 0.56);
        assert_eq!(orders[0].size, 200.0);
    }

    #[test]
    fn collect_my_orders_ignores_non_liquidity_reward_orders() {
        let correlations: OrderCorrelationMap = Arc::new(DashMap::new());
        correlations.insert(
            "mid-order".to_string(),
            crate::strategy::LocalOrderMeta {
                local_order_id: "mid-order".to_string(),
                remote_order_id: None,
                strategy: Arc::from("mid_requote"),
                topic: Some(Arc::from("mid")),
                token: "token-1".to_string(),
                side: QuoteSide::Buy,
                price: Decimal::from(56) / Decimal::from(100),
                order_size: Decimal::from(200),
                status: Arc::from("open"),
            },
        );

        let mut orders = Vec::new();
        collect_my_orders(&correlations, "token-1", RewardMarket::Yes, &mut orders);

        assert!(orders.is_empty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test collect_my_orders_ -- --nocapture
```

Expected: `collect_my_orders_ignores_non_open_liquidity_reward_orders` FAILS because canceled and pending-cancel orders are currently included.

- [ ] **Step 3: Implement live-order filter**

In `src/monitor.rs`, change the iterator filter in `collect_my_orders` from:

```rust
        .filter(|e| e.value().token == token && e.value().strategy.as_ref() == "liquidity_reward")
```

to:

```rust
        .filter(|e| {
            e.value().token == token
                && e.value().strategy.as_ref() == "liquidity_reward"
                && e.value().is_live_for_reward()
        })
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cargo test collect_my_orders_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add src/monitor.rs
git commit -m "filter reward estimates to live orders"
```

---

### Task 5: Remove terminal correlation entries after websocket processing

**Files:**
- Modify: `src/strategy.rs`
- Modify: `src/order.rs`
- Modify: `src/order_ws.rs:120-173`
- Test: `src/strategy.rs` inline tests and full `cargo test`

- [ ] **Step 1: Write the failing test**

Add this test to the existing `src/strategy.rs` test module:

```rust
    #[test]
    fn local_order_meta_identifies_terminal_statuses() {
        assert!(meta_with_status("canceled").is_terminal());
        assert!(meta_with_status("filled").is_terminal());
        assert!(meta_with_status("rejected").is_terminal());
        assert!(meta_with_status("failed").is_terminal());
        assert!(!meta_with_status("open").is_terminal());
        assert!(!meta_with_status("pending_cancel_confirm").is_terminal());
        assert!(!meta_with_status("cancel_requested").is_terminal());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cargo test local_order_meta_identifies_terminal_statuses -- --nocapture
```

Expected: FAIL to compile because `is_terminal` does not exist.

- [ ] **Step 3: Implement terminal helper**

In `src/strategy.rs` `impl LocalOrderMeta`, add:

```rust
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status.as_ref(),
            "canceled" | "filled" | "rejected" | "failed" | "unknown"
        )
    }
```

- [ ] **Step 4: Move correlation helpers to strategy module for websocket use**

Move the helper functions from `src/order.rs` into `src/strategy.rs` as public functions:

```rust
pub fn update_correlation_status(
    correlations: &OrderCorrelationMap,
    local_order_id: &str,
    status: &str,
) {
    let Some(mut meta) = correlations.get(local_order_id).map(|entry| entry.clone()) else {
        return;
    };

    meta.status = Arc::from(status);
    for key in meta.correlation_keys() {
        correlations.insert(key, meta.clone());
    }
}

pub fn remove_correlation_entries(correlations: &OrderCorrelationMap, meta: &LocalOrderMeta) {
    for key in meta.correlation_keys() {
        correlations.remove(&key);
    }
}
```

Update imports in `src/order.rs` from:

```rust
use crate::strategy::{
    LocalOrderMeta, OrderCorrelationMap, OrderSignal, OrderStatusEvent, QuoteSide,
};
```

to:

```rust
use crate::strategy::{
    LocalOrderMeta, OrderCorrelationMap, OrderSignal, OrderStatusEvent, QuoteSide,
    update_correlation_status,
};
```

Remove the private helper definitions from `src/order.rs` after the tests still compile.

Update the `src/order.rs` tests to import cleanup helper explicitly by adding inside the test module:

```rust
    use crate::strategy::remove_correlation_entries;
```

- [ ] **Step 5: Update websocket path to status-sync then cleanup terminal entries**

In `src/order_ws.rs`, update the strategy import block to:

```rust
    strategy::{
        OrderCorrelationMap, OrderFillEvent, OrderStatusEvent, StrategyEvent,
        remove_correlation_entries, update_correlation_status,
    },
```

Inside the `if let Some(local_meta) = local_meta { ... }` block, immediately before the existing `info!(...)`, add:

```rust
                    update_correlation_status(&correlations, &local_meta.local_order_id, status);
                    let local_meta = correlations
                        .get(&local_meta.local_order_id)
                        .map(|entry| entry.clone())
                        .unwrap_or(local_meta);
```

After the existing terminal `strategy_tx.try_send(...)` block, add:

```rust
                        remove_correlation_entries(&correlations, &local_meta);
```

The final terminal block should be:

```rust
                    let is_terminal = matches!(status, "canceled" | "filled" | "rejected" | "failed");
                    if is_terminal {
                        let _ =
                            strategy_tx.try_send(StrategyEvent::OrderStatus(OrderStatusEvent {
                                token: local_meta.token.clone(),
                                local_order_id: local_meta.local_order_id.clone(),
                                status: Arc::from(status),
                            }));
                        remove_correlation_entries(&correlations, &local_meta);
                    }
```

- [ ] **Step 6: Run targeted tests**

Run:

```bash
cargo test local_order_meta_ correlation_ collect_my_orders_ -- --nocapture
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add src/strategy.rs src/order.rs src/order_ws.rs
git commit -m "remove terminal orders from correlations"
```

---

### Task 6: Full verification against the observed liquidity reward case

**Files:**
- No production file changes expected
- Use: `orders.db`, `orders.log` for manual evidence only

- [ ] **Step 1: Run full Rust test suite**

Run:

```bash
cargo test
```

Expected: PASS.

- [ ] **Step 2: Run formatting check**

Run:

```bash
cargo fmt --check
```

Expected: PASS. If it fails, run `cargo fmt`, then repeat `cargo fmt --check` and `cargo test`.

- [ ] **Step 3: Run static check**

Run:

```bash
cargo check
```

Expected: PASS.

- [ ] **Step 4: Manually verify expected score behavior with current DB evidence**

Run:

```bash
python - <<'PY'
import sqlite3
conn=sqlite3.connect('orders.db')
conn.row_factory=sqlite3.Row
cur=conn.cursor()
yes='84133519426074676402386086933432206399821007368095045194658951218608907252971'
no='53265876884248469931537236333947516544893302492335491540800195725715403173567'
mid=0.575
v=4.0

def score(spread):
    return 0 if spread < 0 or spread >= v else ((v-spread)/v)**2

qone=0.0
qtwo=0.0
for tok,market in [(yes,'Yes'),(no,'No')]:
    for r in cur.execute("""
        select side, price, min_order_size
        from orders
        where token=?
          and strategy='liquidity_reward'
          and status='open'
    """, (tok,)):
        price=float(r['price'])
        size=float(r['min_order_size'])
        side=r['side']
        yes_price=price if market == 'Yes' else 1 - price
        if (side == 'buy' and market == 'Yes') or (side == 'sell' and market == 'No'):
            qone += score((mid - yes_price) * 100) * size
        if (side == 'sell' and market == 'Yes') or (side == 'buy' and market == 'No'):
            qtwo += score((yes_price - mid) * 100) * size
print(f'open_only_qone={qone}')
print(f'open_only_qtwo={qtwo}')
PY
```

Expected output for the current downloaded DB:

```text
open_only_qone=78.12500000000061
open_only_qtwo=49.999999999999915
```

This verifies the intended runtime behavior: terminal and pending-cancel orders no longer contribute to reward estimates; only open orders do.

- [ ] **Step 5: Inspect git diff**

Run:

```bash
git diff --stat HEAD~5..HEAD
```

Expected: only `src/strategy.rs`, `src/storage.rs`, `src/order.rs`, `src/order_ws.rs`, and `src/monitor.rs` changed across the implementation commits.

- [ ] **Step 6: Final commit if formatting changed after prior commits**

If `cargo fmt` changed files after Task 5, run:

```bash
git add src/strategy.rs src/storage.rs src/order.rs src/order_ws.rs src/monitor.rs
git commit -m "format liquidity reward correlation changes"
```

If there are no changes, skip this step.

---

## Self-Review

- Spec coverage: The plan covers status tracking, live-order reward filtering, terminal correlation cleanup, recovery conversion, and verification against the observed `my_qtwo` inflation case.
- Placeholder scan: No TBD/TODO placeholders remain; every code-changing step includes exact code or exact replacement instructions.
- Type consistency: `LocalOrderMeta.status` uses `Arc<str>`, matching existing string-sharing style for strategy/topic; helpers consistently take `&OrderCorrelationMap` and string status values.
