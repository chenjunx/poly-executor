# Liquidity Reward Position-Driven Unwind Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make liquidity_reward fill risk unwind trigger when positions become visible, while logging trade WS statuses and extending post-fill position refreshes to 60 seconds.

**Architecture:** Keep order WS as the source for order fill deltas and diagnostics, but make positions snapshots the gating signal for fill unwind execution. Trade WS confirmation remains useful metadata and logging, but it no longer blocks a MarketSell once a positive position is visible.

**Tech Stack:** Rust 2024, Tokio, tracing, polymarket_client_sdk_v2, existing unit tests in `src/order_ws.rs` and `src/strategies/liquidity_reward_fsm.rs`.

---

## File Structure

- Modify `src/order_ws.rs`
  - Add per-trade diagnostic logging for all authenticated user trade WS messages.
  - Keep `trade_confirmed_event` behavior focused on `Confirmed` events.
  - Extend delayed positions refreshes after `partially_filled` / `filled` order updates to 3s, 15s, 30s, and 60s.
  - Add a small test helper for the post-fill refresh delay schedule.

- Modify `src/strategies/liquidity_reward_fsm.rs`
  - Change `TokenFsm::submit_fill_unwind_if_ready` so positive visible positions can trigger fill unwind without `trade_confirmed`.
  - Keep `trade_confirmed` in `FillUnwindIntent` for diagnostics and late confirmation handling.
  - Add/adjust unit tests around unconfirmed visible positions, zero positions, and retry behavior.

---

### Task 1: Extend post-fill positions refresh schedule

**Files:**
- Modify: `src/order_ws.rs:275-283`
- Test: `src/order_ws.rs:364-404`

- [ ] **Step 1: Add a failing test for the desired refresh delays**

Add this helper and test inside `#[cfg(test)] mod tests` in `src/order_ws.rs`, below `fn dec(value: f64) -> Decimal`:

```rust
    fn post_fill_position_refresh_delays() -> &'static [Duration] {
        &[
            Duration::from_secs(3),
            Duration::from_secs(12),
            Duration::from_secs(15),
            Duration::from_secs(30),
        ]
    }

    #[test]
    fn post_fill_position_refreshes_extend_to_one_minute() {
        let cumulative = post_fill_position_refresh_delays()
            .iter()
            .scan(Duration::ZERO, |elapsed, delay| {
                *elapsed += *delay;
                Some(*elapsed)
            })
            .collect::<Vec<_>>();

        assert_eq!(
            cumulative,
            vec![
                Duration::from_secs(3),
                Duration::from_secs(15),
                Duration::from_secs(30),
                Duration::from_secs(60),
            ]
        );
    }
```

- [ ] **Step 2: Run the test and verify it fails before production code uses the helper**

Run:

```powershell
cargo test post_fill_position_refreshes_extend_to_one_minute
```

Expected: the test compiles and passes if the helper is already correct, but production code still has duplicated 3s/12s sleeps. Treat this as a characterization test for the desired schedule before refactoring the production block.

- [ ] **Step 3: Refactor the production delayed refresh block to use the helper**

Replace `src/order_ws.rs:276-283`:

```rust
                if matches!(status, "partially_filled" | "filled") {
                    let positions_refresh_tx = positions_refresh_tx.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderUpdate);
                        tokio::time::sleep(Duration::from_secs(12)).await;
                        let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderUpdate);
                    });
                }
```

with:

```rust
                if matches!(status, "partially_filled" | "filled") {
                    let positions_refresh_tx = positions_refresh_tx.clone();
                    tokio::spawn(async move {
                        for delay in post_fill_position_refresh_delays() {
                            tokio::time::sleep(*delay).await;
                            let _ = positions_refresh_tx.try_send(PositionRefreshTrigger::OrderUpdate);
                        }
                    });
                }
```

Move the `post_fill_position_refresh_delays` helper out of the test module so production can use it. Place it near `fill_delta`:

```rust
fn post_fill_position_refresh_delays() -> &'static [Duration] {
    &[
        Duration::from_secs(3),
        Duration::from_secs(12),
        Duration::from_secs(15),
        Duration::from_secs(30),
    ]
}
```

Remove the duplicate helper from the test module if you initially added it there.

- [ ] **Step 4: Run the focused test**

Run:

```powershell
cargo test post_fill_position_refreshes_extend_to_one_minute
```

Expected: PASS.

---

### Task 2: Log every authenticated trade WS status

**Files:**
- Modify: `src/order_ws.rs:286-291`
- Test: `src/order_ws.rs:364-404`

- [ ] **Step 1: Add a formatting helper and unit test**

Add this helper near `trade_confirmed_event` in `src/order_ws.rs`:

```rust
fn trade_maker_order_ids(trade: &TradeMessage) -> Vec<String> {
    trade
        .maker_orders
        .iter()
        .map(|maker| maker.order_id.clone())
        .collect()
}
```

Add this test inside the existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn trade_maker_order_ids_extracts_order_ids() {
        let trade = TradeMessage::builder()
            .id("trade-1")
            .market("0xfbc0c760359fe3f73b833535186c9592deda90f373d79b10c0af6ea6a1f947f1".parse().unwrap())
            .asset_id("31266632690440281732493182712982317452788219157475457369452413915821186184190".parse().unwrap())
            .side(polymarket_client_sdk_v2::clob::types::Side::Buy)
            .size(dec(1.0))
            .price(dec(0.56))
            .status(TradeMessageStatus::Matched)
            .taker_order_id(Some("taker-1".to_string()))
            .maker_orders(vec![
                polymarket_client_sdk_v2::clob::ws::types::response::MakerOrder::builder()
                    .asset_id("31266632690440281732493182712982317452788219157475457369452413915821186184190".parse().unwrap())
                    .matched_amount(dec(1.0))
                    .order_id("maker-1")
                    .outcome("YES")
                    .owner("owner-1")
                    .price(dec(0.56))
                    .build()
                    .unwrap(),
            ])
            .build()
            .unwrap();

        assert_eq!(trade_maker_order_ids(&trade), vec!["maker-1".to_string()]);
    }
```

If the exact builder methods differ, inspect `polymarket_client_sdk_v2::clob::ws::types::response::TradeMessageBuilder` compiler errors and fill every required field with neutral test values. Keep the test goal unchanged: `trade_maker_order_ids` returns maker order IDs.

- [ ] **Step 2: Run the focused test**

Run:

```powershell
cargo test trade_maker_order_ids_extracts_order_ids
```

Expected: PASS after all required SDK builder fields are provided.

- [ ] **Step 3: Add trade status logging in the WS match arm**

Replace `src/order_ws.rs:286-291`:

```rust
            Ok(WsMessage::Trade(trade)) => {
                if let Some(event) = trade_confirmed_event(&trade) {
                    if let Err(error) = strategy_tx.try_send(StrategyEvent::TradeConfirmed(event)) {
                        warn!(target: "order", trade_id = %trade.id, error = %error, "trade CONFIRMED 事件投递策略失败");
                    }
                }
            }
```

with:

```rust
            Ok(WsMessage::Trade(trade)) => {
                let maker_order_ids = trade_maker_order_ids(&trade);
                info!(
                    target: "order",
                    trade_id = %trade.id,
                    market = %trade.market,
                    asset_id = %trade.asset_id,
                    side = ?trade.side,
                    status = ?trade.status,
                    size = %trade.size,
                    price = %trade.price,
                    taker_order_id = ?trade.taker_order_id,
                    maker_order_ids = ?maker_order_ids,
                    timestamp = ?trade.timestamp,
                    last_update = ?trade.last_update,
                    matchtime = ?trade.matchtime,
                    "收到 trade websocket 更新"
                );
                if let Some(event) = trade_confirmed_event(&trade) {
                    if let Err(error) = strategy_tx.try_send(StrategyEvent::TradeConfirmed(event)) {
                        warn!(target: "order", trade_id = %trade.id, error = %error, "trade CONFIRMED 事件投递策略失败");
                    }
                }
            }
```

- [ ] **Step 4: Run order_ws tests**

Run:

```powershell
cargo test order_ws::tests
```

Expected: PASS.

---

### Task 3: Allow visible positions to trigger fill unwind without trade confirmation

**Files:**
- Modify: `src/strategies/liquidity_reward_fsm.rs:269-309`
- Test: `src/strategies/liquidity_reward_fsm.rs:1880+`

- [ ] **Step 1: Add a failing unit test for position-driven fill unwind**

Add this test in `src/strategies/liquidity_reward_fsm.rs` test module near the existing fill unwind tests:

```rust
    #[test]
    fn fill_unwind_submits_when_position_visible_before_trade_confirmed() {
        let mut fsm = token_fsm();
        fsm.market.best_bid = Some(dec(0.57));
        fsm.mark_fill_unwind_intent("buy-1", Some("remote-buy-1"), dec(20.0));
        fsm.update_fill_unwind_position(dec(1.090908));

        let effects = fsm.submit_fill_unwind_if_ready(&Arc::new(dashmap::DashMap::new()));

        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::MarketSell { token, price, order_size, .. }
                if token == "token1" && *price == dec(0.57) && *order_size == dec(1.09)
        ));
        let intent = fsm
            .pending_fill_unwind
            .as_ref()
            .expect("intent should remain until unwind fills");
        assert!(!intent.trade_confirmed);
        assert_eq!(intent.attempts, 1);
    }
```

- [ ] **Step 2: Run the failing test**

Run:

```powershell
cargo test fill_unwind_submits_when_position_visible_before_trade_confirmed
```

Expected: FAIL because `submit_fill_unwind_if_ready` returns no effects while `trade_confirmed` is false.

- [ ] **Step 3: Change the gating condition**

Replace `src/strategies/liquidity_reward_fsm.rs:269-294`:

```rust
    fn submit_fill_unwind_if_ready(&mut self, tick_size_map: &TickSizeMap) -> Vec<Effect> {
        let Some(intent) = self.pending_fill_unwind.clone() else {
            return Vec::new();
        };
        if !intent.trade_confirmed || intent.attempts >= FILL_UNWIND_MAX_ATTEMPTS {
            return Vec::new();
        }
        if self
            .pending_unwinds
            .values()
            .any(|unwind| unwind.kind == UnwindKind::Fill)
        {
            return Vec::new();
        }
        if intent
            .next_retry_after_ms
            .is_some_and(|next_retry_after_ms| now_ms() < next_retry_after_ms)
        {
            return Vec::new();
        }
        let Some(visible_size) = intent
            .position_visible_size
            .filter(|size| *size > Decimal::ZERO)
        else {
            return Vec::new();
        };
```

with:

```rust
    fn submit_fill_unwind_if_ready(&mut self, tick_size_map: &TickSizeMap) -> Vec<Effect> {
        let Some(intent) = self.pending_fill_unwind.clone() else {
            return Vec::new();
        };
        if intent.attempts >= FILL_UNWIND_MAX_ATTEMPTS {
            return Vec::new();
        }
        if self
            .pending_unwinds
            .values()
            .any(|unwind| unwind.kind == UnwindKind::Fill)
        {
            return Vec::new();
        }
        if intent
            .next_retry_after_ms
            .is_some_and(|next_retry_after_ms| now_ms() < next_retry_after_ms)
        {
            return Vec::new();
        }
        let Some(visible_size) = intent
            .position_visible_size
            .filter(|size| *size > Decimal::ZERO)
        else {
            return Vec::new();
        };
```

This preserves all existing protections except the trade confirmation gate. It still requires positive visible position, attempt budget, no fill unwind already in flight, and no balance retry cooldown.

- [ ] **Step 4: Run the new focused test**

Run:

```powershell
cargo test fill_unwind_submits_when_position_visible_before_trade_confirmed
```

Expected: PASS.

- [ ] **Step 5: Add a regression test proving zero visible position still does not unwind**

Add this test next to the previous test:

```rust
    #[test]
    fn fill_unwind_waits_when_position_not_visible() {
        let mut fsm = token_fsm();
        fsm.market.best_bid = Some(dec(0.57));
        fsm.mark_fill_unwind_intent("buy-1", Some("remote-buy-1"), dec(20.0));
        fsm.update_fill_unwind_position(Decimal::ZERO);

        let effects = fsm.submit_fill_unwind_if_ready(&Arc::new(dashmap::DashMap::new()));

        assert!(effects.is_empty());
        let intent = fsm
            .pending_fill_unwind
            .as_ref()
            .expect("intent should keep waiting for visible position");
        assert_eq!(intent.attempts, 0);
    }
```

- [ ] **Step 6: Run FSM fill unwind tests**

Run:

```powershell
cargo test fill_unwind
```

Expected: PASS.

---

### Task 4: Verify integration behavior and formatting

**Files:**
- Modify: no new code unless previous tasks revealed compilation issues.
- Test: Rust test suite subset and full build.

- [ ] **Step 1: Format the Rust code**

Run:

```powershell
cargo fmt
```

Expected: command exits successfully.

- [ ] **Step 2: Run focused test groups**

Run:

```powershell
cargo test order_ws::tests liquidity_reward_fsm::tests
```

If Cargo treats multiple filters differently, run these two commands instead:

```powershell
cargo test order_ws::tests
cargo test liquidity_reward_fsm::tests
```

Expected: PASS.

- [ ] **Step 3: Run full test suite**

Run:

```powershell
cargo test
```

Expected: PASS.

- [ ] **Step 4: Inspect the diff for scope control**

Run:

```powershell
git diff -- src/order_ws.rs src/strategies/liquidity_reward_fsm.rs docs/superpowers/plans/2026-05-11-liquidity-reward-position-driven-unwind.md
```

Expected: diff only contains the planned changes: trade diagnostics, post-fill refresh schedule, position-driven fill unwind, and tests.

---

## Self-Review

- Spec coverage:
  - Trade WS status logging: Task 2.
  - Positions visible triggers stop-loss/unwind: Task 3.
  - Positions sync extended to 1 minute with multiple retries: Task 1.
  - Verification: Task 4.
- Placeholder scan: no TBD/TODO/fill-later placeholders are present.
- Type consistency: task code uses existing `Duration`, `Decimal`, `TradeMessage`, `TradeMessageStatus`, `Effect::MarketSell`, and `TickSizeMap` patterns already present in the touched files.
