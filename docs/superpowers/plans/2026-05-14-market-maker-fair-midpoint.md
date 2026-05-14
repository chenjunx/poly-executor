# Market Maker Fair Midpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a pure Fair Midpoint calculation function to the market maker strategy using cleaned best bid/ask prices and sizes.

**Architecture:** Keep this as a pure function in `src/strategies/market_maker.rs` so it is easy to test and does not change the current no-op strategy event loop. The function consumes `CleanOrderbook`, computes microprice, rounds to the nearest integer tick, and clamps the result to `[best_bid_price, best_ask_price]`.

**Tech Stack:** Rust 2024, existing `CleanOrderbook` type, `cargo test`, `cargo fmt`.

---

## File Structure

- Modify: `src/strategies/market_maker.rs`
  - Import `CleanOrderbook` from `crate::strategy`.
  - Add `pub fn compute_fair_midpoint(book: &CleanOrderbook) -> u16`.
  - Add tests in the existing `#[cfg(test)] mod tests`.
- No changes to `src/main.rs`.
- No changes to strategy event loop behavior.
- No config changes.

---

### Task 1: Add Fair Midpoint Tests

**Files:**
- Modify: `src/strategies/market_maker.rs`
- Test: `src/strategies/market_maker.rs`

- [ ] **Step 1: Add test helper and failing tests**

In `src/strategies/market_maker.rs`, inside the existing `#[cfg(test)] mod tests`, add `use std::collections::BTreeMap;` and this helper:

```rust
    fn clean_book(
        best_bid_price: u16,
        best_ask_price: u16,
        best_bid_size: u32,
        best_ask_size: u32,
    ) -> CleanOrderbook {
        CleanOrderbook {
            best_bid_price,
            best_bid_size,
            best_ask_price,
            best_ask_size,
            timestamp_ms: 100,
            bids: Arc::new(BTreeMap::new()),
            asks: Arc::new(BTreeMap::new()),
        }
    }
```

Then add these tests in the same test module:

```rust
    #[test]
    fn fair_midpoint_returns_mid_when_best_sizes_are_equal() {
        let book = clean_book(40, 60, 100, 100);

        let fair_mid = compute_fair_midpoint(&book);

        assert_eq!(fair_mid, 50);
    }

    #[test]
    fn fair_midpoint_moves_toward_ask_when_bid_size_is_larger() {
        let book = clean_book(40, 60, 300, 100);

        let fair_mid = compute_fair_midpoint(&book);

        assert_eq!(fair_mid, 55);
    }

    #[test]
    fn fair_midpoint_moves_toward_bid_when_ask_size_is_larger() {
        let book = clean_book(40, 60, 100, 300);

        let fair_mid = compute_fair_midpoint(&book);

        assert_eq!(fair_mid, 45);
    }

    #[test]
    fn fair_midpoint_falls_back_to_mid_when_best_sizes_are_zero() {
        let book = clean_book(41, 60, 0, 0);

        let fair_mid = compute_fair_midpoint(&book);

        assert_eq!(fair_mid, 51);
    }

    #[test]
    fn fair_midpoint_is_clamped_to_best_bid_and_ask() {
        let bid_clamped = compute_fair_midpoint(&clean_book(40, 40, 0, 100));
        let ask_clamped = compute_fair_midpoint(&clean_book(60, 60, 100, 0));

        assert_eq!(bid_clamped, 40);
        assert_eq!(ask_clamped, 60);
    }
```

- [ ] **Step 2: Run tests to verify they fail because the function is missing**

Run:

```powershell
cargo test strategies::market_maker::tests::fair_midpoint
```

Expected: FAIL with an error like:

```text
cannot find function `compute_fair_midpoint` in this scope
```

- [ ] **Step 3: Stop for review**

Show the test diff and failure output. Do not implement the function until this red step is reviewed if the user requested per-step review.

---

### Task 2: Implement Fair Midpoint Function

**Files:**
- Modify: `src/strategies/market_maker.rs`
- Test: `src/strategies/market_maker.rs`

- [ ] **Step 1: Import `CleanOrderbook`**

At the top of `src/strategies/market_maker.rs`, change the strategy import from:

```rust
use crate::strategy::{
    OrderSignal, Strategy, StrategyEvent, StrategyRegistration, TopicRegistration,
};
```

to:

```rust
use crate::strategy::{
    CleanOrderbook, OrderSignal, Strategy, StrategyEvent, StrategyRegistration, TopicRegistration,
};
```

- [ ] **Step 2: Add minimal implementation**

Add this function above `impl MarketMakerStrategy`:

```rust
pub fn compute_fair_midpoint(book: &CleanOrderbook) -> u16 {
    let bid = u64::from(book.best_bid_price);
    let ask = u64::from(book.best_ask_price);
    let bid_size = u64::from(book.best_bid_size);
    let ask_size = u64::from(book.best_ask_size);
    let total_size = bid_size + ask_size;

    let rounded = if total_size > 0 {
        let numerator = ask * bid_size + bid * ask_size;
        (numerator + total_size / 2) / total_size
    } else {
        (bid + ask + 1) / 2
    };

    let clamped = rounded.clamp(bid, ask);
    clamped as u16
}
```

- [ ] **Step 3: Run fair midpoint tests**

Run:

```powershell
cargo test strategies::market_maker::tests::fair_midpoint
```

Expected: PASS, 5 tests matching `fair_midpoint`.

- [ ] **Step 4: Run all market maker tests**

Run:

```powershell
cargo test strategies::market_maker::tests
```

Expected: PASS, existing market maker tests plus the new fair midpoint tests.

- [ ] **Step 5: Stop for review**

Show the implementation diff and test results. Do not proceed to final validation until reviewed if the user requested per-step review.

---

### Task 3: Final Validation

**Files:**
- Modify: `src/strategies/market_maker.rs`

- [ ] **Step 1: Run formatting check**

Run:

```powershell
cargo fmt --check
```

Expected: PASS.

If it fails only because of rustfmt layout, run:

```powershell
cargo fmt; if ($?) { cargo fmt --check }
```

Expected: PASS.

- [ ] **Step 2: Run focused tests**

Run:

```powershell
cargo test strategies::market_maker::tests
```

Expected: PASS.

- [ ] **Step 3: Run full test suite**

Run:

```powershell
cargo test
```

Expected: PASS.

- [ ] **Step 4: Show final diff and status**

Run:

```powershell
git diff -- src/strategies/market_maker.rs docs/superpowers/specs/2026-05-14-market-maker-fair-midpoint-design.md docs/superpowers/plans/2026-05-14-market-maker-fair-midpoint.md; git status --short
```

Expected: diff shows only the fair midpoint function/tests/spec/plan for this task, plus any pre-existing unrelated working tree changes in status.

- [ ] **Step 5: Report completion**

Report:

- `compute_fair_midpoint(&CleanOrderbook) -> u16` added.
- It uses microprice, rounds to nearest tick, and clamps to `[bid, ask]`.
- No event loop changes.
- No `OrderSignal` emission changes.
- No config changes.
- No git commit created unless explicitly requested by the user.
