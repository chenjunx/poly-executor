# Market Maker Strategy Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a default-on `market_maker` strategy skeleton that builds from selected liquidity reward pool entries, registers with the existing strategy routing, and runs without emitting any order signals.

**Architecture:** Create `src/strategies/market_maker.rs` with a `MarketMakerStrategy` implementing the existing `Strategy` trait. Wire it into `main.rs` as an optional strategy built from `MarketStore::load_liquidity_reward_pool_entries()`: empty DB pool returns `None`, non-empty pool registers token/topic routing and starts an empty event loop.

**Tech Stack:** Rust 2024, Tokio, existing `Strategy` trait, `StrategyRegistration`, `TopicRegistration`, `MarketStore`, `ActiveRewardMarketPoolEntry`, cargo test.

---

## File Structure

- Create: `src/strategies/market_maker.rs`
  - Owns the new `MarketMakerStrategy` skeleton.
  - Converts selected `ActiveRewardMarketPoolEntry` rows into rules and strategy registration.
  - Implements `Strategy::spawn()` as an empty event consumer that never sends `OrderSignal`.
- Modify: `src/strategies/mod.rs`
  - Exposes `pub mod market_maker;`.
- Modify: `src/main.rs`
  - Imports `MarketMakerStrategy`.
  - Adds `market_maker: Option<MarketMakerStrategy>` to `StrategyBootstrap`.
  - Builds market maker from DB selected pool entries.
  - Adds market maker registration to routing.
  - Starts market maker task in `spawn_strategy_tasks()`.
  - Adds focused unit tests for registration inclusion.

No git commit should be created during implementation unless the user explicitly asks. At the end of each task, stop for review with diff and test output.

---

### Task 1: Add MarketMakerStrategy registration skeleton

**Files:**
- Create: `src/strategies/market_maker.rs`
- Modify: `src/strategies/mod.rs`

- [ ] **Step 1: Write the failing tests and module export**

Create `src/strategies/market_maker.rs` with tests first and only enough type declarations to show the intended API is missing:

```rust
use std::sync::Arc;

use crate::storage::ActiveRewardMarketPoolEntry;
use crate::strategy::{Strategy, StrategyRegistration, TopicRegistration};

pub struct MarketMakerStrategy;

impl MarketMakerStrategy {
    pub fn from_pool_entries(
        _entries: Vec<ActiveRewardMarketPoolEntry>,
    ) -> anyhow::Result<Option<Self>> {
        unimplemented!("market maker strategy skeleton not implemented yet")
    }
}

impl Strategy for MarketMakerStrategy {
    fn name(&self) -> &str {
        unimplemented!("market maker name not implemented yet")
    }

    fn registration(&self) -> &StrategyRegistration {
        unimplemented!("market maker registration not implemented yet")
    }

    fn spawn(
        self,
        _rx: tokio::sync::mpsc::Receiver<crate::strategy::StrategyEvent>,
        _order_tx: tokio::sync::mpsc::Sender<crate::strategy::OrderSignal>,
    ) -> tokio::task::JoinHandle<()> {
        unimplemented!("market maker spawn not implemented yet")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn active_entry(condition_id: &str, token1: &str, token2: &str) -> ActiveRewardMarketPoolEntry {
        ActiveRewardMarketPoolEntry {
            condition_id: condition_id.to_string(),
            market_slug: Some(format!("slug-{condition_id}")),
            question: None,
            token1: token1.to_string(),
            token2: token2.to_string(),
            tokens_json: "[]".to_string(),
            market_competitiveness: None,
            rewards_min_size: None,
            rewards_max_spread: None,
            market_daily_reward: None,
            volume_24hr_clob: None,
            volume_24hr: None,
            liquidity_reward_roi: None,
            build_date_utc: None,
            pool_version: Some(1),
            liquidity_reward_selected: true,
            liquidity_reward_selected_at_ms: Some(100),
            liquidity_reward_select_reason: Some("roi_descending_top_n".to_string()),
            liquidity_reward_select_rank: Some(1),
            liquidity_reward_halted: false,
            liquidity_reward_halted_at_ms: None,
            liquidity_reward_halt_reason: None,
            liquidity_reward_halted_pool_version: None,
        }
    }

    #[test]
    fn from_pool_entries_returns_none_for_empty_pool() {
        let strategy = MarketMakerStrategy::from_pool_entries(Vec::new())
            .expect("empty pool should not error");

        assert!(strategy.is_none());
    }

    #[test]
    fn from_pool_entries_registers_selected_pool_tokens() {
        let strategy = MarketMakerStrategy::from_pool_entries(vec![
            active_entry("0xbbb", "token-b2", "token-b1"),
            active_entry("0xaaa", "token-a1", "token-b1"),
        ])
        .expect("selected pool should build strategy")
        .expect("non-empty pool should create strategy");

        let registration = strategy.registration();
        assert_eq!(strategy.name(), "market_maker");
        assert_eq!(registration.name.as_ref(), "market_maker");
        assert_eq!(registration.topics.as_ref(), &[Arc::<str>::from("market_maker")]);
        assert_eq!(
            registration.related_tokens.as_ref(),
            &[
                "token-a1".to_string(),
                "token-b1".to_string(),
                "token-b2".to_string(),
            ]
        );
        assert_eq!(registration.topic_tokens.len(), 1);
        assert_eq!(registration.topic_tokens[0].topic.as_ref(), "market_maker");
        assert_eq!(
            registration.topic_tokens[0].tokens.as_ref(),
            registration.related_tokens.as_ref()
        );
    }
}
```

Modify `src/strategies/mod.rs`:

```rust
pub mod liquidity_reward;
pub mod liquidity_reward_fsm;
pub mod market_maker;
pub mod pair_arbitrage;
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test strategies::market_maker::tests::from_pool_entries_returns_none_for_empty_pool
```

Expected: FAIL at runtime with `not implemented: market maker strategy skeleton not implemented yet`.

Run:

```powershell
cargo test strategies::market_maker::tests::from_pool_entries_registers_selected_pool_tokens
```

Expected: FAIL at runtime with `not implemented: market maker strategy skeleton not implemented yet`.

- [ ] **Step 3: Implement minimal registration skeleton**

Replace `src/strategies/market_maker.rs` with:

```rust
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::storage::ActiveRewardMarketPoolEntry;
use crate::strategy::{OrderSignal, Strategy, StrategyEvent, StrategyRegistration, TopicRegistration};

const MARKET_MAKER_TOPIC: &str = "market_maker";
const MARKET_MAKER_NAME: &str = "market_maker";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketMakerRule {
    pub condition_id: String,
    pub market_slug: Option<String>,
    pub token1: String,
    pub token2: String,
}

pub struct MarketMakerStrategy {
    rules: Arc<[MarketMakerRule]>,
    registration: Arc<StrategyRegistration>,
}

impl MarketMakerStrategy {
    pub fn from_pool_entries(
        entries: Vec<ActiveRewardMarketPoolEntry>,
    ) -> anyhow::Result<Option<Self>> {
        if entries.is_empty() {
            return Ok(None);
        }

        let mut rules = Vec::with_capacity(entries.len());
        let mut related_tokens = BTreeSet::new();
        for entry in entries {
            related_tokens.insert(entry.token1.clone());
            related_tokens.insert(entry.token2.clone());
            rules.push(MarketMakerRule {
                condition_id: entry.condition_id,
                market_slug: entry.market_slug,
                token1: entry.token1,
                token2: entry.token2,
            });
        }

        let related_tokens = related_tokens.into_iter().collect::<Vec<_>>();
        let topic = Arc::<str>::from(MARKET_MAKER_TOPIC);
        let registration = Arc::new(StrategyRegistration {
            name: Arc::from(MARKET_MAKER_NAME),
            topics: Arc::<[Arc<str>]>::from(vec![topic.clone()]),
            topic_tokens: Arc::<[TopicRegistration]>::from(vec![TopicRegistration {
                topic,
                tokens: Arc::<[String]>::from(related_tokens.clone()),
            }]),
            related_tokens: Arc::<[String]>::from(related_tokens),
        });

        Ok(Some(Self {
            rules: Arc::<[MarketMakerRule]>::from(rules),
            registration,
        }))
    }

    pub fn rules(&self) -> &[MarketMakerRule] {
        self.rules.as_ref()
    }
}

impl Strategy for MarketMakerStrategy {
    fn name(&self) -> &str {
        MARKET_MAKER_NAME
    }

    fn registration(&self) -> &StrategyRegistration {
        self.registration.as_ref()
    }

    fn spawn(
        self,
        mut rx: tokio::sync::mpsc::Receiver<StrategyEvent>,
        _order_tx: tokio::sync::mpsc::Sender<OrderSignal>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                match event {
                    StrategyEvent::Market(_)
                    | StrategyEvent::Positions(_)
                    | StrategyEvent::OrderStatus(_)
                    | StrategyEvent::OrderFill(_)
                    | StrategyEvent::TradeConfirmed(_)
                    | StrategyEvent::RewardPoolRemoval(_) => {}
                }
            }
        })
    }
}
```

Then keep the tests from Step 1 at the bottom of the file.

- [ ] **Step 4: Run tests to verify they pass**

Run:

```powershell
cargo test strategies::market_maker::tests
```

Expected: PASS, both market maker tests pass.

- [ ] **Step 5: Review checkpoint**

Show:

```powershell
git diff -- src/strategies/market_maker.rs src/strategies/mod.rs
```

Stop for user review before Task 2.

---

### Task 2: Wire MarketMakerStrategy into strategy construction and routing

**Files:**
- Modify: `src/main.rs`
- Test: `src/main.rs` unit tests

- [ ] **Step 1: Write the failing routing test**

In `src/main.rs`, update imports and add the test before implementing routing changes.

Add import near existing strategy imports:

```rust
use strategies::market_maker::MarketMakerStrategy;
```

Change the existing test module at the bottom of `src/main.rs` to include these helper/test additions:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ActiveRewardMarketPoolEntry;

    fn active_pool_entry(condition_id: &str, token1: &str, token2: &str) -> ActiveRewardMarketPoolEntry {
        ActiveRewardMarketPoolEntry {
            condition_id: condition_id.to_string(),
            market_slug: Some(format!("slug-{condition_id}")),
            question: None,
            token1: token1.to_string(),
            token2: token2.to_string(),
            tokens_json: "[]".to_string(),
            market_competitiveness: None,
            rewards_min_size: None,
            rewards_max_spread: None,
            market_daily_reward: None,
            volume_24hr_clob: None,
            volume_24hr: None,
            liquidity_reward_roi: None,
            build_date_utc: None,
            pool_version: Some(1),
            liquidity_reward_selected: true,
            liquidity_reward_selected_at_ms: Some(100),
            liquidity_reward_select_reason: Some("roi_descending_top_n".to_string()),
            liquidity_reward_select_rank: Some(1),
            liquidity_reward_halted: false,
            liquidity_reward_halted_at_ms: None,
            liquidity_reward_halt_reason: None,
            liquidity_reward_halted_pool_version: None,
        }
    }

    #[test]
    fn account_monitor_enabled_respects_account_config() {
        let mut config = AccountConfig::default();
        assert!(!account_monitor_enabled(&config));

        config.enabled = true;
        assert!(account_monitor_enabled(&config));
    }

    #[test]
    fn build_strategy_registrations_includes_market_maker_when_present() {
        let pair_registration = StrategyRegistration {
            name: Arc::from("pair_arbitrage"),
            topics: Arc::<[Arc<str>]>::from(vec![Arc::from("pair")]),
            topic_tokens: Arc::<[TopicRegistration]>::from(vec![TopicRegistration {
                topic: Arc::from("pair"),
                tokens: Arc::<[String]>::from(vec!["pair-token".to_string()]),
            }]),
            related_tokens: Arc::<[String]>::from(vec!["pair-token".to_string()]),
        };
        let market_maker = MarketMakerStrategy::from_pool_entries(vec![active_pool_entry(
            "0xabc",
            "maker-token-1",
            "maker-token-2",
        )])
        .expect("market maker should build")
        .expect("non-empty pool should create market maker");

        let registrations = build_strategy_registrations(
            &pair_registration,
            None,
            Some(&market_maker),
        );

        assert_eq!(registrations.len(), 2);
        assert_eq!(registrations[0].name.as_ref(), "pair_arbitrage");
        assert_eq!(registrations[1].name.as_ref(), "market_maker");
    }
}
```

This changes the expected signature of `build_strategy_registrations()` before implementation.

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test tests::build_strategy_registrations_includes_market_maker_when_present
```

Expected: FAIL to compile with an error like `this function takes 2 arguments but 3 arguments were supplied`.

- [ ] **Step 3: Implement routing registration change**

Update `src/main.rs` imports:

```rust
use strategies::liquidity_reward::{LiquidityRewardRestoreState, LiquidityRewardRule};
use strategies::liquidity_reward_fsm::LiquidityRewardFsmStrategy as LiquidityRewardStrategy;
use strategies::market_maker::MarketMakerStrategy;
use strategies::pair_arbitrage::PairArbitrageStrategy;
```

Update the call site in `main()` after destructuring `StrategyBootstrap` later in Task 3. For this task, update only the function signature/body:

```rust
fn build_strategy_registrations(
    pair_registration: &StrategyRegistration,
    liquidity_reward: Option<&LiquidityRewardStrategy>,
    market_maker: Option<&MarketMakerStrategy>,
) -> Vec<StrategyRegistration> {
    let mut registrations = vec![pair_registration.clone()];
    if let Some(strategy) = liquidity_reward {
        registrations.push(strategy.registration().clone());
    }
    if let Some(strategy) = market_maker {
        registrations.push(strategy.registration().clone());
    }
    registrations
}
```

Temporarily update the existing main call to pass `None` until Task 3 adds `market_maker` to `StrategyBootstrap`:

```rust
let registrations = build_strategy_registrations(
    &pair_registration,
    liquidity_reward.as_ref(),
    None,
);
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
cargo test tests::build_strategy_registrations_includes_market_maker_when_present
```

Expected: PASS.

Also run:

```powershell
cargo test strategies::market_maker::tests
```

Expected: PASS.

- [ ] **Step 5: Review checkpoint**

Show:

```powershell
git diff -- src/main.rs src/strategies/market_maker.rs src/strategies/mod.rs
```

Stop for user review before Task 3.

---

### Task 3: Build market maker from DB selected pool in startup

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Write the failing builder test**

Add this test to the existing `#[cfg(test)] mod tests` in `src/main.rs`:

```rust
#[test]
fn build_market_maker_strategy_uses_selected_pool_entries() {
    let store = MarketStore::open(":memory:").expect("store should open");
    store.init_schema().expect("schema should initialize");
    let build_date = chrono::NaiveDate::from_ymd_opt(2026, 5, 14).unwrap();
    let entries = vec![storage::RewardMarketPoolStorageEntry {
        condition_id: "0xmaker",
        market_slug: Some("maker-market"),
        question: Some("Maker market?"),
        token1: "maker-token-1",
        token2: "maker-token-2",
        tokens_json: "[]",
        market_competitiveness: None,
        rewards_min_size: Some("100"),
        rewards_max_spread: Some("4"),
        market_daily_reward: Some("50"),
        volume_24hr_clob: Some("60000"),
        volume_24hr: Some("65000"),
        liquidity_reward_roi: Some("0.5"),
    }];
    store
        .replace_reward_market_pool_entries(build_date, 1, &entries, 100, 1)
        .expect("pool entries should save");

    let market_maker = build_market_maker_strategy(&store)
        .expect("market maker build should not error")
        .expect("selected pool should create market maker");

    assert_eq!(market_maker.registration().name.as_ref(), "market_maker");
    assert_eq!(
        market_maker.registration().related_tokens.as_ref(),
        &["maker-token-1".to_string(), "maker-token-2".to_string()]
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test tests::build_market_maker_strategy_uses_selected_pool_entries
```

Expected: FAIL to compile with `cannot find function build_market_maker_strategy`.

- [ ] **Step 3: Implement startup builder and bootstrap field**

Update `StrategyBootstrap` in `src/main.rs`:

```rust
struct StrategyBootstrap {
    pair_strategy: PairArbitrageStrategy,
    pair_registration: StrategyRegistration,
    liquidity_reward: Option<LiquidityRewardStrategy>,
    market_maker: Option<MarketMakerStrategy>,
    liquidity_reward_tokens: Arc<HashSet<String>>,
    reward_monitor_configs: HashMap<String, RewardMonitorConfig>,
    tick_size_map: tick_size::TickSizeMap,
}
```

Update `build_strategies()` after liquidity reward strategy is built:

```rust
let liquidity_reward_strategy_opt = build_liquidity_reward_strategy(app_config, &market_store)?;
let market_maker = build_market_maker_strategy(&market_store)?;
```

Update the returned bootstrap:

```rust
Ok(StrategyBootstrap {
    pair_strategy,
    pair_registration,
    liquidity_reward,
    market_maker,
    liquidity_reward_tokens,
    reward_monitor_configs,
    tick_size_map,
})
```

Add helper near `build_liquidity_reward_strategy()`:

```rust
fn build_market_maker_strategy(
    market_store: &MarketStore,
) -> anyhow::Result<Option<MarketMakerStrategy>> {
    let entries = market_store.load_liquidity_reward_pool_entries()?;
    MarketMakerStrategy::from_pool_entries(entries)
}
```

Update destructuring in `main()`:

```rust
let StrategyBootstrap {
    pair_strategy,
    pair_registration,
    liquidity_reward,
    market_maker,
    liquidity_reward_tokens,
    reward_monitor_configs,
    tick_size_map,
} = strategy_bootstrap;
```

Update registration call in `main()`:

```rust
let registrations = build_strategy_registrations(
    &pair_registration,
    liquidity_reward.as_ref(),
    market_maker.as_ref(),
);
```

Update `spawn_strategy_tasks()` call in `main()`:

```rust
spawn_strategy_tasks(
    pair_strategy,
    pair_registration,
    liquidity_reward,
    market_maker,
    order_tx.clone(),
    strategy_rx,
);
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
cargo test tests::build_market_maker_strategy_uses_selected_pool_entries
```

Expected: PASS.

Also run:

```powershell
cargo test tests::build_strategy_registrations_includes_market_maker_when_present
```

Expected: PASS.

- [ ] **Step 5: Review checkpoint**

Show:

```powershell
git diff -- src/main.rs src/strategies/market_maker.rs src/strategies/mod.rs
```

Stop for user review before Task 4.

---

### Task 4: Start market maker task through dispatcher handles

**Files:**
- Modify: `src/main.rs`
- Modify: `src/strategies/market_maker.rs`

- [ ] **Step 1: Write the failing spawn helper test**

To test task registration without starting the whole program, extract a helper that turns optional market maker into a `StrategyHandle` plus channel.

Add this test to `src/main.rs` tests before implementing the helper:

```rust
#[test]
fn market_maker_strategy_handle_is_created_when_strategy_exists() {
    let market_maker = MarketMakerStrategy::from_pool_entries(vec![active_pool_entry(
        "0xabc",
        "maker-token-1",
        "maker-token-2",
    )])
    .expect("market maker should build")
    .expect("non-empty pool should create market maker");

    let (handle, _rx) = market_maker_strategy_handle(&market_maker)
        .expect("market maker handle should exist");

    assert_eq!(handle.name.as_ref(), "market_maker");
    assert_eq!(
        handle.related_tokens.as_ref(),
        &["maker-token-1".to_string(), "maker-token-2".to_string()]
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test tests::market_maker_strategy_handle_is_created_when_strategy_exists
```

Expected: FAIL to compile with `cannot find function market_maker_strategy_handle`.

- [ ] **Step 3: Implement helper and spawn wiring**

Update `spawn_strategy_tasks()` signature:

```rust
fn spawn_strategy_tasks(
    pair_strategy: PairArbitrageStrategy,
    pair_registration: StrategyRegistration,
    liquidity_reward: Option<LiquidityRewardStrategy>,
    market_maker: Option<MarketMakerStrategy>,
    order_tx: OrderSender,
    strategy_rx: StrategyReceiver,
) {
```

Add helper above `spawn_strategy_tasks()`:

```rust
fn market_maker_strategy_handle(
    market_maker: &MarketMakerStrategy,
) -> Option<(StrategyHandle, tokio::sync::mpsc::Receiver<StrategyEvent>)> {
    let registration = market_maker.registration().clone();
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    Some((
        StrategyHandle {
            name: registration.name.clone(),
            topics: registration.topics.clone(),
            related_tokens: registration.related_tokens.clone(),
            tx,
        },
        rx,
    ))
}
```

Update `spawn_strategy_tasks()` body after liquidity reward block and before dispatcher spawn:

```rust
if let Some(market_maker_strategy) = market_maker {
    if let Some((handle, market_maker_rx)) = market_maker_strategy_handle(&market_maker_strategy) {
        strategy_handles.push(handle);
        market_maker_strategy.spawn(market_maker_rx, order_tx.clone());
    }
}
```

Keep liquidity reward spawn unchanged except it should still receive an `order_tx` clone or owned sender safely:

```rust
liquidity_reward_strategy.spawn(liquidity_reward_rx, order_tx.clone());
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
cargo test tests::market_maker_strategy_handle_is_created_when_strategy_exists
```

Expected: PASS.

Also run:

```powershell
cargo test tests::build_market_maker_strategy_uses_selected_pool_entries
cargo test tests::build_strategy_registrations_includes_market_maker_when_present
cargo test strategies::market_maker::tests
```

Expected: all PASS.

- [ ] **Step 5: Review checkpoint**

Show:

```powershell
git diff -- src/main.rs src/strategies/market_maker.rs src/strategies/mod.rs
```

Stop for user review before Task 5.

---

### Task 5: Validate no order signals are emitted by the skeleton event loop

**Files:**
- Modify: `src/strategies/market_maker.rs`

- [ ] **Step 1: Write the no-order-signal async test**

Add this test to `src/strategies/market_maker.rs` tests:

```rust
#[tokio::test]
async fn spawn_consumes_events_without_emitting_order_signals() {
    let strategy = MarketMakerStrategy::from_pool_entries(vec![active_entry(
        "0xabc",
        "maker-token-1",
        "maker-token-2",
    )])
    .expect("market maker should build")
    .expect("non-empty pool should create strategy");
    let (event_tx, event_rx) = tokio::sync::mpsc::channel(8);
    let (order_tx, mut order_rx) = tokio::sync::mpsc::channel(8);

    let handle = strategy.spawn(event_rx, order_tx);
    drop(event_tx);
    handle.await.expect("market maker task should exit when event channel closes");

    assert!(order_rx.try_recv().is_err());
}
```

This test closes the event channel without sending a market event, proving the skeleton task exits cleanly and does not emit order signals.

- [ ] **Step 2: Run test to verify it passes with existing empty loop**

Run:

```powershell
cargo test strategies::market_maker::tests::spawn_consumes_events_without_emitting_order_signals
```

Expected: PASS if Task 1's empty event loop is correct. If it fails or hangs, fix only the event-loop exit behavior.

- [ ] **Step 3: Run market maker and startup focused tests**

Run:

```powershell
cargo test strategies::market_maker::tests
cargo test tests::build_market_maker_strategy_uses_selected_pool_entries
cargo test tests::build_strategy_registrations_includes_market_maker_when_present
cargo test tests::market_maker_strategy_handle_is_created_when_strategy_exists
```

Expected: all PASS.

- [ ] **Step 4: Review checkpoint**

Show:

```powershell
git diff -- src/strategies/market_maker.rs src/main.rs src/strategies/mod.rs
```

Stop for user review before Task 6.

---

### Task 6: Final validation and cleanup

**Files:**
- Validate: `src/strategies/market_maker.rs`
- Validate: `src/strategies/mod.rs`
- Validate: `src/main.rs`

- [ ] **Step 1: Run formatting check**

Run:

```powershell
cargo fmt --check
```

Expected: PASS.

If it fails only due to formatting, run:

```powershell
cargo fmt; if ($?) { cargo fmt --check }
```

Expected: PASS.

- [ ] **Step 2: Run focused tests**

Run:

```powershell
cargo test strategies::market_maker::tests
cargo test tests::build_market_maker_strategy_uses_selected_pool_entries
cargo test tests::build_strategy_registrations_includes_market_maker_when_present
cargo test tests::market_maker_strategy_handle_is_created_when_strategy_exists
```

Expected: all PASS.

- [ ] **Step 3: Run full test suite**

Run:

```powershell
cargo test
```

Expected: PASS. Existing unrelated warnings may remain; do not fix unrelated warnings in this task.

- [ ] **Step 4: Final diff review**

Show:

```powershell
git diff -- src/strategies/market_maker.rs src/strategies/mod.rs src/main.rs docs/superpowers/specs/2026-05-14-market-maker-strategy-skeleton-design.md docs/superpowers/plans/2026-05-14-market-maker-strategy-skeleton.md
```

Also show:

```powershell
git status --short
```

Expected: market maker skeleton files are changed/created; unrelated existing work may still appear in status and should not be included in any commit unless the user explicitly asks.

- [ ] **Step 5: Report completion and wait for review**

Report:

- Tests run and results.
- Confirm no `OrderSignal` is emitted by market maker skeleton.
- Confirm no config switch was added.
- Confirm DB empty pool means no market maker startup.
- Confirm no git commit was created.

Stop for user review.
