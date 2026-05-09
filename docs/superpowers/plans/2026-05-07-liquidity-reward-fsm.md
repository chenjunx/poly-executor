# Liquidity Reward FSM Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new `liquidity_reward_fsm` strategy using explicit state-machine flow, then switch main startup to use it while keeping the legacy `liquidity_reward.rs` as reference only.

**Architecture:** Add a new strategy file with the same public surface as the legacy strategy, but replace implicit boolean/Option state combinations with explicit `QuoteState`, `RiskState`, `TokenFsm`, and `Effect` types. Implementation proceeds in reviewable checkpoints; after each task, stop for human code review before continuing.

**Tech Stack:** Rust 2024, tokio mpsc, existing `Strategy` trait, existing `OrderSignal`/`StrategyEvent` protocol, existing SQLite stores via `OrderStore`/`MarketStore`.

---

## Files and Responsibilities

- Create: `src/strategies/liquidity_reward_fsm.rs`
  - New FSM strategy implementation.
  - Public API mirrors legacy `LiquidityRewardStrategy` where `main.rs` depends on it.
  - Strategy name remains `liquidity_reward` for order correlation compatibility.
- Modify: `src/strategies/mod.rs`
  - Add `pub mod liquidity_reward_fsm;` once the new file compiles.
- Modify: `src/main.rs`
  - Switch import from legacy strategy to FSM strategy using an alias so most main code remains unchanged.
- Keep unchanged: `src/strategies/liquidity_reward.rs`
  - Logic reference only; do not refactor it during this migration.
- Modify tests inside: `src/strategies/liquidity_reward_fsm.rs`
  - Add focused FSM tests in the new file.

---

## Review Protocol

After each task below:

1. Run the listed verification command.
2. Report exactly what changed and the verification result.
3. Stop and wait for human code review approval.
4. Do not start the next task until approval is given.

No git commit is required after each task unless the user explicitly asks. If committing, only commit files touched by the approved task.

---

### Task 1: Add FSM file skeleton and rule/registration construction

**Files:**
- Create: `src/strategies/liquidity_reward_fsm.rs`
- Modify: `src/strategies/mod.rs`

**Purpose:** Create a compiling strategy shell that can load CSV and DB-pool rules, build the same `StrategyRegistration`, and expose the same public constructor/configuration methods used by `main.rs`. The event loop can temporarily consume and ignore events; no order signals are emitted in this task.

- [ ] **Step 1: Create `src/strategies/liquidity_reward_fsm.rs` with copied public data types and skeleton strategy**

Add these elements:

```rust
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use polymarket_client_sdk_v2::types::Decimal;
use tracing::warn;

use crate::{
    notification::Notifier,
    storage::{ActiveRewardMarketPoolEntry, MarketStore, OrderStore},
    strategies::liquidity_reward::{LiquidityRewardRestoreState, LiquidityRewardRule},
    strategy::{OrderSignal, Strategy, StrategyEvent, StrategyRegistration, TopicRegistration},
    tick_size::TickSizeMap,
};

const DEFAULT_TOPIC: &str = "liquidity_reward";

pub struct LiquidityRewardFsmStrategy {
    rules: Arc<HashMap<String, LiquidityRewardRule>>,
    registration: Arc<StrategyRegistration>,
    restored_states: HashMap<String, LiquidityRewardRestoreState>,
    order_store: Option<OrderStore>,
    market_store: Option<MarketStore>,
    simulation_enabled: bool,
    tick_size_map: TickSizeMap,
    notifier: Option<Notifier>,
    balance_cooldown: Duration,
}
```

Implement public methods with legacy-compatible signatures:

```rust
impl LiquidityRewardFsmStrategy {
    pub fn rules(&self) -> impl Iterator<Item = (&String, &LiquidityRewardRule)> {
        self.rules.iter()
    }

    pub fn with_restore_state(
        mut self,
        restored_states: HashMap<String, LiquidityRewardRestoreState>,
        order_store: Option<OrderStore>,
        simulation_enabled: bool,
        tick_size_map: TickSizeMap,
    ) -> Self {
        let restored_count = restored_states.len();
        self.restored_states = restored_states
            .into_iter()
            .filter(|(token, _)| self.rules.contains_key(token))
            .collect();
        let skipped_count = restored_count.saturating_sub(self.restored_states.len());
        if skipped_count > 0 {
            warn!(
                restored_count,
                skipped_count,
                active_rule_count = self.rules.len(),
                "liquidity_reward_fsm 跳过当前规则外的历史恢复状态"
            );
        }
        self.order_store = order_store;
        self.simulation_enabled = simulation_enabled;
        self.tick_size_map = tick_size_map;
        self
    }

    pub fn with_market_store(mut self, market_store: MarketStore) -> Self {
        self.market_store = Some(market_store);
        self
    }

    pub fn with_notifier(mut self, notifier: Option<Notifier>) -> Self {
        self.notifier = notifier;
        self
    }

    pub fn with_balance_cooldown(mut self, cooldown: Duration) -> Self {
        self.balance_cooldown = cooldown;
        self
    }
}
```

- [ ] **Step 2: Implement `from_csv`, `from_pool_entries`, and `from_rules`**

Use the same CSV column semantics as legacy:

```rust
impl LiquidityRewardFsmStrategy {
    pub fn from_csv(csv_file: &str) -> anyhow::Result<Option<Self>> {
        let csv_path = resolve_csv_path(csv_file);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path(&csv_path)
            .map_err(|e| anyhow::anyhow!("无法打开 {}: {}", csv_path, e))?;

        let mut rules = Vec::new();
        for result in reader.records() {
            let record = result?;
            if record.len() < 3 {
                continue;
            }
            let token1 = record[0].trim();
            let token2_raw = record[1].trim();
            if token1.is_empty() {
                continue;
            }
            let token2 = if token2_raw.is_empty() {
                None
            } else {
                Some(token2_raw.to_string())
            };
            let topic: Arc<str> = record
                .get(2)
                .filter(|s| !s.trim().is_empty())
                .map(|s| Arc::from(s.trim()))
                .unwrap_or_else(|| Arc::from(DEFAULT_TOPIC));
            let reward_min_orders = record.get(3).and_then(|v| v.trim().parse::<u32>().ok());
            let reward_max_spread_cents = record.get(4).and_then(|v| v.trim().parse::<f64>().ok());
            let reward_min_size = record.get(5).and_then(|v| v.trim().parse::<f64>().ok());
            let reward_daily_pool = record.get(6).and_then(|v| v.trim().parse::<f64>().ok());
            let fixed_price = record
                .get(7)
                .map(|v| matches!(v.trim(), "true" | "1" | "yes"))
                .unwrap_or(false);

            rules.push(LiquidityRewardRule {
                topic,
                token1: token1.to_string(),
                token2,
                reward_min_orders,
                reward_max_spread_cents,
                reward_min_size,
                reward_daily_pool,
                fixed_price,
                condition_id: None,
                pool_version: None,
            });
        }
        Self::from_rules(rules)
    }

    pub fn from_pool_entries(
        entries: Vec<ActiveRewardMarketPoolEntry>,
    ) -> anyhow::Result<Option<Self>> {
        let rules = entries
            .into_iter()
            .map(|entry| LiquidityRewardRule {
                topic: Arc::from(DEFAULT_TOPIC),
                token1: entry.token1,
                token2: Some(entry.token2),
                reward_min_orders: None,
                reward_max_spread_cents: entry
                    .rewards_max_spread
                    .as_deref()
                    .and_then(|v| v.parse::<f64>().ok()),
                reward_min_size: entry
                    .rewards_min_size
                    .as_deref()
                    .and_then(|v| v.parse::<f64>().ok()),
                reward_daily_pool: entry
                    .market_daily_reward
                    .as_deref()
                    .and_then(|v| v.parse::<f64>().ok()),
                fixed_price: false,
                condition_id: Some(entry.condition_id),
                pool_version: entry.pool_version,
            })
            .collect();
        Self::from_rules(rules)
    }

    pub fn from_rules(rules: Vec<LiquidityRewardRule>) -> anyhow::Result<Option<Self>> {
        if rules.is_empty() {
            return Ok(None);
        }
        let (rules, registration) = build_rules_and_registration(rules)?;
        Ok(Some(Self {
            rules: Arc::new(rules),
            registration: Arc::new(registration),
            restored_states: HashMap::new(),
            order_store: None,
            market_store: None,
            simulation_enabled: false,
            tick_size_map: TickSizeMap::default(),
            notifier: None,
            balance_cooldown: Duration::from_secs(60),
        }))
    }
}
```

- [ ] **Step 3: Implement registration helpers**

```rust
fn build_rules_and_registration(
    rules: Vec<LiquidityRewardRule>,
) -> anyhow::Result<(HashMap<String, LiquidityRewardRule>, StrategyRegistration)> {
    let mut by_token = HashMap::new();
    let mut topic_tokens: HashMap<Arc<str>, Vec<String>> = HashMap::new();
    let mut related_tokens = HashSet::new();

    for rule in rules {
        if rule.token1.is_empty() {
            continue;
        }
        let topic = rule.topic.clone();
        related_tokens.insert(rule.token1.clone());
        topic_tokens
            .entry(topic.clone())
            .or_default()
            .push(rule.token1.clone());
        by_token.insert(rule.token1.clone(), rule.clone());

        if let Some(token2) = rule.token2.as_ref().filter(|token| !token.is_empty()) {
            related_tokens.insert(token2.clone());
            topic_tokens
                .entry(topic)
                .or_default()
                .push(token2.clone());
            by_token.insert(token2.clone(), rule);
        }
    }

    if by_token.is_empty() {
        return Ok((by_token, empty_registration()));
    }

    let mut topics: Vec<Arc<str>> = topic_tokens.keys().cloned().collect();
    topics.sort();

    let topic_token_regs = topic_tokens
        .into_iter()
        .map(|(topic, mut tokens)| {
            tokens.sort();
            tokens.dedup();
            TopicRegistration {
                topic,
                tokens: Arc::<[String]>::from(tokens),
            }
        })
        .collect::<Vec<_>>();

    let mut related_tokens = related_tokens.into_iter().collect::<Vec<_>>();
    related_tokens.sort();

    Ok((
        by_token,
        StrategyRegistration {
            name: Arc::from("liquidity_reward"),
            topics: Arc::<[Arc<str>]>::from(topics),
            topic_tokens: Arc::<[TopicRegistration]>::from(topic_token_regs),
            related_tokens: Arc::<[String]>::from(related_tokens),
        },
    ))
}

fn empty_registration() -> StrategyRegistration {
    StrategyRegistration {
        name: Arc::from("liquidity_reward"),
        topics: Arc::<[Arc<str>]>::from(Vec::<Arc<str>>::new()),
        topic_tokens: Arc::<[TopicRegistration]>::from(Vec::<TopicRegistration>::new()),
        related_tokens: Arc::<[String]>::from(Vec::<String>::new()),
    }
}

fn resolve_csv_path(csv_file: &str) -> String {
    let csv_path = Path::new(csv_file);
    if csv_path.is_absolute() || csv_path.exists() {
        csv_file.to_string()
    } else if let Ok(mut exe_path) = std::env::current_exe() {
        exe_path.pop();
        exe_path.push(csv_file);
        exe_path.to_string_lossy().to_string()
    } else {
        csv_file.to_string()
    }
}
```

- [ ] **Step 4: Implement `Strategy` with a no-op event loop**

```rust
impl Strategy for LiquidityRewardFsmStrategy {
    fn name(&self) -> &str {
        "liquidity_reward"
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
            while rx.recv().await.is_some() {}
        })
    }
}
```

- [ ] **Step 5: Export module**

Modify `src/strategies/mod.rs`:

```rust
pub mod liquidity_reward_fsm;
pub mod pair_arbitrage;
pub mod liquidity_reward;
```

- [ ] **Step 6: Run compile check**

Run:

```bash
cargo check
```

Expected: pass, with no unresolved import/type errors related to `liquidity_reward_fsm`.

- [ ] **Review checkpoint:** Stop for human code review.

---

### Task 2: Add FSM state types and pure transition tests for quote lifecycle

**Files:**
- Modify: `src/strategies/liquidity_reward_fsm.rs`

**Purpose:** Add explicit state types and pure transition helpers for initial quote, cancel wait, and replacement. This task still does not switch `main.rs` or emit orders from the live event loop.

- [ ] **Step 1: Add FSM state and effect types**

Add near the top of `liquidity_reward_fsm.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
struct ActiveOrder {
    order_id: String,
    price: Decimal,
    order_size: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
struct PendingReplacement {
    order_id: String,
    price: Decimal,
    order_size: Decimal,
    mid: Decimal,
}

#[derive(Debug, Clone, PartialEq)]
enum CancelNext {
    Wait,
    Replace(PendingReplacement),
}

#[derive(Debug, Clone, PartialEq)]
enum HaltReason {
    Fill,
    PoolRemoval,
}

#[derive(Debug, Clone, PartialEq)]
enum QuoteState {
    Idle,
    Active { order: ActiveOrder },
    Canceling { order: ActiveOrder, next: CancelNext },
    Halted {
        active: Option<ActiveOrder>,
        cancel_requested: bool,
        reason: HaltReason,
    },
}

#[derive(Debug, Clone, PartialEq)]
enum RiskState {
    Normal,
    PoolRemovalPending {
        position_size: Option<Decimal>,
        submitted: bool,
    },
}

#[derive(Debug, Clone, Default)]
struct MarketSnapshot {
    mid: Option<Decimal>,
    best_bid: Option<Decimal>,
    best_ask: Option<Decimal>,
    bids: Option<Arc<std::collections::BTreeMap<u16, u32>>>,
}

#[derive(Debug, Clone)]
struct TokenFsm {
    token: String,
    topic: Arc<str>,
    quote: QuoteState,
    risk: RiskState,
    market: MarketSnapshot,
}

#[derive(Debug, Clone, PartialEq)]
enum Effect {
    PlaceBuy {
        token: String,
        topic: Arc<str>,
        local_order_id: String,
        mid: Decimal,
        price: Decimal,
        order_size: Decimal,
    },
    CancelBuy {
        token: String,
        topic: Arc<str>,
        local_order_id: String,
    },
}
```

- [ ] **Step 2: Add constructors and quote transition helpers**

```rust
impl TokenFsm {
    fn empty(token: String, topic: Arc<str>) -> Self {
        Self {
            token,
            topic,
            quote: QuoteState::Idle,
            risk: RiskState::Normal,
            market: MarketSnapshot::default(),
        }
    }

    fn place_buy(
        &mut self,
        local_order_id: String,
        mid: Decimal,
        price: Decimal,
        order_size: Decimal,
    ) -> Vec<Effect> {
        self.quote = QuoteState::Active {
            order: ActiveOrder {
                order_id: local_order_id.clone(),
                price,
                order_size,
            },
        };
        vec![Effect::PlaceBuy {
            token: self.token.clone(),
            topic: self.topic.clone(),
            local_order_id,
            mid,
            price,
            order_size,
        }]
    }

    fn cancel_wait(&mut self) -> Vec<Effect> {
        let QuoteState::Active { order } = self.quote.clone() else {
            return Vec::new();
        };
        self.quote = QuoteState::Canceling {
            order: order.clone(),
            next: CancelNext::Wait,
        };
        vec![Effect::CancelBuy {
            token: self.token.clone(),
            topic: self.topic.clone(),
            local_order_id: order.order_id,
        }]
    }

    fn stage_replacement(&mut self, pending: PendingReplacement) -> Vec<Effect> {
        let QuoteState::Active { order } = self.quote.clone() else {
            return Vec::new();
        };
        self.quote = QuoteState::Canceling {
            order: order.clone(),
            next: CancelNext::Replace(pending),
        };
        vec![Effect::CancelBuy {
            token: self.token.clone(),
            topic: self.topic.clone(),
            local_order_id: order.order_id,
        }]
    }

    fn on_cancel_confirmed(&mut self) -> Vec<Effect> {
        let QuoteState::Canceling { next, .. } = self.quote.clone() else {
            return Vec::new();
        };
        match next {
            CancelNext::Wait => {
                self.quote = QuoteState::Idle;
                Vec::new()
            }
            CancelNext::Replace(pending) => {
                self.place_buy(
                    pending.order_id,
                    pending.mid,
                    pending.price,
                    pending.order_size,
                )
            }
        }
    }

    fn on_cancel_failed(&mut self) {
        if let QuoteState::Canceling { order, .. } = self.quote.clone() {
            self.quote = QuoteState::Active { order };
        }
    }
}
```

- [ ] **Step 3: Add unit tests for quote lifecycle**

Inside `#[cfg(test)] mod tests`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn dec(value: f64) -> Decimal {
        Decimal::try_from(value).unwrap()
    }

    fn token_fsm() -> TokenFsm {
        TokenFsm::empty("token1".to_string(), Arc::from("liquidity_reward"))
    }

    #[test]
    fn idle_place_buy_moves_to_active_and_emits_place() {
        let mut fsm = token_fsm();
        let effects = fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));

        assert_eq!(
            fsm.quote,
            QuoteState::Active {
                order: ActiveOrder {
                    order_id: "buy-1".to_string(),
                    price: dec(0.49),
                    order_size: dec(100.0),
                }
            }
        );
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            &effects[0],
            Effect::PlaceBuy { token, local_order_id, price, order_size, .. }
                if token == "token1"
                    && local_order_id == "buy-1"
                    && *price == dec(0.49)
                    && *order_size == dec(100.0)
        ));
    }

    #[test]
    fn active_cancel_wait_moves_to_canceling_and_emits_cancel() {
        let mut fsm = token_fsm();
        fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));

        let effects = fsm.cancel_wait();

        assert!(matches!(
            fsm.quote,
            QuoteState::Canceling { next: CancelNext::Wait, .. }
        ));
        assert!(matches!(
            &effects[0],
            Effect::CancelBuy { token, local_order_id, .. }
                if token == "token1" && local_order_id == "buy-1"
        ));
    }

    #[test]
    fn cancel_confirmed_with_replacement_places_pending_order() {
        let mut fsm = token_fsm();
        fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));
        fsm.stage_replacement(PendingReplacement {
            order_id: "buy-2".to_string(),
            price: dec(0.48),
            order_size: dec(100.0),
            mid: dec(0.5),
        });

        let effects = fsm.on_cancel_confirmed();

        assert_eq!(
            fsm.quote,
            QuoteState::Active {
                order: ActiveOrder {
                    order_id: "buy-2".to_string(),
                    price: dec(0.48),
                    order_size: dec(100.0),
                }
            }
        );
        assert!(matches!(
            &effects[0],
            Effect::PlaceBuy { local_order_id, price, .. }
                if local_order_id == "buy-2" && *price == dec(0.48)
        ));
    }

    #[test]
    fn cancel_failed_restores_active_order() {
        let mut fsm = token_fsm();
        fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));
        fsm.cancel_wait();

        fsm.on_cancel_failed();

        assert_eq!(
            fsm.quote,
            QuoteState::Active {
                order: ActiveOrder {
                    order_id: "buy-1".to_string(),
                    price: dec(0.49),
                    order_size: dec(100.0),
                }
            }
        );
    }
}
```

- [ ] **Step 4: Run targeted tests**

Run:

```bash
cargo test strategies::liquidity_reward_fsm::tests -- --nocapture
```

Expected: four FSM quote lifecycle tests pass.

- [ ] **Review checkpoint:** Stop for human code review.

---

### Task 3: Add Halted and pool-removal risk transitions

**Files:**
- Modify: `src/strategies/liquidity_reward_fsm.rs`

**Purpose:** Model halt and pool-removal unwind intent explicitly without touching the live event loop yet.

- [ ] **Step 1: Add halt transition helpers**

```rust
impl TokenFsm {
    fn halt(&mut self, reason: HaltReason, filled_local_order_id: Option<&str>) -> Vec<Effect> {
        let active = match self.quote.clone() {
            QuoteState::Active { order } => Some(order),
            QuoteState::Canceling { order, .. } => Some(order),
            QuoteState::Halted { active, .. } => active,
            QuoteState::Idle => None,
        };

        let cancel_effect = active
            .as_ref()
            .filter(|order| filled_local_order_id != Some(order.order_id.as_str()))
            .map(|order| Effect::CancelBuy {
                token: self.token.clone(),
                topic: self.topic.clone(),
                local_order_id: order.order_id.clone(),
            });

        self.quote = QuoteState::Halted {
            active,
            cancel_requested: cancel_effect.is_some(),
            reason,
        };

        cancel_effect.into_iter().collect()
    }

    fn retry_halted_cancel(&mut self) -> Vec<Effect> {
        let QuoteState::Halted {
            active: Some(order),
            cancel_requested,
            reason,
        } = self.quote.clone()
        else {
            return Vec::new();
        };
        if cancel_requested {
            return Vec::new();
        }
        self.quote = QuoteState::Halted {
            active: Some(order.clone()),
            cancel_requested: true,
            reason,
        };
        vec![Effect::CancelBuy {
            token: self.token.clone(),
            topic: self.topic.clone(),
            local_order_id: order.order_id,
        }]
    }

    fn on_halted_cancel_failed(&mut self) {
        if let QuoteState::Halted { active, reason, .. } = self.quote.clone() {
            self.quote = QuoteState::Halted {
                active,
                cancel_requested: false,
                reason,
            };
        }
    }

    fn mark_pool_removal_unwind(&mut self) {
        match self.risk {
            RiskState::PoolRemovalPending { submitted: true, .. } => {}
            _ => {
                self.risk = RiskState::PoolRemovalPending {
                    position_size: None,
                    submitted: false,
                };
            }
        }
    }

    fn update_pool_removal_position(&mut self, size: Decimal) {
        if size <= Decimal::ZERO {
            self.risk = RiskState::Normal;
            return;
        }
        if let RiskState::PoolRemovalPending { submitted, .. } = self.risk {
            self.risk = RiskState::PoolRemovalPending {
                position_size: Some(size),
                submitted,
            };
        }
    }
}
```

- [ ] **Step 2: Add halt/risk tests**

Add tests:

```rust
#[test]
fn halt_active_keeps_active_and_emits_cancel() {
    let mut fsm = token_fsm();
    fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));

    let effects = fsm.halt(HaltReason::PoolRemoval, None);

    assert_eq!(effects.len(), 1);
    assert!(matches!(
        &effects[0],
        Effect::CancelBuy { local_order_id, .. } if local_order_id == "buy-1"
    ));
    assert!(matches!(
        fsm.quote,
        QuoteState::Halted { active: Some(_), cancel_requested: true, reason: HaltReason::PoolRemoval }
    ));
}

#[test]
fn halt_filled_order_does_not_emit_cancel_but_remembers_halted() {
    let mut fsm = token_fsm();
    fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));

    let effects = fsm.halt(HaltReason::Fill, Some("buy-1"));

    assert!(effects.is_empty());
    assert!(matches!(
        fsm.quote,
        QuoteState::Halted { active: Some(_), cancel_requested: false, reason: HaltReason::Fill }
    ));
}

#[test]
fn halted_cancel_failed_reopens_retry_window() {
    let mut fsm = token_fsm();
    fsm.place_buy("buy-1".to_string(), dec(0.5), dec(0.49), dec(100.0));
    fsm.halt(HaltReason::PoolRemoval, None);

    fsm.on_halted_cancel_failed();
    let retry = fsm.retry_halted_cancel();

    assert_eq!(retry.len(), 1);
    assert!(matches!(
        &retry[0],
        Effect::CancelBuy { local_order_id, .. } if local_order_id == "buy-1"
    ));
}

#[test]
fn pool_removal_position_size_is_recorded_until_submitted() {
    let mut fsm = token_fsm();
    fsm.mark_pool_removal_unwind();
    fsm.update_pool_removal_position(dec(7.5));

    assert_eq!(
        fsm.risk,
        RiskState::PoolRemovalPending {
            position_size: Some(dec(7.5)),
            submitted: false,
        }
    );
}
```

- [ ] **Step 3: Run targeted tests**

Run:

```bash
cargo test strategies::liquidity_reward_fsm::tests -- --nocapture
```

Expected: all FSM tests pass.

- [ ] **Review checkpoint:** Stop for human code review.

---

### Task 4: Add effect execution and persistence mapping

**Files:**
- Modify: `src/strategies/liquidity_reward_fsm.rs`

**Purpose:** Convert FSM effects into existing `OrderSignal`s and map FSM state to existing SQLite state tables. Live event loop still does not perform quote decisions.

- [ ] **Step 1: Extend Effect with MarketSell and PersistPoolHalt**

Add variants:

```rust
enum Effect {
    PlaceBuy { /* existing fields */ },
    CancelBuy { /* existing fields */ },
    MarketSell {
        token: String,
        topic: Arc<str>,
        local_order_id: String,
        price: Decimal,
        order_size: Decimal,
    },
    PersistPoolHalt {
        condition_id: String,
        pool_version: u64,
        reason: &'static str,
    },
}
```

- [ ] **Step 2: Add `execute_effects`**

```rust
fn execute_effects(
    effects: Vec<Effect>,
    simulated: bool,
    order_tx: &tokio::sync::mpsc::Sender<OrderSignal>,
    market_store: Option<&MarketStore>,
) {
    for effect in effects {
        match effect {
            Effect::PlaceBuy {
                token,
                topic,
                local_order_id,
                mid,
                price,
                order_size,
            } => {
                if let Err(error) = order_tx.try_send(OrderSignal::LiquidityRewardPlace {
                    strategy: Arc::from("liquidity_reward"),
                    topic,
                    token,
                    mid,
                    side: crate::strategy::QuoteSide::Buy,
                    price,
                    order_size,
                    local_order_id,
                    simulated,
                }) {
                    warn!(error = %error, "liquidity_reward_fsm 发送挂单信号失败");
                }
            }
            Effect::CancelBuy {
                token,
                topic,
                local_order_id,
            } => {
                if let Err(error) = order_tx.try_send(OrderSignal::LiquidityRewardCancel {
                    strategy: Arc::from("liquidity_reward"),
                    topic,
                    token,
                    side: crate::strategy::QuoteSide::Buy,
                    active_local_order_id: local_order_id,
                    simulated,
                }) {
                    warn!(error = %error, "liquidity_reward_fsm 发送撤单信号失败");
                }
            }
            Effect::MarketSell {
                token,
                topic,
                local_order_id,
                price,
                order_size,
            } => {
                if let Err(error) = order_tx.try_send(OrderSignal::LiquidityRewardMarketSell {
                    strategy: Arc::from("liquidity_reward"),
                    topic,
                    token,
                    price,
                    order_size,
                    local_order_id,
                    simulated,
                }) {
                    warn!(error = %error, "liquidity_reward_fsm 发送清仓卖单信号失败");
                }
            }
            Effect::PersistPoolHalt {
                condition_id,
                pool_version,
                reason,
            } => {
                let Some(store) = market_store else { continue };
                let halted_at_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                if let Err(error) = store.halt_liquidity_reward_pool_entry(
                    &condition_id,
                    pool_version,
                    reason,
                    halted_at_ms,
                ) {
                    warn!(condition_id = %condition_id, pool_version, error = %error, "liquidity_reward_fsm 写入池子 halt 状态失败");
                }
            }
        }
    }
}
```

- [ ] **Step 3: Add persistence mapping**

```rust
fn persist_token_state(order_store: Option<&OrderStore>, fsm: &TokenFsm) {
    let Some(store) = order_store else { return };

    if let Err(error) = store.upsert_liquidity_reward_shared_state(
        &fsm.token,
        fsm.topic.as_ref(),
        fsm.market.mid,
        fsm.market.best_bid,
        fsm.market.best_ask,
        Decimal::ZERO,
    ) {
        warn!(token = %fsm.token, error = %error, "liquidity_reward_fsm 持久化共享策略状态失败");
    }

    let (active, pending, cancel_requested) = match &fsm.quote {
        QuoteState::Idle => (None, None, false),
        QuoteState::Active { order } => (Some(order), None, false),
        QuoteState::Canceling { order, next } => {
            let pending = match next {
                CancelNext::Wait => None,
                CancelNext::Replace(pending) => Some(pending),
            };
            (Some(order), pending, true)
        }
        QuoteState::Halted { active, cancel_requested, .. } => {
            (active.as_ref(), None, *cancel_requested)
        }
    };

    if let Err(error) = store.upsert_liquidity_reward_side_state(
        &fsm.token,
        crate::strategy::QuoteSide::Buy,
        active.map(|o| o.order_id.as_str()),
        pending.map(|p| p.order_id.as_str()),
        pending.map(|p| p.price),
        pending.map(|p| p.order_size),
        pending.map(|p| p.mid),
        None,
        cancel_requested,
    ) {
        warn!(token = %fsm.token, error = %error, "liquidity_reward_fsm 持久化策略状态失败");
    }
}
```

- [ ] **Step 4: Run compile and FSM tests**

Run:

```bash
cargo check
cargo test strategies::liquidity_reward_fsm::tests -- --nocapture
```

Expected: compile succeeds and FSM tests pass.

- [ ] **Review checkpoint:** Stop for human code review.

---

### Task 5: Wire live Market and OrderStatus events into FSM

**Files:**
- Modify: `src/strategies/liquidity_reward_fsm.rs`

**Purpose:** Implement live quote placement, cancel wait, replacement, and order lifecycle handling using FSM types.

- [ ] **Step 1: Port only the quote-decision dependencies from legacy**

Copy the minimum required legacy helpers into `liquidity_reward_fsm.rs`:

- `QuoteAction`
- `QuoteDecision`
- `quote_decision`
- `price_to_decimal` or direct Decimal conversion helpers
- `scaled_price`
- functions required by `quote_decision`, including own-bid subtraction and tick snapping

Do not copy halt/unwind helpers from legacy in this task.

- [ ] **Step 2: Implement market event handling in `spawn`**

In `StrategyEvent::Market(event)`:

```rust
let Some(rule) = rules.get(event.asset_id.as_ref()) else { continue };
let token = event.asset_id.as_ref().to_string();
let fsm = fsms
    .entry(token.clone())
    .or_insert_with(|| TokenFsm::empty(token.clone(), rule.topic.clone()));

let bid = Decimal::from(event.book.best_bid_price) / Decimal::from(PRICE_SCALE);
let ask = Decimal::from(event.book.best_ask_price) / Decimal::from(PRICE_SCALE);
let mid = (bid + ask) / Decimal::TWO;
fsm.market.mid = Some(mid);
fsm.market.best_bid = Some(bid);
fsm.market.best_ask = Some(ask);
fsm.market.bids = Some(event.book.bids.clone());

let mut effects = fsm.retry_halted_cancel();
if matches!(fsm.quote, QuoteState::Halted { .. }) {
    execute_effects(effects, simulation_enabled, &order_tx, market_store.as_ref());
    persist_token_state(order_store.as_ref(), fsm);
    continue;
}

// calculate order_size/spread, call quote_decision, then call fsm.place_buy/cancel_wait/stage_replacement.
execute_effects(effects, simulation_enabled, &order_tx, market_store.as_ref());
persist_token_state(order_store.as_ref(), fsm);
```

- [ ] **Step 3: Implement `OrderStatus` lifecycle handling**

For non-unwind buy orders:

```rust
match status {
    "filled" => halt_pair_fsm(...),
    "canceled" | "rejected" => {
        let effects = fsm.on_cancel_confirmed();
        execute_effects(effects, simulation_enabled, &order_tx, market_store.as_ref());
        persist_token_state(order_store.as_ref(), fsm);
    }
    "failed" => {
        fsm.on_cancel_failed();
        persist_token_state(order_store.as_ref(), fsm);
    }
    "cancel_failed" => {
        fsm.on_halted_cancel_failed();
        persist_token_state(order_store.as_ref(), fsm);
    }
    _ => {}
}
```

- [ ] **Step 4: Add integration-style tests using strategy spawn**

Add tests:

```rust
#[tokio::test]
async fn fsm_strategy_places_initial_quote_on_market() { ... }

#[tokio::test]
async fn fsm_strategy_does_not_quote_when_halted() { ... }
```

Use existing test style from legacy: create `(event_tx, event_rx)`, `(order_tx, order_rx)`, spawn strategy, send `StrategyEvent::Market`, assert `OrderSignal::LiquidityRewardPlace`.

- [ ] **Step 5: Run targeted tests**

Run:

```bash
cargo test strategies::liquidity_reward_fsm::tests -- --nocapture
cargo check
```

Expected: FSM tests pass and compile succeeds.

- [ ] **Review checkpoint:** Stop for human code review.

---

### Task 6: Wire OrderFill, RewardPoolRemoval, Positions, and unwind behavior

**Files:**
- Modify: `src/strategies/liquidity_reward_fsm.rs`

**Purpose:** Complete risk flow: fills halt pair, pool removal halts pair, positions drive pool-removal unwind, unwind order statuses update retry/manual handling.

- [ ] **Step 1: Add PendingUnwind and unwind effect transitions**

Port legacy `PendingUnwind`, `submit_unwind_for_token`, `submit_remaining_unwind`, `schedule_unwind_retry_if_needed`, and related helpers into FSM style. `submit_unwind_for_token` should return `Effect::MarketSell` and update `pending_unwinds` only when the effect is accepted by transition.

- [ ] **Step 2: Add pair halt coordinator**

Implement:

```rust
fn halt_pair_fsm(
    trigger_token: &str,
    rules: &HashMap<String, LiquidityRewardRule>,
    fsms: &mut HashMap<String, TokenFsm>,
    reason: HaltReason,
    unwind_size: Option<Decimal>,
    filled_local_order_id: Option<&str>,
    tick_size_map: &TickSizeMap,
) -> Vec<Effect> { ... }
```

It must:

- Find the rule for `trigger_token`.
- Halt trigger token and paired token.
- Emit cancel effects for active orders that are not the filled order.
- Emit `PersistPoolHalt` when rule has `condition_id` and `pool_version`.
- Emit unwind sell effect only for `trigger_token` when `unwind_size` is provided.

- [ ] **Step 3: Wire `OrderFill`**

Rules:

- If fill is for pending unwind, update matched size and do not halt.
- If fill is buy side and strategy is `liquidity_reward`, halt pair and unwind `delta_size`.
- If pool removal risk is already pending, wait for positions snapshot instead of direct delta unwind.

- [ ] **Step 4: Wire `RewardPoolRemoval` and `Positions`**

RewardPoolRemoval:

- Find any known token from `token1/token2`.
- Halt both tokens.
- Mark both token FSMs with `RiskState::PoolRemovalPending`.
- If latest positions exist, update each token's pool removal position.

Positions:

- Cache latest snapshot.
- For each changed token with `RiskState::PoolRemovalPending`, update size.
- If size > 0 and market has bid/mid, emit `MarketSell`.

- [ ] **Step 5: Add tests for risk flows**

Add tests equivalent to legacy behavior:

- `reward_pool_removal_halts_pair_cancels_orders_and_unwinds_positions`
- `reward_pool_removal_unwinds_late_position_with_late_market`
- `reward_pool_removal_unwinds_both_positive_positions`
- `repeated_reward_pool_removal_does_not_duplicate_unwind`
- `buy_fill_halts_pair_and_cancels_other_side`

- [ ] **Step 6: Run targeted tests**

Run:

```bash
cargo test strategies::liquidity_reward_fsm::tests -- --nocapture
cargo check
```

Expected: all FSM tests pass and compile succeeds.

- [ ] **Review checkpoint:** Stop for human code review.

---

### Task 7: Switch main strategy import to FSM implementation

**Files:**
- Modify: `src/main.rs`
- Keep unchanged: `src/strategies/liquidity_reward.rs`

**Purpose:** Use FSM strategy in production flow while preserving legacy file for reference.

- [ ] **Step 1: Change import in `src/main.rs`**

Replace:

```rust
use strategies::liquidity_reward::{
    LiquidityRewardRestoreState, LiquidityRewardRule, LiquidityRewardStrategy,
};
```

With:

```rust
use strategies::liquidity_reward::LiquidityRewardRestoreState;
use strategies::liquidity_reward_fsm::{
    LiquidityRewardFsmStrategy as LiquidityRewardStrategy, LiquidityRewardRule,
};
```

If `LiquidityRewardRule` remains imported from legacy for compatibility, use:

```rust
use strategies::liquidity_reward::{LiquidityRewardRestoreState, LiquidityRewardRule};
use strategies::liquidity_reward_fsm::LiquidityRewardFsmStrategy as LiquidityRewardStrategy;
```

Choose the version that matches Task 1 ownership of the public rule type.

- [ ] **Step 2: Run full compile check**

Run:

```bash
cargo check
```

Expected: compile succeeds.

- [ ] **Step 3: Run focused strategy tests**

Run:

```bash
cargo test strategies::liquidity_reward_fsm::tests -- --nocapture
```

Expected: all FSM strategy tests pass.

- [ ] **Review checkpoint:** Stop for human code review.

---

### Task 8: Final verification and cleanup

**Files:**
- Modify as needed only if tests reveal compile/test issues.

**Purpose:** Verify the repository after FSM switch and remove only unused imports introduced by the migration. Do not delete legacy strategy in this task.

- [ ] **Step 1: Run full test suite**

Run:

```bash
cargo test
```

Expected: all tests pass. If legacy tests fail because they target old strategy internals, leave legacy file unchanged and adjust only tests that are invalid due to main switch.

- [ ] **Step 2: Run compile check again**

Run:

```bash
cargo check
```

Expected: pass.

- [ ] **Step 3: Inspect git diff**

Run:

```bash
git diff -- src/strategies/liquidity_reward_fsm.rs src/strategies/mod.rs src/main.rs
```

Expected: diff only contains FSM strategy addition, module export, and main import switch.

- [ ] **Review checkpoint:** Stop for final human code review.

---

## Self-Review Notes

- Spec coverage: The plan creates a new FSM file, keeps legacy unchanged, switches main directly after the FSM is ready, and requires review after every task.
- Placeholder scan: No TBD/TODO placeholders remain in task instructions. Later tasks include exact function names and behavior, while allowing code copied from legacy only where explicitly listed.
- Type consistency: `LiquidityRewardFsmStrategy`, `TokenFsm`, `QuoteState`, `RiskState`, `Effect`, and public strategy API names are consistent across tasks.
- Scope control: This plan does not include reliable channel delivery, pool-selection rebuild semantics, or deleting legacy strategy. Those remain separate follow-up work.
