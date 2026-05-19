# Token Topic Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Simplify market data routing so each Polymarket token is also the internal broadcast topic, and strategies subscribe directly to the tokens they care about.

**Architecture:** Keep the existing WebSocket subscription and `market::run` reducer, but replace logical topic fanout with direct token-keyed broadcast channels. `StrategyRegistration.topics` remains temporarily as the subscription key list, but its values become token IDs; this keeps the diff smaller while changing routing semantics. `tick_store_enabled` / `raw_store_enabled` recorder functionality is explicitly out of scope for this plan and must remain wired as-is.

**Tech Stack:** Rust, Tokio broadcast/mpsc, existing `StrategyRegistration`, `MarketEvent`, `MarketAssetEvent`, `CleanOrderbook`, cargo test/check.

---

## Files

- Modify: `src/strategies/strategy.rs`
  - Change routing helpers so broadcast keys are tokens.
  - Make `merge_topic_tokens` produce token self-mappings: `token -> [token]`.
  - Make `build_token_topics` produce identity mappings: `token -> [token]`.
  - Keep function/type names for this first pass to avoid large rename churn.
- Modify: `src/strategies/pair_arbitrage.rs`
  - Change `PairArbitrageStrategy` internal index from `pairs_by_topic` to `pairs_by_token`.
  - Register each related token as a subscription topic.
  - Check only pairs containing the updated token.
- Modify: `src/strategies/market_maker.rs`
  - Register each related token as a subscription topic instead of the fixed `market_maker` topic.
  - Update tests and log expectations so `MarketEvent.topic == MarketEvent.asset_id`.
- Modify: `src/market.rs`
  - Keep recorder parameters unchanged.
  - Ensure events for `asset_id` publish to exactly the `asset_id` broadcast topic.
  - Update tests that previously expected one token to fan out to multiple logical topics.
- Modify: `src/main.rs`
  - Keep `build_routing` shape but route through token self-mappings.
  - Update affected tests.
- Modify as needed: `src/risk/risk.rs` tests/helpers that construct `StrategyRegistration`.

## Constraints

- Do not delete `run_tick_recorder`, `run_raw_recorder`, `RawStoreEvent`, recorder senders, storage tables, or recorder config in this plan.
- Do not remove `MarketStore` or `market_sqlite_path`.
- Do not commit unless the user explicitly asks.
- Keep old field/function names if that reduces risk; semantic cleanup/renaming can be a later pass.

---

### Task 1: Make route maps token-self-mapped

**Files:**
- Modify: `src/strategies/strategy.rs:182-217`
- Test: `src/strategies/strategy.rs` tests

- [ ] **Step 1: Add/adjust tests for token self-mapping**

In `src/strategies/strategy.rs` tests, add or replace routing tests with these expectations:

```rust
#[test]
fn merge_topic_tokens_uses_each_related_token_as_its_own_topic() {
    let registration = StrategyRegistration {
        name: Arc::from("test"),
        kind: StrategyKind::Monitoring,
        topics: Arc::<[Arc<str>]>::from(vec![Arc::from("token-a"), Arc::from("token-b")]),
        topic_tokens: Arc::<[TopicRegistration]>::from(vec![TopicRegistration {
            topic: Arc::from("legacy-topic"),
            tokens: Arc::<[String]>::from(vec!["token-b".to_string(), "token-a".to_string()]),
        }]),
        related_tokens: Arc::<[String]>::from(vec!["token-b".to_string(), "token-a".to_string()]),
    };

    let routes = merge_topic_tokens(&[registration]);

    assert_eq!(routes.len(), 2);
    assert_eq!(routes.get("token-a").unwrap(), &vec!["token-a".to_string()]);
    assert_eq!(routes.get("token-b").unwrap(), &vec!["token-b".to_string()]);
}

#[test]
fn build_token_topics_returns_identity_topics() {
    let topic_tokens = HashMap::from([
        (Arc::from("token-a"), vec!["token-a".to_string()]),
        (Arc::from("token-b"), vec!["token-b".to_string()]),
    ]);

    let token_topics = build_token_topics(&topic_tokens);

    assert_eq!(token_topics.get("token-a").unwrap().as_ref(), &[Arc::<str>::from("token-a")]);
    assert_eq!(token_topics.get("token-b").unwrap().as_ref(), &[Arc::<str>::from("token-b")]);
}
```

Expected initial result before implementation: at least the first test fails because `merge_topic_tokens` currently keys by `legacy-topic`.

- [ ] **Step 2: Run focused failing tests**

Run:

```bash
cargo test strategy::tests::merge_topic_tokens_uses_each_related_token_as_its_own_topic strategy::tests::build_token_topics_returns_identity_topics --no-fail-fast
```

Expected:
- `merge_topic_tokens_uses_each_related_token_as_its_own_topic` fails before implementation.

- [ ] **Step 3: Implement token self-mapping helpers**

Replace `merge_topic_tokens` and simplify `build_token_topics` in `src/strategies/strategy.rs`:

```rust
pub fn merge_topic_tokens(
    registrations: &[StrategyRegistration],
) -> HashMap<Arc<str>, Vec<String>> {
    let mut tokens: Vec<String> = registrations
        .iter()
        .flat_map(|registration| registration.related_tokens.iter().cloned())
        .collect();
    tokens.sort();
    tokens.dedup();

    tokens
        .into_iter()
        .map(|token| {
            let topic = Arc::<str>::from(token.as_str());
            (topic, vec![token])
        })
        .collect()
}

pub fn build_token_topics(
    topic_tokens: &HashMap<Arc<str>, Vec<String>>,
) -> HashMap<String, Arc<[Arc<str>]>> {
    topic_tokens
        .values()
        .flatten()
        .cloned()
        .map(|token| {
            let topic = Arc::<str>::from(token.as_str());
            (token, Arc::<[Arc<str>]>::from(vec![topic]))
        })
        .collect()
}
```

- [ ] **Step 4: Run focused strategy routing tests**

Run:

```bash
cargo test strategy::tests --no-fail-fast
```

Expected:
- Strategy routing tests pass after updating expectations.

---

### Task 2: Change pair arbitrage to subscribe by token

**Files:**
- Modify: `src/strategies/pair_arbitrage.rs:81-219`
- Test: existing pair arbitrage tests in `src/strategies/pair_arbitrage.rs`

- [ ] **Step 1: Add/adjust tests for token subscription registration**

Update or add a test asserting `PairArbitrageStrategy::from_config` registers each CSV token as a topic/subscription key and still keeps all related tokens:

```rust
#[test]
fn from_config_registers_each_pair_token_as_subscription_topic() {
    let path = temp_csv("token1,token2,topic\npair-token-1,pair-token-2,legacy-topic\n");
    let strategy = PairArbitrageStrategy::from_config(test_filters(), path.to_str().unwrap()).unwrap();
    let registration = strategy.registration();

    assert_eq!(registration.topics.as_ref(), &[Arc::<str>::from("pair-token-1"), Arc::<str>::from("pair-token-2")]);
    assert_eq!(registration.related_tokens.as_ref(), &["pair-token-1".to_string(), "pair-token-2".to_string()]);
}
```

Use the existing test helper names in the file for `temp_csv` and `test_filters`; if the helpers have different names, adapt the test to the existing helper style without changing production behavior.

- [ ] **Step 2: Run focused pair arbitrage test before implementation**

Run:

```bash
cargo test strategies::pair_arbitrage::tests::from_config_registers_each_pair_token_as_subscription_topic --no-fail-fast
```

Expected:
- The test fails before implementation because registration currently subscribes by CSV topic.

- [ ] **Step 3: Implement `pairs_by_token` registration and lookup**

Change the struct field:

```rust
pub struct PairArbitrageStrategy {
    filters: Arc<Filters>,
    pairs_by_token: Arc<HashMap<Arc<str>, Arc<[PairEntry]>>>,
    registration: Arc<StrategyRegistration>,
}
```

In `from_config`, replace topic grouping with token grouping:

```rust
let mut token_pairs: HashMap<Arc<str>, Vec<PairEntry>> = HashMap::new();
let mut asset_ids = HashSet::new();

for entry in pair_entries {
    for token in &entry.tokens {
        asset_ids.insert(token.clone());
        token_pairs
            .entry(Arc::<str>::from(token.as_str()))
            .or_default()
            .push(entry.clone());
    }
}

let mut asset_ids: Vec<String> = asset_ids.into_iter().collect();
asset_ids.sort();

let subscriptions: Vec<Arc<str>> = asset_ids
    .iter()
    .map(|token| Arc::<str>::from(token.as_str()))
    .collect();

let topic_token_regs: Vec<TopicRegistration> = asset_ids
    .iter()
    .map(|token| TopicRegistration {
        topic: Arc::<str>::from(token.as_str()),
        tokens: Arc::<[String]>::from(vec![token.clone()]),
    })
    .collect();

let pairs_by_token: Arc<HashMap<Arc<str>, Arc<[PairEntry]>>> = Arc::new(
    token_pairs
        .into_iter()
        .map(|(token, pairs)| (token, Arc::<[PairEntry]>::from(pairs)))
        .collect(),
);

let registration = Arc::new(StrategyRegistration {
    name: Arc::from("pair_arbitrage"),
    kind: StrategyKind::PairArbitrage,
    topics: Arc::<[Arc<str>]>::from(subscriptions),
    topic_tokens: Arc::<[TopicRegistration]>::from(topic_token_regs),
    related_tokens: Arc::<[String]>::from(asset_ids),
});

Ok(Self {
    filters,
    pairs_by_token,
    registration,
})
```

Update `spawn` to pass `event.asset_id` instead of `event.topic`:

```rust
check_pairs(
    &store,
    &self.pairs_by_token,
    &self.filters,
    &event.asset_id,
    &updated,
    &order_gateway,
);
```

Update `check_pairs` signature and lookup:

```rust
fn check_pairs(
    store: &PriceStore,
    pairs_by_token: &HashMap<Arc<str>, Arc<[PairEntry]>>,
    filters: &Filters,
    token: &Arc<str>,
    updated_assets: &[String],
    _order_gateway: &crate::order_gateway::OrderGatewayHandle,
) {
    let Some(pairs) = pairs_by_token.get(token) else {
        return;
    };
    // keep the existing updated_set and pair checks unchanged
}
```

- [ ] **Step 4: Run pair arbitrage tests**

Run:

```bash
cargo test strategies::pair_arbitrage --no-fail-fast
```

Expected:
- Pair arbitrage tests pass after updating assertions that expected CSV topic subscription.

---

### Task 3: Change market maker to subscribe by token

**Files:**
- Modify: `src/strategies/market_maker.rs:144-155`
- Test: `src/strategies/market_maker.rs` tests

- [ ] **Step 1: Update registration tests**

Change tests that assert a fixed `market_maker` topic so they expect token topics. For a two-token rule, expected registration shape is:

```rust
assert_eq!(strategy.registration().topics.as_ref(), &[Arc::<str>::from("maker-token-1"), Arc::<str>::from("maker-token-2")]);
assert_eq!(strategy.registration().related_tokens.as_ref(), &["maker-token-1".to_string(), "maker-token-2".to_string()]);
```

For `topic_tokens`, expected each token maps to itself:

```rust
let regs = strategy.registration().topic_tokens.as_ref();
assert_eq!(regs.len(), 2);
assert!(regs.iter().any(|reg| reg.topic.as_ref() == "maker-token-1" && reg.tokens.as_ref() == &["maker-token-1".to_string()]));
assert!(regs.iter().any(|reg| reg.topic.as_ref() == "maker-token-2" && reg.tokens.as_ref() == &["maker-token-2".to_string()]));
```

Update log-event tests to construct `MarketEvent` with `topic` equal to the token asset id:

```rust
let event = MarketEvent {
    topic: Arc::from("maker-token-1"),
    asset_id: Arc::from("maker-token-1"),
    book: Arc::new(clean_book(40, 60, 10, 10)),
};
```

- [ ] **Step 2: Run focused failing market maker tests**

Run:

```bash
cargo test strategies::market_maker::tests::from_pool_entries_registers_selected_pool_tokens --no-fail-fast
```

Expected:
- The test fails before implementation because registration currently uses the fixed `market_maker` topic.

- [ ] **Step 3: Implement token subscription registration**

Replace the fixed topic registration in `MarketMakerStrategy::from_rules`:

```rust
let related_tokens = related_tokens.into_iter().collect::<Vec<_>>();
let topics = related_tokens
    .iter()
    .map(|token| Arc::<str>::from(token.as_str()))
    .collect::<Vec<_>>();
let topic_token_regs = related_tokens
    .iter()
    .map(|token| TopicRegistration {
        topic: Arc::<str>::from(token.as_str()),
        tokens: Arc::<[String]>::from(vec![token.clone()]),
    })
    .collect::<Vec<_>>();
let registration = Arc::new(StrategyRegistration {
    name: Arc::from(MARKET_MAKER_NAME),
    kind: StrategyKind::MarketMaker,
    topics: Arc::<[Arc<str>]>::from(topics),
    topic_tokens: Arc::<[TopicRegistration]>::from(topic_token_regs),
    related_tokens: Arc::<[String]>::from(related_tokens),
});
```

Do not remove `MARKET_MAKER_TOPIC` in this task unless compilation shows it is unused and no tests need it; if unused, delete the constant and any test expectations referencing it.

- [ ] **Step 4: Run market maker tests**

Run:

```bash
cargo test strategies::market_maker --no-fail-fast
```

Expected:
- Market maker tests pass after expectation updates.

---

### Task 4: Route market events directly by token

**Files:**
- Modify: `src/market.rs:361-459`
- Test: `src/market.rs` tests

- [ ] **Step 1: Update market routing tests**

Change tests so they create broadcast channels keyed by token id, and expect `MarketEvent.topic == asset_id`.

Replace any expectation that one asset update goes to multiple logical topics with a token-keyed expectation:

```rust
#[tokio::test]
async fn market_run_publishes_topic_event_to_asset_token_channel() {
    let token_topics = Arc::new(HashMap::from([(
        "token-1".to_string(),
        Arc::<[Arc<str>]>::from(vec![Arc::from("token-1")]),
    )]));
    let (topic_tx, mut topic_rx) = tokio::sync::broadcast::channel(8);
    let topic_txs = Arc::new(HashMap::from([(Arc::from("token-1"), topic_tx)]));

    // use the existing test setup for ws_rx, book_publisher, firehose_tx, and WsMessage::Book
    // then assert:
    let event = topic_rx.recv().await.unwrap();
    assert_eq!(event.topic.as_ref(), "token-1");
    assert_eq!(event.asset_id.as_ref(), "token-1");
}
```

Keep existing tests for publish order, reducer continuity, and subscription chunking, but adapt their keys from logical topics to token IDs.

- [ ] **Step 2: Run focused market tests before implementation**

Run:

```bash
cargo test market::tests --no-fail-fast
```

Expected:
- Tests that still expect multi-topic fanout fail before implementation/expectation updates.

- [ ] **Step 3: Simplify `publish_market_asset_update` to asset token topic**

Change `publish_market_asset_update` so it no longer accepts `topics`; it publishes only to the `asset_id` key:

```rust
async fn publish_market_asset_update(
    update: MarketAssetUpdate,
    book_publisher: &MarketBookPublisher,
    firehose_tx: &tokio::sync::mpsc::Sender<MarketAssetEvent>,
    topic_txs: &HashMap<Arc<str>, tokio::sync::broadcast::Sender<MarketEvent>>,
) -> anyhow::Result<()> {
    book_publisher.publish(update.asset_id.clone(), update.book.clone());
    let topics = Arc::<[Arc<str>]>::from(vec![update.asset_id.clone()]);
    firehose_tx
        .send(MarketAssetEvent {
            asset_id: update.asset_id.clone(),
            topics,
            book: update.book.clone(),
        })
        .await?;

    if let Some(tx) = topic_txs.get(&update.asset_id) {
        let _ = tx.send(MarketEvent {
            topic: update.asset_id.clone(),
            asset_id: update.asset_id,
            book: update.book,
        });
    }
    Ok(())
}
```

Update call site in `market::run`:

```rust
if !token_topics.contains_key(asset_id.as_ref()) {
    continue;
}
let book = Arc::new(book);
let update = MarketAssetUpdate {
    asset_id: asset_id.clone(),
    book: book.clone(),
};
if publish_market_asset_update(update, &book_publisher, &firehose_tx, &topic_txs)
    .await
    .is_err()
{
    return;
}
```

Keep `token_topics` parameter for this pass; it acts as the subscribed token allowlist and avoids forwarding unexpected WS tokens.

- [ ] **Step 4: Run market tests**

Run:

```bash
cargo test market::tests --no-fail-fast
```

Expected:
- Market tests pass after expectation updates.
- Recorder-related code remains compiled and unchanged.

---

### Task 5: Update main/risk tests and run full verification

**Files:**
- Modify: `src/main.rs` tests if they assert old topic mapping
- Modify: `src/risk/risk.rs` test helpers if they construct `StrategyRegistration`

- [ ] **Step 1: Update manual `StrategyRegistration` constructors**

Where tests manually construct a registration with logical topic names, change them to token topics. Example shape:

```rust
StrategyRegistration {
    name: Arc::from("test"),
    kind: StrategyKind::Monitoring,
    topics: Arc::<[Arc<str>]>::from(vec![Arc::from("token-1")]),
    topic_tokens: Arc::<[TopicRegistration]>::from(vec![TopicRegistration {
        topic: Arc::from("token-1"),
        tokens: Arc::<[String]>::from(vec!["token-1".to_string()]),
    }]),
    related_tokens: Arc::<[String]>::from(vec!["token-1".to_string()]),
}
```

- [ ] **Step 2: Run all tests**

Run:

```bash
cargo test --no-fail-fast
```

Expected:
- All tests pass.

- [ ] **Step 3: Run cargo check**

Run:

```bash
cargo check
```

Expected:
- `cargo check` exits successfully.
- Existing warnings may remain, but there should be no warnings caused by unused constants/functions introduced by this change.

- [ ] **Step 4: Search for old logical-topic assumptions**

Run:

```bash
rg "pairs_by_topic|MARKET_MAKER_TOPIC|market_maker\"\)|market_asset_event_carries_all_topics|fanout|topic_tokens" src
```

Expected:
- `pairs_by_topic` should have no matches.
- `MARKET_MAKER_TOPIC` should have no matches unless it is still intentionally used in a test that no longer asserts routing.
- `topic_tokens` may still exist in helper names/fields for this compatibility pass.
- Any remaining `market_maker` topic assertions should be reviewed and updated if they assume the fixed topic channel.

- [ ] **Step 5: Report result without committing**

Report:
- Which files changed.
- That tick/raw recorder functionality was not removed.
- Exact verification commands and pass/fail status.
- Remaining compatibility names that can be cleaned in a later pass, such as `topic_tokens` or `subscribe_strategy_topics`.
