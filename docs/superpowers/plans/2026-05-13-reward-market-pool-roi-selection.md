# Reward Market Pool ROI Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change reward market pool construction to build once, filter candidates by long end time and Gamma 24h volume, and select liquidity_reward markets by ROI descending top `pool_market_count`.

**Architecture:** Keep the existing `reward_market_cache.rs` loader, `reward_market_pool_state` table, and selected/halted semantics. Add Gamma market detail enrichment during pool entry loading, persist volume/ROI fields for inspection, and replace competitiveness-tail selection with ROI top-N selection.

**Tech Stack:** Rust 2024, Tokio, polymarket_client_sdk_v2 CLOB/Gamma clients, rusqlite, serde_json, cargo test.

---

## Review Process for This Change

The user wants to review each step. Execute one task at a time, stop after each task, show the diff and test output, and wait for approval before starting the next task.

Do not change runtime dynamic strategy loading in this plan. New selected markets still only affect strategy startup or future explicit reload work.

---

## Files and Responsibilities

- `src/reward_market_cache.rs`
  - Change pool build gating from daily rebuild to build-once.
  - Replace default candidate rules with end-time and ROI prerequisites.
  - Add Gamma market detail enrichment.
  - Compute `volume_24hr_clob`, `volume_24hr`, selected 24h volume, and `liquidity_reward_roi`.
  - Remove competitiveness-tail pool filtering from active rules.

- `src/storage.rs`
  - Add persisted nullable fields to `reward_market_pool_state`: `volume_24hr_clob`, `volume_24hr`, `liquidity_reward_roi`.
  - Thread new fields through `RewardMarketPoolStorageEntry`, insert queries, active entry reads, and tests.
  - Change `select_liquidity_reward_pool_entries` to sort by ROI descending and take top `pool_market_count`.

- `Cargo.toml`
  - No dependency changes expected. `polymarket_client_sdk_v2` already enables the `gamma` feature.

---

## Task 1: Persist volume and ROI fields in reward pool storage

**Files:**
- Modify: `src/storage.rs:93-128`
- Modify: `src/storage.rs:743-844`
- Modify: `src/storage.rs:958-1021`
- Modify: `src/storage.rs:1108-1158`
- Modify: `src/storage.rs:1409-1448`

### Step 1: Write the failing test

Add this test inside the existing `#[cfg(test)] mod tests` in `src/storage.rs`. If the file already has storage tests, place it next to the reward market pool tests.

```rust
#[test]
fn reward_market_pool_persists_volume_and_roi_fields() {
    let store = MarketStore::open(":memory:").expect("store should open");
    store.init_schema().expect("schema should initialize");

    let entries = vec![RewardMarketPoolStorageEntry {
        condition_id: "0xabc",
        market_slug: Some("test-market"),
        question: Some("Test market?"),
        token1: "token1",
        token2: "token2",
        tokens_json: "[]",
        market_competitiveness: Some("12.5"),
        rewards_min_size: Some("100"),
        rewards_max_spread: Some("4"),
        market_daily_reward: Some("25"),
        volume_24hr_clob: Some("60000"),
        volume_24hr: Some("65000"),
        liquidity_reward_roi: Some("0.25"),
    }];

    store
        .replace_reward_market_pool_entries(
            NaiveDate::from_ymd_opt(2026, 5, 13).unwrap(),
            123,
            &entries,
            456,
            1,
        )
        .expect("pool entries should replace");

    let loaded = store
        .load_active_reward_market_pool_entries()
        .expect("active pool entries should load");
    assert_eq!(loaded.len(), 1);
    assert_eq!(loaded[0].volume_24hr_clob.as_deref(), Some("60000"));
    assert_eq!(loaded[0].volume_24hr.as_deref(), Some("65000"));
    assert_eq!(loaded[0].liquidity_reward_roi.as_deref(), Some("0.25"));
}
```

### Step 2: Run test to verify it fails

Run:

```bash
cargo test storage::tests::reward_market_pool_persists_volume_and_roi_fields
```

Expected: FAIL to compile because `RewardMarketPoolStorageEntry` and `ActiveRewardMarketPoolEntry` do not yet have `volume_24hr_clob`, `volume_24hr`, and `liquidity_reward_roi` fields.

### Step 3: Add fields to storage structs

In `src/storage.rs`, extend `RewardMarketPoolStorageEntry`:

```rust
pub struct RewardMarketPoolStorageEntry<'a> {
    pub condition_id: &'a str,
    pub market_slug: Option<&'a str>,
    pub question: Option<&'a str>,
    pub token1: &'a str,
    pub token2: &'a str,
    pub tokens_json: &'a str,
    pub market_competitiveness: Option<&'a str>,
    pub rewards_min_size: Option<&'a str>,
    pub rewards_max_spread: Option<&'a str>,
    pub market_daily_reward: Option<&'a str>,
    pub volume_24hr_clob: Option<&'a str>,
    pub volume_24hr: Option<&'a str>,
    pub liquidity_reward_roi: Option<&'a str>,
}
```

Extend `ActiveRewardMarketPoolEntry`:

```rust
pub struct ActiveRewardMarketPoolEntry {
    pub condition_id: String,
    pub market_slug: Option<String>,
    pub question: Option<String>,
    pub token1: String,
    pub token2: String,
    pub tokens_json: String,
    pub market_competitiveness: Option<String>,
    pub rewards_min_size: Option<String>,
    pub rewards_max_spread: Option<String>,
    pub market_daily_reward: Option<String>,
    pub volume_24hr_clob: Option<String>,
    pub volume_24hr: Option<String>,
    pub liquidity_reward_roi: Option<String>,
    pub build_date_utc: Option<String>,
    pub pool_version: Option<u64>,
    pub liquidity_reward_selected: bool,
    pub liquidity_reward_selected_at_ms: Option<u64>,
    pub liquidity_reward_select_reason: Option<String>,
    pub liquidity_reward_select_rank: Option<u32>,
    pub liquidity_reward_halted: bool,
    pub liquidity_reward_halted_at_ms: Option<u64>,
    pub liquidity_reward_halt_reason: Option<String>,
    pub liquidity_reward_halted_pool_version: Option<u64>,
}
```

### Step 4: Add schema columns

In `MarketStore::init_schema`, add columns to the create-table SQL after `market_daily_reward TEXT`:

```sql
volume_24hr_clob TEXT,
volume_24hr TEXT,
liquidity_reward_roi TEXT,
```

Add migrations after the existing `market_daily_reward` `ensure_column`:

```rust
ensure_column(
    conn,
    "reward_market_pool_state",
    "volume_24hr_clob",
    "TEXT",
)?;
ensure_column(conn, "reward_market_pool_state", "volume_24hr", "TEXT")?;
ensure_column(
    conn,
    "reward_market_pool_state",
    "liquidity_reward_roi",
    "TEXT",
)?;
```

### Step 5: Thread fields through replace insert

Update `replace_reward_market_pool_entries` insert column list:

```sql
INSERT INTO reward_market_pool_state (
    condition_id, market_slug, question, token1, token2, tokens_json,
    market_competitiveness, rewards_min_size, rewards_max_spread,
    market_daily_reward, volume_24hr_clob, volume_24hr,
    liquidity_reward_roi, build_date_utc, pool_version, in_pool,
    first_seen_at_ms, last_seen_at_ms
) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, 1, ?16, ?16)
```

Update params:

```rust
stmt.execute(params![
    entry.condition_id,
    entry.market_slug,
    entry.question,
    entry.token1,
    entry.token2,
    entry.tokens_json,
    entry.market_competitiveness,
    entry.rewards_min_size,
    entry.rewards_max_spread,
    entry.market_daily_reward,
    entry.volume_24hr_clob,
    entry.volume_24hr,
    entry.liquidity_reward_roi,
    build_date_utc,
    pool_version as i64,
    now_ms as i64,
])?;
```

### Step 6: Thread fields through read queries and row mapping

Add fields to both `load_active_reward_market_pool_entries` and `load_liquidity_reward_pool_entries` SELECT lists immediately after `market_daily_reward`:

```sql
volume_24hr_clob, volume_24hr, liquidity_reward_roi,
```

Update `active_reward_market_pool_entry_from_row` indexes. The new layout is:

- 0 condition_id
- 1 market_slug
- 2 question
- 3 token1
- 4 token2
- 5 tokens_json
- 6 market_competitiveness
- 7 rewards_min_size
- 8 rewards_max_spread
- 9 market_daily_reward
- 10 volume_24hr_clob
- 11 volume_24hr
- 12 liquidity_reward_roi
- 13 build_date_utc
- 14 pool_version
- 15 liquidity_reward_selected
- 16 liquidity_reward_selected_at_ms
- 17 liquidity_reward_select_reason
- 18 liquidity_reward_select_rank
- 19 liquidity_reward_halted
- 20 liquidity_reward_halted_at_ms
- 21 liquidity_reward_halt_reason
- 22 liquidity_reward_halted_pool_version

Use this mapping:

```rust
fn active_reward_market_pool_entry_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ActiveRewardMarketPoolEntry> {
    let pool_version = row
        .get::<_, Option<i64>>(14)?
        .and_then(|value| u64::try_from(value).ok());
    let liquidity_reward_selected_at_ms = row
        .get::<_, Option<i64>>(16)?
        .and_then(|value| u64::try_from(value).ok());
    let liquidity_reward_select_rank = row
        .get::<_, Option<i64>>(18)?
        .and_then(|value| u32::try_from(value).ok());
    let liquidity_reward_halted_at_ms = row
        .get::<_, Option<i64>>(20)?
        .and_then(|value| u64::try_from(value).ok());
    let liquidity_reward_halted_pool_version = row
        .get::<_, Option<i64>>(22)?
        .and_then(|value| u64::try_from(value).ok());
    Ok(ActiveRewardMarketPoolEntry {
        condition_id: row.get(0)?,
        market_slug: row.get(1)?,
        question: row.get(2)?,
        token1: row.get(3)?,
        token2: row.get(4)?,
        tokens_json: row.get(5)?,
        market_competitiveness: row.get(6)?,
        rewards_min_size: row.get(7)?,
        rewards_max_spread: row.get(8)?,
        market_daily_reward: row.get(9)?,
        volume_24hr_clob: row.get(10)?,
        volume_24hr: row.get(11)?,
        liquidity_reward_roi: row.get(12)?,
        build_date_utc: row.get(13)?,
        pool_version,
        liquidity_reward_selected: row.get::<_, i64>(15)? != 0,
        liquidity_reward_selected_at_ms,
        liquidity_reward_select_reason: row.get(17)?,
        liquidity_reward_select_rank,
        liquidity_reward_halted: row.get::<_, i64>(19)? != 0,
        liquidity_reward_halted_at_ms,
        liquidity_reward_halt_reason: row.get(21)?,
        liquidity_reward_halted_pool_version,
    })
}
```

### Step 7: Update test fixtures that construct `ActiveRewardMarketPoolEntry`

Any struct literal for `ActiveRewardMarketPoolEntry` must add:

```rust
volume_24hr_clob: None,
volume_24hr: None,
liquidity_reward_roi: None,
```

Known likely file:

- `src/reward_market_pool_monitor.rs` tests construct `ActiveRewardMarketPoolEntry`.

### Step 8: Run storage tests

Run:

```bash
cargo test storage::tests::reward_market_pool_persists_volume_and_roi_fields
```

Expected: PASS.

Then run:

```bash
cargo test storage::tests
```

Expected: PASS.

### Step 9: Stop for user review

Show:

```bash
git diff -- src/storage.rs src/reward_market_pool_monitor.rs
```

Wait for user approval before Task 2.

---

## Task 2: Change liquidity_reward selection from competitiveness tails to ROI top-N

**Files:**
- Modify: `src/storage.rs:1286-1407`

### Step 1: Write failing tests

Add these tests inside `src/storage.rs` tests:

```rust
#[test]
fn selects_liquidity_reward_pool_entries_by_roi_descending_top_n() {
    let entries = vec![
        RewardMarketPoolStorageEntry {
            condition_id: "0xlow",
            market_slug: None,
            question: None,
            token1: "token-low-1",
            token2: "token-low-2",
            tokens_json: "[]",
            market_competitiveness: Some("1"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("10"),
            volume_24hr_clob: Some("60000"),
            volume_24hr: None,
            liquidity_reward_roi: Some("0.10"),
        },
        RewardMarketPoolStorageEntry {
            condition_id: "0xhigh",
            market_slug: None,
            question: None,
            token1: "token-high-1",
            token2: "token-high-2",
            tokens_json: "[]",
            market_competitiveness: Some("999"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("50"),
            volume_24hr_clob: Some("60000"),
            volume_24hr: None,
            liquidity_reward_roi: Some("0.50"),
        },
        RewardMarketPoolStorageEntry {
            condition_id: "0xmid",
            market_slug: None,
            question: None,
            token1: "token-mid-1",
            token2: "token-mid-2",
            tokens_json: "[]",
            market_competitiveness: Some("500"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("25"),
            volume_24hr_clob: Some("60000"),
            volume_24hr: None,
            liquidity_reward_roi: Some("0.25"),
        },
    ];

    let selected = select_liquidity_reward_pool_entries(&entries, 2);

    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].condition_id, "0xhigh");
    assert_eq!(selected[0].reason, "roi_top");
    assert_eq!(selected[0].rank, 1);
    assert_eq!(selected[1].condition_id, "0xmid");
    assert_eq!(selected[1].reason, "roi_top");
    assert_eq!(selected[1].rank, 2);
}

#[test]
fn roi_selection_skips_entries_without_parseable_roi() {
    let entries = vec![
        RewardMarketPoolStorageEntry {
            condition_id: "0xmissing",
            market_slug: None,
            question: None,
            token1: "token-missing-1",
            token2: "token-missing-2",
            tokens_json: "[]",
            market_competitiveness: Some("1"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("10"),
            volume_24hr_clob: Some("60000"),
            volume_24hr: None,
            liquidity_reward_roi: None,
        },
        RewardMarketPoolStorageEntry {
            condition_id: "0xvalid",
            market_slug: None,
            question: None,
            token1: "token-valid-1",
            token2: "token-valid-2",
            tokens_json: "[]",
            market_competitiveness: Some("999"),
            rewards_min_size: Some("100"),
            rewards_max_spread: Some("4"),
            market_daily_reward: Some("50"),
            volume_24hr_clob: Some("60000"),
            volume_24hr: None,
            liquidity_reward_roi: Some("0.50"),
        },
    ];

    let selected = select_liquidity_reward_pool_entries(&entries, 2);

    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].condition_id, "0xvalid");
}
```

### Step 2: Run tests to verify they fail

Run:

```bash
cargo test storage::tests::selects_liquidity_reward_pool_entries_by_roi_descending_top_n storage::tests::roi_selection_skips_entries_without_parseable_roi
```

If Cargo rejects multiple filters, run:

```bash
cargo test storage::tests::selects_liquidity_reward_pool_entries_by_roi_descending_top_n
cargo test storage::tests::roi_selection_skips_entries_without_parseable_roi
```

Expected: first test FAILS because existing selection uses `market_competitiveness` tails and reasons `competitiveness_low_tail` / `competitiveness_high_tail`.

### Step 3: Replace selection implementation

Replace `select_liquidity_reward_pool_entries` with:

```rust
fn select_liquidity_reward_pool_entries<'a>(
    entries: &'a [RewardMarketPoolStorageEntry<'a>],
    market_count: usize,
) -> Vec<LiquidityRewardPoolSelection<'a>> {
    if market_count == 0 {
        return Vec::new();
    }

    let mut sorted = entries
        .iter()
        .filter_map(|entry| {
            entry
                .liquidity_reward_roi
                .and_then(|value| value.parse::<f64>().ok())
                .map(|roi| (entry, roi))
        })
        .collect::<Vec<_>>();
    sorted.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.condition_id.cmp(right.0.condition_id))
    });

    sorted
        .into_iter()
        .take(market_count)
        .enumerate()
        .map(|(index, (entry, _))| LiquidityRewardPoolSelection {
            condition_id: entry.condition_id,
            reason: "roi_top",
            rank: index as u32 + 1,
        })
        .collect()
}
```

### Step 4: Run tests

Run:

```bash
cargo test storage::tests::selects_liquidity_reward_pool_entries_by_roi_descending_top_n
cargo test storage::tests::roi_selection_skips_entries_without_parseable_roi
cargo test storage::tests
```

Expected: PASS.

### Step 5: Stop for user review

Show:

```bash
git diff -- src/storage.rs
```

Wait for user approval before Task 3.

---

## Task 3: Add build-once gating with explicit rebuild hook reserved

**Files:**
- Modify: `src/reward_market_cache.rs:132-189`

### Step 1: Write failing tests for rebuild decision

Add a small pure function and tests. First write tests in `src/reward_market_cache.rs` tests:

```rust
#[test]
fn reward_pool_builds_when_no_previous_meta_exists() {
    assert!(should_build_reward_market_pool(None, RewardMarketPoolBuildTrigger::Startup));
}

#[test]
fn reward_pool_does_not_auto_rebuild_when_previous_meta_exists() {
    let meta = RewardMarketPoolMeta {
        build_date_utc: NaiveDate::from_ymd_opt(2026, 5, 12).unwrap(),
        version: 123,
        built_at_ms: 123,
    };

    assert!(!should_build_reward_market_pool(
        Some(meta),
        RewardMarketPoolBuildTrigger::Startup
    ));
}
```

### Step 2: Run tests to verify they fail

Run:

```bash
cargo test reward_market_cache::tests::reward_pool_builds_when_no_previous_meta_exists
cargo test reward_market_cache::tests::reward_pool_does_not_auto_rebuild_when_previous_meta_exists
```

Expected: FAIL to compile because `should_build_reward_market_pool` and `RewardMarketPoolBuildTrigger` do not exist.

### Step 3: Add rebuild trigger enum and decision function

In `src/reward_market_cache.rs`, near `RewardMarketPoolMeta`, add:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RewardMarketPoolBuildTrigger {
    Startup,
}

fn should_build_reward_market_pool(
    last_success_meta: Option<RewardMarketPoolMeta>,
    trigger: RewardMarketPoolBuildTrigger,
) -> bool {
    match trigger {
        RewardMarketPoolBuildTrigger::Startup => last_success_meta.is_none(),
    }
}
```

This is the reserved mechanism: future code can add `ManualForce`, `ConfigChanged`, or other trigger variants without changing the loader shape.

### Step 4: Use decision function in loader loop

Replace the date-based skip in `run_reward_market_loader`:

```rust
let now = Utc::now();
let build_date_utc = now.date_naive();
if last_success_meta.map(|meta| meta.build_date_utc) == Some(build_date_utc) {
    tokio::time::sleep(duration_until_next_utc_midnight(now)).await;
    continue;
}
```

with:

```rust
let now = Utc::now();
let build_date_utc = now.date_naive();
if !should_build_reward_market_pool(last_success_meta, RewardMarketPoolBuildTrigger::Startup) {
    info!(
        target: "order",
        pool_version = last_success_meta.map(|meta| meta.version),
        "reward_market_loader 已存在奖励市场池，跳过自动重建"
    );
    tokio::time::sleep(retry_interval).await;
    continue;
}
```

After successful build, replace the log message:

```rust
"reward_market_loader 当日奖励市场池构建完成，等待下一个 UTC 零点"
```

with:

```rust
"reward_market_loader 奖励市场池首次构建完成"
```

Replace the post-success sleep:

```rust
tokio::time::sleep(duration_until_next_utc_midnight(Utc::now())).await;
```

with:

```rust
tokio::time::sleep(retry_interval).await;
```

Replace failure log text:

```rust
"reward_market_loader 当日奖励市场池构建失败，保留旧缓存并稍后重试"
```

with:

```rust
"reward_market_loader 奖励市场池构建失败，保留旧缓存并稍后重试"
```

### Step 5: Remove unused daily-midnight helper if it becomes unused

If `duration_until_next_utc_midnight` and `DateTime` import become unused, delete `duration_until_next_utc_midnight` and remove `DateTime` from the chrono import.

### Step 6: Run tests

Run:

```bash
cargo test reward_market_cache::tests::reward_pool_builds_when_no_previous_meta_exists
cargo test reward_market_cache::tests::reward_pool_does_not_auto_rebuild_when_previous_meta_exists
cargo test reward_market_cache::tests
```

Expected: PASS.

### Step 7: Stop for user review

Show:

```bash
git diff -- src/reward_market_cache.rs
```

Wait for user approval before Task 4.

---

## Task 4: Replace active pool rules with 60-day end time and positive min size prerequisites

**Files:**
- Modify: `src/reward_market_cache.rs:52-130`
- Modify: `src/reward_market_cache.rs:466-620`

### Step 1: Write failing tests for basic rules

Add tests in `src/reward_market_cache.rs` tests:

```rust
#[test]
fn default_active_pool_requires_market_to_end_after_sixty_days() {
    let rules = RewardMarketPoolRules::default_active_pool(None);
    let now = NaiveDate::from_ymd_opt(2026, 5, 13)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let too_soon = market(condition_id(1), "1", "2026-07-11");
    let far_enough = market(condition_id(2), "1", "2026-07-13");

    assert!(!rules.basic_matches(&too_soon, now));
    assert!(rules.basic_matches(&far_enough, now));
}

#[test]
fn default_active_pool_requires_positive_rewards_min_size() {
    let rules = RewardMarketPoolRules::default_active_pool(None);
    let now = NaiveDate::from_ymd_opt(2026, 5, 13)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let zero_min_size = market_with_min_size(condition_id(1), "100", "2026-08-01", "0");
    let positive_min_size = market_with_min_size(condition_id(2), "100", "2026-08-01", "100");

    assert!(!rules.basic_matches(&zero_min_size, now));
    assert!(rules.basic_matches(&positive_min_size, now));
}
```

### Step 2: Run tests to verify they fail

Run:

```bash
cargo test reward_market_cache::tests::default_active_pool_requires_market_to_end_after_sixty_days
cargo test reward_market_cache::tests::default_active_pool_requires_positive_rewards_min_size
```

Expected: first test FAILS because current rule is 48 hours, not 60 days; second may pass or fail depending current min-size behavior, but it documents the required behavior.

### Step 3: Change rule enum and defaults

Replace `RewardMarketRule` with:

```rust
#[derive(Clone)]
pub enum RewardMarketRule {
    MinDaysBeforeEnd(i64),
    PositiveRewardsMinSize,
}
```

Replace `default_active_pool` with:

```rust
pub fn default_active_pool(_max_rewards_min_size: Option<Decimal>) -> Self {
    Self::new(vec![
        RewardMarketRule::MinDaysBeforeEnd(60),
        RewardMarketRule::PositiveRewardsMinSize,
    ])
}
```

Keep the `_max_rewards_min_size` argument so call sites do not change, but it is intentionally unused for now.

Replace `RewardMarketRule::basic_matches` with:

```rust
fn basic_matches(&self, market: &CurrentRewardResponse, now: NaiveDateTime) -> bool {
    match self {
        Self::MinDaysBeforeEnd(min_days_before_end) => market_end_time(market).is_some_and(|end_time| {
            end_time - now > ChronoDuration::days(*min_days_before_end)
        }),
        Self::PositiveRewardsMinSize => market.rewards_min_size > Decimal::ZERO,
    }
}
```

Replace `RewardMarketPoolRules::apply_pool_rules` with a no-op or delete it if unused after Task 5. For now:

```rust
fn apply_pool_rules(&self, entries: Vec<RewardMarketPoolEntry>) -> Vec<RewardMarketPoolEntry> {
    entries
}
```

Remove `ExcludeCompetitivenessTails`, `MinDailyReward`, `MinHoursBeforeEnd`, and `MaxRewardsMinSize` branches.

### Step 4: Remove competitiveness-tail helper tests only after replacement tests pass

If existing tests assert `exclude_competitiveness_tails` behavior, either delete those tests or rewrite them to assert ROI top-N in `storage.rs`. Do not keep tests for behavior no longer used.

### Step 5: Run tests

Run:

```bash
cargo test reward_market_cache::tests::default_active_pool_requires_market_to_end_after_sixty_days
cargo test reward_market_cache::tests::default_active_pool_requires_positive_rewards_min_size
cargo test reward_market_cache::tests
```

Expected: PASS.

### Step 6: Stop for user review

Show:

```bash
git diff -- src/reward_market_cache.rs
```

Wait for user approval before Task 5.

---

## Task 5: Add Gamma volume enrichment and volume filtering

**Files:**
- Modify: `src/reward_market_cache.rs:1-24`
- Modify: `src/reward_market_cache.rs:315-384`
- Modify: `src/reward_market_cache.rs:338-363`

### Step 1: Write unit tests for volume choice and threshold

Add pure helper tests in `src/reward_market_cache.rs` tests:

```rust
#[test]
fn selected_volume_prefers_clob_volume_over_total_volume() {
    let snapshot = RewardMarketGammaSnapshot {
        volume_24hr_clob: Some(Decimal::from(60_000u32)),
        volume_24hr: Some(Decimal::from(70_000u32)),
    };

    assert_eq!(selected_volume_24hr(&snapshot), Some(Decimal::from(60_000u32)));
}

#[test]
fn selected_volume_falls_back_to_total_volume() {
    let snapshot = RewardMarketGammaSnapshot {
        volume_24hr_clob: None,
        volume_24hr: Some(Decimal::from(55_000u32)),
    };

    assert_eq!(selected_volume_24hr(&snapshot), Some(Decimal::from(55_000u32)));
}

#[test]
fn volume_rule_requires_at_least_fifty_thousand() {
    assert!(volume_passes_threshold(Some(Decimal::from(50_000u32))));
    assert!(volume_passes_threshold(Some(Decimal::from(50_001u32))));
    assert!(!volume_passes_threshold(Some(Decimal::from(49_999u32))));
    assert!(!volume_passes_threshold(None));
}
```

### Step 2: Run tests to verify they fail

Run:

```bash
cargo test reward_market_cache::tests::selected_volume_prefers_clob_volume_over_total_volume
cargo test reward_market_cache::tests::selected_volume_falls_back_to_total_volume
cargo test reward_market_cache::tests::volume_rule_requires_at_least_fifty_thousand
```

Expected: FAIL to compile because `RewardMarketGammaSnapshot`, `selected_volume_24hr`, and `volume_passes_threshold` do not exist.

### Step 3: Add Gamma snapshot and helpers

In `src/reward_market_cache.rs`, add:

```rust
#[derive(Debug, Clone, Copy, PartialEq)]
struct RewardMarketGammaSnapshot {
    volume_24hr_clob: Option<Decimal>,
    volume_24hr: Option<Decimal>,
}

fn selected_volume_24hr(snapshot: &RewardMarketGammaSnapshot) -> Option<Decimal> {
    snapshot.volume_24hr_clob.or(snapshot.volume_24hr)
}

fn volume_passes_threshold(volume: Option<Decimal>) -> bool {
    volume.is_some_and(|volume| volume >= Decimal::from(50_000u32))
}
```

### Step 4: Add Gamma client import and fetch function

Add imports:

```rust
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketBySlugRequest;
```

Add function:

```rust
async fn load_gamma_market_snapshot(
    gamma_client: &GammaClient,
    market_slug: &str,
) -> anyhow::Result<RewardMarketGammaSnapshot> {
    let market = gamma_client
        .market_by_slug(&MarketBySlugRequest::builder().slug(market_slug).build())
        .await?;
    Ok(RewardMarketGammaSnapshot {
        volume_24hr_clob: market.volume_24hr_clob,
        volume_24hr: market.volume_24hr,
    })
}
```

If `GammaClient::new` requires a host argument, inspect `gamma/client.rs` and use the SDK's default constructor pattern. Do not introduce a new HTTP dependency.

### Step 5: Extend `RewardMarketPoolEntry`

Change:

```rust
pub struct RewardMarketPoolEntry {
    pub market: Arc<CurrentRewardResponse>,
    pub detail: Arc<MarketRewardResponse>,
}
```

to:

```rust
pub struct RewardMarketPoolEntry {
    pub market: Arc<CurrentRewardResponse>,
    pub detail: Arc<MarketRewardResponse>,
    gamma: RewardMarketGammaSnapshot,
}
```

Update all test fixtures `RewardMarketPoolEntry { ... }` to include:

```rust
gamma: RewardMarketGammaSnapshot {
    volume_24hr_clob: Some(Decimal::from(60_000u32)),
    volume_24hr: None,
},
```

### Step 6: Filter entries by Gamma volume in `load_pool_entries`

Create a Gamma client once at the start of `load_pool_entries`:

```rust
let gamma_client = GammaClient::default();
```

If `default()` is unavailable, use the SDK constructor from `gamma/client.rs`.

Change successful detail branch from pushing immediately to:

```rust
Ok(Some(detail)) => match load_gamma_market_snapshot(&gamma_client, &detail.market_slug).await {
    Ok(gamma) if volume_passes_threshold(selected_volume_24hr(&gamma)) => {
        entries.push(RewardMarketPoolEntry {
            market: Arc::new(market),
            detail: Arc::new(detail),
            gamma,
        });
    }
    Ok(gamma) => {
        warn!(
            target: "order",
            condition_id = %condition_id,
            market_slug = %detail.market_slug,
            volume_24hr_clob = ?gamma.volume_24hr_clob,
            volume_24hr = ?gamma.volume_24hr,
            "reward_market_loader Gamma 24h 成交量不足，跳过市场池候选"
        );
    }
    Err(error) => {
        warn!(
            target: "order",
            condition_id = %condition_id,
            market_slug = %detail.market_slug,
            error = %error,
            "reward_market_loader 查询 Gamma 市场详情失败，跳过市场池候选"
        );
    }
},
```

### Step 7: Thread Gamma fields into storage conversion

In `pool_entries_to_storage_entries`, set:

```rust
volume_24hr_clob: entry.gamma.volume_24hr_clob.map(|value| value.to_string()),
volume_24hr: entry.gamma.volume_24hr.map(|value| value.to_string()),
liquidity_reward_roi: liquidity_reward_roi(entry).map(|value| value.to_string()),
```

Add `liquidity_reward_roi` helper in Task 6. For this task, temporarily add the helper exactly as Task 6 Step 3 shows so compilation succeeds.

### Step 8: Run tests

Run:

```bash
cargo test reward_market_cache::tests::selected_volume_prefers_clob_volume_over_total_volume
cargo test reward_market_cache::tests::selected_volume_falls_back_to_total_volume
cargo test reward_market_cache::tests::volume_rule_requires_at_least_fifty_thousand
cargo test reward_market_cache::tests
```

Expected: PASS.

### Step 9: Stop for user review

Show:

```bash
git diff -- src/reward_market_cache.rs src/storage.rs
```

Wait for user approval before Task 6.

---

## Task 6: Compute ROI from daily reward and rewards_min_size

**Files:**
- Modify: `src/reward_market_cache.rs:338-363`
- Modify: `src/reward_market_cache.rs:441-456`

### Step 1: Write failing ROI tests

Add tests in `src/reward_market_cache.rs` tests:

```rust
#[test]
fn liquidity_reward_roi_divides_daily_reward_by_rewards_min_size() {
    let entry = RewardMarketPoolEntry {
        market: Arc::new(market(condition_id(1), "50", "2026-08-01")),
        detail: Arc::new(detail_with_min_size(condition_id(1), "10", "200")),
        gamma: RewardMarketGammaSnapshot {
            volume_24hr_clob: Some(Decimal::from(60_000u32)),
            volume_24hr: None,
        },
    };

    assert_eq!(liquidity_reward_roi(&entry), Some(Decimal::try_from(0.25_f64).unwrap()));
}

#[test]
fn liquidity_reward_roi_is_none_when_rewards_min_size_is_zero() {
    let entry = RewardMarketPoolEntry {
        market: Arc::new(market(condition_id(1), "50", "2026-08-01")),
        detail: Arc::new(detail_with_min_size(condition_id(1), "10", "0")),
        gamma: RewardMarketGammaSnapshot {
            volume_24hr_clob: Some(Decimal::from(60_000u32)),
            volume_24hr: None,
        },
    };

    assert_eq!(liquidity_reward_roi(&entry), None);
}
```

Add fixture helper:

```rust
fn detail_with_min_size(
    condition_id: B256,
    competitiveness: &str,
    rewards_min_size: &str,
) -> MarketRewardResponse {
    serde_json::from_value(json!({
        "condition_id": condition_id.to_string(),
        "question": "question",
        "market_slug": condition_id.to_string(),
        "event_slug": "event",
        "image": "",
        "rewards_max_spread": "1",
        "rewards_min_size": rewards_min_size,
        "market_competitiveness": competitiveness,
        "tokens": [{
            "token_id": "1",
            "outcome": "Yes",
            "price": "0",
            "winner": false
        }, {
            "token_id": "2",
            "outcome": "No",
            "price": "0",
            "winner": false
        }],
        "rewards_config": [],
    }))
    .expect("test market detail should deserialize")
}
```

Then update existing `detail` helper to call:

```rust
fn detail(condition_id: B256, competitiveness: &str) -> MarketRewardResponse {
    detail_with_min_size(condition_id, competitiveness, "1")
}
```

### Step 2: Run tests to verify they fail

Run:

```bash
cargo test reward_market_cache::tests::liquidity_reward_roi_divides_daily_reward_by_rewards_min_size
cargo test reward_market_cache::tests::liquidity_reward_roi_is_none_when_rewards_min_size_is_zero
```

Expected: FAIL if `liquidity_reward_roi` is absent or returns placeholder behavior.

### Step 3: Implement ROI helper

Add:

```rust
fn liquidity_reward_roi(entry: &RewardMarketPoolEntry) -> Option<Decimal> {
    if entry.detail.rewards_min_size <= Decimal::ZERO {
        return None;
    }
    Some(market_daily_reward(&entry.market) / entry.detail.rewards_min_size)
}
```

Ensure `pool_entries_to_storage_entries` uses this helper:

```rust
liquidity_reward_roi: liquidity_reward_roi(entry).map(|value| value.to_string()),
```

### Step 4: Run tests

Run:

```bash
cargo test reward_market_cache::tests::liquidity_reward_roi_divides_daily_reward_by_rewards_min_size
cargo test reward_market_cache::tests::liquidity_reward_roi_is_none_when_rewards_min_size_is_zero
cargo test reward_market_cache::tests
```

Expected: PASS.

### Step 5: Stop for user review

Show:

```bash
git diff -- src/reward_market_cache.rs
```

Wait for user approval before Task 7.

---

## Task 7: Remove unused competitiveness-tail filtering code and align logs

**Files:**
- Modify: `src/reward_market_cache.rs`

### Step 1: Confirm dead code candidates

Inspect whether these are still referenced:

```rust
exclude_competitiveness_tails
percentile_cut_count
RewardMarketPoolRules::apply_pool_rules
RewardMarketRule::ExcludeCompetitivenessTails
```

Use:

```bash
cargo test reward_market_cache::tests
```

Expected before cleanup: tests pass, but compiler may warn about unused helpers.

### Step 2: Remove unused helpers

Delete functions if unreferenced:

```rust
fn exclude_competitiveness_tails(...)
fn percentile_cut_count(...)
```

Delete `RewardMarketPoolRules::apply_pool_rules` if it is no longer used.

In `load_reward_markets_once`, remove:

```rust
let pool_entries = pool_rules.apply_pool_rules(pool_entries);
```

Change counters if needed:

```rust
let reward_market_pool_count = pool_entries.len();
```

### Step 3: Update loader log names if helpful

Keep existing counters but make meaning clear:

- `loaded_market_count`: all current reward markets from CLOB reward endpoint
- `basic_pool_candidate_count`: pass end-time and positive min-size rules
- `detailed_pool_candidate_count`: have reward detail and pass Gamma volume threshold
- `reward_market_pool_count`: stored raw pool count
- `selected_liquidity_reward_market_count`: ROI top-N count

No new test required for log text.

### Step 4: Run tests

Run:

```bash
cargo test reward_market_cache::tests
cargo test storage::tests
cargo test reward_market_pool_monitor::tests
```

Expected: PASS.

### Step 5: Stop for user review

Show:

```bash
git diff -- src/reward_market_cache.rs
```

Wait for user approval before Task 8.

---

## Task 8: Full validation

**Files:**
- Validate all changed files.

### Step 1: Run formatter check

Run:

```bash
cargo fmt --check
```

Expected: PASS.

If it fails, run:

```bash
cargo fmt
cargo fmt --check
```

Then stop and show formatting diff for review.

### Step 2: Run targeted tests

Run:

```bash
cargo test reward_market_cache::tests
cargo test storage::tests
cargo test reward_market_pool_monitor::tests
```

Expected: PASS.

### Step 3: Run full test suite

Run:

```bash
cargo test
```

Expected: PASS.

Existing warnings may appear; do not fix unrelated warnings in this task.

### Step 4: Final review diff

Run:

```bash
git diff -- src/reward_market_cache.rs src/storage.rs src/reward_market_pool_monitor.rs
```

Review for:

- No secrets printed.
- No change to runtime dynamic strategy loading.
- No use of `pool_max_rewards_min_size` in reward pool rules.
- No competitiveness-tail selection.
- ROI selection reason is `roi_top`.
- Build-once logic skips automatic rebuild after any existing pool meta.

### Step 5: Stop for final user review

Show test results and diff summary. Do not commit unless the user explicitly asks.

---

## Self-Review

- Spec coverage: Covered build-once gating, reserved rebuild trigger shape, 60-day end-time rule, Gamma `volume_24hr_clob` then `volume_24hr` threshold, positive `rewards_min_size`, ROI calculation, ROI top-N selection, and removed `pool_max_rewards_min_size` / competitiveness-tail use.
- Placeholder scan: No TBD/TODO placeholders. Each implementation step has exact file targets, code snippets, commands, and expected outcomes.
- Type consistency: `volume_24hr_clob`, `volume_24hr`, and `liquidity_reward_roi` names are consistent across storage structs, SQL, row mapping, and reward cache conversion.
