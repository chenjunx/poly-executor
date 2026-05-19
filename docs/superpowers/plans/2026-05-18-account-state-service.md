# Account State Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the configurable account fund snapshot monitor with an always-on in-memory account state service that polls once per second and exposes latest-state subscription/query APIs.

**Architecture:** `src/account.rs` owns the authenticated CLOB balance/allowance polling loop and publishes the latest `AccountFundSnapshot` through `tokio::sync::watch`. `main.rs` starts the monitor unconditionally and keeps the returned `AccountReadHandle` available for future consumers, without wiring it into risk logic in this change. Storage runtime writes/reads for account fund snapshots are removed, while the existing SQLite table schema is left intact to avoid a destructive migration.

**Tech Stack:** Rust, Tokio, `tokio::sync::watch`, Polymarket CLOB SDK, SQLite storage layer tests, cargo test.

---

## Files

- Modify: `src/account.rs`
  - Remove `AccountConfig` dependency and `MarketStore` persistence from the polling loop.
  - Add `AccountReadHandle` backed by `tokio::sync::watch::Receiver<Option<AccountFundSnapshot>>`.
  - Add a `spawn_account_monitor(auth: AuthConfig) -> AccountReadHandle` entrypoint.
  - Keep pure conversion tests for balance/allowance response serialization.
- Modify: `src/main.rs`
  - Stop reading account monitor config.
  - Start account monitor unconditionally after config/log/store initialization.
  - Keep the returned read handle as `_account_read_handle` until a downstream module consumes it.
- Modify: `src/config.rs`
  - Remove `AppConfig.account` and `AccountConfig`.
  - Remove account config default test.
- Modify: `src/storage.rs`
  - Remove `insert_account_fund_snapshot`, `load_latest_account_fund_snapshot`, `StoredAccountFundSnapshot` if unused after account monitor migration.
  - Remove the round-trip test for account fund snapshot persistence.
  - Keep `CREATE TABLE IF NOT EXISTS account_fund_snapshots` schema block intact.
- Modify: `README.md`
  - Update account monitor description if it still says configurable or DB-backed.

## Constraints

- Do not start the full main program.
- Do not connect to real CLOB/WS in tests.
- Do not place or cancel orders.
- Do not write business DB during verification.
- Do not commit git changes.
- Follow TDD: write each failing test first, run it to verify the expected failure, then implement minimal code.

---

### Task 1: Add account latest-state handle

**Files:**
- Modify: `src/account.rs`

- [ ] **Step 1: Write the failing test for latest snapshot query**

Add this test inside `#[cfg(test)] mod tests` in `src/account.rs`:

```rust
#[test]
fn account_read_handle_returns_latest_snapshot() {
    let (handle, tx) = AccountReadHandle::new_for_test();
    assert!(handle.latest().is_none());

    let snapshot = AccountFundSnapshot {
        checked_at_ms: 42,
        balance: Decimal::from(100u32),
        allowances_json: r#"{"0xabc":"123"}"#.to_string(),
    };
    tx.send_replace(Some(snapshot.clone()));

    assert_eq!(handle.latest(), Some(snapshot));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test account::tests::account_read_handle_returns_latest_snapshot
```

Expected: fail to compile because `AccountReadHandle` and `new_for_test` do not exist.

- [ ] **Step 3: Implement minimal handle**

In `src/account.rs`, add:

```rust
#[derive(Debug, Clone)]
pub struct AccountReadHandle {
    rx: tokio::sync::watch::Receiver<Option<AccountFundSnapshot>>,
}

impl AccountReadHandle {
    pub fn latest(&self) -> Option<AccountFundSnapshot> {
        self.rx.borrow().clone()
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<AccountFundSnapshot>> {
        self.rx.clone()
    }

    #[cfg(test)]
    fn new_for_test() -> (
        Self,
        tokio::sync::watch::Sender<Option<AccountFundSnapshot>>,
    ) {
        let (tx, rx) = tokio::sync::watch::channel(None);
        (Self { rx }, tx)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```powershell
cargo test account::tests::account_read_handle_returns_latest_snapshot
```

Expected: pass.

---

### Task 2: Add account subscription API semantics

**Files:**
- Modify: `src/account.rs`

- [ ] **Step 1: Write the failing test for subscriptions**

Add this async test inside `src/account.rs` tests:

```rust
#[tokio::test]
async fn account_read_handle_subscriber_receives_latest_snapshot() {
    let (handle, tx) = AccountReadHandle::new_for_test();
    let mut subscriber = handle.subscribe();

    let snapshot = AccountFundSnapshot {
        checked_at_ms: 100,
        balance: Decimal::from(200u32),
        allowances_json: r#"{"0xdef":"456"}"#.to_string(),
    };
    tx.send_replace(Some(snapshot.clone()));

    subscriber.changed().await.expect("watch sender should stay alive");
    assert_eq!(subscriber.borrow().clone(), Some(snapshot));
}
```

- [ ] **Step 2: Run test to verify it fails or proves missing behavior**

Run:

```powershell
cargo test account::tests::account_read_handle_subscriber_receives_latest_snapshot
```

Expected before Task 1 implementation: compile failure. If Task 1 is already complete, this should pass and confirms subscription semantics; do not add extra production code.

- [ ] **Step 3: Keep implementation minimal**

If the test passes with Task 1 code, make no production changes. If it fails because `subscribe()` is missing, add only this method to `impl AccountReadHandle`:

```rust
pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<AccountFundSnapshot>> {
    self.rx.clone()
}
```

- [ ] **Step 4: Run account tests**

Run:

```powershell
cargo test account::tests
```

Expected: all account tests pass.

---

### Task 3: Convert account monitor to always-on in-memory service

**Files:**
- Modify: `src/account.rs`

- [ ] **Step 1: Write the failing test for fixed interval constant**

Add this test in `src/account.rs` tests:

```rust
#[test]
fn account_monitor_poll_interval_is_one_second() {
    assert_eq!(ACCOUNT_MONITOR_POLL_INTERVAL, Duration::from_secs(1));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```powershell
cargo test account::tests::account_monitor_poll_interval_is_one_second
```

Expected: fail to compile because `ACCOUNT_MONITOR_POLL_INTERVAL` does not exist.

- [ ] **Step 3: Implement constant and spawn entrypoint**

In `src/account.rs`:

1. Replace `use crate::config::{AccountConfig, AuthConfig};` with:

```rust
use crate::config::AuthConfig;
```

2. Remove `use crate::storage::MarketStore;`.

3. Add near the top:

```rust
pub const ACCOUNT_MONITOR_POLL_INTERVAL: Duration = Duration::from_secs(1);
```

4. Replace the public runtime entrypoint with:

```rust
pub fn spawn_account_monitor(auth: AuthConfig) -> AccountReadHandle {
    let (tx, rx) = tokio::sync::watch::channel(None);
    tokio::spawn(run(auth, tx));
    AccountReadHandle { rx }
}

async fn run(
    auth: AuthConfig,
    tx: tokio::sync::watch::Sender<Option<AccountFundSnapshot>>,
) {
    let client = match build_authenticated_clob_client(&auth).await {
        Ok(client) => client,
        Err(error) => {
            warn!(target: "order", error = %error, "account_monitor 构建 CLOB 客户端失败，账户资金监控退出");
            return;
        }
    };

    loop {
        match fetch_account_fund_snapshot(&client).await {
            Ok(snapshot) => {
                info!(
                    target: "order",
                    checked_at_ms = snapshot.checked_at_ms,
                    balance = %snapshot.balance,
                    allowances_json = %snapshot.allowances_json,
                    "account_monitor 账户资金快照同步完成"
                );
                tx.send_replace(Some(snapshot));
            }
            Err(error) => {
                warn!(target: "order", error = %error, "account_monitor 查询账户资金快照失败");
            }
        }
        tokio::time::sleep(ACCOUNT_MONITOR_POLL_INTERVAL).await;
    }
}
```

- [ ] **Step 4: Remove DB write behavior from account monitor**

Delete the old `run(auth, config, store)` implementation that referenced:

```rust
config.refresh_interval_secs
config.store_enabled
store.insert_account_fund_snapshot(...)
```

- [ ] **Step 5: Run targeted account tests**

Run:

```powershell
cargo test account::tests
```

Expected: all account tests pass.

---

### Task 4: Remove account config from application config

**Files:**
- Modify: `src/config.rs`

- [ ] **Step 1: Write the failing config expectation**

Update or add a config test that asserts account config is no longer part of defaults. If the existing test is named `account_config_defaults_to_disabled_read_only_monitor`, replace it with a test that parses minimal config without account config:

```rust
#[test]
fn app_config_does_not_require_account_section() {
    let toml = r#"
[auth]
private_key = ""
funder = ""

[app]
"#;

    let config: AppConfig = toml::from_str(toml).expect("minimal config should parse without account section");
    assert!(!config.app.assets_file.contains("account"));
}
```

- [ ] **Step 2: Run test before cleanup**

Run:

```powershell
cargo test config::tests::app_config_does_not_require_account_section
```

Expected: pass or compile depending on existing test setup. If it passes immediately, proceed with cleanup and rely on `cargo check` to catch remaining `AccountConfig` users.

- [ ] **Step 3: Remove account config type and field**

In `src/config.rs`:

1. Remove from `AppConfig`:

```rust
#[serde(default)]
pub(crate) account: AccountConfig,
```

2. Delete this type and default impl:

```rust
#[derive(Debug, Deserialize, Clone)]
pub(crate) struct AccountConfig {
    #[serde(default)]
    pub(crate) enabled: bool,
    #[serde(default = "default_account_refresh_interval_secs")]
    pub(crate) refresh_interval_secs: u64,
    #[serde(default)]
    pub(crate) store_enabled: bool,
}

impl Default for AccountConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            refresh_interval_secs: default_account_refresh_interval_secs(),
            store_enabled: false,
        }
    }
}
```

3. Delete `default_account_refresh_interval_secs()` if it becomes unused.

4. Delete the old test:

```rust
fn account_config_defaults_to_disabled_read_only_monitor()
```

- [ ] **Step 4: Run config tests**

Run:

```powershell
cargo test config::tests
```

Expected: all config tests pass.

---

### Task 5: Start account state service unconditionally from main

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Write the failing test for removed config gate**

In `src/main.rs` tests, replace `account_monitor_enabled_respects_account_config` with:

```rust
#[test]
fn account_monitor_starts_without_config_gate() {
    let _spawn_fn: fn(config::AuthConfig) -> account::AccountReadHandle = account::spawn_account_monitor;
}
```

- [ ] **Step 2: Run test to verify failure before wiring cleanup**

Run:

```powershell
cargo test main::tests::account_monitor_starts_without_config_gate
```

Expected: compile failure until `account::spawn_account_monitor` exists and is public.

- [ ] **Step 3: Update main startup**

In `main()` replace:

```rust
spawn_account_monitor(&app_config, market_store.clone());
```

with:

```rust
let _account_read_handle = account::spawn_account_monitor(app_config.auth.clone());
```

- [ ] **Step 4: Remove old helper functions**

Delete from `src/main.rs`:

```rust
fn account_monitor_enabled(config: &AccountConfig) -> bool { ... }

fn spawn_account_monitor(app_config: &AppConfig, market_store: MarketStore) { ... }
```

Remove `AccountConfig` from this import:

```rust
use config::{AccountConfig, AppConfig, load_app_config};
```

so it becomes:

```rust
use config::{AppConfig, load_app_config};
```

- [ ] **Step 5: Run main tests**

Run:

```powershell
cargo test main::tests
```

Expected: all main tests pass.

---

### Task 6: Remove account snapshot runtime storage APIs

**Files:**
- Modify: `src/storage.rs`

- [ ] **Step 1: Confirm active code no longer uses account snapshot storage APIs**

Run:

```powershell
cargo check
```

Expected before deletion: compile succeeds after Tasks 1-5, proving account monitor no longer requires storage methods.

- [ ] **Step 2: Delete runtime APIs and tests**

In `src/storage.rs`, delete these methods if no active code uses them:

```rust
pub fn insert_account_fund_snapshot(
    &self,
    checked_at_ms: u64,
    balance: &str,
    allowances_json: &str,
) -> anyhow::Result<()> { ... }

pub fn load_latest_account_fund_snapshot(
    &self,
) -> anyhow::Result<Option<StoredAccountFundSnapshot>> { ... }
```

Delete `StoredAccountFundSnapshot` if it becomes unused.

Delete the test:

```rust
fn account_fund_snapshot_round_trips_latest_snapshot()
```

Do not delete this schema block:

```sql
CREATE TABLE IF NOT EXISTS account_fund_snapshots (...)
```

- [ ] **Step 3: Run storage tests**

Run:

```powershell
cargo test storage::tests
```

Expected: storage tests pass.

---

### Task 7: Update README and run final verification

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Search README for stale account monitor wording**

Use the Grep tool, not shell grep, for these patterns:

```text
account_monitor
account_fund_snapshots
store_enabled
refresh_interval_secs
```

- [ ] **Step 2: Update README text**

If README describes account monitor as configurable or DB-backed, replace that wording with:

```markdown
账户资金状态服务默认启动，每秒查询一次 CLOB collateral balance/allowance，并通过内存最新状态 handle 提供查询和订阅；不再写入行情库。
```

- [ ] **Step 3: Run formatting and targeted tests**

Run:

```powershell
cargo fmt --check
cargo test account::tests
cargo test config::tests
cargo test main::tests
cargo test storage::tests
```

Expected: all pass.

- [ ] **Step 4: Run final verification**

Run:

```powershell
cargo check
cargo test
```

Expected: both pass. Existing dead-code warnings may remain; fix only warnings introduced by this change.

- [ ] **Step 5: Stop for user review**

Report:

```text
Account state service plan implementation complete for review.
Changed: account monitor now defaults on, polls every second, publishes latest state through watch handle, no DB writes.
Verified: cargo fmt --check, cargo check, cargo test.
Not done: no git commit, no main program run, no real CLOB connection in tests.
```

---

## Self-Review

- Spec coverage: covers default-on polling, fixed one-second interval, watch-based latest query/subscription, removal of config, removal of DB writes, and main wiring.
- Placeholder scan: no TBD/TODO/fill-in placeholders remain.
- Type consistency: `AccountFundSnapshot`, `AccountReadHandle`, `spawn_account_monitor`, `latest`, and `subscribe` names are consistent across tasks.
- Scope check: the plan intentionally does not wire account balance into risk logic and does not perform SQLite schema migration.

## Execution Options

Do not commit git changes during execution unless the user explicitly requests it. Stop for user review after implementation and verification.
