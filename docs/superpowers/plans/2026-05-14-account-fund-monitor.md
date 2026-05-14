# Account Fund Monitor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, read-only account fund monitor that periodically logs Polymarket CLOB collateral balance and allowance state.

**Architecture:** Add a small `account` module that owns the periodic balance query loop and snapshot conversion. Store snapshots in `MarketStore` only when configured, keeping the first version independent from strategies and order execution. Wire the task from `main` behind `[account].enabled` so it does not change runtime behavior unless explicitly enabled.

**Tech Stack:** Rust 2024, Tokio, rusqlite, polymarket_client_sdk_v2 CLOB authenticated client, serde_json, tracing.

---

## File Structure

- Create `src/account.rs` — account monitor loop, snapshot data type, SDK response conversion, optional persistence call.
- Modify `src/config.rs` — add `AccountConfig` and `AppConfig.account` with safe defaults.
- Modify `src/storage.rs` — add `account_fund_snapshots` table plus insert/load-latest helpers.
- Modify `src/main.rs` — declare `mod account;` and spawn the account monitor only when enabled.

## Task 1: Add Account Config Defaults

**Files:**
- Modify: `src/config.rs:6-20`
- Modify: `src/config.rs:106-160`

- [ ] **Step 1: Write the failing config default test**

Add this test module to the bottom of `src/config.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_config_defaults_to_disabled_read_only_monitor() {
        let config = AccountConfig::default();

        assert!(!config.enabled);
        assert_eq!(config.refresh_interval_secs, 60);
        assert!(!config.store_enabled);
    }
}
```

- [ ] **Step 2: Run the config test and verify it fails**

Run:

```powershell
cargo test config::tests::account_config_defaults_to_disabled_read_only_monitor
```

Expected: FAIL to compile with `cannot find type AccountConfig` or equivalent missing-item error.

- [ ] **Step 3: Add `AccountConfig` to runtime config**

In `src/config.rs`, add an account field to `AppConfig`:

```rust
#[derive(Debug, Deserialize)]
pub(crate) struct AppConfig {
    pub(crate) proxy: ProxySettings,
    pub(crate) app: AppSettings,
    pub(crate) auth: AuthConfig,
    pub(crate) order: OrderConfig,
    #[serde(default)]
    pub(crate) simulation: SimulationConfig,
    #[serde(default, alias = "mid_requote")]
    pub(crate) liquidity_reward: LiquidityRewardConfig,
    #[serde(default)]
    pub(crate) account: AccountConfig,
    #[serde(default)]
    pub(crate) notification: NotificationConfig,
    #[serde(default)]
    pub(crate) topic_threads: HashMap<String, usize>,
}
```

Add this struct near the existing config structs:

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

fn default_account_refresh_interval_secs() -> u64 {
    60
}
```

- [ ] **Step 4: Run the config test and verify it passes**

Run:

```powershell
cargo test config::tests::account_config_defaults_to_disabled_read_only_monitor
```

Expected: PASS.

## Task 2: Persist Account Fund Snapshots in MarketStore

**Files:**
- Modify: `src/storage.rs:1-90`
- Modify: `src/storage.rs:688-864`
- Test: `src/storage.rs` test module

- [ ] **Step 1: Write the failing storage test**

Add this test to the existing `#[cfg(test)]` module in `src/storage.rs`:

```rust
#[test]
fn account_fund_snapshot_round_trips_latest_snapshot() {
    let store = MarketStore::open(":memory:").expect("store should open");
    store.init_schema().expect("schema should initialize");

    store
        .insert_account_fund_snapshot(100, "12.34", r#"{"0xabc":"999"}"#)
        .expect("first snapshot should insert");
    store
        .insert_account_fund_snapshot(200, "56.78", r#"{"0xdef":"888"}"#)
        .expect("second snapshot should insert");

    let snapshot = store
        .load_latest_account_fund_snapshot()
        .expect("latest snapshot query should work")
        .expect("latest snapshot should exist");

    assert_eq!(snapshot.checked_at_ms, 200);
    assert_eq!(snapshot.balance, "56.78");
    assert_eq!(snapshot.allowances_json, r#"{"0xdef":"888"}"#);
}
```

- [ ] **Step 2: Run the storage test and verify it fails**

Run:

```powershell
cargo test storage::tests::account_fund_snapshot_round_trips_latest_snapshot
```

Expected: FAIL to compile with missing `insert_account_fund_snapshot`, `load_latest_account_fund_snapshot`, or `StoredAccountFundSnapshot`.

- [ ] **Step 3: Add storage data type**

Near other stored data structs in `src/storage.rs`, add:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAccountFundSnapshot {
    pub checked_at_ms: u64,
    pub balance: String,
    pub allowances_json: String,
}
```

- [ ] **Step 4: Add schema table and index**

Inside `MarketStore::init_schema` SQL batch, after `trade_events` and its index, add:

```sql
CREATE TABLE IF NOT EXISTS account_fund_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checked_at_ms INTEGER NOT NULL,
    balance TEXT NOT NULL,
    allowances_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_account_fund_snapshots_checked_at
    ON account_fund_snapshots (checked_at_ms DESC);
```

- [ ] **Step 5: Add insert and latest-query methods**

Inside `impl MarketStore`, before `insert_liquidity_reward_score`, add:

```rust
pub fn insert_account_fund_snapshot(
    &self,
    checked_at_ms: u64,
    balance: &str,
    allowances_json: &str,
) -> anyhow::Result<()> {
    self.with_conn(|conn| {
        conn.execute(
            "
            INSERT INTO account_fund_snapshots (checked_at_ms, balance, allowances_json)
            VALUES (?1, ?2, ?3)
            ",
            params![checked_at_ms as i64, balance, allowances_json],
        )?;
        Ok(())
    })
}

pub fn load_latest_account_fund_snapshot(
    &self,
) -> anyhow::Result<Option<StoredAccountFundSnapshot>> {
    self.with_conn(|conn| {
        conn.query_row(
            "
            SELECT checked_at_ms, balance, allowances_json
            FROM account_fund_snapshots
            ORDER BY checked_at_ms DESC, id DESC
            LIMIT 1
            ",
            [],
            |row| {
                Ok(StoredAccountFundSnapshot {
                    checked_at_ms: row.get::<_, i64>(0)? as u64,
                    balance: row.get(1)?,
                    allowances_json: row.get(2)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    })
}
```

- [ ] **Step 6: Run the storage test and verify it passes**

Run:

```powershell
cargo test storage::tests::account_fund_snapshot_round_trips_latest_snapshot
```

Expected: PASS.

## Task 3: Add Account Monitor Module

**Files:**
- Create: `src/account.rs`
- Test: `src/account.rs`

- [ ] **Step 1: Write the failing account module tests**

Create `src/account.rs` with this initial test-first content:

```rust
use std::time::{SystemTime, UNIX_EPOCH};

use polymarket_client_sdk_v2::clob::types::response::BalanceAllowanceResponse;
use polymarket_client_sdk_v2::types::Decimal;
use serde_json::json;

#[derive(Debug, Clone, PartialEq)]
pub struct AccountFundSnapshot {
    pub checked_at_ms: u64,
    pub balance: Decimal,
    pub allowances_json: String,
}

fn now_ms() -> anyhow::Result<u64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_client_sdk_v2::types::Address;
    use std::collections::HashMap;
    use std::str::FromStr;

    #[test]
    fn snapshot_from_balance_response_serializes_allowances_as_json() {
        let address = Address::from_str("0x0000000000000000000000000000000000000001")
            .expect("address should parse");
        let mut allowances = HashMap::new();
        allowances.insert(address, "123.45".to_string());
        let response = BalanceAllowanceResponse {
            balance: Decimal::from(100u32),
            allowances,
        };

        let snapshot = snapshot_from_balance_response(42, response)
            .expect("snapshot conversion should work");

        assert_eq!(snapshot.checked_at_ms, 42);
        assert_eq!(snapshot.balance, Decimal::from(100u32));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&snapshot.allowances_json).unwrap(),
            json!({"0x0000000000000000000000000000000000000001":"123.45"})
        );
    }
}
```

- [ ] **Step 2: Run the account test and verify it fails**

Run:

```powershell
cargo test account::tests::snapshot_from_balance_response_serializes_allowances_as_json
```

Expected: FAIL to compile because `account` is not yet declared in `main.rs` and `snapshot_from_balance_response` is missing. If the test target cannot see `src/account.rs`, proceed to Task 4 Step 3 only far enough to add `mod account;`, then rerun and confirm the missing-function failure.

- [ ] **Step 3: Implement snapshot conversion and monitor run loop**

Replace `src/account.rs` with:

```rust
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use polymarket_client_sdk_v2::clob::types::AssetType;
use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk_v2::clob::types::response::BalanceAllowanceResponse;
use polymarket_client_sdk_v2::types::Decimal;
use tracing::{info, warn};

use crate::clob_client::build_authenticated_clob_client;
use crate::config::{AccountConfig, AuthConfig};
use crate::storage::MarketStore;

#[derive(Debug, Clone, PartialEq)]
pub struct AccountFundSnapshot {
    pub checked_at_ms: u64,
    pub balance: Decimal,
    pub allowances_json: String,
}

pub async fn run(auth: AuthConfig, config: AccountConfig, store: MarketStore) {
    let refresh_interval = Duration::from_secs(config.refresh_interval_secs.max(1));
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
                if config.store_enabled {
                    if let Err(error) = store.insert_account_fund_snapshot(
                        snapshot.checked_at_ms,
                        &snapshot.balance.to_string(),
                        &snapshot.allowances_json,
                    ) {
                        warn!(target: "order", error = %error, "account_monitor 账户资金快照入库失败");
                    }
                }
            }
            Err(error) => {
                warn!(target: "order", error = %error, "account_monitor 查询账户资金快照失败");
            }
        }
        tokio::time::sleep(refresh_interval).await;
    }
}

async fn fetch_account_fund_snapshot(
    client: &crate::clob_client::AuthenticatedClobClient,
) -> anyhow::Result<AccountFundSnapshot> {
    let request = BalanceAllowanceRequest::builder()
        .asset_type(AssetType::Collateral)
        .build();
    let response = client.balance_allowance(request).await?;
    snapshot_from_balance_response(now_ms()?, response)
}

fn snapshot_from_balance_response(
    checked_at_ms: u64,
    response: BalanceAllowanceResponse,
) -> anyhow::Result<AccountFundSnapshot> {
    let allowances = response
        .allowances
        .into_iter()
        .map(|(address, allowance)| (address.to_checksum(None), allowance))
        .collect::<BTreeMap<_, _>>();
    let allowances_json = serde_json::to_string(&allowances)
        .context("serialize account allowance map")?;
    Ok(AccountFundSnapshot {
        checked_at_ms,
        balance: response.balance,
        allowances_json,
    })
}

fn now_ms() -> anyhow::Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("系统时间早于 Unix epoch")?;
    Ok(duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_client_sdk_v2::types::Address;
    use serde_json::json;
    use std::collections::HashMap;
    use std::str::FromStr;

    #[test]
    fn snapshot_from_balance_response_serializes_allowances_as_json() {
        let address = Address::from_str("0x0000000000000000000000000000000000000001")
            .expect("address should parse");
        let mut allowances = HashMap::new();
        allowances.insert(address, "123.45".to_string());
        let response = BalanceAllowanceResponse {
            balance: Decimal::from(100u32),
            allowances,
        };

        let snapshot = snapshot_from_balance_response(42, response)
            .expect("snapshot conversion should work");

        assert_eq!(snapshot.checked_at_ms, 42);
        assert_eq!(snapshot.balance, Decimal::from(100u32));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&snapshot.allowances_json).unwrap(),
            json!({"0x0000000000000000000000000000000000000001":"123.45"})
        );
    }
}
```

- [ ] **Step 4: Run the account test and verify it passes**

Run:

```powershell
cargo test account::tests::snapshot_from_balance_response_serializes_allowances_as_json
```

Expected: PASS.

## Task 4: Wire Account Monitor into Startup

**Files:**
- Modify: `src/main.rs:1-20`
- Modify: `src/main.rs:97-195`
- Modify: `src/main.rs:391-448` or nearby helper area

- [ ] **Step 1: Write the failing startup helper test**

Add this test module to the bottom of `src/main.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_monitor_enabled_respects_account_config() {
        let mut config = AccountConfig::default();
        assert!(!account_monitor_enabled(&config));

        config.enabled = true;
        assert!(account_monitor_enabled(&config));
    }
}
```

Also update the `use config` import near the top of `src/main.rs` to include `AccountConfig` for the test:

```rust
use config::{AccountConfig, AppConfig, load_app_config};
```

- [ ] **Step 2: Run the startup helper test and verify it fails**

Run:

```powershell
cargo test main::tests::account_monitor_enabled_respects_account_config
```

Expected: FAIL to compile because `account_monitor_enabled` is missing.

- [ ] **Step 3: Declare account module and add helper**

At the top of `src/main.rs`, add:

```rust
mod account;
```

Near other spawn helpers, add:

```rust
fn account_monitor_enabled(config: &AccountConfig) -> bool {
    config.enabled
}

fn spawn_account_monitor(app_config: &AppConfig, market_store: MarketStore) {
    if !account_monitor_enabled(&app_config.account) {
        return;
    }

    tokio::spawn(account::run(
        app_config.auth.clone(),
        app_config.account.clone(),
        market_store,
    ));
}
```

- [ ] **Step 4: Call the spawn helper from `main`**

After stores and notifier are initialized, add the account monitor before market/strategy tasks:

```rust
spawn_account_monitor(&app_config, market_store.clone());
```

A good location is after:

```rust
let notifier = notification::spawn_dingtalk_notifier(app_config.notification.dingtalk.clone());
```

- [ ] **Step 5: Run the startup helper test and verify it passes**

Run:

```powershell
cargo test main::tests::account_monitor_enabled_respects_account_config
```

Expected: PASS.

## Task 5: Validation and Cleanup

**Files:**
- Modify only if prior tasks exposed formatting or compile issues.

- [ ] **Step 1: Run targeted tests**

Run:

```powershell
cargo test config::tests::account_config_defaults_to_disabled_read_only_monitor
cargo test storage::tests::account_fund_snapshot_round_trips_latest_snapshot
cargo test account::tests::snapshot_from_balance_response_serializes_allowances_as_json
cargo test main::tests::account_monitor_enabled_respects_account_config
```

Expected: all PASS.

- [ ] **Step 2: Run broader affected test groups**

Run:

```powershell
cargo test storage::tests
cargo test account::tests
```

Expected: all PASS.

- [ ] **Step 3: Run formatting check**

Run:

```powershell
cargo fmt --check
```

Expected: PASS. If it fails, run `cargo fmt`, then rerun `cargo fmt --check`.

- [ ] **Step 4: Run full test suite**

Run:

```powershell
cargo test
```

Expected: PASS.

- [ ] **Step 5: Inspect diff for scope**

Run:

```powershell
git diff -- src/account.rs src/config.rs src/storage.rs src/main.rs docs/superpowers/plans/2026-05-14-account-fund-monitor.md
```

Expected: diff only contains account fund monitor changes, storage snapshot table/API, config defaults, startup wiring, and this plan.

## Completion Notes

- Do not commit unless the user explicitly asks.
- Do not run the production binary to live-query the account unless the user explicitly asks.
- This first version must never call `update_balance_allowance` or any approval/fund movement API.
- This first version must not pause strategies or block orders.
