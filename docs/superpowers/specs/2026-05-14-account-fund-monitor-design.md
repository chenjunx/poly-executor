# Account Fund Monitor Design

## Goal

Add a read-only account fund monitor that periodically observes the Polymarket CLOB account funding state for operational visibility.

## Scope

The first version only reads and records account fund state. It does not affect strategy decisions or perform any fund-changing operation.

In scope:

- Query the authenticated CLOB `balance-allowance` endpoint for collateral USDC.
- Log the observed balance and allowance on a fixed interval.
- Optionally persist snapshots to SQLite for later troubleshooting.
- Keep the module isolated so later risk-budget logic can consume the same snapshot source.

Out of scope:

- Token approval or allowance refresh calls.
- Transfer, deposit, withdraw, or any on-chain fund movement.
- Pausing strategies or blocking orders when balance is low.
- Per-strategy budgets, per-market budgets, or open-order reservation accounting.
- Account equity calculation from positions.

## Configuration

Add an `account` section to runtime configuration:

```toml
[account]
enabled = true
refresh_interval_secs = 60
store_enabled = true
```

Defaults:

- `enabled = false`
- `refresh_interval_secs = 60`
- `store_enabled = false`

This keeps the feature opt-in and prevents unexpected database growth when the user only wants logs.

## Components

### `src/account.rs`

Responsibilities:

- Build or receive an authenticated CLOB client using the existing auth configuration.
- Periodically query collateral balance/allowance.
- Convert the SDK response into an internal `AccountFundSnapshot`.
- Log successful snapshots and query failures.
- Write snapshots to storage only when `store_enabled` is true.

Core data shape:

```rust
pub struct AccountFundSnapshot {
    pub checked_at_ms: u64,
    pub balance: Decimal,
    pub allowance: Decimal,
}
```

### `src/config.rs`

Add:

```rust
pub(crate) struct AccountConfig {
    pub(crate) enabled: bool,
    pub(crate) refresh_interval_secs: u64,
    pub(crate) store_enabled: bool,
}
```

`AppConfig` includes `account: AccountConfig` with defaults.

### `src/storage.rs`

If `store_enabled` is true, persist snapshots to a new table:

```sql
CREATE TABLE IF NOT EXISTS account_fund_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    checked_at_ms INTEGER NOT NULL,
    balance TEXT NOT NULL,
    allowance TEXT NOT NULL
);
```

The values remain strings to match existing decimal storage patterns and avoid floating-point precision loss.

### `src/main.rs`

When `account.enabled` is true, spawn the account monitor task during startup. It runs independently from strategies, market loaders, order websocket handling, and position synchronization.

## Data Flow

1. Main loads config.
2. If account monitoring is enabled, main starts `account::run`.
3. The account module builds an authenticated CLOB client from `AuthConfig`.
4. Every `refresh_interval_secs`, it calls `balance_allowance` with `AssetType::Collateral`.
5. On success, it logs the snapshot and optionally writes it to SQLite.
6. On failure, it logs a warning and retries on the next interval.

## Failure Handling

- A failed account query must not stop strategies or other background tasks.
- The account task retries on the next interval.
- Database write failures are logged but do not stop the monitor.
- The monitor never calls `update_balance_allowance`, token approvals, or fund movement APIs.

## Testing

- Unit test `AccountConfig` defaults.
- Unit test account snapshot conversion from SDK balance/allowance response.
- Unit test SQLite insert/query for account fund snapshots if storage is enabled.
- Do not include live CLOB calls in automated tests.

## Future Extensions

This design leaves room for later phases:

- Global balance risk guard that pauses opening new positions.
- Manual resume after insufficient funds.
- Per-strategy or per-market budget allocation.
- Open-order reservation accounting.
- Account equity calculation from USDC plus positions.
