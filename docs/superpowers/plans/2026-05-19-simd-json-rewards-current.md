# Simd JSON Rewards Current Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Parse the direct Polymarket rewards current REST response with `simd-json` instead of `reqwest`'s serde_json-backed `.json()` helper.

**Architecture:** Keep the existing request construction, authentication headers, endpoint URL, and return type unchanged in `load_current_rewards_page`. Replace only the response body decoding path: status-check the response, collect bytes, copy them into a mutable `Vec<u8>`, and deserialize with `simd_json::from_slice` into `Page<RewardCurrentMarket>`. SDK-managed Polymarket REST/WS paths and non-hot-path `serde_json` usage remain unchanged.

**Tech Stack:** Rust, reqwest, simd-json with `serde_impl`, anyhow, existing `Page<RewardCurrentMarket>` serde model, cargo test/check.

---

## Files

- Modify: `src/reward_market_cache.rs:334-347`
  - Change only `load_current_rewards_page` response parsing.
  - Keep `reqwest::Client`, `create_clob_l2_headers`, `request_with_headers`, and URL/cursor behavior unchanged.
- No Cargo dependency changes; `simd-json` is already enabled in `Cargo.toml` with `serde_impl`.

## Constraints

- Do not replace SDK-internal JSON parsing; it is not controlled by this crate.
- Do not replace unrelated `serde_json` usages in account snapshots, notification payloads, position event storage, or tests.
- Do not run the full main program or contact production Polymarket services as part of verification.
- Do not commit unless the user explicitly asks.

---

### Task 1: Parse rewards current REST page with simd-json

**Files:**
- Modify: `src/reward_market_cache.rs:334-347`

- [ ] **Step 1: Inspect the current function**

Confirm the function currently ends with `reqwest`'s `.json().await?` helper:

```rust
async fn load_current_rewards_page(
    auth: &AuthConfig,
    next_cursor: Option<String>,
) -> anyhow::Result<Page<RewardCurrentMarket>> {
    let cursor = next_cursor.map_or(String::new(), |cursor| format!("?next_cursor={cursor}"));
    let url = format!("https://clob.polymarket.com/rewards/markets/current{cursor}");
    let http_client = reqwest::Client::new();
    let request = http_client.request(Method::GET, url).build()?;
    let headers = create_clob_l2_headers(auth, &request)?;
    let response = http_client
        .execute(request_with_headers(request, headers)?)
        .await?;
    Ok(response.error_for_status()?.json().await?)
}
```

- [ ] **Step 2: Replace response decoding with simd-json**

Change only the tail of the function to status-check, read bytes, and deserialize from a mutable slice:

```rust
async fn load_current_rewards_page(
    auth: &AuthConfig,
    next_cursor: Option<String>,
) -> anyhow::Result<Page<RewardCurrentMarket>> {
    let cursor = next_cursor.map_or(String::new(), |cursor| format!("?next_cursor={cursor}"));
    let url = format!("https://clob.polymarket.com/rewards/markets/current{cursor}");
    let http_client = reqwest::Client::new();
    let request = http_client.request(Method::GET, url).build()?;
    let headers = create_clob_l2_headers(auth, &request)?;
    let response = http_client
        .execute(request_with_headers(request, headers)?)
        .await?;
    let mut body = response.error_for_status()?.bytes().await?.to_vec();
    Ok(simd_json::from_slice(&mut body)?)
}
```

Expected compile behavior:
- `simd_json::from_slice` can deserialize into `Page<RewardCurrentMarket>` because `simd-json` is already compiled with `serde_impl`.
- The mutable `Vec<u8>` is required because `simd-json` may operate in-place while parsing.

- [ ] **Step 3: Run a focused compile/test check**

Run:

```bash
cargo test reward_market_cache --no-fail-fast
```

Expected:
- The crate compiles.
- Existing `reward_market_cache` tests pass.
- No network call is required by these tests.

- [ ] **Step 4: Run a full compile check if focused tests pass**

Run:

```bash
cargo check
```

Expected:
- `cargo check` finishes successfully.
- No new warnings are introduced by this change.

- [ ] **Step 5: Report verification result**

Report exactly what changed and which commands passed or failed. If a command fails for pre-existing unrelated reasons, include the first relevant error and do not claim the change is fully verified.
