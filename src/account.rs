use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use log::{debug, warn};
use polymarket_client_sdk_v2::clob::types::AssetType;
use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk_v2::clob::types::response::BalanceAllowanceResponse;
use polymarket_client_sdk_v2::types::Decimal;

use crate::clob_client::build_authenticated_clob_client;
use crate::config::AuthConfig;

pub const ACCOUNT_MONITOR_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq)]
pub struct AccountFundSnapshot {
    pub checked_at_ms: u64,
    pub balance: Decimal,
    pub balance_raw: Decimal,
    pub allowances_json: String,
}

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

pub fn spawn_account_monitor(auth: AuthConfig) -> AccountReadHandle {
    let (tx, rx) = tokio::sync::watch::channel(None);
    tokio::spawn(run(auth, tx));
    AccountReadHandle { rx }
}

async fn run(auth: AuthConfig, tx: tokio::sync::watch::Sender<Option<AccountFundSnapshot>>) {
    let client = match build_authenticated_clob_client(&auth).await {
        Ok(client) => client,
        Err(error) => {
            warn!(target: "order", "account_monitor 构建 CLOB 客户端失败，账户资金监控退出 error={}", error);
            return;
        }
    };

    loop {
        match fetch_account_fund_snapshot(&client).await {
            Ok(snapshot) => {
                debug!(target: "order", "account_monitor 账户资金快照同步完成 checked_at_ms={:?} balance={} balance_raw={} allowances_json={}", snapshot.checked_at_ms, snapshot.balance, snapshot.balance_raw, snapshot.allowances_json);
                tx.send_replace(Some(snapshot));
            }
            Err(error) => {
                warn!(target: "order", "account_monitor 查询账户资金快照失败 error={}", error);
            }
        }
        tokio::time::sleep(ACCOUNT_MONITOR_POLL_INTERVAL).await;
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
    let allowances_json =
        serde_json::to_string(&allowances).context("serialize account allowance map")?;
    let balance_raw = response.balance;
    Ok(AccountFundSnapshot {
        checked_at_ms,
        balance: collateral_balance_to_usdc(balance_raw),
        balance_raw,
        allowances_json,
    })
}

fn collateral_balance_to_usdc(balance: Decimal) -> Decimal {
    balance / Decimal::from(1_000_000u32)
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
    fn account_read_handle_returns_latest_snapshot() {
        let (handle, tx) = AccountReadHandle::new_for_test();
        assert!(handle.latest().is_none());

        let snapshot = AccountFundSnapshot {
            checked_at_ms: 42,
            balance: Decimal::from(100u32),
            balance_raw: Decimal::from(100_000_000u32),
            allowances_json: r#"{"0xabc":"123"}"#.to_string(),
        };
        tx.send_replace(Some(snapshot.clone()));

        assert_eq!(handle.latest(), Some(snapshot));
    }

    #[tokio::test]
    async fn account_read_handle_subscriber_receives_latest_snapshot() {
        let (handle, tx) = AccountReadHandle::new_for_test();
        let mut subscriber = handle.subscribe();

        let snapshot = AccountFundSnapshot {
            checked_at_ms: 100,
            balance: Decimal::from(200u32),
            balance_raw: Decimal::from(200_000_000u32),
            allowances_json: r#"{"0xdef":"456"}"#.to_string(),
        };
        tx.send_replace(Some(snapshot.clone()));

        subscriber
            .changed()
            .await
            .expect("watch sender should stay alive");
        assert_eq!(subscriber.borrow().clone(), Some(snapshot));
    }

    #[test]
    fn account_monitor_poll_interval_is_one_second() {
        assert_eq!(ACCOUNT_MONITOR_POLL_INTERVAL, Duration::from_secs(1));
    }

    #[test]
    fn snapshot_from_balance_response_scales_collateral_balance_to_usdc() {
        let response = BalanceAllowanceResponse::builder()
            .balance(Decimal::from(875_362_001u64))
            .allowances(HashMap::new())
            .build();

        let snapshot =
            snapshot_from_balance_response(42, response).expect("snapshot conversion should work");

        assert_eq!(
            snapshot.balance,
            Decimal::from_str("875.362001").expect("decimal should parse")
        );
    }

    #[test]
    fn snapshot_from_balance_response_serializes_allowances_as_json() {
        let address = Address::from_str("0x0000000000000000000000000000000000000001")
            .expect("address should parse");
        let mut allowances = HashMap::new();
        allowances.insert(address, "123.45".to_string());
        let response = BalanceAllowanceResponse::builder()
            .balance(Decimal::from(100u32))
            .allowances(allowances)
            .build();

        let snapshot =
            snapshot_from_balance_response(42, response).expect("snapshot conversion should work");

        assert_eq!(snapshot.checked_at_ms, 42);
        assert_eq!(snapshot.balance, Decimal::try_from(0.0001_f64).unwrap());
        assert_eq!(snapshot.balance_raw, Decimal::from(100u32));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&snapshot.allowances_json).unwrap(),
            json!({"0x0000000000000000000000000000000000000001":"123.45"})
        );
    }
}
