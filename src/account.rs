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
    let allowances_json =
        serde_json::to_string(&allowances).context("serialize account allowance map")?;
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
        let response = BalanceAllowanceResponse::builder()
            .balance(Decimal::from(100u32))
            .allowances(allowances)
            .build();

        let snapshot =
            snapshot_from_balance_response(42, response).expect("snapshot conversion should work");

        assert_eq!(snapshot.checked_at_ms, 42);
        assert_eq!(snapshot.balance, Decimal::from(100u32));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&snapshot.allowances_json).unwrap(),
            json!({"0x0000000000000000000000000000000000000001":"123.45"})
        );
    }
}
