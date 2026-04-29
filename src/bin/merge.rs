/// 将一对 YES/NO 头寸合并回 USDC。
///
/// 调用 Polygon 上的 CTF（ConditionalTokens）合约的 `mergePositions`：
/// 燃烧等量 YES + NO token，换回相同数量的 USDC。
///
/// 用法：
///   merge <condition_id> <amount_usdc>
///
/// 参数：
///   condition_id — 市场 condition ID，0x 开头的 64 位十六进制
///   amount_usdc  — 要合并的 USDC 数量（例如 100.5）
///
/// 注意：此工具直接用 private_key 地址发交易。
/// 若头寸由 Gnosis Safe（proxy wallet）持有，需确保 private_key 对应的地址
/// 即为 funder，或通过 Safe execTransaction 路由（当前版本暂不支持）。
use std::str::FromStr;

use alloy::network::{Ethereum, EthereumWallet};
use alloy::primitives::{Address, FixedBytes, U256, address};
use alloy::providers::{PendingTransactionBuilder, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use anyhow::Context as _;
use config::{Config, File};
use serde::Deserialize;
use url::Url;

// Polygon 主网合约地址
const CTF_ADDRESS: Address = address!("4D97DCd97eC945f40cF65F87097ACe5EA0476045");
const USDC_ADDRESS: Address = address!("2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

sol! {
    #[sol(rpc)]
    interface IConditionalTokens {
        function mergePositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] partition,
            uint256 amount
        ) external;
    }
}

#[derive(Deserialize)]
struct AuthSection {
    private_key: String,
}

#[derive(Deserialize, Default)]
struct ChainSection {
    #[serde(default = "default_rpc_url")]
    rpc_url: String,
}

fn default_rpc_url() -> String {
    "https://polygon-rpc.com".to_string()
}

#[derive(Deserialize)]
struct MergeConfig {
    auth: AuthSection,
    #[serde(default)]
    chain: ChainSection,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("用法: merge <condition_id> <amount_usdc>");
        eprintln!("示例: merge 0xabcdef...1234 100.0");
        eprintln!();
        eprintln!("condition_id: 市场的 condition ID，0x 开头 64 位十六进制");
        eprintln!("amount_usdc:  要合并的数量（单位 USDC，例如 50.0）");
        std::process::exit(1);
    }

    let condition_id = &args[1];
    let amount_usdc: f64 = args[2]
        .parse()
        .context("解析 amount_usdc 失败，应为浮点数")?;
    if amount_usdc <= 0.0 {
        anyhow::bail!("amount_usdc 必须大于 0");
    }

    let cfg = load_config()?;

    let signer = PrivateKeySigner::from_str(&cfg.auth.private_key)
        .context("解析 private_key 失败，应为 0x 开头的 64 位十六进制")?;
    let from_addr = signer.address();
    let wallet = EthereumWallet::from(signer);

    let rpc_url: Url = cfg.chain.rpc_url.parse().context("解析 rpc_url 失败")?;
    let provider = ProviderBuilder::new().wallet(wallet).connect_http(rpc_url);

    let ctf = IConditionalTokens::new(CTF_ADDRESS, &provider);

    let condition_bytes: FixedBytes<32> = condition_id
        .parse()
        .context("解析 condition_id 失败，应为 0x 开头的 64 位十六进制字符串")?;

    // USDC 精度 6 位，1 USDC = 1_000_000 units
    let amount_units = (amount_usdc * 1_000_000.0).round() as u64;
    let amount = U256::from(amount_units);

    // partition [1, 2]: YES=indexSet 1 (bit0), NO=indexSet 2 (bit1)
    let partition = vec![U256::from(1u64), U256::from(2u64)];

    println!("=== Polymarket 头寸合并 ===");
    println!("  from:         {from_addr}");
    println!("  rpc_url:      {}", cfg.chain.rpc_url);
    println!("  condition_id: {condition_id}");
    println!("  amount:       {amount_usdc} USDC ({amount_units} units)");
    println!("  ctf_contract: {CTF_ADDRESS}");
    println!();
    println!("提交交易中...");

    let tx_builder = ctf.mergePositions(
        USDC_ADDRESS,
        FixedBytes::ZERO,
        condition_bytes,
        partition,
        amount,
    );

    let pending: PendingTransactionBuilder<Ethereum> = tx_builder
        .send()
        .await
        .context("发送 mergePositions 交易失败")?;
    let tx_hash = *pending.tx_hash();
    println!("交易已提交: {tx_hash:#x}");
    println!("等待链上确认...");

    let receipt = pending.get_receipt().await.context("等待交易回执失败")?;

    if receipt.status() {
        println!();
        println!("✓ 合并成功");
        println!("  tx_hash:  {tx_hash:#x}");
        println!("  gas_used: {}", receipt.gas_used);
        println!("  已换回 {amount_usdc} USDC");
    } else {
        anyhow::bail!("合并交易失败（revert），tx_hash={tx_hash:#x}");
    }

    Ok(())
}

fn load_config() -> anyhow::Result<MergeConfig> {
    let resolve = |name: &str| -> String {
        if std::path::Path::new(name).exists() {
            name.to_string()
        } else if let Ok(mut exe_path) = std::env::current_exe() {
            exe_path.pop();
            exe_path.push(name);
            exe_path.to_string_lossy().to_string()
        } else {
            name.to_string()
        }
    };

    let settings = Config::builder()
        .add_source(File::with_name(&resolve("config.toml")).required(false))
        .add_source(File::with_name(&resolve("config.local.toml")).required(false))
        .build()?;
    Ok(settings.try_deserialize()?)
}
