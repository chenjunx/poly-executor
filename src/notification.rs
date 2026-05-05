use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow};
use base64::Engine as _;
use hmac::{Hmac, Mac};
use polymarket_client_sdk_v2::types::Decimal;
use serde::Deserialize;
use serde_json::json;
use sha2::Sha256;
use tokio::sync::mpsc;
use tracing::{info, warn};
use url::form_urlencoded;

use crate::config::DingtalkConfig;
use crate::strategy::QuoteSide;

#[derive(Clone)]
pub(crate) struct Notifier {
    tx: mpsc::Sender<NotificationEvent>,
}

#[derive(Debug, Clone)]
pub(crate) enum NotificationEvent {
    LiquidityRewardFill(LiquidityRewardFillNotification),
    LiquidityRewardUnwindAction(LiquidityRewardUnwindActionNotification),
    LiquidityRewardPoolRemoval(LiquidityRewardPoolRemovalNotification),
}

#[derive(Debug, Clone)]
pub(crate) struct LiquidityRewardFillNotification {
    pub(crate) strategy: String,
    pub(crate) topic: Option<String>,
    pub(crate) token: String,
    pub(crate) local_order_id: String,
    pub(crate) remote_order_id: String,
    pub(crate) side: QuoteSide,
    pub(crate) order_price: Decimal,
    pub(crate) order_size: Decimal,
    pub(crate) delta_size: Decimal,
    pub(crate) total_matched_size: Decimal,
    pub(crate) market: String,
    pub(crate) asset_id: String,
    pub(crate) ws_price: String,
    pub(crate) ws_original_size: Option<String>,
    pub(crate) ws_size_matched: Option<String>,
    pub(crate) ws_status: String,
    pub(crate) ws_msg_type: String,
    pub(crate) ws_timestamp: Option<i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct LiquidityRewardUnwindActionNotification {
    pub(crate) strategy: String,
    pub(crate) topic: Option<String>,
    pub(crate) token: String,
    pub(crate) local_order_id: String,
    pub(crate) side: QuoteSide,
    pub(crate) price: Decimal,
    pub(crate) order_size: Decimal,
    pub(crate) attempts: u8,
    pub(crate) action: String,
    pub(crate) simulated: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct LiquidityRewardPoolRemovalNotification {
    pub(crate) strategy: String,
    pub(crate) condition_id: String,
    pub(crate) market_slug: Option<String>,
    pub(crate) question: Option<String>,
    pub(crate) token1: String,
    pub(crate) token2: String,
    pub(crate) reason: String,
    pub(crate) token1_best_bid: Option<String>,
    pub(crate) token1_best_ask: Option<String>,
    pub(crate) token1_spread: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DingtalkResponse {
    errcode: i64,
    errmsg: String,
}

pub(crate) fn spawn_dingtalk_notifier(config: DingtalkConfig) -> Option<Notifier> {
    if !config.enabled || config.webhook.is_empty() {
        return None;
    }

    let (tx, rx) = mpsc::channel(config.queue_size.max(1));
    tokio::spawn(run_dingtalk_worker(config, rx));
    Some(Notifier { tx })
}

impl Notifier {
    pub(crate) fn try_notify(&self, event: NotificationEvent) {
        if let Err(error) = self.tx.try_send(event) {
            warn!(target: "notification", error = %error, "通知队列已满或已关闭，丢弃通知事件");
        }
    }
}

async fn run_dingtalk_worker(config: DingtalkConfig, mut rx: mpsc::Receiver<NotificationEvent>) {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(config.timeout_secs.max(1)))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            warn!(target: "notification", error = %error, "初始化钉钉通知 HTTP client 失败");
            return;
        }
    };

    info!(target: "notification", "钉钉通知 worker 已启动");

    while let Some(event) = rx.recv().await {
        if let Err(error) = send_dingtalk_message(&client, &config, &event).await {
            warn!(target: "notification", error = %error, "发送钉钉通知失败");
        }
    }
}

async fn send_dingtalk_message(
    client: &reqwest::Client,
    config: &DingtalkConfig,
    event: &NotificationEvent,
) -> anyhow::Result<()> {
    let timestamp_ms = now_ms()?;
    let url = signed_webhook_url(&config.webhook, &config.secret, timestamp_ms)?;
    let payload = build_dingtalk_payload(event);

    let response = client.post(url).json(&payload).send().await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if !status.is_success() {
        return Err(anyhow!("钉钉 webhook HTTP 状态异常: {status}, body={body}"));
    }

    let dingtalk_response: DingtalkResponse = serde_json::from_str(&body)
        .with_context(|| format!("钉钉 webhook 返回非预期 JSON: {body}"))?;
    if dingtalk_response.errcode != 0 {
        return Err(anyhow!(
            "钉钉 webhook 返回错误: errcode={}, errmsg={}",
            dingtalk_response.errcode,
            dingtalk_response.errmsg
        ));
    }

    match event {
        NotificationEvent::LiquidityRewardFill(fill) => {
            info!(
                target: "notification",
                local_order_id = %fill.local_order_id,
                remote_order_id = %fill.remote_order_id,
                delta_size = %fill.delta_size,
                "钉钉 liquidity_reward 成交通知发送成功"
            );
        }
        NotificationEvent::LiquidityRewardUnwindAction(unwind) => {
            info!(
                target: "notification",
                local_order_id = %unwind.local_order_id,
                action = %unwind.action,
                attempts = unwind.attempts,
                "钉钉 liquidity_reward 市价止损动作通知发送成功"
            );
        }
        NotificationEvent::LiquidityRewardPoolRemoval(removal) => {
            info!(
                target: "notification",
                condition_id = %removal.condition_id,
                reason = %removal.reason,
                "钉钉 liquidity_reward 奖励池剔除通知发送成功"
            );
        }
    }
    Ok(())
}

fn build_dingtalk_payload(event: &NotificationEvent) -> serde_json::Value {
    match event {
        NotificationEvent::LiquidityRewardFill(fill) => {
            let text = build_liquidity_reward_fill_markdown(fill);
            json!({
                "msgtype": "markdown",
                "markdown": {
                    "title": "liquidity_reward 成交通知",
                    "text": text,
                },
                "at": {
                    "isAtAll": false,
                },
            })
        }
        NotificationEvent::LiquidityRewardUnwindAction(unwind) => {
            let text = build_liquidity_reward_unwind_action_markdown(unwind);
            json!({
                "msgtype": "markdown",
                "markdown": {
                    "title": "liquidity_reward 市价止损卖出动作",
                    "text": text,
                },
                "at": {
                    "isAtAll": false,
                },
            })
        }
        NotificationEvent::LiquidityRewardPoolRemoval(removal) => {
            let text = build_liquidity_reward_pool_removal_markdown(removal);
            json!({
                "msgtype": "markdown",
                "markdown": {
                    "title": "liquidity_reward 奖励池剔除",
                    "text": text,
                },
                "at": {
                    "isAtAll": false,
                },
            })
        }
    }
}

fn build_liquidity_reward_fill_markdown(fill: &LiquidityRewardFillNotification) -> String {
    let notify_time = chrono::Utc::now().to_rfc3339();
    format!(
        "### liquidity_reward 成交通知\n\n\
        - 策略：{}\n\
        - Topic：{}\n\
        - Token：{}\n\
        - Market：{}\n\
        - 方向：{:?}\n\
        - 本地下单价：{}\n\
        - WS 价格：{}\n\
        - 本地订单量：{}\n\
        - 本次成交：{}\n\
        - 累计成交：{}\n\
        - WS 原始数量：{}\n\
        - WS 累计成交：{}\n\
        - 状态：{}\n\
        - 消息类型：{}\n\
        - Local Order ID：{}\n\
        - Remote Order ID：{}\n\
        - Asset ID：{}\n\
        - WS 时间：{}\n\
        - 通知时间：{}",
        fill.strategy,
        fill.topic.as_deref().unwrap_or("-"),
        fill.token,
        fill.market,
        fill.side,
        fill.order_price,
        fill.ws_price,
        fill.order_size,
        fill.delta_size,
        fill.total_matched_size,
        fill.ws_original_size.as_deref().unwrap_or("-"),
        fill.ws_size_matched.as_deref().unwrap_or("-"),
        fill.ws_status,
        fill.ws_msg_type,
        fill.local_order_id,
        fill.remote_order_id,
        fill.asset_id,
        fill.ws_timestamp
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        notify_time,
    )
}

fn build_liquidity_reward_unwind_action_markdown(
    unwind: &LiquidityRewardUnwindActionNotification,
) -> String {
    let notify_time = chrono::Utc::now().to_rfc3339();
    format!(
        "### liquidity_reward 市价止损卖出动作通知\n\n\
        - 策略：{}\n\
        - Topic：{}\n\
        - Token：{}\n\
        - 动作：{}\n\
        - 方向：{:?}\n\
        - 价格：{}\n\
        - 数量：{}\n\
        - 重试次数：{}\n\
        - Local Order ID：{}\n\
        - 模拟模式：{}\n\
        - 通知时间：{}",
        unwind.strategy,
        unwind.topic.as_deref().unwrap_or("-"),
        unwind.token,
        unwind.action,
        unwind.side,
        unwind.price,
        unwind.order_size,
        unwind.attempts,
        unwind.local_order_id,
        unwind.simulated,
        notify_time,
    )
}

fn build_liquidity_reward_pool_removal_markdown(
    removal: &LiquidityRewardPoolRemovalNotification,
) -> String {
    let notify_time = chrono::Utc::now().to_rfc3339();
    format!(
        "### liquidity_reward 奖励池剔除\n\n\
        - 策略：{}\n\
        - Condition ID：{}\n\
        - Market：{}\n\
        - Question：{}\n\
        - Token1：{}\n\
        - Token2：{}\n\
        - 剔除原因：{}\n\
        - Token1 Best Bid：{}\n\
        - Token1 Best Ask：{}\n\
        - Token1 Spread：{}\n\
        - 通知时间：{}",
        removal.strategy,
        removal.condition_id,
        removal.market_slug.as_deref().unwrap_or("-"),
        removal.question.as_deref().unwrap_or("-"),
        removal.token1,
        removal.token2,
        removal.reason,
        removal.token1_best_bid.as_deref().unwrap_or("-"),
        removal.token1_best_ask.as_deref().unwrap_or("-"),
        removal.token1_spread.as_deref().unwrap_or("-"),
        notify_time,
    )
}

fn signed_webhook_url(webhook: &str, secret: &str, timestamp_ms: i64) -> anyhow::Result<String> {
    if secret.is_empty() {
        return Ok(webhook.to_string());
    }

    let string_to_sign = format!("{timestamp_ms}\n{secret}");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|error| anyhow!("初始化钉钉签名失败: {error}"))?;
    mac.update(string_to_sign.as_bytes());
    let sign = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    let encoded_sign: String = form_urlencoded::byte_serialize(sign.as_bytes()).collect();
    let separator = if webhook.contains('?') { '&' } else { '?' };
    Ok(format!(
        "{webhook}{separator}timestamp={timestamp_ms}&sign={encoded_sign}"
    ))
}

fn now_ms() -> anyhow::Result<i64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("系统时间早于 Unix epoch")?;
    Ok(duration.as_millis() as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_fill() -> LiquidityRewardFillNotification {
        LiquidityRewardFillNotification {
            strategy: "liquidity_reward".to_string(),
            topic: Some("liquidity_reward".to_string()),
            token: "token-1".to_string(),
            local_order_id: "local-1".to_string(),
            remote_order_id: "remote-1".to_string(),
            side: QuoteSide::Buy,
            order_price: Decimal::try_from(0.42_f64).unwrap(),
            order_size: Decimal::try_from(100.0_f64).unwrap(),
            delta_size: Decimal::try_from(1.5_f64).unwrap(),
            total_matched_size: Decimal::try_from(1.5_f64).unwrap(),
            market: "market-1".to_string(),
            asset_id: "asset-1".to_string(),
            ws_price: "0.42".to_string(),
            ws_original_size: Some("100".to_string()),
            ws_size_matched: Some("1.5".to_string()),
            ws_status: "partially_filled".to_string(),
            ws_msg_type: "Update".to_string(),
            ws_timestamp: Some(123456789),
        }
    }

    fn sample_pool_removal() -> LiquidityRewardPoolRemovalNotification {
        LiquidityRewardPoolRemovalNotification {
            strategy: "liquidity_reward".to_string(),
            condition_id: "0xabc".to_string(),
            market_slug: Some("test-market".to_string()),
            question: Some("Test question?".to_string()),
            token1: "token-1".to_string(),
            token2: "token-2".to_string(),
            reason: "token1_spread_gt_threshold: spread=0.4 threshold=0.1".to_string(),
            token1_best_bid: Some("0.02".to_string()),
            token1_best_ask: Some("0.42".to_string()),
            token1_spread: Some("0.4".to_string()),
        }
    }

    fn sample_unwind_action() -> LiquidityRewardUnwindActionNotification {
        LiquidityRewardUnwindActionNotification {
            strategy: "liquidity_reward".to_string(),
            topic: Some("liquidity_reward".to_string()),
            token: "token-1".to_string(),
            local_order_id: "local-unwind-1".to_string(),
            side: QuoteSide::Sell,
            price: Decimal::try_from(0.41_f64).unwrap(),
            order_size: Decimal::try_from(1.5_f64).unwrap(),
            attempts: 1,
            action: "unwind-retry".to_string(),
            simulated: false,
        }
    }

    #[test]
    fn unsigned_webhook_url_returns_original_url() {
        let url = "https://example.com/robot/send?access_token=abc";
        assert_eq!(signed_webhook_url(url, "", 123).unwrap(), url);
    }

    #[test]
    fn signed_webhook_url_adds_timestamp_and_sign() {
        let url = signed_webhook_url(
            "https://example.com/robot/send?access_token=abc",
            "secret",
            123,
        )
        .unwrap();
        assert!(url.contains("&timestamp=123&sign="));
    }

    #[test]
    fn dingtalk_payload_contains_fill_fields() {
        let payload =
            build_dingtalk_payload(&NotificationEvent::LiquidityRewardFill(sample_fill()));
        assert_eq!(payload["msgtype"], "markdown");
        let text = payload["markdown"]["text"].as_str().unwrap();
        assert!(text.contains("liquidity_reward"));
        assert!(text.contains("local-1"));
        assert!(text.contains("remote-1"));
        assert!(text.contains("1.5"));
        assert!(text.contains("partially_filled"));
    }

    #[test]
    fn dingtalk_payload_contains_pool_removal_fields() {
        let payload = build_dingtalk_payload(&NotificationEvent::LiquidityRewardPoolRemoval(
            sample_pool_removal(),
        ));
        assert_eq!(payload["msgtype"], "markdown");
        assert_eq!(
            payload["markdown"]["title"].as_str().unwrap(),
            "liquidity_reward 奖励池剔除"
        );
        let text = payload["markdown"]["text"].as_str().unwrap();
        assert!(text.contains("liquidity_reward"));
        assert!(text.contains("0xabc"));
        assert!(text.contains("test-market"));
        assert!(text.contains("Test question?"));
        assert!(text.contains("token-1"));
        assert!(text.contains("token-2"));
        assert!(text.contains("token1_spread_gt_threshold"));
        assert!(text.contains("0.02"));
        assert!(text.contains("0.42"));
        assert!(text.contains("0.4"));
    }

    #[test]
    fn dingtalk_payload_contains_unwind_action_fields() {
        let payload = build_dingtalk_payload(&NotificationEvent::LiquidityRewardUnwindAction(
            sample_unwind_action(),
        ));
        assert_eq!(payload["msgtype"], "markdown");
        assert_eq!(
            payload["markdown"]["title"].as_str().unwrap(),
            "liquidity_reward 市价止损卖出动作"
        );
        let text = payload["markdown"]["text"].as_str().unwrap();
        assert!(text.contains("liquidity_reward"));
        assert!(text.contains("token-1"));
        assert!(text.contains("local-unwind-1"));
        assert!(text.contains("unwind-retry"));
        assert!(text.contains("0.41"));
        assert!(text.contains("1.5"));
        assert!(text.contains("重试次数：1"));
        assert!(text.contains("模拟模式：false"));
    }
}
