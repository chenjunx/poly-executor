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
}
