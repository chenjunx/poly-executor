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
    LiquidityRewardManualAttention(LiquidityRewardManualAttentionNotification),
    OrderSubmitted(OrderSubmittedNotification),
    OrderFilled(OrderFilledNotification),
    RiskEvent(RiskEventNotification),
}

#[derive(Debug, Clone)]
pub(crate) struct OrderSubmittedNotification {
    pub(crate) strategy_id: String,
    pub(crate) local_order_id: String,
    pub(crate) exchange_order_id: Option<String>,
    pub(crate) market_id: String,
    pub(crate) token_id: String,
    pub(crate) side: String,
    pub(crate) order_type: String,
    pub(crate) price: Option<Decimal>,
    pub(crate) size: Decimal,
    pub(crate) event_kind: String,
}

#[derive(Debug, Clone)]
pub(crate) struct OrderFilledNotification {
    pub(crate) strategy_id: String,
    pub(crate) local_order_id: String,
    pub(crate) market_id: String,
    pub(crate) token_id: String,
    pub(crate) side: String,
    pub(crate) fill_kind: String,
    pub(crate) fill_qty: Decimal,
    pub(crate) fill_price: Decimal,
    pub(crate) cum_qty: Decimal,
    pub(crate) avg_fill_price: Option<Decimal>,
}

#[derive(Debug, Clone)]
pub(crate) struct RiskEventNotification {
    pub(crate) source: String,
    pub(crate) strategy_id: Option<String>,
    pub(crate) local_order_id: Option<String>,
    pub(crate) market_id: Option<String>,
    pub(crate) token_id: Option<String>,
    pub(crate) risk_code: String,
    pub(crate) reason: String,
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

#[derive(Debug, Clone)]
pub(crate) struct LiquidityRewardManualAttentionNotification {
    pub(crate) strategy: String,
    pub(crate) topic: Option<String>,
    pub(crate) token: String,
    pub(crate) trigger_local_order_id: String,
    pub(crate) trigger_remote_order_id: Option<String>,
    pub(crate) remaining_size: Decimal,
    pub(crate) visible_position_size: Option<Decimal>,
    pub(crate) attempts: u8,
    pub(crate) last_error: String,
    pub(crate) waited_secs: u64,
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
    #[cfg(test)]
    pub(crate) fn from_sender(tx: mpsc::Sender<NotificationEvent>) -> Self {
        Self { tx }
    }

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
        NotificationEvent::LiquidityRewardManualAttention(attention) => {
            info!(
                target: "notification",
                token = %attention.token,
                attempts = attention.attempts,
                "钉钉 liquidity_reward 人工处理通知发送成功"
            );
        }
        NotificationEvent::OrderSubmitted(submitted) => {
            info!(
                target: "notification",
                strategy_id = %submitted.strategy_id,
                local_order_id = %submitted.local_order_id,
                token_id = %submitted.token_id,
                "钉钉订单提交通知发送成功"
            );
        }
        NotificationEvent::OrderFilled(fill) => {
            info!(
                target: "notification",
                strategy_id = %fill.strategy_id,
                local_order_id = %fill.local_order_id,
                fill_qty = %fill.fill_qty,
                "钉钉订单成交通知发送成功"
            );
        }
        NotificationEvent::RiskEvent(risk) => {
            info!(
                target: "notification",
                source = %risk.source,
                risk_code = %risk.risk_code,
                "钉钉风控通知发送成功"
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
        NotificationEvent::LiquidityRewardManualAttention(attention) => {
            let text = build_liquidity_reward_manual_attention_markdown(attention);
            json!({
                "msgtype": "markdown",
                "markdown": {
                    "title": "liquidity_reward 止损需人工处理",
                    "text": text,
                },
                "at": {
                    "isAtAll": false,
                },
            })
        }
        NotificationEvent::OrderSubmitted(submitted) => {
            let text = build_order_submitted_markdown(submitted);
            json!({
                "msgtype": "markdown",
                "markdown": {
                    "title": "订单已提交",
                    "text": text,
                },
                "at": {
                    "isAtAll": false,
                },
            })
        }
        NotificationEvent::OrderFilled(fill) => {
            let text = build_order_filled_markdown(fill);
            json!({
                "msgtype": "markdown",
                "markdown": {
                    "title": "订单成交",
                    "text": text,
                },
                "at": {
                    "isAtAll": false,
                },
            })
        }
        NotificationEvent::RiskEvent(risk) => {
            let text = build_risk_event_markdown(risk);
            json!({
                "msgtype": "markdown",
                "markdown": {
                    "title": "风控事件",
                    "text": text,
                },
                "at": {
                    "isAtAll": false,
                },
            })
        }
    }
}

fn build_order_submitted_markdown(submitted: &OrderSubmittedNotification) -> String {
    let notify_time = chrono::Utc::now().to_rfc3339();
    format!(
        "### 订单已提交\n\n\
        - 策略：{}\n\
        - Local Order ID：{}\n\
        - Exchange Order ID：{}\n\
        - Market ID：{}\n\
        - Token ID：{}\n\
        - 方向：{}\n\
        - 订单类型：{}\n\
        - 价格：{}\n\
        - 数量：{}\n\
        - 事件：{}\n\
        - 通知时间：{}",
        submitted.strategy_id,
        submitted.local_order_id,
        submitted.exchange_order_id.as_deref().unwrap_or("-"),
        submitted.market_id,
        submitted.token_id,
        submitted.side,
        submitted.order_type,
        submitted
            .price
            .map(|price| price.to_string())
            .unwrap_or_else(|| "-".to_string()),
        submitted.size,
        submitted.event_kind,
        notify_time,
    )
}

fn build_order_filled_markdown(fill: &OrderFilledNotification) -> String {
    let notify_time = chrono::Utc::now().to_rfc3339();
    format!(
        "### 订单成交\n\n\
        - 策略：{}\n\
        - Local Order ID：{}\n\
        - Market ID：{}\n\
        - Token ID：{}\n\
        - 方向：{}\n\
        - 成交类型：{}\n\
        - 本次成交数量：{}\n\
        - 本次成交价格：{}\n\
        - 累计成交数量：{}\n\
        - 平均成交价格：{}\n\
        - 通知时间：{}",
        fill.strategy_id,
        fill.local_order_id,
        fill.market_id,
        fill.token_id,
        fill.side,
        fill.fill_kind,
        fill.fill_qty,
        fill.fill_price,
        fill.cum_qty,
        fill.avg_fill_price
            .map(|price| price.to_string())
            .unwrap_or_else(|| "-".to_string()),
        notify_time,
    )
}

fn build_risk_event_markdown(risk: &RiskEventNotification) -> String {
    let notify_time = chrono::Utc::now().to_rfc3339();
    format!(
        "### 风控事件\n\n\
        - 来源：{}\n\
        - 策略：{}\n\
        - Local Order ID：{}\n\
        - Market ID：{}\n\
        - Token ID：{}\n\
        - 风控代码：{}\n\
        - 原因：{}\n\
        - 通知时间：{}",
        risk.source,
        risk.strategy_id.as_deref().unwrap_or("-"),
        risk.local_order_id.as_deref().unwrap_or("-"),
        risk.market_id.as_deref().unwrap_or("-"),
        risk.token_id.as_deref().unwrap_or("-"),
        risk.risk_code,
        risk.reason,
        notify_time,
    )
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

fn build_liquidity_reward_manual_attention_markdown(
    attention: &LiquidityRewardManualAttentionNotification,
) -> String {
    let notify_time = chrono::Utc::now().to_rfc3339();
    format!(
        "### liquidity_reward 止损需人工处理\n\n\
        - 策略：{}\n\
        - Topic：{}\n\
        - Token：{}\n\
        - Trigger Local Order ID：{}\n\
        - Trigger Remote Order ID：{}\n\
        - 待止损数量：{}\n\
        - 可见仓位：{}\n\
        - 已尝试次数：{}\n\
        - 已等待秒数：{}\n\
        - 最后错误：{}\n\
        - 通知时间：{}",
        attention.strategy,
        attention.topic.as_deref().unwrap_or("-"),
        attention.token,
        attention.trigger_local_order_id,
        attention.trigger_remote_order_id.as_deref().unwrap_or("-"),
        attention.remaining_size,
        attention
            .visible_position_size
            .map(|size| size.to_string())
            .unwrap_or_else(|| "-".to_string()),
        attention.attempts,
        attention.waited_secs,
        attention.last_error,
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

    fn sample_manual_attention() -> LiquidityRewardManualAttentionNotification {
        LiquidityRewardManualAttentionNotification {
            strategy: "liquidity_reward".to_string(),
            topic: Some("liquidity_reward".to_string()),
            token: "token-1".to_string(),
            trigger_local_order_id: "buy-local-1".to_string(),
            trigger_remote_order_id: Some("buy-remote-1".to_string()),
            remaining_size: Decimal::try_from(2.5_f64).unwrap(),
            visible_position_size: Some(Decimal::try_from(2.0_f64).unwrap()),
            attempts: 5,
            last_error: "not enough balance / allowance".to_string(),
            waited_secs: 60,
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

    #[test]
    fn dingtalk_payload_contains_manual_attention_fields() {
        let payload = build_dingtalk_payload(&NotificationEvent::LiquidityRewardManualAttention(
            sample_manual_attention(),
        ));
        assert_eq!(payload["msgtype"], "markdown");
        assert_eq!(
            payload["markdown"]["title"].as_str().unwrap(),
            "liquidity_reward 止损需人工处理"
        );
        let text = payload["markdown"]["text"].as_str().unwrap();
        assert!(text.contains("liquidity_reward"));
        assert!(text.contains("token-1"));
        assert!(text.contains("buy-local-1"));
        assert!(text.contains("buy-remote-1"));
        assert!(text.contains("2.5"));
        assert!(text.contains("2"));
        assert!(text.contains("已尝试次数：5"));
        assert!(text.contains("not enough balance / allowance"));
    }

    #[test]
    fn dingtalk_payload_contains_order_submitted_fields() {
        let payload = build_dingtalk_payload(&NotificationEvent::OrderSubmitted(
            OrderSubmittedNotification {
                strategy_id: "market_maker".to_string(),
                local_order_id: "mm-local-1".to_string(),
                exchange_order_id: Some("0xorder".to_string()),
                market_id: "0xmarket".to_string(),
                token_id: "token-1".to_string(),
                side: "Buy".to_string(),
                order_type: "LimitGtc".to_string(),
                price: Some(Decimal::try_from(0.42_f64).unwrap()),
                size: Decimal::try_from(10.5_f64).unwrap(),
                event_kind: "Accepted".to_string(),
            },
        ));

        assert_eq!(payload["markdown"]["title"].as_str().unwrap(), "订单已提交");
        let text = payload["markdown"]["text"].as_str().unwrap();
        assert!(text.contains("market_maker"));
        assert!(text.contains("mm-local-1"));
        assert!(text.contains("0xorder"));
        assert!(text.contains("0xmarket"));
        assert!(text.contains("token-1"));
        assert!(text.contains("Buy"));
        assert!(text.contains("LimitGtc"));
        assert!(text.contains("0.42"));
        assert!(text.contains("10.5"));
        assert!(text.contains("Accepted"));
    }

    #[test]
    fn dingtalk_payload_contains_order_fill_fields() {
        let payload =
            build_dingtalk_payload(&NotificationEvent::OrderFilled(OrderFilledNotification {
                strategy_id: "market_maker".to_string(),
                local_order_id: "mm-local-1".to_string(),
                market_id: "0xmarket".to_string(),
                token_id: "token-1".to_string(),
                side: "Buy".to_string(),
                fill_kind: "partial_fill".to_string(),
                fill_qty: Decimal::try_from(1.25_f64).unwrap(),
                fill_price: Decimal::try_from(0.41_f64).unwrap(),
                cum_qty: Decimal::try_from(2.5_f64).unwrap(),
                avg_fill_price: Some(Decimal::try_from(0.415_f64).unwrap()),
            }));

        assert_eq!(payload["markdown"]["title"].as_str().unwrap(), "订单成交");
        let text = payload["markdown"]["text"].as_str().unwrap();
        assert!(text.contains("market_maker"));
        assert!(text.contains("mm-local-1"));
        assert!(text.contains("partial_fill"));
        assert!(text.contains("1.25"));
        assert!(text.contains("0.41"));
        assert!(text.contains("2.5"));
        assert!(text.contains("0.415"));
    }

    #[test]
    fn dingtalk_payload_contains_risk_event_fields() {
        let payload =
            build_dingtalk_payload(&NotificationEvent::RiskEvent(RiskEventNotification {
                source: "order_gateway".to_string(),
                strategy_id: Some("market_maker".to_string()),
                local_order_id: Some("mm-local-1".to_string()),
                market_id: Some("0xmarket".to_string()),
                token_id: Some("token-1".to_string()),
                risk_code: "daily_loss_limit".to_string(),
                reason: "daily loss limit triggered".to_string(),
            }));

        assert_eq!(payload["markdown"]["title"].as_str().unwrap(), "风控事件");
        let text = payload["markdown"]["text"].as_str().unwrap();
        assert!(text.contains("order_gateway"));
        assert!(text.contains("market_maker"));
        assert!(text.contains("mm-local-1"));
        assert!(text.contains("0xmarket"));
        assert!(text.contains("token-1"));
        assert!(text.contains("daily_loss_limit"));
        assert!(text.contains("daily loss limit triggered"));
    }
}
