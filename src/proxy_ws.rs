/// 自定义带代理的 WebSocket 连接层。
///
/// 读取标准代理环境变量（ALL_PROXY / HTTPS_PROXY / HTTP_PROXY），
/// 支持 socks5:// 和 http:// 两种代理协议，对外暴露与 SDK 解析类型兼容的消息流。
use std::time::Duration;

use anyhow::{Context as _, bail};
use futures::{SinkExt as _, StreamExt as _};
use polymarket_client_sdk::clob::ws::types::response::WsMessage;
use serde::Serialize;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::time::{interval, timeout};
use tokio_tungstenite::tungstenite::Message;
use tracing::info;
use url::Url;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);

/// 代理类型。
#[derive(Clone)]
pub enum Proxy {
    Socks5 { host: String, port: u16 },
    Http { host: String, port: u16 },
}

impl Proxy {
    /// 从解析后的字符串读取代理配置。
    pub fn from_raw(val: &str) -> Option<Self> {
        if val.is_empty() {
            return None;
        }
        Self::parse(val)
    }

    /// 从标准环境变量中读取代理配置。
    /// 依次尝试：ALL_PROXY → HTTPS_PROXY → HTTP_PROXY（大小写均检查）。
    pub fn from_env() -> Option<Self> {
        let candidates = [
            "ALL_PROXY",
            "all_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "HTTP_PROXY",
            "http_proxy",
        ];

        for var in candidates {
            if let Ok(val) = std::env::var(var) {
                if val.is_empty() {
                    continue;
                }
                if let Some(proxy) = Self::parse(&val) {
                    info!(proxy_env = %var, proxy_value = %val, "使用代理配置");
                    return Some(proxy);
                }
            }
        }
        None
    }

    fn parse(raw: &str) -> Option<Self> {
        let url = Url::parse(raw).ok()?;
        let host = url.host_str()?.to_owned();
        match url.scheme() {
            "socks5" | "socks5h" => {
                let port = url.port().unwrap_or(1080);
                Some(Self::Socks5 { host, port })
            }
            "http" | "https" => {
                let port = url.port().unwrap_or(8080);
                Some(Self::Http { host, port })
            }
            _ => None,
        }
    }

    /// 建立经由代理的原始 TCP 连接，返回到目标主机的 TCP 流。
    async fn tunnel(&self, target_host: &str, target_port: u16) -> anyhow::Result<TcpStream> {
        let target = format!("{target_host}:{target_port}");

        match self {
            Self::Socks5 { host, port } => {
                let proxy_addr = format!("{host}:{port}");
                let stream =
                    tokio_socks::tcp::Socks5Stream::connect(proxy_addr.as_str(), target.as_str())
                        .await
                        .with_context(|| format!("SOCKS5 连接失败: {proxy_addr} → {target}"))?
                        .into_inner();
                Ok(stream)
            }
            Self::Http { host, port } => {
                let proxy_addr = format!("{host}:{port}");
                let mut stream = TcpStream::connect(&proxy_addr)
                    .await
                    .with_context(|| format!("连接 HTTP 代理失败: {proxy_addr}"))?;

                // HTTP CONNECT 握手
                let req = format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n");
                stream.write_all(req.as_bytes()).await?;

                // 读取代理响应（最多 1024 字节）
                let mut buf = vec![0u8; 1024];
                let n = stream.read(&mut buf).await?;
                let resp = String::from_utf8_lossy(&buf[..n]);

                if !resp.contains("200") {
                    bail!("HTTP CONNECT 失败: {}", resp.trim());
                }
                Ok(stream)
            }
        }
    }
}

/// 订阅请求（与 SDK 协议兼容）。
#[derive(Serialize)]
struct SubRequest<'a> {
    r#type: &'a str,
    operation: &'a str,
    markets: Vec<String>,
    assets_ids: Vec<String>,
    initial_dump: bool,
}

/// 通过代理建立 WebSocket 连接，然后循环接收价格消息并传回 channel。
///
/// 调用者通过 `tx` 接收解析后的 [`WsMessage`]。
pub async fn run(
    proxy: Proxy,
    asset_ids: Vec<String>,
    tx: tokio::sync::mpsc::Sender<WsMessage>,
) -> anyhow::Result<()> {
    let url = Url::parse(WS_URL)?;
    let host = url.host_str().context("URL 缺少 host")?;
    let port = url.port().unwrap_or(443);

    info!(host = %host, port, "通过代理建立隧道");
    let tcp = proxy.tunnel(host, port).await?;

    // TLS 握手
    let cx = native_tls::TlsConnector::new()?;
    let cx = tokio_native_tls::TlsConnector::from(cx);
    let tls_stream = cx
        .connect(host, tcp)
        .await
        .context("TLS 握手失败")?;

    // WebSocket 握手
    let (mut ws, _) = tokio_tungstenite::client_async(WS_URL, tls_stream)
        .await
        .context("WebSocket 握手失败")?;

    info!("WebSocket 已连接（通过代理）");

    // 发送订阅请求
    let req = SubRequest {
        r#type: "market",
        operation: "subscribe",
        markets: vec![],
        assets_ids: asset_ids,
        initial_dump: true,
    };
    ws.send(Message::Text(simd_json::to_string(&req)?.into()))
        .await?;

    // 心跳 + 消息循环
    let mut ping_timer = interval(HEARTBEAT_INTERVAL);
    ping_timer.tick().await; // 跳过立即触发的第一次

    loop {
        tokio::select! {
            _ = ping_timer.tick() => {
                ws.send(Message::Text("PING".into())).await?;
            }

            result = timeout(HEARTBEAT_TIMEOUT, ws.next()) => {
                match result {
                    Err(_) => bail!("心跳超时：服务器无响应"),
                    Ok(None) => bail!("WebSocket 连接已关闭"),
                    Ok(Some(Err(e))) => bail!("WebSocket 错误: {e}"),
                    Ok(Some(Ok(Message::Text(text)))) => {
                        if text == "PONG" {
                            continue;
                        }
                        // simd-json 需要 &mut [u8]（原地解析），先转为字节缓冲
                        let mut buf = text.as_bytes().to_vec();
                        let msgs: Vec<WsMessage> = if buf.first() == Some(&b'[') {
                            simd_json::from_slice(&mut buf).unwrap_or_default()
                        } else {
                            simd_json::from_slice::<WsMessage>(&mut buf)
                                .ok()
                                .into_iter()
                                .collect()
                        };
                        for msg in msgs {
                            let _ = tx.send(msg).await;
                        }
                    }
                    Ok(Some(Ok(Message::Close(_)))) => bail!("服务器主动关闭连接"),
                    Ok(Some(Ok(_))) => {} // 忽略 binary/ping/pong 帧
                }
            }
        }
    }
}
