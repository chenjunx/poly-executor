use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    EnvFilter, Layer as _, filter, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

pub fn init_logging(
    alerts_log_path: &str,
    order_log_path: &str,
) -> anyhow::Result<(WorkerGuard, WorkerGuard, WorkerGuard)> {
    let stdout_appender = tracing_appender::non_blocking(std::io::stdout());
    let alerts_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(alerts_log_path)
        .map_err(|e| anyhow::anyhow!("无法打开日志文件 {}: {}", alerts_log_path, e))?;
    let alerts_appender = tracing_appender::non_blocking(alerts_file);
    let order_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(order_log_path)
        .map_err(|e| anyhow::anyhow!("无法打开日志文件 {}: {}", order_log_path, e))?;
    let order_appender = tracing_appender::non_blocking(order_file);

    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_writer(stdout_appender.0)
        .with_target(false)
        .with_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_filter(filter::filter_fn(|metadata| {
            metadata.target() != "alerts" && metadata.target() != "order"
        }));

    let alerts_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(alerts_appender.0)
        .without_time()
        .with_level(false)
        .with_target(false)
        .with_filter(filter::filter_fn(|metadata| metadata.target() == "alerts"));

    let order_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(order_appender.0)
        .without_time()
        .with_level(false)
        .with_target(false)
        .with_filter(filter::filter_fn(|metadata| metadata.target() == "order"));

    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(alerts_layer)
        .with(order_layer)
        .try_init()?;

    Ok((stdout_appender.1, alerts_appender.1, order_appender.1))
}
