use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    EnvFilter, Layer as _, filter, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

pub fn init_logging(log_path: &str) -> anyhow::Result<(WorkerGuard, WorkerGuard)> {
    let stdout_appender = tracing_appender::non_blocking(std::io::stdout());
    let alerts_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .map_err(|e| anyhow::anyhow!("无法打开日志文件 {}: {}", log_path, e))?;
    let alerts_appender = tracing_appender::non_blocking(alerts_file);

    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_writer(stdout_appender.0)
        .with_target(false)
        .with_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_filter(filter::filter_fn(|metadata| metadata.target() != "alerts"));

    let alerts_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(alerts_appender.0)
        .without_time()
        .with_level(false)
        .with_target(false)
        .with_filter(filter::filter_fn(|metadata| metadata.target() == "alerts"));

    tracing_subscriber::registry()
        .with(stdout_layer)
        .with(alerts_layer)
        .try_init()?;

    Ok((stdout_appender.1, alerts_appender.1))
}
