use std::path::Path;

use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;

const LOG_PATTERN: &str = "{d(%Y-%m-%d %H:%M:%S%.3f)} {h({l})} [{t}] {m}{n}";

pub fn init_logging(log_path: &str) -> anyhow::Result<()> {
    let config = build_log4rs_config(Path::new(log_path))?;
    log4rs::init_config(config)?;
    Ok(())
}

fn build_log4rs_config(log_path: &Path) -> anyhow::Result<Config> {
    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(LOG_PATTERN)))
        .build();
    let file = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(LOG_PATTERN)))
        .build(log_path)?;

    Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("file", Box::new(file)))
        .build(
            Root::builder()
                .appender("stdout")
                .appender("file")
                .build(log_level_from_env()),
        )
        .map_err(Into::into)
}

fn log_level_from_env() -> LevelFilter {
    let Ok(value) = std::env::var("RUST_LOG") else {
        return LevelFilter::Info;
    };
    match value.to_ascii_lowercase().as_str() {
        "off" => LevelFilter::Off,
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logging_config_writes_alerts_and_order_targets_to_same_file() {
        let log_path = std::env::temp_dir().join(format!(
            "poly-executor-log4rs-test-{}.log",
            std::process::id()
        ));

        let config = build_log4rs_config(&log_path).expect("build log4rs config");
        let root = config.root();
        assert_eq!(root.appenders(), ["stdout", "file"]);

        let appender_names = config
            .appenders()
            .iter()
            .map(|appender| appender.name())
            .collect::<Vec<_>>();
        assert_eq!(appender_names, ["stdout", "file"]);
    }
}
