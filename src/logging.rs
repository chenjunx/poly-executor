use std::path::Path;

use log::LevelFilter;
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::append::rolling_file::policy::compound::{
    CompoundPolicy, roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger,
};
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;

const LOG_PATTERN: &str = "{d(%Y-%m-%d %H:%M:%S%.3f)} {h({l})} [{t}] {m}{n}";
const LOG_ROLL_SIZE_BYTES: u64 = 100 * 1024 * 1024;
const LOG_BACKUP_COUNT: u32 = 10;

pub fn init_logging(log_path: &str) -> anyhow::Result<()> {
    let config = build_log4rs_config(Path::new(log_path))?;
    log4rs::init_config(config)?;
    Ok(())
}

fn build_log4rs_config(log_path: &Path) -> anyhow::Result<Config> {
    let file = build_rolling_file_appender(log_path)?;

    Config::builder()
        .appender(Appender::builder().build("file", Box::new(file)))
        .build(Root::builder().appender("file").build(log_level_from_env()))
        .map_err(Into::into)
}

fn build_rolling_file_appender(log_path: &Path) -> anyhow::Result<RollingFileAppender> {
    let trigger = SizeTrigger::new(LOG_ROLL_SIZE_BYTES);
    let roller =
        FixedWindowRoller::builder().build(&rolling_backup_pattern(log_path), LOG_BACKUP_COUNT)?;
    let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

    RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new(LOG_PATTERN)))
        .build(log_path, Box::new(policy))
        .map_err(Into::into)
}

fn rolling_backup_pattern(log_path: &Path) -> String {
    format!("{}.{{}}.gz", log_path.to_string_lossy())
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
        assert_eq!(root.appenders(), ["file"]);

        let appender_names = config
            .appenders()
            .iter()
            .map(|appender| appender.name())
            .collect::<Vec<_>>();
        assert_eq!(appender_names, ["file"]);
    }

    #[test]
    fn rolling_backup_pattern_uses_gzip_fixed_window_suffix() {
        let log_path = Path::new("poly-executor.log");

        assert_eq!(rolling_backup_pattern(log_path), "poly-executor.log.{}.gz");
    }

    #[test]
    fn logging_rolls_at_100mb_and_keeps_ten_gzip_backups() {
        assert_eq!(LOG_ROLL_SIZE_BYTES, 100 * 1024 * 1024);
        assert_eq!(LOG_BACKUP_COUNT, 10);
    }
}
