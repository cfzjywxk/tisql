// Async logging utilities for TiSQL
//
// This module provides non-blocking logging that won't impact the SQL execution hot path.
// Log messages are sent through a channel to a background task that handles actual I/O.
//
// Usage:
//   use tisql::util::log::{init_logger, LogLevel};
//   use tisql::{log_info, log_debug, log_warn, log_error};
//
//   init_logger(LogLevel::Debug);
//   log_info!("Server started on port {}", 4000);
//   log_debug!("Executing query: {}", sql);

use std::fmt;
use std::io::Write;
use std::sync::OnceLock;

use tokio::sync::mpsc;

/// Log levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO ",
            LogLevel::Warn => "WARN ",
            LogLevel::Error => "ERROR",
        };
        write!(f, "{}", s)
    }
}

impl LogLevel {
    /// Parse log level from string (case insensitive)
    pub fn from_str(s: &str) -> Option<LogLevel> {
        match s.to_lowercase().as_str() {
            "trace" => Some(LogLevel::Trace),
            "debug" => Some(LogLevel::Debug),
            "info" => Some(LogLevel::Info),
            "warn" | "warning" => Some(LogLevel::Warn),
            "error" => Some(LogLevel::Error),
            _ => None,
        }
    }
}

/// A log record to be sent to the background writer
struct LogRecord {
    level: LogLevel,
    timestamp: String,
    file: &'static str,
    line: u32,
    message: String,
}

/// Global logger state
struct Logger {
    sender: mpsc::UnboundedSender<LogRecord>,
    min_level: LogLevel,
}

static LOGGER: OnceLock<Logger> = OnceLock::new();

/// Initialize the async logger with the specified minimum log level.
///
/// This spawns a background task that handles log writing.
/// Should be called once at application startup.
///
/// # Example
/// ```ignore
/// use tisql::util::log::{init_logger, LogLevel};
///
/// #[tokio::main]
/// async fn main() {
///     init_logger(LogLevel::Info);
///     // ... rest of application
/// }
/// ```
pub fn init_logger(min_level: LogLevel) {
    let (sender, receiver) = mpsc::unbounded_channel();

    // Spawn background writer task
    tokio::spawn(async move {
        log_writer_task(receiver).await;
    });

    let _ = LOGGER.set(Logger { sender, min_level });
}

/// Initialize logger with level from TISQL_LOG environment variable
/// Falls back to the provided default level if env var is not set
pub fn init_logger_from_env(default_level: LogLevel) {
    let level = std::env::var("TISQL_LOG")
        .ok()
        .and_then(|s| LogLevel::from_str(&s))
        .unwrap_or(default_level);
    init_logger(level);
}

/// Background task that receives log records and writes them
async fn log_writer_task(mut receiver: mpsc::UnboundedReceiver<LogRecord>) {
    let mut stderr = std::io::stderr();

    while let Some(record) = receiver.recv().await {
        let formatted = format!(
            "[{}] [{}] [{}:{}] {}\n",
            record.timestamp,
            record.level,
            record.file,
            record.line,
            record.message
        );

        // Write to stderr (non-blocking from caller's perspective)
        let _ = stderr.write_all(formatted.as_bytes());
        let _ = stderr.flush();
    }
}

/// Get current timestamp in a readable format
fn get_timestamp() -> String {
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();

    let secs = now.as_secs();
    let millis = now.subsec_millis();

    // Convert to readable format (simplified UTC)
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Simple year calculation (approximate, good enough for logging)
    let years = 1970 + (days_since_epoch / 365);
    let day_of_year = days_since_epoch % 365;
    let month = (day_of_year / 30) + 1;
    let day = (day_of_year % 30) + 1;

    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
        years, month.min(12), day.min(31), hours, minutes, seconds, millis
    )
}

/// Internal function to send a log record
#[doc(hidden)]
pub fn __log_impl(level: LogLevel, file: &'static str, line: u32, message: String) {
    if let Some(logger) = LOGGER.get() {
        // Check if this level should be logged
        if level < logger.min_level {
            return;
        }

        let record = LogRecord {
            level,
            timestamp: get_timestamp(),
            file,
            line,
            message,
        };

        // Send to background task (non-blocking)
        let _ = logger.sender.send(record);
    } else {
        // Fallback: if logger not initialized, print directly
        eprintln!(
            "[{}] [{}] [{}:{}] {}",
            get_timestamp(),
            level,
            file,
            line,
            message
        );
    }
}

/// Check if a log level is enabled (useful for avoiding expensive formatting)
pub fn is_level_enabled(level: LogLevel) -> bool {
    LOGGER
        .get()
        .map(|l| level >= l.min_level)
        .unwrap_or(true)
}

// ============================================================================
// Logging Macros
// ============================================================================

/// Log a trace-level message
///
/// # Example
/// ```ignore
/// log_trace!("Detailed trace info: {:?}", some_value);
/// ```
#[macro_export]
macro_rules! log_trace {
    ($($arg:tt)*) => {
        $crate::util::log::__log_impl(
            $crate::util::log::LogLevel::Trace,
            file!(),
            line!(),
            format!($($arg)*)
        )
    };
}

/// Log a debug-level message
///
/// # Example
/// ```ignore
/// log_debug!("Processing query: {}", sql);
/// ```
#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        $crate::util::log::__log_impl(
            $crate::util::log::LogLevel::Debug,
            file!(),
            line!(),
            format!($($arg)*)
        )
    };
}

/// Log an info-level message
///
/// # Example
/// ```ignore
/// log_info!("Server started on port {}", port);
/// ```
#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::util::log::__log_impl(
            $crate::util::log::LogLevel::Info,
            file!(),
            line!(),
            format!($($arg)*)
        )
    };
}

/// Log a warning-level message
///
/// # Example
/// ```ignore
/// log_warn!("Query took {}ms, consider optimization", elapsed);
/// ```
#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => {
        $crate::util::log::__log_impl(
            $crate::util::log::LogLevel::Warn,
            file!(),
            line!(),
            format!($($arg)*)
        )
    };
}

/// Log an error-level message
///
/// # Example
/// ```ignore
/// log_error!("Failed to execute query: {}", err);
/// ```
#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::util::log::__log_impl(
            $crate::util::log::LogLevel::Error,
            file!(),
            line!(),
            format!($($arg)*)
        )
    };
}

// ============================================================================
// SQL Execution Logging Helpers
// ============================================================================

/// Log SQL query execution start
#[macro_export]
macro_rules! log_query_start {
    ($query:expr) => {
        $crate::log_debug!("SQL START: {}", $query)
    };
}

/// Log SQL query execution end with duration
#[macro_export]
macro_rules! log_query_end {
    ($query:expr, $duration_ms:expr) => {
        $crate::log_debug!("SQL END: {} ({}ms)", $query, $duration_ms)
    };
    ($query:expr, $duration_ms:expr, $rows:expr) => {
        $crate::log_debug!("SQL END: {} ({}ms, {} rows)", $query, $duration_ms, $rows)
    };
}

/// Log SQL execution plan
#[macro_export]
macro_rules! log_plan {
    ($plan:expr) => {
        if $crate::util::log::is_level_enabled($crate::util::log::LogLevel::Trace) {
            $crate::log_trace!("PLAN: {:?}", $plan)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
    }

    #[test]
    fn test_log_level_from_str() {
        assert_eq!(LogLevel::from_str("debug"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::from_str("DEBUG"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::from_str("Info"), Some(LogLevel::Info));
        assert_eq!(LogLevel::from_str("invalid"), None);
    }

    #[test]
    fn test_timestamp_format() {
        let ts = get_timestamp();
        // Should be in format: YYYY-MM-DD HH:MM:SS.mmm
        assert!(ts.len() > 20);
        assert!(ts.contains('-'));
        assert!(ts.contains(':'));
        assert!(ts.contains('.'));
    }
}
