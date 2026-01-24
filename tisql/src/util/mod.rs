// Utility modules for TiSQL

pub mod log;
pub mod timing;

// Re-export commonly used items
pub use log::{init_logger, init_logger_from_env, is_level_enabled, LogLevel};
pub use timing::{Timer, TimerGuard};
