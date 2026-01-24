// Timing utilities for measuring query execution

use std::time::Instant;

/// A simple timer for measuring execution duration
pub struct Timer {
    start: Instant,
    label: String,
}

impl Timer {
    /// Create and start a new timer with a label
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            start: Instant::now(),
            label: label.into(),
        }
    }

    /// Get elapsed time in milliseconds
    pub fn elapsed_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }

    /// Get elapsed time in microseconds
    pub fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }

    /// Get the label
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Stop and log the elapsed time at debug level
    pub fn stop_and_log(self) {
        crate::log_debug!("{} completed in {:.3}ms", self.label, self.elapsed_ms());
    }

    /// Stop and log with additional info
    pub fn stop_and_log_with_rows(self, rows: usize) {
        crate::log_debug!(
            "{} completed in {:.3}ms ({} rows)",
            self.label,
            self.elapsed_ms(),
            rows
        );
    }
}

/// Guard that logs timing when dropped
pub struct TimerGuard {
    timer: Option<Timer>,
}

impl TimerGuard {
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            timer: Some(Timer::new(label)),
        }
    }

    /// Get elapsed time without consuming
    pub fn elapsed_ms(&self) -> f64 {
        self.timer.as_ref().map(|t| t.elapsed_ms()).unwrap_or(0.0)
    }

    /// Manually stop and get elapsed time
    pub fn stop(&mut self) -> f64 {
        if let Some(timer) = self.timer.take() {
            timer.elapsed_ms()
        } else {
            0.0
        }
    }
}

impl Drop for TimerGuard {
    fn drop(&mut self) {
        if let Some(timer) = self.timer.take() {
            timer.stop_and_log();
        }
    }
}

/// Macro for timing a block of code
///
/// # Example
/// ```ignore
/// use tisql::time_block;
///
/// let result = time_block!("parse SQL", {
///     parser.parse(sql)
/// });
/// ```
#[macro_export]
macro_rules! time_block {
    ($label:expr, $block:expr) => {{
        let _timer = $crate::util::timing::Timer::new($label);
        let result = $block;
        _timer.stop_and_log();
        result
    }};
}

/// Macro for timing with custom logging
#[macro_export]
macro_rules! time_block_debug {
    ($label:expr, $block:expr) => {{
        let timer = $crate::util::timing::Timer::new($label);
        let result = $block;
        $crate::log_debug!("{}: {:.3}ms", $label, timer.elapsed_ms());
        result
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timer() {
        let timer = Timer::new("test");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = timer.elapsed_ms();
        assert!(elapsed >= 10.0);
        assert!(elapsed < 100.0); // Reasonable upper bound
    }

    #[test]
    fn test_timer_guard() {
        let guard = TimerGuard::new("test guard");
        std::thread::sleep(std::time::Duration::from_millis(5));
        let elapsed = guard.elapsed_ms();
        assert!(elapsed >= 5.0);
    }
}
