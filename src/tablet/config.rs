// Copyright 2024 TiSQL Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! LSM-tree configuration.
//!
//! This module provides configuration for the LSM storage engine,
//! including memtable sizing, compaction thresholds, and file format options.
//!
//! ## Default Values
//!
//! The defaults are chosen for a balance of write throughput and space amplification:
//!
//! | Parameter | Default | Description |
//! |-----------|---------|-------------|
//! | memtable_size | 64 MB | Size threshold for memtable flush |
//! | max_frozen_memtables | 16 | Max frozen memtables before stalling |
//! | block_size | 4 KB | Target data block size |
//! | l0_compaction_trigger | 8 | L0 file count to trigger compaction |
//! | level_size_multiplier | 10 | Size ratio between adjacent levels |
//! | max_levels | 3 | Number of levels (L0 + L1 + L2 by default) |
//!
//! ## Example
//!
//! ```ignore
//! let config = LsmConfig::builder()
//!     .memtable_size(128 * 1024 * 1024)  // 128 MB
//!     .l0_compaction_trigger(8)
//!     .build();
//! ```

use std::path::PathBuf;
use std::time::Duration;

use crate::tablet::sstable::CompressionType;

/// Default memtable size (64 MB).
pub const DEFAULT_MEMTABLE_SIZE: usize = 64 * 1024 * 1024;

/// Default maximum frozen memtables.
pub const DEFAULT_MAX_FROZEN_MEMTABLES: usize = 16;

/// Enter frozen slowdown when frozen count reaches this percentage of max.
pub const DEFAULT_FROZEN_SLOWDOWN_ENTER_PERCENT: u8 = 75;

/// Exit frozen slowdown when frozen count falls to this percentage of max.
pub const DEFAULT_FROZEN_SLOWDOWN_EXIT_PERCENT: u8 = 60;

/// Frozen slowdown minimum write delay in milliseconds.
pub const DEFAULT_FROZEN_SLOWDOWN_MIN_DELAY_MS: u64 = 1;

/// Frozen slowdown maximum write delay in milliseconds.
pub const DEFAULT_FROZEN_SLOWDOWN_MAX_DELAY_MS: u64 = 20;

/// Async writer wait timeout for hard frozen-cap stalls in milliseconds.
pub const DEFAULT_FROZEN_STALL_WAIT_TIMEOUT_MS: u64 = 100;

/// Optional memory-pressure soft threshold in bytes (0 disables byte model).
pub const DEFAULT_MEM_PRESSURE_SOFT_LIMIT_BYTES: usize = 0;

/// Optional memory-pressure hard threshold in bytes (0 disables byte model).
pub const DEFAULT_MEM_PRESSURE_HARD_LIMIT_BYTES: usize = 0;

/// Memory-pressure slowdown minimum delay in milliseconds.
pub const DEFAULT_MEM_PRESSURE_MIN_DELAY_MS: u64 = 1;

/// Memory-pressure slowdown maximum delay in milliseconds.
pub const DEFAULT_MEM_PRESSURE_MAX_DELAY_MS: u64 = 20;

/// L0 guard band (files) for limiting concurrent flush dispatch near stop.
pub const DEFAULT_L0_FLUSH_DISPATCH_GUARD_BAND: usize = 2;

/// Default data block size (4 KB).
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;

/// Default L0 compaction trigger (file count).
pub const DEFAULT_L0_COMPACTION_TRIGGER: usize = 8;

/// Default L0 slowdown trigger (file count).
pub const DEFAULT_L0_SLOWDOWN_TRIGGER: usize = DEFAULT_L0_COMPACTION_TRIGGER * 8;

/// Default L0 stop trigger (file count).
pub const DEFAULT_L0_STOP_TRIGGER: usize = DEFAULT_L0_COMPACTION_TRIGGER * 12;

/// Default level size multiplier (10x between levels).
pub const DEFAULT_LEVEL_SIZE_MULTIPLIER: usize = 10;

/// Default L1 total size (256 MB).
pub const DEFAULT_L1_MAX_SIZE: usize = 256 * 1024 * 1024;

/// Default target file size (64 MB).
pub const DEFAULT_TARGET_FILE_SIZE: usize = 64 * 1024 * 1024;

/// Default number of LSM levels (L0 + L1 + L2).
///
/// TiSQL shards data by tablet, so we keep a small number of levels to control
/// compaction rewrite cost without adopting RocksDB-like deep level stacks.
pub const DEFAULT_MAX_LEVELS: usize = 3;

/// Bloom filter is enabled by default.
pub const DEFAULT_BLOOM_ENABLED: bool = true;

/// Default bloom filter bits per key.
pub const DEFAULT_BLOOM_BITS_PER_KEY: u32 = 12;

/// Shared block cache is enabled by default.
pub const DEFAULT_SHARED_BLOCK_CACHE_ENABLED: bool = true;

/// Reader cache is enabled by default.
///
/// This keeps SST point-lookups from repeatedly reopening readers on hot paths
/// such as INSERT duplicate checks.
pub const DEFAULT_READER_CACHE_ENABLED: bool = true;

/// Row cache is disabled by default (rollout gate).
pub const DEFAULT_ROW_CACHE_ENABLED: bool = false;

/// Foreground scan cache-fill policy gate (off by default in v1 rollout).
pub const DEFAULT_SCAN_FILL_CACHE: bool = false;

/// Foreground scan threshold (in estimated blocks) for cache fill.
pub const DEFAULT_SCAN_FILL_CACHE_THRESHOLD_BLOCKS: usize = 8;

/// Total cache memory ratio (fraction of machine RAM).
pub const DEFAULT_CACHE_TOTAL_RATIO: f64 = 0.50;

/// Reader-cache entry cap.
pub const DEFAULT_READER_CACHE_MAX_ENTRIES: usize = 100_000;

/// Rollout mode for V2.6 reservation/in-flight boundaries.
///
/// Note: Phase 5 normalizes runtime behavior to `On` inside `LsmEngine`.
/// `Off`/`Shadow` are retained to preserve rollout/rollback API shape and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum V26BoundaryMode {
    /// Keep V2.5 behavior (gated boundary only).
    Off = 0,
    /// Compute both paths, old gated path remains authoritative.
    Shadow = 1,
    /// Reservation/in-flight caps become authoritative.
    On = 2,
}

impl V26BoundaryMode {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    #[inline]
    pub const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Off,
            1 => Self::Shadow,
            2 => Self::On,
            _ => Self::Off,
        }
    }
}

/// LSM storage engine configuration.
#[derive(Debug, Clone)]
pub struct LsmConfig {
    /// Data directory for SST files.
    pub data_dir: PathBuf,

    // ==================== MemTable Configuration ====================
    /// Size threshold for memtable flush (bytes).
    ///
    /// When the active memtable exceeds this size, it is frozen
    /// and a new active memtable is created.
    pub memtable_size: usize,

    /// Maximum number of frozen memtables.
    ///
    /// If this limit is reached and flush cannot keep up,
    /// writes will stall until a memtable is flushed.
    pub max_frozen_memtables: usize,

    /// Frozen-count ratio (percentage) to enter slowdown before hard stall.
    pub frozen_slowdown_enter_percent: u8,

    /// Frozen-count ratio (percentage) to exit slowdown (hysteresis).
    pub frozen_slowdown_exit_percent: u8,

    /// Frozen slowdown minimum delay in milliseconds.
    pub frozen_slowdown_min_delay_ms: u64,

    /// Frozen slowdown maximum delay in milliseconds.
    pub frozen_slowdown_max_delay_ms: u64,

    /// Async writer wait timeout (ms) for hard frozen-cap stalls.
    pub frozen_stall_wait_timeout_ms: u64,

    /// Byte-based memory-pressure soft threshold (0 disables byte model).
    pub mem_pressure_soft_limit_bytes: usize,

    /// Byte-based memory-pressure hard threshold (0 disables byte model).
    pub mem_pressure_hard_limit_bytes: usize,

    /// Byte-based slowdown minimum delay in milliseconds.
    pub mem_pressure_min_delay_ms: u64,

    /// Byte-based slowdown maximum delay in milliseconds.
    pub mem_pressure_max_delay_ms: u64,

    /// Minimum memtable age before flush (optional).
    ///
    /// If set, memtables younger than this duration won't be flushed
    /// even if they reach the size threshold. This can help batch
    /// small writes together.
    pub min_memtable_age: Option<Duration>,

    // ==================== SST Configuration ====================
    /// Target data block size (bytes).
    ///
    /// Data blocks are the unit of I/O for SST files.
    /// Larger blocks improve sequential read throughput but
    /// increase read amplification for point lookups.
    pub block_size: usize,

    /// Compression type for data blocks.
    pub compression: CompressionType,

    /// Whether bloom filter support is enabled.
    ///
    /// Phase-B0 gate: this is a pure configuration toggle. Later phases wire
    /// writer/reader behavior behind this flag.
    pub bloom_enabled: bool,

    /// Bloom filter density (bits per inserted key).
    pub bloom_bits_per_key: u32,

    /// Shared block cache gate.
    pub shared_block_cache_enabled: bool,

    /// Reader cache gate.
    pub reader_cache_enabled: bool,

    /// Row cache gate.
    pub row_cache_enabled: bool,

    /// Whether scan paths may fill cache.
    pub scan_fill_cache: bool,

    /// Foreground scan cache-fill threshold.
    pub scan_fill_cache_threshold_blocks: usize,

    /// Fraction of machine memory to reserve for total cache budget.
    pub cache_total_ratio: f64,

    /// Reader cache max entry count.
    pub reader_cache_max_entries: usize,

    /// Target SST file size (bytes).
    ///
    /// SST files will be split to stay near this size.
    pub target_file_size: usize,

    // ==================== Compaction Configuration ====================
    /// L0 file count to trigger compaction.
    ///
    /// When L0 reaches this many files, compaction is triggered
    /// to merge them into L1.
    pub l0_compaction_trigger: usize,

    /// L0 file count to slow down writes.
    ///
    /// When L0 reaches this count, writes are artificially slowed
    /// to let compaction catch up.
    pub l0_slowdown_trigger: usize,

    /// L0 file count to stop writes.
    ///
    /// When L0 reaches this count, writes are blocked entirely
    /// until compaction reduces the count.
    pub l0_stop_trigger: usize,

    /// Near-stop L0 guard band for limiting concurrent flush dispatch.
    pub l0_flush_dispatch_guard_band: usize,

    /// Level size multiplier.
    ///
    /// Each level can hold `multiplier` times more data than
    /// the previous level. Default is 10x.
    pub level_size_multiplier: usize,

    /// L1 maximum size (bytes).
    ///
    /// This is the base for calculating other level sizes.
    /// L2 = L1 * multiplier, L3 = L2 * multiplier, etc.
    pub l1_max_size: usize,

    /// Maximum number of levels.
    pub max_levels: usize,

    // ==================== Background Thread Configuration ====================
    /// Number of compaction threads.
    pub compaction_threads: usize,

    /// Number of flush threads.
    pub flush_threads: usize,

    // ==================== Recovery Configuration ====================
    /// Whether to verify checksums on read.
    pub verify_checksums: bool,

    /// Whether to sync WAL on every write (for durability).
    ///
    /// If false, we rely on OS buffering which improves throughput
    /// but risks losing recent writes on crash.
    pub sync_writes: bool,

    // ==================== V2.6 Rollout Configuration ====================
    /// Initial rollout mode for reservation/in-flight boundaries.
    ///
    /// Runtime mode may change via `LsmEngine::set_v26_mode()`.
    pub v26_boundary_mode: V26BoundaryMode,
}

impl LsmConfig {
    /// Create a new configuration with defaults and the given data directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            memtable_size: DEFAULT_MEMTABLE_SIZE,
            max_frozen_memtables: DEFAULT_MAX_FROZEN_MEMTABLES,
            frozen_slowdown_enter_percent: DEFAULT_FROZEN_SLOWDOWN_ENTER_PERCENT,
            frozen_slowdown_exit_percent: DEFAULT_FROZEN_SLOWDOWN_EXIT_PERCENT,
            frozen_slowdown_min_delay_ms: DEFAULT_FROZEN_SLOWDOWN_MIN_DELAY_MS,
            frozen_slowdown_max_delay_ms: DEFAULT_FROZEN_SLOWDOWN_MAX_DELAY_MS,
            frozen_stall_wait_timeout_ms: DEFAULT_FROZEN_STALL_WAIT_TIMEOUT_MS,
            mem_pressure_soft_limit_bytes: DEFAULT_MEM_PRESSURE_SOFT_LIMIT_BYTES,
            mem_pressure_hard_limit_bytes: DEFAULT_MEM_PRESSURE_HARD_LIMIT_BYTES,
            mem_pressure_min_delay_ms: DEFAULT_MEM_PRESSURE_MIN_DELAY_MS,
            mem_pressure_max_delay_ms: DEFAULT_MEM_PRESSURE_MAX_DELAY_MS,
            min_memtable_age: None,
            block_size: DEFAULT_BLOCK_SIZE,
            compression: CompressionType::None,
            bloom_enabled: DEFAULT_BLOOM_ENABLED,
            bloom_bits_per_key: DEFAULT_BLOOM_BITS_PER_KEY,
            shared_block_cache_enabled: DEFAULT_SHARED_BLOCK_CACHE_ENABLED,
            reader_cache_enabled: DEFAULT_READER_CACHE_ENABLED,
            row_cache_enabled: DEFAULT_ROW_CACHE_ENABLED,
            scan_fill_cache: DEFAULT_SCAN_FILL_CACHE,
            scan_fill_cache_threshold_blocks: DEFAULT_SCAN_FILL_CACHE_THRESHOLD_BLOCKS,
            cache_total_ratio: DEFAULT_CACHE_TOTAL_RATIO,
            reader_cache_max_entries: DEFAULT_READER_CACHE_MAX_ENTRIES,
            target_file_size: DEFAULT_TARGET_FILE_SIZE,
            l0_compaction_trigger: DEFAULT_L0_COMPACTION_TRIGGER,
            l0_slowdown_trigger: DEFAULT_L0_SLOWDOWN_TRIGGER,
            l0_stop_trigger: DEFAULT_L0_STOP_TRIGGER,
            l0_flush_dispatch_guard_band: DEFAULT_L0_FLUSH_DISPATCH_GUARD_BAND,
            level_size_multiplier: DEFAULT_LEVEL_SIZE_MULTIPLIER,
            l1_max_size: DEFAULT_L1_MAX_SIZE,
            max_levels: DEFAULT_MAX_LEVELS,
            compaction_threads: 2,
            flush_threads: 1,
            verify_checksums: true,
            sync_writes: true,
            // Phase 5: reservation/in-flight boundary is authoritative.
            v26_boundary_mode: V26BoundaryMode::On,
        }
    }

    /// Create a builder for configuration.
    pub fn builder(data_dir: impl Into<PathBuf>) -> LsmConfigBuilder {
        LsmConfigBuilder::new(data_dir)
    }

    /// Get the SST directory path.
    pub fn sst_dir(&self) -> PathBuf {
        self.data_dir.join("sst")
    }

    /// Get the maximum size for a level.
    ///
    /// L0 is special (file count based), so this returns 0 for L0.
    /// For L1+, returns L1_max_size * multiplier^(level-1).
    pub fn level_max_size(&self, level: usize) -> usize {
        if level == 0 {
            0 // L0 uses file count, not size
        } else {
            let multiplier = self.level_size_multiplier.pow((level - 1) as u32);
            self.l1_max_size * multiplier
        }
    }

    /// Check if L0 compaction should be triggered.
    pub fn should_trigger_l0_compaction(&self, l0_file_count: usize) -> bool {
        l0_file_count >= self.l0_compaction_trigger
    }

    /// Check if writes should be slowed down.
    pub fn should_slowdown_writes(&self, l0_file_count: usize) -> bool {
        l0_file_count >= self.l0_slowdown_trigger
    }

    /// Check if writes should be stopped.
    pub fn should_stop_writes(&self, l0_file_count: usize) -> bool {
        l0_file_count >= self.l0_stop_trigger
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.memtable_size == 0 {
            return Err("memtable_size must be > 0".to_string());
        }
        if self.max_frozen_memtables == 0 {
            return Err("max_frozen_memtables must be > 0".to_string());
        }
        if !(1..=100).contains(&self.frozen_slowdown_enter_percent) {
            return Err("frozen_slowdown_enter_percent must be in [1, 100]".to_string());
        }
        if !(1..=100).contains(&self.frozen_slowdown_exit_percent) {
            return Err("frozen_slowdown_exit_percent must be in [1, 100]".to_string());
        }
        if self.frozen_slowdown_exit_percent >= self.frozen_slowdown_enter_percent {
            return Err(
                "frozen_slowdown_exit_percent must be < frozen_slowdown_enter_percent".to_string(),
            );
        }
        if self.frozen_slowdown_min_delay_ms == 0 {
            return Err("frozen_slowdown_min_delay_ms must be > 0".to_string());
        }
        if self.frozen_slowdown_max_delay_ms < self.frozen_slowdown_min_delay_ms {
            return Err(
                "frozen_slowdown_max_delay_ms must be >= frozen_slowdown_min_delay_ms".to_string(),
            );
        }
        if self.frozen_stall_wait_timeout_ms == 0 {
            return Err("frozen_stall_wait_timeout_ms must be > 0".to_string());
        }
        let byte_model_disabled =
            self.mem_pressure_soft_limit_bytes == 0 && self.mem_pressure_hard_limit_bytes == 0;
        if !byte_model_disabled {
            if self.mem_pressure_soft_limit_bytes == 0 {
                return Err(
                    "mem_pressure_soft_limit_bytes must be > 0 when byte model is enabled"
                        .to_string(),
                );
            }
            if self.mem_pressure_hard_limit_bytes < self.mem_pressure_soft_limit_bytes {
                return Err(
                    "mem_pressure_hard_limit_bytes must be >= mem_pressure_soft_limit_bytes"
                        .to_string(),
                );
            }
            if self.mem_pressure_min_delay_ms == 0 {
                return Err("mem_pressure_min_delay_ms must be > 0".to_string());
            }
            if self.mem_pressure_max_delay_ms < self.mem_pressure_min_delay_ms {
                return Err(
                    "mem_pressure_max_delay_ms must be >= mem_pressure_min_delay_ms".to_string(),
                );
            }
        }
        if self.block_size == 0 {
            return Err("block_size must be > 0".to_string());
        }
        if self.l0_compaction_trigger == 0 {
            return Err("l0_compaction_trigger must be > 0".to_string());
        }
        if self.l0_slowdown_trigger < self.l0_compaction_trigger {
            return Err("l0_slowdown_trigger must be >= l0_compaction_trigger".to_string());
        }
        if self.l0_stop_trigger < self.l0_slowdown_trigger {
            return Err("l0_stop_trigger must be >= l0_slowdown_trigger".to_string());
        }
        if self.l0_flush_dispatch_guard_band == 0 {
            return Err("l0_flush_dispatch_guard_band must be > 0".to_string());
        }
        if self.level_size_multiplier < 2 {
            return Err("level_size_multiplier must be >= 2".to_string());
        }
        if self.max_levels < 2 {
            return Err("max_levels must be >= 2 (L0 + L1)".to_string());
        }
        if self.bloom_bits_per_key == 0 {
            return Err("bloom_bits_per_key must be > 0".to_string());
        }
        if !(0.0..=0.95).contains(&self.cache_total_ratio) {
            return Err("cache_total_ratio must be in [0.0, 0.95]".to_string());
        }
        if self.scan_fill_cache_threshold_blocks == 0 {
            return Err("scan_fill_cache_threshold_blocks must be > 0".to_string());
        }
        if self.reader_cache_max_entries == 0 && self.reader_cache_enabled {
            return Err(
                "reader_cache_max_entries must be > 0 when reader cache is enabled".to_string(),
            );
        }
        Ok(())
    }
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self::new("./data")
    }
}

/// Builder for LsmConfig.
pub struct LsmConfigBuilder {
    config: LsmConfig,
}

impl LsmConfigBuilder {
    /// Create a new builder with the given data directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            config: LsmConfig::new(data_dir),
        }
    }

    /// Set the memtable size threshold.
    pub fn memtable_size(mut self, size: usize) -> Self {
        self.config.memtable_size = size;
        self
    }

    /// Set the maximum frozen memtables.
    pub fn max_frozen_memtables(mut self, count: usize) -> Self {
        self.config.max_frozen_memtables = count;
        self
    }

    /// Set frozen slowdown enter threshold percentage.
    pub fn frozen_slowdown_enter_percent(mut self, percent: u8) -> Self {
        self.config.frozen_slowdown_enter_percent = percent;
        self
    }

    /// Set frozen slowdown exit threshold percentage.
    pub fn frozen_slowdown_exit_percent(mut self, percent: u8) -> Self {
        self.config.frozen_slowdown_exit_percent = percent;
        self
    }

    /// Set frozen slowdown delay range in milliseconds.
    pub fn frozen_slowdown_delay_ms(mut self, min_ms: u64, max_ms: u64) -> Self {
        self.config.frozen_slowdown_min_delay_ms = min_ms;
        self.config.frozen_slowdown_max_delay_ms = max_ms;
        self
    }

    /// Set async writer wait timeout for hard frozen-cap stalls.
    pub fn frozen_stall_wait_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.config.frozen_stall_wait_timeout_ms = timeout_ms;
        self
    }

    /// Set byte-based memory-pressure limits in bytes. Set both to 0 to disable.
    pub fn mem_pressure_limits_bytes(mut self, soft_bytes: usize, hard_bytes: usize) -> Self {
        self.config.mem_pressure_soft_limit_bytes = soft_bytes;
        self.config.mem_pressure_hard_limit_bytes = hard_bytes;
        self
    }

    /// Set byte-based memory-pressure slowdown delay range in milliseconds.
    pub fn mem_pressure_delay_ms(mut self, min_ms: u64, max_ms: u64) -> Self {
        self.config.mem_pressure_min_delay_ms = min_ms;
        self.config.mem_pressure_max_delay_ms = max_ms;
        self
    }

    /// Set the minimum memtable age before flush.
    pub fn min_memtable_age(mut self, duration: Duration) -> Self {
        self.config.min_memtable_age = Some(duration);
        self
    }

    /// Set the data block size.
    pub fn block_size(mut self, size: usize) -> Self {
        self.config.block_size = size;
        self
    }

    /// Set the compression type.
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.config.compression = compression;
        self
    }

    /// Enable/disable bloom filter usage.
    pub fn bloom_enabled(mut self, enabled: bool) -> Self {
        self.config.bloom_enabled = enabled;
        self
    }

    /// Set bloom filter bits per key.
    pub fn bloom_bits_per_key(mut self, bits: u32) -> Self {
        self.config.bloom_bits_per_key = bits;
        self
    }

    /// Enable/disable shared block cache.
    pub fn shared_block_cache_enabled(mut self, enabled: bool) -> Self {
        self.config.shared_block_cache_enabled = enabled;
        self
    }

    /// Enable/disable reader cache.
    pub fn reader_cache_enabled(mut self, enabled: bool) -> Self {
        self.config.reader_cache_enabled = enabled;
        self
    }

    /// Enable/disable row cache.
    pub fn row_cache_enabled(mut self, enabled: bool) -> Self {
        self.config.row_cache_enabled = enabled;
        self
    }

    /// Enable/disable scan cache fill.
    pub fn scan_fill_cache(mut self, enabled: bool) -> Self {
        self.config.scan_fill_cache = enabled;
        self
    }

    /// Set scan cache-fill threshold in estimated block count.
    pub fn scan_fill_cache_threshold_blocks(mut self, threshold: usize) -> Self {
        self.config.scan_fill_cache_threshold_blocks = threshold;
        self
    }

    /// Set total cache memory ratio.
    pub fn cache_total_ratio(mut self, ratio: f64) -> Self {
        self.config.cache_total_ratio = ratio;
        self
    }

    /// Set reader cache max entry count.
    pub fn reader_cache_max_entries(mut self, max_entries: usize) -> Self {
        self.config.reader_cache_max_entries = max_entries;
        self
    }

    /// Set the target SST file size.
    pub fn target_file_size(mut self, size: usize) -> Self {
        self.config.target_file_size = size;
        self
    }

    /// Set the L0 compaction trigger.
    pub fn l0_compaction_trigger(mut self, count: usize) -> Self {
        self.config.l0_compaction_trigger = count;
        self
    }

    /// Set the L0 slowdown trigger.
    pub fn l0_slowdown_trigger(mut self, count: usize) -> Self {
        self.config.l0_slowdown_trigger = count;
        self
    }

    /// Set the L0 stop trigger.
    pub fn l0_stop_trigger(mut self, count: usize) -> Self {
        self.config.l0_stop_trigger = count;
        self
    }

    /// Set L0 guard band for near-stop flush dispatch limiting.
    pub fn l0_flush_dispatch_guard_band(mut self, files: usize) -> Self {
        self.config.l0_flush_dispatch_guard_band = files;
        self
    }

    /// Set the level size multiplier.
    pub fn level_size_multiplier(mut self, multiplier: usize) -> Self {
        self.config.level_size_multiplier = multiplier;
        self
    }

    /// Set the L1 maximum size.
    pub fn l1_max_size(mut self, size: usize) -> Self {
        self.config.l1_max_size = size;
        self
    }

    /// Set the maximum number of levels.
    pub fn max_levels(mut self, levels: usize) -> Self {
        self.config.max_levels = levels;
        self
    }

    /// Set the number of compaction threads.
    pub fn compaction_threads(mut self, threads: usize) -> Self {
        self.config.compaction_threads = threads;
        self
    }

    /// Set the number of flush threads.
    pub fn flush_threads(mut self, threads: usize) -> Self {
        self.config.flush_threads = threads;
        self
    }

    /// Set whether to verify checksums on read.
    pub fn verify_checksums(mut self, verify: bool) -> Self {
        self.config.verify_checksums = verify;
        self
    }

    /// Set whether to sync writes.
    pub fn sync_writes(mut self, sync: bool) -> Self {
        self.config.sync_writes = sync;
        self
    }

    /// Set initial V2.6 boundary rollout mode.
    pub fn v26_boundary_mode(mut self, mode: V26BoundaryMode) -> Self {
        self.config.v26_boundary_mode = mode;
        self
    }

    /// Build the configuration.
    ///
    /// Returns an error if validation fails.
    pub fn build(self) -> Result<LsmConfig, String> {
        self.config.validate()?;
        Ok(self.config)
    }

    /// Build the configuration without validation.
    pub fn build_unchecked(self) -> LsmConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = LsmConfig::default();
        assert_eq!(config.memtable_size, DEFAULT_MEMTABLE_SIZE);
        assert_eq!(config.max_frozen_memtables, DEFAULT_MAX_FROZEN_MEMTABLES);
        assert_eq!(
            config.frozen_slowdown_enter_percent,
            DEFAULT_FROZEN_SLOWDOWN_ENTER_PERCENT
        );
        assert_eq!(
            config.frozen_slowdown_exit_percent,
            DEFAULT_FROZEN_SLOWDOWN_EXIT_PERCENT
        );
        assert_eq!(
            config.frozen_slowdown_min_delay_ms,
            DEFAULT_FROZEN_SLOWDOWN_MIN_DELAY_MS
        );
        assert_eq!(
            config.frozen_slowdown_max_delay_ms,
            DEFAULT_FROZEN_SLOWDOWN_MAX_DELAY_MS
        );
        assert_eq!(
            config.frozen_stall_wait_timeout_ms,
            DEFAULT_FROZEN_STALL_WAIT_TIMEOUT_MS
        );
        assert_eq!(
            config.mem_pressure_soft_limit_bytes,
            DEFAULT_MEM_PRESSURE_SOFT_LIMIT_BYTES
        );
        assert_eq!(
            config.mem_pressure_hard_limit_bytes,
            DEFAULT_MEM_PRESSURE_HARD_LIMIT_BYTES
        );
        assert_eq!(
            config.mem_pressure_min_delay_ms,
            DEFAULT_MEM_PRESSURE_MIN_DELAY_MS
        );
        assert_eq!(
            config.mem_pressure_max_delay_ms,
            DEFAULT_MEM_PRESSURE_MAX_DELAY_MS
        );
        assert_eq!(config.block_size, DEFAULT_BLOCK_SIZE);
        assert_eq!(config.l0_compaction_trigger, DEFAULT_L0_COMPACTION_TRIGGER);
        assert_eq!(config.l0_slowdown_trigger, DEFAULT_L0_SLOWDOWN_TRIGGER);
        assert_eq!(config.l0_stop_trigger, DEFAULT_L0_STOP_TRIGGER);
        assert_eq!(
            config.l0_flush_dispatch_guard_band,
            DEFAULT_L0_FLUSH_DISPATCH_GUARD_BAND
        );
        assert_eq!(config.max_levels, DEFAULT_MAX_LEVELS);
        assert_eq!(config.bloom_enabled, DEFAULT_BLOOM_ENABLED);
        assert_eq!(config.bloom_bits_per_key, DEFAULT_BLOOM_BITS_PER_KEY);
        assert_eq!(
            config.shared_block_cache_enabled,
            DEFAULT_SHARED_BLOCK_CACHE_ENABLED
        );
        assert_eq!(config.reader_cache_enabled, DEFAULT_READER_CACHE_ENABLED);
        assert_eq!(config.row_cache_enabled, DEFAULT_ROW_CACHE_ENABLED);
        assert_eq!(config.scan_fill_cache, DEFAULT_SCAN_FILL_CACHE);
        assert_eq!(
            config.scan_fill_cache_threshold_blocks,
            DEFAULT_SCAN_FILL_CACHE_THRESHOLD_BLOCKS
        );
        assert_eq!(config.cache_total_ratio, DEFAULT_CACHE_TOTAL_RATIO);
        assert_eq!(
            config.reader_cache_max_entries,
            DEFAULT_READER_CACHE_MAX_ENTRIES
        );
        assert_eq!(config.v26_boundary_mode, V26BoundaryMode::On);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_builder() {
        let config = LsmConfig::builder("./test_data")
            .memtable_size(128 * 1024 * 1024)
            .max_frozen_memtables(8)
            .l0_compaction_trigger(8)
            .l0_slowdown_trigger(16)
            .l0_stop_trigger(24)
            .bloom_enabled(true)
            .bloom_bits_per_key(16)
            .shared_block_cache_enabled(true)
            .reader_cache_enabled(true)
            .row_cache_enabled(true)
            .scan_fill_cache(true)
            .scan_fill_cache_threshold_blocks(16)
            .cache_total_ratio(0.6)
            .reader_cache_max_entries(2048)
            .v26_boundary_mode(V26BoundaryMode::On)
            .build()
            .unwrap();

        assert_eq!(config.memtable_size, 128 * 1024 * 1024);
        assert_eq!(config.max_frozen_memtables, 8);
        assert_eq!(config.l0_compaction_trigger, 8);
        assert_eq!(config.l0_slowdown_trigger, 16);
        assert_eq!(config.l0_stop_trigger, 24);
        assert!(config.bloom_enabled);
        assert_eq!(config.bloom_bits_per_key, 16);
        assert!(config.shared_block_cache_enabled);
        assert!(config.reader_cache_enabled);
        assert!(config.row_cache_enabled);
        assert!(config.scan_fill_cache);
        assert_eq!(config.scan_fill_cache_threshold_blocks, 16);
        assert_eq!(config.cache_total_ratio, 0.6);
        assert_eq!(config.reader_cache_max_entries, 2048);
        assert_eq!(config.v26_boundary_mode, V26BoundaryMode::On);
    }

    #[test]
    fn test_config_level_max_size() {
        let config = LsmConfig::new("./test");

        // L0 returns 0 (file count based)
        assert_eq!(config.level_max_size(0), 0);

        // L1 = 256 MB
        assert_eq!(config.level_max_size(1), DEFAULT_L1_MAX_SIZE);

        // L2 = 256 MB * 10 = 2.56 GB
        assert_eq!(config.level_max_size(2), DEFAULT_L1_MAX_SIZE * 10);

        // L3 = 256 MB * 100 = 25.6 GB
        assert_eq!(config.level_max_size(3), DEFAULT_L1_MAX_SIZE * 100);
    }

    #[test]
    fn test_config_compaction_triggers() {
        let config = LsmConfig::builder("./test")
            .l0_compaction_trigger(4)
            .l0_slowdown_trigger(8)
            .l0_stop_trigger(12)
            .build()
            .unwrap();

        assert!(!config.should_trigger_l0_compaction(3));
        assert!(config.should_trigger_l0_compaction(4));
        assert!(config.should_trigger_l0_compaction(5));

        assert!(!config.should_slowdown_writes(7));
        assert!(config.should_slowdown_writes(8));

        assert!(!config.should_stop_writes(11));
        assert!(config.should_stop_writes(12));
    }

    #[test]
    fn test_config_sst_dir() {
        let config = LsmConfig::new("/data/tisql");
        assert_eq!(config.sst_dir(), PathBuf::from("/data/tisql/sst"));
    }

    #[test]
    fn test_config_validation_memtable_size() {
        let config = LsmConfig::builder("./test")
            .memtable_size(0)
            .build_unchecked();

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_l0_triggers() {
        // slowdown < compaction should fail
        let config = LsmConfig::builder("./test")
            .l0_compaction_trigger(8)
            .l0_slowdown_trigger(4)
            .l0_stop_trigger(12)
            .build_unchecked();

        assert!(config.validate().is_err());

        // stop < slowdown should fail
        let config = LsmConfig::builder("./test")
            .l0_compaction_trigger(4)
            .l0_slowdown_trigger(8)
            .l0_stop_trigger(6)
            .build_unchecked();

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_multiplier() {
        let config = LsmConfig::builder("./test")
            .level_size_multiplier(1)
            .build_unchecked();

        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_min_memtable_age() {
        let config = LsmConfig::builder("./test")
            .min_memtable_age(Duration::from_secs(30))
            .build()
            .unwrap();

        assert_eq!(config.min_memtable_age, Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_config_validation_max_frozen_memtables_zero() {
        let config = LsmConfig::builder("./test")
            .max_frozen_memtables(0)
            .build_unchecked();

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("max_frozen_memtables"));
    }

    #[test]
    fn test_config_validation_block_size_zero() {
        let config = LsmConfig::builder("./test").block_size(0).build_unchecked();

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("block_size"));
    }

    #[test]
    fn test_config_validation_l0_compaction_trigger_zero() {
        let config = LsmConfig::builder("./test")
            .l0_compaction_trigger(0)
            .build_unchecked();

        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("l0_compaction_trigger"));
    }

    #[test]
    fn test_config_builder_block_size() {
        let config = LsmConfig::builder("./test")
            .block_size(8192)
            .build()
            .unwrap();

        assert_eq!(config.block_size, 8192);
    }

    #[test]
    fn test_config_builder_compression() {
        let config = LsmConfig::builder("./test")
            .compression(CompressionType::None)
            .build()
            .unwrap();

        assert_eq!(config.compression, CompressionType::None);
    }

    #[test]
    fn test_config_builder_max_levels() {
        let config = LsmConfig::builder("./test").max_levels(5).build().unwrap();

        assert_eq!(config.max_levels, 5);
    }

    #[test]
    fn test_config_validation_bloom_bits_per_key_zero() {
        let config = LsmConfig::builder("./test")
            .bloom_bits_per_key(0)
            .build_unchecked();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("bloom_bits_per_key"));
    }

    #[test]
    fn test_config_validation_cache_total_ratio_bounds() {
        let low = LsmConfig::builder("./test")
            .cache_total_ratio(-0.1)
            .build_unchecked();
        assert!(low.validate().is_err());

        let high = LsmConfig::builder("./test")
            .cache_total_ratio(1.2)
            .build_unchecked();
        assert!(high.validate().is_err());
    }

    #[test]
    fn test_config_validation_scan_fill_cache_threshold_zero() {
        let config = LsmConfig::builder("./test")
            .scan_fill_cache_threshold_blocks(0)
            .build_unchecked();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("scan_fill_cache_threshold_blocks"));
    }

    #[test]
    fn test_config_validation_reader_cache_entry_cap_when_enabled() {
        let config = LsmConfig::builder("./test")
            .reader_cache_enabled(true)
            .reader_cache_max_entries(0)
            .build_unchecked();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("reader_cache_max_entries"));
    }

    #[test]
    fn test_config_builder_compaction_threads() {
        let config = LsmConfig::builder("./test")
            .compaction_threads(8)
            .build()
            .unwrap();

        assert_eq!(config.compaction_threads, 8);
    }

    #[test]
    fn test_config_builder_flush_threads() {
        let config = LsmConfig::builder("./test")
            .flush_threads(4)
            .build()
            .unwrap();

        assert_eq!(config.flush_threads, 4);
    }

    #[test]
    fn test_config_validation_frozen_hysteresis_invalid() {
        let config = LsmConfig::builder("./test")
            .frozen_slowdown_enter_percent(60)
            .frozen_slowdown_exit_percent(60)
            .build_unchecked();
        let err = config.validate().unwrap_err();
        assert!(err.contains("frozen_slowdown_exit_percent"));
    }

    #[test]
    fn test_config_validation_mem_pressure_limits() {
        let invalid_soft_zero = LsmConfig::builder("./test")
            .mem_pressure_limits_bytes(0, 1024)
            .build_unchecked();
        assert!(invalid_soft_zero.validate().is_err());

        let invalid_hard_smaller = LsmConfig::builder("./test")
            .mem_pressure_limits_bytes(2048, 1024)
            .build_unchecked();
        assert!(invalid_hard_smaller.validate().is_err());

        let valid_enabled = LsmConfig::builder("./test")
            .mem_pressure_limits_bytes(1024, 2048)
            .build()
            .unwrap();
        assert_eq!(valid_enabled.mem_pressure_soft_limit_bytes, 1024);
        assert_eq!(valid_enabled.mem_pressure_hard_limit_bytes, 2048);

        let invalid_enabled_delay = LsmConfig::builder("./test")
            .mem_pressure_limits_bytes(1024, 2048)
            .mem_pressure_delay_ms(0, 1)
            .build_unchecked();
        assert!(invalid_enabled_delay.validate().is_err());

        let disabled_model_allows_zero_delay = LsmConfig::builder("./test")
            .mem_pressure_limits_bytes(0, 0)
            .mem_pressure_delay_ms(0, 0)
            .build_unchecked();
        assert!(disabled_model_allows_zero_delay.validate().is_ok());
    }

    #[test]
    fn test_config_builder_verify_checksums() {
        let config = LsmConfig::builder("./test")
            .verify_checksums(false)
            .build()
            .unwrap();

        assert!(!config.verify_checksums);
    }

    #[test]
    fn test_config_builder_sync_writes() {
        let config = LsmConfig::builder("./test")
            .sync_writes(false)
            .build()
            .unwrap();

        assert!(!config.sync_writes);
    }

    #[test]
    fn test_config_builder_target_file_size() {
        let config = LsmConfig::builder("./test")
            .target_file_size(128 * 1024 * 1024)
            .build()
            .unwrap();

        assert_eq!(config.target_file_size, 128 * 1024 * 1024);
    }

    #[test]
    fn test_config_builder_l1_max_size() {
        let config = LsmConfig::builder("./test")
            .l1_max_size(512 * 1024 * 1024)
            .build()
            .unwrap();

        assert_eq!(config.l1_max_size, 512 * 1024 * 1024);
    }
}
