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
//! | max_frozen_memtables | 4 | Max frozen memtables before stalling |
//! | block_size | 4 KB | Target data block size |
//! | l0_compaction_trigger | 4 | L0 file count to trigger compaction |
//! | level_size_multiplier | 10 | Size ratio between adjacent levels |
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

use crate::storage::sstable::CompressionType;

/// Default memtable size (64 MB).
pub const DEFAULT_MEMTABLE_SIZE: usize = 64 * 1024 * 1024;

/// Default maximum frozen memtables.
pub const DEFAULT_MAX_FROZEN_MEMTABLES: usize = 4;

/// Default data block size (4 KB).
pub const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;

/// Default L0 compaction trigger (file count).
pub const DEFAULT_L0_COMPACTION_TRIGGER: usize = 4;

/// Default level size multiplier (10x between levels).
pub const DEFAULT_LEVEL_SIZE_MULTIPLIER: usize = 10;

/// Default L1 total size (256 MB).
pub const DEFAULT_L1_MAX_SIZE: usize = 256 * 1024 * 1024;

/// Default target file size (64 MB).
pub const DEFAULT_TARGET_FILE_SIZE: usize = 64 * 1024 * 1024;

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
}

impl LsmConfig {
    /// Create a new configuration with defaults and the given data directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            memtable_size: DEFAULT_MEMTABLE_SIZE,
            max_frozen_memtables: DEFAULT_MAX_FROZEN_MEMTABLES,
            min_memtable_age: None,
            block_size: DEFAULT_BLOCK_SIZE,
            compression: CompressionType::None,
            target_file_size: DEFAULT_TARGET_FILE_SIZE,
            l0_compaction_trigger: DEFAULT_L0_COMPACTION_TRIGGER,
            l0_slowdown_trigger: DEFAULT_L0_COMPACTION_TRIGGER * 2,
            l0_stop_trigger: DEFAULT_L0_COMPACTION_TRIGGER * 3,
            level_size_multiplier: DEFAULT_LEVEL_SIZE_MULTIPLIER,
            l1_max_size: DEFAULT_L1_MAX_SIZE,
            max_levels: 7,
            compaction_threads: 2,
            flush_threads: 1,
            verify_checksums: true,
            sync_writes: true,
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
        if self.level_size_multiplier < 2 {
            return Err("level_size_multiplier must be >= 2".to_string());
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
        assert_eq!(config.block_size, DEFAULT_BLOCK_SIZE);
        assert_eq!(config.l0_compaction_trigger, DEFAULT_L0_COMPACTION_TRIGGER);
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
            .build()
            .unwrap();

        assert_eq!(config.memtable_size, 128 * 1024 * 1024);
        assert_eq!(config.max_frozen_memtables, 8);
        assert_eq!(config.l0_compaction_trigger, 8);
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
