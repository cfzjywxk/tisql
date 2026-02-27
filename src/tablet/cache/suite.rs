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

use std::sync::Arc;

use crate::tablet::config::LsmConfig;

use super::block_cache::SharedBlockCache;
use super::reader_cache::{normalize_reader_shards, ReaderCache};
use super::row_cache::RowCache;

/// Global cache-suite configuration.
#[derive(Debug, Clone)]
pub struct CacheSuiteConfig {
    pub shared_block_cache_enabled: bool,
    pub reader_cache_enabled: bool,
    pub row_cache_enabled: bool,
    pub scan_fill_cache: bool,
    pub scan_fill_cache_threshold_blocks: usize,
    pub cache_total_ratio: f64,
    pub reader_cache_max_entries: usize,
    /// Reader-cache shard override (0 = auto).
    pub reader_cache_shards: usize,
}

impl CacheSuiteConfig {
    pub fn from_lsm_config(config: &LsmConfig) -> Self {
        Self {
            shared_block_cache_enabled: config.shared_block_cache_enabled,
            reader_cache_enabled: config.reader_cache_enabled,
            row_cache_enabled: config.row_cache_enabled,
            scan_fill_cache: config.scan_fill_cache,
            scan_fill_cache_threshold_blocks: config.scan_fill_cache_threshold_blocks,
            cache_total_ratio: config.cache_total_ratio,
            reader_cache_max_entries: config.reader_cache_max_entries,
            reader_cache_shards: config.reader_cache_shards,
        }
    }

    fn normalized_ratio(&self) -> f64 {
        self.cache_total_ratio.clamp(0.0, 0.95)
    }
}

impl Default for CacheSuiteConfig {
    fn default() -> Self {
        Self {
            shared_block_cache_enabled: false,
            reader_cache_enabled: false,
            row_cache_enabled: false,
            scan_fill_cache: false,
            scan_fill_cache_threshold_blocks: 8,
            cache_total_ratio: 0.50,
            reader_cache_max_entries: 100_000,
            reader_cache_shards: 0,
        }
    }
}

fn detect_system_memory_bytes() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
            for line in contents.lines() {
                if let Some(rest) = line.strip_prefix("MemTotal:") {
                    let kb = rest
                        .split_whitespace()
                        .next()
                        .and_then(|s| s.parse::<usize>().ok());
                    if let Some(kb) = kb {
                        return kb.saturating_mul(1024);
                    }
                }
            }
        }
    }

    // Conservative fallback.
    8 * 1024 * 1024 * 1024
}

fn compute_total_cache_bytes(ratio: f64) -> usize {
    let mem = detect_system_memory_bytes();
    let by_ratio = (mem as f64 * ratio) as usize;
    by_ratio.max(512 * 1024 * 1024)
}

/// One process-wide cache suite shared by all tablets.
pub struct CacheSuite {
    config: CacheSuiteConfig,
    block_cache: Option<Arc<SharedBlockCache>>,
    reader_cache: Option<Arc<ReaderCache>>,
    row_cache: Option<Arc<RowCache>>,
}

impl CacheSuite {
    pub fn new(config: CacheSuiteConfig) -> Self {
        let total = compute_total_cache_bytes(config.normalized_ratio());
        let block_budget = total * 7 / 8;
        let row_budget = total.saturating_sub(block_budget);

        let block_cache = if config.shared_block_cache_enabled {
            Some(Arc::new(SharedBlockCache::new(block_budget)))
        } else {
            None
        };
        let reader_cache = if config.reader_cache_enabled {
            let shard_count = match normalize_reader_shards(config.reader_cache_shards) {
                Some(shards) => shards,
                None if config.reader_cache_shards == 0 => ReaderCache::auto_shard_count(),
                None => panic!(
                    "invalid reader_cache_shards={} (expected 0 or power-of-two in [1,16])",
                    config.reader_cache_shards
                ),
            };
            Some(Arc::new(ReaderCache::new(
                config.reader_cache_max_entries,
                shard_count,
            )))
        } else {
            None
        };
        let row_cache = if config.row_cache_enabled {
            Some(Arc::new(RowCache::new(row_budget.max(64 * 1024 * 1024))))
        } else {
            None
        };

        Self {
            config,
            block_cache,
            reader_cache,
            row_cache,
        }
    }

    pub fn config(&self) -> &CacheSuiteConfig {
        &self.config
    }

    pub fn block_cache(&self) -> Option<Arc<SharedBlockCache>> {
        self.block_cache.clone()
    }

    pub fn reader_cache(&self) -> Option<Arc<ReaderCache>> {
        self.reader_cache.clone()
    }

    pub fn row_cache(&self) -> Option<Arc<RowCache>> {
        self.row_cache.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_suite_respects_feature_gates() {
        let suite = CacheSuite::new(CacheSuiteConfig {
            shared_block_cache_enabled: true,
            reader_cache_enabled: false,
            row_cache_enabled: true,
            scan_fill_cache: false,
            scan_fill_cache_threshold_blocks: 8,
            cache_total_ratio: 0.50,
            reader_cache_max_entries: 10,
            reader_cache_shards: 0,
        });

        assert!(suite.block_cache().is_some());
        assert!(suite.reader_cache().is_none());
        assert!(suite.row_cache().is_some());
    }

    #[test]
    fn test_cache_suite_from_lsm_config() {
        let cfg = LsmConfig::builder("./test-cache-suite")
            .shared_block_cache_enabled(true)
            .reader_cache_enabled(true)
            .row_cache_enabled(false)
            .scan_fill_cache(true)
            .scan_fill_cache_threshold_blocks(12)
            .cache_total_ratio(0.6)
            .reader_cache_max_entries(321)
            .reader_cache_shards(8)
            .build()
            .unwrap();

        let suite_cfg = CacheSuiteConfig::from_lsm_config(&cfg);
        assert!(suite_cfg.shared_block_cache_enabled);
        assert!(suite_cfg.reader_cache_enabled);
        assert!(!suite_cfg.row_cache_enabled);
        assert!(suite_cfg.scan_fill_cache);
        assert_eq!(suite_cfg.scan_fill_cache_threshold_blocks, 12);
        assert_eq!(suite_cfg.reader_cache_max_entries, 321);
        assert_eq!(suite_cfg.reader_cache_shards, 8);
    }

    #[test]
    fn test_cache_suite_enables_all_cache_layers() {
        let suite = CacheSuite::new(CacheSuiteConfig {
            shared_block_cache_enabled: true,
            reader_cache_enabled: true,
            row_cache_enabled: true,
            scan_fill_cache: true,
            scan_fill_cache_threshold_blocks: 16,
            cache_total_ratio: 0.50,
            reader_cache_max_entries: 64,
            reader_cache_shards: 4,
        });

        assert!(suite.block_cache().is_some());
        assert!(suite.reader_cache().is_some());
        assert!(suite.row_cache().is_some());
        assert_eq!(suite.reader_cache().unwrap().shard_count(), 4);
    }
}
