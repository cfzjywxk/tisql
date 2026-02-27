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

use std::sync::atomic::{AtomicU64, Ordering};

use hashlink::LinkedHashMap;
use parking_lot::Mutex;

use super::key::ReaderCacheKey;
use crate::tablet::sstable::SstReaderRef;
use crate::util::stable_hash64;

const READER_SHARD_HASH_SEED: u64 = 0x5243_5348_4152_4431; // "RCSHARD1"
pub const READER_CACHE_MAX_SHARDS: usize = 16;

#[derive(Debug, Default, Clone, Copy)]
pub struct ReaderCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub invalidations: u64,
}

struct ReaderShard {
    map: LinkedHashMap<ReaderCacheKey, SstReaderRef>,
    max_entries: usize,
}

/// Entry-count-capped sharded reader cache.
pub struct ReaderCache {
    shards: Vec<Mutex<ReaderShard>>,
    shard_mask: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
    hits_delta: AtomicU64,
    misses_delta: AtomicU64,
    inserts_delta: AtomicU64,
    evictions_delta: AtomicU64,
    invalidations_delta: AtomicU64,
}

impl ReaderCache {
    pub fn new(max_entries: usize, shard_count: usize) -> Self {
        assert!(shard_count > 0, "reader-cache shard_count must be > 0");
        assert!(
            shard_count.is_power_of_two(),
            "reader-cache shard_count must be power-of-two"
        );

        let shard_caps = split_capacity_by_shards(max_entries, shard_count);
        let shards = shard_caps
            .into_iter()
            .map(|cap| {
                Mutex::new(ReaderShard {
                    map: LinkedHashMap::new(),
                    max_entries: cap,
                })
            })
            .collect();

        Self {
            shards,
            shard_mask: shard_count - 1,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            hits_delta: AtomicU64::new(0),
            misses_delta: AtomicU64::new(0),
            inserts_delta: AtomicU64::new(0),
            evictions_delta: AtomicU64::new(0),
            invalidations_delta: AtomicU64::new(0),
        }
    }

    pub fn auto_shard_count() -> usize {
        auto_shard_count_for_parallelism(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
        )
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn get(&self, key: &ReaderCacheKey) -> Option<SstReaderRef> {
        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].lock();
        let value = shard.map.to_back(key).cloned();
        if value.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
            self.hits_delta.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            self.misses_delta.fetch_add(1, Ordering::Relaxed);
        }
        value
    }

    pub fn insert(&self, key: ReaderCacheKey, value: SstReaderRef) {
        let shard_idx = self.shard_index(&key);
        let mut shard = self.shards[shard_idx].lock();
        if shard.max_entries == 0 {
            return;
        }

        shard.map.remove(&key);
        shard.map.insert(key, value);
        self.inserts.fetch_add(1, Ordering::Relaxed);
        self.inserts_delta.fetch_add(1, Ordering::Relaxed);

        while shard.map.len() > shard.max_entries {
            if shard.map.pop_front().is_none() {
                break;
            }
            self.evictions.fetch_add(1, Ordering::Relaxed);
            self.evictions_delta.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn remove(&self, key: &ReaderCacheKey) {
        let shard_idx = self.shard_index(key);
        let mut shard = self.shards[shard_idx].lock();
        if shard.map.remove(key).is_some() {
            self.invalidations.fetch_add(1, Ordering::Relaxed);
            self.invalidations_delta.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn remove_sst(&self, ns: u128, sst_id: u64) {
        self.remove(&ReaderCacheKey { ns, sst_id });
    }

    pub fn stats(&self) -> ReaderCacheStats {
        ReaderCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
        }
    }

    pub fn snapshot_and_reset_delta(&self) -> ReaderCacheStats {
        ReaderCacheStats {
            hits: self.hits_delta.swap(0, Ordering::Relaxed),
            misses: self.misses_delta.swap(0, Ordering::Relaxed),
            inserts: self.inserts_delta.swap(0, Ordering::Relaxed),
            evictions: self.evictions_delta.swap(0, Ordering::Relaxed),
            invalidations: self.invalidations_delta.swap(0, Ordering::Relaxed),
        }
    }

    fn shard_index(&self, key: &ReaderCacheKey) -> usize {
        (reader_key_hash(key) as usize) & self.shard_mask
    }
}

pub fn normalize_reader_shards(value: usize) -> Option<usize> {
    if value == 0 {
        return None;
    }
    if value <= READER_CACHE_MAX_SHARDS && value.is_power_of_two() {
        Some(value)
    } else {
        None
    }
}

pub fn auto_shard_count_for_parallelism(parallelism: usize) -> usize {
    let clamped = parallelism.clamp(1, READER_CACHE_MAX_SHARDS);
    1usize << (usize::BITS - 1 - clamped.leading_zeros())
}

fn split_capacity_by_shards(total: usize, shard_count: usize) -> Vec<usize> {
    let base = total / shard_count;
    let rem = total % shard_count;
    (0..shard_count)
        .map(|idx| base + usize::from(idx < rem))
        .collect()
}

fn reader_key_hash(key: &ReaderCacheKey) -> u64 {
    let mut bytes = [0u8; std::mem::size_of::<u128>() + std::mem::size_of::<u64>()];
    let (ns_bytes, sst_bytes) = bytes.split_at_mut(std::mem::size_of::<u128>());
    ns_bytes.copy_from_slice(&key.ns.to_le_bytes());
    sst_bytes.copy_from_slice(&key.sst_id.to_le_bytes());
    stable_hash64(READER_SHARD_HASH_SEED, &bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::IoService;
    use crate::tablet::cache::ReaderCacheKey;
    use crate::tablet::sstable::{SstBuilder, SstBuilderOptions};
    use tempfile::tempdir;

    fn key(ns: u128, sst_id: u64) -> ReaderCacheKey {
        ReaderCacheKey { ns, sst_id }
    }

    fn test_reader() -> SstReaderRef {
        let dir = tempdir().unwrap();
        let path = dir.path().join("reader-cache-test.sst");
        let builder = SstBuilder::new(&path, SstBuilderOptions::default()).unwrap();
        builder.finish(1, 0).unwrap();
        let io = IoService::new_for_test(32).unwrap();
        crate::io::block_on_sync(SstReaderRef::open(path, io)).unwrap()
    }

    fn key_in_shard(cache: &ReaderCache, shard: usize, next_sst_id: &mut u64) -> ReaderCacheKey {
        for _ in 0..10_000 {
            let candidate = key(77, *next_sst_id);
            *next_sst_id += 1;
            if cache.shard_index(&candidate) == shard {
                return candidate;
            }
        }
        panic!("failed to find key for shard {shard}");
    }

    #[test]
    fn test_reader_cache_get_insert() {
        let cache = ReaderCache::new(8, 1);
        let k = key(11, 9);

        assert!(cache.get(&k).is_none());
        cache.insert(k, test_reader());
        assert!(cache.get(&k).is_some());

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.inserts, 1);
    }

    #[test]
    fn test_reader_cache_entry_cap_and_remove_sst() {
        let cache = ReaderCache::new(2, 1);
        let k1 = key(1, 1);
        let k2 = key(1, 2);
        let k3 = key(1, 3);

        cache.insert(k1, test_reader());
        cache.insert(k2, test_reader());
        cache.insert(k3, test_reader());

        let present = [
            cache.get(&k1).is_some() as u8,
            cache.get(&k2).is_some() as u8,
            cache.get(&k3).is_some() as u8,
        ]
        .into_iter()
        .sum::<u8>();
        assert_eq!(present, 2);
        assert!(cache.stats().evictions >= 1);

        cache.remove_sst(1, 3);
        assert!(cache.get(&k3).is_none());
    }

    #[test]
    fn test_reader_cache_remove_sst_keeps_inflight_handle_alive() {
        let cache = ReaderCache::new(8, 1);
        let k = key(9, 99);
        cache.insert(k, test_reader());

        let inflight = cache.get(&k).unwrap();
        cache.remove_sst(9, 99);

        assert!(cache.get(&k).is_none());
        assert_eq!(inflight.num_blocks().unwrap(), 0);
    }

    #[test]
    fn test_auto_reader_shards_clamps_to_power_of_two() {
        assert_eq!(auto_shard_count_for_parallelism(1), 1);
        assert_eq!(auto_shard_count_for_parallelism(3), 2);
        assert_eq!(auto_shard_count_for_parallelism(8), 8);
        assert_eq!(auto_shard_count_for_parallelism(32), 16);
    }

    #[test]
    fn test_normalize_reader_shards_accepts_only_power_of_two_up_to_cap() {
        assert_eq!(normalize_reader_shards(0), None);
        assert_eq!(normalize_reader_shards(1), Some(1));
        assert_eq!(normalize_reader_shards(4), Some(4));
        assert_eq!(normalize_reader_shards(16), Some(16));
        assert_eq!(normalize_reader_shards(3), None);
        assert_eq!(normalize_reader_shards(17), None);
    }

    #[test]
    fn test_reader_cache_shard_count_exposed() {
        let cache = ReaderCache::new(100, 8);
        assert_eq!(cache.shard_count(), 8);
    }

    #[test]
    fn test_reader_cache_multi_shard_eviction_isolated_per_shard() {
        let cache = ReaderCache::new(4, 4);
        let mut next_sst_id = 1;
        let mut first_keys = Vec::new();

        for shard in 0..cache.shard_count() {
            let k = key_in_shard(&cache, shard, &mut next_sst_id);
            cache.insert(k, test_reader());
            first_keys.push(k);
        }

        for k in &first_keys {
            assert!(cache.get(k).is_some());
        }

        // Each shard gets capacity=1 here, so inserting another key in shard 0
        // evicts only shard 0's previous key.
        let replacement = key_in_shard(&cache, 0, &mut next_sst_id);
        cache.insert(replacement, test_reader());

        assert!(cache.get(&first_keys[0]).is_none());
        assert!(cache.get(&replacement).is_some());
        for k in first_keys.iter().skip(1) {
            assert!(cache.get(k).is_some());
        }
    }
}
