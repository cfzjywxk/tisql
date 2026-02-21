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

use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use hashlink::LinkedHashMap;
use parking_lot::Mutex;

use super::key::{BlockCacheKey, BlockKind, CachePriority, TabletCacheNs};

/// Cache value type for block payloads.
pub type BlockCacheValue = Arc<[u8]>;

#[derive(Debug, Default, Clone, Copy)]
pub struct BlockCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub insert_rejects: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BlockCacheNamespaceUsage {
    pub ns: TabletCacheNs,
    pub data_usage_bytes: u64,
    pub data_entries: u64,
    pub index_usage_bytes: u64,
    pub index_entries: u64,
    pub bloom_usage_bytes: u64,
    pub bloom_entries: u64,
}

struct BlockEntry {
    value: BlockCacheValue,
    charge: usize,
}

struct BlockShard {
    normal: LinkedHashMap<BlockCacheKey, BlockEntry>,
    high: LinkedHashMap<BlockCacheKey, BlockEntry>,
    usage: usize,
    capacity: usize,
}

impl BlockShard {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            normal: LinkedHashMap::new(),
            high: LinkedHashMap::new(),
            usage: 0,
            capacity,
        }
    }

    fn remove_existing<F>(&mut self, key: &BlockCacheKey, on_remove: &mut F)
    where
        F: FnMut(&BlockCacheKey, usize),
    {
        if let Some(old) = self.normal.remove(key).or_else(|| self.high.remove(key)) {
            self.usage = self.usage.saturating_sub(old.charge);
            on_remove(key, old.charge);
        }
    }

    fn get(&mut self, key: &BlockCacheKey) -> Option<BlockCacheValue> {
        if let Some(entry) = self.normal.to_back(key) {
            return Some(Arc::clone(&entry.value));
        }
        self.high.to_back(key).map(|entry| Arc::clone(&entry.value))
    }

    fn evict_one_normal_first<F>(&mut self, on_remove: &mut F) -> bool
    where
        F: FnMut(&BlockCacheKey, usize),
    {
        if let Some((key, entry)) = self.normal.pop_front() {
            self.usage = self.usage.saturating_sub(entry.charge);
            on_remove(&key, entry.charge);
            return true;
        }
        if let Some((key, entry)) = self.high.pop_front() {
            self.usage = self.usage.saturating_sub(entry.charge);
            on_remove(&key, entry.charge);
            return true;
        }
        false
    }

    fn insert<F>(
        &mut self,
        key: BlockCacheKey,
        value: BlockCacheValue,
        charge: usize,
        priority: CachePriority,
        mut on_remove: F,
    ) -> (bool, u64)
    where
        F: FnMut(&BlockCacheKey, usize),
    {
        if charge > self.capacity {
            return (false, 0);
        }

        self.remove_existing(&key, &mut on_remove);

        let mut evicted = 0;
        while self.usage + charge > self.capacity {
            if !self.evict_one_normal_first(&mut on_remove) {
                return (false, evicted);
            }
            evicted += 1;
        }

        let entry = BlockEntry { value, charge };
        match priority {
            CachePriority::Normal => {
                self.normal.insert(key, entry);
            }
            CachePriority::High => {
                self.high.insert(key, entry);
            }
        }
        self.usage += charge;
        (true, evicted)
    }

    fn remove_by_sst<F>(&mut self, ns: u128, sst_id: u64, mut on_remove: F) -> u64
    where
        F: FnMut(&BlockCacheKey, usize),
    {
        let mut removed = 0u64;
        let normal_victims: Vec<BlockCacheKey> = self
            .normal
            .keys()
            .copied()
            .filter(|k| k.ns == ns && k.sst_id == sst_id)
            .collect();
        for key in normal_victims {
            if let Some(entry) = self.normal.remove(&key) {
                self.usage = self.usage.saturating_sub(entry.charge);
                on_remove(&key, entry.charge);
                removed += 1;
            }
        }

        let high_victims: Vec<BlockCacheKey> = self
            .high
            .keys()
            .copied()
            .filter(|k| k.ns == ns && k.sst_id == sst_id)
            .collect();
        for key in high_victims {
            if let Some(entry) = self.high.remove(&key) {
                self.usage = self.usage.saturating_sub(entry.charge);
                on_remove(&key, entry.charge);
                removed += 1;
            }
        }
        removed
    }
}

#[derive(Default)]
struct BlockNamespaceSlot {
    data_usage_bytes: AtomicU64,
    data_entries: AtomicU64,
    index_usage_bytes: AtomicU64,
    index_entries: AtomicU64,
    bloom_usage_bytes: AtomicU64,
    bloom_entries: AtomicU64,
}

/// Process-wide shared block cache (sharded strict-cap LRU).
pub struct SharedBlockCache {
    shards: Vec<Mutex<BlockShard>>,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
    invalidations: AtomicU64,
    insert_rejects: AtomicU64,
    hits_delta: AtomicU64,
    misses_delta: AtomicU64,
    inserts_delta: AtomicU64,
    evictions_delta: AtomicU64,
    invalidations_delta: AtomicU64,
    insert_rejects_delta: AtomicU64,
    namespace_slots: DashMap<TabletCacheNs, Arc<BlockNamespaceSlot>>,
}

impl SharedBlockCache {
    pub fn new(capacity_bytes: usize) -> Self {
        let shard_count = ((capacity_bytes / (512 * 1024)).clamp(1, 64)).next_power_of_two();
        let per_shard = (capacity_bytes / shard_count).max(1);
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(Mutex::new(BlockShard::with_capacity(per_shard)));
        }
        Self {
            shards,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            insert_rejects: AtomicU64::new(0),
            hits_delta: AtomicU64::new(0),
            misses_delta: AtomicU64::new(0),
            inserts_delta: AtomicU64::new(0),
            evictions_delta: AtomicU64::new(0),
            invalidations_delta: AtomicU64::new(0),
            insert_rejects_delta: AtomicU64::new(0),
            namespace_slots: DashMap::new(),
        }
    }

    fn shard_index(&self, key: &BlockCacheKey) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(key, &mut hasher);
        (hasher.finish() as usize) & (self.shards.len() - 1)
    }

    pub fn get(&self, key: &BlockCacheKey) -> Option<BlockCacheValue> {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].lock();
        match shard.get(key) {
            Some(v) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                self.hits_delta.fetch_add(1, Ordering::Relaxed);
                Some(v)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                self.misses_delta.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn insert(
        &self,
        key: BlockCacheKey,
        value: BlockCacheValue,
        charge: usize,
        priority: CachePriority,
    ) -> bool {
        let idx = self.shard_index(&key);
        let mut shard = self.shards[idx].lock();
        let (ok, evicted) = shard.insert(key, value, charge, priority, |removed_key, removed| {
            self.account_remove(removed_key.ns, removed_key.block_kind, removed as u64);
        });
        if ok {
            self.inserts.fetch_add(1, Ordering::Relaxed);
            self.inserts_delta.fetch_add(1, Ordering::Relaxed);
            self.account_add(key.ns, key.block_kind, charge as u64);
            if evicted > 0 {
                self.evictions.fetch_add(evicted, Ordering::Relaxed);
                self.evictions_delta.fetch_add(evicted, Ordering::Relaxed);
            }
        } else {
            self.insert_rejects.fetch_add(1, Ordering::Relaxed);
            self.insert_rejects_delta.fetch_add(1, Ordering::Relaxed);
        }
        ok
    }

    pub fn register_namespace(&self, ns: TabletCacheNs) {
        let _ = self.slot_for_ns(ns);
    }

    pub fn unregister_namespace(&self, ns: TabletCacheNs) {
        self.namespace_slots.remove(&ns);
    }

    fn slot_for_ns(&self, ns: TabletCacheNs) -> Arc<BlockNamespaceSlot> {
        if let Some(slot) = self.namespace_slots.get(&ns) {
            return Arc::clone(slot.value());
        }
        let slot = Arc::new(BlockNamespaceSlot::default());
        Arc::clone(
            self.namespace_slots
                .entry(ns)
                .or_insert_with(|| Arc::clone(&slot))
                .value(),
        )
    }

    fn account_add(&self, ns: TabletCacheNs, kind: BlockKind, charge: u64) {
        let slot = self.slot_for_ns(ns);
        match kind {
            BlockKind::Data => {
                slot.data_entries.fetch_add(1, Ordering::Relaxed);
                slot.data_usage_bytes.fetch_add(charge, Ordering::Relaxed);
            }
            BlockKind::Index => {
                slot.index_entries.fetch_add(1, Ordering::Relaxed);
                slot.index_usage_bytes.fetch_add(charge, Ordering::Relaxed);
            }
            BlockKind::Bloom => {
                slot.bloom_entries.fetch_add(1, Ordering::Relaxed);
                slot.bloom_usage_bytes.fetch_add(charge, Ordering::Relaxed);
            }
        }
    }

    fn account_remove(&self, ns: TabletCacheNs, kind: BlockKind, charge: u64) {
        let slot = self.slot_for_ns(ns);
        let dec = |counter: &AtomicU64, delta: u64| {
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(delta))
            });
        };
        match kind {
            BlockKind::Data => {
                dec(&slot.data_entries, 1);
                dec(&slot.data_usage_bytes, charge);
            }
            BlockKind::Index => {
                dec(&slot.index_entries, 1);
                dec(&slot.index_usage_bytes, charge);
            }
            BlockKind::Bloom => {
                dec(&slot.bloom_entries, 1);
                dec(&slot.bloom_usage_bytes, charge);
            }
        }
    }

    pub fn remove_sst(&self, ns: u128, sst_id: u64) {
        let mut removed_total = 0u64;
        for shard in &self.shards {
            removed_total += shard
                .lock()
                .remove_by_sst(ns, sst_id, |removed_key, removed| {
                    self.account_remove(removed_key.ns, removed_key.block_kind, removed as u64);
                });
        }
        if removed_total > 0 {
            self.invalidations
                .fetch_add(removed_total, Ordering::Relaxed);
            self.invalidations_delta
                .fetch_add(removed_total, Ordering::Relaxed);
        }
    }

    pub fn stats(&self) -> BlockCacheStats {
        BlockCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            insert_rejects: self.insert_rejects.load(Ordering::Relaxed),
        }
    }

    pub fn snapshot_and_reset_delta(&self) -> BlockCacheStats {
        BlockCacheStats {
            hits: self.hits_delta.swap(0, Ordering::Relaxed),
            misses: self.misses_delta.swap(0, Ordering::Relaxed),
            inserts: self.inserts_delta.swap(0, Ordering::Relaxed),
            evictions: self.evictions_delta.swap(0, Ordering::Relaxed),
            invalidations: self.invalidations_delta.swap(0, Ordering::Relaxed),
            insert_rejects: self.insert_rejects_delta.swap(0, Ordering::Relaxed),
        }
    }

    pub fn namespace_usage_snapshot(&self) -> Vec<BlockCacheNamespaceUsage> {
        let mut rows: Vec<_> = self
            .namespace_slots
            .iter()
            .map(|entry| BlockCacheNamespaceUsage {
                ns: *entry.key(),
                data_usage_bytes: entry.value().data_usage_bytes.load(Ordering::Relaxed),
                data_entries: entry.value().data_entries.load(Ordering::Relaxed),
                index_usage_bytes: entry.value().index_usage_bytes.load(Ordering::Relaxed),
                index_entries: entry.value().index_entries.load(Ordering::Relaxed),
                bloom_usage_bytes: entry.value().bloom_usage_bytes.load(Ordering::Relaxed),
                bloom_entries: entry.value().bloom_entries.load(Ordering::Relaxed),
            })
            .collect();
        rows.sort_by_key(|row| row.ns);
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::cache::{BlockCacheKey, BlockKind, CachePriority};
    use std::sync::{Arc, Barrier};
    use std::thread;

    fn key(ns: u128, sst_id: u64, offset: u64, kind: BlockKind) -> BlockCacheKey {
        BlockCacheKey {
            ns,
            sst_id,
            block_offset: offset,
            block_kind: kind,
        }
    }

    #[test]
    fn test_block_cache_get_hit_miss_and_stats() {
        let cache = SharedBlockCache::new(1024);
        let k = key(1, 7, 0, BlockKind::Data);
        assert!(cache.get(&k).is_none());
        assert!(cache.insert(
            k,
            Arc::<[u8]>::from(vec![1u8, 2, 3]),
            3,
            CachePriority::Normal
        ));
        let v = cache.get(&k).unwrap();
        assert_eq!(v.as_ref(), &[1u8, 2, 3]);

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.inserts, 1);
    }

    #[test]
    fn test_block_cache_rejects_entry_larger_than_capacity() {
        let cache = SharedBlockCache::new(64);
        let k = key(1, 9, 0, BlockKind::Data);
        assert!(!cache.insert(
            k,
            Arc::<[u8]>::from(vec![7u8; 128]),
            128,
            CachePriority::Normal
        ));
        assert!(cache.get(&k).is_none());
        assert_eq!(cache.stats().insert_rejects, 1);
    }

    #[test]
    fn test_block_cache_evicts_normal_before_high_priority() {
        let cache = SharedBlockCache::new(96);
        let high = key(1, 1, 0, BlockKind::Index);
        let normal_old = key(1, 1, 1, BlockKind::Data);
        let normal_new = key(1, 1, 2, BlockKind::Data);

        assert!(cache.insert(
            high,
            Arc::<[u8]>::from(vec![1u8; 32]),
            32,
            CachePriority::High
        ));
        assert!(cache.insert(
            normal_old,
            Arc::<[u8]>::from(vec![2u8; 32]),
            32,
            CachePriority::Normal
        ));
        assert!(cache.insert(
            normal_new,
            Arc::<[u8]>::from(vec![3u8; 32]),
            32,
            CachePriority::Normal
        ));

        // This insert requires one eviction. High-priority entry should survive.
        let normal_extra = key(1, 1, 3, BlockKind::Data);
        assert!(cache.insert(
            normal_extra,
            Arc::<[u8]>::from(vec![4u8; 32]),
            32,
            CachePriority::Normal
        ));

        assert!(cache.get(&high).is_some());
        let survivors = [
            cache.get(&normal_old).is_some() as u8,
            cache.get(&normal_new).is_some() as u8,
            cache.get(&normal_extra).is_some() as u8,
        ]
        .into_iter()
        .sum::<u8>();
        assert_eq!(survivors, 2);
        assert!(cache.stats().evictions >= 1);
    }

    #[test]
    fn test_block_cache_remove_sst() {
        let cache = SharedBlockCache::new(1024);
        let keep = key(1, 10, 0, BlockKind::Data);
        let drop1 = key(2, 11, 0, BlockKind::Data);
        let drop2 = key(2, 11, 128, BlockKind::Index);

        assert!(cache.insert(
            keep,
            Arc::<[u8]>::from(vec![1u8; 8]),
            8,
            CachePriority::Normal
        ));
        assert!(cache.insert(
            drop1,
            Arc::<[u8]>::from(vec![2u8; 8]),
            8,
            CachePriority::Normal
        ));
        assert!(cache.insert(
            drop2,
            Arc::<[u8]>::from(vec![3u8; 8]),
            8,
            CachePriority::High
        ));

        cache.remove_sst(2, 11);
        assert!(cache.get(&keep).is_some());
        assert!(cache.get(&drop1).is_none());
        assert!(cache.get(&drop2).is_none());
    }

    #[test]
    fn test_block_cache_concurrent_get_insert_and_remove_sst() {
        let cache = Arc::new(SharedBlockCache::new(16 * 1024));
        let keep = key(3, 200, 0, BlockKind::Data);
        let drop = key(4, 300, 0, BlockKind::Data);
        assert!(cache.insert(
            keep,
            Arc::<[u8]>::from(vec![9u8; 32]),
            32,
            CachePriority::High
        ));
        assert!(cache.insert(
            drop,
            Arc::<[u8]>::from(vec![8u8; 32]),
            32,
            CachePriority::Normal
        ));

        let workers = 6usize;
        let start = Arc::new(Barrier::new(workers + 1));
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let cache = Arc::clone(&cache);
            let start = Arc::clone(&start);
            handles.push(thread::spawn(move || {
                start.wait();
                for i in 0..500 {
                    let _ = cache.get(&keep);
                    let _ = cache.get(&drop);
                    let k = key(3, 200 + worker as u64 + 1, i as u64, BlockKind::Data);
                    let _ = cache.insert(
                        k,
                        Arc::<[u8]>::from(vec![worker as u8; 24]),
                        24,
                        CachePriority::Normal,
                    );
                }
            }));
        }

        start.wait();
        cache.remove_sst(4, 300);

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(cache.get(&keep).is_some());
        assert!(cache.get(&drop).is_none());
    }
}
