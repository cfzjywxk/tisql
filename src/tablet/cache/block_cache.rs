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

use std::collections::{HashMap, VecDeque};
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use super::key::{BlockCacheKey, CachePriority};

/// Cache value type for block payloads.
pub type BlockCacheValue = Arc<[u8]>;

#[derive(Debug, Default, Clone, Copy)]
pub struct BlockCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub insert_rejects: u64,
}

struct BlockEntry {
    value: BlockCacheValue,
    charge: usize,
    priority: CachePriority,
}

struct BlockShard {
    map: HashMap<BlockCacheKey, BlockEntry>,
    lru: VecDeque<BlockCacheKey>,
    usage: usize,
    capacity: usize,
}

impl BlockShard {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            lru: VecDeque::new(),
            usage: 0,
            capacity,
        }
    }

    fn touch(&mut self, key: BlockCacheKey) {
        self.lru.retain(|k| *k != key);
        self.lru.push_back(key);
    }

    fn get(&mut self, key: &BlockCacheKey) -> Option<BlockCacheValue> {
        let value = self.map.get(key).map(|entry| Arc::clone(&entry.value))?;
        self.touch(*key);
        Some(value)
    }

    fn evict_one_normal_first(&mut self) -> bool {
        if self.map.is_empty() {
            return false;
        }

        let normal_victim = self.lru.iter().copied().find(|k| {
            self.map
                .get(k)
                .is_some_and(|entry| entry.priority == CachePriority::Normal)
        });
        let victim = normal_victim.or_else(|| self.lru.front().copied());
        let Some(victim) = victim else {
            return false;
        };

        self.lru.retain(|k| *k != victim);
        if let Some(entry) = self.map.remove(&victim) {
            self.usage = self.usage.saturating_sub(entry.charge);
            return true;
        }
        false
    }

    fn insert(
        &mut self,
        key: BlockCacheKey,
        value: BlockCacheValue,
        charge: usize,
        priority: CachePriority,
    ) -> (bool, u64) {
        if charge > self.capacity {
            return (false, 0);
        }

        if let Some(old) = self.map.remove(&key) {
            self.usage = self.usage.saturating_sub(old.charge);
            self.lru.retain(|k| *k != key);
        }

        let mut evicted = 0;
        while self.usage + charge > self.capacity {
            if !self.evict_one_normal_first() {
                return (false, evicted);
            }
            evicted += 1;
        }

        self.map.insert(
            key,
            BlockEntry {
                value,
                charge,
                priority,
            },
        );
        self.touch(key);
        self.usage += charge;
        (true, evicted)
    }

    fn remove_by_sst(&mut self, ns: u128, sst_id: u64) {
        let victims: Vec<BlockCacheKey> = self
            .map
            .keys()
            .copied()
            .filter(|k| k.ns == ns && k.sst_id == sst_id)
            .collect();
        for key in victims {
            if let Some(entry) = self.map.remove(&key) {
                self.usage = self.usage.saturating_sub(entry.charge);
            }
            self.lru.retain(|k| *k != key);
        }
    }
}

/// Process-wide shared block cache (sharded strict-cap LRU).
pub struct SharedBlockCache {
    shards: Vec<Mutex<BlockShard>>,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
    insert_rejects: AtomicU64,
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
            insert_rejects: AtomicU64::new(0),
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
                Some(v)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
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
        let (ok, evicted) = shard.insert(key, value, charge, priority);
        if ok {
            self.inserts.fetch_add(1, Ordering::Relaxed);
            if evicted > 0 {
                self.evictions.fetch_add(evicted, Ordering::Relaxed);
            }
        } else {
            self.insert_rejects.fetch_add(1, Ordering::Relaxed);
        }
        ok
    }

    pub fn remove_sst(&self, ns: u128, sst_id: u64) {
        for shard in &self.shards {
            shard.lock().remove_by_sst(ns, sst_id);
        }
    }

    pub fn stats(&self) -> BlockCacheStats {
        BlockCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            insert_rejects: self.insert_rejects.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::cache::{BlockCacheKey, BlockKind, CachePriority};

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
}
