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

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use hashlink::LinkedHashMap;
use parking_lot::Mutex;

use super::key::{RowCacheKey, TabletCacheNs};

#[derive(Debug, Default, Clone, Copy)]
pub struct RowCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub insert_rejects: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RowCacheNamespaceUsage {
    pub ns: TabletCacheNs,
    pub usage_bytes: u64,
    pub entries: u64,
}

struct RowEntry {
    value: Option<Vec<u8>>,
    charge: usize,
}

struct RowCacheInner {
    map: LinkedHashMap<RowCacheKey, RowEntry>,
    // Secondary index: ns -> user_key -> cached snapshot read_ts set.
    // This avoids O(n) full-cache scans on committed-write invalidation.
    by_user_key: HashMap<u128, HashMap<Vec<u8>, HashSet<u64>>>,
    usage_bytes: usize,
    capacity_bytes: usize,
}

#[derive(Default)]
struct RowNamespaceSlot {
    usage_bytes: AtomicU64,
    entries: AtomicU64,
}

/// Snapshot row cache keyed by `(tablet_ns, user_key, read_ts)`.
pub struct RowCache {
    inner: Mutex<RowCacheInner>,
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
    namespace_slots: DashMap<TabletCacheNs, Arc<RowNamespaceSlot>>,
}

impl RowCache {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(RowCacheInner {
                map: LinkedHashMap::new(),
                by_user_key: HashMap::new(),
                usage_bytes: 0,
                capacity_bytes,
            }),
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

    pub fn register_namespace(&self, ns: TabletCacheNs) {
        let _ = self.slot_for_ns(ns);
    }

    pub fn unregister_namespace(&self, ns: TabletCacheNs) {
        self.namespace_slots.remove(&ns);
    }

    fn slot_for_ns(&self, ns: TabletCacheNs) -> Arc<RowNamespaceSlot> {
        if let Some(slot) = self.namespace_slots.get(&ns) {
            return Arc::clone(slot.value());
        }
        let slot = Arc::new(RowNamespaceSlot::default());
        Arc::clone(
            self.namespace_slots
                .entry(ns)
                .or_insert_with(|| Arc::clone(&slot))
                .value(),
        )
    }

    fn account_add(&self, ns: TabletCacheNs, charge: usize) {
        let slot = self.slot_for_ns(ns);
        slot.entries.fetch_add(1, Ordering::Relaxed);
        slot.usage_bytes.fetch_add(charge as u64, Ordering::Relaxed);
    }

    fn account_remove(&self, ns: TabletCacheNs, charge: usize) {
        let slot = self.slot_for_ns(ns);
        let dec = |counter: &AtomicU64, delta: u64| {
            let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(delta))
            });
        };
        dec(&slot.entries, 1);
        dec(&slot.usage_bytes, charge as u64);
    }

    fn charge_for_value(key: &RowCacheKey, value: &Option<Vec<u8>>) -> usize {
        let base = key.user_key.len() + std::mem::size_of::<u64>() + std::mem::size_of::<u128>();
        base + value.as_ref().map_or(0, |v| v.len())
    }

    fn index_insert(inner: &mut RowCacheInner, key: &RowCacheKey) {
        inner
            .by_user_key
            .entry(key.ns)
            .or_default()
            .entry(key.user_key.clone())
            .or_default()
            .insert(key.read_ts);
    }

    fn index_remove(inner: &mut RowCacheInner, key: &RowCacheKey) {
        let mut remove_ns = false;
        if let Some(by_key) = inner.by_user_key.get_mut(&key.ns) {
            let mut remove_user_key = false;
            if let Some(read_ts_set) = by_key.get_mut(key.user_key.as_slice()) {
                read_ts_set.remove(&key.read_ts);
                remove_user_key = read_ts_set.is_empty();
            }
            if remove_user_key {
                by_key.remove(key.user_key.as_slice());
            }
            remove_ns = by_key.is_empty();
        }
        if remove_ns {
            inner.by_user_key.remove(&key.ns);
        }
    }

    fn take_index_entry_for_user_key(
        inner: &mut RowCacheInner,
        ns: u128,
        user_key: &[u8],
    ) -> Option<HashSet<u64>> {
        let (read_ts_set, remove_ns) = {
            let by_key = inner.by_user_key.get_mut(&ns)?;
            let read_ts_set = by_key.remove(user_key)?;
            (read_ts_set, by_key.is_empty())
        };
        if remove_ns {
            inner.by_user_key.remove(&ns);
        }
        Some(read_ts_set)
    }

    pub fn get(&self, key: &RowCacheKey) -> Option<Option<Vec<u8>>> {
        let mut inner = self.inner.lock();
        let value = inner.map.to_back(key).map(|entry| entry.value.clone());
        match value {
            Some(value) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                self.hits_delta.fetch_add(1, Ordering::Relaxed);
                Some(value)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                self.misses_delta.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn insert(&self, key: RowCacheKey, value: Option<Vec<u8>>) -> bool {
        let mut inner = self.inner.lock();
        let charge = Self::charge_for_value(&key, &value);
        if charge > inner.capacity_bytes {
            self.insert_rejects.fetch_add(1, Ordering::Relaxed);
            self.insert_rejects_delta.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        if let Some(old) = inner.map.remove(&key) {
            inner.usage_bytes = inner.usage_bytes.saturating_sub(old.charge);
            Self::index_remove(&mut inner, &key);
            self.account_remove(key.ns, old.charge);
        }

        while inner.usage_bytes + charge > inner.capacity_bytes {
            let Some((victim, old)) = inner.map.pop_front() else {
                return false;
            };
            inner.usage_bytes = inner.usage_bytes.saturating_sub(old.charge);
            Self::index_remove(&mut inner, &victim);
            self.evictions.fetch_add(1, Ordering::Relaxed);
            self.evictions_delta.fetch_add(1, Ordering::Relaxed);
            self.account_remove(victim.ns, old.charge);
        }

        Self::index_insert(&mut inner, &key);
        inner.map.insert(key.clone(), RowEntry { value, charge });
        inner.usage_bytes += charge;
        self.inserts.fetch_add(1, Ordering::Relaxed);
        self.inserts_delta.fetch_add(1, Ordering::Relaxed);
        self.account_add(key.ns, charge);
        true
    }

    /// Invalidate all snapshot cache entries for one logical user key.
    pub fn invalidate_key(&self, ns: u128, user_key: &[u8]) {
        let mut inner = self.inner.lock();
        let Some(read_ts_set) = Self::take_index_entry_for_user_key(&mut inner, ns, user_key)
        else {
            return;
        };

        let mut victim = RowCacheKey {
            ns,
            user_key: user_key.to_vec(),
            read_ts: 0,
        };
        for read_ts in read_ts_set {
            victim.read_ts = read_ts;
            if let Some(old) = inner.map.remove(&victim) {
                inner.usage_bytes = inner.usage_bytes.saturating_sub(old.charge);
                self.invalidations.fetch_add(1, Ordering::Relaxed);
                self.invalidations_delta.fetch_add(1, Ordering::Relaxed);
                self.account_remove(ns, old.charge);
            }
        }
    }

    pub fn stats(&self) -> RowCacheStats {
        RowCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            inserts: self.inserts.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            insert_rejects: self.insert_rejects.load(Ordering::Relaxed),
        }
    }

    pub fn snapshot_and_reset_delta(&self) -> RowCacheStats {
        RowCacheStats {
            hits: self.hits_delta.swap(0, Ordering::Relaxed),
            misses: self.misses_delta.swap(0, Ordering::Relaxed),
            inserts: self.inserts_delta.swap(0, Ordering::Relaxed),
            evictions: self.evictions_delta.swap(0, Ordering::Relaxed),
            invalidations: self.invalidations_delta.swap(0, Ordering::Relaxed),
            insert_rejects: self.insert_rejects_delta.swap(0, Ordering::Relaxed),
        }
    }

    pub fn namespace_usage_snapshot(&self) -> Vec<RowCacheNamespaceUsage> {
        let mut rows: Vec<_> = self
            .namespace_slots
            .iter()
            .map(|entry| RowCacheNamespaceUsage {
                ns: *entry.key(),
                usage_bytes: entry.value().usage_bytes.load(Ordering::Relaxed),
                entries: entry.value().entries.load(Ordering::Relaxed),
            })
            .collect();
        rows.sort_by_key(|row| row.ns);
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::cache::RowCacheKey;

    fn key(ns: u128, user_key: &[u8], read_ts: u64) -> RowCacheKey {
        RowCacheKey {
            ns,
            user_key: user_key.to_vec(),
            read_ts,
        }
    }

    #[test]
    fn test_row_cache_insert_get_and_negative_entry() {
        let cache = RowCache::new(1024);
        let k1 = key(1, b"k1", 10);
        let k2 = key(1, b"k2", 10);

        assert!(cache.get(&k1).is_none());
        assert!(cache.insert(k1.clone(), Some(b"v1".to_vec())));
        assert!(cache.insert(k2.clone(), None));

        assert_eq!(cache.get(&k1), Some(Some(b"v1".to_vec())));
        assert_eq!(cache.get(&k2), Some(None));
        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_row_cache_invalidate_key_removes_all_snapshots() {
        let cache = RowCache::new(2048);
        let k1 = key(5, b"user", 10);
        let k2 = key(5, b"user", 20);
        let k3 = key(5, b"other", 20);

        assert!(cache.insert(k1.clone(), Some(b"v10".to_vec())));
        assert!(cache.insert(k2.clone(), Some(b"v20".to_vec())));
        assert!(cache.insert(k3.clone(), Some(b"x".to_vec())));

        cache.invalidate_key(5, b"user");
        assert!(cache.get(&k1).is_none());
        assert!(cache.get(&k2).is_none());
        assert_eq!(cache.get(&k3), Some(Some(b"x".to_vec())));
        assert_eq!(cache.stats().invalidations, 2);
    }

    #[test]
    fn test_row_cache_capacity_evicts_lru() {
        let cache = RowCache::new(110);
        let k1 = key(1, b"a", 1);
        let k2 = key(1, b"b", 1);
        let k3 = key(1, b"c", 1);

        assert!(cache.insert(k1.clone(), Some(vec![1u8; 16])));
        assert!(cache.insert(k2.clone(), Some(vec![2u8; 16])));

        // Touch k1 to make k2 the LRU.
        assert!(cache.get(&k1).is_some());
        assert!(cache.insert(k3.clone(), Some(vec![3u8; 16])));

        assert!(cache.get(&k1).is_some());
        assert!(cache.get(&k2).is_none());
        assert!(cache.get(&k3).is_some());
        assert!(cache.stats().evictions >= 1);
    }

    #[test]
    fn test_row_cache_invalidate_key_after_eviction_keeps_index_consistent() {
        let cache = RowCache::new(128);
        let victim = key(9, b"user", 10);
        let survivor = key(9, b"user", 20);
        let other = key(9, b"other", 30);

        assert!(cache.insert(victim.clone(), Some(vec![1u8; 16])));
        assert!(cache.insert(survivor.clone(), Some(vec![2u8; 16])));

        // Trigger one eviction (oldest = victim).
        assert!(cache.insert(other.clone(), Some(vec![3u8; 16])));
        assert!(cache.get(&victim).is_none());

        cache.invalidate_key(9, b"user");
        assert!(cache.get(&survivor).is_none());
        assert_eq!(cache.get(&other), Some(Some(vec![3u8; 16])));
        assert_eq!(cache.stats().invalidations, 1);
    }
}
