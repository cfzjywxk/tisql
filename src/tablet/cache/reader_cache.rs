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

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use hashlink::LinkedHashMap;
use parking_lot::Mutex;

use super::key::ReaderCacheKey;

#[derive(Debug, Default, Clone, Copy)]
pub struct ReaderCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub evictions: u64,
}

struct ReaderCacheInner {
    map: LinkedHashMap<ReaderCacheKey, Arc<dyn Any + Send + Sync>>,
    max_entries: usize,
}

/// Entry-count-capped reader cache.
pub struct ReaderCache {
    inner: Mutex<ReaderCacheInner>,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
}

impl ReaderCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Mutex::new(ReaderCacheInner {
                map: LinkedHashMap::new(),
                max_entries,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    pub fn get_typed<T: Any + Send + Sync>(&self, key: &ReaderCacheKey) -> Option<Arc<T>> {
        let mut inner = self.inner.lock();
        let value = inner.map.to_back(key).cloned();
        match value {
            Some(value) => match Arc::downcast::<T>(value) {
                Ok(typed) => {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                    Some(typed)
                }
                Err(_) => {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    None
                }
            },
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    pub fn insert_typed<T: Any + Send + Sync>(&self, key: ReaderCacheKey, value: Arc<T>) {
        let mut inner = self.inner.lock();
        if inner.max_entries == 0 {
            return;
        }

        inner.map.remove(&key);
        inner.map.insert(key, value as Arc<dyn Any + Send + Sync>);
        self.inserts.fetch_add(1, Ordering::Relaxed);

        while inner.map.len() > inner.max_entries {
            let Some((_, _)) = inner.map.pop_front() else {
                break;
            };
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn remove(&self, key: &ReaderCacheKey) {
        let mut inner = self.inner.lock();
        inner.map.remove(key);
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::cache::ReaderCacheKey;

    #[test]
    fn test_reader_cache_typed_get_insert() {
        let cache = ReaderCache::new(8);
        let key = ReaderCacheKey { ns: 11, sst_id: 9 };

        assert!(cache.get_typed::<u64>(&key).is_none());
        cache.insert_typed(key, Arc::new(42u64));

        assert_eq!(*cache.get_typed::<u64>(&key).unwrap(), 42);
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.inserts, 1);
    }

    #[test]
    fn test_reader_cache_type_mismatch_is_miss() {
        let cache = ReaderCache::new(8);
        let key = ReaderCacheKey { ns: 1, sst_id: 2 };
        cache.insert_typed(key, Arc::new(String::from("reader")));

        assert!(cache.get_typed::<u64>(&key).is_none());
        // Entry remains available for the correct type.
        assert_eq!(cache.get_typed::<String>(&key).unwrap().as_str(), "reader");
    }

    #[test]
    fn test_reader_cache_entry_cap_and_remove_sst() {
        let cache = ReaderCache::new(2);
        let k1 = ReaderCacheKey { ns: 1, sst_id: 1 };
        let k2 = ReaderCacheKey { ns: 1, sst_id: 2 };
        let k3 = ReaderCacheKey { ns: 1, sst_id: 3 };

        cache.insert_typed(k1, Arc::new(10u64));
        cache.insert_typed(k2, Arc::new(20u64));
        cache.insert_typed(k3, Arc::new(30u64));

        // One entry should be evicted due to cap=2.
        let present = [
            cache.get_typed::<u64>(&k1).is_some() as u8,
            cache.get_typed::<u64>(&k2).is_some() as u8,
            cache.get_typed::<u64>(&k3).is_some() as u8,
        ]
        .into_iter()
        .sum::<u8>();
        assert_eq!(present, 2);
        assert!(cache.stats().evictions >= 1);

        cache.remove_sst(1, 3);
        assert!(cache.get_typed::<u64>(&k3).is_none());
    }

    #[test]
    fn test_reader_cache_remove_sst_keeps_inflight_arc_alive() {
        let cache = ReaderCache::new(8);
        let key = ReaderCacheKey { ns: 9, sst_id: 99 };
        cache.insert_typed(key, Arc::new(String::from("reader")));

        let inflight = cache.get_typed::<String>(&key).unwrap();
        cache.remove_sst(9, 99);

        assert!(cache.get_typed::<String>(&key).is_none());
        assert_eq!(inflight.as_str(), "reader");
    }
}
