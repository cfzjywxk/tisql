use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::RwLock;

use crate::error::Result;
use crate::types::{Key, RawValue, Timestamp};

use super::{Snapshot, StorageEngine, WriteBatch, WriteOp};

/// Simple in-memory storage engine using BTreeMap
pub struct MemTableEngine {
    data: RwLock<BTreeMap<Key, RawValue>>,
}

impl MemTableEngine {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
        }
    }
}

impl Default for MemTableEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageEngine for MemTableEngine {
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>> {
        let data = self.data.read().unwrap();
        Ok(data.get(key).cloned())
    }

    fn get_at(&self, key: &[u8], _ts: Timestamp) -> Result<Option<RawValue>> {
        // Simple memtable doesn't support MVCC - just return latest
        self.get(key)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        let mut data = self.data.write().unwrap();
        data.remove(key);
        Ok(())
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        let mut data = self.data.write().unwrap();
        for op in batch.into_ops() {
            match op {
                WriteOp::Put { key, value } => {
                    data.insert(key, value);
                }
                WriteOp::Delete { key } => {
                    data.remove(&key);
                }
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<Box<dyn Snapshot>> {
        let data = self.data.read().unwrap();
        Ok(Box::new(MemTableSnapshot { data: data.clone() }))
    }

    fn scan(&self, range: Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        let data = self.data.read().unwrap();
        // Clone data for iteration to avoid holding lock
        let items: Vec<_> = data
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(Box::new(items.into_iter()))
    }
}

/// Snapshot of MemTable at a point in time
pub struct MemTableSnapshot {
    data: BTreeMap<Key, RawValue>,
}

impl Snapshot for MemTableSnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>> {
        Ok(self.data.get(key).cloned())
    }

    fn scan(&self, range: Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        let items: Vec<_> = self
            .data
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        Ok(Box::new(items.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let engine = MemTableEngine::new();

        // Put and get
        engine.put(b"key1", b"value1").unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value1".to_vec()));

        // Update
        engine.put(b"key1", b"value2").unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value2".to_vec()));

        // Delete
        engine.delete(b"key1").unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), None);
    }

    #[test]
    fn test_write_batch() {
        let engine = MemTableEngine::new();

        let mut batch = WriteBatch::new();
        batch.put(b"k1".to_vec(), b"v1".to_vec());
        batch.put(b"k2".to_vec(), b"v2".to_vec());
        batch.put(b"k3".to_vec(), b"v3".to_vec());

        engine.write_batch(batch).unwrap();

        assert_eq!(engine.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(engine.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(engine.get(b"k3").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_scan() {
        let engine = MemTableEngine::new();

        engine.put(b"a", b"1").unwrap();
        engine.put(b"b", b"2").unwrap();
        engine.put(b"c", b"3").unwrap();
        engine.put(b"d", b"4").unwrap();

        let results: Vec<_> = engine.scan(b"b".to_vec()..b"d".to_vec()).unwrap().collect();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(results[1], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_snapshot() {
        let engine = MemTableEngine::new();

        engine.put(b"key", b"v1").unwrap();
        let snapshot = engine.snapshot().unwrap();

        // Modify after snapshot
        engine.put(b"key", b"v2").unwrap();

        // Snapshot should still see old value
        assert_eq!(snapshot.get(b"key").unwrap(), Some(b"v1".to_vec()));
        // Engine should see new value
        assert_eq!(engine.get(b"key").unwrap(), Some(b"v2".to_vec()));
    }
}
