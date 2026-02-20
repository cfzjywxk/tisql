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

//! Tablet-routed storage adapter.
//!
//! This wrapper keeps TransactionService on one storage trait object while
//! dispatching each operation to the target tablet engine.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

#[cfg(feature = "failpoints")]
use fail::fail_point;

use crate::catalog::types::{Key, RawValue, Timestamp};
use crate::util::codec::key::{decode_index_key, decode_table_id, is_index_key, is_record_key};
use crate::util::error::Result;

use super::mvcc::MvccKey;
use super::router::{is_system_table_id, route_index_to_tablet, route_table_to_tablet, TabletId};
use super::{
    PessimisticStorage, PessimisticWriteError, StorageEngine, TabletEngine, TabletManager,
    TieredMergeIterator, WriteBatch, WriteOp,
};

/// Storage facade that routes all operations through `TabletManager`.
///
/// Preferred routing is metadata-first (`*_on_tablet` trait methods).
/// Key-only trait methods still exist for legacy/internal callers and use
/// conservative key decoding fallback. If a logical tablet is not mounted yet,
/// operations are routed to `TabletId::System`.
#[derive(Clone)]
pub struct RoutedTabletStorage {
    manager: Arc<TabletManager>,
}

impl RoutedTabletStorage {
    pub fn new(manager: Arc<TabletManager>) -> Self {
        Self { manager }
    }

    pub fn tablet_manager(&self) -> &Arc<TabletManager> {
        &self.manager
    }

    fn resolve_mounted_tablet(&self, tablet_id: TabletId) -> (TabletId, Arc<TabletEngine>) {
        if let Some(tablet) = self.manager.get_tablet(tablet_id) {
            (tablet_id, tablet)
        } else {
            (TabletId::System, self.manager.system_tablet())
        }
    }

    fn resolve_key_tablet(&self, key: &[u8]) -> (TabletId, Arc<TabletEngine>) {
        // Legacy/internal key-only fallback path.
        let logical = infer_logical_tablet_from_encoded_key(key);
        self.resolve_mounted_tablet(logical)
    }

    fn resolve_scan_tablet(&self, range: &Range<MvccKey>) -> (TabletId, Arc<TabletEngine>) {
        if range.start.is_unbounded() {
            return self.resolve_mounted_tablet(TabletId::System);
        }

        let start_key = range.start.key();
        let logical = match decode_table_id(start_key) {
            Ok(table_id) => {
                if is_index_key(start_key) {
                    match decode_index_key(start_key) {
                        Ok((decoded_table_id, index_id, _)) if decoded_table_id == table_id => {
                            route_index_to_tablet(table_id, index_id)
                        }
                        _ => route_table_to_tablet(table_id),
                    }
                } else {
                    route_table_to_tablet(table_id)
                }
            }
            Err(_) => infer_logical_tablet_from_encoded_key(start_key),
        };
        self.resolve_mounted_tablet(logical)
    }

    fn split_write_batch_by_tablet(&self, batch: WriteBatch) -> BTreeMap<TabletId, WriteBatch> {
        let commit_ts = batch.commit_ts();
        let clog_lsn = batch.clog_lsn();
        let mut groups: BTreeMap<TabletId, WriteBatch> = BTreeMap::new();

        for (key, op) in batch.into_iter() {
            let (tablet_id, _) = self.resolve_key_tablet(&key);
            let tablet_batch = groups.entry(tablet_id).or_default();
            match op {
                WriteOp::Put { value } => tablet_batch.put(key, value),
                WriteOp::Delete => tablet_batch.delete(key),
            }
        }

        for tablet_batch in groups.values_mut() {
            if let Some(ts) = commit_ts {
                tablet_batch.set_commit_ts(ts);
            }
            if let Some(lsn) = clog_lsn {
                tablet_batch.set_clog_lsn(lsn);
            }
        }

        groups
    }

    fn group_keys_by_mounted_tablet(&self, keys: &[Key]) -> BTreeMap<TabletId, Vec<Key>> {
        let mut grouped = BTreeMap::new();
        for key in keys {
            let (tablet_id, _) = self.resolve_key_tablet(key);
            grouped
                .entry(tablet_id)
                .or_insert_with(Vec::new)
                .push(key.clone());
        }
        grouped
    }
}

fn infer_logical_tablet_from_encoded_key(key: &[u8]) -> TabletId {
    let table_id = match decode_table_id(key) {
        Ok(id) => id,
        Err(_) => return TabletId::System,
    };

    if is_system_table_id(table_id) {
        return TabletId::System;
    }

    if is_record_key(key) || has_record_prefix(key) {
        return route_table_to_tablet(table_id);
    }

    if is_index_key(key) {
        return match decode_index_key(key) {
            Ok((decoded_table_id, index_id, _)) if decoded_table_id == table_id => {
                route_index_to_tablet(table_id, index_id)
            }
            _ => TabletId::System,
        };
    }

    TabletId::System
}

#[inline]
fn has_record_prefix(key: &[u8]) -> bool {
    key.len() >= 11 && &key[9..11] == b"_r"
}

impl StorageEngine for RoutedTabletStorage {
    type Iter = TieredMergeIterator;

    fn scan_iter(&self, range: Range<MvccKey>, owner_ts: Timestamp) -> Result<Self::Iter> {
        let (_, tablet) = self.resolve_scan_tablet(&range);
        tablet.scan_iter(range, owner_ts)
    }

    fn scan_iter_on_tablet(
        &self,
        tablet_id: TabletId,
        range: Range<MvccKey>,
        owner_ts: Timestamp,
    ) -> Result<Self::Iter> {
        let (_, tablet) = self.resolve_mounted_tablet(tablet_id);
        tablet.scan_iter(range, owner_ts)
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return self.manager.system_tablet().write_batch(batch);
        }

        let grouped = self.split_write_batch_by_tablet(batch);
        for (tablet_id, tablet_batch) in grouped {
            let (_, tablet) = self.resolve_mounted_tablet(tablet_id);
            tablet.write_batch(tablet_batch)?;
        }
        Ok(())
    }
}

impl PessimisticStorage for RoutedTabletStorage {
    fn put_pending(
        &self,
        key: &[u8],
        value: RawValue,
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), PessimisticWriteError> {
        let (_, tablet) = self.resolve_key_tablet(key);
        tablet.put_pending(key, value, owner_start_ts)
    }

    fn put_pending_ref(
        &self,
        key: &[u8],
        value: &[u8],
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), PessimisticWriteError> {
        let (_, tablet) = self.resolve_key_tablet(key);
        tablet.put_pending_ref(key, value, owner_start_ts)
    }

    fn put_pending_on_tablet(
        &self,
        tablet_id: TabletId,
        key: &[u8],
        value: RawValue,
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), PessimisticWriteError> {
        let (_, tablet) = self.resolve_mounted_tablet(tablet_id);
        tablet.put_pending(key, value, owner_start_ts)
    }

    fn put_pending_on_tablet_ref(
        &self,
        tablet_id: TabletId,
        key: &[u8],
        value: &[u8],
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), PessimisticWriteError> {
        let (_, tablet) = self.resolve_mounted_tablet(tablet_id);
        tablet.put_pending_ref(key, value, owner_start_ts)
    }

    fn get_lock_owner(&self, key: &[u8]) -> Option<Timestamp> {
        let (_, tablet) = self.resolve_key_tablet(key);
        tablet.get_lock_owner(key)
    }

    fn get_lock_owner_on_tablet(&self, tablet_id: TabletId, key: &[u8]) -> Option<Timestamp> {
        let (_, tablet) = self.resolve_mounted_tablet(tablet_id);
        tablet.get_lock_owner(key)
    }

    fn alloc_and_reserve_commit_lsn(&self, owner_start_ts: Timestamp) -> u64 {
        self.manager.alloc_and_reserve_commit_lsn(owner_start_ts)
    }

    fn release_commit_lsn(&self, owner_start_ts: Timestamp) -> Option<u64> {
        self.manager.release_commit_lsn(owner_start_ts)
    }

    fn is_commit_lsn_reserved(&self, owner_start_ts: Timestamp, lsn: u64) -> bool {
        self.manager.is_commit_lsn_reserved(owner_start_ts, lsn)
    }

    fn finalize_pending(&self, keys: &[Key], owner_start_ts: Timestamp, commit_ts: Timestamp) {
        self.finalize_pending_with_lsn(keys, owner_start_ts, commit_ts, 0)
    }

    fn finalize_pending_with_lsn(
        &self,
        keys: &[Key],
        owner_start_ts: Timestamp,
        commit_ts: Timestamp,
        clog_lsn: u64,
    ) {
        let grouped = self.group_keys_by_mounted_tablet(keys);
        #[cfg(feature = "failpoints")]
        let mut failpoint_fired = false;
        for (tablet_id, tablet_keys) in grouped {
            let (_, tablet) = self.resolve_mounted_tablet(tablet_id);
            tablet.finalize_pending_with_lsn(&tablet_keys, owner_start_ts, commit_ts, clog_lsn);
            #[cfg(feature = "failpoints")]
            if !failpoint_fired {
                fail_point!("tablet_routed_finalize_after_first_tablet");
                failpoint_fired = true;
            }
        }
    }

    fn finalize_pending_grouped_with_lsn(
        &self,
        tablet_groups: &[(TabletId, Vec<Key>)],
        owner_start_ts: Timestamp,
        commit_ts: Timestamp,
        clog_lsn: u64,
    ) {
        #[cfg(feature = "failpoints")]
        let mut failpoint_fired = false;
        for (tablet_id, tablet_keys) in tablet_groups {
            let (_, tablet) = self.resolve_mounted_tablet(*tablet_id);
            tablet.finalize_pending_with_lsn(tablet_keys, owner_start_ts, commit_ts, clog_lsn);
            #[cfg(feature = "failpoints")]
            if !failpoint_fired {
                fail_point!("tablet_routed_finalize_after_first_tablet");
                failpoint_fired = true;
            }
        }
    }

    fn abort_pending(&self, keys: &[Key], owner_start_ts: Timestamp) {
        let grouped = self.group_keys_by_mounted_tablet(keys);
        #[cfg(feature = "failpoints")]
        let mut failpoint_fired = false;
        for (tablet_id, tablet_keys) in grouped {
            let (_, tablet) = self.resolve_mounted_tablet(tablet_id);
            tablet.abort_pending(&tablet_keys, owner_start_ts);
            #[cfg(feature = "failpoints")]
            if !failpoint_fired {
                fail_point!("tablet_routed_abort_after_first_tablet");
                failpoint_fired = true;
            }
        }
    }

    fn abort_pending_grouped(
        &self,
        tablet_groups: &[(TabletId, Vec<Key>)],
        owner_start_ts: Timestamp,
    ) {
        #[cfg(feature = "failpoints")]
        let mut failpoint_fired = false;
        for (tablet_id, tablet_keys) in tablet_groups {
            let (_, tablet) = self.resolve_mounted_tablet(*tablet_id);
            tablet.abort_pending(tablet_keys, owner_start_ts);
            #[cfg(feature = "failpoints")]
            if !failpoint_fired {
                fail_point!("tablet_routed_abort_after_first_tablet");
                failpoint_fired = true;
            }
        }
    }

    fn delete_pending(
        &self,
        key: &[u8],
        owner_start_ts: Timestamp,
    ) -> std::result::Result<bool, PessimisticWriteError> {
        let (_, tablet) = self.resolve_key_tablet(key);
        tablet.delete_pending(key, owner_start_ts)
    }

    fn delete_pending_on_tablet(
        &self,
        tablet_id: TabletId,
        key: &[u8],
        owner_start_ts: Timestamp,
    ) -> std::result::Result<bool, PessimisticWriteError> {
        let (_, tablet) = self.resolve_mounted_tablet(tablet_id);
        tablet.delete_pending(key, owner_start_ts)
    }

    fn get_with_owner<'a>(
        &'a self,
        key: &'a [u8],
        read_ts: Timestamp,
        owner_start_ts: Timestamp,
    ) -> impl std::future::Future<Output = Option<RawValue>> + Send + 'a {
        let (_, tablet) = self.resolve_key_tablet(key);
        async move { tablet.get_with_owner(key, read_ts, owner_start_ts).await }
    }

    fn get_with_owner_on_tablet<'a>(
        &'a self,
        tablet_id: TabletId,
        key: &'a [u8],
        read_ts: Timestamp,
        owner_start_ts: Timestamp,
    ) -> impl std::future::Future<Output = Option<RawValue>> + Send + 'a {
        let (_, tablet) = self.resolve_mounted_tablet(tablet_id);
        async move { tablet.get_with_owner(key, read_ts, owner_start_ts).await }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inner_table::core_tables::{ALL_TABLE_TABLE_ID, USER_TABLE_ID_START};
    use crate::lsn::new_lsn_provider;
    use crate::tablet::MvccIterator;
    use crate::tablet::{encode_key, LsmConfig};
    use crate::util::codec::key::{encode_index_seek_key, encode_record_key_with_handle};
    use tempfile::TempDir;

    fn open_tablet(dir: &std::path::Path) -> Arc<TabletEngine> {
        Arc::new(TabletEngine::open(LsmConfig::new(dir)).unwrap())
    }

    fn read_value(tablet: &TabletEngine, key: &[u8], read_ts: Timestamp) -> Option<RawValue> {
        crate::io::block_on_sync(tablet.get_with_owner(key, read_ts, 0))
    }

    #[test]
    fn test_write_batch_routes_to_mounted_tablets() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let manager = Arc::new(
            TabletManager::new(dir.path(), new_lsn_provider(), Arc::clone(&system)).unwrap(),
        );

        let user_tablet_id = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let user = open_tablet(&manager.tablet_dir(user_tablet_id));
        manager
            .insert_tablet(user_tablet_id, Arc::clone(&user))
            .unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let system_key = encode_record_key_with_handle(ALL_TABLE_TABLE_ID, 1);
        let user_key = encode_record_key_with_handle(USER_TABLE_ID_START, 1);

        let mut batch = WriteBatch::new();
        batch.put(system_key.clone(), b"system_v".to_vec());
        batch.put(user_key.clone(), b"user_v".to_vec());
        batch.set_commit_ts(42);
        storage.write_batch(batch).unwrap();

        assert_eq!(
            read_value(&system, &system_key, 100),
            Some(b"system_v".to_vec())
        );
        assert_eq!(read_value(&system, &user_key, 100), None);

        assert_eq!(read_value(&user, &user_key, 100), Some(b"user_v".to_vec()));
        assert_eq!(read_value(&user, &system_key, 100), None);
    }

    #[test]
    fn test_write_batch_falls_back_to_system_for_unmounted_tablet() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let manager = Arc::new(
            TabletManager::new(dir.path(), new_lsn_provider(), Arc::clone(&system)).unwrap(),
        );
        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let user_key = encode_record_key_with_handle(USER_TABLE_ID_START, 9);
        let mut batch = WriteBatch::new();
        batch.put(user_key.clone(), b"fallback_system".to_vec());
        batch.set_commit_ts(55);
        storage.write_batch(batch).unwrap();

        assert_eq!(
            read_value(&system, &user_key, 100),
            Some(b"fallback_system".to_vec())
        );
    }

    #[test]
    fn test_split_batch_preserves_commit_ts_and_clog_lsn() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let manager = Arc::new(
            TabletManager::new(dir.path(), new_lsn_provider(), Arc::clone(&system)).unwrap(),
        );
        let user_tablet_id = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let user = open_tablet(&manager.tablet_dir(user_tablet_id));
        manager.insert_tablet(user_tablet_id, user).unwrap();
        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let mut batch = WriteBatch::new();
        batch.put(
            encode_record_key_with_handle(ALL_TABLE_TABLE_ID, 1),
            b"sys".to_vec(),
        );
        batch.put(
            encode_record_key_with_handle(USER_TABLE_ID_START, 1),
            b"user".to_vec(),
        );
        batch.set_commit_ts(123);
        batch.set_clog_lsn(456);

        let groups = storage.split_write_batch_by_tablet(batch);
        assert_eq!(groups.len(), 2);
        for group in groups.values() {
            assert_eq!(group.commit_ts(), Some(123));
            assert_eq!(group.clog_lsn(), Some(456));
        }
    }

    #[test]
    fn test_metadata_put_pending_bypasses_key_decode() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let manager = Arc::new(
            TabletManager::new(dir.path(), new_lsn_provider(), Arc::clone(&system)).unwrap(),
        );
        let user_tablet_id = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let user = open_tablet(&manager.tablet_dir(user_tablet_id));
        manager
            .insert_tablet(user_tablet_id, Arc::clone(&user))
            .unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));
        let key = b"not_a_tidb_table_key".to_vec();
        storage
            .put_pending_on_tablet(user_tablet_id, &key, b"pending".to_vec(), 88)
            .unwrap();
        storage.finalize_pending_grouped_with_lsn(
            &[(user_tablet_id, vec![key.clone()])],
            88,
            99,
            1001,
        );

        assert_eq!(read_value(&user, &key, 200), Some(b"pending".to_vec()));
        assert_eq!(read_value(&system, &key, 200), None);
    }

    #[test]
    fn test_finalize_pending_routes_to_all_touched_tablets_with_lsn_tracking() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let manager = Arc::new(
            TabletManager::new(dir.path(), new_lsn_provider(), Arc::clone(&system)).unwrap(),
        );

        let table_tablet = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let index_tablet = TabletId::LocalIndex {
            table_id: USER_TABLE_ID_START,
            index_id: 2001,
        };

        let table_engine = open_tablet(&manager.tablet_dir(table_tablet));
        let index_engine = open_tablet(&manager.tablet_dir(index_tablet));
        manager
            .insert_tablet(table_tablet, Arc::clone(&table_engine))
            .unwrap();
        manager
            .insert_tablet(index_tablet, Arc::clone(&index_engine))
            .unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let record_key = encode_record_key_with_handle(USER_TABLE_ID_START, 1);
        let index_key = encode_index_seek_key(USER_TABLE_ID_START, 2001, b"idx");

        storage
            .put_pending(&record_key, b"row_v".to_vec(), 10)
            .unwrap();
        storage
            .put_pending(&index_key, b"idx_v".to_vec(), 10)
            .unwrap();

        let commit_lsn = 9001;
        storage.finalize_pending_with_lsn(
            &[record_key.clone(), index_key.clone()],
            10,
            20,
            commit_lsn,
        );

        assert_eq!(table_engine.min_unflushed_lsn(), Some(commit_lsn));
        assert_eq!(index_engine.min_unflushed_lsn(), Some(commit_lsn));
        assert_eq!(system.min_unflushed_lsn(), None);

        assert_eq!(
            read_value(&table_engine, &record_key, 30),
            Some(b"row_v".to_vec())
        );
        assert_eq!(
            read_value(&index_engine, &index_key, 30),
            Some(b"idx_v".to_vec())
        );
    }

    #[test]
    fn test_abort_pending_routes_to_all_touched_tablets() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let manager = Arc::new(
            TabletManager::new(dir.path(), new_lsn_provider(), Arc::clone(&system)).unwrap(),
        );

        let table_tablet = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let index_tablet = TabletId::LocalIndex {
            table_id: USER_TABLE_ID_START,
            index_id: 2001,
        };

        let table_engine = open_tablet(&manager.tablet_dir(table_tablet));
        let index_engine = open_tablet(&manager.tablet_dir(index_tablet));
        manager
            .insert_tablet(table_tablet, Arc::clone(&table_engine))
            .unwrap();
        manager
            .insert_tablet(index_tablet, Arc::clone(&index_engine))
            .unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let record_key = encode_record_key_with_handle(USER_TABLE_ID_START, 11);
        let index_key = encode_index_seek_key(USER_TABLE_ID_START, 2001, b"idx_11");

        storage
            .put_pending(&record_key, b"pending_row".to_vec(), 66)
            .unwrap();
        storage
            .put_pending(&index_key, b"pending_idx".to_vec(), 66)
            .unwrap();

        storage.abort_pending(&[record_key.clone(), index_key.clone()], 66);

        assert_eq!(table_engine.get_lock_owner(&record_key), None);
        assert_eq!(index_engine.get_lock_owner(&index_key), None);
    }

    #[test]
    fn test_scan_iter_routes_by_range_start_key() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let manager = Arc::new(
            TabletManager::new(dir.path(), new_lsn_provider(), Arc::clone(&system)).unwrap(),
        );

        let user_tablet_id = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let user = open_tablet(&manager.tablet_dir(user_tablet_id));
        manager
            .insert_tablet(user_tablet_id, Arc::clone(&user))
            .unwrap();

        let row_key = encode_record_key_with_handle(USER_TABLE_ID_START, 123);
        let mut batch = WriteBatch::new();
        batch.put(row_key.clone(), b"row_123".to_vec());
        batch.set_commit_ts(77);
        user.write_batch(batch).unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));
        let start = encode_key(USER_TABLE_ID_START, &[]);
        let end = encode_key(USER_TABLE_ID_START + 1, &[]);
        let range = MvccKey::encode(&start, Timestamp::MAX)..MvccKey::encode(&end, 0);

        let mut iter = storage.scan_iter(range, 0).unwrap();
        crate::io::block_on_sync(async {
            iter.advance().await.unwrap();
            assert!(iter.valid());
            assert_eq!(iter.user_key(), row_key.as_slice());
            assert_eq!(iter.value(), b"row_123");
        });
    }

    #[test]
    fn test_scan_iter_on_tablet_bypasses_key_decode() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let manager = Arc::new(
            TabletManager::new(dir.path(), new_lsn_provider(), Arc::clone(&system)).unwrap(),
        );

        let user_tablet_id = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let user = open_tablet(&manager.tablet_dir(user_tablet_id));
        manager
            .insert_tablet(user_tablet_id, Arc::clone(&user))
            .unwrap();

        let key = b"plain_user_key".to_vec();
        let mut batch = WriteBatch::new();
        batch.put(key.clone(), b"plain_value".to_vec());
        batch.set_commit_ts(77);
        user.write_batch(batch).unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));
        let range =
            MvccKey::encode(&key, Timestamp::MAX)..MvccKey::encode(&key, 0).next_key().unwrap();

        let mut iter = storage
            .scan_iter_on_tablet(user_tablet_id, range, 0)
            .unwrap();
        crate::io::block_on_sync(async {
            iter.advance().await.unwrap();
            assert!(iter.valid());
            assert_eq!(iter.user_key(), key.as_slice());
            assert_eq!(iter.value(), b"plain_value");
        });
    }

    #[test]
    fn test_commit_reservation_uses_manager_global_tracker() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let manager = Arc::new(
            TabletManager::new(dir.path(), new_lsn_provider(), Arc::clone(&system)).unwrap(),
        );
        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let lsn = storage.alloc_and_reserve_commit_lsn(701);
        assert!(storage.is_commit_lsn_reserved(701, lsn));
        assert!(manager.is_commit_lsn_reserved(701, lsn));
        assert!(!system.is_commit_lsn_reserved(701, lsn));

        assert_eq!(storage.release_commit_lsn(701), Some(lsn));
        assert!(!storage.is_commit_lsn_reserved(701, lsn));
    }

    // ==================== Phase-3 QA Tests ====================

    /// T3.2b: Batch with keys from two user tables → 2 sub-batches, each
    /// routed to its own tablet (no system tablet involved).
    #[test]
    fn test_qa_split_batch_two_user_tables() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let lsn = new_lsn_provider();
        let manager = Arc::new(
            TabletManager::new(dir.path(), Arc::clone(&lsn), Arc::clone(&system)).unwrap(),
        );

        let tid_a = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let tid_b = TabletId::Table {
            table_id: USER_TABLE_ID_START + 1,
        };
        let tablet_a = open_tablet(&manager.tablet_dir(tid_a));
        let tablet_b = open_tablet(&manager.tablet_dir(tid_b));
        manager.insert_tablet(tid_a, Arc::clone(&tablet_a)).unwrap();
        manager.insert_tablet(tid_b, Arc::clone(&tablet_b)).unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let key_a = encode_record_key_with_handle(USER_TABLE_ID_START, 1);
        let key_b = encode_record_key_with_handle(USER_TABLE_ID_START + 1, 2);

        let mut batch = WriteBatch::new();
        batch.put(key_a.clone(), b"va".to_vec());
        batch.put(key_b.clone(), b"vb".to_vec());
        batch.set_commit_ts(50);

        let groups = storage.split_write_batch_by_tablet(batch);
        // Two user tablets, no system tablet
        assert_eq!(groups.len(), 2, "should split into exactly 2 sub-batches");
        assert!(!groups.contains_key(&TabletId::System));
        assert!(groups.contains_key(&tid_a));
        assert!(groups.contains_key(&tid_b));
        assert_eq!(groups[&tid_a].len(), 1);
        assert_eq!(groups[&tid_b].len(), 1);
    }

    /// T3.2e: Empty batch → no sub-batches.
    #[test]
    fn test_qa_split_batch_empty() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let manager = Arc::new(
            TabletManager::new(dir.path(), new_lsn_provider(), Arc::clone(&system)).unwrap(),
        );
        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let batch = WriteBatch::new();
        let groups = storage.split_write_batch_by_tablet(batch);
        assert!(
            groups.is_empty(),
            "empty batch should produce zero sub-batches"
        );
    }

    /// T3.2d: Split preserves Delete ops (not just Puts).
    #[test]
    fn test_qa_split_batch_preserves_delete_ops() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let lsn = new_lsn_provider();
        let manager = Arc::new(
            TabletManager::new(dir.path(), Arc::clone(&lsn), Arc::clone(&system)).unwrap(),
        );

        let tid = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let tablet = open_tablet(&manager.tablet_dir(tid));
        manager.insert_tablet(tid, tablet).unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let key_put = encode_record_key_with_handle(USER_TABLE_ID_START, 1);
        let key_del = encode_record_key_with_handle(USER_TABLE_ID_START, 2);

        let mut batch = WriteBatch::new();
        batch.put(key_put.clone(), b"v".to_vec());
        batch.delete(key_del.clone());
        batch.set_commit_ts(77);
        batch.set_clog_lsn(88);

        let groups = storage.split_write_batch_by_tablet(batch);
        assert_eq!(groups.len(), 1);
        let sub = &groups[&tid];
        assert_eq!(sub.len(), 2);
        assert!(matches!(sub.get(&key_put), Some(WriteOp::Put { .. })));
        assert!(matches!(sub.get(&key_del), Some(WriteOp::Delete)));
        assert_eq!(sub.commit_ts(), Some(77));
        assert_eq!(sub.clog_lsn(), Some(88));
    }

    /// T3.3c: Write to two tablets, scan one → only that tablet's data visible.
    #[test]
    fn test_qa_scan_does_not_cross_tablets() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let lsn = new_lsn_provider();
        let manager = Arc::new(
            TabletManager::new(dir.path(), Arc::clone(&lsn), Arc::clone(&system)).unwrap(),
        );

        let tid_a = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let tid_b = TabletId::Table {
            table_id: USER_TABLE_ID_START + 1,
        };
        let tablet_a = open_tablet(&manager.tablet_dir(tid_a));
        let tablet_b = open_tablet(&manager.tablet_dir(tid_b));
        manager.insert_tablet(tid_a, Arc::clone(&tablet_a)).unwrap();
        manager.insert_tablet(tid_b, Arc::clone(&tablet_b)).unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        // Write to both tablets
        let key_a = encode_record_key_with_handle(USER_TABLE_ID_START, 1);
        let key_b = encode_record_key_with_handle(USER_TABLE_ID_START + 1, 1);

        let mut batch_a = WriteBatch::new();
        batch_a.put(key_a.clone(), b"data_a".to_vec());
        batch_a.set_commit_ts(10);
        tablet_a.write_batch(batch_a).unwrap();

        let mut batch_b = WriteBatch::new();
        batch_b.put(key_b.clone(), b"data_b".to_vec());
        batch_b.set_commit_ts(10);
        tablet_b.write_batch(batch_b).unwrap();

        // Scan tablet A's range via RoutedTabletStorage
        let start_a = encode_key(USER_TABLE_ID_START, &[]);
        let end_a = encode_key(USER_TABLE_ID_START + 1, &[]);
        let range_a = MvccKey::encode(&start_a, Timestamp::MAX)..MvccKey::encode(&end_a, 0);

        let mut iter = storage.scan_iter(range_a, 0).unwrap();
        let mut count = 0;
        crate::io::block_on_sync(async {
            iter.advance().await.unwrap();
            while iter.valid() {
                count += 1;
                assert_eq!(
                    iter.user_key(),
                    key_a.as_slice(),
                    "scan of tablet A should only see tablet A's keys"
                );
                iter.advance().await.unwrap();
            }
        });
        assert_eq!(count, 1, "should see exactly 1 entry from tablet A");
    }

    /// T3.4a: put_pending routes by key — verify lock appears on correct tablet.
    #[test]
    fn test_qa_put_pending_routes_by_key() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let lsn = new_lsn_provider();
        let manager = Arc::new(
            TabletManager::new(dir.path(), Arc::clone(&lsn), Arc::clone(&system)).unwrap(),
        );

        let tid = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let tablet = open_tablet(&manager.tablet_dir(tid));
        manager.insert_tablet(tid, Arc::clone(&tablet)).unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let key = encode_record_key_with_handle(USER_TABLE_ID_START, 42);
        let owner_ts = 100;
        storage
            .put_pending(&key, b"pending_v".to_vec(), owner_ts)
            .unwrap();

        // Lock is on user tablet, not system tablet
        assert_eq!(tablet.get_lock_owner(&key), Some(owner_ts));
        assert_eq!(system.get_lock_owner(&key), None);

        // Cleanup
        tablet.abort_pending(&[key], owner_ts);
    }

    /// T3.4b: delete_pending routes by key.
    #[test]
    fn test_qa_delete_pending_routes_by_key() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let lsn = new_lsn_provider();
        let manager = Arc::new(
            TabletManager::new(dir.path(), Arc::clone(&lsn), Arc::clone(&system)).unwrap(),
        );

        let tid = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let tablet = open_tablet(&manager.tablet_dir(tid));
        manager.insert_tablet(tid, Arc::clone(&tablet)).unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        // First insert a committed value so delete_pending has something to delete
        let key = encode_record_key_with_handle(USER_TABLE_ID_START, 55);
        let mut batch = WriteBatch::new();
        batch.put(key.clone(), b"existing".to_vec());
        batch.set_commit_ts(10);
        tablet.write_batch(batch).unwrap();

        let owner_ts = 200;
        let deleted = storage.delete_pending(&key, owner_ts).unwrap();
        assert!(deleted, "delete_pending on existing key should return true");

        // Lock is on user tablet
        assert_eq!(tablet.get_lock_owner(&key), Some(owner_ts));
        assert_eq!(system.get_lock_owner(&key), None);

        // Cleanup
        tablet.abort_pending(&[key], owner_ts);
    }

    /// T3.4f: Two txns lock same key on same tablet → LockConflict.
    #[test]
    fn test_qa_lock_conflict_same_tablet() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let lsn = new_lsn_provider();
        let manager = Arc::new(
            TabletManager::new(dir.path(), Arc::clone(&lsn), Arc::clone(&system)).unwrap(),
        );

        let tid = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let tablet = open_tablet(&manager.tablet_dir(tid));
        manager.insert_tablet(tid, Arc::clone(&tablet)).unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let key = encode_record_key_with_handle(USER_TABLE_ID_START, 1);
        storage.put_pending(&key, b"v1".to_vec(), 100).unwrap();

        let conflict = storage.put_pending(&key, b"v2".to_vec(), 200);
        assert!(
            matches!(
                conflict,
                Err(crate::tablet::PessimisticWriteError::LockConflict(100))
            ),
            "second put_pending on same key should conflict, got {conflict:?}"
        );

        // Cleanup
        tablet.abort_pending(&[key], 100);
    }

    /// T3.4g: Two txns lock different keys on different tablets → no conflict.
    #[test]
    fn test_qa_no_lock_conflict_different_tablets() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let lsn = new_lsn_provider();
        let manager = Arc::new(
            TabletManager::new(dir.path(), Arc::clone(&lsn), Arc::clone(&system)).unwrap(),
        );

        let tid_a = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let tid_b = TabletId::Table {
            table_id: USER_TABLE_ID_START + 1,
        };
        let tablet_a = open_tablet(&manager.tablet_dir(tid_a));
        let tablet_b = open_tablet(&manager.tablet_dir(tid_b));
        manager.insert_tablet(tid_a, Arc::clone(&tablet_a)).unwrap();
        manager.insert_tablet(tid_b, Arc::clone(&tablet_b)).unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let key_a = encode_record_key_with_handle(USER_TABLE_ID_START, 1);
        let key_b = encode_record_key_with_handle(USER_TABLE_ID_START + 1, 1);

        // Different tablets → no conflict
        storage.put_pending(&key_a, b"v_a".to_vec(), 100).unwrap();
        storage.put_pending(&key_b, b"v_b".to_vec(), 200).unwrap();

        assert_eq!(tablet_a.get_lock_owner(&key_a), Some(100));
        assert_eq!(tablet_b.get_lock_owner(&key_b), Some(200));

        // Cleanup
        tablet_a.abort_pending(&[key_a], 100);
        tablet_b.abort_pending(&[key_b], 200);
    }

    /// T3.3d: scan_iter with owner_ts reads pending writes from correct tablet.
    #[test]
    fn test_qa_scan_with_owner_ts_routes_correctly() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let lsn = new_lsn_provider();
        let manager = Arc::new(
            TabletManager::new(dir.path(), Arc::clone(&lsn), Arc::clone(&system)).unwrap(),
        );

        let tid = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let tablet = open_tablet(&manager.tablet_dir(tid));
        manager.insert_tablet(tid, Arc::clone(&tablet)).unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let key = encode_record_key_with_handle(USER_TABLE_ID_START, 7);
        let owner_ts = 300;
        storage
            .put_pending(&key, b"pending_val".to_vec(), owner_ts)
            .unwrap();

        // Scan with owner_ts=300 should see the pending write
        let start = encode_key(USER_TABLE_ID_START, &[]);
        let end = encode_key(USER_TABLE_ID_START + 1, &[]);
        let range = MvccKey::encode(&start, Timestamp::MAX)..MvccKey::encode(&end, 0);

        let mut iter = storage.scan_iter(range, owner_ts).unwrap();
        crate::io::block_on_sync(async {
            iter.advance().await.unwrap();
            assert!(
                iter.valid(),
                "pending write should be visible with owner_ts"
            );
            assert_eq!(iter.user_key(), key.as_slice());
            assert_eq!(iter.value(), b"pending_val");
        });

        // Cleanup
        tablet.abort_pending(&[key], owner_ts);
    }

    /// T3.6a: Reservation is global (one per txn, not per tablet).
    /// Allocate+reserve once, verify it's visible through manager and both tablets,
    /// but NOT on any individual tablet engine.
    #[test]
    fn test_qa_reservation_global_not_per_tablet() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let lsn = new_lsn_provider();
        let manager = Arc::new(
            TabletManager::new(dir.path(), Arc::clone(&lsn), Arc::clone(&system)).unwrap(),
        );

        let tid = TabletId::Table {
            table_id: USER_TABLE_ID_START,
        };
        let tablet = open_tablet(&manager.tablet_dir(tid));
        manager.insert_tablet(tid, Arc::clone(&tablet)).unwrap();

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        let lsn_val = storage.alloc_and_reserve_commit_lsn(500);
        // Visible through RoutedTabletStorage and TabletManager
        assert!(storage.is_commit_lsn_reserved(500, lsn_val));
        assert!(manager.is_commit_lsn_reserved(500, lsn_val));
        // NOT on individual tablet engines
        assert!(!system.is_commit_lsn_reserved(500, lsn_val));
        assert!(!tablet.is_commit_lsn_reserved(500, lsn_val));

        storage.release_commit_lsn(500);
    }

    /// T3.6c: Active reservation pins min_reserved_lsn on manager, preventing
    /// boundary from advancing past it.
    #[test]
    fn test_qa_reservation_pins_min_reserved_lsn() {
        let dir = TempDir::new().unwrap();
        let system = open_tablet(&dir.path().join("system_engine"));
        let lsn = new_lsn_provider();
        let manager = Arc::new(
            TabletManager::new(dir.path(), Arc::clone(&lsn), Arc::clone(&system)).unwrap(),
        );

        let storage = RoutedTabletStorage::new(Arc::clone(&manager));

        assert_eq!(manager.min_reserved_lsn(), None, "no reservations yet");

        let lsn1 = storage.alloc_and_reserve_commit_lsn(600);
        let lsn2 = storage.alloc_and_reserve_commit_lsn(601);
        assert!(lsn1 < lsn2);
        assert_eq!(
            manager.min_reserved_lsn(),
            Some(lsn1),
            "min_reserved should be first reservation"
        );

        // Release first → min advances
        storage.release_commit_lsn(600);
        assert_eq!(
            manager.min_reserved_lsn(),
            Some(lsn2),
            "min_reserved should advance to second after first released"
        );

        storage.release_commit_lsn(601);
        assert_eq!(
            manager.min_reserved_lsn(),
            None,
            "min_reserved should be None after all released"
        );
    }
}
