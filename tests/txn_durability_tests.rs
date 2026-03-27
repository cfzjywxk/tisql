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

//! Integration tests for transaction durability and crash recovery.
//!
//! These tests use LsmEngine + FileClogService + LsmRecovery to verify
//! that committed data survives simulated crashes (drop without flush).

use std::path::Path;
use std::sync::Arc;

use tempfile::tempdir;

use tisql::new_lsn_provider;
use tisql::tablet::TabletTxnStorage;
use tisql::testkit::{
    ConcurrencyManager, FileClogConfig, FileClogService, IlogConfig, IlogService, LocalTso,
    LsmConfig, LsmEngine, LsmRecovery, TransactionService, TxnServiceTestExt, Version,
};
use tisql::{ClogService, StorageEngine, TxnService};

const SYS_TABLE_ID: u64 = 1;

use tisql::catalog::types::Timestamp;
use tisql::tablet::{is_tombstone, MvccIterator, MvccKey};

/// Helper: get value at read_ts from LsmEngine (for verification after recovery).
async fn get_value(engine: &LsmEngine, key: &[u8], read_ts: Timestamp) -> Option<Vec<u8>> {
    let seek_key = MvccKey::encode(key, read_ts);
    let end_key = MvccKey::encode(key, 0)
        .next_key()
        .unwrap_or_else(MvccKey::unbounded);
    let range = seek_key..end_key;

    let mut iter = engine.scan_iter(range, 0).unwrap();
    iter.advance().await.unwrap();

    while iter.valid() {
        let entry_key = iter.user_key();
        let entry_ts = iter.timestamp();
        if entry_key == key && entry_ts <= read_ts {
            let value = iter.value();
            if is_tombstone(value) {
                return None;
            }
            return Some(value.to_vec());
        }
        iter.advance().await.unwrap();
    }
    None
}

fn make_test_io() -> std::sync::Arc<tisql::io::IoService> {
    tisql::io::IoService::new(32).unwrap()
}

/// Create a fresh LsmEngine + TransactionService environment.
/// Uses `Handle::current()` for the clog GroupCommitWriter (spawn_blocking).
fn create_txn_env(
    dir: &Path,
) -> (
    Arc<LsmEngine>,
    TransactionService<TabletTxnStorage<LsmEngine>, FileClogService, LocalTso>,
    Arc<FileClogService>,
) {
    let handle = tokio::runtime::Handle::current();
    let lsn_provider = new_lsn_provider();
    let lsm_config = LsmConfig::builder(dir)
        .memtable_size(1 << 20)
        .build()
        .unwrap();
    let ilog_config = IlogConfig::new(dir);
    let ilog =
        Arc::new(IlogService::open_with_thread(ilog_config, Arc::clone(&lsn_provider)).unwrap());
    let engine = Arc::new(
        LsmEngine::open_with_recovery(
            lsm_config,
            Arc::clone(&lsn_provider),
            ilog,
            Version::new(),
            make_test_io(),
        )
        .unwrap(),
    );
    let clog_config = FileClogConfig::with_dir(dir);
    let clog = Arc::new(
        FileClogService::open_with_lsn_provider(
            clog_config,
            Arc::clone(&lsn_provider),
            make_test_io(),
            &handle,
        )
        .unwrap(),
    );
    let tso = Arc::new(LocalTso::new(1));
    let cm = Arc::new(ConcurrencyManager::new(0));
    let txn_storage = Arc::new(TabletTxnStorage::new(Arc::clone(&engine), Arc::clone(&cm)));
    let txn_service = TransactionService::new(txn_storage, Arc::clone(&clog), tso, cm);
    (engine, txn_service, clog)
}

/// Recover from a data directory, returning a fresh LsmEngine + TransactionService.
fn recover_txn_env(
    dir: &Path,
) -> (
    Arc<LsmEngine>,
    TransactionService<TabletTxnStorage<LsmEngine>, FileClogService, LocalTso>,
) {
    let handle = tokio::runtime::Handle::current();
    let recovery = LsmRecovery::new(dir);
    let result = recovery.recover(&handle).unwrap();

    let engine = Arc::new(result.engine);
    let tso = Arc::new(LocalTso::new(result.stats.max_commit_ts + 1));
    let cm = Arc::new(ConcurrencyManager::new(result.stats.max_commit_ts));
    let txn_storage = Arc::new(TabletTxnStorage::new(Arc::clone(&engine), Arc::clone(&cm)));
    let txn_service = TransactionService::new(txn_storage, Arc::clone(&result.clog), tso, cm);

    (engine, txn_service)
}

#[tokio::test]
async fn test_implicit_txn_crash_recovery() {
    let dir = tempdir().unwrap();
    let max_ts;

    // Phase 1: Write data and "crash" (drop without flush)
    {
        let (_engine, txn_service, clog) = create_txn_env(dir.path());

        txn_service.autocommit_put(b"k1", b"v1").await.unwrap();
        txn_service.autocommit_put(b"k2", b"v2").await.unwrap();
        let info = txn_service.autocommit_put(b"k3", b"v3").await.unwrap();
        max_ts = info.commit_ts;

        clog.sync().unwrap().await.unwrap();
    }

    // Phase 2: Recover and verify
    {
        let (engine, _txn_service) = recover_txn_env(dir.path());

        assert_eq!(
            get_value(&engine, b"k1", max_ts + 100).await,
            Some(b"v1".to_vec()),
            "k1 should survive recovery"
        );
        assert_eq!(
            get_value(&engine, b"k2", max_ts + 100).await,
            Some(b"v2".to_vec()),
            "k2 should survive recovery"
        );
        assert_eq!(
            get_value(&engine, b"k3", max_ts + 100).await,
            Some(b"v3".to_vec()),
            "k3 should survive recovery"
        );
    }
}

#[tokio::test]
async fn test_explicit_txn_crash_recovery() {
    let dir = tempdir().unwrap();
    let max_ts;

    // Phase 1: Write data via explicit transaction and crash
    {
        let (_engine, txn_service, clog) = create_txn_env(dir.path());

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut ctx, SYS_TABLE_ID, b"ek1", b"ev1".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, SYS_TABLE_ID, b"ek2", b"ev2".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, SYS_TABLE_ID, b"ek3", b"ev3".to_vec())
            .await
            .unwrap();
        let info = txn_service.commit(ctx).await.unwrap();
        max_ts = info.commit_ts;

        clog.sync().unwrap().await.unwrap();
    }

    // Phase 2: Recover and verify
    {
        let (engine, _txn_service) = recover_txn_env(dir.path());

        assert_eq!(
            get_value(&engine, b"ek1", max_ts + 100).await,
            Some(b"ev1".to_vec()),
            "ek1 should survive recovery"
        );
        assert_eq!(
            get_value(&engine, b"ek2", max_ts + 100).await,
            Some(b"ev2".to_vec()),
            "ek2 should survive recovery"
        );
        assert_eq!(
            get_value(&engine, b"ek3", max_ts + 100).await,
            Some(b"ev3".to_vec()),
            "ek3 should survive recovery"
        );
    }
}

#[tokio::test]
async fn test_explicit_txn_delete_crash_recovery() {
    let dir = tempdir().unwrap();
    let max_ts;

    // Phase 1: autocommit_put key1, then explicit delete key1
    {
        let (_engine, txn_service, clog) = create_txn_env(dir.path());

        txn_service
            .autocommit_put(b"key1", b"value1")
            .await
            .unwrap();

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .delete(&mut ctx, SYS_TABLE_ID, b"key1")
            .await
            .unwrap();
        let info = txn_service.commit(ctx).await.unwrap();
        max_ts = info.commit_ts;

        clog.sync().unwrap().await.unwrap();
    }

    // Phase 2: Recover and verify key1 is deleted
    {
        let (engine, _txn_service) = recover_txn_env(dir.path());

        assert_eq!(
            get_value(&engine, b"key1", max_ts + 100).await,
            None,
            "key1 should be deleted after recovery"
        );
    }
}

#[tokio::test]
async fn test_mixed_implicit_explicit_crash_recovery() {
    let dir = tempdir().unwrap();
    let max_ts;

    // Phase 1: Mixed operations
    {
        let (_engine, txn_service, clog) = create_txn_env(dir.path());

        // Implicit put key1
        txn_service.autocommit_put(b"key1", b"v1").await.unwrap();

        // Explicit: put key2, delete key1
        let mut ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut ctx, SYS_TABLE_ID, b"key2", b"v2".to_vec())
            .await
            .unwrap();
        txn_service
            .delete(&mut ctx, SYS_TABLE_ID, b"key1")
            .await
            .unwrap();
        txn_service.commit(ctx).await.unwrap();

        // Implicit put key3
        let info = txn_service.autocommit_put(b"key3", b"v3").await.unwrap();
        max_ts = info.commit_ts;

        clog.sync().unwrap().await.unwrap();
    }

    // Phase 2: Recover and verify
    {
        let (engine, _txn_service) = recover_txn_env(dir.path());

        assert_eq!(
            get_value(&engine, b"key1", max_ts + 100).await,
            None,
            "key1 should be deleted"
        );
        assert_eq!(
            get_value(&engine, b"key2", max_ts + 100).await,
            Some(b"v2".to_vec()),
            "key2 should survive"
        );
        assert_eq!(
            get_value(&engine, b"key3", max_ts + 100).await,
            Some(b"v3".to_vec()),
            "key3 should survive"
        );
    }
}

#[tokio::test]
async fn test_explicit_txn_rollback_not_recovered() {
    let dir = tempdir().unwrap();
    let max_ts;

    // Phase 1: One rolled-back txn and one committed txn
    {
        let (_engine, txn_service, clog) = create_txn_env(dir.path());

        // Explicit txn 1: put key1, rollback
        let mut ctx1 = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut ctx1, SYS_TABLE_ID, b"key1", b"v1".to_vec())
            .await
            .unwrap();
        txn_service.rollback(ctx1).unwrap();

        // Explicit txn 2: put key2, commit
        let mut ctx2 = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut ctx2, SYS_TABLE_ID, b"key2", b"v2".to_vec())
            .await
            .unwrap();
        let info = txn_service.commit(ctx2).await.unwrap();
        max_ts = info.commit_ts;

        clog.sync().unwrap().await.unwrap();
    }

    // Phase 2: Recover and verify
    {
        let (engine, _txn_service) = recover_txn_env(dir.path());

        assert_eq!(
            get_value(&engine, b"key1", max_ts + 100).await,
            None,
            "key1 was rolled back, should not be present"
        );
        assert_eq!(
            get_value(&engine, b"key2", max_ts + 100).await,
            Some(b"v2".to_vec()),
            "key2 was committed, should survive"
        );
    }
}

#[tokio::test]
async fn test_large_explicit_txn_10k_keys_crash_recovery() {
    let dir = tempdir().unwrap();
    let max_ts;

    // Phase 1: Commit a large explicit transaction (10k keys), then crash.
    {
        let (_engine, txn_service, clog) = create_txn_env(dir.path());
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        for i in 0..10_000u32 {
            let key = format!("bulk_key_{i:05}").into_bytes();
            txn_service
                .put(
                    &mut ctx,
                    SYS_TABLE_ID,
                    &key,
                    format!("bulk_val_{i:05}").into_bytes(),
                )
                .await
                .unwrap();
        }

        let info = txn_service.commit(ctx).await.unwrap();
        max_ts = info.commit_ts;
        clog.sync().unwrap().await.unwrap();
    }

    // Phase 2: Recover and spot-check representative keys.
    {
        let (engine, _txn_service) = recover_txn_env(dir.path());
        for i in [0u32, 1, 17, 777, 4_321, 9_999] {
            assert_eq!(
                get_value(&engine, format!("bulk_key_{i:05}").as_bytes(), max_ts + 100).await,
                Some(format!("bulk_val_{i:05}").into_bytes()),
                "bulk key {i} should survive recovery"
            );
        }
    }
}
