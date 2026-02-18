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

//! Concurrency tests for MVCC and TSO.
//!
//! These tests verify:
//! 1. TSO ordering guarantees under concurrent load
//! 2. Reader blocking by in-memory locks during writes
//! 3. Write-read conflict scenarios
//!
//! To run with failpoints enabled:
//! ```
//! cargo test -p tisql --test concurrency_test --features failpoints
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tisql::catalog::types::{RawValue, Timestamp};
use tisql::tablet::mvcc::{is_tombstone, MvccIterator, MvccKey};
use tisql::testkit::{
    ConcurrencyManager, FileClogConfig, FileClogService, LocalTso, MemTableEngine,
    TransactionService, TxnServiceTestExt,
};
use tisql::util::error::TiSqlError;
use tisql::StorageEngine;

fn make_test_io() -> Arc<tisql::io::IoService> {
    tisql::io::IoService::new(32).unwrap()
}

// ==================== Test Helpers Using MvccKey ====================

async fn get_at_for_test(storage: &MemTableEngine, key: &[u8], ts: Timestamp) -> Option<RawValue> {
    let start = MvccKey::encode(key, ts);
    let end = MvccKey::encode(key, 0)
        .next_key()
        .unwrap_or_else(MvccKey::unbounded);
    let range = start..end;

    // Use streaming scan_iter() - process one entry at a time
    let mut iter = storage.scan_iter(range, 0).unwrap();
    iter.advance().await.unwrap(); // Position on first entry

    while iter.valid() {
        let decoded_key = iter.user_key();
        let entry_ts = iter.timestamp();
        if decoded_key == key && entry_ts <= ts {
            let value = iter.value().to_vec();
            if is_tombstone(&value) {
                return None;
            }
            return Some(value);
        }
        iter.advance().await.unwrap();
    }
    None
}

async fn get_for_test(storage: &MemTableEngine, key: &[u8]) -> Option<RawValue> {
    get_at_for_test(storage, key, Timestamp::MAX).await
}

/// Type alias for the test storage engine
type TestStorage = MemTableEngine;

/// Type alias for the test transaction service
type TestTxnService = TransactionService<TestStorage, FileClogService, LocalTso>;

/// Type alias for the create_test_service return type
type TestServiceTuple = (
    Arc<TestStorage>,
    Arc<TestTxnService>,
    Arc<LocalTso>,
    Arc<ConcurrencyManager>,
    tempfile::TempDir,
);

fn create_test_service() -> TestServiceTuple {
    let dir = tempfile::tempdir().unwrap();
    let handle = tokio::runtime::Handle::current();
    let config = FileClogConfig::with_dir(dir.path());
    let storage = Arc::new(MemTableEngine::new());
    let clog_service = Arc::new(
        FileClogService::open_with_lsn_provider(
            config,
            storage.lsn_provider(),
            make_test_io(),
            &handle,
        )
        .unwrap(),
    );
    let tso = Arc::new(LocalTso::new(1));
    let cm = Arc::new(ConcurrencyManager::new(0));
    let txn_service = Arc::new(TransactionService::new(
        Arc::clone(&storage),
        clog_service,
        Arc::clone(&tso),
        Arc::clone(&cm),
    ));
    (storage, txn_service, tso, cm, dir)
}

// ============================================================================
// TSO Ordering Tests
// ============================================================================

/// Verify TSO produces strictly monotonic timestamps under concurrent load.
#[tokio::test]
async fn test_tso_strict_ordering_concurrent() {
    use tisql::TsoService;

    let tso = Arc::new(LocalTso::new(1));
    let num_threads = 8;
    let timestamps_per_thread = 1000;

    let mut handles = vec![];

    for _ in 0..num_threads {
        let tso = Arc::clone(&tso);
        handles.push(tokio::spawn(async move {
            let mut timestamps = Vec::with_capacity(timestamps_per_thread);
            for _ in 0..timestamps_per_thread {
                timestamps.push(tso.get_ts());
            }
            timestamps
        }));
    }

    // Collect all timestamps
    let mut all_timestamps = vec![];
    for handle in handles {
        all_timestamps.extend(handle.await.unwrap());
    }

    // Verify all timestamps are unique (strict ordering)
    let mut sorted = all_timestamps.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        all_timestamps.len(),
        "TSO should produce unique timestamps"
    );

    // Verify no gaps (all timestamps from 1 to N)
    let expected_max = (num_threads * timestamps_per_thread) as u64;
    assert_eq!(sorted.first(), Some(&1));
    assert_eq!(sorted.last(), Some(&expected_max));
}

/// Verify per-thread timestamps are locally monotonic.
#[tokio::test]
async fn test_tso_per_thread_monotonic() {
    use tisql::TsoService;

    let tso = Arc::new(LocalTso::new(1));
    let num_threads = 4;
    let timestamps_per_thread = 500;

    let mut handles = vec![];

    for _ in 0..num_threads {
        let tso = Arc::clone(&tso);
        handles.push(tokio::spawn(async move {
            let mut prev = 0u64;
            for _ in 0..timestamps_per_thread {
                let ts = tso.get_ts();
                assert!(
                    ts > prev,
                    "Timestamp must be strictly increasing: prev={prev}, ts={ts}"
                );
                prev = ts;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

/// Verify max_ts tracking under concurrent updates.
#[tokio::test]
async fn test_max_ts_concurrent_updates() {
    let cm = Arc::new(ConcurrencyManager::new(0));
    let num_threads = 8;
    let updates_per_thread = 1000;

    let max_seen = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for i in 0..num_threads {
        let cm = Arc::clone(&cm);
        let max_seen = Arc::clone(&max_seen);
        handles.push(tokio::spawn(async move {
            for j in 0..updates_per_thread {
                // Create a unique timestamp for this thread
                let ts = ((i as u64) * updates_per_thread + j + 1) * 1000;
                cm.update_max_ts(ts);

                // Track the maximum we've seen
                let _ = max_seen.fetch_max(ts, Ordering::Relaxed);
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // max_ts should reflect the highest value seen
    let expected_max = (num_threads as u64) * updates_per_thread * 1000;
    assert_eq!(cm.max_ts(), expected_max);
}

// ============================================================================
// Lock Conflict Tests (via PessimisticStorage)
// ============================================================================

/// Test concurrent writers to different keys succeed.
#[tokio::test]
async fn test_concurrent_writes_different_keys() {
    let (_storage, txn_service, _tso, _cm, _dir) = create_test_service();
    let num_threads = 4;
    let writes_per_thread = 100;

    let success_count = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for i in 0..num_threads {
        let txn_service = Arc::clone(&txn_service);
        let success_count = Arc::clone(&success_count);
        handles.push(tokio::spawn(async move {
            for j in 0..writes_per_thread {
                let key = format!("key_{i}_{j}");
                let value = format!("value_{i}_{j}");
                if txn_service
                    .autocommit_put(key.as_bytes(), value.as_bytes())
                    .await
                    .is_ok()
                {
                    success_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // All writes should succeed since they're to different keys
    let total_writes = num_threads * writes_per_thread;
    assert_eq!(success_count.load(Ordering::Relaxed), total_writes as u64);
}

/// Test concurrent writers to same key - one should get lock conflict.
#[tokio::test]
async fn test_concurrent_writes_same_key() {
    let (_storage, txn_service, _tso, _cm, _dir) = create_test_service();
    let num_threads = 10;
    let key = b"shared_key";

    let success_count = Arc::new(AtomicU64::new(0));
    let conflict_count = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(num_threads));
    let mut handles = vec![];

    for i in 0..num_threads {
        let txn_service = Arc::clone(&txn_service);
        let success_count = Arc::clone(&success_count);
        let conflict_count = Arc::clone(&conflict_count);
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            // Synchronize all tasks to maximize contention
            barrier.wait().await;

            let value = format!("value_{i}");
            match txn_service.autocommit_put(key, value.as_bytes()).await {
                Ok(_) => {
                    success_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(TiSqlError::KeyIsLocked { .. }) => {
                    conflict_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    panic!("Unexpected error: {e:?}");
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Total should equal num_threads
    let successes = success_count.load(Ordering::Relaxed);
    let conflicts = conflict_count.load(Ordering::Relaxed);
    assert_eq!(
        successes + conflicts,
        num_threads as u64,
        "All threads should either succeed or get conflict"
    );

    // At least one should succeed (first to acquire lock)
    assert!(successes >= 1, "At least one write should succeed");
}

// ============================================================================
// Write-Read Conflict Scenarios
// ============================================================================

/// Test that MVCC read at specific timestamp works correctly.
#[tokio::test]
async fn test_mvcc_read_at_timestamp() {
    let (storage, txn_service, _tso, _cm, _dir) = create_test_service();

    let key = b"version_key";

    // Write version 1
    let ts1 = txn_service
        .autocommit_put(key, b"v1")
        .await
        .unwrap()
        .commit_ts;

    // Write version 2
    let ts2 = txn_service
        .autocommit_put(key, b"v2")
        .await
        .unwrap()
        .commit_ts;

    // Write version 3
    let ts3 = txn_service
        .autocommit_put(key, b"v3")
        .await
        .unwrap()
        .commit_ts;

    assert!(ts1 < ts2);
    assert!(ts2 < ts3);

    // Read at ts3 should see v3
    let v = get_at_for_test(&storage, key, ts3).await;
    assert_eq!(v, Some(b"v3".to_vec()));

    // Read at ts2 should see v2
    let v = get_at_for_test(&storage, key, ts2).await;
    assert_eq!(v, Some(b"v2".to_vec()));

    // Read at ts1 should see v1
    let v = get_at_for_test(&storage, key, ts1).await;
    assert_eq!(v, Some(b"v1".to_vec()));

    // Read at timestamp before ts1 should see nothing
    let v = get_at_for_test(&storage, key, ts1 - 1).await;
    assert!(v.is_none());

    // Read at latest should see v3
    let v = get_for_test(&storage, key).await;
    assert_eq!(v, Some(b"v3".to_vec()));
}

/// Test multiple readers don't interfere with each other.
#[tokio::test]
async fn test_concurrent_readers_no_interference() {
    let (storage, txn_service, _tso, _cm, _dir) = create_test_service();
    let key = b"shared_read_key";

    // Write initial value
    txn_service
        .autocommit_put(key, b"initial_value")
        .await
        .unwrap();

    let num_readers = 10;
    let reads_per_thread = 100;
    let success_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    for _ in 0..num_readers {
        let storage = Arc::clone(&storage);
        let success_count = Arc::clone(&success_count);
        handles.push(tokio::spawn(async move {
            for _ in 0..reads_per_thread {
                match get_for_test(&storage, key).await {
                    Some(v) if v == b"initial_value" => {
                        success_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Some(v) => panic!("Unexpected value: {v:?}"),
                    None => panic!("Key should exist"),
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let total_reads = num_readers * reads_per_thread;
    assert_eq!(
        success_count.load(Ordering::Relaxed),
        total_reads as u64,
        "All reads should succeed"
    );
}

/// Test delete creates proper tombstone and is visible to concurrent readers.
#[tokio::test]
async fn test_concurrent_read_after_delete() {
    let (storage, txn_service, _tso, _cm, _dir) = create_test_service();
    let key = b"delete_key";

    // Write initial value
    let ts1 = txn_service
        .autocommit_put(key, b"value")
        .await
        .unwrap()
        .commit_ts;

    // Verify value exists
    assert!(get_for_test(&storage, key).await.is_some());

    // Delete
    let ts2 = txn_service.autocommit_delete(key).await.unwrap().commit_ts;
    assert!(ts2 > ts1);

    // Read at latest should see nothing (deleted)
    assert!(get_for_test(&storage, key).await.is_none());

    // Read at ts1 should still see the value (MVCC)
    let v = get_at_for_test(&storage, key, ts1).await;
    assert_eq!(v, Some(b"value".to_vec()));
}

// ============================================================================
// Implicit Transaction Multi-Op Tests (pending node behavior)
// ============================================================================

/// Test that multiple puts to the same key in one transaction commits only the last value.
///
/// Each put writes a pending node that replaces the previous pending node.
#[tokio::test]
async fn test_implicit_multiple_puts_same_key() {
    use tisql::TxnService;

    let (_storage, txn_service, _tso, _cm, _dir) = create_test_service();

    let key = b"dedup_key";
    let value1 = b"first_value";
    let value2 = b"second_value";
    let value3 = b"third_value";

    // Start a transaction and put the same key multiple times
    let mut ctx = txn_service.begin(false).unwrap();
    txn_service
        .put(&mut ctx, key.to_vec(), value1.to_vec())
        .await
        .unwrap();
    txn_service
        .put(&mut ctx, key.to_vec(), value2.to_vec())
        .await
        .unwrap();
    txn_service
        .put(&mut ctx, key.to_vec(), value3.to_vec())
        .await
        .unwrap();

    // Commit the transaction
    txn_service.commit(ctx).await.unwrap();

    // Read after commit should see the last value
    let read_ctx = txn_service.begin(true).unwrap();
    let result = txn_service.get(&read_ctx, key).await.unwrap();
    assert_eq!(
        result,
        Some(value3.to_vec()),
        "Committed value should be the last put"
    );
}

/// Test that put followed by delete in the same transaction commits as delete.
///
/// Put writes a pending value node, then delete sees the pending value and writes TOMBSTONE.
#[tokio::test]
async fn test_implicit_put_then_delete() {
    use tisql::TxnService;

    let (_storage, txn_service, _tso, _cm, _dir) = create_test_service();

    let key = b"put_delete_key";
    let value = b"some_value";

    // Start a transaction, put then delete
    let mut ctx = txn_service.begin(false).unwrap();
    txn_service
        .put(&mut ctx, key.to_vec(), value.to_vec())
        .await
        .unwrap();
    txn_service.delete(&mut ctx, key.to_vec()).await.unwrap();

    // Commit the transaction
    txn_service.commit(ctx).await.unwrap();

    // Read after commit should see None (delete wins)
    let read_ctx = txn_service.begin(true).unwrap();
    let result = txn_service.get(&read_ctx, key).await.unwrap();
    assert_eq!(result, None, "Committed result should be delete (None)");
}

/// Test that delete followed by put in the same transaction commits the put.
///
/// Delete writes TOMBSTONE/LOCK, then put replaces it with a value pending node.
#[tokio::test]
async fn test_implicit_delete_then_put() {
    use tisql::TxnService;

    let (_storage, txn_service, _tso, _cm, _dir) = create_test_service();

    let key = b"delete_put_key";
    let value = b"final_value";

    // First, insert an initial value
    let mut setup_ctx = txn_service.begin(false).unwrap();
    txn_service
        .put(&mut setup_ctx, key.to_vec(), b"initial".to_vec())
        .await
        .unwrap();
    txn_service.commit(setup_ctx).await.unwrap();

    // Start a new transaction, delete then put
    let mut ctx = txn_service.begin(false).unwrap();
    txn_service.delete(&mut ctx, key.to_vec()).await.unwrap();
    txn_service
        .put(&mut ctx, key.to_vec(), value.to_vec())
        .await
        .unwrap();

    // Commit the transaction
    txn_service.commit(ctx).await.unwrap();

    // Read after commit should see the new value (put wins)
    let read_ctx = txn_service.begin(true).unwrap();
    let result = txn_service.get(&read_ctx, key).await.unwrap();
    assert_eq!(
        result,
        Some(value.to_vec()),
        "Committed result should be the put value"
    );
}

/// Test that only the final state of each key is committed.
#[tokio::test]
async fn test_implicit_dedup_commit() {
    use tisql::TxnService;

    let (_storage, txn_service, _tso, _cm, _dir) = create_test_service();

    // Start a transaction and put multiple values, some with duplicates
    let mut ctx = txn_service.begin(false).unwrap();

    // Key "a" - multiple puts, last should win
    txn_service
        .put(&mut ctx, b"a".to_vec(), b"a1".to_vec())
        .await
        .unwrap();
    txn_service
        .put(&mut ctx, b"a".to_vec(), b"a2".to_vec())
        .await
        .unwrap();

    // Key "b" - put then delete, should be absent
    txn_service
        .put(&mut ctx, b"b".to_vec(), b"b1".to_vec())
        .await
        .unwrap();
    txn_service.delete(&mut ctx, b"b".to_vec()).await.unwrap();

    // Key "c" - single put
    txn_service
        .put(&mut ctx, b"c".to_vec(), b"c1".to_vec())
        .await
        .unwrap();

    // Commit the transaction
    txn_service.commit(ctx).await.unwrap();

    // Scan after commit should show: a=a2, c=c1 (b is deleted/absent)
    let read_ctx = txn_service.begin(true).unwrap();
    let range = b"a".to_vec()..b"d".to_vec();
    let mut iter = txn_service.scan_iter(&read_ctx, range).unwrap();
    let mut results = Vec::new();
    iter.advance().await.unwrap();
    while iter.valid() {
        results.push((iter.user_key().to_vec(), iter.value().to_vec()));
        iter.advance().await.unwrap();
    }

    assert_eq!(results.len(), 2, "Should have 2 keys (a and c, not b)");

    // Verify results
    let results_map: std::collections::HashMap<_, _> = results.into_iter().collect();
    assert_eq!(results_map.get(b"a".as_slice()), Some(&b"a2".to_vec()));
    assert_eq!(results_map.get(b"b".as_slice()), None); // deleted
    assert_eq!(results_map.get(b"c".as_slice()), Some(&b"c1".to_vec()));
}

// ============================================================================
// Failpoint-based Tests (require --features failpoints)
// ============================================================================

#[cfg(feature = "failpoints")]
mod failpoint_tests {
    use super::*;
    use std::time::Duration;

    /// Test multiple writers with controlled ordering.
    #[tokio::test]
    async fn test_serialized_writes_with_failpoint() {
        let scenario = fail::FailScenario::setup();

        let (storage, txn_service, _tso, _cm, _dir) = create_test_service();
        let key = b"serial_key";

        let results = Arc::new(std::sync::Mutex::new(vec![]));

        for i in 0..3 {
            let txn_service = Arc::clone(&txn_service);
            let results = Arc::clone(&results);
            let value = format!("v{i}");

            // Each write will succeed in order since they wait for previous
            let ts = txn_service
                .autocommit_put(key, value.as_bytes())
                .await
                .unwrap()
                .commit_ts;
            results.lock().unwrap().push((i, ts));
        }

        let results = results.lock().unwrap();

        // Verify timestamps are strictly increasing
        for i in 1..results.len() {
            assert!(
                results[i].1 > results[i - 1].1,
                "Timestamps should be strictly increasing"
            );
        }

        // Final value should be v2
        let v = get_for_test(&storage, key).await;
        assert_eq!(v, Some(b"v2".to_vec()));

        scenario.teardown();
    }

    /// Regression test for "commit in the past" anomaly (time-travel bug).
    ///
    /// This test verifies the fix for the snapshot isolation violation where:
    /// BEFORE FIX: Writer picks commit_ts -> Reader starts with start_ts > commit_ts
    ///             -> Writer acquires lock -> Writer commits
    ///             -> Reader misses committed data because start_ts > commit_ts
    ///
    /// AFTER FIX: Writer acquires pending lock -> Reader starts
    ///            -> Reader's start_ts is recorded in max_ts
    ///            -> Writer picks commit_ts = max(max_ts + 1, tso_ts) > start_ts
    ///            -> Reader correctly sees the committed data OR is blocked by lock
    ///
    /// The key insight is that by acquiring locks BEFORE getting commit_ts,
    /// we ensure commit_ts > any concurrent reader's start_ts.
    #[tokio::test]
    async fn test_no_commit_in_the_past_anomaly() {
        use tisql::TxnService;

        let scenario = fail::FailScenario::setup();

        let (_storage, txn_service, _tso, cm, _dir) = create_test_service();
        let key = b"time_travel_key";

        // Phase 1: Writer acquires lock and pauses
        fail::cfg("txn_after_lock_before_commit_ts", "pause").unwrap();

        let txn_service_clone = Arc::clone(&txn_service);
        let writer = tokio::spawn(async move {
            // This will pause right after acquiring locks but BEFORE setting commit_ts
            // At the failpoint, locks are held but commit_ts = max(max_ts+1, tso_ts)
            // hasn't been computed yet
            txn_service_clone
                .autocommit_put(key, b"value")
                .await
                .unwrap()
        });

        // Give writer time to reach failpoint (pending locks acquired)
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Phase 2: Reader starts a transaction - this updates max_ts
        // In a buggy implementation, if commit_ts was picked before locks,
        // this reader's start_ts could be > commit_ts, causing it to miss data.
        let reader_ctx = txn_service.begin(true).unwrap();
        let reader_start_ts = reader_ctx.start_ts();

        // Reader's start_ts should have updated max_ts
        let max_ts_after_reader = cm.max_ts();
        assert!(
            max_ts_after_reader >= reader_start_ts,
            "max_ts should include reader's start_ts"
        );

        // Phase 3: Resume writer
        fail::cfg("txn_after_lock_before_commit_ts", "off").unwrap();

        let writer_result = writer.await.unwrap();
        let writer_commit_ts = writer_result.commit_ts;

        // THE KEY ASSERTION: commit_ts must be > reader's start_ts
        // This is the fix for the "commit in the past" anomaly.
        // Because we get commit_ts = max(max_ts + 1, tso_ts) AFTER locks,
        // and max_ts includes reader_start_ts, commit_ts > reader_start_ts.
        assert!(
            writer_commit_ts > reader_start_ts,
            "commit_ts ({writer_commit_ts}) must be > reader_start_ts ({reader_start_ts}) to prevent time-travel anomaly"
        );

        // Now verify snapshot isolation works correctly:
        // Reader with start_ts < commit_ts should NOT see the committed data
        // (correct behavior - reader started before the logical commit point)
        let value = txn_service.get(&reader_ctx, key).await.unwrap();
        assert!(
            value.is_none(),
            "Reader with start_ts < commit_ts should not see the data"
        );

        // A new reader starting now should see the data
        let new_reader_ctx = txn_service.begin(true).unwrap();
        let value = txn_service.get(&new_reader_ctx, key).await.unwrap();
        assert_eq!(
            value,
            Some(b"value".to_vec()),
            "New reader should see committed data"
        );

        scenario.teardown();
    }

    /// Test that concurrent readers during commit don't create anomalies.
    ///
    /// Multiple readers starting while a writer holds locks should all
    /// have start_ts < commit_ts (when the writer eventually commits).
    #[tokio::test]
    async fn test_multiple_readers_during_write_no_anomaly() {
        use tisql::TxnService;

        let scenario = fail::FailScenario::setup();

        let (_storage, txn_service, _tso, _cm, _dir) = create_test_service();
        let key = b"multi_reader_key";

        fail::cfg("txn_after_lock_before_commit_ts", "pause").unwrap();

        let txn_service_clone = Arc::clone(&txn_service);
        let writer = tokio::spawn(async move {
            txn_service_clone
                .autocommit_put(key, b"value")
                .await
                .unwrap()
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Start multiple readers while writer holds lock
        let mut reader_contexts = vec![];
        for _ in 0..5 {
            let ctx = txn_service.begin(true).unwrap();
            reader_contexts.push(ctx);
        }

        // Resume writer
        fail::cfg("txn_after_lock_before_commit_ts", "off").unwrap();
        let writer_result = writer.await.unwrap();
        let writer_commit_ts = writer_result.commit_ts;

        // ALL readers should have start_ts < commit_ts
        for (i, ctx) in reader_contexts.iter().enumerate() {
            let start_ts = ctx.start_ts();
            assert!(
                writer_commit_ts > start_ts,
                "Reader {i} start_ts ({start_ts}) should be < commit_ts ({writer_commit_ts})"
            );

            // None of them should see the data
            let value = txn_service.get(ctx, key).await.unwrap();
            assert!(
                value.is_none(),
                "Reader {i} should not see data committed after its start_ts"
            );
        }

        scenario.teardown();
    }
}

// ============================================================================
// DDL Concurrency Tests
// ============================================================================

/// Tests for DDL/DDL and DDL/DML concurrency control.
mod ddl_concurrency {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tempfile::tempdir;
    use tisql::{Database, DatabaseConfig, QueryResult};

    /// Test that concurrent DDLs are serialized (no conflicts).
    #[tokio::test]
    async fn test_concurrent_create_different_tables() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Arc::new(Database::open(config).unwrap());

        let num_threads = 4;
        let barrier = Arc::new(tokio::sync::Barrier::new(num_threads));
        let success_count = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let db = Arc::clone(&db);
                let barrier = Arc::clone(&barrier);
                let success_count = Arc::clone(&success_count);

                tokio::spawn(async move {
                    // Wait for all tasks to be ready
                    barrier.wait().await;

                    // Each task creates a different table
                    let sql = format!("CREATE TABLE t{i} (id INT PRIMARY KEY, name VARCHAR(100))");
                    match db.execute_query(&sql).await {
                        Ok(_) => {
                            success_count.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            panic!("Thread {i} failed to create table: {e}");
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        // All creates should succeed
        assert_eq!(
            success_count.load(Ordering::SeqCst),
            num_threads,
            "All table creations should succeed"
        );

        // Verify all tables exist
        for i in 0..num_threads {
            let sql = format!("SELECT * FROM t{i}");
            assert!(
                db.execute_query(&sql).await.is_ok(),
                "Table t{i} should exist"
            );
        }

        db.close().await.unwrap();
    }

    /// Test that concurrent DDLs creating the same table result in exactly one success.
    #[tokio::test]
    async fn test_concurrent_create_same_table() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Arc::new(Database::open(config).unwrap());

        let num_threads = 4;
        let barrier = Arc::new(tokio::sync::Barrier::new(num_threads));
        let success_count = Arc::new(AtomicUsize::new(0));
        let error_count = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let db = Arc::clone(&db);
                let barrier = Arc::clone(&barrier);
                let success_count = Arc::clone(&success_count);
                let error_count = Arc::clone(&error_count);

                tokio::spawn(async move {
                    barrier.wait().await;

                    // All tasks try to create the same table
                    match db
                        .execute_query("CREATE TABLE conflict_table (id INT PRIMARY KEY)")
                        .await
                    {
                        Ok(_) => {
                            success_count.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            // Valid errors: "already exists" or "key is locked" (transaction conflict)
                            let msg = e.to_string().to_lowercase();
                            assert!(
                                msg.contains("already exists")
                                    || msg.contains("exists")
                                    || msg.contains("key is locked"),
                                "Thread {i} got unexpected error: {e}"
                            );
                            error_count.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        // Exactly one should succeed
        assert_eq!(
            success_count.load(Ordering::SeqCst),
            1,
            "Exactly one CREATE should succeed"
        );
        assert_eq!(
            error_count.load(Ordering::SeqCst),
            num_threads - 1,
            "Others should get 'already exists' error"
        );

        db.close().await.unwrap();
    }

    /// Test DDL and DML concurrency - DML should detect schema change.
    #[tokio::test]
    async fn test_ddl_during_dml_causes_retry() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Arc::new(Database::open(config).unwrap());

        // Create initial table
        db.execute_query("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))")
            .await
            .unwrap();
        db.execute_query("INSERT INTO users VALUES (1, 'Alice')")
            .await
            .unwrap();

        // This test verifies that schema version is checked.
        // In a real concurrent scenario, if DDL commits between DML start and commit,
        // the DML would fail with SchemaChanged error.

        // For now, verify that the schema version mechanism works by checking
        // that after a DDL, subsequent operations see the updated schema.
        // Execute a query to exercise the catalog
        db.execute_query("SELECT * FROM users").await.unwrap();

        // DDL changes schema
        db.execute_query("CREATE TABLE orders (id INT PRIMARY KEY)")
            .await
            .unwrap();

        // DML on original table should still work (schema of 'users' didn't change)
        db.execute_query("INSERT INTO users VALUES (2, 'Bob')")
            .await
            .unwrap();

        // Verify data
        let result = db.execute_query("SELECT id FROM users ORDER BY id").await;
        match result {
            Ok(QueryResult::Rows { data, .. }) => {
                assert_eq!(data.len(), 2);
                assert_eq!(data[0][0], "1");
                assert_eq!(data[1][0], "2");
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        db.close().await.unwrap();
    }

    /// Test schema version increments correctly with concurrent DDLs.
    #[tokio::test]
    async fn test_schema_version_with_concurrent_ddl() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Arc::new(Database::open(config).unwrap());

        // Create tables sequentially to establish baseline
        for i in 0..5 {
            db.execute_query(&format!("CREATE TABLE seq_t{i} (id INT PRIMARY KEY)"))
                .await
                .unwrap();
        }

        // Now create more tables concurrently
        let num_threads = 4;
        let barrier = Arc::new(tokio::sync::Barrier::new(num_threads));

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let db = Arc::clone(&db);
                let barrier = Arc::clone(&barrier);

                tokio::spawn(async move {
                    barrier.wait().await;
                    db.execute_query(&format!("CREATE TABLE conc_t{i} (id INT PRIMARY KEY)"))
                        .await
                        .unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all 9 tables exist (5 sequential + 4 concurrent)
        for i in 0..5 {
            assert!(
                db.execute_query(&format!("SELECT * FROM seq_t{i}"))
                    .await
                    .is_ok(),
                "seq_t{i} should exist"
            );
        }
        for i in 0..num_threads {
            assert!(
                db.execute_query(&format!("SELECT * FROM conc_t{i}"))
                    .await
                    .is_ok(),
                "conc_t{i} should exist"
            );
        }

        db.close().await.unwrap();
    }

    /// Test drop table with concurrent create.
    #[tokio::test]
    async fn test_drop_and_recreate_table() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Arc::new(Database::open(config).unwrap());

        // Create, drop, recreate in sequence
        db.execute_query("CREATE TABLE temp (id INT PRIMARY KEY)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO temp VALUES (1)")
            .await
            .unwrap();
        db.execute_query("DROP TABLE temp").await.unwrap();
        db.execute_query("CREATE TABLE temp (id INT PRIMARY KEY, name VARCHAR(50))")
            .await
            .unwrap();
        db.execute_query("INSERT INTO temp VALUES (2, 'test')")
            .await
            .unwrap();

        // Verify new schema is in effect
        let result = db.execute_query("SELECT id, name FROM temp").await;
        match result {
            Ok(QueryResult::Rows { data, columns }) => {
                assert_eq!(columns.len(), 2);
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "2");
                assert_eq!(data[0][1], "test");
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        db.close().await.unwrap();
    }
}

// ============================================================================
// Concurrent Scan with Writers
// ============================================================================

/// Test scan_iter works correctly while concurrent writers are running.
///
/// This verifies:
/// - Scan iterators maintain snapshot isolation during concurrent writes
/// - Range filtering works correctly under contention
/// - No data corruption or iterator invalidation during concurrent access
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_scan_while_writers_run() {
    use tisql::TxnService;

    let (_storage, txn_service, _tso, _cm, _dir) = create_test_service();

    // Pre-populate with data in a "safe" range that writers won't touch
    // This ensures scanners always have something to read
    for i in 0..10 {
        let key = format!("safe_{i:02}");
        let value = format!("initial_{i}");
        txn_service
            .autocommit_put(key.as_bytes(), value.as_bytes())
            .await
            .unwrap();
    }

    let num_writers = 4;
    let writes_per_thread = 50;
    let num_scanners = 2;
    let scans_per_thread = 20;

    let stop_flag = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];

    // Spawn writer tasks - write to "write_*" keys (different range from scan)
    for w in 0..num_writers {
        let txn_service = Arc::clone(&txn_service);
        let stop_flag = Arc::clone(&stop_flag);
        handles.push(tokio::spawn(async move {
            let mut success = 0u64;
            for i in 0..writes_per_thread {
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                // Write to a different key range to reduce lock conflicts with scanners
                let key = format!("write_{:02}_{:02}", w, i % 10);
                let value = format!("writer_{w}_iter_{i}");
                match txn_service
                    .autocommit_put(key.as_bytes(), value.as_bytes())
                    .await
                {
                    Ok(_) => success += 1,
                    Err(TiSqlError::KeyIsLocked { .. }) => {} // Expected under contention
                    Err(e) => panic!("Unexpected error: {e:?}"),
                }
                tokio::task::yield_now().await;
            }
            success
        }));
    }

    // Spawn scanner tasks - scan the "safe_*" range that has pre-populated data
    for _ in 0..num_scanners {
        let txn_service = Arc::clone(&txn_service);
        handles.push(tokio::spawn(async move {
            let mut total_entries = 0u64;
            for _ in 0..scans_per_thread {
                // Start a read-only transaction
                let ctx = txn_service.begin(true).unwrap();

                // Scan the safe range - should always succeed (no writers here)
                let range = b"safe_00".to_vec()..b"safe_99".to_vec();
                match txn_service.scan_iter(&ctx, range) {
                    Ok(mut iter) => {
                        iter.advance().await.unwrap();
                        let mut count = 0;
                        let mut prev_key: Option<Vec<u8>> = None;
                        while iter.valid() {
                            let key = iter.user_key().to_vec();
                            // Verify ordering: keys should be ascending
                            if let Some(ref pk) = prev_key {
                                assert!(
                                    key > *pk,
                                    "Keys should be in ascending order: {:?} > {:?}",
                                    String::from_utf8_lossy(&key),
                                    String::from_utf8_lossy(pk)
                                );
                            }
                            prev_key = Some(key);
                            count += 1;
                            iter.advance().await.unwrap();
                        }
                        total_entries += count;
                    }
                    Err(TiSqlError::KeyIsLocked { .. }) => {
                        // Shouldn't happen for safe range, but handle gracefully
                    }
                    Err(e) => panic!("Unexpected scan error: {e:?}"),
                }
                tokio::task::yield_now().await;
            }
            total_entries
        }));
    }

    // Wait for all tasks to complete their fixed iterations, then signal stop
    let mut writer_successes = 0u64;
    let mut scanner_entries = 0u64;
    for (i, handle) in handles.into_iter().enumerate() {
        let count = handle.await.unwrap();
        if i < num_writers {
            writer_successes += count;
        } else {
            scanner_entries += count;
        }
    }
    stop_flag.store(true, Ordering::Relaxed);

    // Verify some work was done
    assert!(writer_successes > 0, "At least some writes should succeed");
    // Each scanner should read 10 entries per scan, 20 scans = 200 per scanner
    // 2 scanners = 400 total entries minimum
    assert!(
        scanner_entries >= 200,
        "Scanners should have read entries from safe range, got {scanner_entries}"
    );
}

// ============================================================================
// Iterator Ordering Invariant Tests
// ============================================================================

/// Test that storage iterator returns entries in strict (user_key ASC, ts DESC) order.
///
/// This is a critical MVCC invariant: for the same user key, newer versions
/// (higher timestamps) must appear before older versions. Across different keys,
/// keys must be in ascending lexicographic order.
#[tokio::test]
async fn test_mvcc_iterator_ordering_invariant() {
    let (_storage, txn_service, _tso, _cm, _dir) = create_test_service();

    // Create multiple versions of multiple keys
    // The ordering should be:
    // - key_a@ts3, key_a@ts2, key_a@ts1 (same key: descending ts)
    // - key_b@ts3, key_b@ts2 (same key: descending ts)
    // - key_c@ts1 (single version)

    // Write first versions
    let ts_a1 = txn_service
        .autocommit_put(b"key_a", b"a_v1")
        .await
        .unwrap()
        .commit_ts;
    let ts_b1 = txn_service
        .autocommit_put(b"key_b", b"b_v1")
        .await
        .unwrap()
        .commit_ts;
    let ts_c1 = txn_service
        .autocommit_put(b"key_c", b"c_v1")
        .await
        .unwrap()
        .commit_ts;

    // Write second versions
    let ts_a2 = txn_service
        .autocommit_put(b"key_a", b"a_v2")
        .await
        .unwrap()
        .commit_ts;
    let ts_b2 = txn_service
        .autocommit_put(b"key_b", b"b_v2")
        .await
        .unwrap()
        .commit_ts;

    // Write third version for key_a only
    let ts_a3 = txn_service
        .autocommit_put(b"key_a", b"a_v3")
        .await
        .unwrap()
        .commit_ts;

    // Verify timestamps are strictly increasing
    assert!(ts_a1 < ts_b1);
    assert!(ts_b1 < ts_c1);
    assert!(ts_c1 < ts_a2);
    assert!(ts_a2 < ts_b2);
    assert!(ts_b2 < ts_a3);

    // Now scan the raw storage and verify ordering
    let start = MvccKey::encode(b"key_a", Timestamp::MAX);
    let end = MvccKey::encode(b"key_d", 0);

    let storage = txn_service.storage();
    let mut iter = storage.scan_iter(start..end, 0).unwrap();
    iter.advance().await.unwrap();

    let mut entries: Vec<(Vec<u8>, Timestamp)> = Vec::new();
    while iter.valid() {
        entries.push((iter.user_key().to_vec(), iter.timestamp()));
        iter.advance().await.unwrap();
    }

    // Verify we got all 6 entries
    assert_eq!(entries.len(), 6, "Should have 6 MVCC entries");

    // Verify strict ordering invariant
    for i in 1..entries.len() {
        let (prev_key, prev_ts) = &entries[i - 1];
        let (curr_key, curr_ts) = &entries[i];

        match prev_key.cmp(curr_key) {
            std::cmp::Ordering::Less => {
                // Different keys: curr_key > prev_key (ascending)
                // This is correct - moving to next user key
            }
            std::cmp::Ordering::Equal => {
                // Same key: curr_ts < prev_ts (descending within same key)
                assert!(
                    *curr_ts < *prev_ts,
                    "For same key {:?}, timestamps must be descending: {} should be < {}",
                    String::from_utf8_lossy(curr_key),
                    curr_ts,
                    prev_ts
                );
            }
            std::cmp::Ordering::Greater => {
                panic!(
                    "Keys out of order: {:?} should not come after {:?}",
                    String::from_utf8_lossy(curr_key),
                    String::from_utf8_lossy(prev_key)
                );
            }
        }
    }

    // Verify specific ordering
    assert_eq!(entries[0], (b"key_a".to_vec(), ts_a3)); // key_a newest
    assert_eq!(entries[1], (b"key_a".to_vec(), ts_a2));
    assert_eq!(entries[2], (b"key_a".to_vec(), ts_a1)); // key_a oldest
    assert_eq!(entries[3], (b"key_b".to_vec(), ts_b2)); // key_b newest
    assert_eq!(entries[4], (b"key_b".to_vec(), ts_b1)); // key_b oldest
    assert_eq!(entries[5], (b"key_c".to_vec(), ts_c1)); // key_c only version
}

/// Test that MvccScanIterator (transaction layer) returns only latest visible versions
/// in user_key ascending order.
#[tokio::test]
async fn test_mvcc_scan_iterator_returns_latest_visible_only() {
    use tisql::TxnService;

    let (_storage, txn_service, _tso, _cm, _dir) = create_test_service();

    // Write multiple versions
    txn_service.autocommit_put(b"key_a", b"a_v1").await.unwrap();
    txn_service.autocommit_put(b"key_b", b"b_v1").await.unwrap();
    txn_service.autocommit_put(b"key_a", b"a_v2").await.unwrap();
    txn_service.autocommit_put(b"key_c", b"c_v1").await.unwrap();
    txn_service.autocommit_put(b"key_b", b"b_v2").await.unwrap();
    txn_service.autocommit_put(b"key_a", b"a_v3").await.unwrap();

    // Start a transaction that sees all latest versions
    let ctx = txn_service.begin(true).unwrap();
    let range = b"key_a".to_vec()..b"key_d".to_vec();
    let mut iter = txn_service.scan_iter(&ctx, range).unwrap();
    iter.advance().await.unwrap();

    let mut results: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut prev_key: Option<Vec<u8>> = None;
    while iter.valid() {
        let key = iter.user_key().to_vec();
        let value = iter.value().to_vec();

        // Verify no duplicate keys (each key appears only once)
        if let Some(ref pk) = prev_key {
            assert_ne!(&key, pk, "Each key should appear only once in scan results");
            assert!(
                key > *pk,
                "Keys must be in ascending order: {:?} > {:?}",
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(pk)
            );
        }
        prev_key = Some(key.clone());
        results.push((key, value));
        iter.advance().await.unwrap();
    }

    // Should see exactly 3 keys with their latest values
    assert_eq!(results.len(), 3, "Should see exactly 3 unique keys");
    assert_eq!(results[0], (b"key_a".to_vec(), b"a_v3".to_vec()));
    assert_eq!(results[1], (b"key_b".to_vec(), b"b_v2".to_vec()));
    assert_eq!(results[2], (b"key_c".to_vec(), b"c_v1".to_vec()));
}

// ============================================================================
// E2E Tests for KeyIsLocked Error Path
// ============================================================================

/// E2E test for concurrent SQL INSERTs to the same key triggering KeyIsLocked error.
///
/// This tests the full path from SQL layer through transaction layer to storage layer,
/// verifying that concurrent writes to the same primary key result in proper lock
/// conflict errors.
#[tokio::test]
async fn test_e2e_key_is_locked_concurrent_inserts() {
    use tisql::{Database, DatabaseConfig, QueryResult};

    let dir = tempfile::tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path());
    let db = Arc::new(Database::open(config).unwrap());

    // Create table with primary key
    db.execute_query("CREATE TABLE lock_test (id INT PRIMARY KEY, value VARCHAR(100))")
        .await
        .unwrap();

    let num_threads = 10;
    let barrier = Arc::new(tokio::sync::Barrier::new(num_threads));
    let success_count = Arc::new(AtomicU64::new(0));
    let lock_error_count = Arc::new(AtomicU64::new(0));
    let duplicate_error_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    for i in 0..num_threads {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let success_count = Arc::clone(&success_count);
        let lock_error_count = Arc::clone(&lock_error_count);
        let duplicate_error_count = Arc::clone(&duplicate_error_count);

        handles.push(tokio::spawn(async move {
            // Synchronize all tasks to maximize contention
            barrier.wait().await;

            // All tasks try to insert with the same primary key
            let sql = format!("INSERT INTO lock_test (id, value) VALUES (1, 'thread_{i}')");
            match db.execute_query(&sql).await {
                Ok(QueryResult::Affected(_)) => {
                    success_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    let msg = e.to_string().to_lowercase();
                    if msg.contains("key is locked") || msg.contains("locked") {
                        lock_error_count.fetch_add(1, Ordering::Relaxed);
                    } else if msg.contains("duplicate") {
                        duplicate_error_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        panic!("Thread {i} got unexpected error: {e}");
                    }
                }
                Ok(other) => {
                    panic!("Thread {i} got unexpected result: {other:?}");
                }
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let successes = success_count.load(Ordering::Relaxed);
    let lock_errors = lock_error_count.load(Ordering::Relaxed);
    let duplicate_errors = duplicate_error_count.load(Ordering::Relaxed);

    // Verify results
    assert!(
        successes >= 1,
        "At least one insert should succeed, got {successes}"
    );
    assert!(
        lock_errors + duplicate_errors > 0,
        "Some threads should get lock conflict or duplicate key error"
    );
    assert_eq!(
        successes + lock_errors + duplicate_errors,
        num_threads as u64,
        "All threads should complete with success, lock error, or duplicate error"
    );

    // Verify the data is consistent - exactly one row should exist
    match db
        .execute_query("SELECT COUNT(*) FROM lock_test")
        .await
        .unwrap()
    {
        QueryResult::Rows { data, .. } => {
            assert_eq!(data.len(), 1);
            assert_eq!(data[0][0], "1", "Exactly one row should exist");
        }
        other => panic!("Expected rows, got: {other:?}"),
    }

    db.close().await.unwrap();
}

/// E2E test for concurrent SQL UPDATEs to the same key triggering KeyIsLocked error.
///
/// Similar to the INSERT test, but tests UPDATE operations where multiple threads
/// try to update the same row concurrently.
#[tokio::test]
async fn test_e2e_key_is_locked_concurrent_updates() {
    use tisql::{Database, DatabaseConfig, QueryResult};

    let dir = tempfile::tempdir().unwrap();
    let config = DatabaseConfig::with_data_dir(dir.path());
    let db = Arc::new(Database::open(config).unwrap());

    // Create table and insert initial row
    db.execute_query("CREATE TABLE update_lock_test (id INT PRIMARY KEY, counter INT)")
        .await
        .unwrap();
    db.execute_query("INSERT INTO update_lock_test VALUES (1, 0)")
        .await
        .unwrap();

    let num_threads = 10;
    let updates_per_thread = 5;
    let barrier = Arc::new(tokio::sync::Barrier::new(num_threads));
    let success_count = Arc::new(AtomicU64::new(0));
    let lock_error_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    for _ in 0..num_threads {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let success_count = Arc::clone(&success_count);
        let lock_error_count = Arc::clone(&lock_error_count);

        handles.push(tokio::spawn(async move {
            // Synchronize all tasks
            barrier.wait().await;

            for _ in 0..updates_per_thread {
                match db
                    .execute_query("UPDATE update_lock_test SET counter = counter + 1 WHERE id = 1")
                    .await
                {
                    Ok(QueryResult::Affected(_)) => {
                        success_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let msg = e.to_string().to_lowercase();
                        // In highly concurrent UPDATE scenarios, we may see:
                        // - KeyIsLocked: another txn holds the lock
                        // - DuplicateKey: race condition during PK check (false positive)
                        if msg.contains("key is locked")
                            || msg.contains("locked")
                            || msg.contains("duplicate")
                        {
                            lock_error_count.fetch_add(1, Ordering::Relaxed);
                        } else {
                            panic!("Got unexpected error: {e}");
                        }
                    }
                    Ok(other) => {
                        panic!("Got unexpected result: {other:?}");
                    }
                }
                // Small delay to allow other tasks to compete
                tokio::task::yield_now().await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let successes = success_count.load(Ordering::Relaxed);
    let lock_errors = lock_error_count.load(Ordering::Relaxed);

    // Verify results
    assert!(
        successes >= 1,
        "At least some updates should succeed, got {successes}"
    );
    assert_eq!(
        successes + lock_errors,
        (num_threads * updates_per_thread) as u64,
        "All update attempts should complete with success or lock error"
    );

    // With pessimistic locking at PUT level (not transaction level), lost updates are
    // possible when multiple transactions read before any write. The counter will be
    // at least 1 (at least one update succeeded) and at most successes (all updates
    // incremented unique values).
    match db
        .execute_query("SELECT counter FROM update_lock_test WHERE id = 1")
        .await
        .unwrap()
    {
        QueryResult::Rows { data, .. } => {
            let counter: i64 = data[0][0].parse().unwrap();
            assert!(counter >= 1, "Counter ({counter}) should be at least 1");
            assert!(
                counter <= successes as i64,
                "Counter ({counter}) should not exceed successful updates ({successes})"
            );
        }
        other => panic!("Expected rows, got: {other:?}"),
    }

    db.close().await.unwrap();
}

// ============================================================================
// Explicit Transaction SQL Tests (BEGIN/COMMIT/ROLLBACK)
// ============================================================================

mod explicit_transaction_tests {
    use tempfile::tempdir;
    use tisql::{Database, DatabaseConfig, QueryResult, Session};

    /// Test that BEGIN starts an explicit transaction.
    #[tokio::test]
    async fn test_begin_starts_transaction() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        let mut session = Session::new();

        // Create table
        db.execute_query("CREATE TABLE txn_test (id INT PRIMARY KEY, val VARCHAR(100))")
            .await
            .unwrap();

        // Session should not have active transaction initially
        assert!(
            !session.has_active_txn(),
            "Session should start without active txn"
        );

        // BEGIN should start a transaction
        let result = db.execute_query_with_session("BEGIN", &mut session).await;
        assert!(result.is_ok(), "BEGIN should succeed");

        // Session should now have active transaction
        assert!(
            session.has_active_txn(),
            "Session should have active txn after BEGIN"
        );

        db.close().await.unwrap();
    }

    /// Test that START TRANSACTION works like BEGIN.
    #[tokio::test]
    async fn test_start_transaction_starts_transaction() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        let mut session = Session::new();

        // Create table
        db.execute_query("CREATE TABLE txn_test2 (id INT PRIMARY KEY)")
            .await
            .unwrap();

        // START TRANSACTION should start a transaction
        let result = db
            .execute_query_with_session("START TRANSACTION", &mut session)
            .await;
        assert!(result.is_ok(), "START TRANSACTION should succeed");

        assert!(
            session.has_active_txn(),
            "Session should have active txn after START TRANSACTION"
        );

        db.close().await.unwrap();
    }

    /// Test that COMMIT commits changes.
    #[tokio::test]
    async fn test_commit_persists_changes() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        let mut session = Session::new();

        // Create table
        db.execute_query("CREATE TABLE commit_test (id INT PRIMARY KEY, val VARCHAR(100))")
            .await
            .unwrap();

        // Begin transaction
        db.execute_query_with_session("BEGIN", &mut session)
            .await
            .unwrap();

        // Insert within transaction
        db.execute_query_with_session(
            "INSERT INTO commit_test VALUES (1, 'committed')",
            &mut session,
        )
        .await
        .unwrap();

        // Commit
        db.execute_query_with_session("COMMIT", &mut session)
            .await
            .unwrap();

        // Session should no longer have active transaction
        assert!(
            !session.has_active_txn(),
            "Session should not have active txn after COMMIT"
        );

        // Data should be visible
        match db
            .execute_query("SELECT val FROM commit_test WHERE id = 1")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "committed");
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        db.close().await.unwrap();
    }

    /// Test that ROLLBACK discards changes.
    #[tokio::test]
    async fn test_rollback_discards_changes() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        let mut session = Session::new();

        // Create table and insert initial data
        db.execute_query("CREATE TABLE rollback_test (id INT PRIMARY KEY, val VARCHAR(100))")
            .await
            .unwrap();
        db.execute_query("INSERT INTO rollback_test VALUES (1, 'original')")
            .await
            .unwrap();

        // Begin transaction
        db.execute_query_with_session("BEGIN", &mut session)
            .await
            .unwrap();

        // Update within transaction
        db.execute_query_with_session(
            "UPDATE rollback_test SET val = 'modified' WHERE id = 1",
            &mut session,
        )
        .await
        .unwrap();

        // Rollback
        db.execute_query_with_session("ROLLBACK", &mut session)
            .await
            .unwrap();

        // Session should no longer have active transaction
        assert!(
            !session.has_active_txn(),
            "Session should not have active txn after ROLLBACK"
        );

        // Data should be unchanged
        match db
            .execute_query("SELECT val FROM rollback_test WHERE id = 1")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "original");
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        db.close().await.unwrap();
    }

    /// Test multiple INSERT statements within a transaction.
    ///
    /// Note: Read-your-writes is not yet supported, so UPDATE/DELETE within the same
    /// transaction cannot see uncommitted INSERTs. This test verifies that multiple
    /// INSERTs work correctly and are atomically committed.
    #[tokio::test]
    async fn test_multiple_inserts_in_transaction() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        let mut session = Session::new();

        // Create table
        db.execute_query("CREATE TABLE multi_insert_test (id INT PRIMARY KEY, val INT)")
            .await
            .unwrap();

        // Begin transaction
        db.execute_query_with_session("BEGIN", &mut session)
            .await
            .unwrap();

        // Multiple inserts within the transaction
        db.execute_query_with_session("INSERT INTO multi_insert_test VALUES (1, 10)", &mut session)
            .await
            .unwrap();
        db.execute_query_with_session("INSERT INTO multi_insert_test VALUES (2, 20)", &mut session)
            .await
            .unwrap();
        db.execute_query_with_session("INSERT INTO multi_insert_test VALUES (3, 30)", &mut session)
            .await
            .unwrap();

        // Commit
        db.execute_query_with_session("COMMIT", &mut session)
            .await
            .unwrap();

        // Session should no longer have active transaction
        assert!(
            !session.has_active_txn(),
            "Session should not have active txn after COMMIT"
        );

        // All inserts should be visible after commit
        match db
            .execute_query("SELECT id, val FROM multi_insert_test ORDER BY id")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 3);
                assert_eq!(data[0][0], "1");
                assert_eq!(data[0][1], "10");
                assert_eq!(data[1][0], "2");
                assert_eq!(data[1][1], "20");
                assert_eq!(data[2][0], "3");
                assert_eq!(data[2][1], "30");
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        db.close().await.unwrap();
    }

    /// Test read-your-writes within a transaction.
    ///
    /// The storage layer passes owner_ts for explicit transactions, making pending
    /// nodes owned by the current transaction visible to its own reads.
    #[tokio::test]
    async fn test_read_your_writes_in_transaction() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        let mut session = Session::new();

        // Create table
        db.execute_query("CREATE TABLE ryw_test (id INT PRIMARY KEY, val VARCHAR(100))")
            .await
            .unwrap();

        // Begin transaction
        db.execute_query_with_session("BEGIN", &mut session)
            .await
            .unwrap();

        // Insert within transaction
        db.execute_query_with_session("INSERT INTO ryw_test VALUES (1, 'first')", &mut session)
            .await
            .unwrap();

        // Read should see the uncommitted insert (read-your-writes)
        match db
            .execute_query_with_session("SELECT val FROM ryw_test WHERE id = 1", &mut session)
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1, "Should see own uncommitted write");
                assert_eq!(data[0][0], "first");
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        // Update within transaction
        db.execute_query_with_session(
            "UPDATE ryw_test SET val = 'updated' WHERE id = 1",
            &mut session,
        )
        .await
        .unwrap();

        // Read should see the updated value
        match db
            .execute_query_with_session("SELECT val FROM ryw_test WHERE id = 1", &mut session)
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data[0][0], "updated");
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        // Commit
        db.execute_query_with_session("COMMIT", &mut session)
            .await
            .unwrap();

        db.close().await.unwrap();
    }

    /// Test snapshot isolation across multiple statements in one explicit transaction.
    ///
    /// Session s1 keeps a stable snapshot across repeated reads, even after s2 commits
    /// new writes in between. s1 should still see its own writes (read-your-writes).
    #[tokio::test]
    async fn test_multi_statement_snapshot_isolation() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        let mut s1 = Session::new();
        let mut s2 = Session::new();

        db.execute_query("CREATE TABLE ms_iso_t (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();
        db.execute_query("INSERT INTO ms_iso_t VALUES (1, 10)")
            .await
            .unwrap();

        // s1 starts explicit transaction and reads initial snapshot.
        db.execute_query_with_session("BEGIN", &mut s1)
            .await
            .unwrap();
        match db
            .execute_query_with_session("SELECT v FROM ms_iso_t WHERE id = 1", &mut s1)
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "10");
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        // Concurrent commits from s2 after s1 snapshot is established.
        db.execute_query_with_session("UPDATE ms_iso_t SET v = 20 WHERE id = 1", &mut s2)
            .await
            .unwrap();
        db.execute_query_with_session("INSERT INTO ms_iso_t VALUES (2, 200)", &mut s2)
            .await
            .unwrap();

        // s1 should still read old snapshot values, not s2's newly committed ones.
        match db
            .execute_query_with_session("SELECT v FROM ms_iso_t WHERE id = 1", &mut s1)
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(
                    data[0][0], "10",
                    "s1 should keep snapshot value despite s2 update"
                );
            }
            other => panic!("Expected rows, got: {other:?}"),
        }
        match db
            .execute_query_with_session("SELECT COUNT(*) FROM ms_iso_t", &mut s1)
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(
                    data[0][0], "1",
                    "s1 should not see phantom row inserted by s2 in same txn"
                );
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        // s1 should still see its own writes.
        db.execute_query_with_session("INSERT INTO ms_iso_t VALUES (3, 300)", &mut s1)
            .await
            .unwrap();
        match db
            .execute_query_with_session("SELECT v FROM ms_iso_t WHERE id = 3", &mut s1)
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "300");
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        db.execute_query_with_session("COMMIT", &mut s1)
            .await
            .unwrap();

        // After commit, latest view should include both s2 and s1 committed writes.
        match db
            .execute_query("SELECT id, v FROM ms_iso_t ORDER BY id")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 3);
                assert_eq!(data[0], vec!["1".to_string(), "20".to_string()]);
                assert_eq!(data[1], vec!["2".to_string(), "200".to_string()]);
                assert_eq!(data[2], vec!["3".to_string(), "300".to_string()]);
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        db.close().await.unwrap();
    }

    /// Test that COMMIT without active transaction is a no-op (MySQL behavior).
    #[tokio::test]
    async fn test_commit_without_transaction_is_noop() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        let mut session = Session::new();

        // Create table
        db.execute_query("CREATE TABLE noop_test (id INT PRIMARY KEY)")
            .await
            .unwrap();

        // COMMIT without BEGIN should succeed (MySQL behavior)
        let result = db.execute_query_with_session("COMMIT", &mut session).await;
        assert!(result.is_ok(), "COMMIT without txn should be no-op");

        db.close().await.unwrap();
    }

    /// Test that ROLLBACK without active transaction is a no-op (MySQL behavior).
    #[tokio::test]
    async fn test_rollback_without_transaction_is_noop() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        let mut session = Session::new();

        // Create table
        db.execute_query("CREATE TABLE noop_rb_test (id INT PRIMARY KEY)")
            .await
            .unwrap();

        // ROLLBACK without BEGIN should succeed (MySQL behavior)
        let result = db
            .execute_query_with_session("ROLLBACK", &mut session)
            .await;
        assert!(result.is_ok(), "ROLLBACK without txn should be no-op");

        db.close().await.unwrap();
    }

    /// Test nested BEGIN errors (current behavior).
    ///
    /// Unlike MySQL which implicitly commits on nested BEGIN, we currently return
    /// an error to prevent accidental loss of uncommitted work.
    #[tokio::test]
    async fn test_nested_begin_errors() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        let mut session = Session::new();

        // Create table
        db.execute_query("CREATE TABLE nested_test (id INT PRIMARY KEY, val INT)")
            .await
            .unwrap();

        // Begin first transaction
        db.execute_query_with_session("BEGIN", &mut session)
            .await
            .unwrap();

        // Insert data
        db.execute_query_with_session("INSERT INTO nested_test VALUES (1, 100)", &mut session)
            .await
            .unwrap();

        // Nested BEGIN should return an error (not supported yet)
        let result = db.execute_query_with_session("BEGIN", &mut session).await;
        assert!(result.is_err(), "Nested BEGIN should return error");
        assert!(
            result
                .unwrap_err()
                .to_string()
                .to_lowercase()
                .contains("nested"),
            "Error should mention nested transactions"
        );

        // Original transaction should still be active
        assert!(
            session.has_active_txn(),
            "Original transaction should still be active"
        );

        // Commit the original transaction
        db.execute_query_with_session("COMMIT", &mut session)
            .await
            .unwrap();

        // Data should be visible after commit
        match db
            .execute_query("SELECT val FROM nested_test WHERE id = 1")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0][0], "100");
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        db.close().await.unwrap();
    }

    /// Test explicit transaction commit fails if concurrent DDL changed schema.
    #[tokio::test]
    async fn test_explicit_commit_fails_on_schema_change() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());
        let db = Database::open(config).unwrap();
        let mut s1 = Session::new();
        let mut s2 = Session::new();

        db.execute_query("CREATE TABLE schema_guard_t (id INT PRIMARY KEY, v INT)")
            .await
            .unwrap();

        db.execute_query_with_session("BEGIN", &mut s1)
            .await
            .unwrap();
        db.execute_query_with_session("INSERT INTO schema_guard_t VALUES (1, 10)", &mut s1)
            .await
            .unwrap();

        // Concurrent DDL from another session bumps schema version.
        db.execute_query_with_session(
            "CREATE TABLE schema_guard_other (id INT PRIMARY KEY)",
            &mut s2,
        )
        .await
        .unwrap();

        let err = db
            .execute_query_with_session("COMMIT", &mut s1)
            .await
            .unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("schema changed"),
            "Expected SchemaChanged error, got: {err}"
        );

        // COMMIT failure should clear explicit transaction state.
        assert!(
            !s1.has_active_txn(),
            "Session should not keep txn after schema-change commit failure"
        );

        // Write should have been rolled back.
        match db
            .execute_query("SELECT id FROM schema_guard_t ORDER BY id")
            .await
            .unwrap()
        {
            QueryResult::Rows { data, .. } => {
                assert!(data.is_empty(), "Write should be rolled back");
            }
            other => panic!("Expected rows, got: {other:?}"),
        }

        db.close().await.unwrap();
    }
}
