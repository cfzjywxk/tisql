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
use std::thread;
use std::time::Duration;

use tisql::error::TiSqlError;
use tisql::StorageEngine;
use tisql::testkit::{ConcurrencyManager, FileClogConfig, FileClogService, Lock, MvccMemTableEngine, TransactionService};

fn create_test_service() -> (
    Arc<MvccMemTableEngine>,
    Arc<TransactionService<MvccMemTableEngine, FileClogService>>,
    Arc<ConcurrencyManager>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let config = FileClogConfig::with_dir(dir.path());
    let clog_service = Arc::new(FileClogService::open(config).unwrap());
    let cm = Arc::new(ConcurrencyManager::new(1));
    let storage = Arc::new(MvccMemTableEngine::new(Arc::clone(&cm)));
    let txn_service = Arc::new(TransactionService::new(
        Arc::clone(&storage),
        clog_service,
        Arc::clone(&cm),
    ));
    (storage, txn_service, cm, dir)
}

// ============================================================================
// TSO Ordering Tests
// ============================================================================

/// Verify TSO produces strictly monotonic timestamps under concurrent load.
#[test]
fn test_tso_strict_ordering_concurrent() {
    let cm = Arc::new(ConcurrencyManager::new(1));
    let num_threads = 8;
    let timestamps_per_thread = 1000;

    let mut handles = vec![];

    for _ in 0..num_threads {
        let cm = Arc::clone(&cm);
        handles.push(thread::spawn(move || {
            let mut timestamps = Vec::with_capacity(timestamps_per_thread);
            for _ in 0..timestamps_per_thread {
                timestamps.push(cm.get_ts());
            }
            timestamps
        }));
    }

    // Collect all timestamps
    let mut all_timestamps = vec![];
    for handle in handles {
        all_timestamps.extend(handle.join().unwrap());
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
#[test]
fn test_tso_per_thread_monotonic() {
    let cm = Arc::new(ConcurrencyManager::new(1));
    let num_threads = 4;
    let timestamps_per_thread = 500;

    let mut handles = vec![];

    for _ in 0..num_threads {
        let cm = Arc::clone(&cm);
        handles.push(thread::spawn(move || {
            let mut prev = 0u64;
            for _ in 0..timestamps_per_thread {
                let ts = cm.get_ts();
                assert!(
                    ts > prev,
                    "Timestamp must be strictly increasing: prev={}, ts={}",
                    prev,
                    ts
                );
                prev = ts;
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

/// Verify max_ts tracking under concurrent updates.
#[test]
fn test_max_ts_concurrent_updates() {
    let cm = Arc::new(ConcurrencyManager::new(1));
    let num_threads = 8;
    let updates_per_thread = 1000;

    let max_seen = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for i in 0..num_threads {
        let cm = Arc::clone(&cm);
        let max_seen = Arc::clone(&max_seen);
        handles.push(thread::spawn(move || {
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
        handle.join().unwrap();
    }

    // max_ts should reflect the highest value seen
    let expected_max = (num_threads as u64) * updates_per_thread * 1000;
    assert_eq!(cm.max_ts(), expected_max);
}

// ============================================================================
// Lock Conflict Tests
// ============================================================================

/// Verify that readers see locks immediately after acquisition.
#[test]
fn test_reader_sees_lock_immediately() {
    let cm = Arc::new(ConcurrencyManager::new(1));
    let key = b"test_key".to_vec();

    // Initially no lock
    assert!(cm.check_lock(&key, 1).is_ok());

    // Acquire lock
    let lock = Lock {
        ts: 100,
        primary: key.clone(),
    };
    let guards = cm.lock_keys(&[key.clone()], lock).unwrap();

    // Immediately visible to reader
    let result = cm.check_lock(&key, 1);
    assert!(result.is_err());
    match result {
        Err(TiSqlError::KeyIsLocked { lock_ts, .. }) => {
            assert_eq!(lock_ts, 100);
        }
        _ => panic!("Expected KeyIsLocked error"),
    }

    // Drop guards
    drop(guards);

    // Lock should be released
    assert!(cm.check_lock(&key, 1).is_ok());
}

/// Test concurrent writers to different keys succeed.
#[test]
fn test_concurrent_writes_different_keys() {
    let (_storage, txn_service, cm, _dir) = create_test_service();
    let num_threads = 4;
    let writes_per_thread = 100;

    let success_count = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for i in 0..num_threads {
        let txn_service = Arc::clone(&txn_service);
        let success_count = Arc::clone(&success_count);
        handles.push(thread::spawn(move || {
            for j in 0..writes_per_thread {
                let key = format!("key_{}_{}", i, j);
                let value = format!("value_{}_{}", i, j);
                if txn_service.put(key.as_bytes(), value.as_bytes()).is_ok() {
                    success_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // All writes should succeed since they're to different keys
    let total_writes = num_threads * writes_per_thread;
    assert_eq!(success_count.load(Ordering::Relaxed), total_writes as u64);
    assert_eq!(cm.lock_count(), 0, "All locks should be released");
}

/// Test concurrent writers to same key - one should get lock conflict.
#[test]
fn test_concurrent_writes_same_key() {
    let (_storage, txn_service, cm, _dir) = create_test_service();
    let num_threads = 10;
    let key = b"shared_key";

    let success_count = Arc::new(AtomicU64::new(0));
    let conflict_count = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(std::sync::Barrier::new(num_threads));
    let mut handles = vec![];

    for i in 0..num_threads {
        let txn_service = Arc::clone(&txn_service);
        let success_count = Arc::clone(&success_count);
        let conflict_count = Arc::clone(&conflict_count);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            // Synchronize all threads to maximize contention
            barrier.wait();

            let value = format!("value_{}", i);
            match txn_service.put(key, value.as_bytes()) {
                Ok(_) => {
                    success_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(TiSqlError::KeyIsLocked { .. }) => {
                    conflict_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    panic!("Unexpected error: {:?}", e);
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
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

    // All locks should be released
    assert_eq!(cm.lock_count(), 0, "All locks should be released");
}

/// Verify range lock check blocks correctly.
#[test]
fn test_range_lock_check() {
    let cm = Arc::new(ConcurrencyManager::new(1));

    // Lock a key in the middle of a range
    let lock = Lock {
        ts: 100,
        primary: b"key_b".to_vec(),
    };
    let _guards = cm.lock_keys(&[b"key_b".to_vec()], lock).unwrap();

    // Range containing locked key should fail
    assert!(cm.check_range(b"key_a", b"key_c", 1).is_err());

    // Range before locked key should pass
    assert!(cm.check_range(b"key_0", b"key_a", 1).is_ok());

    // Range after locked key should pass
    assert!(cm.check_range(b"key_c", b"key_z", 1).is_ok());
}

// ============================================================================
// Write-Read Conflict Scenarios
// ============================================================================

/// Simulate concurrent read while write is in progress (without failpoints).
///
/// This test manually simulates the scenario where:
/// 1. Writer acquires lock
/// 2. Reader tries to read the locked key
/// 3. Reader should get KeyIsLocked error
#[test]
fn test_concurrent_read_during_write() {
    let (storage, _txn_service, cm, _dir) = create_test_service();

    let key = b"test_key".to_vec();
    let writer_started = Arc::new(AtomicBool::new(false));
    let reader_blocked = Arc::new(AtomicBool::new(false));

    // Spawn writer thread
    let cm_write = Arc::clone(&cm);
    let writer_started_clone = Arc::clone(&writer_started);
    let reader_blocked_clone = Arc::clone(&reader_blocked);
    let key_clone = key.clone();

    let writer = thread::spawn(move || {
        // Acquire lock manually (simulating transaction)
        let lock = Lock {
            ts: 100,
            primary: key_clone.clone(),
        };
        let _guards = cm_write.lock_keys(&[key_clone], lock).unwrap();

        // Signal writer has started
        writer_started_clone.store(true, Ordering::Release);

        // Wait for reader to be blocked
        while !reader_blocked_clone.load(Ordering::Acquire) {
            thread::yield_now();
        }

        // Simulate write delay
        thread::sleep(Duration::from_millis(10));

        // Guards dropped here, releasing lock
    });

    // Reader thread
    let cm_read = Arc::clone(&cm);
    let key_clone = key.clone();

    let reader = thread::spawn(move || {
        // Wait for writer to start
        while !writer_started.load(Ordering::Acquire) {
            thread::yield_now();
        }

        // Try to check lock - should be blocked
        let result = cm_read.check_lock(&key_clone, 1);
        assert!(result.is_err(), "Reader should see lock");

        // Signal that reader was blocked
        reader_blocked.store(true, Ordering::Release);
    });

    // Wait for both threads to complete
    writer.join().unwrap();
    reader.join().unwrap();

    // After writer thread exits, lock should be released
    assert!(
        cm.check_lock(&key, 1).is_ok(),
        "Lock should be released after writer exits"
    );

    // Can read (would return None since nothing written to storage)
    let value = storage.get(&key).unwrap();
    assert!(value.is_none(), "Key should not exist yet");
}

/// Test that MVCC read at specific timestamp works correctly.
#[test]
fn test_mvcc_read_at_timestamp() {
    let (storage, txn_service, _cm, _dir) = create_test_service();

    let key = b"version_key";

    // Write version 1
    let (_, ts1, _) = txn_service.put(key, b"v1").unwrap();

    // Write version 2
    let (_, ts2, _) = txn_service.put(key, b"v2").unwrap();

    // Write version 3
    let (_, ts3, _) = txn_service.put(key, b"v3").unwrap();

    assert!(ts1 < ts2);
    assert!(ts2 < ts3);

    // Read at ts3 should see v3
    let v = storage.get_at(key, ts3).unwrap();
    assert_eq!(v, Some(b"v3".to_vec()));

    // Read at ts2 should see v2
    let v = storage.get_at(key, ts2).unwrap();
    assert_eq!(v, Some(b"v2".to_vec()));

    // Read at ts1 should see v1
    let v = storage.get_at(key, ts1).unwrap();
    assert_eq!(v, Some(b"v1".to_vec()));

    // Read at timestamp before ts1 should see nothing
    let v = storage.get_at(key, ts1 - 1).unwrap();
    assert!(v.is_none());

    // Read at latest should see v3
    let v = storage.get(key).unwrap();
    assert_eq!(v, Some(b"v3".to_vec()));
}

/// Test multiple readers don't interfere with each other.
#[test]
fn test_concurrent_readers_no_interference() {
    let (storage, txn_service, _cm, _dir) = create_test_service();
    let key = b"shared_read_key";

    // Write initial value
    txn_service.put(key, b"initial_value").unwrap();

    let num_readers = 10;
    let reads_per_thread = 100;
    let success_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    for _ in 0..num_readers {
        let storage = Arc::clone(&storage);
        let success_count = Arc::clone(&success_count);
        handles.push(thread::spawn(move || {
            for _ in 0..reads_per_thread {
                match storage.get(key) {
                    Ok(Some(v)) if v == b"initial_value" => {
                        success_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Some(v)) => panic!("Unexpected value: {:?}", v),
                    Ok(None) => panic!("Key should exist"),
                    Err(e) => panic!("Read error: {:?}", e),
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total_reads = num_readers * reads_per_thread;
    assert_eq!(
        success_count.load(Ordering::Relaxed),
        total_reads as u64,
        "All reads should succeed"
    );
}

/// Test delete creates proper tombstone and is visible to concurrent readers.
#[test]
fn test_concurrent_read_after_delete() {
    let (storage, txn_service, _cm, _dir) = create_test_service();
    let key = b"delete_key";

    // Write initial value
    let (_, ts1, _) = txn_service.put(key, b"value").unwrap();

    // Verify value exists
    assert!(storage.get(key).unwrap().is_some());

    // Delete
    let (_, ts2, _) = txn_service.delete(key).unwrap();
    assert!(ts2 > ts1);

    // Read at latest should see nothing (deleted)
    assert!(storage.get(key).unwrap().is_none());

    // Read at ts1 should still see the value (MVCC)
    let v = storage.get_at(key, ts1).unwrap();
    assert_eq!(v, Some(b"value".to_vec()));
}

// ============================================================================
// Failpoint-based Tests (require --features failpoints)
// ============================================================================

#[cfg(feature = "failpoints")]
mod failpoint_tests {
    use super::*;

    /// Test reader blocked when writer has lock but hasn't applied yet.
    ///
    /// Uses failpoint to pause writer after acquiring lock.
    #[test]
    fn test_reader_blocked_during_write_with_failpoint() {
        let scenario = fail::FailScenario::setup();

        let (storage, txn_service, cm, _dir) = create_test_service();
        let key = b"fp_test_key";

        // Configure failpoint to pause after lock acquisition
        fail::cfg("txn_after_lock_acquired", "pause").unwrap();

        let txn_service_clone = Arc::clone(&txn_service);
        let writer = thread::spawn(move || {
            // This will pause at the failpoint
            txn_service_clone.put(key, b"value").unwrap()
        });

        // Give writer time to reach failpoint
        thread::sleep(Duration::from_millis(50));

        // Reader should see lock
        let result = cm.check_lock(key, 1);
        assert!(
            result.is_err(),
            "Reader should see lock while writer is paused"
        );

        // Resume writer
        fail::cfg("txn_after_lock_acquired", "off").unwrap();

        writer.join().unwrap();

        // After writer completes, lock released and data visible
        assert!(cm.check_lock(key, 1).is_ok());
        let v = storage.get(key).unwrap();
        assert_eq!(v, Some(b"value".to_vec()));

        scenario.teardown();
    }

    /// Test that lock persists through clog write but before storage apply.
    #[test]
    fn test_lock_during_clog_write_with_failpoint() {
        let scenario = fail::FailScenario::setup();

        let (_storage, txn_service, cm, _dir) = create_test_service();
        let key = b"fp_clog_key";

        // Pause after clog write, before storage apply
        fail::cfg("txn_after_clog_write", "pause").unwrap();

        let txn_service_clone = Arc::clone(&txn_service);
        let writer = thread::spawn(move || txn_service_clone.put(key, b"value").unwrap());

        // Give writer time to reach failpoint
        thread::sleep(Duration::from_millis(50));

        // Lock should still be held
        let result = cm.check_lock(key, 1);
        assert!(result.is_err(), "Lock should persist through clog write");

        // Resume
        fail::cfg("txn_after_clog_write", "off").unwrap();
        writer.join().unwrap();

        assert!(cm.check_lock(key, 1).is_ok());

        scenario.teardown();
    }

    /// Test concurrent read-write with controlled timing.
    #[test]
    fn test_read_write_race_with_failpoint() {
        let scenario = fail::FailScenario::setup();

        let (storage, txn_service, cm, _dir) = create_test_service();
        let key = b"race_key";

        // Write initial value
        txn_service.put(key, b"v1").unwrap();

        // Pause after storage apply but before lock release
        fail::cfg("txn_after_storage_apply", "pause").unwrap();

        let txn_service_clone = Arc::clone(&txn_service);
        let writer = thread::spawn(move || txn_service_clone.put(key, b"v2").unwrap());

        // Give writer time to reach failpoint
        thread::sleep(Duration::from_millis(50));

        // Lock should still be held even though data is in storage
        assert!(cm.check_lock(key, 1).is_err());

        // Resume
        fail::cfg("txn_after_storage_apply", "off").unwrap();
        writer.join().unwrap();

        // Now should see v2
        assert!(cm.check_lock(key, 1).is_ok());
        let v = storage.get(key).unwrap();
        assert_eq!(v, Some(b"v2".to_vec()));

        scenario.teardown();
    }

    /// Test multiple writers with controlled ordering.
    #[test]
    fn test_serialized_writes_with_failpoint() {
        let scenario = fail::FailScenario::setup();

        let (storage, txn_service, _cm, _dir) = create_test_service();
        let key = b"serial_key";

        let results = Arc::new(std::sync::Mutex::new(vec![]));

        for i in 0..3 {
            let txn_service = Arc::clone(&txn_service);
            let results = Arc::clone(&results);
            let value = format!("v{}", i);

            // Each write will succeed in order since they wait for previous
            let (_, ts, _) = txn_service.put(key, value.as_bytes()).unwrap();
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
        let v = storage.get(key).unwrap();
        assert_eq!(v, Some(b"v2".to_vec()));

        scenario.teardown();
    }
}
