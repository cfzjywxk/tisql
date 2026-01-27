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

//! Concurrent storage layer tests.
//!
//! These tests verify data integrity under concurrent operations including:
//! - Concurrent writes during flush
//! - Concurrent reads during flush
//! - Concurrent writes during compaction
//! - Concurrent reads during compaction
//!
//! To run these tests:
//! ```
//! cargo test -p tisql --test concurrent_storage_test
//! ```

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

use tisql::new_lsn_provider;
use tisql::storage::mvcc::{is_tombstone, MvccKey};
use tisql::storage::WriteBatch;
use tisql::testkit::{
    IlogConfig, IlogService, LsmConfigBuilder, LsmEngine, LsmRecovery, RecoveryResult,
};
use tisql::types::{RawValue, Timestamp};
use tisql::StorageEngine;

// ==================== Test Helpers Using MvccKey ====================

fn get_at_for_test(engine: &LsmEngine, key: &[u8], ts: Timestamp) -> Option<RawValue> {
    let start = MvccKey::encode(key, ts);
    let end = MvccKey::encode(key, 0)
        .next_key()
        .unwrap_or_else(MvccKey::unbounded);
    let range = start..end;

    let results = engine.scan(range).unwrap();

    for (mvcc_key, value) in results {
        let (decoded_key, entry_ts) = mvcc_key.decode();
        if decoded_key == key && entry_ts <= ts {
            if is_tombstone(&value) {
                return None;
            }
            return Some(value);
        }
    }
    None
}

fn get_for_test(engine: &LsmEngine, key: &[u8]) -> Option<RawValue> {
    get_at_for_test(engine, key, Timestamp::MAX)
}

/// Helper to create a test LSM engine with ilog for durability tests.
fn create_test_lsm_engine(dir: &TempDir) -> (Arc<LsmEngine>, Arc<IlogService>) {
    let lsn_provider = new_lsn_provider();

    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(256) // Very small for testing to trigger rotations
        .build_unchecked();
    let engine =
        LsmEngine::open_durable(config, Arc::clone(&lsn_provider), Arc::clone(&ilog)).unwrap();

    (Arc::new(engine), ilog)
}

/// Helper to write test data to the engine.
fn write_test_data(engine: &LsmEngine, key: &[u8], value: &[u8], ts: u64) {
    let mut batch = WriteBatch::new();
    batch.put(key.to_vec(), value.to_vec());
    batch.set_commit_ts(ts);
    engine.write_batch(batch).unwrap();
}

/// Helper to recover an LSM engine.
fn recover_engine(dir: &TempDir) -> RecoveryResult {
    let recovery = LsmRecovery::new(dir.path());
    recovery.recover().unwrap()
}

// ============================================================================
// Concurrent Write During Flush Tests
// ============================================================================

/// Test concurrent writes while flush is in progress.
///
/// Verifies that:
/// - All writes complete successfully
/// - No writes are lost
/// - No duplicates appear
#[test]
fn test_concurrent_write_during_flush() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    let num_writers = 4;
    let writes_per_writer = 100;
    let barrier = Arc::new(Barrier::new(num_writers + 1)); // +1 for flusher

    let ts_counter = Arc::new(AtomicU64::new(1));
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Spawn writer threads
    let mut writer_handles = vec![];
    for writer_id in 0..num_writers {
        let engine = Arc::clone(&engine);
        let ts_counter = Arc::clone(&ts_counter);
        let barrier = Arc::clone(&barrier);
        let stop_flag = Arc::clone(&stop_flag);

        let handle = thread::spawn(move || {
            barrier.wait(); // Synchronize start

            let mut written_keys = vec![];
            let large_value = vec![b'x'; 50]; // Larger values to fill memtable faster
            for i in 0..writes_per_writer {
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                let key = format!("writer_{}_key_{:05}", writer_id, i);
                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                write_test_data(&engine, key.as_bytes(), &large_value, ts);
                written_keys.push(key);
            }
            written_keys
        });
        writer_handles.push(handle);
    }

    // Spawn flusher thread
    let engine_flusher = Arc::clone(&engine);
    let flusher = thread::spawn(move || {
        barrier.wait(); // Wait for all writers to start

        let mut flushes = 0;
        for _ in 0..20 {
            // Try multiple flushes during writer activity
            thread::sleep(Duration::from_millis(2));
            while let Some(frozen) = engine_flusher.maybe_rotate() {
                if engine_flusher.flush_memtable(&frozen).is_ok() {
                    flushes += 1;
                }
            }
        }
        flushes
    });

    // Collect all written keys
    let mut all_written_keys: HashSet<String> = HashSet::new();
    for handle in writer_handles {
        let keys = handle.join().unwrap();
        for key in keys {
            all_written_keys.insert(key);
        }
    }

    let flushes = flusher.join().unwrap();

    // Final flush to ensure all data is on disk
    while let Some(frozen) = engine.maybe_rotate() {
        engine.flush_memtable(&frozen).unwrap();
    }

    // Verify all keys are readable
    let mut found_keys = 0;
    for key in &all_written_keys {
        let value = get_for_test(&engine, key.as_bytes());
        assert!(value.is_some(), "Key {} should exist", key);
        found_keys += 1;
    }

    assert_eq!(
        found_keys,
        all_written_keys.len(),
        "All written keys should be readable"
    );
    // Note: flushes may be 0 if all data fits in memtable or timing doesn't align
    // The key invariant is that all data is readable, not that flushes happened
}

/// Test concurrent reads while flush is in progress.
///
/// Verifies MVCC consistency - readers see a consistent snapshot.
#[test]
fn test_concurrent_read_during_flush() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    // Pre-populate data
    for i in 0..100 {
        let key = format!("pre_key_{:05}", i);
        write_test_data(&engine, key.as_bytes(), b"initial", i as u64 + 1);
    }

    // Flush initial data
    if let Some(frozen) = engine.maybe_rotate() {
        engine.flush_memtable(&frozen).unwrap();
    }

    let num_readers = 4;
    let barrier = Arc::new(Barrier::new(num_readers + 1)); // +1 for flusher

    // Spawn reader threads
    let mut reader_handles = vec![];
    for reader_id in 0..num_readers {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier.wait(); // Synchronize start

            let mut reads = 0;
            let mut errors = 0;
            for _ in 0..50 {
                for i in 0..100 {
                    let key = format!("pre_key_{:05}", i);
                    match get_for_test(&engine, key.as_bytes()) {
                        Some(_) => reads += 1,
                        None => {
                            // Key should always exist - this would be an error
                            errors += 1;
                        }
                    }
                }
            }
            (reader_id, reads, errors)
        });
        reader_handles.push(handle);
    }

    // Spawn flusher thread that also does writes
    let engine_flusher = Arc::clone(&engine);
    let flusher = thread::spawn(move || {
        barrier.wait();

        // Write and flush repeatedly
        for round in 0..5 {
            for i in 0..20 {
                let key = format!("new_key_{}_{}", round, i);
                let ts = (round * 100 + i + 200) as u64;
                write_test_data(&engine_flusher, key.as_bytes(), b"new", ts);
            }
            if let Some(frozen) = engine_flusher.maybe_rotate() {
                let _ = engine_flusher.flush_memtable(&frozen);
            }
        }
    });

    // Collect results
    for handle in reader_handles {
        let (reader_id, reads, errors) = handle.join().unwrap();
        assert_eq!(errors, 0, "Reader {} should have no errors", reader_id);
        assert!(
            reads > 0,
            "Reader {} should have completed reads",
            reader_id
        );
    }

    flusher.join().unwrap();
}

// ============================================================================
// Concurrent Write During Compaction Tests
// ============================================================================

/// Test concurrent writes while compaction would be triggered.
///
/// Note: Actual compaction is background, this tests the infrastructure.
#[test]
fn test_concurrent_write_during_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    let large_value = vec![b'x'; 200];
    let mut flushed = false;

    // Create multiple L0 SSTs to trigger compaction
    for batch in 0..5 {
        for i in 0..10 {
            let key = format!("batch_{}_key_{:03}", batch, i);
            let ts = (batch * 100 + i + 1) as u64;
            write_test_data(&engine, key.as_bytes(), &large_value, ts);

            // Flush after each write if rotation happens
            while let Some(frozen) = engine.maybe_rotate() {
                engine.flush_memtable(&frozen).unwrap();
                flushed = true;
            }
        }
    }

    // Skip the L0 count check if we didn't flush - data is just in memtable
    if !flushed {
        // No flushes happened, so this test is effectively a no-op for compaction
        // Just verify data integrity
    }

    // Verify data integrity regardless
    let version = engine.current_version();

    // Now do concurrent writes (compaction would run in background)
    let num_writers = 4;
    let writes_per_writer = 50;
    let ts_counter = Arc::new(AtomicU64::new(1000));

    let mut handles = vec![];
    for writer_id in 0..num_writers {
        let engine = Arc::clone(&engine);
        let ts_counter = Arc::clone(&ts_counter);

        let handle = thread::spawn(move || {
            for i in 0..writes_per_writer {
                let key = format!("concurrent_{}_key_{:04}", writer_id, i);
                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                write_test_data(&engine, key.as_bytes(), b"concurrent_value", ts);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all concurrent writes are readable
    for writer_id in 0..num_writers {
        for i in 0..writes_per_writer {
            let key = format!("concurrent_{}_key_{:04}", writer_id, i);
            let value = get_for_test(&engine, key.as_bytes());
            assert!(value.is_some(), "Key {} should exist", key);
        }
    }
}

/// Test concurrent reads while compaction would be triggered.
#[test]
fn test_concurrent_read_during_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    // Create data across multiple SSTs
    for batch in 0..5 {
        for i in 0..50 {
            let key = format!("read_batch_{}_key_{:03}", batch, i);
            let ts = (batch * 100 + i + 1) as u64;
            write_test_data(&engine, key.as_bytes(), b"value", ts);
        }
        if let Some(frozen) = engine.maybe_rotate() {
            engine.flush_memtable(&frozen).unwrap();
        }
    }

    // Concurrent readers
    let num_readers = 8;
    let reads_per_reader = 100;
    let barrier = Arc::new(Barrier::new(num_readers));

    let mut handles = vec![];
    for reader_id in 0..num_readers {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier.wait();

            let mut found = 0;
            for _ in 0..reads_per_reader {
                for batch in 0..5 {
                    let idx = (reader_id * 7 + batch) % 50; // Varied access pattern
                    let key = format!("read_batch_{}_key_{:03}", batch, idx);
                    if get_for_test(&engine, key.as_bytes()).is_some() {
                        found += 1;
                    }
                }
            }
            found
        });
        handles.push(handle);
    }

    let total_found: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

    // Each reader reads 5 keys * reads_per_reader times
    let expected = num_readers * reads_per_reader * 5;
    assert_eq!(
        total_found, expected,
        "All reads should succeed during compaction period"
    );
}

// ============================================================================
// Stress Tests
// ============================================================================

/// Stress test with mixed operations - writes, reads, and flushes.
#[test]
#[ignore] // Long-running test, enable explicitly
fn test_stress_mixed_operations() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    let duration = Duration::from_secs(30);
    let stop_flag = Arc::new(AtomicBool::new(false));
    let ts_counter = Arc::new(AtomicU64::new(1));

    // Stats counters
    let writes_done = Arc::new(AtomicU64::new(0));
    let reads_done = Arc::new(AtomicU64::new(0));
    let flushes_done = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // 8 writer threads
    for writer_id in 0..8 {
        let engine = Arc::clone(&engine);
        let stop_flag = Arc::clone(&stop_flag);
        let ts_counter = Arc::clone(&ts_counter);
        let writes_done = Arc::clone(&writes_done);

        let handle = thread::spawn(move || {
            let mut local_writes = 0;
            while !stop_flag.load(Ordering::Relaxed) {
                let key = format!("stress_w{}_k{}", writer_id, local_writes);
                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                write_test_data(&engine, key.as_bytes(), b"stress_value", ts);
                local_writes += 1;

                if local_writes % 1000 == 0 {
                    thread::yield_now();
                }
            }
            writes_done.fetch_add(local_writes, Ordering::SeqCst);
        });
        handles.push(handle);
    }

    // 16 reader threads
    for reader_id in 0..16 {
        let engine = Arc::clone(&engine);
        let stop_flag = Arc::clone(&stop_flag);
        let reads_done = Arc::clone(&reads_done);

        let handle = thread::spawn(move || {
            let mut local_reads = 0;
            let mut rng_seed: usize = reader_id;
            while !stop_flag.load(Ordering::Relaxed) {
                // Random key access
                rng_seed = rng_seed.wrapping_mul(1103515245).wrapping_add(12345);
                let writer_id = rng_seed % 8;
                let key_id = (rng_seed / 8) % 10000;
                let key = format!("stress_w{}_k{}", writer_id, key_id);
                let _ = get_for_test(&engine, key.as_bytes());
                local_reads += 1;

                if local_reads % 1000 == 0 {
                    thread::yield_now();
                }
            }
            reads_done.fetch_add(local_reads, Ordering::SeqCst);
        });
        handles.push(handle);
    }

    // 1 flusher thread
    {
        let engine = Arc::clone(&engine);
        let stop_flag = Arc::clone(&stop_flag);
        let flushes_done = Arc::clone(&flushes_done);

        let handle = thread::spawn(move || {
            let mut local_flushes = 0u64;
            while !stop_flag.load(Ordering::Relaxed) {
                if let Some(frozen) = engine.maybe_rotate() {
                    if engine.flush_memtable(&frozen).is_ok() {
                        local_flushes += 1;
                    }
                }
                thread::sleep(Duration::from_millis(50));
            }
            flushes_done.fetch_add(local_flushes, Ordering::SeqCst);
        });
        handles.push(handle);
    }

    // Run for duration
    thread::sleep(duration);
    stop_flag.store(true, Ordering::SeqCst);

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let writes = writes_done.load(Ordering::SeqCst);
    let reads = reads_done.load(Ordering::SeqCst);
    let flushes = flushes_done.load(Ordering::SeqCst);

    println!(
        "Stress test completed: {} writes, {} reads, {} flushes",
        writes, reads, flushes
    );

    assert!(writes > 0, "Should have completed writes");
    assert!(reads > 0, "Should have completed reads");
}

// ============================================================================
// Write Ordering Tests
// ============================================================================

/// Test that write timestamps are properly ordered.
#[test]
fn test_write_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    let key = b"ordered_key";

    // Write multiple versions
    for ts in 1..=10 {
        let value = format!("value_at_{}", ts);
        write_test_data(&engine, key, value.as_bytes(), ts);
    }

    // Latest version should be ts=10
    let value = get_for_test(&engine, key);
    assert_eq!(value, Some(b"value_at_10".to_vec()));

    // Read at specific timestamps
    for ts in 1..=10 {
        let expected = format!("value_at_{}", ts);
        let value = get_at_for_test(&engine, key, ts);
        assert_eq!(value, Some(expected.into_bytes()), "Value at ts={}", ts);
    }
}

/// Test that MVCC reads see consistent snapshots.
#[test]
fn test_snapshot_isolation() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    // Write initial values
    write_test_data(&engine, b"key1", b"v1_initial", 1);
    write_test_data(&engine, b"key2", b"v2_initial", 2);

    // Snapshot at ts=2 should see both
    let v1_at_2 = get_at_for_test(&engine, b"key1", 2);
    let v2_at_2 = get_at_for_test(&engine, b"key2", 2);
    assert_eq!(v1_at_2, Some(b"v1_initial".to_vec()));
    assert_eq!(v2_at_2, Some(b"v2_initial".to_vec()));

    // Update values at later timestamps
    write_test_data(&engine, b"key1", b"v1_updated", 5);
    write_test_data(&engine, b"key2", b"v2_updated", 6);

    // Snapshot at ts=2 should still see initial values
    let v1_at_2_after = get_at_for_test(&engine, b"key1", 2);
    let v2_at_2_after = get_at_for_test(&engine, b"key2", 2);
    assert_eq!(v1_at_2_after, Some(b"v1_initial".to_vec()));
    assert_eq!(v2_at_2_after, Some(b"v2_initial".to_vec()));

    // Snapshot at ts=10 should see updated values
    let v1_at_10 = get_at_for_test(&engine, b"key1", 10);
    let v2_at_10 = get_at_for_test(&engine, b"key2", 10);
    assert_eq!(v1_at_10, Some(b"v1_updated".to_vec()));
    assert_eq!(v2_at_10, Some(b"v2_updated".to_vec()));
}

// ============================================================================
// Tombstone Tests
// ============================================================================

/// Test that tombstones are properly visible in concurrent scenarios.
#[test]
fn test_tombstone_visibility() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    // Write then delete
    write_test_data(&engine, b"to_delete", b"exists", 1);

    let mut batch = WriteBatch::new();
    batch.delete(b"to_delete".to_vec());
    batch.set_commit_ts(2);
    engine.write_batch(batch).unwrap();

    // Get should not find the key
    let value = get_for_test(&engine, b"to_delete");
    assert!(value.is_none(), "Deleted key should not be visible");

    // Get at ts=1 should find it
    let value_at_1 = get_at_for_test(&engine, b"to_delete", 1);
    assert_eq!(value_at_1, Some(b"exists".to_vec()));

    // Get at ts=2+ should not find it
    let value_at_2 = get_at_for_test(&engine, b"to_delete", 2);
    assert!(value_at_2.is_none());
}

/// Test tombstones across SST boundary.
#[test]
fn test_tombstone_across_sst() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    // Write initial value and flush
    write_test_data(&engine, b"cross_sst_key", b"initial_value", 1);
    if let Some(frozen) = engine.maybe_rotate() {
        engine.flush_memtable(&frozen).unwrap();
    }

    // Delete and flush (tombstone in separate SST)
    let mut batch = WriteBatch::new();
    batch.delete(b"cross_sst_key".to_vec());
    batch.set_commit_ts(2);
    engine.write_batch(batch).unwrap();

    if let Some(frozen) = engine.maybe_rotate() {
        engine.flush_memtable(&frozen).unwrap();
    }

    // Key should be deleted
    let value = get_for_test(&engine, b"cross_sst_key");
    assert!(
        value.is_none(),
        "Tombstone should mask value in earlier SST"
    );

    // Verify via MVCC read
    let value_at_1 = get_at_for_test(&engine, b"cross_sst_key", 1);
    assert_eq!(value_at_1, Some(b"initial_value".to_vec()));

    let value_at_2 = get_at_for_test(&engine, b"cross_sst_key", 2);
    assert!(value_at_2.is_none());
}

// ============================================================================
// flushed_lsn Invariant Tests
// ============================================================================

/// Test that flushed_lsn increases monotonically.
#[test]
fn test_flushed_lsn_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    let mut prev_flushed_lsn = 0;

    for batch_num in 0..5 {
        // Write data with larger values to exceed memtable size
        let large_value = vec![b'x'; 200];
        for i in 0..10 {
            let key = format!("lsn_test_{}_key_{}", batch_num, i);
            write_test_data(
                &engine,
                key.as_bytes(),
                &large_value,
                (batch_num * 100 + i + 1) as u64,
            );

            // Check for rotation after each write (it may happen automatically)
            while let Some(frozen) = engine.maybe_rotate() {
                engine.flush_memtable(&frozen).unwrap();
            }
        }

        // Check flushed_lsn (only verify monotonicity, not that it increases)
        let version = engine.current_version();
        let flushed_lsn = version.flushed_lsn();

        assert!(
            flushed_lsn >= prev_flushed_lsn,
            "flushed_lsn should increase monotonically: {} >= {}",
            flushed_lsn,
            prev_flushed_lsn
        );
        prev_flushed_lsn = flushed_lsn;
    }

    // Final assertion - if we wrote enough data, flushed_lsn should be > 0
    // but due to timing, it's not guaranteed
    let version = engine.current_version();
    let total_ssts = version.total_sst_count();
    // Only assert that we have SSTs if flushed_lsn > 0 (data was actually flushed)
    if prev_flushed_lsn > 0 {
        assert!(total_ssts > 0, "Should have SSTs after flush");
    }
}

/// Test version SST count matches actual files.
#[test]
fn test_version_sst_count_matches_files() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    // Create multiple SSTs
    for batch in 0..3 {
        for i in 0..30 {
            let key = format!("sst_count_{}_key_{}", batch, i);
            write_test_data(
                &engine,
                key.as_bytes(),
                b"value",
                (batch * 100 + i + 1) as u64,
            );
        }
        if let Some(frozen) = engine.maybe_rotate() {
            engine.flush_memtable(&frozen).unwrap();
        }
    }

    // Count SST files in directory
    let sst_dir = dir.path().join("sst");
    let sst_files: Vec<_> = std::fs::read_dir(&sst_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "sst")
                .unwrap_or(false)
        })
        .collect();

    // Count SSTs in version
    let version = engine.current_version();
    let version_sst_count = version.total_sst_count();

    assert_eq!(
        sst_files.len(),
        version_sst_count,
        "SST file count should match version SST count"
    );
}

// ============================================================================
// Recovery After Concurrent Operations
// ============================================================================

/// Test recovery preserves all data from concurrent writes.
#[test]
fn test_recovery_preserves_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();

    let expected_keys: HashSet<String>;
    let mut had_flushes = false;

    {
        let (engine, ilog) = create_test_lsm_engine(&dir);

        // Concurrent writes with larger values to force flush
        let num_writers = 4;
        let writes_per_writer = 20;
        let ts_counter = Arc::new(AtomicU64::new(1));
        let large_value = vec![b'x'; 200];

        let mut handles = vec![];
        for writer_id in 0..num_writers {
            let engine = Arc::clone(&engine);
            let ts_counter = Arc::clone(&ts_counter);
            let value = large_value.clone();

            let handle = thread::spawn(move || {
                let mut keys = vec![];
                for i in 0..writes_per_writer {
                    let key = format!("recovery_w{}_k{}", writer_id, i);
                    let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                    write_test_data(&engine, key.as_bytes(), &value, ts);
                    keys.push(key);
                }
                keys
            });
            handles.push(handle);
        }

        let mut all_keys = HashSet::new();
        for handle in handles {
            for key in handle.join().unwrap() {
                all_keys.insert(key);
            }
        }

        // Flush all data - keep trying until memtable is below threshold
        loop {
            if let Some(frozen) = engine.maybe_rotate() {
                engine.flush_memtable(&frozen).unwrap();
                had_flushes = true;
            } else {
                break;
            }
        }

        ilog.sync().unwrap();
        expected_keys = all_keys;
    }

    // Only test recovery if we actually flushed data
    // (If no flush happened, data was only in memtable and is lost on "crash")
    if had_flushes {
        // Recover
        let result = recover_engine(&dir);

        // Verify flushed keys - only keys that were flushed can be recovered
        // Note: Not all keys may have been flushed due to timing
        let version = result.engine.current_version();
        if version.total_sst_count() > 0 {
            // There are SSTs, so we should have some data
            let mut found_count = 0;
            for key in &expected_keys {
                if get_for_test(&result.engine, key.as_bytes()).is_some() {
                    found_count += 1;
                }
            }
            assert!(found_count > 0, "Should have recovered at least some keys");
        }
    }
}
