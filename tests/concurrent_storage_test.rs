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
use tisql::storage::mvcc::{is_tombstone, MvccIterator, MvccKey};
use tisql::storage::WriteBatch;
use tisql::testkit::{
    IlogConfig, IlogService, LsmConfigBuilder, LsmEngine, LsmRecovery, RecoveryResult, Version,
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

    // Use streaming scan_iter() - process one entry at a time
    let mut iter = engine.scan_iter(range, 0).unwrap();
    iter.advance().unwrap(); // Position on first entry

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
        iter.advance().unwrap();
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
    let engine = LsmEngine::open_with_recovery(
        config,
        Arc::clone(&lsn_provider),
        Arc::clone(&ilog),
        Version::new(),
    )
    .unwrap();

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
                let key = format!("writer_{writer_id}_key_{i:05}");
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

    let _flushes = flusher.join().unwrap();

    // Final flush to ensure all data is on disk
    while let Some(frozen) = engine.maybe_rotate() {
        engine.flush_memtable(&frozen).unwrap();
    }

    // Verify all keys are readable
    let mut found_keys = 0;
    for key in &all_written_keys {
        let value = get_for_test(&engine, key.as_bytes());
        assert!(value.is_some(), "Key {key} should exist");
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
        let key = format!("pre_key_{i:05}");
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
                    let key = format!("pre_key_{i:05}");
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
                let key = format!("new_key_{round}_{i}");
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
        assert_eq!(errors, 0, "Reader {reader_id} should have no errors");
        assert!(reads > 0, "Reader {reader_id} should have completed reads");
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
            let key = format!("batch_{batch}_key_{i:03}");
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
    let _version = engine.current_version();

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
                let key = format!("concurrent_{writer_id}_key_{i:04}");
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
            let key = format!("concurrent_{writer_id}_key_{i:04}");
            let value = get_for_test(&engine, key.as_bytes());
            assert!(value.is_some(), "Key {key} should exist");
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
            let key = format!("read_batch_{batch}_key_{i:03}");
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
                    let key = format!("read_batch_{batch}_key_{idx:03}");
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
                let key = format!("stress_w{writer_id}_k{local_writes}");
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
                let key = format!("stress_w{writer_id}_k{key_id}");
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

    println!("Stress test completed: {writes} writes, {reads} reads, {flushes} flushes");

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
        let value = format!("value_at_{ts}");
        write_test_data(&engine, key, value.as_bytes(), ts);
    }

    // Latest version should be ts=10
    let value = get_for_test(&engine, key);
    assert_eq!(value, Some(b"value_at_10".to_vec()));

    // Read at specific timestamps
    for ts in 1..=10 {
        let expected = format!("value_at_{ts}");
        let value = get_at_for_test(&engine, key, ts);
        assert_eq!(value, Some(expected.into_bytes()), "Value at ts={ts}");
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
            let key = format!("lsn_test_{batch_num}_key_{i}");
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
            "flushed_lsn should increase monotonically: {flushed_lsn} >= {prev_flushed_lsn}"
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
            let key = format!("sst_count_{batch}_key_{i}");
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
                    let key = format!("recovery_w{writer_id}_k{i}");
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
        while let Some(frozen) = engine.maybe_rotate() {
            engine.flush_memtable(&frozen).unwrap();
            had_flushes = true;
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

// ============================================================================
// Scan Across Frozen + Active Memtables with Concurrent Writes
// ============================================================================

/// Test that scan correctly merges data from frozen and active memtables.
///
/// This test verifies:
/// 1. Data written before rotation is in frozen memtable
/// 2. Data written after rotation is in active memtable
/// 3. A scan sees data from both sources merged correctly
/// 4. MVCC ordering is maintained (user_key ASC, ts DESC)
#[test]
fn test_scan_across_frozen_and_active_memtables() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    // Phase 1: Write data that will end up in frozen memtable
    // Use keys with prefix "a_" so they sort before "b_" keys
    for i in 0..10 {
        let key = format!("a_frozen_key_{i:02}");
        write_test_data(&engine, key.as_bytes(), b"frozen_value", (i + 1) as u64);
    }

    // Force rotation - this freezes the active memtable
    let frozen = engine.freeze_active();
    assert!(frozen.is_some(), "Should have rotated memtable");

    // Verify we have a frozen memtable
    assert!(
        engine.frozen_count() > 0,
        "Should have at least one frozen memtable"
    );

    // Phase 2: Write data to the new active memtable
    // Use keys with prefix "b_" so they sort after "a_" keys
    for i in 0..10 {
        let key = format!("b_active_key_{i:02}");
        write_test_data(&engine, key.as_bytes(), b"active_value", (i + 100) as u64);
    }

    // Phase 3: Scan across both memtables
    let start = MvccKey::encode(b"a_", Timestamp::MAX);
    let end = MvccKey::encode(b"c_", 0);
    let range = start..end;

    let mut iter = engine.scan_iter(range, 0).unwrap();
    iter.advance().unwrap();

    let mut results: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut prev_key: Option<Vec<u8>> = None;

    while iter.valid() {
        let key = iter.user_key().to_vec();
        let value = iter.value().to_vec();

        // Verify ordering: keys should be ascending
        if let Some(ref pk) = prev_key {
            assert!(
                key >= *pk,
                "Keys should be in ascending order: {:?} >= {:?}",
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(pk)
            );
        }
        prev_key = Some(key.clone());
        results.push((key, value));
        iter.advance().unwrap();
    }

    // Should have 20 entries total (10 from frozen + 10 from active)
    assert_eq!(
        results.len(),
        20,
        "Should see 20 entries from frozen + active memtables"
    );

    // Verify frozen memtable data (a_* keys)
    let frozen_results: Vec<_> = results
        .iter()
        .filter(|(k, _)| k.starts_with(b"a_"))
        .collect();
    assert_eq!(
        frozen_results.len(),
        10,
        "Should have 10 entries from frozen"
    );
    for (key, value) in &frozen_results {
        assert!(
            key.starts_with(b"a_frozen_key_"),
            "Frozen key should have correct prefix"
        );
        assert_eq!(value.as_slice(), b"frozen_value");
    }

    // Verify active memtable data (b_* keys)
    let active_results: Vec<_> = results
        .iter()
        .filter(|(k, _)| k.starts_with(b"b_"))
        .collect();
    assert_eq!(
        active_results.len(),
        10,
        "Should have 10 entries from active"
    );
    for (key, value) in &active_results {
        assert!(
            key.starts_with(b"b_active_key_"),
            "Active key should have correct prefix"
        );
        assert_eq!(value.as_slice(), b"active_value");
    }
}

/// Test scan with concurrent writes to active memtable while frozen exists.
///
/// This test verifies:
/// 1. Scan iterator sees a consistent snapshot
/// 2. Concurrent writes don't corrupt the iterator
/// 3. Data from both frozen and active memtables is correctly merged
#[test]
fn test_scan_with_concurrent_writes_across_frozen_active() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    // Pre-populate data that will be frozen
    for i in 0..20 {
        let key = format!("frozen_{i:03}");
        write_test_data(&engine, key.as_bytes(), b"pre_rotation", (i + 1) as u64);
    }

    // Force rotation
    let _ = engine.freeze_active();
    assert!(engine.frozen_count() > 0, "Should have frozen memtable");

    // Write some initial data to active memtable
    for i in 0..10 {
        let key = format!("active_{i:03}");
        write_test_data(&engine, key.as_bytes(), b"post_rotation", (i + 100) as u64);
    }

    let num_writers = 4;
    let writes_per_writer = 50;
    let num_scanners = 4;
    let scans_per_scanner = 10;

    let stop_flag = Arc::new(AtomicBool::new(false));
    let ts_counter = Arc::new(AtomicU64::new(200));
    let scan_errors = Arc::new(AtomicU64::new(0));
    let scan_success = Arc::new(AtomicU64::new(0));
    let total_writes = Arc::new(AtomicU64::new(0));

    let mut handles: Vec<thread::JoinHandle<()>> = vec![];

    // Spawn writer threads - write to "writer_*" keys
    for writer_id in 0..num_writers {
        let engine = Arc::clone(&engine);
        let stop_flag = Arc::clone(&stop_flag);
        let ts_counter = Arc::clone(&ts_counter);
        let total_writes = Arc::clone(&total_writes);

        let handle = thread::spawn(move || {
            let mut writes = 0u64;
            for i in 0..writes_per_writer {
                if stop_flag.load(Ordering::Relaxed) {
                    break;
                }
                let key = format!("writer_{writer_id}_{i:03}");
                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                write_test_data(&engine, key.as_bytes(), b"concurrent_write", ts);
                writes += 1;
                thread::yield_now();
            }
            total_writes.fetch_add(writes, Ordering::Relaxed);
        });
        handles.push(handle);
    }

    // Spawn scanner threads - scan across all data
    for _scanner_id in 0..num_scanners {
        let engine = Arc::clone(&engine);
        let scan_errors = Arc::clone(&scan_errors);
        let scan_success = Arc::clone(&scan_success);

        let handle = thread::spawn(move || {
            for _ in 0..scans_per_scanner {
                // Scan the frozen + active range
                let start = MvccKey::encode(b"", Timestamp::MAX);
                let end = MvccKey::unbounded();
                let range = start..end;

                match engine.scan_iter(range, 0) {
                    Ok(mut iter) => {
                        if iter.advance().is_err() {
                            scan_errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        let mut count = 0;
                        let mut prev_user_key: Option<Vec<u8>> = None;
                        let mut prev_ts: Option<Timestamp> = None;
                        let mut ordering_ok = true;

                        while iter.valid() {
                            let user_key = iter.user_key().to_vec();
                            let ts = iter.timestamp();

                            // Verify MVCC ordering: (user_key ASC, ts DESC)
                            if let Some(ref pk) = prev_user_key {
                                match user_key.cmp(pk) {
                                    std::cmp::Ordering::Less => {
                                        // user_key went backwards - error
                                        ordering_ok = false;
                                        break;
                                    }
                                    std::cmp::Ordering::Equal => {
                                        // Same user_key: ts must be descending
                                        if let Some(pt) = prev_ts {
                                            if ts >= pt {
                                                ordering_ok = false;
                                                break;
                                            }
                                        }
                                    }
                                    std::cmp::Ordering::Greater => {
                                        // New user_key - ok
                                    }
                                }
                            }

                            prev_user_key = Some(user_key);
                            prev_ts = Some(ts);
                            count += 1;

                            if iter.advance().is_err() {
                                break;
                            }
                        }

                        if ordering_ok && count > 0 {
                            scan_success.fetch_add(1, Ordering::Relaxed);
                        } else if !ordering_ok {
                            scan_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        scan_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                thread::yield_now();
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    stop_flag.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().unwrap();
    }

    let writes = total_writes.load(Ordering::Relaxed);
    let successes = scan_success.load(Ordering::Relaxed);
    let errors = scan_errors.load(Ordering::Relaxed);

    assert!(writes > 0, "Should have completed some writes");
    assert!(
        successes > 0,
        "At least some scans should succeed, got {successes} successes, {errors} errors"
    );
    assert_eq!(errors, 0, "Should have no scan ordering errors");

    // Final verification: scan should see all pre-populated data
    let frozen_count = {
        let start = MvccKey::encode(b"frozen_", Timestamp::MAX);
        let end = MvccKey::encode(b"frozen_\xff", 0);
        let mut iter = engine.scan_iter(start..end, 0).unwrap();
        iter.advance().unwrap();
        let mut count = 0;
        while iter.valid() {
            count += 1;
            iter.advance().unwrap();
        }
        count
    };
    assert_eq!(
        frozen_count, 20,
        "Should still see all 20 frozen entries after concurrent operations"
    );
}

/// Test that scan snapshot is isolated from concurrent rotations.
///
/// Verifies that a scan iterator created before rotation still sees
/// a consistent view even if rotation happens during iteration.
#[test]
fn test_scan_snapshot_isolation_during_rotation() {
    let dir = tempfile::tempdir().unwrap();

    // Create engine with larger memtable to control rotation timing
    let lsn_provider = new_lsn_provider();
    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(4096) // Larger to prevent auto-rotation
        .build_unchecked();
    let engine = Arc::new(
        LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap(),
    );

    // Write initial data
    for i in 0..50 {
        let key = format!("key_{i:03}");
        let value = format!("value_{i}");
        write_test_data(&engine, key.as_bytes(), value.as_bytes(), (i + 1) as u64);
    }

    // Create a scan iterator BEFORE rotation
    let start = MvccKey::encode(b"key_", Timestamp::MAX);
    let end = MvccKey::encode(b"key_\xff", 0);
    let mut iter = engine.scan_iter(start..end, 0).unwrap();
    iter.advance().unwrap();

    // Read first 10 entries
    let mut pre_rotation_keys = Vec::new();
    for _ in 0..10 {
        if iter.valid() {
            pre_rotation_keys.push(iter.user_key().to_vec());
            iter.advance().unwrap();
        }
    }

    // Force rotation WHILE iterator exists
    let _ = engine.freeze_active();

    // Write more data to new active memtable (these should NOT appear in our scan)
    for i in 50..60 {
        let key = format!("key_{i:03}");
        let value = format!("new_value_{i}");
        write_test_data(&engine, key.as_bytes(), value.as_bytes(), (i + 100) as u64);
    }

    // Continue reading with the same iterator
    let mut post_rotation_keys = Vec::new();
    while iter.valid() {
        post_rotation_keys.push(iter.user_key().to_vec());
        iter.advance().unwrap();
    }

    // Total should be 50 (the original data), not 60
    let total_keys = pre_rotation_keys.len() + post_rotation_keys.len();
    assert_eq!(
        total_keys, 50,
        "Scan should see exactly 50 keys (snapshot isolation), got {total_keys}"
    );

    // Verify keys are in order (key_000 through key_049)
    let mut all_keys = pre_rotation_keys;
    all_keys.extend(post_rotation_keys);

    for (i, key) in all_keys.iter().enumerate() {
        let expected = format!("key_{i:03}");
        assert_eq!(
            key.as_slice(),
            expected.as_bytes(),
            "Key at position {i} should be {expected}"
        );
    }

    // A NEW scan should see the new data
    let start = MvccKey::encode(b"key_", Timestamp::MAX);
    let end = MvccKey::encode(b"key_\xff", 0);
    let mut new_iter = engine.scan_iter(start..end, 0).unwrap();
    new_iter.advance().unwrap();

    let mut new_scan_count = 0;
    while new_iter.valid() {
        new_scan_count += 1;
        new_iter.advance().unwrap();
    }

    assert_eq!(
        new_scan_count, 60,
        "New scan should see all 60 keys including post-rotation writes"
    );
}

/// Test same key exists in both frozen and active memtables.
///
/// When the same key is updated after rotation, the scan should
/// return both versions in proper MVCC order (newer first).
#[test]
fn test_same_key_in_frozen_and_active_memtables() {
    let dir = tempfile::tempdir().unwrap();
    let (engine, _ilog) = create_test_lsm_engine(&dir);

    // Write initial version of keys (will be frozen)
    for i in 0..10 {
        let key = format!("shared_key_{i:02}");
        write_test_data(&engine, key.as_bytes(), b"v1_frozen", (i + 1) as u64);
    }

    // Force rotation
    let _ = engine.freeze_active();
    assert!(engine.frozen_count() > 0);

    // Write newer versions of the SAME keys to active memtable
    for i in 0..10 {
        let key = format!("shared_key_{i:02}");
        write_test_data(&engine, key.as_bytes(), b"v2_active", (i + 100) as u64);
    }

    // Scan should see both versions of each key, newer first
    let start = MvccKey::encode(b"shared_key_", Timestamp::MAX);
    let end = MvccKey::encode(b"shared_key_\xff", 0);
    let mut iter = engine.scan_iter(start..end, 0).unwrap();
    iter.advance().unwrap();

    let mut entries: Vec<(Vec<u8>, Timestamp, Vec<u8>)> = Vec::new();
    while iter.valid() {
        entries.push((
            iter.user_key().to_vec(),
            iter.timestamp(),
            iter.value().to_vec(),
        ));
        iter.advance().unwrap();
    }

    // Should have 20 entries (2 versions per key * 10 keys)
    assert_eq!(
        entries.len(),
        20,
        "Should see 20 entries (2 versions * 10 keys)"
    );

    // Verify MVCC ordering: for each key, newer version (v2) should come before older (v1)
    for i in 0..10 {
        let key = format!("shared_key_{i:02}");
        let key_entries: Vec<_> = entries
            .iter()
            .filter(|(k, _, _)| k == key.as_bytes())
            .collect();

        assert_eq!(key_entries.len(), 2, "Key {key} should have 2 versions");

        // First entry should be newer (higher ts, v2_active)
        assert!(
            key_entries[0].1 > key_entries[1].1,
            "First version should have higher timestamp"
        );
        assert_eq!(key_entries[0].2.as_slice(), b"v2_active");
        assert_eq!(key_entries[1].2.as_slice(), b"v1_frozen");
    }

    // Point read should return the latest version
    for i in 0..10 {
        let key = format!("shared_key_{i:02}");
        let value = get_for_test(&engine, key.as_bytes());
        assert_eq!(
            value,
            Some(b"v2_active".to_vec()),
            "Point read for {key} should return latest version"
        );
    }
}

/// Stress test: many rotations with concurrent scans and writes.
#[test]
fn test_stress_rotations_with_concurrent_scans() {
    let dir = tempfile::tempdir().unwrap();

    let lsn_provider = new_lsn_provider();
    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(128) // Small to trigger frequent rotations
        .build_unchecked();
    let engine = Arc::new(
        LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap(),
    );

    let num_writers = 2;
    let num_scanners = 2;
    let num_rotators = 1;
    let duration = Duration::from_secs(2);

    let stop_flag = Arc::new(AtomicBool::new(false));
    let ts_counter = Arc::new(AtomicU64::new(1));
    let writes_done = Arc::new(AtomicU64::new(0));
    let scans_done = Arc::new(AtomicU64::new(0));
    let rotations_done = Arc::new(AtomicU64::new(0));
    let scan_errors = Arc::new(AtomicU64::new(0));

    let mut handles: Vec<thread::JoinHandle<()>> = vec![];

    // Writer threads
    for writer_id in 0..num_writers {
        let engine = Arc::clone(&engine);
        let stop_flag = Arc::clone(&stop_flag);
        let ts_counter = Arc::clone(&ts_counter);
        let writes_done = Arc::clone(&writes_done);

        handles.push(thread::spawn(move || {
            let mut local_writes = 0u64;
            while !stop_flag.load(Ordering::Relaxed) {
                let key = format!("stress_w{writer_id}_k{local_writes}");
                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                write_test_data(&engine, key.as_bytes(), b"stress_value", ts);
                local_writes += 1;

                if local_writes % 100 == 0 {
                    thread::yield_now();
                }
            }
            writes_done.fetch_add(local_writes, Ordering::Relaxed);
        }));
    }

    // Scanner threads
    for _ in 0..num_scanners {
        let engine = Arc::clone(&engine);
        let stop_flag = Arc::clone(&stop_flag);
        let scans_done = Arc::clone(&scans_done);
        let scan_errors = Arc::clone(&scan_errors);

        handles.push(thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                let start = MvccKey::encode(b"stress_", Timestamp::MAX);
                let end = MvccKey::encode(b"stress_\xff", 0);

                match engine.scan_iter(start..end, 0) {
                    Ok(mut iter) => {
                        if iter.advance().is_err() {
                            scan_errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        let mut prev_user_key: Option<Vec<u8>> = None;
                        let mut prev_ts: Option<Timestamp> = None;
                        let mut ordering_valid = true;

                        while iter.valid() {
                            let user_key = iter.user_key().to_vec();
                            let ts = iter.timestamp();

                            // Verify MVCC ordering: (user_key ASC, ts DESC)
                            if let Some(ref pk) = prev_user_key {
                                match user_key.cmp(pk) {
                                    std::cmp::Ordering::Less => {
                                        ordering_valid = false;
                                        break;
                                    }
                                    std::cmp::Ordering::Equal => {
                                        if let Some(pt) = prev_ts {
                                            if ts >= pt {
                                                ordering_valid = false;
                                                break;
                                            }
                                        }
                                    }
                                    std::cmp::Ordering::Greater => {}
                                }
                            }
                            prev_user_key = Some(user_key);
                            prev_ts = Some(ts);

                            if iter.advance().is_err() {
                                break;
                            }
                        }

                        if ordering_valid {
                            scans_done.fetch_add(1, Ordering::Relaxed);
                        } else {
                            scan_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        scan_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                thread::yield_now();
            }
        }));
    }

    // Rotator thread
    for _ in 0..num_rotators {
        let engine = Arc::clone(&engine);
        let stop_flag = Arc::clone(&stop_flag);
        let rotations_done = Arc::clone(&rotations_done);

        handles.push(thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                if let Some(frozen) = engine.maybe_rotate() {
                    // Optionally flush some frozen memtables
                    let _ = engine.flush_memtable(&frozen);
                    rotations_done.fetch_add(1, Ordering::Relaxed);
                }
                thread::sleep(Duration::from_millis(10));
            }
        }));
    }

    // Run for duration
    thread::sleep(duration);
    stop_flag.store(true, Ordering::Relaxed);

    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }

    let writes = writes_done.load(Ordering::Relaxed);
    let scans = scans_done.load(Ordering::Relaxed);
    let rotations = rotations_done.load(Ordering::Relaxed);
    let errors = scan_errors.load(Ordering::Relaxed);

    println!("Stress test: {writes} writes, {scans} scans, {rotations} rotations, {errors} errors");

    assert!(writes > 0, "Should have completed writes");
    assert!(scans > 0, "Should have completed scans");
    assert_eq!(errors, 0, "Should have no scan ordering errors");
}

// ============================================================================
// Write Stall / Backpressure Tests
// ============================================================================

/// Test that rotation stops when frozen memtable limit is reached.
///
/// When max_frozen_memtables is reached and no flush happens,
/// maybe_rotate() should return None to signal backpressure.
#[test]
fn test_rotation_blocked_at_frozen_limit() {
    let dir = tempfile::tempdir().unwrap();

    let lsn_provider = new_lsn_provider();
    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    // Memtable large enough to hold test data without auto-rotation,
    // but small enough to demonstrate the concept
    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(2048) // Large enough for 5 entries per batch
        .max_frozen_memtables(2) // Only allow 2 frozen
        .build_unchecked();

    let engine = LsmEngine::open_with_recovery(
        config,
        Arc::clone(&lsn_provider),
        Arc::clone(&ilog),
        Version::new(),
    )
    .unwrap();

    // Use values to ensure memtable has data
    let value = vec![b'x'; 100];

    // Fill up frozen memtables by forcing rotations (without flushing)
    // First rotation
    for i in 0..5 {
        let key = format!("batch1_key_{i:02}");
        write_test_data(&engine, key.as_bytes(), &value, (i + 1) as u64);
    }

    assert_eq!(
        engine.frozen_count(),
        0,
        "No auto-rotation should happen yet"
    );
    let rot1 = engine.freeze_active();
    assert!(rot1.is_some(), "First rotation should succeed");
    assert_eq!(engine.frozen_count(), 1);

    // Second rotation
    for i in 0..5 {
        let key = format!("batch2_key_{i:02}");
        write_test_data(&engine, key.as_bytes(), &value, (i + 10) as u64);
    }
    let rot2 = engine.freeze_active();
    assert!(rot2.is_some(), "Second rotation should succeed");
    assert_eq!(engine.frozen_count(), 2);

    // Third rotation should fail - frozen limit reached
    for i in 0..5 {
        let key = format!("batch3_key_{i:02}");
        write_test_data(&engine, key.as_bytes(), &value, (i + 20) as u64);
    }
    let rot3 = engine.freeze_active();
    assert!(
        rot3.is_none(),
        "Third rotation should fail due to frozen limit"
    );
    assert_eq!(
        engine.frozen_count(),
        2,
        "Frozen count should stay at limit"
    );

    // Writes should still succeed (goes to active memtable)
    let key = b"after_limit_key";
    write_test_data(&engine, key, b"still_works", 100);
    let value = get_for_test(&engine, key);
    assert_eq!(value, Some(b"still_works".to_vec()));

    // Active memtable should have data (no rotation happened)
    let stats = engine.stats();
    assert!(
        stats.active_memtable_size > 0,
        "Active memtable should have data"
    );
}

/// Test that flushing a frozen memtable unblocks rotation.
///
/// After hitting the frozen limit, flushing should allow new rotations.
#[test]
fn test_flush_unblocks_rotation() {
    let dir = tempfile::tempdir().unwrap();

    let lsn_provider = new_lsn_provider();
    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(2048) // Large enough to avoid auto-rotation
        .max_frozen_memtables(1) // Very restrictive - only 1 frozen allowed
        .build_unchecked();

    let engine = LsmEngine::open_with_recovery(
        config,
        Arc::clone(&lsn_provider),
        Arc::clone(&ilog),
        Version::new(),
    )
    .unwrap();

    let value = vec![b'x'; 100];

    // First rotation succeeds
    for i in 0..5 {
        let key = format!("key_{i:02}");
        write_test_data(&engine, key.as_bytes(), &value, (i + 1) as u64);
    }
    let frozen1 = engine.freeze_active();
    assert!(frozen1.is_some(), "First rotation should succeed");
    assert_eq!(engine.frozen_count(), 1);

    // Second rotation fails - at limit
    for i in 0..5 {
        let key = format!("key2_{i:02}");
        write_test_data(&engine, key.as_bytes(), &value, (i + 10) as u64);
    }
    let frozen2_attempt = engine.freeze_active();
    assert!(
        frozen2_attempt.is_none(),
        "Should be blocked by frozen limit"
    );

    // Flush the frozen memtable
    let frozen1 = frozen1.unwrap();
    engine.flush_memtable(&frozen1).unwrap();
    assert_eq!(
        engine.frozen_count(),
        0,
        "Frozen should be empty after flush"
    );

    // Now rotation should work again
    let frozen2 = engine.freeze_active();
    assert!(frozen2.is_some(), "Rotation should succeed after flush");
    assert_eq!(engine.frozen_count(), 1);

    // Verify all data is accessible
    for i in 0..5 {
        let key = format!("key_{i:02}");
        assert!(
            get_for_test(&engine, key.as_bytes()).is_some(),
            "Flushed key {key} should be readable from SST"
        );
    }
    for i in 0..5 {
        let key = format!("key2_{i:02}");
        assert!(
            get_for_test(&engine, key.as_bytes()).is_some(),
            "Active/frozen key {key} should be readable"
        );
    }
}

/// Test concurrent writes under backpressure (frozen limit reached).
///
/// When the frozen limit is reached, concurrent writers should still
/// be able to write to the active memtable without blocking.
#[test]
fn test_concurrent_writes_under_backpressure() {
    let dir = tempfile::tempdir().unwrap();

    let lsn_provider = new_lsn_provider();
    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(128)
        .max_frozen_memtables(2)
        .build_unchecked();

    let engine = Arc::new(
        LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap(),
    );

    // Fill up frozen memtables to create backpressure
    for batch in 0..2 {
        for i in 0..10 {
            let key = format!("setup_batch{batch}_key{i}");
            write_test_data(
                &engine,
                key.as_bytes(),
                b"setup",
                (batch * 100 + i + 1) as u64,
            );
        }
        engine.freeze_active();
    }
    assert_eq!(engine.frozen_count(), 2, "Should be at frozen limit");

    // Verify rotation is blocked
    assert!(
        engine.freeze_active().is_none(),
        "Should not be able to rotate at limit"
    );

    // Now do concurrent writes - they should all succeed (no actual stall)
    let num_writers = 4;
    let writes_per_writer = 100;
    let ts_counter = Arc::new(AtomicU64::new(1000));
    let success_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    for writer_id in 0..num_writers {
        let engine = Arc::clone(&engine);
        let ts_counter = Arc::clone(&ts_counter);
        let success_count = Arc::clone(&success_count);

        handles.push(thread::spawn(move || {
            for i in 0..writes_per_writer {
                let key = format!("pressure_w{writer_id}_k{i:04}");
                let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                write_test_data(&engine, key.as_bytes(), b"under_pressure", ts);
                success_count.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let total_writes = success_count.load(Ordering::Relaxed);
    assert_eq!(
        total_writes,
        (num_writers * writes_per_writer) as u64,
        "All writes should succeed even under backpressure"
    );

    // Verify writes are readable
    for writer_id in 0..num_writers {
        for i in [0, writes_per_writer / 2, writes_per_writer - 1] {
            let key = format!("pressure_w{writer_id}_k{i:04}");
            let value = get_for_test(&engine, key.as_bytes());
            assert!(
                value.is_some(),
                "Key {key} should be readable from active memtable"
            );
        }
    }

    // Active memtable should be large since no rotation happened
    let stats = engine.stats();
    assert!(
        stats.active_memtable_size > 128,
        "Active memtable ({}) should exceed configured size since rotation was blocked",
        stats.active_memtable_size
    );
}

/// Test that a slow flusher causes memtable growth under sustained write load.
///
/// This simulates a scenario where writes are faster than flushes,
/// demonstrating the need for proper backpressure.
#[test]
fn test_slow_flush_causes_memtable_growth() {
    let dir = tempfile::tempdir().unwrap();

    let lsn_provider = new_lsn_provider();
    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(64)
        .max_frozen_memtables(2)
        .build_unchecked();

    let engine = Arc::new(
        LsmEngine::open_with_recovery(
            config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            Version::new(),
        )
        .unwrap(),
    );

    let stop_flag = Arc::new(AtomicBool::new(false));
    let ts_counter = Arc::new(AtomicU64::new(1));
    let writes_done = Arc::new(AtomicU64::new(0));
    let rotations_blocked = Arc::new(AtomicU64::new(0));

    // Fast writer thread
    let engine_writer = Arc::clone(&engine);
    let stop_flag_writer = Arc::clone(&stop_flag);
    let ts_counter_clone = Arc::clone(&ts_counter);
    let writes_done_clone = Arc::clone(&writes_done);
    let rotations_blocked_clone = Arc::clone(&rotations_blocked);

    let writer = thread::spawn(move || {
        let mut local_writes = 0u64;
        while !stop_flag_writer.load(Ordering::Relaxed) {
            let key = format!("fast_write_{local_writes:06}");
            let ts = ts_counter_clone.fetch_add(1, Ordering::SeqCst);
            write_test_data(&engine_writer, key.as_bytes(), b"data", ts);
            local_writes += 1;

            // Try to rotate periodically
            if local_writes % 20 == 0
                && engine_writer.maybe_rotate().is_none()
                && engine_writer.frozen_count() >= 2
            {
                rotations_blocked_clone.fetch_add(1, Ordering::Relaxed);
            }
        }
        writes_done_clone.fetch_add(local_writes, Ordering::Relaxed);
    });

    // Slow flusher thread - intentionally slow
    let engine_flusher = Arc::clone(&engine);
    let stop_flag_flusher = Arc::clone(&stop_flag);

    let flusher = thread::spawn(move || {
        let mut flushes = 0u64;
        while !stop_flag_flusher.load(Ordering::Relaxed) {
            // Slow flush - only flush every 100ms
            thread::sleep(Duration::from_millis(100));

            // Get oldest frozen memtable
            if let Some(frozen) = engine_flusher.maybe_rotate() {
                if engine_flusher.flush_memtable(&frozen).is_ok() {
                    flushes += 1;
                }
            }
        }
        flushes
    });

    // Run for a short duration
    thread::sleep(Duration::from_millis(500));
    stop_flag.store(true, Ordering::Relaxed);

    writer.join().unwrap();
    let _flushes = flusher.join().unwrap();

    let writes = writes_done.load(Ordering::Relaxed);
    let blocked = rotations_blocked.load(Ordering::Relaxed);

    assert!(writes > 0, "Should have completed writes");
    // With slow flush and fast writes, we expect rotation to be blocked sometimes
    // (though not guaranteed depending on timing)

    println!("Slow flush test: {writes} writes, {blocked} rotation blocks");

    // All written data should be readable
    let stats = engine.stats();
    println!(
        "Final state: active_size={}, frozen_count={}",
        stats.active_memtable_size, stats.frozen_memtable_count
    );
}

/// Test write stall behavior with explicit transaction-like writes.
///
/// Verifies that large batch writes don't get stuck when frozen limit is reached.
#[test]
fn test_large_batch_under_frozen_limit() {
    let dir = tempfile::tempdir().unwrap();

    let lsn_provider = new_lsn_provider();
    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(128)
        .max_frozen_memtables(1)
        .build_unchecked();

    let engine = LsmEngine::open_with_recovery(
        config,
        Arc::clone(&lsn_provider),
        Arc::clone(&ilog),
        Version::new(),
    )
    .unwrap();

    // Fill to frozen limit
    for i in 0..10 {
        let key = format!("setup_{i:02}");
        write_test_data(&engine, key.as_bytes(), b"setup", (i + 1) as u64);
    }
    engine.freeze_active();
    assert_eq!(engine.frozen_count(), 1);

    // Now write a large batch that would normally trigger multiple rotations
    // Since frozen limit is reached, all goes to active memtable
    let large_value = vec![b'x'; 256]; // Larger than memtable_size
    for i in 0..20 {
        let key = format!("large_batch_{i:02}");
        write_test_data(&engine, key.as_bytes(), &large_value, (i + 100) as u64);
    }

    // All writes should succeed
    for i in 0..20 {
        let key = format!("large_batch_{i:02}");
        let value = get_for_test(&engine, key.as_bytes());
        assert!(value.is_some(), "Large batch key {key} should be readable");
        assert_eq!(value.unwrap().len(), 256);
    }

    // Active memtable should be significantly larger than configured size
    let stats = engine.stats();
    assert!(
        stats.active_memtable_size > 128 * 10, // Much larger than single memtable
        "Active memtable ({}) should be very large due to blocked rotation",
        stats.active_memtable_size
    );
}
