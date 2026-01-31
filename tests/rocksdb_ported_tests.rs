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

//! Tests ported from RocksDB to ensure storage engine correctness.
//!
//! These tests are inspired by RocksDB's comprehensive test suite, particularly:
//! - db_memtable_test.cc (DuplicateSeq, ConcurrentMergeWrite)
//! - compaction_job_test.cc (SimpleDeletion, SingleDeleteSnapshots, etc.)
//! - db_flush_test.cc (FlushWhileWritingManifest, SyncFail)
//!
//! Run with: cargo test --test rocksdb_ported_tests

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

use tisql::new_lsn_provider;
use tisql::storage::mvcc::{is_tombstone, MvccIterator, MvccKey};
use tisql::storage::WriteBatch;
use tisql::testkit::{
    IlogConfig, IlogService, LsmConfigBuilder, LsmEngine, SstBuilder, SstBuilderOptions,
    SstIterator, SstReaderRef, Version,
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
    let mut iter = engine.scan_iter(range).unwrap();

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
        iter.next().unwrap();
    }
    None
}

fn get_for_test(engine: &LsmEngine, key: &[u8]) -> Option<RawValue> {
    get_at_for_test(engine, key, Timestamp::MAX)
}

fn scan_at_for_test(
    engine: &LsmEngine,
    range: &std::ops::Range<Vec<u8>>,
    ts: Timestamp,
) -> Vec<(Vec<u8>, RawValue)> {
    let start = MvccKey::encode(&range.start, ts);
    let end = MvccKey::encode(&range.end, ts);
    let mvcc_range = start..end;

    // Use streaming scan_iter() - process one entry at a time
    let mut iter = engine.scan_iter(mvcc_range).unwrap();

    let mut seen_keys: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut output = Vec::new();

    while iter.valid() {
        let decoded_key = iter.user_key().to_vec();
        let entry_ts = iter.timestamp();
        let value = iter.value().to_vec();

        // Move to next before continue checks (so we don't get stuck)
        iter.next().unwrap();

        if decoded_key < range.start || decoded_key >= range.end {
            continue;
        }
        if entry_ts > ts {
            continue;
        }
        if seen_keys.contains(&decoded_key) {
            continue;
        }
        seen_keys.insert(decoded_key.clone());
        if !is_tombstone(&value) {
            output.push((decoded_key, value));
        }
    }

    output.sort_by(|a, b| a.0.cmp(&b.0));
    output
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

fn create_engine(dir: &TempDir) -> LsmEngine {
    let lsn_provider = new_lsn_provider();
    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(4096)
        .max_frozen_memtables(8)
        .build_unchecked();
    LsmEngine::open_with_recovery(config, lsn_provider, ilog, Version::new()).unwrap()
}

fn create_durable_engine(dir: &TempDir) -> (LsmEngine, Arc<IlogService>) {
    let lsn_provider = new_lsn_provider();
    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(256) // Small for testing
        .max_frozen_memtables(16)
        .build_unchecked();

    let engine =
        LsmEngine::open_with_recovery(config, lsn_provider, Arc::clone(&ilog), Version::new())
            .unwrap();
    (engine, ilog)
}

fn new_batch(commit_ts: Timestamp) -> WriteBatch {
    let mut batch = WriteBatch::new();
    batch.set_commit_ts(commit_ts);
    batch
}

fn new_batch_with_lsn(commit_ts: Timestamp, lsn: u64) -> WriteBatch {
    let mut batch = WriteBatch::new();
    batch.set_commit_ts(commit_ts);
    batch.set_clog_lsn(lsn);
    batch
}

// ============================================================================
// MEMTABLE TESTS (ported from db_memtable_test.cc)
// ============================================================================

/// Test duplicate key detection in memtable with MVCC semantics.
/// Ported from RocksDB's DuplicateSeq test.
///
/// Unlike RocksDB which rejects duplicates with the same sequence number,
/// TiSQL's MVCC allows multiple versions of the same key at different timestamps.
/// This test verifies that:
/// 1. Multiple versions of the same key can coexist
/// 2. Reading at different timestamps returns correct versions
#[test]
fn test_memtable_duplicate_seq_mvcc() {
    let dir = TempDir::new().unwrap();
    let engine = create_engine(&dir);

    // Write same key multiple times at different timestamps
    // (equivalent to RocksDB's different sequence numbers)
    let mut batch1 = new_batch(10);
    batch1.put(b"key".to_vec(), b"value_10".to_vec());
    engine.write_batch(batch1).unwrap();

    let mut batch2 = new_batch(20);
    batch2.put(b"key".to_vec(), b"value_20".to_vec());
    engine.write_batch(batch2).unwrap();

    // Both versions should exist and be accessible at appropriate timestamps
    assert_eq!(
        get_at_for_test(&engine, b"key", 10),
        Some(b"value_10".to_vec()),
        "Version at ts=10 should be accessible"
    );
    assert_eq!(
        get_at_for_test(&engine, b"key", 20),
        Some(b"value_20".to_vec()),
        "Version at ts=20 should be accessible"
    );
    assert_eq!(
        get_at_for_test(&engine, b"key", 15),
        Some(b"value_10".to_vec()),
        "ts=15 should see ts=10 version"
    );
}

/// Test duplicate keys under stress (ported from RocksDB's DuplicateSeq stress loop).
/// This verifies memtable stability with many versions of the same key.
#[test]
fn test_memtable_duplicate_stress() {
    let dir = TempDir::new().unwrap();
    let engine = create_engine(&dir);

    let num_versions = 1000;

    // Write many versions of the same key
    for ts in 1..=num_versions {
        let mut batch = new_batch(ts as Timestamp);
        batch.put(b"stress_key".to_vec(), format!("value_{ts}").into_bytes());
        engine.write_batch(batch).unwrap();
    }

    // Verify all versions are accessible
    for ts in 1..=num_versions {
        let expected = format!("value_{ts}");
        let actual = get_at_for_test(&engine, b"stress_key", ts as Timestamp);
        assert_eq!(
            actual,
            Some(expected.into_bytes()),
            "Version at ts={ts} should match"
        );
    }

    // Latest should be the highest timestamp
    let latest = get_for_test(&engine, b"stress_key");
    assert_eq!(latest, Some(format!("value_{num_versions}").into_bytes()));
}

/// Test concurrent merge writes (ported from RocksDB's ConcurrentMergeWrite).
/// Verifies thread-safety of memtable writes.
#[test]
fn test_memtable_concurrent_writes() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(create_engine(&dir));

    let num_threads = 4;
    let writes_per_thread = 250;
    let barrier = Arc::new(Barrier::new(num_threads));
    let ts_counter = Arc::new(AtomicU64::new(1));

    let handles: Vec<_> = (0..num_threads)
        .map(|tid| {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            let ts_counter = Arc::clone(&ts_counter);

            thread::spawn(move || {
                barrier.wait(); // Synchronize start

                for i in 0..writes_per_thread {
                    let ts = ts_counter.fetch_add(1, Ordering::SeqCst);
                    let mut batch = new_batch(ts);
                    let key = format!("concurrent_key_{tid}_{i}");
                    batch.put(key.into_bytes(), b"value".to_vec());
                    engine.write_batch(batch).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify all writes are present
    for tid in 0..num_threads {
        for i in 0..writes_per_thread {
            let key = format!("concurrent_key_{tid}_{i}");
            assert!(
                get_for_test(&engine, key.as_bytes()).is_some(),
                "Key {key} should exist"
            );
        }
    }
}

/// Test concurrent reads and writes with MVCC snapshots.
/// Verifies that readers see consistent snapshots while writers are active.
#[test]
fn test_memtable_concurrent_reads_writes_mvcc() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(create_engine(&dir));

    // Pre-populate with base data at ts=1
    let mut batch = new_batch(1);
    for i in 0..100 {
        let key = format!("mvcc_key_{i:04}");
        batch.put(key.into_bytes(), b"base_value".to_vec());
    }
    engine.write_batch(batch).unwrap();

    let num_readers = 4;
    let num_writers = 2;
    let keys_per_writer = 50; // Each writer has its own key range
    let barrier = Arc::new(Barrier::new(num_readers + num_writers));
    let errors = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_readers)
        .map(|_| {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);
            let errors = Arc::clone(&errors);

            thread::spawn(move || {
                barrier.wait();

                // Take a snapshot at ts=1
                for _ in 0..100 {
                    for i in 0..100 {
                        let key = format!("mvcc_key_{i:04}");
                        match get_at_for_test(&engine, key.as_bytes(), 1) {
                            Some(v) if v == b"base_value".to_vec() => {}
                            None => {
                                errors.fetch_add(1, Ordering::SeqCst);
                            }
                            Some(_) => {
                                // Should see base_value at ts=1, not updated value
                                errors.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                }
            })
        })
        .chain((0..num_writers).map(|tid| {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                // Each writer writes to its own distinct key range to avoid
                // concurrent writes to the same key (which requires external locking)
                let key_offset = tid * keys_per_writer;
                for i in 0..100 {
                    // Timestamps increase within each writer thread
                    let ts = (100 + i) as Timestamp;
                    let mut batch = new_batch(ts);
                    let key = format!("mvcc_key_{:04}", key_offset + (i % keys_per_writer));
                    batch.put(key.into_bytes(), format!("updated_{ts}").into_bytes());
                    engine.write_batch(batch).unwrap();
                }
            })
        }))
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // No errors should have occurred in readers
    assert_eq!(
        errors.load(Ordering::SeqCst),
        0,
        "Readers should see consistent snapshots"
    );
}

// ============================================================================
// COMPACTION TESTS (ported from compaction_job_test.cc)
// ============================================================================

/// Test simple deletion handling (ported from RocksDB's SimpleDeletion).
/// Verifies that tombstones properly mask older values after flush.
#[test]
fn test_compaction_simple_deletion() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write value at ts=3
    let mut batch1 = new_batch_with_lsn(3, 1);
    batch1.put(b"c".to_vec(), b"val".to_vec());
    engine.write_batch(batch1).unwrap();

    // Delete at ts=4
    let mut batch2 = new_batch_with_lsn(4, 2);
    batch2.delete(b"c".to_vec());
    engine.write_batch(batch2).unwrap();

    // Write another key
    let mut batch3 = new_batch_with_lsn(2, 3);
    batch3.put(b"b".to_vec(), b"val".to_vec());
    engine.write_batch(batch3).unwrap();

    // Flush to SST
    engine.flush_all_with_active().unwrap();

    // After flush, "c" should be deleted at latest ts
    assert_eq!(
        get_for_test(&engine, b"c"),
        None,
        "Key 'c' should be deleted at latest ts"
    );

    // But visible at ts=3
    assert_eq!(
        get_at_for_test(&engine, b"c", 3),
        Some(b"val".to_vec()),
        "Key 'c' should be visible at ts=3"
    );

    // "b" should be visible
    assert_eq!(
        get_for_test(&engine, b"b"),
        Some(b"val".to_vec()),
        "Key 'b' should be visible"
    );
}

/// Test output nothing case (ported from RocksDB's OutputNothing).
/// When all data is deleted, compaction should produce empty/no output.
#[test]
fn test_compaction_output_nothing() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write value at ts=1
    let mut batch1 = new_batch_with_lsn(1, 1);
    batch1.put(b"a".to_vec(), b"val".to_vec());
    engine.write_batch(batch1).unwrap();

    // Delete at ts=2
    let mut batch2 = new_batch_with_lsn(2, 2);
    batch2.delete(b"a".to_vec());
    engine.write_batch(batch2).unwrap();

    // Flush
    engine.flush_all_with_active().unwrap();

    // Latest read should return None (deleted)
    assert_eq!(
        get_for_test(&engine, b"a"),
        None,
        "Deleted key should not be visible"
    );

    // But historical read should work
    assert_eq!(
        get_at_for_test(&engine, b"a", 1),
        Some(b"val".to_vec()),
        "Historical version should be visible"
    );
}

/// Test simple overwrite (ported from RocksDB's SimpleOverwrite).
/// Newer values should overwrite older ones in compaction output.
#[test]
fn test_compaction_simple_overwrite() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // First write: a=val1, b=val2
    let mut batch1 = new_batch_with_lsn(1, 1);
    batch1.put(b"a".to_vec(), b"val1".to_vec());
    batch1.put(b"b".to_vec(), b"val2".to_vec());
    engine.write_batch(batch1).unwrap();

    // Overwrite: a=val3, b=val4
    let mut batch2 = new_batch_with_lsn(3, 2);
    batch2.put(b"a".to_vec(), b"val3".to_vec());
    batch2.put(b"b".to_vec(), b"val4".to_vec());
    engine.write_batch(batch2).unwrap();

    // Flush
    engine.flush_all_with_active().unwrap();

    // Latest values should be the newer ones
    assert_eq!(
        get_for_test(&engine, b"a"),
        Some(b"val3".to_vec()),
        "Key 'a' should have newer value"
    );
    assert_eq!(
        get_for_test(&engine, b"b"),
        Some(b"val4".to_vec()),
        "Key 'b' should have newer value"
    );

    // Older values still accessible via MVCC
    assert_eq!(
        get_at_for_test(&engine, b"a", 1),
        Some(b"val1".to_vec()),
        "Old value of 'a' should be accessible"
    );
    assert_eq!(
        get_at_for_test(&engine, b"b", 2),
        Some(b"val2".to_vec()),
        "Old value of 'b' should be accessible"
    );
}

/// Test snapshot visibility with deletions (ported from RocksDB's SingleDeleteSnapshots).
/// Verifies that snapshots see correct versions during and after compaction.
#[test]
fn test_snapshot_visibility_with_deletions() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Scenario: Multiple keys with various delete/put patterns
    // Snapshot at ts=10 and ts=20

    // Write at ts=5
    let mut batch1 = new_batch_with_lsn(5, 1);
    batch1.put(b"a".to_vec(), b"a_v1".to_vec());
    batch1.put(b"b".to_vec(), b"b_v1".to_vec());
    batch1.put(b"c".to_vec(), b"c_v1".to_vec());
    engine.write_batch(batch1).unwrap();

    // Update 'b' at ts=12 (after first snapshot)
    let mut batch2 = new_batch_with_lsn(12, 2);
    batch2.put(b"b".to_vec(), b"b_v2".to_vec());
    engine.write_batch(batch2).unwrap();

    // Delete 'a' at ts=15
    let mut batch3 = new_batch_with_lsn(15, 3);
    batch3.delete(b"a".to_vec());
    engine.write_batch(batch3).unwrap();

    // Update 'c' at ts=22 (after second snapshot)
    let mut batch4 = new_batch_with_lsn(22, 4);
    batch4.put(b"c".to_vec(), b"c_v2".to_vec());
    engine.write_batch(batch4).unwrap();

    // Flush all
    engine.flush_all_with_active().unwrap();

    // Snapshot at ts=10: should see a_v1, b_v1, c_v1
    assert_eq!(get_at_for_test(&engine, b"a", 10), Some(b"a_v1".to_vec()));
    assert_eq!(get_at_for_test(&engine, b"b", 10), Some(b"b_v1".to_vec()));
    assert_eq!(get_at_for_test(&engine, b"c", 10), Some(b"c_v1".to_vec()));

    // Snapshot at ts=20: should see None (deleted), b_v2, c_v1
    assert_eq!(
        get_at_for_test(&engine, b"a", 20),
        None,
        "'a' should be deleted at ts=20"
    );
    assert_eq!(get_at_for_test(&engine, b"b", 20), Some(b"b_v2".to_vec()));
    assert_eq!(get_at_for_test(&engine, b"c", 20), Some(b"c_v1".to_vec()));

    // Latest: should see None, b_v2, c_v2
    assert_eq!(get_for_test(&engine, b"a"), None);
    assert_eq!(get_for_test(&engine, b"b"), Some(b"b_v2".to_vec()));
    assert_eq!(get_for_test(&engine, b"c"), Some(b"c_v2".to_vec()));
}

/// Test tombstone visible in snapshot (ported from RocksDB's similar test).
/// When a key is deleted, historical snapshots should still see the old value,
/// but newer snapshots should see the deletion.
#[test]
fn test_tombstone_visible_in_snapshot() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write at ts=10
    let mut batch1 = new_batch_with_lsn(10, 1);
    batch1.put(b"tombstone_key".to_vec(), b"original_value".to_vec());
    engine.write_batch(batch1).unwrap();

    // Flush
    engine.flush_all_with_active().unwrap();

    // Delete at ts=20
    let mut batch2 = new_batch_with_lsn(20, 2);
    batch2.delete(b"tombstone_key".to_vec());
    engine.write_batch(batch2).unwrap();

    // Flush again (tombstone goes to SST)
    engine.flush_all_with_active().unwrap();

    // Snapshot before delete (ts=15) should see value
    assert_eq!(
        get_at_for_test(&engine, b"tombstone_key", 15),
        Some(b"original_value".to_vec()),
        "Snapshot before delete should see value"
    );

    // Snapshot after delete (ts=25) should see None
    assert_eq!(
        get_at_for_test(&engine, b"tombstone_key", 25),
        None,
        "Snapshot after delete should see tombstone"
    );
}

// ============================================================================
// FLUSH TESTS (ported from db_flush_test.cc)
// ============================================================================

/// Test concurrent flush operations don't corrupt data.
/// Inspired by RocksDB's FlushWhileWritingManifest.
#[test]
fn test_flush_while_writing() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new({
        let config = LsmConfigBuilder::new(dir.path())
            .memtable_size(128) // Very small to trigger frequent rotations
            .max_frozen_memtables(32)
            .build_unchecked();
        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(dir.path());
        let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());
        LsmEngine::open_with_recovery(config, lsn_provider, ilog, Version::new()).unwrap()
    });

    let num_writers = 4;
    let writes_per_writer = 100;
    let barrier = Arc::new(Barrier::new(num_writers + 1)); // +1 for flusher

    let handles: Vec<_> = (0..num_writers)
        .map(|tid| {
            let engine = Arc::clone(&engine);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                for i in 0..writes_per_writer {
                    let ts = (tid * writes_per_writer + i + 1) as Timestamp;
                    let mut batch = new_batch_with_lsn(ts, ts);
                    let key = format!("flush_while_write_{tid}_{i}");
                    batch.put(key.into_bytes(), format!("value_{ts}").into_bytes());
                    engine.write_batch(batch).unwrap();

                    // Occasional small sleep to vary timing
                    if i % 20 == 0 {
                        thread::sleep(Duration::from_micros(100));
                    }
                }
            })
        })
        .collect();

    // Flusher thread
    let flusher_engine = Arc::clone(&engine);
    let flusher_barrier = Arc::clone(&barrier);
    let flusher = thread::spawn(move || {
        flusher_barrier.wait();

        for _ in 0..50 {
            let _ = flusher_engine.flush_all();
            thread::sleep(Duration::from_millis(5));
        }
    });

    for h in handles {
        h.join().unwrap();
    }
    flusher.join().unwrap();

    // Final flush
    engine.flush_all_with_active().unwrap();

    // Verify all data is present
    for tid in 0..num_writers {
        for i in 0..writes_per_writer {
            let key = format!("flush_while_write_{tid}_{i}");
            let value = get_for_test(&engine, key.as_bytes());
            assert!(
                value.is_some(),
                "Key {key} should exist after concurrent flush/write"
            );
        }
    }
}

/// Test that flush produces correct statistics.
/// Inspired by RocksDB's StatisticsGarbageBasic.
#[test]
fn test_flush_statistics() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write multiple versions of the same keys
    for round in 0..10 {
        let ts = (round + 1) as Timestamp;
        let mut batch = new_batch_with_lsn(ts, ts);
        batch.put(
            b"stat_key_1".to_vec(),
            format!("value_{round}").into_bytes(),
        );
        batch.put(
            b"stat_key_2".to_vec(),
            format!("value_{round}").into_bytes(),
        );
        batch.put(
            b"stat_key_3".to_vec(),
            format!("value_{round}").into_bytes(),
        );
        engine.write_batch(batch).unwrap();
    }

    // Verify data is readable before flush
    let val = get_for_test(&engine, b"stat_key_1");
    assert!(val.is_some(), "Memtable should have data");

    // Get stats before flush (SST count should be 0)
    let stats_before = engine.stats();
    assert_eq!(stats_before.total_sst_count, 0, "No SST before flush");

    // Flush
    engine.flush_all_with_active().unwrap();

    // Get stats after flush
    let stats_after = engine.stats();
    assert!(
        stats_after.total_sst_count > 0,
        "Should have SST files after flush"
    );

    // Verify all versions are accessible
    for round in 0..10 {
        let ts = (round + 1) as Timestamp;
        assert_eq!(
            get_at_for_test(&engine, b"stat_key_1", ts),
            Some(format!("value_{round}").into_bytes()),
            "Version at ts={ts} should be accessible"
        );
    }
}

// ============================================================================
// SST ITERATOR TESTS (similar to RocksDB's iterator tests)
// ============================================================================

/// Test SST iterator with various key patterns.
#[test]
fn test_sst_iterator_key_patterns() {
    let dir = TempDir::new().unwrap();
    let sst_path = dir.path().join("test.sst");

    // Create SST with various key patterns
    // Note: Keys must be added in sorted order for SST
    let options = SstBuilderOptions::default();
    let mut builder = SstBuilder::new(&sst_path, options).unwrap();

    // Binary keys with special bytes (sorted by byte values)
    // 0x00, 0x01, 0x02 sorts before printable characters
    builder.add(&[0x00, 0x01, 0x02], b"binary_value").unwrap();

    // Single byte keys
    builder.add(b"a", b"a_value").unwrap();
    builder.add(b"b", b"b_value").unwrap();

    // Multi-byte keys
    builder.add(b"key", b"key_value").unwrap();
    builder.add(b"key1", b"key1_value").unwrap();
    builder.add(b"key12", b"key12_value").unwrap();

    // High byte value keys sort last
    builder
        .add(&[0xFF, 0xFE, 0xFD], b"high_bytes_value")
        .unwrap();

    builder.finish(1, 0).unwrap();

    // Read and verify
    let reader = SstReaderRef::open(&sst_path).unwrap();
    let mut iter = SstIterator::new(reader).unwrap();

    let mut keys = Vec::new();
    while iter.valid() {
        keys.push(iter.key().to_vec());
        iter.next().unwrap();
    }

    // Keys should be in sorted order
    assert_eq!(keys.len(), 7);
    for i in 1..keys.len() {
        assert!(
            keys[i - 1] < keys[i],
            "Keys should be sorted: {:?} < {:?}",
            keys[i - 1],
            keys[i]
        );
    }
}

/// Test SST with many entries (stress test).
#[test]
fn test_sst_many_entries() {
    let dir = TempDir::new().unwrap();
    let sst_path = dir.path().join("large.sst");

    let num_entries = 10000;

    // Create SST
    let options = SstBuilderOptions::default();
    let mut builder = SstBuilder::new(&sst_path, options).unwrap();

    for i in 0..num_entries {
        let key = format!("key_{i:08}");
        let value = format!("value_{i:08}");
        builder.add(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let meta = builder.finish(1, 0).unwrap();
    assert_eq!(meta.entry_count, num_entries as u64);

    // Read and count
    let reader = SstReaderRef::open(&sst_path).unwrap();
    let mut iter = SstIterator::new(reader).unwrap();

    let mut count = 0;
    let mut prev_key: Option<Vec<u8>> = None;
    while iter.valid() {
        let key = iter.key().to_vec();

        // Verify ordering
        if let Some(ref prev) = prev_key {
            assert!(
                key > *prev,
                "Keys should be strictly ordered: {key:?} > {prev:?}"
            );
        }

        prev_key = Some(key);
        count += 1;
        iter.next().unwrap();
    }

    assert_eq!(count, num_entries);
}

/// Test SST with duplicate keys (MVCC scenario).
#[test]
fn test_sst_mvcc_keys() {
    let dir = TempDir::new().unwrap();
    let sst_path = dir.path().join("mvcc.sst");

    // MVCC key format: user_key || !commit_ts (8 bytes big-endian)
    fn encode_mvcc_key(user_key: &[u8], ts: u64) -> Vec<u8> {
        let mut key = user_key.to_vec();
        key.extend_from_slice(&(!ts).to_be_bytes());
        key
    }

    let options = SstBuilderOptions::default();
    let mut builder = SstBuilder::new(&sst_path, options).unwrap();

    // Multiple versions of "key1": ts=30, 20, 10 (in MVCC order, higher ts first)
    builder.add(&encode_mvcc_key(b"key1", 30), b"v3").unwrap();
    builder.add(&encode_mvcc_key(b"key1", 20), b"v2").unwrap();
    builder.add(&encode_mvcc_key(b"key1", 10), b"v1").unwrap();

    // Multiple versions of "key2"
    builder
        .add(&encode_mvcc_key(b"key2", 25), b"k2_v2")
        .unwrap();
    builder
        .add(&encode_mvcc_key(b"key2", 15), b"k2_v1")
        .unwrap();

    builder.finish(1, 0).unwrap();

    // Read and verify order
    let reader = SstReaderRef::open(&sst_path).unwrap();
    let mut iter = SstIterator::new(reader).unwrap();

    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while iter.valid() {
        entries.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.next().unwrap();
    }

    // Should have 5 entries
    assert_eq!(entries.len(), 5);

    // All "key1" versions should come before "key2" versions
    // Within "key1", higher ts (lower encoded value due to !ts) comes first
    assert_eq!(entries[0].1, b"v3"); // key1@30
    assert_eq!(entries[1].1, b"v2"); // key1@20
    assert_eq!(entries[2].1, b"v1"); // key1@10
    assert_eq!(entries[3].1, b"k2_v2"); // key2@25
    assert_eq!(entries[4].1, b"k2_v1"); // key2@15
}

// ============================================================================
// END-TO-END COMPACTION TESTS
// ============================================================================

/// Test that compaction preserves all MVCC versions.
#[test]
fn test_compaction_preserves_mvcc_versions() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write multiple versions in ascending order (matching real transaction behavior)
    for ts in 1..=5 {
        let mut batch = new_batch_with_lsn(ts as Timestamp, ts as u64);
        batch.put(b"mvcc_test".to_vec(), format!("value_ts_{ts}").into_bytes());
        engine.write_batch(batch).unwrap();
    }

    // Flush to SST
    engine.flush_all_with_active().unwrap();

    // Verify all versions are preserved
    for ts in 1..=5 {
        let value = get_at_for_test(&engine, b"mvcc_test", ts as Timestamp);
        assert_eq!(
            value,
            Some(format!("value_ts_{ts}").into_bytes()),
            "Version at ts={ts} should be preserved after compaction"
        );
    }
}

/// Test scan with range and MVCC after flush.
#[test]
fn test_scan_range_after_flush() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write data at ts=10
    let mut batch1 = new_batch_with_lsn(10, 1);
    batch1.put(b"a".to_vec(), b"a_v1".to_vec());
    batch1.put(b"b".to_vec(), b"b_v1".to_vec());
    batch1.put(b"c".to_vec(), b"c_v1".to_vec());
    batch1.put(b"d".to_vec(), b"d_v1".to_vec());
    engine.write_batch(batch1).unwrap();

    // Update some at ts=20
    let mut batch2 = new_batch_with_lsn(20, 2);
    batch2.put(b"b".to_vec(), b"b_v2".to_vec());
    batch2.put(b"c".to_vec(), b"c_v2".to_vec());
    engine.write_batch(batch2).unwrap();

    // Flush
    engine.flush_all_with_active().unwrap();

    // Range scan at ts=15: should see v1 versions
    let range = b"b".to_vec()..b"d".to_vec();
    let results: Vec<_> = scan_at_for_test(&engine, &range, 15);
    assert_eq!(results.len(), 2);
    assert!(results.iter().any(|(k, v)| k == b"b" && v == b"b_v1"));
    assert!(results.iter().any(|(k, v)| k == b"c" && v == b"c_v1"));

    // Range scan at ts=25: should see v2 versions where updated
    let results: Vec<_> = scan_at_for_test(&engine, &range, 25);
    assert_eq!(results.len(), 2);
    assert!(results.iter().any(|(k, v)| k == b"b" && v == b"b_v2"));
    assert!(results.iter().any(|(k, v)| k == b"c" && v == b"c_v2"));
}

/// Test that deleted keys don't appear in scan.
#[test]
fn test_scan_excludes_deleted_keys() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write data
    let mut batch1 = new_batch_with_lsn(10, 1);
    for c in b'a'..=b'e' {
        batch1.put(vec![c], format!("{}_value", c as char).into_bytes());
    }
    engine.write_batch(batch1).unwrap();

    // Delete 'b' and 'd'
    let mut batch2 = new_batch_with_lsn(20, 2);
    batch2.delete(vec![b'b']);
    batch2.delete(vec![b'd']);
    engine.write_batch(batch2).unwrap();

    // Flush
    engine.flush_all_with_active().unwrap();

    // Scan at ts=25: should not include 'b' and 'd'
    let range = b"a".to_vec()..b"f".to_vec();
    let results: Vec<_> = scan_at_for_test(&engine, &range, 25);
    let keys: HashSet<_> = results.iter().map(|(k, _)| k.clone()).collect();

    assert!(!keys.contains(b"b".as_slice()), "'b' should be excluded");
    assert!(!keys.contains(b"d".as_slice()), "'d' should be excluded");
    assert!(keys.contains(b"a".as_slice()), "'a' should be included");
    assert!(keys.contains(b"c".as_slice()), "'c' should be included");
    assert!(keys.contains(b"e".as_slice()), "'e' should be included");

    // Scan at ts=15: should include all (before delete)
    let results: Vec<_> = scan_at_for_test(&engine, &range, 15);
    assert_eq!(results.len(), 5, "All 5 keys should be visible at ts=15");
}

// ============================================================================
// RECOVERY TESTS
// ============================================================================

/// Test that flushed data survives restart.
#[test]
fn test_recovery_flushed_data() {
    let dir = TempDir::new().unwrap();

    // First session: write and flush
    {
        let (engine, _ilog) = create_durable_engine(&dir);

        let mut batch = new_batch_with_lsn(100, 1);
        batch.put(b"recovery_key".to_vec(), b"recovery_value".to_vec());
        engine.write_batch(batch).unwrap();

        engine.flush_all_with_active().unwrap();
    }

    // Second session: recover and verify
    {
        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(dir.path());
        let (ilog, version, _orphans) =
            IlogService::recover(ilog_config, Arc::clone(&lsn_provider)).unwrap();

        let config = LsmConfigBuilder::new(dir.path())
            .memtable_size(256)
            .max_frozen_memtables(16)
            .build_unchecked();

        let engine =
            LsmEngine::open_with_recovery(config, lsn_provider, Arc::new(ilog), version).unwrap();

        let value = get_for_test(&engine, b"recovery_key");
        assert_eq!(
            value,
            Some(b"recovery_value".to_vec()),
            "Data should survive restart"
        );
    }
}

/// Test that multiple flushes and restart work correctly.
#[test]
fn test_recovery_multiple_flushes() {
    let dir = TempDir::new().unwrap();

    // First session: multiple writes and flushes
    {
        let (engine, _ilog) = create_durable_engine(&dir);

        for round in 0..5 {
            let ts_base = round * 10;
            let lsn_base = round * 10;

            for i in 0..5 {
                let ts = (ts_base + i + 1) as Timestamp;
                let lsn = (lsn_base + i + 1) as u64;
                let mut batch = new_batch_with_lsn(ts, lsn);
                let key = format!("multi_flush_key_{i}");
                batch.put(key.into_bytes(), format!("round_{round}").into_bytes());
                engine.write_batch(batch).unwrap();
            }

            engine.flush_all_with_active().unwrap();
        }
    }

    // Second session: recover and verify
    {
        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(dir.path());
        let (ilog, version, _orphans) =
            IlogService::recover(ilog_config, Arc::clone(&lsn_provider)).unwrap();

        let config = LsmConfigBuilder::new(dir.path())
            .memtable_size(256)
            .max_frozen_memtables(16)
            .build_unchecked();

        let engine =
            LsmEngine::open_with_recovery(config, lsn_provider, Arc::new(ilog), version).unwrap();

        // Latest values should be from round 4
        for i in 0..5 {
            let key = format!("multi_flush_key_{i}");
            let value = get_for_test(&engine, key.as_bytes());
            assert_eq!(
                value,
                Some(b"round_4".to_vec()),
                "Key {key} should have latest value"
            );
        }

        // Verify SST count
        let stats = engine.stats();
        assert!(
            stats.total_sst_count > 0,
            "Should have SST files after recovery"
        );
    }
}

/// Test that MVCC versions survive recovery.
#[test]
fn test_recovery_mvcc_versions() {
    let dir = TempDir::new().unwrap();

    // First session: write multiple versions
    {
        let (engine, _ilog) = create_durable_engine(&dir);

        for ts in [10, 20, 30, 40, 50] {
            let mut batch = new_batch_with_lsn(ts as Timestamp, ts as u64);
            batch.put(
                b"mvcc_recovery".to_vec(),
                format!("value_ts_{ts}").into_bytes(),
            );
            engine.write_batch(batch).unwrap();
        }

        engine.flush_all_with_active().unwrap();
    }

    // Second session: recover and verify all versions
    {
        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(dir.path());
        let (ilog, version, _orphans) =
            IlogService::recover(ilog_config, Arc::clone(&lsn_provider)).unwrap();

        let config = LsmConfigBuilder::new(dir.path())
            .memtable_size(256)
            .max_frozen_memtables(16)
            .build_unchecked();

        let engine =
            LsmEngine::open_with_recovery(config, lsn_provider, Arc::new(ilog), version).unwrap();

        // Verify each version is accessible
        for ts in [10, 20, 30, 40, 50] {
            let value = get_at_for_test(&engine, b"mvcc_recovery", ts as Timestamp);
            assert_eq!(
                value,
                Some(format!("value_ts_{ts}").into_bytes()),
                "Version at ts={ts} should survive recovery"
            );
        }
    }
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

/// Test empty batch write.
#[test]
fn test_empty_batch() {
    let dir = TempDir::new().unwrap();
    let engine = create_engine(&dir);

    let batch = new_batch(1);
    // Empty batch should not cause errors
    engine.write_batch(batch).unwrap();

    // Engine should still work
    let mut batch = new_batch(2);
    batch.put(b"key".to_vec(), b"value".to_vec());
    engine.write_batch(batch).unwrap();

    assert_eq!(get_for_test(&engine, b"key"), Some(b"value".to_vec()));
}

/// Test very large values.
#[test]
fn test_large_values() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // 1MB value
    let large_value: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();

    let mut batch = new_batch_with_lsn(1, 1);
    batch.put(b"large_key".to_vec(), large_value.clone());
    engine.write_batch(batch).unwrap();

    // Verify before flush
    let value = get_for_test(&engine, b"large_key");
    assert_eq!(value.as_ref().map(|v| v.len()), Some(1024 * 1024));

    // Flush and verify
    engine.flush_all_with_active().unwrap();
    let value = get_for_test(&engine, b"large_key");
    assert_eq!(value, Some(large_value));
}

/// Test single-byte and small keys.
///
/// Note: Keys starting with 0xFF have special handling in MVCC encoding
/// for disambiguation with timestamp suffixes, so we test with regular keys.
#[test]
fn test_small_keys() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Test single byte keys (avoiding 0xFF which has special MVCC handling)
    let mut batch = new_batch_with_lsn(1, 1);
    batch.put(vec![0x00], b"null_byte_value".to_vec());
    batch.put(vec![0x01], b"one_byte_value".to_vec());
    batch.put(vec![0x7F], b"mid_byte_value".to_vec());
    batch.put(vec![0xFE], b"high_byte_value".to_vec());
    engine.write_batch(batch).unwrap();

    // Verify before flush
    assert_eq!(
        get_for_test(&engine, &[0x00]),
        Some(b"null_byte_value".to_vec()),
        "Null byte key should work"
    );
    assert_eq!(
        get_for_test(&engine, &[0x01]),
        Some(b"one_byte_value".to_vec()),
        "Single byte key should work"
    );
    assert_eq!(
        get_for_test(&engine, &[0x7F]),
        Some(b"mid_byte_value".to_vec()),
        "0x7F byte key should work"
    );
    assert_eq!(
        get_for_test(&engine, &[0xFE]),
        Some(b"high_byte_value".to_vec()),
        "0xFE byte key should work"
    );

    // Flush and verify
    engine.flush_all_with_active().unwrap();
    assert_eq!(
        get_for_test(&engine, &[0x00]),
        Some(b"null_byte_value".to_vec()),
        "Null byte key should work after flush"
    );
    assert_eq!(
        get_for_test(&engine, &[0xFE]),
        Some(b"high_byte_value".to_vec()),
        "0xFE byte key should work after flush"
    );
}

/// Test binary keys with all byte values.
#[test]
fn test_binary_keys() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    let mut batch = new_batch_with_lsn(1, 1);

    // Keys with all possible byte values
    for byte in 0u8..=255 {
        batch.put(vec![byte], vec![byte]);
    }
    engine.write_batch(batch).unwrap();

    // Flush
    engine.flush_all_with_active().unwrap();

    // Verify all
    for byte in 0u8..=255 {
        let value = get_for_test(&engine, &[byte]);
        assert_eq!(
            value,
            Some(vec![byte]),
            "Binary key 0x{byte:02X} should be retrievable"
        );
    }
}

/// Test timestamp edge cases.
#[test]
fn test_timestamp_edge_cases() {
    let dir = TempDir::new().unwrap();
    let engine = create_engine(&dir);

    // Test with min timestamp
    let mut batch = new_batch(0);
    batch.put(b"ts_0".to_vec(), b"value_0".to_vec());
    engine.write_batch(batch).unwrap();

    // Test with max timestamp (u64::MAX might cause issues)
    let mut batch = new_batch(u64::MAX - 1);
    batch.put(b"ts_max".to_vec(), b"value_max".to_vec());
    engine.write_batch(batch).unwrap();

    // Verify
    assert_eq!(
        get_at_for_test(&engine, b"ts_0", 0),
        Some(b"value_0".to_vec())
    );
    assert_eq!(
        get_at_for_test(&engine, b"ts_max", u64::MAX - 1),
        Some(b"value_max".to_vec())
    );
}
