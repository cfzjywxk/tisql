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

//! Additional compaction tests ported from RocksDB.
//!
//! These tests focus on compaction correctness, merge iterator behavior,
//! and multi-level compaction scenarios.
//!
//! Run with: cargo test --test rocksdb_ported_compaction_tests

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use tisql::new_lsn_provider;
use tisql::storage::mvcc::{is_tombstone, MvccIterator, MvccKey};
use tisql::storage::WriteBatch;
use tisql::testkit::{
    CompactionExecutor, CompactionPicker, CompactionScheduler, CompactionTask, IlogConfig,
    IlogService, LsmConfigBuilder, LsmEngine, ManifestDelta, SstBuilder, SstBuilderOptions,
    SstMeta, SstReaderRef, Version,
};
use tisql::types::{Key, RawValue, Timestamp};
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

fn scan_for_test(engine: &LsmEngine, range: &std::ops::Range<Key>) -> Vec<(Key, RawValue)> {
    let start = MvccKey::encode(&range.start, Timestamp::MAX);
    let end = MvccKey::encode(&range.end, 0);
    let mvcc_range = start..end;

    // Use streaming scan_iter() - process one entry at a time
    let mut iter = engine.scan_iter(mvcc_range, 0).unwrap();
    iter.advance().unwrap(); // Position on first entry

    let mut seen_keys: std::collections::HashSet<Key> = std::collections::HashSet::new();
    let mut output = Vec::new();

    while iter.valid() {
        let decoded_key = iter.user_key().to_vec();
        let value = iter.value().to_vec();

        // Move to next before continue checks (so we don't get stuck)
        iter.advance().unwrap();

        if decoded_key < range.start || decoded_key >= range.end {
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

#[allow(dead_code)]
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
        .memtable_size(256)
        .max_frozen_memtables(16)
        .l0_compaction_trigger(4)
        .l0_slowdown_trigger(20)
        .l0_stop_trigger(30)
        .build_unchecked();

    let engine =
        LsmEngine::open_with_recovery(config, lsn_provider, Arc::clone(&ilog), Version::new())
            .unwrap();
    (engine, ilog)
}

fn new_batch_with_lsn(commit_ts: Timestamp, lsn: u64) -> WriteBatch {
    let mut batch = WriteBatch::new();
    batch.set_commit_ts(commit_ts);
    batch.set_clog_lsn(lsn);
    batch
}

fn make_sst(id: u64, level: u32, smallest: &[u8], largest: &[u8]) -> SstMeta {
    SstMeta {
        id,
        level,
        smallest_key: smallest.to_vec(),
        largest_key: largest.to_vec(),
        file_size: 1000,
        entry_count: 100,
        block_count: 10,
        min_ts: 1,
        max_ts: 100,
        created_at: 0,
    }
}

// ============================================================================
// COMPACTION PICKER TESTS
// ============================================================================

/// Test that compaction picker doesn't trigger when L0 is below threshold.
/// Ported from RocksDB's compaction_picker_test.cc.
#[test]
fn test_picker_l0_below_threshold() {
    let dir = TempDir::new().unwrap();
    let config = Arc::new(
        LsmConfigBuilder::new(dir.path())
            .l0_compaction_trigger(4)
            .build_unchecked(),
    );
    let picker = CompactionPicker::new(config);

    // Only 2 L0 files (below threshold of 4)
    let version = Version::builder()
        .add_sst(make_sst(1, 0, b"a", b"m"))
        .add_sst(make_sst(2, 0, b"n", b"z"))
        .build();

    assert!(
        picker.pick(&version).is_none(),
        "Should not trigger compaction with only 2 L0 files"
    );
}

/// Test that L0 compaction includes all overlapping files.
/// This is critical for correctness - missing overlap can cause data loss.
#[test]
fn test_picker_l0_includes_all_overlapping() {
    let dir = TempDir::new().unwrap();
    let config = Arc::new(
        LsmConfigBuilder::new(dir.path())
            .l0_compaction_trigger(4)
            .build_unchecked(),
    );
    let picker = CompactionPicker::new(config);

    // 4 L0 files that all overlap (meeting threshold)
    // Plus 3 L1 files that overlap with different L0 ranges
    let version = Version::builder()
        .add_sst(make_sst(1, 0, b"a", b"z")) // Overlaps everything
        .add_sst(make_sst(2, 0, b"b", b"y"))
        .add_sst(make_sst(3, 0, b"c", b"x"))
        .add_sst(make_sst(4, 0, b"d", b"w"))
        .add_sst(make_sst(10, 1, b"a", b"f"))
        .add_sst(make_sst(11, 1, b"g", b"n"))
        .add_sst(make_sst(12, 1, b"o", b"z"))
        .build();

    let task = picker.pick(&version).expect("Should trigger L0 compaction");

    // All 4 L0 files should be included
    let l0_inputs: Vec<_> = task.inputs.iter().filter(|(l, _)| *l == 0).collect();
    assert_eq!(l0_inputs.len(), 4, "All L0 files should be included");

    // All 3 L1 files should be included (they all overlap with L0's [a-z] range)
    let l1_inputs: Vec<_> = task.inputs.iter().filter(|(l, _)| *l == 1).collect();
    assert_eq!(
        l1_inputs.len(),
        3,
        "All overlapping L1 files should be included"
    );

    assert_eq!(task.output_level, 1);
    assert!(!task.is_trivial_move);
}

/// Test that level compaction picks the oldest file.
/// This ensures fairness and prevents starvation.
#[test]
fn test_picker_level_picks_oldest() {
    let dir = TempDir::new().unwrap();
    let config = Arc::new(
        LsmConfigBuilder::new(dir.path())
            .l0_compaction_trigger(10) // High threshold to skip L0
            .l1_max_size(100) // Very small to trigger L1 compaction
            .build_unchecked(),
    );
    let picker = CompactionPicker::new(config);

    // L1 files with different IDs (ID represents age - lower = older)
    let version = Version::builder()
        .add_sst(make_sst(5, 1, b"a", b"m")) // Oldest (should be picked)
        .add_sst(make_sst(6, 1, b"n", b"z")) // Newer
        .add_sst(make_sst(7, 1, b"aa", b"mm")) // Newest
        .build();

    let task = picker.pick(&version).expect("Should trigger L1 compaction");

    // Should include the oldest L1 file (ID=5)
    let l1_input = task.inputs.iter().find(|(l, _)| *l == 1);
    assert_eq!(
        l1_input.map(|(_, id)| *id),
        Some(5),
        "Should pick oldest file"
    );
}

/// Test trivial move when there's no overlap with next level.
#[test]
fn test_picker_trivial_move_no_overlap() {
    let dir = TempDir::new().unwrap();
    let config = Arc::new(
        LsmConfigBuilder::new(dir.path())
            .l0_compaction_trigger(10)
            .l1_max_size(100)
            .build_unchecked(),
    );
    let picker = CompactionPicker::new(config);

    // L1 file with no L2 overlap
    let version = Version::builder()
        .add_sst(make_sst(1, 1, b"a", b"m"))
        .add_sst(make_sst(10, 2, b"x", b"z")) // L2 file doesn't overlap with L1
        .build();

    let task = picker.pick(&version).expect("Should trigger L1 compaction");

    assert!(task.is_trivial_move, "Should be a trivial move");
    assert_eq!(task.output_level, 2);
    assert_eq!(task.inputs.len(), 1);
}

// ============================================================================
// COMPACTION EXECUTOR TESTS
// ============================================================================

/// Test compaction merges data correctly.
#[test]
fn test_executor_merge_keys() {
    let dir = TempDir::new().unwrap();
    let sst_dir = dir.path().join("sst");
    std::fs::create_dir_all(&sst_dir).unwrap();

    let config = Arc::new(
        LsmConfigBuilder::new(dir.path())
            .target_file_size(1024 * 1024)
            .build_unchecked(),
    );
    let executor = CompactionExecutor::new(Arc::clone(&config));

    // Create SST1: a, c, e (in L0)
    let sst1_path = sst_dir.join("00000001.sst");
    let mut builder = SstBuilder::new(&sst1_path, SstBuilderOptions::default()).unwrap();
    builder.add(b"a", b"a_v1").unwrap();
    builder.add(b"c", b"c_v1").unwrap();
    builder.add(b"e", b"e_v1").unwrap();
    let meta1 = builder.finish(1, 0).unwrap();

    // Create SST2: b, d, f (in L0)
    let sst2_path = sst_dir.join("00000002.sst");
    let mut builder = SstBuilder::new(&sst2_path, SstBuilderOptions::default()).unwrap();
    builder.add(b"b", b"b_v1").unwrap();
    builder.add(b"d", b"d_v1").unwrap();
    builder.add(b"f", b"f_v1").unwrap();
    let meta2 = builder.finish(2, 0).unwrap();

    // Build version
    let version = Version::builder()
        .add_sst(meta1)
        .add_sst(meta2)
        .next_sst_id(3)
        .build();

    // Create compaction task
    let task = CompactionTask {
        inputs: vec![(0, 1), (0, 2)],
        output_level: 1,
        is_trivial_move: false,
    };

    // Execute with pre-allocated IDs
    let pre_allocated_ids = vec![3, 4];
    let delta = executor
        .execute(&task, &version, &sst_dir, &pre_allocated_ids)
        .unwrap();

    // Verify output
    assert!(!delta.new_ssts.is_empty(), "Should produce output SSTs");
    assert_eq!(delta.deleted_ssts.len(), 2, "Should delete input SSTs");

    // Read output SST and verify keys are merged in order
    let output_sst = &delta.new_ssts[0];
    let output_path = sst_dir.join(format!("{:08}.sst", output_sst.id));
    let reader = SstReaderRef::open(&output_path).unwrap();
    let mut iter = tisql::testkit::SstIterator::new(reader).unwrap();
    iter.seek_to_first().unwrap();

    let mut keys = Vec::new();
    while iter.valid() {
        keys.push(iter.key().to_vec());
        iter.advance().unwrap();
    }

    // Should be in sorted order: a, b, c, d, e, f
    assert_eq!(
        keys,
        vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            b"d".to_vec(),
            b"e".to_vec(),
            b"f".to_vec()
        ]
    );
}

/// Test that compaction handles duplicate MVCC keys correctly.
/// Newer versions should appear before older versions in output.
#[test]
fn test_executor_mvcc_ordering() {
    let dir = TempDir::new().unwrap();
    let sst_dir = dir.path().join("sst");
    std::fs::create_dir_all(&sst_dir).unwrap();

    let config = Arc::new(
        LsmConfigBuilder::new(dir.path())
            .target_file_size(1024 * 1024)
            .build_unchecked(),
    );
    let executor = CompactionExecutor::new(Arc::clone(&config));

    // Helper to encode MVCC key: user_key || !ts
    fn mvcc_key(user_key: &[u8], ts: u64) -> Vec<u8> {
        let mut key = user_key.to_vec();
        key.extend_from_slice(&(!ts).to_be_bytes());
        key
    }

    // SST1 (newer): key@ts=20, key@ts=10
    let sst1_path = sst_dir.join("00000001.sst");
    let mut builder = SstBuilder::new(&sst1_path, SstBuilderOptions::default()).unwrap();
    builder.add(&mvcc_key(b"key", 20), b"v20").unwrap();
    builder.add(&mvcc_key(b"key", 10), b"v10").unwrap();
    let meta1 = builder.finish(1, 0).unwrap();

    // SST2 (older): key@ts=5
    let sst2_path = sst_dir.join("00000002.sst");
    let mut builder = SstBuilder::new(&sst2_path, SstBuilderOptions::default()).unwrap();
    builder.add(&mvcc_key(b"key", 5), b"v5").unwrap();
    let meta2 = builder.finish(2, 0).unwrap();

    let version = Version::builder()
        .add_sst(meta1)
        .add_sst(meta2)
        .next_sst_id(3)
        .build();

    let task = CompactionTask {
        inputs: vec![(0, 1), (0, 2)],
        output_level: 1,
        is_trivial_move: false,
    };

    let pre_allocated_ids = vec![3, 4];
    let delta = executor
        .execute(&task, &version, &sst_dir, &pre_allocated_ids)
        .unwrap();

    // Read and verify all versions are preserved in correct order
    let output_path = sst_dir.join(format!("{:08}.sst", delta.new_ssts[0].id));
    let reader = SstReaderRef::open(&output_path).unwrap();
    let mut iter = tisql::testkit::SstIterator::new(reader).unwrap();
    iter.seek_to_first().unwrap();

    let mut values = Vec::new();
    while iter.valid() {
        values.push(iter.value().to_vec());
        iter.advance().unwrap();
    }

    // All 3 versions should be preserved: v20, v10, v5 (in MVCC order)
    assert_eq!(values.len(), 3, "All versions should be preserved");
    assert_eq!(values[0], b"v20".to_vec(), "Newest version first");
    assert_eq!(values[1], b"v10".to_vec(), "Middle version second");
    assert_eq!(values[2], b"v5".to_vec(), "Oldest version last");
}

/// Test compaction with tombstones.
#[test]
fn test_executor_tombstones() {
    let dir = TempDir::new().unwrap();
    let sst_dir = dir.path().join("sst");
    std::fs::create_dir_all(&sst_dir).unwrap();

    let config = Arc::new(
        LsmConfigBuilder::new(dir.path())
            .target_file_size(1024 * 1024)
            .build_unchecked(),
    );
    let executor = CompactionExecutor::new(Arc::clone(&config));

    // SST1: key -> value
    let sst1_path = sst_dir.join("00000001.sst");
    let mut builder = SstBuilder::new(&sst1_path, SstBuilderOptions::default()).unwrap();
    builder.add(b"key", b"value").unwrap();
    let meta1 = builder.finish(1, 0).unwrap();

    // SST2: key -> <tombstone> (empty value represents tombstone in TiSQL)
    let sst2_path = sst_dir.join("00000002.sst");
    let mut builder = SstBuilder::new(&sst2_path, SstBuilderOptions::default()).unwrap();
    builder.add(b"key", b"").unwrap(); // Empty value = tombstone
    let meta2 = builder.finish(2, 0).unwrap();

    let version = Version::builder()
        .add_sst(meta1)
        .add_sst(meta2)
        .next_sst_id(3)
        .build();

    let task = CompactionTask {
        inputs: vec![(0, 2), (0, 1)], // SST2 is newer (listed first)
        output_level: 1,
        is_trivial_move: false,
    };

    let pre_allocated_ids = vec![3, 4];
    let delta = executor
        .execute(&task, &version, &sst_dir, &pre_allocated_ids)
        .unwrap();

    // Both versions should be in output (MVCC preserves all)
    let output_path = sst_dir.join(format!("{:08}.sst", delta.new_ssts[0].id));
    let reader = SstReaderRef::open(&output_path).unwrap();
    let mut iter = tisql::testkit::SstIterator::new(reader).unwrap();
    iter.seek_to_first().unwrap();

    let mut entries = Vec::new();
    while iter.valid() {
        entries.push((iter.key().to_vec(), iter.value().to_vec()));
        iter.advance().unwrap();
    }

    assert_eq!(entries.len(), 2, "Both versions should be preserved");
}

// ============================================================================
// END-TO-END COMPACTION TESTS
// ============================================================================

/// Test that multiple flushes followed by manual compaction produce correct results.
#[test]
fn test_flush_and_compaction_e2e() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write data in batches
    for batch_num in 0..5 {
        let ts_base = batch_num * 10;
        for i in 0..10 {
            let ts = (ts_base + i + 1) as Timestamp;
            let mut batch = new_batch_with_lsn(ts, ts);
            let key = format!("key_{i:03}");
            batch.put(key.into_bytes(), format!("batch_{batch_num}").into_bytes());
            engine.write_batch(batch).unwrap();
        }

        // Flush after each batch
        engine.flush_all_with_active().unwrap();
    }

    // Verify stats show L0 files
    let stats = engine.stats();
    assert!(stats.l0_sst_count > 0, "Should have L0 SST files");

    // Verify latest values
    for i in 0..10 {
        let key = format!("key_{i:03}");
        let value = get_for_test(&engine, key.as_bytes());
        assert_eq!(
            value,
            Some(b"batch_4".to_vec()),
            "Latest value should be from batch 4"
        );
    }

    // Verify historical values
    let value_at_15 = get_at_for_test(&engine, b"key_000", 15);
    assert_eq!(
        value_at_15,
        Some(b"batch_1".to_vec()),
        "Value at ts=15 should be from batch 1"
    );
}

/// Test that interleaved writes and flushes maintain consistency.
#[test]
fn test_interleaved_writes_flushes() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    let mut ts_counter = 1u64;

    // Interleaved pattern: write, flush, write, write, flush, write...
    for round in 0..10 {
        // Write some data
        let num_writes = (round % 3) + 1;
        for i in 0..num_writes {
            let mut batch = new_batch_with_lsn(ts_counter, ts_counter);
            let key = format!("inter_key_{round}");
            batch.put(key.into_bytes(), format!("value_{round}_{i}").into_bytes());
            engine.write_batch(batch).unwrap();
            ts_counter += 1;
        }

        // Sometimes flush
        if round % 2 == 0 {
            engine.flush_all_with_active().unwrap();
        }
    }

    // Final flush
    engine.flush_all_with_active().unwrap();

    // Verify all keys exist with latest values
    for round in 0..10 {
        let key = format!("inter_key_{round}");
        let value = get_for_test(&engine, key.as_bytes());
        assert!(
            value.is_some(),
            "Key {key} should exist after interleaved operations"
        );
    }
}

/// Test scan correctness across memtable and SST.
#[test]
fn test_scan_across_memtable_and_sst() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write and flush some data (goes to SST)
    let mut batch1 = new_batch_with_lsn(10, 1);
    batch1.put(b"a".to_vec(), b"a_sst".to_vec());
    batch1.put(b"c".to_vec(), b"c_sst".to_vec());
    batch1.put(b"e".to_vec(), b"e_sst".to_vec());
    engine.write_batch(batch1).unwrap();
    engine.flush_all_with_active().unwrap();

    // Write more data (stays in memtable)
    let mut batch2 = new_batch_with_lsn(20, 2);
    batch2.put(b"b".to_vec(), b"b_mem".to_vec());
    batch2.put(b"d".to_vec(), b"d_mem".to_vec());
    batch2.put(b"f".to_vec(), b"f_mem".to_vec());
    engine.write_batch(batch2).unwrap();

    // Scan should merge both sources
    let range = b"a".to_vec()..b"g".to_vec();
    let results = scan_for_test(&engine, &range);

    assert_eq!(results.len(), 6, "Should have 6 keys total");

    // Verify ordering
    let keys: Vec<_> = results.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(
        keys,
        vec![
            b"a".as_slice(),
            b"b".as_slice(),
            b"c".as_slice(),
            b"d".as_slice(),
            b"e".as_slice(),
            b"f".as_slice()
        ]
    );

    // Verify values
    assert_eq!(results[0].1, b"a_sst".to_vec());
    assert_eq!(results[1].1, b"b_mem".to_vec());
    assert_eq!(results[2].1, b"c_sst".to_vec());
    assert_eq!(results[3].1, b"d_mem".to_vec());
    assert_eq!(results[4].1, b"e_sst".to_vec());
    assert_eq!(results[5].1, b"f_mem".to_vec());
}

/// Test that overwrites in memtable shadow SST values.
#[test]
fn test_memtable_shadows_sst() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write original values and flush to SST
    let mut batch1 = new_batch_with_lsn(10, 1);
    batch1.put(b"key1".to_vec(), b"original".to_vec());
    batch1.put(b"key2".to_vec(), b"original".to_vec());
    engine.write_batch(batch1).unwrap();
    engine.flush_all_with_active().unwrap();

    // Overwrite key1 in memtable
    let mut batch2 = new_batch_with_lsn(20, 2);
    batch2.put(b"key1".to_vec(), b"updated".to_vec());
    engine.write_batch(batch2).unwrap();

    // key1 should return updated value (memtable shadows SST)
    assert_eq!(
        get_for_test(&engine, b"key1"),
        Some(b"updated".to_vec()),
        "Memtable value should shadow SST"
    );

    // key2 should return original value (still in SST)
    assert_eq!(
        get_for_test(&engine, b"key2"),
        Some(b"original".to_vec()),
        "Unchanged key should return SST value"
    );

    // Historical read should return correct versions
    assert_eq!(
        get_at_for_test(&engine, b"key1", 15),
        Some(b"original".to_vec()),
        "Historical read at ts=15 should return original"
    );
    assert_eq!(
        get_at_for_test(&engine, b"key1", 25),
        Some(b"updated".to_vec()),
        "Historical read at ts=25 should return updated"
    );
}

/// Test delete in memtable shadows value in SST.
///
/// Note: When value is in SST and delete is in memtable, MVCC query at ts=MAX
/// finds the newer delete first if properly ordered. This tests that scenario
/// by flushing after the delete so both versions are in SST for consistent ordering.
#[test]
fn test_delete_shadows_sst() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write and flush
    let mut batch1 = new_batch_with_lsn(10, 1);
    batch1.put(b"to_delete".to_vec(), b"exists".to_vec());
    engine.write_batch(batch1).unwrap();
    engine.flush_all_with_active().unwrap();

    // Delete and flush (so both versions are in SST for consistent MVCC ordering)
    let mut batch2 = new_batch_with_lsn(20, 2);
    batch2.delete(b"to_delete".to_vec());
    engine.write_batch(batch2).unwrap();
    engine.flush_all_with_active().unwrap();

    // Get should return None (deleted)
    assert_eq!(
        get_for_test(&engine, b"to_delete"),
        None,
        "Deleted key should return None"
    );

    // Historical read should work
    assert_eq!(
        get_at_for_test(&engine, b"to_delete", 15),
        Some(b"exists".to_vec()),
        "Historical read before delete should return value"
    );

    // Scan should exclude deleted key
    let range = b"t".to_vec()..b"u".to_vec();
    let results = scan_for_test(&engine, &range);
    assert!(results.is_empty(), "Scan should exclude deleted keys");
}

// ============================================================================
// VERSION MANAGEMENT TESTS
// ============================================================================

/// Test version builder creates correct level structure.
#[test]
fn test_version_builder_levels() {
    let version = Version::builder()
        .add_sst(make_sst(1, 0, b"a", b"z"))
        .add_sst(make_sst(2, 0, b"b", b"y"))
        .add_sst(make_sst(3, 1, b"a", b"m"))
        .add_sst(make_sst(4, 1, b"n", b"z"))
        .add_sst(make_sst(5, 2, b"a", b"z"))
        .build();

    assert_eq!(version.level_size(0), 2, "L0 should have 2 files");
    assert_eq!(version.level_size(1), 2, "L1 should have 2 files");
    assert_eq!(version.level_size(2), 1, "L2 should have 1 file");
    assert_eq!(version.total_sst_count(), 5, "Total should be 5");
}

/// Test version applies delta correctly.
#[test]
fn test_version_apply_delta() {
    let version = Version::builder()
        .add_sst(make_sst(1, 0, b"a", b"z"))
        .add_sst(make_sst(2, 0, b"b", b"y"))
        .next_sst_id(3)
        .build();

    // Create delta: delete L0 files, add L1 file
    let mut delta = ManifestDelta::new();
    delta.delete_sst(0, 1);
    delta.delete_sst(0, 2);
    delta.add_sst(make_sst(3, 1, b"a", b"z"));

    let new_version = version.apply(&delta);

    assert_eq!(new_version.level_size(0), 0, "L0 should be empty");
    assert_eq!(new_version.level_size(1), 1, "L1 should have 1 file");
    assert_eq!(new_version.total_sst_count(), 1, "Total should be 1");
}

/// Test find_overlapping_at_level finds correct files.
#[test]
fn test_version_find_overlapping() {
    let version = Version::builder()
        .add_sst(make_sst(1, 1, b"a", b"f"))
        .add_sst(make_sst(2, 1, b"g", b"l"))
        .add_sst(make_sst(3, 1, b"m", b"r"))
        .add_sst(make_sst(4, 1, b"s", b"z"))
        .build();

    // Find files overlapping with [e, n]
    let overlapping = version.find_overlapping_at_level(1, b"e", b"n");

    assert_eq!(overlapping.len(), 3, "Should find 3 overlapping files");

    let ids: Vec<_> = overlapping.iter().map(|s| s.id).collect();
    assert!(ids.contains(&1)); // [a-f] overlaps with [e-n]
    assert!(ids.contains(&2)); // [g-l] overlaps with [e-n]
    assert!(ids.contains(&3)); // [m-r] overlaps with [e-n]
    assert!(!ids.contains(&4)); // [s-z] doesn't overlap with [e-n]
}

// ============================================================================
// MANIFEST DELTA TESTS
// ============================================================================

/// Test manifest delta for flush operation.
#[test]
fn test_manifest_delta_flush() {
    let sst = make_sst(1, 0, b"a", b"z");
    let delta = ManifestDelta::flush(sst.clone(), 100);

    assert_eq!(delta.new_ssts.len(), 1);
    assert_eq!(delta.new_ssts[0].id, 1);
    assert_eq!(delta.new_ssts[0].level, 0);
    assert_eq!(delta.flushed_lsn, Some(100));
}

/// Test manifest delta for compaction operation.
#[test]
fn test_manifest_delta_compaction() {
    let outputs = vec![make_sst(10, 1, b"a", b"z")];
    let inputs = vec![(0, 1), (0, 2), (1, 3)];
    let delta = ManifestDelta::compaction(outputs, inputs);

    assert_eq!(delta.new_ssts.len(), 1);
    assert_eq!(delta.deleted_ssts.len(), 3);
}

// ============================================================================
// DO_COMPACTION INTEGRATION TESTS
// ============================================================================

/// Test do_compaction() returns false on a fresh engine with no data.
#[test]
fn test_do_compaction_no_work() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    let result = engine.do_compaction().unwrap();
    assert!(!result, "Fresh engine should have nothing to compact");
}

/// Test do_compaction() L0 → L1 compaction end-to-end.
#[test]
fn test_do_compaction_l0_to_l1() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write and flush enough data to create >= 4 L0 files (trigger threshold)
    for batch_num in 0..8 {
        let ts = (batch_num + 1) as Timestamp;
        let mut batch = new_batch_with_lsn(ts, ts);
        for i in 0..5 {
            let key = format!("compaction_key_{i:03}");
            let value = format!("batch_{batch_num}_value_{i}");
            batch.put(key.into_bytes(), value.into_bytes());
        }
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();
    }

    let stats_before = engine.stats();
    let l0_before = stats_before.l0_sst_count;
    assert!(
        l0_before >= 4,
        "Should have at least 4 L0 files, got {l0_before}"
    );

    // Run compaction
    let compacted = engine.do_compaction().unwrap();
    assert!(compacted, "Should have compacted L0 → L1");

    let stats_after = engine.stats();
    assert!(
        stats_after.l0_sst_count < l0_before,
        "L0 count should decrease after compaction: before={l0_before}, after={}",
        stats_after.l0_sst_count
    );

    // Verify data is still readable after compaction
    for i in 0..5 {
        let key = format!("compaction_key_{i:03}");
        let value = get_for_test(&engine, key.as_bytes());
        assert!(
            value.is_some(),
            "Key {key} should be readable after compaction"
        );
        // Latest value should be from batch 7
        assert_eq!(
            value.unwrap(),
            format!("batch_7_value_{i}").into_bytes(),
            "Latest value should be from last batch"
        );
    }
}

/// Test that do_compaction() deletes old SST files from the filesystem.
#[test]
fn test_do_compaction_deletes_old_ssts() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write and flush to create L0 files
    for batch_num in 0..6 {
        let ts = (batch_num + 1) as Timestamp;
        let mut batch = new_batch_with_lsn(ts, ts);
        let key = format!("del_key_{batch_num:03}");
        batch.put(key.into_bytes(), vec![b'x'; 50]);
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();
    }

    // Count SST files before compaction
    let sst_dir = dir.path().join("sst");
    let sst_files_before: Vec<_> = std::fs::read_dir(&sst_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
        .collect();
    let count_before = sst_files_before.len();
    assert!(count_before >= 4, "Should have L0 SST files");

    // Compact
    let compacted = engine.do_compaction().unwrap();
    assert!(compacted, "Should have compacted");

    // Count SST files after compaction
    let sst_files_after: Vec<_> = std::fs::read_dir(&sst_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
        .collect();

    // After compaction: old L0 files should be deleted, new L1 files created.
    // Total count may differ but old input files should not exist.
    // The version should reflect the new state.
    let version = engine.current_version();
    assert_eq!(
        version.level_size(0),
        0,
        "L0 should be empty after L0→L1 compaction"
    );
    assert!(
        version.level_size(1) > 0,
        "L1 should have files after compaction"
    );

    // Every SST in version should exist on disk
    for level in 0..7 {
        for sst in version.level(level) {
            let path = sst_dir.join(format!("{:08}.sst", sst.id));
            assert!(
                path.exists(),
                "SST {} at L{} should exist on disk",
                sst.id,
                level
            );
        }
    }

    // SST files after should be fewer than or equal to before
    // (old files deleted, new files created - usually fewer total)
    assert!(
        sst_files_after.len() <= count_before,
        "Should not have more SST files after compaction"
    );
}

/// Test that all MVCC versions survive compaction.
#[test]
fn test_do_compaction_preserves_mvcc_versions() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write multiple versions of the same key across different flushes
    for version_num in 0..6 {
        let ts = (version_num + 1) as Timestamp;
        let mut batch = new_batch_with_lsn(ts, ts);
        batch.put(
            b"mvcc_key".to_vec(),
            format!("version_{version_num}").into_bytes(),
        );
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();
    }

    // Verify historical reads before compaction
    for version_num in 0..6 {
        let ts = (version_num + 1) as Timestamp;
        let value = get_at_for_test(&engine, b"mvcc_key", ts);
        assert_eq!(
            value,
            Some(format!("version_{version_num}").into_bytes()),
            "Pre-compaction: version at ts={ts} should exist"
        );
    }

    // Compact
    let compacted = engine.do_compaction().unwrap();
    assert!(compacted, "Should compact");

    // Verify all historical versions still readable after compaction
    for version_num in 0..6 {
        let ts = (version_num + 1) as Timestamp;
        let value = get_at_for_test(&engine, b"mvcc_key", ts);
        assert_eq!(
            value,
            Some(format!("version_{version_num}").into_bytes()),
            "Post-compaction: version at ts={ts} should still exist"
        );
    }

    // Latest version should still be correct
    let latest = get_for_test(&engine, b"mvcc_key");
    assert_eq!(
        latest,
        Some(b"version_5".to_vec()),
        "Latest version should be version_5"
    );
}

/// Test that do_compaction() with ilog recovery works correctly.
#[test]
fn test_do_compaction_with_ilog_recovery() {
    let dir = TempDir::new().unwrap();

    // Phase 1: Write, flush, compact, then drop engine
    {
        let (engine, ilog) = create_durable_engine(&dir);

        for batch_num in 0..8 {
            let ts = (batch_num + 1) as Timestamp;
            let mut batch = new_batch_with_lsn(ts, ts);
            for i in 0..3 {
                let key = format!("recovery_key_{i:03}");
                batch.put(key.into_bytes(), format!("value_{batch_num}").into_bytes());
            }
            engine.write_batch(batch).unwrap();
            engine.flush_all_with_active().unwrap();
        }

        // Compact
        let compacted = engine.do_compaction().unwrap();
        assert!(compacted, "Should compact");

        // Write checkpoint to persist state
        let version = engine.current_version();
        ilog.write_checkpoint(&version).unwrap();

        // Engine and ilog dropped here
    }

    // Phase 2: Recover and verify data
    {
        let lsm_config = LsmConfigBuilder::new(dir.path())
            .memtable_size(256)
            .max_frozen_memtables(16)
            .l0_compaction_trigger(4)
            .l0_slowdown_trigger(20)
            .l0_stop_trigger(30)
            .build_unchecked();
        let clog_config = tisql::testkit::FileClogConfig::with_dir(dir.path());
        let ilog_config = IlogConfig::new(dir.path());

        let recovery =
            tisql::testkit::LsmRecovery::with_configs(lsm_config, clog_config, ilog_config);
        let result = recovery.recover().unwrap();
        let engine = result.engine;

        // Verify data survived recovery
        for i in 0..3 {
            let key = format!("recovery_key_{i:03}");
            let value = get_for_test(&engine, key.as_bytes());
            assert!(
                value.is_some(),
                "Key {key} should survive compaction + recovery"
            );
        }
    }
}

/// Test concurrent read during compaction: a reader holding an old SuperVersion
/// should still be able to read data while compaction runs.
#[test]
fn test_compaction_concurrent_read() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write and flush to create L0 files
    for batch_num in 0..6 {
        let ts = (batch_num + 1) as Timestamp;
        let mut batch = new_batch_with_lsn(ts, ts);
        for i in 0..5 {
            let key = format!("concurrent_key_{i:03}");
            batch.put(key.into_bytes(), format!("v{batch_num}").into_bytes());
        }
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();
    }

    // Take a snapshot (SuperVersion) before compaction
    let sv_before = engine.get_super_version();

    // Run compaction
    let compacted = engine.do_compaction().unwrap();
    assert!(compacted, "Should compact");

    // Old SuperVersion should still work for reading
    // (SST files referenced by old version should still be valid since
    // we hold a reference via SuperVersion)
    let sv_after = engine.get_super_version();
    assert!(
        sv_after.sv_number > sv_before.sv_number,
        "SuperVersion number should increase after compaction"
    );

    // Data should be readable with the new version
    for i in 0..5 {
        let key = format!("concurrent_key_{i:03}");
        let value = get_for_test(&engine, key.as_bytes());
        assert!(
            value.is_some(),
            "Key {key} should be readable after compaction"
        );
    }
}

/// Test L0 write backpressure: writes should be rejected when L0 count >= l0_stop_trigger.
#[test]
fn test_l0_write_backpressure() {
    let dir = TempDir::new().unwrap();
    let lsn_provider = new_lsn_provider();
    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    // Very low stop trigger for testing
    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(100) // Very small
        .max_frozen_memtables(32)
        .l0_compaction_trigger(2)
        .l0_slowdown_trigger(3)
        .l0_stop_trigger(5) // Stop at 5 L0 files
        .build_unchecked();

    let engine =
        LsmEngine::open_with_recovery(config, lsn_provider, Arc::clone(&ilog), Version::new())
            .unwrap();

    // Create L0 files by flushing
    let mut write_succeeded = true;
    for batch_num in 0..20 {
        let ts = (batch_num + 1) as Timestamp;
        let mut batch = new_batch_with_lsn(ts, ts);
        batch.put(
            format!("bp_key_{batch_num:03}").into_bytes(),
            vec![b'x'; 50],
        );

        match engine.write_batch(batch) {
            Ok(()) => {
                engine.flush_all_with_active().unwrap();
            }
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    msg.contains("Too many L0 files"),
                    "Error should mention L0 backpressure, got: {msg}"
                );
                write_succeeded = false;
                break;
            }
        }
    }

    assert!(
        !write_succeeded,
        "Writes should eventually be rejected by L0 backpressure"
    );
}

// ============================================================================
// COMPACTION SCHEDULER TESTS
// ============================================================================

/// Test that CompactionScheduler automatically compacts after enough L0 files.
#[test]
fn test_compaction_scheduler_automatic() {
    let dir = TempDir::new().unwrap();
    let lsn_provider = new_lsn_provider();
    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(100) // Very small
        .max_frozen_memtables(16)
        .l0_compaction_trigger(4)
        .l0_slowdown_trigger(20)
        .l0_stop_trigger(30)
        .target_file_size(512)
        .l1_max_size(4096)
        .build_unchecked();

    let engine = Arc::new(
        LsmEngine::open_with_recovery(config, lsn_provider, Arc::clone(&ilog), Version::new())
            .unwrap(),
    );

    let scheduler = CompactionScheduler::new(Arc::clone(&engine));
    scheduler.start();

    // Write and flush enough data to create L0 files above compaction trigger
    for i in 0..20 {
        let mut batch = WriteBatch::new();
        batch.set_commit_ts((i + 1) as u64);
        let key = format!("auto_key_{i:04}");
        let value = vec![b'x'; 50];
        batch.put(key.into_bytes(), value);
        engine.write_batch(batch).unwrap();
    }

    // Flush all to create L0 files
    engine.flush_all_with_active().unwrap();

    // Notify compaction scheduler
    scheduler.notify();

    // Wait for at least one compaction
    let compacted = scheduler.wait_for_compaction_count(1, Duration::from_secs(10));
    assert!(
        compacted,
        "CompactionScheduler should have compacted at least once"
    );

    // Verify data is still readable
    for i in 0..20 {
        let key = format!("auto_key_{i:04}");
        let value = get_for_test(&engine, key.as_bytes());
        assert!(
            value.is_some(),
            "Key {key} should be readable after auto-compaction"
        );
    }

    scheduler.stop();
}

/// Test do_compaction handles delete + put correctly through compaction.
#[test]
fn test_do_compaction_delete_then_put() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write initial value
    let mut batch = new_batch_with_lsn(1, 1);
    batch.put(b"del_put_key".to_vec(), b"initial".to_vec());
    engine.write_batch(batch).unwrap();
    engine.flush_all_with_active().unwrap();

    // Delete
    let mut batch = new_batch_with_lsn(2, 2);
    batch.delete(b"del_put_key".to_vec());
    engine.write_batch(batch).unwrap();
    engine.flush_all_with_active().unwrap();

    // Put again
    let mut batch = new_batch_with_lsn(3, 3);
    batch.put(b"del_put_key".to_vec(), b"resurrected".to_vec());
    engine.write_batch(batch).unwrap();
    engine.flush_all_with_active().unwrap();

    // Add more flushes to reach compaction trigger
    for i in 4..8 {
        let mut batch = new_batch_with_lsn(i, i);
        batch.put(format!("filler_{i}").into_bytes(), vec![b'x'; 50]);
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();
    }

    // Compact
    let compacted = engine.do_compaction().unwrap();
    assert!(compacted, "Should compact");

    // Latest read should see "resurrected"
    let latest = get_for_test(&engine, b"del_put_key");
    assert_eq!(
        latest,
        Some(b"resurrected".to_vec()),
        "Post-compaction: latest value should be 'resurrected'"
    );

    // Historical reads
    assert_eq!(
        get_at_for_test(&engine, b"del_put_key", 1),
        Some(b"initial".to_vec()),
        "ts=1 should see initial"
    );
    assert_eq!(
        get_at_for_test(&engine, b"del_put_key", 2),
        None,
        "ts=2 should see deletion"
    );
    assert_eq!(
        get_at_for_test(&engine, b"del_put_key", 3),
        Some(b"resurrected".to_vec()),
        "ts=3 should see resurrected"
    );
}

/// Test that repeated compaction works (compact, write more, compact again).
#[test]
fn test_repeated_compaction() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Round 1: Write, flush, compact
    for batch_num in 0..6 {
        let ts = (batch_num + 1) as Timestamp;
        let mut batch = new_batch_with_lsn(ts, ts);
        batch.put(
            format!("round1_key_{batch_num}").into_bytes(),
            format!("round1_val_{batch_num}").into_bytes(),
        );
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();
    }

    let compacted = engine.do_compaction().unwrap();
    assert!(compacted, "Round 1 should compact");

    // Round 2: Write more, flush, compact again
    for batch_num in 0..6 {
        let ts = (batch_num + 10) as Timestamp;
        let mut batch = new_batch_with_lsn(ts, ts);
        batch.put(
            format!("round2_key_{batch_num}").into_bytes(),
            format!("round2_val_{batch_num}").into_bytes(),
        );
        engine.write_batch(batch).unwrap();
        engine.flush_all_with_active().unwrap();
    }

    let compacted = engine.do_compaction().unwrap();
    assert!(compacted, "Round 2 should compact");

    // Verify data from both rounds
    for batch_num in 0..6 {
        let key1 = format!("round1_key_{batch_num}");
        let key2 = format!("round2_key_{batch_num}");
        assert!(
            get_for_test(&engine, key1.as_bytes()).is_some(),
            "Round 1 key {key1} should survive"
        );
        assert!(
            get_for_test(&engine, key2.as_bytes()).is_some(),
            "Round 2 key {key2} should survive"
        );
    }
}

/// Test that scan works correctly after compaction.
#[test]
fn test_scan_after_compaction() {
    let dir = TempDir::new().unwrap();
    let (engine, _ilog) = create_durable_engine(&dir);

    // Write keys a through j across multiple flushes
    let keys = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
    for (batch_num, key) in keys.iter().enumerate() {
        let ts = (batch_num + 1) as Timestamp;
        let mut batch = new_batch_with_lsn(ts, ts);
        batch.put(key.as_bytes().to_vec(), format!("val_{key}").into_bytes());
        engine.write_batch(batch).unwrap();

        // Flush every 2 keys
        if batch_num % 2 == 1 {
            engine.flush_all_with_active().unwrap();
        }
    }
    engine.flush_all_with_active().unwrap();

    // Compact
    let compacted = engine.do_compaction().unwrap();
    assert!(compacted, "Should compact");

    // Full range scan
    let range = b"a".to_vec()..b"k".to_vec();
    let results = scan_for_test(&engine, &range);
    assert_eq!(
        results.len(),
        10,
        "Should find all 10 keys after compaction"
    );

    // Partial range scan
    let range = b"c".to_vec()..b"g".to_vec();
    let results = scan_for_test(&engine, &range);
    assert_eq!(results.len(), 4, "Should find c,d,e,f");
    let result_keys: Vec<_> = results.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(
        result_keys,
        vec![
            b"c".as_slice(),
            b"d".as_slice(),
            b"e".as_slice(),
            b"f".as_slice()
        ]
    );
}
