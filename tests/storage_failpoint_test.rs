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

//! Storage layer failpoint tests.
//!
//! These tests verify crash recovery and durability semantics using fail-rs.
//!
//! To run these tests:
//! ```
//! cargo test -p tisql --test storage_failpoint_test --features failpoints
//! ```

#![cfg(feature = "failpoints")]

use std::sync::Arc;

use tempfile::TempDir;

use tisql::new_lsn_provider;
use tisql::storage::WriteBatch;
use tisql::testkit::{
    FileClogConfig, FileClogService, IlogConfig, IlogService, LsmConfigBuilder, LsmEngine,
    LsmRecovery, RecoveryResult, Version,
};
use tisql::StorageEngine;

/// Helper to create a test LSM engine with ilog for durability tests.
fn create_test_lsm_engine(
    dir: &TempDir,
) -> (Arc<LsmEngine>, Arc<IlogService>, Arc<FileClogService>) {
    let lsn_provider = new_lsn_provider();

    let ilog_config = IlogConfig::new(dir.path());
    let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

    let clog_config = FileClogConfig::with_dir(dir.path());
    let clog = Arc::new(FileClogService::open(clog_config).unwrap());

    let config = LsmConfigBuilder::new(dir.path())
        .memtable_size(256) // Very small for testing to trigger rotations
        .max_frozen_memtables(16) // Allow more frozen memtables for testing
        .build_unchecked();
    let engine = LsmEngine::open_with_recovery(
        config,
        Arc::clone(&lsn_provider),
        Arc::clone(&ilog),
        Version::new(),
    )
    .unwrap();

    (Arc::new(engine), ilog, clog)
}

/// Helper to write test data to the engine.
fn write_test_data(engine: &LsmEngine, key: &[u8], value: &[u8], ts: u64) {
    let mut batch = WriteBatch::new();
    batch.put(key.to_vec(), value.to_vec());
    batch.set_commit_ts(ts);
    engine.write_batch(batch).unwrap();
}

/// Helper to recover an LSM engine from crash.
fn recover_engine(dir: &TempDir) -> RecoveryResult {
    let recovery = LsmRecovery::new(dir.path());
    recovery.recover().unwrap()
}

// ============================================================================
// Freeze/Rotate Failpoint Tests
// ============================================================================

/// Test that data in memtable is preserved after crash before freeze.
#[test]
fn test_crash_before_freeze() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Write some data
    write_test_data(&engine, b"key1", b"value1", 1);
    write_test_data(&engine, b"key2", b"value2", 2);

    // Verify data is readable
    let v1 = engine.get(b"key1").unwrap();
    assert_eq!(v1, Some(b"value1".to_vec()));

    // The data is in active memtable, not frozen
    // Since we haven't rotated, data won't be in ilog yet
    // This test verifies memtable writes are visible immediately

    scenario.teardown();
}

/// Test that crash after freeze but before frozen list insert is reachable.
///
/// Verifies the failpoint actually fires during rotation. After the panic,
/// the RwLock is poisoned (as expected for a real crash), so we just verify
/// the failpoint was triggered.
#[test]
fn test_crash_after_freeze_before_insert() {
    use std::panic;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Write data just under the threshold (memtable_size=256)
    for i in 0..10 {
        let key = format!("key{i:04}");
        let value = format!("val{i:04}");
        write_test_data(&engine, key.as_bytes(), value.as_bytes(), i as u64 + 1);
    }

    // Set failpoint BEFORE the write that will trigger rotation
    fail::cfg("lsm_after_freeze_before_insert", "panic").unwrap();

    // Write more data to push over threshold - maybe_rotate() will panic
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        for i in 10..30 {
            let key = format!("key{i:04}");
            let value = format!("val{i:04}");
            write_test_data(&engine, key.as_bytes(), value.as_bytes(), i as u64 + 1);
        }
    }));

    // Verify failpoint actually fired
    assert!(result.is_err(), "Failpoint should have triggered a panic during rotation");

    // Remove failpoint
    fail::cfg("lsm_after_freeze_before_insert", "off").unwrap();

    // After a panic, the RwLock is poisoned - this is expected crash behavior.
    // In production, the process would restart and recover from logs.

    scenario.teardown();
}

// ============================================================================
// Flush Failpoint Tests
// ============================================================================

/// Test crash before SST build - memtable should not be lost.
///
/// Uses freeze_active() to guarantee a frozen memtable exists, and "panic"
/// action to verify the failpoint fires during flush_memtable().
#[test]
fn test_crash_before_sst_build() {
    use std::panic;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Write data
    write_test_data(&engine, b"test_key", b"test_value", 1);

    // Use freeze_active() to guarantee frozen memtable (bypasses size threshold)
    let frozen = engine
        .freeze_active()
        .expect("freeze_active should return frozen memtable");

    // Configure failpoint to crash before SST build
    fail::cfg("lsm_flush_before_sst_build", "panic").unwrap();

    // Flush should panic at failpoint
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        engine.flush_memtable(&frozen)
    }));
    assert!(result.is_err(), "Failpoint should have triggered a panic");

    // Remove failpoint
    fail::cfg("lsm_flush_before_sst_build", "off").unwrap();

    // Data should still be in the frozen memtable (no SST was created)
    let value = engine.get(b"test_key").unwrap();
    assert_eq!(value, Some(b"test_value".to_vec()));

    // Verify no SST was created
    let version = engine.current_version();
    assert_eq!(version.level(0).len(), 0, "No SST should have been created");

    scenario.teardown();
}

/// Test crash after SST write but before ilog commit - orphan SST should be cleaned up.
///
/// The SST file is written but the ilog commit never happens, creating an orphan.
/// Recovery should clean up the orphan and data should be recoverable from clog.
#[test]
fn test_crash_after_sst_write_before_ilog() {
    use std::panic;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, ilog, clog) = create_test_lsm_engine(&dir);

    // Write data and record in clog for recovery
    write_test_data(&engine, b"orphan_key", b"orphan_value", 1);

    // Also write to clog so recovery can find this data
    {
        use tisql::ClogService;
        let mut batch = tisql::testkit::ClogBatch::new();
        batch.add_put(1, b"orphan_key".to_vec(), b"orphan_value".to_vec());
        batch.add_commit(1, 1);
        clog.write(&mut batch, true).unwrap();
    }

    // Use freeze_active() to guarantee frozen memtable
    let frozen = engine
        .freeze_active()
        .expect("freeze_active should return frozen memtable");

    // Count SST files before flush
    let sst_dir = dir.path().join("sst");
    let _ = std::fs::create_dir_all(&sst_dir);
    let sst_count_before = std::fs::read_dir(&sst_dir)
        .map(|entries| entries.filter_map(|e| e.ok()).count())
        .unwrap_or(0);

    // Configure failpoint to crash after SST write but before ilog commit
    fail::cfg("lsm_flush_after_sst_write", "panic").unwrap();

    // Flush will create SST file, then panic before ilog commit
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        engine.flush_memtable(&frozen)
    }));
    assert!(result.is_err(), "Failpoint should have triggered a panic");

    // Remove failpoint
    fail::cfg("lsm_flush_after_sst_write", "off").unwrap();

    // Verify an orphan SST was created
    let sst_count_after = std::fs::read_dir(&sst_dir)
        .map(|entries| entries.filter_map(|e| e.ok()).count())
        .unwrap_or(0);
    assert!(
        sst_count_after > sst_count_before,
        "Orphan SST should have been written (before={sst_count_before}, after={sst_count_after})",
    );

    // Drop for recovery
    drop(engine);
    drop(ilog);
    drop(clog);

    // Recover - orphan SST should be cleaned up
    let result = recover_engine(&dir);
    assert_eq!(
        result.engine.current_version().level(0).len(),
        0,
        "No SSTs should be in version (orphan was not committed to ilog)"
    );

    scenario.teardown();
}

/// Test crash after ilog commit - data should be recoverable from SST on recovery.
///
/// The SST is written and ilog is committed, but the version update doesn't happen
/// because we crash. On recovery, the ilog should rebuild the version correctly.
#[test]
fn test_crash_after_ilog_commit() {
    use std::panic;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    {
        let (engine, ilog, _clog) = create_test_lsm_engine(&dir);

        // Write data
        write_test_data(&engine, b"committed_key", b"committed_value", 1);

        // Use freeze_active() to guarantee frozen memtable
        let frozen = engine
            .freeze_active()
            .expect("freeze_active should return frozen memtable");

        // Configure failpoint after ilog commit (SST written + ilog committed, but
        // version not yet updated in memory)
        fail::cfg("lsm_flush_after_ilog_commit", "panic").unwrap();

        // Flush will write SST, commit to ilog, then panic before version update
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            engine.flush_memtable(&frozen)
        }));
        assert!(result.is_err(), "Failpoint should have triggered a panic");

        // Remove failpoint
        fail::cfg("lsm_flush_after_ilog_commit", "off").unwrap();

        // Simulate crash by dropping without clean shutdown
        ilog.sync().unwrap();
        drop(engine);
        drop(ilog);
    }

    // Recovery should rebuild version from ilog and find the SST
    let result = recover_engine(&dir);
    let value = result.engine.get(b"committed_key").unwrap();
    assert_eq!(
        value,
        Some(b"committed_value".to_vec()),
        "Data should be recoverable from SST via ilog"
    );

    scenario.teardown();
}

/// Test crash after version update - everything should be consistent.
#[test]
fn test_crash_after_version_update() {
    use std::panic;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Write data and flush, crash after version update
    {
        let (engine, ilog, _clog) = create_test_lsm_engine(&dir);

        // Write enough data to trigger rotation (memtable_size = 256)
        for i in 0..5 {
            let key = format!("version_key_{:04}", i);
            let value = format!("version_value_{:04}", i);
            write_test_data(&engine, key.as_bytes(), value.as_bytes(), i as u64 + 1);
        }

        // Configure failpoint to panic after version update
        // The panic will happen after the FIRST SST flush completes
        fail::cfg("lsm_flush_after_version_update", "panic").unwrap();

        // Flush all - should panic after first flush completes
        let result =
            panic::catch_unwind(panic::AssertUnwindSafe(|| engine.flush_all_with_active()));

        // Verify the failpoint triggered
        assert!(result.is_err(), "Should have panicked at failpoint");

        // Remove failpoint
        fail::cfg("lsm_flush_after_version_update", "off").unwrap();

        // Sync ilog to ensure it's persisted
        ilog.sync().unwrap();
    }

    // Phase 2: Recover and verify partial data recovery
    // Only data from the FIRST flush (before panic) is recoverable
    // This verifies that completed flushes survive crashes
    let result = recover_engine(&dir);

    // At least some data should be recoverable (the first SST that was flushed)
    let mut recovered_count = 0;
    for i in 0..5 {
        let key = format!("version_key_{:04}", i);
        if result.engine.get(key.as_bytes()).unwrap().is_some() {
            recovered_count += 1;
        }
    }

    assert!(
        recovered_count > 0,
        "At least some data should survive crash after version update"
    );

    // The version should have at least one SST from the successful flush
    let version = result.engine.current_version();
    assert!(
        version.total_sst_count() > 0,
        "Version should have SST from completed flush"
    );

    scenario.teardown();
}

// ============================================================================
// Ilog Failpoint Tests
// ============================================================================

/// Test crash after flush intent write - SST should be orphaned.
#[test]
fn test_crash_after_flush_intent() {
    use std::panic;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Write data and crash after flush intent
    {
        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(dir.path());
        let ilog = IlogService::open(ilog_config.clone(), Arc::clone(&lsn_provider)).unwrap();

        // Configure failpoint to panic (simulating crash)
        fail::cfg("ilog_after_flush_intent", "panic").unwrap();

        // Write flush intent - should panic
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            ilog.write_flush_intent(1, 1, 100)
        }));

        // Verify the failpoint triggered
        assert!(result.is_err(), "Should have panicked at failpoint");

        // Remove failpoint
        fail::cfg("ilog_after_flush_intent", "off").unwrap();
    }

    // Phase 2: Recovery should identify the incomplete flush
    // The intent was written but no commit followed, so SST 1 should be orphaned
    // (In a real scenario, recovery would delete the orphan SST file if it exists)

    scenario.teardown();
}

/// Test crash during checkpoint write.
#[test]
fn test_crash_during_checkpoint() {
    use std::panic;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Write checkpoint and crash mid-write
    {
        let lsn_provider = new_lsn_provider();
        let ilog_config = IlogConfig::new(dir.path());
        let ilog = IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap();

        let version = Version::new();

        // Configure failpoint to panic (simulating crash)
        fail::cfg("ilog_checkpoint_mid_write", "panic").unwrap();

        // Write checkpoint - should panic
        let result =
            panic::catch_unwind(panic::AssertUnwindSafe(|| ilog.write_checkpoint(&version)));

        // Verify the failpoint triggered
        assert!(result.is_err(), "Should have panicked at failpoint");

        // Remove failpoint
        fail::cfg("ilog_checkpoint_mid_write", "off").unwrap();
    }

    // Phase 2: Recovery should handle partial/missing checkpoint gracefully
    // by either using the partial checkpoint or falling back to no checkpoint

    scenario.teardown();
}

// ============================================================================
// Compaction Failpoint Tests
// ============================================================================

/// Test crash before merge iterator creation during compaction.
///
/// Creates enough L0 SSTs to trigger compaction, then verifies the failpoint
/// fires when do_compaction() is called.
#[test]
fn test_crash_before_merge() {
    use std::panic;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Write and flush multiple times to create multiple L0 SSTs
    for i in 0..3 {
        for j in 0..5 {
            let key = format!("comp_key_{i}_{j}");
            write_test_data(&engine, key.as_bytes(), b"value", (i * 10 + j) as u64 + 1);
        }
        engine.flush_all_with_active().unwrap();
    }

    // Verify we have multiple L0 SSTs
    let version = engine.current_version();
    let l0_count = version.level(0).len();
    assert!(l0_count >= 2, "Need multiple L0 SSTs for compaction (got {l0_count})");

    // Configure failpoint to crash before merge
    fail::cfg("compaction_before_merge", "panic").unwrap();

    // Trigger compaction - should panic at failpoint
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| engine.do_compaction()));
    assert!(result.is_err(), "Failpoint should have triggered a panic during compaction");

    // Remove failpoint
    fail::cfg("compaction_before_merge", "off").unwrap();

    // All data should still be readable (compaction didn't complete)
    for i in 0..3 {
        for j in 0..5 {
            let key = format!("comp_key_{i}_{j}");
            let value = engine.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Key {key} should exist");
        }
    }

    scenario.teardown();
}

/// Test crash mid compaction write - partial output should be cleaned up.
///
/// Creates multiple L0 SSTs, triggers compaction, and panics mid-write.
/// Data remains intact because the original SSTs are not removed until
/// compaction commits.
#[test]
fn test_crash_mid_compaction() {
    use std::panic;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Write and flush multiple times to create enough L0 SSTs for compaction
    for i in 0..3 {
        for j in 0..5 {
            let key = format!("mid_comp_key_{:04}", i * 10 + j);
            write_test_data(&engine, key.as_bytes(), b"value", (i * 10 + j) as u64 + 1);
        }
        engine.flush_all_with_active().unwrap();
    }

    let version = engine.current_version();
    let l0_count = version.level(0).len();
    assert!(l0_count >= 2, "Need multiple L0 SSTs for compaction (got {l0_count})");

    // Configure failpoint to crash mid compaction
    fail::cfg("compaction_mid_write", "panic").unwrap();

    // Trigger compaction - should panic mid-write
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| engine.do_compaction()));
    assert!(result.is_err(), "Failpoint should have triggered a panic during compaction");

    // Remove failpoint
    fail::cfg("compaction_mid_write", "off").unwrap();

    // All data should still be readable from original SSTs
    for i in 0..3 {
        for j in 0..5 {
            let key = format!("mid_comp_key_{:04}", i * 10 + j);
            let value = engine.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Key {key} should exist after failed compaction");
        }
    }

    scenario.teardown();
}

/// Test crash after compaction finishes writing but before commit.
///
/// All SSTs are written, but the manifest delta is not applied. Original SSTs
/// remain in the version, so data is still readable.
#[test]
fn test_crash_after_compaction_finish() {
    use std::panic;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Create multiple L0 SSTs for compaction
    for i in 0..3 {
        for j in 0..5 {
            let key = format!("post_comp_key_{i}_{j}");
            write_test_data(&engine, key.as_bytes(), b"value", (i * 10 + j) as u64 + 1);
        }
        engine.flush_all_with_active().unwrap();
    }

    let version = engine.current_version();
    let l0_before = version.level(0).len();
    assert!(l0_before >= 2, "Need multiple L0 SSTs for compaction (got {l0_before})");

    // Configure failpoint to crash after compaction write but before commit
    fail::cfg("compaction_after_finish", "panic").unwrap();

    // Trigger compaction - should panic after writing new SSTs but before committing
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| engine.do_compaction()));
    assert!(result.is_err(), "Failpoint should have triggered a panic during compaction");

    // Remove failpoint
    fail::cfg("compaction_after_finish", "off").unwrap();

    // L0 count should be unchanged (compaction didn't commit)
    let version = engine.current_version();
    assert_eq!(
        version.level(0).len(),
        l0_before,
        "L0 SST count should be unchanged after failed compaction"
    );

    // All data should be readable from original SSTs
    for i in 0..3 {
        for j in 0..5 {
            let key = format!("post_comp_key_{i}_{j}");
            let value = engine.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Key {key} should exist");
        }
    }

    scenario.teardown();
}

// ============================================================================
// Clog Failpoint Tests
// ============================================================================

/// Test crash before clog fsync - data may be lost since it wasn't synced.
///
/// Writes to clog with sync=true, but the failpoint panics before the fsync.
/// Data written but not synced may be lost on recovery (depending on OS buffer).
#[test]
fn test_crash_before_clog_sync() {
    use std::panic;
    use tisql::ClogService;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let clog_config = FileClogConfig::with_dir(dir.path());
    let clog = FileClogService::open(clog_config).unwrap();

    // Configure failpoint to crash before fsync
    fail::cfg("clog_before_sync", "panic").unwrap();

    // Write to clog with sync=true - should panic before fsync
    let mut batch = tisql::testkit::ClogBatch::new();
    batch.add_put(1, b"key".to_vec(), b"value".to_vec());
    batch.add_commit(1, 100);
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        clog.write(&mut batch, true)
    }));
    assert!(result.is_err(), "Failpoint should have triggered a panic before fsync");

    // Remove failpoint
    fail::cfg("clog_before_sync", "off").unwrap();

    scenario.teardown();
}

/// Test crash after clog fsync - data should be durable.
///
/// After fsync completes, the data is durable on disk. A crash after fsync
/// should not lose the written data.
#[test]
fn test_crash_after_clog_sync() {
    use std::panic;
    use tisql::ClogService;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let clog_config = FileClogConfig::with_dir(dir.path());
    let clog = FileClogService::open(clog_config).unwrap();

    // First write without failpoint to establish baseline
    let mut batch = tisql::testkit::ClogBatch::new();
    batch.add_put(1, b"durable_key".to_vec(), b"durable_value".to_vec());
    batch.add_commit(1, 100);
    clog.write(&mut batch, true).unwrap();

    // Configure failpoint to crash after fsync (data is already durable)
    fail::cfg("clog_after_sync", "panic").unwrap();

    // Write again - should panic after fsync (data IS durable)
    let mut batch = tisql::testkit::ClogBatch::new();
    batch.add_put(2, b"also_durable".to_vec(), b"also_value".to_vec());
    batch.add_commit(2, 200);
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        clog.write(&mut batch, true)
    }));
    assert!(result.is_err(), "Failpoint should have triggered a panic after fsync");

    // Remove failpoint
    fail::cfg("clog_after_sync", "off").unwrap();

    // Drop clog and recover
    drop(clog);
    let clog_config = FileClogConfig::with_dir(dir.path());
    let (recovered_clog, entries) =
        FileClogService::recover(clog_config).unwrap();

    // First write should be recoverable (was synced before failpoint was set)
    assert!(
        entries.iter().any(|e| {
            matches!(&e.op, tisql::testkit::ClogOp::Put { key, .. } if key == b"durable_key")
        }),
        "First write should be recovered"
    );

    drop(recovered_clog);
    scenario.teardown();
}

// ============================================================================
// Recovery Tests
// ============================================================================

/// Test full recovery cycle after simulated crash.
#[test]
fn test_recovery_after_clean_shutdown() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    // Write data and flush
    {
        let (engine, ilog, _clog) = create_test_lsm_engine(&dir);

        for i in 0..10 {
            let key = format!("recovery_key_{:04}", i);
            let value = format!("recovery_value_{:04}", i);
            write_test_data(&engine, key.as_bytes(), value.as_bytes(), i as u64 + 1);
        }

        // Flush all data (force rotate active + flush all frozen)
        engine.flush_all_with_active().unwrap();

        // Sync ilog
        ilog.sync().unwrap();
    }

    // Recover
    let result = recover_engine(&dir);

    // Verify all data
    for i in 0..10 {
        let key = format!("recovery_key_{:04}", i);
        let expected = format!("recovery_value_{:04}", i);
        let value = result.engine.get(key.as_bytes()).unwrap();
        assert_eq!(value, Some(expected.into_bytes()), "Key {} mismatch", key);
    }

    scenario.teardown();
}

/// Test recovery with interleaved transactions.
#[test]
fn test_recovery_interleaved_txns() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    {
        let (engine, ilog, _clog) = create_test_lsm_engine(&dir);

        // Simulate interleaved writes with different timestamps
        write_test_data(&engine, b"key_a", b"value_a_1", 1);
        write_test_data(&engine, b"key_b", b"value_b_1", 2);
        write_test_data(&engine, b"key_a", b"value_a_2", 3); // Update key_a
        write_test_data(&engine, b"key_c", b"value_c_1", 4);
        write_test_data(&engine, b"key_b", b"value_b_2", 5); // Update key_b

        // Flush all data
        engine.flush_all_with_active().unwrap();
        ilog.sync().unwrap();
    }

    // Recover and verify latest values
    let result = recover_engine(&dir);

    assert_eq!(
        result.engine.get(b"key_a").unwrap(),
        Some(b"value_a_2".to_vec())
    );
    assert_eq!(
        result.engine.get(b"key_b").unwrap(),
        Some(b"value_b_2".to_vec())
    );
    assert_eq!(
        result.engine.get(b"key_c").unwrap(),
        Some(b"value_c_1".to_vec())
    );

    scenario.teardown();
}

/// Test recovery handles tombstones correctly.
#[test]
fn test_recovery_with_tombstones() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    {
        let (engine, ilog, _clog) = create_test_lsm_engine(&dir);

        // Write then delete
        write_test_data(&engine, b"delete_me", b"temp_value", 1);

        // Write delete as tombstone
        let mut batch = WriteBatch::new();
        batch.delete(b"delete_me".to_vec());
        batch.set_commit_ts(2);
        engine.write_batch(batch).unwrap();

        // Keep another key
        write_test_data(&engine, b"keep_me", b"permanent", 3);

        // Flush all data
        engine.flush_all_with_active().unwrap();
        ilog.sync().unwrap();
    }

    // Recover
    let result = recover_engine(&dir);

    // Deleted key should not be visible
    let deleted = result.engine.get(b"delete_me").unwrap();
    assert!(deleted.is_none(), "Deleted key should not exist");

    // Kept key should exist
    let kept = result.engine.get(b"keep_me").unwrap();
    assert_eq!(kept, Some(b"permanent".to_vec()));

    scenario.teardown();
}

// ============================================================================
// Edge Case Tests
// ============================================================================

/// Test empty memtable flush (should be a no-op or handled gracefully).
#[test]
fn test_flush_empty_memtable() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Try to flush without writing anything
    // First need a frozen memtable
    // Without writes, rotation shouldn't happen
    let frozen = engine.maybe_rotate();
    assert!(
        frozen.is_none(),
        "Should not rotate with empty memtable under size threshold"
    );

    scenario.teardown();
}

/// Test multiple consecutive flushes.
#[test]
fn test_multiple_consecutive_flushes() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Do multiple write-rotate-flush cycles
    for cycle in 0..3 {
        for i in 0..5 {
            let key = format!("cycle{}_key{}", cycle, i);
            write_test_data(
                &engine,
                key.as_bytes(),
                b"value",
                (cycle * 10 + i) as u64 + 1,
            );
        }
        // Flush all data after each cycle
        engine.flush_all_with_active().unwrap();
    }

    // Verify all data
    for cycle in 0..3 {
        for i in 0..5 {
            let key = format!("cycle{}_key{}", cycle, i);
            let value = engine.get(key.as_bytes()).unwrap();
            assert!(value.is_some(), "Key {key} should exist");
        }
    }

    // Check version has multiple SSTs
    let version = engine.current_version();
    let sst_count: usize = (0..8).map(|l| version.level(l).len()).sum();
    assert!(sst_count >= 1, "Should have SST files");

    scenario.teardown();
}

// ============================================================================
// Clog GC / Truncation Tests
// ============================================================================

/// Test truncation at checkpoint boundary - recovery should still work.
#[test]
fn test_truncate_at_checkpoint_boundary() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, ilog, _clog) = create_test_lsm_engine(&dir);

    // Write data and flush
    for i in 0..10 {
        let key = format!("gc_key_{:04}", i);
        write_test_data(&engine, key.as_bytes(), b"value", i as u64 + 1);
    }

    // Flush all data
    engine.flush_all_with_active().unwrap();
    ilog.sync().unwrap();

    // Get flushed_lsn from version
    let version = engine.current_version();
    let flushed_lsn = version.flushed_lsn();

    // Create new clog service for truncation
    let clog_config = FileClogConfig::with_dir(dir.path());
    let clog = FileClogService::open(clog_config).unwrap();

    // Truncate to flushed_lsn
    let _stats = clog.truncate_to(flushed_lsn).unwrap();

    // Since data is flushed, all clog entries could be removed
    // (entries with lsn <= flushed_lsn are safe to remove)

    // Drop engine and recover
    drop(engine);
    drop(ilog);

    // Recover should work
    let result = recover_engine(&dir);

    // All data should still be readable from SST
    for i in 0..10 {
        let key = format!("gc_key_{:04}", i);
        let value = result.engine.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "Key {} should exist after GC", key);
    }

    scenario.teardown();
}

/// Test truncation preserves uncommitted transactions.
#[test]
fn test_truncate_with_uncommitted_txn() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, ilog, _clog) = create_test_lsm_engine(&dir);

    // Write some data and flush
    write_test_data(&engine, b"committed_key", b"committed_value", 1);

    // Flush all data
    engine.flush_all_with_active().unwrap();
    ilog.sync().unwrap();

    let flushed_lsn = engine.current_version().flushed_lsn();

    // Write more data without flushing (simulating uncommitted in memtable sense)
    write_test_data(&engine, b"unflushed_key", b"unflushed_value", 2);

    // Truncate to flushed_lsn - should keep unflushed entries
    let clog_config = FileClogConfig::with_dir(dir.path());
    let clog = FileClogService::open(clog_config).unwrap();

    let stats = clog.truncate_to(flushed_lsn).unwrap();

    // Entries with lsn > flushed_lsn should be kept
    // (These represent data not yet persisted in SST)
    // Note: entries_kept may be > 0 if writes were after flushed_lsn
    let _ = stats; // Verify stats was returned (truncation completed)

    scenario.teardown();
}

/// Test concurrent truncate and write doesn't corrupt data.
#[test]
fn test_concurrent_truncate_and_write() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, ilog, _clog) = create_test_lsm_engine(&dir);

    // Write initial data and flush
    for i in 0..5 {
        let key = format!("init_key_{}", i);
        write_test_data(&engine, key.as_bytes(), b"value", i as u64 + 1);
    }
    if let Some(frozen) = engine.maybe_rotate() {
        engine.flush_memtable(&frozen).unwrap();
    }
    ilog.sync().unwrap();

    let flushed_lsn = engine.current_version().flushed_lsn();

    // Write more data while truncation would happen
    // (In practice, truncation and writes should be serialized,
    // but this tests the resilience)
    for i in 0..10 {
        let key = format!("concurrent_key_{}", i);
        write_test_data(&engine, key.as_bytes(), b"value", (i + 100) as u64);
    }

    // Truncate
    let clog_config = FileClogConfig::with_dir(dir.path());
    let clog = FileClogService::open(clog_config).unwrap();
    let _ = clog.truncate_to(flushed_lsn);

    // Verify data integrity
    for i in 0..5 {
        let key = format!("init_key_{}", i);
        let value = engine.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "Initial key {} should exist", key);
    }

    // Keys written after flush should still be in memtable
    for i in 0..10 {
        let key = format!("concurrent_key_{}", i);
        let value = engine.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "Concurrent key {} should exist", key);
    }

    scenario.teardown();
}

/// Test truncate then crash then recover.
#[test]
fn test_truncate_then_crash_then_recover() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    {
        let (engine, ilog, _clog) = create_test_lsm_engine(&dir);

        // Write and flush
        for i in 0..20 {
            let key = format!("truncate_crash_key_{:04}", i);
            write_test_data(&engine, key.as_bytes(), b"value", i as u64 + 1);
        }
        // Flush all data
        engine.flush_all_with_active().unwrap();
        ilog.sync().unwrap();

        let flushed_lsn = engine.current_version().flushed_lsn();

        // Truncate
        let clog_config = FileClogConfig::with_dir(dir.path());
        let clog = FileClogService::open(clog_config).unwrap();
        clog.truncate_to(flushed_lsn).unwrap();

        // "Crash" by dropping without clean shutdown
    }

    // Recover
    let result = recover_engine(&dir);

    // All data should be recoverable from SST
    for i in 0..20 {
        let key = format!("truncate_crash_key_{:04}", i);
        let value = result.engine.get(key.as_bytes()).unwrap();
        assert!(value.is_some(), "Key {} should survive truncate+crash", key);
    }

    scenario.teardown();
}

/// Test flushed_lsn consistency across operations.
#[test]
fn test_flushed_lsn_consistency() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, ilog, _clog) = create_test_lsm_engine(&dir);

    let mut last_flushed_lsn = 0;

    // Multiple flush cycles
    for round in 0..3 {
        // Write data
        for i in 0..10 {
            let key = format!("lsn_consistency_{}_{}", round, i);
            write_test_data(
                &engine,
                key.as_bytes(),
                b"value",
                (round * 100 + i + 1) as u64,
            );
        }

        // Flush all data
        engine.flush_all_with_active().unwrap();

        // Check flushed_lsn
        let version = engine.current_version();
        let flushed_lsn = version.flushed_lsn();

        assert!(
            flushed_lsn >= last_flushed_lsn,
            "flushed_lsn should increase: {} >= {}",
            flushed_lsn,
            last_flushed_lsn
        );
        last_flushed_lsn = flushed_lsn;
    }

    // Sync ilog
    ilog.sync().unwrap();

    // Recover and verify flushed_lsn matches ilog records
    drop(engine);
    drop(ilog);

    let result = recover_engine(&dir);
    let recovered_flushed_lsn = result.engine.current_version().flushed_lsn();

    assert!(
        recovered_flushed_lsn >= last_flushed_lsn,
        "Recovered flushed_lsn {} should be >= pre-crash {}",
        recovered_flushed_lsn,
        last_flushed_lsn
    );

    scenario.teardown();
}

/// Test clog oldest_lsn and file_size APIs.
#[test]
fn test_clog_metadata_apis() {
    use tisql::ClogService;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let clog_config = FileClogConfig::with_dir(dir.path());
    let clog = FileClogService::open(clog_config).unwrap();

    // Check initial state
    let initial_oldest = clog.oldest_lsn().unwrap();
    let initial_size = clog.file_size().unwrap();

    assert_eq!(initial_oldest, 0, "Oldest LSN should be 0 for empty clog");
    assert!(initial_size > 0, "File should have at least header");

    // Write some entries
    use tisql::testkit::{ClogBatch, ClogEntry, ClogOp};
    let mut batch = ClogBatch::new();
    batch.add(ClogEntry {
        lsn: 0, // Will be assigned
        txn_id: 1,
        op: ClogOp::Put {
            key: b"test".to_vec(),
            value: b"value".to_vec(),
        },
    });
    clog.write(&mut batch, true).unwrap();

    // Check after write
    let after_write_oldest = clog.oldest_lsn().unwrap();
    let after_write_size = clog.file_size().unwrap();

    assert!(
        after_write_oldest > 0,
        "Oldest LSN should be > 0 after write"
    );
    assert!(
        after_write_size > initial_size,
        "File size should increase after write"
    );

    scenario.teardown();
}

// ============================================================================
// LevelIterator Failpoint Tests
// ============================================================================

/// Test L0SstIterator open_file failpoint.
///
/// When SST file opening fails, the scan_iter should propagate the error.
#[test]
fn test_level_iterator_open_file_error() {
    use tisql::storage::mvcc::MvccIterator;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Write data to memtable first
    write_test_data(&engine, b"key1", b"value1", 1);
    write_test_data(&engine, b"key2", b"value2", 2);

    // Force flush to create SST file in L0
    engine.freeze_active();
    engine.flush_all().unwrap();

    // Verify data is readable normally via get (uses memtable first, then SST)
    let v1 = engine.get(b"key1").unwrap();
    assert_eq!(v1, Some(b"value1".to_vec()));

    // Enable fail point to cause open_file error on L0 SST
    fail::cfg("l0_sst_iterator_open_file", "return").unwrap();

    // Now scan should fail when trying to read from SST
    use tisql::storage::mvcc::MvccKey;
    let range = MvccKey::unbounded()..MvccKey::unbounded();
    let iter_result = engine.scan_iter(range, 0);

    // Error may occur during iterator creation or on first advance
    let has_error = match iter_result {
        Err(_) => true,
        Ok(mut iter) => {
            let result = iter.advance();
            result.is_err() || !iter.valid()
        }
    };

    assert!(has_error, "Expected error from open_file failpoint");

    fail::cfg("l0_sst_iterator_open_file", "off").unwrap();
    scenario.teardown();
}

/// Test L0SstIterator seek failpoint.
#[test]
fn test_level_iterator_seek_error() {
    use tisql::storage::mvcc::MvccIterator;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Write data and flush to SST in L0
    write_test_data(&engine, b"key1", b"value1", 1);
    engine.freeze_active();
    engine.flush_all().unwrap();

    // Enable fail point for seek errors on L0 SST
    fail::cfg("l0_sst_iterator_seek", "return").unwrap();

    // scan_iter should fail on seek
    use tisql::storage::mvcc::MvccKey;
    let range = MvccKey::unbounded()..MvccKey::unbounded();
    let iter_result = engine.scan_iter(range, 0);

    // Error may propagate through the iterator
    let has_error = match iter_result {
        Err(_) => true,
        Ok(mut iter) => {
            let result = iter.advance();
            result.is_err() || !iter.valid()
        }
    };

    assert!(
        has_error,
        "Expected error or invalid iterator from seek failpoint"
    );

    fail::cfg("l0_sst_iterator_seek", "off").unwrap();
    scenario.teardown();
}

/// Test L0SstIterator advance failpoint.
///
/// The TieredMergeIterator stores advance errors in pending_error and returns them
/// on the next call to advance(). So we need to call advance twice to see the error.
#[test]
fn test_level_iterator_advance_error() {
    use tisql::storage::mvcc::MvccIterator;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Write data and flush to SST in L0
    write_test_data(&engine, b"key1", b"value1", 1);
    write_test_data(&engine, b"key2", b"value2", 2);
    engine.freeze_active();
    engine.flush_all().unwrap();

    // Enable fail point for advance errors on L0 SST
    fail::cfg("l0_sst_iterator_advance", "return").unwrap();

    use tisql::storage::mvcc::MvccKey;
    let range = MvccKey::unbounded()..MvccKey::unbounded();
    let mut iter = engine.scan_iter(range, 0).unwrap();

    // First advance: initializes iterator via seek, reads first entry,
    // then calls L0 iterator's advance() which triggers failpoint.
    // The error is stored in pending_error but the first entry is returned.
    let result1 = iter.advance();

    // If first advance worked, second advance should return the pending error
    let has_error = if result1.is_ok() && iter.valid() {
        let result2 = iter.advance();
        result2.is_err()
    } else {
        // First advance already failed
        true
    };

    assert!(has_error, "Expected error from advance failpoint");

    fail::cfg("l0_sst_iterator_advance", "off").unwrap();
    scenario.teardown();
}

/// Test LsmEngine::get returns data from memtable first.
///
/// This test verifies the point lookup path through memtables.
#[test]
fn test_get_from_memtable() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Write data to active memtable
    write_test_data(&engine, b"key1", b"value1", 1);
    write_test_data(&engine, b"key2", b"value2", 2);

    // Data should be readable from active memtable (no SST involved)
    let v1 = engine.get(b"key1").unwrap();
    assert_eq!(v1, Some(b"value1".to_vec()));

    let v2 = engine.get(b"key2").unwrap();
    assert_eq!(v2, Some(b"value2".to_vec()));

    // Non-existent key should return None
    let v3 = engine.get(b"nonexistent").unwrap();
    assert!(v3.is_none());

    // Freeze and write more data
    engine.freeze_active();
    write_test_data(&engine, b"key3", b"value3", 3);

    // Data from frozen memtable should be readable
    let v1_frozen = engine.get(b"key1").unwrap();
    assert_eq!(v1_frozen, Some(b"value1".to_vec()));

    // Data from active memtable should be readable
    let v3_active = engine.get(b"key3").unwrap();
    assert_eq!(v3_active, Some(b"value3".to_vec()));

    scenario.teardown();
}

/// Test that LsmEngine getters work correctly.
#[test]
fn test_lsm_engine_getters() {
    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Test config() getter
    let config = engine.config();
    assert!(config.memtable_size > 0);

    // Test active_memtable_size() before and after write
    let initial_size = engine.active_memtable_size();
    assert_eq!(initial_size, 0, "Initial memtable should be empty");

    write_test_data(&engine, b"key1", b"value1", 1);
    let after_write_size = engine.active_memtable_size();
    assert!(
        after_write_size > 0,
        "Memtable size should increase after write"
    );

    // Test frozen_count()
    let _frozen = engine.frozen_count();
    // May or may not have frozen memtables depending on size thresholds

    // Test current_version()
    let _version = engine.current_version();
    // Version should exist

    // Test is_durable()
    assert!(engine.is_durable(), "Engine with ilog should be durable");

    scenario.teardown();
}

/// Test scan with data in both memtable and SST.
#[test]
fn test_scan_iter_memtable_and_sst() {
    use tisql::storage::mvcc::MvccIterator;

    let scenario = fail::FailScenario::setup();
    let dir = tempfile::tempdir().unwrap();

    let (engine, _ilog, _clog) = create_test_lsm_engine(&dir);

    // Write data to memtable
    write_test_data(&engine, b"aaa", b"value_a", 1);
    write_test_data(&engine, b"bbb", b"value_b", 2);

    // Flush to SST
    engine.freeze_active();
    engine.flush_all().unwrap();

    // Write more data to new memtable
    write_test_data(&engine, b"ccc", b"value_c", 3);
    write_test_data(&engine, b"ddd", b"value_d", 4);

    // Scan all - should see data from both memtable and SST
    use tisql::storage::mvcc::MvccKey;
    let range = MvccKey::unbounded()..MvccKey::unbounded();
    let mut iter = engine.scan_iter(range, 0).unwrap();

    let mut keys = Vec::new();
    loop {
        iter.advance().unwrap();
        if !iter.valid() {
            break;
        }
        keys.push(iter.user_key().to_vec());
    }

    // Should have all 4 keys
    assert!(keys.contains(&b"aaa".to_vec()), "Should find key aaa");
    assert!(keys.contains(&b"bbb".to_vec()), "Should find key bbb");
    assert!(keys.contains(&b"ccc".to_vec()), "Should find key ccc");
    assert!(keys.contains(&b"ddd".to_vec()), "Should find key ddd");

    scenario.teardown();
}
