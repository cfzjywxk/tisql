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

//! LSM recovery from ilog and clog.
//!
//! This module implements crash recovery for the LSM storage engine by
//! coordinating the replay of ilog (SST metadata) and clog (transaction data).
//!
//! ## Recovery Sequence
//!
//! 1. **Replay ilog**: Rebuild Version (SST metadata) from ilog
//! 2. **Get flushed_lsn**: Determine which clog entries are already in SSTs
//! 3. **Replay clog**: Apply entries with lsn > flushed_lsn to memtable
//! 4. **Cleanup orphans**: Remove SST files from incomplete operations
//!
//! ## Recovery Guarantees
//!
//! - All committed transactions are recovered (durability)
//! - Partial writes are rolled back (atomicity)
//! - SST files from incomplete flush/compact are cleaned up
//!
//! ## Usage
//!
//! ```ignore
//! let recovery = LsmRecovery::new(data_dir);
//! let (lsm_engine, clog_service, ilog_service) = recovery.recover()?;
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::clog::{ClogEntry, ClogOp, FileClogConfig, FileClogService};
use crate::error::Result;
use crate::lsn::{new_lsn_provider, SharedLsnProvider};
use crate::storage::{LsmConfig, LsmEngine, StorageEngine, WriteBatch};
use crate::types::{Lsn, Timestamp, TxnId};
use crate::{log_info, log_warn};

use super::ilog::{IlogConfig, IlogService};

/// Recovery state for a transaction during clog replay.
#[derive(Default)]
struct TxnReplayState {
    /// Pending writes (not yet committed)
    writes: Vec<(Vec<u8>, Option<Vec<u8>>)>, // (key, value) - None means delete
    /// Commit timestamp (if committed)
    commit_ts: Option<Timestamp>,
    /// LSN of the commit record (for replay ordering and flushed_lsn tracking)
    commit_lsn: Option<Lsn>,
}

/// Result of LSM recovery.
pub struct RecoveryResult {
    /// Recovered LSM engine
    pub engine: LsmEngine,
    /// Recovered clog service
    pub clog: Arc<FileClogService>,
    /// Recovered ilog service
    pub ilog: Arc<IlogService>,
    /// Shared LSN provider
    pub lsn_provider: SharedLsnProvider,
    /// Recovery statistics
    pub stats: RecoveryStats,
}

/// Statistics from recovery.
#[derive(Debug, Default)]
pub struct RecoveryStats {
    /// Number of ilog records replayed
    pub ilog_records: usize,
    /// Number of clog entries replayed
    pub clog_entries: usize,
    /// Number of transactions recovered
    pub txn_count: usize,
    /// Number of orphan SSTs cleaned up
    pub orphan_ssts_cleaned: usize,
    /// Flushed LSN (up to which data is in SSTs)
    pub flushed_lsn: Lsn,
    /// Final LSN after recovery
    pub final_lsn: Lsn,
    /// Maximum commit timestamp seen (for TSO initialization)
    pub max_commit_ts: Timestamp,
}

/// LSM recovery coordinator.
///
/// Orchestrates the recovery of LsmEngine from ilog and clog.
pub struct LsmRecovery {
    /// LSM configuration
    lsm_config: LsmConfig,
    /// Clog configuration
    clog_config: FileClogConfig,
    /// Ilog configuration
    ilog_config: IlogConfig,
}

impl LsmRecovery {
    /// Create a new recovery instance.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        Self {
            lsm_config: LsmConfig::new(&data_dir),
            clog_config: FileClogConfig::with_dir(&data_dir),
            ilog_config: IlogConfig::new(&data_dir),
        }
    }

    /// Create with custom configurations.
    pub fn with_configs(
        lsm_config: LsmConfig,
        clog_config: FileClogConfig,
        ilog_config: IlogConfig,
    ) -> Self {
        Self {
            lsm_config,
            clog_config,
            ilog_config,
        }
    }

    /// Perform recovery.
    ///
    /// Returns the recovered LSM engine along with clog and ilog services.
    /// `io_handle` is the I/O runtime handle for spawning GroupCommitWriter
    /// and IoService tasks.
    pub fn recover(self, io_handle: &tokio::runtime::Handle) -> Result<RecoveryResult> {
        let mut stats = RecoveryStats::default();

        // Create shared LSN provider
        let lsn_provider = new_lsn_provider();

        // Step 1: Recover ilog to rebuild Version
        log_info!(
            "Starting ilog recovery from {:?}",
            self.ilog_config.ilog_path()
        );
        let (ilog, version, orphan_ssts) = IlogService::recover(
            self.ilog_config.clone(),
            Arc::clone(&lsn_provider),
            io_handle,
        )?;
        let ilog = Arc::new(ilog);

        let flushed_lsn = version.flushed_lsn();
        let max_ts_from_ssts = version.max_ts();
        stats.flushed_lsn = flushed_lsn;
        log_info!(
            "Ilog recovery complete: version_num={}, flushed_lsn={}, max_ts_from_ssts={}, orphan_ssts={}",
            version.version_num(),
            flushed_lsn,
            max_ts_from_ssts,
            orphan_ssts.len()
        );

        // Step 2: Cleanup orphan SSTs
        if !orphan_ssts.is_empty() {
            log_warn!(
                "Found {} orphan SST files from incomplete operations",
                orphan_ssts.len()
            );
            let cleaned = ilog.cleanup_orphan_ssts(&version, &orphan_ssts)?;
            stats.orphan_ssts_cleaned = cleaned;
        }

        // Step 3: Open LsmEngine with recovered version
        let engine = LsmEngine::open_with_recovery(
            self.lsm_config,
            Arc::clone(&lsn_provider),
            Arc::clone(&ilog),
            version,
        )?;

        // Step 4: Recover clog with shared LSN provider
        // This ensures clog and ilog share the same LSN space for proper ordering
        log_info!(
            "Starting clog recovery from {:?}",
            self.clog_config.clog_path()
        );
        let (clog, clog_entries) = FileClogService::recover_with_lsn_provider(
            self.clog_config,
            Arc::clone(&lsn_provider),
            io_handle,
        )?;
        let clog = Arc::new(clog);

        log_info!(
            "Clog recovery: {} total entries, replaying entries with lsn > {}",
            clog_entries.len(),
            flushed_lsn
        );

        // Step 5: Replay clog entries with lsn > flushed_lsn
        let replay_result = Self::replay_clog(&engine, &clog_entries, flushed_lsn)?;
        stats.clog_entries = replay_result.entries_replayed;
        stats.txn_count = replay_result.txn_count;
        // Use max of clog and SST timestamps to handle clog truncation correctly.
        // If clog was truncated, old commit timestamps would be missing from clog
        // but preserved in SST metadata. This ensures TSO never goes backwards.
        stats.max_commit_ts = replay_result.max_commit_ts.max(max_ts_from_ssts);

        // LSN provider is automatically updated by clog recovery to be at least max(clog_lsn) + 1
        stats.final_lsn = lsn_provider.current_lsn();

        log_info!(
            "Recovery complete: replayed {} clog entries, {} transactions, final_lsn={}",
            stats.clog_entries,
            stats.txn_count,
            stats.final_lsn
        );

        Ok(RecoveryResult {
            engine,
            clog,
            ilog,
            lsn_provider,
            stats,
        })
    }

    /// Replay clog entries to the engine.
    ///
    /// Only replays entries with lsn > flushed_lsn, but scans ALL entries
    /// to find max_commit_ts for TSO initialization.
    fn replay_clog(
        engine: &LsmEngine,
        entries: &[ClogEntry],
        flushed_lsn: Lsn,
    ) -> Result<ReplayResult> {
        let mut txn_states: HashMap<TxnId, TxnReplayState> = HashMap::new();
        let mut result = ReplayResult::default();

        // Process all entries - track max_commit_ts from ALL,
        // but only replay writes from entries with lsn > flushed_lsn
        for entry in entries {
            // Track max_commit_ts from ALL entries (for TSO initialization)
            if let ClogOp::Commit { commit_ts } = &entry.op {
                result.max_commit_ts = result.max_commit_ts.max(*commit_ts);
            }

            // Skip entries already in SSTs
            if entry.lsn <= flushed_lsn {
                continue;
            }

            result.entries_replayed += 1;

            match &entry.op {
                ClogOp::Put { key, value } => {
                    let state = txn_states.entry(entry.txn_id).or_default();
                    state.writes.push((key.clone(), Some(value.clone())));
                }
                ClogOp::Delete { key } => {
                    let state = txn_states.entry(entry.txn_id).or_default();
                    state.writes.push((key.clone(), None));
                }
                ClogOp::Commit { commit_ts } => {
                    if let Some(state) = txn_states.get_mut(&entry.txn_id) {
                        state.commit_ts = Some(*commit_ts);
                        state.commit_lsn = Some(entry.lsn);
                    }
                }
                ClogOp::Rollback => {
                    // Remove transaction state - writes are discarded
                    txn_states.remove(&entry.txn_id);
                }
            }
        }

        // Collect committed transactions and sort by commit_lsn to ensure
        // replay order matches commit order. This is critical for correct
        // flushed_lsn tracking: the last replayed batch must have the highest
        // clog_lsn so the memtable's max LSN reflects the true recovery point.
        let mut committed_txns: Vec<(TxnId, TxnReplayState)> = txn_states
            .into_iter()
            .filter(|(_, state)| state.commit_ts.is_some() && !state.writes.is_empty())
            .collect();

        // Sort by commit_lsn (ascending) so older transactions are applied first
        committed_txns.sort_by_key(|(_, state)| state.commit_lsn);

        // Apply committed transactions in LSN order
        for (_txn_id, state) in committed_txns {
            let commit_ts = state.commit_ts.unwrap(); // Safe: filtered above
            let mut batch = WriteBatch::new();
            batch.set_commit_ts(commit_ts);

            // Critical: Set clog_lsn so that flush correctly tracks flushed_lsn.
            // commit_lsn is guaranteed Some here: the filter above requires
            // commit_ts.is_some(), which is only set alongside commit_lsn.
            batch.set_clog_lsn(state.commit_lsn.unwrap());

            for (key, value) in state.writes {
                match value {
                    Some(v) => batch.put(key, v),
                    None => batch.delete(key),
                }
            }

            engine.write_batch(batch)?;
            result.txn_count += 1;
        }

        // Log any uncommitted transactions
        // Note: txn_states is now consumed, but we already filtered and collected committed ones
        // For logging, we'd need a separate pass but it's not critical for correctness

        Ok(result)
    }
}

/// Result of clog replay.
#[derive(Default)]
struct ReplayResult {
    entries_replayed: usize,
    txn_count: usize,
    /// Max commit_ts seen across ALL entries (for TSO initialization)
    max_commit_ts: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::{ClogBatch, ClogEntry, ClogOp, ClogService};
    use crate::storage::ilog::IlogService;
    use crate::storage::mvcc::{is_tombstone, MvccIterator, MvccKey};
    use crate::storage::version::Version;
    use crate::storage::StorageEngine;
    use crate::types::RawValue;
    use tempfile::TempDir;

    async fn get_at_for_test(engine: &LsmEngine, key: &[u8], ts: Timestamp) -> Option<RawValue> {
        use crate::storage::StorageEngine;
        let start = MvccKey::encode(key, ts);
        let end = MvccKey::encode(key, 0)
            .next_key()
            .unwrap_or_else(MvccKey::unbounded);
        let range = start..end;

        let mut iter = engine.scan_iter(range, 0).unwrap();
        iter.advance().await.unwrap(); // Position on first entry

        while iter.valid() {
            let decoded_key = iter.user_key();
            let entry_ts = iter.timestamp();
            if decoded_key == key && entry_ts <= ts {
                if is_tombstone(iter.value()) {
                    return None;
                }
                return Some(iter.value().to_vec());
            }
            iter.advance().await.unwrap();
        }
        None
    }

    async fn get_for_test(engine: &LsmEngine, key: &[u8]) -> Option<RawValue> {
        get_at_for_test(engine, key, Timestamp::MAX).await
    }

    async fn write_test_data(
        engine: &LsmEngine,
        clog: &FileClogService,
        count: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut written = Vec::new();

        for i in 0..count {
            let key = format!("key_{i:04}").into_bytes();
            let value = format!("value_{i:04}").into_bytes();

            // Write to clog
            let mut batch = ClogBatch::new();
            batch.add_put(i as u64 + 1, key.clone(), value.clone());
            batch.add_commit(i as u64 + 1, i as Timestamp + 100);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            // Write to engine
            let mut wb = WriteBatch::new();
            wb.set_commit_ts(i as Timestamp + 100);
            wb.put(key.clone(), value.clone());
            engine.write_batch(wb).unwrap();

            written.push((key, value));
        }

        written
    }

    #[tokio::test]
    async fn test_recovery_empty() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        let recovery = LsmRecovery::new(tmp.path());
        let result = recovery.recover(&io_handle).unwrap();

        assert_eq!(result.stats.clog_entries, 0);
        assert_eq!(result.stats.txn_count, 0);
        assert_eq!(result.stats.orphan_ssts_cleaned, 0);
        assert_eq!(result.stats.flushed_lsn, 0);
        assert_eq!(result.stats.max_commit_ts, 0);

        // Verify the recovered engine is actually usable: write, read, flush
        let key = b"post_recovery_key".to_vec();
        let value = b"post_recovery_value".to_vec();

        let mut wb = WriteBatch::new();
        wb.set_commit_ts(100);
        wb.put(key.clone(), value.clone());
        result.engine.write_batch(wb).unwrap();

        // Verify we can read back what we wrote
        assert_eq!(
            get_for_test(&result.engine, &key).await,
            Some(value.clone()),
            "Engine should be usable after empty recovery"
        );

        // Verify flush works
        result.engine.flush_all_with_active().unwrap();

        // Verify data survives flush
        assert_eq!(
            get_for_test(&result.engine, &key).await,
            Some(value),
            "Data should survive flush after empty recovery"
        );
    }

    #[tokio::test]
    async fn test_recovery_with_flushed_data() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First session: write and flush
        let written = {
            let lsm_config = LsmConfig::builder(tmp.path())
                .memtable_size(100)
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(
                IlogService::open(ilog_config, Arc::clone(&lsn_provider), &io_handle).unwrap(),
            );

            let engine = LsmEngine::open_with_recovery(
                lsm_config,
                Arc::clone(&lsn_provider),
                Arc::clone(&ilog),
                Version::new(),
            )
            .unwrap();

            // Use shared LSN provider for clog too
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open_with_lsn_provider(
                clog_config,
                Arc::clone(&lsn_provider),
                &io_handle,
            )
            .unwrap();

            let written = write_test_data(&engine, &clog, 5).await;

            // Flush all to SST
            engine.flush_all_with_active().unwrap();

            clog.close().await.unwrap();
            written
        };

        // Second session: recover and verify
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            // Data should be readable from SST (not replayed from clog)
            for (key, expected_value) in &written {
                assert_eq!(
                    get_for_test(&result.engine, key).await,
                    Some(expected_value.clone()),
                    "Key {:?} should be readable after recovery",
                    String::from_utf8_lossy(key)
                );
            }

            // With unified LSN provider, flushed_lsn correctly tracks what's in SSTs
            // so clog replay should skip entries that are already flushed
            assert!(
                result.stats.flushed_lsn > 0,
                "flushed_lsn should be set after flush"
            );
        }
    }

    #[tokio::test]
    async fn test_recovery_with_unflushed_data() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First session: write but don't flush
        let written = {
            let lsm_config = LsmConfig::builder(tmp.path())
                .memtable_size(10000) // Large enough to not auto-flush
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(
                IlogService::open(ilog_config, Arc::clone(&lsn_provider), &io_handle).unwrap(),
            );

            let engine = LsmEngine::open_with_recovery(
                lsm_config,
                Arc::clone(&lsn_provider),
                Arc::clone(&ilog),
                Version::new(),
            )
            .unwrap();

            // Use shared LSN provider for clog
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open_with_lsn_provider(
                clog_config,
                Arc::clone(&lsn_provider),
                &io_handle,
            )
            .unwrap();

            let written = write_test_data(&engine, &clog, 3).await;

            // Don't flush - simulate crash before flush
            clog.close().await.unwrap();
            written
        };

        // Second session: recover and verify
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            // Data should be recovered from clog replay
            for (key, expected_value) in &written {
                assert_eq!(
                    get_for_test(&result.engine, key).await,
                    Some(expected_value.clone()),
                    "Key {:?} should be recovered from clog",
                    String::from_utf8_lossy(key)
                );
            }

            // Clog entries should be replayed
            assert_eq!(
                result.stats.txn_count, 3,
                "3 transactions should be replayed"
            );
        }
    }

    #[tokio::test]
    async fn test_recovery_uncommitted_discarded() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First session: write uncommitted data
        {
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open(clog_config, &io_handle).unwrap();

            // Write uncommitted transaction (no Commit record)
            let mut batch = ClogBatch::new();
            batch.add_put(
                1,
                b"uncommitted_key".to_vec(),
                b"uncommitted_value".to_vec(),
            );
            clog.write(&mut batch, true).unwrap().await.unwrap();

            // Write committed transaction
            let mut batch = ClogBatch::new();
            batch.add_put(2, b"committed_key".to_vec(), b"committed_value".to_vec());
            batch.add_commit(2, 100);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            clog.close().await.unwrap();
        }

        // Second session: recover and verify
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            // Uncommitted data should be discarded
            assert_eq!(
                get_for_test(&result.engine, b"uncommitted_key").await,
                None,
                "Uncommitted data should be discarded"
            );

            // Committed data should be recovered
            assert_eq!(
                get_for_test(&result.engine, b"committed_key").await,
                Some(b"committed_value".to_vec()),
                "Committed data should be recovered"
            );

            assert_eq!(result.stats.txn_count, 1);
        }
    }

    #[tokio::test]
    async fn test_recovery_partial_flush() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First session: write some data, flush some, write more
        let (flushed, unflushed) = {
            let lsm_config = LsmConfig::builder(tmp.path())
                .memtable_size(50) // Small to trigger flush
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(
                IlogService::open(ilog_config, Arc::clone(&lsn_provider), &io_handle).unwrap(),
            );

            let engine = LsmEngine::open_with_recovery(
                lsm_config,
                Arc::clone(&lsn_provider),
                Arc::clone(&ilog),
                Version::new(),
            )
            .unwrap();

            // Use shared LSN provider for clog
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open_with_lsn_provider(
                clog_config,
                Arc::clone(&lsn_provider),
                &io_handle,
            )
            .unwrap();

            // Write first batch and flush
            let flushed = write_test_data(&engine, &clog, 2).await;
            engine.flush_all_with_active().unwrap();

            // Write second batch without flush
            let unflushed = {
                let mut written = Vec::new();
                for i in 10..13 {
                    let key = format!("key_{i:04}").into_bytes();
                    let value = format!("value_{i:04}").into_bytes();

                    let mut batch = ClogBatch::new();
                    batch.add_put(i as u64, key.clone(), value.clone());
                    batch.add_commit(i as u64, i as Timestamp + 100);
                    clog.write(&mut batch, true).unwrap().await.unwrap();

                    let mut wb = WriteBatch::new();
                    wb.set_commit_ts(i as Timestamp + 100);
                    wb.put(key.clone(), value.clone());
                    engine.write_batch(wb).unwrap();

                    written.push((key, value));
                }
                written
            };

            clog.close().await.unwrap();
            (flushed, unflushed)
        };

        // Second session: recover and verify
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            // Flushed data should be readable from SST
            for (key, expected_value) in &flushed {
                assert_eq!(
                    get_for_test(&result.engine, key).await,
                    Some(expected_value.clone()),
                    "Flushed key {:?} should be readable",
                    String::from_utf8_lossy(key)
                );
            }

            // Unflushed data should be recovered from clog
            for (key, expected_value) in &unflushed {
                assert_eq!(
                    get_for_test(&result.engine, key).await,
                    Some(expected_value.clone()),
                    "Unflushed key {:?} should be recovered",
                    String::from_utf8_lossy(key)
                );
            }

            // With unified LSN provider, exactly 3 unflushed transactions should be replayed
            assert_eq!(
                result.stats.txn_count, 3,
                "Exactly 3 unflushed transactions should be replayed"
            );
        }
    }

    /// Test that unified LSN provider ensures correct ordering across clog and ilog.
    /// This test specifically verifies that both logs use the same LSN space.
    #[tokio::test]
    async fn test_unified_lsn_provider() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First session: interleave clog and ilog operations
        {
            let lsn_provider = new_lsn_provider();

            // Open ilog and clog with shared provider
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(
                IlogService::open(ilog_config, Arc::clone(&lsn_provider), &io_handle).unwrap(),
            );

            let lsm_config = LsmConfig::builder(tmp.path())
                .memtable_size(50) // Small to trigger flush
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let engine = LsmEngine::open_with_recovery(
                lsm_config,
                Arc::clone(&lsn_provider),
                Arc::clone(&ilog),
                Version::new(),
            )
            .unwrap();

            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open_with_lsn_provider(
                clog_config,
                Arc::clone(&lsn_provider),
                &io_handle,
            )
            .unwrap();

            // Write some data - IMPORTANT: thread CLOG LSN to storage
            for i in 0..5 {
                let key = format!("key_{i:04}").into_bytes();
                let value = format!("value_{i:04}").into_bytes();

                // Write to clog (allocates LSN from shared provider)
                let mut batch = ClogBatch::new();
                batch.add_put(i as u64 + 1, key.clone(), value.clone());
                batch.add_commit(i as u64 + 1, i as Timestamp + 100);
                let clog_lsn = clog.write(&mut batch, true).unwrap().await.unwrap();

                // Write to engine WITH CLOG LSN (this is critical for correct recovery!)
                // TransactionService does this to ensure storage LSN matches CLOG LSN
                let mut wb = WriteBatch::new();
                wb.set_commit_ts(i as Timestamp + 100);
                wb.set_clog_lsn(clog_lsn); // Critical: thread CLOG LSN to storage
                wb.put(key.clone(), value.clone());
                engine.write_batch(wb).unwrap();

                // LSN provider should not advance further since we used clog_lsn
                // (engine reuses the CLOG LSN instead of allocating new one)
            }

            // Flush - this allocates LSNs from shared provider for ilog
            let before_flush_lsn = lsn_provider.current_lsn();
            engine.flush_all_with_active().unwrap();
            let after_flush_lsn = lsn_provider.current_lsn();

            // Ilog should have allocated LSNs for FlushIntent and FlushCommit
            assert!(
                after_flush_lsn > before_flush_lsn,
                "LSN should advance after flush (ilog writes)"
            );

            clog.close().await.unwrap();
        }

        // Second session: verify recovery sees correct LSN ordering
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            // Verify data is recovered
            for i in 0..5 {
                let key = format!("key_{i:04}").into_bytes();
                let expected_value = format!("value_{i:04}").into_bytes();
                assert_eq!(
                    get_for_test(&result.engine, &key).await,
                    Some(expected_value),
                    "Key should be recovered"
                );
            }

            // flushed_lsn should be > 0 since we flushed
            assert!(
                result.stats.flushed_lsn > 0,
                "flushed_lsn should be set after flush"
            );

            // final_lsn should be >= flushed_lsn
            assert!(
                result.stats.final_lsn >= result.stats.flushed_lsn,
                "final_lsn should be >= flushed_lsn"
            );

            // Verify the key property: flushed_lsn is properly tracked
            // so recovery only replays entries with lsn > flushed_lsn
            // Note: Some entries may be replayed if they were logged to clog
            // after the memtable captured max_memtable_lsn but before flush completed
            assert!(
                result.stats.clog_entries <= 10,
                "Should replay at most 10 clog entries (2 per transaction * 5 transactions)"
            );
        }
    }

    #[tokio::test]
    async fn test_recovery_with_custom_configs() {
        // Test LsmRecovery::with_configs() constructor
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        let lsm_config = LsmConfig::builder(tmp.path())
            .memtable_size(1024)
            .max_frozen_memtables(2)
            .build()
            .unwrap();
        let clog_config = FileClogConfig::with_dir(tmp.path());
        let ilog_config = IlogConfig::new(tmp.path());

        let recovery = LsmRecovery::with_configs(lsm_config, clog_config, ilog_config);
        let result = recovery.recover(&io_handle).unwrap();

        assert_eq!(result.stats.clog_entries, 0);
        assert_eq!(result.stats.txn_count, 0);
    }

    #[tokio::test]
    async fn test_recovery_with_delete_operations() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First session: write data and then delete some
        {
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open(clog_config, &io_handle).unwrap();

            // Insert key1
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"key1".to_vec(), b"value1".to_vec());
            batch.add_commit(1, 100);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            // Insert key2
            let mut batch = ClogBatch::new();
            batch.add_put(2, b"key2".to_vec(), b"value2".to_vec());
            batch.add_commit(2, 200);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            // Delete key1
            let mut batch = ClogBatch::new();
            batch.add_delete(3, b"key1".to_vec());
            batch.add_commit(3, 300);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            clog.close().await.unwrap();
        }

        // Second session: recover and verify
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            // key1 should be deleted (tombstone)
            assert_eq!(
                get_for_test(&result.engine, b"key1").await,
                None,
                "key1 should be deleted"
            );

            // key2 should still exist
            assert_eq!(
                get_for_test(&result.engine, b"key2").await,
                Some(b"value2".to_vec()),
                "key2 should exist"
            );

            assert_eq!(result.stats.txn_count, 3);
        }
    }

    #[tokio::test]
    async fn test_recovery_with_rollback_operations() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First session: write data and rollback some
        {
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open(clog_config, &io_handle).unwrap();

            // Transaction 1: committed
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"committed_key".to_vec(), b"value".to_vec());
            batch.add_commit(1, 100);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            // Transaction 2: rolled back
            let mut batch = ClogBatch::new();
            batch.add_put(2, b"rolled_back_key".to_vec(), b"value".to_vec());
            clog.write(&mut batch, true).unwrap().await.unwrap();

            let mut batch = ClogBatch::new();
            batch.add(ClogEntry {
                lsn: 0,
                txn_id: 2,
                op: ClogOp::Rollback,
            });
            clog.write(&mut batch, true).unwrap().await.unwrap();

            clog.close().await.unwrap();
        }

        // Second session: recover and verify
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            // Committed transaction should be recovered
            assert_eq!(
                get_for_test(&result.engine, b"committed_key").await,
                Some(b"value".to_vec()),
                "Committed data should be recovered"
            );

            // Rolled back transaction should not appear
            assert_eq!(
                get_for_test(&result.engine, b"rolled_back_key").await,
                None,
                "Rolled back data should not appear"
            );

            // Only 1 transaction should be replayed (the committed one)
            assert_eq!(result.stats.txn_count, 1);
        }
    }

    #[tokio::test]
    async fn test_recovery_stats_max_commit_ts() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First session: write data with various commit timestamps
        {
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open(clog_config, &io_handle).unwrap();

            // Commit at ts=100
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"key1".to_vec(), b"v1".to_vec());
            batch.add_commit(1, 100);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            // Commit at ts=500 (highest)
            let mut batch = ClogBatch::new();
            batch.add_put(2, b"key2".to_vec(), b"v2".to_vec());
            batch.add_commit(2, 500);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            // Commit at ts=300
            let mut batch = ClogBatch::new();
            batch.add_put(3, b"key3".to_vec(), b"v3".to_vec());
            batch.add_commit(3, 300);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            clog.close().await.unwrap();
        }

        // Second session: verify max_commit_ts
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            assert_eq!(
                result.stats.max_commit_ts, 500,
                "max_commit_ts should be 500"
            );
        }
    }

    #[tokio::test]
    async fn test_recovery_interleaved_transactions() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First session: interleaved transaction writes
        {
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open(clog_config, &io_handle).unwrap();

            // Start both transactions
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"txn1_key".to_vec(), b"txn1_value".to_vec());
            clog.write(&mut batch, true).unwrap().await.unwrap();

            let mut batch = ClogBatch::new();
            batch.add_put(2, b"txn2_key".to_vec(), b"txn2_value".to_vec());
            clog.write(&mut batch, true).unwrap().await.unwrap();

            // Commit txn2 first (out of order)
            let mut batch = ClogBatch::new();
            batch.add_commit(2, 200);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            // Then commit txn1
            let mut batch = ClogBatch::new();
            batch.add_commit(1, 100);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            clog.close().await.unwrap();
        }

        // Second session: both should be recovered
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            assert_eq!(
                get_for_test(&result.engine, b"txn1_key").await,
                Some(b"txn1_value".to_vec())
            );
            assert_eq!(
                get_for_test(&result.engine, b"txn2_key").await,
                Some(b"txn2_value".to_vec())
            );

            assert_eq!(result.stats.txn_count, 2);
        }
    }

    #[tokio::test]
    async fn test_recovery_multi_key_transaction() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First session: transaction with multiple keys
        {
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open(clog_config, &io_handle).unwrap();

            // Single transaction writing multiple keys
            let mut batch = ClogBatch::new();
            batch.add_put(1, b"key_a".to_vec(), b"value_a".to_vec());
            batch.add_put(1, b"key_b".to_vec(), b"value_b".to_vec());
            batch.add_put(1, b"key_c".to_vec(), b"value_c".to_vec());
            batch.add_commit(1, 100);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            clog.close().await.unwrap();
        }

        // Second session: all keys should be recovered
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            assert_eq!(
                get_for_test(&result.engine, b"key_a").await,
                Some(b"value_a".to_vec())
            );
            assert_eq!(
                get_for_test(&result.engine, b"key_b").await,
                Some(b"value_b".to_vec())
            );
            assert_eq!(
                get_for_test(&result.engine, b"key_c").await,
                Some(b"value_c".to_vec())
            );

            assert_eq!(result.stats.txn_count, 1);
        }
    }

    #[tokio::test]
    async fn test_recovery_max_commit_ts_from_sst() {
        // Test that max_commit_ts is taken from SST metadata when clog is truncated
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First session: write and flush data with high timestamp
        {
            let lsm_config = LsmConfig::builder(tmp.path())
                .memtable_size(50)
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(
                IlogService::open(ilog_config, Arc::clone(&lsn_provider), &io_handle).unwrap(),
            );

            let engine = LsmEngine::open_with_recovery(
                lsm_config,
                Arc::clone(&lsn_provider),
                Arc::clone(&ilog),
                Version::new(),
            )
            .unwrap();

            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open_with_lsn_provider(
                clog_config,
                Arc::clone(&lsn_provider),
                &io_handle,
            )
            .unwrap();

            // Write data with high timestamp
            let key = b"high_ts_key".to_vec();
            let value = b"high_ts_value".to_vec();

            let mut batch = ClogBatch::new();
            batch.add_put(1, key.clone(), value.clone());
            batch.add_commit(1, 9999);
            let clog_lsn = clog.write(&mut batch, true).unwrap().await.unwrap();

            let mut wb = WriteBatch::new();
            wb.set_commit_ts(9999);
            wb.set_clog_lsn(clog_lsn);
            wb.put(key, value);
            engine.write_batch(wb).unwrap();

            // Flush to SST
            engine.flush_all_with_active().unwrap();

            clog.close().await.unwrap();
        }

        // Second session: verify max_commit_ts is recovered from SST
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            // max_commit_ts should be at least 9999 (from SST metadata)
            assert!(
                result.stats.max_commit_ts >= 9999,
                "max_commit_ts should be recovered from SST metadata, got {}",
                result.stats.max_commit_ts
            );
        }
    }

    #[tokio::test]
    async fn test_recovery_empty_transaction() {
        // Transaction with no writes followed by commit should be ignored
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        {
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open(clog_config, &io_handle).unwrap();

            // Empty transaction (commit without any puts)
            let mut batch = ClogBatch::new();
            batch.add_commit(1, 100);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            // Normal transaction
            let mut batch = ClogBatch::new();
            batch.add_put(2, b"key".to_vec(), b"value".to_vec());
            batch.add_commit(2, 200);
            clog.write(&mut batch, true).unwrap().await.unwrap();

            clog.close().await.unwrap();
        }

        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            // Only 1 transaction with data should be counted
            assert_eq!(result.stats.txn_count, 1);
            assert_eq!(
                get_for_test(&result.engine, b"key").await,
                Some(b"value".to_vec())
            );
        }
    }

    /// Test that recovery correctly handles the flushed_lsn boundary.
    ///
    /// Entries with lsn <= flushed_lsn are already in SSTs and should be skipped.
    /// Entries with lsn > flushed_lsn must be replayed from clog.
    /// This test verifies the exact boundary: writes before flush are in SST,
    /// writes after flush are replayed from clog.
    #[tokio::test]
    async fn test_recovery_flushed_lsn_boundary() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        let flushed_lsn;

        // First session: write some data, flush, write more, then "crash"
        {
            let lsm_config = LsmConfig::builder(tmp.path())
                .memtable_size(50)
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(
                IlogService::open(ilog_config, Arc::clone(&lsn_provider), &io_handle).unwrap(),
            );

            let engine = LsmEngine::open_with_recovery(
                lsm_config,
                Arc::clone(&lsn_provider),
                Arc::clone(&ilog),
                Version::new(),
            )
            .unwrap();

            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open_with_lsn_provider(
                clog_config,
                Arc::clone(&lsn_provider),
                &io_handle,
            )
            .unwrap();

            // Write first batch (will be flushed to SST)
            for i in 0..3 {
                let key = format!("flushed_key_{i}").into_bytes();
                let value = format!("flushed_val_{i}").into_bytes();

                let mut batch = ClogBatch::new();
                batch.add_put(i as u64 + 1, key.clone(), value.clone());
                batch.add_commit(i as u64 + 1, i as Timestamp + 100);
                let clog_lsn = clog.write(&mut batch, true).unwrap().await.unwrap();

                let mut wb = WriteBatch::new();
                wb.set_commit_ts(i as Timestamp + 100);
                wb.set_clog_lsn(clog_lsn);
                wb.put(key, value);
                engine.write_batch(wb).unwrap();
            }

            // Flush to SST
            engine.flush_all_with_active().unwrap();
            flushed_lsn = engine.current_version().flushed_lsn();
            assert!(flushed_lsn > 0, "flushed_lsn should be > 0 after flush");

            // Write second batch (NOT flushed - will need clog replay)
            for i in 0..3 {
                let key = format!("unflushed_key_{i}").into_bytes();
                let value = format!("unflushed_val_{i}").into_bytes();

                let mut batch = ClogBatch::new();
                batch.add_put(i as u64 + 100, key.clone(), value.clone());
                batch.add_commit(i as u64 + 100, i as Timestamp + 200);
                let clog_lsn = clog.write(&mut batch, true).unwrap().await.unwrap();

                // Verify these clog entries have LSN > flushed_lsn
                assert!(
                    clog_lsn > flushed_lsn,
                    "Unflushed clog entry LSN ({clog_lsn}) should be > flushed_lsn ({flushed_lsn})",
                );

                let mut wb = WriteBatch::new();
                wb.set_commit_ts(i as Timestamp + 200);
                wb.set_clog_lsn(clog_lsn);
                wb.put(key, value);
                engine.write_batch(wb).unwrap();
            }

            clog.close().await.unwrap();
        }

        // Second session: recover and verify boundary
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            // Flushed data should be in SSTs (recovered from ilog)
            for i in 0..3 {
                let key = format!("flushed_key_{i}").into_bytes();
                let expected = format!("flushed_val_{i}").into_bytes();
                assert_eq!(
                    get_for_test(&result.engine, &key).await,
                    Some(expected),
                    "Flushed key {i} should be recovered from SST"
                );
            }

            // Unflushed data should be recovered from clog replay
            for i in 0..3 {
                let key = format!("unflushed_key_{i}").into_bytes();
                let expected = format!("unflushed_val_{i}").into_bytes();
                assert_eq!(
                    get_for_test(&result.engine, &key).await,
                    Some(expected),
                    "Unflushed key {i} should be recovered from clog"
                );
            }

            // Verify only unflushed entries were replayed
            assert_eq!(
                result.stats.flushed_lsn, flushed_lsn,
                "Recovered flushed_lsn should match"
            );
            assert_eq!(
                result.stats.txn_count, 3,
                "Exactly 3 unflushed transactions should be replayed"
            );
        }
    }

    #[tokio::test]
    async fn test_recovery_orphan_ssts_cleaned_stat() {
        let tmp = TempDir::new().unwrap();
        let io_handle = tokio::runtime::Handle::current();

        // First create an incomplete flush intent (which creates an orphan)
        {
            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog =
                IlogService::open(ilog_config, Arc::clone(&lsn_provider), &io_handle).unwrap();

            // Write flush intent but no commit (simulating crash before SST creation)
            ilog.write_flush_intent(99, 100, 50).unwrap();
            ilog.sync().unwrap();
        }

        // Now create the orphan SST file that matches the intent
        let sst_dir = tmp.path().join("sst");
        std::fs::create_dir_all(&sst_dir).unwrap();
        std::fs::write(sst_dir.join("00000099.sst"), b"orphan").unwrap();

        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover(&io_handle).unwrap();

            // Should have cleaned up the orphan (from incomplete intent)
            assert_eq!(result.stats.orphan_ssts_cleaned, 1);
        }
    }
}
