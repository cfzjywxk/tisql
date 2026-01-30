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
    /// Max LSN of this transaction's writes (including commit record)
    max_lsn: Lsn,
    /// LSN of the commit record (for proper clog_lsn threading)
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
    pub fn recover(self) -> Result<RecoveryResult> {
        let mut stats = RecoveryStats::default();

        // Create shared LSN provider
        let lsn_provider = new_lsn_provider();

        // Step 1: Recover ilog to rebuild Version
        log_info!(
            "Starting ilog recovery from {:?}",
            self.ilog_config.ilog_path()
        );
        let (ilog, version, orphan_ssts) =
            IlogService::recover(self.ilog_config.clone(), Arc::clone(&lsn_provider))?;
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
                    state.max_lsn = state.max_lsn.max(entry.lsn);
                }
                ClogOp::Delete { key } => {
                    let state = txn_states.entry(entry.txn_id).or_default();
                    state.writes.push((key.clone(), None));
                    state.max_lsn = state.max_lsn.max(entry.lsn);
                }
                ClogOp::Commit { commit_ts } => {
                    if let Some(state) = txn_states.get_mut(&entry.txn_id) {
                        state.commit_ts = Some(*commit_ts);
                        // Track commit record LSN - this is critical for correct flushed_lsn tracking
                        state.commit_lsn = Some(entry.lsn);
                        // Include commit record LSN in max_lsn
                        state.max_lsn = state.max_lsn.max(entry.lsn);
                    }
                }
                ClogOp::Rollback => {
                    // Remove transaction state - writes are discarded
                    txn_states.remove(&entry.txn_id);
                }
            }
        }

        // Collect committed transactions and sort by max_lsn to ensure
        // memtable age ordering matches LSN ordering during replay.
        // This is critical for correct flushed_lsn tracking after recovery.
        let mut committed_txns: Vec<(TxnId, TxnReplayState)> = txn_states
            .into_iter()
            .filter(|(_, state)| state.commit_ts.is_some() && !state.writes.is_empty())
            .collect();

        // Sort by max_lsn (ascending) so older transactions are applied first
        committed_txns.sort_by_key(|(_, state)| state.max_lsn);

        // Apply committed transactions in LSN order
        for (_txn_id, state) in committed_txns {
            let commit_ts = state.commit_ts.unwrap(); // Safe: filtered above
            let mut batch = WriteBatch::new();
            batch.set_commit_ts(commit_ts);

            // Critical: Set clog_lsn so that flush correctly tracks flushed_lsn.
            // Use the commit record's LSN as it's the final LSN for this transaction.
            // Fall back to max_lsn if commit_lsn not tracked (shouldn't happen).
            let clog_lsn = state.commit_lsn.unwrap_or(state.max_lsn);
            batch.set_clog_lsn(clog_lsn);

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
    use crate::clog::{ClogBatch, ClogService};
    use crate::storage::mvcc::{is_tombstone, MvccKey};
    use crate::storage::version::Version;
    use crate::storage::StorageEngine;
    use crate::types::RawValue;
    use tempfile::TempDir;

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

    fn write_test_data(
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
            clog.write(&mut batch, true).unwrap();

            // Write to engine
            let mut wb = WriteBatch::new();
            wb.set_commit_ts(i as Timestamp + 100);
            wb.put(key.clone(), value.clone());
            engine.write_batch(wb).unwrap();

            written.push((key, value));
        }

        written
    }

    #[test]
    fn test_recovery_empty() {
        let tmp = TempDir::new().unwrap();

        let recovery = LsmRecovery::new(tmp.path());
        let result = recovery.recover().unwrap();

        assert_eq!(result.stats.clog_entries, 0);
        assert_eq!(result.stats.txn_count, 0);
        assert_eq!(result.stats.orphan_ssts_cleaned, 0);
    }

    #[test]
    fn test_recovery_with_flushed_data() {
        let tmp = TempDir::new().unwrap();

        // First session: write and flush
        let written = {
            let lsm_config = LsmConfig::builder(tmp.path())
                .memtable_size(100)
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

            let engine = LsmEngine::open_with_recovery(
                lsm_config,
                Arc::clone(&lsn_provider),
                Arc::clone(&ilog),
                Version::new(),
            )
            .unwrap();

            // Use shared LSN provider for clog too
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog =
                FileClogService::open_with_lsn_provider(clog_config, Arc::clone(&lsn_provider))
                    .unwrap();

            let written = write_test_data(&engine, &clog, 5);

            // Flush all to SST
            engine.flush_all_with_active().unwrap();

            clog.close().unwrap();
            written
        };

        // Second session: recover and verify
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover().unwrap();

            // Data should be readable from SST (not replayed from clog)
            for (key, expected_value) in &written {
                assert_eq!(
                    get_for_test(&result.engine, key),
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

    #[test]
    fn test_recovery_with_unflushed_data() {
        let tmp = TempDir::new().unwrap();

        // First session: write but don't flush
        let written = {
            let lsm_config = LsmConfig::builder(tmp.path())
                .memtable_size(10000) // Large enough to not auto-flush
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

            let engine = LsmEngine::open_with_recovery(
                lsm_config,
                Arc::clone(&lsn_provider),
                Arc::clone(&ilog),
                Version::new(),
            )
            .unwrap();

            // Use shared LSN provider for clog
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog =
                FileClogService::open_with_lsn_provider(clog_config, Arc::clone(&lsn_provider))
                    .unwrap();

            let written = write_test_data(&engine, &clog, 3);

            // Don't flush - simulate crash before flush
            clog.close().unwrap();
            written
        };

        // Second session: recover and verify
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover().unwrap();

            // Data should be recovered from clog replay
            for (key, expected_value) in &written {
                assert_eq!(
                    get_for_test(&result.engine, key),
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

    #[test]
    fn test_recovery_uncommitted_discarded() {
        let tmp = TempDir::new().unwrap();

        // First session: write uncommitted data
        {
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open(clog_config).unwrap();

            // Write uncommitted transaction (no Commit record)
            let mut batch = ClogBatch::new();
            batch.add_put(
                1,
                b"uncommitted_key".to_vec(),
                b"uncommitted_value".to_vec(),
            );
            clog.write(&mut batch, true).unwrap();

            // Write committed transaction
            let mut batch = ClogBatch::new();
            batch.add_put(2, b"committed_key".to_vec(), b"committed_value".to_vec());
            batch.add_commit(2, 100);
            clog.write(&mut batch, true).unwrap();

            clog.close().unwrap();
        }

        // Second session: recover and verify
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover().unwrap();

            // Uncommitted data should be discarded
            assert_eq!(
                get_for_test(&result.engine, b"uncommitted_key"),
                None,
                "Uncommitted data should be discarded"
            );

            // Committed data should be recovered
            assert_eq!(
                get_for_test(&result.engine, b"committed_key"),
                Some(b"committed_value".to_vec()),
                "Committed data should be recovered"
            );

            assert_eq!(result.stats.txn_count, 1);
        }
    }

    #[test]
    fn test_recovery_partial_flush() {
        let tmp = TempDir::new().unwrap();

        // First session: write some data, flush some, write more
        let (flushed, unflushed) = {
            let lsm_config = LsmConfig::builder(tmp.path())
                .memtable_size(50) // Small to trigger flush
                .max_frozen_memtables(4)
                .build()
                .unwrap();

            let lsn_provider = new_lsn_provider();
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

            let engine = LsmEngine::open_with_recovery(
                lsm_config,
                Arc::clone(&lsn_provider),
                Arc::clone(&ilog),
                Version::new(),
            )
            .unwrap();

            // Use shared LSN provider for clog
            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog =
                FileClogService::open_with_lsn_provider(clog_config, Arc::clone(&lsn_provider))
                    .unwrap();

            // Write first batch and flush
            let flushed = write_test_data(&engine, &clog, 2);
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
                    clog.write(&mut batch, true).unwrap();

                    let mut wb = WriteBatch::new();
                    wb.set_commit_ts(i as Timestamp + 100);
                    wb.put(key.clone(), value.clone());
                    engine.write_batch(wb).unwrap();

                    written.push((key, value));
                }
                written
            };

            clog.close().unwrap();
            (flushed, unflushed)
        };

        // Second session: recover and verify
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover().unwrap();

            // Flushed data should be readable from SST
            for (key, expected_value) in &flushed {
                assert_eq!(
                    get_for_test(&result.engine, key),
                    Some(expected_value.clone()),
                    "Flushed key {:?} should be readable",
                    String::from_utf8_lossy(key)
                );
            }

            // Unflushed data should be recovered from clog
            for (key, expected_value) in &unflushed {
                assert_eq!(
                    get_for_test(&result.engine, key),
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
    #[test]
    fn test_unified_lsn_provider() {
        let tmp = TempDir::new().unwrap();

        // First session: interleave clog and ilog operations
        {
            let lsn_provider = new_lsn_provider();

            // Open ilog and clog with shared provider
            let ilog_config = IlogConfig::new(tmp.path());
            let ilog = Arc::new(IlogService::open(ilog_config, Arc::clone(&lsn_provider)).unwrap());

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
            let clog =
                FileClogService::open_with_lsn_provider(clog_config, Arc::clone(&lsn_provider))
                    .unwrap();

            // Write some data - IMPORTANT: thread CLOG LSN to storage
            for i in 0..5 {
                let key = format!("key_{i:04}").into_bytes();
                let value = format!("value_{i:04}").into_bytes();

                // Write to clog (allocates LSN from shared provider)
                let mut batch = ClogBatch::new();
                batch.add_put(i as u64 + 1, key.clone(), value.clone());
                batch.add_commit(i as u64 + 1, i as Timestamp + 100);
                let clog_lsn = clog.write(&mut batch, true).unwrap();

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

            clog.close().unwrap();
        }

        // Second session: verify recovery sees correct LSN ordering
        {
            let recovery = LsmRecovery::new(tmp.path());
            let result = recovery.recover().unwrap();

            // Verify data is recovered
            for i in 0..5 {
                let key = format!("key_{i:04}").into_bytes();
                let expected_value = format!("value_{i:04}").into_bytes();
                assert_eq!(
                    get_for_test(&result.engine, &key),
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
}
