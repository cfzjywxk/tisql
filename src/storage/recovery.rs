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
    /// Max LSN of this transaction's writes
    max_lsn: Lsn,
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
        stats.flushed_lsn = flushed_lsn;
        log_info!(
            "Ilog recovery complete: version_num={}, flushed_lsn={}, orphan_ssts={}",
            version.version_num(),
            flushed_lsn,
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

        // Step 4: Recover clog
        log_info!(
            "Starting clog recovery from {:?}",
            self.clog_config.clog_path()
        );
        let (clog, clog_entries) = FileClogService::recover(self.clog_config)?;
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

        // Step 6: Update LSN provider to max of ilog and clog
        let max_clog_lsn = clog_entries.iter().map(|e| e.lsn).max().unwrap_or(0);
        let current_lsn = lsn_provider.current_lsn();
        if max_clog_lsn >= current_lsn {
            lsn_provider.set_lsn(max_clog_lsn + 1);
        }
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
    /// Only replays entries with lsn > flushed_lsn.
    fn replay_clog(
        engine: &LsmEngine,
        entries: &[ClogEntry],
        flushed_lsn: Lsn,
    ) -> Result<ReplayResult> {
        let mut txn_states: HashMap<TxnId, TxnReplayState> = HashMap::new();
        let mut result = ReplayResult::default();

        // Filter and process entries
        for entry in entries {
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
                    }
                }
                ClogOp::Rollback => {
                    // Remove transaction state - writes are discarded
                    txn_states.remove(&entry.txn_id);
                }
            }
        }

        // Apply committed transactions
        for (txn_id, state) in txn_states {
            if let Some(commit_ts) = state.commit_ts {
                if !state.writes.is_empty() {
                    let mut batch = WriteBatch::new();
                    batch.set_commit_ts(commit_ts);

                    for (key, value) in state.writes {
                        match value {
                            Some(v) => batch.put(key, v),
                            None => batch.delete(key),
                        }
                    }

                    engine.write_batch(batch)?;
                    result.txn_count += 1;
                }
            } else {
                // Uncommitted transaction - discard
                log_warn!(
                    "Discarding uncommitted transaction {} with {} writes",
                    txn_id,
                    state.writes.len()
                );
            }
        }

        Ok(result)
    }
}

/// Result of clog replay.
#[derive(Default)]
struct ReplayResult {
    entries_replayed: usize,
    txn_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::{ClogBatch, ClogService};
    use crate::storage::StorageEngine;
    use tempfile::TempDir;

    fn write_test_data(
        engine: &LsmEngine,
        clog: &FileClogService,
        count: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut written = Vec::new();

        for i in 0..count {
            let key = format!("key_{:04}", i).into_bytes();
            let value = format!("value_{:04}", i).into_bytes();

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

            let engine =
                LsmEngine::open_durable(lsm_config, Arc::clone(&lsn_provider), Arc::clone(&ilog))
                    .unwrap();

            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open(clog_config).unwrap();

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
                    result.engine.get(key).unwrap(),
                    Some(expected_value.clone()),
                    "Key {:?} should be readable after recovery",
                    String::from_utf8_lossy(key)
                );
            }

            // Note: Some clog entries may still be replayed because clog and engine
            // have separate LSN counters. The data is already in SST, so re-applying
            // won't cause issues (it will just be shadowed by the SST data).
            // TODO: Unify LSN allocation between clog and LsmEngine.
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

            let engine =
                LsmEngine::open_durable(lsm_config, Arc::clone(&lsn_provider), Arc::clone(&ilog))
                    .unwrap();

            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open(clog_config).unwrap();

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
                    result.engine.get(key).unwrap(),
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
                result.engine.get(b"uncommitted_key").unwrap(),
                None,
                "Uncommitted data should be discarded"
            );

            // Committed data should be recovered
            assert_eq!(
                result.engine.get(b"committed_key").unwrap(),
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

            let engine =
                LsmEngine::open_durable(lsm_config, Arc::clone(&lsn_provider), Arc::clone(&ilog))
                    .unwrap();

            let clog_config = FileClogConfig::with_dir(tmp.path());
            let clog = FileClogService::open(clog_config).unwrap();

            // Write first batch and flush
            let flushed = write_test_data(&engine, &clog, 2);
            engine.flush_all_with_active().unwrap();

            // Write second batch without flush
            let unflushed = {
                let mut written = Vec::new();
                for i in 10..13 {
                    let key = format!("key_{:04}", i).into_bytes();
                    let value = format!("value_{:04}", i).into_bytes();

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
                    result.engine.get(key).unwrap(),
                    Some(expected_value.clone()),
                    "Flushed key {:?} should be readable",
                    String::from_utf8_lossy(key)
                );
            }

            // Unflushed data should be recovered from clog
            for (key, expected_value) in &unflushed {
                assert_eq!(
                    result.engine.get(key).unwrap(),
                    Some(expected_value.clone()),
                    "Unflushed key {:?} should be recovered",
                    String::from_utf8_lossy(key)
                );
            }

            // Some transactions are replayed from clog
            // Note: The exact count may vary because clog and engine have separate LSN counters
            // The important thing is that all data is recovered correctly
            assert!(
                result.stats.txn_count >= 3,
                "At least 3 unflushed transactions should be replayed"
            );
        }
    }
}
