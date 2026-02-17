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

//! Transaction service implementation following OceanBase's unified pattern.
//!
//! All transaction operations go through this service, with transaction
//! state held in `TxnCtx` and passed to each operation.

use std::ops::Range;
use std::sync::Arc;

#[cfg(feature = "failpoints")]
use fail::fail_point;

use crate::clog::{ClogEntry, ClogOp, ClogOpRef, ClogService};
use crate::error::{Result, TiSqlError};
use crate::tso::TsoService;

use super::api::{CommitInfo, MutationType, TxnCtx, TxnService, TxnState};
use super::concurrency::ConcurrencyManager;
use crate::storage::mvcc::{is_tombstone, MvccIterator, MvccKey, LOCK, TOMBSTONE};
use crate::storage::{PessimisticStorage, PessimisticWriteError, StorageEngine, WriteBatch};
use crate::types::{Key, RawValue, Timestamp, TxnId};

/// Transaction service manages transactions with durability and MVCC.
///
/// This is the unified interface for all transaction operations, following
/// OceanBase's `ObTransService` pattern. Transaction state is held in `TxnCtx`
/// and passed to each operation.
///
/// All write operations use pessimistic locking via PessimisticStorage:
/// 1. Write pending nodes to storage (locks acquired immediately)
/// 2. On commit: finalize pending nodes with commit_ts
/// 3. On rollback: abort pending nodes
pub struct TransactionService<S: StorageEngine, L: ClogService, T: TsoService> {
    /// Storage engine for state
    storage: Arc<S>,
    /// Commit log service for durability
    clog_service: Arc<L>,
    /// TSO service for timestamp allocation
    tso: Arc<T>,
    /// ConcurrencyManager for lock table and max_ts tracking
    concurrency_manager: Arc<ConcurrencyManager>,
    /// Compatibility switch for legacy commit path that bypasses reservations.
    ///
    /// Production must keep this disabled.
    allow_legacy_write_ops: bool,
}

/// Ensures commit reservations are released on every exit path.
struct CommitReservationReleaseGuard<'a, S: PessimisticStorage + ?Sized> {
    storage: &'a S,
    txn_start_ts: Timestamp,
    released: bool,
}

impl<'a, S: PessimisticStorage + ?Sized> CommitReservationReleaseGuard<'a, S> {
    fn new(storage: &'a S, txn_start_ts: Timestamp) -> Self {
        Self {
            storage,
            txn_start_ts,
            released: false,
        }
    }

    fn release(mut self) -> Option<u64> {
        let released = self.storage.release_commit_lsn(self.txn_start_ts);
        self.released = true;
        released
    }
}

impl<S: PessimisticStorage + ?Sized> Drop for CommitReservationReleaseGuard<'_, S> {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.storage.release_commit_lsn(self.txn_start_ts);
        }
    }
}

impl<S: StorageEngine, L: ClogService, T: TsoService> TransactionService<S, L, T> {
    /// Create a new transaction service
    pub fn new(
        storage: Arc<S>,
        clog_service: Arc<L>,
        tso: Arc<T>,
        concurrency_manager: Arc<ConcurrencyManager>,
    ) -> Self {
        Self::new_with_policy(storage, clog_service, tso, concurrency_manager, true)
    }

    /// Create a production transaction service that requires reservation path.
    pub fn new_strict(
        storage: Arc<S>,
        clog_service: Arc<L>,
        tso: Arc<T>,
        concurrency_manager: Arc<ConcurrencyManager>,
    ) -> Self {
        Self::new_with_policy(storage, clog_service, tso, concurrency_manager, false)
    }

    fn new_with_policy(
        storage: Arc<S>,
        clog_service: Arc<L>,
        tso: Arc<T>,
        concurrency_manager: Arc<ConcurrencyManager>,
        allow_legacy_write_ops: bool,
    ) -> Self {
        Self {
            storage,
            clog_service,
            tso,
            concurrency_manager,
            allow_legacy_write_ops,
        }
    }

    /// Get the next timestamp from TSO
    fn get_ts(&self) -> Timestamp {
        let ts = self.tso.get_ts();
        // Update max_ts in concurrency manager for MVCC visibility
        self.concurrency_manager.update_max_ts(ts);
        ts
    }

    /// Get reference to storage engine
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Get reference to TSO service
    pub fn tso(&self) -> &T {
        &self.tso
    }

    /// Get the last allocated timestamp without advancing the counter.
    ///
    /// Used for computing the GC safe point when no explicit transactions are active.
    pub fn last_ts(&self) -> Timestamp {
        self.tso.last_ts()
    }

    /// Write a pending value and retry when storage requests slowdown delay.
    ///
    /// This keeps storage trait methods synchronous while ensuring async callers
    /// can apply `tokio::time::sleep` for backpressure hints.
    async fn put_pending_with_retry(
        &self,
        key: &[u8],
        value: RawValue,
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), PessimisticWriteError>
    where
        S: PessimisticStorage,
    {
        // Keep retries bounded so explicit txn writes fail fast instead of hanging
        // indefinitely if background compaction cannot clear slowdown pressure.
        const MAX_SLOWDOWN_RETRIES: usize = 32;
        let mut retries = 0;
        loop {
            match self.storage.put_pending(key, value.clone(), owner_start_ts) {
                Ok(()) => return Ok(()),
                Err(PessimisticWriteError::WriteStall { delay: Some(delay) }) => {
                    if retries >= MAX_SLOWDOWN_RETRIES {
                        return Err(PessimisticWriteError::WriteStall { delay: None });
                    }
                    retries += 1;
                    tokio::time::sleep(delay).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Execute a write batch with durability guarantees (for direct/autocommit writes).
    ///
    /// Recover state by replaying commit log entries.
    ///
    /// This should be called at startup to rebuild the in-memory state
    /// from the durable commit log.
    pub fn recover(&self, entries: &[ClogEntry]) -> Result<RecoveryStats> {
        let mut stats = RecoveryStats::default();
        let mut max_ts: Timestamp = 0;

        // First pass: identify committed transactions, find max commit_ts,
        // and build txn metadata (commit_ts + commit LSN per txn).
        let mut committed_txns = std::collections::HashSet::new();
        let mut txn_commit_ts: std::collections::HashMap<TxnId, Timestamp> =
            std::collections::HashMap::new();
        let mut txn_commit_lsn: std::collections::HashMap<TxnId, u64> =
            std::collections::HashMap::new();

        for entry in entries {
            if let ClogOp::Commit { commit_ts } = entry.op {
                committed_txns.insert(entry.txn_id);
                txn_commit_ts.insert(entry.txn_id, commit_ts);
                txn_commit_lsn.insert(entry.txn_id, entry.lsn);
                max_ts = max_ts.max(commit_ts);
                stats.committed_txns += 1;
            }
        }

        // Second pass: apply committed operations with their commit_ts and LSN.
        for entry in entries {
            if !committed_txns.contains(&entry.txn_id) {
                stats.rolled_back_entries += 1;
                continue;
            }

            let commit_ts = *txn_commit_ts.get(&entry.txn_id).unwrap_or(&max_ts);
            // V2.5 per-txn LSN: all txn entries share the commit record's LSN.
            let lsn = *txn_commit_lsn.get(&entry.txn_id).unwrap_or(&entry.lsn);

            match &entry.op {
                ClogOp::Put { key, value } => {
                    // Create a write batch with commit_ts and clog LSN
                    let mut batch = WriteBatch::new();
                    batch.put(key.clone(), value.clone());
                    batch.set_commit_ts(commit_ts);
                    batch.set_clog_lsn(lsn);
                    self.storage.write_batch(batch)?;
                    stats.applied_puts += 1;
                }
                ClogOp::Delete { key } => {
                    let mut batch = WriteBatch::new();
                    batch.delete(key.clone());
                    batch.set_commit_ts(commit_ts);
                    batch.set_clog_lsn(lsn);
                    self.storage.write_batch(batch)?;
                    stats.applied_deletes += 1;
                }
                ClogOp::Commit { .. } | ClogOp::Rollback => {
                    // Already processed
                }
            }
        }

        // Update TSO to continue from recovered state
        self.tso.set_ts(max_ts + 1);
        self.concurrency_manager.update_max_ts(max_ts);

        Ok(stats)
    }

    /// Get commit log service reference
    pub fn clog_service(&self) -> &L {
        &self.clog_service
    }

    /// Get concurrency manager reference
    pub fn concurrency_manager(&self) -> &ConcurrencyManager {
        &self.concurrency_manager
    }

    /// Check if transaction is in active state
    fn check_active(ctx: &TxnCtx) -> Result<()> {
        match ctx.state {
            TxnState::Running => Ok(()),
            TxnState::Preparing => Err(crate::error::TiSqlError::Internal(
                "Transaction in preparing state".into(),
            )),
            TxnState::Prepared { .. } => Err(crate::error::TiSqlError::Internal(
                "Transaction already in prepared state".into(),
            )),
            TxnState::Committed { .. } => Err(crate::error::TiSqlError::Internal(
                "Transaction already committed".into(),
            )),
            TxnState::Aborted => Err(crate::error::TiSqlError::Internal(
                "Transaction already aborted".into(),
            )),
        }
    }
}

/// Implement TxnService trait for TransactionService
///
/// Note: `S: PessimisticStorage` is required because both implicit and explicit
/// transactions use pessimistic locking with pending nodes in storage.
impl<S: PessimisticStorage + 'static, L: ClogService + 'static, T: TsoService> TxnService
    for TransactionService<S, L, T>
{
    type ScanIter = MvccScanIterator<S::Iter>;

    fn begin(&self, read_only: bool) -> Result<TxnCtx> {
        let txn_id = self.get_ts();
        let start_ts = self.get_ts();
        Ok(TxnCtx::new(txn_id, start_ts, read_only))
    }

    fn begin_explicit(&self, read_only: bool) -> Result<TxnCtx> {
        let txn_id = self.get_ts();
        let start_ts = self.get_ts();
        let mut ctx = TxnCtx::new_explicit(txn_id, start_ts, read_only);
        if !read_only {
            self.concurrency_manager.register_txn(start_ts);
            ctx.registered = true;
        }
        Ok(ctx)
    }

    fn get_txn_state(&self, start_ts: Timestamp) -> Option<TxnState> {
        self.concurrency_manager.get_txn_state(start_ts)
    }

    async fn get(&self, ctx: &TxnCtx, key: &[u8]) -> Result<Option<RawValue>> {
        // Use get_with_owner with owner_ts=start_ts for read-your-writes.
        // This sees own pending writes and committed data at start_ts.
        let value = self
            .storage
            .get_with_owner(key, ctx.start_ts, ctx.start_ts)
            .await;
        Ok(value.filter(|v| !is_tombstone(v)))
    }

    fn scan_iter(&self, ctx: &TxnCtx, range: Range<Key>) -> Result<MvccScanIterator<S::Iter>> {
        // Build MVCC key range:
        // - Start: MvccKey::encode(range.start, MAX) - smallest MVCC key with this prefix
        // - End: MvccKey::encode(range.end, 0) - largest MVCC key for range.end (exclusive)
        let start_mvcc = MvccKey::encode(&range.start, Timestamp::MAX);
        let end_mvcc = if range.end.is_empty() {
            MvccKey::unbounded()
        } else {
            MvccKey::encode(&range.end, 0)
        };
        let mvcc_range = start_mvcc..end_mvcc;

        // Explicit transactions use owner_ts=start_ts for read-your-writes
        // (see pending writes from prior statements in the same txn).
        // Implicit transactions use owner_ts=0 because writes within a single
        // statement must not be visible to the ongoing scan (e.g., UPDATE SET a=a+10
        // must not re-scan rows it just inserted). Pending resolution via
        // concurrency_manager handles other txns' pending writes for both paths.
        let owner_ts = if ctx.is_explicit() { ctx.start_ts } else { 0 };
        let storage_iter = self.storage.scan_iter(mvcc_range, owner_ts)?;

        // Always pass concurrency_manager for pending resolution of other txns.
        let cm = Some(Arc::clone(&self.concurrency_manager));
        Ok(MvccScanIterator::new(storage_iter, ctx.start_ts, range, cm))
    }

    async fn put(&self, ctx: &mut TxnCtx, key: Key, value: RawValue) -> Result<()> {
        if ctx.state != TxnState::Running {
            return Err(TiSqlError::TransactionNotActive(format!("{:?}", ctx.state)));
        }
        if ctx.read_only {
            return Err(TiSqlError::ReadOnlyTransaction);
        }

        // Register in state cache BEFORE writing pending node so concurrent readers
        // can always resolve our pending writes via TxnStateCache. Without this,
        // there's a gap between put_pending and register where get_txn_state returns
        // None — currently safe due to max_ts, but fragile to reason about.
        if !ctx.registered {
            self.concurrency_manager.register_txn(ctx.start_ts);
            ctx.registered = true;
        }

        // Write pending node directly to storage.
        match self.put_pending_with_retry(&key, value, ctx.start_ts).await {
            Ok(()) => {
                // Single insert handles both: adding a new Write entry AND
                // upgrading Lock→Write (overwrite) if key had a prior LOCK.
                ctx.mutations.insert(key, MutationType::Write);
                Ok(())
            }
            Err(PessimisticWriteError::LockConflict(lock_owner)) => Err(TiSqlError::KeyIsLocked {
                key,
                lock_ts: lock_owner,
                primary: vec![],
            }),
            Err(PessimisticWriteError::WriteStall { delay: None }) => Err(TiSqlError::Storage(
                "Write stalled: storage backpressure, retry later".to_string(),
            )),
            Err(PessimisticWriteError::WriteStall { delay: Some(_) }) => {
                unreachable!("put_pending_with_retry returns only LockConflict or hard WriteStall")
            }
        }
    }

    async fn delete(&self, ctx: &mut TxnCtx, key: Key) -> Result<()> {
        if ctx.state != TxnState::Running {
            return Err(TiSqlError::TransactionNotActive(format!("{:?}", ctx.state)));
        }
        if ctx.read_only {
            return Err(TiSqlError::ReadOnlyTransaction);
        }

        // Register before writing pending (same reason as put()).
        if !ctx.registered {
            self.concurrency_manager.register_txn(ctx.start_ts);
            ctx.registered = true;
        }

        // Check committed OR own pending value (owner_ts=start_ts sees own pending).
        // This fixes put(k)+delete(k) on non-existing key: sees own pending put → TOMBSTONE.
        let value_exists = self
            .storage
            .get_with_owner(&key, ctx.start_ts, ctx.start_ts)
            .await
            .is_some();

        if value_exists {
            // Value exists (committed or own pending) → TOMBSTONE
            match self
                .put_pending_with_retry(&key, TOMBSTONE.to_vec(), ctx.start_ts)
                .await
            {
                Ok(()) => {
                    ctx.mutations.insert(key, MutationType::Write);
                    Ok(())
                }
                Err(PessimisticWriteError::LockConflict(lock_owner)) => {
                    Err(TiSqlError::KeyIsLocked {
                        key,
                        lock_ts: lock_owner,
                        primary: vec![],
                    })
                }
                Err(PessimisticWriteError::WriteStall { delay: None }) => Err(TiSqlError::Storage(
                    "Write stalled: storage backpressure, retry later".into(),
                )),
                Err(PessimisticWriteError::WriteStall { delay: Some(_) }) => unreachable!(
                    "put_pending_with_retry returns only LockConflict or hard WriteStall"
                ),
            }
        } else {
            // No value exists → LOCK (pessimistic lock only, not persisted to clog)
            match self
                .put_pending_with_retry(&key, LOCK.to_vec(), ctx.start_ts)
                .await
            {
                Ok(()) => {
                    // or_insert preserves existing Write if key was previously put
                    // (won't downgrade Write→Lock).
                    ctx.mutations.entry(key).or_insert(MutationType::Lock);
                    Ok(())
                }
                Err(PessimisticWriteError::LockConflict(lock_owner)) => {
                    Err(TiSqlError::KeyIsLocked {
                        key,
                        lock_ts: lock_owner,
                        primary: vec![],
                    })
                }
                Err(PessimisticWriteError::WriteStall { delay: None }) => Err(TiSqlError::Storage(
                    "Write stalled: storage backpressure, retry later".into(),
                )),
                Err(PessimisticWriteError::WriteStall { delay: Some(_) }) => unreachable!(
                    "put_pending_with_retry returns only LockConflict or hard WriteStall"
                ),
            }
        }
    }

    async fn commit(&self, mut ctx: TxnCtx) -> Result<CommitInfo> {
        Self::check_active(&ctx)?;

        let txn_id = ctx.txn_id;
        let start_ts = ctx.start_ts;

        let mutations = std::mem::take(&mut ctx.mutations);

        // No-write transaction: clean up and return
        if mutations.is_empty() {
            if ctx.registered {
                self.concurrency_manager.remove_txn(start_ts);
            }
            ctx.state = TxnState::Committed {
                commit_ts: start_ts,
            };
            return Ok(CommitInfo {
                txn_id,
                commit_ts: start_ts,
                lsn: 0,
            });
        }

        // Read values for Write keys from storage. Keys are borrowed from
        // the mutations BTreeMap; values are owned (read from storage).
        // Lock keys are skipped — they have no data to persist.
        let mut read_values: Vec<(&[u8], Option<RawValue>)> = Vec::new();
        for (key, mt) in &mutations {
            if *mt == MutationType::Lock {
                continue;
            }
            // TOMBSTONE: MemTableEngine returns Some(TOMBSTONE),
            // LsmEngine returns None. Both handled as Delete in clog.
            match self.storage.get_with_owner(key, start_ts, start_ts).await {
                Some(value) if !is_tombstone(&value) => {
                    read_values.push((key.as_slice(), Some(value)));
                }
                _ => {
                    read_values.push((key.as_slice(), None));
                }
            }
        }

        // FAILPOINT: After locks acquired (pending nodes written), before commit_ts computation
        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_lock_before_commit_ts");

        // Step 1: Signal that we're computing commit_ts.
        // Readers encountering our pending nodes will spin-wait until Prepared.
        self.concurrency_manager.set_preparing_txn(start_ts)?;

        // Step 2: Compute commit_ts (safe: readers spin on Preparing)
        let max_ts = self.concurrency_manager.max_ts();
        let tso_ts = self.get_ts();
        let commit_ts = std::cmp::max(max_ts + 1, tso_ts);

        // Step 3: Set prepared_ts for readers to check visibility
        self.concurrency_manager.prepare_txn(start_ts, commit_ts)?;

        // Step 4: Build ClogOpRef entries from read values (zero key clone —
        // keys borrow from mutations BTreeMap, values borrow from read_values).
        let ops: Vec<ClogOpRef<'_>> = read_values
            .iter()
            .map(|(key, val)| match val {
                Some(v) => ClogOpRef::Put {
                    key,
                    value: v.as_slice(),
                },
                None => ClogOpRef::Delete { key },
            })
            .collect();

        // V2.6: reserve commit LSN ahead of WAL write when storage supports it.
        ctx.reserved_lsn = self.storage.alloc_and_reserve_commit_lsn(start_ts);
        let mut reservation_guard = ctx
            .reserved_lsn
            .map(|_| CommitReservationReleaseGuard::new(self.storage.as_ref(), start_ts));

        #[cfg(feature = "failpoints")]
        if ctx.reserved_lsn.is_some() {
            fail_point!("txn_after_alloc_and_reserve_before_clog_write_v26");
        }

        // Step 5: Write to clog for durability (serializes synchronously, returns
        // future for fsync). After this call, ops/read_values are no longer needed.
        let clog_result = if let Some(txn_lsn) = ctx.reserved_lsn {
            debug_assert!(
                self.storage.is_commit_lsn_reserved(start_ts, txn_lsn),
                "reserved commit lsn missing before clog write: start_ts={}, lsn={}",
                start_ts,
                txn_lsn
            );
            self.clog_service
                .write_ops_with_lsn(txn_id, &ops, commit_ts, txn_lsn, true)
        } else {
            if self.allow_legacy_write_ops {
                tracing::warn!(
                    txn_id,
                    start_ts,
                    "using legacy clog write_ops path without reservation; test compatibility mode"
                );
                self.clog_service.write_ops(txn_id, &ops, commit_ts, true)
            } else {
                Err(TiSqlError::Internal(
                    "storage does not support commit LSN reservation path".into(),
                ))
            }
        };

        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_clog_write_before_fsync_wait_v26");

        // Release borrows on mutations (ops → read_values → mutations).
        drop(ops);
        drop(read_values);

        match clog_result {
            Ok(fsync_future) => match fsync_future.await {
                Ok(lsn) => {
                    #[cfg(feature = "failpoints")]
                    fail_point!("txn_after_clog_fsync_before_finalize_v26");

                    let commit_lsn = if let Some(reserved_lsn) = ctx.reserved_lsn {
                        debug_assert_eq!(
                            reserved_lsn, lsn,
                            "clog returned lsn differs from reserved lsn (start_ts={})",
                            start_ts
                        );
                        reserved_lsn
                    } else {
                        lsn
                    };

                    // Step 6: Finalize all pending nodes by setting commit_ts and
                    // tracking the clog LSN on touched memtables.
                    let keys: Vec<Key> = mutations.into_keys().collect();
                    self.storage
                        .finalize_pending_with_lsn(&keys, start_ts, commit_ts, commit_lsn);

                    #[cfg(feature = "failpoints")]
                    fail_point!("txn_after_finalize_before_reservation_release_v26");

                    if let Some(guard) = reservation_guard.take() {
                        let _ = guard.release();
                    }
                    ctx.reserved_lsn = None;

                    #[cfg(feature = "failpoints")]
                    fail_point!("txn_after_reservation_release_before_state_commit_v26");

                    // Step 7: Transition to Committed in state cache
                    self.concurrency_manager.commit_txn(start_ts, commit_ts)?;
                    self.concurrency_manager.remove_txn(start_ts);
                    ctx.state = TxnState::Committed { commit_ts };
                    Ok(CommitInfo {
                        txn_id,
                        commit_ts,
                        lsn: commit_lsn,
                    })
                }
                Err(e) => {
                    if let Some(guard) = reservation_guard.take() {
                        let _ = guard.release();
                    }
                    ctx.reserved_lsn = None;
                    let keys: Vec<Key> = mutations.into_keys().collect();
                    self.storage.abort_pending(&keys, start_ts);
                    self.concurrency_manager.abort_txn(start_ts).ok();
                    self.concurrency_manager.remove_txn(start_ts);
                    Err(e)
                }
            },
            Err(e) => {
                if let Some(guard) = reservation_guard.take() {
                    let _ = guard.release();
                }
                ctx.reserved_lsn = None;
                let keys: Vec<Key> = mutations.into_keys().collect();
                self.storage.abort_pending(&keys, start_ts);
                self.concurrency_manager.abort_txn(start_ts).ok();
                self.concurrency_manager.remove_txn(start_ts);
                Err(e)
            }
        }
    }

    fn rollback(&self, mut ctx: TxnCtx) -> Result<()> {
        Self::check_active(&ctx)?;

        let mutations = std::mem::take(&mut ctx.mutations);

        // Abort all pending nodes in storage
        if !mutations.is_empty() {
            let keys: Vec<Key> = mutations.into_keys().collect();
            self.storage.abort_pending(&keys, ctx.start_ts);
        }

        if ctx.reserved_lsn.take().is_some() {
            let _ = self.storage.release_commit_lsn(ctx.start_ts);
        }

        // Update state cache
        if ctx.registered {
            self.concurrency_manager.abort_txn(ctx.start_ts)?;
            self.concurrency_manager.remove_txn(ctx.start_ts);
        }

        ctx.state = TxnState::Aborted;
        Ok(())
    }
}

// ============================================================================
// MvccScanIterator - Transaction layer wrapper for storage iterator
// ============================================================================

/// Streaming MVCC scan iterator wrapping the storage iterator.
///
/// This is a thin wrapper that provides MVCC visibility filtering:
/// - Filters entries by `read_ts` (only entries with `ts <= read_ts` are visible)
/// - Filters out tombstones (deleted keys)
/// - Skips older versions of the same key (returns only latest visible version)
///
/// ## Iterator Pattern
///
/// Construction does NOT position the iterator. Call `advance()` first:
///
/// ```ignore
/// let mut iter = scan_iter(ctx, range)?;
/// iter.advance()?;  // Position on first entry
/// while iter.valid() {
///     let key = iter.user_key();
///     let value = iter.value();
///     iter.advance()?;
/// }
/// ```
///
/// ## Error Handling
///
/// Storage errors (I/O, corruption) are propagated through `advance()`.
/// After an error, `valid()` returns false.
///
/// ## Zero-Copy Design
///
/// Uses lazy skipping to avoid value cloning: after finding a visible entry,
/// the storage iterator stays positioned on it. Older versions are skipped
/// at the start of the next `advance()` call. This allows `user_key()`,
/// `timestamp()`, and `value()` to return references directly from storage.
pub struct MvccScanIterator<I: MvccIterator> {
    /// Storage iterator producing MVCC key-value pairs
    storage_iter: I,
    /// Transaction's read timestamp for MVCC filtering
    read_ts: Timestamp,
    /// Key range for filtering
    range: Range<Key>,
    /// Key to skip on next advance (older versions of last returned key).
    /// Also used for tombstone skipping within the same advance call.
    last_returned_key: Option<Vec<u8>>,
    /// Whether positioned on a valid entry (storage_iter points to it)
    is_positioned: bool,
    /// ConcurrencyManager for resolving pending writes from other transactions.
    /// None for explicit transactions (they use read-your-writes directly).
    concurrency_manager: Option<Arc<ConcurrencyManager>>,
}

/// Result of resolving a pending write via TxnStateCache.
enum PendingResolution {
    /// Skip this pending node (writer is Running, Aborted, or committed with ts > read_ts)
    Skip,
    /// The pending node is now committed with ts <= read_ts; treat as visible
    Visible,
}

impl<I: MvccIterator> MvccScanIterator<I> {
    /// Create a new scan iterator.
    ///
    /// The iterator is NOT positioned after construction. Call `advance()`
    /// to position on the first entry.
    ///
    /// # Arguments
    ///
    /// * `storage_iter` - Iterator over storage (MVCC key-value pairs)
    /// * `read_ts` - Transaction's read timestamp for MVCC visibility
    /// * `range` - Key range for filtering
    pub fn new(
        storage_iter: I,
        read_ts: Timestamp,
        range: Range<Key>,
        concurrency_manager: Option<Arc<ConcurrencyManager>>,
    ) -> Self {
        Self {
            storage_iter,
            read_ts,
            range,
            last_returned_key: None,
            is_positioned: false,
            concurrency_manager,
        }
    }

    /// Resolve a pending (uncommitted) write by consulting TxnStateCache.
    ///
    /// This determines whether the pending node should be skipped or is visible
    /// based on the owning transaction's current state.
    fn resolve_pending(&self, owner_ts: Timestamp) -> crate::error::Result<PendingResolution> {
        let cm = match &self.concurrency_manager {
            Some(cm) => cm,
            None => {
                // No concurrency manager (explicit txn path) - skip pending
                return Ok(PendingResolution::Skip);
            }
        };

        const MAX_SPIN: u32 = 40;
        for _ in 0..MAX_SPIN {
            match cm.get_txn_state(owner_ts) {
                Some(TxnState::Running) => {
                    // Writer is still running. Safe to skip because:
                    // Writer will see max_ts >= reader.start_ts when it computes commit_ts,
                    // so commit_ts > reader.start_ts is guaranteed.
                    return Ok(PendingResolution::Skip);
                }
                Some(TxnState::Preparing) => {
                    // Writer is computing commit_ts. Spin-wait until it transitions
                    // to Prepared (where we can check the actual commit_ts).
                    std::thread::yield_now();
                    continue;
                }
                Some(TxnState::Prepared { prepared_ts }) => {
                    if prepared_ts > self.read_ts {
                        // commit_ts > read_ts, not visible to us
                        return Ok(PendingResolution::Skip);
                    }
                    // prepared_ts <= read_ts: we can't determine visibility yet.
                    // The writer may commit with this ts, making it visible.
                    // Return KeyIsLocked to force retry.
                    return Err(TiSqlError::KeyIsLocked {
                        key: self.storage_iter.user_key().to_vec(),
                        lock_ts: owner_ts,
                        primary: vec![],
                    });
                }
                Some(TxnState::Committed { commit_ts }) => {
                    if commit_ts > self.read_ts {
                        return Ok(PendingResolution::Skip);
                    }
                    // Committed with commit_ts <= read_ts: visible!
                    return Ok(PendingResolution::Visible);
                }
                Some(TxnState::Aborted) | None => {
                    // Transaction aborted or already cleaned up - skip
                    return Ok(PendingResolution::Skip);
                }
            }
        }

        // Exceeded spin limit - return KeyIsLocked
        Err(TiSqlError::KeyIsLocked {
            key: self.storage_iter.user_key().to_vec(),
            lock_ts: owner_ts,
            primary: vec![],
        })
    }

    /// Find the next visible entry from storage.
    ///
    /// Uses lazy skipping: older versions of the previously returned key are
    /// skipped at the start of this call (not at the end of the previous call).
    /// This allows `user_key()`, `timestamp()`, and `value()` to return
    /// references directly from the storage iterator without caching.
    ///
    /// After finding a visible entry, the storage iterator stays positioned on it.
    /// The key is recorded in `last_returned_key` for skipping on the next call.
    async fn find_next_visible(&mut self) -> crate::error::Result<()> {
        // Lazy skip: skip older versions of the previously returned key
        if let Some(ref skip_key) = self.last_returned_key {
            while self.storage_iter.valid() && self.storage_iter.user_key() == skip_key.as_slice() {
                self.storage_iter.advance().await?;
            }
        }
        self.last_returned_key = None;
        self.is_positioned = false;

        // Initialize storage iterator if needed
        if !self.storage_iter.valid() {
            self.storage_iter.advance().await?;
        }

        while self.storage_iter.valid() {
            let user_key = self.storage_iter.user_key();
            let ts = self.storage_iter.timestamp();

            // Check if past end of range
            if !self.range.end.is_empty() && user_key >= self.range.end.as_slice() {
                return Ok(()); // is_positioned stays false
            }

            // Skip if before start of range
            if user_key < self.range.start.as_slice() {
                self.storage_iter.advance().await?;
                continue;
            }

            // Resolve pending writes before visibility check
            if self.storage_iter.is_pending() {
                match self.resolve_pending(self.storage_iter.pending_owner())? {
                    PendingResolution::Skip => {
                        self.storage_iter.advance().await?;
                        continue;
                    }
                    PendingResolution::Visible => {
                        // Committed with commit_ts <= read_ts.
                        // Check tombstone before returning.
                        if is_tombstone(self.storage_iter.value()) {
                            let skip_key = user_key.to_vec();
                            self.storage_iter.advance().await?;
                            while self.storage_iter.valid()
                                && self.storage_iter.user_key() == skip_key.as_slice()
                            {
                                self.storage_iter.advance().await?;
                            }
                            continue;
                        }
                        self.last_returned_key = Some(user_key.to_vec());
                        self.is_positioned = true;
                        return Ok(());
                    }
                }
            }

            // Skip if not visible at read_ts
            if ts > self.read_ts {
                self.storage_iter.advance().await?;
                continue;
            }

            // Found a visible entry - check if tombstone
            if is_tombstone(self.storage_iter.value()) {
                // Tombstone: skip this key and all older versions immediately
                // (we don't return tombstones, so no lazy skip needed)
                let skip_key = user_key.to_vec();
                self.storage_iter.advance().await?;
                while self.storage_iter.valid()
                    && self.storage_iter.user_key() == skip_key.as_slice()
                {
                    self.storage_iter.advance().await?;
                }
                continue;
            }

            // Found a visible, non-tombstone entry
            // Record key for lazy skip on next advance, keep iterator positioned here
            self.last_returned_key = Some(user_key.to_vec());
            self.is_positioned = true;
            return Ok(());
        }

        Ok(()) // is_positioned stays false - exhausted
    }
}

impl<I: MvccIterator> MvccIterator for MvccScanIterator<I> {
    async fn seek(&mut self, _target: &MvccKey) -> crate::error::Result<()> {
        // MvccScanIterator is created for a specific range.
        // Seeking within the result is not supported - this is a forward-only iterator.
        // The iterator should be created with the appropriate range instead.
        Ok(())
    }

    async fn advance(&mut self) -> crate::error::Result<()> {
        self.find_next_visible().await
    }

    fn valid(&self) -> bool {
        self.is_positioned
    }

    fn user_key(&self) -> &[u8] {
        debug_assert!(self.is_positioned, "user_key() called on invalid iterator");
        self.storage_iter.user_key()
    }

    fn timestamp(&self) -> Timestamp {
        debug_assert!(self.is_positioned, "timestamp() called on invalid iterator");
        self.storage_iter.timestamp()
    }

    fn value(&self) -> &[u8] {
        debug_assert!(self.is_positioned, "value() called on invalid iterator");
        self.storage_iter.value()
    }
}

/// Statistics from recovery
#[derive(Default, Debug)]
pub struct RecoveryStats {
    /// Number of committed transactions
    pub committed_txns: u64,
    /// Number of put operations applied
    pub applied_puts: u64,
    /// Number of delete operations applied
    pub applied_deletes: u64,
    /// Number of entries from uncommitted transactions (rolled back)
    pub rolled_back_entries: u64,
    /// Reserved for future recovery-shape validation diagnostics.
    /// Under per-transaction LSN, this is currently expected to stay 0.
    pub post_commit_ops: u64,
}

impl RecoveryStats {
    /// Total operations applied
    pub fn total_applied(&self) -> u64 {
        self.applied_puts + self.applied_deletes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::{FileClogConfig, FileClogService};
    use crate::storage::MemTableEngine;
    use crate::tso::LocalTso;
    use tempfile::tempdir;

    type TestStorage = MemTableEngine;

    fn create_test_service() -> (
        Arc<TestStorage>,
        TransactionService<TestStorage, FileClogService, LocalTso>,
        tempfile::TempDir,
    ) {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let io_handle = tokio::runtime::Handle::current();
        let clog_service = Arc::new(FileClogService::open(config, &io_handle).unwrap());
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);
        (storage, txn_service, dir)
    }

    fn create_strict_test_service() -> (
        Arc<TestStorage>,
        TransactionService<TestStorage, FileClogService, LocalTso>,
        tempfile::TempDir,
    ) {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let io_handle = tokio::runtime::Handle::current();
        let clog_service = Arc::new(FileClogService::open(config, &io_handle).unwrap());
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());
        let txn_service =
            TransactionService::new_strict(Arc::clone(&storage), clog_service, tso, cm);
        (storage, txn_service, dir)
    }

    // ========================================================================
    // Test Helpers Using MvccKey
    // ========================================================================

    async fn get_at_for_test<S: StorageEngine>(
        storage: &S,
        key: &[u8],
        ts: Timestamp,
    ) -> Option<RawValue> {
        let seek_key = MvccKey::encode(key, ts);
        let end_key = MvccKey::encode(key, 0)
            .next_key()
            .unwrap_or_else(MvccKey::unbounded);
        let range = seek_key..end_key;

        let mut iter = storage.scan_iter(range, 0).unwrap();
        iter.advance().await.unwrap(); // Position on first entry

        while iter.valid() {
            let decoded_key = iter.user_key();
            let entry_ts = iter.timestamp();
            let value = iter.value();
            if decoded_key == key && entry_ts <= ts {
                if is_tombstone(value) {
                    return None;
                }
                return Some(value.to_vec());
            }
            iter.advance().await.unwrap();
        }
        None
    }

    async fn get_for_test<S: StorageEngine>(storage: &S, key: &[u8]) -> Option<RawValue> {
        get_at_for_test(storage, key, Timestamp::MAX).await
    }

    // Import test extension trait for autocommit helpers
    use crate::testkit::TxnServiceTestExt;

    #[tokio::test]
    async fn test_autocommit_put() {
        let (storage, txn_service, _dir) = create_test_service();

        // Execute writes using proper transaction flow
        let info = txn_service
            .autocommit_put(b"key1", b"value1")
            .await
            .unwrap();
        assert!(info.txn_id > 0);
        assert!(info.commit_ts > 0);
        assert!(info.lsn > 0);

        txn_service
            .autocommit_put(b"key2", b"value2")
            .await
            .unwrap();

        // Verify storage
        let v1 = get_for_test(&*storage, b"key1").await;
        assert_eq!(v1, Some(b"value1".to_vec()));
        let v2 = get_for_test(&*storage, b"key2").await;
        assert_eq!(v2, Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_strict_mode_rejects_storage_without_reservations() {
        let (_storage, txn_service, _dir) = create_strict_test_service();

        let mut ctx = txn_service.begin(false).unwrap();
        txn_service
            .put(&mut ctx, b"strict_key".to_vec(), b"strict_value".to_vec())
            .await
            .unwrap();

        let err = txn_service.commit(ctx).await.unwrap_err();
        match err {
            TiSqlError::Internal(msg) => {
                assert!(
                    msg.contains("reservation"),
                    "unexpected strict mode error message: {msg}"
                );
            }
            other => panic!("expected strict mode Internal error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_recover() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let io_handle = tokio::runtime::Handle::current();

        // Write some data
        {
            let clog_service = Arc::new(FileClogService::open(config.clone(), &io_handle).unwrap());
            let tso = Arc::new(LocalTso::new(1));
            let cm = Arc::new(ConcurrencyManager::new(0));
            let storage = Arc::new(MemTableEngine::new());
            let txn_service = TransactionService::new(storage, clog_service.clone(), tso, cm);

            txn_service.autocommit_put(b"k1", b"v1").await.unwrap();
            txn_service.autocommit_put(b"k2", b"v2").await.unwrap();
            txn_service.autocommit_delete(b"k1").await.unwrap();

            clog_service.close().await.unwrap();
        }

        // Recover
        let (clog_service, entries) = FileClogService::recover(config, &io_handle).unwrap();
        let clog_service = Arc::new(clog_service);
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);

        let stats = txn_service.recover(&entries).unwrap();
        assert_eq!(stats.committed_txns, 3);
        assert_eq!(stats.applied_puts, 2);
        assert_eq!(stats.applied_deletes, 1);

        // Verify recovered state
        assert!(get_for_test(&*storage, b"k1").await.is_none()); // Was deleted
        assert_eq!(get_for_test(&*storage, b"k2").await, Some(b"v2".to_vec()));

        // Verify TSO advanced (use tso().last_ts() to check state)
        assert!(txn_service.tso().last_ts() > 3);
    }

    #[tokio::test]
    async fn test_mvcc_versions_after_writes() {
        let (storage, txn_service, _dir) = create_test_service();

        // Write v1
        let info1 = txn_service.autocommit_put(b"key", b"v1").await.unwrap();
        let ts1 = info1.commit_ts;

        // Write v2
        let info2 = txn_service.autocommit_put(b"key", b"v2").await.unwrap();
        let ts2 = info2.commit_ts;

        // Reading at latest should see v2
        let v = get_for_test(&*storage, b"key").await;
        assert_eq!(v, Some(b"v2".to_vec()));

        // Reading at ts1 should see v1 (MVCC visibility)
        let v = get_at_for_test(&*storage, b"key", ts1).await;
        assert_eq!(v, Some(b"v1".to_vec()));

        // Reading at ts2 should see v2
        let v = get_at_for_test(&*storage, b"key", ts2).await;
        assert_eq!(v, Some(b"v2".to_vec()));
    }

    // ========================================================================
    // TxnService trait tests (new unified API)
    // ========================================================================

    #[tokio::test]
    async fn test_txn_service_begin() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Begin a read-write transaction
        let ctx = txn_service.begin(false).unwrap();

        // Transaction should have valid timestamp
        assert!(ctx.start_ts() > 0);
        assert!(ctx.is_valid());
        assert!(!ctx.is_read_only());
    }

    #[tokio::test]
    async fn test_txn_service_begin_read_only() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Begin a read-only transaction
        let ctx = txn_service.begin(true).unwrap();

        assert!(ctx.start_ts() > 0);
        assert!(ctx.is_valid());
        assert!(ctx.is_read_only());
    }

    #[tokio::test]
    async fn test_txn_service_put_commit() {
        let (storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin(false).unwrap();

        // Put via transaction — writes pending node directly to storage
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();

        // Commit
        let info = txn_service.commit(ctx).await.unwrap();
        assert!(info.commit_ts > 0);
        assert!(info.lsn > 0);

        // Now visible in storage
        let storage_value = get_for_test(&*storage, b"key1").await;
        assert_eq!(storage_value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_txn_service_delete_commit() {
        let (storage, txn_service, _dir) = create_test_service();

        // First commit a value
        txn_service
            .autocommit_put(b"key1", b"value1")
            .await
            .unwrap();
        assert_eq!(
            get_for_test(&*storage, b"key1").await,
            Some(b"value1".to_vec())
        );

        // Delete via transaction
        let mut ctx = txn_service.begin(false).unwrap();
        txn_service
            .delete(&mut ctx, b"key1".to_vec())
            .await
            .unwrap();
        txn_service.commit(ctx).await.unwrap();

        // Should be deleted in storage
        assert!(get_for_test(&*storage, b"key1").await.is_none());
    }

    #[tokio::test]
    async fn test_txn_service_rollback() {
        let (storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin(false).unwrap();

        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();

        // Rollback
        txn_service.rollback(ctx).unwrap();

        // Data should NOT be in storage
        let value = get_for_test(&*storage, b"key1").await;
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_txn_service_snapshot_isolation() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write initial data
        txn_service.autocommit_put(b"key", b"v1").await.unwrap();

        // Begin transaction
        let ctx = txn_service.begin(true).unwrap();

        // Transaction should see v1
        let value = txn_service.get(&ctx, b"key").await.unwrap();
        assert_eq!(value, Some(b"v1".to_vec()));

        // Write v2 via autocommit (outside the transaction)
        txn_service.autocommit_put(b"key", b"v2").await.unwrap();

        // Transaction should still see v1 (snapshot isolation)
        let value = txn_service.get(&ctx, b"key").await.unwrap();
        assert_eq!(value, Some(b"v1".to_vec()));

        // But reading via storage should see v2
        let latest = get_for_test(txn_service.storage(), b"key").await;
        assert_eq!(latest, Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_txn_service_read_only_rejects_writes() {
        let (storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin(true).unwrap();

        // Put should error for read-only transaction
        let result = txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::ReadOnlyTransaction
        ));

        // Delete should also error
        let result = txn_service.delete(&mut ctx, b"key1".to_vec()).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::ReadOnlyTransaction
        ));

        // Commit should succeed (no-op for read-only)
        let info = txn_service.commit(ctx).await.unwrap();
        assert_eq!(info.lsn, 0); // No log written

        // Nothing in storage
        let value = get_for_test(&*storage, b"key1").await;
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_txn_service_scan_mvcc() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write some data via autocommit
        txn_service.autocommit_put(b"a", b"1").await.unwrap();
        txn_service.autocommit_put(b"b", b"2").await.unwrap();
        txn_service.autocommit_put(b"c", b"3").await.unwrap();

        // Begin a read-only transaction
        let ctx = txn_service.begin(true).unwrap();

        // Scan should see all data
        let mut iter = txn_service
            .scan_iter(&ctx, b"a".to_vec()..b"d".to_vec())
            .unwrap();
        let mut results = Vec::new();
        iter.advance().await.unwrap();
        while iter.valid() {
            results.push((iter.user_key().to_vec(), iter.value().to_vec()));
            iter.advance().await.unwrap();
        }

        assert_eq!(
            results.len(),
            3,
            "scan should see 3 keys, start_ts={}, got {:?}",
            ctx.start_ts(),
            results
        );
    }

    #[tokio::test]
    async fn test_scan_storage_only() {
        // Test: Storage has data, scan returns it
        let (_storage, txn_service, _dir) = create_test_service();

        // Commit some data first
        txn_service.autocommit_put(b"m", b"m_value").await.unwrap();
        txn_service.autocommit_put(b"n", b"n_value").await.unwrap();
        txn_service.autocommit_put(b"o", b"o_value").await.unwrap();

        // Begin a read-only transaction
        let ctx = txn_service.begin(true).unwrap();

        // Scan should return storage data
        let mut iter = txn_service
            .scan_iter(&ctx, b"m".to_vec()..b"p".to_vec())
            .unwrap();
        let mut results = Vec::new();
        iter.advance().await.unwrap();
        while iter.valid() {
            results.push((iter.user_key().to_vec(), iter.value().to_vec()));
            iter.advance().await.unwrap();
        }

        assert_eq!(results.len(), 3, "should see all 3 storage keys");
        assert_eq!(results[0], (b"m".to_vec(), b"m_value".to_vec()));
        assert_eq!(results[1], (b"n".to_vec(), b"n_value".to_vec()));
        assert_eq!(results[2], (b"o".to_vec(), b"o_value".to_vec()));
    }

    #[tokio::test]
    async fn test_scan_empty_range() {
        // Test: Empty range returns nothing
        let (_storage, txn_service, _dir) = create_test_service();

        // Commit data outside range
        txn_service.autocommit_put(b"aaa", b"before").await.unwrap();
        txn_service.autocommit_put(b"zzz", b"after").await.unwrap();

        let ctx = txn_service.begin(true).unwrap();

        // Scan a range that has no data
        let mut iter = txn_service
            .scan_iter(&ctx, b"mmm".to_vec()..b"nnn".to_vec())
            .unwrap();
        iter.advance().await.unwrap();
        assert!(!iter.valid(), "should be empty - no data in range");
    }

    #[tokio::test]
    async fn test_scan_range_boundaries() {
        // Test: Keys exactly at range boundaries
        let (_storage, txn_service, _dir) = create_test_service();

        // Storage: "aaa", "bbb", "ccc" (lexicographically ordered)
        txn_service.autocommit_put(b"aaa", b"a_val").await.unwrap();
        txn_service.autocommit_put(b"bbb", b"b_val").await.unwrap();
        txn_service.autocommit_put(b"ccc", b"c_val").await.unwrap();

        let ctx = txn_service.begin(true).unwrap();

        // Range [aaa, ccc) - includes "aaa", excludes "ccc"
        let mut iter = txn_service
            .scan_iter(&ctx, b"aaa".to_vec()..b"ccc".to_vec())
            .unwrap();
        let mut results = Vec::new();
        iter.advance().await.unwrap();
        while iter.valid() {
            results.push((iter.user_key().to_vec(), iter.value().to_vec()));
            iter.advance().await.unwrap();
        }

        assert_eq!(results.len(), 2, "should see aaa and bbb only");
        assert_eq!(results[0], (b"aaa".to_vec(), b"a_val".to_vec()));
        assert_eq!(results[1], (b"bbb".to_vec(), b"b_val".to_vec()));
    }

    #[tokio::test]
    async fn test_tso_advances_on_begin() {
        let (_storage, txn_service, _dir) = create_test_service();

        let ts1 = txn_service.tso().last_ts();

        // Each begin advances the timestamp (allocates txn_id and start_ts)
        let _ctx = txn_service.begin(false).unwrap();
        let ts2 = txn_service.tso().last_ts();
        assert!(ts2 > ts1);

        // Each begin also advances the timestamp
        let _ctx2 = txn_service.begin(true).unwrap();
        let ts3 = txn_service.tso().last_ts();
        assert!(ts3 > ts2);
    }

    // ========================================================================
    // Error Propagation Tests for MvccScanIterator
    // ========================================================================

    /// Mock MvccIterator that injects an error after N successful iterations.
    struct ErrorInjectingIterator {
        entries: Vec<(MvccKey, Vec<u8>)>,
        position: usize,
        error_after: usize,
        has_errored: bool,
    }

    impl ErrorInjectingIterator {
        fn new(entries: Vec<(MvccKey, Vec<u8>)>, error_after: usize) -> Self {
            Self {
                entries,
                position: 0,
                error_after,
                has_errored: false,
            }
        }

        fn current(&self) -> Option<&(MvccKey, Vec<u8>)> {
            self.entries.get(self.position)
        }
    }

    impl MvccIterator for ErrorInjectingIterator {
        async fn seek(&mut self, _target: &MvccKey) -> Result<()> {
            self.position = 0;
            Ok(())
        }

        async fn advance(&mut self) -> Result<()> {
            if self.has_errored {
                return Ok(());
            }

            // Increment first to move past current entry
            self.position += 1;

            // Inject error after specified number of successful advance() calls
            if self.position > self.error_after && !self.has_errored {
                self.has_errored = true;
                return Err(TiSqlError::Storage(
                    "simulated I/O error during scan".to_string(),
                ));
            }
            Ok(())
        }

        fn valid(&self) -> bool {
            !self.has_errored && self.position < self.entries.len()
        }

        fn user_key(&self) -> &[u8] {
            self.current()
                .expect("user_key() called on invalid iterator")
                .0
                .key()
        }

        fn timestamp(&self) -> Timestamp {
            self.current()
                .expect("timestamp() called on invalid iterator")
                .0
                .timestamp()
        }

        fn value(&self) -> &[u8] {
            &self
                .current()
                .expect("value() called on invalid iterator")
                .1
        }
    }

    /// Helper to collect scan results using streaming API
    async fn collect_scan_results<I: MvccIterator>(
        mut iter: MvccScanIterator<I>,
    ) -> Vec<Result<(Key, RawValue)>> {
        let mut results = Vec::new();
        loop {
            match MvccIterator::advance(&mut iter).await {
                Ok(()) => {
                    if !iter.valid() {
                        break;
                    }
                    results.push(Ok((iter.user_key().to_vec(), iter.value().to_vec())));
                }
                Err(e) => {
                    results.push(Err(e));
                    break;
                }
            }
        }
        results
    }

    #[tokio::test]
    async fn test_mvcc_scan_iterator_propagates_storage_error() {
        // Create test data: 3 entries (a@5, b@5, c@5)
        let entries = vec![
            (MvccKey::encode(b"a", 5), b"value_a".to_vec()),
            (MvccKey::encode(b"b", 5), b"value_b".to_vec()),
            (MvccKey::encode(b"c", 5), b"value_c".to_vec()),
        ];

        // Error after 1 successful advance() call on mock.
        // Lazy skip design:
        // - First find_next_visible: position=0, return "a" (no advance needed to read first entry)
        // - Second find_next_visible: lazy skip "a" (advance 0→1, ok), return "b"
        // - Third find_next_visible: lazy skip "b" (advance 1→2, 2>1=ERROR!)
        // Result: 2 Ok entries + 1 Err
        let mock_iter = ErrorInjectingIterator::new(entries, 1);

        let scan_iter = MvccScanIterator::new(
            mock_iter,
            10, // read_ts
            b"a".to_vec()..b"z".to_vec(),
            None,
        );

        let results = collect_scan_results(scan_iter).await;

        // Should have 2 Ok entries followed by error
        assert_eq!(results.len(), 3, "expected 3 items: 2 Ok + 1 Err");

        // First two results should be Ok
        assert!(results[0].is_ok(), "first result should be Ok");
        assert!(results[1].is_ok(), "second result should be Ok");

        // Third result should be Err
        assert!(results[2].is_err(), "third result should be Err");
        let err = results[2].as_ref().unwrap_err();
        assert!(
            matches!(err, TiSqlError::Storage(msg) if msg.contains("simulated I/O error")),
            "expected Storage error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_mvcc_scan_iterator_error_on_first_advance() {
        // Create test data
        let entries = vec![
            (MvccKey::encode(b"a", 5), b"value_a".to_vec()),
            (MvccKey::encode(b"b", 5), b"value_b".to_vec()),
        ];

        // Error after 0 successful advance() calls on mock.
        // Lazy skip design:
        // - First find_next_visible: position=0, return "a" (no advance needed)
        // - Second find_next_visible: lazy skip "a" (advance 0→1, 1>0=ERROR!)
        // Result: 1 Ok entry + 1 Err
        let mock_iter = ErrorInjectingIterator::new(entries, 0);

        let scan_iter = MvccScanIterator::new(mock_iter, 10, b"a".to_vec()..b"z".to_vec(), None);

        let results = collect_scan_results(scan_iter).await;

        // One successful entry, then error when trying to advance
        assert_eq!(results.len(), 2, "expected 2 items: 1 Ok + 1 Err");
        assert!(results[0].is_ok(), "first should be Ok");
        assert!(results[1].is_err(), "second should be error");
    }

    #[tokio::test]
    async fn test_mvcc_scan_iterator_error_after_all_entries() {
        // Create test data
        let entries = vec![
            (MvccKey::encode(b"a", 5), b"value_a".to_vec()),
            (MvccKey::encode(b"b", 5), b"value_b".to_vec()),
        ];

        // Error after 10 successful next() calls - won't trigger (only 2 entries)
        let mock_iter = ErrorInjectingIterator::new(entries, 10);

        let scan_iter = MvccScanIterator::new(mock_iter, 10, b"a".to_vec()..b"z".to_vec(), None);

        let results = collect_scan_results(scan_iter).await;

        // All entries should succeed
        assert_eq!(results.len(), 2, "expected 2 successful entries");
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
    }

    #[tokio::test]
    async fn test_mvcc_scan_iterator_no_silent_truncation() {
        // This test verifies the original bug is fixed:
        // Previously, errors would cause silent truncation (returning None)
        // Now, errors must be explicitly returned to the caller

        let entries = vec![
            (MvccKey::encode(b"a", 5), b"value_a".to_vec()),
            (MvccKey::encode(b"b", 5), b"value_b".to_vec()),
            (MvccKey::encode(b"c", 5), b"value_c".to_vec()),
        ];

        // Error after 1 successful advance() call on mock.
        // Lazy skip: return "a" (no advance), lazy skip "a" (advance #1 ok) return "b",
        //            lazy skip "b" (advance #2, 2>1=ERROR)
        // Result: 2 successful entries before error
        let mock_iter = ErrorInjectingIterator::new(entries.clone(), 1);

        let scan_iter = MvccScanIterator::new(mock_iter, 10, b"a".to_vec()..b"z".to_vec(), None);

        let results = collect_scan_results(scan_iter).await;

        // Count successful and error results
        let successful_count = results.iter().filter(|r| r.is_ok()).count();
        let saw_error = results.iter().any(|r| r.is_err());

        // Verify error is a Storage error
        if let Some(Err(e)) = results.iter().find(|r| r.is_err()) {
            assert!(matches!(e, TiSqlError::Storage(_)));
        }

        // CRITICAL: We must have seen the error, not silent truncation
        assert!(
            saw_error,
            "error must be propagated to caller, not silently dropped"
        );
        assert_eq!(
            successful_count, 2,
            "should have exactly 2 successful entries before error"
        );
    }

    // ========================================================================
    // Explicit Transaction Tests (Pessimistic Locking)
    // ========================================================================

    #[tokio::test]
    async fn test_explicit_txn_begin() {
        let (_storage, txn_service, _dir) = create_test_service();

        let ctx = txn_service.begin_explicit(false).unwrap();

        assert!(ctx.start_ts() > 0);
        assert!(ctx.is_valid());
        assert!(!ctx.is_read_only());
        assert!(ctx.is_explicit());
    }

    #[tokio::test]
    async fn test_explicit_txn_put_commit() {
        let (storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Put via explicit transaction (pessimistic: writes go to storage immediately)
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();

        // Key should be tracked as Write mutation
        assert_eq!(ctx.mutations.len(), 1);
        assert!(ctx.mutations.contains_key(b"key1".as_slice()));

        // Commit
        let info = txn_service.commit(ctx).await.unwrap();
        assert!(info.commit_ts > 0);

        // Data should be visible in storage
        let value = get_for_test(&*storage, b"key1").await;
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_explicit_txn_put_rollback() {
        let (storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Put via explicit transaction
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();

        // Rollback
        txn_service.rollback(ctx).unwrap();

        // Data should NOT be visible in storage (pending node is aborted)
        let value = get_for_test(&*storage, b"key1").await;
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_explicit_txn_delete_commit() {
        let (storage, txn_service, _dir) = create_test_service();

        // First commit a value (via implicit txn)
        txn_service
            .autocommit_put(b"key1", b"value1")
            .await
            .unwrap();
        assert_eq!(
            get_for_test(&*storage, b"key1").await,
            Some(b"value1".to_vec())
        );

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Delete via explicit transaction
        txn_service
            .delete(&mut ctx, b"key1".to_vec())
            .await
            .unwrap();

        // Commit
        txn_service.commit(ctx).await.unwrap();

        // Data should be deleted
        let value = get_for_test(&*storage, b"key1").await;
        assert!(value.is_none());
    }

    #[tokio::test]
    async fn test_explicit_txn_multiple_writes() {
        let (storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Multiple puts
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, b"key2".to_vec(), b"value2".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, b"key3".to_vec(), b"value3".to_vec())
            .await
            .unwrap();

        // All keys should be tracked
        assert_eq!(ctx.mutations.len(), 3);

        // Commit
        txn_service.commit(ctx).await.unwrap();

        // All data should be visible
        assert_eq!(
            get_for_test(&*storage, b"key1").await,
            Some(b"value1".to_vec())
        );
        assert_eq!(
            get_for_test(&*storage, b"key2").await,
            Some(b"value2".to_vec())
        );
        assert_eq!(
            get_for_test(&*storage, b"key3").await,
            Some(b"value3".to_vec())
        );
    }

    #[tokio::test]
    async fn test_explicit_txn_update_same_key() {
        let (storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Write key twice within same transaction
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"v1".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"v2".to_vec())
            .await
            .unwrap();

        // Key should only be tracked once
        assert_eq!(ctx.mutations.len(), 1);

        // Commit
        txn_service.commit(ctx).await.unwrap();

        // Should see latest value
        let value = get_for_test(&*storage, b"key1").await;
        assert_eq!(value, Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn test_explicit_txn_read_only_rejects_writes() {
        let (_storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin_explicit(true).unwrap();

        // Put should error for read-only explicit transaction
        let result = txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::ReadOnlyTransaction
        ));

        // No keys should be locked
        assert!(ctx.mutations.is_empty());

        // Commit should succeed (no-op for read-only)
        let info = txn_service.commit(ctx).await.unwrap();
        assert_eq!(info.lsn, 0);
    }

    #[tokio::test]
    async fn test_explicit_txn_empty_commit() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction but don't write anything
        let ctx = txn_service.begin_explicit(false).unwrap();
        assert!(ctx.mutations.is_empty());

        // Commit should succeed (no-op)
        let info = txn_service.commit(ctx).await.unwrap();
        assert_eq!(info.lsn, 0);
    }

    #[tokio::test]
    async fn test_explicit_txn_empty_rollback() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction but don't write anything
        let ctx = txn_service.begin_explicit(false).unwrap();

        // Rollback should succeed (no-op)
        txn_service.rollback(ctx).unwrap();
    }

    // ==================== Read-Your-Writes Tests ====================

    #[tokio::test]
    async fn test_read_your_writes_get() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Write a value
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();

        // Read it back (read-your-writes)
        let value = txn_service.get(&ctx, b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test]
    async fn test_read_your_writes_update() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Write initial value
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"v1".to_vec())
            .await
            .unwrap();

        // Update it
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"v2".to_vec())
            .await
            .unwrap();

        // Read should return the updated value
        let value = txn_service.get(&ctx, b"key1").await.unwrap();
        assert_eq!(value, Some(b"v2".to_vec()));

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test]
    async fn test_read_your_writes_delete() {
        let (_storage, txn_service, _dir) = create_test_service();

        // First, commit an initial value
        let mut setup_ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut setup_ctx, b"key1".to_vec(), b"initial".to_vec())
            .await
            .unwrap();
        txn_service.commit(setup_ctx).await.unwrap();

        // Start new transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Delete the key
        txn_service
            .delete(&mut ctx, b"key1".to_vec())
            .await
            .unwrap();

        // Read should return None (deleted)
        let value = txn_service.get(&ctx, b"key1").await.unwrap();
        assert_eq!(value, None);

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test]
    async fn test_read_your_writes_scan() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Write multiple values
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, b"key2".to_vec(), b"value2".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, b"key3".to_vec(), b"value3".to_vec())
            .await
            .unwrap();

        // Scan should see all pending writes
        let mut scan_iter = txn_service
            .scan_iter(&ctx, b"key".to_vec()..b"key~".to_vec())
            .unwrap();

        // Collect results
        let mut results = vec![];
        scan_iter.advance().await.unwrap();
        while scan_iter.valid() {
            results.push((scan_iter.user_key().to_vec(), scan_iter.value().to_vec()));
            scan_iter.advance().await.unwrap();
        }

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (b"key1".to_vec(), b"value1".to_vec()));
        assert_eq!(results[1], (b"key2".to_vec(), b"value2".to_vec()));
        assert_eq!(results[2], (b"key3".to_vec(), b"value3".to_vec()));

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test]
    async fn test_read_your_writes_isolation() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start two explicit transactions
        let mut ctx1 = txn_service.begin_explicit(false).unwrap();
        let ctx2 = txn_service.begin_explicit(false).unwrap();

        // Txn1 writes a value
        txn_service
            .put(&mut ctx1, b"key1".to_vec(), b"from_txn1".to_vec())
            .await
            .unwrap();

        // Txn1 should see its own write
        let value1 = txn_service.get(&ctx1, b"key1").await.unwrap();
        assert_eq!(value1, Some(b"from_txn1".to_vec()));

        // Txn2 should NOT see txn1's pending write
        let value2 = txn_service.get(&ctx2, b"key1").await.unwrap();
        assert_eq!(value2, None);

        // Clean up
        txn_service.rollback(ctx1).unwrap();
        txn_service.rollback(ctx2).unwrap();
    }

    #[tokio::test]
    async fn test_read_your_writes_with_committed_base() {
        let (_storage, txn_service, _dir) = create_test_service();

        // First, commit a base value
        let mut setup_ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut setup_ctx, b"key1".to_vec(), b"committed".to_vec())
            .await
            .unwrap();
        txn_service.commit(setup_ctx).await.unwrap();

        // Start new transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Update the key
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"pending".to_vec())
            .await
            .unwrap();

        // Read should return the pending value, not the committed one
        let value = txn_service.get(&ctx, b"key1").await.unwrap();
        assert_eq!(value, Some(b"pending".to_vec()));

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test]
    async fn test_read_your_writes_delete_then_insert() {
        let (_storage, txn_service, _dir) = create_test_service();

        // First, commit a base value
        let mut setup_ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut setup_ctx, b"key1".to_vec(), b"original".to_vec())
            .await
            .unwrap();
        txn_service.commit(setup_ctx).await.unwrap();

        // Start new transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Delete
        txn_service
            .delete(&mut ctx, b"key1".to_vec())
            .await
            .unwrap();

        // Should see None
        let value = txn_service.get(&ctx, b"key1").await.unwrap();
        assert_eq!(value, None);

        // Re-insert with new value
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"reinserted".to_vec())
            .await
            .unwrap();

        // Should see the new value
        let value = txn_service.get(&ctx, b"key1").await.unwrap();
        assert_eq!(value, Some(b"reinserted".to_vec()));

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    // ========================================================================
    // Additional coverage tests for edge cases
    // ========================================================================

    #[tokio::test]
    async fn test_concurrency_manager_getter() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Test that concurrency_manager() getter works
        let cm = txn_service.concurrency_manager();
        assert_eq!(cm.max_ts(), 0);

        // After begin, max_ts should be updated
        let _ctx = txn_service.begin(false).unwrap();
        assert!(cm.max_ts() > 0);
    }

    #[tokio::test]
    async fn test_clog_service_getter() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Test that clog_service() getter works
        let clog = txn_service.clog_service();

        // Write something to test the getter works
        txn_service.autocommit_put(b"test", b"value").await.unwrap();

        // After write, clog should have entries
        assert!(clog.max_lsn().unwrap() >= 1);
    }

    #[tokio::test]
    async fn test_put_on_committed_transaction() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Begin and commit a transaction
        let mut ctx = txn_service.begin(false).unwrap();
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();
        let commit_info = txn_service.commit(ctx).await.unwrap();

        // Manually create a committed context
        let mut committed_ctx =
            TxnCtx::new(commit_info.txn_id + 1, commit_info.commit_ts + 1, false);
        committed_ctx.state = TxnState::Committed {
            commit_ts: commit_info.commit_ts,
        };

        // Try to put on committed transaction - should fail
        let result = txn_service
            .put(&mut committed_ctx, b"key2".to_vec(), b"value2".to_vec())
            .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::TransactionNotActive(_)
        ));
    }

    #[tokio::test]
    async fn test_delete_on_committed_transaction() {
        let (_storage, txn_service, _dir) = create_test_service();

        let ctx = txn_service.begin(false).unwrap();
        let commit_info = txn_service.commit(ctx).await.unwrap();

        // Create committed context
        let mut committed_ctx =
            TxnCtx::new(commit_info.txn_id + 1, commit_info.commit_ts + 1, false);
        committed_ctx.state = TxnState::Committed {
            commit_ts: commit_info.commit_ts,
        };

        // Try to delete on committed transaction - should fail
        let result = txn_service
            .delete(&mut committed_ctx, b"key1".to_vec())
            .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::TransactionNotActive(_)
        ));
    }

    #[tokio::test]
    async fn test_put_on_aborted_transaction() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Create an aborted context
        let mut aborted_ctx = TxnCtx::new(1, 1, false);
        aborted_ctx.state = TxnState::Aborted;

        // Try to put on aborted transaction - should fail
        let result = txn_service
            .put(&mut aborted_ctx, b"key1".to_vec(), b"value1".to_vec())
            .await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::TransactionNotActive(_)
        ));
    }

    #[tokio::test]
    async fn test_delete_on_aborted_transaction() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Create an aborted context
        let mut aborted_ctx = TxnCtx::new(1, 1, false);
        aborted_ctx.state = TxnState::Aborted;

        // Try to delete on aborted transaction - should fail
        let result = txn_service.delete(&mut aborted_ctx, b"key1".to_vec()).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TiSqlError::TransactionNotActive(_)
        ));
    }

    #[tokio::test]
    async fn test_commit_on_aborted_transaction() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Create an aborted context
        let mut aborted_ctx = TxnCtx::new(1, 1, false);
        aborted_ctx.state = TxnState::Aborted;

        // Try to commit aborted transaction - should fail
        let result = txn_service.commit(aborted_ctx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_commit_on_already_committed() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Create a committed context
        let mut committed_ctx = TxnCtx::new(1, 1, false);
        committed_ctx.state = TxnState::Committed { commit_ts: 10 };

        // Try to commit again - should fail
        let result = txn_service.commit(committed_ctx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_recover_with_uncommitted_entries() {
        // Test recovery where some transactions are not committed (rolled back)
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let io_handle = tokio::runtime::Handle::current();

        {
            let clog_service = Arc::new(FileClogService::open(config.clone(), &io_handle).unwrap());

            // Write entries directly to clog without commit record
            // This simulates a crash before commit
            clog_service
                .append(
                    ClogEntry {
                        txn_id: 100,
                        lsn: 1,
                        op: ClogOp::Put {
                            key: b"uncommitted_key".to_vec(),
                            value: b"uncommitted_value".to_vec(),
                        },
                    },
                    true,
                )
                .unwrap();

            // Write a committed transaction
            clog_service
                .append(
                    ClogEntry {
                        txn_id: 101,
                        lsn: 2,
                        op: ClogOp::Put {
                            key: b"committed_key".to_vec(),
                            value: b"committed_value".to_vec(),
                        },
                    },
                    true,
                )
                .unwrap();
            clog_service
                .append(
                    ClogEntry {
                        txn_id: 101,
                        lsn: 3,
                        op: ClogOp::Commit { commit_ts: 200 },
                    },
                    true,
                )
                .unwrap();

            clog_service.close().await.unwrap();
        }

        // Recover
        let (clog_service, entries) = FileClogService::recover(config, &io_handle).unwrap();
        let clog_service = Arc::new(clog_service);
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);

        let stats = txn_service.recover(&entries).unwrap();

        // Should have 1 committed txn, 1 rolled back entry
        assert_eq!(stats.committed_txns, 1);
        assert_eq!(stats.rolled_back_entries, 1);
        assert_eq!(stats.applied_puts, 1);

        // Only committed key should be present
        assert!(get_for_test(&*storage, b"uncommitted_key").await.is_none());
        assert_eq!(
            get_for_test(&*storage, b"committed_key").await,
            Some(b"committed_value".to_vec())
        );
    }

    #[tokio::test]
    async fn test_scan_with_unbounded_end() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write some data
        txn_service
            .autocommit_put(b"key1", b"value1")
            .await
            .unwrap();
        txn_service
            .autocommit_put(b"key2", b"value2")
            .await
            .unwrap();
        txn_service
            .autocommit_put(b"key3", b"value3")
            .await
            .unwrap();

        let ctx = txn_service.begin(true).unwrap();

        // Scan with empty end (unbounded)
        let mut iter = txn_service
            .scan_iter(&ctx, b"key1".to_vec()..vec![])
            .unwrap();
        let mut results = Vec::new();
        iter.advance().await.unwrap();
        while iter.valid() {
            results.push(iter.user_key().to_vec());
            iter.advance().await.unwrap();
        }

        // Should see all keys starting from key1
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], b"key1");
        assert_eq!(results[1], b"key2");
        assert_eq!(results[2], b"key3");
    }

    #[tokio::test]
    async fn test_get_iterates_through_versions() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write multiple versions of same key
        let info1 = txn_service.autocommit_put(b"key", b"v1").await.unwrap();
        let info2 = txn_service.autocommit_put(b"key", b"v2").await.unwrap();
        let info3 = txn_service.autocommit_put(b"key", b"v3").await.unwrap();

        // Read at different timestamps
        let ctx1 = TxnCtx::new(0, info1.commit_ts, true);
        let value1 = txn_service.get(&ctx1, b"key").await.unwrap();
        assert_eq!(value1, Some(b"v1".to_vec()));

        let ctx2 = TxnCtx::new(0, info2.commit_ts, true);
        let value2 = txn_service.get(&ctx2, b"key").await.unwrap();
        assert_eq!(value2, Some(b"v2".to_vec()));

        let ctx3 = TxnCtx::new(0, info3.commit_ts, true);
        let value3 = txn_service.get(&ctx3, b"key").await.unwrap();
        assert_eq!(value3, Some(b"v3".to_vec()));
    }

    #[tokio::test]
    async fn test_get_with_tombstone_in_middle() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Write, delete, write again
        let _info1 = txn_service.autocommit_put(b"key", b"v1").await.unwrap();
        let info2 = txn_service.autocommit_delete(b"key").await.unwrap();
        let info3 = txn_service.autocommit_put(b"key", b"v3").await.unwrap();

        // At delete timestamp, should see None
        let ctx2 = TxnCtx::new(0, info2.commit_ts, true);
        let value2 = txn_service.get(&ctx2, b"key").await.unwrap();
        assert_eq!(value2, None);

        // At v3 timestamp, should see v3
        let ctx3 = TxnCtx::new(0, info3.commit_ts, true);
        let value3 = txn_service.get(&ctx3, b"key").await.unwrap();
        assert_eq!(value3, Some(b"v3".to_vec()));
    }

    #[tokio::test]
    async fn test_explicit_txn_write_conflict() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Transaction 1: Lock key
        let mut ctx1 = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut ctx1, b"conflict_key".to_vec(), b"value1".to_vec())
            .await
            .unwrap();

        // Transaction 2: Try to write same key - should fail
        let mut ctx2 = txn_service.begin_explicit(false).unwrap();
        let result = txn_service
            .put(&mut ctx2, b"conflict_key".to_vec(), b"value2".to_vec())
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            TiSqlError::KeyIsLocked { key, lock_ts, .. } => {
                assert_eq!(key, b"conflict_key".to_vec());
                assert_eq!(lock_ts, ctx1.start_ts);
            }
            other => panic!("Expected KeyIsLocked error, got: {other:?}"),
        }

        // Cleanup
        txn_service.rollback(ctx1).unwrap();
        txn_service.rollback(ctx2).unwrap();
    }

    #[tokio::test]
    async fn test_explicit_txn_delete_conflict() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Commit initial value
        txn_service
            .autocommit_put(b"delete_key", b"value")
            .await
            .unwrap();

        // Transaction 1: Lock key with delete
        let mut ctx1 = txn_service.begin_explicit(false).unwrap();
        txn_service
            .delete(&mut ctx1, b"delete_key".to_vec())
            .await
            .unwrap();

        // Transaction 2: Try to delete same key - should fail
        let mut ctx2 = txn_service.begin_explicit(false).unwrap();
        let result = txn_service.delete(&mut ctx2, b"delete_key".to_vec()).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            TiSqlError::KeyIsLocked { key, lock_ts, .. } => {
                assert_eq!(key, b"delete_key".to_vec());
                assert_eq!(lock_ts, ctx1.start_ts);
            }
            other => panic!("Expected KeyIsLocked error, got: {other:?}"),
        }

        // Cleanup
        txn_service.rollback(ctx1).unwrap();
        txn_service.rollback(ctx2).unwrap();
    }

    #[tokio::test]
    async fn test_recover_with_delete_operation() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let io_handle = tokio::runtime::Handle::current();

        {
            let clog_service = Arc::new(FileClogService::open(config.clone(), &io_handle).unwrap());
            let tso = Arc::new(LocalTso::new(1));
            let cm = Arc::new(ConcurrencyManager::new(0));
            let storage = Arc::new(MemTableEngine::new());
            let txn_service = TransactionService::new(storage, clog_service.clone(), tso, cm);

            // Put then delete
            txn_service
                .autocommit_put(b"del_key", b"value")
                .await
                .unwrap();
            txn_service.autocommit_delete(b"del_key").await.unwrap();

            clog_service.close().await.unwrap();
        }

        // Recover
        let (clog_service, entries) = FileClogService::recover(config, &io_handle).unwrap();
        let clog_service = Arc::new(clog_service);
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);

        let stats = txn_service.recover(&entries).unwrap();

        assert_eq!(stats.committed_txns, 2);
        assert_eq!(stats.applied_puts, 1);
        assert_eq!(stats.applied_deletes, 1);

        // Key should be deleted
        assert!(get_for_test(&*storage, b"del_key").await.is_none());
    }

    #[tokio::test]
    async fn test_recover_empty_entries() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let io_handle = tokio::runtime::Handle::current();
        let clog_service = Arc::new(FileClogService::open(config, &io_handle).unwrap());
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let storage = Arc::new(MemTableEngine::new());
        let txn_service = TransactionService::new(Arc::clone(&storage), clog_service, tso, cm);

        // Recover with empty entries
        let stats = txn_service.recover(&[]).unwrap();

        assert_eq!(stats.committed_txns, 0);
        assert_eq!(stats.applied_puts, 0);
        assert_eq!(stats.applied_deletes, 0);
        assert_eq!(stats.rolled_back_entries, 0);
    }

    #[tokio::test]
    async fn test_storage_getter() {
        let (storage, txn_service, _dir) = create_test_service();

        // Test storage() getter returns the same storage
        let storage_ref = txn_service.storage();
        txn_service
            .autocommit_put(b"test_key", b"test_value")
            .await
            .unwrap();

        // Should be able to read from both references
        let value1 = get_for_test(&*storage, b"test_key").await;
        let value2 = get_for_test(storage_ref, b"test_key").await;
        assert_eq!(value1, value2);
        assert_eq!(value1, Some(b"test_value".to_vec()));
    }

    #[tokio::test]
    async fn test_tso_getter() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Test tso() getter
        let tso = txn_service.tso();
        let ts1 = tso.last_ts();

        // After begin, TSO should advance
        let _ctx = txn_service.begin(false).unwrap();
        let ts2 = tso.last_ts();

        assert!(ts2 > ts1);
    }

    // ========================================================================
    // Clog Durability Tests
    // ========================================================================

    /// Helper: collect clog entries for a given txn_id, excluding Commit/Rollback records.
    fn clog_data_entries(entries: &[ClogEntry], txn_id: TxnId) -> Vec<&ClogEntry> {
        entries
            .iter()
            .filter(|e| {
                e.txn_id == txn_id && !matches!(e.op, ClogOp::Commit { .. } | ClogOp::Rollback)
            })
            .collect()
    }

    #[tokio::test]
    async fn test_implicit_txn_commit_writes_clog() {
        let (_storage, txn_service, _dir) = create_test_service();

        let info = txn_service
            .autocommit_put(b"key1", b"value1")
            .await
            .unwrap();
        assert!(info.lsn > 0, "implicit txn should produce non-zero LSN");

        // Verify clog contains Put + Commit entries
        let entries = txn_service.clog_service().read_all().unwrap();
        let data = clog_data_entries(&entries, info.txn_id);
        assert_eq!(data.len(), 1, "should have 1 Put entry");
        assert!(matches!(&data[0].op, ClogOp::Put { key, value }
            if key == b"key1" && value == b"value1"));

        let commits: Vec<_> = entries
            .iter()
            .filter(|e| e.txn_id == info.txn_id && matches!(e.op, ClogOp::Commit { .. }))
            .collect();
        assert_eq!(commits.len(), 1, "should have 1 Commit entry");
    }

    #[tokio::test]
    async fn test_explicit_txn_commit_writes_clog() {
        let (_storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        let txn_id = ctx.txn_id();
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();
        let info = txn_service.commit(ctx).await.unwrap();
        assert!(info.lsn > 0, "explicit txn should produce non-zero LSN");

        // Verify clog contains Put + Commit entries
        let entries = txn_service.clog_service().read_all().unwrap();
        let data = clog_data_entries(&entries, txn_id);
        assert_eq!(data.len(), 1, "should have 1 Put entry");
        assert!(matches!(&data[0].op, ClogOp::Put { key, value }
            if key == b"key1" && value == b"value1"));

        let commits: Vec<_> = entries
            .iter()
            .filter(|e| e.txn_id == txn_id && matches!(e.op, ClogOp::Commit { .. }))
            .collect();
        assert_eq!(commits.len(), 1, "should have 1 Commit entry");
    }

    #[tokio::test]
    async fn test_explicit_txn_delete_existing_writes_clog() {
        let (_storage, txn_service, _dir) = create_test_service();

        // First, commit a value via autocommit
        txn_service
            .autocommit_put(b"key1", b"value1")
            .await
            .unwrap();

        // Now delete it via explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();
        let txn_id = ctx.txn_id();
        txn_service
            .delete(&mut ctx, b"key1".to_vec())
            .await
            .unwrap();
        let info = txn_service.commit(ctx).await.unwrap();
        assert!(info.lsn > 0);

        // Verify clog contains Delete entry (not LOCK)
        let entries = txn_service.clog_service().read_all().unwrap();
        let data = clog_data_entries(&entries, txn_id);
        assert_eq!(data.len(), 1, "should have 1 Delete entry");
        assert!(
            matches!(&data[0].op, ClogOp::Delete { key } if key == b"key1"),
            "expected Delete entry for key1, got {:?}",
            data[0].op
        );
    }

    #[tokio::test]
    async fn test_explicit_txn_multiple_writes_clog() {
        let (_storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        let txn_id = ctx.txn_id();
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"v1".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, b"key2".to_vec(), b"v2".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, b"key3".to_vec(), b"v3".to_vec())
            .await
            .unwrap();
        txn_service.commit(ctx).await.unwrap();

        let entries = txn_service.clog_service().read_all().unwrap();
        let data = clog_data_entries(&entries, txn_id);
        assert_eq!(data.len(), 3, "should have 3 Put entries");

        let commits: Vec<_> = entries
            .iter()
            .filter(|e| e.txn_id == txn_id && matches!(e.op, ClogOp::Commit { .. }))
            .collect();
        assert_eq!(commits.len(), 1, "should have 1 Commit entry");
    }

    #[tokio::test]
    async fn test_explicit_txn_delete_nonexistent_no_clog() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Delete a key that was never written
        let mut ctx = txn_service.begin_explicit(false).unwrap();
        let txn_id = ctx.txn_id();
        txn_service
            .delete(&mut ctx, b"nonexistent".to_vec())
            .await
            .unwrap();
        txn_service.commit(ctx).await.unwrap();

        // Verify NO data entries in clog (LOCK keys are skipped)
        let entries = txn_service.clog_service().read_all().unwrap();
        let data = clog_data_entries(&entries, txn_id);
        assert!(
            data.is_empty(),
            "LOCK-only delete should produce no data entries, got {} entries",
            data.len()
        );
    }

    #[tokio::test]
    async fn test_explicit_txn_put_then_delete_produces_tombstone() {
        let (_storage, txn_service, _dir) = create_test_service();

        // PUT then DELETE same key (no committed value underneath).
        // delete sees own pending put (owner_ts=start_ts) → writes TOMBSTONE.
        let mut ctx = txn_service.begin_explicit(false).unwrap();
        let txn_id = ctx.txn_id();
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"value1".to_vec())
            .await
            .unwrap();
        txn_service
            .delete(&mut ctx, b"key1".to_vec())
            .await
            .unwrap();
        txn_service.commit(ctx).await.unwrap();

        // TOMBSTONE → Delete entry in clog
        let entries = txn_service.clog_service().read_all().unwrap();
        let data = clog_data_entries(&entries, txn_id);
        assert_eq!(
            data.len(),
            1,
            "PUT then DELETE should produce 1 Delete entry, got {:?}",
            data.iter().map(|e| &e.op).collect::<Vec<_>>()
        );
        assert!(
            matches!(&data[0].op, ClogOp::Delete { key } if key == b"key1"),
            "Should be a Delete for key1"
        );
    }

    #[tokio::test]
    async fn test_explicit_txn_put_delete_put_writes_clog() {
        let (_storage, txn_service, _dir) = create_test_service();

        // PUT → DELETE → PUT sequence (no committed base)
        let mut ctx = txn_service.begin_explicit(false).unwrap();
        let txn_id = ctx.txn_id();
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"v1".to_vec())
            .await
            .unwrap();
        txn_service
            .delete(&mut ctx, b"key1".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, b"key1".to_vec(), b"v2".to_vec())
            .await
            .unwrap();
        txn_service.commit(ctx).await.unwrap();

        // Final state is PUT v2, so clog should have 1 Put entry
        let entries = txn_service.clog_service().read_all().unwrap();
        let data = clog_data_entries(&entries, txn_id);
        assert_eq!(data.len(), 1, "should have 1 Put entry for final value");
        assert!(
            matches!(&data[0].op, ClogOp::Put { key, value }
                if key == b"key1" && value == b"v2"),
            "expected Put(key1, v2), got {:?}",
            data[0].op
        );
    }

    // ========================================================================
    // Snapshot Isolation / Preparing State Tests
    // ========================================================================

    #[tokio::test]
    async fn test_reader_skips_running_pending() {
        // A reader encountering a Running txn's pending node should skip it
        // and see the old committed version.
        let (_storage, txn_service, _dir) = create_test_service();

        // Commit a base value
        txn_service.autocommit_put(b"key1", b"v1").await.unwrap();

        // Start an explicit txn and write a pending value
        let mut writer = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut writer, b"key1".to_vec(), b"v2".to_vec())
            .await
            .unwrap();

        // Writer is Running in TxnStateCache.
        assert_eq!(
            txn_service
                .concurrency_manager()
                .get_txn_state(writer.start_ts),
            Some(TxnState::Running)
        );

        // Reader should see the committed v1 (pending from Running txn is skipped)
        let reader = txn_service.begin(true).unwrap();
        let val = txn_service.get(&reader, b"key1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"v1".as_slice()));

        // Cleanup
        txn_service.rollback(writer).unwrap();
    }

    #[tokio::test]
    async fn test_reader_skips_aborted_pending() {
        // A reader encountering an Aborted txn's pending node should skip it.
        let (_storage, txn_service, _dir) = create_test_service();

        // Commit a base value
        txn_service.autocommit_put(b"key1", b"v1").await.unwrap();

        // Start an explicit txn, write, then rollback (abort)
        let mut writer = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut writer, b"key1".to_vec(), b"v2".to_vec())
            .await
            .unwrap();
        txn_service.rollback(writer).unwrap();

        // Reader should see v1 (aborted pending skipped)
        let reader = txn_service.begin(true).unwrap();
        let val = txn_service.get(&reader, b"key1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"v1".as_slice()));
    }

    #[tokio::test]
    async fn test_implicit_commit_registers_in_txn_state_cache() {
        // Verify that implicit txn commit registers in TxnStateCache
        // and cleans up after itself.
        let (_storage, txn_service, _dir) = create_test_service();

        // Before: no transactions in state cache
        assert_eq!(txn_service.concurrency_manager().active_txn_count(), 0);

        // Autocommit write - should register, commit, and remove from cache
        txn_service.autocommit_put(b"key1", b"v1").await.unwrap();

        // After: should be cleaned up
        assert_eq!(txn_service.concurrency_manager().active_txn_count(), 0);

        // Verify data is committed
        let reader = txn_service.begin(true).unwrap();
        let val = txn_service.get(&reader, b"key1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"v1".as_slice()));
    }

    #[tokio::test]
    async fn test_explicit_commit_uses_preparing_state() {
        // Verify that explicit transaction commit transitions through Preparing.
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction and write
        let mut writer = txn_service.begin_explicit(false).unwrap();
        let start_ts = writer.start_ts;
        txn_service
            .put(&mut writer, b"key1".to_vec(), b"v1".to_vec())
            .await
            .unwrap();

        // Before commit: txn is Running
        assert_eq!(
            txn_service.concurrency_manager().get_txn_state(start_ts),
            Some(TxnState::Running)
        );

        // Commit should succeed (transitions through Preparing -> Prepared -> Committed)
        let info = txn_service.commit(writer).await.unwrap();
        assert!(info.commit_ts > 0);

        // After commit: txn is removed from state cache (cleaned up).
        // This is safe because finalize_pending already committed the nodes in storage,
        // so readers won't encounter pending nodes that need resolution.
        assert_eq!(
            txn_service.concurrency_manager().get_txn_state(start_ts),
            None
        );
    }

    #[tokio::test]
    async fn test_reader_sees_committed_value_after_prepare() {
        // After a writer commits, readers should see the committed value.
        let (_storage, txn_service, _dir) = create_test_service();

        // Commit base value
        txn_service.autocommit_put(b"key1", b"v1").await.unwrap();

        // Writer: explicit txn, write v2, commit
        let mut writer = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut writer, b"key1".to_vec(), b"v2".to_vec())
            .await
            .unwrap();
        let commit_info = txn_service.commit(writer).await.unwrap();

        // Reader with start_ts > commit_ts should see v2
        let reader = txn_service.begin(true).unwrap();
        assert!(reader.start_ts > commit_info.commit_ts);
        let val = txn_service.get(&reader, b"key1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"v2".as_slice()));
    }

    #[tokio::test]
    async fn test_scan_iter_resolves_pending_running() {
        // scan_iter should skip pending nodes from Running transactions.
        let (_storage, txn_service, _dir) = create_test_service();

        // Commit base values
        txn_service.autocommit_put(b"a", b"v1").await.unwrap();
        txn_service.autocommit_put(b"c", b"v3").await.unwrap();

        // Start explicit txn and write to key "b"
        let mut writer = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut writer, b"b".to_vec(), b"pending".to_vec())
            .await
            .unwrap();

        // Reader scan should see a=v1 and c=v3, but skip b (Running pending)
        let reader = txn_service.begin(true).unwrap();
        let mut iter = txn_service
            .scan_iter(&reader, b"a".to_vec()..b"d".to_vec())
            .unwrap();
        iter.advance().await.unwrap();

        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"a");
        assert_eq!(iter.value(), b"v1");
        iter.advance().await.unwrap();

        assert!(iter.valid());
        assert_eq!(iter.user_key(), b"c");
        assert_eq!(iter.value(), b"v3");
        iter.advance().await.unwrap();

        assert!(!iter.valid());

        // Cleanup
        txn_service.rollback(writer).unwrap();
    }

    #[tokio::test]
    async fn test_get_resolves_pending_running() {
        // get() for non-explicit txns should skip Running pending nodes.
        let (_storage, txn_service, _dir) = create_test_service();

        // Commit base value
        txn_service.autocommit_put(b"key1", b"v1").await.unwrap();

        // Start explicit txn (Running) and write a pending update
        let mut writer = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut writer, b"key1".to_vec(), b"v2".to_vec())
            .await
            .unwrap();

        // Non-explicit reader should resolve the pending node:
        // Writer is Running -> skip -> return committed v1
        let reader = txn_service.begin(true).unwrap();
        let val = txn_service.get(&reader, b"key1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"v1".as_slice()));

        // Cleanup
        txn_service.rollback(writer).unwrap();
    }

    #[tokio::test]
    async fn test_pending_resolution_committed_visible() {
        // Test: writer commits with commit_ts <= reader.start_ts
        // -> reader should see the committed value.
        let (_storage, txn_service, _dir) = create_test_service();

        txn_service.autocommit_put(b"key1", b"v1").await.unwrap();

        // Writer commits v2
        let mut writer = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut writer, b"key1".to_vec(), b"v2".to_vec())
            .await
            .unwrap();
        let commit_info = txn_service.commit(writer).await.unwrap();

        // Reader that started after commit sees v2
        let reader = txn_service.begin(true).unwrap();
        assert!(reader.start_ts > commit_info.commit_ts);
        let val = txn_service.get(&reader, b"key1").await.unwrap();
        assert_eq!(val.as_deref(), Some(b"v2".as_slice()));
    }
}
