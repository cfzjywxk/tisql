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

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::Instant as TokioInstant;

#[cfg(feature = "failpoints")]
use fail::fail_point;

use super::api::{
    CommitInfo, MutationMeta, MutationPayload, StatementGuard, StatementUndoEntry, TxnCtx,
    TxnScanCursor, TxnScanEntry, TxnService, TxnState,
};
use super::concurrency::ConcurrencyManager;
use crate::catalog::types::{Key, RawValue, TableId, Timestamp, TxnId};
use crate::clog::{ClogEntry, ClogOp, ClogOpRef, ClogService};
use crate::inner_table::core_tables::USER_TABLE_ID_START;
use crate::kernel::txn_storage::{
    AbortRequest, CommitReservation, FinalizeRequest, PointReadResult, RecoveredWrite,
    TxnDeleteEffect, TxnPointRead, TxnRestoreMutation, TxnStageError, TxnStoragePort,
    TxnStorageRecoveryPort, TxnStorageScanCursor, TxnStorageScanRequest, TxnStorageScanStep,
    TxnStorageVisibleState,
};
use crate::tso::TsoService;
use crate::util::codec::key::decode_table_id;
use crate::util::error::{Result, TiSqlError};

/// Transaction service manages transactions with durability and MVCC.
///
/// This is the unified interface for all transaction operations, following
/// OceanBase's `ObTransService` pattern. Transaction state is held in `TxnCtx`
/// and passed to each operation.
///
/// All write operations go through `TxnStoragePort`:
/// 1. Write pending nodes to storage (locks acquired immediately)
/// 2. On commit: finalize pending nodes with commit_ts
/// 3. On rollback: abort pending nodes
pub struct TransactionService<S: TxnStoragePort, L: ClogService, T: TsoService> {
    /// Storage engine for state
    storage: Arc<S>,
    /// Commit log service for durability
    clog_service: Arc<L>,
    /// TSO service for timestamp allocation
    tso: Arc<T>,
    /// ConcurrencyManager for lock table and max_ts tracking
    concurrency_manager: Arc<ConcurrencyManager>,
    /// Wait/backoff policy for lock conflicts and ambiguous prepared reads.
    conflict_wait_config: ConflictWaitConfig,
    /// Aggregated write-path diagnostics exposed through SHOW ENGINE STATUS.
    write_path_diagnostics: Arc<WritePathDiagnostics>,
}

/// Lock-conflict wait policy shared by read and write paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictWaitConfig {
    /// Total wait budget before surfacing `LockWaitTimeout`.
    pub lock_wait_timeout: Duration,
    /// Number of cooperative yields before sleeping.
    pub spin_retries: u32,
    /// Initial sleep delay after the spin phase.
    pub initial_backoff: Duration,
    /// Maximum sleep delay.
    pub max_backoff: Duration,
}

impl Default for ConflictWaitConfig {
    fn default() -> Self {
        Self {
            lock_wait_timeout: Duration::from_secs(10),
            spin_retries: 32,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(100),
        }
    }
}

impl ConflictWaitConfig {
    pub fn with_lock_wait_timeout(mut self, timeout: Duration) -> Self {
        self.lock_wait_timeout = timeout;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConflictWaitOutcome {
    RetryNow,
    TimedOut,
}

#[derive(Debug, Clone, Copy)]
struct ConflictBackoff {
    started_at: TokioInstant,
    timeout: Duration,
    remaining_spins: u32,
    next_sleep: Duration,
    max_sleep: Duration,
}

impl ConflictBackoff {
    fn new(config: ConflictWaitConfig) -> Self {
        Self {
            started_at: TokioInstant::now(),
            timeout: config.lock_wait_timeout,
            remaining_spins: config.spin_retries,
            next_sleep: config.initial_backoff,
            max_sleep: config.max_backoff,
        }
    }

    fn remaining_time(&self) -> Option<Duration> {
        self.timeout.checked_sub(self.started_at.elapsed())
    }

    async fn wait_next(&mut self) -> ConflictWaitOutcome {
        let Some(remaining) = self.remaining_time() else {
            return ConflictWaitOutcome::TimedOut;
        };
        if remaining.is_zero() {
            return ConflictWaitOutcome::TimedOut;
        }

        if self.remaining_spins > 0 {
            self.remaining_spins -= 1;
            tokio::task::yield_now().await;
            return if self.remaining_time().is_some_and(|left| !left.is_zero()) {
                ConflictWaitOutcome::RetryNow
            } else {
                ConflictWaitOutcome::TimedOut
            };
        }

        let sleep_for = self.next_sleep.min(remaining);
        if sleep_for.is_zero() {
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(sleep_for).await;
        }
        let doubled = self.next_sleep.checked_mul(2).unwrap_or(Duration::MAX);
        self.next_sleep = doubled.min(self.max_sleep);

        if self.remaining_time().is_some_and(|left| !left.is_zero()) {
            ConflictWaitOutcome::RetryNow
        } else {
            ConflictWaitOutcome::TimedOut
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TimingStageSnapshot {
    count: u64,
    total_us: u64,
    avg_us: u64,
    max_us: u64,
}

#[derive(Debug, Default)]
struct TimingStageCounter {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

impl TimingStageCounter {
    fn record_duration(&self, duration: Duration) {
        let us = duration_to_micros_u64(duration);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(us, Ordering::Relaxed);
        update_max_relaxed(&self.max_us, us);
    }

    fn snapshot(&self) -> TimingStageSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let total_us = self.total_us.load(Ordering::Relaxed);
        TimingStageSnapshot {
            count,
            total_us,
            avg_us: if count == 0 { 0 } else { total_us / count },
            max_us: self.max_us.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct WritePathDiagnosticsSnapshot {
    duplicate_check: TimingStageSnapshot,
    put_pending: TimingStageSnapshot,
    lock_pending: TimingStageSnapshot,
    commit_read_values: TimingStageSnapshot,
    commit_prepare_ts: TimingStageSnapshot,
    commit_clog_write: TimingStageSnapshot,
    commit_fsync_wait: TimingStageSnapshot,
    commit_finalize: TimingStageSnapshot,
    commit_total: TimingStageSnapshot,
    commit_success: u64,
    commit_failure: u64,
}

#[derive(Debug, Default)]
struct WritePathDiagnostics {
    duplicate_check: TimingStageCounter,
    put_pending: TimingStageCounter,
    lock_pending: TimingStageCounter,
    commit_read_values: TimingStageCounter,
    commit_prepare_ts: TimingStageCounter,
    commit_clog_write: TimingStageCounter,
    commit_fsync_wait: TimingStageCounter,
    commit_finalize: TimingStageCounter,
    commit_total: TimingStageCounter,
    commit_success: AtomicU64,
    commit_failure: AtomicU64,
}

impl WritePathDiagnostics {
    fn snapshot(&self) -> WritePathDiagnosticsSnapshot {
        WritePathDiagnosticsSnapshot {
            duplicate_check: self.duplicate_check.snapshot(),
            put_pending: self.put_pending.snapshot(),
            lock_pending: self.lock_pending.snapshot(),
            commit_read_values: self.commit_read_values.snapshot(),
            commit_prepare_ts: self.commit_prepare_ts.snapshot(),
            commit_clog_write: self.commit_clog_write.snapshot(),
            commit_fsync_wait: self.commit_fsync_wait.snapshot(),
            commit_finalize: self.commit_finalize.snapshot(),
            commit_total: self.commit_total.snapshot(),
            commit_success: self.commit_success.load(Ordering::Relaxed),
            commit_failure: self.commit_failure.load(Ordering::Relaxed),
        }
    }

    fn metric_pairs(&self) -> Vec<(String, String)> {
        let snapshot = self.snapshot();
        let mut pairs = Vec::with_capacity(32);
        push_stage_metrics(&mut pairs, "duplicate_check", snapshot.duplicate_check);
        push_stage_metrics(&mut pairs, "put_pending", snapshot.put_pending);
        push_stage_metrics(&mut pairs, "lock_pending", snapshot.lock_pending);
        push_stage_metrics(
            &mut pairs,
            "commit.read_values",
            snapshot.commit_read_values,
        );
        push_stage_metrics(&mut pairs, "commit.prepare_ts", snapshot.commit_prepare_ts);
        push_stage_metrics(&mut pairs, "commit.clog_write", snapshot.commit_clog_write);
        push_stage_metrics(&mut pairs, "commit.fsync_wait", snapshot.commit_fsync_wait);
        push_stage_metrics(&mut pairs, "commit.finalize", snapshot.commit_finalize);
        push_stage_metrics(&mut pairs, "commit.total", snapshot.commit_total);
        pairs.push((
            "commit.success_count".to_string(),
            snapshot.commit_success.to_string(),
        ));
        pairs.push((
            "commit.failure_count".to_string(),
            snapshot.commit_failure.to_string(),
        ));
        pairs
    }
}

fn push_stage_metrics(
    pairs: &mut Vec<(String, String)>,
    stage_name: &str,
    stage: TimingStageSnapshot,
) {
    pairs.push((format!("{stage_name}.count"), stage.count.to_string()));
    pairs.push((format!("{stage_name}.avg_us"), stage.avg_us.to_string()));
    pairs.push((format!("{stage_name}.max_us"), stage.max_us.to_string()));
}

fn duration_to_micros_u64(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn update_max_relaxed(target: &AtomicU64, observed: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while observed > current {
        match target.compare_exchange_weak(current, observed, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

/// Ensures commit reservations are released on every exit path.
struct CommitReservationReleaseGuard<'a, S: TxnStoragePort + ?Sized> {
    storage: &'a S,
    reservation: Option<CommitReservation>,
    released: bool,
}

impl<'a, S: TxnStoragePort + ?Sized> CommitReservationReleaseGuard<'a, S> {
    fn new(storage: &'a S, reservation: CommitReservation) -> Self {
        Self {
            storage,
            reservation: Some(reservation),
            released: false,
        }
    }

    fn release(mut self) -> Option<u64> {
        let reservation = self.reservation.take()?;
        let released = self.storage.release_commit_lsn(reservation);
        self.released = true;
        released.then_some(reservation.lsn)
    }
}

impl<S: TxnStoragePort + ?Sized> Drop for CommitReservationReleaseGuard<'_, S> {
    fn drop(&mut self) {
        if !self.released {
            if let Some(reservation) = self.reservation.take() {
                let _ = self.storage.release_commit_lsn(reservation);
            }
        }
    }
}

impl<S: TxnStoragePort, L: ClogService + 'static, T: TsoService> TransactionService<S, L, T> {
    /// Create a new transaction service
    pub fn new(
        storage: Arc<S>,
        clog_service: Arc<L>,
        tso: Arc<T>,
        concurrency_manager: Arc<ConcurrencyManager>,
    ) -> Self {
        Self::new_with_conflict_wait_config(
            storage,
            clog_service,
            tso,
            concurrency_manager,
            ConflictWaitConfig::default(),
        )
    }

    /// Create a new transaction service with an explicit conflict wait policy.
    pub fn new_with_conflict_wait_config(
        storage: Arc<S>,
        clog_service: Arc<L>,
        tso: Arc<T>,
        concurrency_manager: Arc<ConcurrencyManager>,
        conflict_wait_config: ConflictWaitConfig,
    ) -> Self {
        Self {
            storage,
            clog_service,
            tso,
            concurrency_manager,
            conflict_wait_config,
            write_path_diagnostics: Arc::new(WritePathDiagnostics::default()),
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

    /// Convenience single-statement put for concrete callers.
    pub async fn put(
        &self,
        ctx: &mut TxnCtx,
        table_id: TableId,
        key: &[u8],
        value: RawValue,
    ) -> Result<()> {
        let mut stmt = <Self as TxnService>::begin_statement(self, ctx);
        <Self as TxnService>::put(self, ctx, &mut stmt, table_id, key, value).await
    }

    /// Convenience single-statement delete for concrete callers.
    pub async fn delete(&self, ctx: &mut TxnCtx, table_id: TableId, key: &[u8]) -> Result<()> {
        let mut stmt = <Self as TxnService>::begin_statement(self, ctx);
        <Self as TxnService>::delete(self, ctx, &mut stmt, table_id, key).await
    }

    /// Convenience single-statement lock for concrete callers.
    pub async fn lock_key(&self, ctx: &mut TxnCtx, table_id: TableId, key: &[u8]) -> Result<()> {
        let mut stmt = <Self as TxnService>::begin_statement(self, ctx);
        <Self as TxnService>::lock_key(self, ctx, &mut stmt, table_id, key).await
    }

    async fn retry_stage_error(
        &self,
        err: TxnStageError,
        conflict_backoff: &mut ConflictBackoff,
        retries: &mut usize,
    ) -> Result<bool> {
        const MAX_SLOWDOWN_RETRIES: usize = 32;
        match err {
            TxnStageError::LockConflict { .. } => match conflict_backoff.wait_next().await {
                ConflictWaitOutcome::RetryNow => Ok(true),
                ConflictWaitOutcome::TimedOut => Err(TiSqlError::LockWaitTimeout),
            },
            TxnStageError::WriteStall { delay: Some(delay) } => {
                if *retries >= MAX_SLOWDOWN_RETRIES {
                    return Err(TiSqlError::Storage(
                        "Write stalled: storage backpressure, retry later".to_string(),
                    ));
                }
                *retries += 1;
                tokio::time::sleep(delay).await;
                Ok(true)
            }
            TxnStageError::WriteStall { delay: None } => Err(TiSqlError::Storage(
                "Write stalled: storage backpressure, retry later".to_string(),
            )),
        }
    }

    async fn stage_put_with_retry(
        &self,
        key: &[u8],
        value: &[u8],
        owner_start_ts: Timestamp,
    ) -> Result<()> {
        let mut retries = 0;
        let mut conflict_backoff = ConflictBackoff::new(self.conflict_wait_config);

        loop {
            match self.storage.stage_put(key, value, owner_start_ts) {
                Ok(()) => return Ok(()),
                Err(err)
                    if self
                        .retry_stage_error(err, &mut conflict_backoff, &mut retries)
                        .await? =>
                {
                    continue
                }
                Err(_) => unreachable!("retry_stage_error returns on terminal stage errors"),
            }
        }
    }

    async fn stage_delete_with_retry(
        &self,
        key: &[u8],
        owner_start_ts: Timestamp,
    ) -> Result<TxnDeleteEffect> {
        let mut retries = 0;
        let mut conflict_backoff = ConflictBackoff::new(self.conflict_wait_config);

        loop {
            match self.storage.stage_delete(key, owner_start_ts).await {
                Ok(effect) => return Ok(effect),
                Err(err)
                    if self
                        .retry_stage_error(err, &mut conflict_backoff, &mut retries)
                        .await? =>
                {
                    continue
                }
                Err(_) => unreachable!("retry_stage_error returns on terminal stage errors"),
            }
        }
    }

    async fn stage_lock_with_retry(&self, key: &[u8], owner_start_ts: Timestamp) -> Result<()> {
        let mut retries = 0;
        let mut conflict_backoff = ConflictBackoff::new(self.conflict_wait_config);

        loop {
            match self.storage.stage_lock(key, owner_start_ts) {
                Ok(()) => return Ok(()),
                Err(err)
                    if self
                        .retry_stage_error(err, &mut conflict_backoff, &mut retries)
                        .await? =>
                {
                    continue
                }
                Err(_) => unreachable!("retry_stage_error returns on terminal stage errors"),
            }
        }
    }

    async fn restore_pending_same_owner(
        &self,
        key: &[u8],
        previous: &MutationMeta,
        owner_start_ts: Timestamp,
    ) -> Result<()> {
        let mut retries = 0;
        let mut conflict_backoff = ConflictBackoff::new(self.conflict_wait_config);

        loop {
            let mutation = match &previous.mutation {
                MutationPayload::Put(value) => TxnRestoreMutation::Put(value.as_slice()),
                MutationPayload::Delete => TxnRestoreMutation::Delete,
                MutationPayload::Lock => TxnRestoreMutation::Lock,
            };

            match self.storage.restore_pending(key, mutation, owner_start_ts) {
                Ok(()) => return Ok(()),
                Err(TxnStageError::LockConflict {
                    owner_start_ts: actual_owner,
                }) => {
                    return Err(TiSqlError::Internal(format!(
                        "statement rollback restore owner mismatch for key {key:?}: expected owner {owner_start_ts}, found {actual_owner}"
                    )));
                }
                Err(err)
                    if self
                        .retry_stage_error(err, &mut conflict_backoff, &mut retries)
                        .await? =>
                {
                    continue
                }
                Err(_) => unreachable!("retry_stage_error returns on terminal stage errors"),
            }
        }
    }

    /// Execute a write batch with durability guarantees (for direct/autocommit writes).
    ///
    /// Recover state by replaying commit log entries.
    ///
    /// This should be called at startup to rebuild the in-memory state
    /// from the durable commit log.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn recover(&self, entries: &[ClogEntry]) -> Result<RecoveryStats>
    where
        S: TxnStorageRecoveryPort,
    {
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
                    self.storage.apply_recovered_write(
                        RecoveredWrite::Put { key, value },
                        commit_ts,
                        lsn,
                    )?;
                    stats.applied_puts += 1;
                }
                ClogOp::Delete { key } => {
                    self.storage.apply_recovered_write(
                        RecoveredWrite::Delete { key },
                        commit_ts,
                        lsn,
                    )?;
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

    pub fn write_path_metric_pairs(&self) -> Vec<(String, String)> {
        self.write_path_diagnostics.metric_pairs()
    }

    /// Check if transaction is in active state
    fn check_active(ctx: &TxnCtx) -> Result<()> {
        match ctx.state {
            TxnState::Running => Ok(()),
            TxnState::Preparing => Err(crate::util::error::TiSqlError::Internal(
                "Transaction in preparing state".into(),
            )),
            TxnState::Prepared { .. } => Err(crate::util::error::TiSqlError::Internal(
                "Transaction already in prepared state".into(),
            )),
            TxnState::Committed { .. } => Err(crate::util::error::TiSqlError::Internal(
                "Transaction already committed".into(),
            )),
            TxnState::Aborted => Err(crate::util::error::TiSqlError::Internal(
                "Transaction already aborted".into(),
            )),
        }
    }

    fn validate_table_target_key(table_id: TableId, key: &[u8], op_name: &str) -> Result<()> {
        let decoded = decode_table_id(key);
        if table_id < USER_TABLE_ID_START {
            // System-table operations may use non-table-key metadata prefixes.
            if decoded.is_ok_and(|id| id >= USER_TABLE_ID_START) {
                return Err(TiSqlError::Storage(format!(
                    "{op_name} routed to system table id {table_id}, but key belongs to user table space"
                )));
            }
            return Ok(());
        }

        // User-table operations are metadata-first and require encoded table keys.
        let decoded_table_id = decoded.map_err(|_| {
            TiSqlError::Storage(format!(
                "{op_name} requires encoded user-table key for table_id={table_id}"
            ))
        })?;
        if decoded_table_id != table_id {
            return Err(TiSqlError::Storage(format!(
                "{op_name} table_id/key mismatch: target={table_id}, key_table_id={decoded_table_id}"
            )));
        }
        Ok(())
    }

    fn mutation_keys(mutations: BTreeMap<Key, MutationMeta>) -> Vec<Key> {
        mutations.into_keys().collect()
    }

    fn split_mutation_keys_by_type(mutations: BTreeMap<Key, MutationMeta>) -> (Vec<Key>, Vec<Key>) {
        let mut write_keys = Vec::new();
        let mut lock_only_keys = Vec::new();
        for (key, mutation_meta) in mutations {
            if mutation_meta.mutation.is_write() {
                write_keys.push(key);
            } else {
                lock_only_keys.push(key);
            }
        }
        (write_keys, lock_only_keys)
    }
}

/// Implement TxnService trait for TransactionService
impl<S: TxnStoragePort + 'static, L: ClogService + 'static, T: TsoService> TxnService
    for TransactionService<S, L, T>
{
    type ScanCursor = TxnScanCursorAdapter<S::ScanCursor>;

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

    fn record_duplicate_check_duration(&self, duration: Duration) {
        self.write_path_diagnostics
            .duplicate_check
            .record_duration(duration);
    }

    async fn get(&self, ctx: &TxnCtx, table_id: TableId, key: &[u8]) -> Result<Option<RawValue>> {
        Self::validate_table_target_key(table_id, key, "TxnService::get")?;
        // Keep point-get ownership semantics aligned with scan_iter:
        // - explicit txn reads can see their own pending writes
        // - implicit txn reads use owner=0 to avoid intra-statement self-visibility
        let owner_ts = if ctx.is_explicit() { ctx.start_ts } else { 0 };
        let mut conflict_backoff = ConflictBackoff::new(self.conflict_wait_config);

        loop {
            match self
                .storage
                .read_point(TxnPointRead {
                    key,
                    read_ts: ctx.start_ts,
                    owner_ts,
                })
                .await?
            {
                PointReadResult::Missing | PointReadResult::Deleted => return Ok(None),
                PointReadResult::Value(value) => return Ok(Some(value)),
                PointReadResult::Blocked { .. } => match conflict_backoff.wait_next().await {
                    ConflictWaitOutcome::RetryNow => continue,
                    ConflictWaitOutcome::TimedOut => return Err(TiSqlError::LockWaitTimeout),
                },
            }
        }
    }

    fn scan(&self, ctx: &TxnCtx, table_id: TableId, range: Range<Key>) -> Result<Self::ScanCursor> {
        if !range.start.is_empty() {
            Self::validate_table_target_key(table_id, &range.start, "TxnService::scan")?;
        }

        // Explicit transactions use owner_ts=start_ts for read-your-writes
        // (see pending writes from prior statements in the same txn).
        // Implicit transactions use owner_ts=0 because writes within a single
        // statement must not be visible to the ongoing scan (e.g., UPDATE SET a=a+10
        // must not re-scan rows it just inserted). Pending resolution via
        // concurrency_manager handles other txns' pending writes for both paths.
        let owner_ts = if ctx.is_explicit() { ctx.start_ts } else { 0 };
        let storage_cursor = self.storage.scan(TxnStorageScanRequest {
            range,
            read_ts: ctx.start_ts,
            owner_ts,
        })?;

        Ok(TxnScanCursorAdapter::new(
            storage_cursor,
            self.conflict_wait_config,
        ))
    }

    fn begin_statement(&self, _ctx: &mut TxnCtx) -> StatementGuard {
        StatementGuard::default()
    }

    async fn put(
        &self,
        ctx: &mut TxnCtx,
        stmt: &mut StatementGuard,
        table_id: TableId,
        key: &[u8],
        value: RawValue,
    ) -> Result<()> {
        if ctx.state != TxnState::Running {
            return Err(TiSqlError::TransactionNotActive(format!("{:?}", ctx.state)));
        }
        if ctx.read_only {
            return Err(TiSqlError::ReadOnlyTransaction);
        }
        Self::validate_table_target_key(table_id, key, "TxnService::put")?;

        let record_undo = ctx.is_explicit() && !stmt.contains_key(key);
        let previous = record_undo
            .then(|| ctx.mutations.get(key).cloned())
            .flatten();

        // Register in state cache BEFORE writing pending node so concurrent readers
        // can always resolve our pending writes via TxnStateCache.
        if !ctx.registered {
            self.concurrency_manager.register_txn(ctx.start_ts);
            ctx.registered = true;
        }

        let put_begin = Instant::now();
        let put_result = self.stage_put_with_retry(key, &value, ctx.start_ts).await;
        self.write_path_diagnostics
            .put_pending
            .record_duration(put_begin.elapsed());
        match put_result {
            Ok(()) => {
                let key = key.to_vec();
                // Single insert handles both: adding a new Write entry AND
                // upgrading Lock→Write (overwrite) if key had a prior LOCK.
                ctx.mutations.insert(
                    key.clone(),
                    MutationMeta {
                        mutation: MutationPayload::Put(value),
                    },
                );
                if record_undo {
                    stmt.record_first_touch(key.as_slice(), previous);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn delete(
        &self,
        ctx: &mut TxnCtx,
        stmt: &mut StatementGuard,
        table_id: TableId,
        key: &[u8],
    ) -> Result<()> {
        if ctx.state != TxnState::Running {
            return Err(TiSqlError::TransactionNotActive(format!("{:?}", ctx.state)));
        }
        if ctx.read_only {
            return Err(TiSqlError::ReadOnlyTransaction);
        }
        Self::validate_table_target_key(table_id, key, "TxnService::delete")?;

        let record_undo = ctx.is_explicit() && !stmt.contains_key(key);
        let previous = record_undo
            .then(|| ctx.mutations.get(key).cloned())
            .flatten();

        // Register before writing pending (same reason as put()).
        if !ctx.registered {
            self.concurrency_manager.register_txn(ctx.start_ts);
            ctx.registered = true;
        }

        let delete_begin = Instant::now();
        match self.stage_delete_with_retry(key, ctx.start_ts).await? {
            TxnDeleteEffect::Delete => {
                self.write_path_diagnostics
                    .put_pending
                    .record_duration(delete_begin.elapsed());
                let key = key.to_vec();
                ctx.mutations.insert(
                    key.clone(),
                    MutationMeta {
                        mutation: MutationPayload::Delete,
                    },
                );
                if record_undo {
                    stmt.record_first_touch(key.as_slice(), previous);
                }
                Ok(())
            }
            TxnDeleteEffect::Lock => {
                self.write_path_diagnostics
                    .lock_pending
                    .record_duration(delete_begin.elapsed());
                let key = key.to_vec();
                ctx.mutations.entry(key.clone()).or_insert(MutationMeta {
                    mutation: MutationPayload::Lock,
                });
                if record_undo {
                    stmt.record_first_touch(key.as_slice(), previous);
                }
                Ok(())
            }
        }
    }

    async fn lock_key(
        &self,
        ctx: &mut TxnCtx,
        stmt: &mut StatementGuard,
        table_id: TableId,
        key: &[u8],
    ) -> Result<()> {
        if ctx.state != TxnState::Running {
            return Err(TiSqlError::TransactionNotActive(format!("{:?}", ctx.state)));
        }
        if ctx.read_only {
            return Err(TiSqlError::ReadOnlyTransaction);
        }
        Self::validate_table_target_key(table_id, key, "TxnService::lock_key")?;

        let record_undo =
            ctx.is_explicit() && !stmt.contains_key(key) && !ctx.mutations.contains_key(key);

        // Existing write mutation already protects this key; don't downgrade.
        if ctx
            .mutations
            .get(key)
            .is_some_and(|meta| meta.mutation.is_write())
        {
            return Ok(());
        }
        // Existing lock mutation is already in place.
        if ctx
            .mutations
            .get(key)
            .is_some_and(|meta| meta.mutation.is_lock())
        {
            return Ok(());
        }

        if !ctx.registered {
            self.concurrency_manager.register_txn(ctx.start_ts);
            ctx.registered = true;
        }

        let lock_begin = Instant::now();
        let lock_result = self.stage_lock_with_retry(key, ctx.start_ts).await;
        self.write_path_diagnostics
            .lock_pending
            .record_duration(lock_begin.elapsed());
        match lock_result {
            Ok(()) => {
                let key = key.to_vec();
                ctx.mutations.entry(key.clone()).or_insert(MutationMeta {
                    mutation: MutationPayload::Lock,
                });
                if record_undo && ctx.mutations.contains_key(key.as_slice()) {
                    stmt.record_first_touch(key.as_slice(), None);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn rollback_statement(&self, ctx: &mut TxnCtx, stmt: StatementGuard) -> Result<()> {
        if ctx.state != TxnState::Running {
            return Err(TiSqlError::TransactionNotActive(format!("{:?}", ctx.state)));
        }

        if stmt.is_empty() {
            return Ok(());
        }

        let mut restore_entries = Vec::new();
        let mut absent_entries = Vec::new();
        for (key, entry) in stmt.into_entries() {
            match entry {
                StatementUndoEntry::Restore(previous) => restore_entries.push((key, previous)),
                StatementUndoEntry::Absent => absent_entries.push(key),
            }
        }

        for (key, previous) in restore_entries {
            ctx.mutations.get(&key).ok_or_else(|| {
                TiSqlError::Internal(format!(
                    "statement rollback missing current mutation for key {key:?}"
                ))
            })?;
            self.restore_pending_same_owner(&key, &previous, ctx.start_ts)
                .await?;
            ctx.mutations.insert(key, previous);
        }

        for key in &absent_entries {
            ctx.mutations.get(key).ok_or_else(|| {
                TiSqlError::Internal(format!(
                    "statement rollback missing current mutation for key {key:?}"
                ))
            })?;
        }

        if !absent_entries.is_empty() {
            self.storage.abort(AbortRequest {
                owner_start_ts: ctx.start_ts,
                keys: &absent_entries,
            })?;
        }

        for key in absent_entries {
            ctx.mutations.remove(&key);
        }

        Ok(())
    }

    async fn commit(&self, mut ctx: TxnCtx) -> Result<CommitInfo> {
        Self::check_active(&ctx)?;
        let commit_begin = Instant::now();

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

        // Build ClogOpRef entries directly from transaction-tracked mutations.
        // This avoids commit-time storage readback for write keys.
        let read_values_begin = Instant::now();
        let mut ops: Vec<ClogOpRef<'_>> = Vec::with_capacity(mutations.len());
        for (key, mutation_meta) in &mutations {
            match &mutation_meta.mutation {
                MutationPayload::Put(value) => ops.push(ClogOpRef::Put {
                    key,
                    value: value.as_slice(),
                }),
                MutationPayload::Delete => ops.push(ClogOpRef::Delete { key }),
                MutationPayload::Lock => {}
            }
        }
        self.write_path_diagnostics
            .commit_read_values
            .record_duration(read_values_begin.elapsed());

        // FAILPOINT: After locks acquired (pending nodes written), before commit_ts computation
        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_lock_before_commit_ts");

        // Step 1: Signal that we're computing commit_ts.
        // Readers encountering our pending nodes will spin-wait until Prepared.
        let prepare_begin = Instant::now();
        self.concurrency_manager.set_preparing_txn(start_ts)?;

        // Step 2: Compute commit_ts (safe: readers spin on Preparing)
        let max_ts = self.concurrency_manager.max_ts();
        let tso_ts = self.get_ts();
        let commit_ts = std::cmp::max(max_ts + 1, tso_ts);

        // Step 3: Set prepared_ts for readers to check visibility
        self.concurrency_manager.prepare_txn(start_ts, commit_ts)?;
        self.write_path_diagnostics
            .commit_prepare_ts
            .record_duration(prepare_begin.elapsed());

        // Step 4: `ops` already borrows keys/values from mutation tracking.
        let has_wal_ops = !ops.is_empty();

        // V2.6: reserve commit LSN ahead of WAL write.
        let reservation = self.storage.reserve_commit_lsn(start_ts);
        let txn_lsn = reservation.lsn;
        ctx.reserved_lsn = Some(txn_lsn);
        let mut reservation_guard = Some(CommitReservationReleaseGuard::new(
            self.storage.as_ref(),
            reservation,
        ));

        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_alloc_and_reserve_before_clog_write_v26");

        // Step 5: Write to clog for durability (serializes synchronously, returns
        // future for fsync). After this call, ops are no longer needed.
        let clog_write_begin = Instant::now();
        let clog_result = self
            .clog_service
            .write_ops(txn_id, &ops, commit_ts, Some(txn_lsn), true)
            .await;
        self.write_path_diagnostics
            .commit_clog_write
            .record_duration(clog_write_begin.elapsed());

        #[cfg(feature = "failpoints")]
        fail_point!("txn_after_clog_write_before_fsync_wait_v26");

        // Release borrows on mutations (ops → mutations).
        drop(ops);
        let mut pending_mutations = Some(mutations);

        match clog_result {
            Ok(fsync_future) => {
                let fsync_wait_begin = Instant::now();
                let fsync_result = fsync_future.await;
                self.write_path_diagnostics
                    .commit_fsync_wait
                    .record_duration(fsync_wait_begin.elapsed());
                match fsync_result {
                    Ok(lsn) => {
                        #[cfg(feature = "failpoints")]
                        fail_point!("txn_after_clog_fsync_before_finalize_v26");

                        if has_wal_ops {
                            debug_assert_eq!(
                                txn_lsn, lsn,
                                "clog returned lsn differs from reserved lsn (start_ts={start_ts})",
                            );
                        }
                        let commit_lsn = txn_lsn;
                        let (write_keys, lock_only_keys) = Self::split_mutation_keys_by_type(
                            pending_mutations
                                .take()
                                .expect("commit mutation set should be available exactly once"),
                        );
                        let finalize_begin = Instant::now();

                        // Step 6: Finalize write keys and abort lock-only keys so LOCK
                        // sentinels never become committed user-visible versions.
                        self.storage.finalize(FinalizeRequest {
                            owner_start_ts: start_ts,
                            commit_ts,
                            reservation: CommitReservation {
                                owner_start_ts: start_ts,
                                lsn: commit_lsn,
                            },
                            write_keys: &write_keys,
                            lock_only_keys: &lock_only_keys,
                        })?;

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
                        self.write_path_diagnostics
                            .commit_finalize
                            .record_duration(finalize_begin.elapsed());
                        self.write_path_diagnostics
                            .commit_total
                            .record_duration(commit_begin.elapsed());
                        self.write_path_diagnostics
                            .commit_success
                            .fetch_add(1, Ordering::Relaxed);
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
                        let keys = Self::mutation_keys(
                            pending_mutations
                                .take()
                                .expect("commit mutation set should be available exactly once"),
                        );
                        self.storage.abort(AbortRequest {
                            owner_start_ts: start_ts,
                            keys: &keys,
                        })?;
                        self.concurrency_manager.abort_txn(start_ts).ok();
                        self.concurrency_manager.remove_txn(start_ts);
                        self.write_path_diagnostics
                            .commit_total
                            .record_duration(commit_begin.elapsed());
                        self.write_path_diagnostics
                            .commit_failure
                            .fetch_add(1, Ordering::Relaxed);
                        Err(e)
                    }
                }
            }
            Err(e) => {
                if let Some(guard) = reservation_guard.take() {
                    let _ = guard.release();
                }
                ctx.reserved_lsn = None;
                let keys = Self::mutation_keys(
                    pending_mutations
                        .take()
                        .expect("commit mutation set should be available exactly once"),
                );
                self.storage.abort(AbortRequest {
                    owner_start_ts: start_ts,
                    keys: &keys,
                })?;
                self.concurrency_manager.abort_txn(start_ts).ok();
                self.concurrency_manager.remove_txn(start_ts);
                self.write_path_diagnostics
                    .commit_total
                    .record_duration(commit_begin.elapsed());
                self.write_path_diagnostics
                    .commit_failure
                    .fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    fn rollback(&self, mut ctx: TxnCtx) -> Result<()> {
        Self::check_active(&ctx)?;

        let mutations = std::mem::take(&mut ctx.mutations);

        // Abort all pending nodes in storage
        if !mutations.is_empty() {
            let keys = Self::mutation_keys(mutations);
            self.storage.abort(AbortRequest {
                owner_start_ts: ctx.start_ts,
                keys: &keys,
            })?;
        }

        if let Some(lsn) = ctx.reserved_lsn.take() {
            let _ = self.storage.release_commit_lsn(CommitReservation {
                owner_start_ts: ctx.start_ts,
                lsn,
            });
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
// Public logical scan cursor backed by TxnStoragePort scan steps
// ============================================================================

pub struct TxnScanCursorAdapter<C: TxnStorageScanCursor> {
    cursor: C,
    current: Option<TxnScanEntry>,
    conflict_wait_config: ConflictWaitConfig,
}

impl<C: TxnStorageScanCursor> TxnScanCursorAdapter<C> {
    fn new(cursor: C, conflict_wait_config: ConflictWaitConfig) -> Self {
        Self {
            cursor,
            current: None,
            conflict_wait_config,
        }
    }

    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    pub fn user_key(&self) -> &[u8] {
        self.current
            .as_ref()
            .expect("user_key() called on invalid cursor")
            .user_key
            .as_slice()
    }

    pub fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .expect("value() called on invalid cursor")
            .value
            .as_slice()
    }
}

impl<C: TxnStorageScanCursor> TxnScanCursor for TxnScanCursorAdapter<C> {
    async fn advance(&mut self) -> Result<()> {
        let mut conflict_backoff = ConflictBackoff::new(self.conflict_wait_config);
        loop {
            match self.cursor.next_step().await? {
                TxnStorageScanStep::Entry(entry) => match entry.state {
                    TxnStorageVisibleState::Value(value) => {
                        self.current = Some(TxnScanEntry {
                            user_key: entry.user_key,
                            value,
                        });
                        return Ok(());
                    }
                    TxnStorageVisibleState::Deleted => continue,
                },
                TxnStorageScanStep::Blocked(_) => match conflict_backoff.wait_next().await {
                    ConflictWaitOutcome::RetryNow => continue,
                    ConflictWaitOutcome::TimedOut => {
                        self.current = None;
                        return Err(TiSqlError::LockWaitTimeout);
                    }
                },
                TxnStorageScanStep::Exhausted => {
                    self.current = None;
                    return Ok(());
                }
            }
        }
    }

    fn current(&self) -> Option<&TxnScanEntry> {
        self.current.as_ref()
    }
}

/// Statistics from recovery
#[cfg_attr(not(test), allow(dead_code))]
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
    #[allow(dead_code)]
    pub post_commit_ops: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
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
    use crate::inner_table::core_tables::ALL_META_TABLE_ID;
    use crate::kernel::txn_storage::{
        AbortRequest, CommitReservation, FinalizeRequest, PointReadResult, TxnDeleteEffect,
        TxnPointRead, TxnRestoreMutation, TxnStageError, TxnStoragePort, TxnStorageScanCursor,
        TxnStorageScanEntry, TxnStorageScanRequest, TxnStorageScanStep, TxnStorageVisibleState,
    };
    use crate::lsn::new_lsn_provider;
    use crate::tablet::routed_storage::TxnStorageScanAdapter;
    use crate::tablet::{
        is_lock, is_tombstone, route_table_to_tablet, IlogConfig, IlogService, LsmConfig,
        MemTableEngine, MvccIterator, MvccKey, PessimisticStorage, StorageEngine, TabletEngine,
        TabletTxnStorage, Version, WriteBatch, LOCK,
    };
    use crate::testkit::TxnServiceTestExt;
    use crate::transaction::{TxnScanCursor, TxnService as PublicTxnService};
    use crate::tso::LocalTso;
    use std::collections::VecDeque;
    use std::future::Future;
    use std::sync::atomic::Ordering as AtomicOrdering;
    use std::time::Duration;
    use tempfile::tempdir;

    type TestStorage = MemTableEngine;
    type TestTxnStorage = TabletTxnStorage<TestStorage>;

    fn make_test_io() -> Arc<crate::io::IoService> {
        crate::io::IoService::new_for_test(32).unwrap()
    }

    fn create_test_service() -> (
        Arc<TestStorage>,
        TransactionService<TestTxnStorage, FileClogService, LocalTso>,
        tempfile::TempDir,
    ) {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let io_handle = tokio::runtime::Handle::current();
        let storage = Arc::new(MemTableEngine::new());
        let clog_service = Arc::new(
            FileClogService::open_with_lsn_provider(
                config,
                storage.lsn_provider(),
                make_test_io(),
                &io_handle,
            )
            .unwrap(),
        );
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let txn_storage = Arc::new(TabletTxnStorage::new(Arc::clone(&storage), Arc::clone(&cm)));
        let txn_service = TransactionService::new_with_conflict_wait_config(
            txn_storage,
            clog_service,
            tso,
            cm,
            ConflictWaitConfig {
                lock_wait_timeout: Duration::from_millis(100),
                spin_retries: 0,
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(10),
            },
        );
        (storage, txn_service, dir)
    }

    fn create_lsm_test_service_with_config<F>(
        configure: F,
    ) -> (
        Arc<TabletEngine>,
        TransactionService<TabletTxnStorage<TabletEngine>, FileClogService, LocalTso>,
        tempfile::TempDir,
    )
    where
        F: FnOnce(&std::path::Path) -> LsmConfig,
    {
        let dir = tempdir().unwrap();
        let tablet_dir = dir.path().join("tablet");
        let config = FileClogConfig::with_dir(dir.path().join("clog"));
        let io_handle = tokio::runtime::Handle::current();
        let lsn_provider = new_lsn_provider();
        let ilog = Arc::new(
            IlogService::open_with_thread(
                IlogConfig::new(tablet_dir.clone()),
                Arc::clone(&lsn_provider),
            )
            .unwrap(),
        );
        let storage = Arc::new(
            TabletEngine::open_with_recovery(
                configure(tablet_dir.as_path()),
                Arc::clone(&lsn_provider),
                ilog,
                Version::new(),
                make_test_io(),
            )
            .unwrap(),
        );
        let clog_service = Arc::new(
            FileClogService::open_with_lsn_provider(
                config,
                lsn_provider,
                make_test_io(),
                &io_handle,
            )
            .unwrap(),
        );
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let txn_storage = Arc::new(TabletTxnStorage::new(Arc::clone(&storage), Arc::clone(&cm)));
        let txn_service = TransactionService::new_with_conflict_wait_config(
            txn_storage,
            clog_service,
            tso,
            cm,
            ConflictWaitConfig {
                lock_wait_timeout: Duration::from_millis(100),
                spin_retries: 0,
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(10),
            },
        );
        (storage, txn_service, dir)
    }

    fn create_lsm_test_service() -> (
        Arc<TabletEngine>,
        TransactionService<TabletTxnStorage<TabletEngine>, FileClogService, LocalTso>,
        tempfile::TempDir,
    ) {
        create_lsm_test_service_with_config(|tablet_dir| LsmConfig::new(tablet_dir.to_path_buf()))
    }

    #[derive(Clone)]
    struct MockTxnStorage {
        scan_steps: Vec<TxnStorageScanStep>,
    }

    impl MockTxnStorage {
        fn new(scan_steps: Vec<TxnStorageScanStep>) -> Self {
            Self { scan_steps }
        }
    }

    struct MockScanCursor {
        steps: VecDeque<TxnStorageScanStep>,
    }

    impl TxnStorageScanCursor for MockScanCursor {
        async fn next_step(&mut self) -> Result<TxnStorageScanStep> {
            Ok(self
                .steps
                .pop_front()
                .unwrap_or(TxnStorageScanStep::Exhausted))
        }
    }

    impl TxnStoragePort for MockTxnStorage {
        type ScanCursor = MockScanCursor;

        fn read_point<'a>(
            &'a self,
            _req: TxnPointRead<'a>,
        ) -> impl Future<Output = Result<PointReadResult>> + Send + 'a {
            std::future::ready(Ok(PointReadResult::Missing))
        }

        fn scan(&self, _req: TxnStorageScanRequest) -> Result<Self::ScanCursor> {
            Ok(MockScanCursor {
                steps: self.scan_steps.iter().cloned().collect(),
            })
        }

        fn stage_put(
            &self,
            _key: &[u8],
            _value: &[u8],
            _owner_start_ts: Timestamp,
        ) -> std::result::Result<(), TxnStageError> {
            unreachable!("mock scan storage does not stage writes")
        }

        async fn stage_delete<'a>(
            &'a self,
            _key: &'a [u8],
            _owner_start_ts: Timestamp,
        ) -> std::result::Result<TxnDeleteEffect, TxnStageError> {
            unreachable!("mock scan storage does not stage writes")
        }

        fn stage_lock(
            &self,
            _key: &[u8],
            _owner_start_ts: Timestamp,
        ) -> std::result::Result<(), TxnStageError> {
            unreachable!("mock scan storage does not stage writes")
        }

        fn restore_pending(
            &self,
            _key: &[u8],
            _mutation: TxnRestoreMutation<'_>,
            _owner_start_ts: Timestamp,
        ) -> std::result::Result<(), TxnStageError> {
            unreachable!("mock scan storage does not stage writes")
        }

        fn reserve_commit_lsn(&self, owner_start_ts: Timestamp) -> CommitReservation {
            CommitReservation {
                owner_start_ts,
                lsn: 1,
            }
        }

        fn release_commit_lsn(&self, _reservation: CommitReservation) -> bool {
            true
        }

        fn finalize(&self, _req: FinalizeRequest<'_>) -> Result<()> {
            Ok(())
        }

        fn abort(&self, _req: AbortRequest<'_>) -> Result<()> {
            Ok(())
        }
    }

    fn create_mock_scan_service(
        scan_steps: Vec<TxnStorageScanStep>,
        conflict_wait_config: ConflictWaitConfig,
    ) -> (
        TransactionService<MockTxnStorage, FileClogService, LocalTso>,
        tempfile::TempDir,
    ) {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let io_handle = tokio::runtime::Handle::current();
        let clog_service = Arc::new(
            FileClogService::open_with_lsn_provider(
                config,
                crate::lsn::new_lsn_provider(),
                make_test_io(),
                &io_handle,
            )
            .unwrap(),
        );
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let txn_storage = Arc::new(MockTxnStorage::new(scan_steps));
        let txn_service = TransactionService::new_with_conflict_wait_config(
            txn_storage,
            clog_service,
            tso,
            cm,
            conflict_wait_config,
        );
        (txn_service, dir)
    }

    fn write_committed_value(
        storage: &TabletEngine,
        key: &[u8],
        commit_ts: Timestamp,
        value_len: usize,
    ) {
        let mut batch = WriteBatch::new();
        batch.set_commit_ts(commit_ts);
        batch.put(key.to_vec(), vec![b'x'; value_len]);
        storage.write_batch(batch).unwrap();
    }

    struct MvccScanIterator<I: MvccIterator> {
        cursor: TxnStorageScanAdapter<I>,
        current: Option<TxnScanEntry>,
        conflict_wait_config: ConflictWaitConfig,
    }

    impl<I: MvccIterator> MvccScanIterator<I> {
        fn new(
            iter: I,
            read_ts: Timestamp,
            range: Range<Key>,
            concurrency_manager: Option<Arc<ConcurrencyManager>>,
            conflict_wait_config: ConflictWaitConfig,
        ) -> Self {
            let cm = concurrency_manager.unwrap_or_else(|| Arc::new(ConcurrencyManager::new(0)));
            Self {
                cursor: TxnStorageScanAdapter::new(iter, read_ts, range, cm),
                current: None,
                conflict_wait_config,
            }
        }
    }

    impl<I: MvccIterator> MvccIterator for MvccScanIterator<I> {
        async fn seek(&mut self, _target: &MvccKey) -> Result<()> {
            Ok(())
        }

        async fn advance(&mut self) -> Result<()> {
            let mut conflict_backoff = ConflictBackoff::new(self.conflict_wait_config);
            loop {
                match self.cursor.next_step().await? {
                    TxnStorageScanStep::Entry(entry) => match entry.state {
                        TxnStorageVisibleState::Value(value) => {
                            self.current = Some(TxnScanEntry {
                                user_key: entry.user_key,
                                value,
                            });
                            return Ok(());
                        }
                        TxnStorageVisibleState::Deleted => continue,
                    },
                    TxnStorageScanStep::Blocked(_) => match conflict_backoff.wait_next().await {
                        ConflictWaitOutcome::RetryNow => continue,
                        ConflictWaitOutcome::TimedOut => {
                            self.current = None;
                            return Err(TiSqlError::LockWaitTimeout);
                        }
                    },
                    TxnStorageScanStep::Exhausted => {
                        self.current = None;
                        return Ok(());
                    }
                }
            }
        }

        fn valid(&self) -> bool {
            self.current.is_some()
        }

        fn user_key(&self) -> &[u8] {
            self.current
                .as_ref()
                .expect("user_key() called on invalid iterator")
                .user_key
                .as_slice()
        }

        fn timestamp(&self) -> Timestamp {
            0
        }

        fn value(&self) -> &[u8] {
            self.current
                .as_ref()
                .expect("value() called on invalid iterator")
                .value
                .as_slice()
        }
    }

    async fn collect_public_scan<T: PublicTxnService>(
        txn_service: &T,
        ctx: &TxnCtx,
        table_id: TableId,
        range: Range<Key>,
    ) -> Result<Vec<(Key, RawValue)>> {
        let mut cursor = txn_service.scan(ctx, table_id, range)?;
        let mut rows = Vec::new();

        cursor.advance().await?;
        while let Some(entry) = cursor.current() {
            rows.push((entry.user_key.clone(), entry.value.clone()));
            cursor.advance().await?;
        }

        Ok(rows)
    }

    async fn public_put_and_commit<T: PublicTxnService>(
        txn_service: &T,
        table_id: TableId,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let mut ctx = txn_service.begin(false)?;
        let mut stmt = txn_service.begin_statement(&mut ctx);
        PublicTxnService::put(
            txn_service,
            &mut ctx,
            &mut stmt,
            table_id,
            key,
            value.to_vec(),
        )
        .await?;
        txn_service.commit(ctx).await?;
        Ok(())
    }

    #[test]
    fn test_conflict_wait_config_builder_overrides_timeout() {
        let config =
            ConflictWaitConfig::default().with_lock_wait_timeout(Duration::from_millis(25));
        assert_eq!(config.lock_wait_timeout, Duration::from_millis(25));
    }

    #[tokio::test(start_paused = true)]
    async fn test_conflict_backoff_covers_spin_sleep_and_timeout_paths() {
        let mut spin_backoff = ConflictBackoff::new(ConflictWaitConfig {
            lock_wait_timeout: Duration::from_secs(1),
            spin_retries: 1,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        });
        assert_eq!(
            spin_backoff.wait_next().await,
            ConflictWaitOutcome::RetryNow
        );

        let mut sleep_backoff = ConflictBackoff::new(ConflictWaitConfig {
            lock_wait_timeout: Duration::from_secs(1),
            spin_retries: 0,
            initial_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(10),
        });
        assert_eq!(
            sleep_backoff.wait_next().await,
            ConflictWaitOutcome::RetryNow
        );
        assert_eq!(sleep_backoff.next_sleep, Duration::from_millis(10));

        let mut zero_sleep_backoff = ConflictBackoff::new(ConflictWaitConfig {
            lock_wait_timeout: Duration::from_secs(1),
            spin_retries: 0,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        });
        assert_eq!(
            zero_sleep_backoff.wait_next().await,
            ConflictWaitOutcome::RetryNow
        );

        let mut timed_out_backoff = ConflictBackoff {
            timeout: Duration::ZERO,
            started_at: tokio::time::Instant::now(),
            remaining_spins: 0,
            next_sleep: Duration::from_millis(1),
            max_sleep: Duration::from_millis(1),
        };
        assert_eq!(
            timed_out_backoff.wait_next().await,
            ConflictWaitOutcome::TimedOut
        );
    }

    #[tokio::test]
    async fn test_last_ts_reflects_latest_allocated_timestamp() {
        let (_storage, txn_service, _dir) = create_test_service();
        let last_ts_before = txn_service.last_ts();

        let ctx = txn_service.begin(false).unwrap();
        assert_eq!(ctx.txn_id(), last_ts_before);
        assert_eq!(ctx.start_ts(), last_ts_before + 1);
        assert_eq!(txn_service.last_ts(), last_ts_before + 2);
    }

    #[tokio::test]
    async fn test_get_txn_state_reads_shared_transaction_cache() {
        let (_storage, txn_service, _dir) = create_test_service();
        let ctx = txn_service.begin_explicit(false).unwrap();

        assert_eq!(
            txn_service.get_txn_state(ctx.start_ts()),
            Some(TxnState::Running)
        );
    }

    #[tokio::test]
    async fn test_get_rejects_user_table_key_for_system_table_target() {
        let (_storage, txn_service, _dir) = create_test_service();
        let ctx = txn_service.begin(true).unwrap();
        let user_key = crate::tablet::encode_key(USER_TABLE_ID_START, b"row");

        let err = txn_service
            .get(&ctx, ALL_META_TABLE_ID, &user_key)
            .await
            .unwrap_err();

        assert!(
            matches!(err, TiSqlError::Storage(message) if message.contains("belongs to user table space"))
        );
    }

    #[tokio::test]
    async fn test_scan_rejects_unencoded_user_table_start_key() {
        let (_storage, txn_service, _dir) = create_test_service();
        let ctx = txn_service.begin(true).unwrap();

        let err = match txn_service.scan(
            &ctx,
            USER_TABLE_ID_START,
            b"plain".to_vec()..b"plainz".to_vec(),
        ) {
            Ok(_) => panic!("scan should reject unencoded user-table start keys"),
            Err(err) => err,
        };

        assert!(
            matches!(err, TiSqlError::Storage(message) if message.contains("requires encoded user-table key"))
        );
    }

    #[tokio::test]
    async fn test_put_rejects_mismatched_user_table_key() {
        let (_storage, txn_service, _dir) = create_test_service();
        let mut ctx = txn_service.begin(false).unwrap();
        let wrong_key = crate::tablet::encode_key(USER_TABLE_ID_START + 1, b"row");

        let err = txn_service
            .put(&mut ctx, USER_TABLE_ID_START, &wrong_key, b"value".to_vec())
            .await
            .unwrap_err();

        assert!(
            matches!(err, TiSqlError::Storage(message) if message.contains("table_id/key mismatch"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_put_waits_under_frozen_backpressure_before_erroring() {
        let (storage, txn_service, _dir) = create_lsm_test_service_with_config(|tablet_dir| {
            LsmConfig::builder(tablet_dir)
                .memtable_size(64)
                .max_frozen_memtables(2)
                .frozen_slowdown_enter_percent(50)
                .frozen_slowdown_exit_percent(1)
                .frozen_slowdown_delay_ms(5, 5)
                .build()
                .unwrap()
        });
        write_committed_value(&storage, b"frozen-pressure", 1, 60);

        let txn_service = Arc::new(txn_service);
        let put_service = Arc::clone(&txn_service);
        let put_handle = tokio::spawn(async move {
            let mut ctx = put_service.begin(false).unwrap();
            let result = put_service
                .put(&mut ctx, ALL_META_TABLE_ID, b"retry-put", b"v".to_vec())
                .await;
            let _ = put_service.rollback(ctx);
            result
        });

        tokio::task::yield_now().await;
        assert!(
            !put_handle.is_finished(),
            "put should wait while frozen-count slowdown is active"
        );

        for _ in 0..40 {
            tokio::time::advance(Duration::from_millis(5)).await;
            tokio::task::yield_now().await;
        }
        let err = put_handle.await.unwrap().unwrap_err();
        assert!(matches!(err, TiSqlError::Storage(message) if message.contains("Write stalled")));

        let reader = txn_service.begin(true).unwrap();
        assert_eq!(
            txn_service
                .get(&reader, ALL_META_TABLE_ID, b"retry-put")
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn test_put_reports_terminal_write_stall_when_frozen_capacity_is_exhausted() {
        let (storage, txn_service, _dir) = create_lsm_test_service_with_config(|tablet_dir| {
            LsmConfig::builder(tablet_dir)
                .memtable_size(64)
                .max_frozen_memtables(1)
                .build()
                .unwrap()
        });
        write_committed_value(&storage, b"k1", 1, 60);
        write_committed_value(&storage, b"k2", 2, 60);

        let mut batch = WriteBatch::new();
        batch.set_commit_ts(3);
        batch.put(b"k3".to_vec(), vec![b'x'; 60]);
        let _ = storage.write_batch(batch);

        let mut ctx = txn_service.begin(false).unwrap();
        let err = txn_service
            .put(
                &mut ctx,
                ALL_META_TABLE_ID,
                b"pending_stalled",
                b"value".to_vec(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, TiSqlError::Storage(message) if message.contains("Write stalled")));

        txn_service.rollback(ctx).unwrap();

        let reader = txn_service.begin(true).unwrap();
        assert_eq!(
            txn_service
                .get(&reader, ALL_META_TABLE_ID, b"pending_stalled")
                .await
                .unwrap(),
            None
        );
    }

    #[test]
    fn test_recovery_stats_total_applied_sums_puts_and_deletes() {
        let stats = RecoveryStats {
            committed_txns: 1,
            applied_puts: 2,
            applied_deletes: 3,
            rolled_back_entries: 4,
            post_commit_ops: 0,
        };

        assert_eq!(stats.total_applied(), 5);
    }

    #[tokio::test]
    async fn test_public_statement_guard_can_rollback_without_storage_types() {
        let (_storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        let mut stmt = txn_service.begin_statement(&mut ctx);

        PublicTxnService::put(
            &txn_service,
            &mut ctx,
            &mut stmt,
            ALL_META_TABLE_ID,
            b"stmt_key",
            b"value".to_vec(),
        )
        .await
        .unwrap();

        PublicTxnService::rollback_statement(&txn_service, &mut ctx, stmt)
            .await
            .unwrap();

        assert_eq!(
            txn_service
                .get(&ctx, ALL_META_TABLE_ID, b"stmt_key")
                .await
                .unwrap(),
            None
        );
        assert!(collect_public_scan(
            &txn_service,
            &ctx,
            ALL_META_TABLE_ID,
            b"stmt".to_vec()..b"stmu".to_vec(),
        )
        .await
        .unwrap()
        .is_empty());

        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test]
    async fn test_public_scan_cursor_returns_logical_key_values() {
        let (_storage, txn_service, _dir) = create_test_service();

        public_put_and_commit(&txn_service, ALL_META_TABLE_ID, b"alpha", b"v1")
            .await
            .unwrap();
        public_put_and_commit(&txn_service, ALL_META_TABLE_ID, b"beta", b"v2")
            .await
            .unwrap();

        let ctx = txn_service.begin(true).unwrap();
        let rows = collect_public_scan(
            &txn_service,
            &ctx,
            ALL_META_TABLE_ID,
            b"alpha".to_vec()..b"gamma".to_vec(),
        )
        .await
        .unwrap();

        assert_eq!(
            rows,
            vec![
                (b"alpha".to_vec(), b"v1".to_vec()),
                (b"beta".to_vec(), b"v2".to_vec()),
            ]
        );
    }

    #[tokio::test]
    async fn test_public_scan_retries_blocked_same_key_before_advancing() {
        let (txn_service, _dir) = create_mock_scan_service(
            vec![
                TxnStorageScanStep::Blocked(crate::kernel::txn_storage::TxnStorageBlocked {
                    user_key: b"beta".to_vec(),
                    owner_start_ts: 42,
                }),
                TxnStorageScanStep::Entry(TxnStorageScanEntry {
                    user_key: b"beta".to_vec(),
                    state: TxnStorageVisibleState::Value(b"v2".to_vec()),
                }),
                TxnStorageScanStep::Entry(TxnStorageScanEntry {
                    user_key: b"gamma".to_vec(),
                    state: TxnStorageVisibleState::Value(b"v3".to_vec()),
                }),
            ],
            ConflictWaitConfig {
                lock_wait_timeout: Duration::from_millis(100),
                spin_retries: 1,
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(10),
            },
        );

        let ctx = txn_service.begin(true).unwrap();
        let rows = collect_public_scan(
            &txn_service,
            &ctx,
            ALL_META_TABLE_ID,
            b"beta".to_vec()..b"omega".to_vec(),
        )
        .await
        .unwrap();

        assert_eq!(
            rows,
            vec![
                (b"beta".to_vec(), b"v2".to_vec()),
                (b"gamma".to_vec(), b"v3".to_vec()),
            ]
        );
    }

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
        iter.advance().await.unwrap();

        while iter.valid() {
            let decoded_key = iter.user_key();
            let entry_ts = iter.timestamp();
            let value = iter.value();
            if decoded_key == key && entry_ts <= ts {
                if is_tombstone(value) || is_lock(value) {
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

    async fn raw_versions_for_test<S: StorageEngine>(
        storage: &S,
        key: &[u8],
    ) -> Vec<(Timestamp, RawValue)> {
        let seek_key = MvccKey::encode(key, Timestamp::MAX);
        let end_key = MvccKey::encode(key, 0)
            .next_key()
            .unwrap_or_else(MvccKey::unbounded);
        let range = seek_key..end_key;

        let mut iter = storage.scan_iter(range, 0).unwrap();
        iter.advance().await.unwrap();
        let mut versions = Vec::new();
        while iter.valid() {
            if iter.user_key() != key {
                break;
            }
            versions.push((iter.timestamp(), iter.value().to_vec()));
            iter.advance().await.unwrap();
        }
        versions
    }

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

            self.position += 1;

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

    struct PendingIterator {
        entries: Vec<(MvccKey, RawValue, Timestamp)>,
        position: usize,
    }

    impl PendingIterator {
        fn new(entries: Vec<(MvccKey, RawValue, Timestamp)>) -> Self {
            Self {
                entries,
                position: 0,
            }
        }

        fn current(&self) -> Option<&(MvccKey, RawValue, Timestamp)> {
            self.entries.get(self.position)
        }
    }

    impl MvccIterator for PendingIterator {
        async fn seek(&mut self, _target: &MvccKey) -> Result<()> {
            self.position = 0;
            Ok(())
        }

        async fn advance(&mut self) -> Result<()> {
            self.position += 1;
            Ok(())
        }

        fn valid(&self) -> bool {
            self.position < self.entries.len()
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
            self.current()
                .expect("value() called on invalid iterator")
                .1
                .as_slice()
        }

        fn is_pending(&self) -> bool {
            self.valid()
        }

        fn pending_owner(&self) -> Timestamp {
            self.current()
                .expect("pending_owner() called on invalid iterator")
                .2
        }
    }

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

        // Error after the underlying iterator performs more than one successful
        // advance. The scan adapter may eagerly skip the current MVCC key before
        // returning an entry, so assert only the externally visible contract:
        // the stream must surface the storage error instead of truncating.
        let mock_iter = ErrorInjectingIterator::new(entries, 1);

        let scan_iter = MvccScanIterator::new(
            mock_iter,
            10, // read_ts
            b"a".to_vec()..b"z".to_vec(),
            None,
            ConflictWaitConfig::default(),
        );

        let results = collect_scan_results(scan_iter).await;

        assert!(
            results.iter().any(|result| result.is_ok()),
            "scan should return visible rows before surfacing the storage error"
        );
        assert!(
            results.last().is_some_and(|result| result.is_err()),
            "final result should be the propagated storage error"
        );
        let err = results.last().unwrap().as_ref().unwrap_err();
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

        // Error after 0 successful underlying advances. Depending on how the
        // scan adapter skips MVCC versions, the error can surface on the first
        // externally visible step. The contract is that the error is surfaced,
        // not silently converted into EOF.
        let mock_iter = ErrorInjectingIterator::new(entries, 0);

        let scan_iter = MvccScanIterator::new(
            mock_iter,
            10,
            b"a".to_vec()..b"z".to_vec(),
            None,
            ConflictWaitConfig::default(),
        );

        let results = collect_scan_results(scan_iter).await;

        assert_eq!(results.len(), 1, "first scan step should surface the error");
        assert!(
            results[0].is_err(),
            "scan error must not be silently dropped"
        );
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

        let scan_iter = MvccScanIterator::new(
            mock_iter,
            10,
            b"a".to_vec()..b"z".to_vec(),
            None,
            ConflictWaitConfig::default(),
        );

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

        // Error after one successful underlying advance. The adapter may eagerly
        // skip the current key, so assert only that we get some successful rows
        // and then the explicit error instead of a silent EOF.
        let mock_iter = ErrorInjectingIterator::new(entries.clone(), 1);

        let scan_iter = MvccScanIterator::new(
            mock_iter,
            10,
            b"a".to_vec()..b"z".to_vec(),
            None,
            ConflictWaitConfig::default(),
        );

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
            successful_count, 1,
            "current scan adapter should yield exactly one visible row before the injected error"
        );
    }

    #[test]
    fn test_check_active_rejects_non_running_states() {
        type Service = TransactionService<MockTxnStorage, FileClogService, LocalTso>;

        let mut ctx = TxnCtx::new(1, 1, false);
        assert!(Service::check_active(&ctx).is_ok());

        ctx.state = TxnState::Preparing;
        assert!(matches!(
            Service::check_active(&ctx),
            Err(TiSqlError::Internal(message)) if message.contains("preparing")
        ));

        ctx.state = TxnState::Prepared { prepared_ts: 2 };
        assert!(matches!(
            Service::check_active(&ctx),
            Err(TiSqlError::Internal(message)) if message.contains("prepared")
        ));
    }

    #[tokio::test]
    async fn test_write_path_metric_pairs_include_all_stages() {
        let (_storage, txn_service, _dir) = create_test_service();

        txn_service
            .write_path_diagnostics
            .duplicate_check
            .record_duration(Duration::from_micros(3));
        txn_service
            .write_path_diagnostics
            .put_pending
            .record_duration(Duration::from_micros(4));
        txn_service
            .write_path_diagnostics
            .lock_pending
            .record_duration(Duration::from_micros(5));
        txn_service
            .write_path_diagnostics
            .commit_read_values
            .record_duration(Duration::from_micros(6));
        txn_service
            .write_path_diagnostics
            .commit_prepare_ts
            .record_duration(Duration::from_micros(7));
        txn_service
            .write_path_diagnostics
            .commit_clog_write
            .record_duration(Duration::from_micros(8));
        txn_service
            .write_path_diagnostics
            .commit_fsync_wait
            .record_duration(Duration::from_micros(9));
        txn_service
            .write_path_diagnostics
            .commit_finalize
            .record_duration(Duration::from_micros(10));
        txn_service
            .write_path_diagnostics
            .commit_total
            .record_duration(Duration::from_micros(11));
        txn_service
            .write_path_diagnostics
            .commit_success
            .fetch_add(1, AtomicOrdering::Relaxed);
        txn_service
            .write_path_diagnostics
            .commit_failure
            .fetch_add(2, AtomicOrdering::Relaxed);

        let pairs = txn_service.write_path_metric_pairs();
        let metrics: std::collections::BTreeMap<_, _> = pairs.into_iter().collect();
        assert_eq!(metrics.get("duplicate_check.count"), Some(&"1".to_string()));
        assert_eq!(metrics.get("put_pending.avg_us"), Some(&"4".to_string()));
        assert_eq!(metrics.get("lock_pending.max_us"), Some(&"5".to_string()));
        assert_eq!(
            metrics.get("commit.read_values.count"),
            Some(&"1".to_string())
        );
        assert_eq!(
            metrics.get("commit.prepare_ts.avg_us"),
            Some(&"7".to_string())
        );
        assert_eq!(
            metrics.get("commit.clog_write.max_us"),
            Some(&"8".to_string())
        );
        assert_eq!(
            metrics.get("commit.fsync_wait.count"),
            Some(&"1".to_string())
        );
        assert_eq!(
            metrics.get("commit.finalize.avg_us"),
            Some(&"10".to_string())
        );
        assert_eq!(metrics.get("commit.total.max_us"), Some(&"11".to_string()));
        assert_eq!(metrics.get("commit.success_count"), Some(&"1".to_string()));
        assert_eq!(metrics.get("commit.failure_count"), Some(&"2".to_string()));
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
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"value1".to_vec())
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
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"value1".to_vec())
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
            .delete(&mut ctx, ALL_META_TABLE_ID, b"key1")
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
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"value1".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key2", b"value2".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key3", b"value3".to_vec())
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
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"v1".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"v2".to_vec())
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
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"value1".to_vec())
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
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"value1".to_vec())
            .await
            .unwrap();

        // Read it back (read-your-writes)
        let value = txn_service
            .get(&ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Clean up
        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test]
    async fn test_implicit_txn_get_does_not_see_own_pending_write() {
        let (_storage, txn_service, _dir) = create_test_service();

        // begin() creates an implicit context (`explicit = false`)
        let mut ctx = txn_service.begin(false).unwrap();
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"value1".to_vec())
            .await
            .unwrap();

        // Implicit reads use owner_ts=0, so pending writes from this same context
        // are not visible before commit.
        let pre_commit = txn_service
            .get(&ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
        assert_eq!(pre_commit, None);

        txn_service.commit(ctx).await.unwrap();

        let reader = txn_service.begin(true).unwrap();
        let post_commit = txn_service
            .get(&reader, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
        assert_eq!(post_commit, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_read_your_writes_update() {
        let (_storage, txn_service, _dir) = create_test_service();

        // Start explicit transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Write initial value
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"v1".to_vec())
            .await
            .unwrap();

        // Update it
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"v2".to_vec())
            .await
            .unwrap();

        // Read should return the updated value
        let value = txn_service
            .get(&ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
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
            .put(
                &mut setup_ctx,
                ALL_META_TABLE_ID,
                b"key1",
                b"initial".to_vec(),
            )
            .await
            .unwrap();
        txn_service.commit(setup_ctx).await.unwrap();

        // Start new transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Delete the key
        txn_service
            .delete(&mut ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();

        // Read should return None (deleted)
        let value = txn_service
            .get(&ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
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
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"value1".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key2", b"value2".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key3", b"value3".to_vec())
            .await
            .unwrap();

        // Scan should see all pending writes
        let mut scan_iter = txn_service
            .scan_iter(&ctx, ALL_META_TABLE_ID, b"key".to_vec()..b"key~".to_vec())
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
            .put(&mut ctx1, ALL_META_TABLE_ID, b"key1", b"from_txn1".to_vec())
            .await
            .unwrap();

        // Txn1 should see its own write
        let value1 = txn_service
            .get(&ctx1, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
        assert_eq!(value1, Some(b"from_txn1".to_vec()));

        // Txn2 should NOT see txn1's pending write
        let value2 = txn_service
            .get(&ctx2, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
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
            .put(
                &mut setup_ctx,
                ALL_META_TABLE_ID,
                b"key1",
                b"committed".to_vec(),
            )
            .await
            .unwrap();
        txn_service.commit(setup_ctx).await.unwrap();

        // Start new transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Update the key
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"pending".to_vec())
            .await
            .unwrap();

        // Read should return the pending value, not the committed one
        let value = txn_service
            .get(&ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
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
            .put(
                &mut setup_ctx,
                ALL_META_TABLE_ID,
                b"key1",
                b"original".to_vec(),
            )
            .await
            .unwrap();
        txn_service.commit(setup_ctx).await.unwrap();

        // Start new transaction
        let mut ctx = txn_service.begin_explicit(false).unwrap();

        // Delete
        txn_service
            .delete(&mut ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();

        // Should see None
        let value = txn_service
            .get(&ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
        assert_eq!(value, None);

        // Re-insert with new value
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"reinserted".to_vec())
            .await
            .unwrap();

        // Should see the new value
        let value = txn_service
            .get(&ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
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
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"value1".to_vec())
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
            .put(
                &mut committed_ctx,
                ALL_META_TABLE_ID,
                b"key2",
                b"value2".to_vec(),
            )
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
            .delete(&mut committed_ctx, ALL_META_TABLE_ID, b"key1")
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
            .put(
                &mut aborted_ctx,
                ALL_META_TABLE_ID,
                b"key1",
                b"value1".to_vec(),
            )
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
        let result = txn_service
            .delete(&mut aborted_ctx, ALL_META_TABLE_ID, b"key1")
            .await;
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
            let clog_service = Arc::new(
                FileClogService::open(config.clone(), make_test_io(), &io_handle).unwrap(),
            );

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
        let storage = Arc::new(MemTableEngine::new());
        let (clog_service, entries) = FileClogService::recover_with_lsn_provider(
            config,
            storage.lsn_provider(),
            make_test_io(),
            &io_handle,
        )
        .unwrap();
        let clog_service = Arc::new(clog_service);
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let txn_storage = Arc::new(TabletTxnStorage::new(Arc::clone(&storage), Arc::clone(&cm)));
        let txn_service = TransactionService::new(txn_storage, clog_service, tso, cm);

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
            .scan_iter(&ctx, ALL_META_TABLE_ID, b"key1".to_vec()..vec![])
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
        let value1 = txn_service
            .get(&ctx1, ALL_META_TABLE_ID, b"key")
            .await
            .unwrap();
        assert_eq!(value1, Some(b"v1".to_vec()));

        let ctx2 = TxnCtx::new(0, info2.commit_ts, true);
        let value2 = txn_service
            .get(&ctx2, ALL_META_TABLE_ID, b"key")
            .await
            .unwrap();
        assert_eq!(value2, Some(b"v2".to_vec()));

        let ctx3 = TxnCtx::new(0, info3.commit_ts, true);
        let value3 = txn_service
            .get(&ctx3, ALL_META_TABLE_ID, b"key")
            .await
            .unwrap();
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
        let value2 = txn_service
            .get(&ctx2, ALL_META_TABLE_ID, b"key")
            .await
            .unwrap();
        assert_eq!(value2, None);

        // At v3 timestamp, should see v3
        let ctx3 = TxnCtx::new(0, info3.commit_ts, true);
        let value3 = txn_service
            .get(&ctx3, ALL_META_TABLE_ID, b"key")
            .await
            .unwrap();
        assert_eq!(value3, Some(b"v3".to_vec()));
    }

    #[tokio::test(start_paused = true)]
    async fn test_explicit_txn_write_conflict_times_out() {
        let (_storage, txn_service, _dir) = create_test_service();
        let txn_service = Arc::new(txn_service);

        // Transaction 1: Lock key
        let mut ctx1 = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(
                &mut ctx1,
                ALL_META_TABLE_ID,
                b"conflict_key",
                b"value1".to_vec(),
            )
            .await
            .unwrap();

        // Transaction 2: Try to write same key - should fail
        let conflict_service = Arc::clone(&txn_service);
        let conflict_handle = tokio::spawn(async move {
            let mut ctx2 = conflict_service.begin_explicit(false).unwrap();
            let result = conflict_service
                .put(
                    &mut ctx2,
                    ALL_META_TABLE_ID,
                    b"conflict_key",
                    b"value2".to_vec(),
                )
                .await;
            let _ = conflict_service.rollback(ctx2);
            result
        });

        tokio::task::yield_now().await;
        assert!(
            !conflict_handle.is_finished(),
            "conflicting writer should still be waiting"
        );
        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(matches!(
            conflict_handle.await.unwrap(),
            Err(TiSqlError::LockWaitTimeout)
        ));

        // Cleanup
        txn_service.rollback(ctx1).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_explicit_txn_delete_conflict_times_out() {
        let (_storage, txn_service, _dir) = create_test_service();
        let txn_service = Arc::new(txn_service);

        // Commit initial value
        txn_service
            .autocommit_put(b"delete_key", b"value")
            .await
            .unwrap();

        // Transaction 1: Lock key with delete
        let mut ctx1 = txn_service.begin_explicit(false).unwrap();
        txn_service
            .delete(&mut ctx1, ALL_META_TABLE_ID, b"delete_key")
            .await
            .unwrap();

        // Transaction 2: Try to delete same key - should fail
        let conflict_service = Arc::clone(&txn_service);
        let conflict_handle = tokio::spawn(async move {
            let mut ctx2 = conflict_service.begin_explicit(false).unwrap();
            let result = conflict_service
                .delete(&mut ctx2, ALL_META_TABLE_ID, b"delete_key")
                .await;
            let _ = conflict_service.rollback(ctx2);
            result
        });

        tokio::task::yield_now().await;
        assert!(
            !conflict_handle.is_finished(),
            "conflicting delete should still be waiting"
        );
        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(matches!(
            conflict_handle.await.unwrap(),
            Err(TiSqlError::LockWaitTimeout)
        ));

        // Cleanup
        txn_service.rollback(ctx1).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_explicit_txn_write_conflict_waits_then_succeeds_after_rollback() {
        let (_storage, txn_service, _dir) = create_test_service();
        let txn_service = Arc::new(txn_service);

        let mut ctx1 = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(
                &mut ctx1,
                ALL_META_TABLE_ID,
                b"retry_key",
                b"value1".to_vec(),
            )
            .await
            .unwrap();

        let retry_service = Arc::clone(&txn_service);
        let retry_handle = tokio::spawn(async move {
            let mut ctx2 = retry_service.begin_explicit(false).unwrap();
            retry_service
                .put(
                    &mut ctx2,
                    ALL_META_TABLE_ID,
                    b"retry_key",
                    b"value2".to_vec(),
                )
                .await?;
            retry_service.commit(ctx2).await?;
            Ok::<(), TiSqlError>(())
        });

        tokio::task::yield_now().await;
        assert!(
            !retry_handle.is_finished(),
            "conflicting writer should be waiting for lock release"
        );

        txn_service.rollback(ctx1).unwrap();
        tokio::time::advance(Duration::from_millis(10)).await;
        retry_handle.await.unwrap().unwrap();

        let reader = txn_service.begin(true).unwrap();
        let value = txn_service
            .get(&reader, ALL_META_TABLE_ID, b"retry_key")
            .await
            .unwrap();
        assert_eq!(value, Some(b"value2".to_vec()));
    }

    #[tokio::test(start_paused = true)]
    async fn test_point_get_waits_for_prepared_owner_then_returns_committed_value() {
        let (_storage, txn_service, _dir) = create_test_service();
        let txn_service = Arc::new(txn_service);

        txn_service
            .autocommit_put(b"prepared_key", b"base")
            .await
            .unwrap();

        let mut writer = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(
                &mut writer,
                ALL_META_TABLE_ID,
                b"prepared_key",
                b"prepared".to_vec(),
            )
            .await
            .unwrap();

        let reader = txn_service.begin(true).unwrap();
        let read_ts = reader.start_ts;
        txn_service
            .concurrency_manager
            .set_preparing_txn(writer.start_ts)
            .unwrap();
        txn_service
            .concurrency_manager
            .prepare_txn(writer.start_ts, read_ts)
            .unwrap();

        let read_service = Arc::clone(&txn_service);
        let read_handle = tokio::spawn(async move {
            read_service
                .get(&reader, ALL_META_TABLE_ID, b"prepared_key")
                .await
        });

        tokio::task::yield_now().await;
        assert!(
            !read_handle.is_finished(),
            "reader should wait while prepared owner is unresolved"
        );

        txn_service.storage.inner().finalize_pending(
            &[b"prepared_key".to_vec()],
            writer.start_ts,
            read_ts,
        );
        txn_service
            .concurrency_manager
            .commit_txn(writer.start_ts, read_ts)
            .unwrap();
        txn_service.concurrency_manager.remove_txn(writer.start_ts);

        tokio::time::advance(Duration::from_millis(10)).await;
        let value = read_handle.await.unwrap().unwrap();
        assert_eq!(value, Some(b"prepared".to_vec()));
    }

    #[tokio::test(start_paused = true)]
    async fn test_mvcc_scan_iterator_pending_prepared_times_out() {
        let cm = Arc::new(ConcurrencyManager::new(0));
        let owner_ts = 42;
        cm.register_txn(owner_ts);
        cm.set_preparing_txn(owner_ts).unwrap();
        cm.prepare_txn(owner_ts, 10).unwrap();

        let iter = PendingIterator::new(vec![(
            MvccKey::encode(b"pending_key", 0),
            b"pending".to_vec(),
            owner_ts,
        )]);

        let scan_iter = MvccScanIterator::new(
            iter,
            10,
            b"a".to_vec()..b"z".to_vec(),
            Some(cm),
            ConflictWaitConfig {
                lock_wait_timeout: Duration::from_millis(100),
                spin_retries: 0,
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(10),
            },
        );

        let scan_handle = tokio::spawn(async move {
            let mut scan_iter = scan_iter;
            scan_iter.advance().await
        });

        tokio::task::yield_now().await;
        assert!(
            !scan_handle.is_finished(),
            "scan should wait while prepared owner is unresolved"
        );

        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(matches!(
            scan_handle.await.unwrap(),
            Err(TiSqlError::LockWaitTimeout)
        ));
    }

    #[tokio::test]
    async fn test_recover_with_delete_operation() {
        let dir = tempdir().unwrap();
        let config = FileClogConfig::with_dir(dir.path());
        let io_handle = tokio::runtime::Handle::current();

        {
            let storage = Arc::new(MemTableEngine::new());
            let clog_service = Arc::new(
                FileClogService::open_with_lsn_provider(
                    config.clone(),
                    storage.lsn_provider(),
                    make_test_io(),
                    &io_handle,
                )
                .unwrap(),
            );
            let tso = Arc::new(LocalTso::new(1));
            let cm = Arc::new(ConcurrencyManager::new(0));
            let txn_storage =
                Arc::new(TabletTxnStorage::new(Arc::clone(&storage), Arc::clone(&cm)));
            let txn_service = TransactionService::new(txn_storage, clog_service.clone(), tso, cm);

            // Put then delete
            txn_service
                .autocommit_put(b"del_key", b"value")
                .await
                .unwrap();
            txn_service.autocommit_delete(b"del_key").await.unwrap();

            clog_service.close().await.unwrap();
        }

        // Recover
        let storage = Arc::new(MemTableEngine::new());
        let (clog_service, entries) = FileClogService::recover_with_lsn_provider(
            config,
            storage.lsn_provider(),
            make_test_io(),
            &io_handle,
        )
        .unwrap();
        let clog_service = Arc::new(clog_service);
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let txn_storage = Arc::new(TabletTxnStorage::new(Arc::clone(&storage), Arc::clone(&cm)));
        let txn_service = TransactionService::new(txn_storage, clog_service, tso, cm);

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
        let storage = Arc::new(MemTableEngine::new());
        let clog_service = Arc::new(
            FileClogService::open_with_lsn_provider(
                config,
                storage.lsn_provider(),
                make_test_io(),
                &io_handle,
            )
            .unwrap(),
        );
        let tso = Arc::new(LocalTso::new(1));
        let cm = Arc::new(ConcurrencyManager::new(0));
        let txn_storage = Arc::new(TabletTxnStorage::new(Arc::clone(&storage), Arc::clone(&cm)));
        let txn_service = TransactionService::new(txn_storage, clog_service, tso, cm);

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
        let storage_ref = txn_service.storage().inner();
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
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"value1".to_vec())
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
            .delete(&mut ctx, ALL_META_TABLE_ID, b"key1")
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
    async fn test_explicit_txn_repeated_delete_existing_preserves_delete_on_commit() {
        let (storage, txn_service, _dir) = create_test_service();

        txn_service
            .autocommit_put(b"key1", b"value1")
            .await
            .unwrap();

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        let txn_id = ctx.txn_id();
        txn_service
            .delete(&mut ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
        txn_service
            .delete(&mut ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
        txn_service.commit(ctx).await.unwrap();

        assert_eq!(
            get_for_test(&*storage, b"key1").await,
            None,
            "repeated delete must not degrade into a lock-only commit"
        );

        let entries = txn_service.clog_service().read_all().unwrap();
        let data = clog_data_entries(&entries, txn_id);
        assert_eq!(data.len(), 1, "should still write one delete entry");
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
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"v1".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key2", b"v2".to_vec())
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key3", b"v3".to_vec())
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
            .delete(&mut ctx, ALL_META_TABLE_ID, b"nonexistent")
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
    async fn test_commit_mixed_write_and_lock_aborts_lock_node() {
        let (storage, txn_service, _dir) = create_test_service();

        txn_service
            .autocommit_put(b"locked_key", b"base")
            .await
            .unwrap();

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(
                &mut ctx,
                ALL_META_TABLE_ID,
                b"write_key",
                b"write_value".to_vec(),
            )
            .await
            .unwrap();
        txn_service
            .lock_key(&mut ctx, ALL_META_TABLE_ID, b"locked_key")
            .await
            .unwrap();
        let info = txn_service.commit(ctx).await.unwrap();

        let locked_versions = raw_versions_for_test(&*storage, b"locked_key").await;
        assert_eq!(
            locked_versions.len(),
            1,
            "lock key should keep only base committed version after commit"
        );
        assert_eq!(locked_versions[0].1, b"base".to_vec());
        assert!(
            locked_versions[0].0 < info.commit_ts,
            "base version should remain older than mixed txn commit_ts"
        );

        let write_versions = raw_versions_for_test(&*storage, b"write_key").await;
        assert_eq!(write_versions.len(), 1);
        assert_eq!(write_versions[0].0, info.commit_ts);
        assert_eq!(write_versions[0].1, b"write_value".to_vec());
    }

    #[tokio::test]
    async fn test_get_filters_committed_lock_sentinel() {
        let (storage, txn_service, _dir) = create_test_service();

        let mut batch = WriteBatch::new();
        batch.put(b"key1".to_vec(), LOCK.to_vec());
        batch.set_commit_ts(100);
        storage.write_batch(batch).unwrap();

        let reader = txn_service.begin(true).unwrap();
        let value = txn_service
            .get(&reader, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
        assert!(value.is_none(), "LOCK sentinel must be hidden from get()");
    }

    #[tokio::test]
    async fn test_statement_rollback_removes_statement_created_key() {
        let (_storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        let mut stmt = txn_service.begin_statement(&mut ctx);

        PublicTxnService::put(
            &txn_service,
            &mut ctx,
            &mut stmt,
            ALL_META_TABLE_ID,
            b"stmt_key",
            b"value".to_vec(),
        )
        .await
        .unwrap();
        PublicTxnService::rollback_statement(&txn_service, &mut ctx, stmt)
            .await
            .unwrap();

        assert!(
            ctx.mutations.is_empty(),
            "statement-created key should be removed"
        );
        assert_eq!(
            txn_service
                .get(&ctx, ALL_META_TABLE_ID, b"stmt_key")
                .await
                .unwrap(),
            None
        );

        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_statement_rollback_restores_prior_put_without_releasing_lock() {
        let (_storage, txn_service, _dir) = create_test_service();
        let txn_service = Arc::new(txn_service);

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"restore_put", b"v1".to_vec())
            .await
            .unwrap();

        let mut stmt = txn_service.begin_statement(&mut ctx);
        PublicTxnService::put(
            txn_service.as_ref(),
            &mut ctx,
            &mut stmt,
            ALL_META_TABLE_ID,
            b"restore_put",
            b"v2".to_vec(),
        )
        .await
        .unwrap();
        PublicTxnService::rollback_statement(txn_service.as_ref(), &mut ctx, stmt)
            .await
            .unwrap();

        assert!(matches!(
            ctx.mutations
                .get(b"restore_put".as_slice())
                .map(|meta| &meta.mutation),
            Some(MutationPayload::Put(value)) if value == b"v1"
        ));
        assert_eq!(
            txn_service
                .get(&ctx, ALL_META_TABLE_ID, b"restore_put")
                .await
                .unwrap(),
            Some(b"v1".to_vec())
        );

        let conflict_service = Arc::clone(&txn_service);
        let conflict_handle = tokio::spawn(async move {
            let mut ctx2 = conflict_service.begin_explicit(false).unwrap();
            let result = conflict_service
                .put(
                    &mut ctx2,
                    ALL_META_TABLE_ID,
                    b"restore_put",
                    b"other".to_vec(),
                )
                .await;
            let _ = conflict_service.rollback(ctx2);
            result
        });

        tokio::task::yield_now().await;
        assert!(
            !conflict_handle.is_finished(),
            "restored key should stay protected by the original transaction"
        );
        tokio::time::advance(Duration::from_millis(100)).await;
        assert!(matches!(
            conflict_handle.await.unwrap(),
            Err(TiSqlError::LockWaitTimeout)
        ));

        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test]
    async fn test_statement_rollback_restores_prior_delete() {
        let (_storage, txn_service, _dir) = create_test_service();

        txn_service
            .autocommit_put(b"restore_delete", b"base")
            .await
            .unwrap();

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .delete(&mut ctx, ALL_META_TABLE_ID, b"restore_delete")
            .await
            .unwrap();

        let mut stmt = txn_service.begin_statement(&mut ctx);
        PublicTxnService::put(
            &txn_service,
            &mut ctx,
            &mut stmt,
            ALL_META_TABLE_ID,
            b"restore_delete",
            b"replacement".to_vec(),
        )
        .await
        .unwrap();
        PublicTxnService::rollback_statement(&txn_service, &mut ctx, stmt)
            .await
            .unwrap();

        assert!(matches!(
            ctx.mutations
                .get(b"restore_delete".as_slice())
                .map(|meta| &meta.mutation),
            Some(MutationPayload::Delete)
        ));
        assert_eq!(
            txn_service
                .get(&ctx, ALL_META_TABLE_ID, b"restore_delete")
                .await
                .unwrap(),
            None
        );

        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test]
    async fn test_statement_rollback_restores_prior_delete_after_repeated_delete() {
        let (_storage, txn_service, _dir) = create_test_service();

        txn_service
            .autocommit_put(b"restore_delete_twice", b"base")
            .await
            .unwrap();

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .delete(&mut ctx, ALL_META_TABLE_ID, b"restore_delete_twice")
            .await
            .unwrap();

        let mut stmt = txn_service.begin_statement(&mut ctx);
        PublicTxnService::delete(
            &txn_service,
            &mut ctx,
            &mut stmt,
            ALL_META_TABLE_ID,
            b"restore_delete_twice",
        )
        .await
        .unwrap();
        PublicTxnService::rollback_statement(&txn_service, &mut ctx, stmt)
            .await
            .unwrap();

        assert!(matches!(
            ctx.mutations
                .get(b"restore_delete_twice".as_slice())
                .map(|meta| &meta.mutation),
            Some(MutationPayload::Delete)
        ));
        assert_eq!(
            txn_service
                .get(&ctx, ALL_META_TABLE_ID, b"restore_delete_twice")
                .await
                .unwrap(),
            None
        );

        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test]
    async fn test_statement_rollback_restores_prior_delete_after_repeated_delete_on_lsm_storage() {
        let (_storage, txn_service, _dir) = create_lsm_test_service();

        txn_service
            .autocommit_put(b"restore_delete_twice_lsm", b"base")
            .await
            .unwrap();

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .delete(&mut ctx, ALL_META_TABLE_ID, b"restore_delete_twice_lsm")
            .await
            .unwrap();

        let mut stmt = txn_service.begin_statement(&mut ctx);
        PublicTxnService::delete(
            &txn_service,
            &mut ctx,
            &mut stmt,
            ALL_META_TABLE_ID,
            b"restore_delete_twice_lsm",
        )
        .await
        .unwrap();
        PublicTxnService::rollback_statement(&txn_service, &mut ctx, stmt)
            .await
            .unwrap();

        assert!(matches!(
            ctx.mutations
                .get(b"restore_delete_twice_lsm".as_slice())
                .map(|meta| &meta.mutation),
            Some(MutationPayload::Delete)
        ));
        assert_eq!(
            txn_service
                .get(&ctx, ALL_META_TABLE_ID, b"restore_delete_twice_lsm")
                .await
                .unwrap(),
            None
        );

        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test]
    async fn test_statement_rollback_restores_prior_lock() {
        let (_storage, txn_service, _dir) = create_test_service();

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .lock_key(&mut ctx, ALL_META_TABLE_ID, b"restore_lock")
            .await
            .unwrap();

        let mut stmt = txn_service.begin_statement(&mut ctx);
        PublicTxnService::put(
            &txn_service,
            &mut ctx,
            &mut stmt,
            ALL_META_TABLE_ID,
            b"restore_lock",
            b"replacement".to_vec(),
        )
        .await
        .unwrap();
        PublicTxnService::rollback_statement(&txn_service, &mut ctx, stmt)
            .await
            .unwrap();

        assert!(matches!(
            ctx.mutations
                .get(b"restore_lock".as_slice())
                .map(|meta| &meta.mutation),
            Some(MutationPayload::Lock)
        ));
        assert_eq!(
            txn_service
                .get(&ctx, ALL_META_TABLE_ID, b"restore_lock")
                .await
                .unwrap(),
            None
        );

        txn_service.rollback(ctx).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn test_statement_rollback_restore_conflict_fails_immediately() {
        let (_storage, txn_service, _dir) = create_test_service();
        let txn_service = Arc::new(txn_service);

        let mut ctx = txn_service.begin_explicit(false).unwrap();
        txn_service
            .put(
                &mut ctx,
                ALL_META_TABLE_ID,
                b"restore_conflict",
                b"v1".to_vec(),
            )
            .await
            .unwrap();

        let mut stmt = txn_service.begin_statement(&mut ctx);
        PublicTxnService::put(
            txn_service.as_ref(),
            &mut ctx,
            &mut stmt,
            ALL_META_TABLE_ID,
            b"restore_conflict",
            b"v2".to_vec(),
        )
        .await
        .unwrap();

        let tablet_id = route_table_to_tablet(ALL_META_TABLE_ID);
        txn_service
            .storage
            .inner()
            .abort_pending(&[b"restore_conflict".to_vec()], ctx.start_ts);
        txn_service
            .storage
            .inner()
            .put_pending_on_tablet(tablet_id, b"restore_conflict", b"foreign", 9_999)
            .unwrap();

        let rollback_service = Arc::clone(&txn_service);
        let rollback_handle = tokio::spawn(async move {
            let mut ctx = ctx;
            PublicTxnService::rollback_statement(rollback_service.as_ref(), &mut ctx, stmt).await
        });

        tokio::task::yield_now().await;
        assert!(
            rollback_handle.is_finished(),
            "restore conflict should fail immediately instead of waiting for lock timeout"
        );

        let err = rollback_handle.await.unwrap().unwrap_err();
        assert!(matches!(
            err,
            TiSqlError::Internal(message) if message.contains("owner mismatch")
        ));

        txn_service
            .storage
            .inner()
            .abort_pending(&[b"restore_conflict".to_vec()], 9_999);
    }

    #[tokio::test]
    async fn test_scan_iter_filters_committed_lock_sentinel() {
        let (storage, txn_service, _dir) = create_test_service();

        let mut lock_batch = WriteBatch::new();
        lock_batch.put(b"key1".to_vec(), LOCK.to_vec());
        lock_batch.set_commit_ts(1);
        storage.write_batch(lock_batch).unwrap();

        let mut value_batch = WriteBatch::new();
        value_batch.put(b"key2".to_vec(), b"v2".to_vec());
        value_batch.set_commit_ts(2);
        storage.write_batch(value_batch).unwrap();

        let reader = txn_service.begin(true).unwrap();
        let mut iter = txn_service
            .scan_iter(
                &reader,
                ALL_META_TABLE_ID,
                b"key1".to_vec()..b"key3".to_vec(),
            )
            .unwrap();
        iter.advance().await.unwrap();

        assert!(iter.valid(), "scan should still return non-lock rows");
        assert_eq!(iter.user_key(), b"key2");
        assert_eq!(iter.value(), b"v2");
        iter.advance().await.unwrap();
        assert!(!iter.valid());
    }

    #[tokio::test]
    async fn test_explicit_txn_put_then_delete_produces_tombstone() {
        let (_storage, txn_service, _dir) = create_test_service();

        // PUT then DELETE same key (no committed value underneath).
        // delete sees own pending put (owner_ts=start_ts) → writes TOMBSTONE.
        let mut ctx = txn_service.begin_explicit(false).unwrap();
        let txn_id = ctx.txn_id();
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"value1".to_vec())
            .await
            .unwrap();
        txn_service
            .delete(&mut ctx, ALL_META_TABLE_ID, b"key1")
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
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"v1".to_vec())
            .await
            .unwrap();
        txn_service
            .delete(&mut ctx, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
        txn_service
            .put(&mut ctx, ALL_META_TABLE_ID, b"key1", b"v2".to_vec())
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
            .put(&mut writer, ALL_META_TABLE_ID, b"key1", b"v2".to_vec())
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
        let val = txn_service
            .get(&reader, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
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
            .put(&mut writer, ALL_META_TABLE_ID, b"key1", b"v2".to_vec())
            .await
            .unwrap();
        txn_service.rollback(writer).unwrap();

        // Reader should see v1 (aborted pending skipped)
        let reader = txn_service.begin(true).unwrap();
        let val = txn_service
            .get(&reader, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
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
        let val = txn_service
            .get(&reader, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
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
            .put(&mut writer, ALL_META_TABLE_ID, b"key1", b"v1".to_vec())
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
            .put(&mut writer, ALL_META_TABLE_ID, b"key1", b"v2".to_vec())
            .await
            .unwrap();
        let commit_info = txn_service.commit(writer).await.unwrap();

        // Reader with start_ts > commit_ts should see v2
        let reader = txn_service.begin(true).unwrap();
        assert!(reader.start_ts > commit_info.commit_ts);
        let val = txn_service
            .get(&reader, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
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
            .put(&mut writer, ALL_META_TABLE_ID, b"b", b"pending".to_vec())
            .await
            .unwrap();

        // Reader scan should see a=v1 and c=v3, but skip b (Running pending)
        let reader = txn_service.begin(true).unwrap();
        let mut iter = txn_service
            .scan_iter(&reader, ALL_META_TABLE_ID, b"a".to_vec()..b"d".to_vec())
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
            .put(&mut writer, ALL_META_TABLE_ID, b"key1", b"v2".to_vec())
            .await
            .unwrap();

        // Non-explicit reader should resolve the pending node:
        // Writer is Running -> skip -> return committed v1
        let reader = txn_service.begin(true).unwrap();
        let val = txn_service
            .get(&reader, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
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
            .put(&mut writer, ALL_META_TABLE_ID, b"key1", b"v2".to_vec())
            .await
            .unwrap();
        let commit_info = txn_service.commit(writer).await.unwrap();

        // Reader that started after commit sees v2
        let reader = txn_service.begin(true).unwrap();
        assert!(reader.start_ts > commit_info.commit_ts);
        let val = txn_service
            .get(&reader, ALL_META_TABLE_ID, b"key1")
            .await
            .unwrap();
        assert_eq!(val.as_deref(), Some(b"v2".as_slice()));
    }

    #[tokio::test]
    async fn test_write_path_diagnostics_duplicate_check_counter() {
        let (_storage, txn_service, _dir) = create_test_service();

        txn_service.record_duplicate_check_duration(Duration::from_micros(7));
        txn_service.record_duplicate_check_duration(Duration::from_micros(13));

        let snapshot = txn_service.write_path_diagnostics.snapshot();
        assert_eq!(snapshot.duplicate_check.count, 2);
        assert_eq!(snapshot.duplicate_check.total_us, 20);
        assert_eq!(snapshot.duplicate_check.avg_us, 10);
        assert_eq!(snapshot.duplicate_check.max_us, 13);
    }

    #[tokio::test]
    async fn test_write_path_diagnostics_commit_stage_counters() {
        let (_storage, txn_service, _dir) = create_test_service();

        txn_service
            .autocommit_put(b"diag-k", b"diag-v")
            .await
            .unwrap();

        let snapshot = txn_service.write_path_diagnostics.snapshot();
        assert_eq!(snapshot.commit_success, 1);
        assert_eq!(snapshot.commit_failure, 0);
        assert!(snapshot.put_pending.count >= 1);
        assert!(snapshot.commit_read_values.count >= 1);
        assert!(snapshot.commit_prepare_ts.count >= 1);
        assert!(snapshot.commit_clog_write.count >= 1);
        assert!(snapshot.commit_fsync_wait.count >= 1);
        assert!(snapshot.commit_finalize.count >= 1);
        assert!(snapshot.commit_total.count >= 1);
    }
}
