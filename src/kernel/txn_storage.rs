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

//! Transaction-facing storage contracts.
//!
//! `TxnStoragePort` is the inward-facing seam between `TransactionService`
//! and tablet-owned MVCC/routing details. The port reports logical point-read,
//! scan, staging, finalize, and abort outcomes without exposing `TabletId`,
//! `MvccKey`, or sentinel encodings.

use std::future::Future;
use std::ops::Range;
use std::time::Duration;

use crate::catalog::types::{Key, Lsn, RawValue, Timestamp};
use crate::util::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitReservation {
    pub owner_start_ts: Timestamp,
    pub lsn: Lsn,
}

pub struct TxnPointRead<'a> {
    pub key: &'a [u8],
    pub read_ts: Timestamp,
    pub owner_ts: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PointReadResult {
    Missing,
    Value(RawValue),
    Deleted,
    Blocked { owner_start_ts: Timestamp },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnStorageScanRequest {
    pub range: Range<Key>,
    pub read_ts: Timestamp,
    pub owner_ts: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnStorageVisibleState {
    Value(RawValue),
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnStorageScanEntry {
    pub user_key: Key,
    pub state: TxnStorageVisibleState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnStorageBlocked {
    pub user_key: Key,
    pub owner_start_ts: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnStorageScanStep {
    Entry(TxnStorageScanEntry),
    Blocked(TxnStorageBlocked),
    Exhausted,
}

pub trait TxnStorageScanCursor: Send {
    fn next_step(&mut self) -> impl Future<Output = Result<TxnStorageScanStep>> + Send + '_;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStageError {
    LockConflict { owner_start_ts: Timestamp },
    WriteStall { delay: Option<Duration> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnDeleteEffect {
    Delete,
    Lock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnRestoreMutation<'a> {
    Put(&'a [u8]),
    Delete,
    Lock,
}

pub struct FinalizeRequest<'a> {
    pub owner_start_ts: Timestamp,
    pub commit_ts: Timestamp,
    pub reservation: CommitReservation,
    pub write_keys: &'a [Key],
    pub lock_only_keys: &'a [Key],
}

pub struct AbortRequest<'a> {
    pub owner_start_ts: Timestamp,
    pub keys: &'a [Key],
}

pub trait TxnStoragePort: Send + Sync + 'static {
    type ScanCursor: TxnStorageScanCursor;

    /// Read one logical key with MVCC visibility and read-your-writes semantics.
    ///
    /// This may await storage I/O.
    fn read_point<'a>(
        &'a self,
        req: TxnPointRead<'a>,
    ) -> impl Future<Output = Result<PointReadResult>> + Send + 'a;

    /// Create a scan cursor for the requested logical range.
    ///
    /// Cursor construction must stay lightweight; any asynchronous work happens
    /// in `TxnStorageScanCursor::next_step()`.
    fn scan(&self, req: TxnStorageScanRequest) -> Result<Self::ScanCursor>;

    /// Stage or overwrite a pending value for `owner_start_ts`.
    ///
    /// This method is synchronous and must never sleep or block on async I/O.
    /// If storage needs the caller to retry later, it must return
    /// `TxnStageError::WriteStall` instead.
    fn stage_put(
        &self,
        key: &[u8],
        value: &[u8],
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), TxnStageError>;

    /// Stage a logical delete for `owner_start_ts`.
    ///
    /// This may await a visibility check when the key has no same-owner pending
    /// head and storage must distinguish `Delete` from `Lock` semantics.
    fn stage_delete<'a>(
        &'a self,
        key: &'a [u8],
        owner_start_ts: Timestamp,
    ) -> impl Future<Output = std::result::Result<TxnDeleteEffect, TxnStageError>> + Send + 'a;

    /// Stage a lock-only pending marker for `owner_start_ts`.
    ///
    /// This method is synchronous and must never sleep or block on async I/O.
    /// Same-owner write intents remain intact; callers that need exact rollback
    /// restoration must use `restore_pending` instead.
    fn stage_lock(
        &self,
        key: &[u8],
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), TxnStageError>;

    /// Restore the exact pending mutation previously owned by `owner_start_ts`.
    ///
    /// Used by statement rollback to overwrite the current same-owner pending
    /// head with the prior mutation, including restoring `Lock` or `Delete`
    /// after a later statement staged a different value. This method is
    /// synchronous and must never sleep or block on async I/O.
    fn restore_pending(
        &self,
        key: &[u8],
        mutation: TxnRestoreMutation<'_>,
        owner_start_ts: Timestamp,
    ) -> std::result::Result<(), TxnStageError>;

    /// Reserve the commit LSN for a transaction that has already finished
    /// staging its pending mutations and is about to write its durable commit.
    fn reserve_commit_lsn(&self, owner_start_ts: Timestamp) -> CommitReservation;

    /// Release a previously reserved commit LSN.
    ///
    /// Callers must release every reservation on failure/rollback paths.
    /// `abort()` does not release reservations implicitly.
    fn release_commit_lsn(&self, reservation: CommitReservation) -> bool;

    /// Finalize staged mutations after the matching durable commit record lands.
    ///
    /// Callers stage writes first, reserve exactly one commit LSN, durably log
    /// the transaction, then call `finalize()` with that still-held reservation.
    fn finalize(&self, req: FinalizeRequest<'_>) -> Result<()>;

    /// Abort the listed staged mutations for `owner_start_ts`.
    ///
    /// This only clears pending storage state; callers remain responsible for
    /// releasing any commit reservation they previously acquired.
    fn abort(&self, req: AbortRequest<'_>) -> Result<()>;
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum RecoveredWrite<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) trait TxnStorageRecoveryPort: TxnStoragePort {
    fn apply_recovered_write(
        &self,
        write: RecoveredWrite<'_>,
        commit_ts: Timestamp,
        lsn: Lsn,
    ) -> Result<()>;
}
