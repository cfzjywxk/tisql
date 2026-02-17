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

//! Commit Log (clog) module for durability.
//!
//! This module provides a file-based commit log service inspired by TiKV's raft-engine.
//! Key features:
//! - Sequential append-only writes
//! - CRC32 checksums for integrity
//! - Recovery by replaying log files
//! - fsync for durability guarantees
//!
//! Named "clog" (commit log) following OceanBase convention to avoid
//! confusion with application logging.

mod file;
pub(crate) mod group_commit;

// Implementation types - not re-exported from main API
// Available via testkit for integration tests
pub use file::{FileClogConfig, FileClogService, TruncateStats};
pub(crate) use group_commit::GroupCommitWriter;
// Note: ClogFsyncFuture is defined below in this file

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::{Result, TiSqlError};
use crate::types::{Key, Lsn, RawValue, Timestamp, TxnId};
use serde::{Deserialize, Serialize};

/// Commit log entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClogEntry {
    /// Log sequence number
    pub lsn: Lsn,
    /// Transaction ID (0 for DDL operations)
    pub txn_id: TxnId,
    /// The operation
    pub op: ClogOp,
}

/// Commit log operation types
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClogOp {
    /// Put a key-value pair
    Put { key: Key, value: RawValue },
    /// Delete a key
    Delete { key: Key },
    /// Commit a transaction
    Commit { commit_ts: Timestamp },
    /// Rollback a transaction
    Rollback,
}

/// Reference-based clog entry for zero-copy serialization.
///
/// This allows serializing directly from borrowed data without cloning.
/// The on-disk format is identical to `ClogEntry`.
#[derive(Serialize)]
pub(crate) struct ClogEntryRef<'a> {
    pub lsn: Lsn,
    pub txn_id: TxnId,
    pub op: ClogOpRef<'a>,
}

/// Reference-based clog operation for zero-copy serialization.
#[derive(Clone, Copy, Serialize)]
pub enum ClogOpRef<'a> {
    Put { key: &'a [u8], value: &'a [u8] },
    Delete { key: &'a [u8] },
    Commit { commit_ts: Timestamp },
}

/// Batch of commit log entries for atomic append
#[derive(Default)]
pub struct ClogBatch {
    entries: Vec<ClogEntry>,
}

impl ClogBatch {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a commit log entry
    pub fn add(&mut self, entry: ClogEntry) {
        self.entries.push(entry);
    }

    /// Add a put operation
    pub fn add_put(&mut self, txn_id: TxnId, key: Key, value: RawValue) {
        self.add(ClogEntry {
            lsn: 0, // Assigned by ClogService
            txn_id,
            op: ClogOp::Put { key, value },
        });
    }

    /// Add a delete operation
    pub fn add_delete(&mut self, txn_id: TxnId, key: Key) {
        self.add(ClogEntry {
            lsn: 0,
            txn_id,
            op: ClogOp::Delete { key },
        });
    }

    /// Add a commit record
    pub fn add_commit(&mut self, txn_id: TxnId, commit_ts: Timestamp) {
        self.add(ClogEntry {
            lsn: 0,
            txn_id,
            op: ClogOp::Commit { commit_ts },
        });
    }

    /// Get the entries
    pub fn entries(&self) -> &[ClogEntry] {
        &self.entries
    }

    /// Take ownership of entries
    pub fn into_entries(self) -> Vec<ClogEntry> {
        self.entries
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Commit log service interface.
///
/// All write methods return a `ClogFsyncFuture` that resolves when the
/// write (+ optional fsync) is durable. Callers in async contexts `.await`
/// the future.
pub trait ClogService: Send + Sync {
    /// Append batch atomically, returns future that yields the last LSN.
    fn write(&self, batch: &mut ClogBatch, sync: bool) -> Result<ClogFsyncFuture>;

    /// Write pre-built clog ops directly.
    ///
    /// This is the zero-copy commit path: the caller builds `ClogOpRef` entries
    /// that borrow keys from the transaction's mutations BTreeMap and values from
    /// storage reads. The clog allocates LSNs, appends a Commit record, serializes,
    /// and submits to group commit. No key or value cloning occurs.
    ///
    /// V2.6 production commit paths should prefer `write_ops_with_lsn` via strict
    /// reservation flow. `write_ops` remains for compatibility/test paths.
    fn write_ops(
        &self,
        txn_id: TxnId,
        ops: &[ClogOpRef<'_>],
        commit_ts: Timestamp,
        sync: bool,
    ) -> Result<ClogFsyncFuture>;

    /// Write pre-built clog ops using a caller-provided transaction LSN.
    ///
    /// This is used by V2.6 reservation path where storage allocates+reserves
    /// the commit LSN before WAL append.
    fn write_ops_with_lsn(
        &self,
        txn_id: TxnId,
        ops: &[ClogOpRef<'_>],
        commit_ts: Timestamp,
        lsn: Lsn,
        sync: bool,
    ) -> Result<ClogFsyncFuture>;

    /// Append single entry, returns future that yields the LSN.
    fn append(&self, entry: ClogEntry, sync: bool) -> Result<ClogFsyncFuture> {
        let mut batch = ClogBatch::new();
        batch.add(entry);
        self.write(&mut batch, sync)
    }

    /// Sync all pending writes to disk.
    fn sync(&self) -> Result<ClogFsyncFuture>;

    /// Read all entries (for recovery)
    fn read_all(&self) -> Result<Vec<ClogEntry>>;

    /// Get current LSN
    fn current_lsn(&self) -> Lsn;

    /// Close the commit log service
    fn close(&self) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Shutdown the group commit writer channel.
    ///
    /// Must be called before the io_runtime is dropped, otherwise the runtime
    /// drop blocks waiting for the writer loop (spawn_blocking task) to exit.
    fn shutdown(&self);
}

/// Future that resolves when a clog fsync completes, yielding the assigned LSN.
///
/// This is a concrete Future type (no `Box<dyn Future>`, no `async_trait`).
/// The serialization happens eagerly before this future is created, so it only
/// holds a `Lsn` and a `tokio::sync::oneshot::Receiver` — both `Send + 'static`.
pub struct ClogFsyncFuture {
    lsn: Lsn,
    rx: tokio::sync::oneshot::Receiver<std::result::Result<(), String>>,
}

impl ClogFsyncFuture {
    pub(crate) fn new(
        lsn: Lsn,
        rx: tokio::sync::oneshot::Receiver<std::result::Result<(), String>>,
    ) -> Self {
        Self { lsn, rx }
    }
}

impl Future for ClogFsyncFuture {
    type Output = Result<Lsn>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: we only project to rx which is Unpin (oneshot::Receiver is Unpin).
        let this = self.get_mut();
        match Pin::new(&mut this.rx).poll(cx) {
            Poll::Ready(Ok(Ok(()))) => Poll::Ready(Ok(this.lsn)),
            Poll::Ready(Ok(Err(e))) => {
                Poll::Ready(Err(TiSqlError::Internal(format!("Clog fsync error: {e}"))))
            }
            Poll::Ready(Err(_)) => Poll::Ready(Err(TiSqlError::Internal(
                "Clog writer thread dropped".into(),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}
