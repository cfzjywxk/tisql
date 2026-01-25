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

//! Transaction service API definitions.
//!
//! This module defines the core transaction service interfaces following the
//! design patterns from TiKV/OceanBase:
//!
//! - `TxnService`: The main entry point for creating transactions
//! - `ReadSnapshot`: Read-only transaction handle for SELECT statements
//! - `Txn`: Read-write transaction handle for DML statements
//!
//! ## Key Design Principles
//!
//! 1. **Opaque handles**: Internal state (start_ts, commit_ts) is hidden from consumers
//! 2. **Interface separation**: Read-only vs read-write transactions have different interfaces
//! 3. **Trait-based abstraction**: SQL engine depends on traits, not implementations
//!
//! ## Usage
//!
//! ```ignore
//! // Read-only query
//! let snapshot = txn_service.snapshot()?;
//! let value = snapshot.get(key)?;
//!
//! // Read-write transaction
//! let mut txn = txn_service.begin()?;
//! txn.put(key, value);
//! let info = txn.commit()?;
//! ```

use std::ops::Range;

use crate::error::Result;
use crate::types::{Key, Lsn, RawValue, Timestamp, TxnId};

/// Transaction service trait - the main entry point for transactions.
///
/// This is the "thin waist" that all transaction operations go through.
/// Implementations hide internal details like TSO, lock tables, and 2PC.
pub trait TxnService: Send + Sync {
    /// Begin a new read-write transaction.
    ///
    /// The transaction is in "active" state after this call.
    /// All writes are buffered until commit.
    fn begin(&self) -> Result<Box<dyn Txn>>;

    /// Create a read-only snapshot for consistent reads.
    ///
    /// The snapshot is assigned a start_ts from TSO and provides
    /// a consistent view of the database at that timestamp.
    fn snapshot(&self) -> Result<Box<dyn ReadSnapshot>>;

    /// Get the current timestamp from TSO (for debugging/testing only).
    fn current_ts(&self) -> Timestamp;
}

/// Read-only snapshot handle for SELECT statements.
///
/// A snapshot provides a consistent read view at a specific timestamp.
/// All reads see the same version of data regardless of concurrent writes.
///
/// ## MVCC Visibility
///
/// The snapshot can only see data with commit_ts <= start_ts.
/// Concurrent writes with higher timestamps are invisible.
pub trait ReadSnapshot: Send + Sync {
    /// Get the snapshot's read timestamp.
    ///
    /// This is the timestamp at which all reads are performed.
    /// Data with commit_ts > start_ts is invisible.
    fn start_ts(&self) -> Timestamp;

    /// Read a key at the snapshot timestamp.
    ///
    /// Returns the value visible at start_ts, or None if the key
    /// doesn't exist or was deleted before start_ts.
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>>;

    /// Scan a range of keys at the snapshot timestamp.
    ///
    /// Returns an iterator over key-value pairs visible at start_ts.
    fn scan(&self, range: Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>>;
}

/// Read-write transaction handle for DML statements.
///
/// A transaction buffers writes and applies them atomically on commit.
/// The transaction follows 1PC (one-phase commit) pattern:
///
/// 1. Acquire in-memory locks (blocks concurrent readers)
/// 2. Write to commit log
/// 3. Apply to storage with commit_ts
/// 4. Release locks
///
/// ## State Machine
///
/// ```text
/// Active -> Committed
///        -> Aborted
/// ```
pub trait Txn: ReadSnapshot {
    /// Get the transaction ID (for logging/debugging).
    fn txn_id(&self) -> TxnId;

    /// Check if the transaction is still valid (not committed/aborted).
    fn is_valid(&self) -> bool;

    /// Buffer a put operation.
    ///
    /// The write is not visible until commit.
    fn put(&mut self, key: Key, value: RawValue);

    /// Buffer a delete operation.
    ///
    /// The delete is not visible until commit.
    fn delete(&mut self, key: Key);

    /// Commit the transaction and return commit info.
    ///
    /// This is a consuming operation - the transaction cannot be used after.
    /// All buffered writes are applied atomically.
    fn commit(self: Box<Self>) -> Result<CommitInfo>;

    /// Rollback the transaction.
    ///
    /// All buffered writes are discarded.
    fn rollback(self: Box<Self>) -> Result<()>;
}

/// Information returned after a successful commit.
#[derive(Debug, Clone)]
pub struct CommitInfo {
    /// Transaction ID
    pub txn_id: TxnId,
    /// Commit timestamp
    pub commit_ts: Timestamp,
    /// Log sequence number (for durability tracking)
    pub lsn: Lsn,
}

/// Options for beginning a transaction (for future explicit transaction support).
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct BeginOptions {
    /// Whether this is a read-only transaction.
    /// Read-only transactions don't acquire locks.
    pub read_only: bool,

    /// Isolation level (for future use).
    pub isolation: IsolationLevel,
}

/// Options for creating a snapshot (for future stale read support).
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct SnapshotOptions {
    /// Use a specific timestamp instead of getting from TSO.
    /// This is for stale reads.
    pub ts: Option<Timestamp>,
}

/// Transaction isolation levels.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Snapshot Isolation (TiDB default)
    #[default]
    SnapshotIsolation,

    /// Read Committed
    ReadCommitted,

    /// Repeatable Read (MySQL default)
    RepeatableRead,

    /// Serializable
    Serializable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_level_default() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::SnapshotIsolation);
    }

    #[test]
    fn test_begin_options_default() {
        let opts = BeginOptions::default();
        assert!(!opts.read_only);
        assert_eq!(opts.isolation, IsolationLevel::SnapshotIsolation);
    }
}
