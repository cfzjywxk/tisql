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
//! This module defines the unified transaction service interface following
//! OceanBase's design pattern where all transaction operations go through
//! a single service trait.
//!
//! ## Key Design Principles
//!
//! 1. **Unified interface**: All transaction operations in one trait (`TxnService`)
//! 2. **Context-based**: Transaction state is held in `TxnCtx`, passed to operations
//! 3. **No read-only distinction**: Even "reads" may write in distributed txn (lock resolution)
//! 4. **Storage details stay hidden**: callers interact with logical scan rows
//!    and opaque `StatementGuard`s, not MVCC iterators or rollback metadata
//!
//! ## Usage
//!
//! ```ignore
//! // Begin a transaction
//! let mut ctx = txn_service.begin(false)?;  // read_only = false
//!
//! // Read operations
//! let value = txn_service.get(&ctx, table_id, key).await?;
//!
//! // Write operations for direct transaction callers
//! let mut stmt = txn_service.begin_statement(&mut ctx);
//! txn_service.put(&mut ctx, &mut stmt, table_id, key, value).await?;
//!
//! // Commit
//! let info = txn_service.commit(ctx).await?;
//! ```

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::time::Duration;

use crate::catalog::types::{Key, Lsn, RawValue, TableId, Timestamp, TxnId};
use crate::util::error::Result;

/// Final per-key mutation payload tracked in a transaction context.
///
/// The payload mirrors what will be persisted to clog at commit:
/// - `Put(value)` for INSERT/UPDATE
/// - `Delete` for deletes on existing values
/// - `Lock` for delete/lock on non-existent keys (not persisted to clog)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationPayload {
    Put(RawValue),
    Delete,
    Lock,
}

impl MutationPayload {
    #[inline]
    pub fn is_write(&self) -> bool {
        !matches!(self, Self::Lock)
    }

    #[inline]
    pub fn is_lock(&self) -> bool {
        matches!(self, Self::Lock)
    }
}

/// Per-key mutation metadata tracked inside a transaction context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MutationMeta {
    /// Final mutation payload for commit/rollback handling.
    pub(crate) mutation: MutationPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StatementUndoEntry {
    Absent,
    Restore(MutationMeta),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StatementUndo {
    entries: BTreeMap<Key, StatementUndoEntry>,
}

impl StatementUndo {
    #[inline]
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    fn into_entries(self) -> BTreeMap<Key, StatementUndoEntry> {
        self.entries
    }

    #[inline]
    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.entries.contains_key(key)
    }

    pub(crate) fn record_first_touch(&mut self, key: &[u8], previous: Option<MutationMeta>) {
        self.entries
            .entry(key.to_vec())
            .or_insert_with(|| match previous {
                Some(previous) => StatementUndoEntry::Restore(previous),
                None => StatementUndoEntry::Absent,
            });
    }
}

/// Opaque statement-local rollback token.
#[derive(Debug, Default)]
pub struct StatementGuard {
    _private: (),
    undo: StatementUndo,
}

impl StatementGuard {
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.undo.is_empty()
    }

    #[inline]
    pub(crate) fn contains_key(&self, key: &[u8]) -> bool {
        self.undo.contains_key(key)
    }

    #[inline]
    pub(crate) fn record_first_touch(&mut self, key: &[u8], previous: Option<MutationMeta>) {
        self.undo.record_first_touch(key, previous);
    }

    #[inline]
    pub(crate) fn into_entries(self) -> BTreeMap<Key, StatementUndoEntry> {
        self.undo.into_entries()
    }
}

/// One logical key/value pair yielded by a transaction scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnScanEntry {
    pub user_key: Key,
    pub value: RawValue,
}

/// Forward-only logical scan cursor.
pub trait TxnScanCursor: Send {
    fn advance(&mut self) -> impl Future<Output = Result<()>> + Send + '_;
    fn current(&self) -> Option<&TxnScanEntry>;
}

/// Transaction state machine for OceanBase-style pessimistic locking.
///
/// State transitions:
/// ```text
///            BEGIN ───────► Running
///                              │
///                   PREPARE    │   (prepared_ts = max(max_ts, tso_ts))
///                              ▼
///                          Prepared ◄─── Readers with read_ts > prepared_ts
///                              │         must wait (KeyIsLocked)
///              ┌───────────────┼───────────────┐
///              │               │               │
///     COMMIT   │               │               │  ROLLBACK
///              ▼               │               ▼
///         Committed            │           Aborted
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    /// Transaction is actively executing (can perform reads and writes).
    Running,
    /// Transaction is computing commit_ts.
    /// Readers encountering pending nodes from this transaction must spin-wait
    /// until the state transitions to Prepared (with a known commit_ts).
    Preparing,
    /// Transaction has prepared, waiting for commit.
    /// The prepared_ts ensures atomic visibility: readers with read_ts > prepared_ts
    /// must wait because they cannot determine if commit_ts will be <= read_ts.
    Prepared {
        /// prepared_ts = max(current_max_ts, tso_ts)
        prepared_ts: crate::catalog::types::Timestamp,
    },
    /// Transaction has been committed.
    /// commit_ts = prepared_ts for single-server 1PC.
    Committed {
        /// The commit timestamp at which all writes became visible.
        commit_ts: crate::catalog::types::Timestamp,
    },
    /// Transaction has been aborted/rolled back.
    Aborted,
}

/// Transaction context - holds transaction state.
///
/// Similar to OceanBase's `ObTxDesc`. This struct is passed to all
/// transaction operations on `TxnService`.
///
/// The context is created by `TxnService::begin()` and consumed by
/// `commit()` or `rollback()`.
///
/// ## Explicit vs Implicit Transactions
///
/// Both explicit and implicit transactions use pessimistic locking:
/// writes go directly to storage as pending nodes, then are finalized at commit.
/// - **Explicit**: Started with BEGIN/START TRANSACTION, ended with COMMIT/ROLLBACK.
/// - **Implicit**: Auto-commit mode (default). Each statement is a transaction.
#[derive(Debug)]
pub struct TxnCtx {
    /// Unique transaction ID.
    pub(crate) txn_id: TxnId,
    /// Start timestamp for MVCC reads.
    pub(crate) start_ts: Timestamp,
    /// Current transaction state.
    pub(crate) state: TxnState,
    /// Whether this is a read-only transaction.
    pub(crate) read_only: bool,
    /// Whether this is an explicit transaction (started with BEGIN).
    /// Explicit transactions use pessimistic locking.
    pub(crate) explicit: bool,
    /// Keys mutated by this transaction with final per-key mutation payload.
    ///
    /// Each entry tracks the final operation (`Put`, `Delete`, or `Lock`).
    /// Commit serializes durable clog ops directly from this map.
    pub(crate) mutations: BTreeMap<Key, MutationMeta>,
    /// Whether this transaction has been registered in the state cache.
    /// Registration happens on first write (put/delete) for implicit txns,
    /// or at begin_explicit() for explicit txns.
    pub(crate) registered: bool,
    /// Schema version captured when an explicit transaction starts.
    ///
    /// Used by the SQL layer to detect DDL changes before COMMIT.
    /// `None` means schema checking is not enabled for this context.
    pub(crate) schema_version: Option<u64>,
    /// V2.6 commit reservation LSN (when storage reservation protocol is enabled).
    pub(crate) reserved_lsn: Option<Lsn>,
}

impl TxnCtx {
    /// Create a new implicit (auto-commit) transaction context.
    pub(crate) fn new(txn_id: TxnId, start_ts: Timestamp, read_only: bool) -> Self {
        Self {
            txn_id,
            start_ts,
            state: TxnState::Running,
            read_only,
            explicit: false,
            mutations: BTreeMap::new(),
            registered: false,
            schema_version: None,
            reserved_lsn: None,
        }
    }

    /// Create a new explicit transaction context.
    ///
    /// Explicit transactions are started with BEGIN/START TRANSACTION
    /// and ended with COMMIT/ROLLBACK.
    pub(crate) fn new_explicit(txn_id: TxnId, start_ts: Timestamp, read_only: bool) -> Self {
        Self {
            txn_id,
            start_ts,
            state: TxnState::Running,
            read_only,
            explicit: true,
            mutations: BTreeMap::new(),
            registered: false,
            schema_version: None,
            reserved_lsn: None,
        }
    }

    /// Create a transaction context for testing purposes.
    ///
    /// This is a public constructor that allows tests to create TxnCtx
    /// with specific parameters.
    #[cfg(test)]
    pub fn new_for_test(
        txn_id: TxnId,
        start_ts: Timestamp,
        read_only: bool,
        explicit: bool,
    ) -> Self {
        Self {
            txn_id,
            start_ts,
            state: TxnState::Running,
            read_only,
            explicit,
            mutations: BTreeMap::new(),
            registered: false,
            schema_version: None,
            reserved_lsn: None,
        }
    }

    /// Check if this is an explicit transaction (started with BEGIN).
    #[inline]
    pub fn is_explicit(&self) -> bool {
        self.explicit
    }

    /// Get the transaction ID.
    #[inline]
    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    /// Get the start timestamp.
    #[inline]
    pub fn start_ts(&self) -> Timestamp {
        self.start_ts
    }

    /// Check if this is a read-only transaction.
    #[inline]
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// Check if the transaction is still valid (active).
    #[inline]
    pub fn is_valid(&self) -> bool {
        matches!(
            self.state,
            TxnState::Running | TxnState::Preparing | TxnState::Prepared { .. }
        )
    }

    /// Get the current state.
    #[inline]
    pub fn state(&self) -> TxnState {
        self.state
    }

    /// Get schema version captured for this transaction, if any.
    #[inline]
    pub fn schema_version(&self) -> Option<u64> {
        self.schema_version
    }

    /// Capture schema version for DDL/DML concurrency check at COMMIT.
    #[inline]
    pub(crate) fn set_schema_version(&mut self, version: u64) {
        self.schema_version = Some(version);
    }
}

/// Transaction Service - **THE** interface for all data access operations.
///
/// This follows OceanBase's `ObTransService` pattern where all transaction
/// operations go through a single service interface. Transaction state is
/// held in `TxnCtx` and passed to each operation.
///
/// # IMPORTANT: Always Use TxnService for Reads
///
/// **All reads MUST go through `TxnService`** to ensure proper MVCC semantics.
/// Do not access `StorageEngine` directly for reading data.
///
/// ```ignore
/// // CORRECT: MVCC-aware read with transaction semantics
/// let ctx = txn_service.begin(true)?;  // read-only transaction
/// let value = txn_service.get(&ctx, table_id, key)?;
/// let mut scan = txn_service.scan(&ctx, table_id, range)?;
///
/// // WRONG: Bypasses MVCC visibility rules
/// let value = storage.get(key)?;  // DON'T DO THIS
/// ```
///
/// ## Why TxnService for Reads?
///
/// 1. **MVCC Visibility**: Reads see data at `ctx.start_ts`, ignoring uncommitted
///    writes and later commits for snapshot isolation.
///
/// 2. **Lock Checking**: Reads check for conflicting locks from concurrent
///    transactions, returning `KeyIsLocked` error when blocked.
///
/// 3. **Consistent Timestamps**: The `TxnCtx` carries `start_ts` for debugging
///    and ensures all operations in a transaction use the same snapshot.
///
/// ## Read-Your-Writes
///
/// Both implicit and explicit transactions write pending nodes to storage.
/// Reads use `owner_ts=start_ts` to see own pending writes, providing
/// read-your-writes semantics.
///
/// ## Design Rationale
///
/// In distributed transactions (2PC), even "read" operations may need to:
/// - Push forward `min_commit_ts` of concurrent writes
/// - Resolve stale locks
///
/// Therefore, there's no fundamental distinction between read-only and
/// read-write transactions at the API level. The `read_only` flag is
/// a hint for optimization, not a hard constraint.
pub trait TxnService: Send + Sync {
    /// The cursor type returned by `scan`.
    ///
    /// Using an associated type avoids boxing and dynamic dispatch overhead.
    /// Each transaction service implementation defines its own concrete cursor type.
    type ScanCursor: TxnScanCursor + 'static;

    // === Factory ===

    /// Begin a new implicit (auto-commit) transaction.
    ///
    /// Allocates a `start_ts` from TSO and returns a transaction context.
    /// If `read_only` is true, write operations will error.
    fn begin(&self, read_only: bool) -> Result<TxnCtx>;

    /// Begin a new explicit transaction (BEGIN/START TRANSACTION).
    ///
    /// Allocates a `start_ts` from TSO and returns a transaction context
    /// marked as explicit. If `read_only` is true, write operations will error.
    fn begin_explicit(&self, read_only: bool) -> Result<TxnCtx>;

    /// Read current transaction state from the shared state cache.
    ///
    /// Used by background safety sweeps (for example reservation leak defense).
    fn get_txn_state(&self, start_ts: Timestamp) -> Option<TxnState>;

    // === Data Operations ===

    /// Read a key within the transaction.
    ///
    /// Reads from storage at `start_ts`, returning the latest visible version.
    /// Returns `None` if the key doesn't exist or was deleted.
    /// Sees own pending writes (read-your-writes).
    ///
    fn get<'a>(
        &'a self,
        ctx: &'a TxnCtx,
        table_id: TableId,
        key: &'a [u8],
    ) -> impl Future<Output = Result<Option<RawValue>>> + Send + 'a;

    /// Record time spent in duplicate-check reads on write paths.
    ///
    /// The default implementation is a no-op so test mocks and alternative
    /// transaction services do not need to provide diagnostics.
    fn record_duplicate_check_duration(&self, _duration: Duration) {}

    /// Scan a range of keys within one table target (streaming).
    ///
    /// Returns a forward-only cursor over logical key-value pairs visible at `start_ts`.
    /// For explicit transactions, sees own pending writes (read-your-writes).
    ///
    fn scan(&self, ctx: &TxnCtx, table_id: TableId, range: Range<Key>) -> Result<Self::ScanCursor>;

    #[doc(hidden)]
    fn scan_iter(
        &self,
        ctx: &TxnCtx,
        table_id: TableId,
        range: Range<Key>,
    ) -> Result<Self::ScanCursor> {
        self.scan(ctx, table_id, range)
    }

    /// Begin a statement-local rollback scope for subsequent staged writes.
    fn begin_statement(&self, ctx: &mut TxnCtx) -> StatementGuard;

    /// Write a pending put to storage.
    ///
    /// The write is not visible to other transactions until commit.
    /// May read from SSTs for conflict detection (async I/O).
    ///
    /// # Errors
    ///
    /// - `ReadOnlyTransaction` if the transaction was started with `read_only = true`
    /// - `TransactionNotActive` if the transaction is not in `Active` state
    ///
    fn put<'a>(
        &'a self,
        ctx: &'a mut TxnCtx,
        stmt: &'a mut StatementGuard,
        table_id: TableId,
        key: &'a [u8],
        value: RawValue,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Write a pending delete to storage.
    ///
    /// The delete is not visible to other transactions until commit.
    /// May read from SSTs for conflict detection (async I/O).
    ///
    /// # Errors
    ///
    /// - `ReadOnlyTransaction` if the transaction was started with `read_only = true`
    /// - `TransactionNotActive` if the transaction is not in `Active` state
    ///
    fn delete<'a>(
        &'a self,
        ctx: &'a mut TxnCtx,
        stmt: &'a mut StatementGuard,
        table_id: TableId,
        key: &'a [u8],
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Acquire a pessimistic lock marker on a key without changing user data.
    ///
    /// This is used by no-op current reads (for example, UPDATE that matches a row
    /// but produces the same values) so commit still protects the read set.
    fn lock_key<'a>(
        &'a self,
        ctx: &'a mut TxnCtx,
        stmt: &'a mut StatementGuard,
        table_id: TableId,
        key: &'a [u8],
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    /// Roll back only the mutations created by the current statement.
    fn rollback_statement<'a>(
        &'a self,
        ctx: &'a mut TxnCtx,
        stmt: StatementGuard,
    ) -> impl Future<Output = Result<()>> + Send + 'a;

    // === Finalization ===

    /// Commit the transaction.
    ///
    /// For read-only transactions with no writes, returns immediately.
    /// For read-write transactions:
    /// 1. Acquire in-memory locks
    /// 2. Write to commit log (async — yields while fsync completes)
    /// 3. Apply to storage with commit_ts
    /// 4. Release locks
    fn commit(&self, ctx: TxnCtx) -> impl Future<Output = Result<CommitInfo>> + Send + '_;

    /// Rollback the transaction.
    ///
    /// All pending writes are aborted.
    fn rollback(&self, ctx: TxnCtx) -> Result<()>;
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
    use std::future;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct DummyCursor {
        current: Option<TxnScanEntry>,
    }

    impl TxnScanCursor for DummyCursor {
        fn advance(&mut self) -> impl Future<Output = Result<()>> + Send + '_ {
            future::ready(Ok(()))
        }

        fn current(&self) -> Option<&TxnScanEntry> {
            self.current.as_ref()
        }
    }

    #[derive(Default)]
    struct DummyTxnService {
        scan_calls: AtomicUsize,
    }

    impl TxnService for DummyTxnService {
        type ScanCursor = DummyCursor;

        fn begin(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new(1, 10, read_only))
        }

        fn begin_explicit(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new_explicit(2, 20, read_only))
        }

        fn get_txn_state(&self, _start_ts: Timestamp) -> Option<TxnState> {
            None
        }

        fn get<'a>(
            &'a self,
            _ctx: &'a TxnCtx,
            _table_id: TableId,
            _key: &'a [u8],
        ) -> impl Future<Output = Result<Option<RawValue>>> + Send + 'a {
            future::ready(Ok(Some(b"value".to_vec())))
        }

        fn scan(
            &self,
            _ctx: &TxnCtx,
            _table_id: TableId,
            _range: Range<Key>,
        ) -> Result<Self::ScanCursor> {
            self.scan_calls.fetch_add(1, Ordering::SeqCst);
            Ok(DummyCursor {
                current: Some(TxnScanEntry {
                    user_key: b"k".to_vec(),
                    value: b"v".to_vec(),
                }),
            })
        }

        fn begin_statement(&self, _ctx: &mut TxnCtx) -> StatementGuard {
            StatementGuard::default()
        }

        fn put<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: &'a mut StatementGuard,
            _table_id: TableId,
            _key: &'a [u8],
            _value: RawValue,
        ) -> impl Future<Output = Result<()>> + Send + 'a {
            future::ready(Ok(()))
        }

        fn delete<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: &'a mut StatementGuard,
            _table_id: TableId,
            _key: &'a [u8],
        ) -> impl Future<Output = Result<()>> + Send + 'a {
            future::ready(Ok(()))
        }

        fn lock_key<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: &'a mut StatementGuard,
            _table_id: TableId,
            _key: &'a [u8],
        ) -> impl Future<Output = Result<()>> + Send + 'a {
            future::ready(Ok(()))
        }

        fn rollback_statement<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: StatementGuard,
        ) -> impl Future<Output = Result<()>> + Send + 'a {
            future::ready(Ok(()))
        }

        fn commit(&self, ctx: TxnCtx) -> impl Future<Output = Result<CommitInfo>> + Send + '_ {
            future::ready(Ok(CommitInfo {
                txn_id: ctx.txn_id(),
                commit_ts: ctx.start_ts(),
                lsn: 0,
            }))
        }

        fn rollback(&self, _ctx: TxnCtx) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_isolation_level_default() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::SnapshotIsolation);
    }

    #[test]
    fn test_txn_ctx_new() {
        let ctx = TxnCtx::new(1, 100, false);
        assert_eq!(ctx.txn_id(), 1);
        assert_eq!(ctx.start_ts(), 100);
        assert!(!ctx.is_read_only());
        assert!(ctx.is_valid());
        assert_eq!(ctx.state(), TxnState::Running);
    }

    #[test]
    fn test_txn_ctx_read_only() {
        let ctx = TxnCtx::new(2, 200, true);
        assert!(ctx.is_read_only());
        assert!(ctx.is_valid());
    }

    #[test]
    fn test_txn_ctx_validity_for_intermediate_states() {
        let mut ctx = TxnCtx::new(3, 300, false);
        ctx.state = TxnState::Preparing;
        assert!(ctx.is_valid());

        ctx.state = TxnState::Prepared { prepared_ts: 301 };
        assert!(ctx.is_valid());

        ctx.state = TxnState::Committed { commit_ts: 302 };
        assert!(!ctx.is_valid());

        ctx.state = TxnState::Aborted;
        assert!(!ctx.is_valid());
    }

    #[test]
    fn test_statement_guard_tracks_entries() {
        let previous = MutationMeta {
            mutation: MutationPayload::Delete,
        };
        let mut guard = StatementGuard::default();
        assert!(guard.is_empty());

        guard.record_first_touch(b"k1", None);
        guard.record_first_touch(b"k1", Some(previous.clone()));
        guard.record_first_touch(b"k2", Some(previous.clone()));

        assert!(!guard.is_empty());
        assert!(guard.contains_key(b"k1"));
        assert!(guard.contains_key(b"k2"));

        let entries = guard.into_entries();
        assert_eq!(entries.len(), 2);
        assert_eq!(
            entries.get(b"k1".as_slice()),
            Some(&StatementUndoEntry::Absent)
        );
        assert_eq!(
            entries.get(b"k2".as_slice()),
            Some(&StatementUndoEntry::Restore(previous))
        );

        assert!(MutationPayload::Put(b"v".to_vec()).is_write());
        assert!(MutationPayload::Delete.is_write());
        assert!(MutationPayload::Lock.is_lock());
        assert!(!MutationPayload::Lock.is_write());
    }

    #[test]
    fn test_txn_ctx_new_for_test_tracks_explicit_and_schema_version() {
        let mut ctx = TxnCtx::new_explicit(7, 70, true);
        assert!(ctx.is_explicit());
        assert!(ctx.is_read_only());
        assert_eq!(ctx.schema_version(), None);

        ctx.set_schema_version(42);
        assert_eq!(ctx.schema_version(), Some(42));
    }

    #[tokio::test]
    async fn test_txn_service_default_helpers_delegate_to_scan() {
        let service = DummyTxnService::default();
        let ctx = service.begin(true).unwrap();

        service.record_duplicate_check_duration(Duration::from_micros(5));
        let mut cursor = service
            .scan_iter(&ctx, 9, b"a".to_vec()..b"z".to_vec())
            .unwrap();

        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(cursor.current().unwrap().user_key, b"k".to_vec());
        cursor.advance().await.unwrap();
    }
}
