use crate::error::Result;
use crate::types::{Key, Lsn, RawValue, Timestamp, TxnId};

/// Log entry for WAL
#[derive(Clone, Debug)]
pub struct LogEntry {
    pub lsn: Lsn,
    pub txn_id: TxnId,
    pub op: LogOp,
}

/// Log operation types
#[derive(Clone, Debug)]
pub enum LogOp {
    Put { key: Key, value: RawValue },
    Delete { key: Key },
    Commit { commit_ts: Timestamp },
    Rollback,
}

/// Write-ahead log interface
pub trait WriteAheadLog: Send + Sync {
    /// Append entry, returns LSN
    fn append(&self, entry: LogEntry) -> Result<Lsn>;

    /// Append batch of entries atomically
    fn append_batch(&self, entries: Vec<LogEntry>) -> Result<Lsn>;

    /// Sync to disk up to LSN
    fn sync(&self, lsn: Lsn) -> Result<()>;

    /// Read entries from LSN (for recovery)
    fn read_from(&self, lsn: Lsn) -> Result<Vec<LogEntry>>;

    /// Truncate log before LSN (after checkpoint)
    fn truncate_before(&self, lsn: Lsn) -> Result<()>;
}

// File-based WAL will be implemented in M4 milestone
