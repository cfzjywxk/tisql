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
