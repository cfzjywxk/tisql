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

// Implementation types - not re-exported from main API
// Available via testkit for integration tests
pub use file::{FileClogConfig, FileClogService};

use crate::error::Result;
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

/// Commit log service interface
pub trait ClogService: Send + Sync {
    /// Append batch atomically, returns last LSN
    fn write(&self, batch: &mut ClogBatch, sync: bool) -> Result<Lsn>;

    /// Append single entry, returns LSN
    fn append(&self, entry: ClogEntry, sync: bool) -> Result<Lsn> {
        let mut batch = ClogBatch::new();
        batch.add(entry);
        self.write(&mut batch, sync)
    }

    /// Sync all pending writes to disk
    fn sync(&self) -> Result<()>;

    /// Read all entries (for recovery)
    fn read_all(&self) -> Result<Vec<ClogEntry>>;

    /// Get current LSN
    fn current_lsn(&self) -> Lsn;

    /// Close the commit log service
    fn close(&self) -> Result<()>;
}
