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
use crate::types::{Key, RawValue, Timestamp, TxnId};
use std::ops::Range;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
    SnapshotIsolation,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        Self::SnapshotIsolation
    }
}

/// Transaction interface
pub trait Transaction: Send {
    /// Get transaction ID
    fn id(&self) -> TxnId;

    /// Get start timestamp
    fn start_ts(&self) -> Timestamp;

    /// Read a key within transaction
    fn get(&self, key: &[u8]) -> Result<Option<RawValue>>;

    /// Write a key within transaction (buffered until commit)
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key within transaction
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Scan range within transaction
    fn scan(&self, range: Range<Key>) -> Result<Vec<(Key, RawValue)>>;

    /// Commit transaction - returns commit timestamp on success
    fn commit(self) -> Result<Timestamp>;

    /// Rollback transaction
    fn rollback(self) -> Result<()>;
}

/// Transaction manager - creates and coordinates transactions
pub trait TransactionManager: Send + Sync {
    type Txn: Transaction;

    /// Begin a new transaction
    fn begin(&self, isolation: IsolationLevel) -> Result<Self::Txn>;

    /// Get current global timestamp
    fn current_ts(&self) -> Timestamp;
}

// Simple implementation will be added in M2 milestone
