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

//! Transaction layer for TiSQL.
//!
//! This module provides transaction management with durability guarantees.
//! Concurrency control is handled by:
//! - `TsoService` (separate module) - timestamp allocation
//! - `ConcurrencyManager` - max_ts tracking and transaction state management
//! - `PessimisticStorage` - lock management via pending nodes in storage
//!
//! ## Design Pattern
//!
//! Following OceanBase's `ObTransService` pattern, all transaction operations
//! go through a single [`TxnService`] trait. Transaction state is held in
//! [`TxnCtx`] and passed to each operation.
//!
//! ## Key Abstractions
//!
//! - [`TxnService`]: Unified interface for all transaction operations
//! - [`TxnCtx`]: Transaction context holding state (passed to operations)
//! - [`CommitInfo`]: Information returned after successful commit
//! - `TsoService` (in `tso` module): Timestamp allocation (start_ts, commit_ts)
//! - `ConcurrencyManager`: max_ts tracking and TxnStateCache
//!
//! ## Design Principles
//!
//! 1. **Unified interface**: All operations go through `TxnService`
//! 2. **Context-based**: Transaction state is in `TxnCtx`, passed to operations
//! 3. **No read-only distinction at API level**: Even "reads" may write in
//!    distributed transactions (lock resolution, min_commit_ts push)
//! 4. **Separated concerns**: TSO allocates timestamps, storage handles locks
//!
//! ## Example
//!
//! ```ignore
//! // Begin a transaction
//! let mut ctx = txn_service.begin(false)?;  // read_only = false
//!
//! // Read operations
//! let value = txn_service.get(&ctx, table_id, key)?;
//!
//! // Write operations
//! txn_service.put(&mut ctx, table_id, key, value)?;
//!
//! // Commit
//! let info = txn_service.commit(ctx)?;
//! ```

mod api;
mod concurrency;
mod service;
mod txn_state_cache;

// Public API - only expose traits and types needed by consumers
pub use api::{
    CommitInfo, IsolationLevel, MutationPayload, StatementGuard, TxnCtx, TxnScanCursor,
    TxnScanEntry, TxnService, TxnState,
};

pub(crate) use api::MutationMeta;

// Implementation types - not re-exported from lib.rs main API
// Available via testkit for integration tests
pub use concurrency::ConcurrencyManager;
pub use service::{ConflictWaitConfig, TransactionService};
