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
//! This module provides transaction management with durability guarantees,
//! including concurrency control (TSO and lock table).
//!
//! ## Key Abstractions
//!
//! - [`TxnService`]: The main entry point for creating transactions
//! - [`ReadSnapshot`]: Read-only transaction for SELECT statements
//! - [`Txn`]: Read-write transaction for DML statements
//!
//! ## Design Principles
//!
//! 1. **Interface-based**: SQL engine depends on traits, not implementations
//! 2. **Opaque handles**: Internal state (start_ts, commit_ts) is hidden
//! 3. **Read transactions get timestamps**: Even read-only queries allocate start_ts
//! 4. **Concurrency control**: TSO and lock table are internal to transaction layer
//!
//! ## Example
//!
//! ```ignore
//! // Read-only query - allocates start_ts for MVCC snapshot
//! let snapshot = txn_service.snapshot()?;
//! let value = snapshot.get(key)?;
//!
//! // Read-write transaction
//! let mut txn = txn_service.begin()?;
//! txn.put(key, value);
//! let info = txn.commit()?;
//! ```

mod api;
mod concurrency;
mod handle;
mod service;
mod snapshot;

// Public API - only expose traits and types needed by consumers
pub use api::{IsolationLevel, ReadSnapshot, Txn, TxnService};

// Implementation types - not re-exported from lib.rs main API
// Available via testkit for integration tests
pub use concurrency::{ConcurrencyManager, Lock};
pub use service::TransactionService;

// KeyGuard is used internally by handle.rs
#[allow(unused_imports)]
pub(crate) use concurrency::KeyGuard;
