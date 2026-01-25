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

//! Timestamp Oracle (TSO) service.
//!
//! This module provides timestamp allocation for MVCC transactions.
//! Following TiDB/PD patterns, TSO is a standalone service that provides
//! globally monotonic timestamps.
//!
//! ## Design (TiDB/PD Reference)
//!
//! In TiDB/TiKV architecture:
//! - **PD TSO**: Centralized timestamp oracle (server/grpc_service.go, pkg/tso/)
//! - **Oracle interface**: Client-side abstraction (client-go/oracle/oracle.go)
//!
//! Key characteristics:
//! - Timestamps are globally unique and monotonically increasing
//! - TSO is independent of other modules (storage, concurrency control)
//! - Composition: `(physical_ms << 18) | logical` (18 bits for logical)
//!
//! ## Module Structure
//!
//! - `TsoService` trait: Core interface for timestamp allocation
//! - `LocalTso`: In-memory implementation (for single-node deployment)
//! - Future: `PdTso` for distributed deployment with PD

mod local;

pub use local::LocalTso;

use crate::types::Timestamp;

/// Timestamp Oracle service trait.
///
/// Provides globally monotonic timestamps for MVCC transactions.
/// All timestamp allocation (start_ts, commit_ts) should go through this trait.
///
/// ## Thread Safety
///
/// Implementations must be thread-safe (Send + Sync) to support concurrent
/// timestamp allocation from multiple transactions.
///
/// ## Monotonicity Guarantee
///
/// Each call to `get_ts()` must return a timestamp strictly greater than
/// all previously returned timestamps. This is critical for MVCC correctness.
pub trait TsoService: Send + Sync + 'static {
    /// Allocate the next timestamp.
    ///
    /// Returns a monotonically increasing timestamp.
    /// This is used for both `start_ts` (in begin) and `commit_ts` (in commit).
    fn get_ts(&self) -> Timestamp;

    /// Allocate multiple timestamps in batch.
    ///
    /// Returns the first timestamp of the batch. The caller can use
    /// `first..first+count` as the allocated range.
    ///
    /// Default implementation calls `get_ts()` `count` times.
    fn get_ts_batch(&self, count: u32) -> Timestamp {
        if count == 0 {
            return self.get_ts();
        }
        let first = self.get_ts();
        for _ in 1..count {
            self.get_ts();
        }
        first
    }

    /// Get current timestamp without incrementing.
    ///
    /// Returns the last allocated timestamp (or initial value if none allocated).
    /// This is useful for checking the current TSO state.
    fn current_ts(&self) -> Timestamp;

    /// Set the timestamp counter (used during recovery).
    ///
    /// After recovery, call this with `max_recovered_ts + 1` to ensure
    /// new timestamps are greater than all recovered ones.
    fn set_ts(&self, ts: Timestamp);

    /// Check if the given timestamp is valid (not in the future).
    ///
    /// Returns true if `ts <= current_ts()`.
    fn is_valid_ts(&self, ts: Timestamp) -> bool {
        ts <= self.current_ts()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tso_monotonicity() {
        let tso = LocalTso::new(1);

        let ts1 = tso.get_ts();
        let ts2 = tso.get_ts();
        let ts3 = tso.get_ts();

        assert!(ts1 < ts2);
        assert!(ts2 < ts3);
    }

    #[test]
    fn test_tso_batch() {
        let tso = LocalTso::new(1);

        let first = tso.get_ts_batch(5);
        let next = tso.get_ts();

        // After getting batch of 5, next should be first + 5
        assert_eq!(next, first + 5);
    }

    #[test]
    fn test_tso_set_ts() {
        let tso = LocalTso::new(1);

        tso.get_ts(); // 1
        tso.get_ts(); // 2

        tso.set_ts(100);
        let ts = tso.get_ts();
        assert_eq!(ts, 100);
    }
}
