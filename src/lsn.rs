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

//! Log Sequence Number (LSN) provider for unified LSN allocation.
//!
//! This module provides a shared LSN provider that can be used by both
//! the commit log (clog) and index log (ilog) services to ensure consistent
//! ordering during recovery.
//!
//! ## Design
//!
//! A single LSN counter is shared between clog and ilog:
//! - clog records transaction data (puts, deletes, commits)
//! - ilog records SST metadata (flush/compact intents and commits)
//!
//! During recovery, both logs are replayed in LSN order to reconstruct
//! a consistent state.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::types::Lsn;

/// Trait for LSN allocation.
///
/// Implementations must be thread-safe and guarantee monotonically
/// increasing LSN values.
pub trait LsnProvider: Send + Sync {
    /// Allocate the next LSN.
    ///
    /// Each call returns a unique, monotonically increasing value.
    fn alloc_lsn(&self) -> Lsn;

    /// Get the current LSN (next to be allocated).
    ///
    /// This is useful for recovery to know where to resume.
    fn current_lsn(&self) -> Lsn;

    /// Set the current LSN (used during recovery).
    ///
    /// This should only be called during startup to restore
    /// the LSN counter from persisted state.
    fn set_lsn(&self, lsn: Lsn);
}

/// Default LSN provider using atomic counter.
///
/// This is the standard implementation used in production.
/// It's thread-safe and lock-free.
#[derive(Debug)]
pub struct AtomicLsnProvider {
    next_lsn: AtomicU64,
}

impl AtomicLsnProvider {
    /// Create a new LSN provider starting from 1.
    pub fn new() -> Self {
        Self {
            next_lsn: AtomicU64::new(1),
        }
    }

    /// Create a new LSN provider starting from a specific value.
    ///
    /// Used during recovery to restore the LSN counter.
    pub fn with_start(start_lsn: Lsn) -> Self {
        Self {
            next_lsn: AtomicU64::new(start_lsn),
        }
    }
}

impl Default for AtomicLsnProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl LsnProvider for AtomicLsnProvider {
    fn alloc_lsn(&self) -> Lsn {
        self.next_lsn.fetch_add(1, Ordering::SeqCst)
    }

    fn current_lsn(&self) -> Lsn {
        self.next_lsn.load(Ordering::SeqCst)
    }

    fn set_lsn(&self, lsn: Lsn) {
        self.next_lsn.store(lsn, Ordering::SeqCst);
    }
}

/// Shared LSN provider wrapped in Arc for use across services.
pub type SharedLsnProvider = Arc<dyn LsnProvider>;

/// Create a new shared LSN provider.
pub fn new_lsn_provider() -> SharedLsnProvider {
    Arc::new(AtomicLsnProvider::new())
}

/// Create a shared LSN provider starting from a specific value.
pub fn new_lsn_provider_with_start(start_lsn: Lsn) -> SharedLsnProvider {
    Arc::new(AtomicLsnProvider::with_start(start_lsn))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_atomic_lsn_provider_basic() {
        let provider = AtomicLsnProvider::new();

        assert_eq!(provider.current_lsn(), 1);
        assert_eq!(provider.alloc_lsn(), 1);
        assert_eq!(provider.alloc_lsn(), 2);
        assert_eq!(provider.alloc_lsn(), 3);
        assert_eq!(provider.current_lsn(), 4);
    }

    #[test]
    fn test_atomic_lsn_provider_with_start() {
        let provider = AtomicLsnProvider::with_start(100);

        assert_eq!(provider.current_lsn(), 100);
        assert_eq!(provider.alloc_lsn(), 100);
        assert_eq!(provider.alloc_lsn(), 101);
        assert_eq!(provider.current_lsn(), 102);
    }

    #[test]
    fn test_atomic_lsn_provider_set() {
        let provider = AtomicLsnProvider::new();

        provider.set_lsn(500);
        assert_eq!(provider.current_lsn(), 500);
        assert_eq!(provider.alloc_lsn(), 500);
        assert_eq!(provider.current_lsn(), 501);
    }

    #[test]
    fn test_shared_lsn_provider() {
        let provider = new_lsn_provider();

        assert_eq!(provider.alloc_lsn(), 1);
        assert_eq!(provider.alloc_lsn(), 2);
    }

    #[test]
    fn test_concurrent_alloc() {
        let provider = Arc::new(AtomicLsnProvider::new());
        let num_threads = 8;
        let allocs_per_thread = 1000;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let provider = Arc::clone(&provider);
                thread::spawn(move || {
                    let mut lsns = Vec::with_capacity(allocs_per_thread);
                    for _ in 0..allocs_per_thread {
                        lsns.push(provider.alloc_lsn());
                    }
                    lsns
                })
            })
            .collect();

        let mut all_lsns = Vec::new();
        for h in handles {
            all_lsns.extend(h.join().unwrap());
        }

        // Sort and check for duplicates
        all_lsns.sort();
        for i in 1..all_lsns.len() {
            assert!(
                all_lsns[i] > all_lsns[i - 1],
                "Duplicate or out-of-order LSN: {} vs {}",
                all_lsns[i - 1],
                all_lsns[i]
            );
        }

        // Verify all LSNs are accounted for
        assert_eq!(all_lsns.len(), num_threads * allocs_per_thread);
        assert_eq!(all_lsns[0], 1);
        assert_eq!(
            all_lsns[all_lsns.len() - 1],
            (num_threads * allocs_per_thread) as u64
        );
    }
}
