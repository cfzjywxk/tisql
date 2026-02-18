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

//! Local (in-memory) TSO implementation.
//!
//! This is a simple atomic counter implementation for single-node deployment.
//! For distributed deployment, use PD-based TSO instead.

use std::sync::atomic::{AtomicU64, Ordering};

use super::TsoService;
use crate::catalog::types::Timestamp;

/// Local TSO implementation using an atomic counter.
///
/// This is suitable for:
/// - Single-node deployment
/// - Testing and development
/// - Embedded scenarios
///
/// For production distributed deployment, use PD-based TSO.
///
/// ## Thread Safety
///
/// Uses `AtomicU64` with `SeqCst` ordering to guarantee strict
/// monotonicity across threads.
pub struct LocalTso {
    /// Monotonic timestamp counter.
    counter: AtomicU64,
}

impl LocalTso {
    /// Create a new LocalTso with initial timestamp.
    ///
    /// The initial timestamp should be:
    /// - 1 for fresh databases
    /// - `max_recovered_ts + 1` after recovery
    pub fn new(initial_ts: Timestamp) -> Self {
        Self {
            counter: AtomicU64::new(initial_ts),
        }
    }
}

impl TsoService for LocalTso {
    fn get_ts(&self) -> Timestamp {
        self.counter.fetch_add(1, Ordering::SeqCst)
    }

    fn last_ts(&self) -> Timestamp {
        self.counter.load(Ordering::SeqCst)
    }

    fn set_ts(&self, ts: Timestamp) {
        self.counter.store(ts, Ordering::SeqCst);
    }
}

impl Default for LocalTso {
    fn default() -> Self {
        Self::new(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_local_tso_basic() {
        let tso = LocalTso::new(1);

        assert_eq!(tso.get_ts(), 1);
        assert_eq!(tso.get_ts(), 2);
        assert_eq!(tso.get_ts(), 3);
        assert_eq!(tso.last_ts(), 4);
    }

    #[test]
    fn test_local_tso_set_ts() {
        let tso = LocalTso::new(1);

        tso.get_ts(); // 1
        tso.get_ts(); // 2

        tso.set_ts(100);
        assert_eq!(tso.last_ts(), 100);
        assert_eq!(tso.get_ts(), 100);
        assert_eq!(tso.get_ts(), 101);
    }

    #[test]
    fn test_local_tso_concurrent() {
        let tso = Arc::new(LocalTso::new(1));
        let mut handles = vec![];

        for _ in 0..10 {
            let tso = Arc::clone(&tso);
            handles.push(thread::spawn(move || {
                let mut timestamps = vec![];
                for _ in 0..100 {
                    timestamps.push(tso.get_ts());
                }
                timestamps
            }));
        }

        let mut all_timestamps = vec![];
        for handle in handles {
            all_timestamps.extend(handle.join().unwrap());
        }

        // All timestamps should be unique
        all_timestamps.sort();
        for i in 1..all_timestamps.len() {
            assert!(
                all_timestamps[i] > all_timestamps[i - 1],
                "Timestamps must be strictly increasing"
            );
        }

        // Should have allocated exactly 1000 timestamps
        assert_eq!(all_timestamps.len(), 1000);
    }

    #[test]
    fn test_local_tso_is_valid() {
        let tso = LocalTso::new(1);

        tso.get_ts(); // 1
        tso.get_ts(); // 2

        assert!(tso.is_valid_ts(1));
        assert!(tso.is_valid_ts(2));
        assert!(tso.is_valid_ts(3)); // last_ts
        assert!(!tso.is_valid_ts(4)); // future
        assert!(!tso.is_valid_ts(100)); // future
    }
}
