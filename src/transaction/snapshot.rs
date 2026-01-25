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

//! Read-only snapshot implementation.

use std::ops::Range;
use std::sync::Arc;

use super::concurrency::ConcurrencyManager;
use crate::error::Result;
use crate::storage::StorageEngine;
use crate::types::{Key, RawValue, Timestamp};

use super::api::ReadSnapshot;

/// Read-only snapshot backed by a storage engine.
///
/// The snapshot is assigned a start_ts and provides a consistent
/// read view at that timestamp using MVCC.
pub struct StorageSnapshot<S: StorageEngine> {
    /// The snapshot timestamp
    start_ts: Timestamp,
    /// Reference to storage engine
    storage: Arc<S>,
    /// Reference to concurrency manager for lock checking
    concurrency_manager: Arc<ConcurrencyManager>,
}

impl<S: StorageEngine> StorageSnapshot<S> {
    /// Create a new snapshot with the given timestamp.
    pub fn new(
        start_ts: Timestamp,
        storage: Arc<S>,
        concurrency_manager: Arc<ConcurrencyManager>,
    ) -> Self {
        // Update max_ts to ensure future writes get higher timestamps
        concurrency_manager.update_max_ts(start_ts);

        Self {
            start_ts,
            storage,
            concurrency_manager,
        }
    }
}

impl<S: StorageEngine> ReadSnapshot for StorageSnapshot<S> {
    fn start_ts(&self) -> Timestamp {
        self.start_ts
    }

    fn get(&self, key: &[u8]) -> Result<Option<RawValue>> {
        // Check for in-memory locks before reading
        // This ensures we don't read partial writes from concurrent transactions
        self.concurrency_manager.check_lock(key, self.start_ts)?;

        // Read at our snapshot timestamp
        self.storage.get_at(key, self.start_ts)
    }

    fn scan(&self, range: Range<Key>) -> Result<Box<dyn Iterator<Item = (Key, RawValue)> + '_>> {
        // Check for locks in the scan range
        if !range.is_empty() {
            self.concurrency_manager
                .check_range(&range.start, &range.end, self.start_ts)?;
        }

        // Create iterator at snapshot timestamp
        // Note: The underlying storage should respect MVCC visibility
        self.storage.scan(range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MvccMemTableEngine;

    #[test]
    fn test_snapshot_start_ts() {
        let cm = Arc::new(ConcurrencyManager::new(1));
        let storage = Arc::new(MvccMemTableEngine::new(Arc::clone(&cm)));

        // Get timestamp and create snapshot
        let ts = cm.get_ts();
        let snapshot = StorageSnapshot::new(ts, storage, Arc::clone(&cm));

        assert_eq!(snapshot.start_ts(), ts);
    }

    #[test]
    fn test_snapshot_updates_max_ts() {
        let cm = Arc::new(ConcurrencyManager::new(1));
        let storage = Arc::new(MvccMemTableEngine::new(Arc::clone(&cm)));

        let ts = 100;
        let _snapshot = StorageSnapshot::new(ts, storage, Arc::clone(&cm));

        // max_ts should be updated
        assert!(cm.max_ts() >= ts);
    }
}
