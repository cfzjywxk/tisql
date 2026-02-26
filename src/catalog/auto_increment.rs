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

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::catalog::types::TableId;
use crate::inner_table::bootstrap::{read_autoinc_row, write_autoinc_row};
use crate::transaction::TxnService;
use crate::util::error::{Result, TiSqlError};

/// Default durable reserve size for AUTO_INCREMENT batched refill.
pub(crate) const DEFAULT_AUTO_INC_CACHE_SIZE: u64 = 64;

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalRange {
    pub next_local: u64,
    pub end_local: u64,
}

impl LocalRange {
    fn empty() -> Self {
        Self {
            next_local: 1,
            end_local: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct AutoIncEntry {
    /// Local fast-path state.
    pub state: Mutex<LocalRange>,
    /// Slow-path refill and durable explicit bump serialization.
    pub refill_lock: Mutex<()>,
}

impl AutoIncEntry {
    fn new() -> Self {
        Self {
            state: Mutex::new(LocalRange::empty()),
            refill_lock: Mutex::new(()),
        }
    }
}

/// Per-table AUTO_INCREMENT allocator with local cached ranges.
pub(crate) struct AutoIncService<T: TxnService> {
    txn_service: Arc<T>,
    entries: RwLock<HashMap<TableId, Arc<AutoIncEntry>>>,
    cache_size: u64,
}

impl<T: TxnService> AutoIncService<T> {
    pub(crate) fn new(txn_service: Arc<T>, cache_size: u64) -> Self {
        Self {
            txn_service,
            entries: RwLock::new(HashMap::new()),
            cache_size: cache_size.max(1),
        }
    }

    pub(crate) fn remove_table(&self, table_id: TableId) {
        self.entries.write().remove(&table_id);
    }

    /// Allocate the next generated value.
    ///
    /// Lock ordering invariant:
    /// - Fast path uses only `state`.
    /// - Slow path acquires `refill_lock` first, then `state`.
    pub(crate) fn next_value(&self, table_id: TableId) -> Result<u64> {
        let entry = self.entry_for_table(table_id);

        {
            let mut state = entry.state.lock();
            if state.next_local <= state.end_local {
                let value = state.next_local;
                state.next_local = state
                    .next_local
                    .checked_add(1)
                    .ok_or_else(|| TiSqlError::Catalog("AUTO_INCREMENT overflow".into()))?;
                return Ok(value);
            }
        }

        let _refill_guard = entry.refill_lock.lock();
        {
            let mut state = entry.state.lock();
            if state.next_local <= state.end_local {
                let value = state.next_local;
                state.next_local = state
                    .next_local
                    .checked_add(1)
                    .ok_or_else(|| TiSqlError::Catalog("AUTO_INCREMENT overflow".into()))?;
                return Ok(value);
            }
        }

        let reserve = self.cache_size.max(1);
        let (start, reserved) = self.reserve_durable_range(table_id, reserve)?;
        let end = start
            .checked_add(reserved - 1)
            .ok_or_else(|| TiSqlError::Catalog("AUTO_INCREMENT overflow".into()))?;

        let mut state = entry.state.lock();
        state.next_local = start;
        state.end_local = end;

        let value = state.next_local;
        state.next_local = state
            .next_local
            .checked_add(1)
            .ok_or_else(|| TiSqlError::Catalog("AUTO_INCREMENT overflow".into()))?;
        Ok(value)
    }

    /// Observe an explicit successful insert value and advance allocator state
    /// so subsequent generated values are greater than `explicit_v`.
    ///
    /// Lock ordering invariant:
    /// - Fast path uses only `state`.
    /// - Slow path acquires `refill_lock` first, then `state`.
    pub(crate) fn observe_explicit(&self, table_id: TableId, explicit_v: u64) -> Result<()> {
        let target_next = explicit_v
            .checked_add(1)
            .ok_or_else(|| TiSqlError::Catalog("AUTO_INCREMENT overflow".into()))?;
        let entry = self.entry_for_table(table_id);

        {
            let mut state = entry.state.lock();
            if explicit_v < state.next_local {
                return Ok(());
            }
            if state.next_local <= explicit_v && explicit_v <= state.end_local {
                state.next_local = target_next;
                return Ok(());
            }
        }

        let _refill_guard = entry.refill_lock.lock();
        {
            let mut state = entry.state.lock();
            if explicit_v < state.next_local {
                return Ok(());
            }
            if state.next_local <= explicit_v && explicit_v <= state.end_local {
                state.next_local = target_next;
                return Ok(());
            }
        }

        self.bump_durable_floor(table_id, target_next)?;

        let mut state = entry.state.lock();
        if state.next_local < target_next {
            state.next_local = target_next;
        }
        if state.next_local > state.end_local {
            state.end_local = state.next_local.saturating_sub(1);
        }
        Ok(())
    }

    fn reserve_durable_range(&self, table_id: TableId, reserve: u64) -> Result<(u64, u64)> {
        let mut ctx = self.txn_service.begin(false)?;
        let durable_next =
            crate::io::block_on_sync(read_autoinc_row(&ctx, self.txn_service.as_ref(), table_id))?
                .ok_or_else(|| {
                    TiSqlError::Catalog(format!(
                        "AUTO_INCREMENT metadata for table {table_id} not found"
                    ))
                })?;
        let start = durable_next.max(1);
        let new_next = start
            .checked_add(reserve)
            .ok_or_else(|| TiSqlError::Catalog("AUTO_INCREMENT overflow".into()))?;

        crate::io::block_on_sync(write_autoinc_row(
            &mut ctx,
            self.txn_service.as_ref(),
            table_id,
            new_next,
        ))?;
        crate::io::block_on_sync(self.txn_service.commit(ctx))?;
        Ok((start, reserve))
    }

    fn bump_durable_floor(&self, table_id: TableId, floor_next: u64) -> Result<()> {
        let mut ctx = self.txn_service.begin(false)?;
        let durable_next =
            crate::io::block_on_sync(read_autoinc_row(&ctx, self.txn_service.as_ref(), table_id))?
                .ok_or_else(|| {
                    TiSqlError::Catalog(format!(
                        "AUTO_INCREMENT metadata for table {table_id} not found"
                    ))
                })?;

        if durable_next < floor_next {
            crate::io::block_on_sync(write_autoinc_row(
                &mut ctx,
                self.txn_service.as_ref(),
                table_id,
                floor_next,
            ))?;
            crate::io::block_on_sync(self.txn_service.commit(ctx))?;
        } else {
            self.txn_service.rollback(ctx)?;
        }
        Ok(())
    }

    fn entry_for_table(&self, table_id: TableId) -> Arc<AutoIncEntry> {
        if let Some(entry) = self.entries.read().get(&table_id).cloned() {
            return entry;
        }

        let mut map = self.entries.write();
        map.entry(table_id)
            .or_insert_with(|| Arc::new(AutoIncEntry::new()))
            .clone()
    }
}
