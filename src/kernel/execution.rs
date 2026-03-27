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

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use crate::catalog::types::{ColumnId, DataType, IndexId, Key, Row, TableId, Value};
use crate::catalog::TableDef;
use crate::tablet::{
    decode_row_to_values, encode_int_key, encode_key, encode_pk, encode_row, next_key_bound,
};
use crate::transaction::{StatementGuard, TxnCtx, TxnService};
use crate::util::codec::key::{decode_record_key, Handle};
use crate::util::error::{Result, TiSqlError};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LogicalPointKey {
    PrimaryKey(Vec<Value>),
    #[allow(dead_code)]
    /// Reserved for hidden-row-id tables while planner exposure catches up.
    HiddenRowId(i64),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RowKey {
    PrimaryKey(Vec<Value>),
    HiddenRowId(i64),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ProjectionSpec {
    All,
    Columns {
        col_ids: Vec<ColumnId>,
        data_types: Vec<DataType>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct PointGetRequest {
    pub table: TableDef,
    pub key: LogicalPointKey,
    pub projection: ProjectionSpec,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ScanBound {
    pub values: Vec<Value>,
    pub inclusive: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LogicalScanBounds {
    FullTable,
    #[allow(dead_code)]
    /// Reserved for prefix-constrained primary-key scans.
    PrimaryKeyPrefix {
        prefix: Vec<Value>,
    },
    PrimaryKeyRange {
        lower: Option<ScanBound>,
        upper: Option<ScanBound>,
    },
    #[allow(dead_code)]
    /// Reserved for future index-backed scans; planning may emit this before the
    /// execution backend learns how to serve it.
    IndexRange {
        index_id: IndexId,
        lower: Option<ScanBound>,
        upper: Option<ScanBound>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ScanRequest {
    pub table: TableDef,
    pub bounds: LogicalScanBounds,
    pub projection: ProjectionSpec,
}

#[derive(Debug, Clone)]
pub(crate) struct InsertRow {
    pub key: RowKey,
    pub row: Row,
}

#[derive(Debug, Clone)]
pub(crate) struct InsertRequest {
    pub table: TableDef,
    pub rows: Vec<InsertRow>,
    pub ignore_duplicates: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct UpdateRow {
    pub current_key: RowKey,
    pub current_row: Row,
    pub new_row: Row,
}

#[derive(Debug, Clone)]
pub(crate) struct UpdateRequest {
    pub table: TableDef,
    pub rows: Vec<UpdateRow>,
}

#[derive(Debug, Clone)]
pub(crate) struct DeleteRequest {
    pub table: TableDef,
    pub keys: Vec<RowKey>,
}

#[derive(Debug, Clone)]
pub(crate) struct LockRequest {
    pub table: TableDef,
    pub keys: Vec<RowKey>,
}

#[derive(Debug, Clone)]
pub(crate) enum WriteRequest {
    Insert(InsertRequest),
    Update(UpdateRequest),
    Delete(DeleteRequest),
    Lock(LockRequest),
    Batch(Vec<WriteRequest>),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct WriteOutcome {
    pub matched_rows: u64,
    pub affected_rows: u64,
    pub locked_rows: u64,
    pub last_insert_id: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct ExecutionRow {
    pub key: RowKey,
    pub row: Row,
}

pub(crate) trait ExecutionRowStream: Send {
    fn next_row(&mut self) -> impl Future<Output = Result<Option<ExecutionRow>>> + Send + '_;
}

pub(crate) trait ExecutionBackend: Send + Sync {
    type Scan: ExecutionRowStream + 'static;

    fn point_get<'a>(
        &'a self,
        ctx: &'a TxnCtx,
        req: PointGetRequest,
    ) -> impl Future<Output = Result<Option<ExecutionRow>>> + Send + 'a;

    fn scan(&self, ctx: &TxnCtx, req: ScanRequest) -> Result<Self::Scan>;

    /// Applies exactly one logical SQL write statement.
    ///
    /// Implementations must provide statement atomicity: if this returns `Err`,
    /// no mutation staged by this call may remain in `ctx`. Mutations staged by
    /// earlier successful statements in the same explicit transaction must
    /// remain intact.
    fn apply_write<'a>(
        &'a self,
        ctx: &'a mut TxnCtx,
        req: WriteRequest,
    ) -> impl Future<Output = Result<WriteOutcome>> + Send + 'a;
}

pub(crate) struct TxnExecutionBackend<T> {
    txn_service: Arc<T>,
}

impl<T> TxnExecutionBackend<T> {
    pub(crate) fn new(txn_service: Arc<T>) -> Self {
        Self { txn_service }
    }
}

fn encode_logical_point_key(table_id: TableId, key: &LogicalPointKey) -> Key {
    match key {
        LogicalPointKey::PrimaryKey(values) => encode_key(table_id, &encode_pk(values)),
        LogicalPointKey::HiddenRowId(handle) => encode_int_key(table_id, *handle),
    }
}

fn project_columns(
    table: &TableDef,
    projection: &ProjectionSpec,
) -> (Vec<ColumnId>, Vec<DataType>) {
    match projection {
        ProjectionSpec::All => (
            table.columns().iter().map(|c| c.id()).collect(),
            table
                .columns()
                .iter()
                .map(|c| c.data_type().clone())
                .collect(),
        ),
        ProjectionSpec::Columns {
            col_ids,
            data_types,
        } => (col_ids.clone(), data_types.clone()),
    }
}

fn row_key_for(key: &LogicalPointKey) -> RowKey {
    match key {
        LogicalPointKey::PrimaryKey(values) => RowKey::PrimaryKey(values.clone()),
        LogicalPointKey::HiddenRowId(handle) => RowKey::HiddenRowId(*handle),
    }
}

fn encode_row_key(table_id: TableId, key: &RowKey) -> Key {
    match key {
        RowKey::PrimaryKey(values) => encode_key(table_id, &encode_pk(values)),
        RowKey::HiddenRowId(handle) => encode_int_key(table_id, *handle),
    }
}

fn row_key_from_row(table: &TableDef, row: &Row, fallback: &RowKey) -> RowKey {
    if table.primary_key().is_empty() {
        return fallback.clone();
    }

    let pk_values = table
        .pk_column_indices()
        .into_iter()
        .map(|idx| row.get(idx).cloned().unwrap_or(Value::Null))
        .collect();
    RowKey::PrimaryKey(pk_values)
}

fn duplicate_key_error(table: &TableDef, key: &RowKey) -> TiSqlError {
    let key_desc = match key {
        RowKey::PrimaryKey(values) => format!("{values:?}"),
        RowKey::HiddenRowId(handle) => format!("row_id={handle}"),
    };
    TiSqlError::DuplicateKey(format!(
        "Duplicate entry '{}' for key '{}.PRIMARY'",
        key_desc,
        table.name()
    ))
}

fn encode_table_row(table: &TableDef, row: &Row) -> Vec<u8> {
    let col_ids: Vec<ColumnId> = table.columns().iter().map(|c| c.id()).collect();
    encode_row(&col_ids, row.values())
}

impl WriteOutcome {
    fn merge(&mut self, other: WriteOutcome) {
        self.matched_rows += other.matched_rows;
        self.affected_rows += other.affected_rows;
        self.locked_rows += other.locked_rows;
        if self.last_insert_id.is_none() {
            self.last_insert_id = other.last_insert_id;
        }
    }
}

fn table_projection(table: &TableDef) -> (Vec<ColumnId>, Vec<DataType>) {
    (
        table.columns().iter().map(|c| c.id()).collect(),
        table
            .columns()
            .iter()
            .map(|c| c.data_type().clone())
            .collect(),
    )
}

fn project_row(table: &TableDef, projection: &ProjectionSpec, row: &Row) -> Result<Row> {
    match projection {
        ProjectionSpec::All => Ok(row.clone()),
        ProjectionSpec::Columns { col_ids, .. } => {
            let values = col_ids
                .iter()
                .map(|col_id| {
                    let col_idx = table
                        .columns()
                        .iter()
                        .position(|c| c.id() == *col_id)
                        .ok_or_else(|| {
                            crate::util::error::TiSqlError::ColumnNotFound(format!("id={col_id}"))
                        })?;
                    Ok(row.get(col_idx).cloned().unwrap_or(Value::Null))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Row::new(values))
        }
    }
}

fn decode_scan_row_key(table: &TableDef, key: &[u8], row: &Row) -> Result<RowKey> {
    if table.primary_key().is_empty() {
        let (_, handle) = decode_record_key(key)?;
        let Handle::Int(handle) = handle else {
            unreachable!("tables without a primary key must use hidden int handles")
        };
        return Ok(RowKey::HiddenRowId(handle));
    }

    let pk_values = table
        .pk_column_indices()
        .into_iter()
        .map(|idx| row.get(idx).cloned().unwrap_or(Value::Null))
        .collect();
    Ok(RowKey::PrimaryKey(pk_values))
}

fn encode_scan_range(table: &TableDef, bounds: &LogicalScanBounds) -> Result<std::ops::Range<Key>> {
    let table_start = encode_key(table.id(), &[]);
    let table_end = encode_key(table.id() + 1, &[]);

    match bounds {
        LogicalScanBounds::FullTable => Ok(table_start..table_end),
        LogicalScanBounds::PrimaryKeyPrefix { prefix } => {
            let start = encode_key(table.id(), &encode_pk(prefix));
            let mut end = start.clone();
            if !next_key_bound(&mut end) {
                end = table_end;
            }
            Ok(start..end)
        }
        LogicalScanBounds::PrimaryKeyRange { lower, upper } => {
            let mut start = table_start;
            if let Some(lower) = lower {
                start = encode_key(table.id(), &encode_pk(&lower.values));
                if !lower.inclusive && !next_key_bound(&mut start) {
                    return Ok(table_end.clone()..table_end);
                }
            }

            let mut end = table_end.clone();
            if let Some(upper) = upper {
                end = encode_key(table.id(), &encode_pk(&upper.values));
                if upper.inclusive && !next_key_bound(&mut end) {
                    end = table_end.clone();
                }
            }

            if start >= end {
                return Ok(table_end.clone()..table_end);
            }

            Ok(start..end)
        }
        LogicalScanBounds::IndexRange { .. } => Err(crate::util::error::TiSqlError::Execution(
            "index-backed logical scans are not implemented yet".into(),
        )),
    }
}

pub(crate) struct TxnExecutionScan<I> {
    iter: I,
    table: TableDef,
    full_col_ids: Vec<ColumnId>,
    full_data_types: Vec<DataType>,
    projection: ProjectionSpec,
    initialized: bool,
}

impl<I> TxnExecutionScan<I> {
    fn new(iter: I, table: TableDef, projection: ProjectionSpec) -> Self {
        let (full_col_ids, full_data_types) = table_projection(&table);
        Self {
            iter,
            table,
            full_col_ids,
            full_data_types,
            projection,
            initialized: false,
        }
    }
}

impl<I: crate::transaction::TxnScanCursor> ExecutionRowStream for TxnExecutionScan<I> {
    fn next_row(&mut self) -> impl Future<Output = Result<Option<ExecutionRow>>> + Send + '_ {
        async move {
            if !self.initialized {
                self.initialized = true;
                self.iter.advance().await?;
            }

            let Some(entry) = self.iter.current() else {
                return Ok(None);
            };

            let full_values =
                decode_row_to_values(&entry.value, &self.full_col_ids, &self.full_data_types)?;
            let full_row = Row::new(full_values);
            let row = project_row(&self.table, &self.projection, &full_row)?;
            let key = decode_scan_row_key(&self.table, &entry.user_key, &full_row)?;

            self.iter.advance().await?;
            Ok(Some(ExecutionRow { key, row }))
        }
    }
}

impl<T: TxnService> ExecutionBackend for TxnExecutionBackend<T> {
    type Scan = TxnExecutionScan<T::ScanCursor>;

    fn point_get<'a>(
        &'a self,
        ctx: &'a TxnCtx,
        req: PointGetRequest,
    ) -> impl Future<Output = Result<Option<ExecutionRow>>> + Send + 'a {
        async move {
            let encoded_key = encode_logical_point_key(req.table.id(), &req.key);
            let Some(raw_row) = self
                .txn_service
                .get(ctx, req.table.id(), &encoded_key)
                .await?
            else {
                return Ok(None);
            };

            let (col_ids, data_types) = project_columns(&req.table, &req.projection);
            let values = decode_row_to_values(&raw_row, &col_ids, &data_types)?;
            Ok(Some(ExecutionRow {
                key: row_key_for(&req.key),
                row: Row::new(values),
            }))
        }
    }

    fn scan(&self, ctx: &TxnCtx, req: ScanRequest) -> Result<Self::Scan> {
        let range = encode_scan_range(&req.table, &req.bounds)?;
        let iter = self.txn_service.scan(ctx, req.table.id(), range)?;
        Ok(TxnExecutionScan::new(iter, req.table, req.projection))
    }

    fn apply_write<'a>(
        &'a self,
        ctx: &'a mut TxnCtx,
        req: WriteRequest,
    ) -> impl Future<Output = Result<WriteOutcome>> + Send + 'a {
        async move {
            let mut stmt = self.txn_service.begin_statement(ctx);
            let result = self.apply_write_inner(ctx, &mut stmt, req).await;
            match result {
                Ok(outcome) => Ok(outcome),
                Err(err) => {
                    if stmt.is_empty() {
                        Err(err)
                    } else {
                        self.txn_service.rollback_statement(ctx, stmt).await?;
                        Err(err)
                    }
                }
            }
        }
    }
}

impl<T: TxnService> TxnExecutionBackend<T> {
    async fn apply_write_inner(
        &self,
        ctx: &mut TxnCtx,
        stmt: &mut StatementGuard,
        req: WriteRequest,
    ) -> Result<WriteOutcome> {
        let mut requests = vec![req];

        let mut outcome = WriteOutcome::default();
        while let Some(req) = requests.pop() {
            let next = match req {
                WriteRequest::Insert(req) => self.apply_insert(ctx, stmt, req).await?,
                WriteRequest::Update(req) => self.apply_update(ctx, stmt, req).await?,
                WriteRequest::Delete(req) => self.apply_delete(ctx, stmt, req).await?,
                WriteRequest::Lock(req) => self.apply_lock(ctx, stmt, req).await?,
                WriteRequest::Batch(nested) => {
                    requests.extend(nested.into_iter().rev());
                    continue;
                }
            };
            outcome.merge(next);
        }

        Ok(outcome)
    }

    async fn apply_insert(
        &self,
        ctx: &mut TxnCtx,
        stmt: &mut StatementGuard,
        req: InsertRequest,
    ) -> Result<WriteOutcome> {
        let mut outcome = WriteOutcome::default();

        for row in req.rows {
            let encoded_key = encode_row_key(req.table.id(), &row.key);
            let duplicate_check_begin = Instant::now();
            let duplicate_exists = self
                .txn_service
                .get(ctx, req.table.id(), &encoded_key)
                .await?
                .is_some();
            self.txn_service
                .record_duplicate_check_duration(duplicate_check_begin.elapsed());

            if duplicate_exists {
                if req.ignore_duplicates {
                    self.txn_service
                        .lock_key(ctx, stmt, req.table.id(), &encoded_key)
                        .await?;
                    outcome.locked_rows += 1;
                    continue;
                }
                return Err(duplicate_key_error(&req.table, &row.key));
            }

            let value = encode_table_row(&req.table, &row.row);
            self.txn_service
                .put(ctx, stmt, req.table.id(), &encoded_key, value)
                .await?;
            outcome.matched_rows += 1;
            outcome.affected_rows += 1;
            if outcome.last_insert_id.is_none() {
                if let RowKey::HiddenRowId(handle) = row.key {
                    if handle >= 0 {
                        outcome.last_insert_id = Some(handle as u64);
                    }
                }
            }
        }

        Ok(outcome)
    }

    async fn apply_update(
        &self,
        ctx: &mut TxnCtx,
        stmt: &mut StatementGuard,
        req: UpdateRequest,
    ) -> Result<WriteOutcome> {
        let mut outcome = WriteOutcome::default();

        for row in req.rows {
            let current_key = encode_row_key(req.table.id(), &row.current_key);
            let target_key_logical = row_key_from_row(&req.table, &row.new_row, &row.current_key);
            let target_key = encode_row_key(req.table.id(), &target_key_logical);
            let key_changed = target_key != current_key;
            let row_unchanged = !key_changed && row.new_row.values() == row.current_row.values();

            if row_unchanged {
                self.txn_service
                    .lock_key(ctx, stmt, req.table.id(), &current_key)
                    .await?;
                outcome.matched_rows += 1;
                outcome.affected_rows += 1;
                outcome.locked_rows += 1;
                continue;
            }

            if key_changed {
                let duplicate_check_begin = Instant::now();
                let duplicate_exists = self
                    .txn_service
                    .get(ctx, req.table.id(), &target_key)
                    .await?
                    .is_some();
                self.txn_service
                    .record_duplicate_check_duration(duplicate_check_begin.elapsed());
                if duplicate_exists {
                    return Err(duplicate_key_error(&req.table, &target_key_logical));
                }

                self.txn_service
                    .delete(ctx, stmt, req.table.id(), &current_key)
                    .await?;
            }

            let value = encode_table_row(&req.table, &row.new_row);
            self.txn_service
                .put(ctx, stmt, req.table.id(), &target_key, value)
                .await?;
            outcome.matched_rows += 1;
            outcome.affected_rows += 1;
        }

        Ok(outcome)
    }

    async fn apply_delete(
        &self,
        ctx: &mut TxnCtx,
        stmt: &mut StatementGuard,
        req: DeleteRequest,
    ) -> Result<WriteOutcome> {
        let mut outcome = WriteOutcome::default();

        for key in req.keys {
            let encoded_key = encode_row_key(req.table.id(), &key);
            self.txn_service
                .delete(ctx, stmt, req.table.id(), &encoded_key)
                .await?;
            outcome.matched_rows += 1;
            outcome.affected_rows += 1;
        }

        Ok(outcome)
    }

    async fn apply_lock(
        &self,
        ctx: &mut TxnCtx,
        stmt: &mut StatementGuard,
        req: LockRequest,
    ) -> Result<WriteOutcome> {
        let mut outcome = WriteOutcome::default();

        for key in req.keys {
            let encoded_key = encode_row_key(req.table.id(), &key);
            self.txn_service
                .lock_key(ctx, stmt, req.table.id(), &encoded_key)
                .await?;
            outcome.locked_rows += 1;
        }

        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use parking_lot::Mutex;

    use super::*;
    use crate::catalog::types::{Lsn, RawValue, Timestamp, TxnId};
    use crate::catalog::ColumnDef;
    use crate::tablet::encode_row;
    use crate::transaction::{
        CommitInfo, MutationMeta, MutationPayload, StatementGuard, TxnCtx, TxnScanCursor,
        TxnScanEntry, TxnState,
    };

    struct DummyCursor;

    impl TxnScanCursor for DummyCursor {
        fn advance(&mut self) -> impl Future<Output = Result<()>> + Send + '_ {
            async move { Ok(()) }
        }

        fn current(&self) -> Option<&TxnScanEntry> {
            None
        }
    }

    struct VecScanCursor {
        entries: Vec<TxnScanEntry>,
        index: usize,
        current: Option<TxnScanEntry>,
    }

    impl VecScanCursor {
        fn new(entries: Vec<TxnScanEntry>) -> Self {
            Self {
                entries,
                index: 0,
                current: None,
            }
        }
    }

    impl TxnScanCursor for VecScanCursor {
        fn advance(&mut self) -> impl Future<Output = Result<()>> + Send + '_ {
            async move {
                self.current = self.entries.get(self.index).cloned();
                if self.current.is_some() {
                    self.index += 1;
                }
                Ok(())
            }
        }

        fn current(&self) -> Option<&TxnScanEntry> {
            self.current.as_ref()
        }
    }

    struct MockTxnService {
        expected_table_id: TableId,
        expected_key: Key,
        row_value: RawValue,
        get_calls: AtomicUsize,
    }

    impl TxnService for MockTxnService {
        type ScanCursor = DummyCursor;

        fn begin(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new_for_test(1, 1, read_only, false))
        }

        fn begin_explicit(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new_for_test(1, 1, read_only, true))
        }

        fn get_txn_state(&self, _start_ts: Timestamp) -> Option<TxnState> {
            Some(TxnState::Running)
        }

        async fn get<'a>(
            &'a self,
            _ctx: &'a TxnCtx,
            table_id: TableId,
            key: &'a [u8],
        ) -> Result<Option<RawValue>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(table_id, self.expected_table_id);
            assert_eq!(key, self.expected_key.as_slice());
            Ok(Some(self.row_value.clone()))
        }

        fn scan(
            &self,
            _ctx: &TxnCtx,
            _table_id: TableId,
            _range: Range<Key>,
        ) -> Result<Self::ScanCursor> {
            Ok(DummyCursor)
        }

        fn begin_statement(&self, _ctx: &mut TxnCtx) -> StatementGuard {
            StatementGuard::default()
        }

        async fn put<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: &'a mut StatementGuard,
            _table_id: TableId,
            _key: &'a [u8],
            _value: RawValue,
        ) -> Result<()> {
            unreachable!("put should not be called in point-get tests")
        }

        async fn delete<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: &'a mut StatementGuard,
            _table_id: TableId,
            _key: &'a [u8],
        ) -> Result<()> {
            unreachable!("delete should not be called in point-get tests")
        }

        async fn lock_key<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: &'a mut StatementGuard,
            _table_id: TableId,
            _key: &'a [u8],
        ) -> Result<()> {
            unreachable!("lock_key should not be called in point-get tests")
        }

        async fn rollback_statement<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: StatementGuard,
        ) -> Result<()> {
            unreachable!("rollback_statement should not be called in point-get tests")
        }

        async fn commit(&self, _ctx: TxnCtx) -> Result<CommitInfo> {
            Ok(CommitInfo {
                txn_id: 1 as TxnId,
                commit_ts: 2,
                lsn: 3 as Lsn,
            })
        }

        fn rollback(&self, _ctx: TxnCtx) -> Result<()> {
            Ok(())
        }
    }

    struct MockScanTxnService {
        expected_table_id: TableId,
        expected_range: Range<Key>,
        scan_entries: Vec<TxnScanEntry>,
        scan_calls: AtomicUsize,
    }

    impl TxnService for MockScanTxnService {
        type ScanCursor = VecScanCursor;

        fn begin(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new_for_test(1, 1, read_only, false))
        }

        fn begin_explicit(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new_for_test(1, 1, read_only, true))
        }

        fn get_txn_state(&self, _start_ts: Timestamp) -> Option<TxnState> {
            Some(TxnState::Running)
        }

        async fn get<'a>(
            &'a self,
            _ctx: &'a TxnCtx,
            _table_id: TableId,
            _key: &'a [u8],
        ) -> Result<Option<RawValue>> {
            unreachable!("get should not be called in scan tests")
        }

        fn scan(
            &self,
            _ctx: &TxnCtx,
            table_id: TableId,
            range: Range<Key>,
        ) -> Result<Self::ScanCursor> {
            self.scan_calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(table_id, self.expected_table_id);
            assert_eq!(range, self.expected_range);
            Ok(VecScanCursor::new(self.scan_entries.clone()))
        }

        fn begin_statement(&self, _ctx: &mut TxnCtx) -> StatementGuard {
            StatementGuard::default()
        }

        async fn put<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: &'a mut StatementGuard,
            _table_id: TableId,
            _key: &'a [u8],
            _value: RawValue,
        ) -> Result<()> {
            unreachable!("put should not be called in scan tests")
        }

        async fn delete<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: &'a mut StatementGuard,
            _table_id: TableId,
            _key: &'a [u8],
        ) -> Result<()> {
            unreachable!("delete should not be called in scan tests")
        }

        async fn lock_key<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: &'a mut StatementGuard,
            _table_id: TableId,
            _key: &'a [u8],
        ) -> Result<()> {
            unreachable!("lock_key should not be called in scan tests")
        }

        async fn rollback_statement<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: StatementGuard,
        ) -> Result<()> {
            unreachable!("rollback_statement should not be called in scan tests")
        }

        async fn commit(&self, _ctx: TxnCtx) -> Result<CommitInfo> {
            Ok(CommitInfo {
                txn_id: 1 as TxnId,
                commit_ts: 2,
                lsn: 3 as Lsn,
            })
        }

        fn rollback(&self, _ctx: TxnCtx) -> Result<()> {
            Ok(())
        }
    }

    struct MockRollbackTxnService {
        fail_put_call: usize,
        put_calls: AtomicUsize,
        rollback_calls: AtomicUsize,
        active_statement_keys: Mutex<Vec<Key>>,
    }

    impl TxnService for MockRollbackTxnService {
        type ScanCursor = DummyCursor;

        fn begin(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new_for_test(1, 1, read_only, false))
        }

        fn begin_explicit(&self, read_only: bool) -> Result<TxnCtx> {
            Ok(TxnCtx::new_for_test(1, 1, read_only, true))
        }

        fn get_txn_state(&self, _start_ts: Timestamp) -> Option<TxnState> {
            Some(TxnState::Running)
        }

        async fn get<'a>(
            &'a self,
            _ctx: &'a TxnCtx,
            _table_id: TableId,
            _key: &'a [u8],
        ) -> Result<Option<RawValue>> {
            Ok(None)
        }

        fn scan(
            &self,
            _ctx: &TxnCtx,
            _table_id: TableId,
            _range: Range<Key>,
        ) -> Result<Self::ScanCursor> {
            unreachable!("scan should not be called in write tests")
        }

        fn begin_statement(&self, _ctx: &mut TxnCtx) -> StatementGuard {
            self.active_statement_keys.lock().clear();
            StatementGuard::default()
        }

        async fn put<'a>(
            &'a self,
            ctx: &'a mut TxnCtx,
            stmt: &'a mut StatementGuard,
            _table_id: TableId,
            key: &'a [u8],
            value: RawValue,
        ) -> Result<()> {
            let call = self.put_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == self.fail_put_call {
                return Err(TiSqlError::Execution("mock put failure".into()));
            }

            stmt.record_first_touch(key, ctx.mutations.get(key).cloned());

            let key = key.to_vec();
            ctx.mutations.insert(
                key.clone(),
                MutationMeta {
                    mutation: MutationPayload::Put(value),
                },
            );
            self.active_statement_keys.lock().push(key);
            Ok(())
        }

        async fn delete<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: &'a mut StatementGuard,
            _table_id: TableId,
            _key: &'a [u8],
        ) -> Result<()> {
            unreachable!("delete should not be called in write tests")
        }

        async fn lock_key<'a>(
            &'a self,
            _ctx: &'a mut TxnCtx,
            _stmt: &'a mut StatementGuard,
            _table_id: TableId,
            _key: &'a [u8],
        ) -> Result<()> {
            unreachable!("lock_key should not be called in write tests")
        }

        async fn rollback_statement<'a>(
            &'a self,
            ctx: &'a mut TxnCtx,
            _stmt: StatementGuard,
        ) -> Result<()> {
            self.rollback_calls.fetch_add(1, Ordering::SeqCst);
            for key in self.active_statement_keys.lock().drain(..) {
                ctx.mutations.remove(&key);
            }
            Ok(())
        }

        async fn commit(&self, _ctx: TxnCtx) -> Result<CommitInfo> {
            Ok(CommitInfo {
                txn_id: 1 as TxnId,
                commit_ts: 2,
                lsn: 3 as Lsn,
            })
        }

        fn rollback(&self, _ctx: TxnCtx) -> Result<()> {
            Ok(())
        }
    }

    fn single_pk_table(table_id: TableId) -> TableDef {
        TableDef::new(
            table_id,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        )
    }

    fn composite_pk_table(table_id: TableId) -> TableDef {
        TableDef::new(
            table_id,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "tenant_id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(3, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1, 2],
        )
    }

    #[tokio::test]
    async fn test_point_get_backend_returns_hidden_row_id_key() {
        let table = single_pk_table(501);
        let row_value = encode_row(&[1, 2], &[Value::BigInt(42), Value::String("alice".into())]);
        let service = Arc::new(MockTxnService {
            expected_table_id: table.id(),
            expected_key: encode_int_key(table.id(), 42),
            row_value,
            get_calls: AtomicUsize::new(0),
        });
        let backend = TxnExecutionBackend::new(Arc::clone(&service));
        let ctx = TxnCtx::new_for_test(7, 70, true, false);

        let row = backend
            .point_get(
                &ctx,
                PointGetRequest {
                    table,
                    key: LogicalPointKey::HiddenRowId(42),
                    projection: ProjectionSpec::All,
                },
            )
            .await
            .unwrap()
            .expect("point-get should return one row");

        assert_eq!(row.key, RowKey::HiddenRowId(42));
        assert_eq!(
            row.row.values(),
            &[Value::BigInt(42), Value::String("alice".into())]
        );
        assert_eq!(service.get_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_scan_backend_full_table_decodes_rows_and_uses_table_range() {
        let table = single_pk_table(601);
        let user_key = encode_key(table.id(), &encode_pk(&[Value::BigInt(7)]));
        let row_value = encode_row(&[1, 2], &[Value::BigInt(7), Value::String("alice".into())]);
        let service = Arc::new(MockScanTxnService {
            expected_table_id: table.id(),
            expected_range: encode_key(table.id(), &[])..encode_key(table.id() + 1, &[]),
            scan_entries: vec![TxnScanEntry {
                user_key,
                value: row_value,
            }],
            scan_calls: AtomicUsize::new(0),
        });
        let backend = TxnExecutionBackend::new(Arc::clone(&service));
        let ctx = TxnCtx::new_for_test(8, 80, true, false);

        let mut scan = backend
            .scan(
                &ctx,
                ScanRequest {
                    table,
                    bounds: LogicalScanBounds::FullTable,
                    projection: ProjectionSpec::All,
                },
            )
            .unwrap();

        let row = scan
            .next_row()
            .await
            .unwrap()
            .expect("scan should yield one row");
        assert_eq!(row.key, RowKey::PrimaryKey(vec![Value::BigInt(7)]));
        assert_eq!(
            row.row.values(),
            &[Value::BigInt(7), Value::String("alice".into())]
        );
        assert!(scan.next_row().await.unwrap().is_none());
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_scan_backend_pk_prefix_uses_prefix_range() {
        let table = composite_pk_table(602);
        let prefix = vec![Value::BigInt(7)];
        let start = encode_key(table.id(), &encode_pk(&prefix));
        let mut end = start.clone();
        assert!(next_key_bound(&mut end));
        let user_key = encode_key(
            table.id(),
            &encode_pk(&[Value::BigInt(7), Value::BigInt(3)]),
        );
        let row_value = encode_row(
            &[1, 2, 3],
            &[
                Value::BigInt(7),
                Value::BigInt(3),
                Value::String("alice".into()),
            ],
        );
        let service = Arc::new(MockScanTxnService {
            expected_table_id: table.id(),
            expected_range: start..end,
            scan_entries: vec![TxnScanEntry {
                user_key,
                value: row_value,
            }],
            scan_calls: AtomicUsize::new(0),
        });
        let backend = TxnExecutionBackend::new(Arc::clone(&service));
        let ctx = TxnCtx::new_for_test(9, 90, true, false);

        let mut scan = backend
            .scan(
                &ctx,
                ScanRequest {
                    table,
                    bounds: LogicalScanBounds::PrimaryKeyPrefix {
                        prefix: prefix.clone(),
                    },
                    projection: ProjectionSpec::All,
                },
            )
            .unwrap();

        let row = scan
            .next_row()
            .await
            .unwrap()
            .expect("scan should yield one row");
        assert_eq!(
            row.key,
            RowKey::PrimaryKey(vec![Value::BigInt(7), Value::BigInt(3)])
        );
        assert_eq!(
            row.row.values(),
            &[
                Value::BigInt(7),
                Value::BigInt(3),
                Value::String("alice".into()),
            ]
        );
        assert!(scan.next_row().await.unwrap().is_none());
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_scan_backend_pk_range_uses_bounds() {
        let table = single_pk_table(603);
        let user_key = encode_key(table.id(), &encode_pk(&[Value::BigInt(7)]));
        let row_value = encode_row(&[1, 2], &[Value::BigInt(7), Value::String("alice".into())]);
        let service = Arc::new(MockScanTxnService {
            expected_table_id: table.id(),
            expected_range: encode_key(table.id(), &encode_pk(&[Value::BigInt(5)]))
                ..encode_key(table.id(), &encode_pk(&[Value::BigInt(9)])),
            scan_entries: vec![TxnScanEntry {
                user_key,
                value: row_value,
            }],
            scan_calls: AtomicUsize::new(0),
        });
        let backend = TxnExecutionBackend::new(Arc::clone(&service));
        let ctx = TxnCtx::new_for_test(10, 100, true, false);

        let mut scan = backend
            .scan(
                &ctx,
                ScanRequest {
                    table,
                    bounds: LogicalScanBounds::PrimaryKeyRange {
                        lower: Some(ScanBound {
                            values: vec![Value::BigInt(5)],
                            inclusive: true,
                        }),
                        upper: Some(ScanBound {
                            values: vec![Value::BigInt(9)],
                            inclusive: false,
                        }),
                    },
                    projection: ProjectionSpec::All,
                },
            )
            .unwrap();

        let row = scan
            .next_row()
            .await
            .unwrap()
            .expect("scan should yield one row");
        assert_eq!(row.key, RowKey::PrimaryKey(vec![Value::BigInt(7)]));
        assert_eq!(
            row.row.values(),
            &[Value::BigInt(7), Value::String("alice".into())]
        );
        assert!(scan.next_row().await.unwrap().is_none());
        assert_eq!(service.scan_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_apply_write_rolls_back_only_failing_statement_in_explicit_txn() {
        let table = single_pk_table(604);
        let kept_key = encode_key(table.id(), &encode_pk(&[Value::BigInt(1)]));
        let rolled_back_key = encode_key(table.id(), &encode_pk(&[Value::BigInt(2)]));
        let never_staged_key = encode_key(table.id(), &encode_pk(&[Value::BigInt(3)]));
        let service = Arc::new(MockRollbackTxnService {
            fail_put_call: 3,
            put_calls: AtomicUsize::new(0),
            rollback_calls: AtomicUsize::new(0),
            active_statement_keys: Mutex::new(Vec::new()),
        });
        let backend = TxnExecutionBackend::new(Arc::clone(&service));
        let mut ctx = TxnCtx::new_for_test(11, 110, false, true);

        backend
            .apply_write(
                &mut ctx,
                WriteRequest::Insert(InsertRequest {
                    table: table.clone(),
                    rows: vec![InsertRow {
                        key: RowKey::PrimaryKey(vec![Value::BigInt(1)]),
                        row: Row::new(vec![Value::BigInt(1), Value::String("kept".into())]),
                    }],
                    ignore_duplicates: false,
                }),
            )
            .await
            .unwrap();

        let err = backend
            .apply_write(
                &mut ctx,
                WriteRequest::Insert(InsertRequest {
                    table,
                    rows: vec![
                        InsertRow {
                            key: RowKey::PrimaryKey(vec![Value::BigInt(2)]),
                            row: Row::new(vec![
                                Value::BigInt(2),
                                Value::String("rolled_back".into()),
                            ]),
                        },
                        InsertRow {
                            key: RowKey::PrimaryKey(vec![Value::BigInt(3)]),
                            row: Row::new(vec![Value::BigInt(3), Value::String("failed".into())]),
                        },
                    ],
                    ignore_duplicates: false,
                }),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("mock put failure"));
        assert!(ctx.mutations.contains_key(&kept_key));
        assert!(!ctx.mutations.contains_key(&rolled_back_key));
        assert!(!ctx.mutations.contains_key(&never_staged_key));
        assert_eq!(ctx.mutations.len(), 1);
        assert_eq!(service.put_calls.load(Ordering::SeqCst), 3);
        assert_eq!(service.rollback_calls.load(Ordering::SeqCst), 1);
    }
}
