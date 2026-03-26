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

use crate::catalog::types::{ColumnId, DataType, Key, Row, TableId, Value};
use crate::catalog::TableDef;
use crate::tablet::{decode_row_to_values, encode_int_key, encode_key, encode_pk};
use crate::transaction::{TxnCtx, TxnService};
use crate::util::error::Result;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LogicalPointKey {
    PrimaryKey(Vec<Value>),
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

#[derive(Debug, Clone)]
pub(crate) struct ExecutionRow {
    pub key: RowKey,
    pub row: Row,
}

pub(crate) trait ExecutionBackend: Send + Sync {
    fn point_get<'a>(
        &'a self,
        ctx: &'a TxnCtx,
        req: PointGetRequest,
    ) -> impl Future<Output = Result<Option<ExecutionRow>>> + Send + 'a;
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

impl<T: TxnService> ExecutionBackend for TxnExecutionBackend<T> {
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
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::catalog::types::{Lsn, RawValue, Timestamp, TxnId};
    use crate::catalog::ColumnDef;
    use crate::tablet::encode_row;
    use crate::transaction::{CommitInfo, StatementGuard, TxnScanCursor, TxnScanEntry, TxnState};

    struct DummyCursor;

    impl TxnScanCursor for DummyCursor {
        fn advance(&mut self) -> impl Future<Output = Result<()>> + Send + '_ {
            async move { Ok(()) }
        }

        fn current(&self) -> Option<&TxnScanEntry> {
            None
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

    #[tokio::test]
    async fn test_point_get_backend_returns_hidden_row_id_key() {
        let table = TableDef::new(
            501,
            "t".to_string(),
            "default".to_string(),
            vec![
                ColumnDef::new(1, "id".into(), DataType::BigInt, false, None, false),
                ColumnDef::new(2, "name".into(), DataType::Varchar(32), true, None, false),
            ],
            vec![1],
        );
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
}
