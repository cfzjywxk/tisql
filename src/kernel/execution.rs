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
    #[allow(dead_code)]
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

#[allow(dead_code)]
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
