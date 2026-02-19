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

// Worker dispatch module — runs ALL database work on a dedicated worker runtime.
//
// Architecture:
// - Protocol layer is a thin I/O shell (TCP read/write, MySQL wire encoding)
// - ALL database work dispatched here: parse, bind, execute, txn control, SHOW
// - For read queries, worker pulls rows from the live operator tree and streams
//   batches to protocol via bounded mpsc channel — Execution stays on worker
// - For writes/DDL/txn control/SHOW, result returned via oneshot (no streaming)
// - Worker handle: if Database has a dedicated worker runtime, tasks are spawned
//   there; otherwise falls back to tokio::spawn (test path)

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use crate::catalog::types::{Row, Value};
use crate::executor::ExecutionOutput;
use crate::session::ExecutionCtx;
use crate::transaction::TxnCtx;
use crate::util::error::TiSqlError;
use crate::Database;

/// A typed row batch plus its byte-budget permit.
pub struct TypedBatch {
    pub rows: Vec<Row>,
    pub est_bytes: usize,
    _budget_permit: OwnedSemaphorePermit,
}

/// Target rows per emitted batch.
const TARGET_BATCH_ROWS: usize = 256;
/// Soft byte threshold per emitted batch.
const TARGET_BATCH_BYTES: usize = 256 * 1024;
/// Hard byte threshold per emitted batch.
const MAX_BATCH_BYTES: usize = 1024 * 1024;
/// Per-query result-memory budget.
const RESULT_BUDGET_BYTES: usize = 8 * 1024 * 1024;
/// Lower/upper bound for channel capacity derived from byte budget.
const MIN_CHANNEL_CAPACITY: usize = 4;
const MAX_CHANNEL_CAPACITY: usize = 64;
const CHANNEL_CAPACITY_FROM_BUDGET: usize = RESULT_BUDGET_BYTES.div_ceil(TARGET_BATCH_BYTES);

/// Bounded channel capacity (number of batches in flight).
const CHANNEL_CAPACITY: usize = if CHANNEL_CAPACITY_FROM_BUDGET < MIN_CHANNEL_CAPACITY {
    MIN_CHANNEL_CAPACITY
} else if CHANNEL_CAPACITY_FROM_BUDGET > MAX_CHANNEL_CAPACITY {
    MAX_CHANNEL_CAPACITY
} else {
    CHANNEL_CAPACITY_FROM_BUDGET
};

/// Response from query dispatch.
///
/// Read queries carry a `batch_rx` channel for streaming rows. Writes,
/// DDL, transaction control, and SHOW return results directly.
pub enum QueryResponse {
    /// Read query — receive row batches from the channel.
    Rows {
        columns: Vec<String>,
        batch_rx: mpsc::Receiver<Result<TypedBatch, TiSqlError>>,
        txn_ctx: Option<TxnCtx>,
    },
    /// Write operation (INSERT/UPDATE/DELETE).
    Affected { count: u64, txn_ctx: Option<TxnCtx> },
    /// DDL/session/txn control command (CREATE TABLE, BEGIN, COMMIT, ROLLBACK, etc.).
    Ok { txn_ctx: Option<TxnCtx> },
    /// Error before any streaming started.
    Error {
        error: TiSqlError,
        txn_ctx: Option<TxnCtx>,
    },
}

/// Dispatch a full query (parse + bind + execute) to the worker runtime.
///
/// ALL commands go through this function — SELECT, INSERT, DDL, BEGIN,
/// COMMIT, ROLLBACK, SHOW. The protocol layer is a thin I/O shell.
///
/// For read queries, the worker pulls rows and streams batches via mpsc.
/// For everything else, the result is returned directly via oneshot.
pub async fn dispatch_full_query(
    worker_handle: Option<&tokio::runtime::Handle>,
    db: Arc<Database>,
    sql: String,
    exec_ctx: ExecutionCtx,
    txn_ctx: Option<TxnCtx>,
) -> QueryResponse {
    let (response_tx, response_rx) = oneshot::channel::<QueryResponse>();

    let task = async move {
        // Check for SHOW commands (string-matched, not parsed)
        let sql_trimmed = sql.trim();
        let sql_lower = sql_trimmed.to_lowercase();
        if sql_lower.starts_with("show ") {
            let response = handle_show(&db, &sql_lower, &exec_ctx);
            let _ = response_tx.send(response);
            return;
        }

        // CPU: parse and bind
        let plan = match db.parse_and_bind(&sql, &exec_ctx) {
            Ok(plan) => plan,
            Err(e) => {
                let _ = response_tx.send(QueryResponse::Error { error: e, txn_ctx });
                return;
            }
        };

        let columns = if plan.is_read_query() {
            Some(plan.output_columns())
        } else {
            None
        };

        // Execute plan (async — iterators yield during SST I/O)
        let (exec_result, returned_ctx) = db.execute_plan(plan, &exec_ctx, txn_ctx).await;

        match (exec_result, returned_ctx) {
            (Ok(ExecutionOutput::Rows { exec, .. }), returned_ctx) => {
                // Create mpsc channel for row batches
                let (batch_tx, batch_rx) = mpsc::channel(CHANNEL_CAPACITY);

                // Send the response with the channel receiver immediately
                let _ = response_tx.send(QueryResponse::Rows {
                    columns: columns.unwrap_or_default(),
                    batch_rx,
                    txn_ctx: returned_ctx,
                });

                // Pull rows from operator tree and stream batches
                stream_rows(exec, batch_tx).await;
            }
            (Ok(ExecutionOutput::Affected { count }), ctx) => {
                let _ = response_tx.send(QueryResponse::Affected {
                    count,
                    txn_ctx: ctx,
                });
            }
            (Ok(ExecutionOutput::Ok), ctx) => {
                let _ = response_tx.send(QueryResponse::Ok { txn_ctx: ctx });
            }
            (Ok(ExecutionOutput::OkWithEffect(_)), ctx) => {
                // Effect already handled in Database::execute_plan
                let _ = response_tx.send(QueryResponse::Ok { txn_ctx: ctx });
            }
            (Err(e), ctx) => {
                let _ = response_tx.send(QueryResponse::Error {
                    error: e,
                    txn_ctx: ctx,
                });
            }
        }
    };

    // Spawn on worker runtime if available, otherwise on current runtime
    match worker_handle {
        Some(handle) => {
            handle.spawn(task);
        }
        None => {
            tokio::spawn(task);
        }
    }

    response_rx.await.unwrap_or(QueryResponse::Error {
        error: TiSqlError::Internal("Worker task dropped".into()),
        txn_ctx: None,
    })
}

/// Pull rows from the Execution handle and send batches through the channel.
///
/// Accumulates rows into row+byte bounded batches. On error, sends the error
/// through the channel and returns. On EOF, drops the sender (receiver gets None).
async fn stream_rows(
    mut exec: crate::executor::Execution,
    batch_tx: mpsc::Sender<Result<TypedBatch, TiSqlError>>,
) {
    let result_budget = Arc::new(Semaphore::new(RESULT_BUDGET_BYTES));
    let mut batch = Vec::with_capacity(TARGET_BATCH_ROWS);
    let mut batch_est_bytes = 0usize;

    loop {
        match exec.next().await {
            Ok(Some(row)) => {
                batch_est_bytes = batch_est_bytes.saturating_add(estimate_row_text_bytes(&row));
                batch.push(row);
                if batch.len() >= TARGET_BATCH_ROWS
                    || batch_est_bytes >= TARGET_BATCH_BYTES
                    || batch_est_bytes >= MAX_BATCH_BYTES
                {
                    match flush_typed_batch(
                        &batch_tx,
                        &result_budget,
                        &mut batch,
                        &mut batch_est_bytes,
                    )
                    .await
                    {
                        Ok(true) => {}
                        Ok(false) => return, // Receiver dropped (client disconnected)
                        Err(e) => {
                            let _ = batch_tx.send(Err(e)).await;
                            return;
                        }
                    }
                }
            }
            Ok(None) => {
                // Send remaining rows
                if !batch.is_empty() {
                    match flush_typed_batch(
                        &batch_tx,
                        &result_budget,
                        &mut batch,
                        &mut batch_est_bytes,
                    )
                    .await
                    {
                        Ok(true) => {}
                        Ok(false) => return,
                        Err(e) => {
                            let _ = batch_tx.send(Err(e)).await;
                            return;
                        }
                    }
                }
                return; // EOF — drop sender, receiver gets None
            }
            Err(e) => {
                // Send error through the channel
                let _ = batch_tx.send(Err(e)).await;
                return;
            }
        }
    }
}

async fn flush_typed_batch(
    batch_tx: &mpsc::Sender<Result<TypedBatch, TiSqlError>>,
    result_budget: &Arc<Semaphore>,
    batch: &mut Vec<Row>,
    batch_est_bytes: &mut usize,
) -> Result<bool, TiSqlError> {
    if batch.is_empty() {
        return Ok(true);
    }

    let est_bytes = (*batch_est_bytes).max(1);
    let permits = u32::try_from(est_bytes).map_err(|_| {
        TiSqlError::Internal(format!(
            "Batch estimated bytes {est_bytes} exceed semaphore permit range"
        ))
    })?;
    let permit = Arc::clone(result_budget)
        .acquire_many_owned(permits)
        .await
        .map_err(|_| TiSqlError::Internal("Result budget semaphore unexpectedly closed".into()))?;

    let rows = std::mem::replace(batch, Vec::with_capacity(TARGET_BATCH_ROWS));
    *batch_est_bytes = 0;
    let typed_batch = TypedBatch {
        rows,
        est_bytes,
        _budget_permit: permit,
    };

    if batch_tx.send(Ok(typed_batch)).await.is_err() {
        return Ok(false);
    }

    Ok(true)
}

fn estimate_row_text_bytes(row: &Row) -> usize {
    row.iter().fold(0usize, |acc, val| {
        acc.saturating_add(estimate_value_text_bytes(val))
    })
}

fn estimate_value_text_bytes(val: &Value) -> usize {
    match val {
        Value::Null => 1, // 0xFB
        Value::Boolean(b) => {
            let len = if *b { 4 } else { 5 }; // true/false
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::TinyInt(v) => {
            let mut buf = itoa::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::SmallInt(v) => {
            let mut buf = itoa::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::Int(v) => {
            let mut buf = itoa::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::BigInt(v) => {
            let mut buf = itoa::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::Float(v) => {
            let mut buf = ryu::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::Double(v) => {
            let mut buf = ryu::Buffer::new();
            let len = buf.format(*v).len();
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::Decimal(v) | Value::String(v) => lenenc_prefix_len(v.len()).saturating_add(v.len()),
        Value::Bytes(v) => lenenc_prefix_len(v.len()).saturating_add(v.len()),
        Value::Date(_) => lenenc_prefix_len(10).saturating_add(10),
        Value::Time(v) => {
            let len = estimate_time_text_len(*v);
            lenenc_prefix_len(len).saturating_add(len)
        }
        Value::DateTime(v) | Value::Timestamp(v) => {
            let frac = v.rem_euclid(1_000_000);
            let len = if frac == 0 { 19 } else { 26 };
            lenenc_prefix_len(len).saturating_add(len)
        }
    }
}

fn estimate_time_text_len(micros: i64) -> usize {
    let abs = micros.saturating_abs() as u64;
    let total_secs = abs / 1_000_000;
    let hours = total_secs / 3600;
    let mut len = decimal_len_u64(hours).max(2) + 6; // HH:MM:SS
    if micros < 0 {
        len += 1; // '-'
    }
    if abs % 1_000_000 != 0 {
        len += 7; // ".ffffff"
    }
    len
}

fn decimal_len_u64(v: u64) -> usize {
    let mut buf = itoa::Buffer::new();
    buf.format(v).len()
}

fn lenenc_prefix_len(len: usize) -> usize {
    if len < 251 {
        1
    } else if len < (1 << 16) {
        3
    } else if len < (1 << 24) {
        4
    } else {
        9
    }
}

/// Handle SHOW commands directly on the worker (catalog scan).
fn handle_show(db: &Database, sql_lower: &str, exec_ctx: &ExecutionCtx) -> QueryResponse {
    if sql_lower.contains("databases") {
        match db.list_schemas() {
            Ok(databases) => {
                let rows: Vec<Row> = databases
                    .into_iter()
                    .map(|name| Row::new(vec![crate::catalog::types::Value::String(name)]))
                    .collect();
                make_show_response(vec!["Database".to_string()], rows)
            }
            Err(e) => QueryResponse::Error {
                error: e,
                txn_ctx: None,
            },
        }
    } else if sql_lower.contains("tables") {
        let database = &exec_ctx.current_db;
        let col_name = format!("Tables_in_{database}");
        match db.list_tables(database) {
            Ok(tables) => {
                let rows: Vec<Row> = tables
                    .into_iter()
                    .map(|name| Row::new(vec![crate::catalog::types::Value::String(name)]))
                    .collect();
                make_show_response(vec![col_name], rows)
            }
            Err(e) => QueryResponse::Error {
                error: e,
                txn_ctx: None,
            },
        }
    } else if sql_lower.contains("warnings") {
        make_show_response(
            vec![
                "Level".to_string(),
                "Code".to_string(),
                "Message".to_string(),
            ],
            vec![],
        )
    } else if sql_lower.contains("status") {
        make_show_response(
            vec!["Variable_name".to_string(), "Value".to_string()],
            vec![],
        )
    } else {
        QueryResponse::Error {
            error: TiSqlError::Internal(format!("Unsupported SHOW command: {sql_lower}")),
            txn_ctx: None,
        }
    }
}

/// Create a QueryResponse::Rows with all rows in a single batch.
fn make_show_response(columns: Vec<String>, rows: Vec<Row>) -> QueryResponse {
    let (batch_tx, batch_rx) = mpsc::channel(1);
    if !rows.is_empty() {
        // Send all rows as one batch (SHOW results are always small)
        let _ = batch_tx.try_send(make_typed_batch(rows));
    }
    // Drop sender immediately — receiver will get the batch then None
    drop(batch_tx);
    QueryResponse::Rows {
        columns,
        batch_rx,
        txn_ctx: None,
    }
}

fn make_typed_batch(rows: Vec<Row>) -> Result<TypedBatch, TiSqlError> {
    let est_bytes = rows
        .iter()
        .map(estimate_row_text_bytes)
        .fold(0usize, |acc, n| acc.saturating_add(n))
        .max(1);

    let permits = u32::try_from(est_bytes).map_err(|_| {
        TiSqlError::Internal(format!(
            "SHOW batch estimated bytes {est_bytes} exceed semaphore permit range"
        ))
    })?;
    let sem = Arc::new(Semaphore::new(est_bytes));
    let permit = Arc::clone(&sem)
        .try_acquire_many_owned(permits)
        .map_err(|_| TiSqlError::Internal("Failed to acquire SHOW batch result budget".into()))?;

    Ok(TypedBatch {
        rows,
        est_bytes,
        _budget_permit: permit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::types::Value;

    #[test]
    fn test_channel_capacity_from_budget_defaults() {
        assert_eq!(CHANNEL_CAPACITY, 32);
    }

    #[test]
    fn test_estimate_value_text_bytes_numeric_nonzero() {
        let int_len = estimate_value_text_bytes(&Value::Int(12345));
        let float_len = estimate_value_text_bytes(&Value::Double(std::f64::consts::PI));
        assert!(int_len > 0);
        assert!(float_len > 0);
    }

    #[test]
    fn test_estimate_time_text_len_fraction_and_sign() {
        assert_eq!(estimate_time_text_len(0), 8); // 00:00:00
        assert_eq!(estimate_time_text_len(-1_000_000), 9); // -00:00:01
        assert_eq!(estimate_time_text_len(1_234_567), 15); // 00:00:01.234567
    }
}
