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

use tokio::sync::{mpsc, oneshot};

use crate::catalog::types::Row;
use crate::executor::ExecutionOutput;
use crate::session::ExecutionCtx;
use crate::transaction::TxnCtx;
use crate::util::error::TiSqlError;
use crate::Database;

/// A batch of rows streamed from worker to protocol task.
pub type RowBatch = Vec<Row>;

/// Number of rows per batch sent through the mpsc channel.
const BATCH_SIZE: usize = 1024;

/// Bounded channel capacity (number of batches in flight).
const CHANNEL_CAPACITY: usize = 4;

/// Response from query dispatch.
///
/// Read queries carry a `batch_rx` channel for streaming rows. Writes,
/// DDL, transaction control, and SHOW return results directly.
pub enum QueryResponse {
    /// Read query — receive row batches from the channel.
    Rows {
        columns: Vec<String>,
        batch_rx: mpsc::Receiver<Result<RowBatch, TiSqlError>>,
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
        let exec_result = db.execute_plan(plan, &exec_ctx, txn_ctx).await;

        match exec_result {
            Ok((ExecutionOutput::Rows { exec, .. }, returned_ctx)) => {
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
            Ok((ExecutionOutput::Affected { count }, ctx)) => {
                let _ = response_tx.send(QueryResponse::Affected {
                    count,
                    txn_ctx: ctx,
                });
            }
            Ok((ExecutionOutput::Ok, ctx)) => {
                let _ = response_tx.send(QueryResponse::Ok { txn_ctx: ctx });
            }
            Ok((ExecutionOutput::OkWithEffect(_), ctx)) => {
                // Effect already handled in Database::execute_plan
                let _ = response_tx.send(QueryResponse::Ok { txn_ctx: ctx });
            }
            Err(e) => {
                let _ = response_tx.send(QueryResponse::Error {
                    error: e,
                    txn_ctx: None,
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
/// Accumulates up to BATCH_SIZE rows per batch. On error, sends the error
/// through the channel and returns. On EOF, drops the sender (receiver gets None).
async fn stream_rows(
    mut exec: crate::executor::Execution,
    batch_tx: mpsc::Sender<Result<RowBatch, TiSqlError>>,
) {
    let mut batch = Vec::with_capacity(BATCH_SIZE);

    loop {
        match exec.next().await {
            Ok(Some(row)) => {
                batch.push(row);
                if batch.len() >= BATCH_SIZE {
                    let full_batch = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                    if batch_tx.send(Ok(full_batch)).await.is_err() {
                        return; // Receiver dropped (client disconnected)
                    }
                }
            }
            Ok(None) => {
                // Send remaining rows
                if !batch.is_empty() {
                    let _ = batch_tx.send(Ok(batch)).await;
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
        let _ = batch_tx.try_send(Ok(rows));
    }
    // Drop sender immediately — receiver will get the batch then None
    drop(batch_tx);
    QueryResponse::Rows {
        columns,
        batch_rx,
        txn_ctx: None,
    }
}
