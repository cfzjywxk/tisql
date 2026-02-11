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

// Worker dispatch module — runs database work on async tokio tasks.
//
// Architecture:
// - Single tokio runtime for both network I/O and query processing
// - CPU work (parse, bind, row streaming) runs inline on async tasks
// - Storage I/O (execute_plan) awaited directly (iterators are async)
// - tokio::sync::mpsc bridges worker task → network task (row batches)
// - tokio::sync::oneshot for response delivery

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::error::TiSqlError;
use crate::executor::ExecutionOutput;
use crate::session::ExecutionCtx;
use crate::transaction::TxnCtx;
use crate::types::Row;
use crate::Database;

/// Batch of rows sent through the streaming channel.
pub type RowBatch = Vec<Row>;

/// Maximum rows per batch before sending through the channel.
const BATCH_SIZE: usize = 1024;

/// Completion signal for a streaming read query.
pub enum QueryDone {
    /// Execution completed successfully.
    Success { txn_ctx: Option<TxnCtx> },
    /// Execution failed.
    Error {
        error: TiSqlError,
        txn_ctx: Option<TxnCtx>,
    },
}

/// Response from query dispatch.
///
/// Read queries return `Rows` with streaming channels; writes and
/// transaction control return results directly.
pub enum QueryResponse {
    /// Read query — stream rows through mpsc, completion via oneshot.
    Rows {
        columns: Vec<String>,
        batch_rx: mpsc::Receiver<RowBatch>,
        done_rx: oneshot::Receiver<QueryDone>,
    },
    /// Write operation (INSERT/UPDATE/DELETE).
    Affected { count: u64, txn_ctx: Option<TxnCtx> },
    /// DDL/session command (CREATE TABLE, USE, etc.).
    Ok { txn_ctx: Option<TxnCtx> },
    /// Error before any streaming started.
    Error {
        error: TiSqlError,
        txn_ctx: Option<TxnCtx>,
    },
}

/// Dispatch a full query (parse + bind + execute) as an async worker task.
///
/// Spawns a tokio task that:
/// 1. Parses and binds SQL (CPU work, inline)
/// 2. Executes the plan (async — iterators yield during I/O)
/// 3. Streams rows via channels (CPU work, inline)
///
/// For read queries, returns immediately with streaming channels.
/// For writes, awaits execution completion.
pub async fn dispatch_full_query(
    db: Arc<Database>,
    sql: String,
    exec_ctx: ExecutionCtx,
    txn_ctx: Option<TxnCtx>,
) -> QueryResponse {
    let (response_tx, response_rx) = oneshot::channel::<QueryResponse>();

    tokio::spawn(async move {
        // CPU: parse and bind
        let plan = match db.parse_and_bind(&sql, &exec_ctx) {
            Ok(plan) => plan,
            Err(e) => {
                let _ = response_tx.send(QueryResponse::Error { error: e, txn_ctx });
                return;
            }
        };

        if plan.is_read_query() {
            let columns = plan.output_columns();
            let (batch_tx, batch_rx) = mpsc::channel(4);
            let (done_tx, done_rx) = oneshot::channel();

            // Send response with streaming channels to caller
            if response_tx
                .send(QueryResponse::Rows {
                    columns,
                    batch_rx,
                    done_rx,
                })
                .is_err()
            {
                return; // caller dropped
            }

            // Execute plan (async — iterators yield during SST I/O)
            let exec_result = db.execute_plan(plan, &exec_ctx, txn_ctx).await;

            match exec_result {
                Ok((ExecutionOutput::Rows { mut exec, .. }, returned_ctx)) => {
                    // Stream rows from live operator tree via channel
                    let mut batch = Vec::with_capacity(BATCH_SIZE);
                    loop {
                        match exec.next().await {
                            Ok(Some(row)) => {
                                batch.push(row);
                                if batch.len() >= BATCH_SIZE {
                                    if batch_tx
                                        .send(std::mem::take(&mut batch))
                                        .await
                                        .is_err()
                                    {
                                        return; // receiver dropped (client disconnected)
                                    }
                                    batch = Vec::with_capacity(BATCH_SIZE);
                                }
                            }
                            Ok(None) => {
                                if !batch.is_empty() {
                                    let _ = batch_tx.send(batch).await;
                                }
                                drop(batch_tx);
                                let _ = done_tx.send(QueryDone::Success {
                                    txn_ctx: returned_ctx,
                                });
                                return;
                            }
                            Err(e) => {
                                drop(batch_tx);
                                let _ = done_tx.send(QueryDone::Error {
                                    error: e,
                                    txn_ctx: returned_ctx,
                                });
                                return;
                            }
                        }
                    }
                }
                Ok((_, returned_ctx)) => {
                    drop(batch_tx);
                    let _ = done_tx.send(QueryDone::Success {
                        txn_ctx: returned_ctx,
                    });
                }
                Err(e) => {
                    drop(batch_tx);
                    let _ = done_tx.send(QueryDone::Error {
                        error: e,
                        txn_ctx: None,
                    });
                }
            }
        } else {
            // Write/DDL: execute (async)
            let exec_result = db.execute_plan(plan, &exec_ctx, txn_ctx).await;

            match exec_result {
                Ok((ExecutionOutput::Affected { count }, ctx)) => {
                    let _ = response_tx.send(QueryResponse::Affected {
                        count,
                        txn_ctx: ctx,
                    });
                }
                Ok((ExecutionOutput::Ok, ctx)) => {
                    let _ = response_tx.send(QueryResponse::Ok { txn_ctx: ctx });
                }
                Ok((ExecutionOutput::Rows { .. }, ctx)) => {
                    let _ = response_tx.send(QueryResponse::Ok { txn_ctx: ctx });
                }
                Err(e) => {
                    let _ = response_tx.send(QueryResponse::Error {
                        error: e,
                        txn_ctx: None,
                    });
                }
            }
        }
    });

    response_rx.await.unwrap_or(QueryResponse::Error {
        error: TiSqlError::Internal("Worker task dropped".into()),
        txn_ctx: None,
    })
}
