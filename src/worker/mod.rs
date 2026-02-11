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
// - CPU work (parse, bind, operator tree construction) runs on spawned tasks
// - The Execution handle (live operator tree) is returned to the protocol
//   task via oneshot — rows are pulled directly, no intermediate channel
// - tokio::sync::oneshot bridges worker task → protocol task

use std::sync::Arc;

use tokio::sync::oneshot;

use crate::error::TiSqlError;
use crate::executor::{Execution, ExecutionOutput};
use crate::session::ExecutionCtx;
use crate::transaction::TxnCtx;
use crate::Database;

/// Response from query dispatch.
///
/// Read queries return `Rows` with a live `Execution` handle; the protocol
/// task pulls rows directly via `exec.next().await`. Writes and transaction
/// control return results directly.
pub enum QueryResponse {
    /// Read query — pull rows from Execution on the protocol task.
    Rows {
        columns: Vec<String>,
        exec: Execution,
        txn_ctx: Option<TxnCtx>,
    },
    /// Write operation (INSERT/UPDATE/DELETE).
    Affected {
        count: u64,
        txn_ctx: Option<TxnCtx>,
    },
    /// DDL/session command (CREATE TABLE, USE, etc.).
    Ok {
        txn_ctx: Option<TxnCtx>,
    },
    /// Error before any streaming started.
    Error {
        error: TiSqlError,
        txn_ctx: Option<TxnCtx>,
    },
}

/// Dispatch a full query (parse + bind + execute) as an async worker task.
///
/// Spawns a tokio task that:
/// 1. Parses and binds SQL (CPU work)
/// 2. Executes the plan (async — builds operator tree, opens iterators)
/// 3. Returns the result via oneshot
///
/// For read queries, returns an `Execution` handle that the protocol task
/// pulls rows from directly — no intermediate channel or batching.
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

        let columns = if plan.is_read_query() {
            Some(plan.output_columns())
        } else {
            None
        };

        // Execute plan (async — iterators yield during SST I/O)
        let exec_result = db.execute_plan(plan, &exec_ctx, txn_ctx).await;

        let response = match exec_result {
            Ok((ExecutionOutput::Rows { exec, .. }, returned_ctx)) => QueryResponse::Rows {
                columns: columns.unwrap_or_default(),
                exec,
                txn_ctx: returned_ctx,
            },
            Ok((ExecutionOutput::Affected { count }, ctx)) => {
                QueryResponse::Affected { count, txn_ctx: ctx }
            }
            Ok((ExecutionOutput::Ok, ctx)) => QueryResponse::Ok { txn_ctx: ctx },
            Err(e) => QueryResponse::Error {
                error: e,
                txn_ctx: None,
            },
        };

        let _ = response_tx.send(response);
    });

    response_rx.await.unwrap_or(QueryResponse::Error {
        error: TiSqlError::Internal("Worker task dropped".into()),
        txn_ctx: None,
    })
}
