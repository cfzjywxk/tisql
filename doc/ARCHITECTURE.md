# TiSQL Architecture Design

## Layer Overview

```
┌─────────────────────────────────────────────────────────┐
│                   Protocol Layer                        │
│              (MySQL/PostgreSQL Wire Protocol)           │
└─────────────────────────┬───────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│                    Session Layer                        │
│           (Connection, Auth, Session State)             │
└─────────────────────────┬───────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│                     SQL Layer                           │
│         Parser → Binder → Optimizer → Executor          │
└─────────────────────────┬───────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│                  Catalog Layer                          │
│            (Schema, Table, Index Metadata)              │
└─────────────────────────┬───────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│                Transaction Layer                        │
│              (MVCC, Concurrency Control)                │
└─────────────────────────┬───────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│                  Storage Layer                          │
│         (MemTable / LSM-Tree / B-Tree Engine)           │
└─────────────────────────┬───────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│                    Log Layer                            │
│              (WAL / Raft / Paxos)                       │
└─────────────────────────────────────────────────────────┘
```

## Core Trait Definitions

### 1. Storage Engine Trait

This is the foundation. Keep it simple - just KV operations with ordering.

```rust
// src/storage/mod.rs

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type Timestamp = u64;

/// Core KV storage interface - all engines implement this
pub trait StorageEngine: Send + Sync + 'static {
    type Snapshot: Snapshot;
    type WriteBatch: WriteBatch;

    /// Point lookup
    fn get(&self, key: &[u8]) -> Result<Option<Value>>;

    /// Point lookup at specific timestamp (for MVCC)
    fn get_at(&self, key: &[u8], ts: Timestamp) -> Result<Option<Value>>;

    /// Write a single key-value
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key
    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Atomic batch write
    fn write_batch(&self, batch: Self::WriteBatch) -> Result<()>;

    /// Create a consistent snapshot for reads
    fn snapshot(&self) -> Result<Self::Snapshot>;

    /// Range scan - returns iterator
    fn scan(&self, range: Range<&[u8]>) -> Result<Box<dyn Iterator<Item = (Key, Value)>>>;
}

/// Immutable snapshot for consistent reads
pub trait Snapshot: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Value>>;
    fn scan(&self, range: Range<&[u8]>) -> Result<Box<dyn Iterator<Item = (Key, Value)>>>;
}

/// Batch of writes to apply atomically
pub trait WriteBatch: Default {
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
    fn clear(&mut self);
}
```

### 2. Catalog Trait

Schema management - tables, columns, indexes.

```rust
// src/catalog/mod.rs

pub type TableId = u64;
pub type ColumnId = u32;
pub type IndexId = u64;

#[derive(Clone, Debug)]
pub struct ColumnDef {
    pub id: ColumnId,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<Expr>,
}

#[derive(Clone, Debug)]
pub struct TableDef {
    pub id: TableId,
    pub name: String,
    pub schema: String,  // database/schema name
    pub columns: Vec<ColumnDef>,
    pub primary_key: Vec<ColumnId>,
    pub indexes: Vec<IndexDef>,
}

#[derive(Clone, Debug)]
pub struct IndexDef {
    pub id: IndexId,
    pub name: String,
    pub columns: Vec<ColumnId>,
    pub unique: bool,
}

/// Catalog interface - metadata management
pub trait Catalog: Send + Sync {
    /// Schema (database) operations
    fn create_schema(&self, name: &str) -> Result<()>;
    fn drop_schema(&self, name: &str) -> Result<()>;
    fn list_schemas(&self) -> Result<Vec<String>>;

    /// Table operations
    fn create_table(&self, table: TableDef) -> Result<TableId>;
    fn drop_table(&self, schema: &str, table: &str) -> Result<()>;
    fn get_table(&self, schema: &str, table: &str) -> Result<Option<TableDef>>;
    fn get_table_by_id(&self, id: TableId) -> Result<Option<TableDef>>;
    fn list_tables(&self, schema: &str) -> Result<Vec<TableDef>>;

    /// Index operations
    fn create_index(&self, table_id: TableId, index: IndexDef) -> Result<IndexId>;
    fn drop_index(&self, table_id: TableId, index_name: &str) -> Result<()>;
}
```

### 3. Transaction Trait

MVCC and concurrency control.

```rust
// src/transaction/mod.rs

pub type TxnId = u64;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
    SnapshotIsolation,
}

/// Transaction interface
pub trait Transaction: Send {
    /// Get transaction ID
    fn id(&self) -> TxnId;

    /// Get start timestamp
    fn start_ts(&self) -> Timestamp;

    /// Read a key within transaction
    fn get(&self, key: &[u8]) -> Result<Option<Value>>;

    /// Write a key within transaction (buffered until commit)
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key within transaction
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Scan range within transaction
    fn scan(&self, range: Range<&[u8]>) -> Result<Box<dyn Iterator<Item = (Key, Value)>>>;

    /// Commit transaction - returns commit timestamp on success
    fn commit(self) -> Result<Timestamp>;

    /// Rollback transaction
    fn rollback(self) -> Result<()>;
}

/// Transaction manager - creates and coordinates transactions
pub trait TransactionManager: Send + Sync {
    type Txn: Transaction;

    /// Begin a new transaction (allocates start_ts)
    fn begin(&self, isolation: IsolationLevel) -> Result<Self::Txn>;
}
```

### 4. Executor Trait

Query execution interface.

```rust
// src/executor/mod.rs

use arrow::record_batch::RecordBatch;

/// Execution context for a query
pub struct ExecContext<'a> {
    pub catalog: &'a dyn Catalog,
    pub txn: &'a mut dyn Transaction,
    pub session: &'a Session,
}

/// Physical plan node - volcano model iterator
pub trait PhysicalPlan: Send {
    /// Return schema of output
    fn schema(&self) -> &Schema;

    /// Execute and return batches (pull-based)
    fn execute(&mut self, ctx: &mut ExecContext) -> Result<Option<RecordBatch>>;

    /// Children nodes (for explain)
    fn children(&self) -> Vec<&dyn PhysicalPlan>;
}

/// Alternative: Push-based execution for better performance
pub trait PushExecutor: Send {
    fn schema(&self) -> &Schema;

    /// Push execution - calls sink for each batch
    fn execute<S: Sink>(&mut self, ctx: &mut ExecContext, sink: &mut S) -> Result<()>;
}

pub trait Sink {
    fn consume(&mut self, batch: RecordBatch) -> Result<()>;
}
```

### 5. SQL Layer Trait

Parser → Binder → Optimizer → Executor pipeline.

```rust
// src/sql/mod.rs

/// Parsed but unresolved SQL statement
pub enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    Begin,
    Commit,
    Rollback,
    // ... more
}

/// Logical plan - resolved and typed
pub enum LogicalPlan {
    Scan { table: TableDef, filter: Option<Expr> },
    Project { input: Box<LogicalPlan>, exprs: Vec<Expr> },
    Filter { input: Box<LogicalPlan>, predicate: Expr },
    Join { left: Box<LogicalPlan>, right: Box<LogicalPlan>, on: Expr, join_type: JoinType },
    Aggregate { input: Box<LogicalPlan>, group_by: Vec<Expr>, aggs: Vec<AggExpr> },
    Sort { input: Box<LogicalPlan>, order_by: Vec<OrderBy> },
    Limit { input: Box<LogicalPlan>, limit: usize, offset: usize },
    Insert { table: TableDef, values: Vec<Vec<Expr>> },
    // ... more
}

/// SQL frontend interface
pub trait SqlFrontend: Send + Sync {
    /// Parse SQL string to AST
    fn parse(&self, sql: &str) -> Result<Vec<Statement>>;

    /// Bind AST to logical plan (resolve names, check types)
    fn bind(&self, stmt: Statement, catalog: &dyn Catalog) -> Result<LogicalPlan>;

    /// Optimize logical plan
    fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan>;

    /// Convert logical plan to physical plan
    fn plan(&self, logical: LogicalPlan) -> Result<Box<dyn PhysicalPlan>>;
}
```

### 6. Session Trait

Connection and session state.

```rust
// src/session/mod.rs

pub struct SessionConfig {
    pub default_schema: String,
    pub isolation_level: IsolationLevel,
    pub autocommit: bool,
    pub timezone: String,
}

/// Session state
pub trait Session: Send {
    fn id(&self) -> u64;
    fn config(&self) -> &SessionConfig;
    fn config_mut(&mut self) -> &mut SessionConfig;

    /// Current transaction (if any)
    fn current_txn(&self) -> Option<&dyn Transaction>;
    fn current_txn_mut(&mut self) -> Option<&mut dyn Transaction>;

    /// Execute a SQL statement
    fn execute(&mut self, sql: &str) -> Result<QueryResult>;
}

pub enum QueryResult {
    Rows { schema: Schema, batches: Vec<RecordBatch> },
    Affected { rows: u64 },
    Ok,
}
```

### 7. Log/WAL Trait

Write-ahead log for durability.

```rust
// src/log/mod.rs

pub type Lsn = u64;  // Log Sequence Number

#[derive(Clone, Debug)]
pub struct LogEntry {
    pub lsn: Lsn,
    pub txn_id: TxnId,
    pub op: LogOp,
}

#[derive(Clone, Debug)]
pub enum LogOp {
    Put { key: Key, value: Value },
    Delete { key: Key },
    Commit { commit_ts: Timestamp },
    Rollback,
}

/// Write-ahead log interface
pub trait WriteAheadLog: Send + Sync {
    /// Append entry, returns LSN
    fn append(&self, entry: LogEntry) -> Result<Lsn>;

    /// Append batch of entries atomically
    fn append_batch(&self, entries: Vec<LogEntry>) -> Result<Lsn>;

    /// Sync to disk up to LSN
    fn sync(&self, lsn: Lsn) -> Result<()>;

    /// Read entries from LSN (for recovery)
    fn read_from(&self, lsn: Lsn) -> Result<Vec<LogEntry>>;

    /// Truncate log before LSN (after checkpoint)
    fn truncate_before(&self, lsn: Lsn) -> Result<()>;
}
```

## Milestone Implementation Guide

### M0: Hello Query (SELECT 1+1)

**Goal**: Minimal end-to-end execution.

**Components needed**:
- SQL Parser (use `sqlparser-rs`)
- Expression evaluator for literals/arithmetic
- Simple executor that handles expression-only selects

```rust
// Minimal main.rs for M0
fn main() {
    let sql = "SELECT 1 + 1";
    let parser = SqlParser::new();
    let stmt = parser.parse(sql).unwrap();
    let result = execute_simple(stmt);
    println!("{:?}", result);  // Should print: 2
}
```

**Don't need yet**: Storage, catalog, transactions, sessions.

### M1: MemTable CRUD

**Goal**: CREATE TABLE, INSERT, SELECT on in-memory storage.

**New components**:
- `MemTableEngine` implementing `StorageEngine`
- `MemCatalog` implementing `Catalog`
- Table scan executor
- Insert executor

```rust
// Simple memtable implementation
pub struct MemTableEngine {
    data: RwLock<BTreeMap<Key, Value>>,
}

impl StorageEngine for MemTableEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        Ok(self.data.read().unwrap().get(key).cloned())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.data.write().unwrap().insert(key.to_vec(), value.to_vec());
        Ok(())
    }
    // ... etc
}
```

**Key encoding**: `{table_id}{primary_key_columns}` → `{encoded_row}`

### M2: Basic Transactions

**Goal**: BEGIN/COMMIT/ROLLBACK with snapshot isolation.

**New components**:
- `MvccEngine` wrapping storage engine
- `MvccTransaction` implementing `Transaction`
- Timestamp oracle (simple atomic counter for now)

**Key encoding with MVCC**: `{user_key}{!timestamp}` (inverted timestamp for newest-first scan)

```rust
pub struct MvccEngine<E: StorageEngine> {
    engine: E,
    ts_oracle: AtomicU64,
}

pub struct MvccTransaction<E: StorageEngine> {
    engine: Arc<MvccEngine<E>>,
    start_ts: Timestamp,
    writes: HashMap<Key, Option<Value>>,  // buffered writes
}
```

### M3: MySQL Protocol

**Goal**: Connect via `mysql -h 127.0.0.1 -P 4000`.

**New components**:
- TCP listener
- MySQL protocol decoder/encoder
- Session management

**Recommendation**: Use existing crate or port minimal implementation.
- Check `mysql_async` for protocol details
- Or start with a simpler text protocol for testing

```rust
// Simplified protocol handler
async fn handle_connection(stream: TcpStream, server: Arc<Server>) {
    let mut conn = MySqlConnection::new(stream);
    conn.handshake().await?;

    loop {
        let packet = conn.read_packet().await?;
        match packet {
            Packet::Query(sql) => {
                let result = server.execute(&mut session, &sql).await?;
                conn.write_result(result).await?;
            }
            Packet::Quit => break,
            // ... COM_STMT_PREPARE, etc for prepared statements
        }
    }
}
```

### M4: Persistence

**Goal**: Survive restart.

**New components**:
- WAL implementation
- Recovery on startup
- Periodic checkpointing

```rust
pub struct PersistentEngine<E: StorageEngine> {
    memtable: E,
    wal: Box<dyn WriteAheadLog>,
}

impl PersistentEngine {
    pub fn recover(wal_path: &Path) -> Result<Self> {
        let wal = FileWal::open(wal_path)?;
        let memtable = MemTableEngine::new();

        // Replay WAL
        for entry in wal.read_from(0)? {
            match entry.op {
                LogOp::Put { key, value } => memtable.put(&key, &value)?,
                LogOp::Delete { key } => memtable.delete(&key)?,
                _ => {}
            }
        }

        Ok(Self { memtable, wal })
    }
}
```

### M5: Sysbench

**Goal**: `sysbench oltp_read_write --mysql-host=127.0.0.1 --mysql-port=4000 run`

**Requirements**:
- All previous milestones working
- Prepared statement support (COM_STMT_PREPARE)
- Auto-increment columns
- Basic indexes (at least primary key lookups)
- Performance optimization (connection pooling, batch writes)

**Sysbench tables**:
```sql
CREATE TABLE sbtest1 (
  id INT NOT NULL AUTO_INCREMENT,
  k INT NOT NULL DEFAULT 0,
  c CHAR(120) NOT NULL DEFAULT '',
  pad CHAR(60) NOT NULL DEFAULT '',
  PRIMARY KEY (id),
  KEY k_1 (k)
);
```

## Recommended Crate Dependencies

```toml
[dependencies]
# SQL parsing - don't reinvent this
sqlparser = "0.40"

# Arrow for columnar data (optional but useful)
arrow = "50"

# Async runtime
tokio = { version = "1", features = ["full"] }

# Serialization for row encoding
bincode = "1.3"
serde = { version = "1", features = ["derive"] }

# Error handling
thiserror = "1"
anyhow = "1"

# Logging
tracing = "0.1"

# Bytes handling
bytes = "1"

# For later: RocksDB bindings as alternative engine
# rocksdb = "0.21"
```

## Project Structure

```
tisql/
├── Cargo.toml
├── src/
│   ├── main.rs
│   ├── lib.rs
│   ├── error.rs           # Error types
│   ├── types.rs           # Common types (DataType, Value, etc.)
│   │
│   ├── storage/
│   │   ├── mod.rs         # StorageEngine trait
│   │   ├── memtable.rs    # MemTable implementation
│   │   ├── mvcc.rs        # MVCC wrapper
│   │   └── encoding.rs    # Key/value encoding
│   │
│   ├── catalog/
│   │   ├── mod.rs         # Catalog trait
│   │   └── memory.rs      # In-memory catalog
│   │
│   ├── transaction/
│   │   ├── mod.rs         # Transaction traits
│   │   └── mvcc.rs        # MVCC transaction impl
│   │
│   ├── sql/
│   │   ├── mod.rs
│   │   ├── parser.rs      # Wrapper around sqlparser
│   │   ├── binder.rs      # Name resolution, type checking
│   │   ├── planner.rs     # Logical plan
│   │   └── optimizer.rs   # Basic optimizations
│   │
│   ├── executor/
│   │   ├── mod.rs         # Executor traits
│   │   ├── scan.rs        # Table scan
│   │   ├── filter.rs      # Filter
│   │   ├── project.rs     # Projection
│   │   ├── insert.rs      # Insert
│   │   └── aggregate.rs   # Aggregation
│   │
│   ├── session/
│   │   ├── mod.rs         # Session management
│   │   └── context.rs     # Execution context
│   │
│   ├── protocol/
│   │   ├── mod.rs
│   │   └── mysql.rs       # MySQL protocol
│   │
│   └── log/
│       ├── mod.rs         # WAL trait
│       └── file.rs        # File-based WAL
│
└── tests/
    ├── sql_tests.rs       # SQL correctness tests
    └── integration.rs     # End-to-end tests
```

## Key Design Decisions

### 1. Use Arrow RecordBatch for intermediate results?

**Pros**: Columnar format, good for analytics, interop with DataFusion
**Cons**: Overhead for simple OLTP queries

**Recommendation**: Start with simple `Vec<Row>`, add Arrow later for analytics workloads.

### 2. Pull-based (Volcano) vs Push-based execution?

**Recommendation**: Start with Volcano (easier to implement), refactor to push-based for performance later.

### 3. Row format encoding?

**Recommendation**: Use simple `bincode` serialization initially. Optimize later.

```rust
#[derive(Serialize, Deserialize)]
struct Row {
    values: Vec<Value>,
}

fn encode_row(row: &Row) -> Vec<u8> {
    bincode::serialize(row).unwrap()
}
```

### 4. How to handle NULL?

**Recommendation**: Use `Option<Value>` or a null bitmap.

## Testing Strategy

1. **Unit tests**: Each component in isolation
2. **SQL tests**: `sqllogictest` format for SQL correctness
3. **Integration tests**: Full stack execution
4. **Benchmarks**: `criterion` for performance regression

```rust
// Example sqllogictest style
// test.slt
statement ok
CREATE TABLE t (a INT, b TEXT)

statement ok
INSERT INTO t VALUES (1, 'hello'), (2, 'world')

query IT rowsort
SELECT * FROM t
----
1 hello
2 world
```

## References

- OceanBase storage: `~/src/oceanbase/src/storage/`
- OceanBase log service: `~/src/oceanbase/src/logservice/`
- TiKV transaction: https://github.com/tikv/tikv/tree/master/src/storage/txn
- RisingLight (educational DB in Rust): https://github.com/risinglightdb/risinglight
- SQLite architecture: https://sqlite.org/arch.html
