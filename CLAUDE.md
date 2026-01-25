# Claude Code Project Notes - TiSQL

## Development Guidelines

### Design Documents
Do NOT push development design documents to GitHub. Keep design docs local only (in `docs/design/` if needed locally - it's gitignored).

---

## Project Overview

TiSQL is a TiDB-compatible SQL database written in Rust. It aims to be a learning project demonstrating database internals with MySQL protocol compatibility.

### Repository
- GitHub: https://github.com/cfzjywxk/tisql
- License: Apache-2.0

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      MySQL Client                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Tokio Runtime                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              MySqlServer (opensrv-mysql)                  │   │
│  │         Network I/O, MySQL protocol handling              │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    yatp FuturePool                               │
│              "tisql-worker" thread pool                          │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Database                               │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │   │
│  │  │  SQLEngine  │  │ TxnService  │  │   Catalog   │       │   │
│  │  │Parser→Exec  │  │(owns storage│  │  (schemas)  │       │   │
│  │  └─────────────┘  │ clog, CM)   │  └─────────────┘       │   │
│  │                   └─────────────┘                         │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Transaction Layer                              │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              TransactionService (TxnService trait)       │    │
│  │  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐  │    │
│  │  │MvccMemTable   │ │ FileClogSvc   │ │ConcurrencyMgr │  │    │
│  │  │ (storage)     │ │ (durability)  │ │ (TSO + locks) │  │    │
│  │  └───────────────┘ └───────────────┘ └───────────────┘  │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### Session and Query Context Flow

```
MySQL Connection established
        ↓
Session::new() - unique session ID, SessionVars
        ↓
For each SQL statement:
        ↓
session.new_query_ctx() → QueryCtx (inherits from SessionVars)
        ↓
Database.handle_mp_query_with_ctx(sql, query_ctx)
        ↓
┌────────────────────────────────────────────────┐
│ SQLEngine.handle_mp_query(sql, ctx, txn, cat)  │
│   ├── Parser.parse_one(sql) → AST              │
│   ├── Binder.bind(ast) → LogicalPlan           │
│   └── Executor.execute(plan, txn_service)      │
│         ├── Read: txn_service.snapshot()       │
│         └── Write: txn_service.begin() + commit│
└────────────────────────────────────────────────┘
```

---

## Module Structure

```
src/
├── main.rs              # CLI entry point (--data-dir, --port)
├── lib.rs               # Database, SQLEngine, QueryResult
├── error.rs             # TiSqlError, Result type
├── types.rs             # Value, Key, Timestamp, TxnId, Lsn
│
├── session/             # Session management (TiDB-style)
│   └── mod.rs           # Session, SessionVars, QueryCtx
│
├── concurrency/         # Concurrency control
│   └── mod.rs           # ConcurrencyManager (TSO + in-memory lock table)
│
├── clog/                # Commit Log (WAL)
│   ├── mod.rs           # ClogEntry, ClogOp, ClogBatch, ClogService trait
│   └── file.rs          # FileClogService - file-based implementation
│
├── transaction/         # Transaction management
│   ├── mod.rs           # Module exports
│   ├── api.rs           # TxnService, ReadSnapshot, Txn traits
│   ├── service.rs       # TransactionService implementation
│   ├── handle.rs        # TxnHandle (read-write transaction)
│   └── snapshot.rs      # StorageSnapshot (read-only snapshot)
│
├── storage/             # Storage engine
│   ├── mod.rs           # StorageEngine trait, WriteBatch, key encoding
│   ├── memtable.rs      # MemTableEngine (BTreeMap-based, legacy)
│   └── mvcc_memtable.rs # MvccMemTableEngine (crossbeam-skiplist, MVCC)
│
├── catalog/             # Schema metadata
│   ├── mod.rs           # Catalog trait, TableDef, ColumnDef
│   └── memory.rs        # MemoryCatalog
│
├── sql/                 # SQL processing
│   ├── mod.rs           # Module exports
│   ├── parser.rs        # SQL parser (wraps sqlparser)
│   ├── binder.rs        # Semantic analysis, name resolution
│   └── plan.rs          # LogicalPlan, expressions
│
├── executor/            # Query execution
│   ├── mod.rs           # Executor trait, ExecutionResult
│   └── simple.rs        # SimpleExecutor (volcano-style, uses TxnService)
│
├── protocol/            # Network protocol
│   ├── mod.rs           # MySqlServer
│   └── mysql.rs         # MySqlBackend (opensrv-mysql handler)
│
├── worker/              # Thread pool
│   └── mod.rs           # WorkerPool.handle_mp_query()
│
├── codec/               # TiDB-compatible encoding
│   ├── mod.rs           # Module exports
│   ├── key.rs           # Key encoding (table/row/index keys)
│   ├── row.rs           # Row encoding
│   ├── number.rs        # Number encoding (comparable format)
│   └── bytes.rs         # Bytes encoding (escape format)
│
└── util/                # Utilities
    ├── mod.rs           # Module exports
    ├── log.rs           # Logging macros (log_info!, log_trace!, etc.)
    └── timing.rs        # Timer for performance measurement
```

---

## Key Components

### 1. Session Management (TiDB-style)

Following TiDB's architecture (`pkg/session/session.go`, `pkg/sessionctx/`):

- **Session**: Per-connection state with unique ID
- **SessionVars**: Session configuration (current_db, isolation_level, autocommit)
- **QueryCtx**: Per-statement context created via `Session.new_query_ctx()`

```rust
// Session lifetime = MySQL connection lifetime
let session = Session::new();

// QueryCtx lifetime = single statement
let ctx = session.new_query_ctx();
db.handle_mp_query_with_ctx(sql, &ctx)?;
```

### 2. Transaction Service (TxnService trait)

All SQL execution goes through TxnService:
- **Read statements**: `txn_service.snapshot()` → allocates start_ts
- **Write statements**: `txn_service.begin()` → buffer writes → `commit()`

```rust
pub trait TxnService: Send + Sync {
    fn begin(&self) -> Result<Box<dyn Txn>>;
    fn snapshot(&self) -> Result<Box<dyn ReadSnapshot>>;
    fn current_ts(&self) -> Timestamp;
}
```

### 3. ConcurrencyManager (TSO + Lock Table)

Provides:
- **TSO**: Monotonic timestamp allocation (`get_ts()`)
- **In-memory lock table**: Blocks readers during 1PC writes
- **max_ts tracking**: For MVCC read visibility

### 4. MVCC Storage (MvccMemTableEngine)

- **Key encoding**: `user_key || !commit_ts` (descending order)
- **Data structure**: crossbeam-skiplist (lock-free)
- **Read path**: Check locks → scan for version ≤ start_ts
- **Write path**: Acquire locks → write to clog → apply with commit_ts

### 5. Commit Log (FileClogService)

- **Purpose**: Write-ahead logging for durability
- **File format**: Magic("CLOG") + Version + Records
- **Recovery**: Full replay on startup, two-pass (identify committed, then apply)

---

## SQL Execution Flow

```
1. handle_mp_query(sql)
   └── Create QueryCtx (inherits from Session)

2. Parser.parse_one(sql) → AST

3. Binder.bind(ast, catalog) → LogicalPlan

4. Executor.execute(plan, txn_service, catalog)
   ├── Read operations:
   │   └── txn_service.snapshot() → ReadSnapshot (start_ts allocated)
   │       └── snapshot.get(key) / snapshot.scan(range)
   │
   └── Write operations:
       └── txn_service.begin() → Txn (start_ts allocated)
           ├── txn.put(key, value) / txn.delete(key)
           └── txn.commit()
               ├── Acquire in-memory locks
               ├── Write to clog (sync)
               ├── Apply to storage with commit_ts
               └── Release locks

5. Return ExecutionResult → QueryResult
```

---

## Testing

```bash
# All tests (~160 tests)
cargo test

# With failpoints (concurrency testing)
cargo test --features failpoints

# Library tests only
cargo test --lib

# Concurrency tests
cargo test --test concurrency_test
```

---

## Running

```bash
# Start server with persistence
cargo run -- --data-dir ./data --port 4000

# With options
cargo run -- -D ./mydata -P 3306 --worker-min-threads 4

# Connect with MySQL client
mysql -h 127.0.0.1 -P 4000 -uroot
```

---

## Current Progress

### Completed
- [x] SQL parsing (sqlparser)
- [x] Semantic binding
- [x] Volcano-style executor
- [x] MySQL protocol (opensrv-mysql)
- [x] Worker thread pool (yatp)
- [x] Commit log (clog) for durability
- [x] TiDB-compatible key/value codec
- [x] GitHub CI workflow
- [x] **MVCC with TSO and ConcurrencyManager**
  - MvccMemTableEngine with crossbeam-skiplist
  - TSO (local timestamp oracle)
  - In-memory lock table for 1PC atomicity
  - MVCC key encoding (key || !commit_ts)
- [x] **TxnService trait abstraction**
  - All SQL goes through TxnService
  - Read-only statements allocate start_ts
  - Write statements: begin → buffer → commit
- [x] **Session and QueryCtx (TiDB-style)**
  - Session per connection
  - QueryCtx per statement
- [x] **Fail-rs integration for concurrency testing**
  - Fail points in transaction commit path
  - Tests for TSO ordering, lock conflicts

### TODO
- [ ] Persist catalog metadata (tables don't survive restart)
- [ ] True write-ahead logging (currently log-after-write for some paths)
- [ ] Proper transaction support (BEGIN/COMMIT/ROLLBACK)
- [ ] Index support
- [ ] Aggregations (COUNT, SUM, etc.)
- [ ] JOIN operations
- [ ] Subqueries
- [ ] Log rotation and compaction
- [ ] Integrate Session into MySQL protocol handler

---

## Design Notes: MVCC and Concurrent MemTable

### MVCC Key Encoding

TiDB-style MVCC key encoding with descending timestamp:
```
user_key || !commit_ts  (bitwise NOT for descending order)
```
This allows scanning from a key to find the latest visible version first.

### 1PC Transaction Atomicity

**Problem**: For 1PC (one-phase commit), how to prevent readers from seeing partial writes?

**Solution**: In-memory lock table via `ConcurrencyManager`

```
┌─────────────────────────────────────────────────────────────────┐
│  1PC Write Flow:                                                 │
│    ① lock_keys() → acquire in-memory locks                     │
│       → Locks IMMEDIATELY visible to readers                    │
│    ② Write to clog (sync for durability)                       │
│    ③ Apply to storage with commit_ts                           │
│    ④ Drop guards → remove locks from memory                    │
│                                                                 │
│  Concurrent Reader:                                             │
│    ① check_lock(key, start_ts) → check in-memory lock table    │
│    ② Lock found with ts > start_ts? → BLOCKED (KeyIsLocked)    │
│    ③ No lock? → proceed to read at start_ts                    │
└─────────────────────────────────────────────────────────────────┘
```

### TiKV Reference Code Locations

- **ConcurrencyManager**: `components/concurrency_manager/src/lib.rs`
- **In-memory lock storage**: `src/storage/txn/actions/prewrite.rs:683-722`
- **Reader lock check**: `src/storage/mod.rs:1183-1192`

---

## Dependencies

| Crate | Purpose |
|-------|---------|
| sqlparser | SQL parsing |
| tokio | Async runtime |
| opensrv-mysql | MySQL protocol |
| yatp | Thread pool (TiKV) |
| crossbeam-skiplist | Lock-free MVCC memtable |
| bincode | Serialization |
| serde | Serialization derive |
| tracing | Logging |
| clap | CLI parsing |
| thiserror | Error derive |
| fail | Fail point injection (dev) |

---

## File Naming Conventions

- `mod.rs` - Module definition and re-exports
- `*.rs` - Implementation files
- Naming follows Rust conventions (snake_case)
- "clog" = commit log (not "log" to avoid confusion with tracing)
