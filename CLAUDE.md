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
│  │  Parser → Binder → Executor → Storage                     │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Durability Layer                               │
│  ┌─────────────────────┐    ┌─────────────────────────────┐     │
│  │   ClogService       │    │   TransactionService        │     │
│  │   (Commit Log)      │    │   (Autocommit semantics)    │     │
│  └─────────────────────┘    └─────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Module Structure

```
src/
├── main.rs              # CLI entry point
├── lib.rs               # Database facade, QueryResult
├── error.rs             # TiSqlError, Result type
├── types.rs             # Value, Key, Timestamp, TxnId, Lsn
│
├── clog/                # Commit Log (WAL) - named after OceanBase convention
│   ├── mod.rs           # ClogEntry, ClogOp, ClogBatch, ClogService trait
│   └── file.rs          # FileClogService - file-based implementation
│
├── transaction/         # Transaction management
│   ├── mod.rs           # Module exports
│   └── service.rs       # TransactionService with autocommit, recovery
│
├── storage/             # Storage engine
│   ├── mod.rs           # StorageEngine trait, WriteBatch
│   └── memtable.rs      # MemTableEngine (BTreeMap-based)
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
│   └── simple.rs        # SimpleExecutor (volcano-style)
│
├── protocol/            # Network protocol
│   ├── mod.rs           # MySqlServer
│   └── mysql.rs         # MySqlBackend (opensrv-mysql handler)
│
├── worker/              # Thread pool
│   └── mod.rs           # WorkerPool (yatp wrapper)
│
├── codec/               # TiDB-compatible encoding
│   ├── mod.rs           # Module exports
│   ├── key.rs           # Key encoding (table/row/index keys)
│   ├── row.rs           # Row encoding
│   ├── number.rs        # Number encoding (comparable format)
│   └── bytes.rs         # Bytes encoding (escape format)
│
├── session/             # Session management
│   └── mod.rs           # SessionVars
│
└── util/                # Utilities
    ├── mod.rs           # Module exports
    ├── log.rs           # Logging macros (log_info!, log_trace!, etc.)
    └── timing.rs        # Timer for performance measurement
```

---

## Key Components

### 1. Commit Log (clog)
- **Purpose**: Write-ahead logging for durability
- **Named**: "clog" following OceanBase convention (avoids confusion with app logging)
- **File format**: Magic("CLOG") + Version + Records
- **Record format**: Type(4) + Length(4) + CRC32(4) + Data
- **Operations**: Put, Delete, Commit, Rollback
- **Recovery**: Full replay on startup

### 2. Transaction Service
- **Semantics**: Autocommit (each statement is a transaction)
- **Durability**: Log → Sync → Apply → Return
- **Recovery**: Two-pass (identify committed txns, then replay)
- **Counters**: TxnId and Timestamp (monotonic)

### 3. Storage Engine
- **Current**: MemTableEngine (in-memory BTreeMap)
- **Interface**: StorageEngine trait (get, put, delete, scan, write_batch)
- **Key encoding**: TiDB-compatible format for future compatibility

### 4. Worker Pool
- **Purpose**: Separate network I/O from database work
- **Implementation**: yatp FuturePool
- **Communication**: tokio::sync::oneshot channels

### 5. MySQL Protocol
- **Library**: opensrv-mysql
- **Features**: Basic query, COM_QUERY, result sets
- **Fast paths**: SET, SHOW, @@ variables handled in network thread

---

## Supported SQL

### DDL
- `CREATE TABLE name (col type, ...)`
- `DROP TABLE name`

### DML
- `INSERT INTO table VALUES (...)`
- `INSERT INTO table (cols) VALUES (...)`
- `UPDATE table SET col=val WHERE ...`
- `DELETE FROM table WHERE ...`

### Queries
- `SELECT cols FROM table`
- `SELECT * FROM table WHERE condition`
- `SELECT ... ORDER BY col [ASC|DESC]`
- `SELECT ... LIMIT n`
- Expressions: arithmetic, comparison, AND/OR/NOT
- Literals: integers, strings, NULL

### MySQL Compatibility
- `SHOW TABLES`
- `SHOW DATABASES`
- `SET variable = value`
- `SELECT @@variable`

---

## Testing

```bash
# All tests
cargo test

# Library tests only (69 tests)
cargo test --lib

# Storage tests (24 tests)
cargo test -p tisql --test store_test

# Integration tests (3 tests)
cargo test -p testkit
```

---

## Running

```bash
# Start server (default port 4000)
cargo run

# With options
cargo run -- --port 3306 --worker-min-threads 4 --worker-max-threads 8

# Connect with MySQL client
mysql -h 127.0.0.1 -P 4000
```

---

## Current Progress

### Completed
- [x] SQL parsing (sqlparser)
- [x] Semantic binding
- [x] Volcano-style executor
- [x] In-memory storage (MemTableEngine)
- [x] Memory catalog
- [x] MySQL protocol (opensrv-mysql)
- [x] Worker thread pool (yatp)
- [x] Commit log (clog) for durability
- [x] Transaction service with autocommit
- [x] Recovery from commit log
- [x] TiDB-compatible key/value codec
- [x] GitHub CI workflow

### TODO
- [ ] Persist catalog metadata (tables don't survive restart)
- [ ] True write-ahead logging (currently log-after-write)
- [ ] Proper transaction support (BEGIN/COMMIT/ROLLBACK)
- [ ] Multi-version concurrency control (MVCC)
- [ ] Index support
- [ ] Aggregations (COUNT, SUM, etc.)
- [ ] JOIN operations
- [ ] Subqueries
- [ ] Log rotation and compaction

---

## Dependencies

| Crate | Purpose |
|-------|---------|
| sqlparser | SQL parsing |
| tokio | Async runtime |
| opensrv-mysql | MySQL protocol |
| yatp | Thread pool (TiKV) |
| bincode | Serialization |
| serde | Serialization derive |
| tracing | Logging |
| clap | CLI parsing |
| thiserror | Error derive |

---

## File Naming Conventions

- `mod.rs` - Module definition and re-exports
- `*.rs` - Implementation files
- Naming follows Rust conventions (snake_case)
- "clog" = commit log (not "log" to avoid confusion with tracing)
