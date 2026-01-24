# TiSQL Development Context

This file helps restore context for continuing development across sessions.

**Project Structure:**
```
tisql/
├── doc/                 # Documentation
│   ├── ARCHITECTURE.md  # Design and architecture
│   └── CLAUDE.md        # Development context (this file)
├── src/                 # Source code
│   ├── main.rs          # Entry point
│   ├── lib.rs           # Library root
│   ├── protocol/        # MySQL protocol
│   ├── sql/             # SQL parsing, binding, planning
│   ├── executor/        # Query execution
│   ├── storage/         # Storage engine
│   ├── catalog/         # Schema management
│   └── util/            # Utilities (logging, timing)
├── Cargo.toml
└── Cargo.lock
```

---

## Progress Log

### Session 2026-02-08: Async Logging, Simplify Entry Point & Restructure
**Goal**: Add non-blocking logging utilities; simplify to server-only mode; reorganize project

**Completed**:
- Created `src/util/` module with logging and timing utilities
- Implemented channel-based async logger (`src/util/log.rs`)
  - Background task handles I/O, never blocks hot path
  - Format: `[time] [level] [file:line] message`
  - Levels: trace, debug, info, warn, error
- Added timing utilities (`src/util/timing.rs`)
  - `Timer`, `TimerGuard`, `time_block!` macro
- Integrated SQL execution timing (parse/bind/exec breakdown)
- Added `-L/--log-level` CLI option
- Replaced tracing with custom logging in protocol layer
- **Removed REPL mode** - `cargo run` now starts MySQL server directly on port 4000
- **Reorganized project structure**: created `doc/` folder for documentation
- All 26 tests passing

**Files Changed**:
- New: `src/util/mod.rs`, `src/util/log.rs`, `src/util/timing.rs`, `doc/`
- Moved: `CLAUDE.md` → `doc/CLAUDE.md`, `ARCHITECTURE.md` → `doc/ARCHITECTURE.md`
- Modified: `src/lib.rs`, `src/main.rs`, `src/protocol/mod.rs`, `src/protocol/mysql.rs`, `Cargo.toml`

---

### Session 2026-01-24: MySQL Protocol Implementation (M3)
**Goal**: Implement MySQL wire protocol for client connectivity

**Completed**:
- Added `opensrv-mysql` for async MySQL protocol
- Created `MySqlBackend` implementing `AsyncMysqlShim`
- Server mode with `--server` flag, port 4000
- Handles: queries, SHOW commands, system variables (@@version, etc.)
- Successfully tested with `mysql` CLI client
- All 21 tests passing

**Files Changed**:
- New: `src/protocol/mysql.rs`
- Modified: `src/protocol/mod.rs`, `src/main.rs`, `src/lib.rs`, `Cargo.toml`

---

### Earlier Sessions: Foundation (M0-M2)
- M0: Basic SQL parsing and expression evaluation (SELECT 1+1)
- M1: In-memory storage with MemTableEngine, CRUD operations
- M2: Transaction structure (ready for MVCC implementation)
- SQL: Parser, Binder, Executor for SELECT/INSERT/UPDATE/DELETE/DDL
- Catalog: Schema and table metadata management

---

## Project Overview

**TiSQL** is a minimal SQL database written in Rust, inspired by TiDB architecture. The goal is to build a functional database from scratch that can:
- Accept MySQL protocol connections
- Execute SQL queries (SELECT, INSERT, UPDATE, DELETE, DDL)
- Support transactions with MVCC
- Persist data with WAL

**Reference Materials:**
- Architecture design: `doc/ARCHITECTURE.md` (same folder)
- TiDB RFC: https://github.com/TiDBHackers/rfc/blob/master/text/one_node_fits_all_tisql.md
- OceanBase reference: `~/src/oceanbase`

## Milestone Progress

| Milestone | Description | Status |
|-----------|-------------|--------|
| M0 | Hello Query (SELECT 1+1) | ✅ Complete |
| M1 | MemTable CRUD | ✅ Complete |
| M2 | Basic Transactions | ✅ Complete (structure ready) |
| **M3** | **MySQL Protocol** | ✅ **Complete** |
| M4 | Persistence (WAL) | 🔲 Not Started |
| M5 | Sysbench | 🔲 Not Started |

## Current State (as of 2026-01-24, updated 2026-02-08)

### What's Working
- **MySQL Protocol Server**: Connect via `mysql -h127.0.0.1 -P4000 -uroot test`
- **SQL Execution**: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE
- **Expressions**: Arithmetic, comparisons, LIKE, IS NULL, BETWEEN, IN, CASE
- **Clauses**: WHERE, ORDER BY, LIMIT, basic aggregates (COUNT, SUM, AVG, MIN, MAX)
- **In-Memory Storage**: BTreeMap-based with snapshot isolation support
- **Catalog**: Schema and table metadata management
- **Async Logging**: Non-blocking logger with timing info (see Logging section below)

### How to Run

```bash
cd tisql/    # Project root

# Run tests
cargo test

# Start MySQL server (default: 127.0.0.1:4000)
cargo run

# Start with custom host/port
cargo run -- -H 0.0.0.0 -P 3306

# Start with debug logging
cargo run -- -L debug

# Connect with MySQL client
mysql -h127.0.0.1 -P4000 -uroot test
```

### Key Commands Tested
```sql
SELECT 1 + 1;
CREATE TABLE users (id INT, name VARCHAR(255));
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM users WHERE id > 1;
SELECT * FROM users ORDER BY name DESC LIMIT 10;
UPDATE users SET name = 'Charlie' WHERE id = 1;
DELETE FROM users WHERE id = 2;
SHOW TABLES;
SHOW DATABASES;
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Protocol Layer                            │
│              (MySQL Wire Protocol - opensrv-mysql)          │
│              src/protocol/mysql.rs, mod.rs                  │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    Session Layer                            │
│           (Connection, Auth, Session State)                 │
│              src/session/mod.rs                             │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                     SQL Layer                               │
│         Parser → Binder → Optimizer → Executor              │
│    src/sql/parser.rs, binder.rs, plan.rs                    │
│    src/executor/simple.rs                                   │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                  Catalog Layer                              │
│            (Schema, Table, Index Metadata)                  │
│              src/catalog/memory.rs                          │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                Transaction Layer                            │
│              (MVCC, Concurrency Control)                    │
│              src/transaction/mod.rs (stub)                  │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                  Storage Layer                              │
│         (MemTable with BTreeMap + Encoding)                 │
│    src/storage/memtable.rs, encoding.rs                     │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                    Log Layer                                │
│              (WAL - Not implemented yet)                    │
│              src/log/mod.rs (stub)                          │
└─────────────────────────────────────────────────────────────┘
```

## Key Files

| File | Purpose |
|------|---------|
| `src/main.rs` | CLI entry point (REPL or server mode) |
| `src/lib.rs` | Database struct, QueryResult, public API |
| `src/protocol/mysql.rs` | MySQL protocol backend (AsyncMysqlShim) |
| `src/protocol/mod.rs` | MySqlServer TCP listener |
| `src/sql/parser.rs` | SQL parsing (wraps sqlparser-rs) |
| `src/sql/binder.rs` | Name resolution, type checking |
| `src/sql/plan.rs` | LogicalPlan, Expr definitions |
| `src/executor/simple.rs` | Query execution engine |
| `src/storage/memtable.rs` | In-memory KV storage |
| `src/storage/encoding.rs` | Key/value encoding for ordered storage |
| `src/catalog/memory.rs` | In-memory schema catalog |
| `src/types.rs` | DataType, Value, Row, Schema |
| `src/error.rs` | Error types |
| `src/util/log.rs` | Async logging infrastructure |
| `src/util/timing.rs` | Timing utilities for performance measurement |

## Logging Infrastructure

### Overview
TiSQL uses a custom async logging system (`src/util/log.rs`) that:
- Sends log messages through a channel to a background task
- Never blocks the SQL execution hot path
- Includes timing breakdown for query execution

### Log Format
```
[timestamp] [level] [file:line] message
[2026-02-08 07:56:05.697] [TRACE] [src/lib.rs:71] SQL: SELECT * FROM t | parse=0.037ms bind=0.010ms exec=0.009ms total=0.059ms rows=2
```

### Log Levels
- `trace` - Detailed query timing, execution plans
- `debug` - Query text, connection events
- `info` - Server start, new connections
- `warn` - Performance warnings
- `error` - Errors

### Usage in Code
```rust
use tisql::{log_info, log_debug, log_trace, log_error, log_warn};

log_info!("Server started on port {}", port);
log_debug!("Executing query: {}", sql);
log_trace!("Parse time: {:.3}ms", parse_ms);
```

### Command Line Options
```bash
# Set log level via CLI
tisql --server -L debug

# Set via environment variable
TISQL_LOG=trace tisql --server
```

### Timing Utilities
```rust
use tisql::util::{Timer, TimerGuard};

// Manual timer
let timer = Timer::new("operation");
// ... do work ...
timer.stop_and_log();  // Logs: "operation completed in X.XXXms"

// Auto-logging guard (logs on drop)
let _guard = TimerGuard::new("query");
// ... do work ...
// Automatically logs when guard goes out of scope
```

## Dependencies

Key crates used:
- `sqlparser = "0.40"` - SQL parsing
- `opensrv-mysql = "0.7"` - MySQL protocol server
- `tokio = "1"` - Async runtime
- `serde`, `bincode` - Serialization
- `clap = "4"` - CLI argument parsing

## Next Steps

### Immediate (M3 Improvements)
1. **Worker Thread Separation**: Currently queries run in the connection thread. Split into:
   - IO threads for connection handling
   - Worker threads for query execution
2. **Prepared Statement Support**: Full COM_STMT_PREPARE/EXECUTE
3. **Better Error Messages**: MySQL error codes and states

### M4: Persistence
1. Implement WAL (Write-Ahead Log) in `src/log/`
2. Recovery on startup
3. Periodic checkpointing
4. Reference: `../ARCHITECTURE.md` section on WAL

### M5: Sysbench
1. Auto-increment columns (partially done)
2. Secondary index support
3. Prepared statements with parameters
4. Performance optimization

## Known Limitations

- No persistence (data lost on restart)
- No real transactions (no BEGIN/COMMIT/ROLLBACK)
- No secondary indexes
- No JOIN support
- Single-threaded query execution
- Prepared statements return empty results

## Testing

```bash
# Unit tests
cargo test

# Manual MySQL client test
cargo run -- --server &
mysql -h127.0.0.1 -P4000 -uroot test -e "SELECT 1+1"
mysql -h127.0.0.1 -P4000 -uroot test -e "CREATE TABLE t(a INT); INSERT INTO t VALUES (1),(2),(3); SELECT * FROM t;"
```

## Git Status

The project is in `/home/ywxk/src/requirements/build_my_own/tisql/` with uncommitted changes. Consider committing after significant milestones.

---

## Session Workflow Reminder

**At the start of each session:**
1. Read this file: `CLAUDE.md`
2. Check current state: `cargo test`
3. Review recent changes: `git status`, `git diff`

**At the end of each session:**
1. Run tests: `cargo test`
2. Update this file's Progress Log with:
   - Date and goal
   - What was completed
   - Files changed
   - Test status
3. Consider committing: `git add -A && git commit -m "..."`

**Quick Commands:**
```bash
# Build and test
cargo build && cargo test

# Start server (default port 4000)
cargo run

# Start server with debug logging
cargo run -- -L debug

# Connect with MySQL client
mysql -h127.0.0.1 -P4000 -uroot test
```
