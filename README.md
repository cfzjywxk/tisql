# TiSQL

A minimal SQL database in Rust with MySQL protocol support, designed for learning database internals.

[![CI](https://github.com/cfzjywxk/tisql/actions/workflows/ci.yml/badge.svg)](https://github.com/cfzjywxk/tisql/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## Features

- **MySQL protocol server** (`opensrv-mysql`), connect with standard MySQL clients
- **SQL support**: `CREATE TABLE`, `DROP TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `USE`, `SHOW`, `BEGIN/COMMIT/ROLLBACK`
- **Explicit transactions** with snapshot reads, read-your-writes, and pessimistic lock conflict detection
- **Streaming execution**: volcano-style operator tree, worker-to-protocol batched row streaming
- **Storage engine**: LSM tree (`VersionedMemTableEngine` + SSTables) with background flush and compaction
- **Durability & recovery**: commit log (WAL) + ilog with group commit and unified LSN ordering
- **Async SST I/O**: Linux uses `io_uring` (`O_DIRECT`, positional I/O); non-Linux dev builds use a sync fallback backend
- **Persistent catalog**: MVCC inner-table metadata with bootstrap and schema-version checks
- **Drop-table GC**: background worker with read-path SST skip and proactive dropped-SST cleanup
- **Runtime separation**: protocol/runtime work split across dedicated worker, background, and I/O runtimes

## Quick Start

```bash
# Start server
cargo run

# Connect from another terminal
mysql -h127.0.0.1 -P4000 -uroot test
```

```sql
-- Built-in schemas: default, test
SHOW DATABASES;

CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(100),
  age INT
);

INSERT INTO users VALUES (1, 'Alice', 25), (2, 'Bob', 30);

BEGIN;
UPDATE users SET age = age + 1 WHERE id = 1;
COMMIT;

SELECT id, name, age FROM users ORDER BY id;
DROP TABLE users;
```

## SQL Coverage (Current)

- **DDL**: `CREATE TABLE [IF NOT EXISTS]`, `DROP TABLE [IF EXISTS]`
- **DML**: `INSERT`, `UPDATE`, `DELETE`
- **Query**: `SELECT` with expressions, `WHERE`, `ORDER BY`, `LIMIT/OFFSET`, aggregate functions (`COUNT/SUM/AVG/MIN/MAX`)
- **Session/Txn**: `USE`, `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK`
- **SHOW commands**: `SHOW DATABASES`, `SHOW TABLES`, `SHOW WARNINGS`, `SHOW STATUS`

Notes:
- Tables currently require a `PRIMARY KEY`.
- `JOIN` and `GROUP BY` execution are not implemented yet.
- Prepared statement protocol hooks exist, but execution is currently a stub.

## Server Options

```bash
cargo run -- --help

# -H, --host <HOST>            Host address to bind to (default: 127.0.0.1)
# -P, --port <PORT>            Port to listen on (default: 4000)
# -D, --data-dir <DATA_DIR>    Data directory for persistence (default: data)
# -L, --log-level <LOG_LEVEL>  Log level: trace/debug/info/warn/error (default: info)
```

## Build & Test

```bash
# Build
cargo build
cargo build --release

# Format + lint
cargo fmt
cargo clippy --all-targets -- -D warnings

# Core tests
cargo test --lib
cargo test --test store_test
cargo test --test mysql_protocol_test

# E2E (mysql-test style)
cargo run --bin mysqltest-runner -- --all

# Failpoint tests
cargo test --test concurrency_test --features failpoints

# Full local CI gate
make ci
```

macOS dev note:
- TiSQL supports macOS for development (with a non-Linux sync I/O fallback), but production target is Linux.
- `make store-test` and `make prepare` automatically run store tests single-threaded on macOS to avoid file-descriptor/runtime churn issues.
- If you run store tests directly on macOS, prefer:

```bash
cargo test --test store_test -- --test-threads=1
```

## Architecture

```text
MySQL Client
    |
    v
Protocol Layer (main tokio runtime)
  - MySQL wire I/O
  - session fast path (SET / USE / @@vars)
    |
    | mpsc row batches (1024 rows, cap=4)
    v
Worker Runtime (tisql-worker)
  - parse + bind + execute + txn control + SHOW
    |
    v
SQL Layer (Parser -> Binder -> Executor)
    |
    v
Transaction Layer (TxnService + ConcurrencyManager + TSO)
    |
    v
Storage Layer (LsmEngine + Clog + Ilog + LsnProvider)

Side runtimes/services:
- Background runtime (tisql-bg): flush scheduler, compaction scheduler, drop-table GC worker
- I/O runtime (tisql-io): group-commit blocking tasks used by clog/ilog
- SST I/O service thread: Linux `io_uring`; non-Linux sync fallback (dev only)
```

## Project Structure

```text
src/
├── lib.rs           # Database entry point and wiring
├── main.rs          # CLI server binary
├── catalog/         # MVCC catalog (inner-table-backed)
│   └── types.rs     # SQL data/value/type definitions
├── inner_table/     # Bootstrap, core system tables, GC worker
├── sql/             # Parser, binder, logical plan
├── executor/        # Volcano-style executor
├── transaction/     # TxnService, concurrency manager, txn state
├── tablet/          # LSM engine, memtable, SST, recovery
├── log/             # Durability logs
│   ├── clog/        # WAL with group commit
│   ├── ilog.rs      # Manifest/flush/compaction metadata log
│   └── lsn.rs       # Shared LSN allocator for clog/ilog
├── protocol/        # MySQL wire protocol handler
├── worker/          # Worker dispatch + row batch streaming
├── session/         # Per-connection session state
├── tso/             # Local timestamp oracle
├── util/            # Shared utilities
│   ├── error.rs     # Shared error definitions
│   ├── codec/       # TiDB-compatible key/row codec
│   ├── io/          # IoFuture/IoService and DMA file abstractions
│   └── ...          # Logging, fs utils, timing, arena, etc.
```

## Extra Tools

```bash
# Sysbench-like memtable benchmark
cargo run --release --bin sysbench-sim -- --threads 8 --time 60
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
