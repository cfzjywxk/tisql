# TiSQL

A MySQL-compatible SQL database in Rust focused on correctness-first transaction semantics and practical storage-engine design.

[![CI](https://github.com/cfzjywxk/tisql/actions/workflows/ci.yml/badge.svg)](https://github.com/cfzjywxk/tisql/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## Key Designs and Highlights

- **MySQL wire compatibility**: connect with standard MySQL clients/tools over the MySQL protocol (`opensrv-mysql`).
- **Layered architecture**: parser/binder/executor + transaction manager + LSM storage engine (`VersionedMemTableEngine` + SST).
- **Durability model**: WAL-style commit log (`clog`) plus manifest/metadata log (`ilog`) with shared unified LSN ordering.
- **Configurable group commit**: supports `full` (`fsync`) and `data` (`fdatasync`) sync modes, delay window, and no-delay-count threshold.
- **Correctness-first transactions**: snapshot reads, read-your-writes, pessimistic lock conflict detection, and explicit txn lifecycle.
- **Runtime isolation**: dedicated protocol, worker, background, and I/O runtimes to reduce contention.
- **I/O strategy**: Linux `io_uring` for async SST I/O (`O_DIRECT` path), with non-Linux sync fallback for development.
- **Operational safety**: persistent MVCC catalog, recovery path for clog/ilog, and drop-table GC with read-path SST skipping.

## Current Capabilities

- **SQL support**: `CREATE TABLE`, `DROP TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `USE`, `SHOW`, `BEGIN/COMMIT/ROLLBACK`
- **Streaming execution**: volcano-style operator tree with worker-to-protocol batched row streaming
- **Storage maintenance**: background flush, compaction, and dropped-SST cleanup
- **Observability**: `SHOW STATUS` and periodic engine status reporting

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
# --clog-sync-mode <MODE>      Clog sync mode: full|data (default: full)
# --group-commit-delay-us <US> Group commit delay window in microseconds (default: 0)
# --group-commit-no-delay-count <N>
#                              Skip delay once batch size reaches N (default: 16)
# --protocol-threads <N>       Protocol runtime threads (optional override)
# --worker-threads <N>         Worker runtime threads (optional override)
# --bg-threads <N>             Background runtime threads (optional override)
# --io-threads <N>             I/O runtime threads (optional override)
# --flush-threads <N>          Per-tablet flush threads (optional override)
```

## Performance Snapshot (COM_QUERY Write-Only)

Benchmark profile: go-ycsb insert-only, `recordcount=100000`, `operationcount=300000`, `threadcount=16`, COM_QUERY path.

- **Strict durability profile**:
  - TiSQL: `--clog-sync-mode full`
  - MySQL 8.0: `innodb_flush_log_at_trx_commit=1`, `sync_binlog=1`, `log_bin=ON`, `innodb_doublewrite=ON`
  - Observed range (tuned runs on this host): TiSQL `~12.3k-13.5k OPS`, MySQL `~7.4k-7.8k OPS`
- **InnoDB-only MySQL profile** (binlog write/sync disabled):
  - MySQL 8.0: `innodb_flush_log_at_trx_commit=1`, `skip-log-bin`, `sync_binlog=0`, `innodb_doublewrite=ON`
  - Observed run: MySQL `~13.5k OPS`

Takeaway: TiSQL shows comparable write throughput to MySQL 8.0 in this workload class, and remains competitive under strict-durability settings.

## Build & Test

```bash
# Build
cargo build
cargo build --release

# Full local quality gate (fmt + clippy + tests)
make prepare

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
