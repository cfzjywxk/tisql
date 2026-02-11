# TiSQL

A minimal SQL database in Rust with MySQL protocol support, designed for learning database internals.

[![CI](https://github.com/cfzjywxk/tisql/actions/workflows/ci.yml/badge.svg)](https://github.com/cfzjywxk/tisql/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## Features

- **MySQL Protocol**: Connect using any MySQL client
- **SQL Support**: CREATE, INSERT, SELECT, UPDATE, DELETE with explicit transactions (BEGIN/COMMIT/ROLLBACK)
- **MVCC**: Snapshot isolation with TiDB-compatible key encoding
- **Streaming Execution**: Volcano-style operator tree with end-to-end row streaming (zero materialization)
- **Async I/O**: io_uring for SST reads (O_DIRECT, positional reads), async iterator pipeline
- **Durability**: LSM-tree storage with WAL (commit log), group commit for batched fsync
- **Concurrency**: OceanBase-style pessimistic locking with pending version nodes
- **Recovery**: Crash-safe with coordinated clog/ilog recovery

## Quick Start

```bash
# Start the server
cargo run

# Connect using mysql client (in another terminal)
mysql -h127.0.0.1 -P4000 -uroot test

# Example SQL
CREATE TABLE users (id INT, name VARCHAR(100));
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM users;
UPDATE users SET name = 'Charlie' WHERE id = 1;
DELETE FROM users WHERE id = 2;
```

## Server Options

```bash
cargo run -- --help

# Common options:
#   -H, --host <HOST>       Host to bind (default: 127.0.0.1)
#   -P, --port <PORT>       Port to listen (default: 4000)
#   -D, --data-dir <DIR>    Data directory (default: data)
#   -L, --log-level <LEVEL> Log level: trace/debug/info/warn/error (default: info)
```

## Building

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Format and lint
cargo fmt && cargo clippy
```

## Testing

```bash
# Run all tests
cargo test --lib                    # Unit tests
cargo test --test store_test        # Integration tests
cargo test --test testkit           # Test utilities

# Run E2E tests (MySQL-test style)
cargo run --bin mysqltest-runner -- --all

# Run with failpoints (for concurrency testing)
cargo test --test concurrency_test --features failpoints
```

## Architecture

```
                         MySQL Client
                              |
                    +---------+---------+
                    |  Protocol Layer   |  opensrv-mysql (thin I/O shell)
                    +---------+---------+
                              |  mpsc batched row streaming
                    +---------+---------+
                    |  Worker Runtime   |  Dedicated tokio runtime
                    +---------+---------+
                              |
                    +---------+---------+
                    |    SQL Layer      |  Parser -> Binder -> Executor
                    +---------+---------+
                              |
                    +---------+---------+
                    | Transaction Layer |  TxnService + ConcurrencyManager
                    +---------+---------+
                              |
                    +---------+---------+
                    |   Storage Layer   |  LSM Engine + Clog + Ilog
                    +-------------------+
```

## Project Structure

```
src/
├── lib.rs           # Public API: Database, traits
├── catalog/         # Table metadata (MVCC-based)
├── codec/           # TiDB-compatible key encoding
├── clog/            # Commit log (WAL) with group commit
├── executor/        # Volcano-style operator tree (streaming)
├── io/              # io_uring async I/O (O_DIRECT, AlignedBuf, DmaFile)
├── protocol/        # MySQL wire protocol (thin I/O shell)
├── session/         # Per-connection state
├── sql/             # Parser, Binder
├── storage/         # LSM engine, memtables, SSTables
├── transaction/     # TxnService, ConcurrencyManager
├── tso/             # Timestamp oracle
└── worker/          # Dedicated worker runtime dispatch
```

## Design Highlights

- **Physical thread separation**: Protocol I/O on main runtime, all DB work on dedicated worker runtime (OceanBase-inspired)
- **Trait-based layering**: Each layer depends on traits, not implementations
- **TiDB/TiKV patterns**: MVCC key encoding, ConcurrencyManager, TSO
- **OceanBase patterns**: Unified TxnService with TxnCtx, pessimistic locking
- **Batched row streaming**: Worker pulls rows from operator tree, sends batches (1024) via bounded mpsc(4) to protocol
- **Async iterators**: All iterator seek/advance are async fn (native AFIT), yielding during SST I/O

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
