# TiSQL

A minimal SQL database in Rust with MySQL protocol support.

[![CI](https://github.com/cfzjywxk/tisql/actions/workflows/ci.yml/badge.svg)](https://github.com/cfzjywxk/tisql/actions/workflows/ci.yml)

## Features

- MySQL wire protocol compatibility
- Basic SQL support (CREATE, INSERT, SELECT, UPDATE, DELETE)
- In-memory storage engine
- TiDB-compatible key encoding

## Quick Start

```bash
# Start the server
cargo run

# Connect using mysql client
mysql -h127.0.0.1 -P4000 -uroot test

# Example SQL
CREATE TABLE users (id INT, name VARCHAR(100));
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM users;
```

## Building

```bash
# Debug build
cargo build

# Release build
cargo build --release
```

## Testing

```bash
# Run all tests
make test

# Or individually:
cargo test --lib          # Unit tests
cargo test --test store_test   # Integration tests
cargo run --bin mysqltest-runner -- --all  # E2E tests
```

See [TESTING.md](TESTING.md) for detailed testing documentation.

## Project Structure

```
src/
├── catalog/     # Table metadata management
├── codec/       # TiDB-compatible key/value encoding
├── executor/    # Query execution engine
├── protocol/    # MySQL wire protocol
├── sql/         # SQL parsing and binding
├── storage/     # Storage engine
├── transaction/ # Transaction management
├── types/       # Data types and values
└── worker/      # Thread pool for database work

tests/
├── integrationtest/  # E2E MySQL-test style tests
├── store_test.rs     # Integration tests
└── testkit.rs        # Test utilities
```

## License

MIT
