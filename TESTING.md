# TiSQL Testing Guide

This document describes how to run and write tests for TiSQL.

## Quick Start

```bash
# Run all tests (unit + integration)
cargo test

# Run E2E integration tests
cargo run --bin mysqltest-runner -- --all

# Run storage regression suites (ported RocksDB-style tests)
make storage-test

# Run all tests including E2E
make test
```

## Test Categories

TiSQL has four categories of tests:

| Category | Location | Description |
|----------|----------|-------------|
| Unit Tests | `src/**/*.rs` | Module-level tests using `#[cfg(test)]` |
| Store Tests | `tests/store_test.rs` | Integration tests using TestKit API |
| Storage Regression Tests | `tests/rocksdb_ported*.rs` | Flush/compaction/read-path regression suites |
| E2E Tests | `tests/integrationtest/` | MySQL-test style end-to-end tests |

## Running Tests

### Unit Tests

Unit tests are embedded in source files and test individual modules.

```bash
# Run all unit tests
cargo test --lib

# Run tests for a specific module
cargo test --lib storage
cargo test --lib executor
cargo test --lib sql::parser
```

### Store Tests (Internal Integration)

Store tests use the TestKit API for session-like testing.

```bash
# Run all store tests
cargo test --test store_test

# Run specific test module
cargo test --test store_test crud
cargo test --test store_test select
cargo test --test store_test filter
```

macOS note:
- `make store-test` automatically uses single-thread execution on macOS.
- For manual runs on macOS, use:

```bash
cargo test --test store_test -- --test-threads=1
```

This avoids intermittent runtime-drop and file-descriptor exhaustion issues caused by high parallelism in store tests.

### Storage Regression Tests

These suites focus on LSM flush/compaction/read-path correctness.

```bash
# Run all storage regression tests
make storage-test

# Or run suites directly
cargo test --test rocksdb_ported_tests
cargo test --test rocksdb_ported_compaction_tests

# Run failpoint crash-recovery tests (feature-gated)
make failpoint-test
```

### E2E Tests (MySQL-test Format)

E2E tests use the MySQL-test format from TiDB/MySQL.

```bash
# Run all E2E tests
cargo run --bin mysqltest-runner -- --all

# Or use the helper script
./tests/integrationtest/run-tests.sh --all

# Run specific test
cargo run --bin mysqltest-runner -- --test crud/basic
./tests/integrationtest/run-tests.sh -t crud/basic

# Record new expected results
cargo run --bin mysqltest-runner -- --test crud/basic --record
./tests/integrationtest/run-tests.sh -r crud/basic
```

## Writing Tests

### Writing Unit Tests

Add tests in the same file as the code being tested:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature() {
        // Test code here
        assert_eq!(1 + 1, 2);
    }
}
```

### Writing Store Tests

Use the TestKit API in `tests/store_test.rs`:

```rust
use testkit::TestKit;

#[test]
fn test_my_feature() {
    let tk = TestKit::new();

    // Create table
    tk.must_exec("CREATE TABLE t (id INT, name VARCHAR(100))");

    // Insert data
    tk.must_exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob')")
        .check_affected(2);

    // Query and verify
    tk.must_query("SELECT id, name FROM t ORDER BY id")
        .check(rows![["1", "Alice"], ["2", "Bob"]]);

    // Check column names
    tk.must_query("SELECT id, name FROM t")
        .check_columns(vec!["id", "name"]);

    // Check row count only
    tk.must_query("SELECT * FROM t")
        .check_row_count(2);

    // Expect error
    let err = tk.must_exec_err("SELECT * FROM non_existent");
    assert!(err.contains("not found"));
}
```

### Writing E2E Tests

Create a `.test` file in `tests/integrationtest/t/`:

```sql
# tests/integrationtest/t/myfeature/example.test

# Comment lines start with #
CREATE TABLE t (id INT, name VARCHAR(50));

# Insert data
INSERT INTO t VALUES (1, 'test');

# Query
SELECT * FROM t;

# Clean up
DROP TABLE t;
```

Then record the expected results:

```bash
./tests/integrationtest/run-tests.sh -r myfeature/example
```

This creates `tests/integrationtest/r/myfeature/example.result` with expected output.

#### Error Testing

Use `--error` directive to expect SQL errors:

```sql
--error 1146
SELECT * FROM non_existent_table;
```

## Test Directory Structure

```
tests/
├── integrationtest/           # E2E MySQL-test style tests
│   ├── t/                     # Test case files (.test)
│   │   ├── crud/              # CRUD operation tests
│   │   │   ├── basic.test
│   │   │   ├── insert.test
│   │   │   ├── select.test
│   │   │   ├── update.test
│   │   │   ├── delete.test
│   │   │   └── ddl.test
│   │   └── expr/              # Expression tests
│   │       ├── arithmetic.test
│   │       ├── comparison.test
│   │       └── aggregate.test
│   ├── r/                     # Expected result files (.result)
│   │   ├── crud/
│   │   └── expr/
│   ├── runner.rs              # Test runner binary
│   ├── run-tests.sh           # Shell helper script
│   └── README.md
├── testkit.rs                 # TestKit API module
└── store_test.rs              # Internal integration tests
```

## Continuous Integration

Tests are automatically run on GitHub Actions for:
- Every push to `master` branch
- Every pull request targeting `master`

The CI runs:
1. `cargo fmt --check` - Code formatting
2. `cargo clippy` - Linting
3. `cargo test` - Unit and store tests
4. `cargo run --bin mysqltest-runner -- --all` - E2E tests

See `.github/workflows/ci.yml` for details.

## Test Best Practices

1. **Isolation**: Each test should create and clean up its own tables
2. **Determinism**: Use `ORDER BY` when result order matters
3. **Clarity**: Use descriptive test names and comments
4. **Coverage**: Test both success and error cases
5. **Regression**: When fixing bugs, add tests that reproduce the issue first

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
