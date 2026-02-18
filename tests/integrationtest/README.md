# TiSQL Integration Tests

This directory contains MySQL-test style integration tests for TiSQL.

## Directory Structure

```
integrationtest/
├── t/              # Test case files (.test / .t)
│   ├── crud/       # Basic CRUD operation tests
│   ├── error/      # Error expectation tests
│   └── expr/       # Expression and operator tests
├── r/              # Expected result files (.result / .r)
│   ├── crud/
│   ├── error/
│   └── expr/
├── run-tests.sh    # Test runner script
└── README.md       # This file
```

## Test File Format

Test files (`.test` or `.t`) contain SQL statements that are executed sequentially.
Each statement is separated by a semicolon.

The runner records:
- Query results (`SELECT ...`) as tab-separated output (header + rows)
- DML affected rows (`INSERT/UPDATE/DELETE`) as `AFFECTED <n>`

### Basic Format

```sql
# This is a comment
CREATE TABLE t (id INT, name VARCHAR(100));
INSERT INTO t VALUES (1, 'Alice');
SELECT * FROM t;
DROP TABLE t;
```

### Error Expectations

Use `--error` directive to expect an error:

```sql
--error 1146
SELECT * FROM non_existent_table;
```

You can also assert additional properties:

```sql
CREATE TABLE t (id INT);
--error 1050
--error_kind Catalog
--error_contains already exists
CREATE TABLE t (id INT);
```

### Multi-session directives

Use `--session <name>` to run subsequent statements in a named logical session.
This enables mysql-test style multi-session transaction scenarios in one test file.

```sql
--session s1
BEGIN;
UPDATE t SET v = 1 WHERE id = 1;

--session s2
BEGIN;
--error_kind KeyIsLocked
UPDATE t SET v = 2 WHERE id = 1;
```

Use bare `--session` (without a name) to reset back to the default stateless path.

## Running Tests

```bash
# Run all tests
./run-tests.sh

# Run specific test
./run-tests.sh -t crud/basic

# Record new results (updates .result files)
./run-tests.sh -r crud/basic
```

## Adding New Tests

1. Create a new `.test` file in `t/` subdirectory
2. Run with `-r` flag to record expected results
3. Verify the `.result` file is correct
4. Commit both files

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE](../../LICENSE) for details.
