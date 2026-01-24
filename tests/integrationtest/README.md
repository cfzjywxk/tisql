# TiSQL Integration Tests

This directory contains MySQL-test style integration tests for TiSQL.

## Directory Structure

```
integrationtest/
├── t/              # Test case files (.test)
│   ├── crud/       # Basic CRUD operation tests
│   └── expr/       # Expression and operator tests
├── r/              # Expected result files (.result)
│   ├── crud/
│   └── expr/
├── run-tests.sh    # Test runner script
└── README.md       # This file
```

## Test File Format

Test files (`.test`) contain SQL statements that are executed sequentially.
Each statement is separated by a semicolon.

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
