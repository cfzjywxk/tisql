// Copyright 2024 TiSQL Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Store tests - Internal integration tests using session-like API
//!
//! These tests verify the storage layer and SQL execution in an integrated manner,
//! similar to TiDB's session-based tests.

#[macro_use]
mod testkit;

use testkit::TestKit;

/// Basic CRUD tests
mod crud {
    use super::*;

    #[test]
    fn test_create_insert_select() {
        let tk = TestKit::new();

        tk.must_exec(
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))",
        );

        // Single insert
        tk.must_exec(
            "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
        )
        .check_affected(1);

        // Multiple insert
        tk.must_exec("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com'), (3, 'Charlie', 'charlie@example.com')")
            .check_affected(2);

        // Select all
        tk.must_query("SELECT id, name FROM users ORDER BY id")
            .check(rows![["1", "Alice"], ["2", "Bob"], ["3", "Charlie"]]);

        // Select with WHERE
        tk.must_query("SELECT name, email FROM users WHERE id = 2")
            .check(rows![["Bob", "bob@example.com"]]);
    }

    #[test]
    fn test_update_single() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        tk.must_exec("INSERT INTO t VALUES (1, 100)");

        // Update single row
        tk.must_exec("UPDATE t SET val = 150 WHERE id = 1")
            .check_affected(1);
        tk.must_query("SELECT val FROM t WHERE id = 1")
            .check(rows![["150"]]);
    }

    #[test]
    fn test_update_with_expression() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (a INT PRIMARY KEY, b INT)");
        tk.must_exec("INSERT INTO t VALUES (1, 11)");
        tk.must_exec("INSERT INTO t VALUES (2, 22)");

        // Update using column reference in expression (e.g., b = b + 1)
        tk.must_exec("UPDATE t SET b = b + 1").check_affected(2);
        tk.must_query("SELECT a, b FROM t ORDER BY a")
            .check(rows![["1", "12"], ["2", "23"]]);

        // Update primary key column with expression
        tk.must_exec("UPDATE t SET a = a + 10").check_affected(2);
        tk.must_query("SELECT a, b FROM t ORDER BY a")
            .check(rows![["11", "12"], ["12", "23"]]);
    }

    #[test]
    fn test_delete() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY)");
        tk.must_exec("INSERT INTO t VALUES (1), (2), (3), (4), (5)");

        // Delete single row
        tk.must_exec("DELETE FROM t WHERE id = 3").check_affected(1);
        tk.must_query("SELECT id FROM t ORDER BY id")
            .check(rows![["1"], ["2"], ["4"], ["5"]]);

        // Delete multiple rows
        tk.must_exec("DELETE FROM t WHERE id > 2").check_affected(2);
        tk.must_query("SELECT id FROM t ORDER BY id")
            .check(rows![["1"], ["2"]]);
    }
}

/// SELECT operation tests
mod select {
    use super::*;

    #[test]
    fn test_order_by() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (a INT PRIMARY KEY, b VARCHAR(10))");
        tk.must_exec("INSERT INTO t VALUES (3, 'c'), (1, 'a'), (2, 'b')");

        // ORDER BY ASC
        tk.must_query("SELECT a, b FROM t ORDER BY a").check(rows![
            ["1", "a"],
            ["2", "b"],
            ["3", "c"]
        ]);

        // ORDER BY DESC
        tk.must_query("SELECT a, b FROM t ORDER BY a DESC")
            .check(rows![["3", "c"], ["2", "b"], ["1", "a"]]);
    }

    #[test]
    fn test_limit_offset() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY)");
        tk.must_exec("INSERT INTO t VALUES (1), (2), (3), (4), (5)");

        // LIMIT only
        tk.must_query("SELECT id FROM t ORDER BY id LIMIT 3")
            .check(rows![["1"], ["2"], ["3"]]);

        // LIMIT with OFFSET
        tk.must_query("SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2")
            .check(rows![["3"], ["4"]]);
    }

    #[test]
    fn test_projection() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT)");
        tk.must_exec("INSERT INTO t VALUES (1, 2, 3)");

        tk.must_query("SELECT a FROM t").check(rows![["1"]]);
        tk.must_query("SELECT b, a FROM t").check(rows![["2", "1"]]);
        tk.must_query("SELECT c, b, a FROM t")
            .check(rows![["3", "2", "1"]]);
    }

    #[test]
    fn test_literal_select() {
        let tk = TestKit::new();

        tk.must_query("SELECT 1").check(rows![["1"]]);
        tk.must_query("SELECT 1 + 2").check(rows![["3"]]);
        tk.must_query("SELECT 10 * 5").check(rows![["50"]]);
    }
}

/// WHERE clause tests
mod filter {
    use super::*;

    #[test]
    fn test_comparison_operators() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        tk.must_exec("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)");

        // Equality
        tk.must_query("SELECT id FROM t WHERE val = 20")
            .check(rows![["2"]]);

        // Not equal
        tk.must_query("SELECT id FROM t WHERE val != 20 ORDER BY id")
            .check(rows![["1"], ["3"], ["4"]]);

        // Less than
        tk.must_query("SELECT id FROM t WHERE val < 25 ORDER BY id")
            .check(rows![["1"], ["2"]]);

        // Greater than
        tk.must_query("SELECT id FROM t WHERE val > 25 ORDER BY id")
            .check(rows![["3"], ["4"]]);

        // Less than or equal
        tk.must_query("SELECT id FROM t WHERE val <= 20 ORDER BY id")
            .check(rows![["1"], ["2"]]);

        // Greater than or equal
        tk.must_query("SELECT id FROM t WHERE val >= 30 ORDER BY id")
            .check(rows![["3"], ["4"]]);
    }

    #[test]
    fn test_logical_operators() {
        let tk = TestKit::new();

        // Use composite primary key to allow (a, b) combinations
        tk.must_exec("CREATE TABLE t (a INT, b INT, PRIMARY KEY (a, b))");
        tk.must_exec("INSERT INTO t VALUES (1, 1), (1, 2), (2, 1), (2, 2)");

        // AND
        tk.must_query("SELECT a, b FROM t WHERE a = 1 AND b = 2")
            .check(rows![["1", "2"]]);

        // OR
        tk.must_query("SELECT a, b FROM t WHERE a = 1 OR b = 1 ORDER BY a, b")
            .check(rows![["1", "1"], ["1", "2"], ["2", "1"]]);

        // Combined
        tk.must_query("SELECT a, b FROM t WHERE (a = 1 AND b = 1) OR (a = 2 AND b = 2) ORDER BY a")
            .check(rows![["1", "1"], ["2", "2"]]);
    }

    #[test]
    fn test_string_comparison() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))");
        tk.must_exec("INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

        tk.must_query("SELECT id FROM t WHERE name = 'Bob'")
            .check(rows![["2"]]);
    }
}

/// DDL tests
mod ddl {
    use super::*;

    #[test]
    fn test_create_drop_table() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t1 (id INT PRIMARY KEY)")
            .check_ok();
        tk.must_exec("CREATE TABLE t2 (id INT PRIMARY KEY, name VARCHAR(100))")
            .check_ok();

        // Insert and verify
        tk.must_exec("INSERT INTO t1 VALUES (1)");
        tk.must_exec("INSERT INTO t2 VALUES (1, 'test')");

        tk.must_exec("DROP TABLE t1").check_ok();
        tk.must_exec("DROP TABLE t2").check_ok();
    }

    #[test]
    fn test_create_if_not_exists() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY)")
            .check_ok();
        tk.must_exec("CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY)")
            .check_ok();

        tk.must_exec("DROP TABLE t");
    }

    #[test]
    fn test_drop_if_exists() {
        let tk = TestKit::new();

        // Should not error on non-existent table
        tk.must_exec("DROP TABLE IF EXISTS non_existent").check_ok();

        // Create and drop
        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY)");
        tk.must_exec("DROP TABLE IF EXISTS t").check_ok();
        tk.must_exec("DROP TABLE IF EXISTS t").check_ok();
    }
}

/// Data type tests
mod data_types {
    use super::*;

    #[test]
    fn test_integer_types() {
        let tk = TestKit::new();

        tk.must_exec(
            "CREATE TABLE t (tiny TINYINT PRIMARY KEY, small SMALLINT, normal INT, big BIGINT)",
        );
        tk.must_exec("INSERT INTO t VALUES (127, 32767, 2147483647, 9223372036854775807)");

        tk.must_query("SELECT tiny, small, normal, big FROM t")
            .check(rows![["127", "32767", "2147483647", "9223372036854775807"]]);
    }

    #[test]
    fn test_string_types() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (c CHAR(10) PRIMARY KEY, vc VARCHAR(100), txt TEXT)");
        tk.must_exec("INSERT INTO t VALUES ('hello', 'world', 'long text content')");

        tk.must_query("SELECT c, vc, txt FROM t").check(rows![[
            "hello",
            "world",
            "long text content"
        ]]);
    }

    #[test]
    fn test_null_handling() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        tk.must_exec("INSERT INTO t (id) VALUES (1)");
        tk.must_exec("INSERT INTO t VALUES (2, 100)");

        tk.must_query("SELECT id, val FROM t ORDER BY id")
            .check(rows![["1", "NULL"], ["2", "100"]]);
    }
}

/// Expression tests
mod expressions {
    use super::*;

    #[test]
    fn test_arithmetic() {
        let tk = TestKit::new();

        tk.must_query("SELECT 1 + 2").check(rows![["3"]]);
        tk.must_query("SELECT 10 - 3").check(rows![["7"]]);
        tk.must_query("SELECT 4 * 5").check(rows![["20"]]);
        tk.must_query("SELECT 20 / 4").check(rows![["5"]]);
        tk.must_query("SELECT 17 % 5").check(rows![["2"]]);
    }

    #[test]
    fn test_arithmetic_with_columns() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (a INT PRIMARY KEY, b INT)");
        tk.must_exec("INSERT INTO t VALUES (10, 3)");

        tk.must_query("SELECT a + b FROM t").check(rows![["13"]]);
        tk.must_query("SELECT a - b FROM t").check(rows![["7"]]);
        tk.must_query("SELECT a * b FROM t").check(rows![["30"]]);
    }

    #[test]
    fn test_negative_numbers() {
        let tk = TestKit::new();

        tk.must_query("SELECT -5").check(rows![["-5"]]);
        tk.must_query("SELECT 0 - 10").check(rows![["-10"]]);
    }
}

/// Error handling tests
mod errors {
    use super::*;

    #[test]
    fn test_table_not_found() {
        let tk = TestKit::new();
        let err = tk.must_exec_err("SELECT * FROM non_existent");
        assert!(
            err.contains("not found") || err.contains("Table"),
            "Error: {err}"
        );
    }

    #[test]
    fn test_duplicate_table() {
        let tk = TestKit::new();
        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY)");
        let err = tk.must_exec_err("CREATE TABLE t (id INT PRIMARY KEY)");
        assert!(
            err.contains("exists") || err.contains("already"),
            "Error: {err}"
        );
    }
}

// NOTE: The following tests document known limitations in the current implementation:
// 1. Aggregate functions (COUNT, SUM, AVG, MIN, MAX) do not properly collapse rows
// 2. UPDATE with expression (val = val + 1) has issues with row key generation
// These behaviors are captured by E2E tests in tests/integrationtest/ for regression detection.

/// Persistence tests - verify data and catalog survive restart
mod persistence {
    use tempfile::tempdir;
    use tisql::{Database, DatabaseConfig, QueryResult};

    #[test]
    fn test_catalog_survives_restart() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        // First session: create table and insert data
        {
            let db = Database::open(config.clone()).unwrap();
            tisql::io::block_on_sync(
                db.handle_mp_query("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))"),
            )
            .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO users VALUES (1, 'Alice')"))
                .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO users VALUES (2, 'Bob')"))
                .unwrap();
            db.close().unwrap();
        }

        // Second session: verify table and data exist
        {
            let db = Database::open(config.clone()).unwrap();

            // Table should exist
            let result = tisql::io::block_on_sync(
                db.handle_mp_query("SELECT id, name FROM users ORDER BY id"),
            );
            match result {
                Ok(QueryResult::Rows { data, columns }) => {
                    assert_eq!(columns, vec!["id", "name"]);
                    assert_eq!(data.len(), 2);
                    assert_eq!(data[0][0], "1");
                    assert_eq!(data[0][1], "Alice");
                    assert_eq!(data[1][0], "2");
                    assert_eq!(data[1][1], "Bob");
                }
                other => panic!("Expected rows, got: {other:?}"),
            }
            db.close().unwrap();
        }
    }

    #[test]
    fn test_multiple_tables_survive_restart() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        // First session: create multiple tables
        {
            let db = Database::open(config.clone()).unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("CREATE TABLE t1 (a INT PRIMARY KEY)"))
                .unwrap();
            tisql::io::block_on_sync(
                db.handle_mp_query("CREATE TABLE t2 (b INT PRIMARY KEY, c INT)"),
            )
            .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO t1 VALUES (100)")).unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO t2 VALUES (200, 300)"))
                .unwrap();
            db.close().unwrap();
        }

        // Second session: verify both tables exist
        {
            let db = Database::open(config.clone()).unwrap();

            // Check t1
            let result = tisql::io::block_on_sync(db.handle_mp_query("SELECT a FROM t1")).unwrap();
            match result {
                QueryResult::Rows { data, .. } => {
                    assert_eq!(data.len(), 1);
                    assert_eq!(data[0][0], "100");
                }
                other => panic!("Expected rows for t1, got: {other:?}"),
            }

            // Check t2
            let result =
                tisql::io::block_on_sync(db.handle_mp_query("SELECT b, c FROM t2")).unwrap();
            match result {
                QueryResult::Rows { data, .. } => {
                    assert_eq!(data.len(), 1);
                    assert_eq!(data[0][0], "200");
                    assert_eq!(data[0][1], "300");
                }
                other => panic!("Expected rows for t2, got: {other:?}"),
            }
            db.close().unwrap();
        }
    }

    #[test]
    fn test_drop_table_persists() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        // First session: create and drop a table
        {
            let db = Database::open(config.clone()).unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("CREATE TABLE temp (x INT PRIMARY KEY)"))
                .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO temp VALUES (1)")).unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("DROP TABLE temp")).unwrap();
            // Create another table to verify we can still create tables
            tisql::io::block_on_sync(db.handle_mp_query("CREATE TABLE perm (y INT PRIMARY KEY)"))
                .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO perm VALUES (2)")).unwrap();
            db.close().unwrap();
        }

        // Second session: verify temp is gone but perm exists
        {
            let db = Database::open(config.clone()).unwrap();

            // temp should not exist
            let result = tisql::io::block_on_sync(db.handle_mp_query("SELECT * FROM temp"));
            assert!(result.is_err(), "temp table should not exist after restart");

            // perm should exist
            let result =
                tisql::io::block_on_sync(db.handle_mp_query("SELECT y FROM perm")).unwrap();
            match result {
                QueryResult::Rows { data, .. } => {
                    assert_eq!(data.len(), 1);
                    assert_eq!(data[0][0], "2");
                }
                other => panic!("Expected rows for perm, got: {other:?}"),
            }
            db.close().unwrap();
        }
    }

    #[test]
    fn test_table_id_persists() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        // First session: create tables
        {
            let db = Database::open(config.clone()).unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("CREATE TABLE t1 (a INT PRIMARY KEY)"))
                .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("CREATE TABLE t2 (b INT PRIMARY KEY)"))
                .unwrap();
            db.close().unwrap();
        }

        // Second session: create more tables - IDs should continue from where we left off
        {
            let db = Database::open(config.clone()).unwrap();
            // This should succeed without ID conflict
            tisql::io::block_on_sync(db.handle_mp_query("CREATE TABLE t3 (c INT PRIMARY KEY)"))
                .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO t3 VALUES (333)")).unwrap();

            let result = tisql::io::block_on_sync(db.handle_mp_query("SELECT c FROM t3")).unwrap();
            match result {
                QueryResult::Rows { data, .. } => {
                    assert_eq!(data.len(), 1);
                    assert_eq!(data[0][0], "333");
                }
                other => panic!("Expected rows, got: {other:?}"),
            }
            db.close().unwrap();
        }
    }

    /// Test crash recovery without graceful shutdown (simulates kill -9).
    /// This test does NOT call db.close() to simulate an abrupt crash.
    #[test]
    fn test_crash_recovery_no_close() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        // First session: write data and "crash" (no close)
        {
            let db = Database::open(config.clone()).unwrap();
            tisql::io::block_on_sync(db.handle_mp_query(
                "CREATE TABLE crash_test (id INT PRIMARY KEY, data VARCHAR(100))",
            ))
            .unwrap();
            tisql::io::block_on_sync(
                db.handle_mp_query("INSERT INTO crash_test VALUES (1, 'first')"),
            )
            .unwrap();
            tisql::io::block_on_sync(
                db.handle_mp_query("INSERT INTO crash_test VALUES (2, 'second')"),
            )
            .unwrap();
            tisql::io::block_on_sync(
                db.handle_mp_query("INSERT INTO crash_test VALUES (3, 'third')"),
            )
            .unwrap();
            // NO close() - simulate kill -9
            // Database is dropped here, but close() is not called
        }

        // Second session: recover and verify all data
        {
            let db = Database::open(config.clone()).unwrap();

            let result = tisql::io::block_on_sync(
                db.handle_mp_query("SELECT id, data FROM crash_test ORDER BY id"),
            )
            .unwrap();
            match result {
                QueryResult::Rows { data, columns } => {
                    assert_eq!(columns, vec!["id", "data"]);
                    assert_eq!(data.len(), 3, "All 3 rows should be recovered");
                    assert_eq!(data[0][0], "1");
                    assert_eq!(data[0][1], "first");
                    assert_eq!(data[1][0], "2");
                    assert_eq!(data[1][1], "second");
                    assert_eq!(data[2][0], "3");
                    assert_eq!(data[2][1], "third");
                }
                other => panic!("Expected rows, got: {other:?}"),
            }
            db.close().unwrap();
        }
    }

    /// Test crash recovery with updates and deletes.
    #[test]
    fn test_crash_recovery_with_updates_deletes() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        // First session: create, insert, update, delete, then crash
        {
            let db = Database::open(config.clone()).unwrap();
            tisql::io::block_on_sync(
                db.handle_mp_query("CREATE TABLE modify_test (id INT PRIMARY KEY, value INT)"),
            )
            .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO modify_test VALUES (1, 100)"))
                .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO modify_test VALUES (2, 200)"))
                .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO modify_test VALUES (3, 300)"))
                .unwrap();

            // Update id=2
            tisql::io::block_on_sync(
                db.handle_mp_query("UPDATE modify_test SET value = 999 WHERE id = 2"),
            )
            .unwrap();

            // Delete id=1
            tisql::io::block_on_sync(db.handle_mp_query("DELETE FROM modify_test WHERE id = 1"))
                .unwrap();

            // NO close() - crash
        }

        // Second session: verify state
        {
            let db = Database::open(config.clone()).unwrap();

            let result = tisql::io::block_on_sync(
                db.handle_mp_query("SELECT id, value FROM modify_test ORDER BY id"),
            )
            .unwrap();
            match result {
                QueryResult::Rows { data, .. } => {
                    // Should have id=2 (updated) and id=3 (unchanged)
                    // id=1 was deleted
                    assert_eq!(data.len(), 2, "Should have 2 rows after delete");
                    assert_eq!(data[0][0], "2");
                    assert_eq!(data[0][1], "999"); // Updated value
                    assert_eq!(data[1][0], "3");
                    assert_eq!(data[1][1], "300");
                }
                other => panic!("Expected rows, got: {other:?}"),
            }
            db.close().unwrap();
        }
    }

    /// Test crash recovery with multiple crash/recover cycles.
    #[test]
    fn test_multiple_crash_recovery_cycles() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        // Cycle 1: create and insert
        {
            let db = Database::open(config.clone()).unwrap();
            tisql::io::block_on_sync(
                db.handle_mp_query("CREATE TABLE cycle_test (id INT PRIMARY KEY)"),
            )
            .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO cycle_test VALUES (1)"))
                .unwrap();
            // crash - no close()
        }

        // Cycle 2: recover, insert more, crash again
        {
            let db = Database::open(config.clone()).unwrap();

            // Verify cycle 1 data
            let result = tisql::io::block_on_sync(
                db.handle_mp_query("SELECT id FROM cycle_test ORDER BY id"),
            )
            .unwrap();
            match &result {
                QueryResult::Rows { data, .. } => {
                    assert_eq!(data.len(), 1, "Should have 1 row from cycle 1");
                    assert_eq!(data[0][0], "1");
                }
                _ => panic!("Expected rows"),
            }

            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO cycle_test VALUES (2)"))
                .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO cycle_test VALUES (3)"))
                .unwrap();
            // crash - no close()
        }

        // Cycle 3: recover, insert more, crash again
        {
            let db = Database::open(config.clone()).unwrap();

            // Verify cycle 1+2 data
            let result = tisql::io::block_on_sync(
                db.handle_mp_query("SELECT id FROM cycle_test ORDER BY id"),
            )
            .unwrap();
            match &result {
                QueryResult::Rows { data, .. } => {
                    assert_eq!(data.len(), 3, "Should have 3 rows from cycles 1+2");
                    assert_eq!(data[0][0], "1");
                    assert_eq!(data[1][0], "2");
                    assert_eq!(data[2][0], "3");
                }
                _ => panic!("Expected rows"),
            }

            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO cycle_test VALUES (4)"))
                .unwrap();
            // crash - no close()
        }

        // Final: verify all data
        {
            let db = Database::open(config.clone()).unwrap();

            let result = tisql::io::block_on_sync(
                db.handle_mp_query("SELECT id FROM cycle_test ORDER BY id"),
            )
            .unwrap();
            match result {
                QueryResult::Rows { data, .. } => {
                    assert_eq!(data.len(), 4, "Should have all 4 rows from 3 cycles");
                    assert_eq!(data[0][0], "1");
                    assert_eq!(data[1][0], "2");
                    assert_eq!(data[2][0], "3");
                    assert_eq!(data[3][0], "4");
                }
                other => panic!("Expected rows, got: {other:?}"),
            }
            db.close().unwrap();
        }
    }

    /// Test crash during DDL (DROP TABLE) - verify atomicity.
    #[test]
    fn test_crash_recovery_ddl_drop_table() {
        let dir = tempdir().unwrap();
        let config = DatabaseConfig::with_data_dir(dir.path());

        // Create two tables
        {
            let db = Database::open(config.clone()).unwrap();
            tisql::io::block_on_sync(
                db.handle_mp_query("CREATE TABLE keep_me (x INT PRIMARY KEY)"),
            )
            .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO keep_me VALUES (42)"))
                .unwrap();
            tisql::io::block_on_sync(
                db.handle_mp_query("CREATE TABLE drop_me (y INT PRIMARY KEY)"),
            )
            .unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("INSERT INTO drop_me VALUES (99)"))
                .unwrap();
            db.close().unwrap();
        }

        // Drop one table and crash
        {
            let db = Database::open(config.clone()).unwrap();
            tisql::io::block_on_sync(db.handle_mp_query("DROP TABLE drop_me")).unwrap();
            // crash
        }

        // Verify state
        {
            let db = Database::open(config.clone()).unwrap();

            // keep_me should exist
            let result =
                tisql::io::block_on_sync(db.handle_mp_query("SELECT x FROM keep_me")).unwrap();
            match result {
                QueryResult::Rows { data, .. } => {
                    assert_eq!(data[0][0], "42");
                }
                other => panic!("Expected rows for keep_me, got: {other:?}"),
            }

            // drop_me should not exist
            let result = tisql::io::block_on_sync(db.handle_mp_query("SELECT * FROM drop_me"));
            assert!(
                result.is_err(),
                "drop_me should not exist after crash recovery"
            );

            db.close().unwrap();
        }
    }
}

/// Duplicate key constraint tests
mod duplicate_key {
    use super::*;

    #[test]
    fn test_insert_duplicate_primary_key() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        tk.must_exec("INSERT INTO t VALUES (1, 100)")
            .check_affected(1);

        // Try to insert duplicate key - should fail
        let err = tk.must_exec_err("INSERT INTO t VALUES (1, 200)");
        assert!(
            err.contains("Duplicate") || err.contains("duplicate"),
            "Expected duplicate key error, got: {err}"
        );

        // Original row should be unchanged
        tk.must_query("SELECT id, val FROM t")
            .check(rows![["1", "100"]]);
    }

    #[test]
    fn test_insert_duplicate_composite_key() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT, PRIMARY KEY (a, b))");
        tk.must_exec("INSERT INTO t VALUES (1, 1, 100)")
            .check_affected(1);
        tk.must_exec("INSERT INTO t VALUES (1, 2, 200)")
            .check_affected(1);
        tk.must_exec("INSERT INTO t VALUES (2, 1, 300)")
            .check_affected(1);

        // Try to insert duplicate composite key - should fail
        let err = tk.must_exec_err("INSERT INTO t VALUES (1, 1, 999)");
        assert!(
            err.contains("Duplicate") || err.contains("duplicate"),
            "Expected duplicate key error, got: {err}"
        );

        // Check original data unchanged
        tk.must_query("SELECT a, b, c FROM t ORDER BY a, b")
            .check(rows![
                ["1", "1", "100"],
                ["1", "2", "200"],
                ["2", "1", "300"]
            ]);
    }

    #[test]
    fn test_update_to_duplicate_primary_key() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100))");
        tk.must_exec("INSERT INTO t VALUES (1, 'Alice')");
        tk.must_exec("INSERT INTO t VALUES (2, 'Bob')");
        tk.must_exec("INSERT INTO t VALUES (3, 'Charlie')");

        // Update to existing PK - should fail
        let err = tk.must_exec_err("UPDATE t SET id = 1 WHERE id = 2");
        assert!(
            err.contains("Duplicate") || err.contains("duplicate"),
            "Expected duplicate key error, got: {err}"
        );

        // Data should be unchanged
        tk.must_query("SELECT id, name FROM t ORDER BY id")
            .check(rows![["1", "Alice"], ["2", "Bob"], ["3", "Charlie"]]);
    }

    #[test]
    fn test_update_pk_to_new_value() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        tk.must_exec("INSERT INTO t VALUES (1, 100)");
        tk.must_exec("INSERT INTO t VALUES (2, 200)");

        // Update to non-existing PK - should succeed
        tk.must_exec("UPDATE t SET id = 10 WHERE id = 2")
            .check_affected(1);

        // Check the update was applied
        tk.must_query("SELECT id, val FROM t ORDER BY id")
            .check(rows![["1", "100"], ["10", "200"]]);
    }

    #[test]
    fn test_insert_non_duplicate_succeeds() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT PRIMARY KEY, val INT)");
        tk.must_exec("INSERT INTO t VALUES (1, 100)")
            .check_affected(1);

        // Insert with different PK - should succeed
        tk.must_exec("INSERT INTO t VALUES (2, 200)")
            .check_affected(1);
        tk.must_exec("INSERT INTO t VALUES (3, 300)")
            .check_affected(1);

        tk.must_query("SELECT id, val FROM t ORDER BY id")
            .check(rows![["1", "100"], ["2", "200"], ["3", "300"]]);
    }

    #[test]
    fn test_table_without_pk_requires_error() {
        let tk = TestKit::new();

        // Table without explicit PK should be rejected (not supported yet)
        let err = tk.must_exec_err("CREATE TABLE t (a INT, b INT)");
        assert!(
            err.contains("PRIMARY KEY") || err.contains("primary key"),
            "Expected PRIMARY KEY required error, got: {err}"
        );
    }

    #[test]
    fn test_update_all_rows_pk_increment_collision() {
        // Reproduces the scenario from the user bug report:
        // When UPDATE SET a = a + 1 is run on a table with rows (a=1, a=2),
        // updating a=1 to a=2 should fail because a=2 already exists.
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (a INT PRIMARY KEY, b INT)");
        tk.must_exec("INSERT INTO t VALUES (1, 11)");
        tk.must_exec("INSERT INTO t VALUES (2, 22)");

        tk.must_query("SELECT a, b FROM t ORDER BY a")
            .check(rows![["1", "11"], ["2", "22"]]);

        // This should fail: a=1 → a=2 conflicts with existing a=2
        let err = tk.must_exec_err("UPDATE t SET a = a + 1");
        assert!(
            err.contains("Duplicate") || err.contains("duplicate"),
            "Expected duplicate key error, got: {err}"
        );

        // Data should be unchanged
        tk.must_query("SELECT a, b FROM t ORDER BY a")
            .check(rows![["1", "11"], ["2", "22"]]);
    }
}
