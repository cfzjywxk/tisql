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

        tk.must_exec("CREATE TABLE users (id INT, name VARCHAR(100), email VARCHAR(100))");

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

        tk.must_exec("CREATE TABLE t (id INT, val INT)");
        tk.must_exec("INSERT INTO t VALUES (1, 100)");

        // Update single row
        tk.must_exec("UPDATE t SET val = 150 WHERE id = 1")
            .check_affected(1);
        tk.must_query("SELECT val FROM t WHERE id = 1")
            .check(rows![["150"]]);
    }

    #[test]
    fn test_delete() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (id INT)");
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

        tk.must_exec("CREATE TABLE t (a INT, b VARCHAR(10))");
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

        tk.must_exec("CREATE TABLE t (id INT)");
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

        tk.must_exec("CREATE TABLE t (a INT, b INT, c INT)");
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

        tk.must_exec("CREATE TABLE t (id INT, val INT)");
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

        tk.must_exec("CREATE TABLE t (a INT, b INT)");
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

        tk.must_exec("CREATE TABLE t (id INT, name VARCHAR(50))");
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

        tk.must_exec("CREATE TABLE t1 (id INT)").check_ok();
        tk.must_exec("CREATE TABLE t2 (id INT, name VARCHAR(100))")
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

        tk.must_exec("CREATE TABLE t (id INT)").check_ok();
        tk.must_exec("CREATE TABLE IF NOT EXISTS t (id INT)")
            .check_ok();

        tk.must_exec("DROP TABLE t");
    }

    #[test]
    fn test_drop_if_exists() {
        let tk = TestKit::new();

        // Should not error on non-existent table
        tk.must_exec("DROP TABLE IF EXISTS non_existent").check_ok();

        // Create and drop
        tk.must_exec("CREATE TABLE t (id INT)");
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

        tk.must_exec("CREATE TABLE t (tiny TINYINT, small SMALLINT, normal INT, big BIGINT)");
        tk.must_exec("INSERT INTO t VALUES (127, 32767, 2147483647, 9223372036854775807)");

        tk.must_query("SELECT tiny, small, normal, big FROM t")
            .check(rows![["127", "32767", "2147483647", "9223372036854775807"]]);
    }

    #[test]
    fn test_string_types() {
        let tk = TestKit::new();

        tk.must_exec("CREATE TABLE t (c CHAR(10), vc VARCHAR(100), txt TEXT)");
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

        tk.must_exec("CREATE TABLE t (id INT, val INT)");
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

        tk.must_exec("CREATE TABLE t (a INT, b INT)");
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
        tk.must_exec("CREATE TABLE t (id INT)");
        let err = tk.must_exec_err("CREATE TABLE t (id INT)");
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
