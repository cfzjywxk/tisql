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

//! MySQL-test style E2E test runner for TiSQL
//!
//! This runner executes .test files against a TiSQL database and compares
//! results with .result files.
//!
//! Usage:
//!   cargo run --bin mysqltest-runner -- --test crud/basic
//!   cargo run --bin mysqltest-runner -- --record crud/basic
//!   cargo run --bin mysqltest-runner -- --all

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tisql::error::TiSqlError;
use tisql::{Database, QueryResult};

/// Test runner configuration
struct Config {
    test_dir: PathBuf,
    result_dir: PathBuf,
    record_mode: bool,
}

/// Test case
struct TestCase {
    name: String,
    test_file: PathBuf,
    result_file: PathBuf,
}

/// SQL statement with optional error expectation
struct Statement {
    sql: String,
    expectations: Expectations,
    line_number: usize,
}

/// Test result
struct TestResult {
    name: String,
    passed: bool,
    message: String,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("integrationtest");

    let config = Config {
        test_dir: base_dir.join("t"),
        result_dir: base_dir.join("r"),
        record_mode: args.iter().any(|a| a == "--record" || a == "-r"),
    };

    let specific_test = args
        .iter()
        .position(|a| a == "--test" || a == "-t")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let run_all = args.iter().any(|a| a == "--all");

    // Find test cases
    let test_cases = if let Some(test_name) = specific_test {
        vec![find_test_case(&config, &test_name)]
    } else if run_all {
        find_all_test_cases(&config)
    } else {
        eprintln!("Usage: mysqltest-runner [--test <name>] [--all] [--record]");
        eprintln!("  --test, -t  <name>  Run specific test (e.g., crud/basic)");
        eprintln!("  --all               Run all tests");
        eprintln!("  --record, -r        Record results instead of comparing");
        std::process::exit(1);
    };

    // Run tests
    let mut results = Vec::new();
    for test_case in test_cases {
        let result = run_test(&config, &test_case);
        results.push(result);
    }

    // Print summary
    println!("\n========================================");
    println!("Test Summary");
    println!("========================================");

    let mut passed = 0;
    let mut failed = 0;

    for result in &results {
        if result.passed {
            println!("[PASS] {}", result.name);
            passed += 1;
        } else {
            println!("[FAIL] {} - {}", result.name, result.message);
            failed += 1;
        }
    }

    println!("----------------------------------------");
    println!("Passed: {passed}, Failed: {failed}");

    if failed > 0 {
        std::process::exit(1);
    }
}

/// Expected behavior for a statement.
#[derive(Debug, Clone, Default)]
struct Expectations {
    expected_error: Option<ExpectedError>,
    expected_affected_rows: Option<u64>,
    expected_rows: Option<usize>,
}

#[derive(Debug, Clone, Default)]
struct ExpectedError {
    /// MySQL-style error code (e.g. 1146). If omitted, any error is accepted.
    code: Option<u32>,
    /// TiSqlError variant name to match (e.g. "TableNotFound").
    kind: Option<String>,
    /// Substring that must appear in the error message.
    contains: Option<String>,
}

fn find_test_case(config: &Config, name: &str) -> TestCase {
    let test_file = config.test_dir.join(format!("{name}.test"));
    let result_file = config.result_dir.join(format!("{name}.result"));

    if !test_file.exists() {
        eprintln!("Test file not found: {test_file:?}");
        std::process::exit(1);
    }

    TestCase {
        name: name.to_string(),
        test_file,
        result_file,
    }
}

fn find_all_test_cases(config: &Config) -> Vec<TestCase> {
    let mut cases = Vec::new();
    find_tests_recursive(
        &config.test_dir,
        &config.test_dir,
        &config.result_dir,
        &mut cases,
    );
    cases.sort_by(|a, b| a.name.cmp(&b.name));
    cases
}

fn find_tests_recursive(
    base: &Path,
    current: &Path,
    result_base: &Path,
    cases: &mut Vec<TestCase>,
) {
    if let Ok(entries) = fs::read_dir(current) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let subdir_name = path.file_name().unwrap().to_str().unwrap();
                let result_subdir = result_base.join(subdir_name);
                find_tests_recursive(base, &path, &result_subdir, cases);
            } else if path.extension().is_some_and(|e| e == "test") {
                let relative = path.strip_prefix(base).unwrap();
                let name = relative
                    .with_extension("")
                    .to_str()
                    .unwrap()
                    .replace('\\', "/");
                let result_file = result_base.join(format!(
                    "{}.result",
                    path.file_stem().unwrap().to_str().unwrap()
                ));

                cases.push(TestCase {
                    name,
                    test_file: path,
                    result_file,
                });
            }
        }
    }
}

fn run_test(config: &Config, test_case: &TestCase) -> TestResult {
    println!("\nRunning test: {}", test_case.name);

    // Parse test file
    let statements = match parse_test_file(&test_case.test_file) {
        Ok(stmts) => stmts,
        Err(e) => {
            return TestResult {
                name: test_case.name.clone(),
                passed: false,
                message: format!("Failed to parse test file: {e}"),
            };
        }
    };

    // Create database
    let db = Arc::new(Database::new());

    // Execute statements and collect output
    let mut output = String::new();

    for stmt in &statements {
        // Write the SQL to output
        output.push_str(&stmt.sql);
        output.push('\n');

        // Execute
        match db.execute(&stmt.sql) {
            Ok(result) => {
                if stmt.expectations.expected_error.is_some() {
                    return TestResult {
                        name: test_case.name.clone(),
                        passed: false,
                        message: format!(
                            "Line {}: Expected error but got success for: {}",
                            stmt.line_number, stmt.sql
                        ),
                    };
                }

                // Format result
                match result {
                    QueryResult::Rows { columns, data } => {
                        if let Some(expected_rows) = stmt.expectations.expected_rows {
                            if data.len() != expected_rows {
                                return TestResult {
                                    name: test_case.name.clone(),
                                    passed: false,
                                    message: format!(
                                        "Line {}: Expected {} rows but got {} for: {}",
                                        stmt.line_number,
                                        expected_rows,
                                        data.len(),
                                        stmt.sql
                                    ),
                                };
                            }
                        }

                        // Header
                        output.push_str(&columns.join("\t"));
                        output.push('\n');

                        // Data rows
                        for row in &data {
                            output.push_str(&row.join("\t"));
                            output.push('\n');
                        }
                    }
                    QueryResult::Affected(count) => {
                        if let Some(expected) = stmt.expectations.expected_affected_rows {
                            if count != expected {
                                return TestResult {
                                    name: test_case.name.clone(),
                                    passed: false,
                                    message: format!(
                                        "Line {}: Expected {} affected rows but got {} for: {}",
                                        stmt.line_number, expected, count, stmt.sql
                                    ),
                                };
                            }
                        }

                        // Output affected count so result files can validate DML semantics.
                        output.push_str(&format!("AFFECTED {count}\n"));
                    }
                    QueryResult::Ok => {
                        // Don't output for DDL
                    }
                }
            }
            Err(e) => {
                if let Some(expected) = &stmt.expectations.expected_error {
                    // Error was expected; validate it and record a stable representation.
                    let actual_code = mysql_error_code(&e);
                    let actual_kind = tisql_error_kind(&e);

                    if let Some(expected_code) = expected.code {
                        if actual_code != expected_code {
                            return TestResult {
                                name: test_case.name.clone(),
                                passed: false,
                                message: format!(
                                    "Line {}: Expected error code {} but got {} ({}) for: {}",
                                    stmt.line_number,
                                    expected_code,
                                    actual_code,
                                    actual_kind,
                                    stmt.sql
                                ),
                            };
                        }
                    }

                    if let Some(expected_kind) = &expected.kind {
                        if &actual_kind != expected_kind {
                            return TestResult {
                                name: test_case.name.clone(),
                                passed: false,
                                message: format!(
                                    "Line {}: Expected error kind {} but got {} for: {}",
                                    stmt.line_number, expected_kind, actual_kind, stmt.sql
                                ),
                            };
                        }
                    }

                    if let Some(needle) = &expected.contains {
                        let msg = e.to_string();
                        if !msg.contains(needle) {
                            return TestResult {
                                name: test_case.name.clone(),
                                passed: false,
                                message: format!(
                                    "Line {}: Expected error message to contain {:?} but got {:?} for: {}",
                                    stmt.line_number, needle, msg, stmt.sql
                                ),
                            };
                        }
                    }

                    // Keep output stable (avoid encoding full messages that may change).
                    output.push_str(&format!("ERROR {actual_code}: {actual_kind}\n"));
                } else {
                    return TestResult {
                        name: test_case.name.clone(),
                        passed: false,
                        message: format!(
                            "Line {}: Unexpected error: {} for: {}",
                            stmt.line_number, e, stmt.sql
                        ),
                    };
                }
            }
        }
    }

    // Handle record mode
    if config.record_mode {
        if let Some(parent) = test_case.result_file.parent() {
            if let Err(e) = fs::create_dir_all(parent) {
                return TestResult {
                    name: test_case.name.clone(),
                    passed: false,
                    message: format!("Failed to create result directory {parent:?}: {e}"),
                };
            }
        }

        match fs::write(&test_case.result_file, &output) {
            Ok(_) => {
                println!("Recorded: {:?}", test_case.result_file);
                return TestResult {
                    name: test_case.name.clone(),
                    passed: true,
                    message: "Recorded".to_string(),
                };
            }
            Err(e) => {
                return TestResult {
                    name: test_case.name.clone(),
                    passed: false,
                    message: format!("Failed to write result: {e}"),
                };
            }
        }
    }

    // Compare with expected result
    if !test_case.result_file.exists() {
        return TestResult {
            name: test_case.name.clone(),
            passed: false,
            message: format!("Result file not found: {:?}", test_case.result_file),
        };
    }

    let expected = match fs::read_to_string(&test_case.result_file) {
        Ok(s) => s,
        Err(e) => {
            return TestResult {
                name: test_case.name.clone(),
                passed: false,
                message: format!("Failed to read result file: {e}"),
            };
        }
    };

    // Normalize line endings
    let output = output.replace("\r\n", "\n");
    let expected = expected.replace("\r\n", "\n");

    if output == expected {
        TestResult {
            name: test_case.name.clone(),
            passed: true,
            message: "OK".to_string(),
        }
    } else {
        let message = format_output_mismatch(&expected, &output);
        TestResult {
            name: test_case.name.clone(),
            passed: false,
            message,
        }
    }
}

fn parse_test_file(path: &Path) -> Result<Vec<Statement>, String> {
    let file = fs::File::open(path).map_err(|e| e.to_string())?;
    let reader = BufReader::new(file);

    let mut statements = Vec::new();
    let mut current_sql = String::new();
    let mut expectations = Expectations::default();
    let mut start_line: usize = 0;

    // Simple SQL lexer state for splitting on semicolons.
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;
    let mut in_block_comment = false;

    for (idx, line) in reader.lines().enumerate() {
        let line_number = idx + 1;
        let line = line.map_err(|e| e.to_string())?;
        let trimmed = line.trim();

        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        // mysql-test style directives.
        if trimmed.starts_with("--error") {
            // --error [<mysql_code>]
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            let mut expected = expectations.expected_error.take().unwrap_or_default();
            expected.code = parts.get(1).and_then(|s| s.parse::<u32>().ok());
            expectations.expected_error = Some(expected);
            continue;
        }
        if trimmed.starts_with("--error_kind") {
            // --error_kind <TiSqlErrorVariant>
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if let Some(kind) = parts.get(1) {
                let mut expected = expectations.expected_error.take().unwrap_or_default();
                expected.kind = Some(kind.to_string());
                expectations.expected_error = Some(expected);
            }
            continue;
        }
        if trimmed.starts_with("--error_contains") {
            // --error_contains <substring...>
            // Use the remainder of the line after the directive as the needle.
            let needle = trimmed
                .strip_prefix("--error_contains")
                .unwrap()
                .trim()
                .trim_matches('"');
            if !needle.is_empty() {
                let mut expected = expectations.expected_error.take().unwrap_or_default();
                expected.contains = Some(needle.to_string());
                expectations.expected_error = Some(expected);
            }
            continue;
        }
        if trimmed.starts_with("--affected_rows") {
            // --affected_rows <n>
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            expectations.expected_affected_rows = parts.get(1).and_then(|s| s.parse::<u64>().ok());
            continue;
        }
        if trimmed.starts_with("--rows") {
            // --rows <n>
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            expectations.expected_rows = parts.get(1).and_then(|s| s.parse::<usize>().ok());
            continue;
        }

        // Track start line for the next statement chunk.
        if current_sql.trim().is_empty() {
            start_line = line_number;
        }

        // Accumulate and split statements on semicolons outside quotes/comments.
        let mut chars = trimmed.chars().peekable();
        let mut prev_is_whitespace = true; // trimmed => start-of-line behaves like whitespace
        while let Some(ch) = chars.next() {
            if in_block_comment {
                if ch == '*' && chars.peek() == Some(&'/') {
                    chars.next(); // consume '/'
                    in_block_comment = false;
                }
                continue;
            }

            // Start of block comment
            if !in_single_quote
                && !in_double_quote
                && !in_backtick
                && ch == '/'
                && chars.peek() == Some(&'*')
            {
                chars.next(); // consume '*'
                in_block_comment = true;
                continue;
            }

            // Start of line comment (# or -- )
            if !in_single_quote && !in_double_quote && !in_backtick {
                if ch == '#' {
                    break;
                }
                if ch == '-' && chars.peek() == Some(&'-') {
                    // mysql requires whitespace or end after `--`
                    let mut lookahead = chars.clone();
                    lookahead.next(); // second '-'
                    if prev_is_whitespace
                        && (lookahead.peek().is_none()
                            || lookahead.peek().is_some_and(|c| c.is_whitespace()))
                    {
                        // consume second '-'
                        chars.next();
                        break;
                    }
                }
            }

            // Quote tracking (handles doubled quotes/backticks and backslash escapes).
            match ch {
                '\'' if !in_double_quote && !in_backtick => {
                    if in_single_quote {
                        if chars.peek() == Some(&'\'') {
                            // Escaped quote via doubling: ''.
                            current_sql.push('\'');
                            chars.next();
                        } else {
                            in_single_quote = false;
                            current_sql.push('\'');
                        }
                    } else {
                        in_single_quote = true;
                        current_sql.push('\'');
                    }
                    continue;
                }
                '"' if !in_single_quote && !in_backtick => {
                    if in_double_quote {
                        if chars.peek() == Some(&'"') {
                            current_sql.push('"');
                            chars.next();
                        } else {
                            in_double_quote = false;
                            current_sql.push('"');
                        }
                    } else {
                        in_double_quote = true;
                        current_sql.push('"');
                    }
                    continue;
                }
                '`' if !in_single_quote && !in_double_quote => {
                    if in_backtick {
                        if chars.peek() == Some(&'`') {
                            current_sql.push('`');
                            chars.next();
                        } else {
                            in_backtick = false;
                            current_sql.push('`');
                        }
                    } else {
                        in_backtick = true;
                        current_sql.push('`');
                    }
                    continue;
                }
                '\\' if in_single_quote || in_double_quote => {
                    // Preserve escapes; don't interpret semicolons after a backslash specially.
                    current_sql.push('\\');
                    if let Some(next) = chars.next() {
                        current_sql.push(next);
                    }
                    continue;
                }
                ';' if !in_single_quote && !in_double_quote && !in_backtick => {
                    let sql = current_sql.trim().to_string();
                    if !sql.is_empty() {
                        statements.push(Statement {
                            sql,
                            expectations: expectations.clone(),
                            line_number: start_line,
                        });
                    }
                    current_sql.clear();
                    expectations = Expectations::default();
                    // Next statement (if any) starts on the same line.
                    start_line = line_number;
                    continue;
                }
                _ => {}
            }

            current_sql.push(ch);
            prev_is_whitespace = ch.is_whitespace();
        }

        // Ensure tokens across lines don't get concatenated.
        if !current_sql.is_empty() {
            current_sql.push(' ');
        }
    }

    // Handle any remaining SQL without semicolon.
    let tail = current_sql.trim();
    if !tail.is_empty() {
        statements.push(Statement {
            sql: tail.to_string(),
            expectations,
            line_number: start_line,
        });
    }

    Ok(statements)
}

fn tisql_error_kind(err: &TiSqlError) -> String {
    match err {
        TiSqlError::Storage(_) => "Storage".into(),
        TiSqlError::Log(_) => "Log".into(),
        TiSqlError::Catalog(_) => "Catalog".into(),
        TiSqlError::Transaction(_) => "Transaction".into(),
        TiSqlError::Parse(_) => "Parse".into(),
        TiSqlError::Bind(_) => "Bind".into(),
        TiSqlError::Execution(_) => "Execution".into(),
        TiSqlError::TableNotFound(_) => "TableNotFound".into(),
        TiSqlError::ColumnNotFound(_) => "ColumnNotFound".into(),
        TiSqlError::TypeMismatch { .. } => "TypeMismatch".into(),
        TiSqlError::DuplicateKey(_) => "DuplicateKey".into(),
        TiSqlError::TransactionConflict => "TransactionConflict".into(),
        TiSqlError::TransactionAborted => "TransactionAborted".into(),
        TiSqlError::Io(_) => "Io".into(),
        TiSqlError::Internal(_) => "Internal".into(),
        TiSqlError::Codec(_) => "Codec".into(),
    }
}

/// Best-effort mapping from TiSqlError to MySQL-style error codes for mysql-test cases.
fn mysql_error_code(err: &TiSqlError) -> u32 {
    match err {
        TiSqlError::TableNotFound(_) => 1146,    // ER_NO_SUCH_TABLE
        TiSqlError::ColumnNotFound(_) => 1054,   // ER_BAD_FIELD_ERROR
        TiSqlError::DuplicateKey(_) => 1062,     // ER_DUP_ENTRY
        TiSqlError::Parse(_) => 1064,            // ER_PARSE_ERROR
        TiSqlError::TypeMismatch { .. } => 1366, // ER_TRUNCATED_WRONG_VALUE_FOR_FIELD (approx)

        // Heuristics for Catalog/Execution errors.
        TiSqlError::Catalog(msg) => {
            if msg.to_lowercase().contains("already exists") {
                1050 // ER_TABLE_EXISTS_ERROR
            } else {
                1105 // ER_UNKNOWN_ERROR
            }
        }
        TiSqlError::Execution(_) | TiSqlError::Bind(_) => 1105, // ER_UNKNOWN_ERROR

        // Default fallback.
        _ => 1105, // ER_UNKNOWN_ERROR
    }
}

fn format_output_mismatch(expected: &str, output: &str) -> String {
    let output_lines: Vec<&str> = output.lines().collect();
    let expected_lines: Vec<&str> = expected.lines().collect();

    let mut diff_line = 0usize;
    for (i, (out, exp)) in output_lines.iter().zip(expected_lines.iter()).enumerate() {
        if out != exp {
            diff_line = i + 1;
            break;
        }
    }

    if diff_line == 0 && output_lines.len() != expected_lines.len() {
        diff_line = output_lines.len().min(expected_lines.len()) + 1;
    }

    let exp_line = expected_lines
        .get(diff_line.saturating_sub(1))
        .copied()
        .unwrap_or("<EOF>");
    let out_line = output_lines
        .get(diff_line.saturating_sub(1))
        .copied()
        .unwrap_or("<EOF>");

    format!(
        "Output mismatch at line {}. Expected {} lines, got {} lines.\nexpected: {}\nactual  : {}",
        diff_line,
        expected_lines.len(),
        output_lines.len(),
        exp_line,
        out_line
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_supports_multiple_statements_per_line() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        fs::write(
            tmp.path(),
            "CREATE TABLE t (id INT); INSERT INTO t VALUES (1); SELECT id FROM t;",
        )
        .unwrap();

        let stmts = parse_test_file(tmp.path()).unwrap();
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[0].sql, "CREATE TABLE t (id INT)");
        assert_eq!(stmts[1].sql, "INSERT INTO t VALUES (1)");
        assert_eq!(stmts[2].sql, "SELECT id FROM t");
    }

    #[test]
    fn parse_does_not_split_on_semicolon_in_string() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        fs::write(
            tmp.path(),
            "CREATE TABLE t (v VARCHAR(10)); INSERT INTO t VALUES ('a;b'); SELECT v FROM t;",
        )
        .unwrap();

        let stmts = parse_test_file(tmp.path()).unwrap();
        assert_eq!(stmts.len(), 3);
        assert_eq!(stmts[1].sql, "INSERT INTO t VALUES ('a;b')");
    }

    #[test]
    fn directives_apply_to_next_statement_only() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        fs::write(tmp.path(), "--rows 1\nSELECT 1;\nSELECT 2;").unwrap();

        let stmts = parse_test_file(tmp.path()).unwrap();
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0].expectations.expected_rows, Some(1));
        assert_eq!(stmts[1].expectations.expected_rows, None);
    }

    #[test]
    fn error_code_mapping_covers_common_cases() {
        assert_eq!(
            mysql_error_code(&TiSqlError::TableNotFound("t".into())),
            1146
        );
        assert_eq!(
            mysql_error_code(&TiSqlError::ColumnNotFound("c".into())),
            1054
        );
        assert_eq!(mysql_error_code(&TiSqlError::Parse("x".into())), 1064);
    }
}
