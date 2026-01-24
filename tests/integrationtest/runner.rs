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
    expected_error: Option<i32>,
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
    println!("Passed: {}, Failed: {}", passed, failed);

    if failed > 0 {
        std::process::exit(1);
    }
}

fn find_test_case(config: &Config, name: &str) -> TestCase {
    let test_file = config.test_dir.join(format!("{}.test", name));
    let result_file = config.result_dir.join(format!("{}.result", name));

    if !test_file.exists() {
        eprintln!("Test file not found: {:?}", test_file);
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
    find_tests_recursive(&config.test_dir, &config.test_dir, &config.result_dir, &mut cases);
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
            } else if path.extension().map_or(false, |e| e == "test") {
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
                message: format!("Failed to parse test file: {}", e),
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
                if stmt.expected_error.is_some() {
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
                        // Header
                        output.push_str(&columns.join("\t"));
                        output.push('\n');

                        // Data rows
                        for row in &data {
                            output.push_str(&row.join("\t"));
                            output.push('\n');
                        }
                    }
                    QueryResult::Affected(_count) => {
                        // Don't output for DML, just like mysql-test
                    }
                    QueryResult::Ok => {
                        // Don't output for DDL
                    }
                }
            }
            Err(e) => {
                if let Some(expected_code) = stmt.expected_error {
                    // Error was expected
                    output.push_str(&format!("ERROR {}: {}\n", expected_code, e));
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
            fs::create_dir_all(parent).ok();
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
                    message: format!("Failed to write result: {}", e),
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
                message: format!("Failed to read result file: {}", e),
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
        // Find first difference
        let output_lines: Vec<&str> = output.lines().collect();
        let expected_lines: Vec<&str> = expected.lines().collect();

        let mut diff_line = 0;
        for (i, (out, exp)) in output_lines.iter().zip(expected_lines.iter()).enumerate() {
            if out != exp {
                diff_line = i + 1;
                break;
            }
        }

        if diff_line == 0 && output_lines.len() != expected_lines.len() {
            diff_line = output_lines.len().min(expected_lines.len()) + 1;
        }

        TestResult {
            name: test_case.name.clone(),
            passed: false,
            message: format!(
                "Output mismatch at line {}. Expected {} lines, got {} lines",
                diff_line,
                expected_lines.len(),
                output_lines.len()
            ),
        }
    }
}

fn parse_test_file(path: &Path) -> Result<Vec<Statement>, String> {
    let file = fs::File::open(path).map_err(|e| e.to_string())?;
    let reader = BufReader::new(file);

    let mut statements = Vec::new();
    let mut current_sql = String::new();
    let mut expected_error = None;
    let mut start_line = 0;
    let mut line_number = 0;

    for line in reader.lines() {
        line_number += 1;
        let line = line.map_err(|e| e.to_string())?;
        let trimmed = line.trim();

        // Skip empty lines and comments
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        // Handle --error directive
        if trimmed.starts_with("--error") {
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if parts.len() >= 2 {
                expected_error = parts[1].parse().ok();
            }
            continue;
        }

        // Track start line
        if current_sql.is_empty() {
            start_line = line_number;
        }

        // Accumulate SQL
        current_sql.push_str(trimmed);

        // Check if statement is complete (ends with ;)
        if trimmed.ends_with(';') {
            // Remove trailing semicolon for execution
            let sql = current_sql.trim_end_matches(';').to_string();

            statements.push(Statement {
                sql,
                expected_error,
                line_number: start_line,
            });

            current_sql.clear();
            expected_error = None;
        } else {
            current_sql.push(' ');
        }
    }

    // Handle any remaining SQL without semicolon
    if !current_sql.trim().is_empty() {
        statements.push(Statement {
            sql: current_sql.trim().to_string(),
            expected_error,
            line_number: start_line,
        });
    }

    Ok(statements)
}
