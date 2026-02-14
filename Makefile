# TiSQL Makefile

.PHONY: all build test unit-test store-test storage-test failpoint-test e2e-test fmt clippy clean run help prepare

UNIT_TEST_TIMEOUT_SECS ?= 1200
STORE_TEST_TIMEOUT_SECS ?= 1200
E2E_TEST_TIMEOUT_SECS ?= 900
E2E_STMT_TIMEOUT_SECS ?= 15

# Default target
all: build

# Build the project
build:
	cargo build

# Build release version
release:
	cargo build --release

# Run all tests
test: unit-test store-test storage-test e2e-test

# Run unit tests only
unit-test:
	./scripts/run_with_timeout.sh $(UNIT_TEST_TIMEOUT_SECS) cargo test --lib

# Run store tests (internal integration tests)
store-test:
	./scripts/run_with_timeout.sh $(STORE_TEST_TIMEOUT_SECS) cargo test --test store_test

# Run storage regression tests (ported RocksDB-style suites)
storage-test:
	./scripts/run_with_timeout.sh $(STORE_TEST_TIMEOUT_SECS) cargo test --test rocksdb_ported_tests
	./scripts/run_with_timeout.sh $(STORE_TEST_TIMEOUT_SECS) cargo test --test rocksdb_ported_compaction_tests

# Run failpoint crash-recovery tests (opt-in; requires feature flag)
failpoint-test:
	./scripts/run_with_timeout.sh $(UNIT_TEST_TIMEOUT_SECS) cargo test --test storage_failpoint_test --features failpoints

# Run E2E tests (MySQL-test format)
e2e-test:
	./scripts/run_with_timeout.sh $(E2E_TEST_TIMEOUT_SECS) \
		cargo run --bin mysqltest-runner -- --all --statement-timeout-secs $(E2E_STMT_TIMEOUT_SECS)

# Record E2E test results (use when adding new tests)
e2e-record:
	./scripts/run_with_timeout.sh $(E2E_TEST_TIMEOUT_SECS) \
		cargo run --bin mysqltest-runner -- --all --record --statement-timeout-secs $(E2E_STMT_TIMEOUT_SECS)

# Format code
fmt:
	cargo fmt

# Check formatting
fmt-check:
	cargo fmt -- --check

# Run clippy linter
clippy:
	cargo clippy --all-targets -- -D warnings

# Format, lint, and run all tests - run before pushing to pass CI
prepare: fmt clippy test

# Run all CI checks locally (same as prepare but with format check instead of format)
ci: fmt-check clippy test

# Clean build artifacts
clean:
	cargo clean

# Run the server
run:
	cargo run

# Run the server in release mode
run-release:
	cargo run --release

# Help
help:
	@echo "TiSQL Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build        Build the project (debug)"
	@echo "  release      Build the project (release)"
	@echo "  test         Run all tests (unit + store + storage + e2e)"
	@echo "  unit-test    Run unit tests only"
	@echo "  store-test   Run store tests only"
	@echo "  storage-test Run storage regression tests"
	@echo "  failpoint-test Run failpoint crash-recovery tests"
	@echo "  e2e-test     Run E2E tests only"
	@echo "  e2e-record   Record E2E test results"
	@echo "  fmt          Format code"
	@echo "  fmt-check    Check code formatting"
	@echo "  clippy       Run clippy linter"
	@echo "  prepare      Format, lint, and run all tests (run before push)"
	@echo "  ci           Run all CI checks (format check, lint, all tests)"
	@echo "  clean        Clean build artifacts"
	@echo "  run          Run the server (debug)"
	@echo "  run-release  Run the server (release)"
	@echo "  help         Show this help"
