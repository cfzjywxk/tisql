# TiSQL Makefile

.PHONY: all build test unit-test store-test storage-test failpoint-test e2e-test fmt clippy clean run help prepare

UNIT_TEST_TIMEOUT_SECS ?= 1200
STORE_TEST_TIMEOUT_SECS ?= 1200
PESSIMISTIC_TXN_TEST_TIMEOUT_SECS ?= 300
STORAGE_TEST_TIMEOUT_SECS ?= 1200
COMPACTION_BATCH_TIMEOUT_SECS ?= 300
COMPACTION_CASE_TIMEOUT_SECS ?= 240
E2E_TEST_TIMEOUT_SECS ?= 900
E2E_STMT_TIMEOUT_SECS ?= 15
E2E_CASE_TIMEOUT_SECS ?= 180

UNAME_S := $(shell uname -s)

# macOS has a lower default file descriptor limit; running store_test in
# parallel can exhaust descriptors because each test creates multiple runtimes.
ifeq ($(UNAME_S),Darwin)
STORE_TEST_THREAD_ARGS := -- --test-threads=1
endif

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
	./scripts/run_with_timeout.sh $(STORE_TEST_TIMEOUT_SECS) cargo test --test store_test $(STORE_TEST_THREAD_ARGS)
	./scripts/run_with_timeout.sh $(PESSIMISTIC_TXN_TEST_TIMEOUT_SECS) cargo test --test pessimistic_txn_ported_test

# Run storage regression tests (ported RocksDB-style suites)
storage-test:
	./scripts/run_with_timeout.sh $(STORAGE_TEST_TIMEOUT_SECS) cargo test --test rocksdb_ported_tests
	./scripts/run_with_timeout.sh $(COMPACTION_BATCH_TIMEOUT_SECS) \
		cargo test --test rocksdb_ported_compaction_tests -- --test-threads=1 \
		--skip test_compaction_scheduler_automatic \
		--skip test_multi_level_cascading \
		--skip test_multi_level_delete_reinsert_compaction_correctness
	./scripts/run_with_timeout.sh $(COMPACTION_CASE_TIMEOUT_SECS) \
		cargo test --test rocksdb_ported_compaction_tests test_compaction_scheduler_automatic -- --test-threads=1
	./scripts/run_with_timeout.sh $(COMPACTION_CASE_TIMEOUT_SECS) \
		cargo test --test rocksdb_ported_compaction_tests test_multi_level_cascading -- --test-threads=1
	./scripts/run_with_timeout.sh $(COMPACTION_CASE_TIMEOUT_SECS) \
		cargo test --test rocksdb_ported_compaction_tests test_multi_level_delete_reinsert_compaction_correctness -- --test-threads=1

# Run failpoint crash-recovery tests (opt-in; requires feature flag)
failpoint-test:
	./scripts/run_with_timeout.sh $(UNIT_TEST_TIMEOUT_SECS) cargo test --test storage_failpoint_test --features failpoints

# Run E2E tests (MySQL-test format)
e2e-test:
	@set -e; \
	for test_file in $$(find tests/integrationtest/t -type f \( -name '*.test' -o -name '*.t' \) | sort); do \
		test_name=$${test_file#tests/integrationtest/t/}; \
		test_name=$${test_name%.test}; \
		test_name=$${test_name%.t}; \
		echo "Running e2e case: $$test_name"; \
		./scripts/run_with_timeout.sh $(E2E_CASE_TIMEOUT_SECS) \
			cargo run --bin mysqltest-runner -- --test $$test_name --statement-timeout-secs $(E2E_STMT_TIMEOUT_SECS); \
	done

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
