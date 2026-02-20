# TiSQL Makefile

.PHONY: all build test unit-test store-test storage-test failpoint-test e2e-test bench fmt clippy clean run help prepare quick-prepare

CPU_CORES ?= $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 4)
# Process-level test parallelism tuned by machine size. Keep integration/e2e
# more conservative than unit tests for deterministic runtime and lower
# contention under CI.
DEFAULT_CASE_JOB_COUNT ?= $(shell cores=$(CPU_CORES); jobs=$$((cores / 4)); if [ $$jobs -lt 1 ]; then jobs=1; fi; if [ $$jobs -gt 8 ]; then jobs=8; fi; echo $$jobs)
MEDIUM_CASE_JOB_COUNT ?= $(shell cores=$(CPU_CORES); jobs=$$((cores / 8)); if [ $$jobs -lt 1 ]; then jobs=1; fi; if [ $$jobs -gt 4 ]; then jobs=4; fi; echo $$jobs)
HEAVY_CASE_JOB_COUNT ?= $(shell cores=$(CPU_CORES); jobs=$$((cores / 16)); if [ $$jobs -lt 1 ]; then jobs=1; fi; if [ $$jobs -gt 2 ]; then jobs=2; fi; echo $$jobs)

UNIT_TEST_TIMEOUT_SECS ?= 55
UNIT_TEST_THREAD_COUNT ?= 2
UNIT_TEST_JOB_COUNT ?= $(DEFAULT_CASE_JOB_COUNT)
UNIT_TEST_FILTER ?=
STORE_TEST_CASE_TIMEOUT_SECS ?= 55
STORE_TEST_LOG_LEVEL ?= warn
STORE_TEST_JOB_COUNT ?= $(MEDIUM_CASE_JOB_COUNT)
STORE_TEST_FILTER ?=
PESSIMISTIC_TXN_TEST_TIMEOUT_SECS ?= 55
PESSIMISTIC_TXN_TEST_JOB_COUNT ?= $(HEAVY_CASE_JOB_COUNT)
PESSIMISTIC_TXN_TEST_FILTER ?=
STORAGE_TEST_TIMEOUT_SECS ?= 55
STORAGE_TEST_JOB_COUNT ?= $(MEDIUM_CASE_JOB_COUNT)
COMPACTION_BATCH_TIMEOUT_SECS ?= 55
COMPACTION_BATCH_JOB_COUNT ?= $(HEAVY_CASE_JOB_COUNT)
COMPACTION_CASE_TIMEOUT_SECS ?= 55
E2E_TEST_TIMEOUT_SECS ?= 55
E2E_TEST_CASE_JOB_COUNT ?= $(MEDIUM_CASE_JOB_COUNT)
E2E_TEST_FILTER ?=
E2E_STMT_TIMEOUT_SECS ?= 15
E2E_CASE_TIMEOUT_SECS ?= 55
BENCH_TIMEOUT_SECS ?= 55
BENCH_FILTER ?=

# Keep all timeout guards below one minute for fast fail detection.
# Faster local-iteration profile (smaller timeout guards, same segmented flow)
QUICK_UNIT_TEST_TIMEOUT_SECS ?= 45
QUICK_UNIT_TEST_JOB_COUNT ?= $(DEFAULT_CASE_JOB_COUNT)
QUICK_STORE_TEST_CASE_TIMEOUT_SECS ?= 45
QUICK_STORE_TEST_LOG_LEVEL ?= warn
QUICK_STORE_TEST_JOB_COUNT ?= $(MEDIUM_CASE_JOB_COUNT)
QUICK_PESSIMISTIC_TXN_TEST_TIMEOUT_SECS ?= 45
QUICK_PESSIMISTIC_TXN_TEST_JOB_COUNT ?= $(HEAVY_CASE_JOB_COUNT)
QUICK_STORAGE_TEST_TIMEOUT_SECS ?= 45
QUICK_STORAGE_TEST_JOB_COUNT ?= $(MEDIUM_CASE_JOB_COUNT)
QUICK_COMPACTION_BATCH_TIMEOUT_SECS ?= 45
QUICK_COMPACTION_BATCH_JOB_COUNT ?= $(HEAVY_CASE_JOB_COUNT)
QUICK_COMPACTION_CASE_TIMEOUT_SECS ?= 45
QUICK_E2E_TEST_CASE_JOB_COUNT ?= $(MEDIUM_CASE_JOB_COUNT)
QUICK_E2E_STMT_TIMEOUT_SECS ?= 15
QUICK_E2E_CASE_TIMEOUT_SECS ?= 45
QUICK_BENCH_TIMEOUT_SECS ?= 45

# Keep store_test deterministic and avoid runtime/thread explosion when the
# harness executes many tokio::test cases concurrently.
STORE_TEST_THREAD_COUNT ?= 1
PESSIMISTIC_TXN_TEST_THREAD_COUNT ?= 1
STORAGE_TEST_THREAD_COUNT ?= 1

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
	./scripts/run_lib_test.sh \
		$(UNIT_TEST_TIMEOUT_SECS) \
		$(UNIT_TEST_THREAD_COUNT) \
		"$(UNIT_TEST_FILTER)" \
		$(UNIT_TEST_JOB_COUNT)

# Run store tests (internal integration tests)
store-test:
	./scripts/run_store_test.sh \
		$(STORE_TEST_CASE_TIMEOUT_SECS) \
		$(STORE_TEST_LOG_LEVEL) \
		$(STORE_TEST_THREAD_COUNT) \
		"$(STORE_TEST_FILTER)" \
		$(STORE_TEST_JOB_COUNT)
	./scripts/run_integration_test_cases.sh \
		$(PESSIMISTIC_TXN_TEST_TIMEOUT_SECS) \
		pessimistic_txn_ported_test \
		$(PESSIMISTIC_TXN_TEST_THREAD_COUNT) \
		"$(PESSIMISTIC_TXN_TEST_FILTER)" \
		"" \
		$(PESSIMISTIC_TXN_TEST_JOB_COUNT)

# Run storage regression tests (ported RocksDB-style suites)
storage-test:
	./scripts/run_integration_test_cases.sh \
		$(STORAGE_TEST_TIMEOUT_SECS) \
		rocksdb_ported_tests \
		$(STORAGE_TEST_THREAD_COUNT) \
		"" \
		"" \
		$(STORAGE_TEST_JOB_COUNT)
	./scripts/run_integration_test_cases.sh \
		$(COMPACTION_BATCH_TIMEOUT_SECS) \
		rocksdb_ported_compaction_tests \
		$(STORAGE_TEST_THREAD_COUNT) \
		"" \
		"test_compaction_scheduler_automatic,test_multi_level_cascading,test_multi_level_delete_reinsert_compaction_correctness" \
		$(COMPACTION_BATCH_JOB_COUNT)
	./scripts/run_with_timeout.sh $(COMPACTION_CASE_TIMEOUT_SECS) \
		cargo test --test rocksdb_ported_compaction_tests test_compaction_scheduler_automatic -- --exact --test-threads=$(STORAGE_TEST_THREAD_COUNT)
	./scripts/run_with_timeout.sh $(COMPACTION_CASE_TIMEOUT_SECS) \
		cargo test --test rocksdb_ported_compaction_tests test_multi_level_cascading -- --exact --test-threads=$(STORAGE_TEST_THREAD_COUNT)
	./scripts/run_with_timeout.sh $(COMPACTION_CASE_TIMEOUT_SECS) \
		cargo test --test rocksdb_ported_compaction_tests test_multi_level_delete_reinsert_compaction_correctness -- --exact --test-threads=$(STORAGE_TEST_THREAD_COUNT)

# Run failpoint crash-recovery tests (opt-in; requires feature flag)
failpoint-test:
	./scripts/run_with_timeout.sh $(UNIT_TEST_TIMEOUT_SECS) cargo test --test storage_failpoint_test --features failpoints

# Run E2E tests (MySQL-test format)
e2e-test:
	./scripts/run_mysqltest_cases.sh \
		$(E2E_CASE_TIMEOUT_SECS) \
		$(E2E_STMT_TIMEOUT_SECS) \
		$(E2E_TEST_CASE_JOB_COUNT) \
		"$(E2E_TEST_FILTER)"

# Run deterministic performance-regression bench cases
bench:
	./scripts/run_bench.sh \
		$(BENCH_TIMEOUT_SECS) \
		"$(BENCH_FILTER)"

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

# Format, lint, run all tests, and run perf-regression benches.
prepare: fmt clippy test bench

# Fast local gate: same segmented pipeline with tighter timeout guards.
quick-prepare: fmt clippy
	$(MAKE) unit-test \
		UNIT_TEST_TIMEOUT_SECS=$(QUICK_UNIT_TEST_TIMEOUT_SECS) \
		UNIT_TEST_JOB_COUNT=$(QUICK_UNIT_TEST_JOB_COUNT)
	$(MAKE) store-test \
		STORE_TEST_CASE_TIMEOUT_SECS=$(QUICK_STORE_TEST_CASE_TIMEOUT_SECS) \
		STORE_TEST_LOG_LEVEL=$(QUICK_STORE_TEST_LOG_LEVEL) \
		STORE_TEST_JOB_COUNT=$(QUICK_STORE_TEST_JOB_COUNT) \
		PESSIMISTIC_TXN_TEST_TIMEOUT_SECS=$(QUICK_PESSIMISTIC_TXN_TEST_TIMEOUT_SECS) \
		PESSIMISTIC_TXN_TEST_JOB_COUNT=$(QUICK_PESSIMISTIC_TXN_TEST_JOB_COUNT)
	$(MAKE) storage-test \
		STORAGE_TEST_TIMEOUT_SECS=$(QUICK_STORAGE_TEST_TIMEOUT_SECS) \
		COMPACTION_BATCH_TIMEOUT_SECS=$(QUICK_COMPACTION_BATCH_TIMEOUT_SECS) \
		STORAGE_TEST_JOB_COUNT=$(QUICK_STORAGE_TEST_JOB_COUNT) \
		COMPACTION_BATCH_JOB_COUNT=$(QUICK_COMPACTION_BATCH_JOB_COUNT) \
		COMPACTION_CASE_TIMEOUT_SECS=$(QUICK_COMPACTION_CASE_TIMEOUT_SECS)
	$(MAKE) e2e-test \
		E2E_TEST_CASE_JOB_COUNT=$(QUICK_E2E_TEST_CASE_JOB_COUNT) \
		E2E_STMT_TIMEOUT_SECS=$(QUICK_E2E_STMT_TIMEOUT_SECS) \
		E2E_CASE_TIMEOUT_SECS=$(QUICK_E2E_CASE_TIMEOUT_SECS)
	$(MAKE) bench \
		BENCH_TIMEOUT_SECS=$(QUICK_BENCH_TIMEOUT_SECS)

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
	@echo "  bench        Run deterministic performance-regression bench cases"
	@echo "  fmt          Format code"
	@echo "  fmt-check    Check code formatting"
	@echo "  clippy       Run clippy linter"
	@echo "  prepare      Format, lint, run all tests, and run perf-regression benches"
	@echo "  quick-prepare Fast local gate with tighter timeout guards"
	@echo "  ci           Run all CI checks (format check, lint, all tests)"
	@echo "  clean        Clean build artifacts"
	@echo "  run          Run the server (debug)"
	@echo "  run-release  Run the server (release)"
	@echo "  help         Show this help"
