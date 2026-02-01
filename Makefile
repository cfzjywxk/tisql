# TiSQL Makefile

.PHONY: all build test unit-test store-test e2e-test fmt clippy clean run help prepare

# Default target
all: build

# Build the project
build:
	cargo build

# Build release version
release:
	cargo build --release

# Run all tests
test: unit-test store-test e2e-test

# Run unit tests only
unit-test:
	cargo test --lib

# Run store tests (internal integration tests)
store-test:
	cargo test --test store_test

# Run E2E tests (MySQL-test format)
e2e-test:
	cargo run --bin mysqltest-runner -- --all

# Record E2E test results (use when adding new tests)
e2e-record:
	cargo run --bin mysqltest-runner -- --all --record

# Format code
fmt:
	cargo fmt

# Check formatting
fmt-check:
	cargo fmt -- --check

# Run clippy linter
clippy:
	cargo clippy --all-targets -- -D warnings

# Format and lint - run before pushing to pass CI
prepare:
	cargo fmt && cargo clippy --all-targets -- -D warnings

# Run all CI checks locally
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
	@echo "  test         Run all tests (unit + store + e2e)"
	@echo "  unit-test    Run unit tests only"
	@echo "  store-test   Run store tests only"
	@echo "  e2e-test     Run E2E tests only"
	@echo "  e2e-record   Record E2E test results"
	@echo "  fmt          Format code"
	@echo "  fmt-check    Check code formatting"
	@echo "  clippy       Run clippy linter"
	@echo "  prepare      Format and lint (run before push)"
	@echo "  ci           Run all CI checks locally"
	@echo "  clean        Clean build artifacts"
	@echo "  run          Run the server (debug)"
	@echo "  run-release  Run the server (release)"
	@echo "  help         Show this help"
