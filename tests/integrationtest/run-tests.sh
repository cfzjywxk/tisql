#!/bin/bash
# MySQL-test style integration test runner for TiSQL
#
# Usage:
#   ./run-tests.sh              # Run all tests
#   ./run-tests.sh -t crud/basic  # Run specific test
#   ./run-tests.sh -r crud/basic  # Record new results for test
#   ./run-tests.sh -r all        # Record all results

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( cd "$SCRIPT_DIR/../.." && pwd )"

cd "$PROJECT_DIR"

# Parse arguments
RECORD_MODE=""
TEST_NAME=""
RUN_ALL=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--test)
            TEST_NAME="$2"
            shift 2
            ;;
        -r|--record)
            RECORD_MODE="--record"
            if [[ "$2" == "all" ]]; then
                RUN_ALL="--all"
                shift
            else
                TEST_NAME="$2"
            fi
            shift
            ;;
        --all)
            RUN_ALL="--all"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [-t <test>] [-r <test|all>] [--all]"
            exit 1
            ;;
    esac
done

# Build the runner
echo "Building mysqltest-runner..."
cargo build --bin mysqltest-runner --release 2>/dev/null || cargo build --bin mysqltest-runner

# Run tests
if [[ -n "$TEST_NAME" ]]; then
    cargo run --bin mysqltest-runner -- --test "$TEST_NAME" $RECORD_MODE
elif [[ -n "$RUN_ALL" ]]; then
    cargo run --bin mysqltest-runner -- --all $RECORD_MODE
else
    cargo run --bin mysqltest-runner -- --all
fi
