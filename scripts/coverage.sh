#!/bin/bash
# TiSQL Test Coverage Script
#
# Prerequisites:
#   cargo install cargo-llvm-cov
#
# Usage:
#   ./scripts/coverage.sh          # Generate HTML coverage report
#   ./scripts/coverage.sh --json   # Generate JSON coverage report
#   ./scripts/coverage.sh --lcov   # Generate LCOV coverage report

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if cargo-llvm-cov is installed
if ! command -v cargo-llvm-cov &> /dev/null; then
    echo -e "${RED}Error: cargo-llvm-cov is not installed${NC}"
    echo "Install it with: cargo install cargo-llvm-cov"
    exit 1
fi

OUTPUT_DIR="coverage"
FORMAT="html"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --json)
            FORMAT="json"
            shift
            ;;
        --lcov)
            FORMAT="lcov"
            shift
            ;;
        --clean)
            echo -e "${YELLOW}Cleaning coverage data...${NC}"
            cargo llvm-cov clean --workspace
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--json|--lcov|--clean]"
            exit 1
            ;;
    esac
done

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo -e "${GREEN}Running tests with coverage instrumentation...${NC}"

case $FORMAT in
    html)
        cargo llvm-cov --all-features --html --output-dir "$OUTPUT_DIR"
        echo -e "${GREEN}Coverage report generated: $OUTPUT_DIR/html/index.html${NC}"
        ;;
    json)
        cargo llvm-cov --all-features --json --output-path "$OUTPUT_DIR/coverage.json"
        echo -e "${GREEN}Coverage report generated: $OUTPUT_DIR/coverage.json${NC}"
        ;;
    lcov)
        cargo llvm-cov --all-features --lcov --output-path "$OUTPUT_DIR/coverage.lcov"
        echo -e "${GREEN}Coverage report generated: $OUTPUT_DIR/coverage.lcov${NC}"
        ;;
esac

# Show summary
echo ""
echo -e "${YELLOW}Coverage Summary:${NC}"
cargo llvm-cov --all-features --summary-only 2>/dev/null || true
