#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
    echo -e "${RED}Error: cargo-llvm-cov is not installed${NC}"
    echo "Install it with: cargo install cargo-llvm-cov"
    exit 1
fi

OUTPUT_DIR="coverage"
FORMAT="html"
RUN_GATE=1
SUMMARY_ONLY=0
GROUP_FILTER=""
MIN_COVERAGE="${COVERAGE_MIN:-90}"
JSON_PATH="$OUTPUT_DIR/coverage.json"
LCOV_PATH="$OUTPUT_DIR/coverage.lcov"

usage() {
    echo "Usage: $0 [--html|--json|--lcov] [--summary-only] [--no-gate] [--group <name>] [--min <percent>] [--clean]" >&2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --html)
            FORMAT="html"
            shift
            ;;
        --json)
            FORMAT="json"
            shift
            ;;
        --lcov)
            FORMAT="lcov"
            shift
            ;;
        --summary-only)
            SUMMARY_ONLY=1
            shift
            ;;
        --no-gate)
            RUN_GATE=0
            shift
            ;;
        --group)
            GROUP_FILTER="$2"
            shift 2
            ;;
        --min)
            MIN_COVERAGE="$2"
            shift 2
            ;;
        --clean)
            echo -e "${YELLOW}Cleaning coverage data...${NC}"
            cargo llvm-cov clean --workspace
            python3 - <<'PY'
from pathlib import Path
import shutil
path = Path('coverage')
if path.exists():
    shutil.rmtree(path)
PY
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

mkdir -p "$OUTPUT_DIR"

run_phase_suite() {
    cargo llvm-cov clean --workspace >/dev/null 2>&1
    echo -e "${GREEN}Running phase coverage suite (lib + store_test)...${NC}"
    cargo llvm-cov --no-report --all-features --lib --ignore-run-fail
    cargo llvm-cov --no-report --all-features --test store_test
}

run_group_suite() {
    case "$1" in
        executor|transaction|clog|tablet_engine)
            # Group mode filters reporting, but it must use the same workload as
            # the workflow-wide gate or the per-surface numbers become misleading.
            echo -e "${GREEN}Running phase coverage suite filtered to $1...${NC}"
            run_phase_suite
            ;;
        *)
            echo "Unknown coverage group: $1" >&2
            exit 2
            ;;
    esac
}

write_report() {
    case "$FORMAT" in
        html)
            cargo llvm-cov report --html --output-dir "$OUTPUT_DIR"
            echo -e "${GREEN}Coverage report generated: $OUTPUT_DIR/html/index.html${NC}"
            ;;
        json)
            cargo llvm-cov report --json --output-path "$JSON_PATH"
            echo -e "${GREEN}Coverage report generated: $JSON_PATH${NC}"
            ;;
        lcov)
            cargo llvm-cov report --lcov --output-path "$LCOV_PATH"
            echo -e "${GREEN}Coverage report generated: $LCOV_PATH${NC}"
            ;;
    esac
}

if [[ -n "$GROUP_FILTER" ]]; then
    cargo llvm-cov clean --workspace >/dev/null 2>&1
    run_group_suite "$GROUP_FILTER"
else
    run_phase_suite
fi

if [[ "$SUMMARY_ONLY" -eq 1 ]]; then
    cargo llvm-cov report --json --summary-only --output-path "$JSON_PATH"
else
    write_report
fi

if [[ "$RUN_GATE" -eq 1 && ( "$SUMMARY_ONLY" -eq 1 || "$FORMAT" != "json" ) ]]; then
    cargo llvm-cov report --json --summary-only --output-path "$JSON_PATH"
fi

if [[ "$RUN_GATE" -eq 1 ]]; then
    cargo llvm-cov report --lcov --output-path "$LCOV_PATH" >/dev/null

    echo
    echo -e "${YELLOW}Per-surface coverage gate (min ${MIN_COVERAGE}%):${NC}"
    python3 - "$LCOV_PATH" "$MIN_COVERAGE" "$GROUP_FILTER" <<'PY'
import sys
from pathlib import Path

lcov_path = Path(sys.argv[1])
min_coverage = float(sys.argv[2])
selected_group = sys.argv[3]

groups = {
    "executor": [
        "src/executor/simple.rs",
        "src/executor/simple_support.rs",
    ],
    "transaction": [
        "src/transaction/api.rs",
        "src/transaction/concurrency.rs",
        "src/transaction/service.rs",
        "src/transaction/txn_state_cache.rs",
    ],
    "clog": [
        "src/log/clog/group_buffer.rs",
        "src/log/clog/group_commit.rs",
        "src/log/clog/writer.rs",
        "src/log/clog/file.rs",
    ],
    "tablet_engine": [
        "src/tablet/mod.rs",
        "src/tablet/engine.rs",
        "src/tablet/manager.rs",
        "src/tablet/routed_storage.rs",
        "src/tablet/recovery.rs",
        "src/tablet/version.rs",
        "src/tablet/version_set.rs",
    ],
}


def ignored_test_lines(path: Path) -> set[int]:
    if not path.exists():
        return set()

    lines = path.read_text().splitlines()
    ignored: set[int] = set()
    idx = 0
    while idx < len(lines):
        if lines[idx].strip().startswith("#[cfg(test)]"):
            item = idx + 1
            while item < len(lines) and (
                not lines[item].strip() or lines[item].strip().startswith("#[")
            ):
                item += 1
            if item >= len(lines):
                break

            brace_depth = lines[item].count("{") - lines[item].count("}")
            ignored.add(item + 1)
            item += 1
            while item <= len(lines) and brace_depth > 0:
                ignored.add(item + 1)
                brace_depth += lines[item].count("{") - lines[item].count("}")
                item += 1
            idx = item
            continue
        idx += 1
    return ignored


file_hits: dict[str, dict[int, int]] = {}
current = None
for raw in lcov_path.read_text().splitlines():
    if raw.startswith("SF:"):
        current = raw[3:].replace("\\", "/")
        file_hits[current] = {}
    elif raw.startswith("DA:") and current:
        line_no, hits, *_ = raw[3:].split(",")
        file_hits[current][int(line_no)] = int(hits)
    elif raw == "end_of_record":
        current = None

if selected_group:
    groups = {selected_group: groups[selected_group]}

stats = {name: {"covered": 0, "count": 0} for name in groups}

for group_name, paths in groups.items():
    for rel_path in paths:
        rel_path = rel_path.replace("\\", "/")
        matched_hits = None
        for filename, hits in file_hits.items():
            if filename.endswith(rel_path):
                matched_hits = hits
                break
        if matched_hits is None:
            continue

        ignored = ignored_test_lines(Path(rel_path))
        for line_no, line_hits in matched_hits.items():
            if line_no in ignored:
                continue
            stats[group_name]["count"] += 1
            if line_hits > 0:
                stats[group_name]["covered"] += 1

failed = False
for group_name, data in stats.items():
    count = data["count"]
    covered = data["covered"]
    pct = 0.0 if count == 0 else (covered / count) * 100.0
    status = "PASS" if count > 0 and pct >= min_coverage else "FAIL"
    print(f"  {group_name:14} {pct:6.2f}% ({covered}/{count}) {status}")
    if count == 0 or pct < min_coverage:
        failed = True

if failed:
    sys.exit(1)
PY
else
    echo
    echo -e "${YELLOW}Coverage summary gate skipped (--no-gate).${NC}"
fi
