#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 5 ]]; then
    echo "usage: run_integration_test_cases.sh <timeout-seconds> <test-target> <thread-count> [name-filter] [skip-patterns-csv]" >&2
    exit 2
fi

timeout_secs="$1"
test_target="$2"
thread_count="$3"
name_filter="${4:-}"
skip_patterns_csv="${5:-}"

if [[ ! "$timeout_secs" =~ ^[1-9][0-9]*$ ]]; then
    echo "timeout-seconds must be a positive integer, got: $timeout_secs" >&2
    exit 2
fi

if [[ ! "$thread_count" =~ ^[1-9][0-9]*$ ]]; then
    echo "thread-count must be a positive integer, got: $thread_count" >&2
    exit 2
fi

discover_timeout_secs="$timeout_secs"
if (( discover_timeout_secs > 120 )); then
    discover_timeout_secs=120
fi

list_output="$(
    ./scripts/run_with_timeout.sh "$discover_timeout_secs" \
        cargo test --test "$test_target" -- --list
)"

mapfile -t case_names < <(
    TEST_LIST_OUTPUT="$list_output" python3 - "$name_filter" "$skip_patterns_csv" <<'PY'
import re
import sys
import os

name_filter = sys.argv[1]
skip_patterns = [p for p in sys.argv[2].split(",") if p]
cases = set()

for raw in os.environ.get("TEST_LIST_OUTPUT", "").splitlines():
    line = raw.strip()
    m = re.match(r"^(.*): test$", line)
    if not m:
        continue

    case = m.group(1)
    if name_filter and name_filter not in case:
        continue
    if any(pat in case for pat in skip_patterns):
        continue

    cases.add(case)

for case in sorted(cases):
    print(case)
PY
)

if [[ ${#case_names[@]} -eq 0 ]]; then
    if [[ -n "$name_filter" ]]; then
        echo "no $test_target cases matched filter: $name_filter" >&2
    else
        echo "failed to discover runnable cases for test target: $test_target" >&2
    fi
    exit 1
fi

for case_name in "${case_names[@]}"; do
    echo "Running $test_target case: $case_name"
    ./scripts/run_with_timeout.sh "$timeout_secs" \
        cargo test --test "$test_target" "$case_name" -- --exact --test-threads="$thread_count"
done
