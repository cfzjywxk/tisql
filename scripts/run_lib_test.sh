#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "usage: run_lib_test.sh <timeout-seconds> <thread-count> [name-filter]" >&2
    exit 2
fi

timeout_secs="$1"
thread_count="$2"
name_filter="${3:-}"

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
    ./scripts/run_with_timeout.sh "$discover_timeout_secs" cargo test --lib -- --list
)"

mapfile -t test_groups < <(
    TEST_LIST_OUTPUT="$list_output" python3 - "$name_filter" <<'PY'
import re
import sys
import os

name_filter = sys.argv[1]
prefix_groups = set()
exact_groups = set()

for raw in os.environ.get("TEST_LIST_OUTPUT", "").splitlines():
    line = raw.strip()
    m = re.match(r"^(.*): test$", line)
    if not m:
        continue

    test_name = m.group(1)
    if name_filter and name_filter not in test_name:
        continue

    parts = test_name.split("::")
    if "tests" in parts:
        idx = parts.index("tests")
        if idx == 0:
            # Root-level `tests::...` would be too broad as a cargo substring
            # filter. Keep these as exact test-name runs.
            exact_groups.add(test_name)
            continue
        else:
            group = "::".join(parts[: idx + 1])
    elif len(parts) >= 2:
        group = "::".join(parts[:2])
    else:
        group = parts[0]

    prefix_groups.add(group)

for group in sorted(prefix_groups):
    print(f"prefix\t{group}")
for group in sorted(exact_groups):
    print(f"exact\t{group}")
PY
)

if [[ ${#test_groups[@]} -eq 0 ]]; then
    if [[ -n "$name_filter" ]]; then
        echo "no lib tests matched filter: $name_filter" >&2
    else
        echo "failed to discover lib test groups" >&2
    fi
    exit 1
fi

for group_line in "${test_groups[@]}"; do
    group_mode="${group_line%%$'\t'*}"
    group_name="${group_line#*$'\t'}"

    if [[ "$group_mode" == "exact" ]]; then
        echo "Running lib test case: $group_name"
        ./scripts/run_with_timeout.sh "$timeout_secs" \
            cargo test --lib "$group_name" -- --exact --test-threads="$thread_count"
    else
        echo "Running lib test group: $group_name"
        ./scripts/run_with_timeout.sh "$timeout_secs" \
            cargo test --lib "$group_name" -- --test-threads="$thread_count"
    fi
done
