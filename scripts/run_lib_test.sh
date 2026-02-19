#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
    echo "usage: run_lib_test.sh <timeout-seconds> <thread-count> [name-filter] [job-count]" >&2
    exit 2
fi

timeout_secs="$1"
thread_count="$2"
name_filter="${3:-}"
job_count="${4:-1}"

if [[ ! "$timeout_secs" =~ ^[1-9][0-9]*$ ]]; then
    echo "timeout-seconds must be a positive integer, got: $timeout_secs" >&2
    exit 2
fi

if [[ ! "$thread_count" =~ ^[1-9][0-9]*$ ]]; then
    echo "thread-count must be a positive integer, got: $thread_count" >&2
    exit 2
fi

if [[ ! "$job_count" =~ ^[1-9][0-9]*$ ]]; then
    echo "job-count must be a positive integer, got: $job_count" >&2
    exit 2
fi

discover_timeout_secs="$timeout_secs"
if (( discover_timeout_secs > 55 )); then
    discover_timeout_secs=55
fi

list_output="$(
    ./scripts/run_with_timeout.sh "$discover_timeout_secs" cargo test --lib -- --list 2>&1
)"

lib_test_bin="$(
    printf '%s\n' "$list_output" | sed -n 's/^.*Running unittests .* (\(.*\))$/\1/p' | tail -n 1
)"
if [[ -z "$lib_test_bin" || ! -x "$lib_test_bin" ]]; then
    echo "failed to locate lib test binary from cargo --list output" >&2
    exit 1
fi

mapfile -t test_groups < <(
    TEST_LIST_OUTPUT="$list_output" python3 - "$name_filter" <<'PY'
import os
import re
import sys

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

run_group() {
    local mode="$1"
    local name="$2"
    if [[ "$mode" == "exact" ]]; then
        echo "Running lib test case: $name"
        ./scripts/run_with_timeout.sh "$timeout_secs" \
            "$lib_test_bin" "$name" --exact --test-threads="$thread_count"
    else
        echo "Running lib test group: $name"
        ./scripts/run_with_timeout.sh "$timeout_secs" \
            "$lib_test_bin" "$name" --test-threads="$thread_count"
    fi
}

count_active_jobs() {
    local pids=()
    mapfile -t pids < <(jobs -pr || true)
    echo "${#pids[@]}"
}

kill_active_jobs() {
    local pids=()
    mapfile -t pids < <(jobs -pr || true)
    if [[ ${#pids[@]} -gt 0 ]]; then
        kill "${pids[@]}" 2>/dev/null || true
        wait || true
    fi
}

failed=0
for group_line in "${test_groups[@]}"; do
    group_mode="${group_line%%$'\t'*}"
    group_name="${group_line#*$'\t'}"

    while (( $(count_active_jobs) >= job_count )); do
        if ! wait -n; then
            failed=1
            break 2
        fi
    done

    run_group "$group_mode" "$group_name" &
done

if (( failed == 0 )); then
    while (( $(count_active_jobs) > 0 )); do
        if ! wait -n; then
            failed=1
            break
        fi
    done
fi

if (( failed != 0 )); then
    kill_active_jobs
    exit 1
fi
