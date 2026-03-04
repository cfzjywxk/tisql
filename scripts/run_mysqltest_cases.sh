#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "usage: run_mysqltest_cases.sh <case-timeout-seconds> <statement-timeout-seconds> <job-count> [name-filter]" >&2
    exit 2
fi

case_timeout_secs="$1"
stmt_timeout_secs="$2"
job_count="$3"
name_filter="${4:-}"

if [[ ! "$case_timeout_secs" =~ ^[1-9][0-9]*$ ]]; then
    echo "case-timeout-seconds must be a positive integer, got: $case_timeout_secs" >&2
    exit 2
fi

if [[ ! "$stmt_timeout_secs" =~ ^[1-9][0-9]*$ ]]; then
    echo "statement-timeout-seconds must be a positive integer, got: $stmt_timeout_secs" >&2
    exit 2
fi

if [[ ! "$job_count" =~ ^[1-9][0-9]*$ ]]; then
    echo "job-count must be a positive integer, got: $job_count" >&2
    exit 2
fi

base_dir="tests/integrationtest/t"
test_files=()
while IFS= read -r line; do
    test_files+=("$line")
done < <(find "$base_dir" -type f \( -name '*.test' -o -name '*.t' \) | sort)

if [[ ${#test_files[@]} -eq 0 ]]; then
    echo "no mysqltest files discovered under $base_dir" >&2
    exit 1
fi

test_names=()
for test_file in "${test_files[@]}"; do
    test_name="${test_file#$base_dir/}"
    test_name="${test_name%.test}"
    test_name="${test_name%.t}"
    if [[ -n "$name_filter" && "$test_name" != *"$name_filter"* ]]; then
        continue
    fi
    test_names+=("$test_name")
done

if [[ ${#test_names[@]} -eq 0 ]]; then
    echo "no mysqltest cases matched filter: $name_filter" >&2
    exit 1
fi

# Pre-build once so parallel case runs avoid serializing on cargo build locks.
discover_timeout_secs="$case_timeout_secs"
if (( discover_timeout_secs > 55 )); then
    discover_timeout_secs=55
fi
./scripts/run_with_timeout.sh "$discover_timeout_secs" cargo build --bin mysqltest-runner
runner_bin="target/debug/mysqltest-runner"
if [[ ! -x "$runner_bin" ]]; then
    echo "failed to locate mysqltest-runner binary at $runner_bin" >&2
    exit 1
fi

run_case() {
    local test_name="$1"
    echo "Running e2e case: $test_name"
    ./scripts/run_with_timeout.sh "$case_timeout_secs" \
        "$runner_bin" --test "$test_name" --statement-timeout-secs "$stmt_timeout_secs"
}

active_pids() {
    jobs -pr || true
}

count_active_jobs() {
    local pids
    pids="$(active_pids)"
    if [[ -z "$pids" ]]; then
        echo 0
    else
        printf '%s\n' "$pids" | wc -l | tr -d ' '
    fi
}

kill_active_jobs() {
    local pids
    pids="$(active_pids)"
    if [[ -n "$pids" ]]; then
        kill $pids 2>/dev/null || true
        wait || true
    fi
}

wait_for_any_job() {
    if (( BASH_VERSINFO[0] >= 4 )); then
        wait -n
        return
    fi

    local pids first_pid
    pids="$(active_pids)"
    if [[ -z "$pids" ]]; then
        return 0
    fi
    first_pid="${pids%%$'\n'*}"
    wait "$first_pid"
}

failed=0
for test_name in "${test_names[@]}"; do
    while (( $(count_active_jobs) >= job_count )); do
        if ! wait_for_any_job; then
            failed=1
            break 2
        fi
    done
    run_case "$test_name" &
done

if (( failed == 0 )); then
    while (( $(count_active_jobs) > 0 )); do
        if ! wait_for_any_job; then
            failed=1
            break
        fi
    done
fi

if (( failed != 0 )); then
    kill_active_jobs
    exit 1
fi
