#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 5 ]]; then
    echo "usage: run_store_test.sh <timeout-seconds> <log-level> <thread-count> [name-filter] [job-count]" >&2
    exit 2
fi

timeout_secs="$1"
log_level="$2"
thread_count="$3"
name_filter="${4:-}"
job_count="${5:-1}"

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
    ./scripts/run_with_timeout.sh "$discover_timeout_secs" cargo test --test store_test -- --list 2>&1
)"
store_test_bin="$(
    printf '%s\n' "$list_output" | sed -n 's/^.*Running .* (\(.*\))$/\1/p' | tail -n 1
)"
if [[ -z "$store_test_bin" || ! -x "$store_test_bin" ]]; then
    echo "failed to locate store_test binary from cargo --list output" >&2
    exit 1
fi

test_names=()
while IFS= read -r line; do
    test_names+=("$line")
done < <(printf '%s\n' "$list_output" | sed -n 's/^\(.*\): test$/\1/p')

if [[ ${#test_names[@]} -eq 0 ]]; then
    echo "failed to discover store_test cases" >&2
    exit 1
fi

module_prefixes=()
persistence_cases=()

for test_name in "${test_names[@]}"; do
    if [[ -n "$name_filter" && "$test_name" != *"$name_filter"* ]]; then
        continue
    fi

    if [[ "$test_name" == persistence::* ]]; then
        persistence_cases+=("$test_name")
    else
        module_prefixes+=("${test_name%%::*}::")
    fi
done

sorted_modules=()
if [[ ${#module_prefixes[@]} -gt 0 ]]; then
    while IFS= read -r line; do
        sorted_modules+=("$line")
    done < <(printf '%s\n' "${module_prefixes[@]}" | sort -u)
fi

sorted_persistence=()
if [[ ${#persistence_cases[@]} -gt 0 ]]; then
    while IFS= read -r line; do
        sorted_persistence+=("$line")
    done < <(printf '%s\n' "${persistence_cases[@]}" | sort)
fi

if [[ ${#sorted_modules[@]} -eq 0 && ${#sorted_persistence[@]} -eq 0 ]]; then
    echo "no store_test cases matched filter: $name_filter" >&2
    exit 1
fi

run_module() {
    local module_prefix="$1"
    echo "Running store_test module: $module_prefix"
    ./scripts/run_with_timeout.sh "$timeout_secs" \
        env RUST_LOG="$log_level" "$store_test_bin" "$module_prefix" --test-threads="$thread_count"
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
for module_prefix in "${sorted_modules[@]}"; do
    while (( $(count_active_jobs) >= job_count )); do
        if ! wait_for_any_job; then
            failed=1
            break 2
        fi
    done
    run_module "$module_prefix" &
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

for case_name in "${sorted_persistence[@]}"; do
    echo "Running store_test persistence case: $case_name"
    ./scripts/run_with_timeout.sh "$timeout_secs" \
        env RUST_LOG="$log_level" "$store_test_bin" "$case_name" \
        --exact --test-threads="$thread_count" --nocapture
done
