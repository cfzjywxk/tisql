#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
    echo "usage: run_store_test.sh <timeout-seconds> <log-level> <thread-count> [name-filter]" >&2
    exit 2
fi

timeout_secs="$1"
log_level="$2"
thread_count="$3"
name_filter="${4:-}"

if [[ ! "$timeout_secs" =~ ^[1-9][0-9]*$ ]]; then
    echo "timeout-seconds must be a positive integer, got: $timeout_secs" >&2
    exit 2
fi

if [[ ! "$thread_count" =~ ^[1-9][0-9]*$ ]]; then
    echo "thread-count must be a positive integer, got: $thread_count" >&2
    exit 2
fi

list_output="$(cargo test --test store_test -- --list)"
mapfile -t test_names < <(printf '%s\n' "$list_output" | sed -n 's/^\(.*\): test$/\1/p')

if [[ ${#test_names[@]} -eq 0 ]]; then
    echo "failed to discover store_test cases" >&2
    exit 1
fi

declare -A module_prefixes=()
declare -a persistence_cases=()

for test_name in "${test_names[@]}"; do
    if [[ -n "$name_filter" && "$test_name" != *"$name_filter"* ]]; then
        continue
    fi

    if [[ "$test_name" == persistence::* ]]; then
        persistence_cases+=("$test_name")
    else
        module_prefixes["${test_name%%::*}::"]=1
    fi
done

sorted_modules=()
if [[ ${#module_prefixes[@]} -gt 0 ]]; then
    mapfile -t sorted_modules < <(printf '%s\n' "${!module_prefixes[@]}" | sort)
fi

sorted_persistence=()
if [[ ${#persistence_cases[@]} -gt 0 ]]; then
    mapfile -t sorted_persistence < <(printf '%s\n' "${persistence_cases[@]}" | sort)
fi

if [[ ${#sorted_modules[@]} -eq 0 && ${#sorted_persistence[@]} -eq 0 ]]; then
    echo "no store_test cases matched filter: $name_filter" >&2
    exit 1
fi

for module_prefix in "${sorted_modules[@]}"; do
    echo "Running store_test module: $module_prefix"
    ./scripts/run_with_timeout.sh "$timeout_secs" \
        env RUST_LOG="$log_level" cargo test --test store_test "$module_prefix" -- \
        --test-threads="$thread_count"
done

for case_name in "${sorted_persistence[@]}"; do
    echo "Running store_test persistence case: $case_name"
    ./scripts/run_with_timeout.sh "$timeout_secs" \
        env RUST_LOG="$log_level" cargo test --test store_test "$case_name" -- \
        --test-threads="$thread_count" --nocapture
done
