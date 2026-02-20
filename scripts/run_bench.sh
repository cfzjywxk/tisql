#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "usage: run_bench.sh <timeout-seconds> [name-filter]" >&2
    exit 2
fi

timeout_secs="$1"
name_filter="${2:-}"

if [[ ! "$timeout_secs" =~ ^[1-9][0-9]*$ ]]; then
    echo "timeout-seconds must be a positive integer, got: $timeout_secs" >&2
    exit 2
fi

bench_cases=(
    "tablet::engine::tests::test_point_get_from_sst_reads_few_blocks"
    "tablet::engine::tests::test_point_get_from_sst_bloom_path_hits_block_cache"
    "tablet::engine::tests::test_point_get_from_sst_scale_regression_guard"
)

selected_cases=()
for case_name in "${bench_cases[@]}"; do
    if [[ -n "$name_filter" && "$case_name" != *"$name_filter"* ]]; then
        continue
    fi
    selected_cases+=("$case_name")
done

if [[ ${#selected_cases[@]} -eq 0 ]]; then
    echo "no bench cases matched filter: $name_filter" >&2
    exit 1
fi

for case_name in "${selected_cases[@]}"; do
    echo "Running bench case: $case_name"
    ./scripts/run_with_timeout.sh "$timeout_secs" \
        cargo test --lib "$case_name" -- --exact --test-threads=1
done
