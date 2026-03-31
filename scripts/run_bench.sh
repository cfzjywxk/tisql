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

bench_targets=(
    "sql_executor"
    "transaction_service"
    "clog_service"
    "tablet_engine"
)

selected_targets=()
if [[ -n "$name_filter" ]]; then
    for target in "${bench_targets[@]}"; do
        if [[ "$target" == *"$name_filter"* ]]; then
            selected_targets+=("$target")
        fi
    done
fi

if [[ ${#selected_targets[@]} -eq 0 ]]; then
    selected_targets=("${bench_targets[@]}")
fi

bench_log_level="${TISQL_LOG:-${RUST_LOG:-warn}}"

for target in "${selected_targets[@]}"; do
    echo "Running bench target: $target"
    cmd=(env "TISQL_LOG=$bench_log_level" "RUST_LOG=${RUST_LOG:-$bench_log_level}" cargo bench --bench "$target" -- --noplot)
    if [[ -n "$name_filter" ]]; then
        cmd+=("$name_filter")
    fi
    ./scripts/run_with_timeout.sh "$timeout_secs" "${cmd[@]}"
done
