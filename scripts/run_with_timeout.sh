#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "usage: run_with_timeout.sh <timeout-seconds> <command> [args...]" >&2
    exit 2
fi

timeout_secs="$1"
shift

if [[ ! "$timeout_secs" =~ ^[1-9][0-9]*$ ]]; then
    echo "timeout-seconds must be a positive integer, got: $timeout_secs" >&2
    exit 2
fi

# Keep timeout guards strict and fast-fail by default (< 1 minute).
max_timeout_secs="${TISQL_MAX_TIMEOUT_SECS:-55}"
if [[ ! "$max_timeout_secs" =~ ^[1-9][0-9]*$ ]]; then
    echo "TISQL_MAX_TIMEOUT_SECS must be a positive integer, got: $max_timeout_secs" >&2
    exit 2
fi
if (( timeout_secs > max_timeout_secs )); then
    echo "timeout-seconds must be <= ${max_timeout_secs}, got: $timeout_secs" >&2
    exit 2
fi

kill_after_secs="${TISQL_TIMEOUT_KILL_AFTER_SECS:-5}"
if [[ ! "$kill_after_secs" =~ ^[1-9][0-9]*$ ]]; then
    echo "TISQL_TIMEOUT_KILL_AFTER_SECS must be a positive integer, got: $kill_after_secs" >&2
    exit 2
fi

if command -v timeout >/dev/null 2>&1; then
    set +e
    timeout --signal=TERM --kill-after="${kill_after_secs}s" "${timeout_secs}s" "$@"
    status=$?
    set -e

    if [[ $status -eq 124 ]]; then
        echo "ERROR: command timed out after ${timeout_secs}s: $*" >&2
    elif [[ $status -eq 137 ]]; then
        echo "ERROR: command killed after timeout grace period: $*" >&2
    fi

    exit $status
fi

echo "WARN: 'timeout' command not found, running without timeout guard." >&2
exec "$@"
