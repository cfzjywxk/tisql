# Notes For Future Reference

This file captures useful context learned during development/review discussions.

## TiKV 1PC/Async-Commit Visibility vs Snapshot Reads

### The Concern

If a transaction commits with `commit_ts = 10` and another transaction reads with
`start_ts = 15`, the reader must observe the committed results.
A scary interleaving is:

- `lock_cf` is deleted first (so readers see no lock)
- `write_cf` is written later (so readers see no committed version yet)

Or, for multiple keys `k1,k2,k3` in one 1PC commit:

- `k1/k2` appear committed, `k3` not yet committed
- a reader at `start_ts = 15` sees an inconsistent partial state

### What Prevents It In TiKV

1) **No partial apply for a single 1PC command in a region**
- TiKV only uses 1PC when all mutations are within a single region (one Raft
  group).
- The apply step writes MVCC changes for all keys in that command as a single
  engine write, so readers should not observe "k1/k2 done, k3 pending" as a
  committed state.

2) **The real snapshot-isolation hazard is "time travel" commit_ts**
- Even if engine writes are atomic, snapshot isolation can be broken if a commit
  with `commit_ts < read_ts` becomes visible after a read at `read_ts` has
  already been served (e.g. async-commit/1PC lets a reader proceed without
  waiting on locks).
- That would allow a read at `ts=15` to miss a version that later appears with
  `commit_ts=10`, violating repeatable-read semantics.

3) **ConcurrencyManager: `max_ts` / `min_commit_ts`**
- TiKV tracks a store-level `max_ts`: the highest timestamp of reads that have
  been served (in the sense relevant to preventing later commits from landing
  "in the past").
- For async-commit/1PC, TiKV computes `min_commit_ts >= max_ts + 1` (also
  bounded by `start_ts + 1`, etc.).
- If the transaction's proposed `commit_ts` is `< min_commit_ts`, TiKV will not
  allow it to commit at that timestamp (it errors / forces retry with a larger
  commit_ts, or falls back to a non-1PC path depending on the exact flow).

### Summary

- **Atomic engine writes** (batch across CFs) avoid observing "lock removed but
  write missing" and avoid per-key partial visibility within one applied command.
- **`max_ts/min_commit_ts`** is the key additional mechanism that prevents
  "commit in the past" relative to already-served reads, preserving snapshot
  isolation/repeatable-read for async-commit/1PC.

