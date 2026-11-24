# QA Report — Issue #219 · v1

**PR:** https://github.com/lithqube/minga/pull/221  
**Branch:** `issue-219-first-reconcile-gap-count`  
**Head:** `b895f44`  
**Tester run:** Nov 24, 10:39 CET  
**Result:** ✅ PASS — all 4 ACs verified

---

## Verification commands

```
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo fmt --all --check
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo clippy --all-targets -- -D warnings
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-node
```

## Test counts

| Binary | Tests | Result |
|---|---|---|
| minga-node (lib) | 174 passed, 1 ignored | ✅ |
| tests/node_grpc.rs | 9 passed | ✅ |
| tests/tasks_grpc.rs | 6 passed | ✅ |
| tests/containerd_pull.rs | 1 passed | ✅ |
| tests/netns.rs | 0 | ✅ |
| tests/wireguard_netns.rs | 0 | ✅ |
| doc-tests | 0 | ✅ |
| **Total** | **190 passed, 0 failed** across 7 binaries | ✅ |

---

## Scope check

Diff touches exactly one file: `crates/minga-node/src/supervisor.rs`  
`+132 / -1`: one production line changed, two test functions added.  
No changes to `crates/minga-node/src/containerd/runtime.rs` (PR #218 area).  
No changes to anything merged by PR #211 (gap detection infrastructure).

---

## AC1 — First reconcile on overflowed ring counts gap exactly once

**Test:** `supervisor::assignment_tests::first_reconcile_on_overflowed_ring_counts_gap`  
**Setup:** `LogRing` pre-populated with seqs 5..=9 (seqs 1–4 evicted); cursor = 0.  
**Dropped count:** `first.seq - *cursor - 1` = `5 - 0 - 1` = **4** (correct: `first.seq - 1` when cursor = 0).  
**Assert:** `gap_warn_count == 1` after first reconcile; `after_first == 5` (all 5 chunks ship).  
**Result:** ✅ `ok`

---

## AC2 — Gap not counted again on subsequent reconcile

**Test (pre-existing):** `supervisor::assignment_tests::seq_gap_is_not_republished_ac2`  
**Code analysis (cursor=0 path):** After the first gap pass cursor advances to 9 (last chunk seq). Next reconcile: `first.seq(5) > *cursor+1(10)` → `5 > 10` = false → warn cannot fire. Mathematically impossible to double-warn.  
**New test AC4 covers AC2 compat:** second reconcile with same overflowed ring data asserts `gap_warn_count` remains 1.  
**Result:** ✅ `ok` (both tests pass)

---

## AC3 — Fresh ring with seq==1, cursor==0 → NO warning

**Test:** `supervisor::assignment_tests::fresh_ring_no_gap_warning_ac3`  
**Setup:** ring with single chunk seq=1; cursor=0.  
**Condition:** `1 > 0 + 1` = `1 > 1` = false → warn suppressed.  
**Assert:** `gap_warn_count == 0`; `count == 1` (chunk ships).  
**Result:** ✅ `ok`

---

## AC4 — Test fails before fix (red-first proof)

Temporarily restored `&& *cursor > 0` guard at `supervisor.rs:907`.

```
running 1 test
test supervisor::assignment_tests::first_reconcile_on_overflowed_ring_counts_gap ... FAILED

thread '...' panicked at crates/minga-node/src/supervisor.rs:2755:9:
assertion `left == right` failed: first reconcile on overflowed ring: gap-warn must fire exactly once
  left: 0
 right: 1
```

Failure line: `supervisor.rs:2755`. Builder reported `2747`; difference = 8 comment lines added in the solution block at lines 899–906, shifting the assert. Same assertion, same failure.  
**Result:** ✅ confirmed red before fix, green after

---

## False-gap risk analysis

Concern: cursor=0 but ring NOT overflowed — could produce a spurious warning?

`LogRing::default()` always initializes `next_seq: 1`. Every container start (line 1069) and adopt (line 1284 in `containerd/runtime.rs`) creates a fresh ring. `next_seq` is never persisted or carried across restarts. Therefore:

- If cursor=0 and first.seq=1 → `1 > 1` = false → silent. ✓
- If cursor=0 and first.seq=5 → ring genuinely overflowed seqs 1–4. The warning is correct.
- There is **no path** where cursor=0 but first.seq > 1 without real eviction having occurred.

The guard removal introduces no false positives.

---

## fmt / clippy

- `cargo fmt --all --check`: ✅ exit 0
- `cargo clippy --all-targets -- -D warnings`: ✅ exit 0
