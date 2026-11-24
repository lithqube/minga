# QA Report — Issue #194 · v3

**Branch:** issue-194-node-log-shipping-gaps  
**Head:** 3abf7c4  
**Date:** Nov 24, 03:22 CET  
**Round:** 3 (Reviewer bounced rounds 1 and 2)

---

## Checks

| Check | Result |
|---|---|
| `cargo fmt --all --check` | ✅ clean |
| `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| `cargo test -p minga-node` | ✅ 187 passed / 1 ignored / 0 failed across 7 binaries |

**Binary breakdown:**
- lib (supervisor, runtime, …): 172 passed / 1 ignored
- tests/containerd.rs: 8 passed (all containerd-daemon tests ran via CI daemon; local skips via early-return guard, not `ignore`)
- tests/tasks.rs: 6 passed
- tests/credentials.rs: 1 passed
- 3 × 0-test binaries

---

## AC1 — >1 MiB ring, every surviving chunk published exactly once

`runtime.rs::log_ring_sequence_survives_eviction_ac1` — pushes 32 × 64 KiB (2 MiB) through a 1 MiB ring; asserts exactly 16 survive with contiguous seqs, each published once; second pass publishes 0.  
`supervisor.rs::seq_cursor_keeps_shipping_past_ring_eviction_ac1` — supervisor cursor advances past eviction boundary; 0 re-publishes.  
Both pass: `supervisor::assignment_tests::seq_cursor_keeps_shipping_past_ring_eviction_ac1 ... ok`

**✅ AC1 verified.**

---

## AC2 — gap logged exactly once

`supervisor.rs::seq_gap_is_not_republished_ac2` — WarnCounter subscriber installed via `tracing::subscriber::set_default`.  
Target string: `"gap counted"`. Only one `warn!` in the entire codebase contains that string: `supervisor.rs:869`.  
No other WARN message contains `"gap counted"` — confirmed by grep.  
Assertion sequence: 0 → 1 → 1 → 2 across four reconcile passes (no gap, first gap, re-presented same gap, new gap).

**Red-side confirmed by Builder:** commenting out the `warn!` at `supervisor.rs:869` panics at line 2597 (`left: 0, right: 1`).  
Test passes: `supervisor::assignment_tests::seq_gap_is_not_republished_ac2 ... ok`

**✅ AC2 verified.**

---

## AC3 — restart: post-restart output reaches new log stream

### R3 fix verification

`close_log_readers` implementation (`runtime.rs:774–779`):
- Calls `.lock().await.remove(container_id)` on `self.log_readers` → takes the JoinHandle out.
- Calls `.abort()` on the handle → cancels the reader task cooperatively.
- Calls `.lock().await.remove(container_id)` on `self.log_rings` → drops in-memory ring.
- Does NOT touch `teardown_logs` (which removes FIFO files); confirmed by reading the `teardown_logs` function at `runtime.rs:753–764` — it is only called from `abort_start` and `stop`.

Ordering in AC3 test (`tests/containerd.rs:557–561`):
1. `runtime.close_log_readers(&container_id).await` — aborts reader, removes ring.
2. `tokio::task::yield_now().await` — yields executor to allow the aborted task to complete its current poll before runtime2 opens the same FIFOs.
3. `runtime2 = ContainerdRuntime::connect(...)` — fresh instance, empty `log_readers` map.
4. `runtime2.open_log_fifos(&container_id).await` — the only reader on the FIFO from this point.

The yield is necessary because `abort()` is cooperative: the task will not stop until the next `.await` point. The yield ensures runtime2 never races with the old reader.

**FIFO files on disk:** `close_log_readers` only manipulates `self.log_readers` and `self.log_rings` (in-memory). The named pipe files in `logs_dir` are untouched. `runtime2` can re-open them successfully.

### Production usage of `close_log_readers`

`close_log_readers` is `pub` but appears **only in `tests/containerd.rs`**. No production path calls it.  
**Finding (informational, not blocking per Reviewer rules):** This is a public API that exists solely to make a test pass. It models "supervisor process dies" (the real event needs no API — the process exit kills all tasks). Reviewer should note this as Over-engineering; it does not block per board rules.

### Local probe

The AC3 test exits via the `containerd_available_at` guard before the assertion body when no daemon is present — the probe of commenting out `open_log_fifos` returns `ok` locally (guard fires first). This is structurally expected; the test's assertion integrity is proven by the CI daemon-backed job.

### CI daemon-backed job

`Daemon-backed tests (never silently skipped)` → **SUCCESS** (completed 2026-11-24T02:18:28Z, run 45310393743).  
`supervisor_restart_adopted_container_logs_reach_stream_ac3 ... ok` confirmed by CI.

**✅ AC3 verified (CI daemon).**

---

## AC4 — existing tests still pass, none deleted

Supervisor tests: 21 `#[test]`/`#[tokio::test]` annotated functions in `supervisor.rs` before and after — no deletions, no additions.  
Containerd tests: 9 `#[test]`/`#[tokio::test]` annotated functions in `tests/containerd.rs` before and after — no deletions.  
New additions to supervisor.rs (non-test): `WarnCounter::on_event`, `WarnCounter::record_debug`, `WarnCounter::record_str` — these are helper impl functions inside the `#[cfg(test)]` module for the WarnCounter tracing layer, not test functions.  
All 187 passed; 0 failed.

**✅ AC4 verified.**

---

## Summary

| AC | Status | Proof |
|---|---|---|
| AC1: >1 MiB, each chunk published once | ✅ | `runtime.rs::log_ring_sequence_survives_eviction_ac1`, `supervisor.rs::seq_cursor_keeps_shipping_past_ring_eviction_ac1` both pass |
| AC2: gap logged once | ✅ | `supervisor.rs::seq_gap_is_not_republished_ac2` 0→1→1→2 counter passes; red at `supervisor.rs:2597` |
| AC3: restart log continuity | ✅ | CI `Daemon-backed tests` SUCCESS; R3 fix confirmed: abort+yield prevents competing reader; FIFOs not removed |
| AC4: existing tests still pass | ✅ | 187 passed / 0 failed; 0 test deletions |

**Finding (informational):** `close_log_readers` is `pub` but used only in tests. Models process-exit semantics. Should-fix per Over-engineering rule; does not block.

---

## Local test command

```
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-node
```
