# QA Report — issue #150 · v1

**PR:** #215  
**Branch:** issue-150-nats-cluster-test-flake  
**Head:** 41a08ea6cd75eafb8484efb0f03074a3a44bae19  
**Date:** 2026-11-24  
**Tester:** super-qa (OMP)

---

## Summary

All 4 ACs pass. AC3 is CI-proven (20-run step passed on two NATS versions in CI run 45304714376).
The temporary 20-run CI step MUST be removed before merge — this is the Reviewer's check.

---

## fmt / clippy

- `cargo fmt --all --check` → exit 0 ✅
- `cargo clippy --all-targets -- -D warnings` → exit 0 ✅

---

## AC1 — nats-server stderr captured and printed on timeout

**Result: ✅ PASS**

Code path (`crates/minga-bus/tests/nats_cluster.rs`):
- Line 82: `stderr_file = tempfile::NamedTempFile::new()` — creates temp file
- Line 92–95: `File::options().write(true).open(stderr_file.path())` — opens write end
- Line 114: `.stderr(Stdio::from(stderr_write))` — child's stderr routed to the file (NOT `Stdio::null()`)
- Lines 164–170: on timeout, `std::fs::read_to_string(server.stderr_file.path())` reads **content**, then `eprintln!("--- nats-server[{i}] stderr ---\n{log}\n---")` prints content to test stderr

The child is NOT spawned with `Stdio::null()`. The capture is live. The timeout path prints content, not a filename.

Note: lines 83–90 open a read handle to the tempfile and immediately drop it — harmless, does not interfere with the write path.

---

## AC2 — ports held until each nats-server spawns

**Result: ✅ PASS**

Code path (`crates/minga-bus/tests/nats_cluster.rs`):
- Lines 57–64: 9 `TcpListener` slots created as `Vec<Option<TcpListener>>`, port numbers read back via `l.local_addr().port()` BEFORE any drop
- Lines 87–89: for server `index`, three slots (`[index]`, `[3+index]`, `[6+index]`) set to `None` immediately before `Command::new(&binary)...spawn()` on line 97–116
- Lines 100, 102, 108: freed port numbers passed to nats-server via `-p`, `-m`, `--cluster` args

Release is per-server and tightly scoped: the three listeners for server N are released immediately before server N spawns, and the OS-assigned port numbers are passed explicitly to nats-server.

---

## AC3 — 20 consecutive runs pass in CI

**Result: ✅ CI-PROVEN**

CI run 45304714376 — job "NATS suites (never silently skipped)":
- v2.14.10 — step "Cluster flake regression (20 consecutive runs)" → `conclusion: success`
- v2.15.3  — step "Cluster flake regression (20 consecutive runs)" → `conclusion: success`

⚠️ **The temporary 20-run CI step MUST be removed before this PR merges.**
This is the Reviewer's check to make. It is the proof of AC3 for this round; do not remove it now.

---

## AC4 — no production code changed

**Result: ✅ PASS**

`git diff origin/main -- crates/minga-bus/src/` → empty (exit 0)

Changed files:
- `crates/minga-bus/tests/nats_cluster.rs` — test file only
- `.github/workflows/ci.yml` — CI workflow only

No changes outside `crates/minga-bus/tests/` and `.github/workflows/ci.yml`.

---

## Local 5-run results

Command: `CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-bus --test nats_cluster`

| Run | Result | Time |
|-----|--------|------|
| 1   | ✅ ok  | 6.78s |
| 2   | ✅ ok  | 1.86s |
| 3   | ✅ ok  | 1.82s |
| 4   | ✅ ok  | 2.15s |
| 5   | ✅ ok  | 2.10s |

5/5 passed.

---

## Verdict: ✅ PASS — ready for Review
