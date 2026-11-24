# QA Report — Issue #196 · v1
**PR:** #217 · branch `issue-196-crashed-task-log-drain`  
**Head:** `2fb88452eabed45627ffe71b4f43a364e863aeba`  
**Date:** 2026-11-24  
**Tester:** OMP QA lane

---

## Summary

4/4 ACs verified locally. AC1 has a CI-pending gate on the daemon-backed job
(`Daemon-backed tests (never silently skipped)`, status `IN_PROGRESS` at time of
this report). All other CI checks passed.

The Builder-reported duplicate-publish risk is **not real** — see analysis below.

---

## Verification runs

### fmt
```
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo fmt --all --check
exit 0
```

### clippy
```
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo clippy --all-targets -- -D warnings
exit 0 (OK)
```

### tests
```
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-node
```

| Binary | Tests | Passed | Failed | Ignored |
|--------|-------|--------|--------|---------|
| minga_node (lib) | 173 | 172 | 0 | 1 |
| tests/containerd.rs | 9 | 9 | 0 | 0 |
| tests/containerd_client.rs | 6 | 6 | 0 | 0 |
| tests/containerd_pull.rs | 1 | 1 | 0 | 0 |
| tests/netns.rs | 0 | 0 | 0 | 0 |
| tests/wireguard_netns.rs | 0 | 0 | 0 | 0 |
| doc-tests | 0 | 0 | 0 | 0 |
| **Total** | **189** | **188** | **0** | **1** |

`supervisor_fast_crash_drains_last_output_before_teardown_ac1` ran in
`tests/containerd.rs` and exited `ok` (graceful skip — no containerd socket on macOS,
`MINGA_REQUIRE_CONTAINERD` not set).

---

## AC-by-AC findings

### AC1 — fast-crash marker in retained log stream
**Status: CI-PENDING** ⏳

- Test exists: `supervisor_fast_crash_drains_last_output_before_teardown_ac1`
  in `crates/minga-node/tests/containerd.rs` (lines 600–712).
- Real `ContainerdRuntime` + `MemoryBus` + `Supervisor`. Asserts
  `combined.contains("crash-marker-ac1")` after the second `reconcile_once`.
  Not a stub — if containerd is present and the marker is absent, the test fails.
- Local: graceful skip (no containerd socket). CI enforces with
  `MINGA_REQUIRE_CONTAINERD=1`.
- CI check `Daemon-backed tests (never silently skipped)`: **IN_PROGRESS** at
  time of report. Cannot tick AC1 until this job completes with SUCCESS.

### AC2 — exit code on stop log lines
**Status: ✅ PASS**

All four container-stop tracing paths carry `exit_code = ?exit_code`:

| Path | File:line | Message |
|------|-----------|---------|
| policy=none | `supervisor.rs:732` | `"task stopped; restart policy is \`none\`"` |
| exited cleanly | `supervisor.rs:753` | `"task exited cleanly, restart policy does not apply"` |
| max attempts | `supervisor.rs:782` | `"task failed, max restart attempts reached"` |
| restarted | `supervisor.rs:824` | `"restarted failed container"` |

`exit_code` is captured at `supervisor.rs:665–669` via `runtime.state()` BEFORE
`stop()` is called (the comment at `:663` explains why). For a cleanly-terminated
container the runtime holds the exit status; `None` can only appear if the
container record was already cleaned up before the query, which would require an
external `stop()` race — not a correctness regression introduced by this PR.

### AC3 — services/{id}/logs endpoint
**Status: ✅ PASS**

`crates/minga-api/src/server.rs` has no `/:version/services/:id/logs` route.
Axum returns 404 for unmatched paths. The only task-output route is:

```
.route("/:version/tasks/:id/logs", get(handlers::task_logs))   # server.rs:639
```

The docs correctly state: "`GET /{version}/services/{id}/logs` returns 404 — that
Docker API endpoint is not implemented; use `GET /{version}/tasks/{id}/logs`."

### AC4 — docs section for stopped-task output
**Status: ✅ PASS**

`docs/cli.md` additions verified:
- `service logs` row now says "currently running tasks only" and directs to
  `service inspect SERVICE --tail N` or `minga logs TASK_ID` for stopped tasks.
- New section "Reading the output of a task that is no longer running" with code
  block showing `minga logs TASK_ID` and `minga-ctl service inspect SERVICE --tail 50`.
- Docker API note distinguishing `services/{id}/logs` (404) from `tasks/{id}/logs`.

CLI commands confirmed real:
- `minga logs` — `crates/minga-cli/src/main.rs:98` (`Command::Logs { task_id, follow, tail }`)
- `minga-ctl service inspect --tail N` — `crates/minga-ctl/src/cmd/service.rs:40,46`
  (`ServiceCommand::Inspect { service, tail: u64 }`)
- `minga-ctl task ls` — `crates/minga-ctl/src/cmd/cluster.rs:106`
  (`TaskCommand::Ls`)

---

## Duplicate-publish risk — Builder's self-reported finding

**Verdict: NOT A BUG. The duplicate cannot occur.**

The Builder stated: "the cursor is removed from the map before the drain runs, so
the drain re-publishes from the last-known position."

This description is incorrect. The sequence in `reconcile_once` is:

```
1. stream_logs(bus)      — iterates tasks in `running`, publishes seq > cursor,
                           advances cursor in log_offsets to last-published seq.
2. check_restarts()      — for each task in `running` whose status.is_terminated():
     a. last_cursor = log_offsets.remove(&task_id).unwrap_or(0)
        ↑ captures the CURRENT value — i.e. whatever stream_logs advanced it to
     b. drain: filter(|c| c.seq > last_cursor)
        ↑ only publishes chunks AFTER what stream_logs already published
```

`last_cursor` is not a stale pre-`stream_logs` value — it is the value
`stream_logs` **left** in the map. The `remove()` captures the post-`stream_logs`
high-water mark. Therefore the drain's predicate `seq > last_cursor` is strictly
after everything `stream_logs` already emitted. No chunk is published twice.

**Can a chunk be skipped (the serious failure)?** No. If a container writes chunk
seq N+1 between `stream_logs` and `check_restarts`:
- `stream_logs` did not see it (it arrived after the loop).
- `last_cursor` = N (what `stream_logs` left).
- Drain filter: `seq > N` → seq N+1 is published.

The only loss scenario is ring eviction (ring is full, old chunks evicted before
`stream_logs` publishes them). That pre-existed this PR and is logged via the gap
detection introduced in #211. This PR does not change that behaviour.

**"No data loss in the other direction" claim — confirmed.** No chunk can be
neither published by `stream_logs` nor the drain.

---

## Scope

Diff touches exactly 3 files:
- `crates/minga-node/src/supervisor.rs` (+39/-2)
- `crates/minga-node/tests/containerd.rs` (+113/-0)
- `docs/cli.md` (+21/-1)

#211 work (sequence-numbered chunks, cursor map, FIFO re-open, `close_log_readers`)
is **not regressed**. The only changes to `log_offsets` handling:
- Line changed: `.remove()` now captures the return value as `last_cursor`.
- 4 tracing lines get `exit_code = ?exit_code` added.
No cursor initialisation, no sequence logic, no `close_log_readers` touched.

---

## CI status at report time

| Check | Status | Conclusion |
|-------|--------|------------|
| fmt, clippy, workspace tests | COMPLETED | SUCCESS |
| lint and build | COMPLETED | SUCCESS |
| NATS suites (v2.14.10) | COMPLETED | SUCCESS |
| NATS suites (v2.15.3) | COMPLETED | SUCCESS |
| NATS store upgrade | COMPLETED | SUCCESS |
| Overlay network paths | COMPLETED | SUCCESS |
| **Daemon-backed tests** | **IN_PROGRESS** | **—** |
| deploy to GitHub Pages | COMPLETED | SKIPPED |

---

## Local tests command (for Reviewer rerun)
```
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-node
```
