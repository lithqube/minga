# QA Report — issue #195 · v1

**Branch:** issue-195-cli-deploy-env-values  
**Head:** 38b76da  
**Date:** Nov 23, 23:54 CET  
**Tester:** QA lane (super-board)

## Verdict: ✅ PASS — all 4 ACs confirmed

---

## Build Verification

| Check | Result |
|---|---|
| `cargo fmt --all --check` | ✅ clean |
| `cargo clippy --all-targets -- -D warnings` | ✅ clean (0.52s) |
| `cargo test -p minga-cli` | ✅ 79 passed |
| `cargo test -p minga-ctl` | ✅ 190 passed |

---

## Acceptance Criteria

### AC1 — `minga deploy` output contains no env value; keys still listed
**Verified by source audit + test.**

- Fix location: `crates/minga-cli/src/main.rs:2552–2556` (`print_translated`).
- Before fix: `format!("{k}={v}")` — values emitted.
- After fix: `svc.env.keys().map(String::as_str).collect()` — keys only.
- Full `print_translated` function (lines 2507–2596) audited: no other stdout/eprintln path emits env values.
- Test `deploy_plan_hides_env_value_and_shows_key` asserts:
  - `!all.contains("xSECRET_xMARKER_x195x")` (value absent)
  - `stdout.contains("MINGA_TEST_SECRET_KEY")` (key present)
- Test passes on branch 38b76da (included in 79/79).

### AC2 — `minga-ctl stack deploy` output contains no env value
**Verified by call-site analysis.**

- `minga-ctl stack deploy` → `deploy()` at `crates/minga-ctl/src/cmd/stack.rs:220`.
- At line 277–295: spawns `minga deploy` as a child process via `tokio::process::Command`.
- The plan output is emitted by the child `minga` binary, which calls `print_translated` — the same fixed function.
- No separate env-printing code in `stack.rs`.
- minga-ctl test suite 190/190 passes.

### AC3 — Regression test: marker absent from stdout and stderr
**Red-first confirmed.**

Red-first run on merge-base `1d4d0af` (`/private/tmp/minga-base-195`):

```
FAILED: deploy_plan_hides_env_value_and_shows_key
env value leaked into deploy output (issue #195):
services:
  test.svc                 alpine:3                                 1 replica(s)
    env MINGA_STACK=test MINGA_TEST_SECRET_KEY=xSECRET_xMARKER_x195x
```

Test file: `crates/minga-cli/tests/deploy_plan_no_env_values.rs`  
Run command: `CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-cli`

On branch (`38b76da`): 79/79 passed — test `deploy_plan_hides_env_value_and_shows_key` is green.

### AC4 — CHANGELOG notes the change under Security
**Verified in `CHANGELOG.md` `[Unreleased]` section:**

```
### Security
- **`minga deploy` and `minga-ctl stack deploy` no longer print env values (#195), no upgrade action.**
  The deploy plan printed to stdout before each API call now lists env key names only. Values stay
  available in the Compose file and through the API; none appear in terminal scrollback, CI logs,
  or shell history.
```

---

## Leak-surface grep audit

No other env-value render paths found in the deploy output path:

- `crates/minga-cli/src/main.rs` — only `print_translated` at line 752; fixed.
- `crates/minga-ctl/src/cmd/stack.rs` — no independent env printing; delegates entirely to child `minga` process.
- `crates/minga-node/src/containerd/oci.rs:329` — `format!("{k}={v}")` for OCI container spec (runtime, not deploy stdout).
- `crates/minga-api/src/server.rs:2624` — test fixture JSON literal `["env.ADDED", ...]`, not a print path.
- `crates/minga-ctl/src/cmd/service.rs:1026` — comment: "values are not shown".
