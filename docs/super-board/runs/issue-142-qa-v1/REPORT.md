# QA Report — Issue #142 · v1

**Card:** #142 compose: hardening keys are parsed then dropped  
**PR:** #207 (draft)  
**Branch:** issue-142-compose-hardening-keys  
**Head verified:** `04f0d328e348fac5d36ac53ff2e6c331f5e6527c`  
**Merge-base:** `f7a2fc3` (docs: bring docs, CLI help and site in line with main)  
**Worktree:** `.claude/worktrees/issue-142-qa/`  
**QA date:** 2026-11-23  
**Result:** ✅ PASS — both ACs verified

---

## Acceptance Criteria

### AC1 ✅ `minga validate` (and the load path) on a file using any listed key fails with `Unsupported`

All 11 rejection tests pass on `04f0d32`:

| Test | Key | Result |
|---|---|---|
| `hardening_cap_add_is_rejected` | `cap_add` | ✅ |
| `hardening_cap_drop_is_rejected` | `cap_drop` | ✅ |
| `hardening_read_only_is_rejected` | `read_only` | ✅ |
| `hardening_ulimits_is_rejected` | `ulimits` | ✅ |
| `hardening_privileged_true_is_rejected` | `privileged: true` | ✅ |
| `hardening_privileged_false_is_rejected` | `privileged: false` (presence-based) | ✅ |
| `hardening_devices_is_rejected` | `devices` | ✅ |
| `hardening_sysctls_is_rejected` | `sysctls` | ✅ |
| `hardening_tmpfs_is_rejected` | `tmpfs` | ✅ |
| `hardening_init_is_rejected` | `init` | ✅ |
| `hardening_deploy_pids_is_rejected` | `deploy.resources.limits.pids` (nested) | ✅ |

Spot-checks confirmed:
- `privileged: false` → `ComposeError::Unsupported { key: "privileged" }` ✅ (presence-based)
- `tmpfs: []` → rejected on merge-base `load()` returned `Ok(tmpfs: Some(List([])))` → now `Err(Unsupported)` ✅

Nested pids: service with `deploy.resources.limits.memory: 24G` and no `pids` → loads cleanly (`edge_ai_stack_loads`, `supported_compose_file_loads_without_hardening_errors` both pass).

Both validate path (`load()`) and load path (`load_path()`) covered: `unsupported_key()` helper calls `load()` directly; `supported_compose_file_loads_without_hardening_errors` calls `load_path()`.

### AC2 ✅ Compose fixtures that use only supported keys still validate

- `edge-ai.yaml` → `edge_ai_stack_loads` ✅, `edge_ai_profile_enables_debug_ui` ✅, `supported_compose_file_loads_without_hardening_errors` ✅
- `wordpress.yaml` → `wordpress_tutorial_stack` ✅, `wordpress_requires_its_passwords` ✅
- 56 total tests pass, 0 fail

---

## Decision conformance

**Option A** (repo-admin decision 2026-11-23): edge-ai.yaml drops its unsupported keys; dedicated rejection fixture covers all listed keys.

Verified removed from `edge-ai.yaml` (service-level): `ulimits`, `privileged`, `devices`, `cap_add`, `sysctls` — absent ✅

**GPU scope-drift check:**  
`deploy.resources.reservations.devices` (line 39 of edge-ai.yaml) is **not** a hardening key — it is not in `HARDENING_SERVICE`. The `unsupported_note` for `gpus` explicitly directs users *to* `deploy.resources.reservations.devices`. Builder correctly preserved this. No scope drift. ✅

Volume-level `read_only: true` (line 23, inside `volumes[].read_only`) is a volume-mount attribute, not a service-level key — correctly preserved. ✅

---

## Red-first verification

Ran two tests against merge-base `f7a2fc3` by injecting probes into a scratch worktree at that commit:

```
---- qa_red_privileged_false_must_be_rejected ----
load succeeded — SHOULD FAIL: no hardening rejection on merge-base:
  Service { ... privileged: Some(false), ... }

---- qa_red_tmpfs_empty_must_be_rejected ----
load succeeded — SHOULD FAIL: no hardening rejection on merge-base:
  Service { ... tmpfs: Some(List([])), ... }

FAILED: 0 passed; 2 failed
```

Both probe tests FAIL on `f7a2fc3` (load succeeds, keys silently accepted into struct) and PASS on `04f0d32` (load returns `Unsupported` error). The fix is causally responsible for the green result.

---

## Full suite

```
Local tests: CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-compose

running 26 tests (unit)  — all ok
running 30 tests (integration/load.rs)  — all ok
  of which 11 are hardening rejection tests (all new on this branch)
  1 is supported_compose_file_loads_without_hardening_errors (new)
running 0 tests (doc-tests)

test result: ok. 56 passed; 0 failed
```

## Toolchain checks

| Check | Result |
|---|---|
| `cargo fmt --all --check` | ✅ EXIT:0 |
| `cargo clippy --all-targets -- -D warnings` | ✅ EXIT:0 |
| `cargo test -p minga-compose` | ✅ 56 passed, 0 failed |

---

## Findings

None. Both ACs pass. No scope drift. No unexpected behaviour changes in supported fixtures.
