# QA Report — issue #213 · v1

**Branch:** `issue-213-port-conflict-generic-message`  
**Head:** `c57505020b28fc012ad4cfbc9bf7b5c2068d4177`  
**Worktree:** `.claude/worktrees/issue-213-qa/`  
**Date:** 2026-11-24

---

## Verdict: ✅ PASS — all 3 ACs verified

---

## Checks

### `cargo fmt --all --check`
Exit 0. No formatting drift.

### `cargo clippy --all-targets -- -D warnings`
Exit 0. No warnings.

### `cargo test -p minga-api`
Exit 0. **191 tests, 3 suites, all pass.**

| Suite | Binary | Tests |
|---|---|---|
| Unit (handlers.rs mod tests) | `minga_api-da942c4024aff82c` | 177 |
| Integration | `integration-1c3934d80db8b07a` | 13 |
| Doc-tests | `minga_api` | 1 |

Relevant passing tests:
- `handlers::tests::import_port_conflict_returns_generic_message` ✅
- `ingress_port_conflicts_return_http_409` ✅

---

## AC1 — `From<StateError> for ApiError` discards inner message

**File:** `crates/minga-api/src/error.rs:63-65`

```rust
E::PortConflict(_) => ApiError::Conflict(
    "published port conflicts with another cluster service".to_owned(),
),
```

The inner `String` is bound to `_` and dropped. The literal string is character-for-character
identical to the string the two removed match arms in handlers.rs used before the change:
`"published port conflicts with another cluster service"`.

**Result:** ✅ PASS

---

## AC2 — Redundant match arms removed; 409 status preserved

**Before (diff):** both `create_service` (handlers.rs:2003) and `update_service` (handlers.rs:2157)
had an explicit `.map_err(|error| match error { StateError::PortConflict(_) => conflict("…"), other => ApiError::from(other) })?`

**After:** both use `.map_err(ApiError::from)?`

The status code path: `ApiError::Conflict(_) → StatusCode::CONFLICT (409)` in `error.rs:45`.
Unchanged — `From` impl returns `ApiError::Conflict(...)` which maps to 409.

Integration test `ingress_port_conflicts_return_http_409` passed, confirming 409 still returned.

**Result:** ✅ PASS

---

## AC3 — New test drives import path; red-first confirmed

**Test:** `handlers::tests::import_port_conflict_returns_generic_message` (handlers.rs:4440)

**Path exercised:** `import_state()` → `app.store.import(snapshot, q.overwrite).await?` (line 1207)
The `?` operator calls `From<StateError> for ApiError`.

**Assertions:**
1. `matches!(&err, ApiError::Conflict(m) if m == "published port conflicts with another cluster service")`
2. `!msg.contains("svc-")` — no service ID in the message

**Red-first proof:** Reverted `From` impl to the pre-change grouped arm (`| E::PortConflict(_)`).
Test failed at `handlers.rs:4474`:
```
import port-conflict must return the generic message,
got Conflict("ingress port 9090/tcp is already used by service svc-default")
```
Restored fix → test passes.

**Result:** ✅ PASS

---

## Scope check

Diff touches exactly 2 files:
- `crates/minga-api/src/error.rs` — `From` impl split
- `crates/minga-api/src/handlers.rs` — 2 match arms removed, 1 test added

No files outside the card scope.

---

## Other consumer / inner-message audit

Searched all crates for `PortConflict`, `published_port_conflict`, `is already used by service`:

- `minga-state/src/state_machine.rs:918` — produces `StateResponse::Error(conflict)` from Raft state machine (not an API path)
- `minga-state/src/store.rs:851` — wraps into `StateError::PortConflict(message)`; inner message now discarded at the API boundary
- `minga-state/src/types.rs:693` — formats `"ingress port {}/{} is already used by service {}"` — unchanged, correct at the state layer
- `minga-state/tests/single_node.rs:183` — asserts `StateError::PortConflict(message).contains("first")`; tests the state layer directly, unaffected
- `minga-api/src/handlers.rs:1417` — `validate_ingress_ports` already used the generic string and discarded the inner message; unchanged
- `minga-compose/tests/load.rs:267` — different code path (Compose file validation, not API errors); unaffected

No log lines, metrics, or error tooltips read the inner message. No silent message-shape change to any observable surface.

---

## Multi-tenancy

Replacement string `"published port conflicts with another cluster service"` contains no service ID,
tenant ID, port number, or any internal state. Safe by construction.

Cross-tenant reach: state machine scopes `others` to same tenant (`tenant_id` filter); a
cross-tenant caller gets 404 before this path.

---

## Local tests (Reviewer reruns exactly this)

```
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-api
```
