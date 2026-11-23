# QA Report — Issue #153 · v1
**Date:** 2026-11-23T23:32:00Z
**Head:** `7122fcc80cf6cc4f6859599c455f10c266d46d8f`
**Merge-base:** `1d4d0af135cadba0fa6824bfa03a36f9e94fa9be`
**Branch:** issue-153-cross-tenant-ingress-collision
**PR:** https://github.com/lithqube/minga/pull/210

## Summary

PASS — all 4 ACs verified on HEAD; all relevant new tests fail on merge-base (red-first confirmed).

## Checks

| Check | Result |
|---|---|
| `cargo fmt --all --check` | ✅ clean |
| `cargo clippy --all-targets -- -D warnings` | ✅ 0 warnings |
| `cargo test -p minga-state` | ✅ 131/131 passed |
| `cargo test -p minga-api` | ✅ 190/190 passed |

## Red-first (merge-base 1d4d0af)

Tests run against base code (unfixed `published_port_conflict`, no tenant filter):

| Test | Base result | Head result |
|---|---|---|
| `types::tests::cross_tenant_ingress_same_port_is_not_a_conflict` | ❌ FAILED — got `Some("ingress port 8080/tcp is already in use")`, expected `None` | ✅ ok |
| `types::tests::cross_tenant_service_in_mixed_others_does_not_cause_conflict` | ❌ FAILED — same reason | ✅ ok |
| `state_machine::tests::commit_agrees_with_admission_on_cross_tenant_ingress_ports` | ❌ FAILED — got `Error("ingress port 8080/tcp is already in use")`, expected `Ok` | ✅ ok |
| `handlers::tests::cross_tenant_service_on_same_ingress_port_is_admitted` | ❌ FAILED — panic "cross-tenant ingress port overlap must not block admission" | ✅ ok |
| `types::tests::same_tenant_409_does_not_leak_other_tenant_id` | ✅ ok (same-tenant path, unaffected by fix) | ✅ ok |
| `handlers::tests::same_tenant_service_on_same_ingress_port_is_rejected` | ✅ ok (same-tenant path) | ✅ ok |
| `handlers::tests::same_tenant_409_does_not_leak_other_tenant_identifiers` | ✅ ok | ✅ ok |

## AC verification

### AC1 — Two services in different tenants publishing the same ingress port are admitted independently
- **Fix:** `crates/minga-state/src/types.rs` line 682 — added `&& other.tenant_id == service.tenant_id` to the `filter` on the outer loop in `published_port_conflict`.
- **Tests (minga-state):** `cross_tenant_ingress_same_port_is_not_a_conflict` ✅, `cross_tenant_service_in_mixed_others_does_not_cause_conflict` ✅
- **Tests (minga-api):** `cross_tenant_service_on_same_ingress_port_is_admitted` ✅
- **Red on base:** `cross_tenant_ingress_same_port_is_not_a_conflict` panicked: left=`Some("ingress port 8080/tcp is already in use")` right=`None`

### AC2 — Same-tenant collision still 409s with the existing message
- **Existing message:** `"ingress port {port}/{proto} is already used by service {id}"` — pinned in `same_tenant_ingress_conflict_names_service`; message string unchanged from pre-fix code (the else-branch was removed, the same-tenant format string is unchanged).
- **Tests:** `same_tenant_ingress_conflict_names_service` ✅, `same_tenant_service_on_same_ingress_port_is_rejected` ✅

### AC3 — 409/Conflict response leaks no other tenant's identifiers
- **Admission path (handlers.rs:1417-1419):** `published_port_conflict` result is checked with `.is_some()` only; the error string is **discarded** and a fixed generic message returned: `"published port conflicts with another cluster service"` — no service ID, no tenant ID ever exposed.
- **Commit path (state_machine.rs:918):** `published_port_conflict` now only produces a conflict message for same-tenant services; the message names `other.id` which is only reachable after `other.tenant_id == service.tenant_id` passes, so it cannot name a cross-tenant service.
- **Tests:** `same_tenant_409_does_not_leak_other_tenant_id` ✅, `same_tenant_409_does_not_leak_other_tenant_identifiers` ✅

### AC4 — Scheduler and state-machine commit checks agree (no admission/commit skew)
- **Both call sites:** `handlers.rs:1417` (admission) and `state_machine.rs:918` (commit) both call the same `published_port_conflict` function. The fix is in that single function, so both paths are updated atomically — no skew possible.
- **Test:** `commit_agrees_with_admission_on_cross_tenant_ingress_ports` ✅ — creates tenant-A service on port 8080, then tenant-B service on same port; both commit as `StateResponse::Ok`. A second same-tenant-A service on port 8080 returns `StateResponse::Error`.
- **Red on base:** Test panicked at AC4a: `Error("ingress port 8080/tcp is already in use")` ≠ `Ok`

## Third-party boundary check

- tenant-B cannot occupy a port tenant-A reserved: `cross_tenant_service_in_mixed_others_does_not_cause_conflict` verifies the asymmetry — a cross-tenant peer alone causes no conflict; a same-tenant peer does. The filter `other.tenant_id == service.tenant_id` works in both directions.
- The state machine test commits tenant-A and tenant-B independently on the same port and both succeed; then a second same-tenant-A service is correctly refused.

## Local tests

```
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-state && CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-api
```
