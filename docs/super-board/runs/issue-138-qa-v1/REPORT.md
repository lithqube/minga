# QA Report — Issue #138 · PR #187 · v1

**Date:** 2026-11-23  
**Branch:** issue-138-replica-limit  
**Head:** 8446f5d  
**Merge-base:** 72f2659  
**Tester:** OMP QA lane

---

## Acceptance Criteria

### AC1 — Create or update with Replicas above the maximum returns 400 without running placement

**Status: ✅ PASS**

Test: `handlers::tests::create_and_update_reject_excessive_replicas_before_placement`  
(`crates/minga-api/src/handlers.rs`, added in PR diff hunk at line 3836)

Proof:
- Tests exercise `create_service` and `update_service` with replicas = 1001, `u32::MAX + 1`, and `u64::MAX`.
- Each returns `StatusCode::BAD_REQUEST` (400).
- After create attempts: `app.store.get_service("rejected")` is `None` → placement did NOT run.
- After update attempts: `app.store.get_service("web")` equals `original` → state unchanged.
- Full suite: 177 tests pass on head `8446f5d`.

Base `72f2659` does NOT contain this test (0 filtered-in on base run); prior code used `*Replicas as u32` (silent truncation, translate.rs lines 388/392 on base) with no cap in the direct path.

---

### AC2 — A u64 value that does not fit u32 is rejected, not truncated

**Status: ✅ PASS**

Test: `translate::tests::service_spec_rejects_excessive_replicas_without_truncation`  
(`crates/minga-api/src/translate.rs`, added in PR diff hunk at line 1712)

Proof:
- Exercises replicas = 1001, `u64::from(u32::MAX) + 1` (4294967296), and `u64::MAX`.
- Both `Replicated` and `ReplicatedJob` modes tested.
- `service_spec_to_state(&spec).is_err()` asserts for each — no truncation, no wrap.
- Key fix in translate.rs: `u32::try_from(*Replicas)` returns `Err` for values > u32::MAX; the `Ok(r) if r <= MAX_SERVICE_REPLICAS` guard rejects 1001–u32::MAX; the `_` arm rejects > u32::MAX.
- Base code: `*Replicas as u32` on lines 388/392 — this is the truncation bug being fixed.

Boundary: value `u64::from(u32::MAX) + 1 = 4294967296` is explicitly exercised in the test loop. On the base, `4294967296_u64 as u32 == 0` (wraps to 0), which would have been accepted silently as 0 replicas.

---

### AC3 — Change-request and direct paths share one constant

**Status: ✅ PASS**

Structural check (grep evidence):

```
translate.rs:27  pub(crate) const MAX_SERVICE_REPLICAS: u32 = 1000;
translate.rs:395     Ok(replicas) if replicas <= MAX_SERVICE_REPLICAS => replicas,
translate.rs:396     _ => return Err(format!("replicas must not exceed {MAX_SERVICE_REPLICAS}")),
handlers.rs:585      if !(1..=translate::MAX_SERVICE_REPLICAS).contains(&replicas) {
handlers.rs:588          translate::MAX_SERVICE_REPLICAS
```

Single definition; both paths reference it. The literal `1000` present on handlers.rs lines 860/921/983 is pagination (`query.limit.clamp(1, 1000)`), unrelated to replica admission.

Base `handlers.rs:585` used literal `1000`; `service_spec_to_state` had no cap. Post-PR: one constant, two paths.

---

## Merged-PR Regression Check

| PR | Feature | Test evidence |
|---|---|---|
| #185 (secret confirmation) | tenant secret isolation, foreign secret = 404 | `foreign_secret_reference_gets_the_same_answer_as_an_absent_one` ✅, `egress_route_secrets_of_another_tenant_are_not_declared` ✅ |
| #179 (proposal display) | change-request lifecycle | `deciding_a_change_request_needs_a_reviewer_certificate` ✅, `change_requests_are_tenant_scoped_and_maker_checker_enforced` ✅ |
| #177 (tenant bus cleanup) | bus subject scoping, tenant deletion clears bus | `a_deleted_tenant_leaves_no_bus_resources_and_its_id_can_be_reused` ✅, `a_tenant_is_not_created_over_leftover_bus_resources` ✅ |

All 177 tests pass; no regression from conflict resolution.

---

## Verification Commands Run

```
# Format
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo fmt --all --check   → exit 0

# Clippy
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo clippy --all-targets -- -D warnings   → exit 0

# Tests (full minga-api suite)
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-api
→ 163 unit + 13 integration + 1 doc-test = 177 passed, 0 failed

# Red-first: base has no AC tests (tests added by PR)
# Confirmed base translate.rs:388,392 uses `*Replicas as u32` (truncation bug present)
# Confirmed base handlers.rs:585 uses literal 1000, not shared constant
```

**Local tests:** `CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-api`

---

## Summary

All 3 ACs pass. 177 tests green. No regression from merged PRs. Red-first confirmed (base has truncation bug; PR head fixes it with `u32::try_from` + shared constant).
