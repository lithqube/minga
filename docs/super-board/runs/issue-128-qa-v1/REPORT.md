# QA Report — issue-128 · v1

**PR:** #185  
**Branch:** issue-128-api-secret-ref-tenant-leak  
**Head:** e19a1d75de0cac526ab739ab2e20f18e2a7d0898  
**Merge-base (bug present):** d4232b9bf92fa90194941212d86f24e0aba0dd82

## Test plan

| AC | Test |
|---|---|
| AC1: foreign-secret reference → same status + message as absent | `handlers::tests::foreign_secret_reference_gets_the_same_answer_as_an_absent_one` |
| AC2: own missing secret → 404 on inspect and delete | `handlers::tests::own_missing_secret_returns_404_on_inspect_and_delete` |

## Checks

| Check | Result |
|---|---|
| `cargo fmt --all --check` | ✅ clean |
| `cargo clippy --all-targets -- -D warnings` | ✅ clean |
| `cargo test -p minga-api` | ✅ 160 unit + 13 integration + 1 doc-test |
| AC1 test on HEAD | ✅ pass |
| AC2 test on HEAD | ✅ pass |
| AC1 test on d4232b9 (merge-base) | ❌ FAILED — base failure line below |

## Red-first evidence (d4232b9)

```
test handlers::tests::foreign_secret_reference_gets_the_same_answer_as_an_absent_one ... FAILED

---- handlers::tests::foreign_secret_reference_gets_the_same_answer_as_an_absent_one stdout ----
thread panicked at crates/minga-api/src/handlers.rs:3460:9:
foreign secret must be indistinguishable from an absent one: NotFound("secret acme-key not found")
```

Foreign secret returned `NotFound` (404) on base; absent secret returned `BadRequest` (400) — distinguishable, confirming the oracle bug.

## Conflict-resolution verification (#179 × #185)

PR #179 added `ChangeRequestView` with `before`/`after`/`changes`/`hidden_changes` fields and `service_changes()` / `changed_service_fields()` helpers (proposal-display behaviour).  
Verified present on HEAD at the same line structure as on `d4232b9` — both intents intact.

## Local test command

```
CARGO_INCREMENTAL=0 env -u CARGO_TARGET_DIR cargo test -p minga-api
```
