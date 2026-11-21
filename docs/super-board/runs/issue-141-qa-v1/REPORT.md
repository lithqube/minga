# QA report — issue #141 · round 1 · PR #159

Branch: `issue-141-api-security-keys-unsupported` · QA worktree `.claude/worktrees/issue-141-qa/`
Base at test time: `origin/main` @ `91b326fe62a21d2ea867b29743749bdb4548bdc4`
PR head before the QA patch: `02cd96aec2bd246b2fb2e4629fefd3b090160e26`

## Verdict: PASS (2/2 AC)

## Test plan — one observable test per AC

| AC | Observable test | Witness |
|---|---|---|
| AC1 Active hardening values for ReadOnly, Pids, Privileges, CapabilityDrop, or Ulimits return HTTP 400; no-op values are unchanged | Handler JSON cases exercise active ReadOnly, CredentialSpec, Seccomp, AppArmor, NoNewPrivileges, CapabilityDrop, Ulimits, and nonzero Pids on create; a JSON ReadOnly update also returns 400 without changing stored state. Translation tests cover active SELinuxContext and Pids in Limits and Reservations. | `handlers.rs:3175-3277`, `translate.rs:1467-1537,1664-1704` |
| AC2 Specs without active hardening values are unchanged | ReadOnly false, empty lists/Privileges, `SELinuxContext.Disable:false`, empty nested privilege strings, `NoNewPrivileges:false`, and unlimited Pids zero are accepted through translation. | `translate.rs:1539-1662` |

## Test-gap check

- **Wire JSON:** Active ReadOnly, CredentialSpec, Seccomp, AppArmor, NoNewPrivileges, CapabilityDrop, Ulimits, and Pids are deserialized and rejected on create; SELinuxContext is covered by translation tests. ReadOnly update uses JSON. The Ulimits-only test catches serde binding independently. Empty `Privileges`, inactive nested privilege values, and `Pids:0` are accepted by JSON tests.
- **Error behavior:** rejected create requests do not persist a service; rejected update leaves the existing service image unchanged.
- **Mutation proof:** changing the ReadOnly guard to `Some(false)` made the handler test return 201; the previous blanket Privileges/Pids guards failed the no-op regression test. Before adding the Seccomp model, the new handler case returned 201 for `Privileges.Seccomp`. The new JSON no-op regression failed while empty strings and `Disable:false` were treated as active; it passes with value-aware guards.
- **Boundaries:** `ReadOnly:false`, empty lists/Privileges/nested strings, `SELinuxContext.Disable:false`, `NoNewPrivileges:false`, and `Pids:0` are accepted. Moby API v1.55 documents the Privileges options and specifies `Pids:0` as unlimited ([Privileges](https://github.com/moby/moby/blob/master/api/docs/v1.55.yaml), [ResourceSpec.Pids](https://github.com/moby/moby/blob/master/api/docs/v1.55.yaml#L794-L796)).
- **Layer gap:** no browser/e2e coverage; this is an API-only change. Tests call the create/update handlers and exercise `IntoResponse` status conversion, not the Axum router transport extractor.

## Runs

- `cargo fmt --all` → clean.
- `CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/Users/admin/dev/minga/target cargo clippy --all-targets -- -D warnings` → clean.
- `CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/Users/admin/dev/minga/target cargo test -p minga-api` → 153 passed.
- `CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/Users/admin/dev/minga/target cargo test` → 1,202 passed, 4 ignored, 63 suites.
- The shared target initially reused compose/state test binaries that pointed at the separate `issue-123-qa` worktree. Recompiled those test targets from this worktree; the final full workspace run passed.

Screenshots intentionally omitted: API-only behavior; there is no visual surface to capture.
