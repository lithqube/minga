# QA report — issue #141 · round 1 · PR #159

Branch: `issue-141-api-security-keys-unsupported` · QA worktree `.claude/worktrees/issue-141-qa/`
Base at test time: `origin/main` @ `91b326fe62a21d2ea867b29743749bdb4548bdc4`
PR head before the QA patch: `02cd96aec2bd246b2fb2e4629fefd3b090160e26`

## Verdict: PASS (2/2 AC)

## Test plan — one observable test per AC

| AC | Observable test | Witness |
|---|---|---|
| AC1 A spec containing ReadOnly, Pids, Privileges, CapabilityDrop, or Ulimits returns HTTP 400 | Handler-level JSON cases exercise all five fields on create and assert the `ApiError` response status; a JSON ReadOnly update also returns 400 and leaves the stored image unchanged. Translation-level cases separately assert field-specific refusal and Pids in both Limits and Reservations. | `handlers.rs:3175-3262`, `translate.rs:1452-1549` |
| AC2 Specs without those fields are unchanged | The basic service conversion test asserts image, resources, networks, ports, and other ordinary fields; ReadOnly false and empty CapabilityDrop/Ulimits remain accepted. | `translate.rs:1500-1507,1552-1571` |

## Test-gap check

- **Wire JSON:** ReadOnly, Privileges, CapabilityDrop, Ulimits, and Pids are deserialized from JSON and rejected by create; ReadOnly update uses a JSON ServiceSpec. The Ulimits-only test catches a serde-name regression independently of CapabilityDrop.
- **Error behavior:** rejected create requests do not persist a service; rejected update leaves the existing service image unchanged.
- **Mutation proof:** changed the ReadOnly guard from `Some(true)` to `Some(false)` temporarily. The new test failed because create returned 201 for `rejected-read-only`. Restored the guard; the test passed.
- **Boundaries:** `ReadOnly:false` and empty CapabilityDrop/Ulimits are accepted. `Pids:0` and empty Privileges are not separately pinned; the literal AC requires present Pids/Privileges fields to be refused, and the implementation rejects any `Some(Pids)` or `Some(Privileges)`.
- **Layer gap:** no browser/e2e coverage; this is an API-only change. Tests call the create/update handlers and exercise `IntoResponse` status conversion, not the Axum router transport extractor.

## Runs

- `cargo fmt --all` → clean.
- `CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/Users/admin/dev/minga/target cargo clippy --all-targets -- -D warnings` → clean.
- `CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/Users/admin/dev/minga/target cargo test -p minga-api` → 152 passed.
- `CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/Users/admin/dev/minga/target cargo test` → 1,200 passed, 4 ignored, 63 suites.
- The shared target initially reused compose/state test binaries that pointed at the separate `issue-123-qa` worktree. Recompiled those test targets from this worktree; the final full workspace run passed.

Screenshots intentionally omitted: API-only behavior; there is no visual surface to capture.
