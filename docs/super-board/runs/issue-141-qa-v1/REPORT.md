# QA report — issue #141 · round 1 · PR #159

Branch: `issue-141-api-security-keys-unsupported` · QA worktree `.claude/worktrees/issue-141-qa/`
Base at test time: `origin/main` @ `91b326fe62a21d2ea867b29743749bdb4548bdc4`
PR head before the QA patch: `02cd96aec2bd246b2fb2e4629fefd3b090160e26`

## Verdict: PASS (2/2 AC)

## Test plan — one observable test per AC

| AC | Observable test | Witness |
|---|---|---|
| AC1 Active hardening values for ReadOnly, Pids, Privileges, CapabilityDrop, or Ulimits return HTTP 400; no-op values are unchanged | Handler-level JSON cases exercise all five active values on create and assert HTTP 400; a JSON ReadOnly update also returns 400 and leaves the stored image unchanged. Translation-level cases assert field-specific refusal and Pids in both Limits and Reservations. | `handlers.rs:3175-3262`, `translate.rs:1463-1528` |
| AC2 Specs without active hardening values are unchanged | The basic service conversion test pins ordinary fields; ReadOnly false, empty lists/Privileges, and unlimited Pids zero are accepted. | `translate.rs:1530-1579,1624-1643` |

## Test-gap check

- **Wire JSON:** Active ReadOnly, Privileges (CredentialSpec), CapabilityDrop, Ulimits, and Pids are deserialized and rejected on create; ReadOnly update uses a JSON ServiceSpec. The Ulimits-only test catches its serde binding independently. Empty `Privileges` and `Pids:0` JSON are accepted by a separate no-op test.
- **Error behavior:** rejected create requests do not persist a service; rejected update leaves the existing service image unchanged.
- **Mutation proof:** changed the ReadOnly guard from `Some(true)` to `Some(false)` temporarily; the handler test failed because create returned 201. The new no-op regression test also failed under the previous blanket `Privileges.is_some()` / `Pids.is_some()` guards, then passed with active-value checks.
- **Boundaries:** `ReadOnly:false`, empty CapabilityDrop/Ulimits/Privileges, and `Pids:0` are accepted. Moby API v1.55 specifies `Pids:0` as unlimited ([ResourceSpec.Pids](https://github.com/moby/moby/blob/master/api/docs/v1.55.yaml#L794-L796)).
- **Layer gap:** no browser/e2e coverage; this is an API-only change. Tests call the create/update handlers and exercise `IntoResponse` status conversion, not the Axum router transport extractor.

## Runs

- `cargo fmt --all` → clean.
- `CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/Users/admin/dev/minga/target cargo clippy --all-targets -- -D warnings` → clean.
- `CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/Users/admin/dev/minga/target cargo test -p minga-api` → 153 passed.
- `CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=/Users/admin/dev/minga/target cargo test` → 1,201 passed, 4 ignored, 63 suites.
- The shared target initially reused compose/state test binaries that pointed at the separate `issue-123-qa` worktree. Recompiled those test targets from this worktree; the final full workspace run passed.

Screenshots intentionally omitted: API-only behavior; there is no visual surface to capture.
