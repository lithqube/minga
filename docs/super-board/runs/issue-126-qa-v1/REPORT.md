# QA report — issue #126 · round 1 · PR #152

Branch: `issue-126-tenant-reserved-ingress-ports` @ `3329f0f`
Tester worktree: `.claude/worktrees/issue-126-qa/` (detached check of PR head)
Base at test time: `main` @ `ed40e17`

## Verdict: PASS (3/3 AC)

## Test plan — one observable test per AC

| AC | Observable test | Witness |
|---|---|---|
| AC1 Two-tenant: tenant A node reserving 443/tcp does not block tenant B ingress create/update | `handlers::tests::cross_tenant_reserved_port_does_not_block_ingress` (pure `validate_ingress_ports`) + first half of `handlers::tests::service_admission_scopes_reserved_ports_to_the_service_tenant` (store-backed `ensure_placeable`, the admission every write path runs) | `handlers.rs:3322`, `handlers.rs:3346` |
| AC2 Same-tenant reservation still blocks | `handlers::tests::same_tenant_reserved_port_stills_blocks_ingress` (asserts exact `Err("ingress port is reserved on a cluster node")`) + second half of the store test (re-reserve on acme's node → `ApiError::Conflict`) | `handlers.rs:3332`, `handlers.rs:3371-3380` |
| AC3 Change-request approval uses the same tenant-scoped check | Code path verified: `decide_change_request` approves through `ensure_placeable` (`handlers.rs:644`), as do create (`:1807`) and update (`:1955`); all three funnel into `validate_ingress_ports`. Store-backed two-tenant admission test exercises that shared path. | `handlers.rs:644,1807,1955,1172` |

## Red-first proof (test-gap check)

Applied only the test hunks of the PR diff onto pre-fix `main` (`origin/main` @ `ed40e17`) in scratch worktree `.claude/worktrees/issue-126-redfirst/`:

| Test | Pre-fix main | PR branch | Meaning |
|---|---|---|---|
| `cross_tenant_reserved_port_does_not_block_ingress` | **FAILED** (panicked: reservation blocked) | ok | Catches the exact reported bug — not vacuous |
| `same_tenant_reserved_port_still_blocks_ingress` | ok | ok | Guards AC2 without behavior change — not vacuous |
| `service_admission_scopes_reserved_ports_to_the_service_tenant` | **FAILED** (`Conflict("ingress port is reserved on a cluster node")`) | ok | Admission path was broken pre-fix, fixed now |

Note: with a shared `CARGO_TARGET_DIR` across worktrees, a stale rlib from the red-first
checkout shadowed the branch build once; forced rebuild (`touch` + rerun) gave the true
branch result above. All branch results below were taken after that rebuild.

## Runs (CARGO_TARGET_DIR=/Users/admin/dev/minga/target)

- Focused: `cargo test -p minga-api --lib reserved` → 4 passed, 0 failed — `cargo-test-focused.log`
- Full suite: `cargo test -p minga-api` → 133 + 13 + 1 passed, 0 failed — `cargo-test.log`
- `cargo fmt --all -- --check` → clean — `cargo-fmt.log`
- `cargo clippy --all-targets -- -D warnings` → clean — `cargo-clippy.log`

## Test gaps (not written)

- Medium: no HTTP-endpoint-level two-tenant test (create/update/approve through the router);
  admission is proven at the shared `ensure_placeable` chokepoint, which all three handlers
  call. Coverage is adequate for this fix's blast radius.
- Low: update path (`:1955`) has no dedicated reserved-port test; it shares `validate_ingress_ports`
  with create, covered above.
