# Issue 133 QA

Reviewed application head: `d3f0996f2946b8e232a284fe4c73fdb6c878f817`.
Host: Codex, macOS; isolated `.claude/worktrees/issue-133-qa/target`, incremental compilation disabled.
No UI changes; screenshots do not apply. No optional project verifier map exists.

## Independent checks

Before consulting the builder summary, the raw patch suggested three checks:
1. The CLI must select the filtered listener rather than the authenticated full router.
2. Filtering must drop entire families before encoding, preserving node metrics.
3. The operator API must retain all five usage families and reject tenant/anonymous callers.

Source confirms CLI `main.rs:2139` calls telemetry `serve`; its listener selects the filtered router.
The registry filter inspects every sample and label, dropping complete matching families.
No new state, endpoint, bus subject, migration or node resource is added. Existing operator authorization remains in force.
Standards review: 0 findings. Spec review: 0 findings. No prior reviewer findings to reconcile.
Classification: narrow security bug; preserve `bug` and `owner:omp`.

## Acceptance ledger and plan

| AC | Unit | Component/integration | E2E/manual |
|---|---|---|---|
| Standalone exports no tenant label | none | `crates/minga-telemetry/src/lib.rs:71`: real HTTP listener asserts HTTP 200, node family retained, tenant label/value and whole usage families absent | CLI wiring source check at `crates/minga-cli/src/main.rs:2139`; no deployed cluster needed |
| Operator retains usage | none | `crates/minga-api/src/server.rs:1248`: all five families, HTTP 200 operator; 403 tenant-admin; 401 anonymous | n/a: API router is the changed behavior boundary |
| Firewall documentation | n/a: prose | n/a: prose | Read `docs/talos-deployment.md:217`; restrict TCP 9100 to trusted monitoring, deny other tenants even on private network |

## Test gaps

No High gaps: security boundaries and both registry selection branches have observable coverage.
The listener test seeds all five families, including dropped events, and checks complete family metadata removal. API checks positive and negative authentication.
Zero-sample vectors export no tenant samples; labels with empty or Unicode tenant values are still matched by label name. No new numeric limits, date logic, writes, replay semantics or UI states.
Medium gaps (not written): custom future metric families with multiple labels/tenants are not separately registered by this suite; existing whole-registry assertions catch current family leakage. Dropping only the filter or applying it to the API is caught by the two existing tests.
Low gaps (not written): firewall prose checked manually rather than with a brittle text test.
No new tests were necessary; no production code changed by QA. Builder's red/green claim is not counted as an independent QA run.

## Validation

Exact commands and complete output: `tests.log`, `checks.log`.
`unset CARGO_TARGET_DIR; export CARGO_INCREMENTAL=0; cargo test -p minga-telemetry -p minga-api`
`cargo fmt --all --check; cargo clippy --all-targets -- -D warnings`

Results: affected suites passed (142 API unit tests, 13 API integration tests, 10 telemetry tests, 1 API doc test; zero failures/ignored). Workspace formatting and Clippy passed, exit 0. Final pre-commit `cargo fmt --all` also exited 0.
Inline truth-check confidence: 94/100, above the configured 70 threshold. Evidence combines the actual HTTP listener, API authorization tests and direct CLI/documentation inspection; no deployed Talos firewall was tested.
Cleanup inventory: this run created only its QA worktree/target and temporary handoff files. No Docker resources were created by the lane; optional Docker CLI integration tests use the suite's availability checks.
