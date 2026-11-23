# Issue 134 QA

Source reviewed: a50865fdd682194aa965808a97eda29a9398e3be.
Host: Codex, macOS. Clean existing issue QA worktree resumed at the matching PR head.
Owner: owner:omp. Board entry: QA. No product code changed.

## Plan and AC evidence

| AC | Unit | Component / integration | Terminal |
|---|---|---|---|
| Show diff and confirm unless --yes | n/a: CLI process boundary | wrapped.rs:744, change_review_approval_reads_diff_before_confirmation_or_yes; wrapped.rs:785 missing/unreviewable requests fail closed | terminal_check.py: cancellation and acceptance; diff precedes prompt, zero POST before answer, zero POST on cancel, exactly one POST on acceptance |
| Table and JSON escape Cf | n/a: output boundary | wrapped.rs:718, change_review_list_shows_values_and_escapes_unicode; JSON round-trips exact values | terminal.log shows literal U+202E and supplementary U+E0001 UTF-16 escapes |
| API rejects image Cf | n/a: HTTP boundary | server.rs:2690 inside change_requests_are_tenant_scoped_and_maker_checker_enforced: U+202E, U+200B, U+E0001, non-ASCII letter rejected with 400 | n/a: API criterion |

Local tests: CARGO_INCREMENTAL=0 cargo test -p minga-ctl -p minga-api
Result: 350 passed, zero failed/ignored, including one compile doc test (tests.log).
Additional check: python3 docs/super-board/runs/issue-134-qa-v2/terminal_check.py
Formatting: cargo fmt --all --check, exit 0 (fmt.log is empty).
Lint: CARGO_INCREMENTAL=0 cargo clippy -p minga-ctl -p minga-api --all-targets -- -D warnings, exit 0.

## Independent checks and test gaps

Hypotheses checked inline before accepting builder claims:

- Approval must fetch the exact ID before showing values or sending a decision. The CLI selects the returned matching ID and refuses missing/unreviewable responses. Terminal evidence observes GET before any POST.
- A supplementary Cf must not survive in terminal output or corrupt JSON. UTF-16 escaping preserves the value; JSON integration coverage round-trips the fixture.
- ID filtering must remain inside the tenant boundary and run before limiting. Handler first loads the tenant key range; server integration test checks matching tenant gets the request and another tenant gets no rows.
- Non-ASCII proposals must fail at the HTTP handler before persistence. Real router tests assert BAD_REQUEST for the four witness strings.

No High test gaps remain. Existing integration assertions fail if ASCII rejection, escaping, confirmation, or matching-ID lookup is removed. Terminal negative control uses the real --yes public path to deliberately omit the prompt; the prompt assertion rejects it (terminal-negative.log), then the ordinary interactive paths pass again. This is a harness sensitivity check, not a claimed source mutation or base-branch regression run.

Test gaps (not written): Medium: exhaustive all-Cf enumeration, huge before/after objects, and explicit network-failure fixtures. Existing all-non-ASCII validation/escaping covers Cf by construction; the concrete BMP and supplementary witnesses are exercised. Medium: extra legacy malformed responses with empty changes are outside the current server contract. No optional project-owned feature map exists; the only feature-map found is the starter template. No optional verifier pass is claimed.

## Tenancy and limits

No new state, bus subject, endpoint or node-local resource. The new ID query uses the existing tenant-scoped list endpoint, tenant store range and role checks. Cross-tenant lookup yields an empty list and CLI NotFound; it reveals no foreign object. API integration suite also covers maker/checker roles and cross-tenant approval. No migrations or live cluster effects.

CLI transport is a localhost scripted API for process checks; the API suite uses the real router and test state. No live deployment, container or browser was required. Screenshots are inapplicable to these CLI/API ACs. Review threads contain six author summaries and no QA findings or human requests. Existing author summaries are preserved. The Reviewer still owns independent review and the merge gate; the configured human merge policy remains applicable.
