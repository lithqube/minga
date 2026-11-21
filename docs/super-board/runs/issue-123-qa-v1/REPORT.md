# QA report: issue #123 (api: stalled request bodies hold shared API slots)

- Branch: `issue-123-stalled-request-bodies` · built head `35f3caa` (PR #168)
- Date: 2026-11-21 · lane: tester (super-qa, issue-scoped) · v1

## AC 1: a request whose body stalls is closed within the configured deadline: PASS

- `a_stalled_request_body_is_closed_at_its_deadline` (server.rs): real TLS listener, body deadline 300 ms, a POST sends `{` of a 100-byte body and stops. The server closes the connection inside 2 s, and the tenant's single slot is free again (next request gets 200).
- QA added `a_body_dripped_byte_by_byte_is_closed_at_its_deadline`: one byte every ~100 ms, each inside the 300 ms deadline. The connection still closes inside 2 s, so the deadline covers the whole body, not each chunk. Seen red with a per-chunk timeout (see `mutations.txt`).

## AC 2: while tenant A holds its cap of stalled requests, tenant B and an operator are served: PASS

- `a_tenant_holding_its_cap_of_stalled_bodies_locks_out_no_one_else` (server.rs): 6 slots, cap 2 per tenant, 2 reserved for operators. Tenant A holds 2 stalled bodies: A's next connection is refused, tenant B gets 200, the operator gets 200. Tenant B then fills the 4 shared slots: tenant C is refused, the operator still gets 200.
- Seen red when operators lose their exemption (M2 in `mutations.txt`).

## AC 3: `/state` import keeps a longer but finite deadline: PASS

- Same test as AC 1, second half: state deadline 1500 ms vs 300 ms default. A stalled `POST /v1.41/state` from an operator is still open at 800 ms and closed by 3 s.
- Non-admin callers are rejected by `require_admin_role` before the body is read, so they cannot use the longer `/state` budget.

## Cargo gate (board verify_commands)

`cargo fmt --all --check` ✅ · `cargo clippy --all-targets -- -D warnings` ✅ · `cargo test --workspace` ✅ exit 0, 64 suites, 1210 passed, 0 failed. See `cargo-verify.txt`.

## Test gaps (not written)

- Medium: `ApiServer::start_*` wraps the router with `with_body_deadline` and calls `serve_tls_bounded` with `ApiLimits::default()`. The tests build that wiring themselves, so deleting the wrapper from the production start path keeps every test green. Covering it needs the limits injectable on `ApiServer` (app code), or a 30 s test.
- Low: the 30 s / 300 s / 128 / 64 defaults are not asserted.
- Low: the global accept slot is taken before the TLS handshake, so a connection later refused by its tenant cap holds a global slot for up to the header-read timeout. Pre-existing behavior, bounded.
