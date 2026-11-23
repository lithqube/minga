# QA report — issue #137

Card: #137 · `[security] dns: TCP listener has no connection cap or timeout`
PR: #183 · branch `issue-137-dns-tcp-cap-timeout` · builder head `d10f75b`
Base (merge-base with `main`): `8c978dc`
Suite: `crates/minga-net/tests/dns_tcp_limits.rs` (new, written by QA)

## Verdict

All four acceptance criteria of #137 hold on the builder head `d10f75b`, and
the #175 required-egress behaviour did not regress. Each AC test fails on the
merge-base `8c978dc` and passes on `d10f75b`.

## What was run

| Check | Command | Result |
|---|---|---|
| format | `cargo fmt --all --check` | clean |
| lints | `cargo clippy --all-targets -- -D warnings` | clean |
| crate suite (head) | `cargo test -p minga-net` | 85 unit + 6 integration + 1 doc test, all passed |
| suite (base) | `cargo test -p minga-net --test dns_tcp_limits` on `8c978dc` | 4 failed, 2 passed — see `base-test-output.txt` |

Raw output: `head-test-output.txt`, `base-test-output.txt`, `clippy-output.txt`,
`repeat-runs.txt` (5 consecutive runs of the new suite, all green, observed cap
128 every time).

## Per-AC proof

| AC | Test | base `8c978dc` | head `d10f75b` |
|---|---|---|---|
| An idle TCP connection is closed within the timeout | `ac_idle_tcp_connection_is_closed_within_the_timeout` | FAILED — "the connection was still open after 15s" | passed |
| Connections beyond the cap are refused | `ac_connections_beyond_the_cap_are_refused` | FAILED — "no connection was refused: the TCP listener accepts without a cap" | passed — observed 72 of 200 refused, 128 open |
| A length prefix above the maximum is rejected before allocation | `ac_length_prefix_above_the_maximum_is_rejected_before_the_body` | FAILED — "the connection was still open after 3s" | passed |
| `shutdown()` ends open connection tasks | `ac_shutdown_ends_open_connection_tasks` | FAILED — "the connection was still open after 3s" | passed |

Supporting checks:

- `ac_maximum_length_query_is_still_answered` — a 4096-byte query (exactly the
  cap) is still answered NOERROR with the query ID kept; passes on base and
  head, so the new length bound does not reject a legal query.
- `regression_required_egress_refuses_non_local_names_over_tcp` — a source in
  `LocalOnlySources` gets REFUSED for a non-local name over TCP and the
  upstream resolver is never asked; passes on base and head, so the #175
  plumbing survived the change.

## Notes on the observations

- The observed cap is 128 concurrent TCP connections per bound address, which
  matches `MAX_TCP_CONNECTIONS` in the builder's patch.
- "Rejected before allocation" is observed as: the server closes the
  connection while the body is still unsent, so the body buffer sized from
  the length prefix was never needed. The allocation itself is not
  instrumented.
- The idle bound in the test is 15 s, above the server's 5 s timeout, so the
  test does not pin the timeout value; it proves the connection does not
  survive indefinitely.
