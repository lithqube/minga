# QA test plan — issue #137

Card: #137 · `[security] dns: TCP listener has no connection cap or timeout`
Branch: `issue-137-dns-tcp-cap-timeout` · PR #183 · head `d10f75b`
Base (merge-base with `main`): `8c978dc`

Suite: `crates/minga-net/tests/dns_tcp_limits.rs` — an integration test that
drives the public `minga_net::DnsServer` API over real loopback TCP sockets,
so every assertion is wire behaviour a task on a bridge network sees.

## Acceptance criteria → observable test

| # | Acceptance criterion (from issue #137) | Test | Observable |
|---|---|---|---|
| 1 | An idle TCP connection is closed within the timeout | `ac_idle_tcp_connection_is_closed_within_the_timeout` | a client that connects and never sends a byte sees the connection closed by the server |
| 2 | Connections beyond the cap are refused | `ac_connections_beyond_the_cap_are_refused` | 200 concurrent TCP clients: the ones over the cap are closed by the server, the ones under it stay open and are still answered |
| 3 | A length prefix above the maximum is rejected before allocation | `ac_length_prefix_above_the_maximum_is_rejected_before_the_body` | a prefix of 4097 and of 65535 is refused while the body is never sent, so no body buffer was ever needed |
| 3b | boundary of 3: a query of exactly the maximum length is still answered | `ac_maximum_length_query_is_still_answered` | a 4096-byte query gets a NOERROR reply with the query ID kept |
| 4 | `shutdown()` ends open connection tasks | `ac_shutdown_ends_open_connection_tasks` | a connection parked mid-read is closed after `shutdown()` returns |
| — | regression on #175: a required-egress source is refused non-local names over TCP | `regression_required_egress_refuses_non_local_names_over_tcp` | a non-local name gets REFUSED over TCP and the upstream resolver is never asked |

## Bounds used, and why

- Idle connection: closed within 15 s. The server's own timeout is 5 s, so a
  timeout satisfies the bound and an unbounded listener never does.
- Oversized prefix: closed within 3 s — deliberately below the server's 5 s
  read timeout, so the only way to pass is to reject on the prefix alone.
- Cap: the test opens 200 connections rather than the server's 128, so it
  does not hard-code the cap number. The observed cap is printed in the test
  output and recorded in `REPORT.md`.
- Shutdown and refusal: closed within 3 s.

## Base / head comparison

`base-test-output.txt` runs this exact suite against `8c978dc` (the base
version of `crates/minga-net/src/dns_server.rs`, the builder's file reverted).
`head-test-output.txt` runs it against `d10f75b`.

| Test | base `8c978dc` | head `d10f75b` |
|---|---|---|
| `ac_idle_tcp_connection_is_closed_within_the_timeout` | FAILED — connection still open after 15 s | passed |
| `ac_connections_beyond_the_cap_are_refused` | FAILED — no connection was refused | passed |
| `ac_length_prefix_above_the_maximum_is_rejected_before_the_body` | FAILED — connection still open after 3 s | passed |
| `ac_maximum_length_query_is_still_answered` | passed | passed |
| `ac_shutdown_ends_open_connection_tasks` | FAILED — connection still open after 3 s | passed |
| `regression_required_egress_refuses_non_local_names_over_tcp` | passed | passed |

The two tests that pass on the base are the two that must pass on both sides:
the maximum-length boundary and the #175 required-egress regression.
