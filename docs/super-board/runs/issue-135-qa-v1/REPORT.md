# QA v1 — issue #135 compose port host_ip silently dropped

Branch `issue-135-compose-port-host-ip`, tested at `f4cbc51` + QA test commit (this push).

Non-visual issue (CLI / library behavior) — no screenshots; proof is test logs.

## AC1 — `minga validate` on a port with `host_ip` fails with an Unsupported error

- Red-first (pre-fix `16343cc`, merge-base with `main`):
  - `red-first-port-host-ip.log` — builder's load tests `port_host_ip_short_syntax_is_unsupported` / `port_host_ip_long_syntax_is_unsupported` FAIL (host_ip loaded Ok).
  - `red-first-validate-cli.log` — new CLI tests `validate_rejects_port_host_ip_short_syntax` / `validate_rejects_port_host_ip_long_syntax` FAIL: `minga validate` exits 0 and prints `ports 5432->5432/tcp`, host IP silently dropped — the exact bug.
- Green (branch):
  - `green-validate-cli.log` — 3/3 CLI tests pass: validate exits 1 with `services.db.ports[0]: \`host_ip\` is part of the Compose spec but not supported yet` for short and long syntax.
  - `gate-cargo-test-workspace.log` — `cargo test -p minga-compose --test load` (within workspace run): builder's `port_host_ip_*_is_unsupported` tests pass.

## AC2 — Ports without `host_ip` deploy as before

- `validate_accepts_ports_without_host_ip` passes (`green-validate-cli.log`): `minga validate` exits 0 for `['5432:5432', 8080]`.
- Builder's `ports_without_host_ip_still_load` + all 15 pre-existing load tests pass.
- Full workspace suite green: 64 binaries, 1193 passed, 0 failed (`gate-cargo-test-workspace.log`).

## Gate (env: `CARGO_INCREMENTAL=0`)

- `cargo fmt --all --check` — clean.
- `cargo clippy --all-targets -- -D warnings` — clean (`gate-clippy.log`).
- `cargo test --workspace` — exit 0.

Note: the shared `CARGO_TARGET_DIR=/Users/admin/dev/minga/target` was being written concurrently by the issue-140 QA lane, which left foreign-compiled binaries (a `minga-state` fixture test resolved fixture paths into `issue-140-qa/`). The final gate above ran in a private target dir to exclude that contamination; the failure was not related to this change.

## Test-gap check

Builder covered load-level rejection (short + long syntax) and load-level acceptance without host_ip. High gap found: AC1 names the `minga validate` CLI surface, which had no test. Added `crates/minga-cli/tests/validate.rs` (3 tests) red-first. No Medium/Low gaps left unwritten.
