# QA run issue-124-qa-v1

- Issue: #124 — net: network subnets are not confined to the overlay
- PR: #149 · branch `issue-124-net-subnets-confined-to-overlay` · head `5257869`
- Host: macOS (arm64) · worktree `.claude/worktrees/issue-124-qa`
- Verdict: PASS — all 4 AC verified

## Test plan (one observable test per AC)

| AC | Test | Layer |
|---|---|---|
| AC1 API 400 for subnet outside supernet / inside mesh / longer than min prefix | `handlers::tests::network_subnet_outside_the_overlay_is_refused` | unit (API handler) |
| AC2 `UpsertNetwork` refuses the same subnets in the state machine | `tests/single_node.rs::upsert_network_refuses_subnets_the_overlay_must_not_route` | integration (Raft apply) |
| AC3 peer-set unit test: no manager peer set carries a /32 outside the owning network or under two peers | `tests/scheduler.rs::relay_manager_carries_only_unique_addresses_inside_each_network` | integration (scheduler → bus assignment) |
| AC4 `validate_peer_set` rejects a duplicate AllowedIP | `wireguard::tests::an_allowed_ip_listed_under_two_peers_is_refused` | unit |

Supporting new unit test: `overlay::tests::network_subnet_must_stay_inside_the_supernet_and_off_the_mesh`
pins the shared `check_network_subnet` rule (min /28, off the `10.200.0.0/16` mesh, inside the supernet).

## Test-gap check

Every AC maps to a test written in this PR. No High gaps found.

- Medium/Low gaps (not written, non-blocking):
  - No test drives the real WireGuard interface with a duplicate AllowedIP; the kernel-level
    netns suite does not exercise that case. The refusal is covered at the `validate_peer_set`
    unit layer and at the scheduler peer-set layer, and the kernel suite still passes with the
    stricter validator (regression evidence).
  - The API handler checks the default supernet when the store has none set; a test for a
    non-default `--overlay-supernet` end to end is not written (the state-machine path with a
    set supernet is covered by AC2's test).

## Results (local, macOS)

Command: `CARGO_TARGET_DIR=/Users/admin/dev/minga/target cargo test -p minga-net -p minga-state -p minga-api -p minga-scheduler -p minga-node`

- `handlers::tests::network_subnet_outside_the_overlay_is_refused` ... ok
- `overlay::tests::network_subnet_must_stay_inside_the_supernet_and_off_the_mesh` ... ok
- `wireguard::tests::an_allowed_ip_listed_under_two_peers_is_refused` ... ok
- `relay_manager_carries_only_unique_addresses_inside_each_network` ... ok
- `upsert_network_refuses_subnets_the_overlay_must_not_route` ... ok
- minga-api 130 + 13, minga-net 77, minga-node 163, minga-scheduler 102 + 21,
  minga-state 24 + 19 + 11 + 50 + 4 — all suites 0 failed

`cargo fmt --all -- --check` exit 0. `cargo clippy --all-targets -- -D warnings` exit 0.

Full log: `cargo-test.log` in this folder.

## Not runnable on this host

- `minga-node --test netns` and `--test wireguard_netns` are Linux network-namespace
  suites; they compiled but ran 0 tests here (macOS). Supplemental evidence: CI job
  "Overlay network paths on a real kernel (never silently skipped)" passed on
  ubuntu-24.04 for head `5257869` (run 44809222239, 2m36s), as did "Daemon-backed
  tests" (containerd), both NATS store suites, and "fmt, clippy, workspace tests"
  (`gh pr checks 149` — 6/6 pass).
- Disk note: the shared target dir was full at run start and a parallel lane ran
  `cargo clean`; dropped `target/debug/incremental` (40 GB) and re-ran the suite
  cleanly after the clean finished. Final run above is the authoritative one.

## Spot checks of the fix

- API handler (`handlers.rs:2204`) runs `check_network_subnet` with the store supernet
  or `DEFAULT_SUPERNET` before any store write; refused networks are absent afterwards
  (asserted in AC1 test).
- `UpsertNetwork` in the Raft apply runs the same check with the supernet set via
  `StateStore::set_overlay_supernet`; entries before that keep only mesh/size rules
  (asserted in AC2 test).
- `build_wireguard_peer_sets` drops task IPs outside their network subnet and IPs with
  two live claimants; AC3 test asserts the relay manager's published peer set carries
  neither `192.168.1.1/32` nor a duplicated address, and does carry the unique in-subnet ones.
- `validate_peer_set` refuses an AllowedIP under two peers (AC4 test asserts the error text).
