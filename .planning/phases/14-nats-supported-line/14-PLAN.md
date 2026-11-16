---
phase: 14
title: Supported NATS server line (tenancy T0)
depends_on: []
requirements: [NATS-PIN, NATS-LEVEL, NATS-CI, NATS-UPGRADE, NATS-CAP, NATS-DOCS]
status: planned
---

# Phase 14 — Supported NATS server line (tenancy T0)

<objective>Move Minga off nats-server 2.10.22, which reached end of life in May 2025, to the supported 2.15 line. Prove the bus works on both supported lines (2.14 and 2.15) in CI that never skips silently. Prove that an existing cluster's JetStream data survives the upgrade, and read the server's JetStream `api.level` so later features can be gated on what the server supports. This is step T0 of `docs/adr/0008-multi-tenancy.md` and does not depend on Phase 13-07.</objective>

## Decisions taken with the user (2026-11-16)

| Question | Answer |
| --- | --- |
| Bundled line | **2.15.3** (released 2026-11-11). Supported lines today are 2.15 and 2.14; 2.12's last patch was 2026-08-12. |
| External NATS below the floor | **Warn and continue.** A manager connected through `MINGA_NATS_URL` to a server below `api.level` 4 (2.14) logs a warning naming the server version, its level and the floor, then runs. |
| CI | **Both lines, fail closed.** The NATS suites run against 2.14.10 and 2.15.3 using the installed binary, and fail instead of skipping when no server is available. |

## Where the code is today (`main` at `df82468`, read 2026-11-16)

| Concern | Today | File |
| --- | --- | --- |
| Release bundle | `NATS_VERSION: v2.10.22`, with a `nats_sha256` per architecture in the build matrix, checked with `sha256sum -c` | `.github/workflows/release.yml:22`, `:103-105` |
| CI binary | The workspace job downloads v2.10.22 for the `minga-bus` suites | `.github/workflows/ci.yml:30-34` |
| Test images | `GenericImage::new("nats", "2.10-alpine")` in four files, each with its own copy of the start-or-skip code. A missing Docker daemon makes them **skip silently**. Only two honour `MINGA_TEST_NATS_URL`. | `crates/minga-bus/tests/{nats_cluster,nats_integration,nats_lease_expiration,nats_stream_update}.rs` |
| Cluster test | `nats_cluster.rs` needs Docker for three servers. It is the only test of `ensure_replicas` and has never run on 2.12 or newer. | `crates/minga-bus/tests/nats_cluster.rs` |
| Real binaries | `nats_tls.rs` already starts real `nats-server` processes from config (one- and two-server, TLS routes) | `crates/minga-bus/tests/nats_tls.rs:213-310` |
| Version checks | None. Nothing reads the server version or `api.level`. | `crates/minga-bus/src/nats.rs` |
| Docs | CONTRIBUTING says "CI pins v2.10.22"; ADR 0002 and ADR 0008 name the bundled line | `CONTRIBUTING.md:21`, `docs/adr/0002-event-bus-nats.md`, `docs/adr/0008-multi-tenancy.md:91` |

Already verified in PR #85 on 2.15.3: the full workspace suite passes (916 tests), and `setup_kv`'s read-modify-update of a stream config is accepted under 2.12+ strict mode. 2.15.3 reports `api.level` 5. async-nats 0.50 exposes the level (`query_account()`, `Requests::level`, default feature `server_2_11`).

## Release-note review, 2.11–2.15 (14-01, 2026-11-16)

Read in full: the v2.11.0, v2.12.0, v2.14.0 and v2.15.0 release notes, plus the 2.11, 2.12 and 2.14 upgrade notes. Skimmed: the GA patch releases v2.10.23…v2.15.3. Checked at the source: the 1,000-consumer default and `js_raft_delete_range` (v2.15.0 notes), and that each guide covers only the step from the previous minor (the 2.14 notes are "tailored for existing NATS users upgrading from NATS version v2.12.x"). Reported but **not** verified, because the 2.15 guide is not in the archived `nats-io/nats.docs` repo: a recommendation to upgrade to 2.15 "from at least v2.14.7", and downgrade from 2.15 only to v2.14.7 or later.

| Finding | Where | Minga impact | Action |
| --- | --- | --- | --- |
| **Streams default to at most 1,000 consumers** (v2.15.0; overridable per stream with `max_consumers` or server-wide with `default_max_consumers`) | `KV_MINGA_ASSIGN` | Each worker holds two long-lived ordered consumers on it: the supervisor watch (`supervisor.rs:76`) and the WireGuard peer-set watch (`wireguard.rs:270`), plus transient ones when it lists. **Somewhere around 500 workers, new watches would fail** on 2.15, far below the T4 target of 5,000. | Code fix and test: new plan **14-06**. |
| **No supported direct jump 2.10 → 2.15.** Each guide covers only the step from the previous minor. `js_raft_delete_range` became the default in 2.15. | every upgraded cluster | Bundling 2.15.3 means an existing cluster's sidecar jumps four minors at once. | **Release gate** below. 14-04 tests the direct jump first. |
| Mixed-version clusters: replicated streams stay available, but scale and move operations stall until every server is upgraded | T3 rolling upgrade | `ensure_replicas` changes replica counts | 14-04 and the docs: don't change `--nats-replicas` during a rollout. |
| Stream updates with replicas > 1 are rejected on a non-clustered server (v2.14.6) | `raise_replicas` | It only raises when the cluster has the servers; it is a no-op on a single node. | Covered by the 14-03 cluster test. |
| Insecure TLS cipher suites disabled by default (v2.12.0) | client and route TLS | none seen | `nats_tls` suite passes on 2.15.3 (10/10, PR #85). |
| Fix: `verify_and_map` authenticated users with blank passwords (v2.12.14 / v2.14.4) | worker identities (mTLS, no passwords) | Fixed in 2.15.3; the worker ACL suite passes. | Note only. |
| New `$JS.FC` subject format (`js_ack_fc_v2`): off in 2.15, default in 2.16 | worker ACL grants `$JS.FC.KV_MINGA_ASSIGN.>` | Ordered consumers would stall once the new format is on. | Never set `feature_flags`. Update the ACL before adopting 2.16. |
| Graceful SIGTERM now exits 0 (v2.11.0) | sidecar | The sidecar treats an exit as an error only during startup. | Note only. |
| Info and list API requests are queued at lower priority (v2.14.0) | `setup_kv`'s stream-info read | Slower startup under load, not a failure | Note only. |
| Config keys Minga writes (`listen`, `server_name`, `authorization`, `http`, `tls`, `cluster`, `jetstream.{store_dir,max_memory_store,max_file_store}`) | sidecar | None deprecated or renamed. Defaults changed only for TLS ciphers, JetStream strict mode, `max_buffered_*` and the consumer cap. | Covered by the rows above. |

**Release gate.** Bundling 2.15.3 is safe to merge, because releases only happen on `v*` tags. **No release may be tagged with 2.15.3 until 14-04 proves the direct 2.10.22 → 2.15.3 store upgrade and 14-06 lifts the consumer cap.** If the direct jump fails, decide with the user: an intermediate release bundling 2.14.x, or a documented stepwise upgrade.

## Design decisions

1. **Read `api.level` on managers only.** Managers call `query_account()` once after JetStream setup and keep the level on `NatsBus` (`api_level()`), for future feature gates (per-message TTL is level 1, counters 2, and so on). Workers don't: their ACL has no `$JS.API.INFO`, and it stays that way. Under ADR 0008 that endpoint returns account-wide usage, which is cross-tenant information.
2. **Below the floor is a warning, not an error.** The floor is 4 (2.14.0). The warning names the server's version (from `server_info()`), its level and the floor, and says Minga is tested only on 2.14 and 2.15. The bundled sidecar is always above the floor. The decision is a pure function, so it's unit-tested without a server.
3. **Parse no version strings.** Gate on `api.level`, never on `server_info().version`. The version appears only in the message.
4. **One test helper, and it fails closed.** `crates/minga-bus/tests/common/mod.rs` replaces the four copies:
   - It uses `MINGA_TEST_NATS_URL` when set.
   - Otherwise it uses a Docker image pinned to the exact tag `2.15.3-alpine`.
   - With `MINGA_REQUIRE_NATS=1`, a missing server panics with what to set, instead of skipping.

   The cluster test starts three real binaries (from `PATH` or `MINGA_NATS_SERVER_BIN`) using the config pattern already in `nats_tls.rs`, so it no longer needs Docker.
5. **Tests must not assume a fresh server.** CI shares one server per job across test binaries. In PR #85, a test that assumed a fresh bucket failed on a server another test had already used. Tests assert invariants, or use names unique to the test.
6. **Upgrade is proven, not assumed.** A store dir written by 2.10.22 with Minga's streams and buckets must open on 2.15.3 with data intact. A three-server cluster must survive a rolling replacement, one server at a time. Downgrade after the upgrade is not supported unless the research below shows it is; the docs say which.

## Plans

Order: 14-01 first, because it moves the pins everything else runs against. 14-02, 14-03 and 14-06 can run in parallel after it. 14-04 needs 14-03's helper. 14-05 comes last. The release gate above holds until 14-04 and 14-06 are done.

### 14-01 — Pin 2.15.3 (NATS-PIN)
- [ ] `release.yml`: `NATS_VERSION: v2.15.3`, and replace each architecture's `nats_sha256` with the value from the v2.15.3 release's `SHA256SUMS`. Fetch it from the release, don't compute it from a local download.
- [ ] `ci.yml`: the workspace job installs v2.15.3.
- [ ] Bump the four test images to `2.15.3-alpine`. 14-03 then replaces them with the shared helper.
- [x] Read the 2.11, 2.12, 2.14 and 2.15 release notes for changes that touch the sidecar config, stream or KV options Minga sets, or account limits. Findings are in "Release-note review" above; `.planning/STATE.md` points to them.
- **Done when:** a release build bundles 2.15.3 with a verified checksum, and CI runs on it.

### 14-02 — Read and gate on `api.level` (NATS-LEVEL)
- [ ] `NatsBus` (manager path only): after `setup_when_ready`, call `query_account()` and store `requests.level`. Add `pub fn api_level(&self) -> Option<usize>`, which returns `None` on workers.
- [ ] Add a pure `api_level_warning(level, version) -> Option<String>` for the floor of 4, and log the result as a warning at startup.
- [ ] Worker ACL unchanged: assert in `subjects` tests that `worker_acl` grants no `$JS.API.INFO`.
- [ ] Tests:
  - unit tests of the warning function at levels 0, 3, 4 and 5
  - an integration test on each CI line asserting the level is at least 4, and that no warning is logged
- **Done when:** a manager logs the server's level at startup, and warns below 2.14.

### 14-03 — CI on both lines, failing closed (NATS-CI)
- [ ] Add `crates/minga-bus/tests/common/mod.rs` with the helper from design decision 4. Delete the four copies.
- [ ] Port `nats_cluster.rs` to three real binaries, so `ensure_replicas` and `raise_replicas` run on 2.14 and 2.15 clusters.
- [ ] New CI job `NATS suites (never silently skipped)`:
  - a matrix over `v2.14.10` and `v2.15.3`
  - steps: install the binary, start `nats-server -js` in the background, set `MINGA_TEST_NATS_URL` and `MINGA_REQUIRE_NATS=1`, and run `cargo test -p minga-bus`
  - the TLS and cluster suites start their own servers from the installed binary
- [ ] Prove that the guard fails closed: with `MINGA_REQUIRE_NATS=1` and no server, the suite fails with the helper's message. Record that run in STATE.md.
- **Done when:** both matrix legs pass, and no NATS test can pass by skipping in that job.

### 14-04 — Upgrade proof (NATS-UPGRADE)
- [ ] **Single server:** a script under `scripts/` does this:
  1. Start v2.10.22 on a fresh store dir.
  2. Run an `#[ignore]` seeding test that creates every Minga stream and bucket and writes known keys, log entries and a lease.
  3. Stop the server, then start v2.15.3 on the same store dir.
  4. Run an `#[ignore]` verifying test: the keys and log history are intact, `NatsBus` connects (including the lease-TTL update path), and a new lease still expires.

  Run it in CI as a step of the NATS job.
- [ ] **Rolling cluster:** three servers on v2.10.22 with R3 streams (`--nats-replicas 3`). Replace them one at a time with v2.15.3. After each replacement, publishing, KV reads and a log tail must keep working, and once all three are replaced, `ensure_replicas` must be a no-op. Run it locally first. Add it to CI only if it takes under five minutes.
- [ ] Downgrade: try opening the upgraded store dir with v2.10.22 again, and record whether it works. The docs state the result.
- **Done when:** both proofs are recorded in STATE.md, with the commands and the versions used.

### 14-06 — Lift the 2.15 consumer cap on the assignment bucket (NATS-CAP)
- [ ] After `setup_kv` creates or opens `MINGA_ASSIGN`, set `max_consumers: -1` (unlimited) on `KV_MINGA_ASSIGN` with a stream update. Since 2.12.5 the server accepts that change after creation. Do it the same way for `MINGA_STATUS` and `MINGA_LEASES`, which workers read too. Don't use the server-wide `default_max_consumers`: it is unknown to servers before 2.15 and has to be removed before a downgrade.
- [ ] Test on 2.15.3: open more than 1,000 ordered consumers on `KV_MINGA_ASSIGN` and confirm the next watch still succeeds. On 2.14.10 the same test confirms the update is accepted.
- **Done when:** a cluster with more than 500 workers can't hit the cap, shown by the test above on both lines.

### 14-05 — Docs and release notes (NATS-DOCS)
- [ ] CHANGELOG: the bundled server moves 2.10.22 → 2.15.3; external NATS servers below 2.14 get a startup warning; the upgrade and downgrade result from 14-04.
- [ ] `CONTRIBUTING.md`: CI pins 2.14.10 and 2.15.3, and the `MINGA_REQUIRE_NATS=1` / `MINGA_TEST_NATS_URL` variables.
- [ ] ADR 0002 implementation status: the bundled line, and the `api.level` floor. ADR 0008: mark T0 done.
- [ ] `docs/topologies.md` (external NATS): the supported lines and the floor.
- **Done when:** STATE.md has the evidence for 14-01…14-04, and the docs match it.

## Tenancy checklist (ADR 0008)

1. **Owner.** No new state, objects, subjects or keys.
2. **Access.** `$JS.API.INFO` is used only by managers. The worker ACL is unchanged, and a test asserts it grants no `$JS.API.INFO`.
3. **Bus.** No new subjects or streams. The upgrade proof uses the existing streams and buckets.
4. **Node.** The sidecar stays one per manager. Nothing new is local to a node.
5. **Shared plane.** Nothing new on managers, Raft or JetStream. The server's existing `max_memory_store` and `max_file_store` stay pinned.
6. **Audit.** Not applicable: no API writes. The level warning goes to the manager log.
7. **Test.** Not applicable as a two-tenant test, because no tenant-owned object is added. The worker-ACL assertion in 14-02 covers the only access question.

## Risks

- **async-nats 0.50 has no `server_2_15` feature.** Fields that are new in 2.15 aren't modelled. That doesn't matter for T0, since strict mode only rejects fields the client sends, but 2.15-only features have to wait for a client update.
- **Store-format changes between 2.10 and 2.15.** 14-04 exists for this. If the in-place upgrade fails, stop and decide with the user: a stepwise upgrade (2.10 → 2.14 → 2.15), or documenting an export and re-create.
- **The rolling-upgrade proof may be too slow for CI.** If so, it stays a recorded local proof, and the docs say it isn't run in CI.
