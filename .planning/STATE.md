# Planning state

**Phase:** 11 — First production release (`v0.1.0`)
**Status:** Roadmap established; Gate 1 (cluster membership) is the critical path
**Plans:** See `docs/roadmap-v0.1.0.md` — 7 gates
**Last Activity:** 2026-10-27

Waves 1–9 predate this GSD planning workspace and their claims were **not** independently verified. Wave 10 (production hardening) was executed as planned: B (NATS TLS), C (secret references) and A (container logs) are landed in code, with the containerd lifecycle proven against a live daemon.

**Wave 10 outcome, verified:** the real containerd lifecycle test passes with `MINGA_REQUIRE_CONTAINERD=1` — 4 passed, 0 failed, repeated three times with no leftover task, container or snapshot. The `containerd_client` suite is 6/6 against the same daemon. Separate unpiped `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings` and `cargo test --workspace` each exited 0 on macOS. Two genuine runtime bugs were found only on the real daemon and fixed: containerd refuses to delete a container that still has a task (a task whose container is gone can never be deleted again), and task deletion raced process exit.

**What Phase 11 adds.** A source audit plus a new executable probe (`crates/minga-cli/tests/deploy_path_probe.rs`, 3 tests) established four blockers that prevent a real deployment, none of which the earlier waves exposed because the tests supplied their own scaffolding:

- **B1 — no node registration.** `upsert_node` is called only from test files; no production code constructs a `ClusterNode`. A cluster starts with zero nodes, so the scheduler places nothing and the deploy looks successful. Verified by execution: `nodes_in_state=0 → reconcile_actions=0 → supervisor_running=0`; after registering a node, `reconcile_actions=2 → supervisor_running=2`.
- **B2 — `minga deploy` does not deploy.** It translates and prints (`crates/minga-cli/src/main.rs:288-300`). The Docker API path does write to Raft and does work.
- **B3 — no container network.** `minga-net` generates configuration only; no `wg`/`nft`/`ip` call exists, the `OverlayManager` is never invoked by the node path, task IPs are always empty, and the OCI network namespace has no interface.
- **B4 — Compose fidelity.** `command`, `entrypoint`, `labels`, `healthcheck`, volumes and placement constraints parse but are dropped before reaching the container.

**Transport security state:** NATS TLS is production-shaped (no insecure escape hatch, CA + client cert required, mandatory SAN verification). Raft is plain TCP with no TLS, no authentication, and an unbounded length-prefix allocation. The Docker API has no authentication and binds `0.0.0.0:2375` by default. The workspace has no TLS dependency and no PKI lifecycle code.

**10-01 host gate remains BLOCKED** by user choice: Talos staging is out of scope for `v0.1.0` and no hcloud provisioning is authorized. OrbStack Ubuntu is Linux evidence only. Do not advance Talos-dependent claims or compliance qualification — FIPS, SOC 2, VS-NfD and NATO are **not assessed**, which is different from not applicable.

**Immediate next action:** Gate 1 — node self-registration, `POST /nodes`, and a hard failure (not a warning) when a service matches zero eligible nodes.
