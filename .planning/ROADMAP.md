# Minga planning roadmap

This file starts GSD tracking at Wave 10. Waves 1–9 were developed before this planning workspace; see `README.md` and `Wave-9-PLAN.md` for historical descriptions. Their implementation and deployment claims are **not** independently verified by this roadmap.

## Phase 10: Production hardening — TLS, secrets and container logs
**Goal:** Secure NATS client and cluster traffic, deliver file-based container secrets without storing values in Raft, capture real container logs, and validate a Talos/hcloud staging deployment without unsubstantiated compliance claims.
**Depends on:** Wave 9 observability baseline and a verified Talos/hcloud host architecture.
**Requirements:** W10-TLS, W10-SEC, W10-LOG, W10-STAGE, W10-COMP
**Mode:** standard
**Status:** Done (plans 10-02…10-05 landed in code; 10-01 host gate blocked by user choice — see `.planning/STATE.md`)

Sequential implementation for one developer: B (NATS TLS/transport boundary) → C (secrets) → A (stdout/stderr log capture), followed by staging and documentation. The host-feasibility and compliance-control gates must happen before claims of production readiness.

### Plans

**Wave 1 — host feasibility / blocker gate**
- [ ] `10-01-PLAN.md` — supported Talos extension service, hcloud image/ISO correction, operator staging permission.

**Wave 2 *(blocked on Wave 1)* — B: TLS**
- [x] `10-02-PLAN.md` — NATS client and route mTLS; local containerd UDS access control.

**Wave 3 *(blocked on Wave 2)* — C: secrets**
- [x] `10-03-PLAN.md` — Compose refs, safe node-local resolver, read-only OCI mount.

**Wave 4 *(blocked on Wave 3)* — A: logs**
- [x] `10-04-PLAN.md` — real Linux FIFO proof, bounded Minga-owned history and retention.

**Wave 5 *(blocked on Wave 4)* — integration and sign-off**
- [x] `10-05-PLAN.md` — authorized hcloud staging, documentation correction and compliance gap matrix.

**Cross-cutting constraints:** No values/credentials in distributed state or evidence; local containerd socket is privileged; unmasked quality gate; no certification or staging-success claims without proof.

## Phase 11: First production release (`v0.1.0`)
**Goal:** Close the last mile between "a service exists in Raft" and "a container runs on a node and is reachable", then ship an installable binary.
**Depends on:** Phase 10.
**Plans:** `docs/roadmap-v0.1.0.md` — 7 executable gates
**Mode:** standard
**Status:** Done — all 7 gates landed 2026-10-31…2026-11-01; evidence per gate in `.planning/STATE.md`

- [x] Gate 1 — cluster knows its own nodes: self-registration + lease, `POST /nodes`, `POST /nodes/{id}/update`, zero-eligible-node deploys are hard errors. Also landed: Raft write forwarding, scheduler on API nodes, sidecar NATS config fix, bounded Raft frames, JetStream setup retry.
- [x] Gate 2 — `minga deploy` deploys (shared write path with the Docker API; per-service placement report; verified live with the real `docker` CLI against `minga api`).
- [x] Gate 3 — containers talk to each other (bridge + veth + netns per task, IPAM into `TaskSpec.ips`, iptables DNAT publishing, embedded DNS on bridge gateways). Verified live on OrbStack Ubuntu; multi-node WireGuard and host-local hairpin are known limits.
- [x] Gate 4 — Compose fidelity (command/entrypoint/labels/healthcheck/volumes/constraints reach the container; task lifecycle states and NetworksAttachments in the API). Verified: Traefik v3 swarm provider routes to a Minga service, 10/10 200 OK.
- [x] Gate 5 — transport security and secrets staging (Raft mTLS verified live, API bearer auth 401/200 live, NATS token, `minga tls` PKI, content-addressed per-task secret staging with rotation semantics).
- [x] Gate 6 — real container logs (FIFO destinations, per-container bounded readers, JetStream history). Verified live: separated streams, backlog + follow surviving a supervisor restart.
- [x] Gate 7 — shippable: CI (incl. daemon-backed job with `MINGA_REQUIRE_CONTAINERD=1`), linux/amd64 + linux/arm64 release workflow, `install.sh`, v0.1.0 version bump, CHANGELOG, corrected README/Talos docs; clean-checkout proof green.

## Phase 12: Managers-only Raft
**Goal:** Only managers vote. Workers become JetStream clients that get assignments, report status and keep server-stamped leases on the bus, and can join a running cluster. Managers can be added and removed at runtime, and `minga api` folds into the manager.
**Depends on:** v1.2.0 (shipped 2026-11-13). Releases 1.0.1–1.2.0 were done outside this planning workspace; see `CHANGELOG.md`.
**Requirements:** MOR-ADR, MOR-LEASE, MOR-STATUS, MOR-ASSIGN, MOR-AUTHZ, MOR-JOIN, MOR-FOLD, MOR-NATS, MOR-PART, MOR-PROOF
**Plans:** `.planning/phases/12-managers-only-raft/12-PLAN.md`
**Mode:** standard
**Status:** Done (12-01…12-07 merged; live evidence in `.planning/STATE.md` and `CHANGELOG.md`)

- [x] 12-01 — ADR 0006: Workers leave Raft (no code)
- [x] 12-02 — Liveness and registration off Raft
- [x] 12-03 — Actual task state off Raft
- [x] 12-04 — Assignment KV, workers without a StateStore, one dispatch path, NATS subject permissions
- [x] 12-05 — Join and remove managers, cluster the managers' NATS sidecars (can run in parallel with 12-02…12-04)
- [x] 12-06 — Fold `minga api` into the manager
- [x] 12-07 — Live proof, docs, CHANGELOG, site roadmap

## Phase 13: WireGuard mesh and routing mesh
**Goal:** Containers on different nodes reach each other over kernel WireGuard, with the same names and IPs, including edge workers behind NAT (relayed through managers). Published ports in Swarm's default `ingress` mode answer on every node.
**Depends on:** Phase 12 (registration over the bus, `MINGA_ASSIGN` and its write permissions). 13-02 is independent and can land first.
**Requirements:** WG-ADR, WG-SUBNET, WG-KEYS, WG-PEERS, WG-NAT, WG-ISOL, WG-INGRESS, WG-PROOF
**Plans:** `.planning/phases/13-wireguard-mesh/13-PLAN.md`
**Mode:** standard
**Status:** In progress

- [x] 13-01 — ADR 0007: WireGuard mesh and routing mesh (no code)
- [x] 13-02 — Unique subnets and strict network `driver_opts` (independent of Phase 12)
- [x] 13-03 — Node keys and the `minga-wg0` interface
- [x] 13-04 — Peer sets, /32 routes, proxy ARP, cross-node isolation (real-kernel CI evidence)
- [x] 13-05 — NAT traversal by relay through managers (real-kernel CI evidence)
- [x] 13-06 — Routing mesh (`mode: ingress` as in Swarm, cluster-wide port reservation)
- [ ] 13-07 — Live proof on VMs, iperf3 numbers, docs, CHANGELOG, site roadmap

## Phase 14: Agent-ready operations and painless management (pre-v1.3.0)
**Goal:** Make operational facts trustworthy, common failure modes diagnosable, upgrades predictable, and Minga safely usable by automation and AI agents. Agent writes require server-side authorization and human approval.
**Depends on:** Phase 13 implementation (13-01…13-06); 13-07 release-candidate proof remains a release gate.
**Requirements:** OPS-VERSION, OPS-RAFT, OPS-COMPOSE, OPS-DOCTOR, OPS-UPGRADE, AGENT-CONTRACT, AGENT-AUTHZ, AGENT-APPROVAL, RC-PROOF
**Plan:** `.planning/phases/14-agent-ready-operations/14-PLAN.md`
**Mode:** standard
**Status:** Planned for v1.3.0

- [ ] 14-01 — Report exact version/build and persisted Raft membership, leader and quorum; separate these from bootstrap `--peer` inputs.
- [ ] 14-02 — Make Compose fidelity explicit; implement `deploy.labels` or reject it before state changes.
- [ ] 14-03 — Add read-only `minga-ctl doctor` and redacted support evidence.
- [ ] 14-04 — Add upgrade compatibility/preflight plans; never wipe or restart as a planning side effect.
- [ ] 14-05 — Define stable structured agent tools; begin with allow-listed read-only access.
- [ ] 14-06 — Add server-enforced roles, audit and maker-checker change proposals before enabling agent writes.
- [ ] 14-07 — At the v1.3.0 release-candidate gate, complete Phase 13 multi-VM proof and validate upgrade/staging/agent workflows. No VM testing before the candidate is ready.
