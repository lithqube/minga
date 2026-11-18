---
phase: 13
title: WireGuard mesh and routing mesh
depends_on: ["12"]
requirements: [WG-ADR, WG-SUBNET, WG-KEYS, WG-PEERS, WG-NAT, WG-ISOL, WG-INGRESS, WG-PROOF]
status: in_progress
---

# Phase 13 — WireGuard mesh and routing mesh

<objective>Containers on different nodes reach each other over an encrypted WireGuard mesh, using the same names and IPs they use on one node today. That includes edge workers behind NAT that can only dial out. A published port in Swarm's default `ingress` mode answers on every node and forwards to a task anywhere in the cluster. This removes structural limit 5 in `docs/architecture.md`.</objective>

## Decisions taken with the user (2026-11-15)

| Question | Answer |
| --- | --- |
| Order relative to Phase 12 | **After Phase 12.** Keys and endpoints travel with Phase 12's bus registration, and peer sets reach nodes through `MINGA_ASSIGN`. |
| Reachability | **Some workers are behind NAT** with no inbound port. The design relays through managers when no direct path exists. |
| Data path | **Kernel WireGuard only** (Linux 5.6 or newer; Talos ships it). A node that needs peers and lacks it fails loudly. |
| Routing mesh | **Yes.** Compose `mode: ingress` behaves as in Swarm. |

## Where the code is today (v1.2.0, read 2026-11-15)

| Concern | Today | File |
| --- | --- | --- |
| WireGuard | `KeyPair`, `InterfaceConfig` and peer config strings are generated but never applied. `OverlayManager` keeps a WireGuard port **per network**. Its only caller is a `minga net` debug printout. | `crates/minga-net/src/{wireguard,overlay}.rs`, `crates/minga-cli/src/main.rs:1403` |
| Subnets | One /24 per network, cluster-wide. Without a Compose `ipam` subnet, it's `hash(network_id) % 256` inside `10.100.0.0/16`, with **no collision check**: at about 19 networks there's roughly a 50% chance two share a subnet. The node's bridge then breaks, and across nodes the routes would too. The hash is `std`'s `DefaultHasher`, whose output Rust doesn't promise to keep across releases, so a toolchain bump could also move the subnet of every network that has none stored. | `crates/minga-net/src/ipam.rs:179`, `crates/minga-node/src/netns.rs:218` |
| Task IPs | Allocated by the scheduler into `TaskSpec.ips`, unique per network across the cluster, and stored in Raft. | `crates/minga-scheduler/src/net*.rs:64` |
| Data plane | One bridge per network on each node. Every node's bridge has **the same gateway IP** (the first usable address), which also serves DNS. veth pairs, a netns per task, iptables DNAT for published ports, and FORWARD isolation between bridges, all driven through `ip` and `iptables`. | `crates/minga-node/src/netns.rs` |
| Published ports | DNAT only on the node running the task. Compose `ports[].mode` (`ingress`/`host`) is parsed and **never used**. | `crates/minga-compose/src/model.rs:250` |
| Network options | Network `driver_opts` is parsed and **silently dropped**, which breaks the CLAUDE.md rule that unknown keys fail loudly. | `crates/minga-scheduler/src/translate.rs:495` |
| CI | One job runs on a real kernel (`Published-port paths on a real kernel`) and can host several network namespaces acting as nodes. | `.github/workflows/ci.yml:130` |

## Design decisions for ADR 0007 (plan 13-01)

1. **One WireGuard interface per node.** It's called `minga-wg0`, listens on one UDP port (`--wg-listen`, default 51820), and uses one keypair. The per-network ports in `OverlayManager` go away. The private key is generated at first start into `<data_dir>/wg.key` with mode 0600, and never leaves the node. The public key, endpoint and reachability travel in the Phase 12 registration payload, and the leader stores them on `ClusterNode` as new `#[serde(default)]` fields (no format bump).
2. **Routed L3, one subnet per network, no IPAM change.** A network keeps one subnet across the cluster, as Docker's overlay does, and task IPs stay cluster-unique as today. A remote task's IP is a /32 in its node's peer `AllowedIPs`, plus a /32 route through `minga-wg0`. `proxy_arp` on each bridge answers ARP for remote same-subnet IPs, so containers still see one flat /24. The gateway IP stays the same on every node and acts as the local DNS resolver. Rejected: VXLAN over WireGuard (Swarm's L2 overlay), which means two encapsulations, FDB and ARP management, and a smaller MTU. `shortcut:` /32 entries churn with every task start and stop. If that hurts at T4, upgrade to per-node address blocks inside each subnet (Calico-style block affinity).
3. **Subnets are allocated, not hashed (WG-SUBNET).** The leader picks a free /24 when a network is created, and stores it in Raft (`Network.subnet` already exists). A Compose `ipam` subnet that overlaps an existing network fails loudly. Existing networks that collide are reported at startup with the fix (recreate one of them). The supernet becomes configurable (`--overlay-supernet`), and a node refuses one that overlaps its own host routes. This plan doesn't depend on Phase 12 and can land first.
4. **Peer sets are computed by the leader and delivered through `MINGA_ASSIGN` (WG-PEERS).** Key `node.{id}.wg`: the peers this node needs, each with a public key, an endpoint (or none), `AllowedIPs` (the /32s of that peer's tasks on networks this node also hosts) and a direct or relay mark. A node peers only with nodes it shares a network with, plus the managers. It's on demand, not a full mesh, so 5,000 workers don't mean 5,000 peers each. The node applies a diff and never flushes and rebuilds. Only managers can write this key (Phase 12 MOR-AUTHZ).
5. **NAT: keepalive to managers, relay when neither side is reachable (WG-NAT).**
   - Every node peers with every manager.
   - Managers must have a reachable UDP endpoint. That's a documented requirement, and a manager without `--wg-endpoint` fails to start in a multi-node cluster.
   - A worker sets `--wg-endpoint` when it's reachable, or leaves it out when it's behind NAT. Nodes behind NAT send `PersistentKeepalive = 25` to every peer.
   - A pair connects directly when at least one side is reachable; WireGuard learns the NAT'ed side's endpoint from its handshake.
   - When neither side is reachable, the pair relays through the worker's home manager (deterministic: lowest ID among healthy managers). The home manager's peer entry carries the relayed /32s, and the manager forwards `minga-wg0 → minga-wg0` for overlay subnets only.
   - **A relay manager decrypts and re-encrypts, so it sees relayed container traffic in clear.** Managers are already fully trusted (they hold desired state), but the ADR and docs must say so.
   - Out of scope, and the first follow-up: endpoint auto-discovery and UDP hole punching.
6. **Isolation and anti-spoofing hold across nodes (WG-ISOL).** WireGuard's cryptokey routing only accepts a packet from a peer whose source IP is in that peer's `AllowedIPs`, so a node can't send as another node's task. FORWARD rules accept `minga-wg0 → bridge` only when the source is in that bridge's subnet, which extends today's bridge-to-bridge isolation across nodes. Traffic for a remote task's loopback or for the node's own services stays blocked, as in v1.2.0.
7. **MTU.** `minga-wg0` uses 1420 (`--wg-mtu`). Overlay bridges and veths use the same value once the mesh is on, and TCP MSS is clamped on forwarded traffic. Single-host clusters keep 1500, and their data plane doesn't change.
8. **The routing mesh follows Swarm (WG-INGRESS).**
   - The default publish mode is `ingress`, as in Swarm. `mode: host` keeps today's behaviour: only the node running the task answers, and the client IP is preserved.
   - An ingress-published port answers on **every** node. It's DNAT'ed to one of the service's task IPs, local or remote, through an iptables chain per published port using `statistic --mode random`, and SNAT'ed when the backend is remote so replies come back through the ingress node. As in Swarm, ingress mode loses the client IP. The docs point at `mode: host` for that.
   - The ingress backend sets are published to every node in `MINGA_ASSIGN` (`ingress.{port}/{proto}`).
   - Port conflicts become a cluster-wide check. The scheduler refuses an ingress port that another service already holds, or that any node reserves, with a 409.
   - Rejected: IPVS. It's faster at a very large backend count, but it needs another kernel module and its own tooling. That's the upgrade path if iptables chains get too slow.
   - Out of scope: service VIPs (`endpoint_mode: vip`). DNS keeps returning task IPs (dnsrr-like) as today.
9. **The control plane stays off the mesh.** Raft and NATS stay on the underlay, behind their existing mTLS. Peer sets travel over NATS, so putting NATS on the mesh would be circular.
10. **Kernel tools follow the netns pattern.** Use `ip link add type wireguard` and `wg set ... private-key <file>` (the key goes in a file, never as an argument), the same way `netns.rs` drives `ip` and `iptables`. Checks at start: the kernel module and the `wg` binary are present, otherwise there's a clear error. `shortcut:` this depends on wireguard-tools. If a Talos image ships without `wg`, switch to netlink (`wireguard-control` or similar) in 13-03, not later.
11. **Compose strictness.** Network `driver_opts` stops being dropped. `encrypted` is accepted, and is a no-op because every cross-node packet is encrypted. Any other key fails loudly.

## Plans

Order: 13-01 first. 13-02 is independent of Phase 12 and can start any time. 13-03 → 13-04 → 13-05 need Phase 12's 12-02 (registration) and 12-04 (`MINGA_ASSIGN` and its write permissions). 13-06 needs 13-04. 13-07 comes last.

### 13-01 — ADR 0007: WireGuard mesh and routing mesh
- [ ] Write `docs/adr/0007-wireguard-mesh.md` with decisions 1–11, the rejected options (VXLAN over WireGuard, a full mesh, IPVS, the userspace data path), and the relay trust statement.
- **Done when:** the ADR is merged. No code changes in this plan.

### 13-02 — Unique subnets and strict network options (WG-SUBNET)
- [ ] The leader allocates a free /24 when a network is created, on every path: Compose translate and the API's network create. It's stored in `Network.subnet`.
- [ ] A Compose `ipam` subnet that overlaps another network gets a 409 with both names. The node no longer calls `allocate_subnet` for a network whose subnet isn't in Raft; it reports an error instead.
- [ ] At startup, report existing colliding networks with the fix.
- [ ] Add `--overlay-supernet`, refused when it overlaps the node's host routes.
- [ ] Network `driver_opts`: accept `encrypted`, and fail loudly on anything else.
- [ ] Tests: 300 networks get 256 distinct subnets and then a clear "supernet exhausted" error; an overlapping `ipam` subnet is refused; existing Compose fixtures still deploy unchanged.
- **Done when:** no code path can give two networks the same subnet.

### 13-03 — Node keys and the WireGuard interface (WG-KEYS)
- [x] Generate the keypair at first start into `<data_dir>/wg.key` (0600), reusing `KeyPair`. Add `--wg-listen`, `--wg-endpoint` and `--wg-mtu`.
- [x] The registration payload (from 12-02) carries the public key and endpoint. The leader stores them on `ClusterNode` as `#[serde(default)]` fields; the frozen fixtures remain unchanged.
- [x] Create `minga-wg0` only when the node has at least one peer; single-host clusters are unchanged. If the kernel module or `wg` is missing, report a clear error.
- [x] Remove the network-scoped WireGuard config API from `OverlayManager`; the `minga net` output no longer advertises an interface per network.
- [x] Key rotation: delete `wg.key` and restart. Registration refreshes the public key, and the leader reconciles peer assignments.
- [x] Tests: key file mode, stable public key across restarts, and old `ClusterNode` rows decoding with no WireGuard fields.

### 13-04 — Peer sets, routes and cross-node isolation (WG-PEERS, WG-ISOL)
- [ ] The leader computes each node's peer set (decision 4) from Raft placement, and writes `node.{id}.wg` when a placement or node changes. On election it re-derives the whole set, as 12-04 does for assignments.
- [ ] The node applies a diff: peers, `AllowedIPs`, /32 routes, `proxy_arp` on overlay bridges, the MTU, MSS clamping, and the `minga-wg0 → bridge` FORWARD rules from decision 6. Teardown removes all of it.
- [ ] DNS: records already include every task of a service (from cluster state through the 12-04 `net.{id}` data), so remote IPs resolve with no change. Check this with a test.
- [ ] Tests in the real-kernel CI job, with three network namespaces acting as nodes, each with its own `minga-wg0`:
  - Same-network ping and HTTP across "nodes", by name.
  - A different-network task across "nodes" is blocked.
  - A packet spoofing another node's task IP is dropped.
  - A 64 KiB payload crosses without fragmentation errors.
  - Removing a task removes its /32 everywhere.
- **Done when:** the CI job proves all of the above, and never skips silently, like the existing real-kernel job.

### 13-05 — NAT traversal by relay (WG-NAT)
- [ ] Keepalive from nodes without an endpoint. A manager in a multi-node cluster refuses to start without `--wg-endpoint`.
- [ ] The leader marks each pair direct or relayed, and picks a home manager for each NAT'ed worker. That manager carries the relayed /32s, with `ip_forward` on and a FORWARD rule `minga-wg0 → minga-wg0` for overlay subnets only.
- [ ] Failover: when the home manager's lease expires, the leader moves the relay to the next manager.
- [ ] Tests in the same real-kernel job, with two "workers" behind iptables MASQUERADE that have no inbound path:
  - Worker ↔ manager is direct.
  - Worker ↔ worker relays through the home manager.
  - Killing the home manager moves the relay within one lease TTL plus one handshake.
  - A NAT'ed worker ↔ a reachable worker goes direct after the NAT'ed side's first handshake.

### 13-06 — Routing mesh (WG-INGRESS)
- [x] Honor `ports[].mode`: `ingress` is the default, as in Swarm, and `host` keeps today's DNAT path.
- [x] The scheduler reserves ingress ports cluster-wide and returns a 409 on conflict, including with ports a node reserves.
- [x] The leader publishes `ingress.{port}/{proto}` backend sets to every node.
- [x] Every node installs a chain per published port, using `statistic --mode random` across backends, and SNATs to remote backends.
- [x] The Docker API reports `PublishMode` correctly (Traefik and `docker service inspect` read it).
- [x] Tests:
  - [x] With 2 replicas on node B, a published port on node A answers 20 out of 20 times and reaches both backends.
  - [x] `mode: host` uses node-local publishing; the real-kernel test reaches local addresses and does not redirect a non-local external address (no two-node host-mode topology run).
  - [x] A port conflict returns a 409.
  - [x] Backends update when a task moves.
- [x] The CHANGELOG states the behaviour change plainly: existing stacks with short-syntax ports now publish on every node.
**Tenancy checklist (ADR 0008).** T1–T3 are implemented. T4 bus isolation and T5 quotas/audit remain open; these notes describe ingress behavior and remaining tenancy constraints.
1. **Owner.** An ingress assignment is derived from the service that publishes the port. Port/protocol reservations are currently cluster-wide; the assignment keys still lack a tenant segment, which T4 must add.
2. **Access.** The tenant-scoped service API and state-machine commit path return a generic cross-tenant port conflict without naming the other tenant's service. Same-tenant conflicts retain the service name.
3. **Bus.** Only the leader writes assignments, and a worker reads its node's keys. The current node-only ACL is not tenant-scoped; T4 must add the tenant segment and constrain workers to their tenant.
4. **Node.** T3 scopes WireGuard peers and routes by tenant and excludes managers from tenant workload placement in multi-tenant clusters. The ingress mesh still needs tenant-scoped port opening and backend exposure; cross-node behavior remains unsupported until the deferred VM proof passes.
5. **Shared plane.** Writes are bounded by the number of published ingress ports (one assignment per node and port). There is no per-tenant port quota yet, so one tenant could claim the shared port space; tenancy needs a quota. A node's registered reserved ports are capped at 32 and validated.
6. **Audit.** Assignment writes come from the leader's reconcile loop, not a user request. T5 still needs per-tenant audit records.
7. **Test.** The T3 scheduler integration test covers tenant placement and WireGuard peer projection; no two-tenant ingress isolation test exists. Multi-VM proof remains deferred until release readiness.

### 13-07 — Live proof, numbers and docs (WG-PROOF)
Deferred until release readiness by user instruction. Do not run VM tests during Phase 15. The implementation through 13-06 is complete; cross-node networking remains unsupported until this proof passes.
On separate VMs (OrbStack Ubuntu and others), with Raft mTLS and NATS mTLS:
- [ ] **T2:** 1 manager plus 2 workers, one of them behind NAT (iptables MASQUERADE on its VM, no inbound path). Cross-node calls by name work in every direction, worker ↔ worker relays through the manager, and cutting the NAT'ed worker's network for 2 minutes recovers on its own.
- [ ] **T3:** 3 managers plus 2 workers. Killing the home manager moves the relay, and traffic resumes.
- [ ] Traefik on node A routes to a service whose only task is on node B, both through its swarm provider and through an ingress-published port.
- [ ] Isolation: a task on network X on node A can't reach network Y on node B. A spoofed source is dropped.
- [ ] Numbers: iperf3 throughput and latency between tasks over the mesh, compared with the underlay, both direct and relayed, recorded in `docs/benchmark.md`.
- [ ] Docs:
  - `docs/topologies.md`: cross-node traffic becomes supported, with the NAT and relay requirements, the open UDP port, the relay trust statement, and ingress versus host mode.
  - `docs/architecture.md`: remove limit 5.
  - CHANGELOG: known limits stated honestly.
  - `site/src/data/roadmap.ts`: move the WireGuard mesh to shipped.
- **Done when:** the evidence for each item above is recorded in `.planning/STATE.md`.

## Cross-cutting constraints
- Kernel WireGuard only. Hot-path packet handling stays in the kernel, so no Minga code is on the per-packet path.
- No secret on a command line, and no WireGuard private key in Raft, on the bus or in logs.
- Single-host clusters (T1) see no data-plane change: no interface and an MTU of 1500.
- Compose files deploy unchanged. The ingress default is Swarm's behaviour, and the CHANGELOG calls it out.
- Each plan runs `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings` and `cargo test` before it merges.

## Open questions (not blocking the ADR)
- Endpoint auto-discovery and UDP hole punching through the managers, so NAT'ed pairs go direct instead of relaying: next phase, or part of 13-05?
- Ingress mode loses the client IP, as Swarm's does. Is PROXY protocol or `mode: host` guidance enough, or do sites need client IPs through ingress?
- IPv6 overlay and IPv6 underlay endpoints are out of scope. Is any target site IPv6-only?
