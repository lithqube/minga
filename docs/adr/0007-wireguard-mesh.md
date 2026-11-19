# ADR 0007: WireGuard Mesh and Routing Mesh

**Status:** Accepted — implemented in Phase 13  
**Date:** 2026-11-15  
**Deciders:** Ferru

## Context

Minga currently creates a bridge and network namespace per Compose network on each node. A task's network is local to that host: a task on another node cannot be reached by its overlay IP or service name. WireGuard keys and peer configuration can be generated, but no interface is configured; the existing `OverlayManager` allocates a WireGuard port per network. Published ports are DNATed only on the task's node, and the Compose `ports[].mode` is parsed but not honored. Network `driver_opts` are currently dropped, which also violates the rule that unsupported Compose keys fail loudly.

The Phase 12 managers-only Raft design supplies the needed control plane: managers own desired state and placement, and managers alone may write `MINGA_ASSIGN`. Each worker reads its own assignment projection. The mesh must not move Raft or NATS onto itself; that would make control-plane reachability depend on a data plane configured by that same control plane.

The target is kernel WireGuard on Linux, routed L3 between existing per-network bridges, including workers that cannot accept inbound connections. Compose network subnets and task IPs remain cluster-wide. The cluster uses the same task IPs and DNS names as the single-node topology.

## Decision

### 1. One WireGuard interface and keypair per node

Each node uses one interface, `minga-wg0`, with one UDP listen port (`--wg-listen`, default `51820`) and one WireGuard keypair. Generate the private key on first start at `<data_dir>/wg.key` with mode `0600`; it never leaves the node. The public key, endpoint and reachability are registration data and are stored by the leader on `ClusterNode` in new `#[serde(default)]` fields. No persisted-format bump is needed for these additive fields.

`--wg-endpoint` advertises a reachable endpoint, and `--wg-mtu` controls the interface MTU. Key rotation follows the operator-run certificate rotation model: replace `wg.key` and restart; the leader then updates peer sets containing that node.

### 2. Routed L3 over one subnet per network

Keep one subnet per network across the cluster, and keep task IPs cluster-unique. A remote task's address is installed as a `/32` in the relevant WireGuard peer's `AllowedIPs` and routed through `minga-wg0`. Enable `proxy_arp` on each overlay bridge so a task can reach a remote same-subnet task without changing the container's view of the flat subnet. The gateway IP remains the same on every node and continues to serve DNS.

Do not add VXLAN over WireGuard. That would add a second encapsulation, FDB and ARP management, and a smaller MTU. Per-task `/32` routes are intentionally accepted for this phase; they churn as tasks start and stop. If route churn becomes a T4 bottleneck, per-node address blocks with block affinity are a later optimization.

### 3. Allocate collision-free subnets

The leader allocates a free `/24` when a network is created and stores it in the existing `Network.subnet` field. A requested Compose IPAM subnet that overlaps an existing network is rejected with a clear error. At startup, existing collisions are reported with the remediation (recreate one network). The cluster supernet is configurable with `--overlay-supernet`; startup refuses a supernet that overlaps the node's host routes.

Do not hash a network ID into a fixed `/16`: collisions become likely as networks accumulate, and Rust's `DefaultHasher` does not promise stable results across releases. Unique subnet allocation is independent of Phase 12 and may land before the mesh implementation.

### 4. The leader computes on-demand peer sets

The leader derives each node's peer set from placement and writes it to `MINGA_ASSIGN` as `node.{id}.wg`. A peer entry contains its public key, optional endpoint, the `/32` `AllowedIPs` for its tasks on networks this node also hosts, and whether the path is direct or relayed. Nodes peer with every manager and with workers sharing at least one network. This is not a full mesh: unrelated workers do not become peers, so a 5,000-worker cluster does not create 5,000 peers on every node.

The leader re-derives peer sets after election, as it does other assignment projections. Only managers may write these keys, under Phase 12's certificate-bound NATS authorization. A node applies incremental peer changes and must not flush and rebuild its whole interface on each update.

### 5. NAT traversal uses keepalive and manager relay

Every node peers with every manager. A manager in a multi-node cluster must advertise a reachable UDP endpoint with `--wg-endpoint`; a manager without one refuses to start in that topology. A reachable worker may also advertise an endpoint. A worker behind NAT omits the endpoint and sends `PersistentKeepalive = 25` to each peer.

When either side of a worker pair is reachable, establish a direct WireGuard path and let WireGuard learn the NAT-side endpoint from its handshake. When neither side is reachable, relay through the worker's home manager: the lowest-ID healthy manager selected for that worker. The relay manager has the relayed `/32` routes and forwards `minga-wg0` to `minga-wg0` only for overlay subnets. If the home manager's lease expires, the leader selects the next healthy manager and republishes peer sets.

A relay manager decrypts and re-encrypts relayed traffic and therefore sees container traffic in clear text. Managers are trusted control-plane members, but this is an explicit trust boundary and must be stated in operator documentation. Endpoint auto-discovery and UDP hole punching are out of scope; relaying is the initial behavior when neither side is reachable.

### 6. Preserve isolation and prevent source spoofing

WireGuard cryptokey routing accepts packets only when the source address belongs to that peer's `AllowedIPs`; a node cannot send as another node's task. Forwarding from `minga-wg0` to a bridge accepts only sources within that bridge's subnet, extending the existing network isolation across nodes. Traffic to a remote task's loopback and to a node's own services remains blocked.

The implementation must test cross-network isolation and spoofed source rejection with real network namespaces, not only inspect generated rules.

### 7. Use a 1420-byte mesh MTU

The `minga-wg0` interface uses MTU 1420 (`--wg-mtu`). Overlay bridges and task veths use the same MTU when the mesh is active, and forwarded TCP traffic gets MSS clamping. A single-host cluster has no WireGuard interface, keeps MTU 1500, and does not change its data plane.

### 8. Match Swarm's ingress and host publishing modes

Compose published ports default to `ingress`, as in Swarm. `mode: host` retains current behavior: only the node running the task answers, with the client IP preserved. An ingress port answers on every node and forwards to a local or remote task. The leader publishes `ingress.{port}/{proto}` backend sets to every node through `MINGA_ASSIGN`.

Each node installs a chain per ingress-published port. `statistic --mode random` selects a backend; remote backends are SNATed so replies return through the ingress node. As in Swarm, ingress mode does not preserve the client IP; operators needing it use `mode: host`. Ingress ports are reserved cluster-wide, including ports reserved by any node; a conflict is rejected with HTTP 409.

Do not use IPVS in this phase: it is faster at very large backend counts but adds a kernel module and separate operational tooling. It remains an upgrade path if iptables chains become a measured bottleneck. Service VIPs (`endpoint_mode: vip`) are also out of scope; DNS continues returning task IPs (DNS round-robin behavior).

### 9. Keep the control plane on the underlay

Raft and NATS stay on the existing underlay and retain their current mTLS. Peer sets travel over NATS. Putting NATS on the WireGuard mesh would be circular: the control plane would depend on a mesh whose configuration it distributes.

### 10. Configure the kernel with existing host tools

Follow the existing `netns.rs` pattern: create the interface with `ip link add type wireguard` and configure it using `wg set ... private-key <file>`. Never pass a private key as a command-line argument, write it to Raft or the bus, or log it. At startup, a node that needs peers checks for both the kernel WireGuard module and the `wg` executable and fails with a clear error if either is missing. A single-host node with no peers remains unchanged.

This phase uses kernel WireGuard, not a userspace packet path; packet processing remains in the kernel. If a supported Talos image lacks `wireguard-tools`, switch to a netlink implementation such as `wireguard-control` in the interface plan rather than adding a userspace data path.

### 11. Keep Compose network options strict

Do not silently discard `driver_opts`. Accept the `encrypted` option as a no-op because every cross-node packet is encrypted. Reject any other unsupported driver option loudly. Existing Compose files continue to deploy unchanged.

## Topology implications

| Shape | Mesh behavior | Requirements and limits |
| --- | --- | --- |
| **T1 Single host** | No WireGuard interface; existing bridge data plane and MTU 1500. | No change to local-only networking. |
| **T2 Edge site** | Manager is the reachable home relay; a worker behind NAT can dial out and relay to another isolated worker. | Manager needs a reachable UDP endpoint; temporary relay traffic is visible in clear text to that trusted manager. |
| **T3 HA site** | Nodes peer with managers; relay ownership moves when a manager lease expires. | Each manager must advertise a reachable UDP endpoint. Three or five managers retain the Phase 12 quorum model. |
| **T4 Large on-prem** | Peer sets are on-demand and based on shared networks, not a worker-wide full mesh. | The plan's functional behavior does not establish 5,000-worker performance; measure before claiming that scale. |

Cross-node container traffic remains unavailable until the peer, route and relay plans land. IPv6 overlay and IPv6-only underlays are out of scope for this ADR.

## Rejected alternatives

- **VXLAN over WireGuard:** two encapsulations, FDB/ARP management and reduced MTU; routed L3 with proxy ARP preserves the container's subnet view.
- **Full worker mesh:** unnecessary peers and routes for workers that share no network; the leader publishes only relevant peers plus managers.
- **IPVS:** additional module and operational surface before iptables is measured as a bottleneck.
- **Userspace WireGuard data path:** packet processing would leave the kernel hot path; kernel WireGuard is required.
- **Leaf nodes on workers:** adds a process per worker and is unnecessary for the initial minute-scale partition behavior.
- **Automatic endpoint discovery and hole punching:** deferred; relay through a manager is deterministic and testable.

## Consequences

- Cross-node task networking uses kernel WireGuard and existing bridge/task IPs; the control plane remains on the underlay.
- A relay manager is a trusted observer of relayed plaintext traffic.
- Ingress publishing works from every node but loses the client IP; host mode preserves it and only answers on the task's node.
- The single-host topology is unchanged. Networks need unique cluster-wide subnets before the mesh can be enabled.
- IPv6 overlay support, VIP routing, hole punching and any 5,000-worker load claim remain out of scope.

## Implementation status (2026-11-19)

- Phase 13 implemented cluster-wide subnet allocation, node WireGuard interfaces, tenant-scoped peer/route assignments, NAT relay through managers, cross-network isolation and ingress publishing. Since ADR 0008 the peer assignment key carries the tenant: `{tenant}.node.{id}.wg`.
- T2/T3 live proof passed, including cross-node traffic, relay failover and ingress. Short iperf3 results are topology-specific measurements, not capacity estimates.
- Multi-manager topologies require reachable manager UDP endpoints. A manager relaying between NAT-isolated workers decrypts and re-encrypts their packets and is a trusted observer.
- T4 scale (~5,000 workers), IPv6 overlay and IPv6-only underlays are not verified/supported by this ADR.
