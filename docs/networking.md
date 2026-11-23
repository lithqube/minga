# Networking

This page explains how Minga connects tasks to each other and to the outside world. Each statement is tied to the code that implements it.

## Overlay networks and subnets

Every Compose network gets its own `/24` subnet, allocated from the overlay supernet (default `10.100.0.0/16`, override with `--overlay-supernet`). The Raft leader picks the first non-overlapping `/24` and stores it in state; workers read it from there and never compute one on their own.

```
// crates/minga-net/src/ipam.rs
pub const DEFAULT_SUPERNET: &str = "10.100.0.0/16";   // crates/minga-net/src/cidr.rs
```

A `/24` holds 254 usable task addresses. If the supernet runs out (`--overlay-supernet` not widened and all 256 `/24`s are taken) the leader rejects new networks with `supernet exhausted`.

## Bridges

Each network gets one Linux bridge on each node that runs at least one task on that network. The bridge name is `minga-<8 hex chars>`, derived deterministically from the network ID:

```
// crates/minga-node/src/netns.rs
pub fn bridge_name(network_id: &str) -> String {
    format!("minga-{}", short_hash(network_id))
}
```

The bridge's IP is the first usable address of the subnet (the gateway). The node's embedded DNS server listens on that address.

You can inspect running bridges with:

```bash
ip link show type bridge | grep minga-
```

## Per-task network namespaces

Each task gets a dedicated Linux network namespace named `minga-<task-id>`. A veth pair connects it to its network's bridge:

- host-side interface: `v<6hex>h` — attached to the bridge
- container-side interface: `v<6hex>c` — inside the task's netns with the IPAM-assigned address

The task's default route is via the bridge gateway. `net.ipv4.ip_forward` is enabled on the host by Minga when the first bridge is created (`crates/minga-node/src/netns.rs:1354`).

Inspect live task namespaces:

```bash
ip netns list | grep minga-
ip netns exec minga-<task-id> ip addr
```

## Service names and DNS

The embedded DNS server on the bridge gateway answers queries for `minga.local` names from the local [`DnsStore`](../crates/minga-net/src/dns.rs). For each running task the following names are registered (`crates/minga-node/src/supervisor.rs:237-258`):

| Name | Resolves to |
|---|---|
| `<service>` | All task IPs for that service |
| `tasks.<service>` | All task IPs for that service |
| `<stack>.<service>` | All task IPs (compat form from v1.3.0-rc.1) |
| `<service>.<stack>.minga.local` | All task IPs for that service |
| `<task-id>.<service>.<stack>.minga.local` | The individual task's IP |

The bare service name (`<service>`) works directly because the DNS server responds for it without requiring a search-domain expansion. This matters for musl-based images (Alpine, busybox), which do not apply `search` domains from `resolv.conf`.

## resolv.conf and search domains

Each task's `/etc/resolv.conf` is written at start time (`crates/minga-node/src/netns.rs:1761-1768`):

```
nameserver <bridge-gateway-ip>
options ndots:0
search <stack>.minga.local minga.local   # only when MINGA_STACK is set
```

`ndots:0` means every name is tried as an absolute name first. The search domains are set only when the task has a `MINGA_STACK` environment variable (injected by the scheduler for Compose stacks). With search domains present, a lookup for `web` tries `web.<stack>.minga.local` and `web.minga.local` before falling back to the upstream resolver (`crates/minga-node/src/containerd/runtime.rs:682-690`).

## Published ports

Minga supports two port-publication modes (`crates/minga-state/src/types.rs:192-198`):

| Mode | Compose syntax | Behaviour |
|---|---|---|
| `ingress` (default) | `ports: ["8080:80"]` | Port answers on **every node**; the ingress layer forwards to the task wherever it runs. |
| `host` | `ports: ["8080:80"]` with `mode: host` | Port answers only on the node running the task. Minga refuses the same port for two host-mode services. |

For both modes, Minga installs iptables DNAT rules in `PREROUTING` (for traffic addressed to the node) and `OUTPUT` (for clients on the node itself). The `--dst-type LOCAL` match limits capture to traffic addressed to the node, not to passing packets (`crates/minga-node/src/netns.rs:421`).

A published port that the node itself uses (NATS on 4222, Raft on 7001, the embedded DNS on 53, the API) is refused at setup time with an error naming the conflict.

## Cross-node traffic

In a multi-node cluster, tasks on the same overlay network but on different nodes reach each other over a WireGuard mesh (`minga-wg0`). Each node gets an address in `10.200.0.0/16` derived from its node ID (`crates/minga-net/src/overlay.rs:239`). The WireGuard UDP port is 51820 (`crates/minga-net/src/cidr.rs:209`, `DEFAULT_WG_PORT`).

Traffic flow for a task on node A reaching a task on node B:

1. Task on node A sends a packet to the other task's overlay IP.
2. The host routing table forwards it to `minga-wg0`.
3. WireGuard encrypts it and sends it to node B's UDP endpoint.
4. On node B, the packet arrives in the bridge of the shared network and is forwarded into the target task's netns.

A worker behind NAT relays traffic through a trusted manager. The manager's UDP endpoint must be reachable from all workers; NAT-isolated workers connect to it rather than directly to peers.

Ingress ports answer on every node. The node that receives the connection uses `PREROUTING` DNAT to send the packet to the task's overlay IP, which may be on a different node and is reached over WireGuard.

## Network isolation

Tasks on different networks cannot reach each other except through published ports. Minga enforces this with iptables `FORWARD` rules that drop inter-bridge traffic unless the state is `DNAT` (i.e. a published-port forwarding rule applied):

```
// crates/minga-node/src/netns.rs — isolation_rules()
-i minga-<br> -o minga-+ -m conntrack ! --ctstate DNAT -j DROP
-i minga-<br> -o minga-<br> -j ACCEPT    # same-bridge traffic allowed
```

Tasks cannot reach `127.0.0.0/8` on the host (a loopback guard prevents this via CVE-2020-8558 mitigations). Source IP is validated per-veth: a task sending traffic from an IP it was not assigned is dropped.

Tasks under a required egress policy (`x-minga-egress`) have their DNS responses confined to local names; external DNS queries are refused at the resolver level, so the egress proxy is the only outbound route.
