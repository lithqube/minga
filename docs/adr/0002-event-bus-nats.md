# ADR 0002: Event bus — NATS/JetStream behind `minga-bus`

- Status: Accepted
- Date: 2026-10-26
- Input: [NATS/JetStream evaluation](../research/nats-evaluation.md)

## Context

Minga's architecture states that "only desired state and placement go through Raft; heartbeats, task status and agent run steps go through JetStream" (CLAUDE.md). The Raft half exists (`minga-state`). The JetStream half does not: there is no event bus, no task dispatch path, no status reporting channel, and no liveness mechanism.

The gaps, concretely:

1. **Task dispatch** — the scheduler writes `Task` entities to Raft, but nothing tells a node to start the container.
2. **Task status** — `Task.state` lives in the Raft state machine; a worker cannot report actual state without a consensus round-trip.
3. **Node liveness** — `ClusterNode.status` is membership intent; there is no heartbeat or expiry.
4. **Events, logs, agent steps, egress usage** — no streaming channel exists for any of these.

Comparable orchestrators (Swarm, Kubernetes, Nomad) all split desired state (consensus) from actual state (separate channel: gRPC streams, etcd watch + events API, Serf + event broker). Minga needs the same split.

## Decision

Adopt **NATS/JetStream** as the event bus, behind a new **`minga-bus`** crate that abstracts the transport behind an `EventBus` trait. The Go `nats-server` runs as a managed sidecar process on managers (per ADR 0001). Workers are NATS clients over the WireGuard overlay.

Three implementations of `EventBus`:

| Implementation | Use |
|---|---|
| `MemoryBus` | Unit tests, single-node development |
| `NatsBus` | Production multi-node clusters |
| `SpoolingBus` (Phase 1) | Worker-side offline spool wrapper |

## Subject namespace

```
minga.node.{id}.status     JetStream   node status reports
minga.node.{id}.event      JetStream   node lifecycle events
minga.task.{id}.status     JetStream   task status reports
minga.task.{id}.event      JetStream   task lifecycle events
minga.log.{id}             JetStream   container log chunks
minga.ctl.node.{id}.assign JetStream   task assignment command  [work queue]
minga.ctl.node.{id}.stop   JetStream   stop/remove command       [work queue]
minga.ctl.node.{id}.ping   core NATS   synchronous health probe  [req/reply]
minga.ctl.node.{id}.drain  core NATS   synchronous drain         [req/reply]
```

## Streams

| Stream | Subjects | Retention | Limits |
|---|---|---|---|
| `MINGA_EVENTS` | `minga.node.>`, `minga.task.>`, `minga.egress.>` | Limits | MaxAge 72h |
| `MINGA_COMMANDS` | `minga.ctl.node.>` | WorkQueue | MaxAge 24h, AckWait 30s |
| `MINGA_LOGS` | `minga.log.>` | Limits | MaxBytes 512 MiB, MaxAge 24h |
| `MINGA_DEAD_LETTERS` | `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>`, `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>` | Limits | MaxBytes 64 MiB, MaxAge 7d |

Every explicit-ack consumer sets MaxDeliver 5 and BackOff 30s, 1m, 2m, 5m (the first entry is the ack wait). A payload that can never be handled is terminated (`+TERM <reason>`); a transient failure is nak'd with a delay. JetStream has no dead-letter queue, so `MINGA_DEAD_LETTERS` keeps the advisories it emits instead of a hand-rolled copy. An advisory names the stream sequence, not the body, so handlers must tolerate a missing payload.

## KV buckets

| Bucket | Keys | Purpose |
|---|---|---|
| `MINGA_STATUS` | `task.{id}`, `node.{id}` | Latest actual state (materialized view) |
| `MINGA_LEASES` | `lease.node.{id}` | Liveness (timestamp-based, checked against TTL) |

## Type split: desired vs actual

The bus forces a clean split that the current types blur:

- **`Task.state` becomes desired-only.** Raft stores `Pending`/`Assigned` (placement intent). Actual state (`Running`/`Failed`/`Stopped` + metrics) is reported over the bus and mirrored into `MINGA_STATUS`.
- **`ClusterNode.status` stays intent-only** (`Joining`/`Active`/`Draining`/`Left`). Liveness is `is_alive(node_id)` from `MINGA_LEASES`. Placement eligibility becomes `status == Active && is_alive(node)`.
- **Resource accounting switches from Raft `task.state` to actual state** from `MINGA_STATUS`.

## Two consensus systems

A 3-manager cluster runs two independent Raft systems:

- **openraft** (minga-state): desired state and placement.
- **JetStream internal Raft**: per-stream replication for R3 streams.

They manage disjoint data and fail independently. Disk caps are pinned explicitly.

## Sidecar lifecycle

- Managers spawn `nats-server` as a child process with a generated config (JetStream on, pinned storage limits, localhost monitoring, TLS+NKey).
- `MINGA_NATS_URL` env var or `--nats-url` flag points Minga at an external NATS cluster instead of spawning the sidecar.
- Release artifacts ship `minga` + `nats-server` + `SHA256SUMS`.

## Rules

- All NATS specifics live in `minga-bus`. No other crate depends on `async-nats` directly.
- All consumers program against `Arc<dyn EventBus>`.
- `async-nats` version is pinned in `[workspace.dependencies]`.
- Commands (`minga.ctl.>`) are the only subjects on a work-queue stream.
- Status/events/logs are append-only facts on limit-retention streams.
- Secrets never cross the bus.

## Consequences

- The scheduler and supervisor gain a real dispatch and status path — the Phase 0 gate becomes meaningful for multi-node operation.
- Two binaries in the release artifact (minga + nats-server) instead of one — the bounded cost of ADR 0001.
- `async-nats` is 0.x semver; upgrades are one-crate changes behind the trait.
- `rust-natsd` (Rust-native NATS server) is tracked as a future replacement for the sidecar; re-evaluate each release.

## Alternatives considered

| Alternative | Verdict |
|---|---|
| Custom gRPC event streaming (swarmkit pattern) | Rejected — reinvents durability, replay, KV, leases |
| Extend Raft for heartbeats/status | Rejected — violates stated principle, couples liveness to quorum |
| Redis Streams / RabbitMQ / Kafka | Rejected — wrong scale for edge |
| Defer bus, ship supervisor with direct gRPC | Rejected as sequencing — the trait makes the bus cheap now |

## Implementation status (2026-11-02)

- **Built:** `MemoryBus` and `NatsBus`, the streams and KV buckets above (logs live under `minga.log.>`, not `minga.task.>`), NATS mTLS (`MINGA_NATS_TLS_*`), and the manager-spawned sidecar. The sidecar listens on loopback only, so a multi-node cluster points every manager and worker at an external TLS NATS server or cluster.
- **Bounded redelivery:** the command, event and log consumers carry MaxDeliver and BackOff; undecodable commands and log entries are terminated with a reason; failed acks are logged; `MINGA_DEAD_LETTERS` records the advisories and `minga_bus_dead_letters_total` counts what each node terminated (or nak'd on its last delivery). Server-side max-delivery give-ups appear only in the stream, not the counter. Nothing reads `MINGA_DEAD_LETTERS` yet.
- **External NATS cluster:** `--nats-url` takes a comma-separated seed list, and `MINGA_NATS_TLS_SERVER_NAME` a comma-separated list of the names those URLs use; each URL is checked against it. The client connects only to the listed servers and ignores the ones a server advertises. `--nats-replicas` (`MINGA_NATS_REPLICAS`, 1–5) raises `MINGA_EVENTS`, `MINGA_COMMANDS`, `MINGA_DEAD_LETTERS` and both KV buckets on connect, and never lowers them, so a node left at 1 can't undo it. `MINGA_LOGS` stays at one copy. Tested against a 3-server cluster losing one server.
- **Token auth applies only to plaintext loopback connections.** When TLS is configured, the client ignores `--nats-token`/`MINGA_NATS_TOKEN`, so external TLS connections are certificate-only.
- **Not built:** `SpoolingBus`, so workers have no offline spool; and clustering the managers' sidecars, which have no client TLS or route flags, so HA needs NATS servers run outside Minga.
- **Deviation: liveness.** Placement reads a lease kept in Raft (`ClusterNode.last_seen_unix`, renewed by the atomic `RenewNodeLease` command), not `MINGA_LEASES`. The bus lease is refreshed but unused. That puts heartbeats through consensus, against this ADR.
- **Deviation: task state.** The supervisor writes lifecycle transitions (`running`/`stopped`) to the Raft task record so Docker API consumers see them. High-frequency status still goes over the bus.
- **Deviation: dispatch.** Supervisors also reconcile against their local Raft replica, which needs every node to be a Raft member.
- **Release:** archives bundle `nats-server` (checksum-pinned) with `SHA256SUMS`, as decided above.

[docs/architecture.md](../architecture.md) describes the plan to remove these deviations (workers leave Raft).
