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
minga.task.{id}.log        JetStream   container log chunks
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
| `MINGA_LOGS` | `minga.task.*.log` | Limits | MaxBytes 512 MiB, MaxAge 24h |

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
