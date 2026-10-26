# NATS / JetStream Evaluation for Minga

- **Status:** Research complete — recommendation for decision
- **Date:** 2026-10-26
- **Scope:** Should Minga incorporate NATS/JetStream as its event bus, and if so, how and when
- **Related:** [ADR 0001: Language split](../adr/0001-language-split.md), [CLAUDE.md](../../CLAUDE.md), [README.md](../../README.md)

## Executive summary

Minga's architecture already commits to the right principle — "only desired state and placement go through Raft; heartbeats, task status and agent run steps go through JetStream" (`CLAUDE.md`, `crates/minga-state/src/lib.rs`) — but the JetStream half of that sentence does not exist yet: there is no event bus, no worker-to-manager status channel, no task dispatch path, and no liveness mechanism anywhere in the codebase. NATS/JetStream fills exactly those gaps, and it does so with a mature Rust client (`async-nats` 0.50) and a server small enough for a Raspberry Pi. We recommend adopting it, behind a new `minga-bus` crate that abstracts the transport, with the Go `nats-server` running as a managed sidecar process on managers per ADR 0001, wired in as a prerequisite for the node supervisor (the next roadmap item). The Go sidecar is a deliberate, bounded exception to the one-binary goal; a from-scratch Rust server (`rust-natsd`) exists and is actively developed but is not yet cluster-ready for JetStream and should be re-evaluated, not adopted, today.

---

## 1. Where Minga is today

The workspace (`crates/*`) currently contains:

| Crate | Status | What it does |
| --- | --- | --- |
| `minga-compose` | Done | Compose spec parser/validator (28 tests) |
| `minga-state` | Done | Raft state store: openraft 0.9 + redb 2, single-node, 9 integration tests |
| `minga-scheduler` | Done | Compose → state translation, spread placement, reconcile loop (49 tests) |
| `minga-cli` | Partial | `validate` and `deploy` (translation only); `manager`/`worker` are stubs that log "not implemented yet" (`crates/minga-cli/src/main.rs:54-55`) |
| `minga-api` | Scaffold | Docker Engine API subset — one doc-comment line |
| `minga-node` | Scaffold | Node supervisor — one doc-comment line |
| `minga-net` | Scaffold | WireGuard overlay — one doc-comment line |
| `minga-sandbox` | Scaffold | Agent sandboxes — one doc-comment line |
| `minga-egress` | Scaffold | LLM egress proxy — one doc-comment line |

The state layer is real and worth understanding before designing the bus:

- `StateStore` (`crates/minga-state/src/store.rs`) wraps openraft, persists `raft_log.redb` and `state.redb`, and exposes `upsert_*`/`remove_*` for seven entity kinds plus local reads. All writes go through `propose()` → `raft.client_write()`.
- `StateCommand` (`crates/minga-state/src/command.rs`) is the full mutation surface: `Upsert/Remove × {Node, Service, Task, Network, Volume, Secret, Config}`. There is no command for "record a heartbeat" or "record a status observation" — by design.
- Types (`crates/minga-state/src/types.rs`): `ClusterNode { id, address, labels, status, capacity }`, `Task { id, service_id, node_id, state, spec }`, `TaskState::{Pending, Assigned, Starting, Running, Stopping, Stopped, Failed}`, `NodeStatus::{Joining, Active, Draining, Left}`.
- The scheduler (`crates/minga-scheduler/src/lib.rs`) runs a leader-only reconcile loop: `plan()` (`reconcile.rs`) diffs desired services against tasks and emits `CreateTask`/`RemoveTask`; `place_service()` (`placement.rs`) does spread placement with constraint and capacity checks and deterministic task IDs (`{service}.{slot}`).
- Multi-node Raft networking is a stub: `StubNetworkFactory` in `crates/minga-state/src/network.rs` returns errors for all RPCs, with the comment "Multi-node networking (gRPC between managers) will be implemented in a later phase."

So the control plane has a consensus-backed desired-state store and a placement engine, and everything that would make a task actually run on a worker — dispatch, status reporting, liveness, events, logs — is unimplemented.

## 2. Communication gaps in the current architecture

These are the patterns Minga needs, mapped to where they would live today:

| # | Pattern | Producer → Consumer | Volume / cadence | Durability need | Status in Minga |
| --- | --- | --- | --- | --- | --- |
| G1 | **Task dispatch** (run/stop task X with spec S) | Manager (scheduler) → node supervisor | Per placement change | Must survive node/manager restart and network partition (store-and-forward) | Missing. Scheduler writes `Task` to Raft; nothing tells the node to act |
| G2 | **Task status reporting** ("task X is Running/Failed, exit 137") | Node supervisor → manager | Per state transition + periodic | At-least-once; history for audit/debug | Missing. `Task.state` only changes via Raft proposals; no worker-side writer exists |
| G3 | **Node heartbeat / liveness** ("I'm alive, capacity C, labels L") | Node → manager(s) | Every ~5 s per node | Latest-value-wins; expiry = dead | Missing. `ClusterNode.status` is only set via Raft `upsert_node`; nothing refreshes it |
| G4 | **Event streaming** (lifecycle events for `docker events`, `service ps`, audit) | All components → API clients | Bursty | Retained window (hours–days), replayable | Missing. `minga-api` is a scaffold |
| G5 | **Log streaming** (container stdout/stderr → aggregation/API) | Node → manager/API | High, continuous | Bounded retention (bytes/age), tail + replay by time | Missing entirely |
| G6 | **Agent run steps** (tokens, tool calls, intermediate state for AI workloads) | `minga-sandbox` → manager/UI | High during a run | Per-run retention, ordered replay | Missing. `minga-sandbox` is a scaffold |
| G7 | **Egress usage events** (token/cost consumption for budget enforcement) | `minga-egress` → manager | Per request | At-least-once, aggregatable | Missing. `minga-egress` is a scaffold |
| G8 | **Service discovery / presence** (which tasks serve service S, on which IPs) | Nodes → all nodes + API | On change | Latest-value per service; watchable | Not designed. `minga-net` plans WireGuard + nftables; DNS-based discovery assumed |
| G9 | **Synchronous control** (drain/cordon node, ping, exec) | Manager/CLI → node | On demand | Request/reply with timeout; result must come back | Missing |
| G10 | **Inter-node coordination beyond Raft** (membership gossip, network allocation propagation, leader failover signals) | Node ↔ node / manager ↔ manager | Continuous | Best-effort pub/sub + gossip-like propagation | Missing. Raft networking is `StubNetworkFactory`; multi-node Raft is a roadmap item |

Two structural problems in the current types make the gaps sharper:

1. **Actual state has nowhere to live.** `Task.state` (`types.rs`) is stored in the Raft state machine, but nothing on a worker can write it without a consensus round-trip. Worse, `placement.rs` uses `task.state` for resource accounting (tasks not in `Stopped`/`Failed` consume capacity), so a task that dies silently keeps reserving CPU/RAM/GPU until someone proposes a Raft write. And `reconcile.rs::plan()` never retries a `Failed` task — the task ID still matches the desired set, so no action is emitted. The reconcile loop is currently a one-way ratchet: it creates and removes, but never observes.
2. **`NodeStatus` conflates intent with liveness.** `Joining/Active/Draining/Left` is membership intent (fine for Raft), but `placement.rs` filters eligible nodes on `status == NodeStatus::Active` — there is no liveness signal at all. A node that vanishes keeps receiving placements forever.

### 2.1 How comparable orchestrators split this

The desired-state/actual-state split is the industry-standard shape, and each comparable system picked a different transport for the actual-state half:

- **Docker Swarm (swarmkit)** — the system Minga replaces. Managers run embedded Raft for desired state (services, tasks, networks, nodes). Task dispatch and status reporting use a **bidirectional gRPC streaming session** between each agent and the dispatcher (the "session" pattern: assignments flow down, status reports flow up, over one long-lived stream). Membership, network allocation and service discovery propagate over **serf gossip**, encrypted with AES-GCM. So Swarm's answer to G1/G2 is hand-rolled gRPC streams and to G8/G10 is gossip — both of which Minga would otherwise have to build itself.
- **Kubernetes** — everything (spec *and* status) lives in etcd, split by API subresources; controllers `list`+`watch`. Liveness uses **Lease objects with TTL** (kubelet heartbeat) plus periodic `NodeStatus` POSTs. Events go to a separate **Events API**: etcd-backed `Event` objects with TTL (default 1 h), aggregation and counting, written through a rate-limited recorder. The status/events path is deliberately *not* the consensus hot path.
- **Nomad** — servers run a Raft FSM for all state; Serf handles membership/gossip; node liveness is **heartbeat timers with TTL renewed by RPC**. Since Nomad 1.0, every FSM apply publishes to an in-memory **event broker** exposed at `/event/stream` with topics (`Job`, `Allocation`, `Evaluation`, `Deployment`, `Node`). Clients stream task events up to servers.

The common pattern: **Raft/etcd for desired state and placement; a separate event/heartbeat channel for actual state; latest-value caches or TTL leases for liveness; an event log with retention for observability.** That is precisely the split Minga's CLAUDE.md already states, and precisely what NATS Core + JetStream provide off the shelf.

## 3. NATS Core: what it provides

NATS Core is a lightweight, subject-based messaging layer (server binary ~20–30 MB, runs on a Raspberry Pi, idle RSS in the tens of MB):

- **Pub/sub with subject routing.** Dot-separated hierarchical subjects (`minga.task.web.1.status`), wildcards (`*` one token, `>` many) for subscribers and stream capture. No brokers-with-queues to configure — routing is the server's core competency.
- **Queue groups.** Multiple subscribers on the same subject share messages (competing consumers) with zero coordination — load balancing for free.
- **Request/reply.** Synchronous RPC over pub/sub with a reply subject, timeouts, and "no responders" errors. This covers G9 (drain, ping, exec) without a separate RPC framework.
- **Delivery semantics: at-most-once, no persistence.** A message reaches every *currently connected* interested subscriber, once. If nobody is subscribed, it is gone. This is the right semantic for ephemeral signals (heartbeats, presence) and the wrong one for anything that must survive a disconnect — which is what JetStream adds.
- **Connection model.** Clients connect over TCP (TLS via rustls in Rust), with automatic reconnect and server pool failover. Default max payload is 1 MiB — log lines and status reports fit; blobs do not (that is what the object store is for).
- **Clustering.** Servers form full-mesh route clusters; clients connect to any server and the mesh routes. Gateways connect clusters into super-clusters across regions. Leaf nodes extend a cluster to a remote site over a single outbound connection (see §9).

## 4. JetStream: what it adds

JetStream is the persistence layer built into the same server (enabled with `jetstream: enabled`, no extra process):

- **Streams** capture messages published to a set of subjects and store them per a retention policy:
  - `LimitsPolicy` — keep until MaxMsgs/MaxBytes/MaxAge (event log shape, G4).
  - `InterestPolicy` — keep while consumers are interested (notifications).
  - `WorkQueuePolicy` — each message delivered to exactly one consumer, removed on ack (durable command queue shape, G1).
  - Storage: `FileStorage` (persistent) or `MemoryStorage` (volatile, fast). Replicas R1/R3/R5 — R>1 uses JetStream's own internal Raft per stream.
  - Extras that matter for Minga: `MaxMsgsPerSubject` + `DiscardOld` (bounded per-task log/status history), `DuplicateWindow` + `Nats-Msg-Id` (publish dedup, default 2 min), per-message TTL via `Nats-TTL` (server 2.11+, needs `allow_msg_ttl` on the stream), atomic batch publish (2.12+), consumer priority groups incl. `pinned_client` single-active-consumer failover (2.11+).
- **Consumers** read streams with per-consumer progress: pull (backpressure, batch fetch) or push (low latency), durable or ephemeral, `AckExplicit` with `AckWait`/`MaxDeliver`/backoff (at-least-once, redelivery on timeout), `DeliverPolicy` including `DeliverLastPerSubject` (materialized latest-value views) and `DeliverByStartTime` (log tail from a timestamp), ordered consumers for gap-free replay. Ack semantics (+NXT, Nak, Term, InProgress) are rich enough to build exactly-once *processing* on top of at-least-once delivery.
- **Key-Value store.** A stream-backed bucket: latest value per key, history up to 64 revisions, per-key TTL, and **watch** (change notification). This is a materialized-view primitive — ideal for "latest status per task/node" (G2/G3) and for the Kubernetes-style **lease pattern** (heartbeat = key with TTL, expiry = death).
- **Object store.** A bucket for large blobs, chunked and content-addressed — the air-gapped answer to distributing wasm modules, image layers, or model shards between nodes (Phase 2).
- **Versioning caveats** (verified against current docs): server lines in support are 2.12.x and 2.14.x (2.11 EOL April 2026, 2.15 in RC). Since 2.12 the server runs in **strict mode** and rejects unknown fields on `$JS.API.*` requests — always create resources from a clean config, never round-trip server-returned fields. Capability detection is via `$JS.API.INFO` → `api.level` (2.12.0–2.12.4 = 2, 2.12.5+ = 3, 2.14 = 4), not version strings. JetStream memory/file store limits **default to 75% of RAM and disk** — must be pinned explicitly on edge devices (§9).
- **Monitoring.** HTTP endpoints on `:8222` (`/varz`, `/jsz`, `/connz`, `/subsz`, `/routez`, `/leafz`) — enough to supervise the bus without adding Prometheus.

## 5. Mapping Minga's gaps to NATS/JetStream features

| Gap | NATS feature | Why this one |
| --- | --- | --- |
| G1 Task dispatch | JetStream stream, `WorkQueuePolicy`, durable pull consumer per node | A command published while a node is offline waits in the stream until the node reconnects and pulls it — store-and-forward for free. Core NATS would drop it. AckExplicit + MaxDeliver + backoff gives retry semantics for free |
| G2 Task status | JetStream stream (`LimitsPolicy`) + KV bucket (latest per task) | Stream = durable history for audit/replay (`docker service ps` over time, crash forensics); KV = O(1) latest-value reads for the reconcile loop |
| G3 Node heartbeat | KV key with TTL (lease pattern) + ephemeral core pub/sub ping | KV TTL expiry *is* the liveness signal (Kubernetes Lease precedent); needs `marker_ttl` on the bucket so watchers see expirations (2.11+). Core pub/sub heartbeat as a cheap fast-path ping |
| G4 Event streaming | JetStream stream, `LimitsPolicy`, MaxAge 72 h, consumers `DeliverByStartTime`/`DeliverNew` | One event envelope stream feeds the API event stream, audit, and the scheduler's projector simultaneously (fan-out via independent consumers) |
| G5 Log streaming | JetStream stream, `LimitsPolicy`, `MaxBytes` + `MaxAge` caps, `MaxMsgsPerSubject` | Bounded, self-cleaning; tail via `DeliverByStartTime`; chunk lines to stay under 1 MiB |
| G6 Agent run steps | JetStream stream, `LimitsPolicy`, per-run subjects, ordered consumer | Ordered, replayable per-run transcript of tokens/tool calls — exactly the "agent run steps go through JetStream" principle |
| G7 Egress usage | JetStream stream (`LimitsPolicy`) | Budget enforcement aggregates usage events; at-least-once with idempotent aggregation |
| G8 Service discovery | Core pub/sub presence + KV bucket of endpoints | Presence pings on core NATS; endpoint sets in KV (`DeliverLastPerSubject` = current backends). WireGuard DNS remains the data path; the bus is the control path |
| G9 Synchronous control | Core NATS request/reply | Drain/cordon/ping with timeout and no-responders error; no durability needed for a synchronous op |
| G10 Coordination beyond Raft | Core pub/sub + queue groups; NATS cluster routes | Membership chatter, allocation hints, "new leader" signals as plain pub/sub; the NATS mesh replaces a hand-rolled gossip layer |

## 6. Proposed Minga design

### 6.1 Subject namespace

Entity-centric, hierarchical, dot-separated. `{id}` tokens are the Minga IDs already defined in `minga-state::types` (`NodeId = u64`, `TaskId`/`ServiceId` = String like `plant-7.inference.1`).

```
minga.node.{node_id}.heartbeat      core pub/sub   ephemeral liveness ping (5 s)
minga.node.{node_id}.status         JetStream      full status report (capacity, labels, resources, version)
minga.node.{node_id}.event          JetStream      lifecycle events (join, drain, leave, version)
minga.ctl.node.{node_id}.assign     JetStream      task assignment command (TaskSpec payload)   [work queue]
minga.ctl.node.{node_id}.stop       JetStream      stop/remove command for a task               [work queue]
minga.ctl.node.{node_id}.ping       core req/reply synchronous health probe
minga.ctl.node.{node_id}.drain      core req/reply synchronous drain (acks when tasks stopped)
minga.task.{task_id}.status         JetStream      task status reports (worker → manager)
minga.task.{task_id}.event          JetStream      task lifecycle events (assigned/started/exited/failed)
minga.task.{task_id}.log            JetStream      container log chunks (stdout/stderr, chunked)
minga.agent.{run_id}.step           JetStream      agent run steps: token deltas, tool calls, state   (Phase 2)
minga.agent.{run_id}.output         JetStream      agent output stream                               (Phase 2)
minga.egress.{workload_id}.usage    JetStream      token/cost usage events                           (Phase 2)
minga.svc.{service_id}.endpoints    core pub/sub + KV  service presence / backend set
```

Design rules:

- **Commands** (`minga.ctl.>`) are the only subjects on a work-queue stream — a command is consumed once, by its target node, and survives disconnects.
- **Status/events/logs/steps** are append-only facts on limit-retention streams — many consumers, independent progress, replayable.
- **Ephemeral signals** (heartbeat ping, presence) stay on core NATS — at-most-once is the correct semantic; the KV lease is the durable liveness record.
- Subject tokens never contain user-controlled free text (Compose service names are sanitized by `minga-compose` into `ServiceId`s; validate the same way before using an ID in a subject).

### 6.2 Streams

All created idempotently by the manager at startup via the JetStream API (clean configs only — see strict mode, §4). Storage `FileStorage` under `/var/lib/minga/nats/jetstream`; replicas follow the manager count (R1 single manager, R3 with 3 managers).

| Stream | Subjects | Retention | Storage | Replicas | Limits | Key consumers |
| --- | --- | --- | --- | --- | --- | --- |
| `MINGA_EVENTS` | `minga.node.*.status`, `minga.node.*.event`, `minga.task.*.status`, `minga.task.*.event`, `minga.egress.*.usage` | Limits | File | R1→R3 | MaxAge 72 h, MaxBytes cap (e.g. 1 GiB), DiscardOld, DuplicateWindow 2 min | Scheduler projector (durable pull); API event stream (push); audit |
| `MINGA_COMMANDS` | `minga.ctl.node.*.assign`, `minga.ctl.node.*.stop` | **WorkQueue** | File | R1→R3 | AckWait 30 s, MaxDeliver 5 + backoff, MaxAge 24 h | One durable pull consumer per node, filtered `minga.ctl.node.{id}.>` |
| `MINGA_LOGS` | `minga.task.*.log` | Limits | File | R1→R3 | MaxBytes (e.g. 512 MiB), MaxAge 24 h, MaxMsgsPerSubject 100 k, DiscardOld | Log aggregator / API tail (`DeliverByStartTime`) |
| `MINGA_AGENT` (Ph2) | `minga.agent.>` | Limits | File | R1→R3 | MaxAge 7 d, MaxMsgsPerSubject per run, DiscardOld | Run transcript replay (ordered consumer), UI streaming |

Notes:

- `MINGA_EVENTS` deliberately uses several subject patterns in one stream (one stream per bounded context, consumers filter) rather than one stream per entity.
- `MaxMsgsPerSubject` on logs bounds a single chatty task; the stream-level `MaxBytes` bounds the node's disk.
- Work-queue + `AckWait 30 s` means a node that takes a assignment and dies mid-start gets it redelivered — but only after the manager's projector notices the task never reached `Running`, removes it from desired state, and re-places it. The two loops compose.

### 6.3 KV buckets

| Bucket | Keys | Config | Purpose |
| --- | --- | --- | --- |
| `MINGA_STATUS` | `task.{task_id}` → latest `TaskStatusReport`; `node.{node_id}` → latest `NodeStatusReport` | File, R1→R3, history 1 | Materialized **actual state** — the reconcile loop's fast read; `watch()` for event-driven reconcile |
| `MINGA_LEASES` | `node.{node_id}` → heartbeat, **TTL 15 s**, refreshed every 5 s | File, R1→R3, `marker_ttl` set so expirations are visible to watchers | Liveness (Kubernetes Lease pattern): scheduler treats a node as down when its key expires |

The projector (manager, leader) consumes `MINGA_EVENTS` and writes `MINGA_STATUS`; alternatively nodes write both (event for history, KV for latest) and the projector only handles failover. Writing both from the node is simpler and keeps the KV fresh even if the projector lags; the event stream remains the source of truth for history.

### 6.4 Object store (Phase 2)

`MINGA_ARTIFACTS` bucket, R3, File: content-addressed blobs for air-gapped distribution — wasm modules for `minga-sandbox`, OCI layers not in the local registry, model shards. Chunked transfer means a 4 GiB model moves over the same 1 MiB-payload connection as everything else.

### 6.5 Data flow

Desired state (Raft) and actual state (JetStream) as two loops that meet in the scheduler:

```
  Compose file
      │  minga-cli deploy → minga-scheduler::translate_project
      ▼
  Service / Network / Volume / Secret / Config ──► Raft (minga-state) ──► desired state
                                                          │
                              scheduler (leader): plan() + place_service()
                              reads desired (Raft) + actual (MINGA_STATUS KV)
                                                          │
                              ┌───────────────────────────┴───────────────────────┐
                              ▼                                                   ▼
                    Task entities (desired placement)                    actual-state projection
                    → Raft: state = Pending/Assigned                     MINGA_EVENTS stream (history)
                              │                                            MINGA_STATUS KV (latest)
                              ▼                                            MINGA_LEASES KV (TTL liveness)
                    manager publishes to MINGA_COMMANDS ─────────────►  node supervisor (minga-node)
                    (work queue: assign / stop, store-and-forward)              │
                                                                               │ containerd task lifecycle
                                                                               ▼
                                                              node publishes: task status + events
                                                              (minga.task.{id}.status / .event / .log),
                                                              node status, heartbeat lease refresh
```

Worker-side resilience: when a node is disconnected, `minga-bus` spools status reports to a small local ring (in-memory + optional redb spill) and flushes on reconnect; commands are already safe because `MINGA_COMMANDS` is a durable work queue. When a manager is lost, workers keep running their tasks (desired state is already dispatched) and reconnect to another manager's NATS server (client URL list from Raft node addresses); the new leader's projector catches up from `MINGA_EVENTS` replay (`DeliverByStartSequence` from its last checkpoint).

### 6.6 Required changes to `minga-state` types

The bus forces a clean split that the types currently blur:

1. **`Task.state` becomes desired-only.** Raft stores `Pending`/`Assigned` (placement intent). Actual state moves to a new serde type in the bus crate, e.g. `TaskStatusReport { task_id, node_id, state: TaskState, exit_code, message, observed_at, cpu, memory }`, carried on `minga.task.{id}.status` and mirrored into `MINGA_STATUS`. `TaskState` stays in `minga-state::types` (shared vocabulary), but the Raft `Task` stops being the place actual state is recorded.
2. **`ClusterNode.status` stays intent-only** (`Joining/Active/Draining/Left`); liveness becomes `now - last_lease < 3 × interval` from `MINGA_LEASES`. `placement.rs` eligibility becomes `status == Active && alive(node)`.
3. **Resource accounting in `place_service()` switches from Raft `task.state` to actual state** from `MINGA_STATUS` — otherwise a crashed task reserves GPU capacity forever (the silent-failure mode described in §2).
4. **`reconcile.rs::plan()` gains a third action**: `ReplaceTask` (or remove+create) when desired exists but actual is `Failed`/`Stopped` beyond restart policy. Restart policy itself (backoff, max attempts) is new scheduler behavior the event stream enables.

These are Phase 0 changes — they are what makes the Phase 0 gate (10 services on 3 nodes) actually converge.

## 7. Integration architecture

### 7.1 Embedded Go sidecar (ADR 0001, Plan 1)

ADR 0001 already anticipates this: *"Embedded NATS JetStream runs as a Go sidecar process until a Rust-native replacement exists."* The sidecar is a special case of the language-split rule: there is no FFI and no custom gRPC contract — the versioned contract *is* the NATS wire protocol. Minga controls the server through a generated config file and process lifecycle; all Rust code is a NATS **client** (`async-nats`) over localhost TCP. That satisfies "Go only as a separate process" while keeping the hot path in Rust.

Topology:

```
  ┌──────────────────────── minga manager node ────────────────────────┐
  │  ┌────────────┐   ┌──────────────┐   ┌────────────────────────────┐ │
  │  │ minga (Rust)│   │ nats-server  │   │  (3 managers: NATS cluster │ │
  │  │  ┌────────┐ │   │ (Go sidecar) │   │   full mesh, R3 JetStream  │ │
  │  │  │minga- │◄┼───┼─► :4222      │   │   streams, internal Raft)  │ │
  │  │  │bus    │ │   │  :8222 (localhost only) └────────────────────┘ │
  │  │  └───────┘ │   │  store: /var/lib/minga/nats                      │
  │  │  scheduler │   └──────────────────────────────────────────────────┘
  │  │  state ────┼──► Raft (openraft + redb): desired state only        │
  │  └────────────┘                                                      │
  └──────────────────────────────────────────────────────────────────────┘
        ▲ workers connect over the WireGuard overlay (minga-net)
        │  to manager :4222 URLs (from ClusterNode.address in Raft)
  ┌─────┴──────────┐   ┌──────────────────┐   ┌──────────────────┐
  │ minga worker 1 │   │ minga worker 2  │   │ minga worker N  │
  │ minga-bus ─────┘   │ minga-bus        │   │ minga-bus        │
  │ minga-node (supervisor)                │   │ minga-node        │
  └────────────────────────────────────────┘   └──────────────────┘
```

- **Managers** run the sidecar. **Workers do not** in Phase 0/1 — they are clients over the overlay. (Per-site leaf-node servers are a Phase 2 option for partition-prone edge sites, §9.)
- The manager generates the NATS config at startup: JetStream enabled, `store_dir` under the existing `/var/lib/minga` data dir, cluster routes when peers are known, monitoring bound to localhost, **storage limits pinned** (not the 75%-of-RAM/disk defaults), client listen on the overlay interface with TLS + NKey auth (§7.5).
- The manager spawns `nats-server` as a child process, supervises it (restart on exit, forward signals, shut down with the manager), and waits for readiness (`/varz` on localhost or first successful client connect) before starting the scheduler.
- An escape hatch: `MINGA_NATS_URL` (or `--nats-url`) points Minga at an **external** NATS cluster instead of spawning the sidecar. This keeps the sidecar an implementation detail, makes CI trivial, and gives operators who already run NATS an adoption path.

### 7.2 The `minga-bus` crate

All NATS specifics live behind one new crate, `crates/minga-bus`:

```rust
pub trait EventBus: Send + Sync {
    // facts (append-only, durable)
    async fn publish_event(&self, subject: &Subject, payload: Bytes) -> Result<()>;
    async fn subscribe_events(&self, subjects: &[&str]) -> Result<Box<dyn Stream<Item = BusEvent>>>;
    // commands (work queue, store-and-forward)
    async fn send_command(&self, node: NodeId, cmd: Command) -> Result<()>;
    async fn next_command(&self) -> Result<Option<Command>>;
    // latest-value views and leases
    async fn put_status(&self, key: &str, value: &impl Serialize) -> Result<()>;
    async fn get_status(&self, key: &str) -> Result<Option<Bytes>>;
    async fn watch_status(&self) -> Result<Box<dyn Stream<Item = KvEvent>>>;
    async fn refresh_lease(&self, node: NodeId) -> Result<()>;
    // synchronous control
    async fn request(&self, subject: &str, payload: Bytes, timeout: Duration) -> Result<Bytes>;
}
```

Three implementations: `NatsBus` (async-nats), `MemoryBus` (in-process, for unit tests — the scheduler's existing 49 tests keep working without a server), and a `SpoolingBus` wrapper that adds the local offline spool described in §6.5. Because every consumer (`minga-node`, `minga-scheduler`, later `minga-api`, `minga-sandbox`, `minga-egress`) programs against the trait, the transport decision is reversible and the sidecar question never leaks into business logic. `async-nats` is added once, in `[workspace.dependencies]` (`async-nats = "0.50"`, default features: jetstream, kv, object-store, server_2_10…2_14 compat).

### 7.3 Sidecar lifecycle and air-gapped vendoring

- The `nats-server` binary is **not** embedded in the Rust binary (that would mean cgo/FFI, forbidden by ADR 0001, and would balloon the binary by ~25 MB). It ships as a second file in the release artifact: one tarball, two binaries, plus a `SHA256SUMS`. Air-gapped sites vendor the tarball; nothing at runtime reaches the network.
- Version pinning is a security and correctness decision: pin the server minor line (2.12.x LTS) and the `async-nats` version together; `async-nats` 0.50 explicitly supports server lines 2.10–2.14. The sidecar path is resolved as: `MINGA_NATS_SERVER_BIN` env → `nats-server` next to the `minga` executable → `PATH`.
- Config is generated, never shipped: store dir, cluster peers, pinned JetStream limits, ports. The generated config is logged at debug level for supportability.

### 7.4 Two consensus systems, one cluster

A 3-manager Minga cluster will run two independent Raft systems on the same machines:

- **minga-state (openraft):** desired state and placement. Leader-elected, redb-backed, the system of record for *intent*.
- **JetStream internal Raft:** per-stream replication for R3 streams (`MINGA_EVENTS`, `MINGA_COMMANDS`, …). No leader election of its own to manage; stream leaders are per-stream.

They manage disjoint data and fail independently, which is the point: a manager partition takes down neither the bus (R3 streams survive one server) nor desired-state consensus (openraft quorum), and the scheduler tolerates stale actual state by design (level-triggered reconcile). The operational cost is disk: JetStream file storage plus redb plus containerd on the same volume — hence the pinned `max_file_store` and per-stream `MaxBytes` caps. This is worth an ADR of its own (proposed ADR 0002, §13).

### 7.5 Security model

- Transport: clients connect over the WireGuard overlay (already encrypted, `minga-net`), plus TLS on the NATS listener for defense in depth.
- Auth: NKey or user/password per role — manager (full), worker (publish status/logs, consume own commands, refresh own lease), api (read events). No anonymous access.
- Accounts: one account per Minga cluster; `$SYS` monitoring account restricted to localhost.
- Secrets never cross the bus: `minga-state::types::Secret` is metadata-only by design; secret *values* stay out of JetStream (they would be replicated R3 and retained 72 h). Egress redaction (`minga-egress`) applies before any payload is published.

## 8. Rust ecosystem survey

| Component | State (2026-10) | Assessment for Minga |
| --- | --- | --- |
| **`async-nats` 0.50.0** (nats-io/nats.rs) | Official client, ~50 M downloads, actively maintained. Default features include JetStream, KV, object-store, services, NKey, and server-compat 2.10/2.11/2.12/2.14. Tokio-based, rustls. 0.x semver — breaking changes between releases. Higher-level utilities live in the separate "Orbit" crates | **Adopt.** Mature, maintained by the NATS org, exactly the API surface Minga needs (publish, pull/push consumers, KV, object store). Pin the version; hide it behind `minga-bus` so upgrades are one-crate changes |
| **`nats-server` crate** (in the nats.rs repo) | Test-support crate that runs a real `nats-server` binary for integration tests — not a production embedding library | Use for `minga-bus` integration tests, or run the binary directly in test fixtures |
| **`nats_test_server`** | Intentionally buggy fake NATS server for fault injection in client tests | Useful for testing `minga-bus` reconnect/spool behavior |
| **`rust-natsd`** (heikofkoehler, Apache-2.0) | From-scratch Rust NATS server: Core NATS pub/sub, clustering/routes, leaf nodes, **single-node JetStream** (file-persisted), TLS, NKey, multi-account, monitoring. Actively developed (commits through 2026-10-25); JetStream clustering/Raft was scoped and stopped cleanly; e2e interop testing against Go nats-server 2.15 in progress | **Watch, don't adopt.** It is the credible "Rust-native replacement" ADR 0001 imagines, but JetStream clustering (R3 streams) is not there, and protocol parity with the Go server is unproven at scale. Re-evaluate each release; it could replace the sidecar for single-manager deployments first |
| **Go embedding** (`nats-server` as a Go library) | Possible in Go, not from Rust without FFI (forbidden by ADR 0001) | Only viable via the sidecar-process route in §7.1 |

## 9. Edge and air-gapped considerations

- **Footprint.** `nats-server` is a single static Go binary (~20–30 MB) with idle RSS in the tens of MB; NATS runs on Raspberry Pi-class hardware. `async-nats` is pure Rust over tokio and adds a few MB to the `minga` binary. Total distribution: two files instead of one — the bounded cost of ADR 0001 Plan 1.
- **Storage defaults are wrong for edge.** JetStream `max_memory_store` and `max_file_store` default to **75% of RAM and disk**. On a 4 GB ARM board that is a 3 GB cap Minga must override. The generated sidecar config pins both (e.g. `max_memory_store: 64MB`, `max_file_store: 1GB` on small disks) and every stream gets explicit `MaxBytes`/`MaxAge`.
- **Offline operation.** Workers are clients: when the manager is unreachable, tasks keep running, status reports spool locally (§6.5), and commands already queued in `MINGA_COMMANDS` wait. When a node is unreachable, its commands wait in the work queue (store-and-forward) — the property edge sites actually need.
- **Partitioned sites (Phase 2).** NATS **leaf nodes** are the designed answer: one outbound connection from a site hub to the manager cluster, local JetStream on the leaf, store-and-forward in both directions across the WAN link, and fully local messaging while disconnected. Air-gapped multi-site deployments become: managers clustered on-site, leaf per remote site, no inbound ports anywhere.
- **Clustering.** R3 streams need 3 NATS servers (the 3 managers). R1 streams on a single manager are fine for Phase 0 but lose the bus if that manager dies — acceptable at Phase 0, fixed by Phase 1 clustering.
- **Air-gapped builds.** Everything is vendored: the `minga` binary, the `nats-server` binary, and the crates. NATS has no cloud dependency, no phone-home, and its monitoring endpoint binds to localhost by default in our generated config.
- **Message size.** 1 MiB default max payload: status reports and agent steps fit; log lines are chunked; large artifacts go through the object store, never the 1 MiB path.

## 10. Recommendation

**Adopt NATS/JetStream, behind a `minga-bus` abstraction, with the Go server as a managed sidecar on managers — starting now, as a prerequisite for the node supervisor.** The bus is not an optional extra for Minga; it is the missing half of the architecture the project already committed to in writing. The phased plan:

### Phase 0 — bus + sidecar + supervisor (unblocks the Phase 0 gate)

1. New crate `minga-bus`: `EventBus` trait, `NatsBus` (async-nats 0.50), `MemoryBus` for tests. Workspace dep pinned.
2. Manager spawns and supervises the `nats-server` sidecar with a generated config (JetStream on, pinned storage limits, localhost monitoring, TLS+NKey); `MINGA_NATS_URL` escape hatch to an external server. Streams `MINGA_EVENTS`, `MINGA_COMMANDS`, `MINGA_LOGS` and buckets `MINGA_STATUS`, `MINGA_LEASES` created idempotently at startup.
3. `minga-node` supervisor: durable pull consumer on `minga.ctl.node.{id}.>`; runs tasks via containerd; publishes status/events/logs; refreshes its lease.
4. Scheduler: actual-state projection from `MINGA_STATUS`; liveness from `MINGA_LEASES`; placement filters on `Active && alive`; resource accounting uses actual state; `plan()` gains failed-task replacement with a restart policy.
5. `minga-state` types split desired/actual as in §6.6.
6. Release artifacts ship `minga` + `nats-server` + checksums.

Deliverable: the Phase 0 gate — a 10-service Compose stack running on 3 nodes, with live status, real liveness, and task restart on failure.

### Phase 1 — multi-manager and observability

7. NATS cluster across the 3 managers (mesh routes); streams go R3; JetStream internal Raft co-located with openraft (§7.4).
8. Multi-node Raft networking (`StubNetworkFactory` → real gRPC) — independent of the bus; the bus does not replace consensus RPC.
9. API event stream (`minga-api`): `docker events`-compatible feed from `MINGA_EVENTS`; `service ps` from `MINGA_STATUS`; log tail via `DeliverByStartTime`.
10. Drain/cordon via request/reply + durable commands; node removal reconciles leases and re-places tasks.

### Phase 2 — agent-first workloads and edge topologies

11. `MINGA_AGENT` stream: run steps from `minga-sandbox` (tokens, tool calls), ordered replay per run; `minga-egress` usage events into `MINGA_EVENTS` for budget enforcement.
12. `MINGA_ARTIFACTS` object store: air-gapped distribution of wasm modules / image layers / model shards.
13. Leaf-node NATS per remote edge site for partition tolerance; service discovery presence over the bus (`minga.svc.*.endpoints` + KV).
14. Re-evaluate `rust-natsd` as a sidecar replacement (single-manager first, clustered later); consider NATS Services API / agent-fabric patterns for agent-to-agent discovery only if multi-agent workloads demand it.

### What we are explicitly not doing

- Not putting heartbeats/status in Raft (violates the stated principle; couples liveness to consensus availability).
- Not embedding Go via FFI (violates ADR 0001).
- Not adopting `rust-natsd` today (immature JetStream clustering).
- Not building a custom eventing layer over gRPC (see §12 — it is the fallback, not the plan).

## 11. Risks and mitigations

| Risk | Severity | Mitigation |
| --- | --- | --- |
| Two consensus systems (openraft + JetStream Raft) confuse operations and sizing | Medium | Document the boundary (ADR 0002); disjoint data; monitor both via `/jsz` and openraft metrics; pin disk caps |
| Go sidecar breaks the "one binary" promise | Medium | Ship both binaries in one tarball with checksums; `MINGA_NATS_URL` escape hatch; `minga-bus` trait keeps the transport swappable; rust-natsd is the long-term exit |
| `async-nats` is 0.x — breaking changes | Medium | Pin version in `[workspace.dependencies]`; all NATS code behind `minga-bus`; upgrade is a one-crate, test-covered change |
| JetStream storage defaults (75% RAM/disk) exhaust edge devices | High | Generated config pins `max_memory_store`/`max_file_store`; per-stream `MaxBytes`/`MaxAge`; `minga validate`-style preflight could warn on small disks |
| Eventual consistency of actual state → scheduler acts on stale data | Medium | Level-triggered reconcile (re-derives from latest KV each pass); idempotent consumers; 2-min publish dedup window; lease TTL = 3× heartbeat interval |
| Offline nodes lose commands / managers lose reports | High | Work-queue stream = store-and-forward commands; `minga-bus` local spool + flush on reconnect; workers keep running tasks during manager loss |
| 1 MiB max payload | Low | Chunk logs; object store for blobs; reject oversized publishes loudly at the bus boundary |
| Client/server version skew (strict mode in 2.12+) | Low | Pin server line and async-nats together; create resources from clean configs, never round-trip server responses; detect capability via `api.level` |
| Security: bus becomes an attack surface inside the cluster | Medium | TLS + NKey per role; overlay transport; no secret values on the bus; monitoring on localhost only |
| Sidecar process management (crash loops, zombie config) | Low | Supervisor with restart backoff; readiness gate before scheduler starts; config generated and logged; `minga doctor`-style check can report bus health later |
| rust-natsd never reaches parity → permanent second binary | Low–Medium | Acceptable: the sidecar is small, static, and version-pinned; the bus trait means replacement is invisible to consumers |

## 12. Alternatives considered

| Alternative | Pros | Cons | Verdict |
| --- | --- | --- | --- |
| **Custom gRPC event streaming** (extend `StubNetworkFactory` into a real manager↔node gRPC service: dispatch stream down, status stream up — the swarmkit pattern) | No new runtime; pure Rust; full control; proven pattern (Swarm does exactly this) | Minga would build: fan-out, durable queues, reconnect/store-and-forward, replay, latest-value views, TTL leases, log tailing — months of work, all reinvented; no KV/object store; every future consumer (API, sandbox, egress) re-implements subscriptions | **Rejected as the primary path.** It is the fallback if NATS is ever dropped; the `EventBus` trait is shaped so this implementation could be written behind it |
| **Extend Raft for everything** (heartbeats/status as `StateCommand`s) | One system | Violates Minga's stated principle; heartbeats in the consensus log couple liveness to quorum availability (a partitioned manager can't record deaths, so placement freezes); log bloat at 5 s/node; redb write amplification | **Rejected** |
| **Redis Streams** | Familiar; durable pub/sub; consumer groups | Another non-Rust server process; single-threaded; clustering (Cluster/Sentinel) is its own project; heavier footprint; not edge-first | **Rejected** |
| **RabbitMQ** | Mature | Erlang VM, ~100 MB+, AMQP complexity; wrong scale for edge | **Rejected** |
| **Kafka / Redpanda** | High throughput | Datacenter-scale brokers; JVM or heavy native; absurd for a 3-node edge cluster | **Rejected** |
| **ZeroMQ** | Fast, embeddable | No broker, no durability, no admin — Minga would build the broker anyway | **Rejected** |
| **Defer: ship supervisor with direct gRPC, add a bus later** | Zero new dependencies for the Phase 0 gate | The supervisor's first consumer *is* the bus; deferring means designing the trait later under pressure, and G4–G7 (events, logs, agent steps) all land soon after. The trait makes the bus cheap to introduce now and cheap to swap later | **Rejected as sequencing** — but note the trait design is exactly what makes this deferral *safe* if priorities change |

## 13. Open questions and next steps

1. **ADR 0002** should be drafted: "Event bus: NATS/JetStream behind `minga-bus`, Go sidecar on managers" — recording the subject namespace, stream configs, the desired/actual type split, and the two-consensus-systems boundary. This document is its input.
2. **ADR 0001 amendment**: clarify that the NATS sidecar's "versioned contract" is the NATS wire protocol + pinned server version, not a bespoke gRPC API — the current wording implies gRPC for all Go components.
3. Spike (one week, timeboxed): `minga-bus` + sidecar lifecycle + `MINGA_COMMANDS` round-trip (manager → fake node → status report → `MINGA_STATUS` KV) on a single node. Exit criteria: a `Task` proposed to Raft results in a command consumed by a test node, and a status report lands in the KV bucket. This de-risks async-nats ergonomics and sidecar supervision before the supervisor is built on top.
4. Decide the restart-policy semantics for failed tasks (backoff schedule, max attempts, per-service override via Compose `x-minga:` extension) as part of the scheduler change in Phase 0 item 4.
5. Confirm the `minga.task.*.log` chunking format (newline-delimited JSON envelope with stream/source/timestamp, ≤ 256 KiB per message) so `minga-api` log tailing can rely on it.
6. Track `rust-natsd` releases; define the parity checklist (JetStream R3, KV, object store, leaf nodes, monitoring) that would trigger re-evaluation.

## Sources

- Minga codebase: `CLAUDE.md`, `README.md`, `docs/adr/0001-language-split.md`, `crates/minga-state/src/{lib,store,command,types,network,state_machine}.rs`, `crates/minga-scheduler/src/{lib,placement,reconcile}.rs`, `crates/minga-cli/src/main.rs`, crate `Cargo.toml`s.
- NATS/JetStream: nats.io docs (Core NATS deep dive, JetStream model, KV, object store, leaf nodes, sizing & resources, upgrade-to-2.12 notes); Synadia blog/guides (JetStream memory patterns, leaf nodes, DDIL); jetstream-architecture skill reference (`concepts/streams.md`, `concepts/consumers.md`, `concepts/server-features.md` — api.level table, strict mode, per-message TTL, priority groups).
- Rust ecosystem: crates.io API (`async-nats` 0.50.0, 2026-07-20, feature list), docs.rs (`async-nats` jetstream/kv modules), github.com/nats-io/nats.rs (releases, `nats-server` test crate), github.com/heikofkoehler/rust-natsd (feature matrix, phase-34 status, last commit 2026-10-25), lib.rs (`nats_test_server`).
- Comparable systems: Docker Swarm admin guide & swarmkit architecture (Raft + dispatcher gRPC sessions + serf gossip), Kubernetes desired/actual state & Events architecture (status subresource, leases, event TTL/aggregation), Nomad event stream & heartbeat timers (Raft FSM + Serf + `/event/stream` broker).
