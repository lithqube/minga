# Minga: project overview for board workers

Read this first. It points at the source of each rule; the linked ADR or file wins over this page.

Minga is a Rust single-binary orchestrator for edge, on-prem and air-gapped sites. It replaces Docker Swarm and serves the Docker Engine API ([`AGENTS.md`](../../AGENTS.md), [architecture](../architecture.md)).

## Language split

- Rust owns every hot path: per packet, per request, per sandbox ([ADR 0001](../adr/0001-language-split.md)).
- Go is allowed only for control loops, only as a separate process, never through FFI ([ADR 0001](../adr/0001-language-split.md)).
- Today the only Go component is the `nats-server` sidecar that managers spawn ([ADR 0002](../adr/0002-event-bus-nats.md)).

## Raft versus JetStream

| Data | Where it lives | Source |
| --- | --- | --- |
| Desired state (services, networks, volumes, secret and config refs) | Raft, managers only | [ADR 0006](../adr/0006-workers-leave-raft.md) |
| Placement (task to node, IPs) | Raft | [ADR 0006](../adr/0006-workers-leave-raft.md) |
| Task status and failure detail | `MINGA_STATUS` JetStream KV | [ADR 0006](../adr/0006-workers-leave-raft.md) |
| Node liveness (30 s lease) | `MINGA_LEASES` KV | [ADR 0006](../adr/0006-workers-leave-raft.md) |
| Per-node assignments | `MINGA_ASSIGN` KV, written by managers only | [ADR 0006](../adr/0006-workers-leave-raft.md) |
| Events, logs, agent run steps, usage | JetStream streams | [ADR 0002](../adr/0002-event-bus-nats.md), [ADR 0008](../adr/0008-multi-tenancy.md) |

- Only desired state and placement go through Raft. Heartbeats, task status and agent run steps go through JetStream ([`AGENTS.md`](../../AGENTS.md)).
- Only managers vote in Raft. Workers are plain JetStream clients with no `StateStore` ([ADR 0006](../adr/0006-workers-leave-raft.md)).
- Never mirror status back into Raft: heartbeat-rate writes bring back the load ADR 0006 removed ([ADR 0006](../adr/0006-workers-leave-raft.md)).
- All NATS specifics live in `minga-bus`. No other crate depends on `async-nats` ([ADR 0002](../adr/0002-event-bus-nats.md), [`crates/minga-bus/src/lib.rs`](../../crates/minga-bus/src/lib.rs)).
- Secrets never cross the bus ([ADR 0002](../adr/0002-event-bus-nats.md)).
- ADR 0002 predates Phase 12: where it and ADR 0006 differ on liveness or status, ADR 0006 and [architecture](../architecture.md) describe the code.

## Tenancy

Multi-tenancy is part of every change ([ADR 0008](../adr/0008-multi-tenancy.md)).

- Tenants are untrusted. Each owns its objects and nodes; a worker belongs to exactly one tenant.
- A reference to another tenant's object returns 404, not 403. Listings filter by tenant before paging.
- Every bus subject and KV key carries the tenant right after the prefix, for example `minga.<tenant>.task.<id>.status` ([`crates/minga-bus/src/subjects.rs`](../../crates/minga-bus/src/subjects.rs)).
- Placement never crosses tenants (`check_placeable` in [`placement.rs`](../../crates/minga-scheduler/src/placement.rs)). Managers run no tenant workloads once a cluster has two tenants.
- The client certificate carries caller (`CN`), role (`OU`) and tenant (`O`). `O=*` is the cluster operator, the only cross-tenant identity ([ADR 0008](../adr/0008-multi-tenancy.md), [ADR 0005](../adr/0005-api-identity-and-roles.md)).
- Any PR that adds state, an endpoint, a bus subject, a CLI command or a node-local resource answers the seven-question tenancy checklist in its description. "Not applicable" needs a reason ([ADR 0008](../adr/0008-multi-tenancy.md)).
- QA and Review: a test with two tenants must prove tenant B cannot see, use or exhaust tenant A's object (checklist question 7).

## Workspace crates

| Crate | Job | Source |
| --- | --- | --- |
| `minga-cli` | The single binary: manager, worker, `nats-server` sidecar launch | [`crates/minga-cli`](../../crates/minga-cli) |
| `minga-ctl` | Operator CLI for resources, health and observability | [`crates/minga-ctl`](../../crates/minga-ctl), [plan](../minga-ctl-plan.md) |
| `minga-api` | Docker Engine API subset and identity, roles, audit | [`crates/minga-api`](../../crates/minga-api), [ADR 0005](../adr/0005-api-identity-and-roles.md) |
| `minga-compose` | Compose parser; unknown keys fail loudly | [`keys.rs`](../../crates/minga-compose/src/keys.rs) |
| `minga-state` | openraft plus redb: desired state, placement, codec | [`codec.rs`](../../crates/minga-state/src/codec.rs), [`types.rs`](../../crates/minga-state/src/types.rs) |
| `minga-scheduler` | Turns services into placed tasks | [`placement.rs`](../../crates/minga-scheduler/src/placement.rs) |
| `minga-bus` | `EventBus` trait over NATS and an in-memory bus | [`subjects.rs`](../../crates/minga-bus/src/subjects.rs), [ADR 0002](../adr/0002-event-bus-nats.md) |
| `minga-node` | Node supervisor: containerd, agent executor, netns, WireGuard | [`supervisor.rs`](../../crates/minga-node/src/supervisor.rs) |
| `minga-net` | WireGuard overlay, IPAM, DNS | [`overlay.rs`](../../crates/minga-net/src/overlay.rs), [ADR 0007](../adr/0007-wireguard-mesh.md) |
| `minga-sandbox` | Wasmtime agent sandboxes | [`crates/minga-sandbox`](../../crates/minga-sandbox), [ADR 0003](../adr/0003-agent-workloads.md) |
| `minga-egress` | LLM egress proxy: budgets, cache, redaction | [`crates/minga-egress`](../../crates/minga-egress), [agent usage](../agent-usage.md) |
| `minga-telemetry` | Metrics, `/healthz`, `/readyz`, JSON logs | [ADR 0004](../adr/0004-observability.md) |

## Rules that bite

- Compose files deploy unchanged. A key Minga does not support is an error, never skipped ([`keys.rs`](../../crates/minga-compose/src/keys.rs), [`AGENTS.md`](../../AGENTS.md)).
- Raft entries and state rows are JSON with a one-byte `format_version` per redb file. A new field needs `#[serde(default)]`; renaming, removing or re-typing one needs a version bump and a migration ([architecture](../architecture.md), [`codec.rs`](../../crates/minga-state/src/codec.rs)).
- Frozen fixtures in [`crates/minga-state/tests/fixtures`](../../crates/minga-state/tests/fixtures) fail the build when a persisted type changes shape: bump the version and migrate instead ([architecture](../architecture.md)).
- Nodes of one cluster must run the same version; the wire frame carries no schema revision ([architecture](../architecture.md)).
- WireGuard: one `minga-wg0` per node, routed L3, no VXLAN. The mesh peers only within a tenant ([ADR 0007](../adr/0007-wireguard-mesh.md), [ADR 0008](../adr/0008-multi-tenancy.md)).
- Agent WASI capabilities are not active yet: grants are stored and validated but unusable by a guest ([architecture](../architecture.md), [sandbox plan](../sandbox-host-capabilities-plan.md)).
- Token budgets are node-local and checked at request admission. Cluster-wide budgets are planned ([architecture](../architecture.md), [agent-plane roadmap](../agent-plane-roadmap.md)).
- Release ships `minga` and `nats-server` side by side. CI tests the bus against two `nats-server` versions ([ADR 0008](../adr/0008-multi-tenancy.md), [`ci.yml`](../../.github/workflows/ci.yml)).

## Build and verify

- Before committing: `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test` ([`AGENTS.md`](../../AGENTS.md)).
- Each worktree builds in its own `target/` with `CARGO_INCREMENTAL=0`. Never share a target dir between worktrees: a test can link another branch's crates ([`AGENTS.md`](../../AGENTS.md)).
- The board's verify command is `verify_commands` in the gitignored `.claude/super-board/configs/minga.json`; it runs the same three steps with `cargo test --workspace`.
- Linux-only netns, containerd and WireGuard tests run in CI jobs `netns` and `containerd` ([`ci.yml`](../../.github/workflows/ci.yml)).
- Any `unsafe`, FFI or raw-syscall change needs a `// SAFETY:` comment and `unsafe-checker` ([`AGENTS.md`](../../AGENTS.md)).

## Where to read more

- Current code and limits: [architecture](../architecture.md). Supported layouts and evidence: [topologies](../topologies.md).
- Decision records: [ADR index](../README.md). Status notes there say what is merged and what is still open.
- Commands and flags: [CLI reference](../cli.md).
