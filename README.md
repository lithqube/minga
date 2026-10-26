# Minga

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)
[![Status](https://img.shields.io/badge/status-phase%200%20(gate%20passed)-green.svg)]()

A single-binary container and agent orchestrator for **edge, on-prem and air-gapped sites**, written in Rust. It runs Compose files natively, speaks enough of the Docker Swarm API to replace it, and treats AI-agent workloads — sandboxes, durable runs, budgets, GPUs — as first-class objects.

Named after the [minga](https://en.wikipedia.org/wiki/Minga) of Chiloé, where a community moves a whole house together.

> **Status: Phase 0 gate + event bus + NATS sidecar + containerd runtime.** A 10-service Compose stack runs on a 3-node Raft cluster end-to-end with NATS/JetStream event bus and real containerd containers. 170 tests across 6 crates. Networking, API, and agent sandboxes are next. See the [roadmap](#roadmap).

## Why Minga

Docker Swarm is a fine orchestrator for a datacenter, but it was not built for the places Minga targets:

- **Edge sites** with a handful of ARM boards, intermittent connectivity, and no tolerance for a heavyweight control plane.
- **Air-gapped environments** where pulling images from a registry is not an option and every dependency must be vendored.
- **Agent workloads** where you need sandboxed execution (wasmtime, Firecracker), token budgets, and LLM egress control — not just "run a container."

Minga ships as **one small binary**. No Docker daemon, no Kubernetes, no etcd. It reads a `compose.yaml` and makes it run across a cluster of nodes, with Raft for consensus and an overlay network for service discovery.

## Features

- **Compose-native** — `docker compose` files deploy unchanged. Full spec parsing with variable interpolation, profiles, merge keys, and `x-` extensions. Unknown keys fail loudly with suggestions instead of being silently skipped.
- **Swarm-compatible API** — speaks enough of the Docker Engine API to work with existing tooling (`docker stack deploy`, `docker service ls`, …).
- **Raft consensus** — desired state and placement go through [openraft](https://docs.rs/openraft), persisted in [redb](https://docs.rs/redb). No external etcd.
- **Overlay networking** — WireGuard mesh between nodes, nftables/eBPF for service load-balancing and ingress.
- **Agent-first workloads** — wasmtime and Firecracker sandbox pools, LLM egress proxy with per-workload token and cost budgets.
- **Air-gapped ready** — no cloud dependencies, offline image distribution, vendored builds.
- **One binary** — manager, worker, and CLI in a single statically-linked executable.

## Quick start

```bash
# Build from source
cargo build --release

# Validate a Compose file — checks syntax, references, port conflicts,
# dependency cycles, and prints the startup order
cargo run -p minga-cli -- validate -c compose.yaml --profile debug
```

Example output:

```
project plant-7
services in startup order:
  inference        registry.local/vllm:0.6.3                1 replica(s)
  agent-api        registry.local/agent-api:1.4.0           2 replica(s)  ports 443->8080/tcp  after inference
  sensors          registry.local/sensor-reader:2.1         global
  debug-ui         registry.local/debug-ui:latest           1 replica(s)  ports 9000->9000/tcp
```

## Architecture

```
                    ┌──────────────────────────────────────┐
                    │              minga binary              │
                    │  ┌─────────┐  ┌─────────┐  ┌───────┐ │
                    │  │   CLI   │  │ Manager │  │Worker │ │
                    │  └────┬────┘  └────┬────┘  └───┬───┘ │
                    └───────┼────────────┼───────────┼─────┘
                            │            │           │
  ┌─────────────────────────┼────────────┼───────────┼─────────────────────────┐
  │                         ▼            ▼           ▼                         │
  │  ┌──────────┐   ┌──────────────┐  ┌──────────┐  ┌──────────────────────┐   │
  │  │minga-    │   │ minga-state   │  │minga-    │  │ minga-api             │   │
  │  │compose   │   │ (openraft +   │  │scheduler │  │ (Docker Engine API    │   │
  │  │          │   │  redb, Raft)  │  │          │  │  subset + gRPC)       │   │
  │  └──────────┘   └──────────────┘  └────┬─────┘  └──────────────────────┘   │
  │                                          │                                 │
  │  ┌──────────┐   ┌──────────┐   ┌────────▼─────┐  ┌──────────────────────┐ │
  │  │minga-node│   │minga-net  │   │minga-sandbox │  │ minga-egress          │ │
  │  │(containerd│   │(WireGuard,│   │(wasmtime,    │  │ (LLM proxy, budgets,  │ │
  │  │ supervisor)│  │ nftables) │   │ Firecracker) │  │  caching, redaction)  │ │
  │  └──────────┘   └──────────┘   └──────────────┘  └──────────────────────┘ │
  └───────────────────────────────────────────────────────────────────────────┘
```

### Design principles

- **Rust on the hot path.** Per-packet, per-request, per-sandbox work stays in Rust. Any Go component (e.g. embedded NATS) runs as a separate process behind a versioned gRPC contract. No FFI between languages.
- **Only desired state goes through Raft.** Heartbeats, task status, and agent run steps go through JetStream — not the consensus log.
- **Compose files deploy unchanged.** Unknown keys fail loudly instead of being skipped.
- **Simplicity over complexity.** One binary, one footprint, no external dependencies for consensus or storage.

Decisions are recorded as [ADRs](docs/adr/) in the repo.

## Crate layout

| Crate | Status | Purpose |
| --- | --- | --- |
| `minga-cli` | **Done** | The `minga` binary: `validate`, `deploy` (translation), `manager`, and `worker` subcommands wired to the scheduler and supervisor. |
| `minga-compose` | **Done** | Compose Specification parser and validator. Full interpolation, profiles, merge keys, `x-` extensions, dependency cycle detection, port conflict detection. 28 tests. |
| `minga-state` | **Done** | Cluster state: openraft + redb. Multi-node Raft with TCP networking, leader election, failover, persistent log and state machine. 15 tests. |
| `minga-scheduler` | **Done** | Placement engine: Compose → state entities, spread placement with constraints and resource awareness, reconcile loop. 49 tests. |
| `minga-node` | **Done** | Node supervisor: `Runtime` trait with `MockRuntime` (testing) and `ContainerdRuntime` (real containers via containerd gRPC). OCI spec generation, graceful shutdown (SIGTERM→SIGKILL), image pull. Reconcile loop starts/stops containers from Raft + bus. 43 tests. |
| `minga-bus` | **Done** | Event bus abstraction: `EventBus` trait with `MemoryBus` (in-process) and `NatsBus` (NATS/JetStream). Commands, events, KV status, liveness leases, request/reply. 24 tests. |
| `minga-api` | Scaffold | Docker Engine API subset (REST) and native gRPC API. |
| `minga-net` | Scaffold | WireGuard overlay, nftables/eBPF, ingress. |
| `minga-sandbox` | Scaffold | Agent sandboxes: wasmtime and Firecracker warm pools. |
| `minga-egress` | Scaffold | LLM egress proxy: token and cost budgets, caching, secret redaction. |

## Development

```bash
# Clone
 git clone https://github.com/lithqube/minga.git
 cd minga

# Build
 cargo build

# Run all tests
 cargo test

# Lint (must be clean before committing)
 cargo clippy --all-targets -- -D warnings

# Format
 cargo fmt --all

# Run the CLI
 cargo run -p minga-cli -- --help
```

Requirements: Rust 1.80+ (stable), with `rustfmt` and `clippy` components.

## Roadmap

- [x] **Compose parser & validator** — full spec parsing, interpolation, profiles, validation
- [x] **Raft state store** — openraft + redb, multi-node consensus over TCP, leader election, failover, persistent state
- [x] **Scheduler** — Compose → state translation, spread placement with constraints, resource-aware reconcile loop
- [x] **Node supervisor** — `Runtime` trait + `MockRuntime`, reconcile loop that starts/stops containers from Raft-assigned tasks
- [x] **Phase 0 gate** — a 10-service Compose stack runs on 3 nodes (E2E integration test)
- [x] **Event bus (NATS/JetStream)** — `minga-bus` crate: `EventBus` trait, `MemoryBus`, `NatsBus` — [ADR 0002](docs/adr/0002-event-bus-nats.md), [evaluation](docs/research/nats-evaluation.md)
- [x] **Bus integration** — scheduler publishes assign/stop commands, supervisor pulls commands + reports status + refreshes leases, CLI wired with `--nats-url`
- [x] **NATS sidecar lifecycle** — manager spawns `nats-server` as a child process with generated config (JetStream enabled, pinned storage limits, readiness probe, `kill_on_drop` cleanup). Workers require `--nats-url` to connect to the manager's NATS server.
- [x] **Containerd runtime** — `ContainerdRuntime` implementing the `Runtime` trait via containerd's gRPC API (Unix socket). OCI spec generation from `TaskSpec`, image pull, container/task lifecycle, graceful shutdown (SIGTERM → 10s grace → SIGKILL). CLI: `--runtime containerd --containerd-socket /run/containerd/containerd.sock`. Integration tests skip gracefully without containerd.
- [ ] **Overlay networking** — WireGuard mesh, ingress, service discovery
- [ ] **Docker Engine API** — REST subset for `docker stack` / `docker service` compatibility
- [ ] **Agent sandboxes** — wasmtime + Firecracker warm pools
- [ ] **LLM egress proxy** — budgets, caching, secret redaction

## Documentation

- [Design document](docs/design.md) — scope, feature gap vs Swarm, tech stack, roadmap
- [ADR 0001: Language split](docs/adr/0001-language-split.md) — why Rust, when Go is acceptable
- [ADR 0002: Event bus](docs/adr/0002-event-bus-nats.md) — NATS/JetStream behind `minga-bus`
- [NATS/JetStream evaluation](docs/research/nats-evaluation.md) — should Minga adopt NATS as its event bus, and how
- [CLAUDE.md](CLAUDE.md) — project conventions for AI-assisted development

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a PR.

## License

[MIT](LICENSE) — Copyright (c) 2026 Minga contributors.
