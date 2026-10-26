# Minga

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)
[![Status](https://img.shields.io/badge/status-wave%206%20(agent%20workloads)-green.svg)]()

A single-binary container and agent orchestrator for **edge, on-prem and air-gapped sites**, written in Rust. It runs Compose files natively, speaks enough of the Docker Swarm API to replace it, and treats AI-agent workloads — sandboxes, durable runs, budgets, GPUs — as first-class objects.

Named after the [minga](https://en.wikipedia.org/wiki/Minga) of Chiloé, where a community moves a whole house together.

> **Status: Wave 6 complete — agent workloads.** A 10-service Compose stack runs on a 3-node Raft cluster end-to-end with NATS/JetStream event bus, real containerd containers, WireGuard overlay networking (IPAM, DNS, config generation), and a Swarm-compatible Docker Engine API (v1.41) so `docker stack deploy`, `docker service ls`, `docker node ls` work against Minga. Agent sandboxes (wasmtime + warm pools, Firecracker stub) and an LLM egress proxy (budgets, caching, secret redaction) are in. 407 tests across 10 crates. See the [roadmap](#roadmap).

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
| `minga-api` | **Done** | Docker Engine API v1.41 REST subset: services, tasks, nodes, networks, secrets, configs CRUD with label filters. Bidirectional translation between Docker API types and Minga state. `minga api` CLI subcommand. Real `docker` CLI verified via testcontainers. 76 tests. |
| `minga-net` | **Done** | Overlay networking: CIDR utilities, deterministic IPAM (hash-based allocation, linear probing), WireGuard keypair generation (X25519) + `wg-quick` config generation, DNS service discovery (`minga.local`), `OverlayManager` coordinator. 70 tests. |
| `minga-sandbox` | **Done** | Agent sandboxes: `SandboxRuntime` trait with `MockSandboxRuntime` (testing), `WasmtimeRuntime` (real WASM execution with fuel metering, memory limits, timeouts), `WarmPool` (pre-warmed sandbox pool with acquire/release, background replenish/evict), `FirecrackerRuntime` stub (Linux + KVM). CLI: `minga sandbox run`, `minga sandbox info`. 36 tests. |
| `minga-egress` | **Done** | LLM egress proxy (axum): per-workload token budgets with sliding windows, LRU response caching with TTL, secret redaction (10 built-in rules: API keys, AWS keys, JWTs, credit cards, etc.), reverse proxy with token metering from `usage.total_tokens`. CLI: `minga egress --bind --upstream --default-budget`. 53 tests. |

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
- [x] **Overlay networking** — `minga-net` crate: CIDR/IPAM (deterministic allocation from 10.100.0.0/16), WireGuard keypairs + config generation, DNS service discovery (`minga.local`), `OverlayManager` coordinator. State types extended: `Network.subnet`, `ClusterNode.wg_*`, `Task.ips`. CLI: `minga net create/inspect/alloc-subnet`, `minga wg keygen/pubkey/config`. Kernel-level data plane (netlink, `wg` apply) is a follow-up.
- [x] **Docker Engine API** — `minga-api` crate: Docker Engine API v1.41 REST subset (axum). Services, tasks, nodes, networks, secrets, configs — full CRUD with label filters. Bidirectional translation between Docker API types and Minga Raft state. `minga api` CLI subcommand (`--bind`, `--node-id`, `--data-dir`, `--peers`). Real `docker` CLI verified end-to-end via testcontainers (`docker service ls`, `docker node ls`, `docker info`, `docker version`). 76 tests (64 unit + 11 integration + 1 doc).
- [x] **Agent sandboxes** — `minga-sandbox` crate: `SandboxRuntime` trait with `MockSandboxRuntime`, `WasmtimeRuntime` (real WASM execution via wasmtime 24 with fuel metering, memory limits, timeouts, output capture), `WarmPool` (pre-warmed pool with acquire/release/run, background replenish + idle eviction, pool stats), `FirecrackerRuntime` stub (Linux + KVM detection). WASM module contract: exports `memory` + `run(input_len: i32) -> i32`, input at offset 1024, output at offset 0 with 4-byte LE length prefix. CLI: `minga sandbox run --module foo.wasm --input '{"q":"hi"}'`, `minga sandbox info`. 36 tests (38 on Linux).
- [x] **LLM egress proxy** — `minga-egress` crate: axum reverse proxy with per-workload token budgets (`BudgetTracker`, sliding window, register/unregister/check/record), LRU response caching (`ResponseCache`, FNV-1a key, TTL, only caches 2xx), secret redaction (`SecretRedactor`, 10 built-in regex rules: OpenAI/Anthropic keys, AWS keys, JWTs, credit cards, emails, hex secrets, private keys, bearer tokens), token metering from `usage.total_tokens` with char/4 fallback. Workload identified via `X-Minga-Workload` header. CLI: `minga egress --bind 0.0.0.0:8080 --upstream https://api.openai.com --default-budget 100000`. 53 tests.

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
