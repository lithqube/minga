# Minga

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org)
[![Status](https://img.shields.io/badge/status-roadmap%20to%20v0.1.0-in%20progress.svg)]()

A single-binary container and agent orchestrator for **edge, on-prem and air-gapped sites**, written in Rust. It runs Compose files natively, speaks enough of the Docker Swarm API to replace it, and treats AI-agent workloads — sandboxes, durable runs, budgets, GPUs — as first-class objects.

Named after the [minga](https://en.wikipedia.org/wiki/Minga) of Chiloé, where a community moves a whole house together.

> **Status: `v0.1.0`.** The orchestrator core works: Compose parsing that fails loudly on unknown keys, Raft consensus (with mTLS and write forwarding), a scheduler, self-registering nodes with membership leases, a node supervisor, a Swarm-compatible Docker Engine API (v1.41, bearer-token authenticated), a **real containerd runtime proven against a live daemon**, per-task netns/bridge networking with IPAM and embedded DNS, real container logs, NATS/JetStream with TLS, WASM sandboxes, an LLM egress proxy, and Prometheus metrics with health endpoints. ~700 tests across 11 crates; every shipped claim above was verified by execution (see `.planning/STATE.md`).
>
> What `v0.1.0` deliberately does **not** do: pull images (preload them into the `minga` containerd namespace), multi-node WireGuard mesh, healthcheck enforcement, spec-update reprovisioning, automatic rollback, or host-local port hairpinning. The full picture is in the [**v0.1.0 roadmap**](docs/roadmap-v0.1.0.md) and the [changelog](CHANGELOG.md).
>
> The Talos deployment guide is written but **has not been validated on a Talos host**. FIPS, SOC 2, VS-NfD and NATO are **not assessed** — which is not the same as not applicable.

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
- **Overlay networking** — per-task IPs from deterministic IPAM, bridge + netns per task, DNAT port publishing, and embedded service-discovery DNS. (WireGuard mesh between nodes is generated as config but not yet applied.)
- **Agent-first workloads** — wasmtime and Firecracker sandbox pools, LLM egress proxy with per-workload token and cost budgets.
- **Observability built-in** — Prometheus metrics (`/metrics`), health endpoints (`/healthz`, `/readyz`) on `minga api` and on nodes via `--metrics-bind`, structured JSON logs for the long-running roles. (Correlation IDs across bus hops are not propagated yet.)
- **Air-gapped ready** — no cloud dependencies, offline image distribution, vendored builds.
- **One binary** — manager, worker, and CLI in a single executable (glibc-linked; the release bundles `nats-server` alongside it).

## Quick start

```bash
# Install the release binary (linux/amd64, linux/arm64)
curl -fsSL https://raw.githubusercontent.com/lithqube/minga/main/install.sh | sh

# Or build from source
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
  │  ┌──────────┐                                                        │
  │  │minga-    │                                                        │
  │  │telemetry │  Prometheus metrics, health endpoints,                │
  │  │(metrics,  │  structured JSON logging                              │
  │  │ healthz) │                                                        │
  │  └──────────┘                                                        │
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
| `minga-compose` | **Done** | Compose Specification parser and validator. Full interpolation, profiles, merge keys, `x-` extensions (including `x-minga-agent` for agent workloads), dependency cycle detection, port conflict detection. 39 tests. |
| `minga-state` | **Done** | Cluster state: openraft + redb. Multi-node Raft with TCP networking, leader election, failover, persistent log and state machine. `WorkloadKind` discriminant on `TaskSpec` (Container / Agent). 21 tests. |
| `minga-scheduler` | **Done** | Placement engine: Compose → state entities (containers + agents via `x-minga-agent`), spread placement with constraints and resource awareness, reconcile loop. 58 tests. |
| `minga-node` | **Done** | Node supervisor: `Runtime` trait with `MockRuntime` (testing) and `ContainerdRuntime` (real containers via containerd gRPC), plus `AgentExecutor` trait with `MockAgentExecutor` and `WasmtimeAgentExecutor` (WASM sandboxes). OCI spec generation, graceful shutdown (SIGTERM→SIGKILL), image pull. Reconcile loop starts/stops containers and agents from Raft + bus, dispatching by `WorkloadKind`. 67 tests. |
| `minga-bus` | **Done** | Event bus abstraction: `EventBus` trait with `MemoryBus` (in-process) and `NatsBus` (NATS/JetStream). Commands, events, KV status, liveness leases, request/reply. 24 tests. |
| `minga-api` | **Done** | Docker Engine API v1.41 REST subset: services, tasks, nodes, networks, secrets, configs CRUD with label filters. Bidirectional translation between Docker API types and Minga state. `minga api` CLI subcommand. Real `docker` CLI verified via testcontainers. 76 tests. |
| `minga-net` | **Done** | Overlay networking: CIDR utilities, deterministic IPAM (hash-based allocation, linear probing), WireGuard keypair generation (X25519) + `wg-quick` config generation, DNS service discovery (`minga.local`), `OverlayManager` coordinator. 70 tests. |
| `minga-sandbox` | **Done** | Agent sandboxes: `SandboxRuntime` trait with `MockSandboxRuntime` (testing), `WasmtimeRuntime` (real WASM execution with fuel metering, memory limits, timeouts), `WarmPool` (pre-warmed sandbox pool with acquire/release, background replenish/evict), `FirecrackerRuntime` stub (Linux + KVM). Wired into the supervisor via `WasmtimeAgentExecutor`. CLI: `minga sandbox run`, `minga sandbox info`. 36 tests. |
| `minga-egress` | **Done** | LLM egress proxy (axum): per-workload token budgets with sliding windows, LRU response caching with TTL, secret redaction (10 built-in rules: API keys, AWS keys, JWTs, credit cards, etc.), reverse proxy with token metering from `usage.total_tokens`. Wired into the node via `--egress-bind` / `--egress-upstream` flags. CLI: `minga egress --bind --upstream --default-budget`. 53 tests. |
| `minga-telemetry` | **Done** | Observability crate: Prometheus metrics registry (`minga_*` prefix), `/metrics` endpoint, `/healthz` (liveness), `/readyz` (readiness: Raft + bus), structured JSON logging via `tracing` (`MINGA_LOG_LEVEL`). Served by `minga api` and, with `--metrics-bind`, by `minga manager`/`minga worker`. Correlation IDs are not propagated yet. |

## Deployment Topologies

Minga runs anywhere containerd runs. Here are the recommended topologies for different environments.

### 1. Cloud-Native (Hetzner Cloud / Talos Linux) — **Recommended for Production**

```
┌─────────────────────────────────────────────────────────────────┐
│                        Hetzner Cloud                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Load Balancer (LB11) — :6443 (talosctl, API)            │  │
│  └──────────────┬───────────────────────────────────────────┘  │
│                 │                                               │
│  ┌──────────────┴───────────────────────────────────────────┐  │
│  │              Private Network (10.0.0.0/16)               │  │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐       │  │
│  │  │ minga-  │ │ minga-  │ │ minga-  │ │ minga-  │       │  │
│  │  │  cp1    │ │  cp2    │ │  cp3    │ │ worker1 │       │  │
│  │  │ Talos   │ │ Talos   │ │ Talos   │ │ Talos   │       │  │
│  │  │ (mgr)   │ │ (mgr)   │ │ (mgr)   │ │ (wrk)   │       │  │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘       │  │
│  └───────┼────────────┼────────────┼──────────┼────────────┘  │
│          │            │            │          │               │
│    ┌─────┴─────┐ ┌────┴────┐ ┌────┴────┐ ┌──┴────┐          │
│    │ containerd│ │ containerd│ │ containerd│ │containerd    │
│    │ (unix sock)│ │ (unix sock)│ │ (unix sock)│ │ (unix sock)  │
│    └─────┬─────┘ └────┬────┘ └────┬────┘ └──┬────┘          │
│          │            │            │          │               │
│    ┌─────┴────────────┴────────────┴──────────┴────┐        │
│    │           WireGuard Mesh (minga-net)          │        │
│    │  IPAM: 10.200.0.0/16 (deterministic, hash)    │        │
│    │  DNS:  *.minga.local                          │        │
│    └───────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

**Key characteristics:**
- **Talos Linux** — immutable, API-driven, no SSH, atomic upgrades via `talosctl upgrade`
- **3 control plane nodes** — Raft HA (openraft), NATS leader on one CP
- **Load balancer** — stable VIP for talosctl, Docker API, and Minga API
- **Private network** — isolated cluster traffic, WireGuard mesh
- **Cost** — ~€28/mo for 3 CP + 2 Worker (CPX22, Nuremberg)

**Setup:** See [docs/talos-deployment.md](docs/talos-deployment.md#hetzner-cloud-hcloud-deployment) for step-by-step `hcloud` CLI and Terraform.

---

### 2. Edge / On-Prem (Talos on Bare Metal) — **Recommended for Edge**

```
┌─────────────────────────────────────────────────────────────────┐
│                      Edge Site (e.g., factory)                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Control Plane (3 nodes, e.g., Raspberry Pi CM4 / x86)   │  │
│  │  ┌────────┐ ┌────────┐ ┌────────┐                         │  │
│  │  │ minga  │ │ minga  │ │ minga  │  ← Raft + NATS          │  │
│  │  │ cp1    │ │ cp2    │ │ cp3    │  ← Talos (PXE/ISO)      │  │
│  │  └────┬───┘ └────┬───┘ └────┬───┘                         │  │
│  └───────┼────────────┼──────────┼───────────────────────────┘  │
│          │            │          │                              │
│  ┌───────┴────────────┴──────────┴──────────┐                  │
│  │          Worker Nodes (N x ARM/x86)       │                  │
│  │  ┌────────┐ ┌────────┐ ┌────────┐         │                  │
│  │  │ minga  │ │ minga  │ │ minga  │  ← Talos worker        │  │
│  │  │ w1     │ │ w2     │ │ wN     │         │                  │
│  │  └────┬───┘ └────┬───┘ └────┬───┘         │                  │
│  └───────┼────────────┼──────────┼────────────┘                  │
│          │            │          │                               │
│    ┌─────┴────────────┴──────────┴────┐                         │
│    │      WireGuard Mesh (minga-net)  │                         │
│    │  IPAM: 10.200.0.0/16             │                         │
│    │  DNS:  *.minga.local             │                         │
│    └──────────────────────────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

**Key characteristics:**
- **Talos Linux** — same as cloud, but PXE/ISO install on bare metal
- **No load balancer needed** — control plane IPs stable; use DNS round-robin or keepalived if VIP desired
- **Air-gapped friendly** — pre-load images in Talos ISO, `talosctl images load` offline
- **ARM64 + x86_64 mixed** — deterministic IPAM works across architectures

**Setup:** See [docs/talos-deployment.md](docs/talos-deployment.md#quick-start-3-node-cluster) for bare-metal PXE/ISO install.

---

### 3. Air-Gapped / Isolated (Talos ISO + Offline Images)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Air-Gapped Environment                       │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  1. Build Talos ISO with pre-loaded images               │  │
│  │     talosctl image factory --output iso \                 │  │
│  │       --extra-registry registry.local \                   │  │
│  │       --extra-image registry.local/minga:latest          │  │
│  └──────────────────────────┬───────────────────────────────┘  │
│                             │                                  │
│  ┌──────────────────────────┴───────────────────────────────┐  │
│  │  2. Boot nodes from ISO (PXE, USB, virt-install)         │  │
│  │     - No internet access required                        │  │
│  │     - All images baked into ISO                          │  │
│  └──────────────────────────┬───────────────────────────────┘  │
│                             │                                  │
│  ┌──────────────────────────┴───────────────────────────────┐  │
│  │  3. Apply machine config (generate on admin machine,     │  │
│  │     transfer via USB):                                    │  │
│  │     talosctl apply-config --insecure --nodes <ip>        │  │
│  │       --file controlplane.yaml                            │  │
│  └──────────────────────────┬───────────────────────────────┘  │
│                             │                                  │
│  ┌──────────────────────────┴───────────────────────────────┐  │
│  │  4. Bootstrap etcd, deploy Minga binary via talosctl     │  │
│  │     talosctl bootstrap --nodes <cp1-ip>                   │  │
│  │     talosctl --nodes <ip> upload minga /opt/minga/minga  │  │
│  │     talosctl --nodes <ip> run --command "systemctl ...   │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Key characteristics:**
- **Zero internet access** at target site
- **All dependencies vendored** in Talos ISO (containerd, Minga, images)
- **Single admin machine** with internet generates config/ISO
- **USB/PXE transfer** for config and ISO

---

### 4. Development / Single-Node (Talos or Linux)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Single Node (Laptop/VM)                      │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Single Talos node (control plane + worker)              │  │
│  │  ┌────────────────────────────────────────────────────┐  │  │
│  │  │ minga-manager + minga-worker                       │  │  │
│  │  │ Raft (single node), NATS (embedded sidecar)        │  │  │
│  │  │ containerd (unix:///run/containerd/containerd.sock)│  │  │
│  │  └────────────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Key characteristics:**
- **Single Talos node** — self-bootstraps, no LB needed
- **Or native Linux** — any distro with containerd (`apt install containerd` / `dnf install containerd`)
- **Minga binary** — `minga node --containerd-socket unix:///run/containerd/containerd.sock`
- **NATS sidecar** — `minga sidecar nats` (manager spawns it)

---

### 5. Kubernetes Alternative (Not Recommended, but Possible)

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Minga as a DaemonSet on each node                       │  │
│  │  - HostPath: /run/containerd/containerd.sock             │  │
│  │  - HostNetwork: true (for WireGuard)                     │  │
│  │  - Privileged: true (for nftables, WireGuard)            │  │
│  │  - ConfigMap: Minga config                               │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

> **Not recommended** — Minga replaces Kubernetes for these workloads. Running inside K8s defeats the purpose (extra layer, no air-gap, complex networking).

---

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
- [x] **Containerd runtime** — `ContainerdRuntime` implementing the `Runtime` trait via containerd's gRPC API (Unix socket). OCI spec generation from `TaskSpec`, preloaded-image metadata resolution (manifest, index, config, parent snapshot — containerd has no Pull RPC, so images must be preloaded and unpacked in namespace `minga`), container/task lifecycle, graceful shutdown (SIGTERM → grace → SIGKILL, with the task deleted before the container — containerd refuses to delete a container that still has a task). CLI: `--runtime containerd --containerd-socket /run/containerd/containerd.sock`. The lifecycle test passes against a live containerd daemon with `MINGA_REQUIRE_CONTAINERD=1` so a missing daemon cannot silently skip.
- [x] **Overlay networking (config only)** — `minga-net` crate: CIDR/IPAM (deterministic allocation from 10.100.0.0/16), WireGuard keypairs + `wg-quick` config generation, DNS service discovery data model (`minga.local`), `OverlayManager` coordinator. State types extended: `Network.subnet`, `ClusterNode.wg_*`, `Task.ips`. CLI: `minga net create/inspect/alloc-subnet`, `minga wg keygen/pubkey/config`. **No kernel effect yet**: nothing calls `wg`, `ip`, `nft` or `iptables`, task IPs are never assigned, and the overlay manager is not invoked by the node path. A real CNI data plane is roadmap Gate 3.
- [x] **Docker Engine API** — `minga-api` crate: Docker Engine API v1.41 REST subset (axum). Services, tasks, nodes, networks, secrets, configs — full CRUD with label filters. Bidirectional translation between Docker API types and Minga Raft state. `minga api` CLI subcommand (`--bind`, `--node-id`, `--data-dir`, `--peers`). Real `docker` CLI verified end-to-end via testcontainers (`docker service ls`, `docker node ls`, `docker info`, `docker version`). 76 tests (64 unit + 11 integration + 1 doc).
- [x] **Agent sandboxes** — `minga-sandbox` crate: `SandboxRuntime` trait with `MockSandboxRuntime`, `WasmtimeRuntime` (real WASM execution via wasmtime 24 with fuel metering, memory limits, timeouts, output capture), `WarmPool` (pre-warmed pool with acquire/release/run, background replenish + idle eviction, pool stats), `FirecrackerRuntime` stub (Linux + KVM). WASM module contract: exports `memory` + `run(input_len: i32) -> i32`, input at offset 1024, output at offset 0 with 4-byte LE length prefix. CLI: `minga sandbox run --module foo.wasm --input '{"q":"hi"}'`, `minga sandbox info`. 36 tests (38 on Linux).
- [x] **LLM egress proxy** — `minga-egress` crate: axum reverse proxy with per-workload token budgets (`BudgetTracker`, sliding window, register/unregister/check/record), LRU response caching (`ResponseCache`, FNV-1a key, TTL, only caches 2xx), secret redaction (`SecretRedactor`, 10 built-in regex rules: OpenAI/Anthropic keys, AWS keys, JWTs, credit cards, emails, hex secrets, private keys, bearer tokens), token metering from `usage.total_tokens` with char/4 fallback. Workload identified via `X-Minga-Workload` header. CLI: `minga egress --bind 0.0.0.0:8080 --upstream https://api.openai.com --default-budget 100000`. 53 tests.
- [x] **Agent workloads end-to-end** — `x-minga-agent` Compose extension declares agent services (WASM module, fuel/memory/timeout limits, token budget, egress URL, prompt). Flows through the full pipeline: compose parsing → scheduler translation (`WorkloadKind::Agent` on `TaskSpec`) → Raft placement → bus dispatch → supervisor (`AgentExecutor` trait: `MockAgentExecutor`, `WasmtimeAgentExecutor`) → WASM sandbox execution. Egress proxy co-located on the node. CLI: `minga node --sandbox-runtime wasmtime --module-dir ./modules --egress-bind 127.0.0.1:8080 --egress-upstream https://api.openai.com`, `minga agent ls`. [ADR 0003](docs/adr/0003-agent-workloads.md). 3 new pipeline integration tests (full agent pipeline, mixed container+agent, reconcile creates agent tasks).
- [x] **Observability (Wave 9)** — Prometheus metrics (`/metrics`), health endpoints (`/healthz`, `/readyz`), structured JSON logging. `minga-telemetry` crate. [ADR 0004](docs/adr/0004-observability.md). Integrated into `minga-api` and `minga-node`.
- [x] **NATS TLS (Wave 10)** — mTLS between nodes: explicit CA and client identity, TLS-only connector, no insecure escape hatch, mandatory SAN verification, a missing or malformed CA/cert/key is a hard error. Real `nats-server` integration tests in `crates/minga-bus/tests/nats_tls.rs`. The plaintext sidecar is restricted to loopback.

**Roadmap to `v0.1.0`** — full detail in [`docs/roadmap-v0.1.0.md`](docs/roadmap-v0.1.0.md). Seven gates, each with an executable exit criterion; the evidence for every checkmark is in `.planning/STATE.md`:

- [x] **Gate 1 — cluster membership.** Nodes register themselves with a lease; `POST /nodes` + update; zero-eligible-node deploys fail with HTTP 409.
- [x] **Gate 2 — `minga deploy` deploys.** Writes through the shared Docker-API path; per-service placement report; verified with the real `docker` CLI.
- [x] **Gate 3 — containers talk to each other.** Bridge + veth + netns per task, IPAM, DNAT publishing, embedded service DNS — verified container-to-container by name.
- [x] **Gate 4 — Compose fidelity.** `command`, `entrypoint`, `labels`, `healthcheck`, volumes and placement constraints reach the container; Traefik v3 routes to a Minga service.
- [x] **Gate 5 — transport security and secrets.** Raft mTLS (bounded frames), API bearer auth, NATS credentials, `minga tls` PKI, content-addressed secret staging.
- [x] **Gate 6 — logs.** Real FIFO capture, JetStream history, gap-free follow across a supervisor restart.
- [x] **Gate 7 — shippable.** CI on every push (including daemon-backed tests with `MINGA_REQUIRE_CONTAINERD=1`), release artifacts for linux/amd64 and linux/arm64, `install.sh`, changelog, corrected docs.

## Documentation

- [Design document](docs/design.md) — scope, feature gap vs Swarm, tech stack, roadmap
- [**Roadmap to v0.1.0**](docs/roadmap-v0.1.0.md) — the seven release gates, with the current blockers and their evidence
- [ADR 0001: Language split](docs/adr/0001-language-split.md) — why Rust, when Go is acceptable
- [ADR 0002: Event bus](docs/adr/0002-event-bus-nats.md) — NATS/JetStream behind `minga-bus`
- [ADR 0003: Agent workloads](docs/adr/0003-agent-workloads.md) — WASM sandboxes + LLM egress in the orchestration pipeline
- [ADR 0004: Observability](docs/adr/0004-observability.md) — Prometheus metrics, health endpoints, structured logging
- [Talos Deployment Guide](docs/talos-deployment.md) — **Comprehensive**: bare metal, Hetzner Cloud (hcloud), air-gapped, single-node, Terraform
- [NATS/JetStream evaluation](docs/research/nats-evaluation.md) — should Minga adopt NATS as its event bus, and how
- [CLAUDE.md](CLAUDE.md) — project conventions for AI-assisted development

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a PR.

## License

[MIT](LICENSE) — Copyright (c) 2026 Minga contributors.