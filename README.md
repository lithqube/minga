# Minga

A single-binary container and agent orchestrator for edge, on-prem and air-gapped sites, written in Rust. It runs Compose files natively, speaks enough of the Docker Swarm API to replace it, and treats AI-agent workloads (sandboxes, durable runs, budgets, GPUs) as first-class objects.

Named after the minga of Chiloé, where a community moves a whole house together. Status: design and scaffolding (phase 0).

Design doc: https://claude.ai/code/artifact/084e961f-88e4-45e2-aa1a-c8a76928b575

## Layout

| Crate | Purpose |
| --- | --- |
| `minga-cli` | The `minga` binary: manager, worker and CLI |
| `minga-compose` | Compose Specification parser and validator |
| `minga-state` | openraft + redb cluster state, sharded Raft groups |
| `minga-scheduler` | Site-level placement, Compose services to tasks |
| `minga-api` | Docker Engine API subset and native gRPC API |
| `minga-node` | Node supervisor, containerd integration, offline operation |
| `minga-net` | WireGuard overlay, nftables/eBPF, ingress |
| `minga-sandbox` | wasmtime and Firecracker sandbox pools |
| `minga-egress` | LLM egress proxy with budgets |

## Build

```bash
cargo build
```

```bash
cargo run -p minga-cli -- --help
```

## Validate a Compose file

`minga validate` checks a Compose file and prints the services in startup order. It reports every problem at once: typos (with a suggestion), Compose keys Minga does not support yet, undefined references, dependency cycles and port conflicts.

```bash
cargo run -p minga-cli -- validate -c compose.yaml --profile debug
```

## Phase 0 gate

A 10-service Compose stack runs on 3 nodes.
