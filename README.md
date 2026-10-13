# Swarm-NG

A single-binary container and agent orchestrator for edge, on-prem and air-gapped sites, written in Rust. It runs Compose files natively, speaks enough of the Docker Swarm API to replace it, and treats AI-agent workloads (sandboxes, durable runs, budgets, GPUs) as first-class objects.

"Swarm-NG" is a working name. Status: design and scaffolding (phase 0).

Design doc: https://claude.ai/code/artifact/084e961f-88e4-45e2-aa1a-c8a76928b575

## Layout

| Crate | Purpose |
| --- | --- |
| `sng` | The single binary: manager, worker and CLI |
| `sng-compose` | Compose Specification parser and validator |
| `sng-state` | openraft + redb cluster state, sharded Raft groups |
| `sng-scheduler` | Site-level placement, Compose services to tasks |
| `sng-api` | Docker Engine API subset and native gRPC API |
| `sng-node` | Node supervisor, containerd integration, offline operation |
| `sng-net` | WireGuard overlay, nftables/eBPF, ingress |
| `sng-sandbox` | wasmtime and Firecracker sandbox pools |
| `sng-egress` | LLM egress proxy with budgets |

## Build

```bash
cargo build
```

```bash
cargo run -p sng -- --help
```

## Phase 0 gate

A 10-service Compose stack runs on 3 nodes.
