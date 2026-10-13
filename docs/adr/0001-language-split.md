# ADR 0001: Language split

- Status: Proposed
- Date: 2026-10-13

## Context

Minga targets small ARM edge devices as well as on-prem clusters up to about 5,000 nodes. It needs one small binary, steady tail latency in the data plane, and memory-safe parsing of untrusted input. The control plane is mostly reconcile loops where garbage-collection pauses do not matter.

## Decision

Plan 1: an all-Rust single binary. Embedded NATS JetStream runs as a Go sidecar process until a Rust-native replacement exists.

Fallback (plan 2): if the phase 0 spike shows the control plane moving too slowly in Rust, move the manager role (API, scheduler, Raft, policy, network control) to Go. The node supervisor, network data plane, sandboxes and egress proxy stay in Rust in every case.

## Rules

- Rust on the hot path: per packet, per request, per sandbox.
- Go is acceptable only for control loops, and only as a separate process.
- No FFI between languages. Process boundaries use a versioned gRPC contract.

## Consequences

- Plan 1 ships one binary and one footprint to tune, at the cost of fewer ready-made libraries (netlink, CNI) and harder hiring.
- Plan 2 reaches Swarm parity faster, at the cost of two binaries and a manager-to-node API to version.
