# Minga

## What This Is

Rust Cargo workspace for an edge/on-prem/air-gapped orchestrator. It parses Compose, stores desired state and placement via openraft/redb, uses NATS JetStream for ephemeral commands/status/logs, and talks to containerd for container workloads. `CLAUDE.md`, `README.md` and the ADRs define project conventions and current intended architecture; operational claims in deployment guides require validation.

## Core Value

Run Compose-described containers and agents at edge sites with a small Rust control plane, while preserving explicit failure on unsupported Compose syntax and testable security boundaries.

## Requirements

Wave 10 engineering requirements are in `.planning/REQUIREMENTS.md`. They are targets, not verified delivery or compliance approvals.

## Current planning scope

Wave 10 is the first GSD-tracked phase in this repo. One developer, sequential B → C → A implementation: NATS TLS, file-based secrets, real container log capture. A Talos v1.14/hcloud staging deployment and FIPS/SOC 2/VS-NfD/NATO control-gap assessment are requirements, not existing capabilities/certifications. See `.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`, and `10-RESEARCH.md` for conditional technical choices.

## Constraints

- Cargo workspace, Rust hot path, no Go/Rust FFI (see `CLAUDE.md`).
- Only desired state/placement goes through Raft; no secret values in Raft or JetStream.
- Compose files deploy unchanged for supported syntax; unsupported features fail loudly.
- Do not claim a staging deployment from tests alone; do not claim government/audit certification.
- No billable resource provisioning or repository commit/push without explicit operator authorization.
