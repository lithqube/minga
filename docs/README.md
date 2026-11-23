# Minga documentation

## Start here

- [Project README](../README.md): what works, quick start, diagrams
- [Architecture notes](architecture.md): how the pieces fit today, structural limits, and the plan
- [Changelog](../CHANGELOG.md)

## Architecture decision records

- [ADR 0001: Language split](adr/0001-language-split.md): Rust on the hot path; Go components run as separate processes
- [ADR 0002: Event bus: NATS/JetStream](adr/0002-event-bus-nats.md)
- [ADR 0003: Agent workloads](adr/0003-agent-workloads.md): WASM execution and remaining host-capability limits
- [ADR 0004: Observability](adr/0004-observability.md)
- [ADR 0005: API identity, roles and audit](adr/0005-api-identity-and-roles.md): partially implemented; server-side revisions and client-certificate use beyond `minga-ctl change` remain open
- [ADR 0006: Workers leave Raft](adr/0006-workers-leave-raft.md): implemented in v1.3.0
- [ADR 0007: WireGuard and routing mesh](adr/0007-wireguard-mesh.md): implemented in Phase 13; scale limits remain
- [ADR 0008: Multi-tenancy and isolation](adr/0008-multi-tenancy.md): T1–T5 implementation complete; T3 live proof remains open
- [ADR 0009: Version the Raft apply rules](adr/0009-raft-apply-rule-versions.md): state mechanism implemented; the operator command is tracked in #188

Implementation-status notes distinguish what is merged from what remains proposed or unverified.

## Plans

- [Agent-plane roadmap](agent-plane-roadmap.md): Order 1 is merged; remaining cross-node budgets/rate limits and later agent features
- [Sandbox host-capabilities plan](sandbox-host-capabilities-plan.md): Wasmtime runtime limits and grant schema landed; WASI host support remains open
- [Planning state](../.planning/STATE.md) and [roadmap](../.planning/ROADMAP.md): phases and verification gates on `main`
- [Pre-v1.3.0 Phase 16 plan](../.planning/phases/16-agent-ready-operations/16-PLAN.md): operations diagnosis, upgrade preflight, and tenant-scoped agent tools with approval
- [minga-ctl design plan](minga-ctl-plan.md): operator CLI design and implementation boundaries
- [Production-readiness plan](production-readiness-plan.md): historical single-node readiness work
- [Resource benchmark](benchmark.md): measured resource comparisons and caveats
- [Release notes: v1.3.0 (candidate rc.1)](release-1.3.0.md)
- [Release notes: v1.2.0](release-1.2.0.md)
- [Release notes: v1.1.0](release-1.1.0.md)
- [Release notes: v1.0.2](release-1.0.2.md)
- [Release notes: v1.0.1](release-1.0.1.md)

## Deployment

- [Supported topologies](topologies.md): single host, workstation, air-gapped, Talos, container image and multi-node; requirements, proof and limits
- [CLI reference](cli.md): `minga` and `minga-ctl` commands, environment, tenant enrollment and agent usage
- [Agent usage](agent-usage.md): usage events, the usage API, egress routes and `minga-ctl agent usage`
- [Backup and restore](backup.md): state export/import scope and the data it leaves out
- [Quick start: one Linux host](../README.md#quick-start-one-linux-host)
- [systemd unit](../scripts/minga.service) and [environment example](../scripts/minga.env.example)
- [Talos Linux](talos-deployment.md): one docker-provisioned Talos v1.14 node through workload start; bare metal and multi-node remain unvalidated
- [Hetzner ARM staging stack](hetzner-arm-staging.md): example deployment; verify host-specific claims against current status

## History and research

- [Roadmap to v0.1.0](roadmap-v0.1.0.md): the seven release gates (historical, all closed)
- [NATS/JetStream evaluation](research/nats-evaluation.md): the research behind ADR 0002
- [Nevia evaluation](research/nevia-evaluation.md): where Minga could fit with the hosted Nevia agent cloud (notes, no decision)
- [Code Mode evaluation](research/code-mode-evaluation.md): what Minga should take from Cloudflare's Code Mode, and the niche strategy that follows (notes, no decision)
- [Design document](design.md): link to the living design doc
- [Wave 9 plan](../Wave-9-PLAN.md): historical wave plan

## Development

- [Contributing](../CONTRIBUTING.md)
- [Project conventions](../CLAUDE.md)
