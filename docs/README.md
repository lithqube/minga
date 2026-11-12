# Minga documentation

## Start here

- [Project README](../README.md): what works, quick start, diagrams
- [Architecture notes](architecture.md): how the pieces fit today, structural limits, and the plan
- [Changelog](../CHANGELOG.md)

## Architecture decision records

- [ADR 0001: Language split](adr/0001-language-split.md): Rust everywhere, Go only as a separate process
- [ADR 0002: Event bus: NATS/JetStream](adr/0002-event-bus-nats.md)
- [ADR 0003: Agent workloads](adr/0003-agent-workloads.md)
- [ADR 0004: Observability](adr/0004-observability.md)
- [ADR 0005: API identity, roles and audit](adr/0005-api-identity-and-roles.md) (proposed)

Each ADR ends with an *Implementation status* section saying what shipped and where the code deviates.

## Plans

- [minga-ctl design plan](minga-ctl-plan.md): the companion CLI; SSH-only transport implemented (phase 1), mutual TLS and server-side audit next
- [Production-readiness plan](production-readiness-plan.md): what blocked single-node production after v1.0.2, what v1.1.0 fixed, and what is left
- [Release notes: v1.2.0](release-1.2.0.md)
- [Release notes: v1.1.0](release-1.1.0.md)
- [Release notes: v1.0.2](release-1.0.2.md)

## Deployment

- [Backup and restore](backup.md): `minga state export`/`import`, what it leaves out (secret values, volume data, TLS, NATS), and the cold-copy alternative
- [Quick start: one Linux host](../README.md#quick-start-one-linux-host): the verified path
- [systemd unit](../scripts/minga.service) and [environment example](../scripts/minga.env.example)
- [Talos Linux](talos-deployment.md): validated single-node install on Talos v1.14 through workload start, with no system extension. Bare metal and multi-node are not validated.
- [Hetzner ARM staging stack](hetzner-arm-staging.md): an 8-service stack with Traefik, private GHCR images and named users on one CAX21 node

## History and research

- [Roadmap to v0.1.0](roadmap-v0.1.0.md): the seven release gates (historical, all closed)
- [NATS/JetStream evaluation](research/nats-evaluation.md): the research behind ADR 0002
- [Design document](design.md): link to the living design doc
- [Wave 9 plan](../Wave-9-PLAN.md): historical wave plan

## Development

- [Contributing](../CONTRIBUTING.md)
- [Project conventions](../CLAUDE.md)
