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

Each ADR ends with an *Implementation status* section saying what shipped and where the code deviates.

## Plans

- [minga-ctl design plan](minga-ctl-plan.md): proposed companion CLI; SSH-only transport first, mutual TLS and server-side audit next

## Deployment

- [Quick start: one Linux host](../README.md#quick-start-one-linux-host): the verified path
- [systemd unit](../scripts/minga.service) and [environment example](../scripts/minga.env.example)
- [Talos Linux and Hetzner Cloud](talos-deployment.md): **not validated**. Minga on Talos needs a system extension, which doesn't exist yet.

## History and research

- [Roadmap to v0.1.0](roadmap-v0.1.0.md): the seven release gates (historical, all closed)
- [NATS/JetStream evaluation](research/nats-evaluation.md): the research behind ADR 0002
- [Design document](design.md): link to the living design doc
- [Wave 9 plan](../Wave-9-PLAN.md): historical wave plan

## Development

- [Contributing](../CONTRIBUTING.md)
- [Project conventions](../CLAUDE.md)
