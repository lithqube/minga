---
title: 'Minga v0.1.0: seven gates, and what is still missing'
description: 'How v0.1.0 went from a deploy that "succeeded" into an empty cluster to a release where every claim has an executable exit criterion, and the limits we wrote down instead of hiding.'
pubDate: 2026-11-01
---

## The deploy that succeeded into an empty cluster

On 2026-10-27 Minga had 563 passing tests, a real containerd runtime, Raft, a scheduler and a Docker Engine API. Then a probe posted one service to the API and counted what happened next:

```
PROBE api_create_status=201
PROBE services_in_state=1
PROBE nodes_in_state=0
PROBE reconcile_actions=0        ← nothing placed
PROBE supervisor_running=0       ← no container
```

The API said 201. The scheduler logged a warning. No node had ever registered itself, so placement filtered over an empty list and produced nothing. An operator would have seen a successful deploy and an empty cluster.

Every test passed because the gate test registered its nodes by hand. That is the failure mode this release was organised around: tests that pass while the product can't run.

## The gate method

Instead of a feature list, v0.1.0 had seven gates, each with an exit criterion that a machine checks:

1. Cluster membership: nodes self-register, hold leases, and an unplaceable deploy returns HTTP 409 instead of a warning.
2. `minga deploy` deploys through the same API write path as `docker stack deploy`.
3. Container networking on a node: bridge, netns, DNAT and embedded DNS.
4. Compose fidelity: Traefik v3 routes to a Minga service.
5. Transport security and secrets: Raft mTLS, an API token, NATS credentials, `minga tls`, secret staging.
6. Real container logs.
7. Shippable: CI, release archives, `install.sh` and a changelog.

The order mattered. Each blocker was invisible until the previous one was fixed, so fixing them out of order would have produced green tests and a product that still could not run. All seven closed on 2026-11-01.

## What is not in it

These are on the landing page, not in a footnote:

- No image pull. Preload images into the `minga` containerd namespace.
- No container networking across nodes. The WireGuard config is generated but not applied.
- No joining a running cluster. Raft membership is fixed at bootstrap.
- No healthcheck enforcement, spec-update reprovisioning, automatic rollback or host-local port hairpinning.

The structural limit that matters most is less visible: every node is a Raft voter. Workers and `minga api` open a full state store, so quorum depends on workers, and a worker that wins an election pauses placement. The plan is managers-only Raft, with workers as pure JetStream clients.

## Tradeoffs

Writing limits down costs adoption. Someone comparing Minga with Swarm or k3s on a feature grid will pick the mature tool, and they should. What we get in return is that the first person who runs the quickstart on a real edge box doesn't hit an undisclosed wall and decide the project lied.

Single-sourcing the docs costs flexibility. This site renders the repo's README, ADRs and changelog at build time and fails the build if a section it expects disappears. A marketing page can't promise what the README doesn't.

## Key takeaways

- A green test suite proves the tests, not the product. Write the probe that exercises the real path.
- Order gates by dependency. A blocker hidden behind another blocker will pass review.
- Put the limits where the claims are.

Try it on one Linux host with the [quickstart](../../docs/quickstart/), or read what comes next on the [roadmap](../../roadmap/).
