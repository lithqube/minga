# Changelog

## [Unreleased]

### Fixed
- `minga deploy` kept neither `deploy.restart_policy` nor `x-minga-agent`:
  the Docker-API round trip reset the restart policy to its default (and
  always reported `any`), and rejected agent services for having no image.
  Both now reach state; the agent spec travels as the `minga.agent` label.
  A service created without a restart policy now restarts on any exit, as
  in Docker; sub-second restart delays round up to one second.
- `/readyz` always answered 503. It now reflects a known Raft leader (and,
  on nodes, a reachable bus).
- Metrics were registered but never updated. Task starts/stops/restarts,
  running tasks, scheduler reconcile duration and placements, Raft
  leadership and bus publish latency now move.
- A lease renewal could undo an operator's drain that landed between its
  read and its write. Renewal is now one atomic Raft command, and it never
  recreates a node record that was removed.

### Added
- `--metrics-bind` (`MINGA_METRICS_BIND`) on `minga manager`/`minga worker`
  serves `/metrics`, `/healthz` and `/readyz`.
- JSON logs on stderr for `manager`, `worker` and `api`; level from
  `MINGA_LOG_LEVEL`, then `RUST_LOG`, default `info`.
- Release archives bundle `nats-server` (verified against pinned upstream
  checksums) with its Apache-2.0 license, and the release publishes
  `SHA256SUMS`; `install.sh` verifies the checksum, installs both binaries,
  creates a missing `MINGA_PREFIX`, and installs the licenses under
  `share/minga`.

### Upgrade note
- Lease renewal uses a new Raft command. Upgrade every node before
  relying on mixed versions: an older node cannot apply it.

## [0.1.0] — 2026-11-01

First production-shaped release: a single binary an operator can install on
real hardware and run a Compose stack on, with secrets, logs and TLS handled
honestly. Every claim below was verified by execution, on macOS and on
OrbStack Ubuntu (containerd 2.2.1), per the evidence in `.planning/STATE.md`.

### Cluster lifecycle
- Nodes self-register with a membership lease (10s renewal, 30s TTL);
  placement excludes dead nodes, and a deploy matching zero eligible nodes
  fails with HTTP 409 instead of succeeding silently.
- `POST /nodes` and `POST /nodes/{id}/update` (labels, active/drain; role
  changes, renames and pause are refused loudly).
- Raft writes forward to the leader from any node; frames are capped at
  64 MiB; a node with no elected leader waits (loudly) instead of dying.

### Deploy
- `minga deploy` is a real deploy: networks, secrets, configs and services
  go through the shared Docker-API write path, with idempotent redeploy and
  per-service placement reporting.
- Compose fidelity: `command`/`entrypoint` override image Entrypoint/Cmd
  with Docker semantics, labels, healthcheck (carried), named volumes and
  bind mounts, and placement constraints all reach the container.
- Task lifecycle states (`running`/`stopped`) are visible in the Docker API;
  Traefik v3's swarm provider discovers and routes to Minga services.

### Networking
- Deterministic per-task IPs from IPAM; one bridge per overlay network, a
  netns + veth pair per task, iptables DNAT for published ports.
- Embedded DNS on bridge gateways: bare service names, `tasks.<svc>`,
  `<svc>.<stack>.minga.local` and task FQDNs; non-cluster names forward
  upstream.

### Security
- Raft mTLS (`--raft-tls-ca/-cert/-key`): uncredentialed and foreign-CA
  peers fail the handshake before they can vote.
- Docker API bearer token (`--api-token`): unauthenticated requests get 401.
- NATS token auth (`--nats-token`) on top of the existing mTLS.
- `minga tls` CA and node-certificate issuance (rcgen).
- Secrets are staged per-task, content-addressed by SHA-256: a rotated
  secret is visible to a new task and never changes under a running one.

### Operations
- Real container logs: `minga logs [-f] <task>` with separated
  stdout/stderr and a JetStream-retained backlog that survives restarts.

### Known limits (stated, not hidden)
- **No image pull**: preload and unpack images in the `minga` containerd
  namespace (`ctr -n minga images pull …`).
- Multi-node WireGuard mesh, host-local published-port hairpin, healthcheck
  enforcement, spec-update reprovisioning, and automatic rollback are not
  in this release.
- Talos/hcloud staging and compliance certifications (FIPS, SOC 2, VS-NfD,
  NATO) are **not assessed** — which is not the same as not applicable.
