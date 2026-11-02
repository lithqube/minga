# Changelog

## [Unreleased]

### Security
- The LLM egress proxy no longer trusts the self-asserted `X-Minga-Workload`
  header. Each agent run gets a per-sandbox token (`MINGA_EGRESS_TOKEN`,
  sent as `X-Minga-Sandbox-Token`) that the node mints and revokes when the
  run ends. Requests without a live token get 401, and a workload header
  that contradicts the token gets 403. `minga egress` prints the token it
  mints for `default` at startup. Breaking for direct proxy clients.
- Agents hold no provider credentials. `--egress-credential <secret-id>`
  (`--credential` on `minga egress`) loads the key from the node secret
  store, and the proxy injects it as `Authorization: Bearer` or `x-api-key`
  (`--egress-credential-header`). Credential headers the client sends are
  dropped; only `accept`, `anthropic-version` and `anthropic-beta` pass
  through. The key is redacted from proxy errors and upstream error bodies.

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
- Short image references (`nginx:1.27`, the form Compose files habitually
  use) never started: containerd keys images by fully-qualified reference
  (`docker.io/library/nginx:1.27`), and Minga looked up and created with
  the raw string. References are now normalized to the Docker fully
  qualified form before the presence check and container creation; the
  original string is kept on the `minga/image` container label.

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
- The release workflow pushes a multi-arch image (`linux/amd64`,
  `linux/arm64`) to `ghcr.io/lithqube/minga`, built from the same binaries
  as the archives and tagged `vX.Y.Z`, `X.Y` and `latest`. A manual
  workflow run validates the pipeline by pushing only the `:edge` tag.
- `install.sh` is now a small CLI: `--version`/`--prefix` (env equivalents
  kept), `--with-systemd` (installs the unit and an `/etc/minga/minga.env`
  template; the archive now carries both files), `--uninstall` (keeps
  `/etc/minga` and `/var/lib/minga`), and `MINGA_BASE_URL` for mirrors and
  air-gapped sites.
- The release workflow refuses a tag that doesn't match the workspace
  version in `Cargo.toml`.
- Talos install path. On Talos v1.14+ Minga runs as `ContainerConfig`
  containers pulled from the registry — no system extension or custom boot
  asset. `talos/single-node.yaml` holds the documents (plus
  `talos/preload-images.yaml`, a Kubernetes Job that preloads workload
  images into the `minga` namespace with the bundled `ctr`), and
  `scripts/setup-talos-minga.sh` is functional: gen config, append
  documents, validate, apply, bootstrap, verify `/readyz`. Validated on
  docker-provisioned Talos v1.14.5: containers running, `/readyz` OK, API
  with token auth, image preload via the Job, nginx task placed, started
  and listening with correct netns wiring (published-port delivery in the
  nested environment showed a one-directional anomaly; bare-metal
  validation pending).
- The image and the release archive now bundle a checksum-pinned `ctr`
  (containerd v2.1.4). `install.sh` installs it only when the host has no
  `ctr` on PATH (never shadowing a distro one) and records a marker so
  `--uninstall` removes only what it installed.
- The image bundles `iproute2`, `iptables` and `procps`: the supervisor
  shells out to `ip`, `iptables` and `sysctl`. Container deployments
  (including the Talos `ContainerConfig` path) also need `/run/netns`
  mounted with shared propagation so runc on the host sees the handles.
- Release binaries are built on Debian bullseye (glibc 2.31 floor): they
  run on Debian 11+, Ubuntu 20.04+, Fedora 32+ — previously the
  ubuntu-24.04 build required glibc 2.39, excluding Debian 12 and
  Ubuntu 22.04.

### Documentation
- README rewritten against the current CLI. It now has Mermaid diagrams (process architecture, deploy sequence, node networking, topologies), a verified single-host quick start, and an honest multi-node section (external TLS NATS, static Raft membership).
- New `docs/architecture.md`: where state lives, the deploy round trip, known structural limits and the target design.
- ADRs gained *Implementation status* sections; ADR 0001 is now Accepted (plan 1).
- The Talos guide no longer gives install steps that can't work. Talos needs a system extension, which doesn't exist yet. `scripts/setup-talos-minga.sh` now exits with that explanation.
- `scripts/minga.service` fixed: the old unit called `minga node` and `--log-level`, which don't exist, used `Type=notify`, and silently ran the mock runtime. It now runs either role from `scripts/minga.env.example`, with SIGINT for graceful shutdown.
- CONTRIBUTING: correct crate table, plus the `nats-server` and containerd test prerequisites.
- Correction to the `v0.1.0` notes: NATS token auth applies only to plaintext loopback connections. When TLS is configured, the client ignores `--nats-token`, so external TLS connections are certificate-only. A multi-node cluster must point every manager and worker at one external TLS NATS server.

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
