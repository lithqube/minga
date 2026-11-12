# Changelog

## [1.2.0] — 2026-11-12

Security fixes from a source audit of 1.1.1 (all twelve P1 leads, plus the
lower-priority leads that share their root causes). Several change behaviour
that worked before; read the [upgrade notes](https://github.com/lithqube/minga/blob/v1.2.0/docs/release-1.2.0.md#upgrade-notes)
before upgrading. 1.1.1 was prepared but not tagged, so its fix ships here.
The release candidate `v1.2.0-rc.1` is cut from the `release/1.2.0` branch for
the staging run; 1.2.0 is tagged from the same branch once it passes.

### Upgrading
- A Raft TLS key readable by group or others is refused at start; `chmod 600`
  it. `minga tls` now writes keys 0600 in 0700 directories.
- A task whose published port is reserved by the node (53, the Raft, metrics
  and egress ports, the NATS sidecar's 4222 and 8222) or already has a host
  listener fails to start with `port N is in use on this node`.
- With no API token, a request carrying an `Origin` header, or a `Host`
  that is a DNS name other than `localhost` or `*.localhost`, gets 403. Set
  `MINGA_API_TOKEN` on any API that is reachable off loopback.
- `minga-ctl stack deploy` through an ssh context needs a token.

### Security
- Owner-only files: `--data-dir` is tightened to 0700 and the redb files to
  0600 at start; the NATS sidecar directory is 0700 and its `server.conf`
  0600; `install.sh` creates `/etc/minga` 0700 and `minga.env` 0600.
- The API token never leaves through a proxy or a redirect: `minga deploy`
  and `minga state` ignore proxy variables and do not follow redirects, and
  `minga-ctl` removes proxy variables from the `minga deploy` it runs.
- Task networking: a task cannot reach services the node binds on
  `127.0.0.1`, and traffic between two stacks' bridges is dropped, while
  published ports stay reachable through the node address.
- Containers get Docker's default masked paths (`/proc/kcore`,
  `/proc/keys`, `/sys/firmware` and others) and read-only paths
  (`/proc/sys`, `/proc/sysrq-trigger` and others), and the deny-all device
  rule is set even without CPU or memory limits.
- A bus Assign only starts a task that Raft assigns to the node, always from
  the Raft spec. On start the supervisor adopts running containers Raft
  still assigns to it and removes every other container, running or exited,
  with its network, DNAT rules, snapshot and staged secrets.
- The API, metrics and egress listeners close a connection that sends no
  request head within 10s and cap concurrent connections at 1024. The Raft
  RPC server times out the TLS handshake (10s) and each read (60s), and caps
  connections at 512.
- A Raft bind off loopback without `--raft-tls-*`, and an API bind off
  loopback without a token, log a warning at start.
- Release workflow: every action is pinned to a commit SHA, jobs get the
  least permissions they need, checkouts do not keep credentials, and
  rustup is a pinned, checksum-verified download.

### Known limits
- Raft without TLS off loopback still starts, with a warning. Use
  `--raft-tls-*` on every multi-node cluster.
- There is no seccomp profile yet.

## [1.1.1] — 2026-11-12

A fix for relative bind mounts, found on a staging node running 1.1.0. The
[1.1.0 upgrade notes](docs/release-1.1.0.md#upgrade-notes) apply when
coming from 1.0.2; from 1.1.0 no wipe is needed.

### Fixed
- `minga deploy -c compose.yaml`, the default, left relative bind-mount
  sources such as `./config/app.yaml` unresolved, because the file name has
  no directory part. The node then resolved them against its own working
  directory: a manager started from the project directory found the file,
  one started by systemd failed with `failed to fulfil mount request: ...
  no such file or directory`. They now resolve against the current
  directory. The code was the same in 1.0.2, so this is not a 1.1.0
  regression.

### Changed
- The API refuses a bind mount whose source is not an absolute path,
  whatever the client, instead of storing a path the node would resolve
  against its working directory. Services stored with a relative source by
  an earlier version fail to start as before; redeploy them.

## [1.1.0] — 2026-11-12

Single-node production fixes from the
[production-readiness plan](docs/production-readiness-plan.md): state that
survives later upgrades, backup and restore, image pulls through containerd's
own API with credential helpers, published ports that behave, and task
failures that explain themselves. See
[docs/release-1.1.0.md](docs/release-1.1.0.md) for verification status and
upgrade notes.

### Upgrading
- **One last wipe.** State written by 1.0.2 or older is refused at start
  with `written by minga 1.0.2 or older ... remove raft_log.redb and state.redb`.
  Remove `raft_log.redb` and `state.redb` from each role's `--data-dir` on
  every node (not the whole directory: with default paths it also holds
  secret values, named volumes and agent modules), upgrade every node
  together, and redeploy. Later upgrades keep state when they only add
  fields.
- Credentialed image pulls need containerd 1.7 or newer.

### Added
- Backup and restore of desired state: `minga state export [-o FILE]
  [--force]` and `minga state import FILE [--overwrite]`, over
  `GET`/`POST /v1.41/state`. The export holds services, networks, volumes and
  secret and config references; not nodes, tasks or secret values. Files are
  written 0600 and an existing file is not replaced without `--force`.
  [docs/backup.md](docs/backup.md) covers what else to copy.
- A failed task records why: `last_error` (one line, credentials scrubbed)
  and `last_exit_code`, kept while the task restarts. The Docker API returns
  them in `Status.Err`, `Status.Message` and `ContainerStatus.ExitCode`.
  `minga-ctl service ps` and `stack ps` gain an `ERROR` column and a hint
  when tasks are down; new `minga-ctl service inspect`.
- Docker credential helpers: `credHelpers[host]`, then `credsStore`, then
  inline `auths`. A configured helper that is missing or fails is an error
  that names it.
- `minga image pull --containerd-socket`.
- Supplementary groups: a named user gets the groups that list it in the
  image's `/etc/group`.
- The Docker API maps `ContainerSpec.Hosts` to `extra_hosts` and back.
- `minga-ctl`: `remote_token_file` may be an env file with a
  `MINGA_API_TOKEN=` line; `stack deploy --help` says that `minga` runs on
  the workstation, with the macOS install command.

### Changed
- Persisted state, Raft log entries and Raft messages are versioned JSON
  (format 2) instead of bincode. Each redb file stores a `format_version`
  marker; data without one, or from a newer format, is refused with a
  message that says why. Rules for changing a persisted type are in
  `crates/minga-state/src/codec.rs`.
- Image pulls go through containerd's Transfer API on the configured socket.
  The credential is never on a command line; the daemon pulls, so its
  network and CA trust apply. On containerd older than 1.7, anonymous pulls
  fall back to `ctr --address <socket>` and credentialed pulls fail with a
  message naming 1.7.
- A bare numeric `user:` listed in the image's `/etc/passwd` runs with that
  user's primary group instead of gid 0, as Docker does.
- A task with `extra_hosts` but no network attachment fails to start
  instead of silently skipping them.
- A Docker `config.json` that exists but is not valid JSON is an error
  instead of being ignored.

### Fixed
- Published ports redirected any traffic to the published port number,
  so a task calling a remote host on a port published on the node (for
  example outbound HTTPS while 443 is published) reached the local task.
  The DNAT rule now matches node addresses only.
- Host-local access to published ports (`127.0.0.1:<port>`, the node's own
  address) and a task reaching a published port through the node's address
  (hairpin) now work.
- A failed image pull or user lookup left the task's network namespace
  behind; teardown also removes the task's `/etc/hosts` file.
- `minga-ctl stack deploy` without a `minga` binary replaced the saved
  rollback target before failing.
- `minga-ctl --debug` printed the harmless macOS ssh notice
  `setsockopt TCP_NODELAY` for every connection.

### Known limits
- A Transfer pull on containerd 1.7 cannot reach a plain-HTTP registry;
  serve TLS with a CA the node trusts.
- Identity tokens are sent as basic credentials; token refresh is not
  implemented. Only stub credential helpers were tested, not real ones.
- The runtime reports exit codes only, so a signal is inferred from
  128+N and an OOM kill reads as `killed by signal 9`. Agent tasks record a
  reason only for a failed start. Credential scrubbing works by shape.
- `minga state import` skips placement checks and updates existing
  networks, volumes and references even without `--overwrite`.
- Some start failures after network setup (image load, snapshot prepare,
  log FIFOs) can still leave a network namespace behind.
- Nodes of one cluster must run the same version.
- API roles, server-side audit and server-side revisions are designed in
  [ADR 0005](docs/adr/0005-api-identity-and-roles.md) (Proposed), not
  built. `minga-ctl` is not in the release archive.

## [1.0.2] — 2026-11-12

Compose and staging fixes from the first real-world deploy, private-registry
pulls, and the `minga-ctl` companion CLI. See
[docs/release-1.0.2.md](docs/release-1.0.2.md) for verification status and
upgrade notes.

### Added
- `minga-ctl`, a companion CLI (new crate `crates/minga-ctl`, binary
  `minga-ctl`): named contexts, `stack`, `service` (scale, update, rollback
  from a local revision, logs), `secret`, `image`, `health`, `metrics` and
  `registry login/logout/list`. Remote contexts go over the system `ssh`
  client only; the API token is read from the node and never stored on the
  workstation; secrets travel on stdin; changes append to a local audit log.
  It is not in the release archive: `cargo install --path crates/minga-ctl`.
  Design and trust model: [docs/minga-ctl-plan.md](docs/minga-ctl-plan.md).
- Compose `env_file` (string or `{path, required}` entries), merged under
  `environment`, which wins on conflicts.
- Compose `extra_hosts`, written to a per-task `/etc/hosts`. `host-gateway`
  is the host's address on the task's bridge, resolved on the node.
- Named users: `user:` (and an image's `USER`) may be `name[:group]`,
  resolved from the image's own `/etc/passwd` and `/etc/group` at task
  start. A name with no group takes the user's primary group.
- Private registries: image pulls (automatic and `minga image pull`) read
  inline `auth` entries from `$DOCKER_CONFIG/config.json`, else
  `~/.docker/config.json`. Docker Hub's `https://index.docker.io/v1/` key
  matches `docker.io` images.
- The API accepts its token as the HTTP Basic-auth password (any username)
  as well as `Authorization: Bearer`, so Traefik's Swarm provider can
  authenticate.
- Relative bind-mount sources are joined to the compose file's directory.

### Fixed
- A string `command` or `entrypoint` (plain or folded `>`) ran as a single
  argv entry. It is now split with shell quoting, as Compose does; an
  unterminated quote fails the deploy.
- Image pull no longer runs `ctr images unpack`, which containerd 2.2
  removed. `minga image pull` failed outright on Ubuntu 24.04.
- Stopped or orphaned containerd tasks are purged when the supervisor
  starts, so a restart no longer needs a manual `ctr tasks delete`.
- `extra_hosts: host-gateway` resolved to the host's default route, which on
  most VMs is the upstream router rather than the host.

### Changed
- The persisted service and task layout gained `extra_hosts`. State written
  by 1.0.1 does not decode: clear `--data-dir` and redeploy, upgrading every
  node together.

### Known limits
- Registry credential helpers (`credsStore`, `credHelpers`) are not read.
  `ctr` receives credentials on its command line, visible in `ps`.
- Supplementary groups from the image's `/etc/group` are not applied.
- `extra_hosts` takes effect only for tasks with node networking.
- `minga-ctl` has been tested against stubs and test API servers, not
  against a real node over SSH. Its rollback covers only changes made by
  `minga-ctl` on the same workstation.

## [1.0.1] — 2026-11-11

Release contents prepared for `1.0.1`. Includes the `0.1.0` entry below
plus the changes here. `0.1.0` was never tagged; this release is numbered
`1.0.1` by maintainer decision. Publishing requires the `v1.0.1` tag. See
[docs/release-1.0.1.md](docs/release-1.0.1.md) for verification status and
installation details.

### Added
- Rolling updates. A changed service spec now replaces its running tasks,
  honoring `update_config` (`order`, `parallelism`, `delay`, `monitor`,
  `failure_action`). Task IDs carry a spec revision
  (`{service}.{slot}.{revision}`). A service with a published port is always
  `stop-first`. `failure_action: rollback` restores the previous spec when a
  replacement restarts or fails before the monitor window ends, using the
  restored spec's own `update_config`, or its `rollback_config` when the
  service sets one (parsed from Compose `deploy.rollback_config` and the Docker
  API, and kept across a rollback).
- Healthcheck enforcement. A task with a healthcheck stays `Starting`, out of
  DNS, until its first probe passes, and is killed after `retries` straight
  failures so the restart policy takes over.
- Image pull. A task whose image is missing from the `minga` containerd
  namespace pulls and unpacks it with `ctr` (300s pull and 120s unpack
  timeouts) instead of failing. `minga image pull <ref>` does the same by
  hand.
- `update_config` and `rollback_config` reject unknown values the same way on
  the Compose and Docker API paths.
- `node.role` placement constraints.
- Compose `user` (numeric `uid[:gid]`) reaches the container.
- `minga secret put` provisions the node-local store from stdin;
  `talos/provision-secret.yaml` runs it on Talos.

### Fixed
- `minga secret put` removes a stale temp file, cleans up after a failed
  write and leaves the mode of an existing store directory alone. A
  non-numeric Compose `user` is rejected when the service is translated
  instead of failing every start after a stop-first update removed the old
  task. `iptables` calls on the task setup path wait for the xtables lock.
- Containers could not reach anything off their bridge: there was no SNAT.
  Each network now gets a `MASQUERADE` rule and `FORWARD` accepts.
- A service whose own tasks filled a node was evicted by its own placement.
- The restart policy was bypassed: any task not tracked as running was
  restarted on the next pass, ignoring backoff, `max_attempts` and
  `condition: none`. A crashed task also stayed `Running`. Tasks now end
  `Failed` or `Stopped`, and show `Starting` while being retried.

### Known limits
- Image pull runs `ctr` as a subprocess: anonymous registry access only, and
  it ignores `--containerd-socket` (it uses `ctr`'s default socket). Preload
  images from private registries. Not exercised against a real registry or
  on Talos for this candidate.
- A named image `USER` (anything but numeric `uid[:gid]`) is still refused.

### Upgrading
- The cluster state and Raft log are bincode-encoded and their record layouts
  changed, so state from an earlier build does not decode. Clear `--data-dir`
  and redeploy. No tagged release has shipped the old layout.

## [0.1.0] — 2026-11-03

First published production-shaped release: a single binary an operator can
install on real hardware and run a Compose stack on, with secrets, logs and
TLS handled honestly. The seven initial release gates were verified by
execution on macOS and OrbStack Ubuntu (containerd 2.2.1), per
`.planning/STATE.md`. This entry also includes subsequent fixes merged before
the first tag; no earlier build was tagged or published.

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
- A bus message that could never be handled redelivered forever, and ack
  failures were silently dropped. Every explicit-ack JetStream consumer now
  stops after 5 deliveries (backoff 30s, 1m, 2m, 5m); a payload that does
  not decode is terminated with its decode error as the reason, and failed
  acks are logged. JetStream has no dead-letter queue, so its
  `MAX_DELIVERIES` and `MSG_TERMINATED` advisories are kept in a new
  `MINGA_DEAD_LETTERS` stream (7 days, 64 MiB). The advisory holds the
  stream sequence, not the body; the body survives only while the source
  stream retains it. `minga_bus_dead_letters_total{topic,reason}` counts
  messages this node dead-lettered.
- Short image references (`nginx:1.27`, the form Compose files habitually
  use) never started: containerd keys images by fully-qualified reference
  (`docker.io/library/nginx:1.27`), and Minga looked up and created with
  the raw string. References are now normalized to the Docker fully
  qualified form before the presence check and container creation; the
  original string is kept on the `minga/image` container label.

### Added
- High-availability NATS: `--nats-url` takes a comma-separated seed list
  (`MINGA_NATS_TLS_SERVER_NAME` a matching list of names), and
  `--nats-replicas` (`MINGA_NATS_REPLICAS`, 1–5) keeps that many copies of
  the command, event and dead-letter streams and the status and lease
  buckets. Replicas only go up: a node left at the default of 1 never
  lowers them, and the KV buckets are no longer reset on every connect.
  Logs stay at one copy. The client talks only to the servers it was
  given, not ones a server advertises. The built-in sidecar is still a
  single server, so replicas above 1 need `--nats-url`.
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
- `scripts/minga.service` fixed: the old unit called `minga node` and `--log-level`, which don't exist, used `Type=notify`, and silently ran the mock runtime. It now runs either role from `scripts/minga.env.example`, with SIGINT for graceful shutdown.
- CONTRIBUTING: correct crate table, plus the `nats-server` and containerd test prerequisites.

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
