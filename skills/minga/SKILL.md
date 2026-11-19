---
name: minga
description: Operate a Minga cluster (the single-binary Rust orchestrator that replaces Docker Swarm on edge, on-prem and air-gapped sites) with the `minga` and `minga-ctl` CLIs. Use this whenever the user wants to deploy or validate a Compose file on Minga, run or inspect stacks, services, tasks, logs, secrets, images or registry logins, set up a `minga-ctl` context, start manager / worker / api roles, back up or restore cluster state, upgrade a node, or debug a Minga node - a task that is not placed or keeps failing, a 409 on deploy, `/readyz` returning 503, an image pull that fails, a node that refuses its data directory, a rejected Compose key - even if they only say "deploy this to the edge site" or "why is my stack not coming up" in a project that uses Minga.
---

# Operating Minga

Minga runs Compose files on a single Rust binary: managers hold Raft desired state and placement, NATS/JetStream carries runtime status and events, and containerd runs containers. Workers are JetStream clients, not Raft voters. On multi-tenant clusters, each worker serves one tenant. For changing Minga's code, follow the repository's `CLAUDE.md`.

Version claims are release-specific. Check `minga --version` / `minga-ctl --version` and the target machine before acting. The latest tagged release is v1.2.0; current `main` includes unreleased multi-node WireGuard, tenant isolation, agent egress and usage changes. Do not describe current-main behavior as shipped in v1.2.0. For current status and verification gates, read `README.md`, `docs/architecture.md` and `.planning/STATE.md`.

## Two CLIs

| Tool | What it is | Use it for |
| --- | --- | --- |
| `minga` | Node daemon and client commands; manager and worker roles. There is no separate `api` role. | Running nodes, `validate`, `deploy`, `logs`, `image pull`, `state export` / `import`, TLS and tenant-enrollment utilities, sandbox, agent and egress commands. |
| `minga-ctl` | Companion CLI on the operator's machine. Talks to the manager API over SSH or HTTPS. | Contexts, stacks, services, health, metrics, images/secrets/registry and `agent usage`. |

`minga-ctl` is not in the release archive: install it from the matching source tag with `cargo install --locked --path crates/minga-ctl`. It runs the `minga` binary **on the operator's machine** for `stack deploy` (through the tunnel), so `minga` must be on PATH there or `MINGA_BIN` set; the node only needs the API.

Which deploy command: `minga-ctl stack deploy` from a workstation (it saves specs for `service rollback` and has `--prune`); `minga deploy` on the node itself, in CI or scripts. See `docs/cli.md` for commands and `docs/topologies.md` for supported layouts.

## Deploying a Compose file

1. **Validate offline.** `minga validate -c compose.yaml` (add `-p <profile>` as needed). It prints the services in startup order.
2. **Check the node is ready.** `minga-ctl health`, or `curl <metrics-bind>/readyz` on the node. A 503 body says why.
3. **Deploy.**
   - With a context: `minga-ctl stack deploy -c compose.yaml <stack>`. Prefer this: it saves each changed service's previous spec so `service rollback` works.
   - Directly: `MINGA_API_URL=... MINGA_API_TOKEN=... minga deploy -c compose.yaml <stack>`.
4. **Verify.** `minga-ctl stack ps <stack>` and `minga-ctl health --stack <stack>`. `deploy` waits 30s for placement by default (`--wait`); placed is not the same as healthy, so look at the tasks and their `ERROR` column.

**Compose files deploy unchanged.** Minga rejects unknown or unsupported keys with an error instead of skipping them, because a silently dropped key means the workload runs differently from what the file says. When validation rejects a key, do not delete or rewrite it to get past the error. Tell the user which key and service it is and what dropping it would change, and let them decide.

Relative bind mounts resolve on the machine that runs the deploy, against the compose file's directory (the current directory for a bare `-c compose.yaml`, from v1.1.1). The API refuses a relative source, so a client that does not resolve them gets an error, not a guess. `minga-ctl stack deploy` refuses them when the node is another machine; use an absolute path on the node, or deploy from the node. A task failing with `failed to fulfil mount request: ... no such file or directory` on v1.1.0 or older is usually a relative source the node resolved against its own working directory: redeploy with v1.1.1 or an absolute path.

## Day-2 with minga-ctl

IDs: a service is `<stack>.<service>`, a task is `<stack>.<service>.<n>`, a secret is `<stack>_<compose secret key>`.

```bash
minga-ctl context add prod --ssh user@node --remote-token-file /etc/minga/minga-api.env --use
minga-ctl stack ls
minga-ctl stack ps <stack>
minga-ctl service ls
minga-ctl service ps <stack>.<service>
minga-ctl service inspect <stack>.<service>
minga-ctl service logs -f <stack>.<service>
minga-ctl service scale --replicas 3 <stack>.<service>
minga-ctl service update --image <ref> <stack>.<service>
minga-ctl service rm <stack>.<service>
minga-ctl service rollback <stack>.<service>
minga-ctl stack deploy -c compose.yaml --prune <stack>   # also removes the stack's services the file lacks
minga-ctl secret put <stack>_<key> < value-file
minga-ctl registry login ghcr --username <user>     # credential on stdin
minga-ctl image pull <ref>
minga-ctl health --stack <stack>
```

`--remote-token-file` takes a bare token file or an env file with a `MINGA_API_TOKEN=` line (as in a systemd `EnvironmentFile`).

Global flags: `--context <name>`, `-o table|json|yaml` (`--json`), `-q`, `--debug` (traces with credentials redacted). Use `--json` when you need to parse output: the table cuts the `ERROR` column to 50 characters, JSON has the full text and the exit code.

Things that surprise people:

- `scale`, `update` and `rollback` return when the API accepts the spec. They do not wait for the rollout, so check `service ps` afterwards.
- `rollback` restores one previous revision, and only for changes made with `minga-ctl` on the same machine. A deploy from CI cannot be rolled back from a laptop.
- A service deleted from the Compose file keeps running after a plain `stack deploy`; it prints a note naming it. `--prune` removes it, and `service rm` removes one service. Both save the spec, and `service rollback` recreates it.
- `service inspect` also prints the last 20 lines of output of each task that is not running (`--tail`), or says why it could not.
- `stack rm` removes services only. Networks, configs and secrets go only when named with `--with <stack>_...`.
- Plain HTTP to a host other than loopback is refused, with no override. Use `--ssh`, or an `https://` endpoint with `--tls-cert`.
- Exit codes: 1 user or config error, 2 API / transport / auth / failed health check, 3 the named thing does not exist.

## Backup, restore and upgrades

- **Back up desired state** before anything that touches the data directory: `minga state export -o backup.json` (on the node, with `--api-url` and `MINGA_API_TOKEN` like `deploy`). The file holds services, networks, volumes and secret and config references, including environment values, so it is written 0600: treat it as a secret. It does not hold secret values (`--secret-store`), named volume data or TLS material; `docs/backup.md` says how to copy those.
- **Restore** into a fresh cluster with `minga state import backup.json`. It refuses when a service already exists unless `--overwrite`; ask before using that flag. Nodes and tasks are not in the file: nodes re-register and the scheduler places the tasks again.
- **Upgrading from 1.0.2 to 1.1.0** needs one last wipe. A 1.1.0 node on a 1.0.2 data directory stops with `written by minga 1.0.2 or older ... remove raft_log.redb and state.redb`. Remove only those two files in each role's `--data-dir`, on every node, and upgrade every node together. **Never delete the whole data directory**: with default paths it also holds secret values (`secrets/`), named volume data (`volumes/`) and agent modules (`modules/`). Ask before removing anything.
- **Upgrading to 1.2.0** keeps state, but it tightens behaviour that used to work. Read `docs/release-1.2.0.md` first and check each node for:
  - a Raft key (`--raft-tls-key`) readable by group or others: the node refuses to start until it is `chmod 600`. Data directories are tightened to 0700 and the redb files to 0600 at start.
  - a published port the node needs (53, the Raft, metrics and egress ports, the NATS sidecar's 4222/8222) or that a host process already listens on: that task fails to start.
  - tasks that call a service the node binds on `127.0.0.1`, or another stack's network directly: both are now blocked. Publish a port and use the node address.
  - a tokenless API reached by a DNS name, or from a browser: now 403. Set a token instead of working around it.
  - `minga-ctl stack deploy` through an ssh context without a token: now refused.
- Later upgrades keep state when they only add fields. Nodes of one cluster must still run the same version. Nodes with different format versions refuse each other's Raft messages, but two releases that share a format talk to each other, and the older one silently drops fields it does not know.

## Handling secrets and destructive commands

- Tokens and secret values never go in arguments or config files, since both end up in process listings and shell history. `secret put` and `registry login` read stdin. The `minga-ctl` config has no token field on purpose; use `--remote-token-file`, `--token-command` or `MINGA_CTL_TOKEN`.
- `service update --env-add` values are visible in process listings. Use Minga secrets for credentials.
- Ask the user before `stack rm`, `stack deploy --prune`, `service rm`, `service rollback`, `image rm`, `registry logout`, `state import --overwrite`, removing Raft files, or anything that restarts a role. Do not pass `--yes` unless they said to.
- The manager API supports bearer-token authentication and optional TLS/mTLS client identities with caller, role and tenant. Do not assume every operator token is tenant-scoped: cluster-operator credentials have administrative access. `minga-ctl` currently supports bearer-token contexts and trusts an HTTPS server CA; it does not present a client certificate/key.
- Raft without `--raft-tls-*` on a non-loopback bind lets anything that reaches the port rewrite cluster state, and Minga only logs a warning. Flag a multi-node cluster without Raft TLS.

## Troubleshooting

Start with `minga-ctl health --stack <stack>`, `minga-ctl stack ps <stack>` (its stderr hint names the services with tasks down), then `minga-ctl service inspect <stack>.<service>` for the last error, exit code and restarts of each task, and the task's logs (`minga-ctl service logs <stack>.<service>`, or `minga logs <task-id> --nats-url <url>`).

On the node itself, `minga-ctl` still needs a context: `minga-ctl context add local --use` (the endpoint defaults to `http://127.0.0.1:2375`) with the token in `MINGA_CTL_TOKEN`. Without `minga-ctl`, read the API directly: `GET /v1.41/nodes`, `/v1.41/services` and `/v1.41/tasks` with `Authorization: Bearer <token>`; a task's `Status.Err` and `Status.ContainerStatus.ExitCode` carry the reason. Paths need the version prefix; a bare `/nodes` is a 404.

| Symptom | Likely cause and what to check |
| --- | --- |
| Deploy returns 409, or succeeds with no tasks placed | No eligible node. Check nodes are `Ready` / `active` (`GET /v1.41/nodes`), placement constraints, and capacity. A node advertises the labels it was started with (`--label key=value` on `manager` / `worker`), so a `node.labels.*` constraint needs a matching node. |
| A service shows `0/1` with no obvious cause | `minga-ctl service inspect <stack>.<service>`. `exited with code N` is the process's own exit; `killed by signal 9` may be an OOM kill or an explicit kill (the runtime reports exit codes only, so this is inferred). A start error (image pull, user not in the image, secret, network) is recorded as text. No error and no logs usually means the task never started: check the node's journal. |
| Node exits at start: `written by minga 1.0.2 or older` | Data directory from before 1.1.0. See "Backup, restore and upgrades": remove only `raft_log.redb` and `state.redb`, after asking. |
| `/readyz` 503 `raft not ready` | No Raft leader. Check current manager peers and Raft connectivity; managers can be added or removed through the membership API. All nodes in a cluster must run a compatible version. |
| `/readyz` 503 `bus not connected` | Check `--nats-url` and the `MINGA_NATS_TLS_*` environment. |
| Image pull fails, or `401 Unauthorized` on pull | The daemon pulls (containerd's Transfer API), so containerd's network, proxy and CA trust apply, not the `minga` process's. Credentials come from `$DOCKER_CONFIG/config.json`, else `~/.docker/config.json`: `credHelpers`, then `credsStore`, then inline `auths`; a named helper must be on the manager's PATH. Credentialed pulls need containerd 1.7+, and a plain-HTTP registry cannot be reached on 1.7. Fix with `minga-ctl registry login`, or preload with `minga-ctl image pull` / `ctr -n minga images import`. |
| Agent (`x-minga-agent`) task fails | The node needs `--sandbox-runtime wasmtime` and the module staged in `--module-dir`. Fuel, epoch timeout and memory limits are enforced. Capability grants are validated, but WASI host imports are not linked yet; guest env/files/clock/stdio/HTTP are unavailable. |
| `cannot connect to containerd` | `--containerd-socket` takes a filesystem path, not a `unix://` URL. |
| Tasks show `running` but no container exists (`ctr -n minga containers ls` is empty) | The node runs the default `--runtime mock`, which is in-memory. Start `manager` / `worker` with `--runtime containerd`; `scripts/minga.service` already passes it. |
| `task already exists` after a state wipe | Stale containerd task: `ctr -n minga tasks delete --force <id>`. Ask first. |
| Containers on different nodes cannot reach each other | Check Linux WireGuard support, node endpoint and manager UDP endpoint/relay configuration, peer assignment and cluster-wide network subnet. See `docs/topologies.md`; the T4 scale target remains unmeasured. |
| `minga-ctl`: `rejected the credentials (401)` | Token source for the context is wrong. Check `remote_token_file` (a bare token, or an env file with `MINGA_API_TOKEN=`), `token_command`, `MINGA_CTL_TOKEN`. |
| Task fails: `published port N/tcp is in use on this node (...)` | The port is reserved by the node itself or a host process listens on it (the text in brackets says which). Publish a different port; do not stop the node service holding it. |
| Node exits: `private key ... has mode 644; it must not be readable by group or others (chmod 600)` | Raft key with loose permissions (1.2.0+). `chmod 600` it, after confirming the file is the node's own key. |
| API answers 403: `request carries an Origin header` or `Host is a DNS name` | The v1.2.0 API blocks browser-style requests without a token. Set `MINGA_API_TOKEN` on `minga manager`; there is no separate `minga api` role. |
| A task cannot reach a service on the node's `127.0.0.1`, or another stack's containers | Blocked by the host-network isolation rules. Publish the service's port and use the node address. |
| A workload fails writing `/proc/sys/...`, or reading `/proc/kcore` | `/proc/sys` is read-only and Docker's sensitive paths are masked inside containers (1.2.0+). |
| A stopped Minga task container is gone after a node restart | Expected from 1.2.0: at start the supervisor removes every container with a `minga/task_id` label that Raft does not assign to that node, running or exited, with its network and staged secrets, and adopts the ones Raft still assigns. The journal logs each removal. To keep a workload, deploy it through Minga. |
| `stack deploy over ssh needs an API token for this context` | Add a token to the context (`--remote-token-file`, `--token-command` or `MINGA_CTL_TOKEN`). |
| `minga-ctl stack deploy`: minga not found | It runs `minga` on the operator's machine, not the node. Install it there or set `MINGA_BIN`. |

## Limits to state up front

Confirm the version first. Current-main capabilities below are unreleased; the
tagged v1.2.0 does not include Phase 13 mesh, Phase 15 tenants or agent usage.

- T2/T3 multi-node WireGuard mesh and cross-node ingress are implemented and
  live-proofed. T4 scale (~5,000 workers) is not measured; multi-manager
  deployments need reachable UDP endpoints, and a relay manager can inspect
  relayed plaintext traffic.
- Phase 15 T1–T5 implementation is merged. The tenant B cannot place on tenant
  A's node proof is deferred until release readiness; do not run VM tests
  before then.
- Agent egress routes, policy, streaming and usage reporting are implemented.
  Per-workload budgets are node-local; distributed customer window budgets and
  request-rate limits remain planned.
- Wasmtime fuel, epoch timeout and memory limits are enforced. Capability
  grants are parsed and validated, but WASI host imports are not linked, so
  guest I/O and HTTP egress are unavailable. Firecracker remains a stub.
- No seccomp profile. Raft TLS without mTLS warns rather than refusing
  off-loopback binds.

## Where to read more

In a checkout of https://github.com/lithqube/minga these are local files; read the one that matches the task instead of guessing.

- `README.md`: release versus current-main status, quick start and crate map.
- `docs/cli.md`: `minga` / `minga-ctl` commands, flags, environment and API-only tenant operations.
- `docs/topologies.md`: single host, air-gapped, Talos and multi-node requirements/evidence.
- `docs/architecture.md`: current control/data plane, tenant boundaries and limits.
- `docs/agent-plane-roadmap.md`: completed egress/usage work and remaining agent features.
- `docs/sandbox-host-capabilities-plan.md`: enforced Wasmtime limits and pending WASI/HTTP/log support.
- `.planning/STATE.md` and `.planning/ROADMAP.md`: current phase status and release gates.
- `docs/release-1.2.0.md`: tagged release security changes and upgrade checks.
- `docs/backup.md`: export/import scope and what else to copy.
- `docs/minga-ctl-plan.md`: companion CLI design and remaining gaps.
- `docs/talos-deployment.md`, `docs/hetzner-arm-staging.md`: deployment-specific evidence and limits.
- `docs/adr/`: settled design decisions and implementation status.

For NATS / JetStream internals (streams, consumers, retention), use a JetStream skill if one is installed; this skill only covers how Minga uses the bus.
