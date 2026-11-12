---
name: minga
description: Operate a Minga cluster (the single-binary Rust orchestrator that replaces Docker Swarm on edge, on-prem and air-gapped sites) with the `minga` and `minga-ctl` CLIs. Use this whenever the user wants to deploy or validate a Compose file on Minga, run or inspect stacks, services, tasks, logs, secrets, images or registry logins, set up a `minga-ctl` context, start manager / worker / api roles, or debug a Minga node - a task that is not placed, a 409 on deploy, `/readyz` returning 503, an image pull that fails, a rejected Compose key - even if they only say "deploy this to the edge site" or "why is my stack not coming up" in a project that uses Minga.
---

# Operating Minga

Minga runs Compose files on one binary: Raft holds desired state and placement, NATS/JetStream carries status, logs and liveness, containerd runs the containers. This skill is for operating it. For changing Minga's own code, follow the repository's `CLAUDE.md` instead.

Flags change between releases. Before running a command you have not run in this session, check `minga <cmd> --help` or `minga-ctl <cmd> -h` rather than trusting this file.

## Two CLIs

| Tool | What it is | Use it for |
| --- | --- | --- |
| `minga` | The orchestrator itself. Roles (`manager`, `worker`, `api`) plus client commands. | Running nodes, `validate`, `deploy`, `logs`, `image pull`, `tls`, node-local `secret`. |
| `minga-ctl` | Companion CLI on the operator's machine. Talks to `minga api`, usually through an SSH tunnel. | Contexts, stacks, services, scale / update / rollback, health, metrics, secrets and registry logins across nodes. |

`minga-ctl` is not in the release archive: `cargo install --path crates/minga-ctl` from a checkout. It calls the local `minga` binary for `stack deploy` and `service logs`, so `minga` must be on PATH (or set `MINGA_BIN`).

## Deploying a Compose file

1. **Validate offline.** `minga validate -c compose.yaml` (add `-p <profile>` as needed). It prints the services in startup order.
2. **Check the node is ready.** `minga-ctl health`, or `curl <metrics-bind>/readyz` on the node. A 503 body says why.
3. **Deploy.**
   - With a context: `minga-ctl stack deploy -c compose.yaml <stack>`. Prefer this: it saves each changed service's previous spec so `service rollback` works.
   - Directly: `MINGA_API_URL=... MINGA_API_TOKEN=... minga deploy -c compose.yaml <stack>`.
4. **Verify.** `minga-ctl stack ps <stack>` and `minga-ctl health --stack <stack>`. `deploy` waits 30s for placement by default (`--wait`); placed is not the same as healthy, so look at the tasks.

**Compose files deploy unchanged.** Minga rejects unknown or unsupported keys with an error instead of skipping them, because a silently dropped key means the workload runs differently from what the file says. When validation rejects a key, do not delete or rewrite it to get past the error. Tell the user which key and service it is and what dropping it would change, and let them decide.

Relative bind mounts resolve on the machine that runs the deploy. `minga-ctl stack deploy` refuses them when the node is another machine; use an absolute path on the node, or deploy from the node.

## Day-2 with minga-ctl

IDs: a service is `<stack>.<service>`, a task is `<stack>.<service>.<n>`, a secret is `<stack>_<compose secret key>`.

```bash
minga-ctl context add prod --ssh user@node --remote-token-file /etc/minga/api-token --use
minga-ctl stack ls
minga-ctl stack ps <stack>
minga-ctl service ls
minga-ctl service logs -f <stack>.<service>
minga-ctl service scale --replicas 3 <stack>.<service>
minga-ctl service update --image <ref> <stack>.<service>
minga-ctl service rollback <stack>.<service>
minga-ctl secret put <stack>_<key> < value-file
minga-ctl registry login ghcr --username <user>     # credential on stdin
minga-ctl image pull <ref>
minga-ctl health --stack <stack>
```

Global flags: `--context <name>`, `-o table|json|yaml` (`--json`), `-q`, `--debug` (traces with credentials redacted). Use `--json` when you need to parse output.

Things that surprise people:

- `scale`, `update` and `rollback` return when the API accepts the spec. They do not wait for the rollout, so check `service ps` afterwards.
- `rollback` restores one previous revision, and only for changes made with `minga-ctl` on the same machine. A deploy from CI cannot be rolled back from a laptop.
- `stack rm` removes services only. Networks, configs and secrets go only when named with `--with <stack>_...`.
- Plain HTTP to a host other than loopback is refused, with no override. Use `--ssh`, or an `https://` endpoint with `--tls-cert`.
- Exit codes: 1 user or config error, 2 API / transport / auth / failed health check, 3 the named thing does not exist.

## Handling secrets and destructive commands

- Tokens and secret values never go in arguments or config files, since both end up in process listings and shell history. `secret put` and `registry login` read stdin. The `minga-ctl` config has no token field on purpose; use `--remote-token-file`, `--token-command` or `MINGA_CTL_TOKEN`.
- `service update --env-add` values are visible in process listings. Use Minga secrets for credentials.
- Ask the user before `stack rm`, `service rollback`, `image rm`, `registry logout`, or anything that restarts a role. Do not pass `--yes` unless they said to.
- Without `MINGA_API_TOKEN` on `minga api` the API is unauthenticated, and anything that reaches it can deploy privileged containers. Flag that if you see it.

## Troubleshooting

Start with `minga-ctl health --stack <stack>`, `minga-ctl stack ps <stack>`, and the logs of the task (`minga-ctl service logs <stack>.<service>`, or `minga logs <task-id> --nats-url <url>`).

On the node itself, `minga-ctl` still needs a context: `minga-ctl context add local --use` (the endpoint defaults to `http://127.0.0.1:2375`) with the token in `MINGA_CTL_TOKEN`. Without `minga-ctl`, read the API directly: `GET /v1.41/nodes`, `/v1.41/services` and `/v1.41/tasks` with `Authorization: Bearer <token>`. Paths need the version prefix; a bare `/nodes` is a 404.

| Symptom | Likely cause and what to check |
| --- | --- |
| Deploy returns 409, or succeeds with no tasks placed | No eligible node. Check nodes are `Ready` / `active` (`GET /v1.41/nodes`), placement constraints, and capacity. A node advertises the labels it was started with (`--label key=value` on `manager` / `worker`), so a `node.labels.*` constraint needs a matching node. |
| `/readyz` 503 `raft not ready` | No Raft leader. Every `--peer` must be up; membership is fixed at bootstrap. |
| `/readyz` 503 `bus not connected` | Check `--nats-url` and the `MINGA_NATS_TLS_*` environment. |
| `ctr pull <ref> failed`, or `401 Unauthorized` on pull | Registry unreachable or no credential. `minga-ctl registry login`, or preload with `minga-ctl image pull` / `ctr -n minga images import`. Only inline `auth` entries in the Docker credential file are read; credential helpers are not. |
| Agent (`x-minga-agent`) task never runs | The node was not started with `--sandbox-runtime wasmtime`, or the `.wasm` module is not staged in `--module-dir`. |
| `cannot connect to containerd` | `--containerd-socket` takes a filesystem path, not a `unix://` URL. |
| `task already exists` after a state wipe | Stale containerd task: `ctr -n minga tasks delete --force <id>`. Ask first. |
| Containers on different nodes cannot reach each other | Expected today: there is no cross-node mesh yet. |
| `minga-ctl`: `rejected the credentials (401)` | Token source for the context is wrong. Check `remote_token_file`, `token_command`, `MINGA_CTL_TOKEN`. |

## Limits to state up front

Say these before promising a design that depends on them, and confirm against the current README, since they are release-specific (written for v1.0.2):

- Single host is the verified path. Multi-node schedules across nodes but has no cross-node container networking.
- A node cannot join a running cluster. Every node is listed with `--peer` and started together.
- In multi-node, every manager and worker must point at one external NATS (`tls://`); a manager without `--nats-url` starts its own sidecar and splits the bus.

## Where to read more

In a checkout of https://github.com/lithqube/minga these are local files; read the one that matches the task instead of guessing.

- `README.md`: status, quick start, role flags, topologies, air-gapped install.
- `docs/minga-ctl-plan.md`: every `minga-ctl` command with examples, error texts, known limits (sections 4, 5 and 7).
- `docs/talos-deployment.md`: Talos install and its troubleshooting table.
- `docs/hetzner-arm-staging.md`: a real 8-service stack with Traefik and private images, plus a pre-deploy checklist.
- `docs/architecture.md` and `docs/adr/`: why Raft holds only desired state and JetStream the rest.

For NATS / JetStream internals (streams, consumers, retention), use a JetStream skill if one is installed; this skill only covers how Minga uses the bus.
