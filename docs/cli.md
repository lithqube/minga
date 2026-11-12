# CLI reference

Minga has two command-line tools. This page says what each command is for,
where it runs and which flags matter. For the complete flag list of the version
you run, use `minga <command> --help` and `minga-ctl <command> --help`; this
page was written from the `--help` output of v1.2.0.

| Tool | Runs on | What it does | Ships in |
| --- | --- | --- | --- |
| `minga` | Every node, for the `manager`, `worker` and `api` roles; any machine for the client commands | Runs the cluster, and deploys, validates, backs up state, reads logs, pulls images and issues certificates | The release archive and the `ghcr.io/lithqube/minga` image |
| `minga-ctl` | The operator's workstation | Named contexts, stacks, services, secrets, images, registry logins and health checks, over SSH or HTTPS | Not in the archive: `cargo install --locked --path crates/minga-ctl` from a checkout of the release tag |

Supported layouts (single host, multi-node, Talos, air-gapped) are in
[topologies.md](topologies.md).

## Which deploy command to use

Both commands deploy a Compose file through `minga api`. `minga-ctl stack
deploy` runs `minga deploy` on the workstation, through its tunnel, and adds
the day-2 parts:

| | `minga deploy` | `minga-ctl stack deploy` |
| --- | --- | --- |
| Where the Compose file is read and translated | Where it runs | On the workstation: `minga` must be installed there (`MINGA_BIN`, else `minga` on `PATH`) |
| How it reaches the API | `--api-url` or `MINGA_API_URL`, default `http://127.0.0.1:2375` | The context: an SSH tunnel or an `https://` endpoint |
| API token | `MINGA_API_TOKEN` | The context's token source ([below](#contexts)); an SSH context must have one from 1.2.0 |
| Saves each changed service's previous spec for `service rollback` | No | Yes |
| Removes the stack's services that the file no longer has | No | With `--prune` (asks first unless `--yes`) |
| Relative bind-mount sources | Resolved against the Compose file's directory on the machine that runs it | Refused when the node is another machine |
| Profiles and placement wait | `-p`, `--wait` | Same, passed through |

Use `minga-ctl stack deploy` from a workstation, and `minga deploy` on the node
itself, in CI or in scripts. Without `--prune`, a service removed from the
Compose file keeps running with either command.

## `minga`

### Node roles

```bash
minga manager --node-id <ID> --bind <ADDR> [flags]   # scheduler + supervisor (+ NATS sidecar)
minga worker  --node-id <ID> --bind <ADDR> [flags]   # supervisor only
minga api     --node-id <ID> --bind <ADDR> [flags]   # Docker Engine API (v1.41 subset)
```

Every role is a Raft voter with its own `--node-id` and `--data-dir`, including
`minga api`. `manager` and `worker` take the same flags; `api` takes only the
Raft flags plus `--api-bind` and `--api-token`.

**Raft flags (all roles)**

| Flag | Default | Notes |
| --- | --- | --- |
| `--node-id <ID>` | required | Unique per process. |
| `--bind <ADDR>` | required | Address the Raft RPC server listens on and peers dial, for example `127.0.0.1:7001`. |
| `--peer <ID=ADDR>` | none | Repeat for every other voter. Membership is fixed at first start: list all of them, and start them together. |
| `--data-dir <DIR>` | `/var/lib/minga` | Holds `raft_log.redb` and `state.redb`, and with default paths also `secrets/`, `volumes/` and `modules/`. Give `minga api` its own directory on a shared host. Tightened to 0700 at start. |
| `--raft-tls-ca`, `--raft-tls-cert`, `--raft-tls-key` | none | All three together enable mTLS. Required off loopback (without them a node only logs a warning). The key must not be readable by group or others. |

**Manager and worker flags**

| Flag | Default | Notes |
| --- | --- | --- |
| `--runtime <mock\|containerd>` | `mock` | **Set `containerd` for real containers.** `mock` is in-memory and meant for tests. `scripts/minga.service` passes `containerd`. |
| `--containerd-socket <PATH>` | `/run/containerd/containerd.sock` | A filesystem path, not a `unix://` URL. |
| `--nats-url <URL>`, `MINGA_NATS_URL` | none | Unset on a manager: it starts a `nats-server` sidecar on 127.0.0.1:4222 (monitor on 127.0.0.1:8222). Workers must set it. With more than one node, every manager and worker points at the same external `tls://` server or cluster. |
| `--nats-token`, `MINGA_NATS_TOKEN` | none | Token for the sidecar or a plain NATS server. Ignored over TLS, where the connection is certificate-only. |
| `--nats-replicas <N>` | `1` | Copies of the command, event, dead-letter and KV streams. Set 3 or 5 on a NATS cluster of at least that many servers. Needs `--nats-url`. |
| `--label <KEY=VALUE>` | none | Advertised to the cluster; matched by `node.labels.KEY` placement constraints. |
| `--metrics-bind <ADDR>`, `MINGA_METRICS_BIND` | not served | `/metrics`, `/healthz` and `/readyz`. |
| `--secret-store <DIR>` | `/var/lib/minga/secrets` | Secret values on this node. They never go through Raft or the bus. |
| `--interval <SECS>` | `2` | Reconcile interval. |
| `--sandbox-runtime <mock\|wasmtime>`, `--module-dir <DIR>` | none, `/var/lib/minga/modules` | `wasmtime` enables `x-minga-agent` tasks on this node. |
| `--egress-bind`, `--egress-upstream`, `--egress-credential`, `--egress-credential-header` | none, `https://api.openai.com`, none, `authorization` | Starts the LLM egress proxy for agent tasks. The credential is a secret ID in `--secret-store`; agents never see the key. |

**API flags**

| Flag | Default | Notes |
| --- | --- | --- |
| `--api-bind <ADDR>` | `0.0.0.0:2375` | Every interface by default. Bind `127.0.0.1:2375` unless containers (Traefik) must reach it, and then set a token. |
| `--api-token`, `MINGA_API_TOKEN` | none | Required as `Authorization: Bearer <token>`, or as the Basic-auth password for Traefik's Swarm provider. Set it through the environment (an `EnvironmentFile`), not the flag, so it stays out of process listings. Without a token the API is unauthenticated; from 1.2.0 it also refuses requests with an `Origin` header or a `Host` that is a DNS name other than `localhost` or `*.localhost`, and warns at start when bound off loopback. |

### Client commands

| Command | What it does | Where to run it |
| --- | --- | --- |
| `minga validate [-c FILE] [-p PROFILE]` | Parses the Compose file offline, rejects unknown keys and prints the startup order. `-p '*'` enables every profile. | Anywhere |
| `minga deploy [-c FILE] [-p PROFILE] [--wait SECS] STACK` | Deploys the file as a stack and waits for placement (30 s by default). | Anywhere that reaches the API |
| `minga state export [-o FILE] [--force]` | Writes desired state (services, networks, volumes, secret and config references) to stdout or a 0600 file. Not secret values, volume data or TLS material; see [backup.md](backup.md). | Anywhere that reaches the API |
| `minga state import FILE [--overwrite]` | Restores an export. Fails on an existing service unless `--overwrite`. | Anywhere that reaches the API |
| `minga logs [-f] [--tail N] TASK_ID` | Prints a task's output from JetStream (`--nats-url`); `TASK_ID` is `<stack>.<service>.<n>`. | Anywhere that reaches NATS |
| `minga image pull IMAGE` | Pulls into the `minga` containerd namespace. Credentials come from `$DOCKER_CONFIG/config.json`, else `~/.docker/config.json`, and go to containerd over its API. | On the node |
| `minga secret put ID < value` | Writes or rotates a secret value in the node's store, read from stdin. `ID` is `<stack>_<compose secret key>`. | On every node that runs the service |
| `minga tls init-ca --dir DIR` | Creates the cluster CA (`ca.pem`, `ca-key.pem`). | Anywhere; keep `ca-key.pem` off the nodes |
| `minga tls issue-node --ca-dir DIR --out-dir DIR` | Issues a node certificate (`node.pem`, `node-key.pem`, SAN `minga-node`). Keys are written 0600. | Anywhere; copy one pair to each node |
| `minga agent ls` | Lists running agent tasks from the bus. | Anywhere that reaches NATS |
| `minga sandbox run --module FILE [--input DATA] [--fuel N] [--timeout SECS]`, `minga sandbox info` | Runs a WASM module once; lists the sandbox runtimes. | Anywhere |
| `minga egress [--bind ADDR] ...` | A standalone egress proxy. `--bind` defaults to `0.0.0.0:8080`. | Anywhere |

`deploy` and `state` take `--api-url` (or `MINGA_API_URL`, default
`http://127.0.0.1:2375`) and read the token from `MINGA_API_TOKEN`.

### Planning utilities

`minga net create`, `net alloc-subnet` and `net inspect`, and `minga wg
keygen`, `wg pubkey` and `wg config`, compute and print subnets, WireGuard keys
and a sample interface config. **They change nothing**: they do not create a
network, configure an interface or write to Raft. Compose networks are created
by `minga deploy`, and there is no cross-node mesh yet.

## `minga-ctl`

### Contexts

A context says how to reach one cluster. The config lives at
`$XDG_CONFIG_HOME/minga-ctl/config.yaml`, else
`~/.config/minga-ctl/config.yaml`. It holds context definitions only, never a
token, and `minga-ctl` refuses it if group or others can read it (`chmod 600`).

```bash
minga-ctl context add prod --ssh admin@node1 \
  --remote-token-file /etc/minga/minga-api.env \
  --node admin@node1 --node admin@node2 --use
minga-ctl context list
minga-ctl context use prod
minga-ctl context delete old
```

| `context add` flag | Notes |
| --- | --- |
| `--ssh <TARGET>` | SSH target for the API tunnel and remote commands. |
| `--endpoint <URL>` | API URL, as seen from the SSH target with `--ssh`. Default `http://127.0.0.1:2375`. Plain HTTP to a host other than loopback is refused; use `--ssh`, or `https://` with `--tls-cert`. |
| `--remote-token-file <PATH>` | File on the SSH target with the token: the bare token, or an env file with a `MINGA_API_TOKEN=` line (such as the API's `EnvironmentFile`). |
| `--token-command <CMD>` | Local command that prints the token, run without a shell. |
| `--tls-cert <PEM>` | CA certificate to trust for an `https://` endpoint. |
| `--node <TARGET>` | SSH target for node-local commands (secrets, images, registry logins). Repeat for each node. |
| `--nats-url <URL>` | NATS URL for logs, as seen from where `minga logs` runs. |
| `--metrics-endpoint <URL>` | Metrics URL, as seen from the SSH target. |
| `--registry-auth-file <PATH>` | Docker credential file on the node that Minga reads when pulling. |
| `--use` | Make it the current context. |

How a command picks its settings:

- **Context:** `--context`, then `MINGA_CTL_CONTEXT`, then the current context, then `default`.
- **Endpoint:** `MINGA_CTL_ENDPOINT`, then the context's, then `http://127.0.0.1:2375`.
- **Token:** `MINGA_CTL_TOKEN`, then the context's `--token-command`, then its `--remote-token-file`.

### Commands

IDs: a service is `<stack>.<service>`, a task `<stack>.<service>.<n>`, a secret
`<stack>_<compose secret key>`.

Through the API:

| Command | Notes |
| --- | --- |
| `stack deploy [-c FILE] [-p PROFILE] [--wait SECS] [--prune [--yes]] STACK` | See [which deploy command to use](#which-deploy-command-to-use). |
| `stack ls`, `stack ps STACK` | Stacks are services grouped by their stack label. |
| `stack rm STACK [--with ID]... [--yes]` | Removes the services only. A network, config or secret goes only when named with `--with`; it must start with `<stack>_`, and no service outside the stack may use it. |
| `service ls [--stack STACK]`, `service ps SERVICE` | Running and desired replicas; tasks with an `ERROR` column. |
| `service inspect SERVICE [--tail N]` | Image, replicas, ports, networks, update policy, healthcheck, and each task's state, restarts, exit code and last error, plus the last lines of output of tasks that are not running. |
| `service logs SERVICE [-f] [--tail N]` | The output of the service's running tasks, through `minga logs` and the context's `--nats-url`. |
| `service scale --replicas N SERVICE` | Returns when the API accepts the spec, not when the rollout ends. |
| `service update SERVICE --image REF \| --env-add KEY=VALUE \| --env-rm KEY \| --replicas N` | Same. `--env-add` values show in process listings; use secrets for credentials. |
| `service rm SERVICE [--yes]` | Saves the spec first, so `service rollback` can bring it back. |
| `service rollback SERVICE [--yes]` | Restores the spec saved before `minga-ctl` last changed or removed the service. One revision, saved on this machine: a deploy from CI cannot be rolled back from a laptop. |
| `secret list` | Secret metadata; values never leave the nodes. |
| `health [--stack STACK] [--timeout SECS]` | The API, and with `--stack` the stack's services and their health URLs. |
| `metrics` | The Prometheus metrics the cluster reports. |

On the nodes, over SSH (`--ssh TARGET`, repeatable; by default the context's
`--node` targets, else its `--ssh`, else this machine):

| Command | Notes |
| --- | --- |
| `secret put ID [--ssh TARGET]... [--secret-store DIR] < value` | Writes the value on each node and declares the secret in the API. The value is read from stdin. |
| `image pull IMAGE`, `image ls`, `image rm IMAGE [--yes]` | The `minga` containerd namespace on each node. |
| `registry login REGISTRY [--username USER] [--key-file FILE]` | Stores a pull credential on each node; the password is read from stdin. `REGISTRY` is a short name (`dockerhub`, `ghcr`) or a host. |
| `registry logout REGISTRY [--yes]`, `registry list` | `list` shows which registries each node has a credential for, never the secret. |

Global flags: `--context NAME`, `-o table|json|yaml` (`--json`), `-q` (IDs
only; changes print nothing), `--debug` (request and ssh traces on stderr, with
credentials redacted). The table cuts the `ERROR` column; `--json` has the full
text.

Exit codes: `0` success, `1` usage or configuration error, `2` API, transport,
SSH or authentication failure or a failed health check, `3` the named service,
stack or context does not exist.

## Environment variables

| Variable | Read by | Meaning |
| --- | --- | --- |
| `MINGA_API_URL` | `minga deploy`, `minga state` | API URL (default `http://127.0.0.1:2375`). |
| `MINGA_API_TOKEN` | `minga api`; `minga deploy`, `minga state` | The API token, server and client side. |
| `MINGA_NATS_URL` | Node roles, `minga logs`, `minga agent ls` | NATS URL. |
| `MINGA_NATS_TOKEN` | Node roles | NATS token (not used over TLS). |
| `MINGA_NATS_TLS_CA`, `MINGA_NATS_TLS_CERT`, `MINGA_NATS_TLS_KEY`, `MINGA_NATS_TLS_SERVER_NAME` | Everything that connects to NATS | Client certificate settings for a `tls://` URL. |
| `MINGA_NATS_REPLICAS` | Node roles | Same as `--nats-replicas`. |
| `MINGA_NATS_SERVER_BIN` | `minga manager` | The `nats-server` the sidecar runs, instead of the one next to `minga` or on `PATH`. |
| `MINGA_METRICS_BIND` | Node roles | Same as `--metrics-bind`. |
| `MINGA_LOG_LEVEL` | All `minga` processes | Log filter, such as `info` or `minga_node=debug` (default `info`). |
| `DOCKER_CONFIG` | Image pulls | Directory of the Docker `config.json` with registry credentials. |
| `MINGA_CTL_CONTEXT`, `MINGA_CTL_ENDPOINT`, `MINGA_CTL_TOKEN` | `minga-ctl` | Override the context, its endpoint and its token. |
| `MINGA_BIN` | `minga-ctl stack deploy` | The `minga` binary to run locally. |
| `XDG_CONFIG_HOME` | `minga-ctl` | Where the config file lives. |

### systemd

`scripts/minga.service` runs the `manager` or `worker` role from
`/etc/minga/minga.env` ([template](../scripts/minga.env.example)): `MINGA_ROLE`,
`MINGA_NODE_ID`, `MINGA_BIND` and `MINGA_EXTRA_ARGS` for everything else. The
unit already passes `--data-dir /var/lib/minga`, `--runtime containerd` and
`--containerd-socket`, so do not repeat them. `minga api` needs a second unit
with its own node ID and data directory; the
[staging guide](hetzner-arm-staging.md#single-node-configuration) has one.
