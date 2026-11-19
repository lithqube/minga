# CLI reference

Minga has two command-line tools. This page documents the current manager-only
Raft CLI shape and describes what each command does, where it runs and which
flags matter. For the exact flags in your build, run `minga <command> --help`
and `minga-ctl <command> --help`.

| Tool | Runs on | What it does | Ships in |
| --- | --- | --- | --- |
| `minga` | Every node for the `manager` and `worker` roles; any machine for client commands | Runs the cluster, serves the manager's Docker API, and deploys, validates, backs up state, reads logs, pulls images and issues certificates | The release archive and the `ghcr.io/lithqube/minga` image |
| `minga-ctl` | The operator's workstation | Named contexts, stacks, services, secrets, images, registry logins and health checks, over SSH or HTTPS | Not in the archive: `cargo install --locked --path crates/minga-ctl` from a checkout of the release tag |

Supported layouts (single host, multi-node, Talos, air-gapped) are in
[topologies.md](topologies.md).

## Which deploy command to use

Both commands deploy a Compose file through the manager's Docker API. `minga-ctl
stack deploy` runs `minga deploy` on the workstation, through its tunnel, and
adds the day-2 parts:

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
minga manager --node-id <ID> --bind <ADDR> [--api-bind <ADDR>] [flags] # scheduler + API + supervisor (+ NATS sidecar)
minga worker  --node-id <ID> --nats-url <URL> [flags]                # supervisor only
```

`minga api` is retired and exits with guidance to use `minga manager --api-bind`.
Only managers vote in Raft and serve the Docker Engine API. Workers are
JetStream clients and do not open a `StateStore`. A new manager uses `--join`
to open without bootstrapping and wait for an administrator to add it through
the authenticated API.

With the manager API token configured, add a running joiner with
`POST /minga/managers` and `{ "id": ID, "raft_addr": "HOST:PORT" }`.
The API waits for catch-up before promotion. Remove it with
`DELETE /minga/managers/ID`; the last manager cannot be removed.

**Raft flags (managers only)**

| Flag | Default | Notes |
| --- | --- | --- |
| `--node-id <ID>` | required | Unique, stable ID for this Raft node. |
| `--bind <ADDR>` | required | Address the Raft RPC server listens on and peers dial, for example `127.0.0.1:7001`. |
| `--peer <ID=ADDR>` | none | Initial cluster membership only. Repeat for the other voters when bootstrapping; `--join` cannot be combined with `--peer`. |
| `--join` | off | Never initializes a pristine store; waits for a manager to add this node. |
| `--data-dir <DIR>` | `/var/lib/minga` | Manager Raft data (`raft_log.redb`, `state.redb`), plus node-local `secrets/`, `volumes/` and `modules/`. Workers never open Raft files. Tightened to 0700 at start. |
| `--raft-tls-ca`, `--raft-tls-cert`, `--raft-tls-key` | none | All three together enable Raft mTLS. Required off loopback (otherwise a warning). The key must not be readable by group or others. |

**Manager and worker flags**

| Flag | Default | Notes |
| --- | --- | --- |
| `--runtime <mock\|containerd>` | `mock` | **Set `containerd` for real containers.** `mock` is in-memory and meant for tests. `scripts/minga.service` passes `containerd`. |
| `--containerd-socket <PATH>` | `/run/containerd/containerd.sock` | A filesystem path, not a `unix://` URL. |
| `--nats-url <URL>`, `MINGA_NATS_URL` | none | Unset on a manager: it starts the NATS sidecar. Workers must set it; use a comma-separated TLS seed list for clustered managers. |
| `--nats-token`, `MINGA_NATS_TOKEN` | none | Development-only plaintext-loopback token authentication. Mutually exclusive with NATS JWT credentials. |
| `MINGA_NATS_CREDENTIALS_FILE` | none | Owner-only NATS user `.creds` file for operator/JWT authentication; requires mTLS. |
| `--nats-replicas <N>` | `1` | Copies of the event, status, lease, and assignment streams/buckets. Set 3 or 5 on a cluster with at least that many servers. Works with external NATS or route-TLS sidecars. |

| `--nats-cluster-route <URL>` | none | Repeat for existing sidecar routes, e.g. `nats://manager-1:6222`. Requires route TLS. |
| `--nats-cluster-cert`, `--nats-cluster-key`, `--nats-cluster-ca` | none | Route certificate, key, and peer CA for the built-in sidecar; required together and separate from client-facing TLS. `--nats-cluster-port` defaults to `6222`. |
| `--label <KEY=VALUE>` | none | Advertised to the cluster; matched by `node.labels.KEY` placement constraints. |
| `--overlay-supernet <CIDR>`, `MINGA_OVERLAY_SUPERNET` | `10.100.0.0/16` | Manager subnet-allocation range; must contain `/24`s and avoid host routes. Use the same range on every cluster node. |
| `--wg-listen <PORT>` | `51820` | UDP listen port for this node's WireGuard interface. |
| `--wg-endpoint <HOST:PORT>` | none | Advertises a reachable endpoint; omit for a node behind NAT. |
| `--wg-mtu <BYTES>` | `1420` | Interface MTU when this node has WireGuard peers. Single-host nodes keep their existing MTU. |
| `--metrics-bind <ADDR>`, `MINGA_METRICS_BIND` | not served | `/metrics`, `/healthz` and `/readyz`. |
| `--secret-store <DIR>` | `/var/lib/minga/secrets` | Secret values on this node. They never go through Raft or the bus. |
| `--interval <SECS>` | `2` | Reconcile interval. |
| `--sandbox-runtime <mock\|wasmtime>`, `--module-dir <DIR>` | none, `/var/lib/minga/modules` | `wasmtime` enables `x-minga-agent` tasks on this node. |
| `--agent-dir-root <DIR>` (repeatable) | none | Canonicalized allowlist for agent directory grants. A grant must resolve under `<DIR>/<tenant-id>/` for the node's `--tenant-id`, so one tenant cannot name another's directories; a symlinked tenant directory is refused. No configured root permits no directory grants. Grants are validated only: the sandbox mounts no directories and links no WASI yet. |
| `--egress-bind`, `--egress-upstream`, `--egress-credential`, `--egress-credential-header` | none, `https://api.openai.com`, none, `authorization` | Opt-in. Starts the LLM egress proxy for agent and routed container tasks (for example `0.0.0.0:8080`); tasks reach it on their bridge gateway at the bind's port. The port must be fixed and nonzero. On Linux a non-loopback bind installs the `minga_egress_listener` nft table after the socket is bound and before it serves, which closes that port to everything but `lo` and task bridges (not the WireGuard mesh); the table is removed on shutdown, on a failed start, and at start-up without such a bind. Only a wildcard bind (`0.0.0.0` or `[::]`) reaches task bridge gateways, so only then does the node carry the reserved `com.minga.egress.proxy=true` label, and services with egress routes are placed only on such nodes. A loopback bind serves host-local agents through their `egress_url` only. The credential is a secret ID in `--secret-store`; agents never see the key. |
| `--egress-allow-cidr <CIDR>` (repeatable) | none | Tenant egress routes refuse loopback, link-local (`169.254.0.0/16`, `fe80::/10`), shared (`100.64.0.0/10`), `0.0.0.0/8`, unspecified, multicast, broadcast and private or unique-local destinations, checked on literal addresses and on every DNS answer at connect time, including IPv4 reached through NAT64 (`64:ff9b::/96`). Each flag allows one such range, for example an on-prem model gateway at `10.20.0.0/24`. The operator's `--egress-upstream` is not filtered. |

The built-in NATS sidecar caps JetStream file storage at 8GB (not preallocated). The MINGA account's JetStream disk limit must not exceed that cap. JetStream shares the data volume with Raft and containerd, so on nodes with less than about 8GB free, the volume fills before the cap applies.

Each node generates `<data_dir>/wg.key` once with mode `0600`; the private key
stays local. Registration advertises only its public key and optional endpoint.
Delete `wg.key` and restart the node to rotate its key; its next lease updates
the registered public key and endpoint.

**Joining a worker**

Issue a worker identity certificate with `minga tls issue-node --ca-dir DIR
--out-dir DIR --node-id ID --role worker`. Manager certificates issued with
`--role manager` also carry the `localhost` SAN for the built-in sidecar's
local TLS client and `minga-manager-ID` for remote clients. Set
`MINGA_NATS_TLS_SERVER_NAME=localhost` on a manager using its built-in sidecar;
on workers, use the manager hostname from the URL (for example,
`minga-manager-1`) so it matches the manager certificate SAN. Set
`MINGA_NATS_TLS_CA`, `MINGA_NATS_TLS_CERT` and `MINGA_NATS_TLS_KEY`, then run:

If NATS user JWT authentication is enabled, also set
`MINGA_NATS_CREDENTIALS_FILE` to an owner-only `.creds` file. JWT credentials
require TLS and cannot be combined with `--nats-token` or `MINGA_NATS_TOKEN`.
Every manager in operator/JWT mode needs owner-only operator JWT, account JWT
and account seed files, a tenant signer directory and a full-resolver directory
via `MINGA_NATS_OPERATOR_JWT`, `MINGA_NATS_ACCOUNT_JWT_FILE`,
`MINGA_NATS_ACCOUNT_SEED_FILE`, `MINGA_NATS_TENANT_SIGNER_DIR` and
`MINGA_NATS_RESOLVER_DIR`. Pass `--nats-sys-credentials-file` with SYS-user
credentials; manager connections use a Minga-account `.creds` file through
`MINGA_NATS_CREDENTIALS_FILE`. The API join-token endpoint issues the
tenant-and-node-scoped worker credentials.

Create the key once with `minga tls init-join-key --out FILE`, then copy it
securely to every manager and set `MINGA_JOIN_TOKEN_KEY_FILE` to that
owner-only 32-byte key.

An operator pre-registers the node with `POST /v1.41/nodes`, then calls
`POST /minga/nodes/ID/join-token` with cluster-operator authentication. Store
the returned token in an owner-only file and set `MINGA_NODE_JOIN_TOKEN_FILE`
on the worker. Tokens expire after 15 minutes; the manager records the tenant
binding after the first valid lease.

The worker's NATS identity must be restricted to its own lease key
(`lease.node.ID`) by certificate mapping or JWT permissions. The join token
does not replace NATS subject authorization.

```bash
minga worker --node-id ID --nats-url tls://minga-manager-1:4222,tls://minga-manager-2:4222
```

The node ID must match the certificate identity. No Raft bind, peer list, Raft
certificate flags, or previous Raft data directory is needed.

**API flags (manager only)**

| Flag | Default | Notes |
| --- | --- | --- |
| `--api-bind <ADDR>` | `0.0.0.0:2375` | The manager's Docker API listener. Bind `127.0.0.1:2375` unless containers (Traefik) must reach it; when reachable beyond loopback, configure a token and firewall the port. |
| `--api-token`, `MINGA_API_TOKEN` | none | Bearer token on API requests, or Basic-auth password for Traefik's Swarm provider. Prefer the environment (`EnvironmentFile`) over the flag so the token is not in process listings. Manager membership endpoints always require a configured token. |

### Client commands

| Command | What it does | Where to run it |
| --- | --- | --- |
| `minga validate [-c FILE] [-p PROFILE]` | Parses the Compose file offline, rejects unknown keys and prints the startup order. `-p '*'` enables every profile. | Anywhere |
| `minga deploy [-c FILE] [-p PROFILE] [--wait SECS] STACK` | Deploys the file as a stack and waits for placement (30 s by default). | Anywhere that reaches the API |
| `minga state export [-o FILE] [--force]` | Writes desired state (services, networks, volumes, secret and config references) to stdout or a 0600 file. Not secret values, volume data or TLS material; see [backup.md](backup.md). | Anywhere that reaches the API |
| `minga state import FILE [--overwrite]` | Restores an export. Fails on an existing service unless `--overwrite`. Agents get the same checks as a deploy; a pre-v9 file may not declare agent capabilities, and in a v9 file an omitted `capabilities` grants nothing. | Anywhere that reaches the API |
| `minga migrate-v1 --manager-id ID [--manager-id ID ...] [--apply]` | Lists the current voters and those to remove when migrating from v1.2.0. Dry-run by default; `--apply` commits one membership change. Requires the admin API token. | Anywhere that reaches the API |
| `minga logs [-f] [--tail N] TASK_ID` | Prints a task's output from JetStream (`--nats-url`); `TASK_ID` is `<stack>.<service>.<n>`. | Anywhere that reaches NATS |
| `minga image pull IMAGE` | Pulls into the `minga` containerd namespace. Credentials come from `$DOCKER_CONFIG/config.json`, else `~/.docker/config.json`, and go to containerd over its API. | On the node |
| `minga secret put ID < value` | Writes or rotates a secret value in the node's store, read from stdin. `ID` is `<stack>_<compose secret key>`. | On every node that runs the service |
| `minga tls init-ca --dir DIR` | Creates the cluster CA (`ca.pem`, `ca-key.pem`). | Anywhere; keep `ca-key.pem` off the nodes |
| `minga tls issue-node --ca-dir DIR --out-dir DIR --node-id ID --role worker\|manager` | Issues a node certificate with the Raft SAN and role-bound NATS identity; manager certificates also include `localhost` for the manager's local sidecar TLS client. Keys are written 0600. | Anywhere; copy one pair to each node |
| `minga agent ls` | Lists running agent tasks from the bus. | Anywhere that reaches NATS |
| `minga sandbox run --module FILE [--input DATA] [--fuel N] [--timeout SECS]`, `minga sandbox info` | Runs a WASM module once; lists the sandbox runtimes. A `--timeout` over 300 is refused. | Anywhere |
| `minga egress [--bind ADDR] ...` | A standalone egress proxy. `--bind` defaults to `0.0.0.0:8080`. | Anywhere |

`deploy` and `state` take `--api-url` (or `MINGA_API_URL`, default
`http://127.0.0.1:2375`) and read the token from `MINGA_API_TOKEN`.

### Planning utilities

`minga net create` and `net inspect`, and `minga wg keygen`, `wg pubkey` and
`wg config`, compute and print subnets, WireGuard keys and a sample interface
config. **These commands change nothing**: they do not create a cluster
network, configure an interface or write to Raft. Compose networks are created
by `minga deploy`; cross-node networking becomes active when the peer-set plan
lands.

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
| `MINGA_API_TOKEN` | `minga manager`; `minga deploy`, `minga state` | API token, server and client side. |
| `MINGA_NATS_URL` | Manager/worker roles, `minga logs`, `minga agent ls` | NATS URL. |
| `MINGA_NATS_TOKEN` | Manager/worker roles | Development-only NATS token for plaintext loopback. Mutually exclusive with JWT credentials. |
| `MINGA_NATS_CREDENTIALS_FILE` | Manager/worker roles | Owner-only NATS user `.creds` file; requires mTLS. |
| `MINGA_NATS_OPERATOR_JWT` | `minga manager` | Public operator JWT file; enables operator/JWT auth on the built-in sidecar. Set with the resolver directory. |
| `MINGA_NATS_RESOLVER_DIR` | `minga manager` | Writable full-resolver directory for account JWTs; set with the operator JWT file. |
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
`--containerd-socket`, so do not repeat them. The manager serves the API with
`--api-bind`; it does not need a second `minga api` unit. Set `MINGA_API_TOKEN`
in the manager's environment file.
