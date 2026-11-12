# minga-ctl: design plan

Status: phase 1 implemented, 2026-11-12. All seven build-order steps in
section 6 are in `crates/minga-ctl`. They are covered by tests against stubs
and test API servers, but have not yet been run against a real node over
SSH. Phase 2 (section 2.3) is not started.

`minga-ctl` is a companion CLI for Minga: named contexts for several
environments, stack and service operations, health, metrics and release
management. It is a separate binary from `minga` and works for any site that
runs Minga.

This plan is security-first. Phase 1 needs no change to Minga and is specified
fully enough to implement. One command group, registry login (section 5.8),
relies on Minga 1.0.2. Phase 2 is server work and is outlined; it needs its
own ADR before it starts.

Two changes that merged into `main` after v1.0.1 shaped this version of the
plan: the Compose compatibility fixes (PR #16, merged through PR #18), which
include registry credentials, and the Hetzner ARM64 staging guide (PR #17).
Both are on `main` and unreleased; this plan calls the release that will carry
them 1.0.2.

## Quick start

Install `minga-ctl` on the workstation: it is not in the release archive.

```
cargo install --locked --path crates/minga-ctl
```

Add a context for the node. The token file on the node is either the bare
token or an env file with a `MINGA_API_TOKEN=` line (item 3 of section 2.2):

```
minga-ctl context add staging --ssh <user>@<node> --remote-token-file /etc/minga/minga-api.env
minga-ctl stack ls
```

`stack deploy` runs `minga deploy` on the workstation, the machine running
minga-ctl, through the tunnel. It does not run on the node, and the node needs
only the API. So the `minga` binary must be installed on the workstation:
`MINGA_BIN` if set, else `minga` on `PATH`. The release archives are Linux
only, so on macOS build it from a checkout:

```
cargo install --locked --path crates/minga-cli
```

```
minga-ctl stack deploy -c compose.yaml myapp
```

Without `minga` the command stops with exit 1 before anything is deployed
(``error: `minga` not found on PATH``, section 4.4). Command details are in section 5.

## 1. What the code allows today

The design follows what Minga serves, not what the Docker Engine API has in
general. All routes are registered in `crates/minga-api/src/server.rs:91-136`.
File and line references are for `main` at `b1e49ac`, after PR #18 merged.

| Finding | Where | Effect on minga-ctl |
|---|---|---|
| Routes exist for version, info, nodes, services, tasks, networks, secrets, configs. There are none for logs, images, containers, events. Unregistered paths return 404 `{"message":"endpoint not implemented by Minga"}`. | `server.rs:91-136`, `handlers.rs:813` | Logs and image commands cannot be HTTP calls. |
| Every route except `/_ping`, `/healthz`, `/readyz`, `/metrics` needs a version prefix. The prefix is captured and not checked. | `server.rs:93-130` | minga-ctl always uses `/v1.41`. |
| The listener is plain HTTP, default `0.0.0.0:2375`. No TLS, no unix socket. | `server.rs:163`, `main.rs:319` | See section 2. |
| Auth is one shared bearer token, checked on every route including `/metrics`. Failure is 401 `{"message":"authentication required"}`. | `server.rs:49-71` | No per-user identity, roles or audit on the server. |
| Service update takes a full spec. The `version`, `registryAuthFrom` and `rollback` query parameters are ignored. | `handlers.rs:416-465` | Scale and update are read, edit, post back. |
| The server keeps one previous spec only while a rollout is in flight and never returns it. | `handlers.rs:441-456`, `Service.previous` in `minga-state/src/types.rs` | Rollback needs its own revision store. |
| Tasks have a state, not a health field. A task with a healthcheck stays `preparing` until its first probe passes. | `minga-api/src/translate.rs:690-697`, `minga-node/src/supervisor.rs:862-870` | Health is derived from state. |
| Service IDs are `<stack>.<service>`. Networks, secrets and configs are `<stack>_<name>`. A stack is only the label `com.docker.stack.namespace`. | `minga-scheduler/src/translate.rs` (`{stack}.{name}`, `{stack}_{name}`) | Service arguments are `<stack>.<service>`, not `<stack>_<service>`. |
| The task `label` filter matches only the stack label. The network `label` filter always returns an empty list. | `handlers.rs:519-531`, `:574` | Stack networks are found by ID prefix. |
| `/healthz`, `/readyz` and `/metrics` are plain text on the API port. `/readyz` returns 503 with a reason. | `minga-telemetry/src/lib.rs:15-41` | Parsed as text. |
| The API process updates only `minga_raft_leader`. Task and scheduler metrics live in the manager process, served on `--metrics-bind`. | `server.rs:175-185`, `main.rs:278-281` | A context can name a separate metrics endpoint. |
| `minga logs` reads NATS JetStream. Plain NATS is allowed only to loopback. | `run_logs` at `main.rs:1672`, `minga-cli/src/bus.rs:68-83` | Logs run on the node. |
| `minga image pull` runs `ctr` on the machine it is called on. There is no `image ls` or `image rm`. | `run_image` at `main.rs:1746` | Image commands run on the node. |
| `minga secret put` reads stdin and writes a file in the local secret store. The API stores secret metadata only and rejects a non-empty `Data`. | `run_secret` at `main.rs:632`, `handlers.rs:673` | Secret values never use the API. |
| In 1.0.1 image pulls are anonymous: `minga image pull` and the automatic pull before a task starts both run `ctr -n minga images pull <ref>` with no credentials. From 1.0.2 both read `$DOCKER_CONFIG/config.json`, else `$HOME/.docker/config.json`; from 1.1.0 they pull through containerd's Transfer API and also read credential helpers. | `run_image` in `minga-cli/src/main.rs`, `credentials.rs` and `ContainerdClient::pull_image` in `minga-node/src/containerd/` | Registry login maintains that file and needs 1.0.2 (section 5.8). |
| `minga deploy` translates the compose file on the machine it runs on. On `main` that includes reading `env_file` files and joining relative bind-mount sources to the compose file's directory. (`extra_hosts: host-gateway` stays literal and is resolved by the node.) | `minga-cli/src/main.rs`, `minga-compose/src/project.rs` | A deploy from a workstation resolves these on the workstation (section 5.2). |
| Things inside the cluster read the API too. The staging guide runs Traefik as a container whose Swarm provider reads `http://<gateway>:2375`, with the API on `0.0.0.0:2375` behind a host firewall. | `docs/hetzner-arm-staging.md` | The API cannot always be bound to loopback (section 2.2). |

Two things the original brief asked for do not exist and are not faked: image
pull latency and update count metrics.

## 2. Trust architecture

### 2.1 The problem

A CLI that sends a shared token to `http://10.0.0.1:2375` exposes the token and
every spec on the network, cannot tell operators apart, and leaves no record of
who changed what. minga-ctl cannot fix the server from the client side, so
phase 1 takes the API off the network instead.

### 2.2 Phase 1: SSH is the only remote transport

**Node setup** (documented requirement, not enforced by minga-ctl):

- The API must not be reachable from outside the node, and always runs with
  `--api-token`. `--api-bind` takes one address, so there are two cases:
  - Nothing inside the cluster reads the API: `--api-bind 127.0.0.1:2375`.
    The default is `0.0.0.0:2375`; change it.
  - A container reads the API, for example Traefik's Swarm provider in the
    staging guide (`docs/hetzner-arm-staging.md`):
    the API has to listen on an address containers can reach. Keep
    `0.0.0.0:2375` and have the host firewall drop port 2375 on every
    external interface. The staging guide does this with `ufw`, leaving
    only 22, 80 and 443 open.

  Either way minga-ctl reaches the API through the SSH tunnel to the node's
  loopback address.
- `--metrics-bind 127.0.0.1:9100` on the manager.
- Operators have their own SSH accounts and are members of one unix group, for
  example `minga-operators`.
- The API token is in a file owned by root, group `minga-operators`, mode 0640,
  for example `/etc/minga/api-token`.

**Client behaviour:**

1. A context names an SSH target. For API calls minga-ctl starts the system
   `ssh` client with a local forward to a Unix socket in a new 0700
   directory:

   ```
   ssh -N -o ExitOnForwardFailure=yes \
       -L <dir>/api.sock:127.0.0.1:2375 \
       [-L <dir>/metrics.sock:127.0.0.1:9100] -- <target>
   ```

   With `ssh`, `api_endpoint` and `metrics_endpoint` must be `http://` to
   the target's own loopback (`127.0.0.1`, `localhost` or `[::1]`):
   ssh makes the hop from the target to the destination in plain HTTP, so
   any other host would see the token. `context add` checks this too.

   The HTTP client connects to the sockets directly; there is no local TCP
   port. System proxies (`HTTP_PROXY` and friends) are never used. A free TCP port handed to `ssh` would be released first, and
   another local user who reads the port from `ps` could bind it before
   `ssh` does and receive the token. Other local users cannot reach the
   sockets either, so a node that runs the API without a token is not
   exposed to them.

   It waits until the sockets accept a connection or the child exits. The
   wait is limited to 10 s when there is no terminal (ssh cannot prompt
   then); with a terminal the operator may be answering a host-key,
   passphrase or MFA prompt and can press Ctrl-C instead. The token read
   (item 3) runs first and without a limit, so prompts are usually
   answered there. The child is killed when the command ends
   (`kill_on_drop`). On SIGINT, SIGTERM or SIGHUP minga-ctl kills it, removes
   the socket directory and exits with 128 + the signal number. On Linux the
   child also gets `PR_SET_PDEATHSIG`, which covers SIGKILL. Elsewhere a
   SIGKILL leaves the tunnel running.

   The socket directory is under `$TMPDIR`, or under `/tmp` when the socket
   paths would exceed `sun_path` (104 bytes on macOS) or contain a character
   such as `:` that `-L` would misread.
2. Keys, agent, host-key checking, jump hosts and MFA belong to the operator's
   ssh configuration. minga-ctl never reads keys, never sets
   `StrictHostKeyChecking`, and does not set `BatchMode`, so interactive MFA
   prompts still work. Without a terminal ssh fails instead of prompting.
   A `ControlMaster` entry in `~/.ssh/config` avoids repeated prompts.
3. The API token is read from the node when the tunnel starts
   (`ssh -T -- <target> 'cat' '<remote_token_file>'`; `-T` keeps a
   `RequestTTY force` in the ssh config from changing the output), kept in memory and never
   written to the workstation. The file holds either the bare token (the
   whole trimmed content, which may contain `=`) or an env file such as the
   API's `EnvironmentFile=/etc/minga/minga-api.env`: a line
   `MINGA_API_TOKEN=value`, optionally after `export `, with the value
   optionally in single or double quotes. Blank lines and `#` comment lines
   are skipped, and the last such line wins. An env file with other
   `NAME=value` lines (upper-case names) but no `MINGA_API_TOKEN` line, or
   with an empty value, is an error naming the variable instead of a token
   that the API answers with 401. Quoted values have no escapes and a value
   has no trailing `# comment`. Errors name the file and the node and never
   show its content.
4. Direct endpoints (no SSH) are accepted only if the URL is `http://` to
   `127.0.0.1`, `localhost` or `[::1]`, or `https://` (API behind a TLS proxy;
   `tls_cert` pins the CA). Plain HTTP to any other host is refused. There is
   no override flag. This is the rule `connect_bus` already applies to NATS.
5. `config.yaml` has no field for a token value. Unknown keys are an error
   (`deny_unknown_fields`), so an `api_token:` line fails loudly.
6. There is no `--token` flag: arguments are visible in process listings and
   shell history.
7. Config and state files are created with mode 0600 in directories with mode
   0700. A config file readable by group or others is refused.
8. Everything sent to a remote shell is validated against a fixed character
   set, then single-quoted. SSH targets must not start with `-` and are passed
   after `--`.

   | Value | Allowed |
   |---|---|
   | SSH target | `^[A-Za-z0-9][A-Za-z0-9._@:-]*$` |
   | Service, task, secret ID | `^[A-Za-z0-9][A-Za-z0-9._-]*$` |
   | Image reference | `^[A-Za-z0-9][A-Za-z0-9._:/@-]*$` |
   | Remote path | `^/[A-Za-z0-9._/-]+$` |
9. Secret values and registry credentials are read from stdin (or a no-echo
   prompt) only and piped to the remote command's stdin. They are never placed
   in arguments, logs or the audit log, and minga-ctl never writes them to
   the workstation. A credential the operator passes as a file
   (`registry login --key-file`) is already on the workstation; that copy is
   the operator's to protect or delete.
10. `--debug` prints method, path and status, and ssh command lines, to
    stderr. The `Authorization` header is printed as `Bearer <redacted>`.
11. Commands that remove or replace things (`stack rm`, `stack deploy
    --prune`, `service rm`, `image rm`, `service rollback`, `registry
    logout`) list what will change and ask for
    confirmation. With
    no terminal they fail unless `--yes` is given.
12. Every command that changes state appends one line to a local audit log
    (section 4.5), on success and on failure.
13. The health URL read from a service label (section 5.5) is server-provided
    data: only `http` and `https` are fetched, redirects are not followed, and
    no credentials are sent.

**What phase 1 does not give you:** the server still cannot attribute an API
call to a person, there are no roles, and every operator needs an SSH account
on a node. The local audit log is written by the operator's own machine, so it
supports the sshd logs but is not independent evidence.

**Readers inside the cluster.** When containers can reach port 2375, the token
is the only thing between any container on that node and the whole cluster:
the API lets its caller read every spec and change every service. A reader
such as Traefik therefore has to send the token. Whether Traefik's Swarm
provider can send a bearer token has not been checked for this plan. If it
cannot, the choices in phase 1 are to run the API without a token, which
gives every container full control and is acceptable for a throwaway staging
node at most, or to keep the token and not use the Swarm provider. Phase 2
fixes this properly: Traefik's Swarm provider has TLS client-certificate
options, so it can be given a `viewer` certificate.

**Revoking an operator.** The API token is shared, and anyone who can read the
token file can keep a copy; minga-ctl not storing it is a habit of the tool,
not a guarantee. Half measures do not revoke access:

- Removing the operator from the unix group does not affect sessions that are
  already open, and a copied token keeps working.
- Disabling port forwarding does not help while they have a shell: the
  loopback API can be called from the node itself.
- Rotating the token does not help while they can still read the token file.

Revocation is these steps, in this order, on every node:

1. Remove or lock the operator's SSH account and remove them from the
   operators group.
2. End their open sessions.
3. Rotate the API token: write a new token file and restart `minga api` with
   it. The token is read at start-up only.

Until step 3 is done, treat the old token as known to them. Phase 2 replaces
the shared token with per-operator certificates.

### 2.3 Phase 2: mutual TLS, roles, audit and revisions on the server

Outline only. Each item changes Minga and goes through an ADR
(`docs/adr/0005-…`).

- **TLS on the API.** `minga api --api-tls-cert --api-tls-key --api-client-ca`,
  following the existing `--raft-tls-*` flags. With a client CA set, a client
  certificate is required.
- **Operator certificates.** `minga tls issue-client --ca-dir … --cn <operator>
  --role viewer|deployer|admin --days 30`, next to `tls issue-node`. The role
  is carried in the certificate.
- **Roles.** Enforced in the middleware beside `bearer_auth`
  (`server.rs:49`): `viewer` may use GET; `deployer` may also write services,
  networks, configs and secret metadata; `admin` may also write nodes.
- **Audit.** One JSON log line per request that changes state: time,
  certificate CN, role, method, path, status and correlation id
  (`CorrelationIdLayer` already exists in `minga-telemetry`). It goes out
  through the existing JSON logging.
- **Revisions.** The last settled specs of a service are kept in Raft state
  (they are desired state). `rollback=previous` is honored and `PreviousSpec`
  is returned. This replaces the local revision file.
- **Update conflicts.** `Version.Index` changes on every update and the API
  rejects a service update whose `version` is stale with 409. minga-ctl
  already sends the version it read.
- **Forced restarts.** The API keeps `TaskTemplate.ForceUpdate` and a change
  to it replaces the service's tasks, which makes `service update --force`
  possible.
- **Resource ownership.** Networks, configs and secret metadata keep their
  labels, so `stack rm` can tell which ones a stack created.
- **minga-ctl.** A context gains `client_cert` and `client_key`. SSH is then
  needed only for the node-local commands.

### 2.4 Control mapping

SOC 2 attests an organisation's controls. minga-ctl can support those controls
and produce evidence; it cannot provide compliance by itself.

| Criterion | Phase 1 supports it with | Phase 2 adds | Stays with the organisation |
|---|---|---|---|
| CC6.1 logical access | Operators reach the API only through SSH to a node; minga-ctl stores no credentials on workstations | Client certificate per operator | SSH key issuance, MFA, device security |
| CC6.2, CC6.3 provisioning and least privilege | Access is an SSH account on a node plus read access to the token file through a unix group. Revoking an operator means removing the account, ending its sessions and rotating the shared token, in that order (section 2.2). No roles. | `viewer`, `deployer`, `admin`; short-lived certificates | Joiner and leaver process, access reviews |
| CC6.6 boundary protection | API not reachable from outside the node (loopback, or firewalled when containers must read it); metrics on loopback; client refuses plain HTTP off-host. Containers that can reach the API are held back only by the token. | TLS listener; read-only certificate for in-cluster readers | Firewalls, bastion hosts |
| CC6.7 data in transit | All remote traffic inside SSH; secret values on stdin only | TLS on the API | Key management |
| CC7.2 monitoring | sshd logs (who, when, from where) plus the client audit log (what) | Server audit line per change, tied to the certificate identity | Log shipping, retention, alerting |
| CC8.1 change management | Deploys from a compose file; previous revision saved with each accepted change; rollback; audit line per change | Server-side revisions | Review and approval, CI gates |

## 3. Crate layout

New crate `crates/minga-ctl`, binary `minga-ctl`.

```toml
[package]
name = "minga-ctl"
description = "Companion CLI for Minga: contexts, stacks, services, health"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[[bin]]
name = "minga-ctl"
path = "src/main.rs"

[dependencies]
chrono.workspace = true
clap.workspace = true
reqwest = { workspace = true, features = ["rustls-tls"] }
rpassword.workspace = true
serde.workspace = true
serde_json.workspace = true
serde_yaml.workspace = true
thiserror.workspace = true
tokio.workspace = true

[dev-dependencies]
minga-api = { path = "../minga-api" }
minga-state = { path = "../minga-state" }
tempfile.workspace = true
```

Notes:

- The workspace `reqwest` has no TLS backend; `rustls-tls` is added here for
  `https://` endpoints. Cargo unifies features, so a workspace build also links
  it into `minga`.
- `rpassword` is the one new workspace dependency (`rpassword = "7"` in the
  root `[workspace.dependencies]`). It reads the registry token without echo
  and restores the terminal if the prompt is interrupted.
- No runtime dependency on other Minga crates. Service specs are handled as
  `serde_json::Value`, so a spec is posted back exactly as it was read, with
  only the edited field changed.
- No table crate: tables are formatted by hand with padded columns, as in
  `minga-cli`. No `dirs` crate: paths come from `XDG_CONFIG_HOME` and
  `XDG_STATE_HOME`, else `$HOME/.config` and `$HOME/.local/state`.
- minga-ctl runs the `minga` binary for `stack deploy`, `service logs`,
  `image pull` and `secret put`. Locally the path is `MINGA_BIN`, else `minga`
  on `PATH`. On a node it is `minga` on the SSH user's `PATH`.

```
crates/minga-ctl/
  Cargo.toml
  src/main.rs        clap tree, dispatch, exit codes
  src/config.rs      Config, Context, load/save, permission check, resolution
  src/transport.rs   Transport, Tunnel, ssh exec, validation and quoting
  src/api.rs         ApiClient and error mapping
  src/output.rs      table / json / yaml / quiet
  src/state.rs       revision files and audit log
  src/cmd/context.rs stack.rs service.rs image.rs health.rs metrics.rs secret.rs
          registry.rs
  tests/api.rs       against a real minga_api::ApiServer
  tests/wrapped.rs   wrapped commands against a stub MINGA_BIN
```

### Struct sketches

```rust
// config.rs
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub current_context: Option<String>,
    #[serde(default)]
    pub contexts: BTreeMap<String, Context>, // key is the context name
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Context {
    /// SSH target for the API tunnel and remote commands.
    pub ssh: Option<String>,
    /// API URL. With `ssh`, as seen from the SSH target.
    /// Default: http://127.0.0.1:2375
    pub api_endpoint: Option<String>,
    /// File on the SSH target holding the API token.
    pub remote_token_file: Option<String>,
    /// Local command that prints the token; run without a shell.
    pub token_command: Option<Vec<String>>,
    /// PEM CA certificate to trust for an https:// endpoint.
    pub tls_cert: Option<PathBuf>,
    /// Metrics URL. With `ssh`, as seen from the SSH target.
    /// Default: same as api_endpoint.
    pub metrics_endpoint: Option<String>,
    /// NATS URL for logs, as seen from where `minga logs` runs.
    /// Default: nats://127.0.0.1:4222
    pub nats_url: Option<String>,
    /// SSH targets for node-local commands (image, secret put, registry).
    #[serde(default)]
    pub nodes: Vec<String>,
    /// Docker credential file on the node that Minga reads when pulling.
    /// Default: /root/.docker/config.json
    pub registry_auth_file: Option<String>,
}

// What a command runs against, after flags, env and config are merged.
pub struct Resolved {
    pub name: String, // context name, or "default"
    pub context: Context,
    pub token: Option<String>, // in memory only
}

// transport.rs
pub enum Transport {
    Direct { api: String, metrics: String },
    Ssh { target: String, tunnel: Tunnel },
}

pub struct Tunnel {
    child: tokio::process::Child, // kill_on_drop(true)
    pub api: String,     // http://127.0.0.1:<port>
    pub metrics: String, // http://127.0.0.1:<port 2>
}

// api.rs
pub struct ApiClient {
    http: reqwest::Client, // 15 s timeout, bearer header when a token is set
    root: String,          // http://127.0.0.1:2375
}
impl ApiClient {
    pub async fn get(&self, path: &str, filters: Option<&Value>) -> Result<Value, CtlError>;
    pub async fn post(&self, path: &str, body: &Value) -> Result<Value, CtlError>;
    pub async fn delete(&self, path: &str) -> Result<(), CtlError>;
    pub async fn text(&self, url: &str) -> Result<(u16, String), CtlError>; // healthz, readyz, metrics
}

// main.rs
#[derive(Debug, thiserror::Error)]
pub enum CtlError {
    #[error("{0}")]
    Usage(String), // exit 1
    #[error("{0}")]
    Config(String), // exit 1
    #[error("`{bin}` not found on PATH (needed for `{command}`)")]
    MissingBinary { bin: String, command: String }, // exit 1
    #[error("cannot reach the Minga API at {endpoint}: {detail}")]
    Unreachable { endpoint: String, detail: String }, // exit 2
    #[error("ssh to {target} failed: {detail}")]
    Ssh { target: String, detail: String }, // exit 2
    #[error("the Minga API at {endpoint} rejected the credentials (401 {message})")]
    Auth { endpoint: String, message: String }, // exit 2
    #[error("API error {status}: {message}")]
    Api { status: u16, message: String }, // exit 2
    #[error("{kind} `{name}` not found in context `{context}`")]
    NotFound { kind: &'static str, name: String, context: String }, // exit 3
}
```

## 4. Config, environment, flags, exit codes

### 4.1 `config.yaml`

Path: `$XDG_CONFIG_HOME/minga-ctl/config.yaml`, else
`~/.config/minga-ctl/config.yaml`. Mode 0600.

```yaml
current_context: prod
contexts:
  prod:
    ssh: alice@mgr1.example.net
    remote_token_file: /etc/minga/api-token
    metrics_endpoint: http://127.0.0.1:9100
    nodes:
      - alice@mgr1.example.net
      - alice@wrk1.example.net
  local:
    api_endpoint: http://127.0.0.1:2375
  edge:
    api_endpoint: https://minga.edge.example.net
    tls_cert: /etc/minga-ctl/edge-ca.pem
    token_command: ["op", "read", "op://infra/minga-edge/token"]
```

With no config file and no environment override, minga-ctl uses
`http://127.0.0.1:2375` directly, under the context name `default`.

### 4.2 Environment

| Variable | Effect |
|---|---|
| `MINGA_CTL_CONTEXT` | Context to use instead of `current_context`. |
| `MINGA_CTL_ENDPOINT` | Replaces the context's `api_endpoint`. The plain-HTTP rule still applies. |
| `MINGA_CTL_TOKEN` | API token. Wins over `token_command` and `remote_token_file`. |
| `MINGA_BIN` | Path of the local `minga` binary, which `stack deploy` runs on this machine. |

Precedence: flag, then environment, then context, then default.
Token precedence: `MINGA_CTL_TOKEN`, `token_command`, `remote_token_file`, none.

### 4.3 Global flags

| Flag | Default | Meaning |
|---|---|---|
| `--context <name>` | `current_context` | Context for this command. |
| `-o, --output <table\|json\|yaml>` | `table` | Output format. |
| `--json` | off | Same as `--output json`. |
| `-q, --quiet` | off | Lists print IDs only, one per line. Changes print nothing. |
| `--debug` | off | Request and ssh trace on stderr, credentials redacted. |

Results go to stdout. Progress, prompts and errors go to stderr. JSON and YAML
output is an array of the same rows the table shows, with snake_case keys.

### 4.4 Exit codes and error text

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | User or configuration error. |
| 2 | API or transport error, including authentication failure and a failed health check. |
| 3 | The named stack, service, context or revision does not exist. |

A 404 whose message is `endpoint not implemented by Minga` is exit 2. Any
other 404 is exit 3. Error bodies that are not JSON (axum returns plain text
for 400, 415 and 422 request rejections) are shown as the message.

| Case | Text on stderr | Code |
|---|---|---|
| API unreachable | ``error: cannot reach the Minga API at http://127.0.0.1:2375: connection refused`` then ``hint: is `minga api` running there? see `minga-ctl context list` `` | 2 |
| SSH failed | ``error: ssh to alice@mgr1.example.net failed: Permission denied (publickey)`` | 2 |
| Token rejected | ``error: the Minga API at http://127.0.0.1:2375 rejected the credentials (401 authentication required)`` then ``hint: check remote_token_file, token_command or MINGA_CTL_TOKEN for context `prod` `` | 2 |
| Plain HTTP off-host | ``error: refusing plain HTTP to 10.0.0.1: the token and specs would cross the network unencrypted`` then ``hint: use --ssh user@10.0.0.1, or an https:// endpoint with --tls-cert`` | 1 |
| Stack not found | ``error: stack `myapp` not found in context `prod` `` | 3 |
| Service not found | ``error: service `myapp.web` not found in context `prod` `` | 3 |
| No saved revision | ``error: revision `myapp.web` not found in context `prod` `` then ``hint: rollback covers only changes made with minga-ctl on this machine`` | 3 |
| `minga` missing | ``error: `minga` not found on PATH (needed for `stack deploy`)`` then ``hint: install minga on this machine (it runs here, not on the node) or set MINGA_BIN; on macOS: cargo install --locked --path crates/minga-cli`` | 1 |
| Config too open | ``error: ~/.config/minga-ctl/config.yaml is readable by group or others (mode 0644)`` then ``hint: chmod 600 ~/.config/minga-ctl/config.yaml`` | 1 |
| Unknown config key | ``error: ~/.config/minga-ctl/config.yaml: unknown field `api_token` `` then ``hint: tokens are not stored in the config; use remote_token_file or token_command`` | 1 |
| No terminal for a prompt | ``error: `stack rm` needs confirmation; pass --yes to run without a terminal`` | 1 |
| No registry credential given | ``error: no credential on stdin for ghcr.io`` then ``hint: run in a terminal to be prompted, or pipe the token to stdin`` | 1 |
| Registry file not usable | ``error: cannot read /root/.docker/config.json on alice@mgr1.example.net: Permission denied`` then ``hint: the SSH user needs read and write access to the file; minga-ctl does not use sudo`` | 2 |
| Registry file not JSON | ``error: /root/.docker/config.json on alice@mgr1.example.net is not valid JSON; left unchanged`` | 2 |
| Deploy needs the node | ``error: compose.yaml bind-mounts ./conf (service `traefik`), which would resolve on this machine, not on the node`` then ``hint: use an absolute path on the node, or run minga-ctl on the node with a local context`` | 1 |

### 4.5 State files

Directory: `$XDG_STATE_HOME/minga-ctl/`, else `~/.local/state/minga-ctl/`,
mode 0700.

- `revisions/<context>/<service id>.json`, mode 0600. One file per service,
  holding the spec as it was before the last change made by minga-ctl:

  ```json
  {"saved_at":"2026-11-11T13:31:15Z","service_id":"myapp.web","spec":{"Name":"myapp.web","…":"…"}}
  ```

  Files are written to a temporary file and renamed. They hold full specs,
  including environment values.
- `audit.jsonl`, mode 0600, append only. One line per command that changes
  state:

  ```json
  {"ts":"2026-11-11T13:31:15Z","user":"alice","context":"prod","target":"alice@mgr1.example.net","command":"service scale","args":{"service":"myapp.web","replicas":5},"result":"ok","exit_code":0}
  ```

  `ts` is RFC 3339 in UTC. `user` is `$USER`. `args` never contains secret
  values or environment values (`--env-add KEY=VALUE` is logged as the key).

## 5. Commands

Service arguments are service IDs: `<stack>.<service>`.

### 5.1 `context`

Local file only. No API call.

```
minga-ctl context add <name> [--ssh <target>] [--endpoint <url>]
                             [--remote-token-file <path>] [--token-command <cmd>]
                             [--tls-cert <path>] [--metrics-endpoint <url>]
                             [--nats-url <url>] [--node <target>]...
                             [--registry-auth-file <path>] [--use]
minga-ctl context use <name>
minga-ctl context list
minga-ctl context delete <name>
```

- `--endpoint` defaults to `http://127.0.0.1:2375`. The plain-HTTP rule is
  checked at `add` time when `--ssh` is absent.
- `--token-command` is split on whitespace and run without a shell.
- `add` fails if the name exists. The first context added becomes current;
  `--use` makes any new context current.
- `delete` of the current context clears `current_context`.
- `use` and `delete` of an unknown name exit 3.

```
$ minga-ctl context add prod --ssh alice@mgr1.example.net --remote-token-file /etc/minga/api-token
context `prod` added and selected

$ minga-ctl context list
CURRENT  NAME   TRANSPORT                   ENDPOINT                         TOKEN
*        prod   ssh alice@mgr1.example.net  http://127.0.0.1:2375            remote file
         local  direct                      http://127.0.0.1:2375            none
         edge   direct                      https://minga.edge.example.net   command
```

```json
[{"current":true,"name":"prod","transport":"ssh","ssh":"alice@mgr1.example.net","endpoint":"http://127.0.0.1:2375","token":"remote file"}]
```

### 5.2 `stack`

```
minga-ctl stack deploy -c <file> [-p <profile>]... [--wait <secs>] [--prune [--yes]] <stack>
minga-ctl stack ls
minga-ctl stack ps <stack>
minga-ctl stack rm <stack> [--with <id>]... [--yes]
```

**`deploy`** (`-c` default `compose.yaml`, `--wait` default `30`)

0. When the node is another machine, check the compose file before anything
   else (see "Where the compose file is resolved" below). The node is another
   machine in any context with an SSH target, and in any context whose API
   endpoint is not a loopback address, such as an `https://` endpoint.
   Then find the stack's services that the file does not name: the IDs from
   step 1 that are not `<stack>.<key>` for a key under `services:`. With
   `--prune` they are listed and confirmed here, before anything is deployed
   (no terminal and no `--yes`: exit 1, nothing deployed), and a file with no
   services is refused (exit 1), since pruning would empty the stack; use
   `stack rm`. Without `--prune` they are only named in step 5.
1. `GET /v1.41/services?filters={"label":["com.docker.stack.namespace=<stack>"]}`
   and save each existing service's `Spec` as its revision file, keeping the
   revision it replaces in memory. A deploy that is interrupted, or whose
   result cannot be read back, then never leaves a revision from an older
   change to roll back to. If a save fails, the replaced revisions are put
   back and nothing is deployed.
2. Run `minga deploy -c <file> [-p …] --wait <secs> --api-url <endpoint> <stack>`
   with `MINGA_API_TOKEN` in the child's environment. This runs on the
   machine running minga-ctl, never on the node, so `minga` must be
   installed there (`MINGA_BIN`, else `PATH`; see the quick start for
   macOS). With SSH the endpoint is the tunnel's local URL. Output passes
   through.
3. Read the services again, whether `minga` succeeded or not. A service whose
   `Spec` now differs from step 1 keeps the step 1 revision. A service the
   deploy did not change, or did not reach before failing, gets back the
   revision file it had.
4. Exit code: 2 if `minga` did not exit 0. Also 2 if `minga` succeeded but the
   read in step 3 or a revision write failed, with
   ``error: deployed, but could not save revisions for myapp: <reason>`` and
   ``hint: each service's revision is its spec from before this deploy; for a service the deploy did not change, rollback restores that same spec``.
   Otherwise 0.
5. Only when `minga` exited 0. With `--prune`, `DELETE /v1.41/services/{id}`
   for each service confirmed in step 0 that is still there (the stack's
   label is what selected them, so another stack's services are never
   touched), after saving its `Spec` from step 1 as its revision, so
   `service rollback` recreates it; as in `service rm`, a failed delete puts
   the earlier revision back. If pruning fails after step 3 could not save a
   revision, the error names both. Every one is tried; a failure exits 2 with
   ``error: deployed, but could not prune <id> (<reason>)``. Without
   `--prune`, one note on stderr (not with `--quiet`):
   ``note: myapp.old in stack myapp but not in compose.yaml; --prune removes it``.

`minga deploy` validates the compose file itself, and unknown keys fail there.

```
$ minga-ctl stack deploy -c compose.yaml myapp
<output of minga deploy>
saved revision for myapp.web (myapp.worker unchanged)
note: myapp.old in stack myapp but not in compose.yaml; --prune removes it

$ minga-ctl stack deploy -c compose.yaml --prune myapp
After the deploy this removes from context `prod`:
  services: myapp.old
(their specs are saved; `service rollback` recreates one)
Continue? [y/N] y
<output of minga deploy>
saved revision for myapp.web (myapp.worker unchanged)
pruned myapp.old
```

`--prune` reads the service names from the compose file itself, not from
`minga deploy`. Every key under `services:` counts as in the file, whatever
its `profiles`, so a service behind a profile you did not enable is kept.

**Where the compose file is resolved.** `minga deploy` translates the compose
file on the machine it runs on, and minga-ctl runs it on the operator's
machine. On `main`, and so from 1.0.2, three things are resolved there:

| In the compose file | Resolved on the operator's machine as | When the node is another machine |
|---|---|---|
| `env_file` | The files are read and their values go into the service spec | Allowed. It is what a deploy from a workstation should do. The values end up in the spec and in the revision file. |
| A bind mount with a relative source (`./conf:/etc/app`) | An absolute path under the compose file's directory | Refused: that path is on the workstation, not on the node. |
| `extra_hosts: ["name:host-gateway"]` | Nothing: it stays literal, and the node writes its own bridge address | Allowed. |

The check in step 0 reads the compose file as YAML and looks, per service, for
volume entries whose source starts with `./`, `../` or `~`. If it finds one it exits 1 with
the "Deploy needs the node" error from section 4.4 and deploys nothing. It is
a guard on the plain file: it does not follow `extends`, `include` or
variable interpolation. Nothing is refused only when the context has no SSH
target and its endpoint is a loopback address, because then the operator's
machine is the node.

To deploy a stack that needs either feature, use absolute paths and explicit
addresses, or copy the compose file to the node and run minga-ctl there with
a local context.

**`ls`**: `GET /v1.41/services`, grouped by the label
`com.docker.stack.namespace` in `Spec.Labels`. Services without the label are
not listed.

```
NAME    SERVICES
myapp   2
shop    5
```

```json
[{"name":"myapp","services":2},{"name":"shop","services":5}]
```

**`ps`**: `GET /v1.41/tasks?filters={"label":["com.docker.stack.namespace=<stack>"]}`,
`GET /v1.41/services` with the same filter (for healthcheck presence) and
`GET /v1.41/nodes` (for host names). No services with the label: exit 3.

`HEALTH` is derived, because tasks carry no health field:

| Service has a healthcheck | `Status.State` | HEALTH |
|---|---|---|
| no (`Healthcheck` absent, or `Test` empty or `["NONE"]`) | any | `-` |
| yes | `preparing` | `starting` |
| yes | `running` | `healthy` |
| yes | anything else | `-` |

`ERROR` is the last column, so scripts that read the earlier ones keep
working. It is `Status.Err`, the reason the task last exited or failed to
start, cut to 50 characters in the table (`-` when there is none). `-o json`
and `-o yaml` carry the whole text as `error` and the exit code as
`exit_code`, both `null` when absent. When any task is not in state
`running`, `ps` also prints one line on stderr (not with `--quiet`):
``2 task(s) not running: see `minga-ctl service inspect <service>` and `minga-ctl service logs <service>` ``,
with the service ID when only one service has such tasks.

```
ID                      SERVICE       NODE   DESIRED   STATE      HEALTH    ERROR
myapp.web.1.3f9a1c2e    myapp.web     mgr1   running   running    healthy   -
myapp.web.2.3f9a1c2e    myapp.web     wrk1   running   preparing  starting  exited with code 1
myapp.worker.1.77b0d4aa myapp.worker  wrk1   running   running    -         -
```

```json
[{"id":"myapp.web.1.3f9a1c2e","service":"myapp.web","node":"mgr1","desired":"running","state":"running","health":"healthy","error":null,"exit_code":null}]
```

**`rm`** (`minga-ctl stack rm <stack> [--with <id>]... [--yes]`)

Only services carry the stack label, so only services are known to belong to
a stack. Minga keeps no labels on networks, configs or secrets; an ID that
starts with `<stack>_` suggests the stack created it but does not prove it.
`rm` therefore removes services, and removes other resources only when they
are named.

1. List the stack's services as in `deploy` step 1. None: exit 3, unless
   `--with` is given.
2. List `GET /v1.41/networks`, `GET /v1.41/configs`, `GET /v1.41/secrets`.
   Entries whose ID starts with `<stack>_` are candidates.
3. Check every `--with <id>`. It is refused (exit 1, nothing removed) unless
   it is a candidate and no service outside the stack refers to it. Service
   specs list their networks (`TaskTemplate.Networks[].Target`) and secrets
   (`ContainerSpec.Secrets[].SecretID`).
4. Print the services and the named resources that will be removed, and ask
   for confirmation.
5. `DELETE /v1.41/services/{id}` for each service, then
   `DELETE /v1.41/networks/{id}`, `/configs/{id}` or `/secrets/{id}` for each
   named resource. A 409 on a resource is printed as a warning and does not
   fail the command.
6. Print the candidates that were left in place, with the command to remove
   them.

Removing secret metadata does not remove secret files from nodes.

```
$ minga-ctl stack rm myapp
This removes from context `prod`:
  services: myapp.web, myapp.worker
Continue? [y/N] y
removed 2 services
left in place (ownership cannot be confirmed):
  network myapp_default
  secret  myapp_db_password (metadata only)
to remove them: minga-ctl stack rm myapp --with myapp_default --with myapp_db_password
```

Once the services are gone, the same command with `--with` removes the named
resources.

### 5.3 `service`

```
minga-ctl service ls [--stack <stack>]
minga-ctl service ps <service>
minga-ctl service inspect <service> [--tail <n>]
minga-ctl service logs <service> [-f] [--tail <n>]
minga-ctl service scale <service> --replicas <n>
minga-ctl service update <service> [--image <ref>] [--env-add <KEY=VALUE>]...
                                   [--env-rm <KEY>]... [--replicas <n>]
minga-ctl service rm <service> [--yes]
minga-ctl service rollback <service> [--yes]
```

**`ls`**: `GET /v1.41/services` (with the stack label filter when `--stack` is
given) and `GET /v1.41/tasks`, grouped by `ServiceID`. `REPLICAS` is tasks in
state `running` over `Spec.Mode.Replicated.Replicas`; a global service shows
the running count only.

```
ID            STACK  MODE        REPLICAS  IMAGE
myapp.web     myapp  replicated  2/3       ghcr.io/acme/web:1.4.2
myapp.worker  myapp  replicated  1/1       ghcr.io/acme/worker:1.4.2
shop.proxy    shop   global      2         docker.io/library/caddy:2
```

```json
[{"id":"myapp.web","stack":"myapp","mode":"replicated","running":2,"desired":3,"image":"ghcr.io/acme/web:1.4.2"}]
```

**`ps`**: `GET /v1.41/services/{id}` (404: exit 3), then
`GET /v1.41/tasks?filters={"service":["<id>"]}`. Same columns, ERROR column
and stderr hint as `stack ps`.

**`inspect`**: the same two reads plus `GET /v1.41/nodes`. It prints the
service's image, mode and replicas, ports, networks, update policy and
healthcheck, then its tasks with `STATE`, `RESTARTS`, `EXIT` (exit code of
the last run) and `ERROR` in full. `-o json` and `-o yaml` print one object,
`{"service": {...}, "tasks": [...]}`. `RESTARTS` comes from `MingaRestarts`,
a Minga field on the task that Docker clients ignore.

For each task whose state is not `running` it also runs `minga logs <task id>
--tail <n> --nats-url <url>` the way `logs` does (`--tail` default `20`, `0`
means all, applied to lines again here because one log entry can hold many;
four at a time, 20 seconds at most each once started) and prints the lines
under the table, each labelled `[stdout]` or `[stderr]` after the stream of
that command, with control characters shown as `?`. If the command cannot run
(no `minga` there, exit 255 over SSH for an unreachable node, any other
non-zero exit, a timeout) the task gets one line, `skipped: <why>`, and the
command still exits 0. In `-o json` and `-o yaml` those tasks gain
`"logs": {"stdout": [...], "stderr": [...], "error": null}` (`error` holds the
reason, and the two lists are empty, when it was skipped); running tasks have
no `logs` key, and no existing field changes. `--quiet` prints task IDs and
contacts no node.

Why a task ended comes from the node's supervisor, which records it on the
task: `exited with code N` (with the signal when the code is 128 plus one),
or the error from starting the container, cut to 300 characters and with
registry credentials removed. It stays while the task restarts and is cleared
after the task has been `running` for 60 seconds.

**`logs`** (`--tail` default `100`, `0` means all)

1. `GET /v1.41/tasks?filters={"service":["<id>"],"desired-state":["running"]}`.
2. For each task run `minga logs <task id> --tail <n> [-f] --nats-url <url>`:
   on the context's SSH target when it has one, else locally. `<url>` is the
   context's `nats_url`, else `nats://127.0.0.1:4222`.
3. Each line is printed with the task ID in front. stderr chunks go to stderr.
   With `-f` the children run until interrupted, or until stdout is closed
   (`| head`): then every child is stopped and the exit code is 0.

There is no JSON form; `--output` is ignored. If NATS on the node needs a
token, `MINGA_NATS_TOKEN` must be set in the SSH user's environment there;
minga-ctl does not carry NATS credentials.

```
$ minga-ctl service logs myapp.web --tail 2
myapp.web.1.3f9a1c2e | listening on :8080
myapp.web.2.3f9a1c2e | listening on :8080
```

**`rm`**

1. `GET /v1.41/services/{id}` (404: exit 3).
2. Show the service (mode, image) and ask for confirmation.
3. Save the `Spec` read in step 1 as the revision file, so `rollback`
   recreates the service. If that write fails nothing is deleted (exit 2).
4. `DELETE /v1.41/services/{id}`. If that fails, or the service is already
   gone (exit 3), the revision it replaced is put back.

```
$ minga-ctl service rm myapp.old
This removes service myapp.old (replicated x1, ghcr.io/acme/old:1) from context `prod`; `service rollback` can bring it back.
Continue? [y/N] y
myapp.old removed; `minga-ctl service rollback myapp.old` recreates it
```

**`scale`**

1. `GET /v1.41/services/{id}`. If `Spec.Mode` has no `Replicated`, exit 1:
   ``error: service `shop.proxy` is global and cannot be scaled``.
2. Set `Spec.Mode.Replicated.Replicas` in a copy of the spec. If the copy
   equals the spec read in step 1, print `myapp.web already has 5 replicas;
   nothing changed`, exit 0, and touch neither the API nor the revision file.
3. `POST /v1.41/services/{id}/update?version=<Version.Index>` with the whole
   spec. The server ignores `version` today; it is sent so the call stays
   correct if the server starts checking it.
4. Only after the API accepts the update, save the spec read in step 1 as the
   revision file. If the POST fails, the revision file is left as it was, so
   a rejected change never replaces the revision you can roll back to.

The revision file therefore always holds a spec that differs from the running
one: a change that changes nothing never replaces it.

```
$ minga-ctl service scale myapp.web --replicas 5
myapp.web scaled from 3 to 5
```

```json
{"service":"myapp.web","replicas":{"from":3,"to":5}}
```

**`update`**: same sequence as `scale`: read, edit, stop if nothing changed,
post, then save the revision. At least one flag is required.

| Flag | Edit |
|---|---|
| `--image <ref>` | `Spec.TaskTemplate.ContainerSpec.Image` |
| `--env-add KEY=VALUE` | Replace or append in `Spec.TaskTemplate.ContainerSpec.Env` |
| `--env-rm KEY` | Remove from the same list |
| `--replicas <n>` | As `scale` |

`--env-add` values are visible in process listings. Use Minga secrets for
credentials.

There is no `--force`. The API never reads `TaskTemplate.ForceUpdate`
(`crates/minga-api/src/translate.rs` only writes it as `None`), and tasks are
replaced only when the translated spec changes. A force-only update would
report success and restart nothing, so the flag waits for a Minga change.

**Concurrent changes.** The server ignores `version`, so it cannot reject a
stale update. If two operators read the same spec and each posts an edit, the
later post replaces the earlier one and neither is told. minga-ctl reads the
spec immediately before it posts, which keeps the window to one round trip,
and `rollback` reads again after its confirmation prompt. That narrows the
window; it does not close it. Closing it needs the server to check `version`
(section 2.3).

Operating rule until then: only one change at a time may be in flight for a
given service, across every client that can write to the API (minga-ctl,
`minga deploy`, CI jobs, Docker-compatible tools). minga-ctl cannot enforce
this; the team has to, for example by sending every change to a service
through one CI pipeline that does not run jobs for the same stack in parallel.

```
$ minga-ctl service update myapp.web --image ghcr.io/acme/web:1.4.3
myapp.web updated: image ghcr.io/acme/web:1.4.2 -> ghcr.io/acme/web:1.4.3
```

**`rollback`**

1. Read the revision file. Missing: exit 3.
2. `GET /v1.41/services/{id}`. A 404 means `service rm` or `stack deploy
   --prune` removed it: ask for confirmation, then `POST
   /v1.41/services/create` with the saved spec (a 409 means it came back
   meanwhile), print `myapp.old recreated` and stop; the revision stays. If
   the saved spec equals the running one, print `myapp.web already matches the saved revision; nothing
   changed` and exit 0.
3. Show what changes (image, replicas, number of changed environment keys)
   and ask for confirmation.
4. `GET /v1.41/services/{id}` again. If the spec differs from step 2, stop
   with exit 2:
   ``error: service `myapp.web` changed while waiting for confirmation; run rollback again``.
5. `POST /v1.41/services/{id}/update?version=<Version.Index>` with the saved
   spec.
6. On success, save the spec read in step 2 as the new revision file. A second
   rollback therefore returns to where you started. If the POST fails, the
   revision file is left as it was.

The server applies it as a normal update, with the service's `UpdateConfig`.

```
$ minga-ctl service rollback myapp.web
Roll back myapp.web in context `prod` to the revision saved 2026-11-11T13:31:15Z:
  image: ghcr.io/acme/web:1.4.3 -> ghcr.io/acme/web:1.4.2
Continue? [y/N] y
myapp.web rolled back
```

### 5.4 `image`

Node-local. No API call.

```
minga-ctl image pull <ref> [--ssh <target>]...
minga-ctl image ls         [--ssh <target>]...
minga-ctl image rm <ref>   [--ssh <target>]... [--yes]
```

Targets: every `--ssh` given; else the context's `nodes`; else the context's
`ssh`; else the local machine. Targets run one after another. A failure on one
target is reported, the rest still run, and the exit code is 2.

| Command | Runs on each target |
|---|---|
| `pull` | `minga image pull <ref>` |
| `ls` | `ctr -n minga images ls -q` |
| `rm` | `ctr -n minga images rm <ref>` |

`minga` has no `image ls` or `image rm`, so those call `ctr` directly. The SSH
user needs access to the containerd socket; minga-ctl does not call `sudo`.
`minga image pull` is anonymous in 1.0.1. On `main` it uses the node's
registry credentials (section 5.8), the same as a task's automatic pull.

```
$ minga-ctl image ls
NODE                    REF
alice@mgr1.example.net  ghcr.io/acme/web:1.4.2
alice@mgr1.example.net  ghcr.io/acme/worker:1.4.2
alice@wrk1.example.net  ghcr.io/acme/web:1.4.2
```

```json
[{"node":"alice@mgr1.example.net","ref":"ghcr.io/acme/web:1.4.2"}]
```

### 5.5 `health`

```
minga-ctl health [--stack <stack>] [--timeout <secs>]
```

`--timeout` default `5`, per check.

| Check | Call | ok when |
|---|---|---|
| `api /healthz` | `GET /healthz` | 200 |
| `api /readyz` | `GET /readyz` | 200; a 503 body is shown as the detail |
| `<service>` (with `--stack`) | `GET /v1.41/services` and `GET /v1.41/tasks`, stack label filter | running tasks equal desired replicas; fewer is `degraded`, none is `fail` |
| `<service> url` (with `--stack`) | `GET` the URL in the service label `minga.ctl.health.url`, from the operator's machine | 2xx within the timeout |

Exit 0 when every check is `ok`, else 2. With `--stack` and no such stack,
exit 3.

```
$ minga-ctl health --stack myapp
CHECK             STATUS    DETAIL
api /healthz      ok
api /readyz       ok
myapp.web         degraded  2/3 running
myapp.web url     ok        200 in 41 ms
myapp.worker      ok        1/1 running
```

```json
[{"check":"myapp.web","status":"degraded","detail":"2/3 running"}]
```

### 5.6 `metrics`

```
minga-ctl metrics
```

`GET /metrics` on the context's `metrics_endpoint` (default: the API
endpoint). The Prometheus text is parsed line by line: lines starting with `#`
are skipped, the rest are `name{labels} value`.

The API process reports only `minga_raft_leader`. Point `metrics_endpoint` at
the manager's `--metrics-bind` listener to see task and scheduler numbers.

| Row | Source |
|---|---|
| `tasks <state>` | `minga_tasks_total{state}`, one row per label value |
| `task starts`, `task stops`, `task restarts` | `minga_task_starts_total`, `minga_task_stops_total`, `minga_task_restarts_total` |
| `placement <result>` | `minga_scheduler_placement_decisions_total{result}` |
| `reconcile avg` | `minga_scheduler_reconcile_duration_seconds` sum / count |
| `raft leader`, `raft replication lag` | `minga_raft_leader`, `minga_raft_replication_lag` |
| `bus publish avg <topic>` | `minga_bus_publish_latency_seconds{topic}` sum / count |
| `bus dead letters` | `minga_bus_dead_letters_total`, summed |
| `agent pool idle / busy / total` | `minga_agent_pool_idle`, `_busy`, `_total` |
| `containerd <method> avg` | `minga_containerd_grpc_latency_seconds{method}` sum / count |

Histograms are shown as an average only. Metrics missing from the response are
left out of the table.

```
$ minga-ctl metrics
METRIC                   VALUE
tasks running            12
tasks failed             1
task starts              48
task stops               31
task restarts            5
reconcile avg            3.1 ms
raft leader              yes
raft replication lag     0
bus dead letters         0
agent pool               2 idle / 1 busy / 3 total
containerd Create avg    18.4 ms
```

```json
[{"metric":"minga_tasks_total","labels":{"state":"running"},"value":12}]
```

JSON and YAML output is every parsed sample, not only the rows in the table.

### 5.7 `secret`

```
minga-ctl secret put <id> [--ssh <target>]... [--secret-store <dir>]
minga-ctl secret list
```

**`put`**: the value is read from stdin (empty input: exit 1). Targets are
chosen as in section 5.4. `<id>` is `<stack>_<compose secret key>`.

1. On each target run `minga secret put <id> [--secret-store <dir>]` with the
   value on its stdin.
2. `POST /v1.41/secrets/create` with `{"Name":"<id>"}` so services can refer
   to it. A 409 means it is already declared and is not an error.

The value must reach every node that can run the service; `put` reports each
target separately. A failure on any target gives exit 2 and skips step 2.

```
$ pass show acme/db | minga-ctl secret put myapp_db_password
alice@mgr1.example.net  provisioned
alice@wrk1.example.net  provisioned
secret `myapp_db_password` declared in context `prod`
```

**`list`**: `GET /v1.41/secrets`. Metadata only. It does not show which nodes
hold the value.

```
ID                  NAME                CREATED
myapp_db_password   myapp_db_password   2026-11-10T09:12:44Z
```

```json
[{"id":"myapp_db_password","name":"myapp_db_password","created":"2026-11-10T09:12:44Z"}]
```

### 5.8 `registry`

Logs nodes in to private container registries, so that the automatic pull
before a task starts can fetch private images. Node-local, like `secret put`:
the credential goes to each node over SSH on stdin, never through the API,
and minga-ctl never writes it to the workstation. With `--key-file` the key
is a file that already sits on the workstation; minga-ctl reads it and leaves
it there. Piping the credential from a secret manager avoids that copy.

It needs a Minga that reads registry credentials: `main` since PR #16, to be
released as 1.0.2. There the automatic pull reads Docker's credential file on
the node. minga-ctl maintains that file, so `login`, `logout` and `list` need
no further Minga change.

```
minga-ctl registry login <registry> [--username <user>] [--key-file <path>]
                                    [--password-stdin] [--ssh <target>]...
minga-ctl registry logout <registry> [--ssh <target>]... [--yes]
minga-ctl registry list [--ssh <target>]...
```

Targets are chosen as in section 5.4. A credential must be on every node that
can run the service; `list` shows which nodes have it.

**The credential file.** Minga reads `$HOME/.docker/config.json`, where
`$HOME` is that of the user the Minga manager or worker runs as, and looks up
`auths.<host>.auth`: the base64 of `<user>:<secret>`.

- The context field `registry_auth_file` is the path on the node. Default:
  `/root/.docker/config.json`.
- It has to be the file Minga reads: `$DOCKER_CONFIG/config.json` when the
  service sets `DOCKER_CONFIG`, else `$HOME/.docker/config.json`. Set the
  path or the service environment to match; otherwise Minga finds no file
  and pulls anonymously. minga-ctl cannot see the service's environment, so
  when the context sets no `registry_auth_file`, `login` names the default
  file it wrote and says so on stderr; the JSON result has a `file` field.
- `registry` and `image` do not call the API, so they resolve the context
  without its token and never run `token_command`.
- The SSH user must be able to read and write the file. minga-ctl does not
  call `sudo`.

**Registry names.** `<registry>` is a short name or a host. Known registries
get a hint that says which credential to create and the smallest scope that
can pull.

| You type | Key written | Username | Credential the hint asks for |
|---|---|---|---|
| `dockerhub`, `docker.io` | `docker.io` | Docker ID | Access token with the Read-only permission |
| `ghcr`, `github`, `ghcr.io` | `ghcr.io` | GitHub user name | Personal access token (classic) with only `read:packages` |
| a host ending in `-docker.pkg.dev`, or `gcr.io`, `*.gcr.io` | as typed | `_json_key`, set automatically | Service account key (JSON) with the Artifact Registry Reader role, passed with `--key-file` |
| any other `host[:port]` | as typed | asked | Whatever the registry issues, read-only if it has scopes |

Names are lowercased, and `index.docker.io` and `registry-1.docker.io` are
`docker.io`, the way Minga matches keys. `login` first removes every key that
matches the same registry (`https://index.docker.io/v1/` from `docker login`,
`GHCR.IO`), so the entry Minga uses is the new one, and `logout` removes them
all.

A host must match `^[A-Za-z0-9][A-Za-z0-9.-]*(:[0-9]+)?$` and a username
`^[A-Za-z0-9_][A-Za-z0-9._@-]*$`.

**`login`**

1. Resolve the name to a host and print the hint for that registry.
2. Username: `--username`, else a prompt. Google registries need no username.
3. Credential, first match wins:
   - `--key-file <path>`: the file's contents (Google service account key).
   - stdin, when stdin is not a terminal. `--password-stdin` is accepted so
     the `docker login` habit works; it changes nothing.
   - a prompt without echo, when stdin is a terminal.

   There is no `--password` flag. An empty credential is exit 1.
4. `GET /v1.41/version`. If Minga is older than 1.0.2, print a warning and
   continue:
   ``warning: Minga 1.0.1 in context `prod` pulls anonymously; the credential is stored but unused until the nodes run 1.0.2 or later``.
   A node built from `main` before the version is bumped also reports 1.0.1
   and gets this warning, although it does use the credential.
5. On each target:
   - Read the file: `if [ -e '<file>' ]; then cat '<file>'; fi`. A missing
     file counts as `{}`. A file that cannot be read or is not JSON stops
     that target with an error, and the file is left as it is.
   - Set `auths.<host>.auth` to the base64 of `<user>:<credential>`. Every
     other key in the file is kept.
   - Write it back with the new content on stdin, in a way that keeps the
     file readable by the Minga service:
     - The file exists: copy it with `cp -p` to `<file>.minga-ctl.tmp`, which
       keeps its mode and group (and, as root, its owner), drop access for
       others with `chmod -- o-rwx`, write the new content into the copy,
       then `mv -f` it over the file. The rename is atomic: a failed write
       never leaves the file empty or cut off, and the temporary file is
       removed. A non-root SSH user becomes the file's owner; the group and
       its access are kept, so a Minga service reading through the group
       still can. A symlinked file is replaced by a regular file.
     - The file does not exist: `umask 077; mkdir -p '<dir>'`, write the
       temporary file and rename it. The new file belongs to the SSH user
       with mode 0600, so minga-ctl prints
       ``note: created /root/.docker/config.json on alice@mgr1.example.net, owner alice, mode 0600; the Minga service must run as that user or as root to read it``.

   In a context with no SSH target the same is done on the local filesystem.
   A failure on any target gives exit 2; the other targets still run.
6. Append the audit line: host, username and targets. Never the credential.

The existing file, which can hold credentials for other registries, passes
through minga-ctl's memory during step 5. It is not written to the
workstation.

Running `login` again replaces the stored credential. It takes effect at the
next pull; nothing is restarted.

```
$ minga-ctl registry login ghcr
GitHub Container Registry (ghcr.io)
Create a personal access token (classic) with only the read:packages scope.
Username: acme-deploy
Token:
alice@mgr1.example.net  stored
alice@wrk1.example.net  stored
```

```
$ minga-ctl registry login europe-west1-docker.pkg.dev --key-file pull-sa.json
Google Artifact Registry (europe-west1-docker.pkg.dev)
Use a service account key with the Artifact Registry Reader role.
alice@mgr1.example.net  stored
alice@wrk1.example.net  stored
```

```
$ op read op://infra/dockerhub/pull-token | minga-ctl registry login dockerhub --username acmebot
```

```json
[{"node":"alice@mgr1.example.net","registry":"ghcr.io","username":"acme-deploy","stored":true}]
```

**`logout`**: asks for confirmation, then removes `auths.<host>` from the file
on each target with the same read and write steps. Other entries are kept. A
node with no entry for the host is reported as `not logged in` and is not an
error.

**`list`**: reads the file on each target and prints each host with the
username decoded from its entry. It never prints the secret part.

```
$ minga-ctl registry list
NODE                    REGISTRY                      USERNAME
alice@mgr1.example.net  ghcr.io                       acme-deploy
alice@mgr1.example.net  europe-west1-docker.pkg.dev   _json_key
alice@wrk1.example.net  ghcr.io                       acme-deploy
```

```json
[{"node":"alice@mgr1.example.net","registry":"ghcr.io","username":"acme-deploy"}]
```

In the example `wrk1` has no Google credential, so a task placed there cannot
pull from that registry.

**Gaps in what merged, and their status.** These were for Minga to close,
not minga-ctl. They were found against `main` at `b1e49ac`; the status is as
of 1.1.0.

- *Closed in 1.1.0.* The credential was on a command line: the pull ran
  `ctr -n minga images pull --user <user>:<secret> <ref>`, so every local user
  of the node could read it in the process list. Pulls now go through
  containerd's Transfer API (`ContainerdClient::pull_image` in
  `crates/minga-node/src/containerd/`), which asks Minga for the credential
  over a stream, so it never appears on a command line (section 2.2, item 9).
  On containerd older than 1.7 a pull that needs a credential fails instead.
- *Partly closed in 1.1.0.* Failures were quiet. A malformed credential file
  and a configured helper that is missing or fails are now errors. A file
  with no entry for the registry still logs one info line and pulls
  anonymously, so the first sign is a 401 from the registry.
- `minga image pull` uses the same lookup, so `login --verify <image ref>`
  can run it on each node after storing.
- *Closed in 1.0.2.* Docker Hub used a different key: `docker login` writes
  `https://index.docker.io/v1/`. Minga now treats it, `index.docker.io` and
  `registry-1.docker.io` as `docker.io`.
- *Closed in 1.1.0.* `credHelpers` and `credsStore` are read, in that order,
  before inline `auths`. minga-ctl itself still writes inline `auth` entries
  only.

A node-side `minga registry login` with its own store and a fixed path would
be a cleaner contract than a file under `$HOME`. It is not needed for this
plan.

**Limits**

- Credentials that expire within hours stop working when they expire.
  minga-ctl does not refresh them. This covers AWS ECR login passwords and
  Google access tokens (`oauth2accesstoken`). Use long-lived, read-only pull
  credentials.
- minga-ctl does not copy the workstation's own Docker logins to nodes.
  Personal Docker logins are often read-write, and copying them to production
  nodes is the wrong default.
- The file holds credentials as base64, which is an encoding, not encryption.
  File permissions are the only protection.
- `login` and `logout` read the file and write it back. Two operators changing
  the same node's file at the same moment can lose one of the two changes.

## 6. Tests and build order

**Tests**

- `tests/api.rs` starts a real `minga_api::ApiServer` on `127.0.0.1:0`, the
  pattern in `crates/minga-cli/tests/deploy_e2e.rs`, and runs the built
  `minga-ctl` against it with `MINGA_CTL_ENDPOINT`.
- Required: after `service scale`, the service in the state store equals the
  original except for the replica count. This guards the read, edit, post
  round trip; if the API's two translations are not inverse, a scale would
  restart tasks.
- Required: `scale` then `rollback` restores the original service.
- Required: `stack rm app` removes the stack's services and leaves
  `app_default` and `app_shared` in place; `--with app_default` removes it;
  `--with other_net` and `--with` of a network another stack's service uses
  are refused with nothing removed.
- Required: a `service scale` the API rejects leaves the revision file
  unchanged, so `rollback` still restores the earlier spec.
- Required: `rollback` stops when the service changes between its two reads.
- `tests/ops_loop.rs`, against a real API with a stub `minga` and `ssh`:
  `stack deploy --prune` removes only the stack's services that the file
  lacks (not those of `appx` or `other`), saves their revisions, needs
  `--yes` without a terminal before it deploys anything, prunes nothing when
  `minga deploy` fails, and `service rollback` recreates one; without
  `--prune` stderr has the note. `service rm` removes, saves the revision and
  audits; a missing service is exit 3, no terminal without `--yes` is exit 1
  with the service kept. `service inspect` shows the stubbed output of the
  failed task only, in text and JSON, skips with a note when `minga logs`
  fails or the node is unreachable over SSH, and `--quiet` runs no command.
  Not covered: typing `n` at the prompt (it needs a pseudo-terminal).
- A 401 from a server started with a token gives exit 2 and the hint text.
- `tests/wrapped.rs` sets `MINGA_BIN` to a stub script that records its
  arguments, environment and stdin, and checks `stack deploy` and
  `secret put`: the token arrives in the environment, the secret value on
  stdin, and neither in the arguments.
- `registry`, in a local context with `registry_auth_file` in a temporary
  directory: `login ghcr` creates the file with mode 0600 and
  `auths."ghcr.io".auth` equal to the base64 of `user:secret`; an existing
  entry for another host and unknown top-level keys survive; `logout` removes
  only its host; a `-docker.pkg.dev` host stores the username `_json_key`;
  a file that is not JSON is left unchanged and gives exit 2; `list` output
  contains no secret; a second `login` on a file with mode 0640 leaves the
  mode at 0640, a file with mode 0644 becomes 0640, and no temporary file is
  left; `login index.docker.io` replaces a `docker login` entry and `logout`
  of it removes it.
- Required: `service scale` to the current replica count posts nothing and
  leaves the revision file unchanged, so `rollback` still restores the spec
  from before the last real change. The same for a `service update` that
  changes nothing.
- Required: when `MINGA_BIN` succeeds but the API is unreachable for the read
  after it, `stack deploy` exits 2, and the revision is the spec from before
  this deploy, not one from an older change. A service the deploy did not
  change keeps its earlier revision.
- Required: `stack deploy` with a direct `https://` endpoint refuses the same
  compose files as with an SSH context.
- Required: with an SSH context, `stack deploy` of a compose file that uses
  a relative bind mount exits 1 before `MINGA_BIN` is run; the same file
  deploys in a local context; `env_file` and `host-gateway` are allowed in
  both.
- Unit tests: token and endpoint precedence; refusal of a config with mode
  0644 and of an unknown key; plain-HTTP refusal for a non-loopback host;
  validation and quoting of remote arguments (including a target starting
  with `-`); the ssh argument vectors; Prometheus text parsing; the 404
  split between exit 2 and exit 3; redaction in `--debug`.
- No test opens a real SSH connection. The tunnel is covered by its argument
  vector and by the direct transport sharing the same `ApiClient`.

**Build order**

1. `config.rs`, `context` commands, global flags, exit codes.
2. `api.rs` with the direct transport; `stack ls`, `stack ps`, `service ls`,
   `service ps`, `secret list`.
3. `transport.rs`: validation, tunnel, remote exec, token read.
4. `state.rs`; `service scale`, `update`, `rollback`; `stack rm`.
5. Wrapped commands: `stack deploy`, `service logs`, `image`, `secret put`.
6. `health`, `metrics`.
7. `registry`. It can be built at any time; it is useful once the nodes run
   Minga 1.0.2.

Before each commit: `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`,
`cargo test`.

## 7. Known limits

- Phase 1 has no server-side identity, roles or audit. Access control is the
  SSH account and unix group on the node.
- Every operator needs an SSH account on a node. Accounts that only read can
  be limited in `sshd_config` with `PermitOpen 127.0.0.1:2375` and no shell,
  but then `logs`, `image` and `secret put` do not work for them.
- With a direct `http://127.0.0.1` endpoint, other users of the same machine
  can reach the API too. The API token still protects it when the node
  requires one. The SSH tunnel uses private Unix sockets and has no such port.
- Rollback covers one previous revision, and only changes made with minga-ctl
  on the same machine. A deploy from CI cannot be rolled back from a laptop.
- Revision files hold full specs in clear text, including environment values.
- `service scale`, `update` and `rollback` return when the API accepts the
  spec. They do not wait for the rollout; the API reports
  `UpdateStatus.State` as `completed` at all times.
- `stack deploy --prune` and `service rm` remove services only, and keep
  one revision per service: removing a service that already has a revision
  replaces it. Removing a service removes its tasks too, so read
  `service inspect` first if you need their errors. `--prune` takes names from the compose file as
  written: it does not follow variables in service keys, and keeps a service
  behind a profile that was not enabled (section 5.2).
- `stack rm` removes services only. Networks, configs and secret metadata
  are removed when named with `--with`, because Minga keeps no record of
  which stack created them. The `<stack>_` prefix check is a guard against
  typing mistakes, not proof of ownership: a resource someone created by hand
  as `app_shared` passes it. A network given an explicit `name:` in the
  compose file is stored under that name (`network_create_to_state` sets the
  ID to the name), fails the prefix check, and cannot be removed with
  minga-ctl.
- Two operators changing the same service at the same time can overwrite each
  other without warning, because the server ignores `version`. Until the
  server rejects stale updates, changes to a service must be made one at a
  time across all clients (section 5.3).
- The API token is shared. Removing one operator's access means removing
  their SSH account, ending their sessions and rotating the token
  (section 2.2).
- If minga-ctl stops between the API accepting an update and the revision
  file being written, that change has no saved revision.
- `registry` commands have no effect on nodes that run Minga older than
  1.0.2. On 1.0.2 the credential is used, but Minga passes it to `ctr` on
  the command line, where local users of the node can read it during a pull.
  From 1.1.0 it never appears on a command line, and a pull that needs it
  requires containerd 1.7 or newer (section 5.8).
- When a container has to read the API (Traefik's Swarm provider), the API
  listens beyond loopback and the token is the only barrier between any
  container on the node and the cluster (section 2.2).
- `stack deploy` refuses compose files that use relative bind mounts
  whenever the node is another machine (an SSH context, or a
  direct endpoint that is not loopback), because Minga resolves them on the
  machine that runs the deploy (section 5.2).
- `registry login` replaces the credential file by rename. For a non-root
  SSH user that makes the user its owner; mode and group are kept
  (section 5.8).
- A task's error is one line, 300 characters at most. The runtime reports an
  exit code only: a task killed by the out-of-memory killer reads as
  `killed by signal 9`, and a process that itself exits with 137 reads the
  same. Credentials are removed by exact value, URL user info, `key=value`
  pairs and `Bearer`, `Basic` and `--user` arguments; a secret echoed in any
  other shape would pass through. Agent (WASM) tasks that end are not given a
  reason except for a failed start.
- Not included: node commands, events, waiting for rollouts, refresh of
  short-lived registry credentials, client certificates (phase 2).
