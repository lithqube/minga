# minga-ctl: design plan

Status: proposed, 2026-11-11. Nothing here is implemented yet.

`minga-ctl` is a companion CLI for Minga: named contexts for several
environments, stack and service operations, health, metrics and release
management. It is a separate binary from `minga` and works for any site that
runs Minga.

This plan is security-first. Phase 1 needs no change to Minga and is specified
fully enough to implement, with one exception: registry login (section 5.8)
needs a small change on the node side of Minga first. Phase 2 is server work
and is outlined; it needs its own ADR before it starts.

## 1. What the code allows today

The design follows what Minga serves, not what the Docker Engine API has in
general. All routes are registered in `crates/minga-api/src/server.rs:91-136`.

| Finding | Where | Effect on minga-ctl |
|---|---|---|
| Routes exist for version, info, nodes, services, tasks, networks, secrets, configs. There are none for logs, images, containers, events. Unregistered paths return 404 `{"message":"endpoint not implemented by Minga"}`. | `server.rs:91-136`, `handlers.rs:813` | Logs and image commands cannot be HTTP calls. |
| Every route except `/_ping`, `/healthz`, `/readyz`, `/metrics` needs a version prefix. The prefix is captured and not checked. | `server.rs:93-130` | minga-ctl always uses `/v1.41`. |
| The listener is plain HTTP, default `0.0.0.0:2375`. No TLS, no unix socket. | `server.rs:163`, `main.rs:319` | See section 2. |
| Auth is one shared bearer token, checked on every route including `/metrics`. Failure is 401 `{"message":"authentication required"}`. | `server.rs:49-71` | No per-user identity, roles or audit on the server. |
| Service update takes a full spec. The `version`, `registryAuthFrom` and `rollback` query parameters are ignored. | `handlers.rs:416-465` | Scale and update are read, edit, post back. |
| The server keeps one previous spec only while a rollout is in flight and never returns it. | `handlers.rs:441-456`, `Service.previous` in `minga-state/src/types.rs` | Rollback needs its own revision store. |
| Tasks have a state, not a health field. A task with a healthcheck stays `preparing` until its first probe passes. | `minga-api/src/translate.rs:689-697`, `minga-node/src/supervisor.rs:862-870` | Health is derived from state. |
| Service IDs are `<stack>.<service>`. Networks, secrets and configs are `<stack>_<name>`. A stack is only the label `com.docker.stack.namespace`. | `minga-scheduler/src/translate.rs` (`{stack}.{name}`, `{stack}_{name}`) | Service arguments are `<stack>.<service>`, not `<stack>_<service>`. |
| The task `label` filter matches only the stack label. The network `label` filter always returns an empty list. | `handlers.rs:519-531`, `:574` | Stack networks are found by ID prefix. |
| `/healthz`, `/readyz` and `/metrics` are plain text on the API port. `/readyz` returns 503 with a reason. | `minga-telemetry/src/lib.rs:15-41` | Parsed as text. |
| The API process updates only `minga_raft_leader`. Task and scheduler metrics live in the manager process, served on `--metrics-bind`. | `server.rs:175-185`, `main.rs:278-281` | A context can name a separate metrics endpoint. |
| `minga logs` reads NATS JetStream. Plain NATS is allowed only to loopback. | `main.rs:1670-1719`, `minga-cli/src/bus.rs:68-83` | Logs run on the node. |
| `minga image pull` runs `ctr` on the machine it is called on. There is no `image ls` or `image rm`. | `main.rs:1740-1770` | Image commands run on the node. |
| `minga secret put` reads stdin and writes a file in the local secret store. The API stores secret metadata only and rejects a non-empty `Data`. | `main.rs:624-647`, `handlers.rs:673` | Secret values never use the API. |
| Image pulls are anonymous. `minga image pull` and the automatic pull before a task starts both run `ctr -n minga images pull <ref>` with no credentials. | `main.rs:1744`, `minga-node/src/containerd/runtime.rs:483` | Registry login needs a Minga change (section 5.8). |

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

- `minga api --api-bind 127.0.0.1:2375 --api-token …` (the default bind is
  `0.0.0.0:2375`; change it).
- `--metrics-bind 127.0.0.1:9100` on the manager.
- Operators have their own SSH accounts and are members of one unix group, for
  example `minga-operators`.
- The API token is in a file owned by root, group `minga-operators`, mode 0640,
  for example `/etc/minga/api-token`.

**Client behaviour:**

1. A context names an SSH target. For API calls minga-ctl starts the system
   `ssh` client with a local forward and talks HTTP to the forwarded port:

   ```
   ssh -N -o ExitOnForwardFailure=yes \
       -L 127.0.0.1:<free port>:127.0.0.1:2375 \
       [-L 127.0.0.1:<free port 2>:127.0.0.1:9100] -- <target>
   ```

   It waits until the local port accepts a connection (10 s limit) or the
   child exits, and kills the child when the command ends (`kill_on_drop`).
2. Keys, agent, host-key checking, jump hosts and MFA belong to the operator's
   ssh configuration. minga-ctl never reads keys, never sets
   `StrictHostKeyChecking`, and does not set `BatchMode`, so interactive MFA
   prompts still work. Without a terminal ssh fails instead of prompting.
   A `ControlMaster` entry in `~/.ssh/config` avoids repeated prompts.
3. The API token is read from the node when the tunnel starts
   (`ssh -- <target> cat '<remote_token_file>'`), kept in memory and never
   written to the workstation.
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
   in arguments, logs or the audit log, and never written to the workstation.
10. `--debug` prints method, path and status, and ssh command lines, to
    stderr. The `Authorization` header is printed as `Bearer <redacted>`.
11. Commands that remove or replace things (`stack rm`, `image rm`,
    `service rollback`, `registry logout`) list what will change and ask for
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

**Revoking an operator.** The API token is shared, and anyone who can read the
token file can keep a copy; minga-ctl not storing it is a habit of the tool,
not a guarantee. Removing an operator from the unix group stops further reads
of the file and nothing else: with SSH access and a retained token they can
still forward the API port and use it. Revocation is therefore two steps:
remove the SSH account (or its port forwarding), and rotate the token on the
node. Phase 2 replaces the shared token with per-operator certificates.

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
| CC6.1 logical access | API reachable only on node loopback through SSH; no credentials stored on workstations | Client certificate per operator | SSH key issuance, MFA, device security |
| CC6.2, CC6.3 provisioning and least privilege | Access is an SSH account on a node. Group membership only controls who can read the token file. Revoking an operator means removing the SSH account (or its port forwarding) and rotating the shared token. No roles. | `viewer`, `deployer`, `admin`; short-lived certificates | Joiner and leaver process, access reviews |
| CC6.6 boundary protection | API and metrics on loopback; client refuses plain HTTP off-host | TLS listener | Firewalls, bastion hosts |
| CC6.7 data in transit | All remote traffic inside SSH; secret values on stdin only | TLS on the API | Key management |
| CC7.2 monitoring | sshd logs (who, when, from where) plus the client audit log (what) | Server audit line per change, tied to the certificate identity | Log shipping, retention, alerting |
| CC8.1 change management | Deploys from a compose file; revision saved before each change; rollback; audit line per change | Server-side revisions | Review and approval, CI gates |

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
    /// SSH targets for node-local commands (image, secret put).
    #[serde(default)]
    pub nodes: Vec<String>,
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
| `MINGA_BIN` | Path of the local `minga` binary. |

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
| `minga` missing | ``error: `minga` not found on PATH (needed for `stack deploy`)`` then ``hint: install minga or set MINGA_BIN`` | 1 |
| Config too open | ``error: ~/.config/minga-ctl/config.yaml is readable by group or others (mode 0644)`` then ``hint: chmod 600 ~/.config/minga-ctl/config.yaml`` | 1 |
| Unknown config key | ``error: ~/.config/minga-ctl/config.yaml: unknown field `api_token` `` then ``hint: tokens are not stored in the config; use remote_token_file or token_command`` | 1 |
| No terminal for a prompt | ``error: `stack rm` needs confirmation; pass --yes to run without a terminal`` | 1 |
| No registry credential given | ``error: no credential on stdin for ghcr.io`` then ``hint: run in a terminal to be prompted, or pipe the token to stdin`` | 1 |
| Node's Minga too old | ``error: `minga` on alice@mgr1.example.net has no `registry` command`` then ``hint: registry login needs a newer Minga on the node (see docs/minga-ctl-plan.md, section 5.8)`` | 2 |

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
                             [--nats-url <url>] [--node <target>]... [--use]
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
minga-ctl stack deploy -c <file> [-p <profile>]... [--wait <secs>] <stack>
minga-ctl stack ls
minga-ctl stack ps <stack>
minga-ctl stack rm <stack> [--with <id>]... [--yes]
```

**`deploy`** (`-c` default `compose.yaml`, `--wait` default `30`)

1. `GET /v1.41/services?filters={"label":["com.docker.stack.namespace=<stack>"]}`
   and keep each existing service's `Spec` in memory.
2. Run `minga deploy -c <file> [-p …] --wait <secs> --api-url <endpoint> <stack>`
   with `MINGA_API_TOKEN` in the child's environment. With SSH the endpoint is
   the tunnel's local URL. Output passes through.
3. Read the services again, whether `minga` succeeded or not. For each service
   whose `Spec` now differs from step 1, save the step 1 spec as its revision
   file. A service the deploy did not change, or did not reach before failing,
   keeps the revision file it had.
4. Exit code: 0 if `minga` exits 0, else 2.

`minga deploy` validates the compose file itself, and unknown keys fail there.

```
$ minga-ctl stack deploy -c compose.yaml myapp
<output of minga deploy>
saved revision for myapp.web (myapp.worker unchanged)
```

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

```
ID                      SERVICE       NODE   DESIRED   STATE      HEALTH
myapp.web.1.3f9a1c2e    myapp.web     mgr1   running   running    healthy
myapp.web.2.3f9a1c2e    myapp.web     wrk1   running   preparing  starting
myapp.worker.1.77b0d4aa myapp.worker  wrk1   running   running    -
```

```json
[{"id":"myapp.web.1.3f9a1c2e","service":"myapp.web","node":"mgr1","desired":"running","state":"running","health":"healthy"}]
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
minga-ctl service logs <service> [-f] [--tail <n>]
minga-ctl service scale <service> --replicas <n>
minga-ctl service update <service> [--image <ref>] [--env-add <KEY=VALUE>]...
                                   [--env-rm <KEY>]... [--replicas <n>]
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
`GET /v1.41/tasks?filters={"service":["<id>"]}`. Same columns as `stack ps`.

**`logs`** (`--tail` default `100`, `0` means all)

1. `GET /v1.41/tasks?filters={"service":["<id>"],"desired-state":["running"]}`.
2. For each task run `minga logs <task id> --tail <n> [-f] --nats-url <url>`:
   on the context's SSH target when it has one, else locally. `<url>` is the
   context's `nats_url`, else `nats://127.0.0.1:4222`.
3. Each line is printed with the task ID in front. stderr chunks go to stderr.
   With `-f` the children run until interrupted.

There is no JSON form; `--output` is ignored. If NATS on the node needs a
token, `MINGA_NATS_TOKEN` must be set in the SSH user's environment there;
minga-ctl does not carry NATS credentials.

```
$ minga-ctl service logs myapp.web --tail 2
myapp.web.1.3f9a1c2e | listening on :8080
myapp.web.2.3f9a1c2e | listening on :8080
```

**`scale`**

1. `GET /v1.41/services/{id}`. If `Spec.Mode` has no `Replicated`, exit 1:
   ``error: service `shop.proxy` is global and cannot be scaled``.
2. Set `Spec.Mode.Replicated.Replicas` in a copy of the spec and
   `POST /v1.41/services/{id}/update?version=<Version.Index>` with the whole
   spec. The server ignores `version` today; it is sent so the call stays
   correct if the server starts checking it.
3. Only after the API accepts the update, save the spec read in step 1 as the
   revision file. If the POST fails, the revision file is left as it was, so
   a rejected change never replaces the revision you can roll back to.

```
$ minga-ctl service scale myapp.web --replicas 5
myapp.web scaled from 3 to 5
```

```json
{"service":"myapp.web","replicas":{"from":3,"to":5}}
```

**`update`**: same sequence as `scale`: read, edit, post, then save the
revision. At least one flag is required.

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

```
$ minga-ctl service update myapp.web --image ghcr.io/acme/web:1.4.3
myapp.web updated: image ghcr.io/acme/web:1.4.2 -> ghcr.io/acme/web:1.4.3
```

**`rollback`**

1. Read the revision file. Missing: exit 3.
2. `GET /v1.41/services/{id}` (404: exit 3).
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
Pulls are anonymous unless the node holds a credential for the image's
registry (section 5.8).

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

Logs nodes in to private container registries, so that both
`minga-ctl image pull` and the automatic pull before a task starts can fetch
private images. Node-local, like `secret put`: the credential goes to each
node over SSH on stdin, never through the API, and is never stored on the
workstation.

```
minga-ctl registry login <registry> [--username <user>] [--key-file <path>]
                                    [--password-stdin] [--verify <image ref>]
                                    [--ssh <target>]...
minga-ctl registry logout <registry> [--ssh <target>]... [--yes]
minga-ctl registry list [--ssh <target>]...
```

Targets are chosen as in section 5.4. A credential must be on every node that
can run the service; `list` shows which nodes have it.

**Registry names.** `<registry>` is a short name or a host. Known registries
get a hint that says which credential to create and the smallest scope that
can pull.

| You type | Host stored | Username | Credential the hint asks for |
|---|---|---|---|
| `dockerhub`, `docker.io` | `docker.io` | Docker ID | Access token with the Read-only permission |
| `ghcr`, `github`, `ghcr.io` | `ghcr.io` | GitHub user name | Personal access token (classic) with only `read:packages` |
| a host ending in `-docker.pkg.dev`, or `gcr.io`, `*.gcr.io` | as typed | `_json_key`, set automatically | Service account key (JSON) with the Artifact Registry Reader role, passed with `--key-file` |
| any other `host[:port]` | as typed | asked | Whatever the registry issues, read-only if it has scopes |

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
4. On each target run `minga registry login <host> --username <user>` with the
   credential on its stdin. Each target is reported separately; a failure on
   any target gives exit 2.
5. With `--verify <image ref>`, run `minga image pull <ref>` on each target
   and report the result. The reference's host must be the registry just
   logged in to, else exit 1 before anything is stored.
6. Append the audit line: host, username and targets. Never the credential.

Running `login` again replaces the stored credential. It takes effect at the
next pull; nothing is restarted.

```
$ minga-ctl registry login ghcr --verify ghcr.io/acme/web:1.4.2
GitHub Container Registry (ghcr.io)
Create a personal access token (classic) with only the read:packages scope.
Username: acme-deploy
Token:
alice@mgr1.example.net  stored    pull ok
alice@wrk1.example.net  stored    pull ok
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
[{"node":"alice@mgr1.example.net","registry":"ghcr.io","username":"acme-deploy","stored":true,"verified":true}]
```

**`logout`**: asks for confirmation, then runs `minga registry logout <host>`
on each target. A node that has no credential for the host is reported as
`not logged in` and is not an error.

**`list`**: runs `minga registry ls` on each target. It shows no secrets.

```
$ minga-ctl registry list
NODE                    REGISTRY                      USERNAME      UPDATED
alice@mgr1.example.net  ghcr.io                       acme-deploy   2026-11-11T14:02:10Z
alice@mgr1.example.net  europe-west1-docker.pkg.dev   _json_key     2026-11-11T14:05:41Z
alice@wrk1.example.net  ghcr.io                       acme-deploy   2026-11-11T14:02:11Z
```

```json
[{"node":"alice@mgr1.example.net","registry":"ghcr.io","username":"acme-deploy","updated":"2026-11-11T14:02:10Z"}]
```

In the example `wrk1` has no Google credential, so a task placed there cannot
pull from that registry.

**What Minga must add first.** minga-ctl only carries the credential to the
node. Minga has to store it and use it:

- `minga registry login <host> --username <user> [--registry-auth-dir <dir>]`
  reads the credential from stdin, the same way `minga secret put` does.
- `minga registry logout <host>` and `minga registry ls`. `ls` prints one line
  per registry, `host<TAB>username<TAB>updated_at`, and never the credential.
- Storage: one file per host in `/var/lib/minga/registry-auth` (directory 0700,
  files 0600, written to a temporary file and renamed).
  `SecretStore::put` in `crates/minga-node/src/secrets.rs` already writes
  files this way and can be pointed at that directory. It is a separate
  directory from the secret store so a registry credential can never be
  mounted into a task as a Compose secret. Record:

  ```json
  {"host":"ghcr.io","username":"acme-deploy","secret":"…","updated_at":"2026-11-11T14:02:10Z"}
  ```
- Both pull call sites (`crates/minga-cli/src/main.rs:1744` and
  `crates/minga-node/src/containerd/runtime.rs:483`) look up the host of the
  image reference and authenticate when a credential is stored.
  `index.docker.io` and `registry-1.docker.io` are treated as `docker.io`.
- The credential must not appear in a command line, in another process's
  environment, or in a log line. `ctr images pull --user <user>:<secret>` puts
  it on the command line, where every local user of the node can read it while
  the pull runs, so it is not acceptable as it stands.

**Open point.** How the pull authenticates without exposing the credential is
not decided here. It belongs to the Minga change and has to be checked against
the containerd version Minga pins. Candidates: answer `ctr`'s password prompt
from the parent process, or pull through containerd's gRPC API, which Minga
already uses to run containers.

Until a node runs a Minga with these commands, `registry` commands fail with
the "Node's Minga too old" error from section 4.4.

**Limits**

- Credentials that expire within hours stop working when they expire.
  minga-ctl does not refresh them. This covers AWS ECR login passwords and
  Google access tokens (`oauth2accesstoken`). Use long-lived, read-only pull
  credentials.
- There is no import from `~/.docker/config.json`. Personal Docker logins are
  often read-write, and copying them to production nodes is the wrong default.
- The credential is stored in clear text on the node, protected by file
  permissions, as node-local secrets are.

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
- A 401 from a server started with a token gives exit 2 and the hint text.
- `tests/wrapped.rs` sets `MINGA_BIN` to a stub script that records its
  arguments, environment and stdin, and checks `stack deploy` and
  `secret put`: the token arrives in the environment, the secret value on
  stdin, and neither in the arguments.
- `tests/wrapped.rs` also checks `registry login`: the credential arrives on
  the stub's stdin and in neither its arguments nor its environment; `ghcr`
  resolves to `ghcr.io`; a `-docker.pkg.dev` host sends the username
  `_json_key`; `--verify` with a reference on another host exits 1 before
  the stub is called.
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
7. `registry`, once the Minga change in section 5.8 has landed.

Before each commit: `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`,
`cargo test`.

## 7. Known limits

- Phase 1 has no server-side identity, roles or audit. Access control is the
  SSH account and unix group on the node.
- Every operator needs an SSH account on a node. Accounts that only read can
  be limited in `sshd_config` with `PermitOpen 127.0.0.1:2375` and no shell,
  but then `logs`, `image` and `secret put` do not work for them.
- While a command runs, the forwarded local port is reachable by other users
  of the same workstation. The API token still protects it when the node
  requires one.
- Rollback covers one previous revision, and only changes made with minga-ctl
  on the same machine. A deploy from CI cannot be rolled back from a laptop.
- Revision files hold full specs in clear text, including environment values.
- `service scale`, `update` and `rollback` return when the API accepts the
  spec. They do not wait for the rollout; the API reports
  `UpdateStatus.State` as `completed` at all times.
- `stack rm` removes services only. Networks, configs and secret metadata
  are removed when named with `--with`, because Minga keeps no record of
  which stack created them. The `<stack>_` prefix check is a guard against
  typing mistakes, not proof of ownership: a resource someone created by hand
  as `app_shared` passes it. A network given an explicit `name:` in the
  compose file is stored under that name (`network_create_to_state` sets the
  ID to the name), fails the prefix check, and cannot be removed with
  minga-ctl.
- Two operators changing the same service at the same time can overwrite each
  other without warning, because the server ignores `version` (section 5.3).
- The API token is shared. Removing one operator's access means removing
  their SSH account and rotating the token (section 2.2).
- If minga-ctl stops between the API accepting an update and the revision
  file being written, that change has no saved revision.
- `registry` commands do nothing useful until Minga stores registry
  credentials and uses them when pulling (section 5.8). How the pull
  authenticates without exposing the credential is still open.
- Not included: node commands, events, waiting for rollouts, refresh of
  short-lived registry credentials, client certificates (phase 2).
