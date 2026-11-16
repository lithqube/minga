# Sandbox host capabilities: plan

- Status: Draft. Scheduled after orders 1–2 of [agent-plane-roadmap.md](agent-plane-roadmap.md), because every agent we surveyed runs as a container, and container egress, budgets and metering come first.
- Date: 2026-11-16
- Branch: `feat/sandbox-host-capabilities`
- Closes structural limit 4 in [architecture.md](architecture.md) and the "Missing" items in [ADR 0003](adr/0003-agent-workloads.md).

## Decisions

| Question | Decision |
|---|---|
| Guest ABI | Both: WASI preview1 core modules **and** WASI 0.2 components. The existing `run(input_len)` core contract keeps working. |
| Capabilities in scope | Env, clocks, random, stdout/stderr to logs, outbound HTTP via egress, read-only preopened dirs. |
| Default | Deny. A guest gets nothing it was not granted in `x-minga-agent`. |
| HTTP targets | The node's egress proxy only, which forwards only to its configured LLM upstream. No raw sockets, no host allowlist. |
| `token_budget` | Per task run: registered when the run starts, removed when it ends. It applies on top of the roadmap's per-tenant windowed budgets (B6). |
| Out of scope | Firecracker, module distribution, cluster-wide budget accounting, placement by node egress capability. |

## 1. What the code does today

- `run_wasm` instantiates with `Instance::new(&mut store, module, &[])`: no imports, so any WASI module fails to instantiate (`crates/minga-sandbox/src/wasmtime_rt.rs`).
- **The timeout does not stop the guest.** `tokio::time::timeout` wraps `spawn_blocking`; on expiry the caller gets `Timeout` but the wasm thread keeps running until fuel runs out, or forever without a fuel limit.
- `memory_limit` is copied into `SandboxSpec` and never read. The runtime even grows memory itself to `MIN_MEMORY_PAGES`.
- `MINGA_EGRESS_URL` and `MINGA_EGRESS_TOKEN` are put in an env the guest cannot read (`crates/minga-node/src/agent.rs`). Once env is granted, the guest could read the token.
- The egress proxy registers no budgets on a node, so every request gets 429. Its response cache is keyed by method, path and body, not workload (`crates/minga-egress/src/proxy.rs`).
- A fresh `WasmtimeRuntime` (and `Engine`) is built per task, so nothing is cached between runs.
- wasmtime is pinned at 24. Stable `wasmtime-wasi` / `wasmtime-wasi-http` are on the 50.x line (51 is in RC).

## 2. Compose grammar

New optional `capabilities` block under `x-minga-agent`. `deny_unknown_fields` stays, so a misspelt grant fails the deploy.

```yaml
services:
  researcher:
    x-minga-agent:
      module: researcher.wasm        # core module or component, detected from the binary
      memory_limit: 268435456        # now enforced
      timeout: 120                   # now stops the guest
      token_budget: 50000            # per run, at the node's egress proxy
      env:
        MODEL: claude-sonnet-5-5
      capabilities:
        grants: [env, clock, random, stdio, http]
        dirs:
          - host: /srv/minga/datasets/q3
            guest: /data             # always read-only
```

Rules, checked in `minga-compose` and again on the node:

- `grants` values: `env`, `clock`, `random`, `stdio`, `http`. Anything else is an error.
- Without `env`, the `env:` map is not exposed. Declaring `env:` without the grant is an error, not a silent drop.
- `dirs[].host` must be absolute with no `..`; `guest` must be absolute and unique. There is no write mode.
- The node only mounts dirs under roots the operator allows (`minga worker --agent-dir-root PATH`, repeatable). The path is canonicalised before the prefix check, so a symlink cannot escape. No root configured means no dirs.
- `http` needs egress on the node. A run that asks for it on a node without `--egress-bind` fails at start with that reason in `Task.last_error`.
- `egress_url` stays accepted so existing files deploy unchanged; it is informational only and the docs say so.

## 3. State and transport

- `AgentSpec` gains `capabilities: AgentCapabilities { grants: BTreeSet<Grant>, dirs: Vec<DirGrant> }` with `#[serde(default)]`. No format-version bump. Add a new frozen fixture in `crates/minga-state/tests/fixtures/` for the new shape; old fixtures must still load as "no grants".
- The spec travels through the Docker API in the `minga.agent` label. Extend `deploy_e2e::deploy_keeps_restart_policy_and_agent_spec` to round-trip the capabilities.
- Nothing new goes through Raft beyond the spec itself. Run output and logs stay on JetStream / the node.

## 4. Runtime (minga-sandbox)

### 4.1 Engine and limits

- Upgrade `wasmtime` to the latest 50.x and add `wasmtime-wasi` and `wasmtime-wasi-http` at the same version in `[workspace.dependencies]`. Handle API churn from 24 in the same change.
- One shared `Engine` per node (async support, fuel, **epoch interruption**), held by `WasmtimeAgentExecutor`, plus a `Module`/`Component` cache keyed by the module's sha256.
- Timeout: a node-wide ticker calls `engine.increment_epoch()`; each store sets an epoch deadline from `timeout`. Expiry traps the guest and frees the thread. Test: an infinite loop with no fuel limit ends within timeout + one tick.
- Memory: `StoreLimitsBuilder::memory_size(memory_limit)` with `trap_on_grow_failure`. Default when unset stays at a fixed node default (64 MiB) instead of unlimited. Remove the runtime's own `memory.grow`; the `run` contract states the module's minimum memory instead.
- Stdio: bounded pipes (`MemoryOutputPipe` with a cap, 1 MiB default), drained into the task's log buffer.

### 4.2 Two guest paths

Detect by the binary's layer field (`wasmparser::Parser::is_component`).

| | Core module | Component |
|---|---|---|
| WASI | `wasmtime_wasi::preview1` linked into a `Linker<SandboxCtx>` | `wasmtime_wasi::add_to_linker_async` |
| Entry | `run(input_len)` contract as today, else `_start` | `wasi:cli/run`, input on stdin |
| HTTP | `minga:egress/request` host function (below) | `wasi:http/outgoing-handler`, our `send_request` hook |

Both paths build the same `WasiCtx` from the grants: `env` adds only the declared map plus `MINGA_TASK_ID` and `MINGA_SERVICE`; `clock` and `random` are the WASI defaults, otherwise stub clocks that return 0 and a random source that traps; `stdio` attaches the pipes; each dir is `preopened_dir(host, guest, DirPerms::READ, FilePerms::READ)`. No sockets: `allow_tcp(false)`, `allow_udp(false)`, `allow_ip_name_lookup(false)` on every store.

### 4.3 Outbound HTTP

- The guest gets `MINGA_EGRESS_URL=http://egress.minga.internal`, a fixed name that does not resolve anywhere. The host accepts only that authority, rewrites it to the local proxy address, and returns an error to the guest for any other host.
- **Workload identity is host-injected.** The host adds `X-Minga-Sandbox-Token` to each request. The token is never in the guest's env, so a guest can no longer read it and hand it on. `MINGA_EGRESS_TOKEN` is dropped from the wasm path (kept for Firecracker later).
- `minga:egress/request` for core modules is one host function: method, path, headers and body in guest memory, status and body written back, with request and response size caps. Its WIT lives in `crates/minga-sandbox/wit/` so both paths document the same shape.
- Request body, response body and request count per run are capped (defaults 1 MiB, 8 MiB, 256).

## 5. Egress proxy (minga-egress)

The proxy changes move to the roadmap and land first, for containers and agents alike: the cache key per workload (A4), streaming (A5), named routes (A2) and budgets (B6). This plan adds only:

- `SandboxTokens::issue` also registers a per-run cap of `token_budget`, and revoke removes it. A run with no `token_budget` has no per-run cap.
- The wasm HTTP host functions (§4.3) target whichever route the service is allowed to use (A3).

## 6. Logs

- `AgentExecutor` gains `logs(task_id, tail)` backed by a bounded ring buffer per task, kept after the run ends until the task is removed.
- Serve it wherever container logs are served for the task (Docker API logs endpoint and `minga-ctl service logs`). Find the exact call site in step 6; agents must not need a separate command.

## Tenancy (ADR 0008 checklist)

1. **Owner.** `capabilities` is part of the tenant-owned `AgentSpec`. The module cache and directory roots are local to the node, and a node serves one tenant.
2. **Access.** Grants are set by a `deployer` of the owning tenant. `--agent-dir-root` is node configuration that cluster operators set when the tenant's node joins.
3. **Bus.** Logs and run results go to `MINGA_LOGS_<tenant>` and `MINGA_STATUS_<tenant>` under the tenant's subjects (ADR 0008 §7).
4. **Node.** The wasm engine, module cache and epoch ticker are per node, so a single tenant by construction. The ticker and engine are never shared across nodes.
5. **Shared plane.** Runs count against the tenant's task quota.
6. **Audit.** A grant change is a service update, and is already audited with `tenant`.
7. **Test.** Two-tenant test: tenant B's spec naming tenant A's module path or dir root is refused, because B's node has neither.

## 7. Delivery

One PR per step, each green on `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.

| Step | Content | Depends on |
|---|---|---|
| 1 | wasmtime upgrade, shared engine + module cache, epoch timeout, `StoreLimits` memory. Fixes the runaway-timeout bug on its own. | none |
| 2 | Compose `capabilities` grammar, `AgentSpec` field, translation, label round trip, state fixture, node-side validation, `--agent-dir-root`. | none |
| 3 | Core-module path: preview1 linker, `WasiCtx` from grants, stdio capture. | 1, 2 |
| 4 | Component path: WASI 0.2 linker, `wasi:cli/run`, same grants. | 3 |
| 5 | HTTP for both paths, host-attached token, per-run cap. | 3, 4, roadmap orders 1–2 |
| 6 | Agent logs through the existing logs surfaces. | 3 |
| 7 | Docs: update ADR 0003 status, architecture.md limit 4, cli.md flags, example compose. | 5, 6 |

Steps 1 and 2 can run in parallel.

### Test fixtures

- Core modules: written in WAT and compiled in tests with the `wat` crate, as today.
- Components: one small Rust guest crate under `crates/minga-sandbox/tests/guests/` (outside the workspace), built by a script into a checked-in `.wasm` with its sha256 recorded, so CI needs no wasm toolchain. The script is the only way to update it.
- Each grant gets a pair of tests: works when granted, denied when not. HTTP tests run against a local proxy with a fake upstream and assert the guest never sees the token.

## 8. Risks

- **wasmtime upgrade size.** 24 to 50 is a big jump; step 1 isolates it so the rest stays reviewable. It will also probably raise the workspace `rust-version` (1.80 today), so check wasmtime's MSRV first.
- **Toolchain friction.** Rust's `wasm32-wasip1` std needs random at startup; a module built that way traps without the `random` grant. The error must name the missing grant.
- **No egress-aware placement.** An `http` agent placed on a node without egress fails at start, clearly, but is not rescheduled elsewhere. Operators use node labels and constraints until placement learns about it.
- **Budgets are per node, per run.** A service with many replicas or restarts can spend more than `token_budget` in total. This is the decision taken, and the docs must state it.
