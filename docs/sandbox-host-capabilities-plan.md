# Sandbox host capabilities: plan

- Status: Steps 1 and 2 are merged (PRs #101 and #102). Steps 3–7 remain open; host capabilities are sequenced after Order 6 (E14/E15) in [agent-plane-roadmap.md](agent-plane-roadmap.md).
- Date: 2026-11-19
- Closes structural limit 1 in [architecture.md](architecture.md) and updates the implementation status in [ADR 0003](adr/0003-agent-workloads.md).

## Decisions

| Question | Decision |
|---|---|
| Guest ABI | Both: WASI preview1 core modules **and** WASI 0.2 components. The existing `run(input_len)` core contract keeps working. |
| Capabilities in scope | Env, clocks, random, stdout/stderr to logs, outbound HTTP via egress, read-only preopened dirs. |
| Default | Deny. A guest gets nothing it was not granted in `x-minga-agent`. |
| HTTP targets | The node's egress proxy only, which forwards only to its configured LLM upstream. No raw sockets, no host allowlist. |
| `token_budget` | Per task run: registered when the run starts, removed when it ends. It applies on top of the roadmap's per-tenant windowed budgets (B6). |
| Out of scope | Firecracker, module distribution, cluster-wide budget accounting, placement by node egress capability. |

## 1. Current implementation

Current main has no WASI linker or guest HTTP host function. The capability
grant schema and tenant-scoped directory-root validation are present, but
grants are not mounted or exposed to guests. The shared Wasmtime engine,
bounded module cache, epoch timeout and StoreLimits memory cap are enforced.
The egress proxy has tenant-owned named routes, policy, streaming and usage;
its per-workload budget tracking is node-local, not a distributed customer
window budget. `MINGA_EGRESS_URL` / `MINGA_EGRESS_TOKEN` are not usable by a
guest until a WASI environment is added.

Step 2 introduced the capability field in the persisted format v9. Current
main uses format v10 after subsequent tenancy changes; do not use the
historical v9 format as the current version.

## 2. Compose grammar

The optional `capabilities` block is implemented in the Compose schema. `deny_unknown_fields` remains, so a misspelt grant fails deploy validation. The grants are not linked into the guest runtime yet.

```yaml
services:
  researcher:
    x-minga-agent:
      module: researcher.wasm        # core module or component, detected from the binary
      memory_limit: 268435456        # enforced by StoreLimits
      timeout: 120                   # enforced by epoch interruption
      env:
        MODEL: claude-sonnet-5-5
      capabilities:
        grants: [env, clock, random, stdio, http] # schema-valid; not active in guests yet
        dirs:
          - host: /srv/minga/datasets/q3
            guest: /data             # schema-valid; not mounted yet
```

The Compose parser and node validate the shape, without linking the grants:

- `grants` values: `env`, `clock`, `random`, `stdio`, `http`. Anything else is an error.
- Without `env`, the `env:` map is not exposed. Declaring `env:` without the grant is an error.
- `dirs[].host` must be absolute with no `..`; `guest` must be absolute and unique. There is no write mode.
- The node canonicalizes and checks directory roots under `minga worker --agent-dir-root PATH`, and requires the tenant-specific subdirectory. No root means no dirs are accepted. These checks do not mount a guest filesystem yet.
- The `http` grant is accepted by the schema but has no guest host function yet; the planned runtime must require node egress before starting an HTTP-enabled run.
- `egress_url` remains accepted for compatibility and is informational only.

## 3. State and transport

- The capability field was added in v9; current main's persisted format is v10 after later tenancy migrations. Legacy formats migrate through the documented current migration path; wire frames still require a coordinated cluster upgrade. Never edit frozen fixtures.
- The spec travels through the Docker API in the `minga.agent` label. `deploy_e2e::deploy_keeps_restart_policy_and_agent_spec` covers the round trip.
- Nothing new goes through Raft beyond the spec itself. Run output and logs stay on JetStream / the node.

## 4. Runtime (minga-sandbox)

### 4.1 Engine and limits

- Upgrade `wasmtime` to the latest 50.x and add `wasmtime-wasi` and `wasmtime-wasi-http` at the same version in `[workspace.dependencies]`. Handle API churn from 24 in the same change.
- Wasmtime's default features include async APIs for later WASI integration; step 1's core execution remains synchronous. One shared `Engine` per node (fuel, epoch interruption), held by `WasmtimeAgentExecutor`, plus a bounded 64-entry `Module`/`Component` cache keyed by module sha256.
- Timeout: a node-wide 10 ms ticker calls `engine.increment_epoch()`; each store sets an epoch deadline from `timeout`. Expiry traps the guest and frees its blocking thread. Test: an infinite loop with no fuel limit ends within timeout + one tick.
- Memory: `StoreLimitsBuilder::memory_size(memory_limit)` with `trap_on_grow_failure`. Default when unset is 64 MiB. The runtime does not grow core-module memory; the module declares enough minimum memory for its input and output.
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
| 1 | **Complete (PR #101):** Wasmtime upgrade, shared engine + bounded module cache, epoch timeout and enforced `StoreLimits` memory. | none |
| 2 | **Complete (PR #102):** Compose `capabilities` grammar, `AgentSpec` field, translation, label round trip, state migration, node-side validation and `--agent-dir-root`. | none |
| 3 | Core-module path: preview1 linker, `WasiCtx` from grants, stdio capture. | 1, 2 |
| 4 | Component path: WASI 0.2 linker, `wasi:cli/run`, same grants. | 3 |
| 5 | HTTP for both paths, host-attached token, per-run cap. | 3, 4, roadmap orders 1–2 |
| 6 | Agent logs through the existing logs surfaces. | 3 |
| 7 | Docs and runnable examples for the implemented host capabilities. | 5, 6 |

Steps 1 and 2 are complete. Steps 3–7 remain; VM proof is not part of this work until release readiness.

### Test fixtures

- Core modules: written in WAT and compiled in tests with the `wat` crate, as today.
- Components: one small Rust guest crate under `crates/minga-sandbox/tests/guests/` (outside the workspace), built by a script into a checked-in `.wasm` with its sha256 recorded, so CI needs no wasm toolchain. The script is the only way to update it.
- Each grant gets a pair of tests: works when granted, denied when not. HTTP tests run against a local proxy with a fake upstream and assert the guest never sees the token.

## 8. Risks

- **wasmtime upgrade size.** Step 1 upgrades 24 to stable 50.x, which requires Rust 1.97; the workspace MSRV is raised accordingly. The release workflow already builds with Rust 1.99.0.
- **Timeout scope.** Epoch interruption stops guest code, not a blocked host call. WASI host operations must be asynchronous and bounded by an external task deadline.
- **Toolchain friction.** Rust's `wasm32-wasip1` std needs random at startup; a module built that way traps without the `random` grant. The error must name the missing grant.
- **No egress-aware placement.** An `http` agent placed on a node without egress fails at start, clearly, but is not rescheduled elsewhere. Operators use node labels and constraints until placement learns about it.
- **Budgets are per node, per run.** A service with many replicas or restarts can spend more than `token_budget` in total. This is the decision taken, and the docs must state it.
