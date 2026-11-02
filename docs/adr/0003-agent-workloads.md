# ADR 0003: Agent workloads — WASM sandboxes + LLM egress in the orchestration pipeline

- Status: Accepted
- Date: 2026-10-26

## Context

Minga's design treats AI-agent workloads as first-class objects — not an afterthought bolted onto a container runtime. By Wave 6, two pieces existed in isolation:

- **`minga-sandbox`** — `SandboxRuntime` trait with `WasmtimeRuntime` (real WASM execution, fuel metering, memory limits, timeouts, `WarmPool`) and a `FirecrackerRuntime` stub.
- **`minga-egress`** — axum LLM reverse proxy with per-workload token budgets, LRU response caching, and secret redaction.

But neither was wired into the orchestration pipeline. A Compose service could not declare an agent workload, the scheduler did not know about `WorkloadKind::Agent`, and the supervisor dispatched everything through `Runtime::start()` (containerd). Agent sandboxes and egress control were library crates without a path from `compose.yaml` to execution.

## Decision

Introduce a **two-kind workload model** that flows end-to-end through every layer:

```
compose.yaml (x-minga-agent)
  → minga-compose  (AgentExtension parsed from x- key)
  → minga-scheduler (translate → TaskSpec.kind = WorkloadKind::Agent)
  → minga-state     (Raft stores TaskSpec with kind discriminant)
  → minga-bus       (assign/stop commands carry the full TaskSpec)
  → minga-node      (Supervisor dispatches by kind: Container → Runtime, Agent → AgentExecutor)
  → minga-sandbox   (WasmtimeAgentExecutor runs WASM)
  → minga-egress    (token budget enforced at the proxy)
```

### 1. `WorkloadKind` in `TaskSpec` (minga-state)

```rust
pub enum WorkloadKind {
    #[default]
    Container,
    Agent(AgentSpec),
}

pub struct AgentSpec {
    pub module_ref: String,
    pub fuel_limit: Option<u64>,
    pub memory_limit: Option<u64>,
    pub timeout_secs: Option<u64>,
    pub token_budget: Option<u64>,
    pub egress_url: Option<String>,
    pub prompt: Option<String>,
    pub sandbox_env: HashMap<String, String>,
}
```

`TaskSpec.kind: WorkloadKind` with `#[serde(default)]` — existing container tasks deserialize as `Container`. Uses externally-tagged serde representation (bincode-compatible with openraft).

### 2. `x-minga-agent` Compose extension (minga-compose)

```yaml
services:
  researcher:
    x-minga-agent:
      module: /opt/minga/modules/researcher.wasm
      fuel_limit: 10000000
      memory_limit: 268435456   # 256 MiB
      timeout: 120              # seconds
      token_budget: 50000
      egress_url: https://api.openai.com
      prompt: "Analyze the dataset and summarize findings"
      env:
        MODEL: gpt-4o
    # No `image:` required — agent services skip the image check
```

- Extension key: `x-minga-agent` (constant `AGENT_EXTENSION_KEY`).
- `module` is required. All other fields optional with sensible defaults.
- String-or-number deserialization for numeric fields (`"10000000"` or `10000000`).
- Agent services skip the `image` requirement in `check_services`.
- `Service::is_agent()` and `Service::agent_extension()` accessors.

### 3. Scheduler translation (minga-scheduler)

`translate_service` checks `svc.agent_extension()`:

- **Agent service** → `image = ""`, `agent = Some(agent_extension_to_spec(ext))`, `kind = WorkloadKind::Agent(spec)`.
- **Container service** → requires `image`, `agent = None`, `kind = WorkloadKind::Container`.

`place_service` propagates `service.agent` into `TaskSpec.kind`. Placement constraints, resource accounting, and reconcile logic are kind-agnostic — they operate on `TaskSpec` fields (cpu, memory) regardless of kind.

### 4. `AgentExecutor` trait (minga-node)

```rust
pub trait AgentExecutor: Send + Sync {
    fn start(&self, task_id: &str, spec: &AgentSpec) -> Result<(), AgentError>;
    fn stop(&self, task_id: &str) -> Result<(), AgentError>;
    fn status(&self, task_id: &str) -> Result<AgentStatus, AgentError>;
    fn result(&self, task_id: &str) -> Result<Option<AgentRunResult>, AgentError>;
    fn list(&self) -> Vec<AgentInfo>;
}
```

Two implementations:

| Implementation | Use |
|---|---|
| `MockAgentExecutor` | Unit tests, single-node development without WASM |
| `WasmtimeAgentExecutor` | Production: reads WASM module from filesystem, creates `WasmtimeRuntime` per task, runs sandbox, stores results |

`AgentStatus`: `Running` (default), `Completed`, `Failed`.
`AgentRunResult`: output bytes, fuel consumed, duration, exit code.

### 5. Supervisor dispatch by kind (minga-node)

The supervisor holds `agent_executor: Option<Arc<dyn AgentExecutor>>` alongside the existing `runtime: Arc<dyn Runtime>`. Dispatch is by `spec.kind`:

| `spec.kind` | Start path | Stop path | Status source |
|---|---|---|---|
| `Container` | `runtime.start(spec)` | `runtime.stop(task_id)` | `runtime.status(task_id)` |
| `Agent(spec)` | `agent_executor.start(task_id, spec)` | `agent_executor.stop(task_id)` | `agent_executor.status(task_id)` |

The reconcile loop (`raft_reconcile`), command dispatch (`execute_command`), and status publishing (`publish_status`) all branch on kind. Agents are tracked in a separate `running_agents` map. Agent status is published over the bus when it reaches a terminal state (`Completed`/`Failed`).

### 6. CLI wiring (minga-cli)

New flags on `minga node`:

| Flag | Purpose |
|---|---|
| `--sandbox-runtime mock\|wasmtime` | Select agent executor backend (default: mock) |
| `--module-dir PATH` | Directory containing WASM modules (default: `./modules`) |
| `--egress-bind ADDR` | Start the egress proxy on this address |
| `--egress-upstream URL` | LLM API upstream for the egress proxy |

New subcommand: `minga agent ls` — list running/completed agents.

`run_node()` creates the `AgentExecutor` (mock or wasmtime), optionally starts the `EgressProxy`, and passes the executor to `Supervisor::with_agent_executor()`.

## Egress proxy placement

The egress proxy runs **on the worker node** (co-located with the agent executor), not as a separate service. This means:

- Agent WASM modules reach the LLM API only through the local proxy (`127.0.0.1:PORT`).
- Token budgets are enforced per-workload at the proxy, keyed by the workload the request's sandbox token was minted for (see Implementation status).
- Secrets (API keys) never cross the bus or Raft, and agents never hold them: the proxy reads the provider key from the node secret store and injects it in transit.
- In air-gapped deployments, the proxy can point at a local model server instead of a cloud API.

## Module distribution strategy

WASM modules are referenced by `module_ref` (a filesystem path on the node). Distribution is **not** handled by Minga in this wave — it is a follow-up:

- **Phase 1 (now)**: modules are pre-staged on each node's filesystem (shared volume, image layer, or manual copy).
- **Phase 2 (future)**: distribute modules through Raft as a new entity type (`AgentModule`), with content-addressed storage and pull-on-demand from the node that has the module.
- **Phase 3 (future)**: OCI artifact distribution for WASM modules (same path as container images).

## Rules

- `WorkloadKind` is the single discriminant — no parallel `is_agent` flags scattered across types.
- Agent services in Compose do not require `image:`.
- The supervisor never panics on a missing `AgentExecutor` — agent tasks are skipped with a warning.
- `AgentSpec` is bincode-serializable (externally-tagged enum, no internally-tagged variants).
- All agent state transitions go through the bus (JetStream), not Raft — same principle as container task status.

## Consequences

- Agent workloads are now first-class: a single `compose.yaml` can mix containers and agents.
- The `AgentExecutor` trait mirrors the `Runtime` trait — same lifecycle (start/stop/status), same testability (mock for unit tests).
- WASM module distribution is explicitly deferred — the `module_ref` indirection makes it a clean follow-up without changing the trait.
- The egress proxy is per-node, not per-cluster — this is correct for edge/air-gapped deployments where each node has its own upstream.

## Alternatives considered

| Alternative | Verdict |
|---|---|
| Run agents as containers (wrap WASM in a container image) | Rejected — loses fuel metering, warm pools, and sandbox isolation guarantees |
| Separate agent orchestrator service | Rejected — violates the "one binary" principle; agents belong in the same pipeline |
| Distribute WASM modules through Raft now | Rejected as premature — module distribution is a Phase 2 concern; the `module_ref` indirection covers it |
| Global egress proxy (one per cluster) | Rejected — wrong for edge/air-gapped; per-node proxy matches the deployment model |

## Implementation status (2026-11-02)

- **Working:** `x-minga-agent` parsing, scheduler translation, placement, supervisor dispatch by kind, and the wasmtime executor with fuel and timeout limits. Since `minga deploy` goes through the Docker API, the agent spec travels as the `minga.agent` service label, and the label is removed again before state.
- **Missing: host imports.** Modules are instantiated with none: no WASI, network, env or clock. A WASI module fails to instantiate, and a guest can't reach the egress proxy. `MINGA_EGRESS_URL` is set in an environment the guest can't read.
- **Missing: limits.** `memory_limit` and `token_budget` are carried but not enforced.
- **Identity: per-sandbox token.** When `WasmtimeAgentExecutor` starts a run it mints a random 256-bit token for the task (`minga_egress::SandboxTokens`), puts it in the sandbox env as `MINGA_EGRESS_TOKEN`, and revokes it when the run ends. The proxy serves only requests whose `X-Minga-Sandbox-Token` maps to a live token, and takes the workload from that mapping. A request without one gets 401; an `X-Minga-Workload` that names a different workload gets 403. The registry lives in memory, shared by the executor and the proxy inside one `minga worker`. The standalone `minga egress` mints one `default` token at startup and prints it.
  - *Why a token and not a per-sandbox listener:* wasmtime guests have no sockets. Once host imports land, the HTTP call leaves the node process, so a listener per sandbox would see the same caller every time. A token works for that host import, which can attach it without the guest ever seeing it, and also for Firecracker, where the guest does open sockets. It costs one map lookup, with no ports to allocate and no listeners to tear down.
  - *Limit:* while it runs, a guest can read its own token and hand it to another process on the node. That process can spend this task's budget but never sees the provider key, and the token is dead once the run ends.
- **Credential injection.** `--egress-credential <secret-id>` (or `--credential` on `minga egress`) reads the provider key from the node secret store via `SecretStore::read_value`, with the same checks as task secrets. The proxy sends it as `Authorization: Bearer` or, with `--egress-credential-header x-api-key`, as `x-api-key`. Of the client's headers, only `accept`, `anthropic-version` and `anthropic-beta` are forwarded. Anything else, including the client's own `Authorization`/`x-api-key`, the sandbox token and `X-Minga-Workload`, is dropped. The key is a literal `SecretRedactor` rule, and upstream error bodies (status ≥ 400) are redacted along with the proxy's own errors. The key is read once at startup, so rotating it needs a restart.
- **Known gaps in the proxy:** the response cache is keyed by method, path and body but not by workload, so one workload can be served another's cached answer. On a node, no budgets are registered, so every request gets 429 until `token_budget` is enforced (below).
- **Missing: Firecracker.** `FirecrackerRuntime` is a stub.

Until host capabilities land, agents are pure compute: input bytes in, output bytes out.
