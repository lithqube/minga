# Agent plane: roadmap beyond sandbox host capabilities

- Status: Order 1 is complete on current `main` (PR #103, 2026-11-19); Order 2 remains in progress.
- Date: 2026-11-19
- Companion to [sandbox-host-capabilities-plan.md](sandbox-host-capabilities-plan.md)

## Why this exists

We surveyed three downstream projects that deploy, or plan to deploy, AI agents on Minga at edge and single-region sites. What they have in common:

- **Every agent is a container, not WASM.** All three are Python services (FastAPI, crewAI or plain flows, one LLM call per task). None can run under wasmtime without a port. The controls below must therefore work for containers first; the WASM sandbox is extra hardening on top.
- **Agents are one-shot and triggered by events or requests**, with no tools at run time. Context is fetched before the model call, and the results are drafts or advisory.
- **Each project builds its own LLM gateway, budget and metering.** One uses a gateway with per-consumer virtual keys, which is a single point of failure, with budgets "not configured yet". One meters tokens in process memory, so it breaks beyond one replica. One has no design yet.
- **Shared gaps:** no per-tenant budgets or rate limits, cost is attributed to the requested model rather than the one that answered, egress is open, AI is left out of offline appliances, and no one has audit of agent calls.
- **Each project hand-rolls caller identity:** HMAC key rings, per-consumer virtual keys, or an OAuth machine user.
- **Agent-driven ops needs approval.** One project designs an ops agent that may only *request* actions, with a maker-checker approval path and an audit row per call.
- **Scheduled work fails silently.** One project had cron jobs that never ran for months, and nothing noticed.

## What Minga should add

Each item says where its state lives, following CLAUDE.md: desired state goes through Raft, runtime events go through JetStream.

Every item is owned by one Minga **tenant** and follows [ADR 0008](adr/0008-multi-tenancy.md): tenants are untrusted, each gets dedicated nodes and tenant-scoped bus subjects and streams (ADR §7), and a reference across tenants returns 404. Each item's PR answers the ADR's tenancy checklist. Because a worker serves one tenant, node-local parts (proxy, caches, nftables rules) need no tenant key of their own. Anything held on managers or in JetStream does. The *application's* end tenants are called **customers** here, so the two never get confused.

### A. Egress for every workload (highest value)

Implemented on current `main` (PR #103 and preceding agent-plane changes):

1. **Container egress path:** service opt-in egress with a task identity and deny-by-default outbound path when required.
2. **Named routes with fallback:** tenant-owned routes, credentials from the node secret store, configured fallback behavior and destination safety checks. Redirects and proxy environment settings do not bypass the destination policy.
3. **Model and route policy:** per-service route/model allow-lists reject unapproved requests before forwarding.
4. **Per-workload cache key:** cache entries are isolated by workload and route.
5. **Streaming and usage:** upstream response streams through; provider usage is recorded when available, with bounded fallback accounting. Streaming responses are not cached.

### B. Customer attribution, budgets and metering

5. **Customer attribution (implemented):** usage events carry an optional customer ID supplied by the workload. It is attribution, not end-user authentication; the application is trusted to name its customer.
6. **Atomic budgets per (tenant, service, customer, window).** One key per budget in `MINGA_BUDGET_<tenant>`:
   - The value is `{settled, reservations: [{id, tokens, expires_at}]}`. Each request adds a reservation for its worst case (`max_tokens`) with a revision-checked update (KV compare-and-swap), then settles it to the actual usage. Concurrent replicas on different nodes cannot overspend by more than one reservation each.
   - **A crashed proxy cannot leak budget.** Every update drops expired reservations, so an unsettled reservation stops counting after its expiry (the upstream timeout plus a margin). This needs no per-key TTL and no server feature newer than 2.10.
   - Counter streams (`Nats-Incr`, 2.12+) were considered and rejected. They avoid retry loops when many writers hit one key, but an increment from a proxy that crashes is never undone, and they cannot express "reserve only if under the limit". Revisit if retries on one hot budget key become measurable.
   - Windows can be daily or monthly.
   - The limits are desired state: they are declared in Compose and go through Raft.
   - Crossing 80% and 100% publishes an event.
   - An agent's `token_budget` stays what the sandbox plan decided: a cap for one run. It applies alongside these windowed budgets, and whichever runs out first rejects the request.
7. **Rate limits per (tenant, service, customer).** A token bucket per node, with the cluster-wide limit split evenly across nodes running the service. This is a documented shortcut until it needs to be exact.
8. **Usage stream (implemented):** events are emitted only for requests that reach usage finalization in `proxy_handler`. Early validation rejections and requests denied by `BudgetTracker::reserve` (429) produce no usage event and are excluded from totals. Published events use tenant-scoped `MINGA_USAGE_<tenant>` streams, request IDs for de-duplication and bounded retention from tenant quotas; they include route, requested/answered model, customer, token counts, cache hit, latency, status and cost, but no prompt or response body. The `minga_agent_usage_*` Prometheus counters count only what the running process records and start at zero on restart. Retained events are read through `GET /v{version}/agent/usage`: a tenant caller gets its own request-level events (100 by default, at most 1000, filterable by customer), and a cluster operator gets per-tenant totals only, with the customer filter refused. `minga-ctl agent usage` authenticates as an operator, so it shows the totals.

**Still open:** B6 atomic cross-node budgets per `(tenant, service, customer, window)` and B7 request-rate limits. Current per-run/per-workload budget tracking is node-local and does not provide a cluster-wide customer spend limit.

### C. Workload identity and secrets

9. **Workload identity tokens.** Managers mint a short-lived signed JWT per task:
   - claims: tenant, stack, service, task, node, expiry
   - delivered as a file and rotated before expiry
   - signed with a per-tenant key whose public half is served at `/tenants/<id>/.well-known/jwks.json` on the manager API (ADR 0008 §4)

   Application services verify agent callers with a standard JWT library instead of HMAC key rings, and trust only their own tenant's keys. The same token can replace the opaque egress token. The signing keys are tenant desired state.
10. **Credential reload without restart.** The egress proxy watches the secret store and reloads provider keys, which removes the "rotating it needs a restart" gap in ADR 0003. File secrets in tasks are rewritten in place on rotation.

### D. Agent-ready operations

11. **`minga mcp`.** An MCP server over the existing API client, using the role and tenant in the caller's certificate. It sees only that tenant.
    - Read tools: services, tasks, logs, events, agent usage.
    - Write tools only *propose* (item 12).

    This gives an ops agent a scoped interface instead of SSH.

Phase 16 implementation: `minga mcp serve` is a stdio MCP server with a fixed
tenant-scoped read allow-list for status, tenants, nodes, services, tasks,
bounded task logs, event metadata, usage and derived diagnostics. It requires
an HTTPS manager endpoint, a trusted API CA and a tenant client certificate
with a protected private key; `O=*` and bearer tokens are not accepted. The
server never exposes arbitrary HTTP, service environment values, task error
text or event payloads. Event results contain only subject, observation time
and payload size. Its `request_change` write proposal is advertised only to agent
certificates and implements item 12; no other write tool is exposed.

Example setup (the client CA must match the manager's API client CA; the CA
passed to `--ca-cert` must issue the manager's HTTPS server certificate):

```sh
minga tls issue-client --ca-dir ./client-ca --out-dir ./mcp-client \
  --cn tenant-ops --role viewer --tenant tenant-a
minga mcp serve --api-url https://manager.example:2376 \
  --ca-cert ./server-ca/ca.pem \
  --client-cert ./mcp-client/tenant-ops.pem \
  --client-key ./mcp-client/tenant-ops-key.pem
```
12. **Change requests (maker-checker).** Adds a new certificate role, `agent`, below `deployer`:
    - An `agent` can create a change request (deploy, scale, rollback, stop) but cannot apply it.
    - A `deployer` or `admin` **of the same tenant** approves it with `minga-ctl change approve ID`.
    - The approver must not be the requester, and requests expire.
    - Requests are desired state (Raft). Every create, approve, reject and expire writes the existing audit line.

Phase 16 implementation: certificate role `agent` is below `deployer`; only
agent identities can propose, and only a distinct same-tenant deployer/admin
can decide. Requests expire after 15 minutes and persist with their lifecycle
audit in Raft. Approval compares the reviewed base service and applies the
desired-state update atomically; stale proposals become `superseded`. The
existing tenant audit stream records create, approve-and-apply, reject, expire
and supersede outcomes. `minga-ctl context add` accepts
`--client-cert`/`--client-key`; `minga-ctl change list|approve|reject` uses
mTLS and never sends a configured bearer token. The supported proposals are
image deployment for an existing container service, scale to 1–1000 replicas,
rollback to the stored previous revision, and stop by setting replicas to
zero. New-service creation is not an agent proposal.

Provision a tenant agent certificate with:

```sh
minga tls issue-client --ca-dir ./client-ca --out-dir ./mcp-client \
  --cn tenant-agent --role agent --tenant tenant-a
```

Configure `minga mcp serve` with the HTTPS manager URL, server CA, agent
certificate and mode-0600 private key. A deployer/admin uses a separate tenant
certificate and a direct HTTPS `minga-ctl` context to review the image/replica
change and approve or reject it.
13. **Jobs and schedules.**
    - `x-minga-job: { schedule: "0 3 * * *", deadline: 600s, concurrency: forbid }` runs a container or agent to completion.
    - When the deadline passes, the task is killed, which fixes agent threads that cannot be cancelled in process.
    - A run that is missed or failed publishes an event, so silent cron failures become alerts.
    - Schedules are desired state (Raft); runs and their results go through JetStream, in the tenant's streams. Jobs count against the tenant's quotas.
   - The Raft leader fires runs. JetStream message schedules (cron, 2.14+) are not used: they would keep a piece of desired state outside Raft, breaking the CLAUDE.md rule.

### E. Offline and air-gapped AI

14. **Artifact distribution.** WASM modules and model weights are pulled as OCI artifacts by digest, on the same path as images. This is ADR 0003 phase 3, pulled forward because offline sites need vendored weights. Pull credentials belong to the tenant, and the node's artifact cache serves only its own tenant.
15. **Local model route recipe.** Docs and an example Compose file for a `local` route to Ollama or llama.cpp on the node, with failover from cloud to local and the reverse.

### F. Later: a bus for tenant applications

16. **Tenant bus.** All three surveyed projects run their own NATS for application work, and one already uses `tenant.{id}.agents.{type}.task` subjects.
    - Minga could give each tenant a NATS **account**, the right isolation once tenant code connects. It would have its own JetStream limits, and the Synadia agent protocol (agents as NATS micro services, discovered via `$SRV`) would be available inside it.
    - It runs on a NATS cluster separate from Minga's control-plane sidecar, so application load cannot slow scheduling.
    - Not in this roadmap's order. It needs ADR 0008 T4 first.

## What Minga should not do

These stay in the application:

- **Business approval inboxes.** Minga approves changes to *Minga*, not invoices or claims.
- **PII pseudonymisation.** Only the application knows which fields are personal. Secret redaction in the proxy is not a residency control.
- **Semantic or prompt-injection guards, and evals.** These are model-specific and owned by the application.
- **Idempotency and nonce stores, and provisioning the application's own NATS streams** until F16 exists. These keep the application's bus separate from Minga's own.

## Egress engine options

Should Minga keep its own proxy (axum + reqwest, about 1,900 lines in `crates/minga-egress`) or build on an existing one? The candidates are listed below; facts were checked on 2026-11-16.

LLM egress handles few requests, each lasting seconds and dominated by the upstream model. Proxy throughput is not the constraint. What matters is fitting into one binary, knowing which Minga task made the request, and working offline. CLAUDE.md and ADR 0001 also rule out embedding Go: a Go gateway can only run as a separate process behind a versioned contract.

| Option | What it is | Fit for Minga egress |
|---|---|---|
| **Keep `minga-egress`** | Rust, inside `minga worker` | **Chosen.** It already has the sandbox token, the secret store and redaction, and knows the task. The gaps are features (A2, A5, B), not the engine. |
| **Pingora** (Cloudflare) | Rust proxy framework, Apache-2.0, v0.9.1, MSRV 1.85 | Strong engine, but it is a framework that owns the process. `Server::run_forever` never returns and manages signals, daemonising, graceful upgrade and its own runtimes. `minga worker` already has its own runtime and SIGTERM handling. It is still pre-1.0, its cache API is "highly volatile", rustls is experimental (OpenSSL or BoringSSL by default), and the MSRV is above our 1.80. Revisit only if egress becomes its own high-throughput process. |
| **Traefik** | Go ingress proxy | **Not for egress; keep it as ingress.** The open-source rate limit counts requests, not tokens. Token limits and LLM cost budgets are in Traefik Hub's AI Gateway, which is commercial. Minga's job here is to let Traefik discover services via the Docker socket and events route (the blocker below), not to replace it. |
| **Bifrost** (maximhq) | Go LLM gateway, Apache-2.0 | Has many providers, failover, virtual keys, budgets, Prometheus and streaming. But its README lists clustering, adaptive load balancing, guardrails and the MCP gateway as enterprise features, so budgets shared across nodes are not in the open-source tier. It would duplicate Minga's secret store and caller identity, and it cannot tell which Minga task is calling. |
| **Envoy AI Gateway** | Envoy-based, token rate limits | Kubernetes-first. Standalone `aigw run` is documented as a way to test configuration locally. Not a fit for a single binary at the edge. |
| **LiteLLM proxy** | Python gateway | Needs Postgres and Redis for budgets. Same separate-process concerns as Bifrost. |

**Bring your own gateway.** Users who already run Bifrost or LiteLLM keep it. An A2 route can point at it as an OpenAI-compatible upstream, and Minga still adds the task token, the egress lockdown (A1) and the usage stream (B8) in front. This needs no extra code beyond A2.

## Order

| Order | Items | Status / why |
|---|---|---|
| 0 | NATS supported server line (T0) | Complete: NATS 2.15.3, CI matrix and store-upgrade proof. |
| 1 | A1–A5, B5, B8, ADR 0008 T1–T2 | Complete on current `main`; PR #103 finishes named routes, policy, streaming, usage and operator usage query. |
| 1b | ADR 0008 T3–T5 | Implementation complete (PR #98 and Phase 15); T3 live tenant-isolation proof remains deferred until release readiness. |
| 1c | D11, D12 | Pulled forward into Phase 16 before v1.3.0; Phase 15 supplies tenant identity and audit. |
| 2 | B6, then B7 | Planned after Phase 16: distributed customer window budgets, then request-rate limits. |
| 3 | C9, C10 | Workload identity tokens and credential reload without restart. |
| 4 | D13 | Jobs and deadlines. |
| 5 | E14, E15, then [the WASM sandbox plan](sandbox-host-capabilities-plan.md) | Offline AI and WASM host capabilities. |

The shared Wasmtime engine, bounded module cache, epoch timeout, enforced
memory limit and capability-grant schema/validation are already merged (PRs
#101 and #102). WASI, guest HTTP egress and agent logs remain open.

## Historical adoption survey

The adoption blockers below were recorded between 2026-10-27 and 2026-11-03.
They predate Phases 12–15 and must not be treated as current feature status;
use the implementation notes above and `docs/architecture.md`.

