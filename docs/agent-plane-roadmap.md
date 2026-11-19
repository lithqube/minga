# Agent plane: roadmap beyond sandbox host capabilities

- Status: Draft
- Date: 2026-11-16
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

1. **Container egress path.** Containers get the same per-task sandbox token that agents get, delivered as a file at `/run/minga/egress-token` and reached at a fixed address on the task bridge. With `x-minga-egress: { required: true }`, nftables on the task bridge drops all other outbound traffic, giving deny-by-default egress per service. This needs only per-host networking, which exists today.
2. **Named upstream routes with failover.** Replace the single `--egress-upstream` with routes such as `anthropic`, `bedrock`, `mistral` or `local`:
   - Each route has its own credential and auth scheme: bearer, `x-api-key`, and **AWS SigV4** for Bedrock.
   - A route can list fallback routes. Failover happens only on connect errors, 429 or 5xx, never on 4xx, so a bad request does not fail open to another provider.
   - Routes are tenant-owned desired state, and their credentials are secrets in that tenant's node secret store. One tenant's provider key is never used for another tenant.
3. **Model and residency allow-list per service.** For example, `x-minga-egress: { models: [claude-haiku-*], routes: [bedrock-eu] }`. The proxy rejects any other model or route before forwarding. This is policy, not data-residency proof, and the docs say so.
4. **Per-workload cache key.** Moved here from the sandbox plan, so containers get it too.
5. **Streaming.** Today the proxy reads the whole request (`axum::body::to_bytes`, 10 MiB cap) and the whole upstream response (`upstream_resp.bytes()`), so a streamed (SSE) completion arrives all at once at the end, and tokens are guessed from the body length divided by 4. Pass the response through as a stream (`reqwest::Response::bytes_stream` into `axum::body::Body::from_stream`). Read the `usage` the provider reports in the final event (Anthropic `message_delta`, OpenAI `stream_options.include_usage`), and use length/4 only as a fallback. Streamed responses are not cached.

### B. Customer attribution, budgets and metering

5. **Customer attribution.** The workload sends `X-Minga-Customer: <id>`. The service declares whether a customer is required, and optionally a pattern for valid IDs. The workload is trusted to name the customer it serves; Minga does not verify end users. The Minga tenant is never taken from a header: it comes from the task token.
6. **Atomic budgets per (tenant, service, customer, window).** One key per budget in `MINGA_BUDGET_<tenant>`:
   - The value is `{settled, reservations: [{id, tokens, expires_at}]}`. Each request adds a reservation for its worst case (`max_tokens`) with a revision-checked update (KV compare-and-swap), then settles it to the actual usage. Concurrent replicas on different nodes cannot overspend by more than one reservation each.
   - **A crashed proxy cannot leak budget.** Every update drops expired reservations, so an unsettled reservation stops counting after its expiry (the upstream timeout plus a margin). This needs no per-key TTL and no server feature newer than 2.10.
   - Counter streams (`Nats-Incr`, 2.12+) were considered and rejected. They avoid retry loops when many writers hit one key, but an increment from a proxy that crashes is never undone, and they cannot express "reserve only if under the limit". Revisit if retries on one hot budget key become measurable.
   - Windows can be daily or monthly.
   - The limits are desired state: they are declared in Compose and go through Raft.
   - Crossing 80% and 100% publishes an event.
   - An agent's `token_budget` stays what the sandbox plan decided: a cap for one run. It applies alongside these windowed budgets, and whichever runs out first rejects the request.
7. **Rate limits per (tenant, service, customer).** A token bucket per node, with the cluster-wide limit split evenly across nodes running the service. This is a documented shortcut until it needs to be exact.
8. **Usage stream.** One event per request on `MINGA_USAGE_<tenant>`, subjects `minga.<tenant>.usage.<service>`.
   - The subjects are not under `minga.egress.>`: `MINGA_EVENTS` already captures that prefix, and JetStream rejects streams with overlapping subjects.
   - The request ID is the `Nats-Msg-Id`, so a retried publish is not counted twice.
   - `max_age` and `max_bytes` come from the tenant's quota.

   Fields:
   - request id, tenant, service, task, customer
   - route, model requested and **model answered**
   - tokens in and out, cache hit, latency, status
   - cost from a price table the operator supplies

   No prompt or response bodies. Usage is also exposed as Prometheus metrics on the existing telemetry endpoint, and `minga-ctl agent usage --customer C --since 30d` reads it for the caller's tenant. Cluster operators see totals per tenant only.

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
12. **Change requests (maker-checker).** Adds a new certificate role, `agent`, below `deployer`:
    - An `agent` can create a change request (deploy, scale, rollback, stop) but cannot apply it.
    - A `deployer` or `admin` **of the same tenant** approves it with `minga-ctl change approve ID`.
    - The approver must not be the requester, and requests expire.
    - Requests are desired state (Raft). Every create, approve, reject and expire writes the existing audit line.
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

| Order | Items | Why first |
|---|---|---|
| 0 | Verify the blockers below, and ADR 0008 T0 (nats-server upgrade) | They block adoption more than any new feature, and the bundled 2.10 server is out of support. |
| 1 | A1, A2, A4, A5, B8, ADR 0008 T1–T2 | Every project can drop its own gateway, streaming works, and usage data arrives straight away. Tenant ownership and tenant-scoped API land together, so no item is built without an owner. |
| 1b | ADR 0008 T3 | Dedicated nodes and per-tenant WireGuard peer sets, before cross-node networking ships. |
| 2 | B5, B6, B7, A3, ADR 0008 T4–T5 | Budgets and residency, the most common open gap. Budgets live in tenant accounts and are bounded by tenant quotas. |
| 3 | C9, C10 | Removes hand-rolled caller auth and restart-to-rotate. |
| 4 | D13 | Jobs and deadlines, which agents and plain cron both need. |
| 5 | D11, D12 | Agent-driven ops. |
| 6 | E14, E15, then [the WASM sandbox plan](sandbox-host-capabilities-plan.md) | Offline AI, then hardening for agents that can be ported. |

The independent runtime-hardening step (shared Wasmtime runtime, module cache,
epoch timeout and memory cap) is implemented and verified on
`feat/agent-sandbox-capabilities`. WASI grants, HTTP egress and logs remain
sequenced after order 6.

## Before any of this

The downstream projects also cite Minga adoption blockers. Their notes were last checked between 2026-10-27 and 2026-11-03, so some may already be fixed. Verify each one first:

- no log capture
- no Docker unix socket or events route, which Traefik discovery needs
- folded `command: >` scalars not split into arguments
- no cross-node networking
