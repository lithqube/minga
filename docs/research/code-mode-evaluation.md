# Code Mode evaluation: what Minga should take from it

- **Status:** Research notes, no decision. Revisit when a tenant needs tools at agent run time, or when sandbox steps 3–5 land.
- **Date:** 2026-11-21
- **Source:** Cloudflare's [Code Mode post](https://blog.cloudflare.com/code-mode/) (2025-09-26) and the [codemode docs](https://github.com/cloudflare/agents/tree/main/docs/codemode) in `cloudflare/agents` at `main`. Read: the post, and `index.md`, `runtime.md`, `connectors.md`, `approvals.md` and `snippets.md`.
- **Not read:** `vite-plugin.md`, the linked `docs/agents/codemode.md`, and the package source. The follow-up post ([an entire API in 1,000 tokens](https://blog.cloudflare.com/code-mode-mcp/)) and Anthropic's [code execution with MCP](https://www.anthropic.com/engineering/code-execution-with-mcp) were seen only through search summaries, so their numbers need checking against the page. No competitor survey was done.
- **Related:** [ADR 0003](../adr/0003-agent-workloads.md), [ADR 0008](../adr/0008-multi-tenancy.md), [agent-plane-roadmap.md](../agent-plane-roadmap.md), [sandbox-host-capabilities-plan.md](../sandbox-host-capabilities-plan.md)

## Summary

Do not build Code Mode into Minga now. Its security model is already Minga's sandbox plan, its approval flow is a weaker form of Minga's change requests, and its token savings need many tools, which `minga mcp` does not have. One small change is worth considering: typed outputs on `minga mcp`. The research also gives a consistent test for which agent features belong in Minga, set out under [Niche strategy](#niche-strategy).

## What Code Mode is

The model gets one tool that runs code, instead of one tool per operation. External services appear inside the sandbox as typed TypeScript objects, and the model writes a script against them.

| Piece | What it does | State |
| --- | --- | --- |
| Executor | Runs one block of code once in an isolate with no network. Replaceable: one for Workers, one for a browser iframe. | None |
| Connectors | One class per service (MCP server, OpenAPI spec, custom). Holds the credentials. Each tool can require approval and define a `revert`. | Connection and credentials |
| Runtime | A durable log of every tool call. Drives approvals, rollback, audit and saved scripts ("snippets"). | SQLite on a Durable Object |

Mechanisms that matter for the comparison:

- **Discovery inside the sandbox.** The script calls `codemode.search` and `codemode.describe`, so type information lands in the running code and not in the prompt.
- **Abort and replay.** A call that needs approval aborts the run. After approval the same script runs again, earlier calls are served from the log, and the approved call executes for real.
- **Determinism rule.** Replay needs the same calls in the same order. Calls are awaited one at a time, and anything random goes through `codemode.step`.
- **Bounds.** A recorded value is capped at 1 MB, finished runs are pruned to the newest 50, and unanswered approvals expire after 24 hours by default.

The post gives no benchmarks. Its claims are that models write code better than they emit tool calls, and that chained calls stop passing intermediate results through the context. The docs themselves say "Ten tools is fine" for the classic approach.

## How it maps to Minga

| Codemode piece | Minga today | Gap |
| --- | --- | --- |
| Executor | `WasmtimeRuntime` with fuel, memory cap, epoch timeout and `WarmPool` | No JavaScript engine in the guest. No WASI or host functions yet (sandbox plan steps 3–5). |
| No network, bindings only | Sandbox plan: deny by default, no sockets, HTTP only to the egress proxy, token injected by the host | None in design. Not active in guests yet. |
| Connectors | Nothing. Egress routes reach LLM upstreams only. | A tool route kind in `minga-egress` and a tool-dispatch host function. |
| Durable call log | Nothing. AGENTS.md already puts agent run steps on JetStream. | A per-tenant stream and a replay engine. |
| Approvals, expiry, audit | Change requests: distinct same-tenant approver, 15-minute expiry, Raft, tenant audit stream | None. Minga's is stricter. |
| Rollback | `rollback` proposal to the stored previous revision | None |

Three conclusions:

1. **`minga mcp serve` gains nothing.** It has ten tools, seven with no arguments, so there is no context to save. A server-side `execute` tool would break the fixed allow-list and the rule that the server never exposes arbitrary HTTP.
2. **Minga's approval model is the safer one for infrastructure.** Codemode approves one call inside a script that then keeps running. Minga's reviewer approves one fully specified change against a compared base, and a stale proposal becomes `superseded`.
3. **Code Mode is a client technique.** A Code Mode client can already wrap `minga mcp` through its MCP connector. Maker-checker still holds, because the manager enforces it and the client's approval settings do not matter.

None of Cloudflare's code is reusable. It is TypeScript tied to Workers and Durable Objects, and its executors need Cloudflare or a browser, so nothing in it runs on-prem or air-gapped.

## Niche strategy

**Be the governed place to run agents where the cloud cannot go, and own the control points, not the agent.**

Cloudflare's docs show what a mature agent platform ends up needing: a sandbox with no network, credentials held by a supervisor, approvals, an audit log and rollback. Cloudflare offers that inside its cloud, for code written to its runtime. Minga can offer the same guarantees on hardware the customer owns, offline, for the containers they already have.

Four rules keep this consistent:

- **Own the control points, not the intelligence.** Minga decides what an agent can reach, who it is, what it can spend, what it may change and what it did. The framework, prompts, tools and evals stay with the application, as the roadmap's "What Minga should not do" list says.
- **Unmodified workloads first.** A Compose file deploys unchanged and gets the controls. The WASM sandbox is extra hardening, not the entry ticket.
- **Offline is the moat.** Every control works with the network unplugged: local model routes, vendored artifacts, budgets with no SaaS dependency.
- **Enforce on the server.** Tenant scoping and maker-checker hold whatever harness connects.

### Feature test

A proposed agent feature belongs in Minga if it passes all four:

1. The manager or node enforces it, whatever the client does.
2. It works for an unmodified container.
3. It works air-gapped.
4. It is scoped to one tenant (ADR 0008).

| Candidate | Result |
| --- | --- |
| Typed outputs on `minga mcp` | Passes all four. |
| Tool routes in the egress proxy | Passes all four. No demand yet. |
| Code Mode executor in the WASM sandbox | Fails 2. No demand yet. |
| Server-side `execute` on `minga mcp` | Fails the allow-list rule before the test applies. |

### What it means for the roadmap

- **Keep the current order.** Budgets (B6), rate limits (B7), workload identity (C9, C10), jobs (D13) and offline artifacts (E14, E15) are all control points.
- **If tenants ask for tools at run time,** extend the egress proxy from LLM upstreams to tool upstreams first. Containers then get "no keys to leak, every call logged" without a code executor.
- **Build a sandbox executor last, if at all.** It needs tool routes and sandbox steps 3–5 first, and a tenant that must run model-written code on site.

## Ideas worth borrowing

- **Typed tool outputs.** `minga mcp` declares protocol `2024-11-05` and returns JSON as a text string. Codemode's tool type carries an `outputSchema`, so a client can only type results the server declares. Check which MCP revision added `outputSchema` before planning this.
- **Two-state log entries.** A call is logged `executing` when it starts and `applied` once its result is recorded, so a crash between the two re-runs the call instead of replaying a missing result. Useful when job runs (D13) and agent run steps are designed.
- **Executor, connector, runtime split.** A good template if a tenant-facing tool layer is ever built: the stateless part is replaceable, and all durable state has one home.

## Before any of this is built

- The roadmap survey found that all three downstream projects run one-shot Python containers with no tools at run time. Code Mode solves tool chaining, which they do not do.
- "Nobody offers these controls on-prem" is an assumption. Check Nomad, k3s and other edge orchestrators before it goes into positioning.
- Small local models at air-gapped sites may write code worse than they call tools. Nothing here tested that.
