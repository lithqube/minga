# Nevia evaluation: where Minga could fit

- **Status:** Research notes, no decision. Revisit when the sandbox runtime or ingress mesh work is scheduled.
- **Date:** 2026-11-17
- **Source:** <https://nevia.cloud/docs>. Read: the landing page, Infrastructure > Compute, Infrastructure > Networking, and Reference > SDK limits.
- **Not read:** REST API reference, workspace permissions, secrets and variables, Rig adapter, operations and idempotency. Pages were fetched through a summarizing tool, so numbers below need checking against the page before anyone relies on them.
- **Related:** [ADR 0003](../adr/0003-agent-workloads.md), [ADR 0008](../adr/0008-multi-tenancy.md), [agent-plane-roadmap.md](../agent-plane-roadmap.md), [sandbox-host-capabilities-plan.md](../sandbox-host-capabilities-plan.md)

## What Nevia is

A hosted cloud from Aiven for AI agents, in beta for selected teams. Each agent gets its own Linux computer and Postgres database. The docs do not mention self-hosting or bring-your-own infrastructure, so Nevia is a service Minga can call, not something Minga can replace or run.

| Area | What the docs say |
| --- | --- |
| Compute | Firecracker microVM per computer, own kernel, bare-metal hosts, up to 16 vCPUs. Full VMs (up to 160 vCPUs) are roadmap. Started from pre-warmed pools in the low hundreds of milliseconds. OCI images. |
| Idle | Scale to zero. Sleeping machines use no vCPU or RAM. Storage is a replicated volume in the region, so sleep, wake and branch don't move data. |
| Lifecycle | Create, exec, shell, sleep, wake, branch, checkpoint and restore. Databases branch too. |
| Network (now) | A DNS name per computer and database. An ingress proxy on the host routes to the VM. Published ports become HTTPS URLs with managed TLS. A sleeping machine wakes when a client connects. Shell, console and file transfer use a separate control channel. |
| Network (roadmap) | Private connections inside a workspace, and default-deny egress with a per-machine allowlist. **Neither exists yet.** |
| Tenancy | Workspaces own resources. Permissions are per workspace. Secrets and variables are injected into computers. |
| Interfaces | CLI, remote MCP server, console, REST API, SDKs in TypeScript, Python, Rust and Elixir. |
| Frameworks | Adapters for LangGraph, Pydantic AI, Agno, Mastra, Eve, Flue, Rig and Alloy (early access). Computers ship with Git, Python, Node.js, Claude Code and Codex. |

### Limits that matter for integration

- Exec through the API ends after about 50 s. Direct exec avoids that. Output is capped (about 700 KB through the API, last 64 KiB per stream for direct exec).
- A call that ends cannot reattach to a running command, and cancelling the call does not stop the command.
- Commands run as the image's user, usually root.
- Files outside the persistent disk are not copied to a branch. Deleting a computer deletes its checkpoints.
- No restart in the current build (`not_supported`). Env var names starting with `NEVIA_` are rejected.
- File transfer goes over exec in chunks, so 100 MB takes hundreds of requests.

## Where Minga could fit

Ranked by how directly it ties to work already planned.

### 1. Nevia as a sandbox backend for the agent plane

`minga-sandbox` has a `SandboxRuntime` trait. `WasmtimeRuntime` works but gives the guest no host I/O, and `FirecrackerRuntime` is a stub. A `NeviaRuntime` over the REST API (create, exec, branch, sleep) would give VM-isolated agent sandboxes without finishing Firecracker.

- **Scope:** an optional backend for connected sites only. It conflicts with the air-gapped and edge positioning, so it must never be the default.
- **Egress:** Nevia cannot restrict egress yet, so Minga's egress proxy (agent-plane roadmap A and B) would be the only control. The sandbox would have to be told to use the proxy, and nothing would stop it going around it.
- **Tenancy:** one Minga tenant maps to one Nevia workspace. The tenancy checklist from ADR 0008 applies: credentials stay in that tenant's secret store, and a cross-tenant reference returns 404.
- **Open questions:** exec time limits against long agent runs; no reattach, so crash recovery needs its own run log; data residency and who pays.

### 2. Minga's egress proxy as the LLM gateway for coding agents on Nevia

Nevia computers carry Claude Code and Codex, and Nevia can inject variables and secrets. It has no token budgets, per-customer metering or usage stream, which are the gaps the downstream-project survey in `agent-plane-roadmap.md` found. Pointing a computer at a Minga proxy would fill them.

- **Catch:** the proxy is node-local today. It needs a reachable HTTPS ingress, which ties into the phase-13 ingress mesh work.
- **Catch:** a computer with open egress can bypass the proxy. The control only holds once Nevia ships default-deny egress.

### 3. Minga at the site, Nevia in the cloud

Minga runs on-prem or edge workloads. Nevia hosts cloud-side agents and their databases. They connect through Nevia's remote MCP server and Minga's planned `minga mcp` (roadmap D, with maker-checker change requests). Architecture only: neither side has code for it.

### 4. Nevia computers as kernel-capable test hosts for Minga CI

The netns, WireGuard and containerd tests need a real kernel, and a microVM has its own. Checkpoint and branch could give a seeded cluster in milliseconds, and sleeping one node would be a free partition test.

**Unverified.** Check first that the images allow `CAP_NET_ADMIN`, the `wireguard` module, nftables and nested containerd.

## Ideas worth borrowing even without integration

- **Wake on connect** through the ingress proxy. Fits the ingress mesh branch.
- **Checkpoints and branches for tasks**, built on `WarmPool`.
- **Operations with idempotency keys** on the API, for long actions that a client may retry.

## Before any integration

1. Read the unread pages: REST API, workspace permissions, operations and idempotency, secrets and variables.
2. Request beta access and run one real computer. Confirm the kernel capabilities, exec behavior, and whether egress is open.
3. Decide whether an optional connected-site backend is acceptable for the product, given the air-gap positioning.
4. Re-check whether private networking and default-deny egress have shipped, since #1 and #2 depend on them.
