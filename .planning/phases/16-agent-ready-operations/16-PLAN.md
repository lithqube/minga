---
phase: 16
title: Agent-ready operations and painless management
release: v1.3.0
depends_on: [15-implementation]
requirements: [OPS-VERSION, OPS-RAFT, OPS-COMPOSE, OPS-DOCTOR, OPS-UPGRADE, AGENT-CONTRACT, AGENT-APPROVAL, RC-PROOF]
status: planned
---

# Phase 16 — Agent-ready operations and painless management

<objective>Before v1.3.0, make Minga's operational state trustworthy, common failure modes diagnosable, upgrades predictable, and its control plane usable by automation and AI agents through a typed, tenant-scoped workflow. Keep a human approval boundary for changes. This is an operator-agent experience, separate from hosting agent workloads.</objective>

## Current-main context and release boundary

Main has completed Phase 12 managers-only Raft, Phase 13 WireGuard/routing mesh including its T2/T3 VM proof, Phase 14's supported NATS line, and Phase 15's tenant-isolation implementation. Phase 15 T3 end-to-end tenant placement proof remains a release-readiness gate. API client identity, tenant-scoped authorization and audit are implemented; the MCP interface and maker-checker change requests remain planned as D11/D12 in `docs/agent-plane-roadmap.md`.

The staging review still calls for facts, not behavioral guesses: show the installed binary/build separately from API version; distinguish persisted Raft membership and quorum from `--peer` bootstrap input; verify the exact deployed Compose semantics, including `deploy.labels`; and preflight compatibility before an upgrade. Do not infer a release from API behavior or treat a peer address as proof that a process is running.

Implementation and CI work can proceed without VM testing. Defer Phase 15's tenant-isolation proof and the staging upgrade exercise to the v1.3.0 release-candidate gate. No VM test is required before that candidate is ready.

## Plans

### 16-01 — Report release and Raft facts, not guesses (OPS-VERSION, OPS-RAFT)

- Report the exact running Minga binary version and available build revision separately from the `minga-ctl` client and Docker API version.
- Expose persisted Raft voter membership, leader, quorum/readiness and unavailable members. Show bootstrap `--peer` inputs separately when available; do not imply they spawn peers or replace persisted membership.
- Make a missing-voter/no-quorum finding actionable using observed member state, without recommending a wipe or restart as the default.
- **Done when:** diagnostics distinguish a healthy one-manager cluster from a two-voter cluster missing one voter and identify the running binary without inferring its version from behavior.

### 16-02 — Close Compose fidelity gaps (OPS-COMPOSE)

- Audit parsed Compose fields that reach the API/state/runtime path, starting with the staging report's `deploy.labels` behavior.
- Implement supported semantics end to end, or reject unsupported semantics with a service/key-specific error before desired state changes. Never silently discard a recognized key.
- Add a regression proving the Swarm API consumer sees the declared labels, or that validation rejects the key. Update the staging guide to match the current released binary.
- **Done when:** the staging Compose file's label behavior is proven against current code, and no unsupported key can produce an apparently successful but divergent deploy.

### 16-03 — `minga-ctl doctor` and safe support evidence (OPS-DOCTOR)

- Add a read-only diagnostic command for binary/build, API and manager readiness, Raft membership/quorum, NATS connectivity, tenant and node inventory, task health, and auth/TLS configuration state.
- Each finding includes observed evidence, severity and a bounded next action. Separate facts from inference; never diagnose a release from a behavior fingerprint.
- Generate a shareable support bundle with tokens, private keys, secret values, environment values and sensitive file contents redacted. State exactly what is included and omitted. The command must not mutate or restart the cluster.
- **Done when:** healthy, degraded-Raft, unavailable-bus and failed-task cases produce distinct structured findings; redaction tests prove fixture credentials are absent.

### 16-04 — Upgrade preflight and compatibility plan (OPS-UPGRADE)

- Given current and target releases, report state-format migration compatibility, required manager/worker ordering, NATS compatibility, tenant migration implications, backup requirements, disruption and rollback limits using release metadata and implementation evidence.
- Distinguish an in-place compatible upgrade from a required migration. Never delete a data directory, wipe state, restart roles or start an upgrade as a side effect of planning.
- Include the staging scenario: identify the actual installed version, persisted Raft membership/quorum, auth/TLS prerequisites and target release migration steps before suggesting commands.
- **Done when:** the v1.3.0 upgrade path from the supported released version is explicit and backed by migration tests; incompatible paths fail closed with a named reason, never a guessed wipe instruction.

### 16-05 — Typed, tenant-scoped agent interface (AGENT-CONTRACT)

- Implement the planned `minga mcp` read tools from Agent Plane item D11 over the typed API/client path. Start with status, tenants permitted to the caller, nodes, services, tasks, logs, usage and diagnostic findings.
- Return deterministic structured results and stable errors/operation identifiers. Do not expose arbitrary HTTP, shell, filesystem or cluster-wide reads to a tenant-scoped caller.
- Use the caller's existing certificate identity and tenant scope; do not pass shared admin credentials through prompts or tool arguments. Keep the human CLI as a supported interface rather than replacing it with MCP.
- **Done when:** a tenant-scoped tool client can inspect only resources its certificate permits, with authorization enforced server-side and without parsing table output.

### 16-06 — Maker-checker change requests (AGENT-APPROVAL)

- Implement Agent Plane item D12 on top of current tenant identity and audit: an `agent` can request only explicitly supported changes; it cannot apply them directly.
- A distinct `deployer` or `admin` in the same tenant reviews the exact diff and approves or rejects it. The requester cannot approve its own request. Requests expire; target scope, decision and applied result are auditable.
- Destructive operations remain explicit proposals. Do not add arbitrary commands, implicit restarts, or a bypass around API authorization.
- **Done when:** API-level tests prove tenant isolation, role enforcement, requester/approver separation, expiry and audit for create/approve/reject/expire/apply; no mutation is exposed before those checks pass.

### 16-07 — v1.3.0 release-candidate gate (RC-PROOF)

- After the candidate is ready, perform Phase 15's outstanding tenant-B-versus-tenant-A placement proof and an upgrade from the supported release on the exact v1.3.0 candidate. No VM testing before this gate.

- **Pre-RC preparation (no VM access):** freeze the exact candidate version, commit and artifact checksums before the run; use an isolated staging clone with a verified cold backup, not the live customer data directory. Record the source format (v10) and expected target format (v11), manager/worker rollout order, NATS version, and a tested restore point before any upgrade command.
- **Tenant-isolation proof:** provision tenant A and B with separate eligible nodes; deploy A's service, then attempt B placement when only A's node is eligible. Require placement refusal without leaking A's resource identifiers, and capture the tenant-scoped API/task evidence.
- **Upgrade proof:** on the isolated clone, upgrade the supported v1.2.0 state to the exact v1.3.0 candidate. Verify backup restoration first, coordinated format migration, Raft quorum/membership, NATS health, service/task recovery, and rollback limits. Do not test downgrade by reusing the migrated data directory.
- **Operator/agent proof:** capture candidate binary/build and `minga-ctl info`/`doctor`/`upgrade preflight`; validate `deploy.labels` behavior; verify viewer/agent/deployer permissions, tenant boundary, maker-checker expiry and audit; compare all observations with release notes and topology docs.
- Record binary/build version, persisted Raft membership, backup and recovery results, NATS version, actual Compose label behavior, and the diagnostic/agent permission outcomes.
- Update release notes, supported-topology and staging docs to match the tested candidate; publish only claims supported by this evidence.
- **Done when:** the tenant-isolation and upgrade evidence is recorded and the release docs agree with the candidate and its observed behavior.

## Order and dependencies

1. 16-01 and 16-02 verify staging facts and Compose behavior.
2. 16-03 and 16-04 build safe diagnosis and upgrade decisions from those facts.
3. 16-05 and 16-06 deliver Agent Plane D11/D12, pulled forward as the pre-v1.3.0 operator experience; existing tenant-scoped identity and audit are prerequisites.
4. 16-07 is the release-candidate gate, not an implementation-time VM test.

## Out of scope

- Agent workload capabilities such as WASI host imports, guest HTTP or agent logs; those remain in the sandbox/agent-plane plans.
- Unreviewed autonomous mutation, arbitrary command execution, AI-generated shell, or sharing an admin token with an agent.
- VM tests before the v1.3.0 release candidate is ready.
