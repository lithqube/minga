---
phase: 14
title: Agent-ready operations and painless management
release: v1.3.0
depends_on: [13-implementation]
requirements: [OPS-VERSION, OPS-RAFT, OPS-COMPOSE, OPS-DOCTOR, OPS-UPGRADE, AGENT-CONTRACT, AGENT-AUTHZ, AGENT-APPROVAL, RC-PROOF]
status: planned
---

# Phase 14 — Agent-ready operations and painless management

<objective>Before v1.3.0, make Minga's operational state trustworthy, common failure modes diagnosable, upgrades predictable, and its control plane usable by automation and AI agents through a typed, least-privilege workflow. Keep a human approval boundary for changes. This phase does not claim multi-tenancy or permit an agent to run arbitrary shell commands.</objective>

## Context and release boundary

The staging review raised several facts that should be observable rather than inferred: `--peer` may describe bootstrap inputs rather than current Raft membership; a second peer address does not prove a second manager process exists; observed API behavior cannot reliably identify the installed release; and a Compose key parsed but not applied can make an apparently successful deployment differ from its source.

Current main has `minga-ctl` cluster-resource operations and Phase 12/13 multi-node work, but those facts do not make the shared-token API safe for an autonomous writer. The API still needs server-enforced caller identity/authorization before an agent can be trusted with mutations. ADR 0005 is proposed; ADR 0008 multi-tenancy is also proposed and is not delivered by this phase. This phase must describe its single-tenant security boundary honestly.

The implementation and CI work can proceed without VM testing. Defer multi-VM and release-upgrade exercises to the v1.3.0 release-candidate gate, after the candidate is ready; do not use an early VM run as a substitute for finishing implementation or CI coverage.

## Plans

### 14-01 — Report release and Raft facts, not guesses (OPS-VERSION, OPS-RAFT)

- Report the exact running Minga version and available build revision through the existing info/version surface; distinguish daemon version, client version and API version.
- Expose the persisted Raft voter set, current leader and quorum/readiness evidence. Show bootstrap `--peer` inputs separately when available; never imply that a configured peer is a running process or that persisted membership was changed by editing flags.
- Make the no-leader / missing-voter case actionable, including the member identities and the observed health evidence. Avoid suggesting that restarting or wiping state is the default fix.
- **Done when:** output can distinguish a healthy one-manager cluster from a two-voter cluster with one unavailable voter, and identifies the binary/API version without inferring it from behavior.

### 14-02 — Close Compose fidelity gaps (OPS-COMPOSE)

- Audit parsed Compose fields that reach the API/state/runtime path, starting with `deploy.labels` from the staging review.
- Implement supported semantics end to end, or reject unsupported semantics with a service/key-specific error. Do not silently discard a recognized key.
- Add a regression proving the resulting Docker API service/task labels match the Compose declaration (or that validation rejects the unsupported key); update the staging and release docs to match.
- **Done when:** the staging Compose file's labels are visible to its Swarm-provider consumer, or deployment fails before changing cluster state with an explicit diagnostic.

### 14-03 — `minga-ctl doctor` and safe support evidence (OPS-DOCTOR)

- Add a read-only diagnostic command for version/build, API and manager readiness, Raft leader/membership/quorum, bus connectivity, node/task health, and relevant auth/TLS configuration state.
- Each finding includes observed evidence, severity, and a bounded next action. Label inference as inference; never diagnose a release from a behavioral fingerprint.
- Provide a support bundle suitable for sharing: redact tokens, private keys, secret values, environment values and sensitive file contents; state exactly which fields are omitted. Do not make the command mutate or restart the cluster.
- **Done when:** a healthy cluster, unavailable Raft peer, unavailable bus and failed task produce distinct machine-readable findings, and a fixture containing credentials proves the bundle does not disclose them.

### 14-04 — Upgrade preflight and compatibility plan (OPS-UPGRADE)

- Given current and target versions, report state-format compatibility, required ordering across managers/workers, backup requirements, expected disruption, and rollback limits using release metadata/code-supported facts.
- Distinguish a compatible in-place upgrade from a required migration. Never recommend deleting the whole data directory; never wipe, restart or upgrade nodes automatically as a side effect of planning.
- Include a preflight for the staging scenario: verify the actual installed version, current Raft membership/quorum, API compatibility, auth/TLS prerequisites and whether the target release requires migration.
- **Done when:** upgrade planning from v1.2.0 to v1.3.0 states the actual compatibility path and backup steps, and an incompatible path fails closed with a named reason instead of a guessed wipe instruction.

### 14-05 — Stable agent-facing contract (AGENT-CONTRACT)

- Make JSON/YAML output and errors deterministic, documented, and suitable for tools; include stable identifiers, machine-readable error codes and operation status rather than requiring table scraping.
- Add a typed agent interface (MCP may be the transport) over the same API/client contract. Start with allow-listed read tools for cluster status, nodes, services, tasks, logs and diagnostic findings. Do not expose arbitrary HTTP, shell or filesystem tools.
- Keep credentials in the local Minga client/tool host, not model prompts or tool arguments. Until server-side scoped authorization exists, expose only read operations and treat client-side filtering as defense in depth, not authorization.
- **Done when:** a tool client can inspect and diagnose a cluster using stable structured responses without parsing human CLI output or receiving a shared admin token.

### 14-06 — Authorized change proposals and audit (AGENT-AUTHZ, AGENT-APPROVAL)

- Review and accept/update ADR 0005 before implementing caller identity, server-side roles and an audit trail. A client-only allow-list is not sufficient for write security.
- Give an agent a narrowly scoped ability to propose supported changes, not apply them directly. A distinct authorized human/operator approves or rejects a proposal; the requester cannot approve its own change. Expiry, target scope, exact diff and result are recorded.
- Every accepted, rejected, expired and applied request has an attributable audit record. Destructive actions remain explicitly approved; no arbitrary shell, data-directory deletion or implicit restart is allowed.
- Do not claim tenant isolation: use only the single-tenant scope actually implemented. Agent writes are unavailable unless server-side authorization and approval checks are enforced and verified.
- **Done when:** API-level tests prove a viewer/agent cannot mutate state directly, an agent can only submit an allowed proposal, an authorized distinct approver can approve it, and all transitions are auditable. If this gate cannot be met, the release must not expose agent write tools.

### 14-07 — v1.3.0 candidate gate (RC-PROOF)

- Complete Phase 13's outstanding T2/T3 multi-VM proof, performance measurements and topology documentation only after a v1.3.0 release candidate is ready. No VM test is required during Phase 14 implementation.
- Exercise the supported upgrade path from v1.2.0 on the release candidate with a backup and a real staging configuration; record the installed binary version and persisted Raft membership before and after.
- Verify the documented staging Compose file and `deploy.labels` behavior, the diagnostic output, and the agent read/propose/approve boundary. Publish only claims supported by this evidence; retain known limits explicitly.
- **Done when:** release-candidate evidence is recorded, upgrade and recovery instructions are accurate, and the supported-topologies/release/changelog documentation agrees with the tested build.

## Order and dependencies

1. 14-01 and 14-02 establish trustworthy observations and Compose behavior.
2. 14-03 and 14-04 build on those facts for support and upgrade decisions.
3. 14-05 defines the agent contract; 14-06 gates all agent writes on server-side authorization and approval.
4. 14-07 is the final release-candidate gate. Phase 13 kernel CI evidence remains distinct from its outstanding multi-VM proof.

## Out of scope

- Multi-tenancy and cross-tenant isolation (ADR 0008 remains a separate decision and delivery plan).
- Unreviewed autonomous mutation, arbitrary command execution, AI-generated shell, or an agent holding a shared admin token.
- VM testing before the release-candidate gate.
- Claims that a configured peer, an API response, or a green unit test proves a multi-node deployment works in production.
