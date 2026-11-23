# ADR 0008: Multi-tenancy and isolation

- Status: Accepted
- Date: 2026-11-16

## Context

At ADR adoption, Minga had no tenant boundary:

- **The stack is a name prefix, not a boundary.** `Secret`, `Config`, `Network` and `Volume` are cluster-level objects (`crates/minga-state/src/types.rs`). A `SecretRef` names a secret ID, and the API checks only that the secret is declared somewhere, not who owns it (`crates/minga-api/src/handlers.rs:519`).
- **Roles are cluster-wide.** An ADR 0005 certificate carries a caller (`CN`) and a role (`OU`), so a deployer can change every stack.
- **The bus originally had no tenant segment.** Phase 15 adds tenant-first subjects and keys, node-scoped worker credentials, per-tenant status/log/audit resources and manager-only shared assignments/events. Networks are projected per node and tenant.
- **Nodes, the per-node egress proxy and the WireGuard mesh were shared** by everything that ran on them; Phase 15 assigns each worker to one tenant and filters placement and peers accordingly.

## Decision

### 1. Tenant is a first-class object

- `Tenant { id, name, quotas, created_at }` is desired state and goes through Raft.
- Every stack, service, task, secret, config, network, volume, egress route, job, change request and budget has exactly one `tenant`.
- Names are meant to be unique within a tenant, not across the cluster, so two tenants can each have a `db_password`. **Not yet true (#129):** services, secrets, configs, networks and volumes are keyed by their bare name, so the namespace is global. Reading another tenant's name answers 404, as for an absent name, but creating it answers 409 "name is already in use", so a tenant can learn that a name is taken and can claim one another tenant's stack will need. Until #129 keys these tables by tenant and migrates the state format, treat names as cluster-wide: give each tenant's stacks a tenant-specific prefix, and do not rely on a name being free.
- **Upgrade:** a cluster created before this ADR gets one tenant, `default`, that owns everything. New fields use `#[serde(default)]` to decode old records, but persisted shape changes still require a format-version bump and migration. T1 bumps format v4 to v5, migrates v2/v3/v4 state and Raft logs, and accepts v2/v3/v4 portable snapshots. Tenant creation is replicated after initialization, and frozen fixtures from earlier versions remain unchanged. Cluster nodes must upgrade together.
- **Application customers are a different thing.** The tenants of an *application*, such as a hotel served by a marketplace, are called **customers** in Minga. They are attribution labels inside one Minga tenant (agent-plane roadmap B5), never an isolation boundary.

### 2. Tenants are untrusted

Assume a tenant is hostile, both to other tenants and to the control plane:

- A tenant never learns that another tenant's object exists. A reference across tenants returns 404, not 403, and listings filter by tenant before paging.
- On the bus, a tenant's node can reach only its own tenant's subjects, keys and streams, and each tenant's streams have their own limits (§7).
- Each tenant's secret values live only on that tenant's nodes (§3). Raft holds metadata only, as today.
- There is no shared mutable resource between tenants without an explicit cluster-admin grant: no shared egress route, cache, volume or network.

### 3. Dedicated nodes for every tenant

- A worker node belongs to exactly one tenant. The tenant is fixed when the node joins: the join token is minted for a tenant, and the node's certificate records it.
- Moving a node to another tenant means draining it, wiping its data directory and secret store, and joining it again with a new token.
- The scheduler never places a task on a node of another tenant. This is a hard filter in `check_placeable`, not a soft constraint, and a tenant with no ready nodes leaves its tasks pending with that reason.
- **Managers run no tenant workloads once a cluster has two or more tenants.** The scheduler enforces this, and a deploy that would break it is refused. A single-tenant cluster (topology T1, one node) keeps working as it does today.
- Everything local to a node (egress proxy, response cache, module and image cache, host directory roots, ports) is therefore single-tenant by construction.
- **The WireGuard mesh peers only within a tenant.** A worker's peer set is its own tenant's nodes plus the managers. Overlay networks belong to one tenant and never span two.

### 4. Identity: the tenant is in the client certificate

This extends ADR 0005:

- The certificate carries the caller (`CN`), the role (`OU`) and the tenant (`O`). `minga tls issue-client` gains `--tenant`. As before, a role or tenant change means reissuing the certificate.
- Roles apply within the tenant: `viewer`, `agent` (agent-plane roadmap D12), `deployer` and `admin`.
- **Cluster operators** hold `O=*`, the only cross-tenant identity. They can create and delete tenants, set quotas, mint node join tokens and read cluster health. They cannot read or change a tenant's stacks unless they hold a certificate for that tenant.
- A certificate with no `O` is accepted only while the cluster has just the `default` tenant, and maps to it. Once a second tenant exists, such a certificate gets 403 with a message saying to reissue it.
- Workload identity tokens (roadmap C9) carry a `tenant` claim and are signed with a **per-tenant key**. Each tenant's JWKS is served at `/tenants/<id>/.well-known/jwks.json`, so a verifier trusts only its own tenant.

### 5. The shared control plane is protected

Managers, Raft and JetStream are shared, so each tenant has quotas:

- object counts (services, tasks, secrets, networks, jobs)
- the size of desired state in Raft
- API request rate
- JetStream bytes and age for each of the tenant's streams (§7)

A tenant over quota gets 429 or 507 with the quota named. Quotas are desired state, and only cluster operators set them.

### 6. Audit

- The audit line gains `tenant`.
- A tenant `admin` reads only their own tenant's audit trail. Cluster operators read all of it.
- Tenant creation, quota changes and node join or remove are audited with `tenant` set to the target tenant.

### 7. Bus layout

**Tenants never connect to Minga's NATS.** Only managers and workers do, and a worker runs on hardware dedicated to one tenant. The threat is a tenant who escapes a container and uses the worker's bus credentials. The defence is permissions scoped to that node and tenant, not one NATS account per tenant: accounts would split the control-plane data that managers must read as a whole, and need cross-account imports to put it back together.

- **One `MINGA` account for the control plane**, plus the NATS `SYS` account. Per-tenant accounts are kept for a later tenant bus, where tenant code does connect (agent-plane roadmap F16).
- **Every subject and key carries the tenant** immediately after the prefix: `minga.<tenant>.task.<id>.status`, `$KV.MINGA_ASSIGN_<tenant>.<tenant>.node.<n>.>`, `$KV.MINGA_LEASES.<tenant>.lease.node.<n>`. Tenant IDs use NATS token characters only (`a–z 0–9 - _`) and never start with `$`.
- **Worker permissions are tenant-and-node scoped.** Assignment consumers get random names, and the worker ACL pins each consumer's filter subject to the worker's own tenant and node prefix; on the shared buckets a worker has no `STREAM.INFO` or `CONSUMER.INFO`, which would return other tenants' keys or consumer filters; a worker reads only its own task, network, ingress, WireGuard, lease and status keys, and writes only its own lease, status and logs. It cannot publish shared lifecycle events or audit records. `STREAM.INFO` on shared buckets still reveals bucket size; that is accepted and documented.
- **Worker resources are tenant-scoped:**
  - `MINGA_LOGS_<tenant>` and `MINGA_STATUS_<tenant>` are worker-writable.
  - `MINGA_USAGE_<tenant>` and `MINGA_BUDGET_<tenant>` are pre-created for tenant accounting.

  Tenant `jetstream_bytes` and `jetstream_age_secs` limits configure each tenant stream and KV bucket. `MINGA_EVENTS` is shared and manager-only; `MINGA_LEASES` is shared with tenant-first keys. Assignments are one bucket per tenant, `MINGA_ASSIGN_<tenant>` (manager-written, capped at 5,000 consumers): a worker can create consumers of any name on its own tenant's bucket and its ACL cannot count them, so they may fill only that bucket's limit, never another tenant's (#131).
- **Identity uses operator/JWT mode with a full resolver.** The `MINGA` account has one scoped signing key per tenant. Its permission template allows only that tenant's subjects, using user tags for the node (`{{tag(node)}}`). The join flow (§3) mints the node's user JWT with that key, so new workers need no sidecar restart.
  - Removing a node revokes its user; revoked public keys are recorded in replicated tenant state so account claims remain revoked after manager restart.
  - A leaked tenant key can mint users for its own tenant only. mTLS remains the transport.
  - **Initial key custody:** NATS operator/account and tenant-scoped signing keys are manager-local files with restrictive permissions; they are never stored in Raft, JetStream, logs, or worker images. This is a software-signer deployment, not an HSM-equivalent boundary.
  - Each manager that may issue credentials needs the same signer material locally; it is not replicated. A later signer-provider integration can move key operations to an HSM, Vault, or service without changing the worker credential contract.
- **External NATS** (`MINGA_NATS_URL`): a multi-tenant cluster needs the bundled sidecar or an external server in operator mode with the same account layout. Minga checks the layout at startup.
- **Server version.** The release bundles nats-server v2.15.3. CI tests v2.14.10 and v2.15.3; external servers below JetStream API level 4 get a manager startup warning. Managers query `$JS.API.INFO`, not version strings.

## Rule for every change: tenancy checklist

Every plan, ADR and PR that adds state, an endpoint, a bus subject, a node-local resource or a CLI command answers these seven questions in its description:

1. **Owner.** Which tenant owns each new object, and does its Raft key or bus subject include the tenant?
2. **Access.** Which role, in which tenant, may read or change it? Does a reference across tenants return 404?
3. **Bus.** Do its subjects and keys carry the tenant? Does the tenant's permission template stay minimal? Is anything a worker writes in a per-tenant stream with limits? (§7)
4. **Node.** Does it run only on the tenant's nodes? Is any node-local resource shared across tenants?
5. **Shared plane.** Can one tenant exhaust managers, Raft or JetStream through it? Which quota bounds it?
6. **Audit.** Does each write log `tenant`?
7. **Test.** Is there a test with two tenants proving that tenant B cannot see, use or exhaust tenant A's object?

"Not applicable" is a valid answer, but it must come with a reason.

## Consequences

- **Positive:**
  - Customers can share a cluster without sharing machines, bus credentials, secret values or network peers.
  - The node-local work in the agent plane (egress, caches, sandboxes) needs no tenant keying of its own, because a node serves one tenant.
- **Negative:**
  - Small edge sites with several tenants need at least one manager plus one worker per tenant.
  - Every API handler, scheduler filter and bus subscriber has to change.
  - Operator/JWT mode and renamed subjects change how workers and managers connect, so the upgrade needs a bus migration.
- **Not covered:**
  - Tenants sharing hardware: there is no co-location, so there are no microVM or cgroup fairness guarantees to give.
  - Billing.
  - Federation across clusters.

## Alternatives considered

| Alternative | Verdict |
|---|---|
| Tenant = stack | Rejected. A customer with several stacks would get no shared quota, roles or secrets, and the stack name stays a weak prefix. |
| One cluster per tenant | Rejected as the only model. It is too heavy for edge sites that host several small customers. It remains possible. |
| Shared nodes with namespaces and cgroups | Rejected. Tenants are untrusted, and kernel-level co-location raises the isolation bar beyond what containerd and wasmtime alone give us. |
| Tenant from a server-side table keyed by `CN` | Rejected for the same reason ADR 0005 rejected roles in a table: a second source of truth. |

## Delivery

0. **T0 — complete (Phase 14).** The release bundles nats-server v2.15.3, external NATS v2.14.10 and v2.15.3 pass the bus suite, manager feature gating uses API level 4, and single-server plus rolling upgrade proofs passed.
1. **T1 — complete.** The `Tenant` type and quotas are persisted desired state; existing clusters receive a replicated `default` tenant. Cluster operators can create, list, update and delete empty tenants through `/minga/tenants`, authenticated by the current cluster admin token. Deleting `default` or a tenant that owns resources is refused.
2. **T2 — complete.** `minga tls issue-client` issues client-auth certificates with caller (`CN`), role (`OU`) and tenant (`O`); managers can require API mTLS with a dedicated client CA. Tenant-scoped Docker routes and nested responses filter by tenant, and foreign lookups return 404. Tenant CRUD, node inventory, cluster info and cluster health/metrics routes require a cluster operator (`O=*` or the configured API token); `O=*` operators do not receive tenant workload access. Missing `O` maps to `default` only while it is the sole tenant. A bearer token retains default-tenant workload access only while `default` is the sole tenant.
3. **T3 — implementation complete; verification open.** The hard placement filter, manager workload
   exclusion and tenant-scoped WireGuard peer sets are implemented.
   `POST /nodes` pre-assigns a tenant; cluster operators can mint a 15-minute
   node-bound join token at `POST /minga/nodes/{id}/join-token`. Managers
   verify its HMAC, node ID and tenant before accepting a non-default lease,
   and accept it only for an existing node record in the token's tenant, so
   deleting the node revokes outstanding tokens. They then persist the joined
   tenant in JetStream so later renewals cannot change it or require a live
   token. Until that record exists, the scheduler gives a non-default node no
   tasks and no WireGuard peers, even with a live lease. The same owner-only 32-byte signing key must be
   provisioned on every manager. T3 implementation merged in PR #98; end-to-end
   tenant-isolation verification remains open.
4. **T4 — implementation complete.** Tenant-first bus subjects and keys, tenant-and-node worker ACLs, per-tenant status/log/audit resources, manager-only shared events and assignments, scoped operator/JWT credentials and persisted NATS user revocation.
5. **T5 — implementation complete.** Tenant object/state quotas and API request rates are enforced; JetStream byte/age limits apply to each tenant stream and KV bucket; tenant-scoped audit is available at `/v{version}/audit` and cluster-operator audit at `/minga/audit`.

T1 and T2 come before agent-plane roadmap order 2 (budgets), because budgets and usage are tenant-owned.
