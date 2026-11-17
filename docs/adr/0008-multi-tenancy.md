# ADR 0008: Multi-tenancy and isolation

- Status: Proposed
- Date: 2026-11-16

## Context

Minga will run workloads for several customers on one cluster. Today, nothing separates them:

- **The stack is a name prefix, not a boundary.** `Secret`, `Config`, `Network` and `Volume` are cluster-level objects (`crates/minga-state/src/types.rs`). A `SecretRef` names a secret ID, and the API checks only that the secret is declared somewhere, not who owns it (`crates/minga-api/src/handlers.rs:519`).
- **Roles are cluster-wide.** An ADR 0005 certificate carries a caller (`CN`) and a role (`OU`), so a deployer can change every stack.
- **The bus has no tenant segment.** Subjects are `minga.task.>`, `minga.node.>` and `minga.egress.>`, all in one NATS account. Workers already get per-node permissions: `verify_and_map` maps a certificate to a user whose ACL comes from `subjects::worker_acl` (`crates/minga-cli/src/sidecar.rs`). Network records used to be shared at `net.<id>`, readable by every worker. They are now projected per node at `node.<n>.net.<id>`, and only to nodes running a task on that network. One gap remains: all logs share one `MINGA_LOGS` stream (512 MiB, discard old).
- **Nodes, the per-node egress proxy and the WireGuard mesh are shared** by everything that runs on them.

## Decision

### 1. Tenant is a first-class object

- `Tenant { id, name, quotas, created_at }` is desired state and goes through Raft.
- Every stack, service, task, secret, config, network, volume, egress route, job, change request and budget has exactly one `tenant`.
- Names are unique within a tenant, not across the cluster, so two tenants can each have a `db_password`.
- **Upgrade:** a cluster created before this ADR gets one tenant, `default`, that owns everything. New fields use `#[serde(default)]` to decode old records, but persisted shape changes still require a format-version bump and migration. This rollout bumps format v3 to v4, rewrites state and Raft log values atomically, and accepts v2/v3 portable snapshots for import. Frozen fixtures from earlier versions remain unchanged.
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
- **Every subject and key carries the tenant** right after the prefix: `minga.<tenant>.task.<id>.status`, `$KV.MINGA_ASSIGN.<tenant>.node.<n>.>`, `$KV.MINGA_ASSIGN.<tenant>.net.<id>`. Tenant IDs use NATS token characters only (`a–z 0–9 - _`) and never start with `$`.
- **Worker permissions become per tenant.** The per-node network grant (`node.<n>.net.>`, already in place) moves under the tenant segment. Consumers are named after the node, so the `$JS.API.CONSUMER.INFO` grant can name only the node's own consumers. `STREAM.INFO` on shared buckets still reveals bucket size; that is accepted and documented.
- **Workers write only to per-tenant streams and buckets:**
  - `MINGA_LOGS_<tenant>`
  - `MINGA_STATUS_<tenant>`
  - `MINGA_USAGE_<tenant>`
  - `MINGA_BUDGET_<tenant>`

  Each has `max_bytes` and `max_age` from the tenant's quota, so one tenant's log flood evicts only its own logs. Buckets written only by managers (`MINGA_ASSIGN`, `MINGA_EVENTS`) and one-key-per-node `MINGA_LEASES` stay shared, keyed by tenant.
- **Identity moves to operator/JWT mode with a full resolver.** Today workers are a static `authorization.users` list built from `--nats-worker-ids`, so adding a worker means regenerating the sidecar config. In the new model:
  - The `MINGA` account has one **scoped signing key per tenant**. Its permission template allows only that tenant's subjects, using user tags for the node (`{{tag(node)}}`).
  - The join flow (§3) mints the node's user JWT with that key, so no sidecar restart is needed for a join or a new tenant.
  - Removing a node revokes its user.
  - A leaked tenant key can mint users for its own tenant only.
  - mTLS stays as the transport.
- **External NATS** (`MINGA_NATS_URL`): a multi-tenant cluster needs the bundled sidecar, or an external server in operator mode with the same account layout. Docs ship the layout, and Minga checks it at startup.
- **Server version.** The release bundles nats-server v2.15.3. CI tests v2.14.10 and v2.15.3; external servers below JetStream API level 4 get a manager startup warning. Managers query `$JS.API.INFO` instead of parsing server version strings. Consumer-limit update failures during mixed-version rollouts warn and retry on the next manager connection.
  - From 2.12, the server rejects unknown fields on JetStream API requests (strict mode). `setup_kv` reads `info().config` and sends it back through `update_stream`. It is accepted on 2.15.3, and the 2.14.10 and 2.15.3 suites cover KV stream updates; the three-server cluster test covers `ensure_replicas`.
  - Changing the lease TTL failed on any server version: a shorter TTL kept the old duplicate window, which the server refuses to keep longer than max age. `setup_kv` now resizes the window too.

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
1. **T1.** `Tenant` type, `tenant` fields with the `default` upgrade, fixtures, and cluster-operator tenant CRUD.
2. **T2.** `O` in certificates, tenant filtering on every API route and in Docker API responses, and the 404 rule. Compose files deploy unchanged, because the tenant comes from the caller.
3. **T3.** Node join tokens per tenant, the hard placement filter, managers without workloads when there are 2+ tenants, and WireGuard peer sets per tenant.
4. **T4.** Tenant segments in subjects and keys, the per-tenant worker ACL, per-tenant streams and buckets, and operator/JWT mode with scoped signing keys per tenant.
5. **T5.** Quotas and the per-tenant audit trail.

T1 and T2 come before agent-plane roadmap order 2 (budgets), because budgets and usage are tenant-owned.
