# ADR 0006: Workers Leave Raft

**Status:** Accepted — implemented in v1.3.0-rc.1  
**Date:** 2026-11-15  
**Deciders:** Ferru

## Context

This ADR is implemented in v1.3.0 (see the [release notes](../release-1.3.0.md)); the Context describes v1.2.0, before the change.

In v1.2.0, managers, workers, and `minga api` each open a `StateStore`. A pristine store initializes Raft from the `--peer` list, so nodes without peers can bootstrap separate clusters. Every node is a voter, including workers whose links may be intermittent. Node liveness, task lifecycle, and worker dispatch also depend on each process's Raft replica.

The bus already carries logs, events, and leases, but leases are not used for placement. A bus `Assign` is only a hint: the supervisor starts tasks only when its Raft replica assigns them to that node, and it takes the task specification from Raft. That cross-check is a security boundary; replacing it without equivalent authorization would let a bus publisher start an arbitrary specification.

Raft RPCs are AppendEntries, Vote, and InstallSnapshot. Openraft 0.9 supports adding learners and changing membership, so managers can be changed at runtime without adding an RPC. NATS remains the existing sidecar; its config generator already supports route TLS.

This ADR changes the target architecture, not the persisted Raft field shape. Existing `Task` status fields remain decodable and retain `#[serde(default)]`; the new sources of runtime state are JetStream KV buckets.

## Decision

### 1. Workers are not Raft members

Only managers vote in Raft. Workers are plain JetStream clients, not Raft learners. A worker receives assignments, reports lifecycle and leases over the bus, and does not open or replicate a `StateStore`.

Learners were considered and rejected: they still replicate the full state, and would leave liveness dependent on Raft. Removing workers from consensus also makes quorum independent of edge-worker count.

### 2. Assignments live in JetStream KV

Use a `MINGA_ASSIGN` bucket with `REPLICATED` storage. Assignment keys are `node.{node_id}.task.{task_id}`. Values contain the task specification and network attachments, encoded with the state codec and carrying its format version.

After each successful Raft placement, the leader writes the resulting assignments. On election, the leader re-derives and writes the whole bucket from Raft. Raft remains authoritative for desired state and placement; the KV is its recoverable dispatch projection. A lost KV write is repaired from Raft.

Network and DNS data needed by a node use `net.{network_id}` in the same bucket. Workers and managers consume assignments through the same supervisor path.

### 3. Bus authorization preserves the Raft dispatch boundary

Only managers may write `MINGA_ASSIGN` and read all cluster data. Worker identity is bound to its client certificate and node ID. Worker N may read only `$KV.MINGA_ASSIGN.node.N.>` and may write only its own lease, status, and log subjects. The sidecar's NATS subject permissions are bound to the verified client certificate through `verify_and_map`.

This is required because the Raft cross-check that currently prevents a bus publisher from supplying a runnable specification disappears when workers leave Raft. The manager-only assignment write permission replaces that security boundary: a worker identity cannot create or change assignments, read another worker's assignments, or write another node's lease or status. Token-only, non-mTLS mode remains development-only and provides no isolation between workers; documentation must state this explicitly. No bus identity may gain an ability it did not have in v1.2.0.

### 4. Actual task state lives in `MINGA_STATUS`

The supervisor writes lifecycle state, `running_since_unix`, restart counts, failure details, and exit codes to `MINGA_STATUS`, not Raft. Rollouts, restart decisions, Docker API task views, and Traefik discovery combine Raft desired state with actual state from this KV.

The existing status fields on the persisted Raft `Task` stay in place, remain `#[serde(default)]`, and are no longer written. Do not mirror status back into Raft: heartbeat-rate writes would restore the Raft load this change removes.

### 5. Liveness is a server-expiring bus lease

Use `MINGA_LEASES` with `max_age` equal to the 30-second node lease TTL. `is_alive` means the lease key exists. NATS stamps expiration; placement does not compare the worker's clock with the manager's clock.

A lease value carries the node's registration payload. `NodeMembership` refreshes the lease every 10 seconds and makes no Raft proposals. `RenewNodeLease` remains decodable so old Raft logs replay, but no new proposal uses it.

### 6. Registration is driven by the first bus lease

A worker publishes its identity, capacity, and labels with its first lease. A leader-only task registers a newly seen node in Raft. Subsequent leases do not overwrite operator-set labels or availability, and do not recreate a removed node, preserving the rules of `renew_node_lease` today. The node ID must match the client certificate.

### 7. Managers join and leave at runtime

An administrator adds a manager through the authenticated API: `POST /minga/managers` with `{id, raft_addr}` runs `add_learner`, waits for the learner to catch up, then calls `change_membership`. `DELETE /minga/managers/{id}` refuses to remove a manager if the result would be below quorum or would remove the last manager.

A new manager starts with `--join`, meaning it never bootstraps a cluster and waits to be added. The API uses the existing admin-token authentication. This adds no Raft RPC.

### 8. Migration from v1.2.0 is one coordinated upgrade window

Upgrade the managers first. On the leader, change membership once to the manager set, removing worker voters. Upgraded workers ignore an old `raft_log.redb` or `state.redb` found in `--data-dir`, log once that it can be deleted, and never open it. Upgrade every node in one window because the Raft wire rule remains unchanged.

### 9. `minga api` folds into `minga manager`

`minga manager --api-bind` serves the Docker API in the manager process, giving a single-host topology one process and one Raft voter. `minga api` exits non-zero with a message pointing to `--api-bind`; it is not retained as a silent alias.

### 10. Manager sidecars form the NATS cluster

Each manager's existing NATS sidecar routes to the other managers' sidecars over route TLS, using the existing `ClusterTls` support. Workers connect to managers' seed list as NATS clients. In a three- or five-manager cluster, configure `--nats-replicas 3`. External NATS remains supported for sites that already operate a cluster.

A NATS leaf node on every worker was considered and rejected for this phase: it could buffer traffic during a link outage, but adds a process to every worker. Revisit it if minutes-long T2 link outages prove insufficiently tolerated by the partition behavior in Decision 11.

### 11. Bus partitions preserve running tasks

When the supervisor cannot read `MINGA_ASSIGN`, it treats the error as no news and keeps its running tasks. It must not interpret a failed read as an empty assignment set. Only a successful read that removes a task stops it.

When a worker's lease expires at the NATS server, the leader reschedules its tasks elsewhere when constraints allow. Tasks pinned to that node by labels or volumes stay there. When connectivity returns, the worker reads the reduced assignment set and stops tasks that moved. Both copies may run during this interval: delivery is at-least-once, as with a Swarm worker partition. A per-service “never reschedule on lease loss” option is out of scope.

## Required topologies

These are cluster shapes; air-gapped, Talos, container-image, and workstation deployments are ways to run them and do not change the design.

| Shape | Managers (Raft voters) | Workers | NATS | Required behavior | Proof plan |
| --- | --- | --- | --- | --- | --- |
| **T1 Single host** | 1, serving the API | 0; the manager runs tasks | The manager's unclustered sidecar | One process and one voter. Restart and `state export`/`import` round-trip. | 12-06, 12-07 |
| **T2 Edge site** | 1 | A few, on links that drop out | The manager's sidecar; workers connect as clients | A down or isolated worker never stalls a deploy. With the manager down, workers keep running their last assignments and placement waits. | 12-04, 12-07 |
| **T3 HA site** | 3 or 5 | Any number | Sidecars on managers, clustered with route TLS and `--nats-replicas 3`; external NATS remains supported | Losing one manager and its NATS server preserves quorum and the bus. Managers join and leave while running. | 12-05, 12-07 |
| **T4 Large on-prem** | 3 or 5 | Up to about 5,000 (ADR 0001) | As T3, or external | Worker count adds no Raft load: no per-worker Raft writes or replication. | Functional proof in 12-07; load test is an open question |

Cross-node container traffic remains out of scope in every shape until the WireGuard mesh lands. In T2–T4, services that communicate with each other stay on one node with placement constraints, as in v1.2.0.

## Consequences

- Raft quorum depends on managers, not workers. Workers do not replicate cluster state or generate Raft lease/status writes.
- Assignments and actual status are bus projections; the leader can reconstruct assignments from Raft after election, while actual status is owned by the status KV.
- Workers keep their last assignments through a bus partition. Lease expiry allows rescheduling, with a temporary duplicate-run window and at-least-once behavior.
- The NATS sidecars on managers must be clustered for HA deployments; route TLS and certificate-bound subject permissions are part of the trust boundary.
- The single-host API and manager run in one process. The separate `minga api` command is intentionally removed.
- Existing Compose files and persisted Raft field shapes remain unchanged. Unknown Compose keys continue to fail loudly; any future persisted-field shape change requires a format bump and migration.

## Verification plan

- T1: one process serves the API and runs tasks; restart and state export/import round-trip.
- T2: isolate a worker for two minutes; tasks continue running there, moved copies start elsewhere, and the isolated worker stops moved tasks after reconnection. Stop the manager; worker tasks continue and placement resumes after manager recovery.
- T3: with three managers and clustered sidecars, stop one manager; deploys and `minga logs` continue. Add and remove a manager while services run.
- Stop both workers; deploy still accepts services and placement waits or returns 409.
- Add a worker at runtime and verify it receives assignments.
- Prove a worker certificate cannot write assignments and that a worker cannot write another node's lease or status.
- Migrate a v1.2.0 cluster with services intact.
- For T4, verify functionally that worker count introduces no Raft writes or replication. Whether a 5,000-node load test is required remains open.
