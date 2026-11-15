---
phase: 12
title: Managers-only Raft
depends_on: ["v1.2.0"]
requirements: [MOR-ADR, MOR-LEASE, MOR-STATUS, MOR-ASSIGN, MOR-AUTHZ, MOR-JOIN, MOR-FOLD, MOR-NATS, MOR-PART, MOR-PROOF]
status: planned
---

# Phase 12 — Managers-only Raft

<objective>Only managers vote in Raft. Workers become plain JetStream clients that get their assignments from the bus, report status and leases there, and can join a running cluster. Managers can be added to and removed from a running cluster. This removes structural limits 1–3 in `docs/architecture.md`, and makes quorum independent of edge workers.</objective>

## Where the code is today (v1.2.0, read 2026-11-15)

| Concern | Today | File |
| --- | --- | --- |
| Membership | Every `minga manager`, `minga worker` and `minga api` opens a `StateStore`, and membership is the `--peer` list. `open_inner` calls `raft.initialize(members)` when pristine, so a node without `--peer` bootstraps its own cluster. | `crates/minga-state/src/store.rs:88-170`, `crates/minga-cli/src/main.rs:478-502` |
| Liveness | `NodeMembership` proposes `RenewNodeLease` every 10 s. Placement uses `last_seen_unix` with `NODE_LEASE_TTL_SECS` = 30, judged by comparing the node's clock with the leader's. The bus lease (`refresh_lease`/`is_alive`, node clock in millis) is written but no caller reads it. | `crates/minga-node/src/registration.rs`, `crates/minga-scheduler/src/placement.rs:32`, `crates/minga-bus/src/nats.rs:660` |
| Dispatch | There are two paths. A bus `Assign` is only a hint. The supervisor starts a task only when its Raft replica assigns the task to it, and always from the Raft spec. That rule is the **only** thing stopping a bus publisher from running its own spec. | `crates/minga-node/src/supervisor.rs:431, 1011, 1233`, `docs/architecture.md` limit 3 |
| Actual task state | The supervisor writes `state`, `running_since_unix`, `restarts`, `last_error` and `last_exit_code` into the Raft `Task`. Rollouts and the Docker API (Traefik) read them from there. | `supervisor.rs:903-941` |
| DNS data | The supervisor reads `list_services`, `list_tasks` and `list_networks` from its replica to build the records for each bridge. | `supervisor.rs:326-363` |
| `minga api` | A separate process and its own Raft voter, so one host holds two voters. | `main.rs:630` |

openraft is 0.9 (`add_learner` and `change_membership` are available). Raft RPCs are AppendEntries, Vote and InstallSnapshot only (`crates/minga-state/src/network.rs:169`).

The sidecar config generator already writes NATS cluster routes with route TLS (`ClusterTls` in `crates/minga-cli/src/sidecar.rs:63-80`), but the CLI never sets them (`main.rs:298`: "the built-in sidecar is a single server"). Today a multi-node cluster needs an external NATS cluster, because a sidecar on each manager would split the bus.

## Topologies this phase must handle

These are cluster shapes. Air-gapped, Talos, the container image and running from a workstation (`docs/topologies.md`) are ways of deploying one of these shapes, and they don't change the design.

| Shape | Managers (Raft voters) | Workers | NATS | What must hold | Proved in |
| --- | --- | --- | --- | --- | --- |
| **T1 Single host** | 1, serving the API (12-06) | 0 (the manager runs tasks) | The manager's sidecar, unclustered | One process and one voter. Restart and `state export`/`import` round-trip. | 12-06, 12-07 |
| **T2 Edge site** | 1 | A few, on links that drop out | The manager's sidecar. Workers connect as clients. | A worker that is down or cut off never stalls a deploy. When the manager is down, workers keep running their last assignments and placement waits. | 12-04, 12-07 |
| **T3 HA site** | 3 or 5 | Any number | Sidecars on the managers, clustered with route TLS, `--nats-replicas 3`. An external NATS cluster stays supported. | Losing one manager (and its NATS server) keeps quorum and the bus. Managers join and leave while the cluster runs. | 12-05, 12-07 |
| **T4 Large on-prem** | 3 or 5 | Up to about 5,000 (ADR 0001) | As T3, or external | Worker count adds no Raft load: no per-worker Raft writes or replication. | Functional in 12-07; the load test is an open question |

Cross-node container traffic is out of scope in every shape until the WireGuard mesh lands. In T2–T4, services that talk to each other stay on one node with placement constraints, as in v1.2.0.

## Decisions to record in ADR 0006 before code moves (plan 12-01)

1. **Workers leave Raft. They are not learners.** The ADR names learners as the runner-up and rejects them: learners still replicate full state, and liveness would still go through Raft.
2. **Assignments live in a JetStream KV bucket, `MINGA_ASSIGN`.** The key is `node.{node_id}.task.{task_id}`. The value is the task spec plus its network attachments, encoded with the state codec and carrying the format version. The leader's scheduler writes it after each successful Raft placement. Raft stays the source of truth, and the leader re-derives the bucket from Raft on election, so a lost KV write heals itself.
3. **Only managers may write assignments (MOR-AUTHZ).** Without this, workers leaving Raft is a **security regression**: today the Raft cross-check stops a bus publisher from starting a spec, and that check goes away. The sidecar's NATS config gets subject permissions bound to the client certificate (`verify_and_map`):
   - Managers may write `MINGA_ASSIGN` and read everything.
   - Worker *N* may read only `$KV.MINGA_ASSIGN.node.N.>`, and may write only its own lease, status and log subjects.
   - The token-only (non-mTLS) mode stays dev-only, and the docs must say that it gives no isolation between workers.
4. **Actual task state moves to `MINGA_STATUS` (MOR-STATUS).** The supervisor stops writing to Raft. The scheduler (rollouts, restarts) and the Docker API task views read actual state from the status KV. The Raft `Task` fields stay in place, keep `#[serde(default)]` and are no longer written, so the format version doesn't change. Recommendation: **do not** mirror status back into Raft, because that puts heartbeat-rate writes on Raft again.
5. **Liveness comes from `MINGA_LEASES`, stamped by the server (MOR-LEASE).** Set the bucket's `max_age` to the TTL (30 s), so an expired lease disappears on the NATS server, and "alive" means "the key exists". No node clock is compared. `RenewNodeLease` stays decodable, because old Raft logs replay it, but nothing proposes it any more.
6. **Registration goes over the bus.** A worker publishes its identity, capacity and labels with its first lease. The leader turns the first sighting into a Raft `upsert_node`. Later sightings never overwrite operator-set labels or availability, and never recreate a removed node, the same rules `renew_node_lease` enforces today. The node ID must match the client certificate (MOR-AUTHZ).
7. **The operator adds a manager through the API, with the admin token (MOR-JOIN).** `POST /minga/managers {id, raft_addr}` runs `add_learner`, waits until the new node catches up, then runs `change_membership`. `DELETE /minga/managers/{id}` refuses to drop below quorum or remove the last manager. A new manager starts with `--join`, which means "never bootstrap; wait to be added". This adds no new Raft RPC, and it reuses the existing API authentication.
8. **Migrating from v1.2.0.** Upgrade the managers first. On the leader, one `change_membership` to the manager set drops the worker voters. Upgraded workers ignore an old `raft_log.redb` or `state.redb` in `--data-dir`, logging once that they can be deleted, and never open them. Every node is still upgraded in one window, because the wire rule in `docs/architecture.md` is unchanged.
9. **`minga api` folds into `minga manager` (MOR-FOLD).** `minga manager --api-bind` serves the Docker API in the manager process, so a single host has one voter instead of two. `minga api` exits non-zero with a message that points at `--api-bind`. It is not kept as a silent alias.
10. **The managers' sidecars form the NATS cluster (MOR-NATS).** Each manager's sidecar routes to the other managers' sidecars over route TLS, using the `ClusterTls` support that already exists, and workers connect to the managers' seed list as clients. This adds no new process, and NATS quorum then follows Raft quorum: the same 3 or 5 hosts. External NATS stays an option for sites that already run one. The runner-up is a NATS leaf node on each worker, which would buffer bus traffic while a link is down. It's rejected for this phase because it adds a process on every worker, but it's the first thing to revisit if T2 links turn out to be worse than "drops for minutes".
11. **A worker cut off from the bus keeps what it runs (MOR-PART).** The supervisor must treat an error reading `MINGA_ASSIGN` as "no news" and never as an empty assignment set. Only a successful read that drops a task stops that task. Once the worker's lease expires (30 s, on the NATS server), the leader reschedules its tasks elsewhere when constraints allow. Tasks pinned to that node by label or volume stay put. When the link heals, the worker reads its now-smaller assignment set and stops the moved tasks. For a while, both copies run. That's at-least-once, the same as a Swarm worker partition, and the docs must say so. A per-service "never reschedule on lease loss" option is out of scope.

## Plans

Order: 12-01 first. Then 12-02 → 12-03 → 12-04 in sequence, because each step removes one more Raft dependency from workers. 12-05 depends only on 12-01 and can run in parallel with 12-02…12-04. 12-06 and 12-07 come last.

### 12-01 — ADR 0006: Workers leave Raft
- [ ] Write `docs/adr/0006-workers-leave-raft.md` with decisions 1–11, the topology table, the rejected learner and leaf-node options, and the security argument in decision 3.
- [ ] Update the "Target shape" section of `docs/architecture.md` to point at it.
- **Done when:** the ADR is merged. No code changes in this plan.

### 12-02 — Liveness and registration off Raft (MOR-LEASE)
- [ ] Give `MINGA_LEASES` a `max_age` equal to the TTL. `is_alive` means the key exists. The lease value carries the registration payload.
- [ ] Turn `NodeMembership` into a bus client: it calls `refresh_lease` every 10 s and makes no Raft proposals.
- [ ] Add a leader-only task that registers newly seen nodes in Raft, following the same rules as today: no undrain, no recreating a removed node.
- [ ] Switch placement eligibility (`placement.rs:32`) from `last_seen_unix` to the bus lease.
- [ ] Tests: the existing `renewal_does_not_*` tests rewritten for the bus path, and a node whose lease expires on the NATS server becomes ineligible without any clock comparison.
- **Done when:** a running cluster makes no Raft writes while idle (measure the Raft log index over 60 s).

### 12-03 — Actual task state off Raft (MOR-STATUS)
- [ ] The supervisor writes lifecycle, restarts and failures (`record_failure`, `clear_settled_failures`) to `MINGA_STATUS` only.
- [ ] Rollouts and restart logic in `minga-scheduler` read actual state from the status KV.
- [ ] The Docker API task views (`minga-api/src/translate.rs`) merge Raft desired state with KV actual state.
- [ ] Pin with tests: `translate::tests::a_failed_task_reports_its_reason_and_exit_code` and the rolling-update tests pass against KV-sourced state.
- **Done when:** Traefik still discovers live tasks (the Gate 4 setup), and `minga service ps` shows failure reasons.

### 12-04 — Assignment KV, workers without a StateStore, one dispatch path (MOR-ASSIGN, MOR-AUTHZ)
- [ ] Add a `MINGA_ASSIGN` bucket, kept in `REPLICATED` in `minga-bus`.
- [ ] The leader writes assignments after placement and re-derives the whole bucket on election.
- [ ] Network and DNS data for the networks a node hosts go under `net.{network_id}` in the same bucket, replacing the supervisor's `list_services`, `list_tasks` and `list_networks` reads.
- [ ] Give the supervisor a narrow `Assignments` source (watch plus list for this node), implemented on the KV. Managers use the same path: one dispatch path.
- [ ] Delete the bus `Assign`/`Stop` dispatch and the Raft fallback in the supervisor. `bus::Command` has only `Assign` and `Stop` (`crates/minga-bus/src/lib.rs:92`), so the `MINGA_COMMANDS` stream goes away along with `MINGA_DEAD_LETTERS`, if nothing else writes there. If it goes, remove `minga_bus_dead_letters_total` (`minga-telemetry/src/metrics.rs:34`) and its `minga-ctl metrics` view in the same change.
- [ ] `minga worker` stops opening a `StateStore` and takes no `--peer`. Passing `--peer` to a worker is an error, so nothing is silently ignored.
- [ ] The sidecar NATS config gets the per-identity subject permissions from decision 3.
- [ ] Tests:
  - A worker certificate cannot write `MINGA_ASSIGN` or another node's lease or status (live NATS test, mTLS).
  - The supervisor's adopt-or-stop-on-start behaviour holds when the source is the KV.
  - An assignment from an older format version is refused loudly.
  - With the bus unreachable, the supervisor keeps every running task. After reconnecting, it stops only the tasks its assignment set dropped (decision 11).
- **Done when:** a worker that has never had a Raft data directory runs, restarts and loses tasks correctly.

### 12-05 — Join and remove managers (MOR-JOIN)
- [ ] Add `--join` to `minga manager`: open the store without `initialize`.
- [ ] Add `POST` and `DELETE /minga/managers`, behind the admin token: `add_learner` → catch up → `change_membership`. Refuse a change that would lose quorum.
- [ ] Workers join with nothing more than NATS credentials, a node ID and the cluster CA. Document that in `docs/cli.md`.
- [ ] Cluster the managers' sidecars (decision 10): add `--nats-cluster-route` and the route TLS flags to `minga manager`, and wire them into `SidecarOptions.cluster`. A joining manager's sidecar routes to the existing ones. Drop the "single server" restriction at `main.rs:298`.
- [ ] Test: with three managers on clustered sidecars, stopping one manager process keeps both Raft writes and bus reads and writes working.
- [ ] Add the v1.2.0 migration step (decision 8) as a `minga-ctl` or CLI command, with a dry run that lists the voters it would remove.
- [ ] Tests (multi-node, in-process, like `multi_node.rs`):
  - A fourth manager joins a running 3-manager cluster and sees its state.
  - Removing a manager keeps quorum.
  - Removing the last manager is refused.
  - A node started without `--peer` and with `--join` does not bootstrap.
- **Done when:** a running 3-manager cluster grows to 5 and shrinks back to 3, with no restart.

### 12-06 — Fold `minga api` into the manager (MOR-FOLD)
- [ ] Add `minga manager --api-bind` and `--api-token`, reusing `build_router_with_auth`.
- [ ] `minga api` exits non-zero with a message that points at `--api-bind`.
- [ ] Update `docs/topologies.md`, `docs/hetzner-arm-staging.md`, `docs/backup.md`, `docs/cli.md` and README §Topologies: one process and one voter per host.
- **Done when:** the single-host topology is one process, and `minga state export` and `import` still round-trip.

### 12-07 — Live proof and release notes (MOR-PROOF)
On OrbStack Ubuntu, and on more VMs where available, run one setup per shape in the topology table: T1, T2 (1 manager + 2 workers), and T3 (3 managers + 2 workers on clustered sidecars). All of them use Raft mTLS and NATS mTLS. Check each of these:
- [ ] T1: one process serves the API and runs tasks, and it survives a restart.
- [ ] T2: cut a worker's network for 2 minutes. Its tasks keep running, the moved copies start elsewhere, and after the network heals the worker stops its moved tasks.
- [ ] T2: stop the manager. The workers' tasks keep running, and placement resumes when the manager returns.
- [ ] T3: stop one manager. Deploys and `minga logs` keep working.
- [ ] Stopping both workers does not stall `minga deploy`: services are accepted, and placement waits or fails with a 409.
- [ ] A third worker joins at runtime and receives tasks.
- [ ] A manager is added and then removed while services keep running.
- [ ] A worker certificate trying to write an assignment is rejected by NATS.
- [ ] A v1.2.0 cluster migrates (decision 8) with its services intact.
- [ ] Rewrite the multi-node section of `docs/topologies.md` around T2–T4, including the at-least-once note from decision 11.
- [ ] Update `docs/architecture.md`, removing limits 1–3 and keeping 4–5. Write the CHANGELOG entry with known limits stated honestly. Move "Managers-only Raft" to shipped in `site/src/data/roadmap.ts`.
- **Done when:** the evidence for each item above is recorded in `.planning/STATE.md`.

## Cross-cutting constraints
- CLAUDE.md applies: only desired state and placement go through Raft, and Compose files still deploy unchanged.
- No new process. NATS stays the existing sidecar.
- Persisted types keep the frozen fixtures green (`crates/minga-state/tests/fixtures/`). A field shape change needs a format bump and a migration.
- No security step back. After 12-04, a bus identity must not be able to do anything it couldn't do in v1.2.0.
- Each plan runs `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings` and `cargo test` before it merges.

## Open questions for the user
- Does the scale target in ADR 0001 (5,000 nodes) need a load test in 12-07, or is the functional proof enough for this phase?
