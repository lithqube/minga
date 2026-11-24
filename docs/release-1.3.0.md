# Release notes: v1.3.0 (candidate v1.3.0-rc.1)

The authoritative change list is the [changelog](../CHANGELOG.md); this page
records what 1.3.0 contains, what was checked, what was not, and how to
upgrade from 1.2.0. The release workflow checks that the tag matches
`[workspace.package] version` in `Cargo.toml`, so the candidate is tagged
`v1.3.0-rc.1` and the final release `v1.3.0` after the version is bumped.

**This is a release candidate.** It exists so the checks that need real
machines can run against one exact build. Those checks are listed under
[Not yet verified](#not-yet-verified); 1.3.0 is not released until they pass.

## What is in it

1.3.0 changes how a cluster is built, not just what it does. Raft moves to the
managers alone, nodes talk over a WireGuard overlay, tenants are isolated from
each other, and agents get an egress and usage plane. Pull requests are the
numbers in the list; the changelog has the detail and the upgrade steps.

| Area | Change | PRs |
| --- | --- | --- |
| Raft | Only managers vote. Workers get assignments, leases and task status over JetStream with identity-scoped mTLS and no longer open a Raft store. Managers join and leave at runtime; `minga api` is folded into `minga manager --api-bind` | #70 to #76 |
| Overlay | Unique per-network subnets allocated by the leader from `--overlay-supernet`; kernel WireGuard between nodes with `/32` routes and network isolation; NAT-isolated workers relay through a manager; routing-mesh ingress publishing | #68, #79 to #84, #86, #100, #104 |
| NATS | nats-server 2.15.3 is bundled; 2.14 and 2.15 are supported and tested; the 2.10.22 store upgrade is proven; the per-stream consumer cap of 2.15 is lifted | #87 to #90 |
| Tenants | Tenants are first-class: ownership on every resource, tenant in the API client certificate, signed node enrollment, tenant-first bus subjects and keys, per-tenant quotas, audit, assignment buckets and NATS signing keys | #92 to #99 |
| Tenant hardening | Names unique per tenant; a deleted tenant's bus data is deleted; one tenant cannot list another's keys, exhaust its consumers, stall managers with a duplicate WireGuard key, take its subnets or ports, or read its metrics; required-egress tasks get no recursive DNS | #119, #147, #149, #152, #158, #167 to #171, #174 to #177, #182 |
| Agents | WASM sandbox hardened with epoch timeouts, memory caps and a shared module cache; explicit capability grants; egress proxy with routes, budgets, redirects and private destinations refused, and usage events | #101 to #103, #105, #106, #108 |
| Operations | `minga-ctl` gains `manager`, `node`, `network`, `config`, `task`, `info`, `doctor` and `upgrade preflight`; `minga mcp serve` gives agents tenant-scoped reads; changes proposed by an agent need approval by a distinct deployer or admin identity | #112, #114 |
| Compose | `ports` host IPs reach desired state and publish on that IPv4 node address only (an IPv6 host IP is accepted and stored but not published) (#162); `deploy.labels` are rejected instead of silently dropped; container hardening fields the runtime does not implement are refused with 400 | #155, #159, #162 |
| Build and CI | The nats-server checksum is verified in CI, the release build no longer caches Rust artifacts, one shared NATS version pin | #156, #166, #172 |

## Upgrade notes

Upgrading a 1.2.0 cluster is **one coordinated window**, and **1.2.0 cannot
report which of its nodes are Raft voters**. Nothing you can read from a
running 1.2.0 cluster, the node list included, proves it. So the default is
to export the state and import it into a fresh cluster, and a single host may
convert in place only on the evidence described under [Order](#order). The
1.3.0 tools that report the voter set cannot read a 1.2.0 API:
`minga-ctl upgrade preflight --to v1.3.0` is read-only and reports the Raft
membership and quorum, backups, NATS, tenants and a rollback plan, and it
fails closed when it cannot check something. Its diagnostics endpoints are new
in 1.3.0, so run it against an upgraded manager, not a 1.2.0 one.

**State migrates in place, one way.** 1.2.0 state is format 2; 1.3.0 is format
12. Opening a store migrates state and Raft logs from v2 through v11, and a
binary of an older format refuses a store of a newer one. Nodes of different
formats refuse each other's Raft messages, so every node must change together.
Take the backup in [docs/backup.md](backup.md) first, including the Raft data
directory while stopped, because there is no documented way back once a store
has migrated.

### Before you start

- **Manager certificates.** The Raft server accepts a peer only if its
  certificate has a SAN `minga-manager-<id>`, and the client pins the exact
  SAN of the manager it dials. Reissue any manager certificate that lacks it
  with `minga tls issue-node --role manager`.
- **NATS route certificates.** Sidecar routes accept a peer only with the SAN
  `minga-nats-route`. Reissue every `--nats-cluster-cert` from the route CA
  with `minga tls issue-node --role manager`; a manager refuses to start with
  one that lacks it. The route CA must be separate from the client CA.
- **Refused at start.** A manager no longer starts on a non-loopback API bind
  (the default is `0.0.0.0:2375`) without `--api-token` or API client TLS, on
  an empty token, or on plaintext Raft off loopback. These used to log a
  warning.
- **Overlay supernet.** A network's subnet must lie inside `--overlay-supernet`,
  outside the node mesh `10.200.0.0/16`, and be no longer than `/28`. Widen
  the supernet on every node to cover any Compose `ipam` subnet you use, or
  recreate those networks.
- **WireGuard.** Multi-node managers need a reachable `--wg-endpoint`. No two
  nodes may share a WireGuard key. A relay decrypts and re-encrypts traffic
  and can read container payloads.
- **Ports.** Short-syntax Compose ports now publish on every node. Use
  `mode: host` to keep a port on the node that runs the task. Conflicting
  ingress ports return 409.
- **Data volume.** The NATS sidecar's JetStream file store cap rose from 1 GB
  to 8 GB, not preallocated. It shares the data volume with Raft and
  containerd, so size it for that.
- **Agents.** `--agent-dir-root` now grants only `<DIR>/<tenant-id>/` to the
  node's tenant, so directory grants directly under a root must move into the
  tenant's subdirectory. The worker egress proxy is opt-in: it needs
  `--egress-bind`.
- **Building from source.** The minimum Rust version is 1.97.

### Order

**First check the voter set.** In 1.2.0 every node is a Raft voter, and nodes
of different formats refuse each other's messages. The membership change that
removes the worker voters is a Raft write, so it needs a majority of the
1.2.0 voter set, and during step 1 the upgraded managers may be the only
voters that can answer. Upgrading the workers first does not help: an
upgraded worker no longer votes. This candidate has not shown that the
conversion commits when the managers are a minority of the voters, so treat
that shape as unsupported.

**1.2.0 cannot tell you its voter set.** Its API reports leader status and
node counts from the node registry, which is not the Raft membership: nodes
can be registered through the API without being voters, and Raft initializes
its voter set from each node's ID and `--peer` list when the store is created,
so current flags do not prove it for a store that already exists. In 1.2.0
every `minga manager`, `minga worker` and `minga api` process that opened a
store is a voter, and `minga api` is retired in 1.3.0, so it never counts
toward the managers. Counting `docker node ls` rows plus your `minga api`
processes gives an estimate that can be wrong in either direction.

**Fail closed.** For every cluster with more than one node, including one
where your count shows a manager majority, use
[the export and import path](#when-the-managers-are-not-a-majority). Nothing
in 1.2.0 shows which nodes are voters, and nothing here shows that converting
in place works for any shape but the one proof above.

**A single host** (one `minga manager`, no workers, no `minga api` process)
is the one case you may convert in place, and only on this evidence, not on
the flags it was started with: 1.2.0 uses `--peer` only when a store is first
created, so a store started once with peers may still list them as voters.
Take the cold copy, upgrade the manager, and require that it **becomes leader
on its own and accepts a Raft write** (deploy a test service). A node cannot
become leader without a majority of its persisted voters, so that outcome
shows the voter set is only that manager. If it does not become leader, stop,
and restore the copy as described below. `minga migrate-v1` without `--apply`
and `upgrade preflight` run only against an upgraded manager, as a second
check; they cannot replace this one.

**What the in-place conversion does.** The live proof ran it, and a single
host above may run it; it is the only in-place path this candidate offers:

1. Upgrade the managers first, then, on the leader, change membership once to
   the manager set, removing worker voters. `minga migrate-v1` previews that
   voter set before applying it.
2. Upgrade the workers. They ignore an old `raft_log.redb` or `state.redb` in
   `--data-dir`, log once that it can be deleted, and never open it. They join
   as NATS clients.
3. `minga api` exits with a message pointing to `minga manager --api-bind`.
   It is not kept as an alias.

A 1.2.0 cluster went through this in a live proof with a service and a running
container intact, and the upgraded worker adopted the same task. The proof
converted the voters `{1, 2, 99}` to `{1}` with one manager upgraded; the
record does not explain how that reached a majority, so do not rely on it for
a different shape. See [supported topologies](topologies.md) for what it did
and did not cover.

#### If the conversion stalls or a step fails

An older binary refuses a migrated store, and an upgraded node refuses 1.2.0
messages, so a half-upgraded cluster does not recover by restarting it with
the old binary. The way back is the copy you took first:

1. Stop every node.
2. On each node, restore the full data directory you copied before the upgrade
   (`raft_log.redb`, `state.redb` and the rest, [backup.md](backup.md) says to
   cold-copy it) over the migrated one.
3. Start the 1.2.0 binaries with the certificates and flags they had. Keep the
   old certificates until the upgrade has succeeded; issue the new ones beside
   them, not over them.

This follows from how the stores and the wire format behave and from the
backup guide. **It has not been rehearsed**, not in a live run and not in CI,
so rehearse it before the window, on a disposable copy of the cluster, and do
not start the window without the copy. A rehearsal should:

1. Restore each node's cold data directory with its original node ID, address,
   flags and certificates.
2. Start the 1.2.0 binaries and wait for a leader to be elected.
3. Check quorum, perform a Raft write (deploy a test service), and check that
   the state is what it was when the copy was taken.

#### When the managers are not a majority

Do not try to convert in place. Export the desired state from the running
1.2.0 cluster with `minga state export`, build a fresh 1.3.0 cluster, and
restore with `minga state import`, which accepts snapshots from format v2
upward. The export carries services, networks, volumes and secret and config
names, not secret values, volume data or TLS material: copy those as
[backup.md](backup.md) describes. Tests cover importing v2-format snapshots;
this path has not been run as a live upgrade.

#### Upgrading a v1.2.0 single-host layout (manager + api + `--peer`)

The v1.2.0 quick-start runs `minga manager` and `minga api` as separate
processes, both started with `--peer`. That adds `minga api` as a Raft voter
and leaves `--peer` flags in systemd units or start scripts. This section
gives numbered steps for that layout. It is a subset of the export and import
path, not the in-place path: treat the host as a cluster whose voter set is
unknown until step 3 proves otherwise.

Before you start: read [Before you start](#before-you-start) above. Certificate
and NATS reissuance applies here too.

**Steps:**

1. **Export desired state from the running 1.2.0 cluster.**
   ```bash
   minga state export -o /root/minga-state.json --api-url http://127.0.0.1:2375
   ```
   The file holds service specs including environment values; store it
   encrypted and keep its mode 0600. See [backup.md](backup.md) for what the
   export does and does not cover.

2. **Back up all state from the data directory while both processes are still running.**
   Follow [backup.md](backup.md): cold-copy the full data directory
   (`raft_log.redb`, `state.redb`, `nats/`, `volumes/`, `logs/`, TLS files)
   and the secret store (default `/var/lib/minga/secrets`). Stop `minga api`
   and `minga manager` only after the backup is complete.

3. **Stop the old processes and remove the `--peer` flags and the `minga api` unit.**
   Remove or disable the `minga api` systemd unit. Remove `--peer` from the
   `minga manager` unit; in 1.3.0 the API is served by `minga manager --api-bind`.

4. **Install the 1.3.0 binary.**

5. **Start the manager without `--peer`, with `--api-bind`.**
   ```bash
   minga manager \
     --node-id <YOUR_NODE_ID> \
     --bind 127.0.0.1:50051 \
     --api-bind 0.0.0.0:2375 \
     --api-token <YOUR_TOKEN> \
     --data-dir /var/lib/minga
   ```
   A single-host manager does not need `--wg-endpoint` and must not have
   `--peer` (that flag told 1.2.0 about other voters; 1.3.0 reads membership
   from the existing store, and passing `--peer` makes the manager expect a
   multi-node overlay).

6. **Confirm the manager becomes leader and accepts a Raft write.**
   Wait for `/readyz` to return 200, then deploy a test service:
   ```bash
   curl -s http://127.0.0.1:2375/readyz
   MINGA_CTL_ENDPOINT=http://127.0.0.1:2375 minga-ctl upgrade preflight --to v1.3.0
   ```
   `upgrade preflight` reports the Raft membership and quorum. If the
   manager does not become leader within about 30 seconds, stop it and
   restore the backup as described under
   [If the conversion stalls or a step fails](#if-the-conversion-stalls-or-a-step-fails).

7. **Import the desired state into the fresh 1.3.0 store.**
   ```bash
   minga state import /root/minga-state.json --api-url http://127.0.0.1:2375
   ```
   Services, networks, volumes and secret names are restored. Secret values
   must be re-provisioned in `--secret-store` (default `/var/lib/minga/secrets`)
   if the backup was not restored directly; see [backup.md](backup.md).

8. **Verify the result.**
   Check that your services are listed and tasks are placed:
   ```bash
   MINGA_CTL_ENDPOINT=http://127.0.0.1:2375 minga-ctl stack ls
   ```
   Task status and restart counts reset once after the upgrade; this is
   expected (see [What changes after](#what-changes-after)).


### What changes after

- **Task status and restart counts reset** once: tenant-first bus names replace
  the old untenanted keys, and desired state is rebuilt from Raft.
- **Tenant objects get qualified IDs.** A tenant other than `default` creates
  services, networks, secrets and configs with the ID `<tenant>.<name>`; the
  API accepts a name or an ID. Existing objects keep their bare IDs and stay
  owned by their tenant. A non-default tenant's node-local secret value is
  provisioned under the qualified ID, the `SecretID` the API shows.
- **Network removal** returns 409 while a service, a rollback revision or a
  running task still uses the network. `minga-ctl stack rm --with` waits up to
  15 seconds for the stack's tasks to be reaped.
- **Compose** rejects `deploy.labels` it used to drop. `ports` host IPs are published on that IPv4 address only; an IPv6 host IP is accepted and stored but not published (#162). State is format 13 (tenant generation, #203), so every node changes together.
- **Unreleased builds.** Entries in the changelog marked "upgrade action
  needed" also describe changes between unreleased builds of `main`: the
  per-tenant assignment buckets, the NATS operator/JWT mode and the worker ACL
  changes. They matter only to a cluster that ran such a build. If yours did,
  follow those entries, including the flag-day order for the assignment
  buckets and the deletion of the old shared `KV_MINGA_ASSIGN` stream.

### Later upgrades: apply rules

1.3.0 adds a replicated apply-rule level ([ADR 0009](adr/0009-raft-apply-rule-versions.md)).
It starts at 1, which is every apply-time refusal in this release, so **an
upgrade to 1.3.0 has no step for it**. The level exists for the first later
release that adds a refusal to the Raft apply, because a manager that replays
the log (a new or replaced manager) or runs a newer build would otherwise
answer a committed entry with an error that an older manager applied as `Ok`.
That release's notes will name the step. The order is:

1. Upgrade every manager. Until the level is raised, the new build applies
   every entry by the old rules, so managers of different builds that share a
   state format agree. Builds of a different state format still refuse each
   other, as above.
2. Check that every manager runs the new build.
3. Raise the level, once, from any machine with a cluster-operator admin
   identity (`O=*`, OU `admin`, or the admin token):

   ```bash
   minga-ctl info                                  # level in force, level each manager implements
   minga-ctl upgrade apply-rules --to N --yes      # omit --to for the highest level the manager you reach implements
   ```

   `info` and `upgrade preflight` show the level in force and the level each
   manager's build implements. `upgrade apply-rules` refuses unless every
   manager reports a build that implements `N`, and a manager that does not
   answer counts as behind. The manager checks again before it proposes the
   raise, so the refusal does not rest on the client. It goes through Raft as
   one `SetApplyRules` entry, and each raise is logged with the caller's name.
   The API route behind it is `GET` and `POST /minga/apply-rules`; a tenant
   identity gets `403` on both.

Do not raise it while a manager is still old: a manager that implements a lower
level stops applying at that entry instead of applying it by older rules. The
level never goes down, so a raise is not undone by a downgrade; do not roll a
manager back to a build from before 1.3.0 after one. A manager added after the
raise that runs an older build also stops at that entry when it replays the
log, so add managers only with a build that implements the level in force.

A unit test replays a log on a live and on a rebuilt replica and shows the
same results before and after a raise. Tests with two managers on different
maximum levels show the refusal, through the store, the API and `minga-ctl`.
No live rolling upgrade has exercised it, and no rule is gated yet.

## Known issues and limits in this candidate

Open against the 1.3.0 milestone when this was cut. Each needs a decision
before 1.3.0: fix, or document and defer.

| Issue | Summary |
| --- | --- |
| #128 | Secret references reveal other tenants' names. #182 makes a foreign and an absent name answer alike; confirm that closes it |
| #134 | `change approve` does not show the proposed values (a fix is open as #179) |
| #137 | The DNS TCP listener has no connection cap or timeout (a fix is open as #183) |
| #138 | An unbounded replica count runs placement inline in the API |
| #139 | `migrate-v1` sends the admin token through `HTTP_PROXY` |
| #142 | Compose hardening keys are parsed, then dropped |
| #150 | The NATS cluster test flakes on meta-group formation (CI only) |
| #153 | A cross-tenant service-versus-service ingress collision is blocked cluster-wide |

Limits that are documented rather than open issues:

- **Several API managers and tenant lifecycle.** Each tenant has a generation
  in the replicated record, and a marker on the bus names the generation that
  owns its resources (#203). A create claims the ID first, a delete takes it
  before the record goes, and every delete step checks that it still holds it,
  so a delete that pauses on one manager cannot remove what another manager
  made after it. What remains: a step is checked before each bus call, not
  during it, so one call already in flight can still land; the record removal
  itself is not checked against the generation; and a create that failed with
  an unknown outcome leaves its claim, which a delete may take over after two
  minutes. A manager's cached tenant setup is dropped when a status write finds
  the bucket missing, not when any other resource is.
- **A delete can leave names taken.** Object names are unique per tenant, but a
  legacy bare ID, or a `default` name containing a dot, can equal another
  tenant's qualified ID. The later creation is refused with 409.
- **Scale.** The overlay at T4 scale is untested, and `MINGA_LOGS` is a single
  replica.
- **WASM agents.** Capability grants are validated, but WASI host imports are
  not linked, so grants are not yet enforced in the sandbox.

## Verification

CI runs on every change: format, clippy and the workspace tests; the daemon-
backed tests against a real containerd; the real-kernel overlay tests; the
NATS suites against 2.14.10 and 2.15.3; and the nats-server 2.10.22 to 2.15.3
store upgrade. Live proofs on OrbStack Ubuntu VMs covered manager restart,
worker partition and rejoin, runtime worker join, manager failover and
membership changes, NAT worker relays, manager relay failover, cross-network
isolation and anti-spoofing, ingress, and a 1.2.0 to 1.3.0 migration.
Throughput samples are in the [benchmark notes](benchmark.md); they are short
virtualized runs, not capacity figures.

### Manager rejoin after NATS credential changes (#198)

The Phase 13 run log was not kept, so that exact failure was not replayed. A
test against a real `nats-server` in operator/JWT mode reproduced one cause,
and it is fixed. The sidecar copied the account JWT into the resolver
directory only when the file was missing, so a rotated account JWT (for
example a new tenant signer) was ignored on restart and users from the new
signer got an authorization violation. The sidecar now replaces the resolver
claim when the provisioned JWT is at least as new, and keeps a newer claim
that a revocation push wrote. A changed manager credentials file under the
same account was already accepted on restart. A multi-VM rerun on the
candidate build would still confirm that this was the Phase 13 cause.

### Not yet verified

These are what the candidate is for. 1.3.0 waits for them.

- **The tenant-isolation proof on real machines** (tenant B cannot place on
  tenant A's node), recorded against the exact candidate build.
- **A staging upgrade from 1.2.0** on the candidate, with the backup and
  rollback steps rehearsed.
- **The per-tenant assignment bucket upgrade** against real old-server data.
  Its upgrade job in CI passes, but only on the bundled test data.
- **Several managers and several tenants together**, including the tenant
  lifecycle cases above.

## Publishing the candidate

1. Merge the pull request that carries this page and the version bump.
2. Tag the merge commit: `git tag v1.3.0-rc.1 <commit> && git push origin
   v1.3.0-rc.1`. The workflow refuses a tag that does not match the workspace
   version, builds the archives and the image, and marks any tag containing a
   `-` as a pre-release, so `releases/latest` stays on 1.2.0.
3. Run the checks above against that build. Fix what they find on the release
   branch and cut `-rc.2` if needed.
4. For 1.3.0: bump the version to `1.3.0`, turn the changelog section into
   `[1.3.0]`, update the README, site data, `install.sh` and
   `scripts/setup-talos-minga.sh` as the 1.2.0 release did, and tag `v1.3.0`.
