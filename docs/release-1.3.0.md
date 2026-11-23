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
| Compose | `ports` host IPs and `deploy.labels` are rejected instead of silently dropped; container hardening fields the runtime does not implement are refused with 400 | #155, #159 |
| Build and CI | The nats-server checksum is verified in CI, the release build no longer caches Rust artifacts, one shared NATS version pin | #156, #166, #172 |

## Upgrade notes

Upgrading a 1.2.0 cluster is **one coordinated window**. Run
`minga-ctl upgrade preflight --to v1.3.0` first: it is read-only, and it
reports the Raft membership and quorum, backups, NATS, tenants and a rollback
plan. It fails closed when it cannot check something.

**State migrates in place, one way.** 1.2.0 state is format 2; 1.3.0 is format
11. Opening a store migrates state and Raft logs from v2 through v10, and a
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

1. Upgrade the managers first, then, on the leader, change membership once to
   the manager set, removing worker voters. `minga migrate-v1` previews that
   voter set before applying it.
2. Upgrade the workers. They ignore an old `raft_log.redb` or `state.redb` in
   `--data-dir`, log once that it can be deleted, and never open it. They join
   as NATS clients.
3. `minga api` exits with a message pointing to `minga manager --api-bind`.
   It is not kept as an alias.

A 1.2.0 cluster went through this in a live proof with a service and a running
container intact, and the upgraded worker adopted the same task. See
[supported topologies](topologies.md) for what that did and did not cover.

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
- **Compose** rejects `ports` host IPs and `deploy.labels` it used to drop.
- **Unreleased builds.** Entries in the changelog marked "upgrade action
  needed" also describe changes between unreleased builds of `main`: the
  per-tenant assignment buckets, the NATS operator/JWT mode and the worker ACL
  changes. They matter only to a cluster that ran such a build. If yours did,
  follow those entries, including the flag-day order for the assignment
  buckets and the deletion of the old shared `KV_MINGA_ASSIGN` stream.

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

- **Several API managers and tenant lifecycle.** Tenant create and delete are
  serialized within one process only. With more than one API manager, a
  delete that pauses can remove the streams of a tenant another manager has
  since recreated under the same ID, and cached tenant setup on another
  manager is not invalidated. Delete and recreate a tenant ID through one
  manager at a time.
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

### Not yet verified

These are what the candidate is for. 1.3.0 waits for them.

- **The tenant-isolation proof on real machines** (tenant B cannot place on
  tenant A's node), recorded against the exact candidate build.
- **A staging upgrade from 1.2.0** on the candidate, with the backup and
  rollback steps rehearsed.
- **Manager rejoin after NATS credential changes.** A manager failed to
  restart after test credentials were updated, with a NATS authorization
  violation.
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
