# Wave 10 requirements

The controls below are engineering targets, **not** an assertion of certification, government approval or accreditation.

- [x] **W10-TLS** NATS clients and server routes authenticate peers with trusted certificates; invalid/untrusted/expired identity and plaintext are rejected. Local containerd Unix-socket access is permission-gated; remote TLS only if a verified remote endpoint is in scope.
- [x] **W10-SEC** Supported Compose service secret references reach containers as read-only `/run/secrets/<name>` files; control-plane values do not enter Raft, JetStream control messages, API output or diagnostics. Compose `file` is explicitly rejected until secure multi-node import/distribution exists; Docker API inline `Data` must not be silently discarded. Missing/invalid/unprovisioned node secret fails closed; cross-node provisioning, rotation and expiry are documented and tested. Arbitrary workload stdout is potentially sensitive and governed by W10-LOG access controls.
- [x] **W10-LOG** Real Linux container stdout/stderr reach authorized `minga logs` (history/follow with gap-free handoff), preserve stream identity, and have bounded disk/retention and crash-durable replay/ack recovery with documented dedup bounds. FIFO preference is subject to proof with containerd; indexed local history is Minga-owned unless a supported API is demonstrated. Deny cross-identity log reads even when a workload prints sensitive bytes.
- [ ] **W10-STAGE** A **PASS** of the Talos extension-service feasibility gate (not a closed BLOCKED report) precedes Talos-dependent implementation and sign-off. Reproducible authorized Talos/hcloud single-node or multi-node staging evidence demonstrates supported boot/provisioning, runtime and storage/socket access, TLS, secrets, logs and reboot. Configure a private `--api-bind` plus firewall, verify external-network denial; no `talosctl upload/run`, systemd, or hcloud ISO-as-server-image claims without verified commands. A Linux-only test environment does not meet this requirement.
- [ ] **W10-COMP** Map FIPS, SOC 2, VS-NfD and NATO objectives individually to implementation evidence, operator controls, unknowns and external assessment/approval. No certification claim. Complete unmasked `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, and `cargo test`; update deployment and README documentation.

W10-STAGE and W10-COMP stay open: the 10-01 host gate is blocked by user choice (Talos staging is out of scope for `v0.1.0` and no hcloud provisioning is authorized), so the compliance objectives are **not assessed**, which is different from not applicable.

# Phase 12 requirements (Managers-only Raft)

- [x] **MOR-ADR** ADR 0006 records that workers leave Raft (learners rejected), with assignment, status, lease, authorization, join, migration and API-fold decisions, before code moves.
- [x] **MOR-LEASE** Node liveness and registration go through `MINGA_LEASES`, which expire on the NATS server. Placement compares no node clock, and an idle cluster makes no Raft writes.
- [x] **MOR-STATUS** Actual task state (lifecycle, restarts, last error, exit code) is written to `MINGA_STATUS` only. The scheduler and the Docker API read it from there.
- [x] **MOR-ASSIGN** Workers open no `StateStore`. They run what the leader publishes in `MINGA_ASSIGN`, the only dispatch path, which the leader re-derives from Raft on election.
- [x] **MOR-AUTHZ** NATS subject permissions bound to client certificates: only managers write assignments, and a worker reads only its own assignments and writes only its own lease, status and logs. No bus identity can do anything it couldn't do in v1.2.0.
- [x] **MOR-JOIN** Workers join a running cluster with NATS credentials alone. Managers are added and removed at runtime through admin-token API calls that refuse to lose quorum. v1.2.0 clusters migrate by shrinking membership to the managers.
- [x] **MOR-FOLD** `minga manager --api-bind` serves the Docker API, so a single host runs one voter. `minga api` fails loudly and points at the flag.
- [x] **MOR-NATS** With 3 or 5 managers, their sidecars form one NATS cluster over route TLS, and workers connect as clients. Losing one manager keeps the bus. External NATS stays supported.
- [x] **MOR-PART** A worker cut off from the bus keeps running its tasks. The leader reschedules them after the lease expires, and the worker stops the moved copies when the link heals. The docs state the at-least-once window.
- [x] **MOR-PROOF** A live 3-manager plus 2-worker cluster with mTLS demonstrates each item above. Docs, CHANGELOG and the site roadmap are updated.

# Phase 13 requirements (WireGuard mesh and routing mesh)

- [x] **WG-ADR** ADR 0007 records the routed /32 design, the NAT relay with its trust statement, the routing mesh, and the rejected options (VXLAN over WireGuard, a full mesh, IPVS, a userspace data path), before code moves.
- [x] **WG-SUBNET** No two networks can share a subnet. The leader allocates subnets and stores them in Raft, and overlaps fail loudly. Network `driver_opts` accepts `encrypted` and rejects anything else.
- [x] **WG-KEYS** One kernel WireGuard interface and keypair per node. The private key stays in the data directory (0600) and never appears in Raft, on the bus, in logs or on a command line. Single-host clusters are unchanged.
- [x] **WG-PEERS** Each node gets its peer set from the leader through `MINGA_ASSIGN`: peers only for shared networks plus the managers, applied as a diff.
- [x] **WG-NAT** Workers behind NAT with no inbound port reach every other node: directly when one side is reachable, otherwise relayed through a home manager that fails over when its lease expires.
- [x] **WG-ISOL** Network isolation and anti-spoofing hold across nodes. A task can't reach another network on another node, or send as another node's task.
- [x] **WG-INGRESS** `mode: ingress` (the default) answers on every node and load-balances to tasks anywhere. `mode: host` keeps per-node publishing and the client IP. Port conflicts fail cluster-wide with a 409.
- [ ] **WG-PROOF** Separate VMs with one worker behind NAT demonstrate each item above, with iperf3 numbers for direct and relayed paths. Docs, CHANGELOG and the site roadmap are updated.

# Phase 14 requirements (Supported NATS server line, tenancy T0)

- [x] **NATS-PIN** Releases bundle nats-server 2.15.3 with a checksum taken from the release, and CI installs it. No 2.10 pin remains in workflows, test images or docs.
- [x] **NATS-LEVEL** Managers read the server's JetStream `api.level` at startup, keep it for feature gates, and warn (naming version, level and floor) below level 4 (2.14). Workers never get `$JS.API.INFO`.
- [x] **NATS-CI** The `minga-bus` suites, including a three-server cluster test of `ensure_replicas`, run on 2.14.10 and 2.15.3 and fail instead of skipping when no server is available.
- [x] **NATS-UPGRADE** A 2.10.22 store dir with Minga's streams and buckets opens on 2.15.3 with data intact, a three-server cluster survives a rolling replacement, and the downgrade result is recorded.
- [x] **NATS-CAP** The assignment, status and lease buckets accept more than 1,000 consumers on 2.15 (the new default cap), so a cluster with more than 500 workers can still watch its assignments.
- [x] **NATS-DOCS** CHANGELOG, CONTRIBUTING, ADR 0002, ADR 0008 and `docs/topologies.md` state the supported lines and the floor and the upgrade result.

# Phase 15 requirements (Tenant isolation, ADR 0008 T1–T5)

- [x] **TEN-STATE** A `Tenant` is desired state with an owner for every stack, service, task, secret, config, network, volume, egress route and job. Names are unique within a tenant, not across the cluster. An existing cluster receives one replicated `default` tenant, and a format-version bump migrates older state and Raft logs.
- [x] **TEN-IDENTITY** The client certificate carries the caller (`CN`), the role (`OU`) and the tenant (`O`). Roles apply within the tenant. A certificate without `O` maps to `default` only while `default` is the sole tenant.
- [x] **TEN-SCOPE** Workload routes and their nested responses are filtered by tenant. A reference across tenants returns 404, never 403, and listings filter before paging. `O=*` operators hold no tenant workload access.
- [x] **TEN-OPERATOR** Tenant CRUD, node inventory, cluster info and cluster health or metrics routes require a cluster operator (`O=*` or the configured API token). A manager can require API mTLS with a dedicated client CA.
- [x] **TEN-NODE** A node belongs to exactly one tenant, fixed when it joins with a tenant-minted join token. The scheduler refuses to place a task on another tenant's node, managers run no tenant workloads once a cluster has two tenants, and the WireGuard peer set spans one tenant only. Implementation merged in PR #98; end-to-end tenant-isolation proof remains open in `.planning/ROADMAP.md`.
- [x] **TEN-BUS** Every subject and key carries the tenant right after the prefix. Workers write only to per-tenant streams and buckets with the tenant's limits, and a node's bus identity reaches only its own tenant's subjects. Operator/JWT mode mints a node's user JWT with the tenant's scoped signing key.
- [x] **TEN-QUOTA** Object counts, desired-state size, API request rate and JetStream bytes are bounded per tenant, and a tenant over quota gets 429 or 507 naming the quota. The audit trail carries the tenant, and a tenant admin reads only their own trail.
