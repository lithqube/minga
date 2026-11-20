# Wave 10 requirements

The controls below are engineering targets, **not** an assertion of certification, government approval or accreditation.

- [ ] **W10-TLS** NATS clients and server routes authenticate peers with trusted certificates; invalid/untrusted/expired identity and plaintext are rejected. Local containerd Unix-socket access is permission-gated; remote TLS only if a verified remote endpoint is in scope.
- [ ] **W10-SEC** Supported Compose service secret references reach containers as read-only `/run/secrets/<name>` files; control-plane values do not enter Raft, JetStream control messages, API output or diagnostics. Compose `file` is explicitly rejected until secure multi-node import/distribution exists; Docker API inline `Data` must not be silently discarded. Missing/invalid/unprovisioned node secret fails closed; cross-node provisioning, rotation and expiry are documented and tested. Arbitrary workload stdout is potentially sensitive and governed by W10-LOG access controls.
- [ ] **W10-LOG** Real Linux container stdout/stderr reach authorized `minga logs` (history/follow with gap-free handoff), preserve stream identity, and have bounded disk/retention and crash-durable replay/ack recovery with documented dedup bounds. FIFO preference is subject to proof with containerd; indexed local history is Minga-owned unless a supported API is demonstrated. Deny cross-identity log reads even when a workload prints sensitive bytes.
- [ ] **W10-STAGE** A **PASS** of the Talos extension-service feasibility gate (not a closed BLOCKED report) precedes Talos-dependent implementation and sign-off. Reproducible authorized Talos/hcloud single-node or multi-node staging evidence demonstrates supported boot/provisioning, runtime and storage/socket access, TLS, secrets, logs and reboot. Configure a private `--api-bind` plus firewall, verify external-network denial; no `talosctl upload/run`, systemd, or hcloud ISO-as-server-image claims without verified commands. A Linux-only test environment does not meet this requirement.
- [ ] **W10-COMP** Map FIPS, SOC 2, VS-NfD and NATO objectives individually to implementation evidence, operator controls, unknowns and external assessment/approval. No certification claim. Complete unmasked `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, and `cargo test`; update deployment and README documentation.

# Phase 12 requirements (Managers-only Raft)

- [ ] **MOR-ADR** ADR 0006 records that workers leave Raft (learners rejected), with assignment, status, lease, authorization, join, migration and API-fold decisions, before code moves.
- [ ] **MOR-LEASE** Node liveness and registration go through `MINGA_LEASES`, which expire on the NATS server. Placement compares no node clock, and an idle cluster makes no Raft writes.
- [ ] **MOR-STATUS** Actual task state (lifecycle, restarts, last error, exit code) is written to `MINGA_STATUS` only. The scheduler and the Docker API read it from there.
- [ ] **MOR-ASSIGN** Workers open no `StateStore`. They run what the leader publishes in `MINGA_ASSIGN`, the only dispatch path, which the leader re-derives from Raft on election.
- [ ] **MOR-AUTHZ** NATS subject permissions bound to client certificates: only managers write assignments, and a worker reads only its own assignments and writes only its own lease, status and logs. No bus identity can do anything it couldn't do in v1.2.0.
- [ ] **MOR-JOIN** Workers join a running cluster with NATS credentials alone. Managers are added and removed at runtime through admin-token API calls that refuse to lose quorum. v1.2.0 clusters migrate by shrinking membership to the managers.
- [ ] **MOR-FOLD** `minga manager --api-bind` serves the Docker API, so a single host runs one voter. `minga api` fails loudly and points at the flag.
- [ ] **MOR-NATS** With 3 or 5 managers, their sidecars form one NATS cluster over route TLS, and workers connect as clients. Losing one manager keeps the bus. External NATS stays supported.
- [ ] **MOR-PART** A worker cut off from the bus keeps running its tasks. The leader reschedules them after the lease expires, and the worker stops the moved copies when the link heals. The docs state the at-least-once window.
- [ ] **MOR-PROOF** A live 3-manager plus 2-worker cluster with mTLS demonstrates each item above. Docs, CHANGELOG and the site roadmap are updated.

# Phase 13 requirements (WireGuard mesh and routing mesh)

- [ ] **WG-ADR** ADR 0007 records the routed /32 design, the NAT relay with its trust statement, the routing mesh, and the rejected options (VXLAN over WireGuard, a full mesh, IPVS, a userspace data path), before code moves.
- [ ] **WG-SUBNET** No two networks can share a subnet. The leader allocates subnets and stores them in Raft, and overlaps fail loudly. Network `driver_opts` accepts `encrypted` and rejects anything else.
- [ ] **WG-KEYS** One kernel WireGuard interface and keypair per node. The private key stays in the data directory (0600) and never appears in Raft, on the bus, in logs or on a command line. Single-host clusters are unchanged.
- [ ] **WG-PEERS** Each node gets its peer set from the leader through `MINGA_ASSIGN`: peers only for shared networks plus the managers, applied as a diff.
- [ ] **WG-NAT** Workers behind NAT with no inbound port reach every other node: directly when one side is reachable, otherwise relayed through a home manager that fails over when its lease expires.
- [ ] **WG-ISOL** Network isolation and anti-spoofing hold across nodes. A task can't reach another network on another node, or send as another node's task.
- [ ] **WG-INGRESS** `mode: ingress` (the default) answers on every node and load-balances to tasks anywhere. `mode: host` keeps per-node publishing and the client IP. Port conflicts fail cluster-wide with a 409.
- [ ] **WG-PROOF** Separate VMs with one worker behind NAT demonstrate each item above, with iperf3 numbers for direct and relayed paths. Docs, CHANGELOG and the site roadmap are updated.

# Phase 14 requirements (Agent-ready operations and painless management)

- [ ] **OPS-VERSION** The API/CLI report the running binary version and available build revision separately from client and API versions; operators do not infer installed versions from behavior.
- [ ] **OPS-RAFT** Diagnostics report persisted voter membership, leader, quorum/readiness and unavailable members, distinct from bootstrap `--peer` inputs. Configured peers are never represented as proof of running processes.
- [ ] **OPS-COMPOSE** Compose keys are applied end to end or rejected before desired state changes. `deploy.labels` has an end-to-end regression proving the Swarm consumer sees the labels or receives an actionable validation error.
- [ ] **OPS-DOCTOR** `minga-ctl doctor` is read-only and reports evidence-based, machine-readable findings for version, API, Raft, bus, nodes, tasks and auth/TLS. Shared support output redacts tokens, keys, secret values and sensitive environment data.
- [ ] **OPS-UPGRADE** Upgrade preflight reports actual source/target compatibility, persisted format/migration, backup, ordering, downtime and rollback limits. Planning never wipes data, restarts roles or silently begins an upgrade.
- [ ] **AGENT-CONTRACT** Agent tools use a documented typed interface and deterministic structured output/errors. Initial tools are allow-listed reads; agents do not receive shared admin credentials or arbitrary HTTP, shell or filesystem access.
- [ ] **AGENT-AUTHZ** Agent mutations are impossible without server-enforced caller identity and role checks; client-side tool filtering is not treated as authorization. No multi-tenant isolation claim is made until ADR 0008 is implemented.
- [ ] **AGENT-APPROVAL** Agents can only submit bounded, expiring change proposals. A distinct authorized operator approves or rejects the exact diff; every transition and result is attributable in the audit trail.
- [ ] **RC-PROOF** No VM tests are run during implementation. At the v1.3.0 release-candidate gate, Phase 13 multi-VM T2/T3 proof and a staging upgrade from v1.2.0 are recorded against the exact candidate; release claims match that evidence.
