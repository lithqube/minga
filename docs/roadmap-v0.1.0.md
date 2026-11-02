# Minga — Roadmap to a First Production Release

> **Historical.** This roadmap was written on 2026-10-27 to plan `v0.1.0`. All seven gates closed on 2026-11-01 (see the [changelog](../CHANGELOG.md)). The blockers and measurements below describe the tree on 2026-10-27, not today. For the current state and what comes next, see [architecture.md](architecture.md).

**Target:** `v0.1.0`, a single-binary release an operator can install on real hardware and run a Compose stack on, with secrets, logs and TLS handled honestly.

**Status of this document:** grounded in a source audit plus one new executable probe (`crates/minga-cli/tests/deploy_path_probe.rs`) run against the current tree on 2026-10-27. Every "absent" below is a verified absence in code, not an inference from documentation. Claims about Talos, hcloud, FIPS, SOC 2, VS-NfD or NATO are deliberately excluded — none are achieved, and no gate below depends on them.

---

## Where the project actually is

Minga is a **working orchestrator core with a broken production edge**. The container runtime is real and proven against a live containerd daemon; the control plane that decides *what* to run has gaps that make a real deployment fail silently.

Measured state of the tree:

| Metric | Value |
|---|---|
| Commits | 25 (all by one author) |
| Rust LOC | 31,213 across 78 files, 11 crates |
| Tests | 563 passing (`cargo test --workspace`) |
| CI workflows | **none** — no `.github/workflows/` |
| Version | `0.0.1` in `Cargo.toml` |
| Release artifacts / changelog | **none** |

What is genuinely done and verified: Compose parsing with loud failure on unknown keys; Raft consensus over openraft+redb; the scheduler; the supervisor; the Docker Engine API v1.41 surface; the containerd runtime (real daemon, repeatable, no residue); NATS/JetStream bus with TLS; the WASM sandbox; the LLM egress proxy; Prometheus metrics and health endpoints.

What is not: the last mile between "a service exists in Raft" and "a container runs on a node and is reachable".

---

## The four blockers, in dependency order

These are ordered because each one is invisible until the previous one works. Fixing them out of order produces tests that pass and a product that still cannot run.

### B1 — A cluster has no nodes, so nothing can be scheduled

**Verified.** `StateStore::upsert_node` is called **only from test files**. Every non-test call site in the tree is empty:

```
crates/minga-scheduler/src/reconcile.rs:101   ClusterNode { … }   ← inside #[cfg(test)]
crates/minga-scheduler/src/placement.rs:185    ClusterNode { … }   ← inside #[cfg(test)]
crates/minga-scheduler/src/constraints.rs:113   ClusterNode { … }   ← inside #[cfg(test)]
crates/minga-api/src/translate.rs:909,933      ClusterNode { … }   ← inside #[cfg(test)] (cfg(test) at :673)
```

No production code constructs a `ClusterNode`. `minga manager` / `minga worker` open the store, start the supervisor and scheduler, and never register themselves. The API has no `POST /nodes` and no node-create route (`crates/minga-api/src/server.rs:55-56` — GET only).

**Consequence, proven by execution.** The probe posts a service to the Docker API, then reconciles:

```
PROBE api_create_status=201
PROBE services_in_state=1
PROBE nodes_in_state=0
PROBE reconcile_actions=0        ← nothing placed
PROBE tasks_in_state=0
PROBE supervisor_running=0       ← no container
PROBE --- registering node 1 and retrying ---
PROBE nodes_after_register=1
PROBE reconcile_actions_after_node=2
PROBE supervisor_running_after_node=2   ← now it works
```

`place_service` filters on `n.status == NodeStatus::Active` over `list_nodes()` (`crates/minga-scheduler/src/placement.rs:37`). With an empty node list the filter yields nothing, `place_service` returns empty, and `plan()` produces no actions — **with a warning, not an error** (`crates/minga-scheduler/src/placement.rs:52-58`). The operator sees a successful deploy and an empty cluster.

This also explains why the Phase 0 gate test passes: `crates/minga-cli/tests/phase0.rs:163-165` registers the three nodes by hand.

**Fix:** node self-registration on startup — the node announces itself to the leader with its id, bind address, capacity and labels, and renews that record on a lease. Also add the missing `POST /nodes` / `POST /nodes/{id}/update` API surface so `docker node ls` and `docker node update` work, and fail the deploy loudly when a service matches zero eligible nodes instead of warning.

### B2 — `minga deploy` does not deploy

**Verified.** `crates/minga-cli/src/main.rs:288-300`:

```rust
Command::Deploy { compose, stack } => {
    match minga_scheduler::translate_project(&loaded.project, &stack) {
        Ok(translated) => print_translated(&translated),
        …
    }
    tracing::info!(%stack, "cluster deployment: not implemented yet (translation only)");
}
```

It parses, translates and prints. Nothing reaches Raft. The Compose file — the product's central promise — has no working deploy path.

The API path *does* work: `create_service` translates and calls `app.store.upsert_service` (`crates/minga-api/src/handlers.rs:196-214`), and the probe confirms a service created via `POST /v1.41/services/create` lands in Raft. So `docker stack deploy` against `minga api` is the working route and `minga deploy` is the broken one.

**Fix:** `minga deploy` must write the translated services, networks, volumes, secrets and configs to Raft (through the leader), then wait for placement and report per-service outcome — including "placed on node X" or "no eligible node". Then the Compose path and the API path converge on one code path.

### B3 — Containers have no network and no DNS

**Verified.** `minga-net` (1,774 LOC) is a **configuration generator with no kernel effect**:

- `crates/minga-net/src/wireguard.rs:8` — "Generates `wg-quick` compatible INI config files". No `wg-quick`, `wg set`, `ip link`, `nft` or `iptables` invocation exists anywhere in the crate or the workspace.
- The `OverlayManager` is referenced exactly once outside its own module: `crates/minga-cli/src/main.rs:741`, in the `minga net` CLI subcommand. The node supervisor never calls it.
- `Task.ips` is `HashMap::new()` at every construction site (`crates/minga-scheduler/src/placement.rs:122,143,338,371` and four in the supervisor). Task IPs are never assigned.
- The OCI spec declares a `network` namespace with `path: None` (`crates/minga-node/src/containerd/oci.rs:296`) — the container gets its own empty netns with no veth, no bridge and no IP.
- Published ports exist only as an OCI annotation; the spec comments this explicitly: `crates/minga-node/src/containerd/oci.rs:306` — "port mapping is handled by the network layer, not the runtime spec", and there is no network layer.
- DNS: `DnsStore` is used only inside `crates/minga-net/src/overlay.rs`. Nothing serves DNS to a container, and there is no `/etc/hosts` or resolver injection.

**Consequence:** a container starts, has no network interface, cannot resolve a peer service, and no published port is reachable. For a stack like the Mettal production compose (retail-ui → api → postgres), nothing communicates.

**Fix:** a real CNI plugin. The pragmatic choice for a single binary is to implement the CNI config generation and invoke `CNI_COMMAND=ADD` against a vendored bridge plugin, rather than depend on an external CNI binary being present. Wire it to: per-task IPAM from `Task.ips`, a bridge per overlay network, and port publishing via the bridge plugin's `portmap` capability. DNS then becomes a resolver that answers `service.minga.local` from the same state the API serves.

### B4 — Compose fidelity: what parses is not what runs

Verified by grep — each of these is parsed into the Compose model and then dropped before reaching state or the container:

| Compose key | Parsed at | Dropped because |
|---|---|---|
| `command` | `crates/minga-compose/src/model.rs:39` | `state::Service` has no field for it (`crates/minga-state/src/types.rs:98-120`) |
| `entrypoint` | `crates/minga-compose/src/model.rs:40` | same |
| `healthcheck` | `crates/minga-compose/src/model.rs:49` | never referenced in scheduler, node or state |
| `labels` (service + `deploy.labels`) | `crates/minga-compose/src/model.rs:42,557` | `state::Service` has no labels field; only `ClusterNode` does |
| `deploy.update_config` / `rollback_config` | `crates/minga-compose/src/model.rs:561-562` | parsed, never enforced — no staged rollout, no automatic rollback |
| volumes | `crates/minga-compose/src/model.rs:43` | translated to cluster `Volume` records, but `TaskSpec` carries no mounts, so nothing is mounted |
| `deploy.placement.constraints` | `crates/minga-compose/src/model.rs:562` | reaches `Service.constraints` but the API path hardcodes `Vec::new()` (`crates/minga-api/src/translate.rs:121`) |

`ContainerSpec.Command` and `ContainerSpec.Args` **are** accepted by the Docker API model (`crates/minga-api/src/models.rs:445,447`) and are **discarded** by `service_spec_to_state` — the state `Service` has no field for them. The probe confirms the loss: the spec sent `Command: ["nginx"]`, `Args: ["-g","daemon off;"]`, and the resulting task spec carried neither.

`Service.labels` being absent from state is also what breaks Traefik: the API emits only the stack namespace label (`crates/minga-api/src/translate.rs:218-222`), and the Task response has no `NetworksAttachments` field at all — Traefik's swarm provider cannot discover a task's IP or labels from this API.

**Fix:** extend `state::Service` and `state::TaskSpec` with command, entrypoint, labels, healthcheck and mount references; carry them through the API translate, the scheduler and the OCI builder; make the OCI builder apply image defaults overridden by explicit task values; add `NetworksAttachments` with real IPs to the Task response.

---

## Security work, in the order it should be done

The existing NATS TLS work is the one piece of Wave 10 that is genuinely production-shaped: no insecure escape hatch, CA and client cert required, SAN verification mandatory, missing or malformed material is a hard error (`crates/minga-bus/src/tls.rs:10-19`). It is the model for the rest.

Verified gaps, with what stands between here and production:

| Channel | Transport today | Encrypted | Authenticated |
|---|---|---|---|
| Raft consensus | plain TCP, 4-byte length-prefixed bincode (`crates/minga-state/src/network.rs:110-120,224-230`) | **no** | **no** |
| Worker → manager (bus) | NATS/JetStream | yes, with mTLS (`crates/minga-bus/src/tls.rs`) | client cert |
| Docker API | axum HTTP, default bind `0.0.0.0:2375` (`crates/minga-cli/src/main.rs:214`) | **no** | **no** |
| containerd | Unix socket only | n/a | socket permissions |
| Raft length prefix | `read_u32` → `vec![0u8; len]` — **unbounded allocation from an unauthenticated peer** | — | — |

Additional facts: the workspace has **no TLS dependency at all** — `tonic` is declared with only the `transport` feature (`Cargo.toml:44`), and there is no rustls/native-tls anywhere in the tree. The Docker API has zero authentication — grepping `crates/minga-api/src/server.rs` for `auth|bearer|api_key|token|tls|middleware` returns nothing, and there is no auth middleware on the router. NATS supports no token/user/password credential — grepping the bus crate returns only a doc comment. There is **no PKI anywhere**: no certificate issuance, rotation, expiry or revocation logic in the repo.

**Sequenced work:**

1. **Bounded Raft frame.** Cap the length prefix before allocating. This is a remote unauthenticated memory-amplification vector on the cluster's most exposed port, and it is a two-line fix that must land before anything else.
2. **Raft mTLS.** Add `tonic`'s `tls` feature plus rustls. The transport is hand-written (`crates/minga-state/src/network.rs`), so this is a TLS-wrapping layer over the existing `TcpStream` framing — not a rewrite.
3. **Docker API authentication.** A bearer token or mTLS on the API. Without this, an unauthenticated `POST /v1.41/services/create` on `0.0.0.0:2375` is remote code execution by design.
4. **NATS credentials.** Token or user/password alongside the existing mTLS, so a compromised cert is not sufficient.
5. **PKI lifecycle.** Issue, distribute, rotate, expire. Nothing exists today. This is the piece that makes 1–4 operable rather than merely possible.

**Secrets (C).** The current implementation is directionally right and structurally incomplete. `Compose` `external` references propagate into task specs; inline `file:`/`environment:` and Docker API `Data` are rejected rather than silently dropped. But: the mount path can be swapped after validation; there is no per-task staging; no placement inventory; no proof the mount is visible in the container's namespace; no multi-node provisioning; no rotation procedure. The OCI builder does correctly mount resolved secrets read-only with `ro,nosuid,nodev,noexec` (`crates/minga-node/src/containerd/oci.rs:197-212`).

**Fix:** stage each secret to a per-task, per-node, content-addressed path under a directory only the supervisor can write; bind-mount that exact inode read-only; record which node holds which secret version so rotation is a placement operation, not a guess.

**Logs (A).** The gap is exactly one function: `ContainerdRuntime::logs` returns an empty `Vec` with a TODO (`crates/minga-node/src/containerd/runtime.rs:561-566`). Everything around it exists — `Runtime::logs` is in the trait, the supervisor streams chunks to the bus with per-task offsets (`crates/minga-node/src/supervisor.rs:536-560`), the bus stores history, `minga logs` prints it. The CLI probe proved containerd's FIFO separation works at the Linux level (distinct `fifo-stdout`/`fifo-stderr` nodes, distinct redirected output). What is missing: passing real FIFO destinations to `Tasks/Create`, the reader lifecycle, durable acknowledged replay, per-task authorization on history, and gap-free follow.

---

## The release plan

Seven gates. Each has an exit criterion that is executable, not a document.

### Gate 1 — A cluster that knows its own nodes
- Node self-registration on startup with id, address, capacity, labels; lease renewal; deregistration on shutdown.
- `POST /nodes` and `POST /nodes/{id}/update` in the Docker API.
- Placement failure is a hard error, not a warning.
- **Exit:** the probe test, unmodified except for adding node registration as production code, reaches `supervisor_running=2` without any test scaffolding. Extend it to a real `docker stack deploy` against a live 3-node cluster.

### Gate 2 — `minga deploy` deploys
- `minga deploy` writes to Raft through the leader; reports placement outcome per service.
- Both the Compose path and the API path use one shared write path.
- **Exit:** `minga deploy -c compose.yaml stack` on a live cluster results in running containers, verified via `docker service ls` against `minga api`.

### Gate 3 — Containers talk to each other
- CNI: bridge per overlay network, veth per task, IPAM from `Task.ips`, port publishing via portmap.
- DNS: `service.minga.local` resolves from cluster state.
- **Exit:** on OrbStack Ubuntu, a two-service compose proves container→container by service name and a published host port reachable from outside the container.

### Gate 4 — Compose fidelity
- `command`, `entrypoint`, `labels`, `healthcheck`, volumes and placement constraints reach the container.
- Unsupported semantics fail loudly rather than being dropped.
- `NetworksAttachments` with real IPs in the Task response.
- **Exit:** a Traefik v3 `--providers.swarm` instance discovers a Minga service and routes to it, using only real Docker API responses.

### Gate 5 — Transport security and secrets
- Bounded Raft frame, Raft mTLS, Docker API auth, NATS credentials, PKI lifecycle.
- Secret staging with content-addressed per-task paths, placement inventory, rotation.
- **Exit:** a test asserts an unauthenticated API request is rejected, an unauthenticated Raft peer cannot join, and a rotated secret is visible to a new task and not an old one.

### Gate 6 — Logs
- Real FIFO destinations on `Tasks/Create`; reader lifecycle; bounded durable capture; acknowledged replay; per-task authorization; gap-free follow.
- **Exit:** `minga logs -f <task>` shows live stdout and stderr, separated, for a real container, and survives a reader restart without gaps.

### Gate 7 — Shippable
- CI: `fmt`, `clippy -D warnings`, `cargo test --workspace` on every push, plus the Linux/containerd job with `MINGA_REQUIRE_CONTAINERD=1` so the daemon-backed tests can never silently skip.
- Release artifacts for linux/amd64 and linux/arm64; version bumped off `0.0.1`; a changelog; an install path that does not require a Rust toolchain.
- Documentation corrected to match reality — today `docs/talos-deployment.md` tells the operator to run `minga stack deploy` and `minga ctr version`, **neither of which exists**; the CLI has `minga deploy` and no `minga ctr`.
- **Exit:** a clean checkout builds, tests and produces a binary an operator can install with one command.

**Talos and hcloud are not on this path.** They were blocked by your decision, and nothing above requires them. Gate 3 and Gate 6 can be proven on OrbStack Ubuntu. Talos becomes a *host* validation exercise after Gate 7, and hcloud staging needs separate authorization. The Talos guide should be corrected to reflect that its commands do not exist yet rather than shipping instructions that fail on first use.

---

## Explicitly out of scope for `v0.1.0`

State these in the release notes rather than discovering them in production:

- **No image pull.** The operator must preload and unpack the image in namespace `minga`. There is no Pull RPC in containerd's Images service, so a pull path must be built (content transfer + unpack) — real work, correctly deferred, but it must be documented as a manual step.
- **No automatic rollback.** `rollback_config` parses but is not enforced. `v0.1.0` gets manual rollback only.
- **No Firecracker.** The runtime is a stub (Linux + KVM).
- **No compliance certification.** FIPS, SOC 2, VS-NfD and NATO are unassessed. Any control-gap document must say "not assessed", not "not applicable" — the difference matters to an auditor.

---

## Effort and sequencing

Gates 1 and 2 are small and unblock everything: roughly a day each. Gate 3 (CNI) is the largest single item — several days, because it is real systems programming with kernel-visible failure modes. Gate 4 is a broad but mechanical change across four crates. Gate 5 is the most intellectually open: the transport work is bounded, but PKI lifecycle design is a real decision, not a checkbox. Gate 6 is one function done properly plus the surrounding durability work. Gate 7 is straightforward but easy to under-invest in.

The ordering matters more than the estimate. **Do not start Gate 3 before Gate 1** — a CNI implementation with no tasks to attach it to cannot be tested, and the failure will look like a CNI bug.

---

## Verifying this roadmap stays honest

Two habits, both already in use:

1. **Fail closed on the daemon.** `MINGA_REQUIRE_CONTAINERD=1` makes a missing containerd daemon a failure rather than a silent skip. The same treatment belongs on every "is it real?" gate: no skip, no warning, no empty result that reads as success.
2. **Prove with execution, not inspection.** The blocker that matters most here — B1, no nodes in the cluster — was found by reading code and confirmed by running a probe. It would have been easy to report "the scheduler is implemented" and ship a product that schedules nothing. The probe test stays in the tree as a regression test.

The standing rule from the project conventions applies: `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test` before every commit. On 2026-10-27 all three exited 0 on the macOS host, and the containerd-backed suites passed against the live Linux daemon (4/4 lifecycle with `MINGA_REQUIRE_CONTAINERD=1`, repeated three times with no residue; 6/6 client tests).

---

## What is proven, what is planned

**Proven today, with a command you can rerun:** the containerd runtime starts, lists, reports status and stops a real container, repeatedly, without residue. NATS TLS refuses to connect without CA and client material and verifies SANs. The Compose parser fails loudly on unknown keys. The Docker API accepts a service and writes it to Raft. The probe test shows the scheduler and supervisor correctly create and run tasks **when a node exists**.

**Not proven, and claimed nowhere:** a Compose file deployed through `minga deploy`; containers reaching each other; published ports; secrets in a real container; logs from a real container; any node-to-node encryption beyond NATS; any Talos host; any compliance posture.
