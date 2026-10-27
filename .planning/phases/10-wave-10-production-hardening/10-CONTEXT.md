# Wave 10 Context: Production Hardening (TLS, Secrets, Container Logs)

**Phase:** 10
**Name:** production-hardening
**Date:** 2026-10-27
**Status:** Decisions gathered; feasibility gates required before implementation planning. Prior discussion recorded unverified technical assumptions; research findings below supersede those assumptions, not user goals.

---

## Domain

Wave 10 plans **B → C → A**, in sequence for one developer: (B) NATS transport security, (C) file-based secrets, and (A) actual container stdout/stderr capture. **Talos/hcloud is a staging target to validate, not a working deployment claim.** FIPS 140-3, SOC 2, VS-NfD and NATO are distinct stakeholder requirements requiring control mapping and specialist review; none is presently certified or established as compliant.

**Definition of done:** unmasked Rust quality gate, a functioning Talos/hcloud staging demonstration (or explicit blocker, never a false pass), and corrected complete deployment/security documentation. Talos is designed to run Kubernetes and its documented provisioning path bootstraps etcd/Kubernetes; Minga's independent Raft cluster must be proven possible on this host before implementation is called deployable.

---

## Prior Decisions (Carried Forward)

### From Wave 9 (Observability)
- Prometheus metrics (`minga-telemetry` crate), `/healthz`, `/readyz`, structured JSON logging
- `minga-telemetry` integrated into `minga-api` and `minga-node`

### From Wave 8 (NATS + Restart)
- NATS/JetStream event bus with `MINGA_LOGS` stream (`minga.log.>`)
- Restart policies: `None`, `OnFailure`, `Any` with exponential backoff
- Containerd gRPC client working (corrected to v1.7 paths)

### From Wave 7 (Agent Workloads)
- `x-minga-agent` Compose extension, `WorkloadKind::Agent`, WASM sandbox, LLM egress proxy

---

<decisions>
## Implementation Decisions

- **D-01:** One-developer sequence is B (NATS TLS) → C (file secrets) → A (container logs); Talos/hcloud host feasibility is an earlier blocking prerequisite.
- **D-02:** Authenticate NATS client and cluster traffic with distinct workload identities; prefer Talos-issued certificates only if a supported separate issuance path is proved, otherwise use a dedicated workload CA. Do not claim TLS on the local containerd Unix socket.
- **D-03:** Deliver service secrets as read-only `/run/secrets/<name>` files from a verified, protected node-local store; never distribute their values in Raft or NATS. Talos machine config encryption is not presumed.
- **D-04:** Prefer separate stdout/stderr FIFOs with background capture and indexed Minga-owned bounded history, contingent on real Linux containerd proof; retain failures under finite quotas and delete successful local logs only after delivery.
- **D-05:** Definition of done is independently passing unmasked Rust quality gates, authorized Talos/hcloud staging evidence and complete corrected documentation, plus distinct FIPS/SOC 2/VS-NfD/NATO gap mapping; no certification claim.

</decisions>

## Locked Decisions (This Discussion)

### TLS (B) — NATS + containerd gRPC

| Decision | Detail |
|----------|--------|
| **PKI intent** | Prefer Talos-managed certificates if supported; verify whether Talos PKI can issue **separate NATS workload identities**. Do not reuse Talos API/node private keys. If unsupported, plan a dedicated NATS CA with documented offline issuance/rotation. |
| **NATS mTLS** | Client↔server and server↔server, with server-name verification and rotation/reconnect tests. |
| **Client Cert Source** | Requested `/etc/minga/tls/` inside Minga's service/container, contingent on an actual Talos extension/pod delivery mechanism. Machine config does **not** imply that files appear there automatically. |
| **containerd transport** | Talos's local Unix socket is access-controlled by filesystem permissions; TLS on a Unix socket is not established by Talos documentation. Keep local UDS and verify access boundary. Remote TCP containerd mTLS is conditional on a documented daemon config and supported tonic client. |
| **Minga API TLS** | Deferred as a feature, but staging must restrict unauthenticated/plaintext admin API to localhost or a trusted private boundary; never claim cluster-wide mTLS. |

**Canonical Refs:**
- `docs/talos-deployment.md` (Talos PKI, machine config)
- `crates/minga-bus/src/nats.rs:76` (NATS connect)
- `crates/minga-node/src/containerd/client.rs` (containerd gRPC client)
- `crates/minga-api/src/server.rs` (API server)

### Secrets (C) — Talos-native + Docker Swarm Compatible

| Decision | Detail |
|----------|--------|
| **Backend intent** | Talos-native provisioning where independently feasible; Talos machine config is **not** a generic encrypted application secret manager. Evaluate Talos user volumes or another supported provisioning path; explicitly define at-rest encryption and key custody. |
| **Secret Values** | Never put plaintext values in Raft, NATS, CLI output or logs. A metadata-only Raft reference may identify a local provisioned secret, but missing secret must fail closed. |
| **Mount Mechanism** | OCI spec read-only bind mount of an allowlisted per-task host file to `/run/secrets/<name>` inside the container. Verify source directory, mode/ownership, secure path resolution, no symlink escape and lifecycle cleanup. |
| **Delivery** | File-based Swarm-compatible destination; verify full Compose top-level and per-service secret semantics; reject unsupported values explicitly. |

**Canonical Refs:**
- `crates/minga-state/src/types.rs:241` (Secret metadata)
- `crates/minga-api/src/translate.rs:156` (Secret translation)
- `crates/minga-node/src/containerd/oci.rs` (OCI spec generation)
- `crates/minga-node/src/containerd/runtime.rs:118` (task start)

### Container Log Capture (A) — FIFO + Structured JSON

| Decision | Detail |
|----------|--------|
| **Transport intent** | Separate stdout/stderr FIFOs with readers established before `Tasks.Create`; prove containerd shim opens them on the target Linux runtime without deadlock. |
| **Format** | Persist framed records with timestamp, stream and bytes (lossless for non-UTF-8), then serialize CLI/event output safely. Bound records and disk usage. |
| **Read mechanism** | Prior discussion asked for “containerd binary log format + index,” but no standard indexed binary log API has been substantiated. `Tasks.Create` accepts caller-provided stdout/stderr destinations, not a log-read RPC. Research a bounded local index/JSONL implementation; do not claim containerd owns it. |
| **Retention** | Retain failures within finite TTL/size/quota; delete successful-task local files after confirmed delivery. Recover crash/restart and disk-full behavior; JetStream history has independent expiry. |
| **Streaming** | Reader can publish to `MINGA_LOGS`; test no loss, ordering, consumer disconnect, and restart recovery. |

**Canonical Refs:**
- `crates/minga-node/src/containerd/runtime.rs:155` (create_task stdout/stderr)
- `crates/minga-node/src/containerd/runtime.rs:347` (logs() stub)
- `crates/minga-node/src/supervisor.rs` (task lifecycle, cleanup)
- `crates/minga-bus/src/nats.rs` (MINGA_LOGS stream)

---

## Technical verification gates (research takes precedence over earlier assumptions)

1. **Talos/hcloud host model:** `docs/talos-deployment.md` and `README.md` contain unverified `talosctl upload/run`, systemd, ISO-as-image, `minga node`, and price claims. Talos is immutable and does not provide a conventional systemd/package path. Before calling staging deployable, prototype a documented extension/container mechanism with the required containerd socket, dedicated persistent storage, process supervision and firewall; verify hcloud boot image workflow and actual CLI flags. Correct both docs as part of the phase.
2. **PKI and TLS:** Talos API certs are not automatically issued for Minga/NATS. Prove issuance/isolation and rotation in staging or use a dedicated CA. The containerd Unix socket is local; protect it with OS permissions, not an invented Talos TLS feature. TLS on a remote TCP endpoint is optional and needs separate testing.
3. **Secrets:** Talos machine config should not be described as universally encrypted at rest. Prove a supported at-rest encryption/provisioning mechanism, permission isolation, revocation, rotation and cross-node availability; otherwise explicitly block deployment rather than silently substitute plaintext config or Raft values.
4. **Logs:** Containerd's Tasks API offers caller-specified stdout/stderr destinations, not an indexed binary log retrieval API. Test FIFOs on a real Linux containerd+runc host; if FIFOs fail, document a decision checkpoint and use verified split-file destinations instead.
5. **Compliance:** FIPS validation applies to specific cryptographic modules/versions/configurations, not the presence of `ring`/`aws-lc-rs` or Talos. SOC 2 is an attestation over organizational controls and audit period; VS-NfD/NATO require applicable national/contract authority and accreditation. Deliver a gap/control matrix and avoid certification claims.
6. **Only user decisions are binding:** B→C→A order, one developer, Talos/hcloud preference, file secrets, FIFO preference, mTLS for NATS, quality gate + staging + documentation. Alternative technical mechanisms require evidence and should be surfaced as explicit implementation checkpoints.

## Deferred Ideas (Not in Wave 10)

| Idea | Reason |
|------|--------|
| Rolling upgrades | Needs task drain + Raft membership changes; separate phase |
| Agent executor hardening | Real WASM failure/restart cycles; depends on log capture |
| Distributed tracing (OpenTelemetry) | Builds on `minga-telemetry`; Wave 11+ |
| Log aggregation (Vector/Fluent Bit) | Needs log capture first; Wave 11+ |
| Minga API HTTPS/mTLS | Docker Engine API compat; lower priority than NATS/containerd |

---

## Canonical Refs (MANDATORY for Downstream Agents)

```
docs/talos-deployment.md                           # Talos PKI, machine config, hcloud
docs/adr/0004-observability.md                     # Observability baseline
crates/minga-bus/src/nats.rs                       # NATS connect, TLS config
crates/minga-node/src/containerd/client.rs         # containerd gRPC client, TLS
crates/minga-node/src/containerd/runtime.rs        # Container lifecycle, logs, secrets
crates/minga-node/src/containerd/oci.rs            # OCI spec generation (mounts)
crates/minga-node/src/supervisor.rs                # Task lifecycle, cleanup
crates/minga-state/src/types.rs                    # Secret, TaskSpec, WorkloadKind
crates/minga-api/src/translate.rs                  # Secret/Config translation
crates/minga-api/src/server.rs                     # API server (future HTTPS)
crates/minga-telemetry/src/                        # Metrics, health, logging
```

---

## Next Steps

1. **Plan Wave 10** — `/gsd:plan-phase 10` (creates PLAN.md with tasks, verification)
2. **Research** — `/gsd:plan-phase 10 --research` (optional: deep-dive TLS/secrets/logs impl)
3. **Execute** — `/gsd:execute-phase 10` (implement per plan)

**Dependencies:** Wave 9 complete (observability). All quality gates passing.

---

## Discussion Log Summary (user preferences, subject to verification gates)

| Area | User preference | Feasibility note |
|------|-----------------|------------------|
| TLS | Talos PKI, NATS client/server and route mTLS | Dedicated workload PKI issuance may be necessary; do not reuse Talos API identity |
| containerd | Secured gRPC on Talos | Local UDS is not automatically TLS; isolate socket, investigate remote TLS separately |
| Secrets | Talos-native file secrets at `/run/secrets/<name>` | Machine-config at-rest encryption and file exposure not established |
| Logs | FIFO stdout/stderr, JSON records, indexed history | No verified native containerd binary/index format; implement bounded Minga-owned storage |
| Retention | Keep failures, delete successes | Include bounded TTL and disk quota |