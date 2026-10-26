# Wave 10 Context: Production Hardening (TLS, Secrets, Container Logs)

**Phase:** 10
**Name:** production-hardening
**Date:** 2026-10-26
**Status:** Context gathered — ready for planning

---

## Domain

Wave 10 delivers **production hardening** for Minga clusters on Talos/hcloud with FIPS/SOC2/VS-NfD/NATO compliance requirements:

1. **TLS/mTLS** — NATS cluster + containerd gRPC using Talos built-in PKI
2. **Secrets management** — Talos-native machine config + Docker Swarm-compatible `/run/secrets/<name>` mounts
3. **Container log capture** — FIFO pipes with structured JSON, containerd binary log format, policy-aware retention

**Target:** Single-dev team, Talos/hcloud, FIPS/SOC2/VS-NfD/NATO compliance

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

## Locked Decisions (This Discussion)

### TLS (B) — NATS + containerd gRPC

| Decision | Detail |
|----------|--------|
| **CA Model** | Talos built-in PKI — Talos manages CA, certs, rotation via `talosctl` |
| **NATS mTLS** | Client↔server (all nodes) + server↔server (cluster) |
| **Client Cert Source** | Talos generates per-node certs; Minga reads from `/etc/minga/tls/` (machine config) |
| **containerd gRPC TLS** | Use Talos CA bundle + node cert/key (Talos 1.14+ supports TLS on containerd socket) |
| **Minga API TLS** | Deferred — Docker Engine API HTTPS can be added later |

**Canonical Refs:**
- `docs/talos-deployment.md` (Talos PKI, machine config)
- `crates/minga-bus/src/nats.rs:76` (NATS connect)
- `crates/minga-node/src/containerd/client.rs` (containerd gRPC client)
- `crates/minga-api/src/server.rs` (API server)

### Secrets (C) — Talos-native + Docker Swarm Compatible

| Decision | Detail |
|----------|--------|
| **Backend** | Talos-native machine config (encrypted at rest, root-only readable) |
| **Secret Values** | Stored in Talos machine config (encrypted, decrypted at boot) |
| **Mount Mechanism** | OCI spec `mounts` with `type=bind,source=/run/secrets/<name>,target=/run/secrets/<name>` |
| **Mount Options** | Read-only (`options=["ro"]`) |
| **Delivery** | File-based (`/run/secrets/<name>`) — Docker Swarm compatible |

**Canonical Refs:**
- `crates/minga-state/src/types.rs:241` (Secret metadata)
- `crates/minga-api/src/translate.rs:156` (Secret translation)
- `crates/minga-node/src/containerd/oci.rs` (OCI spec generation)
- `crates/minga-node/src/containerd/runtime.rs:118` (task start)

### Container Log Capture (A) — FIFO + Structured JSON

| Decision | Detail |
|----------|--------|
| **Transport** | FIFO pipes (`mkfifo`) with background readers writing structured JSON |
| **Format** | Structured JSON per line: `{"timestamp":"...","stream":"stdout|stderr","message":"..."}` |
| **Read Mechanism** | Use containerd's binary log format + index (standard) |
| **Retention** | Retain on failure, delete on clean stop (policy-aware) |
| **Streaming** | Background readers enable live tail + history |

**Canonical Refs:**
- `crates/minga-node/src/containerd/runtime.rs:155` (create_task stdout/stderr)
- `crates/minga-node/src/containerd/runtime.rs:347` (logs() stub)
- `crates/minga-node/src/supervisor.rs` (task lifecycle, cleanup)
- `crates/minga-bus/src/nats.rs` (MINGA_LOGS stream)

---

## Code Context (Reusable Assets)

| Area | Files | Pattern |
|------|-------|---------|
| **NATS TLS** | `minga-bus/src/nats.rs:60-93` | `async_nats::connect()` with `ClientConfig::tls()` |
| **containerd TLS** | `minga-node/src/containerd/client.rs:1-50` | `tonic::transport::Channel::with_tls()` |
| **OCI Spec** | `minga-node/src/containerd/oci.rs` | `spec_to_any()` — add `mounts` for secrets |
| **Runtime Start** | `minga-node/src/containerd/runtime.rs:118` | Add stdout/stderr FIFO paths to `create_task()` |
| **Runtime Logs** | `minga-node/src/containerd/runtime.rs:347` | Implement `logs()` using binary log format |
| **Supervisor** | `minga-node/src/supervisor.rs:350-366` | Cleanup log files on task stop (policy-aware) |
| **Secret Translation** | `minga-api/src/translate.rs:156` | Extend to resolve secret values from Talos config |

---

## Compliance Requirements (FIPS/SOC2/VS-NfD/NATO)

| Requirement | Implementation |
|-------------|----------------|
| **FIPS 140-2** | Talos PKI uses FIPS-validated crypto; ensure `ring`/`aws-lc-rs` in Cargo.toml |
| **SOC2** | Audit logging via `minga-telemetry` structured JSON + correlation IDs |
| **VS-NfD** | German classified — ensure no secret leakage in logs/metrics; `SecretRedactor` in `minga-egress` |
| **NATO** | mTLS everywhere; certificate rotation automated via Talos |

---

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

## Discussion Log Summary

| Area | Key Decisions |
|------|---------------|
| TLS: CA Model | Talos built-in PKI |
| TLS: NATS mTLS | Client↔server + server↔server |
| TLS: Client Certs | `/etc/minga/tls/` from machine config |
| TLS: containerd gRPC | Talos CA + node certs (TLS on socket) |
| Secrets: Backend | Talos machine config (encrypted) |
| Secrets: Mount | OCI bind mount `/run/secrets/<name>` ro |
| Logs: Transport | FIFO + structured JSON background readers |
| Logs: Read | containerd binary log format + index |
| Logs: Retention | Policy-aware (retain failures, delete clean stops) |