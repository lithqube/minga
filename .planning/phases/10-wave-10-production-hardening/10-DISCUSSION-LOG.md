# Wave 10 Discussion Log

**Phase:** 10
**Name:** production-hardening
**Date:** 2026-10-26
**Participants:** User (visionary), Assistant (builder)

---

## Session Overview

**Priorities:** B (TLS) → C (Secrets) → A (Container Logs)
**Constraints:** Talos/hcloud, FIPS/SOC2/VS-NfD/NATO, 1 dev
**Definition of Done:** Tests + quality gate, deployable to staging, docs complete

---

## TLS (B) — NATS + containerd gRPC

### CA Model
**Options:**
- Talos built-in PKI (recommended — Talos manages CA, certs, rotation via talosctl)
- External CA (Vault, cert-manager, ACME)
- Self-signed with manual distribution

**Decision:** Talos built-in PKI (recommended — Talos manages its own CA, certs, rotation via talosctl)

**Rationale:** Zero external dependencies, automated rotation, Talos-native, fits air-gapped/edge.

### mTLS Layers
**Options:**
- NATS client↔server + server↔server
- containerd gRPC client↔server
- Both NATS and containerd gRPC
- Minga API HTTPS + mTLS

**Decision:** NATS client↔server (all nodes) + NATS server↔server

**Rationale:** Highest impact for cluster security. containerd gRPC addressed separately.

### Client Cert Source
**Options:**
- Talos generates per-node certs; Minga reads from `/etc/minga/tls/` (machine config)
- Minga generates certs at startup using Talos CA (via talosctl API)
- SPIFFE-like node identity

**Decision:** Talos generates per-node certs; Minga reads from `/etc/minga/tls/` (Talos machine config)

**Rationale:** Talos already does this; Minga just reads files. No custom cert logic.

### containerd gRPC TLS
**Options:**
- Talos CA + node certs (Talos 1.14+ supports TLS on containerd socket)
- Skip (local Unix socket, Talos isolates via namespaces)
- TLS only for remote containerd

**Decision:** Use Talos CA bundle + node cert/key for containerd gRPC (Talos 1.14+ supports TLS on containerd socket)

**Rationale:** Defense in depth; Talos 1.14+ supports it natively.

### Minga API TLS
**Deferred:** Docker Engine API HTTPS + mTLS for admin clients — lower priority than NATS/containerd.

---

## Secrets (C) — Talos-native + Docker Swarm Compatible

### Backend
**Options:**
- HashiCorp Vault (HCP or self-hosted)
- Infisical (open-source)
- Talos-native (machine config secrets)
- Cloud provider secret managers

**Decision:** Talos-native (machine config secrets) — minimal, no external deps

**Rationale:** FIPS/SOC2/VS-NfD/NATO compliance via Talos encryption; no external service to manage.

### Secret Storage on Nodes
**Options:**
- Talos machine config (encrypted at rest, root-only)
- Talos secretbox (age-encrypted)
- Vault agent injector

**Decision:** Talos machine config (encrypted at rest, only readable by root)

**Rationale:** Talos encrypts machine config at rest; root-only access at runtime.

### Delivery to Containers
**Options:**
- Runtime injection via containerd task env (current TaskSpec.env path)
- File-based mount `/run/secrets/<name>` (Docker Swarm compatible)
- Both

**Decision:** File-based mount (`/run/secrets/<name>`) — Docker Swarm compatible, requires OCI spec changes

**Rationale:** Compose files deploy unchanged; Swarm compatibility is a design principle.

### Mount Mechanism
**Options:**
- OCI spec `mounts` with `type=bind,source=/run/secrets/<name>,target=/run/secrets/<name>`
- containerd `Container.Spec.Mounts` with `options=["ro"]`
- Custom OCI hook/snapshotter plugin

**Decision:** OCI spec `mounts` with `type=bind,source=/run/secrets/<name>,target=/run/secrets/<name>`

**Rationale:** Standard OCI mechanism; read-only via `options=["ro"]`.

---

## Container Log Capture (A) — FIFO + Structured JSON

### Transport Mechanism
**Options:**
- `file:///var/lib/minga/logs/{task_id}.log` (merged stdout+stderr)
- Split stdout/stderr files
- FIFO pipes with background readers writing structured JSON

**Decision:** FIFO pipes (mkfifo) with background readers writing structured JSON

**Rationale:** Enables live streaming + history; structured JSON for parsing; most flexible.

### Log Format
**Decision:** Structured JSON per line: `{"timestamp":"...","stream":"stdout|stderr","message":"..."}`

**Rationale:** Machine-parseable, correlates with `minga-telemetry` correlation IDs.

### Read Mechanism
**Options:**
- Read last N lines from file on demand
- In-memory ring buffer per task
- containerd's binary log format + index

**Decision:** Use containerd's binary log format + index (complex, but standard)

**Rationale:** Standard containerd mechanism; efficient; works with existing tooling.

### Retention Policy
**Options:**
- Delete on task stop
- Retain N hours/days (cron cleanup)
- Retain on failure, delete on clean stop (policy-aware)

**Decision:** Retain on failure, delete on clean stop (policy-aware)

**Rationale:** Debug failures, don't clutter disk with successful task logs.

---

## Summary of Decisions

| Area | Decision |
|------|----------|
| TLS CA | Talos built-in PKI |
| NATS mTLS | Client↔server + server↔server |
| Client Certs | `/etc/minga/tls/` from machine config |
| containerd gRPC TLS | Talos CA + node certs on socket |
| Secrets Backend | Talos machine config |
| Secrets Storage | Machine config (encrypted, root-only) |
| Secrets Mount | OCI bind mount `/run/secrets/<name>` ro |
| Log Transport | FIFO + structured JSON background readers |
| Log Format | JSON lines with timestamp/stream/message |
| Log Read | containerd binary log format + index |
| Log Retention | Retain failures, delete clean stops |

---

## Deferred Ideas (Future Phases)

- Rolling upgrades (task drain + Raft membership)
- Agent executor hardening (real WASM failure/restart)
- Distributed tracing (OpenTelemetry)
- Log aggregation (Vector/Fluent Bit)
- Minga API HTTPS/mTLS

---

## Next Steps

1. Create Wave 10 plan (`/gsd:plan-phase 10`)
2. Research implementation details if needed
3. Execute with quality gate verification