# Minga planning roadmap

This file starts GSD tracking at Wave 10. Waves 1–9 were developed before this planning workspace; see `README.md` and `Wave-9-PLAN.md` for historical descriptions. Their implementation and deployment claims are **not** independently verified by this roadmap.

## Phase 10: Production hardening — TLS, secrets and container logs
**Goal:** Secure NATS client and cluster traffic, deliver file-based container secrets without storing values in Raft, capture real container logs, and validate a Talos/hcloud staging deployment without unsubstantiated compliance claims.
**Depends on:** Wave 9 observability baseline and a verified Talos/hcloud host architecture.
**Requirements:** W10-TLS, W10-SEC, W10-LOG, W10-STAGE, W10-COMP
**Mode:** standard
**Status:** Pending

Sequential implementation for one developer: B (NATS TLS/transport boundary) → C (secrets) → A (stdout/stderr log capture), followed by staging and documentation. The host-feasibility and compliance-control gates must happen before claims of production readiness.

### Plans

**Wave 1 — host feasibility / blocker gate**
- [ ] `10-01-PLAN.md` — supported Talos extension service, hcloud image/ISO correction, operator staging permission.

**Wave 2 *(blocked on Wave 1)* — B: TLS**
- [ ] `10-02-PLAN.md` — NATS client and route mTLS; local containerd UDS access control.

**Wave 3 *(blocked on Wave 2)* — C: secrets**
- [ ] `10-03-PLAN.md` — Compose refs, safe node-local resolver, read-only OCI mount.

**Wave 4 *(blocked on Wave 3)* — A: logs**
- [ ] `10-04-PLAN.md` — real Linux FIFO proof, bounded Minga-owned history and retention.

**Wave 5 *(blocked on Wave 4)* — integration and sign-off**
- [ ] `10-05-PLAN.md` — authorized hcloud staging, documentation correction and compliance gap matrix.

**Cross-cutting constraints:** No values/credentials in distributed state or evidence; local containerd socket is privileged; unmasked quality gate; no certification or staging-success claims without proof.
