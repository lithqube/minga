---
phase: 10
slug: wave-10-production-hardening
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-10-27
---

# Phase 10 — Validation Strategy

## Test Infrastructure

| Property | Value |
|---|---|
| Framework | Rust `cargo test`; integration tests backed by NATS and Linux containerd |
| Config | Root `Cargo.toml` workspace |
| Quick run | `cargo test -p minga-bus -p minga-node -p minga-scheduler` (select relevant crate when narrowing) |
| Full quality gate | `cargo fmt --all` then `cargo clippy --all-targets -- -D warnings` then `cargo test` — each unpiped, record individual exit code |
| Runtime | Measure on CI; no duration claimed yet |

## Sampling Rate

- After each implementation task, run the focused crate test (without masking failures).
- After each sequence B, C, A run the unpiped full quality gate.
- Linux-backed tests require an actual `containerd` + OCI runtime; skipped tests do not count as passage of integration acceptance.
- Staging requires a Talos/hcloud project and permission to create billable resources. No resources may be provisioned without explicit operator authorization.

## Per-Task Verification Map

| Plan | Requirement | Secure behavior | Automated proof | Environment |
|---|---|---|---|---|
| 10-01 | W10-STAGE | Talos extension service boots, persists data and can reach allowed socket | validate manifests, review official schema | Linux Talos test VM / hcloud staging |
| 10-02 | W10-TLS | reject plaintext, wrong CA, unauthorized identity; rotate | `cargo test -p minga-bus`; sidecar config tests | real NATS instance/cluster |
| 10-03 | W10-SEC | missing/unsafe secret fails closed, value absent from state/API | `cargo test -p minga-scheduler -p minga-node -p minga-api` | Linux containerd for mount proof |
| 10-04 | W10-LOG | real stdout/stderr and bounded history | `cargo test -p minga-node -p minga-bus` | Linux containerd+runc for output proof |
| 10-05 | W10-STAGE, W10-COMP | corrected docs, control gaps, end-to-end evidence | unmasked full quality gate and linked evidence checklist | authorized hcloud staging |

## Wave 0 Requirements

- [ ] Linux environment capable of running a real OCI container for FIFO/log/secret tests.
- [ ] Talos extension-service feasibility spike and reproducible non-secret setup artifacts.
- [ ] A test NATS server/cluster with isolated disposable certificates.

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test instruction |
|---|---|---|---|
| Talos/hcloud production-equivalent staging | W10-STAGE | requires billable cloud account and approved credentials | operator authorizes project; record server, service status and test commands with secrets removed |
| Compliance qualification | W10-COMP | only competent assessor/authority can define scope/accreditation | identify authority/scheme, inventory modules, map controls and record gaps; do not claim pass |

## Validation Sign-Off

- [ ] Every implementation task has a focused automated command or an explicit manual checkpoint.
- [ ] Independent Rust gate exit statuses recorded.
- [ ] Linux container tests executed rather than silently skipped.
- [ ] Staging and compliance claims held pending operator/assessor evidence.

**Approval:** pending.
