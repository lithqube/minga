# QA v1 · issue 117 · head eb58170

Docs-only change; no app code, so `cargo` suites do not apply.

| AC | Result | Proof |
|---|---|---|
| 1. File exists, at most 150 lines | pass | 83 lines, `check-links.py` |
| 2. Every claim links to an existing ADR or path | pass | 67 links, 0 missing; 4 bullets carry no inline link (lines 35, 36, 41, 75): the first three restate ADR 0008, linked in the section's first line; line 75 names the gitignored config. Non-blocking nit |
| 3. PR body has the `needs-you:` line | pass | Risk block carries it verbatim; matches the merge gate regex `^\s*(?:[-*]\s*)?needs-you:\s*(.+?)\s*$` |

Claims spot-checked against sources: 404 not 403 and paging (ADR 0008 l.29), one tenant per worker (l.36),
`O=*` operator (l.49), seven-question checklist (l.92), `MINGA_STATUS` / `MINGA_LEASES` / `MINGA_ASSIGN`
and 30 s lease (ADR 0006 l.27-47), only managers vote (l.21), `minga-wg0` and routed L3 (ADR 0007 l.13-25),
mesh peers within a tenant (ADR 0008 l.41), bundled nats-server and two CI versions (ADR 0008 l.88),
`format_version` and frozen fixtures (architecture.md l.38), only `minga-bus` depends on `async-nats`
(grep of `crates/*/Cargo.toml`), `check_placeable` (`placement.rs:73`), CI jobs `containerd` and `netns`.
