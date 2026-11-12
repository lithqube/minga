# ADR 0005: API Identity, Roles and Audit

**Status:** Proposed
**Date:** 2026-11-12
**Deciders:** Ferru

## Context
The Docker Engine API served by `minga api` is the control plane: whoever can write to it can run any image on any node. Today it has one shared secret and nothing else. Read against the code at `c94fdc0`:

- **One shared bearer token, no identity.** `bearer_auth` (`crates/minga-api/src/server.rs:64`) compares the presented token with the configured one. The Basic-auth password is also accepted, because Traefik's Swarm provider can only send `username`/`password` (`presented_token`, `server.rs:49`). Every holder of the token is the same, all-powerful caller.
- **No roles.** A reader and a deployer have the same access.
- **No audit.** The only log lines are per-handler `tracing::info!` messages such as "service updated via API" (`crates/minga-api/src/handlers.rs:462`). They do not say who, and a rejected request logs nothing.
- **Every route is behind the token, including probes.** The layer is added after the observability router is merged (`server.rs:150-151`), so `/healthz`, `/readyz` and `/metrics` need it too.
- **No token means no check.** With no token configured every request passes (`server.rs:83-85`) and nothing is logged. The default bind is `0.0.0.0:2375` (`crates/minga-cli/src/main.rs:321`), so a bare `minga api` is an unauthenticated control plane on every interface. The flag's own help text calls this "unauthenticated remote code execution by design" (`main.rs:323-326`), but the process starts anyway.
- **No TLS on the listener.** The server is `axum::serve(listener, router)` on a plain `TcpListener` (`server.rs:206`). The token, and the specs and secret metadata in every response, cross the network in clear text.
- **Raft mTLS does not check who the peer is.** `RaftTlsConfig::server_config` (`crates/minga-state/src/network.rs:107`) builds a client verifier from the CA and requires a certificate, but never looks at the peer's name. Any certificate that CA signed can open a Raft connection. The client side does pin the name: it dials with `MINGA_NODE_NAME`, `minga-node` (`network.rs:38`, `:254`). This matters as soon as a second kind of certificate exists: if operator certificates came from the Raft CA, an operator could vote, replicate and be forwarded writes.
- **`minga tls` issues node certificates only** (`main.rs:660-788`, rcgen 0.13). The node certificate has SAN `minga-node`, uses rcgen's default validity (the default `not_after` is the year 4096) and carries no role. The CA and node key files are written with plain `std::fs::write`, so their mode comes from the umask, not 0600.
- **`CorrelationIdLayer` is not an HTTP request id.** It exists in `minga-telemetry` (`crates/minga-telemetry/src/logging.rs:34`) but is a `tracing` layer that copies a `correlation_id` span attribute. Nothing attaches it to HTTP, and nothing in `minga-api` sets one.
- **Service updates ignore `rollback` and `version`.** `update_service` (`handlers.rs:416-465`) takes `Query(_query)` and never reads it, though `ServiceUpdateQuery` models both (`crates/minga-api/src/models.rs:777-784`). `Service` in state already has a `previous` field, but it is a transient auto-rollback target: it is "kept until the rollout settles" (`crates/minga-state/src/types.rs:268-270`) and then cleared (`SettleService`, `crates/minga-scheduler/src/reconcile.rs:32-33`). `Service` has no version, so `Version.Index` is always `1` (`crates/minga-api/src/translate.rs:524`).

`docs/minga-ctl-plan.md` section 2.3 outlines this work as "phase 2" and says it needs its own ADR. Step F of `docs/production-readiness-plan.md` (on PR #34) cuts it to what single-node production needs. This ADR records the decisions; the server work (F2 to F4) starts after review.

## Decision

### 1. TLS on the API listener
`minga api` gains `--api-tls-cert`, `--api-tls-key` and `--api-client-ca`, named after the existing `--raft-tls-*` flags.

- `--api-tls-cert` and `--api-tls-key` come together and turn the listener into TLS (rustls, ring provider, the same as Raft).
- `--api-client-ca` requires the other two and makes a client certificate mandatory at the handshake. A caller without one never reaches a handler.
- The accept loop replaces `axum::serve` with a `tokio_rustls::TlsAcceptor` on `hyper-util`. Both are already workspace dependencies (`Cargo.toml:22`, `:37`); `minga-api` does not use them yet. The loop reads the verified peer certificate and attaches `Caller { name, role }` to the request as an extension.
- The PEM loaders in `network.rs:53-78` become public and are reused, so there is one PEM parser.

### 2. The role lives in the client certificate
An operator certificate carries:

| Field | Value |
|---|---|
| Subject `CN` | the operator, for example `alice` |
| Subject `OU` | exactly one of `viewer`, `deployer`, `admin` |
| Extended key usage | `clientAuth` only |
| Basic constraints | not a CA |
| Validity | 30 days |

The server takes the caller name from `CN` and the role from `OU`. A certificate with zero or several `OU` values, an unknown role, no `CN`, or a missing `clientAuth` usage is accepted by the TLS layer but treated as having no role. Every route then returns 403.

The role is in the certificate, not in a server-side table, so there is nothing to keep in sync across restarts and a role change is a reissue.

### 3. Operator certificates come from their own CA, and Raft checks the peer name
- Operators are issued from a **separate CA**, made with the existing `minga tls init-ca` in a different directory. `--api-client-ca` points at that CA and nothing else. A node certificate (from the Raft CA) is not accepted by the API, and an operator certificate is not accepted by Raft.
- `minga api` also opens the Raft store (it has the `--raft-tls-*` flags, `main.rs:311-319`). It refuses to start if `--api-client-ca` and `--raft-tls-ca` hold the same certificate.
- **Raft also verifies the peer name.** After the handshake, the server side of the Raft transport checks that the peer certificate has SAN `minga-node` and drops the connection if not. This is defence in depth, not a replacement for the CA split: today a certificate the Raft CA signs for any purpose is accepted, and nothing but operator discipline keeps those certificates to nodes. The check is the server-side half of what the client already does with `MINGA_NODE_NAME`.

### 4. Token-only mode stays, as admin, and the open-bind default goes away
- `--api-token` keeps working exactly as today (Bearer or Basic password), so Traefik and existing deployments are unaffected. A request authenticated by the token has no name and counts as **admin**. It is a shared secret; it is the compatibility path, not the target.
- With `--api-client-ca` set, the handshake already demands a certificate, so the token matters only when a TLS listener has no client CA (token over TLS) or when the listener is plain.
- **`minga api` refuses to start** on a non-loopback bind (anything other than 127.0.0.0/8 or `::1` after resolving `--api-bind`) with neither `--api-token` nor `--api-client-ca`. The error names both flags.
- On a loopback bind with neither, it starts and logs a warning on every start that the API is unauthenticated and any local process, including a container that can reach loopback, has full control.
- A token without TLS on a non-loopback bind starts, with a warning that the token crosses the network in clear text.
- The default `--api-bind` stays `0.0.0.0:2375`, so a bare `minga api` now fails to start. That is deliberate: it turns a silent open control plane into a loud error. See Consequences.

### 5. Role matrix
Authorization runs after authentication, on the method and matched route, in a middleware next to `bearer_auth`.

| Request | Minimum role |
|---|---|
| `GET` and `HEAD` on any route, including `/healthz`, `/readyz`, `/metrics` | viewer |
| `POST` or `DELETE` on services (`create`, `update`, delete) | deployer |
| `POST` or `DELETE` on networks, configs and secrets (metadata) | deployer |
| `POST` on nodes (`create`, `update`) | admin |
| state import and export | admin |
| any other state-changing request | admin |
| role missing or below the minimum | 403 with a Docker-style `{"message": ...}` body |

Roles are ordered, so deployer includes viewer and admin includes both. The state import and export endpoints do not exist yet; they arrive with the backup work (plan step D) and must be registered as admin when they do. Routes that match nothing fall through to the existing 404 fallback after authentication.

Probes (`/healthz`, `/readyz`) and scrapers (`/metrics`) therefore need an identity, as they do today. A Kubernetes probe or Prometheus job uses a `viewer` certificate or, on loopback, the token. ADR 0004 already documents the endpoints as "behind `--api-token`"; this keeps that, with certificates alongside.

### 6. Audit line
Every state-changing request (anything but `GET` and `HEAD`) produces exactly one JSON line through the existing `tracing` JSON output (ADR 0004), when the response is ready:

```json
{"timestamp":"…","level":"INFO","target":"minga_api::audit","message":"api request","caller":"alice","role":"deployer","auth":"cert","method":"POST","path":"/v1.41/services/web/update","status":200,"request_id":"3f9a…"}
```

- `caller` is the certificate `CN`, or `token` for token callers. `auth` is `cert` or `token`.
- Rejected requests (401 and 403) are logged too, with `caller` set to the `CN` if a certificate was verified and `null` otherwise. A denied attempt is the line an auditor wants most.
- `request_id` is a random 128-bit value generated per request, returned in an `X-Request-Id` response header, and also written as `correlation_id` so it lines up with the field ADR 0004 already defines for logs. An inbound `X-Request-Id` of at most 64 `[A-Za-z0-9._-]` characters is kept; anything else is replaced. `CorrelationIdLayer` is not used: it reads span attributes and does not touch HTTP.
- Request and response bodies are never logged. Secret values, registry credentials and Compose environments pass through those bodies.

### 7. Certificates: 30 days, no revocation list yet
- `minga tls issue-client --ca-dir <dir> --out-dir <dir> --cn <operator> --role viewer|deployer|admin --days 30` writes `<cn>.pem` and `<cn>-key.pem`. `--days` defaults to 30 and is capped at 30; a longer lifetime needs a new ADR. It refuses to overwrite existing files, like `init-ca`.
- Node certificates keep their current behaviour except for the key-file mode below. Their long validity is out of scope here.
- **All `minga tls` output** (`ca-key.pem`, `node-key.pem`, the new client key) is created with mode 0600, by opening the file with that mode rather than writing and then `chmod`, so there is no window where it is world-readable. Certificates stay 0644.

**Known limit: there is no revocation list.** A leaked operator certificate stays valid until it expires, at most 30 days. What to do on compromise, in order:
1. Remove the operator from whatever grants them network access to the API.
2. If the certificate was `deployer` or `admin`, assume the cluster state was readable and writable by the holder. Review the audit lines for that `caller`, and re-check services, secrets and nodes.
3. To cut access before expiry: replace the operator CA (`init-ca` in a new directory), restart `minga api` with the new `--api-client-ca`, and reissue certificates to everyone else. This is disruptive on purpose; it is the only lever that does not need new code.
4. If the token was also in use, rotate it. Token callers are admin.

A deny list by `CN` or serial, read at start, is the cheapest next step and is left to a later ADR so this one stays reviewable.

### 8. Server-side revisions and conflict detection
This part depends on the versioned state format (plan step C) because it adds fields to `Service`.

- `Service` gains `version: u64` (starts at 1, incremented on every accepted update) and `last_settled: Option<Box<Service>>`, the previous spec kept after a rollout settles. Both are `#[serde(default)]`. The existing `previous` field keeps its meaning (the rollout-in-progress target the scheduler uses, cleared when the rollout settles); `last_settled` is the operator's rollback target and survives it.
- `update_service` reads its query. `?version=N` must equal the stored version or the request fails with 409 and the current version in the body. The response and `GET /services/{id}` return `Version.Index = version` and `PreviousSpec`.
- `?rollback=previous` restores `last_settled` as the new spec (a new version, not a rewind), using the service's `rollback` policy. With no `last_settled` it returns 400.
- minga-ctl already sends the `version` it read and keeps a local revision file; once the server holds the revision it prefers it and keeps the local file as a fallback (F4).

### 9. Control mapping (supporting evidence only)
As in `docs/minga-ctl-plan.md` section 2.4: SOC 2 attests an organisation's controls. This ADR provides mechanisms and log evidence; it does not make a deployment compliant, and nothing here should be read as a claim to that effect.

| Criterion | What this ADR gives | Stays with the organisation |
|---|---|---|
| CC6.1 logical access | A certificate per operator, bound to a key that is never sent | CA custody, issuing and storing keys |
| CC6.2, CC6.3 provisioning, least privilege | Three roles, 30-day certificates | Joiner and leaver process, access reviews |
| CC6.6 boundary protection | TLS listener; refusal to start open on a non-loopback bind | Firewalls, network placement |
| CC6.7 data in transit | TLS on the API | Key management |
| CC7.2 monitoring | One audit line per change, including denied ones, with identity and request id | Log shipping, retention, alerting |
| CC8.1 change management | Server-held revisions, 409 on a stale update | Review and approval, CI gates |

## Consequences
- **Positive:** every change has a name and a role behind it; a read-only credential exists for probes and dashboards; the API can be exposed beyond loopback without a shared secret on the wire; the Raft transport stops trusting any certificate its CA ever signed; key files stop depending on the umask.
- **Negative:**
  - **Breaking default.** A bare `minga api` (no token, default bind `0.0.0.0:2375`) stops starting. Anyone relying on it must set `--api-token`, `--api-client-ca`, or a loopback `--api-bind`. This goes in the release notes.
  - Two CAs to look after instead of one, and operators must be reissued every 30 days. That needs tooling or a calendar; `issue-client` is deliberately a one-command job.
  - Revocation is coarse until a deny list exists (see Decision 7).
  - The token remains a shared admin secret for as long as it is configured. Environments that want only certificates should leave `--api-token` unset on a TLS listener.
  - minga-ctl needs `client_cert` and `client_key` context keys (F4); SSH tunnelling keeps working but is no longer the only secure path.
  - Raft peers with certificates that lack SAN `minga-node` stop connecting. `minga tls issue-node` always sets it, so only hand-made certificates are affected.
- **Open questions for review:**
  - Should `/healthz` stay behind authentication, or be exempt for load balancers that cannot present a certificate? The proposal keeps it behind, to match ADR 0004 and today's behaviour.
  - The API server certificate: issue it from the operator CA, or from an organisation CA via `--api-tls-cert`? The flags accept either; no `issue-server` command is proposed here.

## Alternatives considered
- **One CA for nodes and operators** — rejected: Raft accepts any certificate the CA signed, so an operator certificate could join consensus. The CA split and the `minga-node` name check close that independently.
- **JWT or OIDC** — rejected: needs an identity provider and key distribution that air-gapped and edge sites do not have, adds a large dependency to the API process, and still needs transport security.
- **Per-operator bearer tokens** — rejected: static secrets in a file, no expiry, identity is a table lookup, and they cross the wire in a header. Certificates give identity, role and expiry in one artifact and keep the private key on the client.
- **Roles in a server-side table keyed by `CN`** — rejected: a second source of truth that drifts from the certificate and needs a reload on change; with 30-day certificates a reissue is an acceptable role change.
- **TLS terminated by a reverse proxy that forwards identity in headers** — rejected: the API would have to trust the headers, so anything that can reach it directly can claim to be an admin.
- **Keep the shared token and add only TLS and an audit line** — rejected: the audit line would still name no one.
- **Revocation list in the first cut** — deferred, not rejected: see Decision 7.

## Verification
- `bearer_auth_rejects_and_accepts` (`server.rs:939`) keeps passing unchanged: the token path still works and is admin.
- Role tests beside it: each row of the matrix returns 2xx for the minimum role and 403 below it; a certificate with a bad or missing `OU` gets 403 everywhere.
- TLS tests reuse the `Pki` helper from `crates/minga-state/tests/raft_tls.rs`: no client certificate fails the handshake; a certificate from the Raft CA fails against `--api-client-ca`; a Raft peer whose certificate lacks SAN `minga-node` is dropped.
- Start-up tests: a non-loopback bind with no token and no client CA exits non-zero; a loopback bind logs the warning; a client CA equal to the Raft CA is rejected.
- Audit: one line per `POST` or `DELETE`, none for `GET`, one for a 403, with the fields above and no body content.
- `minga tls`: every key file it writes is mode 0600 (`stat`); `issue-client --days 31` fails.
- Revisions (after step C): a stale `?version=` returns 409; `?rollback=previous` restores `last_settled`; a pre-C state file without the new fields still loads.

## Implementation status (2026-11-12)
Proposed; nothing in this ADR is implemented. The current state is the "Context" section above. Planned in `docs/production-readiness-plan.md`:

- **F1** — this ADR, reviewed before F2 starts.
- **F2** — TLS listener, role middleware, audit line, `minga tls issue-client`, 0600 key files, the start-up refusal, the Raft peer-name check.
- **F3** — server-side revisions and the 409; after step C (versioned state format).
- **F4** — minga-ctl `client_cert` and `client_key` context keys; `service rollback` prefers the server's revision.
