# Plan: single-node production-readiness fixes

Status, 2026-11-12: steps A to E and F1 are merged and ship in v1.1.0
([release notes](release-1.1.0.md)); F2 to F4 wait for
[ADR 0005](adr/0005-api-identity-and-roles.md) to be accepted. The sections
below are the plan as written; where the build differed, the PR says so.

| Step | What | Status |
| --- | --- | --- |
| A | Published-port rules | Merged, #37 |
| B | Runtime gaps (groups, `extra_hosts`, pull cleanup) | Merged, #39 |
| C | Versioned state format | Merged, #38 |
| D | Backup and restore | Merged, #40; the route is `/v1.41/state`, not `/minga/v1/state` |
| E | Image pull without `ctr` | Merged, #43 |
| F1 | ADR 0005 | Merged as Proposed, #36 |
| F2 to F4 | API TLS and roles, server-side revisions, minga-ctl client certificates | Not started |
| From staging | Task failure reasons (#42), minga-ctl papercuts (#41) | Merged; not in the original plan |

## Context

PR #19's assessment (`docs/benchmark.md`) rates v1.0.2 at 7/10 for single-node
production. This plan covers what still blocks it, except real-system
verification, which is being run on a staging system in a separate repo.

Scope: upgrades that keep state, backup and restore, registry credentials, API
identity and roles, and the smaller runtime gaps. Multi-node work
(managers-only Raft, WireGuard mesh) is out of scope.

Decisions: the next state format needs **one last wipe** of
`--data-dir` (no migration of 1.0.2 data).

## Found while reading the code (not in the original list)

1. **Published ports hijack outbound traffic.** `netns.rs:340-358` adds
   `PREROUTING -p <proto> --dport <published> -j DNAT` with no destination
   check. A container calling any external host on a published port (for
   example outbound HTTPS while Traefik publishes 443) is redirected to the
   local task. Read from the rule, not yet observed on a live kernel.
2. **A client certificate could reach Raft.** Raft mTLS accepts any
   certificate the CA signed (`minga-state/src/network.rs:107-131`) and never
   checks the peer name. Operator certificates must not share that CA.
3. `minga api` with no token accepts every request on `0.0.0.0:2375` and logs
   no warning (`server.rs:83-85`, `main.rs:321`).
4. `minga tls` writes CA and node keys with umask modes, not 0600.
5. A failed image pull leaves the task's netns and rules behind
   (`runtime.rs:757-771`); `teardown_task` never removes the `.hosts` file.
6. `ContainerSpec.Hosts` from the Docker API is dropped silently
   (`minga-api/src/translate.rs:326`).
7. `CHANGELOG.md` repeated the 1.0.2 `### Changed` block (fixed in PR #33).

## A. Published ports (`crates/minga-node/src/netns.rs`) — v1.0.3

- `PREROUTING` rule gains `-m addrtype --dst-type LOCAL` (fixes finding 1).
- Host to `127.0.0.1:<port>`: `sysctl net.ipv4.conf.<bridge>.route_localnet=1`
  and `POSTROUTING -o <bridge> -m addrtype --src-type LOCAL -j MASQUERADE` in
  `ensure_bridge`, through the existing `ensure_rule`.
- Container to host address on a published port (itself or a sibling):
  `POSTROUTING -s <subnet> -o <bridge> -m conntrack --ctstate DNAT -j MASQUERADE`
  and `hairpin on` for each veth bridge port.
- `teardown_task` deletes the new rule form and the 1.0.2 form, and removes
  the `.hosts` file.
- Test: a root-only integration test (new CI step beside the containerd suite,
  `sudo`) that checks host to 127.0.0.1, host to its own address, container to
  host address, and that a container reaching a non-local address on the
  published port is **not** redirected.

## B. Runtime gaps — v1.0.3

- **Supplementary groups** (`containerd/oci.rs:517-613`): `lookup` also
  returns the member list of `/etc/group`; `OciUser` gains `additionalGids`.
  A numeric uid with no group takes its primary gid from the image's
  `/etc/passwd` when listed there (Docker behaviour; changelog note). Extend
  `task_user_resolves_names_from_the_top_layer` (its fixture already has
  `staff:x:50:zitadel`) and the daemon test (`id -G`).
- **`extra_hosts`**: translate `ContainerSpec.Hosts` both ways in
  `minga-api/src/translate.rs`; a task with `extra_hosts` and no network
  attachment fails to start with a clear error instead of skipping
  (`runtime.rs:440`).
- **Pull failure cleanup**: call `ensure_image` before `prepare_network` in
  `Runtime::start`; tear the network down on the user-resolution failure path.

## C. Versioned state format (`crates/minga-state`) — v1.1.0

- New `codec.rs`: `encode`/`decode` over `serde_json` (already a workspace
  dependency) and `FORMAT_VERSION: u32 = 2`. Replace every inline bincode call
  in `log_store.rs`, `state_machine.rs` and `network.rs`; drop the `bincode`
  dependency. JSON is self-describing, so the `#[serde(default)]` attributes
  already on the types start working.
- Marker: key `format_version` in `sm_meta` and `raft_meta`. On open: empty
  store writes 2; a non-empty store without the key fails with "written by
  minga 1.0.2 or older: clear --data-dir"; a higher version fails with "written
  by a newer minga"; a lower one runs a `migrate(from)` chain (empty today).
- Wire frame becomes `[u32 length][u8 format version][json]`; a peer with
  another version is refused with a clear log line.
  `// shortcut: additive changes are not detected between peers, so "upgrade
  all nodes together" stays the rule; add a schema revision when rolling
  upgrades are needed.`
- Rules, written in `docs/architecture.md` and at the top of `types.rs`: new
  fields need `#[serde(default)]`; renaming, removing or re-typing a field
  needs a version bump and a migrate step. The "append variants last" comment
  in `command.rs:37-39` goes.
- Tests: checked-in fixtures `tests/fixtures/v2/*.json` (each persisted type, a
  `StateSnapshot`, a log `Entry`) that must decode in every later build; a
  decode with an unknown field and a missing defaulted field; marker errors;
  the existing roundtrip tests (`types.rs:518-671`) and
  `state_persists_across_restart` move to the codec.
- To check while building: openraft's default purge policy against
  `get_current_snapshot` always returning `None` (`state_machine.rs:652`).

## D. Backup and restore — v1.1.0, after C

- `StateStore::export()` returns `{format_version, exported_at, services,
  networks, volumes, secrets, configs}` from the existing `list_*` calls.
  Nodes and tasks are left out: nodes re-register and the scheduler re-places.
- API: `GET /minga/v1/state` and `POST /minga/v1/state`. Import upserts through
  Raft (networks, then secrets, configs, volumes, then services), and returns
  409 when services exist unless `?overwrite=true`.
- CLI: `minga state export` and `minga state import <file> [--overwrite]`,
  following `Deploy` (`main.rs:32`, `deploy.rs`: `--api-url`, `MINGA_API_TOKEN`).
- `docs/backup.md`: what the export holds; what it does not (secret values in
  `--secret-store`, named volumes, TLS files) and how to copy those; the
  cold-copy alternative (stop both processes, copy both data dirs).
- Test: deploy, export, import into a fresh `ApiServer`, compare services
  (pattern: `crates/minga-cli/tests/deploy_e2e.rs`).

## E. Image pull without `ctr` — v1.1.0, after B

- **Spike first.** Hand-written protos (as in `containerd/proto.rs`) for
  containerd's Transfer service and the Streaming service it uses to ask for
  credentials. Prove an authenticated pull on containerd 1.7 (CI) and 2.x.
  I wrote these from memory of the containerd API; if the spike fails I stop
  and report before changing the pull path.
- `ContainerdRuntime::pull_image` uses Transfer on the existing client, so
  `--containerd-socket` is honoured, the image is unpacked by containerd, and
  the secret never appears in argv. On `Unimplemented` (containerd older than
  1.7): anonymous pulls fall back to `pull_with_ctr`; pulls with credentials
  fail and name the minimum version.
- Credentials (`runtime.rs:134-185`): typed `{username, secret}`; lookup order
  `credHelpers[host]`, `credsStore`, inline `auths`. Helper protocol:
  `docker-credential-<name> get`, host on stdin, JSON out. A configured helper
  that is missing is an error.
- `minga image pull` gains `--containerd-socket`.
- Tests: helper lookup against a stub `docker-credential-*` script; a CI
  daemon test against a local `registry:2` with htpasswd.
- Docs and site data that state the old limits (`CHANGELOG`, `README`,
  `site/src/data/{features,quickstart,comparison,faq}.ts`).

## F. API identity, roles and audit — v1.1.0

Phase 2 of `docs/minga-ctl-plan.md` section 2.3, cut to what production needs.

- **F1. ADR 0005** (`docs/adr/0005-api-identity-and-roles.md`, format of 0004).
  It records: TLS flags; role in the certificate (`CN=<operator>`,
  `OU=viewer|deployer|admin`, client-auth usage); operator certificates come
  from a **separate CA**, and Raft also checks the peer name is `minga-node`
  (finding 2); token-only mode stays for loopback and counts as admin;
  `minga api` refuses a non-loopback bind with neither token nor client CA
  (finding 3); 30-day client certificates, no revocation list yet. The ADR is reviewed
  before F2 starts.
- **F2. Server.** `minga api --api-tls-cert --api-tls-key --api-client-ca`.
  Make the PEM loaders in `minga-state/src/network.rs:53-78` public and reuse
  them. Replace `axum::serve` (`server.rs:205`) with a `TlsAcceptor` accept
  loop on `hyper-util` (already a dependency) that attaches the caller's name
  and role to the request. A role check beside `bearer_auth` (`server.rs:64`):
  GET needs viewer; service, network, config and secret writes need deployer;
  node writes and state import need admin; otherwise 403. One JSON audit line
  per changing request: caller, role, method, path, status, request id.
  `minga tls issue-client --cn --role --days 30`; all key files written 0600
  (finding 4).
- **F3. Server-side revisions, after C.** `Service` gains `version` and a kept
  previous spec (both `#[serde(default)]`); `rollback=previous` is honoured,
  `PreviousSpec` and a real `Version.Index` are returned, a stale `version`
  gets 409 (`handlers.rs:416-465`).
- **F4. minga-ctl.** Context keys `client_cert` and `client_key`
  (`config.rs:32-54`); `service rollback` uses the server's revision when
  present, else the local file.
- Tests: role cases beside `bearer_auth_rejects_and_accepts`; TLS cases with
  the `Pki` helper from `minga-state/tests/raft_tls.rs`.

## Order

| Wave | Steps (parallel, separate files) |
| --- | --- |
| 0 | This plan; CHANGELOG fix (done in PR #33) |
| 1 | A, B, C, F1 |
| 2 | D (needs C), E (needs B), F2 (needs ADR 0005 accepted) |
| 3 | F3 (needs C, F2), F4 |

v1.0.3 ships after A and B. v1.1.0 ships after the rest, with the one-wipe
upgrade note. D, E and F2 each add a subcommand or flag in
`minga-cli/src/main.rs`; they merge in that order.

## Not in this plan

Staging verification (separate), multi-node, a matched-workload benchmark,
`minga-ctl` as a release asset, forced restarts and resource ownership labels
from section 2.3, certificate revocation, compliance assessment.

## Verification

- Every PR: `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`,
  `cargo test`; site lint when site data changes.
- A: the new root-only netns test in CI. B and E: the containerd CI suite
  (`MINGA_REQUIRE_CONTAINERD=1`), plus the local authenticated registry for E.
- C: fixture and marker tests; start a 1.0.2 data dir with the new binary and
  see the clear-wipe error.
- D: export from one test server, import into another, compare.
- F: role and TLS tests; `curl --cert` with a viewer certificate gets 403 on a
  POST and one audit line appears.
- A, E and F2 also need one run on a staging node before v1.1.0 is tagged.
