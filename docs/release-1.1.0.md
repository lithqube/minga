# Release notes: v1.1.0

The authoritative change list is the [changelog](../CHANGELOG.md); this page
records what v1.1.0 contains, what was checked, and how to upgrade, install
and publish it. The release workflow checks that the `v1.1.0` tag matches
`[workspace.package] version` in `Cargo.toml`.

## What is in it

v1.1.0 is the single-node part of the
[production-readiness plan](production-readiness-plan.md), plus fixes from
running v1.0.2 on a staging node.

| Area | Change | PR |
| --- | --- | --- |
| State | Versioned JSON state with a `format_version` marker in each redb file; data Minga cannot read is refused with the reason. Later upgrades that only add fields keep state | #38 |
| Backup | `minga state export` and `minga state import` over `GET`/`POST /v1.41/state`; see [backup.md](backup.md) | #40 |
| Images | Pulls through containerd's Transfer API on the configured socket, so the credential is never on a command line; Docker credential helpers (`credHelpers`, `credsStore`) are read; `minga image pull --containerd-socket` | #43 |
| Networking | Published ports redirect only traffic addressed to the node; host-local access and hairpin through the node's address work | #37 |
| Runtime | Supplementary groups from the image's `/etc/group`; a numeric uid gets its primary group; `extra_hosts` through the Docker API; cleanup after a failed pull | #39 |
| Failures | A failed task records its last error and exit code, returned by the Docker API and shown by `minga-ctl service ps`, `stack ps` and the new `service inspect` | #42 |
| `minga-ctl` | `remote_token_file` may be an env file; the macOS ssh notice is gone from `--debug`; `stack deploy` says where `minga` must be installed | #41 |
| Docs | [Production-readiness plan](production-readiness-plan.md), [ADR 0005](adr/0005-api-identity-and-roles.md) (Proposed), [backup guide](backup.md) | #34, #36, #40 |

## Upgrade notes

**One last wipe.** v1.1.0 replaces the bincode state of 1.0.2 and older with
versioned JSON, and it does not migrate old data. A node started on a 1.0.2
data directory stops with:

```text
this data directory was written by minga 1.0.2 or older and cannot be read: remove raft_log.redb and state.redb from --data-dir (not the whole directory) and redeploy
```

So, on every node:

1. Back up anything you need from the old cluster: v1.0.2 has no
   `minga state export`, so keep your compose files at hand.
2. Stop `minga manager`, `minga api` and workers.
3. In each role's `--data-dir` (for example `/var/lib/minga` and
   `/var/lib/minga-api`), remove only the two Raft files:

   ```bash
   sudo rm /var/lib/minga/raft_log.redb /var/lib/minga/state.redb
   ```

   Do not delete the whole directory. With the default paths it also holds
   secret values (`secrets/`, the default `--secret-store`), named volume
   data (`volumes/`) and agent modules (`modules/`). Removing `nats/` as well
   drops queued bus commands from the old version; it is optional, and they
   are dropped with a log line otherwise.
4. Install v1.1.0 on every node, start the roles, and redeploy your stacks.

Every node of a cluster must run the same version: a node refuses Raft
messages in another format, and two releases that share a format silently drop
the fields the older one does not know. From v1.1.0 on, an upgrade that only adds fields
keeps state.

Also check:

- **containerd 1.7 or newer** for private-registry pulls. On 1.6, anonymous
  pulls still work through `ctr`; pulls that need credentials fail with a
  message naming 1.7.
- **The daemon pulls.** Registry traffic now comes from containerd, so its
  proxy settings and CA trust apply, not those of the `minga` process. A
  plain-HTTP registry cannot be reached on containerd 1.7; serve TLS with a
  CA the node trusts.
- **Credential helpers.** If `config.json` names a `credsStore` or
  `credHelpers` entry, the helper must be on the manager's `PATH`. A missing
  helper is an error; the helper's own "credentials not found" answer means
  an anonymous pull.
- **Numeric users.** A bare numeric `user:` that the image's `/etc/passwd`
  lists now runs with that user's primary group instead of gid 0.

## `minga-ctl`

`minga-ctl` runs on a workstation and reaches nodes over SSH. It is not in
the release archive. Build it from a checkout of the tag:

```bash
git checkout v1.1.0
cargo install --locked --path crates/minga-ctl
minga-ctl --version   # minga-ctl 1.1.0
```

`minga-ctl stack deploy` runs `minga deploy` on the workstation through the
tunnel, so the workstation needs the `minga` binary too. On Linux it is in
the release archive; on macOS build it with
`cargo install --locked --path crates/minga-cli`.

## Verification status

| Check | Status |
| --- | --- |
| `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings` | Pass |
| `cargo test` (workspace) | Pass, on macOS and in CI |
| containerd suites in CI (`MINGA_REQUIRE_CONTAINERD=1`), including a credentialed pull from a local TLS `registry:2` | Pass |
| Published-port paths on a real kernel (CI `netns` job: host to 127.0.0.1, host to the node address, task to itself and a sibling through the node address, no redirect of non-local traffic) | Pass |
| Transfer pull on containerd 1.7.27 and 2.4.2; `ctr` fallback on 1.6.38 | Pass, in a test rig, not in CI for 2.x or 1.6 |
| Format marker and fixtures (`crates/minga-state/tests/format.rs`) | Pass |
| Export, import into a fresh API server, scheduler re-places the tasks (`crates/minga-cli/tests/state_e2e.rs`) | Pass |
| The staging stack on a real node with this build | Not run yet |
| Restore on a real fresh host | Not run |
| A real credential helper (osxkeychain, ecr-login, pass) or a hosted private registry (GHCR, ECR) | Not run; stub helpers and a local registry only |
| Start failures of real containers recording their reason | Not run; mock runtime only |
| Talos install with this build | Not run |

## Publishing

Tag only after the staging run. Pushing the tag runs the release workflow.
It builds `x86_64`/`aarch64` archives (with `nats-server` and `ctr`), pushes
`ghcr.io/lithqube/minga:1.1.0`, `ghcr.io/lithqube/minga:1.1` and
`ghcr.io/lithqube/minga:latest`, and creates a GitHub release with
`SHA256SUMS` and `install.sh`.

```bash
git tag -a v1.1.0 -m "v1.1.0"
git push origin v1.1.0
```

## Installing

```bash
curl -fsSL https://github.com/lithqube/minga/releases/download/v1.1.0/install.sh \
  | sh -s -- --version v1.1.0
minga --version   # 1.1.0
```

`install.sh` defaults to `latest`, which resolves to this release once it is
published.
