# Release notes: v1.2.0

The authoritative change list is the [changelog](../CHANGELOG.md); this page
records what v1.2.0 contains, what was checked, and how to upgrade, install
and publish it. The release workflow checks that the `v1.2.0` tag matches
`[workspace.package] version` in `Cargo.toml`.

## What is in it

v1.2.0 fixes the twelve P1 leads of a source-only security audit of 1.1.1,
plus the lower-priority leads that share their root causes. It also carries
the 1.1.1 bind-mount fix, since 1.1.1 was not tagged.

| Area | Change | PR |
| --- | --- | --- |
| Files | TLS keys, `--data-dir`, the redb files, the NATS sidecar config and `minga.env` are owner-only; a group- or world-readable Raft key is refused | #48 |
| Token | `minga deploy` and `minga state` ignore proxy variables and refuse redirects; `minga-ctl` strips proxy variables from the deploy it runs and refuses an ssh `stack deploy` without a token | #49 |
| Networking | Tasks cannot reach the node's loopback services or another stack's bridge; a published port cannot take a reserved or listening node port | #50 |
| Runtime | Docker's default masked and read-only paths; the deny-all device rule is always set | #51 |
| Release | Actions pinned to commit SHAs, per-job permissions, no persisted checkout credentials, pinned and verified rustup | #52 |
| Listeners | Header-read timeout and connection cap on the API, metrics and egress listeners; handshake and read timeouts and a connection cap on Raft RPC; a tokenless API refuses browser-shaped requests; warnings for plaintext Raft and a tokenless API off loopback | #53 |
| Supervisor | A bus Assign starts only what Raft assigns, from the Raft spec; containers left over from before a restart are adopted or stopped | #54 |
| Supervisor | Exited leftover containers are removed at start too, with their network, DNAT rules and staged secrets; docs no longer overstate 1.0.2's cleanup; Raft TLS is documented as required off loopback | #56 |

## Upgrade notes

**State is compatible with 1.1.0: no wipe or migration.** The persisted format
is unchanged (format 2), and the frozen fixtures in
`crates/minga-state/tests/fixtures/` are the same as in 1.1.0, so a 1.1.0 data
directory opens as is. Upgrade every node together, as before. Check these
first:

- **Raft TLS key mode.** A key file passed to `--raft-tls-key` that is
  readable by group or others is refused; `chmod 600` it. Keys written by
  `minga tls` from this version on are 0600.
- **Data directories are tightened.** At start, `--data-dir` becomes 0700
  and `raft_log.redb` and `state.redb` 0600. Anything else that reads them
  must run as the same user.
- **Published ports.** A task that publishes a port the node reserves (53,
  the Raft, metrics and egress ports, the NATS sidecar's 4222 and 8222) or
  that a host process already listens on fails to start with
  `port N is in use on this node`, as Docker does. Move the published port.
- **Tasks and the node's loopback.** A task can no longer reach services the
  node binds on `127.0.0.1` through its bridge, and two stacks' bridges can
  no longer reach each other. A task that relied on either must use a
  published port on the node address.
- **Tokenless API.** Without `MINGA_API_TOKEN`, a request that carries an
  `Origin` header, or whose `Host` is a DNS name other than `localhost` or
  `*.localhost`, gets 403. This stops a web page in a local browser from
  driving a loopback-only API through DNS rebinding. It does not authenticate
  anyone: an API reachable off loopback needs a token. Traefik sends the
  token as its Basic-auth password (see the
  [staging guide](hetzner-arm-staging.md#traefik-as-reverse-proxy)).
- **`minga-ctl stack deploy` over ssh** needs a token in the context
  (`remote_token_file`), because the relay would otherwise expose a tokenless
  API to other users of the workstation.
- **`/proc` inside containers.** `/proc/kcore`, `/proc/keys`, `/proc/acpi`,
  `/proc/timer_list`, `/sys/firmware` and the rest of Docker's list are
  masked; `/proc/sys`, `/proc/fs`, `/proc/irq`, `/proc/bus` and
  `/proc/sysrq-trigger` are read-only. A workload that writes sysctls from
  inside its container now fails.
- **Warnings.** A Raft bind off loopback without `--raft-tls-*`, and an API
  bind off loopback without a token, log a warning at start.

Known risk: Raft without TLS off loopback still starts. Anyone who can reach
the Raft port can then write cluster state. Use `--raft-tls-*` on every
multi-node cluster.

## `minga-ctl`

`minga-ctl` is not in the release archive. Build it from a checkout of the
tag:

```bash
git checkout v1.2.0
cargo install --locked --path crates/minga-ctl
minga-ctl --version   # minga-ctl 1.2.0
```

## Verification status

| Check | Status |
| --- | --- |
| `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings` | Pass, in CI on each PR |
| `cargo test` (workspace) | Pass, in CI on each PR |
| containerd suites, including masked `/proc/kcore` and read-only `/proc/sys` inside a container | Pass, in CI on #51 |
| Real-kernel `netns` job: published port through the node address | Pass, in CI |
| Real-kernel `netns` job: cross-bridge drop, published 53/udp refused | Pass, in CI on #50 |
| All of the above on merged `main` (51ae4d3, with #48–#56) | Pass, in CI |
| Release workflow with pinned actions and verified rustup | Pass on `v1.2.0-rc.1` (run 43302593658): all four jobs passed; the build jobs ran with `contents: read`, the image job with `contents: read` and `packages: write`, Publish with `contents: write`; only `ghcr.io/lithqube/minga:1.2.0-rc.1` was pushed. The `v1.2.0` run is the first to push `:1.2` and `:latest` |
| `v1.2.0-rc.1` on the staging VPS: upgrade from 1.1.0 and redeploy of an 11-service stack | Ran, as reported in the [benchmark update](benchmark.md#update-v120-rc1-benchmark-2026-11-12); this is not a check of the audit leads |
| The audit's owner-observed checks for each lead on a staging node, before and after | Not run |
| A scoped re-audit of the changed units against this commit | Not run |

## Release candidate

`v1.2.0-rc.1` was tagged on 2026-11-12 from the `release/1.2.0` branch, which
was cut from `main` at 51ae4d3 (#48–#56) with the version set to `1.2.0-rc.1`.
The release workflow published it as a GitHub prerelease and pushed only
`ghcr.io/lithqube/minga:1.2.0-rc.1`, so `install.sh` without `--version` kept
installing 1.1.0. Documentation merged on `main` afterwards (#57–#59) came in
through #60. Release fixes land on `release/1.2.0` through pull requests, and
the branch is merged back into `main` after the release.

## Publishing

Once a candidate passes staging, set `version = "1.2.0"` in `Cargo.toml` on
`release/1.2.0` (through a pull request), then tag that commit. Pushing the
tag builds the archives, pushes `ghcr.io/lithqube/minga:1.2.0`, `:1.2` and
`:latest`, and creates the GitHub release. Merge `release/1.2.0` back into
`main`, then set `releasePublished` to `true` in `site/src/config/site.ts`.

Tag the release branch commit, not whatever is checked out:

```bash
git fetch origin
grep -m1 '^version' <(git show origin/release/1.2.0:Cargo.toml)   # version = "1.2.0"
git tag -a v1.2.0 -m "v1.2.0" origin/release/1.2.0
git push origin v1.2.0
```

## Installing

```bash
curl -fsSL https://github.com/lithqube/minga/releases/download/v1.2.0/install.sh \
  | sh -s -- --version v1.2.0
minga --version   # 1.2.0
```
