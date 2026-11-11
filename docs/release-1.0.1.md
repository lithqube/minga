# Release notes: v1.0.1

Release contents prepared from `main`. The authoritative change list is the
[changelog](../CHANGELOG.md); this page records what v1.0.1 contains, what was
checked, and how to install and publish it. The release is not published until
the `v1.0.1` tag is pushed.

## Version numbering

`0.1.0` was never tagged or published. Version `1.0.1` is the promoted stable
version, skipping `0.x` with no `1.0.0`. It contains the full `0.1.0`
changelog entry plus the changes below. The release workflow checks that a
`v1.0.1` tag matches `[workspace.package] version` in `Cargo.toml`.

## What is in it

Beyond the `0.1.0` entry (PRs #10 and #11):

| Area | Change |
| --- | --- |
| Updates | Rolling updates honoring `update_config`; task IDs carry a spec revision; stop-first for published ports |
| Rollback | `failure_action: rollback` restores the previous spec; `rollback_config` is read from Compose and the Docker API |
| Health | Healthchecks run in the container; a task stays `Starting` and out of DNS until the first probe passes |
| Images | Missing images are pulled with `ctr` (300s pull, 120s unpack); `minga image pull <ref>` |
| Placement | `node.role` constraints; a service no longer evicts itself from a full node |
| Networking | `MASQUERADE` per network so containers reach off-bridge hosts |
| Restart policy | Backoff, `max_attempts` and `condition: none` are honored; crashed tasks end `Failed` or `Stopped` |
| Secrets | `minga secret put` (stdin) and `talos/provision-secret.yaml` |
| Compose | Numeric `user` reaches the container; non-numeric is rejected at translate time |
| Release tooling | Stable tags publish a GitHub release and update the `latest` image tag |

Not included in v1.0.1: a named image `USER` (anything but numeric `uid[:gid]`),
a cross-node WireGuard mesh, joining a running cluster, and private-registry
credentials for image pull.

## Upgrade notes

State from any earlier build does not decode (bincode layouts changed). Clear
`--data-dir` and redeploy. Upgrade every node together.

## Verification status

The following checks were reported for the release candidate before version
promotion. No new verification is implied by this version bump:

| Check | Status |
| --- | --- |
| `cargo fmt --all --check` | Pass |
| `cargo clippy --all-targets -- -D warnings` | Pass |
| `cargo test` (workspace, with `nats-server`) | Pass, 0 failures |
| containerd suites on Linux (`MINGA_REQUIRE_CONTAINERD=1`) | Not run |
| Image pull against a real registry | Not run |
| Talos install with this build | Not run |
| Release workflow on a tag | Not run; publishing is triggered by pushing `v1.0.1` |

## Publishing

The release workflow runs when the stable tag is pushed. It builds
`x86_64`/`aarch64` archives, pushes `ghcr.io/lithqube/minga:1.0.1`,
`ghcr.io/lithqube/minga:1.0`, and `ghcr.io/lithqube/minga:latest`, and creates
a GitHub release with `SHA256SUMS` and `install.sh`. Run the workflow's
`workflow_dispatch` first only if updating the `edge` image is intended.

```bash
git tag -a v1.0.1 -m "v1.0.1"
git push origin v1.0.1
```

## Installing

`install.sh` defaults to `latest`, which resolves to this stable release after
the tag has been published. To install this version explicitly:

```bash
curl -fsSL https://github.com/lithqube/minga/releases/download/v1.0.1/install.sh \
  | sh -s -- --version v1.0.1
minga --version   # 1.0.1
```

To install the published latest stable version instead:

```bash
curl -fsSL https://github.com/lithqube/minga/releases/latest/download/install.sh | sh
```

The repository is versioned `1.0.1` now. The GitHub release, downloadable
archives, and `latest` image are not available until `v1.0.1` is pushed.
