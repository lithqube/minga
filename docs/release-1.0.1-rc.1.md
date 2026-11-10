# Release notes: v1.0.1-rc.1

Release candidate, cut 2026-11-10 from `origin/main` (merge of PR #11). The
authoritative change list is the [changelog](../CHANGELOG.md); this page says
what the candidate contains, how it differs from `0.1.0`, what was checked,
and how to cut and test it.

## Version numbering

`0.1.0` was never tagged or published: the repository has no tags and no
GitHub releases. This candidate is numbered `1.0.1-rc.1` by maintainer
decision, so it skips `0.x` and there is no `1.0.0`. It contains the whole
`0.1.0` changelog entry plus the changes below. If the number should differ,
change `[workspace.package] version` in `Cargo.toml`, the changelog heading
and this file's name together; the release workflow refuses a tag that does
not match `Cargo.toml`.

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
| Release tooling | Pre-release tags (any tag containing `-`) publish a GitHub pre-release and do not move `latest` |

Not in the candidate: a named image `USER` (commit `c50f993` on
`fix/secret-owner-and-named-users`, unmerged), a cross-node WireGuard mesh,
joining a running cluster, private-registry credentials for image pull.

## Upgrading

State from any earlier build does not decode (bincode layouts changed). Clear
`--data-dir` and redeploy. Upgrade every node together.

## Verification status

Run on macOS (the containerd suites skip without a runtime) on the release
branch, with the version bump applied:

| Check | Status |
| --- | --- |
| `cargo fmt --all --check` | Pass |
| `cargo clippy --all-targets -- -D warnings` | Pass |
| `cargo test` (workspace, with `nats-server`) | Pass, 0 failures |
| containerd suites on Linux (`MINGA_REQUIRE_CONTAINERD=1`) | Not run |
| Image pull against a real registry | Not run |
| Talos install with this build | Not run |
| Release workflow on a tag | Not run; the pre-release edits to `release.yml` are untested. Run a `workflow_dispatch` dry run first |

## Cutting the candidate

1. Merge this branch (`release/v1.0.1-rc.1`) to `main`.
2. Optional dry run: run the **Release** workflow manually
   (`workflow_dispatch`). It builds both archives and pushes only
   `ghcr.io/lithqube/minga:edge`.
3. Tag the merge commit:

   ```bash
   git tag -a v1.0.1-rc.1 -m "v1.0.1-rc.1"
   git push origin v1.0.1-rc.1
   ```

4. The workflow checks the tag against `Cargo.toml`, builds
   `x86_64`/`aarch64` archives, pushes `ghcr.io/lithqube/minga:1.0.1-rc.1`
   (no `latest`, no `1.0` tag), and publishes a GitHub **pre-release** with
   `SHA256SUMS` and `install.sh`.

## Testing the candidate

`install.sh` defaults to `latest`, which skips pre-releases, so name the tag:

```bash
curl -fsSL https://github.com/lithqube/minga/releases/download/v1.0.1-rc.1/install.sh \
  | sh -s -- --version v1.0.1-rc.1
minga --version   # 1.0.1-rc.1
```

Exercise what changed: deploy a stack, change an image tag and watch the
rolling update, break the new image to see the rollback, add a healthcheck
that fails, and deploy an image that is not on the node. Report problems
against the changelog entry they relate to.

## Promoting to 1.0.1

Bump `version` to `1.0.1`, rename the changelog heading to
`## [1.0.1] — <date>`, leave `## [Unreleased]` above it, update README
status, and tag `v1.0.1`. The site reads the latest release from the first
`## [x.y.z] — date` heading, so it shows `0.1.0` until then.
