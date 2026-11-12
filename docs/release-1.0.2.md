# Release notes: v1.0.2

The authoritative change list is the [changelog](../CHANGELOG.md); this page
records what v1.0.2 contains, what was checked, and how to upgrade, install
and publish it. The release workflow checks that the `v1.0.2` tag matches
`[workspace.package] version` in `Cargo.toml`.

## What is in it

Most of it came from deploying a multi-service stack on a Hetzner CAX21 node
([staging guide](hetzner-arm-staging.md)) and from outside feedback on v1.0.1.

| Area | Change |
| --- | --- |
| Compose | `env_file`; `extra_hosts` with `host-gateway` resolved on the node to the task's bridge address; relative bind-mount sources joined to the compose file's directory; a string `command`/`entrypoint` split with shell quoting |
| Users | `user:` and an image's `USER` may be `name[:group]`, resolved from the image's own `/etc/passwd` and `/etc/group` at task start |
| Images | Private-registry pulls from inline `auth` entries in `$DOCKER_CONFIG/config.json`, else `~/.docker/config.json`, for automatic pulls and `minga image pull`; Docker Hub's `https://index.docker.io/v1/` key matches; no more `ctr images unpack`, which containerd 2.2 removed |
| API | The API token is accepted as the HTTP Basic-auth password, so Traefik's Swarm provider can authenticate |
| Supervisor | Stopped containerd tasks are purged at start, so a restart no longer needs `ctr tasks delete`. Running leftovers, and the network and secrets of exited ones, stayed until 1.2.0 |
| `minga-ctl` | New companion CLI: contexts, stacks, services (scale, update, rollback, logs), secrets, images, health, metrics and registry login, over the system `ssh` client |

## Upgrade notes

`Service` and `TaskSpec` gained `extra_hosts`, and Raft state is bincode, so
state written by 1.0.1 does not decode. Clear `--data-dir` on every node and
redeploy, upgrading every node together.

Private-registry pulls need a credential file the manager can read. The
[staging guide](hetzner-arm-staging.md#private-ghcr-images) writes
`/etc/minga/docker/config.json` and sets `DOCKER_CONFIG` in `minga.env`.
`minga-ctl registry login` can maintain that file; point the context's
`registry_auth_file` at it.

## `minga-ctl`

`minga-ctl` runs on a workstation and reaches nodes over SSH. It is not in
the release archive, which holds the node binaries. Build it from a checkout
of the tag:

```bash
git checkout v1.0.2
cargo install --locked --path crates/minga-ctl
minga-ctl --version   # minga-ctl 1.0.2
```

Node setup, the trust model and every command are in
[the minga-ctl plan](minga-ctl-plan.md). Phase 2 (client certificates, roles,
server-side audit and revisions) is not started.

## Verification status

| Check | Status |
| --- | --- |
| `cargo fmt --all --check` | Pass |
| `cargo clippy --all-targets -- -D warnings` | Pass |
| `cargo test` (workspace) | Pass, locally on macOS and in CI |
| containerd suites in CI (`MINGA_REQUIRE_CONTAINERD=1`, `minga-node`) | Pass |
| The 8-service staging stack on a Hetzner CAX21 node | Pass on `feat/staging-compat`; not re-run after the later fixes |
| Image pull against a real private registry | Not run |
| Talos install with this build | Not run |
| `minga-ctl` against a real node over SSH | Not run; tested against stub `ssh`/`minga` binaries and test API servers |

## Publishing

Pushing the tag runs the release workflow. It builds `x86_64`/`aarch64`
archives (with `nats-server` and `ctr`), pushes
`ghcr.io/lithqube/minga:1.0.2`, `ghcr.io/lithqube/minga:1.0` and
`ghcr.io/lithqube/minga:latest`, and creates a GitHub release with
`SHA256SUMS` and `install.sh`.

```bash
git tag -a v1.0.2 -m "v1.0.2"
git push origin v1.0.2
```

## Installing

```bash
curl -fsSL https://github.com/lithqube/minga/releases/download/v1.0.2/install.sh \
  | sh -s -- --version v1.0.2
minga --version   # 1.0.2
```

`install.sh` defaults to `latest`, which resolves to this release once it is
published.
