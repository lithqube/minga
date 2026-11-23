# Minga super-board pre-flight

Environment facts the board's lanes depend on. Re-verified 2026-11-23.

- [x] `gh` CLI authenticated as `lithqube` (keyring). Token scopes: `admin:org`, `admin:public_repo`, `gist`, `project`, `repo`, `workflow` — `project` and `repo` are the ones the board needs.
- [x] `cargo` 1.99.0 on PATH for `cargo fmt`, `cargo clippy`, `cargo test`.
- [x] `node` v26.13.1 on PATH (workflow syntax check).
- [x] `python3` 3.15.2 (`.claude/bin` helpers).
- [x] Workers run on macOS as a non-root user (uid 502). Linux-only `nets` tests cannot run in the worker's shell; they skip gracefully unless `MINGA_REQUIRE_NETNS=1` is set. CI runs them in the `nets` job (`ubuntu-24.04`, `sudo -E`, `MINGA_REQUIRE_NETNS=1`, `cargo test -p minga-node --test netns --test wireguard_netns`), so that proof comes from CI on the PR, not from the worker's shell.
- [x] `containerd`-backed tests in `crates/minga-node/tests/containerd.rs` skip gracefully when the daemon socket is absent; CI provides the daemon. A containerd-backed acceptance criterion is proven by the CI run on the PR.
- [x] `merge_policy.default` is `human` — every PR waits for a person to approve and merge.
- [x] Lanes end in a worktree with `CARGO_TARGET_DIR=<main checkout>/target` (AGENTS.md, Boundaries). Lanes set `CARGO_INCREMENTAL=0` so the incremental cache does not grow under parallelism.
- [x] Disk headroom: 127 GB free on the volume holding the repository; the board's verify commands refuse to build below 30 GB.
- [x] `paths.project_md` is unset, so no card depends on a gitignored project brief.
