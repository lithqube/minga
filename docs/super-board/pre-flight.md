# Minga super-board pre-flight

Environment facts the board's lanes depend on. Re-verified 2026-11-25 (lint of the rc.2 audit cards #234 to #276).

- [x] `gh` CLI authenticated as `lithqube` (keyring). Token scopes: `admin:org`, `admin:public_repo`, `gist`, `project`, `repo`, `workflow` — `project` and `repo` are the ones the board needs.
- [x] `cargo` 1.99.0 on PATH for `cargo fmt`, `cargo clippy`, `cargo test`.
- [x] `node` v26.13.1 on PATH (workflow syntax check).
- [x] `python3` 3.15.2 (`.claude/bin` helpers).
- [x] Workers run on macOS as a non-root user (uid 502). Linux-only `nets` tests cannot run in the worker's shell; they skip gracefully unless `MINGA_REQUIRE_NETNS=1` is set. CI runs them in the `nets` job (`ubuntu-24.04`, `sudo -E`, `MINGA_REQUIRE_NETNS=1`, `cargo test -p minga-node --test netns --test wireguard_netns`), so that proof comes from CI on the PR, not from the worker's shell.
- [x] `containerd`-backed tests in `crates/minga-node/tests/containerd.rs` skip gracefully when the daemon socket is absent; CI provides the daemon. A containerd-backed acceptance criterion is proven by the CI run on the PR.
- [x] `merge_policy.default` is `human` — every PR waits for a person to approve and merge.
- [x] Each lane builds in its own worktree's `target/` with `CARGO_TARGET_DIR` unset and `CARGO_INCREMENTAL=0`; two worktrees never share a target dir (AGENTS.md, Boundaries).
- [ ] Disk headroom: 14 GB free on 2026-11-25, under the 30 GB the verify commands require, so every build lane refuses to start. Free at least 16 GB before `super-board run`.
- [x] `paths.project_md` is unset, so no card depends on a gitignored project brief.

## rc.2 audit cards

- [x] The repository is public, so the build provenance attestation in #248 works without GitHub Enterprise. Its workflow needs `id-token: write` and `attestations: write` on the release job only.
- [x] `cargo deny`, `cosign` and `syft` are not installed locally. #250 and #248 install them inside GitHub Actions, so their proof is the CI run on the PR.
- [x] Kernel, iptables and netns cards (#247, #258, #260) are proven by the CI `nets` job, as above.
- [x] #249 and #267 change repository settings (branch protection, required checks, Dependabot security updates, private vulnerability reporting). A repository admin applies them, not a lane; they carry `needs-you` and park in Blocked 🙋 at that step.
- [x] #268 runs the 1.3.0 gates on real machines and is owner:claude-code, so an OMP wave skips it. It needs `ssh soultech-staging` (reachable 2026-11-25, minga 1.3.0-rc.2 active), OrbStack (`orb` on PATH) for a three-manager, two-tenant cluster, and a 1.2.0 data copy to upgrade.
