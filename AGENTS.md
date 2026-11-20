# Minga

Rust single-binary orchestrator replacing Docker Swarm for edge/on-prem/air-gapped sites. Design doc: https://claude.ai/code/artifact/084e961f-88e4-45e2-aa1a-c8a76928b575

## Commands

- Before committing: `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.

## Where things live

- Cargo workspace; every crate lives in `crates/`, shared dependency versions go in the root `[workspace.dependencies]`.

## Conventions

- Hot path (per packet, request or sandbox) stays in Rust. Never mix languages in one process through FFI; any Go component (e.g. embedded NATS) runs as a separate process behind a versioned gRPC contract. See `docs/adr/0001-language-split.md`.
- Only desired state and placement go through Raft. Heartbeats, task status and agent run steps go through JetStream.
- Compose files must deploy unchanged; unknown keys fail loudly instead of being skipped.
- Multi-tenancy is part of every change: tenants are untrusted, each owns its objects and nodes, every bus subject and key carries the tenant, and a reference across tenants returns 404. Every plan and PR that adds state, an endpoint, a bus subject or a node-local resource answers the tenancy checklist in `docs/adr/0008-multi-tenancy.md`.
- Commits and PR titles use `type(scope): subject`, for example `feat(agent): Phase 16 agent-ready operations (#114)`. This overrides the emoji format in the super-board writing table below.

## Boundaries

- Keep the disk free. In a worktree, build with `CARGO_TARGET_DIR=<main checkout>/target`: a worktree's own `target/` grows to 20–50 GB. After every PR merge or completed Phase, immediately inventory and clean up that work: remove its merged local branch and owned worktree, task-created temp files, and confirmed task-created dangling Docker containers, networks, images, or volumes; clean task-owned Cargo `target/` artifacts when no active worktree depends on them. Verify merge/completion and ownership first. Never prune global Docker resources, shared Cargo caches/targets, locked or other-session worktrees, or worktrees/files with unrelated uncommitted changes. Preserve unsafe leftovers and report what remains and why; do not silently skip cleanup.
- Parallel board lanes share one target: every lane builds with `CARGO_TARGET_DIR=<main checkout>/target`, so cargo's build lock queues same-workspace compiles and artifacts dedupe. Lanes set `CARGO_INCREMENTAL=0` so the incremental cache does not grow under parallelism. Run read-only lanes (pre-flight, review, QA analysis) in parallel rather than several builders. Test only the crates a lane touches (`cargo test -p <crate>`); run `cargo test --workspace` only at the merge gate.

## Rust skills

- Build/QA/Review lanes: load `rust-best-practices` before writing Rust; `m06-error-handling` for `Result`/error types; `m07-concurrency` for async, tasks and channels; `m10-performance` on hot-path changes.
- Any `unsafe`, FFI or raw-syscall change (netns, WireGuard, containerd): run `unsafe-checker` and justify each `unsafe` block with a `// SAFETY:` comment.
- Use `rust-refactor-helper` for renames and signature changes across crates.

## Agent ownership

Two agents share the board. The OMP harness drains it with `/super-board run`. Claude Code plans, audits security and takes the fixes that need a design call. An issue's owner label says which agent may work it.

| Label | Who works it | The other agent |
|---|---|---|
| `owner:omp` | OMP, from Ready, through the board lanes | NEVER builds, QAs, reviews or pushes to it. May comment a finding or file a new issue. |
| `owner:claude-code` | Claude Code, on its own branch and PR | NEVER claims, moves, builds or comments on it. A wave skips it, even in Ready. |

- Read the owner label before you touch an issue, its board card or its PR. No owner label: don't start it; ask the user.
- Only the user changes an owner label. If an issue needs the other agent, comment why on it and leave the label.
- Whoever files an issue sets its owner label:
  - `owner:omp` for a narrow fix in one area with clear acceptance criteria.
  - `owner:claude-code` for a design call, Raft or bus semantics, a state migration, a policy decision or a change across several crates.
- One orchestrator per board. Claude Code runs `/super-board run` only when the user asks, and only while no OMP wave is active. Check `.claude/super-board/inflight/workflow-wave.lock` and live lanes first.
- NEVER clear `.claude/super-board/github-halt.json` set by the other agent without the user. It pauses both agents.

<!-- super-board:begin v3.1.2 (managed; edits inside are overwritten on upgrade) -->
## Super Board

Board: Backlog · Ready · Building · QA · Review · Blocked · Done. Labels route: `qa` skips Building · `bug`, `feature`, none → built first.

| When | Use | NEVER |
|---|---|---|
| Set up / repair the board | `/super-board onboard` | DON'T hand-edit config mid-run |
| Check tickets before a run | `/super-board lint` | NEVER run with AC-less tickets |
| Drain the board | `/super-board run` | NEVER build, test or merge from the orchestrator |
| Board state / halt | `/super-board status` · `/super-board stop` | |
| Build one ticket | `/super-build` | NEVER build outside the card's worktree |
| QA a branch, or a live URL alone | `/super-qa` · `/super-qa <url>` | NEVER mark QA pass without evidence |
| Review a PR, merge | `/super-review` | NEVER `gh pr merge` direct — merge gate only |
| File bugs from Sentry, PostHog, PRs | `/super-collect` | DON'T file without a verifier pass |
| UI polish | `/ui-refine-loop` (human runs it) | board NEVER runs it |
| Diagram / explainer page | `/visual` | |
| Commit, pull, push this branch | `/git-sync` | NEVER force push or rebase shared history |

| Rule | Value |
|---|---|
| Config | `.claude/super-board/configs/<slug>.json` |
| Isolation | 1 card · 1 worktree · 1 branch |
| Merge | auto for normal changes · money, auth, destructive schema (DROP/TRUNCATE/RENAME) → human · PR > 400 changed lines → human |
| Migrations | additive → robot migrates allowed DBs only · live DB ALWAYS → 🙋 needs you |
| Blocked card | block-template comment · last line `blocked-by:` · 🙋 = needs you |
| Secrets | NEVER read `.env`; key names via `.claude/bin/super-board-env-check.sh` |

## Writing (super-board)

| Thing | Format → `.claude/skills/super-board/references/writing-standard.md` |
|---|---|
| Commit, PR title | `<emoji> [type] scope: subject` + short bullets · ✨ feat 🐛 fix 🔧 chore ♻️ refactor 🧪 test 📝 docs |
| PR body | blocks: status · Problem · Solution · AC + proof · history · Before\|After · Risk — `super-board-pr-body.sh` |
| Ticket | Problem · Context · Fix · AC · Risk · Blocked by → `docs/agents/issue-tracker.md` |
| Comment | `[role] [label] status` · Did · ✅ Done · ❌ Not done · Next · ≤ 8 lines |
| PR author notes | file + critical inline review comments: Purpose · What changed · Why it matters → `pr-author-notes.md` |

- NEVER chain steps with arrows. One step per line, lettered under Where.
- NEVER link screenshots. Embed a raw URL pinned to a sha.
- DON'T repeat file lists in status comments. DON'T write "Not verified" or "Next" in a PR body.
<!-- super-board:end -->
