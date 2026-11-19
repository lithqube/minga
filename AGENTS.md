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

## Boundaries

- Keep the disk free. In a worktree, build with `CARGO_TARGET_DIR=<main checkout>/target`: a worktree's own `target/` grows to 20–50 GB. After every PR merge or completed Phase, immediately inventory and clean up that work: remove its merged local branch and owned worktree, task-created temp files, and confirmed task-created dangling Docker containers, networks, images, or volumes; clean task-owned Cargo `target/` artifacts when no active worktree depends on them. Verify merge/completion and ownership first. Never prune global Docker resources, shared Cargo caches/targets, locked or other-session worktrees, or worktrees/files with unrelated uncommitted changes. Preserve unsafe leftovers and report what remains and why; do not silently skip cleanup.

## Rust skills

- Build/QA/Review lanes: load `rust-best-practices` before writing Rust; `m06-error-handling` for `Result`/error types; `m07-concurrency` for async, tasks and channels; `m10-performance` on hot-path changes.
- Any `unsafe`, FFI or raw-syscall change (netns, WireGuard, containerd): run `unsafe-checker` and justify each `unsafe` block with a `// SAFETY:` comment.
- Use `rust-refactor-helper` for renames and signature changes across crates.

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
