# Minga

Rust single-binary orchestrator replacing Docker Swarm for edge/on-prem/air-gapped sites. Design doc: https://claude.ai/code/artifact/084e961f-88e4-45e2-aa1a-c8a76928b575

- Cargo workspace; every crate lives in `crates/`, shared dependency versions go in the root `[workspace.dependencies]`.
- Hot path (per packet, request or sandbox) stays in Rust. Never mix languages in one process through FFI; any Go component (e.g. embedded NATS) runs as a separate process behind a versioned gRPC contract. See `docs/adr/0001-language-split.md`.
- Only desired state and placement go through Raft. Heartbeats, task status and agent run steps go through JetStream.
- Compose files must deploy unchanged; unknown keys fail loudly instead of being skipped.
- Multi-tenancy is part of every change: tenants are untrusted, each owns its objects and nodes, every bus subject and key carries the tenant, and a reference across tenants returns 404. Every plan and PR that adds state, an endpoint, a bus subject or a node-local resource answers the tenancy checklist in `docs/adr/0008-multi-tenancy.md`.
- Before committing: `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.
- Keep the disk free. In a worktree, build with `CARGO_TARGET_DIR=<main checkout>/target`: a worktree's own `target/` grows to 20–50 GB. Once your PR is merged or abandoned, remove your worktree (`git worktree remove`), delete your local branch (`git branch -d`), and delete temp files you created outside the scratchpad (test containers, networks, downloads). Only remove what you created: never a worktree that is locked or belongs to another session.
