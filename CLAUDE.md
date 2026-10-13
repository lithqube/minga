# Swarm-NG

Rust single-binary orchestrator replacing Docker Swarm for edge/on-prem/air-gapped sites. Design doc: https://claude.ai/code/artifact/084e961f-88e4-45e2-aa1a-c8a76928b575

- Cargo workspace; every crate lives in `crates/`, shared dependency versions go in the root `[workspace.dependencies]`.
- Hot path (per packet, request or sandbox) stays in Rust. Never mix languages in one process through FFI; any Go component (e.g. embedded NATS) runs as a separate process behind a versioned gRPC contract. See `docs/adr/0001-language-split.md`.
- Only desired state and placement go through Raft. Heartbeats, task status and agent run steps go through JetStream.
- Compose files must deploy unchanged; unknown keys fail loudly instead of being skipped.
- Before committing: `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`, `cargo test`.
