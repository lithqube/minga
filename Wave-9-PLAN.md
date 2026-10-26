# Wave 9 Plan: Observability & Production Hardening

## Scope
Wave 9 addresses the two highest-impact gaps from Wave 8:
1. **Observability (ADR 0004)** — Prometheus metrics, health endpoints, structured logging
2. **Real container log capture** — FIFO readers / `file://` paths for containerd stdout/stderr

## Deliverables

### 1. ADR 0004: Observability
- **Decision record** at `docs/adr/0004-observability.md`
- **Metrics crate** (`minga-telemetry`) exposing:
  - Prometheus `/metrics` endpoint (via `axum-prometheus` or similar)
  - Key metrics: task starts/stops/restarts, scheduler decisions, Raft leadership, bus latency, agent executor pool stats
  - Node health endpoint (`/healthz`, `/readyz`)
- **Structured logging** — `tracing` + `tracing-subscriber` with JSON output, correlation IDs via bus message headers

### 2. Real Container Log Capture
- **Containerd runtime** (`minga-node/src/containerd/runtime.rs`):
  - Implement `create_task` with `file:///var/lib/minga/logs/{task_id}.log` for stdout/stderr (merged stream) or separate FIFOs for split streams
  - `Runtime::logs(container_id, tail)` reads the log file(s) and returns last N lines
  - Cleanup log files on task removal
- **MockRuntime** — in-memory buffer for tests (already exists, verify compatibility)
- **Supervisor integration** — ensure log paths are created before task start, cleaned up on stop/restart

### 3. (Deferred) Production Hardening
- TLS for NATS/gRPC — tracked as separate issue
- Secret management — tracked as separate issue
- Rolling upgrades — tracked as separate issue

### 4. (Deferred) Agent Executor Hardening
- Real WASM failure/restart cycles — tracked as separate issue
- Fuel/epoch limits — tracked as separate issue

## Acceptance Criteria

| ID | Criterion | Verification |
|----|-----------|--------------|
| OBS-1 | ADR 0004 authored and committed | `cat docs/adr/0004-observability.md` |
| OBS-2 | `/metrics` endpoint returns Prometheus format | `curl localhost:PORT/metrics` shows `minga_` prefixed metrics |
| OBS-3 | `/healthz` returns 200 OK when node healthy | `curl localhost:PORT/healthz` |
| OBS-4 | `/readyz` returns 200 OK when node ready (Raft caught up, bus connected) | `curl localhost:PORT/readyz` |
| OBS-5 | Structured JSON logs emitted with correlation IDs | `grep correlation_id` in logs |
| LOG-1 | Container task stdout/stderr written to `file://` path | Start container, check log file exists with content |
| LOG-2 | `Runtime::logs(container_id, N)` returns last N lines | Unit test + integration test |
| LOG-3 | Log files cleaned up when task removed | Stop task, verify log file removed |
| LOG-4 | Restart preserves log history (new task gets new log file) | Restart task, verify old log retained, new log created |

## Implementation Order

1. **ADR 0004** — document decisions first
2. **minga-telemetry crate** — new crate with metrics + health endpoints
3. **Wire telemetry into minga-node** — supervisor, runtime, bus
4. **Containerd log capture** — implement `file://` stdout/stderr in runtime
5. **Tests** — unit + integration for metrics and log capture
6. **Quality gate** — `cargo fmt --all && cargo clippy --all-targets -- -D warnings && cargo test`

## Dependencies
- `axum` + `axum-prometheus` (or `prometheus` + custom handler)
- `tracing`, `tracing-subscriber`, `tracing-appender`
- Existing: `minga-bus`, `minga-node`, `minga-scheduler`

## Risks
- macOS containerd cannot run containers → log capture integration test requires Linux CI or Lima VM
- Prometheus metrics cardinality — avoid unbounded labels (use task_id hash or bucket)
- Log file rotation — initial implementation uses single file per task; rotation is follow-up

## Out of Scope
- Distributed tracing (OpenTelemetry) — future wave
- Log aggregation/shipper (Vector, Fluent Bit) — future wave
- TLS, secrets, rolling upgrades — separate issues
- Agent executor hardening — separate issue