# ADR 0004: Observability — Metrics, Health, Structured Logging

**Status:** Accepted
**Date:** 2026-10-26
**Deciders:** Ferru

## Context
Wave 8 delivered NATS log transport, restart policies, and containerd gRPC integration. The system lacks:
- **Metrics** — no visibility into task throughput, scheduler decisions, Raft health, bus latency
- **Health endpoints** — no standard `/healthz` / `/readyz` for load balancers / orchestrators
- **Structured logging** — `tracing` is used but output is human-readable, not machine-parseable; no correlation IDs across bus hops

Operators need Prometheus-scrapable metrics, Kubernetes-compatible health probes, and JSON logs with trace correlation to debug distributed flows.

## Decision
1. **New crate `minga-telemetry`** — owns all observability concerns:
   - Prometheus metrics registry and `/metrics` HTTP endpoint
   - `/healthz` (liveness) and `/readyz` (readiness) endpoints
   - Structured JSON logging setup (`tracing-subscriber` + `tracing-appender`)
   - Correlation ID extraction/propagation from bus message headers

2. **Metrics emitted** (all prefixed `minga_`):
   - `minga_tasks_total{state="running|pending|failed|completed"}` — gauge
   - `minga_task_starts_total`, `minga_task_stops_total`, `minga_task_restarts_total` — counters
   - `minga_scheduler_reconcile_duration_seconds` — histogram
   - `minga_scheduler_placement_decisions_total{result="placed|no_node|constraint_fail"}` — counter
   - `minga_raft_leader{term}` — gauge (1 if leader)
   - `minga_raft_replication_lag` — gauge (log index diff)
   - `minga_bus_publish_latency_seconds{topic="events|logs|commands"}` — histogram
   - `minga_agent_pool_{idle|busy|total}` — gauges
   - `minga_containerd_grpc_latency_seconds{method}` — histogram

3. **Health endpoints**:
   - `GET /healthz` — 200 if process alive (liveness)
   - `GET /readyz` — 200 if Raft node is leader/follower with committed index >= applied index, AND bus connection healthy

4. **Structured logging**:
   - JSON output via `tracing-subscriber::fmt::format().json()`
   - Fields: `timestamp`, `level`, `target`, `message`, `correlation_id` (from `minga.correlation_id` header), `task_id`, `node_id`
   - Log level configurable via `MINGA_LOG_LEVEL` (default `info`)

4. **Integration points**:
   - `minga-node` supervisor: instrument reconcile loop, task start/stop/restart
   - `minga-scheduler`: instrument placement, reconcile
   - `minga-state`: instrument Raft leadership changes, replication
   - `minga-bus`: instrument publish latency, correlation ID propagation
   - `minga-api`: expose `/metrics`, `/healthz`, `/readyz` on admin port

## Consequences
- **Positive**: Standard observability stack; debuggable in production; integrates with Prometheus/Grafana/K8s
- **Negative**: New crate adds compile-time dependency; metrics cardinality must be bounded (no raw task_id labels)
- **Follow-up**: Log rotation, OpenTelemetry tracing, log shipper integration — future waves

## Alternatives Considered
- **Embed in `minga-node`** — rejected: metrics/health are cross-cutting, needed by API and scheduler too
- **OpenTelemetry from day one** — rejected: adds heavy deps; Prometheus + JSON logs suffice for MVP; OTel can wrap later
- **Push metrics to NATS** — rejected: Prometheus pull model is standard; push gateway adds complexity

## Verification
- `cargo test -p minga-telemetry` passes
- `curl /metrics` shows `minga_` metrics
- `curl /healthz` and `/readyz` return 200 on healthy node
- Logs are valid JSON with `correlation_id` when bus messages processed

## Implementation status (2026-11-01)

- **Endpoints:** `minga api` serves `/metrics`, `/healthz` and `/readyz`, behind `--api-token` when it's set. `minga manager` and `minga worker` serve them with `--metrics-bind` (`MINGA_METRICS_BIND`).
- **Readiness:** `/readyz` is ready once a Raft leader is known; on nodes, the bus must also answer. A 503 carries the reason.
- **Metrics that move:**
  - `minga_task_starts_total`, `minga_task_stops_total`, `minga_task_restarts_total`
  - `minga_tasks_total{state="running"}`
  - `minga_scheduler_reconcile_duration_seconds`
  - `minga_scheduler_placement_decisions_total{result="placed"}`
  - `minga_raft_leader`
  - `minga_bus_publish_latency_seconds{topic}`
- **Registered but never updated:**
  - the `no_node` and `constraint_fail` placement results
  - `minga_raft_replication_lag`
  - `minga_agent_pool_*`
  - `minga_containerd_grpc_latency_seconds`
- **Logging:** JSON on stderr for `manager`, `worker` and `api`. The level comes from `MINGA_LOG_LEVEL`, then `RUST_LOG`, and defaults to `info`.
- **Not done:** correlation IDs aren't propagated across bus hops.
