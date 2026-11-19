# Agent request usage

Minga records one body-free `minga_bus::UsageEvent` for each completed agent-model request. The serialized event contains request, tenant, service, task, customer, route, requested and answered model identifiers, token counts, cache-hit state, latency, status, event time (`occurred_at_unix_ms`), and optional integer `cost_micros`. Prompt and response bodies are never fields in the contract.

The publisher interface is `EventBus::publish_usage(&UsageEvent)`. Implementations use the event's `tenant_id` and `service_id` to publish to `minga.<tenant>.usage.<service>` in `MINGA_USAGE_<tenant>`, with `Nats-Msg-Id` set to `request_id`. JetStream deduplicates repeated IDs within the stream duplicate window. Tenant stream max age and max bytes are updated from `TenantQuotas.jetstream_age_secs` and `TenantQuotas.jetstream_bytes`; absent quota values use the tenant stream defaults. The existing `KV_MINGA_USAGE_<tenant>` bucket remains a separate resource.

`GET /v1.41/agent/usage` accepts `since_unix_ms`, optional `customer`, and optional `limit`. The handler derives the tenant from authenticated caller identity and never accepts a tenant query parameter. Tenant callers receive matching rows, with the response limit clamped to 1–1000. Cluster operators receive per-tenant aggregates only; customer, task, and request rows are not exposed by that response. `minga-ctl agent usage --customer C --since 30d` converts the relative duration into a Unix-millisecond lower bound and uses the same endpoint.

Prometheus exports `minga_agent_usage_requests_total`, `minga_agent_usage_input_tokens_total`, `minga_agent_usage_output_tokens_total`, and `minga_agent_usage_cost_micros_total`. Each has a tenant-only label. Customer, service, task, route, and request identifiers are never metric labels.

## Service egress routes

Declare route credentials by secret ID; credential values stay in each tenant's node-local secret store and never enter Raft:

```yaml
services:
  inference:
    image: example/inference:latest
    x-minga-egress:
      required: true
      routes:
        - name: anthropic
          upstream_url: https://api.anthropic.com
          auth:
            scheme: api_key
            secret: anthropic_api_key
          fallback: [gateway]
          input_price_micros_per_million: 3000000
          output_price_micros_per_million: 15000000
        - name: gateway
          upstream_url: https://llm-gateway.example
      models: [claude-haiku-*]
```

The proxy runs only on workers started with `--egress-bind` (for example `0.0.0.0:8080`), and services with routes are placed only on those workers. The runtime sets `MINGA_EGRESS_URL` to the task's bridge gateway at the bind's port. Tokens live only in worker memory: after a worker restart, routed containers are stopped and restarted with a fresh token instead of being adopted, and a stopped task's token is revoked as soon as its process is gone. Each workload reads its token from `/run/minga/egress-token` and sends it as `X-Minga-Sandbox-Token`. Select a route with `X-Minga-Route`; if exactly one route is configured, the header is optional. Model allow-list patterns are exact names or a single trailing `*`; when configured, requests must include a top-level JSON `model` field. Failover occurs only on connect errors, HTTP 429, and 5xx responses. A required policy blocks other direct task egress, including same-bridge peers, while allowing the node's bridge DNS and proxy and replies to connections made to the task. The nft rules match the task bridge and the task's own source address, which the per-veth source guard pins, because with `br_netfilter` the inet hooks see the bridge rather than the veth. Model allow-lists are policy checks, not proof of data residency.

Supported auth schemes are `bearer`, `api_key`, and `aws_sigv4`. SigV4 uses `access_key_secret`, `secret_key_secret`, optional `session_token_secret`, `region`, and `service`; all are secret IDs. Costs are integer microdollars per million input/output tokens and are omitted unless both prices are configured.

Service and task egress policies are persisted from state format v10. Worker assignments carry the format version, so an older worker refuses a task with a policy instead of dropping it; upgrade every node together.

`bearer` sets `Authorization: Bearer …`; `api_key` sets `x-api-key`. The proxy replaces any workload-supplied values for the selected auth headers.
