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

Supported auth schemes are `bearer`, `api_key`, and `aws_sigv4`. SigV4 uses the secret IDs `access_key_secret`, `secret_key_secret`, and optional `session_token_secret`, and the literal strings `region` and `service`. Every secret ID must belong to the service's tenant. Costs are integer microdollars per million input/output tokens and are omitted unless both prices are configured.

Service and task egress policies are persisted from state format v10. Worker assignments carry the format version, so an older worker refuses a task with a policy instead of dropping it; upgrade every node together.

`bearer` sets `Authorization: Bearer …`; `api_key` sets `x-api-key`. The proxy replaces any workload-supplied values for the selected auth headers.

### Proxy limits

- Route destinations: tenant routes never follow redirects, ignore system proxy settings, and refuse loopback, link-local, unspecified, multicast, broadcast and private or unique-local addresses, both as literal hosts and in DNS answers at connect time, unless the worker allows the range with `--egress-allow-cidr`.
- Request identifiers: `x-minga-customer`, `x-minga-model`, `x-minga-route` and the body `model` are limited to 256 bytes (400 otherwise); Compose route names are limited to 256 bytes too.
- Bodies: a non-streamed upstream response is buffered up to 32 MiB (502 beyond). An upstream error response is buffered whole, up to 64 KiB, and redacted before any byte reaches the caller; larger error bodies become a generic 502. Success streams stay incremental, and a stream that is idle for 60 seconds ends.
- Budgets: admission reserves an upper bound, so concurrent requests cannot spend the same balance: the request body's byte length (a token covers at least one byte) plus `max_tokens` or `max_completion_tokens` times `n`. A request without an output cap holds the rest of a limited budget until it settles. Reservations stay counted across a window reset and settle only against the budget registration that issued them, so a re-registered workload is not charged for its predecessor's requests. The budget is enforced at admission, not on the provider: an uncapped response can still exceed what remains, and that overrun is charged and refuses later requests until the window resets. Setting `max_tokens` bounds each admitted request, provided the upstream honors the cap and charges no more input than the request body's byte length. The reservation settles to the provider's counts when the response ends and is released if the request fails before reaching the provider. A client that disconnects mid-stream does not stop the upstream read, so provider-reported usage is still recorded. A client that stays connected but stops reading for the stream idle timeout is cut off the same way: the proxy drains the upstream, settles and publishes, and the client's body ends in an error after what was already queued.
- Usage counts are read from the top-level JSON `usage` field (or `message.usage` in Anthropic events), never from a `"usage"` string elsewhere in the body.
- Cache entries are keyed by workload, route, the forwarded `accept`, `anthropic-version` and `anthropic-beta` values, and an opaque revision of the route settings. The revision includes a per-process keyed fingerprint of the route credentials (never their values), so a rotated credential starts a fresh cache namespace. The cache is bounded by entry count and by 64 MiB of cached bytes.
- Usage events are published after the response ends, with a 5-second deadline and up to three attempts under the same `Nats-Msg-Id`.

Usage totals are not billing-grade: prices come from the tenant's own Compose file and token counts from the upstream the tenant chose. When the usage stream reaches its byte or age limit, JetStream drops the oldest events.
