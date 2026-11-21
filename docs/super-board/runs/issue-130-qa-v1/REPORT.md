# issue-130 QA v1

Head under test: `e9d08ed`, nats-server v2.15.3.

| AC | Result | Proof |
|---|---|---|
| 1 STREAM.INFO with `subjects_filter` recorded | ✅ | `worker_cannot_list_other_tenants_keys_through_stream_info`; with the old grant restored it lists `$KV.MINGA_ASSIGN.beta.node.9.ingress.8443` to worker `default/7` (`ac1-red-with-grant-restored.log`), with the fix it is refused (`nats_tls.log`) |
| 2 no tenant B key through any granted JetStream API | ✅ | STREAM.INFO refused; new `worker_cannot_learn_other_tenants_keys_through_request_bodies`: CONSUMER.CREATE with `filter_subjects` or a mismatched body filter returns 10137 / 10131, DIRECT.GET last-by-subject with a body returns 408 Bad Request, no beta key delivered |

Test gaps (not written):
- Low: `$JS.API.CONSUMER.INFO.KV_MINGA_ASSIGN.*` returns any consumer's config, whose filter names tenant and node. Names are random NUIDs and no granted API lists them.
- Low: JWT signer template covered by unit test only; the server-side checks above are the same for both auth modes.
- CI 2.14.10 leg not run locally.

Logs: `nats_tls.log`, `checks.log` (fmt, clippy, minga-bus, minga-api), `minga-bus-live-nats.log` (suites with `MINGA_REQUIRE_NATS=1`).
