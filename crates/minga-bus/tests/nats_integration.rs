//! Integration tests for [`NatsBus`] against a real NATS server.
//!
//! Each test keeps its NATS server alive until the bus is dropped. Tests are
//! serialized because concurrent stream setup can time out under CI load.
//!
//! To run: `cargo test -p minga-bus --test nats_integration -- --nocapture`

mod common;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use minga_bus::{EventBus, LogEntry, LogStream, NatsBus};
static NATS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct NatsTestBus {
    bus: Arc<NatsBus>,
    _server: Arc<common::NatsServer>,
    _lock: tokio::sync::MutexGuard<'static, ()>,
}

impl std::ops::Deref for NatsTestBus {
    type Target = NatsBus;

    fn deref(&self) -> &Self::Target {
        &self.bus
    }
}

async fn connect() -> Option<NatsTestBus> {
    let lock = NATS_TEST_LOCK.lock().await;
    let url = common::nats_url().await?;
    let bus = NatsBus::connect(&url)
        .await
        .unwrap_or_else(|e| panic!("failed to connect to NATS at {url}: {e}"));
    Some(NatsTestBus {
        bus,
        _server: url,
        _lock: lock,
    })
}

/// Waits for a stream to yield one item, or fails the test on timeout.
async fn next_log(stream: &mut minga_bus::LogEventStream) -> LogEntry {
    tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("timed out waiting for log entry")
        .expect("log stream ended unexpectedly")
}

#[tokio::test]
async fn nats_connect_creates_streams() {
    let Some(bus) = connect().await else {
        return;
    };

    // If the streams were not created, these lookups fail.
    for stream in ["MINGA_EVENTS", "MINGA_LOGS"] {
        let result = bus.jetstream().get_stream(stream).await;
        assert!(
            result.is_ok(),
            "stream {stream} should exist after connect, got: {result:?}"
        );
    }
}

#[tokio::test]
async fn nats_publish_and_subscribe_logs() {
    let Some(bus) = connect().await else {
        return;
    };

    let mut stream = bus
        .subscribe_logs("task-logs-basic")
        .await
        .expect("failed to subscribe to logs");

    let entry = LogEntry::stdout("task-logs-basic", b"hello from nats\n");
    bus.publish_log("task-logs-basic", &entry)
        .await
        .expect("failed to publish log");

    let received = next_log(&mut stream).await;
    assert_eq!(received.task_id, "task-logs-basic");
    assert_eq!(received.data, b"hello from nats\n");
    assert_eq!(received.stream, LogStream::Stdout);
}

#[tokio::test]
async fn nats_log_stream_isolates_task_ids() {
    let Some(bus) = connect().await else {
        return;
    };

    let mut stream = bus
        .subscribe_logs("task-isolate-a")
        .await
        .expect("failed to subscribe to logs");

    // Publishing for another task must not leak into this subscription.
    bus.publish_log(
        "task-isolate-b",
        &LogEntry::stdout("task-isolate-b", b"other\n"),
    )
    .await
    .expect("failed to publish log");

    bus.publish_log(
        "task-isolate-a",
        &LogEntry::stdout("task-isolate-a", b"mine\n"),
    )
    .await
    .expect("failed to publish log");

    let received = next_log(&mut stream).await;
    assert_eq!(
        received.task_id, "task-isolate-a",
        "subscription received a log entry for the wrong task"
    );
    assert_eq!(received.data, b"mine\n");
}

#[tokio::test]
async fn nats_wildcard_log_subscription_receives_all_tasks() {
    let Some(bus) = connect().await else {
        return;
    };

    let mut stream = bus
        .subscribe_logs("*")
        .await
        .expect("failed to subscribe to logs");

    bus.publish_log("task-wild-a", &LogEntry::stdout("task-wild-a", b"a\n"))
        .await
        .expect("failed to publish log");
    bus.publish_log("task-wild-b", &LogEntry::stderr("task-wild-b", b"b\n"))
        .await
        .expect("failed to publish log");

    // The wildcard stream may also carry entries from other tests, so look
    // for our two specific tasks rather than assuming order or exclusivity.
    let mut seen: Vec<LogEntry> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && seen.len() < 2 {
        let Ok(Some(entry)) = tokio::time::timeout(Duration::from_secs(5), stream.next()).await
        else {
            break;
        };
        if entry.task_id == "task-wild-a" || entry.task_id == "task-wild-b" {
            seen.push(entry);
        }
    }

    let mut ids: Vec<String> = seen.iter().map(|e| e.task_id.clone()).collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["task-wild-a".to_string(), "task-wild-b".to_string()],
        "wildcard subscription should receive both tasks"
    );

    let stderr_entry = seen.iter().find(|e| e.task_id == "task-wild-b").unwrap();
    assert_eq!(stderr_entry.stream, LogStream::Stderr);
}

#[tokio::test]
async fn nats_log_entries_survive_a_new_subscriber() {
    let Some(bus) = connect().await else {
        return;
    };

    bus.publish_log(
        "task-persist",
        &LogEntry::stdout("task-persist", b"persisted\n"),
    )
    .await
    .expect("failed to publish log");

    // A live subscription only sees entries published from now on, so the
    // retained backlog is read through log_history.
    let history = bus
        .log_history("task-persist", 0)
        .await
        .expect("failed to read log history");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].data, b"persisted\n");
}

#[tokio::test]
async fn nats_log_history_returns_the_last_n_entries() {
    let Some(bus) = connect().await else {
        return;
    };

    for i in 0..5 {
        bus.publish_log(
            "task-tail",
            &LogEntry::stdout("task-tail", format!("line {i}\n").as_bytes()),
        )
        .await
        .expect("failed to publish log");
    }

    let history = bus
        .log_history("task-tail", 2)
        .await
        .expect("failed to read log history");

    assert_eq!(history.len(), 2, "tail should limit to the last N entries");
    assert_eq!(history[0].data, b"line 3\n");
    assert_eq!(history[1].data, b"line 4\n");
}

#[tokio::test]
async fn nats_log_history_spans_multiple_fetch_batches() {
    let Some(bus) = connect().await else {
        return;
    };
    let task_id = "task-tail-multiple-batches";
    for i in 0..205 {
        bus.publish_log(
            task_id,
            &LogEntry::stdout(task_id, format!("{i}\n").as_bytes()),
        )
        .await
        .expect("failed to publish log");
    }
    let history = bus
        .log_history(task_id, 2)
        .await
        .expect("failed to read history");
    assert_eq!(
        history
            .iter()
            .map(|e| e.data.as_slice())
            .collect::<Vec<_>>(),
        vec![b"203\n".as_slice(), b"204\n".as_slice()]
    );
}

#[tokio::test]
async fn nats_log_history_is_scoped_to_one_task() {
    let Some(bus) = connect().await else {
        return;
    };

    bus.publish_log("task-scope-a", &LogEntry::stdout("task-scope-a", b"a\n"))
        .await
        .expect("failed to publish log");
    bus.publish_log("task-scope-b", &LogEntry::stdout("task-scope-b", b"b\n"))
        .await
        .expect("failed to publish log");

    let history = bus
        .log_history("task-scope-a", 0)
        .await
        .expect("failed to read log history");

    assert_eq!(history.len(), 1);
    assert_eq!(history[0].task_id, "task-scope-a");
}

#[tokio::test]
async fn nats_list_status_returns_existing_keys() {
    let Some(bus) = connect().await else {
        return;
    };

    bus.put_status("task.list-test", b"listed")
        .await
        .expect("failed to put status");

    let entries = bus.list_status().await.expect("failed to list status");

    let found = entries
        .iter()
        .find(|(key, _)| key == "task.list-test")
        .expect("list_status should include task.list-test");
    assert_eq!(found.1.as_slice(), b"listed");
}

#[tokio::test]
async fn nats_publish_and_subscribe_events() {
    let Some(bus) = connect().await else {
        return;
    };

    let mut stream = bus
        .subscribe_tenant_events("default")
        .await
        .expect("failed to subscribe to tenant events");

    let subject = minga_bus::subjects::task_event("default", "abc");
    bus.publish_tenant_event("default", &subject, b"task-event")
        .await
        .expect("failed to publish tenant event");

    let event = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("timed out waiting for event")
        .expect("event stream ended unexpectedly");

    assert_eq!(event.subject, subject);
    assert_eq!(event.payload, b"task-event");
}

#[tokio::test]
async fn nats_status_kv_roundtrip() {
    let Some(bus) = connect().await else {
        return;
    };

    bus.put_status("task.kv-test", b"status-bytes")
        .await
        .expect("failed to put status");

    let got = bus
        .get_status("task.kv-test")
        .await
        .expect("failed to get status");
    assert_eq!(got.as_deref(), Some(&b"status-bytes"[..]));
}

#[tokio::test]
async fn nats_watch_status_sees_existing_keys() {
    let Some(bus) = connect().await else {
        return;
    };

    bus.put_status("task.watch-test", b"watch-me")
        .await
        .expect("failed to put status");

    let mut watch = bus.watch_status().await.expect("failed to watch status");

    // The KV watch must replay existing keys, not only future writes.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut found = false;
    while tokio::time::Instant::now() < deadline && !found {
        let Ok(Some(event)) = tokio::time::timeout(Duration::from_secs(5), watch.next()).await
        else {
            break;
        };
        if event.key == "task.watch-test" {
            found = true;
        }
    }
    assert!(
        found,
        "watch_status should replay the existing task.watch-test key"
    );
}

fn usage_event(request_id: &str, tenant_id: &str, at: u64) -> minga_bus::UsageEvent {
    minga_bus::UsageEvent {
        request_id: request_id.into(),
        tenant_id: tenant_id.into(),
        service_id: "svc".into(),
        task_id: "task".into(),
        customer_id: "customer".into(),
        route: "primary".into(),
        requested_model: "m".into(),
        answered_model: "m".into(),
        input_tokens: 3,
        output_tokens: 4,
        cache_hit: false,
        latency_ms: 1,
        status: 200,
        occurred_at_unix_ms: at,
        cost_micros: None,
    }
}

#[tokio::test]
async fn nats_usage_cutoff_uses_event_time_not_storage_time() {
    let Some(bus) = connect().await else {
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    // A worker clock a minute ahead of the server stores the event before
    // its own timestamp.
    bus.publish_usage(&usage_event("skew-1", "usage-skew", now + 60_000))
        .await
        .expect("publish usage");
    let rows = bus
        .usage_events("usage-skew", now + 30_000, None, 10)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "the event's own time qualifies it");
}

#[tokio::test]
async fn nats_usage_skips_bad_records_and_keeps_newest() {
    let Some(bus) = connect().await else {
        return;
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    for (id, at) in [("u-1", now), ("u-2", now + 1)] {
        bus.publish_usage(&usage_event(id, "usage-a", at))
            .await
            .expect("publish usage");
    }
    // A tenant worker can publish arbitrary bytes on its own usage subject.
    let forged = serde_json::to_vec(&usage_event("u-3", "usage-b", now + 2)).unwrap();
    for payload in [b"not json".to_vec(), forged] {
        bus.jetstream()
            .publish("minga.usage-a.usage.svc", payload.into())
            .await
            .unwrap()
            .await
            .unwrap();
    }

    let rows = bus.usage_events("usage-a", 0, None, 1).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].request_id, "u-2", "limit keeps the newest rows");
    let rows = bus.usage_events("usage-a", now, None, 10).await.unwrap();
    assert_eq!(rows.len(), 2);
    let totals = bus.usage_totals(0).await.unwrap();
    let (_, total) = totals
        .iter()
        .find(|(tenant, _)| tenant == "usage-a")
        .expect("usage-a totals");
    assert_eq!(total.request_count, 2);
    assert!(bus
        .usage_events("usage-none", 0, None, 10)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn nats_tenant_event_history_is_filtered_bounded_and_publish_stamped() {
    let Some(bus) = connect().await else {
        return;
    };
    let tenant = format!("evh{}", std::process::id());
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    for (owner, task) in [
        (tenant.as_str(), "one"),
        ("evh-other", "two"),
        (tenant.as_str(), "three"),
    ] {
        bus.publish_tenant_event(
            owner,
            &format!("minga.{owner}.task.{task}.event"),
            task.as_bytes(),
        )
        .await
        .expect("failed to publish tenant event");
    }
    tokio::time::sleep(Duration::from_millis(1_100)).await;

    let events = bus
        .tenant_event_history(&tenant, 1)
        .await
        .expect("failed to read tenant event history");
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].subject,
        format!("minga.{tenant}.task.three.event")
    );
    assert_eq!(events[0].payload, b"three");
    // The publish time, not the time of the read a second later.
    assert!(events[0].timestamp >= before.saturating_sub(1_000));
    assert!(events[0].timestamp < before + 1_000);

    // A tail across many events, interleaved with another tenant's, keeps the
    // newest `limit` in publish order.
    for n in 0..20 {
        for owner in [tenant.as_str(), "evh-other"] {
            bus.publish_tenant_event(
                owner,
                &format!("minga.{owner}.task.t{n}.event"),
                format!("{n}").as_bytes(),
            )
            .await
            .expect("failed to publish tenant event");
        }
    }
    let events = bus
        .tenant_event_history(&tenant, 3)
        .await
        .expect("failed to read tenant event history");
    let payloads: Vec<_> = events
        .iter()
        .map(|event| event.payload.as_slice())
        .collect();
    assert_eq!(payloads, [b"17".as_slice(), b"18", b"19"]);
    let all = bus.tenant_event_history(&tenant, 100).await.unwrap();
    assert_eq!(all.len(), 22, "fewer than `limit` events returns them all");
}

/// The scheduler sweeps every tenant's assignments for stale keys. A tenant
/// whose bucket is at its consumer limit cannot be listed, and that must not
/// stop the sweep of the others or make its own keys look absent.
#[tokio::test]
async fn one_tenants_full_assignment_bucket_does_not_stop_the_global_listing() {
    let Some(bus) = connect().await else {
        return;
    };
    let pid = std::process::id();
    let (full, fine) = (format!("full{pid}"), format!("fine{pid}"));
    for tenant in [&full, &fine] {
        bus.ensure_tenant_resources(tenant).await.unwrap();
        bus.put_assignment(&format!("{tenant}.node.7.task.t"), b"stale")
            .await
            .unwrap();
    }
    // Fill the bucket: one consumer allowed, and one held.
    let js = bus.jetstream();
    let stream = js
        .get_stream(format!("KV_MINGA_ASSIGN_{full}"))
        .await
        .unwrap();
    let mut config = stream.cached_info().config.clone();
    config.max_consumers = 1;
    js.update_stream(config).await.unwrap();
    stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some("squat".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        bus.list_assignments(&format!("{full}.")).await.is_err(),
        "the full bucket still lists"
    );

    let all = bus.list_assignments("").await.expect("the sweep survives");
    assert!(all
        .iter()
        .any(|(key, _)| key == &format!("{fine}.node.7.task.t")));
    assert!(
        !all.iter()
            .any(|(key, _)| key.starts_with(&format!("{full}."))),
        "an unreadable tenant's keys are reported as present or absent"
    );
    for tenant in [&full, &fine] {
        js.delete_stream(format!("KV_MINGA_ASSIGN_{tenant}"))
            .await
            .unwrap();
    }
}

/// Seeds every place a tenant keeps data on the bus.
async fn seed_tenant(bus: &NatsBus, tenant: &str, node: u64) {
    bus.ensure_tenant_resources(tenant).await.unwrap();
    bus.append_audit(minga_bus::AuditRecord {
        tenant_id: tenant.to_string(),
        actor: "operator".into(),
        action: "tenant.test".into(),
        resource_path: format!("/tenants/{tenant}"),
        occurred_at_unix: 1_700_000_000,
    })
    .await
    .unwrap();
    bus.put_tenant_status(tenant, "node.7.task.t", b"running")
        .await
        .unwrap();
    bus.put_assignment(&format!("{tenant}.node.7.task.t"), b"assigned")
        .await
        .unwrap();
    bus.publish_tenant_event(tenant, &format!("minga.{tenant}.task.t.event"), b"event")
        .await
        .unwrap();
    bus.refresh_tenant_lease(tenant, 7, b"registration")
        .await
        .unwrap();
    // The shared status bucket records which tenant a node joined.
    bus.put_status(&format!("internal.node-join.{node}"), tenant.as_bytes())
        .await
        .unwrap();
}

#[tokio::test]
async fn deleting_a_tenant_removes_everything_it_had_on_the_bus() {
    let Some(bus) = connect().await else {
        return;
    };
    let pid = std::process::id();
    let (gone, kept) = (format!("gone{pid}"), format!("kept{pid}"));
    let (gone_node, kept_node) = (u64::from(pid) * 10 + 1, u64::from(pid) * 10 + 2);
    seed_tenant(&bus, &gone, gone_node).await;
    seed_tenant(&bus, &kept, kept_node).await;
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(bus.tenant_resources_exist(&gone).await.unwrap());

    bus.delete_tenant_resources(&gone).await.unwrap();
    assert!(!bus.tenant_resources_exist(&gone).await.unwrap());
    // Repeating it, as after a delete that failed partway, is harmless.
    bus.delete_tenant_resources(&gone).await.unwrap();

    // The same ID later is a fresh tenant: none of the old owner's data.
    bus.ensure_tenant_resources(&gone).await.unwrap();
    assert!(bus.list_audit(&gone, 10).await.unwrap().is_empty());
    assert!(bus.list_tenant_status(&gone).await.unwrap().is_empty());
    assert!(bus
        .list_assignments(&format!("{gone}."))
        .await
        .unwrap()
        .is_empty());
    assert!(bus
        .tenant_event_history(&gone, 10)
        .await
        .unwrap()
        .is_empty());
    assert!(!bus.is_tenant_node_alive(&gone, 7).await.unwrap());
    assert_eq!(
        bus.get_status(&format!("internal.node-join.{gone_node}"))
            .await
            .unwrap(),
        None,
        "the node-join record outlived its tenant"
    );

    // Another tenant is untouched.
    assert_eq!(bus.list_audit(&kept, 10).await.unwrap().len(), 1);
    assert_eq!(bus.list_tenant_status(&kept).await.unwrap().len(), 1);
    assert_eq!(
        bus.list_assignments(&format!("{kept}."))
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(bus.tenant_event_history(&kept, 10).await.unwrap().len(), 1);
    assert!(bus.is_tenant_node_alive(&kept, 7).await.unwrap());
    assert_eq!(
        bus.get_status(&format!("internal.node-join.{kept_node}"))
            .await
            .unwrap()
            .as_deref(),
        Some(kept.as_bytes())
    );

    for tenant in [&gone, &kept] {
        bus.delete_tenant_resources(tenant).await.unwrap();
    }
}

/// A second manager's connection to the server `bus` is on.
async fn second_manager(bus: &NatsTestBus) -> std::sync::Arc<NatsBus> {
    NatsBus::connect(&bus._server)
        .await
        .expect("second manager connects")
}

#[tokio::test]
async fn a_stale_delete_from_one_manager_cannot_remove_a_recreated_tenants_streams() {
    let Some(a) = connect().await else {
        return;
    };
    let b = second_manager(&a).await;
    let tenant = format!("fenced{}", std::process::id());

    a.claim_tenant(&tenant, 1).await.unwrap();
    seed_tenant(&a, &tenant, u64::from(std::process::id()) * 10 + 3).await;
    // A takes the delete and stalls; B retries it, takes it over and finishes.
    let fence_a = a
        .begin_tenant_delete(&tenant, Some(1))
        .await
        .unwrap()
        .unwrap();
    let fence_b = b.begin_tenant_delete(&tenant, None).await.unwrap().unwrap();
    assert_ne!(fence_a.revision, fence_b.revision);
    b.delete_tenant_incarnation(&tenant, fence_b).await.unwrap();
    assert!(!b.tenant_resources_exist(&tenant).await.unwrap());

    // B creates the ID again under a new generation.
    b.claim_tenant(&tenant, 2).await.unwrap();
    b.ensure_tenant_resources(&tenant).await.unwrap();
    assert!(b.tenant_resources_exist(&tenant).await.unwrap());

    // A resumes: refused, and B's streams stay.
    let error = a
        .delete_tenant_incarnation(&tenant, fence_a)
        .await
        .unwrap_err();
    assert!(matches!(error, minga_bus::BusError::Fenced(_)), "{error}");
    assert!(b.tenant_resources_exist(&tenant).await.unwrap());
    // And the old generation cannot provision over the new one.
    let error = a.claim_tenant(&tenant, 1).await.unwrap_err();
    assert!(matches!(error, minga_bus::BusError::Fenced(_)), "{error}");

    let fence = b
        .begin_tenant_delete(&tenant, Some(2))
        .await
        .unwrap()
        .unwrap();
    b.delete_tenant_incarnation(&tenant, fence).await.unwrap();
}

#[tokio::test]
async fn a_manager_sets_a_tenant_bucket_up_again_after_another_manager_deleted_it() {
    let Some(a) = connect().await else {
        return;
    };
    let b = second_manager(&a).await;
    let tenant = format!("stale{}", std::process::id());

    a.ensure_tenant_resources(&tenant).await.unwrap();
    a.put_tenant_status(&tenant, "node.7.task.t", b"running")
        .await
        .unwrap();
    // Another manager deletes the tenant's buckets; A's cache still says they exist.
    b.delete_tenant_resources(&tenant).await.unwrap();
    assert!(!b.tenant_resources_exist(&tenant).await.unwrap());

    a.put_tenant_status(&tenant, "node.7.task.t", b"running again")
        .await
        .unwrap();
    assert_eq!(
        a.get_tenant_status(&tenant, "node.7.task.t").await.unwrap(),
        Some(b"running again".to_vec())
    );
    a.delete_tenant_resources(&tenant).await.unwrap();
}
