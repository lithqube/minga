//! Integration tests for [`NatsBus`] against a real NATS server.
//!
//! These tests spin up a NATS server with JetStream enabled via
//! `testcontainers` and exercise the production transport end to end. They
//! skip gracefully when Docker is unavailable (e.g. a CI box without a
//! daemon), matching the pattern used by `minga-api`'s integration tests.
//!
//! One container is shared across all tests in this binary: starting NATS
//! costs seconds, and every test needs a server, so the container is started
//! once and reused. Each test uses unique task IDs so tests do not interfere
//! with one another.
//!
//! To run: `cargo test -p minga-bus --test nats_integration -- --nocapture`

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use minga_bus::{EventBus, LogEntry, LogStream, NatsBus};
use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};
use tokio::sync::OnceCell;

/// NATS client port inside the container.
const NATS_PORT: u16 = 4222;

/// Shared NATS container URL, started once per test binary.
static NATS: OnceCell<Option<String>> = OnceCell::const_new();

/// Starts (once) a NATS server with JetStream enabled.
///
/// Returns the client URL (`nats://127.0.0.1:<mapped-port>`), or `None` when
/// Docker is unavailable.
async fn nats_url() -> Option<String> {
    NATS.get_or_init(|| async {
        let image = GenericImage::new("nats", "2.10-alpine")
            .with_exposed_port(NATS_PORT.into())
            // nats-server logs its readiness banner to stderr, not stdout.
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
            .with_cmd(vec!["-js"]);

        let container = match image.start().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skipping: cannot start NATS container: {e}");
                return None;
            }
        };

        let port: u16 = match container.get_host_port_ipv4(NATS_PORT).await {
            Ok(p) => p,
            Err(e) => {
                eprintln!("skipping: cannot resolve NATS port: {e}");
                return None;
            }
        };

        // Leak the container handle so it stays alive for the whole run.
        std::mem::forget(container);
        Some(format!("nats://127.0.0.1:{port}"))
    })
    .await
    .clone()
}

/// Connects a fresh [`NatsBus`] to the shared server, or returns `None` if
/// Docker is unavailable.
async fn connect() -> Option<Arc<NatsBus>> {
    let url = nats_url().await?;
    match NatsBus::connect(&url).await {
        Ok(bus) => Some(bus),
        Err(e) => panic!("failed to connect to NATS at {url}: {e}"),
    }
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
    for stream in ["MINGA_EVENTS", "MINGA_COMMANDS", "MINGA_LOGS"] {
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
        .subscribe_events(&["minga.task.>"])
        .await
        .expect("failed to subscribe to events");

    bus.publish_event("minga.task.abc.event", b"task-event")
        .await
        .expect("failed to publish event");

    let event = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("timed out waiting for event")
        .expect("event stream ended unexpectedly");

    assert_eq!(event.subject, "minga.task.abc.event");
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

#[tokio::test]
async fn nats_command_roundtrip() {
    let Some(bus) = connect().await else {
        return;
    };

    let cmd = minga_bus::Command::Stop {
        task_id: "task-cmd".to_string(),
    };
    bus.send_command(7, &cmd)
        .await
        .expect("failed to send command");

    let received = bus
        .next_command(7)
        .await
        .expect("failed to receive command");
    assert_eq!(received, cmd);
}
