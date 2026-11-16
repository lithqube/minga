//! Ignored tests driven by `scripts/nats-upgrade-proof.sh` across a server restart.

use std::time::Duration;

use minga_bus::{EventBus, LogEntry, NatsBus};
use std::future::Future;

fn test_url() -> String {
    std::env::var("MINGA_TEST_NATS_URL")
        .expect("the upgrade proof script must set MINGA_TEST_NATS_URL")
}

async fn eventually<T, F, Fut>(what: &str, mut operation: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = minga_bus::Result<T>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match operation().await {
            Ok(value) => return value,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(error) => panic!("{what}: {error}"),
        }
    }
}

#[tokio::test]
#[ignore = "run through scripts/nats-upgrade-proof.sh"]
async fn seed() {
    let url = test_url();
    let bus = NatsBus::connect_with_ttl(&url, Duration::from_secs(3_600))
        .await
        .expect("connect to the old NATS server");

    bus.put_assignment("upgrade-proof.task", b"assignment-before-upgrade")
        .await
        .expect("write assignment");
    bus.put_status("node.987.task.upgrade-proof.task", b"status-before-upgrade")
        .await
        .expect("write status");
    bus.refresh_lease(987, b"lease-before-upgrade")
        .await
        .expect("write lease");
    bus.publish_log(
        "upgrade-proof.task",
        &LogEntry::stdout("upgrade-proof.task", b"log-before-upgrade"),
    )
    .await
    .expect("write log");
}

#[tokio::test]
#[ignore = "run through scripts/nats-upgrade-proof.sh"]
async fn verify() {
    let url = test_url();
    let bus = NatsBus::connect_with_ttl(&url, Duration::from_secs(3_600))
        .await
        .expect("connect to the upgraded NATS server");

    assert_eq!(
        bus.get_assignment("upgrade-proof.task")
            .await
            .expect("read assignment")
            .as_deref(),
        Some(&b"assignment-before-upgrade"[..])
    );
    assert_eq!(
        bus.get_status("node.987.task.upgrade-proof.task")
            .await
            .expect("read status")
            .as_deref(),
        Some(&b"status-before-upgrade"[..])
    );
    assert!(bus.is_alive(987).await.expect("read seeded lease"));

    let history = bus
        .log_history("upgrade-proof.task", 0)
        .await
        .expect("read log history");
    assert!(history
        .iter()
        .any(|entry| entry.data == b"log-before-upgrade"));

    let expiring_bus = NatsBus::connect_with_ttl(&url, Duration::from_millis(300))
        .await
        .expect("update the lease bucket TTL after upgrade");
    expiring_bus
        .refresh_lease(988, b"lease-after-upgrade")
        .await
        .expect("write post-upgrade lease");
    assert!(expiring_bus.is_alive(988).await.expect("read new lease"));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while expiring_bus.is_alive(988).await.expect("poll new lease") {
        assert!(
            tokio::time::Instant::now() < deadline,
            "post-upgrade lease did not expire"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let restored_bus = NatsBus::connect_with_ttl(&url, Duration::from_secs(3_600))
        .await
        .expect("restore the long lease TTL for the downgrade check");
    restored_bus
        .refresh_lease(987, b"lease-before-upgrade")
        .await
        .expect("refresh the seeded lease after expiry proof");
    assert!(restored_bus
        .is_alive(987)
        .await
        .expect("read restored seeded lease"));
}

#[tokio::test]
#[ignore = "run through scripts/nats-upgrade-proof.sh"]
async fn downgrade_check() {
    let bus = NatsBus::connect_with_ttl(&test_url(), Duration::from_secs(3_600))
        .await
        .expect("connect to NATS 2.10.22 after the 2.15.3 upgrade");
    assert_eq!(
        bus.get_assignment("upgrade-proof.task")
            .await
            .expect("read downgraded assignment")
            .as_deref(),
        Some(&b"assignment-before-upgrade"[..])
    );
    assert_eq!(
        bus.get_status("node.987.task.upgrade-proof.task")
            .await
            .expect("read downgraded status")
            .as_deref(),
        Some(&b"status-before-upgrade"[..])
    );
    assert!(bus.is_alive(987).await.expect("read downgraded lease"));
    let history = bus
        .log_history("upgrade-proof.task", 0)
        .await
        .expect("read downgraded log history");
    assert!(history
        .iter()
        .any(|entry| entry.data == b"log-before-upgrade"));
}

#[tokio::test]
#[ignore = "run through scripts/nats-rolling-upgrade-proof.sh"]
async fn rolling_seed() {
    let bus = NatsBus::connect(&test_url())
        .await
        .expect("connect to the old NATS cluster");
    bus.ensure_replicas(3)
        .await
        .expect("raise replicated streams");

    let mut logs = bus
        .jetstream()
        .get_stream("MINGA_LOGS")
        .await
        .expect("get log stream");
    let mut config = logs.info().await.expect("read log stream").config.clone();
    config.num_replicas = 3;
    bus.jetstream()
        .update_stream(config)
        .await
        .expect("replicate the log stream for this proof");

    eventually("seed rolling status", || {
        bus.put_status("node.989.task.rolling-proof", b"rolling-seed")
    })
    .await;
    let log_entry = LogEntry::stdout("rolling-proof", b"rolling-seed");
    eventually("seed rolling log", || {
        bus.publish_log("rolling-proof", &log_entry)
    })
    .await;
}

#[tokio::test]
#[ignore = "run through scripts/nats-rolling-upgrade-proof.sh"]
async fn rolling_check() {
    let bus = NatsBus::connect(&test_url())
        .await
        .expect("connect to the rolling NATS cluster");
    let status = eventually("read rolling status", || {
        bus.get_status("node.989.task.rolling-proof")
    })
    .await;
    assert_eq!(status.as_deref(), Some(&b"rolling-seed"[..]));
    eventually("publish rolling status", || {
        bus.put_status("node.989.task.rolling-check", b"rolling-check")
    })
    .await;
    let log_entry = LogEntry::stdout("rolling-proof", b"rolling-check");
    eventually("publish rolling log", || {
        bus.publish_log("rolling-proof", &log_entry)
    })
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match bus.log_history("rolling-proof", 0).await {
            Ok(history)
                if history.iter().any(|entry| entry.data == b"rolling-seed")
                    && history.iter().any(|entry| entry.data == b"rolling-check") =>
            {
                break;
            }
            Ok(_) | Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Ok(_) | Err(_) => panic!("rolling log history did not retain both entries"),
        }
    }
}

#[tokio::test]
#[ignore = "run through scripts/nats-rolling-upgrade-proof.sh"]
async fn rolling_complete() {
    let bus = NatsBus::connect(&test_url())
        .await
        .expect("connect after the rolling upgrade");
    bus.ensure_replicas(3)
        .await
        .expect("ensure R3 remains a no-op after upgrade");
    for stream_name in ["MINGA_EVENTS", "KV_MINGA_STATUS", "MINGA_LOGS"] {
        let mut stream = bus
            .jetstream()
            .get_stream(stream_name)
            .await
            .unwrap_or_else(|error| panic!("{stream_name}: {error}"));
        assert_eq!(
            stream
                .info()
                .await
                .expect("read stream config")
                .config
                .num_replicas,
            3,
            "{stream_name}"
        );
    }
    for bucket in ["KV_MINGA_ASSIGN", "KV_MINGA_STATUS", "KV_MINGA_LEASES"] {
        let mut stream = bus
            .jetstream()
            .get_stream(bucket)
            .await
            .unwrap_or_else(|error| panic!("{bucket}: {error}"));
        assert_eq!(
            stream
                .info()
                .await
                .expect("read KV stream config")
                .config
                .max_consumers,
            50_000,
            "{bucket}"
        );
    }
}
