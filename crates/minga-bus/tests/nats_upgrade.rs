//! Upgrade proof (Phase 14, plan 14-04): a JetStream store written by an
//! older nats-server must keep Minga's data after the server is replaced.
//!
//! These tests are `#[ignore]`d and need MINGA_TEST_NATS_URL; they are run
//! in order by `scripts/nats-upgrade-proof.sh`, which swaps the server
//! binary between them:
//! - `upgrade_seed` runs on the old server against a fresh store.
//! - `upgrade_probe` runs after each replacement and can be repeated.
//! - `upgrade_verify` runs once on the final server.
//!
//! MINGA_UPGRADE_REPLICAS (default 1) is the replica count for a cluster run.

use std::time::Duration;

use futures::StreamExt;
use minga_bus::{subjects, EventBus, LogEntry, NatsBus};

const SEEDED_KEY: &str = "upgrade.seeded";
const SEEDED_VALUE: &[u8] = b"written by the old server";
const STATUS_KEY: &str = "upgrade.status";
const TASK: &str = "upgrade-task";
const SEEDED_LOGS: usize = 3;
const SEEDED_EVENTS: u64 = 3;
const SEEDED_NODE: u64 = 77;
/// Longer than any upgrade run, so the seeded lease outlives the restarts.
const SEED_LEASE_TTL: Duration = Duration::from_secs(3600);
const REPLICATED: [&str; 4] = [
    "MINGA_EVENTS",
    "KV_MINGA_STATUS",
    "KV_MINGA_LEASES",
    "KV_MINGA_ASSIGN_upgrade",
];

fn url() -> String {
    std::env::var("MINGA_TEST_NATS_URL").expect("the upgrade proof needs MINGA_TEST_NATS_URL")
}

fn replicas() -> usize {
    std::env::var("MINGA_UPGRADE_REPLICAS")
        .map(|r| r.parse().expect("MINGA_UPGRADE_REPLICAS is a number"))
        .unwrap_or(1)
}

async fn jetstream() -> async_nats::jetstream::Context {
    let client = async_nats::connect(url()).await.expect("connect");
    async_nats::jetstream::new(client)
}

/// In a cluster, wait until every replicated stream has a leader and all
/// its followers are current. `ensure_replicas` returns once the server
/// accepts the new replica count, and a write during the leader change that
/// follows can time out waiting for its ack.
async fn settled() {
    if replicas() < 2 {
        return;
    }
    let js = jetstream().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    'wait: loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "streams did not settle at {} replicas",
            replicas()
        );
        for name in REPLICATED {
            let mut stream = js.get_stream(name).await.unwrap();
            let info = stream.info().await.unwrap();
            let ready = info.cluster.as_ref().is_some_and(|c| {
                c.leader.is_some()
                    && c.replicas.len() + 1 == replicas()
                    && c.replicas.iter().all(|p| p.current && !p.offline)
            });
            if !ready {
                tokio::time::sleep(Duration::from_millis(250)).await;
                continue 'wait;
            }
        }
        return;
    }
}

#[tokio::test]
#[ignore = "run by scripts/nats-upgrade-proof.sh on the old server"]
async fn upgrade_seed() {
    let bus = NatsBus::connect_with_ttl(&url(), SEED_LEASE_TTL)
        .await
        .expect("connect to the old server");
    // The tenant's assignment bucket, so the raise below covers it.
    bus.ensure_tenant_resources("upgrade").await.unwrap();
    if replicas() > 1 {
        bus.ensure_replicas(replicas())
            .await
            .expect("raise replicas");
    }
    settled().await;
    bus.put_assignment(SEEDED_KEY, SEEDED_VALUE).await.unwrap();
    bus.put_status(STATUS_KEY, b"running").await.unwrap();
    bus.refresh_lease(SEEDED_NODE, b"seeded registration")
        .await
        .unwrap();
    for i in 0..SEEDED_LOGS {
        bus.publish_log(TASK, &LogEntry::stdout(TASK, format!("line {i}")))
            .await
            .unwrap();
    }
    for i in 0..SEEDED_EVENTS {
        bus.publish_event(
            &subjects::node_event("default", SEEDED_NODE),
            format!("event {i}").as_bytes(),
        )
        .await
        .unwrap();
    }
}

/// Repeatable after every replacement: the seeded data is intact, and
/// publishing, KV writes and reads, a watch and a log tail still work.
#[tokio::test]
#[ignore = "run by scripts/nats-upgrade-proof.sh after each server replacement"]
async fn upgrade_probe() {
    // A rolling upgrade waits for the replaced server to catch up.
    settled().await;
    let bus = NatsBus::connect_with_ttl(&url(), SEED_LEASE_TTL)
        .await
        .expect("connect after the replacement");

    assert_eq!(
        bus.get_assignment(SEEDED_KEY).await.unwrap().as_deref(),
        Some(SEEDED_VALUE),
        "seeded assignment"
    );
    assert_eq!(
        bus.get_status(STATUS_KEY).await.unwrap().as_deref(),
        Some(b"running".as_slice()),
        "seeded status"
    );
    assert!(bus.is_alive(SEEDED_NODE).await.unwrap(), "seeded lease");

    let history = bus.log_history(TASK, 1_000).await.unwrap();
    let seeded: Vec<_> = (0..SEEDED_LOGS).map(|i| format!("line {i}")).collect();
    let old: Vec<_> = history
        .iter()
        .take(SEEDED_LOGS)
        .map(|e| String::from_utf8_lossy(&e.data).into_owned())
        .collect();
    assert_eq!(old, seeded, "seeded log history");

    let js = jetstream().await;
    let mut events = js.get_stream("MINGA_EVENTS").await.unwrap();
    let info = events.info().await.unwrap();
    assert!(
        info.state.messages >= SEEDED_EVENTS,
        "seeded events: {}",
        info.state.messages
    );

    // New writes still work.
    bus.put_assignment("upgrade.probe", b"probe").await.unwrap();
    let mut watch = bus.watch_assignments("upgrade.").await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("watch replays current keys")
        .expect("watch ended");
    assert!(first.key.starts_with("upgrade."), "watched {}", first.key);
    bus.publish_log(TASK, &LogEntry::stdout(TASK, "probe"))
        .await
        .unwrap();
    let tail = bus.log_history(TASK, 1).await.unwrap();
    assert_eq!(
        tail.last().map(|e| e.data.as_slice()),
        Some(b"probe".as_slice())
    );
    bus.publish_event(&subjects::node_event("default", SEEDED_NODE), b"probe")
        .await
        .unwrap();
}

/// Once, on the final server: the replica counts and consumer cap are what
/// Minga expects, and the lease-TTL update path works on the old buckets.
#[tokio::test]
#[ignore = "run by scripts/nats-upgrade-proof.sh once on the final server"]
async fn upgrade_verify() {
    let js = jetstream().await;
    let bus = NatsBus::connect_with_ttl(&url(), SEED_LEASE_TTL)
        .await
        .expect("connect");
    let before = stream_configs(&js).await;
    for config in &before {
        assert_eq!(config.num_replicas, replicas(), "{} replicas", config.name);
    }
    if replicas() > 1 {
        bus.ensure_replicas(replicas()).await.unwrap();
        assert_eq!(
            stream_configs(&js).await,
            before,
            "ensure_replicas is a no-op"
        );
    }
    // Buckets the old server created get the 2.15 consumer cap lifted.
    for bucket in [
        "KV_MINGA_ASSIGN_upgrade",
        "KV_MINGA_STATUS",
        "KV_MINGA_LEASES",
    ] {
        let mut stream = js.get_stream(bucket).await.unwrap();
        let config = stream.info().await.unwrap().config.clone();
        assert!(
            config.max_consumers > 1_000,
            "{bucket} max_consumers {}",
            config.max_consumers
        );
    }

    // Shortening the lease TTL updates a bucket the old server created, and
    // a new lease still expires on the server's clock.
    let bus = NatsBus::connect_with_ttl(&url(), Duration::from_secs(2))
        .await
        .expect("connect with a shorter lease TTL");
    bus.refresh_lease(SEEDED_NODE + 1, b"new").await.unwrap();
    assert!(bus.is_alive(SEEDED_NODE + 1).await.unwrap());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while bus.is_alive(SEEDED_NODE + 1).await.unwrap() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "lease did not expire"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Put the seeded TTL and lease back, so `upgrade_probe` can run again
    // (the downgrade check runs it after this).
    let bus = NatsBus::connect_with_ttl(&url(), SEED_LEASE_TTL)
        .await
        .expect("restore the seeded lease TTL");
    bus.refresh_lease(SEEDED_NODE, b"seeded registration")
        .await
        .unwrap();
}

async fn stream_configs(
    js: &async_nats::jetstream::Context,
) -> Vec<async_nats::jetstream::stream::Config> {
    let mut configs = Vec::new();
    for name in REPLICATED {
        let mut stream = js.get_stream(name).await.unwrap();
        configs.push(stream.info().await.unwrap().config.clone());
    }
    configs
}
