mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use minga_bus::nats::api_level_warning;
use minga_bus::NatsBus;

#[tokio::test]
async fn manager_and_worker_api_levels_and_bucket_consumer_limits() {
    let Some(url) = common::nats_url().await else {
        return;
    };

    let manager = NatsBus::connect(&url).await.expect("connect manager");
    let level = manager
        .api_level()
        .expect("manager must query account info");
    assert!(
        level >= 4,
        "supported NATS API level must be at least 4, got {level}"
    );
    assert_eq!(api_level_warning(level, "runtime server"), None);

    let worker = NatsBus::connect_worker(&url, 73)
        .await
        .expect("connect worker using manager-created buckets");
    assert_eq!(
        worker.api_level(),
        None,
        "workers must not query account info"
    );

    for bucket in ["MINGA_ASSIGN", "MINGA_STATUS", "MINGA_LEASES"] {
        let mut stream = manager
            .jetstream()
            .get_stream(format!("KV_{bucket}"))
            .await
            .unwrap_or_else(|error| panic!("open KV_{bucket}: {error}"));
        let info = stream
            .info()
            .await
            .unwrap_or_else(|error| panic!("inspect KV_{bucket}: {error}"));
        assert_eq!(
            info.config.max_consumers, 100_000,
            "KV_{bucket} must override the NATS server default consumer cap"
        );
    }
}

#[tokio::test]
async fn assignment_bucket_accepts_more_than_one_thousand_ordered_consumers() {
    let Some(url) = common::nats_url().await else {
        return;
    };
    let manager = NatsBus::connect(&url).await.expect("connect manager");
    let stream = manager
        .jetstream()
        .get_stream("KV_MINGA_ASSIGN")
        .await
        .expect("assignment stream");
    let client = async_nats::connect(url.as_ref().as_ref())
        .await
        .expect("connect consumer client");
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_nanos();

    // Keep each consumer live until all are created, matching the number of
    // long-lived worker watches that used to reach the server's default cap.
    let mut consumers = Vec::with_capacity(1_001);
    for index in 0..1_001 {
        let inbox = client.new_inbox();
        let consumer = stream
            .create_consumer(async_nats::jetstream::consumer::push::OrderedConfig {
                name: Some(format!("minga-cap-{run_id}-{index}")),
                deliver_subject: inbox,
                filter_subject: "$KV.MINGA_ASSIGN.>".to_string(),
                ..Default::default()
            })
            .await
            .unwrap_or_else(|error| panic!("create ordered consumer {index}: {error}"));
        consumers.push(consumer);
    }
    assert_eq!(consumers.len(), 1_001);
}
