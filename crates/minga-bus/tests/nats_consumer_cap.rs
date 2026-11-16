//! nats-server 2.15 caps a stream at 1,000 consumers unless the stream sets
//! `max_consumers`. Every worker holds two watches on `KV_MINGA_ASSIGN`, so
//! the cap would stop new workers from watching their assignments at about
//! 500 workers. The buckets Minga creates must not be capped.
//!
//! Set MINGA_TEST_NATS_URL to run against a local JetStream server of the
//! version under test; otherwise a Docker image is used.

use std::time::Duration;

use async_nats::jetstream::consumer::pull;
use minga_bus::{EventBus, NatsBus};
use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};

/// One more consumer than nats-server 2.15's default per-stream cap.
const OVER_DEFAULT_CAP: usize = 1_001;

#[tokio::test]
async fn assignment_bucket_accepts_more_consumers_than_the_server_default() {
    let Some((url, _container)) = nats_url().await else {
        return;
    };
    let bus = NatsBus::connect(&url).await.expect("connect to NATS");

    let client = async_nats::connect(&url).await.expect("connect");
    let js = async_nats::jetstream::new(client);
    for bucket in ["KV_MINGA_ASSIGN", "KV_MINGA_STATUS", "KV_MINGA_LEASES"] {
        let mut stream = js.get_stream(bucket).await.expect("bucket stream");
        let config = stream.info().await.expect("stream info").config.clone();
        // -1 means "server default" on 2.15, which is the 1,000 cap.
        assert!(
            config.max_consumers > OVER_DEFAULT_CAP as i32,
            "{bucket} keeps the server's default consumer cap: {}",
            config.max_consumers
        );
    }

    let stream = js
        .get_stream("KV_MINGA_ASSIGN")
        .await
        .expect("assign stream");
    let mut consumers = Vec::with_capacity(OVER_DEFAULT_CAP);
    for i in 0..OVER_DEFAULT_CAP {
        let consumer = stream
            .create_consumer(pull::Config {
                inactive_threshold: Duration::from_secs(60),
                ..Default::default()
            })
            .await
            .unwrap_or_else(|e| panic!("consumer {i} on KV_MINGA_ASSIGN: {e}"));
        consumers.push(consumer);
    }
    // A worker's watch is one more consumer on the same stream.
    let _watch = bus
        .watch_assignments("")
        .await
        .expect("a watch still opens past the server's default cap");
}

/// The URL of a JetStream server, plus the container keeping it alive, or
/// `None` (skip) when neither MINGA_TEST_NATS_URL nor Docker is available.
async fn nats_url() -> Option<(String, Option<testcontainers::ContainerAsync<GenericImage>>)> {
    if let Ok(url) = std::env::var("MINGA_TEST_NATS_URL") {
        return Some((url, None));
    }
    let image = GenericImage::new("nats", "2.15.3-alpine")
        .with_exposed_port(4222.into())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(vec!["-js"]);
    let container = match image.start().await {
        Ok(container) => container,
        Err(error) => {
            eprintln!("skipping: cannot start NATS container: {error}");
            return None;
        }
    };
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("resolve NATS client port");
    Some((format!("nats://127.0.0.1:{port}"), Some(container)))
}
