//! Changing the lease TTL updates the existing bucket in place: the config
//! read back from the server is sent again (nats-server 2.12+ rejects unknown
//! request fields), and a shorter TTL must shrink the duplicate window,
//! which the server refuses to keep longer than max age.
//!
//! Set MINGA_TEST_NATS_URL to run against a local JetStream server of the
//! version under test; otherwise a Docker image is used.

use minga_bus::NatsBus;
use std::time::Duration;
use testcontainers::{core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};

#[tokio::test]
async fn reconnecting_with_a_new_lease_ttl_updates_the_bucket() {
    // Raise, then lower, the TTL. Lowering is the case that used to fail.
    let Some((url, _container)) = nats_url().await else {
        return;
    };
    for ttl in [3, 7, 2] {
        NatsBus::connect_with_ttl(&url, Duration::from_secs(ttl))
            .await
            .unwrap_or_else(|e| panic!("connect with a {ttl}s lease TTL: {e}"));
        let config = lease_stream_config(&url).await;
        assert_eq!(config.max_age, Duration::from_secs(ttl));
        // A shared test server may hold the bucket from an earlier run, so
        // check the invariant, not an exact window.
        assert!(config.duplicate_window <= config.max_age, "{config:?}");
    }
}

async fn lease_stream_config(url: &str) -> async_nats::jetstream::stream::Config {
    let client = async_nats::connect(url).await.expect("connect");
    let js = async_nats::jetstream::new(client);
    let mut stream = js
        .get_stream("KV_MINGA_LEASES")
        .await
        .expect("lease stream");
    stream.info().await.expect("stream info").config.clone()
}

/// The URL of a JetStream server, plus the container keeping it alive, or
/// `None` (skip) when neither MINGA_TEST_NATS_URL nor Docker is available.
async fn nats_url() -> Option<(String, Option<testcontainers::ContainerAsync<GenericImage>>)> {
    if let Ok(url) = std::env::var("MINGA_TEST_NATS_URL") {
        Some((url, None))
    } else {
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
}
