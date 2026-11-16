//! Verifies that JetStream server-side expiry, not client clock comparison,
//! determines node liveness.

mod common;

use futures::StreamExt;
use minga_bus::{EventBus, NatsBus};
use std::time::Duration;

#[tokio::test]
async fn jetstream_expiry_makes_a_node_ineligible() {
    let Some(url) = common::nats_url().await else {
        return;
    };
    let bus = NatsBus::connect_with_ttl(&url, Duration::from_secs(2))
        .await
        .expect("connect to NATS");

    // This opaque payload has no timestamp. The server owns lease expiry.
    bus.refresh_lease(91, b"registration payload")
        .await
        .unwrap();
    assert!(bus.is_alive(91).await.unwrap());

    let mut leases = bus.watch_leases().await.unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(1), leases.next())
        .await
        .expect("lease watch did not replay the current entry")
        .expect("lease watch ended");
    assert_eq!(snapshot.key, "lease.node.91");
    assert_eq!(
        snapshot.value.as_deref(),
        Some(b"registration payload".as_slice())
    );

    let expiry_deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    while bus.is_alive(91).await.unwrap() {
        assert!(
            tokio::time::Instant::now() < expiry_deadline,
            "JetStream did not expire the lease"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!bus.list_alive_nodes().await.unwrap().contains(&91));
}
