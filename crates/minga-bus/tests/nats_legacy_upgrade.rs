//! Upgrading a cluster from before tenant-first bus names: the events stream
//! moves to the new subjects and keys in the old layout are purged.
//!
//! Each test reconfigures shared streams and buckets, so they run one at a
//! time; they live apart from the lease-TTL test, which a manager connect
//! with the default TTL would disturb.
mod common;

use minga_bus::{EventBus, NatsBus};

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn connecting_moves_a_pre_tenant_events_stream_to_tenant_subjects() {
    let _serial = SERIAL.lock().await;
    let Some(url) = common::nats_url().await else {
        return;
    };
    let url: &str = &url;
    let client = async_nats::connect(url).await.expect("connect");
    let js = async_nats::jetstream::new(client);
    let _ = js.delete_stream("MINGA_EVENTS").await;
    js.create_stream(async_nats::jetstream::stream::Config {
        name: "MINGA_EVENTS".to_string(),
        subjects: vec![
            "minga.node.>".to_string(),
            "minga.task.>".to_string(),
            "minga.egress.>".to_string(),
        ],
        ..Default::default()
    })
    .await
    .expect("create pre-tenant events stream");

    let bus = NatsBus::connect(url).await.expect("connect bus");
    bus.publish_event("minga.default.task.upgrade.event", b"after-upgrade")
        .await
        .expect("tenant-first publish after upgrade");

    let mut stream = js.get_stream("MINGA_EVENTS").await.expect("events");
    let subjects = stream.info().await.expect("info").config.subjects.clone();
    assert_eq!(
        subjects,
        ["minga.*.node.>", "minga.*.task.>", "minga.*.egress.>"]
    );
}

#[tokio::test]
async fn connecting_a_manager_purges_pre_tenant_keys_only() {
    let _serial = SERIAL.lock().await;
    let Some(url) = common::nats_url().await else {
        return;
    };
    let url: &str = &url;
    // Create the buckets, then plant keys in the old and new layouts.
    NatsBus::connect(url).await.expect("first connect");
    let client = async_nats::connect(url).await.expect("connect");
    let js = async_nats::jetstream::new(client);
    let assign = js.get_key_value("MINGA_ASSIGN").await.expect("assign");
    let status = js.get_key_value("MINGA_STATUS").await.expect("status");
    for key in ["node.41.task.old", "node.41.wg"] {
        assign.put(key, "old".into()).await.unwrap();
    }
    status.put("node.41.task.old", "old".into()).await.unwrap();
    assign
        .put("default.node.41.task.new", "new".into())
        .await
        .unwrap();
    assign
        .put("node.node.41.task.tenant-named-node", "new".into())
        .await
        .unwrap();
    status
        .put("internal.node-join.41", "default".into())
        .await
        .unwrap();

    NatsBus::connect(url).await.expect("reconnect");

    for key in ["node.41.task.old", "node.41.wg"] {
        assert!(assign.get(key).await.unwrap().is_none(), "{key} kept");
    }
    assert!(status.get("node.41.task.old").await.unwrap().is_none());
    for key in [
        "default.node.41.task.new",
        "node.node.41.task.tenant-named-node",
    ] {
        assert!(assign.get(key).await.unwrap().is_some(), "{key} purged");
    }
    assert!(status.get("internal.node-join.41").await.unwrap().is_some());
}
