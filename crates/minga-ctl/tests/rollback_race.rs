//! `rollback` stops when the service changes between its two reads (plan
//! section 5.3, step 4). Runs the library in-process so the confirmation
//! step can change the service the way a second operator would. Its own test
//! binary, because it sets `XDG_STATE_HOME` for the whole process.

mod common;

use minga_ctl::api::ApiClient;
use minga_ctl::cmd::service;
use minga_ctl::output::Format;
use minga_ctl::state;
use minga_ctl::CtlError;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rollback_stops_when_the_service_changes_while_confirming() {
    let state_home = tempfile::tempdir().unwrap();
    std::env::set_var("XDG_STATE_HOME", state_home.path());

    let cluster = common::cluster().await;
    cluster
        .post(
            "services/create",
            common::service(
                "shop.web",
                "shop",
                json!({ "Replicated": { "Replicas": 2 } }),
                json!({}),
            ),
        )
        .await;
    let client = ApiClient::new(
        &cluster.url,
        Some(common::TOKEN.to_string()),
        None,
        "default",
        false,
    )
    .unwrap();

    // A saved revision that differs from the running spec.
    let running: serde_json::Value = client.get("services/shop.web", None).await.unwrap();
    let mut saved = running["Spec"].clone();
    saved["Mode"]["Replicated"]["Replicas"] = json!(1);
    state::save_revision("default", "shop.web", &saved).unwrap();
    let before = state::load_revision("default", "shop.web").unwrap();

    // While "the operator confirms", someone else scales the service.
    let other = tempfile::tempdir().unwrap();
    let url = cluster.url.clone();
    let result = service::rollback(&client, "shop.web", Format::Table, true, |summary| {
        assert!(summary.contains("replicas: 2 -> 1"), "{summary}");
        let (code, _, err) = tokio::task::block_in_place(|| {
            common::ctl(
                other.path(),
                &url,
                &["service", "scale", "shop.web", "--replicas", "5"],
            )
        });
        assert_eq!(code, 0, "{err}");
        Ok(())
    })
    .await;

    match result {
        Err(err @ CtlError::Halted(_)) => {
            assert_eq!(err.exit_code(), 2);
            assert_eq!(
                err.to_string(),
                "service `shop.web` changed while waiting for confirmation; run rollback again"
            );
        }
        other => panic!("expected the rollback to stop, got {other:?}"),
    }
    // The concurrent change stands and the revision is untouched.
    assert_eq!(cluster.stored("shop.web").await.unwrap().replicas, 5);
    assert_eq!(state::load_revision("default", "shop.web").unwrap(), before);
}
