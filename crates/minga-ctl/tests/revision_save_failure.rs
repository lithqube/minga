//! When the API accepts a change but the revision cannot be saved, the old
//! revision is removed: it would roll back two changes. In-process, in its
//! own test binary, because it sets `XDG_STATE_HOME` for the whole process
//! and blocks the atomic write through the temporary file name, which
//! carries this process's ID.

mod common;

use minga_ctl::api::ApiClient;
use minga_ctl::cmd::service::{self, ServiceCommand};
use minga_ctl::output::Format;
use minga_ctl::state;
use minga_ctl::transport::Connection;
use minga_ctl::CtlError;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_revision_save_removes_the_old_revision() {
    let state_home = tempfile::tempdir().unwrap();
    std::env::set_var("XDG_STATE_HOME", state_home.path());

    let cluster = common::cluster().await;
    cluster
        .post(
            "services/create",
            common::service(
                "shop.api",
                "shop",
                json!({ "Replicated": { "Replicas": 1 } }),
                json!({}),
            ),
        )
        .await;
    let api = ApiClient::new(
        &cluster.url,
        Some(common::TOKEN.to_string()),
        None,
        "default",
        false,
    )
    .unwrap();

    // An earlier revision, then a directory where the next write's temporary
    // file has to go, so the write fails after the POST.
    let running: serde_json::Value = api.get("services/shop.api", None).await.unwrap();
    let mut older = running["Spec"].clone();
    older["Mode"]["Replicated"]["Replicas"] = json!(7);
    state::save_revision("default", "shop.api", &older).unwrap();
    let path = state::revision_path("default", "shop.api").unwrap();
    let tmp = path.with_file_name(format!("shop.api.json.tmp.{}", std::process::id()));
    std::fs::create_dir(&tmp).unwrap();

    let conn = Connection::direct(api, cluster.url.clone());
    let scale = ServiceCommand::Scale {
        service: "shop.api".to_string(),
        replicas: 2,
    };
    let resolved = minga_ctl::config::resolve(&Default::default(), None).unwrap();
    let result = service::run(&scale, &conn, &resolved, false, Format::Table, true).await;

    match result {
        Err(err @ CtlError::Halted(_)) => {
            let text = err.to_string();
            assert_eq!(err.exit_code(), 2);
            assert!(text.contains("its revision could not be saved"), "{text}");
            assert!(
                text.contains(
                    "the previous revision was removed, so rollback has nothing to restore"
                ),
                "{text}"
            );
        }
        other => panic!("expected the save to fail, got {other:?}"),
    }
    // The change went through; no stale revision is left to roll back to.
    assert_eq!(cluster.stored("shop.api").await.unwrap().replicas, 2);
    assert_eq!(state::load_revision("default", "shop.api").unwrap(), None);
}
