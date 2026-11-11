//! `ApiClient` write and plain-text calls against a real `minga api` server.
//!
//! These are the calls build-order steps 4 (`post`, `delete`) and 6 (`text`)
//! build on; the read commands are covered end to end in `tests/api.rs`.

use std::collections::BTreeMap;
use std::sync::Arc;

use minga_ctl::api::{ApiClient, Found};
use minga_ctl::CtlError;
use minga_state::StateStore;
use serde_json::json;

const TOKEN: &str = "client-test-token";

async fn server() -> (minga_api::ApiServer, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
            .await
            .unwrap(),
    );
    let server =
        minga_api::ApiServer::start_with_auth(store, "127.0.0.1:0", Some(TOKEN.to_string()))
            .await
            .unwrap();
    (server, dir)
}

fn client(root: &str, token: &str) -> ApiClient {
    ApiClient::new(root, Some(token.to_string()), None, "test", false).unwrap()
}

#[tokio::test]
async fn post_and_delete_round_trip() {
    let (server, _dir) = server().await;
    let root = format!("http://{}", server.addr());
    let api = client(&root, TOKEN);

    api.post("secrets/create", &json!({ "Name": "shop_db" }))
        .await
        .unwrap();
    let secrets = api.get("secrets", None).await.unwrap();
    assert_eq!(secrets[0]["ID"], "shop_db", "{secrets}");

    assert!(matches!(
        api.delete("secrets/shop_db").await.unwrap(),
        Found::Yes(_)
    ));
    // Gone: a second delete is a plain 404, not an error.
    assert!(matches!(
        api.delete("secrets/shop_db").await.unwrap(),
        Found::No
    ));
}

#[tokio::test]
async fn text_reads_health_with_the_token_and_reports_status() {
    let (server, _dir) = server().await;
    let root = format!("http://{}", server.addr());

    let (status, body) = client(&root, TOKEN)
        .text(&format!("{root}/healthz"))
        .await
        .unwrap();
    assert_eq!(status, 200, "{body}");

    // A wrong token is a 401 status here: `text` returns it unchanged.
    let (status, _) = client(&root, "wrong")
        .text(&format!("{root}/healthz"))
        .await
        .unwrap();
    assert_eq!(status, 401);
}

#[tokio::test]
async fn a_wrong_token_on_post_is_auth() {
    let (server, _dir) = server().await;
    let root = format!("http://{}", server.addr());
    let err = client(&root, "wrong")
        .post("secrets/create", &json!({ "Name": "x" }))
        .await
        .unwrap_err();
    assert!(matches!(err, CtlError::Auth { .. }), "{err:?}");
}
