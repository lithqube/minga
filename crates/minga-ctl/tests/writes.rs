//! `service scale/update/rollback` and `stack rm` against a real API
//! (plan section 6, Required tests), running the built binary.

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use common::{ctl, service, without_previous, Cluster};
use serde_json::{json, Value};

fn mode(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

/// Runs `minga-ctl` off the async runtime, which keeps serving the API.
async fn run(cluster: &Cluster, home: &Path, args: &[&str]) -> (i32, String, String) {
    let (url, home) = (cluster.url.clone(), home.to_path_buf());
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        ctl(&home, &url, &args)
    })
    .await
    .unwrap()
}

fn revision_file(home: &Path, service: &str) -> PathBuf {
    home.join(format!("state/minga-ctl/revisions/default/{service}.json"))
}

fn audit_lines(home: &Path) -> Vec<Value> {
    std::fs::read_to_string(home.join("state/minga-ctl/audit.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scale_update_and_rollback_against_a_real_api() {
    let cluster = common::cluster().await;
    cluster
        .post("secrets/create", json!({ "Name": "shop_db_password" }))
        .await;
    let secrets = json!({
        "Env": ["A=1", "B=2"],
        "Secrets": [{ "SecretID": "shop_db_password", "SecretName": "shop_db_password" }],
    });
    cluster
        .post(
            "services/create",
            service(
                "shop.web",
                "shop",
                json!({ "Replicated": { "Replicas": 2 } }),
                secrets,
            ),
        )
        .await;
    cluster
        .post(
            "services/create",
            service("shop.proxy", "shop", json!({ "Global": {} }), json!({})),
        )
        .await;
    cluster.wait_for_tasks(3).await;
    let original = without_previous(cluster.stored("shop.web").await.unwrap());
    let home = tempfile::tempdir().unwrap();
    let home = home.path();
    let revision = revision_file(home, "shop.web");

    // scale: the stored service equals the original except for replicas.
    let (code, out, err) = run(
        &cluster,
        home,
        &["service", "scale", "shop.web", "--replicas", "3"],
    )
    .await;
    assert_eq!(code, 0, "{err}");
    assert_eq!(out.trim(), "shop.web scaled from 2 to 3");
    let scaled = without_previous(cluster.stored("shop.web").await.unwrap());
    assert_eq!(scaled.replicas, 3);
    assert_eq!(
        minga_state::types::Service {
            replicas: original.replicas,
            ..scaled
        },
        original
    );

    // The revision holds the spec from before the change, privately.
    let saved: Value = serde_json::from_slice(&std::fs::read(&revision).unwrap()).unwrap();
    assert_eq!(saved["service_id"], "shop.web");
    assert_eq!(saved["spec"]["Mode"]["Replicated"]["Replicas"], 2);
    assert_eq!(mode(&revision), 0o600);
    assert_eq!(mode(revision.parent().unwrap()), 0o700);
    assert_eq!(mode(&home.join("state/minga-ctl")), 0o700);
    let first_revision = std::fs::read(&revision).unwrap();

    // A no-op scale or update posts nothing and keeps the revision.
    for args in [
        &["--debug", "service", "scale", "shop.web", "--replicas", "3"][..],
        &[
            "--debug",
            "service",
            "update",
            "shop.web",
            "--replicas",
            "3",
            "--env-add",
            "A=1",
        ][..],
    ] {
        let (code, out, err) = run(&cluster, home, args).await;
        assert_eq!(code, 0, "{err}");
        assert!(out.contains("nothing changed"), "{out}");
        assert!(!err.contains("POST"), "{err}");
        assert_eq!(std::fs::read(&revision).unwrap(), first_revision);
    }

    // A scale the API rejects (the referenced secret is gone) keeps the revision.
    cluster.delete("secrets/shop_db_password").await;
    let (code, _, err) = run(
        &cluster,
        home,
        &["service", "scale", "shop.web", "--replicas", "4"],
    )
    .await;
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("not declared"), "{err}");
    assert_eq!(std::fs::read(&revision).unwrap(), first_revision);
    cluster
        .post("secrets/create", json!({ "Name": "shop_db_password" }))
        .await;

    // rollback needs a terminal or --yes.
    let (code, _, err) = run(&cluster, home, &["service", "rollback", "shop.web"]).await;
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains(
            "error: `service rollback` needs confirmation; pass --yes to run without a terminal"
        ),
        "{err}"
    );

    // scale then rollback restores the original; the revision now holds the
    // scaled spec, so a second rollback returns to it.
    let (code, out, err) = run(
        &cluster,
        home,
        &["service", "rollback", "shop.web", "--yes"],
    )
    .await;
    assert_eq!(code, 0, "{err}");
    assert_eq!(out.trim(), "shop.web rolled back");
    assert_eq!(
        without_previous(cluster.stored("shop.web").await.unwrap()),
        original
    );
    let saved: Value = serde_json::from_slice(&std::fs::read(&revision).unwrap()).unwrap();
    assert_eq!(saved["spec"]["Mode"]["Replicated"]["Replicas"], 3);

    // update: image and environment.
    let (code, out, err) = run(
        &cluster,
        home,
        &[
            "service",
            "update",
            "shop.web",
            "--image",
            "nginx:1.28",
            "--env-add",
            "C=3",
            "--env-rm",
            "A",
        ],
    )
    .await;
    assert_eq!(code, 0, "{err}");
    assert_eq!(
        out.trim(),
        "shop.web updated: image nginx:1.27 -> nginx:1.28, environment 2 keys changed"
    );
    let updated = cluster.stored("shop.web").await.unwrap();
    assert_eq!(updated.image, "nginx:1.28");
    assert_eq!(updated.env.get("C").map(String::as_str), Some("3"));
    assert!(!updated.env.contains_key("A"));
    assert_eq!(updated.env.get("B").map(String::as_str), Some("2"));

    // --json and --quiet.
    let (code, out, err) = run(
        &cluster,
        home,
        &["--json", "service", "scale", "shop.web", "--replicas", "2"],
    )
    .await;
    assert_eq!(code, 0, "{err}");
    let out: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(out["replicas"], json!({ "from": 2, "to": 2 }));
    assert_eq!(out["changed"], false);
    let (code, out, err) = run(
        &cluster,
        home,
        &["--quiet", "service", "scale", "shop.web", "--replicas", "1"],
    )
    .await;
    assert_eq!(code, 0, "{err}");
    assert_eq!(out, "");

    // A global service cannot be scaled; a service never changed has no revision.
    let (code, _, err) = run(
        &cluster,
        home,
        &["service", "scale", "shop.proxy", "--replicas", "2"],
    )
    .await;
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains("service `shop.proxy` is global and cannot be scaled"),
        "{err}"
    );
    let (code, _, err) = run(
        &cluster,
        home,
        &["service", "rollback", "shop.proxy", "--yes"],
    )
    .await;
    assert_eq!(code, 3, "{err}");
    assert!(
        err.contains("error: revision `shop.proxy` not found in context `default`"),
        "{err}"
    );
    assert!(
        err.contains("hint: rollback covers only changes made with minga-ctl on this machine"),
        "{err}"
    );
    let (code, _, err) = run(
        &cluster,
        home,
        &["service", "scale", "shop.nope", "--replicas", "2"],
    )
    .await;
    assert_eq!(code, 3, "{err}");

    // The audit log: one line per write, failures included, no env values.
    let audit = audit_lines(home);
    let commands: Vec<&str> = audit
        .iter()
        .map(|l| l["command"].as_str().unwrap())
        .collect();
    assert_eq!(
        commands,
        [
            "service scale",
            "service scale",
            "service update",
            "service scale",
            "service rollback",
            "service rollback",
            "service update",
            "service scale",
            "service scale",
            "service scale",
            "service rollback",
            "service scale",
        ]
    );
    assert_eq!(audit[0]["result"], "ok");
    assert_eq!(audit[0]["user"], "tester");
    assert_eq!(audit[0]["context"], "default");
    assert_eq!(
        audit[0]["args"],
        json!({ "service": "shop.web", "replicas": 3 })
    );
    assert_eq!(audit[3]["result"], "error");
    assert_eq!(audit[3]["exit_code"], 2);
    assert_eq!(audit[6]["args"]["env_add"], json!(["C"]));
    let text = std::fs::read_to_string(home.join("state/minga-ctl/audit.jsonl")).unwrap();
    assert!(!text.contains("C=3"), "{text}");
    assert_eq!(mode(&home.join("state/minga-ctl/audit.jsonl")), 0o600);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stack_rm_removes_services_and_only_named_resources() {
    let cluster = common::cluster().await;
    for network in ["app_default", "app_shared", "other_net"] {
        cluster
            .post(
                "networks/create",
                json!({ "Name": network, "Driver": "overlay" }),
            )
            .await;
    }
    cluster
        .post("secrets/create", json!({ "Name": "app_db_password" }))
        .await;
    let on = |networks: &[&str]| {
        json!(networks
            .iter()
            .map(|n| json!({ "Target": n }))
            .collect::<Vec<_>>())
    };
    let replicated = json!({ "Replicated": { "Replicas": 1 } });
    for (name, stack, networks) in [
        ("app.web", "app", on(&["app_default", "app_shared"])),
        ("app.db", "app", on(&["app_default"])),
        ("other.svc", "other", on(&["app_shared"])),
    ] {
        let mut spec = service(name, stack, replicated.clone(), json!({}));
        spec["TaskTemplate"]["Networks"] = networks;
        cluster.post("services/create", spec).await;
    }
    let home = tempfile::tempdir().unwrap();
    let home = home.path();
    let networks = ["app_default", "app_shared", "other_net"]
        .map(String::from)
        .to_vec();
    async fn unchanged(cluster: &Cluster) {
        assert_eq!(
            cluster.ids("networks").await,
            ["app_default", "app_shared", "other_net"]
        );
        assert_eq!(cluster.store.list_services().await.unwrap().len(), 3);
    }

    // Without a terminal and without --yes: refused, nothing removed.
    let (code, _, err) = run(&cluster, home, &["stack", "rm", "app"]).await;
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains("error: `stack rm` needs confirmation; pass --yes to run without a terminal"),
        "{err}"
    );
    unchanged(&cluster).await;

    // --with a resource that is not the stack's: refused, nothing removed.
    let (code, _, err) = run(
        &cluster,
        home,
        &[
            "stack",
            "rm",
            "app",
            "--with",
            "app_default",
            "--with",
            "other_net",
            "--yes",
        ],
    )
    .await;
    assert_eq!(code, 1, "{err}");
    assert!(err.contains("refusing --with other_net"), "{err}");
    unchanged(&cluster).await;

    // --with a network another stack's service uses: refused, nothing removed.
    let (code, _, err) = run(
        &cluster,
        home,
        &["stack", "rm", "app", "--with", "app_shared", "--yes"],
    )
    .await;
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains("network `app_shared` is used by service `other.svc` outside stack `app`"),
        "{err}"
    );
    unchanged(&cluster).await;

    // Plain rm: services only; candidates are listed, not removed.
    let (code, out, err) = run(&cluster, home, &["stack", "rm", "app", "--yes"]).await;
    assert_eq!(code, 0, "{err}");
    assert_eq!(
        out,
        "removed 2 services\n\
         left in place (ownership cannot be confirmed):\n  \
         network app_default\n  \
         network app_shared\n  \
         secret  app_db_password (metadata only)\n\
         to remove them: minga-ctl stack rm app --with app_default --with app_shared --with app_db_password\n"
    );
    let services: Vec<String> = cluster
        .store
        .list_services()
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert_eq!(services, ["other.svc"]);
    assert_eq!(cluster.ids("networks").await, networks);

    // The same command with --with removes the named network once the services are gone.
    let (code, out, err) = run(
        &cluster,
        home,
        &["stack", "rm", "app", "--with", "app_default", "--yes"],
    )
    .await;
    assert_eq!(code, 0, "{err}");
    assert!(out.starts_with("removed network app_default\n"), "{out}");
    assert_eq!(cluster.ids("networks").await, ["app_shared", "other_net"]);

    // Nothing left that carries the label and nothing named: exit 3.
    let (code, _, err) = run(&cluster, home, &["stack", "rm", "app", "--yes"]).await;
    assert_eq!(code, 3, "{err}");

    let audit = audit_lines(home);
    assert_eq!(audit.len(), 6);
    assert!(audit.iter().all(|l| l["command"] == "stack rm"));
    assert_eq!(audit[3]["result"], "ok");
    assert_eq!(
        audit[1]["args"]["with"],
        json!(["app_default", "other_net"])
    );
}
