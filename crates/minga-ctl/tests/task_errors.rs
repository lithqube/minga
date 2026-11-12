//! Why a task failed, as `service ps`, `stack ps` and `service inspect` show
//! it, against a real API server over a state store that holds failed tasks.

mod common;

use common::{ctl, service, Cluster};
use minga_state::types::TaskState;
use serde_json::{json, Value};

const LONG_ERROR: &str = "failed to start container: ctr pull registry.example.com/shop/web:2 failed (exit status: 1): not found";

/// Runs `minga-ctl` off the async runtime, which keeps serving the API.
async fn run(cluster: &Cluster, args: &[&str]) -> (i32, String, String) {
    let url = cluster.url.clone();
    let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
    tokio::task::spawn_blocking(move || {
        let home = tempfile::tempdir().unwrap();
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        ctl(home.path(), &url, &args)
    })
    .await
    .unwrap()
}

/// `shop.web` with two tasks: one exited with code 1 for good, one failed
/// to start and is being retried. Returns the IDs `(exited, retrying)`.
async fn cluster_with_failed_tasks() -> (Cluster, String, String) {
    let cluster = common::cluster().await;
    let mut web = service(
        "shop.web",
        "shop",
        json!({ "Replicated": { "Replicas": 2 } }),
        json!({ "Healthcheck": { "Test": ["CMD", "true"], "Interval": 5_000_000_000u64 } }),
    );
    web["UpdateConfig"] =
        json!({ "Parallelism": 1, "FailureAction": "pause", "Order": "stop-first" });
    web["EndpointSpec"] =
        json!({ "Ports": [{ "Protocol": "tcp", "TargetPort": 80, "PublishedPort": 8080 }] });
    cluster.post("services/create", web).await;
    cluster.wait_for_tasks(2).await;

    let mut tasks = cluster.store.list_tasks().await.unwrap();
    tasks.sort_by(|a, b| a.id.cmp(&b.id));
    let mut exited = tasks[0].clone();
    exited.state = TaskState::Failed;
    exited.restarts = 3;
    exited.last_error = Some("exited with code 1".into());
    exited.last_exit_code = Some(1);
    let mut retrying = tasks[1].clone();
    retrying.state = TaskState::Starting;
    retrying.restarts = 1;
    retrying.last_error = Some(LONG_ERROR.into());
    cluster.store.upsert_task(exited.clone()).await.unwrap();
    cluster.store.upsert_task(retrying.clone()).await.unwrap();
    (cluster, exited.id, retrying.id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ps_shows_the_error_last_and_hints_at_what_to_run() {
    let (cluster, exited, retrying) = cluster_with_failed_tasks().await;

    let (code, out, err) = run(&cluster, &["service", "ps", "shop.web"]).await;
    assert_eq!(code, 0, "{err}");
    let header: Vec<&str> = out.lines().next().unwrap().split_whitespace().collect();
    assert_eq!(
        header,
        ["ID", "SERVICE", "NODE", "DESIRED", "STATE", "HEALTH", "ERROR"]
    );
    let row = |id: &str| out.lines().find(|l| l.starts_with(id)).unwrap();
    // The columns scripts already read are where they were.
    let cells: Vec<&str> = row(&exited).split_whitespace().collect();
    assert_eq!(&cells[..2], [exited.as_str(), "shop.web"]);
    assert_eq!(cells[4], "failed");
    assert!(row(&exited).ends_with("exited with code 1"), "{out}");
    // A long error is cut in the table.
    let cut = row(&retrying).split("  ").last().unwrap().trim();
    assert_eq!(cut.chars().count(), 50, "{cut}");
    assert!(cut.ends_with('…'), "{cut}");
    assert!(LONG_ERROR.starts_with(cut.trim_end_matches('…')), "{cut}");
    assert_eq!(
        err.trim(),
        "2 task(s) not running: see `minga-ctl service ps shop.web` and `minga-ctl service logs shop.web`"
    );

    // JSON keeps the whole text and the exit code.
    let (code, out, err) = run(&cluster, &["--json", "stack", "ps", "shop"]).await;
    assert_eq!(code, 0, "{err}");
    let tasks: Value = serde_json::from_str(&out).unwrap();
    let find = |id: &str| {
        tasks
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["id"] == id)
            .unwrap()
    };
    assert_eq!(find(&exited)["error"], "exited with code 1");
    assert_eq!(find(&exited)["exit_code"], 1);
    assert_eq!(find(&retrying)["error"], LONG_ERROR);
    assert_eq!(find(&retrying)["exit_code"], Value::Null);
    assert!(err.contains("2 task(s) not running"), "{err}");

    // Quiet prints IDs only, with no hint.
    let (_, out, err) = run(&cluster, &["--quiet", "stack", "ps", "shop"]).await;
    assert_eq!(out.lines().count(), 2, "{out}");
    assert_eq!(err, "");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_healthy_service_has_no_error_and_no_hint() {
    let cluster = common::cluster().await;
    cluster
        .post(
            "services/create",
            service(
                "shop.db",
                "shop",
                json!({ "Replicated": { "Replicas": 1 } }),
                json!({}),
            ),
        )
        .await;
    cluster.wait_for_tasks(1).await;
    let mut task = cluster.store.list_tasks().await.unwrap().remove(0);
    task.state = TaskState::Running;
    cluster.store.upsert_task(task).await.unwrap();

    let (code, out, err) = run(&cluster, &["service", "ps", "shop.db"]).await;
    assert_eq!(code, 0, "{err}");
    assert!(out.lines().nth(1).unwrap().ends_with(" -"), "{out}");
    assert_eq!(err, "");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspect_shows_the_spec_and_why_each_task_is_down() {
    let (cluster, exited, retrying) = cluster_with_failed_tasks().await;

    let (code, out, err) = run(&cluster, &["service", "inspect", "shop.web"]).await;
    assert_eq!(code, 0, "{err}");
    for expected in [
        "Service:      shop.web",
        "Image:        nginx:1.27",
        "Replicas:     2",
        "Ports:        8080->80/tcp",
        "parallelism 1",
        "order stop-first",
        "CMD true (interval 5s",
    ] {
        assert!(out.contains(expected), "missing `{expected}` in:\n{out}");
    }
    let header = out.lines().find(|l| l.starts_with("ID")).unwrap();
    assert_eq!(
        header.split_whitespace().collect::<Vec<_>>(),
        ["ID", "NODE", "DESIRED", "STATE", "RESTARTS", "EXIT", "ERROR"]
    );
    let row = |id: &str| out.lines().find(|l| l.starts_with(id)).unwrap();
    assert!(row(&exited).contains(" failed "), "{out}");
    assert!(row(&exited).ends_with("exited with code 1"), "{out}");
    let cells: Vec<&str> = row(&exited).split_whitespace().collect();
    assert_eq!((cells[4], cells[5]), ("3", "1"), "{out}");
    // The whole error, not the table's cut.
    assert!(row(&retrying).ends_with(LONG_ERROR), "{out}");

    let (code, out, err) = run(&cluster, &["--json", "service", "inspect", "shop.web"]).await;
    assert_eq!(code, 0, "{err}");
    let value: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(value["service"]["image"], "nginx:1.27");
    assert_eq!(value["service"]["replicas"], 2);
    assert_eq!(value["service"]["ports"][0]["published"], 8080);
    assert_eq!(value["service"]["update"]["failure_action"], "pause");
    assert_eq!(value["service"]["healthcheck"]["interval_secs"], 5);
    let task = value["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["id"] == exited.as_str())
        .unwrap();
    assert_eq!(task["restarts"], 3);
    assert_eq!(task["exit_code"], 1);
    assert_eq!(task["error"], "exited with code 1");

    let (code, _, err) = run(&cluster, &["service", "inspect", "shop.nope"]).await;
    assert_eq!(code, 3, "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_docker_api_reports_the_reason_on_the_task() {
    let (cluster, exited, _) = cluster_with_failed_tasks().await;
    let task: Value = reqwest::Client::new()
        .get(format!("{}/v1.41/tasks/{exited}", cluster.url))
        .bearer_auth(common::TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(task["Status"]["Err"], "exited with code 1");
    assert_eq!(task["Status"]["Message"], "exited with code 1");
    assert_eq!(task["Status"]["ContainerStatus"]["ExitCode"], 1);
}
