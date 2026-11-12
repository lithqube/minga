//! `stack deploy --prune`, `service rm` and the task output in `service
//! inspect`, against a real API server, a stub `minga` and a stub `ssh`.

mod common;

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};

use common::{cluster, ctl_with, service, Cluster, TOKEN};
use minga_state::types::TaskState;
use serde_json::{json, Value};

/// Stub `minga`: records each call; `logs` prints a line on each stream.
/// `STUB_EXIT` is the exit code of every call.
const MINGA: &str = r#"#!/bin/sh
echo "$@" >> "$STUB_DIR/calls"
if [ "$1" = logs ]; then
  echo "boot line"
  echo "oops" >&2
  # One log entry can hold many lines, so `--tail` may return more than asked.
  [ -n "$STUB_LINES" ] && seq 1 "$STUB_LINES" | sed 's/^/line /'
fi
exit "${STUB_EXIT:-0}"
"#;

/// Stub `ssh`: `-N` is the tunnel; `'cat'` reads the token; anything else is
/// a node command, and the node is down.
const SSH: &str = r#"#!/bin/sh
d="$STUB_DIR"
if [ "$1" = "-N" ]; then
  printf '%s\n' "$@" > "$d/tunnel.tmp" && mv "$d/tunnel.tmp" "$d/tunnel.args"
  exec sleep 30
fi
if [ "$4" = "'cat'" ]; then
  printf '%s' "$STUB_TOKEN"
  exit 0
fi
echo "$@" >> "$d/ssh.calls"
echo "ssh: connect to host mgr1.example.net port 22: Connection refused" >&2
exit 255
"#;

struct Env {
    dir: tempfile::TempDir,
}

impl Env {
    fn new() -> Env {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        for (name, script) in [("minga", MINGA), ("ssh", SSH)] {
            let path = bin.join(name);
            std::fs::write(&path, script).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        Env { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn read(&self, name: &str) -> Option<String> {
        std::fs::read_to_string(self.path(name)).ok()
    }

    /// Runs `minga-ctl` against `cluster` with the stub `minga`, off the
    /// async runtime, which keeps serving the API.
    async fn run(
        &self,
        cluster: &Cluster,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> (i32, String, String) {
        self.run_at(&cluster.url, args, env).await
    }

    async fn run_at(
        &self,
        url: &str,
        args: &[&str],
        env: &[(&str, &str)],
    ) -> (i32, String, String) {
        let (home, url) = (self.dir.path().to_path_buf(), url.to_string());
        let minga = self.path("bin/minga").display().to_string();
        let dir = home.display().to_string();
        let args: Vec<String> = args.iter().map(|a| a.to_string()).collect();
        let env: Vec<(String, String)> =
            [("MINGA_BIN", minga.as_str()), ("STUB_DIR", dir.as_str())]
                .into_iter()
                .chain(env.iter().copied())
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
        tokio::task::spawn_blocking(move || {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let env: Vec<(&str, &str)> =
                env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            ctl_with(&home, &url, &args, &env)
        })
        .await
        .unwrap()
    }

    /// The `minga` calls so far, one line each.
    fn calls(&self) -> Vec<String> {
        self.read("calls")
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    fn audit(&self) -> Vec<Value> {
        self.read("state/minga-ctl/audit.jsonl")
            .unwrap_or_default()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn revision(&self, id: &str) -> Option<Value> {
        self.read(&format!("state/minga-ctl/revisions/default/{id}.json"))
            .map(|t| serde_json::from_str(&t).unwrap())
    }

    fn compose(&self, text: &str) -> String {
        let path = self.path("compose.yaml");
        std::fs::write(&path, text).unwrap();
        path.display().to_string()
    }
}

fn spec(name: &str, stack: &str, image: &str) -> Value {
    service(
        name,
        stack,
        json!({ "Replicated": { "Replicas": 1 } }),
        json!({ "Image": image }),
    )
}

/// `app.web`, `app.old` and look-alikes in other stacks, one of whose names
/// starts with `app`.
async fn stacks() -> Cluster {
    let cluster = cluster().await;
    for (name, stack) in [
        ("app.web", "app"),
        ("app.old", "app"),
        ("appx.old", "appx"),
        ("other.old", "other"),
    ] {
        cluster
            .post("services/create", spec(name, stack, &format!("{name}:1")))
            .await;
    }
    cluster
}

async fn names(cluster: &Cluster) -> Vec<String> {
    let mut ids: Vec<String> = cluster
        .store
        .list_services()
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id)
        .collect();
    ids.sort();
    ids
}

const FILE: &str = "services:\n  web:\n    image: web:2\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_prune_a_note_names_the_services_the_file_lacks() {
    let (env, cluster) = (Env::new(), stacks().await);
    let file = env.compose(FILE);
    let (code, _, err) = env
        .run(&cluster, &["stack", "deploy", "-c", &file, "app"], &[])
        .await;
    assert_eq!(code, 0, "{err}");
    assert_eq!(
        err.trim(),
        format!("note: app.old in stack app but not in {file}; --prune removes it")
    );
    assert_eq!(env.calls().len(), 1, "minga deploy ran once");
    assert_eq!(
        names(&cluster).await,
        ["app.old", "app.web", "appx.old", "other.old"]
    );

    let (_, _, err) = env
        .run(
            &cluster,
            &["--quiet", "stack", "deploy", "-c", &file, "app"],
            &[],
        )
        .await;
    assert_eq!(err, "", "--quiet drops the note");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prune_removes_only_this_stacks_absent_services_and_rollback_brings_one_back() {
    let (env, cluster) = (Env::new(), stacks().await);
    let file = env.compose(FILE);
    let deploy = ["stack", "deploy", "-c", &file, "--prune", "app"];

    // No terminal and no --yes: refused before anything is deployed.
    let (code, _, err) = env.run(&cluster, &deploy, &[]).await;
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains("`stack deploy --prune` needs confirmation"),
        "{err}"
    );
    assert!(env.calls().is_empty(), "nothing was deployed");
    assert_eq!(names(&cluster).await.len(), 4);

    // A failed deploy prunes nothing.
    let args = [&deploy[..], &["--yes"]].concat();
    let (code, _, err) = env.run(&cluster, &args, &[("STUB_EXIT", "1")]).await;
    assert_eq!(code, 2, "{err}");
    assert_eq!(names(&cluster).await.len(), 4);

    let (code, out, err) = env.run(&cluster, &args, &[]).await;
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("pruned app.old"), "{out}");
    assert_eq!(names(&cluster).await, ["app.web", "appx.old", "other.old"]);

    // Its spec is its revision; the audit line records the flags.
    let revision = env
        .revision("app.old")
        .expect("revision of the pruned service");
    assert_eq!(
        revision["spec"]["TaskTemplate"]["ContainerSpec"]["Image"],
        "app.old:1"
    );
    let audit = env.audit();
    let last = audit.last().unwrap();
    assert_eq!(
        (last["command"].as_str(), last["result"].as_str()),
        (Some("stack deploy"), Some("ok"))
    );
    assert_eq!(
        (&last["args"]["prune"], &last["args"]["yes"]),
        (&json!(true), &json!(true))
    );

    let (code, out, err) = env
        .run(&cluster, &["service", "rollback", "app.old", "--yes"], &[])
        .await;
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("app.old recreated"), "{out}");
    let back = cluster.stored("app.old").await.expect("recreated");
    assert_eq!(back.image, "app.old:1");

    // Nothing left to prune: no prompt, so no --yes is needed.
    let (code, _, err) = env
        .run(
            &cluster,
            &[
                "stack",
                "deploy",
                "-c",
                &env.compose("services:\n  web: {image: web:2}\n  old: {image: o:1}\n"),
                "--prune",
                "app",
            ],
            &[],
        )
        .await;
    assert_eq!(code, 0, "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prune_refuses_a_file_without_services() {
    let (env, cluster) = (Env::new(), stacks().await);
    let file = env.compose("services: {}\n");
    let (code, _, err) = env
        .run(
            &cluster,
            &["stack", "deploy", "-c", &file, "--prune", "--yes", "app"],
            &[],
        )
        .await;
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains("has no services; refusing to prune the whole stack"),
        "{err}"
    );
    assert!(env.calls().is_empty());
    assert_eq!(names(&cluster).await.len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_rm_confirms_saves_the_revision_and_deletes() {
    let (env, cluster) = (Env::new(), stacks().await);

    // Declined (no terminal, no --yes): nothing changes.
    let (code, _, err) = env.run(&cluster, &["service", "rm", "app.old"], &[]).await;
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains("`service rm` needs confirmation; pass --yes"),
        "{err}"
    );
    assert!(cluster.stored("app.old").await.is_some());
    assert!(env.revision("app.old").is_none());

    let (code, out, err) = env
        .run(&cluster, &["service", "rm", "app.old", "--yes"], &[])
        .await;
    assert_eq!(code, 0, "{err}");
    assert!(out.contains("app.old removed"), "{out}");
    assert_eq!(names(&cluster).await, ["app.web", "appx.old", "other.old"]);
    let revision = env.revision("app.old").expect("revision");
    assert_eq!(
        revision["spec"]["TaskTemplate"]["ContainerSpec"]["Image"],
        "app.old:1"
    );
    let audit = env.audit();
    let last = audit.last().unwrap();
    assert_eq!(last["command"], "service rm");
    assert_eq!(last["args"], json!({ "service": "app.old", "yes": true }));
    assert_eq!(last["result"], "ok");

    // A service that is not there: exit 3, audited as a failure.
    let (code, _, err) = env
        .run(&cluster, &["service", "rm", "app.old", "--yes"], &[])
        .await;
    assert_eq!(code, 3, "{err}");
    assert_eq!(env.audit().last().unwrap()["exit_code"], 3);

    let (code, _, err) = env
        .run(&cluster, &["service", "rollback", "app.old", "--yes"], &[])
        .await;
    assert_eq!(code, 0, "{err}");
    assert!(cluster.stored("app.old").await.is_some());
}

/// `shop.web` with a running task and a failed one. Returns the IDs
/// `(running, failed)`.
async fn with_a_failed_task() -> (Cluster, String, String) {
    let cluster = cluster().await;
    cluster
        .post(
            "services/create",
            service(
                "shop.web",
                "shop",
                json!({ "Replicated": { "Replicas": 2 } }),
                json!({}),
            ),
        )
        .await;
    cluster.wait_for_tasks(2).await;
    let mut tasks = cluster.store.list_tasks().await.unwrap();
    tasks.sort_by(|a, b| a.id.cmp(&b.id));
    let mut running = tasks[0].clone();
    running.state = TaskState::Running;
    let mut failed = tasks[1].clone();
    failed.state = TaskState::Failed;
    failed.last_error = Some("exited with code 1".into());
    cluster.store.upsert_task(running.clone()).await.unwrap();
    cluster.store.upsert_task(failed.clone()).await.unwrap();
    (cluster, running.id, failed.id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspect_shows_the_end_of_each_failed_tasks_output() {
    let (env, (cluster, running, failed)) = (Env::new(), with_a_failed_task().await);

    let (code, out, err) = env
        .run(&cluster, &["service", "inspect", "shop.web"], &[])
        .await;
    assert_eq!(code, 0, "{err}");
    assert!(
        out.contains(&format!(
            "{failed} (failed): last 20 lines of output\n  [stdout] boot line\n  [stderr] oops"
        )),
        "{out}"
    );
    assert!(!out.contains(&format!("{running} (running)")), "{out}");
    // Only the task that is down is asked, with the default tail.
    assert_eq!(
        env.calls(),
        [format!(
            "logs {failed} --tail 20 --nats-url nats://127.0.0.1:4222"
        )]
    );

    let (code, out, _) = env
        .run(
            &cluster,
            &["service", "inspect", "shop.web", "--tail", "3"],
            &[],
        )
        .await;
    assert_eq!(code, 0);
    assert!(out.contains("last 3 lines of output"), "{out}");
    assert!(env.calls()[1].contains("--tail 3"));

    // `--tail` counts lines even when the node returns more.
    let (_, out, _) = env
        .run(
            &cluster,
            &["service", "inspect", "shop.web", "--tail", "3"],
            &[("STUB_LINES", "10")],
        )
        .await;
    assert!(
        out.contains("[stdout] line 10") && out.contains("[stdout] line 8"),
        "{out}"
    );
    assert!(
        !out.contains("line 7") && !out.contains("boot line"),
        "{out}"
    );

    // JSON keeps its fields and gains `logs` on the tasks that are down.
    let (code, out, err) = env
        .run(&cluster, &["--json", "service", "inspect", "shop.web"], &[])
        .await;
    assert_eq!(code, 0, "{err}");
    let value: Value = serde_json::from_str(&out).unwrap();
    let task = |id: &str| {
        value["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["id"] == id)
            .unwrap()
            .clone()
    };
    assert_eq!(task(&failed)["error"], "exited with code 1");
    assert_eq!(
        task(&failed)["logs"],
        json!({ "stdout": ["boot line"], "stderr": ["oops"], "error": null })
    );
    assert!(task(&running).get("logs").is_none());

    // Quiet prints the IDs and contacts no node.
    let before = env.calls().len();
    let (_, out, _) = env
        .run(
            &cluster,
            &["--quiet", "service", "inspect", "shop.web"],
            &[],
        )
        .await;
    assert_eq!(out.lines().count(), 2, "{out}");
    assert_eq!(env.calls().len(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspect_skips_the_output_with_a_note_when_minga_logs_fails() {
    let (env, (cluster, _, failed)) = (Env::new(), with_a_failed_task().await);
    let (code, out, err) = env
        .run(
            &cluster,
            &["service", "inspect", "shop.web"],
            &[("STUB_EXIT", "7")],
        )
        .await;
    assert_eq!(code, 0, "{err}");
    assert!(
        out.contains(&format!(
            "{failed} (failed): last 20 lines of output\n  skipped: `minga logs` exited 7"
        )),
        "{out}"
    );
    assert!(!out.contains("boot line"), "{out}");

    let (code, out, _) = env
        .run(
            &cluster,
            &["--json", "service", "inspect", "shop.web"],
            &[("MINGA_BIN", "/nonexistent/minga")],
        )
        .await;
    assert_eq!(code, 0);
    let value: Value = serde_json::from_str(&out).unwrap();
    let logs = &value["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["id"] == failed.as_str())
        .unwrap()["logs"];
    assert!(
        logs["error"]
            .as_str()
            .unwrap()
            .starts_with("cannot run minga logs"),
        "{logs}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspect_over_an_unreachable_node_still_shows_the_service() {
    let (env, (cluster, _, failed)) = (Env::new(), with_a_failed_task().await);
    let ssh = |args: &[&str]| {
        let path = format!(
            "{}:{}",
            env.path("bin").display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_minga-ctl"));
        cmd.args(args)
            .env("PATH", path)
            .env("STUB_DIR", env.dir.path())
            .env("STUB_TOKEN", TOKEN)
            .env("XDG_CONFIG_HOME", env.path("config"))
            .env("XDG_STATE_HOME", env.path("state"))
            .env("HOME", env.path("home"))
            .env_remove("MINGA_CTL_CONTEXT")
            .env_remove("MINGA_CTL_ENDPOINT")
            .env_remove("MINGA_CTL_TOKEN");
        cmd
    };
    let add = [
        "context",
        "add",
        "prod",
        "--ssh",
        "alice@mgr1.example.net",
        "--remote-token-file",
        "/etc/minga/api-token",
    ];
    assert!(ssh(&add).output().unwrap().status.success());
    let child = ssh(&["service", "inspect", "shop.web"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let run = tokio::task::spawn_blocking(move || child.wait_with_output());

    // The stub's tunnel is a socket this test serves: it relays to the real API.
    let tunnel = env.path("tunnel.args");
    for _ in 0..500 {
        if tunnel.exists() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let args: Vec<String> = env
        .read("tunnel.args")
        .expect("tunnel")
        .lines()
        .map(str::to_string)
        .collect();
    let (sock, _) = args[4].split_once(':').unwrap();
    let listener = tokio::net::UnixListener::bind(sock).unwrap();
    let api = cluster.url.trim_start_matches("http://").to_string();
    let relay = tokio::spawn(async move {
        while let Ok((mut unix, _)) = listener.accept().await {
            let api = api.clone();
            tokio::spawn(async move {
                if let Ok(mut tcp) = tokio::net::TcpStream::connect(&api).await {
                    let _ = tokio::io::copy_bidirectional(&mut unix, &mut tcp).await;
                }
            });
        }
    });
    let out = run.await.unwrap().unwrap();
    relay.abort();
    let (stdout, stderr) = (
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(out.status.success(), "{stderr}");
    assert!(stdout.contains("Service:      shop.web"), "{stdout}");
    assert!(
        stdout.contains(&format!(
            "{failed} (failed): last 20 lines of output\n  skipped: node unreachable (ssh failed)"
        )),
        "{stdout}"
    );
    assert!(env.read("ssh.calls").unwrap().contains("'logs'"));
}

/// A `DELETE` with no answer, or a 2xx the client cannot read, leaves the new
/// revision: the service may be gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn service_rm_keeps_the_new_revision_when_the_delete_outcome_is_unknown() {
    for reply in [None, Some("deleted")] {
        keeps_the_revision(reply).await;
    }
}

async fn keeps_the_revision(delete_reply: Option<&'static str>) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let body = json!({
        "ID": "app.web", "Version": { "Index": 1 },
        "Spec": spec("app.web", "app", "web:9"),
    })
    .to_string();
    std::thread::spawn(move || {
        // The read is answered; the DELETE is read, then answered with plain
        // text or not at all.
        for answer in [true, false] {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let text = match (answer, delete_reply) {
                (true, _) => Some((&body[..], "application/json")),
                (false, Some(text)) => Some((text, "text/plain")),
                (false, None) => None,
            };
            if let Some((text, kind)) = text {
                let reply = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                    text.len()
                );
                let _ = stream.write_all(reply.as_bytes());
            }
        }
    });
    let env = Env::new();
    let (code, _, err) = env
        .run_at(&url, &["service", "rm", "app.web", "--yes"], &[])
        .await;
    assert_eq!(code, 2, "{err}");
    let revision = env.revision("app.web").expect("the revision stays");
    assert_eq!(
        revision["spec"]["TaskTemplate"]["ContainerSpec"]["Image"],
        "web:9"
    );
}
