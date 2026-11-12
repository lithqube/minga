//! The persisted format: frozen fixtures and the format marker.
//!
//! `tests/fixtures/v<FORMAT_VERSION>/*.json` hold the JSON of every persisted
//! type with every field filled in. They must keep decoding in every later
//! build, and every key and value in them must still be written as is. A field
//! added later with `#[serde(default)]` passes: the fixture is a subset of the
//! new JSON. If a test here fails, a persisted type was renamed, removed or
//! re-typed: bump `FORMAT_VERSION`, add a migration (see `codec.rs`) and write a
//! new fixture directory. Never edit the fixtures of an existing version.
//!
//! To write the fixtures of a new version:
//! `MINGA_WRITE_FIXTURES=1 cargo test -p minga-state --test format`

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;

use minga_state::codec::{self, FORMAT_KEY, FORMAT_VERSION};
use minga_state::types::{
    AgentSpec, ClusterNode, FailureAction, Healthcheck, MountKind, NodeRole, NodeStatus,
    PortMapping, ResourceSpec, RestartCondition, RestartPolicy, SecretRef, Service, ServiceMode,
    ServiceMount, Task, TaskSpec, TaskState, UpdateOrder, UpdatePolicy, WorkloadKind,
};
use minga_state::{StateCommand, StateStore, TypeConfig};
use openraft::{BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Vote};
use redb::{Database, TableDefinition};
use serde::de::DeserializeOwned;
use serde::Serialize;

// --- fixtures -----------------------------------------------------------------

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("v{FORMAT_VERSION}"))
        .join(format!("{name}.json"))
}

/// True when every key and value of `old` is in `new`; `new` may have more
/// object keys (fields added since `old` was written).
fn contains(new: &serde_json::Value, old: &serde_json::Value) -> bool {
    use serde_json::Value::{Array, Object};
    match (new, old) {
        (Object(n), Object(o)) => o
            .iter()
            .all(|(k, v)| n.get(k).is_some_and(|nv| contains(nv, v))),
        (Array(n), Array(o)) => n.len() == o.len() && n.iter().zip(o).all(|(a, b)| contains(a, b)),
        _ => new == old,
    }
}

#[test]
fn a_fixture_accepts_added_fields_and_rejects_changed_ones() {
    use serde_json::json;
    let old = json!({"id": "a", "tags": [{"k": 1}], "gone": null});
    assert!(contains(&old, &old));
    // A field added later.
    assert!(contains(
        &json!({"id": "a", "tags": [{"k": 1, "new": true}], "gone": null, "extra": 1}),
        &old
    ));
    // Removed, renamed, re-typed or changed.
    assert!(!contains(&json!({"id": "a", "tags": [{"k": 1}]}), &old));
    assert!(!contains(
        &json!({"id": "b", "tags": [{"k": 1}], "gone": null}),
        &old
    ));
    assert!(!contains(
        &json!({"id": "a", "tags": [{"k": "1"}], "gone": null}),
        &old
    ));
    assert!(!contains(
        &json!({"id": "a", "tags": [], "gone": null}),
        &old
    ));
}

/// Compares `value` with its fixture, and decodes the fixture back.
fn check<T: Serialize + DeserializeOwned>(name: &str, value: &T) {
    let actual = serde_json::to_value(value).unwrap();
    let path = fixture_path(name);
    if std::env::var_os("MINGA_WRITE_FIXTURES").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut text = serde_json::to_string_pretty(&actual).unwrap();
        text.push('\n');
        std::fs::write(&path, text).unwrap();
        return;
    }
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()));
    let expected: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(
        contains(&actual, &expected),
        "{name}: the persisted JSON changed shape; see the module comment\nnow: {actual}\nfixture: {expected}"
    );
    // The stored bytes must decode and encode back to the same JSON.
    let decoded: T = codec::decode(text.as_bytes())
        .unwrap_or_else(|e| panic!("{name}: fixture no longer decodes: {e}"));
    assert!(
        contains(&serde_json::to_value(&decoded).unwrap(), &expected),
        "{name}: the fixture does not survive a decode and encode"
    );
}

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn agent() -> AgentSpec {
    AgentSpec {
        module_ref: "/var/lib/minga/modules/summarize.wasm".into(),
        fuel_limit: Some(1_000_000),
        memory_limit: Some(134_217_728),
        timeout_secs: Some(30),
        token_budget: Some(50_000),
        egress_url: Some("http://127.0.0.1:8088".into()),
        prompt: Some("Summarize".into()),
        sandbox_env: map(&[("MODEL", "small")]),
    }
}

fn healthcheck() -> Healthcheck {
    Healthcheck {
        test: vec!["CMD".into(), "true".into()],
        interval_secs: Some(10),
        timeout_secs: Some(3),
        start_period_secs: Some(5),
        retries: Some(3),
    }
}

fn mount() -> ServiceMount {
    ServiceMount {
        kind: MountKind::Bind,
        source: "/srv/data".into(),
        target: "/data".into(),
        read_only: true,
    }
}

fn port() -> PortMapping {
    PortMapping {
        target: 80,
        published: Some(8080),
        protocol: "tcp".into(),
    }
}

fn service(id: &str) -> Service {
    Service {
        id: id.into(),
        name: "web".into(),
        image: "docker.io/library/nginx:1.27".into(),
        replicas: 2,
        mode: ServiceMode::Replicated,
        constraints: vec!["node.role==worker".into()],
        env: map(&[("MODE", "prod")]),
        ports: vec![port()],
        networks: vec!["shop_default".into()],
        resources: ResourceSpec {
            cpus: 0.5,
            memory_bytes: 268_435_456,
            gpus: 0,
        },
        agent: Some(agent()),
        restart_policy: RestartPolicy {
            condition: RestartCondition::OnFailure,
            delay_secs: 5,
            max_attempts: Some(3),
        },
        secrets: vec![SecretRef {
            secret_id: "shop_password".into(),
            target: "/run/secrets/password".into(),
            version: Some("sha256:00".into()),
        }],
        command: vec!["nginx".into(), "-g".into(), "daemon off;".into()],
        entrypoint: vec!["/docker-entrypoint.sh".into()],
        labels: map(&[("com.docker.stack.namespace", "shop")]),
        healthcheck: Some(healthcheck()),
        mounts: vec![mount()],
        user: Some("nginx:nginx".into()),
        extra_hosts: vec![("db".into(), "10.0.0.5".into())],
        update: UpdatePolicy {
            parallelism: 2,
            order: UpdateOrder::StartFirst,
            delay_secs: 5,
            monitor_secs: 10,
            failure_action: FailureAction::Rollback,
        },
        rollback: Some(UpdatePolicy::default()),
        previous: None,
    }
}

fn task_spec() -> TaskSpec {
    TaskSpec {
        image: "docker.io/library/nginx:1.27".into(),
        env: map(&[("MODE", "prod")]),
        ports: vec![port()],
        resources: ResourceSpec {
            cpus: 0.5,
            memory_bytes: 268_435_456,
            gpus: 0,
        },
        kind: WorkloadKind::Agent(agent()),
        restart_policy: RestartPolicy {
            condition: RestartCondition::Any,
            delay_secs: 2,
            max_attempts: None,
        },
        secrets: vec![SecretRef::new("shop_password", "/run/secrets/password")],
        ips: map(&[("shop_default", "10.100.1.4")]),
        command: vec!["nginx".into()],
        entrypoint: vec!["/docker-entrypoint.sh".into()],
        labels: map(&[("tier", "web")]),
        healthcheck: Some(healthcheck()),
        mounts: vec![mount()],
        user: Some("101".into()),
        extra_hosts: vec![("db".into(), "host-gateway".into())],
    }
}

fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}

#[test]
fn node_fixture() {
    let node = ClusterNode {
        id: 3,
        address: "10.0.0.3:7001".into(),
        labels: map(&[("zone", "a")]),
        status: NodeStatus::Active,
        capacity: ResourceSpec {
            cpus: 4.0,
            memory_bytes: 8_589_934_592,
            gpus: 0,
        },
        wg_public_key: Some("pk".into()),
        wg_endpoint: Some("10.0.0.3:51820".into()),
        overlay_ip: Some("10.100.0.3".into()),
        last_seen_unix: Some(1_794_000_000),
        role: NodeRole::Worker,
    };
    check("node", &node);
}

#[test]
fn service_fixture() {
    // `previous` holds a whole earlier service: the recursive case.
    let mut svc = service("shop.web");
    let mut earlier = service("shop.web");
    earlier.image = "docker.io/library/nginx:1.26".into();
    earlier.agent = None;
    svc.previous = Some(Box::new(earlier));
    check("service", &svc);
}

#[test]
fn task_fixture() {
    let task = Task {
        id: "shop.web.1".into(),
        service_id: "shop.web".into(),
        node_id: 3,
        state: TaskState::Running,
        spec: task_spec(),
        running_since_unix: Some(1_794_000_100),
        restarts: 2,
        last_error: None,
        last_exit_code: None,
    };
    check("task", &task);
}

/// The failure reason was added after v2 was frozen: a task written without
/// it decodes, and one with it round-trips.
#[test]
fn a_task_failure_reason_is_optional_and_round_trips() {
    let text = std::fs::read_to_string(fixture_path("task")).unwrap();
    let old: Task = codec::decode(text.as_bytes()).unwrap();
    assert_eq!((&old.last_error, old.last_exit_code), (&None, None));

    let mut task = old;
    task.last_error = Some("exited with code 1".into());
    task.last_exit_code = Some(1);
    let back: Task = codec::decode(&codec::encode(&task).unwrap()).unwrap();
    assert_eq!(back, task);
}

#[test]
fn command_fixtures() {
    check(
        "command_upsert_service",
        &StateCommand::UpsertService(service("shop.web")),
    );
    check(
        "command_renew_node_lease",
        &StateCommand::RenewNodeLease {
            id: 3,
            last_seen_unix: 1_794_000_200,
        },
    );
    check(
        "command_remove_task",
        &StateCommand::RemoveTask("shop.web.1".into()),
    );
}

#[test]
fn raft_fixtures() {
    let normal: Entry<TypeConfig> = Entry {
        log_id: log_id(7),
        payload: EntryPayload::Normal(StateCommand::UpsertService(service("shop.web"))),
    };
    check("log_entry_normal", &normal);

    let membership: Entry<TypeConfig> = Entry {
        log_id: log_id(8),
        payload: EntryPayload::Membership(Membership::new(
            vec![BTreeSet::from([1u64, 2])],
            BTreeMap::from([
                (1u64, BasicNode::new("10.0.0.1:7001")),
                (2u64, BasicNode::new("10.0.0.2:7001")),
            ]),
        )),
    };
    check("log_entry_membership", &membership);

    check("vote", &Vote::<u64>::new(3, 1));
    check("log_id", &log_id(9));
}

// --- format marker ------------------------------------------------------------

const SM_META: TableDefinition<&str, &[u8]> = TableDefinition::new("sm_meta");
const RAFT_META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

/// Opens a store in a new directory, writes a service, and shuts it down.
async fn written_dir() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    store.upsert_service(service("shop.web")).await.unwrap();
    store.shutdown().await.unwrap();
    (dir, path)
}

fn set_marker(file: PathBuf, table: TableDefinition<&str, &[u8]>, value: Option<u8>) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(table).unwrap();
        match value {
            Some(v) => {
                t.insert(FORMAT_KEY, [v].as_slice()).unwrap();
            }
            None => {
                t.remove(FORMAT_KEY).unwrap();
            }
        }
    }
    tx.commit().unwrap();
}

async fn open_error(path: &std::path::Path) -> String {
    match StateStore::open(1, "127.0.0.1:0", path, BTreeMap::new()).await {
        Ok(_) => panic!("opening must fail"),
        Err(e) => e.to_string(),
    }
}

#[tokio::test]
async fn a_new_store_gets_the_marker_and_reopens() {
    let (_dir, path) = written_dir().await;
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    assert!(store.get_service("shop.web").await.unwrap().is_some());
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_state_file_without_a_marker_is_refused() {
    // Data without a marker is what minga 1.0.2 and older wrote.
    let (_dir, path) = written_dir().await;
    set_marker(path.join("state.redb"), SM_META, None);
    let err = open_error(&path).await;
    assert!(err.contains("1.0.2 or older"), "{err}");
    assert!(err.contains("remove raft_log.redb and state.redb"), "{err}");
}

#[tokio::test]
async fn a_log_file_without_a_marker_is_refused() {
    let (_dir, path) = written_dir().await;
    set_marker(path.join("raft_log.redb"), RAFT_META, None);
    let err = open_error(&path).await;
    assert!(err.contains("1.0.2 or older"), "{err}");
}

#[tokio::test]
async fn data_from_a_newer_format_is_refused() {
    let (_dir, path) = written_dir().await;
    set_marker(path.join("state.redb"), SM_META, Some(FORMAT_VERSION + 1));
    let err = open_error(&path).await;
    assert!(err.contains("newer minga"), "{err}");
}

#[tokio::test]
async fn a_refused_store_is_left_unchanged() {
    // Opening must not write the marker into data it cannot read.
    let (_dir, path) = written_dir().await;
    set_marker(path.join("state.redb"), SM_META, None);
    open_error(&path).await;
    open_error(&path).await;
}
