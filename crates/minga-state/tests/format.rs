//! The persisted format: frozen fixtures and the format marker.
//!
//! `tests/fixtures/v<FORMAT_VERSION>/*.json` contain frozen examples of the
//! persisted types. They must keep decoding in every later build, and every
//! existing key and value must still be written as is. These fixtures detect
//! shape changes but do not replace a format-version bump and migration.
//! Additive fields need `#[serde(default)]` so older values can decode. Never
//! edit the fixtures of an existing version.
//!
//! To write the fixtures of a new version:
//! `MINGA_WRITE_FIXTURES=1 cargo test -p minga-state --test format`

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;

use minga_state::codec::{
    self, FORMAT_KEY, FORMAT_VERSION, V10_FORMAT_VERSION, V2_FORMAT_VERSION, V3_FORMAT_VERSION,
    V4_FORMAT_VERSION, V5_FORMAT_VERSION, V6_FORMAT_VERSION, V7_FORMAT_VERSION, V8_FORMAT_VERSION,
    V9_FORMAT_VERSION,
};
use minga_state::types::{
    AgentCapabilities, AgentDirGrant, AgentGrant, AgentSpec, ChangeKind, ChangeRequest,
    ChangeRequestAudit, ChangeRequestStatus, ClusterNode, Config, EgressAuth, EgressPolicy,
    EgressRoute, EgressWorkload, FailureAction, Healthcheck, MountKind, Network, NodeRole,
    NodeStatus, PortMapping, PortMode, ReservedPort, ResourceSpec, RestartCondition, RestartPolicy,
    Secret, SecretRef, Service, ServiceMode, ServiceMount, Task, TaskSpec, TaskState, Tenant,
    TenantQuotas, UpdateOrder, UpdatePolicy, Volume, WorkloadKind,
};
use minga_state::{ObjectKind, StateCommand, StateSnapshot, StateStore, TypeConfig};
use openraft::{BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership, Vote};
use redb::{Database, ReadableTable, TableDefinition};
use serde::de::DeserializeOwned;
use serde::Serialize;

// --- fixtures -----------------------------------------------------------------

fn fixture_path_for_version(version: u8, name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(format!("v{version}"))
        .join(format!("{name}.json"))
}

fn fixture_path(name: &str) -> PathBuf {
    fixture_path_for_version(FORMAT_VERSION, name)
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
    for version in V2_FORMAT_VERSION..=V10_FORMAT_VERSION {
        let old_path = fixture_path_for_version(version, name);
        if old_path.exists() {
            let old = std::fs::read_to_string(&old_path).unwrap_or_else(|e| {
                panic!("cannot read frozen fixture {}: {e}", old_path.display())
            });
            let _: T = codec::decode(old.as_bytes())
                .unwrap_or_else(|e| panic!("v{version} fixture {name} no longer decodes: {e}"));
        }
    }
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

#[test]
fn old_cluster_node_without_wireguard_or_reservation_fields_decodes() {
    let value = serde_json::json!({
        "id": 3,
        "address": "10.0.0.3:7001",
        "labels": {},
        "status": "Active",
        "capacity": {
            "cpus": 4.0,
            "memory_bytes": 8589934592u64,
            "gpus": 0
        }
    });
    let node: ClusterNode = serde_json::from_value(value).unwrap();
    assert_eq!(node.wg_public_key, None);
    assert_eq!(node.wg_endpoint, None);
    assert_eq!(node.overlay_ip, None);
    assert!(node.reserved_ports.is_empty());
    assert_eq!(node.tenant_id, minga_state::types::DEFAULT_TENANT_ID);
    assert_eq!(node.nats_user_public_key, None);
}

#[test]
fn old_port_mapping_defaults_to_ingress_and_modes_use_lowercase() {
    let old: PortMapping = serde_json::from_value(serde_json::json!({
        "target": 80,
        "published": 8080,
        "protocol": "tcp"
    }))
    .unwrap();
    assert_eq!(old.mode, PortMode::Ingress);
    assert_eq!(serde_json::to_value(&old).unwrap()["mode"], "ingress");
    assert_eq!(PortMode::default(), PortMode::Ingress);

    let mut host = port();
    host.mode = PortMode::Host;
    assert_eq!(serde_json::to_value(&host).unwrap()["mode"], "host");
}

fn node() -> ClusterNode {
    ClusterNode {
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
        overlay_ip: None,
        tenant_id: minga_state::types::DEFAULT_TENANT_ID.to_owned(),
        last_seen_unix: Some(1_794_000_000),
        role: NodeRole::Worker,
        reserved_ports: vec![ReservedPort {
            port: 9090,
            protocol: "tcp".into(),
        }],
        nats_user_public_key: None,
    }
}

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn tenant() -> Tenant {
    Tenant {
        nats_revoked_user_public_keys: Vec::new(),
        id: "tenant-a".into(),
        name: "Tenant A".into(),
        quotas: TenantQuotas {
            services: Some(10),
            tasks: Some(50),
            secrets: Some(20),
            networks: Some(5),
            volumes: Some(6),
            configs: Some(7),
            jobs: Some(8),
            desired_state_bytes: Some(10_485_760),
            api_requests_per_second: Some(100),
            jetstream_bytes: Some(1_073_741_824),
            jetstream_age_secs: Some(86_400),
        },
        created_at: 1_794_000_000,
    }
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
        capabilities: AgentCapabilities {
            grants: BTreeSet::from([AgentGrant::Env, AgentGrant::Http]),
            dirs: vec![AgentDirGrant {
                host: "/srv/data".into(),
                guest: "/data".into(),
            }],
        },
    }
}

fn egress_policy() -> EgressPolicy {
    EgressPolicy {
        required: true,
        routes: vec![
            EgressRoute {
                name: "openai".into(),
                upstream_url: "https://api.openai.com".into(),
                auth: Some(EgressAuth::Bearer {
                    secret: "openai_key".into(),
                }),
                fallback: vec!["claude".into(), "bedrock".into()],
                input_price_micros_per_million: Some(2_500_000),
                output_price_micros_per_million: Some(10_000_000),
            },
            EgressRoute {
                name: "claude".into(),
                upstream_url: "https://api.anthropic.com".into(),
                auth: Some(EgressAuth::ApiKey {
                    secret: "anthropic_key".into(),
                }),
                fallback: Vec::new(),
                input_price_micros_per_million: None,
                output_price_micros_per_million: None,
            },
            EgressRoute {
                name: "bedrock".into(),
                upstream_url: "https://bedrock-runtime.eu-west-1.amazonaws.com".into(),
                auth: Some(EgressAuth::AwsSigv4 {
                    access_key_secret: "aws_access".into(),
                    secret_key_secret: "aws_secret".into(),
                    session_token_secret: Some("aws_session".into()),
                    region: "eu-west-1".into(),
                    service: "bedrock".into(),
                }),
                fallback: Vec::new(),
                input_price_micros_per_million: None,
                output_price_micros_per_million: None,
            },
        ],
        models: vec!["gpt-4o*".into(), "claude-sonnet".into()],
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
        mode: PortMode::Ingress,
    }
}

fn service(id: &str) -> Service {
    Service {
        is_job: false,
        id: id.into(),
        tenant_id: "default".into(),
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
        agent: Some(Box::new(agent())),
        egress: Some(egress_policy()),
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
        kind: WorkloadKind::Agent(Box::new(agent())),
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
        egress: Some(EgressWorkload {
            tenant_id: "default".into(),
            service_id: "shop.web".into(),
            policy: egress_policy(),
        }),
    }
}

#[test]
fn tenant_fixture() {
    check("tenant", &tenant());
}
fn log_id(index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(1, 1), index)
}

#[test]
fn node_fixture() {
    check("node", &node());
}

#[test]
fn v3_port_schema_fixtures() {
    let mut host = port();
    host.mode = PortMode::Host;
    check("port_mapping_host", &host);
    check("port_mode_host", &PortMode::Host);
    check(
        "reserved_port",
        &ReservedPort {
            port: 9090,
            protocol: "tcp".into(),
        },
    );
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

fn change_request() -> ChangeRequest {
    let before = service("shop.web");
    let mut after = before.clone();
    after.replicas += 1;
    ChangeRequest {
        id: "cr-1-2-3".into(),
        tenant_id: "default".into(),
        service_id: "shop.web".into(),
        requester: "agent".into(),
        created_at_unix: 100,
        expires_at_unix: 1000,
        kind: ChangeKind::Scale,
        before,
        after,
        status: ChangeRequestStatus::Pending,
        reviewer: None,
        decided_at_unix: None,
        audit: vec![ChangeRequestAudit {
            action: "created".into(),
            actor: "agent".into(),
            occurred_at_unix: 100,
        }],
    }
}

#[test]
fn change_request_fixtures() {
    let request = change_request();
    check("change_request", &request);
    check(
        "command_create_change_request",
        &StateCommand::CreateChangeRequest(request.clone()),
    );
    check(
        "command_decide_change_request",
        &StateCommand::DecideChangeRequest {
            id: request.id,
            tenant_id: request.tenant_id,
            actor: "deployer".into(),
            approve: true,
            now_unix: 101,
        },
    );
}

#[test]
fn task_fixture() {
    let task = Task {
        id: "shop.web.1".into(),
        tenant_id: "default".into(),
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

#[test]
fn tenant_owned_resource_fixtures() {
    check(
        "network",
        &Network {
            id: "shop_default".into(),
            tenant_id: "default".into(),
            name: "default".into(),
            driver: "bridge".into(),
            attachable: true,
            subnet: Some("10.100.0.0/24".into()),
            wg_port: Some(51820),
        },
    );
    check(
        "volume",
        &Volume {
            id: "shop_data".into(),
            tenant_id: "default".into(),
            name: "data".into(),
            driver: "local".into(),
        },
    );
    check(
        "secret",
        &Secret {
            id: "shop_password".into(),
            tenant_id: "default".into(),
            name: "password".into(),
        },
    );
    check(
        "config",
        &Config {
            id: "shop_settings".into(),
            tenant_id: "default".into(),
            name: "settings".into(),
        },
    );
}

fn legacy_json<T: Serialize>(value: &T) -> serde_json::Value {
    let mut value = serde_json::to_value(value).unwrap();
    strip_tenant_fields(&mut value);
    strip_egress(&mut value);
    value
}

#[test]
fn legacy_tenant_owned_records_default_to_default_tenant() {
    let service: Service = serde_json::from_value(legacy_json(&service("shop.web"))).unwrap();
    assert_eq!(service.tenant_id, "default");

    let task = Task {
        id: "shop.web.1".into(),
        tenant_id: "tenant-a".into(),
        service_id: "shop.web".into(),
        node_id: 3,
        state: TaskState::Running,
        spec: task_spec(),
        running_since_unix: None,
        restarts: 0,
        last_error: None,
        last_exit_code: None,
    };
    let task: Task = serde_json::from_value(legacy_json(&task)).unwrap();
    assert_eq!(task.tenant_id, "default");

    let network = Network {
        id: "shop_default".into(),
        tenant_id: "tenant-a".into(),
        name: "default".into(),
        driver: "bridge".into(),
        attachable: true,
        subnet: None,
        wg_port: None,
    };
    let network: Network = serde_json::from_value(legacy_json(&network)).unwrap();
    assert_eq!(network.tenant_id, "default");

    let volume = Volume {
        id: "shop_data".into(),
        tenant_id: "tenant-a".into(),
        name: "data".into(),
        driver: "local".into(),
    };
    let volume: Volume = serde_json::from_value(legacy_json(&volume)).unwrap();
    assert_eq!(volume.tenant_id, "default");

    let secret = Secret {
        id: "shop_password".into(),
        tenant_id: "tenant-a".into(),
        name: "password".into(),
    };
    let secret: Secret = serde_json::from_value(legacy_json(&secret)).unwrap();
    assert_eq!(secret.tenant_id, "default");

    let config = Config {
        id: "shop_settings".into(),
        tenant_id: "tenant-a".into(),
        name: "settings".into(),
    };
    let config: Config = serde_json::from_value(legacy_json(&config)).unwrap();
    assert_eq!(config.tenant_id, "default");
}

/// The failure reason was added after v2 was frozen: a task written without
/// it decodes, and one with it round-trips.
#[test]
fn a_task_failure_reason_is_optional_and_round_trips() {
    let text =
        std::fs::read_to_string(fixture_path_for_version(V2_FORMAT_VERSION, "task")).unwrap();
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
        "command_create_tenant",
        &StateCommand::CreateTenant(tenant()),
    );
    check(
        "command_update_tenant",
        &StateCommand::UpdateTenant {
            id: "tenant-a".into(),
            name: "Tenant Alpha".into(),
            quotas: TenantQuotas::default(),
        },
    );
    check(
        "command_remove_tenant",
        &StateCommand::RemoveTenant("tenant-a".into()),
    );
    check(
        "command_upsert_service",
        &StateCommand::UpsertService(service("shop.web")),
    );
    check(
        "command_import_service",
        &StateCommand::ImportService {
            service: service("shop.web"),
            byte_allowance: 41,
        },
    );
    check(
        "command_renew_node_lease",
        &StateCommand::RenewNodeLease {
            id: 3,
            last_seen_unix: 1_794_000_200,
        },
    );
    check(
        "command_set_node_nats_user_public_key",
        &StateCommand::SetNodeNatsUserPublicKey {
            id: 3,
            public_key: Some("UABC123".into()),
        },
    );
    check(
        "command_remove_task",
        &StateCommand::RemoveTask("shop.web.1".into()),
    );
    check(
        "command_remove_owned",
        &StateCommand::RemoveOwned {
            kind: ObjectKind::Task,
            id: "shop.web.1".into(),
            tenant_id: "tenant-a".into(),
        },
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
const SM_NODES: TableDefinition<&str, &[u8]> = TableDefinition::new("nodes");
const SM_SERVICES: TableDefinition<&str, &[u8]> = TableDefinition::new("services");
const SM_TASKS: TableDefinition<&str, &[u8]> = TableDefinition::new("tasks");
const SM_TENANTS: TableDefinition<&str, &[u8]> = TableDefinition::new("tenants");
const RAFT_META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");
const RAFT_LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs");

/// Opens a store in a new directory, writes a service and node, and shuts it down.
async fn written_dir() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    store.upsert_service(service("shop.web")).await.unwrap();
    store.upsert_node(node()).await.unwrap();
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

fn strip_v3_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            object.remove("tenants");
            object.remove("reserved_ports");
            object.remove("tenant_id");
            if let Some(serde_json::Value::Array(ports)) = object.get_mut("ports") {
                for port in ports {
                    if let serde_json::Value::Object(port) = port {
                        port.remove("mode");
                    }
                }
            }
            for nested in object.values_mut() {
                strip_v3_fields(nested);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                strip_v3_fields(nested);
            }
        }
        _ => {}
    }
}

fn strip_tenant_fields(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            object.remove("tenants");
            object.remove("tenant_id");
            for nested in object.values_mut() {
                strip_tenant_fields(nested);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                strip_tenant_fields(nested);
            }
        }
        _ => {}
    }
}

fn strip_node_tenant(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            if object.contains_key("address") && object.contains_key("capacity") {
                object.remove("tenant_id");
            }
            for nested in object.values_mut() {
                strip_node_tenant(nested);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                strip_node_tenant(nested);
            }
        }
        _ => {}
    }
}
fn strip_node_nats_identity(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            if object.contains_key("address") && object.contains_key("capacity") {
                object.remove("nats_user_public_key");
            }
            for nested in object.values_mut() {
                strip_node_nats_identity(nested);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                strip_node_nats_identity(nested);
            }
        }
        _ => {}
    }
}

fn strip_tenant_revocations(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            object.remove("nats_revoked_user_public_keys");
            for nested in object.values_mut() {
                strip_tenant_revocations(nested);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                strip_tenant_revocations(nested);
            }
        }
        _ => {}
    }
}

fn strip_agent_capabilities(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            object.remove("capabilities");
            for nested in object.values_mut() {
                strip_agent_capabilities(nested);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                strip_agent_capabilities(nested);
            }
        }
        _ => {}
    }
}
fn strip_egress(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            object.remove("egress");
            for nested in object.values_mut() {
                strip_egress(nested);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                strip_egress(nested);
            }
        }
        _ => {}
    }
}

fn clear_tenants(tx: &redb::WriteTransaction) {
    let keys = {
        let table = tx.open_table(SM_TENANTS).unwrap();
        table
            .iter()
            .unwrap()
            .map(|row| row.unwrap().0.value().to_owned())
            .collect::<Vec<_>>()
    };
    let mut table = tx.open_table(SM_TENANTS).unwrap();
    for key in keys {
        table.remove(key.as_str()).unwrap();
    }
}

fn replace_tenant_commands(value: &mut serde_json::Value) {
    if let serde_json::Value::Object(object) = value {
        if object.remove("CreateTenant").is_some() {
            object.insert("UpsertNode".into(), serde_json::to_value(node()).unwrap());
        }
        for nested in object.values_mut() {
            replace_tenant_commands(nested);
        }
    } else if let serde_json::Value::Array(values) = value {
        for nested in values {
            replace_tenant_commands(nested);
        }
    }
}

fn downgrade_string_table(
    tx: &redb::WriteTransaction,
    definition: TableDefinition<&str, &[u8]>,
    version: u8,
) {
    let records = {
        let table = tx.open_table(definition).unwrap();
        table
            .iter()
            .unwrap()
            .map(|row| {
                let (key, value) = row.unwrap();
                (
                    key.value().to_owned(),
                    serde_json::from_slice::<serde_json::Value>(value.value()).unwrap(),
                )
            })
            .collect::<Vec<_>>()
    };
    let mut table = tx.open_table(definition).unwrap();
    for (key, mut value) in records {
        match version {
            V2_FORMAT_VERSION => strip_v3_fields(&mut value),
            V3_FORMAT_VERSION => strip_tenant_fields(&mut value),
            V5_FORMAT_VERSION => strip_node_tenant(&mut value),
            V6_FORMAT_VERSION => strip_node_nats_identity(&mut value),
            V7_FORMAT_VERSION => strip_tenant_revocations(&mut value),
            V8_FORMAT_VERSION => {
                strip_agent_capabilities(&mut value);
                strip_egress(&mut value);
            }
            V9_FORMAT_VERSION => strip_egress(&mut value),
            _ => {}
        }
        let bytes = serde_json::to_vec(&value).unwrap();
        table.insert(key.as_str(), bytes.as_slice()).unwrap();
    }
}

fn downgrade_log_table(tx: &redb::WriteTransaction, version: u8) {
    let records = {
        let table = tx.open_table(RAFT_LOGS).unwrap();
        table
            .iter()
            .unwrap()
            .map(|row| {
                let (index, value) = row.unwrap();
                (
                    index.value(),
                    serde_json::from_slice::<serde_json::Value>(value.value()).unwrap(),
                )
            })
            .collect::<Vec<_>>()
    };
    let mut table = tx.open_table(RAFT_LOGS).unwrap();
    for (index, mut value) in records {
        match version {
            V2_FORMAT_VERSION => strip_v3_fields(&mut value),
            V3_FORMAT_VERSION => strip_tenant_fields(&mut value),
            V5_FORMAT_VERSION => strip_node_tenant(&mut value),
            V6_FORMAT_VERSION => strip_node_nats_identity(&mut value),
            V7_FORMAT_VERSION => strip_tenant_revocations(&mut value),
            V8_FORMAT_VERSION => {
                strip_agent_capabilities(&mut value);
                strip_egress(&mut value);
            }
            V9_FORMAT_VERSION => strip_egress(&mut value),
            _ => {}
        }
        replace_tenant_commands(&mut value);
        let bytes = serde_json::to_vec(&value).unwrap();
        table.insert(index, bytes.as_slice()).unwrap();
    }
}

fn prepare_v2_state(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_string_table(&tx, SM_NODES, V2_FORMAT_VERSION);
    downgrade_string_table(&tx, SM_SERVICES, V2_FORMAT_VERSION);
    downgrade_string_table(&tx, SM_TASKS, V2_FORMAT_VERSION);
    clear_tenants(&tx);
    tx.open_table(SM_META)
        .unwrap()
        .insert(FORMAT_KEY, [V2_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v3_state(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_string_table(&tx, SM_SERVICES, V3_FORMAT_VERSION);
    clear_tenants(&tx);
    tx.open_table(SM_META)
        .unwrap()
        .insert(FORMAT_KEY, [V3_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v4_state(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    clear_tenants(&tx);
    tx.open_table(SM_META)
        .unwrap()
        .insert(FORMAT_KEY, [V4_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v5_state(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_string_table(&tx, SM_NODES, V5_FORMAT_VERSION);
    tx.open_table(SM_META)
        .unwrap()
        .insert(FORMAT_KEY, [V5_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}
fn prepare_v6_state(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_string_table(&tx, SM_NODES, V6_FORMAT_VERSION);
    tx.open_table(SM_META)
        .unwrap()
        .insert(FORMAT_KEY, [V6_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v7_state(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_string_table(&tx, SM_TENANTS, V7_FORMAT_VERSION);
    tx.open_table(SM_META)
        .unwrap()
        .insert(FORMAT_KEY, [V7_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v8_state(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_string_table(&tx, SM_SERVICES, V8_FORMAT_VERSION);
    downgrade_string_table(&tx, SM_TASKS, V8_FORMAT_VERSION);
    tx.open_table(SM_META)
        .unwrap()
        .insert(FORMAT_KEY, [V8_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}
fn prepare_v9_state(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_string_table(&tx, SM_SERVICES, V9_FORMAT_VERSION);
    downgrade_string_table(&tx, SM_TASKS, V9_FORMAT_VERSION);
    tx.open_table(SM_META)
        .unwrap()
        .insert(FORMAT_KEY, [V9_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v9_log(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_log_table(&tx, V9_FORMAT_VERSION);
    tx.open_table(RAFT_META)
        .unwrap()
        .insert(FORMAT_KEY, [V9_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v2_log(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_log_table(&tx, V2_FORMAT_VERSION);
    tx.open_table(RAFT_META)
        .unwrap()
        .insert(FORMAT_KEY, [V2_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v3_log(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_log_table(&tx, V3_FORMAT_VERSION);
    tx.open_table(RAFT_META)
        .unwrap()
        .insert(FORMAT_KEY, [V3_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v4_log(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_log_table(&tx, V4_FORMAT_VERSION);
    tx.open_table(RAFT_META)
        .unwrap()
        .insert(FORMAT_KEY, [V4_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v5_log(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_log_table(&tx, V5_FORMAT_VERSION);
    tx.open_table(RAFT_META)
        .unwrap()
        .insert(FORMAT_KEY, [V5_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}
fn prepare_v6_log(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_log_table(&tx, V6_FORMAT_VERSION);
    tx.open_table(RAFT_META)
        .unwrap()
        .insert(FORMAT_KEY, [V6_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v7_log(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_log_table(&tx, V7_FORMAT_VERSION);
    tx.open_table(RAFT_META)
        .unwrap()
        .insert(FORMAT_KEY, [V7_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}

fn prepare_v8_log(file: &std::path::Path) {
    let db = Database::create(file).unwrap();
    let tx = db.begin_write().unwrap();
    downgrade_log_table(&tx, V8_FORMAT_VERSION);
    tx.open_table(RAFT_META)
        .unwrap()
        .insert(FORMAT_KEY, [V8_FORMAT_VERSION].as_slice())
        .unwrap();
    tx.commit().unwrap();
}
fn read_marker(file: &std::path::Path, table: TableDefinition<&str, &[u8]>) -> Option<u8> {
    let db = Database::create(file).unwrap();
    let tx = db.begin_read().unwrap();
    let table = tx.open_table(table).unwrap();
    table.get(FORMAT_KEY).unwrap().map(|value| value.value()[0])
}

fn read_state_value(
    file: &std::path::Path,
    table: TableDefinition<&str, &[u8]>,
    key: &str,
) -> serde_json::Value {
    let db = Database::create(file).unwrap();
    let tx = db.begin_read().unwrap();
    let table = tx.open_table(table).unwrap();
    serde_json::from_slice(table.get(key).unwrap().unwrap().value()).unwrap()
}

fn read_log_values(file: &std::path::Path) -> Vec<(u64, serde_json::Value)> {
    let db = Database::create(file).unwrap();
    let tx = db.begin_read().unwrap();
    let table = tx.open_table(RAFT_LOGS).unwrap();
    table
        .iter()
        .unwrap()
        .map(|row| {
            let (index, value) = row.unwrap();
            (
                index.value(),
                serde_json::from_slice(value.value()).unwrap(),
            )
        })
        .collect()
}

fn has_ingress_port_mode(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(object) => {
            object.get("mode") == Some(&serde_json::Value::String("ingress".into()))
                || object.values().any(has_ingress_port_mode)
        }
        serde_json::Value::Array(values) => values.iter().any(has_ingress_port_mode),
        _ => false,
    }
}

fn has_default_tenant(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(object) => {
            object.get("tenant_id") == Some(&serde_json::json!("default"))
                || object.values().any(has_default_tenant)
        }
        serde_json::Value::Array(values) => values.iter().any(has_default_tenant),
        _ => false,
    }
}

fn has_agent_capabilities(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(object) => {
            if object.contains_key("module_ref") {
                return object.get("capabilities")
                    == Some(&serde_json::json!({"grants": ["env"], "dirs": []}));
            }
            object.values().any(has_agent_capabilities)
        }
        serde_json::Value::Array(values) => values.iter().any(has_agent_capabilities),
        _ => false,
    }
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
async fn a_v10_store_migrates_and_initializes_change_requests() {
    let (_dir, path) = written_dir().await;
    let state_file = path.join("state.redb");
    let log_file = path.join("raft_log.redb");
    set_marker(state_file.clone(), SM_META, Some(V10_FORMAT_VERSION));
    set_marker(log_file.clone(), RAFT_META, Some(V10_FORMAT_VERSION));

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    assert!(store.get_service("shop.web").await.unwrap().is_some());
    assert!(store.list_change_requests().await.unwrap().is_empty());
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&state_file, SM_META), Some(FORMAT_VERSION));
    assert_eq!(read_marker(&log_file, RAFT_META), Some(FORMAT_VERSION));
}

#[tokio::test]
async fn change_requests_require_distinct_approval_and_apply_atomically() {
    let (_dir, path) = written_dir().await;
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();

    let request = change_request();
    store.create_change_request(request.clone()).await.unwrap();
    assert!(store
        .decide_change_request(
            request.id.clone(),
            request.tenant_id.clone(),
            request.requester.clone(),
            true,
            101,
        )
        .await
        .is_err());
    assert_eq!(
        store
            .get_service(&request.service_id)
            .await
            .unwrap()
            .unwrap()
            .replicas,
        request.before.replicas
    );
    store
        .decide_change_request(
            request.id.clone(),
            request.tenant_id.clone(),
            "deployer".into(),
            true,
            102,
        )
        .await
        .unwrap();
    let approved = store
        .get_change_request(&request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(approved.status, ChangeRequestStatus::Approved);
    assert_eq!(approved.reviewer.as_deref(), Some("deployer"));
    assert_eq!(approved.audit.len(), 2);
    assert_eq!(approved.audit[0].action, "created");
    assert_eq!(approved.audit[1].action, "approved_and_applied");
    assert_eq!(
        store
            .get_service(&request.service_id)
            .await
            .unwrap()
            .unwrap()
            .replicas,
        request.after.replicas
    );

    let mut expired = change_request();
    expired.id = "cr-expired".into();
    expired.before = store
        .get_service(&expired.service_id)
        .await
        .unwrap()
        .unwrap();
    expired.after = expired.before.clone();
    expired.after.replicas += 1;
    expired.created_at_unix = 200;
    expired.expires_at_unix = 300;
    store.create_change_request(expired.clone()).await.unwrap();
    assert!(store
        .decide_change_request(
            expired.id.clone(),
            expired.tenant_id.clone(),
            "deployer".into(),
            true,
            300,
        )
        .await
        .is_err());
    assert_eq!(
        store
            .get_change_request(&expired.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ChangeRequestStatus::Expired
    );
    let expired = store
        .get_change_request(&expired.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expired.audit[1].action, "expired");

    let mut rejected = change_request();
    rejected.id = "cr-rejected".into();
    rejected.before = store
        .get_service(&rejected.service_id)
        .await
        .unwrap()
        .unwrap();
    rejected.after = rejected.before.clone();
    rejected.after.replicas += 1;
    rejected.created_at_unix = 301;
    rejected.expires_at_unix = 500;
    store.create_change_request(rejected.clone()).await.unwrap();
    assert!(store
        .decide_change_request(
            rejected.id.clone(),
            "another-tenant".into(),
            "deployer".into(),
            false,
            400,
        )
        .await
        .is_err());
    store
        .decide_change_request(
            rejected.id.clone(),
            rejected.tenant_id.clone(),
            "deployer".into(),
            false,
            401,
        )
        .await
        .unwrap();
    let rejected = store
        .get_change_request(&rejected.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rejected.status, ChangeRequestStatus::Rejected);
    assert_eq!(rejected.reviewer.as_deref(), Some("deployer"));
    assert_eq!(rejected.audit[0].action, "created");
    assert_eq!(rejected.audit[1].action, "rejected");
    store.shutdown().await.unwrap();
}
#[tokio::test]
async fn a_v2_state_file_is_rewritten_and_its_marker_advanced() {
    let (_dir, path) = written_dir().await;
    let state_file = path.join("state.redb");
    prepare_v2_state(&state_file);

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    let service = store.get_service("shop.web").await.unwrap().unwrap();
    assert_eq!(service.ports[0].mode, PortMode::Ingress);
    assert_eq!(service.tenant_id, "default");
    let node = store.get_node(3).await.unwrap().unwrap();
    assert!(node.reserved_ports.is_empty());
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&state_file, SM_META), Some(FORMAT_VERSION));
    let service = read_state_value(&state_file, SM_SERVICES, "shop.web");
    assert_eq!(service["ports"][0]["mode"], "ingress");
    assert_eq!(service["tenant_id"], "default");
    let node = read_state_value(&state_file, SM_NODES, "3");
    assert_eq!(node["reserved_ports"], serde_json::json!([]));
}

#[tokio::test]
async fn a_v3_state_file_defaults_legacy_records_to_the_default_tenant() {
    let (_dir, path) = written_dir().await;
    let state_file = path.join("state.redb");
    prepare_v3_state(&state_file);

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    let service = store.get_service("shop.web").await.unwrap().unwrap();
    assert_eq!(service.tenant_id, "default");
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&state_file, SM_META), Some(FORMAT_VERSION));
    assert_eq!(
        read_state_value(&state_file, SM_SERVICES, "shop.web")["tenant_id"],
        "default"
    );
}

#[tokio::test]
async fn a_v4_state_file_is_migrated_and_seeds_the_default_tenant() {
    let (_dir, path) = written_dir().await;
    let state_file = path.join("state.redb");
    prepare_v4_state(&state_file);

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    let service = store.get_service("shop.web").await.unwrap().unwrap();
    assert_eq!(service.tenant_id, "default");
    assert!(store.get_tenant("default").await.unwrap().is_some());
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&state_file, SM_META), Some(FORMAT_VERSION));
}

#[tokio::test]
async fn a_v5_state_and_log_file_migrate_node_tenants_to_default() {
    let (_dir, path) = written_dir().await;
    let state_file = path.join("state.redb");
    let log_file = path.join("raft_log.redb");
    prepare_v5_state(&state_file);
    prepare_v5_log(&log_file);

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        store.get_node(3).await.unwrap().unwrap().tenant_id,
        "default"
    );
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&state_file, SM_META), Some(FORMAT_VERSION));
    assert_eq!(read_marker(&log_file, RAFT_META), Some(FORMAT_VERSION));
    assert_eq!(
        read_state_value(&state_file, SM_NODES, "3")["tenant_id"],
        "default"
    );
}
#[tokio::test]
async fn a_v6_state_and_log_file_migrate_nats_node_identity() {
    let (_dir, path) = written_dir().await;
    let state_file = path.join("state.redb");
    let log_file = path.join("raft_log.redb");
    let indices: Vec<_> = read_log_values(&log_file)
        .into_iter()
        .map(|(index, _)| index)
        .collect();
    prepare_v6_state(&state_file);
    prepare_v6_log(&log_file);

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        store
            .get_node(3)
            .await
            .unwrap()
            .unwrap()
            .nats_user_public_key,
        None
    );
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&state_file, SM_META), Some(FORMAT_VERSION));
    assert_eq!(read_marker(&log_file, RAFT_META), Some(FORMAT_VERSION));
    assert_eq!(
        read_state_value(&state_file, SM_NODES, "3")["nats_user_public_key"],
        serde_json::Value::Null
    );
    let entries = read_log_values(&log_file);
    assert_eq!(
        entries
            .iter()
            .take(indices.len())
            .map(|(index, _)| *index)
            .collect::<Vec<_>>(),
        indices
    );
}

#[tokio::test]
async fn a_v7_state_and_log_file_migrate_tenant_revocation_records() {
    let (_dir, path) = written_dir().await;
    let state_file = path.join("state.redb");
    let log_file = path.join("raft_log.redb");
    prepare_v7_state(&state_file);
    prepare_v7_log(&log_file);

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    assert!(store
        .get_tenant("default")
        .await
        .unwrap()
        .unwrap()
        .nats_revoked_user_public_keys
        .is_empty());
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&state_file, SM_META), Some(FORMAT_VERSION));
    assert_eq!(read_marker(&log_file, RAFT_META), Some(FORMAT_VERSION));
}

#[tokio::test]
async fn a_v8_state_and_log_file_grant_env_to_legacy_agents() {
    let (_dir, path) = written_dir().await;
    let state_file = path.join("state.redb");
    let log_file = path.join("raft_log.redb");
    prepare_v8_state(&state_file);
    prepare_v8_log(&log_file);

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    let service = store.get_service("shop.web").await.unwrap().unwrap();
    assert_eq!(
        service.agent.unwrap().capabilities,
        AgentCapabilities {
            grants: BTreeSet::from([AgentGrant::Env]),
            dirs: Vec::new(),
        }
    );
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&state_file, SM_META), Some(FORMAT_VERSION));
    assert_eq!(read_marker(&log_file, RAFT_META), Some(FORMAT_VERSION));
    assert_eq!(
        read_state_value(&state_file, SM_SERVICES, "shop.web")["agent"]["capabilities"],
        serde_json::json!({"grants": ["env"], "dirs": []})
    );
    assert!(
        read_log_values(&log_file)
            .iter()
            .any(|(_, entry)| has_agent_capabilities(entry)),
        "migrated Raft log entries must grant env to legacy agents"
    );
}
#[tokio::test]
async fn a_v9_state_and_log_file_migrate_without_egress_policy() {
    let (_dir, path) = written_dir().await;
    let state_file = path.join("state.redb");
    let log_file = path.join("raft_log.redb");
    prepare_v9_state(&state_file);
    prepare_v9_log(&log_file);
    assert!(read_state_value(&state_file, SM_SERVICES, "shop.web")
        .get("egress")
        .is_none());

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    let service = store.get_service("shop.web").await.unwrap().unwrap();
    assert_eq!(service.egress, None);
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&state_file, SM_META), Some(FORMAT_VERSION));
    assert_eq!(read_marker(&log_file, RAFT_META), Some(FORMAT_VERSION));
    assert_eq!(
        read_state_value(&state_file, SM_SERVICES, "shop.web").get("egress"),
        Some(&serde_json::Value::Null)
    );
    assert!(
        read_log_values(&log_file)
            .iter()
            .any(|(_, entry)| entry.to_string().contains("\"egress\":null")),
        "migrated Raft log entries must carry the v10 egress shape"
    );
}

#[tokio::test]
async fn a_v2_log_file_rewrites_entries_and_preserves_indices() {
    let (_dir, path) = written_dir().await;
    let log_file = path.join("raft_log.redb");
    let indices: Vec<_> = read_log_values(&log_file)
        .into_iter()
        .map(|(index, _)| index)
        .collect();
    prepare_v2_log(&log_file);

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    assert!(store.get_service("shop.web").await.unwrap().is_some());
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&log_file, RAFT_META), Some(FORMAT_VERSION));
    let entries = read_log_values(&log_file);
    assert_eq!(
        entries
            .iter()
            .take(indices.len())
            .map(|(index, _)| *index)
            .collect::<Vec<_>>(),
        indices
    );
    assert!(entries
        .iter()
        .any(|(_, value)| has_ingress_port_mode(value)));
}

#[tokio::test]
async fn a_v3_log_file_rewrites_entries_and_defaults_tenant_fields() {
    let (_dir, path) = written_dir().await;
    let log_file = path.join("raft_log.redb");
    let indices: Vec<_> = read_log_values(&log_file)
        .into_iter()
        .map(|(index, _)| index)
        .collect();
    prepare_v3_log(&log_file);

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        store
            .get_service("shop.web")
            .await
            .unwrap()
            .unwrap()
            .tenant_id,
        "default"
    );
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&log_file, RAFT_META), Some(FORMAT_VERSION));
    let entries = read_log_values(&log_file);
    assert_eq!(
        entries
            .iter()
            .take(indices.len())
            .map(|(index, _)| *index)
            .collect::<Vec<_>>(),
        indices
    );
    assert!(entries.iter().any(|(_, value)| has_default_tenant(value)));
}

#[tokio::test]
async fn a_v4_log_file_rewrites_entries_and_preserves_tenant_fields() {
    let (_dir, path) = written_dir().await;
    let log_file = path.join("raft_log.redb");
    let indices: Vec<_> = read_log_values(&log_file)
        .into_iter()
        .map(|(index, _)| index)
        .collect();
    prepare_v4_log(&log_file);

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        store
            .get_service("shop.web")
            .await
            .unwrap()
            .unwrap()
            .tenant_id,
        "default"
    );
    store.shutdown().await.unwrap();

    assert_eq!(read_marker(&log_file, RAFT_META), Some(FORMAT_VERSION));
    let entries = read_log_values(&log_file);
    assert_eq!(
        entries
            .iter()
            .take(indices.len())
            .map(|(index, _)| *index)
            .collect::<Vec<_>>(),
        indices
    );
    assert!(entries.iter().any(|(_, value)| has_default_tenant(value)));
}

#[tokio::test]
async fn importing_a_v2_portable_snapshot_defaults_fields_and_exports_current() {
    let (_dir, path) = written_dir().await;
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();

    let mut old_json = serde_json::to_value(store.export().await.unwrap()).unwrap();
    old_json["format_version"] = serde_json::json!(V2_FORMAT_VERSION);
    strip_v3_fields(&mut old_json);
    strip_agent_capabilities(&mut old_json);
    strip_egress(&mut old_json);
    let snapshot: StateSnapshot = serde_json::from_value(old_json).unwrap();
    assert_eq!(snapshot.services[0].ports[0].mode, PortMode::Ingress);

    store.import(snapshot, true).await.unwrap();
    let imported = store.get_service("shop.web").await.unwrap().unwrap();
    assert_eq!(imported.ports[0].mode, PortMode::Ingress);
    assert_eq!(imported.tenant_id, "default");
    assert_eq!(store.export().await.unwrap().format_version, FORMAT_VERSION);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn importing_a_v3_portable_snapshot_defaults_tenant() {
    let (_dir, path) = written_dir().await;
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();

    let mut old_json = serde_json::to_value(store.export().await.unwrap()).unwrap();
    old_json["format_version"] = serde_json::json!(V3_FORMAT_VERSION);
    strip_tenant_fields(&mut old_json);
    strip_agent_capabilities(&mut old_json);
    strip_egress(&mut old_json);
    let snapshot: StateSnapshot = serde_json::from_value(old_json).unwrap();
    assert_eq!(snapshot.services[0].tenant_id, "default");

    store.import(snapshot, true).await.unwrap();
    let imported = store.get_service("shop.web").await.unwrap().unwrap();
    assert_eq!(imported.tenant_id, "default");
    assert_eq!(store.export().await.unwrap().format_version, FORMAT_VERSION);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn importing_a_v4_portable_snapshot_seeds_default_tenant() {
    let (_dir, path) = written_dir().await;
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();

    let mut old_json = serde_json::to_value(store.export().await.unwrap()).unwrap();
    old_json["format_version"] = serde_json::json!(V4_FORMAT_VERSION);
    old_json.as_object_mut().unwrap().remove("tenants");
    strip_agent_capabilities(&mut old_json);
    strip_egress(&mut old_json);
    let snapshot: StateSnapshot = serde_json::from_value(old_json).unwrap();
    assert!(snapshot.tenants.is_empty());

    store.import(snapshot, true).await.unwrap();
    assert!(store.get_tenant("default").await.unwrap().is_some());
    assert_eq!(store.export().await.unwrap().format_version, FORMAT_VERSION);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn importing_a_v8_portable_snapshot_grants_env_to_legacy_agents() {
    let (_dir, path) = written_dir().await;
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();

    let mut old_json = serde_json::to_value(store.export().await.unwrap()).unwrap();
    old_json["format_version"] = serde_json::json!(V8_FORMAT_VERSION);
    strip_agent_capabilities(&mut old_json);
    strip_egress(&mut old_json);
    let snapshot: StateSnapshot = serde_json::from_value(old_json).unwrap();

    store.import(snapshot, true).await.unwrap();
    let imported = store.get_service("shop.web").await.unwrap().unwrap();
    assert_eq!(
        imported.agent.unwrap().capabilities,
        AgentCapabilities {
            grants: BTreeSet::from([AgentGrant::Env]),
            dirs: Vec::new(),
        }
    );
    assert_eq!(store.export().await.unwrap().format_version, FORMAT_VERSION);
    store.shutdown().await.unwrap();
}

/// Imports `snapshot` into an empty store whose `default` tenant has `limit`
/// desired-state bytes, returning the result and the quota left afterwards.
async fn import_with_byte_quota(
    snapshot: StateSnapshot,
    limit: u64,
) -> (Result<(), minga_state::StateError>, Option<u64>) {
    let dir = tempfile::tempdir().unwrap();
    let store = StateStore::open(1, "127.0.0.1:0", &dir.path().join("data"), BTreeMap::new())
        .await
        .unwrap();
    let tenant = store.get_tenant("default").await.unwrap().unwrap();
    let quotas = TenantQuotas {
        desired_state_bytes: Some(limit),
        ..tenant.quotas
    };
    store
        .update_tenant("default", tenant.name, quotas)
        .await
        .unwrap();
    let result = store.import(snapshot, true).await;
    let left = store
        .get_tenant("default")
        .await
        .unwrap()
        .unwrap()
        .quotas
        .desired_state_bytes;
    store.shutdown().await.unwrap();
    (result, left)
}

#[tokio::test]
async fn a_v8_snapshot_imports_at_its_byte_quota_and_keeps_the_quota() {
    let (_dir, path) = written_dir().await;
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    let current = store.export().await.unwrap();
    store.shutdown().await.unwrap();

    let mut old_json = serde_json::to_value(&current).unwrap();
    old_json["format_version"] = serde_json::json!(V8_FORMAT_VERSION);
    strip_agent_capabilities(&mut old_json);
    strip_egress(&mut old_json);
    // Many agent services, each with an agent in `previous` too: their summed
    // growth exceeds any one record, and nested agents grow as well.
    let base = old_json["services"][0].clone();
    old_json["services"] = (0..150)
        .map(|i| {
            let mut copy = base.clone();
            copy["id"] = serde_json::json!(format!("shop.web{i}"));
            copy["name"] = serde_json::json!(format!("web{i}"));
            copy["ports"] = serde_json::json!([]);
            copy["previous"] = base.clone();
            copy
        })
        .collect();
    let v8_sizes: Vec<usize> = old_json["services"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| serde_json::to_vec(s).unwrap().len())
        .collect();
    let v8_bytes: usize = v8_sizes.iter().sum();
    let legacy: StateSnapshot = serde_json::from_value(old_json).unwrap();
    let v9_bytes: usize = legacy
        .services
        .iter()
        .map(|s| serde_json::to_vec(s).unwrap().len())
        .sum();
    assert!(v9_bytes > v8_bytes, "the fixture must grow on re-encoding");

    // At exactly the v8 size, the data fit its v8 cluster and imports.
    let (result, left) = import_with_byte_quota(legacy.clone(), v8_bytes as u64).await;
    result.unwrap();
    assert_eq!(left, Some(v8_bytes as u64));

    // The allowance covers only the re-encoding growth, not more data.
    let (result, left) = import_with_byte_quota(legacy.clone(), v8_bytes as u64 - 1).await;
    assert!(
        result.is_err(),
        "a v8 import over its v8 size must be refused"
    );
    assert_eq!(left, Some(v8_bytes as u64 - 1));

    // A repeated service id replaces its record, so its growth counts once.
    let repeated = StateSnapshot {
        services: vec![
            legacy.services[0].clone(),
            legacy.services[0].clone(),
            legacy.services[1].clone(),
        ],
        ..legacy.clone()
    };
    let limit = (v8_sizes[0] + v8_sizes[1]) as u64;
    let (result, _) = import_with_byte_quota(repeated.clone(), limit).await;
    result.unwrap();
    let (result, _) = import_with_byte_quota(repeated, limit - 1).await;
    assert!(result.is_err(), "a repeated id must not add allowance");

    // The same records as a current-format snapshot get no allowance.
    let as_v9 = StateSnapshot {
        format_version: FORMAT_VERSION,
        ..legacy
    };
    let (result, _) = import_with_byte_quota(as_v9, v9_bytes as u64 - 1).await;
    assert!(result.is_err(), "a v9 import must stay quota-enforced");
}

#[tokio::test]
async fn a_v9_snapshot_imports_at_its_byte_quota_without_egress() {
    let (_dir, path) = written_dir().await;
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    let mut old_json = serde_json::to_value(store.export().await.unwrap()).unwrap();
    store.shutdown().await.unwrap();
    old_json["format_version"] = serde_json::json!(V9_FORMAT_VERSION);
    strip_egress(&mut old_json);
    let v9_bytes = serde_json::to_vec(&old_json["services"][0]).unwrap().len() as u64;
    let legacy: StateSnapshot = serde_json::from_value(old_json).unwrap();
    assert_eq!(legacy.services[0].egress, None);
    let v10_bytes = serde_json::to_vec(&legacy.services[0]).unwrap().len() as u64;
    assert!(v10_bytes > v9_bytes, "the egress key must grow the record");

    let (result, _) = import_with_byte_quota(legacy.clone(), v9_bytes).await;
    result.unwrap();
    let (result, _) = import_with_byte_quota(legacy, v9_bytes - 1).await;
    assert!(
        result.is_err(),
        "a v9 import over its v9 size must be refused"
    );
}

#[tokio::test]
async fn a_v8_labelled_snapshot_cannot_smuggle_capabilities_past_the_quota() {
    let (_dir, path) = written_dir().await;
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    let mut old_json = serde_json::to_value(store.export().await.unwrap()).unwrap();
    store.shutdown().await.unwrap();
    old_json["format_version"] = serde_json::json!(V8_FORMAT_VERSION);
    strip_agent_capabilities(&mut old_json);
    strip_egress(&mut old_json);
    let base = old_json["services"][0].clone();
    let v8_bytes = serde_json::to_vec(&base).unwrap().len() as u64;

    // Valid dir grants, large enough that uncounted they would be a bypass.
    let big = serde_json::json!({
        "grants": ["env"],
        "dirs": (0..AgentCapabilities::MAX_DIRS)
            .map(|i| serde_json::json!({
                "host": format!("/srv/{i}/{}", "a".repeat(1000)),
                "guest": format!("/d{i}"),
            }))
            .collect::<Vec<_>>(),
    });
    let mut top = base.clone();
    top["agent"]["capabilities"] = big.clone();
    let mut nested = base.clone();
    nested["previous"] = base.clone();
    nested["previous"]["agent"]["capabilities"] = big;
    // Even a single extra grant is more than the migration produces.
    let mut http = base.clone();
    http["agent"]["capabilities"] = serde_json::json!({"grants": ["env", "http"]});

    for service in [top, nested, http] {
        let mut json = old_json.clone();
        json["services"] = serde_json::json!([service]);
        let snapshot: StateSnapshot = serde_json::from_value(json).unwrap();
        let (result, _) = import_with_byte_quota(snapshot, v8_bytes).await;
        assert!(
            matches!(result, Err(minga_state::StateError::BadSnapshot(ref m)) if m.contains("capabilities")),
            "{result:?}"
        );
    }

    // Exactly what the migration produces is accepted.
    let mut migrated = base;
    migrated["agent"]["capabilities"] = serde_json::json!({"grants": ["env"], "dirs": []});
    old_json["services"] = serde_json::json!([migrated]);
    let snapshot: StateSnapshot = serde_json::from_value(old_json).unwrap();
    let (result, _) = import_with_byte_quota(snapshot, v8_bytes).await;
    result.unwrap();
}

#[tokio::test]
async fn importing_unsupported_portable_snapshot_versions_is_refused() {
    let (_dir, path) = written_dir().await;
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();

    for version in [1, FORMAT_VERSION + 1] {
        let snapshot = StateSnapshot {
            format_version: version,
            exported_at_unix: 0,
            tenants: Vec::new(),
            services: Vec::new(),
            networks: Vec::new(),
            volumes: Vec::new(),
            secrets: Vec::new(),
            configs: Vec::new(),
        };
        assert!(store.import(snapshot, false).await.is_err());
    }
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_v9_labelled_snapshot_cannot_smuggle_an_egress_policy_past_the_quota() {
    let (_dir, path) = written_dir().await;
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    let mut old_json = serde_json::to_value(store.export().await.unwrap()).unwrap();
    store.shutdown().await.unwrap();
    old_json["format_version"] = serde_json::json!(V9_FORMAT_VERSION);
    // The fixture's egress policy is what a v9 snapshot cannot carry.
    assert!(!old_json["services"][0]["egress"].is_null());
    let snapshot: StateSnapshot = serde_json::from_value(old_json.clone()).unwrap();
    let (result, _) = import_with_byte_quota(snapshot, u64::MAX / 2).await;
    assert!(
        matches!(result, Err(minga_state::StateError::BadSnapshot(ref m)) if m.contains("egress")),
        "{result:?}"
    );
    // Nested in a rollback target too.
    strip_egress(&mut old_json);
    let mut service = old_json["services"][0].clone();
    let mut previous = service.clone();
    previous["egress"] = serde_json::to_value(egress_policy()).unwrap();
    service["previous"] = previous;
    old_json["services"] = serde_json::json!([service]);
    let snapshot: StateSnapshot = serde_json::from_value(old_json).unwrap();
    let (result, _) = import_with_byte_quota(snapshot, u64::MAX / 2).await;
    assert!(
        matches!(result, Err(minga_state::StateError::BadSnapshot(ref m)) if m.contains("egress")),
        "{result:?}"
    );
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
    for (filename, table) in [("state.redb", SM_META), ("raft_log.redb", RAFT_META)] {
        let (_dir, path) = written_dir().await;
        set_marker(path.join(filename), table, Some(FORMAT_VERSION + 1));
        let err = open_error(&path).await;
        assert!(err.contains("newer minga"), "{err}");
    }
}

#[tokio::test]
async fn unsupported_older_format_markers_are_refused() {
    for (filename, table) in [("state.redb", SM_META), ("raft_log.redb", RAFT_META)] {
        let (_dir, path) = written_dir().await;
        set_marker(path.join(filename), table, Some(1));
        let err = open_error(&path).await;
        assert!(err.contains("no migration from data format 1"), "{err}");
    }
}

#[tokio::test]
async fn a_refused_store_is_left_unchanged() {
    // Opening must not write the marker into data it cannot read.
    let (_dir, path) = written_dir().await;
    set_marker(path.join("state.redb"), SM_META, None);
    open_error(&path).await;
    open_error(&path).await;
}
