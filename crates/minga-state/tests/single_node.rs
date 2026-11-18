//! Integration tests for a single-node StateStore.
//!
//! Each test creates a StateStore in a temp directory, exercises it,
//! and shuts it down. Tests run in parallel (separate temp dirs).

use std::collections::{BTreeMap, HashMap};

use minga_state::types::{
    ClusterNode, Config, Network, NodeStatus, PortMapping, ResourceSpec, Secret, Service,
    ServiceMode, Task, TaskSpec, TaskState, Tenant, TenantQuotas, Volume,
};
use minga_state::{StateError, StateStore};

/// Unique port counter for parallel test isolation.
/// Creates a StateStore in a unique temp directory.
///
/// The RPC server binds port 0 (OS-assigned): a single-node cluster never
/// receives RPC connections, and a fixed port can collide with anything on
/// a shared CI runner.
async fn test_store(test_name: &str) -> (StateStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(test_name);
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    (store, dir)
}

fn sample_node(id: u64) -> ClusterNode {
    ClusterNode {
        id,
        address: format!("10.0.0.{id}:50051"),
        labels: HashMap::from([("gpu".to_string(), "true".to_string())]),
        status: NodeStatus::Active,
        capacity: ResourceSpec {
            cpus: 8.0,
            memory_bytes: 32 * 1024 * 1024 * 1024,
            gpus: 1,
        },
        wg_public_key: None,
        wg_endpoint: None,
        overlay_ip: None,
        tenant_id: minga_state::types::DEFAULT_TENANT_ID.to_owned(),
        last_seen_unix: None,
        role: Default::default(),
        reserved_ports: Vec::new(),
        nats_user_public_key: None,
    }
}

fn sample_service(id: &str) -> Service {
    Service {
        is_job: false,
        id: id.to_string(),
        tenant_id: "default".to_string(),
        name: id.to_string(),
        image: format!("registry.local/{id}:1.0"),
        replicas: 2,
        mode: ServiceMode::Replicated,
        constraints: vec!["node.labels.gpu == true".to_string()],
        env: HashMap::from([("LOG_LEVEL".to_string(), "info".to_string())]),
        ports: vec![PortMapping {
            target: 8080,
            published: Some(443),
            protocol: "tcp".to_string(),
            mode: minga_state::types::PortMode::Ingress,
        }],
        networks: vec!["backplane".to_string()],
        resources: ResourceSpec {
            cpus: 2.0,
            memory_bytes: 4 * 1024 * 1024 * 1024,
            gpus: 0,
        },
        agent: None,
        restart_policy: Default::default(),
        secrets: vec![],
        command: vec![],
        entrypoint: vec![],
        labels: HashMap::new(),
        healthcheck: None,
        mounts: vec![],
        user: None,
        update: Default::default(),
        rollback: None,
        previous: None,
        extra_hosts: vec![],
    }
}

fn sample_task(id: &str, service_id: &str, node_id: u64) -> Task {
    Task {
        id: id.to_string(),
        tenant_id: "default".to_string(),
        service_id: service_id.to_string(),
        node_id,
        state: TaskState::Assigned,
        spec: TaskSpec {
            image: format!("registry.local/{service_id}:1.0"),
            env: HashMap::new(),
            ports: vec![],
            resources: ResourceSpec::default(),
            ..Default::default()
        },
        restarts: 0,
        running_since_unix: None,
        last_error: None,
        last_exit_code: None,
    }
}

#[tokio::test]
async fn elects_itself_leader() {
    let (store, _dir) = test_store("elects_itself_leader").await;
    assert!(store.is_leader().await);
    assert_eq!(store.current_leader().await, Some(1));
    assert_eq!(store.node_id(), 1);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn write_and_read_node() {
    let (store, _dir) = test_store("write_and_read_node").await;

    let node = sample_node(1);
    store.upsert_node(node.clone()).await.unwrap();

    let read = store.get_node(1).await.unwrap().unwrap();
    assert_eq!(read, node);

    let nodes = store.list_nodes().await.unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0], node);

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn remove_node() {
    let (store, _dir) = test_store("remove_node").await;

    store.upsert_node(sample_node(1)).await.unwrap();
    store.upsert_node(sample_node(2)).await.unwrap();
    assert_eq!(store.list_nodes().await.unwrap().len(), 2);

    store.remove_node(1).await.unwrap();
    assert!(store.get_node(1).await.unwrap().is_none());
    assert!(store.get_node(2).await.unwrap().is_some());
    assert_eq!(store.list_nodes().await.unwrap().len(), 1);

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn write_and_read_service() {
    let (store, _dir) = test_store("write_and_read_service").await;

    let svc = sample_service("inference");
    store.upsert_service(svc.clone()).await.unwrap();

    let read = store.get_service("inference").await.unwrap().unwrap();
    assert_eq!(read, svc);

    let services = store.list_services().await.unwrap();
    assert_eq!(services.len(), 1);

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn commit_rejects_a_second_service_on_an_ingress_port() {
    let (store, _dir) = test_store("commit_rejects_ingress_port").await;
    let first = sample_service("first");
    store.upsert_service(first.clone()).await.unwrap();

    // Admission can race; the write itself refuses the duplicate.
    let error = store
        .upsert_service(sample_service("second"))
        .await
        .unwrap_err();
    assert!(
        matches!(&error, minga_state::StateError::PortConflict(message) if message.contains("first")),
        "{error}"
    );
    assert!(store.get_service("second").await.unwrap().is_none());

    // Re-writing the holder, or host-mode services sharing a port, is fine.
    store.upsert_service(first).await.unwrap();
    let mut host = sample_service("host");
    host.ports[0].published = Some(8443);
    host.ports[0].mode = minga_state::types::PortMode::Host;
    store.upsert_service(host.clone()).await.unwrap();
    let mut second_host = host;
    second_host.id = "host-2".into();
    store.upsert_service(second_host).await.unwrap();

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn write_and_read_task() {
    let (store, _dir) = test_store("write_and_read_task").await;

    let task = sample_task("task-1", "inference", 1);
    store.upsert_task(task.clone()).await.unwrap();

    let read = store.get_task("task-1").await.unwrap().unwrap();
    assert_eq!(read, task);

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn write_and_read_network_volume_secret_config() {
    let (store, _dir) = test_store("write_and_read_all_types").await;

    let net = Network {
        id: "backplane".to_string(),
        tenant_id: "default".to_string(),
        name: "backplane".to_string(),
        driver: "overlay".to_string(),
        attachable: true,
        subnet: None,
        wg_port: None,
    };
    store.upsert_network(net.clone()).await.unwrap();
    assert_eq!(store.get_network("backplane").await.unwrap(), Some(net));

    let vol = Volume {
        id: "models".to_string(),
        tenant_id: "default".to_string(),
        name: "models".to_string(),
        driver: "local".to_string(),
    };
    store.upsert_volume(vol.clone()).await.unwrap();
    assert_eq!(store.get_volume("models").await.unwrap(), Some(vol));

    let secret = Secret {
        id: "api_token".to_string(),
        tenant_id: "default".to_string(),
        name: "api_token".to_string(),
    };
    store.upsert_secret(secret.clone()).await.unwrap();
    assert_eq!(store.get_secret("api_token").await.unwrap(), Some(secret));

    let config = Config {
        id: "agent_config".to_string(),
        tenant_id: "default".to_string(),
        name: "agent_config".to_string(),
    };
    store.upsert_config(config.clone()).await.unwrap();
    assert_eq!(
        store.get_config("agent_config").await.unwrap(),
        Some(config)
    );

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn multiple_writes_in_sequence() {
    let (store, _dir) = test_store("multiple_writes").await;

    for i in 1..=5 {
        store.upsert_node(sample_node(i)).await.unwrap();
    }
    assert_eq!(store.list_nodes().await.unwrap().len(), 5);

    for i in 1..=3 {
        // Each service needs its own ingress port.
        let mut service = sample_service(&format!("svc-{i}"));
        service.ports[0].published = Some(8440 + i);
        store.upsert_service(service).await.unwrap();
    }
    assert_eq!(store.list_services().await.unwrap().len(), 3);

    for i in 1..=10 {
        store
            .upsert_task(sample_task(&format!("task-{i}"), "svc-1", 1))
            .await
            .unwrap();
    }
    assert_eq!(store.list_tasks().await.unwrap().len(), 10);

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn state_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("persistence");
    let addr = "127.0.0.1:0";

    // First run: write some data.
    {
        let store = StateStore::open(1, addr, &path, BTreeMap::new())
            .await
            .unwrap();
        store.upsert_node(sample_node(1)).await.unwrap();
        store
            .upsert_service(sample_service("inference"))
            .await
            .unwrap();
        store.shutdown().await.unwrap();
    }

    // Second run: data should still be there.
    {
        let store = StateStore::open(1, addr, &path, BTreeMap::new())
            .await
            .unwrap();
        let node = store.get_node(1).await.unwrap().unwrap();
        assert_eq!(node.id, 1);
        assert_eq!(node.status, NodeStatus::Active);

        let svc = store.get_service("inference").await.unwrap().unwrap();
        assert_eq!(svc.replicas, 2);

        store.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn upsert_overwrites_existing() {
    let (store, _dir) = test_store("upsert_overwrites").await;

    let mut node = sample_node(1);
    store.upsert_node(node.clone()).await.unwrap();

    // Update the node.
    node.status = NodeStatus::Draining;
    node.labels.insert("zone".to_string(), "a".to_string());
    store.upsert_node(node.clone()).await.unwrap();

    let read = store.get_node(1).await.unwrap().unwrap();
    assert_eq!(read.status, NodeStatus::Draining);
    assert_eq!(read.labels.get("zone").map(String::as_str), Some("a"));
    assert_eq!(store.list_nodes().await.unwrap().len(), 1);

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn tenant_crud_is_persisted_and_protects_owned_resources() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tenants");
    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();

    let defaults = store.list_tenants().await.unwrap();
    assert_eq!(defaults.len(), 1);
    assert_eq!(defaults[0].id, "default");

    let tenant = Tenant {
        nats_revoked_user_public_keys: Vec::new(),
        id: "tenant-a".into(),
        name: "Tenant A".into(),
        quotas: TenantQuotas {
            services: Some(10),
            ..TenantQuotas::default()
        },
        created_at: 123,
    };
    store.create_tenant(tenant.clone()).await.unwrap();
    assert!(matches!(
        store.create_tenant(tenant.clone()).await,
        Err(StateError::Conflict(_))
    ));

    let updated_quotas = TenantQuotas {
        services: Some(20),
        ..TenantQuotas::default()
    };
    store
        .update_tenant("tenant-a", "Tenant Alpha".into(), updated_quotas.clone())
        .await
        .unwrap();
    let updated = store.get_tenant("tenant-a").await.unwrap().unwrap();
    assert_eq!(updated.name, "Tenant Alpha");
    assert_eq!(updated.quotas, updated_quotas);
    assert_eq!(updated.created_at, 123);

    store
        .upsert_volume(Volume {
            id: "tenant-a-data".into(),
            tenant_id: "tenant-a".into(),
            name: "data".into(),
            driver: "local".into(),
        })
        .await
        .unwrap();
    assert!(matches!(
        store.remove_tenant("tenant-a").await,
        Err(StateError::Conflict(_))
    ));
    store.remove_volume("tenant-a-data".into()).await.unwrap();

    let mut node = minga_state::types::ClusterNode {
        id: 42,
        address: "10.0.0.42:50051".into(),
        labels: Default::default(),
        status: minga_state::types::NodeStatus::Active,
        capacity: Default::default(),
        wg_public_key: None,
        wg_endpoint: None,
        overlay_ip: None,
        tenant_id: "tenant-a".into(),
        last_seen_unix: None,
        role: Default::default(),
        reserved_ports: Vec::new(),
        nats_user_public_key: None,
    };
    store.upsert_node(node.clone()).await.unwrap();
    assert!(
        matches!(
            store.remove_tenant("tenant-a").await,
            Err(StateError::Conflict(_))
        ),
        "a tenant that still owns nodes must not be deleted"
    );
    node.tenant_id = "default".into();
    store.upsert_node(node).await.unwrap();
    store.remove_tenant("tenant-a").await.unwrap();
    assert!(store.get_tenant("tenant-a").await.unwrap().is_none());
    assert!(matches!(
        store.remove_tenant("default").await,
        Err(StateError::Conflict(_))
    ));

    store
        .create_tenant(Tenant {
            nats_revoked_user_public_keys: Vec::new(),
            id: "persisted".into(),
            name: "Persisted".into(),
            quotas: TenantQuotas::default(),
            created_at: 456,
        })
        .await
        .unwrap();
    store.shutdown().await.unwrap();

    let store = StateStore::open(1, "127.0.0.1:0", &path, BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        store
            .get_tenant("persisted")
            .await
            .unwrap()
            .unwrap()
            .created_at,
        456
    );
    store.shutdown().await.unwrap();
}
#[tokio::test]
async fn cluster_revocations_survive_tenant_updates_and_deletion() {
    let (store, _dir) = test_store("tenant-revocations").await;
    store
        .create_tenant(Tenant {
            id: "tenant-a".into(),
            name: "Tenant A".into(),
            quotas: TenantQuotas::default(),
            nats_revoked_user_public_keys: Vec::new(),
            created_at: 1,
        })
        .await
        .unwrap();
    store
        .record_nats_user_revocation("UABC".into())
        .await
        .unwrap();
    store
        .update_tenant(
            "tenant-a",
            "Tenant A updated".into(),
            TenantQuotas::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get_tenant("default")
            .await
            .unwrap()
            .unwrap()
            .nats_revoked_user_public_keys,
        ["UABC"]
    );
    store.remove_tenant("tenant-a").await.unwrap();
    assert!(store.get_tenant("tenant-a").await.unwrap().is_none());
    store.shutdown().await.unwrap();
}
#[tokio::test]
async fn tenant_count_and_desired_state_quotas_are_enforced_transactionally() {
    let (store, _dir) = test_store("tenant-quotas").await;
    store
        .create_tenant(Tenant {
            nats_revoked_user_public_keys: Vec::new(),
            id: "count-limited".into(),
            name: "Count limited".into(),
            quotas: TenantQuotas {
                services: Some(1),
                ..TenantQuotas::default()
            },
            created_at: 1,
        })
        .await
        .unwrap();

    let mut first = sample_service("count-limited.web");
    first.tenant_id = "count-limited".into();
    first.ports.clear();
    store.upsert_service(first).await.unwrap();
    let mut second = sample_service("count-limited.api");
    second.tenant_id = "count-limited".into();
    second.ports.clear();
    assert!(matches!(
        store.upsert_service(second).await,
        Err(StateError::QuotaExceeded(message)) if message == "services limit 1"
    ));
    assert_eq!(
        store
            .list_services()
            .await
            .unwrap()
            .iter()
            .filter(|service| service.tenant_id == "count-limited")
            .count(),
        1
    );

    store
        .create_tenant(Tenant {
            nats_revoked_user_public_keys: Vec::new(),
            id: "bytes-limited".into(),
            name: "Bytes limited".into(),
            quotas: TenantQuotas {
                desired_state_bytes: Some(1),
                ..TenantQuotas::default()
            },
            created_at: 2,
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .upsert_volume(Volume {
                id: "bytes-limited.data".into(),
                tenant_id: "bytes-limited".into(),
                name: "data".into(),
                driver: "local".into(),
            })
            .await,
        Err(StateError::QuotaExceeded(message))
            if message == "desired_state_bytes limit 1"
    ));
    assert!(store.list_volumes().await.unwrap().is_empty());
    store
        .create_tenant(Tenant {
            nats_revoked_user_public_keys: Vec::new(),
            id: "object-limited".into(),
            name: "Object limited".into(),
            quotas: TenantQuotas {
                volumes: Some(0),
                configs: Some(0),
                ..TenantQuotas::default()
            },
            created_at: 3,
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .upsert_volume(Volume {
                id: "object-limited.volume".into(),
                tenant_id: "object-limited".into(),
                name: "volume".into(),
                driver: "local".into(),
            })
            .await,
        Err(StateError::QuotaExceeded(message)) if message == "volumes limit 0"
    ));
    assert!(matches!(
        store
            .upsert_config(Config {
                id: "object-limited.config".into(),
                tenant_id: "object-limited".into(),
                name: "config".into(),
            })
            .await,
        Err(StateError::QuotaExceeded(message)) if message == "configs limit 0"
    ));

    store
        .create_tenant(Tenant {
            nats_revoked_user_public_keys: Vec::new(),
            id: "job-limited".into(),
            name: "Job limited".into(),
            quotas: TenantQuotas {
                jobs: Some(1),
                ..TenantQuotas::default()
            },
            created_at: 4,
        })
        .await
        .unwrap();
    let mut first_job = sample_service("job-limited.first");
    first_job.tenant_id = "job-limited".into();
    first_job.is_job = true;
    first_job.ports.clear();
    store.upsert_service(first_job).await.unwrap();
    let mut second_job = sample_service("job-limited.second");
    second_job.tenant_id = "job-limited".into();
    second_job.ports.clear();
    second_job.is_job = true;
    assert!(matches!(
        store.upsert_service(second_job).await,
        Err(StateError::QuotaExceeded(message)) if message == "jobs limit 1"
    ));
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn nats_public_key_update_preserves_node_and_missing_node_fails() {
    let (store, _dir) = test_store("node-nats-identity").await;
    store.upsert_node(sample_node(7)).await.unwrap();

    store
        .set_node_nats_user_public_key(7, Some("UABC".into()))
        .await
        .unwrap();
    let node = store.get_node(7).await.unwrap().unwrap();
    assert_eq!(node.nats_user_public_key.as_deref(), Some("UABC"));
    assert_eq!(node.labels.get("gpu").map(String::as_str), Some("true"));

    // A full record read before the key was set (registration) must not
    // erase the key that revokes the node's NATS credentials.
    let mut stale = sample_node(7);
    stale.last_seen_unix = Some(42);
    assert!(stale.nats_user_public_key.is_none());
    store.upsert_node(stale).await.unwrap();
    let node = store.get_node(7).await.unwrap().unwrap();
    assert_eq!(node.nats_user_public_key.as_deref(), Some("UABC"));
    assert_eq!(node.last_seen_unix, Some(42));

    assert!(matches!(
        store
            .set_node_nats_user_public_key(999, Some("UDEF".into()))
            .await,
        Err(StateError::NotFound(id)) if id == "node 999"
    ));
    store.shutdown().await.unwrap();
}
