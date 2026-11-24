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
            host_ip: None,
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
        egress: None,
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
async fn upsert_network_refuses_subnets_the_overlay_must_not_route() {
    let (store, _dir) = test_store("upsert_network_refuses_subnets").await;
    let net = |id: &str, subnet: &str| Network {
        id: id.to_string(),
        tenant_id: "default".to_string(),
        name: id.to_string(),
        driver: "overlay".to_string(),
        attachable: true,
        subnet: Some(subnet.to_string()),
        wg_port: None,
    };
    // Before the manager sets a supernet only the mesh and size rules apply.
    store
        .upsert_network(net("lan", "192.168.1.0/24"))
        .await
        .unwrap();
    store.set_overlay_supernet(minga_net::Cidr::parse("10.100.0.0/16").unwrap());
    for (id, subnet) in [
        ("outside", "192.168.2.0/24"),
        ("mesh", "10.200.0.0/24"),
        ("host", "10.100.9.7/32"),
        ("garbage", "not-a-cidr"),
    ] {
        let error = store.upsert_network(net(id, subnet)).await.unwrap_err();
        assert!(matches!(error, StateError::Raft(_)), "{id}: {error:?}");
        assert!(
            store.get_network(id).await.unwrap().is_none(),
            "{id} stored"
        );
    }
    store
        .upsert_network(net("inside", "10.100.9.0/24"))
        .await
        .unwrap();

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn upsert_network_refuses_a_subnet_another_network_holds() {
    let (store, _dir) = test_store("upsert_network_overlap").await;
    let net = |id: &str, subnet: Option<&str>| Network {
        id: id.to_string(),
        tenant_id: "default".to_string(),
        name: id.to_string(),
        driver: "overlay".to_string(),
        attachable: true,
        subnet: subnet.map(String::from),
        wg_port: None,
    };
    store
        .upsert_network(net("a", Some("10.100.5.0/24")))
        .await
        .unwrap();
    for (id, subnet) in [
        ("same", "10.100.5.0/24"),
        ("inner", "10.100.5.128/25"),
        ("outer", "10.100.4.0/23"),
    ] {
        let error = store
            .upsert_network(net(id, Some(subnet)))
            .await
            .unwrap_err();
        assert!(
            matches!(error, StateError::SubnetOverlap),
            "{id}: {error:?}"
        );
        assert!(
            store.get_network(id).await.unwrap().is_none(),
            "{id} stored"
        );
    }
    // Rewriting the holder, a free subnet and a network with no subnet yet are fine.
    store
        .upsert_network(net("a", Some("10.100.5.0/24")))
        .await
        .unwrap();
    store
        .upsert_network(net("free", Some("10.100.6.0/24")))
        .await
        .unwrap();
    store.upsert_network(net("none", None)).await.unwrap();
    // A network may not move onto another's subnet either.
    let error = store
        .upsert_network(net("free", Some("10.100.5.0/25")))
        .await
        .unwrap_err();
    assert!(matches!(error, StateError::SubnetOverlap), "{error:?}");
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_network_with_a_live_task_cannot_be_removed() {
    let (store, _dir) = test_store("network_live_task_remove").await;
    let net = Network {
        id: "net".to_string(),
        tenant_id: "default".to_string(),
        name: "net".to_string(),
        driver: "overlay".to_string(),
        attachable: true,
        subnet: Some("10.100.5.0/24".to_string()),
        wg_port: None,
    };
    store.upsert_network(net.clone()).await.unwrap();
    let mut task = sample_task("t1", "orphan", 1);
    task.state = TaskState::Running;
    task.spec.ips.insert("net".into(), "10.100.5.2".into());
    store.upsert_task(task.clone()).await.unwrap();

    let error = store
        .remove_network("net".into(), "default")
        .await
        .unwrap_err();
    assert!(matches!(error, StateError::Conflict(_)), "{error:?}");
    assert!(store.get_network("net").await.unwrap().is_some());

    // Once the task is terminal the network goes, and its subnet is free again.
    task.state = TaskState::Stopped;
    store.upsert_task(task).await.unwrap();
    store.remove_network("net".into(), "default").await.unwrap();
    assert!(store.get_network("net").await.unwrap().is_none());
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
        generation: 0,
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
    store
        .remove_volume("tenant-a-data".into(), "tenant-a")
        .await
        .unwrap();

    let node = minga_state::types::ClusterNode {
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
    // A node never moves between tenants in place; it is removed first.
    store.remove_node(node.id).await.unwrap();
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
            generation: 0,
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
            generation: 0,
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
            generation: 0,
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
            generation: 0,
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
            generation: 0,
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
            generation: 0,
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

fn named_tenant(id: &str, quotas: TenantQuotas) -> Tenant {
    Tenant {
        nats_revoked_user_public_keys: Vec::new(),
        id: id.into(),
        name: id.into(),
        quotas,
        created_at: 1,
        generation: 0,
    }
}

#[tokio::test]
async fn writes_under_a_missing_tenant_are_refused_but_default_is_permanent() {
    let (store, _dir) = test_store("missing-tenant").await;
    let mut service = sample_service("ghost.web");
    service.tenant_id = "ghost".into();
    service.ports.clear();
    assert!(matches!(
        store.upsert_service(service).await,
        Err(StateError::NotFound(_))
    ));
    let mut node = sample_node(9);
    node.tenant_id = "ghost".into();
    assert!(matches!(
        store.upsert_node(node).await,
        Err(StateError::NotFound(_))
    ));
    assert!(store.list_services().await.unwrap().is_empty());
    store.upsert_node(sample_node(9)).await.unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_node_keeps_its_tenant_and_removals_only_touch_the_owners_object() {
    let (store, _dir) = test_store("owned-writes").await;
    for id in ["tenant-a", "tenant-b"] {
        store
            .create_tenant(named_tenant(id, TenantQuotas::default()))
            .await
            .unwrap();
    }
    store.upsert_node(sample_node(4)).await.unwrap();
    let mut takeover = sample_node(4);
    takeover.tenant_id = "tenant-a".into();
    assert!(matches!(
        store.upsert_node(takeover).await,
        Err(StateError::NameInUse)
    ));
    assert_eq!(
        store.get_node(4).await.unwrap().unwrap().tenant_id,
        "default"
    );

    let mut service = sample_service("shared.web");
    service.tenant_id = "tenant-b".into();
    service.ports.clear();
    store.upsert_service(service).await.unwrap();
    // A delayed removal by tenant A must not delete tenant B's object.
    store
        .remove_service("shared.web".into(), "tenant-a")
        .await
        .unwrap();
    assert!(store.get_service("shared.web").await.unwrap().is_some());
    store
        .remove_service("shared.web".into(), "tenant-b")
        .await
        .unwrap();
    assert!(store.get_service("shared.web").await.unwrap().is_none());
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn quotas_ignore_stopped_tasks_and_never_block_rewrites() {
    let (store, _dir) = test_store("quota-rewrites").await;
    store
        .create_tenant(named_tenant(
            "limited",
            TenantQuotas {
                tasks: Some(1),
                ..TenantQuotas::default()
            },
        ))
        .await
        .unwrap();
    let task = |id: &str, state: TaskState| {
        let mut task = sample_task(id, "limited.web", 1);
        task.tenant_id = "limited".into();
        task.state = state;
        task
    };
    store
        .upsert_task(task("limited.web.1", TaskState::Running))
        .await
        .unwrap();
    assert!(matches!(
        store
            .upsert_task(task("limited.web.2", TaskState::Pending))
            .await,
        Err(StateError::QuotaExceeded(_))
    ));
    // A status write on an existing task never grows the count.
    store
        .upsert_task(task("limited.web.1", TaskState::Failed))
        .await
        .unwrap();
    // A failed task no longer holds a slot.
    store
        .upsert_task(task("limited.web.2", TaskState::Pending))
        .await
        .unwrap();

    // Lowering the byte quota below current usage still lets an object be
    // rewritten at the same size, but not grown.
    let existing = store.list_tasks().await.unwrap();
    let used: u64 = existing
        .iter()
        .map(|task| serde_json::to_vec(task).unwrap().len() as u64)
        .sum();
    store
        .update_tenant(
            "limited",
            "limited".into(),
            TenantQuotas {
                tasks: Some(1),
                desired_state_bytes: Some(used / 2),
                ..TenantQuotas::default()
            },
        )
        .await
        .unwrap();
    store
        .upsert_task(task("limited.web.2", TaskState::Running))
        .await
        .unwrap();
    let mut grown = task("limited.web.2", TaskState::Running);
    grown.spec.image = "x".repeat(4096);
    assert!(matches!(
        store.upsert_task(grown).await,
        Err(StateError::QuotaExceeded(_))
    ));
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_snapshot_with_one_refused_network_writes_none() {
    let (store, _dir) = test_store("import-refused-network").await;
    store.set_overlay_supernet(minga_net::Cidr::parse("10.100.0.0/16").unwrap());
    let net = |id: &str, subnet: &str| Network {
        id: id.to_string(),
        tenant_id: "default".to_string(),
        name: id.to_string(),
        driver: "overlay".to_string(),
        attachable: true,
        subnet: Some(subnet.to_string()),
        wg_port: None,
    };
    let mut snapshot = store.export().await.unwrap();
    snapshot.networks = vec![net("a", "10.100.1.0/24"), net("b", "192.168.1.0/24")];
    let error = store.import(snapshot, true).await.unwrap_err();
    assert!(matches!(error, StateError::BadSnapshot(_)), "{error:?}");
    assert!(store.get_network("a").await.unwrap().is_none());
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_snapshot_with_overlapping_networks_is_refused_before_any_write() {
    let (store, _dir) = test_store("import-overlapping-networks").await;
    let net = |id: &str, subnet: &str| Network {
        id: id.to_string(),
        tenant_id: "default".to_string(),
        name: id.to_string(),
        driver: "overlay".to_string(),
        attachable: true,
        subnet: Some(subnet.to_string()),
        wg_port: None,
    };
    store
        .upsert_network(net("held", "10.100.9.0/24"))
        .await
        .unwrap();
    // Two networks of one snapshot on a subnet, then one on a stored subnet.
    for networks in [
        vec![net("a", "10.100.7.0/24"), net("b", "10.100.7.0/24")],
        vec![net("a", "10.100.1.0/24"), net("c", "10.100.9.128/25")],
    ] {
        let mut snapshot = store.export().await.unwrap();
        snapshot.networks = networks;
        let error = store.import(snapshot, true).await.unwrap_err();
        assert!(matches!(error, StateError::BadSnapshot(_)), "{error:?}");
        assert!(
            store.get_network("a").await.unwrap().is_none(),
            "partial import"
        );
    }
    // The stored holder re-importing its own subnet is fine.
    let mut snapshot = store.export().await.unwrap();
    snapshot.networks = vec![net("held", "10.100.9.0/24"), net("a", "10.100.1.0/24")];
    store.import(snapshot, true).await.unwrap();
    assert!(store.get_network("a").await.unwrap().is_some());
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn importing_a_snapshot_keeps_existing_tenant_quotas() {
    let (store, _dir) = test_store("import-quotas").await;
    let quotas = TenantQuotas {
        services: Some(3),
        ..TenantQuotas::default()
    };
    store
        .update_tenant("default", "Default".into(), quotas.clone())
        .await
        .unwrap();
    // A pre-v8 snapshot carries no tenants, so import synthesizes `default`.
    let mut snapshot = store.export().await.unwrap();
    snapshot.tenants.clear();
    store.import(snapshot, true).await.unwrap();
    let default = store.get_tenant("default").await.unwrap().unwrap();
    assert_eq!(default.quotas, quotas);
    assert_eq!(default.name, "Default");
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_wireguard_key_held_by_another_node_is_refused() {
    let (store, _dir) = test_store("wg-key-unique").await;
    store
        .create_tenant(named_tenant("tenant-a", TenantQuotas::default()))
        .await
        .unwrap();
    const KEY: &str = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
    let mut holder = sample_node(1);
    holder.wg_public_key = Some(KEY.into());
    store.upsert_node(holder.clone()).await.unwrap();
    // The holder may re-register under its own key.
    store.upsert_node(holder).await.unwrap();

    // Another tenant's node may not take the key, and nothing is written.
    let mut copier = sample_node(2);
    copier.tenant_id = "tenant-a".into();
    copier.wg_public_key = Some(KEY.into());
    assert!(matches!(
        store.upsert_node(copier.clone()).await,
        Err(StateError::NameInUse)
    ));
    assert!(store.get_node(2).await.unwrap().is_none());

    // Once the holder is gone the key is free again.
    store.remove_node(1).await.unwrap();
    store.upsert_node(copier).await.unwrap();
    store.shutdown().await.unwrap();
}
