//! End-to-end pipeline tests: Compose YAML → translate → place → reconcile.
//!
//! These tests exercise the pure (non-I/O) path of the scheduler:
//! load a Compose file, translate it to state entities, place tasks on
//! mock nodes, and verify the reconcile diff.

use std::collections::HashMap;

use minga_compose::{LoadOptions, Project};
use minga_scheduler::{place_service, plan, translate_project, ReconcileAction};
use minga_state::types::{ClusterNode, NodeStatus, ResourceSpec, ServiceMode, TaskState};

fn load(yaml: &str) -> Project {
    let loaded = minga_compose::load(yaml, &LoadOptions::default()).expect("compose should load");
    loaded.project
}

fn node(id: u64, cpus: f64, mem: u64, gpus: u32, labels: &[(&str, &str)]) -> ClusterNode {
    ClusterNode {
        id,
        address: format!("10.0.0.{id}:50051"),
        labels: labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>(),
        status: NodeStatus::Active,
        capacity: ResourceSpec {
            cpus,
            memory_bytes: mem,
            gpus,
        },
        wg_public_key: None,
        wg_endpoint: None,
        overlay_ip: None,
    }
}

#[test]
fn full_pipeline_simple_stack() {
    let yaml = r#"
name: plant-7
services:
  web:
    image: nginx:1.25
    ports:
      - "8080:80"
    deploy:
      replicas: 2
  api:
    image: registry.local/api:1.0
    environment:
      - LOG_LEVEL=debug
      - PORT=9000
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: "1.0"
          memory: 512M
"#;
    let project = load(yaml);
    let translated = translate_project(&project, "plant-7").expect("translation should succeed");

    // Verify translation.
    assert_eq!(translated.services.len(), 2);
    let web = translated
        .services
        .iter()
        .find(|s| s.name == "web")
        .unwrap();
    assert_eq!(web.id, "plant-7.web");
    assert_eq!(web.image, "nginx:1.25");
    assert_eq!(web.replicas, 2);
    assert_eq!(web.mode, ServiceMode::Replicated);
    assert_eq!(web.ports.len(), 1);
    assert_eq!(web.ports[0].target, 80);
    assert_eq!(web.ports[0].published, Some(8080));
    assert_eq!(web.networks, vec!["plant-7_default"]);

    let api = translated
        .services
        .iter()
        .find(|s| s.name == "api")
        .unwrap();
    assert_eq!(api.replicas, 3);
    assert_eq!(api.env.get("LOG_LEVEL").map(String::as_str), Some("debug"));
    assert_eq!(api.env.get("PORT").map(String::as_str), Some("9000"));
    assert_eq!(api.resources.cpus, 1.0);
    assert_eq!(api.resources.memory_bytes, 512 * 1024 * 1024);

    // Default network was created.
    assert!(translated
        .networks
        .iter()
        .any(|n| n.id == "plant-7_default"));

    // Place on 3 nodes.
    let nodes = vec![
        node(1, 8.0, 1 << 34, 0, &[]),
        node(2, 8.0, 1 << 34, 0, &[]),
        node(3, 8.0, 1 << 34, 0, &[]),
    ];
    let actions = plan(&translated.services, &[], &nodes);

    // web: 2 replicas, api: 3 replicas → 5 create actions.
    let creates: Vec<&str> = actions
        .iter()
        .filter_map(|a| match a {
            ReconcileAction::CreateTask(t) => Some(t.id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(creates.len(), 5);
    assert!(creates.contains(&"plant-7.web.1"));
    assert!(creates.contains(&"plant-7.web.2"));
    assert!(creates.contains(&"plant-7.api.1"));
    assert!(creates.contains(&"plant-7.api.2"));
    assert!(creates.contains(&"plant-7.api.3"));

    // Re-running reconcile with the placed tasks should be a no-op.
    let tasks: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            ReconcileAction::CreateTask(t) => Some(t.clone()),
            _ => None,
        })
        .collect();
    let actions2 = plan(&translated.services, &tasks, &nodes);
    assert!(actions2.is_empty(), "reconcile should be idempotent");
}

#[test]
fn pipeline_with_constraints_and_global() {
    let yaml = r#"
name: edge
services:
  inference:
    image: registry.local/vllm:0.6.3
    deploy:
      replicas: 1
      placement:
        constraints:
          - node.labels.gpu == true
      resources:
        limits:
          cpus: "4.0"
          memory: 16G
  sensors:
    image: registry.local/sensors:2.1
    deploy:
      mode: global
networks:
  backend:
    driver: overlay
"#;
    let project = load(yaml);
    let translated = translate_project(&project, "edge").expect("translation should succeed");

    let inference = translated
        .services
        .iter()
        .find(|s| s.name == "inference")
        .unwrap();
    assert_eq!(inference.constraints, vec!["node.labels.gpu == true"]);
    assert_eq!(inference.resources.cpus, 4.0);
    assert_eq!(inference.resources.memory_bytes, 16 * 1024 * 1024 * 1024);

    let sensors = translated
        .services
        .iter()
        .find(|s| s.name == "sensors")
        .unwrap();
    assert_eq!(sensors.mode, ServiceMode::Global);

    // Explicit network was translated.
    assert!(translated.networks.iter().any(|n| n.id == "edge_backend"));
    assert_eq!(
        translated
            .networks
            .iter()
            .find(|n| n.id == "edge_backend")
            .unwrap()
            .driver,
        "overlay"
    );

    // Place: only GPU nodes get inference, all nodes get sensors.
    let nodes = vec![
        node(1, 8.0, 1 << 35, 1, &[("gpu", "true")]),
        node(2, 4.0, 1 << 34, 0, &[]),
        node(3, 8.0, 1 << 35, 1, &[("gpu", "true")]),
    ];

    // Inference: 1 replica, GPU constraint → nodes 1 or 3.
    let inference_tasks = place_service(inference, &nodes, &[]);
    assert_eq!(inference_tasks.len(), 1);
    assert!(inference_tasks[0].node_id == 1 || inference_tasks[0].node_id == 3);

    // Sensors: global → one per node.
    let sensors_tasks = place_service(sensors, &nodes, &[]);
    assert_eq!(sensors_tasks.len(), 3);
    let sensor_nodes: Vec<u64> = sensors_tasks.iter().map(|t| t.node_id).collect();
    assert_eq!(sensor_nodes, vec![1, 2, 3]);
}

#[test]
fn pipeline_with_volumes_secrets_configs() {
    let yaml = r#"
name: data
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD_FILE: /run/secrets/db_password
    secrets:
      - db_password
    volumes:
      - pg-data:/var/lib/postgresql/data
volumes:
  pg-data:
secrets:
  db_password:
    file: ./secrets/db_password.txt
configs:
  init_sql:
    file: ./init.sql
"#;
    let project = load(yaml);
    let translated = translate_project(&project, "data").expect("translation should succeed");

    assert_eq!(translated.volumes.len(), 1);
    assert_eq!(translated.volumes[0].id, "data_pg-data");
    assert_eq!(translated.volumes[0].driver, "local");

    assert_eq!(translated.secrets.len(), 1);
    assert_eq!(translated.secrets[0].id, "data_db_password");

    assert_eq!(translated.configs.len(), 1);
    assert_eq!(translated.configs[0].id, "data_init_sql");
}

#[test]
fn pipeline_scale_and_reconcile() {
    let yaml = r#"
name: app
services:
  web:
    image: nginx:latest
    deploy:
      replicas: 3
"#;
    let project = load(yaml);
    let translated = translate_project(&project, "app").unwrap();
    let nodes = vec![node(1, 8.0, 1 << 34, 0, &[]), node(2, 8.0, 1 << 34, 0, &[])];

    // Initial deploy: 3 tasks created.
    let actions = plan(&translated.services, &[], &nodes);
    assert_eq!(actions.len(), 3);

    // Simulate the tasks being created.
    let tasks: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            ReconcileAction::CreateTask(t) => Some(t.clone()),
            _ => None,
        })
        .collect();

    // Scale down to 1 replica.
    let mut scaled = translated.clone();
    scaled.services[0].replicas = 1;
    let actions = plan(&scaled.services, &tasks, &nodes);
    let removes: Vec<&str> = actions
        .iter()
        .filter_map(|a| match a {
            ReconcileAction::RemoveTask { task_id, .. } => Some(task_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        removes.len(),
        2,
        "scaling from 3 to 1 should remove 2 tasks"
    );

    // The surviving task should be app.web.1 (slot 1 is always kept).
    let creates: Vec<&str> = actions
        .iter()
        .filter_map(|a| match a {
            ReconcileAction::CreateTask(t) => Some(t.id.as_str()),
            _ => None,
        })
        .collect();
    assert!(creates.is_empty(), "no new tasks when scaling down");
}

#[test]
fn pipeline_job_mode_fails_loudly() {
    let yaml = r#"
name: batch
services:
  migrate:
    image: registry.local/migrate:1.0
    deploy:
      mode: replicated-job
      replicas: 1
"#;
    let project = load(yaml);
    let result = translate_project(&project, "batch");
    assert!(result.is_err(), "job modes should fail loudly");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("job modes are not supported"));
}

#[test]
fn pipeline_task_states_are_pending() {
    let yaml = r#"
name: test
services:
  web:
    image: nginx:latest
    deploy:
      replicas: 1
"#;
    let project = load(yaml);
    let translated = translate_project(&project, "test").unwrap();
    let nodes = vec![node(1, 4.0, 1 << 34, 0, &[])];
    let tasks = place_service(&translated.services[0], &nodes, &[]);
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].state, TaskState::Pending);
    assert_eq!(tasks[0].spec.image, "nginx:latest");
}

#[test]
fn pipeline_env_list_and_map_forms() {
    let yaml = r#"
name: envtest
services:
  app-list:
    image: app:latest
    environment:
      - FOO=bar
      - BAZ
  app-map:
    image: app:latest
    environment:
      MAP_KEY: map_value
      NUM: 42
"#;
    let project = load(yaml);
    let translated = translate_project(&project, "envtest").unwrap();

    let list_svc = translated
        .services
        .iter()
        .find(|s| s.name == "app-list")
        .unwrap();
    assert_eq!(list_svc.env.get("FOO").map(String::as_str), Some("bar"));
    // BAZ has no value in the compose file; it's resolved from the process
    // env (or empty string if unset).
    assert!(list_svc.env.contains_key("BAZ"));

    let map_svc = translated
        .services
        .iter()
        .find(|s| s.name == "app-map")
        .unwrap();
    assert_eq!(
        map_svc.env.get("MAP_KEY").map(String::as_str),
        Some("map_value")
    );
    assert_eq!(map_svc.env.get("NUM").map(String::as_str), Some("42"));
}
