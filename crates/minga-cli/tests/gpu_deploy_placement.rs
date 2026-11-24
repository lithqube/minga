//! A GPU reservation in a Compose file must survive `minga deploy`
//! (Compose → translate → Docker spec → API → state) and steer placement.

use std::collections::HashMap;

use minga_compose::LoadOptions;
use minga_scheduler::{place_service, translate_project};
use minga_state::types::{ClusterNode, NodeStatus, ResourceSpec, DEFAULT_TENANT_ID};

fn node(id: u64, gpus: u32) -> ClusterNode {
    ClusterNode {
        id,
        address: format!("10.0.0.{id}:50051"),
        labels: HashMap::new(),
        status: NodeStatus::Active,
        capacity: ResourceSpec {
            cpus: 8.0,
            memory_bytes: 1 << 35,
            gpus,
        },
        wg_public_key: None,
        wg_endpoint: None,
        overlay_ip: None,
        tenant_id: DEFAULT_TENANT_ID.to_owned(),
        last_seen_unix: None,
        role: Default::default(),
        reserved_ports: Vec::new(),
        nats_user_public_key: None,
    }
}

#[test]
fn compose_gpu_reservation_survives_deploy_and_places_only_on_a_gpu_node() {
    let yaml = r#"
name: edge
services:
  inference:
    image: registry.local/vllm:0.6.3
    deploy:
      replicas: 2
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
"#;
    let project = minga_compose::load(yaml, &LoadOptions::default())
        .expect("compose should load")
        .project;
    let translated = translate_project(&project, "edge", None).unwrap();

    // What `minga deploy` posts, and what the API stores from it.
    let svc = &translated.services[0];
    minga_api::translate::docker_round_trip_check(svc).unwrap();
    let spec = minga_api::translate::service_to_docker(svc).Spec;
    let stored = minga_api::translate::service_spec_to_state(&spec).unwrap();
    assert_eq!(stored.resources.gpus, 1);

    // Node 1 has no GPU and is listed first.
    let nodes = vec![node(1, 0), node(2, 1), node(3, 1)];
    let tasks = place_service(&stored, &nodes, &[], &[], 0);
    let mut placed: Vec<u64> = tasks.iter().map(|t| t.node_id).collect();
    placed.sort_unstable();
    assert_eq!(placed, [2, 3]);
}
