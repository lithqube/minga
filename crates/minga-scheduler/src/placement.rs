//! Placement: decides which nodes run which tasks for a service.
//!
//! Phase 0 uses a **spread** strategy — replicas are distributed as evenly
//! as possible across eligible nodes (round-robin), with resource-capacity
//! checking. A node is eligible if it is active, passes all of the
//! service's placement constraints, and has enough remaining CPU, memory
//! and GPUs.
//!
//! Task IDs are deterministic: `{service_id}.{slot}` for replicated
//! services (slot = 1..=replicas) and `{service_id}.{node_id}` for global
//! services. This makes reconciliation idempotent — re-running placement
//! produces the same IDs, so the diff in [`crate::reconcile`] is clean.

use std::collections::HashMap;

use minga_state::types::{
    ClusterNode, NodeId, NodeStatus, ResourceSpec, Service, ServiceMode, Task, TaskSpec, TaskState,
};

use crate::constraints;

/// Computes the desired tasks for a service given the current cluster.
///
/// Returns fewer tasks than requested if there are not enough eligible
/// nodes or resources. Never fails — placement is best-effort; the
/// reconcile loop will retry on the next pass when the cluster changes.
///
/// `existing_tasks` should be all tasks in the cluster (not just this
/// service's), so that resource accounting is cross-service.
pub fn place_service(
    service: &Service,
    nodes: &[ClusterNode],
    existing_tasks: &[Task],
) -> Vec<Task> {
    // --- filter eligible nodes ------------------------------------------------
    let eligible: Vec<&ClusterNode> = nodes
        .iter()
        .filter(|n| n.status == NodeStatus::Active)
        .filter(|n| {
            service
                .constraints
                .iter()
                .all(|raw| constraints::matches_str(raw, n))
        })
        .collect();

    if eligible.is_empty() {
        tracing::warn!(
            service = %service.id,
            constraints = ?service.constraints,
            "no eligible nodes for service"
        );
        return Vec::new();
    }

    // --- compute remaining capacity per node ----------------------------------
    // Sum resources consumed by existing tasks (skip terminal states).
    let mut used: HashMap<NodeId, ResourceSpec> = HashMap::new();
    for task in existing_tasks {
        if matches!(task.state, TaskState::Stopped | TaskState::Failed) {
            continue;
        }
        let entry = used.entry(task.node_id).or_default();
        entry.cpus += task.spec.resources.cpus;
        entry.memory_bytes += task.spec.resources.memory_bytes;
        entry.gpus += task.spec.resources.gpus;
    }

    // (node, remaining capacity) pairs we mutate as we place tasks.
    let mut candidates: Vec<(&ClusterNode, ResourceSpec)> = eligible
        .iter()
        .map(|n| {
            let u = used.get(&n.id).cloned().unwrap_or_default();
            (
                *n,
                ResourceSpec {
                    cpus: (n.capacity.cpus - u.cpus).max(0.0),
                    memory_bytes: n.capacity.memory_bytes.saturating_sub(u.memory_bytes),
                    gpus: n.capacity.gpus.saturating_sub(u.gpus),
                },
            )
        })
        .collect();

    let spec = TaskSpec {
        image: service.image.clone(),
        env: service.env.clone(),
        ports: service.ports.clone(),
        resources: service.resources.clone(),
    };

    let mut tasks = Vec::new();

    match service.mode {
        ServiceMode::Global => {
            for (node, avail) in &mut candidates {
                if !fits(avail, &service.resources) {
                    tracing::warn!(
                        service = %service.id,
                        node = node.id,
                        "insufficient resources for global task"
                    );
                    continue;
                }
                tasks.push(Task {
                    id: format!("{}.{}", service.id, node.id),
                    service_id: service.id.clone(),
                    node_id: node.id,
                    state: TaskState::Pending,
                    spec: spec.clone(),
                });
                consume(avail, &service.resources);
            }
        }
        ServiceMode::Replicated => {
            for slot in 1..=service.replicas {
                // Round-robin: start at `slot % len` and wrap around,
                // picking the first node with enough resources.
                let start = (slot as usize - 1) % candidates.len();
                let mut placed = false;
                for i in 0..candidates.len() {
                    let idx = (start + i) % candidates.len();
                    let (node, avail) = &mut candidates[idx];
                    if fits(avail, &service.resources) {
                        tasks.push(Task {
                            id: format!("{}.{}", service.id, slot),
                            service_id: service.id.clone(),
                            node_id: node.id,
                            state: TaskState::Pending,
                            spec: spec.clone(),
                        });
                        consume(avail, &service.resources);
                        placed = true;
                        break;
                    }
                }
                if !placed {
                    tracing::warn!(
                        service = %service.id,
                        slot,
                        "no node with enough resources for replica"
                    );
                }
            }
        }
    }

    tasks
}

/// Returns true if `avail` has enough resources for `request`.
fn fits(avail: &ResourceSpec, request: &ResourceSpec) -> bool {
    avail.cpus >= request.cpus
        && avail.memory_bytes >= request.memory_bytes
        && avail.gpus >= request.gpus
}

/// Subtracts `request` from `avail` (saturating).
fn consume(avail: &mut ResourceSpec, request: &ResourceSpec) {
    avail.cpus = (avail.cpus - request.cpus).max(0.0);
    avail.memory_bytes = avail.memory_bytes.saturating_sub(request.memory_bytes);
    avail.gpus = avail.gpus.saturating_sub(request.gpus);
}

#[cfg(test)]
mod tests {
    use super::*;
    use minga_state::types::PortMapping;
    use std::collections::HashMap;

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
        }
    }

    fn svc(id: &str, replicas: u32, mode: ServiceMode) -> Service {
        Service {
            id: id.to_string(),
            name: id.to_string(),
            image: "nginx:latest".to_string(),
            replicas,
            mode,
            constraints: Vec::new(),
            env: HashMap::new(),
            ports: vec![PortMapping {
                target: 80,
                published: Some(8080),
                protocol: "tcp".to_string(),
            }],
            networks: vec!["test_default".to_string()],
            resources: ResourceSpec::default(),
        }
    }

    #[test]
    fn replicated_spread_across_nodes() {
        let nodes = vec![
            node(1, 4.0, 1 << 30, 0, &[]),
            node(2, 4.0, 1 << 30, 0, &[]),
            node(3, 4.0, 1 << 30, 0, &[]),
        ];
        let service = svc("web", 2, ServiceMode::Replicated);
        let tasks = place_service(&service, &nodes, &[]);
        assert_eq!(tasks.len(), 2);
        // Round-robin: slot 1 → node 1, slot 2 → node 2
        assert_eq!(tasks[0].node_id, 1);
        assert_eq!(tasks[1].node_id, 2);
        assert_eq!(tasks[0].id, "web.1");
        assert_eq!(tasks[1].id, "web.2");
        assert!(tasks.iter().all(|t| t.state == TaskState::Pending));
    }

    #[test]
    fn replicated_more_replicas_than_nodes() {
        let nodes = vec![node(1, 8.0, 1 << 30, 0, &[]), node(2, 8.0, 1 << 30, 0, &[])];
        let service = svc("web", 4, ServiceMode::Replicated);
        let tasks = place_service(&service, &nodes, &[]);
        assert_eq!(tasks.len(), 4);
        // Round-robin: 1→1, 2→2, 3→1, 4→2
        let node_ids: Vec<u64> = tasks.iter().map(|t| t.node_id).collect();
        assert_eq!(node_ids, vec![1, 2, 1, 2]);
    }

    #[test]
    fn global_one_per_node() {
        let nodes = vec![
            node(1, 4.0, 1 << 30, 0, &[]),
            node(2, 4.0, 1 << 30, 0, &[]),
            node(3, 4.0, 1 << 30, 0, &[]),
        ];
        let service = svc("agent", 0, ServiceMode::Global);
        let tasks = place_service(&service, &nodes, &[]);
        assert_eq!(tasks.len(), 3);
        let node_ids: Vec<u64> = tasks.iter().map(|t| t.node_id).collect();
        assert_eq!(node_ids, vec![1, 2, 3]);
        assert_eq!(tasks[0].id, "agent.1");
        assert_eq!(tasks[1].id, "agent.2");
    }

    #[test]
    fn constraint_filters_nodes() {
        let nodes = vec![
            node(1, 4.0, 1 << 30, 1, &[("gpu", "true")]),
            node(2, 4.0, 1 << 30, 0, &[]),
            node(3, 4.0, 1 << 30, 1, &[("gpu", "true")]),
        ];
        let mut service = svc("train", 2, ServiceMode::Replicated);
        service.constraints = vec!["node.labels.gpu == true".to_string()];
        let tasks = place_service(&service, &nodes, &[]);
        assert_eq!(tasks.len(), 2);
        assert!(tasks.iter().all(|t| t.node_id == 1 || t.node_id == 3));
    }

    #[test]
    fn no_eligible_nodes() {
        let nodes = vec![node(1, 4.0, 1 << 30, 0, &[])];
        let mut service = svc("web", 1, ServiceMode::Replicated);
        service.constraints = vec!["node.labels.gpu == true".to_string()];
        let tasks = place_service(&service, &nodes, &[]);
        assert!(tasks.is_empty());
    }

    #[test]
    fn resource_capacity_respected() {
        // Node 1 has 2 CPUs, node 2 has 4 CPUs. Service wants 2 CPUs per replica.
        let nodes = vec![node(1, 2.0, 1 << 30, 0, &[]), node(2, 4.0, 1 << 30, 0, &[])];
        let mut service = svc("cpu-hungry", 2, ServiceMode::Replicated);
        service.resources = ResourceSpec {
            cpus: 2.0,
            memory_bytes: 0,
            gpus: 0,
        };
        let tasks = place_service(&service, &nodes, &[]);
        assert_eq!(tasks.len(), 2);
        // Slot 1 → node 1 (round-robin start), slot 2 → node 2
        // (node 1 is full after slot 1)
        assert_eq!(tasks[0].node_id, 1);
        assert_eq!(tasks[1].node_id, 2);
    }

    #[test]
    fn existing_tasks_consume_capacity() {
        let nodes = vec![node(1, 4.0, 1 << 30, 0, &[]), node(2, 8.0, 1 << 30, 0, &[])];
        let mut service = svc("web", 2, ServiceMode::Replicated);
        service.resources = ResourceSpec {
            cpus: 3.0,
            memory_bytes: 0,
            gpus: 0,
        };
        // Existing task on node 1 uses 3 CPUs, leaving 1 — not enough for a 3-CPU task.
        let existing = vec![Task {
            id: "other.1".to_string(),
            service_id: "other".to_string(),
            node_id: 1,
            state: TaskState::Running,
            spec: TaskSpec {
                image: "other:latest".to_string(),
                env: HashMap::new(),
                ports: vec![],
                resources: ResourceSpec {
                    cpus: 3.0,
                    memory_bytes: 0,
                    gpus: 0,
                },
            },
        }];
        let tasks = place_service(&service, &nodes, &existing);
        assert_eq!(tasks.len(), 2);
        // Both replicas must go to node 2 (node 1 only has 1 CPU left).
        assert!(tasks.iter().all(|t| t.node_id == 2));
    }

    #[test]
    fn stopped_tasks_do_not_consume_capacity() {
        let nodes = vec![node(1, 2.0, 1 << 30, 0, &[])];
        let mut service = svc("web", 1, ServiceMode::Replicated);
        service.resources = ResourceSpec {
            cpus: 2.0,
            memory_bytes: 0,
            gpus: 0,
        };
        let existing = vec![Task {
            id: "old.1".to_string(),
            service_id: "old".to_string(),
            node_id: 1,
            state: TaskState::Stopped,
            spec: TaskSpec {
                image: "old:latest".to_string(),
                env: HashMap::new(),
                ports: vec![],
                resources: ResourceSpec {
                    cpus: 2.0,
                    memory_bytes: 0,
                    gpus: 0,
                },
            },
        }];
        let tasks = place_service(&service, &nodes, &existing);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].node_id, 1);
    }

    #[test]
    fn inactive_nodes_excluded() {
        let mut n1 = node(1, 4.0, 1 << 30, 0, &[]);
        n1.status = NodeStatus::Draining;
        let nodes = vec![n1, node(2, 4.0, 1 << 30, 0, &[])];
        let service = svc("web", 2, ServiceMode::Replicated);
        let tasks = place_service(&service, &nodes, &[]);
        assert_eq!(tasks.len(), 2);
        assert!(tasks.iter().all(|t| t.node_id == 2));
    }

    #[test]
    fn gpu_constraint_and_capacity() {
        let nodes = vec![
            node(1, 8.0, 1 << 34, 2, &[("gpu", "true")]),
            node(2, 8.0, 1 << 34, 1, &[("gpu", "true")]),
        ];
        let mut service = svc("train", 3, ServiceMode::Replicated);
        service.constraints = vec!["node.labels.gpu == true".to_string()];
        service.resources = ResourceSpec {
            cpus: 4.0,
            memory_bytes: 1 << 30,
            gpus: 1,
        };
        let tasks = place_service(&service, &nodes, &[]);
        // Node 1 has 2 GPUs, node 2 has 1. 3 replicas × 1 GPU each.
        // Round-robin: slot 1 → node 1, slot 2 → node 2, slot 3 → node 1.
        assert_eq!(tasks.len(), 3);
        let counts: HashMap<u64, usize> = tasks.iter().fold(HashMap::new(), |mut acc, t| {
            *acc.entry(t.node_id).or_insert(0) += 1;
            acc
        });
        assert_eq!(counts.get(&1), Some(&2));
        assert_eq!(counts.get(&2), Some(&1));
    }

    #[test]
    fn deterministic_task_ids() {
        let nodes = vec![node(1, 4.0, 1 << 30, 0, &[]), node(2, 4.0, 1 << 30, 0, &[])];
        let service = svc("web", 2, ServiceMode::Replicated);
        let tasks_a = place_service(&service, &nodes, &[]);
        let tasks_b = place_service(&service, &nodes, &[]);
        let ids_a: Vec<&str> = tasks_a.iter().map(|t| t.id.as_str()).collect();
        let ids_b: Vec<&str> = tasks_b.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids_a, ids_b);
    }
}
