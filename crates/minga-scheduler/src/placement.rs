//! Placement: decides which nodes run which tasks for a service.
//!
//! Phase 0 uses a **spread** strategy — replicas are distributed as evenly
//! as possible across eligible nodes (round-robin), with resource-capacity
//! checking. A node is eligible if it is active, its membership lease is
//! valid at `now_unix` (or it carries no lease), it passes all of the
//! service's placement constraints, and it has enough remaining CPU, memory
//! and GPUs.
//!
//! Task IDs are deterministic: `{service_id}.{slot}.{revision}` for
//! replicated services (slot = 1..=replicas) and
//! `{service_id}.{node_id}.{revision}` for global services, where the
//! revision is a hash of the task spec. Re-running placement produces the
//! same IDs, so the diff in [`crate::reconcile`] is clean; changing a
//! service produces new IDs, which is what starts a rollout.

use std::collections::HashMap;

use sha2::{Digest, Sha256};

use minga_state::types::{
    ClusterNode, Network, NetworkId, NodeId, NodeStatus, ResourceSpec, Service, ServiceMode, Task,
    TaskSpec, TaskState, WorkloadKind, NODE_LEASE_TTL_SECS,
};

use crate::{constraints, netassign};

/// A node counts for placement if it is active, its lease is valid at
/// `now_unix`, and it matches all of the service's constraints.
fn is_eligible(n: &ClusterNode, service: &Service, now_unix: u64) -> bool {
    n.status == NodeStatus::Active
        && n.is_live_at(now_unix, NODE_LEASE_TTL_SECS)
        && service
            .constraints
            .iter()
            .all(|raw| constraints::matches_str(raw, n))
}

/// Why a service cannot be placed right now.
///
/// Deploy-time checks ([`check_placeable`]) surface this as a hard error;
/// the reconcile loop keeps placement best-effort and retries.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    /// No node is active, live and constraint-compatible.
    #[error(
        "no eligible nodes for service {service}: 0 of {nodes} registered node(s) are active, live and match constraints {constraints:?}"
    )]
    NoEligibleNodes {
        service: String,
        nodes: usize,
        constraints: Vec<String>,
    },
    /// Eligible nodes exist but none has room for even one task.
    #[error(
        "insufficient capacity for service {service}: {eligible} eligible node(s), none with enough free CPU/memory/GPU"
    )]
    InsufficientCapacity { service: String, eligible: usize },
}

/// Deploy-time hard check: fails if `service` would get zero tasks.
///
/// A service that asks for nothing (replicated with `replicas = 0`) always
/// passes. Use this on write paths (service create/update, deploy); the
/// reconcile loop itself stays best-effort.
pub fn check_placeable(
    service: &Service,
    nodes: &[ClusterNode],
    existing_tasks: &[Task],
    networks: &[Network],
    now_unix: u64,
) -> Result<(), PlacementError> {
    let wants_tasks = match service.mode {
        ServiceMode::Replicated => service.replicas > 0,
        ServiceMode::Global => true,
    };
    if !wants_tasks {
        return Ok(());
    }

    let eligible = nodes
        .iter()
        .filter(|n| is_eligible(n, service, now_unix))
        .count();
    if eligible == 0 {
        return Err(PlacementError::NoEligibleNodes {
            service: service.id.clone(),
            nodes: nodes.len(),
            constraints: service.constraints.clone(),
        });
    }
    if place_service(service, nodes, existing_tasks, networks, now_unix).is_empty() {
        return Err(PlacementError::InsufficientCapacity {
            service: service.id.clone(),
            eligible,
        });
    }
    Ok(())
}

/// Computes the desired tasks for a service given the current cluster.
///
/// Returns fewer tasks than requested if there are not enough eligible
/// nodes or resources. Never fails — placement is best-effort; the
/// reconcile loop will retry on the next pass when the cluster changes.
///
/// `existing_tasks` should be all tasks in the cluster (not just this
/// service's), so that resource accounting is cross-service. `now_unix` is
/// the current unix time, used to exclude nodes with expired leases.
/// `networks` is the full cluster network list, used to assign a
/// deterministic IP per task and network ([`crate::netassign`]).
pub fn place_service(
    service: &Service,
    nodes: &[ClusterNode],
    existing_tasks: &[Task],
    networks: &[Network],
    now_unix: u64,
) -> Vec<Task> {
    // --- filter eligible nodes ------------------------------------------------
    let eligible: Vec<&ClusterNode> = nodes
        .iter()
        .filter(|n| is_eligible(n, service, now_unix))
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
        // A service's own tasks are what this placement keeps or replaces;
        // counting them would evict it from a nearly full node.
        if task.service_id == service.id
            || matches!(task.state, TaskState::Stopped | TaskState::Failed)
        {
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

    let kind = match &service.agent {
        Some(agent) => WorkloadKind::Agent(agent.clone()),
        None => WorkloadKind::Container,
    };

    // One IPAM per joined network, seeded with live allocations: tasks
    // created in this call cannot collide with existing tasks or each
    // other. Agent workloads share the host sandbox and get no IPs.
    let is_agent = service.agent.is_some();
    let mut ipams: Vec<(NetworkId, minga_net::Ipam)> = if is_agent {
        Vec::new()
    } else {
        service
            .networks
            .iter()
            .filter_map(|net_id| {
                let network = networks.iter().find(|n| &n.id == net_id)?;
                netassign::ipam_for(network, existing_tasks)
                    .map_err(|e| {
                        tracing::warn!(
                            service = %service.id,
                            network = %net_id,
                            error = %e,
                            "cannot set up IPAM for network"
                        );
                        e
                    })
                    .ok()
                    .map(|ipam| (net_id.clone(), ipam))
            })
            .collect()
    };

    let spec = TaskSpec {
        image: service.image.clone(),
        env: service.env.clone(),
        ports: service.ports.clone(),
        resources: service.resources.clone(),
        kind,
        restart_policy: service.restart_policy.clone(),
        // Secret references travel with the task so the node can resolve
        // the value locally. References only — no values in Raft.
        secrets: service.secrets.clone(),
        // Filled in per task below.
        ips: HashMap::new(),
        command: service.command.clone(),
        entrypoint: service.entrypoint.clone(),
        labels: service.labels.clone(),
        healthcheck: service.healthcheck.clone(),
        mounts: service.mounts.clone(),
        user: service.user.clone(),
    };
    let revision = spec_revision(&spec);

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
                let task_id = format!("{}.{}.{revision}", service.id, node.id);
                let mut spec = spec.clone();
                spec.ips = allocate_ips(&task_id, &mut ipams);
                tasks.push(Task {
                    id: task_id,
                    service_id: service.id.clone(),
                    node_id: node.id,
                    state: TaskState::Pending,
                    spec,
                    restarts: 0,
                    running_since_unix: None,
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
                        let task_id = format!("{}.{}.{revision}", service.id, slot);
                        let mut spec = spec.clone();
                        spec.ips = allocate_ips(&task_id, &mut ipams);
                        tasks.push(Task {
                            id: task_id,
                            service_id: service.id.clone(),
                            node_id: node.id,
                            state: TaskState::Pending,
                            spec,
                            restarts: 0,
                            running_since_unix: None,
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

/// Short content hash of a task spec, taken before IPs are assigned. It
/// goes through `serde_json::Value` because that sorts map keys, and
/// `HashMap` order differs between instances.
fn spec_revision(spec: &TaskSpec) -> String {
    let canonical = serde_json::to_value(spec).expect("task spec serializes");
    format!("{:x}", Sha256::digest(canonical.to_string()))[..8].to_string()
}

/// The slot a task fills: its ID without the revision suffix.
pub fn slot_of(task_id: &str) -> &str {
    task_id.rsplit_once('.').map_or(task_id, |(slot, _)| slot)
}

/// Allocates an IP for `task_id` on each IPAM-tracked network. A network
/// whose subnet is exhausted logs a warning and yields no IP — placement
/// itself stays best-effort.
fn allocate_ips(
    task_id: &str,
    ipams: &mut [(NetworkId, minga_net::Ipam)],
) -> HashMap<NetworkId, String> {
    let mut ips = HashMap::new();
    for (net_id, ipam) in ipams.iter_mut() {
        match ipam.allocate(task_id) {
            Ok(ip) => {
                ips.insert(net_id.clone(), ip.to_string());
            }
            Err(e) => {
                tracing::warn!(task = %task_id, network = %net_id, error = %e, "no IP allocated")
            }
        }
    }
    ips
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
            wg_public_key: None,
            wg_endpoint: None,
            overlay_ip: None,
            last_seen_unix: None,
            role: Default::default(),
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
        let tasks = place_service(&service, &nodes, &[], &[], 0);
        assert_eq!(tasks.len(), 2);
        // Round-robin: slot 1 → node 1, slot 2 → node 2
        assert_eq!(tasks[0].node_id, 1);
        assert_eq!(tasks[1].node_id, 2);
        assert_eq!(slot_of(&tasks[0].id), "web.1");
        assert_eq!(slot_of(&tasks[1].id), "web.2");
        assert!(tasks.iter().all(|t| t.state == TaskState::Pending));
    }

    #[test]
    fn replicated_more_replicas_than_nodes() {
        let nodes = vec![node(1, 8.0, 1 << 30, 0, &[]), node(2, 8.0, 1 << 30, 0, &[])];
        let service = svc("web", 4, ServiceMode::Replicated);
        let tasks = place_service(&service, &nodes, &[], &[], 0);
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
        let tasks = place_service(&service, &nodes, &[], &[], 0);
        assert_eq!(tasks.len(), 3);
        let node_ids: Vec<u64> = tasks.iter().map(|t| t.node_id).collect();
        assert_eq!(node_ids, vec![1, 2, 3]);
        assert_eq!(slot_of(&tasks[0].id), "agent.1");
        assert_eq!(slot_of(&tasks[1].id), "agent.2");
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
        let tasks = place_service(&service, &nodes, &[], &[], 0);
        assert_eq!(tasks.len(), 2);
        assert!(tasks.iter().all(|t| t.node_id == 1 || t.node_id == 3));
    }

    #[test]
    fn no_eligible_nodes() {
        let nodes = vec![node(1, 4.0, 1 << 30, 0, &[])];
        let mut service = svc("web", 1, ServiceMode::Replicated);
        service.constraints = vec!["node.labels.gpu == true".to_string()];
        let tasks = place_service(&service, &nodes, &[], &[], 0);
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
        let tasks = place_service(&service, &nodes, &[], &[], 0);
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
                ..Default::default()
            },
            restarts: 0,
            running_since_unix: None,
        }];
        let tasks = place_service(&service, &nodes, &existing, &[], 0);
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
                ..Default::default()
            },
            restarts: 0,
            running_since_unix: None,
        }];
        let tasks = place_service(&service, &nodes, &existing, &[], 0);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].node_id, 1);
    }

    #[test]
    fn inactive_nodes_excluded() {
        let mut n1 = node(1, 4.0, 1 << 30, 0, &[]);
        n1.status = NodeStatus::Draining;
        let nodes = vec![n1, node(2, 4.0, 1 << 30, 0, &[])];
        let service = svc("web", 2, ServiceMode::Replicated);
        let tasks = place_service(&service, &nodes, &[], &[], 0);
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
        let tasks = place_service(&service, &nodes, &[], &[], 0);
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
    fn revision_ignores_map_order_and_tracks_the_spec() {
        let nodes = vec![node(1, 4.0, 1 << 30, 0, &[])];
        let env = |rev: bool| -> HashMap<String, String> {
            let mut keys: Vec<u32> = (0..64).collect();
            if rev {
                keys.reverse();
            }
            keys.iter().map(|k| (format!("K{k}"), "v".into())).collect()
        };
        let mut a = svc("web", 1, ServiceMode::Replicated);
        a.env = env(false);
        let mut b = a.clone();
        b.env = env(true);
        let mut c = a.clone();
        c.env.insert("K0".to_string(), "changed".to_string());

        let id = |s: &Service| place_service(s, &nodes, &[], &[], 0)[0].id.clone();
        assert_eq!(id(&a), id(&b));
        assert_ne!(id(&a), id(&c));
        assert_eq!(slot_of(&id(&a)), slot_of(&id(&c)));
    }

    #[test]
    fn own_tasks_do_not_evict_the_service_from_a_full_node() {
        let nodes = vec![node(1, 1.0, 1 << 30, 0, &[])];
        let mut service = svc("web", 1, ServiceMode::Replicated);
        service.resources.cpus = 1.0;
        let mut existing = place_service(&service, &nodes, &[], &[], 0);
        existing[0].state = TaskState::Running;
        assert_eq!(place_service(&service, &nodes, &existing, &[], 0).len(), 1);
    }

    #[test]
    fn deterministic_task_ids() {
        let nodes = vec![node(1, 4.0, 1 << 30, 0, &[]), node(2, 4.0, 1 << 30, 0, &[])];
        let service = svc("web", 2, ServiceMode::Replicated);
        let tasks_a = place_service(&service, &nodes, &[], &[], 0);
        let tasks_b = place_service(&service, &nodes, &[], &[], 0);
        let ids_a: Vec<&str> = tasks_a.iter().map(|t| t.id.as_str()).collect();
        let ids_b: Vec<&str> = tasks_b.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids_a, ids_b);
    }

    #[test]
    fn expired_lease_makes_node_ineligible() {
        let mut stale = node(1, 4.0, 1 << 30, 0, &[]);
        stale.last_seen_unix = Some(1_000);
        let fresh = node(2, 4.0, 1 << 30, 0, &[]);
        let mut leased = fresh.clone();
        leased.last_seen_unix = Some(1_000 + NODE_LEASE_TTL_SECS);

        let service = svc("web", 1, ServiceMode::Replicated);
        // One second past the TTL boundary: node 1's lease is expired,
        // node 2's is still valid.
        let now = 1_000 + NODE_LEASE_TTL_SECS + 1;
        let tasks = place_service(&service, &[stale, leased], &[], &[], now);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].node_id, 2);
    }

    #[test]
    fn node_without_lease_is_always_live() {
        let service = svc("web", 1, ServiceMode::Replicated);
        // `node()` builds lease-less (operator-registered) nodes.
        let tasks = place_service(
            &service,
            &[node(1, 4.0, 1 << 30, 0, &[])],
            &[],
            &[],
            u64::MAX,
        );
        assert_eq!(tasks.len(), 1);
    }

    #[test]
    fn check_placeable_rejects_when_no_nodes_registered() {
        let service = svc("web", 1, ServiceMode::Replicated);
        let err = check_placeable(&service, &[], &[], &[], 0).unwrap_err();
        assert_eq!(
            err,
            PlacementError::NoEligibleNodes {
                service: "web".to_string(),
                nodes: 0,
                constraints: vec![],
            }
        );
    }

    #[test]
    fn check_placeable_rejects_on_unmatched_constraints() {
        let nodes = vec![node(1, 4.0, 1 << 30, 0, &[])];
        let mut service = svc("web", 1, ServiceMode::Replicated);
        service.constraints = vec!["node.labels.gpu == true".to_string()];
        let err = check_placeable(&service, &nodes, &[], &[], 0).unwrap_err();
        assert!(matches!(
            err,
            PlacementError::NoEligibleNodes { nodes: 1, .. }
        ));
    }

    #[test]
    fn check_placeable_rejects_on_insufficient_capacity() {
        let nodes = vec![node(1, 1.0, 1 << 30, 0, &[])];
        let mut service = svc("web", 1, ServiceMode::Replicated);
        service.resources = ResourceSpec {
            cpus: 2.0,
            memory_bytes: 0,
            gpus: 0,
        };
        let err = check_placeable(&service, &nodes, &[], &[], 0).unwrap_err();
        assert_eq!(
            err,
            PlacementError::InsufficientCapacity {
                service: "web".to_string(),
                eligible: 1,
            }
        );
    }

    #[test]
    fn check_placeable_allows_zero_replica_scale_down() {
        let service = svc("web", 0, ServiceMode::Replicated);
        assert!(check_placeable(&service, &[], &[], &[], 0).is_ok());
    }

    #[test]
    fn check_placeable_accepts_placeable_service() {
        let nodes = vec![node(1, 4.0, 1 << 30, 0, &[])];
        let service = svc("web", 2, ServiceMode::Replicated);
        assert!(check_placeable(&service, &nodes, &[], &[], 0).is_ok());
    }
}
