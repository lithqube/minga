//! Reconciliation: diffs desired state against actual tasks and emits
//! the actions needed to converge.
//!
//! This is a pure function — no I/O. The [`crate::Scheduler`] calls it
//! with the current cluster state and applies the returned actions to
//! the state store via Raft.
//!
//! The diff is ID-based: [`crate::placement`] produces deterministic task
//! IDs, so a task that already exists with the right ID is left alone,
//! a missing one is created, and a stale one (for a service that was
//! scaled down or removed) is removed.

use std::collections::{HashMap, HashSet};

use minga_state::types::{ClusterNode, NodeId, Service, Task, TaskId};

use crate::placement::place_service;

/// An action the reconcile loop should take to converge the cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum ReconcileAction {
    /// Create (or overwrite) a task in the state store.
    CreateTask(Box<Task>),
    /// Remove a task from the state store. Carries the node_id so the
    /// scheduler can publish a stop command to the right node via the bus.
    RemoveTask { task_id: TaskId, node_id: NodeId },
}

/// Computes the actions needed to make the cluster's tasks match the
/// desired services.
///
/// - `services`: the desired services (from Raft state).
/// - `tasks`: the current tasks (from Raft state).
/// - `nodes`: the current cluster nodes (from Raft state).
pub fn plan(services: &[Service], tasks: &[Task], nodes: &[ClusterNode]) -> Vec<ReconcileAction> {
    let mut actions = Vec::new();

    // Index existing tasks by service ID.
    let mut existing_by_service: HashMap<&str, Vec<&Task>> = HashMap::new();
    for task in tasks {
        existing_by_service
            .entry(task.service_id.as_str())
            .or_default()
            .push(task);
    }

    let service_ids: HashSet<&str> = services.iter().map(|s| s.id.as_str()).collect();

    // --- per-service: create missing, remove stale ----------------------------
    for service in services {
        let desired = place_service(service, nodes, tasks);
        let desired_ids: HashSet<&str> = desired.iter().map(|t| t.id.as_str()).collect();

        let existing = existing_by_service
            .get(service.id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let existing_ids: HashSet<&str> = existing.iter().map(|t| t.id.as_str()).collect();

        // Create tasks that are desired but don't exist yet.
        for task in &desired {
            if !existing_ids.contains(task.id.as_str()) {
                actions.push(ReconcileAction::CreateTask(Box::new(task.clone())));
            }
        }

        // Remove tasks that exist but are no longer desired (e.g. after
        // a scale-down, or when a node became ineligible).
        for task in existing {
            if !desired_ids.contains(task.id.as_str()) {
                actions.push(ReconcileAction::RemoveTask {
                    task_id: task.id.clone(),
                    node_id: task.node_id,
                });
            }
        }
    }

    // --- orphaned tasks: service was deleted ----------------------------------
    for task in tasks {
        if !service_ids.contains(task.service_id.as_str()) {
            actions.push(ReconcileAction::RemoveTask {
                task_id: task.id.clone(),
                node_id: task.node_id,
            });
        }
    }

    actions
}

#[cfg(test)]
mod tests {
    use super::*;
    use minga_state::types::{
        NodeStatus, PortMapping, ResourceSpec, ServiceMode, TaskSpec, TaskState,
    };
    use std::collections::HashMap;

    fn node(id: u64) -> ClusterNode {
        ClusterNode {
            id,
            address: format!("10.0.0.{id}:50051"),
            labels: HashMap::new(),
            status: NodeStatus::Active,
            capacity: ResourceSpec {
                cpus: 8.0,
                memory_bytes: 1 << 34,
                gpus: 0,
            },
            wg_public_key: None,
            wg_endpoint: None,
            overlay_ip: None,
        }
    }

    fn svc(id: &str, replicas: u32) -> Service {
        Service {
            id: id.to_string(),
            name: id.to_string(),
            image: "nginx:latest".to_string(),
            replicas,
            mode: ServiceMode::Replicated,
            constraints: Vec::new(),
            env: HashMap::new(),
            ports: vec![PortMapping {
                target: 80,
                published: None,
                protocol: "tcp".to_string(),
            }],
            networks: vec!["default".to_string()],
            resources: ResourceSpec::default(),
            agent: None,
        }
    }

    fn task(id: &str, service_id: &str, node_id: u64) -> Task {
        Task {
            id: id.to_string(),
            service_id: service_id.to_string(),
            node_id,
            state: TaskState::Running,
            spec: TaskSpec {
                image: "nginx:latest".to_string(),
                env: HashMap::new(),
                ports: vec![],
                resources: ResourceSpec::default(),
                ..Default::default()
            },
            ips: HashMap::new(),
        }
    }

    fn task_ids(actions: &[ReconcileAction]) -> Vec<String> {
        actions
            .iter()
            .map(|a| match a {
                ReconcileAction::CreateTask(t) => format!("+{}", t.id),
                ReconcileAction::RemoveTask { task_id, .. } => format!("-{task_id}"),
            })
            .collect()
    }

    #[test]
    fn new_service_creates_tasks() {
        let nodes = vec![node(1), node(2)];
        let services = vec![svc("web", 2)];
        let actions = plan(&services, &[], &nodes);
        assert_eq!(task_ids(&actions), vec!["+web.1", "+web.2"]);
    }

    #[test]
    fn no_changes_no_actions() {
        let nodes = vec![node(1), node(2)];
        let services = vec![svc("web", 2)];
        // Place first to get the exact task IDs placement would produce.
        let desired = place_service(&services[0], &nodes, &[]);
        let actions = plan(&services, &desired, &nodes);
        assert!(actions.is_empty());
    }

    #[test]
    fn scale_up_creates_only_missing() {
        let nodes = vec![node(1), node(2)];
        // Service scaled from 1 to 2 replicas.
        let services = vec![svc("web", 2)];
        let existing = vec![task("web.1", "web", 1)];
        let actions = plan(&services, &existing, &nodes);
        // web.1 already exists, web.2 is new.
        assert_eq!(task_ids(&actions), vec!["+web.2"]);
    }

    #[test]
    fn scale_down_removes_excess() {
        let nodes = vec![node(1), node(2)];
        // Service scaled from 3 to 1 replica.
        let services = vec![svc("web", 1)];
        let existing = vec![
            task("web.1", "web", 1),
            task("web.2", "web", 2),
            task("web.3", "web", 1),
        ];
        let actions = plan(&services, &existing, &nodes);
        let mut ids = task_ids(&actions);
        ids.sort();
        assert_eq!(ids, vec!["-web.2", "-web.3"]);
    }

    #[test]
    fn deleted_service_removes_all_tasks() {
        let nodes = vec![node(1)];
        let services: Vec<Service> = vec![];
        let existing = vec![task("web.1", "web", 1), task("web.2", "web", 1)];
        let actions = plan(&services, &existing, &nodes);
        let mut ids = task_ids(&actions);
        ids.sort();
        assert_eq!(ids, vec!["-web.1", "-web.2"]);
    }

    #[test]
    fn node_becomes_ineligible_removes_task() {
        let nodes = vec![node(1)];
        let mut service = svc("web", 1);
        service.constraints = vec!["node.labels.gpu == true".to_string()];
        // Node 1 has no gpu label → no eligible nodes → task should be removed.
        let existing = vec![task("web.1", "web", 1)];
        let actions = plan(&[service], &existing, &nodes);
        assert_eq!(task_ids(&actions), vec!["-web.1"]);
    }

    #[test]
    fn multiple_services_independent() {
        let nodes = vec![node(1), node(2)];
        let services = vec![svc("web", 1), svc("api", 2)];
        let existing = vec![task("web.1", "web", 1)];
        let actions = plan(&services, &existing, &nodes);
        let mut ids = task_ids(&actions);
        ids.sort();
        assert_eq!(ids, vec!["+api.1", "+api.2"]);
    }

    #[test]
    fn global_service_reconcile() {
        let nodes = vec![node(1), node(2), node(3)];
        let mut service = svc("agent", 0);
        service.mode = ServiceMode::Global;
        let actions = plan(&[service], &[], &nodes);
        let mut ids = task_ids(&actions);
        ids.sort();
        assert_eq!(ids, vec!["+agent.1", "+agent.2", "+agent.3"]);
    }
}
