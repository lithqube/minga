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

use minga_state::types::{
    ClusterNode, FailureAction, Network, NodeId, Service, ServiceId, Task, TaskId, TaskState,
    UpdateOrder,
};

use crate::placement::{place_service, slot_of};

/// An action the reconcile loop should take to converge the cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum ReconcileAction {
    /// Create (or overwrite) a task in the state store.
    CreateTask(Box<Task>),
    /// Remove a task from the state store. Carries the node_id so the
    /// scheduler can publish a stop command to the right node via the bus.
    RemoveTask { task_id: TaskId, node_id: NodeId },
    /// A rollout failed: put the service's previous spec back.
    RollbackService(ServiceId),
    /// A rollout finished its monitor window: forget the previous spec.
    SettleService(ServiceId),
}

fn is_live(task: &Task) -> bool {
    !matches!(task.state, TaskState::Stopped | TaskState::Failed)
}

fn has_failed(task: &Task) -> bool {
    task.state == TaskState::Failed || task.restarts > 0
}

/// Whether a task has been up for at least `secs`.
fn has_run_for(task: &Task, secs: u64, now_unix: u64) -> bool {
    task.running_since_unix
        .map_or(true, |since| since + secs <= now_unix)
}

fn remove_action(task: &Task) -> ReconcileAction {
    ReconcileAction::RemoveTask {
        task_id: task.id.clone(),
        node_id: task.node_id,
    }
}

/// Computes the actions needed to make the cluster's tasks match the
/// desired services.
///
/// - `services`: the desired services (from Raft state).
/// - `tasks`: the current tasks (from Raft state).
/// - `nodes`: the current cluster nodes (from Raft state).
/// - `networks`: the cluster networks (from Raft state), for task IPs.
/// - `now_unix`: current unix time, for node-lease liveness.
pub fn plan(
    services: &[Service],
    tasks: &[Task],
    nodes: &[ClusterNode],
    networks: &[Network],
    now_unix: u64,
) -> Vec<ReconcileAction> {
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
        let desired = place_service(service, nodes, tasks, networks, now_unix);
        let desired_ids: HashSet<&str> = desired.iter().map(|t| t.id.as_str()).collect();

        let existing = existing_by_service
            .get(service.id.as_str())
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let existing_by_id: HashMap<&str, &Task> =
            existing.iter().map(|t| (t.id.as_str(), *t)).collect();
        let desired_slots: HashSet<&str> = desired.iter().map(|t| slot_of(&t.id)).collect();

        // Stale tasks either hand their slot to a newer revision, or have
        // no successor (a scale-down, or a node became ineligible).
        let mut outgoing: HashMap<&str, Vec<&Task>> = HashMap::new();
        for task in existing {
            if desired_ids.contains(task.id.as_str()) {
                continue;
            }
            if desired_slots.contains(slot_of(&task.id)) {
                outgoing.entry(slot_of(&task.id)).or_default().push(task);
            } else {
                actions.push(remove_action(task));
            }
        }

        let update = service.update;
        let current = |t: &Task| existing_by_id.get(t.id.as_str()).copied();

        // A rollout keeps the previous spec until it settles; a failure of
        // the new revision before then follows `failure_action`.
        let in_rollout = service.previous.is_some();
        if in_rollout
            && update.failure_action == FailureAction::Rollback
            && desired.iter().any(|t| current(t).is_some_and(has_failed))
        {
            actions.push(ReconcileAction::RollbackService(service.id.clone()));
            continue;
        }

        // A published port can't be held by two tasks at once.
        let order = if service.ports.iter().any(|p| p.published.is_some()) {
            UpdateOrder::StopFirst
        } else {
            service.update.order
        };

        // Tasks starting now, or still starting, use up the update budget.
        let busy = desired
            .iter()
            .filter(|t| match current(t) {
                Some(task) if has_failed(task) => update.failure_action != FailureAction::Continue,
                Some(task) => {
                    task.state != TaskState::Running
                        || !has_run_for(task, update.delay_secs, now_unix)
                }
                None => !outgoing
                    .get(slot_of(&t.id))
                    .is_some_and(|old| old.iter().any(|o| is_live(o))),
            })
            .count();
        let mut budget = match service.update.parallelism {
            0 => usize::MAX,
            n => (n as usize).saturating_sub(busy),
        };

        for task in &desired {
            let old = outgoing
                .get(slot_of(&task.id))
                .map_or(&[][..], Vec::as_slice);

            if let Some(new) = current(task) {
                // With a healthcheck, Running means the first probe passed.
                // A new task that fails leaves the old one up.
                if new.state == TaskState::Running && !has_failed(new) {
                    actions.extend(old.iter().map(|o| remove_action(o)));
                }
                continue;
            }

            let (live, dead): (Vec<&Task>, Vec<&Task>) = old.iter().partition(|o| is_live(o));
            actions.extend(dead.iter().map(|o| remove_action(o)));
            if live.is_empty() {
                actions.push(ReconcileAction::CreateTask(Box::new(task.clone())));
            } else if budget > 0 {
                budget -= 1;
                match order {
                    UpdateOrder::StartFirst => {
                        actions.push(ReconcileAction::CreateTask(Box::new(task.clone())));
                    }
                    // The replacement is created on a later pass, once the
                    // old task is gone.
                    UpdateOrder::StopFirst => {
                        actions.extend(live.iter().map(|o| remove_action(o)));
                    }
                }
            }
        }

        let settled = desired.iter().all(|t| {
            current(t).is_some_and(|task| {
                task.state == TaskState::Running
                    && !has_failed(task)
                    && has_run_for(task, update.monitor_secs, now_unix)
            })
        });
        if in_rollout && outgoing.is_empty() && settled {
            actions.push(ReconcileAction::SettleService(service.id.clone()));
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
        NodeStatus, PortMapping, ResourceSpec, ServiceMode, TaskSpec, UpdatePolicy,
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
            last_seen_unix: None,
            role: Default::default(),
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
            restart_policy: Default::default(),
            secrets: vec![],
            command: vec![],
            entrypoint: vec![],
            labels: HashMap::new(),
            healthcheck: None,
            mounts: vec![],
            user: None,
            update: Default::default(),
            previous: None,
        }
    }

    /// The revision placement gives every `svc()` task.
    fn rev() -> String {
        let placed = place_service(&svc("web", 1), &[node(1)], &[], &[], 0);
        placed[0].id.rsplit_once('.').unwrap().1.to_string()
    }

    fn task(id: &str, service_id: &str, node_id: u64) -> Task {
        Task {
            id: format!("{id}.{}", rev()),
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
            restarts: 0,
            running_since_unix: None,
        }
    }

    fn task_ids(actions: &[ReconcileAction]) -> Vec<String> {
        actions
            .iter()
            .map(|a| match a {
                ReconcileAction::CreateTask(t) => format!("+{}", slot_of(&t.id)),
                ReconcileAction::RemoveTask { task_id, .. } => {
                    format!("-{}", slot_of(task_id))
                }
                ReconcileAction::RollbackService(id) => format!("rollback {id}"),
                ReconcileAction::SettleService(id) => format!("settle {id}"),
            })
            .collect()
    }

    fn updated(order: UpdateOrder, parallelism: u32, replicas: u32) -> Service {
        Service {
            image: "nginx:2".to_string(),
            update: UpdatePolicy {
                parallelism,
                order,
                ..Default::default()
            },
            ..svc("web", replicas)
        }
    }

    fn running(service: &Service, nodes: &[ClusterNode]) -> Vec<Task> {
        let mut tasks = place_service(service, nodes, &[], &[], 0);
        tasks.iter_mut().for_each(|t| t.state = TaskState::Running);
        tasks
    }

    /// Applies actions the way the scheduler does.
    fn apply(tasks: &mut Vec<Task>, actions: Vec<ReconcileAction>) {
        for action in actions {
            match action {
                ReconcileAction::CreateTask(t) => tasks.push(*t),
                ReconcileAction::RemoveTask { task_id, .. } => tasks.retain(|t| t.id != task_id),
                _ => {}
            }
        }
    }

    fn mark_new_running(tasks: &mut [Task]) {
        for t in tasks.iter_mut().filter(|t| t.spec.image == "nginx:2") {
            t.state = TaskState::Running;
        }
    }

    #[test]
    fn stop_first_update_removes_then_creates() {
        let nodes = vec![node(1)];
        let mut tasks = running(&svc("web", 1), &nodes);
        let services = vec![updated(UpdateOrder::StopFirst, 1, 1)];

        let actions = plan(&services, &tasks, &nodes, &[], 0);
        assert_eq!(task_ids(&actions), vec!["-web.1"]);
        apply(&mut tasks, actions);

        let actions = plan(&services, &tasks, &nodes, &[], 0);
        assert_eq!(task_ids(&actions), vec!["+web.1"]);
        apply(&mut tasks, actions);
        assert!(plan(&services, &tasks, &nodes, &[], 0).is_empty());
    }

    #[test]
    fn start_first_update_keeps_old_until_new_runs() {
        let nodes = vec![node(1)];
        let mut tasks = running(&svc("web", 1), &nodes);
        let services = vec![updated(UpdateOrder::StartFirst, 1, 1)];

        let actions = plan(&services, &tasks, &nodes, &[], 0);
        assert_eq!(task_ids(&actions), vec!["+web.1"]);
        apply(&mut tasks, actions);
        assert_eq!(tasks.len(), 2);

        // Still starting: nothing moves.
        assert!(plan(&services, &tasks, &nodes, &[], 0).is_empty());

        mark_new_running(&mut tasks);
        let actions = plan(&services, &tasks, &nodes, &[], 0);
        assert_eq!(task_ids(&actions), vec!["-web.1"]);
        apply(&mut tasks, actions);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].spec.image, "nginx:2");
    }

    #[test]
    fn start_first_leaves_old_running_when_new_fails() {
        let nodes = vec![node(1)];
        let mut tasks = running(&svc("web", 1), &nodes);
        let services = vec![updated(UpdateOrder::StartFirst, 1, 1)];

        let actions = plan(&services, &tasks, &nodes, &[], 0);
        apply(&mut tasks, actions);
        for t in tasks.iter_mut().filter(|t| t.spec.image == "nginx:2") {
            t.state = TaskState::Failed;
        }
        assert!(plan(&services, &tasks, &nodes, &[], 0).is_empty());
    }

    /// A service mid-rollout from `svc("web", 1)` to `nginx:2`.
    fn rolling(failure_action: FailureAction, monitor_secs: u64) -> Service {
        let mut service = updated(UpdateOrder::StartFirst, 1, 1);
        service.update.failure_action = failure_action;
        service.update.monitor_secs = monitor_secs;
        service.previous = Some(Box::new(svc("web", 1)));
        service
    }

    #[test]
    fn a_restart_during_the_rollout_triggers_rollback() {
        let nodes = vec![node(1)];
        let service = rolling(FailureAction::Rollback, 30);
        let mut tasks = running(&svc("web", 1), &nodes);
        let services = vec![service];
        let actions = plan(&services, &tasks, &nodes, &[], 0);
        apply(&mut tasks, actions);
        for t in tasks.iter_mut().filter(|t| t.spec.image == "nginx:2") {
            t.restarts = 1;
        }
        let actions = plan(&services, &tasks, &nodes, &[], 0);
        assert_eq!(task_ids(&actions), vec!["rollback web"]);
    }

    #[test]
    fn pause_holds_the_rollout_and_keeps_the_old_task() {
        let nodes = vec![node(1)];
        let services = vec![rolling(FailureAction::Pause, 30)];
        let mut tasks = running(&svc("web", 1), &nodes);
        let actions = plan(&services, &tasks, &nodes, &[], 0);
        apply(&mut tasks, actions);
        for t in tasks.iter_mut().filter(|t| t.spec.image == "nginx:2") {
            t.state = TaskState::Running;
            t.restarts = 1;
        }
        assert!(plan(&services, &tasks, &nodes, &[], 0).is_empty());
    }

    #[test]
    fn rollout_settles_only_after_the_monitor_window() {
        let nodes = vec![node(1)];
        let services = vec![rolling(FailureAction::Rollback, 30)];
        let mut tasks = running(&services[0], &nodes);
        tasks[0].running_since_unix = Some(1000);
        assert!(plan(&services, &tasks, &nodes, &[], 1029).is_empty());
        let actions = plan(&services, &tasks, &nodes, &[], 1030);
        assert_eq!(task_ids(&actions), vec!["settle web"]);
    }

    #[test]
    fn delay_spaces_out_batches() {
        let nodes = vec![node(1)];
        let mut service = updated(UpdateOrder::StartFirst, 1, 2);
        service.update.delay_secs = 10;
        let services = vec![service];
        let mut tasks = running(&svc("web", 2), &nodes);
        let actions = plan(&services, &tasks, &nodes, &[], 1000);
        apply(&mut tasks, actions);
        for t in tasks.iter_mut().filter(|t| t.spec.image == "nginx:2") {
            t.state = TaskState::Running;
            t.running_since_unix = Some(1000);
        }
        let held = plan(&services, &tasks, &nodes, &[], 1009);
        assert_eq!(task_ids(&held), vec!["-web.1"]);
        let next = plan(&services, &tasks, &nodes, &[], 1010);
        assert_eq!(task_ids(&next), vec!["-web.1", "+web.2"]);
    }

    #[test]
    fn parallelism_limits_tasks_replaced_at_once() {
        let nodes = vec![node(1)];
        let mut tasks = running(&svc("web", 2), &nodes);
        let services = vec![updated(UpdateOrder::StartFirst, 1, 2)];

        let actions = plan(&services, &tasks, &nodes, &[], 0);
        assert_eq!(task_ids(&actions), vec!["+web.1"]);
        apply(&mut tasks, actions);

        mark_new_running(&mut tasks);
        let actions = plan(&services, &tasks, &nodes, &[], 0);
        assert_eq!(task_ids(&actions), vec!["-web.1", "+web.2"]);
    }

    #[test]
    fn published_port_forces_stop_first() {
        let nodes = vec![node(1)];
        let published = vec![PortMapping {
            target: 80,
            published: Some(80),
            protocol: "tcp".to_string(),
        }];
        let old = Service {
            ports: published.clone(),
            ..svc("web", 1)
        };
        let tasks = running(&old, &nodes);
        let services = vec![Service {
            ports: published,
            ..updated(UpdateOrder::StartFirst, 1, 1)
        }];

        let actions = plan(&services, &tasks, &nodes, &[], 0);
        assert_eq!(task_ids(&actions), vec!["-web.1"]);
    }

    #[test]
    fn new_service_creates_tasks() {
        let nodes = vec![node(1), node(2)];
        let services = vec![svc("web", 2)];
        let actions = plan(&services, &[], &nodes, &[], 0);
        assert_eq!(task_ids(&actions), vec!["+web.1", "+web.2"]);
    }

    #[test]
    fn no_changes_no_actions() {
        let nodes = vec![node(1), node(2)];
        let services = vec![svc("web", 2)];
        // Place first to get the exact task IDs placement would produce.
        let desired = place_service(&services[0], &nodes, &[], &[], 0);
        let actions = plan(&services, &desired, &nodes, &[], 0);
        assert!(actions.is_empty());
    }

    #[test]
    fn scale_up_creates_only_missing() {
        let nodes = vec![node(1), node(2)];
        // Service scaled from 1 to 2 replicas.
        let services = vec![svc("web", 2)];
        let existing = vec![task("web.1", "web", 1)];
        let actions = plan(&services, &existing, &nodes, &[], 0);
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
        let actions = plan(&services, &existing, &nodes, &[], 0);
        let mut ids = task_ids(&actions);
        ids.sort();
        assert_eq!(ids, vec!["-web.2", "-web.3"]);
    }

    #[test]
    fn deleted_service_removes_all_tasks() {
        let nodes = vec![node(1)];
        let services: Vec<Service> = vec![];
        let existing = vec![task("web.1", "web", 1), task("web.2", "web", 1)];
        let actions = plan(&services, &existing, &nodes, &[], 0);
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
        let actions = plan(&[service], &existing, &nodes, &[], 0);
        assert_eq!(task_ids(&actions), vec!["-web.1"]);
    }

    #[test]
    fn multiple_services_independent() {
        let nodes = vec![node(1), node(2)];
        let services = vec![svc("web", 1), svc("api", 2)];
        let existing = vec![task("web.1", "web", 1)];
        let actions = plan(&services, &existing, &nodes, &[], 0);
        let mut ids = task_ids(&actions);
        ids.sort();
        assert_eq!(ids, vec!["+api.1", "+api.2"]);
    }

    #[test]
    fn global_service_reconcile() {
        let nodes = vec![node(1), node(2), node(3)];
        let mut service = svc("agent", 0);
        service.mode = ServiceMode::Global;
        let actions = plan(&[service], &[], &nodes, &[], 0);
        let mut ids = task_ids(&actions);
        ids.sort();
        assert_eq!(ids, vec!["+agent.1", "+agent.2", "+agent.3"]);
    }
}
