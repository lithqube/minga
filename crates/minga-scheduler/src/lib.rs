//! Scheduler: turns Compose services into placed tasks on cluster nodes.
//!
//! Minga's scheduler has two responsibilities:
//!
//! 1. **Translation** ([`translate`]) — the deploy path. Converts a
//!    Compose [`Project`](minga_compose::Project) into state entities
//!    ([`Service`](minga_state::types::Service),
//!    [`Network`](minga_state::types::Network), …) that are proposed
//!    to Raft.
//!
//! 2. **Reconciliation** ([`reconcile`]) — the manager loop. Runs on
//!    the Raft leader, reads the desired services and current tasks,
//!    computes placement ([`placement`]), and writes the diff back to
//!    the state store.
//!
//! Placement uses a spread strategy with resource-capacity checking and
//! constraint evaluation ([`constraints`]). Task IDs are deterministic,
//! so reconciliation is idempotent.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use minga_scheduler::Scheduler;
//! use minga_state::StateStore;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let state = Arc::new(StateStore::open(1, "10.0.0.1:50051", "/var/lib/minga", Default::default()).await?);
//! let scheduler = Scheduler::new(state);
//! scheduler.run(Duration::from_secs(5)).await;
//! # Ok(())
//! # }
//! ```

pub mod constraints;
mod isolation;
pub mod netassign;
pub mod placement;
pub mod reconcile;
pub mod secrets;
pub mod translate;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use minga_bus::{subjects, EventBus, TaskActualState, TaskStatusReport};
use minga_net::wireguard::{
    WireGuardNetworkAssignment, WireGuardPeerAssignment, WireGuardPeerPath, WireGuardPeerSet,
};
use minga_state::types::{
    ClusterNode, Network, NetworkAssignment, NetworkDnsTask, NodeRole, NodeStatus, Service, Task,
    TaskAssignment, TaskState,
};
use minga_state::{StateError, StateStore};
use thiserror::Error;

use isolation::Isolation;

pub use constraints::{evaluate, matches_str, parse_constraint, Constraint, ConstraintOp};
pub use isolation::{Stage, TenantError};
pub use placement::{check_placeable, place_service, PlacementError};
pub use reconcile::{plan, ReconcileAction};
pub use translate::{
    egress_policy_from_json, parse_memory, translate_project, TranslateError, TranslatedStack,
};

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("state error: {0}")]
    State(#[from] StateError),
    #[error("bus error: {0}")]
    Bus(String),
    #[error("managers in a multi-node cluster require a reachable WireGuard endpoint")]
    ManagerEndpointUnavailable,
    #[error("no active manager with a reachable WireGuard endpoint can relay overlay traffic")]
    RelayManagerUnavailable,
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("node ID {0} has no address in the WireGuard mesh subnet")]
    MeshAddressUnavailable(u64),
}

impl From<minga_bus::BusError> for SchedulerError {
    fn from(error: minga_bus::BusError) -> Self {
        Self::Bus(error.to_string())
    }
}

/// Merges current task status from MINGA_STATUS with Raft's desired task record.
pub fn task_with_actual_status(mut task: Task, report: Option<&TaskStatusReport>) -> Task {
    if let Some(report) = report.filter(|report| report.node_id == task.node_id) {
        task.state = match report.state {
            TaskActualState::Starting => TaskState::Starting,
            TaskActualState::Running => TaskState::Running,
            TaskActualState::Stopped => TaskState::Stopped,
            TaskActualState::Failed => TaskState::Failed,
        };
        task.restarts = report.restarts;
        task.last_error.clone_from(&report.last_error);
        task.last_exit_code = report.last_exit_code;
        task.running_since_unix = report.running_since_unix;
    } else {
        task.state = TaskState::Starting;
        task.running_since_unix = None;
        task.restarts = 0;
        task.last_error = None;
        task.last_exit_code = None;
    }
    task
}

/// The address two overlapping tasks agree to publish a port on: a restricted
/// address always wins over "every address", and of two restricted ones the
/// lower is used, so the result never depends on task order and a restricted
/// port is never published wider.
fn narrower(a: Option<std::net::IpAddr>, b: Option<std::net::IpAddr>) -> Option<std::net::IpAddr> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (one, None) | (None, one) => one,
    }
}

/// The host address of a published port and the backends behind it.
type HostBackends = (
    Option<std::net::IpAddr>,
    Vec<minga_state::types::IngressBackend>,
);

/// The scheduler: runs the reconcile loop on the Raft leader.
///
/// Only the leader reconciles. Followers stay idle — if leadership
/// changes, the new leader picks up the loop.
pub struct Scheduler {
    state: Arc<StateStore>,
    bus: Option<Arc<dyn EventBus>>,
    supernet: minga_net::Cidr,
    /// Subnet collisions already logged, so each is reported once.
    reported: std::sync::Mutex<std::collections::HashSet<(String, String)>>,
    /// Until this instant the lease set may be incomplete after a failed
    /// lease read, so the pass is skipped instead of removing tasks.
    lease_grace_until: std::sync::Mutex<Option<std::time::Instant>>,
}

/// Leases that expired during a bus outage come back within one renew
/// interval; wait two before trusting a missing lease.
const LEASE_GRACE: Duration = Duration::from_secs(2 * minga_state::types::NODE_RENEW_INTERVAL_SECS);

impl Scheduler {
    pub fn new(state: Arc<StateStore>) -> Self {
        Self {
            state,
            bus: None,
            supernet: minga_net::Cidr::parse(minga_net::DEFAULT_SUPERNET)
                .expect("default supernet parses"),
            reported: Default::default(),
            lease_grace_until: Default::default(),
        }
    }

    /// Allocates network subnets from `supernet` instead of 10.100.0.0/16.
    pub fn with_supernet(mut self, supernet: minga_net::Cidr) -> Self {
        self.supernet = supernet;
        self
    }

    /// Create a scheduler that publishes assignments to the event bus.
    pub fn with_bus(state: Arc<StateStore>, bus: Arc<dyn EventBus>) -> Self {
        Self {
            bus: Some(bus),
            ..Self::new(state)
        }
    }

    /// Runs one reconciliation pass. Returns the actions taken.
    ///
    /// If this node is not the leader, returns an empty vec without
    /// doing any work.
    pub async fn reconcile_once(&self) -> Result<Vec<ReconcileAction>, SchedulerError> {
        if !self.state.is_leader().await {
            return Ok(Vec::new());
        }
        let _timer = minga_telemetry::METRICS
            .scheduler_reconcile_duration_seconds
            .start_timer();

        let mut services = self.state.list_services().await?;
        let mut tasks = self.state.list_tasks().await?;
        let mut nodes = self.state.list_nodes().await?;
        if let Some(bus) = &self.bus {
            let live_nodes = match bus.list_alive_nodes().await {
                Ok(nodes) => nodes,
                Err(error) => {
                    tracing::warn!(error = %error, "cannot read node leases; skipping placement");
                    *self.lease_grace_until.lock().unwrap() =
                        Some(std::time::Instant::now() + LEASE_GRACE);
                    return Ok(Vec::new());
                }
            };
            // A missing lease removes that node's tasks. After a bus outage,
            // or with no lease at all, that would stop every task.
            let in_grace = self
                .lease_grace_until
                .lock()
                .unwrap()
                .is_some_and(|until| std::time::Instant::now() < until);
            let none_live =
                !nodes.is_empty() && !nodes.iter().any(|node| live_nodes.contains(&node.id));
            if none_live {
                // Leases may be coming back; a partial set right after this
                // must not count as dead nodes either.
                *self.lease_grace_until.lock().unwrap() =
                    Some(std::time::Instant::now() + LEASE_GRACE);
            }
            // With every registered node live, no lease can be missing in error.
            let all_live = nodes.iter().all(|node| live_nodes.contains(&node.id));
            if all_live {
                // Recovery is complete; a later missing lease is a real failure.
                *self.lease_grace_until.lock().unwrap() = None;
            }
            if none_live || (in_grace && !all_live) {
                tracing::warn!(
                    live = live_nodes.len(),
                    registered = nodes.len(),
                    "node leases may be incomplete; skipping reconciliation"
                );
                return Ok(Vec::new());
            }
            nodes.retain(|node| live_nodes.contains(&node.id));
        }
        // Everything below is per tenant: an error is recorded against the
        // tenant it belongs to, which is then left out of the rest of the
        // pass (see `isolation`). Only the cluster-level steps above, which
        // read Raft state and the node leases, end the pass for everyone.
        let mut isolation = Isolation::default();
        let mut nodes = audit_nodes(nodes, &mut isolation);
        let tenants = self.state.list_tenants().await?;
        let tenant_ids: Vec<_> = tenants.iter().map(|tenant| tenant.id.clone()).collect();
        let mut statuses = HashMap::new();
        if let Some(bus) = &self.bus {
            let (read, joined) =
                read_task_statuses(bus.as_ref(), &tenant_ids, &mut isolation).await;
            statuses = read;
            tasks = overlay_task_statuses(tasks, &statuses);
            // A live lease alone does not admit a non-default node: until a
            // manager accepted its join token it gets no tasks and no peers.
            // A tenant whose status could not be read has no join records to
            // check, so its nodes stay out as well rather than being admitted
            // unchecked.
            nodes.retain(|node| {
                node.tenant_id == minga_state::types::DEFAULT_TENANT_ID
                    || joined.get(&node.id) == Some(&node.tenant_id)
            });
        }
        let mut networks = self.state.list_networks().await?;
        self.fill_subnets(&mut networks, &tasks, &mut isolation)
            .await;

        // A tenant that failed so far is not planned: its status or subnets
        // are unknown, and planning on them would place its tasks again.
        services.retain(|service| !isolation.has_failed(&service.tenant_id));
        tasks.retain(|task| !isolation.has_failed(&task.tenant_id));
        networks.retain(|network| !isolation.has_failed(&network.tenant_id));
        let placement_nodes: Vec<_> = nodes
            .iter()
            .filter(|node| tenants.len() < 2 || node.role != NodeRole::Manager)
            .filter(|node| !isolation.has_failed(&node.tenant_id))
            .cloned()
            .collect();
        let actions = plan(
            &services,
            &tasks,
            &placement_nodes,
            &networks,
            minga_state::types::unix_now(),
        );

        // A network still without a subnet (its write was refused, or the
        // supernet is full) cannot give a task an IP, and a task created
        // without one keeps none while it stays on its node. Wait for the
        // subnet and plan the task again on a later pass.
        let unsubnetted: HashSet<&str> = networks
            .iter()
            .filter(|network| network.subnet.is_none())
            .map(|network| network.id.as_str())
            .collect();
        for action in &actions {
            let Some(tenant_id) = action_tenant(action, &services, &tasks) else {
                continue;
            };
            if isolation.has_failed(tenant_id) {
                continue;
            }
            if let Err(error) = self
                .apply_action(action, &services, &tasks, &unsubnetted)
                .await
            {
                isolation.fail(tenant_id, Stage::Placement, error);
            }
        }

        self.publish_projection(&nodes, &statuses, &mut isolation)
            .await?;
        Ok(actions)
    }

    /// Applies one planned action to the state store.
    async fn apply_action(
        &self,
        action: &ReconcileAction,
        services: &[Service],
        tasks: &[Task],
        unsubnetted: &HashSet<&str>,
    ) -> Result<(), StateError> {
        match action {
            ReconcileAction::CreateTask(task)
                if services.iter().any(|service| {
                    service.id == task.service_id
                        && service.agent.is_none()
                        && service
                            .networks
                            .iter()
                            .any(|network| unsubnetted.contains(network.as_str()))
                }) =>
            {
                tracing::warn!(
                    task = %task.id,
                    service = %task.service_id,
                    "a network of the service has no subnet yet; not creating the task"
                );
            }
            ReconcileAction::CreateTask(task) => {
                tracing::info!(
                    task = %task.id,
                    service = %task.service_id,
                    node = task.node_id,
                    "creating task"
                );
                if let Err(error) = self.state.upsert_task(task.as_ref().clone()).await {
                    return skip_quota_error(error, &task.service_id);
                }
                minga_telemetry::METRICS
                    .scheduler_placement_decisions_total
                    .with_label_values(&["placed"])
                    .inc();
            }
            ReconcileAction::RollbackService(service_id) => {
                let Some(current) = services.iter().find(|s| &s.id == service_id) else {
                    return Ok(());
                };
                let Some(previous) = current.previous.clone() else {
                    return Ok(());
                };
                // An agent with an egress policy would run without its
                // route policy: drop the rollback target and keep the
                // current spec instead.
                if let Err(reason) = previous.check_agent_egress() {
                    tracing::warn!(service = %service_id, reason, "rollout failed; refusing to roll back");
                    let kept = Service {
                        previous: None,
                        ..current.clone()
                    };
                    return match self.state.upsert_service(kept).await {
                        Err(error) => skip_quota_error(error, service_id),
                        Ok(()) => Ok(()),
                    };
                }
                tracing::warn!(service = %service_id, "rollout failed, rolling back");
                let mut rolled_back = *previous;
                rolled_back.rollback = current.rollback;
                if let Some(rollback_policy) = &current.rollback {
                    rolled_back.update = *rollback_policy;
                }
                if let Err(error) = self.state.upsert_service(rolled_back).await {
                    return skip_quota_error(error, service_id);
                }
            }
            ReconcileAction::SettleService(service_id) => {
                let Some(service) = services.iter().find(|s| &s.id == service_id) else {
                    return Ok(());
                };
                tracing::info!(service = %service_id, "rollout settled");
                let settled = Service {
                    previous: None,
                    ..service.clone()
                };
                if let Err(error) = self.state.upsert_service(settled).await {
                    return skip_quota_error(error, service_id);
                }
            }
            ReconcileAction::RemoveTask { task_id, .. } => {
                let Some(task) = tasks.iter().find(|task| &task.id == task_id) else {
                    return Ok(());
                };
                tracing::info!(task = %task_id, "removing task");
                self.state
                    .remove_task(task_id.clone(), &task.tenant_id)
                    .await?;
            }
        }
        Ok(())
    }

    /// Publishes every tenant's assignments, then sweeps stale keys.
    ///
    /// The pass first stages what each tenant should have (pure, no bus
    /// calls), then writes tenant by tenant. A tenant that failed anywhere in
    /// the pass is not written and its keys are not swept: what is on the bus
    /// for it stays as the last good pass left it.
    async fn publish_projection(
        &self,
        nodes: &[ClusterNode],
        statuses: &HashMap<(String, String), TaskStatusReport>,
        isolation: &mut Isolation,
    ) -> Result<(), SchedulerError> {
        let Some(bus) = &self.bus else {
            return Ok(());
        };
        let services = self.state.list_services().await?;
        // Stored task state lags the nodes' reports; without the overlay a
        // replica reported Failed would stay an ingress backend.
        let tasks = overlay_task_statuses(self.state.list_tasks().await?, statuses);
        let networks = self.state.list_networks().await?;
        let live_node_ids: HashSet<_> = nodes.iter().map(|node| node.id).collect();
        let service_by_id: HashMap<_, _> = services
            .iter()
            .map(|service| (service.id.as_str(), service))
            .collect();
        let network_by_id: HashMap<_, _> = networks
            .iter()
            .map(|network| (network.id.as_str(), network))
            .collect();
        let node_by_id: HashMap<_, _> = nodes.iter().map(|node| (node.id, node)).collect();
        let mut network_tasks: HashMap<&str, Vec<NetworkDnsTask>> = HashMap::new();
        // One published port/protocol belongs to one service (an ingress port
        // is unique in the cluster, and the store refuses a repeat inside one
        // service), but tasks of two revisions overlap during a rollout and
        // may differ in `host_ip`. [`narrower`] merges them without depending
        // on task order.
        let mut ingress_backends: HashMap<(String, u16, String), HostBackends> = HashMap::new();
        let mut staged = Staged::default();
        for task in &tasks {
            let Some(service) = service_by_id.get(task.service_id.as_str()) else {
                continue;
            };
            let Some(node) = node_by_id.get(&task.node_id) else {
                continue;
            };
            if service.tenant_id != task.tenant_id || node.tenant_id != task.tenant_id {
                continue;
            }
            let task_networks: Vec<Network> = task
                .spec
                .ips
                .keys()
                .filter_map(|id| {
                    network_by_id
                        .get(id.as_str())
                        .filter(|network| network.tenant_id == task.tenant_id)
                        .map(|network| (*network).clone())
                })
                .collect();
            let stack = service.env.get("MINGA_STACK").cloned().unwrap_or_default();
            let assignment = TaskAssignment {
                task_id: task.id.clone(),
                node_id: task.node_id,
                service_id: task.service_id.clone(),
                service_name: dns_service_name(&service.name, &stack).to_owned(),
                stack,
                spec: task.spec.clone(),
                networks: task_networks,
            };
            staged.put(
                isolation,
                &task.tenant_id,
                subjects::kv_assignment_task(&task.tenant_id, task.node_id, &task.id),
                &assignment,
            );
            for (network_id, ip) in &task.spec.ips {
                if network_by_id
                    .get(network_id.as_str())
                    .is_some_and(|network| network.tenant_id == task.tenant_id)
                {
                    network_tasks
                        .entry(network_id)
                        .or_default()
                        .push(NetworkDnsTask {
                            node_id: task.node_id,
                            task_id: task.id.clone(),
                            service_name: assignment.service_name.clone(),
                            stack: assignment.stack.clone(),
                            ip: ip.clone(),
                        });
                }
            }
            if live_node_ids.contains(&task.node_id)
                && !matches!(
                    task.state,
                    minga_state::types::TaskState::Stopped | minga_state::types::TaskState::Failed
                )
            {
                if let Some((_, ip)) = primary_task_ip(&task.spec.ips).filter(|(network_id, ip)| {
                    network_by_id.get(*network_id).is_some_and(|network| {
                        network.tenant_id == task.tenant_id
                            && routable_subnet(network, &self.supernet).is_some_and(|subnet| {
                                ip.parse().is_ok_and(|ip| subnet.contains(ip))
                            })
                    })
                }) {
                    for port in &task.spec.ports {
                        let (Some(published), minga_state::types::PortMode::Ingress) =
                            (port.published, port.mode)
                        else {
                            continue;
                        };
                        let entry = ingress_backends
                            .entry((task.tenant_id.clone(), published, port.protocol.clone()))
                            .or_default();
                        entry.0 = narrower(entry.0, port.host_ip);
                        entry.1.push(minga_state::types::IngressBackend {
                            node_id: task.node_id,
                            ip: ip.to_owned(),
                            target_port: port.target,
                        });
                    }
                }
            }
        }
        for (_, backends) in ingress_backends.values_mut() {
            backends.sort_by(|left, right| {
                (left.node_id, &left.ip, left.target_port).cmp(&(
                    right.node_id,
                    &right.ip,
                    right.target_port,
                ))
            });
        }
        for node in nodes {
            // Node records without a mesh address were left out up front.
            let Ok(source_overlay_ip) = minga_net::overlay::mesh_node_ip(node.id) else {
                continue;
            };
            for ((tenant_id, published, protocol), (host_ip, backends)) in &ingress_backends {
                if node.tenant_id != *tenant_id {
                    continue;
                }
                let assignment = minga_state::types::IngressAssignment {
                    host_ip: *host_ip,
                    published: *published,
                    protocol: protocol.clone(),
                    backends: backends.clone(),
                    source_overlay_ip: Some(source_overlay_ip.to_string()),
                };
                staged.put(
                    isolation,
                    &node.tenant_id,
                    subjects::kv_assignment_ingress(&node.tenant_id, node.id, *published, protocol),
                    &assignment,
                );
            }
        }
        // Each network goes only to the nodes that run one of its tasks, so a
        // worker never reads other networks' names or task addresses.
        for network in &networks {
            let dns_tasks = network_tasks
                .remove(network.id.as_str())
                .unwrap_or_default();
            let task_nodes: std::collections::BTreeSet<_> =
                dns_tasks.iter().map(|task| task.node_id).collect();
            let Some(value) = staged.encode(
                isolation,
                &network.tenant_id,
                &NetworkAssignment {
                    tasks: dns_tasks,
                    network: network.clone(),
                },
            ) else {
                continue;
            };
            for node_id in task_nodes {
                staged.push(
                    &network.tenant_id,
                    subjects::kv_assignment_network(&network.tenant_id, node_id, &network.id),
                    value.clone(),
                );
            }
        }
        let PeerSets {
            by_node: peer_sets,
            unrelayable,
        } = build_wireguard_peer_sets(nodes, &tasks, &networks, &self.supernet);
        for tenant_id in unrelayable {
            isolation.fail(
                &tenant_id,
                Stage::Projection,
                SchedulerError::RelayManagerUnavailable,
            );
        }
        for node in nodes {
            let Some(peer_set) = peer_sets.get(&node.id) else {
                continue;
            };
            staged.put(
                isolation,
                &node.tenant_id,
                format!("{}.node.{}.wg", node.tenant_id, node.id),
                peer_set,
            );
        }

        for (tenant_id, writes) in &staged.by_tenant {
            for (key, value) in writes {
                if isolation.has_failed(tenant_id) {
                    break;
                }
                if let Err(error) = bus.put_assignment(key, value).await {
                    isolation.fail(tenant_id, Stage::Projection, error);
                }
            }
        }

        // Keys name their tenant first, and a tenant ID has no dot. A tenant
        // that failed keeps every key it has. A listing that fails as a whole
        // belongs to no tenant: the sweep waits for the next pass.
        let desired: HashSet<&str> = staged
            .by_tenant
            .values()
            .flatten()
            .map(|(key, _)| key.as_str())
            .collect();
        match bus.list_assignments("").await {
            Ok(existing) => {
                for (key, _) in existing {
                    let tenant_id = key.split('.').next().unwrap_or_default();
                    if desired.contains(key.as_str()) || isolation.has_failed(tenant_id) {
                        continue;
                    }
                    if let Err(error) = bus.delete_assignment(&key).await {
                        isolation.fail(tenant_id, Stage::Sweep, error);
                    }
                }
            }
            Err(error) => {
                tracing::warn!(%error, "cannot list assignments; skipping the sweep of stale keys");
            }
        }
        Ok(())
    }

    /// Stores a subnet for every network that has none (Raft serializes
    /// these through the leader, so two networks never race for one /24)
    /// and logs each subnet collision once.
    async fn fill_subnets(
        &self,
        networks: &mut [minga_state::types::Network],
        tasks: &[minga_state::types::Task],
        isolation: &mut Isolation,
    ) {
        let (changed, errors) = netassign::fill_subnets(networks, tasks, &self.supernet);
        for e in errors {
            tracing::error!(error = %e, "cannot assign a network subnet");
        }
        for i in changed {
            let net = networks[i].clone();
            tracing::info!(network = %net.id, subnet = ?net.subnet, "network subnet assigned");
            match self.state.upsert_network(net.clone()).await {
                Ok(()) => {}
                // A network created since this pass read state took the subnet.
                // It has none until the next pass picks another, and plans no
                // task IP from the refused one: an IP outside the stored subnet
                // would persist and never route. One tenant's race must not stop
                // reconciliation for the rest.
                Err(StateError::SubnetOverlap) => {
                    tracing::warn!(network = %net.id, "assigned subnet was taken meanwhile; retrying next pass");
                    networks[i].subnet = None;
                }
                // Any other refusal is the network's tenant's alone. Its
                // tasks wait, with no subnet planned from the unstored one.
                Err(error) => {
                    networks[i].subnet = None;
                    isolation.fail(&net.tenant_id, Stage::Subnet, error);
                }
            }
        }
        let mut reported = self.reported.lock().unwrap_or_else(|e| e.into_inner());
        for (a, b, sa, sb) in netassign::subnet_collisions(networks) {
            if reported.insert((a.clone(), b.clone())) {
                tracing::error!(
                    "networks {a} ({sa}) and {b} ({sb}) overlap, so their tasks break each \
                     other's bridges. Fix: remove the services on one of them and the network \
                     itself, then redeploy; it gets a free /24 (or set a non-overlapping \
                     `ipam` subnet in its Compose file)"
                );
            }
        }
        // A stored subnet that breaks the overlay rules never routes between
        // nodes (build_wireguard_peer_sets drops it). Keyed (id, "") so it
        // shares the report-once set with collision pairs.
        for net in networks.iter() {
            let Some(raw) = net.subnet.as_deref() else {
                continue;
            };
            let refused = minga_net::Cidr::parse(raw)
                .map_err(|error| error.to_string())
                .and_then(|subnet| {
                    minga_net::overlay::check_network_subnet(&subnet, Some(&self.supernet))
                });
            if let Err(why) = refused {
                if reported.insert((net.id.clone(), String::new())) {
                    tracing::error!(
                        "network {}: {why}, so its tasks do not reach each other across nodes. \
                         Fix: recreate it with a subnet inside {} (or no `ipam` subnet), or \
                         widen --overlay-supernet on every node",
                        net.id,
                        self.supernet
                    );
                }
            }
        }
    }

    /// Runs the reconcile loop forever, sleeping `interval` between passes.
    ///
    /// This function never returns. Errors in individual passes are logged
    /// and the loop continues.
    pub async fn run(self, interval: Duration) -> ! {
        tracing::info!(?interval, "scheduler loop starting");
        loop {
            match self.reconcile_once().await {
                Ok(actions) => {
                    if !actions.is_empty() {
                        tracing::debug!(count = actions.len(), "reconcile pass applied actions");
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "reconcile pass failed");
                }
            }
            tokio::time::sleep(interval).await;
        }
    }
}

/// The name a service's tasks answer to in DNS: the bare Compose service name.
///
/// A service stored through the API is named `<stack>.<service>` in every
/// tenant (the tenant qualifies only the ID), so the stack prefix comes off
/// here, once. Nodes register what they are sent and must not strip again: a
/// service called `shop.db` in stack `shop` would lose its own prefix.
fn dns_service_name<'a>(name: &'a str, stack: &str) -> &'a str {
    name.strip_prefix(stack)
        .and_then(|rest| rest.strip_prefix('.'))
        .filter(|bare| !stack.is_empty() && !bare.is_empty())
        .unwrap_or(name)
}

/// One tenant at its quota must not stop reconciliation for every tenant:
/// the write is skipped and retried on the next pass.
fn skip_quota_error(error: StateError, service_id: &str) -> Result<(), StateError> {
    match error {
        StateError::QuotaExceeded(quota) => {
            tracing::warn!(service = %service_id, %quota, "tenant quota exceeded; skipping write");
            Ok(())
        }
        other => Err(other),
    }
}

/// Leaves out the node records the projection cannot represent, and freezes
/// their tenants: a manager of a multi-node cluster without a usable
/// WireGuard endpoint (workers could not reach it), and an ID with no address
/// in the mesh subnet. Peers list every node, so a bad record is dropped for
/// everyone, but only its own tenant fails.
fn audit_nodes(nodes: Vec<ClusterNode>, isolation: &mut Isolation) -> Vec<ClusterNode> {
    let multi_node = nodes.len() > 1;
    nodes
        .into_iter()
        .filter(|node| {
            let fault = if multi_node
                && node.role == NodeRole::Manager
                && node
                    .wg_endpoint
                    .as_deref()
                    .is_none_or(|endpoint| !minga_net::wireguard::validate_endpoint(endpoint))
            {
                Some(SchedulerError::ManagerEndpointUnavailable)
            } else if minga_net::overlay::mesh_node_ip(node.id).is_err() {
                Some(SchedulerError::MeshAddressUnavailable(node.id))
            } else {
                None
            };
            let Some(fault) = fault else {
                return true;
            };
            isolation.fail(&node.tenant_id, Stage::Nodes, fault);
            false
        })
        .collect()
}

/// The tenant an action works for, or `None` when its service or task is gone.
fn action_tenant<'a>(
    action: &ReconcileAction,
    services: &'a [Service],
    tasks: &'a [Task],
) -> Option<&'a str> {
    match action {
        ReconcileAction::CreateTask(task) => services
            .iter()
            .find(|service| service.id == task.service_id)
            .map(|service| service.tenant_id.as_str()),
        ReconcileAction::RollbackService(service_id)
        | ReconcileAction::SettleService(service_id) => services
            .iter()
            .find(|service| &service.id == service_id)
            .map(|service| service.tenant_id.as_str()),
        ReconcileAction::RemoveTask { task_id, .. } => tasks
            .iter()
            .find(|task| &task.id == task_id)
            .map(|task| task.tenant_id.as_str()),
    }
}

/// What a pass will write to the assignment bucket, by owning tenant.
#[derive(Default)]
struct Staged {
    by_tenant: std::collections::BTreeMap<String, Vec<(String, Vec<u8>)>>,
}

impl Staged {
    /// Encodes `value`; a value that cannot be encoded fails its tenant.
    fn encode<T: serde::Serialize + ?Sized>(
        &self,
        isolation: &mut Isolation,
        tenant_id: &str,
        value: &T,
    ) -> Option<Vec<u8>> {
        match minga_state::codec::encode_wire(value) {
            Ok(bytes) => Some(bytes),
            Err(error) => {
                isolation.fail(
                    tenant_id,
                    Stage::Projection,
                    SchedulerError::Serialization(error.to_string()),
                );
                None
            }
        }
    }

    fn push(&mut self, tenant_id: &str, key: String, value: Vec<u8>) {
        self.by_tenant
            .entry(tenant_id.to_owned())
            .or_default()
            .push((key, value));
    }

    fn put<T: serde::Serialize + ?Sized>(
        &mut self,
        isolation: &mut Isolation,
        tenant_id: &str,
        key: String,
        value: &T,
    ) {
        if let Some(bytes) = self.encode(isolation, tenant_id, value) {
            self.push(tenant_id, key, bytes);
        }
    }
}

/// Reads the nodes' task reports from MINGA_STATUS, keyed by tenant and task
/// ID, plus each node's joined tenant from the same listing. A report whose
/// key does not match its own node and task is ignored. Each tenant's bucket
/// is read on its own: one that cannot be read fails that tenant and adds
/// nothing, so its tasks must not be taken for tasks without a report.
async fn read_task_statuses(
    bus: &dyn EventBus,
    tenant_ids: &[String],
    isolation: &mut Isolation,
) -> (
    HashMap<(String, String), TaskStatusReport>,
    HashMap<u64, String>,
) {
    let mut statuses = HashMap::new();
    let mut joined = HashMap::new();
    for tenant_id in tenant_ids {
        let entries = match bus.list_tenant_status(tenant_id).await {
            Ok(entries) => entries,
            Err(error) => {
                isolation.fail(tenant_id, Stage::Status, error);
                continue;
            }
        };
        for (key, bytes) in entries {
            if let Some(node_id) = key
                .strip_prefix(subjects::NODE_JOIN_PREFIX)
                .and_then(|id| id.parse::<u64>().ok())
            {
                if std::str::from_utf8(&bytes).ok() == Some(tenant_id.as_str()) {
                    joined.insert(node_id, tenant_id.clone());
                }
                continue;
            }
            // Manager bookkeeping such as node registration shares the bucket;
            // any other key is decoded so a malformed report is still logged.
            if key.starts_with("internal.") {
                continue;
            }

            match TaskStatusReport::from_bytes(&bytes) {
                Ok(report)
                    if key == subjects::kv_node_task_status(report.node_id, &report.task_id) =>
                {
                    statuses.insert((tenant_id.clone(), report.task_id.clone()), report);
                }
                Ok(_) => {}
                Err(error) => tracing::warn!(key, error = %error, "cannot decode task status"),
            }
        }
    }
    (statuses, joined)
}

fn overlay_task_statuses(
    tasks: Vec<Task>,
    statuses: &HashMap<(String, String), TaskStatusReport>,
) -> Vec<Task> {
    tasks
        .into_iter()
        .map(|task| {
            let status = statuses.get(&(task.tenant_id.clone(), task.id.clone()));
            task_with_actual_status(task, status)
        })
        .collect()
}

/// The subnet a network's task addresses may route in: its stored subnet, or
/// `None` when that breaks the overlay rules (one stored before they existed).
/// Peer sets and ingress backends both use it.
fn routable_subnet(network: &Network, supernet: &minga_net::Cidr) -> Option<minga_net::Cidr> {
    let subnet = minga_net::Cidr::parse(network.subnet.as_deref()?).ok()?;
    minga_net::overlay::check_network_subnet(&subnet, Some(supernet)).ok()?;
    Some(subnet)
}

/// Matches `prepare_network`'s sorted attachment order for published-port backends.
fn primary_task_ip(ips: &HashMap<String, String>) -> Option<(&str, &str)> {
    ips.iter()
        .min_by(|(left_network, _), (right_network, _)| left_network.cmp(right_network))
        .map(|(network_id, ip)| (network_id.as_str(), ip.as_str()))
}

/// The WireGuard peer set of each node, and the tenants it cannot serve.
struct PeerSets {
    by_node: HashMap<u64, WireGuardPeerSet>,
    /// Tenants with a network that needs the relay when no manager can relay.
    unrelayable: std::collections::BTreeSet<String>,
}

fn build_wireguard_peer_sets(
    nodes: &[ClusterNode],
    tasks: &[Task],
    networks: &[Network],
    supernet: &minga_net::Cidr,
) -> PeerSets {
    // Peers are addressed by their mesh IP; a node without one cannot be a
    // peer. The pass drops such records up front, so this only guards the
    // indexing below.
    let addressable: Vec<ClusterNode> = nodes
        .iter()
        .filter(|node| minga_net::overlay::mesh_node_ip(node.id).is_ok())
        .cloned()
        .collect();
    let nodes = addressable.as_slice();
    let node_by_id: HashMap<_, _> = nodes.iter().map(|node| (node.id, node)).collect();
    let network_tenants: HashMap<_, _> = networks
        .iter()
        .map(|network| (network.id.as_str(), network.tenant_id.as_str()))
        .collect();
    let network_subnets: HashMap<_, _> = networks
        .iter()
        .filter_map(|network| Some((network.id.as_str(), routable_subnet(network, supernet)?)))
        .collect();
    // A task address routes only inside its own network's subnet, from a node
    // of that network's tenant.
    let owned = |task: &Task, network_id: &str, ip: &str| {
        node_by_id
            .get(&task.node_id)
            .is_some_and(|node| node.tenant_id == task.tenant_id)
            && network_tenants.get(network_id) == Some(&task.tenant_id.as_str())
            && ip.parse().is_ok_and(|ip| {
                network_subnets
                    .get(network_id)
                    .is_some_and(|subnet| subnet.contains(ip))
            })
    };
    let mut claims: HashMap<&str, (HashSet<u64>, HashSet<u64>)> = HashMap::new();
    for task in tasks {
        for (network_id, ip) in &task.spec.ips {
            if owned(task, network_id, ip) {
                let (all, live) = claims.entry(ip.as_str()).or_default();
                all.insert(task.node_id);
                if !task.state.is_terminal() {
                    live.insert(task.node_id);
                }
            }
        }
    }
    // WireGuard hands a /32 listed under two peers to the last one, so an
    // address two nodes claim goes to its one live claimant, or nowhere.
    let routed = |task: &Task, network_id: &str, ip: &str| {
        owned(task, network_id, ip)
            && claims.get(ip).is_some_and(|(all, live)| {
                all.len() == 1 || (live.len() == 1 && live.contains(&task.node_id))
            })
    };
    let mesh_node_ips: HashMap<_, _> = nodes
        .iter()
        .filter_map(|node| {
            minga_net::overlay::mesh_node_ip(node.id)
                .ok()
                .map(|address| (node.id, address.to_string()))
        })
        .collect();
    let mut ingress_network_ids = HashSet::new();
    let mut ingress_task_ips: HashMap<u64, HashMap<String, Vec<String>>> = HashMap::new();
    if nodes.len() > 1 {
        for task in tasks {
            if task.state.is_terminal()
                || !task.spec.ports.iter().any(|port| {
                    port.published.is_some() && port.mode == minga_state::types::PortMode::Ingress
                })
            {
                continue;
            }
            let Some((network_id, ip)) = primary_task_ip(&task.spec.ips) else {
                continue;
            };
            if !routed(task, network_id, ip) {
                continue;
            }
            ingress_network_ids.insert(network_id);
            ingress_task_ips
                .entry(task.node_id)
                .or_default()
                .entry(network_id.to_owned())
                .or_default()
                .push(format!("{ip}/32"));
        }
    }
    let mut task_ips: HashMap<u64, HashMap<String, Vec<String>>> = HashMap::new();
    for task in tasks {
        for (network_id, ip) in &task.spec.ips {
            if routed(task, network_id, ip) {
                task_ips
                    .entry(task.node_id)
                    .or_default()
                    .entry(network_id.clone())
                    .or_default()
                    .push(format!("{ip}/32"));
            }
        }
    }
    for node_networks in task_ips.values_mut() {
        for ips in node_networks.values_mut() {
            ips.sort();
            ips.dedup();
        }
    }
    let mut nat_workers_by_network: HashMap<&str, HashSet<u64>> = HashMap::new();
    for task in tasks {
        let Some(node) = node_by_id.get(&task.node_id) else {
            continue;
        };
        if node.tenant_id != task.tenant_id
            || node.role != NodeRole::Worker
            || node.wg_endpoint.is_some()
        {
            continue;
        }
        for network_id in task.spec.ips.keys() {
            if task_ips
                .get(&task.node_id)
                .is_some_and(|node_networks| node_networks.contains_key(network_id.as_str()))
            {
                nat_workers_by_network
                    .entry(network_id)
                    .or_default()
                    .insert(task.node_id);
            }
        }
    }
    for node in nodes
        .iter()
        .filter(|node| node.role == NodeRole::Worker && node.wg_endpoint.is_none())
    {
        for network_id in &ingress_network_ids {
            if network_tenants.get(*network_id) != Some(&node.tenant_id.as_str()) {
                continue;
            }
            nat_workers_by_network
                .entry(network_id)
                .or_default()
                .insert(node.id);
        }
    }
    let mut relay_network_ids: HashSet<String> = nat_workers_by_network
        .into_iter()
        .filter_map(|(network_id, workers)| (workers.len() > 1).then(|| network_id.to_owned()))
        .collect();

    let mut unrelayable = std::collections::BTreeSet::new();
    let home_manager = if relay_network_ids.is_empty() {
        None
    } else {
        let manager = nodes
            .iter()
            .filter(|node| {
                node.role == NodeRole::Manager
                    && node.status == NodeStatus::Active
                    && node.wg_public_key.is_some()
                    && node
                        .wg_endpoint
                        .as_deref()
                        .is_some_and(minga_net::wireguard::validate_endpoint)
            })
            .map(|node| node.id)
            .min();
        if manager.is_none() {
            // No manager can relay, so the tenants that need it fail; the
            // others have no relay network and are served as usual.
            unrelayable.extend(
                relay_network_ids
                    .drain()
                    .filter_map(|id| network_tenants.get(id.as_str()).map(|t| (*t).to_owned())),
            );
        }
        manager
    };

    let mut peer_sets = HashMap::with_capacity(nodes.len());
    for node in nodes {
        let local_network_ids: HashSet<&str> = task_ips
            .get(&node.id)
            .into_iter()
            .flat_map(|node_networks| node_networks.keys().map(String::as_str))
            .collect();
        let mut routing_network_ids = local_network_ids.clone();
        routing_network_ids.extend(
            ingress_network_ids
                .iter()
                .filter(|network_id| {
                    network_tenants.get(**network_id) == Some(&node.tenant_id.as_str())
                })
                .copied(),
        );
        let mut node_networks: Vec<_> = networks
            .iter()
            .filter_map(|network| {
                let relay =
                    home_manager == Some(node.id) && relay_network_ids.contains(&network.id);
                if network.tenant_id != node.tenant_id && !relay {
                    return None;
                }
                network
                    .subnet
                    .as_ref()
                    .map(|subnet| WireGuardNetworkAssignment {
                        network_id: network.id.clone(),
                        subnet: subnet.clone(),
                        ingress: ingress_network_ids.contains(network.id.as_str())
                            && (network.tenant_id == node.tenant_id || relay),
                        local: local_network_ids.contains(network.id.as_str()),
                        relay,
                    })
            })
            .collect();
        node_networks.sort_by(|left, right| left.network_id.cmp(&right.network_id));

        let mut peers = Vec::new();
        for peer in nodes {
            if peer.id == node.id {
                continue;
            }
            if node.role == NodeRole::Worker
                && peer.role == NodeRole::Worker
                && node.tenant_id != peer.tenant_id
            {
                continue;
            }
            let peer_has_local_network = task_ips.get(&peer.id).is_some_and(|peer_networks| {
                local_network_ids
                    .iter()
                    .any(|network_id| peer_networks.contains_key(*network_id))
                // Every node fronts ingress, so a backend's node and every
                // other node must list each other: either side qualifies.
            }) || ingress_task_ips.contains_key(&node.id)
                || ingress_task_ips.contains_key(&peer.id);
            let peer_is_manager = peer.role == NodeRole::Manager;
            if node.role != NodeRole::Manager && !peer_is_manager && !peer_has_local_network {
                continue;
            }
            let Some(public_key) = &peer.wg_public_key else {
                continue;
            };
            let direct = node.wg_endpoint.is_some() || peer.wg_endpoint.is_some();
            if node.role != NodeRole::Manager && peer.role == NodeRole::Worker && !direct {
                continue;
            }

            let mut allowed_ips = HashSet::new();
            allowed_ips.insert(format!("{}/32", mesh_node_ips[&peer.id]));
            for network_id in &routing_network_ids {
                let peer_ips = if local_network_ids.contains(network_id) {
                    task_ips
                        .get(&peer.id)
                        .and_then(|peer_networks| peer_networks.get(*network_id))
                } else {
                    ingress_task_ips
                        .get(&peer.id)
                        .and_then(|peer_networks| peer_networks.get(*network_id))
                };
                if let Some(ips) = peer_ips {
                    allowed_ips.extend(ips.iter().cloned());
                }
            }

            let mut relayed = false;
            if node.role == NodeRole::Manager
                && home_manager == Some(node.id)
                && peer.role == NodeRole::Worker
                && peer.wg_endpoint.is_none()
            {
                if let Some(peer_networks) = task_ips.get(&peer.id) {
                    for (network_id, ips) in peer_networks {
                        if relay_network_ids.contains(network_id) {
                            allowed_ips.extend(ips.iter().cloned());
                            relayed = true;
                        }
                    }
                }
            } else if node.role == NodeRole::Worker
                && peer.role == NodeRole::Manager
                && home_manager == Some(peer.id)
                && node.wg_endpoint.is_none()
            {
                for (target_node_id, target_networks) in &task_ips {
                    let Some(target) = node_by_id.get(target_node_id) else {
                        continue;
                    };
                    if *target_node_id == node.id
                        || target.role != NodeRole::Worker
                        || target.tenant_id != node.tenant_id
                        || target.wg_endpoint.is_some()
                    {
                        continue;
                    }
                    for (network_id, ips) in target_networks {
                        if !relay_network_ids.contains(network_id)
                            || !routing_network_ids.contains(network_id.as_str())
                        {
                            continue;
                        }
                        let routed_ips = if local_network_ids.contains(network_id.as_str()) {
                            Some(ips.as_slice())
                        } else {
                            ingress_task_ips
                                .get(target_node_id)
                                .and_then(|networks| networks.get(network_id))
                                .map(Vec::as_slice)
                        };
                        if let Some(ips) = routed_ips {
                            allowed_ips.extend(ips.iter().cloned());
                            allowed_ips.insert(format!("{}/32", mesh_node_ips[target_node_id]));
                            relayed = true;
                        }
                    }
                }
                if ingress_network_ids
                    .iter()
                    .any(|network_id| routing_network_ids.contains(network_id))
                {
                    for target in nodes.iter().filter(|target| {
                        target.id != node.id
                            && target.role == NodeRole::Worker
                            && target.tenant_id == node.tenant_id
                            && target.wg_endpoint.is_none()
                    }) {
                        allowed_ips.insert(format!("{}/32", mesh_node_ips[&target.id]));
                        relayed = true;
                    }
                }
            }

            let mut allowed_ips: Vec<_> = allowed_ips.into_iter().collect();
            allowed_ips.sort();
            let mut peer = WireGuardPeerAssignment {
                node_id: peer.id,
                public_key: public_key.clone(),
                allowed_ips,
                endpoint: peer.wg_endpoint.clone(),
                keepalive: if node.wg_endpoint.is_none() { 25 } else { 0 },
                path: WireGuardPeerPath::Direct,
            };
            if relayed {
                peer.path = WireGuardPeerPath::Relay;
            }
            peers.push(peer);
        }
        peers.sort_by_key(|peer| peer.node_id);
        peer_sets.insert(
            node.id,
            WireGuardPeerSet {
                peers,
                networks: node_networks,
            },
        );
    }
    PeerSets {
        by_node: peer_sets,
        unrelayable,
    }
}

#[cfg(test)]
mod narrower_tests {
    use super::narrower;
    use std::net::IpAddr;

    #[test]
    fn a_restricted_address_never_becomes_every_address_whatever_the_task_order() {
        let ip = |s: &str| Some(s.parse::<IpAddr>().unwrap());
        let (loopback, lan) = (ip("127.0.0.1"), ip("10.0.0.5"));
        for (a, b) in [(None, loopback), (loopback, None), (lan, None), (None, lan)] {
            assert_eq!(narrower(a, b), a.or(b), "{a:?} then {b:?}");
        }
        // Two restricted addresses agree on the lower one in either order.
        assert_eq!(narrower(loopback, lan), lan);
        assert_eq!(narrower(lan, loopback), lan);
        assert_eq!(narrower(None, None), None);
    }
}

#[cfg(test)]
mod subnet_race_tests {
    use super::*;
    use minga_state::types::Network;
    use std::collections::BTreeMap;

    fn network(id: &str, subnet: Option<&str>) -> Network {
        Network {
            id: id.to_string(),
            tenant_id: "default".to_string(),
            name: id.to_string(),
            driver: "overlay".to_string(),
            attachable: false,
            subnet: subnet.map(String::from),
            wg_port: None,
        }
    }

    /// A network created after the pass read state holds the /24 the pass picks.
    /// The pass reads one snapshot, so a stale one, which leaves that network
    /// out, stands in for the race: the Raft apply refuses the write.
    #[tokio::test]
    async fn a_refused_subnet_write_leaves_the_network_without_one_until_a_later_pass() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(
            StateStore::open(1, "127.0.0.1:0", dir.path(), BTreeMap::new())
                .await
                .unwrap(),
        );
        store
            .upsert_network(network("held", Some("10.100.0.0/24")))
            .await
            .unwrap();
        store.upsert_network(network("late", None)).await.unwrap();
        let scheduler = Scheduler::new(store.clone());

        let mut stale = vec![network("late", None)];
        scheduler
            .fill_subnets(&mut stale, &[], &mut Isolation::default())
            .await;
        assert_eq!(stale[0].subnet, None, "the refused subnet is still planned");
        let stored = store.get_network("late").await.unwrap().unwrap();
        assert_eq!(stored.subnet, None, "the refused subnet was stored");

        // The retry reads current state and picks a free /24.
        let mut fresh = store.list_networks().await.unwrap();
        scheduler
            .fill_subnets(&mut fresh, &[], &mut Isolation::default())
            .await;
        let stored = store.get_network("late").await.unwrap().unwrap();
        assert_eq!(stored.subnet.as_deref(), Some("10.100.1.0/24"));
        drop(scheduler);
        Arc::try_unwrap(store)
            .ok()
            .expect("scheduler dropped")
            .shutdown()
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod dns_service_name_tests {
    use super::dns_service_name;

    #[test]
    fn only_the_stack_prefix_comes_off_and_only_once() {
        for (name, stack, bare) in [
            ("shop.db", "shop", "db"),
            // A Compose service that is itself called `shop.db`.
            ("shop.shop.db", "shop", "shop.db"),
            // Already bare, no stack, or merely starting with the stack's name.
            ("db", "shop", "db"),
            ("shop.db", "", "shop.db"),
            ("shopdb", "shop", "shopdb"),
            ("shop_db", "shop", "shop_db"),
            ("shop.", "shop", "shop."),
        ] {
            assert_eq!(dns_service_name(name, stack), bare, "{name} in {stack:?}");
        }
    }
}
