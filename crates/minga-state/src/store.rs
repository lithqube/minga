//! The public state store API: wraps a Raft instance and provides
//! high-level methods for reading and writing cluster state.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, Raft};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;

use crate::command::{ObjectKind, StateCommand, StateResponse};
use crate::error::StateError;
use crate::log_store::RedbLogStore;
use crate::network::{forward_write, serve_raft_rpc, RaftTlsConfig, TcpNetworkFactory};
use crate::state_machine::RedbStateMachine;
use crate::types::{
    AgentCapabilities, ChangeRequest, ClusterNode, Config as StateConfig, ConfigId, Network,
    NetworkId, NodeId, Secret, SecretId, Service, ServiceId, Task, TaskId, Tenant, TenantQuotas,
    Volume, VolumeId, DEFAULT_TENANT_ID,
};
use crate::TypeConfig;

/// How long to wait for leader election or write acknowledgment.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// Four 500 ms heartbeats without a quorum acknowledgment is stale.
const QUORUM_ACK_FRESH_MS: u64 = 2_000;

fn quorum_ack_recent(
    node_id: NodeId,
    leader_id: Option<NodeId>,
    ack_age_ms: Option<u64>,
) -> Option<bool> {
    match leader_id {
        None => Some(false),
        Some(leader) if leader == node_id => {
            Some(ack_age_ms.is_some_and(|age| age <= QUORUM_ACK_FRESH_MS))
        }
        Some(_) => None,
    }
}

/// Desired state in a portable form, for backup and restore.
///
/// Holds tenants and desired resources: services, networks, volumes and
/// secret/config metadata. Nodes and tasks are left out: nodes re-register
/// themselves and the scheduler re-places tasks. Secrets and configs are
/// references, never values. Service specs carry environment values, so treat
/// a snapshot as sensitive.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateSnapshot {
    /// [`crate::codec::FORMAT_VERSION`] of the build that exported it.
    pub format_version: u8,
    pub exported_at_unix: u64,
    #[serde(default)]
    pub tenants: Vec<Tenant>,
    pub services: Vec<Service>,
    pub networks: Vec<Network>,
    pub volumes: Vec<Volume>,
    pub secrets: Vec<Secret>,
    pub configs: Vec<StateConfig>,
}

/// A read-only snapshot of this node's observed Raft state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftDiagnostics {
    pub node_id: NodeId,
    pub state: String,
    pub leader_id: Option<NodeId>,
    /// Matches the local readiness check: a leader is known to this node.
    pub ready: bool,
    /// Available only when this node is leader; `None` is not evidence that a
    /// follower has no cluster quorum.
    pub quorum_ack_age_ms: Option<u64>,
    /// For the local leader, whether quorum was acknowledged within two seconds.
    /// `None` means this node is a follower and cannot report the leader's age.
    pub quorum_ack_recent: Option<bool>,
    pub members: Vec<RaftMemberDiagnostics>,
}

/// A member from the membership configuration persisted in the Raft log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMemberDiagnostics {
    pub id: NodeId,
    pub address: String,
    pub voter: bool,
    /// Present on the leader only. `false` means no successful log progress
    /// has been reported for this member, not that its process is stopped.
    pub replication_acknowledged: Option<bool>,
    /// TCP reachability of the address stored in Raft membership; this is not
    /// proof that the remote Raft process is healthy.
    pub transport_reachable: Option<bool>,
}

/// The cluster state store. Owns the Raft instance and provides
/// a high-level API for the rest of Minga.
pub struct StateStore {
    raft: Raft<TypeConfig>,
    reader: RedbStateMachine,
    node_id: NodeId,
    rpc_server: tokio::task::JoinHandle<()>,
    /// Client TLS config for forwarded writes (mTLS), when enabled.
    tls_client: Option<std::sync::Arc<rustls::ClientConfig>>,
    /// Serializes membership changes initiated through this store.
    membership_change: tokio::sync::Mutex<()>,
    /// This manager's `--overlay-supernet`, once set.
    overlay_supernet: std::sync::OnceLock<minga_net::Cidr>,
}

/// Membership changes run only on the leader. A follower says which manager
/// leads instead of surfacing a raw `ForwardToLeader` error.
fn membership_error(e: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> StateError {
    match e.forward_to_leader() {
        Some(forward) => StateError::NotLeader(forward.leader_id),
        None => StateError::Raft(e.to_string()),
    }
}
/// The subnet rules for a network write, checked before proposing. The Raft
/// apply repeats all but the supernet rule: managers may run with different
/// supernets, and the apply must give every replica the same result.
fn check_network_subnet_rules(
    network: &Network,
    supernet: Option<&minga_net::Cidr>,
) -> Result<(), String> {
    let Some(raw) = &network.subnet else {
        return Ok(());
    };
    let subnet = minga_net::Cidr::parse(raw).map_err(|error| error.to_string())?;
    minga_net::overlay::check_network_subnet(&subnet, supernet)
}

fn command_error(message: String) -> StateError {
    if let Some(quota) = message.strip_prefix("tenant quota exceeded: ") {
        StateError::QuotaExceeded(quota.to_owned())
    } else {
        StateError::Raft(message)
    }
}

impl StateStore {
    /// Opens or creates a state store.
    ///
    /// - `node_id`: this node's Raft ID.
    /// - `bind_addr`: this node's address (e.g. `"10.0.0.1:50051"`).
    /// - `data_dir`: directory for redb files (`raft_log.redb`, `state.redb`).
    /// - `peers`: other nodes in the cluster as `(id, address)` pairs.
    ///   For a single-node cluster, pass an empty map.
    pub async fn open(
        node_id: NodeId,
        bind_addr: impl Into<String>,
        data_dir: impl AsRef<Path>,
        peers: BTreeMap<NodeId, String>,
    ) -> Result<Self, StateError> {
        Self::open_inner(node_id, bind_addr, data_dir, peers, None, true).await
    }

    /// Opens a store without initializing a pristine Raft node.
    ///
    /// A fresh node remains available for `add_learner` to populate it. It
    /// waits for a leader only when the local store is already initialized.
    pub async fn open_join(
        node_id: NodeId,
        bind_addr: impl Into<String>,
        data_dir: impl AsRef<Path>,
    ) -> Result<Self, StateError> {
        Self::open_inner(node_id, bind_addr, data_dir, BTreeMap::new(), None, false).await
    }

    /// Opens or creates a state store with mTLS on the Raft transport.
    ///
    /// Every peer (voters, and forwarded client writes) must present a
    /// certificate signed by the cluster CA; anything else fails the TLS
    /// handshake before it can vote, replicate or write.
    pub async fn open_secure(
        node_id: NodeId,
        bind_addr: impl Into<String>,
        data_dir: impl AsRef<Path>,
        peers: BTreeMap<NodeId, String>,
        tls: RaftTlsConfig,
    ) -> Result<Self, StateError> {
        Self::open_inner(node_id, bind_addr, data_dir, peers, Some(tls), true).await
    }

    /// Opens a store with mTLS without initializing a pristine Raft node.
    pub async fn open_join_secure(
        node_id: NodeId,
        bind_addr: impl Into<String>,
        data_dir: impl AsRef<Path>,
        tls: RaftTlsConfig,
    ) -> Result<Self, StateError> {
        Self::open_inner(
            node_id,
            bind_addr,
            data_dir,
            BTreeMap::new(),
            Some(tls),
            false,
        )
        .await
    }

    async fn open_inner(
        node_id: NodeId,
        bind_addr: impl Into<String>,
        data_dir: impl AsRef<Path>,
        peers: BTreeMap<NodeId, String>,
        tls: Option<RaftTlsConfig>,
        initialize_if_pristine: bool,
    ) -> Result<Self, StateError> {
        let data_dir = data_dir.as_ref();
        let log_path = data_dir.join("raft_log.redb");
        let sm_path = data_dir.join("state.redb");
        owner_only(data_dir, &[&log_path, &sm_path])?;

        let log_store =
            RedbLogStore::open(&log_path).map_err(|e| StateError::storage(e.to_string()))?;
        let state_machine =
            RedbStateMachine::open(&sm_path).map_err(|e| StateError::storage(e.to_string()))?;

        // Keep a clone for local reads; the original goes to Raft.
        let reader = state_machine.clone();

        let config = Arc::new(
            openraft::Config {
                cluster_name: "minga".to_string(),
                heartbeat_interval: 500,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                install_snapshot_timeout: 5000,
                ..Default::default()
            }
            .validate()
            .map_err(|e| StateError::storage(format!("invalid raft config: {e}")))?,
        );

        let bind_addr = bind_addr.into();

        let (factory, server_tls, client_tls) = match &tls {
            Some(config) => {
                let client = config
                    .client_config()
                    .map_err(|e| StateError::storage(e.to_string()))?;
                let server = config
                    .server_config()
                    .map_err(|e| StateError::storage(e.to_string()))?;
                (
                    TcpNetworkFactory::new_tls(client.clone()),
                    Some(server),
                    Some(client),
                )
            }
            None => (TcpNetworkFactory::new_plain(), None, None),
        };

        let raft = Raft::new(node_id, config, factory, log_store, state_machine)
            .await
            .map_err(|e| StateError::Fatal(e.to_string()))?;

        // Start the RPC server so peers can reach this node.
        let rpc_server = serve_raft_rpc(raft.clone(), &bind_addr, server_tls)
            .await
            .map_err(|e| StateError::storage(format!("rpc server bind {bind_addr}: {e}")))?;

        // A joining node must never bootstrap a separate cluster.
        let mut initialized = raft
            .is_initialized()
            .await
            .map_err(|e| StateError::Fatal(e.to_string()))?;

        if !initialized && initialize_if_pristine {
            let mut members: BTreeMap<NodeId, BasicNode> = BTreeMap::new();
            members.insert(node_id, BasicNode::new(bind_addr.clone()));
            for (id, addr) in &peers {
                members.insert(*id, BasicNode::new(addr.clone()));
            }
            if let Err(e) = raft.initialize(members).await {
                // Another node may have initialized concurrently.
                tokio::time::sleep(Duration::from_millis(200)).await;
                initialized = raft
                    .is_initialized()
                    .await
                    .map_err(|e| StateError::Fatal(e.to_string()))?;
                if !initialized {
                    return Err(StateError::Raft(format!("initialize: {e}")));
                }
            } else {
                initialized = true;
            }
        }

        if initialized {
            // Wait for a leader to be elected (any node, not just us). A node
            // whose peers are down must not die: a 2-voter cluster can only
            // recover from a restart if the surviving node waits. Log loudly
            // every interval so a misconfigured cluster is still visible.
            loop {
                let wait = raft.wait(Some(DEFAULT_TIMEOUT));
                let result = wait
                    .metrics(
                        |m| m.current_leader.is_some(),
                        "waiting for leader election",
                    )
                    .await;
                match result {
                    Ok(_) => break,
                    Err(e) => {
                        tracing::warn!(
                            node_id,
                            error = %e,
                            "no cluster leader yet; waiting for peers"
                        );
                    }
                }
            }
        }

        let store = Self {
            raft,
            reader,
            node_id,
            rpc_server,
            tls_client: client_tls,
            membership_change: tokio::sync::Mutex::new(()),
            overlay_supernet: std::sync::OnceLock::new(),
        };
        if initialized {
            let default_tenant = Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: DEFAULT_TENANT_ID.to_owned(),
                name: DEFAULT_TENANT_ID.to_owned(),
                quotas: TenantQuotas::default(),
                created_at: crate::types::unix_now(),
            };
            match store.create_tenant(default_tenant).await {
                Ok(()) | Err(StateError::Conflict(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(store)
    }

    /// This node's Raft ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
    /// Returns the current Raft voters.
    pub async fn manager_voters(&self) -> Vec<NodeId> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect()
    }

    /// Whether this node currently meets the Raft portion of `/readyz`.
    pub fn raft_ready(&self) -> bool {
        let metrics = self.raft.metrics();
        let metrics = metrics.borrow();
        let quorum_recent = quorum_ack_recent(
            self.node_id,
            metrics.current_leader,
            metrics.millis_since_quorum_ack,
        );
        metrics.current_leader.is_some() && quorum_recent != Some(false)
    }

    /// Returns persisted membership and observed Raft and transport facts.
    pub async fn raft_diagnostics(&self) -> RaftDiagnostics {
        let metrics = self.raft.metrics().borrow().clone();
        let voters = metrics
            .membership_config
            .voter_ids()
            .collect::<BTreeSet<_>>();
        let mut members = metrics
            .membership_config
            .nodes()
            .map(|(id, node)| RaftMemberDiagnostics {
                id: *id,
                address: node.addr.clone(),
                voter: voters.contains(id),
                replication_acknowledged: metrics
                    .replication
                    .as_ref()
                    .map(|progress| progress.get(id).is_some_and(|log_id| log_id.is_some())),
                transport_reachable: (*id == self.node_id).then_some(true),
            })
            .collect::<Vec<_>>();

        let mut probes = tokio::task::JoinSet::new();
        for member in members.iter().filter(|member| member.id != self.node_id) {
            let id = member.id;
            let address = member.address.clone();
            probes.spawn(async move {
                let reachable = timeout(
                    Duration::from_millis(300),
                    tokio::net::TcpStream::connect(address),
                )
                .await
                .is_ok_and(|result| result.is_ok());
                (id, reachable)
            });
        }
        while let Some(Ok((id, reachable))) = probes.join_next().await {
            if let Some(member) = members.iter_mut().find(|member| member.id == id) {
                member.transport_reachable = Some(reachable);
            }
        }
        let quorum_ack_recent = quorum_ack_recent(
            self.node_id,
            metrics.current_leader,
            metrics.millis_since_quorum_ack,
        );

        RaftDiagnostics {
            node_id: self.node_id,
            state: format!("{:?}", metrics.state),
            leader_id: metrics.current_leader,
            ready: metrics.current_leader.is_some() && quorum_ack_recent != Some(false),
            quorum_ack_age_ms: metrics.millis_since_quorum_ack,
            quorum_ack_recent,
            members,
        }
    }

    /// Adds a manager as a learner, waits for it to catch up, then promotes it.
    pub async fn add_manager(&self, id: NodeId, raft_addr: String) -> Result<(), StateError> {
        let _guard = self.membership_change.lock().await;
        self.require_leader().await?;
        if id == self.node_id {
            return Err(StateError::Membership(format!(
                "manager {id} is already this node"
            )));
        }

        let current = self.raft.metrics().borrow().membership_config.clone();
        let voters = current.voter_ids().collect::<BTreeSet<_>>();
        let existing_addr = current
            .nodes()
            .find(|(node_id, _)| **node_id == id)
            .map(|(_, node)| node.addr.clone());
        if voters.contains(&id) {
            return Err(StateError::Membership(format!(
                "manager {id} is already a voter"
            )));
        }
        if existing_addr
            .as_deref()
            .is_some_and(|addr| addr != raft_addr)
        {
            return Err(StateError::Membership(format!(
                "manager {id} already has a different Raft address"
            )));
        }

        if existing_addr.is_none() {
            timeout(
                Duration::from_secs(60),
                self.raft.add_learner(id, BasicNode::new(raft_addr), true),
            )
            .await
            .map_err(|_| StateError::Raft(format!("manager {id} did not catch up in 60 seconds")))?
            .map_err(membership_error)?;
        }

        let mut voters = self
            .manager_voters()
            .await
            .into_iter()
            .collect::<BTreeSet<_>>();
        voters.insert(id);
        timeout(DEFAULT_TIMEOUT, self.raft.change_membership(voters, false))
            .await
            .map_err(|_| StateError::Raft("manager promotion timed out".into()))?
            .map_err(membership_error)?;
        Ok(())
    }

    /// Removes a manager, refusing to remove the last voter.
    pub async fn remove_manager(&self, id: NodeId) -> Result<(), StateError> {
        let _guard = self.membership_change.lock().await;
        self.require_leader().await?;
        let mut voters = self
            .manager_voters()
            .await
            .into_iter()
            .collect::<BTreeSet<_>>();
        if !voters.contains(&id) {
            return Err(StateError::Membership(format!(
                "manager {id} is not a voter"
            )));
        }
        if voters.len() == 1 {
            return Err(StateError::Membership(
                "cannot remove the last manager; cluster would lose quorum".into(),
            ));
        }
        voters.remove(&id);
        timeout(DEFAULT_TIMEOUT, self.raft.change_membership(voters, false))
            .await
            .map_err(|_| StateError::Raft("manager removal timed out".into()))?
            .map_err(membership_error)?;
        Ok(())
    }

    /// Replaces v1.2.0 voter membership with an explicitly supplied manager set.
    ///
    /// The requested voters must all be current voters; this migration only
    /// removes worker and API voters and commits the transition in one Raft
    /// membership change.
    pub async fn migrate_manager_voters(
        &self,
        manager_ids: Vec<NodeId>,
    ) -> Result<Vec<NodeId>, StateError> {
        let _guard = self.membership_change.lock().await;
        self.require_leader().await?;
        let current = self
            .manager_voters()
            .await
            .into_iter()
            .collect::<BTreeSet<_>>();
        let requested = manager_ids.iter().copied().collect::<BTreeSet<_>>();
        if manager_ids.len() != requested.len() {
            return Err(StateError::Membership(
                "manager IDs must not contain duplicates".into(),
            ));
        }
        if requested.is_empty() {
            return Err(StateError::Membership(
                "migration must retain at least one manager".into(),
            ));
        }
        if !requested.is_subset(&current) {
            return Err(StateError::Membership(
                "migration can only retain current voters".into(),
            ));
        }
        let removed = current.difference(&requested).copied().collect::<Vec<_>>();
        if !removed.is_empty() {
            timeout(
                DEFAULT_TIMEOUT,
                self.raft.change_membership(requested, false),
            )
            .await
            .map_err(|_| StateError::Raft("manager migration timed out".into()))?
            .map_err(membership_error)?;
        }
        Ok(removed)
    }

    /// Whether this node is the current leader.
    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    /// The current leader's node ID, if known.
    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// The last log index observed by this Raft node, for diagnostics.
    pub fn last_log_index(&self) -> Option<u64> {
        self.raft.metrics().borrow().last_log_index
    }

    /// Shuts down the Raft instance and the RPC server.
    pub async fn shutdown(self) -> Result<(), StateError> {
        self.rpc_server.abort();
        self.raft
            .shutdown()
            .await
            .map_err(|e| StateError::Fatal(e.to_string()))
    }

    /// Membership changes are not forwarded; a follower names the leader.
    async fn require_leader(&self) -> Result<(), StateError> {
        if self.is_leader().await {
            Ok(())
        } else {
            Err(StateError::NotLeader(self.current_leader().await))
        }
    }

    // --- write operations (go through Raft) -----------------------------------

    /// Proposes a command to Raft and waits for it to be committed.
    ///
    /// Only the leader accepts writes. If this node is a follower, the
    /// command is forwarded to the current leader over the Raft RPC
    /// transport; if leadership changed mid-flight the command is
    /// retried against the new leader. Bounded attempts: a cluster
    /// without a stable leader fails the write instead of spinning.
    async fn propose(&self, cmd: StateCommand) -> Result<StateResponse, StateError> {
        const MAX_ATTEMPTS: usize = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let result = timeout(DEFAULT_TIMEOUT, self.raft.client_write(cmd.clone()))
                .await
                .map_err(|_| StateError::storage("write timed out"))?;

            match result {
                Ok(response) => return Ok(response.data),
                Err(err) => {
                    let forward = err.forward_to_leader();
                    match forward {
                        Some(fwd) if attempt < MAX_ATTEMPTS => {
                            let leader = fwd
                                .leader_id
                                .zip(fwd.leader_node.as_ref().map(|n| n.addr.clone()));
                            match leader {
                                Some((leader_id, addr)) => {
                                    tracing::debug!(
                                        attempt,
                                        leader_id,
                                        leader_addr = %addr,
                                        "forwarding write to leader"
                                    );
                                    match forward_write(
                                        leader_id,
                                        &addr,
                                        &cmd,
                                        self.tls_client.clone(),
                                    )
                                    .await
                                    {
                                        Ok(resp) => return Ok(resp),
                                        Err(e) => {
                                            tracing::warn!(
                                                attempt,
                                                error = %e,
                                                "forwarded write failed; rediscovering leader"
                                            );
                                        }
                                    }
                                }
                                // Leader unknown: wait for the election to
                                // settle, then retry locally.
                                None => {
                                    tokio::time::sleep(Duration::from_millis(200)).await;
                                }
                            }
                        }
                        _ => return Err(StateError::Raft(err.to_string())),
                    }
                }
            }
        }
        unreachable!("loop returns on success, final failure, or exhausts attempts")
    }
    /// Creates a tenant through Raft.
    pub async fn create_tenant(&self, tenant: Tenant) -> Result<(), StateError> {
        tenant
            .validate()
            .map_err(|error| StateError::InvalidTenant(error.into()))?;
        match self.propose(StateCommand::CreateTenant(tenant)).await? {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::Conflict(message) => Err(StateError::Conflict(message)),
            StateResponse::NotFound => Err(StateError::NotFound("tenant".into())),
            StateResponse::Error(message) => Err(StateError::Raft(message)),
        }
    }

    /// Updates a tenant's name and quotas without changing its identity or creation time.
    pub async fn update_tenant(
        &self,
        id: impl AsRef<str>,
        name: String,
        quotas: TenantQuotas,
    ) -> Result<(), StateError> {
        let id = id.as_ref().to_owned();
        let tenant = Tenant {
            nats_revoked_user_public_keys: Vec::new(),
            id: id.clone(),
            name,
            quotas,
            created_at: 0,
        };
        tenant
            .validate()
            .map_err(|error| StateError::InvalidTenant(error.into()))?;
        match self
            .propose(StateCommand::UpdateTenant {
                id: id.clone(),
                name: tenant.name,
                quotas: tenant.quotas,
            })
            .await?
        {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::NotFound => Err(StateError::NotFound(id)),
            StateResponse::Conflict(message) => Err(StateError::Conflict(message)),
            StateResponse::Error(message) => Err(StateError::Raft(message)),
        }
    }
    /// Records a revoked node NKey in permanent cluster state.
    pub async fn record_nats_user_revocation(
        &self,
        user_public_key: String,
    ) -> Result<(), StateError> {
        match self
            .propose(StateCommand::RecordNatsUserRevocation { user_public_key })
            .await?
        {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::NotFound => Err(StateError::NotFound("default tenant".into())),
            StateResponse::Conflict(message) | StateResponse::Error(message) => {
                Err(StateError::Raft(message))
            }
        }
    }

    /// Upserts a tenant when importing a portable snapshot.
    pub async fn upsert_tenant(&self, tenant: Tenant) -> Result<(), StateError> {
        tenant
            .validate()
            .map_err(|error| StateError::InvalidTenant(error.into()))?;
        self.propose(StateCommand::UpsertTenant(tenant)).await?;
        Ok(())
    }

    /// Removes a tenant only when it owns no resources; `default` is permanent.
    pub async fn remove_tenant(&self, id: impl AsRef<str>) -> Result<(), StateError> {
        let id = id.as_ref();
        Tenant::validate_id(id).map_err(|error| StateError::InvalidTenant(error.into()))?;
        match self
            .propose(StateCommand::RemoveTenant(id.to_owned()))
            .await?
        {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::NotFound => Err(StateError::NotFound(id.to_owned())),
            StateResponse::Conflict(message) => Err(StateError::Conflict(message)),
            StateResponse::Error(message) => Err(StateError::Raft(message)),
        }
    }

    pub async fn upsert_node(&self, node: ClusterNode) -> Result<(), StateError> {
        let tenant_id = node.tenant_id.clone();
        match self.propose(StateCommand::UpsertNode(node)).await? {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::NotFound => Err(StateError::NotFound(format!("tenant {tenant_id}"))),
            StateResponse::Conflict(_) => Err(StateError::NameInUse),
            StateResponse::Error(message) => Err(command_error(message)),
        }
    }

    pub async fn remove_node(&self, id: NodeId) -> Result<(), StateError> {
        self.propose(StateCommand::RemoveNode(id)).await?;
        Ok(())
    }
    /// Stores only the public NATS user identity used for account revocation.
    pub async fn set_node_nats_user_public_key(
        &self,
        id: NodeId,
        public_key: Option<String>,
    ) -> Result<(), StateError> {
        match self
            .propose(StateCommand::SetNodeNatsUserPublicKey { id, public_key })
            .await?
        {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::NotFound => Err(StateError::NotFound(format!("node {id}"))),
            StateResponse::Error(message) => Err(command_error(message)),
            StateResponse::Conflict(_) => Err(StateError::NameInUse),
        }
    }

    /// Returns the request as committed, which a lagging follower may not hold yet.
    pub async fn create_change_request(
        &self,
        request: ChangeRequest,
    ) -> Result<ChangeRequest, StateError> {
        match self
            .propose(StateCommand::CreateChangeRequest(Box::new(request)))
            .await?
        {
            StateResponse::ChangeRequest(request) => Ok(*request),
            StateResponse::Ok => Err(StateError::storage("create returned no change request")),
            StateResponse::NotFound => Err(StateError::NotFound("tenant or service".into())),
            StateResponse::Conflict(message) | StateResponse::Error(message) => {
                Err(StateError::Conflict(message))
            }
        }
    }

    pub async fn decide_change_request(
        &self,
        id: String,
        tenant_id: String,
        actor: String,
        approve: bool,
        now_unix: u64,
    ) -> Result<ChangeRequest, StateError> {
        match self
            .propose(StateCommand::DecideChangeRequest {
                id,
                tenant_id,
                actor,
                approve,
                now_unix,
            })
            .await?
        {
            // Every transition this decision made, including expired and
            // superseded; its status says which.
            StateResponse::ChangeRequest(request) => Ok(*request),
            StateResponse::Ok => Err(StateError::storage("decision returned no change request")),
            StateResponse::NotFound => Err(StateError::NotFound("change request".into())),
            StateResponse::Conflict(message) | StateResponse::Error(message) => {
                Err(StateError::Conflict(message))
            }
        }
    }

    pub async fn upsert_service(&self, service: Service) -> Result<(), StateError> {
        self.write_service(StateCommand::UpsertService(service))
            .await
    }

    async fn write_service(&self, command: StateCommand) -> Result<(), StateError> {
        match self.propose(command).await? {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::Conflict(_) => Err(StateError::NameInUse),
            StateResponse::Error(message) if message.starts_with("tenant quota exceeded: ") => {
                Err(command_error(message))
            }
            StateResponse::Error(message) => Err(StateError::PortConflict(message)),
            StateResponse::NotFound => Err(StateError::NotFound("tenant".into())),
        }
    }

    /// Removes the service only while it still belongs to `tenant_id`;
    /// one that is gone or owned by another tenant is left alone.
    pub async fn remove_service(&self, id: ServiceId, tenant_id: &str) -> Result<(), StateError> {
        self.remove_owned(ObjectKind::Service, id, tenant_id).await
    }

    async fn remove_owned(
        &self,
        kind: ObjectKind,
        id: String,
        tenant_id: &str,
    ) -> Result<(), StateError> {
        match self
            .propose(StateCommand::RemoveOwned {
                kind,
                id,
                tenant_id: tenant_id.to_owned(),
            })
            .await?
        {
            StateResponse::Conflict(message) => Err(StateError::Conflict(message)),
            _ => Ok(()),
        }
    }

    pub async fn upsert_task(&self, task: Task) -> Result<(), StateError> {
        match self.propose(StateCommand::UpsertTask(task)).await? {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::Conflict(_) => Err(StateError::NameInUse),
            StateResponse::Error(message) => Err(command_error(message)),
            StateResponse::NotFound => Err(StateError::NotFound("tenant".into())),
        }
    }

    /// Removes the task only while it still belongs to `tenant_id`;
    /// one that is gone or owned by another tenant is left alone.
    pub async fn remove_task(&self, id: TaskId, tenant_id: &str) -> Result<(), StateError> {
        self.remove_owned(ObjectKind::Task, id, tenant_id).await
    }

    /// Confines network subnets this store proposes to `supernet` from now
    /// on; a later call keeps the first value. Managers call it after opening.
    pub fn set_overlay_supernet(&self, supernet: minga_net::Cidr) {
        let _ = self.overlay_supernet.set(supernet);
    }

    /// The overlay supernet set by `set_overlay_supernet`, if any.
    pub fn overlay_supernet(&self) -> Option<minga_net::Cidr> {
        self.overlay_supernet.get().copied()
    }

    pub async fn upsert_network(&self, network: Network) -> Result<(), StateError> {
        check_network_subnet_rules(&network, self.overlay_supernet.get())
            .map_err(StateError::Raft)?;
        match self.propose(StateCommand::UpsertNetwork(network)).await? {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::Conflict(message) if message == crate::state_machine::SUBNET_OVERLAP => {
                Err(StateError::SubnetOverlap)
            }
            StateResponse::Conflict(_) => Err(StateError::NameInUse),
            StateResponse::Error(message) => Err(command_error(message)),
            StateResponse::NotFound => Err(StateError::NotFound("tenant".into())),
        }
    }

    /// Removes the network only while it still belongs to `tenant_id`;
    /// one that is gone or owned by another tenant is left alone.
    pub async fn remove_network(&self, id: NetworkId, tenant_id: &str) -> Result<(), StateError> {
        self.remove_owned(ObjectKind::Network, id, tenant_id).await
    }

    pub async fn upsert_volume(&self, volume: Volume) -> Result<(), StateError> {
        match self.propose(StateCommand::UpsertVolume(volume)).await? {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::Conflict(_) => Err(StateError::NameInUse),
            StateResponse::Error(message) => Err(command_error(message)),
            StateResponse::NotFound => Err(StateError::NotFound("tenant".into())),
        }
    }
    /// Removes the volume only while it still belongs to `tenant_id`;
    /// one that is gone or owned by another tenant is left alone.
    pub async fn remove_volume(&self, id: VolumeId, tenant_id: &str) -> Result<(), StateError> {
        self.remove_owned(ObjectKind::Volume, id, tenant_id).await
    }

    pub async fn upsert_secret(&self, secret: Secret) -> Result<(), StateError> {
        match self.propose(StateCommand::UpsertSecret(secret)).await? {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::Conflict(_) => Err(StateError::NameInUse),
            StateResponse::Error(message) => Err(command_error(message)),
            StateResponse::NotFound => Err(StateError::NotFound("tenant".into())),
        }
    }

    /// Removes the secret only while it still belongs to `tenant_id`;
    /// one that is gone or owned by another tenant is left alone.
    pub async fn remove_secret(&self, id: SecretId, tenant_id: &str) -> Result<(), StateError> {
        self.remove_owned(ObjectKind::Secret, id, tenant_id).await
    }

    pub async fn upsert_config(&self, config: StateConfig) -> Result<(), StateError> {
        match self.propose(StateCommand::UpsertConfig(config)).await? {
            StateResponse::Ok | StateResponse::ChangeRequest(_) => Ok(()),
            StateResponse::Conflict(_) => Err(StateError::NameInUse),
            StateResponse::Error(message) => Err(command_error(message)),
            StateResponse::NotFound => Err(StateError::NotFound("tenant".into())),
        }
    }

    /// Removes the config only while it still belongs to `tenant_id`;
    /// one that is gone or owned by another tenant is left alone.
    pub async fn remove_config(&self, id: ConfigId, tenant_id: &str) -> Result<(), StateError> {
        self.remove_owned(ObjectKind::Config, id, tenant_id).await
    }

    // --- read operations (local reads from the state machine) -----------------

    pub async fn get_node(&self, id: NodeId) -> Result<Option<ClusterNode>, StateError> {
        self.reader.get_node(id).await.map_err(StateError::storage)
    }

    pub async fn list_nodes(&self) -> Result<Vec<ClusterNode>, StateError> {
        self.reader.list_nodes().await.map_err(StateError::storage)
    }

    pub async fn get_service(&self, id: impl AsRef<str>) -> Result<Option<Service>, StateError> {
        self.reader
            .get_service(id.as_ref())
            .await
            .map_err(StateError::storage)
    }

    pub async fn list_services(&self) -> Result<Vec<Service>, StateError> {
        self.reader
            .list_services()
            .await
            .map_err(StateError::storage)
    }

    pub async fn get_change_request(
        &self,
        tenant_id: &str,
        id: &str,
    ) -> Result<Option<ChangeRequest>, StateError> {
        self.reader
            .get_change_request(tenant_id, id)
            .await
            .map_err(StateError::storage)
    }

    /// One tenant's change requests, unordered.
    pub async fn list_change_requests(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<ChangeRequest>, StateError> {
        self.reader
            .list_change_requests(tenant_id)
            .await
            .map_err(StateError::storage)
    }

    pub async fn get_task(&self, id: impl AsRef<str>) -> Result<Option<Task>, StateError> {
        self.reader
            .get_task(id.as_ref())
            .await
            .map_err(StateError::storage)
    }

    pub async fn list_tasks(&self) -> Result<Vec<Task>, StateError> {
        self.reader.list_tasks().await.map_err(StateError::storage)
    }

    pub async fn get_network(&self, id: impl AsRef<str>) -> Result<Option<Network>, StateError> {
        self.reader
            .get_network(id.as_ref())
            .await
            .map_err(StateError::storage)
    }

    pub async fn list_networks(&self) -> Result<Vec<Network>, StateError> {
        self.reader
            .list_networks()
            .await
            .map_err(StateError::storage)
    }

    pub async fn get_volume(&self, id: impl AsRef<str>) -> Result<Option<Volume>, StateError> {
        self.reader
            .get_volume(id.as_ref())
            .await
            .map_err(StateError::storage)
    }

    pub async fn list_volumes(&self) -> Result<Vec<Volume>, StateError> {
        self.reader
            .list_volumes()
            .await
            .map_err(StateError::storage)
    }

    pub async fn get_secret(&self, id: impl AsRef<str>) -> Result<Option<Secret>, StateError> {
        self.reader
            .get_secret(id.as_ref())
            .await
            .map_err(StateError::storage)
    }

    pub async fn list_secrets(&self) -> Result<Vec<Secret>, StateError> {
        self.reader
            .list_secrets()
            .await
            .map_err(StateError::storage)
    }

    pub async fn get_config(&self, id: impl AsRef<str>) -> Result<Option<StateConfig>, StateError> {
        self.reader
            .get_config(id.as_ref())
            .await
            .map_err(StateError::storage)
    }

    pub async fn list_configs(&self) -> Result<Vec<StateConfig>, StateError> {
        self.reader
            .list_configs()
            .await
            .map_err(StateError::storage)
    }

    pub async fn get_tenant(&self, id: impl AsRef<str>) -> Result<Option<Tenant>, StateError> {
        self.reader
            .get_tenant(id.as_ref())
            .await
            .map_err(StateError::storage)
    }

    pub async fn list_tenants(&self) -> Result<Vec<Tenant>, StateError> {
        let mut tenants = self
            .reader
            .list_tenants()
            .await
            .map_err(StateError::storage)?;
        tenants.sort_unstable_by(|a, b| a.id.cmp(&b.id));
        Ok(tenants)
    }

    // --- backup and restore -----------------------------------------------------

    /// Reads desired state into a [`StateSnapshot`].
    pub async fn export(&self) -> Result<StateSnapshot, StateError> {
        Ok(StateSnapshot {
            format_version: crate::codec::FORMAT_VERSION,
            exported_at_unix: crate::types::unix_now(),
            tenants: self.list_tenants().await?,
            services: self.list_services().await?,
            networks: self.list_networks().await?,
            volumes: self.list_volumes().await?,
            secrets: self.list_secrets().await?,
            configs: self.list_configs().await?,
        })
    }

    /// Writes a snapshot through Raft, tenants before their resources.
    /// Nothing is written when the snapshot is refused. Existing tenant,
    /// network, volume, secret and config records are upserted; existing
    /// services make this fail with [`StateError::Conflict`] unless
    /// `overwrite` is set.
    pub async fn import(&self, snapshot: StateSnapshot, overwrite: bool) -> Result<(), StateError> {
        let want = crate::codec::FORMAT_VERSION;
        match snapshot.format_version {
            crate::codec::FORMAT_VERSION
            | crate::codec::V9_FORMAT_VERSION
            | crate::codec::V11_FORMAT_VERSION
            | crate::codec::V10_FORMAT_VERSION
            | crate::codec::V8_FORMAT_VERSION
            | crate::codec::V7_FORMAT_VERSION
            | crate::codec::V6_FORMAT_VERSION
            | crate::codec::V5_FORMAT_VERSION
            | crate::codec::V4_FORMAT_VERSION
            | crate::codec::V3_FORMAT_VERSION
            | crate::codec::V2_FORMAT_VERSION => {
                // Egress policy defaults to none; pre-v9 agents get the v8
                // migration's grants; legacy resource fields default to
                // `default`; ports without `host_ip` publish on every
                // address. Nodes are not in snapshots.
            }
            found => {
                return Err(StateError::BadSnapshot(format!(
                    "snapshot has format version {found}, this build reads {want}"
                )));
            }
        }
        let legacy = snapshot.format_version < want;
        if legacy {
            for s in &snapshot.services {
                check_legacy_shape(s, snapshot.format_version)?;
            }
        }
        if !overwrite {
            let mut clashes = Vec::new();
            for s in &snapshot.services {
                if self.get_service(&s.id).await?.is_some() {
                    clashes.push(s.id.clone());
                }
            }
            if !clashes.is_empty() {
                return Err(StateError::Conflict(clashes.join(", ")));
            }
        }
        // Every network rule runs before the first write, so a refused network
        // leaves nothing half-imported.
        for network in &snapshot.networks {
            check_network_subnet_rules(network, self.overlay_supernet.get()).map_err(|error| {
                StateError::BadSnapshot(format!("network {}: {error}", network.id))
            })?;
        }
        // The apply refuses a network whose subnet overlaps another's, so replay
        // that rule here: a legacy snapshot with two networks on one subnet is
        // refused whole instead of stopping partway. A network written
        // concurrently after this check can still make the apply refuse one.
        check_import_subnet_overlaps(&self.list_networks().await?, &snapshot.networks)
            .map_err(StateError::BadSnapshot)?;
        let mut tenants = snapshot.tenants;
        if !tenants.iter().any(|tenant| tenant.id == DEFAULT_TENANT_ID) {
            tenants.push(Tenant {
                nats_revoked_user_public_keys: Vec::new(),
                id: DEFAULT_TENANT_ID.to_string(),
                name: DEFAULT_TENANT_ID.to_string(),
                quotas: TenantQuotas::default(),
                created_at: crate::types::unix_now(),
            });
        }
        // Existing tenants keep their quotas and name: a snapshot (or a
        // pre-v8 one with a synthesized `default`) only adds missing tenants.
        for tenant in tenants {
            match self.create_tenant(tenant).await {
                Ok(()) | Err(StateError::Conflict(_)) => {}
                Err(error) => return Err(error),
            }
        }
        for n in snapshot.networks {
            self.upsert_network(n).await?;
        }
        for s in snapshot.secrets {
            self.upsert_secret(s).await?;
        }
        for c in snapshot.configs {
            self.upsert_config(c).await?;
        }
        for v in snapshot.volumes {
            self.upsert_volume(v).await?;
        }
        // An older snapshot's services grow when re-encoded (v9 adds agent
        // capabilities, v10 the egress policy), so a tenant at its byte
        // quota could not import its own data. Each write carries that tenant's growth so far; the quota
        // itself never changes, and current-format imports get no allowance.
        // Keyed by service id: a repeated id replaces its record, so only its
        // latest growth counts.
        let mut growth: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        for s in snapshot.services {
            if legacy {
                let tenant = growth.entry(s.tenant_id.clone()).or_default();
                tenant.insert(s.id.clone(), legacy_growth(&s, snapshot.format_version)?);
                let byte_allowance = tenant.values().fold(0u64, |sum, g| sum.saturating_add(*g));
                self.write_service(StateCommand::ImportService {
                    service: s,
                    byte_allowance,
                })
                .await?;
            } else {
                self.upsert_service(s).await?;
            }
        }
        Ok(())
    }
}

/// Applies the `UpsertNetwork` overlap rule to `incoming`, in order, over the
/// `existing` networks. A network keeping its stored subnet is not rechecked.
fn check_import_subnet_overlaps(existing: &[Network], incoming: &[Network]) -> Result<(), String> {
    let mut subnets: BTreeMap<&str, Option<&str>> = existing
        .iter()
        .map(|network| (network.id.as_str(), network.subnet.as_deref()))
        .collect();
    for network in incoming {
        let subnet = network.subnet.as_deref();
        if subnets.get(network.id.as_str()) != Some(&subnet) {
            if let Some(mine) = subnet.and_then(|raw| minga_net::Cidr::parse(raw).ok()) {
                let clash = subnets.iter().any(|(id, theirs)| {
                    *id != network.id
                        && theirs
                            .and_then(|raw| minga_net::Cidr::parse(raw).ok())
                            .is_some_and(|theirs| theirs.overlaps(&mine))
                });
                if clash {
                    return Err(format!(
                        "network {}: {}",
                        network.id,
                        StateError::SubnetOverlap
                    ));
                }
            }
        }
        subnets.insert(network.id.as_str(), subnet);
    }
    Ok(())
}

/// Refuses a legacy service (nested `previous` included) carrying fields its
/// version could not have: an egress policy before v10, or agent
/// capabilities other than the v8 migration's before v9. The snapshot's
/// version is caller-chosen, so this keeps [`legacy_growth`] to migration
/// growth.
fn check_legacy_shape(service: &Service, from_version: u8) -> Result<(), StateError> {
    if from_version < crate::codec::V10_FORMAT_VERSION && service.egress.is_some() {
        return Err(StateError::BadSnapshot(format!(
            "service `{}`: a pre-v10 snapshot cannot declare an egress policy",
            service.id
        )));
    }
    if from_version < crate::codec::V9_FORMAT_VERSION {
        if let Some(agent) = &service.agent {
            if agent.capabilities != AgentCapabilities::v8_migrated(&agent.sandbox_env) {
                return Err(StateError::BadSnapshot(format!(
                    "service `{}`: a pre-v9 snapshot cannot declare agent capabilities",
                    service.id
                )));
            }
        }
    }
    service.previous.as_deref().map_or(Ok(()), |previous| {
        check_legacy_shape(previous, from_version)
    })
}

/// Bytes a service gained over its encoding at `from_version`: the `egress`
/// key added in v10 and, before v9, the agent `capabilities` key, including
/// in the specs nested in `previous`.
fn legacy_growth(service: &Service, from_version: u8) -> Result<u64, StateError> {
    fn strip(value: &mut serde_json::Value, from_version: u8) {
        let Some(object) = value.as_object_mut() else {
            return;
        };
        // v10 already stored `egress`, so only older formats grew by it.
        if from_version < crate::codec::V10_FORMAT_VERSION {
            object.remove("egress");
        }
        if from_version < crate::codec::V9_FORMAT_VERSION {
            if let Some(agent) = object.get_mut("agent").and_then(|a| a.as_object_mut()) {
                agent.remove("capabilities");
            }
        }
        if let Some(previous) = object.get_mut("previous") {
            strip(previous, from_version);
        }
    }
    let bad = |e: serde_json::Error| StateError::BadSnapshot(e.to_string());
    let mut value = serde_json::to_value(service).map_err(bad)?;
    let new = serde_json::to_vec(&value).map_err(bad)?.len();
    strip(&mut value, from_version);
    let old = serde_json::to_vec(&value).map_err(bad)?.len();
    Ok((new - old) as u64)
}

/// Makes the data directory 0700 and each store file 0600 before redb opens them.
///
/// The Raft log and state machine hold every service's environment values in
/// plaintext, so they get the same owner-only treatment as `minga state export`.
/// Both are tightened when they already exist, so an upgrade fixes older installs.
fn owner_only(dir: &Path, files: &[&Path]) -> std::io::Result<()> {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    for file in files {
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(file)?
            .set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::owner_only;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn data_dir_and_store_files_end_up_owner_only() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("data");
        let file = dir.join("state.redb");
        let mode = |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;

        owner_only(&dir, &[&file]).unwrap();
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(&file), 0o600);

        // An install from before this change is tightened and keeps its data.
        std::fs::write(&file, b"existing").unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
        owner_only(&dir, &[&file]).unwrap();
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(mode(&file), 0o600);
        assert_eq!(std::fs::read(&file).unwrap(), b"existing");
    }
}
