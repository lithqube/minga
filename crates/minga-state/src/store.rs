//! The public state store API: wraps a Raft instance and provides
//! high-level methods for reading and writing cluster state.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Raft};
use tokio::time::timeout;

use crate::command::{StateCommand, StateResponse};
use crate::error::StateError;
use crate::log_store::RedbLogStore;
use crate::network::{forward_write, serve_raft_rpc, RaftTlsConfig, TcpNetworkFactory};
use crate::state_machine::RedbStateMachine;
use crate::types::{
    ClusterNode, Config as StateConfig, ConfigId, Network, NetworkId, NodeId, Secret, SecretId,
    Service, ServiceId, Task, TaskId, Volume, VolumeId,
};
use crate::TypeConfig;

/// How long to wait for leader election or write acknowledgment.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// The cluster state store. Owns the Raft instance and provides
/// a high-level API for the rest of Minga.
pub struct StateStore {
    raft: Raft<TypeConfig>,
    reader: RedbStateMachine,
    node_id: NodeId,
    rpc_server: tokio::task::JoinHandle<()>,
    /// Client TLS config for forwarded writes (mTLS), when enabled.
    tls_client: Option<std::sync::Arc<rustls::ClientConfig>>,
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
        Self::open_inner(node_id, bind_addr, data_dir, peers, None).await
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
        Self::open_inner(node_id, bind_addr, data_dir, peers, Some(tls)).await
    }

    async fn open_inner(
        node_id: NodeId,
        bind_addr: impl Into<String>,
        data_dir: impl AsRef<Path>,
        peers: BTreeMap<NodeId, String>,
        tls: Option<RaftTlsConfig>,
    ) -> Result<Self, StateError> {
        let data_dir = data_dir.as_ref();
        tokio::fs::create_dir_all(data_dir).await?;

        let log_path = data_dir.join("raft_log.redb");
        let sm_path = data_dir.join("state.redb");

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

        // Initialize the cluster if it's pristine.
        let initialized = raft
            .is_initialized()
            .await
            .map_err(|e| StateError::Fatal(e.to_string()))?;

        if !initialized {
            let mut members: BTreeMap<NodeId, BasicNode> = BTreeMap::new();
            members.insert(node_id, BasicNode::new(bind_addr.clone()));
            for (id, addr) in &peers {
                members.insert(*id, BasicNode::new(addr.clone()));
            }
            if let Err(e) = raft.initialize(members).await {
                // Another node may have initialized concurrently.
                tokio::time::sleep(Duration::from_millis(200)).await;
                let now = raft
                    .is_initialized()
                    .await
                    .map_err(|e| StateError::Fatal(e.to_string()))?;
                if !now {
                    return Err(StateError::Raft(format!("initialize: {e}")));
                }
            }
        }

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

        Ok(Self {
            raft,
            reader,
            node_id,
            rpc_server,
            tls_client: client_tls,
        })
    }

    /// This node's Raft ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Whether this node is the current leader.
    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    /// The current leader's node ID, if known.
    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// Shuts down the Raft instance and the RPC server.
    pub async fn shutdown(self) -> Result<(), StateError> {
        self.rpc_server.abort();
        self.raft
            .shutdown()
            .await
            .map_err(|e| StateError::Fatal(e.to_string()))
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
                            let addr = fwd.leader_node.as_ref().map(|n| n.addr.clone());
                            match addr {
                                Some(addr) => {
                                    tracing::debug!(
                                        attempt,
                                        leader_addr = %addr,
                                        "forwarding write to leader"
                                    );
                                    match forward_write(&addr, &cmd, self.tls_client.clone()).await
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

    pub async fn upsert_node(&self, node: ClusterNode) -> Result<(), StateError> {
        self.propose(StateCommand::UpsertNode(node)).await?;
        Ok(())
    }

    pub async fn remove_node(&self, id: NodeId) -> Result<(), StateError> {
        self.propose(StateCommand::RemoveNode(id)).await?;
        Ok(())
    }

    pub async fn upsert_service(&self, service: Service) -> Result<(), StateError> {
        self.propose(StateCommand::UpsertService(service)).await?;
        Ok(())
    }

    pub async fn remove_service(&self, id: ServiceId) -> Result<(), StateError> {
        self.propose(StateCommand::RemoveService(id)).await?;
        Ok(())
    }

    pub async fn upsert_task(&self, task: Task) -> Result<(), StateError> {
        self.propose(StateCommand::UpsertTask(task)).await?;
        Ok(())
    }

    pub async fn remove_task(&self, id: TaskId) -> Result<(), StateError> {
        self.propose(StateCommand::RemoveTask(id)).await?;
        Ok(())
    }

    pub async fn upsert_network(&self, network: Network) -> Result<(), StateError> {
        self.propose(StateCommand::UpsertNetwork(network)).await?;
        Ok(())
    }

    pub async fn remove_network(&self, id: NetworkId) -> Result<(), StateError> {
        self.propose(StateCommand::RemoveNetwork(id)).await?;
        Ok(())
    }

    pub async fn upsert_volume(&self, volume: Volume) -> Result<(), StateError> {
        self.propose(StateCommand::UpsertVolume(volume)).await?;
        Ok(())
    }

    pub async fn remove_volume(&self, id: VolumeId) -> Result<(), StateError> {
        self.propose(StateCommand::RemoveVolume(id)).await?;
        Ok(())
    }

    pub async fn upsert_secret(&self, secret: Secret) -> Result<(), StateError> {
        self.propose(StateCommand::UpsertSecret(secret)).await?;
        Ok(())
    }

    pub async fn remove_secret(&self, id: SecretId) -> Result<(), StateError> {
        self.propose(StateCommand::RemoveSecret(id)).await?;
        Ok(())
    }

    pub async fn upsert_config(&self, config: StateConfig) -> Result<(), StateError> {
        self.propose(StateCommand::UpsertConfig(config)).await?;
        Ok(())
    }

    pub async fn remove_config(&self, id: ConfigId) -> Result<(), StateError> {
        self.propose(StateCommand::RemoveConfig(id)).await?;
        Ok(())
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
}
