//! Raft network implementation over TCP with bincode serialization.
//!
//! Each Raft node listens on a TCP port for incoming RPCs (AppendEntries,
//! Vote, InstallSnapshot). The [`TcpNetworkFactory`] creates clients that
//! connect to peer nodes and send RPCs. Messages are length-prefixed
//! (4-byte big-endian) bincode-encoded enums.
//!
//! For single-node clusters, [`StubNetworkFactory`] is still available —
//! it never sends RPCs because a single-node Raft never calls the network.
//!
//! # Wire protocol
//!
//! ```text
//! Request:  [4-byte BE length][bincode(RaftRequest)]
//! Response: [4-byte BE length][bincode(RaftReply)]
//! ```

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::TypeConfig;

// --- wire protocol ------------------------------------------------------------

/// A Raft RPC sent over the wire.
#[derive(Debug, Serialize, Deserialize)]
enum RaftRequest {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<u64>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

/// The reply to a [`RaftRequest`]. Errors are carried as strings; the
/// client wraps them in [`RPCError::Unreachable`].
#[derive(Debug, Serialize, Deserialize)]
enum RaftReply {
    AppendEntries(Result<AppendEntriesResponse<u64>, String>),
    Vote(Result<VoteResponse<u64>, String>),
    InstallSnapshot(Result<InstallSnapshotResponse<u64>, String>),
}

/// Transport-level error (IO, encode/decode). Wraps into `RPCError`.
#[derive(Debug, thiserror::Error)]
enum TransportError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
}

fn unreachable<E>(msg: &str) -> RPCError<u64, BasicNode, E>
where
    E: std::error::Error,
{
    RPCError::Unreachable(Unreachable::new(&TransportError::Decode(msg.to_string())))
}

fn network_err<E>(msg: &str) -> RPCError<u64, BasicNode, E>
where
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&TransportError::Decode(msg.to_string())))
}

// --- client side --------------------------------------------------------------

/// A network factory that creates TCP-based Raft network clients.
#[derive(Debug, Clone, Default)]
pub struct TcpNetworkFactory;

impl RaftNetworkFactory<TypeConfig> for TcpNetworkFactory {
    type Network = TcpNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        TcpNetwork {
            target,
            addr: node.addr.clone(),
        }
    }
}

/// A TCP-based Raft network client. Connects to the target node on each
/// RPC (simple, no connection pooling — sufficient for Phase 0).
#[derive(Debug, Clone)]
pub struct TcpNetwork {
    target: u64,
    addr: String,
}

impl TcpNetwork {
    /// Sends a request and reads the reply over a fresh TCP connection.
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, TransportError> {
        let encoded = bincode::serde::encode_to_vec(&request, bincode::config::standard())
            .map_err(|e| TransportError::Encode(e.to_string()))?;

        tracing::trace!(target = self.target, addr = %self.addr, "raft rpc send");
        let mut stream = TcpStream::connect(&self.addr).await?;

        // Write length-prefixed request.
        stream.write_u32(encoded.len() as u32).await?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;

        // Read length-prefixed response.
        let len = stream.read_u32().await? as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;

        let (reply, _): (RaftReply, usize) =
            bincode::serde::decode_from_slice(&buf, bincode::config::standard())
                .map_err(|e| TransportError::Decode(e.to_string()))?;

        Ok(reply)
    }
}

impl RaftNetwork<TypeConfig> for TcpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let reply = self
            .send(RaftRequest::AppendEntries(rpc))
            .await
            .map_err(|e| unreachable(&e.to_string()))?;
        match reply {
            RaftReply::AppendEntries(Ok(resp)) => Ok(resp),
            RaftReply::AppendEntries(Err(msg)) => Err(unreachable(&msg)),
            _ => Err(unreachable("wrong reply type for append_entries")),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let reply = self
            .send(RaftRequest::InstallSnapshot(rpc))
            .await
            .map_err(|e| unreachable(&e.to_string()))?;
        match reply {
            RaftReply::InstallSnapshot(Ok(resp)) => Ok(resp),
            RaftReply::InstallSnapshot(Err(msg)) => Err(unreachable(&msg)),
            _ => Err(unreachable("wrong reply type for install_snapshot")),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let reply = self
            .send(RaftRequest::Vote(rpc))
            .await
            .map_err(|e| unreachable(&e.to_string()))?;
        match reply {
            RaftReply::Vote(Ok(resp)) => Ok(resp),
            RaftReply::Vote(Err(msg)) => Err(unreachable(&msg)),
            _ => Err(unreachable("wrong reply type for vote")),
        }
    }
}

// --- server side --------------------------------------------------------------

/// Starts a TCP server that handles incoming Raft RPCs.
///
/// Returns a [`tokio::task::JoinHandle`] for the server task. Abort the
/// handle to stop the server.
///
/// The server accepts connections, reads a length-prefixed [`RaftRequest`],
/// dispatches it to the [`Raft`] instance, and writes back a [`RaftReply`].
pub async fn serve_raft_rpc(
    raft: Raft<TypeConfig>,
    bind_addr: &str,
) -> Result<tokio::task::JoinHandle<()>, std::io::Error> {
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!(addr = bind_addr, "raft rpc server listening");

    let handle = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let raft = raft.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, raft).await {
                            tracing::debug!(%peer, error = %e, "raft rpc connection closed");
                        }
                    });
                }
                Err(e) => {
                    tracing::error!(error = %e, "raft rpc accept error");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    });

    Ok(handle)
}

/// Handles a single Raft RPC connection: read request, dispatch, write reply.
async fn handle_connection(
    mut stream: TcpStream,
    raft: Raft<TypeConfig>,
) -> Result<(), TransportError> {
    // Read length-prefixed request.
    let len = stream.read_u32().await? as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;

    let (request, _): (RaftRequest, usize) =
        bincode::serde::decode_from_slice(&buf, bincode::config::standard())
            .map_err(|e| TransportError::Decode(e.to_string()))?;

    // Dispatch to Raft.
    let reply = match request {
        RaftRequest::AppendEntries(rpc) => match raft.append_entries(rpc).await {
            Ok(resp) => RaftReply::AppendEntries(Ok(resp)),
            Err(e) => RaftReply::AppendEntries(Err(e.to_string())),
        },
        RaftRequest::Vote(rpc) => match raft.vote(rpc).await {
            Ok(resp) => RaftReply::Vote(Ok(resp)),
            Err(e) => RaftReply::Vote(Err(e.to_string())),
        },
        RaftRequest::InstallSnapshot(rpc) => match raft.install_snapshot(rpc).await {
            Ok(resp) => RaftReply::InstallSnapshot(Ok(resp)),
            Err(e) => RaftReply::InstallSnapshot(Err(e.to_string())),
        },
    };

    // Write length-prefixed response.
    let encoded = bincode::serde::encode_to_vec(&reply, bincode::config::standard())
        .map_err(|e| TransportError::Encode(e.to_string()))?;
    stream.write_u32(encoded.len() as u32).await?;
    stream.write_all(&encoded).await?;
    stream.flush().await?;

    Ok(())
}

// --- stub (single-node only) --------------------------------------------------

/// A network factory that creates stub network clients.
///
/// All RPCs fail with an error. Only suitable for single-node clusters
/// where openraft never calls the network.
#[derive(Debug, Clone, Default)]
pub struct StubNetworkFactory;

impl RaftNetworkFactory<TypeConfig> for StubNetworkFactory {
    type Network = StubNetwork;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        StubNetwork
    }
}

/// A stub network client. All RPCs fail with an error.
#[derive(Debug, Clone, Default)]
pub struct StubNetwork;

impl RaftNetwork<TypeConfig> for StubNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(network_err(
            "stub network: append_entries should never be called",
        ))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        Err(network_err(
            "stub network: install_snapshot should never be called",
        ))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(network_err("stub network: vote should never be called"))
    }
}
