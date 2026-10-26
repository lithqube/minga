//! Raft network implementation.
//!
//! For now this is a stub: it returns errors for all RPCs. A single-node
//! cluster never calls these methods. Multi-node networking (gRPC between
//! managers) will be implemented in a later phase.

use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;

use crate::TypeConfig;

/// A network factory that creates stub network clients.
#[derive(Debug, Clone, Default)]
pub struct StubNetworkFactory;

impl RaftNetworkFactory<TypeConfig> for StubNetworkFactory {
    type Network = StubNetwork;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        StubNetwork
    }
}

/// A stub network client. All RPCs fail with an error.
/// A single-node cluster never calls these.
#[derive(Debug, Clone, Default)]
pub struct StubNetwork;

impl RaftNetwork<TypeConfig> for StubNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(RPCError::Unreachable(Unreachable::new(&format_err(
            "append_entries",
        ))))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        Err(RPCError::Unreachable(Unreachable::new(&format_err(
            "install_snapshot",
        ))))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(RPCError::Unreachable(Unreachable::new(&format_err("vote"))))
    }
}

/// A simple error type for the stub network messages.
#[derive(Debug, thiserror::Error)]
#[error("stub network: {0} should never be called on a single-node cluster")]
struct StubError(String);

fn format_err(name: &str) -> StubError {
    StubError(name.to_string())
}
