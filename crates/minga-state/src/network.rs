//! Raft network implementation over TCP with JSON serialization.
//!
//! Each Raft node listens on a TCP port for incoming RPCs (AppendEntries,
//! Vote, InstallSnapshot). The [`TcpNetworkFactory`] creates clients that
//! connect to peer nodes and send RPCs. Messages are length-prefixed
//! (4-byte big-endian) enums encoded by [`crate::codec`].
//!
//! For single-node clusters, [`StubNetworkFactory`] is still available —
//! it never sends RPCs because a single-node Raft never calls the network.
//!
//! # Wire protocol
//!
//! ```text
//! Request:  [4-byte BE length][format version byte][JSON(RaftRequest)]
//! Response: [4-byte BE length][format version byte][JSON(RaftReply)]
//! ```

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::command::{StateCommand, StateResponse};
use crate::error::StateError;
use crate::TypeConfig;

// --- transport security ---------------------------------------------------------

/// The SAN every Minga node certificate carries. Any cluster peer must
/// present a certificate signed by the cluster CA with this name — one
/// identity, no per-node PKI bookkeeping.
pub const MINGA_NODE_NAME: &str = "minga-node";

/// TLS material for the Raft transport (PEM paths). The same certificate
/// is used for the server and client roles (mTLS).
#[derive(Debug, Clone)]
pub struct RaftTlsConfig {
    /// PEM file with the cluster CA certificate.
    pub ca_cert: std::path::PathBuf,
    /// PEM file with this node's certificate (SAN `minga-node`).
    pub cert: std::path::PathBuf,
    /// PEM file with this node's private key.
    pub key: std::path::PathBuf,
}

impl RaftTlsConfig {
    fn load_cert_chain(
        path: &std::path::Path,
    ) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, std::io::Error> {
        rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(path)?))
            .collect::<Result<Vec<_>, _>>()
    }

    fn load_key(
        path: &std::path::Path,
    ) -> Result<rustls::pki_types::PrivateKeyDer<'static>, std::io::Error> {
        let mut rd = std::io::BufReader::new(std::fs::File::open(path)?);
        loop {
            match rustls_pemfile::read_one(&mut rd)? {
                Some(rustls_pemfile::Item::Pkcs1Key(k)) => return Ok(k.into()),
                Some(rustls_pemfile::Item::Pkcs8Key(k)) => return Ok(k.into()),
                Some(rustls_pemfile::Item::Sec1Key(k)) => return Ok(k.into()),
                Some(_) => continue,
                None => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("no private key in {}", path.display()),
                    ))
                }
            }
        }
    }

    /// Client side: verify the peer against the CA and the node SAN, and
    /// present our certificate (mTLS).
    pub fn client_config(&self) -> Result<std::sync::Arc<rustls::ClientConfig>, std::io::Error> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in Self::load_cert_chain(&self.ca_cert)? {
            roots
                .add(cert)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        }
        let certs = Self::load_cert_chain(&self.cert)?;
        let key = Self::load_key(&self.key)?;
        // Explicit provider: the workspace links rustls with both ring
        // and aws-lc-rs available, so auto-detection is ambiguous.
        let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        .with_root_certificates(roots)
        .with_client_auth_cert(certs, key)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(std::sync::Arc::new(config))
    }

    /// Server side: require every peer to present a certificate signed by
    /// the cluster CA. An uncredentialed peer fails the handshake — it
    /// cannot vote, replicate, or be forwarded writes.
    pub fn server_config(&self) -> Result<std::sync::Arc<rustls::ServerConfig>, std::io::Error> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in Self::load_cert_chain(&self.ca_cert)? {
            roots
                .add(cert)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        }
        let client_verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            std::sync::Arc::new(roots),
            std::sync::Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        let certs = Self::load_cert_chain(&self.cert)?;
        let key = Self::load_key(&self.key)?;
        let config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(std::sync::Arc::new(config))
    }
}

// --- wire protocol ------------------------------------------------------------

/// Maximum accepted frame size. Bounds the `vec![0u8; len]` allocation
/// that follows the length prefix: without a cap, any unauthenticated
/// peer that can open a TCP connection controls a 4 GiB allocation.
/// 64 MiB leaves room for snapshot chunks (openraft default: 3 MiB).
const MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;

/// How long a peer may take to finish the TLS handshake. Without a deadline an
/// uncredentialed peer that never completes it holds a socket and a task forever.
const HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// How long a peer may take to send one request frame (up to `MAX_FRAME_BYTES`).
const REQUEST_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
/// Concurrent inbound RPC connections; more wait in the kernel backlog.
const MAX_CONNECTIONS: usize = 512;

/// A Raft RPC sent over the wire.
#[derive(Debug, Serialize, Deserialize)]
enum RaftRequest {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<u64>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    /// A client write forwarded to the leader. Followers cannot accept
    /// writes; the API and node registration must work from any node.
    /// Boxed: `StateCommand` is much larger than the raft RPC variants.
    ClientWrite(Box<StateCommand>),
}

/// The reply to a [`RaftRequest`]. Errors are carried as strings; the
/// client wraps them in [`RPCError::Unreachable`].
#[derive(Debug, Serialize, Deserialize)]
enum RaftReply {
    AppendEntries(Result<AppendEntriesResponse<u64>, String>),
    Vote(Result<VoteResponse<u64>, String>),
    InstallSnapshot(Result<InstallSnapshotResponse<u64>, String>),
    ClientWrite(Result<StateResponse, String>),
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

/// A stream that is either plain TCP or TLS-over-TCP.
trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncReadWrite for T {}
type BoxStream = Box<dyn AsyncReadWrite>;

/// A network factory that creates TCP-based Raft network clients.
#[derive(Debug, Clone, Default)]
pub struct TcpNetworkFactory {
    tls: Option<std::sync::Arc<rustls::ClientConfig>>,
}

impl TcpNetworkFactory {
    /// Plaintext transport (development and tests).
    pub fn new_plain() -> Self {
        Self { tls: None }
    }

    /// mTLS transport: every connection is encrypted and authenticated.
    pub fn new_tls(config: std::sync::Arc<rustls::ClientConfig>) -> Self {
        Self { tls: Some(config) }
    }
}

impl RaftNetworkFactory<TypeConfig> for TcpNetworkFactory {
    type Network = TcpNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        TcpNetwork {
            target,
            addr: node.addr.clone(),
            tls: self.tls.clone(),
        }
    }
}

/// A TCP-based Raft network client. Connects to the target node on each
/// RPC (simple, no connection pooling — sufficient for Phase 0).
#[derive(Clone)]
pub struct TcpNetwork {
    target: u64,
    addr: String,
    tls: Option<std::sync::Arc<rustls::ClientConfig>>,
}

impl std::fmt::Debug for TcpNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpNetwork")
            .field("target", &self.target)
            .field("addr", &self.addr)
            .field("tls", &self.tls.is_some())
            .finish()
    }
}

impl TcpNetwork {
    /// Sends a request and reads the reply over a fresh TCP connection.
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, TransportError> {
        let encoded = crate::codec::encode_wire(&request)
            .map_err(|e| TransportError::Encode(e.to_string()))?;

        tracing::trace!(target = self.target, addr = %self.addr, "raft rpc send");
        let stream = TcpStream::connect(&self.addr).await?;
        let mut stream: BoxStream = match &self.tls {
            Some(config) => {
                let name = rustls::pki_types::ServerName::try_from(MINGA_NODE_NAME)
                    .map_err(|e| TransportError::Decode(e.to_string()))?
                    .to_owned();
                let connector = tokio_rustls::TlsConnector::from(config.clone());
                Box::new(connector.connect(name, stream).await?)
            }
            None => Box::new(stream),
        };

        // Write length-prefixed request.
        stream.write_u32(encoded.len() as u32).await?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;

        // Read length-prefixed response.
        let len = stream.read_u32().await?;
        if len > MAX_FRAME_BYTES {
            return Err(TransportError::Decode(format!(
                "frame length {len} exceeds {MAX_FRAME_BYTES} byte cap"
            )));
        }
        let mut buf = vec![0u8; len as usize];
        stream.read_exact(&mut buf).await?;

        let reply: RaftReply =
            crate::codec::decode_wire(&buf).map_err(|e| TransportError::Decode(e.to_string()))?;

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

/// Forwards a client write to the leader at `addr`. Returns the applied
/// [`StateResponse`]; an error means the write did not commit (the peer
/// is unreachable or no longer leader — retry to rediscover the leader).
pub(crate) async fn forward_write(
    addr: &str,
    cmd: &StateCommand,
    tls: Option<std::sync::Arc<rustls::ClientConfig>>,
) -> Result<StateResponse, StateError> {
    let network = TcpNetwork {
        target: 0,
        addr: addr.to_string(),
        tls,
    };
    let reply = network
        .send(RaftRequest::ClientWrite(Box::new(cmd.clone())))
        .await
        .map_err(|e| StateError::Raft(format!("forward to leader at {addr}: {e}")))?;
    match reply {
        RaftReply::ClientWrite(Ok(resp)) => Ok(resp),
        RaftReply::ClientWrite(Err(msg)) => Err(StateError::Raft(msg)),
        _ => Err(StateError::Raft(
            "wrong reply type for forwarded write".into(),
        )),
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
    tls: Option<std::sync::Arc<rustls::ServerConfig>>,
) -> Result<tokio::task::JoinHandle<()>, std::io::Error> {
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!(
        addr = bind_addr,
        tls = tls.is_some(),
        "raft rpc server listening"
    );

    let limit = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONNECTIONS));
    let handle = tokio::spawn(async move {
        loop {
            let permit = limit
                .clone()
                .acquire_owned()
                .await
                .expect("the connection semaphore is never closed");
            match listener.accept().await {
                Ok((stream, peer)) => {
                    let raft = raft.clone();
                    let tls = tls.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        // When TLS is configured, an uncredentialed peer
                        // fails the handshake here and never reaches Raft.
                        let stream: BoxStream = match tls {
                            Some(config) => {
                                let acceptor = tokio_rustls::TlsAcceptor::from(config);
                                match tokio::time::timeout(
                                    HANDSHAKE_TIMEOUT,
                                    acceptor.accept(stream),
                                )
                                .await
                                {
                                    Ok(Ok(s)) => Box::new(s),
                                    Ok(Err(e)) => {
                                        tracing::debug!(%peer, error = %e, "raft rpc TLS handshake rejected");
                                        return;
                                    }
                                    Err(_) => {
                                        tracing::debug!(%peer, "raft rpc TLS handshake timed out");
                                        return;
                                    }
                                }
                            }
                            None => Box::new(stream),
                        };
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
async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    raft: Raft<TypeConfig>,
) -> Result<(), TransportError> {
    // Read length-prefixed request, within a deadline.
    let buf = tokio::time::timeout(REQUEST_READ_TIMEOUT, async {
        let len = stream.read_u32().await?;
        if len > MAX_FRAME_BYTES {
            return Err(TransportError::Decode(format!(
                "frame length {len} exceeds {MAX_FRAME_BYTES} byte cap"
            )));
        }
        let mut buf = vec![0u8; len as usize];
        stream.read_exact(&mut buf).await?;
        Ok(buf)
    })
    .await
    .map_err(|_| {
        TransportError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "request read timed out",
        ))
    })??;

    let request: RaftRequest =
        crate::codec::decode_wire(&buf).map_err(|e| TransportError::Decode(e.to_string()))?;

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
        RaftRequest::ClientWrite(cmd) => match raft.client_write(*cmd).await {
            Ok(resp) => RaftReply::ClientWrite(Ok(resp.data)),
            Err(e) => RaftReply::ClientWrite(Err(e.to_string())),
        },
    };

    // Write length-prefixed response.
    let encoded =
        crate::codec::encode_wire(&reply).map_err(|e| TransportError::Encode(e.to_string()))?;
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
