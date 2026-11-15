mod bus;
mod deploy;
mod private;
mod sidecar;
mod state;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use minga_compose::{DeployMode, LoadOptions, Loaded};
use sidecar::{ClusterTls, NatsSidecar, ServerTls, SidecarOptions};

/// Minga: single-binary orchestrator for edge, on-prem and air-gapped sites.
#[derive(Parser)]
#[command(name = "minga", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run this node in the manager role (scheduler + supervisor).
    Manager(NodeArgs),
    /// Run this node in the worker role (supervisor only).
    Worker(NodeArgs),
    /// Check a Compose file and print what would be deployed.
    Validate(ComposeArgs),
    /// Deploy a Compose file as a stack.
    Deploy {
        #[command(flatten)]
        compose: ComposeArgs,
        /// Stack name.
        stack: String,
        /// URL of a `minga api` server. Can also be set via MINGA_API_URL.
        #[arg(long, env = "MINGA_API_URL", default_value = "http://127.0.0.1:2375")]
        api_url: String,
        /// Bearer token when the API requires one (MINGA_API_TOKEN).
        #[arg(long, env = "MINGA_API_TOKEN")]
        api_token: Option<String>,
        /// Seconds to wait for tasks to be placed before failing.
        #[arg(long, default_value = "30")]
        wait: u64,
    },
    /// Back up and restore desired state (services, networks, volumes).
    State {
        #[command(subcommand)]
        cmd: StateCommand,
    },
    /// Overlay network management.
    Net {
        #[command(subcommand)]
        cmd: NetCommand,
    },
    /// WireGuard key and config utilities.
    Wg {
        #[command(subcommand)]
        cmd: WgCommand,
    },
    /// Cluster PKI: issue the CA and node certificates for Raft mTLS.
    Tls {
        #[command(subcommand)]
        cmd: TlsCommand,
    },
    /// Node-local secret store.
    Secret {
        #[command(subcommand)]
        cmd: SecretCommand,
    },
    /// Run the Docker Engine API server (Swarm-compatible REST API).
    Api(ApiArgs),
    /// Dry-run or apply the v1.2.0 worker-voter migration.
    MigrateV1(MigrateV1Args),
    /// Agent sandbox management.
    Sandbox {
        #[command(subcommand)]
        cmd: SandboxCommand,
    },
    /// Run the LLM egress proxy server.
    Egress(EgressArgs),
    /// Agent workload management.
    Agent {
        #[command(subcommand)]
        cmd: AgentCommand,
    },
    /// Show logs for a task.
    Logs {
        /// Task ID (e.g. plant-7.web.1).
        task_id: String,
        /// Follow log output (like `tail -f`).
        #[arg(short = 'f', long)]
        follow: bool,
        /// Number of lines to show from the end (0 = all).
        #[arg(long, default_value = "100")]
        tail: usize,
        /// NATS server URL (e.g. nats://localhost:4222).
        #[arg(long, env = "MINGA_NATS_URL")]
        nats_url: Option<String>,
    },
    /// Container image management.
    Image {
        #[command(subcommand)]
        cmd: ImageCommand,
    },
}

#[derive(Subcommand)]
enum StateCommand {
    /// Export desired state as JSON. The file holds service environment
    /// values: keep it private.
    Export {
        /// Write to FILE (mode 0600) instead of stdout.
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Overwrite FILE if it exists.
        #[arg(long, requires = "output")]
        force: bool,
        #[command(flatten)]
        api: StateApiArgs,
    },
    /// Import a file written by `minga state export`.
    Import {
        file: PathBuf,
        /// Replace services that already exist instead of failing.
        #[arg(long)]
        overwrite: bool,
        #[command(flatten)]
        api: StateApiArgs,
    },
}

#[derive(Args)]
struct StateApiArgs {
    /// URL of a `minga api` server. Can also be set via MINGA_API_URL.
    #[arg(long, env = "MINGA_API_URL", default_value = "http://127.0.0.1:2375")]
    api_url: String,
    /// Bearer token when the API requires one (MINGA_API_TOKEN).
    #[arg(long, env = "MINGA_API_TOKEN")]
    api_token: Option<String>,
}

#[derive(Args)]
struct MigrateV1Args {
    /// Manager voter ID to retain (repeat for each manager).
    #[arg(long = "manager-id", required = true)]
    manager_ids: Vec<u64>,
    /// Manager API URL.
    #[arg(long, env = "MINGA_API_URL", default_value = "http://127.0.0.1:2375")]
    api_url: String,
    /// Admin bearer token (MINGA_API_TOKEN).
    #[arg(long, env = "MINGA_API_TOKEN")]
    api_token: Option<String>,
    /// Apply the membership change; without this flag, only print the plan.
    #[arg(long)]
    apply: bool,
}

impl From<StateApiArgs> for state::ApiOptions {
    fn from(a: StateApiArgs) -> Self {
        Self {
            api_url: a.api_url,
            api_token: a.api_token,
        }
    }
}

#[derive(Subcommand)]
enum AgentCommand {
    /// List running agent tasks (reads from the event bus KV).
    Ls {
        /// NATS server URL (e.g. nats://localhost:4222).
        #[arg(long, env = "MINGA_NATS_URL")]
        nats_url: Option<String>,
    },
}

#[derive(Subcommand)]
enum ImageCommand {
    /// Pull an image from a registry into the Minga containerd namespace.
    Pull {
        /// Image reference (e.g. ghcr.io/org/app:latest or nginx:1.27).
        /// Credentials come from the Docker config (`docker login`'s inline
        /// auths, `credsStore` or `credHelpers`); they go to containerd over
        /// its API, never on a command line.
        image_ref: String,
        /// Path to the containerd gRPC socket.
        #[arg(long, default_value = "/run/containerd/containerd.sock")]
        containerd_socket: String,
    },
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum NodeCertificateRole {
    Manager,
    Worker,
}

#[derive(Subcommand)]
enum TlsCommand {
    /// Create a cluster CA: writes ca.pem and ca-key.pem.
    ///
    /// The CA key is the cluster's root of trust — keep it offline.
    /// Refuses to overwrite existing files.
    InitCa {
        /// Directory to write ca.pem and ca-key.pem into.
        #[arg(long)]
        dir: String,
    },
    /// Issue a node certificate with the Raft SAN and a NATS user identity.
    IssueNode {
        /// Directory containing ca.pem and ca-key.pem.
        #[arg(long)]
        ca_dir: String,
        /// Directory to write node.pem and node-key.pem into.
        #[arg(long)]
        out_dir: String,
        /// Node ID bound to the certificate's NATS identity.
        #[arg(long)]
        node_id: u64,
        /// NATS role authorized by the certificate.
        #[arg(long, value_enum)]
        role: NodeCertificateRole,
    },
}

#[derive(Subcommand)]
enum NetCommand {
    /// Create a new overlay network.
    Create {
        /// Network name.
        name: String,
        /// Subnet CIDR (e.g. 10.100.0.0/24). If omitted, the first /24 of 10.100.0.0/16.
        #[arg(long)]
        subnet: Option<String>,
        /// WireGuard listen port (default 51820).
        #[arg(long)]
        wg_port: Option<u16>,
    },
    /// Show CIDR information for a subnet.
    Inspect {
        /// CIDR string (e.g. 10.100.0.0/24).
        cidr: String,
    },
}

#[derive(Subcommand)]
enum WgCommand {
    /// Generate a new WireGuard keypair.
    Keygen,
    /// Show the public key derived from a private key.
    Pubkey {
        /// Base64-encoded private key.
        private_key: String,
    },
    /// Generate a sample WireGuard interface config.
    Config {
        /// Private key (base64). If omitted, a new keypair is generated.
        #[arg(long)]
        private_key: Option<String>,
        /// Listen port (default 51820).
        #[arg(long, default_value = "51820")]
        listen_port: u16,
        /// Overlay IP with CIDR (e.g. 10.100.0.1/24).
        #[arg(long)]
        address: String,
        /// Peer as public_key=endpoint=allowed_ips (repeatable).
        #[arg(long = "peer")]
        peers: Vec<String>,
    },
}

#[derive(Args)]
struct NodeArgs {
    /// This node's Raft ID.
    #[arg(long)]
    node_id: u64,
    /// Address to bind the Raft RPC server (e.g. 0.0.0.0:50051).
    #[arg(long)]
    bind: Option<String>,
    /// Data directory for Raft state (raft_log.redb, state.redb).
    #[arg(long, default_value = "/var/lib/minga")]
    data_dir: String,
    /// Peer node as id=address (repeatable). Include all other nodes.
    #[arg(long = "peer")]
    peers: Vec<String>,
    /// Start as an uninitialized manager and wait to be added to a cluster.
    #[arg(long)]
    join: bool,
    /// PEM file with the cluster CA certificate. Enables mTLS on the Raft
    /// transport; all three raft-tls flags are required together.
    #[arg(long)]
    raft_tls_ca: Option<String>,
    /// PEM file with this node's Raft certificate (SAN `minga-node`).
    #[arg(long)]
    raft_tls_cert: Option<String>,
    /// PEM file with this node's Raft private key.
    #[arg(long)]
    raft_tls_key: Option<String>,
    /// Node label as key=value (repeatable). Advertised to the cluster and
    /// usable in placement constraints (e.g. node.labels.gpu == true).
    #[arg(long = "label")]
    labels: Vec<String>,
    /// Reconcile interval in seconds.
    #[arg(long, default_value = "2")]
    interval: u64,
    /// NATS server URL (e.g. nats://localhost:4222, or tls://… with the
    /// MINGA_NATS_TLS_* variables). If not set, a manager spawns its own
    /// sidecar; workers must set it. Can
    /// also be set via the MINGA_NATS_URL environment variable.
    #[arg(long, env = "MINGA_NATS_URL")]
    nats_url: Option<String>,
    /// NATS token for development token-authenticated connections. Set the
    /// same token on manager and worker; certificate mapping replaces tokens.
    #[arg(long, env = "MINGA_NATS_TOKEN")]
    nats_token: Option<String>,
    /// Client-facing TLS files for the built-in NATS sidecar. Set all three
    /// to require client certificates and map them to per-node permissions.
    #[arg(long)]
    nats_server_cert: Option<String>,
    #[arg(long)]
    nats_server_key: Option<String>,
    #[arg(long)]
    nats_client_ca: Option<String>,
    /// Address for the built-in sidecar listener (non-loopback requires TLS).
    #[arg(long)]
    nats_listen_addr: Option<String>,
    /// Worker node IDs authorized by this manager's built-in sidecar.
    #[arg(long = "nats-worker-id")]
    nats_worker_ids: Vec<u64>,
    /// Existing NATS cluster route to join (repeatable, `nats://host:6222`).
    #[arg(long = "nats-cluster-route")]
    nats_cluster_routes: Vec<String>,
    /// Route TLS certificate, private key, and peer CA (all required together).
    #[arg(long)]
    nats_cluster_cert: Option<String>,
    #[arg(long)]
    nats_cluster_key: Option<String>,
    #[arg(long)]
    nats_cluster_ca: Option<String>,
    /// NATS cluster route listener port (requires route TLS).
    #[arg(long)]
    nats_cluster_port: Option<u16>,
    /// Copies kept of event, status, lease, and assignment streams/buckets.
    /// Set 3 (or 5) on a NATS cluster of at least that many servers so
    /// losing one server loses no bus state; logs stay at one copy. Only
    /// raises: a node left at 1 never shrinks what another node raised.
    /// Needs an external or sidecar NATS cluster.
    #[arg(long, env = "MINGA_NATS_REPLICAS", value_parser = clap::value_parser!(u8).range(1..=5))]
    nats_replicas: Option<u8>,
    /// Directory holding the node-local secret store. Secrets are
    /// provisioned there by the operator; values never travel through
    /// Raft or the event bus.
    #[arg(long, default_value = "/var/lib/minga/secrets")]
    secret_store: String,
    /// Container runtime: `mock` (in-memory, for testing) or `containerd`
    /// (real containers via containerd's gRPC API).
    #[arg(long, default_value = "mock")]
    runtime: String,
    /// Path to the containerd gRPC socket (used with --runtime containerd).
    #[arg(long, default_value = "/run/containerd/containerd.sock")]
    containerd_socket: String,
    /// Agent sandbox runtime: `mock` (in-memory, for testing) or `wasmtime`
    /// (real WASM execution). Set to enable agent workload support.
    #[arg(long)]
    sandbox_runtime: Option<String>,
    /// Base directory for resolving agent WASM module paths.
    #[arg(long, default_value = "/var/lib/minga/modules")]
    module_dir: String,
    /// Bind address for the LLM egress proxy (e.g. 127.0.0.1:18080).
    /// If set, the node starts an egress proxy for agent workloads.
    #[arg(long)]
    egress_bind: Option<String>,
    /// Upstream LLM API URL for the egress proxy.
    #[arg(long, default_value = "https://api.openai.com")]
    egress_upstream: String,
    /// Secret ID in --secret-store holding the upstream API key. The egress
    /// proxy injects it; agents never hold it.
    #[arg(long)]
    egress_credential: Option<String>,
    /// Header the egress proxy sends the key in.
    #[arg(long, default_value = "authorization", value_parser = ["authorization", "x-api-key"])]
    egress_credential_header: String,
    /// Bind address for `/metrics`, `/healthz` and `/readyz`
    /// (e.g. 0.0.0.0:9100). Not served when unset.
    #[arg(long, env = "MINGA_METRICS_BIND")]
    metrics_bind: Option<String>,
    /// Range each network's /24 is allocated from (by the Raft leader).
    /// Refused when it overlaps one of this host's routes.
    #[arg(long, env = "MINGA_OVERLAY_SUPERNET", default_value = minga_net::DEFAULT_SUPERNET)]
    overlay_supernet: String,
}

#[derive(Args)]
struct ComposeArgs {
    /// Path to the Compose file.
    #[arg(short = 'c', long, default_value = "compose.yaml")]
    compose_file: String,
    /// Profile to enable; repeat for several, or use `*` for all.
    #[arg(short, long = "profile")]
    profiles: Vec<String>,
}

#[derive(Args)]
struct ApiArgs {
    /// This node's Raft ID.
    #[arg(long)]
    node_id: u64,
    /// Address to bind the Raft RPC server (e.g. 0.0.0.0:50051).
    #[arg(long)]
    bind: String,
    /// Data directory for Raft state (raft_log.redb, state.redb).
    #[arg(long, default_value = "/var/lib/minga")]
    data_dir: String,
    /// Peer node as id=address (repeatable). Include all other nodes.
    #[arg(long = "peer")]
    peers: Vec<String>,
    /// PEM file with the cluster CA certificate. Enables mTLS on the Raft
    /// transport; all three raft-tls flags are required together.
    #[arg(long)]
    raft_tls_ca: Option<String>,
    /// PEM file with this node's Raft certificate (SAN `minga-node`).
    #[arg(long)]
    raft_tls_cert: Option<String>,
    /// PEM file with this node's Raft private key.
    #[arg(long)]
    raft_tls_key: Option<String>,
    /// NATS server URL for lease-aware placement (defaults to the local manager sidecar).
    #[arg(long, env = "MINGA_NATS_URL", default_value = "nats://127.0.0.1:4222")]
    nats_url: String,
    /// NATS client token, when the server requires one.
    #[arg(long, env = "MINGA_NATS_TOKEN")]
    nats_token: Option<String>,
    /// Address to bind the Docker Engine API server (e.g. 0.0.0.0:2375).
    #[arg(long, default_value = "0.0.0.0:2375")]
    api_bind: String,
    /// Token required on every API request, as `Authorization: Bearer` or
    /// as the Basic-auth password (for Traefik's Swarm provider). Strongly
    /// recommended when binding anything other than localhost — without it,
    /// the API is unauthenticated remote code execution by design.
    #[arg(long, env = "MINGA_API_TOKEN")]
    api_token: Option<String>,
    /// Range each network's /24 is allocated from (by the Raft leader).
    /// Refused when it overlaps one of this host's routes.
    #[arg(long, env = "MINGA_OVERLAY_SUPERNET", default_value = minga_net::DEFAULT_SUPERNET)]
    overlay_supernet: String,
}

#[derive(Subcommand)]
enum SandboxCommand {
    /// Run a WASM module in a sandbox (one-shot).
    Run {
        /// Path to the .wasm or .wat file.
        #[arg(long)]
        module: String,
        /// Input data (string). Use @file to read from a file.
        #[arg(long, default_value = "")]
        input: String,
        /// Fuel limit (instruction budget).
        #[arg(long, default_value = "1000000000")]
        fuel: u64,
        /// Timeout in seconds.
        #[arg(long, default_value = "30")]
        timeout: u64,
    },
    /// Show available sandbox runtimes.
    Info,
}

#[derive(Args)]
struct EgressArgs {
    /// Address to bind the egress proxy (e.g. 0.0.0.0:8080).
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: String,
    /// Upstream LLM API base URL.
    #[arg(long, default_value = "https://api.openai.com")]
    upstream: String,
    /// Default token budget per workload.
    #[arg(long, default_value = "1000000")]
    default_budget: u64,
    /// Budget window in seconds.
    #[arg(long, default_value = "3600")]
    budget_window: u64,
    /// Disable response caching.
    #[arg(long)]
    no_cache: bool,
    /// Directory holding the node-local secret store.
    #[arg(long, default_value = "/var/lib/minga/secrets")]
    secret_store: String,
    /// Secret ID in --secret-store holding the upstream API key, injected
    /// into every upstream request.
    #[arg(long)]
    credential: Option<String>,
    /// Header the key is sent in.
    #[arg(long, default_value = "authorization", value_parser = ["authorization", "x-api-key"])]
    credential_header: String,
}

/// Reads the egress proxy's upstream key from the node secret store.
fn load_upstream_auth(
    secret_store: &str,
    secret_id: Option<&str>,
    header: &str,
) -> Result<Option<minga_egress::UpstreamAuth>, String> {
    let Some(id) = secret_id else {
        return Ok(None);
    };
    let key = minga_node::secrets::SecretStore::new(secret_store)
        .read_value(id)
        .map_err(|e| e.to_string())?;
    if key.is_empty() {
        return Err(format!("secret `{id}` is empty"));
    }
    Ok(Some(match header {
        "authorization" => minga_egress::UpstreamAuth::Bearer(key),
        _ => minga_egress::UpstreamAuth::ApiKey(key),
    }))
}

/// Parses `--peer id=address` into a BTreeMap.
fn parse_peers(raw: &[String]) -> Result<BTreeMap<u64, String>, String> {
    let mut peers = BTreeMap::new();
    for p in raw {
        let (id, addr) = p
            .split_once('=')
            .ok_or_else(|| format!("invalid peer {p:?}, expected id=address"))?;
        let id: u64 = id
            .parse()
            .map_err(|_| format!("invalid peer id {id:?}, expected a number"))?;
        peers.insert(id, addr.to_string());
    }
    Ok(peers)
}

/// Opens the cluster state store, with mTLS when the three raft-tls
/// flags are present. Partial TLS flags are a hard error.
#[allow(clippy::too_many_arguments)]
async fn open_store(
    node_id: u64,
    bind: &str,
    data_dir: &str,
    peers: BTreeMap<u64, String>,
    ca: &Option<String>,
    cert: &Option<String>,
    key: &Option<String>,
    join: bool,
) -> Result<Arc<minga_state::StateStore>, String> {
    let store = match (ca, cert, key) {
        (Some(ca), Some(cert), Some(key)) => {
            let tls = minga_state::network::RaftTlsConfig {
                ca_cert: ca.into(),
                cert: cert.into(),
                key: key.into(),
            };
            if join {
                minga_state::StateStore::open_join_secure(node_id, bind, data_dir, tls).await
            } else {
                minga_state::StateStore::open_secure(node_id, bind, data_dir, peers, tls).await
            }
        }
        (None, None, None) => {
            if !is_loopback_bind(bind) {
                tracing::warn!(
                    bind,
                    "Raft is listening in plaintext on a non-loopback address: any peer that can \
                     reach it can write cluster state, API token or not. Set --raft-tls-ca, \
                     --raft-tls-cert and --raft-tls-key (`minga tls init-ca` / `issue-node`)"
                );
            }
            if join {
                minga_state::StateStore::open_join(node_id, bind, data_dir).await
            } else {
                minga_state::StateStore::open(node_id, bind, data_dir, peers).await
            }
        }
        _ => {
            return Err(
                "--raft-tls-ca, --raft-tls-cert and --raft-tls-key must be given together".into(),
            );
        }
    };
    store
        .map(Arc::new)
        .map_err(|e| format!("cannot open state store: {e}"))
}

/// Parses `--label key=value` into a map. Values may contain `=`.
fn parse_labels(raw: &[String]) -> Result<std::collections::HashMap<String, String>, String> {
    let mut labels = std::collections::HashMap::new();
    for l in raw {
        let (key, value) = l
            .split_once('=')
            .ok_or_else(|| format!("invalid label {l:?}, expected key=value"))?;
        if key.is_empty() {
            return Err(format!("invalid label {l:?}, key must not be empty"));
        }
        labels.insert(key.to_string(), value.to_string());
    }
    Ok(labels)
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    // Long-running roles log JSON for collectors; one-shot commands stay
    // human-readable.
    if matches!(
        cli.command,
        Command::Manager(_) | Command::Worker(_) | Command::Api(_)
    ) {
        minga_telemetry::init_logging();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }

    match cli.command {
        Command::Manager(args) => return run_node(args, true).await,
        Command::Worker(args) => return run_node(args, false).await,
        Command::Validate(args) => {
            let Some(loaded) = load(&args) else {
                return ExitCode::FAILURE;
            };
            print_plan(&loaded);
        }
        Command::Deploy {
            compose,
            stack,
            api_url,
            api_token,
            wait,
        } => {
            let Some(loaded) = load(&compose) else {
                return ExitCode::FAILURE;
            };
            let project_dir = project_dir(&compose.compose_file);
            let translated = match minga_scheduler::translate_project(
                &loaded.project,
                &stack,
                project_dir.as_deref(),
            ) {
                Ok(t) => t,
                Err(e) => {
                    eprintln!("error: {e}");
                    return ExitCode::FAILURE;
                }
            };
            print_translated(&translated);
            let opts = deploy::DeployOptions {
                api_url,
                api_token,
                stack,
                wait: Duration::from_secs(wait),
            };
            if let Err(e) = deploy::deploy_stack(&translated, &opts).await {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        }
        Command::State { cmd } => {
            let result = match cmd {
                StateCommand::Export { output, force, api } => {
                    state::export(&api.into(), output.as_deref(), force).await
                }
                StateCommand::Import {
                    file,
                    overwrite,
                    api,
                } => state::import(&api.into(), &file, overwrite).await,
            };
            if let Err(e) = result {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        }
        Command::Net { cmd } => return run_net(cmd),
        Command::Wg { cmd } => return run_wg(cmd),
        Command::Tls { cmd } => return run_tls(cmd),
        Command::Secret { cmd } => return run_secret(cmd),
        Command::Api(args) => return run_api(args).await,
        Command::Sandbox { cmd } => return run_sandbox(cmd).await,
        Command::Egress(args) => return run_egress(args).await,
        Command::MigrateV1(args) => return run_migrate_v1(args).await,
        Command::Agent { cmd } => return run_agent(cmd).await,
        Command::Logs {
            task_id,
            follow,
            tail,
            nats_url,
        } => {
            return run_logs(&task_id, follow, tail, nats_url.as_deref()).await;
        }
        Command::Image { cmd } => return run_image(cmd).await,
    }
    ExitCode::SUCCESS
}

/// Parses `--overlay-supernet`, refusing one that overlaps this host's
/// routes. Off Linux there is no `/proc/net/route` and nothing to check.
fn overlay_supernet(raw: &str) -> Result<minga_net::Cidr, String> {
    let cidr = minga_net::Cidr::parse(raw).map_err(|e| format!("--overlay-supernet: {e}"))?;
    let routes = std::fs::read_to_string("/proc/net/route").unwrap_or_default();
    minga_net::cidr::check_supernet(&cidr, &routes)?;
    Ok(cidr)
}

async fn run_migrate_v1(args: MigrateV1Args) -> ExitCode {
    let selected = args
        .manager_ids
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if selected.len() != args.manager_ids.len() {
        eprintln!("error: --manager-id values must be unique");
        return ExitCode::FAILURE;
    }

    let url = format!("{}/minga/managers", args.api_url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let mut request = client.get(&url);
    if let Some(token) = &args.api_token {
        request = request.bearer_auth(token);
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            eprintln!("error: cannot list manager voters at {url}: {error}");
            return ExitCode::FAILURE;
        }
    };
    if !response.status().is_success() {
        let status = response.status();
        let detail = response.text().await.unwrap_or_default();
        eprintln!("error: manager voter query failed ({status}): {detail}");
        return ExitCode::FAILURE;
    }
    let current = match response.json::<Vec<u64>>().await {
        Ok(voters) => voters
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        Err(error) => {
            eprintln!("error: invalid manager voter response: {error}");
            return ExitCode::FAILURE;
        }
    };
    if selected.is_empty() || !selected.is_subset(&current) {
        eprintln!("error: --manager-id must name one or more current voters");
        return ExitCode::FAILURE;
    }
    let removed = current.difference(&selected).copied().collect::<Vec<_>>();
    println!("Current voters: {current:?}");
    println!("Managers to retain: {selected:?}");
    println!("Voters to remove: {removed:?}");
    if !args.apply {
        println!("Dry run only; pass --apply to commit this membership change.");
        return ExitCode::SUCCESS;
    }

    let mut request = client
        .post(format!("{url}/migrate-v1"))
        .json(&serde_json::json!({"manager_ids": args.manager_ids}));
    if let Some(token) = &args.api_token {
        request = request.bearer_auth(token);
    }
    match request.send().await {
        Ok(response) if response.status().is_success() => {
            println!("Manager voter migration committed.");
            ExitCode::SUCCESS
        }
        Ok(response) => {
            let status = response.status();
            let detail = response.text().await.unwrap_or_default();
            eprintln!("error: manager voter migration failed ({status}): {detail}");
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("error: manager voter migration request failed: {error}");
            ExitCode::FAILURE
        }
    }
}

/// Runs the Docker Engine API server: opens the state store and starts
/// the HTTP API server. The API server can run on any node — reads are
/// served locally, writes are proposed to Raft (forwarded to the leader).
async fn run_api(args: ApiArgs) -> ExitCode {
    let supernet = match overlay_supernet(&args.overlay_supernet) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let peers = match parse_peers(&args.peers) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };

    tracing::info!(
        node_id = args.node_id,
        bind = %args.bind,
        api_bind = %args.api_bind,
        data_dir = %args.data_dir,
        "starting Docker Engine API server"
    );

    let state = match open_store(
        args.node_id,
        &args.bind,
        &args.data_dir,
        peers,
        &args.raft_tls_ca,
        &args.raft_tls_cert,
        &args.raft_tls_key,
        false,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let bus = match bus::connect_bus(&args.nats_url, args.nats_token.as_deref(), 1).await {
        Ok(bus) => bus,
        Err(error) => {
            eprintln!(
                "error: cannot connect to NATS at {}: {error}",
                args.nats_url
            );
            return ExitCode::FAILURE;
        }
    };

    if args.api_token.is_none() && !is_loopback_bind(&args.api_bind) {
        tracing::warn!(
            api_bind = %args.api_bind,
            "the Docker API has no token and listens beyond loopback: anyone who can reach it \
             is cluster admin. Set --api-token or MINGA_API_TOKEN"
        );
    }
    let server = match minga_api::ApiServer::start_with_bus_and_auth(
        state.clone(),
        bus.clone(),
        &args.api_bind,
        args.api_token,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot start API server: {e}");
            return ExitCode::FAILURE;
        }
    };

    // The reconcile loop must exist on every Raft voter: the scheduler is
    // leadership-gated, and an API-only node can be elected leader. Without
    // this, a cluster whose leader is an API node places no tasks.
    let scheduler = minga_scheduler::Scheduler::with_bus(state, bus).with_supernet(supernet);
    let reconcile = scheduler.run(Duration::from_secs(2));
    tokio::pin!(reconcile);

    println!("Docker Engine API listening on http://{}", server.addr());
    println!("Try: curl http://{}/_ping", server.addr());
    println!("     DOCKER_HOST=tcp://{} docker service ls", server.addr());

    // Wait forever (until SIGINT).
    tokio::select! {
        _ = &mut reconcile => {},
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutting down API server");
        },
    }
    server.shutdown();

    ExitCode::SUCCESS
}

/// Cluster PKI operations: CA creation and node certificate issuance.
#[derive(Subcommand)]
enum SecretCommand {
    /// Provision or rotate a secret on this node. The value is read from
    /// stdin so it never reaches argv or shell history.
    Put {
        /// Secret ID: `<stack>_<compose secret key>`.
        id: String,
        /// Directory holding the node-local secret store.
        #[arg(long, default_value = "/var/lib/minga/secrets")]
        secret_store: String,
    },
}

fn run_secret(cmd: SecretCommand) -> ExitCode {
    use std::io::Read;

    let SecretCommand::Put { id, secret_store } = cmd;
    let mut value = Vec::new();
    if let Err(e) = std::io::stdin().read_to_end(&mut value) {
        eprintln!("error: cannot read the secret value from stdin: {e}");
        return ExitCode::FAILURE;
    }
    if value.is_empty() {
        eprintln!("error: no secret value on stdin");
        return ExitCode::FAILURE;
    }
    match minga_node::secrets::SecretStore::new(&secret_store).put(&id, &value) {
        Ok(_) => {
            println!("secret `{id}` provisioned in {secret_store}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: cannot write secret `{id}`: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run_tls(cmd: TlsCommand) -> ExitCode {
    match cmd {
        TlsCommand::InitCa { dir } => {
            let dir = PathBuf::from(&dir);
            let ca_pem = dir.join("ca.pem");
            let ca_key_pem = dir.join("ca-key.pem");
            for path in [&ca_pem, &ca_key_pem] {
                if path.exists() {
                    eprintln!(
                        "error: {} already exists; refusing to overwrite",
                        path.display()
                    );
                    return ExitCode::FAILURE;
                }
            }
            let key = match rcgen::KeyPair::generate() {
                Ok(k) => k,
                Err(e) => {
                    eprintln!("error: key generation failed: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let mut params =
                rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
            params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let ca = match params.self_signed(&key) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("error: CA self-signing failed: {e}");
                    return ExitCode::FAILURE;
                }
            };
            if let Err(e) = private::create_dir(&dir)
                .and_then(|()| std::fs::write(&ca_pem, ca.pem()))
                .and_then(|()| private::write_file(&ca_key_pem, key.serialize_pem().as_bytes()))
            {
                eprintln!("error: writing CA material: {e}");
                return ExitCode::FAILURE;
            }
            println!("cluster CA written to {}", dir.display());
            println!(
                "  keep {} offline; it signs every node",
                ca_key_pem.display()
            );
        }
        TlsCommand::IssueNode {
            ca_dir,
            out_dir,
            node_id,
            role,
        } => {
            let ca_dir = PathBuf::from(&ca_dir);
            let out_dir = PathBuf::from(&out_dir);
            let read = |name: &str| -> Result<Vec<u8>, String> {
                std::fs::read(ca_dir.join(name))
                    .map_err(|e| format!("cannot read {name} from {}: {e}", ca_dir.display()))
            };
            let (ca_pem_raw, ca_key_raw) = match (read("ca.pem"), read("ca-key.pem")) {
                (Ok(a), Ok(b)) => (a, b),
                (Err(e), _) | (_, Err(e)) => {
                    eprintln!("error: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let ca_key =
                match rcgen::KeyPair::from_pem(std::str::from_utf8(&ca_key_raw).unwrap_or("")) {
                    Ok(k) => k,
                    Err(e) => {
                        eprintln!("error: parsing ca-key.pem: {e}");
                        return ExitCode::FAILURE;
                    }
                };
            let mut ca_params = match rcgen::CertificateParams::from_ca_cert_pem(
                std::str::from_utf8(&ca_pem_raw).unwrap_or(""),
            ) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("error: parsing ca.pem: {e}");
                    return ExitCode::FAILURE;
                }
            };
            ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            let ca = match ca_params.self_signed(&ca_key) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("error: re-creating CA: {e}");
                    return ExitCode::FAILURE;
                }
            };

            let key = match rcgen::KeyPair::generate() {
                Ok(k) => k,
                Err(e) => {
                    eprintln!("error: key generation failed: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let nats_identity = match role {
                NodeCertificateRole::Manager => format!("minga-manager-{node_id}"),
                NodeCertificateRole::Worker => format!("minga-worker-{node_id}"),
            };
            let params = rcgen::CertificateParams::new(vec![
                minga_state::network::MINGA_NODE_NAME.to_string(),
                nats_identity,
            ])
            .expect("node params");
            let cert = match params.signed_by(&key, &ca, &ca_key) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("error: signing node certificate: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let node_pem = out_dir.join("node.pem");
            let node_key_pem = out_dir.join("node-key.pem");
            if let Err(e) = private::create_dir(&out_dir)
                .and_then(|()| std::fs::write(&node_pem, cert.pem()))
                .and_then(|()| private::write_file(&node_key_pem, key.serialize_pem().as_bytes()))
            {
                eprintln!("error: writing node material: {e}");
                return ExitCode::FAILURE;
            }
            println!("node certificate written to {}", out_dir.display());
            println!("  start the node with:");
            println!(
                "    --raft-tls-ca {} --raft-tls-cert {} --raft-tls-key {}",
                ca_dir.join("ca.pem").display(),
                node_pem.display(),
                node_key_pem.display()
            );
        }
    }
    ExitCode::SUCCESS
}

/// Returns true when a worker was given a Raft-only flag.
fn worker_has_manager_only_flags(args: &NodeArgs) -> bool {
    args.bind.is_some()
        || !args.peers.is_empty()
        || args.raft_tls_ca.is_some()
        || args.raft_tls_cert.is_some()
        || args.raft_tls_key.is_some()
        || args.nats_replicas.is_some()
        || args.join
        || args.nats_cluster_port.is_some()
}

/// Runs a node (manager or worker): managers open Raft state, while workers
/// run only the supervisor against their bus assignments.
async fn run_node(args: NodeArgs, is_manager: bool) -> ExitCode {
    if is_manager && args.bind.is_none() {
        eprintln!("error: managers require --bind for the Raft listener");
        return ExitCode::FAILURE;
    }
    if !is_manager && worker_has_manager_only_flags(&args) {
        eprintln!("error: Raft bind, peer, TLS, and replica flags are manager-only");
        return ExitCode::FAILURE;
    }
    if is_manager && args.join && !args.peers.is_empty() {
        eprintln!("error: --join cannot be combined with --peer; add this manager through the API");
        return ExitCode::FAILURE;
    }

    let cluster_tls_parts = [
        args.nats_cluster_cert.is_some(),
        args.nats_cluster_key.is_some(),
        args.nats_cluster_ca.is_some(),
    ];
    let cluster_tls_count = cluster_tls_parts.iter().filter(|set| **set).count();
    if cluster_tls_count != 0 && cluster_tls_count != cluster_tls_parts.len() {
        eprintln!(
            "error: --nats-cluster-cert, --nats-cluster-key, and --nats-cluster-ca must be set together"
        );
        return ExitCode::FAILURE;
    }
    if !args.nats_cluster_routes.is_empty() && cluster_tls_count != cluster_tls_parts.len() {
        eprintln!("error: --nats-cluster-route requires route TLS");
        return ExitCode::FAILURE;
    }
    let cluster = (cluster_tls_count == cluster_tls_parts.len()).then(|| ClusterTls {
        name: "minga".into(),
        cert_file: PathBuf::from(args.nats_cluster_cert.as_deref().unwrap()),
        key_file: PathBuf::from(args.nats_cluster_key.as_deref().unwrap()),
        ca_file: PathBuf::from(args.nats_cluster_ca.as_deref().unwrap()),
        port: args.nats_cluster_port.unwrap_or(6222),
        routes: args.nats_cluster_routes.clone(),
    });
    if args.nats_cluster_port.is_some() && cluster.is_none() {
        eprintln!("error: --nats-cluster-port requires route TLS");
        return ExitCode::FAILURE;
    }
    let sidecar_configured = args.nats_server_cert.is_some()
        || args.nats_server_key.is_some()
        || args.nats_client_ca.is_some()
        || args.nats_listen_addr.is_some()
        || !args.nats_worker_ids.is_empty()
        || cluster.is_some()
        || args.nats_cluster_port.is_some();
    let tls_parts = [
        args.nats_server_cert.is_some(),
        args.nats_server_key.is_some(),
        args.nats_client_ca.is_some(),
    ];
    let tls_count = tls_parts.into_iter().filter(|set| *set).count();
    if tls_count != 0 && tls_count != tls_parts.len() {
        eprintln!("error: --nats-server-cert, --nats-server-key, and --nats-client-ca must be set together");
        return ExitCode::FAILURE;
    }
    if !args.nats_worker_ids.is_empty() && tls_count != tls_parts.len() {
        eprintln!("error: --nats-worker-id requires client TLS and certificate identity mapping");
        return ExitCode::FAILURE;
    }
    if sidecar_configured && (!is_manager || args.nats_url.is_some()) {
        eprintln!("error: built-in sidecar TLS, listener, identity, and route flags require a manager without --nats-url");
        return ExitCode::FAILURE;
    }
    let supernet = match overlay_supernet(&args.overlay_supernet) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let peers = if is_manager {
        match parse_peers(&args.peers) {
            Ok(peers) => peers,
            Err(error) => {
                eprintln!("error: {error}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        std::collections::BTreeMap::new()
    };
    let manager_node_ids: Vec<_> = peers.keys().copied().collect();

    tracing::info!(
        node_id = args.node_id,
        bind = ?args.bind,
        data_dir = %args.data_dir,
        peers = ?peers,
        role = if is_manager { "manager" } else { "worker" },
        "starting minga node"
    );

    let state = if is_manager {
        let bind = args.bind.as_deref().expect("manager bind was validated");
        match open_store(
            args.node_id,
            bind,
            &args.data_dir,
            peers,
            &args.raft_tls_ca,
            &args.raft_tls_cert,
            &args.raft_tls_key,
            args.join,
        )
        .await
        {
            Ok(store) => Some(store),
            Err(error) => {
                eprintln!("error: {error}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };

    // Announce this node to the cluster and keep the membership lease
    // renewed. Without this the scheduler cannot see the node and every
    // deploy would place zero tasks.
    let labels = match parse_labels(&args.labels) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let identity = minga_node::NodeIdentity {
        node_id: args.node_id,
        address: args.bind.clone().unwrap_or_default(),
        labels,
        capacity: minga_node::host_capacity(),
        role: if is_manager {
            minga_state::types::NodeRole::Manager
        } else {
            minga_state::types::NodeRole::Worker
        },
    };

    // Create the event bus.
    // - If --nats-url is set: connect to external NATS.
    // - If manager and no --nats-url: spawn nats-server sidecar.
    // - If worker and no --nats-url: error (workers need the manager's NATS URL).
    let _sidecar: Option<NatsSidecar>;
    let bus: Arc<dyn minga_bus::EventBus> = match &args.nats_url {
        Some(url) => {
            _sidecar = None;
            let connection = if is_manager {
                bus::connect_bus(
                    url,
                    args.nats_token.as_deref(),
                    args.nats_replicas.unwrap_or(1).into(),
                )
                .await
            } else {
                bus::connect_worker_bus(url, args.nats_token.as_deref(), args.node_id).await
            };
            match connection {
                Ok(bus) => bus,
                Err(error) => {
                    eprintln!("error: cannot connect to NATS at {url}: {error}");
                    return ExitCode::FAILURE;
                }
            }
        }
        None => {
            if !is_manager {
                eprintln!(
                    "error: workers must specify --nats-url (or MINGA_NATS_URL) \
                     to connect to the manager's NATS server"
                );
                return ExitCode::FAILURE;
            }
            if args.nats_replicas.unwrap_or(1) > 1 && cluster.is_none() {
                eprintln!(
                    "error: --nats-replicas {} needs an external cluster or route-TLS sidecars",
                    args.nats_replicas.unwrap_or(1)
                );
                return ExitCode::FAILURE;
            }
            // Manager: spawn the NATS sidecar.
            let tls = args.nats_server_cert.as_ref().map(|cert| ServerTls {
                cert_file: PathBuf::from(cert),
                key_file: PathBuf::from(
                    args.nats_server_key
                        .as_deref()
                        .expect("TLS fields validated"),
                ),
                ca_file: PathBuf::from(
                    args.nats_client_ca
                        .as_deref()
                        .expect("TLS fields validated"),
                ),
                verify: true,
                verify_and_map: true,
            });
            let opts = SidecarOptions {
                data_dir: PathBuf::from(&args.data_dir),
                node_id: args.node_id,
                manager_node_ids,
                worker_node_ids: args.nats_worker_ids.clone(),
                client_token: args.nats_token.clone(),
                listen_addr: args
                    .nats_listen_addr
                    .clone()
                    .unwrap_or_else(|| "127.0.0.1".to_string()),
                tls,
                cluster: cluster.clone(),
                ..SidecarOptions::default()
            };
            let sidecar = match NatsSidecar::start(&opts).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error: cannot start NATS sidecar: {e}");
                    return ExitCode::FAILURE;
                }
            };
            let url = sidecar.client_url().to_string();
            tracing::info!(url = %url, "NATS sidecar ready");
            let replicas = args.nats_replicas.unwrap_or(1).into();
            let connect = if cluster.is_some() && replicas > 1 {
                bus::connect_bus_with_replica_retry(&url, args.nats_token.as_deref(), replicas)
                    .await
            } else {
                bus::connect_bus(
                    &url,
                    args.nats_token.as_deref(),
                    if cluster.is_some() { 1 } else { replicas },
                )
                .await
            };
            match connect {
                Ok(b) => {
                    _sidecar = Some(sidecar);
                    b
                }
                Err(e) => {
                    eprintln!("error: cannot connect to sidecar NATS at {url}: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
    };
    // The node announces itself only after its bus connection is ready.
    let _membership = match minga_node::NodeMembership::start(bus.clone(), identity).await {
        Ok(membership) => membership,
        Err(error) => {
            eprintln!("error: cannot publish node lease: {error}");
            return ExitCode::FAILURE;
        }
    };

    let ready = minga_telemetry::ReadyState::new();
    if let Some(state) = state.clone() {
        let (bus, ready, node_id) = (bus.clone(), ready.clone(), args.node_id);
        tokio::spawn(async move {
            loop {
                ready.set_raft_ready(state.current_leader().await.is_some());
                ready.set_bus_connected(bus.is_alive(node_id).await.is_ok());
                minga_telemetry::METRICS
                    .raft_leader
                    .set(if state.is_leader().await { 1.0 } else { 0.0 });
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    } else {
        let (bus, ready, node_id) = (bus.clone(), ready.clone(), args.node_id);
        tokio::spawn(async move {
            ready.set_raft_ready(true);
            loop {
                ready.set_bus_connected(bus.is_alive(node_id).await.is_ok());
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
    if let Some(bind) = &args.metrics_bind {
        match minga_telemetry::serve(bind, ready).await {
            Ok(addr) => tracing::info!(%addr, "metrics and health endpoints listening"),
            Err(e) => {
                eprintln!("error: cannot bind metrics endpoint {bind}: {e}");
                return ExitCode::FAILURE;
            }
        }
    }

    // Node networking (Linux + real containers only): netns/bridge setup
    // for tasks and DNS service discovery. Mock workloads never join
    // overlay networks, so they need neither.
    #[cfg(target_os = "linux")]
    let networking: Option<Arc<minga_node::NetworkSetup>> = if args.runtime == "containerd" {
        // A task may not publish a port this process serves: its DNAT rule
        // would capture the node's own clients and their tokens.
        let mut reserved: Vec<(u16, String)> = [
            (args.bind.as_deref(), "Raft"),
            (args.metrics_bind.as_deref(), "metrics"),
            (args.egress_bind.as_deref(), "egress proxy"),
        ]
        .into_iter()
        .filter_map(|(addr, by)| Some((port_of(addr?)?, by.to_string())))
        .collect();
        if is_manager && args.nats_url.is_none() {
            let sidecar = sidecar::SidecarOptions::default();
            reserved.push((sidecar.client_port, "NATS sidecar".into()));
            reserved.push((sidecar.monitor_port, "NATS sidecar monitor".into()));
            if let Some(cluster) = &cluster {
                reserved.push((cluster.port, "NATS cluster route".into()));
            }
        }
        Some(Arc::new(
            minga_node::NetworkSetup::new(format!("{}/net", args.data_dir))
                .with_reserved_tcp_ports(reserved),
        ))
    } else {
        None
    };

    // Construct the container runtime based on --runtime flag.
    let runtime: Arc<dyn minga_node::Runtime> = match args.runtime.as_str() {
        "mock" => {
            tracing::info!("using MockRuntime (in-memory, no real containers)");
            Arc::new(minga_node::MockRuntime::new())
        }
        "containerd" => {
            tracing::info!(socket = %args.containerd_socket, "connecting to containerd");
            match minga_node::ContainerdRuntime::connect(&args.containerd_socket).await {
                Ok(rt) => {
                    // Attach the node-local secret store. A task that
                    // declares a secret fails closed if the value is not
                    // provisioned here.
                    let store = minga_node::secrets::SecretStore::new(&args.secret_store);
                    tracing::info!(
                        secret_store = %store.root().display(),
                        "node secret store configured"
                    );
                    #[cfg(target_os = "linux")]
                    let rt = rt.with_networking(
                        networking
                            .clone()
                            .expect("containerd runtime has networking"),
                    );
                    let rt = rt
                        .with_volume_dir(format!("{}/volumes", args.data_dir))
                        .with_logs_dir(format!("{}/logs", args.data_dir));
                    Arc::new(rt.with_secret_store(store))
                }
                Err(e) => {
                    eprintln!(
                        "error: cannot connect to containerd at {}: {e}",
                        args.containerd_socket
                    );
                    return ExitCode::FAILURE;
                }
            }
        }
        other => {
            eprintln!("error: unknown runtime '{other}' (expected 'mock' or 'containerd')");
            return ExitCode::FAILURE;
        }
    };

    // Shared by the agent executor (mints) and the egress proxy (checks).
    let egress_tokens = Arc::new(minga_egress::SandboxTokens::new());

    // Construct the agent executor if --sandbox-runtime is set.
    let agent_executor: Option<Arc<dyn minga_node::AgentExecutor>> = match &args.sandbox_runtime {
        Some(rt) => match rt.as_str() {
            "mock" => {
                tracing::info!("using MockAgentExecutor (in-memory, no real sandboxes)");
                Some(Arc::new(minga_node::MockAgentExecutor::new()))
            }
            "wasmtime" => {
                tracing::info!(module_dir = %args.module_dir, "using WasmtimeAgentExecutor");
                match minga_node::WasmtimeAgentExecutor::new(&args.module_dir).await {
                    Ok(exec) => Some(Arc::new(exec.with_egress_tokens(egress_tokens.clone()))),
                    Err(e) => {
                        eprintln!("error: cannot create wasmtime agent executor: {e}");
                        return ExitCode::FAILURE;
                    }
                }
            }
            other => {
                eprintln!(
                    "error: unknown sandbox runtime '{other}' (expected 'mock' or 'wasmtime')"
                );
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    // Start the egress proxy if --egress-bind is set.
    let _egress_proxy: Option<minga_egress::EgressProxy> = if let Some(ref bind) = args.egress_bind
    {
        tracing::info!(bind = %bind, upstream = %args.egress_upstream, "starting egress proxy");
        let budgets = Arc::new(minga_egress::BudgetTracker::new());
        let upstream_auth = match load_upstream_auth(
            &args.secret_store,
            args.egress_credential.as_deref(),
            &args.egress_credential_header,
        ) {
            Ok(a) => a,
            Err(e) => {
                eprintln!("error: cannot load egress credential: {e}");
                return ExitCode::FAILURE;
            }
        };
        let config = minga_egress::EgressProxyConfig {
            upstream_url: args.egress_upstream.clone(),
            timeout: Duration::from_secs(60),
            cache_config: Some(minga_egress::CacheConfig::default()),
            redact_errors: true,
            upstream_auth,
        };
        let mut proxy = minga_egress::EgressProxy::new(config, budgets, egress_tokens.clone());
        match proxy.start(bind).await {
            Ok(addr) => {
                println!("LLM egress proxy listening on http://{addr}");
                Some(proxy)
            }
            Err(e) => {
                eprintln!("error: cannot start egress proxy: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };

    // Create the supervisor with optional agent executor.
    #[allow(unused_mut)]
    let supervisor = match &agent_executor {
        Some(exec) => minga_node::Supervisor::with_agent_executor(
            runtime,
            bus.clone(),
            exec.clone(),
            args.node_id,
        ),
        None => minga_node::Supervisor::with_bus(runtime, bus.clone(), args.node_id),
    };
    #[cfg(target_os = "linux")]
    let supervisor = match networking {
        Some(setup) => {
            let upstream = minga_node::system_upstream_resolver();
            tracing::info!(upstream = ?upstream, "dns: upstream resolver");
            let net = minga_node::supervisor::SupervisorNetworking::new(setup, upstream);
            supervisor.with_networking(net)
        }
        None => supervisor,
    };
    let interval = Duration::from_secs(args.interval);

    if is_manager {
        let state = state.expect("manager StateStore initialized");
        let scheduler = minga_scheduler::Scheduler::with_bus(state.clone(), bus.clone())
            .with_supernet(supernet);
        tracing::info!(
            node_id = args.node_id,
            "starting manager loops (scheduler + supervisor + node registration)"
        );
        tokio::select! {
            _ = scheduler.run(interval) => {},
            _ = minga_node::run_node_registration(state.clone(), bus.clone()) => {},
            _ = supervisor.run(interval) => {},
            _ = tokio::signal::ctrl_c() => {
                tracing::info!(node_id = args.node_id, "shutting down manager");
            },
        }
    } else {
        tracing::info!(
            node_id = args.node_id,
            "starting worker loop (supervisor only)"
        );
        tokio::select! {
            _ = supervisor.run(interval) => {},
            _ = tokio::signal::ctrl_c() => {
                tracing::info!(node_id = args.node_id, "shutting down worker");
            },
        }
    }

    ExitCode::SUCCESS
}

/// Loads and validates the Compose file, printing warnings and errors to stderr.
fn load(args: &ComposeArgs) -> Option<Loaded> {
    let opts = LoadOptions {
        profiles: args.profiles.clone(),
        ..LoadOptions::from_process_env()
    };
    match minga_compose::load_path(&args.compose_file, &opts) {
        Ok(loaded) => {
            for w in &loaded.warnings {
                eprintln!("warning: {w}");
            }
            Some(loaded)
        }
        Err(errors) => {
            for e in errors.iter() {
                eprintln!("error: {e}");
            }
            eprintln!("{}: {} problem(s) found", args.compose_file, errors.0.len());
            None
        }
    }
}

fn print_plan(loaded: &Loaded) {
    let p = &loaded.project;
    if let Some(name) = &p.name {
        println!("project {name}");
    }
    println!("services in startup order:");
    for name in p.startup_order() {
        let svc = &p.services[name];
        let image = if svc.is_agent() {
            "agent"
        } else {
            svc.image.as_deref().unwrap_or("-")
        };
        let deploy = svc.deploy.clone().unwrap_or_default();
        let scale = match deploy.mode {
            DeployMode::Replicated => format!("{} replica(s)", deploy.replicas.unwrap_or(1)),
            DeployMode::Global => "global".to_string(),
            DeployMode::ReplicatedJob => "replicated job".to_string(),
            DeployMode::GlobalJob => "global job".to_string(),
        };
        let ports: Vec<String> = svc
            .ports
            .iter()
            .map(|port| match port.published {
                Some(published) => format!("{published}->{}/{}", port.target, port.protocol),
                None => format!("{}/{}", port.target, port.protocol),
            })
            .collect();
        let after: Vec<&str> = svc.depends_on.keys().map(String::as_str).collect();
        print!("  {name:<16} {image:<40} {scale}");
        if !ports.is_empty() {
            print!("  ports {}", ports.join(", "));
        }
        if !after.is_empty() {
            print!("  after {}", after.join(", "));
        }
        println!();
    }
    if !p.disabled_services.is_empty() {
        println!("disabled by profiles: {}", p.disabled_services.join(", "));
    }
}

/// Prints the state entities that a deploy would create.
fn print_translated(t: &minga_scheduler::TranslatedStack) {
    println!("services:");
    for svc in &t.services {
        let mode = match svc.mode {
            minga_state::types::ServiceMode::Replicated => {
                format!("{} replica(s)", svc.replicas)
            }
            minga_state::types::ServiceMode::Global => "global".to_string(),
        };
        let constraints = if svc.constraints.is_empty() {
            String::new()
        } else {
            format!("  constraints {}", svc.constraints.join(", "))
        };
        let resources = if svc.resources.cpus > 0.0 || svc.resources.memory_bytes > 0 {
            format!(
                "  resources cpu={} mem={}B",
                svc.resources.cpus, svc.resources.memory_bytes
            )
        } else {
            String::new()
        };
        // Show agent info or container image.
        let workload = if let Some(ref agent) = svc.agent {
            format!("agent:{}", agent.module_ref)
        } else {
            svc.image.clone()
        };
        println!(
            "  {:<24} {:<40} {}{}{}",
            svc.id, workload, mode, constraints, resources
        );
        if let Some(ref agent) = svc.agent {
            let mut details = vec![format!("module={}", agent.module_ref)];
            if let Some(fuel) = agent.fuel_limit {
                details.push(format!("fuel={fuel}"));
            }
            if let Some(budget) = agent.token_budget {
                details.push(format!("tokens={budget}"));
            }
            if let Some(ref url) = agent.egress_url {
                details.push(format!("egress={url}"));
            }
            println!("    agent {}", details.join("  "));
        }
        if !svc.env.is_empty() {
            let env: Vec<String> = svc.env.iter().map(|(k, v)| format!("{k}={v}")).collect();
            println!("    env {}", env.join(" "));
        }
        if !svc.ports.is_empty() {
            let ports: Vec<String> = svc
                .ports
                .iter()
                .map(|p| match p.published {
                    Some(pub_) => format!("{pub_}->{}/{}", p.target, p.protocol),
                    None => format!("{}/{}", p.target, p.protocol),
                })
                .collect();
            println!("    ports {}", ports.join(", "));
        }
        if !svc.networks.is_empty() {
            println!("    networks {}", svc.networks.join(", "));
        }
    }
    if !t.networks.is_empty() {
        println!("networks:");
        for net in &t.networks {
            println!("  {:<24} driver={}", net.id, net.driver);
        }
    }
    if !t.volumes.is_empty() {
        println!("volumes:");
        for vol in &t.volumes {
            println!("  {:<24} driver={}", vol.id, vol.driver);
        }
    }
    if !t.secrets.is_empty() {
        println!("secrets:");
        for sec in &t.secrets {
            println!("  {}", sec.id);
        }
    }
    if !t.configs.is_empty() {
        println!("configs:");
        for cfg in &t.configs {
            println!("  {}", cfg.id);
        }
    }
}

// ---------------------------------------------------------------------------
// Network commands
// ---------------------------------------------------------------------------

fn run_net(cmd: NetCommand) -> ExitCode {
    match cmd {
        NetCommand::Create {
            name,
            subnet,
            wg_port,
        } => {
            let mut mgr = minga_net::OverlayManager::new(1);
            let result = match &subnet {
                Some(s) => mgr.create_network(&name, &name, s, wg_port),
                None => mgr.create_network_auto(&name, &name, wg_port),
            };
            match result {
                Ok(()) => {
                    let net = mgr.get_network(&name).unwrap();
                    println!("network created:");
                    println!("  id:       {}", net.id);
                    println!("  name:     {}", net.name);
                    println!("  subnet:   {}", net.subnet);
                    println!("  wg_port:  {}", net.wg_port);
                    println!("  wg_iface: {}", mgr.wg_interface_name(&name));
                    println!("  usable:   {} hosts", net.subnet.usable_hosts());
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        NetCommand::Inspect { cidr } => match minga_net::Cidr::parse(&cidr) {
            Ok(c) => {
                println!("CIDR: {c}");
                println!("  network:    {}", c.network());
                println!("  broadcast:  {}", c.broadcast());
                println!("  prefix:     /{}", c.prefix_len());
                println!("  total:      {} addresses", c.size());
                println!("  usable:     {} hosts", c.usable_hosts());
                if let Some(first) = c.nth_host(0) {
                    println!("  first host: {first}");
                }
                if let Some(last) = c.nth_host(c.usable_hosts().saturating_sub(1)) {
                    println!("  last host:  {last}");
                }
            }
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        },
    }
    ExitCode::SUCCESS
}

// ---------------------------------------------------------------------------
// WireGuard commands
// ---------------------------------------------------------------------------

fn run_wg(cmd: WgCommand) -> ExitCode {
    match cmd {
        WgCommand::Keygen => {
            let kp = minga_net::KeyPair::generate();
            println!("private_key: {}", kp.private_key);
            println!("public_key:  {}", kp.public_key);
        }
        WgCommand::Pubkey { private_key } => {
            match minga_net::KeyPair::public_from_private(&private_key) {
                Ok(pub_key) => println!("{pub_key}"),
                Err(e) => {
                    eprintln!("error: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        WgCommand::Config {
            private_key,
            listen_port,
            address,
            peers,
        } => {
            let private_key = match private_key {
                Some(k) => k,
                None => minga_net::KeyPair::generate().private_key,
            };
            let mut wg_peers = Vec::new();
            for p in &peers {
                // Format: public_key=endpoint=allowed_ips
                let parts: Vec<&str> = p.split('=').collect();
                if parts.len() < 2 {
                    eprintln!("error: invalid peer format '{p}', expected public_key=endpoint=allowed_ips");
                    return ExitCode::FAILURE;
                }
                let public_key = parts[0].to_string();
                let endpoint = if parts.len() > 1 && !parts[1].is_empty() {
                    Some(parts[1].to_string())
                } else {
                    None
                };
                let allowed_ips: Vec<String> = if parts.len() > 2 {
                    parts[2].split(',').map(String::from).collect()
                } else {
                    vec![format!("{}/32", "0.0.0.0")] // placeholder
                };
                wg_peers.push(minga_net::Peer {
                    public_key,
                    allowed_ips,
                    endpoint,
                    keepalive: 25,
                });
            }
            let config = minga_net::InterfaceConfig {
                private_key,
                listen_port,
                address,
                peers: wg_peers,
            };
            print!("{}", config.to_config_string());
        }
    }
    ExitCode::SUCCESS
}

// ---------------------------------------------------------------------------
// Sandbox commands
// ---------------------------------------------------------------------------

async fn run_sandbox(cmd: SandboxCommand) -> ExitCode {
    use minga_sandbox::SandboxRuntime;

    match cmd {
        SandboxCommand::Info => {
            println!("Available sandbox runtimes:");
            println!("  wasmtime    — WASM execution (cross-platform)");
            #[cfg(target_os = "linux")]
            println!("  firecracker — microVM execution (Linux/KVM)");
            println!("  mock        — in-memory mock (testing)");
        }
        SandboxCommand::Run {
            module,
            input,
            fuel,
            timeout,
        } => {
            // Read the module file.
            let module_bytes = match std::fs::read(&module) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("error: cannot read module {module}: {e}");
                    return ExitCode::FAILURE;
                }
            };

            // Read input.
            let input_bytes = if let Some(path) = input.strip_prefix('@') {
                match std::fs::read(path) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("error: cannot read input file {path}: {e}");
                        return ExitCode::FAILURE;
                    }
                }
            } else {
                input.into_bytes()
            };

            let spec = minga_sandbox::SandboxSpec {
                module: module_bytes,
                fuel_limit: Some(fuel),
                timeout: Some(std::time::Duration::from_secs(timeout)),
                ..Default::default()
            };

            let runtime = match minga_sandbox::WasmtimeRuntime::new() {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("error: cannot create wasmtime runtime: {e}");
                    return ExitCode::FAILURE;
                }
            };

            let id = match runtime.create(&spec).await {
                Ok(id) => id,
                Err(e) => {
                    eprintln!("error: cannot create sandbox: {e}");
                    return ExitCode::FAILURE;
                }
            };

            println!("sandbox created: {id}");

            match runtime.run(&id, &input_bytes).await {
                Ok(result) => {
                    println!("exit code:   {}", result.exit_code);
                    println!("fuel used:   {}", result.fuel_consumed);
                    println!("duration:    {:?}", result.duration);
                    if !result.stdout.is_empty() {
                        println!("stdout:      {}", result.stdout);
                    }
                    if !result.stderr.is_empty() {
                        println!("stderr:      {}", result.stderr);
                    }
                    let _ = runtime.destroy(&id).await;
                    if result.exit_code != 0 {
                        return ExitCode::FAILURE;
                    }
                }
                Err(e) => {
                    eprintln!("error: sandbox run failed: {e}");
                    let _ = runtime.destroy(&id).await;
                    return ExitCode::FAILURE;
                }
            }
        }
    }
    ExitCode::SUCCESS
}

// ---------------------------------------------------------------------------
// Egress proxy command
// ---------------------------------------------------------------------------

async fn run_egress(args: EgressArgs) -> ExitCode {
    use std::sync::Arc;
    use std::time::Duration;

    tracing::info!(
        bind = %args.bind,
        upstream = %args.upstream,
        "starting LLM egress proxy"
    );

    let budgets = Arc::new(minga_egress::BudgetTracker::new());

    // Register a default workload budget.
    budgets.register(
        "default",
        minga_egress::Budget::new(args.default_budget, Duration::from_secs(args.budget_window)),
    );

    let upstream_auth = match load_upstream_auth(
        &args.secret_store,
        args.credential.as_deref(),
        &args.credential_header,
    ) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: cannot load upstream credential: {e}");
            return ExitCode::FAILURE;
        }
    };
    let config = minga_egress::EgressProxyConfig {
        upstream_url: args.upstream.clone(),
        timeout: Duration::from_secs(60),
        cache_config: if args.no_cache {
            None
        } else {
            Some(minga_egress::CacheConfig::default())
        },
        redact_errors: true,
        upstream_auth,
    };

    // No agent executor here to mint tokens, so mint one for "default".
    let tokens = Arc::new(minga_egress::SandboxTokens::new());
    let default_token = tokens.issue("default");
    let mut proxy = minga_egress::EgressProxy::new(config, budgets, tokens);

    let addr = match proxy.start(&args.bind).await {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: cannot start egress proxy: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!("LLM egress proxy listening on http://{addr}");
    println!("  upstream:  {}", args.upstream);
    println!("  health:    http://{addr}/health");
    println!("  budgets:   http://{addr}/budgets");
    println!("  proxy:     http://{addr}/v1/chat/completions");
    println!(
        "  identity:  send X-Minga-Sandbox-Token: {}",
        default_token.as_str()
    );

    // Wait forever (until SIGINT).
    tokio::signal::ctrl_c().await.ok();
    tracing::info!("shutting down egress proxy");
    proxy.shutdown();

    ExitCode::SUCCESS
}

// ---------------------------------------------------------------------------
// Agent commands
// ---------------------------------------------------------------------------

async fn run_agent(cmd: AgentCommand) -> ExitCode {
    match cmd {
        AgentCommand::Ls { nats_url } => {
            let Some(url) = nats_url else {
                eprintln!("error: --nats-url (or MINGA_NATS_URL) is required to list agent tasks");
                return ExitCode::FAILURE;
            };

            let bus = match bus::connect_bus(&url, None, 1).await {
                Ok(b) => {
                    let bus: Arc<dyn minga_bus::EventBus> = b;
                    bus
                }
                Err(e) => {
                    eprintln!("error: cannot connect to NATS at {url}: {e}");
                    return ExitCode::FAILURE;
                }
            };

            // Take a snapshot of the status bucket. A watch would only report
            // tasks that change *after* the command starts, so an `ls` that
            // shows nothing would be the common case.
            let statuses = match bus.list_status().await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("error: cannot read status: {e}");
                    return ExitCode::FAILURE;
                }
            };

            let mut reports: Vec<minga_bus::TaskStatusReport> = statuses
                .iter()
                .filter(|(key, _)| key.starts_with("node.") && key.contains(".task."))
                .filter_map(|(_, bytes)| minga_bus::TaskStatusReport::from_bytes(bytes).ok())
                // Agent tasks run in a sandbox and report no container_id.
                .filter(|report| report.container_id.is_none())
                .collect();

            if reports.is_empty() {
                println!("no agent tasks found");
                return ExitCode::SUCCESS;
            }

            reports.sort_by(|a, b| a.task_id.cmp(&b.task_id));

            println!("{:<40} {:<12} {:<8} AGE", "TASK ID", "STATE", "NODE");
            println!("{}", "-".repeat(70));

            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            for report in reports {
                // Guard against clock skew: observed_at may be in the future.
                let age_secs = now_ms.saturating_sub(report.observed_at) / 1000;
                println!(
                    "{:<40} {:<12} {:<8} {}s",
                    report.task_id,
                    format!("{:?}", report.state),
                    report.node_id,
                    age_secs,
                );
            }
        }
    }
    ExitCode::SUCCESS
}

// ---------------------------------------------------------------------------
// Logs command
// ---------------------------------------------------------------------------

async fn run_logs(task_id: &str, follow: bool, tail: usize, nats_url: Option<&str>) -> ExitCode {
    let Some(url) = nats_url else {
        eprintln!("error: --nats-url (or MINGA_NATS_URL) is required to stream logs");
        return ExitCode::FAILURE;
    };

    let bus = match bus::connect_bus(url, None, 1).await {
        Ok(b) => {
            let bus: Arc<dyn minga_bus::EventBus> = b;
            bus
        }
        Err(e) => {
            eprintln!("error: cannot connect to NATS at {url}: {e}");
            return ExitCode::FAILURE;
        }
    };

    use futures::StreamExt;

    // Print the retained backlog first (the last `tail` entries), then — when
    // following — stream new entries as they arrive.
    match bus.log_history(task_id, tail).await {
        Ok(history) => {
            for entry in history {
                print_log_entry(&entry);
            }
        }
        Err(e) => {
            eprintln!("error: cannot read log history for {task_id}: {e}");
            return ExitCode::FAILURE;
        }
    }

    if !follow {
        return ExitCode::SUCCESS;
    }

    let mut log_stream = match bus.subscribe_logs(task_id).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot subscribe to logs for {task_id}: {e}");
            return ExitCode::FAILURE;
        }
    };

    loop {
        tokio::select! {
            entry = log_stream.next() => {
                match entry {
                    Some(entry) => print_log_entry(&entry),
                    None => break,
                }
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }

    ExitCode::SUCCESS
}

/// Writes one log entry to stdout or stderr, preserving the original stream.
fn print_log_entry(entry: &minga_bus::LogEntry) {
    use std::io::Write;

    let data = String::from_utf8_lossy(&entry.data);
    if entry.stream == minga_bus::LogStream::Stderr {
        eprint!("{data}");
        let _ = std::io::stderr().flush();
    } else {
        print!("{data}");
        let _ = std::io::stdout().flush();
    }
}

async fn run_image(cmd: ImageCommand) -> ExitCode {
    match cmd {
        ImageCommand::Pull {
            image_ref,
            containerd_socket,
        } => {
            // The same path as a task's auto-pull: short refs normalized, the
            // Docker credentials honoured, containerd unpacks.
            let image_ref = minga_node::containerd::image::normalize_ref(&image_ref);
            eprintln!("pulling {image_ref} into containerd namespace minga …");
            let client =
                match minga_node::containerd::ContainerdClient::connect(containerd_socket).await {
                    Ok(client) => client,
                    Err(e) => {
                        eprintln!("error: {e}");
                        return ExitCode::FAILURE;
                    }
                };
            match minga_node::containerd::pull_image(
                &client,
                &image_ref,
                minga_node::containerd::DEFAULT_SNAPSHOTTER,
            )
            .await
            {
                Ok(()) => {
                    eprintln!("done");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

/// The absolute directory relative bind-mount sources resolve against: the
/// compose file's directory, which for a bare file name (the default
/// `compose.yaml`) is the current directory. `None` if it cannot be resolved;
/// the API then refuses any relative source instead of letting the node guess.
fn project_dir(compose_file: &str) -> Option<std::path::PathBuf> {
    let dir = match std::path::Path::new(compose_file).parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => std::path::Path::new("."),
    };
    dir.canonicalize().ok()
}

/// Whether a `host:port` bind address only listens on loopback. Unparsable
/// addresses count as not loopback, so the caller warns rather than stays quiet.
fn is_loopback_bind(addr: &str) -> bool {
    let host = match addr.strip_prefix('[') {
        Some(rest) => rest.split(']').next().unwrap_or(""),
        None => addr.rsplit_once(':').map_or(addr, |(h, _)| h),
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// The port of a `host:port` bind address (`[::1]:7001` included).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn port_of(addr: &str) -> Option<u16> {
    addr.rsplit_once(':')?.1.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::{
        is_loopback_bind, port_of, project_dir, run_node, worker_has_manager_only_flags, Cli,
        Command,
    };
    use clap::Parser;

    #[test]
    fn worker_rejects_peer_flag() {
        let cli = Cli::try_parse_from([
            "minga",
            "worker",
            "--node-id",
            "7",
            "--peer",
            "2=127.0.0.1:50051",
        ])
        .unwrap();
        let Command::Worker(args) = cli.command else {
            panic!("expected worker command");
        };
        assert!(worker_has_manager_only_flags(&args));
    }

    #[test]
    fn worker_rejects_nats_cluster_port_flag() {
        let cli = Cli::try_parse_from([
            "minga",
            "worker",
            "--node-id",
            "7",
            "--nats-cluster-port",
            "6223",
        ])
        .unwrap();
        let Command::Worker(args) = cli.command else {
            panic!("expected worker command");
        };
        assert!(worker_has_manager_only_flags(&args));
    }

    #[tokio::test]
    async fn manager_rejects_route_port_without_route_tls() {
        let cli = Cli::try_parse_from([
            "minga",
            "manager",
            "--node-id",
            "1",
            "--bind",
            "127.0.0.1:7001",
            "--nats-cluster-port",
            "6223",
        ])
        .unwrap();
        let Command::Manager(args) = cli.command else {
            panic!("expected manager command");
        };
        assert_eq!(run_node(args, true).await, std::process::ExitCode::FAILURE);
    }
    #[test]
    fn manager_join_and_route_tls_flags_parse() {
        let cli = Cli::try_parse_from([
            "minga",
            "manager",
            "--node-id",
            "4",
            "--bind",
            "127.0.0.1:7004",
            "--join",
            "--nats-cluster-route",
            "nats://manager-1:6222",
            "--nats-cluster-cert",
            "route.pem",
            "--nats-cluster-key",
            "route-key.pem",
            "--nats-cluster-ca",
            "ca.pem",
        ])
        .unwrap();
        let Command::Manager(args) = cli.command else {
            panic!("expected manager command");
        };
        assert!(args.join);
        assert_eq!(args.nats_cluster_routes, ["nats://manager-1:6222"]);
    }

    #[test]
    fn migration_command_defaults_to_dry_run_and_requires_manager_ids() {
        let cli = Cli::try_parse_from(["minga", "migrate-v1", "--manager-id", "1"]).unwrap();
        let Command::MigrateV1(args) = cli.command else {
            panic!("expected migration command");
        };
        assert!(!args.apply);
        assert_eq!(args.manager_ids, [1]);
        assert!(Cli::try_parse_from(["minga", "migrate-v1"]).is_err());
    }

    #[test]
    fn loopback_binds_are_recognised() {
        for addr in [
            "127.0.0.1:7001",
            "127.0.0.2:7001",
            "[::1]:2375",
            "localhost:2375",
        ] {
            assert!(is_loopback_bind(addr), "{addr}");
        }
        for addr in ["0.0.0.0:7001", "10.0.0.1:7001", "[::]:2375", "node1:7001"] {
            assert!(!is_loopback_bind(addr), "{addr}");
        }
    }

    #[test]
    fn port_of_reads_the_bind_port() {
        assert_eq!(port_of("0.0.0.0:7001"), Some(7001));
        assert_eq!(port_of("[::1]:9100"), Some(9100));
        assert_eq!(port_of("no-port"), None);
    }

    #[test]
    fn a_bare_compose_file_name_resolves_against_the_current_directory() {
        let cwd = std::env::current_dir().unwrap().canonicalize().unwrap();
        // `-c compose.yaml`, the default, used to leave relative sources unresolved.
        assert_eq!(project_dir("compose.yaml").as_deref(), Some(cwd.as_path()));
        assert_eq!(
            project_dir("./compose.yaml").as_deref(),
            Some(cwd.as_path())
        );

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("compose.yaml");
        let want = dir.path().canonicalize().unwrap();
        assert_eq!(project_dir(file.to_str().unwrap()), Some(want));
        assert_eq!(project_dir("/nonexistent-minga-dir/compose.yaml"), None);
    }
}
