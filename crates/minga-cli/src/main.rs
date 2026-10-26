mod sidecar;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use minga_compose::{DeployMode, LoadOptions, Loaded};
use sidecar::{NatsSidecar, SidecarOptions};

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
    /// Run the Docker Engine API server (Swarm-compatible REST API).
    Api(ApiArgs),
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
enum NetCommand {
    /// Create a new overlay network.
    Create {
        /// Network name.
        name: String,
        /// Subnet CIDR (e.g. 10.100.0.0/24). If omitted, auto-allocated from 10.100.0.0/16.
        #[arg(long)]
        subnet: Option<String>,
        /// WireGuard listen port (default 51820).
        #[arg(long)]
        wg_port: Option<u16>,
    },
    /// Allocate the next available subnet from the default supernet.
    AllocSubnet {
        /// Network name (used for deterministic allocation).
        name: String,
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
    bind: String,
    /// Data directory for Raft state (raft_log.redb, state.redb).
    #[arg(long, default_value = "/var/lib/minga")]
    data_dir: String,
    /// Peer node as id=address (repeatable). Include all other nodes.
    #[arg(long = "peer")]
    peers: Vec<String>,
    /// Reconcile interval in seconds.
    #[arg(long, default_value = "2")]
    interval: u64,
    /// NATS server URL (e.g. nats://localhost:4222). If not set, uses
    /// an in-memory bus (single-node development only). Can also be set
    /// via the MINGA_NATS_URL environment variable.
    #[arg(long, env = "MINGA_NATS_URL")]
    nats_url: Option<String>,
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
    /// Address to bind the Docker Engine API server (e.g. 0.0.0.0:2375).
    #[arg(long, default_value = "0.0.0.0:2375")]
    api_bind: String,
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

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    match Cli::parse().command {
        Command::Manager(args) => return run_node(args, true).await,
        Command::Worker(args) => return run_node(args, false).await,
        Command::Validate(args) => {
            let Some(loaded) = load(&args) else {
                return ExitCode::FAILURE;
            };
            print_plan(&loaded);
        }
        Command::Deploy { compose, stack } => {
            let Some(loaded) = load(&compose) else {
                return ExitCode::FAILURE;
            };
            match minga_scheduler::translate_project(&loaded.project, &stack) {
                Ok(translated) => print_translated(&translated),
                Err(e) => {
                    eprintln!("error: {e}");
                    return ExitCode::FAILURE;
                }
            }
            tracing::info!(%stack, "cluster deployment: not implemented yet (translation only)");
        }
        Command::Net { cmd } => return run_net(cmd),
        Command::Wg { cmd } => return run_wg(cmd),
        Command::Api(args) => return run_api(args).await,
        Command::Sandbox { cmd } => return run_sandbox(cmd).await,
        Command::Egress(args) => return run_egress(args).await,
        Command::Agent { cmd } => return run_agent(cmd).await,
    }
    ExitCode::SUCCESS
}

/// Runs the Docker Engine API server: opens the state store and starts
/// the HTTP API server. The API server can run on any node — reads are
/// served locally, writes are proposed to Raft (forwarded to the leader).
async fn run_api(args: ApiArgs) -> ExitCode {
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

    let state = match minga_state::StateStore::open(args.node_id, &args.bind, &args.data_dir, peers)
        .await
    {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("error: cannot open state store: {e}");
            return ExitCode::FAILURE;
        }
    };

    let server = match minga_api::ApiServer::start(state, &args.api_bind).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot start API server: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!("Docker Engine API listening on http://{}", server.addr());
    println!("Try: curl http://{}/_ping", server.addr());
    println!("     DOCKER_HOST=tcp://{} docker service ls", server.addr());

    // Wait forever (until SIGTERM/SIGINT).
    tokio::signal::ctrl_c().await.ok();
    tracing::info!("shutting down API server");
    server.shutdown();

    ExitCode::SUCCESS
}

/// Runs a node (manager or worker): opens the state store, starts the
/// supervisor, and (for managers) the scheduler.
async fn run_node(args: NodeArgs, is_manager: bool) -> ExitCode {
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
        data_dir = %args.data_dir,
        peers = ?peers,
        role = if is_manager { "manager" } else { "worker" },
        "starting minga node"
    );

    let state = match minga_state::StateStore::open(args.node_id, &args.bind, &args.data_dir, peers)
        .await
    {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("error: cannot open state store: {e}");
            return ExitCode::FAILURE;
        }
    };

    // Create the event bus.
    // - If --nats-url is set: connect to external NATS.
    // - If manager and no --nats-url: spawn nats-server sidecar.
    // - If worker and no --nats-url: error (workers need the manager's NATS URL).
    let _sidecar: Option<NatsSidecar>;
    let bus: Arc<dyn minga_bus::EventBus> = match &args.nats_url {
        Some(url) => {
            _sidecar = None;
            match minga_bus::NatsBus::connect(url).await {
                Ok(b) => b,
                Err(e) => {
                    eprintln!("error: cannot connect to NATS at {url}: {e}");
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
            // Manager: spawn the NATS sidecar.
            let opts = SidecarOptions {
                data_dir: PathBuf::from(&args.data_dir),
                node_id: args.node_id,
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
            match minga_bus::NatsBus::connect(&url).await {
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

    // Construct the container runtime based on --runtime flag.
    let runtime: Arc<dyn minga_node::Runtime> = match args.runtime.as_str() {
        "mock" => {
            tracing::info!("using MockRuntime (in-memory, no real containers)");
            Arc::new(minga_node::MockRuntime::new())
        }
        "containerd" => {
            tracing::info!(socket = %args.containerd_socket, "connecting to containerd");
            match minga_node::ContainerdRuntime::connect(&args.containerd_socket).await {
                Ok(rt) => Arc::new(rt),
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
                    Ok(exec) => Some(Arc::new(exec)),
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
        let config = minga_egress::EgressProxyConfig {
            upstream_url: args.egress_upstream.clone(),
            timeout: Duration::from_secs(60),
            cache_config: Some(minga_egress::CacheConfig::default()),
            redact_errors: true,
        };
        let mut proxy = minga_egress::EgressProxy::new(config, budgets);
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
    let supervisor = match &agent_executor {
        Some(exec) => minga_node::Supervisor::with_agent_executor(
            state.clone(),
            runtime,
            bus.clone(),
            exec.clone(),
            args.node_id,
        ),
        None => minga_node::Supervisor::with_bus(state.clone(), runtime, bus.clone(), args.node_id),
    };
    let interval = Duration::from_secs(args.interval);

    if is_manager {
        let scheduler = minga_scheduler::Scheduler::with_bus(state, bus);
        tracing::info!(
            node_id = args.node_id,
            "starting manager loops (scheduler + supervisor)"
        );
        tokio::select! {
            _ = scheduler.run(interval) => {},
            _ = supervisor.run(interval) => {},
        }
    } else {
        tracing::info!(
            node_id = args.node_id,
            "starting worker loop (supervisor only)"
        );
        supervisor.run(interval).await;
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
        NetCommand::AllocSubnet { name } => match minga_net::ipam::allocate_subnet(&name) {
            Ok(cidr) => {
                println!("allocated subnet for '{name}': {cidr}");
                println!("  usable hosts: {}", cidr.usable_hosts());
            }
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        },
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

    let config = minga_egress::EgressProxyConfig {
        upstream_url: args.upstream.clone(),
        timeout: Duration::from_secs(60),
        cache_config: if args.no_cache {
            None
        } else {
            Some(minga_egress::CacheConfig::default())
        },
        redact_errors: true,
    };

    let mut proxy = minga_egress::EgressProxy::new(config, budgets);

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
    println!("  workload:  set X-Minga-Workload header to identify the workload");

    // Wait forever (until SIGTERM/SIGINT).
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
            // Without NATS, there's nothing to query — print a message.
            let Some(url) = nats_url else {
                eprintln!("error: --nats-url (or MINGA_NATS_URL) is required to list agent tasks");
                return ExitCode::FAILURE;
            };
            match minga_bus::NatsBus::connect(&url).await {
                Ok(bus) => {
                    // Watch the task status KV for agent tasks.
                    // For now, just indicate the feature is wired.
                    println!("connected to NATS at {url}");
                    println!("agent task listing: query the MINGA_STATUS KV bucket");
                    println!("  keys: task.{{task_id}}");
                    drop(bus);
                }
                Err(e) => {
                    eprintln!("error: cannot connect to NATS at {url}: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
    }
    ExitCode::SUCCESS
}
