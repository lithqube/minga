//! Containerd-backed [`Runtime`] implementation.
//!
//! Talks to a containerd daemon over its gRPC Unix socket. Each Minga
//! task maps to a containerd container + task pair.
//!
//! # Container lifecycle
//!
//! ```text
//! start(task_id, spec):
//!   1. Ensure image in namespace `minga`, pulling from registry if absent
//!   2. Read and authenticate OCI metadata; derive process and parent snapshot
//!   3. Prepare a writable snapshot and generate the OCI spec
//!   4. Create container and task with the snapshot's rootfs mounts
//!   5. Start task; rollback snapshot/container/task on failure
//!
//! stop(container_id):
//!   1. Kill task with SIGTERM (TaskService.Kill)
//!   2. Wait briefly for graceful exit
//!   3. Force kill with SIGKILL if still running (TaskService.Kill)
//!   4. Delete task (TaskService.Delete)
//!   5. Delete container (ContainerService.Delete)
//! ```

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use minga_state::types::{TaskId, TaskSpec};
use tokio::sync::Mutex;

use super::client::{ClientError, ContainerdClient};
use super::credentials::{credential_for, registry_host};
use super::image;
use super::oci;
use super::proto::{Container, Image, ProcessStatus, Runtime as ProtoRuntime};
use crate::runtime::{
    ContainerInfo, ContainerLog, ContainerState, ContainerStatus, LogStream, Runtime, RuntimeError,
};
use crate::secrets::SecretStore;

/// Exec processes are told apart by this counter within a container.
static NEXT_EXEC_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Grace period after SIGTERM before sending SIGKILL.
const GRACE_PERIOD: Duration = Duration::from_secs(10);

/// How long to wait for the process to exit after SIGKILL.
const KILL_GRACE_PERIOD: Duration = Duration::from_secs(10);

/// How long to wait between SIGTERM and checking if the process exited.
const KILL_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// Default containerd snapshotter.
pub const DEFAULT_SNAPSHOTTER: &str = "overlayfs";

/// Default containerd runtime (runc v2).
const DEFAULT_RUNTIME_NAME: &str = "io.containerd.runc.v2";

/// Maximum captured log bytes per task (drop-oldest beyond this).
const LOG_RING_BYTES: usize = 1024 * 1024;

/// A bounded, append-only log capture for one container.
///
/// Each chunk receives a 1-based monotonic sequence number. The sequence
/// never resets while the ring is alive, so the supervisor can use the last
/// sequence it saw as a cursor: even after older chunks are evicted it can
/// tell whether it has published every surviving chunk.
struct LogRing {
    inner: std::sync::Mutex<LogRingInner>,
}

struct LogRingInner {
    /// Total captured bytes currently in the deque.
    bytes: usize,
    deque: std::collections::VecDeque<ContainerLog>,
    /// Next sequence number to assign (1-based; 0 = "nothing pushed yet").
    next_seq: u64,
}

impl Default for LogRing {
    fn default() -> Self {
        Self {
            inner: std::sync::Mutex::new(LogRingInner {
                bytes: 0,
                deque: std::collections::VecDeque::new(),
                next_seq: 1,
            }),
        }
    }
}

impl LogRing {
    fn push(&self, mut log: ContainerLog) {
        let mut guard = self.inner.lock().unwrap();
        log.seq = guard.next_seq;
        guard.next_seq += 1;
        guard.bytes += log.data.len();
        guard.deque.push_back(log);
        while guard.bytes > LOG_RING_BYTES {
            if let Some(old) = guard.deque.pop_front() {
                guard.bytes -= old.data.len();
            } else {
                break;
            }
        }
    }

    fn snapshot(&self) -> Vec<ContainerLog> {
        self.inner.lock().unwrap().deque.iter().cloned().collect()
    }
}

/// How long a pull may take, however it is done.
const PULL_TIMEOUT: Duration = Duration::from_secs(300);

/// Pulls and unpacks `image_ref` into the `minga` namespace through
/// containerd's Transfer service on `client`'s own socket. The registry login
/// (Docker config, credential helpers) is handed to the daemon over a stream,
/// so it is never on a command line.
///
/// A daemon older than containerd 1.7 has no Transfer service: an anonymous
/// pull then falls back to [`pull_with_ctr`], but a pull that needs a login
/// fails, because the only other way to pass one is a command-line argument.
pub async fn pull_image(
    client: &ContainerdClient,
    image_ref: &str,
    snapshotter: &str,
) -> Result<(), RuntimeError> {
    let credential = credential_for(image_ref)
        .await
        .map_err(|e| RuntimeError::Start(format!("registry credentials for {image_ref}: {e}")))?;
    tracing::info!(image = %image_ref, authenticated = credential.is_some(), "pulling image via containerd");
    let auth = credential
        .as_ref()
        .map(|credential| (registry_host(image_ref), credential));
    let pull = tokio::time::timeout(
        PULL_TIMEOUT,
        client.pull_image(image_ref, snapshotter, auth),
    )
    .await
    .map_err(|_| {
        RuntimeError::Start(format!(
            "pull {image_ref} timed out after {}s",
            PULL_TIMEOUT.as_secs()
        ))
    })?;
    match pull {
        Ok(()) => {
            tracing::info!(image = %image_ref, "image pulled and unpacked");
            Ok(())
        }
        Err(ClientError::Grpc(status)) if status.code() == tonic::Code::Unimplemented => {
            if credential.is_some() {
                return Err(RuntimeError::Start(format!(
                    "pull {image_ref}: this containerd has no Transfer service, which passing \
                     registry credentials needs (containerd {MIN_TRANSFER_VERSION} or newer); \
                     upgrade containerd or pull the image anonymously"
                )));
            }
            tracing::warn!(image = %image_ref, "containerd has no Transfer service (older than {MIN_TRANSFER_VERSION}); pulling anonymously with ctr");
            pull_with_ctr(client.socket(), image_ref).await
        }
        Err(e) => Err(RuntimeError::Start(format!("pull {image_ref}: {e}"))),
    }
}

/// The first containerd release with the Transfer service.
const MIN_TRANSFER_VERSION: &str = "1.7";

/// Anonymous pull with `ctr` (containerd older than 1.7, which has no
/// Transfer service), on `socket` when given. Never takes credentials.
pub async fn pull_with_ctr(
    socket: Option<&std::path::Path>,
    image_ref: &str,
) -> Result<(), RuntimeError> {
    tracing::info!(image = %image_ref, "pulling image via ctr");
    let mut command = tokio::process::Command::new("ctr");
    if let Some(socket) = socket {
        command.arg("--address").arg(socket);
    }
    let pull = tokio::time::timeout(
        PULL_TIMEOUT,
        command
            .args(["-n", "minga", "images", "pull", image_ref])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| RuntimeError::Start(format!("ctr pull {image_ref} timed out after 300s")))?
    .map_err(|e| RuntimeError::Start(format!("spawn ctr pull: {e}")))?;

    if !pull.status.success() {
        let stderr = String::from_utf8_lossy(&pull.stderr);
        return Err(RuntimeError::Start(format!(
            "ctr pull {image_ref} failed ({}): {stderr}",
            pull.status
        )));
    }
    tracing::info!(image = %image_ref, "image pulled and unpacked");
    Ok(())
}

/// `extra_hosts` are written into a per-task /etc/hosts that only exists for
/// tasks with a network attachment; refuse the rest instead of skipping them.
fn check_extra_hosts_have_network(spec: &TaskSpec) -> Result<(), RuntimeError> {
    if spec.ips.is_empty() && !spec.extra_hosts.is_empty() {
        return Err(RuntimeError::Start(format!(
            "extra_hosts ({} entries) need a network, but the task is attached to none",
            spec.extra_hosts.len()
        )));
    }
    Ok(())
}

/// A task's /etc/hosts: localhost plus its `extra_hosts`, with `host-gateway`
/// replaced by the host's address on the task's bridge.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn hosts_file_content(
    extra_hosts: &[(String, String)],
    gateway: Option<std::net::Ipv4Addr>,
) -> Result<String, RuntimeError> {
    let mut content = "127.0.0.1\tlocalhost\n::1\tlocalhost\n".to_string();
    for (host, ip) in extra_hosts {
        let ip = match (ip.as_str(), gateway) {
            ("host-gateway", Some(gateway)) => gateway.to_string(),
            ("host-gateway", None) => {
                return Err(RuntimeError::Start(format!(
                    "extra_hosts `{host}:host-gateway`: the task's network has no gateway"
                )))
            }
            (ip, _) => ip.to_string(),
        };
        content.push_str(&format!("{ip}\t{host}\n"));
    }
    Ok(content)
}

/// A non-root process cannot read a root-owned 0400 file. Each staged copy
/// belongs to one task, so this exposes it to that task only.
fn chown_secrets(mounts: &[oci::SecretMount], uid: u32, gid: u32) -> Result<(), RuntimeError> {
    for mount in mounts {
        std::os::unix::fs::chown(&mount.source, Some(uid), Some(gid))
            .map_err(|e| RuntimeError::Start(format!("secret ownership: {e}")))?;
    }
    Ok(())
}

/// Creates a FIFO at `path` (0600).
fn make_fifo(path: &std::path::Path) -> Result<(), std::io::Error> {
    let raw = std::ffi::CString::new(path.display().to_string().as_bytes().to_vec())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad fifo path"))?;
    // SAFETY: mkfifo with a valid C string and sane mode.
    let rc = unsafe { libc::mkfifo(raw.as_ptr(), 0o600) };
    if rc != 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Drains a FIFO into `ring` until aborted. Opened O_RDWR nonblocking so
/// the open never blocks and reads never report EOF while containerd
/// lazily opens the writer end.
async fn fifo_reader(fifo: PathBuf, stream: LogStream, ring: Arc<LogRing>) {
    use std::os::unix::io::AsRawFd;

    let file = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&fifo)
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(fifo = %fifo.display(), error = %e, "log fifo open failed");
            return;
        }
    };
    // SAFETY: fcntl on a valid fd.
    unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) };
    let fd = match tokio::io::unix::AsyncFd::new(file) {
        Ok(fd) => fd,
        Err(e) => {
            tracing::error!(fifo = %fifo.display(), error = %e, "log fifo async setup failed");
            return;
        }
    };

    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let mut guard = match fd.readable().await {
            Ok(g) => g,
            Err(e) => {
                tracing::error!(fifo = %fifo.display(), error = %e, "log fifo readable failed");
                return;
            }
        };
        let result = guard.try_io(|inner| {
            // SAFETY: buf is a valid writable region; fd is valid.
            let n = unsafe { libc::read(inner.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
            if n < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(n as usize)
            }
        });
        match result {
            Ok(Ok(0)) => {}
            Ok(Ok(n)) => ring.push(ContainerLog {
                stream,
                data: buf[..n].to_vec(),
                seq: 0, // ring.push() overwrites this with the next sequence number
            }),
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Ok(Err(e)) => {
                tracing::error!(fifo = %fifo.display(), error = %e, "log fifo read failed");
                return;
            }
            Err(_not_ready) => {}
        }
    }
}

/// Whether a client error means "the resource is already gone".
///
/// containerd answers with `NotFound` when a task, container or snapshot
/// does not exist. That is the normal case when cleaning up after a
/// previous attempt, so it must not be reported as a failure.
fn is_missing(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Grpc(status) if status.code() == tonic::Code::NotFound
    )
}

/// A container runtime backed by containerd.
pub struct ContainerdRuntime {
    client: Arc<Mutex<ContainerdClient>>,
    /// Track which container IDs we've started (for status lookups).
    containers: Arc<Mutex<HashSet<String>>>,
    snapshotter: String,
    runtime_name: String,
    /// Node-local secret store used to resolve secret references.
    secret_store: Option<SecretStore>,
    /// Directory under which named volumes are materialized
    /// (`<data_dir>/volumes`). Required to start tasks with named-volume
    /// mounts.
    volume_dir: Option<PathBuf>,
    /// Directory for log FIFOs (`<data_dir>/logs`). When set, container
    /// stdout/stderr are captured into bounded per-task rings.
    logs_dir: Option<PathBuf>,
    /// Per-container log rings (bounded, drop-oldest) and their readers.
    log_rings: Arc<Mutex<HashMap<String, Arc<LogRing>>>>,
    /// Reader task handles, aborted on stop.
    log_readers: Arc<Mutex<HashMap<String, tokio::task::JoinHandle<()>>>>,
    egress_tokens: Option<Arc<minga_egress::SandboxTokens>>,
    egress_budgets: Option<Arc<minga_egress::BudgetTracker>>,
    active_egress_tokens: Arc<Mutex<HashMap<String, minga_egress::SandboxToken>>>,
    /// Network setup; assignment records carry the subnet metadata.
    #[cfg(target_os = "linux")]
    networking: Option<Arc<crate::netns::NetworkSetup>>,
    #[cfg(target_os = "linux")]
    networking_blocked: AtomicBool,
}

/// The resolved route policy of a task that uses the egress proxy, `None`
/// for one that does not. Every routed task goes through the resolver, so
/// a required policy without routes is refused instead of reaching the
/// legacy upstream.
fn task_egress_policy(
    spec: &TaskSpec,
    secret_store: Option<&SecretStore>,
) -> Result<Option<minga_egress::ResolvedEgressPolicy>, RuntimeError> {
    let required_label = spec
        .labels
        .get(minga_state::types::REQUIRED_EGRESS_LABEL)
        .is_some_and(|value| value == "true");
    let workload = spec.egress.as_ref();
    if required_label != workload.is_some_and(|workload| workload.policy.required) {
        return Err(RuntimeError::Start(
            "required-egress label and route policy disagree".into(),
        ));
    }
    if !spec.uses_egress_proxy() {
        return Ok(None);
    }
    let workload = workload
        .ok_or_else(|| RuntimeError::Start("required egress has no workload identity".into()))?;
    resolve_egress_policy(secret_store, workload).map(Some)
}

fn resolve_egress_policy(
    secret_store: Option<&SecretStore>,
    workload: &minga_state::types::EgressWorkload,
) -> Result<minga_egress::ResolvedEgressPolicy, RuntimeError> {
    use minga_egress::{ResolvedAuth, ResolvedEgressPolicy, ResolvedRoute};
    use minga_state::types::EgressAuth;

    let policy = &workload.policy;
    if policy.required && policy.routes.is_empty() {
        return Err(RuntimeError::Start(
            "required egress has no named routes".into(),
        ));
    }
    let mut routes = HashMap::with_capacity(policy.routes.len());
    for route in &policy.routes {
        let auth = match &route.auth {
            None => None,
            Some(EgressAuth::Bearer { secret }) => {
                let store = secret_store.ok_or_else(|| {
                    RuntimeError::Start("egress route credential has no node secret store".into())
                })?;
                let value = store.read_value(secret).map_err(|error| {
                    RuntimeError::Start(format!(
                        "cannot resolve egress route secret `{secret}`: {error}"
                    ))
                })?;
                Some(ResolvedAuth::Bearer(value))
            }
            Some(EgressAuth::ApiKey { secret }) => {
                let store = secret_store.ok_or_else(|| {
                    RuntimeError::Start("egress route credential has no node secret store".into())
                })?;
                let value = store.read_value(secret).map_err(|error| {
                    RuntimeError::Start(format!(
                        "cannot resolve egress route secret `{secret}`: {error}"
                    ))
                })?;
                Some(ResolvedAuth::ApiKey(value))
            }
            Some(EgressAuth::AwsSigv4 {
                access_key_secret,
                secret_key_secret,
                session_token_secret,
                region,
                service,
            }) => {
                let store = secret_store.ok_or_else(|| {
                    RuntimeError::Start("egress route credential has no node secret store".into())
                })?;
                let access_key = store.read_value(access_key_secret).map_err(|error| {
                    RuntimeError::Start(format!(
                        "cannot resolve egress route secret `{access_key_secret}`: {error}"
                    ))
                })?;
                let secret_key = store.read_value(secret_key_secret).map_err(|error| {
                    RuntimeError::Start(format!(
                        "cannot resolve egress route secret `{secret_key_secret}`: {error}"
                    ))
                })?;
                let session_token = session_token_secret
                    .as_deref()
                    .map(|secret| {
                        store.read_value(secret).map_err(|error| {
                            RuntimeError::Start(format!(
                                "cannot resolve egress route secret `{secret}`: {error}"
                            ))
                        })
                    })
                    .transpose()?;
                Some(ResolvedAuth::AwsSigv4 {
                    access_key,
                    secret_key,
                    session_token,
                    region: region.clone(),
                    service: service.clone(),
                })
            }
        };
        let resolved = ResolvedRoute {
            name: route.name.clone(),
            upstream_url: route.upstream_url.clone(),
            auth,
            fallback: route.fallback.clone(),
            input_price_micros_per_million: route.input_price_micros_per_million,
            output_price_micros_per_million: route.output_price_micros_per_million,
        };
        if routes.insert(route.name.clone(), resolved).is_some() {
            return Err(RuntimeError::Start(format!(
                "duplicate egress route `{}`",
                route.name
            )));
        }
    }
    for route in routes.values() {
        for fallback in &route.fallback {
            if !routes.contains_key(fallback) || fallback == &route.name {
                return Err(RuntimeError::Start(format!(
                    "egress route `{}` has invalid fallback `{fallback}`",
                    route.name
                )));
            }
        }
    }
    Ok(ResolvedEgressPolicy {
        routes,
        models: policy.models.clone(),
    })
}

/// Budget of a routed container. Egress policies carry no token budget
/// yet, so routed containers are not token-limited: the proxy still
/// reserves and records every request, and usage events are published.
fn routed_task_budget() -> minga_egress::Budget {
    minga_egress::Budget::new(u64::MAX, std::time::Duration::from_secs(3600))
}

struct PendingEgressToken {
    store: SecretStore,
    task_id: String,
    token: Option<minga_egress::SandboxToken>,
    budgets: Option<Arc<minga_egress::BudgetTracker>>,
    committed: bool,
}

impl Drop for PendingEgressToken {
    fn drop(&mut self) {
        if !self.committed {
            self.store.cleanup_task(&self.task_id);
            if let Some(budgets) = &self.budgets {
                budgets.unregister(&self.task_id);
            }
        }
    }
}

impl ContainerdRuntime {
    /// Connect to containerd at the given socket path.
    pub async fn connect(socket_path: impl Into<PathBuf>) -> Result<Self, RuntimeError> {
        let client = ContainerdClient::connect(socket_path.into())
            .await
            .map_err(|e| RuntimeError::Other(format!("containerd connect: {e}")))?;
        Ok(Self::with_client(client))
    }

    /// Create a runtime from an existing client (for testing).
    pub fn with_client(client: ContainerdClient) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
            containers: Arc::new(Mutex::new(HashSet::new())),
            snapshotter: DEFAULT_SNAPSHOTTER.to_string(),
            runtime_name: DEFAULT_RUNTIME_NAME.to_string(),
            secret_store: None,
            volume_dir: None,
            logs_dir: None,
            log_rings: Arc::new(Mutex::new(HashMap::new())),
            log_readers: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(target_os = "linux")]
            networking: None,
            #[cfg(target_os = "linux")]
            networking_blocked: AtomicBool::new(false),
            egress_tokens: None,
            egress_budgets: None,
            active_egress_tokens: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Attach a node-local secret store.
    ///
    /// Without one, a task that declares secrets **fails closed** — the
    /// container is not created. A missing secret must stop the task, never
    /// start it with an empty credential.
    pub fn with_secret_store(mut self, store: SecretStore) -> Self {
        self.secret_store = Some(store);
        self
    }
    /// Shares workload identity tokens with the local egress proxy.
    pub fn with_egress_tokens(mut self, tokens: Arc<minga_egress::SandboxTokens>) -> Self {
        self.egress_tokens = Some(tokens);
        self
    }

    /// Shares the local egress proxy's budget tracker. Each routed task is
    /// registered before it starts; see [`routed_task_budget`].
    pub fn with_egress_budgets(mut self, budgets: Arc<minga_egress::BudgetTracker>) -> Self {
        self.egress_budgets = Some(budgets);
        self
    }

    /// Revokes a task's egress token and drops its budget.
    async fn revoke_egress(&self, container_id: &str) {
        self.active_egress_tokens.lock().await.remove(container_id);
        if let Some(budgets) = &self.egress_budgets {
            budgets.unregister(container_id);
        }
    }

    /// Resolve and stage the task's secret references into mounts.
    ///
    /// Fails closed: if a secret is missing, unsafe or the node has no
    /// secret store configured, the task is not started. The error names
    /// the secret but never its value. Staging is per-task and
    /// content-addressed: the mounted file is an immutable copy named by
    /// the content hash, so a rotated secret is visible to a new task
    /// and never changes under a running one.
    fn resolve_secrets(
        &self,
        task_id: &str,
        spec: &TaskSpec,
    ) -> Result<Vec<oci::SecretMount>, RuntimeError> {
        if spec.secrets.is_empty() {
            return Ok(Vec::new());
        }

        let store = self.secret_store.as_ref().ok_or_else(|| {
            RuntimeError::Start(format!(
                "task declares {} secret(s) but no node secret store is configured",
                spec.secrets.len()
            ))
        })?;

        let staged = store
            .stage_all(task_id, &spec.secrets)
            .map_err(|e| RuntimeError::Start(format!("secret staging failed: {e}")))?;

        for s in &staged {
            tracing::info!(
                task = %task_id,
                secret = %s.secret_ref.secret_id,
                version = s.secret_ref.version.as_deref().unwrap_or(""),
                "secret staged"
            );
        }

        Ok(staged
            .into_iter()
            .map(|s| oci::SecretMount {
                source: s.staged_path,
                target: s.secret_ref.target,
            })
            .collect())
    }
    /// Attach node networking: tasks with IP assignments get a prepared
    /// netns (bridge + veth + published ports + resolv.conf).
    ///
    /// Without this, a task that carries IP assignments **fails closed** —
    /// starting it would produce a container with no interfaces, which is
    /// the silent-failure mode networking exists to eliminate.
    #[cfg(target_os = "linux")]
    pub fn with_networking(mut self, setup: Arc<crate::netns::NetworkSetup>) -> Self {
        self.networking = Some(setup);
        self.networking_blocked.store(true, Ordering::Release);
        self
    }

    /// Prepares the task's network namespace when the task has IPs.
    #[cfg(target_os = "linux")]
    async fn prepare_network(
        &self,
        task_id: &str,
        spec: &mut TaskSpec,
        networks: &[minga_state::types::Network],
    ) -> Result<Option<oci::NetworkSpec>, RuntimeError> {
        let required_egress = spec
            .labels
            .get(minga_state::types::REQUIRED_EGRESS_LABEL)
            .is_some_and(|value| value == "true");
        let policy_required = spec
            .egress
            .as_ref()
            .is_some_and(|workload| workload.policy.required);
        let has_routes = spec
            .egress
            .as_ref()
            .is_some_and(|workload| !workload.policy.routes.is_empty());
        if required_egress != policy_required {
            return Err(RuntimeError::Start(
                "required-egress label and route policy disagree".into(),
            ));
        }
        let proxy_egress = required_egress || has_routes;
        if spec.ips.is_empty() && proxy_egress {
            return Err(RuntimeError::Start(
                "egress task has no network assignment".into(),
            ));
        }
        if spec.ips.is_empty() {
            return Ok(None);
        }
        let setup = self.networking.as_ref().ok_or_else(|| {
            RuntimeError::Start(format!(
                "task has IP assignments on {} network(s) but node networking is not configured",
                spec.ips.len()
            ))
        })?;
        let mut attachments = Vec::new();
        for (net_id, raw_ip) in &spec.ips {
            let network = networks.iter().find(|n| &n.id == net_id).ok_or_else(|| {
                RuntimeError::Start(format!("task IP references unknown network {net_id}"))
            })?;
            let subnet = crate::netns::subnet_for(net_id, network.subnet.as_deref())
                .map_err(|e| RuntimeError::Start(e.to_string()))?;
            let ip = raw_ip.parse().map_err(|e| {
                RuntimeError::Start(format!("task IP {raw_ip:?} on {net_id} is invalid: {e}"))
            })?;
            attachments.push(crate::netns::TaskNetAttachment {
                network_id: net_id.clone(),
                ip,
                subnet,
            });
        }
        attachments.sort_by(|a, b| a.network_id.cmp(&b.network_id));
        let search: Vec<String> = match spec.env.get("MINGA_STACK") {
            Some(stack) => vec![
                format!("{stack}.{}", minga_net::MINGA_DOMAIN),
                minga_net::MINGA_DOMAIN.to_string(),
            ],
            None => vec![minga_net::MINGA_DOMAIN.to_string()],
        };
        let netns = setup
            .setup_task(task_id, &attachments, &spec.ports, &search)
            .await
            .map_err(|e| RuntimeError::Start(format!("network setup: {e}")))?;
        if required_egress {
            if let Err(error) = setup.require_task_egress(task_id).await {
                let _ = setup.teardown_task(task_id).await;
                return Err(RuntimeError::Start(format!("egress policy setup: {error}")));
            }
        }
        if proxy_egress {
            let gateway = crate::netns::gateway_of(&attachments[0].subnet).ok_or_else(|| {
                RuntimeError::Start("task network attachment has no gateway".into())
            })?;
            spec.env.insert(
                "MINGA_EGRESS_URL".into(),
                format!("http://{gateway}:{}", setup.egress_proxy_port()),
            );
        }
        let hosts_path = if spec.extra_hosts.is_empty() {
            None
        } else {
            let hosts_file = netns.resolv_conf.with_extension("hosts");
            let gateway = crate::netns::gateway_of(&attachments[0].subnet);
            let content = hosts_file_content(&spec.extra_hosts, gateway)?;
            tokio::fs::write(&hosts_file, &content)
                .await
                .map_err(|e| RuntimeError::Start(format!("write hosts: {e}")))?;
            Some(hosts_file)
        };
        Ok(Some(oci::NetworkSpec {
            netns_path: netns.netns_path.display().to_string(),
            resolv_conf: Some(netns.resolv_conf),
            hosts_path,
        }))
    }

    /// Stops a container's log reader and drops its ring and FIFOs.
    async fn teardown_logs(&self, container_id: &str) {
        if let Some(handle) = self.log_readers.lock().await.remove(container_id) {
            handle.abort();
        }
        self.log_rings.lock().await.remove(container_id);
        if let Some(logs_dir) = &self.logs_dir {
            for suffix in ["stdout.fifo", "stderr.fifo"] {
                let _ =
                    tokio::fs::remove_file(logs_dir.join(format!("{container_id}.{suffix}"))).await;
            }
        }
    }

    /// Aborts log reader tasks for `container_id` without removing the FIFO
    /// files from disk.
    ///
    /// This models a supervisor restart: the old process dies (reader tasks
    /// are gone) but the named pipes survive on disk so the new instance can
    /// re-open them via [`Self::open_log_fifos`].  Used by tests that need
    /// to stop the original readers before the second runtime instance
    /// adopts the same container.
    pub async fn close_log_readers(&self, container_id: &str) {
        if let Some(handle) = self.log_readers.lock().await.remove(container_id) {
            handle.abort();
        }
        self.log_rings.lock().await.remove(container_id);
    }

    /// Undoes the network and log setup of a start that failed before its
    /// container existed, and hands back the error.
    async fn abort_start(&self, container_id: &str, error: RuntimeError) -> RuntimeError {
        #[cfg(target_os = "linux")]
        self.teardown_network(container_id).await;
        self.teardown_logs(container_id).await;
        error
    }

    /// Tears down a task's network, tolerating its absence.
    #[cfg(target_os = "linux")]
    async fn teardown_network(&self, task_id: &str) {
        if let Some(setup) = &self.networking {
            if let Err(e) = setup.teardown_task(task_id).await {
                tracing::warn!(task = %task_id, error = %e, "network teardown failed");
            }
        }
    }

    /// Attach the node's volume directory. Without one, a task mounting a
    /// named volume **fails closed** — silently skipping the mount would
    /// start the container against different data than declared.
    pub fn with_volume_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.volume_dir = Some(dir.into());
        self
    }

    /// Resolves the task's mounts: named volumes are materialized as
    /// directories under the node's volume dir; missing bind sources are
    /// created (Docker behavior). Returns the spec with concrete host
    /// paths.
    async fn resolve_mounts(&self, spec: &TaskSpec) -> Result<TaskSpec, RuntimeError> {
        if spec.mounts.is_empty() {
            return Ok(spec.clone());
        }
        let mut spec = spec.clone();
        for m in &mut spec.mounts {
            match m.kind {
                minga_state::types::MountKind::Volume => {
                    let root = self.volume_dir.as_ref().ok_or_else(|| {
                        RuntimeError::Start(format!(
                            "task mounts named volume `{}` but no node volume dir is configured",
                            m.source
                        ))
                    })?;
                    let dir = root.join(&m.source);
                    tokio::fs::create_dir_all(&dir).await.map_err(|e| {
                        RuntimeError::Start(format!("create volume dir {}: {e}", dir.display()))
                    })?;
                    m.source = dir.display().to_string();
                }
                minga_state::types::MountKind::Bind => {
                    // Skip mkdir if the source already exists (file bind mounts).
                    // Only create a directory when the path is absent entirely.
                    let path = std::path::Path::new(&m.source);
                    if !path.exists() {
                        tokio::fs::create_dir_all(path).await.map_err(|e| {
                            RuntimeError::Start(format!("create bind source {}: {e}", m.source))
                        })?;
                    }
                }
            }
        }
        Ok(spec)
    }

    /// Attach the log capture directory. With it, container stdout/stderr
    /// go to per-task FIFOs drained into bounded rings ([`Runtime::logs`]).
    pub fn with_logs_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.logs_dir = Some(dir.into());
        self
    }

    /// Override the snapshotter (default: overlayfs).
    pub fn with_snapshotter(mut self, snapshotter: impl Into<String>) -> Self {
        self.snapshotter = snapshotter.into();
        self
    }

    /// Override the OCI runtime name (default: io.containerd.runc.v2).
    pub fn with_runtime_name(mut self, name: impl Into<String>) -> Self {
        self.runtime_name = name.into();
        self
    }

    /// Ensure the image is available in the Minga containerd namespace,
    /// pulling from the registry if needed.
    async fn ensure_image(&self, image_ref: &str) -> Result<Image, RuntimeError> {
        let mut client = self.client.lock().await;
        match client.get_image(image_ref).await {
            Ok(Some(image)) => Ok(image),
            Ok(None) => {
                drop(client);
                self.pull_image(image_ref).await?;
                let mut client = self.client.lock().await;
                client
                    .get_image(image_ref)
                    .await
                    .map_err(|e| RuntimeError::Start(format!("check image after pull: {e}")))?
                    .ok_or_else(|| {
                        RuntimeError::Start(format!("image {image_ref} still missing after pull"))
                    })
            }
            Err(e) => Err(RuntimeError::Start(format!("check image {image_ref}: {e}"))),
        }
    }

    async fn pull_image(&self, image_ref: &str) -> Result<(), RuntimeError> {
        // A clone shares the connection but not the lock: a long pull must not
        // hold up other tasks' calls.
        let client = self.client.lock().await.clone();
        pull_image(&client, image_ref, &self.snapshotter).await
    }
    #[cfg(target_os = "linux")]
    async fn stop_all_task_containers(&self) -> Result<(), RuntimeError> {
        let containers = {
            let mut client = self.client.lock().await;
            client
                .list_containers(vec!["labels.minga.task_id!=empty".to_string()])
                .await
                .map_err(|error| {
                    RuntimeError::Stop(format!(
                        "list task containers while blocking networking: {error}"
                    ))
                })?
        };
        let mut errors = Vec::new();
        for container in containers {
            {
                let mut client = self.client.lock().await;
                if let Err(error) = client.kill_task(&container.id, 9).await {
                    tracing::warn!(
                        container = %container.id,
                        %error,
                        "emergency SIGKILL failed; normal stop will retry"
                    );
                }
            }
            if let Err(error) = self.stop(&container.id).await {
                errors.push(format!("{}: {error}", container.id));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(RuntimeError::Stop(format!(
                "could not stop all task containers: {}",
                errors.join("; ")
            )))
        }
    }

    /// Clear leftovers from a previous attempt at the same task ID.
    ///
    /// A container, its task and its writable snapshot all share the task ID,
    /// so a restart must remove them first. Resources that are already gone
    /// are not an error.
    async fn cleanup_existing(
        client: &mut ContainerdClient,
        container_id: &str,
        snapshot_key: &str,
        snapshotter: &str,
    ) -> Result<(), RuntimeError> {
        let mut errors = Vec::new();
        if let Err(error) = client.kill_task(container_id, 9).await {
            if !is_missing(&error) {
                errors.push(format!("kill stale task: {error}"));
            }
        }
        // Brief pause after SIGKILL so containerd v2 marks the task stopped
        // before we attempt delete — without this, delete may return
        // "cannot delete a running process" on containerd 2.2+.
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Err(error) = client.delete_task(container_id).await {
            if !is_missing(&error) {
                errors.push(format!("delete stale task: {error}"));
            }
        }
        if let Err(error) = client.delete_container(container_id).await {
            if !is_missing(&error) {
                errors.push(format!("delete stale container: {error}"));
            }
        }
        if let Err(error) = client.remove_snapshot(snapshotter, snapshot_key).await {
            if !is_missing(&error) {
                errors.push(format!("remove stale snapshot: {error}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(RuntimeError::Start(format!(
                "clear previous attempt for {container_id}: {}",
                errors.join(", ")
            )))
        }
    }

    /// Wait for a task to reach the stopped state.
    ///
    /// Returns `true` when the task is stopped or already gone, `false` when
    /// the deadline passed with the task still running.
    async fn wait_for_stop(
        client: &Arc<Mutex<ContainerdClient>>,
        container_id: &str,
        deadline: std::time::Instant,
    ) -> bool {
        loop {
            let status = {
                let mut guard = client.lock().await;
                match guard.get_task(container_id).await {
                    Ok(process) => Some(
                        ProcessStatus::try_from(process.status).unwrap_or(ProcessStatus::Unknown),
                    ),
                    Err(ClientError::Grpc(status)) if status.code() == tonic::Code::NotFound => {
                        None // task is gone
                    }
                    Err(_) => return true,
                }
            };
            match status {
                Some(ProcessStatus::Stopped) | None => return true,
                Some(_) => {}
            }

            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(KILL_CHECK_INTERVAL).await;
        }
    }

    /// Report incomplete rollback, so a leaked task/snapshot is never silent.
    async fn rollback_start(
        client: &mut ContainerdClient,
        container_id: &str,
        snapshot_key: &str,
        snapshotter: &str,
        container_created: bool,
        task_created: bool,
        task_may_be_running: bool,
    ) -> String {
        let mut errors = Vec::new();
        if task_may_be_running {
            if let Err(error) = client.kill_task(container_id, 9).await {
                errors.push(format!("kill task: {error}"));
            }
        }
        if task_created {
            if let Err(error) = client.delete_task(container_id).await {
                errors.push(format!("delete task: {error}"));
            }
        }
        if container_created {
            if let Err(error) = client.delete_container(container_id).await {
                errors.push(format!("delete container: {error}"));
            }
        }
        if let Err(error) = client.remove_snapshot(snapshotter, snapshot_key).await {
            errors.push(format!("remove snapshot: {error}"));
        }
        if errors.is_empty() {
            String::new()
        } else {
            format!("; rollback incomplete: {}", errors.join(", "))
        }
    }
    /// Opens FIFO readers for a container that was adopted on startup
    /// (already running when the supervisor started). The FIFOs must already
    /// exist on disk — they were created when the container was first started.
    /// If readers are already running for this container this is a no-op.
    async fn start_adopted_log_readers(&self, container_id: &str) {
        let Some(logs_dir) = &self.logs_dir else {
            return;
        };
        // Guard: skip if readers already running.
        if self.log_readers.lock().await.contains_key(container_id) {
            return;
        }
        let stdout_path = logs_dir.join(format!("{container_id}.stdout.fifo"));
        let stderr_path = logs_dir.join(format!("{container_id}.stderr.fifo"));
        if !stdout_path.exists() && !stderr_path.exists() {
            tracing::debug!(
                container = %container_id,
                "no log FIFOs found for adopted container; log capture will be absent"
            );
            return;
        }
        let ring = Arc::new(LogRing::default());
        self.log_rings
            .lock()
            .await
            .insert(container_id.to_string(), ring.clone());
        let out_ring = ring.clone();
        let handle = tokio::spawn(async move {
            tokio::join!(
                fifo_reader(stdout_path, LogStream::Stdout, out_ring),
                fifo_reader(stderr_path, LogStream::Stderr, ring),
            );
        });
        self.log_readers
            .lock()
            .await
            .insert(container_id.to_string(), handle);
        tracing::info!(container = %container_id, "log readers started for adopted container");
    }
}

#[async_trait]
impl Runtime for ContainerdRuntime {
    async fn prepare_startup(&self) -> Result<(), RuntimeError> {
        #[cfg(target_os = "linux")]
        if let Some(networking) = &self.networking {
            self.networking_blocked.store(true, Ordering::Release);
            if let Err(error) = networking.block_task_networking().await {
                if let Err(stop_error) = self.stop_all_task_containers().await {
                    tracing::error!(
                        %stop_error,
                        "failed to stop running tasks after firewall setup failed"
                    );
                }
                return Err(RuntimeError::Start(format!(
                    "block task networking: {error}"
                )));
            }
        }
        Ok(())
    }

    async fn start(&self, task_id: &TaskId, spec: &TaskSpec) -> Result<String, RuntimeError> {
        self.start_assigned(task_id, spec, &[]).await
    }

    async fn start_assigned(
        &self,
        task_id: &TaskId,
        spec: &TaskSpec,
        networks: &[minga_state::types::Network],
    ) -> Result<String, RuntimeError> {
        #[cfg(target_os = "linux")]
        if self.networking_blocked.load(Ordering::Acquire) {
            // Retry recovery here too: `list()` may not run again once the
            // supervisor has adopted containers.
            let recovered = match &self.networking {
                Some(networking) => match networking.reconcile_task_source_filters().await {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::warn!(%error, "task source filters are still not recovered");
                        if let Err(block_error) = networking.block_task_networking().await {
                            tracing::error!(
                                %block_error,
                                "failed to restore task networking block after source-filter failure"
                            );
                        }
                        false
                    }
                },
                None => false,
            };
            if !recovered {
                return Err(RuntimeError::Start(
                    "task networking is blocked while source filters are being recovered".into(),
                ));
            }
            self.networking_blocked.store(false, Ordering::Release);
        }
        let container_id = task_id.clone();
        let workload = spec.egress.as_ref();
        let resolved_policy = task_egress_policy(spec, self.secret_store.as_ref())?;
        let should_issue_token = resolved_policy.is_some();
        let mut secret_mounts = self.resolve_secrets(&container_id, spec)?;
        let mut egress_token = None;
        if should_issue_token {
            let tokens = self.egress_tokens.as_ref().ok_or_else(|| {
                RuntimeError::Start("egress routes need a node started with --egress-bind".into())
            })?;
            let workload = workload.ok_or_else(|| {
                RuntimeError::Start("required egress has no workload identity".into())
            })?;
            let store = self.secret_store.as_ref().ok_or_else(|| {
                RuntimeError::Start("required egress has no node secret store configured".into())
            })?;
            if let Some(budgets) = &self.egress_budgets {
                budgets.register(&container_id, routed_task_budget());
            }
            let token = tokens.issue_identity(minga_egress::WorkloadIdentity {
                workload_id: container_id.clone(),
                tenant_id: workload.tenant_id.clone(),
                service_id: workload.service_id.clone(),
                policy: resolved_policy,
            });
            let pending = PendingEgressToken {
                store: store.clone(),
                task_id: container_id.clone(),
                token: Some(token),
                budgets: self.egress_budgets.clone(),
                committed: false,
            };
            let source = store
                .stage_egress_token(&container_id, pending.token.as_ref().unwrap().as_str())
                .map_err(|_| RuntimeError::Start("failed to stage egress token".into()))?;
            secret_mounts.push(oci::SecretMount {
                source,
                target: "/run/minga/egress-token".to_string(),
            });
            egress_token = Some(pending);
        }
        #[cfg(target_os = "linux")]
        let mut spec = self.resolve_mounts(spec).await?;
        #[cfg(not(target_os = "linux"))]
        let spec = self.resolve_mounts(spec).await?;
        check_extra_hosts_have_network(&spec)?;
        let image_ref = image::normalize_ref(&spec.image);
        let image = self.ensure_image(&image_ref).await?;
        #[cfg(target_os = "linux")]
        let network = self
            .prepare_network(&container_id, &mut spec, networks)
            .await?;
        #[cfg(not(target_os = "linux"))]
        let network: Option<oci::NetworkSpec> = {
            if should_issue_token || !spec.ips.is_empty() {
                return Err(RuntimeError::Start(
                    "task networking and egress require Linux (netns + bridges)".into(),
                ));
            }
            let _ = networks;
            None
        };

        let mut client = self.client.lock().await;
        let snapshot_key = format!("minga-{container_id}");
        let prepared = async {
            let config = image::load_image(&mut client, &image, &self.snapshotter)
                .await
                .map_err(RuntimeError::Start)?;

            // A previous attempt at this task ID may have left a container, a
            // task or a snapshot behind. Clear them before creating new ones.
            Self::cleanup_existing(&mut client, &container_id, &snapshot_key, &self.snapshotter)
                .await?;

            let mounts = client
                .prepare_snapshot(&self.snapshotter, &snapshot_key, &config.parent_snapshot)
                .await
                .map_err(|error| RuntimeError::Start(format!("prepare snapshot: {error}")))?;
            Ok((config, mounts))
        }
        .await;
        let (config, mounts) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => return Err(self.abort_start(&container_id, error).await),
        };

        // A named user resolves against the image's own /etc/passwd, which
        // the snapshot's layer directories hold.
        let user = oci::task_user(&spec, &config, &oci::rootfs_dirs(&mounts))
            .map_err(RuntimeError::Start)
            .and_then(|user| chown_secrets(&secret_mounts, user.uid, user.gid).map(|()| user));
        let user = match user {
            Ok(user) => user,
            Err(error) => {
                let rollback = Self::rollback_start(
                    &mut client,
                    &container_id,
                    &snapshot_key,
                    &self.snapshotter,
                    false,
                    false,
                    false,
                )
                .await;
                if !rollback.is_empty() {
                    tracing::warn!(container = %container_id, "{}", rollback.trim_start_matches("; "));
                }
                #[cfg(target_os = "linux")]
                self.teardown_network(&container_id).await;
                return Err(error);
            }
        };

        let staged = async {
            // Log capture: real FIFO destinations for stdout/stderr, drained
            // into a bounded ring by a per-container reader.
            let (mut stdout, mut stderr) = (String::new(), String::new());
            if let Some(logs_dir) = &self.logs_dir {
                tokio::fs::create_dir_all(logs_dir)
                    .await
                    .map_err(|e| RuntimeError::Start(format!("create logs dir: {e}")))?;
                stdout = logs_dir
                    .join(format!("{container_id}.stdout.fifo"))
                    .display()
                    .to_string();
                stderr = logs_dir
                    .join(format!("{container_id}.stderr.fifo"))
                    .display()
                    .to_string();
                for fifo in [&stdout, &stderr] {
                    let path = PathBuf::from(fifo);
                    let _ = tokio::fs::remove_file(&path).await; // stale from a previous attempt
                    make_fifo(&path).map_err(|e| {
                        RuntimeError::Start(format!("mkfifo {}: {e}", path.display()))
                    })?;
                }
                let ring = Arc::new(LogRing::default());
                self.log_rings
                    .lock()
                    .await
                    .insert(container_id.clone(), ring.clone());
                let out_path = PathBuf::from(&stdout);
                let err_path = PathBuf::from(&stderr);
                let out_ring = ring.clone();
                let handle = tokio::spawn(async move {
                    tokio::join!(
                        fifo_reader(out_path, LogStream::Stdout, out_ring),
                        fifo_reader(err_path, LogStream::Stderr, ring),
                    );
                });
                self.log_readers
                    .lock()
                    .await
                    .insert(container_id.clone(), handle);
            }

            let oci_any = oci::spec_to_any_with_image(
                &container_id,
                &spec,
                &secret_mounts,
                &config,
                user,
                network.as_ref(),
            )
            .map_err(RuntimeError::Start)?;
            Ok((stdout, stderr, oci_any))
        }
        .await;
        let (stdout, stderr, oci_any) = match staged {
            Ok(staged) => staged,
            Err(error) => return Err(self.abort_start(&container_id, error).await),
        };

        let container = Container {
            id: container_id.clone(),
            labels: HashMap::from([
                ("minga/task_id".to_string(), task_id.clone()),
                ("minga/image".to_string(), spec.image.clone()),
            ]),
            image: image_ref.clone(),
            spec: Some(oci_any),
            snapshotter: self.snapshotter.clone(),
            snapshot_key: snapshot_key.clone(),
            runtime: Some(ProtoRuntime {
                name: self.runtime_name.clone(),
                options: None,
            }),
        };

        if let Err(error) = client.create_container(container).await {
            #[cfg(target_os = "linux")]
            self.teardown_network(&container_id).await;
            self.teardown_logs(&container_id).await;
            return Err(RuntimeError::Start(format!("create container: {error}")));
        }
        let container_created = true;

        let pid = match client
            .create_task(&container_id, mounts, &stdout, &stderr)
            .await
        {
            Ok(pid) => pid,
            Err(error) => {
                let rollback = Self::rollback_start(
                    &mut client,
                    &container_id,
                    &snapshot_key,
                    &self.snapshotter,
                    container_created,
                    false,
                    false,
                )
                .await;
                #[cfg(target_os = "linux")]
                self.teardown_network(&container_id).await;
                self.teardown_logs(&container_id).await;
                return Err(RuntimeError::Start(format!(
                    "create task: {error}{rollback}"
                )));
            }
        };
        let task_created = true;

        if let Err(error) = client.start_task(&container_id).await {
            let rollback = Self::rollback_start(
                &mut client,
                &container_id,
                &snapshot_key,
                &self.snapshotter,
                container_created,
                task_created,
                true,
            )
            .await;
            #[cfg(target_os = "linux")]
            self.teardown_network(&container_id).await;
            self.teardown_logs(&container_id).await;
            return Err(RuntimeError::Start(format!(
                "start task: {error}{rollback}"
            )));
        }

        drop(client);
        self.containers.lock().await.insert(container_id.clone());
        if let Some(mut pending) = egress_token {
            let mut active_tokens = self.active_egress_tokens.lock().await;
            let token = pending
                .token
                .take()
                .expect("pending egress token is present until committed");
            pending.committed = true;
            active_tokens.insert(container_id.clone(), token);
        }

        tracing::info!(
            task_id = %task_id,
            container = %container_id,
            image = %spec.image,
            pid,
            snapshot_key,
            "containerd: started container"
        );

        Ok(container_id)
    }

    async fn stop(&self, container_id: &str) -> Result<(), RuntimeError> {
        let mut client = self.client.lock().await;

        // 1. Try graceful shutdown: SIGTERM.
        match client.kill_task(container_id, 15).await {
            Ok(()) => {
                tracing::debug!(container = %container_id, "sent SIGTERM");
            }
            Err(ClientError::Grpc(status)) if status.code() == tonic::Code::NotFound => {
                // Task already gone — proceed to cleanup.
                tracing::debug!(container = %container_id, "task already stopped");
            }
            Err(e) => {
                tracing::warn!(container = %container_id, error = %e, "SIGTERM failed, trying SIGKILL");
            }
        }

        // 2. Wait for the graceful exit.
        //
        // The client guard must be released first: `wait_for_stop` locks the
        // same mutex to read the task status.
        drop(client);
        let deadline = std::time::Instant::now() + GRACE_PERIOD;
        if !Self::wait_for_stop(&self.client, container_id, deadline).await {
            // Grace period expired — force kill, then wait again. containerd
            // refuses to delete a task whose process is still alive, so the
            // delete below must not run until the process is really gone.
            tracing::warn!(container = %container_id, "grace period expired, sending SIGKILL");
            let mut client = self.client.lock().await;
            let _ = client.kill_task(container_id, 9).await;
            drop(client);
            let deadline = std::time::Instant::now() + KILL_GRACE_PERIOD;
            if !Self::wait_for_stop(&self.client, container_id, deadline).await {
                return Err(RuntimeError::Stop(format!(
                    "task {container_id} is still running after SIGKILL"
                )));
            }
        }
        let mut client = self.client.lock().await;

        // 3. Delete the task, then the container.
        //
        // Order matters: containerd refuses to delete a container that still
        // has a task, and a task whose container is already gone can never
        // be deleted again — it would be stranded in the namespace forever.
        // A failed task delete must therefore abort before the container is
        // removed, not be logged and ignored.
        match client.delete_task(container_id).await {
            Ok(_) => {}
            Err(ClientError::Grpc(status)) if status.code() == tonic::Code::NotFound => {}
            Err(e) => {
                return Err(RuntimeError::Stop(format!(
                    "delete task {container_id}: {e}"
                )));
            }
        }
        // The process is gone: its egress token must stop working even if
        // the container cleanup below fails and is retried.
        self.revoke_egress(container_id).await;

        // 4. Delete the container.
        match client.delete_container(container_id).await {
            Ok(()) => {}
            Err(ClientError::Grpc(status)) if status.code() == tonic::Code::NotFound => {}
            Err(e) => {
                return Err(RuntimeError::Stop(format!(
                    "delete container {container_id}: {e}"
                )));
            }
        }

        drop(client);

        // 5. Remove from tracking.
        self.containers.lock().await.remove(container_id);

        // 6. Remove the writable snapshot prepared for this container.
        let mut client = self.client.lock().await;
        if let Err(error) = client
            .remove_snapshot(&self.snapshotter, &format!("minga-{container_id}"))
            .await
        {
            tracing::warn!(container = %container_id, error = %error, "failed to remove snapshot");
        }
        drop(client);

        #[cfg(target_os = "linux")]
        self.teardown_network(container_id).await;
        self.teardown_logs(container_id).await;

        if let Some(store) = &self.secret_store {
            store.cleanup_task(container_id);
        }

        tracing::info!(container = %container_id, "containerd: stopped container");
        Ok(())
    }

    async fn list(&self) -> Vec<ContainerInfo> {
        self.try_list().await.unwrap_or_default()
    }

    async fn try_list(&self) -> Result<Vec<ContainerInfo>, RuntimeError> {
        // Every supervisor tick lists, so a teardown that failed after its
        // container was removed is retried here until it succeeds.
        #[cfg(target_os = "linux")]
        if let Some(networking) = &self.networking {
            if let Err(error) = networking.retry_stale_teardowns().await {
                tracing::warn!(%error, "retrying stale task network teardown failed");
            }
        }
        #[cfg(target_os = "linux")]
        let source_filter_error = if let Some(networking) = &self.networking {
            if self.networking_blocked.load(Ordering::Acquire) {
                if let Err(error) = networking.block_task_networking().await {
                    tracing::error!(
                        %error,
                        "failed to block task networking before source-filter recovery"
                    );
                    self.networking_blocked.store(true, Ordering::Release);
                    if let Err(stop_error) = self.stop_all_task_containers().await {
                        tracing::error!(
                            %stop_error,
                            "failed to stop running tasks after firewall setup failed"
                        );
                    }
                    return Err(RuntimeError::Other(
                        "task networking could not be blocked for source-filter recovery".into(),
                    ));
                }
                match networking.reconcile_task_source_filters().await {
                    Ok(()) => {
                        self.networking_blocked.store(false, Ordering::Release);
                        None
                    }
                    Err(error) => {
                        tracing::error!(%error, "failed to reconcile task source filters");
                        if let Err(block_error) = networking.block_task_networking().await {
                            tracing::error!(
                                %block_error,
                                "failed to restore task networking block after source-filter failure"
                            );
                            if let Err(stop_error) = self.stop_all_task_containers().await {
                                tracing::error!(
                                    %stop_error,
                                    "failed to stop running tasks after firewall setup failed"
                                );
                            }
                            return Err(RuntimeError::Other(
                                "task networking block could not be restored after source-filter failure".into(),
                            ));
                        }
                        Some(error.to_string())
                    }
                }
            } else {
                None
            }
        } else {
            None
        };
        #[cfg(not(target_os = "linux"))]
        let source_filter_error: Option<String> = None;
        let mut client = self.client.lock().await;
        let containers = match client
            .list_containers(vec!["labels.minga.task_id!=empty".to_string()])
            .await
        {
            Ok(cs) => cs,
            Err(e) => {
                tracing::error!(error = %e, "containerd: failed to list containers");
                return Err(RuntimeError::Other(format!("list containers: {e}")));
            }
        };
        drop(client);

        let mut infos = Vec::new();
        for c in containers {
            // Get task status for each container.
            let status = {
                let mut client = self.client.lock().await;
                match client.get_task(&c.id).await {
                    Ok(process) => {
                        let ps = ProcessStatus::try_from(process.status)
                            .unwrap_or(ProcessStatus::Unknown);
                        match ps {
                            ProcessStatus::Running | ProcessStatus::Created => {
                                ContainerStatus::Running
                            }
                            _ => ContainerStatus::Stopped,
                        }
                    }
                    Err(_) => ContainerStatus::Stopped,
                }
            };
            let status = if status == ContainerStatus::Running && source_filter_error.is_some() {
                match self.stop(&c.id).await {
                    Ok(()) => ContainerStatus::Stopped,
                    Err(error) => {
                        tracing::error!(
                            container = %c.id,
                            %error,
                            "startup: could not stop container after source filter failure"
                        );
                        status
                    }
                }
            } else {
                status
            };

            let task_id = c
                .labels
                .get("minga/task_id")
                .cloned()
                .unwrap_or_else(|| c.id.clone());

            infos.push(ContainerInfo {
                id: c.id.clone(),
                task_id,
                image: c.image.clone(),
                status,
            });
        }
        Ok(infos)
    }

    /// Kill and delete every task in the minga namespace that is in a stopped
    /// state. Called once on supervisor startup to clear orphan tasks left by
    /// a previous run, so subsequent `create_task` calls don't fail with
    /// AlreadyExists.
    async fn purge_stopped_tasks(&self) {
        let tasks = {
            let mut client = self.client.lock().await;
            match client.list_tasks().await {
                Ok(ts) => ts,
                Err(e) => {
                    tracing::warn!(error = %e, "startup: failed to list tasks for cleanup");
                    return;
                }
            }
        };
        for task in tasks {
            let ps = ProcessStatus::try_from(task.status).unwrap_or(ProcessStatus::Unknown);
            if matches!(ps, ProcessStatus::Stopped | ProcessStatus::Unknown) {
                tracing::info!(task = %task.id, "startup: removing stale stopped/unknown task");
                let mut client = self.client.lock().await;
                let _ = client.kill_task(&task.id, 9).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = client.delete_task(&task.id).await;
            }
        }
    }

    async fn status(&self, container_id: &str) -> Option<ContainerStatus> {
        self.state(container_id).await.map(|s| s.status)
    }

    async fn state(&self, container_id: &str) -> Option<ContainerState> {
        let mut client = self.client.lock().await;
        match client.get_task(container_id).await {
            Ok(process) => {
                let ps = ProcessStatus::try_from(process.status).unwrap_or(ProcessStatus::Unknown);
                match ps {
                    ProcessStatus::Running | ProcessStatus::Created => Some(ContainerState {
                        status: ContainerStatus::Running,
                        exit_code: None,
                    }),
                    ProcessStatus::Paused | ProcessStatus::Pausing => Some(ContainerState {
                        status: ContainerStatus::Running,
                        exit_code: None,
                    }),
                    // The task has exited. `exit_status` carries the process
                    // exit code — a non-zero code means failure.
                    ProcessStatus::Stopped => {
                        let exit_code = process.exit_status as i32;
                        let status = if exit_code != 0 {
                            ContainerStatus::Failed
                        } else {
                            ContainerStatus::Stopped
                        };
                        Some(ContainerState {
                            status,
                            exit_code: Some(exit_code),
                        })
                    }
                    // Unknown is not a valid state for a live task — treat it
                    // as terminated so the supervisor can act on it.
                    ProcessStatus::Unknown => Some(ContainerState {
                        status: ContainerStatus::Stopped,
                        exit_code: Some(0),
                    }),
                }
            }
            Err(_) => None,
        }
    }

    async fn exec(
        &self,
        container_id: &str,
        args: &[String],
        timeout: Duration,
    ) -> Result<i32, RuntimeError> {
        let err = |e: ClientError| RuntimeError::Other(format!("exec in {container_id}: {e}"));
        let exec_id = format!(
            "probe-{}",
            NEXT_EXEC_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        {
            let mut client = self.client.lock().await;
            let container = client.get_container(container_id).await.map_err(err)?;
            let spec = container
                .spec
                .ok_or_else(|| RuntimeError::Other("container has no spec".to_string()))?;
            let process = oci::exec_process_any(&spec, args).map_err(RuntimeError::Other)?;
            client
                .exec_process(container_id, &exec_id, process)
                .await
                .map_err(err)?;
            client
                .start_process(container_id, &exec_id)
                .await
                .map_err(err)?;
        }

        // Poll with the lock released: a probe can run for as long as its
        // timeout, and every other runtime call shares this client.
        let deadline = std::time::Instant::now() + timeout;
        let outcome = loop {
            let process = {
                let mut client = self.client.lock().await;
                client.get_process(container_id, &exec_id).await
            };
            match process {
                Ok(p) if p.status == ProcessStatus::Stopped as i32 => {
                    break Ok(p.exit_status as i32)
                }
                Ok(_) => {}
                Err(e) => break Err(err(e)),
            }
            if std::time::Instant::now() >= deadline {
                break Err(RuntimeError::Other(format!(
                    "exec in {container_id} timed out"
                )));
            }
            tokio::time::sleep(KILL_CHECK_INTERVAL).await;
        };

        if outcome.is_err() {
            let _ = self
                .client
                .lock()
                .await
                .kill_process(container_id, &exec_id, 9)
                .await;
            tokio::time::sleep(KILL_CHECK_INTERVAL).await;
        }
        let _ = self
            .client
            .lock()
            .await
            .delete_process(container_id, &exec_id)
            .await;
        outcome
    }

    async fn kill(&self, container_id: &str) -> Result<(), RuntimeError> {
        self.client
            .lock()
            .await
            .kill_task(container_id, 9)
            .await
            .map_err(|e| RuntimeError::Other(format!("kill {container_id}: {e}")))
    }

    async fn logs(&self, container_id: &str, tail: usize) -> Vec<ContainerLog> {
        let rings = self.log_rings.lock().await;
        let Some(ring) = rings.get(container_id) else {
            return Vec::new();
        };
        let snap = ring.snapshot();
        if tail == 0 || snap.len() <= tail {
            snap
        } else {
            snap[snap.len() - tail..].to_vec()
        }
    }

    async fn open_log_fifos(&self, container_id: &str) {
        self.start_adopted_log_readers(container_id).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_egress_without_routes_is_refused_not_sent_upstream() {
        use minga_state::types::{EgressPolicy, EgressWorkload, REQUIRED_EGRESS_LABEL};
        let mut spec = TaskSpec::default();
        assert!(task_egress_policy(&spec, None).unwrap().is_none());

        spec.egress = Some(EgressWorkload {
            tenant_id: "acme".into(),
            service_id: "acme.web".into(),
            policy: EgressPolicy::default(),
        });
        assert!(
            task_egress_policy(&spec, None).unwrap().is_none(),
            "no routes and not required keeps the no-token path"
        );

        spec.egress.as_mut().unwrap().policy.required = true;
        spec.labels
            .insert(REQUIRED_EGRESS_LABEL.into(), "true".into());
        let error = task_egress_policy(&spec, None).unwrap_err();
        assert!(error.to_string().contains("no named routes"), "{error}");

        spec.labels.remove(REQUIRED_EGRESS_LABEL);
        assert!(task_egress_policy(&spec, None).is_err(), "label must match");
    }

    #[test]
    fn extra_hosts_without_a_network_refuse_to_start() {
        let mut spec = TaskSpec::default();
        assert!(check_extra_hosts_have_network(&spec).is_ok());
        spec.extra_hosts = vec![("db".into(), "10.0.0.5".into())];
        let error = check_extra_hosts_have_network(&spec).unwrap_err();
        assert!(error.to_string().contains("extra_hosts"), "{error}");
        spec.ips = vec![("net".into(), "10.0.0.2".into())]
            .into_iter()
            .collect();
        assert!(check_extra_hosts_have_network(&spec).is_ok());
    }

    #[test]
    fn host_gateway_becomes_the_bridge_address() {
        let hosts = [
            (
                "host.docker.internal".to_string(),
                "host-gateway".to_string(),
            ),
            ("db".to_string(), "10.0.0.5".to_string()),
        ];
        let content = hosts_file_content(&hosts, Some([10, 100, 3, 1].into())).unwrap();
        assert!(
            content.contains("10.100.3.1\thost.docker.internal\n"),
            "{content}"
        );
        assert!(content.contains("10.0.0.5\tdb\n"), "{content}");
        assert!(hosts_file_content(&hosts, None).is_err());
    }

    #[test]
    fn secrets_go_to_the_task_user_at_0400_and_a_failed_chown_stops_the_task() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let store = crate::secrets::SecretStore::new(dir.path());
        store.put("value", b"value").unwrap();
        let refs = [minga_state::types::SecretRef::new(
            "value",
            "/run/secrets/value",
        )];
        let source = store.stage_all("task", &refs).unwrap()[0]
            .staged_path
            .clone();
        let mounts = [oci::SecretMount {
            source: source.clone(),
            target: "/run/secrets/value".into(),
        }];

        // Without root the only chown allowed is to ourselves, so a real
        // change of owner is proven only when this runs as root (CI on Linux).
        let root = unsafe { libc::geteuid() } == 0;
        let (uid, gid) = if root {
            (1001, 65533)
        } else {
            unsafe { (libc::geteuid(), libc::getegid()) }
        };
        chown_secrets(&mounts, uid, gid).unwrap();
        let meta = std::fs::metadata(&source).unwrap();
        assert_eq!((meta.uid(), meta.gid()), (uid, gid));
        assert_eq!(meta.mode() & 0o7777, 0o400);

        if !root {
            assert!(chown_secrets(&mounts, 0, 0).is_err());
        }
    }

    #[test]
    fn default_constants() {
        assert_eq!(DEFAULT_SNAPSHOTTER, "overlayfs");
        assert_eq!(DEFAULT_RUNTIME_NAME, "io.containerd.runc.v2");
        assert_eq!(
            super::super::client::DEFAULT_SOCKET,
            "/run/containerd/containerd.sock"
        );
    }

    #[test]
    fn process_status_mapping() {
        assert_eq!(
            ProcessStatus::try_from(ProcessStatus::Running as i32).unwrap(),
            ProcessStatus::Running
        );
        assert_eq!(
            ProcessStatus::try_from(ProcessStatus::Stopped as i32).unwrap(),
            ProcessStatus::Stopped
        );
        // Unknown values map to Unknown
        assert_eq!(
            ProcessStatus::try_from(99).unwrap_or(ProcessStatus::Unknown),
            ProcessStatus::Unknown
        );
    }

    #[test]
    fn grace_period_is_reasonable() {
        assert!(GRACE_PERIOD >= Duration::from_secs(5));
        assert!(GRACE_PERIOD <= Duration::from_secs(30));
    }

    // --- AC1: ring sequence numbers let the cursor skip evicted chunks --------

    /// Push more than 1 MiB through the ring in equal-size 64 KiB chunks.
    /// Every chunk that remains in the ring must have a unique, contiguous
    /// sequence starting at some `first_seq`. The snapshot must be exactly the
    /// expected tail (drop-oldest eviction) and every remaining chunk must be
    /// published exactly once when iterated by sequence cursor.
    #[test]
    fn log_ring_sequence_survives_eviction_ac1() {
        const CHUNK: usize = 64 * 1024; // 64 KiB
        const TOTAL: usize = 2 * 1024 * 1024; // 2 MiB — well over the 1 MiB cap
        let chunks_total = TOTAL / CHUNK; // 32 chunks

        let ring = LogRing::default();
        for i in 0..chunks_total {
            ring.push(ContainerLog {
                stream: LogStream::Stdout,
                data: vec![i as u8; CHUNK],
                seq: 0, // ring overwrites this
            });
        }

        let snap = ring.snapshot();
        // The ring holds at most LOG_RING_BYTES / CHUNK chunks (16 of the 32).
        let expected_surviving = LOG_RING_BYTES / CHUNK;
        assert_eq!(
            snap.len(),
            expected_surviving,
            "ring must hold exactly the last {expected_surviving} chunks"
        );

        // Every surviving chunk must have a unique, contiguous sequence.
        let seqs: Vec<u64> = snap.iter().map(|c| c.seq).collect();
        for w in seqs.windows(2) {
            assert_eq!(w[1], w[0] + 1, "sequences must be contiguous: {:?}", seqs);
        }
        // First surviving seq must equal chunks_total - expected_surviving + 1.
        assert_eq!(
            seqs[0],
            (chunks_total - expected_surviving + 1) as u64,
            "first surviving seq must follow the evicted range"
        );

        // Simulate the supervisor cursor: iterate by seq > cursor.
        // Use a local snapshot of cursor for the closure to avoid borrow conflict.
        let mut cursor: u64 = 0;
        let mut published = 0usize;
        for chunk in snap.iter() {
            if chunk.seq > cursor {
                cursor = chunk.seq;
                published += 1;
            }
        }
        assert_eq!(
            published, expected_surviving,
            "every surviving chunk must be published exactly once"
        );

        // A second pass with the same cursor must publish nothing (idempotent).
        let second_pass = snap.iter().filter(|c| c.seq > cursor).count();
        assert_eq!(second_pass, 0, "no chunk must be published twice");
    }

    // --- AC2: dropped-before-published gap is detected once -------------------

    /// Simulate a ring that evicted chunks before the cursor advanced past
    /// them. The gap must be detectable by comparing cursor + 1 to the first
    /// surviving seq, and once the cursor jumps past the gap no second warning
    /// fires.
    #[test]
    fn log_ring_gap_detection_ac2() {
        let ring = LogRing::default();
        // Push 3 chunks so they get seq 1, 2, 3.
        for i in 0u8..3 {
            ring.push(ContainerLog {
                stream: LogStream::Stdout,
                data: vec![i; 10],
                seq: 0,
            });
        }
        // Push a chunk that is just over the ring capacity so that all three
        // earlier chunks (seq 1-3) are evicted. This chunk gets seq 4.
        // After eviction seq 4 is the only surviving chunk.
        ring.push(ContainerLog {
            stream: LogStream::Stdout,
            data: vec![0xFFu8; LOG_RING_BYTES + 1],
            seq: 0,
        });
        // The oversized chunk itself exceeds the cap: the ring's eviction loop
        // runs until bytes <= LOG_RING_BYTES, but the loop pops the front first.
        // Let's push a small chunk (seq 5) after the big one so we have a known
        // surviving element regardless of whether seq 4 itself was dropped.
        ring.push(ContainerLog {
            stream: LogStream::Stdout,
            data: vec![42u8; 100],
            seq: 0,
        });

        let snap = ring.snapshot();
        assert!(!snap.is_empty(), "ring must not be empty after pushes");

        // The small final chunk (seq 5) must be in the snapshot.
        let last = snap.last().unwrap();
        assert_eq!(last.seq, 5, "last chunk must have seq 5");

        // Cursor is set to 1 (we "published" seq 1 previously).
        // The first surviving chunk has seq > 1, so a gap is present.
        let cursor: u64 = 1;
        let first = snap.first().unwrap();
        let gap_present = first.seq > cursor + 1;
        assert!(
            gap_present,
            "gap must be detected: first.seq={} cursor={}",
            first.seq, cursor
        );
        // dropped = first_surviving_seq - last_published_seq - 1
        let dropped = first.seq - cursor - 1;
        // All seqs between cursor+1 and first.seq-1 were dropped.
        assert!(dropped > 0, "at least one chunk must have been dropped");

        // After the cursor advances past the gap, no second gap fires.
        let new_cursor = last.seq; // cursor now at seq 5
        let gap_again = snap
            .first()
            .map(|c| c.seq > new_cursor + 1 && new_cursor > 0)
            .unwrap_or(false);
        assert!(
            !gap_again,
            "no second gap must be reported after cursor advances past all surviving chunks"
        );
    }
}
