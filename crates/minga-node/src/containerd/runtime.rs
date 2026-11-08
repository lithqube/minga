//! Containerd-backed [`Runtime`] implementation.
//!
//! Talks to a containerd daemon over its gRPC Unix socket. Each Minga
//! task maps to a containerd container + task pair.
//!
//! # Container lifecycle
//!
//! ```text
//! start(task_id, spec):
//!   1. Require an image preloaded and unpacked in namespace `minga`
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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use minga_state::types::{TaskId, TaskSpec};
use tokio::sync::Mutex;

use super::client::{ClientError, ContainerdClient};
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
const DEFAULT_SNAPSHOTTER: &str = "overlayfs";

/// Default containerd runtime (runc v2).
const DEFAULT_RUNTIME_NAME: &str = "io.containerd.runc.v2";

/// Maximum captured log bytes per task (drop-oldest beyond this).
const LOG_RING_BYTES: usize = 1024 * 1024;

/// A bounded, append-only log capture for one container.
#[derive(Default)]
struct LogRing {
    inner: std::sync::Mutex<(usize, std::collections::VecDeque<ContainerLog>)>,
}

impl LogRing {
    fn push(&self, log: ContainerLog) {
        let mut guard = self.inner.lock().unwrap();
        let (bytes, deque) = &mut *guard;
        *bytes += log.data.len();
        deque.push_back(log);
        while *bytes > LOG_RING_BYTES {
            if let Some(old) = deque.pop_front() {
                *bytes -= old.data.len();
            } else {
                break;
            }
        }
    }

    fn snapshot(&self) -> Vec<ContainerLog> {
        self.inner.lock().unwrap().1.iter().cloned().collect()
    }
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
    /// Node networking: netns/bridge/port setup, plus cluster state for
    /// network subnet resolution. Required to start tasks with IPs.
    #[cfg(target_os = "linux")]
    networking: Option<(
        Arc<crate::netns::NetworkSetup>,
        Arc<minga_state::StateStore>,
    )>,
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
    pub fn with_networking(
        mut self,
        setup: Arc<crate::netns::NetworkSetup>,
        state: Arc<minga_state::StateStore>,
    ) -> Self {
        self.networking = Some((setup, state));
        self
    }

    /// Prepares the task's network namespace when the task has IPs.
    #[cfg(target_os = "linux")]
    async fn prepare_network(
        &self,
        task_id: &str,
        spec: &TaskSpec,
    ) -> Result<Option<oci::NetworkSpec>, RuntimeError> {
        if spec.ips.is_empty() {
            return Ok(None);
        }
        let (setup, state) = self.networking.as_ref().ok_or_else(|| {
            RuntimeError::Start(format!(
                "task has IP assignments on {} network(s) but node networking is not configured",
                spec.ips.len()
            ))
        })?;

        let networks = state
            .list_networks()
            .await
            .map_err(|e| RuntimeError::Start(format!("list networks: {e}")))?;

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
        // Deterministic primary network (default route + resolver).
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

        tracing::info!(
            task_id = %task_id,
            netns = %netns.netns_path.display(),
            ips = ?spec.ips,
            "containerd: task network prepared"
        );

        Ok(Some(oci::NetworkSpec {
            netns_path: netns.netns_path.display().to_string(),
            resolv_conf: Some(netns.resolv_conf),
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

    /// Tears down a task's network, tolerating its absence.
    #[cfg(target_os = "linux")]
    async fn teardown_network(&self, task_id: &str) {
        if let Some((setup, _)) = &self.networking {
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
                    tokio::fs::create_dir_all(&m.source).await.map_err(|e| {
                        RuntimeError::Start(format!("create bind source {}: {e}", m.source))
                    })?;
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

    /// Require an image already available in Minga's containerd namespace.
    ///
    /// The Images service has no Pull RPC. Image retrieval and unpacking
    /// must be performed by an explicit provisioning step until Minga has a
    /// supported registry/transfer implementation.
    async fn ensure_image(&self, image_ref: &str) -> Result<Image, RuntimeError> {
        let mut client = self.client.lock().await;
        match client.get_image(image_ref).await {
            Ok(Some(image)) => Ok(image),
            Ok(None) => Err(RuntimeError::Start(format!(
                "image {image_ref} is not preloaded in the minga containerd namespace; preload and unpack it before starting this task"
            ))),
            Err(e) => Err(RuntimeError::Start(format!("check image {image_ref}: {e}"))),
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
}

#[async_trait]
impl Runtime for ContainerdRuntime {
    async fn start(&self, task_id: &TaskId, spec: &TaskSpec) -> Result<String, RuntimeError> {
        let container_id = task_id.clone();
        let secret_mounts = self.resolve_secrets(&container_id, spec)?;
        let spec = &self.resolve_mounts(spec).await?;

        #[cfg(target_os = "linux")]
        let network = self.prepare_network(&container_id, spec).await?;
        #[cfg(not(target_os = "linux"))]
        let network: Option<oci::NetworkSpec> = {
            if !spec.ips.is_empty() {
                return Err(RuntimeError::Start(
                    "task networking requires Linux (netns + bridges)".into(),
                ));
            }
            None
        };

        // containerd keys images by fully-qualified reference; Compose files
        // habitually use the short form (`nginx:1.27`).
        let image_ref = image::normalize_ref(&spec.image);
        let image = self.ensure_image(&image_ref).await?;

        let mut client = self.client.lock().await;
        let config = image::load_image(&mut client, &image, &self.snapshotter)
            .await
            .map_err(RuntimeError::Start)?;

        let snapshot_key = format!("minga-{container_id}");

        // A previous attempt at this task ID may have left a container, a
        // task or a snapshot behind. Clear them before creating new ones.
        Self::cleanup_existing(&mut client, &container_id, &snapshot_key, &self.snapshotter)
            .await?;

        let mounts = client
            .prepare_snapshot(&self.snapshotter, &snapshot_key, &config.parent_snapshot)
            .await
            .map_err(|error| RuntimeError::Start(format!("prepare snapshot: {error}")))?;

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
                make_fifo(&path)
                    .map_err(|e| RuntimeError::Start(format!("mkfifo {}: {e}", path.display())))?;
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
            spec,
            &secret_mounts,
            &config,
            network.as_ref(),
        )
        .map_err(RuntimeError::Start)?;

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
        let mut client = self.client.lock().await;
        let containers = match client
            .list_containers(vec!["labels.minga.task_id!=empty".to_string()])
            .await
        {
            Ok(cs) => cs,
            Err(e) => {
                tracing::error!(error = %e, "containerd: failed to list containers");
                return vec![];
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
                            ProcessStatus::Running => ContainerStatus::Running,
                            ProcessStatus::Created => ContainerStatus::Running,
                            _ => ContainerStatus::Stopped,
                        }
                    }
                    Err(_) => ContainerStatus::Stopped,
                }
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
        infos
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
