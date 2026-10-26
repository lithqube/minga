//! Containerd-backed [`Runtime`] implementation.
//!
//! Talks to a containerd daemon over its gRPC Unix socket. Each Minga
//! task maps to a containerd container + task pair.
//!
//! # Container lifecycle
//!
//! ```text
//! start(task_id, spec):
//!   1. Ensure image is pulled (ImageService.Get → Pull if missing)
//!   2. Generate OCI spec from TaskSpec
//!   3. Create container (ContainerService.Create) with OCI spec as Any
//!   4. Create task (TaskService.Create) from the container
//!   5. Start task (TaskService.Start)
//!   → returns container_id (= task_id)
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
use super::oci;
use super::proto::{Container, ProcessStatus, Runtime as ProtoRuntime};
use crate::runtime::{ContainerInfo, ContainerStatus, Runtime, RuntimeError};

/// Grace period after SIGTERM before sending SIGKILL.
const GRACE_PERIOD: Duration = Duration::from_secs(10);

/// How long to wait between SIGTERM and checking if the process exited.
const KILL_CHECK_INTERVAL: Duration = Duration::from_millis(500);

/// Default containerd snapshotter.
const DEFAULT_SNAPSHOTTER: &str = "overlayfs";

/// Default containerd runtime (runc v2).
const DEFAULT_RUNTIME_NAME: &str = "io.containerd.runc.v2";

/// A container runtime backed by containerd.
pub struct ContainerdRuntime {
    client: Arc<Mutex<ContainerdClient>>,
    /// Track which container IDs we've started (for status lookups).
    containers: Arc<Mutex<HashSet<String>>>,
    snapshotter: String,
    runtime_name: String,
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
        }
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

    /// Ensure the image exists; pull it if missing.
    async fn ensure_image(&self, image_ref: &str) -> Result<(), RuntimeError> {
        let mut client = self.client.lock().await;

        match client.get_image(image_ref).await {
            Ok(Some(_)) => {
                tracing::debug!(image = %image_ref, "image already present");
                Ok(())
            }
            Ok(None) => {
                tracing::info!(image = %image_ref, "pulling image");
                client
                    .pull_image(image_ref)
                    .await
                    .map_err(|e| RuntimeError::Start(format!("pull {image_ref}: {e}")))?;
                tracing::info!(image = %image_ref, "image pulled");
                Ok(())
            }
            Err(e) => Err(RuntimeError::Start(format!("check image {image_ref}: {e}"))),
        }
    }
}

#[async_trait]
impl Runtime for ContainerdRuntime {
    async fn start(&self, task_id: &TaskId, spec: &TaskSpec) -> Result<String, RuntimeError> {
        // Use the task_id as the container ID for traceability.
        let container_id = task_id.clone();

        // 1. Ensure image is available.
        self.ensure_image(&spec.image).await?;

        // 2. Generate OCI spec.
        let oci_any = oci::spec_to_any(&container_id, spec).map_err(RuntimeError::Start)?;

        // 3. Create the containerd container.
        let container = Container {
            id: container_id.clone(),
            labels: HashMap::from([
                ("minga/task_id".to_string(), task_id.clone()),
                ("minga/image".to_string(), spec.image.clone()),
            ]),
            image: spec.image.clone(),
            spec: Some(oci_any),
            snapshotter: self.snapshotter.clone(),
            snapshot_key: String::new(), // containerd assigns this
            runtime: Some(ProtoRuntime {
                name: self.runtime_name.clone(),
                options: None,
            }),
        };

        let mut client = self.client.lock().await;
        client
            .create_container(container)
            .await
            .map_err(|e| RuntimeError::Start(format!("create container: {e}")))?;
        drop(client);

        // 4. Create the task.
        let mut client = self.client.lock().await;
        let pid = client
            .create_task(&container_id)
            .await
            .map_err(|e| RuntimeError::Start(format!("create task: {e}")))?;

        // 5. Start the task.
        client
            .start_task(&container_id)
            .await
            .map_err(|e| RuntimeError::Start(format!("start task: {e}")))?;
        drop(client);

        // Track the container.
        self.containers.lock().await.insert(container_id.clone());

        tracing::info!(
            task_id = %task_id,
            container = %container_id,
            image = %spec.image,
            pid,
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

        // 2. Wait for graceful exit (poll task status).
        let deadline = std::time::Instant::now() + GRACE_PERIOD;
        loop {
            match client.get_task(container_id).await {
                Ok(process) => {
                    let status =
                        ProcessStatus::try_from(process.status).unwrap_or(ProcessStatus::Unknown);
                    if status == ProcessStatus::Stopped {
                        break;
                    }
                }
                Err(ClientError::Grpc(status)) if status.code() == tonic::Code::NotFound => {
                    break; // task is gone
                }
                Err(_) => break,
            }

            if std::time::Instant::now() >= deadline {
                // Grace period expired — force kill.
                tracing::warn!(container = %container_id, "grace period expired, sending SIGKILL");
                let _ = client.kill_task(container_id, 9).await;
                break;
            }
            drop(client);
            tokio::time::sleep(KILL_CHECK_INTERVAL).await;
            client = self.client.lock().await;
        }

        // 3. Delete the task.
        match client.delete_task(container_id).await {
            Ok(_) => {}
            Err(ClientError::Grpc(status)) if status.code() == tonic::Code::NotFound => {}
            Err(e) => {
                tracing::warn!(container = %container_id, error = %e, "failed to delete task");
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

        tracing::info!(container = %container_id, "containerd: stopped container");
        Ok(())
    }

    async fn list(&self) -> Vec<ContainerInfo> {
        let mut client = self.client.lock().await;
        let containers = match client
            .list_containers(vec!["labels.minga/task_id exists".to_string()])
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
        // Check if we're tracking it.
        {
            let tracked = self.containers.lock().await;
            if !tracked.contains(container_id) {
                // Might still exist in containerd (e.g. after restart).
                // Fall through to query containerd directly.
            }
        }

        let mut client = self.client.lock().await;
        match client.get_task(container_id).await {
            Ok(process) => {
                let ps = ProcessStatus::try_from(process.status).unwrap_or(ProcessStatus::Unknown);
                Some(match ps {
                    ProcessStatus::Running | ProcessStatus::Created => ContainerStatus::Running,
                    ProcessStatus::Stopped | ProcessStatus::Unknown => ContainerStatus::Stopped,
                    ProcessStatus::Paused | ProcessStatus::Pausing | ProcessStatus::Resuming => {
                        ContainerStatus::Running
                    }
                })
            }
            Err(_) => None,
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
