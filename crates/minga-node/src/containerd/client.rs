//! Low-level gRPC client for the containerd API.
//!
//! Connects to containerd's gRPC socket (Unix domain socket by default)
//! and provides typed methods for the ContainerService, TaskService, and
//! ImageService RPCs that Minga uses.
//!
//! Uses `tonic` for transport and `prost` for serialization. The message
//! types are defined in [`super::proto`].

use std::path::Path;

use hyper_util::rt::TokioIo;
use thiserror::Error;
use tonic::client::Grpc;
use tonic::codec::ProstCodec;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;
use tower::service_fn;

use prost::Message as _;

use super::credentials::Credential;
use super::image::BlobDescriptor;
use super::proto::*;

/// Makes each pull's credential stream name unique within the process.
static NEXT_STREAM: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("failed to connect to containerd at {0}: {1}")]
    Connect(String, String),
    #[error("gRPC call failed: {0}")]
    Grpc(Box<tonic::Status>),
    #[error("containerd returned an error for {method}: {message}")]
    Rpc { method: String, message: String },
    #[error("image not found: {0}")]
    ImageNotFound(String),
    #[error("container not found: {0}")]
    ContainerNotFound(String),
    #[error("task not found for container: {0}")]
    TaskNotFound(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<tonic::Status> for ClientError {
    fn from(status: tonic::Status) -> Self {
        ClientError::Grpc(Box::new(status))
    }
}

/// A client connected to a containerd daemon. Cloning shares the connection.
#[derive(Clone)]
pub struct ContainerdClient {
    channel: Channel,
    socket: Option<std::path::PathBuf>,
}

/// Default containerd socket path.
pub const DEFAULT_SOCKET: &str = "/run/containerd/containerd.sock";

impl ContainerdClient {
    /// Connect to containerd via a Unix domain socket.
    pub async fn connect(
        socket_path: impl AsRef<Path> + Send + 'static,
    ) -> Result<Self, ClientError> {
        let socket_path = socket_path.as_ref().to_path_buf();
        let path_str = socket_path.display().to_string();
        let socket = socket_path.clone();

        let channel = Endpoint::from_static("http://localhost")
            .connect_with_connector(service_fn(move |_| {
                let path = socket_path.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(&path).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }))
            .await
            .map_err(|e| ClientError::Connect(path_str.clone(), e.to_string()))?;

        tracing::debug!(socket = %path_str, "connected to containerd");
        Ok(Self {
            channel,
            socket: Some(socket),
        })
    }

    /// Create a client from an existing channel (for testing).
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            channel,
            socket: None,
        }
    }

    /// The Unix socket this client connected to, if it connected by path.
    pub fn socket(&self) -> Option<&Path> {
        self.socket.as_deref()
    }

    // --- ContainerService ---------------------------------------------------

    /// Create a container with the given spec.
    pub async fn create_container(
        &mut self,
        container: Container,
    ) -> Result<Container, ClientError> {
        let request = CreateContainerRequest {
            container: Some(container),
        };
        let response: CreateContainerResponse =
            self.unary(request, paths::CONTAINER_CREATE).await?;
        response.container.ok_or_else(|| ClientError::Rpc {
            method: "ContainerService/Create".to_string(),
            message: "response missing container".to_string(),
        })
    }

    /// Get a container by ID.
    pub async fn get_container(&mut self, id: &str) -> Result<Container, ClientError> {
        let request = GetContainerRequest { id: id.to_string() };
        let response: GetContainerResponse = self.unary(request, paths::CONTAINER_GET).await?;
        response
            .container
            .ok_or_else(|| ClientError::ContainerNotFound(id.to_string()))
    }

    /// List all containers, optionally filtered.
    pub async fn list_containers(
        &mut self,
        filters: Vec<String>,
    ) -> Result<Vec<Container>, ClientError> {
        let request = ListContainersRequest { filters };
        let response: ListContainersResponse = self.unary(request, paths::CONTAINER_LIST).await?;
        Ok(response.containers)
    }

    /// Delete a container by ID.
    pub async fn delete_container(&mut self, id: &str) -> Result<(), ClientError> {
        let request = DeleteContainerRequest { id: id.to_string() };
        let _: DeleteContainerResponse = self.unary(request, paths::CONTAINER_DELETE).await?;
        Ok(())
    }

    // --- Tasks service ------------------------------------------------------

    /// Create a task from a container. Returns the PID.
    ///
    /// `stdout`/`stderr` are FIFO paths the shim writes container output to.
    /// Pass empty strings to discard the output.
    pub async fn create_task(
        &mut self,
        container_id: &str,
        rootfs: Vec<Mount>,
        stdout: &str,
        stderr: &str,
    ) -> Result<u32, ClientError> {
        let request = CreateTaskRequest {
            container_id: container_id.to_string(),
            rootfs,
            stdin: String::new(),
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
            terminal: false,
            checkpoint: None,
            options: None,
            runtime_path: String::new(),
        };
        let response: CreateTaskResponse = self.unary(request, paths::TASK_CREATE).await?;
        Ok(response.pid)
    }

    /// Start a task. Returns the PID.
    pub async fn start_task(&mut self, container_id: &str) -> Result<u32, ClientError> {
        let request = StartRequest {
            container_id: container_id.to_string(),
            exec_id: String::new(),
        };
        let response: StartResponse = self.unary(request, paths::TASK_START).await?;
        Ok(response.pid)
    }

    /// Kill a task with the given signal (e.g. 15 for SIGTERM, 9 for SIGKILL).
    pub async fn kill_task(&mut self, container_id: &str, signal: u32) -> Result<(), ClientError> {
        let request = KillRequest {
            container_id: container_id.to_string(),
            exec_id: String::new(),
            signal,
            all: false,
        };
        let _: KillResponse = self.unary(request, paths::TASK_KILL).await?;
        Ok(())
    }

    /// Delete a task. Returns the exit info.
    pub async fn delete_task(
        &mut self,
        container_id: &str,
    ) -> Result<DeleteTaskResponse, ClientError> {
        let request = DeleteTaskRequest {
            container_id: container_id.to_string(),
        };
        let response: DeleteTaskResponse = self.unary(request, paths::TASK_DELETE).await?;
        Ok(response)
    }

    /// Get the status of a task.
    pub async fn get_task(&mut self, container_id: &str) -> Result<Process, ClientError> {
        let request = GetTaskRequest {
            container_id: container_id.to_string(),
            exec_id: String::new(),
        };
        let response: GetTaskResponse = self.unary(request, paths::TASK_GET).await?;
        response
            .process
            .ok_or_else(|| ClientError::TaskNotFound(container_id.to_string()))
    }

    /// List all tasks known to containerd.
    pub async fn list_tasks(&mut self) -> Result<Vec<Process>, ClientError> {
        let request = ListTasksRequest {
            filter: String::new(),
        };
        let response: ListTasksResponse = self.unary(request, paths::TASK_LIST).await?;
        Ok(response.tasks)
    }

    /// Wait for a task to exit and return its exit status.
    ///
    /// This blocks until the task's process exits, so callers should only
    /// use it for tasks they expect to terminate.
    pub async fn wait_task(&mut self, container_id: &str) -> Result<u32, ClientError> {
        let request = WaitRequest {
            container_id: container_id.to_string(),
            exec_id: String::new(),
        };
        let response: WaitResponse = self.unary(request, paths::TASK_WAIT).await?;
        Ok(response.exit_status)
    }

    /// Create a process inside a running task, with no stdio.
    pub async fn exec_process(
        &mut self,
        container_id: &str,
        exec_id: &str,
        spec: prost_types::Any,
    ) -> Result<(), ClientError> {
        let request = ExecProcessRequest {
            container_id: container_id.to_string(),
            stdin: String::new(),
            stdout: String::new(),
            stderr: String::new(),
            terminal: false,
            spec: Some(spec),
            exec_id: exec_id.to_string(),
        };
        let _: ExecProcessResponse = self.unary(request, paths::TASK_EXEC).await?;
        Ok(())
    }

    /// Start a process created with [`Self::exec_process`].
    pub async fn start_process(
        &mut self,
        container_id: &str,
        exec_id: &str,
    ) -> Result<(), ClientError> {
        let request = StartRequest {
            container_id: container_id.to_string(),
            exec_id: exec_id.to_string(),
        };
        let _: StartResponse = self.unary(request, paths::TASK_START).await?;
        Ok(())
    }

    /// Read an exec process's status.
    pub async fn get_process(
        &mut self,
        container_id: &str,
        exec_id: &str,
    ) -> Result<Process, ClientError> {
        let request = GetTaskRequest {
            container_id: container_id.to_string(),
            exec_id: exec_id.to_string(),
        };
        let response: GetTaskResponse = self.unary(request, paths::TASK_GET).await?;
        response
            .process
            .ok_or_else(|| ClientError::TaskNotFound(container_id.to_string()))
    }

    /// Kill an exec process.
    pub async fn kill_process(
        &mut self,
        container_id: &str,
        exec_id: &str,
        signal: u32,
    ) -> Result<(), ClientError> {
        let request = KillRequest {
            container_id: container_id.to_string(),
            exec_id: exec_id.to_string(),
            signal,
            all: false,
        };
        let _: KillResponse = self.unary(request, paths::TASK_KILL).await?;
        Ok(())
    }

    /// Remove a finished exec process.
    pub async fn delete_process(
        &mut self,
        container_id: &str,
        exec_id: &str,
    ) -> Result<(), ClientError> {
        let request = DeleteProcessRequest {
            container_id: container_id.to_string(),
            exec_id: exec_id.to_string(),
        };
        let _: DeleteTaskResponse = self.unary(request, paths::TASK_DELETE_PROCESS).await?;
        Ok(())
    }

    // --- ImageService -------------------------------------------------------

    /// Check if an image exists. Returns `Ok(Some(image))` if found,
    /// `Ok(None)` if not found.
    pub async fn get_image(&mut self, name: &str) -> Result<Option<Image>, ClientError> {
        let request = GetImageRequest {
            name: name.to_string(),
        };
        match self
            .unary::<GetImageRequest, GetImageResponse>(request, paths::IMAGE_GET)
            .await
        {
            Ok(response) => Ok(response.image),
            Err(ClientError::Grpc(status)) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    // --- Transfer and Streaming ---------------------------------------------

    /// Pulls `reference` into the image store and unpacks it with
    /// `snapshotter`, all inside containerd (Transfer service, containerd
    /// 1.7 or newer). With `auth` = `(registry host, credential)`, the daemon asks
    /// for the login over a stream and the secret is only ever sent to that
    /// registry; it never touches argv or the environment.
    ///
    /// `Unimplemented` (a daemon older than 1.7) comes back as
    /// `ClientError::Grpc` for the caller to handle.
    pub async fn pull_image(
        &self,
        reference: &str,
        snapshotter: &str,
        auth: Option<(&str, &Credential)>,
    ) -> Result<(), ClientError> {
        // Closing the auth stream and stopping its task is tied to this guard.
        let mut auth_task = None;
        let mut resolver = None;
        if let Some((registry, credential)) = auth {
            let id = format!(
                "minga-pull-{}-{}",
                std::process::id(),
                NEXT_STREAM.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            );
            auth_task = Some(self.serve_credentials(&id, registry, credential).await?);
            resolver = Some(RegistryResolver { auth_stream: id });
        }
        let platform = Platform {
            os: "linux".into(),
            architecture: match std::env::consts::ARCH {
                "x86_64" => "amd64",
                "aarch64" => "arm64",
                other => other,
            }
            .into(),
            variant: String::new(),
        };
        let request = TransferRequest {
            source: Some(any_of(
                type_urls::OCI_REGISTRY,
                &OciRegistry {
                    reference: reference.to_string(),
                    resolver,
                },
            )),
            destination: Some(any_of(
                type_urls::IMAGE_STORE,
                &ImageStore {
                    name: reference.to_string(),
                    platforms: vec![platform.clone()],
                    unpacks: vec![UnpackConfiguration {
                        platform: Some(platform),
                        snapshotter: snapshotter.to_string(),
                    }],
                },
            )),
        };
        let result: Result<(), ClientError> = self.clone().unary(request, paths::TRANSFER).await;
        if let Some(task) = auth_task {
            task.abort();
        }
        result.map(|_| ())
    }

    /// Opens stream `id` and answers the daemon's `AuthRequest`s on it until
    /// the returned task is aborted. Only `registry` gets the credential.
    async fn serve_credentials(
        &self,
        id: &str,
        registry: &str,
        credential: &Credential,
    ) -> Result<tokio::task::JoinHandle<()>, ClientError> {
        let (tx, rx) = tokio::sync::mpsc::channel::<prost_types::Any>(4);
        tx.send(any_of(
            type_urls::STREAM_INIT,
            &StreamInit { id: id.into() },
        ))
        .await
        .expect("receiver is alive");
        let mut request = Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
        request.metadata_mut().insert(
            "containerd-namespace",
            "minga".parse().expect("static namespace"),
        );
        let mut grpc = Grpc::new(self.channel.clone());
        grpc.ready().await.map_err(|e| ClientError::Rpc {
            method: paths::STREAM.to_string(),
            message: e.to_string(),
        })?;
        let mut inbound = grpc
            .streaming(
                request,
                PathAndQuery::from_static(paths::STREAM),
                ProstCodec::<prost_types::Any, prost_types::Any>::default(),
            )
            .await?
            .into_inner();
        // The daemon echoes StreamInit once the stream is registered.
        inbound.message().await?;

        let registry = super::credentials::canonical_host(registry);
        let credential = credential.clone();
        Ok(tokio::spawn(async move {
            while let Ok(Some(message)) = inbound.message().await {
                if message.type_url != type_urls::AUTH_REQUEST {
                    continue;
                }
                let Ok(ask) = AuthRequest::decode(message.value.as_slice()) else {
                    continue;
                };
                // Redirects and token realms are other hosts: they get nothing.
                let answer = if super::credentials::canonical_host(&ask.host) == registry {
                    AuthResponse {
                        auth_type: auth_type::CREDENTIALS,
                        username: credential.username.clone(),
                        secret: credential.secret().to_string(),
                    }
                } else {
                    tracing::debug!(host = %ask.host, "refusing credentials for another host");
                    AuthResponse {
                        auth_type: auth_type::NONE,
                        ..Default::default()
                    }
                };
                if tx
                    .send(any_of(type_urls::AUTH_RESPONSE, &answer))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }))
    }

    // --- Content and snapshot services -------------------------------------

    /// Read and authenticate a bounded image metadata blob. Layers are not
    /// fetched here; the image must already have been unpacked by containerd.
    pub async fn read_content(
        &mut self,
        descriptor: &BlobDescriptor,
    ) -> Result<Vec<u8>, ClientError> {
        let request = ReadContentRequest {
            digest: descriptor.digest.clone(),
            offset: 0,
            size: descriptor.size,
        };
        let mut request = Request::new(request);
        request.metadata_mut().insert(
            "containerd-namespace",
            "minga".parse().expect("static namespace"),
        );
        let mut grpc = Grpc::new(self.channel.clone());
        grpc.ready().await.map_err(|e| ClientError::Rpc {
            method: paths::CONTENT_READ.into(),
            message: e.to_string(),
        })?;
        let response = grpc
            .server_streaming(
                request,
                PathAndQuery::from_static(paths::CONTENT_READ),
                ProstCodec::<ReadContentRequest, ReadContentResponse>::default(),
            )
            .await?;
        let mut stream = response.into_inner();
        let mut data = Vec::with_capacity(descriptor.size as usize);
        while let Some(chunk) = stream.message().await? {
            if chunk.offset != data.len() as i64
                || chunk.data.len() > descriptor.size as usize - data.len()
            {
                return Err(ClientError::Rpc {
                    method: paths::CONTENT_READ.into(),
                    message: "image metadata stream has invalid offset or size".into(),
                });
            }
            data.extend_from_slice(&chunk.data);
        }
        descriptor
            .verify(&data)
            .map_err(|message| ClientError::Rpc {
                method: paths::CONTENT_READ.into(),
                message,
            })?;
        Ok(data)
    }

    pub async fn stat_snapshot(
        &mut self,
        snapshotter: &str,
        key: &str,
    ) -> Result<Option<SnapshotInfo>, ClientError> {
        let request = StatSnapshotRequest {
            snapshotter: snapshotter.into(),
            key: key.into(),
        };
        match self
            .unary::<StatSnapshotRequest, StatSnapshotResponse>(request, paths::SNAPSHOT_STAT)
            .await
        {
            Ok(response) => Ok(response.info),
            Err(ClientError::Grpc(status)) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn prepare_snapshot(
        &mut self,
        snapshotter: &str,
        key: &str,
        parent: &str,
    ) -> Result<Vec<Mount>, ClientError> {
        let request = PrepareSnapshotRequest {
            snapshotter: snapshotter.into(),
            key: key.into(),
            parent: parent.into(),
            labels: Default::default(),
        };
        let response: PrepareSnapshotResponse =
            self.unary(request, paths::SNAPSHOT_PREPARE).await?;
        Ok(response.mounts)
    }

    pub async fn remove_snapshot(
        &mut self,
        snapshotter: &str,
        key: &str,
    ) -> Result<(), ClientError> {
        let request = RemoveSnapshotRequest {
            snapshotter: snapshotter.into(),
            key: key.into(),
        };
        let _: RemoveSnapshotResponse = self.unary(request, paths::SNAPSHOT_REMOVE).await?;
        Ok(())
    }

    // --- Internal -----------------------------------------------------------

    /// Make a unary gRPC call.
    async fn unary<Req, Resp>(
        &mut self,
        request: Req,
        path: &'static str,
    ) -> Result<Resp, ClientError>
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Default + Send + 'static,
    {
        let mut request = Request::new(request);
        request.metadata_mut().insert(
            "containerd-namespace",
            "minga".parse().expect("static namespace"),
        );
        let codec = ProstCodec::<Req, Resp>::default();
        let path = PathAndQuery::from_static(path);

        let mut grpc = Grpc::new(self.channel.clone());
        grpc.ready().await.map_err(|e| ClientError::Rpc {
            method: path.to_string(),
            message: e.to_string(),
        })?;
        let response = grpc
            .unary(request, path, codec)
            .await
            .map_err(ClientError::from)?;
        Ok(response.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_socket_path() {
        assert_eq!(DEFAULT_SOCKET, "/run/containerd/containerd.sock");
    }

    #[test]
    fn client_error_display() {
        let err = ClientError::ContainerNotFound("abc".to_string());
        assert!(err.to_string().contains("abc"));

        let err = ClientError::Connect(
            "/run/containerd/containerd.sock".to_string(),
            "connection refused".to_string(),
        );
        assert!(err.to_string().contains("connection refused"));
    }
}
