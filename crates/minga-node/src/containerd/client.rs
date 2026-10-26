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

use super::proto::*;

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

/// A client connected to a containerd daemon.
pub struct ContainerdClient {
    channel: Channel,
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
        Ok(Self { channel })
    }

    /// Create a client from an existing channel (for testing).
    pub fn from_channel(channel: Channel) -> Self {
        Self { channel }
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
        stdout: &str,
        stderr: &str,
    ) -> Result<u32, ClientError> {
        let request = CreateTaskRequest {
            container_id: container_id.to_string(),
            rootfs: vec![],
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

    /// Pull an image. Streams progress updates; returns when the pull is complete.
    pub async fn pull_image(&mut self, image_ref: &str) -> Result<Image, ClientError> {
        let request = PullImageRequest {
            image: image_ref.to_string(),
            platform: String::new(), // let containerd pick the platform
        };

        let request = Request::new(request);
        let codec = ProstCodec::<PullImageRequest, PullImageResponse>::default();
        let path = PathAndQuery::from_static(paths::IMAGE_PULL);

        let mut grpc = Grpc::new(self.channel.clone());
        let response = grpc
            .server_streaming(request, path, codec)
            .await
            .map_err(ClientError::from)?;

        let mut stream = response.into_inner();
        let mut last_image = None;
        while let Some(result) = stream.message().await.map_err(ClientError::from)? {
            last_image = result.image;
        }

        last_image.ok_or_else(|| ClientError::Rpc {
            method: "ImageService/Pull".to_string(),
            message: "pull completed but no image was returned".to_string(),
        })
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
