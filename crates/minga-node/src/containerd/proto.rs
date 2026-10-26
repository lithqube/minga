//! Protobuf message types for the containerd gRPC API.
//!
//! These structs are wire-compatible with containerd v1.7's proto definitions.
//! Only the fields Minga uses are included — unknown fields are silently
//! ignored on decode (proto3 forward-compatibility).
//!
//! Service paths:
//! - `containerd.services.containers.v1.ContainerService`
//! - `containerd.services.tasks.v1.TaskService`
//! - `containerd.services.images.v1.ImageService`

use std::collections::HashMap;

use prost::Message;

// ---------------------------------------------------------------------------
// containerd.types
// ---------------------------------------------------------------------------

/// `containerd.types.Mount`
#[derive(Clone, PartialEq, Message)]
pub struct Mount {
    #[prost(string, tag = "1")]
    pub r#type: String,
    #[prost(string, tag = "2")]
    pub source: String,
    #[prost(string, tag = "3")]
    pub target: String,
    #[prost(string, repeated, tag = "4")]
    pub options: Vec<String>,
}

/// `containerd.types.Runtime`
#[derive(Clone, PartialEq, Message)]
pub struct Runtime {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(message, optional, tag = "2")]
    pub options: Option<prost_types::Any>,
}

/// `containerd.types.Descriptor`
#[derive(Clone, PartialEq, Message)]
pub struct Descriptor {
    #[prost(string, tag = "1")]
    pub media_type: String,
    #[prost(bytes = "vec", tag = "2")]
    pub digest: Vec<u8>,
    #[prost(int64, tag = "3")]
    pub size: i64,
    #[prost(string, repeated, tag = "4")]
    pub urls: Vec<String>,
    #[prost(map = "string, string", tag = "5")]
    pub annotations: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// containerd.types.task — Process and Status
// ---------------------------------------------------------------------------

/// `containerd.types.Status` enum (task/process status).
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum ProcessStatus {
    Unknown = 0,
    Created = 1,
    Running = 2,
    Stopped = 3,
    Paused = 4,
    Pausing = 5,
    Resuming = 6,
}

/// `containerd.types.Process`
#[derive(Clone, PartialEq, Message)]
pub struct Process {
    #[prost(string, tag = "1")]
    pub container_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
    #[prost(enumeration = "ProcessStatus", tag = "3")]
    pub status: i32,
    #[prost(uint32, tag = "4")]
    pub pid: u32,
}

// ---------------------------------------------------------------------------
// google.rpc.Status
// ---------------------------------------------------------------------------

/// `google.rpc.Status`
#[derive(Clone, PartialEq, Message)]
pub struct RpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(message, repeated, tag = "3")]
    pub details: Vec<prost_types::Any>,
}

// ---------------------------------------------------------------------------
// containerd.services.containers.v1
// ---------------------------------------------------------------------------

/// `containerd.services.containers.v1.Container`
#[derive(Clone, PartialEq, Message)]
pub struct Container {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(map = "string, string", tag = "2")]
    pub labels: HashMap<String, String>,
    #[prost(string, tag = "3")]
    pub image: String,
    #[prost(message, optional, tag = "4")]
    pub spec: Option<prost_types::Any>,
    #[prost(string, tag = "5")]
    pub snapshotter: String,
    #[prost(string, tag = "6")]
    pub snapshot_key: String,
    #[prost(message, optional, tag = "7")]
    pub runtime: Option<Runtime>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateContainerRequest {
    #[prost(message, optional, tag = "1")]
    pub container: Option<Container>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateContainerResponse {
    #[prost(message, optional, tag = "1")]
    pub container: Option<Container>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetContainerRequest {
    #[prost(string, tag = "1")]
    pub id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetContainerResponse {
    #[prost(message, optional, tag = "1")]
    pub container: Option<Container>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ListContainersRequest {
    #[prost(string, repeated, tag = "1")]
    pub filters: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ListContainersResponse {
    #[prost(message, repeated, tag = "1")]
    pub containers: Vec<Container>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteContainerRequest {
    #[prost(string, tag = "1")]
    pub id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteContainerResponse {}

// ---------------------------------------------------------------------------
// containerd.services.tasks.v1
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Message)]
pub struct CreateTaskRequest {
    #[prost(string, tag = "1")]
    pub container_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
    #[prost(message, repeated, tag = "3")]
    pub rootfs: Vec<Mount>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateTaskResponse {
    #[prost(uint32, tag = "1")]
    pub pid: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct StartRequest {
    #[prost(string, tag = "1")]
    pub container_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct StartResponse {
    #[prost(uint32, tag = "1")]
    pub pid: u32,
}

#[derive(Clone, PartialEq, Message)]
pub struct KillRequest {
    #[prost(string, tag = "1")]
    pub container_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
    #[prost(uint32, tag = "3")]
    pub signal: u32,
    #[prost(bool, tag = "4")]
    pub all: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct KillResponse {}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteTaskRequest {
    #[prost(string, tag = "1")]
    pub container_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteTaskResponse {
    #[prost(uint32, tag = "1")]
    pub pid: u32,
    #[prost(message, optional, tag = "2")]
    pub exited_at: Option<prost_types::Timestamp>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetTaskRequest {
    #[prost(string, tag = "1")]
    pub container_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetTaskResponse {
    #[prost(message, optional, tag = "1")]
    pub process: Option<Process>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ListTasksRequest {
    #[prost(string, repeated, tag = "1")]
    pub filters: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ListTasksResponse {
    #[prost(message, repeated, tag = "1")]
    pub processes: Vec<Process>,
}

// ---------------------------------------------------------------------------
// containerd.services.images.v1
// ---------------------------------------------------------------------------

/// `containerd.services.images.v1.Image`
#[derive(Clone, PartialEq, Message)]
pub struct Image {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(map = "string, string", tag = "2")]
    pub labels: HashMap<String, String>,
    #[prost(message, optional, tag = "3")]
    pub target: Option<Descriptor>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetImageRequest {
    #[prost(string, tag = "1")]
    pub name: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetImageResponse {
    #[prost(message, optional, tag = "1")]
    pub image: Option<Image>,
}

#[derive(Clone, PartialEq, Message)]
pub struct PullImageRequest {
    #[prost(string, tag = "1")]
    pub image: String,
    #[prost(string, tag = "2")]
    pub platform: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct PullImageResponse {
    #[prost(message, optional, tag = "1")]
    pub image: Option<Image>,
}

// ---------------------------------------------------------------------------
// gRPC service paths
// ---------------------------------------------------------------------------

pub mod paths {
    pub const CONTAINER_CREATE: &str = "/containerd.services.containers.v1.ContainerService/Create";
    pub const CONTAINER_GET: &str = "/containerd.services.containers.v1.ContainerService/Get";
    pub const CONTAINER_LIST: &str = "/containerd.services.containers.v1.ContainerService/List";
    pub const CONTAINER_DELETE: &str = "/containerd.services.containers.v1.ContainerService/Delete";

    pub const TASK_CREATE: &str = "/containerd.services.tasks.v1.TaskService/Create";
    pub const TASK_START: &str = "/containerd.services.tasks.v1.TaskService/Start";
    pub const TASK_KILL: &str = "/containerd.services.tasks.v1.TaskService/Kill";
    pub const TASK_DELETE: &str = "/containerd.services.tasks.v1.TaskService/Delete";
    pub const TASK_GET: &str = "/containerd.services.tasks.v1.TaskService/Get";

    pub const IMAGE_GET: &str = "/containerd.services.images.v1.ImageService/Get";
    pub const IMAGE_PULL: &str = "/containerd.services.images.v1.ImageService/Pull";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn container_roundtrip() {
        let c = Container {
            id: "test-1".to_string(),
            labels: HashMap::from([("app".to_string(), "nginx".to_string())]),
            image: "docker.io/library/nginx:latest".to_string(),
            spec: Some(prost_types::Any {
                type_url: "types.containerd.io/opencontainers.runtime.spec.v1.Spec".to_string(),
                value: b"{}".to_vec(),
            }),
            snapshotter: "overlayfs".to_string(),
            snapshot_key: "test-1-snap".to_string(),
            runtime: Some(Runtime {
                name: "io.containerd.runc.v2".to_string(),
                options: None,
            }),
        };
        let bytes = c.encode_to_vec();
        let decoded = Container::decode(&bytes[..]).unwrap();
        assert_eq!(decoded.id, "test-1");
        assert_eq!(decoded.image, "docker.io/library/nginx:latest");
        assert_eq!(decoded.labels.get("app").unwrap(), "nginx");
        assert_eq!(decoded.snapshotter, "overlayfs");
        assert!(decoded.spec.is_some());
        assert!(decoded.runtime.is_some());
    }

    #[test]
    fn process_roundtrip() {
        let p = Process {
            container_id: "c1".to_string(),
            exec_id: String::new(),
            status: ProcessStatus::Running as i32,
            pid: 1234,
        };
        let bytes = p.encode_to_vec();
        let decoded = Process::decode(&bytes[..]).unwrap();
        assert_eq!(decoded.container_id, "c1");
        assert_eq!(decoded.status, ProcessStatus::Running as i32);
        assert_eq!(decoded.pid, 1234);
    }

    #[test]
    fn create_task_request_roundtrip() {
        let req = CreateTaskRequest {
            container_id: "c1".to_string(),
            exec_id: String::new(),
            rootfs: vec![Mount {
                r#type: "bind".to_string(),
                source: "/host".to_string(),
                target: "/container".to_string(),
                options: vec!["rbind".to_string(), "ro".to_string()],
            }],
        };
        let bytes = req.encode_to_vec();
        let decoded = CreateTaskRequest::decode(&bytes[..]).unwrap();
        assert_eq!(decoded.container_id, "c1");
        assert_eq!(decoded.rootfs.len(), 1);
        assert_eq!(decoded.rootfs[0].target, "/container");
    }

    #[test]
    fn kill_request_roundtrip() {
        let req = KillRequest {
            container_id: "c1".to_string(),
            exec_id: String::new(),
            signal: 15, // SIGTERM
            all: false,
        };
        let bytes = req.encode_to_vec();
        let decoded = KillRequest::decode(&bytes[..]).unwrap();
        assert_eq!(decoded.signal, 15);
        assert!(!decoded.all);
    }

    #[test]
    fn empty_messages_roundtrip() {
        let del = DeleteContainerResponse {};
        let bytes = del.encode_to_vec();
        assert!(bytes.is_empty() || DeleteContainerResponse::decode(&bytes[..]).is_ok());

        let kill_resp = KillResponse {};
        let bytes = kill_resp.encode_to_vec();
        assert!(KillResponse::decode(&bytes[..]).is_ok());
    }
}
