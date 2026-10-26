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
}

/// `containerd.v1.types.Process`
///
/// Field numbers follow containerd v1.7 `api/types/task/task.proto`:
/// `container_id=1, id=2, pid=3, status=4, stdin=5, stdout=6, stderr=7,
/// terminal=8, exit_status=9, exited_at=10`.
#[derive(Clone, PartialEq, Message)]
pub struct Process {
    #[prost(string, tag = "1")]
    pub container_id: String,
    #[prost(string, tag = "2")]
    pub id: String,
    #[prost(uint32, tag = "3")]
    pub pid: u32,
    #[prost(enumeration = "ProcessStatus", tag = "4")]
    pub status: i32,
    #[prost(string, tag = "5")]
    pub stdin: String,
    #[prost(string, tag = "6")]
    pub stdout: String,
    #[prost(string, tag = "7")]
    pub stderr: String,
    #[prost(bool, tag = "8")]
    pub terminal: bool,
    /// Exit status of the process. Only meaningful once the process stopped.
    #[prost(uint32, tag = "9")]
    pub exit_status: u32,
    #[prost(message, optional, tag = "10")]
    pub exited_at: Option<prost_types::Timestamp>,
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
    pub runtime: Option<Runtime>,
    #[prost(message, optional, tag = "5")]
    pub spec: Option<prost_types::Any>,
    #[prost(string, tag = "6")]
    pub snapshotter: String,
    #[prost(string, tag = "7")]
    pub snapshot_key: String,
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

/// `containerd.services.tasks.v1.CreateTaskRequest`
///
/// containerd v1.7 `api/services/tasks/v1/tasks.proto`:
/// `container_id=1, rootfs=3, stdin=4, stdout=5, stderr=6, terminal=7,
/// checkpoint=8, options=9, runtime_path=10`.
#[derive(Clone, PartialEq, Message)]
pub struct CreateTaskRequest {
    #[prost(string, tag = "1")]
    pub container_id: String,
    #[prost(message, repeated, tag = "3")]
    pub rootfs: Vec<Mount>,
    /// FIFO path for stdin. Empty means "no stdin".
    #[prost(string, tag = "4")]
    pub stdin: String,
    /// FIFO path for stdout. Empty means "discard stdout".
    #[prost(string, tag = "5")]
    pub stdout: String,
    /// FIFO path for stderr. Empty means "discard stderr".
    #[prost(string, tag = "6")]
    pub stderr: String,
    #[prost(bool, tag = "7")]
    pub terminal: bool,
    #[prost(message, optional, tag = "8")]
    pub checkpoint: Option<Descriptor>,
    /// Runtime-specific options (e.g. `io.containerd.runtime.v1.Options`).
    #[prost(message, optional, tag = "9")]
    pub options: Option<prost_types::Any>,
    #[prost(string, tag = "10")]
    pub runtime_path: String,
}

/// `containerd.services.tasks.v1.CreateTaskResponse`
///
/// containerd v1.7: `container_id=1, pid=2`.
#[derive(Clone, PartialEq, Message)]
pub struct CreateTaskResponse {
    #[prost(string, tag = "1")]
    pub container_id: String,
    #[prost(uint32, tag = "2")]
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

/// `containerd.services.tasks.v1.DeleteTaskRequest`
///
/// containerd v1.7: `container_id=1` (no exec_id).
#[derive(Clone, PartialEq, Message)]
pub struct DeleteTaskRequest {
    #[prost(string, tag = "1")]
    pub container_id: String,
}

/// `containerd.services.tasks.v1.DeleteResponse`
///
/// containerd v1.7: `id=1, pid=2, exit_status=3, exited_at=4`.
#[derive(Clone, PartialEq, Message)]
pub struct DeleteTaskResponse {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(uint32, tag = "2")]
    pub pid: u32,
    #[prost(uint32, tag = "3")]
    pub exit_status: u32,
    #[prost(message, optional, tag = "4")]
    pub exited_at: Option<prost_types::Timestamp>,
}

/// `containerd.services.tasks.v1.GetRequest`
///
/// containerd v1.7: `container_id=1, exec_id=2`.
#[derive(Clone, PartialEq, Message)]
pub struct GetTaskRequest {
    #[prost(string, tag = "1")]
    pub container_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
}

/// `containerd.services.tasks.v1.GetResponse`
///
/// containerd v1.7: `process=1`.
#[derive(Clone, PartialEq, Message)]
pub struct GetTaskResponse {
    #[prost(message, optional, tag = "1")]
    pub process: Option<Process>,
}

/// `containerd.services.tasks.v1.ListTasksRequest`
///
/// containerd v1.7: `filter=1`.
#[derive(Clone, PartialEq, Message)]
pub struct ListTasksRequest {
    #[prost(string, tag = "1")]
    pub filter: String,
}

/// `containerd.services.tasks.v1.ListTasksResponse`
///
/// containerd v1.7: `tasks=1`.
#[derive(Clone, PartialEq, Message)]
pub struct ListTasksResponse {
    #[prost(message, repeated, tag = "1")]
    pub tasks: Vec<Process>,
}

/// `containerd.services.tasks.v1.WaitRequest`
///
/// containerd v1.7: `container_id=1, exec_id=2`.
#[derive(Clone, PartialEq, Message)]
pub struct WaitRequest {
    #[prost(string, tag = "1")]
    pub container_id: String,
    #[prost(string, tag = "2")]
    pub exec_id: String,
}

/// `containerd.services.tasks.v1.WaitResponse`
///
/// containerd v1.7: `exit_status=1, exited_at=2`.
#[derive(Clone, PartialEq, Message)]
pub struct WaitResponse {
    #[prost(uint32, tag = "1")]
    pub exit_status: u32,
    #[prost(message, optional, tag = "2")]
    pub exited_at: Option<prost_types::Timestamp>,
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
    pub const CONTAINER_CREATE: &str = "/containerd.services.containers.v1.Containers/Create";
    pub const CONTAINER_GET: &str = "/containerd.services.containers.v1.Containers/Get";
    pub const CONTAINER_LIST: &str = "/containerd.services.containers.v1.Containers/List";
    pub const CONTAINER_DELETE: &str = "/containerd.services.containers.v1.Containers/Delete";

    // containerd v1.7 names this service `Tasks`, not `TaskService`.
    pub const TASK_CREATE: &str = "/containerd.services.tasks.v1.Tasks/Create";
    pub const TASK_START: &str = "/containerd.services.tasks.v1.Tasks/Start";
    pub const TASK_KILL: &str = "/containerd.services.tasks.v1.Tasks/Kill";
    pub const TASK_DELETE: &str = "/containerd.services.tasks.v1.Tasks/Delete";
    pub const TASK_GET: &str = "/containerd.services.tasks.v1.Tasks/Get";
    pub const TASK_LIST: &str = "/containerd.services.tasks.v1.Tasks/List";
    pub const TASK_WAIT: &str = "/containerd.services.tasks.v1.Tasks/Wait";

    pub const IMAGE_GET: &str = "/containerd.services.images.v1.Images/Get";
    pub const IMAGE_PULL: &str = "/containerd.services.images.v1.Images/Pull";
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
            id: String::new(),
            pid: 1234,
            status: ProcessStatus::Running as i32,
            stdin: String::new(),
            stdout: String::new(),
            stderr: String::new(),
            terminal: false,
            exit_status: 0,
            exited_at: None,
        };
        let bytes = p.encode_to_vec();
        let decoded = Process::decode(&bytes[..]).unwrap();
        assert_eq!(decoded.container_id, "c1");
        assert_eq!(decoded.status, ProcessStatus::Running as i32);
        assert_eq!(decoded.pid, 1234);
    }

    /// The `Process` field numbers must match containerd v1.7 exactly —
    /// `containerd.v1.types.Process` in `api/types/task/task.proto`:
    /// `container_id=1, id=2, pid=3, status=4, ..., exit_status=9`.
    #[test]
    fn process_field_numbers_match_containerd() {
        use prost::Message;

        // Encode only the status field and check it lands on wire tag 4.
        let p = Process {
            status: ProcessStatus::Stopped as i32,
            ..Default::default()
        };
        let bytes = p.encode_to_vec();
        // prost encodes tag 4, wire type 0 (varint) as (4 << 3) | 0 = 0x20.
        assert_eq!(bytes[0], 0x20, "status must be field 4");

        // Encode only the pid field and check it lands on wire tag 3.
        let p = Process {
            pid: 4242,
            ..Default::default()
        };
        let bytes = p.encode_to_vec();
        // prost encodes tag 3, wire type 0 (varint) as (3 << 3) | 0 = 0x18.
        assert_eq!(bytes[0], 0x18, "pid must be field 3");

        // Exit status must be field 9.
        let p = Process {
            exit_status: 137,
            ..Default::default()
        };
        let bytes = p.encode_to_vec();
        assert_eq!(bytes[0], 9 << 3, "exit_status must be field 9");
    }

    /// The task service is named `Tasks` in containerd v1.7. Using the
    /// v1.3-era `TaskService` name makes every task RPC fail with
    /// "unimplemented".
    #[test]
    fn task_service_paths_use_the_v17_service_name() {
        for path in [
            paths::TASK_CREATE,
            paths::TASK_START,
            paths::TASK_KILL,
            paths::TASK_DELETE,
            paths::TASK_GET,
            paths::TASK_LIST,
            paths::TASK_WAIT,
        ] {
            assert!(
                path.starts_with("/containerd.services.tasks.v1.Tasks/"),
                "unexpected task service path: {path}"
            );
        }
    }

    #[test]
    fn create_task_request_roundtrip() {
        let req = CreateTaskRequest {
            container_id: "c1".to_string(),
            rootfs: vec![Mount {
                r#type: "bind".to_string(),
                source: "/host".to_string(),
                target: "/container".to_string(),
                options: vec!["rbind".to_string(), "ro".to_string()],
            }],
            stdin: String::new(),
            stdout: "/run/minga/c1.stdout".to_string(),
            stderr: "/run/minga/c1.stderr".to_string(),
            terminal: false,
            checkpoint: None,
            options: None,
            runtime_path: String::new(),
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
