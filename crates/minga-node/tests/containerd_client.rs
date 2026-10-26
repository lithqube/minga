//! Integration tests for the containerd gRPC client.
//!
//! These tests require a running containerd daemon. They skip gracefully
//! if containerd is not available (e.g. on macOS dev machines or CI
//! without containerd).
//!
//! To run: ensure containerd is running, then:
//!   MINGA_CONTAINERD_SOCKET=/path/to/containerd.sock \
//!     cargo test --test containerd_client -- --nocapture

use minga_node::containerd::client::ContainerdClient;

/// Socket path from the environment, or the standard Linux location.
fn socket_path() -> String {
    std::env::var("MINGA_CONTAINERD_SOCKET")
        .unwrap_or_else(|_| "/run/containerd/containerd.sock".to_string())
}

fn containerd_available_at(path: &str) -> bool {
    std::path::Path::new(path).exists()
}

/// Connect to containerd, or return `None` if it is unavailable.
async fn connect() -> Option<ContainerdClient> {
    let socket = socket_path();
    if !containerd_available_at(&socket) {
        eprintln!("SKIP: containerd not available at {socket}");
        return None;
    }
    match ContainerdClient::connect(std::path::PathBuf::from(&socket)).await {
        Ok(client) => Some(client),
        Err(e) => {
            eprintln!("SKIP: cannot connect to containerd: {e}");
            None
        }
    }
}

/// The task service is named `Tasks` in containerd v1.7+.
///
/// Before this was fixed the client used the v1.3-era `TaskService` name,
/// which made every task RPC fail with `Unimplemented`. This test calls
/// `Tasks/List` directly — if the service name were wrong, the server
/// would return `Unimplemented` instead of an empty task list.
#[tokio::test]
async fn tasks_list_reaches_the_real_service() {
    let Some(mut client) = connect().await else {
        return;
    };

    // `Tasks/List` on an empty server returns an empty list, not an error.
    // A wrong service name returns `Unimplemented` ("unknown service").
    let response = client
        .list_tasks()
        .await
        .expect("Tasks/List should reach the real containerd task service");

    // We do not assert emptiness — other containers may be running. We
    // assert that we got a well-formed response.
    for task in &response {
        assert!(!task.container_id.is_empty());
    }
}

/// `Tasks/Get` must resolve against the real service. A container that does
/// not exist must produce a `NotFound` status — NOT `Unimplemented`.
#[tokio::test]
async fn tasks_get_reaches_the_real_service() {
    let Some(mut client) = connect().await else {
        return;
    };

    let result = client.get_task("minga-nonexistent-task-probe").await;
    match result {
        Err(minga_node::containerd::client::ClientError::Grpc(status)) => {
            assert_eq!(
                status.code(),
                tonic::Code::NotFound,
                "expected NotFound for a missing task, got {status:?}"
            );
        }
        Err(e) => panic!("unexpected error: {e}"),
        Ok(process) => {
            // If the task somehow exists, the decoded fields must be sane.
            assert!(!process.container_id.is_empty());
        }
    }
}

/// `Tasks/Create` for an unknown container must fail with `NotFound`, not
/// `Unimplemented`. This proves the request message decodes against the
/// real server's `CreateTaskRequest`.
#[tokio::test]
async fn tasks_create_reaches_the_real_service() {
    let Some(mut client) = connect().await else {
        return;
    };

    let result = client
        .create_task("minga-nonexistent-container-probe", "", "")
        .await;
    match result {
        Err(minga_node::containerd::client::ClientError::Grpc(status)) => {
            assert_eq!(
                status.code(),
                tonic::Code::NotFound,
                "expected NotFound for a missing container, got {status:?}"
            );
        }
        Err(e) => panic!("unexpected error: {e}"),
        Ok(pid) => panic!("create_task for a missing container returned pid {pid}"),
    }
}

/// `Tasks/Kill` for an unknown task must fail with `NotFound`.
#[tokio::test]
async fn tasks_kill_reaches_the_real_service() {
    let Some(mut client) = connect().await else {
        return;
    };

    let result = client.kill_task("minga-nonexistent-task-probe", 15).await;
    match result {
        Err(minga_node::containerd::client::ClientError::Grpc(status)) => {
            assert_eq!(
                status.code(),
                tonic::Code::NotFound,
                "expected NotFound for a missing task, got {status:?}"
            );
        }
        Err(e) => panic!("unexpected error: {e}"),
        Ok(()) => panic!("kill_task for a missing task unexpectedly succeeded"),
    }
}

/// The container service must work and return a decodable response.
#[tokio::test]
async fn containers_list_reaches_the_real_service() {
    let Some(mut client) = connect().await else {
        return;
    };

    let containers = client
        .list_containers(vec![])
        .await
        .expect("ContainerService/List should succeed");

    for container in &containers {
        assert!(!container.id.is_empty());
    }
}

/// The image service must work. Listing a missing image returns `None`
/// rather than an error.
#[tokio::test]
async fn images_get_reaches_the_real_service() {
    let Some(mut client) = connect().await else {
        return;
    };

    let result = client
        .get_image("docker.io/library/definitely-not-a-real-image:latest")
        .await;
    match result {
        Ok(None) => {}
        Ok(Some(image)) => panic!("unexpected image: {}", image.name),
        Err(e) => panic!("unexpected error: {e}"),
    }
}
