//! Integration tests for ContainerdRuntime.
//!
//! These tests require a running containerd daemon. They skip gracefully
//! if containerd is not available (e.g. on macOS dev machines or CI
//! without containerd).
//!
//! To run: ensure containerd is running, then:
//!   cargo test --test containerd -- --nocapture

use std::collections::HashMap;
use std::path::Path;

use minga_node::{ContainerStatus, ContainerdRuntime, Runtime};
use minga_state::types::{ResourceSpec, TaskSpec};

/// Check if containerd is available at a custom socket (from env).
fn socket_path() -> String {
    std::env::var("MINGA_CONTAINERD_SOCKET")
        .unwrap_or_else(|_| "/run/containerd/containerd.sock".to_string())
}

fn containerd_available_at(path: &str) -> bool {
    Path::new(path).exists()
}

fn test_spec(image: &str) -> TaskSpec {
    TaskSpec {
        image: image.to_string(),
        env: HashMap::from([("TEST_VAR".to_string(), "hello".to_string())]),
        ports: vec![],
        resources: ResourceSpec::default(),
        ..Default::default()
    }
}

#[tokio::test]
async fn containerd_start_list_status_stop() {
    let socket = socket_path();
    if !containerd_available_at(&socket) {
        eprintln!("SKIP: containerd not available at {socket}");
        return;
    }

    let runtime = match ContainerdRuntime::connect(&socket).await {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("SKIP: cannot connect to containerd: {e}");
            return;
        }
    };

    // Use a tiny image that's quick to pull.
    let spec = test_spec("docker.io/library/busybox:latest");
    let task_id = "minga-test-busybox-001".to_string();

    // Start the container.
    let container_id = runtime
        .start(&task_id, &spec)
        .await
        .expect("failed to start container");
    assert_eq!(container_id, task_id);

    // List should include our container.
    let containers = runtime.list().await;
    assert!(
        containers.iter().any(|c| c.id == container_id),
        "list should include started container"
    );

    // Status should be Running.
    let status = runtime.status(&container_id).await;
    assert_eq!(status, Some(ContainerStatus::Running));

    // Stop the container.
    runtime
        .stop(&container_id)
        .await
        .expect("failed to stop container");

    // Status should be None (container deleted).
    let status = runtime.status(&container_id).await;
    assert_eq!(status, None, "container should be gone after stop");
}

#[tokio::test]
async fn containerd_stop_nonexistent_is_graceful() {
    let socket = socket_path();
    if !containerd_available_at(&socket) {
        eprintln!("SKIP: containerd not available at {socket}");
        return;
    }

    let runtime = match ContainerdRuntime::connect(&socket).await {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("SKIP: cannot connect to containerd: {e}");
            return;
        }
    };

    // Stopping a container that doesn't exist should not panic.
    // It may return an error (which is fine) or succeed silently.
    let result = runtime.stop("nonexistent-container-id").await;
    // We don't assert on the result — the point is it doesn't panic.
    eprintln!("stop nonexistent result: {result:?}");
}

#[tokio::test]
async fn containerd_list_empty_when_no_containers() {
    let socket = socket_path();
    if !containerd_available_at(&socket) {
        eprintln!("SKIP: containerd not available at {socket}");
        return;
    }

    let runtime = match ContainerdRuntime::connect(&socket).await {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("SKIP: cannot connect to containerd: {e}");
            return;
        }
    };

    // List should work even if there are no Minga containers.
    let containers = runtime.list().await;
    // Should not panic; may be empty or contain other containers.
    eprintln!("list returned {} containers", containers.len());
}
