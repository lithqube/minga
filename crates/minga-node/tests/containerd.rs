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
use std::time::Duration;

use minga_node::{ContainerStatus, ContainerdRuntime, Runtime};
use minga_state::types::{ResourceSpec, TaskSpec};

/// Check if containerd is available at a custom socket (from env).
fn socket_path() -> String {
    std::env::var("MINGA_CONTAINERD_SOCKET")
        .unwrap_or_else(|_| "/run/containerd/containerd.sock".to_string())
}

fn containerd_available_at(path: &str) -> bool {
    let exists = Path::new(path).exists();
    assert!(
        exists || std::env::var_os("MINGA_REQUIRE_CONTAINERD").is_none(),
        "required containerd socket is absent: {path}"
    );
    exists
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
            assert!(
                std::env::var_os("MINGA_REQUIRE_CONTAINERD").is_none(),
                "required containerd connection failed: {e}"
            );
            eprintln!("SKIP: cannot connect to containerd: {e}");
            return;
        }
    };

    // The image must be available (or pullable) in the `minga` namespace.
    //
    // Redis is used because its default command is a long-running server:
    // BusyBox's default command is `sh`, which exits immediately when stdin
    // is closed, so a BusyBox container can never be observed in the
    // Running state. Redis is also the image the reference production
    // stack runs.
    let spec = test_spec("docker.io/library/redis:8.10.2-alpine");
    let task_id = "minga-test-redis-001".to_string();

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
async fn missing_image_fails_after_pull_attempt() {
    let socket = socket_path();
    if !containerd_available_at(&socket) {
        eprintln!("SKIP: containerd not available at {socket}");
        return;
    }
    let runtime = match ContainerdRuntime::connect(&socket).await {
        Ok(rt) => rt,
        Err(e) => {
            assert!(
                std::env::var_os("MINGA_REQUIRE_CONTAINERD").is_none(),
                "required containerd connection failed: {e}"
            );
            eprintln!("SKIP: cannot connect to containerd: {e}");
            return;
        }
    };
    let spec = test_spec("docker.io/library/minga-nonexistent-image:never");
    let err = runtime
        .start(&"minga-missing-image-probe".to_string(), &spec)
        .await
        .expect_err("missing image must fail closed");
    let msg = err.to_string();
    assert!(
        msg.contains("ctr pull") || msg.contains("still missing"),
        "should report pull failure or missing image: {msg}"
    );
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
            assert!(
                std::env::var_os("MINGA_REQUIRE_CONTAINERD").is_none(),
                "required containerd connection failed: {e}"
            );
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
            assert!(
                std::env::var_os("MINGA_REQUIRE_CONTAINERD").is_none(),
                "required containerd connection failed: {e}"
            );
            eprintln!("SKIP: cannot connect to containerd: {e}");
            return;
        }
    };

    // List should work even if there are no Minga containers.
    let containers = runtime.list().await;
    // Should not panic; may be empty or contain other containers.
    eprintln!("list returned {} containers", containers.len());
}

#[tokio::test]
async fn containerd_logs_capture_stdout_and_stderr() {
    let socket = socket_path();
    if !containerd_available_at(&socket) {
        eprintln!("SKIP: containerd not available at {socket}");
        return;
    }

    let runtime = match ContainerdRuntime::connect(&socket).await {
        Ok(rt) => rt,
        Err(e) => {
            assert!(
                std::env::var_os("MINGA_REQUIRE_CONTAINERD").is_none(),
                "required containerd connection failed: {e}"
            );
            eprintln!("SKIP: cannot connect to containerd: {e}");
            return;
        }
    };
    let logs_dir = tempfile::tempdir().unwrap();
    let runtime = runtime.with_logs_dir(logs_dir.path());

    let task_id = "minga-test-logs-001".to_string();
    let spec = minga_state::types::TaskSpec {
        image: "docker.io/library/busybox:latest".to_string(),
        command: vec![
            "sh".to_string(),
            "-c".to_string(),
            "echo out-line-001; echo err-line-002 >&2; sleep 30".to_string(),
        ],
        ..Default::default()
    };

    runtime
        .start(&task_id, &spec)
        .await
        .expect("failed to start log container");

    // The FIFO reader drains asynchronously; give it a moment.
    let mut chunks = Vec::new();
    for _ in 0..20 {
        chunks = runtime.logs(&task_id, 0).await;
        if chunks.len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let stdout: Vec<&[u8]> = chunks
        .iter()
        .filter(|c| c.stream == minga_node::runtime::LogStream::Stdout)
        .map(|c| c.data.as_slice())
        .collect();
    let stderr: Vec<&[u8]> = chunks
        .iter()
        .filter(|c| c.stream == minga_node::runtime::LogStream::Stderr)
        .map(|c| c.data.as_slice())
        .collect();
    assert!(
        stdout
            .iter()
            .any(|c| c.windows(12).any(|w| w == b"out-line-001")),
        "stdout must contain the marker line, got {stdout:?}"
    );
    assert!(
        stderr
            .iter()
            .any(|c| c.windows(12).any(|w| w == b"err-line-002")),
        "stderr must contain the marker line, got {stderr:?}"
    );

    runtime
        .stop(&task_id)
        .await
        .expect("failed to stop log container");

    // Reader and FIFOs are cleaned up on stop.
    assert!(
        !logs_dir
            .path()
            .join(format!("{task_id}.stdout.fifo"))
            .exists(),
        "FIFO must be removed on stop"
    );
}
