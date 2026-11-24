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
        msg.contains("pull docker.io/library/minga-nonexistent-image:never")
            || msg.contains("still missing"),
        "should report pull failure or missing image: {msg}"
    );
}

/// A failed pull must be reported before any network is set up. The runtime
/// here has no node networking, so a task with IPs fails on that first if the
/// network is prepared ahead of the pull.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn pull_failure_comes_before_network_setup() {
    let socket = socket_path();
    if !containerd_available_at(&socket) {
        eprintln!("SKIP: containerd not available at {socket}");
        return;
    }
    let Ok(runtime) = ContainerdRuntime::connect(&socket).await else {
        eprintln!("SKIP: cannot connect to containerd");
        return;
    };
    let mut spec = test_spec("docker.io/library/minga-nonexistent-image:never");
    spec.ips = HashMap::from([("net".to_string(), "10.99.0.2".to_string())]);
    let err = runtime
        .start(&"minga-pull-failure-probe".to_string(), &spec)
        .await
        .expect_err("missing image must fail closed");
    let msg = err.to_string();
    assert!(
        !msg.contains("networking is not configured"),
        "the network must not be prepared before the image is pulled: {msg}"
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

/// A root task cannot write host-global kernel knobs or read kernel memory:
/// /proc/sys is read-only and /proc/kcore is masked. Checked with `test`
/// only, so a regression never writes the CI host's core_pattern.
#[tokio::test]
async fn containerd_kernel_paths_are_masked_and_read_only() {
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

    let task_id = "minga-test-kernel-paths-001".to_string();
    let spec = minga_state::types::TaskSpec {
        image: "docker.io/library/busybox:latest".to_string(),
        command: vec![
            "sh".to_string(),
            "-c".to_string(),
            // busybox `test -w` is always true for root, so read the mount flags instead.
            "if grep -qE '^[^ ]+ /proc/sys [^ ]+ ro[, ]' /proc/self/mounts; then echo sys-protected; else echo sys-writable; fi; \
             if test -s /proc/kcore; then echo kcore-visible; else echo kcore-masked; fi; sleep 30"
                .to_string(),
        ],
        ..Default::default()
    };
    runtime
        .start(&task_id, &spec)
        .await
        .expect("failed to start container");

    let mut out = String::new();
    for _ in 0..20 {
        out = runtime
            .logs(&task_id, 0)
            .await
            .iter()
            .filter(|c| c.stream == minga_node::runtime::LogStream::Stdout)
            .map(|c| String::from_utf8_lossy(&c.data).into_owned())
            .collect();
        if out.contains("kcore-") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    runtime
        .stop(&task_id)
        .await
        .expect("failed to stop container");
    assert!(
        out.contains("sys-protected"),
        "/proc/sys must be read-only: {out:?}"
    );
    assert!(
        out.contains("kcore-masked"),
        "/proc/kcore must be masked: {out:?}"
    );
}

/// A named `user:` resolves against the image's own /etc/passwd and
/// /etc/group (BusyBox: user `nobody` is 65534, group `root` is 0).
#[tokio::test]
async fn containerd_named_user_resolves_from_the_image() {
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

    // Starts a busybox task as `user`, returning its `id` and `id -G` output.
    let run = |task_id: &'static str, user: &'static str| {
        let runtime = &runtime;
        async move {
            let task_id = task_id.to_string();
            let spec = TaskSpec {
                image: "docker.io/library/busybox:latest".to_string(),
                user: Some(user.to_string()),
                command: vec![
                    "sh".into(),
                    "-c".into(),
                    "id; echo G=$(id -G); sleep 30".into(),
                ],
                ..Default::default()
            };
            runtime
                .start(&task_id, &spec)
                .await
                .expect("a user from the image's passwd must start");
            let mut out = Vec::new();
            for _ in 0..20 {
                out = runtime
                    .logs(&task_id, 0)
                    .await
                    .into_iter()
                    .flat_map(|c| c.data)
                    .collect();
                if out.windows(2).any(|w| w == b"G=") && out.ends_with(b"\n") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            runtime.stop(&task_id).await.expect("stop");
            String::from_utf8_lossy(&out).into_owned()
        }
    };

    let out = run("minga-test-named-user-001", "nobody:root").await;
    assert!(
        out.contains("uid=65534") && out.contains("gid=0"),
        "process must run as nobody:root, got {out:?}"
    );

    // A named user is a member of the image's groups: busybox lists root
    // in `wheel` (gid 10), which must be a supplementary group.
    let out = run("minga-test-named-user-003", "root").await;
    assert!(out.contains("G=0 10"), "root must be in wheel, got {out:?}");

    // A numeric uid in the image's passwd takes its primary group, not 0.
    let out = run("minga-test-named-user-004", "65534").await;
    assert!(
        out.contains("uid=65534") && out.contains("gid=65534"),
        "a listed uid must run with its primary gid, got {out:?}"
    );

    // An unknown name fails the start and rolls its snapshot back.
    let unknown = TaskSpec {
        image: "docker.io/library/busybox:latest".to_string(),
        user: Some("no-such-user".to_string()),
        ..Default::default()
    };
    let error = runtime
        .start(&"minga-test-named-user-002".to_string(), &unknown)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("no user `no-such-user`"),
        "{error}"
    );
}

/// AC3 — supervisor restart: output written after a restart reaches the stream.
///
/// Real restart path: a NEW `ContainerdRuntime` instance adopts an already-running
/// container by calling `open_log_fifos`. The original instance's `log_readers`
/// guard does NOT fire because the new instance has empty state. This is exactly
/// what `recover_running_containers` does on a node restart.
///
/// Steps:
/// 1. Start a container on `runtime` (first instance); wait for initial output.
/// 2. Create `runtime2` — a fresh `ContainerdRuntime::connect()` with the SAME
///    `logs_dir`. Its `log_readers` map is empty, so the guard cannot fire.
/// 3. Call `runtime2.open_log_fifos(&container_id)` — this is the restart path.
/// 4. Assert that subsequent output arrives in `runtime2.logs()`.
///
/// **Red-first evidence:** without the production `open_log_fifos` implementation
/// (or with the old same-instance call) `runtime2`'s ring stays empty and
/// `saw_after` never fires — the assert at the bottom of this function fails.
///
/// This test requires containerd and skips on macOS or CI without the daemon.
/// `MINGA_REQUIRE_CONTAINERD=1` turns the skip into a hard failure.
#[tokio::test]
async fn supervisor_restart_adopted_container_logs_reach_stream_ac3() {
    let socket = socket_path();
    if !containerd_available_at(&socket) {
        eprintln!("SKIP: containerd not available at {socket}");
        return;
    }

    // --- First instance: start the container and confirm initial output ---
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

    // Start a container that writes a "before" marker then keeps writing "after" lines.
    let task_id = "minga-ac3-restart-logs-002".to_string();
    let spec = minga_state::types::TaskSpec {
        image: "docker.io/library/busybox:latest".to_string(),
        command: vec![
            "sh".to_string(),
            "-c".to_string(),
            // Write a "before" marker, then keep emitting "after" lines.
            "echo before-restart; while true; do echo after-restart; sleep 0.1; done".to_string(),
        ],
        ..Default::default()
    };

    runtime
        .start(&task_id, &spec)
        .await
        .expect("failed to start AC3 container");

    let container_id = task_id.clone();

    // Wait for the "before-restart" line from the first instance.
    let mut saw_before = false;
    for _ in 0..40 {
        let chunks = runtime.logs(&container_id, 0).await;
        let all: String = chunks
            .iter()
            .flat_map(|c| std::str::from_utf8(&c.data).unwrap_or("").chars())
            .collect();
        if all.contains("before-restart") {
            saw_before = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        saw_before,
        "initial output must arrive before simulating restart"
    );

    // --- Simulate supervisor restart ---
    // Create a SECOND ContainerdRuntime instance with the same logs_dir.
    // Its log_readers map is empty, so open_log_fifos will NOT be short-circuited
    // by the contains_key guard. This is the actual restart path that
    // recover_running_containers exercises.
    let runtime2 = ContainerdRuntime::connect(&socket)
        .await
        .expect("second runtime connect failed");
    let runtime2 = runtime2.with_logs_dir(logs_dir.path());

    // open_log_fifos on the new instance — mirrors recover_running_containers.
    runtime2.open_log_fifos(&container_id).await;

    // Output written after re-open must arrive in runtime2's log ring.
    // (runtime2 has no log ring for this container until open_log_fifos runs.)
    let mut saw_after = false;
    for _ in 0..40 {
        let chunks = runtime2.logs(&container_id, 0).await;
        let all: String = chunks
            .iter()
            .flat_map(|c| std::str::from_utf8(&c.data).unwrap_or("").chars())
            .collect();
        if all.contains("after-restart") {
            saw_after = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        saw_after,
        "output written after supervisor restart must reach the log stream via the new runtime instance"
    );

    // Cleanup: stop via the first instance (which owns the containerd task).
    runtime
        .stop(&container_id)
        .await
        .expect("failed to stop AC3 container");
}
