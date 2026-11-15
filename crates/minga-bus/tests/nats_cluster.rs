//! [`NatsBus`] against a 3-server NATS cluster: replicated streams survive
//! losing one server.
//!
//! Skips when Docker is unavailable, like `nats_integration`.
//!
//! To run: `cargo test -p minga-bus --test nats_cluster -- --nocapture`

use std::time::Duration;

use minga_bus::{Command, EventBus, NatsBus};
use testcontainers::{
    core::{logs::LogFrame, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

const NATS_PORT: u16 = 4222;
const MONITOR_PORT: u16 = 8222;

/// Starts three JetStream servers routed to each other on a private network.
async fn start_cluster() -> Option<(Vec<ContainerAsync<GenericImage>>, String)> {
    let tag = std::process::id();
    let network = format!("minga-nats-{tag}");
    let names: Vec<String> = (1..=3).map(|i| format!("minga-nats-{tag}-{i}")).collect();
    let routes = names
        .iter()
        .map(|n| format!("nats://{n}:6222"))
        .collect::<Vec<_>>()
        .join(",");

    let starts = names.iter().map(|name| {
        let prefix = name.clone();
        GenericImage::new("nats", "2.10-alpine")
            .with_exposed_port(NATS_PORT.into())
            .with_exposed_port(MONITOR_PORT.into())
            .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
            .with_network(&network)
            .with_container_name(name)
            // Server logs (debug level, with the peer-selection reasons) show
            // only when the test fails: the harness captures them otherwise.
            .with_log_consumer(move |frame: &LogFrame| {
                eprint!("[{prefix}] {}", String::from_utf8_lossy(frame.bytes()));
            })
            .with_cmd(vec![
                "-js".to_string(),
                "-D".to_string(),
                "-m".to_string(),
                MONITOR_PORT.to_string(),
                "-n".to_string(),
                name.clone(),
                "--cluster_name".to_string(),
                "minga".to_string(),
                "--cluster".to_string(),
                "nats://0.0.0.0:6222".to_string(),
                "--routes".to_string(),
                routes.clone(),
            ])
            .start()
    });

    // Started together, so no server joins after the meta leader is elected.
    let mut containers = Vec::new();
    for started in futures::future::join_all(starts).await {
        match started {
            Ok(c) => containers.push(c),
            Err(e) => {
                eprintln!("skipping: cannot start NATS container: {e}");
                return None;
            }
        }
    }
    let mut urls = Vec::new();
    let mut monitors = Vec::new();
    for container in &containers {
        let port = container.get_host_port_ipv4(NATS_PORT).await.ok()?;
        urls.push(format!("nats://127.0.0.1:{port}"));
        let port = container.get_host_port_ipv4(MONITOR_PORT).await.ok()?;
        monitors.push(format!("http://127.0.0.1:{port}/jsz"));
    }
    wait_for_meta_group(&monitors).await;
    Some((containers, urls.join(",")))
}

/// Waits until every server reports a meta leader and a three-peer meta
/// group. "Server is ready" comes before either, and a meta group short of
/// three peers is what fails R3 placement with a bare "no suitable peers".
async fn wait_for_meta_group(monitors: &[String]) {
    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("http client");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let mut views = Vec::new();
        for url in monitors {
            let view = match http.get(url).send().await {
                Ok(resp) => resp.json::<serde_json::Value>().await.ok(),
                Err(_) => None,
            };
            views.push(view.map(|jsz| jsz["meta_cluster"].clone()));
        }
        let formed = views.iter().all(|meta| {
            meta.as_ref().is_some_and(|m| {
                m["cluster_size"] == 3 && m["leader"].as_str().is_some_and(|l| !l.is_empty())
            })
        });
        if formed {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "JetStream meta group did not reach three peers: {views:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn replicas(bus: &NatsBus, stream: &str) -> usize {
    bus.jetstream()
        .get_stream(stream)
        .await
        .unwrap_or_else(|e| panic!("{stream}: {e}"))
        .info()
        .await
        .unwrap_or_else(|e| panic!("{stream} info: {e}"))
        .config
        .num_replicas
}

/// Retries `f` until it succeeds or 30s pass: after a server dies, the
/// replicated streams need a few seconds to elect new leaders.
async fn eventually<T, F, Fut>(what: &str, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = minga_bus::Result<T>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match f().await {
            Ok(v) => return v,
            Err(e) if tokio::time::Instant::now() < deadline => {
                eprintln!("{what}: {e}; retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("{what}: {e}"),
        }
    }
}

#[tokio::test]
async fn replicated_bus_survives_losing_one_server() {
    let Some((containers, seeds)) = start_cluster().await else {
        return;
    };

    let bus = NatsBus::connect(&seeds)
        .await
        .expect("connect to the seed list");
    // Every server is a meta peer by now, but the leader learns each peer's
    // limits from its periodic stats; until then placement reports it offline.
    eventually("raise replicas", || bus.ensure_replicas(3)).await;
    for stream in [
        "MINGA_EVENTS",
        "MINGA_COMMANDS",
        "MINGA_DEAD_LETTERS",
        "KV_MINGA_STATUS",
        "KV_MINGA_LEASES",
    ] {
        assert_eq!(replicas(&bus, stream).await, 3, "{stream}");
    }
    assert_eq!(replicas(&bus, "MINGA_LOGS").await, 1);

    // A node left at the default neither shrinks streams nor buckets.
    let worker = NatsBus::connect(&seeds).await.expect("second connect");
    worker.ensure_replicas(1).await.expect("ensure 1");
    assert_eq!(replicas(&bus, "KV_MINGA_STATUS").await, 3);
    assert_eq!(replicas(&bus, "MINGA_COMMANDS").await, 3);

    // New copies catch up for a moment after scaling; writes can time out.
    eventually("put", || bus.put_status("task.ha", b"running")).await;
    let before = Command::Stop {
        task_id: "before-failover".to_string(),
    };
    eventually("send", || bus.send_command(77, &before)).await;

    containers[0].stop().await.expect("stop one server");

    let status = eventually("get_status", || bus.get_status("task.ha")).await;
    assert_eq!(status.as_deref(), Some(&b"running"[..]));
    let received = eventually("next_command", || bus.next_command(77)).await;
    assert_eq!(received, before);

    let after = Command::Stop {
        task_id: "after-failover".to_string(),
    };
    eventually("send after failover", || bus.send_command(77, &after)).await;
    let received = eventually("next_command after failover", || bus.next_command(77)).await;
    assert_eq!(received, after);
}
