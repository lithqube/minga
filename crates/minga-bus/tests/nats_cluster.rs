//! [`NatsBus`] against a 3-server NATS cluster: replicated streams survive
//! losing one server.
//!
//! To run: `cargo test -p minga-bus --test nats_cluster -- --nocapture`

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use minga_bus::{EventBus, NatsBus};

struct ClusterServer {
    child: Child,
    _store: tempfile::TempDir,
}

impl Drop for ClusterServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn nats_server_bin() -> PathBuf {
    if let Ok(path) = std::env::var("MINGA_NATS_SERVER_BIN") {
        let path = PathBuf::from(path);
        assert!(
            path.is_file(),
            "MINGA_NATS_SERVER_BIN is not a file: {}",
            path.display()
        );
        return path;
    }
    std::env::var_os("PATH")
        .into_iter()
        .flat_map(|paths| std::env::split_paths(&paths).collect::<Vec<_>>())
        .map(|dir| dir.join("nats-server"))
        .find(|path| path.is_file())
        .expect("nats-server is required; install it or set MINGA_NATS_SERVER_BIN")
}

fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read ephemeral port")
        .port()
}

/// Starts three JetStream server processes routed to each other.
async fn start_cluster() -> (Vec<ClusterServer>, String, Vec<String>) {
    let binary = nats_server_bin();
    let tag = std::process::id();
    let names: Vec<String> = (1..=3).map(|i| format!("minga-nats-{tag}-{i}")).collect();
    let client_ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let monitor_ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let route_ports: Vec<u16> = (0..3).map(|_| free_port()).collect();
    let routes = route_ports
        .iter()
        .map(|port| format!("nats://127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    let mut servers = Vec::new();
    let mut urls = Vec::new();

    for index in 0..3 {
        let store = tempfile::tempdir().expect("create JetStream store");
        let child = Command::new(&binary)
            .arg("-js")
            .arg("-p")
            .arg(client_ports[index].to_string())
            .arg("-m")
            .arg(monitor_ports[index].to_string())
            .arg("-n")
            .arg(&names[index])
            .arg("--cluster_name")
            .arg("minga")
            .arg("--cluster")
            .arg(format!("nats://127.0.0.1:{}", route_ports[index]))
            .arg("--routes")
            .arg(&routes)
            .arg("--store_dir")
            .arg(store.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start nats-server");
        servers.push(ClusterServer {
            child,
            _store: store,
        });
        urls.push(format!("nats://127.0.0.1:{}", client_ports[index]));
    }
    let monitors: Vec<String> = monitor_ports
        .iter()
        .map(|port| format!("http://127.0.0.1:{port}/jsz"))
        .collect();
    wait_for_meta_group(&monitors).await;
    (servers, urls.join(","), names)
}

/// Waits until every server reports a meta leader and a three-peer meta
/// group. "Server is ready" comes before either, and a meta group short of
/// three peers is what fails R3 placement with a bare "no suitable peers".
async fn wait_for_meta_group(monitors: &[String]) {
    let http = reqwest::Client::builder()
        .no_proxy()
        // Covers send and body: a stalled monitor counts as not formed.
        .timeout(Duration::from_secs(2))
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
    let (mut servers, seeds, names) = start_cluster().await;

    let bus = NatsBus::connect(&seeds)
        .await
        .expect("connect to the seed list");
    // Every server is a meta peer by now, but the leader learns each peer's
    // limits from its periodic stats; until then placement reports it offline.
    eventually("raise replicas", || bus.ensure_replicas(3)).await;
    for stream in [
        "MINGA_EVENTS",
        "KV_MINGA_STATUS",
        "KV_MINGA_LEASES",
        "KV_MINGA_ASSIGN",
    ] {
        assert_eq!(replicas(&bus, stream).await, 3, "{stream}");
    }
    assert_eq!(replicas(&bus, "MINGA_LOGS").await, 1);

    // A node left at the default neither shrinks streams nor buckets.
    let worker = NatsBus::connect(&seeds).await.expect("second connect");
    worker.ensure_replicas(1).await.expect("ensure 1");
    assert_eq!(replicas(&bus, "KV_MINGA_STATUS").await, 3);
    assert_eq!(replicas(&bus, "KV_MINGA_ASSIGN").await, 3);

    // New copies catch up for a moment after scaling; writes can time out.
    eventually("put", || bus.put_status("internal.ha-test", b"running")).await;
    // `ensure_replicas` returns once the server accepts R3. Stopping a server
    // before the new followers are current leaves no quorum to elect a leader.
    eventually("followers current", || async {
        let mut stream = bus
            .jetstream()
            .get_stream("KV_MINGA_STATUS")
            .await
            .map_err(|e| minga_bus::BusError::Other(e.to_string()))?;
        let info = stream
            .info()
            .await
            .map_err(|e| minga_bus::BusError::Other(e.to_string()))?;
        let cluster = info.cluster.as_ref();
        let current = cluster.is_some_and(|c| {
            c.leader.is_some() && c.replicas.len() == 2 && c.replicas.iter().all(|p| p.current)
        });
        if current {
            Ok(())
        } else {
            Err(minga_bus::BusError::Other(format!(
                "followers not current: {cluster:?}"
            )))
        }
    })
    .await;

    // Keep the status leader online so this verifies data availability after a
    // replica loss without making the test depend on JetStream election timing.
    let mut status_stream = bus
        .jetstream()
        .get_stream("KV_MINGA_STATUS")
        .await
        .expect("get status stream before failover");
    let status_leader = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let status_info = status_stream.info().await.expect("status stream info");
            if let Some(leader) = status_info
                .cluster
                .as_ref()
                .and_then(|cluster| cluster.leader.as_deref())
                .filter(|leader| !leader.is_empty())
            {
                break leader.to_owned();
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "status stream leader was not reported"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let leader_index = names
        .iter()
        .position(|name| name == &status_leader)
        .unwrap_or_else(|| panic!("unknown status leader {status_leader}"));
    let seed_urls: Vec<_> = seeds.split(',').collect();
    let survivor = NatsBus::connect(seed_urls[leader_index])
        .await
        .expect("connect directly to the status stream leader");
    let victim = (0..servers.len())
        .find(|index| *index != leader_index)
        .expect("a non-leader server");
    servers[victim].child.kill().expect("stop one server");
    servers[victim].child.wait().expect("reap stopped server");
    let status = eventually("get_status", || survivor.get_status("internal.ha-test")).await;
    assert_eq!(status.as_deref(), Some(&b"running"[..]));
}
