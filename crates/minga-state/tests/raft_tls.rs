//! Integration tests for mTLS on the Raft transport.
//!
//! Gate 5 exit criteria: a cluster of TLS-credentialed nodes elects a
//! leader and replicates writes; a peer without a certificate signed by
//! the cluster CA (no cert, or a foreign CA's cert) cannot join — it
//! fails the TLS handshake before it can vote.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use minga_state::network::{RaftTlsConfig, MINGA_NODE_NAME};
use minga_state::types::{ClusterNode, NodeStatus, ResourceSpec};
use minga_state::StateStore;

/// Returns `N` distinct free localhost addresses chosen by the OS, so parallel
/// runs of this suite (other worktrees, leftover processes) never share ports.
fn free_addrs<const N: usize>() -> [String; N] {
    // Hold every listener until all are bound so the OS can't hand out a port twice.
    let listeners: [std::net::TcpListener; N] =
        std::array::from_fn(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap());
    // shortcut: the port is free between this drop and StateStore binding it; pass a
    // bound listener into StateStore::open if another process ever wins that race.
    listeners.map(|l| l.local_addr().unwrap().to_string())
}

/// A generated cluster CA plus PEMs written under `dir`.
struct Pki {
    ca: rcgen::Certificate,
    ca_key: rcgen::KeyPair,
    dir: tempfile::TempDir,
}

impl Pki {
    fn new() -> Self {
        let ca_key = rcgen::KeyPair::generate().expect("ca key");
        let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca = ca_params.self_signed(&ca_key).expect("ca cert");
        Self {
            ca,
            ca_key,
            dir: tempfile::tempdir().unwrap(),
        }
    }

    /// Issues a node certificate and writes ca.pem/node.pem/node-key.pem
    /// into a fresh subdir; returns the RaftTlsConfig pointing at them.
    fn issue(&self, name: &str) -> RaftTlsConfig {
        let key = rcgen::KeyPair::generate().expect("node key");
        let mut params =
            rcgen::CertificateParams::new(vec![MINGA_NODE_NAME.to_string()]).expect("params");
        params.subject_alt_names = vec![rcgen::SanType::DnsName(
            MINGA_NODE_NAME.try_into().expect("san"),
        )];
        let cert = params
            .signed_by(&key, &self.ca, &self.ca_key)
            .expect("sign node cert");

        let dir = self.dir.path().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("ca.pem"), self.ca.pem()).unwrap();
        std::fs::write(dir.join("node.pem"), cert.pem()).unwrap();
        std::fs::write(dir.join("node-key.pem"), key.serialize_pem()).unwrap();

        RaftTlsConfig {
            ca_cert: dir.join("ca.pem"),
            cert: dir.join("node.pem"),
            key: dir.join("node-key.pem"),
        }
    }
}

fn sample_node(id: u64, addr: &str) -> ClusterNode {
    ClusterNode {
        id,
        address: addr.to_string(),
        labels: HashMap::new(),
        status: NodeStatus::Active,
        capacity: ResourceSpec::default(),
        wg_public_key: None,
        wg_endpoint: None,
        overlay_ip: None,
        last_seen_unix: None,
        role: Default::default(),
    }
}

#[tokio::test]
async fn tls_cluster_elects_and_replicates() {
    let pki = Pki::new();
    let tls1 = pki.issue("n1");
    let tls2 = pki.issue("n2");
    let [addr1, addr2] = free_addrs();

    let dir1 = tempfile::tempdir().unwrap();
    let dir2 = tempfile::tempdir().unwrap();

    let (r1, r2) = tokio::join!(
        StateStore::open_secure(
            1,
            &addr1,
            dir1.path(),
            BTreeMap::from([(2, addr2.clone())]),
            tls1
        ),
        StateStore::open_secure(
            2,
            &addr2,
            dir2.path(),
            BTreeMap::from([(1, addr1.clone())]),
            tls2
        ),
    );
    let store1 = r1.expect("node 1 starts");
    let store2 = r2.expect("node 2 starts");
    std::mem::forget(dir1);
    std::mem::forget(dir2);

    store1.upsert_node(sample_node(1, &addr1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let seen = store2.get_node(1).await.unwrap();
    assert!(seen.is_some(), "write must replicate over mTLS");
}

#[tokio::test]
async fn uncredentialed_peer_cannot_join() {
    let pki = Pki::new();
    let tls1 = pki.issue("n1");
    let [addr1, addr2] = free_addrs();

    // Node 1 runs with TLS; node 2 connects WITHOUT any credentials.
    let dir1 = tempfile::tempdir().unwrap();
    let store1 = StateStore::open_secure(1, &addr1, dir1.path(), BTreeMap::new(), tls1)
        .await
        .expect("node 1 starts");
    std::mem::forget(dir1);

    let dir2 = tempfile::tempdir().unwrap();
    let plain = StateStore::open(2, &addr2, dir2.path(), BTreeMap::from([(1, addr1.clone())]));
    let joined = tokio::time::timeout(Duration::from_secs(3), plain).await;
    assert!(
        joined.is_err(),
        "a plaintext peer must never see a leader on an mTLS cluster"
    );
    std::mem::forget(dir2);

    // Node 1 itself is unaffected.
    store1.upsert_node(sample_node(1, &addr1)).await.unwrap();
}

#[tokio::test]
async fn foreign_ca_peer_cannot_join() {
    let pki = Pki::new();
    let tls1 = pki.issue("n1");
    let foreign = Pki::new();
    let tls2_foreign = foreign.issue("n2");
    let [addr1, addr2] = free_addrs();

    let dir1 = tempfile::tempdir().unwrap();
    let _store1 = StateStore::open_secure(1, &addr1, dir1.path(), BTreeMap::new(), tls1)
        .await
        .expect("node 1 starts");
    std::mem::forget(dir1);

    let dir2 = tempfile::tempdir().unwrap();
    let attempt = StateStore::open_secure(
        2,
        &addr2,
        dir2.path(),
        BTreeMap::from([(1, addr1.clone())]),
        tls2_foreign,
    );
    let joined = tokio::time::timeout(Duration::from_secs(3), attempt).await;
    assert!(
        joined.is_err(),
        "a peer with a foreign CA's certificate must not join"
    );
    std::mem::forget(dir2);
}
