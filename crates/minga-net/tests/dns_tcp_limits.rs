//! Issue #137 — QA regression suite for the DNS-over-TCP listener limits.
//!
//! One test per acceptance criterion of #137, plus a regression test for the
//! required-egress refusal that PR #175 added to the same listener.
//!
//! Every test drives the public `minga_net::DnsServer` API over a real
//! loopback TCP socket, so it observes the wire behaviour a task on a bridge
//! network sees.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use minga_net::{DnsServer, DnsStore, LocalOnlySources};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

const QTYPE_A: u16 = 1;
const RCODE_NOERROR: u16 = 0;
const RCODE_REFUSED: u16 = 5;

/// The largest DNS TCP query the server accepts: the same budget as its
/// UDP/EDNS receive buffer.
const MAX_QUERY_LEN: usize = 4096;

/// Connections opened by the cap test — far above the cap the server claims,
/// so the test does not hard-code the cap number.
const CONNECTION_FLOOD: usize = 200;

/// A bound the server's own idle timeout (5 s) satisfies and an unbounded
/// listener never does.
const IDLE_CLOSED_WITHIN: Duration = Duration::from_secs(15);

/// A bound short enough to sit below the server's idle timeout: the server
/// must reject on the length prefix alone, without waiting for a body.
const REJECTED_WITHIN: Duration = Duration::from_secs(3);

/// A bound for a close the server causes itself (shutdown, refusal).
const CLOSED_WITHIN: Duration = Duration::from_secs(3);

/// Builds a standard DNS query: one question, RD set.
fn encode_query(id: u16, name: &str, qtype: u16) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(&0x0100u16.to_be_bytes()); // RD
    out.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    out.extend_from_slice(&0u16.to_be_bytes()); // ANCOUNT
    out.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
    out.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT
    for label in name.split('.') {
        out.push(label.len() as u8);
        out.extend_from_slice(label.as_bytes());
    }
    out.push(0); // root label
    out.extend_from_slice(&qtype.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes()); // QCLASS IN
    out
}

/// The response code of a DNS reply body.
fn rcode(reply: &[u8]) -> u16 {
    u16::from_be_bytes([reply[2], reply[3]]) & 0x000F
}

/// Whether the reply carries the QR (it is a response) bit.
fn is_response(reply: &[u8]) -> bool {
    reply[2] & 0x80 != 0
}

/// Starts a DNS server on a free loopback port and returns it with the
/// address its TCP listener is bound to.
///
/// `DnsServer::start` binds UDP and TCP on the same address, so the port is
/// picked from the TCP space and reused for UDP.
async fn start_server(
    store: Arc<RwLock<DnsStore>>,
    upstream: Option<SocketAddr>,
    local_only: LocalOnlySources,
) -> (DnsServer, SocketAddr) {
    for attempt in 0..10 {
        let probe = TcpListener::bind("127.0.0.1:0").await.expect("bind probe");
        let addr = probe.local_addr().expect("probe address");
        drop(probe);
        match DnsServer::start(
            vec![addr],
            Arc::clone(&store),
            upstream,
            Arc::clone(&local_only),
        )
        .await
        {
            Ok(server) => return (server, addr),
            Err(e) => eprintln!("attempt {attempt}: {addr} unavailable ({e}), retrying"),
        }
    }
    panic!("no free loopback port for a DNS server");
}

/// Reads a length-prefixed DNS reply and returns its body.
async fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
    let mut len = [0u8; 2];
    tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut len))
        .await
        .expect("timed out waiting for the DNS reply length prefix")
        .expect("read the DNS reply length prefix");
    let mut body = vec![0u8; u16::from_be_bytes(len) as usize];
    stream
        .read_exact(&mut body)
        .await
        .expect("read the DNS reply body");
    body
}

/// Sends one length-prefixed DNS query and returns the reply body.
async fn tcp_query(addr: SocketAddr, query: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect to the DNS server");
    stream
        .write_all(&(query.len() as u16).to_be_bytes())
        .await
        .expect("write the DNS query length prefix");
    stream
        .write_all(query)
        .await
        .expect("write the DNS query body");
    read_reply(&mut stream).await
}

/// Asserts the peer closed the connection within `within`.
async fn assert_closed(stream: &mut TcpStream, within: Duration, what: &str) {
    let mut byte = [0u8; 1];
    match tokio::time::timeout(within, stream.read(&mut byte)).await {
        Ok(Ok(0)) => {}
        Ok(Err(_)) => {}
        Ok(Ok(n)) => panic!("{what}: expected a closed connection, got {n} bytes"),
        Err(_) => panic!("{what}: the connection was still open after {within:?}"),
    }
}

/// AC 1 — an idle TCP connection is closed within the timeout.
///
/// A client that connects and never sends a byte must not pin the listener
/// forever.
#[tokio::test]
async fn ac_idle_tcp_connection_is_closed_within_the_timeout() {
    let (server, addr) = start_server(
        Arc::new(RwLock::new(DnsStore::new())),
        None,
        LocalOnlySources::default(),
    )
    .await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    // No byte is ever sent: only a read timeout can close this connection.
    assert_closed(&mut stream, IDLE_CLOSED_WITHIN, "idle DNS TCP connection").await;

    server.shutdown().await;
}

/// AC 2 — connections beyond the cap are refused.
///
/// The flood is larger than the cap the server claims, so the test does not
/// depend on the exact number.
#[tokio::test]
async fn ac_connections_beyond_the_cap_are_refused() {
    let (server, addr) = start_server(
        Arc::new(RwLock::new(DnsStore::new())),
        None,
        LocalOnlySources::default(),
    )
    .await;

    let mut conns = Vec::with_capacity(CONNECTION_FLOOD);
    for _ in 0..CONNECTION_FLOOD {
        conns.push(TcpStream::connect(addr).await.expect("connect"));
    }
    // Give the listener time to accept the flood and refuse the excess.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut refused = 0usize;
    let mut alive: Vec<(usize, TcpStream)> = Vec::new();
    for (index, conn) in conns.into_iter().enumerate() {
        match conn.try_read(&mut [0u8; 1]) {
            Ok(0) => refused += 1,
            Ok(n) => panic!("unexpected {n} bytes on a refused connection"),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => alive.push((index, conn)),
            Err(_) => refused += 1,
        }
    }

    assert!(
        refused > 0,
        "no connection was refused: the TCP listener accepts without a cap"
    );
    assert!(!alive.is_empty(), "every connection was refused");
    eprintln!(
        "observed: {refused} of {CONNECTION_FLOOD} connections refused, {} stayed open",
        alive.len()
    );

    // The connections under the cap are still served.
    let (index, mut conn) = alive.remove(0);
    assert!(
        index < CONNECTION_FLOOD / 2,
        "the first open connection must be one the server accepted"
    );
    let query = encode_query(0x21, "web.plant-7.minga.local", QTYPE_A);
    conn.write_all(&(query.len() as u16).to_be_bytes())
        .await
        .expect("write the DNS query length prefix");
    conn.write_all(&query)
        .await
        .expect("write the DNS query body");
    let reply = read_reply(&mut conn).await;
    assert_eq!(
        rcode(&reply),
        RCODE_NOERROR,
        "a connection under the cap must still be answered"
    );

    server.shutdown().await;
}

/// AC 3 — a length prefix above the maximum is rejected before the body.
///
/// The body never arrives, so a server that only rejected after reading the
/// body would keep the connection open until its own read timeout.
#[tokio::test]
async fn ac_length_prefix_above_the_maximum_is_rejected_before_the_body() {
    let (server, addr) = start_server(
        Arc::new(RwLock::new(DnsStore::new())),
        None,
        LocalOnlySources::default(),
    )
    .await;

    for len in [MAX_QUERY_LEN as u16 + 1, u16::MAX] {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(&len.to_be_bytes())
            .await
            .expect("write the DNS query length prefix");
        assert_closed(
            &mut stream,
            REJECTED_WITHIN,
            "DNS TCP query above the maximum length",
        )
        .await;
    }

    // The server is still healthy after refusing both.
    let reply = tcp_query(
        addr,
        &encode_query(0x31, "web.plant-7.minga.local", QTYPE_A),
    )
    .await;
    assert_eq!(rcode(&reply), RCODE_NOERROR);

    server.shutdown().await;
}

/// AC 3 (boundary) — a query of exactly the maximum length is still answered.
#[tokio::test]
async fn ac_maximum_length_query_is_still_answered() {
    let (server, addr) = start_server(
        Arc::new(RwLock::new(DnsStore::new())),
        None,
        LocalOnlySources::default(),
    )
    .await;

    let mut query = encode_query(0x41, "web.plant-7.minga.local", QTYPE_A);
    query.resize(MAX_QUERY_LEN, 0);
    let reply = tcp_query(addr, &query).await;

    assert_eq!(
        u16::from_be_bytes([reply[0], reply[1]]),
        0x41,
        "the query ID must be kept"
    );
    assert!(is_response(&reply), "the QR bit must be set");
    assert_eq!(rcode(&reply), RCODE_NOERROR);

    server.shutdown().await;
}

/// AC 4 — `shutdown()` ends open connection tasks.
///
/// A connection parked in a read must not outlive the server.
#[tokio::test]
async fn ac_shutdown_ends_open_connection_tasks() {
    let (server, addr) = start_server(
        Arc::new(RwLock::new(DnsStore::new())),
        None,
        LocalOnlySources::default(),
    )
    .await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    // Half a length prefix: the connection task is parked in a read.
    stream
        .write_all(&[0x00])
        .await
        .expect("write a partial prefix");
    tokio::time::sleep(Duration::from_millis(50)).await;

    server.shutdown().await;

    assert_closed(
        &mut stream,
        CLOSED_WITHIN,
        "DNS TCP connection after shutdown",
    )
    .await;
}

/// Regression — PR #175: a source under a required-egress policy is refused
/// non-local names over TCP, and no upstream resolver is asked.
#[tokio::test]
async fn regression_required_egress_refuses_non_local_names_over_tcp() {
    // An upstream resolver that must never be asked.
    let upstream = UdpSocket::bind("127.0.0.1:0").await.expect("bind upstream");
    let upstream_addr = upstream.local_addr().expect("upstream address");
    let mut store = DnsStore::new();
    store.set(
        "web.plant-7.minga.local",
        vec![Ipv4Addr::new(10, 100, 1, 10)],
    );
    let local_only = LocalOnlySources::default();
    local_only
        .write()
        .expect("lock the confined source set")
        .insert(Ipv4Addr::LOCALHOST);

    let (server, addr) = start_server(
        Arc::new(RwLock::new(store)),
        Some(upstream_addr),
        local_only,
    )
    .await;

    // A local name is still answered locally.
    let local = tcp_query(
        addr,
        &encode_query(0x51, "web.plant-7.minga.local", QTYPE_A),
    )
    .await;
    assert_eq!(rcode(&local), RCODE_NOERROR);

    // A name that would carry data out is refused over TCP.
    let tunnel = tcp_query(
        addr,
        &encode_query(0x52, "c2VjcmV0.exfil.example.com", QTYPE_A),
    )
    .await;
    assert_eq!(rcode(&tunnel), RCODE_REFUSED);

    // The upstream resolver was never asked.
    let mut buf = [0u8; 4096];
    assert!(
        tokio::time::timeout(Duration::from_millis(500), upstream.recv_from(&mut buf))
            .await
            .is_err(),
        "the upstream resolver was queried"
    );

    server.shutdown().await;
}
