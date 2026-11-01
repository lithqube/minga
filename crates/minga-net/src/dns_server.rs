//! DNS server for the overlay: answers `minga.local` service-discovery
//! queries from a [`DnsStore`] and proxies everything else upstream.
//!
//! Each overlay network's bridge gateway IP runs a `DnsServer` on UDP/TCP
//! port 53. Containers point their resolver at the gateway IP (Docker
//! embedded-DNS style). Queries for names under `minga.local` are answered
//! from the local store; all other queries are proxied to the host's
//! upstream resolver (read from `/etc/resolv.conf` by the caller).
//!
//! Semantics mirror Docker's embedded DNS:
//! - `minga.local` names **never** get NXDOMAIN — unknown names return
//!   NOERROR with an empty answer section, so containers don't fall through
//!   to retrying search domains.
//! - Non-A queries for `minga.local` names return NOERROR empty.
//! - Everything else is proxied verbatim (raw datagram) to the upstream
//!   resolver over UDP, preserving the query ID.
//! - With no upstream configured, non-overlay queries get REFUSED.
//!
//! The wire format (header, question, A records) is hand-rolled; no DNS
//! crates are used.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::dns::{is_minga_name, DnsStore};

/// TTL (seconds) for synthesized A records.
const ANSWER_TTL_SECS: u32 = 60;

const QTYPE_A: u16 = 1;
const QCLASS_IN: u16 = 1;
const RCODE_NOERROR: u16 = 0;
const RCODE_REFUSED: u16 = 5;

/// How long to wait for an upstream resolver before giving up.
const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(3);

/// Receive buffer size; covers EDNS-sized datagrams.
const MAX_DATAGRAM: usize = 4096;

/// A DNS server answering overlay queries on one or more bind addresses.
///
/// Dropping the server does not stop the listener tasks; call
/// [`DnsServer::shutdown`] for a clean stop.
pub struct DnsServer {
    tasks: Vec<JoinHandle<()>>,
    addrs: Vec<SocketAddr>,
}

impl DnsServer {
    /// Binds UDP+TCP listeners on each `bind` address (one per bridge
    /// gateway IP). `store` is shared and read per-query; `upstream`
    /// (e.g. `127.0.0.53:53` from `/etc/resolv.conf`) receives
    /// non-`minga.local` queries via UDP proxying.
    pub async fn start(
        binds: Vec<SocketAddr>,
        store: Arc<RwLock<DnsStore>>,
        upstream: Option<SocketAddr>,
    ) -> io::Result<Self> {
        let mut tasks = Vec::new();
        let mut addrs = Vec::new();
        for bind in binds {
            let udp = UdpSocket::bind(bind).await?;
            let tcp = TcpListener::bind(bind).await?;
            addrs.push(udp.local_addr()?);

            let store_udp = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                udp_loop(udp, store_udp, upstream).await;
            }));

            let store_tcp = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                tcp_loop(tcp, store_tcp, upstream).await;
            }));
        }
        Ok(Self { tasks, addrs })
    }

    /// The addresses the UDP listeners are bound to (one per bind given to
    /// [`DnsServer::start`]). TCP listeners use the same address for
    /// non-ephemeral ports.
    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        self.addrs.clone()
    }

    /// Stops all listener tasks.
    pub async fn shutdown(self) {
        for task in &self.tasks {
            task.abort();
        }
        for task in self.tasks {
            let _ = task.await;
        }
    }
}

// ---------------------------------------------------------------------------
// Listener loops
// ---------------------------------------------------------------------------

async fn udp_loop(socket: UdpSocket, store: Arc<RwLock<DnsStore>>, upstream: Option<SocketAddr>) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((n, peer)) => {
                if let Some(resp) = handle_query(&buf[..n], &store, upstream).await {
                    if let Err(e) = socket.send_to(&resp, peer).await {
                        debug!(error = %e, %peer, "failed to send DNS response");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "DNS UDP recv error; stopping listener");
                return;
            }
        }
    }
}

async fn tcp_loop(
    listener: TcpListener,
    store: Arc<RwLock<DnsStore>>,
    upstream: Option<SocketAddr>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let store = Arc::clone(&store);
                tokio::spawn(async move {
                    if let Err(e) = handle_tcp_conn(stream, store, upstream).await {
                        debug!(error = %e, "DNS TCP connection failed");
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "DNS TCP accept error; stopping listener");
                return;
            }
        }
    }
}

/// Handles a single TCP connection: one length-prefixed query (RFC 1035
/// §4.2.2), one response, then close.
async fn handle_tcp_conn(
    mut stream: TcpStream,
    store: Arc<RwLock<DnsStore>>,
    upstream: Option<SocketAddr>,
) -> io::Result<()> {
    let mut len_buf = [0u8; 2];
    stream.read_exact(&mut len_buf).await?;
    let len = u16::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    if let Some(resp) = handle_query(&buf, &store, upstream).await {
        stream.write_all(&(resp.len() as u16).to_be_bytes()).await?;
        stream.write_all(&resp).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Query handling
// ---------------------------------------------------------------------------

/// Produces a response datagram for `datagram`, or `None` if the query is
/// malformed (dropped silently) or an upstream proxy attempt failed.
async fn handle_query(
    datagram: &[u8],
    store: &Arc<RwLock<DnsStore>>,
    upstream: Option<SocketAddr>,
) -> Option<Vec<u8>> {
    let q = match Question::parse(datagram) {
        Ok(q) => q,
        Err(()) => {
            debug!("dropping malformed DNS datagram");
            return None;
        }
    };
    // Local names are answered locally for every query type — never
    // proxied upstream. A service or task name must resolve even as a
    // bare name (musl-based images do not expand search domains), exactly
    // like Docker's embedded DNS on an overlay network. Non-A queries get
    // NOERROR with an empty answer: a companion AAAA lookup returning
    // NXDOMAIN would make musl discard the good A answer.
    let known = is_minga_name(&q.name) || store.read().map(|s| s.exists(&q.name)).unwrap_or(false);
    if known {
        let ips = if q.qtype == QTYPE_A && q.qclass == QCLASS_IN {
            store.read().map(|s| s.resolve(&q.name)).unwrap_or_default()
        } else {
            Vec::new()
        };
        debug!(name = %q.name, found = ips.len(), "dns: overlay query");
        // NOERROR even when empty: never NXDOMAIN for overlay names, so
        // containers don't retry search domains (Docker DNS semantics).
        return Some(build_response(datagram, &q, &ips, RCODE_NOERROR));
    }
    if let Some(upstream) = upstream {
        proxy_udp(datagram, upstream).await
    } else {
        Some(build_response(datagram, &q, &[], RCODE_REFUSED))
    }
}

/// Forwards the raw query datagram to `upstream` and relays the response
/// verbatim (the upstream preserves the query ID).
async fn proxy_udp(datagram: &[u8], upstream: SocketAddr) -> Option<Vec<u8>> {
    let bind: SocketAddr = if upstream.is_ipv4() {
        "0.0.0.0:0".parse().unwrap()
    } else {
        "[::]:0".parse().unwrap()
    };
    let socket = UdpSocket::bind(bind).await.ok()?;
    if let Err(e) = socket.send_to(datagram, upstream).await {
        debug!(error = %e, %upstream, "upstream DNS send failed");
        return None;
    }
    let mut buf = vec![0u8; MAX_DATAGRAM];
    match tokio::time::timeout(UPSTREAM_TIMEOUT, socket.recv(&mut buf)).await {
        Ok(Ok(n)) => Some(buf[..n].to_vec()),
        Ok(Err(e)) => {
            debug!(error = %e, %upstream, "upstream DNS recv failed");
            None
        }
        Err(_) => {
            debug!(%upstream, "upstream DNS query timed out");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

/// A parsed DNS query: header fields plus the single question.
struct Question {
    id: u16,
    flags: u16,
    /// Lowercased, dot-separated query name.
    name: String,
    qtype: u16,
    qclass: u16,
    /// Offset just past the question section in the original datagram.
    end: usize,
}

impl Question {
    fn parse(buf: &[u8]) -> Result<Self, ()> {
        if buf.len() < 12 {
            return Err(());
        }
        let id = u16::from_be_bytes([buf[0], buf[1]]);
        let flags = u16::from_be_bytes([buf[2], buf[3]]);
        if flags & 0x8000 != 0 {
            return Err(()); // response, not a query
        }
        let qdcount = u16::from_be_bytes([buf[4], buf[5]]);
        if qdcount != 1 {
            return Err(());
        }
        let mut pos = 12usize;
        let mut labels = Vec::new();
        loop {
            let len = *buf.get(pos).ok_or(())?;
            if len == 0 {
                pos += 1;
                break;
            }
            if len & 0xC0 != 0 {
                return Err(()); // compression pointers not valid in a question
            }
            let end = pos + 1 + len as usize;
            if end > buf.len() {
                return Err(());
            }
            let label = std::str::from_utf8(&buf[pos + 1..end]).map_err(|_| ())?;
            labels.push(label.to_lowercase());
            pos = end;
        }
        if pos + 4 > buf.len() {
            return Err(());
        }
        let qtype = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
        let qclass = u16::from_be_bytes([buf[pos + 2], buf[pos + 3]]);
        Ok(Self {
            id,
            flags,
            name: labels.join("."),
            qtype,
            qclass,
            end: pos + 4,
        })
    }
}

/// Builds a response datagram: same ID and question as the query, QR set,
/// RD/opcode copied from the query, and one A record per IP (name encoded
/// as a compression pointer to the question).
fn build_response(query: &[u8], q: &Question, ips: &[Ipv4Addr], rcode: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(q.end + ips.len() * 16);
    out.extend_from_slice(&q.id.to_be_bytes());
    // QR | opcode (copied) | RD (copied) | rcode
    let flags: u16 = 0x8000 | (q.flags & 0x7800) | (q.flags & 0x0100) | rcode;
    out.extend_from_slice(&flags.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    out.extend_from_slice(&(ips.len() as u16).to_be_bytes()); // ANCOUNT
    out.extend_from_slice(&0u16.to_be_bytes()); // NSCOUNT
    out.extend_from_slice(&0u16.to_be_bytes()); // ARCOUNT
    out.extend_from_slice(&query[12..q.end]); // question, echoed verbatim
    for ip in ips {
        out.extend_from_slice(&0xC00Cu16.to_be_bytes()); // NAME: pointer to question
        out.extend_from_slice(&QTYPE_A.to_be_bytes());
        out.extend_from_slice(&QCLASS_IN.to_be_bytes());
        out.extend_from_slice(&ANSWER_TTL_SECS.to_be_bytes());
        out.extend_from_slice(&4u16.to_be_bytes()); // RDLENGTH
        out.extend_from_slice(&ip.octets());
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a standard query datagram for `name` (one question, RD set).
    fn encode_query(id: u16, name: &str, qtype: u16) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&id.to_be_bytes());
        out.extend_from_slice(&0x0100u16.to_be_bytes()); // RD
        out.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        for label in name.split('.') {
            out.push(label.len() as u8);
            out.extend_from_slice(label.as_bytes());
        }
        out.push(0);
        out.extend_from_slice(&qtype.to_be_bytes());
        out.extend_from_slice(&QCLASS_IN.to_be_bytes());
        out
    }

    struct ParsedResponse {
        id: u16,
        rcode: u16,
        ips: Vec<Ipv4Addr>,
    }

    /// Parses a response produced by `build_response` (single question,
    /// A records with compression-pointer names).
    fn parse_response(buf: &[u8]) -> ParsedResponse {
        assert!(buf.len() >= 12);
        let id = u16::from_be_bytes([buf[0], buf[1]]);
        let flags = u16::from_be_bytes([buf[2], buf[3]]);
        assert!(flags & 0x8000 != 0, "QR bit must be set");
        let ancount = u16::from_be_bytes([buf[6], buf[7]]);
        let mut pos = 12usize;
        while buf[pos] != 0 {
            pos += 1 + buf[pos] as usize;
        }
        pos += 5; // root label + QTYPE + QCLASS
        let mut ips = Vec::new();
        for _ in 0..ancount {
            assert_eq!(buf[pos] & 0xC0, 0xC0, "answer name must be a pointer");
            let rdlen = u16::from_be_bytes([buf[pos + 10], buf[pos + 11]]) as usize;
            assert_eq!(rdlen, 4);
            ips.push(Ipv4Addr::new(
                buf[pos + 12],
                buf[pos + 13],
                buf[pos + 14],
                buf[pos + 15],
            ));
            pos += 12 + rdlen;
        }
        ParsedResponse {
            id,
            rcode: flags & 0x000F,
            ips,
        }
    }

    /// Sends a query datagram to `addr` over UDP and returns the response.
    async fn query_udp(addr: SocketAddr, query: &[u8]) -> Vec<u8> {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        sock.send_to(query, addr).await.unwrap();
        let mut buf = vec![0u8; MAX_DATAGRAM];
        let (n, _) = tokio::time::timeout(Duration::from_secs(5), sock.recv_from(&mut buf))
            .await
            .expect("timed out waiting for DNS response")
            .unwrap();
        buf.truncate(n);
        buf
    }

    #[test]
    fn encode_decode_round_trip() {
        let query = encode_query(0x1234, "Web.Plant-7.minga.local", QTYPE_A);
        let q = Question::parse(&query).unwrap();
        assert_eq!(q.id, 0x1234);
        assert_eq!(q.name, "web.plant-7.minga.local"); // lowercased
        assert_eq!(q.qtype, QTYPE_A);
        assert_eq!(q.qclass, QCLASS_IN);
        assert_eq!(q.end, query.len());

        let ips = vec![Ipv4Addr::new(10, 100, 0, 5), Ipv4Addr::new(10, 100, 0, 6)];
        let resp = build_response(&query, &q, &ips, RCODE_NOERROR);
        let parsed = parse_response(&resp);
        assert_eq!(parsed.id, 0x1234);
        assert_eq!(parsed.rcode, RCODE_NOERROR);
        assert_eq!(parsed.ips, ips);

        // Truncated / malformed datagrams are rejected.
        assert!(Question::parse(&query[..10]).is_err());
        assert!(Question::parse(&query[..query.len() - 2]).is_err());
    }

    #[tokio::test]
    async fn a_query_known_name_returns_ips() {
        let mut store = DnsStore::new();
        let ips = vec![Ipv4Addr::new(10, 100, 1, 10), Ipv4Addr::new(10, 100, 1, 11)];
        store.set("web.plant-7.minga.local", ips.clone());
        let server = DnsServer::start(
            vec!["127.0.0.1:0".parse().unwrap()],
            Arc::new(RwLock::new(store)),
            None,
        )
        .await
        .unwrap();
        let addr = server.local_addrs()[0];

        let resp = query_udp(
            addr,
            &encode_query(0x42, "web.plant-7.minga.local", QTYPE_A),
        )
        .await;
        let parsed = parse_response(&resp);
        assert_eq!(parsed.id, 0x42);
        assert_eq!(parsed.rcode, RCODE_NOERROR);
        assert_eq!(parsed.ips, ips);

        server.shutdown().await;
    }

    #[tokio::test]
    async fn unknown_minga_name_returns_noerror_empty() {
        let server = DnsServer::start(
            vec!["127.0.0.1:0".parse().unwrap()],
            Arc::new(RwLock::new(DnsStore::new())),
            None,
        )
        .await
        .unwrap();
        let addr = server.local_addrs()[0];

        let resp = query_udp(
            addr,
            &encode_query(0x43, "nope.plant-7.minga.local", QTYPE_A),
        )
        .await;
        let parsed = parse_response(&resp);
        assert_eq!(parsed.id, 0x43);
        assert_eq!(parsed.rcode, RCODE_NOERROR);
        assert!(parsed.ips.is_empty());

        server.shutdown().await;
    }

    #[tokio::test]
    async fn non_a_minga_query_returns_noerror_empty() {
        let mut store = DnsStore::new();
        store.set(
            "web.plant-7.minga.local",
            vec![Ipv4Addr::new(10, 100, 1, 10)],
        );
        let server = DnsServer::start(
            vec!["127.0.0.1:0".parse().unwrap()],
            Arc::new(RwLock::new(store)),
            None,
        )
        .await
        .unwrap();
        let addr = server.local_addrs()[0];

        const QTYPE_AAAA: u16 = 28;
        let resp = query_udp(
            addr,
            &encode_query(0x44, "web.plant-7.minga.local", QTYPE_AAAA),
        )
        .await;
        let parsed = parse_response(&resp);
        assert_eq!(parsed.rcode, RCODE_NOERROR);
        assert!(parsed.ips.is_empty());

        server.shutdown().await;
    }

    #[tokio::test]
    async fn non_minga_without_upstream_refused() {
        let server = DnsServer::start(
            vec!["127.0.0.1:0".parse().unwrap()],
            Arc::new(RwLock::new(DnsStore::new())),
            None,
        )
        .await
        .unwrap();
        let addr = server.local_addrs()[0];

        let resp = query_udp(addr, &encode_query(0x45, "example.com", QTYPE_A)).await;
        let parsed = parse_response(&resp);
        assert_eq!(parsed.id, 0x45);
        assert_eq!(parsed.rcode, RCODE_REFUSED);
        assert!(parsed.ips.is_empty());

        server.shutdown().await;
    }

    #[tokio::test]
    async fn upstream_proxy_relays_response() {
        // Fake upstream: echoes the query back with QR set and one canned
        // A record appended.
        let upstream_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_sock.local_addr().unwrap();
        tokio::spawn(async move {
            let mut buf = vec![0u8; MAX_DATAGRAM];
            let (n, peer) = upstream_sock.recv_from(&mut buf).await.unwrap();
            let mut resp = buf[..n].to_vec();
            resp[2] |= 0x80; // QR
            resp[6..8].copy_from_slice(&1u16.to_be_bytes()); // ANCOUNT = 1
            resp.extend_from_slice(&0xC00Cu16.to_be_bytes());
            resp.extend_from_slice(&QTYPE_A.to_be_bytes());
            resp.extend_from_slice(&QCLASS_IN.to_be_bytes());
            resp.extend_from_slice(&300u32.to_be_bytes());
            resp.extend_from_slice(&4u16.to_be_bytes());
            resp.extend_from_slice(&[93, 184, 216, 34]);
            upstream_sock.send_to(&resp, peer).await.unwrap();
        });

        let server = DnsServer::start(
            vec!["127.0.0.1:0".parse().unwrap()],
            Arc::new(RwLock::new(DnsStore::new())),
            Some(upstream_addr),
        )
        .await
        .unwrap();
        let addr = server.local_addrs()[0];

        let resp = query_udp(addr, &encode_query(0x77, "example.com", QTYPE_A)).await;
        let parsed = parse_response(&resp);
        assert_eq!(parsed.id, 0x77, "query ID must be preserved");
        assert_eq!(parsed.rcode, RCODE_NOERROR);
        assert_eq!(parsed.ips, vec![Ipv4Addr::new(93, 184, 216, 34)]);

        server.shutdown().await;
    }
}
