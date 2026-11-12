//! Serving an axum router with the limits every Minga HTTP listener needs.
//!
//! `axum::serve` (0.7) builds hyper without a timer, which silently turns off
//! hyper's header-read timeout, and it accepts without a connection cap. Any
//! peer that can reach a listener could then hold sockets, tasks and up to
//! ~400 KiB of head buffer each, before any token check runs. The API, the
//! telemetry endpoint and the egress proxy all serve through
//! [`serve_bounded`] instead.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;

/// Per-listener limits for [`serve_bounded`].
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// A client must send a complete request head within this time.
    pub header_read_timeout: Duration,
    /// Connections served at once; further ones wait in the kernel backlog.
    pub max_connections: usize,
    /// Read buffer cap per connection, which bounds a request head.
    pub max_buf_size: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            header_read_timeout: Duration::from_secs(10),
            max_connections: 1024,
            max_buf_size: 64 * 1024,
        }
    }
}

/// Serves `router` on `listener` until `shutdown` completes. Connections in
/// flight when it does are left to finish on their own.
pub async fn serve_bounded<F>(
    listener: TcpListener,
    router: axum::Router,
    limits: Limits,
    shutdown: F,
) where
    F: Future<Output = ()> + Send + 'static,
{
    let slots = Arc::new(tokio::sync::Semaphore::new(limits.max_connections));
    tokio::pin!(shutdown);
    loop {
        let permit = tokio::select! {
            () = &mut shutdown => return,
            permit = slots.clone().acquire_owned() => permit.expect("the semaphore is never closed"),
        };
        let stream = tokio::select! {
            () = &mut shutdown => return,
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(e) => {
                    // EMFILE and friends: back off instead of spinning.
                    tracing::warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        let service = TowerToHyperService::new(router.clone());
        tokio::spawn(async move {
            let _permit = permit;
            // The auto builder reads the first bytes to pick HTTP/1 or HTTP/2
            // with no deadline of its own, before hyper's header timer starts:
            // a peer that sends nothing must not hold the slot.
            if tokio::time::timeout(limits.header_read_timeout, stream.readable())
                .await
                .is_err()
            {
                return;
            }
            let mut builder = auto::Builder::new(TokioExecutor::new());
            builder
                .http1()
                .timer(TokioTimer::new())
                .header_read_timeout(limits.header_read_timeout)
                .max_buf_size(limits.max_buf_size);
            builder.http2().timer(TokioTimer::new());
            if let Err(e) = builder
                .serve_connection_with_upgrades(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!(error = %e, "connection closed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn server(limits: Limits) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = axum::Router::new().route("/", axum::routing::get(|| async { "ok" }));
        tokio::spawn(serve_bounded(
            listener,
            router,
            limits,
            std::future::pending(),
        ));
        addr
    }

    fn short() -> Limits {
        Limits {
            header_read_timeout: Duration::from_millis(300),
            ..Limits::default()
        }
    }

    /// Reads until the server closes the connection, or fails after `within`.
    async fn closed_within(stream: &mut tokio::net::TcpStream, within: Duration) -> bool {
        let mut buf = [0u8; 1024];
        tokio::time::timeout(within, async {
            loop {
                match stream.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
            }
        })
        .await
        .is_ok()
    }

    #[tokio::test]
    async fn a_request_is_served() {
        let addr = server(short()).await;
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut body = String::new();
        s.read_to_string(&mut body).await.unwrap();
        assert!(body.starts_with("HTTP/1.1 200"), "{body}");
        assert!(body.ends_with("ok"), "{body}");
    }

    #[tokio::test]
    async fn a_silent_connection_is_closed() {
        let addr = server(short()).await;
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        assert!(closed_within(&mut s, Duration::from_secs(3)).await);
    }

    #[tokio::test]
    async fn an_idle_keep_alive_connection_is_closed() {
        let addr = server(short()).await;
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 1024];
        let n = s.read(&mut buf).await.unwrap();
        assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200"));
        // No second request: the connection must not stay open forever.
        assert!(closed_within(&mut s, Duration::from_secs(3)).await);
    }

    #[tokio::test]
    async fn a_head_that_never_ends_is_closed() {
        let addr = server(short()).await;
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET / HTTP/1.1\r\nX-Pad: aaaa").await.unwrap();
        assert!(closed_within(&mut s, Duration::from_secs(3)).await);
    }

    #[tokio::test]
    async fn connections_over_the_cap_wait() {
        let addr = server(Limits {
            max_connections: 1,
            ..Limits::default()
        })
        .await;
        // The first connection holds the only slot (default 10 s timeout).
        let _first = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut second = tokio::net::TcpStream::connect(addr).await.unwrap();
        second
            .write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 16];
        let answered =
            tokio::time::timeout(Duration::from_millis(500), second.read(&mut buf)).await;
        assert!(
            answered.is_err(),
            "a second connection must wait for a free slot"
        );
    }
}
