use std::sync::LazyLock;

pub mod health;
pub mod logging;
pub mod metrics;
pub mod serve;

pub use health::{HealthState, Healthz, ReadyState, Readyz};
pub use logging::{init_logging, span_with_correlation_id, CorrelationIdLayer};
pub use metrics::Metrics;

/// Global metrics registry — instantiated once at process start.
pub static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::new);

/// Observability routes with the full registry; callers must enforce operator authentication.
pub fn router(ready: ReadyState) -> axum::Router {
    observability_router(ready, true)
}

fn observability_router(ready: ReadyState, include_tenant_metrics: bool) -> axum::Router {
    use axum::{http::StatusCode, routing::get};

    let readyz = Readyz::new(ready);
    axum::Router::new()
        .route(
            "/metrics",
            get(move || async move {
                let metrics = if include_tenant_metrics {
                    METRICS.gather()
                } else {
                    METRICS.gather_without_tenants()
                };
                metrics.unwrap_or_else(|e| format!("# error gathering metrics: {e}"))
            }),
        )
        .route("/healthz", get(|| async { "OK" }))
        .route(
            "/readyz",
            get(move || {
                let result = readyz.check();
                async move {
                    match result {
                        Ok(()) => (StatusCode::OK, "OK".to_string()),
                        Err(e) => (StatusCode::SERVICE_UNAVAILABLE, e.to_string()),
                    }
                }
            }),
        )
}

/// Serves node metrics and health probes without authentication on `bind`.
/// Tenant-labelled metric families are excluded; returns the bound address.
pub async fn serve(bind: &str, ready: ReadyState) -> std::io::Result<std::net::SocketAddr> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(serve::serve_bounded(
        listener,
        observability_router(ready, false),
        serve::Limits::default(),
        std::future::pending(),
    ));
    Ok(addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn metrics_listener_excludes_tenant_families() {
        METRICS.record_agent_usage("listener-private-tenant", 3, 5, 7);
        METRICS
            .agent_usage_events_dropped_total
            .with_label_values(&["listener-private-tenant"])
            .inc();
        let addr = serve("127.0.0.1:0", ReadyState::new()).await.unwrap();
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            stream.read_to_string(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(
            response.contains("minga_agent_pool_total"),
            "node metrics remain available"
        );
        assert!(
            !response.contains("tenant="),
            "tenant labels leaked: {response}"
        );
        assert!(!response.contains("listener-private-tenant"));
        assert!(
            !response.contains("minga_agent_usage_"),
            "drop the entire family, including metadata"
        );
    }
}
