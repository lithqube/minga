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

/// Observability routes: `/metrics`, `/healthz`, `/readyz`.
pub fn router(ready: ReadyState) -> axum::Router {
    use axum::{http::StatusCode, routing::get};

    let readyz = Readyz::new(ready);
    axum::Router::new()
        .route(
            "/metrics",
            get(|| async {
                METRICS
                    .gather()
                    .unwrap_or_else(|e| format!("# error gathering metrics: {e}"))
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

/// Serves [`router`] on `bind` in a background task; returns the bound
/// address.
pub async fn serve(bind: &str, ready: ReadyState) -> std::io::Result<std::net::SocketAddr> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(serve::serve_bounded(
        listener,
        router(ready),
        serve::Limits::default(),
        std::future::pending(),
    ));
    Ok(addr)
}
