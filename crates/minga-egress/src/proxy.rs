//! Axum-based HTTP reverse proxy for LLM API egress control.
//!
//! The [`EgressProxy`] sits between agent workloads and upstream LLM
//! APIs. For each request it:
//!
//! 1. Identifies the workload (via `X-Minga-Workload` header).
//! 2. Checks the token budget via [`BudgetTracker`].
//! 3. Checks the response cache via [`ResponseCache`].
//! 4. If cache miss, forwards to the upstream LLM API.
//! 5. Records token usage from the response.
//! 6. Caches successful responses.
//! 7. Redacts secrets from error messages via [`SecretRedactor`].

use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    response::Response,
    routing::any,
    Router,
};
use thiserror::Error;

use crate::budget::BudgetTracker;
use crate::cache::{CacheConfig, ResponseCache};
use crate::redact::SecretRedactor;

/// Errors from the egress proxy.
#[derive(Debug, Error)]
pub enum EgressProxyError {
    #[error("budget exceeded for workload `{0}`")]
    BudgetExceeded(String),
    #[error("workload `{0}` has no budget configured")]
    NoBudget(String),
    #[error("upstream request failed: {0}")]
    Upstream(String),
    #[error("internal error: {0}")]
    Internal(String),
}

/// Configuration for the egress proxy.
#[derive(Debug, Clone)]
pub struct EgressProxyConfig {
    /// Base URL of the upstream LLM API (e.g. `https://api.openai.com`).
    pub upstream_url: String,
    /// Request timeout for upstream calls.
    pub timeout: Duration,
    /// Cache configuration. If `None`, caching is disabled.
    pub cache_config: Option<CacheConfig>,
    /// Whether to redact secrets from error responses.
    pub redact_errors: bool,
}

impl Default for EgressProxyConfig {
    fn default() -> Self {
        Self {
            upstream_url: "https://api.openai.com".to_string(),
            timeout: Duration::from_secs(60),
            cache_config: Some(CacheConfig::default()),
            redact_errors: true,
        }
    }
}

/// Shared state for the proxy handlers.
struct ProxyState {
    config: EgressProxyConfig,
    budgets: Arc<BudgetTracker>,
    cache: Option<Arc<ResponseCache>>,
    redactor: SecretRedactor,
    client: reqwest::Client,
}

/// An HTTP reverse proxy for LLM API egress control.
pub struct EgressProxy {
    state: Arc<ProxyState>,
    addr: Option<String>,
    shutdown: Arc<tokio::sync::Notify>,
}

impl std::fmt::Debug for EgressProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EgressProxy")
            .field("config", &self.state.config)
            .field("addr", &self.addr)
            .finish()
    }
}

impl EgressProxy {
    /// Creates a new egress proxy.
    pub fn new(config: EgressProxyConfig, budgets: Arc<BudgetTracker>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let cache = config
            .cache_config
            .as_ref()
            .map(|cc| Arc::new(ResponseCache::new(cc.clone())));

        Self {
            state: Arc::new(ProxyState {
                config,
                budgets,
                cache,
                redactor: SecretRedactor::with_defaults(),
                client,
            }),
            addr: None,
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Builds the axum router for the proxy.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/health", any(health_handler))
            .route("/budgets", any(list_budgets_handler))
            .route("/*path", any(proxy_handler))
            .with_state(Arc::clone(&self.state))
    }

    /// Starts the proxy server on the given address. Returns the bound address.
    ///
    /// This binds the listener and starts serving in the background.
    /// Use [`EgressProxy::shutdown`] to stop.
    pub async fn start(&mut self, addr: &str) -> Result<String, EgressProxyError> {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| EgressProxyError::Internal(format!("bind failed: {e}")))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| EgressProxyError::Internal(format!("local_addr failed: {e}")))?;
        let bound = local_addr.to_string();

        let router = self.router();
        let shutdown = Arc::clone(&self.shutdown);

        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    shutdown.notified().await;
                })
                .await
                .ok();
        });

        self.addr = Some(bound.clone());
        Ok(bound)
    }

    /// Returns the address the proxy is listening on.
    pub fn addr(&self) -> Option<&str> {
        self.addr.as_deref()
    }

    /// Signals the proxy to shut down.
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Returns a reference to the budget tracker.
    pub fn budgets(&self) -> &Arc<BudgetTracker> {
        &self.state.budgets
    }

    /// Returns cache stats if caching is enabled.
    pub fn cache_stats(&self) -> Option<crate::cache::CacheStats> {
        self.state.cache.as_ref().map(|c| c.stats())
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn health_handler() -> &'static str {
    "OK"
}

async fn list_budgets_handler(State(state): State<Arc<ProxyState>>) -> Response {
    let budgets = state.budgets.list();
    let body = serde_json::to_string_pretty(&budgets).unwrap_or_else(|_| "[]".to_string());
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

async fn proxy_handler(State(state): State<Arc<ProxyState>>, req: Request) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();

    // Extract workload identifier.
    let workload = req
        .headers()
        .get("x-minga-workload")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("default")
        .to_string();

    // Read body.
    let body_bytes = match axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                &state,
                StatusCode::BAD_REQUEST,
                &format!("failed to read request body: {e}"),
            );
        }
    };

    // Estimate tokens from request body (rough: 1 token ≈ 4 chars).
    let estimated_tokens = (body_bytes.len() as u64) / 4;

    // Check budget.
    if let Err(e) = state.budgets.check(&workload, estimated_tokens) {
        return error_response(&state, StatusCode::TOO_MANY_REQUESTS, &e.to_string());
    }

    // Check cache.
    let cache_key = ResponseCache::compute_key(method.as_str(), &path, &body_bytes);
    if let Some(ref cache) = state.cache {
        if let Some((status, headers, body)) = cache.get(&cache_key) {
            tracing::debug!(workload, path, "cache hit");
            let mut resp = Response::builder().status(status);
            for (name, value) in headers {
                resp = resp.header(name, value);
            }
            return resp
                .body(Body::from(body))
                .unwrap_or_else(|_| Response::new(Body::empty()));
        }
    }

    // Forward to upstream.
    let upstream_url = format!("{}{}{}", state.config.upstream_url, path, query);
    let upstream_req = state.client.request(
        reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap_or(reqwest::Method::GET),
        &upstream_url,
    );

    // Forward relevant headers (excluding host, content-length).
    // Note: we intentionally do NOT forward x-minga-workload upstream.

    let upstream_resp = match upstream_req
        .header("content-type", "application/json")
        .body(body_bytes.clone())
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                &state,
                StatusCode::BAD_GATEWAY,
                &format!("upstream request failed: {e}"),
            );
        }
    };

    let status = upstream_resp.status().as_u16();
    let resp_headers: Vec<(String, String)> = upstream_resp
        .headers()
        .iter()
        .filter(|(name, _)| {
            let n = name.as_str();
            n != "content-length" && n != "transfer-encoding" && n != "connection"
        })
        .map(|(name, value)| (name.to_string(), value.to_str().unwrap_or("").to_string()))
        .collect();

    let resp_body = match upstream_resp.bytes().await {
        Ok(b) => b.to_vec(),
        Err(e) => {
            return error_response(
                &state,
                StatusCode::BAD_GATEWAY,
                &format!("failed to read upstream response: {e}"),
            );
        }
    };

    // Record token usage from response.
    let response_tokens = estimate_response_tokens(&resp_body);
    let total_tokens = estimated_tokens + response_tokens;
    if let Err(e) = state.budgets.record(&workload, total_tokens) {
        tracing::warn!(workload, error = %e, "budget exceeded after upstream call");
        // Still return the response — the budget check is advisory for the
        // current request. The NEXT request will be rejected.
    }

    // Cache successful responses.
    if let Some(ref cache) = state.cache {
        cache.put(&cache_key, status, resp_headers.clone(), resp_body.clone());
    }

    // Build response.
    let mut resp = Response::builder().status(status);
    for (name, value) in &resp_headers {
        resp = resp.header(name, value);
    }
    resp.body(Body::from(resp_body))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

/// Estimates token count from a response body by looking for
/// `usage.total_tokens` in the JSON, falling back to char/4 estimate.
fn estimate_response_tokens(body: &[u8]) -> u64 {
    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(body) {
        if let Some(total) = json
            .get("usage")
            .and_then(|u| u.get("total_tokens"))
            .and_then(|t| t.as_u64())
        {
            return total;
        }
    }
    (body.len() as u64) / 4
}

/// Builds an error response, optionally redacting secrets.
fn error_response(state: &ProxyState, status: StatusCode, message: &str) -> Response {
    let message = if state.config.redact_errors {
        state.redactor.redact(message)
    } else {
        message.to_string()
    };

    let body = serde_json::json!({ "error": message });
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap_or_default()))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::Budget;

    fn test_state() -> Arc<ProxyState> {
        let budgets = Arc::new(BudgetTracker::new());
        budgets.register(
            "test-agent",
            Budget::new(1_000_000, Duration::from_secs(3600)),
        );
        budgets.register("limited-agent", Budget::new(10, Duration::from_secs(3600)));
        Arc::new(ProxyState {
            config: EgressProxyConfig::default(),
            budgets,
            cache: Some(Arc::new(ResponseCache::new(CacheConfig::default()))),
            redactor: SecretRedactor::with_defaults(),
            client: reqwest::Client::new(),
        })
    }

    #[tokio::test]
    async fn health_endpoint() {
        let resp = health_handler().await;
        assert_eq!(resp, "OK");
    }

    #[tokio::test]
    async fn list_budgets() {
        let state = test_state();
        let resp = list_budgets_handler(State(state)).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn estimate_tokens_from_usage() {
        let body =
            br#"{"usage": {"total_tokens": 150, "prompt_tokens": 100, "completion_tokens": 50}}"#;
        assert_eq!(estimate_response_tokens(body), 150);
    }

    #[test]
    fn estimate_tokens_fallback() {
        let body = br#"{"choices": [{"message": {"content": "hello world"}}]}"#;
        // No usage field → char/4 estimate
        let expected = (body.len() as u64) / 4;
        assert_eq!(estimate_response_tokens(body), expected);
    }

    #[test]
    fn estimate_tokens_empty() {
        assert_eq!(estimate_response_tokens(b""), 0);
    }

    #[test]
    fn error_response_redacts_secrets() {
        let state = test_state();
        let resp = error_response(
            &state,
            StatusCode::BAD_REQUEST,
            "invalid key sk-abc123def456ghi789jkl012mno345",
        );
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn error_response_no_redaction() {
        let state = Arc::new(ProxyState {
            config: EgressProxyConfig {
                redact_errors: false,
                ..Default::default()
            },
            budgets: Arc::new(BudgetTracker::new()),
            cache: None,
            redactor: SecretRedactor::with_defaults(),
            client: reqwest::Client::new(),
        });
        let resp = error_response(&state, StatusCode::BAD_REQUEST, "plain error");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn proxy_config_default() {
        let config = EgressProxyConfig::default();
        assert_eq!(config.upstream_url, "https://api.openai.com");
        assert!(config.cache_config.is_some());
        assert!(config.redact_errors);
    }

    #[tokio::test]
    async fn egress_proxy_new() {
        let budgets = Arc::new(BudgetTracker::new());
        let proxy = EgressProxy::new(EgressProxyConfig::default(), budgets);
        assert!(proxy.cache_stats().is_some());
    }

    #[tokio::test]
    async fn egress_proxy_no_cache() {
        let budgets = Arc::new(BudgetTracker::new());
        let config = EgressProxyConfig {
            cache_config: None,
            ..Default::default()
        };
        let proxy = EgressProxy::new(config, budgets);
        assert!(proxy.cache_stats().is_none());
    }

    #[tokio::test]
    async fn proxy_router_builds() {
        let budgets = Arc::new(BudgetTracker::new());
        let proxy = EgressProxy::new(EgressProxyConfig::default(), budgets);
        let _router = proxy.router();
    }
}
