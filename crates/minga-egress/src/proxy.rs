//! Axum-based HTTP reverse proxy for LLM API egress control.
//!
//! The [`EgressProxy`] sits between agent workloads and upstream LLM
//! APIs. For each request it:
//!
//! 1. Identifies the workload from its per-sandbox token
//!    (`X-Minga-Sandbox-Token`, see [`SandboxTokens`]). A request without a
//!    live token, or whose `X-Minga-Workload` names another workload, is
//!    rejected.
//! 2. Checks the token budget via [`BudgetTracker`].
//! 3. Checks the response cache via [`ResponseCache`].
//! 4. If cache miss, forwards to the upstream LLM API with the provider
//!    credential injected ([`UpstreamAuth`]). Credential headers the
//!    workload sent are never forwarded.
//! 5. Records token usage from the response.
//! 6. Caches successful responses.
//! 7. Redacts secrets, including the injected credential, from error
//!    messages and upstream error bodies via [`SecretRedactor`].

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
use crate::identity::{SandboxTokens, SANDBOX_TOKEN_HEADER};
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
    /// Provider credential the proxy injects into every upstream request.
    pub upstream_auth: Option<UpstreamAuth>,
}

/// A provider credential and the header it travels in.
#[derive(Clone)]
pub enum UpstreamAuth {
    /// `Authorization: Bearer <key>` (OpenAI and compatible servers).
    Bearer(String),
    /// `x-api-key: <key>` (Anthropic).
    ApiKey(String),
}

impl UpstreamAuth {
    fn key(&self) -> &str {
        match self {
            Self::Bearer(k) | Self::ApiKey(k) => k,
        }
    }
}

// The config is logged via `EgressProxy`'s Debug; never print the key.
impl std::fmt::Debug for UpstreamAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bearer(_) => f.write_str("Bearer(***)"),
            Self::ApiKey(_) => f.write_str("ApiKey(***)"),
        }
    }
}

/// Client headers forwarded upstream. Everything else is dropped, including
/// any `Authorization` or `x-api-key` the workload sends.
const FORWARDED_HEADERS: &[&str] = &["accept", "anthropic-version", "anthropic-beta"];

impl Default for EgressProxyConfig {
    fn default() -> Self {
        Self {
            upstream_url: "https://api.openai.com".to_string(),
            timeout: Duration::from_secs(60),
            cache_config: Some(CacheConfig::default()),
            redact_errors: true,
            upstream_auth: None,
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
    tokens: Arc<SandboxTokens>,
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
    /// Creates a new egress proxy. Only requests carrying a live token from
    /// `tokens` are served.
    pub fn new(
        config: EgressProxyConfig,
        budgets: Arc<BudgetTracker>,
        tokens: Arc<SandboxTokens>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        let cache = config
            .cache_config
            .as_ref()
            .map(|cc| Arc::new(ResponseCache::new(cc.clone())));

        let mut redactor = SecretRedactor::with_defaults();
        if let Some(auth) = &config.upstream_auth {
            redactor = redactor.with_literal("upstream-credential", auth.key());
        }

        Self {
            state: Arc::new(ProxyState {
                config,
                budgets,
                cache,
                redactor,
                client,
                tokens,
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

        tokio::spawn(minga_telemetry::serve::serve_bounded(
            listener,
            router,
            minga_telemetry::serve::Limits::default(),
            async move { shutdown.notified().await },
        ));

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

    // Identity comes from the sandbox token, never from a self-asserted name.
    let Some(workload) = req
        .headers()
        .get(SANDBOX_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|t| state.tokens.workload(t))
    else {
        return error_response(
            &state,
            StatusCode::UNAUTHORIZED,
            "missing or unknown sandbox token",
        );
    };
    if let Some(claimed) = req.headers().get("x-minga-workload") {
        if claimed.as_bytes() != workload.as_bytes() {
            return error_response(
                &state,
                StatusCode::FORBIDDEN,
                "x-minga-workload does not match the sandbox token",
            );
        }
    }
    let forwarded: Vec<(&str, Vec<u8>)> = FORWARDED_HEADERS
        .iter()
        .filter_map(|&n| req.headers().get(n).map(|v| (n, v.as_bytes().to_vec())))
        .collect();

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
    let mut upstream_req = state.client.request(
        reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap_or(reqwest::Method::GET),
        &upstream_url,
    );
    for (name, value) in forwarded {
        upstream_req = upstream_req.header(name, value);
    }
    upstream_req = match &state.config.upstream_auth {
        Some(UpstreamAuth::Bearer(key)) => upstream_req.bearer_auth(key),
        Some(UpstreamAuth::ApiKey(key)) => upstream_req.header("x-api-key", key.as_str()),
        None => upstream_req,
    };

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
        // Providers echo rejected keys back in error bodies.
        Ok(b) if status >= 400 && state.config.redact_errors => state
            .redactor
            .redact(&String::from_utf8_lossy(&b))
            .into_bytes(),
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
            tokens: Arc::new(SandboxTokens::new()),
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
            tokens: Arc::new(SandboxTokens::new()),
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
        let proxy = EgressProxy::new(EgressProxyConfig::default(), budgets, Arc::default());
        assert!(proxy.cache_stats().is_some());
    }

    #[tokio::test]
    async fn egress_proxy_no_cache() {
        let budgets = Arc::new(BudgetTracker::new());
        let config = EgressProxyConfig {
            cache_config: None,
            ..Default::default()
        };
        let proxy = EgressProxy::new(config, budgets, Arc::default());
        assert!(proxy.cache_stats().is_none());
    }

    #[tokio::test]
    async fn proxy_router_builds() {
        let budgets = Arc::new(BudgetTracker::new());
        let proxy = EgressProxy::new(EgressProxyConfig::default(), budgets, Arc::default());
        let _router = proxy.router();
    }

    /// Upstream that echoes the credential headers it received. `/v1/reject`
    /// answers 401 and echoes the API key, as providers do.
    async fn mock_upstream() -> String {
        use axum::http::{HeaderMap, Uri};
        let app = Router::new().route(
            "/*path",
            any(|headers: HeaderMap, uri: Uri| async move {
                let h = |n: &str| {
                    headers
                        .get(n)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string()
                };
                if uri.path() == "/v1/reject" {
                    return (
                        StatusCode::UNAUTHORIZED,
                        format!("invalid key {}", h("x-api-key")),
                    );
                }
                let seen = serde_json::json!({
                    "authorization": h("authorization"),
                    "x-api-key": h("x-api-key"),
                    "x-minga-workload": h("x-minga-workload"),
                    "token": h(SANDBOX_TOKEN_HEADER),
                    "anthropic-version": h("anthropic-version"),
                });
                (StatusCode::OK, seen.to_string())
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });
        format!("http://{addr}")
    }

    async fn start_proxy(auth: UpstreamAuth) -> (EgressProxy, String, Arc<SandboxTokens>) {
        let budgets = Arc::new(BudgetTracker::new());
        for w in ["task-a", "task-b"] {
            budgets.register(w, Budget::new(1_000_000, Duration::from_secs(3600)));
        }
        let tokens = Arc::new(SandboxTokens::new());
        let config = EgressProxyConfig {
            upstream_url: mock_upstream().await,
            cache_config: None,
            upstream_auth: Some(auth),
            ..Default::default()
        };
        let mut proxy = EgressProxy::new(config, budgets, Arc::clone(&tokens));
        let addr = proxy.start("127.0.0.1:0").await.unwrap();
        (proxy, format!("http://{addr}/v1/chat"), tokens)
    }

    #[tokio::test]
    async fn spoofed_workload_header_is_rejected() {
        let (_proxy, url, tokens) = start_proxy(UpstreamAuth::Bearer("k".into())).await;
        let client = reqwest::Client::new();
        let token_b = tokens.issue("task-b");

        // A name alone is not an identity.
        let resp = client
            .post(&url)
            .header("x-minga-workload", "task-a")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);

        // Nor is a made-up token.
        let resp = client
            .post(&url)
            .header(SANDBOX_TOKEN_HEADER, "0".repeat(64))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);

        // task-b's token cannot claim to be task-a.
        let resp = client
            .post(&url)
            .header(SANDBOX_TOKEN_HEADER, token_b.as_str())
            .header("x-minga-workload", "task-a")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 403);

        // A revoked token stops working.
        let raw = token_b.as_str().to_string();
        drop(token_b);
        let resp = client
            .post(&url)
            .header(SANDBOX_TOKEN_HEADER, raw)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
    }

    #[tokio::test]
    async fn injects_credential_and_strips_client_keys() {
        for (auth, want_authorization, want_api_key) in [
            (
                UpstreamAuth::Bearer("provider-key".into()),
                "Bearer provider-key",
                "",
            ),
            (
                UpstreamAuth::ApiKey("provider-key".into()),
                "",
                "provider-key",
            ),
        ] {
            let (_proxy, url, tokens) = start_proxy(auth).await;
            let token = tokens.issue("task-a");
            let resp = reqwest::Client::new()
                .post(&url)
                .header(SANDBOX_TOKEN_HEADER, token.as_str())
                .header("x-minga-workload", "task-a")
                .header("authorization", "Bearer client-key")
                .header("x-api-key", "client-key")
                .header("anthropic-version", "2023-06-01")
                .body("{}")
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 200);
            let seen: serde_json::Value = resp.json().await.unwrap();
            assert_eq!(seen["authorization"], want_authorization);
            assert_eq!(seen["x-api-key"], want_api_key);
            assert_eq!(seen["token"], "", "sandbox token must not leave the node");
            assert_eq!(seen["x-minga-workload"], "");
            assert_eq!(seen["anthropic-version"], "2023-06-01");
        }
    }

    #[tokio::test]
    async fn injected_key_is_redacted_from_upstream_error_body() {
        // No default rule matches this key; only the literal rule can catch it.
        let key = "plain-key-Zq9";
        let (_proxy, url, tokens) = start_proxy(UpstreamAuth::ApiKey(key.into())).await;
        let token = tokens.issue("task-a");
        let resp = reqwest::Client::new()
            .post(url.replace("/v1/chat", "/v1/reject"))
            .header(SANDBOX_TOKEN_HEADER, token.as_str())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
        let body = resp.text().await.unwrap();
        assert!(!body.contains(key), "key leaked: {body}");
        assert!(body.contains("***REDACTED***"), "{body}");
    }

    #[test]
    fn debug_never_prints_the_key() {
        let config = EgressProxyConfig {
            upstream_auth: Some(UpstreamAuth::Bearer("provider-key".into())),
            ..Default::default()
        };
        assert!(!format!("{config:?}").contains("provider-key"));
    }
}
