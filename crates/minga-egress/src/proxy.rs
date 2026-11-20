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
        return error_response(&state, StatusCode::UNAUTHORIZED, "missing or unknown sandbox token");
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

    // Keep the existing request cap: only request bodies are buffered.
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
    let estimated_tokens = (body_bytes.len() as u64) / 4;
    let request_stream = serde_json::from_slice::<serde_json::Value>(&body_bytes)
        .ok()
        .and_then(|value| value.get("stream").and_then(serde_json::Value::as_bool))
        .unwrap_or(false);
    if let Err(e) = state.budgets.check(&workload, estimated_tokens) {
        return error_response(&state, StatusCode::TOO_MANY_REQUESTS, &e.to_string());
    }

    let cache_key =
        ResponseCache::compute_key_for_workload(&workload, method.as_str(), &path, &body_bytes);
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
        .body(body_bytes)
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

    let status = upstream_resp.status();
    let resp_headers: Vec<(String, String)> = upstream_resp
        .headers()
        .iter()
        .filter(|(name, _)| {
            !matches!(name.as_str(), "content-length" | "transfer-encoding" | "connection")
        })
        .map(|(name, value)| (name.to_string(), value.to_str().unwrap_or("").to_string()))
        .collect();
    let is_sse = upstream_resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("text/event-stream"));
    if !is_sse && !request_stream {
        let body = match upstream_resp.bytes().await {
            Ok(body) => body,
            Err(error) => {
                return error_response(
                    &state,
                    StatusCode::BAD_GATEWAY,
                    &format!("failed to read upstream response: {error}"),
                );
            }
        };
        let body = if status.is_client_error() || status.is_server_error() {
            if state.config.redact_errors {
                state
                    .redactor
                    .redact(&String::from_utf8_lossy(&body))
                    .into_bytes()
            } else {
                body.to_vec()
            }
        } else {
            body.to_vec()
        };
        let counts = find_usage_fragment(&body).unwrap_or(UsageCounts {
            input_tokens: 0,
            output_tokens: (body.len() as u64) / 4,
            estimated: true,
        });
        if let Err(error) = state.budgets.record(
            &workload,
            estimated_tokens + counts.input_tokens + counts.output_tokens,
        ) {
            tracing::warn!(workload, error = %error, "budget exceeded after upstream call");
        }
        if let Some(cache) = &state.cache {
            cache.put(&cache_key, status.as_u16(), resp_headers.clone(), body.clone());
        }
        let mut response = Response::builder().status(status);
        for (name, value) in &resp_headers {
            response = response.header(name, value);
        }
        return response
            .body(Body::from(body))
            .unwrap_or_else(|_| Response::new(Body::empty()));
    }
    let stream = upstream_resp.bytes_stream();
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let budgets = Arc::clone(&state.budgets);
    let redactor = state.redactor.clone();
    let redact =
        state.config.redact_errors && (status.is_client_error() || status.is_server_error());
    tokio::spawn(async move {
        use futures_util::StreamExt;
        let mut stream = stream;
        let mut parser = UsageParser::new(is_sse);
        let mut response_bytes = 0_u64;
        let mut redaction_tail = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => {
                    response_bytes = response_bytes.saturating_add(chunk.len() as u64);
                    parser.push(&chunk);
                    if redact {
                        redaction_tail.extend_from_slice(&chunk);
                        if redaction_tail.len() > 8192 {
                            let split = redaction_tail.len() - 8192;
                            let safe = String::from_utf8_lossy(&redaction_tail[..split]);
                            let redacted = redactor.redact(&safe);
                            if sender.send(Ok(bytes::Bytes::from(redacted))).await.is_err() {
                                return;
                            }
                            redaction_tail.drain(..split);
                        }
                    } else if sender.send(Ok(chunk)).await.is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = sender
                        .send(Err(std::io::Error::other(error.to_string())))
                        .await;
                    return;
                }
            }
        }
        if redact && !redaction_tail.is_empty() {
            let safe = String::from_utf8_lossy(&redaction_tail);
            let redacted = redactor.redact(&safe);
            if sender.send(Ok(bytes::Bytes::from(redacted))).await.is_err() {
                return;
            }
        }
        let counts = parser.finish(response_bytes);
        if let Err(error) = budgets.record(&workload, estimated_tokens + counts.input_tokens + counts.output_tokens) {
            tracing::warn!(workload, error = %error, "budget exceeded after upstream call");
        }
    });

    let mut resp = Response::builder().status(status);
    for (name, value) in &resp_headers {
        resp = resp.header(name, value);
    }
    // A streamed response is deliberately never inserted in the response cache.
    resp.body(Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(receiver)))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

/// Provider-reported request and response token counts for one completion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageCounts {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub estimated: bool,
}

struct UsageParser {
    is_sse: bool,
    line: Vec<u8>,
    event: Vec<u8>,
    json_scan: Vec<u8>,
    usage: Option<UsageCounts>,
}

impl UsageParser {
    fn new(is_sse: bool) -> Self {
        Self {
            is_sse,
            line: Vec::new(),
            event: Vec::new(),
            json_scan: Vec::new(),
            usage: None,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        if self.is_sse {
            for &byte in chunk {
                if byte == b'\n' {
                    self.process_sse_line();
                } else {
                    self.line.push(byte);
                }
            }
        } else {
            self.json_scan.extend_from_slice(chunk);
            if let Some(usage) = find_usage_fragment(&self.json_scan) {
                self.record_usage(usage);
                self.json_scan.clear();
            } else if let Some(marker) = self
                .json_scan
                .windows(b"\"usage\"".len())
                .position(|window| window == b"\"usage\"")
            {
                if marker > 0 {
                    self.json_scan.drain(..marker);
                }
            } else if self.json_scan.len() > 16 {
                let keep_from = self.json_scan.len() - 16;
                self.json_scan.drain(..keep_from);
            }
        }

    }

    fn record_usage(&mut self, next: UsageCounts) {
        let current = self.usage.get_or_insert(UsageCounts {
            input_tokens: 0,
            output_tokens: 0,
            estimated: false,
        });
        if next.input_tokens > 0 {
            current.input_tokens = next.input_tokens;
        }
        if next.output_tokens > 0 {
            current.output_tokens = next.output_tokens;
        }
        current.estimated = false;
    }

    fn process_sse_line(&mut self) {
        if self.line.last() == Some(&b'\r') {
            self.line.pop();
        }
        if self.line.is_empty() {
            if let Some(usage) = find_usage(&self.event) {
                self.record_usage(usage);
            }
            self.event.clear();
        } else if let Some(data) = self.line.strip_prefix(b"data:") {
            self.event.extend_from_slice(data.strip_prefix(b" ").unwrap_or(data));
            self.event.push(b'\n');
        }
        self.line.clear();
    }

    fn finish(mut self, response_bytes: u64) -> UsageCounts {
        if self.is_sse && !self.line.is_empty() {
            self.process_sse_line();
        }
        self.usage.unwrap_or(UsageCounts {
            input_tokens: 0,
            output_tokens: response_bytes / 4,
            estimated: true,
        })
    }
}

fn find_usage(bytes: &[u8]) -> Option<UsageCounts> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let usage = value
        .get("usage")
        .or_else(|| value.get("message").and_then(|message| message.get("usage")))?;
    usage_counts(Some(usage))
}

fn find_usage_fragment(bytes: &[u8]) -> Option<UsageCounts> {
    let marker = bytes.windows(b"\"usage\"".len()).position(|w| w == b"\"usage\"")?;
    let open = bytes[marker + b"\"usage\"".len()..]
        .iter()
        .position(|byte| *byte == b'{')?
        + marker
        + b"\"usage\"".len();
    let mut depth = 0_u32;
    let mut in_string = false;
    let mut escaped = false;
    for (offset, &byte) in bytes[open..].iter().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    let usage: serde_json::Value =
                        serde_json::from_slice(&bytes[open..=open + offset]).ok()?;
                    return usage_counts(Some(&usage));
                }
            }
            _ => {}
        }
    }
    None
}

fn usage_counts(usage: Option<&serde_json::Value>) -> Option<UsageCounts> {
    let usage = usage?;
    let input = usage
        .get("prompt_tokens")
        .or_else(|| usage.get("input_tokens"))
        .and_then(serde_json::Value::as_u64);
    let output = usage
        .get("completion_tokens")
        .or_else(|| usage.get("output_tokens"))
        .and_then(serde_json::Value::as_u64);
    if input.is_none() && output.is_none() {
        return None;
    }
    Some(UsageCounts {
        input_tokens: input.unwrap_or(0),
        output_tokens: output.unwrap_or(0),
        estimated: false,
    })
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
    fn parses_openai_and_anthropic_json_usage() {
        let openai = br#"{"usage":{"prompt_tokens":12,"completion_tokens":7,"total_tokens":19}}"#;
        assert_eq!(
            find_usage_fragment(openai),
            Some(UsageCounts {
                input_tokens: 12,
                output_tokens: 7,
                estimated: false,
            })
        );
        let anthropic = br#"{"usage":{"input_tokens":21,"output_tokens":9}}"#;
        assert_eq!(
            find_usage(anthropic),
            Some(UsageCounts {
                input_tokens: 21,
                output_tokens: 9,
                estimated: false,
            })
        );
    }

    #[test]
    fn parses_sse_usage_across_arbitrary_chunks() {
        let event = b"data: {\"type\":\"message_delta\",\"usage\":{\"input_tokens\":31,\"output_tokens\":4}}\r\n\r\n";
        let mut parser = UsageParser::new(true);
        for part in event.chunks(3) {
            parser.push(part);
        }
        assert_eq!(
            parser.finish(event.len() as u64),
            UsageCounts {
                input_tokens: 31,
                output_tokens: 4,
                estimated: false,
            }
        );
    }

    #[test]
    fn parses_openai_sse_usage_across_arbitrary_chunks() {
        let openai = b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}\n\n";
        let mut parser = UsageParser::new(true);
        for part in openai.chunks(2) {
            parser.push(part);
        }
        assert_eq!(
            parser.finish(openai.len() as u64),
            UsageCounts {
                input_tokens: 12,
                output_tokens: 3,
                estimated: false,
            }
        );
    }

    #[test]
    fn combines_anthropic_sse_usage_from_message_start_and_delta() {
        let events = concat!(
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":18,\"output_tokens\":0}}}\n\n",
            "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":6}}\n\n",
        );
        let mut parser = UsageParser::new(true);
        for part in events.as_bytes().chunks(7) {
            parser.push(part);
        }
        assert_eq!(
            parser.finish(events.len() as u64),
            UsageCounts {
                input_tokens: 18,
                output_tokens: 6,
                estimated: false,
            }
        );
    }

    #[test]
    fn missing_usage_falls_back_to_response_bytes_over_four() {
        let body = b"response without provider usage";
        let mut parser = UsageParser::new(false);
        for part in body.chunks(5) {
            parser.push(part);
        }
        assert_eq!(
            parser.finish(body.len() as u64),
            UsageCounts {
                input_tokens: 0,
                output_tokens: (body.len() / 4) as u64,
                estimated: true,
            }
        );
    }
    #[tokio::test]
    async fn streamed_upstream_chunks_are_forwarded_before_completion_and_not_cached() {
        use axum::body::Body as AxumBody;
        use bytes::Bytes;
        use futures_util::StreamExt;

        let app = Router::new().route(
            "/*path",
            any(|| async {
                let (sender, receiver) =
                    tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(2);
                tokio::spawn(async move {
                    let _ = sender.send(Ok(Bytes::from_static(b"first\n"))).await;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    let _ = sender.send(Ok(Bytes::from_static(b"second\n"))).await;
                });
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(AxumBody::from_stream(tokio_stream::wrappers::ReceiverStream::new(receiver)))
                    .unwrap()
            }),
        );
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(upstream_listener, app).await.ok() });

        let budgets = Arc::new(BudgetTracker::new());
        budgets.register("stream-task", Budget::new(1_000_000, Duration::from_secs(3600)));
        let tokens = Arc::new(SandboxTokens::new());
        let token = tokens.issue("stream-task");
        let proxy = EgressProxy::new(
            EgressProxyConfig {
                upstream_url: format!("http://{upstream_addr}"),
                ..Default::default()
            },
            budgets,
            tokens,
        );
        let mut proxy = proxy;
        let address = proxy.start("127.0.0.1:0").await.unwrap();
        let response = reqwest::Client::new()
            .post(format!("http://{address}/v1/chat"))
            .header(SANDBOX_TOKEN_HEADER, token.as_str())
            .body("{}")
            .send()
            .await
            .unwrap();
        let mut body = response.bytes_stream();
        let first = tokio::time::timeout(Duration::from_millis(500), body.next())
            .await
            .expect("first chunk should not wait for upstream completion")
            .unwrap()
            .unwrap();
        assert_eq!(first.as_ref(), b"first\n");
        assert_eq!(proxy.cache_stats().unwrap().stores, 0);
        proxy.shutdown();
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
