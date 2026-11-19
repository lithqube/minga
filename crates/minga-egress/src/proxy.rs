//! Axum-based HTTP reverse proxy for model API egress control.
//!
//! Each request is bound to the workload identity behind its sandbox token.
//! The proxy enforces the workload's route and model policy, checks its token
//! budget, and keys cached responses by workload and route. It forwards only
//! approved client headers and injects the selected route's auth. Responses
//! are streamed when requested, provider usage is recorded without request or
//! response bodies, and upstream errors are redacted before reaching callers.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
use crate::routes::{ResolvedAuth, ResolvedRoute, WorkloadIdentity};

use minga_bus::{EventBus, UsageEvent};
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
    bus: Option<Arc<dyn EventBus>>,
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
                bus: None,
            }),
            addr: None,
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Configures durable usage event publishing.
    pub fn with_event_bus(mut self, bus: Arc<dyn EventBus>) -> Self {
        if let Some(state) = Arc::get_mut(&mut self.state) {
            state.bus = Some(bus);
        }
        self
    }

    /// Builds the axum router for the proxy.
    pub fn router(&self) -> Router {
        Router::new()
            .route("/health", any(health_handler))
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

async fn proxy_handler(State(state): State<Arc<ProxyState>>, req: Request) -> Response {
    let started = std::time::Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let token = req
        .headers()
        .get(SANDBOX_TOKEN_HEADER)
        .and_then(|v| v.to_str().ok());
    let Some(identity) = token.and_then(|t| state.tokens.identity(t)) else {
        return error_response(
            &state,
            StatusCode::UNAUTHORIZED,
            "missing or unknown sandbox token",
        );
    };
    if req
        .headers()
        .get("x-minga-workload")
        .is_some_and(|claimed| {
            claimed
                .to_str()
                .map_or(true, |claimed| claimed != identity.workload_id.as_str())
        })
    {
        return error_response(
            &state,
            StatusCode::FORBIDDEN,
            "workload identity does not match sandbox token",
        );
    }
    let customer_id = req
        .headers()
        .get("x-minga-customer")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let workload = identity.workload_id.as_str();
    let forwarded: Vec<(&str, Vec<u8>)> = FORWARDED_HEADERS
        .iter()
        .filter_map(|&n| req.headers().get(n).map(|v| (n, v.as_bytes().to_vec())))
        .collect();
    let mut requested_model = req
        .headers()
        .get("x-minga-model")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let policy = identity.policy.as_ref();
    let mut route_chain: Vec<&ResolvedRoute> = Vec::new();
    let route_name = if let Some(policy) = policy {
        let selected = req
            .headers()
            .get("x-minga-route")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .or_else(|| {
                (policy.routes.len() == 1)
                    .then(|| policy.routes.keys().next().cloned())
                    .flatten()
            });
        let Some(selected) = selected else {
            return error_response(
                &state,
                StatusCode::BAD_REQUEST,
                "egress route must be specified",
            );
        };
        match policy.route_chain(&selected) {
            Ok(chain) => route_chain = chain,
            Err(_) => {
                return error_response(&state, StatusCode::BAD_REQUEST, "unknown egress route")
            }
        }
        selected
    } else {
        String::new()
    };
    let body_bytes = match axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return error_response(
                &state,
                StatusCode::BAD_REQUEST,
                "failed to read request body",
            )
        }
    };
    let request_json = serde_json::from_slice::<serde_json::Value>(&body_bytes).ok();
    let body_model = request_json
        .as_ref()
        .and_then(|value| value.get("model"))
        .and_then(serde_json::Value::as_str);
    if policy.is_some_and(|policy| !policy.models.is_empty()) && body_model.is_none() {
        return error_response(
            &state,
            StatusCode::BAD_REQUEST,
            "model field is required by egress policy",
        );
    }
    if let Some(model) = body_model {
        requested_model = model.to_owned();
    }
    if policy.is_some_and(|policy| !policy.allows_model(&requested_model)) {
        return error_response(
            &state,
            StatusCode::FORBIDDEN,
            "model is not allowed by egress policy",
        );
    }
    let estimated_tokens = body_bytes.len() as u64 / 4;
    let request_stream = request_json
        .as_ref()
        .and_then(|value| value.get("stream"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if let Err(e) = state.budgets.check(workload, estimated_tokens) {
        return error_response(&state, StatusCode::TOO_MANY_REQUESTS, &e.to_string());
    }
    let cache_path = format!("{path}{query}\0{requested_model}");
    let cache_key = ResponseCache::compute_key_for_workload_route(
        workload,
        &route_name,
        method.as_str(),
        &cache_path,
        &body_bytes,
    );
    if let Some(cache) = &state.cache {
        if let Some((status, headers, body)) = cache.get(&cache_key) {
            let counts = find_usage_fragment(&body).unwrap_or(UsageCounts {
                input_tokens: estimated_tokens,
                output_tokens: body.len() as u64 / 4,
                estimated: true,
            });
            let total = if counts.estimated {
                estimated_tokens.saturating_add(counts.output_tokens)
            } else {
                counts.input_tokens.saturating_add(counts.output_tokens)
            };
            if let Err(error) = state.budgets.record(workload, total) {
                tracing::warn!(workload, error = %error, "budget exceeded after cache hit");
            }
            let answered_model =
                find_answered_model(&body).unwrap_or_else(|| requested_model.clone());
            publish_usage(
                &state,
                Arc::clone(&identity),
                UsagePublication {
                    customer_id,
                    route: route_name,
                    requested_model,
                    answered_model,
                    counts,
                    status: StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
                    latency_ms: started.elapsed().as_millis() as u64,
                    cache_hit: true,
                    prices: route_chain.first().map(|route| {
                        (
                            route.input_price_micros_per_million,
                            route.output_price_micros_per_million,
                        )
                    }),
                },
            )
            .await;
            let mut response = Response::builder().status(status);
            for (name, value) in headers {
                response = response.header(name, value);
            }
            return response
                .body(Body::from(body))
                .unwrap_or_else(|_| Response::new(Body::empty()));
        }
    }

    let attempts: Vec<Option<&ResolvedRoute>> = if route_chain.is_empty() {
        vec![None]
    } else {
        route_chain.iter().copied().map(Some).collect()
    };
    let mut selected_route = route_chain.first().copied();
    let mut result = None;
    for (idx, route) in attempts.iter().enumerate() {
        selected_route = *route;
        let upstream_url = route.map_or_else(
            || format!("{}{}{}", state.config.upstream_url, path, query),
            |r| format!("{}{}{}", r.upstream_url.trim_end_matches('/'), path, query),
        );
        let mut builder = state.client.request(
            reqwest::Method::from_bytes(method.as_str().as_bytes()).unwrap_or(reqwest::Method::GET),
            &upstream_url,
        );
        for (name, value) in &forwarded {
            builder = builder.header(*name, value.as_slice());
        }
        builder = builder.header("content-type", "application/json");
        match route.and_then(|r| r.auth.as_ref()) {
            Some(ResolvedAuth::Bearer(key)) => builder = builder.bearer_auth(key),
            Some(ResolvedAuth::ApiKey(key)) => builder = builder.header("x-api-key", key),
            _ if route.is_none() => match &state.config.upstream_auth {
                Some(UpstreamAuth::Bearer(key)) => builder = builder.bearer_auth(key),
                Some(UpstreamAuth::ApiKey(key)) => builder = builder.header("x-api-key", key),
                None => {}
            },
            _ => {}
        }
        let mut request = match builder.body(body_bytes.clone()).build() {
            Ok(request) => request,
            Err(_) => {
                return error_response(&state, StatusCode::BAD_GATEWAY, "invalid upstream request")
            }
        };
        if let Some(ResolvedRoute {
            auth:
                Some(ResolvedAuth::AwsSigv4 {
                    access_key,
                    secret_key,
                    session_token,
                    region,
                    service,
                }),
            ..
        }) = route
        {
            if sign_aws_request(
                &mut request,
                &upstream_url,
                AwsSigningConfig {
                    access_key,
                    secret_key,
                    session_token: session_token.as_deref(),
                    region,
                    service,
                },
                &body_bytes,
            )
            .is_err()
            {
                return error_response(
                    &state,
                    StatusCode::BAD_GATEWAY,
                    "failed to sign upstream request",
                );
            }
        }
        match state.client.execute(request).await {
            Ok(response) => {
                let retry = response.status() == StatusCode::TOO_MANY_REQUESTS
                    || response.status().is_server_error();
                if retry && idx + 1 < attempts.len() {
                    continue;
                }
                result = Some(Ok(response));
                break;
            }
            Err(error) => {
                let retry = error.is_connect();
                if retry && idx + 1 < attempts.len() {
                    continue;
                }
                result = Some(Err(error));
                break;
            }
        }
    }
    let upstream_resp = match result {
        Some(Ok(response)) => response,
        _ => {
            if let Err(error) = state.budgets.record(workload, estimated_tokens) {
                tracing::warn!(workload, error = %error, "budget exceeded after failed upstream request");
            }
            let route = selected_route.map_or(route_name, |route| route.name.clone());
            let answered_model = requested_model.clone();
            publish_usage(
                &state,
                Arc::clone(&identity),
                UsagePublication {
                    customer_id,
                    route,
                    requested_model,
                    answered_model,
                    counts: UsageCounts {
                        input_tokens: estimated_tokens,
                        output_tokens: 0,
                        estimated: true,
                    },
                    status: StatusCode::BAD_GATEWAY,
                    latency_ms: started.elapsed().as_millis() as u64,
                    cache_hit: false,
                    prices: selected_route.map(|route| {
                        (
                            route.input_price_micros_per_million,
                            route.output_price_micros_per_million,
                        )
                    }),
                },
            )
            .await;
            return error_response(&state, StatusCode::BAD_GATEWAY, "upstream request failed");
        }
    };
    let status = upstream_resp.status();
    let resp_headers: Vec<(String, String)> = upstream_resp
        .headers()
        .iter()
        .filter(|(name, _)| {
            !matches!(
                name.as_str(),
                "content-length" | "transfer-encoding" | "connection"
            )
        })
        .map(|(name, value)| (name.to_string(), value.to_str().unwrap_or("").to_string()))
        .collect();
    let is_sse = upstream_resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.to_ascii_lowercase().contains("text/event-stream"));
    let mut redactor = state.redactor.clone();
    for route in &route_chain {
        if let Some(auth) = &route.auth {
            match auth {
                ResolvedAuth::Bearer(secret) | ResolvedAuth::ApiKey(secret) => {
                    redactor = redactor.with_literal("route-credential", secret)
                }
                ResolvedAuth::AwsSigv4 {
                    access_key,
                    secret_key,
                    session_token,
                    ..
                } => {
                    redactor = redactor
                        .with_literal("route-access-key", access_key)
                        .with_literal("route-secret-key", secret_key);
                    if let Some(secret) = session_token {
                        redactor = redactor.with_literal("route-session-token", secret);
                    }
                }
            }
        }
    }
    if !is_sse && !request_stream {
        let raw = match upstream_resp.bytes().await {
            Ok(b) => b,
            Err(_) => {
                return error_response(
                    &state,
                    StatusCode::BAD_GATEWAY,
                    "failed to read upstream response",
                )
            }
        };
        let counts = find_usage_fragment(&raw).unwrap_or(UsageCounts {
            input_tokens: estimated_tokens,
            output_tokens: raw.len() as u64 / 4,
            estimated: true,
        });
        let answered_model = find_answered_model(&raw).unwrap_or_else(|| requested_model.clone());
        let body = if (status.is_client_error() || status.is_server_error())
            && state.config.redact_errors
        {
            redactor.redact(&String::from_utf8_lossy(&raw)).into_bytes()
        } else {
            raw.to_vec()
        };
        let total = if counts.estimated {
            estimated_tokens.saturating_add(counts.output_tokens)
        } else {
            counts.input_tokens.saturating_add(counts.output_tokens)
        };
        if let Err(error) = state.budgets.record(workload, total) {
            tracing::warn!(workload, error = %error, "budget exceeded after upstream call");
        }
        let used_requested_route = match (selected_route, route_chain.first()) {
            (Some(selected), Some(requested)) => selected.name == requested.name,
            (None, None) => true,
            _ => false,
        };
        if used_requested_route {
            if let Some(cache) = &state.cache {
                cache.put(
                    &cache_key,
                    status.as_u16(),
                    resp_headers.clone(),
                    body.clone(),
                );
            }
        }
        publish_usage(
            &state,
            Arc::clone(&identity),
            UsagePublication {
                customer_id,
                route: selected_route.map_or_else(String::new, |route| route.name.clone()),
                requested_model,
                answered_model,
                counts,
                status,
                latency_ms: started.elapsed().as_millis() as u64,
                cache_hit: false,
                prices: selected_route.map(|route| {
                    (
                        route.input_price_micros_per_million,
                        route.output_price_micros_per_million,
                    )
                }),
            },
        )
        .await;
        let mut response = Response::builder().status(status);
        for (name, value) in &resp_headers {
            response = response.header(name, value);
        }
        return response
            .body(Body::from(body))
            .unwrap_or_else(|_| Response::new(Body::empty()));
    }
    let mut stream = upstream_resp.bytes_stream();
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let budgets = Arc::clone(&state.budgets);
    let workload = workload.to_owned();
    let identity = Arc::clone(&identity);
    let customer_id = customer_id.clone();
    let event_bus = state.bus.clone();
    let route = selected_route.map(|r| r.name.clone()).unwrap_or_default();
    let prices = selected_route.map(|r| {
        (
            r.input_price_micros_per_million,
            r.output_price_micros_per_million,
        )
    });
    let redact =
        state.config.redact_errors && (status.is_client_error() || status.is_server_error());
    let stream_redactor = redactor.clone();
    tokio::spawn(async move {
        use futures_util::StreamExt;
        let mut parser = UsageParser::new(is_sse);
        let mut bytes_seen = 0_u64;
        let mut tail = Vec::new();
        while let Some(item) = stream.next().await {
            let chunk = match item {
                Ok(chunk) => chunk,
                Err(error) => {
                    let _ = sender
                        .send(Err(std::io::Error::other(error.to_string())))
                        .await;
                    break;
                }
            };
            bytes_seen = bytes_seen.saturating_add(chunk.len() as u64);
            parser.push(&chunk);
            if redact {
                tail.extend_from_slice(&chunk);
                if tail.len() > 8192 {
                    let mut split = tail.len() - 8192;
                    while split > 0 && tail[split] & 0b1100_0000 == 0b1000_0000 {
                        split -= 1;
                    }
                    let safe = String::from_utf8_lossy(&tail[..split]);
                    let output = stream_redactor.redact(&safe);
                    if sender.send(Ok(bytes::Bytes::from(output))).await.is_err() {
                        break;
                    }
                    tail.drain(..split);
                }
            } else if sender.send(Ok(chunk)).await.is_err() {
                break;
            }
        }
        if redact && !tail.is_empty() {
            let safe = String::from_utf8_lossy(&tail);
            let _ = sender
                .send(Ok(bytes::Bytes::from(stream_redactor.redact(&safe))))
                .await;
        }
        let (mut counts, answered_model) = parser.finish_with_model(bytes_seen);
        let answered_model = answered_model.unwrap_or_else(|| requested_model.clone());
        if counts.estimated {
            counts.input_tokens = estimated_tokens;
        }
        let total = if counts.estimated {
            estimated_tokens.saturating_add(counts.output_tokens)
        } else {
            counts.input_tokens.saturating_add(counts.output_tokens)
        };
        if let Err(error) = budgets.record(&workload, total) {
            tracing::warn!(workload, error = %error, "budget exceeded after upstream call");
        }
        publish_event(
            event_bus,
            identity,
            UsagePublication {
                customer_id,
                route,
                requested_model,
                answered_model,
                counts,
                status,
                latency_ms: started.elapsed().as_millis() as u64,
                cache_hit: false,
                prices,
            },
        )
        .await;
    });
    let mut response = Response::builder().status(status);
    for (name, value) in &resp_headers {
        response = response.header(name, value);
    }
    response
        .body(Body::from_stream(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        ))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

fn safe_request_id() -> String {
    let bytes: [u8; 16] = rand::random();
    let mut id = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(id, "{byte:02x}");
    }
    id
}

struct UsagePublication {
    customer_id: String,
    route: String,
    requested_model: String,
    answered_model: String,
    counts: UsageCounts,
    status: StatusCode,
    latency_ms: u64,
    cache_hit: bool,
    prices: Option<(Option<u64>, Option<u64>)>,
}

async fn publish_usage(
    state: &ProxyState,
    identity: Arc<WorkloadIdentity>,
    usage: UsagePublication,
) {
    publish_event(state.bus.clone(), identity, usage).await;
}

async fn publish_event(
    bus: Option<Arc<dyn EventBus>>,
    identity: Arc<WorkloadIdentity>,
    usage: UsagePublication,
) {
    let Some(bus) = bus else { return };
    let input_tokens = usage.counts.input_tokens;
    let output_tokens = usage.counts.output_tokens;
    let cost_micros = usage.prices.and_then(|(input_price, output_price)| {
        let (Some(input_price), Some(output_price)) = (input_price, output_price) else {
            return None;
        };
        let input_cost = u128::from(input_tokens).checked_mul(u128::from(input_price))?;
        let output_cost = u128::from(output_tokens).checked_mul(u128::from(output_price))?;
        u64::try_from(input_cost.checked_add(output_cost)? / 1_000_000).ok()
    });
    let event = UsageEvent {
        request_id: safe_request_id(),
        tenant_id: identity.tenant_id.clone(),
        service_id: identity.service_id.clone(),
        task_id: identity.workload_id.clone(),
        customer_id: usage.customer_id,
        route: usage.route,
        requested_model: usage.requested_model,
        answered_model: usage.answered_model,
        input_tokens,
        output_tokens,
        cache_hit: usage.cache_hit,
        latency_ms: usage.latency_ms,
        status: usage.status.as_u16(),
        occurred_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        cost_micros,
    };
    if let Err(error) = bus.publish_usage(&event).await {
        tracing::warn!(error = %error, "failed to publish egress usage event");
    }
}

struct AwsSigningConfig<'a> {
    access_key: &'a str,
    secret_key: &'a str,
    session_token: Option<&'a str>,
    region: &'a str,
    service: &'a str,
}

fn sign_aws_request(
    request: &mut reqwest::Request,
    url: &str,
    auth: AwsSigningConfig<'_>,
    body: &[u8],
) -> Result<(), ()> {
    use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
    use aws_sigv4::sign::v4;
    use aws_smithy_runtime_api::client::identity::Identity;
    let credentials = aws_credential_types::Credentials::new(
        auth.access_key,
        auth.secret_key,
        auth.session_token.map(str::to_owned),
        None,
        "minga-egress",
    );
    let identity: Identity = credentials.into();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(auth.region)
        .name(auth.service)
        .time(SystemTime::now())
        .settings(SigningSettings::default())
        .build()
        .map_err(|_| ())?
        .into();
    let headers: Vec<(&str, &str)> = request
        .headers()
        .iter()
        .filter_map(|(name, value)| value.to_str().ok().map(|value| (name.as_str(), value)))
        .collect();
    let signable = SignableRequest::new(
        request.method().as_str(),
        url,
        headers.into_iter(),
        SignableBody::Bytes(body),
    )
    .map_err(|_| ())?;
    let instructions = sign(signable, &params).map_err(|_| ())?.into_parts().0;
    for (name, value) in instructions.headers() {
        let mut value = reqwest::header::HeaderValue::from_str(value).map_err(|_| ())?;
        value.set_sensitive(true);
        request.headers_mut().insert(
            reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|_| ())?,
            value,
        );
    }
    Ok(())
}

/// Provider-reported request and response token counts for one completion.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageCounts {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub estimated: bool,
}

const MAX_USAGE_EVENT_BYTES: usize = 64 * 1024;

struct UsageParser {
    is_sse: bool,
    line: Vec<u8>,
    event: Vec<u8>,
    line_overflow: bool,
    discard_event: bool,
    json_scan: Vec<u8>,
    json_usage_disabled: bool,
    model_scan: Vec<u8>,
    model_scan_disabled: bool,
    answered_model: Option<String>,
    usage: Option<UsageCounts>,
}

impl UsageParser {
    fn new(is_sse: bool) -> Self {
        Self {
            is_sse,
            line: Vec::new(),
            event: Vec::new(),
            line_overflow: false,
            discard_event: false,
            json_scan: Vec::new(),
            json_usage_disabled: false,
            model_scan: Vec::new(),
            model_scan_disabled: false,
            answered_model: None,
            usage: None,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        if !self.is_sse && self.answered_model.is_none() && !self.model_scan_disabled {
            if chunk.len() > MAX_USAGE_EVENT_BYTES.saturating_sub(self.model_scan.len()) {
                self.model_scan.clear();
                self.model_scan_disabled = true;
            } else {
                self.model_scan.extend_from_slice(chunk);
                self.answered_model = find_answered_model(&self.model_scan);
                if self.answered_model.is_some() {
                    self.model_scan.clear();
                }
            }
        }
        if self.is_sse {
            for &byte in chunk {
                if byte == b'\n' {
                    if self.line_overflow {
                        self.line_overflow = false;
                        self.line.clear();
                    } else {
                        self.process_sse_line();
                    }
                } else if !self.line_overflow {
                    if self.line.len() == MAX_USAGE_EVENT_BYTES {
                        self.line.clear();
                        self.event.clear();
                        self.line_overflow = true;
                        self.discard_event = true;
                    } else {
                        self.line.push(byte);
                    }
                }
            }
        } else if !self.json_usage_disabled && self.usage.is_none() {
            if chunk.len() > MAX_USAGE_EVENT_BYTES.saturating_sub(self.json_scan.len()) {
                self.json_scan.clear();
                self.json_usage_disabled = true;
                self.usage = None;
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
            if !self.discard_event {
                if let Some(model) = find_answered_model(&self.event) {
                    self.answered_model = Some(model);
                }
                if let Some(usage) = find_usage(&self.event) {
                    self.record_usage(usage);
                }
            }
            self.event.clear();
            self.discard_event = false;
        } else if !self.discard_event {
            if let Some(data) = self.line.strip_prefix(b"data:") {
                let data = data.strip_prefix(b" ").unwrap_or(data);
                if self.event.len() + data.len() + 1 > MAX_USAGE_EVENT_BYTES {
                    self.event.clear();
                    self.discard_event = true;
                } else {
                    self.event.extend_from_slice(data);
                    self.event.push(b'\n');
                }
            }
        }
        self.line.clear();
    }

    fn finish_with_model(mut self, response_bytes: u64) -> (UsageCounts, Option<String>) {
        if self.is_sse {
            if !self.line.is_empty() {
                self.process_sse_line();
            }
            if !self.event.is_empty() && !self.discard_event {
                if let Some(model) = find_answered_model(&self.event) {
                    self.answered_model = Some(model);
                }
                if let Some(usage) = find_usage(&self.event) {
                    self.record_usage(usage);
                }
            }
        }
        let usage = self.usage.unwrap_or(UsageCounts {
            input_tokens: 0,
            output_tokens: response_bytes / 4,
            estimated: true,
        });
        (usage, self.answered_model)
    }
}

fn find_usage(bytes: &[u8]) -> Option<UsageCounts> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let usage = value.get("usage").or_else(|| {
        value
            .get("message")
            .and_then(|message| message.get("usage"))
    })?;
    usage_counts(Some(usage))
}

fn find_answered_model(bytes: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    value
        .get("model")
        .or_else(|| {
            value
                .get("message")
                .and_then(|message| message.get("model"))
        })
        .and_then(serde_json::Value::as_str)
        .filter(|model| !model.is_empty())
        .map(str::to_owned)
}

fn find_usage_fragment(bytes: &[u8]) -> Option<UsageCounts> {
    let marker = bytes
        .windows(b"\"usage\"".len())
        .position(|w| w == b"\"usage\"")?;
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
            bus: None,
        })
    }

    #[tokio::test]
    async fn health_endpoint() {
        let resp = health_handler().await;
        assert_eq!(resp, "OK");
    }
    #[tokio::test]
    async fn policy_rejects_disallowed_model_and_unknown_route() {
        use crate::routes::{ResolvedEgressPolicy, ResolvedRoute, WorkloadIdentity};
        let state = test_state();
        let mut policy = ResolvedEgressPolicy::default();
        policy.models.push("allowed-model".into());
        for name in ["primary", "secondary"] {
            policy.routes.insert(
                name.into(),
                ResolvedRoute {
                    name: name.into(),
                    upstream_url: "http://127.0.0.1:1".into(),
                    auth: None,
                    fallback: Vec::new(),
                    input_price_micros_per_million: None,
                    output_price_micros_per_million: None,
                },
            );
        }
        let token = state.tokens.issue_identity(WorkloadIdentity {
            workload_id: "test-agent".into(),
            tenant_id: "default".into(),
            service_id: "test-agent".into(),
            policy: Some(policy),
        });
        let disallowed = Request::builder()
            .uri("/v1/chat")
            .header(SANDBOX_TOKEN_HEADER, token.as_str())
            .header("x-minga-route", "primary")
            .body(Body::from(r#"{"model":"denied"}"#))
            .unwrap();
        assert_eq!(
            proxy_handler(State(Arc::clone(&state)), disallowed)
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
        let header_only_model = Request::builder()
            .uri("/v1/chat")
            .header(SANDBOX_TOKEN_HEADER, token.as_str())
            .header("x-minga-route", "primary")
            .header("x-minga-model", "allowed-model")
            .body(Body::from("{}"))
            .unwrap();
        assert_eq!(
            proxy_handler(State(Arc::clone(&state)), header_only_model)
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
        let unknown_route = Request::builder()
            .uri("/v1/chat")
            .header(SANDBOX_TOKEN_HEADER, token.as_str())
            .header("x-minga-route", "missing")
            .body(Body::from(r#"{"model":"allowed-model"}"#))
            .unwrap();
        assert_eq!(
            proxy_handler(State(state), unknown_route).await.status(),
            StatusCode::BAD_REQUEST
        );
    }
    #[tokio::test]
    async fn rate_limited_route_falls_back_to_secondary() {
        use crate::routes::{ResolvedEgressPolicy, ResolvedRoute, WorkloadIdentity};
        let mut addresses = Vec::new();
        for status in [StatusCode::TOO_MANY_REQUESTS, StatusCode::OK] {
            let app = Router::new().route(
                "/*path",
                any(move || async move {
                    Response::builder()
                        .status(status)
                        .body(Body::from("ok"))
                        .unwrap()
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(listener.local_addr().unwrap());
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
        }
        let state = test_state();
        let mut policy = ResolvedEgressPolicy::default();
        policy.routes.insert(
            "primary".into(),
            ResolvedRoute {
                name: "primary".into(),
                upstream_url: format!("http://{}", addresses[0]),
                auth: None,
                fallback: vec!["secondary".into()],
                input_price_micros_per_million: None,
                output_price_micros_per_million: None,
            },
        );
        policy.routes.insert(
            "secondary".into(),
            ResolvedRoute {
                name: "secondary".into(),
                upstream_url: format!("http://{}", addresses[1]),
                auth: None,
                fallback: Vec::new(),
                input_price_micros_per_million: None,
                output_price_micros_per_million: None,
            },
        );
        let token = state.tokens.issue_identity(WorkloadIdentity {
            workload_id: "test-agent".into(),
            tenant_id: "default".into(),
            service_id: "test-agent".into(),
            policy: Some(policy),
        });
        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat")
            .header(SANDBOX_TOKEN_HEADER, token.as_str())
            .header("x-minga-route", "primary")
            .body(Body::from("{}"))
            .unwrap();
        assert_eq!(
            proxy_handler(State(state), request).await.status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn client_error_does_not_fall_back_to_another_route() {
        use crate::routes::{ResolvedEgressPolicy, ResolvedRoute, WorkloadIdentity};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let fallback_hits = Arc::new(AtomicUsize::new(0));
        let mut addresses = Vec::new();
        for status in [StatusCode::BAD_REQUEST, StatusCode::OK] {
            let hits = Arc::clone(&fallback_hits);
            let app = Router::new().route(
                "/*path",
                any(move || async move {
                    if status == StatusCode::OK {
                        hits.fetch_add(1, Ordering::Relaxed);
                    }
                    Response::builder()
                        .status(status)
                        .body(Body::from("response"))
                        .unwrap()
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(listener.local_addr().unwrap());
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
        }

        let state = test_state();
        let mut policy = ResolvedEgressPolicy::default();
        for (name, address, fallback) in [
            ("primary", addresses[0], vec!["secondary".into()]),
            ("secondary", addresses[1], Vec::new()),
        ] {
            policy.routes.insert(
                name.into(),
                ResolvedRoute {
                    name: name.into(),
                    upstream_url: format!("http://{address}"),
                    auth: None,
                    fallback,
                    input_price_micros_per_million: None,
                    output_price_micros_per_million: None,
                },
            );
        }
        let token = state.tokens.issue_identity(WorkloadIdentity {
            workload_id: "test-agent".into(),
            tenant_id: "default".into(),
            service_id: "test-agent".into(),
            policy: Some(policy),
        });
        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat")
            .header(SANDBOX_TOKEN_HEADER, token.as_str())
            .header("x-minga-route", "primary")
            .body(Body::from("{}"))
            .unwrap();
        assert_eq!(
            proxy_handler(State(state), request).await.status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(fallback_hits.load(Ordering::Relaxed), 0);
    }
    #[tokio::test]
    async fn usage_events_are_body_free_and_cost_is_integer_micros() {
        use crate::routes::WorkloadIdentity;
        let bus: Arc<dyn EventBus> = minga_bus::MemoryBus::new();
        publish_event(
            Some(Arc::clone(&bus)),
            Arc::new(WorkloadIdentity {
                workload_id: "task-a".into(),
                tenant_id: "default".into(),
                service_id: "service-a".into(),
                policy: None,
            }),
            UsagePublication {
                customer_id: "customer-a".into(),
                route: "route-a".into(),
                requested_model: "model-a".into(),
                answered_model: "provider-model-a".into(),
                counts: UsageCounts {
                    input_tokens: 5,
                    output_tokens: 7,
                    estimated: false,
                },
                status: StatusCode::OK,
                latency_ms: 3,
                cache_hit: false,
                prices: Some((Some(2_000_000), Some(3_000_000))),
            },
        )
        .await;
        let events = bus.usage_events("default", 0, None, 10).await.unwrap();
        assert_eq!(events[0].cost_micros, Some(31));
        assert_eq!(events[0].answered_model, "provider-model-a");
        let flattened = serde_json::to_value(&events[0]).unwrap();
        assert!(flattened.get("request_body").is_none());
        assert!(flattened.get("response_body").is_none());
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
    fn captures_answered_model_from_json_and_streaming_events() {
        assert_eq!(
            find_answered_model(br#"{"model":"provider-model"}"#).as_deref(),
            Some("provider-model")
        );

        let event = b"data: {\"message\":{\"model\":\"stream-model\"}}\n";
        let mut parser = UsageParser::new(true);
        parser.push(event);
        let (_, answered_model) = parser.finish_with_model(event.len() as u64);
        assert_eq!(answered_model.as_deref(), Some("stream-model"));
    }

    #[test]
    fn parses_sse_usage_across_arbitrary_chunks() {
        let event = b"data: {\"type\":\"message_delta\",\"usage\":{\"input_tokens\":31,\"output_tokens\":4}}\r\n\r\n";
        let mut parser = UsageParser::new(true);
        for part in event.chunks(3) {
            parser.push(part);
        }
        assert_eq!(
            parser.finish_with_model(event.len() as u64).0,
            UsageCounts {
                input_tokens: 31,
                output_tokens: 4,
                estimated: false,
            }
        );
    }

    #[test]
    fn parses_openai_sse_usage_across_arbitrary_chunks() {
        let event =
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3}}\n\n";
        let mut parser = UsageParser::new(true);
        for part in event.chunks(2) {
            parser.push(part);
        }
        assert_eq!(
            parser.finish_with_model(event.len() as u64).0,
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
            parser.finish_with_model(events.len() as u64).0,
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
            parser.finish_with_model(body.len() as u64).0,
            UsageCounts {
                input_tokens: 0,
                output_tokens: (body.len() / 4) as u64,
                estimated: true,
            }
        );
    }

    #[test]
    fn json_usage_parser_disables_oversized_usage_and_falls_back() {
        let mut parser = UsageParser::new(false);
        parser.push(br#"{"usage":{"#);
        let oversized = vec![b' '; MAX_USAGE_EVENT_BYTES];
        parser.push(&oversized);
        assert!(parser.json_usage_disabled);
        assert!(parser.json_scan.is_empty());
        assert_eq!(
            parser.finish_with_model(40).0,
            UsageCounts {
                input_tokens: 0,
                output_tokens: 10,
                estimated: true,
            }
        );
    }

    #[test]
    fn sse_usage_parser_caps_oversized_event_buffers() {
        let mut parser = UsageParser::new(true);
        parser.push(b"data:");
        let oversized = vec![b'x'; MAX_USAGE_EVENT_BYTES + 128];
        parser.push(&oversized);
        assert!(parser.line.len() <= MAX_USAGE_EVENT_BYTES);
        assert!(parser.event.len() <= MAX_USAGE_EVENT_BYTES);
        parser.push(b"\n\n");
        assert!(parser.line.is_empty());
        assert!(parser.event.is_empty());
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
                    .body(AxumBody::from_stream(
                        tokio_stream::wrappers::ReceiverStream::new(receiver),
                    ))
                    .unwrap()
            }),
        );
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream_listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(upstream_listener, app).await.ok() });

        let budgets = Arc::new(BudgetTracker::new());
        budgets.register(
            "stream-task",
            Budget::new(1_000_000, Duration::from_secs(3600)),
        );
        let tokens = Arc::new(SandboxTokens::new());
        let token = tokens.issue("stream-task");
        let mut proxy = EgressProxy::new(
            EgressProxyConfig {
                upstream_url: format!("http://{upstream_addr}"),
                ..Default::default()
            },
            budgets,
            tokens,
        );
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
            bus: None,
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
