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
    /// Destinations a tenant route may reach although they are loopback,
    /// link-local, private, unspecified or multicast (`--egress-allow-cidr`).
    pub allowed_route_cidrs: Vec<ipnet::IpNet>,
    /// Longest wait for the next chunk of a streamed upstream response.
    pub stream_idle_timeout: Duration,
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
            allowed_route_cidrs: Vec::new(),
            stream_idle_timeout: Duration::from_secs(60),
        }
    }
}

/// Shared state for the proxy handlers.
struct ProxyState {
    config: EgressProxyConfig,
    budgets: Arc<BudgetTracker>,
    cache: Option<Arc<ResponseCache>>,
    redactor: SecretRedactor,
    /// Client for the operator's upstream.
    client: reqwest::Client,
    /// Client for tenant routes: no redirects, no proxy, and DNS answers
    /// filtered to allowed destinations. `None` if it could not be built.
    route_client: Option<reqwest::Client>,
    tokens: Arc<SandboxTokens>,
    bus: std::sync::OnceLock<Arc<dyn EventBus>>,
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
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let route_client = route_client(&config);

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
                route_client,
                tokens,
                bus: std::sync::OnceLock::new(),
            }),
            addr: None,
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Configures durable usage event publishing. Routers built earlier
    /// share the state, so they publish too; a second bus is refused.
    pub fn with_event_bus(self, bus: Arc<dyn EventBus>) -> Result<Self, EgressProxyError> {
        self.state
            .bus
            .set(bus)
            .map_err(|_| EgressProxyError::Internal("usage event bus is already set".into()))?;
        Ok(self)
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
        self.serve(listener)
    }

    /// Serves on an already bound listener, so the caller can install a
    /// firewall rule for the port between binding and serving.
    pub fn serve(&mut self, listener: tokio::net::TcpListener) -> Result<String, EgressProxyError> {
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

/// Longest accepted model, customer and route name, in bytes.
const MAX_IDENTIFIER_BYTES: usize = 256;
/// Largest upstream body the proxy buffers when it does not stream.
const MAX_UPSTREAM_BODY_BYTES: usize = 32 * 1024 * 1024;
/// Largest upstream error body buffered for redaction.
const MAX_ERROR_BODY_BYTES: usize = 64 * 1024;
/// Per-attempt deadline and attempts for publishing one usage event.
const USAGE_PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);
const USAGE_PUBLISH_ATTEMPTS: u32 = 3;

fn header_str<'a>(req: &'a Request, name: &str) -> Option<&'a str> {
    req.headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
}

/// Whether a tenant route may connect to `ip`. Loopback, link-local,
/// unspecified, multicast, broadcast and private or unique-local addresses
/// are refused unless an operator CIDR allows them.
fn destination_allowed(ip: std::net::IpAddr, allowed: &[ipnet::IpNet]) -> bool {
    use std::net::IpAddr;
    if allowed.iter().any(|net| net.contains(&ip)) {
        return true;
    }
    // IPv4-mapped and NAT64 (64:ff9b::/96) addresses reach the embedded IPv4.
    let ip = match ip {
        IpAddr::V6(v6) => match v6.segments() {
            [0x64, 0xff9b, 0, 0, 0, 0, hi, lo] => IpAddr::V4(std::net::Ipv4Addr::from(
                (u32::from(hi) << 16) | u32::from(lo),
            )),
            _ => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        },
        v4 => v4,
    };
    if allowed.iter().any(|net| net.contains(&ip)) {
        return true;
    }
    match ip {
        IpAddr::V4(v4) => {
            let [first, second, ..] = v4.octets();
            // 0.0.0.0/8 reaches local addresses on Linux; 100.64.0.0/10 is
            // shared (CGNAT, overlay) address space.
            !(first == 0
                || (first == 100 && (64..128).contains(&second))
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_multicast()
                || v4.is_broadcast()
                || v4.is_private())
        }
        IpAddr::V6(v6) => {
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || v6.is_unicast_link_local()
                || v6.is_unique_local())
        }
    }
}

/// Refuses a route URL whose host is a literal address that is not allowed;
/// hostnames are checked after DNS by [`RouteResolver`].
fn literal_destination_allowed(url: &str, allowed: &[ipnet::IpNet]) -> bool {
    let Ok(url) = reqwest::Url::parse(url) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    host.parse::<std::net::IpAddr>()
        .map_or(true, |ip| destination_allowed(ip, allowed))
}

/// Resolves route hostnames and keeps only allowed addresses, so a name
/// cannot point a tenant route at the node or its private networks.
struct RouteResolver {
    allowed: Arc<[ipnet::IpNet]>,
}

impl reqwest::dns::Resolve for RouteResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let allowed = Arc::clone(&self.allowed);
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host((host.as_str(), 0))
                .await?
                .filter(|addr| destination_allowed(addr.ip(), &allowed))
                .collect();
            if addrs.is_empty() {
                return Err("egress route destination is not allowed".into());
            }
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

fn route_client(config: &EgressProxyConfig) -> Option<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(config.timeout)
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .dns_resolver(Arc::new(RouteResolver {
            allowed: config.allowed_route_cidrs.clone().into(),
        }))
        .build()
        .ok()
}

/// Reads at most `limit` body bytes.
async fn read_bounded(
    response: reqwest::Response,
    limit: usize,
) -> Result<bytes::Bytes, BodyError> {
    use futures_util::StreamExt;
    let mut stream = response.bytes_stream();
    let mut body = bytes::BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BodyError::Read)?;
        if body.len() + chunk.len() > limit {
            return Err(BodyError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

enum BodyError {
    Read,
    TooLarge,
}

/// Tokens charged for a completion: provider counts when reported, else the
/// request estimate for an unknown input plus the estimated output.
fn charged_tokens(counts: &UsageCounts, estimated_input: u64) -> u64 {
    let input = if counts.estimated && counts.input_tokens == 0 {
        estimated_input
    } else {
        counts.input_tokens
    };
    input.saturating_add(counts.output_tokens)
}

fn route_prices(route: Option<&ResolvedRoute>) -> Option<(Option<u64>, Option<u64>)> {
    route.map(|route| {
        (
            route.input_price_micros_per_million,
            route.output_price_micros_per_million,
        )
    })
}

/// Settles the reservation and publishes usage in the background, so a
/// slow bus never delays the response.
fn finish_usage(
    state: &ProxyState,
    identity: &Arc<WorkloadIdentity>,
    reservation: crate::budget::Reservation,
    estimated_input: u64,
    usage: UsagePublication,
) {
    let charged = charged_tokens(&usage.counts, estimated_input);
    if !reservation.settle(charged) {
        tracing::warn!(
            workload = identity.workload_id.as_str(),
            charged,
            "egress usage took the workload past its token budget"
        );
    }
    tokio::spawn(publish_event(
        state.bus.get().cloned(),
        Arc::clone(identity),
        usage,
    ));
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
    let Some(identity) =
        header_str(&req, SANDBOX_TOKEN_HEADER).and_then(|t| state.tokens.identity(t))
    else {
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
    let customer_id = header_str(&req, "x-minga-customer")
        .unwrap_or("")
        .to_owned();
    let mut requested_model = header_str(&req, "x-minga-model").unwrap_or("").to_owned();
    let selected_header = header_str(&req, "x-minga-route").map(str::to_owned);
    if [
        customer_id.as_str(),
        requested_model.as_str(),
        selected_header.as_deref().unwrap_or(""),
    ]
    .iter()
    .any(|value| value.len() > MAX_IDENTIFIER_BYTES)
    {
        return error_response(
            &state,
            StatusCode::BAD_REQUEST,
            "customer, model and route names are limited to 256 bytes",
        );
    }
    let workload = identity.workload_id.as_str();
    let forwarded: Vec<(&str, Vec<u8>)> = FORWARDED_HEADERS
        .iter()
        .filter_map(|&n| req.headers().get(n).map(|v| (n, v.as_bytes().to_vec())))
        .collect();
    let policy = identity.policy.as_ref();
    let mut route_chain: Vec<&ResolvedRoute> = Vec::new();
    let route_name = if let Some(policy) = policy {
        let selected = selected_header.or_else(|| {
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
    if body_model.is_some_and(|model| model.len() > MAX_IDENTIFIER_BYTES) {
        return error_response(
            &state,
            StatusCode::BAD_REQUEST,
            "customer, model and route names are limited to 256 bytes",
        );
    }
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
    // Reserve an upper bound so concurrent requests cannot spend the same
    // balance: a token covers at least one byte, so the body length bounds
    // the input, and without an output cap the request holds the rest.
    let input_bound = body_bytes.len() as u64;
    let max_output = request_json
        .as_ref()
        .and_then(|value| {
            value
                .get("max_tokens")
                .or_else(|| value.get("max_completion_tokens"))
        })
        .and_then(serde_json::Value::as_u64);
    let request_stream = request_json
        .as_ref()
        .and_then(|value| value.get("stream"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    // OpenAI bills output for every one of `n` choices.
    let choices = request_json
        .as_ref()
        .and_then(|value| value.get("n"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1)
        .max(1);
    let reservation = match max_output {
        Some(cap) => state.budgets.reserve(
            workload,
            input_bound.saturating_add(cap.saturating_mul(choices)),
        ),
        None => state.budgets.reserve_rest(workload, input_bound),
    };
    let reservation = match reservation {
        Ok(reservation) => reservation,
        Err(e) => return error_response(&state, StatusCode::TOO_MANY_REQUESTS, &e.to_string()),
    };
    // Forwarded headers (API version, beta flags, accept) change the
    // response, so each value is part of the key; escaping keeps it injective.
    let mut cache_path = format!("{path}{query}\0{requested_model}");
    for (name, value) in &forwarded {
        cache_path.push_str(&format!("\0{name}={}", value.escape_ascii()));
    }
    let cache_route = match policy {
        Some(policy) => format!("{route_name}\0{:016x}", policy.revision()),
        None => route_name.clone(),
    };
    let cache_key = ResponseCache::compute_key_for_workload_route(
        workload,
        &customer_id,
        &cache_route,
        method.as_str(),
        &cache_path,
        &body_bytes,
    );
    if let Some(cache) = &state.cache {
        if let Some((status, headers, body)) = cache.get(&cache_key) {
            let counts = find_usage(&body).unwrap_or(UsageCounts {
                input_tokens: estimated_tokens,
                output_tokens: body.len() as u64 / 4,
                estimated: true,
            });
            let answered_model =
                find_answered_model(&body).unwrap_or_else(|| requested_model.clone());
            finish_usage(
                &state,
                &identity,
                reservation,
                estimated_tokens,
                UsagePublication {
                    customer_id,
                    route: route_name,
                    requested_model,
                    answered_model,
                    counts,
                    status: StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
                    latency_ms: started.elapsed().as_millis() as u64,
                    cache_hit: true,
                    prices: route_prices(route_chain.first().copied()),
                },
            );
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
        let client = if route.is_some() {
            if !literal_destination_allowed(&upstream_url, &state.config.allowed_route_cidrs) {
                return error_response(
                    &state,
                    StatusCode::FORBIDDEN,
                    "egress route destination is not allowed",
                );
            }
            let Some(client) = &state.route_client else {
                return error_response(
                    &state,
                    StatusCode::BAD_GATEWAY,
                    "egress route client is unavailable",
                );
            };
            client
        } else {
            &state.client
        };
        let mut builder = client.request(
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
        match client.execute(request).await {
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
    // The provider may have processed a request whose response never fully
    // arrived: charge the estimate and record it as a gateway failure.
    let failed = |state: &ProxyState,
                  reservation: crate::budget::Reservation,
                  route: Option<&ResolvedRoute>,
                  route_name: String,
                  customer_id: String,
                  requested_model: String,
                  message: &str| {
        let answered_model = requested_model.clone();
        finish_usage(
            state,
            &identity,
            reservation,
            estimated_tokens,
            UsagePublication {
                customer_id,
                route: route.map_or(route_name, |route| route.name.clone()),
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
                prices: route_prices(route),
            },
        );
        error_response(state, StatusCode::BAD_GATEWAY, message)
    };
    let upstream_resp = match result {
        Some(Ok(response)) => response,
        _ => {
            return failed(
                &state,
                reservation,
                selected_route,
                route_name,
                customer_id,
                requested_model,
                "upstream request failed",
            )
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
    let redact =
        state.config.redact_errors && (status.is_client_error() || status.is_server_error());
    // Error bodies are buffered and redacted whole: a credential split
    // across two streamed chunks would escape per-chunk redaction.
    if redact || (!is_sse && !request_stream) {
        let limit = if redact {
            MAX_ERROR_BODY_BYTES
        } else {
            MAX_UPSTREAM_BODY_BYTES
        };
        let raw = match read_bounded(upstream_resp, limit).await {
            Ok(raw) => raw,
            Err(error) => {
                let message = match error {
                    BodyError::Read => "failed to read upstream response",
                    BodyError::TooLarge => "upstream response is too large",
                };
                return failed(
                    &state,
                    reservation,
                    selected_route,
                    route_name,
                    customer_id,
                    requested_model,
                    message,
                );
            }
        };
        let counts = find_usage(&raw).unwrap_or(UsageCounts {
            input_tokens: estimated_tokens,
            output_tokens: raw.len() as u64 / 4,
            estimated: true,
        });
        let answered_model = find_answered_model(&raw).unwrap_or_else(|| requested_model.clone());
        let body = if redact {
            route_redactor(&state.redactor, &route_chain)
                .redact(&String::from_utf8_lossy(&raw))
                .into_bytes()
        } else {
            raw.to_vec()
        };
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
        finish_usage(
            &state,
            &identity,
            reservation,
            estimated_tokens,
            UsagePublication {
                customer_id,
                route: selected_route.map_or_else(String::new, |route| route.name.clone()),
                requested_model,
                answered_model,
                counts,
                status,
                latency_ms: started.elapsed().as_millis() as u64,
                cache_hit: false,
                prices: route_prices(selected_route),
            },
        );
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
    let identity = Arc::clone(&identity);
    let event_bus = state.bus.get().cloned();
    let route = selected_route.map(|r| r.name.clone()).unwrap_or_default();
    let prices = route_prices(selected_route);
    let idle_timeout = state.config.stream_idle_timeout;
    // Set when the client stopped reading: its body then ends in an error
    // instead of a clean end of stream.
    let stalled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let relay_stalled = Arc::clone(&stalled);
    tokio::spawn(async move {
        use futures_util::StreamExt;
        let mut parser = UsageParser::new(is_sse);
        let mut bytes_seen = 0_u64;
        let mut client = Some(sender);
        let relay = |client: &Option<ClientSender>, item: ClientItem| {
            relay_to_client(
                client.clone(),
                item,
                idle_timeout,
                Arc::clone(&relay_stalled),
            )
        };
        loop {
            let chunk = match tokio::time::timeout(idle_timeout, stream.next()).await {
                Ok(Some(Ok(chunk))) => chunk,
                Ok(None) => break,
                Ok(Some(Err(error))) => {
                    relay(&client, Err(std::io::Error::other(error.to_string()))).await;
                    break;
                }
                Err(_) => {
                    relay(
                        &client,
                        Err(std::io::Error::other("upstream stream idle timeout")),
                    )
                    .await;
                    break;
                }
            };
            bytes_seen = bytes_seen.saturating_add(chunk.len() as u64);
            parser.push(&chunk);
            // A client that hung up or stalled stops receiving, but the
            // upstream is drained so its reported usage is still recorded.
            if !relay(&client, Ok(chunk)).await {
                client = None;
            }
        }
        // End the client body before waiting on usage publication.
        drop(client);
        let (mut counts, answered_model) = parser.finish_with_model(bytes_seen);
        let answered_model = answered_model.unwrap_or_else(|| requested_model.clone());
        // A partial provider count keeps its known input.
        if counts.estimated && counts.input_tokens == 0 {
            counts.input_tokens = estimated_tokens;
        }
        let charged = charged_tokens(&counts, estimated_tokens);
        if !reservation.settle(charged) {
            tracing::warn!(
                workload = identity.workload_id.as_str(),
                charged,
                "egress usage took the workload past its token budget"
            );
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
    // A stalled client that resumes reads what was queued, then an error,
    // so it never mistakes a cut-off body for a complete one.
    use futures_util::StreamExt as _;
    let cut_off = futures_util::stream::once(async move {
        stalled
            .load(std::sync::atomic::Ordering::SeqCst)
            .then(|| Err(std::io::Error::other("client stopped reading the stream")))
    })
    .filter_map(std::future::ready);
    response
        .body(Body::from_stream(
            tokio_stream::wrappers::ReceiverStream::new(receiver).chain(cut_off),
        ))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

type ClientItem = Result<bytes::Bytes, std::io::Error>;
type ClientSender = tokio::sync::mpsc::Sender<ClientItem>;

/// Hands one item to the client body within `deadline`, so a client that
/// stays connected but stops reading cannot hold the relay, and with it the
/// reservation. Returns false when the client hung up or stalled; a stall
/// also sets `stalled`, and the caller then drains the upstream, settles
/// and publishes as for a client that hung up.
async fn relay_to_client(
    client: Option<ClientSender>,
    item: ClientItem,
    deadline: Duration,
    stalled: Arc<std::sync::atomic::AtomicBool>,
) -> bool {
    let Some(client) = client else {
        return false;
    };
    match tokio::time::timeout(deadline, client.send(item)).await {
        Ok(result) => result.is_ok(),
        Err(_) => {
            stalled.store(true, std::sync::atomic::Ordering::SeqCst);
            false
        }
    }
}

/// The base redactor plus every credential of the request's route chain.
fn route_redactor(base: &SecretRedactor, route_chain: &[&ResolvedRoute]) -> SecretRedactor {
    let mut redactor = base.clone();
    for route in route_chain {
        match &route.auth {
            None => {}
            Some(ResolvedAuth::Bearer(secret) | ResolvedAuth::ApiKey(secret)) => {
                redactor = redactor.with_literal("route-credential", secret)
            }
            Some(ResolvedAuth::AwsSigv4 {
                access_key,
                secret_key,
                session_token,
                ..
            }) => {
                redactor = redactor
                    .with_literal("route-access-key", access_key)
                    .with_literal("route-secret-key", secret_key);
                if let Some(secret) = session_token {
                    redactor = redactor.with_literal("route-session-token", secret);
                }
            }
        }
    }
    redactor
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
    // The request ID is the JetStream dedup key, so a retry after a lost
    // acknowledgement is not counted twice.
    for attempt in 1..=USAGE_PUBLISH_ATTEMPTS {
        match tokio::time::timeout(USAGE_PUBLISH_TIMEOUT, bus.publish_usage(&event)).await {
            Ok(Ok(())) => return,
            Ok(Err(error)) => {
                tracing::warn!(attempt, error = %error, "failed to publish egress usage event")
            }
            Err(_) => tracing::warn!(attempt, "egress usage event publish timed out"),
        }
    }
    minga_telemetry::METRICS
        .agent_usage_events_dropped_total
        .with_label_values(&[&event.tenant_id])
        .inc();
    tracing::error!(tenant = %event.tenant_id, "dropped an egress usage event after its publish retries");
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

/// Which top-level JSON field a [`TopLevelFields`] capture belongs to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Field {
    Usage,
    Model,
}

/// Streams JSON objects byte by byte and keeps the raw value of each
/// object's top-level `usage` and `model` fields, so a `"usage"` inside a
/// string or a nested object is never mistaken for the provider's counts.
/// State stays bounded: a value longer than [`MAX_USAGE_EVENT_BYTES`] is
/// dropped, whatever the transport's chunk sizes.
#[derive(Default)]
struct TopLevelFields {
    depth: u32,
    in_string: bool,
    escaped: bool,
    expect_key: bool,
    reading_key: bool,
    key: Vec<u8>,
    pending: Option<Field>,
    capture: Option<Field>,
    value: Vec<u8>,
    value_overflow: bool,
    usage: Option<Vec<u8>>,
    model: Option<Vec<u8>>,
}

impl TopLevelFields {
    fn push(&mut self, chunk: &[u8]) {
        for &byte in chunk {
            self.push_byte(byte);
        }
    }

    fn capture_byte(&mut self, byte: u8) {
        if self.value.len() == MAX_USAGE_EVENT_BYTES {
            self.value_overflow = true;
            self.value.clear();
        } else if !self.value_overflow {
            self.value.push(byte);
        }
    }

    fn key_byte(&mut self, byte: u8) {
        // A five-character key takes at most 30 escaped bytes (`\u0075`
        // each), so anything longer is never `usage` or `model`.
        if self.key.len() <= 30 {
            self.key.push(byte);
        }
    }

    fn push_byte(&mut self, byte: u8) {
        if self.in_string {
            if self.capture.is_some() {
                self.capture_byte(byte);
            }
            if self.escaped {
                self.escaped = false;
                if self.reading_key {
                    self.key_byte(byte);
                }
            } else if byte == b'\\' {
                self.escaped = true;
                if self.reading_key {
                    self.key_byte(byte);
                }
            } else if byte == b'"' {
                self.in_string = false;
                if self.reading_key {
                    self.reading_key = false;
                    // Match the decoded key, as the buffered path does.
                    let quoted = [b"\"", self.key.as_slice(), b"\""].concat();
                    self.pending = match serde_json::from_slice::<String>(&quoted).as_deref() {
                        Ok("usage") => Some(Field::Usage),
                        Ok("model") => Some(Field::Model),
                        _ => None,
                    };
                    self.key.clear();
                }
            } else if self.reading_key {
                self.key_byte(byte);
            }
            return;
        }
        if let Some(field) = self.capture {
            if self.depth == 1 && matches!(byte, b',' | b'}') {
                let value = std::mem::take(&mut self.value);
                if !std::mem::take(&mut self.value_overflow) {
                    match field {
                        Field::Usage => self.usage = Some(value),
                        Field::Model => self.model = Some(value),
                    }
                }
                self.capture = None;
            } else {
                self.capture_byte(byte);
                match byte {
                    b'"' => self.in_string = true,
                    b'{' | b'[' => self.depth += 1,
                    b'}' | b']' => self.depth = self.depth.saturating_sub(1),
                    _ => {}
                }
                return;
            }
        }
        match byte {
            b'"' => {
                self.in_string = true;
                if self.depth == 1 && self.expect_key {
                    self.expect_key = false;
                    self.reading_key = true;
                }
            }
            b'{' => {
                self.depth += 1;
                self.expect_key = self.depth == 1;
            }
            b'[' => self.depth += 1,
            b'}' | b']' => self.depth = self.depth.saturating_sub(1),
            b',' if self.depth == 1 => {
                self.expect_key = true;
                self.pending = None;
            }
            b':' if self.depth == 1 => self.capture = self.pending.take(),
            _ => {}
        }
    }
}

struct UsageParser {
    is_sse: bool,
    line: Vec<u8>,
    event: Vec<u8>,
    line_overflow: bool,
    discard_event: bool,
    fields: TopLevelFields,
    answered_model: Option<String>,
    usage: Option<UsageCounts>,
    /// Whether the output count came from a final usage report rather than
    /// Anthropic's initial `message_start` count.
    output_final: bool,
}

impl UsageParser {
    fn new(is_sse: bool) -> Self {
        Self {
            is_sse,
            line: Vec::new(),
            event: Vec::new(),
            line_overflow: false,
            discard_event: false,
            fields: TopLevelFields::default(),
            answered_model: None,
            usage: None,
            output_final: false,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        if !self.is_sse {
            self.fields.push(chunk);
            if let Some(raw) = self.fields.usage.take() {
                let value = serde_json::from_slice::<serde_json::Value>(&raw).ok();
                if let Some(usage) = usage_counts(value.as_ref()) {
                    self.record_usage(usage, true);
                }
            }
            if let Some(raw) = self.fields.model.take() {
                if let Some(model) = serde_json::from_slice::<String>(&raw)
                    .ok()
                    .filter(|model| reported_model_fits(model))
                {
                    self.answered_model = Some(model);
                }
            }
            return;
        }
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
    }

    fn record_usage(&mut self, next: UsageCounts, output_final: bool) {
        self.output_final |= output_final;
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
                if let Some((usage, output_final)) = find_event_usage(&self.event) {
                    self.record_usage(usage, output_final);
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
                if let Some((usage, output_final)) = find_event_usage(&self.event) {
                    self.record_usage(usage, output_final);
                }
            }
        }
        let usage = match self.usage {
            Some(usage) if self.output_final => usage,
            // Cut before the final count: the input is known, the output is
            // at least what the response bytes suggest.
            Some(usage) => UsageCounts {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens.max(response_bytes / 4),
                estimated: true,
            },
            None => UsageCounts {
                input_tokens: 0,
                output_tokens: response_bytes / 4,
                estimated: true,
            },
        };
        (usage, self.answered_model)
    }
}

fn find_usage(bytes: &[u8]) -> Option<UsageCounts> {
    find_event_usage(bytes).map(|(usage, _)| usage)
}

/// Usage of one JSON document and whether its output count is final.
/// Anthropic's `message_start` nests an initial count under `message`; a
/// later top-level `usage` (`message_delta`) carries the final output.
fn find_event_usage(bytes: &[u8]) -> Option<(UsageCounts, bool)> {
    let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    if let Some(usage) = value.get("usage") {
        // An input-only report leaves the output open; an explicit 0 is final.
        let output_final = ["completion_tokens", "output_tokens"]
            .iter()
            .any(|key| usage.get(key).is_some_and(serde_json::Value::is_u64));
        return usage_counts(Some(usage)).map(|usage| (usage, output_final));
    }
    let usage = value.get("message")?.get("usage")?;
    usage_counts(Some(usage)).map(|usage| (usage, false))
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
        .filter(|model| reported_model_fits(model))
        .map(str::to_owned)
}

/// A provider-reported model is kept for accounting only within the request
/// identifier limit; otherwise the requested model stands in for it.
fn reported_model_fits(model: &str) -> bool {
    !model.is_empty() && model.len() <= MAX_IDENTIFIER_BYTES
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
        // Test upstreams listen on loopback, which routes refuse by default.
        let config = EgressProxyConfig {
            allowed_route_cidrs: vec!["127.0.0.0/8".parse().unwrap()],
            ..Default::default()
        };
        Arc::new(ProxyState {
            route_client: route_client(&config),
            config,
            budgets,
            cache: Some(Arc::new(ResponseCache::new(CacheConfig::default()))),
            redactor: SecretRedactor::with_defaults(),
            client: reqwest::Client::new(),
            tokens: Arc::new(SandboxTokens::new()),
            bus: std::sync::OnceLock::new(),
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

    #[tokio::test]
    async fn a_usage_event_dropped_after_its_retries_is_counted() {
        use crate::routes::WorkloadIdentity;
        let dropped = || {
            minga_telemetry::METRICS
                .agent_usage_events_dropped_total
                .with_label_values(&["drop-test"])
                .get()
        };
        let before = dropped();
        let publish = |service_id: &str| {
            publish_event(
                Some(minga_bus::MemoryBus::new() as Arc<dyn EventBus>),
                Arc::new(WorkloadIdentity {
                    workload_id: "task-a".into(),
                    tenant_id: "drop-test".into(),
                    service_id: service_id.into(),
                    policy: None,
                }),
                UsagePublication {
                    customer_id: String::new(),
                    route: "route-a".into(),
                    requested_model: "model-a".into(),
                    answered_model: "model-a".into(),
                    counts: UsageCounts {
                        input_tokens: 1,
                        output_tokens: 1,
                        estimated: false,
                    },
                    status: StatusCode::OK,
                    latency_ms: 1,
                    cache_hit: false,
                    prices: None,
                },
            )
        };
        publish("service-a").await;
        assert_eq!(dropped(), before, "a published event is not dropped");
        // The memory bus refuses an event without a service on every attempt.
        publish("").await;
        assert_eq!(dropped(), before + 1.0);
    }

    #[test]
    fn parses_openai_and_anthropic_json_usage() {
        let openai = br#"{"usage":{"prompt_tokens":12,"completion_tokens":7,"total_tokens":19}}"#;
        assert_eq!(
            find_usage(openai),
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
    fn an_anthropic_stream_cut_before_the_final_usage_estimates_the_output() {
        let events = concat!(
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":18,\"output_tokens\":0}}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"a generated answer that the provider charged for\"}}\n\n",
        );
        let mut parser = UsageParser::new(true);
        parser.push(events.as_bytes());
        let counts = parser.finish_with_model(events.len() as u64).0;
        assert_eq!(
            counts,
            UsageCounts {
                input_tokens: 18,
                output_tokens: events.len() as u64 / 4,
                estimated: true,
            }
        );
        assert_eq!(
            charged_tokens(&counts, 999),
            18 + events.len() as u64 / 4,
            "the known input is kept"
        );
    }

    #[test]
    fn a_top_level_input_only_usage_does_not_finalize_the_output() {
        let events = concat!(
            "data: {\"usage\":{\"input_tokens\":18}}\n\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"a generated answer that the provider charged for\"}}\n\n",
        );
        let mut parser = UsageParser::new(true);
        parser.push(events.as_bytes());
        assert_eq!(
            parser.finish_with_model(events.len() as u64).0,
            UsageCounts {
                input_tokens: 18,
                output_tokens: events.len() as u64 / 4,
                estimated: true,
            }
        );
        let explicit_zero = "data: {\"usage\":{\"input_tokens\":18,\"output_tokens\":0}}\n\n";
        let mut parser = UsageParser::new(true);
        parser.push(explicit_zero.as_bytes());
        assert_eq!(
            parser.finish_with_model(explicit_zero.len() as u64).0,
            UsageCounts {
                input_tokens: 18,
                output_tokens: 0,
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
    fn json_usage_parser_drops_an_oversized_usage_value_and_falls_back() {
        let mut parser = UsageParser::new(false);
        parser.push(br#"{"usage":{"#);
        let oversized = vec![b' '; MAX_USAGE_EVENT_BYTES + 1];
        parser.push(&oversized);
        parser.push(br#""prompt_tokens":1}}"#);
        assert!(parser.fields.value.len() <= MAX_USAGE_EVENT_BYTES);
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
    fn json_usage_parser_reads_only_the_top_level_field_in_any_chunking() {
        // A choice whose text is "usage" and a nested usage object come first.
        let filler = "x".repeat(MAX_USAGE_EVENT_BYTES * 2);
        let body = format!(
            r#"{{"choices":[{{"text":"\"usage\":{{\"prompt_tokens\":99}}","usage":{{"prompt_tokens":98}}}},{{"text":"{filler}"}}],"model":"m-1","usage":{{"prompt_tokens":12,"completion_tokens":7}}}}"#
        );
        let want = UsageCounts {
            input_tokens: 12,
            output_tokens: 7,
            estimated: false,
        };
        for chunk in [1, 7, body.len()] {
            let mut parser = UsageParser::new(false);
            for part in body.as_bytes().chunks(chunk) {
                parser.push(part);
            }
            let (counts, model) = parser.finish_with_model(body.len() as u64);
            assert_eq!(counts, want, "chunk size {chunk}");
            assert_eq!(model.as_deref(), Some("m-1"));
        }
        assert_eq!(find_usage(body.as_bytes()), Some(want));
    }

    #[test]
    fn json_usage_parser_matches_escaped_top_level_keys() {
        let body =
            br#"{"\u006dodel":"m-2","us\u0061ge":{"prompt_tokens":3,"completion_tokens":4}}"#;
        for chunk in [1, body.len()] {
            let mut parser = UsageParser::new(false);
            for part in body.chunks(chunk) {
                parser.push(part);
            }
            let (counts, model) = parser.finish_with_model(body.len() as u64);
            assert_eq!(
                counts,
                UsageCounts {
                    input_tokens: 3,
                    output_tokens: 4,
                    estimated: false,
                },
                "chunk size {chunk}"
            );
            assert_eq!(model.as_deref(), Some("m-2"));
        }
        assert_eq!(
            find_usage(body),
            find_usage(br#"{"usage":{"prompt_tokens":3,"completion_tokens":4}}"#)
        );
    }

    #[test]
    fn oversized_reported_models_fall_back_to_the_requested_one() {
        let long = "m".repeat(MAX_IDENTIFIER_BYTES + 1);
        let body = format!(r#"{{"model":"{long}"}}"#);
        assert_eq!(find_answered_model(body.as_bytes()), None);
        let mut parser = UsageParser::new(false);
        parser.push(body.as_bytes());
        assert_eq!(parser.finish_with_model(body.len() as u64).1, None);
        let fits = "m".repeat(MAX_IDENTIFIER_BYTES);
        assert_eq!(
            find_answered_model(format!(r#"{{"model":"{fits}"}}"#).as_bytes()),
            Some(fits)
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

        // The second chunk waits for the test, so a proxy that buffers the
        // body before sending headers never answers within the deadline.
        let release = Arc::new(tokio::sync::Notify::new());
        let upstream_release = Arc::clone(&release);
        let app = Router::new().route(
            "/*path",
            any(move || {
                let release = Arc::clone(&upstream_release);
                async move {
                    let (sender, receiver) =
                        tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(2);
                    tokio::spawn(async move {
                        let _ = sender.send(Ok(Bytes::from_static(b"first\n"))).await;
                        release.notified().await;
                        let _ = sender.send(Ok(Bytes::from_static(b"second\n"))).await;
                    });
                    Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(AxumBody::from_stream(
                            tokio_stream::wrappers::ReceiverStream::new(receiver),
                        ))
                        .unwrap()
                }
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
        let token = tokens.issue_standalone("stream-task");
        let mut proxy = EgressProxy::new(
            EgressProxyConfig {
                upstream_url: format!("http://{upstream_addr}"),
                ..Default::default()
            },
            budgets,
            tokens,
        );
        let address = proxy.start("127.0.0.1:0").await.unwrap();
        let (mut body, first) = tokio::time::timeout(Duration::from_secs(5), async {
            let response = reqwest::Client::new()
                .post(format!("http://{address}/v1/chat"))
                .header(SANDBOX_TOKEN_HEADER, token.as_str())
                .body("{}")
                .send()
                .await
                .unwrap();
            let mut body = response.bytes_stream();
            let first = body.next().await.unwrap().unwrap();
            (body, first)
        })
        .await
        .expect("headers and the first chunk should not wait for upstream completion");
        assert_eq!(first.as_ref(), b"first\n");
        release.notify_one();
        while body.next().await.is_some() {}
        assert_eq!(proxy.cache_stats().unwrap().stores, 0, "not cached at EOF");
        proxy.shutdown();
    }

    #[tokio::test]
    async fn a_client_that_stops_reading_releases_the_reservation() {
        use bytes::Bytes;
        let app = Router::new().route(
            "/*path",
            any(|| async {
                let chunks = (0..64).map(|_| Ok::<_, std::io::Error>(Bytes::from(vec![b'x'; 64])));
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(futures_util::stream::iter(chunks)))
                    .unwrap()
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });
        let budgets = Arc::new(BudgetTracker::new());
        budgets.register("stalled", Budget::new(1_000_000, Duration::from_secs(3600)));
        let config = EgressProxyConfig {
            upstream_url: format!("http://{address}"),
            stream_idle_timeout: Duration::from_millis(200),
            ..Default::default()
        };
        let tokens = Arc::new(SandboxTokens::new());
        let token = tokens.issue_standalone("stalled");
        let state = Arc::new(ProxyState {
            route_client: route_client(&config),
            config,
            budgets: Arc::clone(&budgets),
            cache: None,
            redactor: SecretRedactor::with_defaults(),
            client: reqwest::Client::new(),
            tokens,
            bus: std::sync::OnceLock::new(),
        });
        // No output cap: the request holds the whole balance until it settles.
        let response = proxy_handler(State(state), post(token.as_str(), "{}")).await;
        assert_eq!(response.status(), StatusCode::OK);
        // The body is never read, so the relay's queue fills and stalls.
        let settled = tokio::time::timeout(Duration::from_secs(5), async {
            while budgets.status("stalled").unwrap().used >= 1_000_000 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        assert!(
            settled.is_ok(),
            "a stalled client still holds the reservation"
        );
        assert!(
            axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .is_err(),
            "a cut-off body ends in an error, not a clean end of stream"
        );
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
            route_client: None,
            tokens: Arc::new(SandboxTokens::new()),
            bus: std::sync::OnceLock::new(),
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
        let token_b = tokens.issue_standalone("task-b");

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
            let token = tokens.issue_standalone("task-a");
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
        let token = tokens.issue_standalone("task-a");
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

    fn route_policy(name: &str, upstream: String) -> crate::routes::ResolvedEgressPolicy {
        let mut policy = crate::routes::ResolvedEgressPolicy::default();
        policy.routes.insert(
            name.into(),
            ResolvedRoute {
                name: name.into(),
                upstream_url: upstream,
                auth: None,
                fallback: Vec::new(),
                input_price_micros_per_million: None,
                output_price_micros_per_million: None,
            },
        );
        policy
    }

    fn routed_token(
        state: &Arc<ProxyState>,
        workload: &str,
        policy: crate::routes::ResolvedEgressPolicy,
    ) -> crate::identity::SandboxToken {
        state.tokens.issue_identity(WorkloadIdentity {
            workload_id: workload.into(),
            tenant_id: "default".into(),
            service_id: workload.into(),
            policy: Some(policy),
        })
    }

    fn post(token: &str, body: impl Into<String>) -> Request {
        Request::builder()
            .method("POST")
            .uri("/v1/chat")
            .header(SANDBOX_TOKEN_HEADER, token)
            .body(Body::from(body.into()))
            .unwrap()
    }

    #[tokio::test]
    async fn routes_refuse_private_destinations_and_redirects_unless_allowed() {
        let app = Router::new()
            .route(
                "/v1/redirect",
                any(|| async {
                    Response::builder()
                        .status(StatusCode::FOUND)
                        .header("location", "http://169.254.169.254/latest")
                        .body(Body::empty())
                        .unwrap()
                }),
            )
            .route("/v1/chat", any(|| async { "ok" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });

        // Default config: a literal loopback upstream is refused.
        let mut denied = test_state();
        Arc::get_mut(&mut denied)
            .unwrap()
            .config
            .allowed_route_cidrs
            .clear();
        let token = routed_token(
            &denied,
            "test-agent",
            route_policy("primary", format!("http://{address}")),
        );
        assert_eq!(
            proxy_handler(State(Arc::clone(&denied)), post(token.as_str(), "{}"))
                .await
                .status(),
            StatusCode::FORBIDDEN
        );
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "169.254.169.254",
            "0.0.0.0",
            "0.1.2.3",
            "100.64.0.1",
            "100.127.255.254",
            "224.0.0.1",
        ] {
            assert!(!destination_allowed(ip.parse().unwrap(), &[]), "{ip}");
        }
        assert!(destination_allowed("100.128.0.1".parse().unwrap(), &[]));
        for ip in [
            "::1",
            "fe80::1",
            "fd00::1",
            "::ffff:127.0.0.1",
            "64:ff9b::7f00:1",
            "64:ff9b::a9fe:a9fe",
        ] {
            assert!(!destination_allowed(ip.parse().unwrap(), &[]), "{ip}");
        }
        assert!(destination_allowed("8.8.8.8".parse().unwrap(), &[]));

        // An allowlisted CIDR works, and a redirect is returned, not followed.
        let allowed = test_state();
        let token = routed_token(
            &allowed,
            "test-agent",
            route_policy("primary", format!("http://{address}")),
        );
        let response = proxy_handler(State(Arc::clone(&allowed)), post(token.as_str(), "{}")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let redirect = Request::builder()
            .method("POST")
            .uri("/v1/redirect")
            .header(SANDBOX_TOKEN_HEADER, token.as_str())
            .body(Body::from("{}"))
            .unwrap();
        assert_eq!(
            proxy_handler(State(allowed), redirect).await.status(),
            StatusCode::FOUND
        );
    }

    #[tokio::test]
    async fn concurrent_requests_cannot_spend_the_same_budget() {
        // An output cap, no output cap at all, an input that a bytes/4
        // estimate would undercount (two 16-token holds fit in 100; two
        // 61-byte bounds do not), and `n` choices (two 23 + 60 holds do not
        // fit; without `n` two 23 + 20 holds would).
        for body in [
            r#"{"max_tokens":60}"#.to_string(),
            "{}".to_string(),
            format!(r#"{{"max_tokens":1,"p":"{}"}}"#, "x".repeat(37)),
            // Output is billed per choice: 3 x 20 plus the input must fit.
            r#"{"max_tokens":20,"n":3}"#.to_string(),
            // An empty body bounds its input at zero but still needs headroom.
            String::new(),
        ] {
            concurrent_requests_cannot_spend_the_same_budget_with(&body).await;
        }
    }

    async fn concurrent_requests_cannot_spend_the_same_budget_with(body: &str) {
        let (release, released) = tokio::sync::watch::channel(false);
        let (arrived_tx, mut arrived) = tokio::sync::mpsc::channel::<()>(4);
        let app = Router::new().route(
            "/*path",
            any(move || {
                let mut released = released.clone();
                let arrived = arrived_tx.clone();
                async move {
                    let _ = arrived.send(()).await;
                    let _ = released.wait_for(|go| *go).await;
                    r#"{"usage":{"prompt_tokens":3,"completion_tokens":2}}"#
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });

        let state = test_state();
        state
            .budgets
            .register("pair", Budget::new(100, Duration::from_secs(3600)));
        let token = routed_token(
            &state,
            "pair",
            route_policy("primary", format!("http://{address}")),
        );
        let first = tokio::spawn(proxy_handler(
            State(Arc::clone(&state)),
            post(token.as_str(), body),
        ));
        arrived.recv().await.unwrap();
        let second = proxy_handler(State(Arc::clone(&state)), post(token.as_str(), body)).await;
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        release.send(true).unwrap();
        assert_eq!(first.await.unwrap().status(), StatusCode::OK);
        // The reservation settled to the provider's counts.
        assert_eq!(state.budgets.status("pair").unwrap().used, 5);
    }

    #[tokio::test]
    async fn upstream_error_streams_are_buffered_and_redacted_whole() {
        let key = "route-key-Zq9";
        let app = Router::new().route(
            "/*path",
            any(move || async move {
                // The key straddles two chunks of an event stream.
                let (sender, receiver) =
                    tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(2);
                tokio::spawn(async move {
                    let _ = sender
                        .send(Ok(bytes::Bytes::from(format!("bad {}", &key[..5]))))
                        .await;
                    let _ = sender
                        .send(Ok(bytes::Bytes::from(key[5..].to_string())))
                        .await;
                });
                Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .header("content-type", "text/event-stream")
                    .body(Body::from_stream(
                        tokio_stream::wrappers::ReceiverStream::new(receiver),
                    ))
                    .unwrap()
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });
        let state = test_state();
        let mut policy = route_policy("primary", format!("http://{address}"));
        policy.routes.get_mut("primary").unwrap().auth = Some(ResolvedAuth::ApiKey(key.into()));
        let token = routed_token(&state, "test-agent", policy);
        let response =
            proxy_handler(State(state), post(token.as_str(), r#"{"stream":true}"#)).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(!body.contains(key), "key leaked: {body}");
        assert!(body.contains("***REDACTED***"), "{body}");
    }

    #[tokio::test]
    async fn oversized_identifiers_are_rejected() {
        let state = test_state();
        let token = routed_token(
            &state,
            "test-agent",
            route_policy("primary", "http://127.0.0.1:1".into()),
        );
        let long = "m".repeat(257);
        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat")
            .header(SANDBOX_TOKEN_HEADER, token.as_str())
            .header("x-minga-customer", long.as_str())
            .body(Body::from("{}"))
            .unwrap();
        assert_eq!(
            proxy_handler(State(Arc::clone(&state)), request)
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
        let body: &'static str = Box::leak(format!(r#"{{"model":"{long}"}}"#).into_boxed_str());
        assert_eq!(
            proxy_handler(State(state), post(token.as_str(), body))
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn a_changed_route_does_not_hit_the_old_cache_entry() {
        let mut addresses = Vec::new();
        for answer in ["old provider", "new provider"] {
            let app = Router::new().route("/*path", any(move || async move { answer }));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.push(listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.ok() });
        }
        let state = test_state();
        for (address, want) in addresses.iter().zip(["old provider", "new provider"]) {
            // Same task ID and route name, restarted with a new upstream.
            let token = routed_token(
                &state,
                "test-agent",
                route_policy("primary", format!("http://{address}")),
            );
            let response =
                proxy_handler(State(Arc::clone(&state)), post(token.as_str(), "{}")).await;
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(&body[..], want.as_bytes());
        }
    }

    #[tokio::test]
    async fn forwarded_variant_headers_are_part_of_the_cache_key() {
        use axum::http::HeaderMap;
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let upstream_hits = Arc::clone(&hits);
        let app = Router::new().route(
            "/*path",
            any(move |headers: HeaderMap| {
                let hits = Arc::clone(&upstream_hits);
                async move {
                    hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let version = headers
                        .get("anthropic-version")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_owned();
                    format!("version {version}")
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });
        let state = test_state();
        let token = routed_token(
            &state,
            "test-agent",
            route_policy("primary", format!("http://{address}")),
        );
        for (version, want) in [
            ("2023-06-01", "version 2023-06-01"),
            ("2024-01-01", "version 2024-01-01"),
            ("2023-06-01", "version 2023-06-01"),
        ] {
            let mut request = post(token.as_str(), "{}");
            request
                .headers_mut()
                .insert("anthropic-version", version.parse().unwrap());
            let response = proxy_handler(State(Arc::clone(&state)), request).await;
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(&body[..], want.as_bytes());
        }
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "a repeated variant is still a cache hit"
        );
    }

    #[tokio::test]
    async fn customers_do_not_share_cached_responses() {
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let upstream_hits = Arc::clone(&hits);
        let app = Router::new().route(
            "/*path",
            any(move || {
                let hits = Arc::clone(&upstream_hits);
                async move {
                    let n = hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    format!("answer {n}")
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });
        let state = test_state();
        let token = routed_token(
            &state,
            "test-agent",
            route_policy("primary", format!("http://{address}")),
        );
        for (customer, want) in [
            ("alice", "answer 1"),
            ("bob", "answer 2"),
            ("alice", "answer 1"),
        ] {
            let mut request = post(token.as_str(), "{}");
            request
                .headers_mut()
                .insert("x-minga-customer", customer.parse().unwrap());
            let response = proxy_handler(State(Arc::clone(&state)), request).await;
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert_eq!(&body[..], want.as_bytes(), "customer {customer}");
        }
    }

    #[tokio::test]
    async fn registered_routed_task_first_request_is_admitted() {
        let app = Router::new().route("/*path", any(|| async { "ok" }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.ok() });
        let state = test_state();
        let token = routed_token(
            &state,
            "fresh-task",
            route_policy("primary", format!("http://{address}")),
        );
        assert_eq!(
            proxy_handler(State(Arc::clone(&state)), post(token.as_str(), "{}"))
                .await
                .status(),
            StatusCode::TOO_MANY_REQUESTS,
            "an unregistered workload has no budget"
        );
        // What the node runtime registers before a routed task starts.
        state.budgets.register(
            "fresh-task",
            Budget::new(u64::MAX, Duration::from_secs(3600)),
        );
        assert_eq!(
            proxy_handler(State(state), post(token.as_str(), "{}"))
                .await
                .status(),
            StatusCode::OK
        );
    }
}
