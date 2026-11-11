//! HTTP client for the Minga Docker Engine API.
//!
//! The client only knows a root URL and a token. How that root is reached
//! (directly, or through an SSH tunnel) is decided in `transport.rs`.

use std::path::Path;
use std::time::Duration;

use reqwest::Method;
use serde_json::Value;

use crate::error::CtlError;

/// API version segment Minga serves.
const API_VERSION: &str = "v1.41";

/// The message Minga's fallback route returns for an unregistered path.
const NOT_IMPLEMENTED: &str = "endpoint not implemented by Minga";

/// A client bound to one API root URL.
pub struct ApiClient {
    http: reqwest::Client,
    /// Root without a trailing slash, e.g. `http://127.0.0.1:2375`.
    root: String,
    /// Context name, for not-found errors.
    pub context: String,
    token: Option<String>,
    debug: bool,
    /// `text()` URLs under this root go through this client instead (the
    /// SSH tunnel's metrics socket).
    text_route: Option<(String, reqwest::Client)>,
}

/// Builder settings every client shares.
fn builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::none())
}

/// The outcome of a request that may name something that does not exist.
pub enum Found {
    Yes(Value),
    No,
}

impl ApiClient {
    /// A client for `root`. `tls_cert`, when set, is the only CA trusted for
    /// an `https://` root. The caller has already applied the endpoint policy.
    pub fn new(
        root: &str,
        token: Option<String>,
        tls_cert: Option<&Path>,
        context: &str,
        debug: bool,
    ) -> Result<Self, CtlError> {
        let mut builder = builder();
        if let Some(path) = tls_cert {
            let pem = std::fs::read(path).map_err(|e| {
                CtlError::Config(format!("cannot read tls_cert {}: {e}", path.display()))
            })?;
            let cert = reqwest::Certificate::from_pem(&pem).map_err(|e| {
                CtlError::Config(format!(
                    "tls_cert {} is not a PEM certificate: {e}",
                    path.display()
                ))
            })?;
            // Pin: trust this CA only, not the system roots.
            builder = builder
                .tls_built_in_root_certs(false)
                .add_root_certificate(cert);
        }
        let http = builder
            .build()
            .map_err(|e| CtlError::Config(format!("cannot build the HTTP client: {e}")))?;
        Ok(ApiClient {
            http,
            root: root.trim_end_matches('/').to_string(),
            context: context.to_string(),
            token,
            debug,
            text_route: None,
        })
    }

    /// A client whose connections all go to the Unix socket `sock` (the SSH
    /// tunnel). `root` is the API URL as seen from the SSH target; it names
    /// the API in messages and the `Host` header, and is never resolved.
    pub fn over_socket(
        sock: &Path,
        root: &str,
        token: Option<String>,
        context: &str,
        debug: bool,
    ) -> Result<Self, CtlError> {
        let http = socket_client(sock)?;
        Ok(ApiClient {
            http,
            root: root.trim_end_matches('/').to_string(),
            context: context.to_string(),
            token,
            debug,
            text_route: None,
        })
    }

    /// Sends [`ApiClient::text`] requests for URLs under `root` through the
    /// Unix socket `sock` (the tunnel's metrics forward).
    pub fn route_text(mut self, root: &str, sock: &Path) -> Result<Self, CtlError> {
        self.text_route = Some((root.trim_end_matches('/').to_string(), socket_client(sock)?));
        Ok(self)
    }

    /// The root URL requests go to.
    pub fn root(&self) -> &str {
        &self.root
    }

    /// `GET /v1.41/<path>` with optional Docker `filters`.
    pub async fn get(&self, path: &str, filters: Option<&Value>) -> Result<Value, CtlError> {
        let found = self.get_found(path, filters).await?;
        expect_found(found, path)
    }

    /// Like [`ApiClient::get`], but a plain 404 is [`Found::No`] instead of an
    /// error, so the caller can name what was not found (exit 3).
    pub async fn get_found(&self, path: &str, filters: Option<&Value>) -> Result<Found, CtlError> {
        let (status, body) = self.send(Method::GET, path, filters, None).await?;
        classify(&self.root, status, &body)
    }

    /// `POST /v1.41/<path>` with a JSON body. An empty response body is `null`.
    pub async fn post(&self, path: &str, body: &Value) -> Result<Value, CtlError> {
        let (status, text) = self.send(Method::POST, path, None, Some(body)).await?;
        expect_found(classify(&self.root, status, &text)?, path)
    }

    /// `DELETE /v1.41/<path>`. A plain 404 is [`Found::No`]; any other error,
    /// such as a 409 for a resource still in use, is returned as `Api`.
    pub async fn delete(&self, path: &str) -> Result<Found, CtlError> {
        let (status, body) = self.send(Method::DELETE, path, None, None).await?;
        classify(&self.root, status, &body)
    }

    /// `GET` of a plain-text URL such as `/healthz`, `/readyz` or `/metrics`,
    /// returning the status and body unchanged. The token is sent only when
    /// `url` is on this client's root, never to another host or port.
    pub async fn text(&self, url: &str) -> Result<(u16, String), CtlError> {
        let http = match &self.text_route {
            Some((root, client)) if on_root(root, url) => client,
            _ => &self.http,
        };
        let mut request = http.get(url);
        if let Some(token) = self.token.as_ref().filter(|_| on_root(&self.root, url)) {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(|e| CtlError::Unreachable {
            endpoint: url.to_string(),
            detail: reqwest_detail(&e),
        })?;
        let status = response.status().as_u16();
        if self.debug {
            eprintln!("debug: GET {url} -> {status}");
        }
        let body = response.text().await.map_err(|e| CtlError::Unreachable {
            endpoint: url.to_string(),
            detail: reqwest_detail(&e),
        })?;
        Ok((status, body))
    }

    /// Sends a versioned API request; returns the status and body.
    async fn send(
        &self,
        method: Method,
        path: &str,
        filters: Option<&Value>,
        body: Option<&Value>,
    ) -> Result<(u16, String), CtlError> {
        let url = format!("{}/{API_VERSION}/{path}", self.root);
        let mut request = self.http.request(method.clone(), &url);
        if let Some(filters) = filters {
            request = request.query(&[("filters", filters.to_string())]);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(|e| CtlError::Unreachable {
            endpoint: self.root.clone(),
            detail: reqwest_detail(&e),
        })?;
        let status = response.status().as_u16();
        if self.debug {
            // Method, path and status only; the Authorization header is never printed.
            eprintln!("debug: {method} /{API_VERSION}/{path} -> {status}");
        }
        let text = response.text().await.map_err(|e| CtlError::Unreachable {
            endpoint: self.root.clone(),
            detail: reqwest_detail(&e),
        })?;
        Ok((status, text))
    }
}

/// Maps a status and body: 2xx is the JSON body (`null` when empty), 401 is
/// `Auth`, a plain 404 is [`Found::No`], anything else is `Api`.
fn classify(root: &str, status: u16, body: &str) -> Result<Found, CtlError> {
    match status {
        200..=299 if body.trim().is_empty() => Ok(Found::Yes(Value::Null)),
        200..=299 => serde_json::from_str(body)
            .map(Found::Yes)
            .map_err(|e| CtlError::Api {
                status,
                message: format!("response is not JSON: {e}"),
            }),
        401 => Err(CtlError::Auth {
            endpoint: root.to_string(),
            message: error_message(body),
        }),
        404 if error_message(body) != NOT_IMPLEMENTED => Ok(Found::No),
        _ => Err(CtlError::Api {
            status,
            message: error_message(body),
        }),
    }
}

/// Turns [`Found::No`] into an exit-2 error for callers that expect the path
/// to exist.
fn expect_found(found: Found, path: &str) -> Result<Value, CtlError> {
    match found {
        Found::Yes(value) => Ok(value),
        Found::No => Err(CtlError::Api {
            status: 404,
            message: format!("/{API_VERSION}/{path} not found"),
        }),
    }
}

/// A client whose connections all go to the Unix socket `sock`.
fn socket_client(sock: &Path) -> Result<reqwest::Client, CtlError> {
    builder()
        .unix_socket(sock)
        .build()
        .map_err(|e| CtlError::Config(format!("cannot build the HTTP client: {e}")))
}

/// Whether `url` is on `root`: same scheme, host and port, then `/` or the end.
fn on_root(root: &str, url: &str) -> bool {
    url.strip_prefix(root)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

/// The `message` of a Docker error body, or the body itself when it is not
/// JSON (axum answers some rejections in plain text).
fn error_message(body: &str) -> String {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v["message"].as_str().map(str::to_string))
        .unwrap_or_else(|| body.trim().to_string())
}

/// The innermost cause of a reqwest error, e.g. `Connection refused (os error 61)`.
fn reqwest_detail(err: &reqwest::Error) -> String {
    let mut source: &dyn std::error::Error = err;
    while let Some(next) = source.source() {
        source = next;
    }
    source.to_string()
}

/// Checks an ID that goes into a URL path: `^[A-Za-z0-9][A-Za-z0-9._-]*$`.
pub fn check_id(kind: &str, id: &str) -> Result<(), CtlError> {
    let mut chars = id.chars();
    let valid = chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if valid {
        Ok(())
    } else {
        Err(CtlError::Usage(format!(
            "invalid {kind} `{id}`: use letters, digits, `.`, `_` and `-`, starting with a letter or digit"
        )))
    }
}

/// Runs an async command body on a single-threaded runtime.
pub fn block_on<T>(
    future: impl std::future::Future<Output = Result<T, CtlError>>,
) -> Result<T, CtlError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| CtlError::Config(format!("cannot start the async runtime: {e}")))?
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_messages_come_from_json_or_plain_text() {
        assert_eq!(
            error_message(r#"{"message":"authentication required"}"#),
            "authentication required"
        );
        assert_eq!(
            error_message("Failed to parse the request body\n"),
            "Failed to parse the request body"
        );
    }

    #[test]
    fn statuses_are_classified() {
        let root = "http://127.0.0.1:2375";
        assert!(matches!(
            classify(root, 204, ""),
            Ok(Found::Yes(Value::Null))
        ));
        assert!(matches!(
            classify(root, 404, r#"{"message":"no such secret"}"#),
            Ok(Found::No)
        ));
        assert!(matches!(
            classify(
                root,
                404,
                r#"{"message":"endpoint not implemented by Minga"}"#
            ),
            Err(CtlError::Api { status: 404, .. })
        ));
        assert!(matches!(
            classify(root, 401, "{}"),
            Err(CtlError::Auth { .. })
        ));
        assert!(matches!(
            classify(root, 409, r#"{"message":"in use"}"#),
            Err(CtlError::Api { status: 409, .. })
        ));
    }

    #[test]
    fn the_token_goes_only_to_the_api_root() {
        let root = "http://127.0.0.1:2375";
        assert!(on_root(root, "http://127.0.0.1:2375/healthz"));
        assert!(on_root(root, "http://127.0.0.1:2375"));
        assert!(!on_root(root, "http://127.0.0.1:23750/metrics"));
        assert!(!on_root(root, "http://127.0.0.1:9100/metrics"));
        assert!(!on_root(root, "http://127.0.0.1:2375.evil.example/metrics"));
    }

    #[test]
    fn ids_for_url_paths_are_checked() {
        assert!(check_id("service", "shop.web").is_ok());
        assert!(check_id("service", "shop_db-1").is_ok());
        for bad in ["", "../x", "-x", "a/b", "a b", "a?b", ".hidden"] {
            assert!(check_id("service", bad).is_err(), "{bad:?}");
        }
    }
}
