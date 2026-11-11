//! HTTP client for the Minga Docker Engine API.
//!
//! This is the direct transport (plan section 2.2, item 4): `http://` to a
//! loopback address, or `https://` with an optional pinned CA. Contexts with
//! an SSH target need the tunnel, which is build-order step 3.

use std::time::Duration;

use serde_json::Value;

use crate::config::{self, Resolved};
use crate::error::CtlError;

/// API version segment Minga serves.
const API_VERSION: &str = "v1.41";

/// The message Minga's fallback route returns for an unregistered path.
const NOT_IMPLEMENTED: &str = "endpoint not implemented by Minga";

/// A client bound to one context's API endpoint.
pub struct ApiClient {
    http: reqwest::Client,
    /// Endpoint without a trailing slash, e.g. `http://127.0.0.1:2375`.
    root: String,
    /// Context name, for not-found errors.
    pub context: String,
    token: Option<String>,
    debug: bool,
}

/// The outcome of a GET that may name something that does not exist.
pub enum Found {
    Yes(Value),
    No,
}

impl ApiClient {
    /// Builds a client for a resolved context over the direct transport.
    pub fn direct(resolved: &Resolved, debug: bool) -> Result<Self, CtlError> {
        if let Some(ssh) = &resolved.context.ssh {
            return Err(CtlError::Usage(format!(
                "context `{}` reaches the API over ssh ({ssh}); the SSH tunnel is not built yet",
                resolved.name
            )));
        }
        config::check_endpoint_policy(None, &resolved.endpoint)?;

        let mut builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(path) = &resolved.context.tls_cert {
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
            root: resolved.endpoint.trim_end_matches('/').to_string(),
            context: resolved.name.clone(),
            token: resolved.token.clone(),
            debug,
        })
    }

    /// `GET /v1.41/<path>` with optional Docker `filters`.
    pub async fn get(&self, path: &str, filters: Option<&Value>) -> Result<Value, CtlError> {
        match self.get_found(path, filters).await? {
            Found::Yes(value) => Ok(value),
            Found::No => Err(CtlError::Api {
                status: 404,
                message: format!("/{API_VERSION}/{path} not found"),
            }),
        }
    }

    /// Like [`ApiClient::get`], but a plain 404 is [`Found::No`] instead of an
    /// error, so the caller can name what was not found (exit 3).
    pub async fn get_found(&self, path: &str, filters: Option<&Value>) -> Result<Found, CtlError> {
        let url = format!("{}/{API_VERSION}/{path}", self.root);
        let mut request = self.http.get(&url);
        if let Some(filters) = filters {
            request = request.query(&[("filters", filters.to_string())]);
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
            eprintln!("debug: GET /{API_VERSION}/{path} -> {status}");
        }
        let body = response.text().await.map_err(|e| CtlError::Unreachable {
            endpoint: self.root.clone(),
            detail: reqwest_detail(&e),
        })?;
        match status {
            200..=299 => serde_json::from_str(&body)
                .map(Found::Yes)
                .map_err(|e| CtlError::Api {
                    status,
                    message: format!("response is not JSON: {e}"),
                }),
            401 => Err(CtlError::Auth {
                endpoint: self.root.clone(),
                message: error_message(&body),
            }),
            404 if error_message(&body) != NOT_IMPLEMENTED => Ok(Found::No),
            _ => Err(CtlError::Api {
                status,
                message: error_message(&body),
            }),
        }
    }
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
    fn ids_for_url_paths_are_checked() {
        assert!(check_id("service", "shop.web").is_ok());
        assert!(check_id("service", "shop_db-1").is_ok());
        for bad in ["", "../x", "-x", "a/b", "a b", "a?b", ".hidden"] {
            assert!(check_id("service", bad).is_err(), "{bad:?}");
        }
    }
}
