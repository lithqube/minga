//! `minga-ctl health` (plan section 5.5).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use clap::Args;
use serde_json::{json, Value};

use crate::api::{self, ApiClient};
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::transport::Connection;
use crate::view;

/// The service label that names an HTTP health URL.
const URL_LABEL: &str = "minga.ctl.health.url";

/// Health arguments.
#[derive(Debug, Args)]
pub struct HealthArgs {
    /// Also check this stack's services and their health URLs.
    #[arg(long)]
    stack: Option<String>,
    /// Seconds each check may take.
    #[arg(long, default_value_t = 5)]
    timeout: u64,
}

/// One row of the health table.
#[derive(Debug, PartialEq, Eq)]
struct Check {
    name: String,
    status: &'static str,
    detail: String,
}

impl Check {
    fn new(name: impl Into<String>, status: &'static str, detail: impl Into<String>) -> Self {
        Check {
            name: name.into(),
            status,
            detail: detail.into(),
        }
    }
}

/// Runs the checks, prints them, and fails (exit 2) when any is not `ok`.
pub async fn run(
    args: &HealthArgs,
    conn: &Connection,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let timeout = Duration::from_secs(args.timeout);
    let mut checks = Vec::new();
    for path in ["healthz", "readyz"] {
        checks.push(api_check(&conn.api, path, timeout).await?);
    }
    if let Some(stack) = &args.stack {
        api::check_id("stack", stack)?;
        checks.extend(stack_checks(&conn.api, stack, timeout).await?);
    }

    let mut rows = Rows::new(&["CHECK", "STATUS", "DETAIL"]);
    for c in &checks {
        rows.push(
            vec![c.name.clone(), c.status.to_string(), c.detail.clone()],
            json!({"check": c.name, "status": c.status, "detail": c.detail}),
        );
    }
    output::print(format, quiet, &rows)?;

    let failed = checks.iter().filter(|c| c.status != "ok").count();
    if failed > 0 {
        return Err(CtlError::HealthFailed { failed });
    }
    Ok(())
}

/// `GET <api root>/healthz` or `/readyz`: ok on 200, else the body is the detail.
async fn api_check(api: &ApiClient, path: &str, timeout: Duration) -> Result<Check, CtlError> {
    let name = format!("api /{path}");
    let url = format!("{}/{path}", api.root());
    let Ok(response) = tokio::time::timeout(timeout, api.text(&url)).await else {
        return Ok(Check::new(
            name,
            "fail",
            format!("no answer in {timeout:?}"),
        ));
    };
    let (status, body) = response?;
    Ok(if status == 200 {
        Check::new(name, "ok", "")
    } else {
        Check::new(name, "fail", format!("{status} {}", body.trim()))
    })
}

/// Per-service replica checks and health-URL checks for one stack.
async fn stack_checks(
    api: &ApiClient,
    stack: &str,
    timeout: Duration,
) -> Result<Vec<Check>, CtlError> {
    let filter = view::stack_filter(stack);
    let services = api.get("services", Some(&filter)).await?;
    if view::items(&services).is_empty() {
        return Err(CtlError::NotFound {
            kind: "stack",
            name: stack.to_string(),
            context: api.context.clone(),
        });
    }
    let tasks = api.get("tasks", Some(&filter)).await?;
    let mut running: HashMap<&str, u64> = HashMap::new();
    for task in view::items(&tasks) {
        if task.pointer("/Status/State").and_then(Value::as_str) == Some("running") {
            *running
                .entry(task["ServiceID"].as_str().unwrap_or(""))
                .or_default() += 1;
        }
    }

    // One plain client for every health URL: no redirects, no credentials.
    let plain = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .build()
        .map_err(|e| CtlError::Config(format!("cannot build the HTTP client: {e}")))?;

    let mut services: Vec<&Value> = view::items(&services).iter().collect();
    services.sort_by(|a, b| view::id(a).cmp(view::id(b)));
    let mut checks = Vec::new();
    for service in services {
        let id = view::id(service);
        let up = running.get(id).copied().unwrap_or(0);
        let (status, detail) = replica_status(view::mode(service).1, up);
        checks.push(Check::new(id, status, detail));
        let label = service
            .pointer("/Spec/Labels")
            .and_then(|l| l.get(URL_LABEL));
        if let Some(url) = label.and_then(Value::as_str) {
            checks.push(url_check(&plain, &format!("{id} url"), url).await);
        }
    }
    Ok(checks)
}

/// Running tasks against desired replicas: equal is `ok`, fewer but some is
/// `degraded`, none is `fail`. Global modes have no desired count; any
/// running task is `ok`.
fn replica_status(desired: Option<u64>, up: u64) -> (&'static str, String) {
    match desired {
        Some(desired) => {
            let status = if up == desired {
                "ok"
            } else if up == 0 {
                "fail"
            } else {
                "degraded" // fewer than desired, or extra tasks still draining
            };
            (status, format!("{up}/{desired} running"))
        }
        None => (if up > 0 { "ok" } else { "fail" }, format!("{up} running")),
    }
}

/// Parses a health URL from a service label. The label is server-provided
/// data (plan section 2.2 item 13): only `http` and `https`, and no
/// credentials in the URL.
fn parse_health_url(raw: &str) -> Result<reqwest::Url, String> {
    let url = reqwest::Url::parse(raw).map_err(|e| format!("invalid URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!(
            "refusing {}:// (only http and https)",
            url.scheme()
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("refusing a URL with credentials".to_string());
    }
    Ok(url)
}

/// `GET` the label's URL from this machine: ok on 2xx.
async fn url_check(plain: &reqwest::Client, name: &str, raw: &str) -> Check {
    let url = match parse_health_url(raw) {
        Ok(url) => url,
        Err(detail) => return Check::new(name, "fail", detail),
    };
    let start = Instant::now();
    match plain.get(url).send().await {
        Ok(response) => {
            let code = response.status().as_u16();
            let ms = start.elapsed().as_millis();
            // A redirect is a failure here: redirects are not followed.
            let status = if response.status().is_success() {
                "ok"
            } else {
                "fail"
            };
            Check::new(name, status, format!("{code} in {ms} ms"))
        }
        Err(e) if e.is_timeout() => Check::new(name, "fail", "timed out"),
        Err(e) => Check::new(name, "fail", format!("request failed: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicas_derive_ok_degraded_and_fail() {
        assert_eq!(replica_status(Some(3), 3).0, "ok");
        assert_eq!(
            replica_status(Some(3), 2),
            ("degraded", "2/3 running".into())
        );
        assert_eq!(replica_status(Some(3), 0).0, "fail");
        assert_eq!(replica_status(Some(1), 2).0, "degraded");
        assert_eq!(replica_status(Some(0), 0).0, "ok");
        assert_eq!(replica_status(None, 2).0, "ok");
        assert_eq!(replica_status(None, 0).0, "fail");
    }

    #[test]
    fn only_http_and_https_urls_are_fetched() {
        assert!(parse_health_url("http://10.0.0.5:8080/up").is_ok());
        assert!(parse_health_url("https://app.example.net/healthz").is_ok());
        for bad in [
            "file:///etc/passwd",
            "ftp://example.net/x",
            "gopher://example.net",
            "http://user:pw@example.net/",
            "http://user@example.net/",
            "not a url",
        ] {
            assert!(parse_health_url(bad).is_err(), "{bad}");
        }
        assert!(parse_health_url("file:///etc/passwd")
            .unwrap_err()
            .contains("file://"));
    }

    #[tokio::test]
    async fn a_refused_scheme_is_a_failed_check_without_a_request() {
        let plain = reqwest::Client::new();
        let check = url_check(&plain, "shop.web url", "ftp://example.net/x").await;
        assert_eq!(check.status, "fail");
        assert!(check.detail.contains("ftp://"), "{}", check.detail);
    }
}
