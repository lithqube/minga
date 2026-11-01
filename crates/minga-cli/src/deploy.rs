//! `minga deploy`: deploys a Compose stack through the Docker Engine API.
//!
//! The CLI is a client of the same write path `docker stack deploy` uses —
//! there is one way to get desired state into Raft, and this is it:
//! networks, secrets and configs are created (idempotently), services are
//! created or updated, then the command polls task placement and reports
//! the per-service outcome.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use minga_scheduler::TranslatedStack;
use minga_state::types::{Service as StateService, ServiceMode};

/// How and where to deploy.
pub struct DeployOptions {
    /// Base URL of a `minga api` server, e.g. `http://127.0.0.1:2375`.
    pub api_url: String,
    /// Stack name (already applied to entity IDs by translation).
    pub stack: String,
    /// How long to wait for tasks to appear before reporting failure.
    pub wait: Duration,
}

/// One service's deployment outcome.
struct ServiceOutcome {
    name: String,
    detail: String,
    ok: bool,
}

/// Deploys a translated stack. Returns `Err` with the full per-service
/// report if anything failed; prints progress to stdout.
pub async fn deploy_stack(
    translated: &TranslatedStack,
    opts: &DeployOptions,
) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|e| format!("cannot build HTTP client: {e}"))?;
    let base = format!("{}/v1.41", opts.api_url.trim_end_matches('/'));

    // Fail fast with a clear message when the API is unreachable.
    client
        .get(format!("{base}/version"))
        .send()
        .await
        .map_err(|e| {
            format!(
                "cannot reach the Minga API at {} (is `minga api` running?): {e}",
                opts.api_url
            )
        })?;

    // --- networks, secrets, configs (idempotent) ------------------------------
    for net in &translated.networks {
        let body = serde_json::json!({
            "Name": net.name,
            "Driver": net.driver,
            "Attachable": net.attachable,
            "Labels": {"com.docker.stack.namespace": opts.stack},
        });
        match post(&client, &base, "networks/create", &body).await? {
            (201, _) => println!("network {}: created", net.name),
            (409, _) => println!("network {}: already exists", net.name),
            (status, body) => {
                return Err(format!(
                    "network {}: create failed ({status}): {body}",
                    net.name
                ))
            }
        }
    }
    for secret in &translated.secrets {
        // The service references the stack-prefixed ID; metadata only —
        // values are provisioned on node-local secret stores.
        let body = serde_json::json!({"Name": secret.id});
        match post(&client, &base, "secrets/create", &body).await? {
            (201, _) => println!("secret {}: metadata created", secret.id),
            (409, _) => println!("secret {}: already exists", secret.id),
            (status, body) => {
                return Err(format!(
                    "secret {}: create failed ({status}): {body}",
                    secret.id
                ))
            }
        }
    }
    for config in &translated.configs {
        let body = serde_json::json!({"Name": config.id, "Data": ""});
        match post(&client, &base, "configs/create", &body).await? {
            (201, _) => println!("config {}: metadata created", config.id),
            (409, _) => println!("config {}: already exists", config.id),
            (status, body) => {
                return Err(format!(
                    "config {}: create failed ({status}): {body}",
                    config.id
                ))
            }
        }
    }

    // --- services ---------------------------------------------------------------
    let mut outcomes: Vec<ServiceOutcome> = Vec::new();
    for svc in &translated.services {
        // The outbound Docker spec of the translated service round-trips
        // through the API's inbound translation — one shared write path.
        let spec = minga_api::translate::service_to_docker(svc).Spec;
        let body = serde_json::to_value(&spec)
            .map_err(|e| format!("service {}: cannot serialize spec: {e}", svc.id))?;

        match post(&client, &base, "services/create", &body).await? {
            (201, _) => {
                println!("service {}: created", svc.id);
                outcomes.push(ServiceOutcome {
                    name: svc.id.clone(),
                    detail: "created".to_string(),
                    ok: true,
                });
            }
            (409, resp_body) if is_already_exists(&resp_body) => {
                let path = format!("services/{}/update?version=1", svc.id);
                match post(&client, &base, &path, &body).await? {
                    (200, _) => {
                        println!("service {}: updated", svc.id);
                        outcomes.push(ServiceOutcome {
                            name: svc.id.clone(),
                            detail: "updated".to_string(),
                            ok: true,
                        });
                    }
                    (status, body) => {
                        outcomes.push(ServiceOutcome {
                            name: svc.id.clone(),
                            detail: format!("update failed ({status}): {body}"),
                            ok: false,
                        });
                    }
                }
            }
            // A 409 here is the hard placement failure — the message says
            // exactly why (no eligible nodes / insufficient capacity).
            (status, body) => {
                outcomes.push(ServiceOutcome {
                    name: svc.id.clone(),
                    detail: format!("rejected ({status}): {body}"),
                    ok: false,
                });
            }
        }
    }

    // --- wait for placement -----------------------------------------------------
    let eligible_nodes = count_ready_nodes(&client, &base).await.unwrap_or(0);
    let deadline = Instant::now() + opts.wait;
    for svc in &translated.services {
        let Some(outcome) = outcomes.iter_mut().find(|o| o.name == svc.id) else {
            continue;
        };
        if !outcome.ok {
            continue;
        }
        let desired = desired_tasks(svc, eligible_nodes);
        loop {
            let placed = count_service_tasks(&client, &base, &svc.id)
                .await
                .unwrap_or(0);
            if placed >= desired {
                outcome.detail = format!("{} — {placed}/{desired} task(s) placed", outcome.detail);
                break;
            }
            if Instant::now() >= deadline {
                outcome.ok = false;
                outcome.detail = format!(
                    "{} — only {placed}/{desired} task(s) placed within {}s",
                    outcome.detail,
                    opts.wait.as_secs()
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    // --- report -------------------------------------------------------------------
    println!("\ndeployment of stack `{}`:", opts.stack);
    let mut failed = Vec::new();
    for outcome in &outcomes {
        let mark = if outcome.ok { "ok " } else { "ERR" };
        println!("  [{mark}] {}: {}", outcome.name, outcome.detail);
        if !outcome.ok {
            failed.push(outcome.name.clone());
        }
    }
    if failed.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "{} service(s) failed: {}",
            failed.len(),
            failed.join(", ")
        ))
    }
}

/// A 409 from service create can mean "already exists" (→ update) or a
/// placeability rejection (→ report). Only the former falls through.
fn is_already_exists(body: &serde_json::Value) -> bool {
    body.get("message")
        .and_then(|m| m.as_str())
        .is_some_and(|m| m.contains("already exists"))
}

/// How many tasks a service should have: its replica count, or one per
/// ready node for global services.
fn desired_tasks(svc: &StateService, ready_nodes: usize) -> usize {
    match svc.mode {
        ServiceMode::Replicated => svc.replicas as usize,
        ServiceMode::Global => ready_nodes.max(1),
    }
}

/// POSTs `body` to `{base}/v1.41/{path}`; returns (status, response body).
async fn post(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    body: &serde_json::Value,
) -> Result<(u16, serde_json::Value), String> {
    let resp = client
        .post(format!("{base}/{path}"))
        .json(body)
        .send()
        .await
        .map_err(|e| format!("POST {path}: {e}"))?;
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    // Error bodies may be plain text (axum extractor rejections); surface
    // them rather than collapsing to null.
    let body = serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text));
    Ok((status, body))
}

/// Counts task records for a service.
async fn count_service_tasks(
    client: &reqwest::Client,
    base: &str,
    service: &str,
) -> Result<usize, String> {
    let filters = serde_json::json!({"service": [service]}).to_string();
    let tasks: Vec<serde_json::Value> = client
        .get(format!("{base}/tasks"))
        .query(&[("filters", filters)])
        .send()
        .await
        .map_err(|e| format!("GET tasks: {e}"))?
        .json()
        .await
        .map_err(|e| format!("GET tasks: invalid response: {e}"))?;
    Ok(tasks.len())
}

/// Counts nodes whose reported status is `ready`.
async fn count_ready_nodes(client: &reqwest::Client, base: &str) -> Result<usize, String> {
    let nodes: Vec<HashMap<String, serde_json::Value>> = client
        .get(format!("{base}/nodes"))
        .send()
        .await
        .map_err(|e| format!("GET nodes: {e}"))?
        .json()
        .await
        .map_err(|e| format!("GET nodes: invalid response: {e}"))?;
    Ok(nodes
        .iter()
        .filter(|n| {
            n.get("Status")
                .and_then(|s| s.get("State"))
                .and_then(|s| s.as_str())
                == Some("ready")
        })
        .count())
}
