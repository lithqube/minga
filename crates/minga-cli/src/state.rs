//! `minga state export` / `minga state import`: back up and restore desired
//! state through the Minga API (`GET`/`POST /v1.41/state`, see `docs/backup.md`).

use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::time::Duration;

/// Where the API is and how to authenticate.
pub struct ApiOptions {
    pub api_url: String,
    pub api_token: Option<String>,
}

fn client(opts: &ApiOptions) -> Result<reqwest::Client, String> {
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = &opts.api_token {
        headers.insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {token}")
                .parse()
                .map_err(|_| "API token is not valid header data".to_string())?,
        );
    }
    reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .default_headers(headers)
        .build()
        .map_err(|e| format!("cannot build HTTP client: {e}"))
}

fn url(opts: &ApiOptions) -> String {
    format!("{}/v1.41/state", opts.api_url.trim_end_matches('/'))
}

/// Fetches the snapshot and writes it to `out`, or to stdout when `None`.
/// The file is created with mode 0600 (it holds service environment values)
/// and an existing file is left alone unless `force` is set.
pub async fn export(opts: &ApiOptions, out: Option<&Path>, force: bool) -> Result<(), String> {
    let resp = client(opts)?
        .get(url(opts))
        .send()
        .await
        .map_err(|e| format!("cannot reach the Minga API at {}: {e}", opts.api_url))?;
    let status = resp.status();
    let body = resp.bytes().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        return Err(format!(
            "export failed ({status}): {}",
            String::from_utf8_lossy(&body)
        ));
    }
    let Some(path) = out else {
        return std::io::stdout()
            .write_all(&body)
            .map_err(|e| format!("cannot write to stdout: {e}"));
    };

    let mut opts_file = std::fs::OpenOptions::new();
    opts_file.write(true).mode(0o600);
    if force {
        opts_file.create(true);
    } else {
        opts_file.create_new(true);
    }
    let mut file = opts_file.open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            format!("{} exists; pass --force to overwrite it", path.display())
        } else {
            format!("cannot create {}: {e}", path.display())
        }
    })?;
    // `mode` only applies to a new file: tighten a pre-existing one before
    // writing, then drop its old content.
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .and_then(|()| file.set_len(0))
        .and_then(|()| file.write_all(&body))
        .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    eprintln!(
        "wrote {} (mode 0600). It holds service environment values: keep it private.",
        path.display()
    );
    Ok(())
}

/// Reads a snapshot file and imports it.
pub async fn import(opts: &ApiOptions, file: &Path, overwrite: bool) -> Result<(), String> {
    let body = std::fs::read(file).map_err(|e| format!("cannot read {}: {e}", file.display()))?;
    let resp = client(opts)?
        .post(url(opts))
        .query(&[("overwrite", overwrite)])
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| format!("cannot reach the Minga API at {}: {e}", opts.api_url))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    let message = serde_json::from_str::<serde_json::Value>(&text)
        .ok()
        .and_then(|v| v["message"].as_str().map(str::to_string))
        .unwrap_or(text);
    if status.is_success() {
        println!("{message}");
        Ok(())
    } else {
        Err(format!("import failed ({status}): {message}"))
    }
}
