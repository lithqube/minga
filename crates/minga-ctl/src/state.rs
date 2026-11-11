//! Local state: revision files and the audit log (plan section 4.5).
//!
//! The directory is `$XDG_STATE_HOME/minga-ctl/`, else
//! `~/.local/state/minga-ctl/`, mode 0700. Every file in it is mode 0600.

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::api;
use crate::config::{self, display_path};
use crate::error::CtlError;

/// The state directory.
pub fn dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_STATE_HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(dir).join("minga-ctl");
    }
    config::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".local")
        .join("state")
        .join("minga-ctl")
}

/// A service spec as it was before the last change minga-ctl made to it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Revision {
    /// RFC 3339, UTC.
    pub saved_at: String,
    pub service_id: String,
    /// The full `Spec`, exactly as the API returned it.
    pub spec: Value,
}

/// `revisions/<context>/<service id>.json`. Both names go into a path, so
/// both are checked like URL IDs.
pub fn revision_path(context: &str, service: &str) -> Result<PathBuf, CtlError> {
    api::check_id("context name", context)?;
    api::check_id("service", service)?;
    Ok(dir()
        .join("revisions")
        .join(context)
        .join(format!("{service}.json")))
}

/// The saved revision of `service` in `context`, if there is one.
pub fn load_revision(context: &str, service: &str) -> Result<Option<Revision>, CtlError> {
    let path = revision_path(context, service)?;
    let invalid = |detail: String| CtlError::ConfigInvalid {
        path: display_path(&path),
        detail,
    };
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| invalid(e.to_string())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(invalid(e.to_string())),
    }
}

/// Saves `spec` as the revision of `service` in `context`, atomically.
pub fn save_revision(context: &str, service: &str, spec: &Value) -> Result<(), CtlError> {
    let path = revision_path(context, service)?;
    let revisions = dir().join("revisions");
    for dir in [dir(), revisions.clone(), revisions.join(context)] {
        config::create_private_dir(&dir)?;
    }
    let revision = Revision {
        saved_at: now(),
        service_id: service.to_string(),
        spec: spec.clone(),
    };
    let mut bytes = serde_json::to_vec(&revision)?;
    bytes.push(b'\n');
    config::write_private_file(&path, &bytes)
}

/// Appends one line to `audit.jsonl` for a command that changes state, on
/// success and on failure. `args` must not hold secret or environment values.
pub fn audit(
    context: &str,
    target: &str,
    command: &str,
    args: &Value,
    result: &Result<(), CtlError>,
) -> Result<(), CtlError> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut line = json!({
        "ts": now(),
        "user": std::env::var("USER").unwrap_or_else(|_| "unknown".to_string()),
        "context": context,
        "target": target,
        "command": command,
        "args": args,
        "result": if result.is_ok() { "ok" } else { "error" },
        "exit_code": result.as_ref().map_or_else(CtlError::exit_code, |()| 0),
    });
    if let Err(err) = result {
        line["error"] = json!(err.to_string());
    }
    let mut bytes = serde_json::to_vec(&line)?;
    bytes.push(b'\n');

    let path = dir().join("audit.jsonl");
    config::create_private_dir(&dir())?;
    fs::OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .open(&path)
        // One write per line: O_APPEND keeps concurrent lines whole.
        .and_then(|mut file| file.write_all(&bytes))
        .map_err(|e| CtlError::ConfigInvalid {
            path: display_path(&path),
            detail: e.to_string(),
        })
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
