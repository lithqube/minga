//! Registry credentials from the Docker configuration file.
//!
//! The file is `$DOCKER_CONFIG/config.json`, else `$HOME/.docker/config.json`:
//! what `docker login` and `minga-ctl registry login` write. For a registry
//! host the lookup order is `credHelpers[host]`, then `credsStore`, then the
//! inline `auths[host].auth`.
//!
//! A helper is the `docker-credential-<name>` program on `PATH`. It is run as
//! `docker-credential-<name> get` with the registry on stdin and answers with
//! `{"ServerURL","Username","Secret"}`. A configured helper that is missing
//! or fails is an error that names it; the pull never falls back to
//! anonymous. The one exception is the helper protocol's own "not found"
//! answer, which means no credential is stored for this registry.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine as _;
use thiserror::Error;
use tokio::io::AsyncWriteExt as _;

/// The answer a helper gives when it holds nothing for the registry.
const NOT_FOUND: &str = "credentials not found in native keychain";

/// How long a helper may take (a keychain can prompt the user).
const HELPER_TIMEOUT: Duration = Duration::from_secs(30);

/// A registry login. The secret is never printed.
#[derive(Clone, PartialEq, Eq)]
pub struct Credential {
    pub username: String,
    secret: String,
}

impl Credential {
    pub fn new(username: impl Into<String>, secret: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            secret: secret.into(),
        }
    }

    pub fn secret(&self) -> &str {
        &self.secret
    }
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credential")
            .field("username", &self.username)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Why credentials could not be read. None of these carry a secret.
#[derive(Debug, Error)]
pub enum CredentialError {
    #[error("docker config {path}: {reason}")]
    Config { path: String, reason: String },
    #[error("credential helper `docker-credential-{name}` for {host}: {reason}")]
    Helper {
        name: String,
        host: String,
        reason: String,
    },
}

/// The registry an image reference names: its first path component when that
/// looks like a host, else Docker Hub.
pub fn registry_host(image_ref: &str) -> &str {
    image_ref
        .split_once('/')
        .map(|(first, _)| first)
        .filter(|h| h.contains('.') || h.contains(':') || *h == "localhost")
        .unwrap_or("docker.io")
}

/// A registry key or host in one spelling: scheme and path dropped, and the
/// Docker Hub names (`index.docker.io`, `registry-1.docker.io`) as `docker.io`.
pub fn canonical_host(key: &str) -> String {
    let key = key
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let key = key.split('/').next().unwrap_or(key);
    match key {
        "index.docker.io" | "registry-1.docker.io" => "docker.io".into(),
        other => other.into(),
    }
}

fn config_path() -> Option<PathBuf> {
    match std::env::var_os("DOCKER_CONFIG") {
        Some(dir) => Some(PathBuf::from(dir).join("config.json")),
        None => Some(PathBuf::from(std::env::var_os("HOME")?).join(".docker/config.json")),
    }
}

/// The credential for the registry `image_ref` names, or `None` to pull
/// anonymously (no config file, or nothing configured for this registry).
pub async fn credential_for(image_ref: &str) -> Result<Option<Credential>, CredentialError> {
    let Some(path) = config_path() else {
        return Ok(None);
    };
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(CredentialError::Config {
                path: path.display().to_string(),
                reason: e.to_string(),
            })
        }
    };
    let config: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| CredentialError::Config {
            path: path.display().to_string(),
            reason: format!("not valid JSON (line {}, column {})", e.line(), e.column()),
        })?;
    let host = registry_host(image_ref);
    let search_path = std::env::var_os("PATH").unwrap_or_default();
    let found = lookup(&config, host, &search_path).await?;
    if found.is_none() {
        tracing::info!(
            registry = host,
            config = %path.display(),
            "no credential configured for this registry; pulling anonymously"
        );
    }
    Ok(found)
}

/// The inline `auths` credential for the registry `image_ref` names, read
/// synchronously and without running any credential helper. For blanking a
/// known credential out of error text, not for pulling.
pub(crate) fn inline_credential(image_ref: &str) -> Option<Credential> {
    inline_credential_in(&config_path()?, image_ref)
}

fn inline_credential_in(path: &Path, image_ref: &str) -> Option<Credential> {
    let bytes = std::fs::read(path).ok()?;
    let config: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    inline_auth(&config, &canonical_host(registry_host(image_ref)))
}

/// `credHelpers[host]`, else `credsStore`, else the inline `auths` entry.
async fn lookup(
    config: &serde_json::Value,
    host: &str,
    search_path: &OsStr,
) -> Result<Option<Credential>, CredentialError> {
    let host = canonical_host(host);
    let helper = config["credHelpers"]
        .as_object()
        .and_then(|helpers| helpers.iter().find(|(key, _)| canonical_host(key) == host))
        .and_then(|(_, name)| name.as_str())
        .or_else(|| config["credsStore"].as_str());
    if let Some(name) = helper {
        return run_helper(name, &host, search_path).await;
    }
    Ok(inline_auth(config, &host))
}

/// The decoded `auths.<key>.auth` whose key names `host`. Keys may carry a
/// scheme or path (`https://index.docker.io/v1/` is how `docker login`
/// records Docker Hub).
fn inline_auth(config: &serde_json::Value, host: &str) -> Option<Credential> {
    let (_, entry) = config["auths"]
        .as_object()?
        .iter()
        .find(|(key, _)| canonical_host(key) == host)?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(entry["auth"].as_str()?)
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (user, secret) = decoded.split_once(':')?;
    Some(Credential::new(user, secret))
}

/// Runs `docker-credential-<name> get` for `host`.
async fn run_helper(
    name: &str,
    host: &str,
    search_path: &OsStr,
) -> Result<Option<Credential>, CredentialError> {
    let fail = |reason: String| CredentialError::Helper {
        name: name.to_string(),
        host: host.to_string(),
        reason,
    };
    // The name becomes part of a program name: no path separators.
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(fail(
            "name has characters other than letters, digits, `-`, `_`, `.`".into(),
        ));
    }
    let program = format!("docker-credential-{name}");
    let path = find_in_path(&program, search_path)
        .ok_or_else(|| fail("not found on PATH (is it installed?)".into()))?;
    // Docker Hub is stored under its legacy index URL by `docker login`.
    let server = if host == "docker.io" {
        "https://index.docker.io/v1/"
    } else {
        host
    };

    let mut child = tokio::process::Command::new(&path)
        .arg("get")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| fail(format!("cannot start {}: {e}", path.display())))?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let server = server.to_string();
    // Written from a task so a helper that never reads cannot wedge us.
    tokio::spawn(async move {
        let _ = stdin.write_all(server.as_bytes()).await;
    });
    let output = tokio::time::timeout(HELPER_TIMEOUT, child.wait_with_output())
        .await
        .map_err(|_| fail(format!("no answer within {}s", HELPER_TIMEOUT.as_secs())))?
        .map_err(|e| fail(format!("wait: {e}")))?;

    if !output.status.success() {
        let said = String::from_utf8_lossy(&output.stdout);
        if said.trim() == NOT_FOUND {
            return Ok(None);
        }
        // The helper's own error text; on failure stdout holds no secret.
        let said: String = format!(
            "{} {}",
            said.trim(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .trim()
        .chars()
        .take(200)
        .collect();
        return Err(fail(format!("exited with {}: {said}", output.status)));
    }

    #[derive(serde::Deserialize)]
    struct Answer {
        #[serde(rename = "Username")]
        username: String,
        #[serde(rename = "Secret")]
        secret: String,
    }
    // Not `{e}`: a parse error may quote the output, and the output is the secret.
    let answer: Answer = serde_json::from_slice(&output.stdout).map_err(|_| {
        fail("answered with something other than {\"Username\",\"Secret\"} JSON".into())
    })?;
    Ok(Some(Credential::new(answer.username, answer.secret)))
}

fn find_in_path(program: &str, search_path: &OsStr) -> Option<PathBuf> {
    std::env::split_paths(&OsString::from(search_path))
        .map(|dir| dir.join(program))
        .find(|candidate| is_executable(candidate))
}

fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// Tests that write and run stub scripts take this first: a thread that
    /// forks while another still has a script open for writing makes the
    /// exec fail with ETXTBSY ("text file busy") on Linux.
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn stub(dir: &Path, name: &str, script: &str) {
        let path = dir.join(format!("docker-credential-{name}"));
        std::fs::write(&path, format!("#!/bin/sh\n{script}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    async fn look(
        config: serde_json::Value,
        host: &str,
        dir: &Path,
    ) -> Result<Option<Credential>, CredentialError> {
        lookup(&config, host, dir.as_os_str()).await
    }

    #[tokio::test]
    async fn inline_auths_match_docker_login_keys() {
        // base64("me:tok") and base64("hub:tok")
        let config = serde_json::json!({"auths": {
            "https://ghcr.io": {"auth": "bWU6dG9r"},
            "https://index.docker.io/v1/": {"auth": "aHViOnRvaw=="},
            "quay.io": {}
        }});
        let empty = tempfile::tempdir().unwrap();
        let get = |image: &str| {
            let config = config.clone();
            let host = registry_host(image).to_string();
            let dir = empty.path().to_path_buf();
            async move { look(config, &host, &dir).await.unwrap() }
        };
        assert_eq!(
            get("ghcr.io/org/app:1").await,
            Some(Credential::new("me", "tok"))
        );
        assert_eq!(
            get("docker.io/library/nginx:1.27").await,
            Some(Credential::new("hub", "tok"))
        );
        assert_eq!(get("nginx:1.27").await, Some(Credential::new("hub", "tok")));
        assert_eq!(get("quay.io/org/app:1").await, None);
        assert_eq!(get("registry.example.com:5000/app:1").await, None);
        assert_eq!(registry_host("localhost/app:1"), "localhost");
    }

    #[test]
    fn inline_credential_reads_auths_and_never_runs_a_helper() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        // base64("me:tok"); quay.io is served by a helper that does not exist.
        std::fs::write(
            &path,
            r#"{"auths": {"https://ghcr.io": {"auth": "bWU6dG9r"}},
                "credHelpers": {"quay.io": "minga-test-missing"}}"#,
        )
        .unwrap();
        assert_eq!(
            inline_credential_in(&path, "ghcr.io/org/app:1"),
            Some(Credential::new("me", "tok"))
        );
        assert_eq!(inline_credential_in(&path, "quay.io/org/app:1"), None);
        assert_eq!(
            inline_credential_in(&dir.path().join("absent.json"), "ghcr.io/org/app:1"),
            None
        );
    }

    #[tokio::test]
    async fn helper_answers_by_registry_and_gets_the_host_on_stdin() {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir().unwrap();
        // Echo the stdin back as the username so the test sees what was sent.
        stub(
            dir.path(),
            "stub",
            r#"[ "$1" = get ] || exit 2
host=$(cat)
printf '{"ServerURL":"%s","Username":"%s","Secret":"s3cret"}' "$host" "$host""#,
        );
        let config = serde_json::json!({"credsStore": "stub"});
        let got = look(config.clone(), "ghcr.io", dir.path()).await.unwrap();
        assert_eq!(got, Some(Credential::new("ghcr.io", "s3cret")));
        // Docker Hub is asked for under the index URL `docker login` stores.
        let got = look(config, "registry-1.docker.io", dir.path())
            .await
            .unwrap();
        assert_eq!(
            got,
            Some(Credential::new("https://index.docker.io/v1/", "s3cret"))
        );
    }

    #[tokio::test]
    async fn cred_helpers_beat_creds_store_beat_inline_auths() {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir().unwrap();
        stub(
            dir.path(),
            "per",
            r#"printf '{"Username":"per","Secret":"x"}'"#,
        );
        stub(
            dir.path(),
            "store",
            r#"printf '{"Username":"store","Secret":"x"}'"#,
        );
        // base64("inline:x")
        let config = serde_json::json!({
            "credHelpers": {"https://ghcr.io": "per"},
            "credsStore": "store",
            "auths": {"quay.io": {"auth": "aW5saW5lOng="}}
        });
        let user = |host: &'static str| {
            let config = config.clone();
            let path = dir.path().to_path_buf();
            async move { look(config, host, &path).await.unwrap().unwrap().username }
        };
        assert_eq!(user("ghcr.io").await, "per");
        // credsStore wins over an inline entry, as in docker.
        assert_eq!(user("quay.io").await, "store");
    }

    #[tokio::test]
    async fn missing_or_failing_helper_is_an_error_naming_it_and_never_anonymous() {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let config = serde_json::json!({
            "credHelpers": {"ghcr.io": "nope"},
            "auths": {"ghcr.io": {"auth": "bWU6dG9r"}}
        });
        let err = look(config, "ghcr.io", dir.path()).await.unwrap_err();
        let text = err.to_string();
        assert!(text.contains("docker-credential-nope"), "{text}");
        assert!(text.contains("not found"), "{text}");

        stub(dir.path(), "bad", "echo 'keychain locked' >&2; exit 1");
        let config = serde_json::json!({"credsStore": "bad"});
        let text = look(config, "ghcr.io", dir.path())
            .await
            .unwrap_err()
            .to_string();
        assert!(text.contains("docker-credential-bad"), "{text}");
        assert!(text.contains("keychain locked"), "{text}");
    }

    #[tokio::test]
    async fn helper_not_found_answer_means_anonymous() {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir().unwrap();
        stub(
            dir.path(),
            "empty",
            "echo 'credentials not found in native keychain'; exit 1",
        );
        let config = serde_json::json!({"credsStore": "empty"});
        assert_eq!(look(config, "ghcr.io", dir.path()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn helper_secret_never_reaches_error_text() {
        let _serial = SERIAL.lock().await;
        let dir = tempfile::tempdir().unwrap();
        // Truncated JSON holding a secret: the parse error must not echo it.
        stub(
            dir.path(),
            "odd",
            r#"printf '{"Username":"u","Secret":"hunter2-hunter2","Extra":'"#,
        );
        let config = serde_json::json!({"credsStore": "odd"});
        let text = look(config, "ghcr.io", dir.path())
            .await
            .unwrap_err()
            .to_string();
        assert!(text.contains("docker-credential-odd"), "{text}");
        assert!(!text.contains("hunter2"), "{text}");
        let debug = format!("{:?}", Credential::new("u", "hunter2"));
        assert!(!debug.contains("hunter2"), "{debug}");
    }

    #[tokio::test]
    async fn helper_names_cannot_be_paths() {
        let dir = tempfile::tempdir().unwrap();
        let config = serde_json::json!({"credsStore": "../evil"});
        let err = look(config, "ghcr.io", dir.path()).await.unwrap_err();
        assert!(err.to_string().contains("characters"), "{err}");
    }
}
