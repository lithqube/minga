//! Configuration file handling for `minga-ctl`.
//!
//! The config lives at `$XDG_CONFIG_HOME/minga-ctl/config.yaml`, or
//! `~/.config/minga-ctl/config.yaml`. It holds only context definitions:
//! never a token value. An unknown key is an error, so a stray `api_token:`
//! line fails loudly instead of being silently ignored.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::error::CtlError;

/// Endpoint used when neither a flag, the environment nor a context names one.
pub const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:2375";

/// The whole config file.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Context used when no `--context` is given.
    pub current_context: Option<String>,
    /// Contexts by name.
    #[serde(default)]
    pub contexts: BTreeMap<String, Context>,
}

/// One named context: how to reach a cluster and how to authenticate.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Context {
    /// SSH target for the API tunnel and remote commands.
    pub ssh: Option<String>,
    /// API URL. With `ssh`, as seen from the SSH target.
    pub api_endpoint: Option<String>,
    /// File on the SSH target holding the API token.
    pub remote_token_file: Option<String>,
    /// Local command that prints the token; run without a shell.
    pub token_command: Option<Vec<String>>,
    /// PEM CA certificate to trust for an `https://` endpoint.
    pub tls_cert: Option<PathBuf>,
    /// Metrics URL. With `ssh`, as seen from the SSH target.
    pub metrics_endpoint: Option<String>,
    /// NATS URL for logs, as seen from where `minga logs` runs.
    pub nats_url: Option<String>,
    /// SSH targets for node-local commands (image, secret put, registry).
    #[serde(default)]
    pub nodes: Vec<String>,
    /// Docker credential file on the node that Minga reads when pulling.
    pub registry_auth_file: Option<String>,
}

/// Where the API token comes from, in precedence order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenSource {
    /// `MINGA_CTL_TOKEN`.
    Env,
    /// A local command run without a shell.
    Command(Vec<String>),
    /// A file on the SSH target; read over SSH.
    RemoteFile(String),
    /// No token configured.
    None,
}

/// What a command runs against, after flags, environment and config merge.
#[derive(Debug, Clone)]
pub struct Resolved {
    /// Context name, or `default`.
    pub name: String,
    /// The context's settings.
    pub context: Context,
    /// API endpoint in use.
    pub endpoint: String,
    /// Where the token comes from.
    pub token_source: TokenSource,
    /// The token itself, in memory only.
    pub token: Option<String>,
}

/// Directory holding the config file.
pub fn config_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty()) {
        return PathBuf::from(dir).join("minga-ctl");
    }
    home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config")
        .join("minga-ctl")
}

/// Path of the config file.
pub fn config_path() -> PathBuf {
    config_dir().join("config.yaml")
}

/// The user's home directory.
pub fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// Renders a path with the home directory shortened to `~`.
pub fn display_path(path: &Path) -> String {
    if let Some(home) = home_dir() {
        if let Ok(rest) = path.strip_prefix(&home) {
            return format!("~/{}", rest.display());
        }
    }
    path.display().to_string()
}

/// Reads the config file. A missing file is an empty config.
pub fn load() -> Result<Config, CtlError> {
    let path = config_path();
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Config::default()),
        Err(e) => {
            return Err(CtlError::ConfigInvalid {
                path: display_path(&path),
                detail: e.to_string(),
            })
        }
    };
    check_permissions(&path)?;
    parse(&text, &path)
}

/// Parses config text. Unknown keys are an error.
pub fn parse(text: &str, path: &Path) -> Result<Config, CtlError> {
    let shown = display_path(path);
    let deserializer = serde_yaml::Deserializer::from_str(text);
    match serde_path_to_error::deserialize::<_, Config>(deserializer) {
        Ok(config) => Ok(config),
        Err(err) => {
            let inner = err.into_inner();
            let message = inner.to_string();
            match unknown_field(&message) {
                Some(field) => Err(CtlError::ConfigUnknownField { path: shown, field }),
                None => Err(CtlError::ConfigInvalid {
                    path: shown,
                    detail: message,
                }),
            }
        }
    }
}

/// Writes the config file with mode 0600, in a directory with mode 0700.
pub fn save(config: &Config) -> Result<(), CtlError> {
    let dir = config_dir();
    create_private_dir(&dir)?;
    let path = config_path();
    let text = serde_yaml::to_string(config).map_err(|e| CtlError::ConfigInvalid {
        path: display_path(&path),
        detail: e.to_string(),
    })?;
    write_private_file(&path, text.as_bytes())
}

/// Creates a directory with mode 0700, tightening the mode if it exists.
pub fn create_private_dir(dir: &Path) -> Result<(), CtlError> {
    fs::create_dir_all(dir).map_err(|e| CtlError::ConfigInvalid {
        path: display_path(dir),
        detail: e.to_string(),
    })?;
    set_mode(dir, 0o700)
}

/// Writes a file with mode 0600, replacing any existing content atomically:
/// the bytes go to a private temporary file beside it, which is synced and
/// renamed over `path`. A failed write leaves the old file intact.
pub fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), CtlError> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let fail = |e: std::io::Error| CtlError::ConfigInvalid {
        path: display_path(path),
        detail: e.to_string(),
    };
    let mut tmp_name = path.file_name().unwrap_or_default().to_os_string();
    tmp_name.push(format!(".tmp.{}", std::process::id()));
    let tmp = path.with_file_name(tmp_name);

    let written = (|| {
        let _ = fs::remove_file(&tmp); // left by a crashed run with this pid
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&tmp, path)
    })();
    if let Err(e) = written {
        let _ = fs::remove_file(&tmp);
        return Err(fail(e));
    }
    set_mode(path, 0o600)
}

/// Loads the config, applies `change` and saves the result while holding an
/// exclusive lock on `config.lock`, so two concurrent `context` commands
/// cannot overwrite each other's change.
pub fn update<T>(change: impl FnOnce(&mut Config) -> Result<T, CtlError>) -> Result<T, CtlError> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;

    let dir = config_dir();
    create_private_dir(&dir)?;
    let lock_path = dir.join("config.lock");
    let lock = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&lock_path)
        .map_err(|e| CtlError::ConfigInvalid {
            path: display_path(&lock_path),
            detail: e.to_string(),
        })?;
    // SAFETY: flock on a descriptor this function owns; released on close.
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(CtlError::ConfigInvalid {
            path: display_path(&lock_path),
            detail: std::io::Error::last_os_error().to_string(),
        });
    }
    let mut config = load()?;
    let result = change(&mut config)?;
    save(&config)?;
    drop(lock);
    Ok(result)
}

/// Refuses a config file that group or others can read.
pub fn check_permissions(path: &Path) -> Result<(), CtlError> {
    let Some(mode) = file_mode(path) else {
        return Ok(());
    };
    if mode & 0o077 != 0 {
        return Err(CtlError::ConfigPermissions {
            path: display_path(path),
            mode,
        });
    }
    Ok(())
}

/// The permission bits of a file, if it exists.
pub fn file_mode(path: &Path) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(path)
        .ok()
        .map(|m| m.permissions().mode() & 0o777)
}

/// Sets a path's permission bits.
pub fn set_mode(path: &Path, mode: u32) -> Result<(), CtlError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).map_err(|e| {
        CtlError::ConfigInvalid {
            path: display_path(path),
            detail: e.to_string(),
        }
    })
}

/// Extracts the field name from a serde "unknown field" message.
///
/// `serde_path_to_error` prefixes the message with the path of the offending
/// key, so the phrase is looked up anywhere in the text.
fn unknown_field(message: &str) -> Option<String> {
    let rest = message.split_once("unknown field `")?.1;
    let end = rest.find('`')?;
    Some(rest[..end].to_string())
}

/// Refuses a plain-HTTP endpoint that is not on the loopback interface.
///
/// An endpoint behind an SSH target is reached through the tunnel, so only
/// direct endpoints are checked.
pub fn check_endpoint_policy(ssh: Option<&str>, endpoint: &str) -> Result<(), CtlError> {
    if ssh.is_some() {
        return Ok(());
    }
    if let Some(host) = plain_http_host(endpoint) {
        return Err(CtlError::PlainHttp { host });
    }
    Ok(())
}

/// Returns the host of a plain-HTTP endpoint that is not loopback.
fn plain_http_host(endpoint: &str) -> Option<String> {
    let (scheme, rest) = endpoint.split_once("://")?;
    if !scheme.eq_ignore_ascii_case("http") {
        return None;
    }
    // The authority ends at the first `/`, `?` or `#` (WHATWG URL), so
    // `http://10.0.0.1?@localhost/` names 10.0.0.1, not localhost.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = host_of(authority);
    if host.eq_ignore_ascii_case("localhost") || host == "::1" {
        return None;
    }
    if host == "127.0.0.1" {
        return None;
    }
    Some(host.to_string())
}

/// The host part of an authority, without userinfo, port or brackets.
fn host_of(authority: &str) -> &str {
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    if let Some(rest) = authority.strip_prefix('[') {
        // An IPv6 literal: `[::1]:2375`.
        return rest.split(']').next().unwrap_or(rest);
    }
    authority.split(':').next().unwrap_or(authority)
}

/// Runs a token command without a shell and returns its trimmed stdout.
pub fn run_token_command(command: &[String]) -> Result<String, CtlError> {
    let (program, args) = command
        .split_first()
        .ok_or_else(|| CtlError::Usage("a token command needs a program to run".to_string()))?;
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|e| CtlError::Usage(format!("cannot run token command `{program}`: {e}")))?;
    if !output.status.success() {
        return Err(CtlError::Usage(format!(
            "token command `{program}` failed with {}",
            output.status
        )));
    }
    let token = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if token.is_empty() {
        return Err(CtlError::Usage(
            "token command printed an empty token".to_string(),
        ));
    }
    Ok(token)
}

/// Merges flags, environment and config into what a command runs against.
pub fn resolve(config: &Config, flag_context: Option<&str>) -> Result<Resolved, CtlError> {
    let env_context = std::env::var("MINGA_CTL_CONTEXT")
        .ok()
        .filter(|v| !v.is_empty());
    let name = flag_context
        .map(|s| s.to_string())
        .or(env_context)
        .or_else(|| config.current_context.clone())
        .unwrap_or_else(|| "default".to_string());

    let context = if name == "default" {
        config.contexts.get("default").cloned().unwrap_or_default()
    } else {
        match config.contexts.get(&name) {
            Some(context) => context.clone(),
            None => return Err(CtlError::ContextNotFound { name }),
        }
    };

    let endpoint = std::env::var("MINGA_CTL_ENDPOINT")
        .ok()
        .filter(|v| !v.is_empty())
        .or_else(|| context.api_endpoint.clone())
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());

    let env_token = std::env::var("MINGA_CTL_TOKEN")
        .ok()
        .filter(|v| !v.is_empty());
    let (token_source, token) = match env_token {
        Some(token) => (TokenSource::Env, Some(token)),
        None => match &context.token_command {
            Some(command) => {
                let token = run_token_command(command)?;
                (TokenSource::Command(command.clone()), Some(token))
            }
            None => match &context.remote_token_file {
                Some(file) => (TokenSource::RemoteFile(file.clone()), None),
                None => (TokenSource::None, None),
            },
        },
    };

    Ok(Resolved {
        name,
        context,
        endpoint,
        token_source,
        token,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_config(text: &str, mode: u32) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.yaml");
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(text.as_bytes()).unwrap();
        set_mode(&path, mode).unwrap();
        dir
    }

    #[test]
    fn parse_reads_contexts() {
        let text = r#"
current_context: prod
contexts:
  prod:
    ssh: alice@mgr1.example.net
    remote_token_file: /etc/minga/api-token
    nodes:
      - alice@mgr1.example.net
  local:
    api_endpoint: http://127.0.0.1:2375
"#;
        let config = parse(text, Path::new("/tmp/config.yaml")).unwrap();
        assert_eq!(config.current_context.as_deref(), Some("prod"));
        assert_eq!(config.contexts.len(), 2);
        let prod = &config.contexts["prod"];
        assert_eq!(prod.ssh.as_deref(), Some("alice@mgr1.example.net"));
        assert_eq!(
            prod.remote_token_file.as_deref(),
            Some("/etc/minga/api-token")
        );
        assert_eq!(prod.nodes, vec!["alice@mgr1.example.net"]);
    }

    #[test]
    fn parse_refuses_an_unknown_field() {
        let text = "api_token: hunter2\n";
        let err = parse(text, Path::new("/tmp/config.yaml")).unwrap_err();
        match err {
            CtlError::ConfigUnknownField { field, .. } => assert_eq!(field, "api_token"),
            other => panic!("expected an unknown-field error, got {other:?}"),
        }
    }

    #[test]
    fn parse_refuses_an_unknown_field_inside_a_context() {
        let text = "contexts:\n  prod:\n    api_token: hunter2\n";
        let err = parse(text, Path::new("/tmp/config.yaml")).unwrap_err();
        match err {
            CtlError::ConfigUnknownField { field, .. } => assert_eq!(field, "api_token"),
            other => panic!("expected an unknown-field error, got {other:?}"),
        }
    }

    #[test]
    fn a_group_readable_config_is_refused() {
        let dir = write_config("current_context: prod\n", 0o644);
        let path = dir.path().join("config.yaml");
        let err = check_permissions(&path).unwrap_err();
        match err {
            CtlError::ConfigPermissions { mode, .. } => assert_eq!(mode, 0o644),
            other => panic!("expected a permissions error, got {other:?}"),
        }
    }

    #[test]
    fn a_private_config_is_accepted() {
        let dir = write_config("current_context: prod\n", 0o600);
        let path = dir.path().join("config.yaml");
        assert!(check_permissions(&path).is_ok());
    }

    #[test]
    fn plain_http_to_a_remote_host_is_refused() {
        let err = check_endpoint_policy(None, "http://10.0.0.1:2375").unwrap_err();
        match err {
            CtlError::PlainHttp { host } => assert_eq!(host, "10.0.0.1"),
            other => panic!("expected a plain-HTTP error, got {other:?}"),
        }
    }

    #[test]
    fn a_query_or_fragment_cannot_smuggle_a_loopback_host() {
        for endpoint in [
            "http://10.0.0.1:2375?@localhost/",
            "http://10.0.0.1:2375#@127.0.0.1",
            "http://10.0.0.1?x=1",
            "HTTP://10.0.0.1:2375",
        ] {
            match check_endpoint_policy(None, endpoint) {
                Err(CtlError::PlainHttp { host }) => assert_eq!(host, "10.0.0.1", "{endpoint}"),
                other => panic!("{endpoint}: expected a plain-HTTP error, got {other:?}"),
            }
        }
        assert!(check_endpoint_policy(None, "http://user@localhost:2375/v1.41?x=1").is_ok());
    }

    #[test]
    fn plain_http_to_the_loopback_interface_is_allowed() {
        assert!(check_endpoint_policy(None, "http://127.0.0.1:2375").is_ok());
        assert!(check_endpoint_policy(None, "http://localhost:2375").is_ok());
        assert!(check_endpoint_policy(None, "http://[::1]:2375").is_ok());
    }

    #[test]
    fn https_to_any_host_is_allowed() {
        assert!(check_endpoint_policy(None, "https://minga.edge.example.net").is_ok());
    }

    #[test]
    fn an_endpoint_behind_an_ssh_target_is_not_checked() {
        assert!(
            check_endpoint_policy(Some("alice@mgr1.example.net"), "http://10.0.0.1:2375").is_ok()
        );
    }

    #[test]
    fn an_unknown_context_is_not_found() {
        let config = Config::default();
        let err = resolve(&config, Some("nope")).unwrap_err();
        match err {
            CtlError::ContextNotFound { name } => assert_eq!(name, "nope"),
            other => panic!("expected a not-found error, got {other:?}"),
        }
    }

    #[test]
    fn a_token_command_runs_without_a_shell() {
        let command = vec!["printf".to_string(), "%s".to_string(), "s3cret".to_string()];
        let token = run_token_command(&command).unwrap();
        assert_eq!(token, "s3cret");
    }

    #[test]
    fn a_failing_token_command_is_a_usage_error() {
        let command = vec!["false".to_string()];
        let err = run_token_command(&command).unwrap_err();
        assert!(matches!(err, CtlError::Usage(_)), "got {err:?}");
    }
}
