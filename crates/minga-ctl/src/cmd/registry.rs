//! `minga-ctl registry` — `login`, `logout`, `list` (plan section 5.8).
//!
//! Node-local, like `secret put`: the Docker credential file Minga reads on
//! each pull is read over ssh, edited in memory and written back through the
//! remote command's stdin. The credential is never in argv, logs or the audit
//! log, and never written to the workstation. A local context runs the same
//! scripts with the local `sh`.

use std::io::{BufRead, IsTerminal, Read};
use std::path::PathBuf;
use std::process::Stdio;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use clap::{Args, Subcommand};
use serde_json::{json, Map, Value};
use tokio::io::AsyncWriteExt;

use crate::cmd::service::confirm;
use crate::config::Resolved;
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::state;
use crate::transport::{self, RemoteArg, RemoteOutput};

/// Where Minga reads credentials when the context sets no `registry_auth_file`.
const DEFAULT_AUTH_FILE: &str = "/root/.docker/config.json";

/// Prints the file, or nothing when it does not exist.
const READ: &str = r#"if [ -e "$1" ]; then cat -- "$1"; fi"#;

/// Writes stdin to a temporary file and renames it over the file, so a failed
/// write never leaves the file empty or cut off. `cp -p` carries an existing
/// file's mode and group (and, as root, owner) to the copy; access for others
/// is dropped, since the file holds credentials. A new file is created 0600.
/// shortcut: a symlinked file is replaced by a regular file; resolve the link
/// first if a node needs one.
const WRITE: &str = r#"umask 077
t="$1.minga-ctl.tmp"
rm -f -- "$t" || exit
if [ -e "$1" ]; then
  cp -p -- "$1" "$t" && chmod -- o-rwx "$t" && cat > "$t" && mv -f -- "$t" "$1" ||
    { s=$?; rm -f -- "$t"; exit "$s"; }
else
  mkdir -p -- "$(dirname -- "$1")" && cat > "$t" && mv -f -- "$t" "$1" ||
    { s=$?; rm -f -- "$t"; exit "$s"; }
  echo "created $(id -un)"
fi"#;

/// Registry commands.
#[derive(Debug, Subcommand)]
pub enum RegistryCommand {
    /// Store a pull credential for a registry on each node.
    Login(LoginArgs),
    /// Remove a registry's credential from each node.
    Logout {
        /// Short name (`dockerhub`, `ghcr`) or host.
        registry: String,
        /// Node to change; repeatable. Default: the context's nodes.
        #[arg(long = "ssh")]
        ssh: Vec<String>,
        /// Do not ask for confirmation.
        #[arg(long)]
        yes: bool,
    },
    /// Show which registries each node has a credential for. Never secrets.
    List {
        /// Node to read; repeatable. Default: the context's nodes.
        #[arg(long = "ssh")]
        ssh: Vec<String>,
    },
}

/// `registry login` flags. There is no `--password`: argv is visible to
/// every local user.
#[derive(Debug, Args)]
pub struct LoginArgs {
    /// Short name (`dockerhub`, `ghcr`) or host.
    pub registry: String,
    /// Registry user name. Google registries use `_json_key`.
    #[arg(long)]
    pub username: Option<String>,
    /// Read the credential from this file (a Google service account key).
    #[arg(long)]
    pub key_file: Option<PathBuf>,
    /// Accepted for the `docker login` habit; stdin is read when it is not a
    /// terminal anyway.
    #[arg(long)]
    pub password_stdin: bool,
    /// Node to change; repeatable. Default: the context's nodes.
    #[arg(long = "ssh")]
    pub ssh: Vec<String>,
}

/// Runs a registry command.
pub async fn run(
    cmd: &RegistryCommand,
    resolved: &Resolved,
    format: Format,
    quiet: bool,
    debug: bool,
) -> Result<(), CtlError> {
    let flags = match cmd {
        RegistryCommand::Login(args) => &args.ssh,
        RegistryCommand::Logout { ssh, .. } | RegistryCommand::List { ssh } => ssh,
    };
    let job = Job {
        targets: targets(flags, resolved),
        path: resolved
            .context
            .registry_auth_file
            .clone()
            .unwrap_or_else(|| DEFAULT_AUTH_FILE.to_string()),
        default_path: resolved.context.registry_auth_file.is_none(),
        debug,
    };
    if job.targets.iter().any(Option::is_some) {
        transport::validate_remote_path(&job.path)?;
    }
    let (command, mut args) = match cmd {
        RegistryCommand::List { .. } => return list(&job, format, quiet).await,
        RegistryCommand::Login(login) => ("registry login", json!({ "registry": login.registry })),
        RegistryCommand::Logout { registry, .. } => {
            ("registry logout", json!({ "registry": registry }))
        }
    };
    let names: Vec<String> = job.targets.iter().map(|t| name(t.as_deref())).collect();
    args["targets"] = json!(names);
    let result = match cmd {
        RegistryCommand::Login(login) => self::login(login, &job, &mut args, format, quiet).await,
        RegistryCommand::Logout { registry, yes, .. } => {
            logout(registry, *yes, &job, &mut args, format, quiet).await
        }
        RegistryCommand::List { .. } => unreachable!("handled above"),
    };
    if let Err(err) = state::audit(&resolved.name, &names.join(","), command, &args, &result) {
        eprintln!("warning: could not write the audit log: {err}");
    }
    result
}

/// What every subcommand works on.
struct Job {
    /// `None` is the local machine.
    targets: Vec<Option<String>>,
    path: String,
    /// `path` is [`DEFAULT_AUTH_FILE`] because the context sets none.
    default_path: bool,
    debug: bool,
}

/// Every `--ssh`; else the context's `nodes`; else its `ssh`; else local.
fn targets(flags: &[String], resolved: &Resolved) -> Vec<Option<String>> {
    let context = &resolved.context;
    if !flags.is_empty() {
        flags.iter().cloned().map(Some).collect()
    } else if !context.nodes.is_empty() {
        context.nodes.iter().cloned().map(Some).collect()
    } else {
        vec![context.ssh.clone()]
    }
}

fn name(target: Option<&str>) -> String {
    target.unwrap_or("local").to_string()
}

/// A registry the user named.
#[derive(Debug, PartialEq)]
struct Registry {
    /// The `auths` key written.
    host: String,
    /// Google registries take a JSON key with the fixed user `_json_key`.
    google: bool,
    /// What to print before asking for the credential.
    hint: String,
}

fn registry(name: &str) -> Result<Registry, CtlError> {
    let known = |host: &str, hint: &str| Registry {
        host: host.to_string(),
        google: false,
        hint: hint.to_string(),
    };
    // Named as Minga matches it, so login replaces and logout removes the
    // entry pulls use: `INDEX.docker.io` is `docker.io`.
    let name = name.to_ascii_lowercase();
    if !matches!(name.as_str(), "dockerhub" | "ghcr" | "github") {
        validate_host(&name)?;
    }
    Ok(match canonical(&name).as_str() {
        "dockerhub" | "docker.io" => known(
            "docker.io",
            "Docker Hub (docker.io)\nCreate an access token with the Read-only permission.",
        ),
        "ghcr" | "github" | "ghcr.io" => known(
            "ghcr.io",
            "GitHub Container Registry (ghcr.io)\n\
             Create a personal access token (classic) with only the read:packages scope.",
        ),
        host => {
            let bare = host.split(':').next().unwrap_or(host);
            let google =
                bare.ends_with("-docker.pkg.dev") || bare == "gcr.io" || bare.ends_with(".gcr.io");
            Registry {
                host: host.to_string(),
                google,
                hint: if google {
                    format!(
                        "Google registry ({host})\nUse a service account key with the \
                         Artifact Registry Reader role, passed with --key-file."
                    )
                } else {
                    format!("{host}\nUse a read-only pull credential if the registry has scopes.")
                },
            }
        }
    })
}

/// `^[A-Za-z0-9][A-Za-z0-9.-]*(:[0-9]+)?$`
fn validate_host(host: &str) -> Result<(), CtlError> {
    let (name, port) = match host.split_once(':') {
        Some((name, port)) => (name, Some(port)),
        None => (host, None),
    };
    let mut chars = name.chars();
    let ok = chars.next().is_some_and(|c| c.is_ascii_alphanumeric())
        && chars.all(|c| c.is_ascii_alphanumeric() || ".-".contains(c))
        && !port.is_some_and(|p| p.is_empty() || !p.chars().all(|c| c.is_ascii_digit()));
    if ok {
        return Ok(());
    }
    Err(CtlError::Usage(format!(
        "invalid registry `{}`: use dockerhub, ghcr or a host[:port]",
        host.escape_debug()
    )))
}

/// `^[A-Za-z0-9_][A-Za-z0-9._@-]*$`. No `:`, which ends the user in `auth`.
fn validate_username(user: &str) -> Result<(), CtlError> {
    let mut chars = user.chars();
    let ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || "._@-".contains(c));
    if ok {
        return Ok(());
    }
    Err(CtlError::Usage(format!(
        "invalid username `{}`: use letters, digits and `._@-`",
        user.escape_debug()
    )))
}

/// The registry an `auths` key names, as Minga matches it
/// (`auth_for_host` in minga-node): `https://index.docker.io/v1/`, which
/// `docker login` writes, is `docker.io`. Lowercased, so a key stored as
/// `GHCR.IO` is replaced by a new `ghcr.io` login.
fn canonical(key: &str) -> String {
    let key = key.to_ascii_lowercase();
    let key = key
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    match key.split('/').next().unwrap_or(key) {
        "index.docker.io" | "registry-1.docker.io" => "docker.io".to_string(),
        other => other.to_string(),
    }
}

async fn login(
    login: &LoginArgs,
    job: &Job,
    audit: &mut Value,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let reg = registry(&login.registry)?;
    audit["registry"] = json!(reg.host);
    eprintln!("{}", reg.hint);
    let user = match (&login.username, reg.google) {
        (_, true) => "_json_key".to_string(),
        (Some(user), false) => user.clone(),
        (None, false) => prompt_username()?,
    };
    validate_username(&user)?;
    audit["username"] = json!(user);
    let secret = credential(&reg.host, login)?;
    let auth = BASE64.encode(format!("{user}:{secret}"));
    drop(secret);

    let mut rows = Rows::new(&["NODE", "RESULT"]);
    let mut failed = 0;
    for target in &job.targets {
        let node = name(target.as_deref());
        let stored = edit(job, target.as_deref(), |auths| {
            // An alias such as `https://ghcr.io` would compete with the new entry.
            auths.retain(|key, _| canonical(key) != reg.host);
            auths.insert(reg.host.clone(), json!({ "auth": auth }));
            true
        })
        .await;
        let stored = report_failure(stored, &mut failed);
        rows.push(
            vec![
                node.clone(),
                if stored { "stored" } else { "failed" }.into(),
            ],
            json!({
                "node": node, "registry": reg.host, "username": user,
                "file": job.path, "stored": stored,
            }),
        );
    }
    if job.default_path && !quiet {
        eprintln!("{}", default_path_note(&job.path));
    }
    finish(rows, failed, job, format, quiet)
}

/// Said after a login to the default file: Minga reads a different file when
/// its service sets `DOCKER_CONFIG` or runs as another user, and then pulls
/// would silently stay anonymous.
fn default_path_note(path: &str) -> String {
    format!(
        "note: credentials were written to {path}, the default. Minga reads \
         $DOCKER_CONFIG/config.json when its service sets DOCKER_CONFIG, else \
         $HOME/.docker/config.json of the user it runs as. If that is another file \
         (the staging guide uses /etc/minga/docker/config.json), set the context's \
         registry_auth_file to it and log in again."
    )
}

async fn logout(
    registry_name: &str,
    yes: bool,
    job: &Job,
    audit: &mut Value,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    let reg = registry(registry_name)?;
    audit["registry"] = json!(reg.host);
    let nodes: Vec<String> = job.targets.iter().map(|t| name(t.as_deref())).collect();
    confirm(
        "registry logout",
        yes,
        &format!(
            "Remove the {} credential from {} on: {}",
            reg.host,
            job.path,
            nodes.join(", ")
        ),
    )?;

    let mut rows = Rows::new(&["NODE", "RESULT"]);
    let mut failed = 0;
    for (target, node) in job.targets.iter().zip(nodes) {
        let removed = edit(job, target.as_deref(), |auths| {
            let before = auths.len();
            auths.retain(|key, _| canonical(key) != reg.host);
            auths.len() != before
        })
        .await;
        let ok = removed.is_ok();
        let removed = report_failure(removed, &mut failed);
        let result = match (ok, removed) {
            (false, _) => "failed",
            (true, true) => "removed",
            (true, false) => "not logged in",
        };
        rows.push(
            vec![node.clone(), result.into()],
            json!({ "node": node, "registry": reg.host, "result": result }),
        );
    }
    finish(rows, failed, job, format, quiet)
}

async fn list(job: &Job, format: Format, quiet: bool) -> Result<(), CtlError> {
    let mut rows = Rows::new(&["NODE", "REGISTRY", "USERNAME"]).with_id_column(1);
    let mut failed = 0;
    for target in &job.targets {
        let node = name(target.as_deref());
        let doc = read(job, target.as_deref()).await;
        let Some(mut doc) = report_failure(doc.map(Some), &mut failed) else {
            continue;
        };
        let auths = match auths(&mut doc, job, target.as_deref()) {
            Ok(auths) => auths,
            Err(err) => {
                report_failure::<()>(Err(err), &mut failed);
                continue;
            }
        };
        for (key, entry) in auths.iter() {
            // Only the part before `:` is decoded into the output.
            let user = entry["auth"]
                .as_str()
                .and_then(|auth| BASE64.decode(auth).ok())
                .and_then(|bytes| {
                    let colon = bytes.iter().position(|&b| b == b':')?;
                    String::from_utf8(bytes[..colon].to_vec()).ok()
                })
                .unwrap_or_else(|| "-".to_string());
            rows.push(
                vec![node.clone(), key.clone(), user.clone()],
                json!({ "node": node, "registry": key, "username": user }),
            );
        }
    }
    output::print(format, quiet, &rows)?;
    failures(failed, job)
}

/// Prints a target's error and counts it; the other targets still run.
fn report_failure<T: Default>(result: Result<T, CtlError>, failed: &mut usize) -> T {
    result.unwrap_or_else(|err| {
        eprintln!("error: {err}");
        if let Some(hint) = err.hint() {
            eprintln!("hint: {hint}");
        }
        *failed += 1;
        T::default()
    })
}

/// Prints a change's rows (nothing with `--quiet`) and the overall result.
fn finish(
    rows: Rows,
    failed: usize,
    job: &Job,
    format: Format,
    quiet: bool,
) -> Result<(), CtlError> {
    if !quiet {
        output::print(format, false, &rows)?;
    }
    failures(failed, job)
}

fn failures(failed: usize, job: &Job) -> Result<(), CtlError> {
    match failed {
        0 => Ok(()),
        n => Err(CtlError::Halted(format!(
            "{n} of {} target(s) failed; see the errors above",
            job.targets.len()
        ))),
    }
}

fn prompt_username() -> Result<String, CtlError> {
    let stdin = std::io::stdin();
    if !stdin.is_terminal() {
        return Err(CtlError::Usage(
            "pass --username when stdin is not a terminal".to_string(),
        ));
    }
    eprint!("Username: ");
    let mut user = String::new();
    stdin
        .lock()
        .read_line(&mut user)
        .map_err(|e| CtlError::Usage(format!("cannot read the username: {e}")))?;
    Ok(user.trim().to_string())
}

/// `--key-file`, else stdin when it is not a terminal, else a no-echo prompt.
fn credential(host: &str, login: &LoginArgs) -> Result<String, CtlError> {
    let read_err = |what: String| move |e: std::io::Error| CtlError::Usage(format!("{what}: {e}"));
    let (secret, missing) = if let Some(path) = &login.key_file {
        let what = format!("cannot read {}", path.display());
        (
            std::fs::read_to_string(path).map_err(read_err(what))?,
            format!("the key file {} is empty", path.display()),
        )
    } else if !std::io::stdin().is_terminal() {
        let mut secret = String::new();
        std::io::stdin()
            .read_to_string(&mut secret)
            .map_err(read_err("cannot read stdin".to_string()))?;
        (
            secret,
            format!(
                "no credential on stdin for {host}\n\
                 hint: run in a terminal to be prompted, or pipe the token to stdin"
            ),
        )
    } else {
        (
            rpassword::prompt_password("Token: ")
                .map_err(read_err("cannot read the token".to_string()))?,
            format!("no credential given for {host}"),
        )
    };
    let secret = secret.trim_end_matches(['\r', '\n']);
    if secret.is_empty() {
        return Err(CtlError::Usage(missing));
    }
    Ok(secret.to_string())
}

/// Reads the file on `target`, applies `change` to its `auths` and writes it
/// back when `change` returns true. Every other key is kept.
async fn edit(
    job: &Job,
    target: Option<&str>,
    change: impl FnOnce(&mut Map<String, Value>) -> bool,
) -> Result<bool, CtlError> {
    let mut doc = read(job, target).await?;
    if !change(auths(&mut doc, job, target)?) {
        return Ok(false);
    }
    let mut bytes = serde_json::to_vec_pretty(&doc)?;
    bytes.push(b'\n');
    let out = sh(job, target, WRITE, Some(&bytes)).await?;
    if !out.status.success() {
        return Err(file_error("write", job, target, &out));
    }
    if let Some(owner) = String::from_utf8_lossy(&out.stdout)
        .trim()
        .strip_prefix("created ")
    {
        eprintln!(
            "note: created {} on {}, owner {owner}, mode 0600; the Minga service must run as \
             that user or as root to read it",
            job.path,
            name(target)
        );
    }
    Ok(true)
}

/// The file as JSON; a missing or empty file is `{}`.
async fn read(job: &Job, target: Option<&str>) -> Result<Value, CtlError> {
    let out = sh(job, target, READ, None).await?;
    if !out.status.success() {
        return Err(file_error("read", job, target, &out));
    }
    if out.stdout.iter().all(u8::is_ascii_whitespace) {
        return Ok(json!({}));
    }
    serde_json::from_slice::<Value>(&out.stdout)
        .ok()
        .filter(Value::is_object)
        .ok_or_else(|| not_json(job, target))
}

/// The `auths` object, added when missing.
fn auths<'a>(
    doc: &'a mut Value,
    job: &Job,
    target: Option<&str>,
) -> Result<&'a mut Map<String, Value>, CtlError> {
    doc.as_object_mut()
        .ok_or_else(|| not_json(job, target))?
        .entry("auths")
        .or_insert_with(|| json!({}))
        .as_object_mut()
        .ok_or_else(|| not_json(job, target))
}

fn not_json(job: &Job, target: Option<&str>) -> CtlError {
    CtlError::Halted(format!(
        "{} on {} is not valid JSON; left unchanged",
        job.path,
        name(target)
    ))
}

fn file_error(verb: &str, job: &Job, target: Option<&str>, out: &RemoteOutput) -> CtlError {
    let text = String::from_utf8_lossy(&out.stderr);
    let detail = text
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        // `cat: /path: Permission denied` → `Permission denied`
        .and_then(|line| line.rsplit(": ").next())
        .map(|line| {
            line.chars()
                .map(|c| if c.is_control() { '?' } else { c })
                .collect()
        })
        .unwrap_or_else(|| format!("exit status {}", out.status));
    CtlError::Halted(format!(
        "cannot {verb} {} on {}: {detail}\n\
         hint: the SSH user needs read and write access to the file; minga-ctl does not use sudo",
        job.path,
        name(target)
    ))
}

/// Runs `script` with the file as `$1`, over ssh or with the local `sh`.
async fn sh(
    job: &Job,
    target: Option<&str>,
    script: &'static str,
    stdin: Option<&[u8]>,
) -> Result<RemoteOutput, CtlError> {
    if let Some(target) = target {
        let argv = [
            RemoteArg::Fixed("sh"),
            RemoteArg::Fixed("-c"),
            RemoteArg::Script(script),
            RemoteArg::Fixed("sh"),
            RemoteArg::Value(&job.path),
        ];
        return transport::remote_exec(target, &argv, stdin, job.debug).await;
    }
    let local = |e: std::io::Error| CtlError::Halted(format!("cannot run sh: {e}"));
    let mut child = tokio::process::Command::new("sh")
        .args(["-c", script, "sh", &job.path])
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(local)?;
    let pipe = child.stdin.take();
    let write = async move {
        if let (Some(mut pipe), Some(bytes)) = (pipe, stdin) {
            pipe.write_all(bytes).await?;
        }
        Ok::<_, std::io::Error>(())
    };
    let (written, output) = tokio::join!(write, child.wait_with_output());
    let output = output.map_err(local)?;
    written.map_err(local)?;
    Ok(RemoteOutput {
        status: output.status,
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_path_note_names_the_file_and_the_fix() {
        let note = default_path_note(DEFAULT_AUTH_FILE);
        assert!(note.contains("/root/.docker/config.json"), "{note}");
        assert!(note.contains("DOCKER_CONFIG"), "{note}");
        assert!(note.contains("registry_auth_file"), "{note}");
    }

    #[test]
    fn short_names_resolve_to_hosts() {
        assert_eq!(registry("dockerhub").unwrap().host, "docker.io");
        assert_eq!(registry("github").unwrap().host, "ghcr.io");
        assert!(registry("europe-west1-docker.pkg.dev").unwrap().google);
        assert!(registry("eu.gcr.io").unwrap().google);
        assert!(!registry("registry.example.com:5000").unwrap().google);
        assert_eq!(registry("index.docker.io").unwrap().host, "docker.io");
        assert_eq!(registry("GHCR.IO").unwrap().host, "ghcr.io");
        assert_eq!(registry("Reg.Example.com").unwrap().host, "reg.example.com");
        for bad in ["-x", "a b", "host:", "host:12a", "a/b", "'"] {
            assert!(registry(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn usernames_cannot_hold_a_colon() {
        assert!(validate_username("acme-deploy").is_ok());
        assert!(validate_username("_json_key").is_ok());
        assert!(validate_username("a:b").is_err());
        assert!(validate_username("").is_err());
    }

    #[test]
    fn docker_login_keys_match_like_minga() {
        assert_eq!(canonical("https://index.docker.io/v1/"), "docker.io");
        assert_eq!(canonical("https://ghcr.io"), "ghcr.io");
        assert_eq!(canonical("GHCR.IO"), "ghcr.io");
        assert_eq!(
            canonical("registry.example.com:5000"),
            "registry.example.com:5000"
        );
    }
}
