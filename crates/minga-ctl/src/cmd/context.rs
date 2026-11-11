//! `minga-ctl context` — named contexts.

use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::config::{self, Context, TokenSource};
use crate::error::CtlError;
use crate::output::{self, Format, Rows};
use crate::transport;

/// Context commands. Local file only; no API call.
#[derive(Debug, Subcommand)]
pub enum ContextCommand {
    /// Add a context.
    Add(Box<AddArgs>),
    /// Switch the current context.
    Use {
        /// Name of the context.
        name: String,
    },
    /// List the contexts.
    List,
    /// Delete a context.
    Delete {
        /// Name of the context.
        name: String,
    },
}

/// Arguments of `minga-ctl context add`.
#[derive(Debug, Args)]
pub struct AddArgs {
    /// Name of the context.
    pub name: String,
    /// SSH target for the API tunnel and remote commands.
    #[arg(long)]
    pub ssh: Option<String>,
    /// API URL. With `--ssh`, as seen from the SSH target.
    #[arg(long, default_value = config::DEFAULT_ENDPOINT)]
    pub endpoint: String,
    /// File on the SSH target holding the API token.
    #[arg(long)]
    pub remote_token_file: Option<String>,
    /// Local command that prints the token; split on whitespace, run
    /// without a shell.
    #[arg(long)]
    pub token_command: Option<String>,
    /// PEM CA certificate to trust for an `https://` endpoint.
    #[arg(long)]
    pub tls_cert: Option<PathBuf>,
    /// Metrics URL. With `--ssh`, as seen from the SSH target.
    #[arg(long)]
    pub metrics_endpoint: Option<String>,
    /// NATS URL for logs, as seen from where `minga logs` runs.
    #[arg(long)]
    pub nats_url: Option<String>,
    /// SSH target for node-local commands; may be repeated.
    #[arg(long = "node")]
    pub nodes: Vec<String>,
    /// Docker credential file on the node that Minga reads when pulling.
    #[arg(long)]
    pub registry_auth_file: Option<String>,
    /// Make this the current context.
    #[arg(long = "use")]
    pub use_current: bool,
}

/// Runs a context command.
pub fn run(cmd: &ContextCommand, format: Format, quiet: bool) -> Result<(), CtlError> {
    match cmd {
        ContextCommand::Add(args) => add(args, quiet),
        ContextCommand::Use { name } => use_context(name, quiet),
        ContextCommand::List => list(format, quiet),
        ContextCommand::Delete { name } => delete(name, quiet),
    }
}

/// Adds a context and selects it when it is the first one.
fn add(args: &AddArgs, quiet: bool) -> Result<(), CtlError> {
    let AddArgs {
        name,
        ssh,
        endpoint,
        remote_token_file,
        token_command,
        tls_cert,
        metrics_endpoint,
        nats_url,
        nodes,
        registry_auth_file,
        use_current,
    } = args;

    if name.is_empty() {
        return Err(CtlError::Usage("a context needs a name".to_string()));
    }
    for target in ssh.iter().chain(nodes) {
        transport::validate_target(target)?;
    }
    if let Some(path) = remote_token_file {
        if ssh.is_none() {
            return Err(CtlError::Usage(
                "--remote-token-file needs --ssh: the file is read on the SSH target".to_string(),
            ));
        }
        transport::validate_remote_path(path)?;
    }
    if ssh.is_some() {
        // The same destination rules `connect` applies to the tunnel.
        for url in std::iter::once(endpoint).chain(metrics_endpoint) {
            transport::forward_dest(url)?;
        }
    } else {
        // The plain-HTTP rule is checked at add time when there is no SSH
        // target to tunnel through.
        config::check_endpoint_policy(None, endpoint)?;
    }

    let command = token_command.as_ref().map(|raw| {
        raw.split_whitespace()
            .map(str::to_string)
            .collect::<Vec<String>>()
    });
    if let Some(command) = &command {
        if command.is_empty() {
            return Err(CtlError::Usage(
                "a token command needs a program to run".to_string(),
            ));
        }
    }

    let context = Context {
        ssh: ssh.clone(),
        api_endpoint: Some(endpoint.clone()),
        remote_token_file: remote_token_file.clone(),
        token_command: command,
        tls_cert: tls_cert.clone(),
        metrics_endpoint: metrics_endpoint.clone(),
        nats_url: nats_url.clone(),
        nodes: nodes.clone(),
        registry_auth_file: registry_auth_file.clone(),
    };

    let selected = config::update(|config| {
        if config.contexts.contains_key(name) {
            return Err(CtlError::Usage(format!(
                "context `{name}` already exists; use `minga-ctl context delete {name}` first"
            )));
        }
        let selected = *use_current || config.contexts.is_empty();
        config.contexts.insert(name.clone(), context);
        if selected {
            config.current_context = Some(name.clone());
        }
        Ok(selected)
    })?;

    if selected {
        output::say(quiet, &format!("context `{name}` added and selected"))
    } else {
        output::say(quiet, &format!("context `{name}` added"))
    }
}

/// Switches the current context.
fn use_context(name: &str, quiet: bool) -> Result<(), CtlError> {
    config::update(|config| {
        if !config.contexts.contains_key(name) {
            return Err(CtlError::ContextNotFound {
                name: name.to_string(),
            });
        }
        config.current_context = Some(name.to_string());
        Ok(())
    })?;
    output::say(quiet, &format!("context `{name}` selected"))
}

/// Deletes a context; deleting the current one clears the selection.
fn delete(name: &str, quiet: bool) -> Result<(), CtlError> {
    config::update(|config| {
        if config.contexts.remove(name).is_none() {
            return Err(CtlError::ContextNotFound {
                name: name.to_string(),
            });
        }
        if config.current_context.as_deref() == Some(name) {
            config.current_context = None;
        }
        Ok(())
    })?;
    output::say(quiet, &format!("context `{name}` deleted"))
}

/// Lists the contexts.
fn list(format: Format, quiet: bool) -> Result<(), CtlError> {
    let config = config::load()?;
    let mut rows =
        Rows::new(&["CURRENT", "NAME", "TRANSPORT", "ENDPOINT", "TOKEN"]).with_id_column(1);
    for (name, context) in &config.contexts {
        let current = config.current_context.as_deref() == Some(name.as_str());
        let (transport, ssh) = match &context.ssh {
            Some(ssh) => (format!("ssh {ssh}"), Some(ssh.as_str())),
            None => ("direct".to_string(), None),
        };
        let endpoint = context
            .api_endpoint
            .clone()
            .unwrap_or_else(|| config::DEFAULT_ENDPOINT.to_string());
        let token = match token_label(context) {
            TokenSource::Env => "env".to_string(),
            TokenSource::Command(_) => "command".to_string(),
            TokenSource::RemoteFile(_) => "remote file".to_string(),
            TokenSource::None => "none".to_string(),
        };
        rows.push(
            vec![
                if current {
                    "*".to_string()
                } else {
                    String::new()
                },
                name.clone(),
                transport.clone(),
                endpoint.clone(),
                token.clone(),
            ],
            output::context_value(current, name, &transport, ssh, &endpoint, &token),
        );
    }
    output::print(format, quiet, &rows)?;
    Ok(())
}

/// How a context gets its token, for the list output.
fn token_label(context: &Context) -> TokenSource {
    if let Some(command) = &context.token_command {
        TokenSource::Command(command.clone())
    } else if let Some(file) = &context.remote_token_file {
        TokenSource::RemoteFile(file.clone())
    } else {
        TokenSource::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plain_http_remote_endpoint_is_refused_at_add_time() {
        let err = config::check_endpoint_policy(None, "http://10.0.0.1:2375").unwrap_err();
        assert!(matches!(err, CtlError::PlainHttp { .. }), "{err:?}");
    }

    #[test]
    fn a_token_command_is_split_on_whitespace() {
        let command = "op read op://infra/minga/token";
        let parts: Vec<String> = command.split_whitespace().map(str::to_string).collect();
        assert_eq!(parts, vec!["op", "read", "op://infra/minga/token"]);
    }

    #[test]
    fn a_context_with_a_remote_token_file_is_labelled() {
        let context = Context {
            remote_token_file: Some("/etc/minga/api-token".to_string()),
            ..Context::default()
        };
        assert!(matches!(token_label(&context), TokenSource::RemoteFile(_)));
    }

    #[test]
    fn a_context_without_a_token_is_labelled_none() {
        assert!(matches!(
            token_label(&Context::default()),
            TokenSource::None
        ));
    }
}
