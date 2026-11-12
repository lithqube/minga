//! `minga-ctl` — companion CLI for Minga: contexts, stacks, services, health.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use minga_ctl::api;
use minga_ctl::error::CtlError;
use minga_ctl::output::Format;
use minga_ctl::transport::{self, Connection};
use minga_ctl::{cmd, config, state};

/// Companion CLI for Minga: contexts, stack and service operations, health,
/// metrics and release management.
#[derive(Debug, Parser)]
#[command(name = "minga-ctl", version, about)]
struct Cli {
    /// Context to use for this command.
    #[arg(long, global = true)]
    context: Option<String>,
    /// Output format.
    #[arg(short = 'o', long, global = true, value_enum, default_value_t = Format::Table)]
    output: Format,
    /// Same as `--output json`.
    #[arg(long, global = true)]
    json: bool,
    /// Print IDs only; changes print nothing.
    #[arg(short, long, global = true)]
    quiet: bool,
    /// Print request and ssh traces on stderr, with credentials redacted.
    #[arg(long, global = true)]
    debug: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Manage named contexts.
    #[command(subcommand)]
    Context(cmd::context::ContextCommand),
    /// List, inspect and remove stacks.
    #[command(subcommand)]
    Stack(cmd::stack::StackCommand),
    /// List services, scale, update and roll them back.
    #[command(subcommand)]
    Service(cmd::service::ServiceCommand),
    /// List secret metadata.
    #[command(subcommand)]
    Secret(cmd::secret::SecretCommand),
    /// Check the API, and optionally a stack's services.
    Health(cmd::health::HealthArgs),
    /// Show the Prometheus metrics the cluster reports.
    Metrics,
    /// Log nodes in to private container registries.
    #[command(subcommand)]
    Registry(cmd::registry::RegistryCommand),
}

/// The output format after `--json` is folded in.
impl Cli {
    fn format(&self) -> Format {
        if self.json {
            Format::Json
        } else {
            self.output
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            if let Some(hint) = err.hint() {
                eprintln!("hint: {hint}");
            }
            ExitCode::from(err.exit_code())
        }
    }
}

/// Runs the selected command.
fn run(cli: &Cli) -> Result<(), CtlError> {
    let (format, quiet) = (cli.format(), cli.quiet);
    match &cli.command {
        Command::Context(cmd) => cmd::context::run(cmd, format, quiet),
        Command::Stack(cmd) => api::block_on(async {
            let resolved = resolve(cli)?;
            let result = async {
                let conn = transport::connect(&resolved, cli.debug).await?;
                cmd::stack::run(cmd, &conn, format, quiet).await
            }
            .await;
            audit(&resolved, cmd.audit(), &result);
            result
        }),
        Command::Service(cmd) => api::block_on(async {
            let resolved = resolve(cli)?;
            let result = async {
                let conn = transport::connect(&resolved, cli.debug).await?;
                cmd::service::run(cmd, &conn, format, quiet).await
            }
            .await;
            audit(&resolved, cmd.audit(), &result);
            result
        }),
        Command::Secret(cmd) => api::block_on(async {
            let conn = connect(cli).await?;
            cmd::secret::run(cmd, &conn, format, quiet).await
        }),
        Command::Health(args) => api::block_on(async {
            let conn = connect(cli).await?;
            cmd::health::run(args, &conn, format, quiet).await
        }),
        Command::Metrics => api::block_on(async {
            let conn = connect(cli).await?;
            cmd::metrics::run(&conn, format, quiet).await
        }),
        Command::Registry(cmd) => api::block_on(cmd::registry::run(
            cmd,
            &resolve(cli)?,
            format,
            quiet,
            cli.debug,
        )),
    }
}

/// Connects to the selected context. The connection (and, once step 3 lands,
/// its SSH tunnel) lives until the command returns.
async fn connect(cli: &Cli) -> Result<Connection, CtlError> {
    transport::connect(&resolve(cli)?, cli.debug).await
}

/// Loads the config and resolves the selected context.
fn resolve(cli: &Cli) -> Result<config::Resolved, CtlError> {
    let config = config::load()?;
    config::resolve(&config, cli.context.as_deref())
}

/// Appends the audit line for a command that changes state (`entry` is
/// `None` for reads). A failed append is a warning: by then the change has
/// already happened or failed.
fn audit(
    resolved: &config::Resolved,
    entry: Option<(&str, serde_json::Value)>,
    result: &Result<(), CtlError>,
) {
    let Some((command, args)) = entry else {
        return;
    };
    let target = resolved
        .context
        .ssh
        .as_deref()
        .unwrap_or(&resolved.endpoint);
    if let Err(err) = state::audit(&resolved.name, target, command, &args, result) {
        eprintln!("warning: could not write the audit log: {err}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn the_cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn json_overrides_the_output_format() {
        let cli = Cli::parse_from(["minga-ctl", "--json", "context", "list"]);
        assert_eq!(cli.format(), Format::Json);
    }

    #[test]
    fn the_output_format_defaults_to_table() {
        let cli = Cli::parse_from(["minga-ctl", "context", "list"]);
        assert_eq!(cli.format(), Format::Table);
    }
}
