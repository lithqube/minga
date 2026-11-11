//! `minga-ctl` — companion CLI for Minga: contexts, stacks, services, health.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use minga_ctl::api;
use minga_ctl::error::CtlError;
use minga_ctl::output::Format;
use minga_ctl::transport::{self, Connection};
use minga_ctl::{cmd, config};

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
    /// List stacks and their tasks.
    #[command(subcommand)]
    Stack(cmd::stack::StackCommand),
    /// List services and their tasks.
    #[command(subcommand)]
    Service(cmd::service::ServiceCommand),
    /// List secret metadata.
    #[command(subcommand)]
    Secret(cmd::secret::SecretCommand),
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
            let conn = connect(cli).await?;
            cmd::stack::run(cmd, &conn, format, quiet).await
        }),
        Command::Service(cmd) => api::block_on(async {
            let conn = connect(cli).await?;
            cmd::service::run(cmd, &conn, format, quiet).await
        }),
        Command::Secret(cmd) => api::block_on(async {
            let conn = connect(cli).await?;
            cmd::secret::run(cmd, &conn, format, quiet).await
        }),
    }
}

/// Connects to the selected context. The connection (and, once step 3 lands,
/// its SSH tunnel) lives until the command returns.
async fn connect(cli: &Cli) -> Result<Connection, CtlError> {
    let config = config::load()?;
    let resolved = config::resolve(&config, cli.context.as_deref())?;
    transport::connect(&resolved, cli.debug).await
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
