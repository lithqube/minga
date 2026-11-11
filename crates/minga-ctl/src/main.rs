//! `minga-ctl` — companion CLI for Minga: contexts, stacks, services, health.

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use minga_ctl::cmd;
use minga_ctl::error::CtlError;
use minga_ctl::output::Format;

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
    match &cli.command {
        Command::Context(cmd) => cmd::context::run(cmd, cli.format(), cli.quiet),
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
