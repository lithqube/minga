use clap::{Parser, Subcommand};

/// Swarm-NG: single-binary orchestrator for edge, on-prem and air-gapped sites.
#[derive(Parser)]
#[command(name = "sng", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run this node in the manager role.
    Manager,
    /// Run this node in the worker role.
    Worker,
    /// Deploy a Compose file as a stack.
    Deploy {
        /// Path to the Compose file.
        #[arg(short = 'c', long, default_value = "compose.yaml")]
        compose_file: String,
        /// Stack name.
        stack: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    match Cli::parse().command {
        Command::Manager => tracing::info!("manager role: not implemented yet"),
        Command::Worker => tracing::info!("worker role: not implemented yet"),
        Command::Deploy { compose_file, stack } => {
            tracing::info!(%compose_file, %stack, "deploy: not implemented yet")
        }
    }
    Ok(())
}
