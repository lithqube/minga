use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use minga_compose::{DeployMode, LoadOptions, Loaded};

/// Minga: single-binary orchestrator for edge, on-prem and air-gapped sites.
#[derive(Parser)]
#[command(name = "minga", version)]
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
    /// Check a Compose file and print what would be deployed.
    Validate(ComposeArgs),
    /// Deploy a Compose file as a stack.
    Deploy {
        #[command(flatten)]
        compose: ComposeArgs,
        /// Stack name.
        stack: String,
    },
}

#[derive(Args)]
struct ComposeArgs {
    /// Path to the Compose file.
    #[arg(short = 'c', long, default_value = "compose.yaml")]
    compose_file: String,
    /// Profile to enable; repeat for several, or use `*` for all.
    #[arg(short, long = "profile")]
    profiles: Vec<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    match Cli::parse().command {
        Command::Manager => tracing::info!("manager role: not implemented yet"),
        Command::Worker => tracing::info!("worker role: not implemented yet"),
        Command::Validate(args) => {
            let Some(loaded) = load(&args) else {
                return ExitCode::FAILURE;
            };
            print_plan(&loaded);
        }
        Command::Deploy { compose, stack } => {
            let Some(loaded) = load(&compose) else {
                return ExitCode::FAILURE;
            };
            match minga_scheduler::translate_project(&loaded.project, &stack) {
                Ok(translated) => print_translated(&translated),
                Err(e) => {
                    eprintln!("error: {e}");
                    return ExitCode::FAILURE;
                }
            }
            tracing::info!(%stack, "cluster deployment: not implemented yet (translation only)");
        }
    }
    ExitCode::SUCCESS
}

/// Loads and validates the Compose file, printing warnings and errors to stderr.
fn load(args: &ComposeArgs) -> Option<Loaded> {
    let opts = LoadOptions {
        profiles: args.profiles.clone(),
        ..LoadOptions::from_process_env()
    };
    match minga_compose::load_path(&args.compose_file, &opts) {
        Ok(loaded) => {
            for w in &loaded.warnings {
                eprintln!("warning: {w}");
            }
            Some(loaded)
        }
        Err(errors) => {
            for e in errors.iter() {
                eprintln!("error: {e}");
            }
            eprintln!("{}: {} problem(s) found", args.compose_file, errors.0.len());
            None
        }
    }
}

fn print_plan(loaded: &Loaded) {
    let p = &loaded.project;
    if let Some(name) = &p.name {
        println!("project {name}");
    }
    println!("services in startup order:");
    for name in p.startup_order() {
        let svc = &p.services[name];
        let image = svc.image.as_deref().unwrap_or("-");
        let deploy = svc.deploy.clone().unwrap_or_default();
        let scale = match deploy.mode {
            DeployMode::Replicated => format!("{} replica(s)", deploy.replicas.unwrap_or(1)),
            DeployMode::Global => "global".to_string(),
            DeployMode::ReplicatedJob => "replicated job".to_string(),
            DeployMode::GlobalJob => "global job".to_string(),
        };
        let ports: Vec<String> = svc
            .ports
            .iter()
            .map(|port| match port.published {
                Some(published) => format!("{published}->{}/{}", port.target, port.protocol),
                None => format!("{}/{}", port.target, port.protocol),
            })
            .collect();
        let after: Vec<&str> = svc.depends_on.keys().map(String::as_str).collect();
        print!("  {name:<16} {image:<40} {scale}");
        if !ports.is_empty() {
            print!("  ports {}", ports.join(", "));
        }
        if !after.is_empty() {
            print!("  after {}", after.join(", "));
        }
        println!();
    }
    if !p.disabled_services.is_empty() {
        println!("disabled by profiles: {}", p.disabled_services.join(", "));
    }
}

/// Prints the state entities that a deploy would create.
fn print_translated(t: &minga_scheduler::TranslatedStack) {
    println!("services:");
    for svc in &t.services {
        let mode = match svc.mode {
            minga_state::types::ServiceMode::Replicated => {
                format!("{} replica(s)", svc.replicas)
            }
            minga_state::types::ServiceMode::Global => "global".to_string(),
        };
        let constraints = if svc.constraints.is_empty() {
            String::new()
        } else {
            format!("  constraints {}", svc.constraints.join(", "))
        };
        let resources = if svc.resources.cpus > 0.0 || svc.resources.memory_bytes > 0 {
            format!(
                "  resources cpu={} mem={}B",
                svc.resources.cpus, svc.resources.memory_bytes
            )
        } else {
            String::new()
        };
        println!(
            "  {:<24} {:<40} {}{}{}",
            svc.id, svc.image, mode, constraints, resources
        );
        if !svc.env.is_empty() {
            let env: Vec<String> = svc.env.iter().map(|(k, v)| format!("{k}={v}")).collect();
            println!("    env {}", env.join(" "));
        }
        if !svc.ports.is_empty() {
            let ports: Vec<String> = svc
                .ports
                .iter()
                .map(|p| match p.published {
                    Some(pub_) => format!("{pub_}->{}/{}", p.target, p.protocol),
                    None => format!("{}/{}", p.target, p.protocol),
                })
                .collect();
            println!("    ports {}", ports.join(", "));
        }
        if !svc.networks.is_empty() {
            println!("    networks {}", svc.networks.join(", "));
        }
    }
    if !t.networks.is_empty() {
        println!("networks:");
        for net in &t.networks {
            println!("  {:<24} driver={}", net.id, net.driver);
        }
    }
    if !t.volumes.is_empty() {
        println!("volumes:");
        for vol in &t.volumes {
            println!("  {:<24} driver={}", vol.id, vol.driver);
        }
    }
    if !t.secrets.is_empty() {
        println!("secrets:");
        for sec in &t.secrets {
            println!("  {}", sec.id);
        }
    }
    if !t.configs.is_empty() {
        println!("configs:");
        for cfg in &t.configs {
            println!("  {}", cfg.id);
        }
    }
}
