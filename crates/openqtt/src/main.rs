//! The `openqtt` binary: one executable for every role.
//!
//! `openqtt run` will start the roles this process is configured for: edge, router, log and
//! admin, any of them or all of them. The other subcommands are the operator's tools. Only
//! `convert` is implemented yet; each of the others says so and exits with status 1.
//!
//! When rustls arrives with QUIC, `main` installs aws-lc-rs as the process-wide crypto provider
//! before anything builds a TLS configuration. `make layers` already refuses ring in this
//! binary's tree, so there is only ever one provider to install.

#![expect(
    clippy::print_stderr,
    reason = "a command line reports to its operator on stderr"
)]

mod convert;

use std::process::ExitCode;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

/// The log filter when `OPENQTT_LOG` is unset.
const DEFAULT_LOG_FILTER: &str = "info";

/// Exit status for configuration the process cannot start with. clap uses the same status for a
/// command line it cannot parse.
const EXIT_CONFIG: u8 = 2;

/// OpenQTT, an MQTT 5 broker over QUIC.
#[derive(Debug, Parser)]
#[command(name = "openqtt", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the broker roles this process is configured for.
    Run,
    /// Operate a running cluster through its admin API.
    Ctl,
    /// Check or print the effective configuration.
    Config,
    /// Convert 1.x ACL and password files to their 2.0 formats.
    #[command(subcommand)]
    Convert(convert::ConvertCommand),
    /// Migrate state, such as retained messages, from a 1.x broker.
    Migrate,
}

impl Command {
    fn name(&self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Ctl => "ctl",
            Self::Config => "config",
            Self::Convert(_) => "convert",
            Self::Migrate => "migrate",
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if let Err(error) = init_logging() {
        eprintln!("openqtt: {error}");
        return ExitCode::from(EXIT_CONFIG);
    }
    let name = cli.command.name();
    tracing::debug!(command = name, "parsed the command line");
    if let Command::Convert(command) = cli.command {
        return convert::run(command);
    }
    eprintln!("openqtt {name}: not implemented yet");
    ExitCode::FAILURE
}

/// Logs to stderr, filtered by `OPENQTT_LOG` as read by `openqtt-config`. Not
/// `EnvFilter::from_default_env`, which would read `RUST_LOG` behind the configuration's back.
fn init_logging() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let directives = openqtt_config::log_filter()?;
    let directives = directives.as_deref().unwrap_or(DEFAULT_LOG_FILTER);
    let filter = EnvFilter::builder()
        .parse(directives)
        .map_err(|error| format!("{}: {error}", openqtt_config::LOG_VAR))?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init()
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::Cli;

    #[test]
    fn the_command_line_is_well_formed() {
        Cli::command().debug_assert();
    }
}
