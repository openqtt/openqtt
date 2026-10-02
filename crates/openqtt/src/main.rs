//! The `openqtt` binary: one executable for every role.
//!
//! `openqtt run` will start the roles this process is configured for: edge, router, log and
//! admin, any of them or all of them. `openqtt config` checks and prints the configuration. The
//! other subcommands are the operator's tools; none is implemented yet, and each says so and
//! exits with status 1.
//!
//! Settings come from openqtt-config, the one crate that reads the environment, from the file
//! `--config` names. The exit status is 0 for success, 1 for a failure while running, and 2 for
//! a command line or a configuration the process cannot start with.
//!
//! `main` installs aws-lc-rs as the process-wide crypto provider before anything builds a TLS
//! configuration: the OTLP exporter's today, QUIC's when the transport arrives. `make layers`
//! refuses ring in this binary's tree, so there is only ever the one provider.

#![expect(
    clippy::print_stderr,
    reason = "a command line reports to its operator on stderr"
)]

mod logging;
mod telemetry;

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use openqtt_config::{Error, Loaded, Settings, Sources};

/// Exit status for configuration the process cannot start with. clap uses the same status for a
/// command line it cannot parse.
const EXIT_CONFIG: u8 = 2;

/// OpenQTT, an MQTT 5 broker over QUIC.
#[derive(Debug, Parser)]
#[command(name = "openqtt", version, about)]
struct Cli {
    /// The configuration file. Without it, OPENQTT_CONFIG names the file, if it is set; without
    /// either, no file is read.
    #[arg(long, global = true, value_name = "PATH")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the broker roles this process is configured for.
    Run,
    /// Operate a running cluster through its admin API.
    Ctl,
    /// Check or print the configuration.
    #[command(subcommand)]
    Config(ConfigCommand),
    /// Convert 1.x ACL and password files to their 2.0 formats.
    Convert,
    /// Migrate state, such as retained messages, from a 1.x broker.
    Migrate,
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Load the configuration, apply every rule, and check that every file it names can be
    /// read. Each problem is reported, and the status is 2 if there is any.
    Check,
    /// Print what the file and the variables set, as TOML.
    Print {
        /// Print every setting instead, defaults included, as a file that loads back to the
        /// same settings.
        #[arg(long)]
        effective: bool,
    },
}

impl Command {
    fn name(&self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Ctl => "ctl",
            Self::Config(_) => "config",
            Self::Convert => "convert",
            Self::Migrate => "migrate",
        }
    }
}

fn main() -> ExitCode {
    // Fails only when a provider is already installed, which nothing does before this line.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cli = Cli::parse();
    match cli.command {
        Command::Run => run(cli.config),
        Command::Config(ConfigCommand::Check) => check(cli.config),
        Command::Config(ConfigCommand::Print { effective }) => print(cli.config, effective),
        other => {
            eprintln!("openqtt {}: not implemented yet", other.name());
            ExitCode::FAILURE
        }
    }
}

/// Starts the roles. Today it loads and checks the settings, starts logging and metrics, and
/// stops there.
fn run(config: Option<PathBuf>) -> ExitCode {
    let settings = match load(config).and_then(|loaded| {
        let settings = loaded.into_settings();
        settings.validate().map(|()| settings)
    }) {
        Ok(settings) => settings,
        Err(error) => return refuse(&error),
    };
    if let Err(error) = logging::init(&settings.observability) {
        eprintln!("openqtt: {error}");
        return ExitCode::from(EXIT_CONFIG);
    }
    let telemetry = match telemetry::start(&settings) {
        Ok(telemetry) => telemetry,
        Err(error) => {
            eprintln!("openqtt: {error}");
            return ExitCode::from(EXIT_CONFIG);
        }
    };
    tracing::debug!(roles = ?settings.cluster.roles, "loaded the configuration");
    eprintln!("openqtt run: not implemented yet");
    telemetry.shutdown();
    ExitCode::FAILURE
}

/// `openqtt config check`: every problem, from every check, at once.
fn check(config: Option<PathBuf>) -> ExitCode {
    let loaded = match load(config) {
        Ok(loaded) => loaded,
        Err(error) => return refuse(&error),
    };
    let settings = loaded.settings();
    let mut problems: Vec<String> = Vec::new();
    if let Err(error) = settings.validate() {
        problems.extend(error.problems().iter().map(ToString::to_string));
    }
    match settings.check_files() {
        // What the files hold is worth reading only once they can all be opened.
        Ok(()) => problems.extend(telemetry::check(settings)),
        Err(error) => problems.extend(error.problems().iter().map(ToString::to_string)),
    }
    if let Err(problem) = logging::filter(&settings.observability.log_level) {
        problems.push(problem);
    }
    if !problems.is_empty() {
        for problem in problems {
            eprintln!("openqtt: {problem}");
        }
        return ExitCode::from(EXIT_CONFIG);
    }
    let source = loaded.file().map_or_else(
        || "no file, the defaults and the variables".to_owned(),
        |file| file.display().to_string(),
    );
    write_stdout(&format!("the configuration is valid: {source}\n"))
}

/// `openqtt config print [--effective]`.
fn print(config: Option<PathBuf>, effective: bool) -> ExitCode {
    match load(config) {
        Ok(loaded) if effective => write_stdout(&loaded.settings().to_toml()),
        Ok(loaded) => write_stdout(&loaded.explicit_toml()),
        Err(error) => refuse(&error),
    }
}

fn load(config: Option<PathBuf>) -> Result<Loaded, Error> {
    Settings::load(&Sources::from_process(config))
}

/// Reports each problem on a line of its own, and exits as a configuration the process cannot
/// start with.
fn refuse(error: &Error) -> ExitCode {
    for problem in error.problems() {
        eprintln!("openqtt: {problem}");
    }
    ExitCode::from(EXIT_CONFIG)
}

/// Writes what a command answers to stdout, where a script reads it.
fn write_stdout(text: &str) -> ExitCode {
    let mut stdout = std::io::stdout().lock();
    match stdout
        .write_all(text.as_bytes())
        .and_then(|()| stdout.flush())
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("openqtt: cannot write to stdout: {error}");
            ExitCode::FAILURE
        }
    }
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
