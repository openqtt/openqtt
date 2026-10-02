//! `openqtt convert`: the files of OpenQTT 1.x into their 2.0 formats (report R2, rule 30).
//!
//! Each subcommand writes the converted file to stdout and its notes to stderr, one per line,
//! as `<file>:<line>: <note>`. The status is 0 when the file was converted, 1 when it could not
//! be or, with `--strict`, when a rule conflicts with R2 rules 13 to 16, and 2 for a command
//! line clap refuses.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Subcommand;
use openqtt_auth::acl::contract::Contract;
use openqtt_auth::convert::{Note, convert_acl, convert_authn};

#[derive(Debug, Subcommand)]
pub(crate) enum ConvertCommand {
    /// Convert a 1.x acl.conf into the 2.0 ACL format (docs/spec/acl.md), with the same
    /// decisions, and name every rule that conflicts with R2 rules 13 to 16.
    Acl {
        /// The acl.conf to convert.
        #[arg(value_name = "ACL_CONF")]
        file: PathBuf,
        /// Write nothing, and fail, if a rule conflicts with R2 rules 13 to 16.
        #[arg(long)]
        strict: bool,
        /// A filter devices receive commands on, as their rules see it, before the mountpoint
        /// (R2 rule 13). Repeat it for more; commands/# without it.
        #[arg(long = "commands", value_name = "FILTER")]
        commands: Vec<String>,
        /// The user name prefix only service credentials carry (R2 rule 15).
        #[arg(long, value_name = "PREFIX")]
        service_prefix: Option<String>,
        /// A service credential named exactly (R2 rule 15). Repeat it for more.
        #[arg(long = "service", value_name = "NAME")]
        services: Vec<String>,
    },
    /// Convert a 1.x user file, user_id,password,is_superuser with plain passwords, into a 2.0
    /// bootstrap file in the hashed format. A superuser is refused.
    Authn {
        /// The CSV file to convert.
        #[arg(value_name = "AUTHN_CSV")]
        file: PathBuf,
    },
}

pub(crate) fn run(command: ConvertCommand) -> ExitCode {
    match command {
        ConvertCommand::Acl {
            file,
            strict,
            commands,
            service_prefix,
            services,
        } => {
            let Some(source) = read(&file, "acl") else {
                return ExitCode::FAILURE;
            };
            let mut contract = Contract {
                service_prefix,
                services,
                ..Contract::default()
            };
            if !commands.is_empty() {
                contract.commands = commands;
            }
            let converted = match convert_acl(&source, &contract) {
                Ok(converted) => converted,
                Err(error) => return refuse(&file, &error.to_string()),
            };
            report(&file, "warning: ", &converted.warnings);
            report(&file, "conflict: ", &converted.conflicts);
            if strict && !converted.conflicts.is_empty() {
                eprintln!(
                    "openqtt convert acl: {} conflicts with R2 rules 13 to 16, and --strict \
                     refuses them; nothing was written",
                    converted.conflicts.len()
                );
                return ExitCode::FAILURE;
            }
            write_stdout(&converted.text)
        }
        ConvertCommand::Authn { file } => {
            let Some(source) = read(&file, "authn") else {
                return ExitCode::FAILURE;
            };
            match convert_authn(&source) {
                Ok(converted) => {
                    report(&file, "warning: ", &converted.warnings);
                    write_stdout(&converted.text)
                }
                Err(error) => refuse(&file, &error.to_string()),
            }
        }
    }
}

fn read(file: &Path, command: &str) -> Option<String> {
    match std::fs::read_to_string(file) {
        Ok(text) => Some(text),
        Err(error) => {
            eprintln!(
                "openqtt convert {command}: cannot read {}: {error}",
                file.display()
            );
            None
        }
    }
}

/// Every problem that kept the file from being converted, one per line.
fn refuse(file: &Path, problems: &str) -> ExitCode {
    for problem in problems.lines() {
        match problem.strip_prefix("line ") {
            Some(rest) => eprintln!("{}:{}", file.display(), rest.replacen(": ", ": error: ", 1)),
            None => eprintln!("{}: error: {problem}", file.display()),
        }
    }
    ExitCode::FAILURE
}

fn report(file: &Path, kind: &str, notes: &[Note]) {
    for note in notes {
        if note.line == 0 {
            eprintln!("{}: {kind}{}", file.display(), note.message);
        } else {
            eprintln!("{}:{}: {kind}{}", file.display(), note.line, note.message);
        }
    }
}

/// Writes the converted file to stdout, where the operator redirects it.
fn write_stdout(text: &str) -> ExitCode {
    let mut stdout = std::io::stdout().lock();
    match stdout
        .write_all(text.as_bytes())
        .and_then(|()| stdout.flush())
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("openqtt convert: cannot write to stdout: {error}");
            ExitCode::FAILURE
        }
    }
}
