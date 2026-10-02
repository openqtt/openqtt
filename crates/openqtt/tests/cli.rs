//! The binary as an operator meets it: help, the subcommands, and a bad log filter.

use std::process::Command;

const SUBCOMMANDS: [&str; 5] = ["run", "ctl", "config", "convert", "migrate"];

fn openqtt() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_openqtt"));
    command.env_remove("OPENQTT_LOG");
    command
}

#[test]
fn help_lists_every_subcommand() {
    let out = openqtt().arg("--help").output().unwrap();
    assert!(out.status.success());
    let help = String::from_utf8(out.stdout).unwrap();
    for name in SUBCOMMANDS {
        assert!(
            help.contains(name),
            "--help does not mention {name}:\n{help}"
        );
    }
}

#[test]
fn every_subcommand_says_it_is_not_implemented_and_fails() {
    for name in SUBCOMMANDS {
        let out = openqtt().arg(name).output().unwrap();
        assert_eq!(out.status.code(), Some(1), "openqtt {name}");
        let stderr = String::from_utf8(out.stderr).unwrap();
        assert!(
            stderr.contains(&format!("openqtt {name}: not implemented yet")),
            "openqtt {name}: {stderr}"
        );
    }
}

#[test]
fn a_log_filter_that_does_not_parse_stops_the_process() {
    let out = openqtt()
        .arg("run")
        .env("OPENQTT_LOG", "openqtt=loudest")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("OPENQTT_LOG"), "{stderr}");
}

#[test]
fn a_valid_log_filter_is_used() {
    let out = openqtt()
        .arg("run")
        .env("OPENQTT_LOG", "debug")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).unwrap();
    assert!(stderr.contains("parsed the command line"), "{stderr}");
}
