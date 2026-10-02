//! The binary as an operator meets it: help, the subcommands, a bad log filter, and the 1.x
//! converters.

use std::path::PathBuf;
use std::process::Command;

const SUBCOMMANDS: [&str; 5] = ["run", "ctl", "config", "convert", "migrate"];

/// The subcommands that are not built yet.
const NOT_IMPLEMENTED: [&str; 4] = ["run", "ctl", "config", "migrate"];

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
fn every_subcommand_not_built_says_so_and_fails() {
    for name in NOT_IMPLEMENTED {
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

/// A file written for one test, in the directory Cargo gives integration tests.
fn file(name: &str, text: &str) -> PathBuf {
    let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    std::fs::write(&path, text).expect("the test directory is writable");
    path
}

const ACL_CONF: &str = "%% A 1.x acl.conf.\n\
    {allow, {username, \"svc:platform\"}, all, all}.\n\
    {deny, all, publish, [\"commands/#\"]}.\n\
    {allow, all, publish, [\"telemetry/#\"]}.\n\
    {allow, {ipaddr, \"127.0.0.1\"}, subscribe, [\"$SYS/#\"]}.\n\
    {deny, all}.\n";

#[test]
fn convert_names_its_two_converters() {
    let out = openqtt().arg("convert").output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    let usage = String::from_utf8(out.stderr).unwrap();
    assert!(usage.contains("acl") && usage.contains("authn"), "{usage}");
}

#[test]
fn convert_acl_writes_the_rules_and_names_each_conflict() {
    let conf = file("cli-acl.conf", ACL_CONF);
    let out = openqtt()
        .args(["convert", "acl", "--service-prefix", "svc:"])
        .arg(&conf)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    let text = String::from_utf8(out.stdout).unwrap();
    let acl = openqtt_auth::Acl::from_toml(&text).unwrap();
    assert_eq!(acl.len(), 4, "{text}");
    let notes = String::from_utf8(out.stderr).unwrap();
    let place = conf.display().to_string();
    assert!(
        notes.contains(&format!("{place}:5: conflict: R2 rule 14: not converted")),
        "{notes}"
    );
    assert!(
        notes.contains(&format!("{place}:4: conflict: R2 rule 16")),
        "{notes}"
    );
    // --strict writes nothing and fails.
    let strict = openqtt()
        .args(["convert", "acl", "--strict", "--service-prefix", "svc:"])
        .arg(&conf)
        .output()
        .unwrap();
    assert_eq!(strict.status.code(), Some(1));
    assert!(strict.stdout.is_empty());
    let message = String::from_utf8(strict.stderr).unwrap();
    assert!(
        message.contains("--strict refuses them; nothing was written"),
        "{message}"
    );
}

#[test]
fn convert_acl_reports_what_it_cannot_convert_by_line() {
    let conf = file(
        "cli-bad.conf",
        "{allow, all, all, all}.\n{allow, X, all, all}.\n",
    );
    let out = openqtt()
        .args(["convert", "acl"])
        .arg(&conf)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    let message = String::from_utf8(out.stderr).unwrap();
    assert!(
        message.contains(&format!("{}:2: error: a variable", conf.display())),
        "{message}"
    );
    let missing = openqtt()
        .args(["convert", "acl", "/nonexistent/acl.conf"])
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(1));
    let message = String::from_utf8(missing.stderr).unwrap();
    assert!(
        message.contains("cannot read /nonexistent/acl.conf"),
        "{message}"
    );
}

#[test]
fn convert_authn_writes_a_hashed_file_and_refuses_superusers() {
    let csv = file(
        "cli-authn.csv",
        "user_id,password,is_superuser\nsvc:platform,Synthetic-pw,false\n",
    );
    let out = openqtt()
        .args(["convert", "authn"])
        .arg(&csv)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(!text.contains("Synthetic-pw"), "{text}");
    let list =
        openqtt_auth::PasswordList::parse(&text, openqtt_auth::BootstrapFormat::Hashed, None)
            .unwrap();
    assert_eq!(list.len(), 1);
    let superuser = file(
        "cli-superuser.csv",
        "user_id,password,is_superuser\nroot,Synthetic-root,true\n",
    );
    let out = openqtt()
        .args(["convert", "authn"])
        .arg(&superuser)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(out.stdout.is_empty());
    let message = String::from_utf8(out.stderr).unwrap();
    assert!(message.contains("superusers"), "{message}");
    assert!(!message.contains("Synthetic-root"), "{message}");
}
