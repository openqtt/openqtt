//! The binary as an operator meets it: help, the subcommands, the configuration commands, the
//! logs, and the 1.x converters.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

use openqtt_config::{Settings, Sources};

const SUBCOMMANDS: [&str; 5] = ["run", "ctl", "config", "convert", "migrate"];

/// The binary, with an empty environment: the settings a test gives are the only ones, whatever
/// the shell running the tests holds.
fn openqtt() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_openqtt"));
    command.env_clear();
    command
}

/// Writes `text` to a new file of this call's own and returns its path.
fn file(name: &str, text: &str) -> PathBuf {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let call = CALLS.fetch_add(1, Ordering::Relaxed);
    let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("openqtt-cli")
        .join(format!("{}-{call}-{name}", std::process::id()));
    let directory = path.parent().expect("the path is in a directory");
    std::fs::create_dir_all(directory).expect("the test directory can be made");
    std::fs::write(&path, text).expect("the test file can be written");
    path
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("stdout is UTF-8")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("stderr is UTF-8")
}

/// The variables that give the default listener the certificate the edge needs, so that the
/// rules pass. The files are named, not read.
const CERTIFICATE: [(&str, &str); 2] = [
    (
        "OPENQTT_LISTENERS__QUIC__DEFAULT__CERT_FILE",
        "/etc/openqtt/tls/tls.crt",
    ),
    (
        "OPENQTT_LISTENERS__QUIC__DEFAULT__KEY_FILE",
        "/etc/openqtt/tls/tls.key",
    ),
];

#[test]
fn help_lists_every_subcommand() {
    let out = openqtt().arg("--help").output().unwrap();
    assert!(out.status.success());
    let help = stdout(&out);
    for name in SUBCOMMANDS {
        assert!(
            help.contains(name),
            "--help does not mention {name}:\n{help}"
        );
    }
    let out = openqtt().args(["config", "--help"]).output().unwrap();
    let help = stdout(&out);
    assert!(help.contains("check") && help.contains("print"), "{help}");
}

#[test]
fn the_tools_say_they_are_not_implemented_and_fail() {
    for name in ["ctl", "migrate"] {
        let out = openqtt().arg(name).output().unwrap();
        assert_eq!(out.status.code(), Some(1), "openqtt {name}");
        let stderr = stderr(&out);
        assert!(
            stderr.contains(&format!("openqtt {name}: not implemented yet")),
            "openqtt {name}: {stderr}"
        );
    }
    let out = openqtt().arg("config").output().unwrap();
    assert_eq!(out.status.code(), Some(2), "config needs a subcommand");
}

#[test]
fn run_loads_and_checks_the_settings_before_it_says_it_is_not_implemented() {
    let out = openqtt().arg("run").envs(CERTIFICATE).output().unwrap();
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(stderr(&out).contains("openqtt run: not implemented yet"));

    let out = openqtt().arg("run").output().unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(
        stderr(&out),
        "openqtt: listeners.quic.default.cert_file: required: a QUIC listener needs a \
         certificate\nopenqtt: listeners.quic.default.key_file: required: a QUIC listener needs \
         a private key\n"
    );
}

#[test]
fn unknown_names_stop_the_process_with_the_nearest_valid_one() {
    let out = openqtt()
        .arg("run")
        .envs(CERTIFICATE)
        .env("OPENQTT_LOG", "debug")
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(
        stderr(&out),
        "openqtt: OTEL_EXPORTER_OTLP_ENDPOINT is set, but OpenQTT reads no OTEL_ variables: set \
         OPENQTT_OBSERVABILITY__OTLP__ENDPOINT or another observability.otlp setting instead\n\
         openqtt: unknown variable OPENQTT_LOG; the nearest valid one is \
         OPENQTT_OBSERVABILITY__LOG_LEVEL\n"
    );
}

#[test]
fn a_log_filter_that_does_not_parse_stops_the_process() {
    let out = openqtt()
        .arg("run")
        .envs(CERTIFICATE)
        .env("OPENQTT_OBSERVABILITY__LOG_LEVEL", "openqtt=loudest")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(
        stderr(&out).starts_with("openqtt: observability.log_level: "),
        "{}",
        stderr(&out)
    );
}

#[test]
fn logs_are_json_lines_by_default_and_text_when_asked() {
    let out = openqtt()
        .arg("run")
        .envs(CERTIFICATE)
        .env("OPENQTT_OBSERVABILITY__LOG_LEVEL", "debug")
        .output()
        .unwrap();
    let stderr = stderr(&out);
    let line = stderr
        .lines()
        .find(|line| line.contains("loaded the configuration"))
        .unwrap_or_else(|| panic!("no log line in {stderr}"));
    let event: serde_json::Value = serde_json::from_str(line).unwrap();
    assert_eq!(event["level"], "DEBUG");
    assert_eq!(event["message"], "loaded the configuration");
    assert_eq!(event["roles"], "[All]");
    assert_eq!(event["target"], "openqtt");
    assert!(event["timestamp"].is_string(), "{event}");

    let out = openqtt()
        .arg("run")
        .envs(CERTIFICATE)
        .env("OPENQTT_OBSERVABILITY__LOG_LEVEL", "debug")
        .env("OPENQTT_OBSERVABILITY__LOG_FORMAT", "text")
        .output()
        .unwrap();
    let stderr = self::stderr(&out);
    let line = stderr
        .lines()
        .find(|line| line.contains("loaded the configuration"))
        .unwrap_or_else(|| panic!("no log line in {stderr}"));
    assert!(
        serde_json::from_str::<serde_json::Value>(line).is_err(),
        "{line}"
    );
    assert!(
        line.contains("DEBUG") && line.contains("roles=[All]"),
        "{line}"
    );
}

#[test]
fn config_check_passes_a_configuration_whose_files_exist() {
    let certificate = file("tls.crt", "certificate");
    let key = file("tls.key", "key");
    let config = file(
        "check.toml",
        &format!(
            "[listeners.quic.default]\ncert_file = \"{}\"\nkey_file = \"{}\"\n",
            certificate.display(),
            key.display()
        ),
    );
    let out = openqtt()
        .args(["config", "check", "--config"])
        .arg(&config)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    assert_eq!(
        stdout(&out),
        format!("the configuration is valid: {}\n", config.display())
    );
    assert_eq!(stderr(&out), "");
}

#[test]
fn config_check_reports_every_problem_and_fails() {
    let config = file(
        "problems.toml",
        "[listeners.quic.default]\ncert_file = \"/nowhere/tls.crt\"\nkey_file = \"/nowhere/tls.key\"\n\
         identity_from_cn = true\n[observability]\nlog_level = \"openqtt=loudest\"\n",
    );
    let out = openqtt()
        .args(["config", "check"])
        .env("OPENQTT_CONFIG", &config)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(stdout(&out), "");
    let stderr = stderr(&out);
    let lines: Vec<&str> = stderr.lines().collect();
    assert_eq!(lines.len(), 5, "{stderr}");
    assert!(
        lines.iter().all(|line| line.starts_with("openqtt: ")),
        "{stderr}"
    );
    for expected in [
        "listeners.quic.default.identity_from_cn: needs require_client_cert",
        "listeners.quic.default.enable_authn: must be false with identity_from_cn",
        "listeners.quic.default.cert_file: cannot read /nowhere/tls.crt",
        "listeners.quic.default.key_file: cannot read /nowhere/tls.key",
        "observability.log_level: ",
    ] {
        assert!(
            stderr.contains(expected),
            "`{expected}` is not in\n{stderr}"
        );
    }

    let unknown = file(
        "unknown.toml",
        "[listeners.quic.default]\ncertfile = \"x\"\n",
    );
    let out = openqtt()
        .args(["config", "check", "--config"])
        .arg(&unknown)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(
        self::stderr(&out),
        format!(
            "openqtt: unknown key `listeners.quic.default.certfile` in {}; the nearest valid key \
             is `listeners.quic.default.cert_file`\n",
            unknown.display()
        )
    );
}

#[test]
fn the_command_line_file_comes_before_openqtt_config_wherever_the_flag_is() {
    let flag = file("flag.toml", "[cluster]\nzone = \"flag\"\n");
    let variable = file("variable.toml", "[cluster]\nzone = \"variable\"\n");
    for args in [
        vec![
            "--config".as_ref(),
            flag.as_os_str(),
            "config".as_ref(),
            "print".as_ref(),
        ],
        vec![
            "config".as_ref(),
            "print".as_ref(),
            "--config".as_ref(),
            flag.as_os_str(),
        ],
    ] {
        let out = openqtt()
            .args(args)
            .env("OPENQTT_CONFIG", &variable)
            .output()
            .unwrap();
        assert_eq!(stdout(&out), "[cluster]\nzone = \"flag\"\n");
    }
    let out = openqtt()
        .args(["config", "print"])
        .env("OPENQTT_CONFIG", &variable)
        .output()
        .unwrap();
    assert_eq!(stdout(&out), "[cluster]\nzone = \"variable\"\n");
}

#[test]
fn config_print_effective_prints_every_setting_as_a_file_that_loads_back() {
    let out = openqtt()
        .args(["config", "print", "--effective"])
        .env("OPENQTT_LIMITS__RECEIVE_MAXIMUM", "16")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", stderr(&out));
    let printed = stdout(&out);
    assert!(printed.contains("receive_maximum = 16\n"), "{printed}");
    assert!(printed.contains("# node_name is unset\n"), "{printed}");

    let reloaded = Settings::load(&Sources::new(
        Some(file("effective.toml", &printed)),
        Vec::<(String, String)>::new(),
    ))
    .unwrap();
    let mut expected = Settings::default();
    expected.limits.receive_maximum = 16;
    assert_eq!(reloaded.settings(), &expected);
}

#[test]
fn no_secret_reaches_stdout_or_stderr() {
    const SECRET: &str = "hunter2";
    let token = file("token", SECRET);
    let out = openqtt()
        .args(["config", "print", "--effective"])
        .env("OPENQTT_OBSERVABILITY__PROMETHEUS__TOKEN_FILE", &token)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0));
    assert!(stdout(&out).contains(&token.display().to_string()));
    assert!(!stdout(&out).contains(SECRET));

    let out = openqtt()
        .args(["config", "check"])
        .env("OPENQTT_AUTH__PASSWORD", SECRET)
        .env(
            "OPENQTT_OBSERVABILITY__OTLP__ENDPOINT",
            "https://user:hunter2@collector",
        )
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(!stderr(&out).contains(SECRET), "{}", stderr(&out));
    assert!(!stdout(&out).contains(SECRET));
}

#[test]
fn run_starts_the_exporter_and_a_collector_that_is_down_does_not_hold_it() {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let started = std::time::Instant::now();
    let out = openqtt()
        .arg("run")
        .envs(CERTIFICATE)
        .env("OPENQTT_OBSERVABILITY__OTLP__ENDPOINT", &endpoint)
        .env("OPENQTT_OBSERVABILITY__OTLP__TIMEOUT", "2s")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(stderr(&out).contains("openqtt run: not implemented yet"));
    assert!(started.elapsed() < std::time::Duration::from_secs(20));
}

#[test]
fn a_headers_file_the_exporter_cannot_use_stops_run_and_fails_the_check() {
    let headers = file("headers", "Authorization Bearer hunter2\n");
    let otlp = [
        (
            "OPENQTT_OBSERVABILITY__OTLP__ENDPOINT",
            "http://127.0.0.1:4318",
        ),
        (
            "OPENQTT_OBSERVABILITY__OTLP__HEADERS_FILE",
            headers.to_str().unwrap(),
        ),
    ];
    let expected = format!(
        "openqtt: observability.otlp.headers_file: line 1 of {} is not `name: value`\n",
        headers.display()
    );

    let out = openqtt()
        .arg("run")
        .envs(CERTIFICATE)
        .envs(otlp)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(stderr(&out), expected);

    let certificate = file("tls.crt", "certificate");
    let key = file("tls.key", "key");
    let out = openqtt()
        .args(["config", "check"])
        .env("OPENQTT_LISTENERS__QUIC__DEFAULT__CERT_FILE", &certificate)
        .env("OPENQTT_LISTENERS__QUIC__DEFAULT__KEY_FILE", &key)
        .envs(otlp)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert_eq!(stderr(&out), expected);
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
