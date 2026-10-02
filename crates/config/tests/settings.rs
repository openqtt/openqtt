//! Loading settings as a process does: the defaults, a file, variables, and every way they can
//! be wrong. Variables are passed in rather than set, since a test cannot safely change its own
//! environment.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use openqtt_config::{
    ByteSize, Duration, Error, Loaded, LogFormat, PasswordType, Role, SecretFile, Settings, Sources,
};

/// Writes `text` to a new file under the directory Cargo gives integration tests, and returns
/// its path. Every call gets a file of its own: tests run at once, in processes and threads,
/// and one must never read a file another is writing.
fn file(name: &str, text: &str) -> PathBuf {
    static CALLS: AtomicUsize = AtomicUsize::new(0);
    let call = CALLS.fetch_add(1, Ordering::Relaxed);
    let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("openqtt-config")
        .join(format!("{}-{call}-{name}", std::process::id()));
    let directory = path.parent().expect("the path is in a directory");
    std::fs::create_dir_all(directory).expect("the test directory can be made");
    std::fs::write(&path, text).expect("the test file can be written");
    path
}

fn load(config: Option<&Path>, vars: &[(&str, &str)]) -> Result<Loaded, Error> {
    let vars = vars.iter().map(|(name, value)| (*name, *value));
    Settings::load(&Sources::new(config.map(Path::to_owned), vars))
}

fn settings(config: Option<&Path>, vars: &[(&str, &str)]) -> Settings {
    load(config, vars)
        .expect("these settings load")
        .into_settings()
}

/// Every problem in an error, as its message.
fn problems(error: &Error) -> Vec<String> {
    error.problems().iter().map(ToString::to_string).collect()
}

fn load_error(config: Option<&Path>, vars: &[(&str, &str)]) -> Vec<String> {
    problems(&load(config, vars).expect_err("these settings are refused"))
}

fn rule_error(settings: &Settings) -> Vec<String> {
    problems(
        &settings
            .validate()
            .expect_err("a rule refuses these settings"),
    )
}

/// The defaults, as `openqtt config print --effective` writes them. A default changes here,
/// in the declarations and in docs/spec/config.md, or the tests fail.
const DEFAULTS: &str = r#"[cluster]
name = "openqtt"
# node_name is unset
roles = ["all"]
seeds = []
# zone is unset
# cert_file is unset
# key_file is unset
# ca_file is unset

[listeners.quic.default]
bind = "0.0.0.0:14567"
# cert_file is unset
# key_file is unset
# client_ca_file is unset
require_client_cert = false
identity_from_cn = false
mountpoint = ""
enable_authn = true
max_streams = 8
stream_window = "1MiB"
connection_window = "1MiB"
mtu_discovery = true
session_tickets = true

[limits]
keep_alive_min = "10s"
keep_alive_max = "20m"
receive_maximum = 32
maximum_packet_size = "1MiB"
topic_alias_maximum = 64
session_expiry_max = "7d"
max_subscriptions = 1000
max_topic_levels = 128
max_queued_messages = 1000
max_client_id_length = 256

[auth]
# password_bootstrap_file is unset
password_bootstrap_type = "hashed"
# acl_file is unset

[auth.jwt]
enabled = false

[auth.http]
enabled = false

[edge]
interest_threshold = 64
interest_floor = 1
interest_grace = "30s"

[router]
shards = 64

[log]
partitions = 256
replication_factor = 1

[admin]
# api_key_bootstrap_file is unset

[http]
bind = "0.0.0.0:8080"

[observability]
log_level = "info"
log_format = "json"

[observability.otlp]
# endpoint is unset
# headers_file is unset
# ca_file is unset
interval = "1m"
timeout = "10s"

[observability.prometheus]
enabled = false
# token_file is unset

[storage]
data_dir = "/var/lib/openqtt"
"#;

#[test]
fn every_default_is_the_one_the_reports_chose() {
    assert_eq!(Settings::default().to_toml(), DEFAULTS);

    // The defaults that come from a report, by name.
    let defaults = Settings::default();
    let limits = &defaults.limits;
    assert_eq!(limits.keep_alive_min, Duration::from_secs(10)); // R1, O4
    assert_eq!(limits.keep_alive_max, Duration::from_secs(1_200)); // R1, O4
    assert_eq!(limits.receive_maximum, 32); // R1, O3
    assert_eq!(limits.maximum_packet_size, ByteSize::new(1_048_576)); // R1, O5
    assert_eq!(limits.topic_alias_maximum, 64); // R1, O6
    assert_eq!(limits.session_expiry_max, Duration::from_secs(604_800)); // R1, O7
    assert_eq!(limits.max_subscriptions, 1_000); // R1, O16
    assert_eq!(limits.max_topic_levels, 128); // R1, O16
    assert_eq!(limits.max_queued_messages, 1_000); // R1, O12
    assert_eq!(limits.max_client_id_length, 256); // R1, O10
    assert_eq!(defaults.edge.interest_threshold, 64); // R6, D3
    assert_eq!(defaults.edge.interest_floor, 1); // R6, D3
    assert_eq!(defaults.edge.interest_grace, Duration::from_secs(30)); // R6, D5
    assert_eq!(defaults.router.shards, 64); // R3
    assert_eq!(defaults.log.partitions, 256); // R3
    let listener = &defaults.listeners.quic["default"];
    assert_eq!(listener.bind.port(), 14567); // spec section 1
    assert_eq!(listener.max_streams, 8); // R7, D2
    assert_eq!(listener.stream_window, ByteSize::mib(1)); // R7, D2
    assert_eq!(listener.connection_window, ByteSize::mib(1)); // R7, D2
    assert!(listener.mtu_discovery); // R7, D2
    assert!(listener.session_tickets); // R7, D5
    assert_eq!(defaults.cluster.roles, [Role::All]);
    assert_eq!(defaults.observability.log_format, LogFormat::Json);
    assert_eq!(defaults.auth.password_bootstrap_type, PasswordType::Hashed);
}

#[test]
fn with_no_file_and_no_variables_the_settings_are_the_defaults() {
    let loaded = load(None, &[("PATH", "/bin"), ("HOME", "/home/x")]).unwrap();
    assert_eq!(loaded.file(), None);
    assert_eq!(loaded.settings(), &Settings::default());
    assert_eq!(loaded.explicit_toml(), "");
}

#[test]
fn a_file_overrides_the_defaults_and_a_variable_overrides_the_file() {
    let path = file(
        "precedence.toml",
        "[limits]\nreceive_maximum = 16\ntopic_alias_maximum = 8\n[cluster]\nzone = \"a\"\n",
    );
    let from_file = settings(Some(&path), &[]);
    assert_eq!(from_file.limits.receive_maximum, 16);
    assert_eq!(from_file.limits.topic_alias_maximum, 8);
    assert_eq!(from_file.cluster.zone.as_deref(), Some("a"));
    assert_eq!(
        from_file.limits.max_topic_levels, 128,
        "a default the file leaves alone"
    );

    let both = settings(
        Some(&path),
        &[
            ("OPENQTT_LIMITS__RECEIVE_MAXIMUM", "8"),
            ("OPENQTT_STORAGE__DATA_DIR", "/data"),
        ],
    );
    assert_eq!(
        both.limits.receive_maximum, 8,
        "the variable wins over the file"
    );
    assert_eq!(
        both.limits.topic_alias_maximum, 8,
        "the file wins over the default"
    );
    assert_eq!(
        both.storage.data_dir,
        Path::new("/data"),
        "the variable wins over the default"
    );
}

#[test]
fn the_command_line_names_the_file_before_openqtt_config_does() {
    let flag = file("flag.toml", "[cluster]\nzone = \"flag\"\n");
    let variable = file("variable.toml", "[cluster]\nzone = \"variable\"\n");
    let config_var = ("OPENQTT_CONFIG", variable.to_str().unwrap());

    let loaded = load(Some(&flag), &[config_var]).unwrap();
    assert_eq!(loaded.file(), Some(flag.as_path()));
    assert_eq!(loaded.settings().cluster.zone.as_deref(), Some("flag"));

    let loaded = load(None, &[config_var]).unwrap();
    assert_eq!(loaded.file(), Some(variable.as_path()));
    assert_eq!(loaded.settings().cluster.zone.as_deref(), Some("variable"));

    let loaded = load(None, &[("OPENQTT_CONFIG", "")]).unwrap();
    assert_eq!(loaded.file(), None, "an empty OPENQTT_CONFIG names no file");
}

#[test]
fn an_empty_variable_unsets_an_optional_setting() {
    let path = file(
        "unset.toml",
        "[cluster]\nzone = \"a\"\nnode_name = \"edge-0\"\n",
    );
    let settings = settings(Some(&path), &[("OPENQTT_CLUSTER__ZONE", "")]);
    assert_eq!(settings.cluster.zone, None);
    assert_eq!(settings.cluster.node_name.as_deref(), Some("edge-0"));
}

#[test]
fn variables_hold_every_type_of_value() {
    let settings = settings(
        None,
        &[
            ("OPENQTT_CLUSTER__ROLES", "edge, router"),
            (
                "OPENQTT_CLUSTER__SEEDS",
                "openqtt-log-0:7000,[2001:db8::1]:7000",
            ),
            ("OPENQTT_LIMITS__KEEP_ALIVE_MAX", "30m"),
            ("OPENQTT_LIMITS__MAXIMUM_PACKET_SIZE", "2MiB"),
            ("OPENQTT_LISTENERS__QUIC__DEFAULT__STREAM_WINDOW", "65536"),
            ("OPENQTT_LISTENERS__QUIC__DEFAULT__MTU_DISCOVERY", "false"),
            ("OPENQTT_LISTENERS__QUIC__DEFAULT__BIND", "[::]:443"),
            ("OPENQTT_OBSERVABILITY__LOG_FORMAT", "text"),
            (
                "OPENQTT_OBSERVABILITY__OTLP__ENDPOINT",
                "http://collector:4318",
            ),
            (
                "OPENQTT_AUTH__PASSWORD_BOOTSTRAP_FILE",
                "/run/secrets/users",
            ),
        ],
    );
    assert_eq!(settings.cluster.roles, [Role::Edge, Role::Router]);
    let seeds: Vec<String> = settings
        .cluster
        .seeds
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(seeds, ["openqtt-log-0:7000", "[2001:db8::1]:7000"]);
    assert_eq!(settings.limits.keep_alive_max, Duration::from_secs(1_800));
    assert_eq!(settings.limits.maximum_packet_size, ByteSize::mib(2));
    let listener = &settings.listeners.quic["default"];
    assert_eq!(listener.stream_window, ByteSize::kib(64));
    assert!(!listener.mtu_discovery);
    assert_eq!(listener.bind, "[::]:443".parse().unwrap());
    assert_eq!(settings.observability.log_format, LogFormat::Text);
    let endpoint = settings.observability.otlp.endpoint.as_ref().unwrap();
    assert_eq!(endpoint.as_str(), "http://collector:4318");
    assert_eq!(
        settings.auth.password_bootstrap_file,
        Some(SecretFile::new("/run/secrets/users"))
    );

    let empty = self::settings(None, &[("OPENQTT_CLUSTER__SEEDS", "")]);
    assert!(empty.cluster.seeds.is_empty());
}

#[test]
fn variables_change_the_listeners_that_exist_and_create_none() {
    // With no file, the default listener is the one that exists.
    let settings = settings(
        None,
        &[("OPENQTT_LISTENERS__QUIC__DEFAULT__BIND", "0.0.0.0:443")],
    );
    assert_eq!(settings.listeners.quic.len(), 1);
    assert_eq!(settings.listeners.quic["default"].bind.port(), 443);

    // A file that names listeners replaces the default one.
    let path = file(
        "listeners.toml",
        "[listeners.quic.devices]\nbind = \"0.0.0.0:443\"\n[listeners.quic.services]\nbind = \"0.0.0.0:14567\"\n",
    );
    let from_file = self::settings(Some(&path), &[]);
    let names: Vec<&String> = from_file.listeners.quic.keys().collect();
    assert_eq!(names, ["devices", "services"]);
    assert_eq!(
        from_file.listeners.quic["devices"].max_streams, 8,
        "unset keys keep their defaults"
    );

    let changed = self::settings(
        Some(&path),
        &[(
            "OPENQTT_LISTENERS__QUIC__DEVICES__MOUNTPOINT",
            "ingest/${username}/",
        )],
    );
    assert_eq!(
        changed.listeners.quic["devices"].mountpoint,
        "ingest/${username}/"
    );

    // A misspelt or absent listener is an unknown variable, not a new listener.
    assert_eq!(
        load_error(
            Some(&path),
            &[("OPENQTT_LISTENERS__QUIC__DEVICS__BIND", "0.0.0.0:1")]
        ),
        [
            "unknown variable OPENQTT_LISTENERS__QUIC__DEVICS__BIND; the nearest valid one is \
          OPENQTT_LISTENERS__QUIC__DEVICES__BIND"
        ]
    );
    assert_eq!(
        load_error(
            Some(&path),
            &[("OPENQTT_LISTENERS__QUIC__DEFAULT__BIND", "0.0.0.0:1")]
        ),
        [
            "unknown variable OPENQTT_LISTENERS__QUIC__DEFAULT__BIND; the nearest valid one is \
          OPENQTT_LISTENERS__QUIC__DEVICES__BIND"
        ]
    );
}

#[test]
fn an_unknown_key_in_the_file_names_the_nearest_valid_one() {
    let path = file(
        "unknown-key.toml",
        "[listeners.quic.default]\ncertfile = \"/tls.crt\"\n[clustre]\nname = \"x\"\n",
    );
    let shown = path.display();
    assert_eq!(
        load_error(Some(&path), &[]),
        [
            format!("unknown key `clustre` in {shown}; the nearest valid key is `cluster`"),
            format!(
                "unknown key `listeners.quic.default.certfile` in {shown}; the nearest valid key \
                 is `listeners.quic.default.cert_file`"
            ),
        ]
    );
}

#[test]
fn an_unknown_variable_names_the_nearest_valid_one() {
    for (name, nearest) in [
        (
            "OPENQTT_OBSERVABILITY__LOGLEVEL",
            "OPENQTT_OBSERVABILITY__LOG_LEVEL",
        ),
        (
            "OPENQTT_LIMITS__RECIEVE_MAXIMUM",
            "OPENQTT_LIMITS__RECEIVE_MAXIMUM",
        ),
        (
            "OPENQTT_limits__receive_maximum",
            "OPENQTT_LIMITS__RECEIVE_MAXIMUM",
        ),
        ("OPENQTT_CLUSTER__NODE", "OPENQTT_CLUSTER__NODE_NAME"),
        ("OPENQTT_STORAGE__DATA_DIR__X", "OPENQTT_STORAGE__DATA_DIR"),
        // Names this repository used before the settings existed.
        ("OPENQTT_LOG", "OPENQTT_OBSERVABILITY__LOG_LEVEL"),
        ("OPENQTT_SEEDS", "OPENQTT_CLUSTER__SEEDS"),
    ] {
        assert_eq!(
            load_error(None, &[(name, "x")]),
            [format!(
                "unknown variable {name}; the nearest valid one is {nearest}"
            )],
            "{name}"
        );
    }
    // A section is not a setting, and neither is a name with nothing after the prefix.
    for name in ["OPENQTT_CLUSTER", "OPENQTT_", "OPENQTT_NODE__COOKIE"] {
        let error = load(None, &[(name, "x")]).unwrap_err();
        assert!(
            matches!(error, Error::UnknownVariable { .. }),
            "{name}: {error}"
        );
    }
}

#[test]
fn otel_variables_are_refused_with_the_setting_that_does_their_job() {
    for (name, setting) in [
        (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OPENQTT_OBSERVABILITY__OTLP__ENDPOINT",
        ),
        (
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            "OPENQTT_OBSERVABILITY__OTLP__ENDPOINT",
        ),
        (
            "OTEL_EXPORTER_OTLP_HEADERS",
            "OPENQTT_OBSERVABILITY__OTLP__HEADERS_FILE",
        ),
        (
            "OTEL_EXPORTER_OTLP_TIMEOUT",
            "OPENQTT_OBSERVABILITY__OTLP__TIMEOUT",
        ),
        (
            "OTEL_METRIC_EXPORT_INTERVAL",
            "OPENQTT_OBSERVABILITY__OTLP__INTERVAL",
        ),
        (
            "OTEL_EXPORTER_OTLP_CERTIFICATE",
            "OPENQTT_OBSERVABILITY__OTLP__CA_FILE",
        ),
        ("OTEL_SERVICE_NAME", "OPENQTT_OBSERVABILITY__OTLP__ENDPOINT"),
    ] {
        assert_eq!(
            load_error(None, &[(name, "x")]),
            [format!(
                "{name} is set, but OpenQTT reads no OTEL_ variables: set {setting} or another \
                 observability.otlp setting instead"
            )],
            "{name}"
        );
    }
}

#[test]
fn every_problem_is_reported_at_once_in_order() {
    let path = file(
        "several.toml",
        "[limits]\nreceive_maximum = \"many\"\nreceive_maxim = 2\n[storage]\ndir = \"/x\"\n",
    );
    let shown = path.display();
    assert_eq!(
        load_error(
            Some(&path),
            &[
                ("OPENQTT_ZONE", "a"),
                ("OPENQTT_LIMITS__TOPIC_ALIAS_MAXIMUM", "-1"),
                ("OTEL_SDK_DISABLED", "true"),
            ]
        ),
        [
            "OTEL_SDK_DISABLED is set, but OpenQTT reads no OTEL_ variables: set \
             OPENQTT_OBSERVABILITY__OTLP__ENDPOINT or another observability.otlp setting instead"
                .to_owned(),
            format!(
                "unknown key `limits.receive_maxim` in {shown}; the nearest valid key is \
                 `limits.receive_maximum`"
            ),
            format!(
                "`limits.receive_maximum` in {shown}: invalid type: string \"many\", expected u16"
            ),
            format!(
                "unknown key `storage.dir` in {shown}; the nearest valid key is `storage.data_dir`"
            ),
            "OPENQTT_LIMITS__TOPIC_ALIAS_MAXIMUM: invalid value: integer `-1`, expected u16"
                .to_owned(),
            "unknown variable OPENQTT_ZONE; the nearest valid one is OPENQTT_CLUSTER__ZONE"
                .to_owned(),
        ]
    );
}

#[test]
fn a_value_names_its_key_and_where_it_came_from_when_it_does_not_fit() {
    let path = file(
        "values.toml",
        "[limits]\nkeep_alive_max = 1200\n[cluster]\nseeds = [\"log-0\"]\nroles = [\"edgy\"]\n\
         [listeners.quic.My-Devices]\nbind = \"0.0.0.0:443\"\n[http]\nbind = { ip = 1 }\n",
    );
    let shown = path.display();
    assert_eq!(
        load_error(Some(&path), &[]),
        [
            format!(
                "`cluster.roles` in {shown}: unknown variant `edgy`, expected one of `all`, \
                 `edge`, `router`, `log`, `admin`"
            ),
            format!(
                "`cluster.seeds` in {shown}: expected `host:port`, such as `openqtt-log:7000`, \
                 found `log-0`"
            ),
            format!("`http.bind` in {shown}: invalid type: map, expected socket address"),
            format!(
                "`limits.keep_alive_max` in {shown}: a duration needs a unit: `1200s` for \
                 seconds, `1200ms` for milliseconds"
            ),
            format!(
                "`listeners.quic.My-Devices` in {shown}: a name here is lowercase letters and \
                 digits, in words joined by single underscores, at most 63 characters"
            ),
        ]
    );
    assert_eq!(
        load_error(
            None,
            &[
                ("OPENQTT_LIMITS__RECEIVE_MAXIMUM", "lots"),
                ("OPENQTT_LISTENERS__QUIC__DEFAULT__ENABLE_AUTHN", "yes"),
                ("OPENQTT_OBSERVABILITY__OTLP__INTERVAL", "60"),
            ]
        ),
        [
            "OPENQTT_LIMITS__RECEIVE_MAXIMUM: expected a whole number, found `lots`",
            "OPENQTT_LISTENERS__QUIC__DEFAULT__ENABLE_AUTHN: expected `true` or `false`, found `yes`",
            "OPENQTT_OBSERVABILITY__OTLP__INTERVAL: expected a duration such as `500ms`, `30s`, \
             `20m`, `2h` or `7d`, found `60`",
        ]
    );
}

#[test]
fn a_file_that_cannot_be_read_or_parsed_is_reported_with_where() {
    let missing = Path::new(env!("CARGO_TARGET_TMPDIR")).join("openqtt-config/missing.toml");
    let error = load(Some(&missing), &[]).unwrap_err();
    assert!(matches!(error, Error::ReadFile { .. }), "{error}");
    assert!(error.to_string().starts_with(&format!(
        "cannot read the configuration file {}: ",
        missing.display()
    )));

    let broken = file("broken.toml", "[limits]\nreceive_maximum = \n");
    let error = load(Some(&broken), &[]).unwrap_err();
    assert!(matches!(error, Error::Syntax { .. }), "{error}");
    assert!(error.to_string().contains("line 2"), "{error}");
}

#[test]
fn a_file_that_is_not_toml_is_refused_by_line_and_column_without_repeating_the_line() {
    for (text, place) in [
        (
            "[auth]\npassword = \"hunter2\" trailing\n",
            "line 2, column 22",
        ),
        (
            "[auth]\n\npassword_bootstrap_file = \"/x\nhunter2\n",
            "line 4, column 8",
        ),
        (
            "[a\u{e9}uth]]\npassword = \"hunter2\"\n",
            "line 1, column 8",
        ),
    ] {
        let path = file("syntax.toml", text);
        let error = load(Some(&path), &[]).unwrap_err();
        let message = error.to_string();
        assert!(matches!(error, Error::Syntax { .. }), "{message}");
        assert!(
            message.starts_with(&format!("{} is not valid TOML: {place}: ", path.display())),
            "{message}"
        );
        assert!(!message.contains("hunter2"), "{message}");
        assert_eq!(message.lines().count(), 1, "{message}");
    }
}

#[cfg(unix)]
#[test]
fn a_variable_that_is_not_unicode_is_refused() {
    use std::os::unix::ffi::OsStringExt;

    let value = std::ffi::OsString::from_vec(vec![b'a', 0x80]);
    let name = std::ffi::OsString::from("OPENQTT_CLUSTER__ZONE");
    let sources = Sources::new(None, [(name, value)]);
    let error = Settings::load(&sources).unwrap_err();
    assert_eq!(
        error.to_string(),
        "OPENQTT_CLUSTER__ZONE is not valid Unicode"
    );
}

/// A deployment's edge, with one listener for devices identified by their certificates and one
/// for services that log in with a password.
fn edge_settings() -> Settings {
    let path = file(
        "edge.toml",
        r#"
[cluster]
roles = ["edge"]
node_name = "openqtt-edge-0"
seeds = ["openqtt-log:7000"]
zone = "zone-a"
cert_file = "/etc/openqtt/cluster/tls.crt"
key_file = "/etc/openqtt/cluster/tls.key"
ca_file = "/etc/openqtt/cluster/ca.crt"

[listeners.quic.devices]
bind = "0.0.0.0:443"
cert_file = "/etc/openqtt/tls/tls.crt"
key_file = "/etc/openqtt/tls/tls.key"
client_ca_file = "/etc/openqtt/tls/devices-ca.crt"
require_client_cert = true
identity_from_cn = true
enable_authn = false
mountpoint = "ingest/${username}/"

[listeners.quic.services]
bind = "0.0.0.0:14567"
cert_file = "/etc/openqtt/tls/tls.crt"
key_file = "/etc/openqtt/tls/tls.key"

[auth]
password_bootstrap_file = "/etc/openqtt/users"
acl_file = "/etc/openqtt/acl.toml"

[observability.otlp]
endpoint = "https://collector.example:4318"
ca_file = "/etc/openqtt/collector-ca.crt"
headers_file = "/etc/openqtt/collector-headers"

[observability.prometheus]
enabled = true
token_file = "/etc/openqtt/prometheus-token"
"#,
    );
    settings(Some(&path), &[])
}

#[test]
fn a_complete_configuration_passes_every_rule() {
    let edge = edge_settings();
    edge.validate().unwrap();
    assert!(edge.runs(Role::Edge));
    assert!(!edge.runs(Role::Log));

    let mut all = Settings::default();
    all.listeners = edge.listeners.clone();
    all.validate().unwrap();
    assert!(all.runs(Role::Log), "`all` runs every role");
}

#[test]
fn rules_refuse_settings_that_cannot_work_together() {
    type Change = fn(&mut Settings);
    let cases: [(Change, &str); 30] = [
        (
            |s| s.cluster.roles.clear(),
            "cluster.roles: name at least one role",
        ),
        (
            |s| s.cluster.roles.push(Role::Edge),
            "cluster.roles: `edge` is listed twice",
        ),
        (
            |s| s.cluster.roles = vec![Role::All, Role::Log],
            "cluster.roles: `all` already runs",
        ),
        (
            |s| s.cluster.name = "OpenQTT".into(),
            "cluster.name: a cluster's name is",
        ),
        (
            |s| s.cluster.node_name = Some("edge_0".into()),
            "cluster.node_name: a node's name is",
        ),
        (
            |s| s.cluster.node_name = None,
            "cluster.node_name: required when",
        ),
        (
            |s| s.cluster.zone = Some(String::new()),
            "cluster.zone: empty",
        ),
        (
            |s| s.cluster.ca_file = None,
            "cluster.cert_file: cluster.cert_file, cluster.key_file",
        ),
        (
            |s| quic(s, "services").cert_file = None,
            "listeners.quic.services.cert_file: required",
        ),
        (
            |s| quic(s, "services").key_file = None,
            "listeners.quic.services.key_file: required",
        ),
        (
            |s| quic(s, "devices").client_ca_file = None,
            "listeners.quic.devices.client_ca_file",
        ),
        (
            |s| quic(s, "devices").require_client_cert = false,
            "listeners.quic.devices.identity_from_cn: needs require_client_cert",
        ),
        (
            |s| quic(s, "devices").enable_authn = true,
            "listeners.quic.devices.enable_authn: must be",
        ),
        (
            |s| quic(s, "devices").mountpoint = "ingest/+/".into(),
            "devices.mountpoint: a mountpoint",
        ),
        (
            |s| quic(s, "devices").mountpoint = "${user}/".into(),
            "devices.mountpoint: the only",
        ),
        (
            |s| quic(s, "devices").max_streams = 0,
            "listeners.quic.devices.max_streams: from 1",
        ),
        (
            |s| quic(s, "devices").max_streams = 257,
            "listeners.quic.devices.max_streams: from 1",
        ),
        (
            |s| quic(s, "devices").stream_window = ByteSize::new(512),
            "devices.stream_window: at least",
        ),
        (
            |s| quic(s, "services").bind = "10.0.0.1:443".parse().unwrap(),
            "services.bind: 10.0.0.1:443 overlaps",
        ),
        (
            |s| s.listeners.quic.clear(),
            "listeners.quic: the edge role needs at least one listener",
        ),
        (
            |s| s.limits.keep_alive_min = Duration::from_millis(10_500),
            "limits.keep_alive_min: Keep Alive is a whole",
        ),
        (
            |s| s.limits.keep_alive_min = Duration::from_secs(0),
            "limits.keep_alive_min: at least 1s",
        ),
        (
            |s| s.limits.keep_alive_max = Duration::from_secs(65_536),
            "limits.keep_alive_max: at most 65535s",
        ),
        (
            |s| s.limits.keep_alive_min = Duration::from_secs(1_201),
            "limits.keep_alive_min: longer than",
        ),
        (
            |s| s.limits.receive_maximum = 0,
            "limits.receive_maximum: at least 1",
        ),
        (
            |s| s.limits.maximum_packet_size = ByteSize::new(1_000),
            "limits.maximum_packet_size: from 1KiB",
        ),
        (
            |s| s.limits.max_client_id_length = 22,
            "limits.max_client_id_length: at least 23",
        ),
        (
            |s| s.auth.jwt.enabled = true,
            "auth.jwt.enabled: JWT authentication is not available",
        ),
        (
            |s| s.log.replication_factor = 2,
            "log.replication_factor: 1 or 3",
        ),
        (
            |s| s.observability.prometheus.token_file = None,
            "observability.prometheus.token_file: required",
        ),
    ];
    for (change, expected) in cases {
        let mut settings = edge_settings();
        change(&mut settings);
        let errors = rule_error(&settings);
        assert!(
            errors.iter().any(|error| error.contains(expected)),
            "expected `{expected}` among {errors:?}"
        );
    }
}

#[test]
fn rules_refuse_what_has_no_effect_or_is_not_built() {
    type Change = fn(&mut Settings);
    let cases: [(Change, &str); 9] = [
        (
            |s| s.auth.http.enabled = true,
            "auth.http.enabled: HTTP authentication is not available",
        ),
        (
            |s| s.observability.otlp.endpoint = None,
            "observability.otlp.headers_file: has no effect",
        ),
        (
            |s| s.observability.otlp.endpoint = None,
            "observability.otlp.ca_file: has no effect",
        ),
        (
            |s| s.observability.otlp.endpoint = Some("http://collector:4318".parse().unwrap()),
            "observability.otlp.ca_file: set, but the endpoint is not https",
        ),
        (
            |s| s.observability.otlp.interval = Duration::from_millis(500),
            "observability.otlp.interval: at least 1s",
        ),
        (
            |s| s.observability.otlp.timeout = Duration::from_secs(0),
            "observability.otlp.timeout",
        ),
        (
            |s| s.observability.log_level = " ".into(),
            "observability.log_level: empty",
        ),
        (|s| s.router.shards = 0, "router.shards: at least 1"),
        (
            |s| s.edge.interest_threshold = 0,
            "edge.interest_threshold: at least 1",
        ),
    ];
    for (change, expected) in cases {
        let mut settings = edge_settings();
        change(&mut settings);
        let errors = rule_error(&settings);
        assert!(
            errors.iter().any(|error| error.contains(expected)),
            "expected `{expected}` among {errors:?}"
        );
    }
}

fn quic<'s>(settings: &'s mut Settings, name: &str) -> &'s mut openqtt_config::QuicListener {
    settings
        .listeners
        .quic
        .get_mut(name)
        .expect("the listener is configured")
}

#[test]
fn a_listener_needs_its_certificate_only_where_the_edge_role_runs() {
    let mut settings = Settings::default();
    let errors = rule_error(&settings);
    assert_eq!(
        errors,
        [
            "listeners.quic.default.cert_file: required: a QUIC listener needs a certificate",
            "listeners.quic.default.key_file: required: a QUIC listener needs a private key",
        ]
    );
    settings.cluster.roles = vec![Role::Log];
    settings.cluster.node_name = Some("openqtt-log-0".into());
    settings.validate().unwrap();
}

#[test]
fn check_files_reports_every_file_that_cannot_be_opened() {
    let mut settings = Settings::default();
    settings.check_files().unwrap();

    let present = file("present.pem", "certificate");
    let directory = present.parent().unwrap().to_owned();
    let missing = directory.join("missing.pem");
    let listener = settings.listeners.quic.get_mut("default").unwrap();
    listener.cert_file = Some(present.clone());
    listener.key_file = Some(SecretFile::new(&missing));
    settings.auth.acl_file = Some(directory.clone());
    let errors = problems(&settings.check_files().unwrap_err());
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(errors[0].starts_with(&format!(
        "listeners.quic.default.key_file: cannot read {}: ",
        missing.display()
    )));
    assert_eq!(
        errors[1],
        format!(
            "auth.acl_file: cannot read {}: it is a directory",
            directory.display()
        )
    );
}

#[test]
fn a_secret_never_reaches_a_message_or_the_printed_settings() {
    const SECRET: &str = "hunter2";

    // Set inline by mistake, under a name that is not a setting.
    let inline = file(
        "inline-secret.toml",
        &format!("[auth]\npassword = \"{SECRET}\"\n"),
    );
    for error in [
        load_error(Some(&inline), &[]),
        load_error(None, &[("OPENQTT_AUTH__PASSWORD", SECRET)]),
        load_error(
            None,
            &[("OTEL_EXPORTER_OTLP_HEADERS", "authorization=Bearer hunter2")],
        ),
        load_error(
            None,
            &[(
                "OPENQTT_OBSERVABILITY__OTLP__ENDPOINT",
                "https://user:hunter2@collector:4318",
            )],
        ),
    ] {
        assert!(!error.concat().contains(SECRET), "{error:?}");
    }
    // And the message points at the key that names a file instead.
    assert_eq!(
        load_error(None, &[("OPENQTT_AUTH__PASSWORD", SECRET)]),
        [
            "unknown variable OPENQTT_AUTH__PASSWORD; the nearest valid one is \
          OPENQTT_AUTH__PASSWORD_BOOTSTRAP_FILE"
        ]
    );

    // Held in a file the settings name: the path is a setting, the contents are not.
    let secret_file = file("token", SECRET);
    let config = file(
        "secret-file.toml",
        &format!(
            "[observability.prometheus]\nenabled = true\ntoken_file = \"{}\"\n",
            secret_file.display()
        ),
    );
    let vars = [(
        "OPENQTT_ADMIN__API_KEY_BOOTSTRAP_FILE",
        secret_file.to_str().unwrap(),
    )];
    let loaded = load(Some(&config), &vars).unwrap();
    let settings = loaded.settings();
    settings.validate().unwrap_err(); // the default listener has no certificate
    settings.check_files().unwrap();
    for printed in [
        settings.to_toml(),
        loaded.explicit_toml(),
        format!("{settings:?}"),
        format!("{loaded:?}"),
        format!("{:?}", Sources::new(None, vars)),
    ] {
        assert!(!printed.contains(SECRET), "{printed}");
    }
    let token = settings
        .observability
        .prometheus
        .token_file
        .as_ref()
        .unwrap()
        .read()
        .unwrap();
    assert_eq!(token.expose(), SECRET.as_bytes());
    assert_eq!(
        format!("{token} {token:?}"),
        "<redacted> Secret(<redacted>)"
    );
}

#[test]
fn the_effective_settings_print_as_a_file_that_loads_back_to_them() {
    for settings in [Settings::default(), edge_settings()] {
        let printed = file("printed.toml", &settings.to_toml());
        assert_eq!(self::settings(Some(&printed), &[]), settings);
    }
}

#[test]
fn the_plain_print_shows_only_what_the_file_and_the_variables_set() {
    let path = file(
        "explicit.toml",
        "[storage]\ndata_dir = \"/data\"\n[cluster]\nzone = \"a\"\nroles = [\"all\"]\n",
    );
    let loaded = load(
        Some(&path),
        &[
            ("OPENQTT_CLUSTER__ZONE", ""),
            ("OPENQTT_LISTENERS__QUIC__DEFAULT__BIND", "0.0.0.0:443"),
        ],
    )
    .unwrap();
    assert_eq!(
        loaded.explicit_toml(),
        "[cluster]\nroles = [\"all\"]\n\n[listeners.quic.default]\nbind = \"0.0.0.0:443\"\n\n\
         [storage]\ndata_dir = \"/data\"\n"
    );
}

#[test]
fn both_prints_keep_an_empty_set_of_listeners_and_a_listener_with_no_settings_of_its_own() {
    // No listener at all, as a router's configuration may say, and a listener that takes every
    // default. Leaving either table out of the print would bring the default listener back.
    for (text, names) in [
        ("[listeners.quic]\n", &[][..]),
        ("[listeners.quic.custom]\n", &["custom"][..]),
        (
            "[listeners.quic.custom]\n[listeners.quic.other]\nbind = \"0.0.0.0:443\"\n",
            &["custom", "other"][..],
        ),
    ] {
        let loaded = load(Some(&file("listeners-in.toml", text)), &[]).unwrap();
        assert_eq!(
            loaded.settings().listeners.quic.keys().collect::<Vec<_>>(),
            names
        );
        assert_eq!(
            loaded.explicit_toml(),
            text.replace("[listeners.quic.other]", "\n[listeners.quic.other]")
        );
        for printed in [loaded.settings().to_toml(), loaded.explicit_toml()] {
            let reloaded = settings(Some(&file("listeners-out.toml", &printed)), &[]);
            assert_eq!(&reloaded, loaded.settings(), "{printed}");
        }
    }
}

#[test]
fn the_plain_print_loads_back_to_the_same_settings() {
    let loaded = load(
        Some(&file("edge-again.toml", &edge_settings().to_toml())),
        &[],
    )
    .unwrap();
    let printed = file("edge-plain.toml", &loaded.explicit_toml());
    assert_eq!(&settings(Some(&printed), &[]), loaded.settings());
}
