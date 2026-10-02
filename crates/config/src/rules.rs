//! Rules that judge settings together, and against what OpenQTT can do.
//!
//! Loading checks each value on its own. These rules refuse what is valid alone but cannot
//! work: a key without its certificate, a listener whose identity comes from a certificate that
//! is not required, a feature that is not built yet. A refusal names the setting to change.

use std::collections::BTreeSet;
use std::net::SocketAddr;

use crate::Error;
use crate::settings::{
    Auth, Cluster, Edge, Limits, Listeners, Log, Observability, QuicListener, Role, Router,
    Settings, Storage,
};

/// The largest packet MQTT can describe: a fixed header byte, a four-byte Remaining Length and
/// 268,435,455 bytes more.
const LARGEST_PACKET: u64 = 268_435_460;

/// The largest Keep Alive MQTT can carry, in seconds: a two-byte integer.
const LARGEST_KEEP_ALIVE: u64 = 65_535;

/// The largest Session Expiry Interval that is not "never", in seconds.
const LARGEST_SESSION_EXPIRY: u64 = 4_294_967_294;

/// The most streams a listener lets a client open. quinn keeps state for each from the start
/// of a connection (R7, F1), so this bounds that cost.
const MOST_STREAMS: u32 = 256;

/// The Client Identifier length MQTT requires every server to accept ([MQTT-3.1.3-5]).
const SHORTEST_CLIENT_ID_LIMIT: u16 = 23;

/// The placeholders a mountpoint may hold.
const PLACEHOLDERS: [&str; 2] = ["${username}", "${clientid}"];

/// The problems found so far.
#[derive(Default)]
struct Problems(Vec<Error>);

impl Problems {
    fn refuse(&mut self, key: impl Into<String>, reason: impl Into<String>) {
        self.0.push(Error::Rule {
            key: key.into(),
            reason: reason.into(),
        });
    }
}

impl Settings {
    /// Whether this process runs `role`, by name or as part of `all`.
    pub fn runs(&self, role: Role) -> bool {
        self.cluster
            .roles
            .iter()
            .any(|runs| *runs == role || *runs == Role::All)
    }

    /// Checks the settings against each other, and against what OpenQTT supports.
    ///
    /// # Errors
    ///
    /// One [`Error::Rule`] for every setting refused, naming it and saying why.
    pub fn validate(&self) -> Result<(), Error> {
        let mut problems = Problems::default();
        self.cluster.validate(&mut problems);
        self.listeners
            .validate(self.runs(Role::Edge), &mut problems);
        self.limits.validate(&mut problems);
        self.auth.validate(&mut problems);
        self.edge.validate(&mut problems);
        self.router.validate(&mut problems);
        self.log.validate(&mut problems);
        self.observability.validate(&mut problems);
        self.storage.validate(&mut problems);
        Error::check(problems.0)
    }
}

impl Cluster {
    fn validate(&self, problems: &mut Problems) {
        if self.roles.is_empty() {
            problems.refuse("cluster.roles", "name at least one role, or `all`");
        }
        let mut seen = BTreeSet::new();
        for role in &self.roles {
            if !seen.insert(role) {
                problems.refuse("cluster.roles", format!("`{role}` is listed twice"));
            }
        }
        if self.roles.contains(&Role::All) && self.roles.len() > 1 {
            problems.refuse(
                "cluster.roles",
                "`all` already runs every role, so it stands alone",
            );
        }
        if !is_name(&self.name, 63, false) {
            problems.refuse(
                "cluster.name",
                "a cluster's name is 1 to 63 lowercase letters, digits and `-`, starting and \
                 ending with a letter or digit",
            );
        }
        match &self.node_name {
            Some(name) if !is_name(name, 253, true) => problems.refuse(
                "cluster.node_name",
                "a node's name is 1 to 253 lowercase letters, digits, `-` and `.`, starting and \
                 ending with a letter or digit",
            ),
            None if self.roles != [Role::All] => problems.refuse(
                "cluster.node_name",
                "required when the process runs some of the roles rather than `all`",
            ),
            _ => {}
        }
        if self.zone.as_deref().is_some_and(str::is_empty) {
            problems.refuse("cluster.zone", "empty; leave it unset for no zone");
        }
        let tls = [
            self.cert_file.is_some(),
            self.key_file.is_some(),
            self.ca_file.is_some(),
        ];
        if tls.contains(&true) && tls.contains(&false) {
            problems.refuse(
                "cluster.cert_file",
                "cluster.cert_file, cluster.key_file and cluster.ca_file go together: set all \
                 three or none",
            );
        }
    }
}

/// Whether `name` is 1 to `longest` lowercase letters, digits and `-` (and `.` with `dots`),
/// starting and ending with a letter or digit: a DNS label, or with dots a DNS name, as
/// Kubernetes names pods.
fn is_name(name: &str, longest: usize, dots: bool) -> bool {
    let alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    let (Some(first), Some(last)) = (name.bytes().next(), name.bytes().last()) else {
        return false;
    };
    name.len() <= longest
        && alphanumeric(first)
        && alphanumeric(last)
        && name
            .bytes()
            .all(|byte| alphanumeric(byte) || byte == b'-' || (dots && byte == b'.'))
}

impl Listeners {
    fn validate(&self, edge_runs: bool, problems: &mut Problems) {
        if edge_runs && self.quic.is_empty() {
            problems.refuse(
                "listeners.quic",
                "the edge role needs at least one listener to accept clients on",
            );
        }
        let mut binds: Vec<(&str, SocketAddr)> = Vec::new();
        for (name, listener) in &self.quic {
            let key = |setting: &str| format!("listeners.quic.{name}.{setting}");
            listener.validate(edge_runs, &key, problems);
            if let Some((other, _)) = binds.iter().find(|(_, bind)| overlap(*bind, listener.bind)) {
                problems.refuse(
                    key("bind"),
                    format!("{} overlaps listeners.quic.{other}.bind", listener.bind),
                );
            }
            binds.push((name, listener.bind));
        }
    }
}

/// Whether two UDP binds would compete for the same port.
fn overlap(a: SocketAddr, b: SocketAddr) -> bool {
    a.port() == b.port() && (a.ip() == b.ip() || a.ip().is_unspecified() || b.ip().is_unspecified())
}

impl QuicListener {
    fn validate(&self, edge_runs: bool, key: &dyn Fn(&str) -> String, problems: &mut Problems) {
        if edge_runs && self.cert_file.is_none() {
            problems.refuse(
                key("cert_file"),
                "required: a QUIC listener needs a certificate",
            );
        }
        if edge_runs && self.key_file.is_none() {
            problems.refuse(
                key("key_file"),
                "required: a QUIC listener needs a private key",
            );
        }
        if self.cert_file.is_some() != self.key_file.is_some() {
            problems.refuse(key("key_file"), "cert_file and key_file go together");
        }
        if self.require_client_cert && self.client_ca_file.is_none() {
            problems.refuse(
                key("client_ca_file"),
                "required with require_client_cert: it names the CA client certificates must \
                 chain to",
            );
        }
        if self.identity_from_cn && !self.require_client_cert {
            problems.refuse(
                key("identity_from_cn"),
                "needs require_client_cert: a client without a certificate would have no identity",
            );
        }
        if self.identity_from_cn && self.enable_authn {
            problems.refuse(
                key("enable_authn"),
                "must be false with identity_from_cn: the certificate authenticates the client, \
                 which sends no password (R2 rule 4)",
            );
        }
        if let Err(reason) = check_mountpoint(&self.mountpoint) {
            problems.refuse(key("mountpoint"), reason);
        }
        if !(1..=MOST_STREAMS).contains(&self.max_streams) {
            problems.refuse(
                key("max_streams"),
                format!(
                    "from 1, the control stream, to {MOST_STREAMS}; quinn keeps state for every \
                     stream a client may open (R7, F1)"
                ),
            );
        }
        for (setting, window) in [
            ("stream_window", self.stream_window),
            ("connection_window", self.connection_window),
        ] {
            if window.bytes() < 1_024 {
                problems.refuse(key(setting), "at least 1KiB");
            }
        }
    }
}

/// Checks a mountpoint: placeholders OpenQTT knows, and no wildcard, since every topic a client
/// sends goes under it.
fn check_mountpoint(mountpoint: &str) -> Result<(), String> {
    if mountpoint.contains(['+', '#']) {
        return Err("a mountpoint cannot hold the wildcards `+` or `#`".to_owned());
    }
    if mountpoint.contains('\0') {
        return Err("a mountpoint cannot hold U+0000".to_owned());
    }
    let mut rest = mountpoint;
    while let Some(start) = rest.find("${") {
        let after = &rest[start..];
        let Some(placeholder) = PLACEHOLDERS.iter().find(|known| after.starts_with(**known)) else {
            return Err("the only placeholders are `${username}` and `${clientid}`".to_owned());
        };
        rest = &after[placeholder.len()..];
    }
    Ok(())
}

impl Limits {
    fn validate(&self, problems: &mut Problems) {
        let keep_alive = [
            ("limits.keep_alive_min", self.keep_alive_min),
            ("limits.keep_alive_max", self.keep_alive_max),
        ];
        for (key, value) in keep_alive {
            match value.as_whole_secs() {
                None => problems.refuse(key, "Keep Alive is a whole number of seconds"),
                Some(0) => problems.refuse(key, "at least 1s: a Keep Alive of 0 turns it off"),
                Some(secs) if secs > LARGEST_KEEP_ALIVE => problems.refuse(
                    key,
                    format!("at most {LARGEST_KEEP_ALIVE}s, the largest Keep Alive MQTT carries"),
                ),
                Some(_) => {}
            }
        }
        if self.keep_alive_min > self.keep_alive_max {
            problems.refuse("limits.keep_alive_min", "longer than limits.keep_alive_max");
        }
        if self.receive_maximum == 0 {
            problems.refuse(
                "limits.receive_maximum",
                "at least 1: MQTT has no Receive Maximum of 0",
            );
        }
        let packet = self.maximum_packet_size.bytes();
        if !(1_024..=LARGEST_PACKET).contains(&packet) {
            problems.refuse(
                "limits.maximum_packet_size",
                format!(
                    "from 1KiB, below which ordinary packets would be refused, to \
                     {LARGEST_PACKET} bytes, the largest packet MQTT can describe"
                ),
            );
        }
        match self.session_expiry_max.as_whole_secs() {
            None => problems.refuse(
                "limits.session_expiry_max",
                "the Session Expiry Interval is a whole number of seconds",
            ),
            Some(secs) if secs > LARGEST_SESSION_EXPIRY => problems.refuse(
                "limits.session_expiry_max",
                format!("at most {LARGEST_SESSION_EXPIRY}s; MQTT reads one second more as never"),
            ),
            Some(_) => {}
        }
        for (key, value) in [
            ("limits.max_subscriptions", self.max_subscriptions),
            ("limits.max_topic_levels", self.max_topic_levels),
            ("limits.max_queued_messages", self.max_queued_messages),
        ] {
            if value == 0 {
                problems.refuse(key, "at least 1");
            }
        }
        if self.max_client_id_length < SHORTEST_CLIENT_ID_LIMIT {
            problems.refuse(
                "limits.max_client_id_length",
                format!(
                    "at least {SHORTEST_CLIENT_ID_LIMIT}: MQTT requires every server to accept \
                     Client Identifiers that long [MQTT-3.1.3-5]"
                ),
            );
        }
    }
}

impl Auth {
    fn validate(&self, problems: &mut Problems) {
        if self.jwt.enabled {
            problems.refuse(
                "auth.jwt.enabled",
                "JWT authentication is not available yet; leave it false",
            );
        }
        if self.http.enabled {
            problems.refuse(
                "auth.http.enabled",
                "HTTP authentication is not available yet; leave it false",
            );
        }
    }
}

impl Edge {
    fn validate(&self, problems: &mut Problems) {
        if self.interest_threshold == 0 {
            problems.refuse(
                "edge.interest_threshold",
                "at least 1; 0 would coarsen every level",
            );
        }
    }
}

impl Router {
    fn validate(&self, problems: &mut Problems) {
        if self.shards == 0 {
            problems.refuse("router.shards", "at least 1");
        }
    }
}

impl Log {
    fn validate(&self, problems: &mut Problems) {
        if self.partitions == 0 {
            problems.refuse("log.partitions", "at least 1");
        }
        if ![1, 3].contains(&self.replication_factor) {
            problems.refuse("log.replication_factor", "1 or 3 (R3)");
        }
    }
}

impl Observability {
    fn validate(&self, problems: &mut Problems) {
        if self.log_level.trim().is_empty() {
            problems.refuse("observability.log_level", "empty; `info` is the default");
        }
        let otlp = &self.otlp;
        match &otlp.endpoint {
            None => {
                if otlp.headers_file.is_some() {
                    problems.refuse(
                        "observability.otlp.headers_file",
                        "has no effect without observability.otlp.endpoint",
                    );
                }
                if otlp.ca_file.is_some() {
                    problems.refuse(
                        "observability.otlp.ca_file",
                        "has no effect without observability.otlp.endpoint",
                    );
                }
            }
            Some(endpoint) => {
                if otlp.ca_file.is_some() && !endpoint.is_https() {
                    problems.refuse(
                        "observability.otlp.ca_file",
                        "set, but the endpoint is not https: nothing would check it",
                    );
                }
            }
        }
        if otlp.interval.as_millis() < 1_000 {
            problems.refuse("observability.otlp.interval", "at least 1s");
        }
        if otlp.timeout.as_millis() == 0 {
            problems.refuse("observability.otlp.timeout", "longer than 0s");
        }
        if self.prometheus.enabled && self.prometheus.token_file.is_none() {
            problems.refuse(
                "observability.prometheus.token_file",
                "required when the endpoint is enabled: metrics are served only to a scraper \
                 that presents the token",
            );
        }
    }
}

impl Storage {
    fn validate(&self, problems: &mut Problems) {
        if self.data_dir.as_os_str().is_empty() {
            problems.refuse("storage.data_dir", "empty; name a directory");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_dns_labels_or_with_dots_dns_names() {
        for name in ["openqtt", "a", "edge-0", "openqtt-log-2"] {
            assert!(is_name(name, 63, false), "{name}");
        }
        assert!(is_name("edge-0.openqtt-edge", 253, true));
        for name in ["", "-a", "a-", "A", "a_b", "a.b", "é"] {
            assert!(!is_name(name, 63, false), "{name}");
        }
        assert!(!is_name(&"a".repeat(64), 63, false));
        assert!(!is_name(".a", 253, true));
    }

    #[test]
    fn mountpoints_hold_known_placeholders_and_no_wildcards() {
        for mountpoint in [
            "",
            "ingest/",
            "ingest/${username}/",
            "a/${clientid}/${username}",
            "$x/",
        ] {
            assert_eq!(check_mountpoint(mountpoint), Ok(()), "{mountpoint}");
        }
        for mountpoint in ["ingest/+/", "ingest/#", "${user}/", "${username", "a\0b"] {
            assert!(check_mountpoint(mountpoint).is_err(), "{mountpoint}");
        }
    }

    #[test]
    fn binds_overlap_on_one_port_when_an_address_matches_or_is_a_wildcard() {
        let bind = |text: &str| text.parse::<SocketAddr>().unwrap();
        assert!(overlap(bind("0.0.0.0:443"), bind("10.0.0.1:443")));
        assert!(overlap(bind("10.0.0.1:443"), bind("10.0.0.1:443")));
        assert!(overlap(bind("[::]:443"), bind("10.0.0.1:443")));
        assert!(!overlap(bind("10.0.0.1:443"), bind("10.0.0.2:443")));
        assert!(!overlap(bind("0.0.0.0:443"), bind("0.0.0.0:14567")));
    }
}
