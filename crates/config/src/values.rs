//! The value types settings hold beyond strings, numbers, booleans and paths.
//!
//! Each reads from the same text in the file and in a variable, and writes back the text it read
//! from, so `openqtt config print` produces a file that loads to the same settings.

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A span of time: a whole number and one unit, `ms`, `s`, `m`, `h` or `d`, such as `30s` or
/// `7d`. A bare number is refused, because nothing could say which unit it meant.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Duration {
    millis: u64,
}

/// Units from the largest down, so a duration prints in the largest unit that divides it.
const DURATION_UNITS: [(&str, u64); 5] = [
    ("d", 86_400_000),
    ("h", 3_600_000),
    ("m", 60_000),
    ("s", 1_000),
    ("ms", 1),
];

impl Duration {
    /// A duration of `millis` milliseconds.
    pub const fn from_millis(millis: u64) -> Self {
        Self { millis }
    }

    /// A duration of `secs` seconds, saturating at the longest one this type holds.
    pub const fn from_secs(secs: u64) -> Self {
        Self {
            millis: secs.saturating_mul(1_000),
        }
    }

    /// The duration in milliseconds.
    pub const fn as_millis(self) -> u64 {
        self.millis
    }

    /// The duration in whole seconds, or `None` when it is not a whole number of them.
    pub const fn as_whole_secs(self) -> Option<u64> {
        if self.millis.is_multiple_of(1_000) {
            Some(self.millis / 1_000)
        } else {
            None
        }
    }

    /// The same duration as the standard library holds it.
    pub const fn as_std(self) -> std::time::Duration {
        std::time::Duration::from_millis(self.millis)
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.millis == 0 {
            return f.write_str("0s");
        }
        let (unit, size) = DURATION_UNITS
            .iter()
            .find(|(_, size)| self.millis.is_multiple_of(*size))
            .copied()
            .unwrap_or(("ms", 1));
        write!(f, "{}{unit}", self.millis / size)
    }
}

impl FromStr for Duration {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let (number, unit) = split_number(text);
        let size = DURATION_UNITS
            .iter()
            .find(|(name, _)| *name == unit)
            .map(|(_, size)| *size);
        match (number.parse::<u64>(), size) {
            (Ok(number), Some(size)) => number
                .checked_mul(size)
                .map(Self::from_millis)
                .ok_or_else(|| format!("`{text}` is longer than any duration OpenQTT holds")),
            _ => Err(format!(
                "expected a duration such as `500ms`, `30s`, `20m`, `2h` or `7d`, found `{text}`"
            )),
        }
    }
}

impl Serialize for Duration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DurationVisitor;

        impl Visitor<'_> for DurationVisitor {
            type Value = Duration;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a duration such as `30s`")
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<Duration, E> {
                text.parse().map_err(E::custom)
            }

            fn visit_i64<E: de::Error>(self, number: i64) -> Result<Duration, E> {
                Err(E::custom(format!(
                    "a duration needs a unit: `{number}s` for seconds, `{number}ms` for milliseconds"
                )))
            }

            fn visit_u64<E: de::Error>(self, number: u64) -> Result<Duration, E> {
                Err(E::custom(format!(
                    "a duration needs a unit: `{number}s` for seconds, `{number}ms` for milliseconds"
                )))
            }
        }

        deserializer.deserialize_any(DurationVisitor)
    }
}

/// A number of bytes: a whole number, or a whole number and one unit, `B`, `KiB`, `MiB` or
/// `GiB`, such as `1MiB`. Units are binary: `1KiB` is 1,024 bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ByteSize {
    bytes: u64,
}

/// Units from the largest down, so a size prints in the largest unit that divides it.
const BYTE_UNITS: [(&str, u64); 4] = [
    ("GiB", 1 << 30),
    ("MiB", 1 << 20),
    ("KiB", 1 << 10),
    ("B", 1),
];

impl ByteSize {
    /// A size of `bytes` bytes.
    pub const fn new(bytes: u64) -> Self {
        Self { bytes }
    }

    /// A size of `kib` KiB, saturating at the largest size this type holds.
    pub const fn kib(kib: u64) -> Self {
        Self {
            bytes: kib.saturating_mul(1 << 10),
        }
    }

    /// A size of `mib` MiB, saturating at the largest size this type holds.
    pub const fn mib(mib: u64) -> Self {
        Self {
            bytes: mib.saturating_mul(1 << 20),
        }
    }

    /// The size in bytes.
    pub const fn bytes(self) -> u64 {
        self.bytes
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.bytes == 0 {
            return f.write_str("0B");
        }
        let (unit, size) = BYTE_UNITS
            .iter()
            .find(|(_, size)| self.bytes.is_multiple_of(*size))
            .copied()
            .unwrap_or(("B", 1));
        write!(f, "{}{unit}", self.bytes / size)
    }
}

impl FromStr for ByteSize {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let (number, unit) = split_number(text);
        let size = if unit.is_empty() {
            Some(1)
        } else {
            BYTE_UNITS
                .iter()
                .find(|(name, _)| *name == unit)
                .map(|(_, size)| *size)
        };
        match (number.parse::<u64>(), size) {
            (Ok(number), Some(size)) => number
                .checked_mul(size)
                .map(Self::new)
                .ok_or_else(|| format!("`{text}` is larger than any size OpenQTT holds")),
            _ => Err(format!(
                "expected a size such as `65536`, `64KiB`, `1MiB` or `1GiB`, found `{text}`"
            )),
        }
    }
}

impl Serialize for ByteSize {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ByteSizeVisitor;

        impl Visitor<'_> for ByteSizeVisitor {
            type Value = ByteSize;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a size such as `1MiB`")
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<ByteSize, E> {
                text.parse().map_err(E::custom)
            }

            fn visit_i64<E: de::Error>(self, number: i64) -> Result<ByteSize, E> {
                u64::try_from(number)
                    .map(ByteSize::new)
                    .map_err(|_| E::custom(format!("a size cannot be negative, found `{number}`")))
            }

            fn visit_u64<E: de::Error>(self, number: u64) -> Result<ByteSize, E> {
                Ok(ByteSize::new(number))
            }
        }

        deserializer.deserialize_any(ByteSizeVisitor)
    }
}

/// The leading digits of `text`, and the rest.
fn split_number(text: &str) -> (&str, &str) {
    let end = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    text.split_at(end)
}

/// An `http://` or `https://` URL with no credentials, query or fragment, valid by the WHATWG
/// URL rules that browsers and the `url` crate follow.
///
/// A URL carrying a user or password is refused, because the setting would then hold a secret
/// inline: credentials go in a file. No error about a URL repeats it, so a password typed into
/// one cannot reach a log through the message that refuses it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Endpoint(String);

impl Endpoint {
    /// The URL as written.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether the URL is `https://`.
    pub fn is_https(&self) -> bool {
        self.0.starts_with("https://")
    }

    /// The URL of `path` below this one, with exactly one `/` between them: the base
    /// `http://collector:4318` and `/v1/metrics` give `http://collector:4318/v1/metrics`.
    pub fn join(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.0.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Endpoint {
    type Err = String;

    /// Checks the text as written first, so that no message about it can repeat a credential and
    /// so that nothing the WHATWG rules would quietly repair, an extra slash, a tab, a backslash,
    /// passes as something else; then parses it by those rules, which hold the host, the IP
    /// address and the port to their full syntax.
    fn from_str(text: &str) -> Result<Self, String> {
        let rest = text
            .strip_prefix("https://")
            .or_else(|| text.strip_prefix("http://"))
            .ok_or("expected a URL starting with http:// or https://")?;
        let authority = rest.split('/').next().unwrap_or_default();
        if authority.contains('@') {
            return Err(
                "a URL must not carry credentials; give them in the headers file instead".into(),
            );
        }
        if text
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || c == '\\')
        {
            return Err("a URL cannot contain spaces, backslashes or control characters".into());
        }
        if text.contains(['?', '#']) {
            return Err("the URL must not have a query or a fragment".into());
        }
        if authority.is_empty() {
            return Err("the URL names no host".into());
        }
        // A parse error names what is wrong, never the text.
        let url = url::Url::parse(text).map_err(|error| format!("not a valid URL: {error}"))?;
        if url.host_str().is_none_or(str::is_empty) {
            return Err("the URL names no host".into());
        }
        if url.port() == Some(0) {
            return Err("the URL's port is not a number from 1 to 65535".into());
        }
        Ok(Self(text.to_owned()))
    }
}

impl Serialize for Endpoint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Endpoint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

/// `host` and the port after its last `:`, reading an IPv6 address in brackets whole.
fn split_host_port(authority: &str) -> (&str, Option<&str>) {
    if let Some(inner) = authority.strip_prefix('[') {
        return match inner.split_once(']') {
            Some((host, rest)) => (host, rest.strip_prefix(':')),
            None => ("", None),
        };
    }
    match authority.rsplit_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (authority, None),
    }
}

/// A host name or IP address and a port, `host:port`. An IPv6 address goes in brackets:
/// `[2001:db8::1]:7000`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostPort {
    host: String,
    port: u16,
}

impl HostPort {
    /// The host name or address, without brackets.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port.
    pub const fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for HostPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

impl FromStr for HostPort {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let refuse = || format!("expected `host:port`, such as `openqtt-log:7000`, found `{text}`");
        let (host, port) = split_host_port(text);
        let port = port
            .and_then(|port| port.parse::<u16>().ok())
            .filter(|port| *port > 0)
            .ok_or_else(refuse)?;
        let bracketed = text.starts_with('[');
        let well_formed = !host.is_empty()
            && !host
                .chars()
                .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '/' | '@' | '[' | ']'))
            && (bracketed || !host.contains(':'));
        if !well_formed {
            return Err(refuse());
        }
        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }
}

impl Serialize for HostPort {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for HostPort {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

/// The path of a file holding a secret: a private key, a token, a list of passwords.
///
/// The path is the setting, and prints like any other. The contents are not, and are read only
/// when they are needed, into a [`Secret`]. A secret is never a value in the file or a
/// variable, so it never appears in `openqtt config print` or in an error about the
/// configuration.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretFile(PathBuf);

impl SecretFile {
    /// The secret held in the file at `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self(path.into())
    }

    /// The file's path.
    pub fn path(&self) -> &Path {
        &self.0
    }

    /// Reads the secret.
    ///
    /// # Errors
    ///
    /// When the file cannot be read. The error names the path, never the contents.
    pub fn read(&self) -> std::io::Result<Secret> {
        std::fs::read(&self.0).map(Secret)
    }
}

/// The contents of a secret file. Debug and Display both print `<redacted>`, so a secret
/// cannot reach a log line or an error message by accident; [`Secret::expose`] is the one way
/// to its bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(Vec<u8>);

impl Secret {
    /// A secret holding `bytes`.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// The secret's bytes, for the one place that uses them.
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_read_one_unit_and_print_the_largest_that_divides() {
        for (text, millis, printed) in [
            ("0s", 0, "0s"),
            ("0ms", 0, "0s"),
            ("1500ms", 1_500, "1500ms"),
            ("30s", 30_000, "30s"),
            ("60s", 60_000, "1m"),
            ("1200s", 1_200_000, "20m"),
            ("20m", 1_200_000, "20m"),
            ("2h", 7_200_000, "2h"),
            ("604800s", 604_800_000, "7d"),
            ("7d", 604_800_000, "7d"),
        ] {
            let duration: Duration = text.parse().unwrap();
            assert_eq!(duration.as_millis(), millis, "{text}");
            assert_eq!(duration.to_string(), printed, "{text}");
        }
    }

    #[test]
    fn durations_without_a_known_unit_are_refused() {
        for text in [
            "", "30", "s", "30 s", "-30s", "+30s", "1h30m", "30sec", "1.5s", "30S",
        ] {
            let error = text.parse::<Duration>().unwrap_err();
            assert!(error.contains("expected a duration"), "{text}: {error}");
        }
        let error = "99999999999999999999d".parse::<Duration>().unwrap_err();
        assert!(error.contains("expected a duration"), "{error}");
        let error = "999999999999999999d".parse::<Duration>().unwrap_err();
        assert!(error.contains("longer than any duration"), "{error}");
    }

    #[test]
    fn whole_seconds_are_told_apart() {
        assert_eq!(Duration::from_secs(10).as_whole_secs(), Some(10));
        assert_eq!(Duration::from_millis(10_500).as_whole_secs(), None);
        assert_eq!(Duration::from_secs(u64::MAX).as_millis(), u64::MAX);
    }

    #[test]
    fn a_bare_number_is_not_a_duration_and_the_error_says_what_to_write() {
        let error = Duration::deserialize(toml::Value::Integer(30)).unwrap_err();
        assert!(error.to_string().contains("`30s` for seconds"), "{error}");
    }

    #[test]
    fn sizes_read_bytes_or_binary_units_and_print_the_largest_that_divides() {
        for (text, bytes, printed) in [
            ("0", 0, "0B"),
            ("1000", 1_000, "1000B"),
            ("1024", 1_024, "1KiB"),
            ("64KiB", 65_536, "64KiB"),
            ("1MiB", 1_048_576, "1MiB"),
            ("1048576B", 1_048_576, "1MiB"),
            ("8MiB", 8_388_608, "8MiB"),
            ("2GiB", 2_147_483_648, "2GiB"),
        ] {
            let size: ByteSize = text.parse().unwrap();
            assert_eq!(size.bytes(), bytes, "{text}");
            assert_eq!(size.to_string(), printed, "{text}");
        }
        assert_eq!(
            ByteSize::deserialize(toml::Value::Integer(4096)).unwrap(),
            ByteSize::kib(4)
        );
        assert!(ByteSize::deserialize(toml::Value::Integer(-1)).is_err());
        for text in ["", "1MB", "1 MiB", "1mib", "MiB", "1.5MiB", "-1"] {
            assert!(text.parse::<ByteSize>().is_err(), "{text}");
        }
    }

    #[test]
    fn endpoints_are_http_or_https_urls_without_credentials() {
        for text in [
            "http://collector:4318",
            "https://collector.example:4318/",
            "https://otlp.example/prefix",
            "http://[::1]:4318",
            "http://127.0.0.1",
        ] {
            let endpoint: Endpoint = text.parse().unwrap();
            assert_eq!(endpoint.as_str(), text);
        }
        assert!("https://a".parse::<Endpoint>().unwrap().is_https());
        assert!(!"http://a".parse::<Endpoint>().unwrap().is_https());
        for (text, why) in [
            ("collector:4318", "starting with http"),
            ("ftp://collector", "starting with http"),
            ("http://", "names no host"),
            ("http:///v1", "names no host"),
            ("http://collector:0", "port"),
            ("http://collector:http", "port"),
            ("http://collector:4318?x=1", "query"),
            ("http://collector:4318#top", "fragment"),
            ("http://collector 4318", "spaces"),
            ("http://[not-ipv6]", "not a valid URL"),
            ("http://[::1", "not a valid URL"),
            ("http://999.1.1.1:4318", "not a valid URL"),
            ("http://collector:65536", "port"),
            ("http://col\tlector:4318", "control characters"),
            ("http://exa%mple:4318", "not a valid URL"),
        ] {
            let error = text.parse::<Endpoint>().unwrap_err();
            assert!(error.contains(why), "{text}: {error}");
        }
    }

    #[test]
    fn an_endpoint_with_credentials_is_refused_without_repeating_them() {
        for text in [
            "https://user:hunter2@collector:4318",
            "http://hunter2@collector",
            "ftp://user:hunter2@collector",
            "https://user:hunter2@[not-ipv6]",
            "https://user:hunter2@collector:99999",
            "https://hunter2:@collector",
        ] {
            let error = text.parse::<Endpoint>().unwrap_err();
            assert!(!error.contains("hunter2"), "{error}");
        }
        let error = "https://user:hunter2@collector"
            .parse::<Endpoint>()
            .unwrap_err();
        assert!(error.contains("credentials"), "{error}");
    }

    #[test]
    fn joining_a_path_puts_one_slash_between() {
        for base in ["http://c:4318", "http://c:4318/"] {
            let endpoint: Endpoint = base.parse().unwrap();
            assert_eq!(endpoint.join("/v1/metrics"), "http://c:4318/v1/metrics");
            assert_eq!(endpoint.join("v1/metrics"), "http://c:4318/v1/metrics");
        }
    }

    #[test]
    fn host_ports_read_names_addresses_and_bracketed_ipv6() {
        for (text, host, port) in [
            ("openqtt-log:7000", "openqtt-log", 7000),
            ("10.0.0.1:7000", "10.0.0.1", 7000),
            ("[2001:db8::1]:7000", "2001:db8::1", 7000),
        ] {
            let seed: HostPort = text.parse().unwrap();
            assert_eq!((seed.host(), seed.port()), (host, port));
            assert_eq!(seed.to_string(), text);
        }
        for text in [
            "",
            "openqtt-log",
            ":7000",
            "openqtt-log:",
            "openqtt-log:0",
            "openqtt-log:70000",
            "2001:db8::1:7000",
            "[2001:db8::1]",
            "a b:7000",
            "user@host:7000",
        ] {
            assert!(text.parse::<HostPort>().is_err(), "{text}");
        }
    }

    #[test]
    fn a_secret_prints_redacted_and_exposes_its_bytes_only_when_asked() {
        let secret = Secret::new(b"hunter2".to_vec());
        assert_eq!(format!("{secret}"), "<redacted>");
        assert_eq!(format!("{secret:?}"), "Secret(<redacted>)");
        assert_eq!(format!("{:?}", Some(&secret)), "Some(Secret(<redacted>))");
        assert_eq!(secret.expose(), b"hunter2");
    }
}
