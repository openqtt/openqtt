//! The rules as a file states them, before they are compiled: what the TOML reader produces,
//! what the 1.x converter builds and writes, and what the tests' reference interpreter reads.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use openqtt_core::QoS;

/// Whether a rule allows or denies what it matches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Decision {
    /// `allow`.
    Allow,
    /// `deny`.
    Deny,
}

impl Decision {
    /// The word the file uses.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        }
    }
}

/// What a rule applies to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ActionKind {
    /// `publish`: PUBLISH, and a CONNECT's Will Message.
    Publish,
    /// `subscribe`: each filter of a SUBSCRIBE, and a delivery to the client.
    Subscribe,
    /// `all`: both.
    All,
}

impl ActionKind {
    /// The word the file uses.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Subscribe => "subscribe",
            Self::All => "all",
        }
    }

    /// Whether it covers publishing.
    pub const fn publishes(self) -> bool {
        matches!(self, Self::Publish | Self::All)
    }

    /// Whether it covers subscribing.
    pub const fn subscribes(self) -> bool {
        matches!(self, Self::Subscribe | Self::All)
    }
}

/// A set of QoS levels.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct QosSet(u8);

impl QosSet {
    /// Every level, which is what a rule without `qos` applies to.
    pub const ALL: Self = Self(0b111);

    /// No level.
    pub const EMPTY: Self = Self(0);

    /// The set with `qos` added.
    #[must_use]
    pub const fn with(self, qos: QoS) -> Self {
        Self(self.0 | (1 << qos.value()))
    }

    /// Whether `qos` is in the set.
    pub const fn contains(self, qos: QoS) -> bool {
        self.0 & (1 << qos.value()) != 0
    }

    /// Whether the set is empty.
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The levels in it, lowest first.
    pub fn iter(self) -> impl Iterator<Item = QoS> {
        [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce]
            .into_iter()
            .filter(move |&qos| self.contains(qos))
    }
}

impl fmt::Debug for QosSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter().map(QoS::value)).finish()
    }
}

/// How a name is matched: a user name, a client identifier, or an attribute's value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum NameMatch {
    /// The name is exactly this, byte for byte.
    Exact(String),
    /// The name begins with this.
    Prefix(String),
    /// The whole name matches this regular expression, in the syntax of the regex crate.
    Regex(String),
}

/// Which clients a rule applies to. Every condition given must hold; a rule with none applies
/// to every client. Within one condition, a list matches when any of its entries does.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Who {
    /// The principal's user name. A client without one never matches.
    pub username: Option<Vec<NameMatch>>,
    /// The Client Identifier.
    pub client_id: Option<Vec<NameMatch>>,
    /// The client's address. A client without one never matches.
    pub address: Option<Vec<Cidr>>,
    /// Attributes of the principal, by name, sorted. A client without the attribute never
    /// matches.
    pub attributes: Vec<(String, Vec<NameMatch>)>,
}

impl Who {
    /// Whether it names no condition, and so applies to every client.
    pub fn is_everyone(&self) -> bool {
        self.username.is_none()
            && self.client_id.is_none()
            && self.address.is_none()
            && self.attributes.is_empty()
    }

    /// Whether it names something about the client other than its address: the address alone
    /// grants nothing (R2 rule 14).
    pub fn names_identity(&self) -> bool {
        self.username.is_some() || self.client_id.is_some() || !self.attributes.is_empty()
    }
}

/// One entry of a rule's `topics`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TopicSpec {
    /// A topic filter, `+` and `#` as MQTT defines them, with `${username}` and `${clientid}`
    /// standing for the client's own.
    Filter(String),
    /// Exactly this text, wildcards included and without placeholders: `{ eq = "..." }`.
    Exact(String),
    /// Every topic, those beginning with `$` included: `{ all = true }`.
    All,
}

/// One rule, as written.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RuleSpec {
    /// What it decides.
    pub decision: Decision,
    /// Whom it applies to.
    pub who: Who,
    /// What it applies to.
    pub action: ActionKind,
    /// The QoS levels it applies to.
    pub qos: QosSet,
    /// For a publish, whether it applies only with RETAIN set (`true`) or only without
    /// (`false`); `None` for either.
    pub retain: Option<bool>,
    /// The topics it applies to, at least one.
    pub topics: Vec<TopicSpec>,
}

impl RuleSpec {
    /// A rule that `decision`s `action` on `topics` for every client, at every QoS.
    pub fn new(decision: Decision, action: ActionKind, topics: Vec<TopicSpec>) -> Self {
        Self {
            decision,
            who: Who::default(),
            action,
            qos: QosSet::ALL,
            retain: None,
            topics,
        }
    }
}

/// An IP network: an address and a prefix length, with the bits past the prefix zero. An IPv4
/// address written as IPv6 (`::ffff:a.b.c.d`) is held as IPv4, as a client's address is
/// compared, so that one listener on `[::]` sees the same rules match as one on `0.0.0.0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Cidr {
    address: IpAddr,
    prefix: u8,
}

/// Why text is not a [`Cidr`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CidrError {
    /// Not an address, or a prefix that is not a number or too long for the address.
    Malformed,
    /// An address with bits set past the prefix: the network it most likely means.
    HostBits(Cidr),
}

impl Cidr {
    /// The network of `address` with `prefix` bits, its other bits cleared.
    pub fn new(address: IpAddr, prefix: u8) -> Option<Self> {
        let (address, prefix) = match canonical(address) {
            IpAddr::V4(v4) => match address {
                // A mapped address keeps the prefix it had within the IPv6 space.
                IpAddr::V6(_) => (IpAddr::V4(v4), prefix.checked_sub(96)?),
                IpAddr::V4(_) => (IpAddr::V4(v4), prefix),
            },
            v6 => (v6, prefix),
        };
        let bits = width(address);
        if prefix > bits {
            return None;
        }
        Some(Self {
            address: mask(address, prefix),
            prefix,
        })
    }

    /// The network's first address.
    pub fn address(self) -> IpAddr {
        self.address
    }

    /// The prefix length.
    pub fn prefix(self) -> u8 {
        self.prefix
    }

    /// Whether `address` is in the network.
    pub fn contains(self, address: IpAddr) -> bool {
        let address = canonical(address);
        width(address) == width(self.address) && mask(address, self.prefix) == self.address
    }

    /// Reads `a.b.c.d/n`, `a:b::/n`, or a bare address, which is a network of one.
    ///
    /// # Errors
    ///
    /// [`CidrError::HostBits`] when bits past the prefix are set, with the network meant;
    /// [`CidrError::Malformed`] for anything else.
    pub fn parse(text: &str) -> Result<Self, CidrError> {
        let (address, prefix) = match text.split_once('/') {
            Some((address, prefix)) => {
                let canonical = !prefix.is_empty()
                    && prefix.bytes().all(|b| b.is_ascii_digit())
                    && (prefix == "0" || !prefix.starts_with('0'));
                if !canonical {
                    return Err(CidrError::Malformed);
                }
                (
                    address,
                    Some(prefix.parse::<u8>().map_err(|_| CidrError::Malformed)?),
                )
            }
            None => (text, None),
        };
        let address: IpAddr = address.parse().map_err(|_| CidrError::Malformed)?;
        let prefix = prefix.unwrap_or(match address {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        });
        let network = Self::new(address, prefix).ok_or(CidrError::Malformed)?;
        if network.address != canonical(address) {
            return Err(CidrError::HostBits(network));
        }
        Ok(network)
    }
}

impl fmt::Display for Cidr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.address, self.prefix)
    }
}

impl FromStr for Cidr {
    type Err = CidrError;

    fn from_str(text: &str) -> Result<Self, CidrError> {
        Self::parse(text)
    }
}

/// An IPv4 address written as IPv6, as the IPv4 address it is.
pub(crate) fn canonical(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        v4 => v4,
    }
}

fn width(address: IpAddr) -> u8 {
    match address {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    }
}

fn mask(address: IpAddr, prefix: u8) -> IpAddr {
    match address {
        IpAddr::V4(v4) => {
            let keep = u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0);
            IpAddr::V4(Ipv4Addr::from(u32::from(v4) & keep))
        }
        IpAddr::V6(v6) => {
            let keep = u128::MAX.checked_shl(128 - u32::from(prefix)).unwrap_or(0);
            IpAddr::V6(Ipv6Addr::from(u128::from(v6) & keep))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    #[test]
    fn networks_hold_their_addresses() {
        let private = Cidr::parse("10.0.0.0/8").unwrap();
        assert!(private.contains(ip("10.1.2.3")));
        assert!(!private.contains(ip("11.0.0.0")));
        assert!(!private.contains(ip("fd00::1")));
        // An IPv4 client on an IPv6 socket is still an IPv4 client.
        assert!(private.contains(ip("::ffff:10.9.8.7")));
        let one = Cidr::parse("192.0.2.7").unwrap();
        assert_eq!(one.prefix(), 32);
        assert!(one.contains(ip("192.0.2.7")) && !one.contains(ip("192.0.2.8")));
        let v6 = Cidr::parse("fd00::/8").unwrap();
        assert!(v6.contains(ip("fd12:3456::1")) && !v6.contains(ip("fe80::1")));
        let everything = Cidr::parse("0.0.0.0/0").unwrap();
        assert!(everything.contains(ip("203.0.113.9")));
        assert!(!everything.contains(ip("::1")));
        let all_v6 = Cidr::parse("::/0").unwrap();
        assert!(all_v6.contains(ip("::1")));
        // A mapped network is held as IPv4.
        let mapped = Cidr::parse("::ffff:10.0.0.0/104").unwrap();
        assert_eq!(mapped, private);
        assert_eq!(mapped.to_string(), "10.0.0.0/8");
        assert_eq!(Cidr::parse("::1").unwrap().to_string(), "::1/128");
    }

    #[test]
    fn host_bits_and_bad_text_are_refused() {
        assert_eq!(
            Cidr::parse("10.0.0.5/8"),
            Err(CidrError::HostBits(Cidr::parse("10.0.0.0/8").unwrap()))
        );
        for bad in [
            "",
            "10.0.0.0/",
            "10.0.0.0/33",
            "10.0.0.0/08",
            "10.0.0.0/+8",
            "fd00::/129",
            "nope",
            "10.0.0/8",
            "::ffff:10.0.0.0/95",
        ] {
            assert_eq!(Cidr::parse(bad), Err(CidrError::Malformed), "{bad}");
        }
    }

    #[test]
    fn qos_sets_hold_levels() {
        let set = QosSet::EMPTY.with(QoS::AtMostOnce).with(QoS::ExactlyOnce);
        assert!(set.contains(QoS::AtMostOnce) && !set.contains(QoS::AtLeastOnce));
        assert_eq!(format!("{set:?}"), "{0, 2}");
        assert!(QosSet::EMPTY.is_empty());
        assert_eq!(QosSet::ALL.iter().count(), 3);
    }
}
