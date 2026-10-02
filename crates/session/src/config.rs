//! What a listener configures for the sessions it holds, and what the transport knows about one
//! connection.

use std::fmt;
use std::num::{NonZeroU16, NonZeroU32, NonZeroUsize};
use std::time::Duration;

use openqtt_codec::QoS;
use openqtt_topic::Mountpoint;

use crate::Error;

/// The settings one listener applies to every session it holds: the limits it announces in
/// CONNACK and holds clients to, and the choices report R1 makes where MQTT 5.0 leaves one open.
///
/// [`Config::default`] is R1's defaults, each named by the decision or open choice that sets it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Receive Maximum announced in CONNACK: the most QoS 1 and 2 PUBLISH packets a client may
    /// leave unacknowledged, held to with DISCONNECT 0x93 as each arrives. A QoS 2 PUBLISH that
    /// repeats one whose exchange is still open counts once with it. It also caps the client's
    /// own Receive Maximum when the server sends (R1, O3 and D13). 32 by default.
    pub receive_maximum: NonZeroU16,
    /// Maximum Packet Size announced in CONNACK, for the whole packet (R1, O5 and D23). The
    /// transport's decoder refuses a larger packet, which reaches the machine as a decoding
    /// error. 1 MiB by default.
    pub maximum_packet_size: NonZeroU32,
    /// Topic Alias Maximum announced in CONNACK, and the most aliases the server assigns on the
    /// PUBLISH packets it sends (R1, O6 and D22). 64 by default; 0 turns aliases off.
    pub topic_alias_maximum: u16,
    /// The Keep Alive values used as sent; any other gets a Server Keep Alive (R1, O4).
    pub keep_alive: KeepAliveBounds,
    /// The longest Session Expiry Interval kept, in seconds (R1, O7 and D18). 7 days by
    /// default.
    pub session_expiry_maximum: u32,
    /// Maximum QoS (R1, O19). 2 by default, which CONNACK leaves out.
    pub maximum_qos: QoS,
    /// Retain Available (R1, O19). True by default, which CONNACK leaves out.
    pub retain_available: bool,
    /// Wildcard Subscription Available (R1, O19). True by default, which CONNACK leaves out.
    pub wildcard_subscription_available: bool,
    /// Subscription Identifiers Available (R1, O19). True by default, which CONNACK leaves out.
    pub subscription_identifiers_available: bool,
    /// Shared Subscription Available (R1, O19). True by default, which CONNACK leaves out.
    pub shared_subscription_available: bool,
    /// Subscriptions per session; a new one beyond it gets SUBACK 0x97 (R1, O16). 1,000 by
    /// default.
    pub maximum_subscriptions: usize,
    /// Levels in a Topic Name or Topic Filter, counted before mounting (R1, O16, and report R2
    /// rule 8). 128 by default.
    pub maximum_topic_levels: usize,
    /// Messages a session holds before they are sent: one more ends the session (R1, O12).
    /// 1,000 by default.
    pub maximum_queued_messages: usize,
    /// Packets from the client the machine holds unprocessed, while it waits on an answer,
    /// before it asks the transport to stop reading ([`Effect::PauseReading`]). 64 by default.
    ///
    /// [`Effect::PauseReading`]: crate::Effect::PauseReading
    pub maximum_pending_packets: NonZeroUsize,
    /// Bytes of those packets, as encoded, before it asks the same. 256 KiB by default; a
    /// single packet up to the Maximum Packet Size is always taken.
    pub maximum_pending_bytes: NonZeroUsize,
    /// How long a connection has to send its CONNECT and be accepted, or `None` for no limit.
    /// 10 seconds by default.
    pub connect_timeout: Option<Duration>,
    /// Where a session's Client Identifier and User Name come from (R2 rule 4, R1 D20).
    pub identity: Identity,
    /// The mountpoint (R2 rule 6), resolved for each connection once it is authenticated.
    pub mountpoint: Option<Mountpoint>,
}

impl Config {
    /// R1's default Receive Maximum (O3).
    pub const RECEIVE_MAXIMUM: u16 = 32;
    /// R1's default Maximum Packet Size, 1 MiB (O5).
    pub const MAXIMUM_PACKET_SIZE: u32 = 1 << 20;
    /// R1's default Topic Alias Maximum (O6).
    pub const TOPIC_ALIAS_MAXIMUM: u16 = 64;
    /// R1's cap on Session Expiry Interval, 7 days in seconds (O7).
    pub const SESSION_EXPIRY_MAXIMUM: u32 = 7 * 24 * 60 * 60;
    /// R1's limit on subscriptions per session (O16).
    pub const MAXIMUM_SUBSCRIPTIONS: usize = 1_000;
    /// R1's limit on topic levels (O16, R2 rule 8).
    pub const MAXIMUM_TOPIC_LEVELS: usize = 128;
    /// R1's limit on the messages a session holds (O12).
    pub const MAXIMUM_QUEUED_MESSAGES: usize = 1_000;
    /// The default limit on unprocessed packets.
    pub const MAXIMUM_PENDING_PACKETS: usize = 64;
    /// The default limit on the bytes of unprocessed packets, 256 KiB.
    pub const MAXIMUM_PENDING_BYTES: usize = 256 << 10;
    /// The default time a connection has to be accepted.
    pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
}

impl Default for Config {
    fn default() -> Self {
        Self {
            receive_maximum: NonZeroU16::new(Self::RECEIVE_MAXIMUM).unwrap_or(NonZeroU16::MIN),
            maximum_packet_size: NonZeroU32::new(Self::MAXIMUM_PACKET_SIZE)
                .unwrap_or(NonZeroU32::MAX),
            topic_alias_maximum: Self::TOPIC_ALIAS_MAXIMUM,
            keep_alive: KeepAliveBounds::default(),
            session_expiry_maximum: Self::SESSION_EXPIRY_MAXIMUM,
            maximum_qos: QoS::ExactlyOnce,
            retain_available: true,
            wildcard_subscription_available: true,
            subscription_identifiers_available: true,
            shared_subscription_available: true,
            maximum_subscriptions: Self::MAXIMUM_SUBSCRIPTIONS,
            maximum_topic_levels: Self::MAXIMUM_TOPIC_LEVELS,
            maximum_queued_messages: Self::MAXIMUM_QUEUED_MESSAGES,
            maximum_pending_packets: NonZeroUsize::new(Self::MAXIMUM_PENDING_PACKETS)
                .unwrap_or(NonZeroUsize::MIN),
            maximum_pending_bytes: NonZeroUsize::new(Self::MAXIMUM_PENDING_BYTES)
                .unwrap_or(NonZeroUsize::MIN),
            connect_timeout: Some(Self::CONNECT_TIMEOUT),
            identity: Identity::Credentials,
            mountpoint: None,
        }
    }
}

/// The Keep Alive values a session uses as the client sent them (report R1, O4).
///
/// A value from `min` to `max` seconds is used as sent. 0, which would turn liveness off, and
/// anything above `max` get a Server Keep Alive of `max`; anything below `min` gets `min`.
/// Liveness bounds how long a dead client holds its session, its takeover and its will.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeepAliveBounds {
    min: u16,
    max: u16,
}

impl KeepAliveBounds {
    /// R1's bounds: 10 to 1,200 seconds.
    pub const DEFAULT: Self = Self { min: 10, max: 1200 };

    /// Bounds from `min` to `max` seconds.
    ///
    /// # Errors
    ///
    /// [`Error::KeepAliveBounds`] when `min` is 0 or above `max`.
    pub const fn new(min: u16, max: u16) -> Result<Self, Error> {
        if min == 0 || min > max {
            return Err(Error::KeepAliveBounds { min, max });
        }
        Ok(Self { min, max })
    }

    /// The lowest Keep Alive used as sent.
    pub const fn min(self) -> u16 {
        self.min
    }

    /// The highest Keep Alive used as sent.
    pub const fn max(self) -> u16 {
        self.max
    }

    /// The Keep Alive the session uses for a client that asked for `requested` seconds.
    pub const fn apply(self, requested: u16) -> u16 {
        if requested == 0 || requested > self.max {
            self.max
        } else if requested < self.min {
            self.min
        } else {
            requested
        }
    }
}

impl Default for KeepAliveBounds {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Where a session's Client Identifier and User Name come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Identity {
    /// From the CONNECT: its Client Identifier names the session, or the server assigns one
    /// when it is empty, and its User Name and Password go to the authenticator.
    #[default]
    Credentials,
    /// From the client certificate (report R2 rule 4, R1 D20): its subject CN is the Client
    /// Identifier and the User Name, whatever the CONNECT says, and it goes back to the client
    /// as the Assigned Client Identifier. The CONNECT's User Name and Password are ignored.
    Certificate,
}

/// What the transport knows about a connection before its first packet.
#[derive(Clone, PartialEq, Eq)]
pub struct Peer {
    /// The subject CN of the client certificate, which names the session on a listener
    /// configured for certificate identity ([`Identity::Certificate`]).
    pub certificate_cn: Option<String>,
    /// Whether the TLS handshake has completed. A connection accepted with 0-RTT starts with
    /// it false, and the machine holds CONNACK until [`Input::HandshakeComplete`]
    /// (docs/spec/mqtt-over-quic.md, section 4).
    ///
    /// [`Input::HandshakeComplete`]: crate::Input::HandshakeComplete
    pub handshake_complete: bool,
    /// 128 bits from a cryptographically secure source. The machine draws the identifier it
    /// assigns a client that sent an empty one from them (report R1, O9), so a test that fixes
    /// them fixes the identifier.
    pub random: u128,
}

impl Peer {
    /// A connection whose handshake is complete, without a client certificate.
    pub const fn new(random: u128) -> Self {
        Self {
            certificate_cn: None,
            handshake_complete: true,
            random,
        }
    }
}

/// Debug output that never shows the random bits: they predict the identifier a client is
/// assigned.
impl fmt::Debug for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Peer")
            .field("certificate_cn", &self.certificate_cn)
            .field("handshake_complete", &self.handshake_complete)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn r1_o4_keep_alive_bounds() {
        let bounds = KeepAliveBounds::default();
        assert_eq!((bounds.min(), bounds.max()), (10, 1200));
        assert_eq!(bounds.apply(0), 1200);
        assert_eq!(bounds.apply(1), 10);
        assert_eq!(bounds.apply(9), 10);
        assert_eq!(bounds.apply(10), 10);
        assert_eq!(bounds.apply(30), 30);
        assert_eq!(bounds.apply(1200), 1200);
        assert_eq!(bounds.apply(1201), 1200);
        assert_eq!(bounds.apply(u16::MAX), 1200);
        assert_eq!(
            KeepAliveBounds::new(0, 5),
            Err(Error::KeepAliveBounds { min: 0, max: 5 })
        );
        assert_eq!(
            KeepAliveBounds::new(6, 5),
            Err(Error::KeepAliveBounds { min: 6, max: 5 })
        );
        assert_eq!(KeepAliveBounds::new(5, 5).unwrap().apply(60), 5);
    }

    #[test]
    fn r1_defaults() {
        let config = Config::default();
        assert_eq!(config.receive_maximum.get(), 32);
        assert_eq!(config.maximum_packet_size.get(), 1_048_576);
        assert_eq!(config.topic_alias_maximum, 64);
        assert_eq!(config.session_expiry_maximum, 604_800);
        assert_eq!(config.maximum_qos, QoS::ExactlyOnce);
        assert_eq!(config.maximum_subscriptions, 1_000);
        assert_eq!(config.maximum_topic_levels, 128);
        assert_eq!(config.maximum_queued_messages, 1_000);
    }

    #[test]
    fn a_peer_hides_its_random_bits() {
        let peer = Peer::new(0x1234_5678);
        let shown = format!("{peer:?}");
        assert!(!shown.contains("random"), "{shown}");
        assert!(!shown.contains("305419896"), "{shown}");
    }
}
