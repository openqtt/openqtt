//! The small types every packet shares: packet types, Quality of Service, the sender of a
//! packet, and identifiers.

use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};

/// A Packet Identifier (section 2.2.1). Zero is never a valid one ([MQTT-2.2.1-3],
/// [MQTT-2.2.1-4]), so the type cannot hold it.
pub type PacketId = NonZeroU16;

/// A Subscription Identifier, 1 to 268,435,455 (section 3.8.2.1.2). The type rules out 0, and
/// encoding refuses a value above [`MAX_VARIABLE_BYTE_INTEGER`](crate::MAX_VARIABLE_BYTE_INTEGER).
pub type SubscriptionId = NonZeroU32;

/// The MQTT Control Packet type: the high four bits of the first byte (section 2.1.2, Table
/// 2-1). Type 0 is reserved and forbidden, so it has no variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum PacketType {
    /// Connection request, client to server.
    Connect = 1,
    /// Connect acknowledgement, server to client.
    ConnAck = 2,
    /// Publish message, either way.
    Publish = 3,
    /// Publish acknowledgement for QoS 1, either way.
    PubAck = 4,
    /// Publish received, QoS 2 delivery part 1, either way.
    PubRec = 5,
    /// Publish release, QoS 2 delivery part 2, either way.
    PubRel = 6,
    /// Publish complete, QoS 2 delivery part 3, either way.
    PubComp = 7,
    /// Subscribe request, client to server.
    Subscribe = 8,
    /// Subscribe acknowledgement, server to client.
    SubAck = 9,
    /// Unsubscribe request, client to server.
    Unsubscribe = 10,
    /// Unsubscribe acknowledgement, server to client.
    UnsubAck = 11,
    /// PING request, client to server.
    PingReq = 12,
    /// PING response, server to client.
    PingResp = 13,
    /// Disconnect notification, either way.
    Disconnect = 14,
    /// Authentication exchange, either way.
    Auth = 15,
}

impl PacketType {
    /// Every packet type, in order of value.
    pub const ALL: [Self; 15] = [
        Self::Connect,
        Self::ConnAck,
        Self::Publish,
        Self::PubAck,
        Self::PubRec,
        Self::PubRel,
        Self::PubComp,
        Self::Subscribe,
        Self::SubAck,
        Self::Unsubscribe,
        Self::UnsubAck,
        Self::PingReq,
        Self::PingResp,
        Self::Disconnect,
        Self::Auth,
    ];

    /// The packet type with this value, or `None` for 0, which is reserved, and for anything
    /// that does not fit in four bits.
    pub const fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            1 => Self::Connect,
            2 => Self::ConnAck,
            3 => Self::Publish,
            4 => Self::PubAck,
            5 => Self::PubRec,
            6 => Self::PubRel,
            7 => Self::PubComp,
            8 => Self::Subscribe,
            9 => Self::SubAck,
            10 => Self::Unsubscribe,
            11 => Self::UnsubAck,
            12 => Self::PingReq,
            13 => Self::PingResp,
            14 => Self::Disconnect,
            15 => Self::Auth,
            _ => return None,
        })
    }

    /// The four-bit value.
    pub const fn value(self) -> u8 {
        self as u8
    }

    /// The name the specification uses, such as `"CONNACK"`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Connect => "CONNECT",
            Self::ConnAck => "CONNACK",
            Self::Publish => "PUBLISH",
            Self::PubAck => "PUBACK",
            Self::PubRec => "PUBREC",
            Self::PubRel => "PUBREL",
            Self::PubComp => "PUBCOMP",
            Self::Subscribe => "SUBSCRIBE",
            Self::SubAck => "SUBACK",
            Self::Unsubscribe => "UNSUBSCRIBE",
            Self::UnsubAck => "UNSUBACK",
            Self::PingReq => "PINGREQ",
            Self::PingResp => "PINGRESP",
            Self::Disconnect => "DISCONNECT",
            Self::Auth => "AUTH",
        }
    }

    /// The low four bits of the first byte that Table 2-2 requires ([MQTT-2.1.3-1]), or
    /// `None` for PUBLISH, whose bits are its DUP, QoS and RETAIN flags.
    pub const fn fixed_header_flags(self) -> Option<u8> {
        match self {
            Self::Publish => None,
            Self::PubRel | Self::Subscribe | Self::Unsubscribe => Some(0b0010),
            _ => Some(0),
        }
    }

    /// Whether `sender` may send this packet type, by the direction of flow in Table 2-1.
    pub const fn is_sent_by(self, sender: Sender) -> bool {
        match self {
            Self::Connect | Self::Subscribe | Self::Unsubscribe | Self::PingReq => {
                matches!(sender, Sender::Client)
            }
            Self::ConnAck | Self::SubAck | Self::UnsubAck | Self::PingResp => {
                matches!(sender, Sender::Server)
            }
            Self::Publish
            | Self::PubAck
            | Self::PubRec
            | Self::PubRel
            | Self::PubComp
            | Self::Disconnect
            | Self::Auth => true,
        }
    }
}

impl fmt::Display for PacketType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Quality of Service, the delivery guarantee of an Application Message (sections 3.3.1.2 and
/// 4.3). Ordered, so the lesser of two is their minimum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(u8)]
pub enum QoS {
    /// QoS 0: at most once.
    #[default]
    AtMostOnce = 0,
    /// QoS 1: at least once.
    AtLeastOnce = 1,
    /// QoS 2: exactly once.
    ExactlyOnce = 2,
}

impl QoS {
    /// The QoS with this value, or `None` for 3 and above. QoS 3 is reserved and must not be
    /// used (Table 3-2).
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::AtMostOnce),
            1 => Some(Self::AtLeastOnce),
            2 => Some(Self::ExactlyOnce),
            _ => None,
        }
    }

    /// The value, 0 to 2.
    pub const fn value(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for QoS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QoS {}", self.value())
    }
}

/// The end of a connection that sent a packet. Some packets, properties and reason codes are
/// only ever sent by one end (Tables 2-1, 3-4, 3-5, 3-10 and 3-11, and [MQTT-3.3.4-6] and
/// [MQTT-3.14.2-2]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Sender {
    /// The client.
    Client,
    /// The server.
    Server,
}

impl fmt::Display for Sender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Client => "client",
            Self::Server => "server",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_2_1_packet_types_and_values() {
        for (index, packet_type) in PacketType::ALL.into_iter().enumerate() {
            let value = u8::try_from(index + 1).unwrap();
            assert_eq!(packet_type.value(), value);
            assert_eq!(PacketType::from_u8(value), Some(packet_type));
        }
        // Type 0 is reserved and forbidden, and nothing above 15 fits in four bits.
        assert_eq!(PacketType::from_u8(0), None);
        assert_eq!(PacketType::from_u8(16), None);
    }

    #[test]
    fn table_2_1_direction_of_flow() {
        use PacketType as T;
        let client_only = [T::Connect, T::Subscribe, T::Unsubscribe, T::PingReq];
        let server_only = [T::ConnAck, T::SubAck, T::UnsubAck, T::PingResp];
        for packet_type in PacketType::ALL {
            let client = packet_type.is_sent_by(Sender::Client);
            let server = packet_type.is_sent_by(Sender::Server);
            if client_only.contains(&packet_type) {
                assert!(client && !server, "{packet_type}");
            } else if server_only.contains(&packet_type) {
                assert!(!client && server, "{packet_type}");
            } else {
                assert!(client && server, "{packet_type}");
            }
        }
    }

    #[test]
    fn mqtt_2_1_3_1_table_2_2_reserved_flags() {
        use PacketType as T;
        for packet_type in PacketType::ALL {
            let expected = match packet_type {
                T::Publish => None,
                T::PubRel | T::Subscribe | T::Unsubscribe => Some(0b0010),
                _ => Some(0b0000),
            };
            assert_eq!(packet_type.fixed_header_flags(), expected, "{packet_type}");
        }
    }

    #[test]
    fn table_3_2_qos_values() {
        assert_eq!(QoS::from_u8(0), Some(QoS::AtMostOnce));
        assert_eq!(QoS::from_u8(1), Some(QoS::AtLeastOnce));
        assert_eq!(QoS::from_u8(2), Some(QoS::ExactlyOnce));
        assert_eq!(QoS::from_u8(3), None);
        assert!(QoS::AtMostOnce < QoS::AtLeastOnce && QoS::AtLeastOnce < QoS::ExactlyOnce);
        assert_eq!(QoS::ExactlyOnce.to_string(), "QoS 2");
    }
}
