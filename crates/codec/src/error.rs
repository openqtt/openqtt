//! The codec's one error type.

use crate::{
    ConnectReasonCode, DisconnectReasonCode, PacketType, PropertyContext, PropertyId, QoS, Sender,
};

/// Why bytes could not be decoded as a packet, or a packet could not be encoded.
///
/// Each variant names the rule it enforces, by statement number where the specification gives
/// one. A decoding error ends the connection: section 4.13 has the receiver close it, after a
/// CONNACK or DISCONNECT carrying [`connack_reason_code`](Self::connack_reason_code) or
/// [`disconnect_reason_code`](Self::disconnect_reason_code).
///
/// Where the specification calls a violation a Malformed Packet or a Protocol Error, the
/// variant says so and maps to 0x81 or 0x82 accordingly. Where it is silent, a bit pattern that
/// cannot be read (a flags byte with an impossible combination) is malformed, and a value that
/// reads but is not allowed is a Protocol Error, by the definitions of section 1.2.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    // Malformed Packet, 0x81.
    /// The first byte names packet type 0, which is reserved and forbidden (Table 2-1).
    /// Malformed.
    #[error("packet type 0 is reserved")]
    ReservedPacketType,
    /// A Variable Byte Integer runs past four bytes, or is longer than its value needs
    /// ([MQTT-1.5.5-1]). Malformed.
    #[error("the {field} is not a valid Variable Byte Integer")]
    MalformedVariableByteInteger {
        /// The field holding it.
        field: &'static str,
    },
    /// The packet, as long as its Remaining Length says, ends inside a field. Malformed.
    #[error("the packet ends inside the {field}")]
    Truncated {
        /// The field cut short.
        field: &'static str,
    },
    /// Bytes follow the last field of a packet, so the packet does not match its format
    /// ([MQTT-3.1.4-1] for CONNECT, section 2 for the rest). Malformed.
    #[error("{count} bytes follow the last field of the {packet_type}")]
    TrailingBytes {
        /// The packet.
        packet_type: PacketType,
        /// How many bytes are left over.
        count: usize,
    },
    /// The low four bits of the first byte break Table 2-2 ([MQTT-2.1.3-1], and for PUBREL,
    /// SUBSCRIBE, UNSUBSCRIBE and AUTH [MQTT-3.6.1-1], [MQTT-3.8.1-1], [MQTT-3.10.1-1] and
    /// [MQTT-3.15.1-1]), or a PUBLISH sets both QoS bits ([MQTT-3.3.1-4]) or DUP at QoS 0
    /// ([MQTT-3.3.1-2]). Malformed.
    #[error("the flags {flags:#06b} are not valid for {packet_type}")]
    InvalidFlags {
        /// The packet.
        packet_type: PacketType,
        /// The low four bits of its first byte.
        flags: u8,
    },
    /// A UTF-8 Encoded String is not well-formed UTF-8, which includes encoding a surrogate
    /// ([MQTT-1.5.4-1]). Malformed.
    #[error("the {field} is not well-formed UTF-8")]
    InvalidUtf8 {
        /// The field holding the string.
        field: &'static str,
    },
    /// A UTF-8 Encoded String contains the null character U+0000 ([MQTT-1.5.4-2]).
    /// Malformed.
    #[error("the {field} contains the null character U+0000")]
    NullCharacter {
        /// The field holding the string.
        field: &'static str,
    },
    /// A property identifier that Table 2-4 does not define, or does not allow where it
    /// appears. Malformed (section 2.2.2.2).
    #[error("property identifier {id:#04x} is not valid in {context}")]
    InvalidPropertyId {
        /// Where the property appeared.
        context: PropertyContext,
        /// The identifier as read.
        id: u32,
    },
    /// The Connect Flags set the reserved bit ([MQTT-3.1.2-3]), set Will QoS to 3
    /// ([MQTT-3.1.2-12]), or set Will QoS or Will Retain without the Will Flag
    /// ([MQTT-3.1.2-11], [MQTT-3.1.2-13]). Malformed.
    #[error("the Connect Flags {flags:#010b} are not valid")]
    InvalidConnectFlags {
        /// The flags byte.
        flags: u8,
    },
    /// The Connect Acknowledge Flags set a reserved bit ([MQTT-3.2.2-1]). Malformed.
    #[error("the Connect Acknowledge Flags {flags:#010b} set a reserved bit")]
    InvalidConnAckFlags {
        /// The flags byte.
        flags: u8,
    },

    /// Subscription Options set a reserved bit, bit 6 or 7 ([MQTT-3.8.3-5]). Malformed.
    #[error("the Subscription Options {options:#010b} set a reserved bit")]
    ReservedSubscriptionOptions {
        /// The options byte.
        options: u8,
    },

    // Protocol Error, 0x82.
    /// A property other than User Property, or Subscription Identifier in a PUBLISH, appears
    /// twice. Each property's section makes that a Protocol Error.
    #[error("the {property} appears more than once")]
    DuplicateProperty {
        /// The property.
        property: PropertyId,
    },
    /// A property value its section does not allow: a Byte other than 0 or 1 where only those
    /// are defined, a Receive Maximum, Maximum Packet Size or Subscription Identifier of 0, a
    /// Maximum QoS of 2. Protocol Error.
    #[error("the {property} has the value {value}, which is not allowed")]
    InvalidPropertyValue {
        /// The property.
        property: PropertyId,
        /// The value.
        value: u32,
    },
    /// A Packet Identifier of 0 ([MQTT-2.2.1-3], [MQTT-2.2.1-4]). Protocol Error.
    #[error("the {packet_type} has Packet Identifier 0")]
    ZeroPacketIdentifier {
        /// The packet.
        packet_type: PacketType,
    },
    /// Subscription Options with a Maximum QoS of 3 or a Retain Handling of 3 (section
    /// 3.8.3.1). Protocol Error.
    #[error("the Subscription Options {options:#010b} ask for QoS 3 or Retain Handling 3")]
    InvalidSubscriptionOptions {
        /// The options byte.
        options: u8,
    },
    /// A SUBSCRIBE or UNSUBSCRIBE without a Topic Filter ([MQTT-3.8.3-2], [MQTT-3.10.3-2]),
    /// or a SUBACK or UNSUBACK without a reason code, though Table 2-5 requires each a
    /// payload. Protocol Error.
    #[error("the {packet_type} has an empty payload")]
    EmptyPayload {
        /// The packet.
        packet_type: PacketType,
    },
    /// A PUBLISH with an empty Topic Name and no Topic Alias to stand for it (section
    /// 3.3.2.1). Protocol Error.
    #[error("the PUBLISH has an empty Topic Name and no Topic Alias")]
    EmptyTopicName,
    /// A reason code that the packet's table does not list ([MQTT-3.2.2-8], [MQTT-3.4.2-1] and
    /// the like). Protocol Error.
    #[error("{code:#04x} is not a {packet_type} reason code")]
    InvalidReasonCode {
        /// The packet.
        packet_type: PacketType,
        /// The code as read.
        code: u8,
    },
    /// Authentication properties without the Authentication Method they belong to:
    /// Authentication Data in a CONNECT without one (section 3.1.2.11.10), or an AUTH without
    /// one (section 3.15.2.2.2). Protocol Error.
    #[error("the {packet_type} has no Authentication Method")]
    MissingAuthenticationMethod {
        /// The packet.
        packet_type: PacketType,
    },
    /// A packet type the sender never sends, by the direction of flow in Table 2-1: a CONNACK
    /// from a client, a SUBSCRIBE from a server. Protocol Error.
    #[error("a {sender} does not send {packet_type}")]
    NotSentBy {
        /// The end that sent it.
        sender: Sender,
        /// The packet.
        packet_type: PacketType,
    },
    /// A reason code only the other end sends: 0x10 in a PUBACK or PUBREC from a client
    /// (Tables 3-4 and 3-5), and the Sent by columns of DISCONNECT and AUTH (Tables 3-10 and
    /// 3-11). Protocol Error.
    #[error("a {sender} does not send {packet_type} reason code {code:#04x}")]
    ReasonCodeNotSentBy {
        /// The end that sent it.
        sender: Sender,
        /// The packet.
        packet_type: PacketType,
        /// The reason code.
        code: u8,
    },
    /// A property only the other end sends: a Subscription Identifier in a PUBLISH from a
    /// client ([MQTT-3.3.4-6]), a Session Expiry Interval in a DISCONNECT from a server
    /// ([MQTT-3.14.2-2]). Protocol Error.
    #[error("a {sender} does not send the {property} in {packet_type}")]
    PropertyNotSentBy {
        /// The end that sent it.
        sender: Sender,
        /// The packet.
        packet_type: PacketType,
        /// The property.
        property: PropertyId,
    },
    /// A CONNACK sets Session Present with a reason code other than Success
    /// ([MQTT-3.2.2-6]). Protocol Error.
    #[error("the CONNACK sets Session Present with a failure reason code")]
    SessionPresentWithError,

    // Other reason codes.
    /// A Topic Alias of 0 ([MQTT-3.3.2-8]): DISCONNECT 0x94, Topic Alias invalid (section
    /// 3.3.4).
    #[error("the Topic Alias is 0")]
    ZeroTopicAlias,
    /// A packet larger than the Maximum Packet Size: one received, or one about to be sent
    /// ([MQTT-3.1.2-24], [MQTT-3.2.2-15]), or one larger than the protocol can express. 0x95,
    /// Packet too large.
    #[error("the packet is {size} bytes, more than the maximum of {maximum}")]
    PacketTooLarge {
        /// The packet's size in bytes, fixed header included.
        size: usize,
        /// The maximum it exceeds.
        maximum: u32,
    },
    /// A CONNECT whose Protocol Name is not `"MQTT"`, whatever its bytes, or whose Protocol
    /// Version is not 5 ([MQTT-3.1.2-1], [MQTT-3.1.2-2]). The name tells protocols apart
    /// (section 3.1.2.1), so one that is not even UTF-8 still names another protocol rather
    /// than making a malformed MQTT packet. The rest of the packet is not read, since another
    /// protocol lays it out differently. CONNACK 0x84, Unsupported Protocol Version; see
    /// [`ProtocolRefusal`](crate::ProtocolRefusal) for the bytes to answer with.
    #[error("protocol {name:?} version {level} is not MQTT 5.0")]
    UnsupportedProtocol {
        /// The Protocol Name, such as `"MQIsdp"` for MQTT 3.1, with any bytes that are not
        /// UTF-8 replaced by U+FFFD.
        name: String,
        /// The Protocol Version, such as 4 for MQTT 3.1.1.
        level: u8,
    },

    // Encoding only.
    /// A PUBLISH with a Packet Identifier at QoS 0 ([MQTT-2.2.1-2]), or without one at QoS 1
    /// or 2 (section 3.3.2.2). Only encoding reports it: on the wire the QoS decides whether
    /// the field is there.
    #[error("the Packet Identifier does not match the PUBLISH's {qos}")]
    PacketIdentifierMismatch {
        /// The packet's QoS.
        qos: QoS,
    },
    /// A string or Binary Data value is longer than its Two Byte Integer length prefix can say
    /// (sections 1.5.4 and 1.5.6). Only encoding reports it.
    #[error("the {field} is {len} bytes long, more than the 65535 a length prefix can say")]
    TooLong {
        /// The field holding the value.
        field: &'static str,
        /// Its length in bytes.
        len: usize,
    },
}

/// Which reason code an error calls for: the kinds section 4.13.1 distinguishes.
enum Class {
    /// The packet cannot be parsed: 0x81 Malformed Packet.
    Malformed,
    /// The packet parses but breaks a rule: 0x82 Protocol Error.
    Protocol,
    /// A Topic Alias of 0: 0x94 Topic Alias invalid in a DISCONNECT.
    TopicAlias,
    /// 0x95 Packet too large.
    TooLarge,
    /// A CONNECT that is not MQTT 5.0: 0x84 Unsupported Protocol Version in a CONNACK.
    Version,
    /// A packet this side built cannot be encoded. Nothing was received, so there is nothing
    /// to tell the peer; 0x83 Implementation specific error stands in should a caller report
    /// it anyway.
    Local,
}

impl Error {
    /// The kind of error, which decides its reason code.
    const fn class(&self) -> Class {
        match self {
            Self::ReservedPacketType
            | Self::MalformedVariableByteInteger { .. }
            | Self::Truncated { .. }
            | Self::TrailingBytes { .. }
            | Self::InvalidFlags { .. }
            | Self::InvalidUtf8 { .. }
            | Self::NullCharacter { .. }
            | Self::InvalidPropertyId { .. }
            | Self::InvalidConnectFlags { .. }
            | Self::InvalidConnAckFlags { .. }
            | Self::ReservedSubscriptionOptions { .. } => Class::Malformed,
            Self::DuplicateProperty { .. }
            | Self::InvalidPropertyValue { .. }
            | Self::ZeroPacketIdentifier { .. }
            | Self::InvalidSubscriptionOptions { .. }
            | Self::EmptyPayload { .. }
            | Self::EmptyTopicName
            | Self::InvalidReasonCode { .. }
            | Self::MissingAuthenticationMethod { .. }
            | Self::NotSentBy { .. }
            | Self::ReasonCodeNotSentBy { .. }
            | Self::PropertyNotSentBy { .. }
            | Self::SessionPresentWithError => Class::Protocol,
            Self::ZeroTopicAlias => Class::TopicAlias,
            Self::PacketTooLarge { .. } => Class::TooLarge,
            Self::UnsupportedProtocol { .. } => Class::Version,
            Self::PacketIdentifierMismatch { .. } | Self::TooLong { .. } => Class::Local,
        }
    }

    /// The reason code of the CONNACK a server may send, before closing the connection, when
    /// the CONNECT fails to decode (sections 3.1.4 and 4.13.1).
    pub const fn connack_reason_code(&self) -> ConnectReasonCode {
        match self.class() {
            Class::Malformed => ConnectReasonCode::MalformedPacket,
            // A CONNECT cannot carry a Topic Alias, so this is only ever a Protocol Error.
            Class::Protocol | Class::TopicAlias => ConnectReasonCode::ProtocolError,
            Class::TooLarge => ConnectReasonCode::PacketTooLarge,
            Class::Version => ConnectReasonCode::UnsupportedProtocolVersion,
            Class::Local => ConnectReasonCode::ImplementationSpecificError,
        }
    }

    /// The reason code of the DISCONNECT that closes a connection when any packet after the
    /// CONNECT fails to decode (section 4.13.1).
    pub const fn disconnect_reason_code(&self) -> DisconnectReasonCode {
        match self.class() {
            Class::Malformed => DisconnectReasonCode::MalformedPacket,
            // After CONNACK an unsupported CONNECT is a second CONNECT, itself a Protocol
            // Error ([MQTT-3.1.0-2]), and DISCONNECT has no 0x84.
            Class::Protocol | Class::Version => DisconnectReasonCode::ProtocolError,
            Class::TopicAlias => DisconnectReasonCode::TopicAliasInvalid,
            Class::TooLarge => DisconnectReasonCode::PacketTooLarge,
            Class::Local => DisconnectReasonCode::ImplementationSpecificError,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One error of each variant with the CONNACK and DISCONNECT reason codes it maps to.
    fn table() -> Vec<(Error, ConnectReasonCode, DisconnectReasonCode)> {
        use ConnectReasonCode as C;
        use DisconnectReasonCode as D;
        let field = "Topic Name";
        let packet_type = PacketType::Publish;
        let malformed = (C::MalformedPacket, D::MalformedPacket);
        let protocol = (C::ProtocolError, D::ProtocolError);
        let local = (
            C::ImplementationSpecificError,
            D::ImplementationSpecificError,
        );
        [
            (Error::ReservedPacketType, malformed),
            (Error::MalformedVariableByteInteger { field }, malformed),
            (Error::Truncated { field }, malformed),
            (
                Error::TrailingBytes {
                    packet_type,
                    count: 1,
                },
                malformed,
            ),
            (
                Error::InvalidFlags {
                    packet_type,
                    flags: 6,
                },
                malformed,
            ),
            (Error::InvalidUtf8 { field }, malformed),
            (Error::NullCharacter { field }, malformed),
            (
                Error::InvalidPropertyId {
                    context: PropertyContext::Connect,
                    id: 0x23,
                },
                malformed,
            ),
            (Error::InvalidConnectFlags { flags: 1 }, malformed),
            (Error::InvalidConnAckFlags { flags: 2 }, malformed),
            (
                Error::ReservedSubscriptionOptions { options: 0x40 },
                malformed,
            ),
            (
                Error::DuplicateProperty {
                    property: PropertyId::ContentType,
                },
                protocol,
            ),
            (
                Error::InvalidPropertyValue {
                    property: PropertyId::ReceiveMaximum,
                    value: 0,
                },
                protocol,
            ),
            (Error::ZeroPacketIdentifier { packet_type }, protocol),
            (Error::InvalidSubscriptionOptions { options: 3 }, protocol),
            (Error::EmptyPayload { packet_type }, protocol),
            (Error::EmptyTopicName, protocol),
            (
                Error::InvalidReasonCode {
                    packet_type,
                    code: 0x8E,
                },
                protocol,
            ),
            (Error::MissingAuthenticationMethod { packet_type }, protocol),
            (
                Error::NotSentBy {
                    sender: Sender::Client,
                    packet_type,
                },
                protocol,
            ),
            (
                Error::ReasonCodeNotSentBy {
                    sender: Sender::Client,
                    packet_type,
                    code: 0x10,
                },
                protocol,
            ),
            (
                Error::PropertyNotSentBy {
                    sender: Sender::Client,
                    packet_type,
                    property: PropertyId::SubscriptionIdentifier,
                },
                protocol,
            ),
            (Error::SessionPresentWithError, protocol),
            (
                Error::ZeroTopicAlias,
                (C::ProtocolError, D::TopicAliasInvalid),
            ),
            (
                Error::PacketTooLarge {
                    size: 11,
                    maximum: 10,
                },
                (C::PacketTooLarge, D::PacketTooLarge),
            ),
            (
                Error::UnsupportedProtocol {
                    name: "MQTT".into(),
                    level: 4,
                },
                (C::UnsupportedProtocolVersion, D::ProtocolError),
            ),
            (
                Error::PacketIdentifierMismatch {
                    qos: QoS::AtMostOnce,
                },
                local,
            ),
            (Error::TooLong { field, len: 65_536 }, local),
        ]
        .into_iter()
        .map(|(error, (connack, disconnect))| (error, connack, disconnect))
        .collect()
    }

    /// A different number for every variant, so the table above is checked to cover them all:
    /// a new variant fails to compile here until it is added.
    fn variant(error: &Error) -> usize {
        match error {
            Error::ReservedPacketType => 0,
            Error::MalformedVariableByteInteger { .. } => 1,
            Error::Truncated { .. } => 2,
            Error::TrailingBytes { .. } => 3,
            Error::InvalidFlags { .. } => 4,
            Error::InvalidUtf8 { .. } => 5,
            Error::NullCharacter { .. } => 6,
            Error::InvalidPropertyId { .. } => 7,
            Error::InvalidConnectFlags { .. } => 8,
            Error::InvalidConnAckFlags { .. } => 9,
            Error::ReservedSubscriptionOptions { .. } => 10,
            Error::DuplicateProperty { .. } => 11,
            Error::InvalidPropertyValue { .. } => 12,
            Error::ZeroPacketIdentifier { .. } => 13,
            Error::InvalidSubscriptionOptions { .. } => 14,
            Error::EmptyPayload { .. } => 15,
            Error::EmptyTopicName => 16,
            Error::InvalidReasonCode { .. } => 17,
            Error::MissingAuthenticationMethod { .. } => 18,
            Error::NotSentBy { .. } => 19,
            Error::ReasonCodeNotSentBy { .. } => 20,
            Error::PropertyNotSentBy { .. } => 21,
            Error::SessionPresentWithError => 22,
            Error::ZeroTopicAlias => 23,
            Error::PacketTooLarge { .. } => 24,
            Error::UnsupportedProtocol { .. } => 25,
            Error::PacketIdentifierMismatch { .. } => 26,
            Error::TooLong { .. } => 27,
        }
    }

    #[test]
    fn mqtt_4_13_1_every_error_maps_to_the_reason_code_its_kind_calls_for() {
        let table = table();
        let mut seen: Vec<usize> = table.iter().map(|(error, ..)| variant(error)).collect();
        seen.sort_unstable();
        assert_eq!(seen, (0..=27).collect::<Vec<_>>());
        for (error, connack, disconnect) in table {
            assert_eq!(error.connack_reason_code(), connack, "{error}");
            assert_eq!(error.disconnect_reason_code(), disconnect, "{error}");
            // Every code the errors map to is one a CONNACK or DISCONNECT may carry.
            assert!(connack.is_error() && disconnect.is_error(), "{error}");
        }
    }

    #[test]
    fn errors_read_as_sentences() {
        assert_eq!(
            Error::InvalidFlags {
                packet_type: PacketType::PubRel,
                flags: 0
            }
            .to_string(),
            "the flags 0b0000 are not valid for PUBREL"
        );
        assert_eq!(
            Error::UnsupportedProtocol {
                name: "MQIsdp".into(),
                level: 3
            }
            .to_string(),
            "protocol \"MQIsdp\" version 3 is not MQTT 5.0"
        );
        assert_eq!(
            Error::PropertyNotSentBy {
                sender: Sender::Server,
                packet_type: PacketType::Disconnect,
                property: PropertyId::SessionExpiryInterval
            }
            .to_string(),
            "a server does not send the Session Expiry Interval in DISCONNECT"
        );
    }
}
