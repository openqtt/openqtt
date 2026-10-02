//! The codec's one error type.

use crate::{ConnectReasonCode, DisconnectReasonCode, PacketType, PropertyContext, PropertyId};

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
    /// A CONNECT whose Protocol Name is not `"MQTT"` or whose Protocol Version is not 5
    /// ([MQTT-3.1.2-1], [MQTT-3.1.2-2]). The rest of the packet is not read, since an older
    /// protocol lays it out differently. CONNACK 0x84, Unsupported Protocol Version; see
    /// [`ProtocolRefusal`](crate::ProtocolRefusal) for the bytes to answer with.
    #[error("protocol {name:?} version {level} is not MQTT 5.0")]
    UnsupportedProtocol {
        /// The Protocol Name, such as `"MQIsdp"` for MQTT 3.1.
        name: String,
        /// The Protocol Version, such as 4 for MQTT 3.1.1.
        level: u8,
    },

    // Encoding only.
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
            Self::MalformedVariableByteInteger { .. }
            | Self::Truncated { .. }
            | Self::TrailingBytes { .. }
            | Self::InvalidUtf8 { .. }
            | Self::NullCharacter { .. }
            | Self::InvalidPropertyId { .. }
            | Self::InvalidConnectFlags { .. }
            | Self::InvalidConnAckFlags { .. } => Class::Malformed,
            Self::DuplicateProperty { .. }
            | Self::InvalidPropertyValue { .. }
            | Self::InvalidReasonCode { .. }
            | Self::MissingAuthenticationMethod { .. }
            | Self::SessionPresentWithError => Class::Protocol,
            Self::ZeroTopicAlias => Class::TopicAlias,
            Self::PacketTooLarge { .. } => Class::TooLarge,
            Self::UnsupportedProtocol { .. } => Class::Version,
            Self::TooLong { .. } => Class::Local,
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
