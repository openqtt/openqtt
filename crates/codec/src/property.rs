//! Properties (section 2.2.2): the 27 identifiers, their data types, and the packets each may
//! appear in (Table 2-4).

use std::fmt;

use crate::PacketType;

/// The data type of a property value (section 1.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    /// A single byte.
    Byte,
    /// A Two Byte Integer (section 1.5.2).
    TwoByteInteger,
    /// A Four Byte Integer (section 1.5.3).
    FourByteInteger,
    /// A Variable Byte Integer (section 1.5.5).
    VariableByteInteger,
    /// A UTF-8 Encoded String (section 1.5.4).
    Utf8EncodedString,
    /// Binary Data (section 1.5.6).
    BinaryData,
    /// A UTF-8 String Pair (section 1.5.7).
    Utf8StringPair,
}

/// Where a set of properties appears: the variable header of a packet, or the Will Properties
/// in the payload of a CONNECT. PINGREQ and PINGRESP carry no properties.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum PropertyContext {
    /// CONNECT properties (section 3.1.2.11).
    Connect,
    /// The Will Properties of a CONNECT (section 3.1.3.2).
    Will,
    /// CONNACK properties (section 3.2.2.3).
    ConnAck,
    /// PUBLISH properties (section 3.3.2.3).
    Publish,
    /// PUBACK properties (section 3.4.2.2).
    PubAck,
    /// PUBREC properties (section 3.5.2.2).
    PubRec,
    /// PUBREL properties (section 3.6.2.2).
    PubRel,
    /// PUBCOMP properties (section 3.7.2.2).
    PubComp,
    /// SUBSCRIBE properties (section 3.8.2.1).
    Subscribe,
    /// SUBACK properties (section 3.9.2.1).
    SubAck,
    /// UNSUBSCRIBE properties (section 3.10.2.1).
    Unsubscribe,
    /// UNSUBACK properties (section 3.11.2.1).
    UnsubAck,
    /// DISCONNECT properties (section 3.14.2.2).
    Disconnect,
    /// AUTH properties (section 3.15.2.2).
    Auth,
}

impl PropertyContext {
    /// Every context, the packets in order of type with the Will Properties after CONNECT.
    pub const ALL: [Self; 14] = [
        Self::Connect,
        Self::Will,
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
        Self::Disconnect,
        Self::Auth,
    ];

    /// The properties in the variable header of a packet type, or `None` for PINGREQ and
    /// PINGRESP.
    pub const fn of(packet_type: PacketType) -> Option<Self> {
        Some(match packet_type {
            PacketType::Connect => Self::Connect,
            PacketType::ConnAck => Self::ConnAck,
            PacketType::Publish => Self::Publish,
            PacketType::PubAck => Self::PubAck,
            PacketType::PubRec => Self::PubRec,
            PacketType::PubRel => Self::PubRel,
            PacketType::PubComp => Self::PubComp,
            PacketType::Subscribe => Self::Subscribe,
            PacketType::SubAck => Self::SubAck,
            PacketType::Unsubscribe => Self::Unsubscribe,
            PacketType::UnsubAck => Self::UnsubAck,
            PacketType::Disconnect => Self::Disconnect,
            PacketType::Auth => Self::Auth,
            PacketType::PingReq | PacketType::PingResp => return None,
        })
    }

    /// The name Table 2-4 uses, such as `"CONNACK"` or `"Will Properties"`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Connect => "CONNECT",
            Self::Will => "Will Properties",
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
            Self::Disconnect => "DISCONNECT",
            Self::Auth => "AUTH",
        }
    }

    /// This context's bit in a set of contexts.
    const fn bit(self) -> u16 {
        1 << (self as u8)
    }
}

impl fmt::Display for PropertyContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A property identifier (section 2.2.2.2, Table 2-4). The identifier is a Variable Byte
/// Integer on the wire, though every one this version defines fits in a single byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum PropertyId {
    /// 0x01 Payload Format Indicator, a Byte.
    PayloadFormatIndicator = 0x01,
    /// 0x02 Message Expiry Interval, a Four Byte Integer.
    MessageExpiryInterval = 0x02,
    /// 0x03 Content Type, a UTF-8 Encoded String.
    ContentType = 0x03,
    /// 0x08 Response Topic, a UTF-8 Encoded String.
    ResponseTopic = 0x08,
    /// 0x09 Correlation Data, Binary Data.
    CorrelationData = 0x09,
    /// 0x0B Subscription Identifier, a Variable Byte Integer.
    SubscriptionIdentifier = 0x0B,
    /// 0x11 Session Expiry Interval, a Four Byte Integer.
    SessionExpiryInterval = 0x11,
    /// 0x12 Assigned Client Identifier, a UTF-8 Encoded String.
    AssignedClientIdentifier = 0x12,
    /// 0x13 Server Keep Alive, a Two Byte Integer.
    ServerKeepAlive = 0x13,
    /// 0x15 Authentication Method, a UTF-8 Encoded String.
    AuthenticationMethod = 0x15,
    /// 0x16 Authentication Data, Binary Data.
    AuthenticationData = 0x16,
    /// 0x17 Request Problem Information, a Byte.
    RequestProblemInformation = 0x17,
    /// 0x18 Will Delay Interval, a Four Byte Integer.
    WillDelayInterval = 0x18,
    /// 0x19 Request Response Information, a Byte.
    RequestResponseInformation = 0x19,
    /// 0x1A Response Information, a UTF-8 Encoded String.
    ResponseInformation = 0x1A,
    /// 0x1C Server Reference, a UTF-8 Encoded String.
    ServerReference = 0x1C,
    /// 0x1F Reason String, a UTF-8 Encoded String.
    ReasonString = 0x1F,
    /// 0x21 Receive Maximum, a Two Byte Integer.
    ReceiveMaximum = 0x21,
    /// 0x22 Topic Alias Maximum, a Two Byte Integer.
    TopicAliasMaximum = 0x22,
    /// 0x23 Topic Alias, a Two Byte Integer.
    TopicAlias = 0x23,
    /// 0x24 Maximum QoS, a Byte.
    MaximumQos = 0x24,
    /// 0x25 Retain Available, a Byte.
    RetainAvailable = 0x25,
    /// 0x26 User Property, a UTF-8 String Pair.
    UserProperty = 0x26,
    /// 0x27 Maximum Packet Size, a Four Byte Integer.
    MaximumPacketSize = 0x27,
    /// 0x28 Wildcard Subscription Available, a Byte.
    WildcardSubscriptionAvailable = 0x28,
    /// 0x29 Subscription Identifier Available, a Byte.
    SubscriptionIdentifierAvailable = 0x29,
    /// 0x2A Shared Subscription Available, a Byte.
    SharedSubscriptionAvailable = 0x2A,
}

impl PropertyId {
    /// Every identifier, in order of value.
    pub const ALL: [Self; 27] = [
        Self::PayloadFormatIndicator,
        Self::MessageExpiryInterval,
        Self::ContentType,
        Self::ResponseTopic,
        Self::CorrelationData,
        Self::SubscriptionIdentifier,
        Self::SessionExpiryInterval,
        Self::AssignedClientIdentifier,
        Self::ServerKeepAlive,
        Self::AuthenticationMethod,
        Self::AuthenticationData,
        Self::RequestProblemInformation,
        Self::WillDelayInterval,
        Self::RequestResponseInformation,
        Self::ResponseInformation,
        Self::ServerReference,
        Self::ReasonString,
        Self::ReceiveMaximum,
        Self::TopicAliasMaximum,
        Self::TopicAlias,
        Self::MaximumQos,
        Self::RetainAvailable,
        Self::UserProperty,
        Self::MaximumPacketSize,
        Self::WildcardSubscriptionAvailable,
        Self::SubscriptionIdentifierAvailable,
        Self::SharedSubscriptionAvailable,
    ];

    /// The identifier with this value, or `None` when Table 2-4 defines none.
    pub const fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            0x01 => Self::PayloadFormatIndicator,
            0x02 => Self::MessageExpiryInterval,
            0x03 => Self::ContentType,
            0x08 => Self::ResponseTopic,
            0x09 => Self::CorrelationData,
            0x0B => Self::SubscriptionIdentifier,
            0x11 => Self::SessionExpiryInterval,
            0x12 => Self::AssignedClientIdentifier,
            0x13 => Self::ServerKeepAlive,
            0x15 => Self::AuthenticationMethod,
            0x16 => Self::AuthenticationData,
            0x17 => Self::RequestProblemInformation,
            0x18 => Self::WillDelayInterval,
            0x19 => Self::RequestResponseInformation,
            0x1A => Self::ResponseInformation,
            0x1C => Self::ServerReference,
            0x1F => Self::ReasonString,
            0x21 => Self::ReceiveMaximum,
            0x22 => Self::TopicAliasMaximum,
            0x23 => Self::TopicAlias,
            0x24 => Self::MaximumQos,
            0x25 => Self::RetainAvailable,
            0x26 => Self::UserProperty,
            0x27 => Self::MaximumPacketSize,
            0x28 => Self::WildcardSubscriptionAvailable,
            0x29 => Self::SubscriptionIdentifierAvailable,
            0x2A => Self::SharedSubscriptionAvailable,
            _ => return None,
        })
    }

    /// The identifier's value.
    pub const fn value(self) -> u8 {
        self as u8
    }

    /// The name Table 2-4 gives it, such as `"Receive Maximum"`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::PayloadFormatIndicator => "Payload Format Indicator",
            Self::MessageExpiryInterval => "Message Expiry Interval",
            Self::ContentType => "Content Type",
            Self::ResponseTopic => "Response Topic",
            Self::CorrelationData => "Correlation Data",
            Self::SubscriptionIdentifier => "Subscription Identifier",
            Self::SessionExpiryInterval => "Session Expiry Interval",
            Self::AssignedClientIdentifier => "Assigned Client Identifier",
            Self::ServerKeepAlive => "Server Keep Alive",
            Self::AuthenticationMethod => "Authentication Method",
            Self::AuthenticationData => "Authentication Data",
            Self::RequestProblemInformation => "Request Problem Information",
            Self::WillDelayInterval => "Will Delay Interval",
            Self::RequestResponseInformation => "Request Response Information",
            Self::ResponseInformation => "Response Information",
            Self::ServerReference => "Server Reference",
            Self::ReasonString => "Reason String",
            Self::ReceiveMaximum => "Receive Maximum",
            Self::TopicAliasMaximum => "Topic Alias Maximum",
            Self::TopicAlias => "Topic Alias",
            Self::MaximumQos => "Maximum QoS",
            Self::RetainAvailable => "Retain Available",
            Self::UserProperty => "User Property",
            Self::MaximumPacketSize => "Maximum Packet Size",
            Self::WildcardSubscriptionAvailable => "Wildcard Subscription Available",
            Self::SubscriptionIdentifierAvailable => "Subscription Identifier Available",
            Self::SharedSubscriptionAvailable => "Shared Subscription Available",
        }
    }

    /// The data type of the value that follows the identifier.
    pub const fn data_type(self) -> DataType {
        match self {
            Self::PayloadFormatIndicator
            | Self::RequestProblemInformation
            | Self::RequestResponseInformation
            | Self::MaximumQos
            | Self::RetainAvailable
            | Self::WildcardSubscriptionAvailable
            | Self::SubscriptionIdentifierAvailable
            | Self::SharedSubscriptionAvailable => DataType::Byte,
            Self::ServerKeepAlive
            | Self::ReceiveMaximum
            | Self::TopicAliasMaximum
            | Self::TopicAlias => DataType::TwoByteInteger,
            Self::MessageExpiryInterval
            | Self::SessionExpiryInterval
            | Self::WillDelayInterval
            | Self::MaximumPacketSize => DataType::FourByteInteger,
            Self::SubscriptionIdentifier => DataType::VariableByteInteger,
            Self::ContentType
            | Self::ResponseTopic
            | Self::AssignedClientIdentifier
            | Self::AuthenticationMethod
            | Self::ResponseInformation
            | Self::ServerReference
            | Self::ReasonString => DataType::Utf8EncodedString,
            Self::CorrelationData | Self::AuthenticationData => DataType::BinaryData,
            Self::UserProperty => DataType::Utf8StringPair,
        }
    }

    /// Whether Table 2-4 allows this property in `context`. A packet carrying a property its
    /// context does not allow is a Malformed Packet (section 2.2.2.2).
    pub const fn is_valid_in(self, context: PropertyContext) -> bool {
        use PropertyContext as C;
        let contexts = match self {
            Self::PayloadFormatIndicator
            | Self::MessageExpiryInterval
            | Self::ContentType
            | Self::ResponseTopic
            | Self::CorrelationData => C::Publish.bit() | C::Will.bit(),
            Self::SubscriptionIdentifier => C::Publish.bit() | C::Subscribe.bit(),
            Self::SessionExpiryInterval => {
                C::Connect.bit() | C::ConnAck.bit() | C::Disconnect.bit()
            }
            Self::AssignedClientIdentifier
            | Self::ServerKeepAlive
            | Self::ResponseInformation
            | Self::MaximumQos
            | Self::RetainAvailable
            | Self::WildcardSubscriptionAvailable
            | Self::SubscriptionIdentifierAvailable
            | Self::SharedSubscriptionAvailable => C::ConnAck.bit(),
            Self::AuthenticationMethod | Self::AuthenticationData => {
                C::Connect.bit() | C::ConnAck.bit() | C::Auth.bit()
            }
            Self::RequestProblemInformation | Self::RequestResponseInformation => C::Connect.bit(),
            Self::WillDelayInterval => C::Will.bit(),
            Self::ServerReference => C::ConnAck.bit() | C::Disconnect.bit(),
            Self::ReasonString => {
                C::ConnAck.bit()
                    | C::PubAck.bit()
                    | C::PubRec.bit()
                    | C::PubRel.bit()
                    | C::PubComp.bit()
                    | C::SubAck.bit()
                    | C::UnsubAck.bit()
                    | C::Disconnect.bit()
                    | C::Auth.bit()
            }
            Self::ReceiveMaximum | Self::TopicAliasMaximum | Self::MaximumPacketSize => {
                C::Connect.bit() | C::ConnAck.bit()
            }
            Self::TopicAlias => C::Publish.bit(),
            Self::UserProperty => u16::MAX,
        };
        contexts & context.bit() != 0
    }
}

impl fmt::Display for PropertyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_2_4_has_27_identifiers_that_round_trip() {
        assert_eq!(PropertyId::ALL.len(), 27);
        let defined: Vec<u8> = (0..=u8::MAX)
            .filter(|&v| PropertyId::from_u8(v).is_some())
            .collect();
        assert_eq!(
            defined,
            [
                0x01, 0x02, 0x03, 0x08, 0x09, 0x0B, 0x11, 0x12, 0x13, 0x15, 0x16, 0x17, 0x18, 0x19,
                0x1A, 0x1C, 0x1F, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A
            ]
        );
        for id in PropertyId::ALL {
            assert_eq!(PropertyId::from_u8(id.value()), Some(id));
        }
    }

    /// Table 2-4 as printed: identifier, data type and the packets that may carry it.
    #[test]
    fn table_2_4_data_types_and_packets() {
        use DataType as D;
        use PropertyContext as C;
        use PropertyId as P;
        let acks = [
            C::ConnAck,
            C::PubAck,
            C::PubRec,
            C::PubRel,
            C::PubComp,
            C::SubAck,
            C::UnsubAck,
            C::Disconnect,
            C::Auth,
        ];
        let rows: [(P, D, &[C]); 27] = [
            (P::PayloadFormatIndicator, D::Byte, &[C::Publish, C::Will]),
            (
                P::MessageExpiryInterval,
                D::FourByteInteger,
                &[C::Publish, C::Will],
            ),
            (P::ContentType, D::Utf8EncodedString, &[C::Publish, C::Will]),
            (
                P::ResponseTopic,
                D::Utf8EncodedString,
                &[C::Publish, C::Will],
            ),
            (P::CorrelationData, D::BinaryData, &[C::Publish, C::Will]),
            (
                P::SubscriptionIdentifier,
                D::VariableByteInteger,
                &[C::Publish, C::Subscribe],
            ),
            (
                P::SessionExpiryInterval,
                D::FourByteInteger,
                &[C::Connect, C::ConnAck, C::Disconnect],
            ),
            (
                P::AssignedClientIdentifier,
                D::Utf8EncodedString,
                &[C::ConnAck],
            ),
            (P::ServerKeepAlive, D::TwoByteInteger, &[C::ConnAck]),
            (
                P::AuthenticationMethod,
                D::Utf8EncodedString,
                &[C::Connect, C::ConnAck, C::Auth],
            ),
            (
                P::AuthenticationData,
                D::BinaryData,
                &[C::Connect, C::ConnAck, C::Auth],
            ),
            (P::RequestProblemInformation, D::Byte, &[C::Connect]),
            (P::WillDelayInterval, D::FourByteInteger, &[C::Will]),
            (P::RequestResponseInformation, D::Byte, &[C::Connect]),
            (P::ResponseInformation, D::Utf8EncodedString, &[C::ConnAck]),
            (
                P::ServerReference,
                D::Utf8EncodedString,
                &[C::ConnAck, C::Disconnect],
            ),
            (P::ReasonString, D::Utf8EncodedString, &acks),
            (
                P::ReceiveMaximum,
                D::TwoByteInteger,
                &[C::Connect, C::ConnAck],
            ),
            (
                P::TopicAliasMaximum,
                D::TwoByteInteger,
                &[C::Connect, C::ConnAck],
            ),
            (P::TopicAlias, D::TwoByteInteger, &[C::Publish]),
            (P::MaximumQos, D::Byte, &[C::ConnAck]),
            (P::RetainAvailable, D::Byte, &[C::ConnAck]),
            (P::UserProperty, D::Utf8StringPair, &PropertyContext::ALL),
            (
                P::MaximumPacketSize,
                D::FourByteInteger,
                &[C::Connect, C::ConnAck],
            ),
            (P::WildcardSubscriptionAvailable, D::Byte, &[C::ConnAck]),
            (P::SubscriptionIdentifierAvailable, D::Byte, &[C::ConnAck]),
            (P::SharedSubscriptionAvailable, D::Byte, &[C::ConnAck]),
        ];
        for (id, data_type, contexts) in rows {
            assert_eq!(id.data_type(), data_type, "{id}");
            for context in PropertyContext::ALL {
                assert_eq!(
                    id.is_valid_in(context),
                    contexts.contains(&context),
                    "{id} in {context}"
                );
            }
        }
    }

    #[test]
    fn every_packet_but_pingreq_and_pingresp_has_properties() {
        for packet_type in PacketType::ALL {
            let context = PropertyContext::of(packet_type);
            match packet_type {
                PacketType::PingReq | PacketType::PingResp => assert_eq!(context, None),
                _ => assert_eq!(context.map(PropertyContext::name), Some(packet_type.name())),
            }
        }
    }
}
