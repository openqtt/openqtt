//! Properties (section 2.2.2): the 27 identifiers, their data types, and the packets each may
//! appear in (Table 2-4), with the machinery that reads and writes a set of them.
//!
//! Each packet's properties are a struct with a field per property it allows, so a property
//! that does not belong cannot be built. Decoding reads identifiers and values off the wire
//! into one; encoding walks its fields as [`Value`]s. The value rules that sections 2.2.2 and 3
//! set, such as "a Receive Maximum of 0 is a Protocol Error", live in [`check_number`] and
//! apply both ways, so a packet that encodes always decodes.

use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};

use bytes::{BufMut, BytesMut};

use crate::primitives::{
    Reader, binary_len, put_binary, put_string, put_variable_byte_integer, string_len,
    variable_byte_integer_len,
};
use crate::{Error, MAX_PACKET_SIZE, MAX_VARIABLE_BYTE_INTEGER, PacketType, PayloadFormat, QoS};

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

/// One property value on its way out, borrowed from the packet that holds it.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Value<'a> {
    /// A Byte.
    Byte(u8),
    /// A Two Byte Integer.
    TwoByteInteger(u16),
    /// A Four Byte Integer.
    FourByteInteger(u32),
    /// A Variable Byte Integer.
    VariableByteInteger(u32),
    /// A UTF-8 Encoded String.
    String(&'a str),
    /// Binary Data.
    Binary(&'a [u8]),
    /// A UTF-8 String Pair.
    Pair(&'a str, &'a str),
}

impl Value<'_> {
    /// The value's encoded size, without its identifier.
    fn len(self) -> usize {
        match self {
            Self::Byte(_) => 1,
            Self::TwoByteInteger(_) => 2,
            Self::FourByteInteger(_) => 4,
            Self::VariableByteInteger(value) => variable_byte_integer_len(value),
            Self::String(text) => 2 + text.len(),
            Self::Binary(data) => 2 + data.len(),
            Self::Pair(name, value) => 4 + name.len() + value.len(),
        }
    }

    /// Checks the value against the rules a receiver applies to property `id`.
    fn check(self, id: PropertyId) -> Result<(), Error> {
        match self {
            Self::Byte(value) => check_number(id, value.into()),
            Self::TwoByteInteger(value) => check_number(id, value.into()),
            Self::FourByteInteger(value) | Self::VariableByteInteger(value) => {
                check_number(id, value)
            }
            Self::String(text) => string_len(text, id.name()).map(drop),
            Self::Binary(data) => binary_len(data, id.name()).map(drop),
            Self::Pair(name, value) => {
                string_len(name, id.name())?;
                string_len(value, id.name()).map(drop)
            }
        }
    }

    /// Writes the identifier and the value.
    fn write(self, id: PropertyId, dst: &mut BytesMut) {
        // Every identifier of Table 2-4 is below 128, so its Variable Byte Integer is the one
        // byte of its value.
        dst.put_u8(id.value());
        match self {
            Self::Byte(value) => dst.put_u8(value),
            Self::TwoByteInteger(value) => dst.put_u16(value),
            Self::FourByteInteger(value) => dst.put_u32(value),
            Self::VariableByteInteger(value) => put_variable_byte_integer(dst, value),
            Self::String(text) => put_string(dst, text),
            Self::Binary(data) => put_binary(dst, data),
            Self::Pair(name, value) => {
                put_string(dst, name);
                put_string(dst, value);
            }
        }
    }
}

/// A packet's set of properties, as the fields of a struct.
pub(crate) trait Properties {
    /// Calls `f` with every property present, in a fixed order. A repeated property comes in
    /// the order it was given, which keeps User Properties in order ([MQTT-3.3.2-18]).
    fn for_each(&self, f: impl FnMut(PropertyId, Value<'_>));
}

/// The Property Length of a set: the bytes of its properties, without the length itself.
pub(crate) fn property_length(properties: &impl Properties) -> usize {
    let mut len = 0;
    properties.for_each(|_, value| len += 1 + value.len());
    len
}

/// Checks a set of properties against the rules a receiver applies, and returns its encoded
/// size: the Property Length as a Variable Byte Integer, then the properties. A set with no
/// properties still takes one byte, a Property Length of zero ([MQTT-2.2.2-1]).
pub(crate) fn measure_properties(
    properties: &impl Properties,
    context: PropertyContext,
) -> Result<usize, Error> {
    let mut checked = Ok(());
    properties.for_each(|id, value| {
        debug_assert!(id.is_valid_in(context), "{id} in {context}");
        if checked.is_ok() {
            checked = value.check(id);
        }
    });
    checked?;
    let len = property_length(properties);
    match u32::try_from(len) {
        Ok(prefix) if prefix <= MAX_VARIABLE_BYTE_INTEGER => {
            Ok(variable_byte_integer_len(prefix) + len)
        }
        _ => Err(Error::PacketTooLarge {
            size: len,
            maximum: MAX_PACKET_SIZE,
        }),
    }
}

/// Writes a set of properties checked by [`measure_properties`].
pub(crate) fn put_properties(dst: &mut BytesMut, properties: &impl Properties) {
    let len = property_length(properties);
    put_variable_byte_integer(dst, u32::try_from(len).unwrap_or(MAX_VARIABLE_BYTE_INTEGER));
    properties.for_each(|id, value| value.write(id, dst));
}

/// The numeric rules sections 3.1 to 3.15 set for property values. Decoding applies them to
/// what arrives and encoding to what leaves, so the two cannot drift apart.
///
/// A Payload Format Indicator other than 0 or 1 is refused like the other Byte properties
/// that define only those two values: section 3.3.2.3.2 lists no others, and the definitions
/// of section 1.2 make a value that parses but is not allowed a Protocol Error.
pub(crate) fn check_number(id: PropertyId, value: u32) -> Result<(), Error> {
    use PropertyId as P;
    let allowed = match id {
        P::PayloadFormatIndicator
        | P::RequestProblemInformation
        | P::RequestResponseInformation
        | P::MaximumQos
        | P::RetainAvailable
        | P::WildcardSubscriptionAvailable
        | P::SubscriptionIdentifierAvailable
        | P::SharedSubscriptionAvailable => value <= 1,
        P::ReceiveMaximum | P::MaximumPacketSize => value != 0,
        P::SubscriptionIdentifier => value != 0 && value <= MAX_VARIABLE_BYTE_INTEGER,
        P::TopicAlias if value == 0 => return Err(Error::ZeroTopicAlias),
        _ => true,
    };
    if allowed {
        Ok(())
    } else {
        Err(Error::InvalidPropertyValue {
            property: id,
            value,
        })
    }
}

/// Reads a Property Length and the properties it covers, handing each identifier that Table
/// 2-4 allows in `context` to `each`, which reads the value with the readers below.
pub(crate) fn read_properties<'a>(
    reader: &mut Reader<'a>,
    context: PropertyContext,
    mut each: impl FnMut(PropertyId, &mut Reader<'a>) -> Result<(), Error>,
) -> Result<(), Error> {
    let len = reader.variable_byte_integer("Property Length")?;
    let len = usize::try_from(len).map_err(|_| Error::Truncated {
        field: "Properties",
    })?;
    let mut properties = reader.take(len, "Properties")?;
    while !properties.is_empty() {
        let id = properties.variable_byte_integer("Property Identifier")?;
        let property = u8::try_from(id)
            .ok()
            .and_then(PropertyId::from_u8)
            .filter(|property| property.is_valid_in(context))
            .ok_or(Error::InvalidPropertyId { context, id })?;
        each(property, &mut properties)?;
    }
    Ok(())
}

/// The error for an identifier that a context's reader does not handle. [`read_properties`]
/// only hands over identifiers the context allows, so this means a reader and Table 2-4
/// disagree; the tests check every reader against the table.
pub(crate) fn unexpected(id: PropertyId, context: PropertyContext) -> Error {
    Error::InvalidPropertyId {
        context,
        id: id.value().into(),
    }
}

/// Stores a property that may appear at most once; a second is a Protocol Error, as each
/// property's section says.
pub(crate) fn once<T>(slot: &mut Option<T>, value: T, id: PropertyId) -> Result<(), Error> {
    if slot.is_some() {
        return Err(Error::DuplicateProperty { property: id });
    }
    *slot = Some(value);
    Ok(())
}

/// A Byte property defined as 0 or 1.
pub(crate) fn read_bool(reader: &mut Reader<'_>, id: PropertyId) -> Result<bool, Error> {
    let value = reader.u8(id.name())?;
    check_number(id, value.into())?;
    Ok(value == 1)
}

/// The Maximum QoS of a CONNACK, 0 or 1 (section 3.2.2.3.4).
pub(crate) fn read_qos(reader: &mut Reader<'_>, id: PropertyId) -> Result<QoS, Error> {
    let value = reader.u8(id.name())?;
    check_number(id, value.into())?;
    QoS::from_u8(value).ok_or(Error::InvalidPropertyValue {
        property: id,
        value: value.into(),
    })
}

/// A Payload Format Indicator, 0 or 1.
pub(crate) fn read_payload_format(
    reader: &mut Reader<'_>,
    id: PropertyId,
) -> Result<PayloadFormat, Error> {
    let value = reader.u8(id.name())?;
    check_number(id, value.into())?;
    PayloadFormat::from_u8(value).ok_or(Error::InvalidPropertyValue {
        property: id,
        value: value.into(),
    })
}

/// A Two Byte Integer property.
pub(crate) fn read_u16(reader: &mut Reader<'_>, id: PropertyId) -> Result<u16, Error> {
    let value = reader.u16(id.name())?;
    check_number(id, value.into())?;
    Ok(value)
}

/// A Four Byte Integer property.
pub(crate) fn read_u32(reader: &mut Reader<'_>, id: PropertyId) -> Result<u32, Error> {
    let value = reader.u32(id.name())?;
    check_number(id, value)?;
    Ok(value)
}

/// A Two Byte Integer property that may not be 0.
pub(crate) fn read_nonzero_u16(
    reader: &mut Reader<'_>,
    id: PropertyId,
) -> Result<NonZeroU16, Error> {
    NonZeroU16::new(read_u16(reader, id)?).ok_or(Error::InvalidPropertyValue {
        property: id,
        value: 0,
    })
}

/// A Four Byte Integer property that may not be 0.
pub(crate) fn read_nonzero_u32(
    reader: &mut Reader<'_>,
    id: PropertyId,
) -> Result<NonZeroU32, Error> {
    NonZeroU32::new(read_u32(reader, id)?).ok_or(Error::InvalidPropertyValue {
        property: id,
        value: 0,
    })
}

/// A Subscription Identifier, a Variable Byte Integer from 1 to 268,435,455.
pub(crate) fn read_subscription_identifier(
    reader: &mut Reader<'_>,
    id: PropertyId,
) -> Result<NonZeroU32, Error> {
    let value = reader.variable_byte_integer(id.name())?;
    check_number(id, value)?;
    NonZeroU32::new(value).ok_or(Error::InvalidPropertyValue {
        property: id,
        value,
    })
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
