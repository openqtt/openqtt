//! SUBSCRIBE, SUBACK, UNSUBSCRIBE and UNSUBACK (sections 3.8 to 3.11).
//!
//! Topic Filters are checked here as UTF-8 Encoded Strings and no further. Their syntax
//! (section 4.7), shared subscriptions (section 4.8) and the rule that No Local may not be set
//! on one ([MQTT-3.8.3-4]) belong to openqtt-topic and the session, which can answer an
//! invalid filter with its own reason code in the SUBACK rather than end the connection.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the decoder that calls the subscription decoders arrives in a following commit"
    )
)]

use bytes::{BufMut, Bytes, BytesMut};

use crate::ack::AckProperties;
use crate::encode::{Encode, encode_methods};
use crate::primitives::{Reader, put_string, string_len};
use crate::property::{
    Properties, Value, measure_properties, once, put_properties, read_properties,
    read_subscription_identifier, unexpected,
};
use crate::{
    Error, PacketId, PacketType, PropertyContext, PropertyId, QoS, SubAckReasonCode,
    SubscriptionId, UnsubAckReasonCode,
};

/// The fields of the Subscription Options byte (section 3.8.3.1, Figure 3-20).
const NO_LOCAL: u8 = 0b0000_0100;
const RETAIN_AS_PUBLISHED: u8 = 0b0000_1000;
const RETAIN_HANDLING_SHIFT: u8 = 4;
const RESERVED_OPTIONS: u8 = 0b1100_0000;

/// SUBSCRIBE: a client's request for one or more subscriptions (section 3.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    /// The Packet Identifier, which the SUBACK repeats ([MQTT-3.8.4-2]).
    pub packet_id: PacketId,
    /// The SUBSCRIBE properties (section 3.8.2.1).
    pub properties: SubscribeProperties,
    /// The Topic Filters and their options, at least one ([MQTT-3.8.3-2]).
    pub subscriptions: Vec<Subscription>,
}

/// The properties of a SUBSCRIBE (section 3.8.2.1).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubscribeProperties {
    /// Subscription Identifier for every subscription this packet makes, at most one (section
    /// 3.8.2.1.2).
    pub subscription_identifier: Option<SubscriptionId>,
    /// User Properties, name and value, in order (section 3.8.2.1.3).
    pub user_properties: Vec<(String, String)>,
}

/// One Topic Filter of a SUBSCRIBE with its Subscription Options (section 3.8.3).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Subscription {
    /// The Topic Filter, a UTF-8 Encoded String ([MQTT-3.8.3-1]).
    pub filter: String,
    /// The Subscription Options.
    pub options: SubscriptionOptions,
}

/// The Subscription Options byte (section 3.8.3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SubscriptionOptions {
    /// Maximum QoS: the highest QoS the server may deliver this subscription at.
    pub maximum_qos: QoS,
    /// No Local: never deliver the client's own publications to it ([MQTT-3.8.3-3]).
    pub no_local: bool,
    /// Retain As Published: keep the RETAIN flag a message was published with.
    pub retain_as_published: bool,
    /// Retain Handling: when to send retained messages for this subscription.
    pub retain_handling: RetainHandling,
}

/// The Retain Handling option of a subscription (section 3.8.3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum RetainHandling {
    /// 0: send retained messages when the subscription is made ([MQTT-3.3.1-9]).
    #[default]
    SendAtSubscribe = 0,
    /// 1: send them only if the subscription did not already exist ([MQTT-3.3.1-10]).
    SendIfNew = 1,
    /// 2: do not send them ([MQTT-3.3.1-11]).
    DoNotSend = 2,
}

impl RetainHandling {
    /// The Retain Handling with this value, or `None` for 3 and above.
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::SendAtSubscribe),
            1 => Some(Self::SendIfNew),
            2 => Some(Self::DoNotSend),
            _ => None,
        }
    }

    /// The value, 0 to 2.
    pub const fn value(self) -> u8 {
        self as u8
    }
}

impl SubscriptionOptions {
    /// The byte on the wire.
    pub const fn to_byte(self) -> u8 {
        let mut byte =
            self.maximum_qos.value() | self.retain_handling.value() << RETAIN_HANDLING_SHIFT;
        if self.no_local {
            byte |= NO_LOCAL;
        }
        if self.retain_as_published {
            byte |= RETAIN_AS_PUBLISHED;
        }
        byte
    }

    /// Reads the byte on the wire.
    ///
    /// # Errors
    ///
    /// [`Error::ReservedSubscriptionOptions`] when bit 6 or 7 is set, a Malformed Packet
    /// ([MQTT-3.8.3-5]), and [`Error::InvalidSubscriptionOptions`] for a Maximum QoS or a
    /// Retain Handling of 3, a Protocol Error (section 3.8.3.1).
    pub const fn from_byte(options: u8) -> Result<Self, Error> {
        if options & RESERVED_OPTIONS != 0 {
            return Err(Error::ReservedSubscriptionOptions { options });
        }
        let (Some(maximum_qos), Some(retain_handling)) = (
            QoS::from_u8(options & 0b11),
            RetainHandling::from_u8(options >> RETAIN_HANDLING_SHIFT & 0b11),
        ) else {
            return Err(Error::InvalidSubscriptionOptions { options });
        };
        Ok(Self {
            maximum_qos,
            no_local: options & NO_LOCAL != 0,
            retain_as_published: options & RETAIN_AS_PUBLISHED != 0,
            retain_handling,
        })
    }
}

impl Subscribe {
    /// Decodes the variable header and payload of a SUBSCRIBE.
    pub(crate) fn decode(body: &Bytes) -> Result<Self, Error> {
        let mut reader = Reader::new(body);
        let packet_id = reader.packet_id(PacketType::Subscribe)?;
        let properties = SubscribeProperties::decode(&mut reader)?;
        let mut subscriptions = Vec::new();
        while !reader.is_empty() {
            let filter = reader.string("Topic Filter")?;
            let options = SubscriptionOptions::from_byte(reader.u8("Subscription Options")?)?;
            subscriptions.push(Subscription { filter, options });
        }
        if subscriptions.is_empty() {
            return Err(Error::EmptyPayload {
                packet_type: PacketType::Subscribe,
            });
        }
        Ok(Self {
            packet_id,
            properties,
            subscriptions,
        })
    }
}

impl Encode for Subscribe {
    fn first_byte(&self) -> u8 {
        PacketType::Subscribe.value() << 4 | 0b0010
    }

    fn remaining_length(&self) -> Result<usize, Error> {
        if self.subscriptions.is_empty() {
            return Err(Error::EmptyPayload {
                packet_type: PacketType::Subscribe,
            });
        }
        let mut len = 2 + measure_properties(&self.properties, PropertyContext::Subscribe)?;
        for subscription in &self.subscriptions {
            len += string_len(&subscription.filter, "Topic Filter")? + 1;
        }
        Ok(len)
    }

    fn write_body(&self, dst: &mut BytesMut) {
        dst.put_u16(self.packet_id.get());
        put_properties(dst, &self.properties);
        for subscription in &self.subscriptions {
            put_string(dst, &subscription.filter);
            dst.put_u8(subscription.options.to_byte());
        }
    }
}

encode_methods!(Subscribe);

impl SubscribeProperties {
    /// Reads the SUBSCRIBE properties.
    fn decode(reader: &mut Reader<'_>) -> Result<Self, Error> {
        let context = PropertyContext::Subscribe;
        let mut p = Self::default();
        read_properties(reader, context, |id, reader| match id {
            PropertyId::SubscriptionIdentifier => once(
                &mut p.subscription_identifier,
                read_subscription_identifier(reader, id)?,
                id,
            ),
            PropertyId::UserProperty => {
                p.user_properties.push(reader.string_pair(id.name())?);
                Ok(())
            }
            _ => Err(unexpected(id, context)),
        })?;
        Ok(p)
    }
}

impl Properties for SubscribeProperties {
    fn for_each(&self, mut f: impl FnMut(PropertyId, Value<'_>)) {
        if let Some(value) = self.subscription_identifier {
            f(
                PropertyId::SubscriptionIdentifier,
                Value::VariableByteInteger(value.get()),
            );
        }
        for (name, value) in &self.user_properties {
            f(PropertyId::UserProperty, Value::Pair(name, value));
        }
    }
}

/// SUBACK: the server's answer to a SUBSCRIBE, one reason code per Topic Filter in the same
/// order ([MQTT-3.8.4-6], [MQTT-3.9.3-1]) (section 3.9).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    /// The Packet Identifier of the SUBSCRIBE ([MQTT-3.8.4-2]).
    pub packet_id: PacketId,
    /// The Reason String and User Properties (section 3.9.2.1).
    pub properties: AckProperties,
    /// A Subscribe Reason Code for each Topic Filter, at least one (section 3.9.3).
    pub reason_codes: Vec<SubAckReasonCode>,
}

impl SubAck {
    /// Decodes the variable header and payload of a SUBACK.
    pub(crate) fn decode(body: &Bytes) -> Result<Self, Error> {
        let (packet_id, properties, reason_codes) = decode_codes(
            body,
            PacketType::SubAck,
            PropertyContext::SubAck,
            SubAckReasonCode::from_u8,
        )?;
        Ok(Self {
            packet_id,
            properties,
            reason_codes,
        })
    }
}

impl Encode for SubAck {
    fn first_byte(&self) -> u8 {
        PacketType::SubAck.value() << 4
    }

    fn remaining_length(&self) -> Result<usize, Error> {
        codes_remaining_length(
            PacketType::SubAck,
            &self.properties,
            PropertyContext::SubAck,
            self.reason_codes.len(),
        )
    }

    fn write_body(&self, dst: &mut BytesMut) {
        dst.put_u16(self.packet_id.get());
        put_properties(dst, &self.properties);
        for code in &self.reason_codes {
            dst.put_u8(code.value());
        }
    }
}

encode_methods!(SubAck);

/// UNSUBSCRIBE: a client's request to remove subscriptions (section 3.10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    /// The Packet Identifier, which the UNSUBACK repeats ([MQTT-3.10.4-5]).
    pub packet_id: PacketId,
    /// The UNSUBSCRIBE properties (section 3.10.2.1).
    pub properties: UnsubscribeProperties,
    /// The Topic Filters to remove, at least one ([MQTT-3.10.3-2]), each a UTF-8 Encoded
    /// String ([MQTT-3.10.3-1]).
    pub filters: Vec<String>,
}

/// The properties of an UNSUBSCRIBE (section 3.10.2.1): only User Properties.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UnsubscribeProperties {
    /// User Properties, name and value, in order (section 3.10.2.1.2).
    pub user_properties: Vec<(String, String)>,
}

impl Unsubscribe {
    /// Decodes the variable header and payload of an UNSUBSCRIBE.
    pub(crate) fn decode(body: &Bytes) -> Result<Self, Error> {
        let mut reader = Reader::new(body);
        let packet_id = reader.packet_id(PacketType::Unsubscribe)?;
        let properties = UnsubscribeProperties::decode(&mut reader)?;
        let mut filters = Vec::new();
        while !reader.is_empty() {
            filters.push(reader.string("Topic Filter")?);
        }
        if filters.is_empty() {
            return Err(Error::EmptyPayload {
                packet_type: PacketType::Unsubscribe,
            });
        }
        Ok(Self {
            packet_id,
            properties,
            filters,
        })
    }
}

impl Encode for Unsubscribe {
    fn first_byte(&self) -> u8 {
        PacketType::Unsubscribe.value() << 4 | 0b0010
    }

    fn remaining_length(&self) -> Result<usize, Error> {
        if self.filters.is_empty() {
            return Err(Error::EmptyPayload {
                packet_type: PacketType::Unsubscribe,
            });
        }
        let mut len = 2 + measure_properties(&self.properties, PropertyContext::Unsubscribe)?;
        for filter in &self.filters {
            len += string_len(filter, "Topic Filter")?;
        }
        Ok(len)
    }

    fn write_body(&self, dst: &mut BytesMut) {
        dst.put_u16(self.packet_id.get());
        put_properties(dst, &self.properties);
        for filter in &self.filters {
            put_string(dst, filter);
        }
    }
}

encode_methods!(Unsubscribe);

impl UnsubscribeProperties {
    /// Reads the UNSUBSCRIBE properties.
    fn decode(reader: &mut Reader<'_>) -> Result<Self, Error> {
        let context = PropertyContext::Unsubscribe;
        let mut p = Self::default();
        read_properties(reader, context, |id, reader| match id {
            PropertyId::UserProperty => {
                p.user_properties.push(reader.string_pair(id.name())?);
                Ok(())
            }
            _ => Err(unexpected(id, context)),
        })?;
        Ok(p)
    }
}

impl Properties for UnsubscribeProperties {
    fn for_each(&self, mut f: impl FnMut(PropertyId, Value<'_>)) {
        for (name, value) in &self.user_properties {
            f(PropertyId::UserProperty, Value::Pair(name, value));
        }
    }
}

/// UNSUBACK: the server's answer to an UNSUBSCRIBE, one reason code per Topic Filter in the
/// same order ([MQTT-3.11.3-1]) (section 3.11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAck {
    /// The Packet Identifier of the UNSUBSCRIBE ([MQTT-3.10.4-5]).
    pub packet_id: PacketId,
    /// The Reason String and User Properties (section 3.11.2.1).
    pub properties: AckProperties,
    /// An Unsubscribe Reason Code for each Topic Filter, at least one (section 3.11.3).
    pub reason_codes: Vec<UnsubAckReasonCode>,
}

impl UnsubAck {
    /// Decodes the variable header and payload of an UNSUBACK.
    pub(crate) fn decode(body: &Bytes) -> Result<Self, Error> {
        let (packet_id, properties, reason_codes) = decode_codes(
            body,
            PacketType::UnsubAck,
            PropertyContext::UnsubAck,
            UnsubAckReasonCode::from_u8,
        )?;
        Ok(Self {
            packet_id,
            properties,
            reason_codes,
        })
    }
}

impl Encode for UnsubAck {
    fn first_byte(&self) -> u8 {
        PacketType::UnsubAck.value() << 4
    }

    fn remaining_length(&self) -> Result<usize, Error> {
        codes_remaining_length(
            PacketType::UnsubAck,
            &self.properties,
            PropertyContext::UnsubAck,
            self.reason_codes.len(),
        )
    }

    fn write_body(&self, dst: &mut BytesMut) {
        dst.put_u16(self.packet_id.get());
        put_properties(dst, &self.properties);
        for code in &self.reason_codes {
            dst.put_u8(code.value());
        }
    }
}

encode_methods!(UnsubAck);

/// Decodes what SUBACK and UNSUBACK share: the Packet Identifier, the properties, then a
/// payload of one or more reason codes (sections 3.9 and 3.11). Unlike PUBACK, neither may
/// leave out its Property Length.
fn decode_codes<T>(
    body: &Bytes,
    packet_type: PacketType,
    context: PropertyContext,
    reason_code: fn(u8) -> Option<T>,
) -> Result<(PacketId, AckProperties, Vec<T>), Error> {
    let mut reader = Reader::new(body);
    let packet_id = reader.packet_id(packet_type)?;
    let properties = AckProperties::decode(&mut reader, context)?;
    let mut codes = Vec::with_capacity(reader.remaining());
    while !reader.is_empty() {
        let code = reader.u8("Reason Code")?;
        codes.push(reason_code(code).ok_or(Error::InvalidReasonCode { packet_type, code })?);
    }
    if codes.is_empty() {
        return Err(Error::EmptyPayload { packet_type });
    }
    Ok((packet_id, properties, codes))
}

/// The Remaining Length of a SUBACK or UNSUBACK with `codes` reason codes.
fn codes_remaining_length(
    packet_type: PacketType,
    properties: &AckProperties,
    context: PropertyContext,
    codes: usize,
) -> Result<usize, Error> {
    if codes == 0 {
        return Err(Error::EmptyPayload { packet_type });
    }
    Ok(2 + measure_properties(properties, context)? + codes)
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::DisconnectReasonCode;
    use crate::test_util::{concat, encode_parts, prefixed, properties, sample};

    fn id(value: u16) -> PacketId {
        PacketId::new(value).unwrap()
    }

    #[test]
    fn mqtt_3_8_figures_3_19_and_3_21_subscribe() {
        // Packet Identifier 10 and no properties (Figure 3-19), then "a/b" at QoS 1 and
        // "c/d" at QoS 2 (Figure 3-21).
        let body = Bytes::from_static(&[
            0x00, 0x0A, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x01, 0x00, 0x03, b'c', b'/', b'd',
            0x02,
        ]);
        let subscribe = Subscribe::decode(&body).unwrap();
        let at = |filter: &str, maximum_qos| Subscription {
            filter: filter.into(),
            options: SubscriptionOptions {
                maximum_qos,
                ..SubscriptionOptions::default()
            },
        };
        assert_eq!(
            subscribe,
            Subscribe {
                packet_id: id(10),
                properties: SubscribeProperties::default(),
                subscriptions: vec![at("a/b", QoS::AtLeastOnce), at("c/d", QoS::ExactlyOnce)],
            }
        );
        assert_eq!(encode_parts(&subscribe), (0x82, body));
    }

    #[test]
    fn mqtt_3_8_3_2_a_subscribe_has_at_least_one_topic_filter() {
        let error = Subscribe::decode(&Bytes::from_static(&[0x00, 0x01, 0x00])).unwrap_err();
        assert_eq!(
            error,
            Error::EmptyPayload {
                packet_type: PacketType::Subscribe
            }
        );
        assert_eq!(
            error.disconnect_reason_code(),
            DisconnectReasonCode::ProtocolError
        );
        let empty = Subscribe {
            packet_id: id(1),
            properties: SubscribeProperties::default(),
            subscriptions: Vec::new(),
        };
        assert_eq!(empty.encoded_len(), Err(error));
    }

    #[test]
    fn mqtt_3_8_3_1_topic_filters_are_utf8_strings() {
        let body = concat(&[&[0x00, 0x01, 0x00], &prefixed(&[0xED, 0xA0, 0x80]), &[0x00]]);
        assert_eq!(
            Subscribe::decode(&body),
            Err(Error::InvalidUtf8 {
                field: "Topic Filter"
            })
        );
        let body = concat(&[&[0x00, 0x01, 0x00], &prefixed(b"a")]);
        assert_eq!(
            Subscribe::decode(&body),
            Err(Error::Truncated {
                field: "Subscription Options"
            })
        );
    }

    #[test]
    fn mqtt_3_8_3_5_reserved_option_bits_are_malformed() {
        for options in [0x40, 0x80, 0xC0, 0xC3] {
            let body = concat(&[&[0x00, 0x01, 0x00], &prefixed(b"a"), &[options]]);
            let error = Subscribe::decode(&body).unwrap_err();
            assert_eq!(error, Error::ReservedSubscriptionOptions { options });
            assert_eq!(
                error.disconnect_reason_code(),
                DisconnectReasonCode::MalformedPacket
            );
        }
    }

    #[test]
    fn section_3_8_3_1_a_maximum_qos_or_retain_handling_of_3_is_a_protocol_error() {
        for options in [0x03, 0x30, 0x33, 0x3F] {
            let body = concat(&[&[0x00, 0x01, 0x00], &prefixed(b"a"), &[options]]);
            let error = Subscribe::decode(&body).unwrap_err();
            assert_eq!(error, Error::InvalidSubscriptionOptions { options });
            assert_eq!(
                error.disconnect_reason_code(),
                DisconnectReasonCode::ProtocolError
            );
        }
    }

    #[test]
    fn section_3_8_3_1_every_valid_options_byte_round_trips() {
        let valid: Vec<u8> = (0..=u8::MAX)
            .filter(|&byte| SubscriptionOptions::from_byte(byte).is_ok())
            .collect();
        // Three QoS, No Local, Retain As Published, three Retain Handling.
        assert_eq!(valid.len(), 3 * 2 * 2 * 3);
        for byte in valid {
            let options = SubscriptionOptions::from_byte(byte).unwrap();
            assert_eq!(options.to_byte(), byte);
        }
        let options = SubscriptionOptions::from_byte(0b0010_1110).unwrap();
        assert_eq!(
            options,
            SubscriptionOptions {
                maximum_qos: QoS::ExactlyOnce,
                no_local: true,
                retain_as_published: true,
                retain_handling: RetainHandling::DoNotSend,
            }
        );
    }

    #[test]
    fn mqtt_3_8_3_4_no_local_on_a_shared_subscription_is_left_to_the_session() {
        let body = concat(&[&[0x00, 0x01, 0x00], &prefixed(b"$share/g/a"), &[0x04]]);
        let subscribe = Subscribe::decode(&body).unwrap();
        assert!(subscribe.subscriptions[0].options.no_local);
    }

    #[test]
    fn section_3_8_2_1_2_the_subscription_identifier() {
        let body = concat(&[
            &[0x00, 0x01],
            &properties(&[&[0x0B, 0x80, 0x01]]),
            &prefixed(b"a"),
            &[0x00],
        ]);
        let subscribe = Subscribe::decode(&body).unwrap();
        assert_eq!(
            subscribe.properties.subscription_identifier,
            NonZeroU32::new(128)
        );
        assert_eq!(encode_parts(&subscribe).1, body);

        let zero = concat(&[
            &[0x00, 0x01],
            &properties(&[&[0x0B, 0x00]]),
            &prefixed(b"a"),
            &[0x00],
        ]);
        assert_eq!(
            Subscribe::decode(&zero),
            Err(Error::InvalidPropertyValue {
                property: PropertyId::SubscriptionIdentifier,
                value: 0
            })
        );
        let twice = concat(&[
            &[0x00, 0x01],
            &properties(&[&[0x0B, 0x01], &[0x0B, 0x02]]),
            &prefixed(b"a"),
            &[0x00],
        ]);
        assert_eq!(
            Subscribe::decode(&twice),
            Err(Error::DuplicateProperty {
                property: PropertyId::SubscriptionIdentifier
            })
        );
    }

    #[test]
    fn mqtt_2_2_1_3_subscribe_and_unsubscribe_need_a_nonzero_packet_identifier() {
        let body = concat(&[&[0x00, 0x00, 0x00], &prefixed(b"a"), &[0x00]]);
        assert_eq!(
            Subscribe::decode(&body),
            Err(Error::ZeroPacketIdentifier {
                packet_type: PacketType::Subscribe
            })
        );
        let body = concat(&[&[0x00, 0x00, 0x00], &prefixed(b"a")]);
        assert_eq!(
            Unsubscribe::decode(&body),
            Err(Error::ZeroPacketIdentifier {
                packet_type: PacketType::Unsubscribe
            })
        );
    }

    /// Every property in `context`, decoded by `decode` from a packet that carries it.
    fn check_table_2_4(context: PropertyContext, decode: impl Fn(&[u8]) -> Result<(), Error>) {
        for id in PropertyId::ALL {
            let result = decode(&properties(&[&sample(id)]));
            if id.is_valid_in(context) {
                assert_eq!(result, Ok(()), "{id} in {context}");
            } else {
                assert_eq!(
                    result,
                    Err(Error::InvalidPropertyId {
                        context,
                        id: id.value().into()
                    })
                );
            }
        }
    }

    #[test]
    fn section_2_2_2_2_properties_table_2_4_does_not_allow_are_malformed() {
        check_table_2_4(PropertyContext::Subscribe, |props| {
            Subscribe::decode(&concat(&[&[0x00, 0x01], props, &prefixed(b"a"), &[0x00]])).map(drop)
        });
        check_table_2_4(PropertyContext::SubAck, |props| {
            SubAck::decode(&concat(&[&[0x00, 0x01], props, &[0x00]])).map(drop)
        });
        check_table_2_4(PropertyContext::Unsubscribe, |props| {
            Unsubscribe::decode(&concat(&[&[0x00, 0x01], props, &prefixed(b"a")])).map(drop)
        });
        check_table_2_4(PropertyContext::UnsubAck, |props| {
            UnsubAck::decode(&concat(&[&[0x00, 0x01], props, &[0x00]])).map(drop)
        });
    }

    #[test]
    fn suback_round_trips_with_a_code_per_filter() {
        let suback = SubAck {
            packet_id: id(10),
            properties: AckProperties {
                reason_string: Some("partly".into()),
                user_properties: vec![("k".into(), "v".into())],
            },
            reason_codes: vec![
                SubAckReasonCode::GrantedQos1,
                SubAckReasonCode::NotAuthorized,
                SubAckReasonCode::GrantedQos0,
            ],
        };
        let (first, body) = encode_parts(&suback);
        assert_eq!(first, 0x90);
        // [MQTT-3.9.3-1]: the codes stay in the order of the Topic Filters.
        assert_eq!(SubAck::decode(&body), Ok(suback));
    }

    #[test]
    fn mqtt_3_9_3_2_suback_codes_come_from_table_3_8() {
        for code in 0..=u8::MAX {
            let body = Bytes::copy_from_slice(&[0x00, 0x01, 0x00, 0x00, code]);
            match SubAckReasonCode::from_u8(code) {
                Some(reason) => assert_eq!(
                    SubAck::decode(&body).unwrap().reason_codes,
                    [SubAckReasonCode::GrantedQos0, reason]
                ),
                None => assert_eq!(
                    SubAck::decode(&body),
                    Err(Error::InvalidReasonCode {
                        packet_type: PacketType::SubAck,
                        code
                    })
                ),
            }
        }
    }

    #[test]
    fn suback_and_unsuback_need_a_reason_code() {
        // Also what an MQTT 3.1.1 SUBACK granting QoS 0 looks like: it has no Property Length.
        let body = Bytes::from_static(&[0x00, 0x01, 0x00]);
        assert_eq!(
            SubAck::decode(&body),
            Err(Error::EmptyPayload {
                packet_type: PacketType::SubAck
            })
        );
        assert_eq!(
            UnsubAck::decode(&body),
            Err(Error::EmptyPayload {
                packet_type: PacketType::UnsubAck
            })
        );
    }

    #[test]
    fn mqtt_3_10_figure_3_30_unsubscribe() {
        let body = Bytes::from_static(&[
            0x00, 0x0B, 0x00, 0x00, 0x03, b'a', b'/', b'b', 0x00, 0x03, b'c', b'/', b'd',
        ]);
        let unsubscribe = Unsubscribe::decode(&body).unwrap();
        assert_eq!(
            unsubscribe,
            Unsubscribe {
                packet_id: id(11),
                properties: UnsubscribeProperties::default(),
                filters: vec!["a/b".into(), "c/d".into()],
            }
        );
        assert_eq!(encode_parts(&unsubscribe), (0xA2, body));
    }

    #[test]
    fn mqtt_3_10_3_1_and_mqtt_3_10_3_2_unsubscribe_needs_utf8_topic_filters() {
        assert_eq!(
            Unsubscribe::decode(&Bytes::from_static(&[0x00, 0x01, 0x00])),
            Err(Error::EmptyPayload {
                packet_type: PacketType::Unsubscribe
            })
        );
        let body = concat(&[&[0x00, 0x01, 0x00], &prefixed(b"a\0")]);
        assert_eq!(
            Unsubscribe::decode(&body),
            Err(Error::NullCharacter {
                field: "Topic Filter"
            })
        );
    }

    #[test]
    fn mqtt_3_11_3_2_unsuback_codes_come_from_table_3_9() {
        for code in 0..=u8::MAX {
            let body = Bytes::copy_from_slice(&[0x00, 0x01, 0x00, code]);
            match UnsubAckReasonCode::from_u8(code) {
                Some(reason) => {
                    assert_eq!(UnsubAck::decode(&body).unwrap().reason_codes, [reason]);
                }
                None => assert_eq!(
                    UnsubAck::decode(&body),
                    Err(Error::InvalidReasonCode {
                        packet_type: PacketType::UnsubAck,
                        code
                    })
                ),
            }
        }
        let unsuback = UnsubAck {
            packet_id: id(3),
            properties: AckProperties::default(),
            reason_codes: vec![
                UnsubAckReasonCode::NoSubscriptionExisted,
                UnsubAckReasonCode::Success,
            ],
        };
        assert_eq!(
            encode_parts(&unsuback),
            (0xB0, Bytes::from_static(&[0x00, 0x03, 0x00, 0x11, 0x00]))
        );
    }
}
