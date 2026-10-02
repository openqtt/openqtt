//! PUBLISH (section 3.3).

use std::num::NonZeroU16;

use bytes::{BufMut, Bytes, BytesMut};

use crate::encode::{Encode, encode_methods};
use crate::primitives::{Reader, put_string, string_len};
use crate::property::{
    Properties, Value, measure_properties, once, put_properties, read_nonzero_u16,
    read_payload_format, read_properties, read_subscription_identifier, read_u32, unexpected,
};
use crate::{
    Error, PacketId, PacketType, PayloadFormat, PropertyContext, PropertyId, QoS, SubscriptionId,
};

/// The PUBLISH flags of the first byte (section 3.3.1, Figure 3-8).
const DUP: u8 = 0b1000;
const QOS_SHIFT: u8 = 1;
const RETAIN: u8 = 0b0001;

/// PUBLISH: an Application Message, from a client to the server or from the server to a
/// subscriber (section 3.3).
///
/// The Topic Name is checked here as a UTF-8 Encoded String and no further. Its syntax, no
/// wildcard characters in it ([MQTT-3.3.2-2]) or in a Response Topic ([MQTT-3.3.2-14]), is
/// openqtt-topic's to check, so the session can choose between a PUBACK with 0x90 (Topic Name
/// invalid) and a DISCONNECT.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Publish {
    /// DUP: this may be a redelivery of an earlier attempt (section 3.3.1.1). Never at QoS 0
    /// ([MQTT-3.3.1-2]).
    pub dup: bool,
    /// The QoS of this delivery (section 3.3.1.2).
    pub qos: QoS,
    /// RETAIN (section 3.3.1.3).
    pub retain: bool,
    /// The Topic Name (section 3.3.2.1). Empty only when a Topic Alias stands for it.
    pub topic: String,
    /// The Packet Identifier, present exactly when QoS is above 0 ([MQTT-2.2.1-2], section
    /// 3.3.2.2).
    pub packet_id: Option<PacketId>,
    /// The PUBLISH properties (section 3.3.2.3).
    pub properties: PublishProperties,
    /// The Application Message, which may be empty (section 3.3.3). A decoded payload shares
    /// the buffer it arrived in.
    pub payload: Bytes,
}

/// The properties of a PUBLISH (section 3.3.2.3).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PublishProperties {
    /// Payload Format Indicator (section 3.3.2.3.2), forwarded unaltered ([MQTT-3.3.2-4]).
    pub payload_format_indicator: Option<PayloadFormat>,
    /// Message Expiry Interval in seconds; absent means the message does not expire (section
    /// 3.3.2.3.3).
    pub message_expiry_interval: Option<u32>,
    /// Topic Alias (section 3.3.2.3.4). 0 is not a Topic Alias ([MQTT-3.3.2-8]); whether one
    /// is above the Topic Alias Maximum is the session's to check.
    pub topic_alias: Option<NonZeroU16>,
    /// Response Topic, which makes the message a request (section 3.3.2.3.5).
    pub response_topic: Option<String>,
    /// Correlation Data (section 3.3.2.3.6).
    pub correlation_data: Option<Bytes>,
    /// User Properties, name and value, in the order a server keeps ([MQTT-3.3.2-17],
    /// [MQTT-3.3.2-18]).
    pub user_properties: Vec<(String, String)>,
    /// Subscription Identifiers of the subscriptions a server delivers this for, in any order
    /// (section 3.3.2.3.8). A client sends none ([MQTT-3.3.4-6]).
    pub subscription_identifiers: Vec<SubscriptionId>,
    /// Content Type (section 3.3.2.3.9).
    pub content_type: Option<String>,
}

/// The QoS in a PUBLISH's flags, refusing both QoS bits set ([MQTT-3.3.1-4]) and DUP at QoS 0
/// ([MQTT-3.3.1-2]), both malformed: no packet can be read from such flags.
pub(crate) fn publish_qos(flags: u8) -> Result<QoS, Error> {
    QoS::from_u8((flags >> QOS_SHIFT) & 0b11)
        .filter(|&qos| qos != QoS::AtMostOnce || flags & DUP == 0)
        .ok_or(Error::InvalidFlags {
            packet_type: PacketType::Publish,
            flags,
        })
}

impl Publish {
    /// Decodes the variable header and payload of a PUBLISH whose first byte carried `flags`.
    pub(crate) fn decode(flags: u8, body: &Bytes) -> Result<Self, Error> {
        let qos = publish_qos(flags)?;
        let mut reader = Reader::new(body);
        let topic = reader.string("Topic Name")?;
        let packet_id = match qos {
            QoS::AtMostOnce => None,
            QoS::AtLeastOnce | QoS::ExactlyOnce => Some(reader.packet_id(PacketType::Publish)?),
        };
        let properties = PublishProperties::decode(&mut reader)?;
        let publish = Self {
            dup: flags & DUP != 0,
            qos,
            retain: flags & RETAIN != 0,
            topic,
            packet_id,
            properties,
            payload: reader.rest(),
        };
        publish.check_topic()?;
        Ok(publish)
    }

    /// An empty Topic Name needs a Topic Alias to stand for it (section 3.3.2.1).
    fn check_topic(&self) -> Result<(), Error> {
        if self.topic.is_empty() && self.properties.topic_alias.is_none() {
            return Err(Error::EmptyTopicName);
        }
        Ok(())
    }
}

impl Encode for Publish {
    fn first_byte(&self) -> u8 {
        PacketType::Publish.value() << 4
            | u8::from(self.dup) << 3
            | self.qos.value() << QOS_SHIFT
            | u8::from(self.retain)
    }

    fn remaining_length(&self) -> Result<usize, Error> {
        publish_qos(self.first_byte() & 0x0F)?;
        let packet_id = match (self.qos, self.packet_id) {
            (QoS::AtMostOnce, None) => 0,
            (QoS::AtLeastOnce | QoS::ExactlyOnce, Some(_)) => 2,
            _ => return Err(Error::PacketIdentifierMismatch { qos: self.qos }),
        };
        self.check_topic()?;
        Ok(string_len(&self.topic, "Topic Name")?
            + packet_id
            + measure_properties(&self.properties, PropertyContext::Publish)?
            + self.payload.len())
    }

    fn write_body(&self, dst: &mut BytesMut) {
        put_string(dst, &self.topic);
        if let Some(packet_id) = self.packet_id {
            dst.put_u16(packet_id.get());
        }
        put_properties(dst, &self.properties);
        dst.put_slice(&self.payload);
    }
}

encode_methods!(Publish);

impl PublishProperties {
    /// Reads the PUBLISH properties.
    fn decode(reader: &mut Reader<'_>) -> Result<Self, Error> {
        use PropertyId as P;
        let context = PropertyContext::Publish;
        let mut p = Self::default();
        read_properties(reader, context, |id, reader| match id {
            P::PayloadFormatIndicator => once(
                &mut p.payload_format_indicator,
                read_payload_format(reader, id)?,
                id,
            ),
            P::MessageExpiryInterval => {
                once(&mut p.message_expiry_interval, read_u32(reader, id)?, id)
            }
            P::TopicAlias => once(&mut p.topic_alias, read_nonzero_u16(reader, id)?, id),
            P::ResponseTopic => once(&mut p.response_topic, reader.string(id.name())?, id),
            P::CorrelationData => once(&mut p.correlation_data, reader.binary(id.name())?, id),
            P::UserProperty => {
                p.user_properties.push(reader.string_pair(id.name())?);
                Ok(())
            }
            // One for each subscription the message matched (section 3.3.2.3.8).
            P::SubscriptionIdentifier => {
                p.subscription_identifiers
                    .push(read_subscription_identifier(reader, id)?);
                Ok(())
            }
            P::ContentType => once(&mut p.content_type, reader.string(id.name())?, id),
            _ => Err(unexpected(id, context)),
        })?;
        Ok(p)
    }
}

impl Properties for PublishProperties {
    fn for_each(&self, mut f: impl FnMut(PropertyId, Value<'_>)) {
        use PropertyId as P;
        if let Some(value) = self.payload_format_indicator {
            f(P::PayloadFormatIndicator, Value::Byte(value.value()));
        }
        if let Some(value) = self.message_expiry_interval {
            f(P::MessageExpiryInterval, Value::FourByteInteger(value));
        }
        if let Some(value) = self.topic_alias {
            f(P::TopicAlias, Value::TwoByteInteger(value.get()));
        }
        if let Some(value) = &self.response_topic {
            f(P::ResponseTopic, Value::String(value));
        }
        if let Some(value) = &self.correlation_data {
            f(P::CorrelationData, Value::Binary(value));
        }
        for (name, value) in &self.user_properties {
            f(P::UserProperty, Value::Pair(name, value));
        }
        for value in &self.subscription_identifiers {
            f(
                P::SubscriptionIdentifier,
                Value::VariableByteInteger(value.get()),
            );
        }
        if let Some(value) = &self.content_type {
            f(P::ContentType, Value::String(value));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::test_util::{concat, encode_parts, prefixed, properties, sample};
    use crate::{ConnectReasonCode, DisconnectReasonCode, MAX_VARIABLE_BYTE_INTEGER};

    fn id(value: u16) -> PacketId {
        PacketId::new(value).unwrap()
    }

    /// A QoS 1 PUBLISH to "t" with packet identifier 1 and these properties.
    fn with_properties(props: &[&[u8]]) -> Bytes {
        concat(&[
            &prefixed(b"t"),
            &[0x00, 0x01],
            &properties(props),
            b"payload",
        ])
    }

    #[test]
    fn mqtt_3_3_2_figure_3_9_variable_header() {
        // Topic Name "a/b", Packet Identifier 10, no properties.
        let body = concat(&[&[0x00, 0x03, b'a', b'/', b'b', 0x00, 0x0A, 0x00], b"hello"]);
        let publish = Publish::decode(0b0010, &body).unwrap();
        assert_eq!(
            publish,
            Publish {
                qos: QoS::AtLeastOnce,
                topic: "a/b".into(),
                packet_id: Some(id(10)),
                payload: Bytes::from_static(b"hello"),
                ..Publish::default()
            }
        );
        assert_eq!(encode_parts(&publish), (0x32, body));
    }

    #[test]
    fn a_qos_0_publish_without_properties_or_payload_is_six_bytes() {
        let publish = Publish {
            topic: "a/b".into(),
            ..Publish::default()
        };
        let mut dst = BytesMut::new();
        publish.encode(&mut dst).unwrap();
        assert_eq!(dst[..], [0x30, 0x06, 0x00, 0x03, b'a', b'/', b'b', 0x00]);
        assert_eq!(
            Publish::decode(0, &Bytes::copy_from_slice(&dst[2..])),
            Ok(publish)
        );
    }

    #[test]
    fn mqtt_2_2_1_2_a_qos_0_publish_has_no_packet_identifier() {
        // At QoS 0 the two bytes after the Topic Name are already the properties.
        let body = concat(&[&prefixed(b"t"), &[0x00], b"\x00\x01"]);
        let publish = Publish::decode(0, &body).unwrap();
        assert_eq!(publish.packet_id, None);
        assert_eq!(publish.payload[..], [0x00, 0x01]);

        let with_id = Publish {
            topic: "t".into(),
            packet_id: Some(id(1)),
            ..Publish::default()
        };
        assert_eq!(
            with_id.encoded_len(),
            Err(Error::PacketIdentifierMismatch {
                qos: QoS::AtMostOnce
            })
        );
        let without_id = Publish {
            qos: QoS::ExactlyOnce,
            topic: "t".into(),
            ..Publish::default()
        };
        assert_eq!(
            without_id.encoded_len(),
            Err(Error::PacketIdentifierMismatch {
                qos: QoS::ExactlyOnce
            })
        );
    }

    #[test]
    fn mqtt_2_2_1_3_and_mqtt_2_2_1_4_a_packet_identifier_of_zero_is_a_protocol_error() {
        let body = concat(&[&prefixed(b"t"), &[0x00, 0x00, 0x00]]);
        let error = Publish::decode(0b0010, &body).unwrap_err();
        assert_eq!(
            error,
            Error::ZeroPacketIdentifier {
                packet_type: PacketType::Publish
            }
        );
        assert_eq!(
            error.disconnect_reason_code(),
            DisconnectReasonCode::ProtocolError
        );
    }

    #[test]
    fn mqtt_3_3_1_4_both_qos_bits_set_is_malformed() {
        for flags in [0b0110, 0b0111, 0b1110, 0b1111] {
            let error = Publish::decode(flags, &with_properties(&[])).unwrap_err();
            assert_eq!(
                error,
                Error::InvalidFlags {
                    packet_type: PacketType::Publish,
                    flags
                }
            );
            assert_eq!(
                error.disconnect_reason_code(),
                DisconnectReasonCode::MalformedPacket
            );
        }
    }

    #[test]
    fn mqtt_3_3_1_2_dup_is_zero_at_qos_0() {
        let body = concat(&[&prefixed(b"t"), &[0x00]]);
        assert_eq!(
            Publish::decode(0b1000, &body),
            Err(Error::InvalidFlags {
                packet_type: PacketType::Publish,
                flags: 0b1000
            })
        );
        let dup = Publish {
            dup: true,
            topic: "t".into(),
            ..Publish::default()
        };
        assert_eq!(
            dup.encoded_len(),
            Err(Error::InvalidFlags {
                packet_type: PacketType::Publish,
                flags: 0b1000
            })
        );
    }

    #[test]
    fn mqtt_3_3_1_1_and_section_3_3_1_3_dup_qos_and_retain_are_kept() {
        for (flags, dup, qos, retain) in [
            (0b0001, false, QoS::AtMostOnce, true),
            (0b1010, true, QoS::AtLeastOnce, false),
            (0b1101, true, QoS::ExactlyOnce, true),
        ] {
            let publish = Publish::decode(flags, &with_properties(&[])).unwrap();
            assert_eq!(
                (publish.dup, publish.qos, publish.retain),
                (dup, qos, retain)
            );
            assert_eq!(encode_parts(&publish).0, 0x30 | flags);
        }
    }

    #[test]
    fn mqtt_3_3_2_1_the_topic_name_is_a_utf8_string() {
        let cases = [
            (
                concat(&[&prefixed(&[0xFF]), &[0x00]]),
                Error::InvalidUtf8 {
                    field: "Topic Name",
                },
            ),
            (
                concat(&[&prefixed(b"a\0"), &[0x00]]),
                Error::NullCharacter {
                    field: "Topic Name",
                },
            ),
            (
                Bytes::from_static(&[0x00]),
                Error::Truncated {
                    field: "Topic Name",
                },
            ),
        ];
        for (body, error) in cases {
            assert_eq!(Publish::decode(0, &body), Err(error));
        }
    }

    #[test]
    fn mqtt_3_3_2_2_wildcards_are_left_to_the_topic_crate() {
        let body = concat(&[&prefixed(b"a/+/#"), &[0x00]]);
        assert_eq!(Publish::decode(0, &body).unwrap().topic, "a/+/#");
    }

    #[test]
    fn section_3_3_2_1_an_empty_topic_name_needs_a_topic_alias() {
        let body = concat(&[&prefixed(b""), &[0x00]]);
        let error = Publish::decode(0, &body).unwrap_err();
        assert_eq!(error, Error::EmptyTopicName);
        assert_eq!(
            error.disconnect_reason_code(),
            DisconnectReasonCode::ProtocolError
        );
        let aliased = concat(&[&prefixed(b""), &properties(&[&[0x23, 0x00, 0x07]])]);
        let publish = Publish::decode(0, &aliased).unwrap();
        assert_eq!(publish.properties.topic_alias, NonZeroU16::new(7));
        assert_eq!(encode_parts(&publish).1, aliased);
        assert_eq!(Publish::default().encoded_len(), Err(Error::EmptyTopicName));
    }

    #[test]
    fn mqtt_3_3_2_8_a_topic_alias_of_zero_is_invalid() {
        let error = Publish::decode(0b0010, &with_properties(&[&[0x23, 0x00, 0x00]])).unwrap_err();
        assert_eq!(error, Error::ZeroTopicAlias);
        assert_eq!(
            error.disconnect_reason_code(),
            DisconnectReasonCode::TopicAliasInvalid
        );
        assert_eq!(
            error.connack_reason_code(),
            ConnectReasonCode::ProtocolError
        );
    }

    #[test]
    fn section_3_3_2_3_8_subscription_identifiers() {
        // Several, in the order given, each from 1 to 268,435,455.
        let body = with_properties(&[
            &[0x0B, 0x01],
            &[0x0B, 0xFF, 0xFF, 0xFF, 0x7F],
            &[0x0B, 0x01],
        ]);
        let publish = Publish::decode(0b0010, &body).unwrap();
        assert_eq!(
            publish.properties.subscription_identifiers,
            [1, MAX_VARIABLE_BYTE_INTEGER, 1].map(|v| NonZeroU32::new(v).unwrap())
        );
        assert_eq!(encode_parts(&publish).1, body);

        assert_eq!(
            Publish::decode(0b0010, &with_properties(&[&[0x0B, 0x00]])),
            Err(Error::InvalidPropertyValue {
                property: PropertyId::SubscriptionIdentifier,
                value: 0
            })
        );
        let too_large = Publish {
            topic: "t".into(),
            properties: PublishProperties {
                subscription_identifiers: vec![
                    NonZeroU32::new(MAX_VARIABLE_BYTE_INTEGER + 1).unwrap(),
                ],
                ..PublishProperties::default()
            },
            ..Publish::default()
        };
        assert_eq!(
            too_large.encoded_len(),
            Err(Error::InvalidPropertyValue {
                property: PropertyId::SubscriptionIdentifier,
                value: MAX_VARIABLE_BYTE_INTEGER + 1
            })
        );
    }

    #[test]
    fn section_3_3_3_the_payload_may_be_empty_and_is_not_copied() {
        let body = concat(&[&prefixed(b"t"), &[0x00, 0x01, 0x00]]);
        let publish = Publish::decode(0b0010, &body).unwrap();
        assert!(publish.payload.is_empty());

        let body = with_properties(&[]);
        let publish = Publish::decode(0b0010, &body).unwrap();
        assert_eq!(publish.payload[..], b"payload"[..]);
        assert_eq!(publish.payload.as_ptr(), body[body.len() - 7..].as_ptr());
    }

    #[test]
    fn publish_properties_round_trip() {
        let publish = Publish {
            dup: true,
            qos: QoS::ExactlyOnce,
            retain: true,
            topic: "sensors/1/temperature".into(),
            packet_id: Some(id(0xFFFF)),
            properties: PublishProperties {
                payload_format_indicator: Some(PayloadFormat::Utf8),
                message_expiry_interval: Some(60),
                topic_alias: NonZeroU16::new(3),
                response_topic: Some("replies/1".into()),
                correlation_data: Some(Bytes::from_static(&[0, 1, 2])),
                user_properties: vec![
                    ("unit".into(), "C".into()),
                    ("unit".into(), "K".into()),
                    ("a".into(), "".into()),
                ],
                subscription_identifiers: vec![NonZeroU32::new(42).unwrap()],
                content_type: Some("text/plain; charset=utf-8".into()),
            },
            payload: Bytes::from_static(b"21.5"),
        };
        let (first, body) = encode_parts(&publish);
        assert_eq!(first, 0x3D);
        // [MQTT-3.3.2-17] [MQTT-3.3.2-18]: every User Property, in order, repeats kept. What a
        // server forwards unaltered, Response Topic, Correlation Data and Content Type
        // ([MQTT-3.3.2-15], [MQTT-3.3.2-16], [MQTT-3.3.2-20]), comes out as it went in.
        assert_eq!(Publish::decode(0b1101, &body), Ok(publish));
    }

    #[test]
    fn mqtt_3_3_2_13_and_mqtt_3_3_2_19_response_topic_and_content_type_are_utf8_strings() {
        for (id, field) in [(0x08, "Response Topic"), (0x03, "Content Type")] {
            let ill_formed = concat(&[&[id], &prefixed(&[0xC3, 0x28])]);
            assert_eq!(
                Publish::decode(0b0010, &with_properties(&[&ill_formed])),
                Err(Error::InvalidUtf8 { field })
            );
            let null = concat(&[&[id], &prefixed(b"a\0")]);
            assert_eq!(
                Publish::decode(0b0010, &with_properties(&[&null])),
                Err(Error::NullCharacter { field })
            );
        }
    }

    #[test]
    fn section_2_2_2_2_properties_table_2_4_does_not_allow_are_malformed() {
        for id in PropertyId::ALL {
            let result = Publish::decode(0b0010, &with_properties(&[&sample(id)]));
            if id.is_valid_in(PropertyContext::Publish) {
                assert!(result.is_ok(), "{id}: {result:?}");
            } else {
                assert_eq!(
                    result,
                    Err(Error::InvalidPropertyId {
                        context: PropertyContext::Publish,
                        id: id.value().into()
                    })
                );
            }
        }
    }

    #[test]
    fn section_3_3_2_3_properties_other_than_user_property_and_subscription_identifier_appear_once()
    {
        for id in PropertyId::ALL {
            if matches!(
                id,
                PropertyId::UserProperty | PropertyId::SubscriptionIdentifier
            ) || !id.is_valid_in(PropertyContext::Publish)
            {
                continue;
            }
            let property = sample(id);
            assert_eq!(
                Publish::decode(0b0010, &with_properties(&[&property, &property])),
                Err(Error::DuplicateProperty { property: id })
            );
        }
    }

    #[test]
    fn section_3_3_2_3_2_the_payload_format_indicator_is_0_or_1() {
        for (value, format) in [(0, PayloadFormat::Unspecified), (1, PayloadFormat::Utf8)] {
            let body = with_properties(&[&[0x01, value]]);
            let publish = Publish::decode(0b0010, &body).unwrap();
            // [MQTT-3.3.2-4]: kept as sent, 0 included, so it can be forwarded unaltered.
            assert_eq!(publish.properties.payload_format_indicator, Some(format));
            assert_eq!(encode_parts(&publish).1, body);
        }
        assert_eq!(
            Publish::decode(0b0010, &with_properties(&[&[0x01, 0x02]])),
            Err(Error::InvalidPropertyValue {
                property: PropertyId::PayloadFormatIndicator,
                value: 2
            })
        );
    }
}
