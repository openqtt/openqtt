//! The acknowledgements of a publication: PUBACK, PUBREC, PUBREL and PUBCOMP (sections 3.4
//! to 3.7), and the properties they share with SUBACK and UNSUBACK.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the decoder that calls the acknowledgement decoders arrives in a following commit"
    )
)]

use bytes::{BufMut, Bytes, BytesMut};

use crate::encode::{Encode, encode_methods};
use crate::primitives::Reader;
use crate::property::{
    Properties, Value, measure_properties, once, property_length, put_properties, read_properties,
    unexpected,
};
use crate::{
    Error, PacketId, PacketType, PropertyContext, PropertyId, PubAckReasonCode, PubCompReasonCode,
    PubRecReasonCode, PubRelReasonCode,
};

/// The properties of PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK and UNSUBACK: a Reason String and
/// User Properties, for diagnostics.
///
/// A sender drops them rather than exceed the receiver's Maximum Packet Size
/// ([MQTT-3.4.2-2], [MQTT-3.4.2-3] and the like); measure the packet with `encoded_len` to
/// decide.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AckProperties {
    /// Reason String, human readable and not for parsing (section 3.4.2.2.2 and the like).
    pub reason_string: Option<String>,
    /// User Properties, name and value, in order (section 3.4.2.2.3 and the like).
    pub user_properties: Vec<(String, String)>,
}

impl AckProperties {
    /// Whether there are no properties, which lets PUBACK, PUBREC, PUBREL and PUBCOMP leave
    /// out their Property Length.
    pub fn is_empty(&self) -> bool {
        property_length(self) == 0
    }

    /// Reads the properties of an acknowledgement in `context`.
    pub(crate) fn decode(reader: &mut Reader<'_>, context: PropertyContext) -> Result<Self, Error> {
        let mut p = Self::default();
        read_properties(reader, context, |id, reader| match id {
            PropertyId::ReasonString => once(&mut p.reason_string, reader.string(id.name())?, id),
            PropertyId::UserProperty => {
                p.user_properties.push(reader.string_pair(id.name())?);
                Ok(())
            }
            _ => Err(unexpected(id, context)),
        })?;
        Ok(p)
    }
}

impl Properties for AckProperties {
    fn for_each(&self, mut f: impl FnMut(PropertyId, Value<'_>)) {
        if let Some(value) = &self.reason_string {
            f(PropertyId::ReasonString, Value::String(value));
        }
        for (name, value) in &self.user_properties {
            f(PropertyId::UserProperty, Value::Pair(name, value));
        }
    }
}

/// Decodes the variable header shared by PUBACK, PUBREC, PUBREL and PUBCOMP: the Packet
/// Identifier, then a Reason Code and the properties, which may be left off from the end.
/// Without a Reason Code, a Remaining Length of 2, the code is 0x00 Success; without a
/// Property Length, a Remaining Length below 4, there are no properties (sections 3.4.2.1
/// and 3.4.2.2.1, and the same in 3.5 to 3.7).
fn decode_ack<T>(
    body: &Bytes,
    packet_type: PacketType,
    context: PropertyContext,
    reason_code: fn(u8) -> Option<T>,
) -> Result<(PacketId, T, AckProperties), Error> {
    let mut reader = Reader::new(body);
    let packet_id = reader.packet_id(packet_type)?;
    let code = if reader.is_empty() {
        0x00
    } else {
        reader.u8("Reason Code")?
    };
    let reason_code = reason_code(code).ok_or(Error::InvalidReasonCode { packet_type, code })?;
    let properties = if reader.is_empty() {
        AckProperties::default()
    } else {
        AckProperties::decode(&mut reader, context)?
    };
    reader.finish(packet_type)?;
    Ok((packet_id, reason_code, properties))
}

/// The Remaining Length of an acknowledgement, in its shortest form: 2 for Success without
/// properties, 3 for another code without properties, and the full form otherwise.
fn ack_remaining_length(
    code: u8,
    properties: &AckProperties,
    context: PropertyContext,
) -> Result<usize, Error> {
    let size = measure_properties(properties, context)?;
    Ok(match (code, properties.is_empty()) {
        (0x00, true) => 2,
        (_, true) => 3,
        (_, false) => 3 + size,
    })
}

/// Writes the variable header of an acknowledgement in the form
/// [`ack_remaining_length`] measured.
fn write_ack(dst: &mut BytesMut, packet_id: PacketId, code: u8, properties: &AckProperties) {
    dst.put_u16(packet_id.get());
    let empty = properties.is_empty();
    if code != 0x00 || !empty {
        dst.put_u8(code);
    }
    if !empty {
        put_properties(dst, properties);
    }
}

/// Defines one of the four publication acknowledgements.
macro_rules! ack_packet {
    (
        $(#[$meta:meta])*
        $name:ident, $packet_type:ident, $code:ident, flags = $flags:literal
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name {
            /// The Packet Identifier of the packet being acknowledged ([MQTT-2.2.1-5]).
            pub packet_id: PacketId,
            /// The Reason Code. Success, with no properties, is left off the wire.
            pub reason_code: $code,
            /// The Reason String and User Properties.
            pub properties: AckProperties,
        }

        impl $name {
            /// The acknowledgement of `packet_id` with reason code Success and no properties.
            pub fn new(packet_id: PacketId) -> Self {
                Self {
                    packet_id,
                    reason_code: $code::default(),
                    properties: AckProperties::default(),
                }
            }

            /// Decodes the variable header.
            pub(crate) fn decode(body: &Bytes) -> Result<Self, Error> {
                let (packet_id, reason_code, properties) = decode_ack(
                    body,
                    PacketType::$packet_type,
                    PropertyContext::$packet_type,
                    $code::from_u8,
                )?;
                Ok(Self {
                    packet_id,
                    reason_code,
                    properties,
                })
            }
        }

        impl Encode for $name {
            fn first_byte(&self) -> u8 {
                PacketType::$packet_type.value() << 4 | $flags
            }

            fn remaining_length(&self) -> Result<usize, Error> {
                ack_remaining_length(
                    self.reason_code.value(),
                    &self.properties,
                    PropertyContext::$packet_type,
                )
            }

            fn write_body(&self, dst: &mut BytesMut) {
                write_ack(dst, self.packet_id, self.reason_code.value(), &self.properties);
            }
        }

        encode_methods!($name);
    };
}

ack_packet! {
    /// PUBACK: the answer to a QoS 1 PUBLISH (section 3.4).
    PubAck, PubAck, PubAckReasonCode, flags = 0b0000
}

ack_packet! {
    /// PUBREC: the answer to a QoS 2 PUBLISH, the first of its three acknowledgements
    /// (section 3.5).
    PubRec, PubRec, PubRecReasonCode, flags = 0b0000
}

ack_packet! {
    /// PUBREL: the answer to a PUBREC, releasing the QoS 2 message (section 3.6). Its fixed
    /// header flags are 0010 ([MQTT-3.6.1-1]).
    PubRel, PubRel, PubRelReasonCode, flags = 0b0010
}

ack_packet! {
    /// PUBCOMP: the answer to a PUBREL, completing the QoS 2 exchange (section 3.7).
    PubComp, PubComp, PubCompReasonCode, flags = 0b0000
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{concat, encode_parts, prefixed, properties, sample};

    fn id(value: u16) -> PacketId {
        PacketId::new(value).unwrap()
    }

    #[test]
    fn mqtt_3_4_2_1_a_remaining_length_of_2_means_success() {
        assert_eq!(
            PubAck::decode(&Bytes::from_static(&[0x12, 0x34])),
            Ok(PubAck::new(id(0x1234)))
        );
        let mut dst = BytesMut::new();
        PubAck::new(id(0x1234)).encode(&mut dst).unwrap();
        assert_eq!(dst[..], [0x40, 0x02, 0x12, 0x34]);
    }

    #[test]
    fn section_3_4_2_2_1_a_remaining_length_below_4_means_no_properties() {
        let ack = PubAck::decode(&Bytes::from_static(&[0x00, 0x01, 0x10])).unwrap();
        assert_eq!(ack.reason_code, PubAckReasonCode::NoMatchingSubscribers);
        assert!(ack.properties.is_empty());
        assert_eq!(
            encode_parts(&ack),
            (0x40, Bytes::from_static(&[0x00, 0x01, 0x10]))
        );
    }

    #[test]
    fn the_longer_forms_decode_to_the_same_packet() {
        // Success written out, with and without an empty Property Length, is the short form.
        for body in [&[0x00, 0x07, 0x00][..], &[0x00, 0x07, 0x00, 0x00]] {
            let ack = PubComp::decode(&Bytes::copy_from_slice(body)).unwrap();
            assert_eq!(ack, PubComp::new(id(7)));
            assert_eq!(encode_parts(&ack).1[..], [0x00, 0x07]);
        }
        let ack = PubRec::decode(&Bytes::from_static(&[0x00, 0x07, 0x87, 0x00])).unwrap();
        assert_eq!(ack.reason_code, PubRecReasonCode::NotAuthorized);
        assert_eq!(encode_parts(&ack).1[..], [0x00, 0x07, 0x87]);
    }

    #[test]
    fn acknowledgement_properties_round_trip() {
        let ack = PubRel {
            packet_id: id(9),
            reason_code: PubRelReasonCode::PacketIdentifierNotFound,
            properties: AckProperties {
                reason_string: Some("no such message".into()),
                user_properties: vec![
                    ("trace".into(), "abc".into()),
                    ("trace".into(), "def".into()),
                ],
            },
        };
        let (first, body) = encode_parts(&ack);
        assert_eq!(first, 0x62);
        assert_eq!(PubRel::decode(&body), Ok(ack));
        // Properties with Success still need the Reason Code written out.
        let ack = PubAck {
            properties: AckProperties {
                reason_string: Some("ok".into()),
                ..AckProperties::default()
            },
            ..PubAck::new(id(1))
        };
        assert_eq!(
            encode_parts(&ack).1,
            concat(&[
                &[0x00, 0x01, 0x00],
                &properties(&[&concat(&[&[0x1F], &prefixed(b"ok")])])
            ])
        );
    }

    /// Every byte as the Reason Code of each acknowledgement: accepted exactly when its table
    /// lists it.
    #[test]
    fn mqtt_3_4_2_1_mqtt_3_5_2_1_mqtt_3_6_2_1_and_mqtt_3_7_2_1_reason_codes_come_from_their_tables()
    {
        for code in 0..=u8::MAX {
            let body = Bytes::copy_from_slice(&[0x00, 0x01, code]);
            let invalid =
                |packet_type| Err::<(), _>(Error::InvalidReasonCode { packet_type, code });
            match PubAckReasonCode::from_u8(code) {
                Some(reason) => assert_eq!(PubAck::decode(&body).unwrap().reason_code, reason),
                None => assert_eq!(PubAck::decode(&body).map(drop), invalid(PacketType::PubAck)),
            }
            match PubRecReasonCode::from_u8(code) {
                Some(reason) => assert_eq!(PubRec::decode(&body).unwrap().reason_code, reason),
                None => assert_eq!(PubRec::decode(&body).map(drop), invalid(PacketType::PubRec)),
            }
            match PubRelReasonCode::from_u8(code) {
                Some(reason) => assert_eq!(PubRel::decode(&body).unwrap().reason_code, reason),
                None => assert_eq!(PubRel::decode(&body).map(drop), invalid(PacketType::PubRel)),
            }
            match PubCompReasonCode::from_u8(code) {
                Some(reason) => assert_eq!(PubComp::decode(&body).unwrap().reason_code, reason),
                None => {
                    assert_eq!(
                        PubComp::decode(&body).map(drop),
                        invalid(PacketType::PubComp)
                    );
                }
            }
        }
    }

    #[test]
    fn mqtt_2_2_1_5_a_packet_identifier_of_zero_is_a_protocol_error() {
        let body = Bytes::from_static(&[0x00, 0x00]);
        assert_eq!(
            PubAck::decode(&body),
            Err(Error::ZeroPacketIdentifier {
                packet_type: PacketType::PubAck
            })
        );
        assert_eq!(
            PubComp::decode(&body),
            Err(Error::ZeroPacketIdentifier {
                packet_type: PacketType::PubComp
            })
        );
        assert_eq!(
            PubRec::decode(&Bytes::from_static(&[0x00])),
            Err(Error::Truncated {
                field: "Packet Identifier"
            })
        );
    }

    #[test]
    fn section_2_2_2_2_only_reason_string_and_user_property_are_allowed() {
        for (packet_type, context) in [
            (PacketType::PubAck, PropertyContext::PubAck),
            (PacketType::PubRec, PropertyContext::PubRec),
            (PacketType::PubRel, PropertyContext::PubRel),
            (PacketType::PubComp, PropertyContext::PubComp),
        ] {
            for property in PropertyId::ALL {
                let body = concat(&[&[0x00, 0x01, 0x00], &properties(&[&sample(property)])]);
                let result = match packet_type {
                    PacketType::PubAck => PubAck::decode(&body).map(drop),
                    PacketType::PubRec => PubRec::decode(&body).map(drop),
                    PacketType::PubRel => PubRel::decode(&body).map(drop),
                    _ => PubComp::decode(&body).map(drop),
                };
                if property.is_valid_in(context) {
                    assert_eq!(result, Ok(()), "{property} in {packet_type}");
                } else {
                    assert_eq!(
                        result,
                        Err(Error::InvalidPropertyId {
                            context,
                            id: property.value().into()
                        })
                    );
                }
            }
        }
        let reason = sample(PropertyId::ReasonString);
        let body = concat(&[&[0x00, 0x01, 0x00], &properties(&[&reason, &reason])]);
        assert_eq!(
            PubAck::decode(&body),
            Err(Error::DuplicateProperty {
                property: PropertyId::ReasonString
            })
        );
    }

    #[test]
    fn nothing_follows_the_properties() {
        let body = concat(&[&[0x00, 0x01, 0x00], &properties(&[]), &[0xAA]]);
        assert_eq!(
            PubAck::decode(&body),
            Err(Error::TrailingBytes {
                packet_type: PacketType::PubAck,
                count: 1
            })
        );
    }
}
