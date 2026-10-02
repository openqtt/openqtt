//! CONNACK (section 3.2).

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the decoder that calls ConnAck::decode arrives in a following commit"
    )
)]

use std::num::{NonZeroU16, NonZeroU32};

use bytes::{BufMut, Bytes, BytesMut};

use crate::encode::{Encode, encode_methods};
use crate::primitives::Reader;
use crate::property::{
    Properties, Value, measure_properties, once, put_properties, read_bool, read_nonzero_u16,
    read_nonzero_u32, read_properties, read_qos, read_u16, read_u32, unexpected,
};
use crate::{ConnectReasonCode, Error, PacketType, PropertyContext, PropertyId, QoS};

/// The one flag of the Connect Acknowledge Flags; bits 7 to 1 are reserved
/// ([MQTT-3.2.2-1]).
const SESSION_PRESENT: u8 = 0x01;

/// CONNACK: the server's answer to a CONNECT (section 3.2).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnAck {
    /// Session Present: the server resumed the session the Client Identifier had (section
    /// 3.2.2.1.1). Only with [`ConnectReasonCode::Success`] ([MQTT-3.2.2-6]).
    pub session_present: bool,
    /// The Connect Reason Code; 0x80 or above refuses the connection (section 3.2.2.2).
    pub reason_code: ConnectReasonCode,
    /// The CONNACK properties (section 3.2.2.3).
    pub properties: ConnAckProperties,
}

/// The properties of a CONNACK (section 3.2.2.3). Absent ones mean what the specification
/// says they default to, noted on each.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnAckProperties {
    /// Session Expiry Interval the server uses instead of the client's; absent means the
    /// client's (section 3.2.2.3.2).
    pub session_expiry_interval: Option<u32>,
    /// Receive Maximum, the server's limit on unacknowledged QoS 1 and 2 publications;
    /// absent means 65,535 (section 3.2.2.3.3).
    pub receive_maximum: Option<NonZeroU16>,
    /// Maximum QoS the server supports, 0 or 1; absent means 2 (section 3.2.2.3.4).
    pub maximum_qos: Option<QoS>,
    /// Retain Available; absent means true (section 3.2.2.3.5).
    pub retain_available: Option<bool>,
    /// Maximum Packet Size the server accepts; absent means no limit beyond the protocol's
    /// (section 3.2.2.3.6).
    pub maximum_packet_size: Option<NonZeroU32>,
    /// Assigned Client Identifier, for a client that connected with an empty one (section
    /// 3.2.2.3.7, [MQTT-3.2.2-16]).
    pub assigned_client_identifier: Option<String>,
    /// Topic Alias Maximum, the highest Topic Alias the server accepts; absent means 0, none
    /// (section 3.2.2.3.8).
    pub topic_alias_maximum: Option<u16>,
    /// Reason String, for diagnostics (section 3.2.2.3.9).
    pub reason_string: Option<String>,
    /// User Properties, name and value, in order (section 3.2.2.3.10).
    pub user_properties: Vec<(String, String)>,
    /// Wildcard Subscription Available; absent means true (section 3.2.2.3.11).
    pub wildcard_subscription_available: Option<bool>,
    /// Subscription Identifiers Available; absent means true (section 3.2.2.3.12).
    pub subscription_identifier_available: Option<bool>,
    /// Shared Subscription Available; absent means true (section 3.2.2.3.13).
    pub shared_subscription_available: Option<bool>,
    /// Server Keep Alive, which replaces the client's Keep Alive (section 3.2.2.3.14).
    pub server_keep_alive: Option<u16>,
    /// Response Information, the basis of a Response Topic (section 3.2.2.3.15).
    pub response_information: Option<String>,
    /// Server Reference, another server to use (section 3.2.2.3.16).
    pub server_reference: Option<String>,
    /// Authentication Method (section 3.2.2.3.17).
    pub authentication_method: Option<String>,
    /// Authentication Data (section 3.2.2.3.18).
    pub authentication_data: Option<Bytes>,
}

impl ConnAck {
    /// Decodes the variable header of a CONNACK.
    pub(crate) fn decode(body: &Bytes) -> Result<Self, Error> {
        let mut reader = Reader::new(body);
        let flags = reader.u8("Connect Acknowledge Flags")?;
        if flags & !SESSION_PRESENT != 0 {
            return Err(Error::InvalidConnAckFlags { flags });
        }
        let code = reader.u8("Connect Reason Code")?;
        let reason_code = ConnectReasonCode::from_u8(code).ok_or(Error::InvalidReasonCode {
            packet_type: PacketType::ConnAck,
            code,
        })?;
        let properties = ConnAckProperties::decode(&mut reader)?;
        reader.finish(PacketType::ConnAck)?;
        let connack = Self {
            session_present: flags & SESSION_PRESENT != 0,
            reason_code,
            properties,
        };
        connack.check()?;
        Ok(connack)
    }

    /// Session Present only with Success ([MQTT-3.2.2-6]).
    fn check(&self) -> Result<(), Error> {
        if self.session_present && self.reason_code != ConnectReasonCode::Success {
            return Err(Error::SessionPresentWithError);
        }
        Ok(())
    }
}

impl Encode for ConnAck {
    fn first_byte(&self) -> u8 {
        PacketType::ConnAck.value() << 4
    }

    fn remaining_length(&self) -> Result<usize, Error> {
        self.check()?;
        Ok(2 + measure_properties(&self.properties, PropertyContext::ConnAck)?)
    }

    fn write_body(&self, dst: &mut BytesMut) {
        dst.put_u8(if self.session_present {
            SESSION_PRESENT
        } else {
            0
        });
        dst.put_u8(self.reason_code.value());
        put_properties(dst, &self.properties);
    }
}

encode_methods!(ConnAck);

impl ConnAckProperties {
    /// Reads the CONNACK properties.
    fn decode(reader: &mut Reader<'_>) -> Result<Self, Error> {
        use PropertyId as P;
        let context = PropertyContext::ConnAck;
        let mut p = Self::default();
        read_properties(reader, context, |id, reader| match id {
            P::SessionExpiryInterval => {
                once(&mut p.session_expiry_interval, read_u32(reader, id)?, id)
            }
            P::ReceiveMaximum => once(&mut p.receive_maximum, read_nonzero_u16(reader, id)?, id),
            P::MaximumQos => once(&mut p.maximum_qos, read_qos(reader, id)?, id),
            P::RetainAvailable => once(&mut p.retain_available, read_bool(reader, id)?, id),
            P::MaximumPacketSize => once(
                &mut p.maximum_packet_size,
                read_nonzero_u32(reader, id)?,
                id,
            ),
            P::AssignedClientIdentifier => once(
                &mut p.assigned_client_identifier,
                reader.string(id.name())?,
                id,
            ),
            P::TopicAliasMaximum => once(&mut p.topic_alias_maximum, read_u16(reader, id)?, id),
            P::ReasonString => once(&mut p.reason_string, reader.string(id.name())?, id),
            P::UserProperty => {
                p.user_properties.push(reader.string_pair(id.name())?);
                Ok(())
            }
            P::WildcardSubscriptionAvailable => once(
                &mut p.wildcard_subscription_available,
                read_bool(reader, id)?,
                id,
            ),
            P::SubscriptionIdentifierAvailable => once(
                &mut p.subscription_identifier_available,
                read_bool(reader, id)?,
                id,
            ),
            P::SharedSubscriptionAvailable => once(
                &mut p.shared_subscription_available,
                read_bool(reader, id)?,
                id,
            ),
            P::ServerKeepAlive => once(&mut p.server_keep_alive, read_u16(reader, id)?, id),
            P::ResponseInformation => {
                once(&mut p.response_information, reader.string(id.name())?, id)
            }
            P::ServerReference => once(&mut p.server_reference, reader.string(id.name())?, id),
            P::AuthenticationMethod => {
                once(&mut p.authentication_method, reader.string(id.name())?, id)
            }
            P::AuthenticationData => {
                once(&mut p.authentication_data, reader.binary(id.name())?, id)
            }
            _ => Err(unexpected(id, context)),
        })?;
        Ok(p)
    }
}

impl Properties for ConnAckProperties {
    fn for_each(&self, mut f: impl FnMut(PropertyId, Value<'_>)) {
        use PropertyId as P;
        if let Some(value) = self.session_expiry_interval {
            f(P::SessionExpiryInterval, Value::FourByteInteger(value));
        }
        if let Some(value) = self.receive_maximum {
            f(P::ReceiveMaximum, Value::TwoByteInteger(value.get()));
        }
        if let Some(value) = self.maximum_qos {
            f(P::MaximumQos, Value::Byte(value.value()));
        }
        if let Some(value) = self.retain_available {
            f(P::RetainAvailable, Value::Byte(value.into()));
        }
        if let Some(value) = self.maximum_packet_size {
            f(P::MaximumPacketSize, Value::FourByteInteger(value.get()));
        }
        if let Some(value) = &self.assigned_client_identifier {
            f(P::AssignedClientIdentifier, Value::String(value));
        }
        if let Some(value) = self.topic_alias_maximum {
            f(P::TopicAliasMaximum, Value::TwoByteInteger(value));
        }
        if let Some(value) = &self.reason_string {
            f(P::ReasonString, Value::String(value));
        }
        for (name, value) in &self.user_properties {
            f(P::UserProperty, Value::Pair(name, value));
        }
        if let Some(value) = self.wildcard_subscription_available {
            f(P::WildcardSubscriptionAvailable, Value::Byte(value.into()));
        }
        if let Some(value) = self.subscription_identifier_available {
            f(
                P::SubscriptionIdentifierAvailable,
                Value::Byte(value.into()),
            );
        }
        if let Some(value) = self.shared_subscription_available {
            f(P::SharedSubscriptionAvailable, Value::Byte(value.into()));
        }
        if let Some(value) = self.server_keep_alive {
            f(P::ServerKeepAlive, Value::TwoByteInteger(value));
        }
        if let Some(value) = &self.response_information {
            f(P::ResponseInformation, Value::String(value));
        }
        if let Some(value) = &self.server_reference {
            f(P::ServerReference, Value::String(value));
        }
        if let Some(value) = &self.authentication_method {
            f(P::AuthenticationMethod, Value::String(value));
        }
        if let Some(value) = &self.authentication_data {
            f(P::AuthenticationData, Value::Binary(value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProtocolRefusal;
    use crate::test_util::{concat, encode_parts, prefixed, properties, sample};

    fn with_properties(props: &[&[u8]]) -> Bytes {
        concat(&[&[0x00, 0x00], &properties(props)])
    }

    #[test]
    fn a_successful_connack_without_properties_is_five_bytes() {
        let mut dst = BytesMut::new();
        ConnAck::default().encode(&mut dst).unwrap();
        assert_eq!(dst[..], [0x20, 0x03, 0x00, 0x00, 0x00]);
        assert_eq!(
            ConnAck::decode(&Bytes::from_static(&[0x00, 0x00, 0x00])),
            Ok(ConnAck::default())
        );
    }

    #[test]
    fn mqtt_3_1_2_2_the_mqtt_5_refusal_is_a_connack_with_0x84() {
        let refusal = ConnAck {
            reason_code: ConnectReasonCode::UnsupportedProtocolVersion,
            ..ConnAck::default()
        };
        let mut dst = BytesMut::new();
        refusal.encode(&mut dst).unwrap();
        assert_eq!(dst[..], *ProtocolRefusal::ConnAckV5.bytes());
    }

    #[test]
    fn mqtt_3_2_2_1_reserved_acknowledge_flags_are_malformed() {
        for flags in [0x02, 0x80, 0xFE, 0xFF] {
            let body = Bytes::copy_from_slice(&[flags, 0x00, 0x00]);
            let error = ConnAck::decode(&body).unwrap_err();
            assert_eq!(error, Error::InvalidConnAckFlags { flags });
            assert_eq!(
                error.disconnect_reason_code(),
                crate::DisconnectReasonCode::MalformedPacket
            );
        }
    }

    #[test]
    fn mqtt_3_2_2_6_session_present_only_with_success() {
        let present = ConnAck::decode(&Bytes::from_static(&[0x01, 0x00, 0x00])).unwrap();
        assert!(present.session_present);
        assert_eq!(encode_parts(&present).1[..], [0x01, 0x00, 0x00]);
        assert_eq!(
            ConnAck::decode(&Bytes::from_static(&[0x01, 0x80, 0x00])),
            Err(Error::SessionPresentWithError)
        );
        let refused = ConnAck {
            session_present: true,
            reason_code: ConnectReasonCode::NotAuthorized,
            ..ConnAck::default()
        };
        assert_eq!(refused.encoded_len(), Err(Error::SessionPresentWithError));
    }

    #[test]
    fn mqtt_3_2_2_8_the_reason_code_comes_from_table_3_1() {
        for code in 0..=u8::MAX {
            let body = Bytes::copy_from_slice(&[0x00, code, 0x00]);
            match ConnectReasonCode::from_u8(code) {
                Some(reason_code) => assert_eq!(
                    ConnAck::decode(&body).map(|connack| connack.reason_code),
                    Ok(reason_code)
                ),
                None => assert_eq!(
                    ConnAck::decode(&body),
                    Err(Error::InvalidReasonCode {
                        packet_type: PacketType::ConnAck,
                        code
                    })
                ),
            }
        }
    }

    #[test]
    fn mqtt_2_2_2_1_the_property_length_is_present_even_when_zero() {
        assert_eq!(
            ConnAck::decode(&Bytes::from_static(&[0x00, 0x00])),
            Err(Error::Truncated {
                field: "Property Length"
            })
        );
        assert!(matches!(
            ConnAck::decode(&Bytes::from_static(&[0x00, 0x00, 0x00, 0x00])),
            Err(Error::TrailingBytes {
                packet_type: PacketType::ConnAck,
                count: 1
            })
        ));
    }

    #[test]
    fn connack_properties_round_trip() {
        let connack = ConnAck {
            session_present: true,
            reason_code: ConnectReasonCode::Success,
            properties: ConnAckProperties {
                session_expiry_interval: Some(0),
                receive_maximum: NonZeroU16::new(100),
                maximum_qos: Some(QoS::AtLeastOnce),
                retain_available: Some(false),
                maximum_packet_size: NonZeroU32::new(1_048_576),
                assigned_client_identifier: Some("auto-1F2E".into()),
                topic_alias_maximum: Some(0),
                reason_string: Some("welcome".into()),
                user_properties: vec![("region".into(), "ca".into())],
                wildcard_subscription_available: Some(true),
                subscription_identifier_available: Some(false),
                shared_subscription_available: Some(true),
                server_keep_alive: Some(45),
                response_information: Some("responses/".into()),
                server_reference: Some("edge-2.example".into()),
                authentication_method: Some("SCRAM-SHA-256".into()),
                authentication_data: Some(Bytes::from_static(b"server-final")),
            },
        };
        let (first, body) = encode_parts(&connack);
        assert_eq!(first, 0x20);
        assert_eq!(ConnAck::decode(&body), Ok(connack));
    }

    #[test]
    fn section_2_2_2_2_properties_table_2_4_does_not_allow_are_malformed() {
        for id in PropertyId::ALL {
            let result = ConnAck::decode(&with_properties(&[&sample(id)]));
            if id.is_valid_in(PropertyContext::ConnAck) {
                assert!(result.is_ok(), "{id}: {result:?}");
            } else {
                assert_eq!(
                    result,
                    Err(Error::InvalidPropertyId {
                        context: PropertyContext::ConnAck,
                        id: id.value().into()
                    })
                );
            }
        }
    }

    #[test]
    fn section_3_2_2_3_properties_other_than_user_property_appear_once() {
        for id in PropertyId::ALL {
            if id == PropertyId::UserProperty || !id.is_valid_in(PropertyContext::ConnAck) {
                continue;
            }
            let property = sample(id);
            assert_eq!(
                ConnAck::decode(&with_properties(&[&property, &property])),
                Err(Error::DuplicateProperty { property: id })
            );
        }
    }

    #[test]
    fn section_3_2_2_3_values_the_connack_properties_do_not_allow() {
        let cases: [(&[u8], PropertyId, u32); 7] = [
            (&[0x24, 0x02], PropertyId::MaximumQos, 2),
            (&[0x25, 0x02], PropertyId::RetainAvailable, 2),
            (&[0x28, 0x03], PropertyId::WildcardSubscriptionAvailable, 3),
            (
                &[0x29, 0x80],
                PropertyId::SubscriptionIdentifierAvailable,
                128,
            ),
            (&[0x2A, 0xFF], PropertyId::SharedSubscriptionAvailable, 255),
            (&[0x21, 0x00, 0x00], PropertyId::ReceiveMaximum, 0),
            (
                &[0x27, 0x00, 0x00, 0x00, 0x00],
                PropertyId::MaximumPacketSize,
                0,
            ),
        ];
        for (property, id, value) in cases {
            assert_eq!(
                ConnAck::decode(&with_properties(&[property])),
                Err(Error::InvalidPropertyValue {
                    property: id,
                    value
                })
            );
        }
        // A Maximum QoS of 2 is what an absent one means; it is never sent.
        let connack = ConnAck {
            properties: ConnAckProperties {
                maximum_qos: Some(QoS::ExactlyOnce),
                ..ConnAckProperties::default()
            },
            ..ConnAck::default()
        };
        assert_eq!(
            connack.encoded_len(),
            Err(Error::InvalidPropertyValue {
                property: PropertyId::MaximumQos,
                value: 2
            })
        );
    }

    #[test]
    fn string_properties_follow_the_string_rules() {
        let property = concat(&[&[0x1F], &prefixed(&[0xED, 0xA0, 0x80])]);
        assert_eq!(
            ConnAck::decode(&with_properties(&[&property])),
            Err(Error::InvalidUtf8 {
                field: "Reason String"
            })
        );
    }
}
