//! DISCONNECT (section 3.14).

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the decoder that calls Disconnect::decode arrives in a following commit"
    )
)]

use bytes::{BufMut, Bytes, BytesMut};

use crate::encode::{Encode, encode_methods};
use crate::primitives::Reader;
use crate::property::{
    Properties, Value, measure_properties, once, property_length, put_properties, read_properties,
    read_u32, unexpected,
};
use crate::{DisconnectReasonCode, Error, PacketType, PropertyContext, PropertyId};

/// DISCONNECT: the last packet either end sends on a connection, saying why it is closing
/// (section 3.14).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Disconnect {
    /// The Disconnect Reason Code (section 3.14.2.1). Normal disconnection, with no
    /// properties, is left off the wire.
    pub reason_code: DisconnectReasonCode,
    /// The DISCONNECT properties (section 3.14.2.2).
    pub properties: DisconnectProperties,
}

/// The properties of a DISCONNECT (section 3.14.2.2).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DisconnectProperties {
    /// Session Expiry Interval, a client's new one for the session; never sent by the server
    /// ([MQTT-3.14.2-2]) (section 3.14.2.2.2).
    pub session_expiry_interval: Option<u32>,
    /// Reason String, for diagnostics (section 3.14.2.2.3).
    pub reason_string: Option<String>,
    /// User Properties, name and value, in order (section 3.14.2.2.4).
    pub user_properties: Vec<(String, String)>,
    /// Server Reference, the server to use instead, with reason code 0x9C or 0x9D (section
    /// 3.14.2.2.5).
    pub server_reference: Option<String>,
}

impl Disconnect {
    /// Decodes the variable header of a DISCONNECT. A Remaining Length of 0 means Normal
    /// disconnection (section 3.14.2.1), and one below 2 means no properties (section
    /// 3.14.2.2.1).
    pub(crate) fn decode(body: &Bytes) -> Result<Self, Error> {
        let mut reader = Reader::new(body);
        if reader.is_empty() {
            return Ok(Self::default());
        }
        let code = reader.u8("Disconnect Reason Code")?;
        let reason_code = DisconnectReasonCode::from_u8(code).ok_or(Error::InvalidReasonCode {
            packet_type: PacketType::Disconnect,
            code,
        })?;
        let properties = if reader.is_empty() {
            DisconnectProperties::default()
        } else {
            DisconnectProperties::decode(&mut reader)?
        };
        reader.finish(PacketType::Disconnect)?;
        Ok(Self {
            reason_code,
            properties,
        })
    }
}

impl Encode for Disconnect {
    fn first_byte(&self) -> u8 {
        PacketType::Disconnect.value() << 4
    }

    fn remaining_length(&self) -> Result<usize, Error> {
        let size = measure_properties(&self.properties, PropertyContext::Disconnect)?;
        let empty = property_length(&self.properties) == 0;
        Ok(match (self.reason_code, empty) {
            (DisconnectReasonCode::NormalDisconnection, true) => 0,
            (_, true) => 1,
            (_, false) => 1 + size,
        })
    }

    fn write_body(&self, dst: &mut BytesMut) {
        let empty = property_length(&self.properties) == 0;
        if self.reason_code != DisconnectReasonCode::NormalDisconnection || !empty {
            dst.put_u8(self.reason_code.value());
        }
        if !empty {
            put_properties(dst, &self.properties);
        }
    }
}

encode_methods!(Disconnect);

impl DisconnectProperties {
    /// Reads the DISCONNECT properties.
    fn decode(reader: &mut Reader<'_>) -> Result<Self, Error> {
        use PropertyId as P;
        let context = PropertyContext::Disconnect;
        let mut p = Self::default();
        read_properties(reader, context, |id, reader| match id {
            P::SessionExpiryInterval => {
                once(&mut p.session_expiry_interval, read_u32(reader, id)?, id)
            }
            P::ReasonString => once(&mut p.reason_string, reader.string(id.name())?, id),
            P::UserProperty => {
                p.user_properties.push(reader.string_pair(id.name())?);
                Ok(())
            }
            P::ServerReference => once(&mut p.server_reference, reader.string(id.name())?, id),
            _ => Err(unexpected(id, context)),
        })?;
        Ok(p)
    }
}

impl Properties for DisconnectProperties {
    fn for_each(&self, mut f: impl FnMut(PropertyId, Value<'_>)) {
        use PropertyId as P;
        if let Some(value) = self.session_expiry_interval {
            f(P::SessionExpiryInterval, Value::FourByteInteger(value));
        }
        if let Some(value) = &self.reason_string {
            f(P::ReasonString, Value::String(value));
        }
        for (name, value) in &self.user_properties {
            f(P::UserProperty, Value::Pair(name, value));
        }
        if let Some(value) = &self.server_reference {
            f(P::ServerReference, Value::String(value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{concat, encode_parts, properties, sample};

    #[test]
    fn section_3_14_2_1_a_remaining_length_of_0_means_normal_disconnection() {
        assert_eq!(Disconnect::decode(&Bytes::new()), Ok(Disconnect::default()));
        let mut dst = BytesMut::new();
        Disconnect::default().encode(&mut dst).unwrap();
        assert_eq!(dst[..], [0xE0, 0x00]);
    }

    #[test]
    fn section_3_14_2_2_1_a_remaining_length_of_1_means_no_properties() {
        let disconnect = Disconnect::decode(&Bytes::from_static(&[0x8E])).unwrap();
        assert_eq!(
            disconnect.reason_code,
            DisconnectReasonCode::SessionTakenOver
        );
        assert_eq!(disconnect.properties, DisconnectProperties::default());
        let mut dst = BytesMut::new();
        disconnect.encode(&mut dst).unwrap();
        assert_eq!(dst[..], [0xE0, 0x01, 0x8E]);
    }

    #[test]
    fn section_3_14_2_figure_3_24_variable_header() {
        // Normal disconnection with a Session Expiry Interval of 0. The figure labels the
        // Property Length 5 and draws its bits as 7; the five bytes that follow settle it.
        let body = Bytes::from_static(&[0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x00]);
        let disconnect = Disconnect::decode(&body).unwrap();
        assert_eq!(
            disconnect,
            Disconnect {
                reason_code: DisconnectReasonCode::NormalDisconnection,
                properties: DisconnectProperties {
                    session_expiry_interval: Some(0),
                    ..DisconnectProperties::default()
                },
            }
        );
        assert_eq!(encode_parts(&disconnect), (0xE0, body));
    }

    #[test]
    fn mqtt_3_14_2_1_reason_codes_come_from_table_3_10() {
        for code in 0..=u8::MAX {
            let body = Bytes::copy_from_slice(&[code]);
            match DisconnectReasonCode::from_u8(code) {
                Some(reason) => {
                    assert_eq!(Disconnect::decode(&body).unwrap().reason_code, reason);
                }
                None => assert_eq!(
                    Disconnect::decode(&body),
                    Err(Error::InvalidReasonCode {
                        packet_type: PacketType::Disconnect,
                        code
                    })
                ),
            }
        }
        // 0x8C, from Table 2-6 though not Table 3-10.
        assert_eq!(
            Disconnect::decode(&Bytes::from_static(&[0x8C]))
                .unwrap()
                .reason_code,
            DisconnectReasonCode::BadAuthenticationMethod
        );
    }

    #[test]
    fn disconnect_properties_round_trip() {
        let disconnect = Disconnect {
            reason_code: DisconnectReasonCode::UseAnotherServer,
            properties: DisconnectProperties {
                session_expiry_interval: Some(3600),
                reason_string: Some("draining".into()),
                user_properties: vec![("node".into(), "edge-3".into())],
                server_reference: Some("edge-4.example:14567".into()),
            },
        };
        let (first, body) = encode_parts(&disconnect);
        assert_eq!(first, 0xE0);
        assert_eq!(Disconnect::decode(&body), Ok(disconnect));
    }

    #[test]
    fn section_2_2_2_2_and_3_14_2_2_disconnect_properties() {
        for id in PropertyId::ALL {
            let body = concat(&[&[0x80], &properties(&[&sample(id)])]);
            let result = Disconnect::decode(&body);
            if id.is_valid_in(PropertyContext::Disconnect) {
                assert!(result.is_ok(), "{id}: {result:?}");
                if id != PropertyId::UserProperty {
                    let twice = concat(&[&[0x80], &properties(&[&sample(id), &sample(id)])]);
                    assert_eq!(
                        Disconnect::decode(&twice),
                        Err(Error::DuplicateProperty { property: id })
                    );
                }
            } else {
                assert_eq!(
                    result,
                    Err(Error::InvalidPropertyId {
                        context: PropertyContext::Disconnect,
                        id: id.value().into()
                    })
                );
            }
        }
    }

    #[test]
    fn nothing_follows_the_properties() {
        let body = concat(&[&[0x00], &properties(&[]), &[0x00]]);
        assert_eq!(
            Disconnect::decode(&body),
            Err(Error::TrailingBytes {
                packet_type: PacketType::Disconnect,
                count: 1
            })
        );
    }
}
