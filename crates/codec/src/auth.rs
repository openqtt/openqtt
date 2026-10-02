//! AUTH (section 3.15).

use bytes::{BufMut, Bytes, BytesMut};

use crate::encode::{Encode, encode_methods};
use crate::primitives::Reader;
use crate::property::{
    Properties, Value, measure_properties, once, property_length, put_properties, read_properties,
    unexpected,
};
use crate::{AuthReasonCode, Error, PacketType, PropertyContext, PropertyId};

/// AUTH: one step of an extended authentication exchange, either way (section 3.15).
///
/// Every AUTH names its Authentication Method (section 3.15.2.2.2) except the bare form, a
/// Remaining Length of 0, which means Success with no properties (section 3.15.2.1). That is
/// [`Auth::default`]. Whether the method matches the CONNECT's ([MQTT-4.12.0-5]) is the
/// session's to check.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Auth {
    /// The Authenticate Reason Code (section 3.15.2.1).
    pub reason_code: AuthReasonCode,
    /// The AUTH properties (section 3.15.2.2).
    pub properties: AuthProperties,
}

/// The properties of an AUTH (section 3.15.2.2).
#[derive(Clone, PartialEq, Eq, Default)]
pub struct AuthProperties {
    /// Authentication Method, required but in the bare form (section 3.15.2.2.2).
    pub authentication_method: Option<String>,
    /// Authentication Data (section 3.15.2.2.3).
    pub authentication_data: Option<Bytes>,
    /// Reason String, for diagnostics (section 3.15.2.2.4).
    pub reason_string: Option<String>,
    /// User Properties, name and value, in order (section 3.15.2.2.5).
    pub user_properties: Vec<(String, String)>,
}

/// Debug output that never shows authentication_data: a credential must not reach a log
/// through `{:?}`.
impl core::fmt::Debug for AuthProperties {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AuthProperties")
            .field("authentication_method", &self.authentication_method)
            .field(
                "authentication_data",
                &crate::redact::Redacted(&self.authentication_data),
            )
            .field("reason_string", &self.reason_string)
            .field("user_properties", &self.user_properties)
            .finish()
    }
}

impl Auth {
    /// Decodes the variable header of an AUTH.
    ///
    /// The Reason Code and the Property Length may be left off together, a Remaining Length
    /// of 0, but not the Property Length alone: section 3.15.2.1 allows only the first, where
    /// section 3.14.2.2.1 lets a DISCONNECT do the second. A Remaining Length of 1 is
    /// therefore malformed.
    pub(crate) fn decode(body: &Bytes) -> Result<Self, Error> {
        let mut reader = Reader::new(body);
        if reader.is_empty() {
            return Ok(Self::default());
        }
        let code = reader.u8("Authenticate Reason Code")?;
        let reason_code = AuthReasonCode::from_u8(code).ok_or(Error::InvalidReasonCode {
            packet_type: PacketType::Auth,
            code,
        })?;
        let properties = AuthProperties::decode(&mut reader)?;
        reader.finish(PacketType::Auth)?;
        let auth = Self {
            reason_code,
            properties,
        };
        auth.check()?;
        Ok(auth)
    }

    /// Whether this is the bare form: Success with no properties.
    fn is_bare(&self) -> bool {
        self.reason_code == AuthReasonCode::Success && property_length(&self.properties) == 0
    }

    /// Every AUTH but the bare form names its Authentication Method (section 3.15.2.2.2).
    fn check(&self) -> Result<(), Error> {
        if self.properties.authentication_method.is_none() && !self.is_bare() {
            return Err(Error::MissingAuthenticationMethod {
                packet_type: PacketType::Auth,
            });
        }
        Ok(())
    }
}

impl Encode for Auth {
    fn first_byte(&self) -> u8 {
        PacketType::Auth.value() << 4
    }

    fn remaining_length(&self) -> Result<usize, Error> {
        self.check()?;
        let size = measure_properties(&self.properties, PropertyContext::Auth)?;
        Ok(if self.is_bare() { 0 } else { 1 + size })
    }

    fn write_body(&self, dst: &mut BytesMut) {
        if !self.is_bare() {
            dst.put_u8(self.reason_code.value());
            put_properties(dst, &self.properties);
        }
    }
}

encode_methods!(Auth);

impl AuthProperties {
    /// Reads the AUTH properties.
    fn decode(reader: &mut Reader<'_>) -> Result<Self, Error> {
        use PropertyId as P;
        let context = PropertyContext::Auth;
        let mut p = Self::default();
        read_properties(reader, context, |id, reader| match id {
            P::AuthenticationMethod => {
                once(&mut p.authentication_method, reader.string(id.name())?, id)
            }
            P::AuthenticationData => {
                once(&mut p.authentication_data, reader.binary(id.name())?, id)
            }
            P::ReasonString => once(&mut p.reason_string, reader.string(id.name())?, id),
            P::UserProperty => {
                p.user_properties.push(reader.string_pair(id.name())?);
                Ok(())
            }
            _ => Err(unexpected(id, context)),
        })?;
        Ok(p)
    }
}

impl Properties for AuthProperties {
    fn for_each(&self, mut f: impl FnMut(PropertyId, Value<'_>)) {
        use PropertyId as P;
        if let Some(value) = &self.authentication_method {
            f(P::AuthenticationMethod, Value::String(value));
        }
        if let Some(value) = &self.authentication_data {
            f(P::AuthenticationData, Value::Binary(value));
        }
        if let Some(value) = &self.reason_string {
            f(P::ReasonString, Value::String(value));
        }
        for (name, value) in &self.user_properties {
            f(P::UserProperty, Value::Pair(name, value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{concat, encode_parts, prefixed, properties, sample};
    use crate::{ConnectReasonCode, DisconnectReasonCode};

    fn method(name: &[u8]) -> Vec<u8> {
        concat(&[&[0x15], &prefixed(name)]).to_vec()
    }

    #[test]
    fn section_3_15_2_1_a_remaining_length_of_0_means_success() {
        assert_eq!(Auth::decode(&Bytes::new()), Ok(Auth::default()));
        let mut dst = BytesMut::new();
        Auth::default().encode(&mut dst).unwrap();
        assert_eq!(dst[..], [0xF0, 0x00]);
        // Written out in full, the same packet.
        assert_eq!(
            Auth::decode(&Bytes::from_static(&[0x00, 0x00])),
            Ok(Auth::default())
        );
    }

    #[test]
    fn section_3_15_2_1_a_remaining_length_of_1_is_malformed() {
        let error = Auth::decode(&Bytes::from_static(&[0x18])).unwrap_err();
        assert_eq!(
            error,
            Error::Truncated {
                field: "Property Length"
            }
        );
        assert_eq!(
            error.disconnect_reason_code(),
            DisconnectReasonCode::MalformedPacket
        );
    }

    #[test]
    fn section_3_15_2_2_2_the_authentication_method_is_required() {
        for body in [
            &[0x18, 0x00][..],
            &[0x19, 0x00],
            &[0x00, 0x03, 0x1F, 0x00, 0x00],
        ] {
            let error = Auth::decode(&Bytes::copy_from_slice(body)).unwrap_err();
            assert_eq!(
                error,
                Error::MissingAuthenticationMethod {
                    packet_type: PacketType::Auth
                },
                "{body:02X?}"
            );
            assert_eq!(
                error.connack_reason_code(),
                ConnectReasonCode::ProtocolError
            );
        }
        let continuing = Auth {
            reason_code: AuthReasonCode::ContinueAuthentication,
            ..Auth::default()
        };
        assert_eq!(
            continuing.encoded_len(),
            Err(Error::MissingAuthenticationMethod {
                packet_type: PacketType::Auth
            })
        );
    }

    #[test]
    fn section_4_12_the_scram_example_round_trips() {
        // Server to client: AUTH rc=0x18, Authentication Method "SCRAM-SHA-1",
        // Authentication Data server-first-data.
        let body = concat(&[
            &[0x18],
            &properties(&[
                &method(b"SCRAM-SHA-1"),
                &concat(&[&[0x16], &prefixed(b"server-first-data")]),
            ]),
        ]);
        let auth = Auth::decode(&body).unwrap();
        assert_eq!(
            auth,
            Auth {
                reason_code: AuthReasonCode::ContinueAuthentication,
                properties: AuthProperties {
                    authentication_method: Some("SCRAM-SHA-1".into()),
                    authentication_data: Some(Bytes::from_static(b"server-first-data")),
                    ..AuthProperties::default()
                },
            }
        );
        assert_eq!(encode_parts(&auth), (0xF0, body));
    }

    #[test]
    fn mqtt_3_15_2_1_reason_codes_come_from_table_3_11() {
        for code in 0..=u8::MAX {
            let body = concat(&[&[code], &properties(&[&method(b"m")])]);
            match AuthReasonCode::from_u8(code) {
                Some(reason) => assert_eq!(Auth::decode(&body).unwrap().reason_code, reason),
                None => assert_eq!(
                    Auth::decode(&body),
                    Err(Error::InvalidReasonCode {
                        packet_type: PacketType::Auth,
                        code
                    })
                ),
            }
        }
    }

    #[test]
    fn section_2_2_2_2_and_3_15_2_2_auth_properties() {
        let named = method(b"m");
        for id in PropertyId::ALL {
            let property = sample(id);
            let body = concat(&[&[0x18], &properties(&[&named, &property])]);
            let result = Auth::decode(&body);
            if id == PropertyId::AuthenticationMethod {
                assert_eq!(result, Err(Error::DuplicateProperty { property: id }));
            } else if id.is_valid_in(PropertyContext::Auth) {
                assert!(result.is_ok(), "{id}: {result:?}");
                if id != PropertyId::UserProperty {
                    let twice = concat(&[&[0x18], &properties(&[&named, &property, &property])]);
                    assert_eq!(
                        Auth::decode(&twice),
                        Err(Error::DuplicateProperty { property: id })
                    );
                }
            } else {
                assert_eq!(
                    result,
                    Err(Error::InvalidPropertyId {
                        context: PropertyContext::Auth,
                        id: id.value().into()
                    })
                );
            }
        }
    }
}
