//! CONNECT (section 3.1), and the bytes that refuse a CONNECT that is not MQTT 5.0.

use std::num::{NonZeroU16, NonZeroU32};

use bytes::{BufMut, Bytes, BytesMut};

use crate::encode::{Encode, encode_methods};
use crate::primitives::{Reader, binary_len, put_binary, put_string, string_len};
use crate::property::{
    Properties, Value, measure_properties, once, put_properties, read_bool, read_nonzero_u16,
    read_nonzero_u32, read_payload_format, read_properties, read_u16, read_u32, unexpected,
};
use crate::{Error, PacketType, PayloadFormat, PropertyContext, PropertyId, QoS};

/// The Protocol Name of every MQTT version since 3.1.1 ([MQTT-3.1.2-1]).
pub const PROTOCOL_NAME: &str = "MQTT";

/// The Protocol Version of MQTT 5.0 (section 3.1.2.2).
pub const PROTOCOL_VERSION: u8 = 5;

/// Connect Flags (section 3.1.2.3, Figure 3-4).
const RESERVED: u8 = 0x01;
const CLEAN_START: u8 = 0x02;
const WILL_FLAG: u8 = 0x04;
const WILL_QOS_SHIFT: u8 = 3;
const WILL_RETAIN: u8 = 0x20;
const PASSWORD_FLAG: u8 = 0x40;
const USER_NAME_FLAG: u8 = 0x80;

/// CONNECT: the first packet a client sends on a connection, asking the server for a session
/// (section 3.1).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Connect {
    /// Clean Start: discard any existing session and start a new one (section 3.1.2.4).
    pub clean_start: bool,
    /// Keep Alive in seconds, the longest the client may stay silent; 0 turns the mechanism
    /// off (section 3.1.2.10).
    pub keep_alive: u16,
    /// The CONNECT properties (section 3.1.2.11).
    pub properties: ConnectProperties,
    /// The Client Identifier (section 3.1.3.1). It may be empty, in which case a server that
    /// accepts it assigns one ([MQTT-3.1.3-6]); whether to accept it is not the codec's call.
    pub client_id: String,
    /// The Will Message, present exactly when the Will Flag is set (section 3.1.2.5).
    pub will: Option<Will>,
    /// The User Name, present exactly when the User Name Flag is set (sections 3.1.2.8 and
    /// 3.1.3.5).
    pub username: Option<String>,
    /// The Password, present exactly when the Password Flag is set: any credential, as Binary
    /// Data (sections 3.1.2.9 and 3.1.3.6). Unlike MQTT 3.1.1, it may come without a User
    /// Name.
    pub password: Option<Bytes>,
}

/// The Will Message of a CONNECT: what the server publishes for the client when the
/// connection ends without a normal DISCONNECT (sections 3.1.2.5 to 3.1.2.7 and 3.1.3.2 to
/// 3.1.3.4).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Will {
    /// Will QoS (section 3.1.2.6).
    pub qos: QoS,
    /// Will Retain: publish the Will Message as a retained message (section 3.1.2.7).
    pub retain: bool,
    /// The Will Properties (section 3.1.3.2).
    pub properties: WillProperties,
    /// The Will Topic (section 3.1.3.3).
    pub topic: String,
    /// The Will Payload (section 3.1.3.4).
    pub payload: Bytes,
}

/// The properties of a CONNECT (section 3.1.2.11).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnectProperties {
    /// Session Expiry Interval in seconds; absent means 0, the session ends with the
    /// connection, and `u32::MAX` means it never expires (section 3.1.2.11.2).
    pub session_expiry_interval: Option<u32>,
    /// Receive Maximum, the client's limit on unacknowledged QoS 1 and 2 publications;
    /// absent means 65,535 (section 3.1.2.11.3).
    pub receive_maximum: Option<NonZeroU16>,
    /// Maximum Packet Size the client accepts; absent means no limit beyond the protocol's
    /// (section 3.1.2.11.4).
    pub maximum_packet_size: Option<NonZeroU32>,
    /// Topic Alias Maximum, the highest Topic Alias the client accepts; absent means 0, none
    /// (section 3.1.2.11.5).
    pub topic_alias_maximum: Option<u16>,
    /// Request Response Information; absent means false (section 3.1.2.11.6).
    pub request_response_information: Option<bool>,
    /// Request Problem Information; absent means true (section 3.1.2.11.7).
    pub request_problem_information: Option<bool>,
    /// User Properties, name and value, in order (section 3.1.2.11.8).
    pub user_properties: Vec<(String, String)>,
    /// Authentication Method, which starts extended authentication (section 3.1.2.11.9).
    pub authentication_method: Option<String>,
    /// Authentication Data, which needs an Authentication Method (section 3.1.2.11.10).
    pub authentication_data: Option<Bytes>,
}

/// The Will Properties of a CONNECT (section 3.1.3.2).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WillProperties {
    /// Will Delay Interval in seconds; absent means 0 (section 3.1.3.2.2).
    pub will_delay_interval: Option<u32>,
    /// Payload Format Indicator of the Will Payload (section 3.1.3.2.3).
    pub payload_format_indicator: Option<PayloadFormat>,
    /// Message Expiry Interval of the Will Message in seconds (section 3.1.3.2.4).
    pub message_expiry_interval: Option<u32>,
    /// Content Type of the Will Message (section 3.1.3.2.5).
    pub content_type: Option<String>,
    /// Response Topic, which makes the Will Message a request (section 3.1.3.2.6).
    pub response_topic: Option<String>,
    /// Correlation Data (section 3.1.3.2.7).
    pub correlation_data: Option<Bytes>,
    /// User Properties, name and value, in the order the server must keep
    /// ([MQTT-3.1.3-10]).
    pub user_properties: Vec<(String, String)>,
}

impl Connect {
    /// Decodes the variable header and payload of a CONNECT.
    pub(crate) fn decode(body: &Bytes) -> Result<Self, Error> {
        let mut reader = Reader::new(body);
        // The name tells MQTT from other protocols (section 3.1.2.1), so it is compared as
        // bytes: one that is not "MQTT", even one that is not UTF-8, names another protocol
        // rather than a malformed MQTT packet.
        let name = reader.binary("Protocol Name")?;
        let level = reader.u8("Protocol Version")?;
        if name != PROTOCOL_NAME.as_bytes() || level != PROTOCOL_VERSION {
            // An older protocol lays the rest out differently, so reading on would only
            // invent errors. [MQTT-3.1.2-1] [MQTT-3.1.2-2]
            return Err(Error::UnsupportedProtocol {
                name: String::from_utf8_lossy(&name).into_owned(),
                level,
            });
        }

        let flags = reader.u8("Connect Flags")?;
        let will_flag = flags & WILL_FLAG != 0;
        let will_retain = flags & WILL_RETAIN != 0;
        let will_qos = QoS::from_u8((flags >> WILL_QOS_SHIFT) & 0b11)
            .filter(|&qos| {
                // [MQTT-3.1.2-3], then [MQTT-3.1.2-11] and [MQTT-3.1.2-13]; a Will QoS of 3
                // failed from_u8 already ([MQTT-3.1.2-12]).
                flags & RESERVED == 0 && (will_flag || (qos == QoS::AtMostOnce && !will_retain))
            })
            .ok_or(Error::InvalidConnectFlags { flags })?;

        let keep_alive = reader.u16("Keep Alive")?;
        let properties = ConnectProperties::decode(&mut reader)?;
        // The payload fields come in this order when present ([MQTT-3.1.3-1]), the Client
        // Identifier always ([MQTT-3.1.3-3]).
        let client_id = reader.string("Client Identifier")?;
        let will = if will_flag {
            let properties = WillProperties::decode(&mut reader)?;
            let topic = reader.string("Will Topic")?;
            let payload = reader.binary("Will Payload")?;
            Some(Will {
                qos: will_qos,
                retain: will_retain,
                properties,
                topic,
                payload,
            })
        } else {
            None
        };
        let username = if flags & USER_NAME_FLAG != 0 {
            Some(reader.string("User Name")?)
        } else {
            None
        };
        let password = if flags & PASSWORD_FLAG != 0 {
            Some(reader.binary("Password")?)
        } else {
            None
        };
        reader.finish(PacketType::Connect)?;

        Ok(Self {
            clean_start: flags & CLEAN_START != 0,
            keep_alive,
            properties,
            client_id,
            will,
            username,
            password,
        })
    }

    /// The Connect Flags byte for this packet.
    fn flags(&self) -> u8 {
        let mut flags = 0;
        if self.clean_start {
            flags |= CLEAN_START;
        }
        if let Some(will) = &self.will {
            flags |= WILL_FLAG | will.qos.value() << WILL_QOS_SHIFT;
            if will.retain {
                flags |= WILL_RETAIN;
            }
        }
        if self.password.is_some() {
            flags |= PASSWORD_FLAG;
        }
        if self.username.is_some() {
            flags |= USER_NAME_FLAG;
        }
        flags
    }
}

impl Encode for Connect {
    fn first_byte(&self) -> u8 {
        PacketType::Connect.value() << 4
    }

    fn remaining_length(&self) -> Result<usize, Error> {
        self.properties.check()?;
        // Protocol Name, Protocol Version, Connect Flags and Keep Alive.
        let mut len = string_len(PROTOCOL_NAME, "Protocol Name")? + 1 + 1 + 2;
        len += measure_properties(&self.properties, PropertyContext::Connect)?;
        len += string_len(&self.client_id, "Client Identifier")?;
        if let Some(will) = &self.will {
            len += measure_properties(&will.properties, PropertyContext::Will)?;
            len += string_len(&will.topic, "Will Topic")?;
            len += binary_len(&will.payload, "Will Payload")?;
        }
        if let Some(username) = &self.username {
            len += string_len(username, "User Name")?;
        }
        if let Some(password) = &self.password {
            len += binary_len(password, "Password")?;
        }
        Ok(len)
    }

    fn write_body(&self, dst: &mut BytesMut) {
        put_string(dst, PROTOCOL_NAME);
        dst.put_u8(PROTOCOL_VERSION);
        dst.put_u8(self.flags());
        dst.put_u16(self.keep_alive);
        put_properties(dst, &self.properties);
        put_string(dst, &self.client_id);
        if let Some(will) = &self.will {
            put_properties(dst, &will.properties);
            put_string(dst, &will.topic);
            put_binary(dst, &will.payload);
        }
        if let Some(username) = &self.username {
            put_string(dst, username);
        }
        if let Some(password) = &self.password {
            put_binary(dst, password);
        }
    }
}

encode_methods!(Connect);

impl ConnectProperties {
    /// Reads the CONNECT properties.
    fn decode(reader: &mut Reader<'_>) -> Result<Self, Error> {
        use PropertyId as P;
        let context = PropertyContext::Connect;
        let mut properties = Self::default();
        read_properties(reader, context, |id, reader| match id {
            P::SessionExpiryInterval => once(
                &mut properties.session_expiry_interval,
                read_u32(reader, id)?,
                id,
            ),
            P::ReceiveMaximum => once(
                &mut properties.receive_maximum,
                read_nonzero_u16(reader, id)?,
                id,
            ),
            P::MaximumPacketSize => once(
                &mut properties.maximum_packet_size,
                read_nonzero_u32(reader, id)?,
                id,
            ),
            P::TopicAliasMaximum => once(
                &mut properties.topic_alias_maximum,
                read_u16(reader, id)?,
                id,
            ),
            P::RequestResponseInformation => once(
                &mut properties.request_response_information,
                read_bool(reader, id)?,
                id,
            ),
            P::RequestProblemInformation => once(
                &mut properties.request_problem_information,
                read_bool(reader, id)?,
                id,
            ),
            P::UserProperty => {
                properties
                    .user_properties
                    .push(reader.string_pair(id.name())?);
                Ok(())
            }
            P::AuthenticationMethod => once(
                &mut properties.authentication_method,
                reader.string(id.name())?,
                id,
            ),
            P::AuthenticationData => once(
                &mut properties.authentication_data,
                reader.binary(id.name())?,
                id,
            ),
            _ => Err(unexpected(id, context)),
        })?;
        properties.check()?;
        Ok(properties)
    }

    /// Authentication Data without an Authentication Method is a Protocol Error (section
    /// 3.1.2.11.10).
    fn check(&self) -> Result<(), Error> {
        if self.authentication_data.is_some() && self.authentication_method.is_none() {
            return Err(Error::MissingAuthenticationMethod {
                packet_type: PacketType::Connect,
            });
        }
        Ok(())
    }
}

impl Properties for ConnectProperties {
    fn for_each(&self, mut f: impl FnMut(PropertyId, Value<'_>)) {
        use PropertyId as P;
        if let Some(value) = self.session_expiry_interval {
            f(P::SessionExpiryInterval, Value::FourByteInteger(value));
        }
        if let Some(value) = self.receive_maximum {
            f(P::ReceiveMaximum, Value::TwoByteInteger(value.get()));
        }
        if let Some(value) = self.maximum_packet_size {
            f(P::MaximumPacketSize, Value::FourByteInteger(value.get()));
        }
        if let Some(value) = self.topic_alias_maximum {
            f(P::TopicAliasMaximum, Value::TwoByteInteger(value));
        }
        if let Some(value) = self.request_response_information {
            f(P::RequestResponseInformation, Value::Byte(value.into()));
        }
        if let Some(value) = self.request_problem_information {
            f(P::RequestProblemInformation, Value::Byte(value.into()));
        }
        for (name, value) in &self.user_properties {
            f(P::UserProperty, Value::Pair(name, value));
        }
        if let Some(value) = &self.authentication_method {
            f(P::AuthenticationMethod, Value::String(value));
        }
        if let Some(value) = &self.authentication_data {
            f(P::AuthenticationData, Value::Binary(value));
        }
    }
}

impl WillProperties {
    /// Reads the Will Properties.
    fn decode(reader: &mut Reader<'_>) -> Result<Self, Error> {
        use PropertyId as P;
        let context = PropertyContext::Will;
        let mut properties = Self::default();
        read_properties(reader, context, |id, reader| match id {
            P::WillDelayInterval => once(
                &mut properties.will_delay_interval,
                read_u32(reader, id)?,
                id,
            ),
            P::PayloadFormatIndicator => once(
                &mut properties.payload_format_indicator,
                read_payload_format(reader, id)?,
                id,
            ),
            P::MessageExpiryInterval => once(
                &mut properties.message_expiry_interval,
                read_u32(reader, id)?,
                id,
            ),
            P::ContentType => once(&mut properties.content_type, reader.string(id.name())?, id),
            P::ResponseTopic => once(
                &mut properties.response_topic,
                reader.string(id.name())?,
                id,
            ),
            P::CorrelationData => once(
                &mut properties.correlation_data,
                reader.binary(id.name())?,
                id,
            ),
            P::UserProperty => {
                properties
                    .user_properties
                    .push(reader.string_pair(id.name())?);
                Ok(())
            }
            _ => Err(unexpected(id, context)),
        })?;
        Ok(properties)
    }
}

impl Properties for WillProperties {
    fn for_each(&self, mut f: impl FnMut(PropertyId, Value<'_>)) {
        use PropertyId as P;
        if let Some(value) = self.will_delay_interval {
            f(P::WillDelayInterval, Value::FourByteInteger(value));
        }
        if let Some(value) = self.payload_format_indicator {
            f(P::PayloadFormatIndicator, Value::Byte(value.value()));
        }
        if let Some(value) = self.message_expiry_interval {
            f(P::MessageExpiryInterval, Value::FourByteInteger(value));
        }
        if let Some(value) = &self.content_type {
            f(P::ContentType, Value::String(value));
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
    }
}

/// How a server answers a CONNECT that is not MQTT 5.0, reported by decoding as
/// [`Error::UnsupportedProtocol`], before it closes the connection.
///
/// ADR 0001 refuses every such CONNECT with CONNACK reason code 0x84 and leaves the exact
/// bytes an older client receives to report R1. **Until R1 settles them, the session sends
/// [`ProtocolRefusal::ConnAckV5`] whatever the Protocol Name and Version, then closes.** The
/// other two exist so that R1 can change the answer without changing the codec: a 3.1.1
/// client cannot parse the MQTT 5.0 CONNACK, whose Remaining Length is 3 where it expects 2,
/// and MQTT 3.1, which names the protocol `"MQIsdp"`, may be closed on without a CONNACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProtocolRefusal {
    /// The MQTT 5.0 CONNACK with reason code 0x84 (Unsupported Protocol Version) and no
    /// properties, `20 03 00 84 00`, which [MQTT-3.1.2-1] and [MQTT-3.1.2-2] name.
    ConnAckV5,
    /// The MQTT 3.1.1 CONNACK with return code 0x01 (unacceptable protocol level),
    /// `20 02 00 01`, which a 3.1.1 server sends for a level it does not support and a 3.1.1
    /// client can parse. MQTT 3.1 uses the same bytes for an unacceptable protocol version.
    ConnAckV311,
    /// No CONNACK: close the connection. [MQTT-3.1.2-1] allows it when the Protocol Name is
    /// not `"MQTT"`, as with MQTT 3.1's `"MQIsdp"`.
    Close,
}

impl ProtocolRefusal {
    /// The bytes to send before closing the connection: none for [`ProtocolRefusal::Close`].
    pub const fn bytes(self) -> &'static [u8] {
        match self {
            Self::ConnAckV5 => &[0x20, 0x03, 0x00, 0x84, 0x00],
            Self::ConnAckV311 => &[0x20, 0x02, 0x00, 0x01],
            Self::Close => &[],
        }
    }

    /// Appends [`bytes`](Self::bytes) to `dst`.
    pub fn encode(self, dst: &mut BytesMut) {
        dst.put_slice(self.bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{concat, encode_parts, prefixed, properties, sample};
    use crate::{ConnectReasonCode, DisconnectReasonCode};

    /// The variable header of Figure 3-6: protocol "MQTT" version 5; User Name, Password,
    /// Will QoS 1, Will Flag and Clean Start set; Keep Alive 10; Session Expiry Interval 10.
    const FIGURE_3_6: [u8; 16] = [
        0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0xCE, 0x00, 0x0A, 0x05, 0x11, 0x00, 0x00, 0x00,
        0x0A,
    ];

    /// Figure 3-6 followed by a payload with every field its flags call for.
    fn figure_3_6_packet() -> Bytes {
        concat(&[
            &FIGURE_3_6,
            &prefixed(b"client-1"),
            &properties(&[]),
            &prefixed(b"last/will"),
            &prefixed(b"gone"),
            &prefixed(b"user"),
            &prefixed(&[0xDE, 0xAD]),
        ])
    }

    fn header(flags: u8) -> Vec<u8> {
        concat(&[&prefixed(b"MQTT"), &[0x05, flags, 0x00, 0x3C]]).to_vec()
    }

    #[test]
    fn mqtt_3_1_2_figure_3_6_decodes_and_encodes() {
        let body = figure_3_6_packet();
        let connect = Connect::decode(&body).unwrap();
        assert_eq!(
            connect,
            Connect {
                clean_start: true,
                keep_alive: 10,
                properties: ConnectProperties {
                    session_expiry_interval: Some(10),
                    ..ConnectProperties::default()
                },
                client_id: "client-1".into(),
                will: Some(Will {
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    properties: WillProperties::default(),
                    topic: "last/will".into(),
                    payload: Bytes::from_static(b"gone"),
                }),
                username: Some("user".into()),
                password: Some(Bytes::from_static(&[0xDE, 0xAD])),
            }
        );
        // [MQTT-3.1.3-1]: written back in the same order.
        assert_eq!(encode_parts(&connect), (0x10, body));
    }

    #[test]
    fn a_minimal_connect_is_fifteen_bytes() {
        let connect = Connect {
            clean_start: true,
            ..Connect::default()
        };
        let mut dst = BytesMut::new();
        connect.encode(&mut dst).unwrap();
        assert_eq!(
            dst[..],
            [
                0x10, 0x0D, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x00, 0x00, 0x00,
                0x00
            ]
        );
        assert_eq!(connect.encoded_len(), Ok(15));
        assert_eq!(
            Connect::decode(&Bytes::copy_from_slice(&dst[2..])),
            Ok(connect)
        );
    }

    #[test]
    fn mqtt_3_1_2_1_a_protocol_name_other_than_mqtt_is_unsupported() {
        // MQTT 3.1's CONNECT names the protocol "MQIsdp" at version 3.
        let body = concat(&[&prefixed(b"MQIsdp"), &[0x03, 0x02, 0x00, 0x3C]]);
        let error = Connect::decode(&body).unwrap_err();
        assert_eq!(
            error,
            Error::UnsupportedProtocol {
                name: "MQIsdp".into(),
                level: 3
            }
        );
        assert_eq!(
            error.connack_reason_code(),
            ConnectReasonCode::UnsupportedProtocolVersion
        );
        let body = concat(&[&prefixed(b"MQTT5"), &[0x05]]);
        assert!(matches!(
            Connect::decode(&body),
            Err(Error::UnsupportedProtocol { level: 5, .. })
        ));
    }

    #[test]
    fn mqtt_3_1_2_1_any_name_but_mqtt_is_another_protocol_whatever_its_bytes() {
        // Not UTF-8, and containing U+0000: still a Protocol Name that is not "MQTT", never a
        // malformed string.
        let cases: [(&[u8], &str); 4] = [
            (&[0xFF, 0xFE], "\u{FFFD}\u{FFFD}"),
            (b"MQ\0T", "MQ\0T"),
            (b"mqtt", "mqtt"),
            (b"", ""),
        ];
        for (raw, name) in cases {
            let body = concat(&[&prefixed(raw), &[0x05, 0x02, 0x00, 0x3C, 0x00, 0x00, 0x00]]);
            let error = Connect::decode(&body).unwrap_err();
            assert_eq!(
                error,
                Error::UnsupportedProtocol {
                    name: name.into(),
                    level: 5
                },
                "{raw:02X?}"
            );
            assert_eq!(
                error.connack_reason_code(),
                ConnectReasonCode::UnsupportedProtocolVersion
            );
        }
    }

    #[test]
    fn mqtt_3_1_2_2_a_protocol_version_other_than_5_is_unsupported() {
        // 4 is MQTT 3.1.1; 0x85 is version 5 with the bridge bit some brokers define.
        for level in [0, 3, 4, 6, 0x85, 0xFF] {
            let body = concat(&[&prefixed(b"MQTT"), &[level, 0x02, 0x00, 0x3C, 0x00]]);
            assert_eq!(
                Connect::decode(&body),
                Err(Error::UnsupportedProtocol {
                    name: "MQTT".into(),
                    level
                }),
                "level {level}"
            );
        }
    }

    #[test]
    fn the_rest_of_an_unsupported_connect_is_not_read() {
        // A 3.1.1 CONNECT has no properties, so read as MQTT 5.0 its Client Identifier length
        // would be taken for a Property Length. Only the version is reported.
        let body = concat(&[&prefixed(b"MQTT"), &[0x04, 0xFF], &prefixed(b"client")]);
        assert_eq!(
            Connect::decode(&body),
            Err(Error::UnsupportedProtocol {
                name: "MQTT".into(),
                level: 4
            })
        );
    }

    #[test]
    fn mqtt_3_1_2_3_the_reserved_flag_must_be_zero() {
        let body = concat(&[&header(0x03), &properties(&[]), &prefixed(b"c")]);
        let error = Connect::decode(&body).unwrap_err();
        assert_eq!(error, Error::InvalidConnectFlags { flags: 0x03 });
        assert_eq!(
            error.connack_reason_code(),
            ConnectReasonCode::MalformedPacket
        );
    }

    #[test]
    fn mqtt_3_1_2_11_and_mqtt_3_1_2_13_will_qos_and_retain_need_the_will_flag() {
        // Will QoS 1, Will QoS 2 and Will Retain, each without the Will Flag.
        for flags in [0x08, 0x10, 0x20, 0x28] {
            let body = concat(&[&header(flags), &properties(&[]), &prefixed(b"c")]);
            assert_eq!(
                Connect::decode(&body),
                Err(Error::InvalidConnectFlags { flags }),
                "{flags:#04x}"
            );
        }
    }

    #[test]
    fn mqtt_3_1_2_12_will_qos_3_is_malformed() {
        let body = concat(&[
            &header(0x1C),
            &properties(&[]),
            &prefixed(b"c"),
            &properties(&[]),
            &prefixed(b"t"),
            &prefixed(b"p"),
        ]);
        assert_eq!(
            Connect::decode(&body),
            Err(Error::InvalidConnectFlags { flags: 0x1C })
        );
    }

    #[test]
    fn mqtt_3_1_2_12_mqtt_3_1_2_14_and_mqtt_3_1_2_15_will_qos_and_retain_are_kept() {
        for (flags, qos, retain) in [
            (0x04, QoS::AtMostOnce, false),
            (0x0C, QoS::AtLeastOnce, false),
            (0x14, QoS::ExactlyOnce, false),
            (0x34, QoS::ExactlyOnce, true),
        ] {
            let body = concat(&[
                &header(flags),
                &properties(&[]),
                &prefixed(b"c"),
                &properties(&[]),
                &prefixed(b"t"),
                &prefixed(b"p"),
            ]);
            let connect = Connect::decode(&body).unwrap();
            let will = connect.will.as_ref().unwrap();
            assert_eq!((will.qos, will.retain), (qos, retain), "{flags:#04x}");
            assert_eq!(encode_parts(&connect).1, body);
        }
    }

    #[test]
    fn mqtt_3_1_2_9_the_will_fields_must_be_present() {
        let start = [header(0x04), properties(&[]), prefixed(b"c")].concat();
        let cases: [(&[&[u8]], &str); 3] = [
            (&[&start], "Property Length"),
            (&[&start, &properties(&[])], "Will Topic"),
            (&[&start, &properties(&[]), &prefixed(b"t")], "Will Payload"),
        ];
        for (parts, field) in cases {
            assert_eq!(
                Connect::decode(&concat(parts)),
                Err(Error::Truncated { field }),
                "{field}"
            );
        }
    }

    #[test]
    fn mqtt_3_1_2_16_and_mqtt_3_1_2_17_a_user_name_is_present_exactly_when_flagged() {
        let missing = concat(&[&header(0x80), &properties(&[]), &prefixed(b"c")]);
        assert_eq!(
            Connect::decode(&missing),
            Err(Error::Truncated { field: "User Name" })
        );
        let unflagged = concat(&[
            &header(0x00),
            &properties(&[]),
            &prefixed(b"c"),
            &prefixed(b"u"),
        ]);
        assert_eq!(
            Connect::decode(&unflagged),
            Err(Error::TrailingBytes {
                packet_type: PacketType::Connect,
                count: 3
            })
        );
    }

    #[test]
    fn mqtt_3_1_2_18_and_mqtt_3_1_2_19_a_password_is_present_exactly_when_flagged() {
        let missing = concat(&[
            &header(0xC0),
            &properties(&[]),
            &prefixed(b"c"),
            &prefixed(b"u"),
        ]);
        assert_eq!(
            Connect::decode(&missing),
            Err(Error::Truncated { field: "Password" })
        );
        let unflagged = concat(&[
            &header(0x80),
            &properties(&[]),
            &prefixed(b"c"),
            &prefixed(b"u"),
            &prefixed(b"p"),
        ]);
        assert!(matches!(
            Connect::decode(&unflagged),
            Err(Error::TrailingBytes { .. })
        ));
    }

    #[test]
    fn section_3_1_2_9_a_password_may_come_without_a_user_name() {
        let body = concat(&[
            &header(0x40),
            &properties(&[]),
            &prefixed(b"c"),
            &prefixed(b"token"),
        ]);
        let connect = Connect::decode(&body).unwrap();
        assert_eq!(connect.username, None);
        assert_eq!(connect.password.as_deref(), Some(&b"token"[..]));
        assert_eq!(encode_parts(&connect).1, body);
    }

    #[test]
    fn mqtt_3_1_3_3_the_client_identifier_must_be_present() {
        let body = concat(&[&header(0x02), &properties(&[])]);
        assert_eq!(
            Connect::decode(&body),
            Err(Error::Truncated {
                field: "Client Identifier"
            })
        );
    }

    #[test]
    fn mqtt_3_1_3_6_an_empty_client_identifier_decodes() {
        let body = concat(&[&header(0x02), &properties(&[]), &prefixed(b"")]);
        assert_eq!(Connect::decode(&body).unwrap().client_id, "");
    }

    #[test]
    fn mqtt_3_1_3_4_mqtt_3_1_3_11_and_mqtt_3_1_3_12_text_fields_are_utf8() {
        let bad = prefixed(&[0xC0, 0x80]);
        let cases = [
            (
                concat(&[&header(0x00), &properties(&[]), &bad]),
                "Client Identifier",
            ),
            (
                concat(&[
                    &header(0x04),
                    &properties(&[]),
                    &prefixed(b"c"),
                    &properties(&[]),
                    &bad,
                ]),
                "Will Topic",
            ),
            (
                concat(&[&header(0x80), &properties(&[]), &prefixed(b"c"), &bad]),
                "User Name",
            ),
        ];
        for (body, field) in cases {
            assert_eq!(
                Connect::decode(&body),
                Err(Error::InvalidUtf8 { field }),
                "{field}"
            );
        }
    }

    #[test]
    fn connect_properties_round_trip() {
        let connect = Connect {
            clean_start: false,
            keep_alive: 600,
            properties: ConnectProperties {
                session_expiry_interval: Some(u32::MAX),
                receive_maximum: NonZeroU16::new(32),
                maximum_packet_size: NonZeroU32::new(1 << 20),
                topic_alias_maximum: Some(10),
                request_response_information: Some(true),
                request_problem_information: Some(false),
                user_properties: vec![("b".into(), "2".into()), ("a".into(), "1".into())],
                authentication_method: Some("SCRAM-SHA-256".into()),
                authentication_data: Some(Bytes::from_static(b"client-first")),
            },
            client_id: "device-7".into(),
            will: Some(Will {
                qos: QoS::ExactlyOnce,
                retain: true,
                properties: WillProperties {
                    will_delay_interval: Some(30),
                    payload_format_indicator: Some(PayloadFormat::Utf8),
                    message_expiry_interval: Some(3600),
                    content_type: Some("text/plain".into()),
                    response_topic: Some("replies/device-7".into()),
                    correlation_data: Some(Bytes::from_static(&[1, 2, 3])),
                    user_properties: vec![("z".into(), "26".into()), ("y".into(), "25".into())],
                },
                topic: "devices/device-7/status".into(),
                payload: Bytes::from_static(b"offline"),
            }),
            username: Some("device-7".into()),
            password: Some(Bytes::from_static(b"secret")),
        };
        let (first, body) = encode_parts(&connect);
        assert_eq!(first, 0x10);
        // [MQTT-3.1.3-10]: User Properties keep their order.
        assert_eq!(Connect::decode(&body), Ok(connect));
    }

    /// A CONNECT carrying `connect` as its properties and, when given, `will` as its Will
    /// Properties.
    fn with_properties(connect: &[&[u8]], will: Option<&[&[u8]]>) -> Bytes {
        let flags = if will.is_some() { 0x06 } else { 0x02 };
        let mut parts = vec![header(flags), properties(connect), prefixed(b"c")];
        if let Some(will) = will {
            parts.extend([properties(will), prefixed(b"t"), prefixed(b"p")]);
        }
        Bytes::from(parts.concat())
    }

    #[test]
    fn section_2_2_2_2_properties_table_2_4_does_not_allow_are_malformed() {
        // Authentication Data needs the Authentication Method beside it.
        let method = sample(PropertyId::AuthenticationMethod);
        for id in PropertyId::ALL {
            let property = sample(id);
            let result = if id == PropertyId::AuthenticationData {
                Connect::decode(&with_properties(&[&method, &property], None))
            } else {
                Connect::decode(&with_properties(&[&property], None))
            };
            if id.is_valid_in(PropertyContext::Connect) {
                assert!(result.is_ok(), "{id} in CONNECT: {result:?}");
            } else {
                assert_eq!(
                    result,
                    Err(Error::InvalidPropertyId {
                        context: PropertyContext::Connect,
                        id: id.value().into()
                    })
                );
            }
            let result = Connect::decode(&with_properties(&[], Some(&[&property])));
            if id.is_valid_in(PropertyContext::Will) {
                assert!(result.is_ok(), "{id} in Will Properties: {result:?}");
            } else {
                assert_eq!(
                    result,
                    Err(Error::InvalidPropertyId {
                        context: PropertyContext::Will,
                        id: id.value().into()
                    })
                );
            }
        }
        // An identifier Table 2-4 does not define, and one written in two bytes.
        let error = Connect::decode(&with_properties(&[&[0x7F, 0x00]], None)).unwrap_err();
        assert_eq!(
            error.connack_reason_code(),
            ConnectReasonCode::MalformedPacket
        );
        assert_eq!(
            Connect::decode(&with_properties(&[&[0x80, 0x01, 0x00]], None)),
            Err(Error::InvalidPropertyId {
                context: PropertyContext::Connect,
                id: 0x80
            })
        );
        assert_eq!(
            Connect::decode(&with_properties(
                &[&[0x91, 0x00, 0x00, 0x00, 0x00, 0x0A]],
                None
            )),
            Err(Error::MalformedVariableByteInteger {
                field: "Property Identifier"
            })
        );
    }

    #[test]
    fn section_3_1_2_11_and_3_1_3_2_properties_other_than_user_property_appear_once() {
        for id in PropertyId::ALL {
            if id == PropertyId::UserProperty {
                continue;
            }
            let property = sample(id);
            let twice: [&[u8]; 2] = [&property, &property];
            if id.is_valid_in(PropertyContext::Connect) {
                let error = Connect::decode(&with_properties(&twice, None)).unwrap_err();
                assert_eq!(error, Error::DuplicateProperty { property: id });
                assert_eq!(
                    error.connack_reason_code(),
                    ConnectReasonCode::ProtocolError
                );
            }
            if id.is_valid_in(PropertyContext::Will) {
                assert_eq!(
                    Connect::decode(&with_properties(&[], Some(&twice))),
                    Err(Error::DuplicateProperty { property: id })
                );
            }
        }
        let user = sample(PropertyId::UserProperty);
        let connect =
            Connect::decode(&with_properties(&[&user, &user], Some(&[&user, &user]))).unwrap();
        assert_eq!(connect.properties.user_properties.len(), 2);
        assert_eq!(connect.will.unwrap().properties.user_properties.len(), 2);
    }

    #[test]
    fn section_3_1_2_11_values_the_connect_properties_do_not_allow() {
        let cases: [(&[u8], PropertyId, u32); 6] = [
            (&[0x21, 0x00, 0x00], PropertyId::ReceiveMaximum, 0),
            (
                &[0x27, 0x00, 0x00, 0x00, 0x00],
                PropertyId::MaximumPacketSize,
                0,
            ),
            (&[0x19, 0x02], PropertyId::RequestResponseInformation, 2),
            (&[0x17, 0xFF], PropertyId::RequestProblemInformation, 255),
            (&[0x01, 0x02], PropertyId::PayloadFormatIndicator, 2),
            (&[0x01, 0x80], PropertyId::PayloadFormatIndicator, 128),
        ];
        for (property, id, value) in cases {
            let body = if id.is_valid_in(PropertyContext::Connect) {
                with_properties(&[property], None)
            } else {
                with_properties(&[], Some(&[property]))
            };
            let error = Connect::decode(&body).unwrap_err();
            assert_eq!(
                error,
                Error::InvalidPropertyValue {
                    property: id,
                    value
                }
            );
            assert_eq!(
                error.connack_reason_code(),
                ConnectReasonCode::ProtocolError
            );
        }
    }

    #[test]
    fn section_3_1_2_11_10_authentication_data_needs_a_method() {
        let data = sample(PropertyId::AuthenticationData);
        let error = Connect::decode(&with_properties(&[&data], None)).unwrap_err();
        assert_eq!(
            error,
            Error::MissingAuthenticationMethod {
                packet_type: PacketType::Connect
            }
        );
        assert_eq!(
            error.connack_reason_code(),
            ConnectReasonCode::ProtocolError
        );

        let connect = Connect {
            properties: ConnectProperties {
                authentication_data: Some(Bytes::from_static(b"data")),
                ..ConnectProperties::default()
            },
            ..Connect::default()
        };
        assert_eq!(connect.encoded_len(), Err(error));
    }

    #[test]
    fn properties_must_fit_their_property_length() {
        // A property running past its Property Length, then a Property Length running past
        // the packet.
        let body = concat(&[&header(0x02), &[0x03, 0x11, 0x00, 0x00], &prefixed(b"c")]);
        assert_eq!(
            Connect::decode(&body),
            Err(Error::Truncated {
                field: "Session Expiry Interval"
            })
        );
        let body = concat(&[&header(0x02), &[0x09]]);
        assert_eq!(
            Connect::decode(&body),
            Err(Error::Truncated {
                field: "Properties"
            })
        );
    }

    #[test]
    fn encoding_refuses_what_decoding_refuses() {
        let null = Connect {
            client_id: "a\0b".into(),
            ..Connect::default()
        };
        assert_eq!(
            null.encoded_len(),
            Err(Error::NullCharacter {
                field: "Client Identifier"
            })
        );
        let long = Connect {
            username: Some("u".repeat(65_536)),
            ..Connect::default()
        };
        let mut dst = BytesMut::from(&b"kept"[..]);
        assert_eq!(
            long.encode(&mut dst),
            Err(Error::TooLong {
                field: "User Name",
                len: 65_536
            })
        );
        // Nothing is written when encoding fails.
        assert_eq!(dst[..], b"kept"[..]);
        let will = Connect {
            will: Some(Will {
                properties: WillProperties {
                    user_properties: vec![("k".into(), "\0".into())],
                    ..WillProperties::default()
                },
                ..Will::default()
            }),
            ..Connect::default()
        };
        assert_eq!(
            will.encoded_len(),
            Err(Error::NullCharacter {
                field: "User Property"
            })
        );
    }

    #[test]
    fn mqtt_3_1_2_1_and_mqtt_3_1_2_2_refusals() {
        // The MQTT 5.0 refusal is an ordinary CONNACK with reason code 0x84.
        let connack = [0x20, 0x03, 0x00, 0x84, 0x00];
        assert_eq!(ProtocolRefusal::ConnAckV5.bytes(), connack);
        assert_eq!(
            ProtocolRefusal::ConnAckV311.bytes(),
            [0x20, 0x02, 0x00, 0x01]
        );
        assert_eq!(ProtocolRefusal::Close.bytes(), b"");
        let mut dst = BytesMut::new();
        ProtocolRefusal::ConnAckV311.encode(&mut dst);
        assert_eq!(dst[..], [0x20, 0x02, 0x00, 0x01]);
    }

    #[test]
    fn errors_after_connack_map_to_disconnect_codes() {
        let error = Error::UnsupportedProtocol {
            name: "MQTT".into(),
            level: 4,
        };
        assert_eq!(
            error.disconnect_reason_code(),
            DisconnectReasonCode::ProtocolError
        );
    }
}
