//! [`Packet`]: any of the fifteen control packets, and what applies to all of them.

use bytes::{Bytes, BytesMut};

use crate::encode::{Encode, encode_methods, exceeds};
use crate::publish::publish_qos;
use crate::{
    Auth, ConnAck, Connect, Disconnect, Error, PacketType, PropertyId, PubAck, PubComp, PubRec,
    PubRel, Publish, Sender, SubAck, Subscribe, UnsubAck, Unsubscribe,
};

/// An MQTT 5.0 control packet (section 2).
///
/// CONNECT and CONNACK are boxed: each comes once per connection and is far larger than the
/// packets that come all the time, which would otherwise make every `Packet` their size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    /// CONNECT (section 3.1).
    Connect(Box<Connect>),
    /// CONNACK (section 3.2).
    ConnAck(Box<ConnAck>),
    /// PUBLISH (section 3.3).
    Publish(Publish),
    /// PUBACK (section 3.4).
    PubAck(PubAck),
    /// PUBREC (section 3.5).
    PubRec(PubRec),
    /// PUBREL (section 3.6).
    PubRel(PubRel),
    /// PUBCOMP (section 3.7).
    PubComp(PubComp),
    /// SUBSCRIBE (section 3.8).
    Subscribe(Subscribe),
    /// SUBACK (section 3.9).
    SubAck(SubAck),
    /// UNSUBSCRIBE (section 3.10).
    Unsubscribe(Unsubscribe),
    /// UNSUBACK (section 3.11).
    UnsubAck(UnsubAck),
    /// PINGREQ, which has no variable header or payload (section 3.12).
    PingReq,
    /// PINGRESP, which has no variable header or payload (section 3.13).
    PingResp,
    /// DISCONNECT (section 3.14).
    Disconnect(Disconnect),
    /// AUTH (section 3.15).
    Auth(Auth),
}

/// PINGREQ and PINGRESP: a fixed header and nothing else.
struct Ping(PacketType);

impl Encode for Ping {
    fn first_byte(&self) -> u8 {
        self.0.value() << 4
    }

    fn remaining_length(&self) -> Result<usize, Error> {
        Ok(0)
    }

    fn write_body(&self, _: &mut BytesMut) {}
}

const PING_REQ: Ping = Ping(PacketType::PingReq);
const PING_RESP: Ping = Ping(PacketType::PingResp);

/// Checks the low four bits of a first byte against Table 2-2 ([MQTT-2.1.3-1]), and for
/// PUBLISH against its QoS rules ([MQTT-3.3.1-2], [MQTT-3.3.1-4]). Any other value is a
/// Malformed Packet, which [MQTT-3.6.1-1], [MQTT-3.8.1-1], [MQTT-3.10.1-1], [MQTT-3.14.1-1]
/// and [MQTT-3.15.1-1] repeat for their packets.
pub(crate) fn check_flags(packet_type: PacketType, flags: u8) -> Result<(), Error> {
    match packet_type.fixed_header_flags() {
        Some(required) if flags == required => Ok(()),
        Some(_) => Err(Error::InvalidFlags { packet_type, flags }),
        None => publish_qos(flags).map(drop),
    }
}

impl Packet {
    /// The packet's type.
    pub const fn packet_type(&self) -> PacketType {
        match self {
            Self::Connect(_) => PacketType::Connect,
            Self::ConnAck(_) => PacketType::ConnAck,
            Self::Publish(_) => PacketType::Publish,
            Self::PubAck(_) => PacketType::PubAck,
            Self::PubRec(_) => PacketType::PubRec,
            Self::PubRel(_) => PacketType::PubRel,
            Self::PubComp(_) => PacketType::PubComp,
            Self::Subscribe(_) => PacketType::Subscribe,
            Self::SubAck(_) => PacketType::SubAck,
            Self::Unsubscribe(_) => PacketType::Unsubscribe,
            Self::UnsubAck(_) => PacketType::UnsubAck,
            Self::PingReq => PacketType::PingReq,
            Self::PingResp => PacketType::PingResp,
            Self::Disconnect(_) => PacketType::Disconnect,
            Self::Auth(_) => PacketType::Auth,
        }
    }

    /// Checks the rules that depend on which end sent the packet, so a server can hold a
    /// client to them and a client a server. Each is a Protocol Error:
    ///
    /// - the direction of flow of each packet type (Table 2-1), so a server refuses a
    ///   CONNACK, SUBACK, UNSUBACK or PINGRESP;
    /// - reason codes only one end sends: 0x10 in PUBACK and PUBREC (Tables 3-4 and 3-5), and
    ///   the Sent by columns of DISCONNECT and AUTH (Tables 3-10 and 3-11);
    /// - a Subscription Identifier in a PUBLISH from a client ([MQTT-3.3.4-6]), and a Session
    ///   Expiry Interval in a DISCONNECT from a server ([MQTT-3.14.2-2]).
    ///
    /// A [`Decoder`](crate::Decoder) told the sender applies this to every packet it decodes.
    ///
    /// # Errors
    ///
    /// [`Error::NotSentBy`], [`Error::ReasonCodeNotSentBy`] or [`Error::PropertyNotSentBy`],
    /// for the first rule the packet breaks.
    pub fn check_sender(&self, sender: Sender) -> Result<(), Error> {
        let packet_type = self.packet_type();
        if !packet_type.is_sent_by(sender) {
            return Err(Error::NotSentBy {
                sender,
                packet_type,
            });
        }
        let reason_code = match self {
            Self::PubAck(ack) => {
                Some((ack.reason_code.value(), ack.reason_code.is_sent_by(sender)))
            }
            Self::PubRec(ack) => {
                Some((ack.reason_code.value(), ack.reason_code.is_sent_by(sender)))
            }
            Self::Disconnect(disconnect) => Some((
                disconnect.reason_code.value(),
                disconnect.reason_code.is_sent_by(sender),
            )),
            Self::Auth(auth) => Some((
                auth.reason_code.value(),
                auth.reason_code.is_sent_by(sender),
            )),
            _ => None,
        };
        if let Some((code, false)) = reason_code {
            return Err(Error::ReasonCodeNotSentBy {
                sender,
                packet_type,
                code,
            });
        }
        let property = match (self, sender) {
            (Self::Publish(publish), Sender::Client)
                if !publish.properties.subscription_identifiers.is_empty() =>
            {
                Some(PropertyId::SubscriptionIdentifier)
            }
            (Self::Disconnect(disconnect), Sender::Server)
                if disconnect.properties.session_expiry_interval.is_some() =>
            {
                Some(PropertyId::SessionExpiryInterval)
            }
            _ => None,
        };
        match property {
            Some(property) => Err(Error::PropertyNotSentBy {
                sender,
                packet_type,
                property,
            }),
            None => Ok(()),
        }
    }

    /// Decodes the packet after a fixed header of `packet_type` and `flags`, already checked by
    /// [`check_flags`], from `body`, its variable header and payload.
    pub(crate) fn decode(packet_type: PacketType, flags: u8, body: &Bytes) -> Result<Self, Error> {
        Ok(match packet_type {
            PacketType::Connect => Self::Connect(Box::new(Connect::decode(body)?)),
            PacketType::ConnAck => Self::ConnAck(Box::new(ConnAck::decode(body)?)),
            PacketType::Publish => Self::Publish(Publish::decode(flags, body)?),
            PacketType::PubAck => Self::PubAck(PubAck::decode(body)?),
            PacketType::PubRec => Self::PubRec(PubRec::decode(body)?),
            PacketType::PubRel => Self::PubRel(PubRel::decode(body)?),
            PacketType::PubComp => Self::PubComp(PubComp::decode(body)?),
            PacketType::Subscribe => Self::Subscribe(Subscribe::decode(body)?),
            PacketType::SubAck => Self::SubAck(SubAck::decode(body)?),
            PacketType::Unsubscribe => Self::Unsubscribe(Unsubscribe::decode(body)?),
            PacketType::UnsubAck => Self::UnsubAck(UnsubAck::decode(body)?),
            PacketType::PingReq | PacketType::PingResp => {
                // Neither has a variable header or a payload (sections 3.12 and 3.13).
                if !body.is_empty() {
                    return Err(Error::TrailingBytes {
                        packet_type,
                        count: body.len(),
                    });
                }
                if packet_type == PacketType::PingReq {
                    Self::PingReq
                } else {
                    Self::PingResp
                }
            }
            PacketType::Disconnect => Self::Disconnect(Disconnect::decode(body)?),
            PacketType::Auth => Self::Auth(Auth::decode(body)?),
        })
    }

    /// The packet, as the encoder sees it.
    fn as_encode(&self) -> &dyn Encode {
        match self {
            Self::Connect(packet) => &**packet,
            Self::ConnAck(packet) => &**packet,
            Self::Publish(packet) => packet,
            Self::PubAck(packet) => packet,
            Self::PubRec(packet) => packet,
            Self::PubRel(packet) => packet,
            Self::PubComp(packet) => packet,
            Self::Subscribe(packet) => packet,
            Self::SubAck(packet) => packet,
            Self::Unsubscribe(packet) => packet,
            Self::UnsubAck(packet) => packet,
            Self::PingReq => &PING_REQ,
            Self::PingResp => &PING_RESP,
            Self::Disconnect(packet) => packet,
            Self::Auth(packet) => packet,
        }
    }
}

impl Encode for Packet {
    fn first_byte(&self) -> u8 {
        self.as_encode().first_byte()
    }

    fn remaining_length(&self) -> Result<usize, Error> {
        self.as_encode().remaining_length()
    }

    fn write_body(&self, dst: &mut BytesMut) {
        self.as_encode().write_body(dst);
    }
}

encode_methods!(Packet);

impl Packet {
    /// Makes the packet fit in `max_packet_size` bytes, the receiver's Maximum Packet Size, by
    /// dropping its Reason String and then its User Properties, and returns its size.
    ///
    /// Those two properties of a CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK,
    /// DISCONNECT or AUTH are diagnostics, and their sender must leave them out rather than
    /// exceed the receiver's limit ([MQTT-3.2.2-19], [MQTT-3.2.2-20], [MQTT-3.4.2-2],
    /// [MQTT-3.4.2-3], [MQTT-3.5.2-2], [MQTT-3.5.2-3], [MQTT-3.6.2-2], [MQTT-3.6.2-3],
    /// [MQTT-3.7.2-2], [MQTT-3.7.2-3], [MQTT-3.9.2-1], [MQTT-3.9.2-2], [MQTT-3.11.2-1],
    /// [MQTT-3.11.2-2], [MQTT-3.14.2-3], [MQTT-3.14.2-4], [MQTT-3.15.2-2], [MQTT-3.15.2-3]).
    /// Nothing is dropped from a packet that already fits, and nothing from other packet
    /// types: a PUBLISH that does not fit is discarded whole ([MQTT-3.1.2-25]).
    ///
    /// # Errors
    ///
    /// [`Error::PacketTooLarge`] when the packet does not fit even without them, and any
    /// error encoding the packet would return.
    pub fn fit_within(&mut self, max_packet_size: u32) -> Result<usize, Error> {
        for diagnostic in [Diagnostic::ReasonString, Diagnostic::UserProperties] {
            let size = self.encoded_len()?;
            if !exceeds(size, max_packet_size) {
                return Ok(size);
            }
            if !self.drop_diagnostic(diagnostic) {
                break;
            }
        }
        let size = self.encoded_len()?;
        if exceeds(size, max_packet_size) {
            return Err(Error::PacketTooLarge {
                size,
                maximum: max_packet_size,
            });
        }
        Ok(size)
    }

    /// Drops one diagnostic property from a packet whose sender drops them to fit, or
    /// returns false for a packet that has none to drop.
    fn drop_diagnostic(&mut self, diagnostic: Diagnostic) -> bool {
        let (reason_string, user_properties) = match self {
            Self::ConnAck(packet) => (
                &mut packet.properties.reason_string,
                &mut packet.properties.user_properties,
            ),
            Self::PubAck(PubAck { properties, .. })
            | Self::PubRec(PubRec { properties, .. })
            | Self::PubRel(PubRel { properties, .. })
            | Self::PubComp(PubComp { properties, .. })
            | Self::SubAck(SubAck { properties, .. })
            | Self::UnsubAck(UnsubAck { properties, .. }) => (
                &mut properties.reason_string,
                &mut properties.user_properties,
            ),
            Self::Disconnect(Disconnect { properties, .. }) => (
                &mut properties.reason_string,
                &mut properties.user_properties,
            ),
            Self::Auth(Auth { properties, .. }) => (
                &mut properties.reason_string,
                &mut properties.user_properties,
            ),
            Self::Connect(_)
            | Self::Publish(_)
            | Self::Subscribe(_)
            | Self::Unsubscribe(_)
            | Self::PingReq
            | Self::PingResp => return false,
        };
        match diagnostic {
            Diagnostic::ReasonString => *reason_string = None,
            Diagnostic::UserProperties => user_properties.clear(),
        }
        true
    }
}

/// What [`Packet::fit_within`] drops, in order.
#[derive(Clone, Copy)]
enum Diagnostic {
    /// The Reason String.
    ReasonString,
    /// Every User Property.
    UserProperties,
}

/// Wraps each packet struct into its [`Packet`].
macro_rules! into_packet {
    ($($name:ident => $wrap:expr),+ $(,)?) => {$(
        impl From<$name> for Packet {
            fn from(packet: $name) -> Self {
                $wrap(packet)
            }
        }
    )+};
}

into_packet! {
    Connect => |packet| Self::Connect(Box::new(packet)),
    ConnAck => |packet| Self::ConnAck(Box::new(packet)),
    Publish => Self::Publish,
    PubAck => Self::PubAck,
    PubRec => Self::PubRec,
    PubRel => Self::PubRel,
    PubComp => Self::PubComp,
    Subscribe => Self::Subscribe,
    SubAck => Self::SubAck,
    Unsubscribe => Self::Unsubscribe,
    UnsubAck => Self::UnsubAck,
    Disconnect => Self::Disconnect,
    Auth => Self::Auth,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AckProperties, ConnAckProperties, ConnectReasonCode, DisconnectProperties,
        DisconnectReasonCode, PacketId, PubAckReasonCode, PublishProperties,
    };

    fn unauthorized_puback() -> Packet {
        Packet::PubAck(PubAck {
            reason_code: PubAckReasonCode::NotAuthorized,
            properties: AckProperties {
                reason_string: Some("topic is outside the device namespace".into()),
                user_properties: vec![("rule".into(), "13".into())],
            },
            ..PubAck::new(PacketId::new(1).unwrap())
        })
    }

    #[test]
    fn mqtt_3_4_2_2_and_mqtt_3_4_2_3_diagnostics_are_dropped_to_fit() {
        let full = unauthorized_puback();
        let full_size = full.encoded_len().unwrap();

        // Room for everything: nothing is dropped.
        let mut packet = full.clone();
        assert_eq!(
            packet.fit_within(u32::try_from(full_size).unwrap()),
            Ok(full_size)
        );
        assert_eq!(packet, full);

        // Room for the User Property but not the Reason String: the Reason String goes.
        let mut packet = full.clone();
        let without_reason = full_size - (1 + 2 + 37);
        assert_eq!(
            packet.fit_within(u32::try_from(without_reason).unwrap()),
            Ok(without_reason)
        );
        let Packet::PubAck(ack) = &packet else {
            panic!("still a PUBACK");
        };
        assert_eq!(ack.properties.reason_string, None);
        assert_eq!(ack.properties.user_properties.len(), 1);

        // Room for neither: both go, and the reason code stays.
        let mut packet = full;
        assert_eq!(packet.fit_within(5), Ok(3 + 2));
        assert_eq!(
            packet,
            Packet::PubAck(PubAck {
                reason_code: PubAckReasonCode::NotAuthorized,
                ..PubAck::new(PacketId::new(1).unwrap())
            })
        );
        assert_eq!(
            packet.fit_within(4),
            Err(Error::PacketTooLarge {
                size: 5,
                maximum: 4
            })
        );
    }

    #[test]
    fn mqtt_3_2_2_19_and_mqtt_3_2_2_20_connack_diagnostics_are_dropped_to_fit() {
        let mut connack = Packet::from(ConnAck {
            reason_code: ConnectReasonCode::NotAuthorized,
            properties: ConnAckProperties {
                reason_string: Some("not on the list".into()),
                user_properties: vec![("a".into(), "b".into())],
                server_keep_alive: Some(30),
                ..ConnAckProperties::default()
            },
            ..ConnAck::default()
        });
        // Five bytes of CONNACK plus three of Server Keep Alive, which is not a diagnostic.
        assert_eq!(connack.fit_within(8), Ok(8));
        let Packet::ConnAck(connack) = connack else {
            panic!("still a CONNACK");
        };
        assert_eq!(connack.properties.server_keep_alive, Some(30));
        assert!(connack.properties.reason_string.is_none());
        assert!(connack.properties.user_properties.is_empty());
    }

    #[test]
    fn mqtt_3_14_2_3_and_mqtt_3_14_2_4_disconnect_diagnostics_are_dropped_to_fit() {
        let mut disconnect = Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::ServerShuttingDown,
            properties: DisconnectProperties {
                reason_string: Some("rolling upgrade".into()),
                user_properties: vec![("drain".into(), "true".into())],
                ..DisconnectProperties::default()
            },
        });
        assert_eq!(disconnect.fit_within(3), Ok(3));
        assert_eq!(
            disconnect,
            Packet::Disconnect(Disconnect {
                reason_code: DisconnectReasonCode::ServerShuttingDown,
                ..Disconnect::default()
            })
        );
    }

    #[test]
    fn mqtt_3_1_2_25_a_publish_is_never_trimmed() {
        let publish = Packet::Publish(Publish {
            topic: "t".into(),
            properties: PublishProperties {
                user_properties: vec![("k".into(), "v".into())],
                ..PublishProperties::default()
            },
            payload: Bytes::from_static(b"payload"),
            ..Publish::default()
        });
        let size = publish.encoded_len().unwrap();
        let mut packet = publish.clone();
        assert_eq!(
            packet.fit_within(10),
            Err(Error::PacketTooLarge { size, maximum: 10 })
        );
        assert_eq!(packet, publish);
    }
}
