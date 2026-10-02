//! The incremental decoder: packets out of a buffer that bytes arrive in.

use std::num::NonZeroU32;

use bytes::BytesMut;

use crate::encode::exceeds;
use crate::packet::check_flags;
use crate::primitives::decode_variable_byte_integer;
use crate::{Error, MAX_PACKET_SIZE, Packet, PacketType, Sender};

/// Reads packets from the front of a buffer as bytes arrive in it.
///
/// [`decode`](Self::decode) takes the buffer a connection reads into. While the buffer holds
/// less than a whole packet it returns `Ok(None)` and leaves the buffer untouched, so the
/// caller reads more and tries again. Once it holds a packet, it returns it and removes exactly
/// that packet's bytes from the front. The decoder keeps no state between calls.
///
/// ```
/// use bytes::BytesMut;
/// use openqtt_codec::{Decoder, Packet};
///
/// let decoder = Decoder::new();
/// let mut buffer = BytesMut::from(&[0xC0][..]);
/// assert_eq!(decoder.decode(&mut buffer), Ok(None));
/// buffer.extend_from_slice(&[0x00, 0xD0]);
/// assert_eq!(decoder.decode(&mut buffer), Ok(Some(Packet::PingReq)));
/// assert_eq!(&buffer[..], &[0xD0]);
/// ```
///
/// An error means the connection cannot go on: the stream cannot be read past a packet that
/// does not parse, and section 4.13 has the receiver close the connection, after the CONNACK
/// or DISCONNECT that [`Error::connack_reason_code`] and [`Error::disconnect_reason_code`]
/// name. An error found in the fixed header leaves the buffer untouched; one found after it
/// may have consumed the packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decoder {
    max_packet_size: u32,
    sender: Option<Sender>,
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder {
    /// A decoder that accepts any packet the protocol can express, from either end.
    pub const fn new() -> Self {
        Self {
            max_packet_size: MAX_PACKET_SIZE,
            sender: None,
        }
    }

    /// Refuses packets larger than `maximum` bytes, the Maximum Packet Size this end sent its
    /// peer ([MQTT-3.1.2-24], [MQTT-3.2.2-15]). The refusal comes as soon as the fixed header
    /// gives the size away, before the rest of the packet arrives.
    #[must_use]
    pub const fn with_max_packet_size(self, maximum: NonZeroU32) -> Self {
        Self {
            max_packet_size: maximum.get(),
            ..self
        }
    }

    /// Checks every packet against the rules for what `sender` may send, with
    /// [`Packet::check_sender`]. A server's decoder reads what clients send:
    /// `with_sender(Sender::Client)`.
    #[must_use]
    pub const fn with_sender(self, sender: Sender) -> Self {
        Self {
            sender: Some(sender),
            ..self
        }
    }

    /// The largest packet this decoder accepts, in bytes.
    pub const fn max_packet_size(&self) -> u32 {
        self.max_packet_size
    }

    /// The end whose packets this decoder reads, if it was told.
    pub const fn sender(&self) -> Option<Sender> {
        self.sender
    }

    /// Takes the next packet from the front of `src`.
    ///
    /// Returns `Ok(None)` without consuming anything when `src` holds less than a whole
    /// packet, and `Ok(Some(packet))` after consuming exactly the packet's bytes. Payloads and
    /// other Binary Data in the packet share `src`'s memory rather than copy it.
    ///
    /// # Errors
    ///
    /// Any [`Error`] the packet's bytes call for; see its reason code methods for what to
    /// send before closing the connection.
    pub fn decode(&self, src: &mut BytesMut) -> Result<Option<Packet>, Error> {
        let Some(&first) = src.first() else {
            return Ok(None);
        };
        // Type 0 is reserved and forbidden (Table 2-1).
        let packet_type = PacketType::from_u8(first >> 4).ok_or(Error::ReservedPacketType)?;
        let flags = first & 0x0F;
        check_flags(packet_type, flags)?;
        let Some((remaining_length, length_len)) =
            decode_variable_byte_integer(src.get(1..).unwrap_or_default(), "Remaining Length")?
        else {
            return Ok(None);
        };
        let header_len = 1 + length_len;
        // The packet size is the fixed header plus the Remaining Length (section 2.1.4).
        let size = usize::try_from(remaining_length)
            .map_or(usize::MAX, |remaining_length| header_len + remaining_length);
        if exceeds(size, self.max_packet_size) {
            return Err(Error::PacketTooLarge {
                size,
                maximum: self.max_packet_size,
            });
        }
        if src.len() < size {
            return Ok(None);
        }
        let frame = src.split_to(size).freeze();
        let packet = Packet::decode(packet_type, flags, &frame.slice(header_len..))?;
        if let Some(sender) = self.sender {
            packet.check_sender(sender)?;
        }
        Ok(Some(packet))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{
        AuthReasonCode, ConnectReasonCode, Disconnect, DisconnectProperties, DisconnectReasonCode,
        PacketId, PropertyId, PubAck, PubAckReasonCode, Publish, PublishProperties, QoS,
        SubscriptionId,
    };

    fn decode(bytes: &[u8]) -> Result<Option<Packet>, Error> {
        Decoder::new().decode(&mut BytesMut::from(bytes))
    }

    #[test]
    fn need_more_consumes_nothing() {
        // PUBLISH QoS 1 to "a/b", Packet Identifier 10, no properties, payload "hi".
        let packet = [
            0x32, 0x0A, 0x00, 0x03, b'a', b'/', b'b', 0x00, 0x0A, 0x00, b'h', b'i',
        ];
        for len in 0..packet.len() {
            let mut src = BytesMut::from(&packet[..len]);
            assert_eq!(Decoder::new().decode(&mut src), Ok(None), "{len} bytes");
            assert_eq!(src[..], packet[..len]);
        }
        let mut src = BytesMut::from(&packet[..]);
        assert!(Decoder::new().decode(&mut src).unwrap().is_some());
        assert!(src.is_empty());
    }

    #[test]
    fn exactly_one_packet_is_consumed_at_a_time() {
        let mut src = BytesMut::from(
            &[
                0x30, 0x05, 0x00, 0x01, b't', 0x00, b'x', // PUBLISH QoS 0 to "t", payload "x"
                0x40, 0x02, 0x00, 0x07, // PUBACK 7
                0xE0, // the start of a DISCONNECT
            ][..],
        );
        let start = src.as_ptr();
        let decoder = Decoder::new();
        let Some(Packet::Publish(publish)) = decoder.decode(&mut src).unwrap() else {
            panic!("a PUBLISH comes first");
        };
        assert_eq!(publish.topic, "t");
        // The payload is the byte it arrived in, not a copy of it.
        assert_eq!(publish.payload.as_ptr(), start.wrapping_add(6));
        assert_eq!(
            decoder.decode(&mut src),
            Ok(Some(Packet::PubAck(PubAck::new(PacketId::new(7).unwrap()))))
        );
        assert_eq!(decoder.decode(&mut src), Ok(None));
        assert_eq!(src[..], [0xE0]);
        src.extend_from_slice(&[0x00]);
        assert_eq!(
            decoder.decode(&mut src),
            Ok(Some(Packet::Disconnect(Disconnect::default())))
        );
        assert!(src.is_empty());
    }

    #[test]
    fn table_2_1_packet_type_0_is_reserved() {
        let mut src = BytesMut::from(&[0x00][..]);
        let error = Decoder::new().decode(&mut src).unwrap_err();
        assert_eq!(error, Error::ReservedPacketType);
        assert_eq!(
            error.connack_reason_code(),
            ConnectReasonCode::MalformedPacket
        );
        assert_eq!(src.len(), 1);
    }

    #[test]
    fn mqtt_2_1_3_1_reserved_flags_are_refused_from_the_first_byte() {
        for packet_type in PacketType::ALL {
            let Some(required) = packet_type.fixed_header_flags() else {
                continue;
            };
            for flags in 0..16 {
                let first = packet_type.value() << 4 | flags;
                let result = decode(&[first]);
                if flags == required {
                    assert_eq!(result, Ok(None), "{packet_type} {flags:#06b}");
                } else {
                    assert_eq!(
                        result,
                        Err(Error::InvalidFlags { packet_type, flags }),
                        "{packet_type} {flags:#06b}"
                    );
                }
            }
        }
    }

    #[test]
    fn mqtt_3_6_1_1_mqtt_3_8_1_1_and_mqtt_3_10_1_1_require_flags_0010() {
        // PUBREL, SUBSCRIBE and UNSUBSCRIBE with their usual flags of 0000 by mistake.
        for (first, packet_type) in [
            (0x60, PacketType::PubRel),
            (0x80, PacketType::Subscribe),
            (0xA0, PacketType::Unsubscribe),
        ] {
            let error = decode(&[first, 0x02, 0x00, 0x01]).unwrap_err();
            assert_eq!(
                error,
                Error::InvalidFlags {
                    packet_type,
                    flags: 0
                }
            );
            assert_eq!(
                error.disconnect_reason_code(),
                DisconnectReasonCode::MalformedPacket
            );
        }
    }

    #[test]
    fn mqtt_3_14_1_1_and_mqtt_3_15_1_1_disconnect_and_auth_flags_are_zero() {
        assert_eq!(
            decode(&[0xE1, 0x00]),
            Err(Error::InvalidFlags {
                packet_type: PacketType::Disconnect,
                flags: 1
            })
        );
        assert_eq!(
            decode(&[0xF8, 0x00]),
            Err(Error::InvalidFlags {
                packet_type: PacketType::Auth,
                flags: 8
            })
        );
    }

    #[test]
    fn mqtt_3_3_1_4_and_mqtt_3_3_1_2_publish_flags_are_refused_from_the_first_byte() {
        assert_eq!(
            decode(&[0x36]),
            Err(Error::InvalidFlags {
                packet_type: PacketType::Publish,
                flags: 0b0110
            })
        );
        assert_eq!(
            decode(&[0x38]),
            Err(Error::InvalidFlags {
                packet_type: PacketType::Publish,
                flags: 0b1000
            })
        );
    }

    #[test]
    fn mqtt_1_5_5_1_the_remaining_length_is_minimal_and_four_bytes_at_most() {
        let malformed = Err(Error::MalformedVariableByteInteger {
            field: "Remaining Length",
        });
        assert_eq!(decode(&[0xC0, 0x80, 0x00]), malformed);
        assert_eq!(decode(&[0x30, 0x80, 0x80, 0x80, 0x80, 0x01]), malformed);
        assert_eq!(decode(&[0x30, 0xFF, 0xFF, 0xFF, 0xFF]), malformed);
        assert_eq!(decode(&[0x30, 0xFF, 0xFF, 0xFF]), Ok(None));
    }

    #[test]
    fn mqtt_3_1_2_24_and_mqtt_3_2_2_15_packets_over_the_maximum_are_refused_early() {
        let decoder = Decoder::new().with_max_packet_size(NonZeroU32::new(10).unwrap());
        assert_eq!(decoder.max_packet_size(), 10);
        // A PUBLISH announcing 11 bytes is refused with only its fixed header in hand.
        let mut src = BytesMut::from(&[0x30, 0x09][..]);
        let error = decoder.decode(&mut src).unwrap_err();
        assert_eq!(
            error,
            Error::PacketTooLarge {
                size: 11,
                maximum: 10
            }
        );
        assert_eq!(
            error.disconnect_reason_code(),
            DisconnectReasonCode::PacketTooLarge
        );
        assert_eq!(
            error.connack_reason_code(),
            ConnectReasonCode::PacketTooLarge
        );
        assert_eq!(src.len(), 2);
        // Ten bytes is within the limit.
        let mut src = BytesMut::from(&[0x30, 0x08, 0x00, 0x01, b't', 0x00, 1, 2, 3, 4][..]);
        assert!(decoder.decode(&mut src).unwrap().is_some());
    }

    #[test]
    fn the_largest_remaining_length_is_accepted_by_default() {
        let mut src = BytesMut::from(&[0x30, 0xFF, 0xFF, 0xFF, 0x7F][..]);
        assert_eq!(Decoder::new().decode(&mut src), Ok(None));
        assert_eq!(Decoder::default().max_packet_size(), MAX_PACKET_SIZE);
    }

    #[test]
    fn sections_3_12_and_3_13_pingreq_and_pingresp() {
        assert_eq!(decode(&[0xC0, 0x00]), Ok(Some(Packet::PingReq)));
        assert_eq!(decode(&[0xD0, 0x00]), Ok(Some(Packet::PingResp)));
        assert_eq!(
            decode(&[0xC0, 0x01, 0x00]),
            Err(Error::TrailingBytes {
                packet_type: PacketType::PingReq,
                count: 1
            })
        );
        for (packet, bytes) in [
            (Packet::PingReq, [0xC0, 0x00]),
            (Packet::PingResp, [0xD0, 0x00]),
        ] {
            let mut dst = BytesMut::new();
            packet.encode(&mut dst).unwrap();
            assert_eq!(dst[..], bytes);
            assert_eq!(packet.encoded_len(), Ok(2));
        }
    }

    #[test]
    fn table_2_1_a_server_refuses_packets_only_a_server_sends() {
        let server = Decoder::new().with_sender(Sender::Client);
        assert_eq!(server.sender(), Some(Sender::Client));
        for bytes in [
            &[0x20, 0x03, 0x00, 0x00, 0x00][..],   // CONNACK
            &[0x90, 0x04, 0x00, 0x01, 0x00, 0x00], // SUBACK
            &[0xB0, 0x04, 0x00, 0x01, 0x00, 0x00], // UNSUBACK
            &[0xD0, 0x00],                         // PINGRESP
        ] {
            let error = server.decode(&mut BytesMut::from(bytes)).unwrap_err();
            assert!(
                matches!(
                    error,
                    Error::NotSentBy {
                        sender: Sender::Client,
                        ..
                    }
                ),
                "{error}"
            );
            assert_eq!(
                error.disconnect_reason_code(),
                DisconnectReasonCode::ProtocolError
            );
        }
        let client = Decoder::new().with_sender(Sender::Server);
        for bytes in [
            &[0xC0, 0x00][..],                                 // PINGREQ
            &[0x82, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00], // SUBSCRIBE to "" at QoS 0
        ] {
            assert!(matches!(
                client.decode(&mut BytesMut::from(bytes)),
                Err(Error::NotSentBy {
                    sender: Sender::Server,
                    ..
                })
            ));
        }
        assert_eq!(
            Decoder::new().decode(&mut BytesMut::from(&[0xD0, 0x00][..])),
            Ok(Some(Packet::PingResp))
        );
    }

    #[test]
    fn mqtt_3_3_4_6_a_client_sends_no_subscription_identifier() {
        let publish = Packet::Publish(Publish {
            topic: "t".into(),
            properties: PublishProperties {
                subscription_identifiers: vec![SubscriptionId::new(1).unwrap()],
                ..PublishProperties::default()
            },
            ..Publish::default()
        });
        assert_eq!(publish.check_sender(Sender::Server), Ok(()));
        assert_eq!(
            publish.check_sender(Sender::Client),
            Err(Error::PropertyNotSentBy {
                sender: Sender::Client,
                packet_type: PacketType::Publish,
                property: PropertyId::SubscriptionIdentifier
            })
        );
        let mut src = BytesMut::new();
        publish.encode(&mut src).unwrap();
        assert!(matches!(
            Decoder::new().with_sender(Sender::Client).decode(&mut src),
            Err(Error::PropertyNotSentBy { .. })
        ));
    }

    #[test]
    fn mqtt_3_14_2_2_a_server_sends_no_session_expiry_interval_in_disconnect() {
        let disconnect = Packet::Disconnect(Disconnect {
            properties: DisconnectProperties {
                session_expiry_interval: Some(0),
                ..DisconnectProperties::default()
            },
            ..Disconnect::default()
        });
        assert_eq!(disconnect.check_sender(Sender::Client), Ok(()));
        assert_eq!(
            disconnect.check_sender(Sender::Server),
            Err(Error::PropertyNotSentBy {
                sender: Sender::Server,
                packet_type: PacketType::Disconnect,
                property: PropertyId::SessionExpiryInterval
            })
        );
    }

    #[test]
    fn reason_codes_only_one_end_sends() {
        let no_subscribers = Packet::PubAck(PubAck {
            reason_code: PubAckReasonCode::NoMatchingSubscribers,
            ..PubAck::new(PacketId::new(1).unwrap())
        });
        assert_eq!(no_subscribers.check_sender(Sender::Server), Ok(()));
        assert_eq!(
            no_subscribers.check_sender(Sender::Client),
            Err(Error::ReasonCodeNotSentBy {
                sender: Sender::Client,
                packet_type: PacketType::PubAck,
                code: 0x10
            })
        );
        let taken_over = Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::SessionTakenOver,
            ..Disconnect::default()
        });
        assert!(taken_over.check_sender(Sender::Client).is_err());
        assert_eq!(taken_over.check_sender(Sender::Server), Ok(()));
        let with_will = Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::DisconnectWithWillMessage,
            ..Disconnect::default()
        });
        assert!(with_will.check_sender(Sender::Server).is_err());
        // The bare AUTH is Success, which only a server sends.
        assert!(
            Packet::Auth(crate::Auth::default())
                .check_sender(Sender::Client)
                .is_err()
        );
        let mut src = BytesMut::from(&[0xF0, 0x00][..]);
        assert_eq!(
            Decoder::new().with_sender(Sender::Client).decode(&mut src),
            Err(Error::ReasonCodeNotSentBy {
                sender: Sender::Client,
                packet_type: PacketType::Auth,
                code: AuthReasonCode::Success.value()
            })
        );
    }

    #[test]
    fn every_packet_converts_into_a_packet() {
        let publish = Publish {
            qos: QoS::AtLeastOnce,
            topic: "t".into(),
            packet_id: PacketId::new(1),
            ..Publish::default()
        };
        assert_eq!(Packet::from(publish.clone()), Packet::Publish(publish));
        assert_eq!(
            Packet::from(crate::Connect::default()).packet_type(),
            PacketType::Connect
        );
        let mut src = BytesMut::new();
        Packet::from(crate::ConnAck::default())
            .encode(&mut src)
            .unwrap();
        assert_eq!(
            Decoder::new().decode(&mut src),
            Ok(Some(Packet::from(crate::ConnAck::default())))
        );
        assert_eq!(Bytes::from(src), Bytes::new());
    }
}
