//! Golden vectors: whole packets assembled by hand from the byte layouts the specification
//! draws, each decoded and encoded against the packet it stands for.
//!
//! Every non-normative example of chapters 1 to 4 that shows bytes is here: Figure 3-6
//! (CONNECT variable header), Figure 3-9 (PUBLISH variable header), Figures 3-19 and 3-21
//! (SUBSCRIBE), Figure 3.30 (UNSUBSCRIBE payload), Figure 3-24 (DISCONNECT variable header)
//! and the SCRAM exchange of section 4.12. Where a figure shows only part of a packet, the rest
//! follows the layout of its section, as each vector's comment says. Figure 1-2 and Table 1-1
//! are covered where the codec reads strings and Variable Byte Integers.
//!
//! The bytes are written out rather than built by the codec, so a mistake in the codec cannot
//! also be a mistake in the vector.

use std::num::{NonZeroU16, NonZeroU32};

use bytes::{Bytes, BytesMut};
use openqtt_codec::{
    AckProperties, Auth, AuthProperties, AuthReasonCode, ConnAck, ConnAckProperties, Connect,
    ConnectProperties, ConnectReasonCode, Decoder, Disconnect, DisconnectProperties,
    DisconnectReasonCode, Packet, PacketId, ProtocolRefusal, PubAck, PubAckReasonCode, PubComp,
    PubCompReasonCode, PubRec, PubRecReasonCode, PubRel, Publish, PublishProperties, QoS,
    RetainHandling, SubAck, SubAckReasonCode, Subscribe, SubscribeProperties, Subscription,
    SubscriptionOptions, UnsubAck, UnsubAckReasonCode, Unsubscribe, UnsubscribeProperties, Will,
    WillProperties,
};

/// A packet and the bytes that carry it.
struct Vector {
    /// What the vector shows, and where in the specification its layout comes from.
    name: &'static str,
    /// The packet, fixed header included.
    bytes: Vec<u8>,
    /// What it decodes to.
    packet: Packet,
}

fn id(value: u16) -> PacketId {
    PacketId::new(value).expect("a Packet Identifier is never 0")
}

/// Joins the lines of a vector.
fn bytes(lines: &[&[u8]]) -> Vec<u8> {
    lines.concat()
}

/// Packets that encode back to exactly the bytes they were decoded from.
fn canonical() -> Vec<Vector> {
    vec![
        Vector {
            name: "CONNECT: Figure 3-6, then the payload its flags call for (section 3.1.3)",
            bytes: bytes(&[
                &[0x10, 0x36],                         // CONNECT, Remaining Length 54
                &[0x00, 0x04, b'M', b'Q', b'T', b'T'], // Protocol Name (Figure 3-2)
                &[0x05],                               // Protocol Version 5 (Figure 3-3)
                &[0xCE],       // User Name, Password, Will QoS 1, Will Flag, Clean Start
                &[0x00, 0x0A], // Keep Alive 10
                &[0x05, 0x11, 0x00, 0x00, 0x00, 0x0A], // Session Expiry Interval 10
                &[0x00, 0x08],
                b"client-1", // Client Identifier
                &[0x00],     // Will Properties: none
                &[0x00, 0x09],
                b"last/will", // Will Topic
                &[0x00, 0x04],
                b"gone", // Will Payload
                &[0x00, 0x04],
                b"user",                   // User Name
                &[0x00, 0x02, 0xDE, 0xAD], // Password
            ]),
            packet: Packet::from(Connect {
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
            }),
        },
        Vector {
            name: "CONNECT: the least a client can send, an empty Client Identifier",
            bytes: bytes(&[
                &[0x10, 0x0D],
                &[0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05],
                &[0x02],       // Clean Start
                &[0x00, 0x00], // Keep Alive 0, off
                &[0x00],       // no properties
                &[0x00, 0x00], // Client Identifier "" (section 3.1.3.1)
            ]),
            packet: Packet::from(Connect {
                clean_start: true,
                ..Connect::default()
            }),
        },
        Vector {
            name: "CONNACK: Session Present with an Assigned Client Identifier (section 3.2)",
            bytes: bytes(&[
                &[0x20, 0x0D],
                &[0x01], // Session Present (section 3.2.2.1.1)
                &[0x00], // Success
                &[0x0A, 0x12, 0x00, 0x07],
                b"auto-42", // Assigned Client Identifier
            ]),
            packet: Packet::from(ConnAck {
                session_present: true,
                reason_code: ConnectReasonCode::Success,
                properties: ConnAckProperties {
                    assigned_client_identifier: Some("auto-42".into()),
                    ..ConnAckProperties::default()
                },
            }),
        },
        Vector {
            name: "CONNACK: 0x84, the refusal of an older protocol ([MQTT-3.1.2-2])",
            bytes: ProtocolRefusal::ConnAckV5.bytes().to_vec(),
            packet: Packet::from(ConnAck {
                reason_code: ConnectReasonCode::UnsupportedProtocolVersion,
                ..ConnAck::default()
            }),
        },
        Vector {
            name: "PUBLISH: Figure 3-9 at QoS 1, then a payload (section 3.3.3)",
            bytes: bytes(&[
                &[0x32, 0x0D],                   // PUBLISH, QoS 1, Remaining Length 13
                &[0x00, 0x03, b'a', b'/', b'b'], // Topic Name "a/b"
                &[0x00, 0x0A],                   // Packet Identifier 10
                &[0x00],                         // no properties
                b"hello",
            ]),
            packet: Packet::Publish(Publish {
                qos: QoS::AtLeastOnce,
                topic: "a/b".into(),
                packet_id: Some(id(10)),
                payload: Bytes::from_static(b"hello"),
                ..Publish::default()
            }),
        },
        Vector {
            name: "PUBLISH: a Topic Alias in place of the Topic Name (section 3.3.2.3.4)",
            bytes: bytes(&[
                &[0x30, 0x07],             // PUBLISH, QoS 0
                &[0x00, 0x00],             // Topic Name ""
                &[0x03, 0x23, 0x00, 0x01], // Topic Alias 1
                b"x",
            ]),
            packet: Packet::Publish(Publish {
                properties: PublishProperties {
                    topic_alias: NonZeroU16::new(1),
                    ..PublishProperties::default()
                },
                payload: Bytes::from_static(b"x"),
                ..Publish::default()
            }),
        },
        Vector {
            name: "PUBLISH: a Remaining Length of 206, two bytes of Variable Byte Integer",
            bytes: bytes(&[
                &[0x31],       // PUBLISH, QoS 0, RETAIN
                &[0xCE, 0x01], // 206 = 0x4E + 1 * 128 (section 1.5.5)
                &[0x00, 0x03, b'a', b'/', b'b', 0x00],
                &[0x5A; 200],
            ]),
            packet: Packet::Publish(Publish {
                retain: true,
                topic: "a/b".into(),
                payload: Bytes::from_static(&[0x5A; 200]),
                ..Publish::default()
            }),
        },
        Vector {
            name: "PUBLISH: a redelivery at QoS 2 with properties (section 3.3.2.3)",
            bytes: bytes(&[
                &[0x3C, 0x20],                               // PUBLISH, DUP, QoS 2, Remaining Length 32
                &[0x00, 0x01, b't'],                         // Topic Name "t"
                &[0xFF, 0xFF],                               // Packet Identifier 65535
                &[0x16],                                     // Property Length 22
                &[0x01, 0x01],                               // Payload Format Indicator: UTF-8
                &[0x02, 0x00, 0x00, 0x0E, 0x10],             // Message Expiry Interval 3600
                &[0x08, 0x00, 0x01, b'r'],                   // Response Topic "r"
                &[0x09, 0x00, 0x01, 0x07],                   // Correlation Data 07
                &[0x26, 0x00, 0x01, b'k', 0x00, 0x01, b'v'], // User Property k=v
                b"21.5",
            ]),
            packet: Packet::Publish(Publish {
                dup: true,
                qos: QoS::ExactlyOnce,
                topic: "t".into(),
                packet_id: Some(id(0xFFFF)),
                properties: PublishProperties {
                    payload_format_indicator: Some(openqtt_codec::PayloadFormat::Utf8),
                    message_expiry_interval: Some(3600),
                    response_topic: Some("r".into()),
                    correlation_data: Some(Bytes::from_static(&[0x07])),
                    user_properties: vec![("k".into(), "v".into())],
                    ..PublishProperties::default()
                },
                payload: Bytes::from_static(b"21.5"),
                ..Publish::default()
            }),
        },
        Vector {
            name: "PUBACK: Success, Remaining Length 2 (section 3.4.2.1)",
            bytes: vec![0x40, 0x02, 0x00, 0x0A],
            packet: Packet::PubAck(PubAck::new(id(10))),
        },
        Vector {
            name: "PUBACK: Not authorized, Remaining Length 3 (section 3.4.2.2.1)",
            bytes: vec![0x40, 0x03, 0x00, 0x0A, 0x87],
            packet: Packet::PubAck(PubAck {
                reason_code: PubAckReasonCode::NotAuthorized,
                ..PubAck::new(id(10))
            }),
        },
        Vector {
            name: "PUBREC: No matching subscribers with a Reason String (section 3.5)",
            bytes: vec![0x50, 0x08, 0x00, 0x0B, 0x10, 0x04, 0x1F, 0x00, 0x01, b'x'],
            packet: Packet::PubRec(PubRec {
                reason_code: PubRecReasonCode::NoMatchingSubscribers,
                properties: AckProperties {
                    reason_string: Some("x".into()),
                    ..AckProperties::default()
                },
                ..PubRec::new(id(11))
            }),
        },
        Vector {
            name: "PUBREL: flags 0010 ([MQTT-3.6.1-1]), Success",
            bytes: vec![0x62, 0x02, 0x00, 0x0B],
            packet: Packet::PubRel(PubRel::new(id(11))),
        },
        Vector {
            name: "PUBCOMP: Packet Identifier not found (section 3.7.2.1)",
            bytes: vec![0x70, 0x03, 0x00, 0x0B, 0x92],
            packet: Packet::PubComp(PubComp {
                reason_code: PubCompReasonCode::PacketIdentifierNotFound,
                ..PubComp::new(id(11))
            }),
        },
        Vector {
            name: "SUBSCRIBE: Figure 3-19 then Figure 3-21",
            bytes: bytes(&[
                &[0x82, 0x0F],                         // SUBSCRIBE, flags 0010, Remaining Length 15
                &[0x00, 0x0A, 0x00],                   // Packet Identifier 10, no properties
                &[0x00, 0x03, b'a', b'/', b'b', 0x01], // "a/b", Maximum QoS 1
                &[0x00, 0x03, b'c', b'/', b'd', 0x02], // "c/d", Maximum QoS 2
            ]),
            packet: Packet::Subscribe(Subscribe {
                packet_id: id(10),
                properties: SubscribeProperties::default(),
                subscriptions: vec![
                    Subscription {
                        filter: "a/b".into(),
                        options: SubscriptionOptions {
                            maximum_qos: QoS::AtLeastOnce,
                            ..SubscriptionOptions::default()
                        },
                    },
                    Subscription {
                        filter: "c/d".into(),
                        options: SubscriptionOptions {
                            maximum_qos: QoS::ExactlyOnce,
                            ..SubscriptionOptions::default()
                        },
                    },
                ],
            }),
        },
        Vector {
            name: "SUBSCRIBE: every option bit and a Subscription Identifier (section 3.8.3.1)",
            bytes: bytes(&[
                &[0x82, 0x0B],
                &[0x00, 0x01],       // Packet Identifier 1
                &[0x02, 0x0B, 0x01], // Subscription Identifier 1
                &[0x00, 0x03, b'a', b'/', b'#'],
                &[0x2D], // Retain Handling 2, Retain As Published, No Local, Maximum QoS 1
            ]),
            packet: Packet::Subscribe(Subscribe {
                packet_id: id(1),
                properties: SubscribeProperties {
                    subscription_identifier: NonZeroU32::new(1),
                    ..SubscribeProperties::default()
                },
                subscriptions: vec![Subscription {
                    filter: "a/#".into(),
                    options: SubscriptionOptions {
                        maximum_qos: QoS::AtLeastOnce,
                        no_local: true,
                        retain_as_published: true,
                        retain_handling: RetainHandling::DoNotSend,
                    },
                }],
            }),
        },
        Vector {
            name: "SUBACK: the answer to Figure 3-21, Granted QoS 1 and 2 (section 3.9)",
            bytes: vec![0x90, 0x05, 0x00, 0x0A, 0x00, 0x01, 0x02],
            packet: Packet::SubAck(SubAck {
                packet_id: id(10),
                properties: AckProperties::default(),
                reason_codes: vec![SubAckReasonCode::GrantedQos1, SubAckReasonCode::GrantedQos2],
            }),
        },
        Vector {
            name: "UNSUBSCRIBE: Figure 3.30, flags 0010 ([MQTT-3.10.1-1])",
            bytes: bytes(&[
                &[0xA2, 0x0D],
                &[0x00, 0x0B, 0x00], // Packet Identifier 11, no properties
                &[0x00, 0x03, b'a', b'/', b'b'],
                &[0x00, 0x03, b'c', b'/', b'd'],
            ]),
            packet: Packet::Unsubscribe(Unsubscribe {
                packet_id: id(11),
                properties: UnsubscribeProperties::default(),
                filters: vec!["a/b".into(), "c/d".into()],
            }),
        },
        Vector {
            name: "UNSUBACK: Success, then No subscription existed (section 3.11)",
            bytes: vec![0xB0, 0x05, 0x00, 0x0B, 0x00, 0x00, 0x11],
            packet: Packet::UnsubAck(UnsubAck {
                packet_id: id(11),
                properties: AckProperties::default(),
                reason_codes: vec![
                    UnsubAckReasonCode::Success,
                    UnsubAckReasonCode::NoSubscriptionExisted,
                ],
            }),
        },
        Vector {
            name: "PINGREQ (Figure 3.33)",
            bytes: vec![0xC0, 0x00],
            packet: Packet::PingReq,
        },
        Vector {
            name: "PINGRESP (Figure 3.34)",
            bytes: vec![0xD0, 0x00],
            packet: Packet::PingResp,
        },
        Vector {
            name: "DISCONNECT: Normal disconnection, Remaining Length 0 (section 3.14.2.1)",
            bytes: vec![0xE0, 0x00],
            packet: Packet::Disconnect(Disconnect::default()),
        },
        Vector {
            name: "DISCONNECT: Session taken over, Remaining Length 1 (section 3.14.2.2.1)",
            bytes: vec![0xE0, 0x01, 0x8E],
            packet: Packet::Disconnect(Disconnect {
                reason_code: DisconnectReasonCode::SessionTakenOver,
                ..Disconnect::default()
            }),
        },
        Vector {
            name: "DISCONNECT: Figure 3-24, its Property Length read as the 5 it is labelled",
            bytes: vec![0xE0, 0x07, 0x00, 0x05, 0x11, 0x00, 0x00, 0x00, 0x00],
            packet: Packet::Disconnect(Disconnect {
                reason_code: DisconnectReasonCode::NormalDisconnection,
                properties: DisconnectProperties {
                    session_expiry_interval: Some(0),
                    ..DisconnectProperties::default()
                },
            }),
        },
        Vector {
            name: "AUTH: Success, Remaining Length 0 (section 3.15.2.1)",
            bytes: vec![0xF0, 0x00],
            packet: Packet::Auth(Auth::default()),
        },
        Vector {
            name: "AUTH: the server's challenge in the SCRAM example of section 4.12",
            bytes: bytes(&[
                &[0xF0, 0x24], // AUTH, Remaining Length 36
                &[0x18],       // Continue authentication
                &[0x22],       // Property Length 34
                &[0x15, 0x00, 0x0B],
                b"SCRAM-SHA-1", // Authentication Method
                &[0x16, 0x00, 0x11],
                b"server-first-data", // Authentication Data
            ]),
            packet: Packet::Auth(Auth {
                reason_code: AuthReasonCode::ContinueAuthentication,
                properties: AuthProperties {
                    authentication_method: Some("SCRAM-SHA-1".into()),
                    authentication_data: Some(Bytes::from_static(b"server-first-data")),
                    ..AuthProperties::default()
                },
            }),
        },
    ]
}

/// Packets written in a longer form the specification also allows, which decode to the same
/// packet as their shortest form and encode to it.
fn longer_forms() -> Vec<Vector> {
    vec![
        Vector {
            name: "PUBACK: Success and an empty Property Length written out",
            bytes: vec![0x40, 0x04, 0x00, 0x0A, 0x00, 0x00],
            packet: Packet::PubAck(PubAck::new(id(10))),
        },
        Vector {
            name: "PUBCOMP: Success written out, Remaining Length 3",
            bytes: vec![0x70, 0x03, 0x00, 0x0B, 0x00],
            packet: Packet::PubComp(PubComp::new(id(11))),
        },
        Vector {
            name: "DISCONNECT: Normal disconnection and an empty Property Length written out",
            bytes: vec![0xE0, 0x02, 0x00, 0x00],
            packet: Packet::Disconnect(Disconnect::default()),
        },
        Vector {
            name: "AUTH: Success and an empty Property Length written out",
            bytes: vec![0xF0, 0x02, 0x00, 0x00],
            packet: Packet::Auth(Auth::default()),
        },
    ]
}

fn decode_one(bytes: &[u8]) -> Packet {
    let mut src = BytesMut::from(bytes);
    let packet = Decoder::new()
        .decode(&mut src)
        .unwrap_or_else(|error| panic!("{error}"))
        .expect("a whole packet");
    assert!(src.is_empty(), "the packet is all of the bytes");
    packet
}

fn encode(packet: &Packet) -> Vec<u8> {
    let mut dst = BytesMut::new();
    packet
        .encode(&mut dst)
        .expect("every vector's packet encodes");
    assert_eq!(packet.encoded_len(), Ok(dst.len()));
    dst.to_vec()
}

#[test]
fn every_vector_decodes_to_its_packet() {
    for vector in canonical().into_iter().chain(longer_forms()) {
        assert_eq!(decode_one(&vector.bytes), vector.packet, "{}", vector.name);
    }
}

#[test]
fn every_canonical_vector_is_what_its_packet_encodes_to() {
    for vector in canonical() {
        assert_eq!(encode(&vector.packet), vector.bytes, "{}", vector.name);
    }
}

#[test]
fn longer_forms_encode_to_their_shortest_form() {
    for vector in longer_forms() {
        let shortest = encode(&vector.packet);
        assert!(shortest.len() < vector.bytes.len(), "{}", vector.name);
        assert_eq!(decode_one(&shortest), vector.packet, "{}", vector.name);
    }
}

#[test]
fn the_vectors_cover_every_packet_type() {
    let mut types: Vec<u8> = canonical()
        .iter()
        .map(|vector| vector.packet.packet_type().value())
        .collect();
    types.sort_unstable();
    types.dedup();
    assert_eq!(types, (1..=15).collect::<Vec<_>>());
}

#[test]
fn the_vectors_decode_back_to_back_from_one_buffer() {
    let vectors = canonical();
    let mut src = BytesMut::new();
    for vector in &vectors {
        src.extend_from_slice(&vector.bytes);
    }
    let decoder = Decoder::new();
    for vector in &vectors {
        assert_eq!(
            decoder.decode(&mut src),
            Ok(Some(vector.packet.clone())),
            "{}",
            vector.name
        );
    }
    assert!(src.is_empty());
}
