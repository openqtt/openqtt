//! Property tests over the public API.
//!
//! - Every valid packet encodes to exactly `encoded_len` bytes and decodes back to itself.
//! - A valid encoding cut short at any byte is "need more", never a packet or an error.
//! - Packets decode back in order however the stream is cut into reads.
//! - Arbitrary bytes, and valid encodings with bytes changed, never panic the decoder, and
//!   whatever they decode to encodes and decodes back to the same packet.
//!
//! The strategies build packets that keep every rule the codec checks, so a failure is a
//! codec bug and not a strategy that made something the specification forbids.

use std::num::{NonZeroU16, NonZeroU32};

use bytes::{Bytes, BytesMut};
use openqtt_codec::{
    AckProperties, Auth, AuthProperties, AuthReasonCode, ConnAck, ConnAckProperties, Connect,
    ConnectProperties, ConnectReasonCode, Decoder, Disconnect, DisconnectProperties,
    DisconnectReasonCode, Error, MAX_VARIABLE_BYTE_INTEGER, Packet, PacketId, PayloadFormat,
    PubAck, PubAckReasonCode, PubComp, PubCompReasonCode, PubRec, PubRecReasonCode, PubRel,
    PubRelReasonCode, Publish, PublishProperties, QoS, RetainHandling, Sender, SubAck,
    SubAckReasonCode, Subscribe, SubscribeProperties, Subscription, SubscriptionOptions, UnsubAck,
    UnsubAckReasonCode, Unsubscribe, UnsubscribeProperties, Will, WillProperties,
};
use proptest::collection::vec;
use proptest::option::of;
use proptest::prelude::*;
use proptest::sample::select;

/// A UTF-8 Encoded String: any characters but U+0000, short enough to keep cases quick.
fn text() -> impl Strategy<Value = String> {
    vec(any::<char>(), 0..12).prop_map(|chars| chars.into_iter().filter(|&c| c != '\0').collect())
}

/// Binary Data.
fn data() -> impl Strategy<Value = Bytes> {
    vec(any::<u8>(), 0..24).prop_map(Bytes::from)
}

/// User Properties, repeats and all.
fn pairs() -> impl Strategy<Value = Vec<(String, String)>> {
    vec((text(), text()), 0..3)
}

fn qos() -> impl Strategy<Value = QoS> {
    select(&[QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce][..])
}

fn packet_id() -> impl Strategy<Value = PacketId> {
    any::<u16>().prop_filter_map("0 is not a Packet Identifier", PacketId::new)
}

fn nonzero_u16() -> impl Strategy<Value = NonZeroU16> {
    any::<u16>().prop_filter_map("not zero", NonZeroU16::new)
}

fn nonzero_u32() -> impl Strategy<Value = NonZeroU32> {
    any::<u32>().prop_filter_map("not zero", NonZeroU32::new)
}

fn subscription_id() -> impl Strategy<Value = NonZeroU32> {
    (1..=MAX_VARIABLE_BYTE_INTEGER).prop_filter_map("not zero", NonZeroU32::new)
}

fn payload_format() -> impl Strategy<Value = PayloadFormat> {
    select(&[PayloadFormat::Unspecified, PayloadFormat::Utf8][..])
}

fn ack_properties() -> impl Strategy<Value = AckProperties> {
    (of(text()), pairs()).prop_map(|(reason_string, user_properties)| AckProperties {
        reason_string,
        user_properties,
    })
}

fn will() -> impl Strategy<Value = Will> {
    let properties = (
        of(any::<u32>()),
        of(payload_format()),
        of(any::<u32>()),
        of(text()),
        of(text()),
        of(data()),
        pairs(),
    )
        .prop_map(
            |(delay, format, expiry, content_type, response_topic, correlation, user)| {
                WillProperties {
                    will_delay_interval: delay,
                    payload_format_indicator: format,
                    message_expiry_interval: expiry,
                    content_type,
                    response_topic,
                    correlation_data: correlation,
                    user_properties: user,
                }
            },
        );
    (qos(), any::<bool>(), properties, text(), data()).prop_map(
        |(qos, retain, properties, topic, payload)| Will {
            qos,
            retain,
            properties,
            topic,
            payload,
        },
    )
}

fn connect() -> impl Strategy<Value = Connect> {
    let properties = (
        of(any::<u32>()),
        of(nonzero_u16()),
        of(nonzero_u32()),
        of(any::<u16>()),
        of(any::<bool>()),
        of(any::<bool>()),
        pairs(),
        // Authentication Data only ever with an Authentication Method.
        of((text(), of(data()))),
    )
        .prop_map(
            |(expiry, receive, packet, alias, response, problem, user, auth)| {
                let (method, data) =
                    auth.map_or((None, None), |(method, data)| (Some(method), data));
                ConnectProperties {
                    session_expiry_interval: expiry,
                    receive_maximum: receive,
                    maximum_packet_size: packet,
                    topic_alias_maximum: alias,
                    request_response_information: response,
                    request_problem_information: problem,
                    user_properties: user,
                    authentication_method: method,
                    authentication_data: data,
                }
            },
        );
    (
        any::<bool>(),
        any::<u16>(),
        properties,
        text(),
        of(will()),
        of(text()),
        of(data()),
    )
        .prop_map(
            |(clean_start, keep_alive, properties, client_id, will, username, password)| Connect {
                clean_start,
                keep_alive,
                properties,
                client_id,
                will,
                username,
                password,
            },
        )
}

fn connack() -> impl Strategy<Value = ConnAck> {
    let first = (
        of(any::<u32>()),
        of(nonzero_u16()),
        // A Maximum QoS of 2 is what leaving it out means; it is never sent.
        of(select(&[QoS::AtMostOnce, QoS::AtLeastOnce][..])),
        of(any::<bool>()),
        of(nonzero_u32()),
        of(text()),
        of(any::<u16>()),
        of(text()),
        pairs(),
    );
    let second = (
        of(any::<bool>()),
        of(any::<bool>()),
        of(any::<bool>()),
        of(any::<u16>()),
        of(text()),
        of(text()),
        of(text()),
        of(data()),
    );
    (select(ConnectReasonCode::ALL), any::<bool>(), first, second).prop_map(
        |(reason_code, present, first, second)| {
            let (expiry, receive, qos, retain, packet, assigned, alias, reason, user) = first;
            let (wildcard, ids, shared, keep_alive, response, reference, method, data) = second;
            ConnAck {
                // Session Present only with Success [MQTT-3.2.2-6].
                session_present: present && reason_code == ConnectReasonCode::Success,
                reason_code,
                properties: ConnAckProperties {
                    session_expiry_interval: expiry,
                    receive_maximum: receive,
                    maximum_qos: qos,
                    retain_available: retain,
                    maximum_packet_size: packet,
                    assigned_client_identifier: assigned,
                    topic_alias_maximum: alias,
                    reason_string: reason,
                    user_properties: user,
                    wildcard_subscription_available: wildcard,
                    subscription_identifier_available: ids,
                    shared_subscription_available: shared,
                    server_keep_alive: keep_alive,
                    response_information: response,
                    server_reference: reference,
                    authentication_method: method,
                    authentication_data: data,
                },
            }
        },
    )
}

fn publish() -> impl Strategy<Value = Publish> {
    let properties = (
        of(payload_format()),
        of(any::<u32>()),
        of(nonzero_u16()),
        of(text()),
        of(data()),
        pairs(),
        vec(subscription_id(), 0..3),
        of(text()),
    )
        .prop_map(
            |(format, expiry, alias, response_topic, correlation, user, ids, content_type)| {
                PublishProperties {
                    payload_format_indicator: format,
                    message_expiry_interval: expiry,
                    topic_alias: alias,
                    response_topic,
                    correlation_data: correlation,
                    user_properties: user,
                    subscription_identifiers: ids,
                    content_type,
                }
            },
        );
    (
        qos(),
        any::<bool>(),
        any::<bool>(),
        text(),
        packet_id(),
        properties,
        data(),
    )
        .prop_map(|(qos, dup, retain, topic, id, mut properties, payload)| {
            let delivered = qos != QoS::AtMostOnce;
            // An empty Topic Name stands for a Topic Alias (section 3.3.2.1).
            if topic.is_empty() && properties.topic_alias.is_none() {
                properties.topic_alias = NonZeroU16::new(1);
            }
            Publish {
                // DUP and a Packet Identifier only above QoS 0 ([MQTT-3.3.1-2], [MQTT-2.2.1-2]).
                dup: dup && delivered,
                qos,
                retain,
                topic,
                packet_id: delivered.then_some(id),
                properties,
                payload,
            }
        })
}

fn subscription_options() -> impl Strategy<Value = SubscriptionOptions> {
    let retain_handling = select(
        &[
            RetainHandling::SendAtSubscribe,
            RetainHandling::SendIfNew,
            RetainHandling::DoNotSend,
        ][..],
    );
    (qos(), any::<bool>(), any::<bool>(), retain_handling).prop_map(
        |(maximum_qos, no_local, retain_as_published, retain_handling)| SubscriptionOptions {
            maximum_qos,
            no_local,
            retain_as_published,
            retain_handling,
        },
    )
}

fn subscribe() -> impl Strategy<Value = Subscribe> {
    let subscriptions = vec(
        (text(), subscription_options())
            .prop_map(|(filter, options)| Subscription { filter, options }),
        1..4,
    );
    (packet_id(), of(subscription_id()), pairs(), subscriptions).prop_map(
        |(packet_id, identifier, user_properties, subscriptions)| Subscribe {
            packet_id,
            properties: SubscribeProperties {
                subscription_identifier: identifier,
                user_properties,
            },
            subscriptions,
        },
    )
}

fn unsubscribe() -> impl Strategy<Value = Unsubscribe> {
    (packet_id(), pairs(), vec(text(), 1..4)).prop_map(|(packet_id, user_properties, filters)| {
        Unsubscribe {
            packet_id,
            properties: UnsubscribeProperties { user_properties },
            filters,
        }
    })
}

fn disconnect() -> impl Strategy<Value = Disconnect> {
    (
        select(DisconnectReasonCode::ALL),
        of(any::<u32>()),
        of(text()),
        pairs(),
        of(text()),
    )
        .prop_map(
            |(reason_code, expiry, reason, user, reference)| Disconnect {
                reason_code,
                properties: DisconnectProperties {
                    session_expiry_interval: expiry,
                    reason_string: reason,
                    user_properties: user,
                    server_reference: reference,
                },
            },
        )
}

fn auth() -> impl Strategy<Value = Auth> {
    (
        select(AuthReasonCode::ALL),
        of(text()),
        of(data()),
        of(text()),
        pairs(),
    )
        .prop_map(|(reason_code, method, data, reason, user)| {
            let mut auth = Auth {
                reason_code,
                properties: AuthProperties {
                    authentication_method: method,
                    authentication_data: data,
                    reason_string: reason,
                    user_properties: user,
                },
            };
            // Only the bare Success may leave out the Authentication Method (section
            // 3.15.2.2.2).
            if auth.properties.authentication_method.is_none() && auth != Auth::default() {
                auth.properties.authentication_method = Some("SCRAM-SHA-256".into());
            }
            auth
        })
}

/// Any valid packet, every type equally likely.
fn packet() -> impl Strategy<Value = Packet> {
    prop_oneof![
        connect().prop_map(Packet::from),
        connack().prop_map(Packet::from),
        publish().prop_map(Packet::Publish),
        (packet_id(), select(PubAckReasonCode::ALL), ack_properties()).prop_map(
            |(packet_id, reason_code, properties)| Packet::PubAck(PubAck {
                packet_id,
                reason_code,
                properties
            })
        ),
        (packet_id(), select(PubRecReasonCode::ALL), ack_properties()).prop_map(
            |(packet_id, reason_code, properties)| Packet::PubRec(PubRec {
                packet_id,
                reason_code,
                properties
            })
        ),
        (packet_id(), select(PubRelReasonCode::ALL), ack_properties()).prop_map(
            |(packet_id, reason_code, properties)| Packet::PubRel(PubRel {
                packet_id,
                reason_code,
                properties
            })
        ),
        (
            packet_id(),
            select(PubCompReasonCode::ALL),
            ack_properties()
        )
            .prop_map(
                |(packet_id, reason_code, properties)| Packet::PubComp(PubComp {
                    packet_id,
                    reason_code,
                    properties
                })
            ),
        subscribe().prop_map(Packet::Subscribe),
        (
            packet_id(),
            ack_properties(),
            vec(select(SubAckReasonCode::ALL), 1..5)
        )
            .prop_map(
                |(packet_id, properties, reason_codes)| Packet::SubAck(SubAck {
                    packet_id,
                    properties,
                    reason_codes
                })
            ),
        unsubscribe().prop_map(Packet::Unsubscribe),
        (
            packet_id(),
            ack_properties(),
            vec(select(UnsubAckReasonCode::ALL), 1..5)
        )
            .prop_map(
                |(packet_id, properties, reason_codes)| Packet::UnsubAck(UnsubAck {
                    packet_id,
                    properties,
                    reason_codes
                })
            ),
        Just(Packet::PingReq),
        Just(Packet::PingResp),
        disconnect().prop_map(Packet::Disconnect),
        auth().prop_map(Packet::Auth),
    ]
}

fn encode(packet: &Packet) -> Vec<u8> {
    let mut dst = BytesMut::new();
    packet
        .encode(&mut dst)
        .expect("the strategies build packets that encode");
    dst.to_vec()
}

/// Decodes `bytes` packet by packet until they run out or stop parsing, checking on the way
/// that whatever decodes encodes and decodes back to itself.
fn decode_everything(bytes: &[u8]) {
    let mut src = BytesMut::from(bytes);
    let decoder = Decoder::new();
    while let Ok(Some(packet)) = decoder.decode(&mut src) {
        let encoded = encode(&packet);
        assert_eq!(packet.encoded_len(), Ok(encoded.len()));
        let mut again = BytesMut::from(&encoded[..]);
        assert_eq!(decoder.decode(&mut again), Ok(Some(packet.clone())));
        assert!(again.is_empty());
        // Neither end's rules may panic on anything that decodes.
        let _ = packet.check_sender(Sender::Client);
        let _ = packet.check_sender(Sender::Server);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    fn every_valid_packet_round_trips(packet in packet()) {
        let bytes = encode(&packet);
        prop_assert_eq!(packet.encoded_len(), Ok(bytes.len()));
        let mut src = BytesMut::from(&bytes[..]);
        prop_assert_eq!(Decoder::new().decode(&mut src), Ok(Some(packet)));
        prop_assert!(src.is_empty());
    }

    #[test]
    fn a_valid_encoding_cut_short_needs_more(packet in packet()) {
        let bytes = encode(&packet);
        for len in 0..bytes.len() {
            let mut src = BytesMut::from(&bytes[..len]);
            prop_assert_eq!(Decoder::new().decode(&mut src), Ok(None), "{} of {} bytes", len, bytes.len());
            prop_assert_eq!(&src[..], &bytes[..len]);
        }
    }

    #[test]
    fn packets_decode_in_order_however_the_stream_is_cut(
        packets in vec(packet(), 1..6),
        cuts in vec(1usize..40, 1..20),
    ) {
        let stream: Vec<u8> = packets.iter().flat_map(encode).collect();
        let decoder = Decoder::new();
        let mut src = BytesMut::new();
        let mut decoded = Vec::new();
        let mut rest = &stream[..];
        let mut cuts = cuts.into_iter().cycle();
        while !rest.is_empty() {
            let take = cuts.next().unwrap_or(1).min(rest.len());
            src.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            while let Some(packet) = decoder.decode(&mut src).expect("valid packets decode") {
                decoded.push(packet);
            }
        }
        prop_assert!(src.is_empty());
        prop_assert_eq!(decoded, packets);
    }

    #[test]
    fn a_decoder_with_a_maximum_refuses_exactly_the_larger_packets(
        packet in packet(),
        maximum in 2u32..400,
    ) {
        let bytes = encode(&packet);
        let decoder = Decoder::new().with_max_packet_size(NonZeroU32::new(maximum).expect("not zero"));
        let result = decoder.decode(&mut BytesMut::from(&bytes[..]));
        if bytes.len() > usize::try_from(maximum).expect("fits") {
            prop_assert_eq!(result, Err(Error::PacketTooLarge { size: bytes.len(), maximum }));
        } else {
            prop_assert_eq!(result, Ok(Some(packet.clone())));
        }
        // encode_within draws the same line before writing anything.
        let mut dst = BytesMut::new();
        prop_assert_eq!(packet.encode_within(&mut dst, maximum).is_ok(), dst.len() == bytes.len());
    }

    #[test]
    fn arbitrary_bytes_never_panic(bytes in vec(any::<u8>(), 0..512)) {
        decode_everything(&bytes);
    }

    #[test]
    fn arbitrary_bytes_after_a_fixed_header_never_panic(
        first in any::<u8>(),
        body in vec(any::<u8>(), 0..300),
    ) {
        // A believable fixed header gets random bytes past the first checks.
        let mut bytes = vec![first];
        let mut len = body.len();
        loop {
            let digit = u8::try_from(len % 128).expect("below 128");
            len /= 128;
            bytes.push(if len > 0 { digit | 0x80 } else { digit });
            if len == 0 {
                break;
            }
        }
        bytes.extend(body);
        decode_everything(&bytes);
    }

    #[test]
    fn valid_encodings_with_bytes_changed_never_panic(
        packet in packet(),
        changes in vec((any::<usize>(), any::<u8>()), 1..4),
    ) {
        let mut bytes = encode(&packet);
        for (at, value) in changes {
            let at = at % bytes.len();
            bytes[at] = value;
        }
        decode_everything(&bytes);
    }
}
