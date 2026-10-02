//! Property tests: random sequences of client packets, answers, deliveries, timers and orders,
//! with invariants checked after every step.
//!
//! Every packet the client sends is encoded and decoded by the codec first, as a server's
//! decoder would read it, so the machine sees only what a transport can hand it; one the codec
//! refuses reaches it as a decoding error. Requests are answered only when a step says so, in
//! any order, so the interleavings an edge can produce are explored. The invariants:
//!
//! - nothing panics;
//! - every packet the server sends is one a server may send, encodes, decodes to itself, and
//!   fits the client's Maximum Packet Size ([MQTT-3.1.2-24]);
//! - CONNACK comes once at most, before any other packet but AUTH ([MQTT-3.2.0-1],
//!   [MQTT-3.2.0-2]), and nothing is sent after a DISCONNECT or a refusing CONNACK
//!   ([MQTT-3.14.4-1]);
//! - QoS 1 and 2 messages in flight to the client never exceed its Receive Maximum
//!   ([MQTT-3.3.4-9]), and those from it never exceed the server's ([MQTT-3.3.4-7]);
//! - the server answers the QoS 1 and 2 PUBLISH packets of each stream in the order they
//!   arrived (report R1, O15);
//! - the PUBLISH packets of one QoS 2 exchange are all answered alike, accepted or refused with
//!   the same code, and a repeat takes no slot of the server's Receive Maximum, wherever the
//!   first PUBLISH is: in the backlog, being authorized, committing or refused
//!   ([MQTT-4.3.3-9], [MQTT-4.3.3-10]). The model follows the client's exchanges through the
//!   acknowledgements and asserts only where the server cannot take another view;
//! - Packet Identifiers in flight are never reused ([MQTT-2.2.1-4]);
//! - every QoS 1 and 2 PUBLISH from the client is acknowledged at most once, and exactly once
//!   when the connection is still open after every request is answered ([MQTT-3.3.4-1]);
//! - a Topic Alias goes only on the control stream and within the client's maximum;
//! - the connection closes once at most, and a claim that succeeded is released exactly once.

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroU16;

use bytes::{Bytes, BytesMut};
use openqtt_codec::{
    Auth, AuthProperties, AuthReasonCode, Connect, ConnectProperties, ConnectReasonCode, Decoder,
    Disconnect, DisconnectProperties, DisconnectReasonCode, MAX_PACKET_SIZE, Packet, PacketType,
    PubAck, PubAckReasonCode, PubComp, PubCompReasonCode, PubRec, PubRecReasonCode, PubRel,
    Publish, PublishProperties, QoS, Sender, Subscribe, SubscribeProperties, Subscription,
    SubscriptionOptions, Unsubscribe, UnsubscribeProperties, Will, WillProperties,
};
use openqtt_core::{ClientId, Deadline, Message, Timestamp, TopicFilter, TopicName};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::select;

use super::harness::at;
use crate::{
    AuthResult, ClaimResult, Config, Decision, Delivery, Effect, Input, Peer, PublishOutcome,
    PublishToken, Session, Shutdown, StreamEnd, StreamId, Timer,
};

/// Topics names and filters are drawn from: valid and invalid, `$` ones, shared ones.
const TOPICS: &[&str] = &[
    "a",
    "b",
    "a/b",
    "a/+",
    "#",
    "+/b",
    "$SYS/x",
    "$share/g/a",
    "a/#/b",
];

/// Topic Names the broker delivers on.
const NAMES: &[&str] = &["a", "b", "a/b", "c/d", "$SYS/x"];

/// What happens next.
#[derive(Debug, Clone)]
enum Step {
    /// The client sends a packet.
    Client {
        stream: StreamId,
        packet: Packet,
        early: bool,
    },
    /// The client's bytes do not decode.
    Garbage {
        stream: StreamId,
        packet_type: Option<PacketType>,
    },
    /// The authenticator answers.
    Authenticate(u8),
    /// The authorizer answers the oldest request, deciding by these bits.
    Authorize(u8),
    /// The log answers the claim.
    Claim(u8),
    /// A commit completes.
    Commit { pick: usize, outcome: u8 },
    /// The broker delivers a message.
    Deliver {
        topic: &'static str,
        qos: u8,
        retain: bool,
        own: bool,
        expiry: Option<u32>,
        pick: u8,
    },
    /// The oldest retained read completes.
    Retained { count: u8 },
    /// The client acknowledges a message the server has in flight, or releases one of its own
    /// QoS 2 messages, picking among them.
    Acknowledge { pick: usize, failure: bool },
    /// The clock moves, by milliseconds.
    Advance(u32),
    /// The handshake of a 0-RTT connection completes.
    Handshake(bool),
    /// The client ends a data stream.
    StreamEnded(u64, bool),
    /// Another connection takes the session over.
    TakeOver(bool),
    /// The edge shuts the connection down.
    Shutdown(u8),
    /// The transport closes.
    TransportClosed,
}

fn qos() -> impl Strategy<Value = QoS> {
    select(&[QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce][..])
}

fn stream() -> impl Strategy<Value = StreamId> {
    prop_oneof![
        8 => Just(StreamId::Control),
        1 => Just(StreamId::Data(4)),
        1 => Just(StreamId::Data(8)),
    ]
}

fn connect() -> impl Strategy<Value = Packet> {
    (
        select(&["", "c1", "client-1"][..]),
        any::<bool>(),
        select(&[0u16, 5, 30][..]),
        select(&[None, Some(0u32), Some(60)][..]),
        select(&[None, Some(1u16), Some(2), Some(100)][..]),
        select(&[None, Some(20u32), Some(64), Some(1000)][..]),
        select(&[None, Some(0u16), Some(2)][..]),
        any::<bool>(),
        proptest::option::of((
            select(TOPICS),
            qos(),
            any::<bool>(),
            select(&[0u32, 10][..]),
        )),
    )
        .prop_map(
            |(id, clean_start, keep_alive, expiry, receive, size, aliases, problems, will)| {
                Packet::from(Connect {
                    clean_start,
                    keep_alive,
                    properties: ConnectProperties {
                        session_expiry_interval: expiry,
                        receive_maximum: receive.and_then(std::num::NonZeroU16::new),
                        maximum_packet_size: size.and_then(std::num::NonZeroU32::new),
                        topic_alias_maximum: aliases,
                        request_problem_information: (!problems).then_some(false),
                        ..ConnectProperties::default()
                    },
                    client_id: id.to_owned(),
                    will: will.map(|(topic, qos, retain, delay)| Will {
                        qos,
                        retain,
                        topic: topic.to_owned(),
                        payload: Bytes::from_static(b"gone"),
                        properties: WillProperties {
                            will_delay_interval: Some(delay),
                            ..WillProperties::default()
                        },
                    }),
                    ..Connect::default()
                })
            },
        )
}

fn publish() -> impl Strategy<Value = Packet> {
    (
        select(TOPICS),
        qos(),
        any::<bool>(),
        any::<bool>(),
        1u16..5,
        select(&[None, Some(1u16), Some(2), Some(65)][..]),
        any::<bool>(),
        select(&[None, Some(0u32), Some(5)][..]),
        proptest::option::of(select(TOPICS)),
    )
        .prop_map(
            |(topic, qos, retain, dup, id, alias, drop_topic, expiry, response)| {
                let topic = if drop_topic && alias.is_some() {
                    ""
                } else {
                    topic
                };
                Packet::Publish(Publish {
                    dup: dup && qos != QoS::AtMostOnce,
                    qos,
                    retain,
                    topic: topic.to_owned(),
                    packet_id: (qos != QoS::AtMostOnce)
                        .then(|| openqtt_codec::PacketId::new(id))
                        .flatten(),
                    properties: PublishProperties {
                        topic_alias: alias.and_then(std::num::NonZeroU16::new),
                        message_expiry_interval: expiry,
                        response_topic: response.map(str::to_owned),
                        ..PublishProperties::default()
                    },
                    payload: Bytes::from_static(b"payload"),
                })
            },
        )
}

fn acknowledgement() -> impl Strategy<Value = Packet> {
    let id = (1u16..7).prop_map(|id| openqtt_codec::PacketId::new(id).expect("non-zero"));
    (id, 0u8..4, any::<bool>()).prop_map(|(packet_id, kind, failure)| match kind {
        0 => Packet::PubAck(PubAck {
            reason_code: if failure {
                PubAckReasonCode::UnspecifiedError
            } else {
                PubAckReasonCode::Success
            },
            ..PubAck::new(packet_id)
        }),
        1 => Packet::PubRec(PubRec {
            reason_code: if failure {
                PubRecReasonCode::NotAuthorized
            } else {
                PubRecReasonCode::Success
            },
            ..PubRec::new(packet_id)
        }),
        2 => Packet::PubRel(PubRel::new(packet_id)),
        _ => Packet::PubComp(PubComp::new(packet_id)),
    })
}

fn subscribe() -> impl Strategy<Value = Packet> {
    let filter = (select(TOPICS), qos(), any::<bool>(), any::<bool>(), 0u8..3).prop_map(
        |(filter, maximum_qos, no_local, retain_as_published, handling)| Subscription {
            filter: filter.to_owned(),
            options: SubscriptionOptions {
                maximum_qos,
                no_local,
                retain_as_published,
                retain_handling: openqtt_codec::RetainHandling::from_u8(handling)
                    .unwrap_or_default(),
            },
        },
    );
    (1u16..5, vec(filter, 1..4), proptest::option::of(1u32..3)).prop_map(
        |(id, subscriptions, identifier)| {
            Packet::Subscribe(Subscribe {
                packet_id: openqtt_codec::PacketId::new(id).expect("non-zero"),
                properties: SubscribeProperties {
                    subscription_identifier: identifier.and_then(std::num::NonZeroU32::new),
                    ..SubscribeProperties::default()
                },
                subscriptions,
            })
        },
    )
}

fn unsubscribe() -> impl Strategy<Value = Packet> {
    (1u16..5, vec(select(TOPICS), 1..3)).prop_map(|(id, filters)| {
        Packet::Unsubscribe(Unsubscribe {
            packet_id: openqtt_codec::PacketId::new(id).expect("non-zero"),
            properties: UnsubscribeProperties::default(),
            filters: filters.into_iter().map(str::to_owned).collect(),
        })
    })
}

fn other_packet() -> impl Strategy<Value = Packet> {
    prop_oneof![
        Just(Packet::PingReq),
        (
            select(&[0u8, 4, 0x80][..]),
            select(&[None, Some(0u32), Some(30)][..])
        )
            .prop_map(|(code, expiry)| Packet::Disconnect(Disconnect {
                reason_code: DisconnectReasonCode::from_u8(code).unwrap_or_default(),
                properties: DisconnectProperties {
                    session_expiry_interval: expiry,
                    ..DisconnectProperties::default()
                },
            })),
        (any::<bool>(), any::<bool>()).prop_map(|(again, method)| Packet::Auth(Auth {
            reason_code: if again {
                AuthReasonCode::ReAuthenticate
            } else {
                AuthReasonCode::ContinueAuthentication
            },
            properties: AuthProperties {
                authentication_method: method.then(|| "M".to_owned()),
                ..AuthProperties::default()
            },
        })),
    ]
}

fn client_packet() -> impl Strategy<Value = Packet> {
    prop_oneof![
        1 => connect(),
        8 => publish(),
        5 => acknowledgement(),
        3 => subscribe(),
        1 => unsubscribe(),
        2 => other_packet(),
    ]
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        12 => (stream(), client_packet(), prop::bool::weighted(0.05))
            .prop_map(|(stream, packet, early)| Step::Client { stream, packet, early }),
        1 => (stream(), proptest::option::of(select(&[PacketType::Connect, PacketType::Publish][..])))
            .prop_map(|(stream, packet_type)| Step::Garbage { stream, packet_type }),
        3 => (0u8..8).prop_map(Step::Authenticate),
        4 => any::<u8>().prop_map(Step::Authorize),
        3 => (0u8..8).prop_map(Step::Claim),
        4 => (any::<usize>(), 0u8..6).prop_map(|(pick, outcome)| Step::Commit { pick, outcome }),
        4 => (select(NAMES), 0u8..3, any::<bool>(), any::<bool>(), select(&[None, Some(0u32), Some(3)][..]), any::<u8>())
            .prop_map(|(topic, qos, retain, own, expiry, pick)| Step::Deliver { topic, qos, retain, own, expiry, pick }),
        2 => (0u8..3).prop_map(|count| Step::Retained { count }),
        4 => (any::<usize>(), prop::bool::weighted(0.1))
            .prop_map(|(pick, failure)| Step::Acknowledge { pick, failure }),
        2 => (0u32..60_000).prop_map(Step::Advance),
        1 => any::<bool>().prop_map(Step::Handshake),
        1 => (select(&[4u64, 8][..]), any::<bool>()).prop_map(|(stream, finished)| Step::StreamEnded(stream, finished)),
        1 => any::<bool>().prop_map(Step::TakeOver),
        1 => (0u8..4).prop_map(Step::Shutdown),
        1 => Just(Step::TransportClosed),
    ]
}

/// A CONNECT a session can be accepted for: no will to refuse, and a Maximum Packet Size the
/// CONNACK fits in.
fn calm_connect() -> impl Strategy<Value = Connect> {
    (
        select(&["", "client-1"][..]),
        select(&[30u16, 60][..]),
        select(&[None, Some(60u32)][..]),
        select(&[None, Some(1u16), Some(2), Some(5)][..]),
        select(&[None, Some(64u32), Some(200), Some(1000)][..]),
        select(&[None, Some(0u16), Some(2)][..]),
    )
        .prop_map(|(id, keep_alive, expiry, receive, size, aliases)| Connect {
            clean_start: true,
            keep_alive,
            properties: ConnectProperties {
                session_expiry_interval: expiry,
                receive_maximum: receive.and_then(std::num::NonZeroU16::new),
                maximum_packet_size: size.and_then(std::num::NonZeroU32::new),
                topic_alias_maximum: aliases,
                ..ConnectProperties::default()
            },
            client_id: id.to_owned(),
            ..Connect::default()
        })
}

/// A PUBLISH with a Topic Alias the server accepts, which still may name an invalid topic.
fn calm_publish() -> impl Strategy<Value = Packet> {
    (
        select(TOPICS),
        qos(),
        any::<bool>(),
        1u16..9,
        select(&[None, None, Some(1u16), Some(2)][..]),
        select(&[None, Some(0u32), Some(5)][..]),
    )
        .prop_map(|(topic, qos, retain, id, alias, expiry)| {
            Packet::Publish(Publish {
                qos,
                retain,
                topic: topic.to_owned(),
                packet_id: (qos != QoS::AtMostOnce)
                    .then(|| openqtt_codec::PacketId::new(id))
                    .flatten(),
                properties: PublishProperties {
                    topic_alias: alias.and_then(std::num::NonZeroU16::new),
                    message_expiry_interval: expiry,
                    ..PublishProperties::default()
                },
                payload: Bytes::from_static(b"payload"),
                ..Publish::default()
            })
        })
}

/// What a well-behaved client sends once connected: no CONNECT, AUTH or DISCONNECT.
fn calm_packet() -> impl Strategy<Value = Packet> {
    prop_oneof![
        8 => calm_publish(),
        3 => acknowledgement(),
        4 => subscribe(),
        1 => unsubscribe(),
        1 => Just(Packet::PingReq),
    ]
}

/// What happens to a connected session: no order ends it, and requests are answered often.
fn calm_step() -> impl Strategy<Value = Step> {
    prop_oneof![
        10 => (prop_oneof![9 => Just(StreamId::Control), 1 => stream()], calm_packet())
            .prop_map(|(stream, packet)| Step::Client { stream, packet, early: false }),
        5 => (32u8..=255).prop_map(Step::Authorize),
        5 => (any::<usize>(), 0u8..8).prop_map(|(pick, outcome)| Step::Commit { pick, outcome }),
        6 => (select(NAMES), 0u8..3, any::<bool>(), any::<bool>(), select(&[None, Some(0u32), Some(3)][..]), any::<u8>())
            .prop_map(|(topic, qos, retain, own, expiry, pick)| Step::Deliver { topic, qos, retain, own, expiry, pick }),
        5 => (0u8..3).prop_map(|count| Step::Retained { count }),
        8 => (any::<usize>(), prop::bool::weighted(0.1))
            .prop_map(|(pick, failure)| Step::Acknowledge { pick, failure }),
        1 => (0u32..20_000).prop_map(Step::Advance),
    ]
}

/// Authorization answers that allow and deny one action (see [`Step::Authorize`]).
const ALLOW: u8 = 32;
const DENY: u8 = 33;

/// The client sends a PUBLISH on `stream`, with `id` unless at QoS 0.
fn client_publish(stream: StreamId, qos: QoS, id: u16, dup: bool) -> Step {
    Step::Client {
        stream,
        packet: Packet::Publish(Publish {
            dup: dup && qos != QoS::AtMostOnce,
            qos,
            topic: "t".to_owned(),
            packet_id: (qos != QoS::AtMostOnce)
                .then(|| openqtt_codec::PacketId::new(id))
                .flatten(),
            payload: Bytes::from_static(b"payload"),
            ..Publish::default()
        }),
        early: false,
    }
}

/// The client sends a PUBREL on `stream`.
fn client_pubrel(stream: StreamId, id: u16) -> Step {
    Step::Client {
        stream,
        packet: Packet::PubRel(PubRel::new(
            openqtt_codec::PacketId::new(id).expect("non-zero"),
        )),
        early: false,
    }
}

/// What a client does to its QoS 2 exchanges, densely: PUBLISH packets with two identifiers,
/// repeated and spread over three streams, PUBREL packets in and out of turn, and QoS 0 and 1
/// PUBLISH packets that hold the machine on their authorizations and commits, with the answers
/// in any order.
fn exchange_step() -> impl Strategy<Value = Step> {
    let stream = || {
        prop_oneof![
            2 => Just(StreamId::Control),
            1 => Just(StreamId::Data(4)),
            1 => Just(StreamId::Data(8)),
        ]
    };
    // QoS 1 keeps to identifiers of its own, so that every QoS 2 PUBLISH with 7 or 8 is one
    // exchange's or another's.
    let publish = (stream(), qos(), 0u16..2, any::<bool>()).prop_map(|(stream, qos, n, dup)| {
        let id = if qos == QoS::ExactlyOnce {
            7 + n
        } else {
            1 + n
        };
        client_publish(stream, qos, id, dup)
    });
    let pubrel = (stream(), 7u16..9).prop_map(|(stream, id)| client_pubrel(stream, id));
    prop_oneof![
        8 => publish,
        2 => pubrel,
        4 => (32u8..=255).prop_map(Step::Authorize),
        3 => (any::<usize>(), 0u8..6).prop_map(|(pick, outcome)| Step::Commit { pick, outcome }),
        2 => any::<usize>().prop_map(|pick| Step::Acknowledge { pick, failure: false }),
    ]
}

/// Sequences reviews found the machine wrong on, each with the server's Receive Maximum, for a
/// connected session.
fn reviewed_sequences() -> Vec<(u16, Vec<Step>)> {
    use StreamId::{Control, Data};
    let accept = |pick| Step::Commit { pick, outcome: 3 };
    vec![
        // A repeat while its first PUBLISH is being authorized.
        (
            1,
            vec![
                client_publish(Control, QoS::ExactlyOnce, 7, false),
                client_publish(Control, QoS::ExactlyOnce, 7, true),
                Step::Authorize(ALLOW),
                accept(0),
            ],
        ),
        // A PUBLISH and its repeat queued behind another's authorization.
        (
            1,
            vec![
                client_publish(Control, QoS::AtMostOnce, 0, false),
                client_publish(Control, QoS::ExactlyOnce, 7, false),
                client_publish(Control, QoS::ExactlyOnce, 7, true),
                Step::Authorize(ALLOW),
                Step::Authorize(ALLOW),
                accept(0),
            ],
        ),
        // A new PUBLISH behind a PUBREL with its identifier, and a repeat of it.
        (
            2,
            vec![
                client_publish(Control, QoS::ExactlyOnce, 7, false),
                Step::Authorize(ALLOW),
                accept(0),
                client_publish(Control, QoS::AtMostOnce, 0, false),
                client_pubrel(Control, 7),
                client_publish(Control, QoS::ExactlyOnce, 7, false),
                client_publish(Control, QoS::ExactlyOnce, 7, true),
                Step::Authorize(ALLOW),
                Step::Authorize(ALLOW),
                accept(0),
            ],
        ),
        // An older exchange's refusal, held behind a commit on stream 4, goes out while a newer
        // exchange with the identifier is refused behind one on stream 8; then the newer one's
        // first PUBLISH is repeated.
        (
            8,
            vec![
                client_publish(Data(4), QoS::AtLeastOnce, 1, false),
                Step::Authorize(ALLOW),
                client_publish(Data(8), QoS::AtLeastOnce, 2, false),
                Step::Authorize(ALLOW),
                client_publish(Control, QoS::ExactlyOnce, 7, false),
                client_publish(Data(4), QoS::ExactlyOnce, 7, true),
                Step::Authorize(DENY),
                client_publish(Data(8), QoS::ExactlyOnce, 7, false),
                Step::Authorize(DENY),
                accept(0),
                client_publish(Control, QoS::ExactlyOnce, 7, true),
                Step::Authorize(ALLOW),
                accept(1),
                accept(0),
            ],
        ),
    ]
}

/// A sequence a review found, with random steps woven in, so that its neighbours are explored.
fn neighbour() -> impl Strategy<Value = (u16, Vec<Step>)> {
    let count = reviewed_sequences().len();
    (
        0..count,
        vec((any::<proptest::sample::Index>(), exchange_step()), 0..12),
    )
        .prop_map(|(which, woven)| {
            let (receive_maximum, mut steps) = reviewed_sequences().swap_remove(which);
            for (at, step) in woven {
                let at = at.index(steps.len() + 1);
                steps.insert(at, step);
            }
            (receive_maximum, steps)
        })
}

/// A script: whether the connection starts in 0-RTT, the first packet, then the steps.
fn script() -> impl Strategy<Value = (bool, Packet, Vec<Step>)> {
    (
        prop::bool::weighted(0.1),
        prop_oneof![9 => connect(), 1 => client_packet()],
        vec(step(), 0..80),
    )
}

/// The edge around the machine, answering only when a step says so, and the invariants.
struct Driver {
    session: Session,
    /// The server's Receive Maximum.
    receive_maximum: u16,
    now: Timestamp,
    authenticating: bool,
    authorizations: VecDeque<crate::Authorization>,
    claim: Option<crate::Claim>,
    commits: Vec<PublishToken>,
    reads: VecDeque<crate::RetainedRead>,
    interest: BTreeMap<TopicFilter, ()>,
    timers: BTreeMap<Timer, Timestamp>,
    /// What the first CONNECT asked for, once the machine read it.
    client: Option<Connect>,
    /// Whether the client's first packet went in.
    started: bool,
    /// Acknowledgements owed for QoS 1 and 2 PUBLISH packets, by kind and identifier.
    owed: BTreeMap<(PacketType, u16), i64>,
    connacks: usize,
    accepted: bool,
    silenced: bool,
    closes: usize,
    claimed: usize,
    releases: usize,
    sent_before_connack: Vec<PacketType>,
    /// QoS 1 and 2 PUBLISH packets the client sent before it had a CONNACK, which are not held
    /// to a Receive Maximum it could not know.
    pipelined: u16,
    /// Data streams whose server side was finished.
    finished: Vec<u64>,
    /// Whether the machine asked the transport to stop reading.
    paused: bool,
    /// Whether the driver honours a pause as a transport does, holding the client's packets
    /// until the machine resumes reading. Otherwise it keeps feeding them.
    honour_pause: bool,
    /// Packets the client sent while reading was paused.
    unread: VecDeque<(StreamId, Packet, bool)>,
    /// What the server sent at QoS 1 and 2 and the client has not acknowledged in full, by
    /// identifier, with the packet the client answers with next. Only to aim acknowledgements.
    out: BTreeMap<u16, PacketType>,
    /// QoS 2 messages of the client's that the server acknowledged with PUBREC.
    releasable: Vec<u16>,
    /// QoS 1 and 2 PUBLISH packets each stream has not answered, in the order they arrived.
    unanswered: BTreeMap<StreamId, VecDeque<Unanswered>>,
    /// PUBREL packets not answered by PUBCOMP, by identifier in the order they arrived, each
    /// with the exchange that was the latest when it came.
    pubrels: BTreeMap<u16, VecDeque<Option<u64>>>,
    /// The client's latest QoS 2 exchange with each identifier.
    exchanges: BTreeMap<u16, Exchange>,
    next_exchange: u64,
    /// How the server answered each exchange the model is sure of: accepted, or refused with
    /// this code.
    outcomes: BTreeMap<u64, Result<(), PubRecReasonCode>>,
    /// Whether the PUBLISH going in certainly repeats an open exchange, so that it may not be
    /// taken for one PUBLISH too many.
    repeat_arriving: bool,
    /// Whether the handshake completed, so that no early data can be rejected any more.
    handshake_done: bool,
    /// Whether early data was rejected and the connection went on, after which the model no
    /// longer knows which packets the server holds.
    lost: bool,
}

/// A QoS 1 or 2 PUBLISH from the client that its stream has not answered. A stream's are
/// answered in the order they arrived (report R1, O15), so every acknowledgement is matched to
/// the PUBLISH it answers.
#[derive(Debug, Clone, Copy)]
struct Unanswered {
    qos: QoS,
    id: u16,
    /// At QoS 2, the exchange the model puts it in, and whether the model is sure of it.
    exchange: u64,
    certain: bool,
}

/// The latest QoS 2 exchange of the client's with an identifier, as the model follows it from
/// the client's packets and the server's answers. A PUBLISH with the identifier arriving while
/// it is open repeats it.
#[derive(Debug, Clone, Copy)]
struct Exchange {
    number: u64,
    /// Whether the server certainly took its first PUBLISH for a new exchange: none came before
    /// with the identifier, or the one before had certainly ended.
    certain: bool,
    /// Whether a PUBREC refusing one of its PUBLISH packets, or a PUBCOMP releasing it, went
    /// out: a PUBLISH arriving now is a new message ([MQTT-4.3.3-9], [MQTT-4.3.3-12]).
    ended: bool,
    /// Whether a PUBREC accepting one of its PUBLISH packets went out.
    accepted: bool,
    /// Whether a PUBREL arrived since its first PUBLISH, which may end it before a PUBLISH
    /// after it is processed, depending on its commit.
    released: bool,
    /// Whether one arrived once it was accepted, which certainly ends it first.
    released_for_certain: bool,
}

impl Driver {
    fn new(early: bool, receive_maximum: u16) -> Self {
        let now = at(0);
        let peer = Peer {
            handshake_complete: !early,
            ..Peer::new(99)
        };
        let config = Config {
            receive_maximum: NonZeroU16::new(receive_maximum).unwrap_or(NonZeroU16::MIN),
            ..Config::default()
        };
        let (session, effects) = Session::new(config, peer, now);
        let mut driver = Self {
            session,
            receive_maximum,
            now,
            authenticating: false,
            authorizations: VecDeque::new(),
            claim: None,
            commits: Vec::new(),
            reads: VecDeque::new(),
            interest: BTreeMap::new(),
            timers: BTreeMap::new(),
            client: None,
            started: false,
            owed: BTreeMap::new(),
            connacks: 0,
            accepted: false,
            silenced: false,
            closes: 0,
            claimed: 0,
            releases: 0,
            sent_before_connack: Vec::new(),
            pipelined: 0,
            finished: Vec::new(),
            paused: false,
            honour_pause: false,
            unread: VecDeque::new(),
            out: BTreeMap::new(),
            releasable: Vec::new(),
            unanswered: BTreeMap::new(),
            pubrels: BTreeMap::new(),
            exchanges: BTreeMap::new(),
            next_exchange: 0,
            outcomes: BTreeMap::new(),
            repeat_arriving: false,
            handshake_done: !early,
            lost: false,
        };
        driver.check(effects.into_vec());
        driver
    }

    /// A driver whose client connected with `first` and is past authentication and the claim,
    /// with pauses honoured.
    fn connected(first: Connect, receive_maximum: u16) -> Self {
        let mut driver = Self::new(false, receive_maximum);
        driver.honour_pause = true;
        driver.client(StreamId::Control, Packet::from(first), false);
        driver.step(Step::Authenticate(7));
        driver.step(Step::Claim(7));
        assert!(driver.session.is_connected());
        driver
    }

    /// Feeds an input and checks what comes out.
    fn feed(&mut self, input: Input) {
        let effects = self.session.handle(input, self.now);
        self.check(effects.into_vec());
        self.check_state();
    }

    /// The client sends a packet, as a decoder would hand it over.
    fn client(&mut self, stream: StreamId, packet: Packet, early: bool) {
        let mut buffer = BytesMut::new();
        if packet.encode(&mut buffer).is_err() {
            return;
        }
        let decoder = Decoder::new()
            .with_sender(Sender::Client)
            .with_max_packet_size(Config::default().maximum_packet_size);
        let decoded = match decoder.decode(&mut buffer) {
            Ok(Some(decoded)) => decoded,
            Ok(None) => unreachable!("a whole packet was encoded"),
            Err(error) => {
                return self.feed(Input::DecodeError {
                    stream,
                    error,
                    packet_type: Some(packet.packet_type()),
                });
            }
        };
        assert_eq!(decoded, packet, "the codec round trips what it encodes");
        if !self.started && !self.session.is_closed() {
            self.started = true;
            if let (StreamId::Control, Packet::Connect(connect)) = (stream, &decoded) {
                self.client = Some((**connect).clone());
            }
        }
        if !self.session.is_closed()
            && let Packet::Publish(publish) = &decoded
            && let Some(packet_id) = publish.packet_id
        {
            let kind = if publish.qos == QoS::AtLeastOnce {
                PacketType::PubAck
            } else {
                PacketType::PubRec
            };
            *self.owed.entry((kind, packet_id.get())).or_default() += 1;
            if self.connacks == 0 {
                self.pipelined = self.pipelined.saturating_add(1);
            }
        }
        if !self.session.is_closed() && !self.lost {
            self.arrive(stream, &decoded);
        }
        self.feed(Input::Packet {
            stream,
            packet: decoded,
            early,
        });
        self.repeat_arriving = false;
    }

    /// Follows a packet from the client into the model as it arrives.
    fn arrive(&mut self, stream: StreamId, packet: &Packet) {
        match packet {
            Packet::Publish(publish) => {
                let Some(packet_id) = publish.packet_id else {
                    return;
                };
                let id = packet_id.get();
                let (exchange, certain) = if publish.qos == QoS::ExactlyOnce {
                    self.arrive_exactly_once(id)
                } else {
                    (0, false)
                };
                self.unanswered
                    .entry(stream)
                    .or_default()
                    .push_back(Unanswered {
                        qos: publish.qos,
                        id,
                        exchange,
                        certain,
                    });
            }
            Packet::PubRel(pubrel) => {
                let id = pubrel.packet_id.get();
                let latest = self.exchanges.get_mut(&id).map(|exchange| {
                    exchange.released = true;
                    exchange.released_for_certain |= exchange.accepted;
                    exchange.number
                });
                self.pubrels.entry(id).or_default().push_back(latest);
            }
            _ => {}
        }
    }

    /// A QoS 2 PUBLISH arrives: a repeat of the latest exchange with its identifier while that
    /// is open, else the first of a new one.
    fn arrive_exactly_once(&mut self, id: u16) -> (u64, bool) {
        let latest = self.exchanges.get(&id).copied();
        if let Some(exchange) = latest
            && !exchange.ended
            && !exchange.released
        {
            self.repeat_arriving = exchange.certain;
            return (exchange.number, exchange.certain);
        }
        let certain = latest.is_none_or(|exchange| exchange.ended || exchange.released_for_certain);
        let number = self.next_exchange;
        self.next_exchange += 1;
        self.exchanges.insert(
            id,
            Exchange {
                number,
                certain,
                ended: false,
                accepted: false,
                released: false,
                released_for_certain: false,
            },
        );
        (number, certain)
    }

    /// The server answered a QoS 1 or 2 PUBLISH on `stream`: the one that arrived first there.
    fn answered(&mut self, stream: StreamId, qos: QoS, id: u16) -> Option<Unanswered> {
        if self.lost {
            return None;
        }
        let answered = self
            .unanswered
            .get_mut(&stream)
            .and_then(VecDeque::pop_front)
            .unwrap_or_else(|| {
                panic!("an acknowledgement of {id} answering nothing on {stream:?}")
            });
        assert_eq!(
            (answered.qos, answered.id),
            (qos, id),
            "acknowledgements on {stream:?} go in the order their PUBLISH packets arrived"
        );
        Some(answered)
    }

    /// The server answered a QoS 2 PUBLISH of an exchange: all of an exchange's alike.
    fn exchange_answered(&mut self, answered: Unanswered, outcome: Result<(), PubRecReasonCode>) {
        if answered.certain {
            let first = *self.outcomes.entry(answered.exchange).or_insert(outcome);
            assert_eq!(
                first, outcome,
                "the PUBLISH packets of one QoS 2 exchange are answered alike: {answered:?}"
            );
        }
        if let Some(exchange) = self.exchanges.get_mut(&answered.id)
            && exchange.number == answered.exchange
        {
            if outcome.is_err() {
                exchange.ended = true;
            } else {
                exchange.accepted = true;
            }
        }
    }

    /// The handshake completes. What came in early data is dropped if it is rejected.
    fn handshake(&mut self, accepted: bool) {
        let rejected = !self.handshake_done && !accepted;
        self.handshake_done = true;
        self.feed(Input::HandshakeComplete {
            early_data_accepted: accepted,
        });
        if rejected && !self.session.is_closed() {
            self.lost = true;
        }
    }

    fn step(&mut self, step: Step) {
        self.act(step);
        // A transport reads again what it held back once the machine resumes.
        while !self.paused
            && let Some((stream, packet, early)) = self.unread.pop_front()
        {
            self.client(stream, packet, early);
        }
    }

    fn act(&mut self, step: Step) {
        match step {
            Step::Client {
                stream,
                packet,
                early,
            } if self.paused && self.honour_pause => {
                self.unread.push_back((stream, packet, early));
            }
            Step::Client {
                stream,
                packet,
                early,
            } => self.client(stream, packet, early),
            Step::Garbage {
                stream,
                packet_type,
            } => self.feed(Input::DecodeError {
                stream,
                error: openqtt_codec::Error::Truncated {
                    field: "Topic Name",
                },
                packet_type,
            }),
            Step::Authenticate(answer) => {
                if std::mem::take(&mut self.authenticating) {
                    let result = match answer {
                        0 => AuthResult::Failure(ConnectReasonCode::NotAuthorized),
                        1 => AuthResult::Continue { data: None },
                        _ => AuthResult::Success { data: None },
                    };
                    self.feed(Input::Authenticated(result));
                }
            }
            Step::Authorize(bits) => {
                if let Some(request) = self.authorizations.pop_front() {
                    let decisions = (0..request.actions.len())
                        .map(|i| {
                            if bits
                                .checked_shr(u32::try_from(i).unwrap_or(31))
                                .unwrap_or(0)
                                & 1
                                == 1
                                || bits > 200
                            {
                                Decision::Deny
                            } else {
                                Decision::Allow
                            }
                        })
                        .collect();
                    self.feed(Input::Authorized {
                        request: request.request,
                        decisions,
                    });
                }
            }
            Step::Claim(answer) => {
                if let Some(claim) = self.claim.take() {
                    let result = match answer {
                        0 => ClaimResult::Refused(ConnectReasonCode::ServerBusy),
                        1 if claim.assigned => ClaimResult::Taken,
                        _ => ClaimResult::Claimed { session: None },
                    };
                    if matches!(result, ClaimResult::Claimed { .. }) {
                        self.claimed += 1;
                    }
                    self.feed(Input::Claimed(result));
                }
            }
            Step::Commit { pick, outcome } => {
                if !self.commits.is_empty() {
                    let token = self.commits.remove(pick % self.commits.len());
                    let outcome = match outcome {
                        0 => PublishOutcome::QuotaExceeded,
                        1 => PublishOutcome::Failed,
                        2 => PublishOutcome::Accepted { matched: false },
                        _ => PublishOutcome::Accepted { matched: true },
                    };
                    self.feed(Input::Committed { token, outcome });
                }
            }
            Step::Deliver {
                topic,
                qos,
                retain,
                own,
                expiry,
                pick,
            } => {
                let Ok(topic) = TopicName::new(topic) else {
                    return;
                };
                let mut message = Message::new(topic, Bytes::from_static(b"delivered"));
                message.qos = openqtt_core::QoS::from_u8(qos).unwrap_or_default();
                message.retain = retain;
                message.publisher = self
                    .session
                    .client_id()
                    .filter(|_| own)
                    .cloned()
                    .or_else(|| ClientId::new("other").ok());
                message.expiry = expiry.map(|interval| Deadline::after(self.now, interval));
                let subscriptions: Vec<TopicFilter> = self
                    .interest
                    .keys()
                    .enumerate()
                    .filter(|(i, filter)| {
                        filter.matches(&message.topic) && (usize::from(pick) >> (i % 8)) & 1 == 0
                    })
                    .map(|(_, filter)| filter.clone())
                    .collect();
                self.feed(Input::Deliver(Delivery {
                    message,
                    subscriptions,
                }));
            }
            Step::Retained { count } => {
                if let Some(read) = self.reads.pop_front() {
                    let messages = (0..count)
                        .filter_map(|n| {
                            let topic = TopicName::new(&format!("a/{n}")).ok()?;
                            let mut message = Message::new(topic, Bytes::from_static(b"kept"));
                            message.retain = true;
                            message.qos = openqtt_core::QoS::AtLeastOnce;
                            Some(message)
                        })
                        .collect();
                    self.feed(Input::Retained { read, messages });
                }
            }
            Step::Acknowledge { pick, failure } => {
                let candidates = self.out.len() + self.releasable.len();
                if candidates == 0 {
                    return;
                }
                let pick = pick % candidates;
                let packet = if let Some((&id, &next)) = self.out.iter().nth(pick) {
                    let packet_id = openqtt_codec::PacketId::new(id).expect("non-zero");
                    match next {
                        PacketType::PubAck => {
                            self.out.remove(&id);
                            Packet::PubAck(PubAck {
                                reason_code: if failure {
                                    PubAckReasonCode::UnspecifiedError
                                } else {
                                    PubAckReasonCode::Success
                                },
                                ..PubAck::new(packet_id)
                            })
                        }
                        PacketType::PubRec if failure => {
                            self.out.remove(&id);
                            Packet::PubRec(PubRec {
                                reason_code: PubRecReasonCode::NotAuthorized,
                                ..PubRec::new(packet_id)
                            })
                        }
                        PacketType::PubRec => Packet::PubRec(PubRec::new(packet_id)),
                        _ => {
                            self.out.remove(&id);
                            Packet::PubComp(PubComp::new(packet_id))
                        }
                    }
                } else {
                    let id = self.releasable.remove(pick - self.out.len());
                    Packet::PubRel(PubRel::new(
                        openqtt_codec::PacketId::new(id).expect("non-zero"),
                    ))
                };
                self.client(StreamId::Control, packet, false);
            }
            Step::Advance(millis) => {
                let until = self
                    .now
                    .saturating_add(std::time::Duration::from_millis(u64::from(millis)));
                while let Some((timer, when)) = self
                    .timers
                    .iter()
                    .filter(|(_, when)| **when <= until)
                    .min_by_key(|(_, when)| **when)
                    .map(|(timer, when)| (*timer, *when))
                {
                    self.timers.remove(&timer);
                    self.now = self.now.max(when);
                    self.feed(Input::Timer(timer));
                }
                self.now = until;
            }
            Step::Handshake(accepted) => self.handshake(accepted),
            Step::StreamEnded(stream, finished) => self.feed(Input::StreamEnded {
                stream,
                end: if finished {
                    StreamEnd::ClientFinished
                } else {
                    StreamEnd::ServerStopped
                },
            }),
            Step::TakeOver(session_ends) => self.feed(Input::StepDown { session_ends }),
            Step::Shutdown(kind) => self.feed(Input::Shutdown(match kind {
                0 => Shutdown::ServerShuttingDown,
                1 => Shutdown::UseAnotherServer {
                    server_reference: Some("elsewhere".into()),
                },
                2 => Shutdown::ServerMoved {
                    server_reference: None,
                },
                _ => Shutdown::AdministrativeAction,
            })),
            Step::TransportClosed => self.feed(Input::TransportClosed),
        }
    }

    /// Answers everything outstanding favourably, until nothing is.
    fn settle(&mut self) {
        for _ in 0..1_000 {
            if self.authenticating {
                self.step(Step::Authenticate(7));
            } else if !self.authorizations.is_empty() {
                self.step(Step::Authorize(u8::MAX));
            } else if self.claim.is_some() {
                self.step(Step::Claim(7));
            } else if !self.commits.is_empty() {
                self.step(Step::Commit {
                    pick: 0,
                    outcome: 3,
                });
            } else if !self.reads.is_empty() {
                self.step(Step::Retained { count: 0 });
            } else {
                self.handshake(true);
                if self.authenticating
                    || !self.authorizations.is_empty()
                    || self.claim.is_some()
                    || !self.commits.is_empty()
                    || !self.reads.is_empty()
                {
                    continue;
                }
                return;
            }
        }
        panic!("the machine kept asking");
    }

    /// The invariants over one call's effects.
    fn check(&mut self, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Send { stream, packet } => self.check_sent(stream, &packet),
                Effect::SendRefusal(_) => {
                    assert!(!self.silenced && self.connacks == 0);
                    self.silenced = true;
                }
                Effect::Close(_) => {
                    self.closes += 1;
                    assert_eq!(self.closes, 1, "the connection closes once");
                }
                Effect::Authenticate(_) => {
                    assert!(!self.authenticating, "one authentication at a time");
                    self.authenticating = true;
                }
                Effect::Authorize(request) => self.authorizations.push_back(request),
                Effect::Claim(claim) => {
                    assert!(self.claim.is_none(), "one claim at a time");
                    self.claim = Some(claim);
                }
                Effect::Release(_) => {
                    self.releases += 1;
                    assert!(
                        self.releases <= self.claimed,
                        "only a claim held is released"
                    );
                }
                Effect::Publish(publication) => {
                    // A receipt exactly for a QoS 2 message, for the log to keep with it.
                    assert_eq!(
                        publication.receipt.is_some(),
                        publication.message.qos == openqtt_core::QoS::ExactlyOnce,
                        "{publication:?}"
                    );
                    if let Some(token) = publication.token {
                        self.commits.push(token);
                    }
                }
                Effect::Subscribe(interest) => {
                    if let Some(read) = interest.retained {
                        self.reads.push_back(read);
                    }
                    self.interest.insert(interest.filter, ());
                }
                Effect::Unsubscribe(filter) => {
                    self.interest.remove(&filter);
                }
                Effect::SetTimer { timer, at } => {
                    self.timers.insert(timer, at);
                }
                Effect::CancelTimer(timer) => {
                    self.timers.remove(&timer);
                }
                Effect::FinishStream(stream) => {
                    assert!(!self.finished.contains(&stream), "a stream finished twice");
                    self.finished.push(stream);
                }
                Effect::PauseReading => {
                    assert!(!self.paused, "paused twice");
                    self.paused = true;
                }
                Effect::ResumeReading => {
                    assert!(self.paused, "resumed without a pause");
                    self.paused = false;
                }
                Effect::Will(_) | Effect::Count(_) | Effect::ReleaseReceipt(_) => {}
            }
        }
    }

    /// The invariants over one packet the server sent.
    fn check_sent(&mut self, stream: StreamId, packet: &Packet) {
        assert!(
            !self.silenced,
            "nothing after a DISCONNECT or a refusal: {packet:?}"
        );
        assert_eq!(self.closes, 0, "nothing after the close: {packet:?}");
        if let StreamId::Data(id) = stream {
            assert!(
                !self.finished.contains(&id),
                "nothing on a finished stream: {packet:?}"
            );
        }
        packet
            .check_sender(Sender::Server)
            .unwrap_or_else(|error| panic!("{packet:?} is not a server's: {error}"));
        let mut buffer = BytesMut::new();
        packet
            .encode(&mut buffer)
            .unwrap_or_else(|error| panic!("{packet:?} does not encode: {error}"));
        let maximum = self
            .client
            .as_ref()
            .and_then(|connect| connect.properties.maximum_packet_size)
            .map_or(MAX_PACKET_SIZE, std::num::NonZeroU32::get);
        assert!(
            u32::try_from(buffer.len()).is_ok_and(|size| size <= maximum),
            "{packet:?} is over the client's Maximum Packet Size {maximum}"
        );
        let decoded = Decoder::new()
            .with_sender(Sender::Server)
            .decode(&mut buffer)
            .unwrap_or_else(|error| panic!("{packet:?} does not decode: {error}"));
        assert_eq!(decoded.as_ref(), Some(packet));
        match packet {
            Packet::ConnAck(connack) => {
                self.connacks += 1;
                assert_eq!(self.connacks, 1, "one CONNACK");
                assert!(
                    self.sent_before_connack
                        .iter()
                        .all(|kind| *kind == PacketType::Auth)
                );
                if connack.reason_code.is_error() {
                    assert!(!connack.session_present);
                    self.silenced = true;
                } else {
                    self.accepted = true;
                }
            }
            Packet::Disconnect(disconnect) => {
                assert!(self.accepted, "no DISCONNECT before CONNACK 0x00");
                assert!(
                    !(self.repeat_arriving
                        && disconnect.reason_code == DisconnectReasonCode::ReceiveMaximumExceeded),
                    "a repeat taken for one PUBLISH too many"
                );
                self.silenced = true;
            }
            Packet::PubAck(ack) => {
                self.acknowledged(PacketType::PubAck, ack.packet_id.get());
                self.answered(stream, QoS::AtLeastOnce, ack.packet_id.get());
            }
            Packet::PubRec(rec) => {
                let id = rec.packet_id.get();
                self.acknowledged(PacketType::PubRec, id);
                let outcome = if rec.reason_code.is_error() {
                    Err(rec.reason_code)
                } else {
                    Ok(())
                };
                if let Some(answered) = self.answered(stream, QoS::ExactlyOnce, id) {
                    self.exchange_answered(answered, outcome);
                }
                if outcome.is_ok() {
                    self.releasable.push(id);
                }
            }
            Packet::PubComp(comp) => {
                let id = comp.packet_id.get();
                if !self.lost {
                    let released = self
                        .pubrels
                        .get_mut(&id)
                        .and_then(VecDeque::pop_front)
                        .unwrap_or_else(|| panic!("PUBCOMP {id} answering no PUBREL"));
                    // A PUBREL that released something released the latest exchange then.
                    if comp.reason_code == PubCompReasonCode::Success
                        && let Some(number) = released
                        && let Some(exchange) = self.exchanges.get_mut(&id)
                        && exchange.number == number
                    {
                        exchange.ended = true;
                    }
                }
            }
            Packet::PubRel(rel) => {
                self.out.insert(rel.packet_id.get(), PacketType::PubComp);
            }
            Packet::Publish(publish) => {
                if let Some(packet_id) = publish.packet_id {
                    let next = if publish.qos == QoS::AtLeastOnce {
                        PacketType::PubAck
                    } else {
                        PacketType::PubRec
                    };
                    self.out.insert(packet_id.get(), next);
                }
                assert!(self.accepted, "no PUBLISH before CONNACK 0x00");
                // Sent again only when a session resumes, and none does here.
                assert!(!publish.dup, "{publish:?}");
                if let Some(alias) = publish.properties.topic_alias {
                    assert_eq!(stream, StreamId::Control);
                    let maximum = self
                        .client
                        .as_ref()
                        .and_then(|connect| connect.properties.topic_alias_maximum)
                        .unwrap_or(0)
                        .min(Config::default().topic_alias_maximum);
                    assert!(alias.get() <= maximum, "{publish:?}");
                }
            }
            _ => {}
        }
        if self.connacks == 0 {
            self.sent_before_connack.push(packet.packet_type());
        }
    }

    /// An acknowledgement of a PUBLISH from the client: never more than were sent.
    fn acknowledged(&mut self, kind: PacketType, packet_id: u16) {
        let owed = self.owed.entry((kind, packet_id)).or_default();
        *owed -= 1;
        assert!(
            *owed >= 0,
            "{kind} {packet_id} acknowledged more often than published"
        );
    }

    /// The invariants over the machine's state.
    fn check_state(&self) {
        let receive_maximum = self
            .client
            .as_ref()
            .and_then(|connect| connect.properties.receive_maximum)
            .map_or(u16::MAX, std::num::NonZeroU16::get)
            .min(Config::RECEIVE_MAXIMUM);
        assert!(self.session.in_flight_out() <= usize::from(receive_maximum));
        assert!(
            self.session.in_flight_in() <= self.receive_maximum.saturating_add(self.pipelined),
            "{} in flight from the client, against a Receive Maximum of {} and {} sent before it",
            self.session.in_flight_in(),
            self.receive_maximum,
            self.pipelined
        );
        if let Some(state) = self.session.snapshot() {
            let mut ids: Vec<_> = state
                .outbound
                .iter()
                .filter_map(|stored| stored.packet_id())
                .collect();
            let all = ids.len();
            ids.sort_unstable();
            ids.dedup();
            assert_eq!(ids.len(), all, "a Packet Identifier in flight twice");
        }
    }

    /// Once every request is answered and the connection is still open, every QoS 1 and 2
    /// PUBLISH from the client has its acknowledgement.
    fn check_settled(&self) {
        assert!(self.closes <= 1);
        if self.session.is_closed() {
            assert_eq!(self.closes, 1);
            assert_eq!(self.releases, self.claimed, "every claim held is released");
        }
        if self.session.is_connected() {
            for ((kind, packet_id), owed) in &self.owed {
                assert_eq!(*owed, 0, "{kind} {packet_id} never acknowledged");
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        ..ProptestConfig::default()
    })]

    #[test]
    fn a_connected_session_keeps_the_invariants(
        (first, steps) in (calm_connect(), vec(calm_step(), 0..150)),
        receive_maximum in select(&[1u16, 2, Config::RECEIVE_MAXIMUM][..]),
    ) {
        let mut driver = Driver::new(false, receive_maximum);
        driver.honour_pause = true;
        driver.client(StreamId::Control, Packet::from(first), false);
        driver.step(Step::Authenticate(7));
        driver.step(Step::Claim(7));
        prop_assert!(driver.session.is_connected());
        for step in steps {
            driver.step(step);
        }
        driver.settle();
        driver.check_settled();
    }

    #[test]
    fn qos_2_exchanges_are_answered_alike_and_counted_once(
        first in calm_connect(),
        steps in vec(exchange_step(), 0..150),
        receive_maximum in select(&[1u16, 2, 4][..]),
    ) {
        let mut driver = Driver::connected(first, receive_maximum);
        for step in steps {
            driver.step(step);
        }
        driver.settle();
        driver.check_settled();
    }

    #[test]
    fn neighbours_of_the_reviewed_sequences_keep_the_invariants(
        (receive_maximum, steps) in neighbour(),
    ) {
        let mut driver = Driver::connected(reviewed_connect(), receive_maximum);
        for step in steps {
            driver.step(step);
        }
        driver.settle();
        driver.check_settled();
    }

    #[test]
    fn random_sequences_keep_the_invariants(
        (early, first, steps) in script(),
        receive_maximum in select(&[1u16, 2, Config::RECEIVE_MAXIMUM][..]),
    ) {
        let mut driver = Driver::new(early, receive_maximum);
        driver.client(StreamId::Control, first, early);
        for step in steps {
            driver.step(step);
        }
        driver.settle();
        driver.check_settled();
    }
}

/// The CONNECT of the reviewed sequences.
fn reviewed_connect() -> Connect {
    Connect {
        clean_start: true,
        keep_alive: 60,
        client_id: "client-1".to_owned(),
        ..Connect::default()
    }
}

#[test]
fn the_sequences_reviews_found_keep_the_invariants() {
    for (receive_maximum, steps) in reviewed_sequences() {
        let mut driver = Driver::connected(reviewed_connect(), receive_maximum);
        for step in steps {
            driver.step(step);
        }
        driver.settle();
        driver.check_settled();
    }
}
