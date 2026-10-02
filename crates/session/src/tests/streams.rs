//! MQTT over QUIC (docs/spec/mqtt-over-quic.md): which packets travel on which stream, a data
//! stream the client ends, and 0-RTT.

use std::num::NonZeroU16;

use openqtt_codec::{DisconnectReasonCode, Packet, Publish, PublishProperties, QoS};
use openqtt_core::QoS as CoreQoS;

use super::harness::{
    Harness, connect, filter, message, puback, publish0, publish1, publish2, subscribe1,
};
use crate::{Config, Delivery, Effect, Input, Peer, PublishOutcome, StreamEnd, StreamId};

const DATA: StreamId = StreamId::Data(4);
const OTHER: StreamId = StreamId::Data(8);

fn protocol_error(packets: &[(StreamId, Packet)]) {
    match packets {
        [(StreamId::Control, Packet::Disconnect(disconnect))] => {
            assert_eq!(disconnect.reason_code, DisconnectReasonCode::ProtocolError);
        }
        other => panic!("expected DISCONNECT 0x82 on the control stream, got {other:?}"),
    }
}

#[test]
fn a_topic_alias_on_a_data_stream_is_a_protocol_error() {
    let mut harness = Harness::connected();
    let aliased = Publish {
        properties: PublishProperties {
            topic_alias: NonZeroU16::new(1),
            ..PublishProperties::default()
        },
        ..publish0("t", "x")
    };
    protocol_error(&harness.send_on(DATA, aliased));
}

#[test]
fn connection_packets_travel_on_the_control_stream_only() {
    for packet in [
        Packet::PingReq,
        Packet::Disconnect(openqtt_codec::Disconnect::default()),
        Packet::from(connect("c")),
    ] {
        let mut harness = Harness::connected();
        protocol_error(&harness.send_on(DATA, packet));
    }
}

#[test]
fn an_acknowledgement_travels_on_the_stream_of_its_packet() {
    let mut harness = Harness::connected();
    let packets = harness.send_on(DATA, publish1("t", 1, "x"));
    assert!(
        matches!(packets[..], [(DATA, Packet::PubAck(_))]),
        "{packets:?}"
    );
    let packets = harness.send_on(OTHER, publish2("t", 2, "x"));
    assert!(
        matches!(packets[..], [(OTHER, Packet::PubRec(_))]),
        "{packets:?}"
    );
    let packets = harness.send_on(OTHER, super::harness::pubrel(2));
    assert!(
        matches!(packets[..], [(OTHER, Packet::PubComp(_))]),
        "{packets:?}"
    );
    let packets = harness.send_on(DATA, subscribe1(3, "t", QoS::AtMostOnce));
    assert!(
        matches!(packets[..], [(DATA, Packet::SubAck(_))]),
        "{packets:?}"
    );
}

#[test]
fn a_delivery_travels_on_the_stream_of_its_subscription() {
    let mut harness = Harness::connected();
    harness.send_on(DATA, subscribe1(1, "t/+", QoS::AtLeastOnce));
    harness.send_on(OTHER, subscribe1(2, "t/#", QoS::AtLeastOnce));
    harness.send_on(StreamId::Control, subscribe1(3, "#", QoS::AtMostOnce));
    let packets = harness.input_on(Input::Deliver(Delivery {
        message: message("t/a", CoreQoS::AtLeastOnce, "x"),
        subscriptions: vec![filter("#"), filter("t/#"), filter("t/+")],
    }));
    // One copy, on the stream of the subscription with the highest QoS, the oldest among
    // equals (section 2.3).
    assert!(
        matches!(packets[..], [(DATA, Packet::Publish(_))]),
        "{packets:?}"
    );
    // Topic Aliases are never sent on a data stream.
    let [(_, Packet::Publish(publish))] = &packets[..] else {
        unreachable!();
    };
    assert_eq!(publish.properties.topic_alias, None);
}

#[test]
fn ending_a_stream_with_an_exchange_open_closes_the_connection() {
    // A QoS 1 delivery on the stream, not yet acknowledged.
    let mut harness = Harness::connected();
    harness.send_on(DATA, subscribe1(1, "t", QoS::AtLeastOnce));
    harness.input(Input::Deliver(Delivery {
        message: message("t", CoreQoS::AtLeastOnce, "x"),
        subscriptions: vec![filter("t")],
    }));
    protocol_error(&harness.input_on(Input::StreamEnded {
        stream: 4,
        end: StreamEnd::ClientFinished,
    }));

    // A QoS 2 message from the client on the stream, whose PUBREL has not come.
    let mut harness = Harness::connected();
    harness.send_on(DATA, publish2("t", 1, "x"));
    protocol_error(&harness.input_on(Input::StreamEnded {
        stream: 4,
        end: StreamEnd::ClientFinished,
    }));
}

#[test]
fn ending_a_stream_with_a_qos_2_publish_not_yet_processed_closes_the_connection() {
    // Still being authorized: once published, it would wait for a PUBREL the stream can no
    // longer carry, and hold its slot of Receive Maximum for good.
    let mut harness = Harness::connected();
    harness.auto.authorize = None;
    harness.send_on(DATA, publish2("t", 1, "x"));
    protocol_error(&harness.input_on(Input::StreamEnded {
        stream: 4,
        end: StreamEnd::ClientFinished,
    }));

    // Still queued behind another packet's authorization.
    let mut harness = Harness::connected();
    harness.auto.authorize = None;
    harness.send(publish0("a", "x"));
    harness.send_on(DATA, publish2("t", 1, "x"));
    protocol_error(&harness.input_on(Input::StreamEnded {
        stream: 4,
        end: StreamEnd::ClientFinished,
    }));

    // A QoS 1 PUBLISH needs nothing more from the client: only the stream ends.
    let mut harness = Harness::connected();
    harness.auto.authorize = None;
    harness.send_on(DATA, publish1("t", 1, "x"));
    assert!(
        harness
            .input_on(Input::StreamEnded {
                stream: 4,
                end: StreamEnd::ClientFinished,
            })
            .is_empty()
    );
    assert!(harness.session.is_connected());
}

#[test]
fn a_stream_ended_in_order_moves_its_subscriptions_to_the_control_stream() {
    let mut harness = Harness::connected();
    harness.send_on(DATA, subscribe1(1, "t", QoS::AtLeastOnce));
    harness.send_on(DATA, publish2("u", 2, "x"));
    harness.send_on(DATA, super::harness::pubrel(2));
    let mark = harness.log.len();
    assert!(
        harness
            .input_on(Input::StreamEnded {
                stream: 4,
                end: StreamEnd::ClientFinished,
            })
            .is_empty()
    );
    assert!(harness.since(mark).contains(&Effect::FinishStream(4)));
    let packets = harness.input_on(Input::Deliver(Delivery {
        message: message("t", CoreQoS::AtLeastOnce, "x"),
        subscriptions: vec![filter("t")],
    }));
    assert!(
        matches!(packets[..], [(StreamId::Control, Packet::Publish(_))]),
        "{packets:?}"
    );
    harness.send(puback(1));
    assert!(harness.session.is_connected());
}

#[test]
fn an_owed_acknowledgement_goes_out_before_the_server_finishes_its_side() {
    let mut harness = Harness::connected();
    harness.auto.loopback = false;
    harness.send_on(DATA, publish1("t", 1, "x"));
    let mark = harness.log.len();
    assert!(
        harness
            .input_on(Input::StreamEnded {
                stream: 4,
                end: StreamEnd::ClientFinished,
            })
            .is_empty()
    );
    assert!(!harness.since(mark).contains(&Effect::FinishStream(4)));
    let token = harness.commits[0].token.unwrap();
    let packets = harness.input_on(Input::Committed {
        token,
        outcome: PublishOutcome::Accepted { matched: true },
    });
    assert!(
        matches!(packets[..], [(DATA, Packet::PubAck(_))]),
        "{packets:?}"
    );
    assert_eq!(harness.log.last(), Some(&Effect::FinishStream(4)));
}

#[test]
fn an_acknowledgement_owed_on_a_stopped_stream_closes_the_connection() {
    let mut harness = Harness::connected();
    harness.auto.loopback = false;
    harness.send_on(DATA, publish1("t", 1, "x"));
    protocol_error(&harness.input_on(Input::StreamEnded {
        stream: 4,
        end: StreamEnd::ServerStopped,
    }));

    // With nothing owed, only the stream ends.
    let mut harness = Harness::connected();
    harness.send_on(DATA, subscribe1(1, "t", QoS::AtMostOnce));
    assert!(
        harness
            .input_on(Input::StreamEnded {
                stream: 4,
                end: StreamEnd::ServerStopped,
            })
            .is_empty()
    );
    // A later acknowledgement that cannot go on it closes the connection then.
    protocol_error(&harness.send_on(DATA, subscribe1(2, "u", QoS::AtMostOnce)));
}

#[test]
fn a_qos_2_repeat_on_another_stream_gets_its_pubrec_with_the_first() {
    let mut harness = Harness::connected();
    harness.auto.loopback = false;
    harness.send_on(DATA, publish2("t", 4, "x"));
    // Identifiers are the session's, not a stream's: this is the same message again.
    assert!(
        harness
            .send_on(StreamId::Control, publish2("t", 4, "x"))
            .is_empty()
    );
    let token = harness.commits[0].token.unwrap();
    let packets = harness.input_on(Input::Committed {
        token,
        outcome: PublishOutcome::Accepted { matched: true },
    });
    assert!(
        matches!(
            packets[..],
            [
                (DATA, Packet::PubRec(_)),
                (StreamId::Control, Packet::PubRec(_))
            ]
        ),
        "{packets:?}"
    );
    assert_eq!(harness.published().len(), 1);
}

#[test]
fn a_packet_on_a_stream_the_client_finished_is_a_protocol_error() {
    let mut harness = Harness::connected();
    harness.input_on(Input::StreamEnded {
        stream: 4,
        end: StreamEnd::ClientFinished,
    });
    protocol_error(&harness.send_on(DATA, publish0("t", "x")));
}

#[test]
fn a_data_stream_opened_before_connack_waits_for_it() {
    let mut harness = Harness::new();
    harness.auto.claim = false;
    harness.send(connect("c"));
    assert!(harness.send_on(DATA, publish1("t", 1, "x")).is_empty());
    let packets = harness.input_on(Input::Claimed(crate::ClaimResult::Claimed {
        session: None,
    }));
    assert!(
        matches!(
            packets[..],
            [
                (StreamId::Control, Packet::ConnAck(_)),
                (DATA, Packet::PubAck(_))
            ]
        ),
        "{packets:?}"
    );
}

/// A machine for a connection accepted in 0-RTT, its handshake not complete.
fn early() -> Harness {
    Harness::with_peer(
        Config::default(),
        Peer {
            handshake_complete: false,
            ..Peer::new(0)
        },
    )
}

fn early_packet(packet: impl Into<Packet>) -> Input {
    Input::Packet {
        stream: StreamId::Control,
        packet: packet.into(),
        early: true,
    }
}

#[test]
fn connack_waits_for_the_handshake_and_early_publishes_wait_with_it() {
    let mut harness = early();
    assert!(harness.input(early_packet(connect("c"))).is_empty());
    // The CONNECT was acted on: authenticated and claimed.
    assert!(
        harness
            .log
            .iter()
            .any(|effect| matches!(effect, Effect::Claim(_)))
    );
    // A PUBLISH or SUBSCRIBE in 0-RTT data is not acted on: it could be a replay.
    assert!(
        harness
            .input(early_packet(publish1("t", 1, "x")))
            .is_empty()
    );
    assert!(
        harness
            .input(early_packet(subscribe1(2, "t", QoS::AtMostOnce)))
            .is_empty()
    );
    assert!(harness.published().is_empty());
    assert!(
        !harness
            .log
            .iter()
            .any(|effect| matches!(effect, Effect::Authorize(_)))
    );
    let packets = harness.input(Input::HandshakeComplete {
        early_data_accepted: true,
    });
    assert!(
        matches!(
            packets[..],
            [Packet::ConnAck(_), Packet::PubAck(_), Packet::SubAck(_)]
        ),
        "{packets:?}"
    );
    assert_eq!(harness.published().len(), 1);
}

#[test]
fn rejected_early_data_is_dropped() {
    let mut harness = early();
    harness.send(connect("c"));
    assert!(
        harness
            .input(early_packet(publish0("t", "replayed")))
            .is_empty()
    );
    let packets = harness.input(Input::HandshakeComplete {
        early_data_accepted: false,
    });
    assert!(matches!(packets[..], [Packet::ConnAck(_)]), "{packets:?}");
    assert!(harness.published().is_empty());

    // A CONNECT that came in rejected early data leaves nothing to go on with.
    let mut harness = early();
    harness.input(early_packet(connect("c")));
    assert!(
        harness
            .input(Input::HandshakeComplete {
                early_data_accepted: false,
            })
            .is_empty()
    );
    assert!(harness.session.is_closed());
}
