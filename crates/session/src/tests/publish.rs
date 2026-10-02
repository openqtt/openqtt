//! Messages from the client: PUBLISH at each QoS, its authorization, commit and refusals, the
//! acknowledgements in arrival order, Receive Maximum, Topic Aliases and PUBREL.

use std::num::NonZeroU16;

use bytes::Bytes;
use openqtt_codec::{
    DisconnectReasonCode, Error, Packet, PacketType, PubAckReasonCode, PubCompReasonCode,
    PubRecReasonCode, Publish, PublishProperties, QoS,
};
use openqtt_core::PayloadFormat;
use openqtt_topic::Mountpoint;

use super::harness::{
    Harness, at, connect_with, id, name, publish, publish0, publish1, publish2, pubrel, retained,
    seconds, subscribe1,
};
use crate::{Action, Config, Counter, Decision, Input, PublishOutcome, PublishToken, StreamId};

/// The one packet the client received.
fn one(packets: Vec<Packet>) -> Packet {
    match <[Packet; 1]>::try_from(packets) {
        Ok([packet]) => packet,
        Err(packets) => panic!("expected one packet, got {packets:?}"),
    }
}

fn puback_code(packet: Packet) -> (u16, PubAckReasonCode) {
    match packet {
        Packet::PubAck(ack) => (ack.packet_id.get(), ack.reason_code),
        other => panic!("expected PUBACK, got {other:?}"),
    }
}

fn pubrec_code(packet: Packet) -> (u16, PubRecReasonCode) {
    match packet {
        Packet::PubRec(rec) => (rec.packet_id.get(), rec.reason_code),
        other => panic!("expected PUBREC, got {other:?}"),
    }
}

/// The token of the last publication waiting for its commit.
fn last_token(harness: &Harness) -> PublishToken {
    harness
        .commits
        .last()
        .and_then(|publication| publication.token)
        .expect("a publication waits for its commit")
}

// covers: MQTT-2.2.1-5
#[test]
fn mqtt_3_3_4_1_a_publish_gets_the_response_its_qos_calls_for() {
    let mut harness = Harness::connected();
    assert!(harness.send(publish0("t", "zero")).is_empty());
    assert_eq!(
        puback_code(one(harness.send(publish1("t", 7, "one")))),
        (7, PubAckReasonCode::NoMatchingSubscribers)
    );
    assert_eq!(
        pubrec_code(one(harness.send(publish2("t", 9, "two")))),
        (9, PubRecReasonCode::NoMatchingSubscribers)
    );
    let Packet::PubComp(comp) = one(harness.send(pubrel(9))) else {
        panic!("PUBCOMP");
    };
    assert_eq!(comp.packet_id, id(9));
    assert_eq!(harness.published().len(), 3);
}

// covers: MQTT-4.3.3-8
#[test]
fn mqtt_4_3_2_4_puback_waits_until_the_message_is_durable() {
    let mut harness = Harness::connected();
    harness.auto.loopback = false;
    assert!(harness.send(publish1("t", 1, "x")).is_empty());
    let token = last_token(&harness);
    let packets = harness.input(Input::Committed {
        token,
        outcome: PublishOutcome::Accepted { matched: true },
    });
    assert_eq!(puback_code(one(packets)), (1, PubAckReasonCode::Success));

    assert!(harness.send(publish2("t", 2, "x")).is_empty());
    let token = last_token(&harness);
    let packets = harness.input(Input::Committed {
        token,
        outcome: PublishOutcome::Accepted { matched: true },
    });
    assert_eq!(pubrec_code(one(packets)), (2, PubRecReasonCode::Success));
}

#[test]
fn r1_o15_acknowledgements_leave_in_arrival_order() {
    let mut harness = Harness::connected();
    harness.auto.loopback = false;
    harness.send(publish1("a", 1, "x"));
    let first = last_token(&harness);
    harness.send(publish2("b", 2, "x"));
    let second = last_token(&harness);
    harness.send(publish1("c", 3, "x"));
    let third = last_token(&harness);
    // The third commits first, then the second: both wait for the first.
    let accepted = PublishOutcome::Accepted { matched: true };
    assert!(
        harness
            .input(Input::Committed {
                token: third,
                outcome: accepted
            })
            .is_empty()
    );
    assert!(
        harness
            .input(Input::Committed {
                token: second,
                outcome: accepted
            })
            .is_empty()
    );
    let packets = harness.input(Input::Committed {
        token: first,
        outcome: accepted,
    });
    let ids: Vec<_> = packets
        .iter()
        .map(|packet| match packet {
            Packet::PubAck(ack) => ack.packet_id.get(),
            Packet::PubRec(rec) => rec.packet_id.get(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(ids, [1, 2, 3]);
}

#[test]
fn mqtt_4_3_2_5_after_its_puback_an_identifier_is_a_new_message() {
    let mut harness = Harness::connected();
    harness.send(publish1("t", 5, "first"));
    let mut again = publish1("t", 5, "second");
    again.dup = true;
    puback_code(one(harness.send(again)));
    // Both are messages: whatever its DUP flag, the second is published too.
    let payloads: Vec<_> = harness
        .published()
        .iter()
        .map(|message| message.payload.clone())
        .collect();
    assert_eq!(
        payloads,
        [Bytes::from_static(b"first"), Bytes::from_static(b"second")]
    );
}

// covers: MQTT-4.3.3-11, MQTT-4.3.3-12
#[test]
fn mqtt_4_3_3_10_a_qos_2_publish_repeated_before_pubrel_gets_pubrec_and_no_second_delivery() {
    let mut harness = Harness::connected();
    pubrec_code(one(harness.send(publish2("t", 4, "once"))));
    let mut repeat = publish2("t", 4, "once");
    repeat.dup = true;
    // Report R1, D8: PUBREC 0x00 again, not 0x91, and no second publication. 0x00 is always
    // allowed (O26), and whether anything matches now is not known without routing again.
    assert_eq!(
        pubrec_code(one(harness.send(repeat))),
        (4, PubRecReasonCode::Success)
    );
    assert_eq!(harness.published().len(), 1);
    // PUBREL completes it with PUBCOMP carrying the identifier.
    let Packet::PubComp(comp) = one(harness.send(pubrel(4))) else {
        panic!("PUBCOMP");
    };
    assert_eq!(
        (comp.packet_id, comp.reason_code),
        (id(4), PubCompReasonCode::Success)
    );
    // After PUBCOMP the identifier is a new message.
    pubrec_code(one(harness.send(publish2("t", 4, "again"))));
    assert_eq!(harness.published().len(), 2);
}

#[test]
fn a_qos_2_publish_repeated_while_it_commits_gets_its_pubrec_after_the_commit() {
    let mut harness = Harness::connected();
    harness.auto.loopback = false;
    harness.send(publish2("t", 4, "once"));
    let token = last_token(&harness);
    assert!(harness.send(publish2("t", 4, "once")).is_empty());
    let packets = harness.input(Input::Committed {
        token,
        outcome: PublishOutcome::Accepted { matched: true },
    });
    assert_eq!(packets.len(), 2);
    for packet in packets {
        assert_eq!(pubrec_code(packet), (4, PubRecReasonCode::Success));
    }
    assert_eq!(harness.published().len(), 1);
    // One slot of the Receive Maximum is held, not two.
    assert_eq!(harness.session.in_flight_in(), 1);
}

/// The state a connection left when it ended with the commit of QoS 2 message 5 outstanding.
fn cut_off_mid_commit() -> crate::SessionState {
    let mut harness = Harness::new();
    harness.connect(connect_with("client-1", |connect| {
        connect.properties = super::harness::expiry(600);
    }));
    harness.auto.loopback = false;
    harness.send(publish2("t", 5, "once"));
    // Every QoS 2 publication carries its receipt, for the log to keep with the message.
    assert_eq!(harness.commits[0].receipt, Some(id(5)));
    harness.input(Input::TransportClosed);
    // The commit answers after the end: too late for this connection.
    let token = harness.commits[0].token.unwrap();
    harness.input(Input::Committed {
        token,
        outcome: PublishOutcome::Accepted { matched: true },
    });
    match &harness.releases()[0].session {
        crate::SessionEnd::Keep(state) => state.clone(),
        crate::SessionEnd::Discard => panic!("kept"),
    }
}

/// A connection resuming `state`, its commits left to the test.
fn resumed(state: crate::SessionState) -> Harness {
    let mut harness = Harness::new();
    harness.stored = Some(state);
    harness.auto.loopback = false;
    let connack = harness.connect(connect_with("client-1", |connect| {
        connect.clean_start = false;
        connect.properties = super::harness::expiry(600);
    }));
    assert!(connack.session_present);
    harness
}

// covers: MQTT-4.3.3-10
#[test]
fn a_qos_2_message_whose_commit_was_cut_off_is_published_again_under_its_receipt() {
    let state = cut_off_mid_commit();
    // The identifier stays reserved: its commit had not answered.
    assert_eq!(state.awaiting_commit, [id(5)]);
    assert!(state.awaiting_release.is_empty());
    let mut harness = resumed(state);
    let mut repeat = publish2("t", 5, "once");
    repeat.dup = true;
    // Published again with its receipt: the log routes it only if the first never committed.
    assert!(harness.send(repeat.clone()).is_empty());
    assert_eq!(harness.commits.len(), 1);
    assert_eq!(harness.commits[0].receipt, Some(id(5)));
    // Another repeat while that commit is out is not published a third time.
    assert!(harness.send(repeat).is_empty());
    assert_eq!(harness.commits.len(), 1);
    let token = harness.commits[0].token.unwrap();
    let packets = harness.input(Input::Committed {
        token,
        outcome: PublishOutcome::Accepted { matched: true },
    });
    assert_eq!(packets.len(), 2);
    for packet in packets {
        assert_eq!(pubrec_code(packet), (5, PubRecReasonCode::Success));
    }
    // PUBREL ends it, and the receipt goes.
    let Packet::PubComp(comp) = one(harness.send(pubrel(5))) else {
        panic!("PUBCOMP");
    };
    assert_eq!(comp.reason_code, PubCompReasonCode::Success);
    assert_eq!(harness.released_receipts(), [5]);
}

#[test]
fn a_refused_repeat_of_a_reserved_identifier_releases_its_receipt() {
    let mut harness = resumed(cut_off_mid_commit());
    harness.auto.authorize = Some(|_| Decision::Deny);
    let mut repeat = publish2("t", 5, "once");
    repeat.dup = true;
    assert_eq!(
        pubrec_code(one(harness.send(repeat))),
        (5, PubRecReasonCode::NotAuthorized)
    );
    // The client takes 5 as free again, so whatever the log holds under it goes.
    assert_eq!(harness.released_receipts(), [5]);
    assert!(
        harness
            .session
            .snapshot()
            .unwrap()
            .awaiting_commit
            .is_empty()
    );
}

#[test]
fn a_pubrel_releases_the_receipt_and_only_qos_2_has_one() {
    let mut harness = Harness::connected();
    harness.auto.loopback = false;
    harness.send(publish1("t", 1, "x"));
    assert_eq!(harness.commits[0].receipt, None);
    harness.send(publish2("t", 2, "x"));
    assert_eq!(harness.commits[1].receipt, Some(id(2)));
    let token = harness.commits[1].token.unwrap();
    harness.input(Input::Committed {
        token,
        outcome: PublishOutcome::Accepted { matched: true },
    });
    assert!(harness.released_receipts().is_empty());
    harness.send(pubrel(2));
    assert_eq!(harness.released_receipts(), [2]);
}

#[test]
fn mqtt_4_3_3_9_after_a_failure_pubrec_an_identifier_is_a_new_message() {
    let mut harness = Harness::connected();
    harness.auto.loopback = false;
    harness.send(publish2("t", 6, "x"));
    let token = last_token(&harness);
    // The retained store refused it (report R1, D15).
    let packets = harness.input(Input::Committed {
        token,
        outcome: PublishOutcome::QuotaExceeded,
    });
    assert_eq!(
        pubrec_code(one(packets)),
        (6, PubRecReasonCode::QuotaExceeded)
    );
    // The identifier is free: the same one is a new message, published again.
    harness.send(publish2("t", 6, "x"));
    assert_eq!(harness.commits.len(), 2);
    // And a PUBREL for the refused one finds nothing.
    let mut harness = Harness::connected();
    harness.auto.authorize = Some(|_| Decision::Deny);
    pubrec_code(one(harness.send(publish2("t", 6, "x"))));
    let Packet::PubComp(comp) = one(harness.send(pubrel(6))) else {
        panic!("PUBCOMP");
    };
    assert_eq!(
        comp.reason_code,
        PubCompReasonCode::PacketIdentifierNotFound
    );
}

#[test]
fn mqtt_4_3_3_13_a_qos_2_exchange_completes_though_the_message_expired() {
    let mut harness = Harness::connected();
    let mut expired = publish2("t", 3, "x");
    expired.properties.message_expiry_interval = Some(0);
    pubrec_code(one(harness.send(expired)));
    // The message is already expired (report R1, O8), and the exchange still completes.
    assert!(harness.published()[0].is_expired(harness.now));
    let Packet::PubComp(comp) = one(harness.send(pubrel(3))) else {
        panic!("PUBCOMP");
    };
    assert_eq!(comp.reason_code, PubCompReasonCode::Success);
}

#[test]
fn r1_d2_a_denied_publish_is_refused_visibly_and_a_denied_qos_0_one_is_counted() {
    let mut harness = Harness::new();
    // Allow the will-less CONNECT, deny every publish.
    harness.auto.authorize = Some(|action| match action {
        Action::Publish { .. } => Decision::Deny,
        Action::Subscribe { .. } => Decision::Allow,
    });
    harness.connect(super::harness::connect("c"));
    assert_eq!(
        puback_code(one(harness.send(publish1("t", 1, "x")))),
        (1, PubAckReasonCode::NotAuthorized)
    );
    assert_eq!(
        pubrec_code(one(harness.send(publish2("t", 2, "x")))),
        (2, PubRecReasonCode::NotAuthorized)
    );
    assert!(harness.send(publish0("t", "x")).is_empty());
    assert_eq!(harness.count(Counter::PublishDenied), 1);
    assert!(harness.published().is_empty());
    // The connection stays.
    assert_eq!(one(harness.send(Packet::PingReq)), Packet::PingResp);
}

// covers: MQTT-3.3.2-2, MQTT-3.3.2-14, MQTT-4.7.0-1
#[test]
fn r1_d32_a_topic_name_that_breaks_section_4_7_refuses_that_publish_alone() {
    let mut harness = Harness::connected();
    assert_eq!(
        puback_code(one(harness.send(publish1("a/+", 1, "x")))),
        (1, PubAckReasonCode::TopicNameInvalid)
    );
    assert_eq!(
        pubrec_code(one(harness.send(publish2("a/#", 2, "x")))),
        (2, PubRecReasonCode::TopicNameInvalid)
    );
    let mut with_response = publish1("a", 3, "x");
    with_response.properties.response_topic = Some("reply/+".into());
    assert_eq!(
        puback_code(one(harness.send(with_response))),
        (3, PubAckReasonCode::TopicNameInvalid)
    );
    assert!(harness.send(publish0("+", "x")).is_empty());
    assert_eq!(harness.count(Counter::PublishTopicInvalid), 1);
    assert!(harness.published().is_empty());
    assert!(harness.session.is_connected());
}

#[test]
fn r1_o16_a_topic_over_128_levels_is_refused_with_0x90() {
    let mut harness = Harness::connected();
    let deep = vec!["l"; 129].join("/");
    assert_eq!(
        puback_code(one(harness.send(publish1(&deep, 1, "x")))),
        (1, PubAckReasonCode::TopicNameInvalid)
    );
    let deepest = vec!["l"; 128].join("/");
    assert_eq!(
        puback_code(one(harness.send(publish1(&deepest, 2, "x")))),
        (2, PubAckReasonCode::NoMatchingSubscribers)
    );
}

#[test]
fn r1_o26_0x10_only_for_a_message_neither_retained_nor_matched() {
    let mut harness = Harness::connected();
    // Nothing matched, not retained: 0x10.
    assert_eq!(
        puback_code(one(harness.send(publish1("t", 1, "x")))),
        (1, PubAckReasonCode::NoMatchingSubscribers)
    );
    // Retained: kept for later subscribers, so 0x00.
    assert_eq!(
        puback_code(one(harness.send(retained(
            "t",
            QoS::AtLeastOnce,
            Some(2),
            "x"
        )))),
        (2, PubAckReasonCode::Success)
    );
    // Matched: 0x00.
    harness.send(subscribe1(3, "t", QoS::AtMostOnce));
    let packets = harness.send(publish1("t", 4, "x"));
    assert!(packets.iter().any(|packet| matches!(
        packet,
        Packet::PubAck(ack) if ack.packet_id == id(4) && ack.reason_code == PubAckReasonCode::Success
    )));
}

#[test]
fn a_commit_that_fails_is_answered_with_0x80() {
    let mut harness = Harness::connected();
    harness.auto.loopback = false;
    harness.send(publish1("t", 1, "x"));
    let token = last_token(&harness);
    let packets = harness.input(Input::Committed {
        token,
        outcome: PublishOutcome::Failed,
    });
    assert_eq!(
        puback_code(one(packets)),
        (1, PubAckReasonCode::UnspecifiedError)
    );
}

// covers: MQTT-3.3.4-7
#[test]
fn r1_d13_more_unacknowledged_publishes_than_receive_maximum_get_disconnect_0x93() {
    let mut harness = Harness::connected();
    harness.auto.loopback = false;
    for packet_id in 1..=32 {
        let qos = if packet_id % 2 == 0 {
            QoS::ExactlyOnce
        } else {
            QoS::AtLeastOnce
        };
        assert!(
            harness
                .send(publish("t", qos, Some(packet_id), "x"))
                .is_empty()
        );
    }
    assert_eq!(harness.session.in_flight_in(), 32);
    // QoS 0 takes no slot.
    assert!(harness.send(publish0("t", "x")).is_empty());
    let Packet::Disconnect(disconnect) = one(harness.send(publish1("t", 33, "x"))) else {
        panic!("DISCONNECT");
    };
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::ReceiveMaximumExceeded
    );
}

#[test]
fn receive_maximum_holds_as_packets_arrive_not_when_they_are_processed() {
    let mut harness = Harness::connected();
    harness.auto.authorize = None;
    // One PUBLISH waits for its authorization, and holds up everything behind it.
    harness.send(publish0("held", "x"));
    for packet_id in 1..=32 {
        assert!(harness.send(publish1("t", packet_id, "x")).is_empty());
    }
    // The 33rd unacknowledged QoS 1 PUBLISH is refused when it arrives.
    let Packet::Disconnect(disconnect) = one(harness.send(publish1("t", 33, "x"))) else {
        panic!("DISCONNECT");
    };
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::ReceiveMaximumExceeded
    );
}

#[test]
fn publishes_sent_before_connack_are_not_held_to_a_limit_the_client_could_not_know() {
    let mut harness = Harness::new();
    harness.auto.claim = false;
    harness.auto.loopback = false;
    harness.send(super::harness::connect("c"));
    // Forty QoS 1 PUBLISH packets behind the CONNECT: the client assumes 65,535 until the
    // CONNACK says 32.
    for packet_id in 1..=40 {
        harness.send(publish1("t", packet_id, "x"));
    }
    let packets = harness.input(Input::Claimed(crate::ClaimResult::Claimed {
        session: None,
    }));
    assert!(matches!(packets[..], [Packet::ConnAck(_)]), "{packets:?}");
    assert!(harness.session.is_connected());
    assert_eq!(harness.session.in_flight_in(), 40);
    // Once the client has the CONNACK it is held to it.
    let Packet::Disconnect(disconnect) = one(harness.send(publish1("t", 41, "x"))) else {
        panic!("DISCONNECT");
    };
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::ReceiveMaximumExceeded
    );
}

#[test]
fn a_slot_frees_with_the_puback_or_the_pubcomp() {
    let mut harness = Harness::connected();
    harness.send(publish1("t", 1, "x"));
    assert_eq!(harness.session.in_flight_in(), 0);
    harness.send(publish2("t", 2, "x"));
    // A QoS 2 message holds its slot until PUBCOMP (section 4.9).
    assert_eq!(harness.session.in_flight_in(), 1);
    harness.send(pubrel(2));
    assert_eq!(harness.session.in_flight_in(), 0);
}

#[test]
fn mqtt_3_2_2_11_a_publish_above_maximum_qos_gets_disconnect_0x9b() {
    let config = Config {
        maximum_qos: QoS::AtLeastOnce,
        ..Config::default()
    };
    let mut harness = Harness::connected_with(config);
    let Packet::Disconnect(disconnect) = one(harness.send(publish2("t", 1, "x"))) else {
        panic!("DISCONNECT");
    };
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::QosNotSupported
    );
}

#[test]
fn mqtt_3_2_2_14_a_retained_publish_without_retain_available_gets_disconnect_0x9a() {
    let config = Config {
        retain_available: false,
        ..Config::default()
    };
    let mut harness = Harness::connected_with(config);
    let Packet::Disconnect(disconnect) =
        one(harness.send(retained("t", QoS::AtMostOnce, None, "x")))
    else {
        panic!("DISCONNECT");
    };
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::RetainNotSupported
    );
}

fn aliased(topic: &str, alias: u16, packet_id: u16) -> Publish {
    Publish {
        properties: PublishProperties {
            topic_alias: NonZeroU16::new(alias),
            ..PublishProperties::default()
        },
        ..publish1(topic, packet_id, "x")
    }
}

#[test]
fn mqtt_3_3_2_12_every_alias_up_to_the_maximum_is_accepted() {
    let mut harness = Harness::connected();
    for alias in [1, 32, 64] {
        let topic = format!("t/{alias}");
        harness.send(aliased(&topic, alias, alias));
        harness.send(aliased("", alias, alias + 100));
    }
    let topics: Vec<_> = harness
        .published()
        .iter()
        .map(|message| message.topic.to_string())
        .collect();
    assert_eq!(topics, ["t/1", "t/1", "t/32", "t/32", "t/64", "t/64"]);
    // A mapping can be replaced.
    harness.send(aliased("u", 1, 200));
    harness.send(aliased("", 1, 201));
    assert_eq!(harness.published().last().unwrap().topic.as_str(), "u");
}

// covers: MQTT-3.3.2-9
#[test]
fn mqtt_3_2_2_17_an_alias_above_the_maximum_gets_disconnect_0x94() {
    let mut harness = Harness::connected();
    let Packet::Disconnect(disconnect) = one(harness.send(aliased("t", 65, 1))) else {
        panic!("DISCONNECT");
    };
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::TopicAliasInvalid
    );
}

#[test]
fn mqtt_3_3_2_8_alias_0_gets_disconnect_0x94() {
    let mut harness = Harness::connected();
    let packets = harness.input(Input::DecodeError {
        stream: StreamId::Control,
        error: Error::ZeroTopicAlias,
        packet_type: Some(PacketType::Publish),
    });
    let Packet::Disconnect(disconnect) = one(packets) else {
        panic!("DISCONNECT");
    };
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::TopicAliasInvalid
    );
}

#[test]
fn an_alias_that_maps_to_nothing_is_a_protocol_error() {
    let mut harness = Harness::connected();
    let Packet::Disconnect(disconnect) = one(harness.send(aliased("", 3, 1))) else {
        panic!("DISCONNECT");
    };
    assert_eq!(disconnect.reason_code, DisconnectReasonCode::ProtocolError);
}

#[test]
fn mqtt_3_3_2_7_aliases_never_carry_over_to_another_connection() {
    let mut harness = Harness::new();
    harness.connect(connect_with("c", |connect| {
        connect.properties = super::harness::expiry(60);
    }));
    harness.send(aliased("t", 1, 1));
    harness.send(openqtt_codec::Disconnect::default());
    let state = match &harness.releases()[0].session {
        crate::SessionEnd::Keep(state) => state.clone(),
        crate::SessionEnd::Discard => panic!("kept"),
    };
    let mut next = Harness::new();
    next.stored = Some(state);
    let connack = next.connect(connect_with("c", |connect| {
        connect.clean_start = false;
        connect.properties = super::harness::expiry(60);
    }));
    assert!(connack.session_present);
    let Packet::Disconnect(disconnect) = one(next.send(aliased("", 1, 2))) else {
        panic!("DISCONNECT");
    };
    assert_eq!(disconnect.reason_code, DisconnectReasonCode::ProtocolError);
}

// covers: MQTT-3.3.2-4, MQTT-3.3.2-15, MQTT-3.3.2-16, MQTT-3.3.2-17, MQTT-3.3.2-20,
// covers: MQTT-3.3.1-3
#[test]
fn a_subscriber_receives_the_properties_unaltered_and_its_own_dup() {
    let mut harness = Harness::connected();
    harness.send(subscribe1(1, "sensors/#", QoS::AtLeastOnce));
    let user_properties: Vec<(String, String)> = vec![
        ("b".into(), "2".into()),
        ("a".into(), "1".into()),
        ("b".into(), "3".into()),
    ];
    let sent = Publish {
        // A retransmission from the client's side.
        dup: true,
        properties: PublishProperties {
            payload_format_indicator: Some(openqtt_codec::PayloadFormat::Utf8),
            message_expiry_interval: Some(30),
            response_topic: Some("reply/to/me".into()),
            correlation_data: Some(Bytes::from_static(b"\x00\x01")),
            user_properties: user_properties.clone(),
            content_type: Some("text/plain".into()),
            ..PublishProperties::default()
        },
        ..publish1("sensors/1", 1, "21.5")
    };
    let packets = harness.send(sent);
    // What the broker was handed.
    let message = harness.published()[0].clone();
    assert_eq!(message.topic, name("sensors/1"));
    assert_eq!(message.qos, openqtt_core::QoS::AtLeastOnce);
    assert_eq!(message.publisher.unwrap().as_str(), "client-1");
    assert_eq!(message.payload_format, Some(PayloadFormat::Utf8));
    // Report R1, O8: the deadline is the moment of receipt plus the interval.
    let deadline = message.expiry.unwrap();
    assert_eq!((deadline.at(), deadline.interval()), (at(30), 30));
    // What the subscriber receives: every property as sent, User Properties in order, and a
    // DUP of its own, which says only whether this PUBLISH is a retransmission.
    let delivered = packets
        .iter()
        .find_map(|packet| match packet {
            Packet::Publish(publish) => Some(publish.clone()),
            _ => None,
        })
        .expect("the subscriber receives it");
    assert!(!delivered.dup);
    assert_eq!(delivered.topic, "sensors/1");
    assert_eq!(delivered.payload, Bytes::from_static(b"21.5"));
    let properties = delivered.properties;
    assert_eq!(
        properties.payload_format_indicator,
        Some(openqtt_codec::PayloadFormat::Utf8)
    );
    assert_eq!(properties.response_topic.as_deref(), Some("reply/to/me"));
    assert_eq!(
        properties.correlation_data,
        Some(Bytes::from_static(b"\x00\x01"))
    );
    assert_eq!(properties.user_properties, user_properties);
    assert_eq!(properties.content_type.as_deref(), Some("text/plain"));
    assert_eq!(properties.message_expiry_interval, Some(30));
}

#[test]
fn r1_o8_the_deadline_runs_from_the_moment_of_receipt() {
    let mut harness = Harness::connected();
    harness.auto.authorize = None;
    // The first PUBLISH waits for its authorization; the second arrives meanwhile, at 0 s.
    harness.send(publish1("a", 1, "x"));
    let mut expiring = publish1("b", 2, "y");
    expiring.properties.message_expiry_interval = Some(10);
    harness.send(expiring);
    harness.advance(seconds(4));
    for _ in 0..2 {
        let request = harness.authorizations.remove(0);
        harness.input(Input::Authorized {
            request: request.request,
            decisions: vec![Decision::Allow],
        });
    }
    // Processed at 4 s, it still expires 10 s after it arrived.
    let deadline = harness.published()[1].expiry.unwrap();
    assert_eq!(deadline.at(), at(10));
}

#[test]
fn r2_rules_6_and_7_a_publish_is_authorized_as_sent_then_mounted() {
    let config = Config {
        mountpoint: Some(Mountpoint::parse("ingest/${username}/").unwrap()),
        ..Config::default()
    };
    let mut harness = Harness::with(config);
    harness.auto.authorize = None;
    harness.connect(connect_with("pump-3", |connect| {
        connect.username = Some("acme/pump-3".into());
    }));
    let mut sent = publish1("temperature", 1, "21.5");
    sent.properties.response_topic = Some("replies/1".into());
    harness.send(sent);
    let request = harness.authorizations.remove(0);
    assert_eq!(
        request.actions,
        [Action::Publish {
            topic: name("temperature"),
            qos: openqtt_core::QoS::AtLeastOnce,
            retain: false,
        }]
    );
    harness.input(Input::Authorized {
        request: request.request,
        decisions: vec![Decision::Allow],
    });
    let message = harness.published()[0].clone();
    assert_eq!(message.topic.as_str(), "ingest/acme/pump-3/temperature");
    // The Response Topic is neither mounted nor stripped ([MQTT-3.3.2-15]).
    assert_eq!(message.response_topic, Some(name("replies/1")));
}

#[test]
fn packets_wait_in_order_while_one_is_authorized() {
    let mut harness = Harness::connected();
    harness.auto.authorize = None;
    harness.send(publish1("a", 1, "x"));
    // A PINGREQ behind it waits too: the order of the client's packets is kept.
    assert!(harness.send(Packet::PingReq).is_empty());
    harness.send(publish0("b", "y"));
    let request = harness.authorizations.remove(0);
    let packets = harness.input(Input::Authorized {
        request: request.request,
        decisions: vec![Decision::Allow],
    });
    assert_eq!(packets[0], Packet::PingResp);
    // The second PUBLISH is authorized only after the first one is published.
    assert_eq!(harness.published().len(), 1);
    let request = harness.authorizations.remove(0);
    harness.input(Input::Authorized {
        request: request.request,
        decisions: vec![Decision::Allow],
    });
    let topics: Vec<_> = harness
        .published()
        .iter()
        .map(|message| message.topic.to_string())
        .collect();
    assert_eq!(topics, ["a", "b"]);
}

#[test]
fn an_answer_to_no_request_changes_nothing() {
    let mut harness = Harness::connected();
    let mark = harness.log.len();
    harness.input(Input::Authorized {
        request: crate::RequestId(999),
        decisions: vec![Decision::Allow],
    });
    harness.input(Input::Committed {
        token: PublishToken(999),
        outcome: PublishOutcome::Failed,
    });
    harness.input(Input::Authenticated(crate::AuthResult::Success {
        data: None,
    }));
    assert!(harness.since(mark).is_empty());
    harness.advance(seconds(1));
    assert!(harness.session.is_connected());
}
