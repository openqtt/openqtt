//! Messages to the client: QoS, Packet Identifiers, Receive Maximum, Maximum Packet Size, Topic
//! Aliases, expiry, No Local, Retain As Published, overlapping subscriptions, the mountpoint,
//! the queue, the client's acknowledgements and the resends of a resumed session.

use std::num::{NonZeroU16, NonZeroU32};
use std::time::Duration;

use openqtt_codec::{
    DisconnectReasonCode, Packet, PubAck, PubAckReasonCode, PubRec, PubRecReasonCode,
    PubRelReasonCode, QoS, SubscriptionOptions,
};
use openqtt_core::{ClientId, Deadline, QoS as CoreQoS, SubOpts, SubscriptionId};
use openqtt_topic::Mountpoint;

use super::harness::{
    Harness, at, connect_with, expiry, filter, id, message, options, payloads, puback, pubcomp,
    publishes, pubrec, seconds, subscribe, subscribe1, unsubscribe,
};
use crate::{
    Config, Counter, Delivery, Effect, Input, SessionEnd, SessionState, StoredOutbound,
    StoredSubscription, WillOrder,
};

/// Delivers a message from another client on `topic`, naming `filters`.
fn deliver(
    harness: &mut Harness,
    topic: &str,
    qos: CoreQoS,
    payload: &str,
    filters: &[&str],
) -> Vec<Packet> {
    harness.input(Input::Deliver(Delivery {
        message: message(topic, qos, payload),
        subscriptions: filters.iter().map(|text| filter(text)).collect(),
    }))
}

/// A session connected with a Receive Maximum of `receive_maximum` and a subscription to `t/#`
/// at QoS 2.
fn subscribed(receive_maximum: u16) -> Harness {
    let mut harness = Harness::new();
    harness.connect(connect_with("client-1", |connect| {
        connect.properties.receive_maximum = NonZeroU16::new(receive_maximum);
    }));
    harness.send(subscribe1(1, "t/#", QoS::ExactlyOnce));
    harness
}

#[test]
fn mqtt_4_3_1_1_a_qos_0_delivery_goes_out_at_qos_0_with_dup_0() {
    let mut harness = Harness::connected();
    harness.send(subscribe1(1, "t", QoS::AtMostOnce));
    let packets = deliver(&mut harness, "t", CoreQoS::AtLeastOnce, "x", &["t"]);
    let [publish] = publishes(&packets)[..] else {
        panic!("{packets:?}");
    };
    assert_eq!(
        (publish.qos, publish.dup, publish.packet_id),
        (QoS::AtMostOnce, false, None)
    );
    assert_eq!(harness.session.in_flight_out(), 0);
}

#[test]
fn mqtt_4_3_2_2_a_qos_1_delivery_goes_out_with_dup_0_and_stays_until_its_puback() {
    // covers: MQTT-4.3.2-1, MQTT-4.3.2-3, MQTT-2.2.1-4
    let mut harness = subscribed(32);
    let packets = deliver(&mut harness, "t/a", CoreQoS::AtLeastOnce, "one", &["t/#"]);
    let [first] = publishes(&packets)[..] else {
        panic!("{packets:?}");
    };
    assert_eq!(
        (first.qos, first.dup, first.packet_id),
        (QoS::AtLeastOnce, false, Some(id(1)))
    );
    let packets = deliver(&mut harness, "t/b", CoreQoS::AtLeastOnce, "two", &["t/#"]);
    // A different identifier while the first is in flight.
    assert_eq!(publishes(&packets)[0].packet_id, Some(id(2)));
    assert_eq!(harness.session.in_flight_out(), 2);
    assert!(harness.send(puback(1)).is_empty());
    assert_eq!(harness.session.in_flight_out(), 1);
    // A PUBACK for nothing in flight is counted and changes nothing.
    harness.send(puback(1));
    assert_eq!(harness.count(Counter::UnknownAcknowledgement), 1);
    assert_eq!(harness.session.in_flight_out(), 1);
}

#[test]
fn mqtt_4_3_3_2_a_qos_2_delivery_takes_pubrec_pubrel_and_pubcomp() {
    // covers: MQTT-4.3.3-1, MQTT-4.3.3-3, MQTT-4.3.3-4, MQTT-4.3.3-5, MQTT-2.2.1-5
    let mut harness = subscribed(32);
    let packets = deliver(&mut harness, "t/a", CoreQoS::ExactlyOnce, "x", &["t/#"]);
    let [publish] = publishes(&packets)[..] else {
        panic!("{packets:?}");
    };
    assert_eq!(
        (publish.qos, publish.dup, publish.packet_id),
        (QoS::ExactlyOnce, false, Some(id(1)))
    );
    // PUBREC with a code below 0x80 gets a PUBREL with the same identifier.
    let packets = harness.send(pubrec(1));
    let [Packet::PubRel(pubrel)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(
        (pubrel.packet_id, pubrel.reason_code),
        (id(1), PubRelReasonCode::Success)
    );
    // The PUBREL stays unacknowledged, holding its slot, until PUBCOMP.
    assert_eq!(harness.session.in_flight_out(), 1);
    harness.send(pubcomp(1));
    assert_eq!(harness.session.in_flight_out(), 0);
}

#[test]
fn mqtt_3_6_2_1_a_repeated_pubrec_gets_pubrel_0x00_again() {
    let mut harness = subscribed(32);
    deliver(&mut harness, "t/a", CoreQoS::ExactlyOnce, "x", &["t/#"]);
    harness.send(pubrec(1));
    // Report R1, D10: never 0x91, which is not a PUBREL code.
    let packets = harness.send(pubrec(1));
    let [Packet::PubRel(pubrel)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(pubrel.reason_code, PubRelReasonCode::Success);
    // A PUBREC for nothing in flight gets PUBREL 0x92.
    let packets = harness.send(pubrec(9));
    let [Packet::PubRel(pubrel)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(
        pubrel.reason_code,
        PubRelReasonCode::PacketIdentifierNotFound
    );
}

#[test]
fn mqtt_4_4_0_2_a_failure_acknowledgement_ends_the_exchange() {
    // covers: MQTT-4.3.3-4
    let mut harness = subscribed(32);
    deliver(&mut harness, "t/a", CoreQoS::ExactlyOnce, "x", &["t/#"]);
    deliver(&mut harness, "t/b", CoreQoS::AtLeastOnce, "y", &["t/#"]);
    // Report R1, D9: a failure PUBREC gets no PUBREL, and the identifier is free.
    let refused = PubRec {
        reason_code: PubRecReasonCode::NotAuthorized,
        ..pubrec(1)
    };
    assert!(harness.send(refused).is_empty());
    let failed = PubAck {
        reason_code: PubAckReasonCode::UnspecifiedError,
        ..puback(2)
    };
    assert!(harness.send(failed).is_empty());
    assert_eq!(harness.session.in_flight_out(), 0);
    // Neither is sent again: the session holds nothing in flight.
    assert!(harness.session.snapshot().unwrap().outbound.is_empty());
}

#[test]
fn mqtt_3_3_4_9_no_more_qos_1_and_2_in_flight_than_the_clients_receive_maximum() {
    // covers: MQTT-3.3.4-10, MQTT-4.9.0-1, MQTT-4.9.0-2, MQTT-4.9.0-3
    let mut harness = subscribed(2);
    let mut sent = Vec::new();
    for payload in ["1", "2", "3", "4"] {
        sent.extend(payloads(&deliver(
            &mut harness,
            "t/a",
            CoreQoS::AtLeastOnce,
            payload,
            &["t/#"],
        )));
    }
    // The quota starts at the Receive Maximum and stops at zero.
    assert_eq!(sent, ["1", "2"]);
    assert_eq!(harness.session.in_flight_out(), 2);
    assert_eq!(harness.session.queued(), 2);
    // At a quota of zero every other packet is still processed and answered.
    assert_eq!(harness.send(Packet::PingReq), [Packet::PingResp]);
    assert!(matches!(
        harness.send(subscribe1(2, "u", QoS::AtMostOnce))[..],
        [Packet::SubAck(_)]
    ));
    // Each PUBACK frees a slot for the next, in order.
    assert_eq!(payloads(&harness.send(puback(1))), ["3"]);
    assert_eq!(payloads(&harness.send(puback(2))), ["4"]);
}

#[test]
fn r1_o3_the_window_is_the_clients_receive_maximum_capped_at_32() {
    let mut harness = subscribed(100);
    for n in 0..40 {
        deliver(
            &mut harness,
            "t/a",
            CoreQoS::AtLeastOnce,
            &n.to_string(),
            &["t/#"],
        );
    }
    assert_eq!(harness.session.in_flight_out(), 32);
    assert_eq!(harness.session.queued(), 8);
}

#[test]
fn mqtt_3_1_2_24_a_delivery_too_large_for_the_client_is_discarded_as_if_sent() {
    // covers: MQTT-3.1.2-25
    let mut harness = Harness::new();
    harness.connect(connect_with("client-1", |connect| {
        connect.properties.maximum_packet_size = NonZeroU32::new(20);
    }));
    harness.send(subscribe1(1, "t", QoS::AtLeastOnce));
    // A QoS 1 PUBLISH to `t` is 2 + 3 + 2 + 1 bytes before its payload: 12 bytes of payload
    // make exactly 20, which is sent (report R1, D6), and 13 make 21, which is not.
    let packets = deliver(
        &mut harness,
        "t",
        CoreQoS::AtLeastOnce,
        "123456789012",
        &["t"],
    );
    let [publish] = publishes(&packets)[..] else {
        panic!("{packets:?}");
    };
    assert_eq!(publish.encoded_len(), Ok(20));
    let packets = deliver(
        &mut harness,
        "t",
        CoreQoS::AtLeastOnce,
        "1234567890123",
        &["t"],
    );
    assert!(packets.is_empty());
    assert_eq!(harness.count(Counter::DeliveryTooLarge), 1);
    // It took no slot and no identifier: only the first is in flight.
    assert_eq!(harness.session.in_flight_out(), 1);
    let packets = deliver(&mut harness, "t", CoreQoS::AtLeastOnce, "x", &["t"]);
    assert_eq!(publishes(&packets)[0].packet_id, Some(id(2)));
}

#[test]
fn mqtt_3_1_2_26_aliases_go_up_to_the_clients_maximum_and_are_never_remapped() {
    // covers: MQTT-3.3.2-11
    let mut harness = Harness::new();
    harness.connect(connect_with("client-1", |connect| {
        connect.properties.topic_alias_maximum = Some(2);
    }));
    harness.send(subscribe1(1, "#", QoS::AtMostOnce));
    let mut seen = Vec::new();
    for topic in ["a", "b", "c", "a", "b", "c"] {
        let packets = deliver(&mut harness, topic, CoreQoS::AtMostOnce, "x", &["#"]);
        let publish = publishes(&packets)[0].clone();
        seen.push((
            publish.topic,
            publish.properties.topic_alias.map(NonZeroU16::get),
        ));
    }
    let expected = [
        ("a", Some(1)),
        ("b", Some(2)),
        ("c", None),
        ("", Some(1)),
        ("", Some(2)),
        ("c", None),
    ];
    let expected: Vec<_> = expected
        .iter()
        .map(|(topic, alias)| ((*topic).to_owned(), *alias))
        .collect();
    assert_eq!(seen, expected);
}

#[test]
fn mqtt_3_1_2_27_no_alias_for_a_client_without_a_topic_alias_maximum() {
    for maximum in [None, Some(0)] {
        let mut harness = Harness::new();
        harness.connect(connect_with("client-1", |connect| {
            connect.properties.topic_alias_maximum = maximum;
        }));
        harness.send(subscribe1(1, "#", QoS::AtMostOnce));
        for _ in 0..2 {
            let packets = deliver(&mut harness, "a", CoreQoS::AtMostOnce, "x", &["#"]);
            let publish = publishes(&packets)[0].clone();
            assert_eq!(
                (publish.topic.as_str(), publish.properties.topic_alias),
                ("a", None)
            );
        }
    }
}

#[test]
fn r1_o6_the_server_assigns_at_most_its_own_maximum() {
    let config = Config {
        topic_alias_maximum: 1,
        ..Config::default()
    };
    let mut harness = Harness::with(config);
    harness.connect(connect_with("client-1", |connect| {
        connect.properties.topic_alias_maximum = Some(100);
    }));
    harness.send(subscribe1(1, "#", QoS::AtMostOnce));
    deliver(&mut harness, "a", CoreQoS::AtMostOnce, "x", &["#"]);
    let packets = deliver(&mut harness, "b", CoreQoS::AtMostOnce, "x", &["#"]);
    assert_eq!(publishes(&packets)[0].properties.topic_alias, None);
}

#[test]
fn mqtt_3_3_2_5_a_copy_expired_before_its_delivery_started_is_deleted() {
    // covers: MQTT-3.3.2-6
    let mut harness = subscribed(1);
    // Received 10 s before now with an interval of 10: expired from its deadline on.
    let mut stale = message("t/a", CoreQoS::AtLeastOnce, "stale");
    stale.expiry = Some(Deadline::after(at(0).saturating_add(Duration::ZERO), 0));
    let packets = harness.input(Input::Deliver(Delivery {
        message: stale,
        subscriptions: vec![filter("t/#")],
    }));
    assert!(packets.is_empty());
    assert_eq!(harness.count(Counter::DeliveryExpired), 1);

    // Received now with 10 s, sent 3.5 s later: 6.5 s left, sent as 7, rounded up.
    harness.advance(Duration::from_millis(500));
    let mut fresh = message("t/a", CoreQoS::AtLeastOnce, "fresh");
    fresh.expiry = Some(Deadline::after(at(0), 10));
    harness.now = at(3).saturating_add(Duration::from_millis(500));
    let packets = harness.input(Input::Deliver(Delivery {
        message: fresh,
        subscriptions: vec![filter("t/#")],
    }));
    let publish = publishes(&packets)[0].clone();
    assert_eq!(publish.properties.message_expiry_interval, Some(7));

    // One that expires while it waits for a slot is deleted when its turn comes.
    let mut waiting = message("t/a", CoreQoS::AtLeastOnce, "waiting");
    waiting.expiry = Some(Deadline::after(harness.now, 2));
    harness.input(Input::Deliver(Delivery {
        message: waiting,
        subscriptions: vec![filter("t/#")],
    }));
    assert_eq!(harness.session.queued(), 1);
    harness.advance(seconds(2));
    assert!(harness.send(puback(1)).is_empty());
    assert_eq!(harness.count(Counter::DeliveryExpired), 2);
    assert_eq!(harness.session.queued(), 0);
}

#[test]
fn mqtt_3_8_4_8_a_delivery_goes_out_at_the_lower_of_the_published_and_granted_qos() {
    let mut harness = Harness::connected();
    harness.send(subscribe1(1, "one", QoS::AtLeastOnce));
    harness.send(subscribe1(2, "two", QoS::ExactlyOnce));
    let packets = deliver(&mut harness, "one", CoreQoS::ExactlyOnce, "x", &["one"]);
    assert_eq!(publishes(&packets)[0].qos, QoS::AtLeastOnce);
    let packets = deliver(&mut harness, "two", CoreQoS::AtMostOnce, "x", &["two"]);
    assert_eq!(publishes(&packets)[0].qos, QoS::AtMostOnce);
}

#[test]
fn mqtt_3_8_3_3_no_local_holds_back_the_clients_own_messages() {
    let mut harness = Harness::connected();
    let no_local = SubscriptionOptions {
        no_local: true,
        ..options(QoS::AtMostOnce)
    };
    harness.send(subscribe(1, &[("t", no_local)]));
    // The client's own message, looped back by the broker, is not delivered to it.
    let packets = harness.send(super::harness::publish0("t", "mine"));
    assert!(publishes(&packets).is_empty());
    assert_eq!(harness.count(Counter::DeliveryUnmatched), 1);
    // Another client's is.
    let packets = deliver(&mut harness, "t", CoreQoS::AtMostOnce, "theirs", &["t"]);
    assert_eq!(payloads(&packets), ["theirs"]);
    // A message the platform published has no publisher, and is delivered too.
    let mut platform = message("t", CoreQoS::AtMostOnce, "platform");
    platform.publisher = None;
    let packets = harness.input(Input::Deliver(Delivery {
        message: platform,
        subscriptions: vec![filter("t")],
    }));
    assert_eq!(payloads(&packets), ["platform"]);
}

#[test]
fn mqtt_3_3_1_12_retain_as_published_0_forwards_retain_0() {
    // covers: MQTT-3.3.1-13
    let mut harness = Harness::connected();
    let as_published = SubscriptionOptions {
        retain_as_published: true,
        ..options(QoS::AtMostOnce)
    };
    harness.send(subscribe(1, &[("plain", options(QoS::AtMostOnce))]));
    harness.send(subscribe(2, &[("kept", as_published)]));
    for (topic, expected) in [("plain", false), ("kept", true)] {
        let mut live = message(topic, CoreQoS::AtMostOnce, "x");
        live.retain = true;
        let packets = harness.input(Input::Deliver(Delivery {
            message: live,
            subscriptions: vec![filter(topic)],
        }));
        assert_eq!(publishes(&packets)[0].retain, expected, "{topic}");
    }
}

#[test]
fn r1_o11_overlapping_subscriptions_deliver_one_copy_at_the_highest_qos() {
    // covers: MQTT-3.3.4-2, MQTT-3.3.4-3, MQTT-3.3.4-4
    let mut harness = Harness::connected();
    let mut identified = subscribe1(1, "t/+", QoS::AtLeastOnce);
    identified.properties.subscription_identifier = NonZeroU32::new(7);
    harness.send(identified);
    let mut other = subscribe1(2, "t/#", QoS::ExactlyOnce);
    other.properties.subscription_identifier = NonZeroU32::new(268_435_455);
    harness.send(other);
    harness.send(subscribe1(3, "#", QoS::AtMostOnce));
    let packets = deliver(
        &mut harness,
        "t/a",
        CoreQoS::ExactlyOnce,
        "x",
        &["t/+", "t/#", "#"],
    );
    let [publish] = publishes(&packets)[..] else {
        panic!("{packets:?}");
    };
    assert_eq!(publish.qos, QoS::ExactlyOnce);
    let ids: Vec<_> = publish
        .properties
        .subscription_identifiers
        .iter()
        .map(|id| id.get())
        .collect();
    assert_eq!(ids, [7, 268_435_455]);
}

#[test]
fn r2_rule_6_a_delivery_is_stripped_and_checked_against_the_clients_own_filter() {
    // covers: MQTT-3.3.2-3, MQTT-4.7.2-1
    let config = Config {
        mountpoint: Some(Mountpoint::parse("ingest/${username}/").unwrap()),
        ..Config::default()
    };
    let mut harness = Harness::with(config);
    harness.connect(connect_with("pump-3", |connect| {
        connect.username = Some("acme/pump-3".into());
    }));
    harness.send(subscribe1(1, "#", QoS::AtMostOnce));
    // The interest is the mounted filter.
    assert!(
        harness
            .interest
            .contains_key(&filter("ingest/acme/pump-3/#"))
    );
    let mounted = ["ingest/acme/pump-3/#"];
    let packets = deliver(
        &mut harness,
        "ingest/acme/pump-3/commands/firmware",
        CoreQoS::AtMostOnce,
        "x",
        &mounted,
    );
    assert_eq!(publishes(&packets)[0].topic, "commands/firmware");
    // The mounted `#` matches the mount's `$SYS/x`, the client's own `#` does not.
    let packets = deliver(
        &mut harness,
        "ingest/acme/pump-3/$SYS/x",
        CoreQoS::AtMostOnce,
        "x",
        &mounted,
    );
    assert!(packets.is_empty());
    assert_eq!(harness.count(Counter::DeliveryUnmatched), 1);
    // Outside the namespace, its parent level included, nothing is delivered.
    for topic in ["ingest/acme/other/x", "ingest/acme/pump-3", "elsewhere"] {
        assert!(deliver(&mut harness, topic, CoreQoS::AtMostOnce, "x", &mounted).is_empty());
    }
    assert_eq!(harness.count(Counter::DeliveryOutsideNamespace), 3);
}

#[test]
fn mqtt_3_10_4_2_after_unsubscribe_no_new_message_is_added() {
    // covers: MQTT-3.10.4-3
    let mut harness = Harness::connected();
    harness.send(subscribe1(1, "t", QoS::ExactlyOnce));
    deliver(&mut harness, "t", CoreQoS::AtLeastOnce, "one", &["t"]);
    deliver(&mut harness, "t", CoreQoS::ExactlyOnce, "two", &["t"]);
    harness.send(unsubscribe(2, &["t"]));
    assert!(deliver(&mut harness, "t", CoreQoS::AtLeastOnce, "three", &["t"]).is_empty());
    // The deliveries already started complete.
    assert!(harness.send(puback(1)).is_empty());
    assert!(matches!(harness.send(pubrec(2))[..], [Packet::PubRel(_)]));
    harness.send(pubcomp(2));
    assert_eq!(harness.session.in_flight_out(), 0);
}

#[test]
fn mqtt_4_1_0_1_a_full_queue_ends_the_session_rather_than_drop_a_message() {
    let config = Config {
        maximum_queued_messages: 3,
        ..Config::default()
    };
    let mut harness = Harness::with(config);
    harness.connect(connect_with("client-1", |connect| {
        connect.properties = openqtt_codec::ConnectProperties {
            receive_maximum: NonZeroU16::new(1),
            session_expiry_interval: Some(600),
            ..openqtt_codec::ConnectProperties::default()
        };
        connect.will = Some(openqtt_codec::Will {
            topic: "gone".into(),
            properties: openqtt_codec::WillProperties {
                will_delay_interval: Some(30),
                ..openqtt_codec::WillProperties::default()
            },
            ..openqtt_codec::Will::default()
        });
    }));
    harness.send(subscribe1(1, "t", QoS::AtLeastOnce));
    // One in flight and three queued: the session holds them.
    for n in 0..4 {
        deliver(
            &mut harness,
            "t",
            CoreQoS::AtLeastOnce,
            &n.to_string(),
            &["t"],
        );
    }
    assert!(harness.session.is_connected());
    // Report R1, O12: one more ends the session, DISCONNECT 0x97 first.
    let packets = deliver(&mut harness, "t", CoreQoS::AtLeastOnce, "4", &["t"]);
    let [Packet::Disconnect(disconnect)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(disconnect.reason_code, DisconnectReasonCode::QuotaExceeded);
    assert_eq!(harness.releases()[0].session, SessionEnd::Discard);
    // The session ended, so the will goes at once.
    let wills = harness.wills();
    let [WillOrder::Publish { delay, .. }] = wills.as_slice() else {
        panic!("{wills:?}");
    };
    assert_eq!(*delay, 0);
}

/// A session state with three messages in flight, as a previous connection left it: a QoS 1
/// PUBLISH, a QoS 2 PUBLISH before its PUBREC, and a QoS 2 message whose PUBREL went out.
fn interrupted() -> SessionState {
    let mut harness = Harness::new();
    harness.connect(connect_with("client-1", |connect| {
        connect.properties = expiry(600);
    }));
    harness.send(subscribe1(1, "t", QoS::ExactlyOnce));
    let mut expiring = message("t", CoreQoS::AtLeastOnce, "first");
    expiring.expiry = Some(Deadline::after(harness.now, 5));
    harness.input(Input::Deliver(Delivery {
        message: expiring,
        subscriptions: vec![filter("t")],
    }));
    deliver(&mut harness, "t", CoreQoS::ExactlyOnce, "second", &["t"]);
    deliver(&mut harness, "t", CoreQoS::ExactlyOnce, "third", &["t"]);
    harness.send(pubrec(3));
    let mark = harness.log.len();
    // No timer sends anything again within the connection, the first message expiring
    // meanwhile.
    harness.advance(seconds(40));
    assert!(
        !harness
            .since(mark)
            .iter()
            .any(|effect| matches!(effect, Effect::Send { .. }))
    );
    harness.input(Input::TransportClosed);
    match &harness.releases()[0].session {
        SessionEnd::Keep(state) => state.clone(),
        SessionEnd::Discard => panic!("kept"),
    }
}

#[test]
fn mqtt_4_4_0_1_a_resumed_session_resends_what_was_in_flight_in_order() {
    // covers: MQTT-3.3.1-1, MQTT-4.3.3-6, MQTT-4.3.3-7
    let state = interrupted();
    assert!(matches!(state.outbound[2], StoredOutbound::Release(packet_id) if packet_id == id(3)));
    let mut harness = Harness::new();
    harness.stored = Some(state);
    harness.now = at(1000);
    let packets = harness.send(connect_with("client-1", |connect| {
        connect.clean_start = false;
        connect.properties = expiry(600);
    }));
    assert!(matches!(&packets[0], Packet::ConnAck(connack) if connack.session_present));
    let resent: Vec<_> = packets[1..]
        .iter()
        .map(|packet| match packet {
            Packet::Publish(publish) => {
                assert!(publish.dup, "{publish:?}");
                (
                    publish.packet_id.unwrap().get(),
                    String::from_utf8_lossy(&publish.payload).into_owned(),
                )
            }
            // The PUBLISH is never sent again once its PUBREL went out.
            Packet::PubRel(pubrel) => (pubrel.packet_id.get(), "PUBREL".to_owned()),
            other => panic!("{other:?}"),
        })
        .collect();
    // In the order first sent, the expired first one included: no expiry once sent.
    let expected = [(1, "first"), (2, "second"), (3, "PUBREL")];
    let expected: Vec<_> = expected
        .iter()
        .map(|(id, text)| (*id, (*text).to_owned()))
        .collect();
    assert_eq!(resent, expected);
    // New messages take identifiers that are not in flight.
    harness.send(subscribe1(9, "u", QoS::AtLeastOnce));
    let packets = deliver(&mut harness, "u", CoreQoS::AtLeastOnce, "new", &["u"]);
    assert_eq!(publishes(&packets)[0].packet_id, Some(id(4)));
}

#[test]
fn a_resumed_session_resends_within_the_new_receive_maximum() {
    let state = interrupted();
    let mut harness = Harness::new();
    harness.stored = Some(state);
    let packets = harness.send(connect_with("client-1", |connect| {
        connect.clean_start = false;
        connect.properties = openqtt_codec::ConnectProperties {
            receive_maximum: NonZeroU16::new(1),
            session_expiry_interval: Some(600),
            ..openqtt_codec::ConnectProperties::default()
        };
    }));
    // One PUBLISH fits the new window; the rest wait for it, in order.
    assert_eq!(packets.len(), 2);
    assert_eq!(payloads(&packets), ["first"]);
    // The next PUBLISH takes the freed slot, and the PUBREL behind it needs none.
    let packets = harness.send(puback(1));
    assert_eq!(payloads(&packets), ["second"]);
    assert!(matches!(&packets[1], Packet::PubRel(pubrel) if pubrel.packet_id == id(3)));
    let packets = harness.send(pubrec(2));
    assert!(matches!(&packets[..], [Packet::PubRel(pubrel)] if pubrel.packet_id == id(2)));
}

#[test]
fn r1_d7_identifiers_skip_those_in_flight_and_wrap() {
    let client_id = ClientId::new("client-1").unwrap();
    let mut state = SessionState::new(client_id, 600);
    state.subscriptions.push(StoredSubscription {
        filter: filter("t"),
        mounted: filter("t"),
        options: SubOpts::new(CoreQoS::AtLeastOnce),
    });
    state.outbound.push(StoredOutbound::Release(id(u16::MAX)));
    state.outbound.push(StoredOutbound::Release(id(1)));
    state.next_packet_id = u16::MAX;
    let mut harness = Harness::new();
    harness.stored = Some(state);
    let packets = harness.send(connect_with("client-1", |connect| {
        connect.clean_start = false;
        connect.properties = expiry(600);
    }));
    // The PUBRELs in flight go again, and hold their identifiers.
    assert_eq!(packets.len(), 3);
    let packets = deliver(&mut harness, "t", CoreQoS::AtLeastOnce, "x", &["t"]);
    // 65,535 and 1 are in flight, and 0 is never one: the next free is 2.
    assert_eq!(publishes(&packets)[0].packet_id, Some(id(2)));
    let packets = deliver(&mut harness, "t", CoreQoS::AtLeastOnce, "y", &["t"]);
    assert_eq!(publishes(&packets)[0].packet_id, Some(id(3)));
}

#[test]
fn a_resumed_session_in_another_namespace_is_not_resumed() {
    let config = Config {
        mountpoint: Some(Mountpoint::parse("ingest/${username}/").unwrap()),
        ..Config::default()
    };
    let client_id = ClientId::new("client-1").unwrap();
    let mut state = SessionState::new(client_id, 600);
    state.mount = Some("ingest/alice/".into());
    let mut harness = Harness::with(config);
    harness.stored = Some(state);
    let connack = harness.connect(connect_with("client-1", |connect| {
        connect.clean_start = false;
        connect.username = Some("mallory".into());
    }));
    assert!(!connack.session_present);
}

#[test]
fn the_snapshot_keeps_subscription_identifiers() {
    let mut harness = Harness::connected();
    let mut identified = subscribe1(1, "t", QoS::AtMostOnce);
    identified.properties.subscription_identifier = NonZeroU32::new(9);
    harness.send(identified);
    let state = harness.session.snapshot().unwrap();
    assert_eq!(state.subscriptions[0].options.id, SubscriptionId::new(9));
}
