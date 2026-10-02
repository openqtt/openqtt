//! SUBSCRIBE and UNSUBSCRIBE: reason codes, replacement, retained messages and Retain
//! Handling, shared subscriptions, limits, and the mountpoint.

use std::num::NonZeroU32;

use openqtt_codec::{
    DisconnectReasonCode, Packet, QoS, RetainHandling, SubAck, SubAckReasonCode,
    SubscriptionOptions, UnsubAck, UnsubAckReasonCode,
};
use openqtt_core::QoS as CoreQoS;
use openqtt_topic::Mountpoint;

use super::harness::{
    Harness, connect_with, filter, message, options, payloads, publishes, retained, subscribe,
    subscribe1, unsubscribe,
};
use crate::{Action, Config, Decision, Delivery, Effect, Input};

fn suback(packets: &[Packet]) -> &SubAck {
    match packets {
        [Packet::SubAck(suback), ..] => suback,
        other => panic!("expected SUBACK first, got {other:?}"),
    }
}

fn unsuback(packets: &[Packet]) -> &UnsubAck {
    match packets {
        [Packet::UnsubAck(unsuback)] => unsuback,
        other => panic!("expected one UNSUBACK, got {other:?}"),
    }
}

fn handling(retain_handling: RetainHandling) -> SubscriptionOptions {
    SubscriptionOptions {
        retain_handling,
        ..options(QoS::AtLeastOnce)
    }
}

/// A connected session with a retained message on `t`, published by another client.
fn with_retained() -> Harness {
    let mut harness = Harness::connected();
    let mut kept = message("t", CoreQoS::AtLeastOnce, "kept");
    kept.retain = true;
    harness.retained.insert(kept.topic.clone(), kept);
    harness
}

// covers: MQTT-3.8.4-2, MQTT-2.2.1-6
#[test]
fn mqtt_3_8_4_1_every_subscribe_gets_a_suback_with_its_identifier() {
    let mut harness = Harness::connected();
    for packet_id in [1, 300, u16::MAX] {
        let packets = harness.send(subscribe1(packet_id, "t", QoS::AtMostOnce));
        assert_eq!(suback(&packets).packet_id.get(), packet_id);
        // UNSUBACK too, whether or not anything was deleted.
        for _ in 0..2 {
            let packets = harness.send(unsubscribe(packet_id, &["t"]));
            assert_eq!(unsuback(&packets).packet_id.get(), packet_id);
        }
    }
}

// covers: MQTT-3.8.4-6, MQTT-3.8.4-7, MQTT-3.9.3-1
#[test]
fn mqtt_3_8_4_5_several_filters_are_answered_by_one_suback_in_order() {
    let mut harness = Harness::new();
    harness.auto.authorize = Some(|action| match action {
        Action::Subscribe { filter, .. } if filter.as_str() == "secret" => Decision::Deny,
        _ => Decision::Allow,
    });
    harness.connect(super::harness::connect("c"));
    let packets = harness.send(subscribe(
        1,
        &[
            ("a", options(QoS::AtLeastOnce)),
            ("a/#/b", options(QoS::AtLeastOnce)),
            ("secret", options(QoS::AtMostOnce)),
            ("b/+", options(QoS::ExactlyOnce)),
            ("c", options(QoS::AtMostOnce)),
        ],
    ));
    assert_eq!(
        suback(&packets).reason_codes,
        [
            SubAckReasonCode::GrantedQos1,
            SubAckReasonCode::TopicFilterInvalid,
            SubAckReasonCode::NotAuthorized,
            SubAckReasonCode::GrantedQos2,
            SubAckReasonCode::GrantedQos0,
        ]
    );
    // Each accepted filter is its own subscription; the refused ones are none.
    let filters: Vec<_> = harness.interest.keys().map(ToString::to_string).collect();
    assert_eq!(filters, ["a", "b/+", "c"]);
    // The connection stays (report R1, D2 and D32).
    assert!(harness.session.is_connected());
}

#[test]
fn mqtt_3_8_4_3_an_identical_filter_replaces_the_subscription() {
    let mut harness = Harness::connected();
    harness.send(subscribe1(1, "t", QoS::AtMostOnce));
    harness.send(subscribe1(2, "t", QoS::ExactlyOnce));
    assert_eq!(harness.interest.len(), 1);
    assert_eq!(harness.interest[&filter("t")].qos, CoreQoS::ExactlyOnce);
    let packets = harness.input(Input::Deliver(Delivery {
        message: message("t", CoreQoS::ExactlyOnce, "x"),
        subscriptions: vec![filter("t")],
    }));
    // One copy, at the new QoS.
    let [publish] = publishes(&packets)[..] else {
        panic!("{packets:?}");
    };
    assert_eq!(publish.qos, QoS::ExactlyOnce);
}

#[test]
fn mqtt_3_3_1_9_retain_handling_0_sends_the_retained_messages_after_the_suback() {
    let mut harness = with_retained();
    let packets = harness.send(subscribe(
        1,
        &[("t", handling(RetainHandling::SendAtSubscribe))],
    ));
    assert!(matches!(packets[0], Packet::SubAck(_)), "{packets:?}");
    let [publish] = publishes(&packets)[..] else {
        panic!("{packets:?}");
    };
    // Sent because the subscription was made: RETAIN 1 ([MQTT-3.3.1-12]).
    assert!(publish.retain);
    assert_eq!(payloads(&packets), ["kept"]);
}

// covers: MQTT-3.3.1-10
#[test]
fn mqtt_3_8_4_4_a_replacement_with_retain_handling_0_sends_them_again() {
    let mut harness = with_retained();
    let first = harness.send(subscribe(
        1,
        &[("t", handling(RetainHandling::SendAtSubscribe))],
    ));
    assert_eq!(payloads(&first), ["kept"]);
    let again = harness.send(subscribe(
        2,
        &[("t", handling(RetainHandling::SendAtSubscribe))],
    ));
    assert_eq!(payloads(&again), ["kept"]);
    // Retain Handling 1 sends them only for a subscription that did not exist.
    let replaced = harness.send(subscribe(3, &[("t", handling(RetainHandling::SendIfNew))]));
    assert!(payloads(&replaced).is_empty());
    let mut fresh = with_retained();
    let new = fresh.send(subscribe(1, &[("t", handling(RetainHandling::SendIfNew))]));
    assert_eq!(payloads(&new), ["kept"]);
}

#[test]
fn mqtt_3_3_1_11_retain_handling_2_sends_no_retained_messages() {
    let mut harness = with_retained();
    let packets = harness.send(subscribe(1, &[("t", handling(RetainHandling::DoNotSend))]));
    assert_eq!(packets.len(), 1);
    assert!(!harness.log.iter().any(|effect| matches!(
        effect,
        Effect::Subscribe(interest) if interest.retained.is_some()
    )));
}

#[test]
fn r1_o2_live_messages_wait_for_the_retained_ones() {
    let mut harness = Harness::connected();
    harness.auto.retained = false;
    harness.send(subscribe1(1, "t/#", QoS::AtLeastOnce));
    let [read] = harness.retained_reads[..] else {
        panic!("one retained read");
    };
    // A live message for the subscription arrives before the retained read completes.
    let live = harness.input(Input::Deliver(Delivery {
        message: message("t/live", CoreQoS::AtMostOnce, "live"),
        subscriptions: vec![filter("t/#")],
    }));
    assert!(live.is_empty());
    assert_eq!(harness.session.queued(), 1);
    let mut kept = message("t/kept", CoreQoS::AtMostOnce, "kept");
    kept.retain = true;
    let packets = harness.input(Input::Retained {
        read,
        messages: vec![kept],
    });
    assert_eq!(payloads(&packets), ["kept", "live"]);
    // A delivery for another subscription does not wait.
    harness.send(subscribe(2, &[("u", handling(RetainHandling::DoNotSend))]));
    harness.send(subscribe1(3, "t/#", QoS::AtLeastOnce));
    let other = harness.input(Input::Deliver(Delivery {
        message: message("u", CoreQoS::AtMostOnce, "other"),
        subscriptions: vec![filter("u")],
    }));
    assert_eq!(payloads(&other), ["other"]);
}

#[test]
fn an_answer_to_a_retained_read_whose_subscription_was_replaced_is_ignored() {
    let retained = |payload| {
        let mut kept = message("t", CoreQoS::AtMostOnce, payload);
        kept.retain = true;
        kept
    };
    let live = |harness: &mut Harness| {
        harness.input(Input::Deliver(Delivery {
            message: message("t", CoreQoS::AtMostOnce, "live"),
            subscriptions: vec![filter("t")],
        }))
    };
    // Subscribed, unsubscribed and subscribed again, with both reads unanswered.
    let mut harness = Harness::connected();
    harness.auto.retained = false;
    harness.send(subscribe1(1, "t", QoS::AtLeastOnce));
    harness.send(unsubscribe(2, &["t"]));
    harness.send(subscribe1(3, "t", QoS::AtLeastOnce));
    let [stale, current] = harness.retained_reads[..] else {
        panic!("two retained reads");
    };
    assert!(live(&mut harness).is_empty());
    let packets = harness.input(Input::Retained {
        read: stale,
        messages: vec![retained("old")],
    });
    assert!(packets.is_empty(), "{packets:?}");
    let packets = harness.input(Input::Retained {
        read: current,
        messages: vec![retained("new")],
    });
    assert_eq!(payloads(&packets), ["new", "live"]);

    // Replaced by a subscription that asks for none: the old read is stale, and live
    // messages wait for nothing.
    let mut harness = Harness::connected();
    harness.auto.retained = false;
    harness.send(subscribe1(1, "t", QoS::AtLeastOnce));
    harness.send(subscribe(2, &[("t", handling(RetainHandling::DoNotSend))]));
    let [stale] = harness.retained_reads[..] else {
        panic!("one retained read");
    };
    assert_eq!(payloads(&live(&mut harness)), ["live"]);
    let packets = harness.input(Input::Retained {
        read: stale,
        messages: vec![retained("old")],
    });
    assert!(packets.is_empty(), "{packets:?}");
}

#[test]
fn a_shared_subscription_gets_no_retained_messages() {
    let mut harness = with_retained();
    let packets = harness.send(subscribe1(1, "$share/workers/t", QoS::AtLeastOnce));
    assert_eq!(
        suback(&packets).reason_codes,
        [SubAckReasonCode::GrantedQos1]
    );
    assert!(publishes(&packets).is_empty());
    // It takes deliveries by its whole filter, the ShareName included.
    let packets = harness.input(Input::Deliver(Delivery {
        message: message("t", CoreQoS::AtLeastOnce, "x"),
        subscriptions: vec![filter("$share/workers/t")],
    }));
    assert_eq!(payloads(&packets), ["x"]);
}

#[test]
fn mqtt_3_8_3_4_no_local_on_a_shared_subscription_is_a_protocol_error() {
    let mut harness = Harness::connected();
    let no_local = SubscriptionOptions {
        no_local: true,
        ..options(QoS::AtMostOnce)
    };
    let packets = harness.send(subscribe(
        1,
        &[("t", options(QoS::AtMostOnce)), ("$share/g/t", no_local)],
    ));
    let [Packet::Disconnect(disconnect)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(disconnect.reason_code, DisconnectReasonCode::ProtocolError);
    // Nothing of the packet was acted on.
    assert!(harness.interest.is_empty());
}

// covers: MQTT-4.7.1-1, MQTT-4.7.1-2, MQTT-4.7.3-1, MQTT-4.8.2-1, MQTT-4.8.2-2
#[test]
fn r1_d32_a_filter_that_breaks_sections_4_7_or_4_8_is_refused_alone() {
    let mut harness = Harness::connected();
    let invalid = [
        "a/#/b",
        "a#",
        "a/b+",
        "",
        "$share//t",
        "$share/g",
        "$share/g+/t",
    ];
    let mut filters: Vec<(&str, SubscriptionOptions)> = invalid
        .iter()
        .map(|text| (*text, options(QoS::AtMostOnce)))
        .collect();
    filters.push(("ok", options(QoS::AtMostOnce)));
    let packets = harness.send(subscribe(1, &filters));
    let codes = &suback(&packets).reason_codes;
    assert_eq!(codes.len(), invalid.len() + 1);
    assert!(
        codes[..invalid.len()]
            .iter()
            .all(|code| *code == SubAckReasonCode::TopicFilterInvalid)
    );
    assert_eq!(codes[invalid.len()], SubAckReasonCode::GrantedQos0);
    let packets = harness.send(unsubscribe(2, &["a/#/b", "ok"]));
    assert_eq!(
        unsuback(&packets).reason_codes,
        [
            UnsubAckReasonCode::TopicFilterInvalid,
            UnsubAckReasonCode::Success
        ]
    );
}

#[test]
fn r1_o16_subscriptions_and_levels_have_limits() {
    let config = Config {
        maximum_subscriptions: 2,
        ..Config::default()
    };
    let mut harness = Harness::connected_with(config);
    let packets = harness.send(subscribe(
        1,
        &[
            ("a", options(QoS::AtMostOnce)),
            ("b", options(QoS::AtMostOnce)),
            ("c", options(QoS::AtMostOnce)),
            // Replacing one is not a new one.
            ("a", options(QoS::AtLeastOnce)),
        ],
    ));
    assert_eq!(
        suback(&packets).reason_codes,
        [
            SubAckReasonCode::GrantedQos0,
            SubAckReasonCode::GrantedQos0,
            SubAckReasonCode::QuotaExceeded,
            SubAckReasonCode::GrantedQos1,
        ]
    );
    let mut harness = Harness::connected();
    let deep = vec!["l"; 129].join("/");
    let packets = harness.send(subscribe1(1, &deep, QoS::AtMostOnce));
    assert_eq!(
        suback(&packets).reason_codes,
        [SubAckReasonCode::TopicFilterInvalid]
    );
}

#[test]
fn r1_d12_every_subscription_identifier_is_accepted() {
    let mut harness = Harness::connected();
    for (packet_id, value) in [(1, 1), (2, 268_435_455)] {
        let mut identified = subscribe1(packet_id, &format!("t/{value}"), QoS::AtMostOnce);
        identified.properties.subscription_identifier = NonZeroU32::new(value);
        let packets = harness.send(identified);
        assert_eq!(
            suback(&packets).reason_codes,
            [SubAckReasonCode::GrantedQos0]
        );
    }
}

#[test]
fn capabilities_turned_off_refuse_their_subscriptions() {
    let config = Config {
        subscription_identifiers_available: false,
        wildcard_subscription_available: false,
        shared_subscription_available: false,
        ..Config::default()
    };
    let mut harness = Harness::connected_with(config);
    let packets = harness.send(subscribe(
        1,
        &[
            ("t/+", options(QoS::AtMostOnce)),
            ("$share/g/t", options(QoS::AtMostOnce)),
            ("t", options(QoS::AtMostOnce)),
        ],
    ));
    assert_eq!(
        suback(&packets).reason_codes,
        [
            SubAckReasonCode::WildcardSubscriptionsNotSupported,
            SubAckReasonCode::SharedSubscriptionsNotSupported,
            SubAckReasonCode::GrantedQos0,
        ]
    );
    let mut identified = subscribe1(2, "t", QoS::AtMostOnce);
    identified.properties.subscription_identifier = NonZeroU32::new(5);
    let packets = harness.send(identified);
    assert_eq!(
        suback(&packets).reason_codes,
        [SubAckReasonCode::SubscriptionIdentifiersNotSupported]
    );
}

#[test]
fn r2_rule_11_a_subscription_is_authorized_as_sent_then_mounted() {
    let config = Config {
        mountpoint: Some(Mountpoint::parse("ingest/${username}/").unwrap()),
        ..Config::default()
    };
    let mut harness = Harness::with(config);
    harness.auto.authorize = None;
    harness.connect(connect_with("pump-3", |connect| {
        connect.username = Some("acme/pump-3".into());
    }));
    harness.send(subscribe(
        1,
        &[
            ("commands/#", options(QoS::AtLeastOnce)),
            ("$share/g/jobs/+", options(QoS::AtMostOnce)),
        ],
    ));
    let request = harness.authorizations.remove(0);
    assert_eq!(
        request.actions,
        [
            Action::Subscribe {
                filter: filter("commands/#"),
                qos: CoreQoS::AtLeastOnce,
            },
            Action::Subscribe {
                filter: filter("$share/g/jobs/+"),
                qos: CoreQoS::AtMostOnce,
            },
        ]
    );
    harness.input(Input::Authorized {
        request: request.request,
        decisions: vec![Decision::Allow, Decision::Allow],
    });
    let filters: Vec<_> = harness.interest.keys().map(ToString::to_string).collect();
    assert_eq!(
        filters,
        [
            "$share/g/ingest/acme/pump-3/jobs/+",
            "ingest/acme/pump-3/commands/#"
        ]
    );
    // UNSUBSCRIBE names the filter as the client subscribed it.
    let packets = harness.send(unsubscribe(2, &["commands/#"]));
    assert_eq!(
        unsuback(&packets).reason_codes,
        [UnsubAckReasonCode::Success]
    );
    assert!(
        !harness
            .interest
            .contains_key(&filter("ingest/acme/pump-3/commands/#"))
    );
}

// covers: MQTT-3.10.4-4, MQTT-3.10.4-5
#[test]
fn mqtt_3_10_4_1_an_exact_match_deletes_the_subscription() {
    let mut harness = Harness::connected();
    harness.send(subscribe1(1, "t/#", QoS::AtMostOnce));
    harness.send(subscribe1(2, "$queue/t", QoS::AtMostOnce));
    // Compared byte for byte: a filter matching the same topics is not the same filter, and
    // `$share/$queue/t` is not `$queue/t` (report R1, D17).
    let packets = harness.send(unsubscribe(3, &["t/+", "$share/$queue/t"]));
    let answer = unsuback(&packets);
    assert_eq!(answer.packet_id.get(), 3);
    assert_eq!(
        answer.reason_codes,
        [
            UnsubAckReasonCode::NoSubscriptionExisted,
            UnsubAckReasonCode::NoSubscriptionExisted
        ]
    );
    assert_eq!(harness.interest.len(), 2);
    let packets = harness.send(unsubscribe(4, &["t/#"]));
    assert_eq!(
        unsuback(&packets).reason_codes,
        [UnsubAckReasonCode::Success]
    );
    assert_eq!(harness.interest.len(), 1);
    assert!(
        harness
            .log
            .iter()
            .any(|effect| *effect == Effect::Unsubscribe(filter("t/#")))
    );
}

// covers: MQTT-3.11.3-1
#[test]
fn mqtt_3_10_4_6_several_filters_are_answered_by_one_unsuback_in_order() {
    let mut harness = Harness::connected();
    harness.send(subscribe(
        1,
        &[
            ("a", options(QoS::AtMostOnce)),
            ("c", options(QoS::AtMostOnce)),
        ],
    ));
    let packets = harness.send(unsubscribe(2, &["a", "b", "c", "a"]));
    assert_eq!(
        unsuback(&packets).reason_codes,
        [
            UnsubAckReasonCode::Success,
            UnsubAckReasonCode::NoSubscriptionExisted,
            UnsubAckReasonCode::Success,
            UnsubAckReasonCode::NoSubscriptionExisted,
        ]
    );
    assert!(harness.interest.is_empty());
}

#[test]
fn a_retained_message_also_follows_no_local_and_the_clients_filter() {
    let mut harness = Harness::connected();
    // The client's own retained message.
    harness.send(retained("mine", QoS::AtMostOnce, None, "own"));
    let no_local = SubscriptionOptions {
        no_local: true,
        ..options(QoS::AtMostOnce)
    };
    let packets = harness.send(subscribe(1, &[("mine", no_local)]));
    assert!(publishes(&packets).is_empty());
    let packets = harness.send(subscribe(2, &[("mine/#", options(QoS::AtMostOnce))]));
    assert_eq!(payloads(&packets), ["own"]);
}
