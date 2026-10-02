//! What the client sends while the machine waits on an answer: a backlog bounded by packets
//! and bytes, reading paused past it and resumed once it drains, a transport that ignores the
//! pause cut off, and Keep Alive while reading is paused.

use std::num::NonZeroUsize;

use openqtt_codec::{ConnectReasonCode, DisconnectReasonCode, Packet, QoS};

use super::harness::{Harness, connect, publish, publish0, seconds};
use crate::{Config, Decision, Effect, Input};

/// R1's defaults with a backlog of `packets` packets and `bytes` bytes.
fn limited(packets: usize, bytes: usize) -> Config {
    Config {
        maximum_pending_packets: NonZeroUsize::new(packets).unwrap(),
        maximum_pending_bytes: NonZeroUsize::new(bytes).unwrap(),
        ..Config::default()
    }
}

/// Answers the oldest authorization with an allow.
fn allow_one(harness: &mut Harness) -> Vec<Packet> {
    let request = harness.authorizations.remove(0);
    harness.input(Input::Authorized {
        request: request.request,
        decisions: vec![Decision::Allow],
    })
}

fn pauses(harness: &Harness) -> usize {
    harness
        .log
        .iter()
        .filter(|effect| **effect == Effect::PauseReading)
        .count()
}

#[test]
fn a_backlog_past_its_limit_pauses_reading_until_it_drains() {
    let mut harness = Harness::connected_with(limited(4, 1 << 20));
    harness.auto.authorize = None;
    // The first PUBLISH is being authorized; four more wait, which the limit allows.
    for _ in 0..5 {
        harness.send(publish0("t", "x"));
    }
    assert!(!harness.paused);
    // A fifth waiting one is past it: the transport stops reading.
    harness.send(publish0("t", "x"));
    assert!(harness.paused);
    assert_eq!(pauses(&harness), 1);
    // Reading resumes once the backlog is down to half the limit.
    allow_one(&mut harness);
    allow_one(&mut harness);
    assert!(harness.paused);
    allow_one(&mut harness);
    assert!(!harness.paused);
    assert!(harness.log.contains(&Effect::ResumeReading));
}

#[test]
fn bytes_bound_the_backlog_too() {
    let mut harness = Harness::connected_with(limited(64, 100));
    harness.auto.authorize = None;
    harness.send(publish0("t", "x"));
    // 2 + 3 + 1 + 40 bytes each: two wait within 100 bytes, a third does not.
    let payload = "p".repeat(40);
    harness.send(publish0("t", &payload));
    harness.send(publish0("t", &payload));
    assert!(!harness.paused);
    harness.send(publish0("t", &payload));
    assert!(harness.paused);
}

#[test]
fn a_transport_that_keeps_reading_past_twice_the_limit_is_cut_off() {
    let mut harness = Harness::connected_with(limited(4, 1 << 20));
    harness.auto.authorize = None;
    harness.send(publish0("t", "x"));
    for _ in 0..8 {
        assert!(harness.send(publish0("t", "x")).is_empty());
    }
    let packets = harness.send(publish0("t", "x"));
    let [Packet::Disconnect(disconnect)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(disconnect.reason_code, DisconnectReasonCode::QuotaExceeded);

    // Before CONNACK, the refusal is a CONNACK.
    let mut harness = Harness::with(limited(4, 1 << 20));
    harness.auto.authenticate = false;
    harness.send(connect("c"));
    for _ in 0..8 {
        assert!(harness.send(publish0("t", "x")).is_empty());
    }
    let packets = harness.send(publish0("t", "x"));
    let [Packet::ConnAck(connack)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(connack.reason_code, ConnectReasonCode::QuotaExceeded);
}

#[test]
fn one_packet_up_to_the_maximum_packet_size_is_always_taken() {
    // A backlog of 100 bytes still takes a PUBLISH of 100 KiB, and the next small one the
    // transport had already decoded.
    let mut harness = Harness::connected_with(limited(4, 100));
    harness.auto.authorize = None;
    harness.send(publish0("t", "x"));
    harness.send(publish0("t", &"p".repeat(100 << 10)));
    assert!(harness.paused);
    assert!(harness.send(publish0("t", "x")).is_empty());
    assert!(harness.session.is_connected());
}

#[test]
fn keep_alive_does_not_end_a_connection_whose_reading_is_paused() {
    let mut harness = Harness::connected_with(limited(1, 1 << 20));
    harness.auto.authorize = None;
    harness.send(publish0("t", "x"));
    harness.send(publish0("t", "x"));
    harness.send(publish(
        "t",
        QoS::AtMostOnce,
        None,
        "the client keeps sending, unread",
    ));
    assert!(harness.paused);
    // Keep Alive is 30 seconds: 1.5 times it passes while the machine reads nothing.
    assert!(harness.advance(seconds(100)).is_empty());
    assert!(harness.session.is_connected());
    // Once reading resumes, silence counts again.
    allow_one(&mut harness);
    allow_one(&mut harness);
    allow_one(&mut harness);
    assert!(!harness.paused);
    let packets = harness.advance(seconds(46));
    assert!(
        matches!(&packets[..], [Packet::Disconnect(disconnect)] if disconnect.reason_code == DisconnectReasonCode::KeepAliveTimeout),
        "{packets:?}"
    );
}
