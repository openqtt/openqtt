//! The end of a connection: DISCONNECT either way, the will, takeover, shutdown, the loss of the
//! transport, Keep Alive, and the release of the claim.

use bytes::Bytes;
use openqtt_codec::{
    Connect, ConnectProperties, Disconnect, DisconnectProperties, DisconnectReasonCode, Packet,
    QoS, Will, WillProperties,
};
use openqtt_core::{ClientId, QoS as CoreQoS};
use openqtt_topic::Mountpoint;

use super::harness::{
    Harness, at, connect_with, expiry, message, name, publish0, seconds, subscribe1,
};
use crate::{
    ClaimResult, CloseCode, Config, Delivery, Effect, Input, SessionEnd, Shutdown, WillOrder,
};

/// A CONNECT with a will on `gone` delayed `delay` seconds and a session that lives `life`
/// seconds after the connection.
fn with_will(delay: u32, life: u32) -> Connect {
    connect_with("client-1", |connect| {
        connect.properties = expiry(life);
        connect.will = Some(Will {
            qos: QoS::AtLeastOnce,
            topic: "gone".into(),
            payload: Bytes::from_static(b"offline"),
            properties: WillProperties {
                will_delay_interval: Some(delay),
                ..WillProperties::default()
            },
            ..Will::default()
        });
    })
}

/// The delay of the one will order published, or `None` for a discarded will, and a panic for
/// anything else.
fn will_delay(harness: &Harness) -> Option<u32> {
    match harness.wills().as_slice() {
        [WillOrder::Publish { delay, .. }] => Some(*delay),
        [WillOrder::Discard] => None,
        other => panic!("expected one will order, got {other:?}"),
    }
}

fn disconnect_code(packets: &[Packet]) -> DisconnectReasonCode {
    match packets {
        [Packet::Disconnect(disconnect)] => {
            // The server never sends a Session Expiry Interval ([MQTT-3.14.2-2]).
            assert_eq!(disconnect.properties.session_expiry_interval, None);
            disconnect.reason_code
        }
        other => panic!("expected one DISCONNECT, got {other:?}"),
    }
}

#[test]
fn mqtt_3_1_2_8_an_unclean_end_publishes_the_will_after_its_delay() {
    // The delay, or the session's life if shorter: the will goes when either runs out.
    for (delay, life, expected) in [(30, 600, 30), (30, 10, 10), (30, 0, 0), (0, 600, 0)] {
        let mut harness = Harness::new();
        harness.connect(with_will(delay, life));
        harness.input(Input::TransportClosed);
        assert_eq!(will_delay(&harness), Some(expected), "{delay} {life}");
    }
}

#[test]
fn mqtt_3_1_2_10_disconnect_0x00_deletes_the_will() {
    // covers: MQTT-3.14.4-3
    let mut harness = Harness::new();
    harness.connect(with_will(30, 600));
    harness.send(Disconnect::default());
    assert_eq!(will_delay(&harness), None);
    assert_eq!(harness.closed(), Some(CloseCode::NoError));

    // Any other code from the client publishes it, 0x04 by asking for it.
    for code in [
        DisconnectReasonCode::DisconnectWithWillMessage,
        DisconnectReasonCode::UnspecifiedError,
    ] {
        let mut harness = Harness::new();
        harness.connect(with_will(5, 600));
        harness.send(Disconnect {
            reason_code: code,
            ..Disconnect::default()
        });
        assert_eq!(will_delay(&harness), Some(5), "{code:?}");
    }
}

#[test]
fn mqtt_3_1_2_14_the_will_is_published_retained_only_with_will_retain() {
    // covers: MQTT-3.1.2-15, MQTT-3.1.3-10
    for retain in [false, true] {
        let config = Config {
            mountpoint: Some(Mountpoint::parse("ingest/${clientid}/").unwrap()),
            ..Config::default()
        };
        let mut harness = Harness::with(config);
        let mut connect = with_will(0, 0);
        if let Some(will) = connect.will.as_mut() {
            will.retain = retain;
            will.properties.user_properties =
                vec![("z".into(), "1".into()), ("a".into(), "2".into())];
            will.properties.message_expiry_interval = Some(60);
        }
        harness.connect(connect);
        harness.input(Input::TransportClosed);
        let wills = harness.wills();
        let [WillOrder::Publish { will, .. }] = wills.as_slice() else {
            panic!("{wills:?}");
        };
        let message = &will.message;
        assert_eq!(message.retain, retain);
        // Mounted like any topic the client publishes (report R2, rule 6), published as the
        // client, its User Properties in order.
        assert_eq!(message.topic, name("ingest/client-1/gone"));
        assert_eq!(
            message.publisher.as_ref().map(ClientId::as_str),
            Some("client-1")
        );
        assert_eq!(message.qos, CoreQoS::AtLeastOnce);
        assert_eq!(
            message.user_properties,
            [
                ("z".to_owned(), "1".to_owned()),
                ("a".to_owned(), "2".to_owned())
            ]
        );
        // Its expiry runs from when it is published.
        assert_eq!((message.expiry, will.expiry_interval), (None, Some(60)));
    }
}

#[test]
fn the_will_goes_with_the_claim() {
    let mut harness = Harness::new();
    harness.connect(with_will(30, 600));
    let claim = harness
        .log
        .iter()
        .find_map(|effect| match effect {
            Effect::Claim(claim) => Some(claim.clone()),
            _ => None,
        })
        .unwrap();
    let will = claim.will.unwrap();
    assert_eq!((will.message.topic.as_str(), will.delay), ("gone", 30));
}

#[test]
fn mqtt_3_1_3_9_a_takeover_within_the_will_delay_publishes_no_will() {
    // covers: MQTT-3.1.4-3
    let mut harness = Harness::new();
    harness.connect(with_will(30, 600));
    let packets = harness.input(Input::StepDown {
        session_ends: false,
    });
    assert_eq!(
        disconnect_code(&packets),
        DisconnectReasonCode::SessionTakenOver
    );
    assert!(harness.wills().is_empty());
    assert!(matches!(harness.releases()[0].session, SessionEnd::Keep(_)));

    // With no delay the will goes as the connection closes, and when the new connection
    // ends the session it goes because the session ends (section 3.1.2.5).
    for (delay, session_ends) in [(0, false), (30, true)] {
        let mut harness = Harness::new();
        harness.connect(with_will(delay, 600));
        harness.input(Input::StepDown { session_ends });
        assert_eq!(will_delay(&harness), Some(0), "{delay} {session_ends}");
    }
}

#[test]
fn r1_o23_a_server_shutdown_publishes_the_will_after_its_delay() {
    let mut harness = Harness::new();
    harness.connect(with_will(30, 600));
    let packets = harness.input(Input::Shutdown(Shutdown::UseAnotherServer {
        server_reference: Some("edge-7.example".into()),
    }));
    let [Packet::Disconnect(disconnect)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::UseAnotherServer
    );
    assert_eq!(
        disconnect.properties.server_reference.as_deref(),
        Some("edge-7.example")
    );
    // A client that reconnects within its delay publishes none: the delay stands.
    assert_eq!(will_delay(&harness), Some(30));
    for (shutdown, code) in [
        (
            Shutdown::ServerShuttingDown,
            DisconnectReasonCode::ServerShuttingDown,
        ),
        (
            Shutdown::ServerMoved {
                server_reference: None,
            },
            DisconnectReasonCode::ServerMoved,
        ),
        (
            Shutdown::AdministrativeAction,
            DisconnectReasonCode::AdministrativeAction,
        ),
    ] {
        let mut harness = Harness::connected();
        let packets = harness.input(Input::Shutdown(shutdown));
        assert_eq!(disconnect_code(&packets), code);
    }
}

#[test]
fn mqtt_3_14_4_1_nothing_is_sent_after_a_disconnect() {
    let mut harness = Harness::connected();
    harness.send(subscribe1(1, "t", QoS::AtMostOnce));
    harness.send(Disconnect::default());
    let mark = harness.log.len();
    assert!(harness.send(Packet::PingReq).is_empty());
    let packets = harness.input(Input::Deliver(Delivery {
        message: message("t", CoreQoS::AtMostOnce, "x"),
        subscriptions: vec![super::harness::filter("t")],
    }));
    assert!(packets.is_empty());
    assert!(harness.since(mark).is_empty());

    // Nor after the server's own.
    let mut harness = Harness::connected();
    harness.input(Input::Shutdown(Shutdown::ServerShuttingDown));
    assert!(harness.send(publish0("t", "x")).is_empty());
    assert!(harness.advance(seconds(100)).is_empty());
    let closes = harness
        .log
        .iter()
        .filter(|effect| matches!(effect, Effect::Close(_)))
        .count();
    assert_eq!(closes, 1);
}

#[test]
fn mqtt_3_14_2_2_the_server_never_sends_a_session_expiry_interval_on_disconnect() {
    let mut harness = Harness::new();
    harness.connect(connect_with("c", |connect| connect.properties = expiry(60)));
    let packets = harness.send(connect_with("c", |connect| connect.properties = expiry(60)));
    assert_eq!(
        disconnect_code(&packets),
        DisconnectReasonCode::ProtocolError
    );
}

#[test]
fn r1_o7_a_disconnect_that_raises_the_expiry_is_capped() {
    let mut harness = Harness::new();
    harness.connect(connect_with("c", |connect| connect.properties = expiry(60)));
    harness.send(Disconnect {
        properties: DisconnectProperties {
            session_expiry_interval: Some(u32::MAX),
            ..DisconnectProperties::default()
        },
        ..Disconnect::default()
    });
    let SessionEnd::Keep(state) = &harness.releases()[0].session else {
        panic!("kept");
    };
    assert_eq!(state.expiry, 604_800);

    // Lowering it to 0 ends the session with the connection.
    let mut harness = Harness::new();
    harness.connect(connect_with("c", |connect| connect.properties = expiry(60)));
    harness.send(Disconnect {
        properties: DisconnectProperties {
            session_expiry_interval: Some(0),
            ..DisconnectProperties::default()
        },
        ..Disconnect::default()
    });
    assert_eq!(harness.releases()[0].session, SessionEnd::Discard);
}

#[test]
fn a_disconnect_that_gives_life_to_a_session_that_had_none_is_a_protocol_error() {
    let mut harness = Harness::new();
    harness.connect(with_will(5, 0));
    let packets = harness.send(Disconnect {
        properties: DisconnectProperties {
            session_expiry_interval: Some(10),
            ..DisconnectProperties::default()
        },
        ..Disconnect::default()
    });
    assert_eq!(
        disconnect_code(&packets),
        DisconnectReasonCode::ProtocolError
    );
    // It is not taken as a DISCONNECT: the will is published, at once since the session ends.
    assert_eq!(will_delay(&harness), Some(0));
    assert_eq!(harness.releases()[0].session, SessionEnd::Discard);
}

#[test]
fn mqtt_3_1_2_22_a_silent_client_is_disconnected_at_one_and_a_half_times_its_keep_alive() {
    let mut harness = Harness::new();
    harness.connect(with_will(0, 0));
    // Report R2, rule 24: Keep Alive 30, disconnected at 45 seconds.
    assert!(
        harness
            .advance(std::time::Duration::from_millis(44_999))
            .is_empty()
    );
    let packets = harness.advance(std::time::Duration::from_millis(1));
    assert_eq!(
        disconnect_code(&packets),
        DisconnectReasonCode::KeepAliveTimeout
    );
    assert_eq!(harness.now, at(45));
    // As if the network had failed: the will is published (report R1, D5).
    assert_eq!(will_delay(&harness), Some(0));
}

#[test]
fn every_packet_resets_keep_alive() {
    let mut harness = Harness::connected();
    harness.advance(seconds(40));
    assert_eq!(harness.send(Packet::PingReq), [Packet::PingResp]);
    // The deadline is now 45 seconds after the PINGREQ: 85.
    assert!(harness.advance(seconds(44)).is_empty());
    harness.send(publish0("t", "x"));
    assert!(harness.advance(seconds(44)).is_empty());
    let packets = harness.advance(seconds(1));
    assert_eq!(
        disconnect_code(&packets),
        DisconnectReasonCode::KeepAliveTimeout
    );
    assert_eq!(harness.now, at(129));
}

#[test]
fn r1_o4_a_server_keep_alive_is_the_one_enforced() {
    let mut harness = Harness::new();
    let connack = harness.connect(connect_with("c", |connect| connect.keep_alive = 2));
    assert_eq!(connack.properties.server_keep_alive, Some(10));
    assert!(harness.advance(seconds(14)).is_empty());
    let packets = harness.advance(seconds(1));
    assert_eq!(
        disconnect_code(&packets),
        DisconnectReasonCode::KeepAliveTimeout
    );
    assert_eq!(harness.now, at(15));
}

#[test]
fn every_claim_is_released_once() {
    // The transport closes while the claim is outstanding: it is released when it completes,
    // its will discarded, since the connection was never accepted.
    let mut harness = Harness::new();
    harness.auto.claim = false;
    harness.send(with_will(30, 600));
    harness.input(Input::TransportClosed);
    assert!(harness.releases().is_empty());
    assert_eq!(harness.closed(), Some(CloseCode::NoError));
    let packets = harness.input(Input::Claimed(ClaimResult::Claimed { session: None }));
    assert!(packets.is_empty());
    assert_eq!(harness.releases().len(), 1);
    assert_eq!(will_delay(&harness), None);
    // A second answer releases nothing more.
    harness.input(Input::Claimed(ClaimResult::Claimed { session: None }));
    assert_eq!(harness.releases().len(), 1);

    // A refused claim was never held, and is not released.
    let mut harness = Harness::new();
    harness.auto.claim = false;
    harness.send(connect_with("c", |connect| {
        connect.properties = ConnectProperties::default()
    }));
    let packets = harness.input(Input::Claimed(ClaimResult::Refused(
        openqtt_codec::ConnectReasonCode::ServerBusy,
    )));
    let [Packet::ConnAck(connack)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(
        connack.reason_code,
        openqtt_codec::ConnectReasonCode::ServerBusy
    );
    assert!(harness.releases().is_empty());
}

#[test]
fn a_step_down_before_connack_closes_without_a_reply() {
    let mut harness = Harness::with_peer(
        Config::default(),
        crate::Peer {
            handshake_complete: false,
            ..crate::Peer::new(0)
        },
    );
    harness.send(with_will(0, 600));
    // Claimed, waiting for the handshake: another connection takes the session over.
    let packets = harness.input(Input::StepDown {
        session_ends: false,
    });
    assert!(packets.is_empty());
    // Its claim was replaced, and with it its will.
    assert!(harness.wills().is_empty());
    assert_eq!(harness.releases().len(), 1);
}
