//! CONNECT and CONNACK: the first packet, the Client Identifier, refusals and what CONNACK
//! announces.

use std::num::NonZeroU16;

use openqtt_codec::{
    ConnAck, ConnectProperties, ConnectReasonCode, Disconnect, DisconnectReasonCode, Error, Packet,
    PacketType, ProtocolRefusal, PubAckReasonCode, QoS, SubAckReasonCode, Will, WillProperties,
};
use openqtt_core::ClientId;

use super::harness::{Harness, connect, connect_with, expiry, publish0, publish1, subscribe1};
use crate::{
    ClaimResult, CloseCode, Config, Decision, Effect, Identity, Input, Peer, SessionEnd,
    SessionState, StoredSubscription,
};

fn refusal(packets: &[Packet]) -> ConnAck {
    match packets {
        [Packet::ConnAck(connack)] => (**connack).clone(),
        other => panic!("expected one CONNACK, got {other:?}"),
    }
}

#[test]
fn mqtt_3_1_0_1_a_first_packet_other_than_connect_is_closed_on_without_a_reply() {
    let mut harness = Harness::new();
    assert!(harness.send(Packet::PingReq).is_empty());
    assert_eq!(harness.closed(), Some(CloseCode::ProtocolError));
    // Nothing is processed after it.
    assert!(harness.send(connect("c")).is_empty());
    assert!(
        !harness
            .log
            .iter()
            .any(|effect| matches!(effect, Effect::Authenticate(_)))
    );

    // The same for a first packet that does not decode and is not a CONNECT.
    let mut harness = Harness::new();
    let packets = harness.input(Input::DecodeError {
        stream: crate::StreamId::Control,
        error: Error::EmptyTopicName,
        packet_type: Some(PacketType::Publish),
    });
    assert!(packets.is_empty());
    assert_eq!(harness.closed(), Some(CloseCode::ProtocolError));
}

#[test]
fn mqtt_3_1_0_2_a_second_connect_is_a_protocol_error() {
    // covers: MQTT-3.2.0-2
    let mut harness = Harness::connected();
    let packets = harness.send(connect("client-1"));
    // DISCONNECT 0x82, never a second CONNACK.
    let [Packet::Disconnect(disconnect)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(disconnect.reason_code, DisconnectReasonCode::ProtocolError);
    assert_eq!(harness.closed(), Some(CloseCode::NoError));
    let connacks = harness
        .log
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                Effect::Send {
                    packet: Packet::ConnAck(_),
                    ..
                }
            )
        })
        .count();
    assert_eq!(connacks, 1);
}

#[test]
fn r1_d1_an_older_protocol_gets_the_refusal_its_version_reads() {
    let cases = [
        ("MQTT", 4, Some(ProtocolRefusal::ConnAckV311)),
        ("MQIsdp", 3, Some(ProtocolRefusal::ConnAckV311)),
        ("MQTT", 0x84, Some(ProtocolRefusal::ConnAckV311)),
        ("MQTT", 0x85, Some(ProtocolRefusal::ConnAckV5)),
        ("MQTT", 6, Some(ProtocolRefusal::ConnAckV5)),
        ("HTTP", 5, None),
    ];
    for (name, level, expected) in cases {
        let mut harness = Harness::new();
        harness.input(Input::DecodeError {
            stream: crate::StreamId::Control,
            error: Error::UnsupportedProtocol {
                name: name.into(),
                level,
            },
            packet_type: Some(PacketType::Connect),
        });
        let refusal = harness.log.iter().find_map(|effect| match effect {
            Effect::SendRefusal(refusal) => Some(*refusal),
            _ => None,
        });
        assert_eq!(refusal, expected, "{name} {level}");
        let code = if expected.is_some() {
            CloseCode::NoError
        } else {
            CloseCode::ProtocolError
        };
        assert_eq!(harness.closed(), Some(code), "{name} {level}");
    }
}

#[test]
fn mqtt_3_1_4_1_a_malformed_connect_gets_connack_0x81_and_a_protocol_error_0x82() {
    // covers: MQTT-3.1.2-3, MQTT-3.1.2-9, MQTT-3.1.2-11
    let cases = [
        (
            Error::InvalidConnectFlags { flags: 1 },
            ConnectReasonCode::MalformedPacket,
        ),
        (
            Error::Truncated {
                field: "Client Identifier",
            },
            ConnectReasonCode::MalformedPacket,
        ),
        (
            Error::InvalidPropertyValue {
                property: openqtt_codec::PropertyId::ReceiveMaximum,
                value: 0,
            },
            ConnectReasonCode::ProtocolError,
        ),
        (
            Error::PacketTooLarge {
                size: 2_000_000,
                maximum: 1_048_576,
            },
            ConnectReasonCode::PacketTooLarge,
        ),
    ];
    for (error, code) in cases {
        let mut harness = Harness::new();
        let packets = harness.input(Input::DecodeError {
            stream: crate::StreamId::Control,
            error: error.clone(),
            packet_type: Some(PacketType::Connect),
        });
        let connack = refusal(&packets);
        assert_eq!(connack.reason_code, code, "{error}");
        assert!(!connack.session_present);
        assert_eq!(harness.closed(), Some(CloseCode::NoError));
    }
}

#[test]
fn mqtt_3_1_4_5_connack_0x00_goes_out_only_after_the_claim() {
    let mut harness = Harness::new();
    harness.auto.claim = false;
    assert!(harness.send(connect("client-1")).is_empty());
    let [claim] = harness.claims.as_slice() else {
        panic!("one claim");
    };
    assert_eq!(claim.client_id.as_str(), "client-1");
    assert!(claim.clean_start && !claim.assigned);
    let packets = harness.input(Input::Claimed(ClaimResult::Claimed { session: None }));
    let connack = refusal(&packets);
    assert_eq!(connack.reason_code, ConnectReasonCode::Success);
    assert!(harness.session.is_connected());
}

#[test]
fn mqtt_3_1_4_6_after_a_refused_connect_nothing_the_client_sent_is_processed() {
    let mut harness = Harness::new();
    harness.auto.authenticate = false;
    harness.send(connect("client-1"));
    // Pipelined after the CONNECT, before any answer.
    harness.send(subscribe1(1, "t", QoS::AtLeastOnce));
    harness.send(publish1("t", 2, "x"));
    let packets = harness.input(Input::Authenticated(crate::AuthResult::Failure(
        ConnectReasonCode::BadUserNameOrPassword,
    )));
    let connack = refusal(&packets);
    assert_eq!(
        connack.reason_code,
        ConnectReasonCode::BadUserNameOrPassword
    );
    assert!(harness.published().is_empty());
    assert!(!harness.log.iter().any(|effect| matches!(
        effect,
        Effect::Subscribe(_) | Effect::Authorize(_) | Effect::Claim(_)
    )));
    // And nothing after the close either.
    assert!(harness.send(Packet::PingReq).is_empty());
}

#[test]
fn mqtt_3_2_0_1_connack_comes_before_any_other_packet() {
    let mut harness = Harness::new();
    harness.auto.claim = false;
    harness.send(connect("client-1"));
    // Packets the client sent behind its CONNECT wait for the CONNACK.
    assert!(harness.send(subscribe1(1, "t", QoS::AtMostOnce)).is_empty());
    assert!(harness.send(Packet::PingReq).is_empty());
    let packets = harness.input(Input::Claimed(ClaimResult::Claimed { session: None }));
    assert!(matches!(packets[0], Packet::ConnAck(_)), "{packets:?}");
    assert!(matches!(packets[1], Packet::SubAck(_)), "{packets:?}");
    assert_eq!(packets[2], Packet::PingResp);
    assert_eq!(packets.len(), 3);
}

#[test]
fn mqtt_3_1_3_5_identifiers_of_1_to_23_characters_and_up_to_256_bytes_are_accepted() {
    // covers: MQTT-3.1.3-2
    for id in [
        "a".to_owned(),
        "0123456789abcdefghijklm".to_owned(),
        "ABCDEFGHIJKLMNOPQRSTUVW".to_owned(),
        "\u{e9}".repeat(128),
        "dev/ice 1".to_owned(),
    ] {
        let mut harness = Harness::new();
        let connack = harness.connect(connect(&id));
        assert_eq!(connack.reason_code, ConnectReasonCode::Success, "{id}");
        // The identifier names the session, and is not announced back.
        assert_eq!(harness.session.client_id().unwrap().as_str(), id);
        assert_eq!(connack.properties.assigned_client_identifier, None);
    }
}

#[test]
fn mqtt_3_1_3_8_an_identifier_over_256_bytes_gets_connack_0x85() {
    let mut harness = Harness::new();
    let packets = harness.send(connect(&"x".repeat(257)));
    let connack = refusal(&packets);
    assert_eq!(
        connack.reason_code,
        ConnectReasonCode::ClientIdentifierNotValid
    );
    assert_eq!(harness.closed(), Some(CloseCode::NoError));
}

#[test]
fn mqtt_3_1_3_6_an_empty_identifier_is_assigned_one_with_either_clean_start() {
    // covers: MQTT-3.1.3-7
    for clean_start in [true, false] {
        let mut harness = Harness::with_peer(Config::default(), Peer::new(1));
        let connack = harness.connect(connect_with("", |connect| {
            connect.clean_start = clean_start
        }));
        assert_eq!(connack.reason_code, ConnectReasonCode::Success);
        let assigned = connack.properties.assigned_client_identifier.unwrap();
        assert_eq!(assigned, ClientId::assigned(1).as_str());
        // Used as if the client had sent it: the session, the claim and the authenticator see it.
        assert_eq!(harness.session.client_id().unwrap().as_str(), assigned);
        let claim = harness
            .log
            .iter()
            .find_map(|effect| match effect {
                Effect::Claim(claim) => Some(claim),
                _ => None,
            })
            .unwrap();
        assert_eq!(claim.client_id.as_str(), assigned);
        assert!(claim.assigned);
    }
}

#[test]
fn mqtt_3_2_2_16_an_assigned_identifier_in_use_is_drawn_again() {
    let mut harness = Harness::with_peer(Config::default(), Peer::new(42));
    harness.auto.claim = false;
    harness.send(connect(""));
    let first = harness.claims.remove(0).client_id;
    assert_eq!(first, ClientId::assigned(42));
    assert_eq!(first.as_str().len(), 23);
    assert!(first.as_str().starts_with("oq"));
    // Report R1, O9: the claim refuses an identifier in use, and another is drawn.
    harness.input(Input::Claimed(ClaimResult::Taken));
    let second = harness.claims.remove(0).client_id;
    assert_ne!(second, first);
    assert_eq!(second.as_str().len(), 23);
    let packets = harness.input(Input::Claimed(ClaimResult::Claimed { session: None }));
    let connack = refusal(&packets);
    assert_eq!(
        connack.properties.assigned_client_identifier.as_deref(),
        Some(second.as_str())
    );
}

#[test]
fn an_identifier_the_client_chose_is_never_drawn_again() {
    let mut harness = Harness::new();
    harness.auto.claim = false;
    harness.send(connect("client-1"));
    let packets = harness.input(Input::Claimed(ClaimResult::Taken));
    assert_eq!(
        refusal(&packets).reason_code,
        ConnectReasonCode::ClientIdentifierNotValid
    );
}

#[test]
fn r1_d20_on_a_certificate_listener_the_cn_names_the_session() {
    // covers: MQTT-3.1.3-2, MQTT-3.2.2-16
    let config = Config {
        identity: Identity::Certificate,
        ..Config::default()
    };
    let peer = Peer {
        certificate_cn: Some("acme/production/pump-3".into()),
        ..Peer::new(0)
    };
    let mut harness = Harness::with_peer(config.clone(), peer.clone());
    harness.auto.authenticate = false;
    harness.send(connect_with("someone-else", |connect| {
        connect.username = Some("mallory".into());
        connect.password = Some(bytes::Bytes::from_static(b"secret"));
    }));
    let request = harness.authentications.remove(0);
    // Report R2, rule 4: the CN is the Client Identifier and the User Name; what the client
    // sent is ignored.
    assert_eq!(request.client_id.as_str(), "acme/production/pump-3");
    assert_eq!(request.username.as_deref(), Some("acme/production/pump-3"));
    assert_eq!(request.password, None);
    let packets = harness.input(Input::Authenticated(crate::AuthResult::Success {
        data: None,
    }));
    let connack = refusal(&packets);
    assert_eq!(
        connack.properties.assigned_client_identifier.as_deref(),
        Some("acme/production/pump-3")
    );
    assert_eq!(
        harness.session.client_id().unwrap().as_str(),
        "acme/production/pump-3"
    );

    // An empty identifier names the device's own session, not a new one.
    let mut harness = Harness::with_peer(config.clone(), peer);
    let connack = harness.connect(connect(""));
    assert_eq!(
        connack.properties.assigned_client_identifier.as_deref(),
        Some("acme/production/pump-3")
    );

    // Without a certificate there is no identity to give the session.
    let mut harness = Harness::with_peer(config, Peer::new(0));
    let packets = harness.send(connect("c"));
    assert_eq!(
        refusal(&packets).reason_code,
        ConnectReasonCode::NotAuthorized
    );
}

#[test]
fn mqtt_3_2_2_2_clean_start_1_never_resumes_a_session() {
    let mut harness = Harness::new();
    let client_id = ClientId::new("client-1").unwrap();
    harness.stored = Some(SessionState::new(client_id.clone(), 60));
    harness.auto.claim = false;
    harness.send(connect("client-1"));
    // Even if the log returned a session, Clean Start 1 discards it.
    let packets = harness.input(Input::Claimed(ClaimResult::Claimed {
        session: Some(SessionState::new(client_id, 60)),
    }));
    assert!(!refusal(&packets).session_present);
}

#[test]
fn mqtt_3_2_2_3_clean_start_0_resumes_the_session_the_claim_found() {
    let client_id = ClientId::new("client-1").unwrap();
    let mut state = SessionState::new(client_id, 300);
    state.subscriptions.push(StoredSubscription {
        filter: super::harness::filter("t/#"),
        mounted: super::harness::filter("t/#"),
        options: openqtt_core::SubOpts::new(openqtt_core::QoS::AtLeastOnce),
    });
    let mut harness = Harness::new();
    harness.stored = Some(state);
    let connack = harness.connect(connect_with("client-1", |connect| {
        connect.clean_start = false;
        connect.properties = expiry(300);
    }));
    assert!(connack.session_present);
    // The subscription is the session's again: an UNSUBSCRIBE finds it.
    let packets = harness.send(super::harness::unsubscribe(1, &["t/#"]));
    let [Packet::UnsubAck(unsuback)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(
        unsuback.reason_codes,
        [openqtt_codec::UnsubAckReasonCode::Success]
    );

    // Without a stored session, Session Present is 0.
    let mut harness = Harness::new();
    let connack = harness.connect(connect_with("client-1", |connect| {
        connect.clean_start = false
    }));
    assert!(!connack.session_present);
}

#[test]
fn mqtt_3_2_2_6_a_refusing_connack_has_session_present_0() {
    // A 0-RTT connection whose claim resumed a session, refused before its CONNACK went out.
    let mut harness = Harness::with_peer(
        Config::default(),
        Peer {
            handshake_complete: false,
            ..Peer::new(0)
        },
    );
    harness.stored = Some(SessionState::new(ClientId::new("client-1").unwrap(), 60));
    assert!(
        harness
            .send(connect_with("client-1", |connect| {
                connect.clean_start = false;
                connect.properties = expiry(60);
            }))
            .is_empty()
    );
    let packets = harness.input(Input::Shutdown(crate::Shutdown::ServerShuttingDown));
    let connack = refusal(&packets);
    assert_eq!(connack.reason_code, ConnectReasonCode::ServerUnavailable);
    assert!(!connack.session_present);
}

#[test]
fn mqtt_3_1_2_28_no_response_information_even_when_asked() {
    let mut harness = Harness::new();
    let connack = harness.connect(connect_with("client-1", |connect| {
        connect.properties.request_response_information = Some(true);
    }));
    assert_eq!(connack.properties.response_information, None);
}

#[test]
fn mqtt_3_2_2_9_maximum_qos_is_announced_only_when_lowered() {
    // covers: MQTT-3.2.2-10
    let mut harness = Harness::new();
    let connack = harness.connect(connect("c"));
    assert_eq!(connack.properties.maximum_qos, None);

    let config = Config {
        maximum_qos: QoS::AtLeastOnce,
        ..Config::default()
    };
    let mut harness = Harness::with(config);
    let connack = harness.connect(connect("c"));
    assert_eq!(connack.properties.maximum_qos, Some(QoS::AtLeastOnce));
    // A SUBSCRIBE at QoS 2 is still accepted, granted at the server's maximum.
    let packets = harness.send(subscribe1(1, "t", QoS::ExactlyOnce));
    let [Packet::SubAck(suback)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(suback.reason_codes, [SubAckReasonCode::GrantedQos1]);
}

#[test]
fn r1_o19_capabilities_are_left_out_unless_turned_off() {
    let mut harness = Harness::new();
    let connack = harness.connect(connect("c"));
    let properties = &connack.properties;
    assert_eq!(properties.receive_maximum, NonZeroU16::new(32));
    assert_eq!(
        properties.maximum_packet_size.map(|size| size.get()),
        Some(1_048_576)
    );
    assert_eq!(properties.topic_alias_maximum, Some(64));
    assert_eq!(properties.retain_available, None);
    assert_eq!(properties.wildcard_subscription_available, None);
    assert_eq!(properties.subscription_identifier_available, None);
    assert_eq!(properties.shared_subscription_available, None);
    assert_eq!(properties.server_keep_alive, None);
    assert_eq!(properties.session_expiry_interval, None);

    let config = Config {
        retain_available: false,
        wildcard_subscription_available: false,
        subscription_identifiers_available: false,
        shared_subscription_available: false,
        topic_alias_maximum: 0,
        ..Config::default()
    };
    let mut harness = Harness::with(config);
    let connack = harness.connect(connect("c"));
    let properties = &connack.properties;
    assert_eq!(properties.retain_available, Some(false));
    assert_eq!(properties.wildcard_subscription_available, Some(false));
    assert_eq!(properties.subscription_identifier_available, Some(false));
    assert_eq!(properties.shared_subscription_available, Some(false));
    assert_eq!(properties.topic_alias_maximum, None);
}

#[test]
fn mqtt_3_2_2_12_a_will_qos_above_the_maximum_gets_connack_0x9b() {
    let config = Config {
        maximum_qos: QoS::AtLeastOnce,
        ..Config::default()
    };
    let mut harness = Harness::with(config);
    let packets = harness.send(connect_with("c", |connect| {
        connect.will = Some(Will {
            qos: QoS::ExactlyOnce,
            topic: "gone".into(),
            ..Will::default()
        });
    }));
    assert_eq!(
        refusal(&packets).reason_code,
        ConnectReasonCode::QosNotSupported
    );
    assert_eq!(harness.closed(), Some(CloseCode::NoError));
}

#[test]
fn mqtt_3_2_2_13_a_retained_will_without_retain_available_gets_connack_0x9a() {
    let config = Config {
        retain_available: false,
        ..Config::default()
    };
    let mut harness = Harness::with(config);
    let packets = harness.send(connect_with("c", |connect| {
        connect.will = Some(Will {
            retain: true,
            topic: "gone".into(),
            ..Will::default()
        });
    }));
    assert_eq!(
        refusal(&packets).reason_code,
        ConnectReasonCode::RetainNotSupported
    );
}

#[test]
fn a_will_topic_that_is_not_a_topic_name_gets_connack_0x90() {
    for topic in ["a/+", "#", "a/b/#"] {
        let mut harness = Harness::new();
        let packets = harness.send(connect_with("c", |connect| {
            connect.will = Some(Will {
                topic: topic.into(),
                ..Will::default()
            });
        }));
        assert_eq!(
            refusal(&packets).reason_code,
            ConnectReasonCode::TopicNameInvalid,
            "{topic}"
        );
    }
    let mut harness = Harness::new();
    let packets = harness.send(connect_with("c", |connect| {
        connect.will = Some(Will {
            topic: "gone".into(),
            properties: WillProperties {
                response_topic: Some("reply/+".into()),
                ..WillProperties::default()
            },
            ..Will::default()
        });
    }));
    assert_eq!(
        refusal(&packets).reason_code,
        ConnectReasonCode::TopicNameInvalid
    );
}

#[test]
fn a_will_the_authorizer_denies_gets_connack_0x87() {
    let mut harness = Harness::new();
    harness.auto.authorize = Some(|_| Decision::Deny);
    let packets = harness.send(connect_with("c", |connect| {
        connect.will = Some(Will {
            topic: "commands/reboot".into(),
            ..Will::default()
        });
    }));
    assert_eq!(
        refusal(&packets).reason_code,
        ConnectReasonCode::NotAuthorized
    );
    assert!(
        !harness
            .log
            .iter()
            .any(|effect| matches!(effect, Effect::Claim(_)))
    );
}

#[test]
fn mqtt_3_2_2_22_server_keep_alive_only_when_the_clients_value_is_not_used() {
    for (requested, used) in [
        (30, None),
        (10, None),
        (1200, None),
        (0, Some(1200)),
        (5, Some(10)),
        (1201, Some(1200)),
        (u16::MAX, Some(1200)),
    ] {
        let mut harness = Harness::new();
        let connack = harness.connect(connect_with("c", |connect| connect.keep_alive = requested));
        assert_eq!(connack.properties.server_keep_alive, used, "{requested}");
    }
}

#[test]
fn r1_d18_session_expiry_is_capped_at_seven_days_and_announced_when_changed() {
    for (requested, announced, used) in [
        (None, None, 0),
        (Some(0), None, 0),
        (Some(3600), None, 3600),
        (Some(604_800), None, 604_800),
        (Some(604_801), Some(604_800), 604_800),
        (Some(u32::MAX), Some(604_800), 604_800),
    ] {
        let mut harness = Harness::new();
        let connack = harness.connect(connect_with("c", |connect| {
            connect.properties = ConnectProperties {
                session_expiry_interval: requested,
                ..ConnectProperties::default()
            };
        }));
        assert_eq!(
            connack.properties.session_expiry_interval, announced,
            "{requested:?}"
        );
        let claim = harness
            .log
            .iter()
            .find_map(|effect| match effect {
                Effect::Claim(claim) => Some(claim.session_expiry),
                _ => None,
            })
            .unwrap();
        assert_eq!(claim, used, "{requested:?}");
    }
}

#[test]
fn a_session_with_expiry_is_kept_with_its_state_and_one_without_is_discarded() {
    let mut harness = Harness::new();
    harness.connect(connect_with("c", |connect| {
        connect.properties = expiry(600)
    }));
    harness.send(subscribe1(1, "t/+", QoS::AtLeastOnce));
    harness.send(Disconnect::default());
    let releases = harness.releases();
    let [release] = releases.as_slice() else {
        panic!("one release");
    };
    let SessionEnd::Keep(state) = &release.session else {
        panic!("kept");
    };
    assert_eq!(state.expiry, 600);
    assert_eq!(state.subscriptions.len(), 1);
    assert_eq!(state.subscriptions[0].filter.as_str(), "t/+");

    let mut harness = Harness::connected();
    harness.send(Disconnect::default());
    assert_eq!(harness.releases()[0].session, SessionEnd::Discard);
}

#[test]
fn mqtt_3_1_2_29_without_problem_information_only_publish_connack_and_disconnect_carry_reasons() {
    let mut harness = Harness::new();
    harness.auto.authorize = Some(|_| Decision::Deny);
    harness.connect(connect_with("c", |connect| {
        connect.properties.request_problem_information = Some(false);
    }));
    let packets = harness.send(publish1("t", 1, "x"));
    let [Packet::PubAck(puback)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(puback.reason_code, PubAckReasonCode::NotAuthorized);
    assert_eq!(puback.properties.reason_string, None);
    let packets = harness.send(subscribe1(2, "t", QoS::AtMostOnce));
    let [Packet::SubAck(suback)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(suback.properties.reason_string, None);
    // A DISCONNECT keeps its Reason String.
    let packets = harness.send(connect("c"));
    let [Packet::Disconnect(disconnect)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(
        disconnect.properties.reason_string.as_deref(),
        Some("protocol error")
    );

    // With problem information, refusals carry their phrase (report R1, O20).
    let mut harness = Harness::new();
    harness.auto.authorize = Some(|_| Decision::Deny);
    harness.connect(connect("c"));
    let packets = harness.send(publish1("t", 1, "x"));
    let [Packet::PubAck(puback)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(
        puback.properties.reason_string.as_deref(),
        Some("not authorized")
    );
}

#[test]
fn mqtt_4_13_1_1_a_malformed_packet_after_connack_gets_disconnect_and_closes() {
    // covers: MQTT-3.14.0-1
    let mut harness = Harness::connected();
    let packets = harness.input(Input::DecodeError {
        stream: crate::StreamId::Control,
        error: Error::InvalidFlags {
            packet_type: PacketType::Publish,
            flags: 0b0110,
        },
        packet_type: Some(PacketType::Publish),
    });
    let [Packet::Disconnect(disconnect)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::MalformedPacket
    );
    assert!(harness.session.is_closed());

    // A Protocol Error after the CONNECT but before acceptance is a CONNACK, never a
    // DISCONNECT ([MQTT-3.14.0-1]).
    let mut harness = Harness::new();
    harness.auto.authenticate = false;
    harness.send(connect_with("c", |connect| {
        connect.properties.authentication_method = Some("SCRAM-SHA-256".into());
    }));
    harness.input(Input::Authenticated(crate::AuthResult::Continue {
        data: None,
    }));
    let packets = harness.send(publish0("t", "x"));
    assert_eq!(
        refusal(&packets).reason_code,
        ConnectReasonCode::ProtocolError
    );
    assert!(!harness.log.iter().any(|effect| matches!(
        effect,
        Effect::Send {
            packet: Packet::Disconnect(_),
            ..
        }
    )));
}

#[test]
fn a_packet_the_client_never_sends_is_a_protocol_error() {
    let mut harness = Harness::connected();
    let packets = harness.send(Packet::PingResp);
    let [Packet::Disconnect(disconnect)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(disconnect.reason_code, DisconnectReasonCode::ProtocolError);
}

#[test]
fn the_connect_timer_closes_a_connection_that_sends_nothing() {
    let mut harness = Harness::new();
    assert!(harness.advance(super::harness::seconds(9)).is_empty());
    assert_eq!(harness.closed(), None);
    harness.advance(super::harness::seconds(1));
    assert_eq!(harness.closed(), Some(CloseCode::ProtocolError));

    // Once connected the timer is cancelled.
    let mut harness = Harness::connected();
    harness.advance(super::harness::seconds(20));
    assert_eq!(harness.closed(), None);
}
