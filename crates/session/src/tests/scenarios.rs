//! Scenarios: a script of steps run against the harness, each saying exactly what the client
//! receives and which effects the machine asks for. A scenario reads as the exchange it checks,
//! from the first packet to the last.

use std::time::Duration;

use bytes::Bytes;
use openqtt_codec::{
    AckProperties, ConnAck, ConnAckProperties, Connect, ConnectProperties, ConnectReasonCode,
    Disconnect, DisconnectProperties, DisconnectReasonCode, Packet, PubAck, PubAckReasonCode,
    Publish, PublishProperties, QoS, SubAck, SubAckReasonCode, Will, WillProperties,
};
use openqtt_core::{ClientId, Message, QoS as CoreQoS};
use openqtt_topic::Mountpoint;

use super::harness::{Harness, at, filter, id, name, puback, subscribe1};
use crate::{
    Action, Config, Decision, Delivery, Effect, Identity, Input, Peer, SessionEnd, WillOrder,
};

/// One step of a scenario.
enum Step {
    /// The client sends a packet and then has received exactly these.
    Send(Packet, Vec<Packet>),
    /// The edge hands the machine an input, and the client then has received exactly these.
    Input(Input, Vec<Packet>),
    /// The clock moves, and the client then has received exactly these.
    Advance(Duration, Vec<Packet>),
    /// Among the effects of the step before, one satisfies this.
    Effect(&'static str, fn(&Effect) -> bool),
    /// None of the effects of the step before satisfies this.
    NoEffect(&'static str, fn(&Effect) -> bool),
}

/// Runs the steps in order, and says which step failed and why.
fn run(mut harness: Harness, steps: Vec<Step>) -> Harness {
    let mut mark = harness.log.len();
    for (index, step) in steps.into_iter().enumerate() {
        let number = index + 1;
        let (received, expected) = match step {
            Step::Send(packet, expected) => {
                mark = harness.log.len();
                (harness.send(packet), expected)
            }
            Step::Input(input, expected) => {
                mark = harness.log.len();
                (harness.input(input), expected)
            }
            Step::Advance(by, expected) => {
                mark = harness.log.len();
                (harness.advance(by), expected)
            }
            Step::Effect(what, check) => {
                assert!(
                    harness.since(mark).iter().any(check),
                    "step {number}: no effect {what} in {:#?}",
                    harness.since(mark)
                );
                continue;
            }
            Step::NoEffect(what, check) => {
                assert!(
                    !harness.since(mark).iter().any(check),
                    "step {number}: an effect {what} in {:#?}",
                    harness.since(mark)
                );
                continue;
            }
        };
        assert_eq!(
            received, expected,
            "step {number}: the client received otherwise"
        );
    }
    harness
}

/// The CONNACK R1's defaults give an accepted client, changed by `change`.
fn accepted(change: impl FnOnce(&mut ConnAckProperties)) -> Packet {
    let mut properties = ConnAckProperties {
        receive_maximum: std::num::NonZeroU16::new(32),
        maximum_packet_size: std::num::NonZeroU32::new(1 << 20),
        topic_alias_maximum: Some(64),
        ..ConnAckProperties::default()
    };
    change(&mut properties);
    Packet::from(ConnAck {
        session_present: false,
        reason_code: ConnectReasonCode::Success,
        properties,
    })
}

fn suback(packet_id: u16, codes: &[SubAckReasonCode]) -> Packet {
    Packet::SubAck(SubAck {
        packet_id: id(packet_id),
        properties: AckProperties::default(),
        reason_codes: codes.to_vec(),
    })
}

fn puback_with(packet_id: u16, reason_code: PubAckReasonCode, phrase: Option<&str>) -> Packet {
    Packet::PubAck(PubAck {
        packet_id: id(packet_id),
        reason_code,
        properties: AckProperties {
            reason_string: phrase.map(str::to_owned),
            ..AckProperties::default()
        },
    })
}

/// A PUBLISH the server sends for the first time, without properties.
fn sent(topic: &str, qos: QoS, packet_id: Option<u16>, retain: bool, payload: &str) -> Packet {
    Packet::Publish(Publish {
        dup: false,
        qos,
        retain,
        topic: topic.to_owned(),
        packet_id: packet_id.map(id),
        properties: PublishProperties::default(),
        payload: Bytes::copy_from_slice(payload.as_bytes()),
    })
}

/// A device on a certificate listener with the production mountpoint (report R2, rules 4 to
/// 7, 12, 18, 22 and 24): it connects with whatever Client Identifier, gets its CN, works in
/// its own namespace, is refused visibly, and is taken over by its next connection.
#[test]
fn a_device_on_a_certificate_listener() {
    let config = Config {
        identity: Identity::Certificate,
        mountpoint: Some(Mountpoint::parse("ingest/${username}/").unwrap()),
        ..Config::default()
    };
    let peer = Peer {
        certificate_cn: Some("acme/pump-3".into()),
        ..Peer::new(0)
    };
    let mut harness = Harness::with_peer(config, peer);
    // Rule 16 by way of the authorizer: a device may not publish retained, nor to commands.
    harness.auto.authorize = Some(|action| match action {
        Action::Publish { topic, retain, .. }
            if *retain || topic.as_str().starts_with("commands/") =>
        {
            Decision::Deny
        }
        _ => Decision::Allow,
    });
    // A retained command waits for the device.
    let mut command = Message::new(
        name("ingest/acme/pump-3/commands/firmware"),
        Bytes::from_static(b"v2"),
    );
    command.qos = CoreQoS::AtLeastOnce;
    command.retain = true;
    command.publisher = ClientId::new("platform").ok();
    harness.retained.insert(command.topic.clone(), command);

    let connect = Connect {
        clean_start: true,
        keep_alive: 30,
        client_id: "anything".into(),
        username: Some("mallory".into()),
        will: Some(Will {
            topic: "status".into(),
            payload: Bytes::from_static(b"offline"),
            properties: WillProperties {
                will_delay_interval: Some(5),
                ..WillProperties::default()
            },
            ..Will::default()
        }),
        ..Connect::default()
    };
    let harness = run(
        harness,
        vec![
            // Rule 4: the CN names the session and comes back as the Assigned Client
            // Identifier.
            Step::Send(
                connect.into(),
                vec![accepted(|properties| {
                    properties.assigned_client_identifier = Some("acme/pump-3".into());
                })],
            ),
            Step::Effect("claim of the CN with the will mounted", |effect| {
                matches!(effect, Effect::Claim(claim)
                    if claim.client_id.as_str() == "acme/pump-3"
                        && claim.will.as_ref().is_some_and(|will| will.message.topic.as_str() == "ingest/acme/pump-3/status"))
            }),
            // Rules 6 and 18: subscribed in its namespace, the retained command right after the
            // SUBACK, as the device names it.
            Step::Send(
                subscribe1(1, "commands/#", QoS::AtLeastOnce).into(),
                vec![
                    suback(1, &[SubAckReasonCode::GrantedQos1]),
                    sent("commands/firmware", QoS::AtLeastOnce, Some(1), true, "v2"),
                ],
            ),
            Step::Effect("interest in the mounted filter", |effect| {
                matches!(effect, Effect::Subscribe(interest)
                    if interest.filter.as_str() == "ingest/acme/pump-3/commands/#")
            }),
            Step::Send(puback(1).into(), vec![]),
            // Rule 7: telemetry is authorized as the device names it, and published mounted.
            Step::Send(
                super::harness::publish1("temperature", 2, "21.5").into(),
                vec![puback_with(
                    2,
                    PubAckReasonCode::NoMatchingSubscribers,
                    None,
                )],
            ),
            Step::Effect("telemetry published in the namespace", |effect| {
                matches!(effect, Effect::Publish(publication)
                    if publication.message.topic.as_str() == "ingest/acme/pump-3/temperature")
            }),
            // Rules 12, 13 and 16: refusals are visible, and the connection stays.
            Step::Send(
                super::harness::publish1("commands/reboot", 3, "now").into(),
                vec![puback_with(
                    3,
                    PubAckReasonCode::NotAuthorized,
                    Some("not authorized"),
                )],
            ),
            Step::Send(
                super::harness::retained("status", QoS::AtLeastOnce, Some(4), "up").into(),
                vec![puback_with(
                    4,
                    PubAckReasonCode::NotAuthorized,
                    Some("not authorized"),
                )],
            ),
            // A live command from the platform reaches it, the mountpoint stripped.
            Step::Input(
                Input::Deliver(Delivery {
                    message: {
                        let mut live = Message::new(
                            name("ingest/acme/pump-3/commands/restart"),
                            Bytes::from_static(b"go"),
                        );
                        live.qos = CoreQoS::AtLeastOnce;
                        live
                    },
                    subscriptions: vec![filter("ingest/acme/pump-3/commands/#")],
                }),
                vec![sent(
                    "commands/restart",
                    QoS::AtLeastOnce,
                    Some(2),
                    false,
                    "go",
                )],
            ),
            Step::Send(puback(2).into(), vec![]),
            // Rule 24: silent for 44 seconds, it is still connected.
            Step::Advance(Duration::from_secs(44), vec![]),
            // Rule 22: its next connection takes the session over.
            Step::Input(
                Input::StepDown {
                    session_ends: false,
                },
                vec![Packet::Disconnect(Disconnect {
                    reason_code: DisconnectReasonCode::SessionTakenOver,
                    properties: DisconnectProperties {
                        reason_string: Some("session taken over".into()),
                        ..DisconnectProperties::default()
                    },
                })],
            ),
            // Within its will delay: no will.
            Step::NoEffect("a will order", |effect| matches!(effect, Effect::Will(_))),
            Step::Effect("the claim released", |effect| {
                matches!(effect, Effect::Release(_))
            }),
        ],
    );
    assert!(harness.session.is_closed());
}

/// A session that lives past its connection: it subscribes, loses the connection with a
/// message in flight, and its next connection resumes it and receives the message again.
#[test]
fn a_session_resumed_on_a_new_connection() {
    let with_expiry = |clean_start: bool| Connect {
        clean_start,
        keep_alive: 60,
        client_id: "sensor-17".into(),
        properties: ConnectProperties {
            session_expiry_interval: Some(3600),
            ..ConnectProperties::default()
        },
        ..Connect::default()
    };
    let first = run(
        Harness::new(),
        vec![
            Step::Send(with_expiry(true).into(), vec![accepted(|_| {})]),
            Step::Send(
                subscribe1(1, "alerts/+", QoS::ExactlyOnce).into(),
                vec![suback(1, &[SubAckReasonCode::GrantedQos2])],
            ),
            Step::Input(
                Input::Deliver(Delivery {
                    message: super::harness::message(
                        "alerts/fire",
                        CoreQoS::ExactlyOnce,
                        "evacuate",
                    ),
                    subscriptions: vec![filter("alerts/+")],
                }),
                vec![sent(
                    "alerts/fire",
                    QoS::ExactlyOnce,
                    Some(1),
                    false,
                    "evacuate",
                )],
            ),
            Step::Input(Input::TransportClosed, vec![]),
        ],
    );
    let SessionEnd::Keep(state) = &first.releases()[0].session else {
        panic!("the session outlives its connection");
    };
    let mut second = Harness::new();
    second.stored = Some(state.clone());
    second.now = at(600);
    let resent = Publish {
        dup: true,
        ..match sent("alerts/fire", QoS::ExactlyOnce, Some(1), false, "evacuate") {
            Packet::Publish(publish) => publish,
            _ => unreachable!(),
        }
    };
    let second = run(
        second,
        vec![
            Step::Send(
                with_expiry(false).into(),
                vec![
                    {
                        let Packet::ConnAck(mut connack) = accepted(|_| {}) else {
                            unreachable!()
                        };
                        connack.session_present = true;
                        Packet::ConnAck(connack)
                    },
                    Packet::Publish(resent),
                ],
            ),
            Step::Send(
                super::harness::pubrec(1).into(),
                vec![Packet::PubRel(openqtt_codec::PubRel::new(id(1)))],
            ),
            Step::Send(super::harness::pubcomp(1).into(), vec![]),
            // The subscription came back with the session.
            Step::Send(
                super::harness::unsubscribe(2, &["alerts/+"]).into(),
                vec![Packet::UnsubAck(openqtt_codec::UnsubAck {
                    packet_id: id(2),
                    properties: AckProperties::default(),
                    reason_codes: vec![openqtt_codec::UnsubAckReasonCode::Success],
                })],
            ),
            Step::Send(Disconnect::default().into(), vec![]),
            Step::NoEffect("a will", |effect| {
                matches!(effect, Effect::Will(WillOrder::Publish { .. }))
            }),
        ],
    );
    assert!(second.session.is_closed());
}
