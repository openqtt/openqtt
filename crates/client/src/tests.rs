//! The client against a scripted server over an in-memory stream, on a paused clock, so every
//! exchange and every timer can be checked exactly.

use std::io;
use std::num::{NonZeroU16, NonZeroU32};
use std::sync::Mutex;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use openqtt_codec::{
    AckProperties, ConnAck, ConnAckProperties, Connect, ConnectReasonCode, Decoder, Disconnect,
    DisconnectReasonCode, Packet, PacketId, PubAck, PubComp, PubCompReasonCode, PubRec,
    PubRecReasonCode, PubRel, Publish, PublishProperties, QoS, Sender, SubAck, SubAckReasonCode,
    SubscriptionId, SubscriptionOptions, UnsubAck, UnsubAckReasonCode,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::time::Instant;

use crate::transport::{BoxFuture, CloseCode, Link, LinkHandle, Transport};
use crate::{Client, CloseReason, ConnectOptions, Error, Event, Events, Published, Session};

/// A transport that hands out one end of an in-memory stream.
struct Duplex(Mutex<Option<DuplexStream>>);

impl Transport for Duplex {
    fn connect(&self) -> BoxFuture<'_, io::Result<Link>> {
        let stream = self.0.lock().unwrap().take();
        Box::pin(async move {
            let stream = stream.ok_or_else(|| io::Error::other("already connected"))?;
            let (reader, writer) = tokio::io::split(stream);
            Ok(Link::new(reader, writer, Unclosable))
        })
    }
}

/// An in-memory stream has nothing to close beyond its halves.
struct Unclosable;

impl LinkHandle for Unclosable {
    fn close(&self, _: CloseCode) {}

    fn closed(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// The server's end: reads what the client sends and writes what the test scripts.
struct Server {
    stream: DuplexStream,
    buffer: BytesMut,
}

impl Server {
    /// The next packet from the client; panics after a minute of virtual time.
    async fn recv(&mut self) -> Packet {
        let decoder = Decoder::new().with_sender(Sender::Client);
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if let Some(packet) = decoder.decode(&mut self.buffer).unwrap() {
                    return packet;
                }
                let read = self.stream.read_buf(&mut self.buffer).await.unwrap();
                assert_ne!(read, 0, "the client closed the stream");
            }
        })
        .await
        .expect("a packet from the client")
    }

    /// Whether the client sends nothing for `wait` of virtual time.
    async fn silent_for(&mut self, wait: Duration) -> bool {
        let decoder = Decoder::new().with_sender(Sender::Client);
        if decoder.decode(&mut self.buffer.clone()).unwrap().is_some() {
            return false;
        }
        tokio::time::timeout(wait, self.stream.read_buf(&mut self.buffer))
            .await
            .is_err()
    }

    async fn send(&mut self, packet: impl Into<Packet>) {
        let mut out = BytesMut::new();
        packet.into().encode(&mut out).unwrap();
        self.stream.write_all(&out).await.unwrap();
    }

    async fn send_bytes(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
    }
}

fn pid(value: u16) -> PacketId {
    PacketId::new(value).unwrap()
}

fn qos1(topic: &str, payload: &'static str) -> Publish {
    Publish {
        qos: QoS::AtLeastOnce,
        topic: topic.into(),
        payload: Bytes::from_static(payload.as_bytes()),
        ..Publish::default()
    }
}

/// Connects `options` to a fresh server that answers with `connack`, and returns the CONNECT
/// it received.
async fn connect_with(
    options: ConnectOptions,
    connack: ConnAck,
) -> (Client, Events, Server, Connect) {
    let (client_end, server_end) = tokio::io::duplex(64 * 1024);
    connect_over(client_end, server_end, options, connack).await
}

async fn connect_over(
    client_end: DuplexStream,
    server_end: DuplexStream,
    options: ConnectOptions,
    connack: ConnAck,
) -> (Client, Events, Server, Connect) {
    let transport = Duplex(Mutex::new(Some(client_end)));
    let mut server = Server {
        stream: server_end,
        buffer: BytesMut::new(),
    };
    let (connected, connect) = tokio::join!(Client::connect(&transport, options), async {
        let Packet::Connect(connect) = server.recv().await else {
            panic!("CONNECT comes first");
        };
        server.send(connack).await;
        *connect
    });
    let (client, events) = connected.unwrap();
    (client, events, server, connect)
}

async fn connect(options: ConnectOptions) -> (Client, Events, Server) {
    let (client, events, server, _) = connect_with(options, ConnAck::default()).await;
    (client, events, server)
}

#[tokio::test(start_paused = true)]
async fn connect_sends_every_option_and_returns_the_connack() {
    let options = ConnectOptions::new("sensor-1")
        .clean_start(false)
        .keep_alive(30)
        .session_expiry_interval(3_600)
        .receive_maximum(NonZeroU16::new(10).unwrap())
        .maximum_packet_size(NonZeroU32::new(4_096).unwrap())
        .topic_alias_maximum(8)
        .request_response_information(true)
        .request_problem_information(false)
        .user_property("site", "lab")
        .will(crate::Will {
            topic: "status/sensor-1".into(),
            payload: Bytes::from_static(b"gone"),
            ..crate::Will::default()
        })
        .credentials("user", "pass");
    let expected = options.connect_packet().clone();
    let connack = ConnAck {
        properties: ConnAckProperties {
            assigned_client_identifier: Some("ignored-when-an-id-was-sent".into()),
            ..ConnAckProperties::default()
        },
        ..ConnAck::default()
    };
    let (client, _events, _server, connect) = connect_with(options, connack.clone()).await;
    assert_eq!(connect, expected);
    assert!(!connect.clean_start);
    assert_eq!(connect.properties.topic_alias_maximum, Some(8));
    assert_eq!(client.connack(), &connack);
    assert_eq!(client.client_id(), "ignored-when-an-id-was-sent");
}

#[tokio::test(start_paused = true)]
async fn a_refusing_connack_is_an_error() {
    let (client_end, server_end) = tokio::io::duplex(1024);
    let transport = Duplex(Mutex::new(Some(client_end)));
    let mut server = Server {
        stream: server_end,
        buffer: BytesMut::new(),
    };
    let (result, ()) = tokio::join!(
        Client::connect(&transport, ConnectOptions::new("c")),
        async {
            server.recv().await;
            server
                .send(ConnAck {
                    reason_code: ConnectReasonCode::NotAuthorized,
                    ..ConnAck::default()
                })
                .await;
        }
    );
    let Err(Error::Refused(connack)) = result else {
        panic!("refused");
    };
    assert_eq!(connack.reason_code, ConnectReasonCode::NotAuthorized);
}

#[tokio::test(start_paused = true)]
async fn a_packet_before_connack_is_refused() {
    let (client_end, server_end) = tokio::io::duplex(1024);
    let transport = Duplex(Mutex::new(Some(client_end)));
    let mut server = Server {
        stream: server_end,
        buffer: BytesMut::new(),
    };
    let (result, ()) = tokio::join!(
        Client::connect(&transport, ConnectOptions::new("c")),
        async {
            server.recv().await;
            server.send(Packet::PingResp).await;
        }
    );
    assert!(matches!(
        result,
        Err(Error::UnexpectedPacket(openqtt_codec::PacketType::PingResp))
    ));
}

#[tokio::test(start_paused = true)]
async fn no_connack_in_time_is_a_timeout() {
    let (client_end, _server_end) = tokio::io::duplex(1024);
    let transport = Duplex(Mutex::new(Some(client_end)));
    let options = ConnectOptions::new("c").connect_timeout(Duration::from_secs(3));
    let start = Instant::now();
    let result = Client::connect(&transport, options).await;
    assert!(matches!(result, Err(Error::Timeout("CONNACK"))));
    assert_eq!(start.elapsed(), Duration::from_secs(3));
}

#[tokio::test(start_paused = true)]
async fn mqtt_3_1_2_20_a_pingreq_follows_a_keep_alive_without_packets() {
    let (client, _events, mut server) = connect(ConnectOptions::new("c").keep_alive(10)).await;
    let start = Instant::now();
    assert_eq!(server.recv().await, Packet::PingReq);
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    server.send(Packet::PingResp).await;
    assert_eq!(server.recv().await, Packet::PingReq);
    assert_eq!(start.elapsed(), Duration::from_secs(20));
    server.send(Packet::PingResp).await;

    // Anything sent counts: a PUBLISH at 25 s moves the next PINGREQ to 35 s.
    tokio::time::sleep_until(start + Duration::from_secs(25)).await;
    client
        .publish(Publish {
            topic: "t".into(),
            ..Publish::default()
        })
        .await
        .unwrap();
    assert!(matches!(server.recv().await, Packet::Publish(_)));
    assert_eq!(server.recv().await, Packet::PingReq);
    assert_eq!(start.elapsed(), Duration::from_secs(35));
}

// covers: MQTT-3.2.2-21
#[tokio::test(start_paused = true)]
async fn mqtt_3_1_2_21_the_server_keep_alive_replaces_the_clients() {
    let connack = ConnAck {
        properties: ConnAckProperties {
            server_keep_alive: Some(4),
            ..ConnAckProperties::default()
        },
        ..ConnAck::default()
    };
    let (_client, _events, mut server, _) =
        connect_with(ConnectOptions::new("c").keep_alive(60), connack).await;
    let start = Instant::now();
    assert_eq!(server.recv().await, Packet::PingReq);
    assert_eq!(start.elapsed(), Duration::from_secs(4));
}

#[tokio::test(start_paused = true)]
async fn keep_alive_0_sends_no_pingreq() {
    let (_client, _events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    assert!(server.silent_for(Duration::from_secs(3_600)).await);
}

#[tokio::test(start_paused = true)]
async fn a_pingreq_left_unanswered_ends_the_connection() {
    let options = ConnectOptions::new("c")
        .keep_alive(10)
        .ping_timeout(Duration::from_secs(5));
    let (client, mut events, mut server) = connect(options).await;
    assert_eq!(server.recv().await, Packet::PingReq);
    let start = Instant::now();
    let Some(Event::Closed(CloseReason::Lost(_))) = events.recv().await else {
        panic!("the connection is lost");
    };
    assert_eq!(start.elapsed(), Duration::from_secs(5));
    assert!(client.is_closed());
}

#[tokio::test(start_paused = true)]
async fn mqtt_3_2_2_4_session_present_without_session_state_closes_the_connection() {
    let (client_end, server_end) = tokio::io::duplex(1024);
    let transport = Duplex(Mutex::new(Some(client_end)));
    let mut server = Server {
        stream: server_end,
        buffer: BytesMut::new(),
    };
    let (result, ()) = tokio::join!(
        Client::connect(&transport, ConnectOptions::new("c").clean_start(false)),
        async {
            server.recv().await;
            server
                .send(ConnAck {
                    session_present: true,
                    ..ConnAck::default()
                })
                .await;
        }
    );
    assert!(matches!(result, Err(Error::UnexpectedSessionPresent)));
}

/// Publishes QoS 1 "a", "b" and "c" and a QoS 2 "d", acknowledges none but the PUBREC of "d",
/// and returns the session the client keeps.
async fn session_with_four_in_flight() -> Session {
    let (client, _events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    let mut ids = Vec::new();
    for (topic, qos) in [
        ("a", QoS::AtLeastOnce),
        ("b", QoS::AtLeastOnce),
        ("c", QoS::AtLeastOnce),
        ("d", QoS::ExactlyOnce),
    ] {
        let publisher = client.clone();
        tokio::spawn(async move {
            drop(
                publisher
                    .publish(Publish {
                        qos,
                        ..qos1(topic, "x")
                    })
                    .await,
            );
        });
        let Packet::Publish(publish) = server.recv().await else {
            panic!("a PUBLISH");
        };
        assert_eq!(publish.topic, topic);
        assert!(!publish.dup);
        ids.push(publish.packet_id.unwrap());
    }
    server.send(PubRec::new(ids[3])).await;
    assert_eq!(server.recv().await, Packet::PubRel(PubRel::new(ids[3])));
    let session = client.disconnect().await.unwrap();
    assert_eq!(session.unacknowledged(), 4);
    session
}

// covers: MQTT-4.6.0-4
#[tokio::test(start_paused = true)]
async fn mqtt_4_6_0_1_a_resumed_session_resends_in_the_original_order() {
    let session = session_with_four_in_flight().await;
    let resumed = ConnAck {
        session_present: true,
        ..ConnAck::default()
    };
    let (client, _events, mut server, connect) =
        connect_with(ConnectOptions::new("ignored").resume(session), resumed).await;
    assert!(!connect.clean_start);
    assert_eq!(connect.client_id, "c");

    // PUBREL for the message that had its PUBREC, then the PUBLISH packets in the order they
    // were first sent, each with DUP and its first identifier.
    let Packet::PubRel(pubrel) = server.recv().await else {
        panic!("PUBREL first");
    };
    let mut resent = Vec::new();
    for _ in 0..3 {
        let Packet::Publish(publish) = server.recv().await else {
            panic!("a PUBLISH");
        };
        assert!(publish.dup);
        resent.push((publish.topic, publish.packet_id.unwrap()));
    }
    assert_eq!(
        resent.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>(),
        ["a", "b", "c"]
    );
    for (_, id) in &resent {
        server.send(PubAck::new(*id)).await;
    }
    server.send(PubComp::new(pubrel.packet_id)).await;

    // A new message takes an identifier none of those hold.
    let publisher = client.clone();
    let published = tokio::spawn(async move { publisher.publish(qos1("e", "y")).await });
    let Packet::Publish(publish) = server.recv().await else {
        panic!("a PUBLISH");
    };
    server.send(PubAck::new(publish.packet_id.unwrap())).await;
    assert!(published.await.unwrap().unwrap().is_accepted());
    assert_eq!(client.disconnect().await.unwrap().unacknowledged(), 0);
}

#[tokio::test(start_paused = true)]
async fn mqtt_3_2_2_5_session_present_0_discards_the_session_state() {
    let session = session_with_four_in_flight().await;
    let (client, _events, mut server, _) =
        connect_with(ConnectOptions::new("c").resume(session), ConnAck::default()).await;
    // Nothing is resent.
    assert!(server.silent_for(Duration::from_secs(5)).await);
    assert_eq!(client.disconnect().await.unwrap().unacknowledged(), 0);
}

// covers: MQTT-3.3.4-8
#[tokio::test(start_paused = true)]
async fn mqtt_3_3_4_7_qos_1_and_2_wait_for_the_servers_receive_maximum() {
    let connack = ConnAck {
        properties: ConnAckProperties {
            receive_maximum: NonZeroU16::new(2),
            ..ConnAckProperties::default()
        },
        ..ConnAck::default()
    };
    let (client, _events, mut server, _) =
        connect_with(ConnectOptions::new("c").keep_alive(0), connack).await;
    let mut pending = Vec::new();
    for topic in ["a", "b", "c"] {
        let publisher = client.clone();
        pending.push(tokio::spawn(async move {
            publisher.publish(qos1(topic, "x")).await
        }));
    }
    let mut ids = Vec::new();
    for _ in 0..2 {
        let Packet::Publish(publish) = server.recv().await else {
            panic!("a PUBLISH");
        };
        ids.push(publish.packet_id.unwrap());
    }
    // The quota is used up: the third waits, but QoS 0 and SUBSCRIBE still go out.
    assert!(server.silent_for(Duration::from_secs(1)).await);
    client
        .publish(Publish {
            topic: "q0".into(),
            ..Publish::default()
        })
        .await
        .unwrap();
    let Packet::Publish(qos0) = server.recv().await else {
        panic!("the QoS 0 PUBLISH");
    };
    assert_eq!(qos0.topic, "q0");
    let subscriber = client.clone();
    let subscribed = tokio::spawn(async move {
        subscriber
            .subscribe("f", SubscriptionOptions::default())
            .await
    });
    let Packet::Subscribe(subscribe) = server.recv().await else {
        panic!("SUBSCRIBE");
    };
    server
        .send(SubAck {
            packet_id: subscribe.packet_id,
            properties: AckProperties::default(),
            reason_codes: vec![SubAckReasonCode::GrantedQos0],
        })
        .await;
    subscribed.await.unwrap().unwrap();

    // A PUBACK frees a slot, and the third message goes.
    server.send(PubAck::new(ids[0])).await;
    let Packet::Publish(third) = server.recv().await else {
        panic!("the third PUBLISH");
    };
    assert_eq!(third.topic, "c");
    server.send(PubAck::new(ids[1])).await;
    server.send(PubAck::new(third.packet_id.unwrap())).await;
    for result in pending {
        assert!(result.await.unwrap().unwrap().is_accepted());
    }
}

// covers: MQTT-4.6.0-2, MQTT-4.6.0-3
#[tokio::test(start_paused = true)]
async fn mqtt_4_5_0_2_every_publish_is_acknowledged_in_arrival_order() {
    let (_client, events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    // The application never reads a message: acknowledgements go out regardless.
    drop(events);
    for (qos, id) in [
        (QoS::AtLeastOnce, 7),
        (QoS::AtLeastOnce, 3),
        (QoS::ExactlyOnce, 9),
        (QoS::ExactlyOnce, 4),
    ] {
        server
            .send(Publish {
                qos,
                packet_id: Some(pid(id)),
                ..qos1("t", "x")
            })
            .await;
    }
    assert_eq!(server.recv().await, Packet::PubAck(PubAck::new(pid(7))));
    assert_eq!(server.recv().await, Packet::PubAck(PubAck::new(pid(3))));
    assert_eq!(server.recv().await, Packet::PubRec(PubRec::new(pid(9))));
    assert_eq!(server.recv().await, Packet::PubRec(PubRec::new(pid(4))));
}

#[tokio::test(start_paused = true)]
async fn a_qos_2_message_repeated_before_pubrel_is_delivered_once() {
    let (_client, mut events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    let message = Publish {
        qos: QoS::ExactlyOnce,
        packet_id: Some(pid(5)),
        ..qos1("t", "once")
    };
    server.send(message.clone()).await;
    assert_eq!(server.recv().await, Packet::PubRec(PubRec::new(pid(5))));
    server
        .send(Publish {
            dup: true,
            ..message.clone()
        })
        .await;
    assert_eq!(server.recv().await, Packet::PubRec(PubRec::new(pid(5))));
    server.send(PubRel::new(pid(5))).await;
    assert_eq!(server.recv().await, Packet::PubComp(PubComp::new(pid(5))));
    // A PUBREL for an identifier no longer held gets 0x92.
    server.send(PubRel::new(pid(5))).await;
    assert_eq!(
        server.recv().await,
        Packet::PubComp(PubComp {
            reason_code: PubCompReasonCode::PacketIdentifierNotFound,
            ..PubComp::new(pid(5))
        })
    );
    assert_eq!(events.next_message().await, Some(message));
    // No second delivery: the next event is the close.
    server.send(Disconnect::default()).await;
    assert!(matches!(
        events.recv().await,
        Some(Event::Closed(CloseReason::ByServer(_)))
    ));
}

#[tokio::test(start_paused = true)]
async fn qos_2_completes_with_pubrec_pubrel_pubcomp() {
    let (client, _events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    let publisher = client.clone();
    let published = tokio::spawn(async move {
        publisher
            .publish(Publish {
                qos: QoS::ExactlyOnce,
                ..qos1("t", "x")
            })
            .await
    });
    let Packet::Publish(publish) = server.recv().await else {
        panic!("a PUBLISH");
    };
    let id = publish.packet_id.unwrap();
    server.send(PubRec::new(id)).await;
    assert_eq!(server.recv().await, Packet::PubRel(PubRel::new(id)));
    server.send(PubComp::new(id)).await;
    assert_eq!(
        published.await.unwrap().unwrap(),
        Published::ExactlyOnce {
            pubrec: PubRec::new(id),
            pubcomp: Some(PubComp::new(id)),
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_failure_pubrec_ends_the_exchange_without_pubrel() {
    let (client, _events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    let publisher = client.clone();
    let published = tokio::spawn(async move {
        publisher
            .publish(Publish {
                qos: QoS::ExactlyOnce,
                ..qos1("t", "x")
            })
            .await
    });
    let Packet::Publish(publish) = server.recv().await else {
        panic!("a PUBLISH");
    };
    let refusal = PubRec {
        reason_code: PubRecReasonCode::NotAuthorized,
        ..PubRec::new(publish.packet_id.unwrap())
    };
    server.send(refusal.clone()).await;
    let outcome = published.await.unwrap().unwrap();
    assert_eq!(
        outcome,
        Published::ExactlyOnce {
            pubrec: refusal,
            pubcomp: None,
        }
    );
    assert!(!outcome.is_accepted());
    assert!(server.silent_for(Duration::from_secs(1)).await);
    assert_eq!(client.disconnect().await.unwrap().unacknowledged(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_puback_refusal_is_returned_not_an_error() {
    let (client, _events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    let publisher = client.clone();
    let published = tokio::spawn(async move { publisher.publish(qos1("t", "x")).await });
    let Packet::Publish(publish) = server.recv().await else {
        panic!("a PUBLISH");
    };
    let refusal = PubAck {
        reason_code: openqtt_codec::PubAckReasonCode::NotAuthorized,
        ..PubAck::new(publish.packet_id.unwrap())
    };
    server.send(refusal.clone()).await;
    assert_eq!(
        published.await.unwrap().unwrap(),
        Published::AtLeastOnce(refusal)
    );
}

#[tokio::test(start_paused = true)]
async fn mqtt_3_2_2_15_a_packet_over_the_servers_maximum_is_not_sent() {
    let connack = ConnAck {
        properties: ConnAckProperties {
            maximum_packet_size: NonZeroU32::new(64),
            ..ConnAckProperties::default()
        },
        ..ConnAck::default()
    };
    let (client, _events, mut server, _) =
        connect_with(ConnectOptions::new("c").keep_alive(0), connack).await;
    let large = Publish {
        topic: "t".into(),
        payload: Bytes::from(vec![0; 100]),
        ..Publish::default()
    };
    assert!(matches!(
        client.publish(large).await,
        Err(Error::Invalid(openqtt_codec::Error::PacketTooLarge {
            maximum: 64,
            ..
        }))
    ));
    // Exactly at the limit is allowed: 2 + 2 + 1 + 1 + 58 bytes.
    let at_limit = Publish {
        topic: "t".into(),
        payload: Bytes::from(vec![0; 58]),
        ..Publish::default()
    };
    assert_eq!(Packet::Publish(at_limit.clone()).encoded_len(), Ok(64));
    client.publish(at_limit.clone()).await.unwrap();
    assert_eq!(server.recv().await, Packet::Publish(at_limit));
}

// covers: MQTT-3.3.2-9
#[tokio::test(start_paused = true)]
async fn mqtt_3_2_2_17_a_topic_alias_above_the_servers_maximum_is_not_sent() {
    let connack = ConnAck {
        properties: ConnAckProperties {
            topic_alias_maximum: Some(2),
            ..ConnAckProperties::default()
        },
        ..ConnAck::default()
    };
    let (client, _events, mut server, _) =
        connect_with(ConnectOptions::new("c").keep_alive(0), connack).await;
    let aliased = |topic: &str, alias: u16| Publish {
        topic: topic.into(),
        properties: PublishProperties {
            topic_alias: NonZeroU16::new(alias),
            ..PublishProperties::default()
        },
        ..Publish::default()
    };
    assert!(matches!(
        client.publish(aliased("t", 3)).await,
        Err(Error::TopicAlias {
            alias: 3,
            maximum: 2
        })
    ));
    // An alias alone needs a topic mapped to it on this connection first.
    assert!(matches!(
        client.publish(aliased("", 2)).await,
        Err(Error::TopicAlias { alias: 2, .. })
    ));
    client.publish(aliased("t", 2)).await.unwrap();
    client.publish(aliased("", 2)).await.unwrap();
    assert_eq!(server.recv().await, Packet::Publish(aliased("t", 2)));
    assert_eq!(server.recv().await, Packet::Publish(aliased("", 2)));
}

#[tokio::test(start_paused = true)]
async fn mqtt_3_2_2_18_no_topic_alias_without_a_server_maximum() {
    let (client, _events, _server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    let aliased = Publish {
        topic: "t".into(),
        properties: PublishProperties {
            topic_alias: NonZeroU16::new(1),
            ..PublishProperties::default()
        },
        ..Publish::default()
    };
    assert!(matches!(
        client.publish(aliased).await,
        Err(Error::TopicAlias {
            alias: 1,
            maximum: 0
        })
    ));
}

#[tokio::test(start_paused = true)]
async fn mqtt_3_3_2_10_every_alias_up_to_the_clients_maximum_is_resolved() {
    let (_client, mut events, mut server) = connect(
        ConnectOptions::new("c")
            .keep_alive(0)
            .topic_alias_maximum(3),
    )
    .await;
    let aliased = |topic: &str, alias: u16| Publish {
        topic: topic.into(),
        properties: PublishProperties {
            topic_alias: NonZeroU16::new(alias),
            ..PublishProperties::default()
        },
        ..Publish::default()
    };
    for (topic, alias) in [("a", 1), ("b", 3), ("", 1), ("", 3), ("c", 1), ("", 1)] {
        server.send(aliased(topic, alias)).await;
    }
    let mut topics = Vec::new();
    for _ in 0..6 {
        topics.push(events.next_message().await.unwrap().topic);
    }
    assert_eq!(topics, ["a", "b", "a", "b", "c", "c"]);

    // Above the maximum: DISCONNECT 0x94.
    server.send(aliased("d", 4)).await;
    assert_eq!(
        server.recv().await,
        Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::TopicAliasInvalid,
            ..Disconnect::default()
        })
    );
    assert!(matches!(
        events.recv().await,
        Some(Event::Closed(CloseReason::ProtocolError {
            reason_code: DisconnectReasonCode::TopicAliasInvalid,
            ..
        }))
    ));
}

#[tokio::test(start_paused = true)]
async fn mqtt_3_3_4_6_a_client_publish_carries_no_subscription_identifier() {
    let (client, _events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    let with_identifier = Publish {
        topic: "t".into(),
        properties: PublishProperties {
            subscription_identifiers: vec![SubscriptionId::new(1).unwrap()],
            ..PublishProperties::default()
        },
        ..Publish::default()
    };
    assert!(matches!(
        client.publish(with_identifier).await,
        Err(Error::Invalid(
            openqtt_codec::Error::PropertyNotSentBy { .. }
        ))
    ));
    assert!(server.silent_for(Duration::from_secs(1)).await);
}

#[tokio::test(start_paused = true)]
async fn mqtt_3_2_2_11_a_qos_above_the_servers_maximum_is_not_sent() {
    let connack = ConnAck {
        properties: ConnAckProperties {
            maximum_qos: Some(QoS::AtLeastOnce),
            ..ConnAckProperties::default()
        },
        ..ConnAck::default()
    };
    let (client, _events, _server, _) =
        connect_with(ConnectOptions::new("c").keep_alive(0), connack).await;
    let result = client
        .publish(Publish {
            qos: QoS::ExactlyOnce,
            ..qos1("t", "x")
        })
        .await;
    assert!(matches!(
        result,
        Err(Error::QosNotSupported {
            requested: QoS::ExactlyOnce,
            maximum: QoS::AtLeastOnce
        })
    ));
}

#[tokio::test(start_paused = true)]
async fn mqtt_3_2_2_14_retain_is_not_sent_when_unavailable() {
    let connack = ConnAck {
        properties: ConnAckProperties {
            retain_available: Some(false),
            ..ConnAckProperties::default()
        },
        ..ConnAck::default()
    };
    let (client, _events, _server, _) =
        connect_with(ConnectOptions::new("c").keep_alive(0), connack).await;
    let result = client
        .publish(Publish {
            retain: true,
            topic: "t".into(),
            ..Publish::default()
        })
        .await;
    assert!(matches!(result, Err(Error::RetainNotSupported)));
}

#[tokio::test(start_paused = true)]
async fn subscribe_and_unsubscribe_return_their_acknowledgements() {
    let (client, _events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    let subscriber = client.clone();
    let subscribed = tokio::spawn(async move {
        subscriber
            .subscribe(
                "a/+",
                SubscriptionOptions {
                    maximum_qos: QoS::AtLeastOnce,
                    no_local: true,
                    ..SubscriptionOptions::default()
                },
            )
            .await
    });
    let Packet::Subscribe(subscribe) = server.recv().await else {
        panic!("SUBSCRIBE");
    };
    assert_eq!(subscribe.subscriptions[0].filter, "a/+");
    assert!(subscribe.subscriptions[0].options.no_local);
    let suback = SubAck {
        packet_id: subscribe.packet_id,
        properties: AckProperties::default(),
        reason_codes: vec![SubAckReasonCode::GrantedQos1],
    };
    server.send(suback.clone()).await;
    assert_eq!(subscribed.await.unwrap().unwrap(), suback);

    let unsubscriber = client.clone();
    let unsubscribed = tokio::spawn(async move { unsubscriber.unsubscribe("a/+").await });
    let Packet::Unsubscribe(unsubscribe) = server.recv().await else {
        panic!("UNSUBSCRIBE");
    };
    assert_eq!(unsubscribe.filters, ["a/+"]);
    let unsuback = UnsubAck {
        packet_id: unsubscribe.packet_id,
        properties: AckProperties::default(),
        reason_codes: vec![UnsubAckReasonCode::Success],
    };
    server.send(unsuback.clone()).await;
    assert_eq!(unsubscribed.await.unwrap().unwrap(), unsuback);
}

#[tokio::test(start_paused = true)]
async fn the_packet_log_carries_every_packet_from_the_server() {
    let (client, mut events, mut server) =
        connect(ConnectOptions::new("c").keep_alive(5).packet_log(true)).await;
    assert_eq!(
        events.recv().await,
        Some(Event::Received(Packet::from(ConnAck::default())))
    );
    assert_eq!(server.recv().await, Packet::PingReq);
    server.send(Packet::PingResp).await;
    assert_eq!(events.recv().await, Some(Event::Received(Packet::PingResp)));
    let message = Publish {
        topic: "t".into(),
        ..Publish::default()
    };
    server.send(message.clone()).await;
    assert_eq!(
        events.recv().await,
        Some(Event::Received(Packet::Publish(message.clone())))
    );
    assert_eq!(events.recv().await, Some(Event::Message(message)));
    drop(client);
}

#[tokio::test(start_paused = true)]
async fn a_server_disconnect_ends_the_connection_and_fails_what_waits() {
    let (client, mut events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    let publisher = client.clone();
    let published = tokio::spawn(async move { publisher.publish(qos1("t", "x")).await });
    let _ = server.recv().await;
    let takeover = Disconnect {
        reason_code: DisconnectReasonCode::SessionTakenOver,
        ..Disconnect::default()
    };
    server.send(takeover.clone()).await;
    assert_eq!(
        events.recv().await,
        Some(Event::Closed(CloseReason::ByServer(takeover)))
    );
    assert_eq!(events.recv().await, None);
    assert!(matches!(published.await.unwrap(), Err(Error::Closed)));
    // The message stays in the session, to be sent again if it resumes.
    assert_eq!(client.disconnect().await.unwrap().unacknowledged(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_malformed_packet_gets_disconnect_0x81() {
    let (_client, mut events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    // Packet type 0 is reserved (Table 2-1).
    server.send_bytes(&[0x00, 0x00]).await;
    assert_eq!(
        server.recv().await,
        Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::MalformedPacket,
            ..Disconnect::default()
        })
    );
    assert!(matches!(
        events.recv().await,
        Some(Event::Closed(CloseReason::ProtocolError {
            reason_code: DisconnectReasonCode::MalformedPacket,
            ..
        }))
    ));
}

#[tokio::test(start_paused = true)]
async fn a_second_connack_is_a_protocol_error() {
    let (_client, mut events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    server.send(ConnAck::default()).await;
    assert_eq!(
        server.recv().await,
        Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::ProtocolError,
            ..Disconnect::default()
        })
    );
    assert!(matches!(
        events.recv().await,
        Some(Event::Closed(CloseReason::ProtocolError { .. }))
    ));
}

#[tokio::test(start_paused = true)]
async fn disconnect_sends_0x00_and_returns_the_session() {
    let (client, mut events, mut server) = connect(ConnectOptions::new("dev").keep_alive(0)).await;
    let session = client.disconnect().await.unwrap();
    assert_eq!(session.client_id(), "dev");
    assert_eq!(
        server.recv().await,
        Packet::Disconnect(Disconnect::default())
    );
    assert_eq!(
        events.recv().await,
        Some(Event::Closed(CloseReason::Disconnected))
    );
    assert!(matches!(client.disconnect().await, Err(Error::Closed)));
    assert!(matches!(
        client.publish(qos1("t", "x")).await,
        Err(Error::Closed)
    ));
}

#[tokio::test(start_paused = true)]
async fn an_assigned_client_identifier_names_the_session() {
    let connack = ConnAck {
        properties: ConnAckProperties {
            assigned_client_identifier: Some("oqAssigned".into()),
            ..ConnAckProperties::default()
        },
        ..ConnAck::default()
    };
    let (client, _events, _server, connect) =
        connect_with(ConnectOptions::new("").keep_alive(0), connack).await;
    assert_eq!(connect.client_id, "");
    assert_eq!(client.client_id(), "oqAssigned");
    assert_eq!(client.disconnect().await.unwrap().client_id(), "oqAssigned");
}

#[tokio::test(start_paused = true)]
async fn the_server_may_not_exceed_the_clients_receive_maximum_for_qos_2() {
    let (_client, mut events, mut server) = connect(
        ConnectOptions::new("c")
            .keep_alive(0)
            .receive_maximum(NonZeroU16::new(1).unwrap()),
    )
    .await;
    for id in [1, 2] {
        server
            .send(Publish {
                qos: QoS::ExactlyOnce,
                packet_id: Some(pid(id)),
                ..qos1("t", "x")
            })
            .await;
    }
    assert_eq!(server.recv().await, Packet::PubRec(PubRec::new(pid(1))));
    assert_eq!(
        server.recv().await,
        Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::ReceiveMaximumExceeded,
            ..Disconnect::default()
        })
    );
    assert!(events.next_message().await.is_some());
    assert!(matches!(
        events.recv().await,
        Some(Event::Closed(CloseReason::ProtocolError {
            reason_code: DisconnectReasonCode::ReceiveMaximumExceeded,
            ..
        }))
    ));
}

#[tokio::test(start_paused = true)]
async fn events_left_unread_stop_reading_until_the_application_catches_up() {
    let (client_end, server_end) = tokio::io::duplex(64 * 1024);
    let (_client, mut events, mut server, _) = connect_over(
        client_end,
        server_end,
        ConnectOptions::new("c").keep_alive(0).event_capacity(1),
        ConnAck::default(),
    )
    .await;
    for id in 1..=3 {
        server
            .send(Publish {
                packet_id: Some(pid(id)),
                ..qos1("t", "x")
            })
            .await;
    }
    // One event fits the channel and one waits in the client, which then stops reading: the
    // third PUBLISH is not acknowledged yet.
    assert_eq!(server.recv().await, Packet::PubAck(PubAck::new(pid(1))));
    assert_eq!(server.recv().await, Packet::PubAck(PubAck::new(pid(2))));
    assert!(server.silent_for(Duration::from_secs(1)).await);
    for _ in 0..3 {
        assert!(events.next_message().await.is_some());
    }
    assert_eq!(server.recv().await, Packet::PubAck(PubAck::new(pid(3))));
}

#[tokio::test(start_paused = true)]
async fn a_disconnect_that_cannot_be_encoded_is_an_error_and_the_connection_stays() {
    let (client, _events, mut server) = connect(ConnectOptions::new("c").keep_alive(0)).await;
    // A string field of 70,000 bytes has no encoding.
    let unencodable = Disconnect {
        properties: openqtt_codec::DisconnectProperties {
            server_reference: Some("x".repeat(70_000)),
            ..openqtt_codec::DisconnectProperties::default()
        },
        ..Disconnect::default()
    };
    assert!(matches!(
        client.disconnect_with(unencodable).await,
        Err(Error::Invalid(_))
    ));
    // Nothing was sent, and the connection still works.
    assert!(!client.is_closed());
    assert!(server.silent_for(Duration::from_secs(1)).await);
    client
        .publish(Publish {
            topic: "t".into(),
            ..Publish::default()
        })
        .await
        .unwrap();
    assert!(matches!(server.recv().await, Packet::Publish(_)));
    client.disconnect().await.unwrap();
    assert_eq!(
        server.recv().await,
        Packet::Disconnect(Disconnect::default())
    );
}

#[tokio::test(start_paused = true)]
async fn a_disconnect_reason_string_that_would_not_fit_the_server_is_left_out() {
    let connack = ConnAck {
        properties: ConnAckProperties {
            maximum_packet_size: NonZeroU32::new(16),
            ..ConnAckProperties::default()
        },
        ..ConnAck::default()
    };
    let (client, _events, mut server, _) =
        connect_with(ConnectOptions::new("c").keep_alive(0), connack).await;
    let with_will = Disconnect {
        reason_code: DisconnectReasonCode::DisconnectWithWillMessage,
        properties: openqtt_codec::DisconnectProperties {
            reason_string: Some("going away for maintenance".into()),
            ..openqtt_codec::DisconnectProperties::default()
        },
    };
    client.disconnect_with(with_will).await.unwrap();
    assert_eq!(
        server.recv().await,
        Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::DisconnectWithWillMessage,
            ..Disconnect::default()
        })
    );
}
