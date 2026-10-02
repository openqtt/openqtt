//! Multi-stream mode (docs/spec/mqtt-over-quic.md, sections 2.3 and 2.4): data streams held
//! until the connection is accepted and refused with it, what they may carry, how their ends are
//! reported, and the limits on how many a client opens.

use std::num::NonZeroU16;

use openqtt_testkit::codec::{
    ConnAck, ConnectReasonCode, Disconnect, Packet, PacketType, PublishProperties, QoS,
};
use openqtt_testkit::{Close, RawConnection, Recorded, TestPki, packets};
use openqtt_transport::{
    CloseCode, Error, Event, MqttConnection, QuicConnection, StreamEnd, StreamTag, Violation,
};

use crate::{
    QUIET, WAIT, bind, config, connected, encode, next, quiet, quinn_client, take_connect, target,
};

/// A client connected and accepted: CONNECT, CONNACK, and data streams accepted.
async fn accepted(pki: &TestPki) -> (RawConnection, QuicConnection, openqtt_transport::Endpoint) {
    let endpoint = bind(config(pki));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, pki)).await;
    client
        .send(packets::connect("device"))
        .await
        .expect("the CONNECT goes out");
    take_connect(&mut server).await;
    server
        .send(StreamTag::Control, &Packet::from(ConnAck::default()))
        .expect("the CONNACK is queued");
    server.accept_data_streams();
    server.flush().await.expect("the CONNACK goes out");
    assert!(matches!(
        client.recv_packet(WAIT).await,
        Some(Packet::ConnAck(_))
    ));
    (client, server, endpoint)
}

#[tokio::test]
async fn data_streams_are_held_until_the_connection_is_accepted() {
    let pki = TestPki::new("Streams CA").unwrap();
    let endpoint = bind(config(&pki));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    client.send(packets::connect("device-1")).await.unwrap();
    // Opened before CONNACK, which a client may do; the server reads it only once it accepts.
    let mut stream = client.open_stream().await.unwrap();
    assert_eq!(stream.index(), 1);
    stream
        .send(packets::publish("t/1", QoS::AtLeastOnce, 1, "early"))
        .await
        .unwrap();
    take_connect(&mut server).await;
    assert!(quiet(&mut server).await);

    server
        .send(StreamTag::Control, &Packet::from(ConnAck::default()))
        .unwrap();
    server.accept_data_streams();
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Data(1),
            packet: Packet::from(packets::publish("t/1", QoS::AtLeastOnce, 1, "early")),
        }
    );
    // The acknowledgement travels on the stream that carried the PUBLISH.
    server
        .send(StreamTag::Data(1), &Packet::from(packets::puback(1)))
        .unwrap();
    server.flush().await.unwrap();
    assert_eq!(
        stream.recv_packet(WAIT).await,
        Some(Packet::from(packets::puback(1)))
    );
    // Data streams opened later are read at once.
    let mut later = client.open_stream().await.unwrap();
    later
        .send(packets::subscribe(2, "c/#", QoS::AtMostOnce))
        .await
        .unwrap();
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Data(2),
            packet: Packet::from(packets::subscribe(2, "c/#", QoS::AtMostOnce)),
        }
    );
}

#[tokio::test]
async fn data_streams_of_a_refused_connection_are_refused_with_code_3() {
    let pki = TestPki::new("Streams CA").unwrap();
    let endpoint = bind(config(&pki));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    client.send(packets::connect("device-2")).await.unwrap();
    let mut stream = client.open_stream().await.unwrap();
    stream
        .send(packets::publish("t/2", QoS::AtMostOnce, 0, "never read"))
        .await
        .unwrap();
    take_connect(&mut server).await;
    // The session refuses the connection.
    let refusal = ConnAck {
        reason_code: ConnectReasonCode::NotAuthorized,
        ..ConnAck::default()
    };
    server
        .send(StreamTag::Control, &Packet::from(refusal.clone()))
        .unwrap();
    server
        .shutdown(CloseCode::NoError, std::time::Duration::from_secs(1))
        .await;
    assert_eq!(client.recv_packet(WAIT).await, Some(Packet::from(refusal)));
    // Stream refused, both ways (section 8).
    assert_eq!(stream.recv(WAIT).await, Some(Recorded::StreamReset(3)));
    assert_eq!(stream.stopped(WAIT).await, Some(3));
    assert!(matches!(
        client.closed(WAIT).await,
        Some(Close::Application { code: 0, .. })
    ));
}

#[tokio::test]
async fn a_control_packet_on_a_data_stream_is_a_protocol_error() {
    let pki = TestPki::new("Streams CA").unwrap();
    for (packet, packet_type) in [
        (Packet::PingReq, PacketType::PingReq),
        (Packet::from(packets::connect("again")), PacketType::Connect),
        (
            Packet::from(openqtt_testkit::codec::Disconnect::default()),
            PacketType::Disconnect,
        ),
    ] {
        let (client, mut server, _endpoint) = accepted(&pki).await;
        let mut stream = client.open_stream().await.unwrap();
        stream.send(packet).await.unwrap();
        let error = next(&mut server).await.unwrap_err();
        assert!(
            matches!(
                error,
                Error::Violation {
                    stream: StreamTag::Data(1),
                    violation: Violation::ControlPacketOnDataStream { packet_type: found },
                } if found == packet_type
            ),
            "{error}"
        );
        // The session answers with DISCONNECT 0x82 on the control stream.
        assert_eq!(
            Violation::ControlPacketOnDataStream { packet_type }.disconnect_reason_code(),
            Some(openqtt_testkit::codec::DisconnectReasonCode::ProtocolError)
        );
    }
}

#[tokio::test]
async fn a_topic_alias_on_a_data_stream_is_a_protocol_error() {
    let pki = TestPki::new("Streams CA").unwrap();
    let (client, mut server, _endpoint) = accepted(&pki).await;
    let mut stream = client.open_stream().await.unwrap();
    let mut publish = packets::publish("t/3", QoS::AtMostOnce, 0, "aliased");
    publish.properties = PublishProperties {
        topic_alias: NonZeroU16::new(1),
        ..PublishProperties::default()
    };
    stream.send(publish).await.unwrap();
    let error = next(&mut server).await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Violation {
                stream: StreamTag::Data(1),
                violation: Violation::TopicAliasOnDataStream,
            }
        ),
        "{error}"
    );
}

#[tokio::test]
async fn the_server_sends_on_a_data_stream_only_what_it_may_carry() {
    let pki = TestPki::new("Streams CA").unwrap();
    let (client, mut server, _endpoint) = accepted(&pki).await;
    let mut stream = client.open_stream().await.unwrap();
    stream
        .send(packets::subscribe(1, "c/#", QoS::AtMostOnce))
        .await
        .unwrap();
    assert!(matches!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Data(1),
            ..
        }
    ));
    for packet in [
        Packet::PingResp,
        Packet::from(ConnAck::default()),
        Packet::from(openqtt_testkit::codec::Disconnect::default()),
    ] {
        let error = server.send(StreamTag::Data(1), &packet).unwrap_err();
        assert!(matches!(error, Error::WrongStream { .. }), "{error}");
    }
    let mut aliased = packets::publish("c/1", QoS::AtMostOnce, 0, "x");
    aliased.properties.topic_alias = NonZeroU16::new(1);
    assert!(matches!(
        server.send(StreamTag::Data(1), &Packet::from(aliased)),
        Err(Error::WrongStream { .. })
    ));
    // A stream the client has not opened.
    assert!(matches!(
        server.send(StreamTag::Data(9), &Packet::from(packets::puback(1))),
        Err(Error::StreamClosed {
            stream: StreamTag::Data(9)
        })
    ));
    // A delivery on the stream of its subscription.
    let delivery = packets::publish("c/1", QoS::AtMostOnce, 0, "to you");
    server
        .send(StreamTag::Data(1), &Packet::from(delivery.clone()))
        .unwrap();
    server.flush().await.unwrap();
    assert_eq!(stream.recv_packet(WAIT).await, Some(Packet::from(delivery)));
}

#[tokio::test]
async fn the_end_of_each_side_of_a_stream_is_reported() {
    let pki = TestPki::new("Streams CA").unwrap();
    let (client, mut server, _endpoint) = accepted(&pki).await;

    // Finished: the client is done with the stream, and the server finishes its side in turn
    // (section 2.4).
    let mut finished = client.open_stream().await.unwrap();
    finished
        .send(packets::publish("t/4", QoS::AtMostOnce, 0, "last"))
        .await
        .unwrap();
    finished.finish();
    assert!(matches!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Data(1),
            ..
        }
    ));
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::StreamEnded {
            stream: StreamTag::Data(1),
            end: StreamEnd::Finished,
        }
    );
    server.finish(StreamTag::Data(1)).unwrap();
    assert!(matches!(
        server.send(StreamTag::Data(1), &Packet::from(packets::puback(1))),
        Err(Error::StreamClosed { .. })
    ));
    server.flush().await.unwrap();
    assert_eq!(finished.recv(WAIT).await, Some(Recorded::StreamFinished));

    // Reset: whatever the client was still sending is gone.
    let mut reset = client.open_stream().await.unwrap();
    reset
        .send(packets::publish(
            "t/5",
            QoS::AtMostOnce,
            0,
            "before the reset",
        ))
        .await
        .unwrap();
    assert!(matches!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Data(2),
            ..
        }
    ));
    reset.reset(7);
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::StreamEnded {
            stream: StreamTag::Data(2),
            end: StreamEnd::Reset(7),
        }
    );

    // Stopped: nothing the server sends on the stream reaches the client any more.
    let mut stopped = client.open_stream().await.unwrap();
    stopped
        .send(packets::subscribe(3, "c/#", QoS::AtMostOnce))
        .await
        .unwrap();
    assert!(matches!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Data(3),
            ..
        }
    ));
    stopped.stop(9);
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::StreamEnded {
            stream: StreamTag::Data(3),
            end: StreamEnd::Stopped(9),
        }
    );
    assert!(matches!(
        server.send(StreamTag::Data(3), &Packet::from(packets::puback(3))),
        Err(Error::StreamClosed { .. })
    ));
    // The client may still send on its own side until it ends it.
    stopped
        .send(packets::publish("t/6", QoS::AtMostOnce, 0, "still"))
        .await
        .unwrap();
    assert!(matches!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Data(3),
            ..
        }
    ));

    // The control stream is untouched by all of it.
    server.send(StreamTag::Control, &Packet::PingResp).unwrap();
    server.flush().await.unwrap();
    let mut client = client;
    assert_eq!(client.recv_packet(WAIT).await, Some(Packet::PingResp));
}

#[tokio::test]
async fn a_client_opens_no_more_streams_than_the_listener_allows() {
    let pki = TestPki::new("Streams CA").unwrap();
    let endpoint = bind(config(&pki).max_streams(2));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    client.send(packets::connect("device-3")).await.unwrap();
    take_connect(&mut server).await;
    server
        .send(StreamTag::Control, &Packet::from(ConnAck::default()))
        .unwrap();
    server.accept_data_streams();
    server.flush().await.unwrap();

    // The control stream and one data stream: a third bidirectional stream waits for room.
    let mut first = client.open_stream().await.unwrap();
    assert!(
        tokio::time::timeout(QUIET, client.open_stream())
            .await
            .is_err()
    );
    // No unidirectional stream at all (R7, D2).
    assert!(
        tokio::time::timeout(QUIET, client.quic().open_uni())
            .await
            .is_err()
    );

    // Once both sides of the data stream are done, quinn gives the client room for another.
    first
        .send(packets::publish("t/7", QoS::AtMostOnce, 0, "x"))
        .await
        .unwrap();
    first.finish();
    assert!(matches!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Data(1),
            ..
        }
    ));
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::StreamEnded {
            stream: StreamTag::Data(1),
            end: StreamEnd::Finished,
        }
    );
    server.finish(StreamTag::Data(1)).unwrap();
    server.flush().await.unwrap();
    assert_eq!(first.recv(WAIT).await, Some(Recorded::StreamFinished));
    let mut second = tokio::time::timeout(WAIT, async {
        loop {
            // Drive the server, which lets go of the stream once the client acknowledged its end.
            tokio::select! {
                opened = client.open_stream() => return opened.unwrap(),
                _ = tokio::time::sleep(QUIET) => {
                    let _ = tokio::time::timeout(QUIET, server.recv()).await;
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(second.index(), 2);
    second
        .send(packets::publish("t/8", QoS::AtMostOnce, 0, "y"))
        .await
        .unwrap();
    assert!(matches!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Data(2),
            ..
        }
    ));
}

#[tokio::test]
async fn a_disconnect_comes_before_the_stop_that_follows_it() {
    let pki = TestPki::new("Streams CA").unwrap();
    let endpoint = bind(config(&pki));
    let (_client, _connection, mut send, mut recv, mut server) =
        quinn_client(&endpoint, &target(&endpoint, &pki)).await;
    // The client says goodbye and stops the server's side of the control stream, and both
    // arrive before the server reads either. Taken in the other order, the stop would look like
    // an abnormal end, and the session would publish the Will Message.
    send.write_all(&encode(Disconnect::default()))
        .await
        .unwrap();
    recv.stop(quinn::VarInt::from_u32(5)).unwrap();
    tokio::time::sleep(QUIET).await;
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Control,
            packet: Packet::from(Disconnect::default()),
        }
    );
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::StreamEnded {
            stream: StreamTag::Control,
            end: StreamEnd::Stopped(5),
        }
    );
}

#[tokio::test]
async fn a_publish_comes_before_the_stop_of_its_data_stream() {
    let pki = TestPki::new("Streams CA").unwrap();
    let (client, mut server, _endpoint) = accepted(&pki).await;
    let mut stream = client.open_stream().await.unwrap();
    // The session must hear of the PUBLISH to know it owes a PUBACK it can no longer send
    // (section 2.4).
    stream
        .send(packets::publish("t/9", QoS::AtLeastOnce, 1, "owed"))
        .await
        .unwrap();
    stream.stop(6);
    tokio::time::sleep(QUIET).await;
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Data(1),
            packet: Packet::from(packets::publish("t/9", QoS::AtLeastOnce, 1, "owed")),
        }
    );
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::StreamEnded {
            stream: StreamTag::Data(1),
            end: StreamEnd::Stopped(6),
        }
    );
}
