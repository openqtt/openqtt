//! Backpressure: the session's PauseReading and ResumeReading, and a send backlog that, once
//! full, stops the connection reading until the client takes what it was sent.

use std::sync::Arc;
use std::time::Duration;

use openqtt_testkit::codec::{ConnAck, Packet, QoS};
use openqtt_testkit::{RawConnection, Recorded, Target, TestPki, packets};
use openqtt_transport::{Event, ListenerConfig, MqttConnection, QuicConnection, StreamTag};

use crate::{WAIT, bind, config, connected, next, quiet, take_connect, target};

/// A client connected to a listener as `config` sets it, accepted with data streams.
async fn accepted(
    config: ListenerConfig,
    target: impl FnOnce(&openqtt_transport::Endpoint) -> Target,
) -> (RawConnection, QuicConnection, openqtt_transport::Endpoint) {
    let endpoint = bind(config);
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint)).await;
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
async fn reading_paused_by_the_session_waits_for_it_to_resume() {
    let pki = TestPki::new("Backpressure CA").unwrap();
    let (mut client, mut server, _endpoint) =
        accepted(config(&pki), |endpoint| target(endpoint, &pki)).await;
    server.pause_reading();
    for id in 1..=3 {
        client
            .send(packets::publish("t/1", QoS::AtLeastOnce, id, "held"))
            .await
            .unwrap();
    }
    assert!(quiet(&mut server).await);
    server.resume_reading();
    for id in 1..=3 {
        assert_eq!(
            next(&mut server).await.unwrap(),
            Event::Packet {
                stream: StreamTag::Control,
                packet: Packet::from(packets::publish("t/1", QoS::AtLeastOnce, id, "held")),
            }
        );
    }
}

#[tokio::test]
async fn a_client_that_does_not_read_holds_the_server_to_its_backlog() {
    let pki = TestPki::new("Backpressure CA").unwrap();
    // The client takes at most 8 KiB on a stream before it reads, and the server's QUIC stack
    // holds at most 16 KiB it sent and the client has not acknowledged.
    let mut window = quinn::TransportConfig::default();
    window
        .stream_receive_window(quinn::VarInt::from_u32(8 * 1024))
        .receive_window(quinn::VarInt::from_u32(16 * 1024))
        .datagram_receive_buffer_size(None);
    let window = Arc::new(window);
    let config = config(&pki).send_window(16 * 1024).send_backlog(16 * 1024);
    let (mut client, mut server, _endpoint) = accepted(config, |endpoint| {
        target(endpoint, &pki).with_transport_config(Arc::clone(&window))
    })
    .await;
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

    // Deliveries on the data stream, which the client does not read, until the backlog is full.
    let payload = "x".repeat(1_000);
    let mut sent = 0;
    while server.is_writable() {
        server
            .send(
                StreamTag::Data(1),
                &Packet::from(packets::publish("c/1", QoS::AtMostOnce, 0, &payload)),
            )
            .unwrap();
        sent += 1;
        // Let what QUIC takes go out, so the backlog is what flow control holds back.
        let _ = tokio::time::timeout(Duration::from_millis(5), server.flush()).await;
    }
    assert!(sent > 16, "{sent} deliveries filled the backlog");

    // The connection reads nothing more meanwhile, so the client cannot make it queue more.
    client
        .send(packets::publish("t/2", QoS::AtLeastOnce, 1, "waits"))
        .await
        .unwrap();
    assert!(quiet(&mut server).await);
    assert!(!server.is_writable());

    // The client reads everything it was sent; the backlog drains, and reading resumes.
    let reader = tokio::spawn(async move {
        let mut received = 0;
        while let Some(Recorded::Received(Packet::Publish(_))) = stream.recv(WAIT).await {
            received += 1;
            if received == sent {
                break;
            }
        }
        received
    });
    assert_eq!(next(&mut server).await.unwrap(), Event::Writable);
    assert!(server.is_writable());
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            stream: StreamTag::Control,
            packet: Packet::from(packets::publish("t/2", QoS::AtLeastOnce, 1, "waits")),
        }
    );
    // The rest goes out as the client reads it.
    let flushed = tokio::time::timeout(WAIT, server.flush()).await;
    assert!(matches!(flushed, Ok(Ok(()))), "{flushed:?}");
    assert_eq!(
        tokio::time::timeout(WAIT, reader).await.unwrap().unwrap(),
        sent
    );
}
