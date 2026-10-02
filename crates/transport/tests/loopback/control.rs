//! The control stream in single-stream mode (docs/spec/mqtt-over-quic.md, sections 2.1 and 2.2),
//! the packets it may carry, closing (section 7), connection IDs and endpoints.

use std::num::{NonZeroU8, NonZeroU32};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use openqtt_client::{Client, ConnectOptions, QuicTransport, TlsConfig};
use openqtt_core::NodeId;
use openqtt_testkit::codec::{
    ConnAck, Disconnect, DisconnectReasonCode, Packet, PacketType, ProtocolRefusal,
};
use openqtt_testkit::{Close, Recorded, TestPki, packets};
use openqtt_transport::{
    CID_LEN, CidRoute, CloseCode, Closed, Error, Event, Listener, MqttConnection, QuicConnection,
    StreamEnd, StreamTag, Violation,
};

use crate::{QUIET, Relay, WAIT, bind, config, connected, next, quiet, take_connect, target};

#[tokio::test]
async fn the_control_stream_must_begin_with_connect() {
    let pki = TestPki::new("Control CA").unwrap();
    let endpoint = bind(config(&pki));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    client.send(Packet::PingReq).await.unwrap();
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::HandshakeComplete { early_data: false }
    );
    let error = next(&mut server).await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Violation {
                stream: StreamTag::Control,
                violation: Violation::FirstPacketNotConnect {
                    packet_type: PacketType::PingReq
                },
            }
        ),
        "{error}"
    );
    // Report R1 closes without a reply ([MQTT-3.1.0-1]); nothing more is read meanwhile.
    client.send(packets::connect("device-1")).await.unwrap();
    assert!(quiet(&mut server).await);
    server.close(CloseCode::ProtocolError);
    assert!(matches!(
        client.closed(WAIT).await,
        Some(Close::Application { code: 1, .. })
    ));
}

#[tokio::test]
async fn an_older_client_gets_the_refusal_of_its_own_version() {
    let pki = TestPki::new("Control CA").unwrap();
    let endpoint = bind(config(&pki));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    // An MQTT 3.1.1 CONNECT: protocol level 4, Clean Session, Keep Alive 60, no client id.
    client
        .send_bytes(&[
            0x10, 0x0C, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x00,
        ])
        .await
        .unwrap();
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::HandshakeComplete { early_data: false }
    );
    let error = next(&mut server).await.unwrap_err();
    let Error::Decode {
        stream: StreamTag::Control,
        error: openqtt_testkit::codec::Error::UnsupportedProtocol { name, level },
    } = error
    else {
        panic!("an unsupported protocol, not {error}");
    };
    // What the edge does with it (report R1, D1): the bytes the client's version reads, then a
    // close once they are delivered.
    let refusal = ProtocolRefusal::for_connect(&name, level);
    server
        .send_bytes(StreamTag::Control, refusal.bytes())
        .unwrap();
    server
        .shutdown(CloseCode::NoError, Duration::from_secs(1))
        .await;
    let Some(Recorded::Malformed { bytes, .. }) = client.recv(WAIT).await.map(|r| r.event) else {
        panic!("the 3.1.1 CONNACK, which no MQTT 5 decoder reads");
    };
    assert_eq!(bytes.as_ref(), [0x20, 0x02, 0x00, 0x01]);
    assert_eq!(
        client.recv(WAIT).await.map(|r| r.event),
        Some(Recorded::StreamFinished)
    );
    assert!(matches!(
        client.closed(WAIT).await,
        Some(Close::Application { code: 0, .. })
    ));
}

#[tokio::test]
async fn a_packet_over_the_maximum_size_is_refused_from_its_fixed_header() {
    let pki = TestPki::new("Control CA").unwrap();
    let endpoint = bind(config(&pki).max_packet_size(NonZeroU32::new(1_024).unwrap()));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    client.send(packets::connect("device-2")).await.unwrap();
    take_connect(&mut server).await;
    // A PUBLISH whose Remaining Length says 2,048 bytes, of which none follow.
    client.send_bytes(&[0x30, 0x80, 0x10]).await.unwrap();
    let error = next(&mut server).await.unwrap_err();
    assert!(
        matches!(
            error,
            Error::Decode {
                stream: StreamTag::Control,
                error: openqtt_testkit::codec::Error::PacketTooLarge {
                    size: 2_051,
                    maximum: 1_024
                },
            }
        ),
        "{error}"
    );
}

#[tokio::test]
async fn a_disconnect_and_a_close_with_code_0_end_the_connection_cleanly() {
    let pki = TestPki::new("Control CA").unwrap();
    let endpoint = bind(config(&pki));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    client.send(packets::connect("device-3")).await.unwrap();
    take_connect(&mut server).await;
    server
        .send(StreamTag::Control, &Packet::from(ConnAck::default()))
        .unwrap();
    server.flush().await.unwrap();
    assert!(matches!(
        client.recv_packet(WAIT).await,
        Some(Packet::ConnAck(_))
    ));
    // The client says goodbye, and closes once that has arrived: closing at once would lose it
    // on the client's side. The server reads nothing meanwhile, as a busy session would have it,
    // and still hears the DISCONNECT before the close, so no Will Message goes out.
    server.pause_reading();
    client.send(Disconnect::default()).await.unwrap();
    client.finish();
    tokio::time::sleep(QUIET).await;
    client.close(0);
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
            end: StreamEnd::Finished,
        }
    );
    let error = next(&mut server).await.unwrap_err();
    assert!(
        matches!(error, Error::Closed(Closed::Application { code: 0, .. })),
        "{error}"
    );
    // Closed stays closed.
    assert!(matches!(next(&mut server).await, Err(Error::Closed(_))));
    assert!(matches!(
        server.send(StreamTag::Control, &Packet::PingResp),
        Err(Error::Closed(_))
    ));
}

#[tokio::test]
async fn an_abrupt_close_is_told_apart_from_a_clean_one() {
    let pki = TestPki::new("Control CA").unwrap();
    let endpoint = bind(config(&pki));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    client.send(packets::connect("device-4")).await.unwrap();
    take_connect(&mut server).await;
    client.close(2);
    let error = next(&mut server).await.unwrap_err();
    assert!(
        matches!(error, Error::Closed(Closed::Application { code: 2, .. })),
        "{error}"
    );
}

#[tokio::test]
async fn the_server_closes_after_its_disconnect_is_delivered() {
    let pki = TestPki::new("Control CA").unwrap();
    let endpoint = bind(config(&pki));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    client.send(packets::connect("device-5")).await.unwrap();
    take_connect(&mut server).await;
    server
        .send(StreamTag::Control, &Packet::from(ConnAck::default()))
        .unwrap();
    let disconnect = Disconnect {
        reason_code: DisconnectReasonCode::UseAnotherServer,
        ..Disconnect::default()
    };
    server
        .send(StreamTag::Control, &Packet::from(disconnect.clone()))
        .unwrap();
    // The close follows the client's acknowledgement, long before the linger runs out.
    let started = std::time::Instant::now();
    server
        .shutdown(CloseCode::NoError, Duration::from_secs(5))
        .await;
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(matches!(
        client.recv_packet(WAIT).await,
        Some(Packet::ConnAck(_))
    ));
    assert_eq!(
        client.recv_packet(WAIT).await,
        Some(Packet::from(disconnect))
    );
    assert_eq!(
        client.recv(WAIT).await.map(|r| r.event),
        Some(Recorded::StreamFinished)
    );
    assert!(matches!(
        client.closed(WAIT).await,
        Some(Close::Application { code: 0, .. })
    ));
}

#[tokio::test]
async fn openqtt_client_connects_pings_and_disconnects() {
    let pki = TestPki::new("Control CA").unwrap();
    let endpoint = bind(config(&pki));
    let events = Arc::new(Mutex::new(Vec::new()));
    let server = tokio::spawn({
        let endpoint = endpoint.clone();
        let events = Arc::clone(&events);
        async move {
            let accepting = endpoint.accept().await.expect("an attempt");
            let connection = accepting.establish().await.expect("the handshake");
            serve(connection, &events).await;
        }
    });
    let tls = TlsConfig::builder()
        .root_certificate(pki.ca_certificate())
        .build()
        .unwrap();
    let transport = QuicTransport::new(endpoint.local_address(), "localhost", &tls).unwrap();
    let options = ConnectOptions::new("device-6")
        .keep_alive(1)
        .packet_log(true);
    let (client, mut client_events) = Client::connect(&transport, options).await.unwrap();
    for _ in 0..2 {
        loop {
            let event = tokio::time::timeout(WAIT, client_events.recv())
                .await
                .unwrap();
            if event == Some(openqtt_client::Event::Received(Packet::PingResp)) {
                break;
            }
        }
    }
    client.disconnect().await.unwrap();
    tokio::time::timeout(WAIT, server).await.unwrap().unwrap();
    let events = events.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(
        events.first(),
        Some(&Ok(Event::HandshakeComplete { early_data: false }))
    );
    let pings = events
        .iter()
        .filter(|event| {
            matches!(
                event,
                Ok(Event::Packet {
                    packet: Packet::PingReq,
                    ..
                })
            )
        })
        .count();
    assert!(pings >= 2, "{events:?}");
    let ends: Vec<_> = events.iter().rev().take(3).rev().collect();
    assert!(
        matches!(
            ends[..],
            [
                Ok(Event::Packet {
                    packet: Packet::Disconnect(_),
                    ..
                }),
                Ok(Event::StreamEnded {
                    stream: StreamTag::Control,
                    end: StreamEnd::Finished,
                }),
                Err(Closed::Application { code: 0, .. }),
            ]
        ),
        "{events:?}"
    );
}

/// The least of a broker: CONNACK for CONNECT and PINGRESP for PINGREQ, recording every event
/// until the connection closes.
async fn serve(mut connection: QuicConnection, events: &Mutex<Vec<Result<Event, Closed>>>) {
    loop {
        let event = connection.recv().await;
        let record = match &event {
            Ok(event) => Ok(event.clone()),
            Err(Error::Closed(closed)) => Err(closed.clone()),
            Err(other) => panic!("the client broke no rule, but {other}"),
        };
        events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(record);
        match event {
            Ok(Event::Packet {
                stream,
                packet: Packet::Connect(_),
            }) => connection
                .send(stream, &Packet::from(ConnAck::default()))
                .expect("the CONNACK is queued"),
            Ok(Event::Packet {
                stream,
                packet: Packet::PingReq,
            }) => connection
                .send(stream, &Packet::PingResp)
                .expect("the PINGRESP is queued"),
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

#[tokio::test]
async fn connection_ids_name_the_node_and_the_endpoint() {
    let pki = TestPki::new("Control CA").unwrap();
    let node = NodeId::new(0x0123_4567_89AB_CDEF);
    let endpoint = bind(config(&pki).node(node));
    let relay = Relay::start(endpoint.local_address(), usize::MAX).await;
    let target =
        openqtt_testkit::Target::new(relay.address, "localhost", vec![pki.ca_certificate()]);
    let (mut client, mut server) = connected(&endpoint, &target).await;
    client.send(packets::connect("device-7")).await.unwrap();
    take_connect(&mut server).await;
    server.send(StreamTag::Control, &Packet::PingResp).unwrap();
    server.flush().await.unwrap();
    assert_eq!(client.recv_packet(WAIT).await, Some(Packet::PingResp));
    let ids = relay.short_header_ids();
    assert!(!ids.is_empty());
    for id in ids {
        assert_eq!(id.len(), CID_LEN);
        assert_eq!(
            CidRoute::of(&id),
            Some(route(node, 0)),
            "{id:02x?} does not name the endpoint"
        );
    }
}

/// The route of endpoint `endpoint` on `node`, as a connection ID names it.
fn route(node: NodeId, endpoint: u8) -> CidRoute {
    let mut cid = [0; CID_LEN];
    cid[0] = 0x11;
    cid[1..9].copy_from_slice(&node.get().to_be_bytes());
    cid[9] = endpoint;
    CidRoute::of(&cid).expect("a route")
}

#[cfg(unix)]
#[tokio::test]
async fn several_endpoints_share_one_port() {
    let pki = TestPki::new("Control CA").unwrap();
    let listener = Listener::new(config(&pki).endpoints(NonZeroU8::new(3).unwrap())).unwrap();
    assert_eq!(listener.endpoints(), 3);
    let endpoints = listener.bind_all().unwrap();
    assert_eq!(endpoints.len(), 3);
    let address = endpoints[0].local_address();
    assert_ne!(address.port(), 0);
    for (index, endpoint) in endpoints.iter().enumerate() {
        assert_eq!(endpoint.local_address(), address);
        assert_eq!(usize::from(endpoint.index()), index);
    }
    assert!(listener.bind(3).is_err());
    // Linux hands the client to one of them by a hash of its address; macOS to the last bound.
    let target = openqtt_testkit::Target::new(address, "localhost", vec![pki.ca_certificate()]);
    let (client, server) = tokio::join!(openqtt_testkit::RawConnection::connect(&target), async {
        let accepting = tokio::select! {
            accepting = endpoints[0].accept() => accepting,
            accepting = endpoints[1].accept() => accepting,
            accepting = endpoints[2].accept() => accepting,
        };
        accepting.expect("an attempt").establish().await
    });
    let mut client = client.unwrap();
    let mut server: QuicConnection = server.unwrap();
    client.send(packets::connect("device-8")).await.unwrap();
    take_connect(&mut server).await;
}

#[tokio::test]
async fn an_endpoint_that_stops_accepting_keeps_its_connections() {
    let pki = TestPki::new("Control CA").unwrap();
    let endpoint = bind(config(&pki));
    let target = target(&endpoint, &pki);
    let (mut client, mut server) = connected(&endpoint, &target).await;
    endpoint.stop_accepting();
    // A new client is refused before its handshake, while the edge goes on accepting.
    let accepting = tokio::spawn({
        let endpoint = endpoint.clone();
        async move { endpoint.accept().await.is_some() }
    });
    let refused = openqtt_testkit::RawConnection::connect(&target).await;
    assert!(
        matches!(
            refused,
            Err(openqtt_testkit::Error::Connection(
                quinn::ConnectionError::ConnectionClosed(ref close)
            )) if u64::from(close.error_code) == 0x2
        ),
        "{refused:?}"
    );
    // The one already connected carries on.
    client.send(packets::connect("device-9")).await.unwrap();
    take_connect(&mut server).await;
    assert_eq!(endpoint.open_connections(), 1);
    endpoint.close(CloseCode::NoError);
    assert!(matches!(
        client.closed(WAIT).await,
        Some(Close::Application { code: 0, .. })
    ));
    tokio::time::timeout(WAIT, endpoint.wait_idle())
        .await
        .unwrap();
    // Closing the endpoint ends the accept loop.
    assert!(
        !tokio::time::timeout(WAIT, accepting)
            .await
            .unwrap()
            .unwrap()
    );
}

#[tokio::test]
async fn a_connection_let_go_without_a_close_ends_with_an_internal_error() {
    let pki = TestPki::new("Control CA").unwrap();
    let endpoint = bind(config(&pki));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    client.send(packets::connect("device-10")).await.unwrap();
    take_connect(&mut server).await;
    // As when the edge's task panics or is aborted: the client must not read a clean end.
    drop(server);
    assert!(matches!(
        client.closed(WAIT).await,
        Some(Close::Application { code: 2, .. })
    ));
}

#[test]
fn settings_that_cannot_work_are_refused() {
    let pki = TestPki::new("Control CA").unwrap();
    for config in [
        config(&pki).send_backlog(0),
        config(&pki).handshake_timeout(Duration::ZERO),
        config(&pki).handshake_timeout(Duration::MAX),
    ] {
        let error = Listener::new(config).unwrap_err();
        assert!(matches!(error, Error::Setting { .. }), "{error}");
    }
}
