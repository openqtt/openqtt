//! The test kit against itself over QUIC on the loopback interface: the raw client and the fake
//! server, mutual TLS and its refusals, and a scenario played on the fake broker.

use std::time::Duration;

use bytes::Bytes;
use openqtt_testkit::codec::{ConnAck, Disconnect, DisconnectReasonCode, Packet, PacketType, QoS};
use openqtt_testkit::{
    ClientAuth, Close, FakeBroker, FakeServer, Identity, RawConnection, Recorded, Runner, Scenario,
    Target, TestPki, packets,
};

const WAIT: Duration = Duration::from_secs(5);

/// A CA, a server certificate for localhost, and a target that trusts it.
fn setup() -> (TestPki, Identity, impl Fn(&FakeServer) -> Target) {
    let pki = TestPki::new("Loopback CA").expect("a CA");
    let server = pki.server(&["localhost"]).expect("a server certificate");
    let ca = pki.ca_certificate();
    let target =
        move |server: &FakeServer| Target::new(server.addr(), "localhost", vec![ca.clone()]);
    (pki, server, target)
}

/// Connects a raw client that sends `first`, and accepts it. The two run together: quinn
/// holds a handshake until the server accepts it, and the server learns of the control stream
/// only once the client sends on it.
async fn pair(
    server: &FakeServer,
    target: &Target,
    first: impl Into<Packet>,
) -> (RawConnection, RawConnection) {
    let first = first.into();
    let (client, peer) = tokio::join!(
        async {
            let mut client = RawConnection::connect(target)
                .await
                .expect("the client connects");
            client.send(first).await.expect("the client sends");
            client
        },
        server.accept()
    );
    (client, peer.expect("the server accepts"))
}

#[tokio::test]
async fn the_raw_client_records_both_ways_with_time() {
    let (_pki, identity, target) = setup();
    let server = FakeServer::bind(&identity, &ClientAuth::None).unwrap();
    let target = target(&server);
    let (mut client, mut peer) = pair(&server, &target, packets::connect("raw-1")).await;
    assert_eq!(peer.alpn().as_deref(), Some(&b"mqtt"[..]));
    let Some(Packet::Connect(connect)) = peer.recv_packet(WAIT).await else {
        panic!("CONNECT");
    };
    assert_eq!(connect.client_id, "raw-1");
    peer.send(ConnAck::default()).await.unwrap();
    assert_eq!(
        client.recv_packet(WAIT).await,
        Some(Packet::from(ConnAck::default()))
    );

    let records = client.records();
    assert!(matches!(
        records[0].event,
        Recorded::Sent(Packet::Connect(_))
    ));
    assert!(matches!(
        records[1].event,
        Recorded::Received(Packet::ConnAck(_))
    ));
    assert!(records[0].at <= records[1].at);
    let server_records = peer.records();
    assert!(matches!(
        server_records[0].event,
        Recorded::Received(Packet::Connect(_))
    ));
    assert!(matches!(
        server_records[1].event,
        Recorded::Sent(Packet::ConnAck(_))
    ));
}

#[tokio::test]
async fn bytes_that_do_not_decode_are_recorded_and_reading_goes_on() {
    let (_pki, identity, target) = setup();
    let server = FakeServer::bind(&identity, &ClientAuth::None).unwrap();
    let target = target(&server);
    let (client, peer) = tokio::join!(
        async {
            let mut client = RawConnection::connect(&target).await.unwrap();
            client.send_bytes(&[0x10, 0x00]).await.unwrap();
            client
        },
        server.accept()
    );
    let (mut client, mut peer) = (client, peer.unwrap());
    // CONNECT with a Remaining Length of 0 does not decode either.
    let Some(Recorded::Malformed { bytes, .. }) = peer.recv(WAIT).await.map(|r| r.event) else {
        panic!("malformed");
    };
    assert_eq!(bytes.as_ref(), [0x10, 0x00]);

    // The MQTT 3.1.1 CONNACK, a reserved packet type, then a PINGRESP.
    peer.send_bytes(&[0x20, 0x02, 0x00, 0x00, 0x00, 0x00, 0xD0, 0x00])
        .await
        .unwrap();
    for expected in [&[0x20, 0x02, 0x00, 0x00][..], &[0x00, 0x00]] {
        let Some(Recorded::Malformed { bytes, error }) = client.recv(WAIT).await.map(|r| r.event)
        else {
            panic!("malformed");
        };
        assert_eq!(bytes.as_ref(), expected, "{error}");
    }
    assert_eq!(client.recv_packet(WAIT).await, Some(Packet::PingResp));
    assert!(matches!(
        client.records()[0].event,
        Recorded::SentBytes(ref bytes) if bytes == &Bytes::from_static(&[0x10, 0x00])
    ));
}

#[tokio::test]
async fn a_close_is_recorded_after_what_came_before_it() {
    let (_pki, identity, target) = setup();
    let server = FakeServer::bind(&identity, &ClientAuth::None).unwrap();
    let target = target(&server);
    let (mut client, mut peer) = pair(&server, &target, packets::connect("closing")).await;
    peer.send(Disconnect {
        reason_code: DisconnectReasonCode::SessionTakenOver,
        ..Disconnect::default()
    })
    .await
    .unwrap();
    peer.finish();
    // Give the DISCONNECT time to be acknowledged before the close discards it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    peer.close(1);
    assert!(matches!(
        client.recv_packet(WAIT).await,
        Some(Packet::Disconnect(Disconnect {
            reason_code: DisconnectReasonCode::SessionTakenOver,
            ..
        }))
    ));
    assert_eq!(
        client.closed(WAIT).await,
        Some(Close::Application {
            code: 1,
            reason: Bytes::new()
        })
    );
    let records = client.records();
    assert!(matches!(
        records.last().map(|record| &record.event),
        Some(Recorded::Closed(_))
    ));
}

#[tokio::test]
async fn mutual_tls_takes_a_certificate_from_the_ca_and_refuses_the_rest() {
    let (pki, identity, target) = setup();
    let server =
        FakeServer::bind(&identity, &ClientAuth::Required(vec![pki.ca_certificate()])).unwrap();
    let target = target(&server);

    // A device certificate from the CA: accepted, and the server sees the chain.
    let device = pki.client("device-17").unwrap();
    let with_device = target.clone().with_identity(device.clone());
    let (client, peer) = pair(&server, &with_device, packets::connect("device-17")).await;
    assert_eq!(peer.peer_certificates(), Some(device.chain.clone()));
    drop((client, peer));

    // No certificate, a look-alike issuer, another CA, and a server certificate presented by a
    // client: each handshake fails.
    let other = TestPki::new("Other CA").unwrap();
    let refused = [
        None,
        Some(pki.impostor_client("device-18").unwrap()),
        Some(other.client("device-19").unwrap()),
        Some(pki.client_with_server_eku("device-20").unwrap()),
    ];
    for identity in refused {
        let target = match identity {
            Some(identity) => target.clone().with_identity(identity),
            None => target.clone(),
        };
        let (client, accepted) = tokio::join!(
            async {
                let mut client = RawConnection::connect(&target).await?;
                client.send(packets::connect("x")).await?;
                // TLS 1.3 finishes the client's side before the server has checked its
                // certificate, so the refusal arrives as the close of the connection.
                Ok::<_, openqtt_testkit::Error>(client.closed(WAIT).await)
            },
            tokio::time::timeout(WAIT, server.accept())
        );
        let accepted = accepted.ok().and_then(Result::ok);
        assert!(
            accepted.is_none(),
            "the server accepted a refused certificate"
        );
        if let Ok(close) = client {
            assert!(
                matches!(close, Some(Close::Transport { .. })),
                "the client saw {close:?}"
            );
        }
    }
}

#[tokio::test]
async fn a_handshake_without_alpn_mqtt_fails() {
    let (_pki, identity, target) = setup();
    let server = FakeServer::bind(&identity, &ClientAuth::None).unwrap();
    let target = target(&server).with_alpn(vec![b"h3".to_vec()]);
    let (client, accepted) = tokio::join!(RawConnection::connect(&target), server.accept());
    assert!(client.is_err());
    assert!(accepted.is_err());
}

/// Two clients through the fake broker: a QoS 1 subscription and a publication to it.
fn pub_sub() -> Scenario {
    Scenario::new("pub_sub", "A QoS 1 publication reaches a QoS 1 subscriber")
        .connect("sub", packets::connect("{ns}-sub"))
        .send("sub", packets::subscribe(1, "{ns}/t/+", QoS::AtLeastOnce))
        .expect_type("sub", PacketType::SubAck)
        .connect("pub", packets::connect("{ns}-pub"))
        .send(
            "pub",
            packets::publish("{ns}/t/1", QoS::AtLeastOnce, 9, "hello"),
        )
        .expect_type("pub", PacketType::PubAck)
        .expect_packet(
            "sub",
            "the publication",
            |packet| matches!(packet, Packet::Publish(publish) if publish.payload == "hello"),
        )
        .send("sub", packets::puback(1))
        .disconnect("pub")
        .disconnect("sub")
}

#[tokio::test]
async fn a_scenario_plays_on_the_fake_broker_and_two_runs_trace_the_same() {
    let (_pki, identity, target) = setup();
    let server = FakeServer::bind(&identity, &ClientAuth::None).unwrap();
    let target = target(&server);
    let _broker = FakeBroker::start(server);
    let runner = Runner::new(target);
    let first = runner.run(&pub_sub()).await;
    let second = runner.run(&pub_sub()).await;
    assert!(first.failures.is_empty(), "{:#?}", first.failures);
    assert!(second.failures.is_empty(), "{:#?}", second.failures);
    assert_ne!(first.namespace, second.namespace);
    assert_eq!(first.trace(), second.trace());
    assert_ne!(first.raw_trace(), second.raw_trace());

    let trace = first.trace();
    let sub = trace["clients"]["sub"].as_array().unwrap();
    let delivery = sub
        .iter()
        .find_map(|entry| {
            entry["received"]
                .as_object()
                .filter(|p| p["type"] == "PUBLISH")
        })
        .expect("the subscriber received the publication");
    assert_eq!(delivery["topic"], "{ns}/t/1");
    assert_eq!(delivery["packet_id"], "s1");
    let publication = trace["clients"]["pub"]
        .as_array()
        .unwrap()
        .iter()
        .find_map(|entry| entry["sent"].as_object().filter(|p| p["type"] == "PUBLISH"))
        .expect("the publisher sent its publication");
    assert_eq!(publication["packet_id"], "c1");
}

#[tokio::test]
async fn a_failed_expectation_is_reported_and_the_run_goes_on() {
    let (_pki, identity, target) = setup();
    let server = FakeServer::bind(&identity, &ClientAuth::None).unwrap();
    let target = target(&server);
    let _broker = FakeBroker::start(server);
    let scenario = Scenario::new("wrong", "Expects a SUBACK where a PUBACK comes")
        .connect("a", packets::connect("{ns}-a"))
        .send("a", packets::publish("{ns}/t", QoS::AtLeastOnce, 1, "x"))
        .expect_type("a", PacketType::SubAck)
        .send("a", packets::subscribe(2, "{ns}/t", QoS::AtMostOnce))
        .expect_type("a", PacketType::SubAck)
        .expect_type("a", PacketType::PingResp)
        .disconnect("a");
    let outcome = Runner::new(target).run(&scenario).await;
    assert_eq!(outcome.failures.len(), 2, "{:#?}", outcome.failures);
    assert!(
        outcome.failures[0].contains("expected SUBACK, got PubAck"),
        "{}",
        outcome.failures[0]
    );
    assert!(
        outcome.failures[1].contains("got nothing"),
        "{}",
        outcome.failures[1]
    );
}
