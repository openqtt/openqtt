//! The client over real QUIC on the loopback interface, against the test kit's fake server and
//! fake broker: mutual TLS, every QoS both ways, keep alive, and a clean close, with no Docker
//! and no broker.

use std::time::Duration;

use openqtt_client::codec::{ConnectReasonCode, Packet, SubAckReasonCode};
use openqtt_client::{
    Client, ConnectOptions, Event, Published, QoS, QuicTransport, SubscriptionOptions, TlsConfig,
};
use openqtt_testkit::codec::ConnAck;
use openqtt_testkit::{ClientAuth, Close, FakeBroker, FakeServer, Recorded, TestPki};

const WAIT: Duration = Duration::from_secs(5);

/// A CA, and a fake broker presenting a certificate for localhost that requires a client
/// certificate from that CA.
fn broker(pki: &TestPki) -> FakeBroker {
    let identity = pki.server(&["localhost"]).expect("a server certificate");
    let server = FakeServer::bind(&identity, &ClientAuth::Required(vec![pki.ca_certificate()]))
        .expect("a fake server");
    FakeBroker::start(server)
}

/// A transport to `addr` trusting `pki`, presenting a client certificate for `device`.
fn transport(pki: &TestPki, addr: std::net::SocketAddr, device: &str) -> QuicTransport {
    let identity = pki.client(device).expect("a client certificate");
    let tls = TlsConfig::builder()
        .root_certificate(pki.ca_certificate())
        .client_certificate(identity.chain.clone(), identity.key())
        .build()
        .expect("a TLS configuration");
    QuicTransport::new(addr, "localhost", &tls).expect("a QUIC transport")
}

#[tokio::test]
async fn every_qos_goes_both_ways_over_quic_with_mutual_tls() {
    let pki = TestPki::new("Client loopback CA").unwrap();
    let broker = broker(&pki);
    let transport = transport(&pki, broker.addr(), "device-1");
    let (client, mut events) = Client::connect(&transport, ConnectOptions::new("device-1"))
        .await
        .unwrap();
    assert_eq!(client.connack().reason_code, ConnectReasonCode::Success);

    let suback = client
        .subscribe(
            "t/#",
            SubscriptionOptions {
                maximum_qos: QoS::ExactlyOnce,
                ..SubscriptionOptions::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(suback.reason_codes, [SubAckReasonCode::GrantedQos2]);

    for (qos, payload) in [
        (QoS::AtMostOnce, "zero"),
        (QoS::AtLeastOnce, "one"),
        (QoS::ExactlyOnce, "two"),
    ] {
        let published = client
            .publish(openqtt_client::Publish {
                qos,
                topic: format!("t/{payload}"),
                payload: payload.into(),
                ..openqtt_client::Publish::default()
            })
            .await
            .unwrap();
        assert!(published.is_accepted(), "{published:?}");
        assert!(matches!(
            (qos, &published),
            (QoS::AtMostOnce, Published::AtMostOnce)
                | (QoS::AtLeastOnce, Published::AtLeastOnce(_))
                | (
                    QoS::ExactlyOnce,
                    Published::ExactlyOnce {
                        pubcomp: Some(_),
                        ..
                    }
                )
        ));
        let message = tokio::time::timeout(WAIT, events.next_message())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.topic, format!("t/{payload}"));
        assert_eq!(message.payload, payload);
        assert_eq!(message.qos, qos);
    }
    let session = client.disconnect().await.unwrap();
    assert_eq!(session.unacknowledged(), 0);
    assert_eq!(
        events.recv().await,
        Some(Event::Closed(openqtt_client::CloseReason::Disconnected))
    );
}

#[tokio::test]
async fn keep_alive_pings_go_out_and_are_answered() {
    let pki = TestPki::new("Client loopback CA").unwrap();
    let broker = broker(&pki);
    let transport = transport(&pki, broker.addr(), "device-2");
    let options = ConnectOptions::new("device-2")
        .keep_alive(1)
        .packet_log(true);
    let (client, mut events) = Client::connect(&transport, options).await.unwrap();
    assert!(matches!(
        events.recv().await,
        Some(Event::Received(Packet::ConnAck(_)))
    ));
    for _ in 0..2 {
        let event = tokio::time::timeout(WAIT, events.recv()).await.unwrap();
        assert_eq!(event, Some(Event::Received(Packet::PingResp)));
    }
    assert!(!client.is_closed());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn a_disconnect_reaches_the_server_before_the_quic_close_with_code_0() {
    let pki = TestPki::new("Client loopback CA").unwrap();
    let identity = pki.server(&["localhost"]).unwrap();
    let server = FakeServer::bind(&identity, &ClientAuth::None).unwrap();
    let tls = TlsConfig::builder()
        .root_certificate(pki.ca_certificate())
        .build()
        .unwrap();
    let transport = QuicTransport::new(server.addr(), "localhost", &tls).unwrap();
    let (connected, accepted) = tokio::join!(
        Client::connect(&transport, ConnectOptions::new("device-3")),
        async {
            let mut peer = server.accept().await.unwrap();
            let Some(Packet::Connect(connect)) = peer.recv_packet(WAIT).await else {
                panic!("CONNECT first");
            };
            assert_eq!(connect.client_id, "device-3");
            peer.send(ConnAck::default()).await.unwrap();
            peer
        }
    );
    let (client, _events) = connected.unwrap();
    let mut peer = accepted;
    client.disconnect().await.unwrap();
    assert_eq!(
        peer.recv_packet(WAIT).await,
        Some(Packet::Disconnect(openqtt_client::Disconnect::default()))
    );
    // The client finished its side of the control stream, then closed with code 0
    // (docs/spec/mqtt-over-quic.md, section 7).
    assert_eq!(
        peer.recv(WAIT).await.map(|record| record.event),
        Some(Recorded::StreamFinished)
    );
    assert!(matches!(
        peer.closed(WAIT).await,
        Some(Close::Application { code: 0, .. })
    ));
}

#[tokio::test]
async fn a_client_without_the_certificate_the_server_requires_is_refused() {
    let pki = TestPki::new("Client loopback CA").unwrap();
    let broker = broker(&pki);
    let tls = TlsConfig::builder()
        .root_certificate(pki.ca_certificate())
        .build()
        .unwrap();
    let transport = QuicTransport::new(broker.addr(), "localhost", &tls).unwrap();
    let options = ConnectOptions::new("anonymous").connect_timeout(WAIT);
    assert!(Client::connect(&transport, options).await.is_err());
}

#[tokio::test]
async fn a_client_certificate_from_another_ca_is_refused() {
    let pki = TestPki::new("Client loopback CA").unwrap();
    let broker = broker(&pki);
    let other = TestPki::new("Other CA").unwrap();
    let foreign = other.client("device-4").unwrap();
    let tls = TlsConfig::builder()
        .root_certificate(pki.ca_certificate())
        .client_certificate(foreign.chain.clone(), foreign.key())
        .build()
        .unwrap();
    let transport = QuicTransport::new(broker.addr(), "localhost", &tls).unwrap();
    let options = ConnectOptions::new("device-4").connect_timeout(WAIT);
    assert!(Client::connect(&transport, options).await.is_err());
}

#[tokio::test]
async fn a_server_the_roots_do_not_vouch_for_is_refused() {
    let pki = TestPki::new("Client loopback CA").unwrap();
    let broker = broker(&pki);
    let other = TestPki::new("Other CA").unwrap();
    let identity = pki.client("device-5").unwrap();
    let tls = TlsConfig::builder()
        .root_certificate(other.ca_certificate())
        .client_certificate(identity.chain.clone(), identity.key())
        .build()
        .unwrap();
    let transport = QuicTransport::new(broker.addr(), "localhost", &tls).unwrap();
    let error = Client::connect(&transport, ConnectOptions::new("device-5"))
        .await
        .unwrap_err();
    assert!(
        matches!(error, openqtt_client::Error::Transport(_)),
        "{error}"
    );
}
