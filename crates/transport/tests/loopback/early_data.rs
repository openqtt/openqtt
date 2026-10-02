//! 0-RTT (docs/spec/mqtt-over-quic.md, section 4), which a listener accepts only when told to and
//! with rustls's session cache (report R7, F2): a CONNECT sent in 0-RTT reaches the edge before
//! the handshake completes, and the HandshakeComplete event marks where confirmed data begins.

use std::net::{Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::sync::Arc;

use openqtt_testkit::codec::{ConnAck, Packet};
use openqtt_testkit::{TestPki, packets};
use openqtt_transport::{
    ClientAuth, Endpoint, Event, MqttConnection, QuicConnection, Resumption, StreamTag,
};
use quinn::crypto::rustls::QuicClientConfig;

use crate::{WAIT, bind, config, encode, next};

/// A client that keeps the sessions it is given and sends 0-RTT data on resuming one, with a
/// client certificate from `pki`.
fn client_config(pki: &TestPki) -> quinn::ClientConfig {
    let device = pki.client("device-1").expect("a client certificate");
    let mut roots = rustls::RootCertStore::empty();
    roots.add(pki.ca_certificate()).expect("the CA");
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3")
        .with_root_certificates(roots)
        .with_client_auth_cert(device.chain.clone(), device.key())
        .expect("the client certificate");
    tls.alpn_protocols = vec![b"mqtt".to_vec()];
    tls.enable_early_data = true;
    let crypto = QuicClientConfig::try_from(Arc::new(tls)).expect("TLS for QUIC");
    quinn::ClientConfig::new(Arc::new(crypto))
}

/// Accepts the next connection on `endpoint`.
async fn accept(endpoint: &Endpoint) -> QuicConnection {
    endpoint
        .accept()
        .await
        .expect("a connection attempt")
        .establish()
        .await
        .expect("the handshake")
}

#[tokio::test]
async fn a_connect_in_0_rtt_arrives_before_the_handshake_completes() {
    let pki = TestPki::new("Early data CA").unwrap();
    let endpoint = bind(
        config(&pki)
            .client_auth(ClientAuth::Required(vec![pki.ca_certificate()]))
            .resumption(Resumption::SessionCache(NonZeroUsize::new(64).unwrap()))
            .early_data(true),
    );
    let client = quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap();
    let config = client_config(&pki);
    let address = endpoint.local_address();

    // A full handshake first: no early data, and a session for the client to resume.
    let (connection, mut server) = tokio::join!(
        async {
            client
                .connect_with(config.clone(), address, "localhost")
                .unwrap()
                .await
                .unwrap()
        },
        accept(&endpoint)
    );
    assert!(!server.peer().early_data);
    let (mut send, mut recv) = connection.open_bi().await.unwrap();
    send.write_all(&encode(packets::connect("device-1")))
        .await
        .unwrap();
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::HandshakeComplete { early_data: false }
    );
    assert!(matches!(
        next(&mut server).await.unwrap(),
        Event::Packet {
            packet: Packet::Connect(_),
            ..
        }
    ));
    server
        .send(StreamTag::Control, &Packet::from(ConnAck::default()))
        .unwrap();
    server.flush().await.unwrap();
    // The CONNACK came after the session tickets, so the client holds one now.
    let mut connack = [0; 5];
    recv.read_exact(&mut connack).await.unwrap();
    connection.close(quinn::VarInt::from_u32(0), b"");
    drop(server);

    // Resumed with CONNECT in 0-RTT data.
    let connecting = client.connect_with(config, address, "localhost").unwrap();
    let (connection, accepted) = connecting
        .into_0rtt()
        .unwrap_or_else(|_| panic!("the client resumes its session"));
    let (mut send, _recv) = connection.open_bi().await.unwrap();
    send.write_all(&encode(packets::connect("device-1")))
        .await
        .unwrap();
    let mut server = accept(&endpoint).await;
    assert!(server.peer().early_data);
    // The certificate verified when the session began.
    assert_eq!(server.peer().certificates.len(), 1);
    // The CONNECT first, then the handshake completing: until then the session acts on no
    // PUBLISH, SUBSCRIBE or UNSUBSCRIBE, and sends no CONNACK.
    let event = next(&mut server).await.unwrap();
    assert!(
        matches!(
            &event,
            Event::Packet {
                stream: StreamTag::Control,
                packet: Packet::Connect(connect),
            } if connect.client_id == "device-1"
        ),
        "{event:?}"
    );
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::HandshakeComplete { early_data: true }
    );
    assert!(tokio::time::timeout(WAIT, accepted).await.unwrap());
}
