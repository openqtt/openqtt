//! The handshake: TLS 1.3 with ALPN `mqtt`, and client certificates verified against the
//! listener's CAs alone (R2 rules 1 to 3, docs/spec/mqtt-over-quic.md sections 1 and 3).

use std::net::Ipv4Addr;

use openqtt_testkit::codec::Packet;
use openqtt_testkit::{Close, RawConnection, TestPki, packets};
use openqtt_transport::{
    ClientAuth, Closed, Error, Event, MqttConnection, QuicConnection, StreamTag,
};

use crate::{bind, config, connected, next, pair, target};

/// The QUIC transport error code of TLS alert `alert` (RFC 9001, section 4.8).
const fn crypto_error(alert: u64) -> u64 {
    0x100 + alert
}

/// The transport error code the server refused a client with, from its side and the client's.
async fn refusal(
    client: Result<RawConnection, openqtt_testkit::Error>,
    server: Result<QuicConnection, Error>,
) -> (u64, u64) {
    let server = match server {
        Err(Error::Handshake(Closed::Transport { code, .. })) => code,
        Err(other) => panic!("a refused handshake, not {other}"),
        Ok(_) => panic!("the server accepted the client"),
    };
    // The client may have finished its side of the handshake before the server judged its
    // certificate, so the refusal can come after it thinks it is connected.
    let client = match client {
        Ok(mut client) => match client.closed(crate::WAIT).await {
            Some(Close::Transport { code, .. }) => code,
            other => panic!("a transport close, not {other:?}"),
        },
        Err(openqtt_testkit::Error::Connection(quinn::ConnectionError::ConnectionClosed(
            close,
        ))) => u64::from(close.error_code),
        Err(other) => panic!("a refused handshake, not {other}"),
    };
    (server, client)
}

#[tokio::test]
async fn a_client_without_a_certificate_connects_where_none_is_asked() {
    let pki = TestPki::new("Handshake CA").unwrap();
    let endpoint = bind(config(&pki));
    let (mut client, mut server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    let peer = server.peer();
    assert!(peer.certificates.is_empty());
    assert_eq!(peer.alpn.as_deref(), Some(&b"mqtt"[..]));
    assert!(!peer.early_data);
    assert_eq!(&*peer.listener, "devices");
    assert_eq!(peer.address.ip(), Ipv4Addr::LOCALHOST);
    assert_eq!(client.alpn().as_deref(), Some(&b"mqtt"[..]));

    client.send(packets::connect("device-1")).await.unwrap();
    assert_eq!(
        next(&mut server).await.unwrap(),
        Event::HandshakeComplete { early_data: false }
    );
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
}

#[tokio::test]
async fn a_required_certificate_is_verified_and_handed_up() {
    let pki = TestPki::new("Handshake CA").unwrap();
    let endpoint = bind(config(&pki).client_auth(ClientAuth::Required(vec![pki.ca_certificate()])));
    let device = pki.client("device-2").unwrap();
    let target = target(&endpoint, &pki).with_identity(device.clone());
    let (_client, server) = connected(&endpoint, &target).await;
    // The chain as the client presented it, for openqtt-auth to read the CN and the extended key
    // usage from.
    let certificates = &server.peer().certificates;
    assert_eq!(certificates.len(), 1);
    assert_eq!(certificates[0].der(), device.chain[0].as_ref());
}

#[tokio::test]
async fn a_client_without_the_required_certificate_is_refused_with_alert_116() {
    let pki = TestPki::new("Handshake CA").unwrap();
    let endpoint = bind(config(&pki).client_auth(ClientAuth::Required(vec![pki.ca_certificate()])));
    let (client, server) = pair(&endpoint, &target(&endpoint, &pki)).await;
    // certificate_required (R2 rule 1).
    assert_eq!(
        refusal(client, server).await,
        (crypto_error(116), crypto_error(116))
    );
}

#[tokio::test]
async fn a_certificate_from_another_issuer_is_refused() {
    let pki = TestPki::new("Handshake CA").unwrap();
    let endpoint = bind(config(&pki).client_auth(ClientAuth::Required(vec![pki.ca_certificate()])));
    let other = TestPki::new("Other CA").unwrap();
    for device in [
        other.client("device-3").unwrap(),
        // The same issuer name, signed by another key (R2 rule 2).
        pki.impostor_client("device-4").unwrap(),
        // A server certificate presented as a client's: no clientAuth usage (R2 rule 3).
        pki.client_with_server_eku("device-5").unwrap(),
    ] {
        let target = target(&endpoint, &pki).with_identity(device);
        let (client, server) = pair(&endpoint, &target).await;
        let (server, client) = refusal(client, server).await;
        assert_eq!(server, client);
        assert!(
            (0x100..0x200).contains(&server),
            "a TLS alert, not {server:#x}"
        );
    }
}

#[tokio::test]
async fn a_certificate_without_an_extended_key_usage_is_left_to_the_authenticator() {
    // rustls takes a certificate with no extended key usage as valid for any use (R7, F4). The
    // transport hands its chain up, and openqtt-auth refuses it for lacking clientAuth.
    let pki = TestPki::new("Handshake CA").unwrap();
    let endpoint = bind(config(&pki).client_auth(ClientAuth::Required(vec![pki.ca_certificate()])));
    let device = pki.client_without_eku("device-6").unwrap();
    let target = target(&endpoint, &pki).with_identity(device.clone());
    let (_client, server) = connected(&endpoint, &target).await;
    assert_eq!(
        server.peer().certificates[0].der(),
        device.chain[0].as_ref()
    );
}

#[tokio::test]
async fn an_optional_certificate_may_be_absent_but_not_wrong() {
    let pki = TestPki::new("Handshake CA").unwrap();
    let endpoint = bind(config(&pki).client_auth(ClientAuth::Optional(vec![pki.ca_certificate()])));
    let (_client, server) = connected(&endpoint, &target(&endpoint, &pki)).await;
    assert!(server.peer().certificates.is_empty());

    let device = pki.client("device-7").unwrap();
    let target_with = target(&endpoint, &pki).with_identity(device);
    let (_client, server) = connected(&endpoint, &target_with).await;
    assert_eq!(server.peer().certificates.len(), 1);

    let other = TestPki::new("Other CA").unwrap();
    let target_wrong = target(&endpoint, &pki).with_identity(other.client("device-8").unwrap());
    let (client, server) = pair(&endpoint, &target_wrong).await;
    let (server, _) = refusal(client, server).await;
    assert!(
        (0x100..0x200).contains(&server),
        "a TLS alert, not {server:#x}"
    );
}

#[tokio::test]
async fn an_application_protocol_other_than_mqtt_is_refused_with_alert_120() {
    let pki = TestPki::new("Handshake CA").unwrap();
    let endpoint = bind(config(&pki));
    for alpn in [vec![b"h3".to_vec()], Vec::new()] {
        let target = target(&endpoint, &pki).with_alpn(alpn);
        let (client, server) = pair(&endpoint, &target).await;
        // no_application_protocol (docs/spec/mqtt-over-quic.md, section 1).
        assert_eq!(
            refusal(client, server).await,
            (crypto_error(120), crypto_error(120))
        );
    }
}
