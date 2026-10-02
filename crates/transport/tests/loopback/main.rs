//! The client listener over real QUIC on the loopback interface, against the test kit's raw
//! client and `openqtt-client`: the handshake and its refusals, the stream mapping of
//! docs/spec/mqtt-over-quic.md, stream ends, backpressure, 0-RTT and closing.

mod control;
mod handshake;

use std::net::SocketAddr;
use std::time::Duration;

use openqtt_testkit::{RawConnection, Target, TestPki};
use openqtt_transport::{
    Endpoint, Error, Event, Listener, ListenerConfig, MqttConnection, QuicConnection, StreamTag,
};

/// How long a test waits for something that must happen.
pub(crate) const WAIT: Duration = Duration::from_secs(5);

/// How long a test waits to be sure that something does not happen.
pub(crate) const QUIET: Duration = Duration::from_millis(300);

/// A listener named `devices` on a loopback port of the system's choosing, presenting a
/// certificate for localhost from `pki`.
pub(crate) fn config(pki: &TestPki) -> ListenerConfig {
    let identity = pki.server(&["localhost"]).expect("a server certificate");
    ListenerConfig::new("devices", identity.chain.clone(), identity.key())
        .address(SocketAddr::from(([127, 0, 0, 1], 0)))
}

/// The one endpoint of a listener as `config` sets it.
pub(crate) fn bind(config: ListenerConfig) -> Endpoint {
    Listener::new(config)
        .expect("a listener")
        .bind(0)
        .expect("an endpoint")
}

/// A raw client's view of `endpoint`, trusting `pki`.
pub(crate) fn target(endpoint: &Endpoint, pki: &TestPki) -> Target {
    Target::new(
        endpoint.local_address(),
        "localhost",
        vec![pki.ca_certificate()],
    )
}

/// Connects a raw client to `target` and accepts it on `endpoint`, together: quinn holds a
/// handshake until the server accepts it.
pub(crate) async fn pair(
    endpoint: &Endpoint,
    target: &Target,
) -> (
    Result<RawConnection, openqtt_testkit::Error>,
    Result<QuicConnection, Error>,
) {
    tokio::join!(RawConnection::connect(target), async {
        endpoint
            .accept()
            .await
            .expect("a connection attempt")
            .establish()
            .await
    })
}

/// [`pair`], for a client the server accepts.
pub(crate) async fn connected(
    endpoint: &Endpoint,
    target: &Target,
) -> (RawConnection, QuicConnection) {
    let (client, server) = pair(endpoint, target).await;
    (
        client.expect("the client connects"),
        server.expect("the server accepts"),
    )
}

/// The connection's next event, which must come in time.
pub(crate) async fn next(connection: &mut QuicConnection) -> Result<Event, Error> {
    tokio::time::timeout(WAIT, connection.recv())
        .await
        .expect("an event in time")
}

/// Whether the connection yields nothing for a while.
pub(crate) async fn quiet(connection: &mut QuicConnection) -> bool {
    tokio::time::timeout(QUIET, connection.recv())
        .await
        .is_err()
}

/// Takes the start of every connection: the handshake, then the CONNECT on the control stream.
pub(crate) async fn take_connect(server: &mut QuicConnection) {
    assert_eq!(
        next(server).await.expect("the handshake"),
        Event::HandshakeComplete { early_data: false }
    );
    let event = next(server).await.expect("the CONNECT");
    assert!(
        matches!(
            event,
            Event::Packet {
                stream: StreamTag::Control,
                packet: openqtt_testkit::codec::Packet::Connect(_),
            }
        ),
        "{event:?}"
    );
}
