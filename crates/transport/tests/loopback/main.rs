//! The client listener over real QUIC on the loopback interface, against the test kit's raw
//! client and `openqtt-client`: the handshake and its refusals, the stream mapping of
//! docs/spec/mqtt-over-quic.md, stream ends, backpressure, 0-RTT and closing.

mod backpressure;
mod control;
mod early_data;
mod handshake;
mod streams;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use openqtt_testkit::{RawConnection, Target, TestPki};
use openqtt_transport::CID_LEN;
use openqtt_transport::{
    Endpoint, Error, Event, Listener, ListenerConfig, MqttConnection, QuicConnection, StreamTag,
};
use tokio::net::UdpSocket;

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

/// A UDP relay between one client and a server, which records the destination connection ID of
/// every short-header packet the client sends, and can lose what the client sends.
pub(crate) struct Relay {
    pub(crate) address: SocketAddr,
    ids: Arc<Mutex<Vec<Bytes>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Relay {
    /// Relays to `server` the first `forwarded` datagrams the client sends, and drops the rest.
    pub(crate) async fn start(server: SocketAddr, forwarded: usize) -> Self {
        let outside = UdpSocket::bind("127.0.0.1:0").await.expect("a socket");
        let inside = UdpSocket::bind("127.0.0.1:0").await.expect("a socket");
        inside.connect(server).await.expect("the server's address");
        let address = outside.local_addr().expect("a bound socket");
        let ids = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&ids);
        let task = tokio::spawn(async move {
            let mut from_client = vec![0; 65_536];
            let mut from_server = vec![0; 65_536];
            let mut client = None;
            let mut forwarded = forwarded;
            loop {
                tokio::select! {
                    received = outside.recv_from(&mut from_client) => {
                        let Ok((length, from)) = received else { return };
                        client = Some(from);
                        let datagram = &from_client[..length];
                        // A short header: the form bit clear, the connection ID right after.
                        if datagram.first().is_some_and(|first| first & 0x80 == 0)
                            && let Some(id) = datagram.get(1..1 + CID_LEN)
                        {
                            recorded
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner)
                                .push(Bytes::copy_from_slice(id));
                        }
                        if forwarded == 0 {
                            continue;
                        }
                        forwarded -= 1;
                        if inside.send(datagram).await.is_err() {
                            return;
                        }
                    }
                    received = inside.recv(&mut from_server) => {
                        let Ok(length) = received else { return };
                        if let Some(client) = client
                            && outside.send_to(&from_server[..length], client).await.is_err()
                        {
                            return;
                        }
                    }
                }
            }
        });
        Self { address, ids, task }
    }

    /// The destination connection IDs of the client's short-header packets.
    pub(crate) fn short_header_ids(&self) -> Vec<Bytes> {
        self.ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}
