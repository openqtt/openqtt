//! A fake server for client tests: a QUIC listener that hands each connection to the test as
//! a [`RawConnection`], and a tiny broker on top of it.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use openqtt_codec::{
    AckProperties, ConnAck, ConnAckProperties, Packet, PacketId, PubAck, PubComp, PubRec, PubRel,
    Publish, QoS, Sender, SubAck, SubAckReasonCode, UnsubAck, UnsubAckReasonCode,
};
use quinn::crypto::rustls::QuicServerConfig;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::raw::{RawConnection, Recorded};
use crate::tls::{ClientAuth, server_config};
use crate::{Error, Identity};

/// A QUIC listener on a loopback port that speaks TLS with ALPN `mqtt` and hands every
/// connection to the test, which plays the server's side packet by packet.
#[derive(Debug)]
pub struct FakeServer {
    endpoint: quinn::Endpoint,
    addr: SocketAddr,
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        self.endpoint.close(quinn::VarInt::from_u32(0), b"");
    }
}

impl FakeServer {
    /// Listens on 127.0.0.1 with a port of the system's choosing, presenting `identity`.
    ///
    /// # Errors
    ///
    /// When the TLS configuration is refused or the socket cannot be bound.
    pub fn bind(identity: &Identity, client_auth: &ClientAuth) -> Result<Self, Error> {
        let tls = server_config(identity, client_auth)?;
        let crypto = QuicServerConfig::try_from(tls)
            .map_err(|error| Error::TlsForQuic(error.to_string()))?;
        let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        config.transport_config(crate::raw::transport_config()?);
        let endpoint = quinn::Endpoint::server(config, SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
        let addr = endpoint.local_addr()?;
        Ok(Self { endpoint, addr })
    }

    /// Where clients connect.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The next connection, once its handshake completed and its control stream carried its
    /// first bytes.
    ///
    /// # Errors
    ///
    /// When the handshake fails, as it does for a client certificate the server refuses.
    pub async fn accept(&self) -> Result<RawConnection, Error> {
        let incoming = self.endpoint.accept().await.ok_or(Error::EndpointClosed)?;
        let connection = incoming.await?;
        let (send, recv) = connection.accept_bi().await?;
        Ok(RawConnection::start(
            connection,
            send,
            recv,
            Sender::Client,
            None,
        ))
    }
}

/// A broker just large enough to exercise a client: CONNACK for every CONNECT, SUBACK and
/// UNSUBACK, routing of PUBLISH to matching subscriptions at the lower of the two QoS, every
/// acknowledgement of QoS 1 and 2, PINGRESP, and close on DISCONNECT. No sessions outlive a
/// connection, no retained messages, no will, no limits.
#[derive(Debug)]
pub struct FakeBroker {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for FakeBroker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The subscriptions of every connection.
#[derive(Debug, Default)]
struct Routes {
    next_connection: AtomicU64,
    subscriptions: Mutex<HashMap<u64, Subscriber>>,
}

#[derive(Debug)]
struct Subscriber {
    filters: Vec<(String, QoS)>,
    deliver: mpsc::UnboundedSender<Publish>,
}

impl FakeBroker {
    /// Serves every connection `server` accepts, each on a task of its own.
    pub fn start(server: FakeServer) -> Self {
        let addr = server.addr();
        let routes = Arc::new(Routes::default());
        let task = tokio::spawn(async move {
            while let Ok(connection) = server.accept().await {
                let id = routes.next_connection.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(serve(connection, Arc::clone(&routes), id));
            }
        });
        Self { addr, task }
    }

    /// Where clients connect.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

/// Serves one connection until it closes.
async fn serve(mut connection: RawConnection, routes: Arc<Routes>, id: u64) {
    let (deliver, mut deliveries) = mpsc::unbounded_channel();
    routes
        .subscriptions
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(
            id,
            Subscriber {
                filters: Vec::new(),
                deliver,
            },
        );
    let mut next_id: u16 = 0;
    loop {
        tokio::select! {
            record = connection.recv(Duration::from_secs(3_600)) => {
                let Some(Recorded::Received(packet)) = record.map(|record| record.event) else {
                    break;
                };
                if handle(&mut connection, &routes, id, packet).await.is_err() {
                    break;
                }
            }
            Some(mut publish) = deliveries.recv() => {
                if publish.qos != QoS::AtMostOnce {
                    next_id = next_id.checked_add(1).unwrap_or(1);
                    publish.packet_id = PacketId::new(next_id);
                }
                if connection.send(publish).await.is_err() {
                    break;
                }
            }
        }
    }
    routes
        .subscriptions
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&id);
}

/// Answers one packet; an error ends the connection.
async fn handle(
    connection: &mut RawConnection,
    routes: &Routes,
    id: u64,
    packet: Packet,
) -> Result<(), Error> {
    match packet {
        Packet::Connect(connect) => {
            let assigned = connect.client_id.is_empty().then(|| format!("fake-{id}"));
            connection
                .send(ConnAck {
                    properties: ConnAckProperties {
                        assigned_client_identifier: assigned,
                        ..ConnAckProperties::default()
                    },
                    ..ConnAck::default()
                })
                .await
        }
        Packet::Subscribe(subscribe) => {
            let mut reason_codes = Vec::new();
            {
                let mut subscriptions = routes
                    .subscriptions
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if let Some(subscriber) = subscriptions.get_mut(&id) {
                    for subscription in subscribe.subscriptions {
                        let qos = subscription.options.maximum_qos;
                        subscriber
                            .filters
                            .retain(|(filter, _)| *filter != subscription.filter);
                        subscriber.filters.push((subscription.filter, qos));
                        reason_codes.push(match qos {
                            QoS::AtMostOnce => SubAckReasonCode::GrantedQos0,
                            QoS::AtLeastOnce => SubAckReasonCode::GrantedQos1,
                            QoS::ExactlyOnce => SubAckReasonCode::GrantedQos2,
                        });
                    }
                }
            }
            connection
                .send(SubAck {
                    packet_id: subscribe.packet_id,
                    properties: AckProperties::default(),
                    reason_codes,
                })
                .await
        }
        Packet::Unsubscribe(unsubscribe) => {
            let mut reason_codes = Vec::new();
            {
                let mut subscriptions = routes
                    .subscriptions
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if let Some(subscriber) = subscriptions.get_mut(&id) {
                    for filter in &unsubscribe.filters {
                        let before = subscriber.filters.len();
                        subscriber.filters.retain(|(kept, _)| kept != filter);
                        reason_codes.push(if subscriber.filters.len() < before {
                            UnsubAckReasonCode::Success
                        } else {
                            UnsubAckReasonCode::NoSubscriptionExisted
                        });
                    }
                }
            }
            connection
                .send(UnsubAck {
                    packet_id: unsubscribe.packet_id,
                    properties: AckProperties::default(),
                    reason_codes,
                })
                .await
        }
        Packet::Publish(publish) => {
            route(routes, &publish);
            match (publish.qos, publish.packet_id) {
                (QoS::AtLeastOnce, Some(packet_id)) => {
                    connection.send(PubAck::new(packet_id)).await
                }
                (QoS::ExactlyOnce, Some(packet_id)) => {
                    connection.send(PubRec::new(packet_id)).await
                }
                _ => Ok(()),
            }
        }
        Packet::PubRel(pubrel) => connection.send(PubComp::new(pubrel.packet_id)).await,
        Packet::PubRec(pubrec) => connection.send(PubRel::new(pubrec.packet_id)).await,
        Packet::PingReq => connection.send(Packet::PingResp).await,
        Packet::Disconnect(_) => Err(Error::Scenario("the client disconnected".into())),
        _ => Ok(()),
    }
}

/// Hands a copy of `publish` to every connection with a matching subscription.
fn route(routes: &Routes, publish: &Publish) {
    let subscriptions = routes
        .subscriptions
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    for subscriber in subscriptions.values() {
        let granted = subscriber
            .filters
            .iter()
            .filter(|(filter, _)| matches(filter, &publish.topic))
            .map(|(_, qos)| *qos)
            .max();
        if let Some(granted) = granted {
            let mut copy = publish.clone();
            copy.qos = copy.qos.min(granted);
            copy.packet_id = None;
            copy.dup = false;
            copy.retain = false;
            // A receiver that went away is cleaned up by its own task.
            drop(subscriber.deliver.send(copy));
        }
    }
}

/// Whether a Topic Filter matches a Topic Name, with `+` and `#` (section 4.7).
fn matches(filter: &str, topic: &str) -> bool {
    let mut topic_levels = topic.split('/');
    for level in filter.split('/') {
        match (level, topic_levels.next()) {
            ("#", _) => return true,
            ("+", Some(_)) => {}
            (level, Some(name)) if level == name => {}
            _ => return false,
        }
    }
    topic_levels.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_match_by_level() {
        assert!(matches("a/b", "a/b"));
        assert!(!matches("a/b", "a/b/c"));
        assert!(matches("a/+", "a/b"));
        assert!(!matches("a/+", "a/b/c"));
        assert!(matches("a/#", "a/b/c"));
        assert!(matches("a/#", "a"));
        assert!(matches("#", "a"));
        assert!(!matches("a/b/c", "a/b"));
    }
}
