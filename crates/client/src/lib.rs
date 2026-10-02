//! An MQTT 5 over QUIC client, for devices, the test kit and benchmarks.
//!
//! It speaks MQTT 5.0 and nothing older (docs/adr/0001-mqtt5-only.md), over QUIC as
//! docs/spec/mqtt-over-quic.md defines it: ALPN `mqtt`, TLS 1.3 on rustls with aws-lc-rs, and
//! the control stream carrying every packet (single-stream mode, section 2.2), which every
//! OpenQTT and EMQX server supports. It is built on `openqtt-codec`, not on the broker's
//! session machine, so it must not depend on `openqtt-session`.
//!
//! ```no_run
//! # async fn run(ca: openqtt_client::CertificateDer<'static>) -> Result<(), openqtt_client::Error> {
//! use openqtt_client::{Client, ConnectOptions, QuicTransport, TlsConfig};
//!
//! let tls = TlsConfig::builder().root_certificate(ca).build()?;
//! let transport = QuicTransport::new("192.0.2.10:14567".parse().unwrap(), "broker.example", &tls)?;
//! let (client, mut events) = Client::connect(&transport, ConnectOptions::new("sensor-17")).await?;
//! # Ok(()) }
//! ```
//!
//! # What the client does for the application
//!
//! - Packet Identifiers: taken from those not in use, scoped to the session ([MQTT-2.2.1-3]).
//! - Acknowledgements: every PUBLISH from the server gets the PUBACK or PUBREC its QoS calls
//!   for, in arrival order, and every PUBREL its PUBCOMP ([MQTT-4.5.0-2], [MQTT-4.6.0-2],
//!   [MQTT-4.6.0-3]). A QoS 2 message repeated before its PUBREL is not delivered twice.
//! - Flow control: QoS 1 and 2 messages wait for a free slot of the server's Receive Maximum
//!   ([MQTT-3.3.4-7]); everything else goes at once ([MQTT-3.3.4-8]).
//! - Limits the server announced in CONNACK: Maximum QoS, Retain Available, Maximum Packet Size
//!   and Topic Alias Maximum are checked before a packet is sent, and a packet that breaks one
//!   is refused with an error rather than sent.
//! - Keep Alive: PINGREQ after a Keep Alive with nothing else sent ([MQTT-3.1.2-20]), with the
//!   server's Server Keep Alive taking over when it sends one ([MQTT-3.1.2-21]).
//! - Topic Aliases from the server, up to the client's Topic Alias Maximum ([MQTT-3.3.2-10]).
//! - Sessions: [`Client::disconnect`] returns the client's half of the session, and
//!   [`ConnectOptions::resume`] carries it to the next connection, which resends what was in
//!   flight in its original order ([MQTT-4.6.0-1]).
//!
//! Every packet the server sends is available with [`ConnectOptions::packet_log`], for tests.
//!
//! # Transports
//!
//! [`Transport`] is the seam ADR 0002 reserves (docs/adr/0002-quic-only-tcp-seam-reserved.md):
//! it opens a connection and yields its control stream as ordered bytes each way.
//! [`QuicTransport`] is the implementation today. An MQTT 5 over TLS/TCP transport would be a
//! second implementation, and no other part of the API would change.
//!
//! # Not yet
//!
//! - Multi-stream mode (docs/spec/mqtt-over-quic.md, section 2.3). It will arrive as a handle
//!   on a data stream, opened from a [`Client`], whose publications and subscriptions travel on
//!   that stream, with an optional method on [`LinkHandle`] that opens one; transports that
//!   cannot open streams keep the default, which refuses.
//! - Enhanced authentication: a server that answers CONNECT with AUTH gets the connection
//!   closed ([`Error::EnhancedAuthentication`]).
//! - Reconnecting by itself: the application reconnects, with [`ConnectOptions::resume`].
//! - 0-RTT and connection migration (docs/spec/mqtt-over-quic.md, sections 4 and 5).

mod client;
mod driver;
mod error;
mod ids;
mod options;
mod quic;
mod session;
mod tls;
mod transport;

pub use client::{Client, CloseReason, Discard, Event, Events, Published};
pub use error::Error;
pub use options::{
    ConnectOptions, DEFAULT_CONNECT_TIMEOUT, DEFAULT_EVENT_CAPACITY, DEFAULT_KEEP_ALIVE,
};
pub use quic::{DEFAULT_IDLE_TIMEOUT, DEFAULT_PORT, QuicTransport};
pub use session::Session;
pub use tls::{ALPN, TlsConfig, TlsConfigBuilder};
pub use transport::{BoxFuture, CloseCode, Link, LinkHandle, Transport};

/// The packet types the API speaks, from `openqtt-codec`.
pub use openqtt_codec as codec;
pub use openqtt_codec::{
    ConnAck, Disconnect, DisconnectProperties, DisconnectReasonCode, Packet, PubAck, PubComp,
    PubRec, Publish, PublishProperties, QoS, RetainHandling, SubAck, SubscribeProperties,
    Subscription, SubscriptionOptions, UnsubAck, UnsubscribeProperties, Will, WillProperties,
};
/// rustls, whose configuration [`TlsConfig::from_rustls`] takes.
pub use rustls;
pub use rustls::pki_types::{CertificateDer, PrivateKeyDer};

#[cfg(test)]
mod tests;
