//! The client listener: MQTT 5 over QUIC.
//!
//! It yields [`MqttConnection`], the transport seam of
//! docs/adr/0002-quic-only-tcp-seam-reserved.md: packets in and out, each tagged with the stream
//! it travels on ([`StreamTag`]), the peer's address and verified certificate chain ([`Peer`]),
//! the end of the handshake and of each stream ([`Event`]), and a way to close with a reason
//! ([`CloseCode`]). Sessions never see a QUIC type. An MQTT 5 over TLS/TCP listener would be a
//! second implementation of the seam, with only the control stream.
//!
//! It must not depend on `openqtt-session`, `openqtt-wire` or `openqtt-auth` (`make layers`).

mod error;
mod seam;

pub use error::{Closed, Error, Violation};
pub use seam::{CloseCode, Event, MqttConnection, Peer, StreamEnd, StreamTag};
