//! The client listener: MQTT 5 over QUIC.
//!
//! A [`Listener`] is made once from a listener's settings ([`ListenerConfig`], which reads a
//! `[listeners.quic.<name>]` section): its TLS and QUIC configuration, shared by its
//! [`Endpoint`]s, one UDP socket each, one per core when asked (report R7, D7). An endpoint
//! yields [`Accepting`] connection attempts, and each completes its handshake into a
//! [`QuicConnection`].
//!
//! [`QuicConnection`] implements [`MqttConnection`], the transport seam of
//! docs/adr/0002-quic-only-tcp-seam-reserved.md: packets in and out, each tagged with the stream
//! it travels on ([`StreamTag`]), the peer's address and verified certificate chain ([`Peer`]),
//! the end of the handshake and of each stream ([`Event`]), and a way to close with a reason
//! ([`CloseCode`]). Sessions never see a QUIC type. An MQTT 5 over TLS/TCP listener would be a
//! second implementation of the seam, with only the control stream.
//!
//! # What the transport holds clients to
//!
//! docs/spec/mqtt-over-quic.md, as far as the streams decide it:
//!
//! - TLS 1.3 with ALPN `mqtt` (section 1), on rustls with aws-lc-rs and the post-quantum key
//!   share first (R7, D6). A client certificate, when the listener asks for one, chains to the
//!   listener's CAs and nothing else (section 3, R2 rules 1 and 2).
//! - At most `max_streams` bidirectional streams at once, no unidirectional stream and no
//!   datagram (R7, D2). The first bidirectional stream is the control stream and begins with
//!   CONNECT; the others are data streams, held unread until the edge accepts the connection,
//!   and refused if it does not (sections 2.1 and 2.3).
//! - Each stream is decoded on its own, and a packet larger than the listener's Maximum Packet
//!   Size is refused from its fixed header on. A data stream carries packet types 3 to 11
//!   only, and no Topic Alias (sections 2.1 and 2.3).
//! - Nothing goes out before the handshake completes (section 4), and the QUIC idle timeout
//!   outlasts 1.5 times the longest Keep Alive (section 6).
//! - Connection IDs name the node and the endpoint, for a load balancer to route by (section
//!   5, [`CidRoute`]).
//!
//! # What it leaves to others
//!
//! It decides no identity: the verified chain goes up in [`Peer`], and the CN, the clientAuth
//! extended key usage, which rustls does not require (R7, F4), and issuer pins are
//! `openqtt-auth`'s, which the edge calls. What a packet means, which CONNACK or DISCONNECT
//! answers a broken rule, and when to accept data streams, finish one or close, are the
//! session's and the edge's: the transport reports, and does what it is told.
//!
//! It must not depend on `openqtt-session`, `openqtt-wire` or `openqtt-auth` (`make layers`).

mod cid;
mod config;
mod connection;
mod error;
mod listener;
mod seam;
mod tls;

pub use cid::{CID_LEN, CidRoute};
pub use config::{ClientAuth, DEFAULT_PORT, ListenerConfig, Resumption};
pub use connection::QuicConnection;
pub use error::{Closed, Error, Violation};
pub use listener::{Accepting, Endpoint, Listener};
pub use seam::{CloseCode, Event, MqttConnection, Peer, StreamEnd, StreamTag};
pub use tls::ALPN;
