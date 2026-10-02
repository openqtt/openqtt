//! The transport seam of docs/adr/0002-quic-only-tcp-seam-reserved.md: what the edge needs from a
//! client connection, whatever carries it.

use std::fmt;
use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use openqtt_codec::Packet;
use openqtt_ext::Certificate;

use crate::Error;

/// The stream of a connection a packet travels on (docs/spec/mqtt-over-quic.md, section 2).
///
/// A transport without streams, such as MQTT 5 over TLS/TCP, has only
/// [`Control`](Self::Control).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StreamTag {
    /// The control stream: the first bidirectional stream the client opens. It begins with
    /// CONNECT, and carries every packet in single-stream mode (sections 2.1 and 2.2).
    Control,
    /// A data stream (section 2.3), numbered by the order in which the client opened its
    /// bidirectional streams: its QUIC stream index. The control stream is the first, so the
    /// first data stream is `Data(1)`, and a number is never used twice on a connection. 64 bits
    /// wide, since a client may open a stream for every exchange of a connection that lasts for
    /// months.
    Data(u64),
}

impl fmt::Display for StreamTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control => f.write_str("the control stream"),
            Self::Data(number) => write!(f, "data stream {number}"),
        }
    }
}

/// How one side of a stream ended (docs/spec/mqtt-over-quic.md, section 2.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StreamEnd {
    /// The client finished its side: every packet it sent on the stream has been delivered.
    Finished,
    /// The client reset its side with this application error code. A packet it had not finished
    /// sending is lost.
    Reset(u64),
    /// The client stopped the server's side with this application error code (STOP_SENDING):
    /// nothing sent on the stream reaches it any more, and what it had not acknowledged may be
    /// lost. Its own side may still carry packets until it ends.
    Stopped(u64),
}

/// What a connection yields, in the order it happened.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// The handshake is complete, so nothing the client sends from now on can be a replay. It
    /// comes once, first unless 0-RTT data came before it, and nothing the server queues leaves
    /// before the handshake completes: CONNACK must not be sent earlier
    /// (docs/spec/mqtt-over-quic.md, section 4).
    ///
    /// Packets delivered before it arrived in 0-RTT data, which an attacker can replay, so the
    /// session acts on no PUBLISH, SUBSCRIBE or UNSUBSCRIBE among them until it comes, and on
    /// none at all if the connection closes first. Whatever is delivered after it is confirmed,
    /// whenever it arrived.
    HandshakeComplete {
        /// Whether the client's 0-RTT data was accepted, so that packets came before this.
        early_data: bool,
    },
    /// A packet the client sent, decoded and held to what its stream may carry.
    Packet {
        /// The stream it travelled on.
        stream: StreamTag,
        /// The packet.
        packet: Packet,
    },
    /// One side of a stream ended. Each side ends once: the client's with
    /// [`Finished`](StreamEnd::Finished) or [`Reset`](StreamEnd::Reset), after which no packet
    /// arrives on the stream, and the server's with [`Stopped`](StreamEnd::Stopped), after which
    /// nothing can be sent on it. Either end of the control stream ends the MQTT connection
    /// (section 2.1); section 2.4 says what follows the end of a data stream, which the session
    /// then [`finish`](MqttConnection::finish)es or [`reset`](MqttConnection::reset)s so that the
    /// connection lets it go.
    StreamEnded {
        /// The stream.
        stream: StreamTag,
        /// Which side ended, and how.
        end: StreamEnd,
    },
    /// What waits to be sent fell below the connection's limit after reaching it: the
    /// connection takes more packets again, and reading resumes if the backlog alone had
    /// stopped it.
    Writable,
}

/// The client at the other end of a connection, as the handshake established it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Peer {
    /// The address the client connected from, its own: QUIC carries no proxy header, and a
    /// passthrough load balancer leaves the address as it is (R2 rule 27).
    pub address: SocketAddr,
    /// The certificate chain the client presented, leaf first, verified against the listener's
    /// client CAs and nothing else; empty when it presented none. On a resumed TLS session it
    /// is the chain verified when the session began (docs/spec/mqtt-over-quic.md, section 3).
    /// Who the chain names, and whether its leaf carries the clientAuth extended key usage, is
    /// for the authenticator to decide (`openqtt-auth`).
    pub certificates: Arc<[Certificate]>,
    /// The application protocol the handshake settled on: `mqtt`.
    pub alpn: Option<Bytes>,
    /// Whether the client's 0-RTT data was accepted.
    pub early_data: bool,
    /// The name of the listener the client connected to, as configured.
    pub listener: Arc<str>,
}

impl Peer {
    /// A client at `address` on `listener`, with no certificate, no application protocol and no
    /// early data: set those with the `with_` methods.
    pub fn new(address: SocketAddr, listener: &str) -> Self {
        Self {
            address,
            certificates: Arc::from(Vec::new()),
            alpn: None,
            early_data: false,
            listener: Arc::from(listener),
        }
    }

    /// With the verified chain `certificates`, leaf first.
    #[must_use]
    pub fn with_certificates(mut self, certificates: Arc<[Certificate]>) -> Self {
        self.certificates = certificates;
        self
    }

    /// With the application protocol `alpn`.
    #[must_use]
    pub fn with_alpn(mut self, alpn: Bytes) -> Self {
        self.alpn = Some(alpn);
        self
    }

    /// With 0-RTT data accepted or not.
    #[must_use]
    pub fn with_early_data(mut self, early_data: bool) -> Self {
        self.early_data = early_data;
        self
    }
}

/// The QUIC application error codes of docs/spec/mqtt-over-quic.md, section 8, which close a
/// connection or reset a stream. A client that does not know them reads any code but 0 as an
/// abnormal close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CloseCode {
    /// 0x0: no error, after DISCONNECT.
    NoError,
    /// 0x1: a protocol error, as MQTT's 0x82.
    ProtocolError,
    /// 0x2: an internal error.
    InternalError,
    /// 0x3: a data stream opened before the connection was accepted, then refused.
    StreamRefused,
}

impl CloseCode {
    /// The code on the wire.
    pub const fn value(self) -> u32 {
        match self {
            Self::NoError => 0x0,
            Self::ProtocolError => 0x1,
            Self::InternalError => 0x2,
            Self::StreamRefused => 0x3,
        }
    }

    /// The code `value` names, if the specification defines it.
    pub const fn from_value(value: u64) -> Option<Self> {
        match value {
            0x0 => Some(Self::NoError),
            0x1 => Some(Self::ProtocolError),
            0x2 => Some(Self::InternalError),
            0x3 => Some(Self::StreamRefused),
            _ => None,
        }
    }
}

/// One client connection, whatever carries it: the seam of
/// docs/adr/0002-quic-only-tcp-seam-reserved.md.
///
/// The edge drives a connection from one task. It awaits [`recv`](Self::recv) beside its other
/// inputs, hands each [`Event`] to the session, and carries out the session's effects with the
/// other methods. Nothing here blocks: [`send`](Self::send) queues a packet, and what is queued
/// goes out while `recv` or [`flush`](Self::flush) is polled, as fast as flow control allows.
///
/// What waits to be sent is bounded. Once it reaches the connection's limit,
/// [`is_writable`](Self::is_writable) turns false, the connection stops reading from the client
/// until it falls back below the limit, and [`Event::Writable`] says when it has. The edge takes
/// no further deliveries for the client meanwhile. So a client that does not read cannot make
/// the server queue without bound: neither the acknowledgements its packets call for nor the
/// messages sent to it.
///
/// An implementation for QUIC is [`QuicConnection`](crate::QuicConnection). One for MQTT 5 over
/// TLS/TCP would yield only [`StreamTag::Control`], never accept 0-RTT, and change nothing for
/// the session.
pub trait MqttConnection: Send {
    /// The client, as the handshake established it.
    fn peer(&self) -> &Peer;

    /// Polls for the next [`Event`], sending what is queued meanwhile. See
    /// [`recv`](Self::recv).
    ///
    /// # Errors
    ///
    /// As [`recv`](Self::recv).
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<Event, Error>>;

    /// Queues `packet` on `stream`, encoded at once. It goes out after everything queued on that
    /// stream before it, and never before the handshake is complete.
    ///
    /// The caller holds the packet to the client's Maximum Packet Size and Topic Alias Maximum;
    /// the connection holds it to what `stream` may carry.
    ///
    /// # Errors
    ///
    /// [`Error::Encode`] for a packet the codec refuses, [`Error::WrongStream`] for a packet
    /// `stream` may not carry, [`Error::StreamClosed`] when the server's side of `stream` is not
    /// open, and [`Error::Closed`] once the connection is.
    fn send(&mut self, stream: StreamTag, packet: &Packet) -> Result<(), Error>;

    /// Queues `bytes` on `stream` as they are: for the refusal of an older protocol's CONNECT
    /// (report R1, D1), which no MQTT 5 packet can express.
    ///
    /// # Errors
    ///
    /// [`Error::StreamClosed`] when the server's side of `stream` is not open, and
    /// [`Error::Closed`] once the connection is.
    fn send_bytes(&mut self, stream: StreamTag, bytes: &[u8]) -> Result<(), Error>;

    /// Whether what waits to be sent is below the connection's limit, so that it takes more.
    fn is_writable(&self) -> bool;

    /// Polls until everything queued has been handed to the transport. See
    /// [`flush`](Self::flush).
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] when the connection closed first.
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>>;

    /// Ends the server's side of `stream` once what is queued on it has been sent (section 2.4).
    ///
    /// # Errors
    ///
    /// [`Error::StreamClosed`] when that side is not open, and [`Error::Closed`] once the
    /// connection is.
    fn finish(&mut self, stream: StreamTag) -> Result<(), Error>;

    /// Abandons both sides of `stream` with `code`: what is queued on it is dropped, and the
    /// client is told to stop sending.
    ///
    /// # Errors
    ///
    /// [`Error::StreamClosed`] for a stream the connection does not have, and [`Error::Closed`]
    /// once the connection is.
    fn reset(&mut self, stream: StreamTag, code: CloseCode) -> Result<(), Error>;

    /// Reads the data streams the client opened so far, and those it opens later, which until
    /// now were held unread (section 2.3). The edge calls it once the CONNACK accepting the
    /// connection is queued. Calling it again does nothing.
    fn accept_data_streams(&mut self);

    /// Stops reading from the client, so that flow control holds it back: the session's
    /// PauseReading. Packets already read are still delivered, and so is the end of the
    /// connection.
    fn pause_reading(&mut self);

    /// Reads from the client again: the session's ResumeReading.
    fn resume_reading(&mut self);

    /// Ends the server's side of every stream once what is queued on it has been sent, and
    /// refuses the data streams never accepted ([`CloseCode::StreamRefused`], section 2.3). The
    /// first step of [`shutdown`](Self::shutdown).
    fn finish_all(&mut self);

    /// Polls until the client has acknowledged everything sent on the streams whose server side
    /// is finished, or stopped them, or the connection closed.
    fn poll_delivered(&mut self, cx: &mut Context<'_>) -> Poll<()>;

    /// Closes the connection at once with `code`. What the client has not acknowledged is lost,
    /// so after a CONNACK or DISCONNECT use [`shutdown`](Self::shutdown) instead.
    fn close(&mut self, code: CloseCode);

    /// The next [`Event`], sending what is queued meanwhile.
    ///
    /// Cancel safe: everything read is kept in the connection, so `recv` can be dropped in a
    /// `select!` and called again without losing anything.
    ///
    /// # Errors
    ///
    /// [`Error::Decode`] and [`Error::Violation`] when the client broke the protocol on a
    /// stream: the session answers with the CONNACK or DISCONNECT the error calls for, and
    /// closes. The connection reads nothing more after one, but what is queued still goes out.
    /// [`Error::Closed`] once the connection is closed, then on every later call.
    fn recv(&mut self) -> impl Future<Output = Result<Event, Error>> + Send
    where
        Self: Sized,
    {
        poll_fn(move |cx| self.poll_recv(cx))
    }

    /// Completes once everything queued has been handed to the transport, which sends it as flow
    /// control allows. Cancel safe.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] when the connection closed first.
    fn flush(&mut self) -> impl Future<Output = Result<(), Error>> + Send
    where
        Self: Sized,
    {
        poll_fn(move |cx| self.poll_flush(cx))
    }

    /// Closes the connection with `code` once the client has acknowledged what was sent, or
    /// `linger` has passed: closing a QUIC connection discards what is not yet acknowledged, so
    /// a final CONNACK or DISCONNECT needs the wait (report R1, MQTT-3.2.2-7). The server's side
    /// of every stream is finished first, and data streams never accepted are refused.
    fn shutdown(&mut self, code: CloseCode, linger: Duration) -> impl Future<Output = ()> + Send
    where
        Self: Sized,
    {
        async move {
            self.finish_all();
            {
                let delivered = async {
                    if self.flush().await.is_ok() {
                        poll_fn(|cx| self.poll_delivered(cx)).await;
                    }
                };
                // Running out of time is the expected way for a client that went silent.
                drop(tokio::time::timeout(linger, delivered).await);
            }
            self.close(code);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streams_are_named_for_messages() {
        assert_eq!(StreamTag::Control.to_string(), "the control stream");
        assert_eq!(StreamTag::Data(3).to_string(), "data stream 3");
        assert!(StreamTag::Control < StreamTag::Data(1));
    }

    #[test]
    fn close_codes_are_those_of_section_8() {
        for (code, value) in [
            (CloseCode::NoError, 0),
            (CloseCode::ProtocolError, 1),
            (CloseCode::InternalError, 2),
            (CloseCode::StreamRefused, 3),
        ] {
            assert_eq!(code.value(), value);
            assert_eq!(CloseCode::from_value(u64::from(value)), Some(code));
        }
        assert_eq!(CloseCode::from_value(4), None);
    }

    #[test]
    fn a_peer_starts_with_nothing_but_its_address() {
        let address = SocketAddr::from(([192, 0, 2, 7], 50_000));
        let peer = Peer::new(address, "devices");
        assert_eq!(peer.address, address);
        assert!(peer.certificates.is_empty());
        assert_eq!(peer.alpn, None);
        assert!(!peer.early_data);
        assert_eq!(&*peer.listener, "devices");
        let peer = peer
            .with_alpn(Bytes::from_static(b"mqtt"))
            .with_early_data(true)
            .with_certificates(Arc::from(vec![Certificate::from_der(vec![0x30, 0x00])]));
        assert_eq!(peer.alpn.as_deref(), Some(&b"mqtt"[..]));
        assert!(peer.early_data);
        assert_eq!(peer.certificates.len(), 1);
    }
}
