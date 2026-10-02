//! The transport seam: how the client reaches a server, independent of QUIC.

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;

use tokio::io::{AsyncRead, AsyncWrite};

/// A boxed future, so the transport traits can be used as trait objects.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A way to reach a server.
///
/// Each call to [`connect`](Self::connect) opens one connection and returns its control
/// stream: ordered, reliable bytes each way, which carry every packet in single-stream mode
/// (docs/spec/mqtt-over-quic.md, section 2.2). [`QuicTransport`](crate::QuicTransport) is the
/// implementation for QUIC. An MQTT 5 over TLS/TCP transport, should one be built
/// (docs/adr/0002-quic-only-tcp-seam-reserved.md), implements this trait and nothing about
/// [`Client`](crate::Client) changes.
pub trait Transport: Send + Sync {
    /// Opens a connection to the server.
    fn connect(&self) -> BoxFuture<'_, io::Result<Link>>;
}

/// One open connection: the two directions of its control stream, and a handle on the whole
/// connection.
pub struct Link {
    pub(crate) reader: Box<dyn AsyncRead + Send + Unpin>,
    pub(crate) writer: Box<dyn AsyncWrite + Send + Unpin>,
    pub(crate) handle: Box<dyn LinkHandle>,
}

impl Link {
    /// A link reading from `reader`, writing to `writer`, and closed through `handle`.
    pub fn new(
        reader: impl AsyncRead + Send + Unpin + 'static,
        writer: impl AsyncWrite + Send + Unpin + 'static,
        handle: impl LinkHandle + 'static,
    ) -> Self {
        Self {
            reader: Box::new(reader),
            writer: Box::new(writer),
            handle: Box::new(handle),
        }
    }
}

impl fmt::Debug for Link {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Link").finish_non_exhaustive()
    }
}

/// What the client needs from a connection besides its control stream.
pub trait LinkHandle: Send + Sync {
    /// Closes the connection at once. Data not yet delivered may be lost, so the client waits
    /// for its DISCONNECT to arrive, or for a short linger, before it calls this.
    fn close(&self, code: CloseCode);

    /// Completes once the connection is closed and the peer has been told, as far as the
    /// transport can tell. The client bounds the wait.
    fn closed(&self) -> BoxFuture<'_, ()>;
}

/// How a connection ends, as the transport says it to the peer: the QUIC application error
/// codes of docs/spec/mqtt-over-quic.md, section 8.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum CloseCode {
    /// 0x0: no error, after DISCONNECT.
    NoError,
    /// 0x1: protocol error, as MQTT's 0x82.
    ProtocolError,
    /// 0x2: internal error, such as a peer that stopped answering.
    InternalError,
}

impl CloseCode {
    /// The code on the wire.
    pub const fn value(self) -> u32 {
        match self {
            Self::NoError => 0x0,
            Self::ProtocolError => 0x1,
            Self::InternalError => 0x2,
        }
    }
}
