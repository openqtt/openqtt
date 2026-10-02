//! The QUIC transport: one quinn endpoint and connection per client connection, and the first
//! bidirectional stream as the control stream (docs/spec/mqtt-over-quic.md, sections 1 and 2).

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::QuicClientConfig;
use quinn::{IdleTimeout, TransportConfig, VarInt};

use crate::transport::{BoxFuture, CloseCode, Link, LinkHandle, Transport};
use crate::{Error, TlsConfig};

/// The UDP port MQTT over QUIC listens on unless told otherwise, as EMQX's
/// (docs/spec/mqtt-over-quic.md, section 1).
pub const DEFAULT_PORT: u16 = 14567;

/// How long a QUIC connection may stay silent before either end drops it, unless
/// [`QuicTransport::idle_timeout`] says otherwise. It has to be longer than 1.5 times the
/// Keep Alive in use, so QUIC never ends a connection MQTT still considers alive
/// (docs/spec/mqtt-over-quic.md, section 6); this covers Keep Alive up to 400 seconds.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// MQTT 5 over QUIC (docs/spec/mqtt-over-quic.md) in single-stream mode: every packet travels
/// on the control stream, the first bidirectional stream the client opens.
#[derive(Debug, Clone)]
pub struct QuicTransport {
    server: SocketAddr,
    server_name: String,
    config: quinn::ClientConfig,
    idle_timeout: Duration,
    keep_alive_interval: Option<Duration>,
}

impl QuicTransport {
    /// Connects to `server`, whose certificate must name `server_name` and chain to a root of
    /// `tls`.
    ///
    /// # Errors
    ///
    /// [`Error::TlsForQuic`] when the TLS configuration cannot run QUIC, which needs TLS 1.3
    /// and its initial cipher suite.
    pub fn new(
        server: SocketAddr,
        server_name: impl Into<String>,
        tls: &TlsConfig,
    ) -> Result<Self, Error> {
        let crypto = QuicClientConfig::try_from(tls.rustls())
            .map_err(|error| Error::TlsForQuic(error.to_string()))?;
        Ok(Self {
            server,
            server_name: server_name.into(),
            config: quinn::ClientConfig::new(Arc::new(crypto)),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            keep_alive_interval: None,
        })
    }

    /// How long the connection may stay silent before QUIC drops it. Keep it above 1.5 times
    /// the Keep Alive (docs/spec/mqtt-over-quic.md, section 6).
    #[must_use]
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Sends QUIC PINGs at this interval, below the MQTT Keep Alive, to hold NAT bindings open
    /// (docs/spec/mqtt-over-quic.md, section 6). Off unless set.
    #[must_use]
    pub fn quic_keep_alive(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = Some(interval);
        self
    }

    /// The server this transport connects to.
    pub fn server(&self) -> SocketAddr {
        self.server
    }

    async fn open(&self) -> io::Result<Link> {
        let mut transport = TransportConfig::default();
        transport.max_idle_timeout(Some(
            IdleTimeout::try_from(self.idle_timeout).map_err(io::Error::other)?,
        ));
        transport.keep_alive_interval(self.keep_alive_interval);
        // MQTT over QUIC uses no datagrams (docs/spec/mqtt-over-quic.md, section 1), so none
        // are offered. EMQX 5.8.9 also drops any connection whose client offers them: its
        // connection process has no callback for msquic's datagram state event.
        transport.datagram_receive_buffer_size(None);
        // The server never opens streams (docs/spec/mqtt-over-quic.md, section 2.3).
        transport.max_concurrent_bidi_streams(VarInt::from_u32(0));
        transport.max_concurrent_uni_streams(VarInt::from_u32(0));
        let mut config = self.config.clone();
        config.transport_config(Arc::new(transport));

        let bind = if self.server.is_ipv6() {
            SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))
        } else {
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
        };
        let endpoint = quinn::Endpoint::client(bind)?;
        let connection = endpoint
            .connect_with(config, self.server, &self.server_name)
            .map_err(io::Error::other)?
            .await
            .map_err(io::Error::other)?;
        let (send, recv) = connection.open_bi().await.map_err(io::Error::other)?;
        Ok(Link::new(
            recv,
            send,
            QuicHandle {
                connection,
                endpoint,
            },
        ))
    }
}

impl Transport for QuicTransport {
    fn connect(&self) -> BoxFuture<'_, io::Result<Link>> {
        Box::pin(self.open())
    }
}

/// The connection and the endpoint it runs on, which lives as long as the connection.
struct QuicHandle {
    connection: quinn::Connection,
    endpoint: quinn::Endpoint,
}

impl LinkHandle for QuicHandle {
    fn close(&self, code: CloseCode) {
        self.connection.close(VarInt::from_u32(code.value()), b"");
    }

    fn closed(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.endpoint.wait_idle())
    }
}
