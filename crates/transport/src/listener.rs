//! The listener, its endpoints, and the connection attempts they accept.

use std::io;
use std::net::SocketAddr;
use std::num::{NonZeroU8, NonZeroU32};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use quinn::{TokioRuntime, VarInt};
use socket2::{Domain, Protocol, Socket, Type};

use crate::config::ListenerConfig;
use crate::connection::{PendingHandshake, QuicConnection};
use crate::{CloseCode, Closed, Error};

/// What a listener's endpoints and connections share.
pub(crate) struct Shared {
    pub(crate) name: Arc<str>,
    pub(crate) max_packet_size: NonZeroU32,
    pub(crate) send_backlog: usize,
    early_data: bool,
    handshake_timeout: Duration,
    endpoints: NonZeroU8,
    server: quinn::ServerConfig,
    config: ListenerConfig,
    /// The address the first endpoint bound, so that the others share its port when the
    /// configured one is 0.
    bound: Mutex<Option<SocketAddr>>,
}

/// A client listener: the name, address, TLS and QUIC settings its endpoints share.
///
/// The TLS configuration, and with it the keys that seal session tickets, is made once here, so
/// a client resumes on whichever endpoint of the listener receives it.
///
/// ```no_run
/// # async fn run(config: openqtt_transport::ListenerConfig) -> Result<(), openqtt_transport::Error> {
/// use openqtt_transport::{Listener, MqttConnection};
///
/// let listener = Listener::new(config)?;
/// let endpoint = listener.bind(0)?;
/// while let Some(accepting) = endpoint.accept().await {
///     tokio::spawn(async move {
///         if let Ok(mut connection) = accepting.establish().await {
///             while let Ok(_event) = connection.recv().await {
///                 // Hand the event to the session.
///             }
///         }
///     });
/// }
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct Listener {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Listener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Listener")
            .field("name", &self.shared.name)
            .field("config", &self.shared.config)
            .finish_non_exhaustive()
    }
}

impl Listener {
    /// A listener as `config` sets it. Nothing is bound yet.
    ///
    /// # Errors
    ///
    /// [`Error::Tls`] when rustls refuses the certificate or key, and [`Error::Setting`] for a
    /// setting that cannot work, such as several endpoints where `SO_REUSEPORT` does not exist.
    pub fn new(config: ListenerConfig) -> Result<Self, Error> {
        if config.send_backlog == 0 {
            return Err(Error::Setting {
                setting: "send_backlog".to_owned(),
                reason: "at least 1 byte: a connection with none would never read".to_owned(),
            });
        }
        if config.handshake_timeout.is_zero() || config.handshake_timeout > LONGEST_HANDSHAKE {
            return Err(Error::Setting {
                setting: "handshake_timeout".to_owned(),
                reason: "more than 0 and at most an hour".to_owned(),
            });
        }
        if config.endpoints.get() > 1 && !SHARES_PORTS {
            return Err(Error::Setting {
                setting: "endpoints".to_owned(),
                reason: "more than one needs SO_REUSEPORT, which this platform does not have"
                    .to_owned(),
            });
        }
        if config.endpoints.get() > 1 && !BALANCES_PORTS {
            tracing::warn!(
                listener = config.name(),
                endpoints = config.endpoints.get(),
                "SO_REUSEPORT on this platform delivers every datagram to one socket: one endpoint \
                 serves every client and the others stay idle"
            );
        }
        let server = config.server_config()?;
        Ok(Self {
            shared: Arc::new(Shared {
                name: Arc::clone(&config.name),
                max_packet_size: config.max_packet_size,
                send_backlog: config.send_backlog,
                early_data: config.early_data,
                handshake_timeout: config.handshake_timeout,
                endpoints: config.endpoints,
                server,
                config,
                bound: Mutex::new(None),
            }),
        })
    }

    /// The listener's name.
    pub fn name(&self) -> &str {
        &self.shared.name
    }

    /// How many endpoints the listener has, each bound with [`bind`](Self::bind).
    pub fn endpoints(&self) -> u8 {
        self.shared.endpoints.get()
    }

    /// Binds endpoint `index`, from 0 to [`endpoints`](Self::endpoints) less one: a UDP socket on
    /// the listener's address and a quinn endpoint on it. The tokio runtime of the caller drives
    /// it and every connection it accepts, so for one endpoint per core, bind each from the
    /// runtime of its core.
    ///
    /// # Errors
    ///
    /// [`Error::Setting`] for an index out of range, and [`Error::Bind`] when the socket cannot
    /// be set up.
    pub fn bind(&self, index: u8) -> Result<Endpoint, Error> {
        if index >= self.shared.endpoints.get() {
            return Err(Error::Setting {
                setting: "endpoints".to_owned(),
                reason: format!(
                    "endpoint {index} of a listener with {}",
                    self.shared.endpoints
                ),
            });
        }
        // The first endpoint chooses the port when the configured one is 0, and the others take
        // it. Choosing, binding and publishing happen under one lock, or endpoints bound at once
        // from their cores' runtimes would each choose a port of their own.
        let (socket, address) = {
            let mut bound = self
                .shared
                .bound
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let address = match *bound {
                Some(bound) if self.shared.config.address.port() == 0 => bound,
                _ => self.shared.config.address,
            };
            let socket = self
                .socket(address)
                .map_err(|source| Error::Bind { address, source })?;
            let local_address = socket
                .local_addr()
                .map_err(|source| Error::Bind { address, source })?;
            bound.get_or_insert(local_address);
            (socket, address)
        };
        let endpoint = quinn::Endpoint::new(
            self.shared.config.endpoint_config(index),
            Some(self.shared.server.clone()),
            socket,
            Arc::new(TokioRuntime),
        )
        .map_err(|source| Error::Bind { address, source })?;
        let local_address = endpoint
            .local_addr()
            .map_err(|source| Error::Bind { address, source })?;
        Ok(Endpoint {
            endpoint,
            shared: Arc::clone(&self.shared),
            index,
            local_address,
            accepting: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Binds every endpoint, from the caller's runtime.
    ///
    /// # Errors
    ///
    /// As [`bind`](Self::bind).
    pub fn bind_all(&self) -> Result<Vec<Endpoint>, Error> {
        (0..self.shared.endpoints.get())
            .map(|index| self.bind(index))
            .collect()
    }

    /// A UDP socket bound to `address`, sharing the port with the listener's other endpoints.
    fn socket(&self, address: SocketAddr) -> io::Result<std::net::UdpSocket> {
        let socket = Socket::new(
            Domain::for_address(address),
            Type::DGRAM,
            Some(Protocol::UDP),
        )?;
        if self.shared.endpoints.get() > 1 {
            share_port(&socket)?;
        }
        if let Some(bytes) = self.shared.config.socket_buffer {
            socket.set_recv_buffer_size(bytes)?;
            socket.set_send_buffer_size(bytes)?;
        }
        socket.bind(&address.into())?;
        Ok(socket.into())
    }
}

/// The longest handshake a listener allows: anything longer is a mistake, and an unbounded one
/// would overflow the deadline.
const LONGEST_HANDSHAKE: Duration = Duration::from_secs(3_600);

/// Whether this platform lets several sockets bind one UDP port.
const SHARES_PORTS: bool = cfg!(all(
    unix,
    not(any(target_os = "solaris", target_os = "illumos"))
));

/// Whether it also spreads datagrams across them, by a hash of the client's address and port.
const BALANCES_PORTS: bool = cfg!(any(target_os = "linux", target_os = "android"));

#[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
fn share_port(socket: &Socket) -> io::Result<()> {
    socket.set_reuse_port(true)
}

#[cfg(not(all(unix, not(any(target_os = "solaris", target_os = "illumos")))))]
fn share_port(_: &Socket) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SO_REUSEPORT is not available on this platform",
    ))
}

/// One endpoint of a listener: a UDP socket and the quinn endpoint on it, which accepts
/// connection attempts.
#[derive(Clone)]
pub struct Endpoint {
    endpoint: quinn::Endpoint,
    shared: Arc<Shared>,
    index: u8,
    local_address: SocketAddr,
    /// Cleared by [`stop_accepting`](Self::stop_accepting), for every clone.
    accepting: Arc<AtomicBool>,
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Endpoint")
            .field("listener", &self.shared.name)
            .field("index", &self.index)
            .field("local_address", &self.local_address)
            .finish_non_exhaustive()
    }
}

impl Endpoint {
    /// The address the endpoint is bound to.
    pub fn local_address(&self) -> SocketAddr {
        self.local_address
    }

    /// The endpoint's index on its listener, which its connection IDs carry.
    pub fn index(&self) -> u8 {
        self.index
    }

    /// The next connection attempt, before its handshake, or `None` once the endpoint is
    /// closed. Complete each with [`Accepting::establish`] on a task of its own, so that a
    /// slow handshake holds up no other.
    ///
    /// After [`stop_accepting`](Self::stop_accepting) it refuses each attempt as it comes, and
    /// returns only once the endpoint is closed, so keep calling it while draining.
    pub async fn accept(&self) -> Option<Accepting> {
        loop {
            let incoming = self.endpoint.accept().await?;
            if self.accepting.load(Ordering::Acquire) {
                return Some(Accepting {
                    incoming,
                    shared: Arc::clone(&self.shared),
                });
            }
            incoming.refuse();
        }
    }

    /// Refuses new connections, with QUIC's CONNECTION_REFUSED so that a client tries elsewhere
    /// at once, and keeps the open ones: the start of a drain, before each client is sent
    /// DISCONNECT 0x9C (report R3).
    pub fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    /// Closes every connection of the endpoint at once with `code`, and the endpoint.
    pub fn close(&self, code: CloseCode) {
        self.endpoint.close(VarInt::from_u32(code.value()), b"");
    }

    /// Completes once every connection of the endpoint is closed and its peer told.
    pub async fn wait_idle(&self) {
        self.endpoint.wait_idle().await;
    }

    /// How many connections the endpoint holds, handshakes included.
    pub fn open_connections(&self) -> usize {
        self.endpoint.open_connections()
    }
}

/// A connection attempt, before its handshake.
pub struct Accepting {
    incoming: quinn::Incoming,
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Accepting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Accepting")
            .field("listener", &self.shared.name)
            .field("remote_address", &self.incoming.remote_address())
            .finish_non_exhaustive()
    }
}

impl Accepting {
    /// The client's address.
    pub fn remote_address(&self) -> SocketAddr {
        self.incoming.remote_address()
    }

    /// Refuses the attempt before its handshake, as an edge at capacity does.
    pub fn refuse(self) {
        self.incoming.refuse();
    }

    /// Completes the handshake: TLS 1.3 with ALPN `mqtt`, and the client's certificate checked
    /// as the listener requires, within the listener's handshake timeout.
    ///
    /// With 0-RTT on, the connection comes as soon as the client opens a stream in 0-RTT data,
    /// and [`Event::HandshakeComplete`](crate::Event::HandshakeComplete) follows once the
    /// handshake confirms it, or the connection closes when the timeout runs out first;
    /// otherwise it comes once the handshake is complete.
    ///
    /// # Errors
    ///
    /// [`Error::Handshake`] when the handshake fails: a client certificate missing or not
    /// issued by the listener's CAs, no ALPN `mqtt`, or, with [`Closed::TimedOut`], a client too
    /// slow to complete it.
    pub async fn establish(self) -> Result<QuicConnection, Error> {
        let deadline = tokio::time::Instant::now() + self.shared.handshake_timeout;
        // On the heap, so that a task that awaits this and then serves the connection is not
        // sized for the handshake for the rest of the connection's life. Dropping the attempt
        // when time runs out closes it.
        Box::pin(tokio::time::timeout_at(deadline, self.handshake(deadline)))
            .await
            .unwrap_or(Err(Error::Handshake(Closed::TimedOut)))
    }

    async fn handshake(self, deadline: tokio::time::Instant) -> Result<QuicConnection, Error> {
        let failed = |error: quinn::ConnectionError| Error::Handshake(Closed::of(&error));
        let connecting = self.incoming.accept().map_err(failed)?;
        if !self.shared.early_data {
            let connection = connecting.await.map_err(failed)?;
            return Ok(QuicConnection::new(
                connection,
                &self.shared,
                None,
                None,
                false,
            ));
        }
        // A server's attempt always converts: it may then read 0-RTT data.
        let (connection, mut handshake) = match connecting.into_0rtt() {
            Ok(converted) => converted,
            Err(connecting) => {
                let connection = connecting.await.map_err(failed)?;
                return Ok(QuicConnection::new(
                    connection,
                    &self.shared,
                    None,
                    None,
                    false,
                ));
            }
        };
        // Whichever comes first: the end of the handshake, or a stream the client opened, in
        // 0-RTT data if the handshake is still going on.
        let first = tokio::select! {
            biased;
            _ = &mut handshake => None,
            stream = connection.accept_bi() => Some(stream),
        };
        let Some(stream) = first else {
            return match connection.close_reason() {
                Some(error) => Err(failed(error)),
                None => Ok(QuicConnection::new(
                    connection,
                    &self.shared,
                    None,
                    None,
                    false,
                )),
            };
        };
        let (send, recv) = stream.map_err(failed)?;
        // A stream taken while the handshake goes on came in 0-RTT data. One taken after it
        // came with a handshake that completed, or with a close, which quinn tells apart only
        // once the handshake future ends: the connection judges that, so that a close is never
        // taken for a completed handshake.
        let early_data = recv.is_0rtt();
        // The rest of the handshake keeps the deadline of the whole.
        let rest: PendingHandshake =
            Box::pin(async move { tokio::time::timeout_at(deadline, handshake).await.is_ok() });
        Ok(QuicConnection::new(
            connection,
            &self.shared,
            Some(rest),
            Some((send, recv)),
            early_data,
        ))
    }
}
