//! A listener's settings, and the QUIC configuration made from them.

use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};
use std::num::{NonZeroU8, NonZeroU32, NonZeroUsize};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openqtt_config::{Limits, QuicListener};
use openqtt_core::NodeId;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::{IdleTimeout, MtuDiscoveryConfig, TransportConfig, VarInt};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use crate::Error;
use crate::cid::NodeConnectionIds;

/// The UDP port MQTT over QUIC listens on unless told otherwise, as EMQX's
/// (docs/spec/mqtt-over-quic.md, section 1).
pub const DEFAULT_PORT: u16 = 14567;

/// The largest packet a client may send by default: 1 MiB, the Maximum Packet Size of report R1
/// (O5).
const DEFAULT_MAX_PACKET_SIZE: NonZeroU32 = match NonZeroU32::new(1 << 20) {
    Some(size) => size,
    None => NonZeroU32::MIN,
};

/// How much longer than 1.5 times the longest Keep Alive the QUIC idle timeout is. The session
/// closes a silent client at 1.5 times its Keep Alive, within a second (R2 rule 24), so QUIC
/// never closes a connection MQTT still considers alive (docs/spec/mqtt-over-quic.md, section
/// 6), and is there only for a session that failed to.
const IDLE_MARGIN: Duration = Duration::from_secs(10);

/// Which clients must present a certificate, and the CAs it must chain to.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum ClientAuth {
    /// No certificate is asked for.
    #[default]
    None,
    /// A client may present a certificate, which must then chain to one of these CAs; one that
    /// presents none connects without.
    Optional(Vec<CertificateDer<'static>>),
    /// Every client must present a certificate chaining to one of these CAs (R2 rule 1). The
    /// handshake of one that presents none fails with TLS alert 116, certificate required.
    Required(Vec<CertificateDer<'static>>),
}

/// How clients resume TLS sessions (report R7, D5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Resumption {
    /// Every handshake is a full one.
    Off,
    /// Stateless tickets: nothing is stored per client. The keys that seal them are random to
    /// the process and rotate every six hours, and a ticket is good for twelve. rustls accepts
    /// no 0-RTT data with them (R7, F2).
    Tickets,
    /// rustls's session cache, holding this many sessions, each usable once. What 0-RTT needs
    /// on rustls (R7, F2); every resumption searches the cache, which slows as it fills.
    SessionCache(NonZeroUsize),
}

/// A listener's settings: its address and endpoints, TLS, the QUIC transport settings of report
/// R7 (D2), and the limits its connections enforce.
///
/// [`new`](Self::new) starts from the defaults of docs/spec/config.md, and
/// [`from_settings`](Self::from_settings) reads a `[listeners.quic.<name>]` section.
pub struct ListenerConfig {
    pub(crate) name: Arc<str>,
    pub(crate) address: SocketAddr,
    pub(crate) endpoints: NonZeroU8,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    pub(crate) client_auth: ClientAuth,
    max_streams: u32,
    stream_window: u64,
    connection_window: u64,
    send_window: u64,
    pub(crate) send_backlog: usize,
    mtu_discovery: bool,
    pub(crate) resumption: Resumption,
    pub(crate) early_data: bool,
    keep_alive_max: Duration,
    pub(crate) max_packet_size: NonZeroU32,
    node: Option<NodeId>,
    pub(crate) socket_buffer: Option<usize>,
}

impl fmt::Debug for ListenerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListenerConfig")
            .field("name", &self.name)
            .field("address", &self.address)
            .field("endpoints", &self.endpoints)
            .field("chain", &self.chain.len())
            .field("client_auth", &self.client_auth)
            .field("max_streams", &self.max_streams)
            .field("stream_window", &self.stream_window)
            .field("connection_window", &self.connection_window)
            .field("send_window", &self.send_window)
            .field("send_backlog", &self.send_backlog)
            .field("mtu_discovery", &self.mtu_discovery)
            .field("resumption", &self.resumption)
            .field("early_data", &self.early_data)
            .field("keep_alive_max", &self.keep_alive_max)
            .field("max_packet_size", &self.max_packet_size)
            .field("node", &self.node)
            .field("socket_buffer", &self.socket_buffer)
            .finish_non_exhaustive()
    }
}

impl Clone for ListenerConfig {
    fn clone(&self) -> Self {
        Self {
            name: Arc::clone(&self.name),
            chain: self.chain.clone(),
            key: self.key.clone_key(),
            client_auth: self.client_auth.clone(),
            ..*self
        }
    }
}

impl ListenerConfig {
    /// The listener `name`, presenting the certificate `chain`, leaf first, with its private
    /// `key`, and otherwise the defaults of docs/spec/config.md and report R7: UDP 14567 on
    /// every address, one endpoint, no client certificate, 8 bidirectional streams and no
    /// unidirectional ones, 1 MiB windows, MTU discovery, stateless tickets, no 0-RTT, a Keep
    /// Alive of at most 20 minutes and packets of at most 1 MiB.
    pub fn new(
        name: &str,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Self {
        Self {
            name: Arc::from(name),
            address: SocketAddr::from((Ipv4Addr::UNSPECIFIED, DEFAULT_PORT)),
            endpoints: NonZeroU8::MIN,
            chain,
            key,
            client_auth: ClientAuth::None,
            max_streams: 8,
            stream_window: 1 << 20,
            connection_window: 1 << 20,
            send_window: 1 << 20,
            send_backlog: 64 * 1024,
            mtu_discovery: true,
            resumption: Resumption::Tickets,
            early_data: false,
            keep_alive_max: Duration::from_secs(1_200),
            max_packet_size: DEFAULT_MAX_PACKET_SIZE,
            node: None,
            socket_buffer: None,
        }
    }

    /// The listener `name` as its `[listeners.quic.<name>]` section and the `[limits]` section
    /// set it. The certificate, key and client CA files are read here.
    ///
    /// | Setting | What it sets |
    /// | --- | --- |
    /// | `bind` | [`address`](Self::address) |
    /// | `cert_file`, `key_file` | the certificate chain and private key, PEM |
    /// | `client_ca_file`, `require_client_cert` | [`ClientAuth::Required`] with both, [`ClientAuth::Optional`] with the file alone, [`ClientAuth::None`] without it |
    /// | `max_streams` | [`max_streams`](Self::max_streams) |
    /// | `stream_window`, `connection_window` | [`stream_window`](Self::stream_window), [`connection_window`](Self::connection_window) |
    /// | `mtu_discovery` | [`mtu_discovery`](Self::mtu_discovery) |
    /// | `session_tickets` | [`Resumption::Tickets`] or [`Resumption::Off`] |
    /// | `limits.keep_alive_max` | [`keep_alive_max`](Self::keep_alive_max), and so the idle timeout |
    /// | `limits.maximum_packet_size` | [`max_packet_size`](Self::max_packet_size) |
    ///
    /// `identity_from_cn`, `enable_authn` and `mountpoint` are the edge's: they decide who a
    /// client is and what it may do, not how it connects.
    ///
    /// # Errors
    ///
    /// [`Error::Setting`] when the certificate or key is not set, or a client certificate is
    /// required without a CA to check it against, and [`Error::File`] for a file that cannot be
    /// read or holds no certificate or key.
    pub fn from_settings(
        name: &str,
        listener: &QuicListener,
        limits: &Limits,
    ) -> Result<Self, Error> {
        let key = |setting: &str| format!("listeners.quic.{name}.{setting}");
        let cert_file = listener.cert_file.as_ref().ok_or_else(|| Error::Setting {
            setting: key("cert_file"),
            reason: "required: a QUIC listener needs a certificate".to_owned(),
        })?;
        let key_file = listener.key_file.as_ref().ok_or_else(|| Error::Setting {
            setting: key("key_file"),
            reason: "required: a QUIC listener needs a private key".to_owned(),
        })?;
        let chain = certificates(&key("cert_file"), cert_file)?;
        let private_key = private_key(&key("key_file"), key_file.path(), || key_file.read())?;
        let client_auth = match (&listener.client_ca_file, listener.require_client_cert) {
            (Some(file), true) => ClientAuth::Required(certificates(&key("client_ca_file"), file)?),
            (Some(file), false) => {
                ClientAuth::Optional(certificates(&key("client_ca_file"), file)?)
            }
            (None, false) => ClientAuth::None,
            (None, true) => {
                return Err(Error::Setting {
                    setting: key("client_ca_file"),
                    reason: "required with require_client_cert: it names the CA client \
                             certificates must chain to"
                        .to_owned(),
                });
            }
        };
        let max_packet_size = u32::try_from(limits.maximum_packet_size.bytes())
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| Error::Setting {
                setting: "limits.maximum_packet_size".to_owned(),
                reason: "from 1 byte to 4GiB".to_owned(),
            })?;
        Ok(Self::new(name, chain, private_key)
            .address(listener.bind)
            .client_auth(client_auth)
            .max_streams(listener.max_streams)
            .stream_window(listener.stream_window.bytes())
            .connection_window(listener.connection_window.bytes())
            .mtu_discovery(listener.mtu_discovery)
            .resumption(if listener.session_tickets {
                Resumption::Tickets
            } else {
                Resumption::Off
            })
            .keep_alive_max(limits.keep_alive_max.as_std())
            .max_packet_size(max_packet_size))
    }

    /// The UDP address to listen on.
    #[must_use]
    pub fn address(mut self, address: SocketAddr) -> Self {
        self.address = address;
        self
    }

    /// How many endpoints to bind to the address, each a UDP socket and a quinn endpoint of its
    /// own: one per core is the layout report R7 settled on (D7). Above 1 they share the port
    /// with `SO_REUSEPORT`. Linux then spreads clients across them by a hash of their addresses;
    /// macOS and the BSDs deliver every datagram to one of them, so the others stay idle there.
    /// Windows has no `SO_REUSEPORT`, and refuses more than one.
    #[must_use]
    pub fn endpoints(mut self, endpoints: NonZeroU8) -> Self {
        self.endpoints = endpoints;
        self
    }

    /// Which clients must present a certificate, and the CAs it must chain to.
    #[must_use]
    pub fn client_auth(mut self, client_auth: ClientAuth) -> Self {
        self.client_auth = client_auth;
        self
    }

    /// The bidirectional streams a client may have open at once, the control stream included:
    /// the control stream and seven data streams by default (R7, D2). quinn keeps state for
    /// each from the moment a connection starts (R7, F1). A client may open no unidirectional
    /// stream.
    #[must_use]
    pub fn max_streams(mut self, max_streams: u32) -> Self {
        self.max_streams = max_streams;
        self
    }

    /// The receive window of each stream, in bytes (R7, D2).
    #[must_use]
    pub fn stream_window(mut self, bytes: u64) -> Self {
        self.stream_window = bytes;
        self
    }

    /// The receive window of the whole connection, in bytes (R7, D2).
    #[must_use]
    pub fn connection_window(mut self, bytes: u64) -> Self {
        self.connection_window = bytes;
        self
    }

    /// The most a connection's QUIC stack holds of what the server sent and the client has not
    /// acknowledged, in bytes: 1 MiB by default, where quinn's own default is 10 MB.
    #[must_use]
    pub fn send_window(mut self, bytes: u64) -> Self {
        self.send_window = bytes;
        self
    }

    /// How many bytes may wait to be sent on a connection before it stops taking more and stops
    /// reading from the client: 64 KiB by default, as `openqtt-client` holds a server. A
    /// connection holds at most this much and one packet more, beside its send window.
    #[must_use]
    pub fn send_backlog(mut self, bytes: usize) -> Self {
        self.send_backlog = bytes;
        self
    }

    /// Whether to probe each path for an MTU above QUIC's 1,200 bytes (R7, D2).
    #[must_use]
    pub fn mtu_discovery(mut self, on: bool) -> Self {
        self.mtu_discovery = on;
        self
    }

    /// How clients resume TLS sessions.
    #[must_use]
    pub fn resumption(mut self, resumption: Resumption) -> Self {
        self.resumption = resumption;
        self
    }

    /// Whether to accept 0-RTT data. Off by default, and to stay off until report R7 has
    /// measured where resuming clients land (D5). It needs [`Resumption::SessionCache`], the one
    /// way rustls accepts 0-RTT (R7, F2). A connection then reaches the edge as soon as 0-RTT
    /// data arrives, and [`Event::HandshakeComplete`](crate::Event::HandshakeComplete) marks
    /// where it ends.
    #[must_use]
    pub fn early_data(mut self, on: bool) -> Self {
        self.early_data = on;
        self
    }

    /// The longest Keep Alive the session grants (`limits.keep_alive_max`), from which the QUIC
    /// idle timeout follows; see [`idle_timeout`](Self::idle_timeout).
    #[must_use]
    pub fn keep_alive_max(mut self, keep_alive_max: Duration) -> Self {
        self.keep_alive_max = keep_alive_max;
        self
    }

    /// The largest packet a client may send, counted over the whole packet: the Maximum Packet
    /// Size the session announces in CONNACK (R1, O5). A larger one is refused as soon as its
    /// fixed header arrives.
    #[must_use]
    pub fn max_packet_size(mut self, bytes: NonZeroU32) -> Self {
        self.max_packet_size = bytes;
        self
    }

    /// The node this listener runs on, which its connection IDs name so that a load balancer or
    /// forwarder can route by them (`CidRoute`). Without one, the connection IDs are quinn's own,
    /// random ones.
    #[must_use]
    pub fn node(mut self, node: NodeId) -> Self {
        self.node = Some(node);
        self
    }

    /// The send and receive buffer of each endpoint's UDP socket, in bytes. Unset, the system's
    /// default; Linux caps what is granted at `net.core.rmem_max` and `net.core.wmem_max`, far
    /// below the 8 MiB report R7 measured with.
    #[must_use]
    pub fn socket_buffer(mut self, bytes: usize) -> Self {
        self.socket_buffer = Some(bytes);
        self
    }

    /// The listener's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The QUIC idle timeout: 1.5 times the longest Keep Alive and a margin, so that QUIC never
    /// ends a connection MQTT still considers alive (docs/spec/mqtt-over-quic.md, section 6).
    /// The connection's timeout is the lower of this and the client's own.
    pub fn idle_timeout(&self) -> Duration {
        let one_and_a_half = self.keep_alive_max.saturating_add(self.keep_alive_max / 2);
        one_and_a_half.saturating_add(IDLE_MARGIN)
    }

    /// The QUIC transport settings of every connection (R7, D2): the stream limits, the windows,
    /// no datagrams, MTU discovery as set, and the idle timeout.
    ///
    /// # Errors
    ///
    /// [`Error::Setting`] for a value QUIC cannot carry.
    pub fn transport_config(&self) -> Result<TransportConfig, Error> {
        let varint = |setting: &str, value: u64| {
            VarInt::from_u64(value).map_err(|_| Error::Setting {
                setting: setting.to_owned(),
                reason: "larger than QUIC can carry".to_owned(),
            })
        };
        if self.max_streams == 0 {
            return Err(Error::Setting {
                setting: "max_streams".to_owned(),
                reason: "at least 1, for the control stream".to_owned(),
            });
        }
        let idle = IdleTimeout::try_from(self.idle_timeout()).map_err(|_| Error::Setting {
            setting: "keep_alive_max".to_owned(),
            reason: "too long for a QUIC idle timeout".to_owned(),
        })?;
        let mut transport = TransportConfig::default();
        transport
            .max_concurrent_bidi_streams(VarInt::from_u32(self.max_streams))
            .max_concurrent_uni_streams(VarInt::from_u32(0))
            .stream_receive_window(varint("stream_window", self.stream_window)?)
            .receive_window(varint("connection_window", self.connection_window)?)
            .send_window(self.send_window)
            // MQTT over QUIC uses no datagrams (spec section 1), so none are offered.
            .datagram_receive_buffer_size(None)
            .mtu_discovery_config(self.mtu_discovery.then(MtuDiscoveryConfig::default))
            .max_idle_timeout(Some(idle))
            // MQTT's Keep Alive is the liveness rule (R7, D3); the client pings, not the server.
            .keep_alive_interval(None);
        Ok(transport)
    }

    /// The configuration a quinn endpoint accepts connections with: TLS as
    /// [`ClientAuth`] and [`Resumption`] say, and [`transport_config`](Self::transport_config).
    ///
    /// # Errors
    ///
    /// [`Error::Tls`] when rustls refuses the certificate or key, and [`Error::Setting`] for a
    /// setting that cannot work.
    pub fn server_config(&self) -> Result<quinn::ServerConfig, Error> {
        let tls = crate::tls::server_config(self, self.chain.clone(), self.key.clone_key())?;
        let crypto = QuicServerConfig::try_from(tls).map_err(|error| Error::Setting {
            setting: "tls".to_owned(),
            reason: error.to_string(),
        })?;
        let mut server = quinn::ServerConfig::with_crypto(Arc::new(crypto));
        server.transport_config(Arc::new(self.transport_config()?));
        Ok(server)
    }

    /// The configuration of endpoint `index`: its connection IDs name the node and the index
    /// when a node is set (`CidRoute`), and its stateless reset key is its own, so that one
    /// endpoint cannot reset another's connections.
    pub fn endpoint_config(&self, index: u8) -> quinn::EndpointConfig {
        let mut endpoint = quinn::EndpointConfig::default();
        if let Some(node) = self.node {
            endpoint.cid_generator(move || Box::new(NodeConnectionIds::new(node, index)));
        }
        endpoint
    }
}

/// The certificates of a PEM file, which must hold at least one.
fn certificates(setting: &str, path: &Path) -> Result<Vec<CertificateDer<'static>>, Error> {
    let fail = |reason: String| Error::File {
        setting: setting.to_owned(),
        path: path.to_owned(),
        reason,
    };
    let certificates = CertificateDer::pem_file_iter(path)
        .map_err(|error| fail(error.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| fail(error.to_string()))?;
    if certificates.is_empty() {
        return Err(fail("holds no certificate".to_owned()));
    }
    Ok(certificates)
}

/// The private key of a PEM secret, read by `read`. No error repeats what the file holds.
fn private_key(
    setting: &str,
    path: &Path,
    read: impl FnOnce() -> std::io::Result<openqtt_config::Secret>,
) -> Result<PrivateKeyDer<'static>, Error> {
    let fail = |reason: String| Error::File {
        setting: setting.to_owned(),
        path: path.to_owned(),
        reason,
    };
    let secret = read().map_err(|error| fail(format!("cannot read it: {}", error.kind())))?;
    PrivateKeyDer::from_pem_slice(secret.expose())
        .map_err(|_| fail("holds no PEM private key".to_owned()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use openqtt_config::SecretFile;
    use openqtt_testkit::TestPki;

    use super::*;

    fn config() -> ListenerConfig {
        let pki = TestPki::new("Config CA").unwrap();
        let identity = pki.server(&["localhost"]).unwrap();
        ListenerConfig::new("default", identity.chain.clone(), identity.key())
    }

    /// A directory of its own for one test's files.
    fn directory(test: &str) -> PathBuf {
        let directory =
            std::env::temp_dir().join(format!("openqtt-transport-{test}-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        directory
    }

    /// A listener section naming a certificate, key and client CA written to `directory`.
    fn files(directory: &Path, pki: &TestPki) -> QuicListener {
        let identity = pki.server(&["localhost"]).unwrap();
        let cert = directory.join("tls.crt");
        let key = directory.join("tls.key");
        let ca = directory.join("ca.crt");
        std::fs::write(&cert, &identity.certificate_pem).unwrap();
        std::fs::write(&key, &identity.key_pem).unwrap();
        std::fs::write(&ca, pki.ca_pem()).unwrap();
        let mut listener = QuicListener::default();
        listener.cert_file = Some(cert);
        listener.key_file = Some(SecretFile::new(key));
        listener.client_ca_file = Some(ca);
        listener
    }

    #[test]
    fn the_idle_timeout_outlasts_one_and_a_half_keep_alives() {
        let config = config();
        // limits.keep_alive_max defaults to 20 minutes: 30 minutes and the margin.
        assert_eq!(config.idle_timeout(), Duration::from_secs(1_810));
        let config = config.keep_alive_max(Duration::from_secs(30));
        assert_eq!(config.idle_timeout(), Duration::from_secs(55));
    }

    #[test]
    fn the_defaults_are_those_of_the_reference_and_r7() {
        let config = config();
        assert_eq!(config.name(), "default");
        assert_eq!(config.address, "0.0.0.0:14567".parse().unwrap());
        assert_eq!(config.endpoints.get(), 1);
        assert_eq!(config.max_streams, 8);
        assert_eq!(config.stream_window, 1 << 20);
        assert_eq!(config.connection_window, 1 << 20);
        assert_eq!(config.send_window, 1 << 20);
        assert_eq!(config.send_backlog, 64 * 1024);
        assert_eq!(config.max_packet_size.get(), 1 << 20);
        assert_eq!(config.resumption, Resumption::Tickets);
        assert!(!config.early_data);
        assert!(config.mtu_discovery);
        assert!(config.transport_config().is_ok());
        assert!(config.server_config().is_ok());
        let debug = format!("{config:?}");
        assert!(debug.contains("chain: 1"), "{debug}");
        assert!(!debug.contains("key:"), "{debug}");
    }

    #[test]
    fn a_listener_without_streams_or_with_huge_windows_is_refused() {
        let config = config();
        let error = config
            .clone()
            .max_streams(0)
            .transport_config()
            .unwrap_err();
        assert!(error.to_string().contains("control stream"), "{error}");
        let error = config
            .clone()
            .stream_window(u64::MAX)
            .transport_config()
            .unwrap_err();
        assert!(error.to_string().starts_with("stream_window"), "{error}");
        let error = config
            .keep_alive_max(Duration::MAX)
            .transport_config()
            .unwrap_err();
        assert!(error.to_string().starts_with("keep_alive_max"), "{error}");
    }

    #[test]
    fn a_listener_section_maps_onto_the_transport() {
        let pki = TestPki::new("Settings CA").unwrap();
        let directory = directory("settings");
        let mut listener = files(&directory, &pki);
        listener.bind = "127.0.0.1:443".parse().unwrap();
        listener.require_client_cert = true;
        listener.max_streams = 3;
        listener.stream_window = openqtt_config::ByteSize::kib(64);
        listener.connection_window = openqtt_config::ByteSize::kib(256);
        listener.mtu_discovery = false;
        listener.session_tickets = false;
        let mut limits = Limits::default();
        limits.keep_alive_max = openqtt_config::Duration::from_secs(60);
        limits.maximum_packet_size = openqtt_config::ByteSize::kib(16);
        let config = ListenerConfig::from_settings("devices", &listener, &limits).unwrap();
        assert_eq!(config.name(), "devices");
        assert_eq!(config.address, "127.0.0.1:443".parse().unwrap());
        assert!(matches!(&config.client_auth, ClientAuth::Required(roots) if roots.len() == 1));
        assert_eq!(config.max_streams, 3);
        assert_eq!(config.stream_window, 64 * 1024);
        assert_eq!(config.connection_window, 256 * 1024);
        assert!(!config.mtu_discovery);
        assert_eq!(config.resumption, Resumption::Off);
        assert_eq!(config.idle_timeout(), Duration::from_secs(100));
        assert_eq!(config.max_packet_size.get(), 16 * 1024);
        assert!(config.server_config().is_ok());

        listener.require_client_cert = false;
        let config = ListenerConfig::from_settings("devices", &listener, &limits).unwrap();
        assert!(matches!(&config.client_auth, ClientAuth::Optional(_)));
        listener.client_ca_file = None;
        let config = ListenerConfig::from_settings("devices", &listener, &limits).unwrap();
        assert!(matches!(&config.client_auth, ClientAuth::None));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn missing_settings_and_unreadable_files_name_the_setting() {
        let pki = TestPki::new("Settings CA").unwrap();
        let directory = directory("missing");
        let limits = Limits::default();
        let listener = files(&directory, &pki);

        let mut without_cert = listener.clone();
        without_cert.cert_file = None;
        let error = ListenerConfig::from_settings("d", &without_cert, &limits).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("listeners.quic.d.cert_file: required"),
            "{error}"
        );

        let mut without_ca = listener.clone();
        without_ca.client_ca_file = None;
        without_ca.require_client_cert = true;
        let error = ListenerConfig::from_settings("d", &without_ca, &limits).unwrap_err();
        assert!(
            error
                .to_string()
                .starts_with("listeners.quic.d.client_ca_file"),
            "{error}"
        );

        let mut absent = listener.clone();
        absent.cert_file = Some(directory.join("absent.crt"));
        let error = ListenerConfig::from_settings("d", &absent, &limits).unwrap_err();
        assert!(matches!(error, Error::File { .. }), "{error}");

        let empty = directory.join("empty.crt");
        std::fs::write(&empty, "").unwrap();
        let mut no_certificate = listener;
        no_certificate.client_ca_file = Some(empty);
        let error = ListenerConfig::from_settings("d", &no_certificate, &limits).unwrap_err();
        assert!(
            error.to_string().ends_with("holds no certificate"),
            "{error}"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_key_file_is_never_quoted() {
        let path = Path::new("/secret/tls.key");
        let leak = "-----BEGIN PRIVATE KEY-----\nnot base64 at all\n";
        let error = private_key("listeners.quic.default.key_file", path, || {
            Ok(openqtt_config::Secret::new(leak.as_bytes()))
        })
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "listeners.quic.default.key_file: /secret/tls.key: holds no PEM private key"
        );
        let error = private_key("listeners.quic.default.key_file", path, || {
            Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
        })
        .unwrap_err();
        assert!(error.to_string().contains("permission denied"), "{error}");
    }

    #[test]
    fn endpoints_name_their_node_in_their_connection_ids() {
        let config = config().node(NodeId::new(7));
        // quinn takes the generator through the endpoint configuration; the generator itself
        // is tested in the cid module, and the IDs on the wire in the loopback tests.
        let _endpoint = config.endpoint_config(2);
    }
}
