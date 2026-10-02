//! The settings, section by section.
//!
//! Each field's doc comment is its entry in the reference, docs/spec/config.md, so it is
//! written for an operator: what the setting does, what its value means, and the report that
//! decided its default.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;

use crate::schema::{choice, section};
use crate::values::{ByteSize, Duration, Endpoint, HostPort, SecretFile};

section! {
    /// Every setting of an OpenQTT process. docs/spec/config.md describes each one, and
    /// [`Settings::load`](crate::Settings::load) reads them.
    pub struct Settings {
        /// This node, the cluster it belongs to, and the roles it runs (R3).
        pub cluster: Cluster = Cluster::default(),
        /// The listeners clients connect to. Only the edge role opens them.
        pub listeners: Listeners = Listeners::default(),
        /// What an edge allows each client, from the choices report R1 makes where MQTT 5.0
        /// leaves a value open (its O-entries).
        pub limits: Limits = Limits::default(),
        /// How clients are authenticated and authorized.
        pub auth: Auth = Auth::default(),
        /// The edge role: client connections and the interest it registers with the routers.
        pub edge: Edge = Edge::default(),
        /// The router role: the interest index.
        pub router: Router = Router::default(),
        /// The log role: durable state in Raft partitions.
        pub log: Log = Log::default(),
        /// The admin role: the REST API.
        pub admin: Admin = Admin::default(),
        /// The HTTP listener every process runs, whatever its roles.
        pub http: Http = Http::default(),
        /// Logs and metrics.
        pub observability: Observability = Observability::default(),
        /// Where the process keeps state on disk.
        pub storage: Storage = Storage::default(),
    }
}

section! {
    /// The `[cluster]` section.
    pub struct Cluster {
        /// The cluster's name. Every certificate between roles carries it, in
        /// `spiffe://openqtt/<cluster>/<role>/<node>` (R3), so it is lowercase letters, digits and
        /// `-`, at most 63 characters.
        pub name: String = "openqtt".to_owned(),
        /// This node's name, unique in the cluster: in Kubernetes, the pod's name. Required unless
        /// `roles` is `["all"]`. Lowercase letters, digits, `-` and `.`, at most 253 characters.
        pub node_name: Option<String> = None,
        /// The roles this process runs: `all`, or one or more of `edge`, `router`, `log` and
        /// `admin` (R3).
        pub roles: Vec<Role> = vec![Role::All],
        /// Where this node finds the others when it starts. In Kubernetes, the headless Service
        /// of each role (R3).
        pub seeds: Vec<HostPort> = Vec::new(),
        /// The failure zone this node runs in. The log spreads the replicas of a partition across
        /// zones (R3). Unset, the node is in no zone.
        pub zone: Option<String> = None,
        /// The certificate chain (PEM) this node presents to the others, naming the cluster, its
        /// role and itself (R3).
        pub cert_file: Option<PathBuf> = None,
        /// The private key (PEM) of `cert_file`.
        pub key_file: Option<SecretFile> = None,
        /// The CA certificates (PEM) the other nodes' certificates must chain to.
        pub ca_file: Option<PathBuf> = None,
    }
}

choice! {
    /// A role a process runs (R3).
    pub enum Role {
        /// Every role in one process, for development and small deployments.
        All = "all",
        /// Client connections.
        Edge = "edge",
        /// The interest index.
        Router = "router",
        /// Durable state.
        Log = "log",
        /// The REST API.
        Admin = "admin",
    }
}

section! {
    /// The `[listeners]` section.
    pub struct Listeners {
        /// MQTT 5 over QUIC listeners (docs/spec/mqtt-over-quic.md), each named by the operator.
        /// There is one by default, named `default`; a file that names any replaces it. A
        /// variable can change a listener that exists but never create one, so a misspelt name
        /// is an error rather than a second listener. Two things have no setting: 0-RTT stays
        /// off until report R7 has measured where resuming clients land (R7, D5), and QUIC
        /// datagrams are off because MQTT over QUIC does not use them (spec section 1).
        pub quic: BTreeMap<String, QuicListener> = default_listeners(),
    }
}

/// One listener, named `default`, with every default.
fn default_listeners() -> BTreeMap<String, QuicListener> {
    BTreeMap::from([("default".to_owned(), QuicListener::default())])
}

section! {
    /// A `[listeners.quic.<name>]` section.
    pub struct QuicListener {
        /// The UDP address to listen on. 14567 is the port EMQX uses for MQTT over QUIC; a
        /// deployment facing the internet should also listen on 443 (spec section 1).
        pub bind: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 14567)),
        /// The server's certificate chain (PEM). Required where the edge role runs.
        pub cert_file: Option<PathBuf> = None,
        /// The private key (PEM) of `cert_file`. Required where the edge role runs.
        pub key_file: Option<SecretFile> = None,
        /// The CA certificates (PEM) a client certificate must chain to. Only these are trusted
        /// for clients, not the roots of `cert_file` (R2 rule 2).
        pub client_ca_file: Option<PathBuf> = None,
        /// Refuse a client that presents no certificate (R2 rule 1). Needs `client_ca_file`.
        pub require_client_cert: bool = false,
        /// Name each client by its certificate: the subject CN becomes its username and its
        /// client identifier, whatever the client sends (R2 rule 4, R1 D20). Needs
        /// `require_client_cert`, and `enable_authn` off: the certificate is the authentication.
        pub identity_from_cn: bool = false,
        /// A prefix added to every topic a client of this listener publishes and subscribes to,
        /// and removed from every topic delivered to it (R2 rule 6). `${username}` and
        /// `${clientid}` stand for the client's own. Empty, topics are left as they are.
        pub mountpoint: String = String::new(),
        /// Authenticate each client with the user list of `[auth]` (R2 rule 5). Off, a client
        /// connects without a password: for development, or with `identity_from_cn`.
        pub enable_authn: bool = true,
        /// The bidirectional streams a client may open, the control stream included (R7, D2):
        /// the control stream and seven data streams. quinn keeps about 40 bytes of state for
        /// each from the moment a connection starts. A client may open no unidirectional
        /// streams.
        pub max_streams: u32 = 8,
        /// The receive window of each stream (R7, D2).
        pub stream_window: ByteSize = ByteSize::mib(1),
        /// The receive window of the whole connection (R7, D2).
        pub connection_window: ByteSize = ByteSize::mib(1),
        /// Probe each path for an MTU above QUIC's 1,200 bytes (R7, D2).
        pub mtu_discovery: bool = true,
        /// Let clients resume TLS sessions from stateless tickets (R7, D5). Off, every
        /// handshake is a full one.
        pub session_tickets: bool = true,
    }
}

section! {
    /// The `[limits]` section.
    pub struct Limits {
        /// The shortest Keep Alive a client is given. A client asking for less, other than 0,
        /// gets this value as Server Keep Alive (R1, O4).
        pub keep_alive_min: Duration = Duration::from_secs(10),
        /// The longest Keep Alive a client is given. A client asking for more, or for 0, gets
        /// this value as Server Keep Alive (R1, O4). The QUIC idle timeout is set above 1.5
        /// times it (spec section 6).
        pub keep_alive_max: Duration = Duration::from_secs(1_200),
        /// The Receive Maximum announced in CONNACK, which inbound QoS 1 and 2 messages are held
        /// to; messages to a client are held to the lower of it and the client's own (R1, O3).
        pub receive_maximum: u16 = 32,
        /// The Maximum Packet Size announced in CONNACK, counted over the whole packet (R1, O5,
        /// R2 rule 8).
        pub maximum_packet_size: ByteSize = ByteSize::mib(1),
        /// The Topic Alias Maximum announced in CONNACK; 0 accepts no aliases (R1, O6).
        pub topic_alias_maximum: u16 = 64,
        /// The longest Session Expiry Interval a session is kept for. A longer one, never
        /// included, is cut to this and returned in CONNACK (R1, O7).
        pub session_expiry_max: Duration = Duration::from_secs(604_800),
        /// The subscriptions one session may hold; one more gets SUBACK 0x97 (R1, O16).
        pub max_subscriptions: u32 = 1_000,
        /// The levels a topic or filter may have. A PUBLISH with more gets 0x90 and a filter
        /// with more 0x8F (R1, O16, R2 rule 8).
        pub max_topic_levels: u32 = 128,
        /// The messages one session may have queued. Reaching it ends the session (R1, O12).
        pub max_queued_messages: u32 = 1_000,
        /// The longest Client Identifier accepted, in bytes; a longer one gets CONNACK 0x85
        /// (R1, O10). At least 23, which MQTT requires every server to accept.
        pub max_client_id_length: u16 = 256,
    }
}

section! {
    /// The `[auth]` section.
    pub struct Auth {
        /// Users to load when the cluster starts, for listeners with `enable_authn` (R2 rule 5).
        /// The admin API manages them after that.
        pub password_bootstrap_file: Option<SecretFile> = None,
        /// How `password_bootstrap_file` holds passwords: `plain`, hashed when loaded, or
        /// already `hashed`.
        pub password_bootstrap_type: PasswordType = PasswordType::Hashed,
        /// The authorization rules, evaluated in order, the first match deciding (R2 rules 9 to
        /// 11). Unset, no rule matches, so every publish and subscription is denied.
        pub acl_file: Option<PathBuf> = None,
        /// Authentication by JSON Web Token. Not available yet.
        pub jwt: JwtAuthn = JwtAuthn::default(),
        /// Authentication by an HTTP service. Not available yet.
        pub http: HttpAuthn = HttpAuthn::default(),
    }
}

choice! {
    /// How a password bootstrap file holds passwords.
    pub enum PasswordType {
        /// In the clear; hashed when loaded.
        Plain = "plain",
        /// Already hashed.
        Hashed = "hashed",
    }
}

section! {
    /// The `[auth.jwt]` section, held for JWT authentication.
    pub struct JwtAuthn {
        /// Must stay `false`: JWT authentication is not built yet, and turning it on is refused
        /// rather than ignored.
        pub enabled: bool = false,
    }
}

section! {
    /// The `[auth.http]` section, held for HTTP authentication.
    pub struct HttpAuthn {
        /// Must stay `false`: HTTP authentication is not built yet, and turning it on is refused
        /// rather than ignored.
        pub enabled: bool = false,
    }
}

section! {
    /// The `[edge]` section.
    pub struct Edge {
        /// T in report R6: below a node of an edge's interest, a level with more than this many
        /// distinct values becomes `+`, and the resulting shape is registered once more than
        /// this many filters share it (R6, D2 and D3).
        pub interest_threshold: u32 = 64,
        /// The shallowest depth at which interest is coarsened: the root is 0 and the first
        /// level 1 (R6, D3). A deployment that places connections by namespace raises it.
        pub interest_floor: u32 = 1,
        /// How long an edge keeps a departed client's interest before withdrawing it, so that a
        /// client reconnecting to the same edge changes no route (R6, D5).
        pub interest_grace: Duration = Duration::from_secs(30),
    }
}

section! {
    /// The `[router]` section.
    pub struct Router {
        /// The virtual shards of the interest index, assigned to the live routers by
        /// rendezvous hashing (R3).
        pub shards: u32 = 64,
    }
}

section! {
    /// The `[log]` section.
    pub struct Log {
        /// The log's partitions, each a Raft group. Fixed when the cluster is created (R3).
        pub partitions: u32 = 256,
        /// The replicas of each partition: 1, or 3 across zones (R3).
        pub replication_factor: u32 = 1,
    }
}

section! {
    /// The `[admin]` section.
    pub struct Admin {
        /// API keys to load when the cluster starts. The admin API manages them after that.
        pub api_key_bootstrap_file: Option<SecretFile> = None,
    }
}

section! {
    /// The `[http]` section.
    pub struct Http {
        /// The TCP address of the HTTP listener every process runs: `/healthz` and `/readyz` on
        /// every role, `/metrics` when `observability.prometheus.enabled`, and the admin API
        /// where the admin role runs. Keep it inside the cluster (R3).
        pub bind: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8080)),
    }
}

section! {
    /// The `[observability]` section.
    pub struct Observability {
        /// Which log events are written: a level (`error`, `warn`, `info`, `debug`, `trace`),
        /// or directives such as `info,openqtt_edge=debug`.
        pub log_level: String = "info".to_owned(),
        /// How log lines are written to stderr: `json`, one object per line, or `text`.
        pub log_format: LogFormat = LogFormat::Json,
        /// Pushing metrics to an OpenTelemetry collector.
        pub otlp: Otlp = Otlp::default(),
        /// Metrics for Prometheus to scrape.
        pub prometheus: Prometheus = Prometheus::default(),
    }
}

choice! {
    /// How log lines are written.
    pub enum LogFormat {
        /// One JSON object per line, for a log collector.
        Json = "json",
        /// Plain text, for a person at a terminal.
        Text = "text",
    }
}

section! {
    /// The `[observability.otlp]` section.
    pub struct Otlp {
        /// The collector's base URL, for OTLP over HTTP with protobuf bodies: metrics go to its
        /// `/v1/metrics`. Unset, nothing is pushed.
        pub endpoint: Option<Endpoint> = None,
        /// HTTP headers to send with every export, one `name: value` per line, for a collector
        /// that wants a token.
        pub headers_file: Option<SecretFile> = None,
        /// The CA certificates (PEM) an `https` endpoint's certificate must chain to. Unset,
        /// the Mozilla roots built into OpenQTT are trusted.
        pub ca_file: Option<PathBuf> = None,
        /// How often metrics are pushed.
        pub interval: Duration = Duration::from_secs(60),
        /// How long one push may take.
        pub timeout: Duration = Duration::from_secs(10),
    }
}

section! {
    /// The `[observability.prometheus]` section.
    pub struct Prometheus {
        /// Serve metrics at `/metrics` on the HTTP listener, for a scraper that presents the
        /// token in `token_file`.
        pub enabled: bool = false,
        /// The bearer token a scraper must present. Required when `enabled`: metrics are never
        /// served to anyone who asks.
        pub token_file: Option<SecretFile> = None,
    }
}

section! {
    /// The `[storage]` section.
    pub struct Storage {
        /// The directory the process keeps its state in. The log role's partitions live here;
        /// report R4 decides the engine and its settings.
        pub data_dir: PathBuf = PathBuf::from("/var/lib/openqtt"),
    }
}
