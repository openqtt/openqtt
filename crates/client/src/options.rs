//! What a connection asks for: the CONNECT packet and how the client behaves on it.

use std::num::{NonZeroU16, NonZeroU32};
use std::time::Duration;

use bytes::Bytes;
use openqtt_codec::{Connect, Will};

use crate::Session;

/// How long [`Client::connect`](crate::Client::connect) waits for the transport and the
/// CONNACK, unless [`ConnectOptions::connect_timeout`] says otherwise.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The Keep Alive a new [`ConnectOptions`] asks for, in seconds.
pub const DEFAULT_KEEP_ALIVE: u16 = 60;

/// How many events wait for the application before the client stops reading from the
/// server, unless [`ConnectOptions::event_capacity`] says otherwise.
pub const DEFAULT_EVENT_CAPACITY: usize = 1024;

/// What to connect with: every field of CONNECT (section 3.1), and how the client behaves on
/// the connection.
///
/// A new one asks for Clean Start 1 and a Keep Alive of [`DEFAULT_KEEP_ALIVE`] seconds, and
/// leaves every property at the default the specification gives its absence.
///
/// ```
/// use openqtt_client::{ConnectOptions, QoS, Will};
///
/// let options = ConnectOptions::new("sensor-17")
///     .keep_alive(30)
///     .session_expiry_interval(3_600)
///     .will(Will {
///         qos: QoS::AtLeastOnce,
///         topic: "status/sensor-17".into(),
///         payload: "offline".into(),
///         ..Will::default()
///     })
///     .credentials("sensor-17", "secret");
/// assert_eq!(options.connect_packet().keep_alive, 30);
/// ```
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    pub(crate) connect: Connect,
    pub(crate) session: Option<Session>,
    pub(crate) connect_timeout: Duration,
    pub(crate) ping_timeout: Option<Duration>,
    pub(crate) packet_log: bool,
    pub(crate) event_capacity: usize,
}

impl ConnectOptions {
    /// Connects as `client_id`, with Clean Start 1. An empty identifier asks the server to
    /// assign one ([MQTT-3.1.3-6]); [`Client::client_id`](crate::Client::client_id) then
    /// returns it.
    pub fn new(client_id: impl Into<String>) -> Self {
        Self::from_connect(Connect {
            clean_start: true,
            keep_alive: DEFAULT_KEEP_ALIVE,
            client_id: client_id.into(),
            ..Connect::default()
        })
    }

    /// Connects with exactly this CONNECT packet.
    pub fn from_connect(connect: Connect) -> Self {
        Self {
            connect,
            session: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            ping_timeout: None,
            packet_log: false,
            event_capacity: DEFAULT_EVENT_CAPACITY,
        }
    }

    /// The CONNECT packet these options send.
    pub fn connect_packet(&self) -> &Connect {
        &self.connect
    }

    /// Clean Start (section 3.1.2.4). With 0 and no [`resume`](Self::resume)d session the
    /// client holds no session state, so a Session Present 1 makes it close the connection
    /// ([MQTT-3.2.2-4]).
    #[must_use]
    pub fn clean_start(mut self, clean_start: bool) -> Self {
        self.connect.clean_start = clean_start;
        self
    }

    /// Resumes `session`: Clean Start 0 and the session's Client Identifier. If the server
    /// still holds the session, the client resends what was in flight ([MQTT-4.4.0-1]).
    #[must_use]
    pub fn resume(mut self, session: Session) -> Self {
        self.connect.clean_start = false;
        self.connect.client_id.clone_from(&session.client_id);
        self.session = Some(session);
        self
    }

    /// Keep Alive in seconds; 0 turns it off (section 3.1.2.10). The client sends PINGREQ when
    /// it has sent nothing else for that long ([MQTT-3.1.2-20]), and uses the server's Server
    /// Keep Alive instead when CONNACK carries one ([MQTT-3.1.2-21]).
    #[must_use]
    pub fn keep_alive(mut self, seconds: u16) -> Self {
        self.connect.keep_alive = seconds;
        self
    }

    /// Session Expiry Interval in seconds (section 3.1.2.11.2); 0, the default, ends the
    /// session with the connection.
    #[must_use]
    pub fn session_expiry_interval(mut self, seconds: u32) -> Self {
        self.connect.properties.session_expiry_interval = Some(seconds);
        self
    }

    /// Receive Maximum: how many QoS 1 and 2 messages the server may have unacknowledged
    /// towards this client (section 3.1.2.11.3).
    #[must_use]
    pub fn receive_maximum(mut self, maximum: NonZeroU16) -> Self {
        self.connect.properties.receive_maximum = Some(maximum);
        self
    }

    /// Maximum Packet Size this client accepts (section 3.1.2.11.4). A larger packet from the
    /// server is a protocol error, and the client disconnects with 0x95.
    #[must_use]
    pub fn maximum_packet_size(mut self, maximum: NonZeroU32) -> Self {
        self.connect.properties.maximum_packet_size = Some(maximum);
        self
    }

    /// Topic Alias Maximum: the highest Topic Alias the server may use towards this client
    /// (section 3.1.2.11.5). The client resolves every alias from 1 to it ([MQTT-3.3.2-10]).
    #[must_use]
    pub fn topic_alias_maximum(mut self, maximum: u16) -> Self {
        self.connect.properties.topic_alias_maximum = Some(maximum);
        self
    }

    /// Request Response Information (section 3.1.2.11.6).
    #[must_use]
    pub fn request_response_information(mut self, request: bool) -> Self {
        self.connect.properties.request_response_information = Some(request);
        self
    }

    /// Request Problem Information (section 3.1.2.11.7).
    #[must_use]
    pub fn request_problem_information(mut self, request: bool) -> Self {
        self.connect.properties.request_problem_information = Some(request);
        self
    }

    /// Adds a User Property to CONNECT (section 3.1.2.11.8).
    #[must_use]
    pub fn user_property(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.connect
            .properties
            .user_properties
            .push((name.into(), value.into()));
        self
    }

    /// The Will Message (sections 3.1.2.5 and 3.1.3.2 to 3.1.3.4).
    #[must_use]
    pub fn will(mut self, will: Will) -> Self {
        self.connect.will = Some(will);
        self
    }

    /// A User Name and Password (sections 3.1.3.5 and 3.1.3.6).
    #[must_use]
    pub fn credentials(self, username: impl Into<String>, password: impl Into<Bytes>) -> Self {
        self.username(username).password(password)
    }

    /// A User Name (section 3.1.3.5).
    #[must_use]
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.connect.username = Some(username.into());
        self
    }

    /// A Password, which MQTT 5 allows without a User Name (section 3.1.3.6).
    #[must_use]
    pub fn password(mut self, password: impl Into<Bytes>) -> Self {
        self.connect.password = Some(password.into());
        self
    }

    /// How long to wait for the transport and the CONNACK together.
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// How long to wait for PINGRESP before the client gives the connection up as lost.
    /// Unset, it is the Keep Alive in use.
    #[must_use]
    pub fn ping_timeout(mut self, timeout: Duration) -> Self {
        self.ping_timeout = Some(timeout);
        self
    }

    /// Emits [`Event::Received`](crate::Event::Received) for every packet the server sends,
    /// CONNACK first, before the client acts on it. For tests and diagnostics.
    #[must_use]
    pub fn packet_log(mut self, on: bool) -> Self {
        self.packet_log = on;
        self
    }

    /// How many events may wait for the application before the client stops reading from
    /// the server; at least 1.
    #[must_use]
    pub fn event_capacity(mut self, capacity: usize) -> Self {
        self.event_capacity = capacity.max(1);
        self
    }
}
