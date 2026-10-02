//! What happens to sessions, for an extension that wants to hear of it.

use openqtt_core::{ClientId, SubOpts, Timestamp, TopicFilter};

use crate::ClientInfo;

/// A client's CONNECT was accepted.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Connected {
    /// The client.
    pub client: ClientInfo,
    /// Whether it resumed a session it had (Session Present in the CONNACK).
    pub session_present: bool,
    /// When the CONNACK was sent.
    pub at: Timestamp,
}

impl Connected {
    /// `client` connected at `at`.
    pub fn new(client: ClientInfo, session_present: bool, at: Timestamp) -> Self {
        Self {
            client,
            session_present,
            at,
        }
    }
}

/// Why a connection ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// The client sent DISCONNECT with this reason code.
    #[non_exhaustive]
    Client {
        /// The Disconnect Reason Code.
        code: u8,
    },
    /// The server sent DISCONNECT with this reason code: 0x8D when Keep Alive ran out, 0x9C
    /// on a drain (R3), and so on. A takeover's 0x8E is reported as [`TakenOver`] as well.
    ///
    /// [`TakenOver`]: crate::TakenOver
    #[non_exhaustive]
    Server {
        /// The Disconnect Reason Code.
        code: u8,
    },
    /// The connection closed with no DISCONNECT from either side.
    ConnectionLost,
}

impl DisconnectReason {
    /// The client sent DISCONNECT with `code`.
    pub const fn client(code: u8) -> Self {
        Self::Client { code }
    }

    /// The server sent DISCONNECT with `code`.
    pub const fn server(code: u8) -> Self {
        Self::Server { code }
    }
}

/// A connection ended. The session may outlive it (Session Expiry Interval).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Disconnected {
    /// The client whose connection ended.
    pub client_id: ClientId,
    /// Why.
    pub reason: DisconnectReason,
    /// When.
    pub at: Timestamp,
}

impl Disconnected {
    /// The connection of `client_id` ended at `at`, for `reason`.
    pub fn new(client_id: ClientId, reason: DisconnectReason, at: Timestamp) -> Self {
        Self {
            client_id,
            reason,
            at,
        }
    }
}

/// A subscription was made, or replaced by one with the same filter.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Subscribed {
    /// The client.
    pub client_id: ClientId,
    /// The filter as the client sent it.
    pub filter: TopicFilter,
    /// The filter as the cluster routes it, behind the listener's mountpoint (R2 rule 6); the
    /// same as `filter` without one.
    pub mounted: TopicFilter,
    /// Its options.
    pub options: SubOpts,
    /// When the SUBACK was sent.
    pub at: Timestamp,
}

impl Subscribed {
    /// `client_id` subscribed to `filter`, routed as `mounted`, at `at`.
    pub fn new(
        client_id: ClientId,
        filter: TopicFilter,
        mounted: TopicFilter,
        options: SubOpts,
        at: Timestamp,
    ) -> Self {
        Self {
            client_id,
            filter,
            mounted,
            options,
            at,
        }
    }
}

/// A subscription was removed by UNSUBSCRIBE.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Unsubscribed {
    /// The client.
    pub client_id: ClientId,
    /// The filter as the client sent it.
    pub filter: TopicFilter,
    /// The filter as the cluster routed it.
    pub mounted: TopicFilter,
    /// When the UNSUBACK was sent.
    pub at: Timestamp,
}

impl Unsubscribed {
    /// `client_id` unsubscribed from `filter`, routed as `mounted`, at `at`.
    pub fn new(
        client_id: ClientId,
        filter: TopicFilter,
        mounted: TopicFilter,
        at: Timestamp,
    ) -> Self {
        Self {
            client_id,
            filter,
            mounted,
            at,
        }
    }
}

/// A new connection with the same Client Identifier took the session over; the old one was
/// sent DISCONNECT 0x8E (R2 rule 22).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TakenOver {
    /// The client.
    pub client_id: ClientId,
    /// When the old connection was told.
    pub at: Timestamp,
}

impl TakenOver {
    /// The session of `client_id` was taken over at `at`.
    pub fn new(client_id: ClientId, at: Timestamp) -> Self {
        Self { client_id, at }
    }
}

/// Hears about connections, disconnections, subscriptions and takeovers.
///
/// Every method does nothing by default, so an implementation writes only those it needs. They
/// are called on the connection's own task, synchronously: an implementation returns at once
/// and hands any slow work, a write to a store or a call to a service, to a task or a queue of
/// its own.
pub trait SessionEvents: Send + Sync + 'static {
    /// A CONNECT was accepted.
    fn connected(&self, event: &Connected) {
        let _ = event;
    }

    /// A connection ended.
    fn disconnected(&self, event: &Disconnected) {
        let _ = event;
    }

    /// A subscription was made.
    fn subscribed(&self, event: &Subscribed) {
        let _ = event;
    }

    /// A subscription was removed.
    fn unsubscribed(&self, event: &Unsubscribed) {
        let _ = event;
    }

    /// A session was taken over by a new connection.
    fn taken_over(&self, event: &TakenOver) {
        let _ = event;
    }
}
