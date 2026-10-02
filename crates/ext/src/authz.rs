//! Authorization: deciding what a client may do.

use std::sync::Arc;

use openqtt_core::{QoS, TopicFilter, TopicName};

use crate::ClientInfo;

/// Something a client asks to do, with its topic as the client wrote it, before the listener's
/// mountpoint is applied (report R2, rule 7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Action<'a> {
    /// A PUBLISH, or the Will Message of a CONNECT.
    #[non_exhaustive]
    Publish {
        /// Its Topic Name.
        topic: &'a TopicName,
        /// Its QoS.
        qos: QoS,
        /// Its RETAIN flag (R2 rule 16).
        retain: bool,
    },
    /// One Topic Filter of a SUBSCRIBE. A shared subscription keeps its `$share/{ShareName}/`
    /// here; whether that part matters is the authorizer's choice.
    #[non_exhaustive]
    Subscribe {
        /// The filter.
        filter: &'a TopicFilter,
        /// The Maximum QoS it asks for.
        qos: QoS,
    },
    /// A message about to be delivered to the client, its topic as the client will receive
    /// it, without the mountpoint.
    #[non_exhaustive]
    Receive {
        /// The Topic Name.
        topic: &'a TopicName,
        /// The QoS it is delivered at.
        qos: QoS,
        /// The RETAIN flag it is delivered with.
        retain: bool,
    },
}

impl<'a> Action<'a> {
    /// A PUBLISH to `topic`.
    pub fn publish(topic: &'a TopicName, qos: QoS, retain: bool) -> Self {
        Self::Publish { topic, qos, retain }
    }

    /// A subscription to `filter`.
    pub fn subscribe(filter: &'a TopicFilter, qos: QoS) -> Self {
        Self::Subscribe { filter, qos }
    }

    /// A delivery of `topic` to the client.
    pub fn receive(topic: &'a TopicName, qos: QoS, retain: bool) -> Self {
        Self::Receive { topic, qos, retain }
    }
}

/// An authorization decision. A denied PUBLISH gets PUBACK or PUBREC 0x87 and a denied filter
/// SUBACK 0x87 (R2 rule 12, R1 D2); a denied QoS 0 PUBLISH is dropped and counted (R1 O14).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Permission {
    /// The client may.
    Allow,
    /// The client may not.
    Deny,
}

impl Permission {
    /// Whether this is [`Permission::Allow`].
    pub const fn is_allowed(self) -> bool {
        matches!(self, Self::Allow)
    }
}

/// Decides what clients may do, from rules held in memory.
///
/// It is synchronous because it runs for every PUBLISH and every filter of a SUBSCRIBE, inside
/// the edge (R3, the hot path): rules are cached where they are decided, and an implementation
/// that loads them from elsewhere does so in the background. The edge [binds](Self::bind) it
/// to each client once, when the client connects, so that the work a client's own rules need,
/// such as which of them name it, is done once rather than per message.
pub trait Authorizer: Send + Sync + 'static {
    /// Decides whether `client` may do `action`.
    fn authorize(&self, client: &ClientInfo, action: &Action<'_>) -> Permission;

    /// This authorizer specialised to `client`, kept for as long as the connection lasts. The
    /// default decides every action with [`authorize`](Self::authorize).
    fn bind(self: Arc<Self>, client: ClientInfo) -> Box<dyn ClientAuthorizer> {
        Box::new(Bound {
            authorizer: self,
            client,
        })
    }
}

/// An [`Authorizer`] bound to one client.
pub trait ClientAuthorizer: Send + Sync {
    /// The client it decides for.
    fn client(&self) -> &ClientInfo;

    /// Decides whether the client may do `action`.
    fn authorize(&self, action: &Action<'_>) -> Permission;
}

/// What the default [`Authorizer::bind`] returns.
struct Bound<A: ?Sized> {
    authorizer: Arc<A>,
    client: ClientInfo,
}

impl<A: Authorizer + ?Sized> ClientAuthorizer for Bound<A> {
    fn client(&self) -> &ClientInfo {
        &self.client
    }

    fn authorize(&self, action: &Action<'_>) -> Permission {
        self.authorizer.authorize(&self.client, action)
    }
}
