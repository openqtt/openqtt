//! A session's state as plain data: what a connection hands over when it ends, and what a claim
//! hands to the connection that resumes the session.

use openqtt_codec::{PacketId, Publish};
use openqtt_core::{ClientId, Message, QoS, SubOpts, SubscriptionId, TopicFilter, TopicName};

/// Everything a session keeps between connections (section 4.1): its subscriptions, the
/// messages in flight both ways, the messages waiting to be sent, and where Packet Identifiers
/// go next. The Will Message is not here: it belongs to a connection and is stored with its
/// claim (report R3, Sessions). Topic Aliases are not here either: they never outlive a
/// connection ([MQTT-3.3.2-7]).
///
/// Plain data with public fields, so that the log can store it and a claim can return it. Its
/// stable encoding is report R4's to define; until then it moves as this value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionState {
    /// The Client Identifier the session is kept under.
    pub client_id: ClientId,
    /// The Session Expiry Interval in seconds, as capped (report R1, O7).
    pub expiry: u32,
    /// The mount the state was built under (report R2, rule 6), if any. A connection whose
    /// mount differs does not resume the state: its topics belong to another namespace.
    pub mount: Option<String>,
    /// The subscriptions, oldest first.
    pub subscriptions: Vec<StoredSubscription>,
    /// QoS 1 and 2 messages sent to the client and not completely acknowledged, in the order
    /// they were first sent, which is the order they are sent again ([MQTT-4.4.0-1], report
    /// R1, D11).
    pub outbound: Vec<StoredOutbound>,
    /// Packet Identifiers of QoS 2 messages from the client whose PUBREL has not arrived. A
    /// PUBLISH that repeats one is acknowledged again and not delivered twice (report R1, D8).
    pub awaiting_release: Vec<PacketId>,
    /// Messages for the client not yet sent, oldest first.
    pub queue: Vec<StoredDelivery>,
    /// Where the search for a free Packet Identifier starts.
    pub next_packet_id: u16,
}

impl SessionState {
    /// A session with nothing in it.
    pub fn new(client_id: ClientId, expiry: u32) -> Self {
        Self {
            client_id,
            expiry,
            mount: None,
            subscriptions: Vec::new(),
            outbound: Vec::new(),
            awaiting_release: Vec::new(),
            queue: Vec::new(),
            next_packet_id: 1,
        }
    }
}

/// A subscription of a stored session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSubscription {
    /// The Topic Filter as the client sent it, which UNSUBSCRIBE names and deliveries are
    /// checked against once the mountpoint is taken off.
    pub filter: TopicFilter,
    /// The filter as the rest of the cluster sees it, mounted.
    pub mounted: TopicFilter,
    /// The options granted, the Subscription Identifier included.
    pub options: SubOpts,
}

/// A message sent to the client and not completely acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredOutbound {
    /// A PUBLISH at QoS 1, or at QoS 2 before its PUBREC: sent again with DUP set
    /// ([MQTT-3.3.1-1]). As it was first sent, without a Topic Alias.
    Publish(Box<Publish>),
    /// A QoS 2 message whose PUBREC arrived: its PUBREL is sent again.
    Release(PacketId),
}

impl StoredOutbound {
    /// The Packet Identifier the message holds.
    pub fn packet_id(&self) -> Option<PacketId> {
        match self {
            Self::Publish(publish) => publish.packet_id,
            Self::Release(packet_id) => Some(*packet_id),
        }
    }
}

/// A message for the client, matched and not yet sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDelivery {
    /// The message as the broker delivered it, its topic mounted.
    pub message: Message,
    /// The topic as the client sees it, the mountpoint taken off.
    pub topic: TopicName,
    /// The QoS it goes out at: the message's, capped by the subscriptions that matched it
    /// ([MQTT-3.8.4-8]).
    pub qos: QoS,
    /// The RETAIN flag it goes out with ([MQTT-3.3.1-12], [MQTT-3.3.1-13]).
    pub retain: bool,
    /// The Subscription Identifiers of the subscriptions that matched it ([MQTT-3.3.4-3]).
    pub subscription_ids: Vec<SubscriptionId>,
}
