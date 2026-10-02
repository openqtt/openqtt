//! What the machine asks its caller to do.

use std::fmt;
use std::ops::Deref;

use bytes::Bytes;
use openqtt_codec::{Packet, ProtocolRefusal};
use openqtt_core::{ClientId, Message, QoS, SubOpts, Timestamp, TopicFilter, TopicName};

use crate::redact::Redacted;
use crate::{SessionState, StreamId};

/// One thing for the caller to do. [`Session::handle`](crate::Session::handle) returns them in
/// the order the caller performs them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Send `packet` to the client on `stream`. It already fits the client's Maximum Packet
    /// Size ([MQTT-3.1.2-24]).
    Send {
        /// The stream to send it on.
        stream: StreamId,
        /// The packet.
        packet: Packet,
    },
    /// Send the bytes of a refusal on the control stream: the answer to a CONNECT that is not
    /// MQTT 5.0 (report R1, D1). A [`Close`](Effect::Close) follows.
    SendRefusal(ProtocolRefusal),
    /// Finish the server's sending side of a data stream the client ended, once the server owes
    /// nothing more on it (docs/spec/mqtt-over-quic.md, section 2.4).
    FinishStream(u64),
    /// Stop reading from the client. The machine holds as much as it may of what the client
    /// sent and has not processed while it waits on an answer
    /// ([`Config::maximum_pending_packets`](crate::Config::maximum_pending_packets)); the rest
    /// waits in the transport, and QUIC's flow control holds the client back. The transport
    /// hands over at most what it already decoded: the machine ends a connection that sends
    /// it twice its limits.
    PauseReading,
    /// Read from the client again: what the machine held has been processed.
    ResumeReading,
    /// Close the network connection, once what was sent is delivered or a linger ends
    /// ([MQTT-3.2.2-7], [MQTT-3.14.4-2]). It comes once, last but for the answers to
    /// requests still outstanding: a claim made before it is released when it completes.
    Close(CloseCode),
    /// Authenticate the client and answer with [`Input::Authenticated`](crate::Input::Authenticated).
    /// Until the answer, the machine processes nothing else the client sent.
    Authenticate(Authentication),
    /// Decide whether the client may do each action and answer with
    /// [`Input::Authorized`](crate::Input::Authorized). Until the answer, the machine
    /// processes nothing else the client sent, so the order of its packets is kept.
    Authorize(Authorization),
    /// Claim the Client Identifier at its log partition and answer with
    /// [`Input::Claimed`](crate::Input::Claimed). CONNACK waits for the answer (report R3,
    /// Sessions).
    Claim(Claim),
    /// Give back the claim: the connection no longer owns the Client Identifier. It comes once
    /// for every claim that succeeded.
    Release(Release),
    /// Publish a message into the broker. With a token, answer with
    /// [`Input::Committed`](crate::Input::Committed) once the message is durable; the PUBACK
    /// or PUBREC waits for it (report R3, The hot path; report R1, D26).
    Publish(Publication),
    /// Add or replace a subscription's interest.
    Subscribe(Interest),
    /// Remove the interest of a subscription, by its mounted filter.
    Unsubscribe(TopicFilter),
    /// What to do with the Will Message stored with the claim, at the end of the connection.
    Will(WillOrder),
    /// Fire `timer` at `at`, replacing any earlier setting of it.
    SetTimer {
        /// The timer.
        timer: Timer,
        /// When to fire it.
        at: Timestamp,
    },
    /// Do not fire `timer`.
    CancelTimer(Timer),
    /// Count one occurrence of something worth a metric.
    Count(Counter),
}

/// The effects of one call, in order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Effects(Vec<Effect>);

impl Effects {
    /// No effects.
    pub const fn new() -> Self {
        Self(Vec::new())
    }

    /// Adds an effect at the end.
    pub(crate) fn push(&mut self, effect: Effect) {
        self.0.push(effect);
    }

    /// The effects, as a vector.
    pub fn into_vec(self) -> Vec<Effect> {
        self.0
    }
}

impl Deref for Effects {
    type Target = [Effect];

    fn deref(&self) -> &[Effect] {
        &self.0
    }
}

impl IntoIterator for Effects {
    type Item = Effect;
    type IntoIter = std::vec::IntoIter<Effect>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Effects {
    type Item = &'a Effect;
    type IntoIter = std::slice::Iter<'a, Effect>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// The QUIC application error code a connection closes with (docs/spec/mqtt-over-quic.md,
/// section 8).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CloseCode {
    /// 0x0: the MQTT exchange ended in order, a CONNACK or DISCONNECT having said why where one
    /// could.
    NoError,
    /// 0x1: the client broke the protocol where no packet could say so, as with a first packet
    /// other than CONNECT ([MQTT-3.1.0-1]).
    ProtocolError,
}

impl CloseCode {
    /// The application error code on the wire.
    pub const fn value(self) -> u64 {
        match self {
            Self::NoError => 0x0,
            Self::ProtocolError => 0x1,
        }
    }
}

/// A request to authenticate the client, at CONNECT or on a step of enhanced authentication or
/// re-authentication.
#[derive(Clone, PartialEq, Eq)]
pub struct Authentication {
    /// Which step this is.
    pub step: AuthStep,
    /// The Client Identifier the session goes under: the one the client sent, the one the
    /// server assigned, or the certificate's CN.
    pub client_id: ClientId,
    /// The User Name: the CONNECT's, or the certificate's CN on a listener configured for
    /// certificate identity (report R2, rule 4).
    pub username: Option<String>,
    /// The CONNECT's Password, at the first step only; never on a listener configured for
    /// certificate identity.
    pub password: Option<Bytes>,
    /// The subject CN of the client certificate, if the transport has one.
    pub certificate_cn: Option<String>,
    /// The Authentication Method, for enhanced authentication (section 4.12).
    pub method: Option<String>,
    /// The Authentication Data of the CONNECT or AUTH this step answers.
    pub data: Option<Bytes>,
}

/// Debug output that never shows the password or the authentication data: a credential must
/// not reach a log through `{:?}`.
impl fmt::Debug for Authentication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Authentication")
            .field("step", &self.step)
            .field("client_id", &self.client_id)
            .field("username", &self.username)
            .field("password", &Redacted(&self.password))
            .field("certificate_cn", &self.certificate_cn)
            .field("method", &self.method)
            .field("data", &Redacted(&self.data))
            .finish()
    }
}

/// Which step of authentication a request is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthStep {
    /// The CONNECT.
    Connect,
    /// The client's AUTH with reason code 0x18, continuing an exchange.
    Continue,
    /// The client's AUTH with reason code 0x19, starting a re-authentication (section 4.12.1).
    Reauthenticate,
}

/// A request to authorize actions, against the client's own topics before mounting (report R2,
/// rule 7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorization {
    /// The request, which the answer names.
    pub request: RequestId,
    /// What the client asks to do, one decision each.
    pub actions: Vec<Action>,
}

/// An action to authorize.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Action {
    /// Publish to a topic, as a PUBLISH or as the Will Message.
    Publish {
        /// The Topic Name, as the client sent it.
        topic: TopicName,
        /// The QoS.
        qos: QoS,
        /// The RETAIN flag (report R2, rule 16).
        retain: bool,
    },
    /// Subscribe to a filter.
    Subscribe {
        /// The Topic Filter, as the client sent it.
        filter: TopicFilter,
        /// The QoS the subscription would be granted.
        qos: QoS,
    },
}

/// Names an [`Authorization`], so that its answer finds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RequestId(pub(crate) u64);

impl RequestId {
    /// The number.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The claim of a Client Identifier at its log partition (report R3, Sessions).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claim {
    /// The identifier.
    pub client_id: ClientId,
    /// Whether the server assigned it: such a claim is refused when the identifier is in use,
    /// rather than taking the session over (report R1, O9).
    pub assigned: bool,
    /// Clean Start: discard any existing session ([MQTT-3.1.2-4]).
    pub clean_start: bool,
    /// The Session Expiry Interval the session gets, capped (report R1, O7).
    pub session_expiry: u32,
    /// The Will Message to store with the claim (report R3, Sessions).
    pub will: Option<WillMessage>,
}

/// The end of a claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release {
    /// The Client Identifier released.
    pub client_id: ClientId,
    /// What happens to the session.
    pub session: SessionEnd,
}

/// What happens to the session when its connection ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionEnd {
    /// It is kept for its Session Expiry Interval ([MQTT-3.1.2-23]), in this state: the
    /// handoff to whichever connection resumes it.
    Keep(SessionState),
    /// It ends now: its Session Expiry Interval is 0, or a limit ended it (report R1, O12).
    Discard,
}

/// A message to publish into the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publication {
    /// The message, its topic mounted.
    pub message: Message,
    /// Present for QoS 1 and 2: the [`Input::Committed`](crate::Input::Committed) that answers
    /// it carries it back.
    pub token: Option<PublishToken>,
}

/// Names a [`Publication`] whose outcome the machine waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PublishToken(pub(crate) u64);

impl PublishToken {
    /// The number.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A subscription's interest, for the edge's index or the session's log partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Interest {
    /// The filter, mounted. A delivery names the interest by it.
    pub filter: TopicFilter,
    /// The options granted, the Subscription Identifier included.
    pub options: SubOpts,
    /// A read of the retained messages matching the filter, to answer with
    /// [`Input::Retained`](crate::Input::Retained) naming it ([MQTT-3.3.1-9],
    /// [MQTT-3.3.1-10]); `None` when none are sent.
    pub retained: Option<RetainedRead>,
}

/// Names a read of retained messages, so that its answer finds the subscription that asked for
/// it, and an answer for a subscription since replaced or removed is told from the current one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RetainedRead(pub(crate) u64);

impl RetainedRead {
    /// The number.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A Will Message, as stored with a claim and published at the end of a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WillMessage {
    /// The message, its topic mounted, its publisher the session's Client Identifier, and no
    /// deadline yet.
    pub message: Message,
    /// The Message Expiry Interval, which runs from when the will is published.
    pub expiry_interval: Option<u32>,
    /// The Will Delay Interval in seconds ([MQTT-3.1.2-8]).
    pub delay: u32,
}

/// What to do with a connection's Will Message when the connection ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WillOrder {
    /// Publish it after `delay` seconds unless a new connection claims the session first
    /// ([MQTT-3.1.2-8], [MQTT-3.1.3-9]). The delay is already the lesser of the Will Delay
    /// Interval and the session's remaining life.
    Publish {
        /// The will.
        will: Box<WillMessage>,
        /// Seconds to wait.
        delay: u32,
    },
    /// Delete it unpublished ([MQTT-3.1.2-10], [MQTT-3.14.4-3]).
    Discard,
}

/// A timer the machine sets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Timer {
    /// The time a connection has to be accepted ([`Config::connect_timeout`](crate::Config::connect_timeout)).
    Connect,
    /// Keep Alive: 1.5 times the Keep Alive after the last packet ([MQTT-3.1.2-22]).
    KeepAlive,
}

/// Something worth counting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Counter {
    /// A QoS 0 PUBLISH the authorizer denied, dropped (report R1, O14).
    PublishDenied,
    /// A QoS 0 PUBLISH with a Topic Name or Response Topic that breaks section 4.7, dropped
    /// (report R1, O25).
    PublishTopicInvalid,
    /// A delivery whose message expired before it could be sent (report R1, O8).
    DeliveryExpired,
    /// A delivery too large for the client's Maximum Packet Size, discarded as if sent
    /// ([MQTT-3.1.2-25], report R1, D6).
    DeliveryTooLarge,
    /// A delivery whose topic is outside the client's namespace (report R2, rule 6).
    DeliveryOutsideNamespace,
    /// A delivery none of whose subscriptions takes it once checked again: unsubscribed, a
    /// filter that does not match the unmounted topic, or No Local.
    DeliveryUnmatched,
    /// An acknowledgement from the client for a packet the server has not sent.
    UnknownAcknowledgement,
    /// A packet the server could not send within the client's Maximum Packet Size even
    /// without its diagnostics.
    PacketTooLarge,
}
