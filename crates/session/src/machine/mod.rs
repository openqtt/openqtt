//! The machine: one [`Session`] for each network connection.

mod connect;
mod deliver;
mod publish;
mod streams;
mod subscribe;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use openqtt_codec::{
    self as codec, ConnAck, ConnAckProperties, ConnectReasonCode, Disconnect, DisconnectProperties,
    DisconnectReasonCode, MAX_PACKET_SIZE, Packet, PacketId, PacketType, Publish, Sender,
    SubAckReasonCode, SubscriptionOptions, Will,
};
use openqtt_core::{ClientId, Message, SubOpts, SubscriptionId, Timestamp, TopicFilter, TopicName};
use openqtt_topic::Mount;

use crate::phrase::phrase;
use crate::redact::{Credential, Secret};
use crate::state::{StoredDelivery, StoredOutbound, StoredSubscription};
use crate::{
    CloseCode, Config, Counter, Delivery, Effect, Effects, Input, Peer, Release, SessionEnd,
    SessionState, Shutdown, StreamId, Timer, WillMessage, WillOrder,
};

/// The MQTT 5 connection and session state machine of one network connection.
///
/// It is driven by [`handle`](Self::handle): each [`Input`] goes in with the time the caller
/// read, and the [`Effects`] the caller performs come out. It reads no clock, opens no socket
/// and spawns nothing, so a test drives it with scripted inputs and a clock of its own.
///
/// # The connection
///
/// 1. The first packet must be CONNECT ([MQTT-3.1.0-1]). The machine checks what the codec
///    leaves to it, picks the Client Identifier, assigning one for an empty identifier (report
///    R1, O9) or taking the certificate's CN (D20), and asks for authentication. Until the
///    answer, it processes nothing else the client sent, in order.
/// 2. Once authenticated, it resolves the mountpoint (report R2, rule 6), has the Will Message
///    authorized, and claims the Client Identifier at its log partition (report R3). CONNACK
///    0x00 goes out only after the claim, and only once the handshake of a 0-RTT connection
///    has completed (docs/spec/mqtt-over-quic.md, section 4).
/// 3. Then packets are processed in the order they arrived. A PUBLISH is authorized against
///    the client's own topic, mounted, and published with a token; its PUBACK or PUBREC waits
///    for the message to be durable, and acknowledgements leave in arrival order (D26, O15).
///    Deliveries from the broker go out within the client's Receive Maximum and Maximum Packet
///    Size, the mountpoint taken off.
/// 4. The connection ends with a DISCONNECT either way, a protocol error, a takeover, the loss
///    of the transport or an order from the edge. The machine then decides the will, releases
///    the claim with the session's state, and closes.
///
/// Every refusal is visible: a CONNACK before acceptance and a DISCONNECT after it (D4), and a
/// reason code in the acknowledgement when one PUBLISH or one subscription is refused (D2,
/// D32).
#[derive(Debug)]
pub struct Session {
    config: Arc<Config>,
    /// The subject CN of the client certificate.
    certificate_cn: Option<String>,
    /// Whether the TLS handshake has completed.
    handshake_complete: bool,
    /// The bits identifiers are drawn from, which never show in `{:?}`.
    random: Secret<u128>,
    /// How many identifiers have been drawn.
    draws: u32,
    phase: Phase,
    /// Whether a CONNECT was read, so that a refusal is a CONNACK.
    connect_read: bool,
    /// Whether the CONNECT arrived in 0-RTT data.
    connect_early: bool,
    /// What the client accepts of the packets the server sends.
    limits: Limits,
    /// What the CONNECT settled.
    client: Option<Client>,
    /// What the client sent and the machine has not processed yet, in arrival order.
    inbox: VecDeque<Received>,
    /// The bytes of the packets in `inbox`, as encoded.
    inbox_bytes: usize,
    /// The QoS 1 and 2 PUBLISH packets in `inbox` that will take a slot of the server's Receive
    /// Maximum, which counts them from when they arrive.
    inbox_slots: u16,
    /// Whether the transport was asked to stop reading.
    paused: bool,
    next_request: u64,
    next_token: u64,
    /// The packet waiting for its authorization.
    authorizing: Option<Authorizing>,
    reauth: Reauth,
    claim: ClaimState,
    /// How the connection ended, while its claim is still outstanding.
    late_ending: Option<Ending>,
    /// Acknowledgements owed, per stream, in the order the packets they answer arrived.
    replies: BTreeMap<StreamId, VecDeque<Reply>>,
    /// Publications waiting for their commit, by token.
    commits: BTreeMap<u64, Commit>,
    /// QoS 2 messages from the client, by Packet Identifier, until their PUBREL.
    inbound: BTreeMap<u16, Inbound>,
    /// QoS 1 and 2 PUBLISH packets from the client on this connection not yet answered in full,
    /// held to the server's Receive Maximum.
    inbound_in_flight: u16,
    /// Topic Aliases the client set, by alias.
    aliases_in: BTreeMap<u16, String>,
    /// QoS 1 and 2 messages to the client not completely acknowledged, in the order first sent.
    outbound: VecDeque<Outbound>,
    /// How many of them count against the client's Receive Maximum on this connection.
    outbound_in_window: usize,
    /// The Packet Identifiers they hold.
    ids: BTreeSet<u16>,
    /// Where the search for a free Packet Identifier starts.
    next_id: u16,
    /// Topic Aliases the server assigned, by topic.
    aliases_out: BTreeMap<TopicName, NonZeroU16>,
    /// Messages for the client, matched and not yet sent.
    queue: VecDeque<Queued>,
    /// Deliveries held until the retained messages of a new subscription are out.
    held: VecDeque<Delivery>,
    /// Subscriptions, by mounted filter.
    subscriptions: BTreeMap<TopicFilter, Subscription>,
    /// Retained reads not answered yet, by token, with the mounted filter that asked.
    reads: BTreeMap<u64, TopicFilter>,
    next_read: u64,
    next_subscription: u64,
    /// Data streams the client ended, by QUIC stream id.
    streams: BTreeMap<u64, DataStream>,
    /// When the last packet arrived, or the CONNACK went out.
    last_activity: Timestamp,
    connect_timer: bool,
    keep_alive_timer: bool,
}

/// The machine's progress through the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Waiting for CONNECT.
    Start,
    /// Waiting for the authenticator.
    Authenticating,
    /// Enhanced authentication: waiting for the client's next AUTH.
    Challenged,
    /// Waiting for the authorization of the Will Message.
    AuthorizingWill(u64),
    /// Waiting for the claim.
    Claiming,
    /// Claimed, waiting for the handshake to complete before CONNACK.
    Accepted,
    /// CONNACK 0x00 is out.
    Connected,
    /// The connection is closed.
    Closed,
}

/// How a connection ended, which decides its will and its session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// The client sent DISCONNECT 0x00: the will is deleted ([MQTT-3.14.4-3]).
    Clean,
    /// Any other end once accepted: the will is published after its delay ([MQTT-3.1.2-8]).
    Unclean,
    /// Another connection claimed the session. The will goes at once when its delay is 0 or
    /// the new connection ends the session, and not at all otherwise ([MQTT-3.1.3-9]).
    TakenOver {
        /// Whether the new connection has Clean Start 1.
        session_ends: bool,
    },
    /// Another connection claimed the session before this one was accepted: its claim, and so
    /// its will, was replaced.
    Superseded,
    /// A limit ended the session (report R1, O12): the will goes at once.
    SessionDiscarded,
    /// The connection was never accepted: its will never came into force.
    NotAccepted,
}

/// Where the claim of the Client Identifier stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimState {
    /// None made.
    None,
    /// Made, and not answered yet.
    Outstanding,
    /// Granted, and not released.
    Held,
    /// Released.
    Released,
}

/// Where a re-authentication stands (section 4.12.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reauth {
    /// None in progress.
    Idle,
    /// Waiting for the authenticator.
    Authenticating,
    /// Waiting for the client's next AUTH.
    Challenged,
}

/// What the client accepts of the packets the server sends.
#[derive(Debug, Clone, Copy)]
struct Limits {
    /// The client's Maximum Packet Size ([MQTT-3.1.2-24]).
    maximum_packet_size: u32,
    /// Request Problem Information ([MQTT-3.1.2-29]).
    problem_information: bool,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum_packet_size: MAX_PACKET_SIZE,
            problem_information: true,
        }
    }
}

/// Something the client sent.
#[derive(Debug)]
enum Received {
    /// A packet, and when it arrived.
    Packet {
        stream: StreamId,
        packet: Packet,
        early: bool,
        arrival: Arrival,
    },
    /// Bytes that did not decode.
    Error {
        stream: StreamId,
        error: codec::Error,
        packet_type: Option<PacketType>,
    },
}

/// When a packet arrived.
#[derive(Debug, Clone, Copy)]
struct Arrival {
    /// The time.
    at: Timestamp,
    /// Its size as encoded, which counts against the backlog.
    size: usize,
    /// Whether it is a QoS 1 or 2 PUBLISH that takes a slot of the server's Receive Maximum:
    /// every one but a QoS 2 repeat of an exchange still open when the machine gets to it. One
    /// that turns out to be a repeat all the same gives its slot back.
    slot: bool,
}

impl Received {
    fn stream(&self) -> StreamId {
        match self {
            Self::Packet { stream, .. } | Self::Error { stream, .. } => *stream,
        }
    }

    fn is_early(&self) -> bool {
        matches!(self, Self::Packet { early: true, .. })
    }
}

/// What the CONNECT settled for the connection.
#[derive(Debug)]
struct Client {
    /// The Client Identifier the session goes under.
    id: ClientId,
    /// Whether the server drew it (report R1, O9).
    assigned: bool,
    /// Whether CONNACK names it as the Assigned Client Identifier.
    announce_id: bool,
    /// The User Name, or the certificate's CN.
    username: Option<String>,
    /// The mount, once resolved (report R2, rule 6).
    mount: Option<Mount>,
    clean_start: bool,
    session_present: bool,
    /// The Keep Alive in use, in seconds.
    keep_alive: u16,
    /// What CONNACK says when the Keep Alive in use is not the client's (report R1, O4).
    server_keep_alive: Option<u16>,
    /// The Session Expiry Interval the CONNECT asked for.
    requested_expiry: Option<u32>,
    /// The Session Expiry Interval in use, capped (report R1, O7).
    expiry: u32,
    /// The client's Receive Maximum.
    receive_maximum: u16,
    /// The client's Topic Alias Maximum.
    topic_alias_maximum: u16,
    /// The Authentication Method of enhanced authentication.
    method: Option<String>,
    /// Authentication Data for the CONNACK, which never shows in `{:?}`.
    auth_data: Credential,
    /// The Will Message as checked, before mounting.
    will: Option<WillDraft>,
    /// The Will Message as stored with the claim.
    will_message: Option<WillMessage>,
}

/// A Will Message whose topics are checked.
#[derive(Debug)]
struct WillDraft {
    topic: TopicName,
    response_topic: Option<TopicName>,
    will: Will,
}

/// A packet waiting for its authorization.
#[derive(Debug)]
enum Authorizing {
    Publish {
        request: u64,
        stream: StreamId,
        /// When the PUBLISH arrived, which its message's deadline runs from.
        received_at: Timestamp,
        publish: Box<Publish>,
        topic: TopicName,
        response_topic: Option<TopicName>,
    },
    Subscribe {
        request: u64,
        stream: StreamId,
        packet_id: PacketId,
        id: Option<SubscriptionId>,
        plan: Vec<Planned>,
    },
}

impl Authorizing {
    fn stream(&self) -> StreamId {
        match self {
            Self::Publish { stream, .. } | Self::Subscribe { stream, .. } => *stream,
        }
    }
}

/// One filter of a SUBSCRIBE, as checked before authorization.
#[derive(Debug)]
enum Planned {
    /// Refused with this code.
    Refused(SubAckReasonCode),
    /// Valid, and waiting for its decision.
    Pending {
        filter: TopicFilter,
        options: SubscriptionOptions,
    },
}

/// An acknowledgement owed for a PUBLISH from the client.
#[derive(Debug)]
struct Reply {
    packet_id: PacketId,
    kind: ReplyKind,
    state: ReplyState,
    /// Whether sending it with this code frees a slot of the server's Receive Maximum.
    counted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplyKind {
    PubAck,
    PubRec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplyState {
    /// Waiting for the commit with this token.
    Waiting(u64),
    /// Ready to go with this code.
    Ready(AckCode),
}

/// The codes the server answers a PUBLISH with, the same in PUBACK and PUBREC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckCode {
    Success,
    NoMatchingSubscribers,
    UnspecifiedError,
    NotAuthorized,
    TopicNameInvalid,
    QuotaExceeded,
}

impl AckCode {
    fn puback(self) -> codec::PubAckReasonCode {
        use codec::PubAckReasonCode as C;
        match self {
            Self::Success => C::Success,
            Self::NoMatchingSubscribers => C::NoMatchingSubscribers,
            Self::UnspecifiedError => C::UnspecifiedError,
            Self::NotAuthorized => C::NotAuthorized,
            Self::TopicNameInvalid => C::TopicNameInvalid,
            Self::QuotaExceeded => C::QuotaExceeded,
        }
    }

    fn pubrec(self) -> codec::PubRecReasonCode {
        use codec::PubRecReasonCode as C;
        match self {
            Self::Success => C::Success,
            Self::NoMatchingSubscribers => C::NoMatchingSubscribers,
            Self::UnspecifiedError => C::UnspecifiedError,
            Self::NotAuthorized => C::NotAuthorized,
            Self::TopicNameInvalid => C::TopicNameInvalid,
            Self::QuotaExceeded => C::QuotaExceeded,
        }
    }

    fn is_error(self) -> bool {
        self.puback().is_error()
    }
}

/// A publication waiting for its commit.
#[derive(Debug)]
struct Commit {
    stream: StreamId,
    packet_id: PacketId,
    qos: codec::QoS,
    retain: bool,
}

/// A QoS 2 message from the client.
#[derive(Debug)]
struct Inbound {
    state: InboundState,
    stream: StreamId,
    /// Whether it holds a slot of the server's Receive Maximum on this connection.
    counted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InboundState {
    /// Published with this token, not yet durable.
    Committing(u64),
    /// Durable and acknowledged with PUBREC; waiting for PUBREL.
    AwaitingRelease,
    /// Published on an earlier connection, which ended before the commit answered. The
    /// identifier stays reserved: a repeat is published again under its receipt, and the log
    /// decides by the receipt whether it is new.
    Reserved,
    /// Refused with this code, by a PUBREC that has not gone out. A PUBLISH with the identifier
    /// arriving now was sent before the client could know: a repeat, with the same refusal.
    Refusing(AckCode),
    /// Refused with this code by a PUBREC that went out, while PUBLISH packets that arrived as
    /// repeats before it still wait: each gets the same refusal, and the entry goes with the
    /// last. One arriving now is a new message ([MQTT-4.3.3-9]).
    Refused(AckCode),
}

/// A QoS 1 or 2 message to the client.
#[derive(Debug)]
struct Outbound {
    packet_id: PacketId,
    stage: Stage,
    /// The PUBLISH as first sent, without a Topic Alias, until its PUBREC.
    publish: Option<Publish>,
    stream: StreamId,
    /// Whether it holds a slot of the client's Receive Maximum on this connection.
    counted: bool,
    /// Whether it went out on this connection.
    sent: bool,
}

/// What a message to the client waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// PUBACK, at QoS 1.
    Acknowledgement,
    /// PUBREC, at QoS 2.
    Receipt,
    /// PUBCOMP, at QoS 2 once PUBREL went out.
    Completion,
}

/// A message for the client, matched and not yet sent.
#[derive(Debug, Clone)]
struct Queued {
    message: Message,
    topic: TopicName,
    qos: openqtt_core::QoS,
    retain: bool,
    subscription_ids: Vec<SubscriptionId>,
    stream: StreamId,
}

/// A subscription of the session.
#[derive(Debug, Clone)]
struct Subscription {
    /// The filter as the client sent it.
    filter: TopicFilter,
    options: SubOpts,
    /// The stream deliveries for it travel on (docs/spec/mqtt-over-quic.md, section 2.3).
    stream: StreamId,
    /// When it was made, for the oldest among equals.
    order: u64,
    /// The retained read the subscription waits for; live deliveries for it wait too (report
    /// R1, O2). An answer to any other read of the same filter is stale.
    awaiting_read: Option<u64>,
}

/// A data stream the client ended.
#[derive(Debug, Default)]
struct DataStream {
    client_finished: bool,
    server_stopped: bool,
    /// Whether the server finished its side.
    finished: bool,
}

/// How many identifiers a connection draws before it gives up on finding a free one.
const MAX_DRAWS: u32 = 4;

impl Session {
    /// A machine for a connection the transport just accepted, and the timer it starts with.
    pub fn new(config: impl Into<Arc<Config>>, peer: Peer, now: Timestamp) -> (Self, Effects) {
        let config = config.into();
        let connect_timeout = config.connect_timeout;
        let mut session = Self {
            config,
            certificate_cn: peer.certificate_cn,
            handshake_complete: peer.handshake_complete,
            random: Secret(peer.random),
            draws: 0,
            phase: Phase::Start,
            connect_read: false,
            connect_early: false,
            limits: Limits::default(),
            client: None,
            inbox: VecDeque::new(),
            inbox_bytes: 0,
            inbox_slots: 0,
            paused: false,
            next_request: 1,
            next_token: 1,
            authorizing: None,
            reauth: Reauth::Idle,
            claim: ClaimState::None,
            late_ending: None,
            replies: BTreeMap::new(),
            commits: BTreeMap::new(),
            inbound: BTreeMap::new(),
            inbound_in_flight: 0,
            aliases_in: BTreeMap::new(),
            outbound: VecDeque::new(),
            outbound_in_window: 0,
            ids: BTreeSet::new(),
            next_id: 1,
            aliases_out: BTreeMap::new(),
            queue: VecDeque::new(),
            held: VecDeque::new(),
            subscriptions: BTreeMap::new(),
            reads: BTreeMap::new(),
            next_read: 1,
            next_subscription: 0,
            streams: BTreeMap::new(),
            last_activity: now,
            connect_timer: false,
            keep_alive_timer: false,
        };
        let mut fx = Effects::new();
        if let Some(timeout) = connect_timeout {
            session.connect_timer = true;
            fx.push(Effect::SetTimer {
                timer: Timer::Connect,
                at: now.saturating_add(timeout),
            });
        }
        (session, fx)
    }

    /// Takes one input at `now`, the time the caller read, and returns what to do about it.
    pub fn handle(&mut self, input: Input, now: Timestamp) -> Effects {
        let mut fx = Effects::new();
        match input {
            Input::Packet {
                stream: StreamId::Data(id),
                ..
            } if self
                .streams
                .get(&id)
                .is_some_and(|data| data.client_finished) =>
            {
                // The client said it would send nothing more on the stream.
                self.protocol_error(now, &mut fx);
            }
            Input::Packet {
                stream,
                packet,
                early,
            } => self.receive_packet(stream, packet, early, now, &mut fx),
            Input::DecodeError {
                stream,
                error,
                packet_type,
            } => self.receive(
                Received::Error {
                    stream,
                    error,
                    packet_type,
                },
                now,
                &mut fx,
            ),
            Input::HandshakeComplete {
                early_data_accepted,
            } => self.handshake_completed(early_data_accepted, now, &mut fx),
            Input::Authenticated(result) => self.authenticated(result, now, &mut fx),
            Input::Authorized { request, decisions } => {
                self.authorized(request.0, &decisions, now, &mut fx);
            }
            Input::Claimed(result) => self.claimed(result, now, &mut fx),
            Input::Committed { token, outcome } => self.committed(token.0, outcome, now, &mut fx),
            Input::Deliver(delivery) => self.deliver(delivery, now, &mut fx),
            Input::Retained { read, messages } => self.retained(read.0, messages, now, &mut fx),
            Input::Timer(timer) => self.timer(timer, now, &mut fx),
            Input::StepDown { session_ends } => self.step_down(session_ends, now, &mut fx),
            Input::StreamEnded { stream, end } => self.stream_ended(stream, end, now, &mut fx),
            Input::TransportClosed => self.transport_closed(&mut fx),
            Input::Shutdown(shutdown) => self.shutdown(shutdown, now, &mut fx),
        }
        self.drain(now, &mut fx);
        self.flow(now, &mut fx);
        fx
    }

    /// The Client Identifier the session goes under, once a CONNECT settled it.
    pub fn client_id(&self) -> Option<&ClientId> {
        self.client.as_ref().map(|client| &client.id)
    }

    /// Whether CONNACK 0x00 went out and the connection has not ended.
    pub fn is_connected(&self) -> bool {
        self.phase == Phase::Connected
    }

    /// Whether the connection has ended: the machine sends nothing more.
    pub fn is_closed(&self) -> bool {
        self.phase == Phase::Closed
    }

    /// QoS 1 and 2 PUBLISH packets sent to the client on this connection and not yet
    /// acknowledged in full, which the client's Receive Maximum caps ([MQTT-3.3.4-9]).
    pub fn in_flight_out(&self) -> usize {
        self.outbound_in_window
    }

    /// QoS 1 and 2 PUBLISH packets from the client on this connection not yet answered in full,
    /// which the server's Receive Maximum caps ([MQTT-3.3.4-7]) for every packet but those the
    /// client sent before CONNACK announced it.
    pub fn in_flight_in(&self) -> u16 {
        self.inbound_in_flight
    }

    /// Messages for the client that wait to be sent.
    pub fn queued(&self) -> usize {
        self.queue.len() + self.held.len()
    }

    /// The session's state as it stands, once a CONNECT settled its Client Identifier: what
    /// [`Release`] hands over when the connection ends.
    pub fn snapshot(&self) -> Option<SessionState> {
        self.client.as_ref().map(|client| self.state(client))
    }

    /// Takes in a packet the client sent.
    fn receive_packet(
        &mut self,
        stream: StreamId,
        packet: Packet,
        early: bool,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        if self.phase == Phase::Closed {
            return;
        }
        // A packet that came before the CONNACK went out, when the client could not know the
        // server's Receive Maximum yet and assumed 65,535 (section 3.2.2.3.3).
        let pipelined = self.phase != Phase::Connected;
        let slot = match &packet {
            Packet::Publish(publish) => match (publish.qos, publish.packet_id) {
                (codec::QoS::AtMostOnce, _) | (_, None) => false,
                // A repeat takes no slot, however far its first PUBLISH got.
                (codec::QoS::ExactlyOnce, Some(id)) => !self.arrives_as_repeat(id.get()),
                (codec::QoS::AtLeastOnce, Some(_)) => true,
            },
            _ => false,
        };
        // Receive Maximum holds from when a PUBLISH arrives, not when the machine gets to it
        // ([MQTT-3.3.4-7], report R1 O3 and D13); one sent behind the CONNECT, before the
        // CONNACK announced the limit, is held to none.
        let held = u32::from(self.inbound_in_flight) + u32::from(self.inbox_slots);
        if slot && !pipelined && held >= u32::from(self.config.receive_maximum.get()) {
            self.last_activity = self.last_activity.max(now);
            return self.close_with(DisconnectReasonCode::ReceiveMaximumExceeded, None, now, fx);
        }
        let arrival = Arrival {
            at: now,
            size: packet.encoded_len().unwrap_or(0),
            slot,
        };
        self.receive(
            Received::Packet {
                stream,
                packet,
                early,
                arrival,
            },
            now,
            fx,
        );
    }

    /// Takes in something the client sent, after anything it sent before, as long as the
    /// backlog allows.
    fn receive(&mut self, received: Received, now: Timestamp, fx: &mut Effects) {
        if self.phase == Phase::Closed {
            return;
        }
        // Every packet resets Keep Alive, from the moment it arrives ([MQTT-3.1.2-22]): a
        // client whose packets wait is not silent.
        self.last_activity = self.last_activity.max(now);
        if let Received::Packet { arrival, .. } = &received {
            self.inbox_bytes = self.inbox_bytes.saturating_add(arrival.size);
            if arrival.slot {
                self.inbox_slots = self.inbox_slots.saturating_add(1);
            }
        }
        self.inbox.push_back(received);
        // A transport keeps reading after a pause only for what it had already decoded. Twice
        // the limits is more than that: the connection ends rather than the backlog grow. A
        // single packet up to the Maximum Packet Size never does it.
        let packets = self.config.maximum_pending_packets.get();
        let bytes = self.config.maximum_pending_bytes.get();
        let packet_size =
            usize::try_from(self.config.maximum_packet_size.get()).unwrap_or(usize::MAX);
        let overflowing = self.inbox.len() > packets.saturating_mul(2)
            || self.inbox_bytes > bytes.saturating_mul(2).saturating_add(packet_size);
        if self.inbox.len() > 1 && overflowing {
            self.overflow(now, fx);
        }
    }

    /// Whether a QoS 2 PUBLISH with this Packet Identifier, arriving now, is a repeat the
    /// machine will answer from the exchange open for the identifier ([MQTT-4.3.3-10]), and so
    /// takes no slot of Receive Maximum. The exchange is followed from where it stands through
    /// the backlog, in the order the machine will get to it: a PUBLISH with the identifier opens
    /// it, or repeats it if it is open. A PUBREL may end it, depending on whether its commit
    /// came back first, so after one it is not counted on until the next PUBLISH. A refused
    /// exchange stays open until the PUBREC refusing it goes out, PUBREL or not
    /// ([MQTT-4.3.3-9]), and a reserved identifier is published again. The backlog is bounded,
    /// so following it costs little.
    fn arrives_as_repeat(&self, id: u16) -> bool {
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum Exchange {
            Closed,
            Open,
            Refusing,
        }
        let authorizing = matches!(
            &self.authorizing,
            Some(Authorizing::Publish { publish, .. }) if exactly_once(publish, id)
        );
        let mut exchange = match self.inbound.get(&id).map(|inbound| inbound.state) {
            _ if authorizing => Exchange::Open,
            Some(InboundState::Committing(_) | InboundState::AwaitingRelease) => Exchange::Open,
            Some(InboundState::Refusing(_)) => Exchange::Refusing,
            Some(InboundState::Reserved | InboundState::Refused(_)) | None => Exchange::Closed,
        };
        for received in &self.inbox {
            let Received::Packet {
                packet, arrival, ..
            } = received
            else {
                continue;
            };
            match packet {
                Packet::Publish(publish) if arrival.slot && exactly_once(publish, id) => {
                    exchange = Exchange::Open;
                }
                Packet::PubRel(pubrel)
                    if pubrel.packet_id.get() == id && exchange == Exchange::Open =>
                {
                    exchange = Exchange::Closed;
                }
                _ => {}
            }
        }
        exchange != Exchange::Closed
    }

    /// Whether a PUBLISH that arrived as a repeat of a refused exchange still waits: a QoS 2
    /// one with the identifier ahead of any that arrived as the first of a new exchange.
    fn refused_repeats_queued(&self, id: u16) -> bool {
        self.inbox
            .iter()
            .find_map(|received| match received {
                Received::Packet {
                    packet: Packet::Publish(publish),
                    arrival,
                    ..
                } if exactly_once(publish, id) => Some(!arrival.slot),
                _ => None,
            })
            .unwrap_or(false)
    }

    /// The next thing the client sent, out of the backlog.
    fn next_received(&mut self) -> Option<Received> {
        let received = self.inbox.pop_front()?;
        if let Received::Packet { arrival, .. } = &received {
            self.inbox_bytes = self.inbox_bytes.saturating_sub(arrival.size);
            if arrival.slot {
                self.inbox_slots = self.inbox_slots.saturating_sub(1);
            }
        }
        Some(received)
    }

    /// Drops what came in rejected early data. A QoS 2 repeat whose first PUBLISH is dropped
    /// takes its place in the exchange, and its slot.
    fn drop_early(&mut self) {
        let mut orphaned = BTreeSet::new();
        for mut received in std::mem::take(&mut self.inbox) {
            let early = received.is_early();
            if let Received::Packet {
                packet: Packet::Publish(publish),
                arrival,
                ..
            } = &mut received
                && publish.qos == codec::QoS::ExactlyOnce
                && let Some(id) = publish.packet_id
            {
                if early && arrival.slot {
                    orphaned.insert(id.get());
                } else if !early && !arrival.slot && orphaned.remove(&id.get()) {
                    arrival.slot = true;
                }
            }
            if !early {
                self.inbox.push_back(received);
            }
        }
        self.recount_inbox();
    }

    /// Counts the backlog again, after packets left it other than in order.
    fn recount_inbox(&mut self) {
        let (bytes, slots) = self
            .inbox
            .iter()
            .fold((0usize, 0u16), |(bytes, slots), received| match received {
                Received::Packet { arrival, .. } => (
                    bytes.saturating_add(arrival.size),
                    slots.saturating_add(u16::from(arrival.slot)),
                ),
                Received::Error { .. } => (bytes, slots),
            });
        self.inbox_bytes = bytes;
        self.inbox_slots = slots;
    }

    /// Asks the transport to stop reading while the backlog is past its limits, and to read
    /// again once it is down to half of them.
    fn flow(&mut self, now: Timestamp, fx: &mut Effects) {
        if self.phase == Phase::Closed {
            return;
        }
        let packets = self.config.maximum_pending_packets.get();
        let bytes = self.config.maximum_pending_bytes.get();
        if !self.paused && (self.inbox.len() > packets || self.inbox_bytes > bytes) {
            self.paused = true;
            fx.push(Effect::PauseReading);
        } else if self.paused && self.inbox.len() <= packets / 2 && self.inbox_bytes <= bytes / 2 {
            self.paused = false;
            // The client was not silent while nothing was read from it.
            self.last_activity = self.last_activity.max(now);
            fx.push(Effect::ResumeReading);
        }
    }

    /// Ends a connection whose transport kept feeding the machine past twice the backlog's
    /// limits: DISCONNECT 0x97 once accepted, CONNACK 0x97 before.
    fn overflow(&mut self, now: Timestamp, fx: &mut Effects) {
        match self.phase {
            Phase::Connected => {
                self.close_with(DisconnectReasonCode::QuotaExceeded, None, now, fx);
            }
            Phase::Closed => {}
            _ if self.connect_read => self.refuse(ConnectReasonCode::QuotaExceeded, now, fx),
            _ => self.finish(Ending::NotAccepted, CloseCode::ProtocolError, fx),
        }
    }

    /// Processes what the client sent, in order, until the machine has to wait for an answer.
    fn drain(&mut self, now: Timestamp, fx: &mut Effects) {
        loop {
            let ready = match self.phase {
                Phase::Start | Phase::Challenged => true,
                Phase::Connected => self.authorizing.is_none(),
                _ => false,
            };
            if !ready {
                return;
            }
            let Some(received) = self.next_received() else {
                return;
            };
            match self.phase {
                Phase::Start => self.first(received, now, fx),
                Phase::Challenged => self.challenged(received, now, fx),
                _ => self.connected(received, now, fx),
            }
        }
    }

    /// A packet, or an error, once the connection is accepted.
    fn connected(&mut self, received: Received, now: Timestamp, fx: &mut Effects) {
        let (stream, packet, arrival) = match received {
            Received::Error { error, .. } => {
                // [MQTT-4.13.1-1]
                return self.close_with(error.disconnect_reason_code(), None, now, fx);
            }
            Received::Packet {
                stream,
                packet,
                arrival,
                ..
            } => (stream, packet, arrival),
        };
        // Only the client's packets, with the client's reason codes and properties (Table 2-1,
        // [MQTT-3.3.4-6]); a decoder told the sender refuses these already.
        if let Err(error) = packet.check_sender(Sender::Client) {
            return self.close_with(error.disconnect_reason_code(), None, now, fx);
        }
        // CONNECT, PINGREQ, DISCONNECT and AUTH travel on the control stream only
        // (docs/spec/mqtt-over-quic.md, section 2.1).
        if stream != StreamId::Control && !matches!(packet.packet_type().value(), 3..=11) {
            return self.protocol_error(now, fx);
        }
        match packet {
            // [MQTT-3.1.0-2]
            Packet::Connect(_) => self.protocol_error(now, fx),
            Packet::Publish(publish) => self.publish(stream, publish, arrival, now, fx),
            Packet::PubAck(ack) => self.puback(&ack, now, fx),
            Packet::PubRec(rec) => self.pubrec(stream, &rec, now, fx),
            Packet::PubRel(rel) => self.pubrel(stream, &rel, now, fx),
            Packet::PubComp(comp) => self.pubcomp(&comp, now, fx),
            Packet::Subscribe(subscribe) => self.subscribe(stream, subscribe, now, fx),
            Packet::Unsubscribe(unsubscribe) => self.unsubscribe(stream, &unsubscribe, now, fx),
            // [MQTT-3.12.4-1]
            Packet::PingReq => self.send(StreamId::Control, Packet::PingResp, now, fx),
            Packet::Disconnect(disconnect) => self.client_disconnect(&disconnect, now, fx),
            Packet::Auth(auth) => self.reauthenticate(&auth, now, fx),
            // Refused by check_sender above.
            Packet::ConnAck(_) | Packet::SubAck(_) | Packet::UnsubAck(_) | Packet::PingResp => {
                self.protocol_error(now, fx);
            }
        }
    }

    /// The client's DISCONNECT ([MQTT-3.14.4-1]: nothing is sent after it).
    fn client_disconnect(&mut self, disconnect: &Disconnect, now: Timestamp, fx: &mut Effects) {
        let maximum = self.config.session_expiry_maximum;
        if let (Some(expiry), Some(client)) = (
            disconnect.properties.session_expiry_interval,
            self.client.as_mut(),
        ) {
            if client.requested_expiry.unwrap_or(0) == 0 && expiry != 0 {
                // A session that was to end with the connection cannot be given a life at its
                // end: a Protocol Error, and not taken as a DISCONNECT (section 3.14.2.2.2).
                return self.close_with(DisconnectReasonCode::ProtocolError, None, now, fx);
            }
            // Capped as at CONNECT (report R1, O7).
            client.expiry = expiry.min(maximum);
        }
        let ending = if disconnect.reason_code == DisconnectReasonCode::NormalDisconnection {
            // [MQTT-3.14.4-3]
            Ending::Clean
        } else {
            Ending::Unclean
        };
        self.finish(ending, CloseCode::NoError, fx);
    }

    /// Sends a packet to the client, held to what the client accepts.
    fn send(
        &mut self,
        stream: StreamId,
        packet: impl Into<Packet>,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        if self.phase == Phase::Closed {
            // [MQTT-3.14.4-1]
            return;
        }
        if let StreamId::Data(id) = stream
            && self
                .streams
                .get(&id)
                .is_some_and(|data| data.server_stopped)
        {
            // An acknowledgement that can no longer be delivered (docs/spec/mqtt-over-quic.md,
            // section 2.4).
            return self.protocol_error(now, fx);
        }
        let mut packet = packet.into();
        if !self.limits.problem_information {
            // [MQTT-3.1.2-29]
            strip_diagnostics(&mut packet);
        }
        // Reason Strings and User Properties go first ([MQTT-3.2.2-19] and the like).
        if packet.fit_within(self.limits.maximum_packet_size).is_ok() {
            return fx.push(Effect::Send { stream, packet });
        }
        fx.push(Effect::Count(Counter::PacketTooLarge));
        match packet {
            // The last packet of the connection goes with its reason code alone, if that fits.
            Packet::Disconnect(disconnect) => {
                let mut bare = Packet::Disconnect(Disconnect {
                    reason_code: disconnect.reason_code,
                    ..Disconnect::default()
                });
                if bare.fit_within(self.limits.maximum_packet_size).is_ok() {
                    fx.push(Effect::Send {
                        stream,
                        packet: bare,
                    });
                }
            }
            Packet::ConnAck(connack) => {
                let mut bare = Packet::from(ConnAck {
                    session_present: false,
                    reason_code: connack.reason_code,
                    properties: ConnAckProperties::default(),
                });
                if bare.fit_within(self.limits.maximum_packet_size).is_ok() {
                    fx.push(Effect::Send {
                        stream,
                        packet: bare,
                    });
                }
            }
            // A PUBLISH is discarded as if sent ([MQTT-3.1.2-25]); its caller measures it first.
            Packet::Publish(_) => {}
            // Any other packet completes an exchange the client waits on. One it can never
            // receive leaves it waiting for ever, so the connection ends instead.
            _ => self.fail_too_large(now, fx),
        }
    }

    /// Ends the connection because the client's Maximum Packet Size cannot hold a packet the
    /// server owes it: DISCONNECT 0x95 once accepted, CONNACK 0x95 before.
    fn fail_too_large(&mut self, now: Timestamp, fx: &mut Effects) {
        match self.phase {
            Phase::Connected => {
                self.close_with(DisconnectReasonCode::PacketTooLarge, None, now, fx);
            }
            Phase::Closed => {}
            _ => self.refuse(ConnectReasonCode::PacketTooLarge, now, fx),
        }
    }

    /// Ends the connection over a Protocol Error, with the packet the phase allows: DISCONNECT
    /// 0x82 once accepted, CONNACK 0x82 before, and nothing before a CONNECT (report R1, D4;
    /// [MQTT-3.14.0-1]).
    fn protocol_error(&mut self, now: Timestamp, fx: &mut Effects) {
        match self.phase {
            Phase::Connected => {
                self.close_with(DisconnectReasonCode::ProtocolError, None, now, fx);
            }
            Phase::Closed => {}
            _ if self.connect_read => self.refuse(ConnectReasonCode::ProtocolError, now, fx),
            _ => self.finish(Ending::NotAccepted, CloseCode::ProtocolError, fx),
        }
    }

    /// Sends DISCONNECT and ends an accepted connection. The will is published
    /// ([MQTT-3.1.2-8]).
    fn close_with(
        &mut self,
        code: DisconnectReasonCode,
        server_reference: Option<String>,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        self.close_ending(code, server_reference, Ending::Unclean, now, fx);
    }

    /// Sends DISCONNECT and ends an accepted connection as `ending` says.
    fn close_ending(
        &mut self,
        code: DisconnectReasonCode,
        server_reference: Option<String>,
        ending: Ending,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        // The server never sends a Session Expiry Interval here ([MQTT-3.14.2-2]).
        let disconnect = Disconnect {
            reason_code: code,
            properties: DisconnectProperties {
                reason_string: phrase(code.value()),
                server_reference,
                ..DisconnectProperties::default()
            },
        };
        self.send(StreamId::Control, disconnect, now, fx);
        self.finish(ending, CloseCode::NoError, fx);
    }

    /// Refuses the connection with CONNACK and closes it ([MQTT-3.2.2-7], [MQTT-3.2.2-6]).
    fn refuse(&mut self, code: ConnectReasonCode, now: Timestamp, fx: &mut Effects) {
        self.refuse_ending(code, None, Ending::NotAccepted, now, fx);
    }

    /// Refuses the connection with CONNACK and closes it, as `ending` says.
    fn refuse_ending(
        &mut self,
        code: ConnectReasonCode,
        server_reference: Option<String>,
        ending: Ending,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        if !self.handshake_complete {
            // No CONNACK before the handshake completes, a refusal included
            // (docs/spec/mqtt-over-quic.md, section 4): the refusal is a close without a reply,
            // which report R1 (D4) allows before acceptance.
            return self.finish(ending, CloseCode::NoError, fx);
        }
        let code = if code.is_error() {
            code
        } else {
            ConnectReasonCode::UnspecifiedError
        };
        let connack = ConnAck {
            session_present: false,
            reason_code: code,
            properties: ConnAckProperties {
                reason_string: phrase(code.value()),
                server_reference,
                ..ConnAckProperties::default()
            },
        };
        self.send(StreamId::Control, connack, now, fx);
        self.finish(ending, CloseCode::NoError, fx);
    }

    /// Ends the connection: the will and the claim are settled, and the transport closes. After
    /// this the machine sends nothing ([MQTT-3.14.4-1], [MQTT-3.1.4-6]).
    fn finish(&mut self, ending: Ending, code: CloseCode, fx: &mut Effects) {
        if self.phase == Phase::Closed {
            return;
        }
        self.phase = Phase::Closed;
        self.inbox.clear();
        self.recount_inbox();
        self.authorizing = None;
        if self.connect_timer {
            self.connect_timer = false;
            fx.push(Effect::CancelTimer(Timer::Connect));
        }
        if self.keep_alive_timer {
            self.keep_alive_timer = false;
            fx.push(Effect::CancelTimer(Timer::KeepAlive));
        }
        match self.claim {
            ClaimState::Held => self.release(ending, fx),
            ClaimState::Outstanding => self.late_ending = Some(ending),
            ClaimState::None | ClaimState::Released => {}
        }
        fx.push(Effect::Close(code));
    }

    /// Settles the will and gives back the claim.
    fn release(&mut self, ending: Ending, fx: &mut Effects) {
        self.claim = ClaimState::Released;
        let Some(client) = &self.client else {
            return;
        };
        if let Some(will) = &client.will_message {
            let order = match ending {
                // [MQTT-3.1.2-10] [MQTT-3.14.4-3]
                Ending::Clean | Ending::NotAccepted => Some(WillOrder::Discard),
                // [MQTT-3.1.2-8]: after the delay, or when the session ends if sooner.
                Ending::Unclean => Some(WillOrder::Publish {
                    will: Box::new(will.clone()),
                    delay: will.delay.min(client.expiry),
                }),
                // The non-normative comment of section 3.1.2.5 and [MQTT-3.1.3-9].
                Ending::TakenOver { session_ends } if session_ends || will.delay == 0 => {
                    Some(WillOrder::Publish {
                        will: Box::new(will.clone()),
                        delay: 0,
                    })
                }
                Ending::TakenOver { .. } | Ending::Superseded => None,
                Ending::SessionDiscarded => Some(WillOrder::Publish {
                    will: Box::new(will.clone()),
                    delay: 0,
                }),
            };
            if let Some(order) = order {
                fx.push(Effect::Will(order));
            }
        }
        let keep = client.expiry > 0
            && !matches!(
                ending,
                Ending::SessionDiscarded | Ending::TakenOver { session_ends: true }
            );
        let session = if keep {
            SessionEnd::Keep(self.state(client))
        } else {
            SessionEnd::Discard
        };
        fx.push(Effect::Release(Release {
            client_id: client.id.clone(),
            session,
        }));
    }

    /// The session's state, for a handoff.
    fn state(&self, client: &Client) -> SessionState {
        let mut subscriptions: Vec<_> = self.subscriptions.iter().collect();
        subscriptions.sort_by_key(|(_, subscription)| subscription.order);
        let stored = |queued: &Queued| StoredDelivery {
            message: queued.message.clone(),
            topic: queued.topic.clone(),
            qos: queued.qos,
            retain: queued.retain,
            subscription_ids: queued.subscription_ids.clone(),
        };
        SessionState {
            client_id: client.id.clone(),
            expiry: client.expiry,
            mount: client.mount.as_ref().map(|mount| mount.as_str().to_owned()),
            subscriptions: subscriptions
                .into_iter()
                .map(|(mounted, subscription)| StoredSubscription {
                    filter: subscription.filter.clone(),
                    mounted: mounted.clone(),
                    options: subscription.options,
                })
                .collect(),
            outbound: self
                .outbound
                .iter()
                .map(|outbound| match (&outbound.publish, outbound.stage) {
                    (Some(publish), Stage::Acknowledgement | Stage::Receipt) => {
                        StoredOutbound::Publish(Box::new(publish.clone()))
                    }
                    _ => StoredOutbound::Release(outbound.packet_id),
                })
                .collect(),
            awaiting_release: self
                .inbound
                .iter()
                .filter(|(_, inbound)| inbound.state == InboundState::AwaitingRelease)
                .filter_map(|(&id, _)| PacketId::new(id))
                .collect(),
            // A commit still out may have gone through: the identifier stays reserved, and the
            // log tells a repeat from a new message by the receipt it committed.
            awaiting_commit: self
                .inbound
                .iter()
                .filter(|(_, inbound)| {
                    matches!(
                        inbound.state,
                        InboundState::Committing(_) | InboundState::Reserved
                    )
                })
                .filter_map(|(&id, _)| PacketId::new(id))
                .collect(),
            queue: self
                .queue
                .iter()
                .map(stored)
                .chain(
                    self.held
                        .iter()
                        .filter_map(|delivery| self.resolve(delivery).ok())
                        .map(|queued| stored(&queued)),
                )
                .collect(),
            next_packet_id: self.next_id,
        }
    }

    /// A timer fired.
    fn timer(&mut self, timer: Timer, now: Timestamp, fx: &mut Effects) {
        match timer {
            Timer::Connect => {
                if !self.connect_timer {
                    return;
                }
                self.connect_timer = false;
                match self.phase {
                    Phase::Start => self.finish(Ending::NotAccepted, CloseCode::ProtocolError, fx),
                    // The client did not finish authenticating in time.
                    Phase::Challenged => self.refuse(ConnectReasonCode::NotAuthorized, now, fx),
                    Phase::Connected | Phase::Closed => {}
                    _ => self.refuse(ConnectReasonCode::ServerUnavailable, now, fx),
                }
            }
            Timer::KeepAlive => {
                if !self.keep_alive_timer || self.phase != Phase::Connected {
                    return;
                }
                if self.paused {
                    // Nothing is read from the client while paused, so its silence says
                    // nothing: Keep Alive starts again from now.
                    self.last_activity = self.last_activity.max(now);
                }
                let deadline = self.keep_alive_deadline();
                if now >= deadline {
                    // [MQTT-3.1.2-22], report R1 D5: as if the network had failed, so the will
                    // is published.
                    self.keep_alive_timer = false;
                    self.close_with(DisconnectReasonCode::KeepAliveTimeout, None, now, fx);
                } else {
                    fx.push(Effect::SetTimer {
                        timer: Timer::KeepAlive,
                        at: deadline,
                    });
                }
            }
        }
    }

    /// 1.5 times the Keep Alive after the last packet ([MQTT-3.1.2-22]).
    fn keep_alive_deadline(&self) -> Timestamp {
        let keep_alive = self.client.as_ref().map_or(0, |client| client.keep_alive);
        self.last_activity
            .saturating_add(Duration::from_millis(u64::from(keep_alive) * 1_500))
    }

    /// Another connection claimed the Client Identifier ([MQTT-3.1.4-3]).
    fn step_down(&mut self, session_ends: bool, now: Timestamp, fx: &mut Effects) {
        match self.phase {
            Phase::Connected => self.close_ending(
                DisconnectReasonCode::SessionTakenOver,
                None,
                Ending::TakenOver { session_ends },
                now,
                fx,
            ),
            // Claimed but not yet accepted: there is no CONNACK that says "taken over", and no
            // DISCONNECT may come first ([MQTT-3.14.0-1]).
            Phase::Accepted => self.finish(Ending::Superseded, CloseCode::NoError, fx),
            _ => {}
        }
    }

    /// The edge or an operator ends the connection.
    fn shutdown(&mut self, shutdown: Shutdown, now: Timestamp, fx: &mut Effects) {
        let (disconnect, connack, server_reference) = match shutdown {
            Shutdown::ServerShuttingDown => (
                DisconnectReasonCode::ServerShuttingDown,
                ConnectReasonCode::ServerUnavailable,
                None,
            ),
            Shutdown::UseAnotherServer { server_reference } => (
                DisconnectReasonCode::UseAnotherServer,
                ConnectReasonCode::UseAnotherServer,
                server_reference,
            ),
            Shutdown::ServerMoved { server_reference } => (
                DisconnectReasonCode::ServerMoved,
                ConnectReasonCode::ServerMoved,
                server_reference,
            ),
            Shutdown::AdministrativeAction => (
                DisconnectReasonCode::AdministrativeAction,
                ConnectReasonCode::NotAuthorized,
                None,
            ),
        };
        match self.phase {
            // The will waits for its delay, so a client that reconnects within it publishes
            // none (report R1, O23).
            Phase::Connected => self.close_with(disconnect, server_reference, now, fx),
            Phase::Closed => {}
            _ if self.connect_read => {
                self.refuse_ending(connack, server_reference, Ending::NotAccepted, now, fx);
            }
            _ => self.finish(Ending::NotAccepted, CloseCode::NoError, fx),
        }
    }

    /// The transport closed under the machine.
    fn transport_closed(&mut self, fx: &mut Effects) {
        let ending = if self.phase == Phase::Connected {
            Ending::Unclean
        } else {
            Ending::NotAccepted
        };
        self.finish(ending, CloseCode::NoError, fx);
    }

    /// A new request number.
    fn request(&mut self) -> u64 {
        let request = self.next_request;
        self.next_request += 1;
        request
    }

    /// A new publication token.
    fn token(&mut self) -> u64 {
        let token = self.next_token;
        self.next_token += 1;
        token
    }
}

/// Whether `publish` is a QoS 2 PUBLISH with the Packet Identifier `id`.
fn exactly_once(publish: &Publish, id: u16) -> bool {
    publish.qos == codec::QoS::ExactlyOnce
        && publish
            .packet_id
            .is_some_and(|packet_id| packet_id.get() == id)
}

/// Drops the Reason String and User Properties from a packet that may not carry them when the
/// client asked for no problem information: all but PUBLISH, CONNACK and DISCONNECT
/// ([MQTT-3.1.2-29]).
fn strip_diagnostics(packet: &mut Packet) {
    let properties = match packet {
        Packet::PubAck(codec::PubAck { properties, .. })
        | Packet::PubRec(codec::PubRec { properties, .. })
        | Packet::PubRel(codec::PubRel { properties, .. })
        | Packet::PubComp(codec::PubComp { properties, .. })
        | Packet::SubAck(codec::SubAck { properties, .. })
        | Packet::UnsubAck(codec::UnsubAck { properties, .. }) => properties,
        Packet::Auth(auth) => {
            auth.properties.reason_string = None;
            auth.properties.user_properties.clear();
            return;
        }
        _ => return,
    };
    properties.reason_string = None;
    properties.user_properties.clear();
}

/// The `n`th 128 bits drawn from `random`: the bits themselves first, then the bits mixed with
/// the draw number, so that a draw after a collision is another identifier.
fn draw(random: u128, n: u32) -> u128 {
    if n == 0 {
        return random;
    }
    // splitmix64's finalizer on each half, after adding the draw number times the golden
    // ratio. Not a cryptographic generator: the caller's bits are the secret, and a redraw is
    // only needed after a collision, which 125 random bits make all but impossible.
    let mix = |mut z: u64| {
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    let step = u64::from(n).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let high = u64::try_from(random >> 64).unwrap_or(0);
    let low = u64::try_from(random & u128::from(u64::MAX)).unwrap_or(0);
    (u128::from(mix(high.wrapping_add(step))) << 64) | u128::from(mix(low ^ step))
}
