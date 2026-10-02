//! What the machine is told: packets and decoding errors from the client, the answers to its
//! requests, deliveries from the broker, timers, and orders from the edge.

use bytes::Bytes;
use openqtt_codec::{ConnectReasonCode, Packet, PacketType};
use openqtt_core::{Message, TopicFilter};

use crate::{PublishToken, RequestId, SessionState, Timer};

/// A stream of the QUIC connection (docs/spec/mqtt-over-quic.md, section 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StreamId {
    /// The control stream, the first bidirectional stream the client opens. It carries
    /// CONNECT, CONNACK, PINGREQ, PINGRESP, DISCONNECT and AUTH, and in single-stream mode
    /// everything else too (section 2.1 and 2.2).
    Control,
    /// A data stream, by its QUIC stream id (section 2.3).
    Data(u64),
}

/// One thing that happened to the connection, for [`Session::handle`](crate::Session::handle).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    /// A packet the transport decoded from the client.
    Packet {
        /// The stream it came on.
        stream: StreamId,
        /// The packet.
        packet: Packet,
        /// Whether it arrived in 0-RTT data, which may be replayed until the handshake
        /// completes (docs/spec/mqtt-over-quic.md, section 4).
        early: bool,
    },
    /// The client's bytes on `stream` could not be decoded. Nothing more can be read from that
    /// stream.
    DecodeError {
        /// The stream.
        stream: StreamId,
        /// Why the decoder refused them.
        error: openqtt_codec::Error,
        /// The packet type in the first byte of the refused packet, when the decoder got that
        /// far. It tells a malformed CONNECT, which is refused with a CONNACK, from another
        /// packet sent first, which is not ([MQTT-3.1.0-1], [MQTT-3.1.4-1]).
        packet_type: Option<PacketType>,
    },
    /// The TLS handshake of a connection accepted with 0-RTT completed.
    HandshakeComplete {
        /// Whether the early data was accepted. When it was not, what arrived in it is dropped
        /// (docs/spec/mqtt-over-quic.md, section 4).
        early_data_accepted: bool,
    },
    /// The authenticator's answer to the last [`Effect::Authenticate`](crate::Effect::Authenticate).
    Authenticated(AuthResult),
    /// The authorizer's answer to an [`Effect::Authorize`](crate::Effect::Authorize).
    Authorized {
        /// The request it answers.
        request: RequestId,
        /// One decision for each action of the request, in order. A missing one is a denial.
        decisions: Vec<Decision>,
    },
    /// The log's answer to the [`Effect::Claim`](crate::Effect::Claim).
    Claimed(ClaimResult),
    /// What became of a message the machine published with a token: once it is durable, or
    /// once it was refused (report R3, The hot path).
    Committed {
        /// The token of the [`Publication`](crate::Publication).
        token: PublishToken,
        /// The outcome.
        outcome: PublishOutcome,
    },
    /// A message for this session, from the broker.
    Deliver(Delivery),
    /// The retained messages an [`Interest`](crate::Interest) with `send_retained` asked for.
    /// It must come for every such interest, with no messages when there are none: live
    /// deliveries for the subscription wait for it (report R1, O2).
    Retained {
        /// The mounted filter of the interest.
        filter: TopicFilter,
        /// The retained messages matching it, in the log's order.
        messages: Vec<Message>,
    },
    /// A timer the machine set fired.
    Timer(Timer),
    /// Another connection claimed this session's Client Identifier: this one steps down with
    /// DISCONNECT 0x8E ([MQTT-3.1.4-3], report R3, Sessions).
    StepDown {
        /// Whether the new connection has Clean Start 1, which ends the session and so
        /// publishes the will at once.
        session_ends: bool,
    },
    /// The client ended a data stream (docs/spec/mqtt-over-quic.md, section 2.4).
    StreamEnded {
        /// The QUIC stream id of the data stream.
        stream: u64,
        /// How it ended.
        end: StreamEnd,
    },
    /// The network connection closed, or the control stream ended, without the machine asking
    /// for it: the client is gone.
    TransportClosed,
    /// The edge or an operator ends the connection.
    Shutdown(Shutdown),
}

impl Input {
    /// A packet from the client on the control stream, outside 0-RTT data.
    pub fn packet(packet: impl Into<Packet>) -> Self {
        Self::Packet {
            stream: StreamId::Control,
            packet: packet.into(),
            early: false,
        }
    }
}

/// The authenticator's answer.
#[derive(Clone, PartialEq, Eq)]
pub enum AuthResult {
    /// The client is who it says it is. With enhanced authentication, `data` goes back to it as
    /// the Authentication Data of the CONNACK, or of the AUTH that ends a re-authentication.
    Success {
        /// Authentication Data for the client.
        data: Option<Bytes>,
    },
    /// Another step of enhanced authentication: `data` goes to the client in an AUTH with
    /// reason code 0x18 ([MQTT-4.12.0-2]). Only for a client that sent an Authentication
    /// Method.
    Continue {
        /// Authentication Data for the client.
        data: Option<Bytes>,
    },
    /// The client is refused with this reason code: 0x86, 0x87, 0x8A or 0x8C, say. A code
    /// below 0x80 is taken as 0x80.
    Failure(ConnectReasonCode),
}

/// Debug output that never shows authentication data: a credential must not reach a log
/// through `{:?}`.
impl std::fmt::Debug for AuthResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success { data } => f
                .debug_struct("Success")
                .field("data", &data.as_ref().map(|_| "<redacted>"))
                .finish(),
            Self::Continue { data } => f
                .debug_struct("Continue")
                .field("data", &data.as_ref().map(|_| "<redacted>"))
                .finish(),
            Self::Failure(code) => f.debug_tuple("Failure").field(code).finish(),
        }
    }
}

/// The authorizer's decision on one action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Decision {
    /// The action is allowed.
    Allow,
    /// The action is refused: 0x87 in the PUBACK, PUBREC or SUBACK (report R1, D2), or a
    /// CONNACK 0x87 for a Will Message.
    Deny,
}

/// The log's answer to a claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimResult {
    /// The claim committed, and the connection now owns the Client Identifier. With Clean
    /// Start 0, `session` is the session it resumes, if the identifier had one.
    Claimed {
        /// The session the claim found.
        session: Option<SessionState>,
    },
    /// An identifier the server assigned is already in use (report R1, O9). The machine draws
    /// another and claims again.
    Taken,
    /// The claim failed: the CONNECT is refused with this reason code, 0x88 or 0x89 say.
    Refused(ConnectReasonCode),
}

/// What became of a published message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PublishOutcome {
    /// The message is durable and routed. `matched` says whether any subscription took it: a
    /// message that is not retained and that nothing matched is acknowledged with 0x10 (report
    /// R1, O26).
    Accepted {
        /// Whether any subscription, durable or live, matched it.
        matched: bool,
    },
    /// The message was refused for a limit, as the retained store refuses one (report R1,
    /// D15): 0x97.
    QuotaExceeded,
    /// The message could not be made durable: 0x80.
    Failed,
}

/// A message for this session, from the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// The message, its topic mounted, as the rest of the cluster sees it.
    pub message: Message,
    /// The mounted filters of this session's subscriptions that matched it, as its
    /// [`Interest`](crate::Interest) effects named them. The machine checks each again
    /// against the client's own filter and the topic with the mountpoint taken off (report R2,
    /// rule 6), and ignores one it no longer holds ([MQTT-3.10.4-2]).
    pub subscriptions: Vec<TopicFilter>,
}

/// How the client ended a data stream (docs/spec/mqtt-over-quic.md, section 2.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StreamEnd {
    /// The client finished or reset its sending side: it sends nothing more on the stream.
    ClientFinished,
    /// The client stopped the server's sending side (STOP_SENDING): the server can send
    /// nothing more on it.
    ServerStopped,
}

/// Why the edge or an operator ends a connection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Shutdown {
    /// The edge is shutting down: DISCONNECT 0x8B, or CONNACK 0x88 before acceptance.
    ServerShuttingDown,
    /// The client should use another server for now, as on a drain (report R3, Sessions):
    /// DISCONNECT or CONNACK 0x9C.
    UseAnotherServer {
        /// The Server Reference to send, if there is one.
        server_reference: Option<String>,
    },
    /// The client should use another server from now on: DISCONNECT or CONNACK 0x9D.
    ServerMoved {
        /// The Server Reference to send, if there is one.
        server_reference: Option<String>,
    },
    /// An operator disconnected the client: DISCONNECT 0x98, or CONNACK 0x87 before
    /// acceptance.
    AdministrativeAction,
}
