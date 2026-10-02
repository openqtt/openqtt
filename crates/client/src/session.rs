//! The client's half of a session, kept between network connections (section 4.1).

use std::collections::{BTreeSet, VecDeque};

use openqtt_codec::{PacketId, Publish};

use crate::ids::PacketIds;

/// What the client keeps of an MQTT session between network connections (section 4.1): the
/// QoS 1 and 2 messages it sent that are not completely acknowledged, the QoS 2 messages it
/// received whose PUBREL has not arrived, and the Packet Identifiers those hold.
///
/// [`Client::disconnect`](crate::Client::disconnect) hands it back. Passing it to
/// [`ConnectOptions::resume`](crate::ConnectOptions::resume) connects with Clean Start 0; if
/// the server still has the session it answers Session Present 1, and the client resends the
/// unacknowledged messages in the order it first sent them ([MQTT-4.6.0-1]). If the server
/// answers Session Present 0, the client discards this state ([MQTT-3.2.2-5]).
///
/// [`Session::new`] is a session with nothing in flight. Connecting with Clean Start 0 and no
/// session means the client holds no state, so a server answering Session Present 1 makes it
/// close the connection ([MQTT-3.2.2-4]).
#[derive(Debug, Clone)]
pub struct Session {
    /// The Client Identifier the session is kept under: the one sent in CONNECT, or the one
    /// the server assigned.
    pub(crate) client_id: String,
    /// Messages sent at QoS 1 or 2 and not yet completely acknowledged, in the order they
    /// were first sent.
    pub(crate) outbound: VecDeque<Outbound>,
    /// Identifiers of QoS 2 messages received whose PUBREL has not arrived. Each was
    /// delivered once; a repeat of it before PUBREL is not delivered again.
    pub(crate) inbound: BTreeSet<u16>,
    /// The Packet Identifiers this end has in use.
    pub(crate) ids: PacketIds,
}

/// A QoS 1 or 2 message the client sent, and the acknowledgement it waits for.
#[derive(Debug, Clone)]
pub(crate) struct Outbound {
    /// Its Packet Identifier.
    pub(crate) id: PacketId,
    /// The PUBLISH, kept to resend on a new connection.
    pub(crate) publish: Publish,
    /// What the client waits for next.
    pub(crate) stage: Stage,
    /// Whether it went out on the current connection. A resumed message waits for a free
    /// slot of the server's Receive Maximum before it is sent again.
    pub(crate) sent: bool,
}

/// What a QoS 1 or 2 message waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    /// PUBACK, for QoS 1.
    Acknowledgement,
    /// PUBREC, for QoS 2.
    Receipt,
    /// PUBCOMP, for QoS 2 once PUBREL went out.
    Completion,
}

impl Session {
    /// A session for `client_id` with nothing in flight.
    pub fn new(client_id: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            outbound: VecDeque::new(),
            inbound: BTreeSet::new(),
            ids: PacketIds::new(),
        }
    }

    /// The Client Identifier the session is kept under.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// How many QoS 1 and 2 messages wait for an acknowledgement, and are sent again when the
    /// session resumes.
    pub fn unacknowledged(&self) -> usize {
        self.outbound.len()
    }

    /// Discards everything in flight, as when the server has no session ([MQTT-3.2.2-5]).
    pub(crate) fn clear(&mut self) {
        self.outbound.clear();
        self.inbound.clear();
        self.ids = PacketIds::new();
    }

    /// The position of the message waiting on `id`.
    pub(crate) fn position(&self, id: PacketId) -> Option<usize> {
        self.outbound.iter().position(|outbound| outbound.id == id)
    }
}
