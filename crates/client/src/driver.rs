//! The connection task: everything the client does on an open connection, from the CONNACK
//! to the close.

use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BytesMut};
use openqtt_codec::{
    ConnAck, Connect, Decoder, Disconnect, DisconnectReasonCode, MAX_PACKET_SIZE, Packet, PubAck,
    PubComp, PubCompReasonCode, PubRec, PubRel, PubRelReasonCode, Publish, QoS, Sender, SubAck,
    Subscribe, SubscribeProperties, Subscription, UnsubAck, Unsubscribe, UnsubscribeProperties,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep_until, timeout};

use crate::client::{CloseReason, Discard, Event, Published, Shared};
use crate::session::{Outbound, Stage};
use crate::transport::{CloseCode, Link, LinkHandle};
use crate::{Error, Session};

/// How long the client waits after its DISCONNECT for it to be written, then for the server
/// to close the connection, and then for the close to reach the server. Closing a QUIC
/// connection discards stream data not yet acknowledged, so closing at once could lose the
/// DISCONNECT.
const LINGER: Duration = Duration::from_secs(1);

/// How many bytes may wait to be written before publications wait too. A server that stops
/// reading holds the client to about this much, besides what callers have in hand.
pub(crate) const WRITE_LIMIT: usize = 64 * 1024;

/// How many publications may wait in the connection task, for room to write or for a slot of
/// the server's Receive Maximum. Past it, the task takes no more from the publication channel,
/// so further callers wait with their payloads in hand, and the task goes on with everything
/// else.
pub(crate) const QUEUE_LIMIT: usize = 64;

/// A publication from a [`Client`](crate::Client) handle. Publications have a channel of their
/// own, which the connection task stops taking from when its queue is full; the other calls
/// keep theirs.
pub(crate) struct Publication {
    pub(crate) publish: Publish,
    pub(crate) reply: oneshot::Sender<Result<Published, Error>>,
}

/// A call from a [`Client`](crate::Client) handle, other than a publication.
pub(crate) enum Command {
    /// Subscribe.
    Subscribe {
        subscriptions: Vec<Subscription>,
        properties: SubscribeProperties,
        reply: oneshot::Sender<Result<SubAck, Error>>,
    },
    /// Unsubscribe.
    Unsubscribe {
        filters: Vec<String>,
        properties: UnsubscribeProperties,
        reply: oneshot::Sender<Result<UnsubAck, Error>>,
    },
    /// Send DISCONNECT and close, or report why the DISCONNECT cannot be sent.
    Disconnect {
        disconnect: Disconnect,
        reply: oneshot::Sender<Result<Session, Error>>,
    },
}

/// What CONNECT and CONNACK settled.
pub(crate) struct Negotiated {
    /// The Keep Alive in use: the server's if it sent one ([MQTT-3.1.2-21]), else the
    /// client's; `None` when it is 0.
    keep_alive: Option<Duration>,
    /// How long to wait for PINGRESP.
    ping_timeout: Duration,
    /// The client's Receive Maximum.
    receive_maximum: u16,
    /// The client's Topic Alias Maximum.
    topic_alias_maximum: u16,
    /// The server's Receive Maximum: how many QoS 1 and 2 messages may wait for it.
    server_receive_maximum: u16,
    /// The server's Maximum Packet Size.
    server_maximum_packet_size: u32,
    /// The server's Topic Alias Maximum.
    server_topic_alias_maximum: u16,
    /// The server's Maximum QoS.
    server_maximum_qos: QoS,
    /// Whether the server takes retained messages.
    server_retain_available: bool,
}

impl Negotiated {
    /// Reads what the CONNECT asked for and the CONNACK granted; absent properties take the
    /// defaults section 3.2.2.3 gives them.
    pub(crate) fn new(
        connect: &Connect,
        connack: &ConnAck,
        ping_timeout: Option<Duration>,
    ) -> Self {
        let seconds = connack
            .properties
            .server_keep_alive
            .unwrap_or(connect.keep_alive);
        let keep_alive = (seconds > 0).then(|| Duration::from_secs(u64::from(seconds)));
        let properties = &connack.properties;
        Self {
            keep_alive,
            ping_timeout: ping_timeout.or(keep_alive).unwrap_or(LINGER),
            receive_maximum: connect
                .properties
                .receive_maximum
                .map_or(u16::MAX, NonZeroU16::get),
            topic_alias_maximum: connect.properties.topic_alias_maximum.unwrap_or(0),
            server_receive_maximum: properties.receive_maximum.map_or(u16::MAX, NonZeroU16::get),
            server_maximum_packet_size: properties
                .maximum_packet_size
                .map_or(MAX_PACKET_SIZE, std::num::NonZeroU32::get),
            server_topic_alias_maximum: properties.topic_alias_maximum.unwrap_or(0),
            server_maximum_qos: properties.maximum_qos.unwrap_or(QoS::ExactlyOnce),
            server_retain_available: properties.retain_available.unwrap_or(true),
        }
    }
}

/// Who waits for the acknowledgement of a packet the client sent.
enum Reply {
    /// A QoS 1 or 2 PUBLISH, with its PUBREC once that arrived.
    Publish {
        reply: oneshot::Sender<Result<Published, Error>>,
        pubrec: Option<PubRec>,
    },
    /// A SUBSCRIBE.
    Subscribe(oneshot::Sender<Result<SubAck, Error>>),
    /// An UNSUBSCRIBE.
    Unsubscribe(oneshot::Sender<Result<UnsubAck, Error>>),
}

/// Why the connection task stopped serving.
enum Stop {
    /// The client sent DISCONNECT, at a caller's request or because nothing holds the
    /// connection any more.
    Disconnected(Option<oneshot::Sender<Result<Session, Error>>>),
    /// The server sent DISCONNECT.
    ByServer(Disconnect),
    /// The server broke the protocol; the client sent DISCONNECT with this code.
    Protocol {
        reason_code: DisconnectReasonCode,
        detail: String,
    },
    /// The connection failed.
    Lost(String),
}

/// The task that owns an open connection.
///
/// It never waits on a write inside a handler: handlers encode into `write_buf`, and the
/// serving loop writes it out while it goes on reading, taking calls and keeping time, so a
/// server that stops reading cannot stop the client from noticing.
pub(crate) struct Driver {
    reader: Box<dyn AsyncRead + Send + Unpin>,
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    handle: Box<dyn LinkHandle>,
    decoder: Decoder,
    read_buf: BytesMut,
    /// Encoded packets waiting to be written, in order.
    write_buf: BytesMut,
    session: Session,
    negotiated: Negotiated,
    /// How many outbound messages hold a slot of the server's Receive Maximum.
    in_flight: usize,
    /// Publications waiting to be sent: for room in the write buffer, and at QoS 1 and 2 for a
    /// slot of the server's Receive Maximum too ([MQTT-3.3.4-7]).
    queued: VecDeque<(Publish, oneshot::Sender<Result<Published, Error>>)>,
    /// Callers waiting for an acknowledgement, by Packet Identifier.
    replies: HashMap<u16, Reply>,
    /// Topic Aliases the server set on this connection ([MQTT-3.3.2-7]: none carry over).
    inbound_aliases: HashMap<u16, String>,
    /// Topic Aliases this client set on this connection.
    outbound_aliases: HashMap<u16, String>,
    /// When the client last wrote anything.
    last_sent: Instant,
    /// When an unanswered PINGREQ was queued.
    ping_sent: Option<Instant>,
    commands: mpsc::Receiver<Command>,
    commands_open: bool,
    publications: mpsc::Receiver<Publication>,
    publications_open: bool,
    events: mpsc::Sender<Event>,
    events_closed: bool,
    /// Events waiting for room in the channel; the client reads nothing more meanwhile.
    outbox: VecDeque<Event>,
    /// The CONNACK, for the packet log.
    connack: Option<Packet>,
    /// Whether every packet received goes to the application too.
    packet_log: bool,
    shared: Arc<Shared>,
}

impl Driver {
    #[expect(
        clippy::too_many_arguments,
        reason = "called once, by Client::connect, with what the handshake produced"
    )]
    pub(crate) fn new(
        link: Link,
        read_buf: BytesMut,
        decoder: Decoder,
        session: Session,
        negotiated: Negotiated,
        commands: mpsc::Receiver<Command>,
        publications: mpsc::Receiver<Publication>,
        events: mpsc::Sender<Event>,
        connack: Option<Packet>,
        shared: Arc<Shared>,
    ) -> Self {
        Self {
            reader: link.reader,
            writer: link.writer,
            handle: link.handle,
            decoder,
            read_buf,
            write_buf: BytesMut::new(),
            session,
            negotiated,
            in_flight: 0,
            queued: VecDeque::new(),
            replies: HashMap::new(),
            inbound_aliases: HashMap::new(),
            outbound_aliases: HashMap::new(),
            last_sent: Instant::now(),
            ping_sent: None,
            commands,
            commands_open: true,
            publications,
            publications_open: true,
            events,
            events_closed: false,
            outbox: VecDeque::new(),
            packet_log: connack.is_some(),
            connack,
            shared,
        }
    }

    /// Serves the connection until it ends, then closes it, fails whatever still waits, and
    /// hands back the session.
    pub(crate) async fn run(mut self) {
        let stop = match self.serve().await {
            Ok(never) => match never {},
            Err(stop) => stop,
        };
        let (reason, code, reply) = match stop {
            Stop::Disconnected(reply) => {
                self.drain().await;
                self.linger().await;
                (CloseReason::Disconnected, CloseCode::NoError, reply)
            }
            Stop::ByServer(disconnect) => {
                (CloseReason::ByServer(disconnect), CloseCode::NoError, None)
            }
            Stop::Protocol {
                reason_code,
                detail,
            } => {
                self.drain().await;
                self.linger().await;
                (
                    CloseReason::ProtocolError {
                        reason_code,
                        detail,
                    },
                    CloseCode::ProtocolError,
                    None,
                )
            }
            Stop::Lost(detail) => (CloseReason::Lost(detail), CloseCode::InternalError, None),
        };
        self.handle.close(code);
        // Bounded: a peer that is gone never acknowledges the close.
        drop(timeout(LINGER, self.handle.closed()).await);

        for (id, waiting) in self.replies.drain() {
            match waiting {
                Reply::Publish { reply, .. } => drop(reply.send(Err(Error::Closed))),
                Reply::Subscribe(reply) => drop(reply.send(Err(Error::Closed))),
                Reply::Unsubscribe(reply) => drop(reply.send(Err(Error::Closed))),
            }
            // SUBSCRIBE and UNSUBSCRIBE are not session state; a PUBLISH keeps its
            // identifier for as long as it stays in the session.
            if let Some(id) = NonZeroU16::new(id)
                && self.session.position(id).is_none()
            {
                self.session.ids.release(id);
            }
        }
        for (_, reply) in self.queued.drain(..) {
            drop(reply.send(Err(Error::Closed)));
        }
        for outbound in &mut self.session.outbound {
            outbound.sent = false;
        }

        let Self {
            session,
            events,
            events_closed,
            mut outbox,
            shared,
            ..
        } = self;
        match reply {
            Some(reply) => {
                if let Err(Ok(session)) = reply.send(Ok(session)) {
                    shared.keep_session(session);
                }
            }
            None => shared.keep_session(session),
        }
        if !events_closed {
            outbox.push_back(Event::Closed(reason));
            // The connection is gone; what is left to deliver waits for the application on a
            // task of its own.
            tokio::spawn(async move {
                for event in outbox {
                    if events.send(event).await.is_err() {
                        break;
                    }
                }
            });
        }
    }

    /// Runs until the connection ends; the error says why.
    async fn serve(&mut self) -> Result<Infallible, Stop> {
        if let Some(connack) = self.connack.take() {
            self.emit(Event::Received(connack));
        }
        self.resume()?;
        // Packets that came in the same read as the CONNACK.
        self.decode_buffered()?;
        loop {
            // A publication whose caller gave up, by a timeout or by dropping the call, is
            // not sent and not held.
            self.queued.retain(|(_, reply)| !reply.is_closed());
            let ping_at = self.ping_deadline();
            // A due PINGREQ, or an overdue PINGRESP, comes before anything else: the timer
            // branch below is reached only when nothing else is ready, and a server that
            // keeps sending keeps the reads ready.
            if ping_at.is_some_and(|at| at <= Instant::now()) {
                self.keep_alive_due()?;
                continue;
            }
            let pending = !self.outbox.is_empty();
            let writing = !self.write_buf.is_empty();
            let (events, outbox) = (&self.events, &mut self.outbox);
            // Hands one waiting event over as soon as the channel has room.
            let deliver = async move {
                let permit = events.reserve().await.map_err(drop)?;
                if let Some(event) = outbox.pop_front() {
                    permit.send(event);
                }
                Ok::<(), ()>(())
            };
            tokio::select! {
                biased;
                delivered = deliver, if pending => {
                    if delivered.is_err() {
                        // The application dropped its Events.
                        self.events_closed = true;
                        self.outbox.clear();
                        if !self.commands_open {
                            return Err(self.abandon());
                        }
                    }
                    // Packets that arrived while the application was behind.
                    self.decode_buffered()?;
                }
                // The application dropped its Events, whether or not one was waiting.
                () = self.events.closed(), if !self.events_closed => {
                    self.events_closed = true;
                    self.outbox.clear();
                    if !self.commands_open {
                        return Err(self.abandon());
                    }
                }
                publication = self.publications.recv(),
                    if self.publications_open && self.queued.len() < QUEUE_LIMIT =>
                {
                    match publication {
                        Some(Publication { publish, reply }) => self.publish(publish, reply),
                        None => self.publications_open = false,
                    }
                }
                command = self.commands.recv(), if self.commands_open => match command {
                    Some(command) => self.command(command)?,
                    None => {
                        self.commands_open = false;
                        if self.events_closed {
                            return Err(self.abandon());
                        }
                    }
                },
                // Whatever the server can take, while everything else goes on.
                written = self.writer.write(&self.write_buf), if writing => match written {
                    Ok(0) => return Err(Stop::Lost("the control stream takes no more data".into())),
                    Ok(count) => {
                        self.write_buf.advance(count);
                        self.last_sent = Instant::now();
                        self.pump();
                    }
                    Err(error) => return Err(Stop::Lost(error.to_string())),
                },
                // Nothing more is read while events wait for the application.
                read = self.reader.read_buf(&mut self.read_buf), if !pending => match read {
                    Ok(0) => return Err(Stop::Lost("the server closed the control stream".into())),
                    Ok(_) => self.decode_buffered()?,
                    Err(error) => return Err(Stop::Lost(error.to_string())),
                },
                // Wakes an idle connection when the next PINGREQ falls due; the check at the
                // top of the loop serves it.
                () = sleep_until(ping_at.unwrap_or_else(Instant::now)), if ping_at.is_some() => {}
            }
        }
    }

    /// Queues an event for the application, unless it stopped listening.
    fn emit(&mut self, event: Event) {
        if !self.events_closed {
            self.outbox.push_back(event);
        }
    }

    /// Nothing holds the connection any more: DISCONNECT as a normal close.
    fn abandon(&mut self) -> Stop {
        drop(self.encode(&Packet::Disconnect(Disconnect::default())));
        Stop::Disconnected(None)
    }

    /// Writes out what waits to be written, the DISCONNECT last, for as long as the server
    /// takes it within [`LINGER`].
    async fn drain(&mut self) {
        let (writer, pending) = (&mut self.writer, &self.write_buf);
        drop(timeout(LINGER, writer.write_all(pending)).await);
        self.write_buf.clear();
    }

    /// Waits a short while for the server to close the connection after a DISCONNECT, so the
    /// DISCONNECT is delivered before the close.
    async fn linger(&mut self) {
        drop(timeout(LINGER, self.writer.shutdown()).await);
        let reader = &mut self.reader;
        let mut sink = [0; 256];
        drop(
            timeout(LINGER, async {
                while let Ok(read) = reader.read(&mut sink).await {
                    if read == 0 {
                        break;
                    }
                }
            })
            .await,
        );
    }

    /// When the next PINGREQ is due, or the answer to the last one overdue.
    fn ping_deadline(&self) -> Option<Instant> {
        let keep_alive = self.negotiated.keep_alive?;
        Some(match self.ping_sent {
            Some(sent) => sent + self.negotiated.ping_timeout,
            None => self.last_sent + keep_alive,
        })
    }

    /// The Keep Alive passed with nothing sent: PINGREQ ([MQTT-3.1.2-20]), unless the last
    /// one is still unanswered, in which case the server is gone (section 3.1.2.10). A
    /// PINGREQ the server never takes is unanswered too.
    fn keep_alive_due(&mut self) -> Result<(), Stop> {
        if self.ping_sent.is_some() {
            return Err(Stop::Lost("the server did not answer PINGREQ".into()));
        }
        self.send(&Packet::PingReq)?;
        self.ping_sent = Some(Instant::now());
        Ok(())
    }

    /// Resends what a resumed session left in flight ([MQTT-4.4.0-1]): PUBREL for the
    /// messages waiting on PUBCOMP, then the PUBLISH packets in the order they were first sent
    /// ([MQTT-4.6.0-1]), with DUP set, as the server's Receive Maximum allows.
    fn resume(&mut self) -> Result<(), Stop> {
        let mut releases = Vec::new();
        for outbound in &mut self.session.outbound {
            outbound.sent = outbound.stage == Stage::Completion;
            if outbound.sent {
                releases.push(outbound.id);
            }
        }
        self.in_flight = releases.len();
        for id in releases {
            self.send(&Packet::PubRel(PubRel::new(id)))?;
        }
        self.pump();
        Ok(())
    }

    /// Whether publications may be encoded: the write buffer is under [`WRITE_LIMIT`].
    fn room(&self) -> bool {
        self.write_buf.len() < WRITE_LIMIT
    }

    /// Sends the publications that can go, while the write buffer has room: resumed QoS 1 and 2
    /// messages first, in their original order, then queued ones in the order they came, QoS
    /// 1 and 2 only while the server's Receive Maximum has a slot ([MQTT-3.3.4-7]) and QoS 0
    /// whatever it is.
    fn pump(&mut self) {
        let maximum = usize::from(self.negotiated.server_receive_maximum);
        while self.room() {
            let quota = self.in_flight < maximum;
            if quota && let Some(index) = self.session.outbound.iter().position(|o| !o.sent) {
                self.resend(index);
                continue;
            }
            let Some(position) = self
                .queued
                .iter()
                .position(|(publish, _)| quota || publish.qos == QoS::AtMostOnce)
            else {
                break;
            };
            let Some((publish, reply)) = self.queued.remove(position) else {
                break;
            };
            self.send_publish(publish, reply);
        }
    }

    /// Sends a resumed message again, with DUP, or discards it when the server's CONNACK no
    /// longer allows it.
    fn resend(&mut self, index: usize) {
        let mut publish = self.session.outbound[index].publish.clone();
        publish.dup = true;
        let refusal = self.forbidden(&publish).or_else(|| {
            self.encode(&Packet::Publish(publish))
                .err()
                .map(Discard::Invalid)
        });
        match refusal {
            // The server would refuse it now; it leaves the session unsent, and the
            // application hears of it.
            Some(reason) => {
                if let Some(outbound) = self.session.outbound.remove(index) {
                    self.session.ids.release(outbound.id);
                    self.emit(Event::Discarded {
                        publish: outbound.publish,
                        reason,
                    });
                }
            }
            None => {
                self.session.outbound[index].sent = true;
                self.in_flight += 1;
            }
        }
    }

    /// Sends a new publication: QoS 0 as it is, QoS 1 and 2 with a Packet Identifier and a
    /// place in the session until it is acknowledged.
    fn send_publish(
        &mut self,
        mut publish: Publish,
        reply: oneshot::Sender<Result<Published, Error>>,
    ) {
        if reply.is_closed() {
            // Its caller gave up before it could go.
            return;
        }
        if publish.qos == QoS::AtMostOnce {
            let result = self.encode_publish(&publish).map(|_| Published::AtMostOnce);
            drop(reply.send(result));
            return;
        }
        let Some(id) = self.session.ids.allocate() else {
            drop(reply.send(Err(Error::PacketIdsExhausted)));
            return;
        };
        publish.packet_id = Some(id);
        let stored = match self.encode_publish(&publish) {
            Ok(stored) => stored,
            Err(error) => {
                self.session.ids.release(id);
                drop(reply.send(Err(error)));
                return;
            }
        };
        let stage = if publish.qos == QoS::AtLeastOnce {
            Stage::Acknowledgement
        } else {
            Stage::Receipt
        };
        self.session.outbound.push_back(Outbound {
            id,
            publish: stored,
            stage,
            sent: true,
        });
        self.in_flight += 1;
        self.replies.insert(
            id.get(),
            Reply::Publish {
                reply,
                pubrec: None,
            },
        );
    }

    /// Removes a completed message from the session and frees its identifier and its slot.
    fn finish_outbound(&mut self, index: usize) {
        if let Some(outbound) = self.session.outbound.remove(index) {
            if outbound.sent {
                self.in_flight = self.in_flight.saturating_sub(1);
            }
            self.session.ids.release(outbound.id);
        }
    }

    /// Checks a packet against what a client may send and the server's Maximum Packet Size,
    /// and appends it to the write buffer.
    fn encode(&mut self, packet: &Packet) -> Result<(), openqtt_codec::Error> {
        packet.check_sender(Sender::Client)?;
        packet.encode_within(
            &mut self.write_buf,
            self.negotiated.server_maximum_packet_size,
        )
    }

    /// What a server that sent this CONNACK no longer allows of a message the session held:
    /// a QoS above its Maximum QoS ([MQTT-3.2.2-11]), or RETAIN without Retain Available
    /// ([MQTT-3.2.2-14]).
    fn forbidden(&self, publish: &Publish) -> Option<Discard> {
        let negotiated = &self.negotiated;
        if publish.qos > negotiated.server_maximum_qos {
            Some(Discard::QosNotSupported {
                maximum: negotiated.server_maximum_qos,
            })
        } else if publish.retain && !negotiated.server_retain_available {
            Some(Discard::RetainNotSupported)
        } else {
            None
        }
    }

    /// Queues a packet the client makes itself, which never fails to encode unless the
    /// server's limits leave no room for it.
    fn send(&mut self, packet: &Packet) -> Result<(), Stop> {
        self.encode(packet)
            .map_err(|error| Stop::Lost(format!("cannot send {}: {error}", packet.packet_type())))
    }

    /// Encodes a PUBLISH, checking its Topic Alias against what this connection mapped, and
    /// returns the copy to keep in the session: with the topic written out and no alias, since
    /// no alias outlives its connection ([MQTT-3.3.2-7]).
    fn encode_publish(&mut self, publish: &Publish) -> Result<Publish, Error> {
        let mut stored = publish.clone();
        if let Some(alias) = publish.properties.topic_alias {
            let maximum = self.negotiated.server_topic_alias_maximum;
            if publish.topic.is_empty() {
                match self.outbound_aliases.get(&alias.get()) {
                    Some(topic) => stored.topic.clone_from(topic),
                    None => {
                        return Err(Error::TopicAlias {
                            alias: alias.get(),
                            maximum,
                        });
                    }
                }
            }
            stored.properties.topic_alias = None;
        }
        self.encode(&Packet::Publish(publish.clone()))
            .map_err(Error::Invalid)?;
        if let Some(alias) = publish.properties.topic_alias
            && !publish.topic.is_empty()
        {
            self.outbound_aliases
                .insert(alias.get(), publish.topic.clone());
        }
        Ok(stored)
    }

    /// Handles a call from a handle.
    fn command(&mut self, command: Command) -> Result<(), Stop> {
        match command {
            Command::Subscribe {
                subscriptions,
                properties,
                reply,
            } => {
                let Some(id) = self.session.ids.allocate() else {
                    drop(reply.send(Err(Error::PacketIdsExhausted)));
                    return Ok(());
                };
                let packet = Packet::Subscribe(Subscribe {
                    packet_id: id,
                    properties,
                    subscriptions,
                });
                match self.encode(&packet) {
                    Ok(()) => {
                        self.replies.insert(id.get(), Reply::Subscribe(reply));
                    }
                    Err(error) => {
                        self.session.ids.release(id);
                        drop(reply.send(Err(Error::Invalid(error))));
                    }
                }
                Ok(())
            }
            Command::Unsubscribe {
                filters,
                properties,
                reply,
            } => {
                let Some(id) = self.session.ids.allocate() else {
                    drop(reply.send(Err(Error::PacketIdsExhausted)));
                    return Ok(());
                };
                let packet = Packet::Unsubscribe(Unsubscribe {
                    packet_id: id,
                    properties,
                    filters,
                });
                match self.encode(&packet) {
                    Ok(()) => {
                        self.replies.insert(id.get(), Reply::Unsubscribe(reply));
                    }
                    Err(error) => {
                        self.session.ids.release(id);
                        drop(reply.send(Err(Error::Invalid(error))));
                    }
                }
                Ok(())
            }
            Command::Disconnect { disconnect, reply } => {
                // A Reason String or User Property that would take the DISCONNECT past the
                // server's Maximum Packet Size is left out ([MQTT-3.14.2-3], [MQTT-3.14.2-4]).
                // A DISCONNECT that still cannot be sent is the caller's to fix, and the
                // connection carries on as it was.
                let mut packet = Packet::Disconnect(disconnect);
                let encoded = packet
                    .fit_within(self.negotiated.server_maximum_packet_size)
                    .and_then(|_| self.encode(&packet));
                if let Err(error) = encoded {
                    drop(reply.send(Err(Error::Invalid(error))));
                    return Ok(());
                }
                Err(Stop::Disconnected(Some(reply)))
            }
        }
    }

    /// Takes a publication: dropped when its caller already gave up, refused at once when it
    /// breaks a limit the server announced, otherwise queued for [`pump`](Self::pump).
    fn publish(&mut self, mut publish: Publish, reply: oneshot::Sender<Result<Published, Error>>) {
        if reply.is_closed() {
            return;
        }
        let negotiated = &self.negotiated;
        let refusal = if publish.qos > negotiated.server_maximum_qos {
            // [MQTT-3.2.2-11]
            Some(Error::QosNotSupported {
                requested: publish.qos,
                maximum: negotiated.server_maximum_qos,
            })
        } else if publish.retain && !negotiated.server_retain_available {
            // [MQTT-3.2.2-14]
            Some(Error::RetainNotSupported)
        } else {
            // [MQTT-3.2.2-17] [MQTT-3.2.2-18] [MQTT-3.3.2-9]
            publish
                .properties
                .topic_alias
                .filter(|alias| alias.get() > negotiated.server_topic_alias_maximum)
                .map(|alias| Error::TopicAlias {
                    alias: alias.get(),
                    maximum: negotiated.server_topic_alias_maximum,
                })
        };
        if let Some(error) = refusal {
            drop(reply.send(Err(error)));
            return;
        }
        publish.dup = false;
        if publish.qos == QoS::AtMostOnce {
            publish.packet_id = None;
        }
        self.queued.push_back((publish, reply));
        self.pump();
    }

    /// Handles the whole packets in the read buffer, until one of them leaves an event for
    /// the application: what follows waits until the application has taken it, so a slow
    /// application holds back the acknowledgements of what it has not yet been given room for.
    fn decode_buffered(&mut self) -> Result<(), Stop> {
        while self.outbox.is_empty() {
            match self.decoder.decode(&mut self.read_buf) {
                Ok(Some(packet)) => self.on_packet(packet)?,
                Ok(None) => return Ok(()),
                Err(error) => {
                    return Err(
                        self.protocol_error(error.disconnect_reason_code(), error.to_string())
                    );
                }
            }
        }
        Ok(())
    }

    /// Answers a server that broke the protocol with DISCONNECT, as section 4.13 asks.
    fn protocol_error(&mut self, reason_code: DisconnectReasonCode, detail: String) -> Stop {
        drop(self.encode(&Packet::Disconnect(Disconnect {
            reason_code,
            ..Disconnect::default()
        })));
        Stop::Protocol {
            reason_code,
            detail,
        }
    }

    /// Handles one packet from the server.
    fn on_packet(&mut self, packet: Packet) -> Result<(), Stop> {
        if self.packet_log {
            self.emit(Event::Received(packet.clone()));
        }
        match packet {
            Packet::Publish(publish) => self.on_publish(publish),
            Packet::PubAck(puback) => {
                self.on_puback(puback);
                Ok(())
            }
            Packet::PubRec(pubrec) => self.on_pubrec(pubrec),
            Packet::PubRel(pubrel) => {
                let known = self.session.inbound.remove(&pubrel.packet_id.get());
                let reason_code = if known {
                    PubCompReasonCode::Success
                } else {
                    PubCompReasonCode::PacketIdentifierNotFound
                };
                self.send(&Packet::PubComp(PubComp {
                    reason_code,
                    ..PubComp::new(pubrel.packet_id)
                }))
            }
            Packet::PubComp(pubcomp) => {
                self.on_pubcomp(pubcomp);
                Ok(())
            }
            Packet::SubAck(suback) => {
                let id = suback.packet_id;
                match self.replies.remove(&id.get()) {
                    Some(Reply::Subscribe(reply)) => {
                        self.session.ids.release(id);
                        drop(reply.send(Ok(suback)));
                    }
                    Some(other) => {
                        self.replies.insert(id.get(), other);
                    }
                    None => {}
                }
                Ok(())
            }
            Packet::UnsubAck(unsuback) => {
                let id = unsuback.packet_id;
                match self.replies.remove(&id.get()) {
                    Some(Reply::Unsubscribe(reply)) => {
                        self.session.ids.release(id);
                        drop(reply.send(Ok(unsuback)));
                    }
                    Some(other) => {
                        self.replies.insert(id.get(), other);
                    }
                    None => {}
                }
                Ok(())
            }
            Packet::PingResp => {
                self.ping_sent = None;
                Ok(())
            }
            Packet::Disconnect(disconnect) => Err(Stop::ByServer(disconnect)),
            // At most one CONNACK per connection ([MQTT-3.2.0-2]); no AUTH without an
            // Authentication Method ([MQTT-4.12.0-6]); the rest only a client sends.
            other => Err(self.protocol_error(
                DisconnectReasonCode::ProtocolError,
                format!("the server sent {}", other.packet_type()),
            )),
        }
    }

    /// An Application Message from the server.
    fn on_publish(&mut self, mut publish: Publish) -> Result<(), Stop> {
        if let Some(alias) = publish.properties.topic_alias {
            if alias.get() > self.negotiated.topic_alias_maximum {
                return Err(self.protocol_error(
                    DisconnectReasonCode::TopicAliasInvalid,
                    format!("Topic Alias {alias} is above the client's maximum"),
                ));
            }
            if publish.topic.is_empty() {
                // [MQTT-3.3.2-10]
                match self.inbound_aliases.get(&alias.get()) {
                    Some(topic) => publish.topic.clone_from(topic),
                    None => {
                        return Err(self.protocol_error(
                            DisconnectReasonCode::ProtocolError,
                            format!("Topic Alias {alias} was never set"),
                        ));
                    }
                }
            } else {
                self.inbound_aliases
                    .insert(alias.get(), publish.topic.clone());
            }
        }
        match (publish.qos, publish.packet_id) {
            (QoS::AtMostOnce, _) => self.emit(Event::Message(publish)),
            (QoS::AtLeastOnce, Some(id)) => {
                // Acknowledged whether or not the application gets to it ([MQTT-4.5.0-2]),
                // in the order the messages arrived ([MQTT-4.6.0-2]).
                self.emit(Event::Message(publish));
                self.send(&Packet::PubAck(PubAck::new(id)))?;
            }
            (QoS::ExactlyOnce, Some(id)) => {
                if !self.session.inbound.contains(&id.get()) {
                    if self.session.inbound.len() >= usize::from(self.negotiated.receive_maximum) {
                        return Err(self.protocol_error(
                            DisconnectReasonCode::ReceiveMaximumExceeded,
                            "more QoS 2 messages than the client's Receive Maximum".into(),
                        ));
                    }
                    self.session.inbound.insert(id.get());
                    self.emit(Event::Message(publish));
                }
                // A repeat before PUBREL gets PUBREC again and is not delivered twice
                // ([MQTT-4.6.0-3] keeps the order).
                self.send(&Packet::PubRec(PubRec::new(id)))?;
            }
            // The codec gives every QoS 1 and 2 PUBLISH its identifier.
            (_, None) => {}
        }
        Ok(())
    }

    /// PUBACK for a QoS 1 message.
    fn on_puback(&mut self, puback: PubAck) {
        let id = puback.packet_id;
        let Some(index) = self.session.position(id) else {
            return;
        };
        if self.session.outbound[index].stage != Stage::Acknowledgement {
            return;
        }
        self.finish_outbound(index);
        if let Some(Reply::Publish { reply, .. }) = self.replies.remove(&id.get()) {
            drop(reply.send(Ok(Published::AtLeastOnce(puback))));
        }
        self.pump();
    }

    /// PUBREC for a QoS 2 message: PUBREL when it accepts the message ([MQTT-4.6.0-4] keeps
    /// the order), the end of the exchange when it refuses it ([MQTT-4.3.3-4],
    /// [MQTT-4.4.0-2]).
    fn on_pubrec(&mut self, pubrec: PubRec) -> Result<(), Stop> {
        let id = pubrec.packet_id;
        let Some(index) = self.session.position(id) else {
            return self.send(&Packet::PubRel(PubRel {
                reason_code: PubRelReasonCode::PacketIdentifierNotFound,
                ..PubRel::new(id)
            }));
        };
        match self.session.outbound[index].stage {
            Stage::Receipt => {}
            // A repeated PUBREC: the PUBREL may not have arrived.
            Stage::Completion => return self.send(&Packet::PubRel(PubRel::new(id))),
            Stage::Acknowledgement => return Ok(()),
        }
        if pubrec.reason_code.is_error() {
            self.finish_outbound(index);
            if let Some(Reply::Publish { reply, .. }) = self.replies.remove(&id.get()) {
                drop(reply.send(Ok(Published::ExactlyOnce {
                    pubrec,
                    pubcomp: None,
                })));
            }
            self.pump();
            return Ok(());
        }
        self.session.outbound[index].stage = Stage::Completion;
        if let Some(Reply::Publish { pubrec: kept, .. }) = self.replies.get_mut(&id.get()) {
            *kept = Some(pubrec);
        }
        self.send(&Packet::PubRel(PubRel::new(id)))
    }

    /// PUBCOMP, the end of a QoS 2 exchange.
    fn on_pubcomp(&mut self, pubcomp: PubComp) {
        let id = pubcomp.packet_id;
        let Some(index) = self.session.position(id) else {
            return;
        };
        if self.session.outbound[index].stage != Stage::Completion {
            return;
        }
        self.finish_outbound(index);
        if let Some(Reply::Publish { reply, pubrec }) = self.replies.remove(&id.get()) {
            drop(reply.send(Ok(Published::ExactlyOnce {
                pubrec: pubrec.unwrap_or_else(|| PubRec::new(id)),
                pubcomp: Some(pubcomp),
            })));
        }
        self.pump();
    }
}
