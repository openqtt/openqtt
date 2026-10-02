//! A client connection over QUIC: the stream mapping of docs/spec/mqtt-over-quic.md, section 2,
//! a decoder per stream, and sending bounded by the backlog.
//!
//! Everything runs in the task that polls the connection: no task, channel or buffer of decoded
//! packets per connection, so that an idle connection costs little beyond quinn's own state
//! (report R7, D1). Each poll sends what it can, takes the streams the client opened, and reads
//! one packet.

use std::future::Future;
use std::pin::{Pin, pin};
use std::slice;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use openqtt_codec::{Decoder, Packet, Sender};
use openqtt_ext::Certificate;
use quinn::{ConnectionError, ReadError, RecvStream, SendStream, StoppedError, VarInt, WriteError};
use rustls::pki_types::CertificateDer;

use crate::listener::Shared;
use crate::{
    CloseCode, Closed, Error, Event, MqttConnection, Peer, StreamEnd, StreamTag, Violation,
};

/// A future the connection keeps between polls.
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The rest of a handshake, for a connection yielded with 0-RTT data: whether it ended in time.
pub(crate) type PendingHandshake = BoxFuture<bool>;

/// The most bytes taken from a stream at a time.
const READ_CHUNK: usize = 64 * 1024;

/// A packet is copied out of the buffer it was read into when that buffer is this many times its
/// size or more, so that a packet kept for long, such as a retained message, never keeps a far
/// larger buffer alive. Below it the packet shares the buffer, as the codec decodes.
const SHARE_RATIO: usize = 4;

/// Buffers no larger than this are shared whatever the packet: they pin little.
const SHARE_FLOOR: usize = 16 * 1024;

/// Where the handshake stands.
enum Handshake {
    /// Still going on, the connection having come with 0-RTT data.
    Pending(PendingHandshake),
    /// Complete; `reported` once the session has heard.
    Complete { reported: bool },
    /// Failed or ran out of time: nothing the client sent is confirmed.
    Failed,
}

/// One MQTT connection over QUIC, from its handshake to its close: the [`MqttConnection`] of
/// [`Endpoint`](crate::Endpoint)s.
///
/// The control stream is the first bidirectional stream the client opens, and must begin with
/// CONNECT. Data streams opened before [`accept_data_streams`](MqttConnection::accept_data_streams)
/// are held unread, and refused if the connection is refused. Each stream carries only the
/// packets the mapping lets it carry, and each is decoded on its own, held to the listener's
/// Maximum Packet Size from the fixed header on.
pub struct QuicConnection {
    connection: quinn::Connection,
    peer: Peer,
    decoder: Decoder,
    /// The control stream, once the client opened it.
    control: Option<Stream>,
    /// The data streams being read, in the order the client opened them.
    data: Vec<Stream>,
    /// Data streams opened before the connection was accepted, unread (section 2.3).
    held: Vec<Held>,
    data_accepted: bool,
    /// The client's next bidirectional stream, or why the connection closed.
    accept: Option<BoxFuture<Result<(SendStream, RecvStream), ConnectionError>>>,
    handshake: Handshake,
    paused: bool,
    /// The client broke the protocol: nothing more is read.
    failed: bool,
    closed: Option<Closed>,
    /// The session has heard of the close: nothing else is reported after it.
    close_reported: bool,
    backlog_limit: usize,
    /// The backlog reached its limit, and [`Event::Writable`] is owed once it falls below.
    blocked: bool,
    /// Where the next round of reads starts: 0 for the control stream, then the data streams.
    next_read: usize,
}

/// A data stream not read yet.
struct Held {
    tag: StreamTag,
    send: SendStream,
    recv: RecvStream,
}

/// Whether the client stopped the server's side of a stream, and whether the session heard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    None,
    Unreported(u64),
    Reported,
}

/// One accepted stream: its two sides, what was read and not yet decoded, and what waits to be
/// sent.
struct Stream {
    tag: StreamTag,
    /// The client's side, until it ends.
    recv: Option<RecvStream>,
    /// The server's side, until it is stopped or reset. Kept after it is finished, until the
    /// client acknowledged everything on it.
    send: Option<SendStream>,
    /// Completes when the client stops the server's side, or acknowledges all of it once
    /// finished.
    stopped: Option<BoxFuture<Result<Option<VarInt>, StoppedError>>>,
    stop: Stop,
    /// Bytes read and not yet a whole packet.
    read: BytesMut,
    /// Encoded packets not yet handed to quinn.
    queued: BytesMut,
    /// The bytes being handed to quinn.
    writing: Option<Bytes>,
    /// The session finished the server's side: finish it once everything queued is handed over.
    finish: bool,
    /// The server's side was finished in quinn.
    finished: bool,
    /// The control stream began with CONNECT.
    connected: bool,
}

/// What reading a stream gave.
enum Read {
    /// Nothing for now.
    Pending,
    /// An event, or a broken rule.
    Event(Result<Event, Error>),
    /// The connection is gone.
    Lost(ConnectionError),
}

impl Stream {
    fn new(tag: StreamTag, send: SendStream, recv: RecvStream) -> Self {
        Self {
            tag,
            stopped: Some(Box::pin(send.stopped())),
            recv: Some(recv),
            send: Some(send),
            stop: Stop::None,
            read: BytesMut::new(),
            queued: BytesMut::new(),
            writing: None,
            finish: false,
            finished: false,
            connected: false,
        }
    }

    /// How many bytes wait to be handed to quinn.
    fn backlog(&self) -> usize {
        self.queued.len() + self.writing.as_ref().map_or(0, Bytes::len)
    }

    /// Whether the session may still queue packets on the server's side.
    fn open_for_sending(&self) -> bool {
        self.send.is_some() && !self.finish
    }

    /// Whether neither side has anything left to do, so the stream can be forgotten.
    fn is_done(&self) -> bool {
        self.recv.is_none()
            && self.stopped.is_none()
            && (self.send.is_none() || self.finished)
            && !matches!(self.stop, Stop::Unreported(_))
    }

    /// Drops what waits to be sent, which can no longer reach the client.
    fn discard_output(&mut self) {
        self.queued = BytesMut::new();
        self.writing = None;
    }

    /// The client stopped the server's side with `code`.
    fn stopped_by_client(&mut self, code: u64) {
        self.discard_output();
        self.send = None;
        self.stopped = None;
        if self.stop == Stop::None {
            self.stop = Stop::Unreported(code);
        }
    }

    /// Finishes the server's side in quinn, which then sends what it holds and the end.
    fn finish_now(&mut self) {
        if let Some(send) = self.send.as_mut() {
            // A stream already finished or reset is not an error here.
            drop(send.finish());
        }
        self.finished = true;
    }

    /// Hands what is queued to quinn, as far as flow control allows, and finishes the server's
    /// side once all of it is handed over and the session asked for that. Returns the error that
    /// lost the connection, if one did.
    fn poll_write(&mut self, cx: &mut Context<'_>) -> Option<ConnectionError> {
        loop {
            let send = self.send.as_mut()?;
            if self.writing.is_none() {
                if self.queued.is_empty() {
                    if self.finish && !self.finished {
                        self.finish_now();
                    }
                    // Nothing waits: let the allocation go, as an idle connection should hold
                    // none.
                    self.queued = BytesMut::new();
                    return None;
                }
                self.writing = Some(self.queued.split().freeze());
            }
            let chunk = self.writing.as_mut()?;
            let polled = {
                let mut write = pin!(send.write_chunks(slice::from_mut(chunk)));
                write.as_mut().poll(cx)
            };
            match polled {
                Poll::Pending => return None,
                Poll::Ready(Ok(_)) => {
                    if chunk.is_empty() {
                        self.writing = None;
                    }
                }
                Poll::Ready(Err(WriteError::Stopped(code))) => {
                    self.stopped_by_client(code.into_inner());
                    return None;
                }
                Poll::Ready(Err(WriteError::ConnectionLost(error))) => {
                    self.discard_output();
                    return Some(error);
                }
                // Finished or reset already, or a 0-RTT stream a server never has: nothing
                // more can go out on it.
                Poll::Ready(Err(WriteError::ClosedStream | WriteError::ZeroRttRejected)) => {
                    self.discard_output();
                    self.send = None;
                    return None;
                }
            }
        }
    }

    /// Notes the client stopping the server's side, or acknowledging all of it once finished.
    /// Returns the error that lost the connection, if one did.
    fn poll_stopped(&mut self, cx: &mut Context<'_>) -> Option<ConnectionError> {
        let stopped = self.stopped.as_mut()?;
        match stopped.as_mut().poll(cx) {
            Poll::Pending => None,
            Poll::Ready(Ok(Some(code))) => {
                self.stopped_by_client(code.into_inner());
                None
            }
            Poll::Ready(Ok(None)) => {
                // Everything sent was acknowledged after the finish.
                self.stopped = None;
                None
            }
            Poll::Ready(Err(StoppedError::ConnectionLost(error))) => {
                self.stopped = None;
                Some(error)
            }
            Poll::Ready(Err(StoppedError::ZeroRttRejected)) => {
                self.stopped = None;
                None
            }
        }
    }

    /// The next packet on the client's side, or how that side ended.
    fn poll_read(&mut self, decoder: Decoder, cx: &mut Context<'_>) -> Read {
        loop {
            if !self.read.is_empty() {
                match take_packet(&mut self.read, decoder) {
                    Ok(Some(packet)) => return Read::Event(self.check(packet)),
                    Ok(None) => {}
                    Err(error) => {
                        return Read::Event(Err(Error::Decode {
                            stream: self.tag,
                            error,
                        }));
                    }
                }
            }
            let Some(recv) = self.recv.as_mut() else {
                return Read::Pending;
            };
            let polled = {
                let mut read = pin!(recv.read_chunk(READ_CHUNK, true));
                read.as_mut().poll(cx)
            };
            match polled {
                Poll::Pending => return Read::Pending,
                Poll::Ready(Ok(Some(chunk))) => self.read.extend_from_slice(&chunk.bytes),
                Poll::Ready(Ok(None)) => {
                    self.recv = None;
                    return Read::Event(if self.read.is_empty() {
                        Ok(Event::StreamEnded {
                            stream: self.tag,
                            end: StreamEnd::Finished,
                        })
                    } else {
                        Err(Error::Violation {
                            stream: self.tag,
                            violation: Violation::EndedInsidePacket,
                        })
                    });
                }
                Poll::Ready(Err(ReadError::Reset(code))) => {
                    self.recv = None;
                    self.read = BytesMut::new();
                    return Read::Event(Ok(Event::StreamEnded {
                        stream: self.tag,
                        end: StreamEnd::Reset(code.into_inner()),
                    }));
                }
                Poll::Ready(Err(ReadError::ConnectionLost(error))) => {
                    self.recv = None;
                    return Read::Lost(error);
                }
                // Stopped by the server, or a 0-RTT stream a server never has: nothing more
                // comes on it.
                Poll::Ready(Err(
                    ReadError::ClosedStream
                    | ReadError::IllegalOrderedRead
                    | ReadError::ZeroRttRejected,
                )) => {
                    self.recv = None;
                    return Read::Pending;
                }
            }
        }
    }

    /// Holds a packet to what its stream may carry (sections 2.1 and 2.3).
    fn check(&mut self, packet: Packet) -> Result<Event, Error> {
        let violation = match self.tag {
            StreamTag::Control if !self.connected => match packet {
                Packet::Connect(_) => {
                    self.connected = true;
                    None
                }
                ref other => Some(Violation::FirstPacketNotConnect {
                    packet_type: other.packet_type(),
                }),
            },
            StreamTag::Control => None,
            StreamTag::Data(_) => data_stream_violation(&packet),
        };
        match violation {
            Some(violation) => Err(Error::Violation {
                stream: self.tag,
                violation,
            }),
            None => Ok(Event::Packet {
                stream: self.tag,
                packet,
            }),
        }
    }
}

/// Takes the next whole packet off the front of `read`, the bytes a stream delivered.
///
/// The codec decodes without copying, so a packet's payload shares the buffer it arrived in, and
/// keeps all of it alive. A packet small beside that buffer is copied out first, and what is left
/// after it moves to a buffer of its own size, so that no packet keeps alive a buffer much larger
/// than itself: a retained message of ten bytes read behind a packet of a megabyte would
/// otherwise hold the megabyte.
fn take_packet(
    read: &mut BytesMut,
    decoder: Decoder,
) -> Result<Option<Packet>, openqtt_codec::Error> {
    let packet = match frame_len(read) {
        Some(len) if len <= read.len() && wasteful(read.capacity(), len) => {
            let mut frame = BytesMut::from(&read[..len]);
            let packet = decoder.decode(&mut frame)?;
            read.advance(len);
            packet
        }
        _ => decoder.decode(read)?,
    };
    if packet.is_some() {
        if read.is_empty() {
            // Let the buffer go: the packet keeps what it needs of it.
            *read = BytesMut::new();
        } else if wasteful(read.capacity(), read.len()) {
            *read = BytesMut::from(&read[..]);
        }
    }
    Ok(packet)
}

/// Whether `len` bytes would pin a buffer of `capacity` far larger than themselves.
fn wasteful(capacity: usize, len: usize) -> bool {
    capacity > SHARE_FLOOR && capacity / SHARE_RATIO > len
}

/// The size of the packet at the front of `buffer`, from its fixed header: `None` until the
/// Remaining Length is in, or when it is not a valid Variable Byte Integer, which the codec then
/// refuses.
fn frame_len(buffer: &[u8]) -> Option<usize> {
    let mut remaining: usize = 0;
    for (index, byte) in buffer.iter().skip(1).take(4).enumerate() {
        remaining |= usize::from(byte & 0x7F) << (7 * index);
        if byte & 0x80 == 0 {
            return Some(1 + index + 1 + remaining);
        }
    }
    None
}

/// What a data stream may not carry, either way: a packet of the control stream (section 2.1),
/// or a PUBLISH with a Topic Alias (section 2.3). Data streams carry packet types 3 to 11.
fn data_stream_violation(packet: &Packet) -> Option<Violation> {
    match packet {
        Packet::Publish(publish) if publish.properties.topic_alias.is_some() => {
            Some(Violation::TopicAliasOnDataStream)
        }
        Packet::Publish(_)
        | Packet::PubAck(_)
        | Packet::PubRec(_)
        | Packet::PubRel(_)
        | Packet::PubComp(_)
        | Packet::Subscribe(_)
        | Packet::SubAck(_)
        | Packet::Unsubscribe(_)
        | Packet::UnsubAck(_) => None,
        other => Some(Violation::ControlPacketOnDataStream {
            packet_type: other.packet_type(),
        }),
    }
}

/// Waits for the client's next bidirectional stream on `connection`.
fn next_stream(
    connection: &quinn::Connection,
) -> BoxFuture<Result<(SendStream, RecvStream), ConnectionError>> {
    let connection = connection.clone();
    Box::pin(async move { connection.accept_bi().await })
}

/// Refuses a stream with `code` both ways.
fn refuse(mut send: SendStream, mut recv: RecvStream, code: CloseCode) {
    let code = VarInt::from_u32(code.value());
    // Already ended is as good as refused.
    drop(send.reset(code));
    drop(recv.stop(code));
}

impl Closed {
    /// How quinn says a connection ended.
    pub(crate) fn of(error: &ConnectionError) -> Self {
        match error {
            ConnectionError::ApplicationClosed(close) => Self::Application {
                code: close.error_code.into_inner(),
                reason: close.reason.clone(),
            },
            ConnectionError::ConnectionClosed(close) => Self::Transport {
                code: u64::from(close.error_code),
                reason: String::from_utf8_lossy(&close.reason).into_owned(),
            },
            ConnectionError::TransportError(error) => Self::Transport {
                code: u64::from(error.code),
                reason: error.reason.clone(),
            },
            ConnectionError::TimedOut => Self::TimedOut,
            ConnectionError::Reset => Self::Reset,
            ConnectionError::LocallyClosed => Self::Locally,
            other => Self::Other(other.to_string()),
        }
    }
}

impl QuicConnection {
    /// A connection whose handshake completed, or, with `handshake` given, one whose handshake
    /// was still going on when its client opened `first`, in 0-RTT data when `early_data`.
    pub(crate) fn new(
        connection: quinn::Connection,
        shared: &Shared,
        handshake: Option<PendingHandshake>,
        first: Option<(SendStream, RecvStream)>,
        early_data: bool,
    ) -> Self {
        let certificates: Vec<Certificate> = connection
            .peer_identity()
            .and_then(|identity| identity.downcast::<Vec<CertificateDer<'static>>>().ok())
            .map(|chain| {
                chain
                    .iter()
                    .map(|certificate| Certificate::from_der(Bytes::copy_from_slice(certificate)))
                    .collect()
            })
            .unwrap_or_default();
        let alpn = connection
            .handshake_data()
            .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
            .and_then(|data| data.protocol)
            .map(Bytes::from);
        let mut peer = Peer::new(connection.remote_address(), &shared.name)
            .with_certificates(Arc::from(certificates))
            .with_early_data(early_data);
        if let Some(alpn) = alpn {
            peer = peer.with_alpn(alpn);
        }
        let mut this = Self {
            accept: Some(next_stream(&connection)),
            connection,
            peer,
            decoder: Decoder::new()
                .with_sender(Sender::Client)
                .with_max_packet_size(shared.max_packet_size),
            control: None,
            data: Vec::new(),
            held: Vec::new(),
            data_accepted: false,
            handshake: match handshake {
                Some(pending) => Handshake::Pending(pending),
                None => Handshake::Complete { reported: false },
            },
            paused: false,
            failed: false,
            closed: None,
            close_reported: false,
            backlog_limit: shared.send_backlog,
            blocked: false,
            next_read: 0,
        };
        if let Some((send, recv)) = first {
            this.admit(send, recv);
        }
        this
    }

    /// Takes a stream the client opened: the control stream, a data stream to read, or one to
    /// hold until the connection is accepted.
    fn admit(&mut self, send: SendStream, recv: RecvStream) {
        let index = recv.id().index();
        if index == 0 {
            self.control = Some(Stream::new(StreamTag::Control, send, recv));
            return;
        }
        let tag = StreamTag::Data(index);
        if self.data_accepted {
            self.data.push(Stream::new(tag, send, recv));
        } else {
            self.held.push(Held { tag, send, recv });
        }
    }

    /// The control stream and the data streams being read.
    fn streams_mut(&mut self) -> impl Iterator<Item = &mut Stream> {
        self.control.iter_mut().chain(self.data.iter_mut())
    }

    fn stream_mut(&mut self, tag: StreamTag) -> Option<&mut Stream> {
        match tag {
            StreamTag::Control => self.control.as_mut(),
            StreamTag::Data(_) => self.data.iter_mut().find(|stream| stream.tag == tag),
        }
    }

    /// How many bytes wait to be handed to quinn, on every stream.
    fn backlog(&self) -> usize {
        self.control
            .iter()
            .chain(self.data.iter())
            .map(Stream::backlog)
            .sum()
    }

    fn lose(&mut self, error: &ConnectionError) {
        self.closed.get_or_insert_with(|| Closed::of(error));
    }

    fn closed_error(&self) -> Result<(), Error> {
        match &self.closed {
            Some(closed) => Err(Error::Closed(closed.clone())),
            None => Ok(()),
        }
    }

    /// Whether the handshake is complete, polling it if it is going on.
    fn poll_handshake(&mut self, cx: &mut Context<'_>) -> bool {
        if let Handshake::Pending(pending) = &mut self.handshake {
            let Poll::Ready(in_time) = pending.as_mut().poll(cx) else {
                return false;
            };
            if !in_time {
                // The client sent 0-RTT data and never completed the handshake.
                self.connection
                    .close(VarInt::from_u32(CloseCode::ProtocolError.value()), b"");
                self.handshake = Handshake::Failed;
                self.closed.get_or_insert(Closed::TimedOut);
            } else if let Some(error) = self.connection.close_reason() {
                // It ended with the connection rather than completing: quinn signals both the
                // same way, and sets the close reason first.
                self.handshake = Handshake::Failed;
                self.lose(&error);
            } else {
                self.handshake = Handshake::Complete { reported: false };
            }
        }
        matches!(self.handshake, Handshake::Complete { .. })
    }

    /// Hands what is queued to quinn, on every stream. Nothing leaves before the handshake is
    /// complete (section 4).
    fn drive_writes(&mut self, cx: &mut Context<'_>) {
        if !self.poll_handshake(cx) {
            return;
        }
        let mut lost = None;
        for stream in self.streams_mut() {
            if let Some(error) = stream.poll_write(cx) {
                lost = Some(error);
            }
        }
        if let Some(error) = lost {
            self.lose(&error);
        }
    }

    /// Takes the streams the client opened since the last poll.
    fn drive_accept(&mut self, cx: &mut Context<'_>) {
        while let Some(accept) = self.accept.as_mut() {
            match accept.as_mut().poll(cx) {
                Poll::Pending => return,
                Poll::Ready(Ok((send, recv))) => {
                    self.accept = Some(next_stream(&self.connection));
                    self.admit(send, recv);
                }
                Poll::Ready(Err(error)) => {
                    self.accept = None;
                    self.lose(&error);
                }
            }
        }
    }

    /// Notes the streams whose server side the client stopped or acknowledged, and forgets the
    /// data streams that are done.
    fn drive_stopped(&mut self, cx: &mut Context<'_>) {
        let mut lost = None;
        for stream in self.streams_mut() {
            if let Some(error) = stream.poll_stopped(cx) {
                lost = Some(error);
            }
        }
        if let Some(error) = lost {
            self.lose(&error);
        }
        self.data.retain(|stream| !stream.is_done());
    }

    /// Whether packets may be read now.
    fn reading(&self) -> bool {
        // A closed connection is drained whatever else holds reading back, so that a DISCONNECT
        // that came before the close is not mistaken for an abnormal end. Not one whose handshake
        // never completed: what it sent may be a replay.
        !self.failed
            && match self.closed {
                Some(_) => matches!(self.handshake, Handshake::Complete { .. }),
                None => !self.paused && self.backlog() < self.backlog_limit,
            }
    }

    /// The next stop of a server's side to report, once the client's side of that stream has
    /// nothing left that arrived before it: those packets come first, so that a DISCONNECT
    /// followed by a stop of the control stream is not taken for an abnormal end, nor a PUBLISH
    /// whose acknowledgement can no longer be sent missed. While nothing may be read, the stop
    /// waits too.
    fn take_stop(&mut self, cx: &mut Context<'_>) -> Option<Result<Event, Error>> {
        if !self.reading() {
            return None;
        }
        let decoder = self.decoder;
        let mut lost = None;
        let mut taken = None;
        for stream in self.control.iter_mut().chain(self.data.iter_mut()) {
            let Stop::Unreported(code) = stream.stop else {
                continue;
            };
            match stream.poll_read(decoder, cx) {
                Read::Event(result) => {
                    taken = Some(result);
                    break;
                }
                // Nothing more will arrive on it.
                Read::Lost(error) => lost = Some(error),
                Read::Pending => {}
            }
            stream.stop = Stop::Reported;
            taken = Some(Ok(Event::StreamEnded {
                stream: stream.tag,
                end: StreamEnd::Stopped(code),
            }));
            break;
        }
        if let Some(error) = lost {
            self.lose(&error);
        }
        if matches!(taken, Some(Err(_))) {
            self.failed = true;
        }
        if taken.is_some() {
            self.data.retain(|stream| !stream.is_done());
        }
        taken
    }

    /// Reads the next packet, taking the streams in turn so that none starves the others.
    fn drive_reads(&mut self, cx: &mut Context<'_>) -> Option<Result<Event, Error>> {
        if !self.reading() {
            return None;
        }
        let decoder = self.decoder;
        let count = 1 + self.data.len();
        for offset in 0..count {
            let slot = (self.next_read + offset) % count;
            let stream = if slot == 0 {
                self.control.as_mut()
            } else {
                self.data.get_mut(slot - 1)
            };
            let Some(stream) = stream else {
                continue;
            };
            match stream.poll_read(decoder, cx) {
                Read::Pending => {}
                Read::Lost(error) => self.lose(&error),
                Read::Event(result) => {
                    self.next_read = slot + 1;
                    if result.is_err() {
                        self.failed = true;
                    }
                    self.data.retain(|stream| !stream.is_done());
                    return Some(result);
                }
            }
        }
        None
    }
}

impl MqttConnection for QuicConnection {
    fn peer(&self) -> &Peer {
        &self.peer
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Result<Event, Error>> {
        if self.close_reported {
            let closed = self.closed.clone().unwrap_or(Closed::Locally);
            return Poll::Ready(Err(Error::Closed(closed)));
        }
        // Once quinn has a close it takes in nothing more, so a round of polls that starts after
        // the close misses nothing that came before it.
        let closed_before = self.closed.is_some();
        if self.poll_handshake(cx)
            && matches!(self.handshake, Handshake::Complete { reported: false })
        {
            self.handshake = Handshake::Complete { reported: true };
            return Poll::Ready(Ok(Event::HandshakeComplete {
                early_data: self.peer.early_data,
            }));
        }
        self.drive_writes(cx);
        if self.blocked && self.closed.is_none() && self.backlog() < self.backlog_limit {
            self.blocked = false;
            return Poll::Ready(Ok(Event::Writable));
        }
        self.drive_accept(cx);
        self.drive_stopped(cx);
        if let Some(result) = self.take_stop(cx) {
            return Poll::Ready(result);
        }
        if let Some(result) = self.drive_reads(cx) {
            return Poll::Ready(result);
        }
        match &self.closed {
            Some(closed) if closed_before => {
                self.close_reported = true;
                Poll::Ready(Err(Error::Closed(closed.clone())))
            }
            Some(_) => {
                // Seen partway through this round, after some streams were read: read them all
                // once more before saying so.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            None => Poll::Pending,
        }
    }

    fn send(&mut self, stream: StreamTag, packet: &Packet) -> Result<(), Error> {
        self.closed_error()?;
        let packet_type = packet.packet_type();
        if matches!(stream, StreamTag::Data(_)) && data_stream_violation(packet).is_some() {
            return Err(Error::WrongStream {
                stream,
                packet_type,
            });
        }
        let target = self
            .stream_mut(stream)
            .filter(|target| target.open_for_sending())
            .ok_or(Error::StreamClosed { stream })?;
        packet
            .encode(&mut target.queued)
            .map_err(|error| Error::Encode { packet_type, error })?;
        if self.backlog() >= self.backlog_limit {
            self.blocked = true;
        }
        Ok(())
    }

    fn send_bytes(&mut self, stream: StreamTag, bytes: &[u8]) -> Result<(), Error> {
        self.closed_error()?;
        let target = self
            .stream_mut(stream)
            .filter(|target| target.open_for_sending())
            .ok_or(Error::StreamClosed { stream })?;
        target.queued.extend_from_slice(bytes);
        if self.backlog() >= self.backlog_limit {
            self.blocked = true;
        }
        Ok(())
    }

    fn is_writable(&self) -> bool {
        self.backlog() < self.backlog_limit
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.drive_writes(cx);
        self.closed_error()?;
        if matches!(self.handshake, Handshake::Complete { .. }) && self.backlog() == 0 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn finish(&mut self, stream: StreamTag) -> Result<(), Error> {
        self.closed_error()?;
        let handshake_done = matches!(self.handshake, Handshake::Complete { .. });
        let target = self
            .stream_mut(stream)
            .filter(|target| target.open_for_sending())
            .ok_or(Error::StreamClosed { stream })?;
        target.finish = true;
        if target.backlog() == 0 && handshake_done {
            target.finish_now();
        }
        Ok(())
    }

    fn reset(&mut self, stream: StreamTag, code: CloseCode) -> Result<(), Error> {
        self.closed_error()?;
        if let Some(position) = self.held.iter().position(|held| held.tag == stream) {
            let held = self.held.swap_remove(position);
            refuse(held.send, held.recv, code);
            return Ok(());
        }
        let target = self
            .stream_mut(stream)
            .ok_or(Error::StreamClosed { stream })?;
        let quic_code = VarInt::from_u32(code.value());
        if let Some(send) = target.send.as_mut() {
            // Already ended is as good as reset.
            drop(send.reset(quic_code));
        }
        if let Some(recv) = target.recv.as_mut() {
            drop(recv.stop(quic_code));
        }
        target.discard_output();
        target.send = None;
        target.recv = None;
        target.stopped = None;
        target.stop = Stop::Reported;
        self.data.retain(|stream| !stream.is_done());
        Ok(())
    }

    fn accept_data_streams(&mut self) {
        if self.data_accepted {
            return;
        }
        self.data_accepted = true;
        for held in std::mem::take(&mut self.held) {
            self.data.push(Stream::new(held.tag, held.send, held.recv));
        }
    }

    fn pause_reading(&mut self) {
        self.paused = true;
    }

    fn resume_reading(&mut self) {
        self.paused = false;
    }

    fn finish_all(&mut self) {
        for held in std::mem::take(&mut self.held) {
            refuse(held.send, held.recv, CloseCode::StreamRefused);
        }
        let handshake_done = matches!(self.handshake, Handshake::Complete { .. });
        for stream in self.streams_mut() {
            if stream.open_for_sending() {
                stream.finish = true;
                if stream.backlog() == 0 && handshake_done {
                    stream.finish_now();
                }
            }
        }
    }

    fn poll_delivered(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.drive_writes(cx);
        self.drive_stopped(cx);
        if self.closed.is_some() {
            return Poll::Ready(());
        }
        let waiting = self.streams_mut().any(|stream| {
            stream.finish
                && stream.send.is_some()
                && (stream.backlog() > 0 || stream.stopped.is_some())
        });
        if waiting {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    fn close(&mut self, code: CloseCode) {
        self.connection.close(VarInt::from_u32(code.value()), b"");
        self.closed.get_or_insert(Closed::Locally);
    }
}

impl Drop for QuicConnection {
    /// Closes with an internal error a connection the edge let go of without closing it, as when
    /// its task panicked or was aborted. quinn would close it with code 0, which tells the
    /// client the end was clean (section 8).
    fn drop(&mut self) {
        if self.closed.is_none() {
            self.connection
                .close(VarInt::from_u32(CloseCode::InternalError.value()), b"");
        }
    }
}

#[cfg(test)]
mod tests {
    use openqtt_codec::{
        Auth, ConnAck, Connect, Disconnect, PacketId, PubAck, Publish, PublishProperties, QoS,
        SubAck, Subscribe, UnsubAck,
    };

    use super::*;

    #[test]
    fn data_streams_carry_packet_types_3_to_11_only() {
        let id = PacketId::new(1).unwrap();
        for packet in [
            Packet::from(Publish {
                topic: "t".into(),
                ..Publish::default()
            }),
            Packet::from(PubAck::new(id)),
            Packet::from(SubAck {
                packet_id: id,
                properties: openqtt_codec::AckProperties::default(),
                reason_codes: vec![openqtt_codec::SubAckReasonCode::GrantedQos0],
            }),
            Packet::from(UnsubAck {
                packet_id: id,
                properties: openqtt_codec::AckProperties::default(),
                reason_codes: vec![openqtt_codec::UnsubAckReasonCode::Success],
            }),
            Packet::from(Subscribe {
                packet_id: id,
                properties: openqtt_codec::SubscribeProperties::default(),
                subscriptions: vec![openqtt_codec::Subscription {
                    filter: "t".into(),
                    options: openqtt_codec::SubscriptionOptions::default(),
                }],
            }),
        ] {
            assert_eq!(data_stream_violation(&packet), None, "{packet:?}");
        }
        for packet in [
            Packet::from(Connect::default()),
            Packet::from(ConnAck::default()),
            Packet::PingReq,
            Packet::PingResp,
            Packet::from(Disconnect::default()),
            Packet::from(Auth::default()),
        ] {
            assert_eq!(
                data_stream_violation(&packet),
                Some(Violation::ControlPacketOnDataStream {
                    packet_type: packet.packet_type()
                })
            );
        }
        let aliased = Packet::from(Publish {
            qos: QoS::AtMostOnce,
            topic: "t".into(),
            properties: PublishProperties {
                topic_alias: std::num::NonZeroU16::new(1),
                ..PublishProperties::default()
            },
            ..Publish::default()
        });
        assert_eq!(
            data_stream_violation(&aliased),
            Some(Violation::TopicAliasOnDataStream)
        );
    }

    /// Encodes a PUBLISH of `payload` bytes at QoS 0.
    fn publish(payload: usize) -> BytesMut {
        let mut bytes = BytesMut::new();
        Packet::from(Publish {
            topic: "t".into(),
            payload: Bytes::from(vec![7; payload]),
            ..Publish::default()
        })
        .encode(&mut bytes)
        .unwrap();
        bytes
    }

    /// Whether `payload` lies inside `buffer`'s memory.
    fn inside(payload: &Bytes, buffer: std::ops::Range<usize>) -> bool {
        buffer.contains(&(payload.as_ptr() as usize))
    }

    fn payload(packet: Option<Packet>) -> Bytes {
        match packet {
            Some(Packet::Publish(publish)) => publish.payload,
            other => panic!("a PUBLISH, not {other:?}"),
        }
    }

    #[test]
    fn the_fixed_header_gives_the_packet_size() {
        assert_eq!(frame_len(&[]), None);
        assert_eq!(frame_len(&[0xC0]), None);
        assert_eq!(frame_len(&[0xC0, 0x00]), Some(2));
        assert_eq!(frame_len(&[0x30, 0x80, 0x10]), Some(2_051));
        assert_eq!(frame_len(&[0x30, 0xFF, 0xFF, 0xFF]), None);
        assert_eq!(
            frame_len(&[0x30, 0xFF, 0xFF, 0xFF, 0x7F]),
            Some(268_435_460)
        );
        // A fifth length byte: not a Variable Byte Integer, left to the codec to refuse.
        assert_eq!(frame_len(&[0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]), None);
        assert_eq!(frame_len(&publish(100)), Some(publish(100).len()));
    }

    #[test]
    fn a_small_packet_does_not_keep_a_large_buffer_alive() {
        let decoder = Decoder::new().with_sender(Sender::Client);
        // A large packet, then a small one and the start of another, in one buffer, as a burst
        // read into it leaves them.
        let mut read = publish(200_000);
        read.extend_from_slice(&publish(10));
        read.extend_from_slice(&publish(10)[..5]);
        let start = read.as_ptr() as usize;
        let memory = start..start + read.capacity();

        // The large packet shares the buffer, which is not much larger than it.
        let large = payload(take_packet(&mut read, decoder).unwrap());
        assert_eq!(large.len(), 200_000);
        assert!(inside(&large, memory.clone()));
        // The small one is copied out, and what follows it moves to a buffer of its own.
        let small = payload(take_packet(&mut read, decoder).unwrap());
        assert_eq!(small.len(), 10);
        assert!(!inside(&small, memory.clone()));
        assert_eq!(read.len(), 5);
        assert!(!inside(&Bytes::copy_from_slice(&read), memory.clone()));
        assert!(read.capacity() < SHARE_FLOOR);
        assert_eq!(take_packet(&mut read, decoder), Ok(None));
    }

    #[test]
    fn packets_in_a_small_buffer_share_it() {
        let decoder = Decoder::new().with_sender(Sender::Client);
        let mut read = publish(100);
        read.extend_from_slice(&publish(100));
        let start = read.as_ptr() as usize;
        let memory = start..start + read.capacity();
        for _ in 0..2 {
            assert!(inside(
                &payload(take_packet(&mut read, decoder).unwrap()),
                memory.clone()
            ));
        }
        // Emptied, the buffer is let go.
        assert_eq!(read.capacity(), 0);
    }

    #[test]
    fn closes_are_read_from_quinn() {
        assert_eq!(Closed::of(&ConnectionError::TimedOut), Closed::TimedOut);
        assert_eq!(Closed::of(&ConnectionError::Reset), Closed::Reset);
        assert_eq!(Closed::of(&ConnectionError::LocallyClosed), Closed::Locally);
        assert!(matches!(
            Closed::of(&ConnectionError::VersionMismatch),
            Closed::Other(_)
        ));
    }
}
