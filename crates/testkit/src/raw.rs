//! The raw-packet connection: MQTT over a QUIC control stream with nothing in between. It
//! sends whatever it is given, malformed bytes included, and records every packet each way
//! with the time it passed, and how the connection closed.

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use openqtt_codec::{
    Decoder, Packet, PubAck, PubAckReasonCode, PubComp, PubRec, PubRecReasonCode, QoS, Sender,
};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ConnectionError, IdleTimeout, ReadError, RecvStream, SendStream, VarInt};
use rustls::pki_types::CertificateDer;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::tls::{ALPN, client_config};
use crate::{Error, Identity};

/// How long a QUIC connection of the test kit may stay silent: long enough for every Keep
/// Alive a test uses, since the server's MQTT Keep Alive is what a test watches.
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// Where a raw connection goes: the server's address, the name its certificate carries, the
/// roots that certificate chains to, and optionally a client certificate.
#[derive(Debug, Clone)]
pub struct Target {
    /// The server's UDP address.
    pub addr: SocketAddr,
    /// The name the server's certificate must carry.
    pub server_name: String,
    roots: Vec<CertificateDer<'static>>,
    identity: Option<Identity>,
    alpn: Vec<Vec<u8>>,
}

impl Target {
    /// A server at `addr` whose certificate names `server_name` and chains to one of `roots`.
    pub fn new(
        addr: SocketAddr,
        server_name: impl Into<String>,
        roots: Vec<CertificateDer<'static>>,
    ) -> Self {
        Self {
            addr,
            server_name: server_name.into(),
            roots,
            identity: None,
            alpn: vec![ALPN.to_vec()],
        }
    }

    /// Presents `identity` as the client certificate.
    #[must_use]
    pub fn with_identity(mut self, identity: Identity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Offers these ALPN protocols instead of `mqtt`, to test a server that must refuse them.
    #[must_use]
    pub fn with_alpn(mut self, alpn: Vec<Vec<u8>>) -> Self {
        self.alpn = alpn;
        self
    }

    fn quinn_config(&self) -> Result<quinn::ClientConfig, Error> {
        let tls = client_config(&self.roots, self.identity.as_ref(), &self.alpn)?;
        let crypto = QuicClientConfig::try_from(tls)
            .map_err(|error| Error::TlsForQuic(error.to_string()))?;
        let mut config = quinn::ClientConfig::new(Arc::new(crypto));
        config.transport_config(transport_config()?);
        Ok(config)
    }
}

/// The QUIC settings of both ends of the test kit: a long idle timeout, and no datagrams,
/// which MQTT over QUIC does not use (docs/spec/mqtt-over-quic.md, section 1). EMQX 5.8.9 drops
/// any connection whose peer offers datagrams.
pub(crate) fn transport_config() -> Result<Arc<quinn::TransportConfig>, Error> {
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(
        IdleTimeout::try_from(IDLE_TIMEOUT)
            .map_err(|error| Error::TlsForQuic(error.to_string()))?,
    ));
    transport.datagram_receive_buffer_size(None);
    Ok(Arc::new(transport))
}

/// One thing that happened on a connection, and when, from the moment it opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Time since the connection opened.
    pub at: Duration,
    /// What happened.
    pub event: Recorded,
}

/// What a [`Record`] holds.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Recorded {
    /// This end sent a packet.
    Sent(Packet),
    /// This end sent bytes as they were given, malformed or not.
    SentBytes(Bytes),
    /// The peer sent a packet.
    Received(Packet),
    /// The peer sent bytes that frame as a packet but do not decode as one an MQTT 5 peer may
    /// send. The bytes are the whole frame, or everything left when no frame can be found.
    Malformed {
        /// The bytes.
        bytes: Bytes,
        /// Why they do not decode.
        error: String,
    },
    /// The peer finished its side of the control stream.
    StreamFinished,
    /// The peer reset its side of the control stream with this code.
    StreamReset(u64),
    /// The connection closed. Nothing is recorded after it.
    Closed(Close),
}

impl Recorded {
    /// Whether this came from the peer, or from the connection ending.
    pub fn is_incoming(&self) -> bool {
        !matches!(self, Self::Sent(_) | Self::SentBytes(_))
    }

    /// The packet, when this is one the peer sent.
    pub fn received(&self) -> Option<&Packet> {
        match self {
            Self::Received(packet) => Some(packet),
            _ => None,
        }
    }
}

/// How a QUIC connection closed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Close {
    /// The peer closed it with an application error code (docs/spec/mqtt-over-quic.md,
    /// section 8).
    Application {
        /// The code.
        code: u64,
        /// The reason phrase.
        reason: Bytes,
    },
    /// The peer's QUIC stack closed it with a transport error code.
    Transport {
        /// The code.
        code: u64,
        /// The reason phrase.
        reason: String,
    },
    /// Nothing arrived for the idle timeout.
    TimedOut,
    /// The peer reset the connection, usually after it restarted.
    Reset,
    /// This end closed it.
    Locally,
    /// Anything else, as quinn describes it.
    Other(String),
}

impl Close {
    fn of(error: &ConnectionError) -> Self {
        match error {
            ConnectionError::ApplicationClosed(close) => Self::Application {
                code: close.error_code.into_inner(),
                reason: close.reason.clone(),
            },
            ConnectionError::ConnectionClosed(close) => Self::Transport {
                code: u64::from(close.error_code),
                reason: String::from_utf8_lossy(&close.reason).into_owned(),
            },
            ConnectionError::TimedOut => Self::TimedOut,
            ConnectionError::Reset => Self::Reset,
            ConnectionError::LocallyClosed => Self::Locally,
            other => Self::Other(other.to_string()),
        }
    }
}

/// The records of one connection, shared between its reader task and the test.
#[derive(Debug)]
struct Log {
    opened: Instant,
    records: Mutex<Vec<Record>>,
    /// The number of records, bumped on every push so a waiting test wakes.
    count: watch::Sender<usize>,
}

impl Log {
    fn push(&self, event: Recorded) {
        let at = self.opened.elapsed();
        let mut records = self.records.lock().unwrap_or_else(PoisonError::into_inner);
        records.push(Record { at, event });
        self.count.send_replace(records.len());
    }

    fn snapshot(&self) -> Vec<Record> {
        self.records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// An MQTT connection over QUIC that sends exactly what it is told, and records everything.
///
/// It is the client side when opened with [`connect`](Self::connect), and the server side when
/// a [`FakeServer`](crate::FakeServer) accepts it. Either way it decodes what the peer sends,
/// holding the peer to the rules of its end ([`Packet::check_sender`]), and keeps bytes that
/// do not decode as [`Recorded::Malformed`] instead of failing.
#[derive(Debug)]
pub struct RawConnection {
    connection: quinn::Connection,
    send: SendStream,
    log: Arc<Log>,
    /// The index of the next record [`recv`](Self::recv) looks at.
    cursor: usize,
    /// The index of the next record [`acknowledge`](Self::acknowledge) looks at.
    acknowledged: usize,
    count: watch::Receiver<usize>,
    reader: JoinHandle<()>,
    /// The client's own endpoint, which must live as long as its connection.
    _endpoint: Option<quinn::Endpoint>,
}

impl Drop for RawConnection {
    fn drop(&mut self) {
        self.connection.close(VarInt::from_u32(0), b"");
        self.reader.abort();
    }
}

impl RawConnection {
    /// Opens a QUIC connection to `target` and its control stream. Nothing is sent yet.
    ///
    /// # Errors
    ///
    /// When the handshake fails, as it must for a server that refuses the client's
    /// certificate or ALPN.
    pub async fn connect(target: &Target) -> Result<Self, Error> {
        let config = target.quinn_config()?;
        let bind = if target.addr.is_ipv6() {
            SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))
        } else {
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
        };
        let endpoint = quinn::Endpoint::client(bind)?;
        let connection = endpoint
            .connect_with(config, target.addr, &target.server_name)?
            .await?;
        let (send, recv) = connection.open_bi().await?;
        Ok(Self::start(
            connection,
            send,
            recv,
            Sender::Server,
            Some(endpoint),
        ))
    }

    /// Records a connection whose control stream is open, reading what `peer` sends.
    pub(crate) fn start(
        connection: quinn::Connection,
        send: SendStream,
        recv: RecvStream,
        peer: Sender,
        endpoint: Option<quinn::Endpoint>,
    ) -> Self {
        let (count, watcher) = watch::channel(0);
        let log = Arc::new(Log {
            opened: Instant::now(),
            records: Mutex::new(Vec::new()),
            count,
        });
        let reader = tokio::spawn(read(recv, connection.clone(), Arc::clone(&log), peer));
        Self {
            connection,
            send,
            log,
            cursor: 0,
            acknowledged: 0,
            count: watcher,
            reader,
            _endpoint: endpoint,
        }
    }

    /// Encodes and sends a packet.
    ///
    /// # Errors
    ///
    /// [`Error::Encode`] for a packet the codec refuses to encode, which
    /// [`send_bytes`](Self::send_bytes) sends anyway, and [`Error::Write`] when the stream
    /// is closed.
    pub async fn send(&mut self, packet: impl Into<Packet>) -> Result<(), Error> {
        let packet = packet.into();
        let mut bytes = BytesMut::new();
        packet.encode(&mut bytes)?;
        // Recorded before the write, which can take long enough for the peer's answer to
        // arrive first: a large packet the peer refuses from its fixed header.
        self.log.push(Recorded::Sent(packet));
        self.send.write_all(&bytes).await?;
        Ok(())
    }

    /// Sends bytes as they are: a malformed packet, an older protocol's CONNECT, or half a
    /// packet.
    ///
    /// # Errors
    ///
    /// [`Error::Write`] when the stream is closed.
    pub async fn send_bytes(&mut self, bytes: &[u8]) -> Result<(), Error> {
        self.log
            .push(Recorded::SentBytes(Bytes::copy_from_slice(bytes)));
        self.send.write_all(bytes).await?;
        Ok(())
    }

    /// Acknowledges, in arrival order, every PUBLISH at QoS 1 or 2 and every PUBREL received
    /// since the last call: PUBACK, PUBREC or PUBCOMP with Success, or, when `refuse` is set,
    /// PUBACK and PUBREC with 0x80 (Unspecified error). Returns how many it sent.
    ///
    /// # Errors
    ///
    /// [`Error::Write`] when the stream is closed.
    pub async fn acknowledge(&mut self, refuse: bool) -> Result<usize, Error> {
        let records = self.records();
        let owed: Vec<Packet> = records
            .get(self.acknowledged..)
            .unwrap_or_default()
            .iter()
            .filter_map(|record| match record.event.received()? {
                Packet::Publish(publish) => match (publish.qos, publish.packet_id) {
                    (QoS::AtLeastOnce, Some(id)) => Some(Packet::PubAck(PubAck {
                        reason_code: if refuse {
                            PubAckReasonCode::UnspecifiedError
                        } else {
                            PubAckReasonCode::Success
                        },
                        ..PubAck::new(id)
                    })),
                    (QoS::ExactlyOnce, Some(id)) => Some(Packet::PubRec(PubRec {
                        reason_code: if refuse {
                            PubRecReasonCode::UnspecifiedError
                        } else {
                            PubRecReasonCode::Success
                        },
                        ..PubRec::new(id)
                    })),
                    _ => None,
                },
                Packet::PubRel(pubrel) => Some(Packet::PubComp(PubComp::new(pubrel.packet_id))),
                _ => None,
            })
            .collect();
        self.acknowledged = records.len();
        for packet in &owed {
            self.send(packet.clone()).await?;
        }
        Ok(owed.len())
    }

    /// Sends the last packet or bytes sent again, a PUBLISH at QoS 1 or 2 with DUP set, as a
    /// retransmission is ([MQTT-3.3.1-1]).
    ///
    /// # Errors
    ///
    /// [`Error::Scenario`] when nothing was sent yet, and [`Error::Write`] when the stream is
    /// closed.
    pub async fn repeat(&mut self) -> Result<(), Error> {
        let last = self
            .records()
            .into_iter()
            .rev()
            .find_map(|record| match record.event {
                Recorded::Sent(packet) => Some(Ok(packet)),
                Recorded::SentBytes(bytes) => Some(Err(bytes)),
                _ => None,
            });
        match last {
            Some(Ok(mut packet)) => {
                if let Packet::Publish(publish) = &mut packet
                    && publish.qos != QoS::AtMostOnce
                {
                    publish.dup = true;
                }
                self.send(packet).await
            }
            Some(Err(bytes)) => self.send_bytes(&bytes).await,
            None => Err(Error::Scenario("nothing was sent to repeat".into())),
        }
    }

    /// The next record from the peer, or of the connection ending, not yet returned; `None`
    /// when nothing arrives within `timeout`.
    pub async fn recv(&mut self, timeout: Duration) -> Option<Record> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            self.count.borrow_and_update();
            if let Some(record) = self.next_unread() {
                return Some(record);
            }
            match tokio::time::timeout_at(deadline, self.count.changed()).await {
                Ok(Ok(())) => {}
                // The reader is gone; nothing more will come.
                Ok(Err(_)) => return self.next_unread(),
                Err(_) => return None,
            }
        }
    }

    /// The next packet from the peer, skipping nothing: `None` when the next record is not a
    /// packet or nothing arrives in time.
    pub async fn recv_packet(&mut self, timeout: Duration) -> Option<Packet> {
        match self.recv(timeout).await?.event {
            Recorded::Received(packet) => Some(packet),
            _ => None,
        }
    }

    /// Waits until the connection closes, taking every record before it, and returns how it
    /// closed; `None` if it is still open after `timeout`. A close already recorded is
    /// returned at once, even one an earlier call took.
    pub async fn closed(&mut self, timeout: Duration) -> Option<Close> {
        if let Some(close) = self
            .records()
            .into_iter()
            .find_map(|record| match record.event {
                Recorded::Closed(close) => Some(close),
                _ => None,
            })
        {
            self.cursor = self
                .log
                .records
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .len();
            return Some(close);
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if let Recorded::Closed(close) = self.recv(left).await?.event {
                return Some(close);
            }
        }
    }

    /// Waits until the peer ends the control stream or the connection closes, taking every
    /// record before it, and returns that record; `None` if neither happens within `timeout`.
    pub async fn ended(&mut self, timeout: Duration) -> Option<Recorded> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let event = self.recv(left).await?.event;
            if matches!(
                event,
                Recorded::StreamFinished | Recorded::StreamReset(_) | Recorded::Closed(_)
            ) {
                return Some(event);
            }
        }
    }

    fn next_unread(&mut self) -> Option<Record> {
        let records = self
            .log
            .records
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        while let Some(record) = records.get(self.cursor) {
            self.cursor += 1;
            if record.event.is_incoming() {
                return Some(record.clone());
            }
        }
        None
    }

    /// Everything recorded so far, both ways, in order.
    pub fn records(&self) -> Vec<Record> {
        self.log.snapshot()
    }

    /// Finishes this end's side of the control stream.
    pub fn finish(&mut self) {
        // Finishing twice is not an error a test cares about.
        drop(self.send.finish());
    }

    /// Closes the QUIC connection at once with an application error code, without a
    /// DISCONNECT: an abnormal close, after which a server publishes the Will Message.
    pub fn close(&self, code: u32) {
        self.connection.close(VarInt::from_u32(code), b"");
    }

    /// The certificate chain the peer presented, leaf first, if it presented one.
    pub fn peer_certificates(&self) -> Option<Vec<CertificateDer<'static>>> {
        self.connection
            .peer_identity()?
            .downcast::<Vec<CertificateDer<'static>>>()
            .ok()
            .map(|chain| *chain)
    }

    /// The ALPN protocol the handshake settled on.
    pub fn alpn(&self) -> Option<Vec<u8>> {
        self.connection
            .handshake_data()?
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .ok()?
            .protocol
    }
}

/// Reads the control stream until it ends, recording each packet as it completes, then
/// records how the connection closed.
async fn read(mut recv: RecvStream, connection: quinn::Connection, log: Arc<Log>, peer: Sender) {
    let decoder = Decoder::new().with_sender(peer);
    let mut buffer = BytesMut::new();
    loop {
        match recv.read_chunk(64 * 1024, true).await {
            Ok(Some(chunk)) => {
                buffer.extend_from_slice(&chunk.bytes);
                while let Some(frame) = next_frame(&mut buffer) {
                    log.push(match frame {
                        Ok(frame) => decode(decoder, frame),
                        Err(rest) => Recorded::Malformed {
                            bytes: rest,
                            error: "the Remaining Length is not a valid Variable Byte Integer"
                                .into(),
                        },
                    });
                }
            }
            Ok(None) => {
                if !buffer.is_empty() {
                    log.push(Recorded::Malformed {
                        bytes: buffer.split().freeze(),
                        error: "the stream ended inside a packet".into(),
                    });
                }
                log.push(Recorded::StreamFinished);
                break;
            }
            Err(ReadError::Reset(code)) => {
                log.push(Recorded::StreamReset(code.into_inner()));
                break;
            }
            Err(ReadError::ConnectionLost(error)) => {
                log.push(Recorded::Closed(Close::of(&error)));
                return;
            }
            Err(error) => {
                log.push(Recorded::Closed(Close::Other(error.to_string())));
                return;
            }
        }
    }
    // The stream ended; the connection's close follows, if the peer closes it.
    let error = connection.closed().await;
    log.push(Recorded::Closed(Close::of(&error)));
}

/// Decodes one whole frame.
fn decode(decoder: Decoder, frame: Bytes) -> Recorded {
    match decoder.decode(&mut BytesMut::from(&frame[..])) {
        Ok(Some(packet)) => Recorded::Received(packet),
        Ok(None) => Recorded::Malformed {
            bytes: frame,
            error: "the decoder wants more bytes than the frame holds".into(),
        },
        Err(error) => Recorded::Malformed {
            bytes: frame,
            error: error.to_string(),
        },
    }
}

/// Takes the next whole frame, the fixed header and as many bytes as its Remaining Length
/// says, off the front of `buffer`. `Some(Err(rest))` when the Remaining Length cannot be
/// read, which leaves nothing to frame by: everything is taken.
fn next_frame(buffer: &mut BytesMut) -> Option<Result<Bytes, Bytes>> {
    let mut remaining: usize = 0;
    let mut length_bytes = 0;
    loop {
        let byte = *buffer.get(1 + length_bytes)?;
        remaining |= usize::from(byte & 0x7F) << (7 * length_bytes);
        length_bytes += 1;
        if byte & 0x80 == 0 {
            break;
        }
        if length_bytes == 4 {
            return Some(Err(buffer.split().freeze()));
        }
    }
    let size = 1 + length_bytes + remaining;
    if buffer.len() < size {
        return None;
    }
    Some(Ok(buffer.split_to(size).freeze()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_are_cut_by_their_remaining_length() {
        let mut buffer = BytesMut::from(&[0xD0, 0x00, 0x20, 0x03, 0x00, 0x00][..]);
        assert_eq!(
            next_frame(&mut buffer),
            Some(Ok(Bytes::from_static(&[0xD0, 0x00])))
        );
        // A CONNACK announcing three bytes, with two here: wait for more.
        assert_eq!(next_frame(&mut buffer), None);
        buffer.extend_from_slice(&[0x00]);
        assert_eq!(
            next_frame(&mut buffer),
            Some(Ok(Bytes::from_static(&[0x20, 0x03, 0x00, 0x00, 0x00])))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn a_remaining_length_over_four_bytes_takes_the_rest() {
        let mut buffer = BytesMut::from(&[0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x02][..]);
        assert_eq!(
            next_frame(&mut buffer),
            Some(Err(Bytes::from_static(&[
                0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x02
            ])))
        );
        assert!(buffer.is_empty());
    }

    #[test]
    fn a_frame_that_does_not_decode_is_kept_with_its_reason() {
        // The MQTT 3.1.1 CONNACK, which has no Property Length.
        let recorded = decode(
            Decoder::new().with_sender(Sender::Server),
            Bytes::from_static(&[0x20, 0x02, 0x00, 0x00]),
        );
        let Recorded::Malformed { bytes, error } = recorded else {
            panic!("malformed");
        };
        assert_eq!(bytes.as_ref(), [0x20, 0x02, 0x00, 0x00]);
        assert!(error.contains("Property Length"), "{error}");
    }
}
