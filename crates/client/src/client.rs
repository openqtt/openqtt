//! The client handle, its events, and the CONNECT exchange.

use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::BytesMut;
use openqtt_codec::{
    ConnAck, Decoder, Disconnect, Packet, PubAck, PubComp, PubRec, Publish, Sender, SubAck,
    SubscribeProperties, Subscription, SubscriptionOptions, UnsubAck, UnsubscribeProperties,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use crate::driver::{Command, Driver, Negotiated};
use crate::transport::{CloseCode, Transport};
use crate::{ConnectOptions, Error, Session};

/// The capacity of the channel that carries calls to the connection task.
const COMMAND_CAPACITY: usize = 64;

/// A connection to an MQTT 5 server.
///
/// [`Client::connect`] opens it and returns this handle with the [`Events`] that carry
/// incoming messages. The connection runs on a task of its own: it acknowledges what the
/// server sends, keeps the connection alive with PINGREQ, and holds the client's half of the
/// session. The handle is cheap to clone; every clone drives the same connection.
///
/// ```no_run
/// # async fn run(transport: openqtt_client::QuicTransport) -> Result<(), openqtt_client::Error> {
/// use openqtt_client::{Client, ConnectOptions, Publish, QoS, SubscriptionOptions};
///
/// let (client, mut events) = Client::connect(&transport, ConnectOptions::new("sensor-17")).await?;
/// client
///     .subscribe("commands/sensor-17/#", SubscriptionOptions {
///         maximum_qos: QoS::AtLeastOnce,
///         ..SubscriptionOptions::default()
///     })
///     .await?;
/// client
///     .publish(Publish {
///         qos: QoS::AtLeastOnce,
///         topic: "telemetry/sensor-17".into(),
///         payload: "21.5".into(),
///         ..Publish::default()
///     })
///     .await?;
/// if let Some(message) = events.next_message().await {
///     println!("{}: {:?}", message.topic, message.payload);
/// }
/// client.disconnect().await?;
/// # Ok(()) }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    commands: mpsc::Sender<Command>,
    shared: Arc<Shared>,
}

/// What every handle sees of the connection.
#[derive(Debug)]
pub(crate) struct Shared {
    /// The server's answer to CONNECT.
    pub(crate) connack: ConnAck,
    /// The Client Identifier the session is kept under.
    pub(crate) client_id: String,
    /// The client's half of the session, once the connection task has ended.
    pub(crate) session: Mutex<Option<Session>>,
}

impl Shared {
    /// Keeps the session of a connection that ended.
    pub(crate) fn keep_session(&self, session: Session) {
        *self.session.lock().unwrap_or_else(PoisonError::into_inner) = Some(session);
    }

    fn take_session(&self) -> Option<Session> {
        self.session
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

/// What the connection reports to the application, in the order it happens.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// An Application Message. The client has already sent the PUBACK or PUBREC its QoS calls
    /// for ([MQTT-4.5.0-2]), and resolved a Topic Alias, so the topic is never empty.
    Message(Publish),
    /// A packet as the server sent it, before the client acted on it. Only with
    /// [`ConnectOptions::packet_log`].
    Received(Packet),
    /// The connection ended. Always the last event.
    Closed(CloseReason),
}

/// Why a connection ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CloseReason {
    /// This client sent DISCONNECT.
    Disconnected,
    /// The server sent DISCONNECT, here.
    ByServer(Disconnect),
    /// The server broke the protocol. The client sent DISCONNECT with this reason code and
    /// closed the connection.
    ProtocolError {
        /// The DISCONNECT Reason Code sent.
        reason_code: openqtt_codec::DisconnectReasonCode,
        /// What was wrong.
        detail: String,
    },
    /// The connection was lost without a DISCONNECT, or the server stopped answering PINGREQ.
    Lost(String),
}

/// The receiving end of a connection: its [`Event`]s, in order.
///
/// When the application lets events pile up past
/// [`ConnectOptions::event_capacity`], the client stops reading from the server until it
/// catches up. Dropping it discards events from then on, and the connection carries on.
#[derive(Debug)]
pub struct Events {
    rx: mpsc::Receiver<Event>,
}

impl Events {
    /// The next event, or `None` once [`Event::Closed`] has been taken.
    pub async fn recv(&mut self) -> Option<Event> {
        self.rx.recv().await
    }

    /// The next Application Message, skipping every other event, or `None` once the
    /// connection has ended.
    pub async fn next_message(&mut self) -> Option<Publish> {
        while let Some(event) = self.rx.recv().await {
            if let Event::Message(publish) = event {
                return Some(publish);
            }
        }
        None
    }
}

/// How a PUBLISH ended.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Published {
    /// QoS 0: handed to the transport. Nothing comes back.
    AtMostOnce,
    /// QoS 1: the server's PUBACK. A reason code of 0x80 or above means it refused the message.
    AtLeastOnce(PubAck),
    /// QoS 2: the server's PUBREC and, when the PUBREC accepted the message, its PUBCOMP. A
    /// PUBREC of 0x80 or above ends the exchange without a PUBREL ([MQTT-4.3.3-4]).
    ExactlyOnce {
        /// The PUBREC.
        pubrec: PubRec,
        /// The PUBCOMP, absent when the PUBREC refused the message.
        pubcomp: Option<PubComp>,
    },
}

impl Published {
    /// Whether the server took the message: always for QoS 0, and for QoS 1 and 2 when its
    /// reason codes are below 0x80.
    pub fn is_accepted(&self) -> bool {
        match self {
            Self::AtMostOnce => true,
            Self::AtLeastOnce(puback) => !puback.reason_code.is_error(),
            Self::ExactlyOnce { pubrec, pubcomp } => {
                !pubrec.reason_code.is_error()
                    && pubcomp
                        .as_ref()
                        .is_some_and(|pubcomp| !pubcomp.reason_code.is_error())
            }
        }
    }
}

impl Client {
    /// Opens a connection over `transport` and sends CONNECT, and returns once the server
    /// accepted it with CONNACK.
    ///
    /// # Errors
    ///
    /// [`Error::Refused`] when the CONNACK refuses the connection, [`Error::Timeout`] when it
    /// does not come within [`ConnectOptions::connect_timeout`], and the errors of a broken
    /// transport or protocol.
    pub async fn connect(
        transport: &(impl Transport + ?Sized),
        options: ConnectOptions,
    ) -> Result<(Self, Events), Error> {
        let timeout = options.connect_timeout;
        tokio::time::timeout(timeout, Self::handshake(transport, options))
            .await
            .map_err(|_| Error::Timeout("CONNACK"))?
    }

    async fn handshake(
        transport: &(impl Transport + ?Sized),
        options: ConnectOptions,
    ) -> Result<(Self, Events), Error> {
        let ConnectOptions {
            connect,
            session,
            ping_timeout,
            packet_log,
            event_capacity,
            ..
        } = options;
        let has_state = session.is_some();
        let mut session = session.unwrap_or_else(|| Session::new(connect.client_id.clone()));

        let link = transport.connect().await.map_err(Error::Transport)?;
        let (mut reader, mut writer, handle) = (link.reader, link.writer, link.handle);

        let connect_packet = Packet::from(connect.clone());
        connect_packet
            .check_sender(Sender::Client)
            .map_err(Error::Invalid)?;
        let mut out = BytesMut::new();
        connect_packet.encode(&mut out).map_err(Error::Invalid)?;
        writer.write_all(&out).await.map_err(Error::Transport)?;

        let mut decoder = Decoder::new().with_sender(Sender::Server);
        if let Some(maximum) = connect.properties.maximum_packet_size {
            decoder = decoder.with_max_packet_size(maximum);
        }
        let mut buffer = BytesMut::with_capacity(8 * 1024);
        let connack = loop {
            match decoder.decode(&mut buffer) {
                Ok(Some(Packet::ConnAck(connack))) => break connack,
                Ok(Some(Packet::Auth(_))) => {
                    handle.close(CloseCode::ProtocolError);
                    return Err(Error::EnhancedAuthentication);
                }
                // CONNACK comes before anything else the server sends ([MQTT-3.2.0-1]).
                Ok(Some(other)) => {
                    handle.close(CloseCode::ProtocolError);
                    return Err(Error::UnexpectedPacket(other.packet_type()));
                }
                Ok(None) => {
                    let read = reader
                        .read_buf(&mut buffer)
                        .await
                        .map_err(Error::Transport)?;
                    if read == 0 {
                        return Err(Error::Transport(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "the server closed the connection before CONNACK",
                        )));
                    }
                }
                Err(error) => {
                    handle.close(CloseCode::ProtocolError);
                    return Err(Error::Protocol(error));
                }
            }
        };
        if connack.reason_code.is_error() {
            // A refusing CONNACK is the server's last packet ([MQTT-3.2.2-7]).
            handle.close(CloseCode::NoError);
            return Err(Error::Refused(connack));
        }
        if connack.session_present && !has_state {
            // [MQTT-3.2.2-4]
            handle.close(CloseCode::ProtocolError);
            return Err(Error::UnexpectedSessionPresent);
        }
        if !connack.session_present {
            // [MQTT-3.2.2-5]
            session.clear();
        }
        if let Some(assigned) = &connack.properties.assigned_client_identifier {
            session.client_id.clone_from(assigned);
        }

        let negotiated = Negotiated::new(&connect, &connack, ping_timeout);
        let (commands_tx, commands_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (events_tx, events_rx) = mpsc::channel(event_capacity);
        let shared = Arc::new(Shared {
            client_id: session.client_id.clone(),
            connack: (*connack).clone(),
            session: Mutex::new(None),
        });
        let driver = Driver::new(
            crate::Link {
                reader,
                writer,
                handle,
            },
            buffer,
            decoder,
            session,
            negotiated,
            commands_rx,
            events_tx,
            packet_log.then(|| Packet::ConnAck(connack)),
            Arc::clone(&shared),
        );
        tokio::spawn(driver.run());
        Ok((
            Self {
                commands: commands_tx,
                shared,
            },
            Events { rx: events_rx },
        ))
    }

    /// The server's CONNACK.
    pub fn connack(&self) -> &ConnAck {
        &self.shared.connack
    }

    /// The Client Identifier the session is kept under: the one sent in CONNECT, or the one
    /// the server assigned ([MQTT-3.1.3-7]).
    pub fn client_id(&self) -> &str {
        &self.shared.client_id
    }

    /// Whether the connection has ended.
    pub fn is_closed(&self) -> bool {
        self.commands.is_closed()
    }

    /// Publishes an Application Message, and returns how the server answered.
    ///
    /// The client assigns the Packet Identifier and clears DUP. At QoS 1 and 2 the call waits
    /// for the acknowledgement, and for a free slot of the server's Receive Maximum before
    /// sending ([MQTT-3.3.4-7]). A Topic Alias is sent as given, after a check against the
    /// server's Topic Alias Maximum.
    ///
    /// # Errors
    ///
    /// [`Error::QosNotSupported`], [`Error::RetainNotSupported`], [`Error::TopicAlias`] or
    /// [`Error::Invalid`] when the message breaks a rule or a limit of the server, in which
    /// case nothing is sent; [`Error::Closed`] when the connection ended first. A QoS 1 or 2
    /// message in flight when it ended stays in the session, and is sent again if the session
    /// resumes.
    pub async fn publish(&self, publish: Publish) -> Result<Published, Error> {
        self.request(|reply| Command::Publish { publish, reply })
            .await
    }

    /// Subscribes to one Topic Filter.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] when the connection ended before the SUBACK, and [`Error::Invalid`]
    /// for a SUBSCRIBE the server would refuse as malformed.
    pub async fn subscribe(
        &self,
        filter: impl Into<String>,
        options: SubscriptionOptions,
    ) -> Result<SubAck, Error> {
        self.subscribe_many(
            vec![Subscription {
                filter: filter.into(),
                options,
            }],
            SubscribeProperties::default(),
        )
        .await
    }

    /// Subscribes to several Topic Filters in one SUBSCRIBE, with its properties.
    ///
    /// # Errors
    ///
    /// As [`subscribe`](Self::subscribe).
    pub async fn subscribe_many(
        &self,
        subscriptions: Vec<Subscription>,
        properties: SubscribeProperties,
    ) -> Result<SubAck, Error> {
        self.request(|reply| Command::Subscribe {
            subscriptions,
            properties,
            reply,
        })
        .await
    }

    /// Unsubscribes from one Topic Filter.
    ///
    /// # Errors
    ///
    /// As [`subscribe`](Self::subscribe).
    pub async fn unsubscribe(&self, filter: impl Into<String>) -> Result<UnsubAck, Error> {
        self.unsubscribe_many(vec![filter.into()], UnsubscribeProperties::default())
            .await
    }

    /// Unsubscribes from several Topic Filters in one UNSUBSCRIBE, with its properties.
    ///
    /// # Errors
    ///
    /// As [`subscribe`](Self::subscribe).
    pub async fn unsubscribe_many(
        &self,
        filters: Vec<String>,
        properties: UnsubscribeProperties,
    ) -> Result<UnsubAck, Error> {
        self.request(|reply| Command::Unsubscribe {
            filters,
            properties,
            reply,
        })
        .await
    }

    /// Sends DISCONNECT with reason code 0x00, so the server discards the Will Message
    /// ([MQTT-3.14.4-3]), closes the connection, and returns the client's half of the session
    /// for [`ConnectOptions::resume`].
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] when another handle already disconnected. When the connection had
    /// ended by itself, nothing is sent and the session is returned as it was left.
    pub async fn disconnect(&self) -> Result<Session, Error> {
        self.disconnect_with(Disconnect::default()).await
    }

    /// [`disconnect`](Self::disconnect) with a DISCONNECT of the caller's: 0x04 to have the
    /// Will Message published, or a new Session Expiry Interval. A Reason String or User
    /// Properties that would take it past the server's Maximum Packet Size are left out
    /// ([MQTT-3.14.2-3], [MQTT-3.14.2-4]).
    ///
    /// # Errors
    ///
    /// As [`disconnect`](Self::disconnect), and [`Error::Invalid`] for a DISCONNECT a client
    /// may not send or that does not encode. Nothing is sent then, and the connection stays
    /// open.
    pub async fn disconnect_with(&self, disconnect: Disconnect) -> Result<Session, Error> {
        Packet::Disconnect(disconnect.clone())
            .check_sender(Sender::Client)
            .map_err(Error::Invalid)?;
        let (reply, response) = oneshot::channel();
        if self
            .commands
            .send(Command::Disconnect { disconnect, reply })
            .await
            .is_err()
        {
            return self.shared.take_session().ok_or(Error::Closed);
        }
        match response.await {
            Ok(result) => result,
            Err(_) => self.shared.take_session().ok_or(Error::Closed),
        }
    }

    /// Sends a command and waits for its reply.
    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, Error>>) -> Command,
    ) -> Result<T, Error> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(command(reply))
            .await
            .map_err(|_| Error::Closed)?;
        response.await.map_err(|_| Error::Closed)?
    }
}
