//! The transport's one error type.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;

use bytes::Bytes;
use openqtt_codec::{DisconnectReasonCode, PacketType};

use crate::StreamTag;

/// Why a listener cannot be set up, a connection cannot go on, or a packet cannot be sent.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A setting of the listener is missing, out of the range the transport supports, or at
    /// odds with another.
    #[error("{setting}: {reason}")]
    Setting {
        /// The setting, dotted as in the configuration file where it comes from there.
        setting: String,
        /// Why it is refused.
        reason: String,
    },
    /// A file a setting names cannot be read, or does not hold what it should. The reason never
    /// quotes the file, which may hold a private key.
    #[error("{setting}: {}: {reason}", .path.display())]
    File {
        /// The setting that names the file.
        setting: String,
        /// The file.
        path: PathBuf,
        /// What is wrong with it.
        reason: String,
    },
    /// rustls refused the TLS configuration, such as a key that does not match its certificate.
    #[error("TLS: {0}")]
    Tls(#[from] rustls::Error),
    /// The UDP socket of an endpoint cannot be set up.
    #[error("cannot bind {address}: {source}")]
    Bind {
        /// The address.
        address: SocketAddr,
        /// Why.
        #[source]
        source: io::Error,
    },
    /// A connection attempt ended before its handshake completed: refused for its certificate
    /// or its application protocol, or silent for too long.
    #[error("the handshake failed: {0}")]
    Handshake(Closed),
    /// Bytes on a stream do not decode as a packet the client may send. The session answers
    /// with the CONNACK or DISCONNECT whose reason code the codec error names, then closes the
    /// connection (MQTT 5.0, section 4.13).
    #[error("{stream}: {error}")]
    Decode {
        /// The stream.
        stream: StreamTag,
        /// What the codec found.
        #[source]
        error: openqtt_codec::Error,
    },
    /// The client broke the mapping of MQTT onto streams (docs/spec/mqtt-over-quic.md, section
    /// 2). [`Violation::disconnect_reason_code`] says how the session answers.
    #[error("{stream}: {violation}")]
    Violation {
        /// The stream.
        stream: StreamTag,
        /// The rule broken.
        violation: Violation,
    },
    /// The connection is closed: nothing more is sent or received.
    #[error("the connection is closed: {0}")]
    Closed(Closed),
    /// A packet handed to [`send`](crate::MqttConnection::send) cannot be encoded.
    #[error("cannot encode a {packet_type}: {error}")]
    Encode {
        /// The packet's type.
        packet_type: PacketType,
        /// Why the codec refused it.
        #[source]
        error: openqtt_codec::Error,
    },
    /// A packet handed to [`send`](crate::MqttConnection::send) is one the stream may not carry:
    /// on a data stream, a packet of the control stream or a PUBLISH with a Topic Alias
    /// (sections 2.1 and 2.3).
    #[error("{stream} cannot carry a {packet_type} like this one")]
    WrongStream {
        /// The stream.
        stream: StreamTag,
        /// The packet's type.
        packet_type: PacketType,
    },
    /// The server's side of the stream is not open: the connection has no such stream, or that
    /// side was finished, reset or stopped.
    #[error("{stream} is not open for sending")]
    StreamClosed {
        /// The stream.
        stream: StreamTag,
    },
}

/// A rule of docs/spec/mqtt-over-quic.md, section 2, broken by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Violation {
    /// The control stream did not begin with CONNECT ([MQTT-3.1.0-1]).
    FirstPacketNotConnect {
        /// The type of the packet it began with.
        packet_type: PacketType,
    },
    /// CONNECT, PINGREQ, DISCONNECT or AUTH arrived on a data stream: they travel only on the
    /// control stream (section 2.1).
    ControlPacketOnDataStream {
        /// The packet's type.
        packet_type: PacketType,
    },
    /// A PUBLISH on a data stream carried a Topic Alias, which travels on the control stream
    /// only (section 2.3).
    TopicAliasOnDataStream,
    /// The client finished its side of the stream in the middle of a packet.
    EndedInsidePacket,
}

impl Violation {
    /// The reason code of the DISCONNECT the session sends before closing the connection, or
    /// `None` when it closes without a reply. Before the CONNACK, a refusal is a CONNACK with the
    /// same code instead (report R1, D4).
    ///
    /// A first packet other than CONNECT gets no reply (report R1, MQTT-3.1.0-1). A packet of
    /// the control stream, or a Topic Alias, on a data stream is a Protocol Error, 0x82
    /// (sections 2.1 and 2.3). A packet cut short is a Malformed Packet, 0x81.
    pub const fn disconnect_reason_code(&self) -> Option<DisconnectReasonCode> {
        match self {
            Self::FirstPacketNotConnect { .. } => None,
            Self::ControlPacketOnDataStream { .. } | Self::TopicAliasOnDataStream => {
                Some(DisconnectReasonCode::ProtocolError)
            }
            Self::EndedInsidePacket => Some(DisconnectReasonCode::MalformedPacket),
        }
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FirstPacketNotConnect { packet_type } => {
                write!(f, "the first packet is a {packet_type}, not CONNECT")
            }
            Self::ControlPacketOnDataStream { packet_type } => {
                write!(f, "a {packet_type} travels only on the control stream")
            }
            Self::TopicAliasOnDataStream => {
                f.write_str("a Topic Alias travels only on the control stream")
            }
            Self::EndedInsidePacket => f.write_str("the stream ended inside a packet"),
        }
    }
}

/// How a connection closed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Closed {
    /// The client closed it with this application error code (docs/spec/mqtt-over-quic.md,
    /// section 8): 0 after a DISCONNECT, any other for an abnormal close.
    Application {
        /// The code.
        code: u64,
        /// The reason phrase.
        reason: Bytes,
    },
    /// A QUIC transport error closed it, sent by the client or found by this end: a handshake
    /// the TLS stack refused (0x100 plus the TLS alert), or a limit of the connection broken.
    Transport {
        /// The code.
        code: u64,
        /// The reason phrase.
        reason: String,
    },
    /// Nothing arrived within the idle timeout.
    TimedOut,
    /// The client lost the connection's state and said so with a stateless reset.
    Reset,
    /// This end closed it.
    Locally,
    /// Anything else, as the QUIC stack describes it.
    Other(String),
}

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Application { code, .. } => write!(f, "closed by the client with code {code}"),
            Self::Transport { code, reason } => {
                write!(f, "QUIC transport error {code:#x}: {reason}")
            }
            Self::TimedOut => f.write_str("idle for longer than the timeout"),
            Self::Reset => f.write_str("reset by the client"),
            Self::Locally => f.write_str("closed by the server"),
            Self::Other(detail) => f.write_str(detail),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn violations_map_to_the_reason_codes_of_r1() {
        assert_eq!(
            Violation::FirstPacketNotConnect {
                packet_type: PacketType::PingReq
            }
            .disconnect_reason_code(),
            None
        );
        assert_eq!(
            Violation::ControlPacketOnDataStream {
                packet_type: PacketType::Disconnect
            }
            .disconnect_reason_code(),
            Some(DisconnectReasonCode::ProtocolError)
        );
        assert_eq!(
            Violation::TopicAliasOnDataStream.disconnect_reason_code(),
            Some(DisconnectReasonCode::ProtocolError)
        );
        assert_eq!(
            Violation::EndedInsidePacket.disconnect_reason_code(),
            Some(DisconnectReasonCode::MalformedPacket)
        );
    }

    #[test]
    fn errors_name_the_stream() {
        let error = Error::Violation {
            stream: StreamTag::Data(2),
            violation: Violation::ControlPacketOnDataStream {
                packet_type: PacketType::PingReq,
            },
        };
        assert_eq!(
            error.to_string(),
            "data stream 2: a PINGREQ travels only on the control stream"
        );
        let closed = Error::Closed(Closed::Application {
            code: 0,
            reason: Bytes::new(),
        });
        assert_eq!(
            closed.to_string(),
            "the connection is closed: closed by the client with code 0"
        );
    }
}
