//! The client's one error type.

use std::io;

use openqtt_codec::{ConnAck, PacketType, QoS};

/// Why a call on the client failed.
///
/// A refusal by the server that the protocol carries in an acknowledgement, such as PUBACK
/// 0x87, is not an error: the call returns the acknowledgement and its reason code. These are
/// the failures that leave no acknowledgement to return.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The transport could not connect, or failed while connected.
    #[error("transport: {0}")]
    Transport(#[source] io::Error),
    /// rustls refused the TLS configuration.
    #[error("TLS configuration: {0}")]
    Tls(#[from] rustls::Error),
    /// The TLS configuration cannot be used for QUIC, which needs TLS 1.3 and its initial
    /// cipher suite.
    #[error("TLS configuration: {0}")]
    TlsForQuic(String),
    /// The server took longer than the connect timeout to answer CONNECT.
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),
    /// The server refused the connection; the CONNACK carries the reason code.
    #[error("the server refused the connection with reason code {:#04x}", .0.reason_code.value())]
    Refused(Box<ConnAck>),
    /// The server sent bytes that are not a valid packet from a server; the connection is
    /// closed.
    #[error("the server broke the protocol: {0}")]
    Protocol(#[source] openqtt_codec::Error),
    /// The server sent a packet that is not allowed where it came, such as anything but
    /// CONNACK in answer to CONNECT ([MQTT-3.2.0-1]).
    #[error("the server sent {0} where it is not allowed")]
    UnexpectedPacket(PacketType),
    /// The server answered Clean Start 0 with Session Present 1, but this client connected
    /// without session state, so it closed the connection ([MQTT-3.2.2-4]). Connect again with
    /// Clean Start 1, or resume with a [`Session`](crate::Session).
    #[error("the server resumed a session this client holds no state for")]
    UnexpectedSessionPresent,
    /// The server started enhanced authentication (AUTH), which this client does not do yet.
    #[error("the server asked for enhanced authentication, which this client does not support")]
    EnhancedAuthentication,
    /// The packet breaks a rule of the protocol, or does not fit the server's Maximum Packet
    /// Size ([MQTT-3.2.2-15]), so it was not sent.
    #[error("not sent: {0}")]
    Invalid(#[source] openqtt_codec::Error),
    /// The QoS is above the Maximum QoS the server announced ([MQTT-3.2.2-11]); not sent.
    #[error("not sent: {requested} is above the server's maximum, {maximum}")]
    QosNotSupported {
        /// The QoS asked for.
        requested: QoS,
        /// The server's Maximum QoS.
        maximum: QoS,
    },
    /// RETAIN is set, and the server announced Retain Available 0 ([MQTT-3.2.2-14]); not
    /// sent.
    #[error("not sent: the server does not support retained messages")]
    RetainNotSupported,
    /// The Topic Alias is above the server's Topic Alias Maximum ([MQTT-3.2.2-17],
    /// [MQTT-3.3.2-9]), or stands alone for a topic this connection never mapped it to; not
    /// sent.
    #[error("not sent: Topic Alias {alias} is not valid here (the server accepts 1 to {maximum})")]
    TopicAlias {
        /// The alias.
        alias: u16,
        /// The server's Topic Alias Maximum; 0 means it accepts none ([MQTT-3.2.2-18]).
        maximum: u16,
    },
    /// Every Packet Identifier is in use.
    #[error("no Packet Identifier is free")]
    PacketIdsExhausted,
    /// The connection is closed.
    #[error("the connection is closed")]
    Closed,
}
