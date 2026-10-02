//! The codec's one error type.

use crate::{ConnectReasonCode, DisconnectReasonCode};

/// Why bytes could not be decoded as a packet, or a packet could not be encoded.
///
/// Each variant names the rule it enforces, by statement number where the specification gives
/// one. A decoding error ends the connection: section 4.13 has the receiver close it, after a
/// CONNACK or DISCONNECT carrying the reason code the error maps to.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A Variable Byte Integer runs past four bytes, or is longer than its value needs
    /// ([MQTT-1.5.5-1]).
    #[error("the {field} is not a valid Variable Byte Integer")]
    MalformedVariableByteInteger {
        /// The field holding it.
        field: &'static str,
    },
    /// The packet, as long as its Remaining Length says, ends inside a field.
    #[error("the packet ends inside the {field}")]
    Truncated {
        /// The field cut short.
        field: &'static str,
    },
    /// A UTF-8 Encoded String is not well-formed UTF-8, which includes encoding a surrogate
    /// ([MQTT-1.5.4-1]).
    #[error("the {field} is not well-formed UTF-8")]
    InvalidUtf8 {
        /// The field holding the string.
        field: &'static str,
    },
    /// A UTF-8 Encoded String contains the null character U+0000 ([MQTT-1.5.4-2]).
    #[error("the {field} contains the null character U+0000")]
    NullCharacter {
        /// The field holding the string.
        field: &'static str,
    },
    /// Encoding only: a string or Binary Data value is longer than its Two Byte Integer length
    /// prefix can say (sections 1.5.4 and 1.5.6).
    #[error("the {field} is {len} bytes long, more than the 65535 a length prefix can say")]
    TooLong {
        /// The field holding the value.
        field: &'static str,
        /// Its length in bytes.
        len: usize,
    },
}

/// Which reason code an error calls for: the kinds section 4.13.1 distinguishes.
enum Class {
    /// The packet cannot be parsed: 0x81 Malformed Packet.
    Malformed,
    /// A packet this side built cannot be encoded. Nothing was received, so there is nothing
    /// to tell the peer; 0x83 Implementation specific error stands in should a caller report
    /// it anyway.
    Local,
}

impl Error {
    /// The kind of error, which decides its reason code.
    const fn class(&self) -> Class {
        match self {
            Self::MalformedVariableByteInteger { .. }
            | Self::Truncated { .. }
            | Self::InvalidUtf8 { .. }
            | Self::NullCharacter { .. } => Class::Malformed,
            Self::TooLong { .. } => Class::Local,
        }
    }

    /// The reason code of the CONNACK a server may send, before closing the connection, when
    /// the CONNECT fails to decode (sections 3.1.4 and 4.13.1).
    pub const fn connack_reason_code(&self) -> ConnectReasonCode {
        match self.class() {
            Class::Malformed => ConnectReasonCode::MalformedPacket,
            Class::Local => ConnectReasonCode::ImplementationSpecificError,
        }
    }

    /// The reason code of the DISCONNECT that closes a connection when any packet after the
    /// CONNECT fails to decode (section 4.13.1).
    pub const fn disconnect_reason_code(&self) -> DisconnectReasonCode {
        match self.class() {
            Class::Malformed => DisconnectReasonCode::MalformedPacket,
            Class::Local => DisconnectReasonCode::ImplementationSpecificError,
        }
    }
}
