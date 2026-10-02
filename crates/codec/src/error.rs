//! The codec's one error type.

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
