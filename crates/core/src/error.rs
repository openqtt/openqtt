//! The core crate's one error type.

/// Why a value cannot be one of the core types.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An empty Client Identifier. A client sends one to have the server assign it one
    /// ([MQTT-3.1.3-6]), so it never names a session.
    #[error("a Client Identifier names a session and cannot be empty")]
    EmptyClientId,
    /// A Client Identifier over 256 bytes (report R1, O10), which the session refuses with
    /// CONNACK 0x85 ([MQTT-3.1.3-8]).
    #[error("a Client Identifier is at most 256 bytes, not {len}")]
    ClientIdTooLong {
        /// Its length in bytes.
        len: usize,
    },
    /// A User Name longer than a UTF-8 Encoded String can be.
    #[error("a User Name is at most 65,535 bytes, not {len}")]
    UsernameTooLong {
        /// Its length in bytes.
        len: usize,
    },
    /// The null character, which no UTF-8 Encoded String holds ([MQTT-1.5.4-2]).
    #[error("the {field} contains the null character U+0000")]
    NullCharacter {
        /// What held it.
        field: &'static str,
    },
}
