//! Sending a client to another server (section 4.11).

use std::fmt;

use crate::{ClientInfo, Error};

/// Which redirect the server sends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RedirectKind {
    /// 0x9C Use another server: for now, as on a drain or when an edge fences itself (R3).
    UseAnotherServer,
    /// 0x9D Server moved: for good.
    ServerMoved,
}

impl RedirectKind {
    /// The reason code, in a DISCONNECT or a CONNACK.
    pub const fn code(self) -> u8 {
        match self {
            Self::UseAnotherServer => 0x9C,
            Self::ServerMoved => 0x9D,
        }
    }
}

/// Why the server sends the client away.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RedirectCause {
    /// The edge is shutting down and drains its clients (R3, Sessions).
    Drain,
    /// The edge could not renew its lease and fences itself (R3, Membership and placement).
    Fenced,
    /// An operator moved the client.
    Moved,
}

/// A client about to be sent away.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Redirect {
    /// The client.
    pub client: ClientInfo,
    /// The reason code it will get.
    pub kind: RedirectKind,
    /// Why.
    pub cause: RedirectCause,
}

impl Redirect {
    /// `client` gets `kind`, because of `cause`.
    pub fn new(client: ClientInfo, kind: RedirectKind, cause: RedirectCause) -> Self {
        Self {
            client,
            kind,
            cause,
        }
    }
}

/// A Server Reference: where the client should connect instead, as the server tells it
/// (section 4.11). Its format is the deployment's; MQTT suggests a host, an optional port, and
/// a space between several.
#[derive(Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct ServerReference(Box<str>);

impl ServerReference {
    /// The reference `text`.
    ///
    /// # Errors
    ///
    /// [`Error::Invalid`] when it is empty, holds U+0000, or is longer than a UTF-8 Encoded
    /// String can be.
    pub fn new(text: &str) -> Result<Self, Error> {
        let reason = if text.is_empty() {
            "it is empty"
        } else if text.len() > 65_535 {
            "it is longer than 65,535 bytes"
        } else if text.contains('\0') {
            "it contains U+0000"
        } else {
            return Ok(Self(text.into()));
        };
        Err(Error::Invalid {
            what: "Server Reference",
            reason,
        })
    }

    /// The reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ServerReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ServerReference")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for ServerReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Chooses the Server Reference sent with 0x9C and 0x9D.
///
/// It is synchronous because a drain sends a DISCONNECT to every client of an edge within its
/// grace period; a policy that needs data from elsewhere keeps it in memory.
pub trait RedirectPolicy: Send + Sync + 'static {
    /// The reference to send with `redirect`, or `None` to send none, which leaves the client
    /// to its own list of servers.
    fn server_reference(&self, redirect: &Redirect) -> Option<ServerReference>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_server_reference_is_a_non_empty_string_without_null() {
        let reference = ServerReference::new("edge-2.example.net:14567").unwrap();
        assert_eq!(reference.as_str(), "edge-2.example.net:14567");
        assert_eq!(reference.to_string(), "edge-2.example.net:14567");
        for (text, reason) in [
            ("", "it is empty"),
            ("a\0b", "it contains U+0000"),
            (&"x".repeat(65_536), "it is longer than 65,535 bytes"),
        ] {
            assert_eq!(
                ServerReference::new(text),
                Err(Error::Invalid {
                    what: "Server Reference",
                    reason
                })
            );
        }
        assert_eq!(RedirectKind::UseAnotherServer.code(), 0x9C);
        assert_eq!(RedirectKind::ServerMoved.code(), 0x9D);
    }
}
