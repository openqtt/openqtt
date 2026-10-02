//! Client Identifiers and User Names.

use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use crate::Error;

/// The characters every server accepts in a Client Identifier ([MQTT-3.1.3-5]), in the order
/// the specification lists them. An assigned identifier is written in them.
const ALPHABET: &[u8; 62] = b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
/// What an assigned identifier starts with (report R1, O9).
const ASSIGNED_PREFIX: &str = "oq";
/// The random characters after it.
const ASSIGNED_DIGITS: usize = 21;
/// How many assigned identifiers there are: 62^21, a little over 2^125.
const ASSIGNED_SPACE: u128 = 62u128.pow(21);

/// A Client Identifier: the name a session is kept under ([MQTT-3.1.3-2]), and the key that
/// places it on a log partition ([`partition_of`](crate::partition_of)).
///
/// One to 256 bytes of UTF-8 without U+0000 (report R1, O10 and D25). That takes in every
/// identifier of 1 to 23 characters from `0-9a-zA-Z`, which every server must accept
/// ([MQTT-3.1.3-5]); the session refuses a longer one with CONNACK 0x85 ([MQTT-3.1.3-8]). A
/// CONNECT with an empty one asks the server for one ([MQTT-3.1.3-6]), which the session draws
/// with [`ClientId::assigned`].
///
/// Identifiers compare, hash and order byte for byte. A clone shares the text.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ClientId(Arc<str>);

impl ClientId {
    /// The longest identifier accepted, in bytes (R1, O10). The identifier is a key in the log
    /// (`own/{cid}`, R3), so it is bounded well below what a CONNECT could carry.
    pub const MAX_LEN: usize = 256;

    /// The length of an assigned identifier (R1, O9).
    pub const ASSIGNED_LEN: usize = ASSIGNED_PREFIX.len() + ASSIGNED_DIGITS;

    /// Checks `id` and takes a copy of it.
    ///
    /// # Errors
    ///
    /// [`Error::EmptyClientId`], [`Error::ClientIdTooLong`] or [`Error::NullCharacter`].
    pub fn new(id: &str) -> Result<Self, Error> {
        if id.is_empty() {
            return Err(Error::EmptyClientId);
        }
        if id.len() > Self::MAX_LEN {
            return Err(Error::ClientIdTooLong { len: id.len() });
        }
        if id.contains('\0') {
            return Err(Error::NullCharacter {
                field: "Client Identifier",
            });
        }
        Ok(Self(id.into()))
    }

    /// The identifier to assign a client that sent an empty one ([MQTT-3.1.3-6],
    /// [MQTT-3.1.3-7]), made from 128 bits the caller drew from a random source: `oq` and 21
    /// characters from `0-9a-zA-Z`, 23 in all, the form every server accepts (R1, O9).
    ///
    /// The 21 characters are `random` modulo 62^21 written in base 62. At most eight of the
    /// 2^128 values of `random` give any one identifier, so one made from uniform bits carries
    /// at least 125 bits of randomness. Uniqueness is the log's to check: the claim of a session
    /// refuses an identifier in use, and the session draws again (R1, O9).
    pub fn assigned(random: u128) -> Self {
        let mut value = random % ASSIGNED_SPACE;
        let mut digits = [0u8; ASSIGNED_DIGITS];
        for digit in digits.iter_mut().rev() {
            // A remainder of division by 62 fits in usize.
            *digit = ALPHABET[(value % 62) as usize];
            value /= 62;
        }
        let mut id = String::with_capacity(Self::ASSIGNED_LEN);
        id.push_str(ASSIGNED_PREFIX);
        id.extend(digits.iter().map(|&b| char::from(b)));
        Self(id.into())
    }

    /// The identifier.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ClientId").field(&self.as_str()).finish()
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ClientId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for ClientId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl FromStr for ClientId {
    type Err = Error;

    fn from_str(id: &str) -> Result<Self, Error> {
        Self::new(id)
    }
}

impl TryFrom<&str> for ClientId {
    type Error = Error;

    fn try_from(id: &str) -> Result<Self, Error> {
        Self::new(id)
    }
}

/// A User Name: what the client sent in its CONNECT, or on a listener configured for
/// certificate identity the certificate's CN (report R2, rule 4).
///
/// Any UTF-8 without U+0000, up to 65,535 bytes, empty included, since a CONNECT may carry an
/// empty one. It may contain `/`, as production's CNs do, which matters to a mountpoint
/// (`openqtt_topic::Mountpoint`). A clone shares the text.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Username(Arc<str>);

impl Username {
    /// The longest User Name, in bytes: what a UTF-8 Encoded String holds.
    pub const MAX_LEN: usize = 65_535;

    /// Checks `name` and takes a copy of it.
    ///
    /// # Errors
    ///
    /// [`Error::UsernameTooLong`] or [`Error::NullCharacter`].
    pub fn new(name: &str) -> Result<Self, Error> {
        if name.len() > Self::MAX_LEN {
            return Err(Error::UsernameTooLong { len: name.len() });
        }
        if name.contains('\0') {
            return Err(Error::NullCharacter { field: "User Name" });
        }
        Ok(Self(name.into()))
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Username {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Username").field(&self.as_str()).finish()
    }
}

impl fmt::Display for Username {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for Username {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for Username {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl FromStr for Username {
    type Err = Error;

    fn from_str(name: &str) -> Result<Self, Error> {
        Self::new(name)
    }
}

impl TryFrom<&str> for Username {
    type Error = Error;

    fn try_from(name: &str) -> Result<Self, Error> {
        Self::new(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_client_identifier_is_1_to_256_bytes() {
        assert_eq!(ClientId::new(""), Err(Error::EmptyClientId));
        ClientId::new("a").unwrap();
        let longest = "x".repeat(ClientId::MAX_LEN);
        assert_eq!(ClientId::new(&longest).unwrap().as_str(), longest);
        assert_eq!(
            ClientId::new(&format!("{longest}x")),
            Err(Error::ClientIdTooLong { len: 257 })
        );
        // Bytes count, not characters.
        let wide = "\u{e9}".repeat(128);
        assert_eq!(wide.len(), 256);
        ClientId::new(&wide).unwrap();
        assert_eq!(
            ClientId::new(&format!("{wide}a")),
            Err(Error::ClientIdTooLong { len: 257 })
        );
    }

    #[test]
    fn every_identifier_the_specification_requires_is_accepted() {
        // Each character the server must allow, alone and filling 23 characters.
        for &c in ALPHABET {
            let c = char::from(c);
            ClientId::new(&c.to_string()).unwrap();
            ClientId::new(&c.to_string().repeat(23)).unwrap();
        }
        ClientId::new("0123456789abcdefghijklm").unwrap();
    }

    #[test]
    fn any_text_but_the_null_character_is_accepted() {
        for id in [
            "client/1",
            "caf\u{e9}",
            "a b",
            "$SYS",
            "+#",
            "\u{1f600}",
            "\t",
        ] {
            assert_eq!(ClientId::new(id).unwrap().to_string(), id);
        }
        assert_eq!(
            ClientId::new("a\0b"),
            Err(Error::NullCharacter {
                field: "Client Identifier"
            })
        );
    }

    #[test]
    fn assigned_identifiers_are_pinned() {
        let cases: [(u128, &str); 5] = [
            (0, "oq000000000000000000000"),
            (1, "oq000000000000000000001"),
            (61, "oq00000000000000000000Z"),
            (62, "oq000000000000000000010"),
            (ASSIGNED_SPACE - 1, "oqZZZZZZZZZZZZZZZZZZZZZ"),
        ];
        for (random, id) in cases {
            assert_eq!(ClientId::assigned(random).as_str(), id, "{random}");
        }
        // The space wraps.
        assert_eq!(ClientId::assigned(ASSIGNED_SPACE), ClientId::assigned(0));
        assert_eq!(
            ClientId::assigned(u128::MAX).as_str(),
            "oqN42dgm5tFLK9N8MT7fHC7"
        );
    }

    #[test]
    fn an_assigned_identifier_is_23_characters_from_the_required_set() {
        // At most eight values of 128 bits give one identifier: 2^128 / 62^21 is under 8.
        assert_eq!(u128::MAX / ASSIGNED_SPACE, 7);
        for random in [0, 1, 0xdead_beef, u128::MAX / 3, u128::MAX] {
            let id = ClientId::assigned(random);
            assert_eq!(id.as_str().len(), ClientId::ASSIGNED_LEN);
            assert!(id.as_str().starts_with("oq"));
            assert!(id.as_str().bytes().all(|b| ALPHABET.contains(&b)));
            assert_eq!(ClientId::new(id.as_str()).unwrap(), id);
        }
    }

    #[test]
    fn a_user_name_may_be_empty_and_hold_separators() {
        assert_eq!(Username::new("").unwrap().as_str(), "");
        let cn = Username::new("acme/production/pump-3").unwrap();
        assert_eq!(cn.to_string(), "acme/production/pump-3");
        assert_eq!(format!("{cn:?}"), "Username(\"acme/production/pump-3\")");
        assert_eq!(
            Username::new("a\0"),
            Err(Error::NullCharacter { field: "User Name" })
        );
        Username::new(&"u".repeat(65_535)).unwrap();
        assert_eq!(
            Username::new(&"u".repeat(65_536)),
            Err(Error::UsernameTooLong { len: 65_536 })
        );
    }
}
