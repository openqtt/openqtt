//! The name prefix reserved for service credentials (report R2, rule 15).

use std::fmt;

use crate::Error;

/// A user name prefix that only service credentials may carry, so that rules for services can
/// match it and no client can claim it by naming itself (R2 rule 15).
///
/// Every authenticator here keeps it for services: the password list gives it only to entries
/// that are service credentials, a certificate whose CN begins with it is refused, and so is a
/// client that names itself with it on a listener that does not authenticate. Names compare
/// byte for byte, so `svc:` reserves `svc:platform` and not `SVC:platform`.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ReservedPrefix(Box<str>);

impl ReservedPrefix {
    /// The prefix `prefix`.
    ///
    /// # Errors
    ///
    /// [`Error::ReservedPrefix`] when it is empty, which would reserve every name, or holds
    /// U+0000, which no user name can.
    pub fn new(prefix: &str) -> Result<Self, Error> {
        if prefix.is_empty() {
            return Err(Error::ReservedPrefix {
                reason: "it is empty, and would reserve every name",
            });
        }
        if prefix.contains('\0') {
            return Err(Error::ReservedPrefix {
                reason: "it contains U+0000",
            });
        }
        Ok(Self(prefix.into()))
    }

    /// The prefix.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether `name` begins with the prefix, and so belongs to a service.
    pub fn reserves(&self, name: &str) -> bool {
        name.starts_with(&*self.0)
    }
}

impl fmt::Debug for ReservedPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReservedPrefix").field(&&*self.0).finish()
    }
}

impl fmt::Display for ReservedPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn r2_rule_15_a_prefix_reserves_names_byte_for_byte() {
        let prefix = ReservedPrefix::new("svc:").unwrap();
        assert!(prefix.reserves("svc:platform"));
        assert!(prefix.reserves("svc:"));
        assert!(!prefix.reserves("SVC:platform"));
        assert!(!prefix.reserves("svc"));
        assert!(!prefix.reserves("device-svc:x"));
        assert_eq!(prefix.to_string(), "svc:");
        assert!(ReservedPrefix::new("").is_err());
        assert!(ReservedPrefix::new("a\0").is_err());
    }
}
