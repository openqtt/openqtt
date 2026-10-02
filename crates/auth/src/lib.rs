//! Built-in authentication and authorization, as implementations of the `openqtt-ext` traits.
//!
//! - [`PasswordAuthenticator`] over a [`PasswordList`] of PBKDF2 hashes, loaded from a bootstrap
//!   file in the `plain` or `hashed` format (report R2, rule 5).
//! - [`CertificateIdentity`]: the client named by its certificate's CN, which must carry the
//!   clientAuth extended key usage and may have to come from a pinned CA (R2 rules 2 to 4).
//! - [`Anonymous`], for a listener that does not authenticate.
//! - The ACL engine ([`acl`]): ordered rules in a TOML file, the first match deciding and no
//!   match denying, compiled once and bound to each client so that a decision per message
//!   costs bit tests and topic comparisons (R2 rules 9 to 16, `docs/spec/acl.md`).
//! - A [`ReservedPrefix`] that only service credentials carry, which every authenticator here
//!   keeps from anyone else (R2 rule 15).
//!
//! It makes no network calls, so it must not depend on hyper or reqwest (`make layers`).
//! Authentication over HTTP belongs to the edge.
//!
//! Statement numbers such as `[MQTT-3.1.4-2]` refer to the OASIS MQTT Version 5.0 standard of
//! 7 March 2019.

pub mod acl;
mod anonymous;
mod certificate;
mod error;
pub mod password;
mod pool;
mod prefix;

pub use acl::{Acl, AclAuthorizer, ClientRules};
pub use anonymous::Anonymous;
pub use certificate::{CertificateIdentity, IssuerPin, MAX_COMMON_NAME, certificates_from_pem};
pub use error::Error;
pub use password::{BootstrapFormat, PasswordAuthenticator, PasswordHash, PasswordList};
pub use prefix::ReservedPrefix;
