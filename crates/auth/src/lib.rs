//! Built-in authentication and authorization, as implementations of the `openqtt-ext` traits.
//!
//! - [`PasswordAuthenticator`] over a [`PasswordList`] of PBKDF2 hashes, loaded from a bootstrap
//!   file in the `plain` or `hashed` format (report R2, rule 5).
//! - [`Anonymous`], for a listener that does not authenticate.
//! - A [`ReservedPrefix`] that only service credentials carry, which every authenticator here
//!   keeps from anyone else (R2 rule 15).
//!
//! It makes no network calls, so it must not depend on hyper or reqwest (`make layers`).
//! Authentication over HTTP belongs to the edge.
//!
//! Statement numbers such as `[MQTT-3.1.4-2]` refer to the OASIS MQTT Version 5.0 standard of
//! 7 March 2019.

mod anonymous;
mod error;
pub mod password;
mod pool;
mod prefix;

pub use anonymous::Anonymous;
pub use error::Error;
pub use password::{BootstrapFormat, PasswordAuthenticator, PasswordHash, PasswordList};
pub use prefix::ReservedPrefix;
