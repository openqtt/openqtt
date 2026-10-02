//! The extension seam: traits a build of the broker implements, compiled in.
//!
//! - [`Authenticator`] decides who a connecting client is, over a password, a certificate or
//!   the steps of enhanced authentication (section 4.12).
//! - [`Authorizer`] decides what a client may publish, subscribe to and receive, from rules it
//!   holds in memory.
//! - [`SessionEvents`] hears about connections, disconnections, subscriptions and takeovers.
//! - [`InterestSource`] and [`Forwarder`] let a component outside the cluster's own roles add
//!   subscription interest and take delivery of the messages that match it.
//! - [`LogConsumer`] reads committed messages from the log, in order, from a cursor the log
//!   keeps for it.
//! - [`RedirectPolicy`] chooses the Server Reference sent with 0x9C and 0x9D.
//!
//! Nothing is loaded at runtime. Other builds compile against this crate, so it is public API:
//! its types are `#[non_exhaustive]`, every trait method that may be added later comes with a
//! default, it exports [`API_VERSION`], and cargo-semver-checks guards every change (see
//! `README.md` beside this crate's manifest). It holds traits and the plain types they speak in,
//! and must not depend on any implementation crate (`make layers`).
//!
//! # Futures and trait objects
//!
//! The asynchronous methods return a [`BoxFuture`], a boxed future, rather than being written
//! as `async fn`. A trait with an `async fn` cannot be used as a trait object, and the node
//! holds its extensions as `Arc<dyn Authenticator>` and the like, chosen when it is assembled.
//! A macro crate such as async-trait would write the same boxes, and bring a second major
//! version of syn into the build for it. The box costs one allocation per call, and the calls
//! are per connection (authentication), per batch (the log) or per message to a destination
//! outside the cluster, where the network costs far more. The synchronous methods, those of
//! [`Authorizer`], [`SessionEvents`] and [`RedirectPolicy`], run on the hot path and box
//! nothing.
//!
//! Statement numbers such as `[MQTT-4.12.0-1]` refer to the OASIS MQTT Version 5.0 standard of
//! 7 March 2019.

mod authn;
mod authz;
mod client;
mod error;
mod events;
mod forward;
mod log;
mod redirect;

use std::future::Future;
use std::pin::Pin;

pub use authn::{
    AuthExchange, Authenticator, Certificate, Challenge, ConnectInfo, Grant, Refusal, Secret,
    Verdict,
};
pub use authz::{Action, Authorizer, ClientAuthorizer, Permission};
pub use client::{Attributes, ClientInfo, Principal};
pub use error::Error;
pub use events::{
    Connected, DisconnectReason, Disconnected, SessionEvents, Subscribed, TakenOver, Unsubscribed,
};
pub use forward::{Ack, ExternalId, Forwarder, Interest, InterestRegistry, InterestSource};
pub use log::{Entry, LogConsumer};
pub use redirect::{Redirect, RedirectCause, RedirectKind, RedirectPolicy, ServerReference};

/// A boxed future that can be sent between threads: what the asynchronous methods return, so
/// that every trait here can be used as a trait object.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The version of this seam, which a build checks at compile time:
///
/// ```
/// use openqtt_ext::{API_VERSION, ApiVersion};
///
/// const _: () = assert!(API_VERSION.satisfies(ApiVersion::new(1, 0)));
/// ```
pub const API_VERSION: ApiVersion = ApiVersion::new(1, 0);

/// A version of the seam. The major number changes when an implementation written against
/// the previous one may no longer compile or may behave wrongly; the minor number when
/// something is added, a trait method with a default, a type, a variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub struct ApiVersion {
    major: u16,
    minor: u16,
}

impl ApiVersion {
    /// The version `major.minor`.
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// The major number.
    pub const fn major(self) -> u16 {
        self.major
    }

    /// The minor number.
    pub const fn minor(self) -> u16 {
        self.minor
    }

    /// Whether an implementation written against `required` works with this version: the same
    /// major number, and a minor number at least as high.
    pub const fn satisfies(self, required: Self) -> bool {
        self.major == required.major && self.minor >= required.minor
    }
}

impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_version_satisfies_its_own_major_at_a_lower_or_equal_minor() {
        let current = ApiVersion::new(1, 3);
        assert!(current.satisfies(ApiVersion::new(1, 0)));
        assert!(current.satisfies(ApiVersion::new(1, 3)));
        assert!(!current.satisfies(ApiVersion::new(1, 4)));
        assert!(!current.satisfies(ApiVersion::new(2, 0)));
        assert!(!current.satisfies(ApiVersion::new(0, 3)));
        assert_eq!(current.to_string(), "1.3");
        assert!(API_VERSION.satisfies(ApiVersion::new(1, 0)));
    }
}
