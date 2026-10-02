//! The extension seam: traits a build of the broker implements, compiled in.
//!
//! - `Authenticator` decides who a connecting client is.
//! - `Authorizer` decides what a client may publish and subscribe to.
//! - `SessionEvents` hears about connections, disconnections and the end of sessions.
//! - `InterestSource` and `Forwarder` let a component outside the cluster's own roles add
//!   subscription interest and take delivery of the messages that match it.
//! - `LogConsumer` reads committed messages from the log, in order, from a cursor it keeps.
//! - `RedirectPolicy` decides when a client is sent to another server.
//!
//! Nothing is loaded at runtime. Other builds compile against this crate, so it is public API:
//! its types are `#[non_exhaustive]`, it exports `API_VERSION`, and cargo-semver-checks guards
//! every change. It holds traits only and must not depend on any implementation crate.
