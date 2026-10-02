//! The MQTT 5 connection and session state machine, without IO.
//!
//! One [`Session`] serves one network connection. It takes an [`Input`] (a decoded packet, the
//! answer to one of its requests, a delivery from the broker, a timer, an order from the edge)
//! together with the time the caller read, and returns the [`Effects`] the caller performs:
//! packets to send, requests to authenticate, authorize and claim, messages to publish,
//! interest to register, timers to set, and at the end the will to settle, the claim to release
//! with the session's state, and the connection to close. Its state is plain data
//! ([`SessionState`]), so handing a session to another node is serialization.
//!
//! It never touches a socket, a clock or a runtime, so it must not depend on tokio, quinn or any
//! other IO crate. The transport seam of docs/adr/0002-quic-only-tcp-seam-reserved.md keeps it
//! that way, and the same machine will serve an MQTT over TLS/TCP listener. Randomness comes in
//! as bits the transport drew ([`Peer::random`]).
//!
//! Report R1 (docs/reports/R01-conformance.md) is normative for it: every statement whose layer
//! is `session` is the machine's, with the decisions D1 to D32 and the open choices O1 to O26
//! where they bear on a session. Statement numbers such as `[MQTT-3.1.2-22]` refer to the OASIS
//! MQTT Version 5.0 standard of 7 March 2019, and the tests that prove one carry its number.
//!
//! # Requests and their answers
//!
//! The machine waits on its caller in four places, each a request effect answered by an input:
//!
//! | Request | Answer | While it waits |
//! | --- | --- | --- |
//! | [`Effect::Authenticate`] | [`Input::Authenticated`] | at CONNECT and on a step of enhanced authentication, nothing else the client sent is processed |
//! | [`Effect::Authorize`] | [`Input::Authorized`] | nothing after the packet being authorized is processed, so order is kept |
//! | [`Effect::Claim`] | [`Input::Claimed`] | CONNACK waits (report R3, Sessions) |
//! | [`Effect::Publish`] with a token | [`Input::Committed`] | its PUBACK or PUBREC waits, and so do the acknowledgements after it on its stream (report R1, D26 and O15); everything else goes on |
//!
//! An [`Interest`] with a retained read is a fifth: live deliveries for that subscription wait
//! for the [`Input::Retained`] that answers it (report R1, O2).
//!
//! # What is left to others
//!
//! - The codec decodes, and refuses malformed packets; the transport hands the machine the
//!   error with [`Input::DecodeError`]. The decoder should be told the server's Maximum Packet
//!   Size and that the client is the sender.
//! - The transport closes the connection once a CONNACK or DISCONNECT is delivered or a linger
//!   ends ([MQTT-3.2.2-7], [MQTT-3.14.4-2]), and holds data streams opened before CONNACK.
//! - The log keeps sessions, retained messages and wills, and publishes a will whose delay
//!   runs out; the router and the edge match topics. A [`Delivery`] names the subscriptions
//!   the edge matched, and the machine checks each again (report R2, rule 6).
//! - The log decides whether a QoS 2 PUBLISH is new. Every QoS 2 [`Publication`] carries its
//!   receipt, the client's Packet Identifier, and the partition keeps it as `rel/{cid}/{pid}`
//!   (report R3):
//!   - from the commit that writes it with the message until [`Effect::ReleaseReceipt`], or
//!     the end of the session;
//!   - a publication whose receipt it already holds is answered as accepted and not routed
//!     again, and the receipt stays;
//!   - a publication answered with a failure, [`PublishOutcome::QuotaExceeded`] or
//!     [`PublishOutcome::Failed`], wrote nothing, its receipt included. A failure is definite:
//!     an outcome the edge cannot know is not one; it waits, or ends the connection.
//!
//!   The machine releases the receipt whenever the exchange for the identifier ends: on
//!   PUBREL, and with every PUBREC that refuses the message, before it is published or after
//!   its commit failed. For a fresh publication nothing is then held and the release changes
//!   nothing. For a repeat of a reserved identifier it clears the receipt the first, cut-off
//!   commit may have left: a connection that ends with a commit still out hands its
//!   identifier over reserved ([`SessionState::awaiting_commit`]), the client's repeat on the
//!   next connection is published again under the same receipt, and the message is delivered
//!   once whichever way the first commit went.

#![forbid(unsafe_code)]

mod config;
mod convert;
mod effect;
mod error;
mod input;
mod machine;
mod phrase;
mod redact;
mod state;

#[cfg(test)]
mod tests;

pub use config::{Config, Identity, KeepAliveBounds, Peer};
pub use effect::{
    Action, AuthStep, Authentication, Authorization, Claim, CloseCode, Counter, Effect, Effects,
    Interest, Publication, PublishToken, Release, RequestId, RetainedRead, SessionEnd, Timer,
    WillMessage, WillOrder,
};
pub use error::Error;
pub use input::{
    AuthResult, ClaimResult, Decision, Delivery, Input, PublishOutcome, Shutdown, StreamEnd,
    StreamId,
};
pub use machine::Session;
pub use state::{SessionState, StoredDelivery, StoredOutbound, StoredSubscription};
