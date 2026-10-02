//! The MQTT 5 connection and session state machine, without IO.
//!
//! It takes an `Input` (a decoded packet, a timer, a delivery) together with the current time,
//! and returns the `Effect`s its caller performs: packets to send, messages to route, state to
//! persist. Its state is plain data, so handing a session to another node is serialization.
//!
//! It never touches a socket, a clock or a runtime, so it must not depend on tokio, quinn or any
//! other IO crate. The transport seam of docs/adr/0002-quic-only-tcp-seam-reserved.md keeps it
//! that way.
