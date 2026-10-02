//! The log role: durable state.
//!
//! Raft-replicated partitions, 256 by default, behind a `KvEngine` storage trait. They hold who
//! owns each client id (the authority for takeover, and the will), sessions, queued and
//! in-flight messages, and retained messages. A PUBACK goes out only after every partition that
//! must hold the message has committed it.
//!
//! It must not depend on `openqtt-codec`, `openqtt-session`, `openqtt-transport` or axum.
