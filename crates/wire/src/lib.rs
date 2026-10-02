//! The protocol between roles.
//!
//! QUIC with mutual TLS, where each role proves its identity with a SPIFFE-style SAN. Messages
//! are prost envelopes whose generated code is committed, a `Hello { min, max }` exchange agrees
//! the version, each release talks to the one before it, and a range of message types is
//! reserved for extensions.
//!
//! It carries what roles say to each other, not MQTT, so it must not depend on `openqtt-codec`,
//! `openqtt-session` or `openqtt-auth`.
