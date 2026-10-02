//! An MQTT 5 over QUIC client, for devices, the test kit and benchmarks.
//!
//! It is built on the codec, not on the broker's session machine, so it must not depend on
//! `openqtt-session`.
