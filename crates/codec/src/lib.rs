//! MQTT 5.0 packets, properties and reason codes, encoded and decoded over `bytes`.
//!
//! The codec speaks MQTT 5.0 and nothing older (docs/adr/0001-mqtt5-only.md): a CONNECT at any
//! other protocol level is refused with CONNACK reason code 0x84, unsupported protocol version.
//!
//! It is pure data in and data out. It performs no IO and knows no runtime, transport, TLS
//! stack or serialization framework, so it must not depend on tokio, quinn, rustls or serde.
//! Malformed input is an error value, never a panic: every byte it reads came from the network.
//!
//! Statement numbers such as `[MQTT-1.5.4-2]` refer to the OASIS MQTT Version 5.0 standard of
//! 7 March 2019, which the codec implements and its tests cite.

mod error;
mod primitives;

pub use error::Error;
pub use primitives::{
    MAX_STRING_LEN, MAX_VARIABLE_BYTE_INTEGER, disallowed_code_point, is_disallowed_code_point,
};
