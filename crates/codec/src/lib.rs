//! MQTT 5.0 packets, properties and reason codes, encoded and decoded over `bytes`.
//!
//! The codec speaks MQTT 5.0 and nothing older (docs/adr/0001-mqtt5-only.md): a CONNECT at any
//! other protocol level is refused with CONNACK reason code 0x84, unsupported protocol version.
//!
//! It is pure data in and data out. It performs no IO and knows no runtime, transport, TLS
//! stack or serialization framework, so it must not depend on tokio, quinn, rustls or serde.
//! Malformed input is an error value, never a panic: every byte it reads came from the network.
