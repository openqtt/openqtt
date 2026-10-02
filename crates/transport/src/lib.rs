//! The client listener: MQTT 5 over QUIC.
//!
//! An endpoint per core, mutual TLS with a verifier pinned to the issuing CA, connection IDs that
//! encode the node, a gate that holds 0-RTT data until the handshake confirms it, and the mapping
//! of MQTT onto QUIC streams.
//!
//! It yields `MqttConnection`, the transport seam of
//! docs/adr/0002-quic-only-tcp-seam-reserved.md: ordered bytes each way, the peer's certificate
//! chain and address, and a way to close with a reason. Sessions never see a QUIC type. It must
//! not depend on `openqtt-session`, `openqtt-wire` or `openqtt-auth`.
