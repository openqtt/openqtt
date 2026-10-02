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

mod ack;
mod connack;
mod connect;
mod encode;
mod error;
mod primitives;
mod property;
mod publish;
mod reason;
mod subscribe;
#[cfg(test)]
mod test_util;
mod types;

pub use ack::{AckProperties, PubAck, PubComp, PubRec, PubRel};
pub use connack::{ConnAck, ConnAckProperties};
pub use connect::{
    Connect, ConnectProperties, PROTOCOL_NAME, PROTOCOL_VERSION, ProtocolRefusal, Will,
    WillProperties,
};
pub use error::Error;
pub use primitives::{
    MAX_PACKET_SIZE, MAX_STRING_LEN, MAX_VARIABLE_BYTE_INTEGER, disallowed_code_point,
    is_disallowed_code_point,
};
pub use property::{DataType, PropertyContext, PropertyId};
pub use publish::{Publish, PublishProperties};
pub use reason::{
    AuthReasonCode, ConnectReasonCode, DisconnectReasonCode, PubAckReasonCode, PubCompReasonCode,
    PubRecReasonCode, PubRelReasonCode, ReasonCode, SubAckReasonCode, UnsubAckReasonCode,
};
pub use subscribe::{
    RetainHandling, SubAck, Subscribe, SubscribeProperties, Subscription, SubscriptionOptions,
    UnsubAck, Unsubscribe, UnsubscribeProperties,
};
pub use types::{PacketId, PacketType, PayloadFormat, QoS, Sender, SubscriptionId};
