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
//!
//! # Decoding
//!
//! A [`Decoder`] takes packets from the front of the [`BytesMut`](bytes::BytesMut) a
//! connection reads into. It returns `Ok(None)`, consuming nothing, until the buffer holds a
//! whole packet, then the [`Packet`], consuming exactly its bytes. A payload, and any other
//! Binary Data, is a slice of the buffer it arrived in rather than a copy. A decoder given a
//! Maximum Packet Size refuses a larger packet as soon as its fixed header arrives, and one
//! told the sender holds each packet to what that end may send ([`Packet::check_sender`]).
//!
//! # Encoding
//!
//! [`Packet`] and every packet struct have `encoded_len`, `encode` and `encode_within`.
//! Encoding checks a packet against every rule its receiver would refuse it for before it
//! writes a byte, so a packet that encodes decodes to itself, and one that does not leaves
//! the buffer as it was. `encoded_len` is exact, for holding a packet to the peer's Maximum
//! Packet Size before writing it ([MQTT-3.1.2-24]); `encode_within` does that check itself.
//!
//! # Errors and reason codes
//!
//! Every way a packet can be refused is an [`Error`] variant, which names its statement and
//! maps to the reason code to send before closing the connection:
//! [`Error::connack_reason_code`] while the CONNECT is the packet at fault, and
//! [`Error::disconnect_reason_code`] after it. The specification calls most violations a
//! Malformed Packet (0x81) or a Protocol Error (0x82). Where it is silent, the codec reads a
//! flags byte that cannot describe a packet as malformed, and a value that parses but is not
//! allowed as a Protocol Error, by the definitions of section 1.2.
//!
//! # What is left to the session
//!
//! The codec decides everything a packet's own bytes decide. It leaves alone what needs more:
//!
//! - **Topic syntax.** Topic Names, Topic Filters and Response Topics are checked as UTF-8
//!   Encoded Strings only. Wildcards in a Topic Name ([MQTT-3.3.2-2]) or Response Topic
//!   ([MQTT-3.3.2-14]), filter syntax (section 4.7), shared subscriptions (section 4.8) and
//!   No Local on one ([MQTT-3.8.3-4]) are openqtt-topic's, so the session can refuse a single
//!   message or subscription with its reason code instead of a DISCONNECT. The codec does
//!   refuse an empty Topic Name without a Topic Alias, which needs no topic grammar.
//! - **Connection state.** That CONNECT comes first and once ([MQTT-3.1.0-1],
//!   [MQTT-3.1.0-2]), Topic Aliases against the Topic Alias Maximum, Receive Maximum, Maximum
//!   QoS, Retain Available, the Authentication Method matching the CONNECT's
//!   ([MQTT-4.12.0-5]), and Request Problem Information ([MQTT-3.1.2-29]).
//! - **Policy.** Which Client Identifiers to accept ([MQTT-3.1.3-5]), whether a payload
//!   matches its Payload Format Indicator, and which Disallowed Unicode code points to refuse.
//!
//! # Unicode
//!
//! A UTF-8 Encoded String must be well-formed UTF-8 with no surrogates ([MQTT-1.5.4-1]) and no
//! U+0000 ([MQTT-1.5.4-2]), and a byte order mark in one is kept ([MQTT-1.5.4-3]). The
//! Disallowed Unicode code points of section 1.5.4, the C0 and C1 controls and the Unicode
//! noncharacters, are accepted: the specification makes refusing them optional, and refusing
//! them here would refuse a tab in a User Property value. [`disallowed_code_point`] finds
//! them for a caller that refuses them where it chooses, such as in Topic Names (section
//! 5.4.9).
//!
//! # Older protocols
//!
//! A CONNECT naming another protocol or version decodes to
//! [`Error::UnsupportedProtocol`], carrying the Protocol Name and Version.
//! [`ProtocolRefusal`] holds the answers: the MQTT 5.0 CONNACK with 0x84, the MQTT 3.1.1
//! CONNACK with return code 0x01, or closing without a CONNACK. Until report R1 settles the
//! bytes an older client receives, the session sends the MQTT 5.0 CONNACK, as ADR 0001
//! decides.
//!
//! # Where the specification is ambiguous
//!
//! - Table 2-6 lists reason code 0x8C (Bad authentication method) for DISCONNECT, and Table
//!   3-10 does not. DISCONNECT accepts it, from either end.
//! - Nothing says how to treat DUP set on a QoS 0 PUBLISH, which [MQTT-3.3.1-2] forbids, or
//!   Will QoS and Will Retain set without the Will Flag ([MQTT-3.1.2-11], [MQTT-3.1.2-13]).
//!   Both are flags no packet can have, so both are malformed.
//! - Section 3.3.2.3.2 defines a Payload Format Indicator of 0 and 1 and says nothing of other
//!   values. They are a Protocol Error, as for every other Byte property with only those two.
//! - A Packet Identifier of 0, a reason code outside its packet's table, and a SUBACK or
//!   UNSUBACK with no reason code are Protocol Errors: each parses, and none is allowed.
//! - AUTH may leave off its Reason Code and Property Length together (section 3.15.2.1) but,
//!   unlike DISCONNECT (section 3.14.2.2.1), not the Property Length alone. A Remaining
//!   Length of 1 is malformed. Every AUTH but that bare Success needs its Authentication
//!   Method (section 3.15.2.2.2).
//! - Figure 3-24 labels its Property Length 5 and draws the bits of 7. The five bytes that
//!   follow it settle on 5.

mod ack;
mod auth;
mod connack;
mod connect;
mod decoder;
mod disconnect;
mod encode;
mod error;
mod packet;
mod primitives;
mod property;
mod publish;
mod reason;
mod subscribe;
#[cfg(test)]
mod test_util;
mod types;

pub use ack::{AckProperties, PubAck, PubComp, PubRec, PubRel};
pub use auth::{Auth, AuthProperties};
pub use connack::{ConnAck, ConnAckProperties};
pub use connect::{
    Connect, ConnectProperties, PROTOCOL_NAME, PROTOCOL_VERSION, ProtocolRefusal, Will,
    WillProperties,
};
pub use decoder::Decoder;
pub use disconnect::{Disconnect, DisconnectProperties};
pub use error::Error;
pub use packet::Packet;
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
