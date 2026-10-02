//! Domain types shared by every role, and the placement of a session on a partition.
//!
//! The names a session is known by ([`ClientId`], [`Username`]), the message that travels
//! between roles ([`Message`]) and the rule that expires it ([`Deadline`]), the options of a
//! subscription ([`SubOpts`]), a pod's identity in the cluster ([`NodeId`], [`Epoch`]), and
//! [`partition_of`], which places a client's session on a log partition: xxh3 with a fixed
//! seed, pinned by a golden test, because changing it would move every session in a running
//! cluster.
//!
//! Everything here is plain data and pure functions, so this crate must not depend on an IO
//! crate. Time comes in as a [`Timestamp`] the caller read, and randomness as bits the caller
//! drew.
//!
//! Statement numbers such as `[MQTT-3.1.3-5]` refer to the OASIS MQTT Version 5.0 standard of
//! 7 March 2019.
//!
//! # Topics
//!
//! [`TopicName`] and [`TopicFilter`] are `openqtt-topic`'s, re-exported here so that a role
//! finds every domain type in one place. Core depends on the topic crate rather than the other
//! way round: the topic crate needs nothing from here, and stays a leaf that the codec and
//! everything above can use.
//!
//! # Types the codec has too
//!
//! [`QoS`], [`PayloadFormat`], [`RetainHandling`] and [`SubscriptionId`] have twins in
//! `openqtt-codec`. The router and the log depend on this crate and must not depend on the
//! codec (`make layers`), so the session, which sees both, converts between them.

mod client;
mod error;
mod message;
mod node;
mod partition;
mod subscription;

pub use client::{ClientId, Username};
pub use error::Error;
pub use message::{Deadline, Message, PayloadFormat, QoS, Timestamp};
pub use node::{Epoch, NodeId};
pub use openqtt_topic::{TopicFilter, TopicName};
pub use partition::partition_of;
pub use subscription::{RetainHandling, SubOpts, SubscriptionId};
