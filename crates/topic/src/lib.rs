//! MQTT topic names and topic filters.
//!
//! Validation of names and filters (section 4.7 of MQTT 5.0), the `$share/{ShareName}/` form of
//! shared subscriptions (section 4.8), matching a name against a filter, including the rule that
//! a filter starting with a wildcard does not match a topic beginning with `$`, and mountpoints,
//! which give each client a namespace of its own (report R2, rule 6).
//!
//! Topics are strings here, not packets, so this crate must not depend on `openqtt-codec`. The
//! codec checks that a Topic Name or a Topic Filter is a UTF-8 Encoded String and no more; the
//! rest of its syntax is checked here, so that the session can refuse one message or one
//! subscription with its own reason code and keep the connection (report R1, O25).
//!
//! Statement numbers such as `[MQTT-4.7.1-1]` refer to the OASIS MQTT Version 5.0 standard of
//! 7 March 2019.
//!
//! # Where the topic types live
//!
//! [`TopicName`] and [`TopicFilter`] are defined here, and `openqtt-core` re-exports them with
//! its other domain types. Core depends on this crate rather than the other way round, so the
//! roles that never parse MQTT (router, log) reach the topic types through core without
//! reaching the codec, and this crate stays a leaf with nothing below it.

mod error;
mod filter;
mod mount;
mod name;

pub use error::Error;
pub use filter::TopicFilter;
pub use mount::{Mount, Mountpoint, Placeholder};
pub use name::{MAX_TOPIC_LEN, TopicName};
