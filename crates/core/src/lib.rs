//! Domain types shared by every role, and the placement of a session on a partition.
//!
//! The names a session is known by ([`ClientId`], [`Username`]), and [`partition_of`], which
//! places a client's session on a log partition: xxh3 with a fixed seed, pinned by a golden
//! test, because changing it would move every session in a running cluster.
//!
//! Everything here is plain data and pure functions, so this crate must not depend on an IO
//! crate. Randomness comes in as bits the caller drew.
//!
//! Statement numbers such as `[MQTT-3.1.3-5]` refer to the OASIS MQTT Version 5.0 standard of
//! 7 March 2019.

mod client;
mod error;
mod partition;

pub use client::{ClientId, Username};
pub use error::Error;
pub use partition::partition_of;
