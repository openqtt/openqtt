//! Test tooling.
//!
//! A raw-packet client, an in-process cluster, deterministic network simulation, a scenario
//! language and a differential runner that plays the same script against two brokers.
//!
//! It is only ever a dev-dependency: `make layers` refuses it as a normal dependency of any crate.
