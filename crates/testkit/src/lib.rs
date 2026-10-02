//! Test tooling for OpenQTT.
//!
//! - [`RawConnection`]: an MQTT connection over QUIC that sends whatever it is given,
//!   malformed bytes included, and records every packet each way with its time, and how the
//!   connection closed: what statements that end in "MUST close the connection" need.
//! - [`TestPki`]: throwaway certificate authorities and certificates, with the negative cases
//!   a server must refuse.
//! - [`FakeServer`] and [`FakeBroker`]: a QUIC listener whose connections a test plays packet
//!   by packet, and a tiny broker on it, so a client is tested without a real broker.
//! - [`Scenario`] and [`Runner`]: a sequence of client actions and expectations, played
//!   against any broker address, with a normalized JSON trace per client.
//! - [`Oracle`] and the [`differential`] helpers: OpenQTT 1.x in Docker, to play the same
//!   scenarios against, and the comparison of traces. The harness itself is the
//!   `differential` test of this crate (`tests/differential/README.md`).
//!
//! Still to come: an in-process cluster and deterministic network simulation.
//!
//! It is only ever a dev-dependency: `make layers` refuses it as a normal dependency of any
//! crate. It does not depend on `openqtt-client`, so the client's own tests can use it.

pub mod differential;
mod error;
mod oracle;
pub mod packets;
mod pki;
mod raw;
mod scenario;
mod server;
mod tls;
pub mod trace;

pub use error::Error;
pub use oracle::{ORACLE_IMAGE, Oracle};
pub use pki::{Identity, TestPki};
pub use raw::{Close, RawConnection, Record, Recorded, Target};
pub use scenario::{CLOSE_WAIT, Check, DEFAULT_WAIT, Outcome, Runner, Scenario, Step, substitute};
pub use server::{FakeBroker, FakeServer};
pub use tls::{ALPN, ClientAuth};

/// The packet types scenarios are written in, from `openqtt-codec`.
pub use openqtt_codec as codec;
