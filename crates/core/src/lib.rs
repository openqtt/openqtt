//! Domain types shared by every role, and session placement.
//!
//! `partition_of(client_id)` places a client's session on a partition: xxh3 with a fixed seed,
//! pinned by a golden test, because changing it would move every session in a running cluster.
//!
//! Everything here is plain data and pure functions, so this crate must not depend on an IO
//! crate.
