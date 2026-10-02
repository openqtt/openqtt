//! Cluster membership and placement.
//!
//! Seeds from headless DNS; membership leases, epochs and partition placement kept in a meta Raft
//! group on the first three log pods; rendezvous hashing; and self-fencing, so a node that loses
//! its lease stops acting as an owner before another node starts.
//!
//! It must not depend on `openqtt-codec`, `openqtt-session` or `openqtt-transport`.
