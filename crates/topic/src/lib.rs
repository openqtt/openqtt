//! MQTT topic names and topic filters.
//!
//! Validation of names and filters, the `$share/<group>/` form of shared subscriptions, the
//! rule that wildcards do not match topics beginning with `$`, mounting a client's mountpoint
//! onto its topics and stripping it on delivery, and `TopicIndex<V>`, a generic trie that finds
//! every filter matching a topic.
//!
//! Topics are strings here, not packets, so this crate must not depend on `openqtt-codec`.
