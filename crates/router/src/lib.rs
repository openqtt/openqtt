//! The router role: subscription interest.
//!
//! Interest shards (an arena trie with roaring bitmaps of destinations) and the streams of route
//! views that edges follow. Interest is registered per edge, coarsened, never per client: no
//! cluster-wide structure holds an entry per device.
//!
//! It must not depend on `openqtt-codec`, `openqtt-session` or a storage engine.
