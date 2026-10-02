//! The edge role: client connections.
//!
//! A task per connection driving `openqtt-session`, the local subscription index, coarse
//! interest sent to routers, fan-out, takeover, drain, limiters and HTTP authentication.
//! Authorization runs here, on every message, without leaving the pod.
//!
//! It holds no durable state, so it must not depend on openraft, a storage engine or axum.
