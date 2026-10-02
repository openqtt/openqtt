//! The admin role: the REST API.
//!
//! axum with an OpenAPI description from utoipa: clients and kicks, users, bans, publishing and
//! importing retained messages. API keys come from a bootstrap file. There are no admin users and
//! no login sessions, because there is no UI.
//!
//! It must not depend on `openqtt-codec`, `openqtt-session` or `openqtt-transport`.
