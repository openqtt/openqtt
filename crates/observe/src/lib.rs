//! Observability conventions shared by every role.
//!
//! Metric handles on the OpenTelemetry API, the names and fields of spans, and the health
//! registry behind the liveness and readiness probes.
//!
//! It defines what is measured, not where it goes: installing an exporter or a subscriber is the
//! binary's job, so this crate must not depend on the OpenTelemetry SDK or tracing-subscriber.
