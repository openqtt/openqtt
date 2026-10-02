//! Observability conventions shared by every role.
//!
//! - [`metrics`]: the instruments a role records on, typed so that no name or attribute can be
//!   misspelt, with names drafted for report R9.
//! - [`span`]: the names of spans and of the fields they carry.
//! - [`health`]: the readiness registry behind `/readyz`, and the answer of `/healthz`.
//!
//! It defines what is measured, not where it goes. Installing an exporter or a subscriber is the
//! binary's job, so this crate depends on the OpenTelemetry API alone, never on its SDK or on
//! tracing-subscriber (`make layers` holds it to that). Until the binary installs a meter
//! provider, every instrument records nothing, at almost no cost.

pub mod health;
pub mod metrics;
pub mod span;

/// Why the health registry refused a check.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A check is already registered under this name.
    #[error("a readiness check named `{name}` is already registered")]
    DuplicateCheck {
        /// The name.
        name: String,
    },
    /// The name cannot name a check.
    #[error(
        "`{name}` cannot name a readiness check: use 1 to 64 lowercase letters, digits, `_`, `-` \
         and `.`"
    )]
    InvalidCheckName {
        /// The name.
        name: String,
    },
}
