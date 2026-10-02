//! The seam's one error type.

/// Why an extension could not do what it was asked, or a value a type of this crate refuses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The extension cannot do it now, and the caller may try again: a destination that is
    /// down, a store that is busy.
    #[error("unavailable: {0}")]
    Unavailable(String),
    /// The extension refuses it, and trying again would be refused too.
    #[error("refused: {0}")]
    Refused(String),
    /// A value one of this crate's types cannot hold.
    #[error("not a valid {what}: {reason}")]
    Invalid {
        /// The kind of value.
        what: &'static str,
        /// Why it is refused.
        reason: &'static str,
    },
}
