//! The session crate's one error type.

/// Why a setting cannot be used.
///
/// The machine itself never fails: everything a client or the broker can send it is an input,
/// and a refusal is an effect. Only configuration can be wrong, and it is checked where it is
/// built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Keep Alive bounds whose lower end is 0, which would leave a client without liveness, or
    /// is above the upper end (report R1, O4).
    #[error("the Keep Alive bounds {min} to {max} seconds are not valid")]
    KeepAliveBounds {
        /// The lowest Keep Alive used as sent.
        min: u16,
        /// The highest Keep Alive used as sent.
        max: u16,
    },
}
