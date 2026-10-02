//! The auth crate's one error type.

/// Why a credential, a list, a rule file or a conversion was refused.
///
/// No message repeats a password or a hash: a value that might be one is named by where it is,
/// a line or a rule, never quoted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A password hash is not a PHC string this crate reads.
    #[error("not a pbkdf2-sha256 PHC string: {reason}")]
    PasswordHash {
        /// What is wrong with it.
        reason: &'static str,
    },
    /// The system's random source failed, so no salt could be drawn.
    #[error("the system random source failed")]
    Random,
    /// A reserved prefix is empty, or holds a character no user name can.
    #[error("not a valid reserved prefix: {reason}")]
    ReservedPrefix {
        /// What is wrong with it.
        reason: &'static str,
    },
    /// A user name the list refuses.
    #[error("the user name `{name}` {reason}")]
    UserName {
        /// The name.
        name: String,
        /// What is wrong with it.
        reason: &'static str,
    },
    /// Not one thread of a pool could start.
    #[error("no thread could start: {reason}")]
    Threads {
        /// The system's reason.
        reason: String,
    },
    /// A password bootstrap format other than `plain` and `hashed`.
    #[error("unknown password bootstrap format `{name}`; it is `plain` or `hashed`")]
    BootstrapFormat {
        /// The name given.
        name: String,
    },
    /// A line of a password bootstrap file that cannot be read.
    #[error("line {line}: {reason}")]
    Bootstrap {
        /// The line, counted from 1.
        line: usize,
        /// What is wrong with it.
        reason: String,
    },
}
