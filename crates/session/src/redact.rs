//! Debug formatting for what must never reach a log through `{:?}`: credentials, and the random
//! bits assigned identifiers are drawn from, which predict them.

use std::fmt;

use bytes::Bytes;

/// Formats an optional credential as its presence and length only, as the codec does.
pub(crate) struct Redacted<'a>(pub(crate) &'a Option<Bytes>);

impl fmt::Debug for Redacted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(value) => write!(f, "Some(<redacted, {} bytes>)", value.len()),
            None => f.write_str("None"),
        }
    }
}

/// An optional credential the machine keeps, which formats as [`Redacted`] does.
#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) struct Credential(pub(crate) Option<Bytes>);

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Redacted(&self.0).fmt(f)
    }
}

/// A value whose Debug output says only that it is there.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Secret<T>(pub(crate) T);

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}
