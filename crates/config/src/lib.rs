//! Configuration, and the only crate that reads the process environment.
//!
//! Every other crate is refused `std::env::var` and its siblings by clippy's
//! `disallowed-methods` (see `clippy.toml`). A setting is read once, here, where the process
//! starts, and passed down; a variable read in the middle of the broker would be a switch that
//! nothing documents.
//!
//! Configuration will come from a TOML file plus `OPENQTT_SECTION__KEY` variables, and an
//! unknown key or variable will be an error rather than something silently ignored. That arrives
//! with the first role. Today this crate reads one variable, [`LOG_VAR`].

use std::ffi::OsString;

/// The variable holding the log filter, in `tracing_subscriber::EnvFilter` directive syntax,
/// for example `info,openqtt_edge=debug`.
pub const LOG_VAR: &str = "OPENQTT_LOG";

/// Why configuration could not be read.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A variable is set to bytes that are not valid Unicode.
    #[error("{name} is not valid Unicode")]
    NotUnicode {
        /// The variable.
        name: &'static str,
    },
}

/// The log filter from [`LOG_VAR`], or `None` when the variable is unset or blank and the
/// caller's default applies.
///
/// # Errors
///
/// [`Error::NotUnicode`] when the variable holds bytes that are not Unicode. That is refused
/// rather than read as unset, so a broken filter is not mistaken for the default one.
pub fn log_filter() -> Result<Option<String>, Error> {
    #[expect(
        clippy::disallowed_methods,
        reason = "openqtt-config is the one crate that reads the environment"
    )]
    let value = std::env::var_os(LOG_VAR);
    log_filter_from(value)
}

/// [`log_filter`] over a value already read, so tests need not change the environment.
fn log_filter_from(value: Option<OsString>) -> Result<Option<String>, Error> {
    let Some(raw) = value else {
        return Ok(None);
    };
    let text = raw
        .into_string()
        .map_err(|_| Error::NotUnicode { name: LOG_VAR })?;
    let text = text.trim();
    Ok((!text.is_empty()).then(|| text.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_or_blank_means_the_default() {
        assert_eq!(log_filter_from(None).unwrap(), None);
        assert_eq!(log_filter_from(Some("".into())).unwrap(), None);
        assert_eq!(log_filter_from(Some("  \t".into())).unwrap(), None);
    }

    #[test]
    fn a_filter_is_passed_through_trimmed() {
        let filter = log_filter_from(Some(" info,openqtt_edge=debug\n".into())).unwrap();
        assert_eq!(filter.as_deref(), Some("info,openqtt_edge=debug"));
    }

    #[cfg(unix)]
    #[test]
    fn bytes_that_are_not_unicode_are_refused() {
        use std::os::unix::ffi::OsStringExt;

        let raw = OsString::from_vec(vec![b'i', b'n', 0x80]);
        let error = log_filter_from(Some(raw)).unwrap_err();
        assert!(matches!(error, Error::NotUnicode { name: LOG_VAR }));
        assert_eq!(error.to_string(), "OPENQTT_LOG is not valid Unicode");
    }
}
