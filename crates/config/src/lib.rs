//! Configuration, and the only crate that reads the process environment.
//!
//! Every other crate is refused `std::env::var` and its siblings by clippy's
//! `disallowed-methods` (see `clippy.toml`). Settings are read once, here, where the process
//! starts, and passed down; a variable read in the middle of the broker would be a switch that
//! nothing documents.
//!
//! # Where settings come from
//!
//! [`Settings::load`] reads three layers, each over the one before:
//!
//! 1. the built-in defaults, [`Settings::default`];
//! 2. a TOML file, named by `--config` or else by [`CONFIG_VAR`];
//! 3. `OPENQTT_SECTION__KEY` variables, with a double underscore for each level of nesting:
//!    `OPENQTT_LISTENERS__QUIC__DEFAULT__BIND` sets `listeners.quic.default.bind`.
//!
//! [`Sources`] gathers the variables once, when the process starts, and tests pass them in
//! instead, since a test cannot safely change its own environment.
//!
//! # Nothing is silently ignored
//!
//! A key in the file or an `OPENQTT_` variable that names no setting is an error, and the
//! message names the nearest valid one: a stale name never goes unnoticed. So is any `OTEL_`
//! variable, which the OpenTelemetry exporter would otherwise read behind the configuration's
//! back. [`Settings::validate`] then refuses settings that cannot work together, and a feature
//! that is not built yet cannot be switched on.
//!
//! # Secrets
//!
//! No setting holds a secret. One that needs a secret names a file that holds it, in a key
//! ending in `_file` ([`SecretFile`]), and the file is read only when needed, into a [`Secret`]
//! that prints as `<redacted>`. A URL that carries a password is refused, and no error repeats
//! the value of an unknown key or variable, which may be a secret set by mistake.
//!
//! docs/spec/config.md lists every setting. [`reference()`] writes its tables from the
//! declarations in `settings.rs`, and a test holds the document to them.

mod load;
mod render;
mod rules;
mod schema;
mod settings;
mod values;

use std::path::PathBuf;

pub use load::{CONFIG_VAR, Loaded, Sources};
pub use render::reference;
pub use settings::{
    Admin, Auth, Cluster, Edge, Http, HttpAuthn, JwtAuthn, Limits, Listeners, Log, LogFormat,
    Observability, Otlp, PasswordType, Prometheus, QuicListener, Role, Router, Settings, Storage,
};
pub use values::{ByteSize, Duration, Endpoint, HostPort, Secret, SecretFile};

/// Why the configuration cannot be used. One error can hold several problems, so that
/// `openqtt config check` reports every one of them at once.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The configuration file cannot be read.
    #[error("cannot read the configuration file {}: {source}", .path.display())]
    ReadFile {
        /// The file.
        path: PathBuf,
        /// Why it cannot be read.
        #[source]
        source: std::io::Error,
    },
    /// The configuration file is not valid TOML.
    #[error("{} is not valid TOML: {message}", .path.display())]
    Syntax {
        /// The file.
        path: PathBuf,
        /// The parser's account, with the line and column.
        message: String,
    },
    /// A variable holds bytes that are not valid Unicode.
    #[error("{name} is not valid Unicode")]
    NotUnicode {
        /// The variable.
        name: String,
    },
    /// The file holds a key that is not a setting.
    #[error("unknown key `{key}` in {}; the nearest valid key is `{nearest}`", .path.display())]
    UnknownKey {
        /// The key, dotted from the top of the file.
        key: String,
        /// The file.
        path: PathBuf,
        /// The valid key nearest to it.
        nearest: String,
    },
    /// An `OPENQTT_` variable names no setting.
    #[error("unknown variable {name}; the nearest valid one is {nearest}")]
    UnknownVariable {
        /// The variable.
        name: String,
        /// The valid variable nearest to it.
        nearest: String,
    },
    /// An `OTEL_` variable is set. OpenQTT configures its exporter itself and reads none, so
    /// one that is set would either be ignored or change the exporter behind the configuration.
    #[error(
        "{name} is set, but OpenQTT reads no OTEL_ variables: set {setting} or another \
         observability.otlp setting instead"
    )]
    OtelVariable {
        /// The variable.
        name: String,
        /// The variable of the setting that does its job.
        setting: String,
    },
    /// A value does not fit its setting.
    #[error("{place}: {reason}")]
    Value {
        /// The key and file, or the variable, the value came from.
        place: String,
        /// Why it does not fit.
        reason: String,
    },
    /// A setting that does not work with the others, or asks for what OpenQTT cannot do.
    #[error("{key}: {reason}")]
    Rule {
        /// The setting, dotted.
        key: String,
        /// Why it is refused.
        reason: String,
    },
    /// A file a setting names cannot be read.
    #[error("{key}: cannot read {}: {source}", .path.display())]
    File {
        /// The setting, dotted.
        key: String,
        /// The file.
        path: PathBuf,
        /// Why it cannot be read.
        #[source]
        source: std::io::Error,
    },
    /// Several of the above, in the order they were found, one per line.
    #[error("{}", lines(.0))]
    Several(Vec<Error>),
}

impl Error {
    /// The problems this error holds, one for each line of its message.
    pub fn problems(&self) -> Vec<&Error> {
        match self {
            Self::Several(all) => all.iter().flat_map(Self::problems).collect(),
            one => vec![one],
        }
    }

    /// `Ok` when there are no problems, else the one problem or all of them.
    pub(crate) fn check(problems: Vec<Error>) -> Result<(), Error> {
        if problems.is_empty() {
            Ok(())
        } else {
            Err(Self::from_problems(problems))
        }
    }

    /// The one problem, or all of them.
    pub(crate) fn from_problems(mut problems: Vec<Error>) -> Error {
        if problems.len() == 1 {
            problems.remove(0)
        } else {
            Self::Several(problems)
        }
    }
}

fn lines(errors: &[Error]) -> String {
    errors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}
