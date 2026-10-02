//! Logs: the events of every crate, filtered by `observability.log_level` and written to stderr
//! in the format `observability.log_format` names.

use openqtt_config::{LogFormat, Observability};
use tracing_subscriber::EnvFilter;

/// The filter `level` describes. Not `EnvFilter::from_default_env`, which would read `RUST_LOG`
/// behind the configuration's back.
pub(crate) fn filter(level: &str) -> Result<EnvFilter, String> {
    EnvFilter::builder()
        .parse(level)
        .map_err(|error| format!("observability.log_level: {error}"))
}

/// Installs the process's subscriber. A JSON line carries the event's fields at the top level
/// and the spans it was written in, with theirs (openqtt-observe's `span` names them), so a log
/// collector can search on a client identifier without parsing the message.
pub(crate) fn init(observability: &Observability) -> Result<(), String> {
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter(&observability.log_level)?)
        .with_writer(std::io::stderr);
    let installed = match observability.log_format {
        LogFormat::Json => builder
            .json()
            .flatten_event(true)
            .with_current_span(false)
            .with_span_list(true)
            .try_init(),
        LogFormat::Text => builder.try_init(),
    };
    installed.map_err(|error| format!("cannot start logging: {error}"))
}
