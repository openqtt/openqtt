//! The test kit's one error type.

use std::io;

/// Why a test tool could not do its job. A broker's answer, however wrong, is never one of
/// these: it is recorded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A file, socket or process failed.
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// rustls refused a configuration.
    #[error("TLS: {0}")]
    Tls(#[from] rustls::Error),
    /// A TLS configuration cannot run QUIC.
    #[error("TLS for QUIC: {0}")]
    TlsForQuic(String),
    /// rcgen could not make a certificate.
    #[error("certificate: {0}")]
    Certificate(#[from] rcgen::Error),
    /// A QUIC connection could not start.
    #[error("QUIC connect: {0}")]
    Connect(#[from] quinn::ConnectError),
    /// A QUIC connection failed, the handshake included.
    #[error("QUIC connection: {0}")]
    Connection(#[from] quinn::ConnectionError),
    /// Writing to a QUIC stream failed.
    #[error("QUIC write: {0}")]
    Write(#[from] quinn::WriteError),
    /// The QUIC endpoint closed.
    #[error("the endpoint is closed")]
    EndpointClosed,
    /// A packet does not encode; send its bytes instead to send it anyway.
    #[error("encoding: {0}")]
    Encode(#[from] openqtt_codec::Error),
    /// Docker could not run the oracle.
    #[error("docker: {0}")]
    Docker(String),
    /// A scenario cannot run as written.
    #[error("scenario: {0}")]
    Scenario(String),
    /// Something did not happen in time.
    #[error("timed out: {0}")]
    Timeout(String),
    /// A TOML file does not parse or does not have the expected shape.
    #[error("TOML: {0}")]
    Toml(String),
    /// A trace does not serialize.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
}
