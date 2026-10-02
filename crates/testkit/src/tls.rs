//! rustls configurations for the raw client and the fake server: aws-lc-rs, TLS 1.3, ALPN
//! `mqtt` unless a test asks for another.

use std::sync::Arc;

use rustls::crypto::CryptoProvider;
use rustls::pki_types::CertificateDer;
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

use crate::{Error, Identity};

/// The ALPN of MQTT over QUIC (docs/spec/mqtt-over-quic.md, section 1).
pub const ALPN: &[u8] = b"mqtt";

/// aws-lc-rs, the one crypto provider OpenQTT allows.
pub(crate) fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

fn root_store(roots: &[CertificateDer<'static>]) -> Result<RootCertStore, Error> {
    let mut store = RootCertStore::empty();
    for root in roots {
        store.add(root.clone())?;
    }
    Ok(store)
}

/// A client configuration trusting `roots`, presenting `identity` when given, and offering
/// `alpn`.
pub(crate) fn client_config(
    roots: &[CertificateDer<'static>],
    identity: Option<&Identity>,
    alpn: &[Vec<u8>],
) -> Result<Arc<ClientConfig>, Error> {
    let builder = ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(root_store(roots)?);
    let mut config = match identity {
        Some(identity) => builder.with_client_auth_cert(identity.chain.clone(), identity.key())?,
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = alpn.to_vec();
    Ok(Arc::new(config))
}

/// Whether the fake server asks for client certificates.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum ClientAuth {
    /// No client certificate is asked for.
    #[default]
    None,
    /// A client certificate is required and must chain to one of these roots. The check is
    /// rustls's, which takes a certificate without an extended key usage as valid for any use.
    Required(Vec<CertificateDer<'static>>),
}

/// A server configuration presenting `identity` with ALPN `mqtt`.
pub(crate) fn server_config(
    identity: &Identity,
    client_auth: &ClientAuth,
) -> Result<Arc<ServerConfig>, Error> {
    let builder = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?;
    let builder = match client_auth {
        ClientAuth::None => builder.with_no_client_auth(),
        ClientAuth::Required(roots) => {
            let verifier = WebPkiClientVerifier::builder_with_provider(
                Arc::new(root_store(roots)?),
                provider(),
            )
            .build()
            .map_err(|error| Error::Tls(rustls::Error::General(error.to_string())))?;
            builder.with_client_cert_verifier(verifier)
        }
    };
    let mut config = builder.with_single_cert(identity.chain.clone(), identity.key())?;
    config.alpn_protocols = vec![ALPN.to_vec()];
    Ok(Arc::new(config))
}
