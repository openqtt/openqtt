//! TLS for the client: rustls on aws-lc-rs, TLS 1.3, ALPN `mqtt`.

use std::fmt;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore};

use crate::Error;

/// The ALPN protocol MQTT over QUIC uses (docs/spec/mqtt-over-quic.md, section 1); a server
/// refuses a handshake without it.
pub const ALPN: &[u8] = b"mqtt";

/// The TLS side of a connection: the roots the server's certificate must chain to and, for
/// mutual TLS, the client's own certificate.
///
/// It always uses aws-lc-rs, the one crypto provider OpenQTT allows, and TLS 1.3, the only
/// version QUIC runs. Trust comes only from the roots given: there are no built-in or
/// platform roots.
///
/// ```
/// # fn roots() -> openqtt_client::CertificateDer<'static> { unimplemented!() }
/// # fn run() -> Result<(), openqtt_client::Error> {
/// use openqtt_client::TlsConfig;
///
/// let tls = TlsConfig::builder().root_certificate(roots()).build()?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct TlsConfig {
    config: Arc<ClientConfig>,
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConfig")
            .field("alpn", &self.config.alpn_protocols)
            .finish_non_exhaustive()
    }
}

impl TlsConfig {
    /// Starts a configuration.
    pub fn builder() -> TlsConfigBuilder {
        TlsConfigBuilder::default()
    }

    /// Uses a rustls configuration as it is, but for its ALPN list, which becomes `mqtt`. Its
    /// crypto provider is the caller's choice.
    pub fn from_rustls(mut config: ClientConfig) -> Self {
        config.alpn_protocols = vec![ALPN.to_vec()];
        Self {
            config: Arc::new(config),
        }
    }

    /// The rustls configuration handshakes use.
    pub fn rustls(&self) -> Arc<ClientConfig> {
        Arc::clone(&self.config)
    }
}

/// Builds a [`TlsConfig`].
#[derive(Default)]
pub struct TlsConfigBuilder {
    roots: Vec<CertificateDer<'static>>,
    identity: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
}

impl fmt::Debug for TlsConfigBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConfigBuilder")
            .field("roots", &self.roots.len())
            .field("client_certificate", &self.identity.is_some())
            .finish()
    }
}

impl TlsConfigBuilder {
    /// Trusts a root certificate, DER encoded, as an issuer of server certificates. At least
    /// one is needed.
    #[must_use]
    pub fn root_certificate(mut self, root: CertificateDer<'static>) -> Self {
        self.roots.push(root);
        self
    }

    /// Trusts several root certificates.
    #[must_use]
    pub fn root_certificates(
        mut self,
        roots: impl IntoIterator<Item = CertificateDer<'static>>,
    ) -> Self {
        self.roots.extend(roots);
        self
    }

    /// Presents a client certificate, for servers that require mutual TLS: the chain, leaf
    /// first, and the leaf's private key. A server that takes its identity from the
    /// certificate needs the clientAuth extended key usage on it
    /// (docs/spec/mqtt-over-quic.md, section 3).
    #[must_use]
    pub fn client_certificate(
        mut self,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Self {
        self.identity = Some((chain, key));
        self
    }

    /// The configuration.
    ///
    /// # Errors
    ///
    /// [`Error::Tls`] when no root was given, a root does not parse, or the client key does
    /// not match what rustls accepts.
    pub fn build(self) -> Result<TlsConfig, Error> {
        if self.roots.is_empty() {
            return Err(Error::Tls(rustls::Error::General(
                "no root certificate to trust".into(),
            )));
        }
        let mut roots = RootCertStore::empty();
        for root in self.roots {
            roots.add(root)?;
        }
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let builder = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(roots);
        let mut config = match self.identity {
            Some((chain, key)) => builder.with_client_auth_cert(chain, key)?,
            None => builder.with_no_client_auth(),
        };
        config.alpn_protocols = vec![ALPN.to_vec()];
        Ok(TlsConfig {
            config: Arc::new(config),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configuration_needs_a_root() {
        let error = TlsConfig::builder().build().unwrap_err();
        assert!(matches!(error, Error::Tls(_)), "{error}");
    }

    #[test]
    fn a_root_that_does_not_parse_is_refused() {
        let error = TlsConfig::builder()
            .root_certificate(CertificateDer::from(vec![0x30, 0x03, 0x01, 0x01, 0xFF]))
            .build()
            .unwrap_err();
        assert!(matches!(error, Error::Tls(_)), "{error}");
    }

    #[test]
    fn from_rustls_sets_the_alpn() {
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let mut config = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        config.alpn_protocols = vec![b"h3".to_vec()];
        let tls = TlsConfig::from_rustls(config);
        assert_eq!(tls.rustls().alpn_protocols, vec![b"mqtt".to_vec()]);
    }
}
