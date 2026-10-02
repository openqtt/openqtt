//! Throwaway certificate authorities and certificates, made with rcgen on aws-lc-rs.

use std::fmt;

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::Error;

/// A certificate authority that lives as long as a test: it issues server and client
/// certificates, including the ones a server must refuse.
///
/// ```
/// let pki = openqtt_testkit::TestPki::new("Test CA")?;
/// let server = pki.server(&["localhost", "127.0.0.1"])?;
/// let device = pki.client("device-17")?;
/// // Negative cases: no extended key usage, and a look-alike issuer.
/// let no_eku = pki.client_without_eku("device-18")?;
/// let impostor = pki.impostor_client("device-19")?;
/// # Ok::<(), openqtt_testkit::Error>(())
/// ```
pub struct TestPki {
    name: String,
    certificate: CertificateDer<'static>,
    certificate_pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl fmt::Debug for TestPki {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TestPki")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// A certificate chain and its private key.
#[derive(Clone)]
pub struct Identity {
    /// The chain, leaf first. A leaf issued by a [`TestPki`] is the whole chain.
    pub chain: Vec<CertificateDer<'static>>,
    /// The leaf, PEM encoded.
    pub certificate_pem: String,
    /// The private key, PKCS#8 PEM encoded.
    pub key_pem: String,
    key_der: Vec<u8>,
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity")
            .field("chain", &self.chain.len())
            .finish_non_exhaustive()
    }
}

impl Identity {
    /// The private key, as rustls takes it.
    pub fn key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key_der.clone()))
    }
}

/// The extended key usage a leaf certificate carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Usage {
    Server,
    Client,
    None,
}

impl TestPki {
    /// A new authority whose certificate names `name` as its common name.
    ///
    /// # Errors
    ///
    /// [`Error::Certificate`] when rcgen fails.
    pub fn new(name: &str) -> Result<Self, Error> {
        Self::with_key(name, KeyPair::generate()?)
    }

    fn with_key(name: &str, key: KeyPair) -> Result<Self, Error> {
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.distinguished_name = distinguished_name(name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let certificate = params.self_signed(&key)?;
        Ok(Self {
            name: name.to_owned(),
            certificate_pem: certificate.pem(),
            certificate: certificate.der().clone(),
            issuer: Issuer::new(params, key),
        })
    }

    /// The authority's certificate, DER encoded: the root a client or server trusts.
    pub fn ca_certificate(&self) -> CertificateDer<'static> {
        self.certificate.clone()
    }

    /// The authority's certificate, PEM encoded.
    pub fn ca_pem(&self) -> &str {
        &self.certificate_pem
    }

    /// A server certificate for `names`, each a DNS name or an IP address, with the serverAuth
    /// extended key usage. The first name is also the common name.
    ///
    /// # Errors
    ///
    /// [`Error::Certificate`] when a name is neither, or rcgen fails.
    pub fn server(&self, names: &[&str]) -> Result<Identity, Error> {
        let common_name = names.first().copied().unwrap_or("server");
        self.leaf(common_name, names, Usage::Server)
    }

    /// A client certificate with `common_name` as its subject CN and the clientAuth extended
    /// key usage: what a device presents (docs/spec/mqtt-over-quic.md, section 3).
    ///
    /// # Errors
    ///
    /// [`Error::Certificate`] when rcgen fails.
    pub fn client(&self, common_name: &str) -> Result<Identity, Error> {
        self.leaf(common_name, &[], Usage::Client)
    }

    /// A client certificate with no extended key usage at all, which a server checking for
    /// clientAuth must refuse (R2 rule 3). Note that a plain RFC 5280 verifier, rustls's
    /// included, accepts it: an absent extension allows any use.
    ///
    /// # Errors
    ///
    /// [`Error::Certificate`] when rcgen fails.
    pub fn client_without_eku(&self, common_name: &str) -> Result<Identity, Error> {
        self.leaf(common_name, &[], Usage::None)
    }

    /// A certificate with `common_name` and the serverAuth usage only, presented as a client
    /// certificate: refused by any verifier that checks for clientAuth (R2 rule 3).
    ///
    /// # Errors
    ///
    /// [`Error::Certificate`] when rcgen fails.
    pub fn client_with_server_eku(&self, common_name: &str) -> Result<Identity, Error> {
        self.leaf(common_name, &[], Usage::Server)
    }

    /// A client certificate that names this authority as its issuer but is signed by another
    /// key: the wrong issuer, which only the signature check catches (R2 rule 2). A
    /// certificate from another [`TestPki`] is the plainer case of a wrong issuer.
    ///
    /// # Errors
    ///
    /// [`Error::Certificate`] when rcgen fails.
    pub fn impostor_client(&self, common_name: &str) -> Result<Identity, Error> {
        Self::with_key(&self.name, KeyPair::generate()?)?.client(common_name)
    }

    fn leaf(&self, common_name: &str, names: &[&str], usage: Usage) -> Result<Identity, Error> {
        let names: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
        let mut params = CertificateParams::new(names)?;
        params.distinguished_name = distinguished_name(common_name);
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = match usage {
            Usage::Server => vec![ExtendedKeyUsagePurpose::ServerAuth],
            Usage::Client => vec![ExtendedKeyUsagePurpose::ClientAuth],
            Usage::None => Vec::new(),
        };
        params.use_authority_key_identifier_extension = true;
        let key = KeyPair::generate()?;
        let certificate = params.signed_by(&key, &self.issuer)?;
        Ok(Identity {
            chain: vec![certificate.der().clone()],
            certificate_pem: certificate.pem(),
            key_pem: key.serialize_pem(),
            key_der: key.serialize_der(),
        })
    }
}

fn distinguished_name(common_name: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, common_name);
    name.push(DnType::OrganizationName, "OpenQTT test kit");
    name
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rustls::RootCertStore;
    use rustls::client::danger::ServerCertVerifier;
    use rustls::pki_types::{ServerName, UnixTime};
    use rustls::server::WebPkiClientVerifier;

    use super::*;

    /// The DER of the extended key usage extension's OID, 2.5.29.37.
    const EKU_OID: &[u8] = &[0x06, 0x03, 0x55, 0x1D, 0x25];
    /// The DER of id-kp-clientAuth, 1.3.6.1.5.5.7.3.2.
    const CLIENT_AUTH: &[u8] = &[0x06, 0x08, 0x2B, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x02];
    /// The DER of id-kp-serverAuth, 1.3.6.1.5.5.7.3.1.
    const SERVER_AUTH: &[u8] = &[0x06, 0x08, 0x2B, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x01];

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }

    fn verify_client(pki: &TestPki, identity: &Identity) -> Result<(), rustls::Error> {
        let mut roots = RootCertStore::empty();
        roots.add(pki.ca_certificate()).unwrap();
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .unwrap();
        verifier
            .verify_client_cert(&identity.chain[0], &[], UnixTime::now())
            .map(drop)
    }

    #[test]
    fn a_client_certificate_carries_clientauth_and_chains_to_the_ca() {
        let pki = TestPki::new("Test CA").unwrap();
        let device = pki.client("device-17").unwrap();
        let der = device.chain[0].as_ref();
        assert!(contains(der, EKU_OID) && contains(der, CLIENT_AUTH));
        assert!(!contains(der, SERVER_AUTH));
        assert!(contains(der, b"device-17"));
        verify_client(&pki, &device).unwrap();
        assert!(
            device
                .certificate_pem
                .starts_with("-----BEGIN CERTIFICATE-----")
        );
        assert!(device.key_pem.starts_with("-----BEGIN PRIVATE KEY-----"));
    }

    #[test]
    fn negative_cases_are_what_they_say() {
        let pki = TestPki::new("Test CA").unwrap();

        // No extended key usage at all. rustls's verifier accepts it, as RFC 5280 allows, so
        // a server that requires clientAuth needs its own check.
        let no_eku = pki.client_without_eku("device-18").unwrap();
        assert!(!contains(no_eku.chain[0].as_ref(), EKU_OID));
        verify_client(&pki, &no_eku).unwrap();

        // A server certificate presented as a client certificate is refused.
        let server_only = pki.client_with_server_eku("device-19").unwrap();
        assert!(contains(server_only.chain[0].as_ref(), SERVER_AUTH));
        assert!(verify_client(&pki, &server_only).is_err());

        // The same issuer name with another key: refused.
        let impostor = pki.impostor_client("device-20").unwrap();
        assert!(contains(impostor.chain[0].as_ref(), CLIENT_AUTH));
        assert!(verify_client(&pki, &impostor).is_err());

        // A certificate from another authority altogether: refused.
        let other = TestPki::new("Other CA").unwrap();
        assert!(verify_client(&pki, &other.client("device-21").unwrap()).is_err());
    }

    #[test]
    fn a_server_certificate_names_its_hosts() {
        let pki = TestPki::new("Test CA").unwrap();
        let server = pki.server(&["localhost", "127.0.0.1"]).unwrap();
        let mut roots = RootCertStore::empty();
        roots.add(pki.ca_certificate()).unwrap();
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let verifier =
            rustls::client::WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()
                .unwrap();
        for name in ["localhost", "127.0.0.1"] {
            verifier
                .verify_server_cert(
                    &server.chain[0],
                    &[],
                    &ServerName::try_from(name).unwrap(),
                    &[],
                    UnixTime::now(),
                )
                .unwrap();
        }
        assert!(
            verifier
                .verify_server_cert(
                    &server.chain[0],
                    &[],
                    &ServerName::try_from("elsewhere.example").unwrap(),
                    &[],
                    UnixTime::now(),
                )
                .is_err()
        );
    }
}
