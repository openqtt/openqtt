//! A client's identity from its certificate (report R2, rules 2 to 4).
//!
//! The TLS handshake has verified the chain against the listener's client CAs before any of
//! this runs. What it leaves to this module:
//!
//! - **The clientAuth extended key usage** (rule 3). An RFC 5280 verifier, rustls's included,
//!   accepts a certificate with no extended key usage at all, since an absent extension allows
//!   any use. Here the extension must be present and name clientAuth; `anyExtendedKeyUsage`
//!   alone is not enough.
//! - **The subject CN** (rule 4): exactly one, a string of 1 to 64 bytes, as RFC 5280 bounds a
//!   common name. It may contain `/`. It becomes the client's User Name and its Client
//!   Identifier (R1 D20), and may not begin with the prefix reserved for service credentials
//!   (rule 15).
//! - **The issuing CA**, when pinned (rule 2). A listener's CA file should hold only the device
//!   issuing CA; a pin keeps a certificate from another intermediate under the same root out
//!   even when the file holds the root. A pin names a certificate of the listener's CA file, and
//!   is resolved to it once, when the listener is set up: a pin that names none is refused then.
//!   A client certificate passes only when the public key of a pinned certificate verifies its
//!   signature and its issuer is that certificate's subject. Nothing the client sends but its
//!   own certificate is looked at, so a forged issuer carrying a pinned Subject Key Identifier,
//!   or the pinned CA's real certificate sent beside a leaf another CA issued, gains nothing.
//!
//! A refusal is CONNACK 0x87 Not authorized.

use std::fmt;
use std::str::FromStr;

use aws_lc_rs::digest;
use openqtt_core::{ClientId, Username};
use openqtt_ext::{
    Authenticator, BoxFuture, Certificate, ConnectInfo, Grant, Principal, Refusal, Verdict,
};
use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::ParsedExtension;
use x509_parser::parse_x509_certificate;
use x509_parser::pem::Pem;

use crate::{Error, ReservedPrefix};

/// The longest CN, in bytes: RFC 5280's upper bound for a common name, and R2 rule 4's.
pub const MAX_COMMON_NAME: usize = 64;

/// A certificate of the listener's CA file that must have issued a client certificate (R2
/// rule 2). Each names a certificate the operator configured, never one a client sends.
#[derive(Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum IssuerPin {
    /// The SHA-256 of the CA's SubjectPublicKeyInfo, DER encoded: `spki-sha256:<hex>`. The pin
    /// to prefer, since it names the CA's key, and so outlives a renewal of the CA's
    /// certificate with the same key.
    SpkiSha256([u8; 32]),
    /// The SHA-256 of the CA's certificate, DER encoded, its fingerprint: `sha256:<hex>`.
    CertificateSha256([u8; 32]),
    /// The Subject Key Identifier the CA's certificate carries: `ski:<hex>`.
    SubjectKeyId(Box<[u8]>),
}

/// The forms of a pin, for a message about one that cannot be read.
const PIN_FORMS: &str = "a pin is `spki-sha256:<hex>`, `sha256:<hex>` or `ski:<hex>`";

impl IssuerPin {
    /// Reads `spki-sha256:<hex>`, `sha256:<hex>` or `ski:<hex>`; the hex may separate its bytes
    /// with `:`, as `openssl x509 -fingerprint` writes them.
    ///
    /// # Errors
    ///
    /// [`Error::IssuerPin`] for anything else, or a hash that is not 32 bytes.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let fail = |reason| Error::IssuerPin { reason };
        let (kind, hex) = text.split_once(':').ok_or_else(|| fail(PIN_FORMS))?;
        let bytes = decode_hex(hex).ok_or_else(|| fail("the value is not hexadecimal"))?;
        let hash = |bytes: Vec<u8>| -> Result<[u8; 32], Error> {
            bytes
                .try_into()
                .map_err(|_| fail("a SHA-256 hash is 32 bytes"))
        };
        match kind.to_ascii_lowercase().as_str() {
            "spki-sha256" => hash(bytes).map(Self::SpkiSha256),
            "sha256" => hash(bytes).map(Self::CertificateSha256),
            "ski" if !bytes.is_empty() => Ok(Self::SubjectKeyId(bytes.into_boxed_slice())),
            "ski" => Err(fail("a Subject Key Identifier is at least one byte")),
            _ => Err(fail(PIN_FORMS)),
        }
    }

    /// Whether this pin names `certificate`, one of the listener's CA certificates.
    fn names(&self, certificate: &X509Certificate<'_>) -> bool {
        let sha256 = |bytes: &[u8]| digest::digest(&digest::SHA256, bytes);
        match self {
            Self::SpkiSha256(pinned) => sha256(certificate.public_key().raw).as_ref() == pinned,
            Self::CertificateSha256(pinned) => sha256(certificate.as_raw()).as_ref() == pinned,
            Self::SubjectKeyId(pinned) => subject_key_id(certificate) == Some(&**pinned),
        }
    }
}

impl fmt::Debug for IssuerPin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IssuerPin({self})")
    }
}

impl fmt::Display for IssuerPin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, bytes): (_, &[u8]) = match self {
            Self::SpkiSha256(bytes) => ("spki-sha256", bytes),
            Self::CertificateSha256(bytes) => ("sha256", bytes),
            Self::SubjectKeyId(bytes) => ("ski", bytes),
        };
        f.write_str(kind)?;
        for byte in bytes {
            write!(f, ":{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for IssuerPin {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Error> {
        Self::parse(text)
    }
}

/// Hex digits, optionally with a `:` between each pair.
fn decode_hex(text: &str) -> Option<Vec<u8>> {
    let digits: Vec<u8> = text.bytes().filter(|&b| b != b':').collect();
    if digits.is_empty() || !digits.len().is_multiple_of(2) {
        return None;
    }
    digits
        .chunks(2)
        .map(|pair| {
            let high = char::from(pair[0]).to_digit(16)?;
            let low = char::from(pair[1]).to_digit(16)?;
            u8::try_from(high * 16 + low).ok()
        })
        .collect()
}

fn subject_key_id<'a>(certificate: &X509Certificate<'a>) -> Option<&'a [u8]> {
    certificate
        .extensions()
        .iter()
        .find_map(|extension| match extension.parsed_extension() {
            ParsedExtension::SubjectKeyIdentifier(id) => Some(id.0),
            _ => None,
        })
}

/// Reads the certificates of a PEM file, such as a listener's client CA file, in order.
///
/// # Errors
///
/// [`Error::Certificate`] when a block cannot be read, or is not a certificate.
pub fn certificates_from_pem(pem: &[u8]) -> Result<Vec<Certificate>, Error> {
    let fail = |reason| Error::Certificate { reason };
    let mut certificates = Vec::new();
    for block in Pem::iter_from_buffer(pem) {
        let block = block.map_err(|_| fail("a PEM block cannot be read"))?;
        if block.label != "CERTIFICATE" {
            return Err(fail("a PEM block is not a CERTIFICATE"));
        }
        parse_x509_certificate(&block.contents)
            .map_err(|_| fail("a PEM block is not an X.509 certificate"))?;
        certificates.push(Certificate::from_der(block.contents));
    }
    Ok(certificates)
}

/// Authenticates a client by its certificate, on a listener configured for certificate
/// identity: the CN names the client, as its User Name and its Client Identifier, whatever the
/// CONNECT says (R2 rule 4, R1 D20).
#[derive(Clone, Debug, Default)]
pub struct CertificateIdentity {
    reserved: Option<ReservedPrefix>,
    /// The listener's CA certificates the pins named: a client certificate must have been
    /// issued by one of them. Empty when nothing is pinned.
    issuers: Vec<Certificate>,
}

impl CertificateIdentity {
    /// Identifies clients by their CN, refusing a CN with `reserved`.
    pub fn new(reserved: Option<ReservedPrefix>) -> Self {
        Self {
            reserved,
            issuers: Vec::new(),
        }
    }

    /// And requires every client certificate to have been issued by a certificate one of `pins`
    /// names among `authorities`, the listener's client CA certificates, the ones the operator
    /// configured. Each pin is resolved here, once.
    ///
    /// # Errors
    ///
    /// [`Error::UnresolvedPin`] for a pin that names none of `authorities`, and
    /// [`Error::Certificate`] for one of them that cannot be read.
    pub fn with_issuer_pins(
        mut self,
        pins: &[IssuerPin],
        authorities: &[Certificate],
    ) -> Result<Self, Error> {
        let mut issuers: Vec<Certificate> = Vec::new();
        for pin in pins {
            let mut named = false;
            for authority in authorities {
                let (_, parsed) =
                    parse_x509_certificate(authority.der()).map_err(|_| Error::Certificate {
                        reason: "a CA certificate of the listener cannot be read",
                    })?;
                if pin.names(&parsed) {
                    named = true;
                    if !issuers.contains(authority) {
                        issuers.push(authority.clone());
                    }
                }
            }
            if !named {
                return Err(Error::UnresolvedPin {
                    pin: pin.to_string(),
                });
            }
        }
        self.issuers = issuers;
        Ok(self)
    }

    /// The CN of the leaf of `chain`, the chain a client presented, leaf first, once all the
    /// checks above pass.
    ///
    /// # Errors
    ///
    /// [`Error::Certificate`], saying which check failed.
    pub fn identify(&self, chain: &[Certificate]) -> Result<Username, Error> {
        let fail = |reason| Error::Certificate { reason };
        let leaf = chain
            .first()
            .ok_or_else(|| fail("the client presented no certificate"))?;
        let (rest, leaf) = parse_x509_certificate(leaf.der())
            .map_err(|_| fail("the client certificate cannot be read"))?;
        if !rest.is_empty() {
            return Err(fail("the client certificate has bytes after its end"));
        }
        let usage = leaf
            .extended_key_usage()
            .map_err(|_| fail("the extended key usage cannot be read"))?
            .ok_or_else(|| fail("the certificate has no extended key usage, so no clientAuth"))?;
        if !usage.value.client_auth {
            return Err(fail(
                "the certificate's extended key usage does not name clientAuth",
            ));
        }
        let mut names = leaf.subject().iter_common_name();
        let name = names
            .next()
            .ok_or_else(|| fail("the certificate's subject has no CN"))?;
        if names.next().is_some() {
            return Err(fail("the certificate's subject has more than one CN"));
        }
        let name = name
            .as_str()
            .map_err(|_| fail("the CN is not a UTF-8 or printable string"))?;
        if name.is_empty() || name.len() > MAX_COMMON_NAME {
            return Err(fail("the CN is not 1 to 64 bytes"));
        }
        let name = Username::new(name).map_err(|_| fail("the CN contains U+0000"))?;
        if self
            .reserved
            .as_ref()
            .is_some_and(|prefix| prefix.reserves(name.as_str()))
        {
            return Err(fail(
                "the CN begins with the prefix reserved for service credentials",
            ));
        }
        if !self.issuers.is_empty() && !self.issued_by_pinned(&leaf) {
            return Err(fail("the certificate was not issued by a pinned CA"));
        }
        Ok(name)
    }

    /// Whether a pinned CA issued `leaf`: one whose subject is the leaf's issuer and whose key,
    /// as the listener's configuration holds it, verifies the leaf's signature. The rest of the
    /// chain the client sent is never consulted: whatever it holds, a leaf only this key could
    /// have signed is the pinned CA's.
    fn issued_by_pinned(&self, leaf: &X509Certificate<'_>) -> bool {
        self.issuers.iter().any(|issuer| {
            let Ok((_, issuer)) = parse_x509_certificate(issuer.der()) else {
                return false;
            };
            issuer.subject().as_raw() == leaf.issuer().as_raw()
                && leaf.verify_signature(Some(issuer.public_key())).is_ok()
        })
    }
}

impl Authenticator for CertificateIdentity {
    fn authenticate<'a>(&'a self, connect: &'a ConnectInfo) -> BoxFuture<'a, Verdict> {
        let verdict = match self.identify(&connect.certificates) {
            Ok(name) => match ClientId::new(name.as_str()) {
                Ok(client_id) => {
                    Verdict::Allow(Grant::new(Principal::new(Some(name))).with_client_id(client_id))
                }
                Err(_) => Verdict::Deny(Refusal::NotAuthorized),
            },
            Err(error) => {
                // The client is told only 0x87; the operator learns which check failed.
                tracing::debug!(
                    listener = &*connect.listener,
                    address = %connect.address,
                    reason = %error,
                    "client certificate refused"
                );
                Verdict::Deny(Refusal::NotAuthorized)
            }
        };
        Box::pin(std::future::ready(verdict))
    }
}
