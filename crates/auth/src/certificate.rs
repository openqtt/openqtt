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
//!   even when the file holds the root. The CA that really issued the leaf is found by its
//!   signature, not by its name, among the certificates the client sent and the listener's CA
//!   certificates, and its Subject Key Identifier or the SHA-256 of its DER must be pinned.
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

/// The issuing CA a client certificate must come from (R2 rule 2).
#[derive(Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum IssuerPin {
    /// The issuing CA's Subject Key Identifier, as its certificate carries it.
    SubjectKeyId(Box<[u8]>),
    /// The SHA-256 of the issuing CA's certificate, DER encoded: its fingerprint.
    Sha256([u8; 32]),
}

impl IssuerPin {
    /// Reads `ski:<hex>` or `sha256:<hex>`; the hex may separate its bytes with `:`, as
    /// `openssl x509 -fingerprint` writes them.
    ///
    /// # Errors
    ///
    /// [`Error::IssuerPin`] for anything else, or a fingerprint that is not 32 bytes.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let fail = |reason| Error::IssuerPin { reason };
        let (kind, hex) = text
            .split_once(':')
            .ok_or_else(|| fail("a pin is `ski:<hex>` or `sha256:<hex>`"))?;
        let bytes = decode_hex(hex).ok_or_else(|| fail("the value is not hexadecimal"))?;
        match kind.to_ascii_lowercase().as_str() {
            "ski" if !bytes.is_empty() => Ok(Self::SubjectKeyId(bytes.into_boxed_slice())),
            "ski" => Err(fail("a Subject Key Identifier is at least one byte")),
            "sha256" => bytes
                .try_into()
                .map(Self::Sha256)
                .map_err(|_| fail("a SHA-256 fingerprint is 32 bytes")),
            _ => Err(fail("a pin is `ski:<hex>` or `sha256:<hex>`")),
        }
    }

    fn matches(&self, issuer: &X509Certificate<'_>) -> bool {
        match self {
            Self::SubjectKeyId(pinned) => subject_key_id(issuer) == Some(&**pinned),
            Self::Sha256(pinned) => {
                digest::digest(&digest::SHA256, issuer.as_raw()).as_ref() == pinned.as_slice()
            }
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
            Self::SubjectKeyId(bytes) => ("ski", bytes),
            Self::Sha256(bytes) => ("sha256", bytes),
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
    pins: Vec<IssuerPin>,
    authorities: Vec<Certificate>,
}

impl CertificateIdentity {
    /// Identifies clients by their CN, refusing a CN with `reserved`.
    pub fn new(reserved: Option<ReservedPrefix>) -> Self {
        Self {
            reserved,
            pins: Vec::new(),
            authorities: Vec::new(),
        }
    }

    /// And requires the CA that issued the leaf to match one of `pins`. It is looked for among
    /// the certificates the client sends and `authorities`, the listener's client CA
    /// certificates, since a client need not send the CA its certificate was issued by.
    #[must_use]
    pub fn with_issuer_pins(mut self, pins: Vec<IssuerPin>, authorities: Vec<Certificate>) -> Self {
        self.pins = pins;
        self.authorities = authorities;
        self
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
        if !self.pins.is_empty() && !self.issued_by_pinned(&leaf, &chain[1..]) {
            return Err(fail("the certificate was not issued by a pinned CA"));
        }
        Ok(name)
    }

    /// Whether a CA that matches a pin signed `leaf`: one whose subject is the leaf's issuer and
    /// whose key verifies the leaf's signature, among `sent` and the listener's CAs.
    fn issued_by_pinned(&self, leaf: &X509Certificate<'_>, sent: &[Certificate]) -> bool {
        sent.iter().chain(&self.authorities).any(|candidate| {
            let Ok((_, candidate)) = parse_x509_certificate(candidate.der()) else {
                return false;
            };
            candidate.subject().as_raw() == leaf.issuer().as_raw()
                && leaf.verify_signature(Some(candidate.public_key())).is_ok()
                && self.pins.iter().any(|pin| pin.matches(&candidate))
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
