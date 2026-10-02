//! Client identity from certificates (R2 rules 2 to 4, 15), over a throwaway PKI: a root, the
//! device issuing CA under it, another intermediate under the same root, and an impostor that
//! copies the device CA's name with a key of its own.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use openqtt_auth::{CertificateIdentity, Error, IssuerPin, ReservedPrefix, certificates_from_pem};
use openqtt_ext::{Authenticator, Certificate, ConnectInfo, Refusal, Verdict};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose,
};
use x509_parser::extensions::ParsedExtension;

/// A CA that lives as long as a test.
struct Ca {
    der: Vec<u8>,
    pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl Ca {
    fn root(name: &str) -> Self {
        let key = KeyPair::generate().expect("a key");
        let params = ca_params(name);
        let certificate = params.self_signed(&key).expect("a root");
        Self {
            der: certificate.der().to_vec(),
            pem: certificate.pem(),
            issuer: Issuer::new(params, key),
        }
    }

    fn intermediate(&self, name: &str) -> Self {
        let key = KeyPair::generate().expect("a key");
        let mut params = ca_params(name);
        params.use_authority_key_identifier_extension = true;
        let certificate = params
            .signed_by(&key, &self.issuer)
            .expect("an intermediate");
        Self {
            der: certificate.der().to_vec(),
            pem: certificate.pem(),
            issuer: Issuer::new(params, key),
        }
    }

    fn certificate(&self) -> Certificate {
        Certificate::from_der(self.der.clone())
    }

    /// A leaf with `names` as its subject and `usages` as its extended key usage.
    fn leaf(&self, names: &[(DnType, &str)], usages: &[ExtendedKeyUsagePurpose]) -> Certificate {
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
        let mut name = DistinguishedName::new();
        for (kind, value) in names {
            name.push(kind.clone(), *value);
        }
        params.distinguished_name = name;
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = usages.to_vec();
        params.use_authority_key_identifier_extension = true;
        let key = KeyPair::generate().expect("a key");
        let certificate = params.signed_by(&key, &self.issuer).expect("a leaf");
        Certificate::from_der(certificate.der().to_vec())
    }

    /// A device certificate: CN `name` and clientAuth.
    fn device(&self, name: &str) -> Certificate {
        self.leaf(
            &[(DnType::CommonName, name)],
            &[ExtendedKeyUsagePurpose::ClientAuth],
        )
    }

    fn subject_key_id(&self) -> Vec<u8> {
        let (_, parsed) = x509_parser::parse_x509_certificate(&self.der).expect("parses");
        parsed
            .extensions()
            .iter()
            .find_map(|extension| match extension.parsed_extension() {
                ParsedExtension::SubjectKeyIdentifier(id) => Some(id.0.to_vec()),
                _ => None,
            })
            .expect("a CA made by rcgen carries a Subject Key Identifier")
    }

    fn fingerprint(&self) -> String {
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &self.der);
        hex(digest.as_ref())
    }
}

fn ca_params(name: &str) -> CertificateParams {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
    let mut distinguished = DistinguishedName::new();
    distinguished.push(DnType::CommonName, name);
    params.distinguished_name = distinguished;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    params
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Polls a future that never waits.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("the future was not ready"),
    }
}

fn refusal(identity: &CertificateIdentity, chain: &[Certificate]) -> String {
    match identity.identify(chain) {
        Err(Error::Certificate { reason }) => reason.to_owned(),
        other => panic!("not refused: {other:?}"),
    }
}

#[test]
fn r2_rule_4_the_cn_is_the_username_and_the_client_identifier() {
    let root = Ca::root("Root");
    let devices = root.intermediate("Devices");
    let identity = CertificateIdentity::new(None);
    let chain = [
        devices.device("acme/production/pump-3"),
        devices.certificate(),
    ];
    assert_eq!(
        identity.identify(&chain).unwrap().as_str(),
        "acme/production/pump-3"
    );
    // The CONNECT's own names do not matter: the authenticator reads the certificate only.
    let connect = ConnectInfo::new("192.0.2.9:40000".parse().unwrap(), "devices")
        .with_client_id(openqtt_core::ClientId::new("someone-else").unwrap())
        .with_username(openqtt_core::Username::new("someone-else").unwrap())
        .with_certificates(chain.to_vec());
    let Verdict::Allow(grant) = ready(identity.authenticate(&connect)) else {
        panic!("a valid device was refused");
    };
    assert_eq!(
        grant.principal.username.unwrap().as_str(),
        "acme/production/pump-3"
    );
    assert_eq!(grant.client_id.unwrap().as_str(), "acme/production/pump-3");
}

#[test]
fn r2_rule_4_a_cn_is_one_string_of_1_to_64_bytes() {
    let root = Ca::root("Root");
    let identity = CertificateIdentity::new(None);
    let longest = "d".repeat(64);
    assert_eq!(
        identity
            .identify(&[root.device(&longest)])
            .unwrap()
            .as_str(),
        longest
    );
    assert_eq!(
        refusal(&identity, &[root.device(&"d".repeat(65))]),
        "the CN is not 1 to 64 bytes"
    );
    let no_cn = root.leaf(
        &[(DnType::OrganizationName, "Acme")],
        &[ExtendedKeyUsagePurpose::ClientAuth],
    );
    assert_eq!(
        refusal(&identity, &[no_cn]),
        "the certificate's subject has no CN"
    );
    // A second CN, under the same object identifier, makes the name ambiguous.
    let two = root.leaf(
        &[
            (DnType::CommonName, "pump-3"),
            (DnType::CustomDnType(vec![2, 5, 4, 3]), "pump-4"),
        ],
        &[ExtendedKeyUsagePurpose::ClientAuth],
    );
    assert_eq!(
        refusal(&identity, &[two]),
        "the certificate's subject has more than one CN"
    );
    assert_eq!(
        refusal(&identity, &[]),
        "the client presented no certificate"
    );
    assert_eq!(
        refusal(
            &identity,
            &[Certificate::from_der(vec![0x30, 0x03, 1, 2, 3])]
        ),
        "the client certificate cannot be read"
    );
}

#[test]
fn r2_rule_3_the_certificate_must_carry_client_auth() {
    let root = Ca::root("Root");
    let identity = CertificateIdentity::new(None);
    // No extended key usage at all: an RFC 5280 verifier, rustls's included, accepts it.
    let none = root.leaf(&[(DnType::CommonName, "pump-3")], &[]);
    assert_eq!(
        refusal(&identity, std::slice::from_ref(&none)),
        "the certificate has no extended key usage, so no clientAuth"
    );
    // A server certificate presented as a client certificate.
    let server = root.leaf(
        &[(DnType::CommonName, "pump-3")],
        &[ExtendedKeyUsagePurpose::ServerAuth],
    );
    assert_eq!(
        refusal(&identity, &[server]),
        "the certificate's extended key usage does not name clientAuth"
    );
    // anyExtendedKeyUsage alone is not clientAuth.
    let any = root.leaf(
        &[(DnType::CommonName, "pump-3")],
        &[ExtendedKeyUsagePurpose::Any],
    );
    assert_eq!(
        refusal(&identity, &[any]),
        "the certificate's extended key usage does not name clientAuth"
    );
    let both = root.leaf(
        &[(DnType::CommonName, "pump-3")],
        &[
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ],
    );
    identity.identify(&[both]).unwrap();
    // Refused as 0x87.
    let connect = ConnectInfo::new("192.0.2.9:40000".parse().unwrap(), "devices")
        .with_certificates(vec![none]);
    assert!(matches!(
        ready(identity.authenticate(&connect)),
        Verdict::Deny(Refusal::NotAuthorized)
    ));
}

#[test]
fn r2_rule_2_only_the_pinned_issuing_ca_is_trusted() {
    let root = Ca::root("Root");
    let devices = root.intermediate("Devices");
    let other = root.intermediate("Other");
    let pins = [
        IssuerPin::SubjectKeyId(devices.subject_key_id().into_boxed_slice()),
        format!("sha256:{}", devices.fingerprint()).parse().unwrap(),
    ];
    for pin in pins {
        // The listener's CA file holds the root and the device CA.
        let identity = CertificateIdentity::new(None).with_issuer_pins(
            vec![pin.clone()],
            vec![root.certificate(), devices.certificate()],
        );
        // Sent with its CA, or alone: the device CA is found either way.
        let device = devices.device("pump-3");
        identity
            .identify(&[device.clone(), devices.certificate()])
            .unwrap();
        identity.identify(&[device]).unwrap();
        // Another intermediate under the same root is refused (Changed from 1.x).
        let stranger = other.device("pump-3");
        assert_eq!(
            refusal(&identity, &[stranger, other.certificate()]),
            "the certificate was not issued by a pinned CA",
            "{pin}"
        );
        // An impostor with the device CA's name and its own key: the name matches, the
        // signature does not, and its own certificate is not pinned.
        let impostor = root.intermediate("Devices");
        let forged = impostor.device("pump-3");
        assert_eq!(
            refusal(&identity, std::slice::from_ref(&forged)),
            "the certificate was not issued by a pinned CA"
        );
        assert_eq!(
            refusal(&identity, &[forged, impostor.certificate()]),
            "the certificate was not issued by a pinned CA"
        );
    }
}

#[test]
fn r2_rule_15_a_cn_cannot_claim_a_service_name() {
    let root = Ca::root("Root");
    let identity = CertificateIdentity::new(Some(ReservedPrefix::new("svc:").unwrap()));
    assert_eq!(
        refusal(&identity, &[root.device("svc:platform")]),
        "the CN begins with the prefix reserved for service credentials"
    );
    identity.identify(&[root.device("pump-svc:3")]).unwrap();
}

#[test]
fn pins_read_as_openssl_writes_fingerprints() {
    let colons = "SHA256:".to_owned() + &["AB"; 32].join(":");
    let pin: IssuerPin = colons.parse().unwrap();
    assert_eq!(pin, IssuerPin::Sha256([0xAB; 32]));
    assert_eq!(pin.to_string(), format!("sha256:{}", ["ab"; 32].join(":")));
    let ski: IssuerPin = "ski:0a1B".parse().unwrap();
    assert_eq!(
        ski,
        IssuerPin::SubjectKeyId(vec![0x0a, 0x1b].into_boxed_slice())
    );
    for bad in [
        "",
        "ab",
        "md5:00",
        "sha256:00",
        "ski:",
        "ski:0",
        "ski:zz",
        "sha256",
    ] {
        assert!(bad.parse::<IssuerPin>().is_err(), "{bad}");
    }
}

#[test]
fn a_ca_file_reads_as_its_certificates() {
    let root = Ca::root("Root");
    let devices = root.intermediate("Devices");
    let pem = format!("{}{}", root.pem, devices.pem);
    let read = certificates_from_pem(pem.as_bytes()).unwrap();
    assert_eq!(read, [root.certificate(), devices.certificate()]);
    assert!(certificates_from_pem(b"").unwrap().is_empty());
    let key = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n";
    assert!(certificates_from_pem(key.as_bytes()).is_err());
}
