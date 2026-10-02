//! Certificates for a run: one CA, a server certificate and a pool of client certificates with
//! the clientAuth extended key usage, all ECDSA P-256 as rcgen generates them on aws-lc-rs. The
//! client CN has the shape a device CN has (R2: the CN is the device's identity).

use std::path::Path;

use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub fn generate(dir: &Path, clients: usize) -> Result<(), Error> {
    std::fs::create_dir_all(dir)?;
    let mut ca = CertificateParams::new(Vec::<String>::new())?;
    ca.distinguished_name
        .push(DnType::CommonName, "s4 device CA");
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca = CertifiedIssuer::self_signed(ca, KeyPair::generate()?)?;
    std::fs::write(dir.join("ca.pem"), ca.pem())?;

    let key = KeyPair::generate()?;
    let mut p = CertificateParams::new(vec!["localhost".to_string(), "*.s4.test".to_string()])?;
    p.subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse()?));
    p.distinguished_name.push(DnType::CommonName, "s4 edge");
    p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let cert = p.signed_by(&key, &ca)?;
    std::fs::write(dir.join("server.pem"), cert.pem())?;
    std::fs::write(dir.join("server.key"), key.serialize_pem())?;

    let (mut certs, mut keys) = (String::new(), String::new());
    for i in 0..clients {
        let key = KeyPair::generate()?;
        let mut p = CertificateParams::new(Vec::<String>::new())?;
        p.distinguished_name
            .push(DnType::CommonName, format!("acme/production/device-{i:06}"));
        p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        certs.push_str(&p.signed_by(&key, &ca)?.pem());
        keys.push_str(&key.serialize_pem());
    }
    std::fs::write(dir.join("clients.pem"), certs)?;
    std::fs::write(dir.join("clients.key"), keys)?;
    Ok(())
}

pub fn roots(dir: &Path) -> Result<rustls::RootCertStore, Error> {
    let mut roots = rustls::RootCertStore::empty();
    for c in CertificateDer::pem_file_iter(dir.join("ca.pem"))? {
        roots.add(c?)?;
    }
    Ok(roots)
}

pub fn server(dir: &Path) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), Error> {
    let chain =
        CertificateDer::pem_file_iter(dir.join("server.pem"))?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(dir.join("server.key"))?;
    Ok((chain, key))
}

pub fn clients(
    dir: &Path,
) -> Result<Vec<(CertificateDer<'static>, PrivateKeyDer<'static>)>, Error> {
    let certs =
        CertificateDer::pem_file_iter(dir.join("clients.pem"))?.collect::<Result<Vec<_>, _>>()?;
    let keys =
        PrivateKeyDer::pem_file_iter(dir.join("clients.key"))?.collect::<Result<Vec<_>, _>>()?;
    Ok(certs.into_iter().zip(keys).collect())
}
