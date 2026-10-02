//! TLS for the client listener: rustls on aws-lc-rs, TLS 1.3 only, ALPN `mqtt`, client
//! certificates verified against the listener's CAs alone, and resumption as report R7 decided.

use std::sync::Arc;

use rustls::RootCertStore;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::danger::ClientCertVerifier;
use rustls::server::{NoServerSessionStorage, ServerSessionMemoryCache, WebPkiClientVerifier};

use crate::Error;
use crate::config::{ClientAuth, ListenerConfig, Resumption};

/// The ALPN protocol of MQTT over QUIC (docs/spec/mqtt-over-quic.md, section 1). rustls refuses
/// a QUIC handshake that does not offer it, with TLS alert 120, no application protocol.
pub const ALPN: &[u8] = b"mqtt";

/// The rustls configuration of a listener presenting `chain` and `key`.
pub(crate) fn server_config(
    config: &ListenerConfig,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Arc<rustls::ServerConfig>, Error> {
    // aws-lc-rs is the one crypto provider (deny.toml). Its defaults put the post-quantum key
    // share X25519MLKEM768 first, as R7 decided (D6), since the workspace builds rustls with
    // prefer-post-quantum.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13])?;
    let builder = match &config.client_auth {
        ClientAuth::None => builder.with_no_client_auth(),
        ClientAuth::Optional(roots) => {
            builder.with_client_cert_verifier(verifier(roots, &provider, true)?)
        }
        ClientAuth::Required(roots) => {
            builder.with_client_cert_verifier(verifier(roots, &provider, false)?)
        }
    };
    let mut tls = builder.with_single_cert(chain, key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    match config.resumption {
        Resumption::Off => {
            tls.session_storage = Arc::new(NoServerSessionStorage {});
            tls.send_tls13_tickets = 0;
        }
        Resumption::Tickets => {
            // Nothing is stored per client: the ticket carries the session, sealed with keys
            // that rotate every six hours.
            tls.session_storage = Arc::new(NoServerSessionStorage {});
            tls.ticketer = rustls::crypto::aws_lc_rs::Ticketer::new()?;
        }
        Resumption::SessionCache(capacity) => {
            tls.session_storage = ServerSessionMemoryCache::new(capacity.get());
        }
    }
    if config.early_data {
        if !matches!(config.resumption, Resumption::SessionCache(_)) {
            return Err(Error::Setting {
                setting: "early_data".to_owned(),
                reason: "needs the session cache: rustls accepts 0-RTT data only from it, never \
                         with stateless tickets (R7, F2)"
                    .to_owned(),
            });
        }
        // QUIC allows no other value than 0 and this one (RFC 9001, section 4.6.1).
        tls.max_early_data_size = u32::MAX;
    }
    Ok(Arc::new(tls))
}

/// A verifier of client certificates against `roots` and nothing else (R2 rule 2): the
/// operator's CA file, never a system trust store. `optional` lets a client present none.
///
/// rustls refuses a certificate whose extended key usage lacks clientAuth, but takes one with no
/// extended key usage at all (R7, F4); `openqtt-auth` requires the extension, from the chain the
/// connection hands up.
fn verifier(
    roots: &[CertificateDer<'static>],
    provider: &Arc<CryptoProvider>,
    optional: bool,
) -> Result<Arc<dyn ClientCertVerifier>, Error> {
    let mut store = RootCertStore::empty();
    for root in roots {
        store.add(root.clone())?;
    }
    let builder =
        WebPkiClientVerifier::builder_with_provider(Arc::new(store), Arc::clone(provider));
    let builder = if optional {
        builder.allow_unauthenticated()
    } else {
        builder
    };
    builder.build().map_err(|error| Error::Setting {
        setting: "client_ca_file".to_owned(),
        reason: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use openqtt_testkit::TestPki;

    use super::*;

    fn config(pki: &TestPki) -> ListenerConfig {
        let identity = pki.server(&["localhost"]).unwrap();
        ListenerConfig::new("default", identity.chain.clone(), identity.key())
    }

    fn tls(config: &ListenerConfig, pki: &TestPki) -> Result<Arc<rustls::ServerConfig>, Error> {
        let identity = pki.server(&["localhost"]).unwrap();
        server_config(config, identity.chain.clone(), identity.key())
    }

    #[test]
    fn tls_is_1_3_with_alpn_mqtt_and_stateless_tickets() {
        let pki = TestPki::new("TLS CA").unwrap();
        let tls = tls(&config(&pki), &pki).unwrap();
        assert_eq!(tls.alpn_protocols, vec![b"mqtt".to_vec()]);
        assert!(tls.ticketer.enabled());
        assert_eq!(tls.ticketer.lifetime(), 12 * 60 * 60);
        assert!(!tls.session_storage.can_cache());
        assert_eq!(tls.max_early_data_size, 0);
    }

    #[test]
    fn the_post_quantum_key_share_comes_first() {
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        assert_eq!(
            provider.kx_groups.first().map(|group| group.name()),
            Some(rustls::NamedGroup::X25519MLKEM768)
        );
    }

    #[test]
    fn resumption_can_be_off_or_a_session_cache() {
        let pki = TestPki::new("TLS CA").unwrap();
        let off = tls(&config(&pki).resumption(Resumption::Off), &pki).unwrap();
        assert!(!off.ticketer.enabled());
        assert_eq!(off.send_tls13_tickets, 0);
        let capacity = NonZeroUsize::new(16).unwrap();
        let cache = tls(
            &config(&pki).resumption(Resumption::SessionCache(capacity)),
            &pki,
        )
        .unwrap();
        assert!(!cache.ticketer.enabled());
        assert!(cache.session_storage.can_cache());
    }

    #[test]
    fn early_data_needs_the_session_cache() {
        let pki = TestPki::new("TLS CA").unwrap();
        let error = tls(&config(&pki).early_data(true), &pki).unwrap_err();
        assert!(error.to_string().contains("session cache"), "{error}");
        let capacity = NonZeroUsize::new(16).unwrap();
        let tls = tls(
            &config(&pki)
                .resumption(Resumption::SessionCache(capacity))
                .early_data(true),
            &pki,
        )
        .unwrap();
        assert_eq!(tls.max_early_data_size, u32::MAX);
    }

    #[test]
    fn a_client_ca_that_does_not_parse_is_refused() {
        let pki = TestPki::new("TLS CA").unwrap();
        let config =
            config(&pki).client_auth(ClientAuth::Required(vec![CertificateDer::from(vec![
                0x30, 0x03, 0x01, 0x01, 0xFF,
            ])]));
        assert!(tls(&config, &pki).is_err());
        let config = config.client_auth(ClientAuth::Required(Vec::new()));
        let error = tls(&config, &pki).unwrap_err();
        assert!(error.to_string().starts_with("client_ca_file"), "{error}");
    }
}
