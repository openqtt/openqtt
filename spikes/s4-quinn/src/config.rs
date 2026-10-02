//! TLS and QUIC configuration for both ends: the aws-lc-rs provider, TLS 1.3 only, ALPN `mqtt`
//! (docs/spec/mqtt-over-quic.md section 1), a required client certificate checked against the
//! one CA, and the transport profiles the spike compares.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{AckFrequencyConfig, IdleTimeout, TransportConfig, VarInt};
use rustls::client::Resumption;
use rustls::crypto::CryptoProvider;
use rustls::server::{
    ProducesTickets, ServerSessionMemoryCache, StoresServerSessions, WebPkiClientVerifier,
};

use crate::pki::{self, Error};

pub const ALPN: &[u8] = b"mqtt";

/// Resumptions the server granted: tickets it decrypted, or sessions it found in its cache.
pub static RESUMED: AtomicU64 = AtomicU64::new(0);
/// Sessions the server stored for stateful resumption, ever.
pub static STORED: AtomicU64 = AtomicU64::new(0);
/// Sessions the server's cache holds now: stored, less those taken by a resumption and those
/// the cache evicted.
pub static LIVE_SESSIONS: AtomicU64 = AtomicU64::new(0);
/// Sessions the cache evicted to make room.
pub static EVICTED: AtomicU64 = AtomicU64::new(0);
/// Bytes the cache allocated when it was made, before holding any session.
pub static CACHE_PREALLOCATED: AtomicU64 = AtomicU64::new(0);

/// aws-lc-rs, the only provider (ring is banned). With `pq` the key share is rustls's default,
/// X25519MLKEM768 first; without it only the classical groups are offered.
pub fn provider(pq: bool) -> Arc<CryptoProvider> {
    let mut p = rustls::crypto::aws_lc_rs::default_provider();
    if !pq {
        use rustls::crypto::aws_lc_rs::kx_group;
        p.kx_groups = vec![kx_group::X25519, kx_group::SECP256R1];
    }
    Arc::new(p)
}

/// Transport settings by name. Each profile but `default` and `lean` changes one thing, so its
/// effect can be read alone; `lean` is all of them together.
pub fn transport(profile: &str, keep_alive: Option<Duration>) -> Result<TransportConfig, Error> {
    let mut t = TransportConfig::default();
    // QUIC idle must outlast 1.5 times the MQTT keepalive (spec section 6); 30 s keepalive.
    t.max_idle_timeout(Some(IdleTimeout::try_from(Duration::from_secs(60))?));
    t.keep_alive_interval(keep_alive);
    let streams = |t: &mut TransportConfig| {
        // MQTT needs the control stream and a few data streams, all client-initiated and
        // bidirectional (spec section 2); the server opens none.
        t.max_concurrent_bidi_streams(VarInt::from_u32(4));
        t.max_concurrent_uni_streams(VarInt::from_u32(0));
    };
    // quinn's defaults are 1.25 MB per stream, unlimited per connection and 10 MB to send;
    // these are limits, not allocations, which is what this profile tests.
    let windows = |t: &mut TransportConfig| {
        t.stream_receive_window(VarInt::from_u32(64 << 10));
        t.receive_window(VarInt::from_u32(256 << 10));
        t.send_window(256 << 10);
    };
    let datagrams = |t: &mut TransportConfig| {
        t.datagram_receive_buffer_size(None);
        t.datagram_send_buffer_size(0);
    };
    let mtud = |t: &mut TransportConfig| {
        t.mtu_discovery_config(None);
    };
    match profile {
        "default" => {}
        "streams" => streams(&mut t),
        "windows" => windows(&mut t),
        "no-datagrams" => datagrams(&mut t),
        "no-mtu-discovery" => mtud(&mut t),
        "ack-frequency" => {
            t.ack_frequency_config(Some(AckFrequencyConfig::default()));
        }
        "lean" => {
            streams(&mut t);
            windows(&mut t);
            datagrams(&mut t);
            mtud(&mut t);
        }
        other => return Err(format!("unknown profile {other}").into()),
    }
    Ok(t)
}

/// How the server resumes sessions.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tickets {
    /// Encrypted tickets, nothing kept per client. rustls refuses 0-RTT with these.
    Stateless,
    /// A session cache of this many entries; needed for 0-RTT, which rustls allows only with
    /// single-use, stateful tickets.
    Stateful(usize),
}

struct CountingTicketer(Arc<dyn ProducesTickets>);

impl std::fmt::Debug for CountingTicketer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CountingTicketer")
    }
}

impl ProducesTickets for CountingTicketer {
    fn enabled(&self) -> bool {
        self.0.enabled()
    }
    fn lifetime(&self) -> u32 {
        self.0.lifetime()
    }
    fn encrypt(&self, plain: &[u8]) -> Option<Vec<u8>> {
        self.0.encrypt(plain)
    }
    fn decrypt(&self, cipher: &[u8]) -> Option<Vec<u8>> {
        let r = self.0.decrypt(cipher);
        if r.is_some() {
            RESUMED.fetch_add(1, Relaxed);
        }
        r
    }
}

/// rustls's session cache, counting what it holds. The cache does not say, so the count follows
/// its rules: every put is a new key (rustls stores each ticket under 32 random bytes), a take
/// that finds its key removes it, and an insertion that fills the cache's order queue evicts
/// the oldest entry. That queue is made with exactly the capacity asked for, which `server`
/// checks.
#[derive(Debug)]
struct CountingStore {
    cache: Arc<ServerSessionMemoryCache>,
    capacity: usize,
    live: std::sync::Mutex<usize>,
}

impl StoresServerSessions for CountingStore {
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> bool {
        let mut live = self.live.lock().expect("lock");
        STORED.fetch_add(1, Relaxed);
        let stored = self.cache.put(key, value);
        *live += 1;
        if *live == self.capacity {
            *live -= 1;
            EVICTED.fetch_add(1, Relaxed);
        }
        LIVE_SESSIONS.store(*live as u64, Relaxed);
        stored
    }
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let r = self.cache.get(key);
        if r.is_some() {
            RESUMED.fetch_add(1, Relaxed);
        }
        r
    }
    fn take(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut live = self.live.lock().expect("lock");
        let r = self.cache.take(key);
        if r.is_some() {
            RESUMED.fetch_add(1, Relaxed);
            *live -= 1;
            LIVE_SESSIONS.store(*live as u64, Relaxed);
        }
        r
    }
    fn can_cache(&self) -> bool {
        true
    }
}

pub fn server(
    pki_dir: &Path,
    profile: &str,
    pq: bool,
    tickets: Tickets,
) -> Result<quinn::ServerConfig, Error> {
    let p = provider(pq);
    let roots = Arc::new(pki::roots(pki_dir)?);
    // Only the one CA, and its client certificates must carry clientAuth (spec section 3).
    let verifier = WebPkiClientVerifier::builder_with_provider(roots, p.clone()).build()?;
    let (chain, key) = pki::server(pki_dir)?;
    let mut tls = rustls::ServerConfig::builder_with_provider(p)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, key)?;
    tls.alpn_protocols = vec![ALPN.to_vec()];
    match tickets {
        Tickets::Stateless => {
            tls.ticketer = Arc::new(CountingTicketer(rustls::crypto::aws_lc_rs::Ticketer::new()?));
            tls.max_early_data_size = 0;
        }
        Tickets::Stateful(n) => {
            // The eviction rule CountingStore follows assumes the queue's capacity is n.
            let queue = std::collections::VecDeque::<Vec<u8>>::with_capacity(n);
            if queue.capacity() != n {
                return Err(format!("a VecDeque made for {n} holds {}", queue.capacity()).into());
            }
            drop(queue);
            let before = crate::alloc::now().bytes;
            let cache = ServerSessionMemoryCache::new(n);
            let made = crate::alloc::now().bytes - before;
            CACHE_PREALLOCATED.store(made as u64, Relaxed);
            tls.session_storage = Arc::new(CountingStore {
                cache,
                capacity: n,
                live: std::sync::Mutex::new(0),
            });
            // QUIC allows only 0 or u32::MAX here; the latter accepts 0-RTT.
            tls.max_early_data_size = u32::MAX;
        }
    }
    let mut cfg = quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(tls)?));
    cfg.transport_config(Arc::new(transport(profile, None)?));
    Ok(cfg)
}

/// One client configuration per identity in the pool.
pub fn clients(
    pki_dir: &Path,
    profile: &str,
    pq: bool,
    keep_alive: Option<Duration>,
    resume: bool,
    early_data: bool,
) -> Result<Vec<quinn::ClientConfig>, Error> {
    let p = provider(pq);
    let roots = Arc::new(pki::roots(pki_dir)?);
    let transport = Arc::new(transport(profile, keep_alive)?);
    let mut out = Vec::new();
    for (cert, key) in pki::clients(pki_dir)? {
        let mut tls = rustls::ClientConfig::builder_with_provider(p.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])?
            .with_root_certificates(roots.clone())
            .with_client_auth_cert(vec![cert], key)?;
        tls.alpn_protocols = vec![ALPN.to_vec()];
        tls.resumption = if resume {
            Resumption::in_memory_sessions(256)
        } else {
            Resumption::disabled()
        };
        tls.enable_early_data = early_data;
        let mut cfg = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls)?));
        cfg.transport_config(transport.clone());
        out.push(cfg);
    }
    Ok(out)
}
