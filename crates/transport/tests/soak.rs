//! A soak of idle connections: thousands of clients connect with a client certificate, each sends
//! CONNECT and gets CONNACK, and the heap the server holds for them is counted, to compare with
//! report R7's budget of 40 KiB per idle connection (D1) and the 37.8 KiB it measured with
//! quinn's defaults and 28.6 KiB with its lean settings (S4).
//!
//! Ignored, since it takes a while and thousands of sockets' worth of memory:
//!
//! ```text
//! cargo nextest run -p openqtt-transport --test soak --run-ignored only --no-capture
//! ```
//!
//! The clients run in a second process, this test binary started again with a marker argument, so
//! that the heap counted is the server's alone. The parent hands them what they need on their
//! standard input, and ends them by closing it.

use std::alloc::System;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use cap::Cap;
use openqtt_testkit::codec::{ConnAck, Packet};
use openqtt_testkit::{TestPki, packets};
use openqtt_transport::{Accepting, ClientAuth, Event, Listener, ListenerConfig, MqttConnection};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

#[global_allocator]
static ALLOCATOR: Cap<System> = Cap::new(System, usize::MAX);

/// How many connections the soak holds.
const CONNECTIONS: usize = 4_000;

/// The argument that makes this binary the clients' process.
const CHILD: &str = "openqtt-transport-soak-clients";

/// The test that runs the clients.
const CLIENTS_TEST: &str = "the_clients_of_the_soak";

/// How long the connections stay idle before the heap is counted, so that what their setup
/// left in flight is acknowledged and freed.
const SETTLE: Duration = Duration::from_secs(3);

/// Serves one connection as an edge would while it is idle: CONNACK for its CONNECT, PINGRESP for
/// its PINGREQ, and nothing else.
async fn serve(accepting: Accepting, connected: Arc<AtomicUsize>) {
    let Ok(mut connection) = accepting.establish().await else {
        return;
    };
    loop {
        match connection.recv().await {
            Ok(Event::Packet {
                stream,
                packet: Packet::Connect(_),
            }) => {
                if connection
                    .send(stream, &Packet::from(ConnAck::default()))
                    .is_err()
                {
                    return;
                }
                connected.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Event::Packet {
                stream,
                packet: Packet::PingReq,
            }) => {
                if connection.send(stream, &Packet::PingResp).is_err() {
                    return;
                }
            }
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

#[test]
#[ignore = "a soak of thousands of connections, run on purpose"]
#[expect(clippy::print_stderr, reason = "the soak reports what it measured")]
fn an_idle_connection_costs_about_what_r7_budgets() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let pki = TestPki::new("Soak CA").unwrap();
    let identity = pki.server(&["localhost"]).unwrap();
    let device = pki.client("device-soak").unwrap();
    let config = ListenerConfig::new("soak", identity.chain.clone(), identity.key())
        .address(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .client_auth(ClientAuth::Required(vec![pki.ca_certificate()]));
    let connected = Arc::new(AtomicUsize::new(0));
    let (address, accept) = runtime.block_on(async {
        let endpoint = Listener::new(config).unwrap().bind(0).unwrap();
        let address = endpoint.local_address();
        let connected = Arc::clone(&connected);
        let accept = tokio::spawn(async move {
            while let Some(accepting) = endpoint.accept().await {
                tokio::spawn(serve(accepting, Arc::clone(&connected)));
            }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        (address, accept)
    });
    let before = ALLOCATOR.allocated();
    let started = Instant::now();

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([CLIENTS_TEST, "--exact", "--ignored", "--nocapture", CHILD])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let materials = [pki.ca_pem(), &device.certificate_pem, &device.key_pem].join("\0");
    write!(
        input,
        "{address} {CONNECTIONS} {}\n{materials}",
        materials.len()
    )
    .unwrap();
    input.flush().unwrap();
    let output = BufReader::new(child.stdout.take().unwrap());
    let ready = output
        .lines()
        .map_while(Result::ok)
        .any(|line| line == "ready");
    assert!(ready, "the clients connected");
    runtime.block_on(async {
        while connected.load(Ordering::Relaxed) < CONNECTIONS {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(SETTLE).await;
    });
    let after = ALLOCATOR.allocated();
    let per_connection = after.saturating_sub(before) / CONNECTIONS;
    eprintln!(
        "{CONNECTIONS} idle connections in {:.1} s: {} KiB of heap on the server, {:.1} KiB each",
        started.elapsed().as_secs_f64(),
        after.saturating_sub(before) / 1_024,
        f64::from(u32::try_from(per_connection).unwrap_or(u32::MAX)) / 1_024.0
    );
    drop(input);
    child.wait().unwrap();
    accept.abort();
    // R7 budgets 40 KiB per idle connection for the transport (D1), before the session's state.
    assert!(
        per_connection < 40 * 1_024,
        "{per_connection} bytes per idle connection, over R7's budget"
    );
}

/// The clients of the soak: idle once their CONNECT has its CONNACK, until the parent closes
/// their standard input. As a test of its own, without the marker argument, it does nothing.
#[test]
#[ignore = "the clients of the soak, started by it"]
#[expect(
    clippy::print_stdout,
    reason = "the clients tell the soak they are ready"
)]
fn the_clients_of_the_soak() {
    if !std::env::args().any(|argument| argument == CHILD) {
        return;
    }
    let mut stdin = BufReader::new(std::io::stdin());
    let mut header = String::new();
    stdin.read_line(&mut header).unwrap();
    let mut fields = header.split_whitespace();
    let address: SocketAddr = fields.next().unwrap().parse().unwrap();
    let count: usize = fields.next().unwrap().parse().unwrap();
    let length: usize = fields.next().unwrap().parse().unwrap();
    let mut materials = vec![0; length];
    stdin.read_exact(&mut materials).unwrap();
    let materials = String::from_utf8(materials).unwrap();
    let [ca, certificate, key]: [&str; 3] = materials
        .split('\0')
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    let config = client_config(ca, certificate, key);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let held = runtime.block_on(async {
        // Like R7's client: a few dozen sockets, a few hundred handshakes at a time.
        let endpoints: Vec<quinn::Endpoint> = (0..32)
            .map(|_| quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).unwrap())
            .collect();
        let handshakes = Arc::new(tokio::sync::Semaphore::new(256));
        let mut tasks = tokio::task::JoinSet::new();
        for index in 0..count {
            let permit = Arc::clone(&handshakes).acquire_owned().await.unwrap();
            let endpoint = endpoints[index % endpoints.len()].clone();
            let config = config.clone();
            tasks.spawn(async move {
                let connection = endpoint
                    .connect_with(config, address, "localhost")
                    .unwrap()
                    .await
                    .unwrap();
                let (mut send, mut recv) = connection.open_bi().await.unwrap();
                let mut connect = BytesMut::new();
                Packet::from(packets::connect(&format!("device-{index}")))
                    .encode(&mut connect)
                    .unwrap();
                send.write_all(&connect).await.unwrap();
                let mut connack = [0; 5];
                recv.read_exact(&mut connack).await.unwrap();
                drop(permit);
                (connection, send, recv)
            });
        }
        let mut held = Vec::with_capacity(count);
        while let Some(connection) = tasks.join_next().await {
            held.push(connection.unwrap());
        }
        (endpoints, held)
    });
    println!("ready");
    // Idle until the parent is done counting.
    let mut rest = Vec::new();
    drop(stdin.read_to_end(&mut rest));
    drop(held);
}

/// A client presenting `certificate` and `key`, trusting `ca`, PEM all three.
fn client_config(ca: &str, certificate: &str, key: &str) -> quinn::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_slice_iter(ca.as_bytes()) {
        roots
            .add(certificate.expect("a CA certificate"))
            .expect("a CA rustls takes");
    }
    let chain = CertificateDer::pem_slice_iter(certificate.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .expect("a client certificate");
    let key = PrivateKeyDer::from_pem_slice(key.as_bytes()).expect("a private key");
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .expect("TLS 1.3")
        .with_root_certificates(roots)
        .with_client_auth_cert(chain, key)
        .expect("a client certificate rustls takes");
    tls.alpn_protocols = vec![b"mqtt".to_vec()];
    let crypto = QuicClientConfig::try_from(Arc::new(tls)).expect("TLS for QUIC");
    let mut transport = quinn::TransportConfig::default();
    transport
        .max_idle_timeout(Some(
            quinn::IdleTimeout::try_from(Duration::from_secs(600)).expect("an idle timeout"),
        ))
        .datagram_receive_buffer_size(None);
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport));
    config
}
