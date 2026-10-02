//! The server process: QUIC endpoints that accept MQTT-shaped connections and otherwise sit
//! idle, printing one JSON line of counters and process statistics every interval.
//!
//! Each connection is served the way the broker will: the client's first bidirectional stream
//! is the control stream, CONNECT is answered with CONNACK and PINGREQ with PINGRESP.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use serde_json::json;

use crate::config::{self, Tickets};
use crate::{Args, alloc, os};

static LIVE: AtomicU64 = AtomicU64::new(0);
static HANDSHAKES: AtomicU64 = AtomicU64::new(0);
static FAILED: AtomicU64 = AtomicU64::new(0);
static PINGS: AtomicU64 = AtomicU64::new(0);

const CONNACK: [u8; 4] = [0x20, 0x02, 0x00, 0x00];
const PINGRESP: [u8; 2] = [0xd0, 0x00];

async fn serve(conn: quinn::Connection) {
    if let Ok((mut send, mut recv)) = conn.accept_bi().await {
        let mut buf = [0u8; 512];
        while let Ok(Some(n)) = recv.read(&mut buf).await {
            if n == 0 {
                continue;
            }
            let reply: &[u8] = match buf[0] >> 4 {
                1 => &CONNACK,
                12 => {
                    PINGS.fetch_add(1, Relaxed);
                    &PINGRESP
                }
                _ => &[],
            };
            if !reply.is_empty() && send.write_all(reply).await.is_err() {
                break;
            }
        }
    }
    conn.closed().await;
}

async fn accept_loop(endpoint: quinn::Endpoint) {
    while let Some(incoming) = endpoint.accept().await {
        tokio::spawn(async move {
            let Ok(connecting) = incoming.accept() else {
                FAILED.fetch_add(1, Relaxed);
                return;
            };
            match connecting.await {
                Ok(conn) => {
                    HANDSHAKES.fetch_add(1, Relaxed);
                    LIVE.fetch_add(1, Relaxed);
                    serve(conn).await;
                    LIVE.fetch_sub(1, Relaxed);
                }
                Err(_) => {
                    FAILED.fetch_add(1, Relaxed);
                }
            }
        });
    }
}

fn stats_line(start: Instant) -> serde_json::Value {
    let heap = alloc::now();
    let (footprint, resident) = os::memory();
    let (user, sys) = os::cpu();
    json!({
        "t": start.elapsed().as_secs_f64(),
        "live": LIVE.load(Relaxed),
        "handshakes": HANDSHAKES.load(Relaxed),
        "failed": FAILED.load(Relaxed),
        "pings": PINGS.load(Relaxed),
        "resumed": config::RESUMED.load(Relaxed),
        "stored_sessions": config::STORED.load(Relaxed),
        "live_sessions": config::LIVE_SESSIONS.load(Relaxed),
        "evicted_sessions": config::EVICTED.load(Relaxed),
        "heap": heap.bytes,
        "allocs": heap.allocs,
        "footprint": footprint,
        "resident": resident,
        "user_s": user,
        "sys_s": sys,
    })
}

pub fn run(a: &Args) -> Result<(), crate::pki::Error> {
    let pki = PathBuf::from(a.text("pki", "target/pki"));
    let port = u16::try_from(a.num("port", 14567)).unwrap_or(14567);
    let endpoints = usize::try_from(a.num("endpoints", 1)).unwrap_or(1).max(1);
    let threads = usize::try_from(a.num("threads", 0)).unwrap_or(0);
    let profile = a.text("profile", "default");
    let pq = a.num("pq", 1) == 1;
    let tickets = match a.num("sessions", 0) {
        0 => Tickets::Stateless,
        n => Tickets::Stateful(usize::try_from(n).unwrap_or(1)),
    };
    let buffer = usize::try_from(a.num("socket-buffer", 8 << 20)).unwrap_or(8 << 20);
    let stats_ms = a.num("stats-ms", 1000);

    let server = config::server(&pki, &profile, pq, tickets)?;
    let mut sockets = Vec::new();
    let mut buffers = Vec::new();
    for i in 0..endpoints {
        let addr = SocketAddr::from(([127, 0, 0, 1], port + u16::try_from(i).unwrap_or(0)));
        let (s, r, w) = os::udp_socket(addr, buffer)?;
        buffers.push(json!({"port": addr.port(), "recv_buffer": r, "send_buffer": w}));
        sockets.push(s);
    }
    println!(
        "{}",
        json!({ "listening": buffers, "endpoints": endpoints, "profile": profile,
        "pq": pq, "tickets": format!("{tickets:?}"),
        "cache_preallocated": config::CACHE_PREALLOCATED.load(Relaxed) })
    );

    let start = Instant::now();
    std::thread::spawn(move || {
        loop {
            println!("{}", stats_line(start));
            std::thread::sleep(Duration::from_millis(stats_ms));
        }
    });

    let runtime_for = |workers: usize| -> std::io::Result<tokio::runtime::Runtime> {
        if workers == 1 {
            os::prefer_performance_cores();
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
        } else {
            let mut b = tokio::runtime::Builder::new_multi_thread();
            if workers > 1 {
                b.worker_threads(workers);
            }
            b.on_thread_start(os::prefer_performance_cores)
                .enable_all()
                .build()
        }
    };

    if endpoints == 1 {
        let rt = runtime_for(threads)?;
        let socket = sockets.remove(0);
        rt.block_on(async move {
            let ep = quinn::Endpoint::new(
                quinn::EndpointConfig::default(),
                Some(server),
                socket,
                Arc::new(quinn::TokioRuntime),
            )?;
            accept_loop(ep).await;
            Ok::<(), std::io::Error>(())
        })?;
    } else {
        // One endpoint per thread, each on its own single-threaded runtime: the shape R7
        // calls an endpoint per core.
        let mut handles = Vec::new();
        for socket in sockets {
            let server = server.clone();
            handles.push(std::thread::spawn(move || -> std::io::Result<()> {
                let rt = runtime_for(1)?;
                rt.block_on(async move {
                    let ep = quinn::Endpoint::new(
                        quinn::EndpointConfig::default(),
                        Some(server),
                        socket,
                        Arc::new(quinn::TokioRuntime),
                    )?;
                    accept_loop(ep).await;
                    Ok(())
                })
            }));
        }
        for h in handles {
            let _ = h.join();
        }
    }
    Ok(())
}
