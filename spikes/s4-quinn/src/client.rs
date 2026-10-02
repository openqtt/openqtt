//! The client process. Two modes:
//!
//! - `idle`: open N connections like N devices (CONNECT, CONNACK), then keep each alive every
//!   interval and otherwise stay quiet, until killed. Progress goes to stdout as JSON lines.
//! - `handshake`: lanes that connect, exchange CONNECT and CONNACK, close and connect again,
//!   for a fixed time; the summary goes to stdout as one JSON line.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::Semaphore;

use crate::{Args, config, os};

static ESTABLISHED: AtomicU64 = AtomicU64::new(0);
static FAILED: AtomicU64 = AtomicU64::new(0);
static CLOSED: AtomicU64 = AtomicU64::new(0);
static PINGS: AtomicU64 = AtomicU64::new(0);
static ZERO_RTT_TRIED: AtomicU64 = AtomicU64::new(0);
static ZERO_RTT_ACCEPTED: AtomicU64 = AtomicU64::new(0);

/// CONNECT for MQTT 5 with a 30 s keepalive and an empty client id; the server only looks at
/// the packet type.
const CONNECT: [u8; 15] = [
    0x10, 0x0d, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x1e, 0x00, 0x00, 0x00,
];
const PINGREQ: [u8; 2] = [0xc0, 0x00];

fn endpoints(
    count: usize,
    buffer: usize,
) -> Result<(Vec<quinn::Endpoint>, usize), crate::pki::Error> {
    let mut eps = Vec::new();
    let mut recv = 0;
    for _ in 0..count {
        let (s, r, _) = os::udp_socket(SocketAddr::from(([127, 0, 0, 1], 0)), buffer)?;
        recv = r;
        eps.push(quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            None,
            s,
            Arc::new(quinn::TokioRuntime),
        )?);
    }
    Ok((eps, recv))
}

/// Connects and exchanges CONNECT for CONNACK on the control stream.
async fn open(
    ep: &quinn::Endpoint,
    cfg: quinn::ClientConfig,
    addr: SocketAddr,
    name: &str,
) -> Result<(quinn::Connection, quinn::SendStream, quinn::RecvStream), crate::pki::Error> {
    let conn = ep.connect_with(cfg, addr, name)?.await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&CONNECT).await?;
    let mut ack = [0u8; 4];
    recv.read_exact(&mut ack).await?;
    Ok((conn, send, recv))
}

pub fn run(a: &Args) -> Result<(), crate::pki::Error> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(usize::try_from(a.num("threads", 8)).unwrap_or(8))
        .enable_all()
        .build()?;
    rt.block_on(async {
        match a.text("mode", "idle").as_str() {
            "handshake" => handshakes(a).await,
            _ => idle(a).await,
        }
    })
}

async fn idle(a: &Args) -> Result<(), crate::pki::Error> {
    let pki = PathBuf::from(a.text("pki", "target/pki"));
    let port = u16::try_from(a.num("port", 14567)).unwrap_or(14567);
    let server_ports = a.num("endpoints", 1).max(1);
    let n = a.num("conns", 1000);
    let interval = Duration::from_secs(a.num("interval", 30));
    let keepalive = a.text("keepalive", "quic");
    let profile = a.text("profile", "default");
    let pq = a.num("pq", 1) == 1;
    let quic_ping = (keepalive == "quic").then_some(interval);
    let cfgs = config::clients(&pki, &profile, pq, quic_ping, false, false)?;
    let sockets = usize::try_from(a.num("sockets", 16)).unwrap_or(16).max(1);
    let (eps, recv_buffer) = endpoints(sockets, 8 << 20)?;
    let gate = Arc::new(Semaphore::new(
        usize::try_from(a.num("concurrency", 256)).unwrap_or(256),
    ));
    let start = Instant::now();
    println!(
        "{}",
        json!({ "client_sockets": sockets, "recv_buffer": recv_buffer })
    );

    for i in 0..n {
        let iu = usize::try_from(i).unwrap_or(0);
        let ep = eps[iu % eps.len()].clone();
        let cfg = cfgs[iu % cfgs.len()].clone();
        let addr = SocketAddr::from((
            [127, 0, 0, 1],
            port + u16::try_from(i % server_ports).unwrap_or(0),
        ));
        let gate = gate.clone();
        let keepalive = keepalive.clone();
        tokio::spawn(async move {
            let permit = gate.acquire_owned().await;
            let (conn, mut send, mut recv) = match open(&ep, cfg, addr, "localhost").await {
                Ok(x) => x,
                Err(e) => {
                    if FAILED.fetch_add(1, Relaxed) < 5 {
                        eprintln!("connect failed: {e}");
                    }
                    return;
                }
            };
            ESTABLISHED.fetch_add(1, Relaxed);
            drop(permit);
            // A first PINGREQ at a random point of the interval spreads the keepalives evenly:
            // quinn restarts its keep-alive timer on every packet it receives, the PINGRESP
            // included.
            let offset = (i.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 11)
                % (interval.as_millis() as u64).max(1);
            tokio::time::sleep(Duration::from_millis(offset)).await;
            let mut pong = [0u8; 2];
            let mut ok =
                send.write_all(&PINGREQ).await.is_ok() && recv.read_exact(&mut pong).await.is_ok();
            if ok {
                PINGS.fetch_add(1, Relaxed);
            }
            if keepalive == "stream" {
                while ok {
                    tokio::time::sleep(interval).await;
                    ok = send.write_all(&PINGREQ).await.is_ok()
                        && recv.read_exact(&mut pong).await.is_ok();
                    if ok {
                        PINGS.fetch_add(1, Relaxed);
                    }
                }
            }
            conn.closed().await;
            CLOSED.fetch_add(1, Relaxed);
        });
    }

    let mut ready = false;
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (e, f) = (ESTABLISHED.load(Relaxed), FAILED.load(Relaxed));
        let line = json!({ "t": start.elapsed().as_secs_f64(), "established": e, "failed": f,
            "closed": CLOSED.load(Relaxed), "pings": PINGS.load(Relaxed) });
        if !ready && e + f >= n {
            ready = true;
            println!(
                "{}",
                json!({ "ready": true, "established": e, "failed": f,
                "seconds": start.elapsed().as_secs_f64() })
            );
        } else {
            println!("{line}");
        }
    }
}

fn percentile(v: &mut [u64], p: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i.min(v.len() - 1)]
}

async fn handshakes(a: &Args) -> Result<(), crate::pki::Error> {
    let pki = PathBuf::from(a.text("pki", "target/pki"));
    let port = u16::try_from(a.num("port", 14567)).unwrap_or(14567);
    let lanes = usize::try_from(a.num("lanes", 64)).unwrap_or(64);
    let warmup = Duration::from_secs(a.num("warmup", 3));
    let duration = Duration::from_secs(a.num("duration", 20));
    let kind = a.text("handshake", "full");
    let pq = a.num("pq", 1) == 1;
    let resume = kind != "full";
    let early = kind == "0rtt";
    let cfgs = config::clients(&pki, "default", pq, None, resume, early)?;
    let (eps, _) = endpoints(
        usize::try_from(a.num("sockets", 16)).unwrap_or(16).max(1),
        8 << 20,
    )?;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let start = Instant::now();
    let measure_from = start + warmup;
    let deadline = measure_from + duration;
    let mut tasks = Vec::new();
    for lane in 0..lanes {
        let ep = eps[lane % eps.len()].clone();
        let cfg = cfgs[lane % cfgs.len()].clone();
        // A server name per lane keeps each lane's tickets apart in the client's cache.
        let name = format!("lane{lane}.s4.test");
        tasks.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            let mut done = 0u64;
            while Instant::now() < deadline {
                let t0 = Instant::now();
                let measured = t0 >= measure_from;
                let Ok(connecting) = ep.connect_with(cfg.clone(), addr, &name) else {
                    FAILED.fetch_add(1, Relaxed);
                    continue;
                };
                let (conn, accepted) = if early {
                    match connecting.into_0rtt() {
                        Ok((conn, accepted)) => {
                            if measured {
                                ZERO_RTT_TRIED.fetch_add(1, Relaxed);
                            }
                            (conn, Some(accepted))
                        }
                        Err(connecting) => match connecting.await {
                            Ok(c) => (c, None),
                            Err(_) => {
                                FAILED.fetch_add(1, Relaxed);
                                continue;
                            }
                        },
                    }
                } else {
                    match connecting.await {
                        Ok(c) => (c, None),
                        Err(_) => {
                            FAILED.fetch_add(1, Relaxed);
                            continue;
                        }
                    }
                };
                let ok = async {
                    let (mut send, mut recv) = conn.open_bi().await?;
                    send.write_all(&CONNECT).await?;
                    let mut ack = [0u8; 4];
                    recv.read_exact(&mut ack).await?;
                    Ok::<(), crate::pki::Error>(())
                }
                .await
                .is_ok();
                if let Some(acc) = accepted
                    && acc.await
                    && measured
                {
                    ZERO_RTT_ACCEPTED.fetch_add(1, Relaxed);
                }
                conn.close(quinn::VarInt::from_u32(0), b"");
                if !ok {
                    FAILED.fetch_add(1, Relaxed);
                    continue;
                }
                if measured && Instant::now() < deadline {
                    done += 1;
                    lat.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
                }
            }
            (done, lat)
        }));
    }
    let mut done = 0;
    let mut lat = Vec::new();
    for t in tasks {
        if let Ok((d, l)) = t.await {
            done += d;
            lat.extend(l);
        }
    }
    println!(
        "{}",
        json!({
            "summary": true, "handshake": kind, "pq": pq, "lanes": lanes,
            "seconds": duration.as_secs_f64(), "completed": done,
            "per_second": done as f64 / duration.as_secs_f64(),
            "failed": FAILED.load(Relaxed),
            "zero_rtt_tried": ZERO_RTT_TRIED.load(Relaxed),
            "zero_rtt_accepted": ZERO_RTT_ACCEPTED.load(Relaxed),
            "latency_us_p50": percentile(&mut lat, 0.5),
            "latency_us_p99": percentile(&mut lat, 0.99),
        })
    );
    for ep in &eps {
        ep.close(quinn::VarInt::from_u32(0), b"");
    }
    Ok(())
}
