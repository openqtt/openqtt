//! Experiment 6. Replication: openraft with one group per partition against primary-backup with
//! epoch fencing, three replicas in one process over a simulated network.
//!
//! Both run on the same runtime, the same delay line (one-way delay per hop, 1 or 2 ms to model
//! zones in one region) and the same per-node disk model (group commit with a modelled flush
//! time). What differs is only the protocol. Absolute CPU figures are in-process: no
//! serialisation, no sockets, no TLS; `netcost` measures what a loopback message costs to put
//! next to them.

pub mod net;
pub mod pb;
pub mod raft;
pub mod raft10;

use std::io::{Read as _, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::{Value, json};

use crate::util::{Lat, Rng, client_id, emit, own_value, round2, usage};
use net::DelayLine;
use raft::{Cmd, Timing};

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Scheme {
    /// openraft 0.9.25
    Raft,
    /// openraft 0.10.0-alpha.36
    Raft10,
    Pb,
}

impl Scheme {
    fn name(self) -> &'static str {
        match self {
            Scheme::Raft => "raft",
            Scheme::Raft10 => "raft10",
            Scheme::Pb => "pb",
        }
    }
}

enum AnyCluster {
    Raft(raft::Cluster),
    Raft10(raft10::Cluster),
    Pb(pb::Cluster),
}

impl AnyCluster {
    async fn start(
        scheme: Scheme,
        groups: u32,
        line: DelayLine,
        delay: Duration,
        flush: Duration,
        timing: &Timing,
    ) -> Result<Self> {
        Ok(match scheme {
            Scheme::Raft => AnyCluster::Raft(raft::Cluster::start(groups, line, delay, flush, timing).await?),
            Scheme::Raft10 => {
                AnyCluster::Raft10(raft10::Cluster::start(groups, line, delay, flush, timing).await?)
            }
            Scheme::Pb => AnyCluster::Pb(pb::Cluster::start(groups, line, delay, flush)),
        })
    }

    async fn write(&self, g: u32, cmd: Cmd) -> Result<()> {
        match self {
            AnyCluster::Raft(c) => c.write(g, cmd).await,
            AnyCluster::Raft10(c) => c.write(g, cmd).await,
            AnyCluster::Pb(c) => c.write(g, cmd).await,
        }
    }

    fn messages(&self) -> u64 {
        match self {
            AnyCluster::Raft(c) => c.router.rpcs.load(Ordering::Relaxed),
            AnyCluster::Raft10(c) => c.router.rpcs.load(Ordering::Relaxed),
            AnyCluster::Pb(c) => c.messages(),
        }
    }

    fn disks(&self) -> &[net::Disk] {
        match self {
            AnyCluster::Raft(c) => &c.disks,
            AnyCluster::Raft10(c) => &c.disks,
            AnyCluster::Pb(c) => &c.disks,
        }
    }

    async fn extra(&self) -> Value {
        match self {
            AnyCluster::Raft(c) => json!({ "misplaced_leaders": c.misplaced_leaders() }),
            AnyCluster::Raft10(c) => json!({ "misplaced_leaders": c.misplaced_leaders().await }),
            AnyCluster::Pb(c) => json!({ "refused": c.refused() }),
        }
    }

    async fn shutdown(self) {
        match self {
            AnyCluster::Raft(c) => c.shutdown().await,
            AnyCluster::Raft10(c) => c.shutdown().await,
            AnyCluster::Pb(_) => {}
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn claim(rng: &mut Rng) -> Cmd {
    let cid = client_id(rng.below(100_000));
    Cmd {
        cid,
        val: own_value(1, 2, rng.next_u64()),
    }
}

#[derive(Clone, Copy)]
pub enum Offered {
    /// Writes per second across all groups, on schedule.
    Open(u64),
    /// Writes outstanding at all times, across all groups.
    Closed(u64),
}

impl Offered {
    fn label(self) -> String {
        match self {
            Offered::Open(r) => format!("open:{r}/s"),
            Offered::Closed(c) => format!("closed:{c}"),
        }
    }
}

/// Offers load to a running cluster and measures commit latency from when each write was due.
async fn offer(c: Arc<AnyCluster>, groups: u32, load: Offered, warmup: Duration, measure: Duration) -> Value {
    let start = Instant::now();
    let from = start + warmup;
    let until = from + measure;
    let lat = Arc::new(Mutex::new(Lat::default()));
    let completed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let outstanding = Arc::new(AtomicU64::new(0));
    let first_error: Arc<Mutex<Option<String>>> = Arc::default();
    let handle = tokio::runtime::Handle::current();
    let generator = match load {
        Offered::Open(rate) => {
            let (c, lat, completed, errors, outstanding) =
                (c.clone(), lat.clone(), completed.clone(), errors.clone(), outstanding.clone());
            let first_error = first_error.clone();
            Some(std::thread::spawn(move || {
                let mut rng = Rng::new(rate);
                let interval = Duration::from_secs_f64(1.0 / rate as f64);
                let mut next = start;
                while next < until {
                    let now = Instant::now();
                    while next <= now && next < until {
                        let g = rng.below(u64::from(groups)) as u32;
                        let cmd = claim(&mut rng);
                        let due = next;
                        let (c, lat, completed, errors, outstanding) = (
                            c.clone(),
                            lat.clone(),
                            completed.clone(),
                            errors.clone(),
                            outstanding.clone(),
                        );
                        let first_error = first_error.clone();
                        outstanding.fetch_add(1, Ordering::Relaxed);
                        handle.spawn(async move {
                            let r = c.write(g, cmd).await;
                            let at = Instant::now();
                            if let Err(e) = r {
                                errors.fetch_add(1, Ordering::Relaxed);
                                first_error.lock().expect("not poisoned").get_or_insert_with(|| {
                                    format!("{:.3}s after start: {e:#}", (at - start).as_secs_f64())
                                });
                            } else if due >= from && at <= until {
                                lat.lock().expect("not poisoned").record(at - due);
                                completed.fetch_add(1, Ordering::Relaxed);
                            }
                            outstanding.fetch_sub(1, Ordering::Relaxed);
                        });
                        next += interval;
                    }
                    std::thread::sleep(Duration::from_micros(100));
                }
            }))
        }
        Offered::Closed(n) => {
            for k in 0..n {
                let (c, lat, completed, errors, outstanding) =
                    (c.clone(), lat.clone(), completed.clone(), errors.clone(), outstanding.clone());
                let first_error = first_error.clone();
                outstanding.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let mut rng = Rng::new(k);
                    while Instant::now() < until {
                        let g = rng.below(u64::from(groups)) as u32;
                        let due = Instant::now();
                        let r = c.write(g, claim(&mut rng)).await;
                        let at = Instant::now();
                        if let Err(e) = r {
                            errors.fetch_add(1, Ordering::Relaxed);
                            first_error.lock().expect("not poisoned").get_or_insert_with(|| {
                                format!("{:.3}s after start: {e:#}", (at - start).as_secs_f64())
                            });
                        } else if due >= from && at <= until {
                            lat.lock().expect("not poisoned").record(at - due);
                            completed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    outstanding.fetch_sub(1, Ordering::Relaxed);
                });
            }
            None
        }
    };
    tokio::time::sleep_until(from.into()).await;
    let u0 = usage();
    let m0 = c.messages();
    let d0: Vec<(u64, u64)> = c
        .disks()
        .iter()
        .map(|d| (d.flushes.load(Ordering::Relaxed), d.items.load(Ordering::Relaxed)))
        .collect();
    tokio::time::sleep_until(until.into()).await;
    let u1 = usage();
    let m1 = c.messages();
    let (mut flushes, mut items) = (0, 0);
    for (d, (f0, i0)) in c.disks().iter().zip(d0) {
        flushes += d.flushes.load(Ordering::Relaxed) - f0;
        items += d.items.load(Ordering::Relaxed) - i0;
    }
    if let Some(g) = generator {
        let _ = tokio::task::spawn_blocking(move || g.join()).await;
    }
    let wait = Instant::now();
    while outstanding.load(Ordering::Relaxed) > 0 && wait.elapsed() < Duration::from_secs(60) {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let secs = measure.as_secs_f64();
    let n = completed.load(Ordering::Relaxed);
    let summary = lat.lock().expect("not poisoned").summary();
    json!({
        "lat_us": summary,
        "completed": n,
        "per_s": round2(n as f64 / secs),
        "errors": errors.load(Ordering::Relaxed),
        "first_error": *first_error.lock().expect("not poisoned"),
        "cpu_cores_total": round2((u1.cpu_s - u0.cpu_s) / secs),
        "messages_per_s": round2((m1 - m0) as f64 / secs),
        "disk_flushes_per_s": round2(flushes as f64 / secs),
        "disk_items_per_flush": if flushes > 0 { round2(items as f64 / flushes as f64) } else { 0.0 },
        "drain_s": round2(wait.elapsed().as_secs_f64()),
    })
}

pub struct LatencyArgs {
    pub schemes: Vec<Scheme>,
    pub groups: Vec<u32>,
    pub delays_us: Vec<u64>,
    pub flush_us: Vec<u64>,
    pub loads: Vec<Offered>,
    pub warmup: Duration,
    pub measure: Duration,
    pub timing: Timing,
}

pub fn parse_offered(s: &str) -> Offered {
    let (k, n) = s.split_at(1);
    let n: u64 = n.parse().expect("o<rate> or c<outstanding>");
    match k {
        "o" => Offered::Open(n),
        "c" => Offered::Closed(n),
        _ => panic!("o<rate> or c<outstanding>, got {s}"),
    }
}

pub fn latency(out: &std::path::Path, a: &LatencyArgs) -> Result<()> {
    let rt = runtime();
    for &groups in &a.groups {
        for &d in &a.delays_us {
            for &f in &a.flush_us {
                for &load in &a.loads {
                    for &scheme in &a.schemes {
                        let rec = rt.block_on(async {
                            let line = DelayLine::new();
                            let c = Arc::new(
                                AnyCluster::start(
                                    scheme,
                                    groups,
                                    line.clone(),
                                    Duration::from_micros(d),
                                    Duration::from_micros(f),
                                    &a.timing,
                                )
                                .await?,
                            );
                            line.lateness.lock().expect("not poisoned").reset();
                            let r = offer(c.clone(), groups, load, a.warmup, a.measure).await;
                            let extra = c.extra().await;
                            if let Ok(c) = Arc::try_unwrap(c) {
                                c.shutdown().await;
                            }
                            let late = line.lateness.lock().expect("not poisoned").summary();
                            anyhow::Ok(json!({
                                "exp": "repl", "scheme": scheme.name(), "groups": groups,
                                "delay_us": d, "flush_us": f, "load": load.label(),
                                "heartbeat_ms": a.timing.heartbeat_ms,
                                "result": r, "delay_lateness_us": late, "extra": extra,
                            }))
                        })?;
                        emit(out, rec);
                    }
                }
            }
        }
    }
    Ok(())
}

/// CPU an idle cluster burns: groups elected, nothing written, heartbeats only.
pub fn idle(
    out: &std::path::Path,
    schemes: &[Scheme],
    groups: &[u32],
    heartbeats: &[(u64, u64, u64)],
    delay_us: u64,
    secs: f64,
) -> Result<()> {
    let rt = runtime();
    for &g in groups {
        for &(hb, emin, emax) in heartbeats {
            for &scheme in schemes {
                if scheme == Scheme::Pb && hb != heartbeats[0].0 {
                    continue; // no per-group timers to vary
                }
                let timing = Timing {
                    heartbeat_ms: hb,
                    election_min_ms: emin,
                    election_max_ms: emax,
                };
                let rec = rt.block_on(async {
                    let line = DelayLine::new();
                    let c = AnyCluster::start(
                        scheme,
                        g,
                        line.clone(),
                        Duration::from_micros(delay_us),
                        Duration::ZERO,
                        &timing,
                    )
                    .await?;
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let u0 = usage();
                    let l0 = line.cpu_s();
                    let m0 = c.messages();
                    tokio::time::sleep(Duration::from_secs_f64(secs)).await;
                    let u1 = usage();
                    // The delay thread reports its CPU every 200 ms; let it catch up.
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    let l1 = line.cpu_s();
                    let m1 = c.messages();
                    let total = u1.cpu_s - u0.cpu_s;
                    let line_cpu = (l1 - l0).max(0.0);
                    let protocol = (total - line_cpu).max(0.0);
                    let extra = c.extra().await;
                    c.shutdown().await;
                    anyhow::Ok(json!({
                        "exp": "repl-idle", "scheme": scheme.name(), "groups": g,
                        "heartbeat_ms": if scheme == Scheme::Pb { 0 } else { hb },
                        "election_ms": [emin, emax], "delay_us": delay_us, "secs": secs,
                        "cpu_cores_total": round2(total / secs),
                        "cpu_cores_delay_line": round2(line_cpu / secs),
                        "cpu_cores_per_node": round2(protocol / secs / 3.0),
                        "cpu_us_per_group_replica_per_s": round2(protocol / secs / f64::from(g * 3) * 1e6),
                        "messages_per_s": round2((m1 - m0) as f64 / secs),
                        "extra": extra,
                    }))
                })?;
                emit(out, rec);
            }
        }
    }
    Ok(())
}

/// What a small request and its reply cost over loopback TCP, CPU per round trip, to set next to
/// the in-process message counts. QUIC with TLS and protobuf costs more, so this is a floor.
pub fn netcost(out: &std::path::Path, secs: f64, size: usize) -> Result<()> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = std::thread::spawn(move || -> std::io::Result<()> {
        let (mut s, _) = listener.accept()?;
        s.set_nodelay(true)?;
        let mut buf = vec![0u8; size];
        loop {
            if s.read_exact(&mut buf).is_err() {
                return Ok(());
            }
            s.write_all(&buf)?;
        }
    });
    let mut c = std::net::TcpStream::connect(addr)?;
    c.set_nodelay(true)?;
    let buf = vec![7u8; size];
    let mut rbuf = vec![0u8; size];
    let u0 = usage();
    let t = Instant::now();
    let mut n = 0u64;
    let mut lat = Lat::default();
    while t.elapsed().as_secs_f64() < secs {
        let t0 = Instant::now();
        c.write_all(&buf)?;
        c.read_exact(&mut rbuf)?;
        lat.record(t0.elapsed());
        n += 1;
    }
    let u1 = usage();
    drop(c);
    let _ = server.join();
    emit(
        out,
        json!({
            "exp": "netcost", "size": size, "round_trips": n,
            "cpu_us_per_round_trip": round2((u1.cpu_s - u0.cpu_s) / n as f64 * 1e6),
            "rtt_us": lat.summary(),
        }),
    );
    Ok(())
}
