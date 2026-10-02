//! Experiment 2. The session-claim workload of a reconnect storm, on one node's engine.
//!
//! Every client that reconnects makes its partition leader read `own/{cid}`, bump the connection
//! generation and commit the new owner. Here the sessions already exist (a storm is the fleet
//! coming back), claims arrive on an open-loop schedule, apply workers stand in for partition
//! state machines (a client id always lands on the same worker), and every claim is a Raft entry
//! plus the `own` write, durable through group commit before it counts.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::json;

use crate::commit::{Committer, Req, Shape};
use crate::engine::{self, Engine, Kind, Op, Opts, Space};
use crate::exp::Window;
use crate::exp::write::merge;
use crate::util::{
    Lat, Rng, TAG_OWN, TAG_SESS, client_id, dir_size, emit, fresh_dir, key, log_key, own_conn_gen,
    own_value, partition, round2, sess_value, usage,
};

/// Writes `n` sessions (`own` and `sess`) in client-id order, which is random key order, in
/// batches of 10,000 sessions with a sync every ten batches. Without the syncs redb keeps every
/// non-durable commit's pages until the end, and its load slows by orders of magnitude.
pub fn preload(eng: &dyn Engine, n: u64) -> Result<()> {
    let mut ops = Vec::with_capacity(20_000);
    let mut batches = 0u64;
    for i in 0..n {
        let cid = client_id(i);
        let p = partition(&cid);
        ops.push(Op::Put(
            Space::State,
            key(p, TAG_OWN, &[&cid]),
            own_value((i % 3) as u32, 1, 1),
        ));
        ops.push(Op::Put(
            Space::State,
            key(p, TAG_SESS, &[&cid]),
            sess_value(&cid),
        ));
        if ops.len() >= 20_000 {
            batches += 1;
            eng.write(&ops, batches % 10 == 0)?;
            ops.clear();
        }
    }
    eng.write(&ops, true)?;
    Ok(())
}

/// A claim as the Raft entry would carry it: client id, new owner and epoch.
fn claim_entry(cid: &[u8], node: u32, epoch: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(cid.len() + 16);
    v.extend_from_slice(&(cid.len() as u16).to_be_bytes());
    v.extend_from_slice(cid);
    v.extend_from_slice(&node.to_le_bytes());
    v.extend_from_slice(&epoch.to_le_bytes());
    v
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    data: &Path,
    out: &Path,
    engines: &[Kind],
    n: u64,
    rates: &[u64],
    workers: usize,
    warmup: Duration,
    measure: Duration,
    cache_mb: u64,
) -> Result<()> {
    for &kind in engines {
        let dir = fresh_dir(data, &format!("claims-{}", kind.name()));
        let opts = Opts {
            cache_bytes: cache_mb << 20,
            ..Opts::default()
        };
        let eng = engine::open(kind, &dir, opts)?;
        let t = Instant::now();
        preload(eng.as_ref(), n)?;
        eng.flush()?;
        let preload_s = t.elapsed().as_secs_f64();
        let (apparent, allocated) = dir_size(&dir);
        emit(
            out,
            json!({
                "exp": "claims-preload", "engine": kind.name(), "sessions": n,
                "secs": round2(preload_s), "disk_apparent": apparent, "disk_allocated": allocated,
                "engine_stats": eng.stats(),
            }),
        );
        for &rate in rates {
            let c = Committer::start(eng.clone(), Duration::ZERO, Shape::Combined, 16_384);
            let mut rec = storm(&eng, &c, n, rate, workers, warmup, measure);
            c.stop();
            let backlog = rec["outstanding_at_end"].as_u64().unwrap_or(0);
            merge(
                &mut rec,
                json!({
                    "exp": "claims", "engine": kind.name(), "sessions": n, "rate": rate,
                    "workers": workers, "cache_mb": cache_mb,
                }),
            );
            emit(out, rec);
            // Two seconds of backlog means the engine is past saturation; higher rates only
            // take longer to drain.
            if backlog > 2 * rate {
                break;
            }
        }
        drop(eng);
        let _ = std::fs::remove_dir_all(&dir);
    }
    Ok(())
}

fn storm(
    eng: &Arc<dyn Engine>,
    c: &Committer,
    n: u64,
    rate: u64,
    workers: usize,
    warmup: Duration,
    measure: Duration,
) -> serde_json::Value {
    let start = Instant::now();
    let from = start + warmup;
    let until = from + measure;
    let window = Arc::new(Window::new(from, measure));
    let log_index = Arc::new(AtomicU64::new(0));
    let reads = Arc::new(Mutex::new(Lat::default()));
    let mut txs = Vec::new();
    let mut hs = Vec::new();
    for _ in 0..workers {
        let (tx, rx) = mpsc::channel::<(Instant, u64)>();
        txs.push(tx);
        let eng = eng.clone();
        let sender = c.sender();
        let window = window.clone();
        let log_index = log_index.clone();
        let reads = reads.clone();
        hs.push(std::thread::spawn(move || {
            let mut read_lat = Lat::default();
            while let Ok((due, i)) = rx.recv() {
                let cid = client_id(i);
                let p = partition(&cid);
                let own_key = key(p, TAG_OWN, &[&cid]);
                let r0 = Instant::now();
                let cur = eng.get(Space::State, &own_key).expect("read own");
                if due >= window.from && due < window.until {
                    read_lat.record(r0.elapsed());
                }
                let generation = cur.as_deref().map(own_conn_gen).unwrap_or(0) + 1;
                let idx = log_index.fetch_add(1, Ordering::Relaxed);
                let node = (i % 3) as u32;
                let ops = vec![
                    Op::Put(
                        Space::Log,
                        log_key(u32::from(p), idx),
                        claim_entry(&cid, node, 2),
                    ),
                    Op::Put(Space::State, own_key, own_value(node, 2, generation)),
                ];
                let w = window.clone();
                sender.submit(Req {
                    ops,
                    done: Some(Box::new(move |at| w.done(due, at, true))),
                });
            }
            reads.lock().expect("not poisoned").add(&read_lat);
        }));
    }

    // Open loop: every 100 us, hand out everything that has come due.
    let mut rng = Rng::new(rate);
    let interval = Duration::from_secs_f64(1.0 / rate as f64);
    let mut next = start;
    let mut u0 = None;
    while next < until {
        let now = Instant::now();
        if u0.is_none() && now >= from {
            u0 = Some(usage());
            c.reset_stats();
        }
        while next <= now && next < until {
            let i = rng.below(n);
            let w = usize::from(partition(&client_id(i))) % workers;
            window.offered(next);
            let _ = txs[w].send((next, i));
            next += interval;
        }
        std::thread::sleep(Duration::from_micros(100));
    }
    crate::util::sleep_until(until);
    let u1 = usage();
    let u0 = u0.unwrap_or(u1);
    let commit = c.stats.lock().expect("not poisoned").summary();
    window.close();
    drop(txs);
    for h in hs {
        let _ = h.join();
    }
    // Every claim offered in the window counts, so wait for the slowest of them.
    window.drain(Duration::from_secs(300));
    let secs = measure.as_secs_f64();
    let mut rec = window.json();
    merge(
        &mut rec,
        json!({
            "read_us": reads.lock().expect("not poisoned").summary(),
            "cpu_cores": round2((u1.cpu_s - u0.cpu_s) / secs),
            "disk_written": u1.disk_written.saturating_sub(u0.disk_written),
            "commit": commit,
            "engine_stats": eng.stats(),
        }),
    );
    rec
}
