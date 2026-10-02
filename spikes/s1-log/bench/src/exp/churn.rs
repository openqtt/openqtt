//! Experiment 4. Offline queues filling and draining for minutes, to see compaction or space
//! reclaim surface as write latency spikes.
//!
//! Messages for offline sessions arrive on schedule; each is stored once (`msg/{seq}`) and
//! queued for its session (`q/{cid}/{seq}`), durably. Each session reconnects after a random
//! 5 to 60 seconds and drains: its queue is scanned and range-deleted. Once a second, message
//! bodies below the oldest still-queued sequence number are range-deleted in every partition,
//! as R3's "collected below the lowest session cursor" says.

use std::cmp::Reverse;
use std::collections::{BTreeSet, BinaryHeap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::{Value, json};

use crate::commit::{Committer, Req, Shape};
use crate::engine::{self, Kind, Op, Opts, Space};
use crate::util::{
    Lat, Rng, TAG_MSG, TAG_QUEUE, client_id, dir_size, emit, fresh_dir, key, partition, round2,
    usage,
};

fn q_key(p: u16, cid: &[u8], seq: u64) -> Vec<u8> {
    key(p, TAG_QUEUE, &[cid, &[0], &seq.to_be_bytes()])
}

fn msg_key(p: u16, seq: u64) -> Vec<u8> {
    key(p, TAG_MSG, &[&seq.to_be_bytes()])
}

struct Session {
    cid: Vec<u8>,
    p: u16,
    queued: Vec<u64>,
}

/// Per-second latency histograms, indexed by the second the request was due in.
struct Seconds(Mutex<Vec<Lat>>);

impl Seconds {
    fn new(n: usize) -> Self {
        Seconds(Mutex::new((0..n).map(|_| Lat::default()).collect()))
    }
    fn record(&self, sec: usize, d: Duration) {
        let mut v = self.0.lock().expect("not poisoned");
        if let Some(h) = v.get_mut(sec) {
            h.record(d);
        }
    }
    fn series(&self, q: f64) -> Vec<u64> {
        self.0
            .lock()
            .expect("not poisoned")
            .iter()
            .map(|h| if h.len() > 0 { h.quantile(q) } else { 0 })
            .collect()
    }
    fn total(&self) -> Lat {
        let mut t = Lat::default();
        for h in self.0.lock().expect("not poisoned").iter() {
            t.add(h);
        }
        t
    }
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    data: &Path,
    out: &Path,
    kind: Kind,
    secs: u64,
    rate: u64,
    sessions: u64,
    body: usize,
    min_off_s: u64,
    max_off_s: u64,
) -> Result<()> {
    let dir = fresh_dir(data, &format!("churn-{}", kind.name()));
    let eng = engine::open(kind, &dir, Opts::default())?;
    let committer = Committer::start(eng.clone(), Duration::ZERO, Shape::Combined, 16_384);
    let sender = committer.sender();

    let n_secs = secs as usize + 1;
    let writes = Arc::new(Seconds::new(n_secs));
    let drains = Arc::new(Seconds::new(n_secs));
    let gcs = Arc::new(Seconds::new(n_secs));
    let scans = Seconds::new(n_secs);
    let outstanding = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let end = start + Duration::from_secs(secs);
    let ss: Vec<Session> = (0..sessions)
        .map(|i| {
            let cid = client_id(1_000_000_000 + i);
            let p = partition(&cid);
            Session {
                cid,
                p,
                queued: Vec::new(),
            }
        })
        .collect();
    // Sessions and the set of still-queued sequence numbers, shared by the publisher and the
    // reconnect thread.
    let shared = Arc::new(Mutex::new((ss, BTreeSet::<u64>::new())));
    let published = Arc::new(AtomicU64::new(0));
    let u0 = usage();

    // Publisher: open loop, each message stamped with its own due time.
    let publisher = {
        let shared = shared.clone();
        let sender = sender.clone();
        let writes = writes.clone();
        let outstanding = outstanding.clone();
        let published = published.clone();
        std::thread::spawn(move || {
            let mut rng = Rng::new(42);
            let interval = Duration::from_secs_f64(1.0 / rate as f64);
            let mut next_msg = start;
            let mut seq: u64 = 1;
            while next_msg < end {
                let now = Instant::now();
                while next_msg <= now && next_msg < end {
                    let si = rng.below(sessions) as usize;
                    let (p, qk) = {
                        let mut g = shared.lock().expect("not poisoned");
                        let (ss, live) = &mut *g;
                        let s = &mut ss[si];
                        s.queued.push(seq);
                        live.insert(seq);
                        (s.p, q_key(s.p, &s.cid, seq))
                    };
                    let ops = vec![
                        Op::Put(Space::State, msg_key(p, seq), rng.bytes(body)),
                        Op::Put(Space::State, qk, seq.to_le_bytes().repeat(2)),
                    ];
                    seq += 1;
                    published.fetch_add(1, Ordering::Relaxed);
                    let sec = (next_msg - start).as_secs() as usize;
                    let due_at = next_msg;
                    let w = writes.clone();
                    let o = outstanding.clone();
                    o.fetch_add(1, Ordering::Relaxed);
                    sender.submit(Req {
                        ops,
                        done: Some(Box::new(move |at| {
                            w.record(sec, at - due_at);
                            o.fetch_sub(1, Ordering::Relaxed);
                        })),
                    });
                    next_msg += interval;
                }
                std::thread::sleep(Duration::from_micros(100));
            }
            seq
        })
    };

    // Reconnects, garbage collection of message bodies, and disk samples.
    let mut rng = Rng::new(43);
    let mut due: BinaryHeap<Reverse<(Instant, usize)>> = (0..sessions as usize)
        .map(|i| {
            let at = start + Duration::from_millis(rng.below(max_off_s * 1000));
            Reverse((at, i))
        })
        .collect();
    let mut gc_low: u64 = 0;
    let mut next_gc = start + Duration::from_secs(1);
    let mut next_sample = start;
    let mut disk_series: Vec<Value> = Vec::new();
    let mut drained_entries: u64 = 0;
    let mut drains_n: u64 = 0;
    let mut scan_short: u64 = 0;
    while Instant::now() < end {
        let now = Instant::now();
        while let Some(Reverse((at, si))) = due.peek().copied() {
            if at > now {
                break;
            }
            due.pop();
            let taken = {
                let mut g = shared.lock().expect("not poisoned");
                let (ss, live) = &mut *g;
                let s = &mut ss[si];
                let q = std::mem::take(&mut s.queued);
                for x in &q {
                    live.remove(x);
                }
                (s.p, s.cid.clone(), q)
            };
            let (p, cid, q) = taken;
            if let Some(&last) = q.last() {
                let t0 = Instant::now();
                let mut found = 0u64;
                eng.scan(
                    Space::State,
                    &q_key(p, &cid, 0),
                    &q_key(p, &cid, last + 1),
                    usize::MAX,
                    &mut |_, _| found += 1,
                )?;
                let sec = (t0 - start).as_secs() as usize;
                scans.record(sec, t0.elapsed());
                // Entries still in the committer are not visible yet; count, do not fail.
                if found < q.len() as u64 {
                    scan_short += 1;
                }
                drained_entries += q.len() as u64;
                drains_n += 1;
                let d = drains.clone();
                let o = outstanding.clone();
                o.fetch_add(1, Ordering::Relaxed);
                sender.submit(Req {
                    ops: vec![Op::DelRange(
                        Space::State,
                        q_key(p, &cid, 0),
                        q_key(p, &cid, last + 1),
                    )],
                    done: Some(Box::new(move |at| {
                        d.record(sec, at - t0);
                        o.fetch_sub(1, Ordering::Relaxed);
                    })),
                });
            }
            let off = min_off_s * 1000 + rng.below((max_off_s - min_off_s) * 1000);
            due.push(Reverse((now + Duration::from_millis(off), si)));
        }
        if now >= next_gc {
            next_gc += Duration::from_secs(1);
            let low = {
                let g = shared.lock().expect("not poisoned");
                g.1.first().copied()
            };
            if let Some(low) = low.filter(|&l| l > gc_low) {
                let ops: Vec<Op> = (0..crate::util::PARTITIONS as u16)
                    .map(|p| Op::DelRange(Space::State, msg_key(p, gc_low), msg_key(p, low)))
                    .collect();
                gc_low = low;
                let t0 = Instant::now();
                let sec = (t0 - start).as_secs() as usize;
                let g = gcs.clone();
                let o = outstanding.clone();
                o.fetch_add(1, Ordering::Relaxed);
                sender.submit(Req {
                    ops,
                    done: Some(Box::new(move |at| {
                        g.record(sec, at - t0);
                        o.fetch_sub(1, Ordering::Relaxed);
                    })),
                });
            }
        }
        if now >= next_sample {
            next_sample += Duration::from_secs(10);
            let (apparent, allocated) = dir_size(&dir);
            let live_n = shared.lock().expect("not poisoned").1.len();
            disk_series.push(json!({
                "t": (now - start).as_secs(), "apparent": apparent, "allocated": allocated,
                "live_msgs": live_n, "stats": eng.stats(),
            }));
        }
        std::thread::sleep(Duration::from_micros(200));
    }
    let _ = publisher.join();
    let published = published.load(Ordering::Relaxed);
    let scan_mismatch = scan_short;
    let u1 = usage();
    while outstanding.load(Ordering::Relaxed) > 0 {
        std::thread::sleep(Duration::from_millis(5));
    }
    // The committer's thread ends when its last sender goes.
    drop(sender);
    committer.stop();
    let (apparent, allocated) = dir_size(&dir);
    let w_p99 = writes.series(0.99);
    let w_max = writes.series(1.0);
    let w_p50 = writes.series(0.5);
    let total = writes.total();
    // A second whose p99 is over three times the run's p99 is counted as a stall.
    let run_p99 = total.quantile(0.99).max(1);
    let stalls = w_p99.iter().filter(|&&x| x > 3 * run_p99).count();
    emit(
        out,
        json!({
            "exp": "churn", "engine": kind.name(), "secs": secs, "rate": rate,
            "sessions": sessions, "body": body, "offline_s": [min_off_s, max_off_s],
            "published": published, "drains": drains_n, "drained_entries": drained_entries,
            "scan_mismatch": scan_mismatch,
            "write_us": total.summary(), "drain_us": drains.total().summary(),
            "scan_us": scans.total().summary(), "gc_us": gcs.total().summary(),
            "stall_seconds": stalls,
            "series": { "write_p50": w_p50, "write_p99": w_p99, "write_max": w_max,
                        "drain_p99": drains.series(0.99), "scan_p99": scans.series(0.99) },
            "disk": disk_series,
            "final_disk": { "apparent": apparent, "allocated": allocated },
            "cpu_cores": round2((u1.cpu_s - u0.cpu_s) / secs as f64),
            "disk_written": u1.disk_written.saturating_sub(u0.disk_written),
            "engine_stats": eng.stats(),
        }),
    );
    drop(eng);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
