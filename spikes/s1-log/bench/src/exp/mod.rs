//! The experiments, and the two ways they offer load: closed loop (a fixed number of requests
//! always outstanding, which finds throughput) and open loop (requests on a fixed schedule,
//! latency measured from when each was due, so a stall cannot hide behind fewer requests).

pub mod churn;
pub mod claims;
pub mod footprint;
pub mod fsync;
pub mod recovery;
pub mod write;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::commit::{CommitSender, Committer, Req};
use crate::engine::Op;
use crate::util::{Lat, round2, sleep_until, usage};

#[derive(Clone, Copy, Debug)]
pub enum Load {
    /// This many requests outstanding at all times.
    Closed(u64),
    /// This many requests per second, on schedule.
    Open(u64),
}

impl Load {
    pub fn parse(s: &str) -> Load {
        let (kind, n) = s.split_at(1);
        let n: u64 = n.parse().expect("load is c<N> or o<N>");
        match kind {
            "c" => Load::Closed(n),
            "o" => Load::Open(n),
            _ => panic!("load is c<N> or o<N>, got {s}"),
        }
    }

    pub fn label(&self) -> String {
        match self {
            Load::Closed(n) => format!("closed:{n}"),
            Load::Open(n) => format!("open:{n}/s"),
        }
    }
}

pub type MakeOps = Arc<dyn Fn(u64) -> Vec<Op> + Send + Sync>;

pub struct Drive {
    pub lat: Lat,
    pub completed: u64,
    pub measure_s: f64,
    pub cpu_s: f64,
    pub disk_written: u64,
    pub commit: Value,
    /// Requests still queued when the measurement window closed.
    pub backlog: u64,
    /// Seconds after the window closed until every request had finished.
    pub drain_s: f64,
}

impl Drive {
    pub fn json(&self) -> Value {
        json!({
            "lat_us": self.lat.summary(),
            "completed": self.completed,
            "per_s": round2(self.completed as f64 / self.measure_s),
            "cpu_cores": round2(self.cpu_s / self.measure_s),
            "disk_written": self.disk_written,
            "commit": self.commit,
            "backlog_at_end": self.backlog,
            "drain_s": round2(self.drain_s),
        })
    }
}

struct Shared {
    sender: CommitSender,
    make: MakeOps,
    seq: AtomicU64,
    from: Instant,
    until: Instant,
    lat: Mutex<Lat>,
    completed: AtomicU64,
    outstanding: AtomicU64,
}

impl Shared {
    fn submit(self: &Arc<Self>, due: Instant, resubmit: bool) {
        let i = self.seq.fetch_add(1, Ordering::Relaxed);
        let ops = (self.make)(i);
        let me = self.clone();
        self.outstanding.fetch_add(1, Ordering::Relaxed);
        self.sender.submit(Req {
            ops,
            done: Some(Box::new(move |at| me.done(due, at, resubmit))),
        });
    }

    fn done(self: &Arc<Self>, due: Instant, at: Instant, resubmit: bool) {
        if due >= self.from && at <= self.until {
            self.lat.lock().expect("not poisoned").record(at - due);
            self.completed.fetch_add(1, Ordering::Relaxed);
        }
        self.outstanding.fetch_sub(1, Ordering::Relaxed);
        if resubmit {
            let now = Instant::now();
            if now < self.until {
                self.submit(now, true);
            }
        }
    }
}

/// Offers `load` to the committer for `warmup` then `measure`, and reports what it saw during
/// `measure` only.
pub fn drive(
    committer: &Committer,
    load: Load,
    warmup: Duration,
    measure: Duration,
    make: MakeOps,
) -> Drive {
    let start = Instant::now();
    let from = start + warmup;
    let until = from + measure;
    let shared = Arc::new(Shared {
        sender: committer.sender(),
        make,
        seq: AtomicU64::new(0),
        from,
        until,
        lat: Mutex::new(Lat::default()),
        completed: AtomicU64::new(0),
        outstanding: AtomicU64::new(0),
    });

    match load {
        Load::Closed(n) => {
            for _ in 0..n {
                shared.submit(Instant::now(), true);
            }
            sleep_until(from);
        }
        Load::Open(rate) => {
            let interval = Duration::from_secs_f64(1.0 / rate as f64);
            let mut next = start;
            // Wake every 100 us and send everything that has come due, each stamped with its
            // own due time.
            while next < from {
                let now = Instant::now();
                while next <= now && next < until {
                    shared.submit(next, false);
                    next += interval;
                }
                std::thread::sleep(Duration::from_micros(100));
            }
            // Fall through to the measured part below with the generator still running.
            let u0 = usage();
            committer.reset_stats();
            while next < until {
                let now = Instant::now();
                while next <= now && next < until {
                    shared.submit(next, false);
                    next += interval;
                }
                std::thread::sleep(Duration::from_micros(100));
            }
            sleep_until(until);
            return finish(committer, &shared, u0, until, measure);
        }
    }
    let u0 = usage();
    committer.reset_stats();
    sleep_until(until);
    finish(committer, &shared, u0, until, measure)
}

fn finish(
    committer: &Committer,
    shared: &Arc<Shared>,
    u0: crate::util::Usage,
    until: Instant,
    measure: Duration,
) -> Drive {
    let u1 = usage();
    let commit = committer.stats.lock().expect("not poisoned").summary();
    let backlog = committer.queued.load(Ordering::Relaxed);
    // Let whatever is in flight finish so the next run starts clean.
    let wait_from = Instant::now();
    while shared.outstanding.load(Ordering::Relaxed) > 0 {
        if wait_from.elapsed() > Duration::from_secs(120) {
            eprintln!("drive: requests still outstanding after 120 s");
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let lat = std::mem::take(&mut *shared.lat.lock().expect("not poisoned"));
    Drive {
        lat,
        completed: shared.completed.load(Ordering::Relaxed),
        measure_s: measure.as_secs_f64(),
        cpu_s: u1.cpu_s - u0.cpu_s,
        disk_written: u1.disk_written.saturating_sub(u0.disk_written),
        commit,
        backlog,
        drain_s: (Instant::now().max(until) - until).as_secs_f64(),
    }
}
