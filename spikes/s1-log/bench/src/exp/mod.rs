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
    /// The window's counts and latency, as `Window::json` writes them.
    pub window: Value,
    pub measure_s: f64,
    pub cpu_s: f64,
    pub disk_written: u64,
    pub commit: Value,
}

impl Drive {
    pub fn json(&self) -> Value {
        let mut v = self.window.clone();
        if let Value::Object(m) = &mut v {
            m.insert(
                "cpu_cores".into(),
                json!(round2(self.cpu_s / self.measure_s)),
            );
            m.insert("disk_written".into(), json!(self.disk_written));
            m.insert("commit".into(), self.commit.clone());
        }
        v
    }
}

/// What a measurement window saw, counted the same way by every experiment that offers load.
///
/// Throughput counts requests that completed inside the window, whenever they were offered, so
/// work left over from the warmup counts against the capacity it used. Latency covers every
/// request offered inside the window, through the drain after it, so the slowest requests of a
/// saturated run are kept rather than cut off at the window's end.
pub struct Window {
    pub from: Instant,
    pub until: Instant,
    pub measure_s: f64,
    lat: Mutex<Lat>,
    completed: AtomicU64,
    offered: AtomicU64,
    failed: AtomicU64,
    outstanding: AtomicU64,
    outstanding_at_end: AtomicU64,
    drain_s: Mutex<f64>,
}

impl Window {
    pub fn new(from: Instant, measure: Duration) -> Self {
        Window {
            from,
            until: from + measure,
            measure_s: measure.as_secs_f64(),
            lat: Mutex::new(Lat::default()),
            completed: AtomicU64::new(0),
            offered: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            outstanding: AtomicU64::new(0),
            outstanding_at_end: AtomicU64::new(0),
            drain_s: Mutex::new(0.0),
        }
    }

    /// A request due at `due` was handed to the system under test.
    pub fn offered(&self, due: Instant) {
        self.outstanding.fetch_add(1, Ordering::Relaxed);
        if due >= self.from && due < self.until {
            self.offered.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A request due at `due` finished at `at`. `ok` is false for a request that failed, which
    /// counts as neither throughput nor latency.
    pub fn done(&self, due: Instant, at: Instant, ok: bool) {
        let in_window = due >= self.from && due < self.until;
        if ok {
            if at >= self.from && at <= self.until {
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
            if in_window {
                self.lat.lock().expect("not poisoned").record(at - due);
            }
        } else if in_window {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
        self.outstanding.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn outstanding(&self) -> u64 {
        self.outstanding.load(Ordering::Relaxed)
    }

    /// Call when the window closes: notes what is still in flight.
    pub fn close(&self) {
        self.outstanding_at_end
            .store(self.outstanding(), Ordering::Relaxed);
    }

    /// Waits for every request to finish, at most `cap`, and records how long that took.
    pub fn drain(&self, cap: Duration) {
        let t = Instant::now();
        while self.outstanding() > 0 && t.elapsed() < cap {
            std::thread::sleep(Duration::from_millis(1));
        }
        *self.drain_s.lock().expect("not poisoned") = t.elapsed().as_secs_f64();
    }

    /// The same as `drain`, for async callers.
    pub async fn drain_async(&self, cap: Duration) {
        let t = Instant::now();
        while self.outstanding() > 0 && t.elapsed() < cap {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        *self.drain_s.lock().expect("not poisoned") = t.elapsed().as_secs_f64();
    }

    pub fn completed(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    pub fn json(&self) -> Value {
        let lat = self.lat.lock().expect("not poisoned");
        let offered = self.offered.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        json!({
            "lat_us": lat.summary(),
            "completed": self.completed(),
            "per_s": round2(self.completed() as f64 / self.measure_s),
            "offered_in_window": offered,
            "failed_in_window": failed,
            // Offered in the window but never finished, not even during the drain; their
            // latency is missing from lat_us.
            "unfinished": offered.saturating_sub(lat.len()).saturating_sub(failed),
            "outstanding_at_end": self.outstanding_at_end.load(Ordering::Relaxed),
            "drain_s": round2(*self.drain_s.lock().expect("not poisoned")),
        })
    }
}

struct Shared {
    sender: CommitSender,
    make: MakeOps,
    seq: AtomicU64,
    window: Window,
}

impl Shared {
    fn submit(self: &Arc<Self>, due: Instant, resubmit: bool) {
        let i = self.seq.fetch_add(1, Ordering::Relaxed);
        let ops = (self.make)(i);
        let me = self.clone();
        self.window.offered(due);
        self.sender.submit(Req {
            ops,
            done: Some(Box::new(move |at| me.done(due, at, resubmit))),
        });
    }

    fn done(self: &Arc<Self>, due: Instant, at: Instant, resubmit: bool) {
        self.window.done(due, at, true);
        if resubmit {
            let now = Instant::now();
            if now < self.window.until {
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
        window: Window::new(from, measure),
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
            return finish(committer, &shared, u0);
        }
    }
    let u0 = usage();
    committer.reset_stats();
    sleep_until(until);
    finish(committer, &shared, u0)
}

fn finish(committer: &Committer, shared: &Arc<Shared>, u0: crate::util::Usage) -> Drive {
    let u1 = usage();
    let commit = committer.stats.lock().expect("not poisoned").summary();
    shared.window.close();
    // Everything offered in the window counts, so wait for it; the cap only guards a run that
    // would never finish.
    shared.window.drain(Duration::from_secs(300));
    Drive {
        window: shared.window.json(),
        measure_s: shared.window.measure_s,
        cpu_s: u1.cpu_s - u0.cpu_s,
        disk_written: u1.disk_written.saturating_sub(u0.disk_written),
        commit,
    }
}
