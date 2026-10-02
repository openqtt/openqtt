//! Group commit: one thread owns the engine's write path and turns whatever requests are waiting
//! into one atomic batch and one fsync. The window is how long it waits after the first request
//! of a batch for more to arrive; at 0 it takes only what queued up during the previous fsync.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::engine::{Engine, Op, Space};
use crate::util::Lat;

/// How a batch's log entries and state-machine writes reach the engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Shape {
    /// Everything in one atomic write and one fsync.
    Combined,
    /// The log entries with an fsync, then the state writes with a second fsync.
    SplitSync,
    /// The log entries with an fsync, then the state writes without one: they become durable
    /// with the next log fsync (one shared journal or file), and a crash replays them from the
    /// log anyway.
    SplitLazy,
}

pub type DoneFn = Box<dyn FnOnce(Instant) + Send>;

pub struct Req {
    pub ops: Vec<Op>,
    /// Called on the committer thread with the instant the batch became durable.
    pub done: Option<DoneFn>,
}

#[derive(Default)]
pub struct CommitStats {
    pub batches: u64,
    pub requests: u64,
    pub ops: u64,
    pub logical_bytes: u64,
    /// Time inside engine writes per batch (the fsync and whatever the engine does with it).
    pub write: Lat,
    pub batch_sizes: Vec<u32>,
}

impl CommitStats {
    pub fn summary(&self) -> Value {
        let mut sizes = self.batch_sizes.clone();
        sizes.sort_unstable();
        let q = |p: f64| {
            if sizes.is_empty() {
                0
            } else {
                sizes[((sizes.len() - 1) as f64 * p) as usize]
            }
        };
        json!({
            "batches": self.batches,
            "requests": self.requests,
            "mean_batch": if self.batches > 0 { self.requests as f64 / self.batches as f64 } else { 0.0 },
            "batch_p50": q(0.5),
            "batch_p99": q(0.99),
            "batch_max": q(1.0),
            "engine_write_us": self.write.summary(),
            "logical_bytes": self.logical_bytes,
        })
    }
}

pub struct Committer {
    tx: Option<mpsc::Sender<Req>>,
    handle: Option<JoinHandle<()>>,
    pub stats: Arc<Mutex<CommitStats>>,
    pub queued: Arc<AtomicU64>,
}

impl Committer {
    pub fn start(engine: Arc<dyn Engine>, window: Duration, shape: Shape, max_batch: usize) -> Self {
        let (tx, rx) = mpsc::channel::<Req>();
        let stats = Arc::new(Mutex::new(CommitStats::default()));
        let queued = Arc::new(AtomicU64::new(0));
        let st = stats.clone();
        let q = queued.clone();
        let handle = std::thread::Builder::new()
            .name("committer".into())
            .spawn(move || run(engine, rx, window, shape, max_batch, st, q))
            .expect("spawn committer");
        Committer {
            tx: Some(tx),
            handle: Some(handle),
            stats,
            queued,
        }
    }

    pub fn sender(&self) -> CommitSender {
        CommitSender {
            tx: self.tx.clone().expect("running"),
            queued: self.queued.clone(),
        }
    }

    pub fn reset_stats(&self) {
        *self.stats.lock().expect("not poisoned") = CommitStats::default();
    }

    pub fn stop(mut self) {
        self.tx.take();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

#[derive(Clone)]
pub struct CommitSender {
    tx: mpsc::Sender<Req>,
    queued: Arc<AtomicU64>,
}

impl CommitSender {
    pub fn submit(&self, req: Req) {
        self.queued.fetch_add(1, Ordering::Relaxed);
        let _ = self.tx.send(req);
    }
}

fn run(
    engine: Arc<dyn Engine>,
    rx: mpsc::Receiver<Req>,
    window: Duration,
    shape: Shape,
    max_batch: usize,
    stats: Arc<Mutex<CommitStats>>,
    queued: Arc<AtomicU64>,
) {
    let mut batch: Vec<Req> = Vec::with_capacity(max_batch);
    loop {
        let Ok(first) = rx.recv() else { return };
        let opened = Instant::now();
        batch.push(first);
        if !window.is_zero() {
            let deadline = opened + window;
            while batch.len() < max_batch {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                match rx.recv_timeout(deadline - now) {
                    Ok(r) => batch.push(r),
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        }
        while batch.len() < max_batch {
            match rx.try_recv() {
                Ok(r) => batch.push(r),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        queued.fetch_sub(batch.len() as u64, Ordering::Relaxed);

        let mut log_ops = Vec::new();
        let mut state_ops = Vec::new();
        let mut logical = 0;
        for r in &mut batch {
            for op in r.ops.drain(..) {
                logical += op.logical_bytes();
                if shape != Shape::Combined && op.space() == Space::State {
                    state_ops.push(op);
                } else {
                    log_ops.push(op);
                }
            }
        }
        let n_ops = (log_ops.len() + state_ops.len()) as u64;
        let t = Instant::now();
        let res = engine.write(&log_ops, true);
        let durable = Instant::now();
        if let Err(e) = res {
            eprintln!("committer: write failed: {e:#}");
            std::process::exit(2);
        }
        // Split shapes apply the state after the log is durable, as Raft applies after commit.
        if !state_ops.is_empty() {
            let res = engine.write(&state_ops, shape == Shape::SplitSync);
            if let Err(e) = res {
                eprintln!("committer: state write failed: {e:#}");
                std::process::exit(2);
            }
        }
        let applied = Instant::now();
        {
            let mut s = stats.lock().expect("not poisoned");
            s.batches += 1;
            s.requests += batch.len() as u64;
            s.ops += n_ops;
            s.logical_bytes += logical;
            s.write.record(applied - t);
            s.batch_sizes.push(batch.len() as u32);
        }
        // A request is done when its log entry is durable and its state write is applied.
        let _ = durable;
        for r in batch.drain(..) {
            if let Some(done) = r.done {
                done(applied);
            }
        }
    }
}
