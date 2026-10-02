//! The simulated pieces both replication schemes run on: a delay line for network hops and
//! modelled flushes, and a per-node disk that group-commits whatever is waiting.
//!
//! tokio's timer wheel ticks in whole milliseconds, so `tokio::time::sleep(1 ms)` sleeps 1 to
//! 2 ms. The delay line is one OS thread with a heap of deadlines; it fires them with the
//! precision of a thread wakeup (tens of microseconds) and records how late each one was.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use crate::util::{Lat, thread_cpu_s};

struct Timer {
    at: Instant,
    seq: u64,
    tx: oneshot::Sender<()>,
}

impl PartialEq for Timer {
    fn eq(&self, o: &Self) -> bool {
        (self.at, self.seq) == (o.at, o.seq)
    }
}
impl Eq for Timer {}
impl PartialOrd for Timer {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Timer {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        (self.at, self.seq).cmp(&(o.at, o.seq))
    }
}

#[derive(Clone)]
pub struct DelayLine {
    tx: mpsc::Sender<(Instant, oneshot::Sender<()>)>,
    pub lateness: Arc<Mutex<Lat>>,
    /// CPU seconds the delay thread has used, as f64 bits, so it can be subtracted from the
    /// process total.
    pub cpu_bits: Arc<AtomicU64>,
}

impl DelayLine {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel::<(Instant, oneshot::Sender<()>)>();
        let lateness = Arc::new(Mutex::new(Lat::default()));
        let cpu_bits = Arc::new(AtomicU64::new(0));
        let (l, c) = (lateness.clone(), cpu_bits.clone());
        std::thread::Builder::new()
            .name("delay-line".into())
            .spawn(move || run(rx, l, c))
            .expect("spawn delay line");
        DelayLine {
            tx,
            lateness,
            cpu_bits,
        }
    }

    /// Completes `d` from now. Zero completes at once without touching the thread.
    pub fn sleep(&self, d: Duration) -> impl Future<Output = ()> + Send + 'static {
        let rx = if d.is_zero() {
            None
        } else {
            let (tx, rx) = oneshot::channel();
            let _ = self.tx.send((Instant::now() + d, tx));
            Some(rx)
        };
        async move {
            if let Some(rx) = rx {
                let _ = rx.await;
            }
        }
    }

    /// Completes at `at`, or at once if that has passed.
    pub fn sleep_until(&self, at: Instant) -> impl Future<Output = ()> + Send + 'static {
        let rx = if at <= Instant::now() {
            None
        } else {
            let (tx, rx) = oneshot::channel();
            let _ = self.tx.send((at, tx));
            Some(rx)
        };
        async move {
            if let Some(rx) = rx {
                let _ = rx.await;
            }
        }
    }

    pub fn cpu_s(&self) -> f64 {
        f64::from_bits(self.cpu_bits.load(Ordering::Relaxed))
    }
}

/// macOS stretches a thread's timed waits to coalesce wakeups, by about a quarter of the wait
/// for an ordinary thread: a 1 ms delay fired 250 us late, and only when nothing else woke the
/// thread first, which favoured protocols that send many messages. The user-interactive class
/// gets the least slack.
#[cfg(target_os = "macos")]
fn raise_timer_precision() {
    // SAFETY: sets the calling thread's own scheduling class; no pointers involved.
    unsafe {
        libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE, 0);
    }
}

#[cfg(not(target_os = "macos"))]
fn raise_timer_precision() {}

/// The last stretch before a deadline is spun rather than slept, so a hop is late by
/// microseconds whatever else the thread is doing.
const SPIN: Duration = Duration::from_micros(150);

fn run(
    rx: mpsc::Receiver<(Instant, oneshot::Sender<()>)>,
    lateness: Arc<Mutex<Lat>>,
    cpu_bits: Arc<AtomicU64>,
) {
    raise_timer_precision();
    let mut heap: BinaryHeap<Reverse<Timer>> = BinaryHeap::new();
    let mut seq = 0u64;
    let mut late = Lat::default();
    let mut last_report = Instant::now();
    loop {
        let wait = heap
            .peek()
            .map(|Reverse(t)| t.at.saturating_duration_since(Instant::now()));
        let got = match wait {
            None => rx.recv().map_err(|_| mpsc::RecvTimeoutError::Disconnected),
            Some(d) if d.is_zero() => Err(mpsc::RecvTimeoutError::Timeout),
            Some(d) if d <= SPIN => {
                std::hint::spin_loop();
                rx.try_recv().map_err(|e| match e {
                    mpsc::TryRecvError::Empty => mpsc::RecvTimeoutError::Timeout,
                    mpsc::TryRecvError::Disconnected => mpsc::RecvTimeoutError::Disconnected,
                })
            }
            Some(d) => rx.recv_timeout(d - SPIN),
        };
        match got {
            Ok((at, tx)) => {
                heap.push(Reverse(Timer { at, seq, tx }));
                seq += 1;
                while let Ok((at, tx)) = rx.try_recv() {
                    heap.push(Reverse(Timer { at, seq, tx }));
                    seq += 1;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }
        let now = Instant::now();
        while heap.peek().is_some_and(|Reverse(t)| t.at <= now) {
            let Reverse(t) = heap.pop().expect("peeked");
            late.record(now - t.at);
            let _ = t.tx.send(());
        }
        if now - last_report > Duration::from_millis(200) {
            last_report = now;
            cpu_bits.store(thread_cpu_s().to_bits(), Ordering::Relaxed);
            lateness.lock().expect("not poisoned").add(&late);
            late.reset();
        }
    }
}

type Done = Box<dyn FnOnce() + Send>;

/// One node's disk: callers queue work that must be durable, a task takes everything queued,
/// "flushes" it in one go (a modelled fsync of `flush`), then completes all of it in order. All
/// the groups on a node share it, as they would share one engine and one journal.
#[derive(Clone)]
pub struct Disk {
    tx: tokio::sync::mpsc::UnboundedSender<Done>,
    pub flushes: Arc<AtomicU64>,
    pub items: Arc<AtomicU64>,
}

impl Disk {
    pub fn new(line: DelayLine, flush: Duration) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Done>();
        let flushes = Arc::new(AtomicU64::new(0));
        let items = Arc::new(AtomicU64::new(0));
        let (f, it) = (flushes.clone(), items.clone());
        tokio::spawn(async move {
            let mut batch: Vec<Done> = Vec::new();
            while let Some(first) = rx.recv().await {
                batch.push(first);
                while let Ok(x) = rx.try_recv() {
                    batch.push(x);
                }
                line.sleep(flush).await;
                f.fetch_add(1, Ordering::Relaxed);
                it.fetch_add(batch.len() as u64, Ordering::Relaxed);
                for d in batch.drain(..) {
                    d();
                }
            }
        });
        Disk { tx, flushes, items }
    }

    pub fn submit(&self, done: impl FnOnce() + Send + 'static) {
        let _ = self.tx.send(Box::new(done));
    }

    /// Waits until everything queued so far is durable.
    pub async fn barrier(&self) {
        let (tx, rx) = oneshot::channel();
        self.submit(move || {
            let _ = tx.send(());
        });
        let _ = rx.await;
    }
}
