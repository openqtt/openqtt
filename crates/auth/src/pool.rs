//! A few threads for work too slow to run on an async executor: checking a password costs tens
//! of milliseconds of CPU, and a task that spends them stalls every connection sharing its
//! thread.
//!
//! The queue is bounded, so a burst of CONNECTs, or a flood of wrong passwords, waits for a
//! thread or is turned away at once, and never takes more than the threads given.

use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};
use std::thread;

use crate::Error;

type Job = Box<dyn FnOnce() + Send>;

/// Threads that run jobs from a bounded queue. Dropping the pool lets each thread finish the
/// job it has and stop.
pub(crate) struct Pool {
    queue: SyncSender<Job>,
}

/// The queue is full: every thread is busy and `queue` jobs are waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Busy;

impl Pool {
    /// `threads` threads named `name`, and room for `queue` jobs waiting for them.
    ///
    /// # Errors
    ///
    /// [`Error::Threads`] when not one thread could start.
    pub(crate) fn new(name: &str, threads: NonZeroUsize, queue: usize) -> Result<Self, Error> {
        let (sender, receiver) = mpsc::sync_channel::<Job>(queue);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut started = 0;
        let mut failure = None;
        for _ in 0..threads.get() {
            let receiver = Arc::clone(&receiver);
            let spawned = thread::Builder::new().name(name.to_owned()).spawn(move || {
                loop {
                    // The lock is held only while waiting, so one thread waits on the
                    // queue at a time and the others run their jobs.
                    let job = receiver
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .recv();
                    match job {
                        Ok(job) => job(),
                        Err(_) => return,
                    }
                }
            });
            // A thread the system cannot start is one fewer: the queue drains as long as one
            // started, and `run` reports Busy once it is full.
            match spawned {
                Ok(_) => started += 1,
                Err(error) => failure = Some(error.to_string()),
            }
        }
        if started == 0 {
            return Err(Error::Threads {
                reason: failure.unwrap_or_default(),
            });
        }
        Ok(Self { queue: sender })
    }

    /// Runs `work` on a thread of the pool, and returns what it returns, or `None` if it
    /// panicked.
    ///
    /// # Errors
    ///
    /// [`Busy`] when the queue is full.
    pub(crate) fn run<T, F>(&self, work: F) -> Result<Pending<T>, Busy>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let slot = Arc::new(Slot {
            state: Mutex::new(State::Waiting(None)),
        });
        let filled = Arc::clone(&slot);
        let job: Job = Box::new(move || {
            let output = catch_unwind(AssertUnwindSafe(work)).ok();
            filled.fill(output);
        });
        match self.queue.try_send(job) {
            Ok(()) => Ok(Pending { slot }),
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => Err(Busy),
        }
    }
}

struct Slot<T> {
    state: Mutex<State<T>>,
}

enum State<T> {
    Waiting(Option<Waker>),
    Done(Option<T>),
    Taken,
}

impl<T> Slot<T> {
    fn fill(&self, output: Option<T>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let State::Waiting(waker) = std::mem::replace(&mut *state, State::Done(output)) {
            drop(state);
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

/// The output of a job, once a thread has run it: `None` if the job panicked.
pub(crate) struct Pending<T> {
    slot: Arc<Slot<T>>,
}

impl<T> Future for Pending<T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut state = self
            .slot
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match std::mem::replace(&mut *state, State::Taken) {
            State::Done(output) => Poll::Ready(output),
            State::Waiting(_) => {
                *state = State::Waiting(Some(cx.waker().clone()));
                Poll::Pending
            }
            // Polled again after it was ready, which a future's caller must not do.
            State::Taken => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Wake;

    use super::*;

    /// Wakes by setting a flag, so a test can wait for a future without an executor.
    struct Flag(AtomicBool);

    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Runs a future to completion on this thread, spinning until it is woken.
    pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
        let flag = Arc::new(Flag(AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&flag));
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
            while !flag.0.swap(false, Ordering::SeqCst) {
                thread::yield_now();
            }
        }
    }

    #[test]
    fn a_job_runs_on_the_pool_and_its_output_comes_back() {
        let pool = Pool::new("test", NonZeroUsize::new(2).unwrap(), 4).unwrap();
        let pending = pool.run(|| 6 * 7).unwrap();
        assert_eq!(block_on(pending), Some(42));
        let panicked = pool.run(|| -> u32 { panic!("a job that fails") }).unwrap();
        assert_eq!(block_on(panicked), None);
        // The thread survived the panic.
        assert_eq!(
            block_on(pool.run(|| "still here").unwrap()),
            Some("still here")
        );
    }

    #[test]
    fn a_full_queue_is_busy() {
        let pool = Pool::new("test", NonZeroUsize::new(1).unwrap(), 1).unwrap();
        let started = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let (s, r) = (Arc::clone(&started), Arc::clone(&release));
        let first = pool
            .run(move || {
                s.wait();
                r.wait();
            })
            .unwrap();
        // The thread holds the first job; the queue holds one more, and refuses a third.
        started.wait();
        let second = pool.run(|| ()).unwrap();
        assert!(matches!(pool.run(|| ()), Err(Busy)));
        release.wait();
        assert_eq!(block_on(first), Some(()));
        assert_eq!(block_on(second), Some(()));
    }
}
