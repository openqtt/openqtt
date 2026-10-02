//! A global allocator that counts live heap bytes and allocations.
//!
//! Memory per filter is read from here rather than from the resident set: the count is exact,
//! independent of the platform's allocator and of memory compression, and starts from zero at
//! any point of the run. It counts requested sizes, so allocator rounding is not in it; the
//! allocation count is reported beside it so that overhead can be estimated.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Wraps the system allocator; installed as the global allocator in `main`.
pub struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);

fn grew(by: usize) {
    LIVE.fetch_add(by, Relaxed);
}

// SAFETY: every method forwards to `System` with the caller's arguments unchanged and only
// updates counters around the call, so `System`'s guarantees hold.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            grew(layout.size());
            ALLOCS.fetch_add(1, Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            grew(layout.size());
            ALLOCS.fetch_add(1, Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Relaxed);
        ALLOCS.fetch_sub(1, Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            if new_size >= layout.size() {
                grew(new_size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - new_size, Relaxed);
            }
        }
        p
    }
}

/// Live heap bytes and live allocations at one moment.
#[derive(Clone, Copy, Debug)]
pub struct Heap {
    pub bytes: usize,
    pub allocs: usize,
}

pub fn now() -> Heap {
    Heap {
        bytes: LIVE.load(Relaxed),
        allocs: ALLOCS.load(Relaxed),
    }
}
