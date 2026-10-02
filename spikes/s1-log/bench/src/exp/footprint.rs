//! Experiment 3. Bytes on disk and resident memory per idle session (`own` plus a small `sess`).
//!
//! Two phases in two processes, so the memory reading is not polluted by the load: `load` writes
//! `n` sessions and measures the disk before and after a full compaction; `idle` opens the same
//! database with a small cache, reads a sample of sessions the way a quiet cluster does, and
//! measures what the process holds.

use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::json;

use crate::engine::{self, Kind, Opts, Space};
use crate::exp::claims::preload;
use crate::util::{Rng, TAG_OWN, client_id, dir_size, emit, fresh_dir, key, partition, round2, usage};

fn dir_for(data: &Path, kind: Kind, n: u64) -> std::path::PathBuf {
    data.join(format!("footprint-{}-{n}", kind.name()))
}

pub fn load(data: &Path, out: &Path, kind: Kind, n: u64, cache_mb: u64) -> Result<()> {
    let dir = fresh_dir(data, &format!("footprint-{}-{n}", kind.name()));
    let eng = engine::open(
        kind,
        &dir,
        Opts {
            cache_bytes: cache_mb << 20,
            ..Opts::default()
        },
    )?;
    let t = Instant::now();
    preload(eng.as_ref(), n)?;
    eng.flush()?;
    let load_s = t.elapsed().as_secs_f64();
    let (apparent, allocated) = dir_size(&dir);
    let stats_loaded = eng.stats();
    let t = Instant::now();
    eng.compact()?;
    let compact_s = t.elapsed().as_secs_f64();
    let (c_apparent, c_allocated) = dir_size(&dir);
    let logical = logical_bytes(n);
    emit(
        out,
        json!({
            "exp": "footprint-load", "engine": kind.name(), "sessions": n,
            "load_s": round2(load_s), "compact_s": round2(compact_s),
            "logical_bytes": logical,
            "loaded": { "apparent": apparent, "allocated": allocated,
                        "per_session": round2(allocated as f64 / n as f64), "stats": stats_loaded },
            "compacted": { "apparent": c_apparent, "allocated": c_allocated,
                           "per_session": round2(c_allocated as f64 / n as f64), "stats": eng.stats() },
            "load_cache_mb": cache_mb,
        }),
    );
    Ok(())
}

/// The bytes the application asked to store: keys and values of `own` and `sess`.
fn logical_bytes(n: u64) -> u64 {
    (0..n.min(100_000))
        .map(|i| {
            let cid = client_id(i);
            // own: 3 + cid + 32; sess: 3 + cid + 96
            (2 * (3 + cid.len()) + 32 + 96) as u64
        })
        .sum::<u64>()
        * (n / n.min(100_000).max(1))
}

pub fn idle(data: &Path, out: &Path, kind: Kind, n: u64, cache_mb: u64, reads: u64) -> Result<()> {
    let dir = dir_for(data, kind, n);
    let base = usage();
    let t = Instant::now();
    let eng = engine::open(
        kind,
        &dir,
        Opts {
            cache_bytes: cache_mb << 20,
            ..Opts::default()
        },
    )?;
    let open_s = t.elapsed().as_secs_f64();
    let opened = usage();
    let mut rng = Rng::new(7);
    let t = Instant::now();
    let mut missing = 0;
    for _ in 0..reads {
        let i = rng.below(n);
        let cid = client_id(i);
        if eng.get(Space::State, &key(partition(&cid), TAG_OWN, &[&cid]))?.is_none() {
            missing += 1;
        }
    }
    let read_s = t.elapsed().as_secs_f64();
    let warm = usage();
    std::thread::sleep(Duration::from_secs(3));
    let settled = usage();
    let per = |a: u64, b: u64| round2(a.saturating_sub(b) as f64 / n as f64);
    emit(
        out,
        json!({
            "exp": "footprint-idle", "engine": kind.name(), "sessions": n, "cache_mb": cache_mb,
            "open_s": round2(open_s), "reads": reads, "reads_missing": missing,
            "read_us_mean": round2(read_s * 1e6 / reads.max(1) as f64),
            "rss": { "base": base.rss, "opened": opened.rss, "warm": warm.rss, "settled": settled.rss },
            "footprint": { "base": base.footprint, "opened": opened.footprint, "warm": warm.footprint,
                           "settled": settled.footprint },
            // Read right after the sample reads: on a machine under memory pressure the kernel
            // compresses idle pages within seconds, which `settled` shows.
            "rss_per_session": per(warm.rss, base.rss),
            "footprint_per_session": per(warm.footprint, base.footprint),
            "engine_stats": eng.stats(),
        }),
    );
    Ok(())
}

pub fn remove(data: &Path, kind: Kind, n: u64) {
    let _ = std::fs::remove_dir_all(dir_for(data, kind, n));
}
