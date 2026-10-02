//! Experiment 5. How long a log pod takes to come back: open a large database after a clean stop
//! and after `kill -9`, and replay Raft entries into the state machine.
//!
//! Phases run as separate processes (`run.sh recovery` drives them): `load` fills the database,
//! `open` measures time to ready, `crash-writer` writes durably until it is killed, `replay`
//! measures applying log entries to the state.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::json;

use crate::commit::{Committer, Req, Shape};
use crate::engine::{self, Kind, Op, Opts, Space};
use crate::exp::claims::preload;
use crate::util::{
    Rng, TAG_MSG, TAG_OWN, client_id, dir_size, emit, fresh_dir, key, log_key, own_value,
    partition, round2,
};

fn dir_for(data: &Path, kind: Kind) -> PathBuf {
    data.join(format!("recovery-{}", kind.name()))
}

fn opts(quick_repair: bool) -> Opts {
    Opts {
        redb_quick_repair: quick_repair,
        ..Opts::default()
    }
}

/// Fills a database with `gib` GiB: a million sessions, Raft log entries (a tenth of the bytes)
/// and message bodies (the rest), 1 KiB each, synced every 256 MiB.
pub fn load(data: &Path, out: &Path, kind: Kind, gib: u64, quick_repair: bool) -> Result<()> {
    let dir = fresh_dir(data, &format!("recovery-{}", kind.name()));
    let eng = engine::open(kind, &dir, opts(quick_repair))?;
    let t = Instant::now();
    preload(eng.as_ref(), 1_000_000)?;
    let target = gib << 30;
    let mut rng = Rng::new(9);
    let mut written: u64 = 0;
    let mut since_sync: u64 = 0;
    let mut idx: u64 = 0;
    let mut ops = Vec::with_capacity(1000);
    while written < target {
        let v = rng.bytes(1024);
        if idx % 10 == 0 {
            ops.push(Op::Put(Space::Log, log_key((idx % 256) as u32, idx), v));
        } else {
            ops.push(Op::Put(
                Space::State,
                key((idx % 256) as u16, TAG_MSG, &[&idx.to_be_bytes()]),
                v,
            ));
        }
        idx += 1;
        written += 1024 + 12;
        since_sync += 1024 + 12;
        if ops.len() == 1000 {
            let sync = since_sync >= 256 << 20;
            eng.write(&ops, sync)?;
            ops.clear();
            if sync {
                since_sync = 0;
            }
        }
    }
    eng.write(&ops, true)?;
    eng.flush()?;
    let load_s = t.elapsed().as_secs_f64();
    let (apparent, allocated) = dir_size(&dir);
    emit(
        out,
        json!({
            "exp": "recovery-load", "engine": kind.name(), "gib": gib, "quick_repair": quick_repair,
            "load_s": round2(load_s), "entries": idx, "apparent": apparent, "allocated": allocated,
            "engine_stats": eng.stats(),
        }),
    );
    Ok(())
}

/// Opens the existing database and reports the time until it has served a read and a durable
/// write.
pub fn open(data: &Path, out: &Path, kind: Kind, label: &str, quick_repair: bool) -> Result<()> {
    let dir = dir_for(data, kind);
    let t = Instant::now();
    let eng = engine::open(kind, &dir, opts(quick_repair))?;
    let open_s = t.elapsed().as_secs_f64();
    let cid = client_id(12_345);
    let found = eng
        .get(Space::State, &key(partition(&cid), TAG_OWN, &[&cid]))?
        .is_some();
    let read_s = t.elapsed().as_secs_f64();
    eng.write(
        &[Op::Put(Space::Log, log_key(9_999, 0), b"ready".to_vec())],
        true,
    )?;
    let ready_s = t.elapsed().as_secs_f64();
    let (apparent, allocated) = dir_size(&dir);
    emit(
        out,
        json!({
            "exp": "recovery-open", "engine": kind.name(), "label": label,
            "quick_repair": quick_repair, "open_s": round2(open_s), "first_read_s": round2(read_s),
            "ready_s": round2(ready_s), "found": found, "apparent": apparent, "allocated": allocated,
            "engine_stats": eng.stats(),
        }),
    );
    Ok(())
}

/// Writes 1 KiB entries durably, 64 at a time in flight, until killed. Prints `writing` once
/// the database is open so the caller knows when to start its clock.
pub fn crash_writer(data: &Path, kind: Kind, quick_repair: bool) -> Result<()> {
    let dir = dir_for(data, kind);
    let eng = engine::open(kind, &dir, opts(quick_repair))?;
    let c = Committer::start(eng.clone(), Duration::ZERO, Shape::Combined, 8192);
    let sender = c.sender();
    let n = Arc::new(AtomicU64::new(0));
    println!("writing");
    std::io::stdout().flush()?;
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    let mut rng = Rng::new(3);
    let mut i: u64 = 0;
    let mut inflight = 0;
    loop {
        while inflight < 64 {
            let cid = client_id(i % 1_000_000);
            let p = partition(&cid);
            let ops = vec![
                Op::Put(Space::Log, log_key(10_000 + u32::from(p), i), rng.bytes(1024)),
                Op::Put(Space::State, key(p, TAG_OWN, &[&cid]), own_value(2, 3, i)),
            ];
            let tx = tx.clone();
            let n = n.clone();
            sender.submit(Req {
                ops,
                done: Some(Box::new(move |_| {
                    n.fetch_add(1, Ordering::Relaxed);
                    let _ = tx.send(());
                })),
            });
            i += 1;
            inflight += 1;
        }
        let _ = rx.recv();
        inflight -= 1;
    }
}

/// Appends `entries` claim entries to the log, durably, then measures reading them back in order
/// and applying each to `own`, the work a restarted replica does between its last applied index
/// and the end of its log.
pub fn replay(data: &Path, out: &Path, kind: Kind, entries: u64) -> Result<()> {
    let dir = dir_for(data, kind);
    let eng = engine::open(kind, &dir, opts(false))?;
    let group = 20_000u32;
    let t = Instant::now();
    let mut ops = Vec::with_capacity(1000);
    for i in 0..entries {
        let cid = client_id(i);
        let mut e = Vec::with_capacity(cid.len() + 32);
        e.extend_from_slice(&(cid.len() as u16).to_be_bytes());
        e.extend_from_slice(&cid);
        e.extend_from_slice(&own_value(1, 4, i));
        ops.push(Op::Put(Space::Log, log_key(group, i), e));
        if ops.len() == 1000 {
            eng.write(&ops, true)?;
            ops.clear();
        }
    }
    eng.write(&ops, true)?;
    ops.clear();
    let append_s = t.elapsed().as_secs_f64();

    let t = Instant::now();
    let mut applied = 0u64;
    let mut from = 0u64;
    let mut read_s = 0.0;
    while from < entries {
        let to = (from + 10_000).min(entries);
        let r0 = Instant::now();
        let mut batch: Vec<Op> = Vec::with_capacity(10_000);
        eng.scan(
            Space::Log,
            &log_key(group, from),
            &log_key(group, to),
            usize::MAX,
            &mut |_, v| {
                let len = usize::from(u16::from_be_bytes([v[0], v[1]]));
                let cid = &v[2..2 + len];
                let val = v[2 + len..].to_vec();
                batch.push(Op::Put(Space::State, key(partition(cid), TAG_OWN, &[cid]), val));
            },
        )?;
        read_s += r0.elapsed().as_secs_f64();
        applied += batch.len() as u64;
        eng.write(&batch, false)?;
        from = to;
    }
    eng.sync()?;
    let replay_s = t.elapsed().as_secs_f64();
    emit(
        out,
        json!({
            "exp": "recovery-replay", "engine": kind.name(), "entries": entries,
            "append_s": round2(append_s), "replay_s": round2(replay_s), "read_s": round2(read_s),
            "applied": applied, "replay_per_s": round2(applied as f64 / replay_s),
        }),
    );
    Ok(())
}

pub fn remove(data: &Path, kind: Kind) {
    let _ = std::fs::remove_dir_all(dir_for(data, kind));
}
