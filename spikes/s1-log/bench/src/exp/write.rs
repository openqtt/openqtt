//! Experiment 1. Durable small writes through group commit: latency at each window and load,
//! throughput, and whether the Raft log and the state machine can share one fsync.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde_json::json;

use crate::commit::{Committer, Shape};
use crate::engine::{self, Kind, Op, Opts, Space};
use crate::exp::{Load, MakeOps, drive};
use crate::util::{Rng, TAG_OWN, client_id, emit, fresh_dir, key, log_key, own_value, partition};

/// Raft log appends of `size`-byte entries spread over `groups` groups, as one node sees them.
fn log_appends(size: usize, groups: u64) -> MakeOps {
    Arc::new(move |i| {
        let g = (i % groups) as u32;
        let mut rng = Rng::new(i);
        vec![Op::Put(Space::Log, log_key(g, i / groups), rng.bytes(size))]
    })
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    data: &Path,
    out: &Path,
    engines: &[Kind],
    sizes: &[usize],
    windows_us: &[u64],
    loads: &[Load],
    warmup: Duration,
    measure: Duration,
) -> Result<()> {
    // Engines innermost, so each one meets the machine in the same state as the others.
    for &size in sizes {
        for &w in windows_us {
            for &load in loads {
                for &kind in engines {
                    let dir = fresh_dir(data, &format!("write-{}", kind.name()));
                    let eng = engine::open(kind, &dir, Opts::default())?;
                    let c = Committer::start(
                        eng.clone(),
                        Duration::from_micros(w),
                        Shape::Combined,
                        8192,
                    );
                    let d = drive(&c, load, warmup, measure, log_appends(size, 128));
                    c.stop();
                    let mut rec = json!({
                        "exp": "write",
                        "engine": kind.name(),
                        "size": size,
                        "window_us": w,
                        "load": load.label(),
                        "engine_stats": eng.stats(),
                    });
                    merge(&mut rec, d.json());
                    emit(out, rec);
                    drop(eng);
                    let _ = std::fs::remove_dir_all(&dir);
                }
            }
        }
    }
    Ok(())
}

/// Each request is a Raft entry plus the `own/{cid}` write it applies to, written in one of the
/// three shapes, or the entry alone as the baseline.
pub fn shared(
    data: &Path,
    out: &Path,
    engines: &[Kind],
    loads: &[Load],
    warmup: Duration,
    measure: Duration,
) -> Result<()> {
    let shapes: [(&str, Option<Shape>); 4] = [
        ("log-only", None),
        ("combined", Some(Shape::Combined)),
        ("split-lazy", Some(Shape::SplitLazy)),
        ("split-sync", Some(Shape::SplitSync)),
    ];
    for &load in loads {
        for (label, shape) in shapes {
            for &kind in engines {
                let dir = fresh_dir(data, &format!("shared-{}", kind.name()));
                let eng = engine::open(kind, &dir, Opts::default())?;
                let c = Committer::start(
                    eng.clone(),
                    Duration::ZERO,
                    shape.unwrap_or(Shape::Combined),
                    8192,
                );
                let with_state = shape.is_some();
                let make: MakeOps = Arc::new(move |i| {
                    let mut rng = Rng::new(i);
                    let cid = client_id(i % 1_000_000);
                    let p = partition(&cid);
                    let mut ops = vec![Op::Put(
                        Space::Log,
                        log_key(u32::from(p), i),
                        rng.bytes(128),
                    )];
                    if with_state {
                        ops.push(Op::Put(
                            Space::State,
                            key(p, TAG_OWN, &[&cid]),
                            own_value(1, 1, i),
                        ));
                    }
                    ops
                });
                let d = drive(&c, load, warmup, measure, make);
                c.stop();
                let mut rec = json!({
                    "exp": "shared-fsync",
                    "engine": kind.name(),
                    "shape": label,
                    "load": load.label(),
                    "engine_stats": eng.stats(),
                });
                merge(&mut rec, d.json());
                emit(out, rec);
                drop(eng);
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
    }
    Ok(())
}

pub fn merge(a: &mut serde_json::Value, b: serde_json::Value) {
    if let (Some(a), serde_json::Value::Object(b)) = (a.as_object_mut(), b) {
        a.extend(b);
    }
}
