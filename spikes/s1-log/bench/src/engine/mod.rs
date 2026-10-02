//! The three candidate engines behind one small trait, the shape `KvEngine` would take in
//! `openqtt-log`: atomic batches over two keyspaces (the Raft log and the state machine), point
//! reads, ordered scans, and an explicit durability point.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

mod fjall_engine;
mod redb_engine;
mod rocks_engine;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Fjall,
    Redb,
    Rocksdb,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Kind::Fjall => "fjall",
            Kind::Redb => "redb",
            Kind::Rocksdb => "rocksdb",
        }
    }
}

/// Which keyspace an operation touches: the Raft log, or the state machine it is applied to.
/// One engine instance holds both, so one atomic batch and one fsync can cover both.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Space {
    Log,
    State,
}

pub enum Op {
    Put(Space, Vec<u8>, Vec<u8>),
    Del(Space, Vec<u8>),
    /// Removes every key in `[start, end)`. RocksDB writes one range tombstone; fjall writes a
    /// tombstone per key it finds; redb removes the keys from its tree.
    DelRange(Space, Vec<u8>, Vec<u8>),
}

impl Op {
    pub fn space(&self) -> Space {
        match self {
            Op::Put(s, ..) | Op::Del(s, ..) | Op::DelRange(s, ..) => *s,
        }
    }

    pub fn logical_bytes(&self) -> u64 {
        match self {
            Op::Put(_, k, v) => (k.len() + v.len()) as u64,
            Op::Del(_, k) => k.len() as u64,
            Op::DelRange(_, a, b) => (a.len() + b.len()) as u64,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Opts {
    /// Block cache (fjall, RocksDB) or page cache (redb).
    pub cache_bytes: u64,
    /// Memtable size per keyspace for the LSM engines.
    pub memtable_bytes: u64,
    /// redb only: save the allocator state on every commit so a crash needs no full repair.
    pub redb_quick_repair: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            cache_bytes: 256 << 20,
            memtable_bytes: 64 << 20,
            redb_quick_repair: false,
        }
    }
}

pub trait Engine: Send + Sync {
    /// Applies `ops` atomically. With `sync`, they and every earlier write are durable (one
    /// fsync for the whole batch) before this returns.
    fn write(&self, ops: &[Op], sync: bool) -> Result<()>;
    fn get(&self, space: Space, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Calls `f` for each pair in `[start, end)`, in key order, at most `limit` times.
    fn scan(
        &self,
        space: Space,
        start: &[u8],
        end: &[u8],
        limit: usize,
        f: &mut dyn FnMut(&[u8], &[u8]),
    ) -> Result<usize>;
    /// Makes every earlier write durable.
    fn sync(&self) -> Result<()>;
    /// LSM engines write their memtables out; redb has nothing to do.
    fn flush(&self) -> Result<()>;
    /// Full compaction (LSM) or redb's compact, to measure a settled footprint.
    fn compact(&self) -> Result<()>;
    /// Engine-specific counters, recorded with each run.
    fn stats(&self) -> Value;
}

pub fn open(kind: Kind, dir: &Path, opts: Opts) -> Result<Arc<dyn Engine>> {
    Ok(match kind {
        Kind::Fjall => Arc::new(fjall_engine::Fjall::open(dir, opts)?),
        Kind::Redb => Arc::new(redb_engine::Redb::open(dir, opts)?),
        Kind::Rocksdb => Arc::new(rocks_engine::Rocks::open(dir, opts)?),
    })
}
