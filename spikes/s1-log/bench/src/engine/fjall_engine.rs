//! fjall 3: an LSM tree per keyspace, one journal shared by every keyspace of the database.

use std::path::Path;

use anyhow::Result;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use serde_json::{Value, json};

use super::{Engine, Op, Opts, Space};

pub struct Fjall {
    db: Database,
    log: Keyspace,
    state: Keyspace,
}

impl Fjall {
    pub fn open(dir: &Path, opts: Opts) -> Result<Self> {
        let db = Database::builder(dir).cache_size(opts.cache_bytes).open()?;
        let memtable = opts.memtable_bytes;
        let log = db.keyspace("log", || {
            KeyspaceCreateOptions::default().max_memtable_size(memtable)
        })?;
        let state = db.keyspace("state", || {
            KeyspaceCreateOptions::default().max_memtable_size(memtable)
        })?;
        Ok(Fjall { db, log, state })
    }

    fn ks(&self, s: Space) -> &Keyspace {
        match s {
            Space::Log => &self.log,
            Space::State => &self.state,
        }
    }
}

impl Engine for Fjall {
    fn write(&self, ops: &[Op], sync: bool) -> Result<()> {
        // Every item of a fjall batch gets the same sequence number, so a put and a tombstone for
        // one key in one batch do not apply in order. Resolve the batch first: a range delete
        // drops the puts before it that it covers, then tombstones every committed key in range.
        let mut items: Vec<(Space, Vec<u8>, Option<&[u8]>)> = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                Op::Put(s, k, v) => items.push((*s, k.clone(), Some(v.as_slice()))),
                Op::Del(s, k) => items.push((*s, k.clone(), None)),
                Op::DelRange(s, a, z) => {
                    items.retain(|(is, k, _)| {
                        !(is == s && k.as_slice() >= a.as_slice() && k.as_slice() < z.as_slice())
                    });
                    // No range tombstone in fjall 3: a tombstone for every live key.
                    for g in self.ks(*s).range(a.as_slice()..z.as_slice()) {
                        items.push((*s, g.key()?.to_vec(), None));
                    }
                }
            }
        }
        let mut b = self.db.batch();
        for (s, k, v) in items {
            match v {
                Some(v) => b.insert(self.ks(s), k, v),
                None => b.remove(self.ks(s), k),
            }
        }
        // SyncData is fdatasync on Linux and F_FULLFSYNC on macOS.
        let b = b.durability(sync.then_some(PersistMode::SyncData));
        b.commit()?;
        Ok(())
    }

    fn get(&self, space: Space, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.ks(space).get(key)?.map(|v| v.to_vec()))
    }

    fn scan(
        &self,
        space: Space,
        start: &[u8],
        end: &[u8],
        limit: usize,
        f: &mut dyn FnMut(&[u8], &[u8]),
    ) -> Result<usize> {
        let mut n = 0;
        for g in self.ks(space).range(start..end).take(limit) {
            let (k, v) = g.into_inner()?;
            f(&k, &v);
            n += 1;
        }
        Ok(n)
    }

    fn sync(&self) -> Result<()> {
        self.db.persist(PersistMode::SyncData)?;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.log.rotate_memtable_and_wait()?;
        self.state.rotate_memtable_and_wait()?;
        Ok(())
    }

    fn compact(&self) -> Result<()> {
        self.flush()?;
        self.log.major_compact()?;
        self.state.major_compact()?;
        Ok(())
    }

    fn stats(&self) -> Value {
        let ks = |k: &Keyspace| {
            json!({
                "tables": k.table_count(),
                "l0_tables": k.l0_table_count(),
                "disk": k.disk_space(),
                "approx_len": k.approximate_len(),
            })
        };
        json!({
            "disk": self.db.disk_space().unwrap_or(0),
            "journals": self.db.journal_count(),
            "journal_disk": self.db.journal_disk_space().unwrap_or(0),
            "write_buffer": self.db.write_buffer_size(),
            "cache_used": self.db.cache_size(),
            "compactions": self.db.compactions_completed(),
            "compacting_s": self.db.time_compacting().as_secs_f64(),
            "log": ks(&self.log),
            "state": ks(&self.state),
        })
    }
}
