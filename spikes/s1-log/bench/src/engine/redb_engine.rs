//! redb 4: a copy-on-write B-tree in one file. One write transaction at a time; every table
//! commits together, so the log and the state machine share the commit's fsync.

use std::path::Path;
use std::sync::RwLock;

use anyhow::Result;
use redb::{Database, Durability, ReadableDatabase, ReadableTableMetadata, TableDefinition};
use serde_json::{Value, json};

use super::{Engine, Op, Opts, Space};

const LOG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("log");
const STATE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("state");

fn table(s: Space) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    match s {
        Space::Log => LOG,
        Space::State => STATE,
    }
}

pub struct Redb {
    // compact() needs &mut Database; everything else shares it.
    db: RwLock<Database>,
    quick_repair: bool,
}

impl Redb {
    pub fn open(dir: &Path, opts: Opts) -> Result<Self> {
        let path = dir.join("db.redb");
        let mut b = Database::builder();
        b.set_cache_size(opts.cache_bytes as usize);
        let db = b.create(path)?;
        let tx = db.begin_write()?;
        tx.open_table(LOG)?;
        tx.open_table(STATE)?;
        tx.commit()?;
        Ok(Redb {
            db: RwLock::new(db),
            quick_repair: opts.redb_quick_repair,
        })
    }
}

impl Engine for Redb {
    fn write(&self, ops: &[Op], sync: bool) -> Result<()> {
        let db = self.db.read().expect("not poisoned");
        let mut tx = db.begin_write()?;
        // None commits are held in memory until the next Immediate one, which persists them all.
        tx.set_durability(if sync {
            Durability::Immediate
        } else {
            Durability::None
        })?;
        tx.set_quick_repair(self.quick_repair);
        {
            let mut log = tx.open_table(LOG)?;
            let mut state = tx.open_table(STATE)?;
            for op in ops {
                let t = match op.space() {
                    Space::Log => &mut log,
                    Space::State => &mut state,
                };
                match op {
                    Op::Put(_, k, v) => {
                        t.insert(k.as_slice(), v.as_slice())?;
                    }
                    Op::Del(_, k) => {
                        t.remove(k.as_slice())?;
                    }
                    Op::DelRange(_, a, z) => {
                        t.retain_in(a.as_slice()..z.as_slice(), |_, _| false)?;
                    }
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn get(&self, space: Space, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let db = self.db.read().expect("not poisoned");
        let tx = db.begin_read()?;
        let t = tx.open_table(table(space))?;
        Ok(t.get(key)?.map(|g| g.value().to_vec()))
    }

    fn scan(
        &self,
        space: Space,
        start: &[u8],
        end: &[u8],
        limit: usize,
        f: &mut dyn FnMut(&[u8], &[u8]),
    ) -> Result<usize> {
        let db = self.db.read().expect("not poisoned");
        let tx = db.begin_read()?;
        let t = tx.open_table(table(space))?;
        let mut n = 0;
        for r in t.range(start..end)?.take(limit) {
            let (k, v) = r?;
            f(k.value(), v.value());
            n += 1;
        }
        Ok(n)
    }

    fn sync(&self) -> Result<()> {
        self.write(&[], true)
    }

    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn compact(&self) -> Result<()> {
        let mut db = self.db.write().expect("not poisoned");
        for _ in 0..8 {
            if !db.compact()? {
                break;
            }
        }
        Ok(())
    }

    fn stats(&self) -> Value {
        let db = self.db.read().expect("not poisoned");
        let mut v = json!({});
        if let Ok(tx) = db.begin_write() {
            if let Ok(s) = tx.stats() {
                v = json!({
                    "tree_height": s.tree_height(),
                    "allocated_pages": s.allocated_pages(),
                    "leaf_pages": s.leaf_pages(),
                    "branch_pages": s.branch_pages(),
                    "stored_bytes": s.stored_bytes(),
                    "metadata_bytes": s.metadata_bytes(),
                    "fragmented_bytes": s.fragmented_bytes(),
                    "page_size": s.page_size(),
                });
            }
            let _ = tx.abort();
        }
        if let Ok(tx) = db.begin_read() {
            for (name, def) in [("log", LOG), ("state", STATE)] {
                if let Ok(t) = tx.open_table(def) {
                    v[name] = json!({ "len": t.len().unwrap_or(0) });
                }
            }
        }
        v
    }
}
