//! RocksDB through rust-rocksdb: two column families sharing one write-ahead log.

use std::path::Path;

use anyhow::{Result, anyhow};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType,
    IteratorMode, Options, ReadOptions, WriteBatch, WriteOptions,
};
use serde_json::{Value, json};

use super::{Engine, Op, Opts, Space};

pub struct Rocks {
    db: DB,
    _cache: Cache,
}

fn cf_name(s: Space) -> &'static str {
    match s {
        Space::Log => "log",
        Space::State => "state",
    }
}

impl Rocks {
    pub fn open(dir: &Path, opts: Opts) -> Result<Self> {
        let cache = Cache::new_lru_cache(opts.cache_bytes as usize);
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        table.set_bloom_filter(10.0, false);
        let mut cf = Options::default();
        cf.set_block_based_table_factory(&table);
        // LZ4, the compression fjall uses by default.
        cf.set_compression_type(DBCompressionType::Lz4);
        cf.set_write_buffer_size(opts.memtable_bytes as usize);
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_max_background_jobs(4);
        let db = DB::open_cf_descriptors(
            &db_opts,
            dir,
            vec![
                ColumnFamilyDescriptor::new("log", cf.clone()),
                ColumnFamilyDescriptor::new("state", cf),
            ],
        )?;
        Ok(Rocks { db, _cache: cache })
    }

    fn cf(&self, s: Space) -> Result<&ColumnFamily> {
        self.db
            .cf_handle(cf_name(s))
            .ok_or_else(|| anyhow!("missing column family"))
    }

    fn prop(&self, s: Space, name: &str) -> u64 {
        self.cf(s)
            .ok()
            .and_then(|cf| self.db.property_int_value_cf(cf, name).ok().flatten())
            .unwrap_or(0)
    }
}

impl Engine for Rocks {
    fn write(&self, ops: &[Op], sync: bool) -> Result<()> {
        let mut b = WriteBatch::default();
        for op in ops {
            let cf = self.cf(op.space())?;
            match op {
                Op::Put(_, k, v) => b.put_cf(cf, k, v),
                Op::Del(_, k) => b.delete_cf(cf, k),
                Op::DelRange(_, a, z) => b.delete_range_cf(cf, a, z),
            }
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(sync);
        self.db.write_opt(b, &wo)?;
        Ok(())
    }

    fn get(&self, space: Space, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .db
            .get_pinned_cf(self.cf(space)?, key)?
            .map(|v| v.to_vec()))
    }

    fn scan(
        &self,
        space: Space,
        start: &[u8],
        end: &[u8],
        limit: usize,
        f: &mut dyn FnMut(&[u8], &[u8]),
    ) -> Result<usize> {
        let mut ro = ReadOptions::default();
        ro.set_iterate_upper_bound(end.to_vec());
        let it = self.db.iterator_cf_opt(
            self.cf(space)?,
            ro,
            IteratorMode::From(start, rocksdb::Direction::Forward),
        );
        let mut n = 0;
        for kv in it.take(limit) {
            let (k, v) = kv?;
            f(&k, &v);
            n += 1;
        }
        Ok(n)
    }

    fn sync(&self) -> Result<()> {
        self.db.flush_wal(true)?;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.db.flush_cf(self.cf(Space::Log)?)?;
        self.db.flush_cf(self.cf(Space::State)?)?;
        Ok(())
    }

    fn compact(&self) -> Result<()> {
        self.flush()?;
        for s in [Space::Log, Space::State] {
            self.db
                .compact_range_cf(self.cf(s)?, None::<&[u8]>, None::<&[u8]>);
        }
        Ok(())
    }

    fn stats(&self) -> Value {
        let cf = |s: Space| {
            json!({
                "sst_bytes": self.prop(s, "rocksdb.total-sst-files-size"),
                "live_data": self.prop(s, "rocksdb.estimate-live-data-size"),
                "keys": self.prop(s, "rocksdb.estimate-num-keys"),
                "memtables": self.prop(s, "rocksdb.cur-size-all-mem-tables"),
                "table_readers_mem": self.prop(s, "rocksdb.estimate-table-readers-mem"),
                "l0_files": self.prop(s, "rocksdb.num-files-at-level0"),
                "pending_compaction_bytes": self.prop(s, "rocksdb.estimate-pending-compaction-bytes"),
            })
        };
        json!({
            "block_cache_usage": self.prop(Space::Log, "rocksdb.block-cache-usage"),
            "running_compactions": self.prop(Space::Log, "rocksdb.num-running-compactions"),
            "delayed_write_rate": self.prop(Space::Log, "rocksdb.actual-delayed-write-rate"),
            "write_stopped": self.prop(Space::Log, "rocksdb.is-write-stopped"),
            "log": cf(Space::Log),
            "state": cf(Space::State),
        })
    }
}
