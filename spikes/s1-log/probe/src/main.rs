//! The smallest program that opens each engine, writes one key durably and reads it back, built
//! once per feature so the difference against `none` is what the engine adds to compile time and
//! binary size.

fn main() {
    let dir = std::env::temp_dir().join(format!("s1-probe-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);

    #[cfg(feature = "fjall")]
    {
        let db = fjall::Database::builder(dir.join("fjall")).open().expect("open");
        let ks = db.keyspace("k", fjall::KeyspaceCreateOptions::default).expect("keyspace");
        ks.insert("a", "b").expect("insert");
        db.persist(fjall::PersistMode::SyncData).expect("persist");
        assert!(ks.get("a").expect("get").is_some());
    }

    #[cfg(feature = "redb")]
    {
        use redb::{ReadableDatabase as _, TableDefinition};
        const T: TableDefinition<&str, &str> = TableDefinition::new("k");
        let db = redb::Database::create(dir.join("redb")).expect("create");
        let tx = db.begin_write().expect("begin");
        tx.open_table(T).expect("table").insert("a", "b").expect("insert");
        tx.commit().expect("commit");
        let rx = db.begin_read().expect("read");
        assert!(rx.open_table(T).expect("table").get("a").expect("get").is_some());
    }

    #[cfg(feature = "rocksdb")]
    {
        let db = rocksdb::DB::open_default(dir.join("rocksdb")).expect("open");
        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(true);
        db.put_opt("a", "b", &wo).expect("put");
        assert!(db.get("a").expect("get").is_some());
    }

    #[cfg(feature = "openraft")]
    {
        let c = openraft::Config::default().validate().expect("valid config");
        assert!(c.heartbeat_interval > 0);
    }

    let _ = std::fs::remove_dir_all(&dir);
    println!("ok");
}
