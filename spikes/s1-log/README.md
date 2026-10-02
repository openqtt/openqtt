# Spike S1: the log's engine and replication

Measures which storage engine (fjall, redb, RocksDB) and which replication scheme
(openraft with one group per partition, or primary-backup fenced by epochs) the
`log` role is built on. The findings are in
[report R4](../../docs/reports/R04-log.md); the raw numbers are in
`bench/results/s1-<date>.json`.

This is spike code: measured, reported, then discarded or harvested. It is a
workspace of its own, excluded from the OpenQTT workspace, so its pinned
dependencies never reach the broker's `Cargo.lock` or `make check`.

## Layout

- `bench/src/engine/`: the three engines behind one `KvEngine`-shaped trait, two
  keyspaces each (the Raft log and the state machine).
- `bench/src/commit.rs`: group commit, one batch and one fsync per flush.
- `bench/src/exp/`: the single-node experiments (durability floor, write
  latency, shared fsync, claim storm, footprint, queue churn, recovery).
- `bench/src/repl/`: openraft 0.9 and 0.10 and primary-backup on a simulated
  network with a per-node disk model.
- `probe/`: the smallest program per engine, for compile time and binary size.

## Running

```console
cargo build --release
DATA=/path/with/60GiB/free ./run.sh fsync     # then write, shared, claims, footprint,
                                              # footprint-reopen, churn, window, recovery,
                                              # repl, repl-max, repl-storm, idle, netcost,
                                              # build
./docker-build.sh                             # build friction in rust:1.99.0-bookworm
python3 collect.py                            # out/*.jsonl -> bench/results/s1-<date>.json
python3 summarize.py                          # the tables R4 quotes
```

Each step appends one JSON object per measured run to `out/<step>.jsonl`. Run
steps one at a time on a quiet machine; the write and shared steps run three
passes to show their spread.

On macOS, RocksDB is compiled with `-DHAVE_FULLFSYNC` (see `.cargo/config.toml`),
because rust-rocksdb leaves it out and its sync would otherwise not flush the
drive's cache, which fjall and redb do through Rust's standard library.
