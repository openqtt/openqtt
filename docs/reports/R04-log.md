# R4. The log: engine, replication and what committed means

Status: draft. Measured by spike S1 (`spikes/s1-log`) on 2026-10-01 and 02. Every
number below is in `bench/results/s1-2026-10-02.json`, with the machine, the
versions and the load average of each run.

## The question

R3 left the foundation of the log role to a measured spike:

- which storage engine sits behind `KvEngine`: fjall (an LSM tree in Rust),
  redb (a copy-on-write B-tree in Rust) or RocksDB (C++ through rust-rocksdb);
- whether partitions replicate as one openraft group each (256 by default, three
  replicas) or as a simpler primary-backup scheme fenced by epochs from the meta
  group;
- how long group commit waits for a batch, and whether the Raft log and the state
  machine can share one fsync;
- what a reconnect storm costs (target: 50,000 session claims a second per
  cluster), what an idle session costs on disk and in memory at 10^8, and how
  long a log pod takes to come back.

## The answer

- **Engine: RocksDB**, through rust-rocksdb, one instance per log node holding
  every partition the node replicates, the Raft log and the state machine as two
  column families, LZ4.
- **Replication: one openraft group per partition, on openraft 0.10**, with the
  log role's transport pipelining appends through `stream_append`, heartbeats
  every 250 ms and elections after 1 to 2 s. Not openraft 0.9, and not
  primary-backup.
- **Group commit with no window**: a node's log writer takes whatever is waiting
  when the previous flush ends and flushes it as one batch, bounded in bytes.
- **One fsync per batch, shared**: Raft entries of every partition on a node and
  the state machine's writes go to one engine, whose write-ahead log one flush
  covers; the state machine never syncs on its own.

The three numbers that decided it:

1. **The storm on one node's engine**: RocksDB applied 200,000 claims a second
   (each a read, a Raft entry and an `own` write, durable) at a p99 of 12 ms;
   fjall the same rate at 115 ms; redb fell behind between 25,000 and 50,000.
2. **Stalls over ten minutes of queue churn**: seconds in which a durable
   write took over 100 ms, out of 600: RocksDB 5, fjall 62, redb 351.
3. **Commit latency, pipelined openraft 0.10 against primary-backup**: 4.2 ms
   against 3.5 ms at p50 in a storm of 50,000 writes a second over 256 groups
   (1 ms one way, 1 ms flush), while openraft 0.9 lost 120 of 128 leaders in the
   same storm spread over 128 groups. Raft costs no latency worth writing a
   replication protocol to save.

And the answers R3 asked for: a cluster absorbs 50,000 claims a second with the
engine four times over and replication 250 times over per partition; an idle
session costs 108 bytes of disk and 4 bytes of memory per replica, so 10^8
sessions take 30 GiB of disk and 1.1 GiB of memory across a cluster with three
replicas; a log pod with 50 GiB of RocksDB data is ready 0.13 s after a clean
stop and 0.41 s after `SIGKILL`, and replays a million log entries in about 2 s.

## How it was measured

**These are laptop numbers.** Every run used one Apple M2 Pro (6 performance and
4 efficiency cores, 16 GiB) with its internal SSD under APFS, macOS 26.5 and
Rust 1.99.0, while other work shared the machine: the load average was 2 to 20
and is recorded with every result. A server with a datacentre disk has other
absolute numbers. What carries over is the comparison between candidates, which
always ran interleaved under the same conditions, and the shape of each curve.

**Durable means a full cache flush.** On macOS, `fsync(2)` leaves data in the
drive's volatile cache; only `fcntl(F_FULLFSYNC)` survives power loss, and
Rust's `File::sync_data` uses it. fjall and redb sync through Rust's standard
library. rust-rocksdb's build script never defines `HAVE_FULLFSYNC`, so a macOS
build of RocksDB silently syncs with plain `fsync`; the spike compiles it with
`-DHAVE_FULLFSYNC` so that all three pay the same price. On Linux all three use
`fdatasync`.

**What ran.**

| Candidate | Version | Configuration |
| --- | --- | --- |
| fjall | 3.1.10 (lsm-tree 3.1.10) | defaults: 64 MiB memtable per keyspace, leveled compaction, LZ4 from level 2 down, bloom filters |
| redb | 4.3.0 | defaults: one-phase commit with checksums; quick repair off unless stated |
| RocksDB | rust-rocksdb 0.25.0, RocksDB 11.8.1 | LZ4, 10-bit bloom filters, 64 MiB memtables, 4 background jobs |
| openraft | 0.9.25 and 0.10.0-alpha.36 | heartbeat 50 ms, election 150 to 300 ms, unless stated |
| primary-backup | written for the spike, under 300 lines, no failover | epoch fencing, one acknowledgement from two backups |

Every engine instance holds two keyspaces (fjall keyspaces, redb tables, RocksDB
column families): the Raft log and the state machine. One instance per node holds
every partition the node replicates, so keys start with the partition. The cache
is 256 MiB except where a footprint measurement says otherwise.

**Group commit.** One thread owns each engine's write path. It takes every
request waiting, writes them as one atomic batch with one fsync, and completes
them all. The window is how long it waits after the first request of a batch
for more: at 0 it takes only what queued during the previous flush.

**Load.** Closed loop keeps N requests outstanding and finds throughput. Open
loop sends on a fixed schedule and measures each latency from when the request
was due, so a stall shows up as the queue behind it rather than as fewer
requests.

**Replication** runs three replicas of every group in one process. Messages cross
a delay line (one thread with a deadline heap, because tokio's timer rounds to
whole milliseconds) with 1 or 2 ms one way, modelling zones in one region. Each
node has one disk shared by all its groups: whatever is queued is flushed
together after a modelled flush time (0, or 1 ms for a network block device), so
the replication schemes are compared on the protocol alone, and the engine's real
flush cost comes from the single-node results. In-process messages cost no
serialisation, sockets or TLS; a loopback measurement puts a floor under that.

macOS stretches an ordinary thread's timed waits by about a quarter when nothing
else wakes it, so a first replication pass fired 1 ms hops 250 us late in quiet
runs and on time in busy ones, which favoured the scheme that sends more
messages. That pass was discarded. The delay line now runs at the
user-interactive scheduling class and spins the last 150 us: in the median run
a hop is 1 us late at p50 and 60 us at p99, except in the saturation runs of one
partition leader at its limit, where every core was busy and hops ran about
250 us late for every scheme alike.

## Results

### 1. The durability floor

One thread writing and syncing, appending to a file as a log does; the last
column is three threads on three files together. Two passes, two and a half
hours apart. Each cell is p50 / p99 in milliseconds and syncs a second.

| Primitive | Pass | 128 B | 4 KiB | 64 KiB | 1 MiB | 4 KiB, 3 threads |
| --- | --- | --- | --- | --- | --- | --- |
| `F_FULLFSYNC` | 1 (load 8 to 22) | 5.0 / 39.8 (130/s) | 5.1 / 9.0 (187/s) | 5.2 / 9.0 (183/s) | 6.0 / 11.8 (162/s) | 9.9 / 17.8 (307/s) |
| `F_FULLFSYNC` | 2 (load 4 to 7) | 4.0 / 13.5 (214/s) | 4.0 / 7.2 (245/s) | 4.0 / 7.3 (243/s) | 4.0 / 8.2 (225/s) | 12.0 / 18.3 (254/s) |
| std `sync_data` | 1 (load 6 to 11) | 5.0 / 8.0 (200/s) | 5.0 / 8.1 (196/s) | 5.0 / 8.0 (199/s) | 5.4 / 8.4 (179/s) | 10.1 / 17.0 (295/s) |
| std `sync_data` | 2 (load 4 to 16) | 2.2 / 3.1 (481/s) | 1.2 / 3.1 (584/s) | 2.9 / 4.0 (423/s) | 2.4 / 4.2 (391/s) | 7.3 / 12.0 (406/s) |
| `F_BARRIERFSYNC` | 1 (load 8 to 14) | 1.3 / 4.0 (714/s) | 1.1 / 4.0 (786/s) | 0.93 / 5.1 (837/s) | 1.3 / 4.9 (591/s) | 1.7 / 4.8 (1666/s) |
| `F_BARRIERFSYNC` | 2 (load 3 to 6) | 0.13 / 0.40 (6418/s) | 0.14 / 1.1 (5332/s) | 0.17 / 0.82 (4388/s) | 1.4 / 5.2 (641/s) | 0.46 / 0.83 (6038/s) |
| `fsync` | 1 (load 8 to 23) | 0.04 / 0.70 (11617/s) | 0.04 / 1.1 (8392/s) | 0.05 / 1.1 (7223/s) | 0.33 / 6.8 (1220/s) | 0.06 / 1.8 (13096/s) |
| `fsync` | 2 (load 4 to 10) | 0.03 / 0.04 (39204/s) | 0.02 / 0.04 (45936/s) | 0.03 / 0.04 (31758/s) | 0.24 / 0.32 (2760/s) | 0.03 / 0.06 (108101/s) |

- **A durable write costs 1 to 6 ms on this machine, whatever its size up to
  1 MiB**, and the same call varies by a factor of four from one minute to the
  next: a flush of the drive's cache serves every writer waiting on it, so what
  other processes flush moves ours. Everything durable below is a multiple of
  it. Rust's `sync_data` is `F_FULLFSYNC`, as expected.
- **`fsync` without the cache flush is a hundred times cheaper**, which is why a
  macOS build of RocksDB without `HAVE_FULLFSYNC` looks fast and is not durable.
- **Flushes do not run in parallel.** Three threads on three files got 1.0 to
  1.6 times the syncs of one, not three. Nothing is gained by giving each
  partition its own log file and flushing them side by side; one log per node
  with group commit is the way to batch.
- Writing into a region synced beforehand instead of appending did not make the
  flush cheaper (all four primitives are in the appendix).

### 2. Group commit

Raft log appends of 128 B and 1 KiB spread over 128 groups, as one node sees
them; window 0; median of three passes, engines interleaved within each
configuration. Each cell is entries a second, then p50 / p99 in milliseconds.

| Entry | Load | fjall | redb | RocksDB |
| --- | --- | --- | --- | --- |
| 128 B | 1 outstanding | 139/s, 6.8 / 17.0 | 127/s, 6.8 / 16.1 | 147/s, 6.1 / 17.0 |
| 128 B | 1,024 outstanding | 88.7k/s, 8.7 / 34.2 | 56.6k/s, 16.2 / 38.6 | 116.0k/s, 7.8 / 21.2 |
| 128 B | 10,000/s | 10.0k/s, 10.2 / 17.8 | 10.0k/s, 18.0 / 67.8 | 10.0k/s, 9.5 / 16.4 |
| 128 B | 50,000/s | 49.9k/s, 10.5 / 43.9 | 49.9k/s, 23.7 / 154 | 49.9k/s, 9.7 / 20.4 |
| 1 KiB | 1 outstanding | 146/s, 6.3 / 12.3 | 127/s, 6.9 / 23.2 | 145/s, 6.8 / 12.2 |
| 1 KiB | 1,024 outstanding | 41.7k/s, 20.4 / 76.5 | 27.4k/s, 35.2 / 76.5 | 63.2k/s, 11.4 / 44.4 |
| 1 KiB | 10,000/s | 10.0k/s, 15.4 / 95.7 | 10.0k/s, 30.1 / 136 | 10.0k/s, 11.1 / 25.3 |
| 1 KiB | 50,000/s | 49.5k/s, 43.0 / 289 | 13.8k/s, 4098 / 5620 | 49.9k/s, 17.9 / 83.4 |

- **Throughput comes from batch size, not from faster flushes.** With 1,024
  requests outstanding one flush carries 1,024 entries: RocksDB writes 116,000
  entries of 128 B a second at a p99 of 21 ms, fjall 89,000 at 34 ms, redb
  57,000 at 39 ms.
- **At 50,000 entries a second, a node's share of a storm,** RocksDB's p99 is
  20 ms for 128 B entries and 83 ms for 1 KiB ones, fjall's 44 ms and 289 ms,
  redb's 154 ms, and redb cannot keep up with 1 KiB entries at all: 13,800 a
  second and seconds of queue.
- **fjall's tail is its backpressure.** When four sealed memtables wait to be
  flushed, fjall stops the writer in 100 ms sleeps (`local_backpressure` in
  fjall 3.1.10, `src/keyspace/mod.rs`), and single batch writes then reach 170
  to 465 ms. RocksDB's background flushes kept up on the same disk.
- **redb pays for copy-on-write on every commit.** A commit rewrites the pages
  from each touched leaf to the root: about 80 KB written for a one-entry commit
  against 15 KB for the LSM engines (most of it APFS's own metadata), and 18 to
  125 times the logical bytes on small batches against 2 to 3 times.

**The window.** A second run put the three windows side by side within every
configuration (128 B entries, two passes, a quieter machine: load average 2 to
6), so that drift in the flush cost does not pass for an effect of the window.
Each cell is p50 / p99 in milliseconds.

| Load | Engine | No window | 1 ms | 2 ms |
| --- | --- | --- | --- | --- |
| 1 outstanding | fjall | 4.5 / 12.0 | 4.5 / 6.9 | 6.0 / 9.3 |
| 1 outstanding | redb | 3.5 / 6.3 | 5.0 / 8.4 | 6.5 / 12.1 |
| 1 outstanding | RocksDB | 3.5 / 5.5 | 4.5 / 10.2 | 6.2 / 9.5 |
| 10,000/s | fjall | 5.1 / 9.1 | 5.3 / 9.7 | 6.2 / 10.9 |
| 10,000/s | redb | 6.2 / 27.8 | 7.1 / 17.0 | 8.3 / 17.6 |
| 10,000/s | RocksDB | 4.6 / 8.3 | 5.3 / 22.1 | 5.6 / 9.9 |
| 50,000/s | fjall | 4.5 / 79.8 | 4.8 / 17.6 | 6.3 / 13.2 |
| 50,000/s | redb | 8.4 / 17.6 | 8.9 / 17.9 | 11.4 / 22.8 |
| 50,000/s | RocksDB | 4.0 / 7.8 | 5.5 / 10.9 | 6.6 / 20.7 |

- **A window only adds latency here.** With nothing else waiting it adds its own
  length; under load it adds part of it and gains nothing, because a flush takes
  longer than the window and the next batch has gathered by the time the writer
  is free. The p99 column moves by more than the window in both directions from
  run to run, which is the machine, not the window (fjall's 79.8 is one of its
  backpressure stalls).
- A window would pay only on a device whose flush is much shorter than the gap
  between requests, where it trades latency for fewer flushes; a cap on flushes
  a second does that without delaying a lone write. (macOS also stretches a
  thread's timed waits, so a "1 ms" window lasted somewhat longer; that does not
  change the comparison with no window, which waits for nothing.)

### 3. One fsync for the log and the state machine

Each request is a Raft entry (128 B) plus the `own` write it applies to,
written as: the entry alone (baseline); entry and state in one atomic batch with
one flush (combined); the state written after the flush without a sync of its
own (split, lazy); or the state with a second flush (split, synced). Median of
three passes; entries a second, then p50 / p99 in milliseconds.

| Shape | fjall, 256 outstanding | redb | RocksDB | fjall, 20,000/s | redb | RocksDB |
| --- | --- | --- | --- | --- | --- | --- |
| entry alone | 35.9k, 6.1 / 18.4 | 17.7k, 13.5 / 31.2 | 42.1k, 5.7 / 14.2 | 9.8 / 32.5 | 25.2 / 64.6 | 9.4 / 37.5 |
| combined | 31.1k, 7.4 / 19.4 | 9.3k, 24.2 / 66.8 | 32.8k, 6.6 / 20.2 | 10.4 / 27.4 | 81.3 / 286 | 9.2 / 31.7 |
| split, lazy | 30.4k, 7.3 / 17.4 | 9.9k, 22.6 / 59.6 | 33.3k, 6.9 / 16.8 | 10.4 / 32.5 | 82.8 / 164 | 9.5 / 21.2 |
| split, synced | 19.1k, 12.3 / 25.1 | 7.7k, 29.2 / 80.0 | 20.8k, 11.9 / 20.3 | 18.8 / 33.3 | 92.3 / 277 | 18.9 / 40.7 |

- **All three engines can share one flush.** fjall's journal and RocksDB's
  write-ahead log are shared by every keyspace, so a batch across keyspaces is
  atomic under one sync, and a state write without a sync becomes durable with
  the next log flush. redb commits every table in one transaction, and a
  non-durable commit is persisted by the next durable one.
- **Sharing is free in the LSM engines and a second flush is not.** Combined and
  lazy cost what the entry alone costs, within the spread between passes; a
  second flush doubles the median latency and takes 40 to 50 percent off
  throughput.
  In redb the state writes themselves are the cost: a random key is a new leaf
  per update.

### 4. The claim storm

A million sessions exist; claims arrive on schedule, each reading `own/{cid}`,
then appending the claim to the Raft log and writing the new `own` in one batch,
durable before it counts. Four apply threads stand in for partition state
machines (a client id always lands on the same one); one thread is the most a
single partition's state machine applies. One pass each; entries a second, then
p50 / p99 in milliseconds.

| Offered | fjall, 4 threads | redb | RocksDB | fjall, 1 thread | redb | RocksDB |
| --- | --- | --- | --- | --- | --- | --- |
| 5,000/s | | | | 5.0k, 3.9 / 7.3 | 5.0k, 8.5 / 25.4 | 5.0k, 3.5 / 23.9 |
| 10,000/s | 10.0k, 9.9 / 50.1 | 10.0k, 19.8 / 38.0 | 10.0k, 4.1 / 8.1 | 10.0k, 1.8 / 5.9 | 10.0k, 25.0 / 71.7 | 10.0k, 3.4 / 141 |
| 25,000/s | 25.0k, 9.6 / 25.9 | 24.7k, 109 / 183 | 25.0k, 3.4 / 6.1 | 25.0k, 3.4 / 6.2 | 24.0k, 123 / 211 | 25.0k, 3.3 / 24.5 |
| 50,000/s | 49.9k, 12.9 / 131 | 43.7k, 763 / 1061 | 50.0k, 3.6 / 10.6 | 50.0k, 3.9 / 37.4 | 44.4k, 1154 / 1516 | 50.0k, 4.1 / 19.2 |
| 100,000/s | 99.9k, 6.8 / 66.1 | 35.0k, 3873 / 5091 | 100.0k, 4.0 / 7.9 | 100.0k, 4.4 / 58.5 | 34.2k, 4086 / 5370 | 99.9k, 4.9 / 43.6 |
| 200,000/s | 199.8k, 9.5 / 115 | not run | 199.9k, 5.5 / 11.7 | | | |

- **One node's engine is not what limits a storm.** RocksDB applied 200,000
  claims a second at a p99 of 12 ms, four times the 50,000 a second the whole
  cluster has to absorb, and a single partition's apply thread kept up with
  100,000 a second. fjall kept up too, with p99 tails of 66 to 131 ms; redb
  stopped keeping up between 25,000 and 50,000 a second.
- **Reads are not the cost.** With a million sessions the `own` lookups hit the
  cache (2 to 9 us at p50); the claim's cost is its share of a flush. With ten
  million and a 64 MiB cache, where most lookups miss it, a read took 23 us on
  average in either LSM engine (section 5).
- **Per partition leader, the engine allows 100,000 claims a second**; with 256
  partitions a storm of 50,000 a second is about 200 a second per partition, so
  replication, not the engine, sets the per-partition limit (section 8).
- Each RocksDB and fjall p99 above a few tens of milliseconds is a single
  run's stall (one pass, a busy machine); the medians are the steadier signal.

### 5. What an idle session costs

A session is its `own` (32 B) and a small `sess` (96 B: two subscriptions, the
packet identifier, limits), keyed by a 16 to 64 character client id: 214
logical bytes. Sessions were written in random key order with a sync every
100,000, then compacted, closed and reopened; memory is the process's physical
footprint after 100,000 random reads of `own` with a 64 MiB cache, and the read
column is their mean latency. Disk is bytes per
session after reopening, which is when fjall deletes the files its compactions
made obsolete (right after compaction it still held nearly three times as
much).

| Engine | Sessions | Load s | Disk B/session | Memory after reads MiB | Memory right after open MiB | Read us, mean |
| --- | --- | --- | --- | --- | --- | --- |
| fjall | 1,000,000 | 2.7 | 186 | 171 | 91.0 | 3.6 |
| fjall | 10,000,000 | 29.8 | 112 | 90 | 1.6 | 22.7 |
| RocksDB | 1,000,000 | 2.3 | 109 | 71 | 6.0 | 2.6 |
| RocksDB | 10,000,000 | 20.6 | 108 | 107 | 41.1 | 23.1 |
| redb | 1,000,000 | 24.7 | 339 | 86 | 0.3 | 1.8 |
| redb | 10,000,000 | stopped after 2,010 s | | | | |

**Extrapolated.** Disk per session is constant between the two sizes once
fjall's 64 MiB journal file is set aside (67 bytes a session at a million, 7 at
ten million), so it scales linearly; redb's ten-million load did not finish in
33 minutes (a first attempt without the periodic syncs was stopped after 35),
so its line rests on one size. Memory that grows with sessions is the slope
between the two sizes (the cache, the runtime and the memtables are the same in
both and cancel). RocksDB's is its index and bloom filter blocks, which it keeps
outside the block cache by default; fjall's does not grow; redb's needs two
sizes and has one. Totals are for the whole cluster at three replicas; divide
by the number of log nodes.

| Engine | Disk B/session | Memory B/session | 10^8 sessions: disk | memory | 10^9 sessions: disk | memory |
| --- | --- | --- | --- | --- | --- | --- |
| fjall | 112 | 0 | 31 GiB | 0 | 313 GiB | 0 |
| RocksDB | 108 | 4.1 | 30 GiB | 1.1 GiB | 302 GiB | 11.4 GiB |
| redb | 339 (10^6 only) | not measured | 95 GiB | | 947 GiB | |

- **The LSM engines store a session in about half its logical size**: LZ4 and
  prefix compression of sorted keys win more than the per-entry overhead costs.
  redb stores it in 1.6 times its logical size, three times RocksDB.
- **At 10^9 sessions RocksDB needs about 300 GiB of disk and 11 GiB of memory
  across the cluster** for idle sessions alone, three replicas included. The
  memory is index and filter blocks; with `cache_index_and_filter_blocks` they
  move into the block cache and memory becomes a fixed budget, at the price of
  reading them back on a cache miss.
- fjall reads its deeper index and filter blocks through the cache it is given,
  so its memory does not grow with sessions; it even fell, because the smaller
  database paid 91 MiB at open replaying a journal it had not yet discarded.
  redb reads every page through its cache by design.
- **Loading is where redb falls furthest behind**: random inserts into a B-tree
  larger than its cache. Ten million sessions took RocksDB 21 s and fjall 30 s;
  redb had written 4.3 GB of its file after 33 minutes and was stopped. A log
  pod rebuilding a replica pays the same.

### 6. Queue churn

Ten minutes per engine: 10,000 messages a second for 50,000 offline sessions,
each message a 256 B `msg/{seq}` and a `q/{cid}/{seq}` entry, durable through
group commit; every session reconnects after 5 to 60 s, scans its queue and
range-deletes it; once a second, message bodies below the oldest queued
sequence number are range-deleted in all 256 partitions. About 200,000
messages stay queued at any time. Latency in milliseconds.

| Engine | Write p50 | p99 | p99.9 | max | Seconds with a write over 100 ms | Drain p99 | Scan p99 | Collection p99 | Disk, end / peak MiB | CPU cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fjall | 4.3 | 69.2 | 269 | 609 | 62 | 61.1 | 0.16 | 125 | 570 / 584 | 0.24 |
| RocksDB | 5.7 | 22.1 | 588 | 950 | 5 | 22.0 | 2.8 | 22.9 | 271 / 309 | 0.52 |
| redb | 48.3 | 472 | 1,079 | 1,347 | 351 | 273 | 0.03 | 696 | 346 / 346 | 0.40 |

- **RocksDB has the steadier write path**: 5 seconds of the 600 had a write
  over 100 ms, against 62 for fjall and 351 for redb. RocksDB's worst was one
  burst around 100 s (950 ms), on a machine another job was sharing; fjall's
  were spread through the run, its 100 ms backpressure sleeps again; redb kept
  pace only at a p50 of 48 ms and a p99 of 472 ms.
- **Space is reclaimed by all three**, and none grew over the ten minutes:
  RocksDB held 170 to 310 MiB for about 200,000 live messages, fjall 215 to 585
  MiB, redb grew its file to 346 MiB and reused it.
- **Range tombstones cost RocksDB's readers a little**: a queue scan's p99 was
  2.8 ms against 0.16 ms in fjall, because every read that crosses a range
  tombstone checks it until compaction drops it. Draining and collecting were
  three to five times faster in RocksDB (drain p99 22 against 61 ms,
  collection 23 against 125 ms): each is one tombstone, where fjall reads the
  range and writes a tombstone per key.

### 7. Coming back

Each engine was filled with 10 GiB (a million sessions, Raft entries for a tenth
of the bytes and message bodies for the rest, 1 KiB each, synced every MiB),
closed, and opened again; then a writer synced 1 KiB entries into it for 10 s and
was killed with `SIGKILL`, and it was opened again. Ready means it has served a
read and a durable write. Replay is reading a million claim entries back from
the log in order and applying each to `own`, the work a replica does between its
last applied index and the end of its log. Seconds.

| Engine | Load 10 GiB | On disk | Ready after a clean stop | Ready after `SIGKILL` | Replay of 10^6 entries |
| --- | --- | --- | --- | --- | --- |
| fjall | 158 | 10.4 GiB | 0.08 | 0.49 (two journals replayed) | 3.1 (319,000 a second) |
| RocksDB | 110 | 10.4 GiB | 0.09 | 0.25 (write-ahead log replayed) | 2.1 (471,000 a second) |
| redb | 342 | 18.1 GiB (9.5 GiB of it free pages) | 0.01 | 1,067 (full repair) | 10.4 (97,000 a second) |
| redb, quick repair, 2 GiB | 79 | 3.9 GiB | | 0.03 | |
| RocksDB, 50 GiB | 1,102 (synced every 16 MiB) | 51.0 GiB | 0.13 | 0.41 | |

- **The LSM engines are back in under half a second after `SIGKILL`**: they
  replay what their journal or write-ahead log holds beyond the last flushed
  memtable, which is bounded by the memtable size, not by the data. RocksDB at
  50 GiB was ready in 0.13 s after a clean stop and 0.41 s after `SIGKILL`,
  holding 320 MB of index and filter blocks and 66 GB of compaction still to
  do.
- **redb took 18 minutes.** After an unclean stop it walks the whole database to
  rebuild its allocator state and check its checksums, so the time grows with
  the data. Quick repair saves the allocator state with every commit and made
  reopening instant; it does so by committing in two phases, two flushes per
  commit instead of one, a cost the write measurements above did not include.
- **Replaying the log is quick**: a million entries in 2 to 3 s for the LSM
  engines, 10 s for redb. A replica restarting with a few seconds of a storm's
  entries unapplied catches up in well under a second.
- redb's file held 18 GiB for 10 GiB of data straight after loading: freed pages
  stay in the file for reuse until a compaction.

### 8. Replication

Three replicas of every group, group `g` led by node `g mod 3`, writes spread
over the groups, one-way delay 1 or 2 ms, the modelled flush 1 ms. Each cell is
commit latency p50 / p99 in milliseconds, from when a write was due to when its
leader answered it applied. Five schemes ran on the same delay line, disks and
runtime: openraft 0.9.25 as it is; 0.9.25 behind a proposer that batches waiting
writes into one entry; openraft 0.10.0-alpha.36 with its default network, which
sends one append and waits for its answer; 0.10 with appends pipelined through
`stream_append`, which is what a transport over QUIC streams would do; and the
primary-backup scheme.

| Groups | Writes | openraft 0.9 | 0.9, batching proposer | 0.10, sequential | 0.10, pipelined | primary-backup |
| --- | --- | --- | --- | --- | --- | --- |
| 1 ms one way | | | | | | |
| 1 | 1,000/s | lost its leader | 9.8 / 30.6 | 4.8 / 14.7 | 3.6 / 4.3 | 3.6 / 5.8 |
| 1 | 20,000/s | lost its leader | 8.6 / 12.5 | 4.5 / 10.9 | 3.6 / 18.8 | 3.5 / 4.2 |
| 16 | 1,000/s | 4.7 / 9.8 | 4.8 / 10.1 | 3.2 / 9.6 | 3.2 / 9.8 | 3.1 / 3.2 |
| 16 | 20,000/s | lost all 16 leaders | 10.4 / 14.6 | 5.4 / 7.7 | 3.5 / 5.8 | 3.4 / 4.0 |
| 128 | 1,000/s | 4.6 / 6.6 | 4.6 / 6.5 | 3.2 / 5.2 | 3.2 / 3.4 | 3.2 / 3.3 |
| 128 | 20,000/s | 6.4 / 17.7 | 6.6 / 12.8 | 4.6 / 9.1 | 3.6 / 4.1 | 3.4 / 4.0 |
| 2 ms one way | | | | | | |
| 1 | 1,000/s | lost its leader | 15.5 / 33.8 | 7.2 / 13.8 | 5.6 / 6.9 | 5.6 / 24.9 |
| 1 | 20,000/s | lost its leader | 15.6 / 21.9 | 7.0 / 28.4 | 5.7 / 12.5 | 5.6 / 6.2 |
| 16 | 1,000/s | 7.1 / 13.2 | 7.2 / 16.0 | 5.3 / 10.2 | 5.2 / 17.9 | 5.1 / 6.1 |
| 16 | 20,000/s | lost all 16 leaders | 15.1 / 22.3 | 7.1 / 10.4 | 5.6 / 6.9 | 5.5 / 6.1 |
| 128 | 1,000/s | 6.8 / 10.6 | 6.8 / 10.9 | 5.3 / 11.4 | 5.3 / 19.5 | 5.2 / 6.2 |
| 128 | 20,000/s | 10.2 / 22.2 | 12.6 / 22.0 | 6.8 / 10.4 | 5.7 / 10.0 | 5.5 / 6.1 |

**The storm.** 50,000 writes a second across the cluster, 1 ms one way, 1 ms
flush: p50 / p99 / p99.9 in milliseconds, and the CPU of all three nodes
(in-process, without the delay line's thread).

| Groups | openraft 0.9 | 0.9, batching proposer | 0.10, sequential | 0.10, pipelined | primary-backup |
| --- | --- | --- | --- | --- | --- |
| 16 | lost all 16 leaders | 11.9 / 16.7 / 17.4, 1.2 cores | 6.0 / 9.2 / 9.8, 1.6 cores | 3.6 / 4.2 / 5.4, 4.2 cores | 3.5 / 4.1 / 6.1, 0.9 cores |
| 128 | lost 120 of 128 leaders | 9.6 / 14.4 / 15.0, 2.8 cores | 5.1 / 7.8 / 10.7, 4.1 cores | 4.0 / 5.1 / 6.2, 7.0 cores | 3.5 / 4.0 / 4.1, 0.9 cores |
| 256 | 7.3 / 24.4 / 38.0, 3.5 cores | 7.7 / 13.7 / 15.1, 4.3 cores | 4.9 / 8.1 / 20.1, 6.0 cores | 4.2 / 9.1 / 15.5, 7.6 cores | 3.5 / 4.1 / 4.2, 0.9 cores |

**One partition leader at its limit.** One group, writes always outstanding,
1 ms one way: writes a second, then p50 / p99 in milliseconds.

| Flush | Outstanding | openraft 0.9 | 0.9, batching proposer | 0.10, sequential | 0.10, pipelined | primary-backup |
| --- | --- | --- | --- | --- | --- | --- |
| 0 ms | 16 | 3.0k/s, 5.2 / 7.9 | 2.0k/s, 7.7 / 10.6 | 3.5k/s, 4.3 / 5.3 | 6.1k/s, 2.6 / 2.9 | 6.4k/s, 2.5 / 2.8 |
| 0 ms | 256 | 49k/s, 5.2 / 7.8 | 35k/s, 7.6 / 11.6 | 52k/s, 5.2 / 5.5 | 117k/s, 2.1 / 2.6 | 123k/s, 2.1 / 2.4 |
| 0 ms | 1,024 | 113k/s, 8.0 / 17.3 | 141k/s, 7.5 / 10.7 | 111k/s, 8.4 / 20.1 | 423k/s, 2.3 / 10.0 | 225k/s, 4.6 / 5.3 |
| 1 ms | 16 | 686/s, 23.2 / 26.2 | 1.4k/s, 11.3 / 15.8 | 2.8k/s, 5.8 / 6.7 | 3.6k/s, 4.4 / 5.2 | 3.4k/s, 5.1 / 5.3 |
| 1 ms | 256 | 736/s, 333 / 340 | 25k/s, 9.5 / 15.4 | 45k/s, 5.2 / 8.0 | 52k/s, 5.0 / 5.1 | 59k/s, 4.5 / 4.9 |
| 1 ms | 1,024 | lost its leader | 107k/s, 9.7 / 13.7 | 78k/s, 11.9 / 23.2 | 222k/s, 4.5 / 11.0 | 241k/s, 4.2 / 5.4 |

**Idle.** Every group elected, nothing written, 20 seconds; CPU of the
protocol, then the same per node and per group replica, and the messages a
second the heartbeats cost across the cluster.

| Scheme | Groups | Heartbeat, election ms | Cores, 3 nodes | Per node | us/s per group replica | Messages/s |
| --- | --- | --- | --- | --- | --- | --- |
| openraft 0.9 | 128 | 50, 150 to 300 | 0.15 | 0.05 | 403 | 3,187 |
| openraft 0.9 | 128 | 250, 1,000 to 2,000 | 0.03 | 0.01 | 88 | 678 |
| openraft 0.9 | 256 | 50, 150 to 300 | 0.32 | 0.11 | 416 | 6,374 |
| openraft 0.9 | 256 | 250, 1,000 to 2,000 | 0.05 | 0.02 | 70 | 1,357 |
| openraft 0.10 | 128 | 50, 150 to 300 | 0.83 | 0.28 | 2,162 | 4,642 |
| openraft 0.10 | 128 | 250, 1,000 to 2,000 | 0.18 | 0.06 | 457 | 932 |
| openraft 0.10 | 256 | 50, 150 to 300 | 1.88 | 0.63 | 2,449 | 9,254 |
| openraft 0.10 | 256 | 250, 1,000 to 2,000 | 0.27 | 0.09 | 350 | 1,859 |
| primary-backup | 128 | none (node lease 1 s) | 0.00 | 0.00 | 0.4 | 2 |
| primary-backup | 256 | none (node lease 1 s) | 0.00 | 0.00 | 0.2 | 2 |

A 128-byte request and reply over loopback TCP cost 13.2 us of CPU (RTT 18 us
at p50); QUIC with TLS and protobuf costs more, so in a real cluster 256 groups
heartbeating every 250 ms add at least 0.01 cores per node for the transport on
top of the table, and every 50 ms at least 0.04.

- **openraft 0.9.25 cannot hold leadership once a flush takes a millisecond.**
  Its core awaits every log flush before it handles anything else
  (`append_to_log` in `src/core/raft_core.rs` waits on the flush callback) and
  appends one client write per flush. One group at 1,000 writes a second, 16
  groups at 20,000, or 128 groups in a storm starve the heartbeats until
  followers stand for election, and every write after that fails. A proposer
  that batches waiting writes into one entry keeps 0.9 stable, at 1.8 to 2.7
  times the latency of pipelined 0.10.
- **openraft 0.10 does not block on the flush** ("submit IO request, do not
  wait for the response") and batches queued writes into one append. With the
  default network it commits in about 1.5 round trips; with appends pipelined
  it commits in one round trip plus one flush, like primary-backup.
- **At p50, pipelined openraft 0.10 and primary-backup cannot be told apart**:
  3.2 to 3.6 ms at 1 ms one way and 5.2 to 5.7 ms at 2 ms, and 4.2 against
  3.5 ms in the storm across 256 groups. One partition leader commits 52,000 to
  59,000 writes a second with 256 outstanding, against about 200 a second per
  partition in a storm.
- **Primary-backup is cheaper and has the tighter tail.** In the storm it used
  0.9 cores for three nodes against 7.6 for pipelined openraft 0.10 (2.5 per
  node, 50 us per write per node), and its p99 stayed at 4 ms where openraft's
  reached 5 to 9. Idle, it costs nothing, against 0.09 cores per node for 256
  groups at a 250 ms heartbeat (0.63 at 50 ms). Some of openraft's CPU is the
  spike's pipelining plumbing (the sequential variant used 6.0 cores, not 7.6),
  and none of it is serialisation or sockets.
- **Memory**: 256 groups on three nodes held 155 MiB resident (300 MiB
  footprint) under openraft 0.10, about 0.4 MiB per group replica.

### 9. Build friction

The smallest program that opens each engine, writes one key durably and reads
it back, built from an empty target directory once per engine, with crates
already downloaded; `none` is the same program without one. Release builds use
the image's settings (stripped, no debug information).

| Engine | Clean release build, macOS, 10 cores | Added to the stripped binary | Clean debug build | Release build in `rust:1.99.0-bookworm` |
| --- | --- | --- | --- | --- |
| none | 0.6 s | | 0.2 s | 1.0 s |
| fjall | 7.0 s | 1.1 MiB | 4.6 s | 6.4 s |
| redb | 5.0 s | 0.7 MiB | 2.9 s | 4.4 s |
| RocksDB | 88.7 s | 5.9 MiB | 96.5 s | fails without `libclang-dev`; 706 s with it, 7.2 MiB added |
| openraft 0.9 | 10.8 s | 0.5 MiB | 8.3 s | 13.2 s |

The image builds natively for arm64 here, inside Docker Desktop's Linux VM,
which other containers were sharing at the time; amd64 was not built.
`rust:1.99.0-bookworm` has gcc but no `libclang`, so RocksDB's build script stops
("Unable to find libclang") until the build stage installs `libclang-dev`. The
RocksDB probe built there then ran in `gcr.io/distroless/cc-debian12:nonroot`,
which carries `libstdc++.so.6` and `libgcc_s.so.1`.

- **RocksDB is the only one that changes the build**: a minute and a half of C++
  on ten cores natively and twelve minutes in the shared Docker VM, where the
  pure-Rust engines take seconds; 6 to 7 MiB more binary; `libclang` wherever
  it is built, because rust-rocksdb generates its bindings at build time; and
  `libstdc++` at run time, which the runtime image already has. A cached
  `target/` in CI pays it once per RocksDB version, not per change.
- openraft 0.9 compiles in 11 to 13 s and adds half a MiB, and it depends on
  `clap` with the `env` feature, which feature unification would switch on for
  the broker's own command line, against the rule that only `openqtt-config`
  reads the environment. 0.10 makes `clap` optional; with its default features
  off, the spike's build has no `env` feature from it.

## Decision

### Engine: RocksDB

- **It is the fastest of the three where the log role works hardest.** At a
  node's share of a storm it applied claims four times faster than the cluster
  needs at a p99 of 12 ms, and its durable writes had the lowest or equal-lowest
  tail at every load (20 ms p99 at 50,000 entries a second of 128 B, against
  44 for fjall).
- **It is the smallest on disk, with fjall**: 108 bytes an idle session against
  112 for fjall and 339 for redb, so 30 GiB for 10^8 sessions at three
  replicas. It is the only one whose memory grows with sessions (4 bytes each,
  index and filters), which a cache setting turns into a fixed budget.
- **It stalls least.** Over ten minutes of queue churn it had 5 seconds with a
  write over 100 ms, against 62 for fjall and 351 for redb, and its write p99
  was 22 ms against 69 and 472.
- **Range deletes are native.** Draining an offline queue and collecting
  message bodies below the lowest cursor are one range tombstone each, three to
  five times faster than fjall's read-then-tombstone-each-key; the price is a
  slower scan across fresh tombstones (2.8 ms p99 against 0.16).
- **It comes back fast at any size measured**: ready 0.09 s after a clean stop
  and 0.25 s after `SIGKILL` with 10 GiB, 0.13 s and 0.41 s with 50 GiB, and it
  replays a million log entries in 2.1 s. redb needed 18 minutes after
  `SIGKILL` at 10 GiB.
- **The price is the build.** A clean build compiles RocksDB's C++ for a
  minute and a half on ten cores (twelve minutes in the shared Docker VM), the
  build image needs `libclang-dev`, and the binary grows by 6 to 7 MiB and links
  `libstdc++`, which `distroless/cc-debian12` provides.
- **Why not fjall**, which is close behind and pure Rust: its tail at sustained
  write rates comes from a 100 ms sleep loop in its write path when flushes fall
  behind, its range deletes are per key, and it is young (first published in
  December 2023, 3.0 in January 2026) where RocksDB has run production databases
  for over a decade. It is the fallback if the C++ build becomes a problem,
  behind the same `KvEngine` trait.
- **Why not redb**: copy-on-write makes every commit rewrite a path of pages, so
  it writes 18 to 125 times the logical bytes on small batches, falls behind
  between 25,000 and 50,000 claims a second, had not finished loading ten
  million sessions after 33 minutes where RocksDB took 21 s, and took 18 minutes
  to reopen 10 GiB after `SIGKILL` unless quick repair doubles the flushes of
  every commit.

### Replication: openraft 0.10, one group per partition

- **Same latency as the simpler scheme.** Primary-backup's only real advantage
  was that its lanes pipelined; given a pipelined transport, openraft commits in
  one round trip plus one flush, as primary-backup does, and one partition
  leader commits 52,000 writes a second, 250 times its share of a storm.
- **What primary-backup still wins is CPU**: an eighth of openraft's in the
  storm and nothing at idle. Against that, primary-backup's missing half is
  promotion after a failure: fencing the old primary's epoch in the meta group,
  finding the backup holding every committed entry, truncating and catching up
  the other, and doing the same for membership changes and snapshots. That is a
  consensus protocol, and Raft is the proven one; R3 already puts a Raft group
  (the meta group) under the cluster. The CPU is a cost to manage, not a reason
  to own that protocol: 0.09 cores per node idle for 256 groups at a 250 ms
  heartbeat, and 2.5 cores per node across three log nodes in a storm, which
  divides by the number of log nodes and can be cut by batching claims per
  partition into one entry.
- **Not openraft 0.9.** Its core waits for every log flush before it does
  anything else and appends one client write per flush, so a millisecond of
  flush is enough for one group at 1,000 writes a second to lose its leader. A
  batching proposer keeps it alive at 1.8 to 2.7 times the latency, and any
  flush stall still stops that group's heartbeats.
- **0.10 is an alpha** (0.10.0-alpha.36 on 2026-09-29, after 0.9.25 on
  2026-07-28). `openqtt-log` pins the exact version and keeps openraft's types
  behind its own, so the move to 0.10.0 touches one crate; if 0.10 stalls, 0.9
  with a batching proposer is the measured fallback.
- **Timing**: heartbeat 250 ms, election timeout 1 to 2 s. That keeps R3's
  "connects pause for one to two seconds" when a log pod dies, and costs a
  fifth to a seventh of the default's idle CPU.

### Group commit: no window

On every load measured, a window added its own length to an idle write and
nothing to throughput: a flush already takes longer than the window, so the next
batch has gathered by the time the writer is free. A batch needs a cap so one
flush cannot grow without bound (the spike capped requests at 8,192 to 16,384;
the log role should cap bytes). A deployment whose disk flushes much faster than
requests arrive, and charges per operation, wants a cap on flushes a second
instead of a window.

### One fsync, shared

The state machine writes after the entry is durable, without a sync of its own,
and its writes reach disk with the next log flush. After a crash RocksDB replays
its write-ahead log, which holds both, and openraft replays log entries above the
last applied index: with 10 GiB, RocksDB was ready 0.25 s after `SIGKILL`, and
it replays a million entries in 2.1 s.

## The data model, confirmed and corrected

R3's keys hold, with three corrections that come from putting every partition a
node replicates into one engine instance, which group commit across partitions
needs.

1. **Every key starts with its partition**, two bytes big-endian, then a one-byte
   tag instead of a prefix string: `{p}o{cid}` for `own/{cid}`, `{p}s{cid}`,
   `{p}m{seq}`, `{p}q{cid}...`. A partition is then one contiguous key range in
   each keyspace, so snapshotting, moving or dropping a partition is a range
   scan or a range delete, and the tag saves three bytes a key.
2. **A client id inside a key ends with a zero byte.** `q/{cid}/{seq}` as
   written lets the range of client `a` cover client `a/b` (a slash is legal in
   a client identifier). The key is `{p}q{cid}\x00{seq}`, and the same for
   `infl` and `rel`. A zero byte cannot occur in a client id, because an MQTT
   UTF-8 string must not contain U+0000 [MQTT-1.5.4-2].
3. **The Raft log is a second keyspace of the same engine**, keyed
   `{group}{index}` (4 and 8 bytes, big-endian), not a separate store. That is
   what lets one fsync cover the log entries of every partition on the node and
   the state machine writes that ride along. Purging the log is a range delete.

Confirmed:

- `own/{cid}` for every client, with the will and the expiry in it, is
  affordable: with `sess` beside it, 108 bytes on disk per session in RocksDB,
  half the logical size, and 4 bytes of memory.
- `msg/{seq}` once per partition with `q/{cid}/{seq}` pointing at it works, and
  collecting message bodies "below the lowest session cursor" is one range
  delete per partition per collection pass. That is cheap only where range
  deletes are cheap: one range tombstone each in RocksDB (a collection pass over
  256 partitions took 23 ms at p99), a read and a tombstone per message in fjall
  (125 ms).
- Values as protobuf: a claim's Raft entry is about 55 bytes and `own` 32 bytes,
  so the engine handles hundreds of thousands of small values a second and
  per-value overhead, not bandwidth, decides its cost.

## What committed means before a PUBACK

R3 says a PUBACK waits until every partition holding durable interest has
committed, and that committed means fsynced by a majority of the partition's
replicas. The spike makes that precise:

1. **Durable on one replica** means the entry is in the replica's log keyspace
   and a flush that started after it was written has completed: `fdatasync` on
   Linux, `F_FULLFSYNC` on macOS. Group commit shares that flush among every
   entry waiting on the node, from every partition, so durability costs one
   flush of latency, not one flush per entry.
2. **Committed** means durable on a majority of the partition's replicas: two of
   three, or the only one at replication factor 1. The leader counts its own
   copy only once its own flush completes; with two followers acknowledging, an
   entry may commit before the leader's flush ends.
3. **The state machine needs no flush of its own.** Its writes go to the same
   engine without a sync and become durable with the next log flush; after a
   crash they are rebuilt by replaying the log from the last applied index. The
   PUBACK never waits for them.
4. **A PUBACK (or PUBREC) is sent** when every durable destination's partition
   has answered committed and the retained write, if any, has too. A leader that
   dies before answering produces no PUBACK, and the client resends with DUP.
5. **A leader that has lost its epoch cannot commit.** Raft's term does this per
   group; a primary-backup scheme needs the epoch from the meta group on every
   append, refused when stale.

## Open risks

- **The disk is not the target disk.** Every latency here sits on a laptop SSD's
  `F_FULLFSYNC` (1 to 6 ms at p50, moving with whatever else flushes). The shape
  holds on a network block device or local NVMe; the absolute commit latency,
  and so the claim latency during a storm, has to be measured again on the
  cluster's disks in M3 with the same `fsync` step.
- **openraft 0.10 is an alpha**, and the decision leans on what 0.10 changed:
  log IO that does not block the core, batched appends, pipelined
  `stream_append`. Pin it exactly, wrap it in `openqtt-log`, and track 0.10.0;
  0.9 with a batching proposer is the fallback, measured at 1.8 to 2.7 times the
  latency.
- **openraft's CPU per write is high in this spike**: about 50 us per write per
  node in the storm, eight times primary-backup's. Some of it is the spike's
  in-memory log and plumbing; how much is openraft has to be profiled in M3 on
  the real transport. Batching claims per partition into one entry is the
  lever if it matters.
- **One machine played three nodes.** Replication shared one runtime, one CPU and
  a modelled disk. Real replicas add serialisation, QUIC with TLS, and three
  independent disks; the loopback figure bounds the first two from below only.
- **Failover was not measured.** Neither election time after a leader dies nor
  how long writes pause. R5 owns that, with the chaos suite.
- **Snapshots and replica moves were not measured.** Adding a replica, moving a
  partition between nodes and installing a snapshot of a large partition all
  stream state; their cost per GiB is unknown. RocksDB checkpoints (hard links
  to its immutable files) are the natural snapshot and were not tried.
- **Ten minutes is short for an LSM.** Compaction debt from a day of churn, and
  space amplification at the bottom level once it fills, need a soak (M6). The
  RocksDB database loaded for recovery still had 20 GB of pending compaction
  when it was reopened.
- **Range tombstones slow reads that cross them** until compaction removes them
  (a queue scan's p99 was 2.8 ms against 0.16 ms in fjall). A partition whose
  queues drain constantly should be watched for scan latency.
- **The C++ build**: RocksDB adds a minute and a half to twelve minutes to every
  clean build and needs `libclang` wherever it is built; CI caches matter, and
  the 12-minute bound on `make check` in CI has to hold with it.
- **A macOS build of rust-rocksdb is not durable** unless compiled with
  `-DHAVE_FULLFSYNC`; anyone measuring durability on a Mac needs the spike's
  `.cargo/config.toml`. Linux, where the broker runs, is unaffected.
- **The extrapolation to 10^8 and 10^9 sessions assumes** the measured cost per
  session stays constant and client ids look like the spike's (16 to 64 random
  characters). Long shared prefixes compress better; random binary ids worse.

## Appendix: every run

Generated by `spikes/s1-log/summarize.py --appendix` from the results file.
Latencies are in milliseconds unless a column says otherwise. `closed:N` keeps N
requests outstanding; `open:R/s` sends R a second on schedule and measures from
when each was due. Replication schemes: `raft` is openraft 0.9.25, `raft-batched`
the same behind a batching proposer, `raft10-seq` openraft 0.10.0-alpha.36 with
its default sequential appends, `raft10` the same with pipelined appends, `pb`
primary-backup. Replication CPU is the protocol's, without the delay line's
thread, for all three nodes together.

### The durability floor, both passes

| Pass | Primitive | File | Size | Threads | Syncs/s | p50 ms | p99 ms | Load average |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | std sync_data | append | 128 | 1 | 200 | 5.0 | 8.0 | 6.27 |
| 1 | std sync_data | append | 128 | 3 | 304 | 10.0 | 16.6 | 6.17 |
| 1 | std sync_data | append | 4096 | 1 | 196 | 5.0 | 8.1 | 5.99 |
| 1 | std sync_data | append | 4096 | 3 | 295 | 10.1 | 17.0 | 6.55 |
| 1 | std sync_data | append | 65536 | 1 | 199 | 5.0 | 8.0 | 6.51 |
| 1 | std sync_data | append | 65536 | 3 | 315 | 9.4 | 16.1 | 6.07 |
| 1 | std sync_data | append | 1048576 | 1 | 179 | 5.4 | 8.4 | 7.26 |
| 1 | std sync_data | append | 1048576 | 3 | 347 | 7.5 | 18.7 | 8.04 |
| 1 | fsync | append | 128 | 1 | 11617 | 0.04 | 0.70 | 8.28 |
| 1 | fsync | append | 128 | 3 | 23964 | 0.05 | 1.2 | 7.86 |
| 1 | fsync | append | 4096 | 1 | 8392 | 0.04 | 1.1 | 7.87 |
| 1 | fsync | append | 4096 | 3 | 13096 | 0.06 | 1.8 | 7.96 |
| 1 | fsync | append | 65536 | 1 | 7223 | 0.05 | 1.1 | 7.8 |
| 1 | fsync | append | 65536 | 3 | 17788 | 0.07 | 1.7 | 7.82 |
| 1 | fsync | append | 1048576 | 1 | 1220 | 0.33 | 6.8 | 7.91 |
| 1 | fsync | append | 1048576 | 3 | 1235 | 0.44 | 24.1 | 8.72 |
| 1 | F_FULLFSYNC | append | 128 | 1 | 130 | 5.0 | 39.8 | 9.06 |
| 1 | F_FULLFSYNC | append | 128 | 3 | 287 | 10.2 | 20.3 | 8.82 |
| 1 | F_FULLFSYNC | append | 4096 | 1 | 187 | 5.1 | 9.0 | 8.27 |
| 1 | F_FULLFSYNC | append | 4096 | 3 | 307 | 9.9 | 17.8 | 7.85 |
| 1 | F_FULLFSYNC | append | 65536 | 1 | 183 | 5.2 | 9.0 | 8.34 |
| 1 | F_FULLFSYNC | append | 65536 | 3 | 324 | 9.0 | 16.1 | 7.91 |
| 1 | F_FULLFSYNC | append | 1048576 | 1 | 162 | 6.0 | 11.8 | 8.4 |
| 1 | F_FULLFSYNC | append | 1048576 | 3 | 382 | 6.1 | 20.0 | 8.05 |
| 1 | F_BARRIERFSYNC | append | 128 | 1 | 714 | 1.3 | 4.0 | 7.96 |
| 1 | F_BARRIERFSYNC | append | 128 | 3 | 1557 | 1.9 | 3.5 | 8.37 |
| 1 | F_BARRIERFSYNC | append | 4096 | 1 | 786 | 1.1 | 4.0 | 9.62 |
| 1 | F_BARRIERFSYNC | append | 4096 | 3 | 1666 | 1.7 | 4.8 | 10.13 |
| 1 | F_BARRIERFSYNC | append | 65536 | 1 | 837 | 0.93 | 5.1 | 9.96 |
| 1 | F_BARRIERFSYNC | append | 65536 | 3 | 2029 | 1.4 | 3.9 | 9.8 |
| 1 | F_BARRIERFSYNC | append | 1048576 | 1 | 591 | 1.3 | 4.9 | 10.06 |
| 1 | F_BARRIERFSYNC | append | 1048576 | 3 | 450 | 4.5 | 23.9 | 11.97 |
| 1 | std sync_data | prealloc | 128 | 1 | 131 | 6.8 | 10.5 | 11.33 |
| 1 | std sync_data | prealloc | 128 | 3 | 239 | 10.1 | 20.6 | 11.31 |
| 1 | std sync_data | prealloc | 4096 | 1 | 134 | 7.0 | 12.2 | 10.8 |
| 1 | std sync_data | prealloc | 4096 | 3 | 214 | 10.1 | 39.5 | 9.94 |
| 1 | std sync_data | prealloc | 65536 | 1 | 122 | 7.3 | 18.6 | 9.46 |
| 1 | std sync_data | prealloc | 65536 | 3 | 266 | 9.1 | 17.9 | 8.78 |
| 1 | std sync_data | prealloc | 1048576 | 1 | 102 | 9.0 | 16.9 | 8.32 |
| 1 | std sync_data | prealloc | 1048576 | 3 | 181 | 10.1 | 53.7 | 7.97 |
| 1 | fsync | prealloc | 128 | 1 | 7313 | 0.04 | 1.5 | 8.22 |
| 1 | fsync | prealloc | 128 | 3 | 14862 | 0.05 | 2.1 | 14.69 |
| 1 | fsync | prealloc | 4096 | 1 | 6206 | 0.04 | 2.1 | 15.43 |
| 1 | fsync | prealloc | 4096 | 3 | 3748 | 0.06 | 7.8 | 15.0 |
| 1 | fsync | prealloc | 65536 | 1 | 7895 | 0.05 | 1.2 | 20.04 |
| 1 | fsync | prealloc | 65536 | 3 | 8014 | 0.06 | 1.7 | 18.6 |
| 1 | fsync | prealloc | 1048576 | 1 | 1412 | 0.30 | 6.1 | 23.43 |
| 1 | fsync | prealloc | 1048576 | 3 | 1334 | 0.40 | 23.4 | 22.44 |
| 1 | F_FULLFSYNC | prealloc | 128 | 1 | 101 | 6.1 | 53.6 | 21.92 |
| 1 | F_FULLFSYNC | prealloc | 128 | 3 | 172 | 11.2 | 59.8 | 20.65 |
| 1 | F_FULLFSYNC | prealloc | 4096 | 1 | 120 | 7.4 | 19.7 | 19.55 |
| 1 | F_FULLFSYNC | prealloc | 4096 | 3 | 235 | 10.0 | 21.1 | 18.31 |
| 1 | F_FULLFSYNC | prealloc | 65536 | 1 | 134 | 6.9 | 13.2 | 17.08 |
| 1 | F_FULLFSYNC | prealloc | 65536 | 3 | 243 | 10.0 | 20.0 | 16.27 |
| 1 | F_FULLFSYNC | prealloc | 1048576 | 1 | 97 | 8.9 | 22.4 | 15.61 |
| 1 | F_FULLFSYNC | prealloc | 1048576 | 3 | 219 | 9.1 | 35.7 | 14.44 |
| 1 | F_BARRIERFSYNC | prealloc | 128 | 1 | 655 | 1.2 | 7.1 | 13.76 |
| 1 | F_BARRIERFSYNC | prealloc | 128 | 3 | 1824 | 1.1 | 9.8 | 14.26 |
| 1 | F_BARRIERFSYNC | prealloc | 4096 | 1 | 870 | 0.98 | 3.5 | 14.48 |
| 1 | F_BARRIERFSYNC | prealloc | 4096 | 3 | 1933 | 1.1 | 6.0 | 13.96 |
| 1 | F_BARRIERFSYNC | prealloc | 65536 | 1 | 751 | 1.0 | 4.8 | 13.57 |
| 1 | F_BARRIERFSYNC | prealloc | 65536 | 3 | 1402 | 1.0 | 11.0 | 12.56 |
| 1 | F_BARRIERFSYNC | prealloc | 1048576 | 1 | 399 | 1.7 | 7.7 | 12.75 |
| 1 | F_BARRIERFSYNC | prealloc | 1048576 | 3 | 389 | 4.5 | 65.2 | 11.97 |
| 2 | std sync_data | append | 128 | 1 | 481 | 2.2 | 3.1 | 15.69 |
| 2 | std sync_data | append | 128 | 3 | 420 | 7.1 | 12.1 | 14.75 |
| 2 | std sync_data | append | 4096 | 1 | 584 | 1.2 | 3.1 | 13.81 |
| 2 | std sync_data | append | 4096 | 3 | 406 | 7.3 | 12.0 | 13.03 |
| 2 | std sync_data | append | 65536 | 1 | 423 | 2.9 | 4.0 | 12.14 |
| 2 | std sync_data | append | 65536 | 3 | 391 | 7.9 | 12.4 | 11.57 |
| 2 | std sync_data | append | 1048576 | 1 | 391 | 2.4 | 4.2 | 10.88 |
| 2 | std sync_data | append | 1048576 | 3 | 359 | 8.9 | 14.1 | 10.49 |
| 2 | fsync | append | 128 | 1 | 39204 | 0.03 | 0.04 | 9.89 |
| 2 | fsync | append | 128 | 3 | 94755 | 0.03 | 0.06 | 9.42 |
| 2 | fsync | append | 4096 | 1 | 45936 | 0.02 | 0.04 | 8.83 |
| 2 | fsync | append | 4096 | 3 | 108101 | 0.03 | 0.06 | 9.32 |
| 2 | fsync | append | 65536 | 1 | 31758 | 0.03 | 0.04 | 8.81 |
| 2 | fsync | append | 65536 | 3 | 68728 | 0.04 | 0.08 | 8.59 |
| 2 | fsync | append | 1048576 | 1 | 2760 | 0.24 | 0.32 | 7.98 |
| 2 | fsync | append | 1048576 | 3 | 7078 | 0.39 | 1.2 | 7.42 |
| 2 | F_FULLFSYNC | append | 128 | 1 | 214 | 4.0 | 13.5 | 6.83 |
| 2 | F_FULLFSYNC | append | 128 | 3 | 255 | 12.0 | 18.1 | 6.36 |
| 2 | F_FULLFSYNC | append | 4096 | 1 | 245 | 4.0 | 7.2 | 6.09 |
| 2 | F_FULLFSYNC | append | 4096 | 3 | 254 | 12.0 | 18.3 | 6.24 |
| 2 | F_FULLFSYNC | append | 65536 | 1 | 243 | 4.0 | 7.3 | 5.82 |
| 2 | F_FULLFSYNC | append | 65536 | 3 | 251 | 12.0 | 19.1 | 5.52 |
| 2 | F_FULLFSYNC | append | 1048576 | 1 | 225 | 4.0 | 8.2 | 5.07 |
| 2 | F_FULLFSYNC | append | 1048576 | 3 | 249 | 12.0 | 23.0 | 4.83 |
| 2 | F_BARRIERFSYNC | append | 128 | 1 | 6418 | 0.13 | 0.40 | 4.6 |
| 2 | F_BARRIERFSYNC | append | 128 | 3 | 5022 | 0.45 | 4.3 | 5.91 |
| 2 | F_BARRIERFSYNC | append | 4096 | 1 | 5332 | 0.14 | 1.1 | 5.84 |
| 2 | F_BARRIERFSYNC | append | 4096 | 3 | 6038 | 0.46 | 0.83 | 5.61 |
| 2 | F_BARRIERFSYNC | append | 65536 | 1 | 4388 | 0.17 | 0.82 | 5.56 |
| 2 | F_BARRIERFSYNC | append | 65536 | 3 | 4748 | 0.42 | 3.9 | 5.2 |
| 2 | F_BARRIERFSYNC | append | 1048576 | 1 | 641 | 1.4 | 5.2 | 4.94 |
| 2 | F_BARRIERFSYNC | append | 1048576 | 3 | 572 | 4.7 | 23.0 | 4.71 |
| 2 | std sync_data | prealloc | 128 | 1 | 175 | 4.8 | 15.6 | 4.97 |
| 2 | std sync_data | prealloc | 128 | 3 | 203 | 9.8 | 33.8 | 4.73 |
| 2 | std sync_data | prealloc | 4096 | 1 | 165 | 5.2 | 15.1 | 4.51 |
| 2 | std sync_data | prealloc | 4096 | 3 | 522 | 5.4 | 10.1 | 4.63 |
| 2 | std sync_data | prealloc | 65536 | 1 | 624 | 1.2 | 3.5 | 4.5 |
| 2 | std sync_data | prealloc | 65536 | 3 | 608 | 4.3 | 9.2 | 4.22 |
| 2 | std sync_data | prealloc | 1048576 | 1 | 414 | 2.1 | 6.9 | 4.12 |
| 2 | std sync_data | prealloc | 1048576 | 3 | 236 | 12.1 | 23.9 | 4.03 |
| 2 | fsync | prealloc | 128 | 1 | 40647 | 0.02 | 0.05 | 4.19 |
| 2 | fsync | prealloc | 128 | 3 | 100212 | 0.02 | 0.06 | 4.25 |
| 2 | fsync | prealloc | 4096 | 1 | 43956 | 0.02 | 0.03 | 4.47 |
| 2 | fsync | prealloc | 4096 | 3 | 92564 | 0.03 | 0.07 | 4.6 |
| 2 | fsync | prealloc | 65536 | 1 | 26902 | 0.03 | 0.07 | 6.79 |
| 2 | fsync | prealloc | 65536 | 3 | 61094 | 0.04 | 0.10 | 6.49 |
| 2 | fsync | prealloc | 1048576 | 1 | 2448 | 0.23 | 0.36 | 5.97 |
| 2 | fsync | prealloc | 1048576 | 3 | 6232 | 0.39 | 1.2 | 5.65 |
| 2 | F_FULLFSYNC | prealloc | 128 | 1 | 200 | 4.0 | 12.9 | 5.2 |
| 2 | F_FULLFSYNC | prealloc | 128 | 3 | 333 | 8.0 | 14.0 | 4.94 |
| 2 | F_FULLFSYNC | prealloc | 4096 | 1 | 231 | 4.0 | 7.7 | 5.02 |
| 2 | F_FULLFSYNC | prealloc | 4096 | 3 | 339 | 8.0 | 14.1 | 4.86 |
| 2 | F_FULLFSYNC | prealloc | 65536 | 1 | 234 | 4.0 | 7.3 | 4.79 |
| 2 | F_FULLFSYNC | prealloc | 65536 | 3 | 315 | 8.0 | 15.0 | 4.41 |
| 2 | F_FULLFSYNC | prealloc | 1048576 | 1 | 221 | 4.0 | 8.2 | 4.22 |
| 2 | F_FULLFSYNC | prealloc | 1048576 | 3 | 243 | 12.0 | 20.3 | 4.04 |
| 2 | F_BARRIERFSYNC | prealloc | 128 | 1 | 4818 | 0.15 | 1.0 | 3.71 |
| 2 | F_BARRIERFSYNC | prealloc | 128 | 3 | 7569 | 0.32 | 0.92 | 3.9 |
| 2 | F_BARRIERFSYNC | prealloc | 4096 | 1 | 5577 | 0.15 | 0.44 | 3.74 |
| 2 | F_BARRIERFSYNC | prealloc | 4096 | 3 | 6516 | 0.33 | 1.2 | 3.68 |
| 2 | F_BARRIERFSYNC | prealloc | 65536 | 1 | 2807 | 0.18 | 2.5 | 3.55 |
| 2 | F_BARRIERFSYNC | prealloc | 65536 | 3 | 5186 | 0.42 | 2.6 | 3.5 |
| 2 | F_BARRIERFSYNC | prealloc | 1048576 | 1 | 610 | 1.4 | 5.2 | 3.38 |
| 2 | F_BARRIERFSYNC | prealloc | 1048576 | 3 | 564 | 5.0 | 10.7 | 3.27 |

### Group commit, every window and load (median of three passes)

| Size | Load | Engine | Writes/s | p50 ms | p99 ms | p99.9 ms | p99 range ms | Batch | Write amp |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 128 | closed:1 | fjall | 139 | 6.8 | 17.0 | 27.3 | 12.2 to 22.8 | 1 | 118.2 |
| 128 | closed:1 | redb | 127 | 6.8 | 16.1 | 36.7 | 14.2 to 26.4 | 1 | 589.0 |
| 128 | closed:1 | rocksdb | 147 | 6.1 | 17.0 | 27.3 | 11.1 to 20.7 | 1 | 108.0 |
| 128 | closed:64 | fjall | 7.4k | 8.1 | 23.0 | 41.6 | 22.7 to 30.8 | 64 | 3.0 |
| 128 | closed:64 | redb | 4.6k | 11.0 | 48.4 | 84.4 | 18.3 to 57.4 | 64 | 124.4 |
| 128 | closed:64 | rocksdb | 9.1k | 6.7 | 19.4 | 28.6 | 13.5 to 25.8 | 64 | 2.9 |
| 128 | closed:1024 | fjall | 88.7k | 8.7 | 34.2 | 165 | 16.8 to 39.2 | 1024 | 2.4 |
| 128 | closed:1024 | redb | 56.6k | 16.2 | 38.6 | 62.5 | 37.3 to 52.1 | 1024 | 17.8 |
| 128 | closed:1024 | rocksdb | 116.0k | 7.8 | 21.2 | 37.9 | 12.0 to 25.5 | 1024 | 2.1 |
| 128 | open:10000/s | fjall | 10.0k | 10.2 | 17.8 | 24.5 | 12.3 to 102 | 68 | 2.9 |
| 128 | open:10000/s | redb | 10.0k | 18.0 | 67.8 | 98.9 | 50.8 to 198 | 125 | 105.5 |
| 128 | open:10000/s | rocksdb | 10.0k | 9.5 | 16.4 | 23.2 | 11.0 to 22.2 | 63 | 2.9 |
| 128 | open:50000/s | fjall | 49.9k | 10.5 | 43.9 | 72.8 | 20.6 to 149 | 352 | 2.6 |
| 128 | open:50000/s | redb | 49.9k | 23.7 | 154 | 181 | 67.3 to 229 | 880 | 20.3 |
| 128 | open:50000/s | rocksdb | 49.9k | 9.7 | 20.4 | 26.3 | 14.3 to 55.3 | 321 | 2.4 |
| 1024 | closed:1 | fjall | 146 | 6.3 | 12.3 | 23.3 | 11.0 to 19.6 | 1 | 16.8 |
| 1024 | closed:1 | redb | 127 | 6.9 | 23.2 | 32.1 | 18.3 to 25.7 | 1 | 99.4 |
| 1024 | closed:1 | rocksdb | 145 | 6.8 | 12.2 | 19.8 | 11.4 to 18.6 | 1 | 16.4 |
| 1024 | closed:64 | fjall | 7.3k | 8.0 | 22.4 | 54.6 | 18.1 to 23.9 | 64 | 2.5 |
| 1024 | closed:64 | redb | 4.4k | 12.0 | 35.9 | 70.3 | 29.1 to 47.9 | 64 | 24.1 |
| 1024 | closed:64 | rocksdb | 7.8k | 6.2 | 16.6 | 24.0 | 12.3 to 27.9 | 64 | 2.3 |
| 1024 | closed:1024 | fjall | 41.7k | 20.4 | 76.5 | 147 | 66.2 to 81.3 | 1024 | 3.2 |
| 1024 | closed:1024 | redb | 27.4k | 35.2 | 76.5 | 105 | 69.2 to 121 | 1024 | 3.5 |
| 1024 | closed:1024 | rocksdb | 63.2k | 11.4 | 44.4 | 54.4 | 39.7 to 49.3 | 1024 | 3.0 |
| 1024 | open:10000/s | fjall | 10.0k | 15.4 | 95.7 | 139 | 54.3 to 405 | 103 | 2.2 |
| 1024 | open:10000/s | redb | 10.0k | 30.1 | 136 | 184 | 89.2 to 189 | 201 | 12.8 |
| 1024 | open:10000/s | rocksdb | 10.0k | 11.1 | 25.3 | 43.5 | 20.3 to 44.3 | 77 | 2.0 |
| 1024 | open:50000/s | fjall | 49.5k | 43.0 | 289 | 326 | 275 to 565 | 1104 | 3.0 |
| 1024 | open:50000/s | redb | 13.8k | 4098 | 5620 | 5640 | 5513 to 6513 | 8192 | 2.2 |
| 1024 | open:50000/s | rocksdb | 49.9k | 17.9 | 83.4 | 115 | 62.2 to 92.6 | 579 | 2.6 |

#### Group commit window, 128 B (median of passes)

| Load | Engine | 0 ms p50/p99 | 1 ms p50/p99 | 2 ms p50/p99 |
| --- | --- | --- | --- | --- |
| closed:1 | fjall | 6.8 / 17.0 | 7.0 / 13.1 | 9.9 / 24.3 |
| closed:1 | redb | 6.8 / 16.1 | 7.5 / 16.8 | 9.1 / 19.5 |
| closed:1 | rocksdb | 6.1 / 17.0 | 7.3 / 15.8 | 9.8 / 23.6 |
| closed:64 | fjall | 8.1 / 23.0 | 7.9 / 22.8 | 9.1 / 16.0 |
| closed:64 | redb | 11.0 / 48.4 | 11.1 / 26.6 | 12.7 / 28.0 |
| closed:64 | rocksdb | 6.7 / 19.4 | 7.0 / 12.8 | 9.9 / 16.1 |
| open:10000/s | fjall | 10.2 / 17.8 | 11.2 / 44.1 | 13.1 / 24.4 |
| open:10000/s | redb | 18.0 / 67.8 | 30.3 / 229 | 25.8 / 113 |
| open:10000/s | rocksdb | 9.5 / 16.4 | 11.0 / 41.8 | 12.2 / 81.6 |
| open:50000/s | fjall | 10.5 / 43.9 | 12.3 / 81.5 | 16.1 / 63.4 |
| open:50000/s | redb | 23.7 / 154 | 28.1 / 137 | 35.1 / 103 |
| open:50000/s | rocksdb | 9.7 / 20.4 | 12.9 / 27.7 | 13.3 / 40.1 |

### Shared fsync (median of three passes)

| Load | Shape | Engine | Writes/s | p50 ms | p99 ms | Engine write p50 ms | Write amp |
| --- | --- | --- | --- | --- | --- | --- | --- |
| closed:256 | combined | fjall | 31.1k | 7.4 | 19.4 | 7.3 | 1.9 |
| closed:256 | combined | redb | 9.3k | 24.2 | 66.8 | 24.0 | 129.5 |
| closed:256 | combined | rocksdb | 32.8k | 6.6 | 20.2 | 6.5 | 1.3 |
| closed:256 | log-only | fjall | 35.9k | 6.1 | 18.4 | 6.0 | 2.6 |
| closed:256 | log-only | redb | 17.7k | 13.5 | 31.2 | 13.4 | 96.4 |
| closed:256 | log-only | rocksdb | 42.1k | 5.7 | 14.2 | 5.6 | 1.5 |
| closed:256 | split-lazy | fjall | 30.4k | 7.3 | 17.4 | 7.2 | 1.8 |
| closed:256 | split-lazy | redb | 9.9k | 22.6 | 59.6 | 22.5 | 125.0 |
| closed:256 | split-lazy | rocksdb | 33.3k | 6.9 | 16.8 | 6.8 | 1.3 |
| closed:256 | split-sync | fjall | 19.1k | 12.3 | 25.1 | 12.1 | 1.8 |
| closed:256 | split-sync | redb | 7.7k | 29.2 | 80.0 | 29.0 | 129.1 |
| closed:256 | split-sync | rocksdb | 20.8k | 11.9 | 20.3 | 11.8 | 1.6 |
| open:20000/s | combined | fjall | 20.0k | 10.4 | 27.4 | 6.1 | 2.2 |
| open:20000/s | combined | redb | 19.7k | 81.3 | 286 | 46.6 | 76.6 |
| open:20000/s | combined | rocksdb | 20.0k | 9.2 | 31.7 | 5.5 | 1.7 |
| open:20000/s | log-only | fjall | 20.0k | 9.8 | 32.5 | 6.0 | 2.0 |
| open:20000/s | log-only | redb | 19.9k | 25.2 | 64.6 | 14.0 | 77.7 |
| open:20000/s | log-only | rocksdb | 20.0k | 9.4 | 37.5 | 5.2 | 2.0 |
| open:20000/s | split-lazy | fjall | 20.0k | 10.4 | 32.5 | 6.0 | 2.0 |
| open:20000/s | split-lazy | redb | 19.8k | 82.8 | 164 | 45.6 | 74.4 |
| open:20000/s | split-lazy | rocksdb | 20.0k | 9.5 | 21.2 | 5.9 | 1.7 |
| open:20000/s | split-sync | fjall | 20.0k | 18.8 | 33.3 | 11.8 | 2.2 |
| open:20000/s | split-sync | redb | 19.7k | 92.3 | 277 | 54.9 | 69.1 |
| open:20000/s | split-sync | rocksdb | 20.0k | 18.9 | 40.7 | 11.8 | 1.6 |

### The claim storm

| Engine | Workers | Offered/s | Done/s | p50 ms | p99 ms | p99.9 ms | Read p50 us | Batch | CPU cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fjall | 4 | 10.0k | 10.0k | 9.9 | 50.1 | 68.8 | 9 | 64 | 0.36 |
| fjall | 4 | 25.0k | 25.0k | 9.6 | 25.9 | 35.6 | 7 | 153 | 0.49 |
| fjall | 4 | 50.0k | 49.9k | 12.9 | 131 | 193 | 5 | 418 | 0.78 |
| fjall | 4 | 100.0k | 99.9k | 6.8 | 66.1 | 95.2 | 4 | 444 | 1.03 |
| fjall | 4 | 200.0k | 199.8k | 9.5 | 115 | 133 | 4 | 1218 | 2.0 |
| redb | 4 | 10.0k | 10.0k | 19.8 | 38.0 | 46.9 | 2 | 132 | 0.53 |
| redb | 4 | 25.0k | 24.7k | 109 | 183 | 197 | 3 | 1820 | 0.84 |
| redb | 4 | 50.0k | 43.7k | 763 | 1061 | 1084 | 3 | 16288 | 1.07 |
| redb | 4 | 100.0k | 35.0k | 3873 | 5091 | 5120 | 3 | 16384 | 1.45 |
| rocksdb | 4 | 10.0k | 10.0k | 4.1 | 8.1 | 33.0 | 5 | 25 | 0.15 |
| rocksdb | 4 | 25.0k | 25.0k | 3.4 | 6.1 | 9.1 | 4 | 51 | 0.33 |
| rocksdb | 4 | 50.0k | 50.0k | 3.6 | 10.6 | 29.5 | 4 | 112 | 0.59 |
| rocksdb | 4 | 100.0k | 100.0k | 4.0 | 7.9 | 11.4 | 3 | 249 | 0.89 |
| rocksdb | 4 | 200.0k | 199.9k | 5.5 | 11.7 | 16.0 | 3 | 719 | 1.83 |
| fjall | 1 | 5.0k | 5.0k | 3.9 | 7.3 | 14.0 | 6 | 12 | 0.16 |
| fjall | 1 | 10.0k | 10.0k | 1.8 | 5.9 | 9.1 | 4 | 12 | 0.16 |
| fjall | 1 | 25.0k | 25.0k | 3.4 | 6.2 | 9.4 | 3 | 48 | 0.23 |
| fjall | 1 | 50.0k | 50.0k | 3.9 | 37.4 | 91.7 | 3 | 125 | 0.44 |
| fjall | 1 | 100.0k | 100.0k | 4.4 | 58.5 | 83.0 | 3 | 280 | 0.75 |
| redb | 1 | 5.0k | 5.0k | 8.5 | 25.4 | 44.1 | 2 | 28 | 0.39 |
| redb | 1 | 10.0k | 10.0k | 25.0 | 71.7 | 79.0 | 2 | 170 | 0.57 |
| redb | 1 | 25.0k | 24.0k | 123 | 211 | 247 | 1 | 2046 | 0.69 |
| redb | 1 | 50.0k | 44.4k | 1154 | 1516 | 1548 | 1 | 16384 | 0.77 |
| redb | 1 | 100.0k | 34.2k | 4086 | 5370 | 5394 | 1 | 16384 | 0.94 |
| rocksdb | 1 | 5.0k | 5.0k | 3.5 | 23.9 | 47.8 | 6 | 11 | 0.12 |
| rocksdb | 1 | 10.0k | 10.0k | 3.4 | 141 | 172 | 5 | 21 | 0.17 |
| rocksdb | 1 | 25.0k | 25.0k | 3.3 | 24.5 | 47.0 | 4 | 48 | 0.29 |
| rocksdb | 1 | 50.0k | 50.0k | 4.1 | 19.2 | 28.9 | 4 | 128 | 0.52 |
| rocksdb | 1 | 100.0k | 99.9k | 4.9 | 43.6 | 50.3 | 3 | 301 | 0.84 |

preload fjall: 1000000 sessions in 5.74 s, 282 MiB

preload redb: 1000000 sessions in 13.96 s, 467 MiB

preload rocksdb: 1000000 sessions in 2.16 s, 111 MiB

preload fjall: 1000000 sessions in 2.69 s, 238 MiB

preload redb: 1000000 sessions in 13.74 s, 467 MiB

preload rocksdb: 1000000 sessions in 2.32 s, 111 MiB

| Engine | Workers | Highest rate kept up with | p99 ms there |
| --- | --- | --- | --- |
| fjall | 1 | 100.0k | 58.5 |
| fjall | 4 | 200.0k | 115 |
| redb | 1 | 10.0k | 71.7 |
| redb | 4 | 10.0k | 38.0 |
| rocksdb | 1 | 100.0k | 43.6 |
| rocksdb | 4 | 200.0k | 11.7 |

### Footprint, first pass (one sync at the end of the load; redb's ten million stopped)

| Engine | Sessions | Load s | Disk B/session (loaded) | Disk B/session (compacted) | Memory after reads MiB | Open s |
| --- | --- | --- | --- | --- | --- | --- |
| fjall | 1,000,000 | 3.57 | 250.02 | 384.95 | 173 | 0.9 |
| fjall | 10,000,000 | 36.28 | 240.95 | 351.08 | 90 | 0.01 |
| redb | 1,000,000 | 15.02 | 489.7 | 338.87 | 85 | 0.01 |
| rocksdb | 1,000,000 | 2.81 | 115.96 | 109.4 | 71 | 0.02 |
| rocksdb | 10,000,000 | 26.31 | 131.89 | 107.99 | 106 | 0.02 |

logical bytes per session: 214

| Engine | Disk B/session (compacted, largest run) | Memory B/session (slope) | Disk at 10^8 GiB | Disk at 10^9 GiB | Memory at 10^8 GiB | Memory at 10^9 GiB |
| --- | --- | --- | --- | --- | --- | --- |
| fjall | 351 | 0.0 | 32.7 | 327 | 0.00 | 0.0 |
| rocksdb | 108 | 4.1 | 10.1 | 101 | 0.38 | 3.8 |

### Footprint, second pass (sync every 100,000 sessions, disk after reopen)

| Engine | Sessions | Load s | Disk B/session (loaded) | Disk B/session (compacted) | Memory after reads MiB | Open s |
| --- | --- | --- | --- | --- | --- | --- |
| fjall | 1,000,000 | 2.67 | 250.02 | 384.95 | 171 | 0.6 |
| fjall | 10,000,000 | 29.84 | 203.24 | 305.92 | 90 | 0.0 |
| redb | 1,000,000 | 24.66 | 750.65 | 338.87 | 86 | 0.01 |
| rocksdb | 1,000,000 | 2.26 | 115.96 | 109.4 | 71 | 0.02 |
| rocksdb | 10,000,000 | 20.62 | 135.43 | 107.99 | 107 | 0.02 |

logical bytes per session: 214

| Engine | Disk B/session (compacted, largest run) | Memory B/session (slope) | Disk at 10^8 GiB | Disk at 10^9 GiB | Memory at 10^8 GiB | Memory at 10^9 GiB |
| --- | --- | --- | --- | --- | --- | --- |
| fjall | 306 | 0.0 | 28.5 | 285 | 0.00 | 0.0 |
| rocksdb | 108 | 4.1 | 10.1 | 101 | 0.38 | 3.8 |

### Queue churn

| Engine | Secs | Published | Write p50 ms | p99 ms | p99.9 ms | max ms | Worst second p99 ms | Seconds with a write over 100 ms | Drain p99 ms | Scan p99 ms | GC p99 ms | Disk end MiB | Disk max MiB | CPU |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fjall | 600 | 6,000,000 | 4.3 | 69.2 | 269 | 609 | 599 | 62 | 61.1 | 0.16 | 125 | 570 | 584 | 0.24 |
| rocksdb | 600 | 6,000,000 | 5.7 | 22.1 | 588 | 950 | 941 | 5 | 22.0 | 2.8 | 22.9 | 271 | 309 | 0.52 |
| redb | 600 | 6,000,000 | 48.3 | 472 | 1079 | 1347 | 1336 | 351 | 273 | 0.03 | 696 | 346 | 346 | 0.4 |

### Recovery at 10 GiB

- load fjall 10.0 GiB quick_repair=False: 157.98 s, 11.0 GiB on disk
- open fjall clean quick_repair=False: open 0.08 s, ready 0.08 s, found=True
- open fjall crash quick_repair=False: open 0.48 s, ready 0.49 s, found=True
- replay fjall: 1,000,000 entries, append 5.32 s, replay 3.13 s (319.2k/s), read 0.35 s
- load rocksdb 10.0 GiB quick_repair=False: 109.71 s, 16.2 GiB on disk
- open rocksdb clean quick_repair=False: open 0.07 s, ready 0.09 s, found=True
- open rocksdb crash quick_repair=False: open 0.23 s, ready 0.25 s, found=True
- replay rocksdb: 1,000,000 entries, append 7.59 s, replay 2.12 s (470.9k/s), read 0.49 s
- load redb 10.0 GiB quick_repair=False: 341.78 s, 18.1 GiB on disk
- open redb clean quick_repair=False: open 0.01 s, ready 0.01 s, found=True
- open redb crash quick_repair=False: open 1066.88 s, ready 1066.89 s, found=True
- replay redb: 1,000,000 entries, append 6.27 s, replay 10.35 s (96.6k/s), read 0.5 s
- load redb 2.0 GiB quick_repair=True: 79.19 s, 3.9 GiB on disk
- open redb crash quick_repair=True: open 0.02 s, ready 0.03 s, found=True

### Recovery at 50 GiB

- load rocksdb 50.0 GiB quick_repair=False: 1102.18 s, 52.5 GiB on disk
- open rocksdb clean quick_repair=False: open 0.12 s, ready 0.13 s, found=True
- open rocksdb crash quick_repair=False: open 0.4 s, ready 0.41 s, found=True

### Replication grid

| Scheme | Groups | One-way ms | Flush ms | Load | Done/s | p50 ms | p99 ms | Errors | Msgs/s | Items/flush | CPU cores (3 nodes) | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| raft | 1 | 1 | 0 | open:1000/s | 1000 | 3.6 | 6.3 | 0 | 932 | 1.0 | 0.09 |  |
| raft-batched | 1 | 1 | 0 | open:1000/s | 1000 | 5.5 | 8.8 | 0 | 948 | 1.0 | 0.08 |  |
| raft10-seq | 1 | 1 | 0 | open:1000/s | 1000 | 3.1 | 5.0 | 0 | 965 | 1.0 | 0.13 |  |
| raft10 | 1 | 1 | 0 | open:1000/s | 1000 | 2.2 | 2.4 | 0 | 4.0k | 1.0 | 0.21 |  |
| pb | 1 | 1 | 0 | open:1000/s | 1000 | 2.1 | 2.5 | 0 | 4.0k | 1.0 | 0.08 |  |
| raft | 1 | 1 | 0 | open:20000/s | 20.0k | 4.4 | 50.8 | 0 | 883 | 1.0 | 0.41 |  |
| raft-batched | 1 | 1 | 0 | open:20000/s | 20.0k | 6.9 | 9.8 | 0 | 938 | 1.0 | 0.24 |  |
| raft10-seq | 1 | 1 | 0 | open:20000/s | 20.0k | 3.0 | 7.0 | 0 | 957 | 1.26 | 0.5 |  |
| raft10 | 1 | 1 | 0 | open:20000/s | 20.0k | 2.1 | 3.1 | 0 | 39.3k | 1.27 | 1.44 |  |
| pb | 1 | 1 | 0 | open:20000/s | 20.0k | 2.1 | 44.0 | 0 | 73.4k | 1.04 | 0.5 |  |
| raft | 1 | 1 | 1 | open:1000/s | 0 | - | - | 9998 | 26 | 0.0 | 0.05 | 1 leaders moved |
| raft-batched | 1 | 1 | 1 | open:1000/s | 999 | 9.8 | 30.6 | 0 | 649 | 1.0 | 0.1 |  |
| raft10-seq | 1 | 1 | 1 | open:1000/s | 1000 | 4.8 | 14.7 | 0 | 641 | 1.05 | 0.13 |  |
| raft10 | 1 | 1 | 1 | open:1000/s | 1000 | 3.6 | 4.3 | 0 | 4.0k | 1.02 | 0.2 |  |
| pb | 1 | 1 | 1 | open:1000/s | 1000 | 3.6 | 5.8 | 0 | 4.0k | 1.02 | 0.08 |  |
| raft | 1 | 1 | 1 | open:20000/s | 0 | - | - | 199999 | 26 | 1.0 | 0.2 | 1 leaders moved |
| raft-batched | 1 | 1 | 1 | open:20000/s | 20.0k | 8.6 | 12.5 | 0 | 722 | 1.0 | 0.25 |  |
| raft10-seq | 1 | 1 | 1 | open:20000/s | 20.0k | 4.5 | 10.9 | 0 | 645 | 11.18 | 0.47 |  |
| raft10 | 1 | 1 | 1 | open:20000/s | 20.0k | 3.6 | 18.8 | 0 | 29.4k | 14.52 | 0.93 |  |
| pb | 1 | 1 | 1 | open:20000/s | 20.0k | 3.5 | 4.2 | 0 | 40.7k | 19.95 | 0.4 |  |
| raft | 1 | 2 | 0 | open:1000/s | 999 | 9.4 | 11.7 | 0 | 447 | 1.0 | 0.06 |  |
| raft-batched | 1 | 2 | 0 | open:1000/s | 998 | 12.6 | 20.3 | 0 | 445 | 1.0 | 0.07 |  |
| raft10-seq | 1 | 2 | 0 | open:1000/s | 999 | 6.0 | 16.4 | 0 | 478 | 1.0 | 0.09 |  |
| raft10 | 1 | 2 | 0 | open:1000/s | 1000 | 4.2 | 4.8 | 0 | 4.0k | 1.0 | 0.2 |  |
| pb | 1 | 2 | 0 | open:1000/s | 1000 | 4.1 | 4.8 | 0 | 4.0k | 1.0 | 0.09 |  |
| raft | 1 | 2 | 0 | open:20000/s | 20.0k | 7.7 | 14.8 | 0 | 439 | 1.0 | 0.33 |  |
| raft-batched | 1 | 2 | 0 | open:20000/s | 20.0k | 13.4 | 20.6 | 0 | 441 | 1.0 | 0.24 |  |
| raft10-seq | 1 | 2 | 0 | open:20000/s | 20.0k | 6.6 | 15.9 | 0 | 472 | 1.26 | 0.47 |  |
| raft10 | 1 | 2 | 0 | open:20000/s | 20.0k | 4.1 | 5.3 | 0 | 39.5k | 1.26 | 1.48 |  |
| pb | 1 | 2 | 0 | open:20000/s | 20.0k | 4.1 | 4.7 | 0 | 73.1k | 1.05 | 0.56 |  |
| raft | 1 | 2 | 1 | open:1000/s | 0 | - | - | 9999 | 26 | 0.0 | 0.04 | 1 leaders moved |
| raft-batched | 1 | 2 | 1 | open:1000/s | 997 | 15.5 | 33.8 | 0 | 408 | 1.0 | 0.08 |  |
| raft10-seq | 1 | 2 | 1 | open:1000/s | 999 | 7.2 | 13.8 | 0 | 422 | 1.03 | 0.11 |  |
| raft10 | 1 | 2 | 1 | open:1000/s | 999 | 5.6 | 6.9 | 0 | 4.0k | 1.03 | 0.21 |  |
| pb | 1 | 2 | 1 | open:1000/s | 999 | 5.6 | 24.9 | 0 | 3.9k | 1.05 | 0.1 |  |
| raft | 1 | 2 | 1 | open:20000/s | 0 | - | - | 199999 | 26 | 1.0 | 0.17 | 1 leaders moved |
| raft-batched | 1 | 2 | 1 | open:20000/s | 20.0k | 15.6 | 21.9 | 0 | 417 | 1.0 | 0.21 |  |
| raft10-seq | 1 | 2 | 1 | open:20000/s | 20.0k | 7.0 | 28.4 | 0 | 422 | 12.99 | 0.34 |  |
| raft10 | 1 | 2 | 1 | open:20000/s | 20.0k | 5.7 | 12.5 | 0 | 27.8k | 14.18 | 0.85 |  |
| pb | 1 | 2 | 1 | open:20000/s | 20.0k | 5.6 | 6.2 | 0 | 41.1k | 19.97 | 0.31 |  |
| raft | 16 | 1 | 0 | open:1000/s | 1000 | 2.1 | 4.2 | 0 | 4.1k | 1.0 | 0.13 |  |
| raft-batched | 16 | 1 | 0 | open:1000/s | 1000 | 2.2 | 5.3 | 0 | 4.1k | 1.0 | 0.15 |  |
| raft10-seq | 16 | 1 | 0 | open:1000/s | 1000 | 2.2 | 4.3 | 0 | 4.4k | 1.01 | 0.22 |  |
| raft10 | 16 | 1 | 0 | open:1000/s | 1000 | 2.2 | 5.0 | 0 | 4.6k | 1.01 | 0.29 |  |
| pb | 16 | 1 | 0 | open:1000/s | 1000 | 2.1 | 6.9 | 0 | 4.0k | 1.0 | 0.09 |  |
| raft | 16 | 1 | 0 | open:20000/s | 20.0k | 3.4 | 6.0 | 0 | 15.7k | 1.01 | 0.8 |  |
| raft-batched | 16 | 1 | 0 | open:20000/s | 20.0k | 5.3 | 9.0 | 0 | 15.8k | 1.01 | 0.72 |  |
| raft10-seq | 16 | 1 | 0 | open:20000/s | 20.0k | 2.7 | 4.0 | 0 | 16.2k | 1.1 | 1.15 |  |
| raft10 | 16 | 1 | 0 | open:20000/s | 20.0k | 2.2 | 2.8 | 0 | 76.6k | 1.14 | 2.81 |  |
| pb | 16 | 1 | 0 | open:20000/s | 20.0k | 2.1 | 3.4 | 0 | 78.0k | 1.02 | 0.45 |  |
| raft | 16 | 1 | 1 | open:1000/s | 1000 | 4.7 | 9.8 | 0 | 3.9k | 1.24 | 0.17 |  |
| raft-batched | 16 | 1 | 1 | open:1000/s | 999 | 4.8 | 10.1 | 0 | 3.9k | 1.24 | 0.16 |  |
| raft10-seq | 16 | 1 | 1 | open:1000/s | 1000 | 3.2 | 9.6 | 0 | 4.2k | 1.22 | 0.23 |  |
| raft10 | 16 | 1 | 1 | open:1000/s | 1000 | 3.2 | 9.8 | 0 | 4.6k | 1.19 | 0.26 |  |
| pb | 16 | 1 | 1 | open:1000/s | 1000 | 3.1 | 3.2 | 0 | 4.0k | 1.18 | 0.07 |  |
| raft | 16 | 1 | 1 | open:20000/s | 0 | - | - | 199979 | 408 | 0.0 | 0.15 | 16 leaders moved |
| raft-batched | 16 | 1 | 1 | open:20000/s | 20.0k | 10.4 | 14.6 | 0 | 9.6k | 3.38 | 0.49 |  |
| raft10-seq | 16 | 1 | 1 | open:20000/s | 20.0k | 5.4 | 7.7 | 0 | 8.5k | 9.39 | 0.78 |  |
| raft10 | 16 | 1 | 1 | open:20000/s | 20.0k | 3.5 | 5.8 | 0 | 67.8k | 20.13 | 2.01 |  |
| pb | 16 | 1 | 1 | open:20000/s | 20.0k | 3.4 | 4.0 | 0 | 45.8k | 20.16 | 0.3 |  |
| raft | 16 | 2 | 0 | open:1000/s | 999 | 4.2 | 10.0 | 0 | 3.8k | 1.0 | 0.14 |  |
| raft-batched | 16 | 2 | 0 | open:1000/s | 999 | 4.2 | 14.3 | 0 | 3.8k | 1.0 | 0.14 |  |
| raft10-seq | 16 | 2 | 0 | open:1000/s | 999 | 4.2 | 8.2 | 0 | 4.1k | 1.0 | 0.21 |  |
| raft10 | 16 | 2 | 0 | open:1000/s | 1000 | 4.1 | 4.2 | 0 | 4.6k | 1.0 | 0.22 |  |
| pb | 16 | 2 | 0 | open:1000/s | 1000 | 4.1 | 4.2 | 0 | 4.0k | 1.0 | 0.06 |  |
| raft | 16 | 2 | 0 | open:20000/s | 20.0k | 7.0 | 12.8 | 0 | 7.2k | 1.01 | 0.52 |  |
| raft-batched | 16 | 2 | 0 | open:20000/s | 20.0k | 11.8 | 19.6 | 0 | 7.1k | 1.19 | 0.45 |  |
| raft10-seq | 16 | 2 | 0 | open:20000/s | 20.0k | 5.7 | 8.3 | 0 | 8.4k | 1.11 | 0.77 |  |
| raft10 | 16 | 2 | 0 | open:20000/s | 20.0k | 4.1 | 4.2 | 0 | 76.9k | 1.13 | 2.72 |  |
| pb | 16 | 2 | 0 | open:20000/s | 20.0k | 4.1 | 4.3 | 0 | 78.8k | 1.01 | 0.38 |  |
| raft | 16 | 2 | 1 | open:1000/s | 999 | 7.1 | 13.2 | 0 | 3.6k | 1.26 | 0.15 |  |
| raft-batched | 16 | 2 | 1 | open:1000/s | 999 | 7.2 | 16.0 | 0 | 3.7k | 1.28 | 0.15 |  |
| raft10-seq | 16 | 2 | 1 | open:1000/s | 999 | 5.3 | 10.2 | 0 | 3.9k | 1.28 | 0.22 |  |
| raft10 | 16 | 2 | 1 | open:1000/s | 999 | 5.2 | 17.9 | 0 | 4.6k | 1.24 | 0.24 |  |
| pb | 16 | 2 | 1 | open:1000/s | 999 | 5.1 | 6.1 | 0 | 3.9k | 1.23 | 0.07 |  |
| raft | 16 | 2 | 1 | open:20000/s | 0 | - | - | 199984 | 412 | 0.0 | 0.15 | 16 leaders moved |
| raft-batched | 16 | 2 | 1 | open:20000/s | 20.0k | 15.1 | 22.3 | 0 | 6.6k | 2.37 | 0.41 |  |
| raft10-seq | 16 | 2 | 1 | open:20000/s | 20.0k | 7.1 | 10.4 | 0 | 6.6k | 8.9 | 0.73 |  |
| raft10 | 16 | 2 | 1 | open:20000/s | 20.0k | 5.6 | 6.9 | 0 | 65.9k | 20.48 | 2.36 |  |
| pb | 16 | 2 | 1 | open:20000/s | 20.0k | 5.5 | 6.1 | 0 | 46.3k | 20.14 | 0.43 |  |
| raft | 128 | 1 | 0 | open:1000/s | 1000 | 2.2 | 3.5 | 0 | 7.2k | 1.0 | 0.36 |  |
| raft-batched | 128 | 1 | 0 | open:1000/s | 1000 | 2.2 | 5.0 | 0 | 7.2k | 1.0 | 0.34 |  |
| raft10-seq | 128 | 1 | 0 | open:1000/s | 999 | 2.2 | 6.3 | 0 | 8.6k | 1.01 | 0.71 |  |
| raft10 | 128 | 1 | 0 | open:1000/s | 1000 | 2.2 | 2.3 | 0 | 8.6k | 1.0 | 0.86 |  |
| pb | 128 | 1 | 0 | open:1000/s | 1000 | 2.1 | 2.4 | 0 | 4.0k | 1.0 | 0.09 |  |
| raft | 128 | 1 | 0 | open:20000/s | 20.0k | 2.3 | 5.3 | 0 | 68.3k | 1.01 | 1.95 |  |
| raft-batched | 128 | 1 | 0 | open:20000/s | 20.0k | 2.4 | 6.3 | 0 | 69.4k | 1.02 | 2.15 |  |
| raft10-seq | 128 | 1 | 0 | open:20000/s | 20.0k | 2.3 | 5.7 | 0 | 70.6k | 1.12 | 3.06 |  |
| raft10 | 128 | 1 | 0 | open:20000/s | 20.0k | 2.2 | 4.0 | 0 | 83.9k | 1.17 | 3.62 |  |
| pb | 128 | 1 | 0 | open:20000/s | 20.0k | 2.1 | 2.2 | 0 | 78.5k | 1.01 | 0.55 |  |
| raft | 128 | 1 | 1 | open:1000/s | 1000 | 4.6 | 6.6 | 0 | 7.1k | 1.22 | 0.4 |  |
| raft-batched | 128 | 1 | 1 | open:1000/s | 999 | 4.6 | 6.5 | 0 | 7.2k | 1.22 | 0.4 |  |
| raft10-seq | 128 | 1 | 1 | open:1000/s | 1000 | 3.2 | 5.2 | 0 | 8.6k | 1.19 | 0.86 |  |
| raft10 | 128 | 1 | 1 | open:1000/s | 1000 | 3.2 | 3.4 | 0 | 8.6k | 1.18 | 0.94 |  |
| pb | 128 | 1 | 1 | open:1000/s | 1000 | 3.2 | 3.3 | 0 | 4.0k | 1.19 | 0.1 |  |
| raft | 128 | 1 | 1 | open:20000/s | 20.0k | 6.4 | 17.7 | 0 | 51.6k | 17.23 | 1.66 |  |
| raft-batched | 128 | 1 | 1 | open:20000/s | 20.0k | 6.6 | 12.8 | 0 | 58.5k | 17.77 | 1.92 |  |
| raft10-seq | 128 | 1 | 1 | open:20000/s | 20.0k | 4.6 | 9.1 | 0 | 59.4k | 18.56 | 2.79 |  |
| raft10 | 128 | 1 | 1 | open:20000/s | 20.0k | 3.6 | 4.1 | 0 | 82.7k | 20.56 | 3.56 |  |
| pb | 128 | 1 | 1 | open:20000/s | 20.0k | 3.4 | 4.0 | 0 | 45.6k | 20.18 | 0.42 |  |
| raft | 128 | 2 | 0 | open:1000/s | 1000 | 4.2 | 7.4 | 0 | 7.1k | 1.0 | 0.36 |  |
| raft-batched | 128 | 2 | 0 | open:1000/s | 1000 | 4.2 | 7.5 | 0 | 7.1k | 1.0 | 0.37 |  |
| raft10-seq | 128 | 2 | 0 | open:1000/s | 1000 | 4.2 | 9.6 | 0 | 8.5k | 1.01 | 0.74 |  |
| raft10 | 128 | 2 | 0 | open:1000/s | 1000 | 4.2 | 4.4 | 0 | 8.6k | 1.0 | 0.87 |  |
| pb | 128 | 2 | 0 | open:1000/s | 1000 | 4.2 | 4.2 | 0 | 4.0k | 1.0 | 0.08 |  |
| raft | 128 | 2 | 0 | open:20000/s | 20.0k | 5.9 | 11.2 | 0 | 51.9k | 1.01 | 1.6 |  |
| raft-batched | 128 | 2 | 0 | open:20000/s | 20.0k | 7.8 | 40.6 | 0 | 51.3k | 1.04 | 1.75 |  |
| raft10-seq | 128 | 2 | 0 | open:20000/s | 20.0k | 6.2 | 39.1 | 0 | 50.3k | 1.21 | 2.52 |  |
| raft10 | 128 | 2 | 0 | open:20000/s | 20.0k | 4.2 | 7.3 | 0 | 83.7k | 1.19 | 3.55 |  |
| pb | 128 | 2 | 0 | open:20000/s | 20.0k | 4.1 | 4.2 | 0 | 78.5k | 1.01 | 0.49 |  |
| raft | 128 | 2 | 1 | open:1000/s | 999 | 6.8 | 10.6 | 0 | 7.0k | 1.27 | 0.38 |  |
| raft-batched | 128 | 2 | 1 | open:1000/s | 999 | 6.8 | 10.9 | 0 | 7.0k | 1.27 | 0.41 |  |
| raft10-seq | 128 | 2 | 1 | open:1000/s | 999 | 5.3 | 11.4 | 0 | 8.5k | 1.27 | 0.8 |  |
| raft10 | 128 | 2 | 1 | open:1000/s | 999 | 5.3 | 19.5 | 0 | 8.6k | 1.31 | 0.8 |  |
| pb | 128 | 2 | 1 | open:1000/s | 999 | 5.2 | 6.2 | 0 | 4.0k | 1.25 | 0.09 |  |
| raft | 128 | 2 | 1 | open:20000/s | 20.0k | 10.2 | 22.2 | 0 | 40.5k | 16.02 | 1.46 |  |
| raft-batched | 128 | 2 | 1 | open:20000/s | 20.0k | 12.6 | 22.0 | 0 | 45.0k | 15.38 | 1.64 |  |
| raft10-seq | 128 | 2 | 1 | open:20000/s | 20.0k | 6.8 | 10.4 | 0 | 49.0k | 17.16 | 2.56 |  |
| raft10 | 128 | 2 | 1 | open:20000/s | 20.0k | 5.7 | 10.0 | 0 | 82.2k | 21.21 | 3.53 |  |
| pb | 128 | 2 | 1 | open:20000/s | 20.0k | 5.5 | 6.1 | 0 | 46.3k | 20.1 | 0.43 |  |

### Replication, the storm

| Scheme | Groups | One-way ms | Flush ms | Load | Done/s | p50 ms | p99 ms | Errors | Msgs/s | Items/flush | CPU cores (3 nodes) | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| raft10 | 16 | 1 | 1 | open:50000/s | 50.0k | 3.6 | 4.2 | 0 | 137.7k | 48.62 | 4.16 |  |
| raft10-seq | 16 | 1 | 1 | open:50000/s | 50.0k | 6.0 | 9.2 | 0 | 7.7k | 22.02 | 1.56 |  |
| pb | 16 | 1 | 1 | open:50000/s | 50.0k | 3.5 | 4.1 | 0 | 96.8k | 47.98 | 0.92 |  |
| raft-batched | 16 | 1 | 1 | open:50000/s | 49.9k | 11.9 | 16.7 | 0 | 8.5k | 3.39 | 1.22 |  |
| raft | 16 | 1 | 1 | open:50000/s | 0 | - | - | 499984 | 430 | 2.46 | 0.47 | 16 leaders moved |
| raft10 | 128 | 1 | 1 | open:50000/s | 50.0k | 4.0 | 5.1 | 0 | 190.3k | 55.06 | 6.95 |  |
| raft10-seq | 128 | 1 | 1 | open:50000/s | 50.0k | 5.1 | 7.8 | 0 | 74.1k | 36.75 | 4.14 |  |
| pb | 128 | 1 | 1 | open:50000/s | 50.0k | 3.5 | 4.0 | 0 | 96.9k | 48.02 | 0.89 |  |
| raft-batched | 128 | 1 | 1 | open:50000/s | 49.9k | 9.6 | 14.4 | 0 | 76.7k | 26.06 | 2.81 |  |
| raft | 128 | 1 | 1 | open:50000/s | 13.5k | 61.9 | 615 | 310960 | 6.5k | 7.68 | 0.89 | 120 leaders moved |
| raft10 | 256 | 1 | 1 | open:50000/s | 50.0k | 4.2 | 9.1 | 0 | 199.7k | 58.44 | 7.6 |  |
| raft10-seq | 256 | 1 | 1 | open:50000/s | 50.0k | 4.9 | 8.1 | 0 | 129.3k | 46.02 | 6.02 |  |
| pb | 256 | 1 | 1 | open:50000/s | 50.0k | 3.5 | 4.1 | 0 | 96.9k | 48.0 | 0.92 |  |
| raft-batched | 256 | 1 | 1 | open:50000/s | 50.0k | 7.7 | 13.7 | 0 | 129.7k | 41.55 | 4.28 |  |
| raft | 256 | 1 | 1 | open:50000/s | 49.9k | 7.3 | 24.4 | 0 | 108.0k | 40.39 | 3.49 |  |

### Replication, one partition leader at its limit

| Scheme | Groups | One-way ms | Flush ms | Load | Done/s | p50 ms | p99 ms | Errors | Msgs/s | Items/flush | CPU cores (3 nodes) | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| raft | 1 | 1 | 0 | closed:16 | 3.0k | 5.2 | 7.9 | 0 | 776 | 1.0 | 0.09 |  |
| raft-batched | 1 | 1 | 0 | closed:16 | 2.0k | 7.7 | 10.6 | 0 | 774 | 1.0 | 0.06 |  |
| raft10-seq | 1 | 1 | 0 | closed:16 | 3.5k | 4.3 | 5.3 | 0 | 808 | 1.76 | 0.12 |  |
| raft10 | 1 | 1 | 0 | closed:16 | 6.1k | 2.6 | 2.9 | 0 | 2.6k | 2.14 | 0.2 |  |
| pb | 1 | 1 | 0 | closed:16 | 6.4k | 2.5 | 2.8 | 0 | 13.7k | 1.44 | 0.24 |  |
| raft | 1 | 1 | 0 | closed:256 | 48.6k | 5.2 | 7.8 | 0 | 766 | 1.0 | 0.81 |  |
| raft-batched | 1 | 1 | 0 | closed:256 | 34.7k | 7.6 | 11.6 | 0 | 781 | 1.0 | 0.34 |  |
| raft10-seq | 1 | 1 | 0 | closed:256 | 52.0k | 5.2 | 5.5 | 0 | 800 | 4.49 | 0.76 |  |
| raft10 | 1 | 1 | 0 | closed:256 | 117.1k | 2.1 | 2.6 | 0 | 48.0k | 1.69 | 2.7 |  |
| pb | 1 | 1 | 0 | closed:256 | 123.5k | 2.1 | 2.4 | 0 | 294.9k | 1.42 | 3.29 |  |
| raft | 1 | 1 | 0 | closed:1024 | 112.8k | 8.0 | 17.3 | 0 | 760 | 1.0 | 1.13 |  |
| raft-batched | 1 | 1 | 0 | closed:1024 | 141.2k | 7.5 | 10.7 | 0 | 773 | 1.0 | 1.6 |  |
| raft10-seq | 1 | 1 | 0 | closed:1024 | 110.7k | 8.4 | 20.1 | 0 | 774 | 5.23 | 1.97 |  |
| raft10 | 1 | 1 | 0 | closed:1024 | 423.3k | 2.3 | 10.0 | 0 | 29.6k | 2.78 | 5.06 |  |
| pb | 1 | 1 | 0 | closed:1024 | 225.0k | 4.6 | 5.3 | 0 | 283.6k | 3.0 | 6.85 |  |
| raft | 1 | 1 | 1 | closed:16 | 686 | 23.2 | 26.2 | 0 | 214 | 1.0 | 0.05 |  |
| raft-batched | 1 | 1 | 1 | closed:16 | 1.4k | 11.3 | 15.8 | 0 | 592 | 1.0 | 0.07 |  |
| raft10-seq | 1 | 1 | 1 | closed:16 | 2.8k | 5.8 | 6.7 | 0 | 555 | 2.03 | 0.12 |  |
| raft10 | 1 | 1 | 1 | closed:16 | 3.6k | 4.4 | 5.2 | 0 | 3.7k | 1.74 | 0.23 |  |
| pb | 1 | 1 | 1 | closed:16 | 3.4k | 5.1 | 5.3 | 0 | 5.8k | 3.28 | 0.1 |  |
| raft | 1 | 1 | 1 | closed:256 | 736 | 333 | 340 | 0 | 15 | 1.0 | 0.03 |  |
| raft-batched | 1 | 1 | 1 | closed:256 | 25.0k | 9.5 | 15.4 | 0 | 613 | 1.0 | 0.26 |  |
| raft10-seq | 1 | 1 | 1 | closed:256 | 44.8k | 5.2 | 8.0 | 0 | 586 | 14.14 | 0.79 |  |
| raft10 | 1 | 1 | 1 | closed:256 | 52.0k | 5.0 | 5.1 | 0 | 5.8k | 5.58 | 0.95 |  |
| pb | 1 | 1 | 1 | closed:256 | 58.8k | 4.5 | 4.9 | 0 | 30.8k | 33.51 | 1.19 |  |
| raft | 1 | 1 | 1 | closed:1024 | 0 | - | - | 13163649 | 26 | 0.0 | 6.5 | 1 leaders moved |
| raft-batched | 1 | 1 | 1 | closed:1024 | 106.6k | 9.7 | 13.7 | 0 | 648 | 1.0 | 1.43 |  |
| raft10-seq | 1 | 1 | 1 | closed:1024 | 77.6k | 11.9 | 23.2 | 0 | 553 | 18.6 | 1.33 |  |
| raft10 | 1 | 1 | 1 | closed:1024 | 221.7k | 4.5 | 11.0 | 0 | 8.3k | 12.68 | 2.58 |  |
| pb | 1 | 1 | 1 | closed:1024 | 240.9k | 4.2 | 5.4 | 0 | 118.2k | 140.49 | 4.68 |  |

### Replication, idle

| Scheme | Groups | Heartbeat ms | Election ms | CPU cores, 3 nodes | Per node | us/s per group replica | Messages/s |
| --- | --- | --- | --- | --- | --- | --- | --- |
| raft | 128 | 50 | 150 to 300 | 0.15 | 0.05 | 403.34 | 3.2k |
| raft10 | 128 | 50 | 150 to 300 | 0.83 | 0.28 | 2162.03 | 4.6k |
| pb | 128 | 0 | 150 to 300 | 0.0 | 0.0 | 0.35 | 2 |
| raft | 128 | 250 | 1000 to 2000 | 0.03 | 0.01 | 88.0 | 678 |
| raft10 | 128 | 250 | 1000 to 2000 | 0.18 | 0.06 | 457.13 | 932 |
| raft | 256 | 50 | 150 to 300 | 0.32 | 0.11 | 416.32 | 6.4k |
| raft10 | 256 | 50 | 150 to 300 | 1.88 | 0.63 | 2449.28 | 9.3k |
| pb | 256 | 0 | 150 to 300 | 0.0 | 0.0 | 0.15 | 2 |
| raft | 256 | 250 | 1000 to 2000 | 0.05 | 0.02 | 70.09 | 1.4k |
| raft10 | 256 | 250 | 1000 to 2000 | 0.27 | 0.09 | 349.62 | 1.9k |

### Group commit windows side by side (median of two passes)

| Size | Load | Engine | Writes/s | p50 ms | p99 ms | p99.9 ms | p99 range ms | Batch | Write amp |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 128 | closed:1 | fjall | 211 | 4.5 | 12.0 | 17.8 | 7.2 to 16.9 | 1 | 118.3 |
| 128 | closed:1 | redb | 284 | 3.5 | 6.3 | 10.7 | 5.1 to 7.5 | 1 | 610.6 |
| 128 | closed:1 | rocksdb | 309 | 3.5 | 5.5 | 10.5 | 3.9 to 7.1 | 1 | 112.7 |
| 128 | open:10000/s | fjall | 10.0k | 5.1 | 9.1 | 12.8 | 6.1 to 12.1 | 33 | 5.0 |
| 128 | open:10000/s | redb | 10.0k | 6.2 | 27.8 | 37.6 | 10.0 to 45.6 | 42 | 145.2 |
| 128 | open:10000/s | rocksdb | 10.0k | 4.6 | 8.3 | 19.1 | 6.0 to 10.6 | 30 | 5.6 |
| 128 | open:50000/s | fjall | 50.0k | 4.5 | 79.8 | 122 | 7.8 to 152 | 125 | 3.3 |
| 128 | open:50000/s | redb | 49.9k | 8.4 | 17.6 | 22.2 | 17.4 to 17.9 | 279 | 64.1 |
| 128 | open:50000/s | rocksdb | 50.0k | 4.0 | 7.8 | 17.5 | 7.2 to 8.5 | 124 | 3.0 |

#### Group commit window, 128 B (median of passes)

| Load | Engine | 0 ms p50/p99 | 1 ms p50/p99 | 2 ms p50/p99 |
| --- | --- | --- | --- | --- |
| closed:1 | fjall | 4.5 / 12.0 | 4.5 / 6.9 | 6.0 / 9.3 |
| closed:1 | redb | 3.5 / 6.3 | 5.0 / 8.4 | 6.5 / 12.1 |
| closed:1 | rocksdb | 3.5 / 5.5 | 4.5 / 10.2 | 6.2 / 9.5 |
| open:10000/s | fjall | 5.1 / 9.1 | 5.3 / 9.7 | 6.2 / 10.9 |
| open:10000/s | redb | 6.2 / 27.8 | 7.1 / 17.0 | 8.3 / 17.6 |
| open:10000/s | rocksdb | 4.6 / 8.3 | 5.3 / 22.1 | 5.6 / 9.9 |
| open:50000/s | fjall | 4.5 / 79.8 | 4.8 / 17.6 | 6.3 / 13.2 |
| open:50000/s | redb | 8.4 / 17.6 | 8.9 / 17.9 | 11.4 / 22.8 |
| open:50000/s | rocksdb | 4.0 / 7.8 | 5.5 / 10.9 | 6.6 / 20.7 |

### Loopback TCP

| Size B | Round trips | CPU us per round trip | RTT p50 us | RTT p99 us |
| --- | --- | --- | --- | --- |
| 128 | 251,171 | 13.21 | 18 | 49 |

### Build friction

| Where | Feature | Profile | Variant | Built | Clean build s | Binary MiB | Added MiB | Error |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Darwin-arm64 | none | probe | native | yes | 0.6 | 0.3 | 0.0 |  |
| Darwin-arm64 | none | dev | native | yes | 0.2 | 0.5 | 0.0 |  |
| Darwin-arm64 | fjall | probe | native | yes | 7.0 | 1.4 | 1.1 |  |
| Darwin-arm64 | fjall | dev | native | yes | 4.6 | 6.8 | 6.3 |  |
| Darwin-arm64 | redb | probe | native | yes | 5.0 | 1.1 | 0.7 |  |
| Darwin-arm64 | redb | dev | native | yes | 2.9 | 4.8 | 4.3 |  |
| Darwin-arm64 | rocksdb | probe | native | yes | 88.7 | 6.3 | 5.9 |  |
| Darwin-arm64 | rocksdb | dev | native | yes | 96.5 | 31.5 | 31.0 |  |
| Darwin-arm64 | openraft | probe | native | yes | 10.8 | 0.8 | 0.5 |  |
| Darwin-arm64 | openraft | dev | native | yes | 8.3 | 3.9 | 3.4 |  |
| docker-rust:1.99.0-bookworm-arm64 | none | probe | plain image | yes | 1.0 | 0.3 | 0.0 |  |
| docker-rust:1.99.0-bookworm-arm64 | fjall | probe | plain image | yes | 6.4 | 1.4 | 1.1 |  |
| docker-rust:1.99.0-bookworm-arm64 | redb | probe | plain image | yes | 4.4 | 1.1 | 0.8 |  |
| docker-rust:1.99.0-bookworm-arm64 | openraft | probe | plain image | yes | 13.2 | 0.8 | 0.5 |  |
| docker-rust:1.99.0-bookworm-arm64 | rocksdb | probe | plain image | no | - | - | - | error: failed to run custom build command for `librocksdb-sys v0.19.0+11.8.1`   Unable to find libclang: 'couldn't find any valid shared libraries matching: ['libclang.so', 'libclang-*.so', 'libclang.so.*', 'libclang-*.so.*'], set the `LIBCLANG_PATH` environment variable to a path where one of these files can be found (invalid: [])'  |
| docker-rust:1.99.0-bookworm-arm64 | rocksdb | probe | with libclang-dev | yes | 706.2 | 7.5 | 7.2 |  |

Runtime check: probe with rocksdb in `gcr.io/distroless/cc-debian12:nonroot` exited 0 (output: ok); C++ runtime libraries in the image: lib/aarch64-linux-gnu/libgcc_s.so.1 usr/lib/aarch64-linux-gnu/libstdc++.so.6.0.30.
