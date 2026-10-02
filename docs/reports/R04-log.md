# R4. The log: engine, replication and what committed means

Status: draft. Measured by spike S1 (`spikes/s1-log`) on 2026-10-01 and 02, and
measured again on 2026-10-02 after a review found four faults in the
measurement code (below, "Revision 2"). Every number below is in
`bench/results/s1-2026-10-02.json`, with the machine, the versions and the load
average of each run; the records the faults affected are kept there beside the
ones that replace them.

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
  primary-backup. 0.10 is a pre-release, pinned exactly; Open risks says what
  happens if it changes or stalls.
- **Group commit with no window**: a node's log writer takes whatever is waiting
  when the previous flush ends and flushes it as one batch, bounded in bytes.
- **One fsync per batch, shared**: Raft entries of every partition on a node and
  the state machine's writes go to one engine, whose write-ahead log one flush
  covers; the state machine never syncs on its own.

The three numbers that decided it:

1. **The storm on one node's engine**: RocksDB applied 200,000 claims a second
   (each a read, a Raft entry and an `own` write, durable) at a p99 of 28 and
   30 ms in two passes; fjall the same rate at 106 and 328 ms; redb was at its
   limit by 25,000.
2. **Stalls over ten minutes of queue churn**: seconds in which a durable
   write took over 100 ms, out of 600, in two passes: RocksDB 3 and 2, fjall 49
   and 34, redb 339 and 338.
3. **Commit latency, pipelined openraft 0.10 against primary-backup**: 4.0 ms
   against 3.5 ms at p50 in a storm of 50,000 writes a second over 256 groups
   (1 ms one way, 1 ms flush), while openraft 0.9 lost 121 of 128 leaders in the
   same storm spread over 128 groups. Raft costs no latency worth writing a
   replication protocol to save.

The review's re-runs (Revision 2, below) moved these numbers, from a p99 of
12 ms for RocksDB's storm, from 5, 62 and 351 stalled seconds, and from 4.2
against 3.5 ms, and changed none of the four answers.

And the answers R3 asked for: a cluster absorbs 50,000 claims a second with the
engine four times over and replication 250 times over per partition; an idle
session costs 108 bytes of disk and 4 bytes of memory per replica, so 10^8
sessions take 30 GiB of disk and 1.1 GiB of memory across a cluster with three
replicas; a log pod with 50 GiB of RocksDB data is ready 0.13 s after a clean
stop and 0.41 s after `SIGKILL`, and replays a million log entries in about 2 s.

## How it was measured

**These are laptop numbers.** Every run used one Apple M2 Pro (6 performance and
4 efficiency cores, 16 GiB) with its internal SSD under APFS, macOS 26.5 and
Rust 1.99.0, while other work shared the machine: the load average was 1.4 to
11 in the runs this report quotes (up to 39 in records it no longer quotes) and
is recorded with every result. A server with a datacentre disk has other
absolute numbers. What carries over is the comparison between candidates and
the shape of each curve, and the comparison holds only where the candidates ran
under the same conditions. Where a step allowed it they ran interleaved: the
engines within every configuration of the group commit and shared-fsync steps,
the windows within every load, the replication schemes within every
configuration. The claim storm and queue churn run one engine at a time for
minutes each, so each ran twice, the second pass with the engines in reverse
order; footprint, recovery and the durability floor ran one candidate at a
time, and the floor ran twice, an hour and a half apart.

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
user-interactive scheduling class and spins the last 150 us. In the median run
of the replication grid a hop is 1 us late at p50 and 250 us at p99 (60 us in
the first revision's grid), and in 12 of the grid's 120 runs
it was over a millisecond late at p99 (marked in section 8, and in the
appendix's lateness column). In the saturation runs of one partition leader at
its limit every core was busy and hops ran 250 to 1,000 us late at p99 for
every scheme alike.

**Revision 2.** A review of the first revision found four faults in the
measurement code. Each is fixed and every step it touched was run again; the
new records carry `"rev": 2` and the first revision's carry no `rev`.

1. **Requests at the edges of a window were dropped.** Throughput and latency
   counted only requests both offered and finished inside the measured window.
   Under a backlog that leaves out the warmup's requests, which use measured
   capacity, and the slowest of the window's own, which finish after it: an
   engine that sustains 25,000 a second, offered 50,000, would have reported
   18,750 a second and kept 150,000 of its 400,000 latencies. Throughput now
   counts every request that finished inside the window, and latency every
   request offered inside it, waiting for the last of them after the window
   closes. This touched every step that offers load: group commit, the windows,
   shared fsync, the claim storm and replication.
2. **Primary-backup did less work than Raft.** Its backups discarded the entries
   and only the primary applied them, where every Raft replica stores and
   applies every entry. Backups now keep the entries and apply them once the
   primary reports them committed, as a Raft follower does.
3. **The durability floor's appends were mostly overwrites.** The writer rewound
   every 64 MiB without truncating, so 94% of 1 MiB writes landed on blocks
   already written. It now only appends, and stops at 4 GiB.
4. **A queue drain could run ahead of the writes it drained.** A message joined
   its session's queue before its write was submitted, so a drain's range
   delete could reach the engine before the insert; and a drain counted its
   whole queue as drained whatever its scan found, which in the first revision
   was short in 0.15% of drains for the LSM engines and 1.5% for redb.
   Publishing now joins the queue and submits the write in one step, and a
   drain waits until every entry it took is durable, scans, counts what it
   found and deletes the range it scanned.

## Results

### 1. The durability floor

One thread writing and syncing, appending to a file as a log does (every write
extends the file); the last column is three threads on three files together.
Two passes, an hour and a half apart. Each cell is p50 / p99 in milliseconds and
syncs a second.

| Primitive | Pass | 128 B | 4 KiB | 64 KiB | 1 MiB | 4 KiB, 3 threads |
| --- | --- | --- | --- | --- | --- | --- |
| `F_FULLFSYNC` | 1 (load 5 to 6) | 4.0 / 9.0 (232/s) | 4.0 / 7.6 (240/s) | 4.0 / 7.1 (245/s) | 4.0 / 8.2 (241/s) | 12.0 / 16.2 (268/s) |
| `F_FULLFSYNC` | 2 (load 3) | 4.0 / 8.0 (238/s) | 4.0 / 8.1 (236/s) | 4.0 / 8.3 (235/s) | 4.9 / 10.6 (199/s) | 12.0 / 26.1 (244/s) |
| std `sync_data` | 1 (load 5 to 6) | 2.9 / 3.6 (402/s) | 1.5 / 3.5 (515/s) | 1.8 / 4.0 (489/s) | 3.0 / 7.2 (325/s) | 7.1 / 14.5 (408/s) |
| std `sync_data` | 2 (load 4 to 5) | 4.0 / 8.8 (227/s) | 4.0 / 8.5 (231/s) | 4.0 / 7.7 (236/s) | 5.0 / 11.0 (190/s) | 12.0 / 18.7 (252/s) |
| `F_BARRIERFSYNC` | 1 (load 4) | 0.14 / 0.40 (6420/s) | 0.13 / 0.33 (6744/s) | 0.17 / 0.94 (4007/s) | 1.1 / 5.6 (586/s) | 0.50 / 1.8 (5177/s) |
| `F_BARRIERFSYNC` | 2 (load 3 to 4) | 0.14 / 0.35 (6338/s) | 0.14 / 0.35 (6363/s) | 0.17 / 1.4 (4250/s) | 0.92 / 6.3 (560/s) | 0.45 / 1.4 (5869/s) |
| `fsync` | 1 (load 5 to 6) | 0.02 / 0.03 (45976/s) | 0.03 / 0.08 (35226/s) | 0.04 / 0.89 (11829/s) | 0.29 / 3.9 (1491/s) | 0.03 / 0.14 (70940/s) |
| `fsync` | 2 (load 4) | 0.02 / 0.04 (41088/s) | 0.03 / 0.05 (36716/s) | 0.04 / 1.1 (11302/s) | 0.36 / 4.1 (1296/s) | 0.03 / 0.07 (78132/s) |

- **A durable write costs 1.5 to 5 ms at p50 on this machine, whatever its size
  up to 1 MiB**, and the same call moves by a factor of up to three between
  passes (four in the first revision): a flush of the drive's cache serves every
  writer waiting on it, so what other processes flush moves ours. Everything
  durable below is a multiple of it. Rust's `sync_data` calls `F_FULLFSYNC`
  (std's source says so), so its two rows differ from the `F_FULLFSYNC` rows
  only in when they ran.
- **`fsync` without the cache flush is a hundred times cheaper** up to 64 KiB,
  which is why a macOS build of RocksDB without `HAVE_FULLFSYNC` looks fast and
  is not durable.
- **Flushes do not run in parallel.** Three threads on three files got 0.8 to
  1.1 times the full flushes of one, not three (plain `fsync`, which flushes
  nothing in the drive, got twice). Nothing is gained by giving each partition
  its own log file and flushing them side by side; one log per node with group
  commit is the way to batch.
- **Appending costs no more than rewriting** a region synced beforehand: a full
  flush of 1 MiB took 4.0 to 4.9 ms at p50 either way. Only plain `fsync`, which
  leaves the data in the drive's cache, shows the cost of extending the file:
  1,300 to 1,500 appends of 1 MiB a second against 4,300 to 4,500 rewrites (all
  four primitives are in the appendix). The first revision's appends were
  mostly rewrites (Revision 2, item 3) and found the same flush cost.

### 2. Group commit

Raft log appends of 128 B and 1 KiB spread over 128 groups, as one node sees
them; window 0; median of three passes, engines interleaved within each
configuration. Each cell is entries a second, then p50 / p99 in milliseconds.

| Entry | Load | fjall | redb | RocksDB |
| --- | --- | --- | --- | --- |
| 128 B | 1 outstanding | 353/s, 3.0 / 5.4 | 279/s, 3.8 / 6.3 | 245/s, 4.0 / 7.1 |
| 128 B | 1,024 outstanding | 158.1k/s, 5.8 / 17.0 | 101.5k/s, 9.4 / 15.9 | 165.6k/s, 5.7 / 13.8 |
| 128 B | 10,000/s | 10.0k/s, 6.5 / 13.7 | 10.0k/s, 8.4 / 21.3 | 10.0k/s, 6.3 / 12.2 |
| 128 B | 50,000/s | 50.0k/s, 7.0 / 17.1 | 50.0k/s, 12.5 / 24.9 | 50.0k/s, 6.4 / 16.9 |
| 1 KiB | 1 outstanding | 359/s, 3.0 / 4.0 | 472/s, 1.9 / 4.1 | 569/s, 1.2 / 3.4 |
| 1 KiB | 1,024 outstanding | 88.3k/s, 7.7 / 40.4 | 67.3k/s, 14.5 / 24.5 | 116.4k/s, 5.8 / 29.7 |
| 1 KiB | 10,000/s | 10.0k/s, 5.8 / 25.1 | 10.0k/s, 7.8 / 19.5 | 10.0k/s, 6.2 / 13.3 |
| 1 KiB | 50,000/s | 50.0k/s, 7.8 / 40.6 | 50.0k/s, 15.7 / 39.8 | 50.0k/s, 4.3 / 10.0 |

- **Throughput comes from batch size, not from faster flushes.** With one write
  outstanding an engine does 245 to 570 a second, one flush each; with 1,024
  outstanding one flush carries 1,024 entries, and RocksDB writes 166,000
  entries of 128 B a second at a p99 of 14 ms, fjall 158,000 at 17 ms, redb
  102,000 at 16 ms (of 1 KiB: 116,000, 88,000 and 67,000).
- **At 50,000 entries a second, a node's share of a storm,** RocksDB's p99 was
  17 ms for 128 B entries and 10 ms for 1 KiB ones, fjall's 17 and 41 ms,
  redb's 25 and 40 ms. This step moves with the machine more than any other:
  the first revision, in a busier hour, measured RocksDB at 20 and 83 ms, fjall
  at 44 and 289 ms, and redb falling behind with 1 KiB entries (13,800 a
  second). RocksDB's tail at this rate was the shortest or level in both.
- **fjall's tail is its backpressure.** When four sealed memtables wait to be
  flushed, fjall stops the writer in 100 ms sleeps (`local_backpressure` in
  fjall 3.1.10, `src/keyspace/mod.rs`), and single batch writes then took up to
  240 ms here (620 in the first revision). RocksDB had single writes as long in
  this step (one of 516 ms), but over ten minutes of queue churn it stalled in
  2 or 3 seconds where fjall stalled in 34 to 49 (section 6).
- **redb pays for copy-on-write on every commit.** A commit rewrites the pages
  from each touched leaf to the root: about 85 KB written for a one-entry commit
  against 16 KB for the LSM engines (most of it APFS's own metadata), and 18 to
  146 times the logical bytes on batches of 1,024 down to 64 entries of 128 B,
  against 2 to 3 times.

**The window.** A second run put the three windows side by side within every
configuration (128 B entries, two passes, load average 2 to 6), so that drift
in the flush cost does not pass for an effect of the window.
Each cell is p50 / p99 in milliseconds.

| Load | Engine | No window | 1 ms | 2 ms |
| --- | --- | --- | --- | --- |
| 1 outstanding | fjall | 3.5 / 5.4 | 4.9 / 7.6 | 5.8 / 6.6 |
| 1 outstanding | redb | 3.5 / 6.1 | 4.9 / 6.7 | 6.0 / 7.6 |
| 1 outstanding | RocksDB | 3.5 / 5.4 | 4.0 / 5.8 | 6.0 / 6.6 |
| 10,000/s | fjall | 3.9 / 6.7 | 4.1 / 7.4 | 5.2 / 8.9 |
| 10,000/s | redb | 5.2 / 11.2 | 6.6 / 14.5 | 7.0 / 13.6 |
| 10,000/s | RocksDB | 4.0 / 6.1 | 4.3 / 7.0 | 4.4 / 7.8 |
| 50,000/s | fjall | 3.9 / 65.9 | 4.5 / 9.4 | 8.2 / 19.5 |
| 50,000/s | redb | 9.0 / 21.1 | 11.8 / 23.1 | 13.6 / 24.9 |
| 50,000/s | RocksDB | 3.9 / 9.4 | 6.7 / 14.3 | 9.1 / 22.4 |

- **A window only adds latency here.** With nothing else waiting it adds its own
  length; under load it adds part of it and gains nothing, because a flush takes
  longer than the window and the next batch has gathered by the time the writer
  is free. The p99 mostly rises with the window too, except where one pass's
  stall dominates a cell (fjall's 65.9 is the median of a 122 ms backpressure
  stall in one pass and 9.6 in the other).
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
| entry alone | 53.1k, 4.6 / 10.2 | 22.8k, 10.8 / 17.4 | 59.9k, 4.0 / 8.9 | 6.7 / 12.7 | 13.2 / 27.0 | 6.4 / 13.6 |
| combined | 49.2k, 5.0 / 13.9 | 14.0k, 18.0 / 30.6 | 49.1k, 4.9 / 12.2 | 7.4 / 17.0 | 51.8 / 88.9 | 6.4 / 12.4 |
| split, lazy | 44.4k, 5.1 / 13.2 | 14.5k, 17.3 / 33.4 | 45.7k, 5.0 / 11.1 | 7.0 / 12.6 | 36.8 / 65.7 | 6.3 / 12.2 |
| split, synced | 22.3k, 10.9 / 17.0 | 11.5k, 21.6 / 38.0 | 30.4k, 8.0 / 12.2 | 13.9 / 25.3 | 52.6 / 101 | 12.7 / 22.0 |

- **All three engines can share one flush.** fjall's journal and RocksDB's
  write-ahead log are shared by every keyspace, so a batch across keyspaces is
  atomic under one sync, and a state write without a sync becomes durable with
  the next log flush. redb commits every table in one transaction, and a
  non-durable commit is persisted by the next durable one.
- **Sharing is nearly free in the LSM engines and a second flush is not.** At
  20,000 a second, combined and lazy have the entry alone's latency (6.3 to
  7.4 ms at p50); with 256 outstanding they carry 7 to 24 percent less, the
  cost of writing the state at all. A second flush doubles the median
  latency and takes half the throughput (49 to 58 percent). In redb the state
  writes themselves are the cost: a random key is a new leaf per update.

### 4. The claim storm

A million sessions exist; claims arrive on schedule, each reading `own/{cid}`,
then appending the claim to the Raft log and writing the new `own` in one batch,
durable before it counts. Four apply threads stand in for partition state
machines (a client id always lands on the same one); one thread is the most a
single partition's state machine applies. An engine runs its rates as one block
of a few minutes and stops at the first it cannot keep up with, so the step ran
twice, the second pass with the engines in reverse order. Each cell is claims
done a second, then p50 / p99 in milliseconds.

| Offered, 4 threads | fjall, pass 1 | pass 2 | redb, pass 1 | pass 2 | RocksDB, pass 1 | pass 2 |
| --- | --- | --- | --- | --- | --- | --- |
| 10,000/s | 10.0k, 2.5 / 32.6 | 10.0k, 6.5 / 18.4 | 10.0k, 40.7 / 146 | 10.0k, 34.1 / 63.7 | 10.0k, 6.4 / 31.6 | 10.0k, 6.3 / 12.9 |
| 25,000/s | 25.0k, 2.3 / 7.8 | 25.0k, 6.3 / 11.5 | 24.7k, 238 / 373 | 25.3k, 330 / 771 | 25.0k, 6.5 / 14.0 | 25.0k, 6.2 / 11.5 |
| 50,000/s | 50.0k, 4.1 / 69.7 | 50.0k, 7.4 / 76.0 | 34.8k, 3,322 / 5,456 | 32.8k, 3,826 / 5,951 | 50.0k, 6.5 / 16.4 | 50.0k, 6.3 / 14.8 |
| 100,000/s | 100.0k, 5.5 / 467 | 100.0k, 8.3 / 73.3 | not run | not run | 100.0k, 7.2 / 18.7 | 100.0k, 7.1 / 16.3 |
| 200,000/s | 200.7k, 10.0 / 106 | 200.7k, 17.6 / 328 | not run | not run | 200.0k, 10.2 / 27.7 | 200.2k, 9.9 / 30.0 |

| Offered, 1 thread | fjall, pass 1 | pass 2 | redb, pass 1 | pass 2 | RocksDB, pass 1 | pass 2 |
| --- | --- | --- | --- | --- | --- | --- |
| 5,000/s | 5.0k, 5.1 / 10.9 | 5.0k, 7.0 / 19.6 | 5.0k, 7.7 / 19.6 | 5.0k, 11.1 / 70.1 | 5.0k, 6.6 / 26.2 | 5.0k, 3.4 / 8.3 |
| 10,000/s | 10.0k, 6.4 / 12.0 | 10.0k, 6.4 / 12.0 | 10.0k, 28.2 / 69.0 | 10.0k, 57.4 / 279 | 10.0k, 6.3 / 12.7 | 10.0k, 2.7 / 10.1 |
| 25,000/s | 25.0k, 6.5 / 12.4 | 25.0k, 6.3 / 13.3 | 24.9k, 160 / 264 | 18.3k, 2,750 / 4,862 | 25.0k, 6.2 / 10.9 | 25.0k, 2.5 / 8.4 |
| 50,000/s | 50.0k, 12.0 / 183 | 50.0k, 4.1 / 34.4 | 36.9k, 2,177 / 3,793 | not run | 50.0k, 6.3 / 14.7 | 50.0k, 3.4 / 7.5 |
| 100,000/s | 100.2k, 38.3 / 198 | 100.0k, 4.7 / 102 | not run | not run | 99.9k, 7.1 / 17.4 | 100.0k, 3.5 / 9.1 |

- **One node's engine is not what limits a storm.** RocksDB applied 200,000
  claims a second at a p99 of 28 and 30 ms in the two passes, four times the
  50,000 a second the whole cluster has to absorb, and a single partition's
  apply thread kept up with 100,000 a second (p99 17 and 9 ms). fjall kept up
  at every rate too, but from 50,000 a second up its p99 was 34 to 467 ms
  against RocksDB's 7.5 to 30. redb was at its limit at 25,000 a second with
  four threads (p50 238 and 330 ms) and fell behind by 50,000; with one thread
  it fell behind at 25,000 in one of its passes.
- **The machine's flush cost moved during the step, and the passes show it.**
  RocksDB's one-thread p50 was 6.2 to 7.1 ms in one pass and 2.5 to 3.5 ms in
  the other, minutes apart, and fjall's four-thread p50 up to 100,000 a second
  2.3 to 5.5 ms in one and 6.3 to 8.3 in the other. Medians compare engines only
  within a pass; the tails and the rates each engine kept up with agree across
  passes.
- **Reads are not the cost.** With a million sessions the `own` lookups hit the
  cache (1 to 6 us at p50); the claim's cost is its share of a flush. With ten
  million and a 64 MiB cache, where most lookups miss it, a read took 23 us on
  average in either LSM engine (section 5).
- **Per partition leader, the engine allows 100,000 claims a second**; with 256
  partitions a storm of 50,000 a second is about 200 a second per partition, so
  replication, not the engine, sets the per-partition limit (section 8).
- The first revision ran one pass, when a flush was cheaper (RocksDB's p50 at
  200,000 a second was 5.5 ms, its p99 12 ms), and dropped the claims in flight
  at a window's edges. For an engine that kept up that was under half a percent
  of a window's claims; for redb, which fell behind, it was the slowest part of
  every window. The ranking is the same.

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

Ten minutes per engine, twice, the second pass in reverse engine order: 10,000
messages a second for 50,000 offline sessions, each message a 256 B
`msg/{seq}` and a `q/{cid}/{seq}` entry, durable through group commit; every
session reconnects after 5 to 60 s, waits until everything queued for it is
durable, scans its queue and range-deletes what it scanned; once a second,
message bodies below the oldest queued sequence number are range-deleted in all
256 partitions. About 200,000 messages stay queued at any time. Latency in
milliseconds.

| Engine | Pass | Write p50 | p99 | p99.9 | max | Seconds with a write over 100 ms | Drain p99 | Scan p99 | Collection p99 | Disk, end / peak MiB | CPU cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fjall | 1 | 6.2 | 56.5 | 171 | 352 | 49 | 51.4 | 0.07 | 154 | 570 / 585 | 0.18 |
| fjall | 2 | 6.7 | 53.4 | 210 | 622 | 34 | 46.4 | 0.06 | 146 | 571 / 585 | 0.18 |
| RocksDB | 1 | 6.5 | 21.6 | 54.2 | 456 | 3 | 21.8 | 2.9 | 21.6 | 272 / 312 | 0.48 |
| RocksDB | 2 | 6.5 | 18.6 | 64.9 | 228 | 2 | 18.4 | 2.8 | 18.6 | 270 / 302 | 0.46 |
| redb | 1 | 54.6 | 900 | 2,724 | 3,271 | 339 | 203 | 0.03 | 935 | 479 / 479 | 0.41 |
| redb | 2 | 54.5 | 230 | 427 | 921 | 338 | 181 | 0.03 | 305 | 355 / 355 | 0.41 |

- **RocksDB has the steadier write path**: 3 and 2 seconds of the 600 had a
  write over 100 ms, against 49 and 34 for fjall and 339 and 338 for redb, and
  its write p99 was 19 to 22 ms against 53 to 57 for fjall. RocksDB's slow
  seconds came late in each run, in one or two bursts (456 ms at worst);
  fjall's were spread through the run, its 100 ms backpressure sleeps again;
  redb kept pace only at a p50 of 55 ms.
- **Space is reclaimed by all three**: after the first two minutes RocksDB held
  170 to 310 MiB for about 200,000 live messages and fjall 210 to 585 MiB, and
  neither grew; redb's file grew in steps when it stalled, to 355 and 479 MiB,
  and was reused in between.
- **Range tombstones cost RocksDB's readers a little**: a queue scan's p99 was
  2.8 and 2.9 ms against 0.06 and 0.07 ms in fjall, because every read that
  crosses a range tombstone checks it until compaction drops it. Draining and
  collecting were two to eight times faster in RocksDB (drain p99 18 to 22
  against 46 to 51 ms, collection 19 to 22 against 146 to 154 ms): each is one
  tombstone, where fjall reads the range and writes a tombstone per key.
- **Every drain found its whole queue.** In the first revision a drain could
  scan before its queue's last writes were durable, and 0.15% of the LSM
  engines' scans and 1.5% of redb's came up short. Waiting for them was rare
  (p99 1 us, p99.9 2 to 3 ms in the LSM engines) and added up to 6 to 12 s of
  the ten minutes there; in redb it added up to 320 s, and its drains fell up
  to 2% behind schedule. The first revision's single pass gave the same
  ranking (5 seconds over 100 ms for RocksDB, 62 for fjall, 351 for redb).

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
| 1 | 1,000/s | lost its leader | 9.9 / 14.4 | 5.5 / 7.5 | 3.5 / 4.4 | 3.4 / 4.2 |
| 1 | 20,000/s | lost its leader | 10.0 / 14.4 | 4.7 / 6.4 | 3.5 / 12.1 | 3.5 / 9.3 |
| 16 | 1,000/s | 5.4 / 9.5 | 5.5 / 10.4 | 3.3 / 6.3 | 3.2 / 3.5 | 3.2 / 3.3 |
| 16 | 20,000/s | lost all 16 leaders | 11.9 / 16.8 | 6.1 / 9.3 | 3.5 / 4.8 | 3.5 / 5.0 |
| 128 | 1,000/s | 5.3 / 7.7 | 5.2 / 7.6 | 3.2 / 5.3 | 3.2 / 3.4 | 3.2 / 3.3 |
| 128 | 20,000/s | 7.7 / 24.5 | 12.8 / 44.9* | 4.8 / 72.8* | 3.7 / 34.5* | 3.4 / 4.0 |
| 2 ms one way | | | | | | |
| 1 | 1,000/s | lost its leader | 15.2 / 23.4 | 8.0 / 10.9 | 5.6 / 6.5 | 5.5 / 14.0* |
| 1 | 20,000/s | lost its leader | 15.6 / 23.4 | 8.1 / 11.0 | 5.7 / 6.3 | 5.6 / 6.2 |
| 16 | 1,000/s | 7.2 / 12.4 | 7.3 / 16.0 | 5.4 / 10.2 | 5.2 / 6.1 | 5.2 / 6.7 |
| 16 | 20,000/s | lost all 16 leaders | 15.8 / 23.5 | 7.6 / 14.1 | 5.7 / 8.4 | 5.5 / 6.2 |
| 128 | 1,000/s | 6.8 / 10.5 | 6.8 / 10.8 | 5.2 / 9.2 | 5.2 / 6.2 | 5.2 / 6.1 |
| 128 | 20,000/s | 10.3 / 22.6 | 13.0 / 22.3 | 6.7 / 10.3 | 5.7 / 81.2* | 5.5 / 44.2* |

An asterisk marks a run in which the delay line itself delivered hops over a
millisecond late at p99 while other work took the machine: its tail measures
the machine, not the scheme.

**The storm.** 50,000 writes a second across the cluster, 1 ms one way, 1 ms
flush: p50 / p99 / p99.9 in milliseconds, and the CPU of all three nodes
(in-process, without the delay line's thread).

| Groups | openraft 0.9 | 0.9, batching proposer | 0.10, sequential | 0.10, pipelined | primary-backup |
| --- | --- | --- | --- | --- | --- |
| 16 | lost all 16 leaders | 11.9 / 17.0 / 43.3, 0.7 cores | 6.1 / 9.7 / 14.5, 1.1 cores | 3.6 / 4.6 / 8.6, 3.4 cores | 3.6 / 4.8 / 7.9, 1.0 cores |
| 128 | lost 121 of 128 leaders | 9.5 / 14.3 / 14.9, 2.4 cores | 5.1 / 7.8 / 8.3, 3.5 cores | 3.8 / 5.1 / 7.9, 6.0 cores | 3.5 / 4.1 / 4.6, 0.8 cores |
| 256 | 7.1 / 23.0 / 34.8, 3.0 cores | 7.6 / 13.9 / 19.3, 3.6 cores | 4.8 / 9.3 / 20.7, 5.2 cores | 4.0 / 7.1 / 11.6, 6.7 cores | 3.5 / 4.2 / 5.8, 0.9 cores |

**One partition leader at its limit.** One group, writes always outstanding,
1 ms one way: writes a second, then p50 / p99 in milliseconds.

| Flush | Outstanding | openraft 0.9 | 0.9, batching proposer | 0.10, sequential | 0.10, pipelined | primary-backup |
| --- | --- | --- | --- | --- | --- | --- |
| 0 ms | 16 | 2.7k/s, 6.2 / 7.8 | 2.1k/s, 7.7 / 10.6 | 4.1k/s, 3.9 / 5.5 | 6.1k/s, 2.6 / 3.1 | 6.8k/s, 2.4 / 2.8 |
| 0 ms | 256 | 49k/s, 5.2 / 7.6 | 36k/s, 7.5 / 11.8 | 55k/s, 4.8 / 6.1 | 118k/s, 2.1 / 2.6 | 120k/s, 2.1 / 2.7 |
| 0 ms | 1,024 | 116k/s, 8.0 / 13.0 | 147k/s, 7.4 / 11.5 | 112k/s, 8.4 / 16.9 | 439k/s, 2.2 / 7.3 | 258k/s, 3.8 / 6.7 |
| 1 ms | 16 | 688/s, 23.2 / 25.5 | 1.4k/s, 11.2 / 16.2 | 2.8k/s, 5.3 / 7.5 | 3.7k/s, 3.9 / 5.3 | 3.5k/s, 4.8 / 5.9 |
| 1 ms | 256 | 768/s, 332 / 341 | 24k/s, 10.1 / 21.0 | 38k/s, 7.4 / 8.9 | 53k/s, 4.9 / 5.6 | 61k/s, 4.3 / 5.4 |
| 1 ms | 1,024 | lost its leader | 98k/s, 10.0 / 23.2 | 78k/s, 11.8 / 19.9 | 215k/s, 4.6 / 10.4 | 208k/s, 4.8 / 7.0 |

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
  that batches waiting writes into one entry keeps 0.9 stable, at 1.3 to 3.4
  times the median latency of pipelined 0.10.
- **openraft 0.10 does not block on the flush** ("submit IO request, do not
  wait for the response") and batches queued writes into one append. With the
  default network it commits in about 1.5 round trips; with appends pipelined
  it commits in one round trip plus one flush, like primary-backup.
- **At p50, pipelined openraft 0.10 and primary-backup cannot be told apart**:
  3.2 to 3.7 ms against 3.2 to 3.5 at 1 ms one way, 5.2 to 5.7 against 5.2 to
  5.6 at 2 ms, and 4.0 against 3.5 ms in the storm across 256 groups. One
  partition leader commits 53,000 to 61,000 writes a second with 256
  outstanding, against about 200 a second per partition in a storm.
- **Primary-backup is cheaper and has the tighter tail.** In the storm it used
  0.8 to 1.0 cores for three nodes against 3.4 to 6.7 for pipelined openraft
  0.10 (at 256 groups 2.2 cores per node, 45 us per write per node), and its
  p99 stayed at 4.1 to 4.8 ms where openraft's reached 4.6 to 7.1. Idle, it
  costs nothing, against 0.09 cores per node for 256 groups at a 250 ms
  heartbeat (0.63 at 50 ms).
- **The CPU gap is the protocol's, not the storage's.** In the first revision
  primary-backup's backups dropped the entries and only its primary applied
  them; with every replica storing and applying every entry, as Raft's do, its
  CPU in the storm stayed at 0.8 to 1.0 cores (a check run over 256 groups
  counted 499,726 to 499,731 of the 500,000 committed entries applied on each
  node, the rest each group's last few). openraft spends it elsewhere: it
  sent 201,000 messages a second in the storm over 256 groups against
  primary-backup's 102,000 coalesced ones, and some of it is the spike's
  pipelining plumbing (the sequential variant used 5.2 cores, not 6.7). None of
  it is serialisation or sockets.
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
  needs at a p99 of 28 to 30 ms, against 106 to 328 for fjall at the same rate,
  and at 50,000 durable entries a second its tail was the shortest or level
  with fjall's (17 ms p99 for 128 B entries, and 10 ms for 1 KiB against
  fjall's 41).
- **It is the smallest on disk, with fjall**: 108 bytes an idle session against
  112 for fjall and 339 for redb, so 30 GiB for 10^8 sessions at three
  replicas. It is the only one whose memory grows with sessions (4 bytes each,
  index and filters), which a cache setting turns into a fixed budget.
- **It stalls least.** Over ten minutes of queue churn it had 3 and 2 seconds
  with a write over 100 ms in two passes, against 49 and 34 for fjall and 339
  and 338 for redb, and its write p99 was 19 to 22 ms against 53 to 57 and 230
  to 900.
- **Range deletes are native.** Draining an offline queue and collecting
  message bodies below the lowest cursor are one range tombstone each, two to
  eight times faster than fjall's read-then-tombstone-each-key; the price is a
  slower scan across fresh tombstones (2.8 to 2.9 ms p99 against 0.06 to
  0.07).
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
  it writes 18 to 146 times the logical bytes on small batches, is at its limit
  by 25,000 claims a second, had not finished loading ten million sessions after
  33 minutes where RocksDB took 21 s, and took 18 minutes to reopen 10 GiB after
  `SIGKILL` unless quick repair doubles the flushes of every commit.

### Replication: openraft 0.10, one group per partition

- **Same latency as the simpler scheme.** Primary-backup's only real advantage
  was that its lanes pipelined; given a pipelined transport, openraft commits in
  one round trip plus one flush, as primary-backup does, and one partition
  leader commits 53,000 writes a second, over 250 times its share of a storm.
- **What primary-backup still wins is CPU**: about a seventh of openraft's in
  the storm, doing the same storage and apply work, and nothing at idle.
  Against that, primary-backup's missing half is promotion after a failure:
  fencing the old primary's epoch in the meta group, finding the backup holding
  every committed entry, truncating and catching up the other, and doing the
  same for membership changes and snapshots. That is a consensus protocol, and
  Raft is the proven one; R3 already puts a Raft group (the meta group) under
  the cluster. The CPU is a cost to manage, not a reason
  to own that protocol: 0.09 cores per node idle for 256 groups at a 250 ms
  heartbeat, and 2.2 cores per node across three log nodes in a storm, which
  divides by the number of log nodes and can be cut by batching claims per
  partition into one entry.
- **Not openraft 0.9.** Its core waits for every log flush before it does
  anything else and appends one client write per flush, so a millisecond of
  flush is enough for one group at 1,000 writes a second to lose its leader. A
  batching proposer keeps it alive at 1.3 to 3.4 times the latency, and any
  flush stall still stops that group's heartbeats.
- **0.10 is a pre-release** (0.10.0-alpha.36 on 2026-09-29, after 0.9.25 on
  2026-07-28). `openqtt-log` pins the exact version and keeps openraft's types
  behind its own, so the move to 0.10.0 touches one crate; if 0.10 stalls, the
  pinned release is vendored, and if it fails us, primary-backup is the
  fallback (Open risks says what each costs).
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
  256 partitions took 19 to 22 ms at p99), a read and a tombstone per message in
  fjall (146 to 154 ms).
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
- **openraft 0.10 is a pre-release** (0.10.0-alpha.36), and the decision leans
  on what 0.10 changed: log IO that does not block the core, batched appends,
  pipelined `stream_append`. An alpha may change its API or its behaviour in
  any release, or stop short of 0.10.0. What we do:
  - **Pin it exactly** (`=0.10.0-alpha.36`, as the spike's manifest does) and
    keep its types behind `openqtt-log`'s own, so an upgrade is one crate's
    change, made on purpose once this spike's replication steps pass against
    it, never by `cargo update`.
  - **If it changes** in a way we do not want, stay on the pinned alpha:
    openraft talks only to the cluster's own log nodes, never to a client, so
    only a bug we hit forces a move, and that is the next case.
  - **If it stalls**, vendor the pinned release (MIT or Apache-2.0; four crates,
    about 61,000 lines with its runtime and macros) and fix what we need
    ourselves. That makes us the maintainers of a Raft implementation we did
    not write.
  - **If it fails us outright, the fallback is primary-backup**, not openraft
    0.9, which stops a group's heartbeats during any flush stall and needed a
    batching proposer and 1.3 to 3.4 times the latency to stay up.
    Primary-backup matched pipelined 0.10's latency here and used about a
    seventh of its CPU in the storm, so the fallback costs no performance. It
    costs the half this spike did not build: fencing a primary's epoch in the
    meta group, promoting the backup that holds every committed entry,
    truncating and catching up the other, membership changes and snapshot
    transfer, each proven under R5's fault injection before it carries data.
    That is a consensus protocol of our own to write and prove, which is the
    work choosing openraft avoids.
- **openraft's CPU per write is high in this spike**: about 45 us per write per
  node in the storm, seven times primary-backup's, with both storing and
  applying every entry on every replica. Some of it is the spike's in-memory
  log and pipelining plumbing (sequential appends used 5.2 cores, not 6.7); how
  much is openraft has to be profiled in M3 on the real transport. Batching
  claims per partition into one entry is the lever if it matters.
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
  (a queue scan's p99 was 2.8 to 2.9 ms against 0.06 to 0.07 ms in fjall). A
  partition whose queues drain constantly should be watched for scan latency.
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
Steps run again after review (revision 2 of the measurement code: the fsync,
group commit, window, shared, claim, churn and replication steps) show only
their revision 2 records; the results file keeps the revision 1 records too.
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
| 1 | std sync_data | append | 128 | 1 | 402 | 2.9 | 3.6 | 5.69 |
| 1 | std sync_data | append | 128 | 3 | 382 | 7.2 | 28.9 | 5.8 |
| 1 | std sync_data | append | 4096 | 1 | 515 | 1.5 | 3.5 | 5.81 |
| 1 | std sync_data | append | 4096 | 3 | 408 | 7.1 | 14.5 | 5.83 |
| 1 | std sync_data | append | 65536 | 1 | 489 | 1.8 | 4.0 | 5.44 |
| 1 | std sync_data | append | 65536 | 3 | 367 | 8.1 | 13.6 | 5.24 |
| 1 | std sync_data | append | 1048576 | 1 | 325 | 3.0 | 7.2 | 4.98 |
| 1 | std sync_data | append | 1048576 | 3 | 363 | 8.0 | 14.9 | 5.06 |
| 1 | fsync | append | 128 | 1 | 45976 | 0.02 | 0.03 | 5.14 |
| 1 | fsync | append | 128 | 3 | 98272 | 0.03 | 0.06 | 5.05 |
| 1 | fsync | append | 4096 | 1 | 35226 | 0.03 | 0.08 | 5.04 |
| 1 | fsync | append | 4096 | 3 | 70940 | 0.03 | 0.14 | 4.96 |
| 1 | fsync | append | 65536 | 1 | 11829 | 0.04 | 0.89 | 4.96 |
| 1 | fsync | append | 65536 | 3 | 18653 | 0.05 | 0.62 | 5.77 |
| 1 | fsync | append | 1048576 | 1 | 1491 | 0.29 | 3.9 | 5.79 |
| 1 | fsync | append | 1048576 | 3 | 1036 | 0.91 | 14.9 | 5.56 |
| 1 | F_FULLFSYNC | append | 128 | 1 | 232 | 4.0 | 9.0 | 5.52 |
| 1 | F_FULLFSYNC | append | 128 | 3 | 244 | 12.0 | 21.4 | 5.4 |
| 1 | F_FULLFSYNC | append | 4096 | 1 | 240 | 4.0 | 7.6 | 5.28 |
| 1 | F_FULLFSYNC | append | 4096 | 3 | 268 | 12.0 | 16.2 | 5.34 |
| 1 | F_FULLFSYNC | append | 65536 | 1 | 245 | 4.0 | 7.1 | 5.15 |
| 1 | F_FULLFSYNC | append | 65536 | 3 | 255 | 12.0 | 18.0 | 4.82 |
| 1 | F_FULLFSYNC | append | 1048576 | 1 | 241 | 4.0 | 8.2 | 4.75 |
| 1 | F_FULLFSYNC | append | 1048576 | 3 | 228 | 12.6 | 24.9 | 4.53 |
| 1 | F_BARRIERFSYNC | append | 128 | 1 | 6420 | 0.14 | 0.40 | 4.41 |
| 1 | F_BARRIERFSYNC | append | 128 | 3 | 6103 | 0.46 | 0.80 | 4.46 |
| 1 | F_BARRIERFSYNC | append | 4096 | 1 | 6744 | 0.13 | 0.33 | 4.42 |
| 1 | F_BARRIERFSYNC | append | 4096 | 3 | 5177 | 0.50 | 1.8 | 4.15 |
| 1 | F_BARRIERFSYNC | append | 65536 | 1 | 4007 | 0.17 | 0.94 | 3.97 |
| 1 | F_BARRIERFSYNC | append | 65536 | 3 | 3892 | 0.65 | 3.8 | 3.9 |
| 1 | F_BARRIERFSYNC | append | 1048576 | 1 | 586 | 1.1 | 5.6 | 3.74 |
| 1 | F_BARRIERFSYNC | append | 1048576 | 3 | 560 | 4.7 | 14.6 | 3.52 |
| 1 | std sync_data | prealloc | 128 | 1 | 245 | 4.0 | 7.2 | 3.64 |
| 1 | std sync_data | prealloc | 128 | 3 | 361 | 8.0 | 13.2 | 3.67 |
| 1 | std sync_data | prealloc | 4096 | 1 | 244 | 4.0 | 7.1 | 3.94 |
| 1 | std sync_data | prealloc | 4096 | 3 | 356 | 8.0 | 14.0 | 4.02 |
| 1 | std sync_data | prealloc | 65536 | 1 | 239 | 4.0 | 7.3 | 4.18 |
| 1 | std sync_data | prealloc | 65536 | 3 | 365 | 8.0 | 12.1 | 4.01 |
| 1 | std sync_data | prealloc | 1048576 | 1 | 242 | 4.0 | 7.4 | 4.0 |
| 1 | std sync_data | prealloc | 1048576 | 3 | 232 | 12.1 | 33.5 | 5.69 |
| 1 | fsync | prealloc | 128 | 1 | 34345 | 0.02 | 0.06 | 7.63 |
| 1 | fsync | prealloc | 128 | 3 | 93201 | 0.03 | 0.09 | 7.74 |
| 1 | fsync | prealloc | 4096 | 1 | 42076 | 0.02 | 0.05 | 7.36 |
| 1 | fsync | prealloc | 4096 | 3 | 44564 | 0.03 | 0.11 | 7.01 |
| 1 | fsync | prealloc | 65536 | 1 | 28369 | 0.03 | 0.06 | 6.85 |
| 1 | fsync | prealloc | 65536 | 3 | 57650 | 0.04 | 0.24 | 6.54 |
| 1 | fsync | prealloc | 1048576 | 1 | 4328 | 0.23 | 0.34 | 6.26 |
| 1 | fsync | prealloc | 1048576 | 3 | 5572 | 0.39 | 1.5 | 6.08 |
| 1 | F_FULLFSYNC | prealloc | 128 | 1 | 225 | 4.0 | 9.2 | 5.75 |
| 1 | F_FULLFSYNC | prealloc | 128 | 3 | 339 | 8.0 | 15.1 | 6.09 |
| 1 | F_FULLFSYNC | prealloc | 4096 | 1 | 229 | 4.0 | 7.2 | 5.84 |
| 1 | F_FULLFSYNC | prealloc | 4096 | 3 | 341 | 8.0 | 15.0 | 5.7 |
| 1 | F_FULLFSYNC | prealloc | 65536 | 1 | 239 | 4.0 | 7.3 | 5.48 |
| 1 | F_FULLFSYNC | prealloc | 65536 | 3 | 336 | 8.1 | 17.9 | 5.36 |
| 1 | F_FULLFSYNC | prealloc | 1048576 | 1 | 195 | 5.0 | 9.3 | 5.41 |
| 1 | F_FULLFSYNC | prealloc | 1048576 | 3 | 264 | 12.0 | 20.8 | 5.06 |
| 1 | F_BARRIERFSYNC | prealloc | 128 | 1 | 4869 | 0.17 | 0.44 | 4.89 |
| 1 | F_BARRIERFSYNC | prealloc | 128 | 3 | 8374 | 0.32 | 0.70 | 4.9 |
| 1 | F_BARRIERFSYNC | prealloc | 4096 | 1 | 4523 | 0.21 | 0.42 | 4.75 |
| 1 | F_BARRIERFSYNC | prealloc | 4096 | 3 | 7732 | 0.34 | 0.86 | 4.61 |
| 1 | F_BARRIERFSYNC | prealloc | 65536 | 1 | 3590 | 0.22 | 0.86 | 5.12 |
| 1 | F_BARRIERFSYNC | prealloc | 65536 | 3 | 4961 | 0.43 | 4.7 | 5.51 |
| 1 | F_BARRIERFSYNC | prealloc | 1048576 | 1 | 564 | 1.4 | 7.5 | 5.31 |
| 1 | F_BARRIERFSYNC | prealloc | 1048576 | 3 | 563 | 5.3 | 14.0 | 5.12 |
| 2 | std sync_data | append | 128 | 1 | 227 | 4.0 | 8.8 | 4.38 |
| 2 | std sync_data | append | 128 | 3 | 227 | 13.0 | 22.0 | 4.35 |
| 2 | std sync_data | append | 4096 | 1 | 231 | 4.0 | 8.5 | 4.72 |
| 2 | std sync_data | append | 4096 | 3 | 252 | 12.0 | 18.7 | 4.75 |
| 2 | std sync_data | append | 65536 | 1 | 236 | 4.0 | 7.7 | 4.37 |
| 2 | std sync_data | append | 65536 | 3 | 245 | 12.0 | 18.3 | 4.18 |
| 2 | std sync_data | append | 1048576 | 1 | 190 | 5.0 | 11.0 | 4.24 |
| 2 | std sync_data | append | 1048576 | 3 | 186 | 14.9 | 36.4 | 4.54 |
| 2 | fsync | append | 128 | 1 | 41088 | 0.02 | 0.04 | 4.5 |
| 2 | fsync | append | 128 | 3 | 97253 | 0.03 | 0.06 | 4.22 |
| 2 | fsync | append | 4096 | 1 | 36716 | 0.03 | 0.05 | 4.44 |
| 2 | fsync | append | 4096 | 3 | 78132 | 0.03 | 0.07 | 4.33 |
| 2 | fsync | append | 65536 | 1 | 11302 | 0.04 | 1.1 | 4.14 |
| 2 | fsync | append | 65536 | 3 | 18794 | 0.06 | 0.79 | 3.89 |
| 2 | fsync | append | 1048576 | 1 | 1296 | 0.36 | 4.1 | 3.74 |
| 2 | fsync | append | 1048576 | 3 | 1342 | 0.85 | 13.2 | 3.6 |
| 2 | F_FULLFSYNC | append | 128 | 1 | 238 | 4.0 | 8.0 | 3.31 |
| 2 | F_FULLFSYNC | append | 128 | 3 | 243 | 12.0 | 22.0 | 3.28 |
| 2 | F_FULLFSYNC | append | 4096 | 1 | 236 | 4.0 | 8.1 | 3.18 |
| 2 | F_FULLFSYNC | append | 4096 | 3 | 244 | 12.0 | 26.1 | 3.49 |
| 2 | F_FULLFSYNC | append | 65536 | 1 | 235 | 4.0 | 8.3 | 3.45 |
| 2 | F_FULLFSYNC | append | 65536 | 3 | 239 | 12.0 | 24.0 | 3.49 |
| 2 | F_FULLFSYNC | append | 1048576 | 1 | 199 | 4.9 | 10.6 | 3.29 |
| 2 | F_FULLFSYNC | append | 1048576 | 3 | 224 | 13.0 | 21.0 | 3.27 |
| 2 | F_BARRIERFSYNC | append | 128 | 1 | 6338 | 0.14 | 0.35 | 3.57 |
| 2 | F_BARRIERFSYNC | append | 128 | 3 | 5605 | 0.47 | 1.2 | 3.28 |
| 2 | F_BARRIERFSYNC | append | 4096 | 1 | 6363 | 0.14 | 0.35 | 3.26 |
| 2 | F_BARRIERFSYNC | append | 4096 | 3 | 5869 | 0.45 | 1.4 | 3.4 |
| 2 | F_BARRIERFSYNC | append | 65536 | 1 | 4250 | 0.17 | 1.4 | 3.37 |
| 2 | F_BARRIERFSYNC | append | 65536 | 3 | 3962 | 0.61 | 3.8 | 3.82 |
| 2 | F_BARRIERFSYNC | append | 1048576 | 1 | 560 | 0.92 | 6.3 | 3.51 |
| 2 | F_BARRIERFSYNC | append | 1048576 | 3 | 548 | 5.0 | 18.9 | 3.63 |
| 2 | std sync_data | prealloc | 128 | 1 | 239 | 4.0 | 7.3 | 3.5 |
| 2 | std sync_data | prealloc | 128 | 3 | 328 | 8.0 | 22.6 | 3.3 |
| 2 | std sync_data | prealloc | 4096 | 1 | 241 | 4.0 | 7.3 | 3.19 |
| 2 | std sync_data | prealloc | 4096 | 3 | 341 | 8.0 | 15.2 | 3.1 |
| 2 | std sync_data | prealloc | 65536 | 1 | 235 | 4.0 | 8.1 | 2.93 |
| 2 | std sync_data | prealloc | 65536 | 3 | 318 | 8.0 | 21.0 | 2.7 |
| 2 | std sync_data | prealloc | 1048576 | 1 | 216 | 4.4 | 8.3 | 2.72 |
| 2 | std sync_data | prealloc | 1048576 | 3 | 240 | 12.0 | 23.0 | 2.9 |
| 2 | fsync | prealloc | 128 | 1 | 49486 | 0.02 | 0.03 | 2.83 |
| 2 | fsync | prealloc | 128 | 3 | 116966 | 0.02 | 0.05 | 3.4 |
| 2 | fsync | prealloc | 4096 | 1 | 46585 | 0.02 | 0.03 | 3.13 |
| 2 | fsync | prealloc | 4096 | 3 | 113380 | 0.02 | 0.05 | 3.12 |
| 2 | fsync | prealloc | 65536 | 1 | 32430 | 0.03 | 0.04 | 2.95 |
| 2 | fsync | prealloc | 65536 | 3 | 72726 | 0.04 | 0.08 | 2.87 |
| 2 | fsync | prealloc | 1048576 | 1 | 4460 | 0.22 | 0.25 | 3.04 |
| 2 | fsync | prealloc | 1048576 | 3 | 7079 | 0.39 | 0.77 | 3.12 |
| 2 | F_FULLFSYNC | prealloc | 128 | 1 | 240 | 4.0 | 8.1 | 2.95 |
| 2 | F_FULLFSYNC | prealloc | 128 | 3 | 336 | 8.0 | 16.6 | 2.71 |
| 2 | F_FULLFSYNC | prealloc | 4096 | 1 | 239 | 4.0 | 7.3 | 2.66 |
| 2 | F_FULLFSYNC | prealloc | 4096 | 3 | 347 | 8.0 | 15.0 | 2.44 |
| 2 | F_FULLFSYNC | prealloc | 65536 | 1 | 241 | 4.0 | 7.1 | 2.65 |
| 2 | F_FULLFSYNC | prealloc | 65536 | 3 | 321 | 8.0 | 17.2 | 2.76 |
| 2 | F_FULLFSYNC | prealloc | 1048576 | 1 | 217 | 4.1 | 10.0 | 2.7 |
| 2 | F_FULLFSYNC | prealloc | 1048576 | 3 | 246 | 12.0 | 18.9 | 2.96 |
| 2 | F_BARRIERFSYNC | prealloc | 128 | 1 | 6210 | 0.15 | 0.30 | 2.88 |
| 2 | F_BARRIERFSYNC | prealloc | 128 | 3 | 8363 | 0.30 | 0.99 | 2.89 |
| 2 | F_BARRIERFSYNC | prealloc | 4096 | 1 | 6214 | 0.15 | 0.31 | 2.74 |
| 2 | F_BARRIERFSYNC | prealloc | 4096 | 3 | 7418 | 0.30 | 1.9 | 2.68 |
| 2 | F_BARRIERFSYNC | prealloc | 65536 | 1 | 4377 | 0.16 | 1.1 | 2.71 |
| 2 | F_BARRIERFSYNC | prealloc | 65536 | 3 | 5312 | 0.47 | 2.0 | 2.89 |
| 2 | F_BARRIERFSYNC | prealloc | 1048576 | 1 | 644 | 1.4 | 5.1 | 2.9 |
| 2 | F_BARRIERFSYNC | prealloc | 1048576 | 3 | 636 | 4.5 | 11.1 | 3.23 |

### Group commit, every window and load (median of three passes)

| Size | Load | Engine | Writes/s | p50 ms | p99 ms | p99.9 ms | p99 range ms | Batch | Write amp |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 128 | closed:1 | fjall | 353 | 3.0 | 5.4 | 11.9 | 3.7 to 7.2 | 1 | 118.3 |
| 128 | closed:1 | redb | 279 | 3.8 | 6.3 | 11.0 | 3.9 to 7.3 | 1 | 617.8 |
| 128 | closed:1 | rocksdb | 245 | 4.0 | 7.1 | 11.9 | 3.5 to 7.2 | 1 | 112.5 |
| 128 | closed:64 | fjall | 13.7k | 4.9 | 7.9 | 11.9 | 4.7 to 8.1 | 64 | 3.0 |
| 128 | closed:64 | redb | 9.9k | 6.0 | 11.2 | 20.0 | 9.9 to 11.7 | 64 | 145.7 |
| 128 | closed:64 | rocksdb | 15.2k | 4.0 | 7.3 | 12.0 | 7.2 to 7.4 | 64 | 2.9 |
| 128 | closed:1024 | fjall | 158.1k | 5.8 | 17.0 | 76.9 | 14.4 to 17.5 | 1024 | 2.5 |
| 128 | closed:1024 | redb | 101.5k | 9.4 | 15.9 | 26.1 | 13.7 to 17.8 | 1024 | 18.2 |
| 128 | closed:1024 | rocksdb | 165.6k | 5.7 | 13.8 | 22.0 | 12.3 to 15.9 | 1024 | 2.2 |
| 128 | open:10000/s | fjall | 10.0k | 6.5 | 13.7 | 29.9 | 6.4 to 25.2 | 44 | 3.8 |
| 128 | open:10000/s | redb | 10.0k | 8.4 | 21.3 | 37.5 | 9.9 to 30.1 | 56 | 136.7 |
| 128 | open:10000/s | rocksdb | 10.0k | 6.3 | 12.2 | 19.2 | 6.1 to 13.0 | 42 | 3.8 |
| 128 | open:50000/s | fjall | 50.0k | 7.0 | 17.1 | 47.0 | 8.6 to 30.1 | 233 | 2.8 |
| 128 | open:50000/s | redb | 50.0k | 12.5 | 24.9 | 30.8 | 16.7 to 26.1 | 416 | 42.7 |
| 128 | open:50000/s | rocksdb | 50.0k | 6.4 | 16.9 | 27.6 | 6.7 to 17.2 | 213 | 2.6 |
| 1024 | closed:1 | fjall | 359 | 3.0 | 4.0 | 9.1 | 3.6 to 5.1 | 1 | 16.9 |
| 1024 | closed:1 | redb | 472 | 1.9 | 4.1 | 8.6 | 3.7 to 4.1 | 1 | 101.6 |
| 1024 | closed:1 | rocksdb | 569 | 1.2 | 3.4 | 7.6 | 3.1 to 3.6 | 1 | 16.8 |
| 1024 | closed:64 | fjall | 20.8k | 3.0 | 6.1 | 21.2 | 5.1 to 6.3 | 64 | 2.7 |
| 1024 | closed:64 | redb | 11.6k | 5.0 | 11.0 | 13.1 | 10.6 to 11.4 | 64 | 25.8 |
| 1024 | closed:64 | rocksdb | 24.7k | 2.9 | 5.6 | 9.2 | 5.1 to 6.2 | 64 | 2.2 |
| 1024 | closed:1024 | fjall | 88.3k | 7.7 | 40.4 | 155 | 37.7 to 73.0 | 1024 | 4.3 |
| 1024 | closed:1024 | redb | 67.3k | 14.5 | 24.5 | 32.4 | 18.1 to 27.0 | 1024 | 3.9 |
| 1024 | closed:1024 | rocksdb | 116.4k | 5.8 | 29.7 | 42.1 | 23.4 to 30.1 | 1024 | 3.9 |
| 1024 | open:10000/s | fjall | 10.0k | 5.8 | 25.1 | 89.2 | 13.4 to 37.1 | 38 | 2.4 |
| 1024 | open:10000/s | redb | 10.0k | 7.8 | 19.5 | 27.8 | 15.8 to 20.8 | 51 | 27.0 |
| 1024 | open:10000/s | rocksdb | 10.0k | 6.2 | 13.3 | 18.1 | 6.7 to 14.7 | 41 | 2.2 |
| 1024 | open:50000/s | fjall | 50.0k | 7.8 | 40.6 | 57.7 | 18.1 to 47.6 | 279 | 3.1 |
| 1024 | open:50000/s | redb | 50.0k | 15.7 | 39.8 | 59.0 | 24.6 to 42.9 | 526 | 5.9 |
| 1024 | open:50000/s | rocksdb | 50.0k | 4.3 | 10.0 | 19.4 | 9.9 to 22.4 | 141 | 2.7 |

#### Group commit window, 128 B (median of passes)

| Load | Engine | 0 ms p50/p99 | 1 ms p50/p99 | 2 ms p50/p99 |
| --- | --- | --- | --- | --- |
| closed:1 | fjall | 3.0 / 5.4 | 5.1 / 9.4 | 6.0 / 6.6 |
| closed:1 | redb | 3.8 / 6.3 | 5.9 / 9.4 | 6.0 / 7.3 |
| closed:1 | rocksdb | 4.0 / 7.1 | 5.6 / 9.2 | 5.7 / 7.2 |
| closed:64 | fjall | 4.9 / 7.9 | 6.0 / 9.9 | 6.0 / 9.5 |
| closed:64 | redb | 6.0 / 11.2 | 8.0 / 14.1 | 7.5 / 13.1 |
| closed:64 | rocksdb | 4.0 / 7.3 | 6.0 / 9.7 | 5.8 / 10.7 |
| open:10000/s | fjall | 6.5 / 13.7 | 4.5 / 8.0 | 5.1 / 10.1 |
| open:10000/s | redb | 8.4 / 21.3 | 6.0 / 12.2 | 6.7 / 32.6 |
| open:10000/s | rocksdb | 6.3 / 12.2 | 4.6 / 7.2 | 4.3 / 8.0 |
| open:50000/s | fjall | 7.0 / 17.1 | 4.0 / 8.9 | 5.0 / 10.9 |
| open:50000/s | redb | 12.5 / 24.9 | 8.9 / 17.8 | 9.7 / 31.7 |
| open:50000/s | rocksdb | 6.4 / 16.9 | 4.1 / 8.4 | 4.9 / 8.9 |

### Shared fsync (median of three passes)

| Load | Shape | Engine | Writes/s | p50 ms | p99 ms | Engine write p50 ms | Write amp |
| --- | --- | --- | --- | --- | --- | --- | --- |
| closed:256 | combined | fjall | 49.2k | 5.0 | 13.9 | 4.9 | 2.6 |
| closed:256 | combined | redb | 14.0k | 18.0 | 30.6 | 17.9 | 143.1 |
| closed:256 | combined | rocksdb | 49.1k | 4.9 | 12.2 | 4.9 | 2.1 |
| closed:256 | log-only | fjall | 53.1k | 4.6 | 10.2 | 4.5 | 2.7 |
| closed:256 | log-only | redb | 22.8k | 10.8 | 17.4 | 10.7 | 102.5 |
| closed:256 | log-only | rocksdb | 59.9k | 4.0 | 8.9 | 3.9 | 2.4 |
| closed:256 | split-lazy | fjall | 44.4k | 5.1 | 13.2 | 5.0 | 2.4 |
| closed:256 | split-lazy | redb | 14.5k | 17.3 | 33.4 | 17.2 | 132.6 |
| closed:256 | split-lazy | rocksdb | 45.7k | 5.0 | 11.1 | 4.9 | 2.1 |
| closed:256 | split-sync | fjall | 22.3k | 10.9 | 17.0 | 10.8 | 2.2 |
| closed:256 | split-sync | redb | 11.5k | 21.6 | 38.0 | 21.6 | 143.6 |
| closed:256 | split-sync | rocksdb | 30.4k | 8.0 | 12.2 | 7.9 | 1.6 |
| open:20000/s | combined | fjall | 20.0k | 7.4 | 17.0 | 4.8 | 2.1 |
| open:20000/s | combined | redb | 19.9k | 51.8 | 88.9 | 33.5 | 96.6 |
| open:20000/s | combined | rocksdb | 20.0k | 6.4 | 12.4 | 4.0 | 1.9 |
| open:20000/s | log-only | fjall | 20.0k | 6.7 | 12.7 | 4.0 | 2.5 |
| open:20000/s | log-only | redb | 20.0k | 13.2 | 27.0 | 8.6 | 116.3 |
| open:20000/s | log-only | rocksdb | 20.0k | 6.4 | 13.6 | 4.0 | 2.4 |
| open:20000/s | split-lazy | fjall | 20.0k | 7.0 | 12.6 | 4.8 | 2.2 |
| open:20000/s | split-lazy | redb | 19.9k | 36.8 | 65.7 | 24.2 | 105.9 |
| open:20000/s | split-lazy | rocksdb | 20.0k | 6.3 | 12.2 | 4.0 | 1.9 |
| open:20000/s | split-sync | fjall | 20.0k | 13.9 | 25.3 | 8.9 | 2.1 |
| open:20000/s | split-sync | redb | 19.9k | 52.6 | 101 | 34.3 | 99.1 |
| open:20000/s | split-sync | rocksdb | 20.0k | 12.7 | 22.0 | 8.0 | 1.9 |

### The claim storm

| Engine | Workers | Pass | Offered/s | Done/s | p50 ms | p99 ms | p99.9 ms | Read p50 us | Batch | CPU cores | Drain s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fjall | 4 | 1 | 10.0k | 10.0k | 2.5 | 32.6 | 48.7 | 5 | 16 | 0.2 | 0.0 |
| fjall | 4 | 2 | 10.0k | 10.0k | 6.5 | 18.4 | 32.9 | 4 | 43 | 0.2 | 0.01 |
| fjall | 4 | 1 | 25.0k | 25.0k | 2.3 | 7.8 | 15.7 | 4 | 38 | 0.29 | 0.0 |
| fjall | 4 | 2 | 25.0k | 25.0k | 6.3 | 11.5 | 19.1 | 4 | 104 | 0.3 | 0.01 |
| fjall | 4 | 1 | 50.0k | 50.0k | 4.1 | 69.7 | 109 | 4 | 126 | 0.6 | 0.0 |
| fjall | 4 | 2 | 50.0k | 50.0k | 7.4 | 76.0 | 101 | 4 | 252 | 0.56 | 0.01 |
| fjall | 4 | 1 | 100.0k | 100.0k | 5.5 | 467 | 536 | 4 | 371 | 0.97 | 0.01 |
| fjall | 4 | 2 | 100.0k | 100.0k | 8.3 | 73.3 | 99.2 | 4 | 565 | 0.95 | 0.01 |
| fjall | 4 | 1 | 200.0k | 200.7k | 10.0 | 106 | 124 | 4 | 1374 | 1.97 | 0.01 |
| fjall | 4 | 2 | 200.0k | 200.7k | 17.6 | 328 | 361 | 4 | 2504 | 1.94 | 0.01 |
| fjall | 1 | 1 | 5.0k | 5.0k | 5.1 | 10.9 | 18.0 | 5 | 14 | 0.14 | 0.01 |
| fjall | 1 | 2 | 5.0k | 5.0k | 7.0 | 19.6 | 31.2 | 5 | 22 | 0.16 | 0.01 |
| fjall | 1 | 1 | 10.0k | 10.0k | 6.4 | 12.0 | 17.6 | 4 | 42 | 0.12 | 0.01 |
| fjall | 1 | 2 | 10.0k | 10.0k | 6.4 | 12.0 | 19.1 | 4 | 42 | 0.13 | 0.01 |
| fjall | 1 | 1 | 25.0k | 25.0k | 6.5 | 12.4 | 17.6 | 4 | 108 | 0.22 | 0.01 |
| fjall | 1 | 2 | 25.0k | 25.0k | 6.3 | 13.3 | 43.5 | 4 | 104 | 0.22 | 0.0 |
| fjall | 1 | 1 | 50.0k | 50.0k | 12.0 | 183 | 246 | 5 | 382 | 0.68 | 0.01 |
| fjall | 1 | 2 | 50.0k | 50.0k | 4.1 | 34.4 | 90.0 | 3 | 133 | 0.4 | 0.0 |
| fjall | 1 | 1 | 100.0k | 100.2k | 38.3 | 198 | 265 | 4 | 1509 | 1.21 | 0.02 |
| fjall | 1 | 2 | 100.0k | 100.0k | 4.7 | 102 | 173 | 3 | 295 | 0.71 | 0.01 |
| redb | 4 | 1 | 10.0k | 10.0k | 40.7 | 146 | 180 | 2 | 275 | 0.55 | 0.03 |
| redb | 4 | 2 | 10.0k | 10.0k | 34.1 | 63.7 | 76.2 | 2 | 227 | 0.5 | 0.03 |
| redb | 4 | 1 | 25.0k | 24.7k | 238 | 373 | 413 | 3 | 3878 | 0.83 | 0.22 |
| redb | 4 | 2 | 25.0k | 25.3k | 330 | 771 | 821 | 3 | 5316 | 0.76 | 0.19 |
| redb | 4 | 1 | 50.0k | 34.8k | 3322 | 5456 | 5513 | 3 | 16384 | 0.93 | 5.29 |
| redb | 4 | 2 | 50.0k | 32.8k | 3826 | 5951 | 6021 | 3 | 16384 | 0.89 | 5.73 |
| redb | 1 | 1 | 5.0k | 5.0k | 7.7 | 19.6 | 24.1 | 2 | 26 | 0.41 | 0.01 |
| redb | 1 | 2 | 5.0k | 5.0k | 11.1 | 70.1 | 102 | 2 | 39 | 0.39 | 0.01 |
| redb | 1 | 1 | 10.0k | 10.0k | 28.2 | 69.0 | 80.1 | 2 | 188 | 0.59 | 0.03 |
| redb | 1 | 2 | 10.0k | 10.0k | 57.4 | 279 | 312 | 2 | 284 | 0.49 | 0.06 |
| redb | 1 | 1 | 25.0k | 24.9k | 160 | 264 | 286 | 2 | 2691 | 0.78 | 0.09 |
| redb | 1 | 2 | 25.0k | 18.3k | 2750 | 4862 | 4936 | 1 | 16263 | 0.48 | 4.36 |
| redb | 1 | 1 | 50.0k | 36.9k | 2177 | 3793 | 3834 | 1 | 16384 | 0.82 | 3.63 |
| rocksdb | 4 | 1 | 10.0k | 10.0k | 6.4 | 31.6 | 51.7 | 4 | 43 | 0.13 | 0.01 |
| rocksdb | 4 | 2 | 10.0k | 10.0k | 6.3 | 12.9 | 33.1 | 4 | 42 | 0.13 | 0.01 |
| rocksdb | 4 | 1 | 25.0k | 25.0k | 6.5 | 14.0 | 23.4 | 4 | 107 | 0.26 | 0.01 |
| rocksdb | 4 | 2 | 25.0k | 25.0k | 6.2 | 11.5 | 18.7 | 4 | 102 | 0.3 | 0.0 |
| rocksdb | 4 | 1 | 50.0k | 50.0k | 6.5 | 16.4 | 22.3 | 3 | 216 | 0.53 | 0.01 |
| rocksdb | 4 | 2 | 50.0k | 50.0k | 6.3 | 14.8 | 21.4 | 3 | 209 | 0.53 | 0.01 |
| rocksdb | 4 | 1 | 100.0k | 100.0k | 7.2 | 18.7 | 28.2 | 3 | 484 | 0.83 | 0.01 |
| rocksdb | 4 | 2 | 100.0k | 100.0k | 7.1 | 16.3 | 31.3 | 3 | 477 | 0.85 | 0.01 |
| rocksdb | 4 | 1 | 200.0k | 200.0k | 10.2 | 27.7 | 36.1 | 3 | 1412 | 1.79 | 0.01 |
| rocksdb | 4 | 2 | 200.0k | 200.2k | 9.9 | 30.0 | 48.6 | 3 | 1375 | 1.78 | 0.01 |
| rocksdb | 1 | 1 | 5.0k | 5.0k | 6.6 | 26.2 | 36.9 | 5 | 22 | 0.09 | 0.01 |
| rocksdb | 1 | 2 | 5.0k | 5.0k | 3.4 | 8.3 | 19.6 | 6 | 9 | 0.12 | 0.0 |
| rocksdb | 1 | 1 | 10.0k | 10.0k | 6.3 | 12.7 | 20.4 | 4 | 41 | 0.12 | 0.01 |
| rocksdb | 1 | 2 | 10.0k | 10.0k | 2.7 | 10.1 | 22.9 | 5 | 16 | 0.15 | 0.0 |
| rocksdb | 1 | 1 | 25.0k | 25.0k | 6.2 | 10.9 | 21.6 | 4 | 102 | 0.22 | 0.01 |
| rocksdb | 1 | 2 | 25.0k | 25.0k | 2.5 | 8.4 | 23.9 | 4 | 40 | 0.24 | 0.0 |
| rocksdb | 1 | 1 | 50.0k | 50.0k | 6.3 | 14.7 | 19.7 | 3 | 211 | 0.41 | 0.01 |
| rocksdb | 1 | 2 | 50.0k | 50.0k | 3.4 | 7.5 | 12.5 | 3 | 93 | 0.39 | 0.0 |
| rocksdb | 1 | 1 | 100.0k | 99.9k | 7.1 | 17.4 | 29.2 | 3 | 472 | 0.67 | 0.01 |
| rocksdb | 1 | 2 | 100.0k | 100.0k | 3.5 | 9.1 | 14.3 | 3 | 211 | 0.65 | 0.0 |

preload fjall: 1000000 sessions in 2.55 s, 238 MiB  
preload redb: 1000000 sessions in 27.89 s, 716 MiB  
preload rocksdb: 1000000 sessions in 2.23 s, 111 MiB  
preload rocksdb: 1000000 sessions in 2.35 s, 111 MiB  
preload redb: 1000000 sessions in 27.7 s, 716 MiB  
preload fjall: 1000000 sessions in 3.11 s, 238 MiB  
preload fjall: 1000000 sessions in 2.57 s, 238 MiB  
preload redb: 1000000 sessions in 36.26 s, 716 MiB  
preload rocksdb: 1000000 sessions in 2.49 s, 111 MiB  
preload rocksdb: 1000000 sessions in 2.39 s, 111 MiB  
preload redb: 1000000 sessions in 74.7 s, 716 MiB  
preload fjall: 1000000 sessions in 2.86 s, 238 MiB  

| Engine | Workers | Pass | Highest rate kept up with | p99 ms there |
| --- | --- | --- | --- | --- |
| fjall | 4 | 1 | 200.0k | 106 |
| fjall | 4 | 2 | 200.0k | 328 |
| fjall | 1 | 1 | 100.0k | 198 |
| fjall | 1 | 2 | 100.0k | 102 |
| redb | 4 | 1 | 10.0k | 146 |
| redb | 4 | 2 | 25.0k | 771 |
| redb | 1 | 1 | 25.0k | 264 |
| redb | 1 | 2 | 10.0k | 279 |
| rocksdb | 4 | 1 | 200.0k | 27.7 |
| rocksdb | 4 | 2 | 200.0k | 30.0 |
| rocksdb | 1 | 1 | 100.0k | 17.4 |
| rocksdb | 1 | 2 | 100.0k | 9.1 |

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

| Engine | Pass | Secs | Published | Write p50 ms | p99 ms | p99.9 ms | max ms | Worst second p99 ms | Seconds with a write over 100 ms | Drain p99 ms | Scan p99 ms | Scans short | GC p99 ms | Disk end MiB | Disk max MiB | CPU |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| fjall | 1 | 600 | 6,000,000 | 6.2 | 56.5 | 171 | 352 | 342 | 49 | 51.4 | 0.07 | 0 | 154 | 570 | 585 | 0.18 |
| fjall | 2 | 600 | 6,000,000 | 6.7 | 53.4 | 210 | 622 | 612 | 34 | 46.4 | 0.06 | 0 | 146 | 571 | 585 | 0.18 |
| redb | 1 | 600 | 6,000,000 | 54.6 | 900 | 2724 | 3271 | 3260 | 339 | 203 | 0.03 | 0 | 935 | 479 | 479 | 0.41 |
| redb | 2 | 600 | 6,000,000 | 54.5 | 230 | 427 | 921 | 911 | 338 | 181 | 0.03 | 0 | 305 | 355 | 355 | 0.41 |
| rocksdb | 1 | 600 | 6,000,000 | 6.5 | 21.6 | 54.2 | 456 | 446 | 3 | 21.8 | 2.9 | 0 | 21.6 | 272 | 312 | 0.48 |
| rocksdb | 2 | 600 | 6,000,000 | 6.5 | 18.6 | 64.9 | 228 | 218 | 2 | 18.4 | 2.8 | 0 | 18.6 | 270 | 302 | 0.46 |

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

| Scheme | Groups | One-way ms | Flush ms | Load | Done/s | p50 ms | p99 ms | p99.9 ms | Errors | Unfinished | Msgs/s | Items/flush | CPU cores (3 nodes) | Hop late p99 us | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| raft | 1 | 1 | 0 | open:1000/s | 1.0k | 3.7 | 6.6 | 7.4 | 0 | 0 | 838 | 1.0 | 0.07 | 284 |  |
| raft-batched | 1 | 1 | 0 | open:1000/s | 1000 | 6.6 | 10.8 | 11.3 | 0 | 0 | 833 | 1.0 | 0.07 | 284 |  |
| raft10-seq | 1 | 1 | 0 | open:1000/s | 1.0k | 3.2 | 5.2 | 8.2 | 0 | 0 | 855 | 1.0 | 0.1 | 647 |  |
| raft10 | 1 | 1 | 0 | open:1000/s | 1.0k | 2.3 | 2.4 | 2.5 | 0 | 0 | 4.0k | 1.0 | 0.15 | 217 |  |
| pb | 1 | 1 | 0 | open:1000/s | 1.0k | 2.1 | 2.2 | 2.2 | 0 | 0 | 4.0k | 1.0 | 0.06 | 103 |  |
| raft | 1 | 1 | 0 | open:20000/s | 20.0k | 4.8 | 7.1 | 9.6 | 0 | 0 | 820 | 1.0 | 0.26 | 287 |  |
| raft-batched | 1 | 1 | 0 | open:20000/s | 20.0k | 6.8 | 10.1 | 11.9 | 0 | 0 | 829 | 1.0 | 0.18 | 287 |  |
| raft10-seq | 1 | 1 | 0 | open:20000/s | 20.0k | 3.8 | 5.3 | 9.4 | 0 | 0 | 856 | 1.37 | 0.34 | 287 |  |
| raft10 | 1 | 1 | 0 | open:20000/s | 20.0k | 2.2 | 3.8 | 11.1 | 0 | 0 | 35.3k | 1.34 | 1.25 | 9 |  |
| pb | 1 | 1 | 0 | open:20000/s | 20.0k | 2.1 | 2.2 | 4.7 | 0 | 0 | 73.8k | 1.04 | 0.47 | 7 |  |
| raft | 1 | 1 | 1 | open:1000/s | 0 | - | - | - | 9999 | 0 | 26 | 0.0 | 0.04 | 291 | 1 leaders moved |
| raft-batched | 1 | 1 | 1 | open:1000/s | 1000 | 9.9 | 14.4 | 15.2 | 0 | 0 | 641 | 1.0 | 0.06 | 283 |  |
| raft10-seq | 1 | 1 | 1 | open:1000/s | 1.0k | 5.5 | 7.5 | 7.7 | 0 | 0 | 594 | 1.12 | 0.08 | 283 |  |
| raft10 | 1 | 1 | 1 | open:1000/s | 1.0k | 3.5 | 4.4 | 4.5 | 0 | 0 | 4.0k | 1.03 | 0.15 | 232 |  |
| pb | 1 | 1 | 1 | open:1000/s | 1.0k | 3.4 | 4.2 | 4.2 | 0 | 0 | 4.0k | 1.02 | 0.07 | 137 |  |
| raft | 1 | 1 | 1 | open:20000/s | 0 | - | - | - | 199999 | 0 | 26 | 1.0 | 0.15 | 285 | 1 leaders moved |
| raft-batched | 1 | 1 | 1 | open:20000/s | 20.0k | 10.0 | 14.4 | 15.2 | 0 | 0 | 647 | 1.0 | 0.18 | 283 |  |
| raft10-seq | 1 | 1 | 1 | open:20000/s | 20.0k | 4.7 | 6.4 | 8.2 | 0 | 0 | 592 | 11.91 | 0.31 | 279 |  |
| raft10 | 1 | 1 | 1 | open:20000/s | 20.0k | 3.5 | 12.1 | 20.6 | 0 | 0 | 26.7k | 13.35 | 0.82 | 75 |  |
| pb | 1 | 1 | 1 | open:20000/s | 20.0k | 3.5 | 9.3 | 15.4 | 0 | 0 | 40.0k | 20.1 | 0.42 | 225 |  |
| raft | 1 | 2 | 0 | open:1000/s | 1.0k | 7.9 | 17.2 | 25.5 | 0 | 0 | 389 | 1.0 | 0.07 | 1213 |  |
| raft-batched | 1 | 2 | 0 | open:1000/s | 999 | 15.0 | 34.3 | 46.3 | 0 | 0 | 364 | 1.0 | 0.08 | 2517 |  |
| raft10-seq | 1 | 2 | 0 | open:1000/s | 1000 | 6.4 | 9.0 | 11.2 | 0 | 0 | 432 | 1.0 | 0.08 | 1008 |  |
| raft10 | 1 | 2 | 0 | open:1000/s | 1.0k | 4.3 | 4.5 | 6.4 | 0 | 0 | 4.0k | 1.0 | 0.18 | 236 |  |
| pb | 1 | 2 | 0 | open:1000/s | 1.0k | 4.1 | 4.2 | 4.4 | 0 | 0 | 4.0k | 1.0 | 0.07 | 71 |  |
| raft | 1 | 2 | 0 | open:20000/s | 20.0k | 8.6 | 16.0 | 20.1 | 0 | 0 | 392 | 1.0 | 0.27 | 786 |  |
| raft-batched | 1 | 2 | 0 | open:20000/s | 20.0k | 13.8 | 21.5 | 25.2 | 0 | 0 | 393 | 1.0 | 0.19 | 787 |  |
| raft10-seq | 1 | 2 | 0 | open:20000/s | 20.0k | 7.5 | 11.1 | 15.9 | 0 | 0 | 428 | 1.39 | 0.36 | 785 |  |
| raft10 | 1 | 2 | 0 | open:20000/s | 20.0k | 4.1 | 4.4 | 7.9 | 0 | 0 | 34.9k | 1.31 | 1.21 | 6 |  |
| pb | 1 | 2 | 0 | open:20000/s | 20.0k | 4.1 | 4.2 | 13.3 | 0 | 0 | 72.8k | 1.05 | 0.51 | 6 |  |
| raft | 1 | 2 | 1 | open:1000/s | 0 | - | - | - | 9999 | 0 | 26 | 0.0 | 0.03 | 778 | 1 leaders moved |
| raft-batched | 1 | 2 | 1 | open:1000/s | 1.0k | 15.2 | 23.4 | 24.8 | 0 | 0 | 392 | 1.0 | 0.05 | 776 |  |
| raft10-seq | 1 | 2 | 1 | open:1000/s | 1.0k | 8.0 | 10.9 | 11.4 | 0 | 0 | 414 | 1.08 | 0.07 | 281 |  |
| raft10 | 1 | 2 | 1 | open:1000/s | 1.0k | 5.6 | 6.5 | 7.4 | 0 | 0 | 4.0k | 1.04 | 0.18 | 227 |  |
| pb | 1 | 2 | 1 | open:1000/s | 1.0k | 5.5 | 14.0 | 17.2 | 0 | 0 | 3.8k | 1.1 | 0.08 | 1467 |  |
| raft | 1 | 2 | 1 | open:20000/s | 0 | - | - | - | 199999 | 0 | 26 | 1.0 | 0.18 | 1596 | 1 leaders moved |
| raft-batched | 1 | 2 | 1 | open:20000/s | 20.0k | 15.6 | 23.4 | 24.7 | 0 | 0 | 394 | 1.0 | 0.18 | 776 |  |
| raft10-seq | 1 | 2 | 1 | open:20000/s | 20.0k | 8.1 | 11.0 | 14.4 | 0 | 0 | 414 | 12.87 | 0.3 | 281 |  |
| raft10 | 1 | 2 | 1 | open:20000/s | 20.0k | 5.7 | 6.3 | 9.1 | 0 | 0 | 27.1k | 13.75 | 0.74 | 8 |  |
| pb | 1 | 2 | 1 | open:20000/s | 20.0k | 5.6 | 6.2 | 8.9 | 0 | 0 | 40.9k | 20.06 | 0.34 | 5 |  |
| raft | 16 | 1 | 0 | open:1000/s | 1.0k | 2.2 | 4.2 | 5.2 | 0 | 0 | 4.1k | 1.0 | 0.13 | 262 |  |
| raft-batched | 16 | 1 | 0 | open:1000/s | 1.0k | 2.2 | 4.4 | 5.4 | 0 | 0 | 4.1k | 1.0 | 0.14 | 258 |  |
| raft10-seq | 16 | 1 | 0 | open:1000/s | 1.0k | 2.2 | 3.8 | 4.3 | 0 | 0 | 4.4k | 1.0 | 0.2 | 248 |  |
| raft10 | 16 | 1 | 0 | open:1000/s | 1.0k | 2.2 | 2.4 | 2.5 | 0 | 0 | 4.6k | 1.0 | 0.22 | 216 |  |
| pb | 16 | 1 | 0 | open:1000/s | 1.0k | 2.1 | 2.3 | 2.7 | 0 | 0 | 4.0k | 1.0 | 0.06 | 106 |  |
| raft | 16 | 1 | 0 | open:20000/s | 20.0k | 3.4 | 6.0 | 7.1 | 0 | 0 | 15.4k | 1.01 | 0.61 | 216 |  |
| raft-batched | 16 | 1 | 0 | open:20000/s | 20.0k | 5.7 | 9.5 | 11.0 | 0 | 0 | 14.6k | 1.06 | 0.56 | 234 |  |
| raft10-seq | 16 | 1 | 0 | open:20000/s | 20.0k | 2.8 | 4.1 | 4.2 | 0 | 0 | 16.2k | 1.11 | 1.09 | 7 |  |
| raft10 | 16 | 1 | 0 | open:20000/s | 20.0k | 2.2 | 2.3 | 2.4 | 0 | 0 | 76.5k | 1.15 | 2.71 | 14 |  |
| pb | 16 | 1 | 0 | open:20000/s | 20.0k | 2.1 | 2.2 | 2.3 | 0 | 0 | 78.4k | 1.01 | 0.4 | 7 |  |
| raft | 16 | 1 | 1 | open:1000/s | 1000 | 5.4 | 9.5 | 10.9 | 0 | 0 | 3.8k | 1.29 | 0.13 | 276 |  |
| raft-batched | 16 | 1 | 1 | open:1000/s | 1.0k | 5.5 | 10.4 | 12.6 | 0 | 0 | 3.9k | 1.3 | 0.13 | 276 |  |
| raft10-seq | 16 | 1 | 1 | open:1000/s | 1.0k | 3.3 | 6.3 | 7.1 | 0 | 0 | 4.2k | 1.23 | 0.2 | 240 |  |
| raft10 | 16 | 1 | 1 | open:1000/s | 1.0k | 3.2 | 3.5 | 3.6 | 0 | 0 | 4.6k | 1.2 | 0.24 | 192 |  |
| pb | 16 | 1 | 1 | open:1000/s | 1.0k | 3.2 | 3.3 | 3.5 | 0 | 0 | 4.0k | 1.2 | 0.07 | 105 |  |
| raft | 16 | 1 | 1 | open:20000/s | 0 | - | - | - | 199981 | 0 | 400 | 0.0 | 0.15 | 281 | 16 leaders moved |
| raft-batched | 16 | 1 | 1 | open:20000/s | 20.0k | 11.9 | 16.8 | 17.5 | 0 | 0 | 8.5k | 3.39 | 0.43 | 251 |  |
| raft10-seq | 16 | 1 | 1 | open:20000/s | 20.0k | 6.1 | 9.3 | 9.8 | 0 | 0 | 7.7k | 10.45 | 0.69 | 244 |  |
| raft10 | 16 | 1 | 1 | open:20000/s | 20.0k | 3.5 | 4.8 | 6.4 | 0 | 0 | 66.8k | 20.01 | 2.03 | 30 |  |
| pb | 16 | 1 | 1 | open:20000/s | 20.0k | 3.5 | 5.0 | 5.9 | 0 | 0 | 45.7k | 20.29 | 0.38 | 41 |  |
| raft | 16 | 2 | 0 | open:1000/s | 1.0k | 4.4 | 9.4 | 11.1 | 0 | 0 | 3.8k | 1.0 | 0.13 | 652 |  |
| raft-batched | 16 | 2 | 0 | open:1000/s | 1.0k | 4.4 | 11.1 | 14.7 | 0 | 0 | 3.8k | 1.0 | 0.13 | 638 |  |
| raft10-seq | 16 | 2 | 0 | open:1000/s | 1000 | 5.3 | 14.9 | 19.0 | 0 | 0 | 4.0k | 1.01 | 0.29 | 2965 |  |
| raft10 | 16 | 2 | 0 | open:1000/s | 1.0k | 4.2 | 11.1 | 17.9 | 0 | 0 | 4.6k | 1.01 | 0.3 | 2659 |  |
| pb | 16 | 2 | 0 | open:1000/s | 1.0k | 4.2 | 8.0 | 13.8 | 0 | 0 | 4.0k | 1.0 | 0.08 | 801 |  |
| raft | 16 | 2 | 0 | open:20000/s | 20.0k | 8.1 | 14.1 | 15.9 | 0 | 0 | 6.3k | 1.01 | 0.47 | 736 |  |
| raft-batched | 16 | 2 | 0 | open:20000/s | 20.0k | 13.2 | 21.6 | 24.9 | 0 | 0 | 6.4k | 1.15 | 0.37 | 749 |  |
| raft10-seq | 16 | 2 | 0 | open:20000/s | 20.0k | 6.5 | 10.1 | 10.9 | 0 | 0 | 7.7k | 1.2 | 0.69 | 717 |  |
| raft10 | 16 | 2 | 0 | open:20000/s | 20.0k | 4.2 | 5.5 | 16.4 | 0 | 0 | 75.8k | 1.16 | 2.61 | 21 |  |
| pb | 16 | 2 | 0 | open:20000/s | 20.0k | 4.1 | 6.0 | 29.1 | 0 | 0 | 77.5k | 1.02 | 0.43 | 8 |  |
| raft | 16 | 2 | 1 | open:1000/s | 1000 | 7.2 | 12.4 | 14.4 | 0 | 0 | 3.5k | 1.28 | 0.13 | 261 |  |
| raft-batched | 16 | 2 | 1 | open:1000/s | 1000 | 7.3 | 16.0 | 19.3 | 0 | 0 | 3.6k | 1.3 | 0.14 | 260 |  |
| raft10-seq | 16 | 2 | 1 | open:1000/s | 1000 | 5.4 | 10.2 | 10.6 | 0 | 0 | 3.9k | 1.29 | 0.2 | 205 |  |
| raft10 | 16 | 2 | 1 | open:1000/s | 1.0k | 5.2 | 6.1 | 6.3 | 0 | 0 | 4.6k | 1.24 | 0.23 | 177 |  |
| pb | 16 | 2 | 1 | open:1000/s | 1.0k | 5.2 | 6.7 | 8.8 | 0 | 0 | 4.0k | 1.25 | 0.08 | 125 |  |
| raft | 16 | 2 | 1 | open:20000/s | 0 | - | - | - | 199984 | 0 | 400 | 0.0 | 0.16 | 752 | 16 leaders moved |
| raft-batched | 16 | 2 | 1 | open:20000/s | 20.0k | 15.8 | 23.5 | 24.6 | 0 | 0 | 6.3k | 2.38 | 0.38 | 255 |  |
| raft10-seq | 16 | 2 | 1 | open:20000/s | 20.0k | 7.6 | 14.1 | 30.2 | 0 | 0 | 6.1k | 9.35 | 0.73 | 262 |  |
| raft10 | 16 | 2 | 1 | open:20000/s | 20.0k | 5.7 | 8.4 | 14.3 | 0 | 0 | 65.1k | 20.57 | 1.96 | 62 |  |
| pb | 16 | 2 | 1 | open:20000/s | 20.0k | 5.5 | 6.2 | 9.0 | 0 | 0 | 47.1k | 20.21 | 0.38 | 7 |  |
| raft | 128 | 1 | 0 | open:1000/s | 1.0k | 2.2 | 3.8 | 5.2 | 0 | 0 | 7.2k | 1.0 | 0.24 | 195 |  |
| raft-batched | 128 | 1 | 0 | open:1000/s | 1.0k | 2.2 | 5.5 | 8.1 | 0 | 0 | 7.2k | 1.0 | 0.26 | 384 |  |
| raft10-seq | 128 | 1 | 0 | open:1000/s | 1.0k | 2.2 | 4.0 | 5.9 | 0 | 0 | 8.6k | 1.0 | 0.59 | 173 |  |
| raft10 | 128 | 1 | 0 | open:1000/s | 1.0k | 2.2 | 2.4 | 3.4 | 0 | 0 | 8.6k | 1.0 | 0.63 | 800 |  |
| pb | 128 | 1 | 0 | open:1000/s | 1.0k | 2.1 | 2.2 | 2.3 | 0 | 0 | 4.0k | 1.0 | 0.06 | 102 |  |
| raft | 128 | 1 | 0 | open:20000/s | 20.0k | 2.3 | 5.3 | 6.1 | 0 | 0 | 68.3k | 1.01 | 1.58 | 12 |  |
| raft-batched | 128 | 1 | 0 | open:20000/s | 20.0k | 2.4 | 6.2 | 8.2 | 0 | 0 | 69.4k | 1.02 | 1.75 | 14 |  |
| raft10-seq | 128 | 1 | 0 | open:20000/s | 20.0k | 2.2 | 4.2 | 7.6 | 0 | 0 | 70.9k | 1.11 | 2.64 | 18 |  |
| raft10 | 128 | 1 | 0 | open:20000/s | 20.0k | 2.2 | 3.2 | 5.6 | 0 | 0 | 84.0k | 1.16 | 3.1 | 29 |  |
| pb | 128 | 1 | 0 | open:20000/s | 20.0k | 2.1 | 5.1 | 13.5 | 0 | 0 | 77.1k | 1.02 | 0.52 | 65 |  |
| raft | 128 | 1 | 1 | open:1000/s | 1000 | 5.3 | 7.7 | 9.3 | 0 | 0 | 7.1k | 1.29 | 0.25 | 270 |  |
| raft-batched | 128 | 1 | 1 | open:1000/s | 1000 | 5.2 | 7.6 | 9.6 | 0 | 0 | 7.1k | 1.29 | 0.25 | 270 |  |
| raft10-seq | 128 | 1 | 1 | open:1000/s | 1.0k | 3.2 | 5.3 | 8.3 | 0 | 0 | 8.5k | 1.2 | 0.59 | 123 |  |
| raft10 | 128 | 1 | 1 | open:1000/s | 1.0k | 3.2 | 3.4 | 3.9 | 0 | 0 | 8.6k | 1.19 | 0.62 | 114 |  |
| pb | 128 | 1 | 1 | open:1000/s | 1.0k | 3.2 | 3.3 | 4.5 | 0 | 0 | 4.0k | 1.21 | 0.07 | 103 |  |
| raft | 128 | 1 | 1 | open:20000/s | 20.0k | 7.7 | 24.5 | 39.7 | 0 | 0 | 48.1k | 18.27 | 1.31 | 175 |  |
| raft-batched | 128 | 1 | 1 | open:20000/s | 20.0k | 12.8 | 44.9 | 70.6 | 0 | 0 | 45.2k | 21.98 | 1.44 | 2433 |  |
| raft10-seq | 128 | 1 | 1 | open:20000/s | 20.1k | 4.8 | 72.8 | 112 | 0 | 0 | 57.0k | 19.1 | 2.32 | 1882 |  |
| raft10 | 128 | 1 | 1 | open:20000/s | 20.0k | 3.7 | 34.5 | 58.1 | 0 | 0 | 80.5k | 23.41 | 2.76 | 1860 |  |
| pb | 128 | 1 | 1 | open:20000/s | 20.0k | 3.4 | 4.0 | 10.7 | 0 | 0 | 45.8k | 20.22 | 0.38 | 7 |  |
| raft | 128 | 2 | 0 | open:1000/s | 1.0k | 4.2 | 7.7 | 9.0 | 0 | 0 | 7.1k | 1.0 | 0.23 | 289 |  |
| raft-batched | 128 | 2 | 0 | open:1000/s | 1.0k | 4.2 | 7.7 | 9.0 | 0 | 0 | 7.1k | 1.0 | 0.23 | 255 |  |
| raft10-seq | 128 | 2 | 0 | open:1000/s | 1.0k | 4.2 | 7.2 | 8.2 | 0 | 0 | 8.6k | 1.01 | 0.54 | 127 |  |
| raft10 | 128 | 2 | 0 | open:1000/s | 1.0k | 4.2 | 4.4 | 4.6 | 0 | 0 | 8.6k | 1.01 | 0.59 | 113 |  |
| pb | 128 | 2 | 0 | open:1000/s | 1000 | 4.1 | 4.4 | 5.6 | 0 | 0 | 4.0k | 1.0 | 0.07 | 81 |  |
| raft | 128 | 2 | 0 | open:20000/s | 20.0k | 6.0 | 20.2 | 48.4 | 0 | 0 | 51.3k | 1.01 | 1.41 | 326 |  |
| raft-batched | 128 | 2 | 0 | open:20000/s | 20.0k | 7.2 | 15.3 | 18.0 | 0 | 0 | 53.6k | 1.01 | 1.49 | 11 |  |
| raft10-seq | 128 | 2 | 0 | open:20000/s | 20.0k | 5.4 | 8.1 | 8.2 | 0 | 0 | 55.4k | 1.09 | 2.26 | 13 |  |
| raft10 | 128 | 2 | 0 | open:20000/s | 20.0k | 4.2 | 15.9 | 44.6 | 0 | 0 | 82.7k | 1.22 | 2.97 | 922 |  |
| pb | 128 | 2 | 0 | open:20000/s | 20.0k | 4.1 | 4.2 | 4.6 | 0 | 0 | 78.4k | 1.01 | 0.41 | 7 |  |
| raft | 128 | 2 | 1 | open:1000/s | 1000 | 6.8 | 10.5 | 12.4 | 0 | 0 | 7.0k | 1.27 | 0.24 | 257 |  |
| raft-batched | 128 | 2 | 1 | open:1000/s | 1000 | 6.8 | 10.8 | 13.0 | 0 | 0 | 7.1k | 1.27 | 0.25 | 256 |  |
| raft10-seq | 128 | 2 | 1 | open:1000/s | 1.0k | 5.2 | 9.2 | 10.2 | 0 | 0 | 8.5k | 1.25 | 0.54 | 107 |  |
| raft10 | 128 | 2 | 1 | open:1000/s | 1.0k | 5.2 | 6.2 | 6.3 | 0 | 0 | 8.6k | 1.24 | 0.62 | 107 |  |
| pb | 128 | 2 | 1 | open:1000/s | 1.0k | 5.2 | 6.1 | 6.2 | 0 | 0 | 4.0k | 1.25 | 0.08 | 92 |  |
| raft | 128 | 2 | 1 | open:20000/s | 20.0k | 10.3 | 22.6 | 31.0 | 0 | 0 | 40.2k | 16.08 | 1.16 | 210 |  |
| raft-batched | 128 | 2 | 1 | open:20000/s | 20.0k | 13.0 | 22.3 | 24.6 | 0 | 0 | 44.4k | 15.42 | 1.31 | 202 |  |
| raft10-seq | 128 | 2 | 1 | open:20000/s | 20.0k | 6.7 | 10.3 | 10.8 | 0 | 0 | 49.3k | 17.07 | 2.19 | 24 |  |
| raft10 | 128 | 2 | 1 | open:20000/s | 20.0k | 5.7 | 81.2 | 123 | 0 | 0 | 79.7k | 23.12 | 2.8 | 2217 |  |
| pb | 128 | 2 | 1 | open:20000/s | 20.0k | 5.5 | 44.2 | 114 | 0 | 0 | 47.1k | 20.65 | 0.48 | 4999 |  |

### Replication, the storm

| Scheme | Groups | One-way ms | Flush ms | Load | Done/s | p50 ms | p99 ms | p99.9 ms | Errors | Unfinished | Msgs/s | Items/flush | CPU cores (3 nodes) | Hop late p99 us | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| raft10 | 16 | 1 | 1 | open:50000/s | 50.0k | 3.6 | 4.6 | 8.6 | 0 | 0 | 138.5k | 48.69 | 3.43 | 43 |  |
| raft10-seq | 16 | 1 | 1 | open:50000/s | 50.0k | 6.1 | 9.7 | 14.5 | 0 | 0 | 7.6k | 22.12 | 1.14 | 239 |  |
| pb | 16 | 1 | 1 | open:50000/s | 50.0k | 3.6 | 4.8 | 7.9 | 0 | 0 | 101.3k | 49.21 | 0.98 | 63 |  |
| raft-batched | 16 | 1 | 1 | open:50000/s | 50.0k | 11.9 | 17.0 | 43.3 | 0 | 0 | 8.5k | 3.39 | 0.65 | 245 |  |
| raft | 16 | 1 | 1 | open:50000/s | 2 | - | - | - | 499984 | 0 | 434 | 2.36 | 0.33 | 284 | 16 leaders moved |
| raft10 | 128 | 1 | 1 | open:50000/s | 50.0k | 3.8 | 5.1 | 7.9 | 0 | 0 | 191.5k | 53.37 | 5.97 | 177 |  |
| raft10-seq | 128 | 1 | 1 | open:50000/s | 50.0k | 5.1 | 7.8 | 8.3 | 0 | 0 | 74.6k | 36.58 | 3.51 | 56 |  |
| pb | 128 | 1 | 1 | open:50000/s | 50.0k | 3.5 | 4.1 | 4.6 | 0 | 0 | 101.4k | 49.04 | 0.81 | 14 |  |
| raft-batched | 128 | 1 | 1 | open:50000/s | 50.0k | 9.5 | 14.3 | 14.9 | 0 | 0 | 77.0k | 26.05 | 2.35 | 53 |  |
| raft | 128 | 1 | 1 | open:50000/s | 15.8k | 61.2 | 558 | 880 | 293087 | 0 | 6.9k | 8.57 | 0.6 | 264 | 121 leaders moved |
| raft10 | 256 | 1 | 1 | open:50000/s | 50.0k | 4.0 | 7.1 | 11.6 | 0 | 0 | 201.1k | 55.86 | 6.69 | 250 |  |
| raft10-seq | 256 | 1 | 1 | open:50000/s | 50.0k | 4.8 | 9.3 | 20.7 | 0 | 0 | 129.5k | 45.84 | 5.2 | 140 |  |
| pb | 256 | 1 | 1 | open:50000/s | 50.0k | 3.5 | 4.2 | 5.8 | 0 | 0 | 101.7k | 49.07 | 0.9 | 15 |  |
| raft-batched | 256 | 1 | 1 | open:50000/s | 50.0k | 7.6 | 13.9 | 19.3 | 0 | 0 | 130.0k | 41.47 | 3.63 | 81 |  |
| raft | 256 | 1 | 1 | open:50000/s | 50.0k | 7.1 | 23.0 | 34.8 | 0 | 0 | 109.3k | 40.15 | 2.96 | 59 |  |

### Replication, one partition leader at its limit

| Scheme | Groups | One-way ms | Flush ms | Load | Done/s | p50 ms | p99 ms | p99.9 ms | Errors | Unfinished | Msgs/s | Items/flush | CPU cores (3 nodes) | Hop late p99 us | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| raft | 1 | 1 | 0 | closed:16 | 2.7k | 6.2 | 7.8 | 9.5 | 0 | 0 | 776 | 1.0 | 0.1 | 357 |  |
| raft-batched | 1 | 1 | 0 | closed:16 | 2.1k | 7.7 | 10.6 | 13.9 | 0 | 0 | 776 | 1.0 | 0.1 | 341 |  |
| raft10-seq | 1 | 1 | 0 | closed:16 | 4.1k | 3.9 | 5.5 | 8.8 | 0 | 0 | 810 | 1.65 | 0.16 | 347 |  |
| raft10 | 1 | 1 | 0 | closed:16 | 6.1k | 2.6 | 3.1 | 6.1 | 0 | 0 | 2.9k | 2.03 | 0.22 | 346 |  |
| pb | 1 | 1 | 0 | closed:16 | 6.8k | 2.4 | 2.8 | 4.1 | 0 | 0 | 17.2k | 1.34 | 0.31 | 266 |  |
| raft | 1 | 1 | 0 | closed:256 | 48.6k | 5.2 | 7.6 | 17.7 | 0 | 0 | 762 | 1.0 | 0.84 | 316 |  |
| raft-batched | 1 | 1 | 0 | closed:256 | 35.5k | 7.5 | 11.8 | 14.1 | 0 | 0 | 772 | 1.0 | 0.35 | 349 |  |
| raft10-seq | 1 | 1 | 0 | closed:256 | 54.8k | 4.8 | 6.1 | 19.0 | 0 | 0 | 789 | 5.48 | 0.93 | 306 |  |
| raft10 | 1 | 1 | 0 | closed:256 | 118.2k | 2.1 | 2.6 | 8.3 | 0 | 0 | 54.8k | 1.57 | 2.52 | 12 |  |
| pb | 1 | 1 | 0 | closed:256 | 120.4k | 2.1 | 2.7 | 3.2 | 0 | 0 | 215.6k | 1.84 | 3.54 | 25 |  |
| raft | 1 | 1 | 0 | closed:1024 | 115.9k | 8.0 | 13.0 | 21.2 | 0 | 0 | 774 | 1.0 | 1.5 | 290 |  |
| raft-batched | 1 | 1 | 0 | closed:1024 | 147.4k | 7.4 | 11.5 | 12.7 | 0 | 0 | 773 | 1.0 | 1.61 | 303 |  |
| raft10-seq | 1 | 1 | 0 | closed:1024 | 111.6k | 8.4 | 16.9 | 29.7 | 0 | 0 | 780 | 6.41 | 1.84 | 291 |  |
| raft10 | 1 | 1 | 0 | closed:1024 | 438.9k | 2.2 | 7.3 | 10.3 | 0 | 0 | 39.0k | 2.56 | 4.6 | 12 |  |
| pb | 1 | 1 | 0 | closed:1024 | 257.9k | 3.8 | 6.7 | 7.5 | 0 | 0 | 62.3k | 12.04 | 4.3 | 84 |  |
| raft | 1 | 1 | 1 | closed:16 | 688 | 23.2 | 25.5 | 26.2 | 0 | 0 | 214 | 1.0 | 0.05 | 332 |  |
| raft-batched | 1 | 1 | 1 | closed:16 | 1.4k | 11.2 | 16.2 | 18.5 | 0 | 0 | 592 | 1.0 | 0.08 | 346 |  |
| raft10-seq | 1 | 1 | 1 | closed:16 | 2.8k | 5.3 | 7.5 | 11.1 | 0 | 0 | 558 | 1.85 | 0.13 | 399 |  |
| raft10 | 1 | 1 | 1 | closed:16 | 3.7k | 3.9 | 5.3 | 11.2 | 0 | 0 | 3.6k | 1.72 | 0.26 | 298 |  |
| pb | 1 | 1 | 1 | closed:16 | 3.5k | 4.8 | 5.9 | 52.8 | 0 | 0 | 6.0k | 3.45 | 0.15 | 343 |  |
| raft | 1 | 1 | 1 | closed:256 | 768 | 332 | 341 | 342 | 0 | 0 | 15 | 1.0 | 0.03 | 341 |  |
| raft-batched | 1 | 1 | 1 | closed:256 | 23.7k | 10.1 | 21.0 | 26.6 | 0 | 0 | 590 | 1.0 | 0.17 | 653 |  |
| raft10-seq | 1 | 1 | 1 | closed:256 | 37.6k | 7.4 | 8.9 | 17.0 | 0 | 0 | 561 | 11.77 | 0.53 | 733 |  |
| raft10 | 1 | 1 | 1 | closed:256 | 52.9k | 4.9 | 5.6 | 17.5 | 0 | 0 | 6.4k | 6.24 | 1.03 | 272 |  |
| pb | 1 | 1 | 1 | closed:256 | 61.1k | 4.3 | 5.4 | 7.9 | 0 | 0 | 36.9k | 36.04 | 1.39 | 225 |  |
| raft | 1 | 1 | 1 | closed:1024 | 0 | - | - | - | 14165375 | 0 | 26 | 0.0 | 5.83 | 569 | 1 leaders moved |
| raft-batched | 1 | 1 | 1 | closed:1024 | 97.7k | 10.0 | 23.2 | 39.7 | 0 | 0 | 604 | 1.0 | 0.59 | 976 |  |
| raft10-seq | 1 | 1 | 1 | closed:1024 | 78.0k | 11.8 | 19.9 | 31.8 | 0 | 0 | 555 | 19.33 | 1.01 | 289 |  |
| raft10 | 1 | 1 | 1 | closed:1024 | 214.9k | 4.6 | 10.4 | 16.4 | 0 | 0 | 11.2k | 14.66 | 1.91 | 646 |  |
| pb | 1 | 1 | 1 | closed:1024 | 207.8k | 4.8 | 7.0 | 8.5 | 0 | 0 | 70.5k | 119.93 | 3.67 | 75 |  |

### Replication, primary-backup's apply checked

| Scheme | Groups | One-way ms | Flush ms | Load | Done/s | p50 ms | p99 ms | p99.9 ms | Errors | Unfinished | Msgs/s | Items/flush | CPU cores (3 nodes) | Hop late p99 us | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| pb | 256 | 1 | 1 | open:50000/s | 50.0k | 3.5 | 4.4 | 5.5 | 0 | 0 | 100.8k | 48.94 | 0.97 | 18 |  |

#### Entries applied on each node (primary-backup)

| Scheme | Groups | Load | Committed | Applied, node 0 | node 1 | node 2 |
| --- | --- | --- | --- | --- | --- | --- |
| pb | 256 | open:50000/s | 500,000 | 499,731 | 499,726 | 499,731 |

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
| 128 | closed:1 | fjall | 291 | 3.5 | 5.4 | 10.5 | 3.6 to 7.2 | 1 | 118.3 |
| 128 | closed:1 | redb | 276 | 3.5 | 6.1 | 10.6 | 4.0 to 8.2 | 1 | 615.7 |
| 128 | closed:1 | rocksdb | 298 | 3.5 | 5.4 | 10.4 | 3.2 to 7.5 | 1 | 112.7 |
| 128 | open:10000/s | fjall | 10.0k | 3.9 | 6.7 | 10.6 | 6.6 to 6.8 | 25 | 5.8 |
| 128 | open:10000/s | redb | 10.0k | 5.2 | 11.2 | 18.3 | 9.9 to 12.5 | 34 | 153.0 |
| 128 | open:10000/s | rocksdb | 10.0k | 4.0 | 6.1 | 9.8 | 6.1 to 6.1 | 25 | 5.8 |
| 128 | open:50000/s | fjall | 50.0k | 3.9 | 65.9 | 107 | 9.6 to 122 | 117 | 3.3 |
| 128 | open:50000/s | redb | 50.0k | 9.0 | 21.1 | 28.8 | 17.6 to 24.7 | 303 | 59.0 |
| 128 | open:50000/s | rocksdb | 50.0k | 3.9 | 9.4 | 15.6 | 7.2 to 11.5 | 118 | 3.1 |

#### Group commit window, 128 B (median of passes)

| Load | Engine | 0 ms p50/p99 | 1 ms p50/p99 | 2 ms p50/p99 |
| --- | --- | --- | --- | --- |
| closed:1 | fjall | 3.5 / 5.4 | 4.9 / 7.6 | 5.8 / 6.6 |
| closed:1 | redb | 3.5 / 6.1 | 4.9 / 6.7 | 6.0 / 7.6 |
| closed:1 | rocksdb | 3.5 / 5.4 | 4.0 / 5.8 | 6.0 / 6.6 |
| open:10000/s | fjall | 3.9 / 6.7 | 4.1 / 7.4 | 5.2 / 8.9 |
| open:10000/s | redb | 5.2 / 11.2 | 6.6 / 14.5 | 7.0 / 13.6 |
| open:10000/s | rocksdb | 4.0 / 6.1 | 4.3 / 7.0 | 4.4 / 7.8 |
| open:50000/s | fjall | 3.9 / 65.9 | 4.5 / 9.4 | 8.2 / 19.5 |
| open:50000/s | redb | 9.0 / 21.1 | 11.8 / 23.1 | 13.6 / 24.9 |
| open:50000/s | rocksdb | 3.9 / 9.4 | 6.7 / 14.3 | 9.1 / 22.4 |

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
