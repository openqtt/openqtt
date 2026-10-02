# R6. Routing at scale

Status: draft. Measured by spike S2 (`spikes/s2-routing`); the raw results are in
`bench/results/s2-*.json`. It answers what R3 leaves to it (section Router: memory per filter,
match latency at a million filters, the false-positive rate on device-shaped topics, and T) and
proposes amendments to R3, listed at the end.

The times are laptop numbers: one Apple M2 Pro (6 performance and 4 efficiency cores, 16 GiB),
macOS 26.5, Rust 1.99.0, one thread, while other work kept about 4 GiB wired and the memory
compressor busy. Read them as orders of magnitude and as ratios between rows, not as what a
server will do. Memory is counted by a global allocator that adds up requested bytes, so those
figures do not depend on the machine; allocator rounding comes on top.

## Questions

1. What does a filter cost in the router's trie, and how fast do filters come and go?
2. How long does a match take at a million filters, and what does fan-out to many destinations
   add?
3. Interest coarsening at the edge: in what form, and with which T, does the interest an edge
   registers stay small without sending it messages that none of its subscribers wants?
4. How large are route-view changes, and what does applying them cost, when 1% of
   subscriptions churn every minute?

## Answers

- **Memory.** 138 to 150 bytes per device filter once compacted, from 10^5 to 10^7 filters, with
  practically no allocation per filter. Growth slack takes it to as much as 220 bytes while
  vectors and tables grow: 2.20 GB as built at 10^7 filters, 1.43 GB compacted.
- **Changes.** On one thread, 0.85 to 2.3 million inserts and 0.55 to 1.6 million removals in
  random order per second, the lower figures at the larger sizes.
- **Match.** At 10^6 filters a match takes 0.75 to 1.9 µs at the median and 5 to 7 µs at p99.
  It is bound by memory latency: at 10^5 filters, which fit in the caches, it takes 0.21 to
  0.42 µs.
- **Fan-out.** 4 to 5 ns per destination read from one set. Overlapping sets cost 20 to 28 ns
  per destination to deduplicate by sorting, 10 to 13 ns by bitmap union.
- **Coarsening.** Replacing a node by `prefix/#`, as R3 writes it, sends every device's
  telemetry to every edge. A shape cover (D2) keeps telemetry exact. Under uniform placement,
  which is what a load balancer gives, no T keeps per-device entries out of the route view
  while coarsening stays at or below the namespace level: half to four fifths of all devices
  keep their own entry at 10^7 devices, millions of entries that every edge would hold.
  Coarsening from the first level keeps the view at 421 entries for 100 edges, at 10^6 and at
  10^7 devices; the price is that every command to a device reaches every edge. **T = 64.**
- **Churn.** With that cover, 1% of devices churning per minute changes nothing in the route
  view. With the floor at the namespace it would cost 12,000 to 17,000 records a minute per
  10^6 devices, applied in about 10 ms.

## Method

### What was built

`spikes/s2-routing` implements the router structure of R3 and nothing more:

- Nodes in one vector, 24 bytes each: the parent, the interned level leading to the node, the
  `+` child, the terminal of the filter ending there and of the one ending there with `/#`,
  and a count of literal children.
- Level strings interned once in a byte arena, with reference counts so that a level no node
  uses any more is freed.
- One hash table for every literal edge of the trie, keyed by parent and level. It stores only
  the child's 32-bit index and compares against the parent and level the child node already
  holds, so an edge costs 4 bytes plus a control byte.
- A terminal (40 bytes) per filter: its destinations and its shared groups. A destination set
  holds four entries inline, then a sorted vector, and becomes a roaring bitmap above 64
  entries (a vector again at 32).
- Matching per MQTT 5.0 section 4.7, including that a filter whose first level is a wildcard
  never matches a topic starting with `$` [MQTT-4.7.2-1]. Unit tests check the specification's
  examples, pruning back to an empty trie, and shared groups.

### Workloads

The production shape is the deployment pattern R2 restates: a device's certificate CN is its
username and the mountpoint is `ingest/${username}/`, so the subscription a device makes to its
commands is `ingest/<org>/<ns>/<device>/commands/#`. It is live interest on the device's edge,
since a device connects with clean start and keeps no session (R2 rule 23).

- **(a) devices.** One such filter per device. Organisations are Zipf distributed (exponent 1)
  over n/100 organisations, so the largest holds about a tenth of all devices and the smallest
  a handful; each has one to four namespaces (`production`, `staging`, `field`, `lab`), again
  Zipf distributed. Device identifiers are 16 hexadecimal characters and organisation
  identifiers 9, so a filter averages 54 characters. Every value derives from the device's
  index through a fixed hash, so every run sees the same population.
- **(b) mixed.** 90% device command filters, 5% exact device topics (`.../config`), 3%
  namespace wildcards (`ingest/<org>/<ns>/+/telemetry` and `.../+/events/#`), 2% organisation
  wildcards (`ingest/<org>/+/+/status` and `ingest/<org>/#`), and 20 per million global ones
  (`ingest/+/+/+/alerts/+`, `+/+/+/+/telemetry`, `$SYS/brokers/+/clients/#` and `#`). A
  wildcard subscriber picks its organisation uniformly. Published topics are half telemetry,
  a fifth commands, and the rest config, status, events and `$SYS`.
- **(c) shared groups.** Workload (a) plus 1,000 groups
  `$share/grp<g>/ingest/<org>/+/+/telemetry` over the 100 largest organisations, with 2 to
  64 members each and 1,000 for every hundredth group: 43,527 members.

### Coarsening model

100 edges, each holding the exact interest of the devices connected to it, and consumers:

- edge 0 holds a consumer of all telemetry, `ingest/+/+/+/telemetry`;
- twenty narrow consumers, each of one of the twenty largest organisations' telemetry
  (`ingest/<org>/+/+/telemetry`), sit on edges chosen by hash, which they share with devices
  as they would behind a load balancer.

Two placements of devices on edges:

- **Uniform.** A connection lands on a uniformly random edge, as an L4 load balancer spreads
  connections. Placing by a hash of the client identifier is the same thing statistically.
- **By namespace.** The devices of one namespace share an edge, chosen by rendezvous hashing
  of `<org>/<ns>`, largest namespaces first onto the least loaded of their first choices; a
  namespace larger than an edge's mean load spreads over just enough edges. The most loaded
  edge carries 1.16 times the mean at 10^6 devices (1.02 under uniform placement). Something
  has to steer each connection to its namespace's edge, which is a question for R7.

Each edge computes a cover from its exact interest; the route view is the union of every
edge's cover. Three forms of cover:

- **`#`**: a node with more than T distinct children becomes `prefix/#`, as R3 writes it.
- **`+`**: the node's children are merged under `prefix/+`, keeping what follows them.
- **Shape**: below the node, a level position with more than T distinct values becomes `+`,
  and a resulting shape is registered only when more than T filters share it. Every other
  filter stays exact.

A floor F keeps nodes shallower than depth F from being coarsened: the root is depth 0,
`ingest` 1, `ingest/<org>` 2, `ingest/<org>/<ns>` 3.

The grid at 10^6 devices is both placements, the three forms, floors 1, 2 and 3, and T in
{1, 4, 16, 64, 256, 1024}; at 10^7 it is floors 1 and 3 with T in {16, 64, 256, 1024}, and at
10^5 floors 1 and 3 with T in {16, 64, 256}. For each:

- **Route entries**: (filter, edge) pairs in the route view. Every edge holds all of them,
  merged; the routers hold them sharded.
- **Deliveries and false positives**: for 200,000 devices drawn uniformly, one command to the
  device and one telemetry message from it, the edges each is delivered to, and how many of
  those have no local subscriber that wants it. Commands come from the platform through the
  admin API (R2 rule 21), so every edge that gets one counts. Telemetry comes from the
  device's own edge, which delivers to its local subscribers without a lane, so that edge does
  not count.

### Churn model

1% of devices churn every minute: each disconnects at a random second and reconnects five
seconds later, on the edge placement gives it (another edge under uniform placement, the
same edge by namespace). For covers with the floor at the namespace, each edge's cover is kept
up to date change by change, and every change is a route-view record the moment it happens.
Two options are measured: hysteresis, where a coarse node stays coarse until it falls to T/2
children, and a grace period, where an edge withdraws a departed device's entry only after
30 s, so a device that comes back to the same edge costs nothing. For the shape cover from
floor 1, each edge's cover is recomputed once a minute and the difference counted, which is
what a stream batched per minute would carry. A record is an operation byte, then a sequence
number, the edge and the filter's length as varints, then the filter. Applying the records is
timed on a trie holding the whole route view. Six minutes; the first is left out of the means.

### Rerunning

`spikes/s2-routing/run.sh` builds the spike and writes every file under `bench/results/` that
this report quotes; `./run.sh coarsen churn` reruns only those. It takes about fifteen minutes
and up to 3 GiB.

## Results

### 1. Memory, insert and remove

Workload (a), one thread:

| Filters | Bytes per filter, as built | Bytes per filter, compacted | Allocations per filter | Inserts per second | Removals per second, random order |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 100,000 | 172 | 138 | 0.00006 | 2.26 million | 1.62 million |
| 1,000,000 | 153 | 150 | 0.00001 | 1.39 million | 0.55 million |
| 10,000,000 | 220 | 143 | 0.00000 | 0.85 million | 0.56 million |

Where the bytes go at 10^7 filters, compacted, per filter: nodes 48.8 (two per filter, the
device level and `commands`, at 24 bytes each), terminal 40.0, child table 16.8, level arena
16.1 (the device identifier), level spans 12.1, level table 8.4. Before compacting, vectors and
tables carry growth slack, up to half their size just after doubling: 2.20 GB at 10^7 filters
against 1.43 GB compacted. Workloads (b) and (c) cost the same per filter: 149 and 150 bytes
compacted, the 43,527 group members of (c) adding 6 bytes each. Removing every filter returns
the trie to its root and an empty interner.

### 2. Match latency at 10^6 filters

Per call, one thread, after warming on the same topics in another order. The timer ticks every
41 ns, so per-call figures are multiples of it.

| Workload and stream | Matching filters | Destinations | p50 | p99 | p99.9 | Mean | Trie walk p50 | Trie walk p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| (a) command to a subscribed device (one match) | 1.00 | 1.0 | 1.67 µs | 6.71 µs | 12.92 µs | 1.71 µs | 1.21 µs | 5.88 µs |
| (a) telemetry (no match) | 0.00 | 0.0 | 0.75 µs | 5.25 µs | 8.25 µs | 0.79 µs | 0.71 µs | 5.12 µs |
| (b) mixed publish stream | 2.72 | 83.1 | 1.88 µs | 6.79 µs | 12.08 µs | 1.91 µs | 0.79 µs | 5.25 µs |
| (c) telemetry into shared groups | 1.00 | 10.0 | 0.92 µs | 5.17 µs | 7.79 µs | 0.74 µs | 0.50 µs | 0.88 µs |

"Trie walk" is the match without gathering destinations. Each topic level costs an interner
probe and a child-table probe, and at 10^6 filters (150 MB) both miss the caches: a command
walks six levels in 1.2 µs at the median. The walk is memory-latency bound, which is why its
p99 is four to five times its median.

At 10^5 filters (17 MB) the same streams take a fifth of the time or less:

| Workload and stream | Matching filters | Destinations | p50 | p99 | p99.9 | Mean | Trie walk p50 | Trie walk p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| (a) command to a subscribed device (one match) | 1.00 | 1.0 | 0.29 µs | 1.42 µs | 5.33 µs | 0.31 µs | 0.25 µs | 0.54 µs |
| (a) telemetry (no match) | 0.00 | 0.0 | 0.21 µs | 0.58 µs | 5.92 µs | 0.21 µs | 0.21 µs | 0.46 µs |
| (b) mixed publish stream | 2.52 | 11.5 | 0.42 µs | 1.29 µs | 5.38 µs | 0.49 µs | 0.33 µs | 0.62 µs |
| (c) telemetry into shared groups | 1.00 | 10.0 | 0.29 µs | 0.96 µs | 5.29 µs | 0.33 µs | 0.25 µs | 0.54 µs |

The run at 10^6 had no swapping and 228 major page faults in 20 s (`/usr/bin/time -l`), so the
difference is cache and TLB misses, not the machine's memory pressure. A repeat run at 10^6
agreed within 15%.

### 3. Fan-out

One topic matching one filter with N destinations, then three overlapping filters (a second
with half of the destinations, a third with a quarter of them plus a quarter more), among 10^6
other filters. Every destination is read once after the match.

| Destinations on the filter | Matching filters | Distinct destinations | Sorted and deduplicated | Per destination | Bitmap union | Per destination |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 1 | 0.14 µs | 142.2 ns | 0.23 µs | 230.9 ns |
| 1 | 3 | 1 | 0.14 µs | 142.7 ns | 0.23 µs | 227.3 ns |
| 10 | 1 | 10 | 0.14 µs | 14.3 ns | 0.68 µs | 67.8 ns |
| 10 | 3 | 12 | 0.27 µs | 22.4 ns | 1.02 µs | 84.7 ns |
| 100 | 1 | 100 | 0.71 µs | 7.1 ns | 1.33 µs | 13.3 ns |
| 100 | 3 | 125 | 1.94 µs | 15.5 ns | 4.08 µs | 32.6 ns |
| 1,000 | 1 | 999 | 4.22 µs | 4.2 ns | 4.79 µs | 4.8 ns |
| 1,000 | 3 | 1,249 | 25.40 µs | 20.3 ns | 11.87 µs | 9.5 ns |
| 10,000 | 1 | 9,953 | 46.29 µs | 4.7 ns | 39.81 µs | 4.0 ns |
| 10,000 | 3 | 12,433 | 351.24 µs | 28.3 ns | 159.46 µs | 12.8 ns |

Reading one set costs 4 to 5 ns per destination once it is large. When several matching sets
overlap, deduplicating them by sorting costs 20 to 28 ns per destination at 1,000 or more;
a bitmap union costs 10 to 13. Below about 1,000 destinations sorting wins.

### 4. Shared subscriptions

Picking one member from each of ten matching groups adds 0.4 µs to the 0.5 µs walk (workload
(c), telemetry into shared groups), about 40 ns per group. R1 (O1) delivers round robin per
publishing edge over the members whose sessions are connected. A publishing edge cannot see
members on other edges, so the route view carries, for each group and filter, the edges that
hold connected members and how many: one entry per (group, filter, edge), at most one per edge
however many members it holds. The receiving edge then picks among its own members. That
bounds a group's route-view cost by the edge count, at the cost of round robin that is fair
across edges, weighted by member count, rather than across members.

### 5. Coarsening

Selected rows; the JSON holds the whole grid (54 rules per placement at 10^6 devices, 24 at
10^7). Deliveries are per message, to edges other than the publishing one; a false positive is
a delivery to an edge where no subscriber wants the message. Exact routing delivers a command
to 1 edge and a telemetry message to 1.35 (1.29 at 10^7): the consumer on edge 0, plus a narrow
consumer for devices of the twenty largest organisations.

Uniform placement:

| Devices | Cover | Floor | T | Route entries | Most on one edge | Command deliveries | Command false positives | Telemetry deliveries | Telemetry false positives |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10^6 | `#` | 1 | 1 to 1,024 | 100 | 1 | 100 | 99.0% | 99.0 | 98.6% |
| 10^6 | `+` | 1 | 16 or 64 | 420 | 5 | 100 | 99.0% | 19.8 | 93.2% |
| 10^6 | shape | 1 | 4 to 64 | 421 | 6 | 100 | 99.0% | 1.35 | 0% |
| 10^6 | shape | 1 | 256 | 22,574 | 259 | 97.8 | 99.0% | 1.35 | 0% |
| 10^6 | shape | 1 | 1,024 | 86,204 | 940 | 91.4 | 98.9% | 1.35 | 0% |
| 10^6 | `#` | 3 | 64 | 705,554 | 7,250 | 30.2 | 96.7% | 30.0 | 95.5% |
| 10^6 | shape | 3 | 16 | 583,944 | 6,035 | 42.3 | 97.6% | 1.35 | 0% |
| 10^6 | shape | 3 | 64 | 705,554 | 7,250 | 30.2 | 96.7% | 1.35 | 0% |
| 10^6 | shape | 3 | 1,024 | 1,000,021 | 10,242 | 1.00 | 0% | 1.35 | 0% |
| 10^7 | `#` | 1 | 16 to 1,024 | 100 | 1 | 100 | 99.0% | 99.0 | 98.7% |
| 10^7 | shape | 1 | 16 to 1,024 | 421 | 6 | 100 | 99.0% | 1.29 | 0% |
| 10^7 | shape | 3 | 16 | 4,990,011 | 50,355 | 50.7 | 98.0% | 1.29 | 0% |
| 10^7 | shape | 3 | 1,024 | 8,116,227 | 82,707 | 19.7 | 94.9% | 1.29 | 0% |

Placement by namespace:

| Devices | Cover | Floor | T | Route entries | Most on one edge | Command deliveries | Command false positives | Telemetry deliveries | Telemetry false positives |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10^6 | shape | 3 | 1 | 24,449 | 296 | 1.94 | 48.4% | 1.35 | 0% |
| 10^6 | shape | 3 | 16 | 123,319 | 1,360 | 1.94 | 48.4% | 1.35 | 0% |
| 10^6 | shape | 3 | 64 | 262,391 | 3,432 | 1.94 | 48.4% | 1.35 | 0% |
| 10^6 | `#` | 3 | 16 | 123,319 | 1,360 | 1.94 | 48.4% | 2.28 | 40.8% |
| 10^6 | shape | 1 | 64 | 898 | 64 | 98.8 | 99.0% | 1.35 | 0% |
| 10^7 | shape | 3 | 16 | 1,165,717 | 12,432 | 1.59 | 37.2% | 1.28 | 0% |
| 10^7 | shape | 3 | 64 | 2,296,116 | 26,326 | 1.59 | 37.2% | 1.28 | 0% |

A route entry costs 195 bytes in a merged view of 705,554 entries (uniform placement, floor 3,
T 64) and 262 bytes in one of 262,391 (by namespace), where entries share fewer levels. The 421
entries of the shape cover from the first level are four shapes that every edge holds and the
21 consumers' filters, each held by one edge: 46 KB in all.

What the rows show:

1. **`prefix/#` floods telemetry.** With the floor at 1, every T up to 1,024 collapses each
   edge to `ingest/#`, because an edge serves devices of more organisations than T: every
   message, telemetry included, goes to every edge. With the floor at the namespace,
   `ingest/<org>/<ns>/#` still draws all of that namespace's telemetry. Telemetry is most of
   the traffic, so the form of the cover matters more than T.
2. **The `+`-cover widens consumers.** It keeps device telemetry out, but merging every child
   of `ingest` turns a consumer of one organisation's telemetry into a consumer of every
   organisation's: each edge holding a narrow consumer receives all telemetry, 19.8 deliveries
   per message.
3. **The shape cover keeps telemetry exact** in every configuration measured.
4. **Under uniform placement the long tail defeats T.** An organisation with fewer devices than
   there are edges has one or two of them on any edge, so its namespace node never has more
   than T children there. With coarsening at the namespace level and below, 58% (T 16) to 100%
   (T 1,024) of devices keep their own route entry at 10^6, and 50% to 81% at 10^7: up to 8
   million entries that every edge would hold, about 1.6 GB each at 195 bytes. That is the
   per-device, cluster-wide structure R3 rules out.
5. **Coarsening from the first level bounds the view** at four to six entries per edge, the
   same at 10^6 and 10^7 devices: `ingest/+/<ns>/+/commands/#` for each namespace name, and the
   consumers exact. The price is commands: each reaches all 100 edges, 99 of which drop it after
   a lookup in their local index.
6. **Placement by namespace changes the trade.** With the floor at the namespace and a small T,
   a command reaches 1.9 edges (1.6 at 10^7), the excess coming from the largest namespaces,
   which span several edges, and at T 1 the view holds 2.4% of devices as entries. Coarsening
   from the first level would undo that, since each edge still serves more than T
   organisations. The floor follows placement.

What broadcasting commands costs: at 10^7 devices receiving one command a day each, 116
commands a second, so 11,600 lane messages a second across the cluster and 116 at each edge. A
rollout to every device at 10,000 commands a second is 10^6 lane messages a second, 10,000 at
each edge, each dropped after a lookup in a local index of 10^5 filters (0.2 to 0.3 µs,
section 2). The platform publishes commands through the admin API at a pace it chooses (R2 rules
17 and 21), so the rollout rate is the operator's to bound.

### 6. Route-view churn

10,000 devices (1% of 10^6) disconnect and reconnect every minute. The figures are for the
stream every edge follows; means over minutes 2 to 6. With the floor at the namespace, the `+`
and shape covers give device filters the same entries, and the event model computes the `+`
form.

| Placement | Cover | Floor | T | Hysteresis | Grace | Route entries | Records a minute | Bytes a minute | Most records in one second | Apply a minute |
| --- | --- | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| uniform | shape | 1 | 16 or 64 | | | 421 | 0 | 0 | 0 | 0 |
| uniform | `+` | 3 | 16 | no | none | 583,923 | 13,023 | 775 KB | 280 | 8.3 ms |
| uniform | `+` | 3 | 16 | yes | none | 583,923 | 12,014 | 716 KB | 247 | 8.0 ms |
| uniform | `+` | 3 | 64 | no | none | 705,533 | 16,787 | 1.00 MB | 439 | 11.7 ms |
| uniform | `+` | 3 | 64 | yes | 30 s | 705,533 | 14,449 | 862 KB | 323 | 12.3 ms |
| uniform | `+` | 3 | 256 | no | none | 812,888 | 16,163 | 967 KB | 309 | 11.6 ms |
| by namespace | `+` | 3 | 16 | no | none | 123,298 | 4,502 | 265 KB | 144 | 1.7 ms |
| by namespace | `+` | 3 | 16 | yes | none | 123,298 | 2,336 | 137 KB | 53 | 1.0 ms |
| by namespace | `+` | 3 | 16 to 256 | either | 30 s | 123,298 to 407,385 | 0 | 0 | 0 | 0 |
| by namespace | shape | 1 | 16 or 64 | | | 419 or 898 | 0 | 0 | 0 | 0 |

- With the shape cover from the first level the view does not change at all: devices come and
  go below shapes that stay.
- Under uniform placement with the floor at the namespace, a churning device costs 1.2 to 1.7
  records (a withdrawal on the edge it left and an addition on the one it reaches, less those
  under coarse nodes) of about 60 bytes: 12 to 17 KB a second per 10^6 devices, applied at 0.6
  to 0.9 µs a record. A grace period barely helps there, because 99 reconnects in 100 land on
  another edge. Hysteresis cuts transitions of coarse nodes by two thirds (526 to 182 at T 16)
  and records by 8 to 13%.
- By namespace, a device comes back to its own edge, so a 30 s grace period absorbs every
  reconnect.
- A new edge downloads the whole view as its snapshot: 35 to 49 MB under uniform placement with
  the floor at the namespace (10^6 devices), 7 MB by namespace at T 16, and 421 records, about
  25 KB, with the shape cover from the first level.

## Decisions

**D1. The interest index is R3's arena trie, as measured here:** 138 to 150 bytes per filter,
almost no allocation per filter, 0.55 to 2.3 million changes a second on one thread, a match in
0.75 to 1.9 µs at the median at 10^6 filters. Destination sets as R3 says: inline up to four, a
sorted vector up to 64, a roaring bitmap above, and back to a vector at 32. When several
matching filters contribute destinations, deduplicate by sorting below about 1,000 destinations
and by bitmap union above. Three savings to measure when it is built for real: a 24-byte
terminal instead of 40 (the destination set takes 32 for its inline form), path compression of
single-child chains (each device filter's `commands` node costs a node and a child-table entry,
about 30 bytes), and short levels stored inline in their node, so that a match probes one table
per level instead of two.

**D2. An edge registers a shape cover, not `prefix/#`.** Below a node, a level position with
more than T distinct values becomes `+`, and a resulting shape is registered only when more than
T of the edge's filters share it; every other filter is registered as it is. In every
configuration measured it never sent an edge telemetry its subscribers did not want, and it
never widened a consumer's narrow filter.

**D3. T is 64, and coarsening may start at the first level.** At production scale a device
shape has thousands of filters on an edge, far above 64, so it collapses; a consumer can hold
up to 64 similar filters on one edge and stay exact. At 10^5 devices on 100 edges some shape
groups stay at or below 64 and are registered filter by filter, 7,148 entries in all, which
costs nothing at that size. R3's default of 256 leaves 22,574 entries at 10^6 devices. A floor
is a deployment setting: where connections are placed by namespace (R7), coarsening from the
namespace level down with a T as low as 4 brings each command to under two edges.

**D4. What an edge keeps locally:**

- its exact index of local subscriptions, in the same trie, whose destinations are local
  sessions: about 150 bytes a subscription, 15 MB for 10^5 devices;
- its cover state: per coarse node, the distinct values at each level position and the size of
  each shape group, with hysteresis at T/2, so that a change in the local index updates the
  cover without recomputing it;
- the merged route view of every shard: the covers of all edges and the durable interest of log
  partitions, 421 entries for 100 edges under D3;
- per registered coarse entry, a count of deliveries that matched no local subscriber, which is
  what R3's "the counter tunes T" reads and where false positives become visible.

**D5. The route-view stream** carries sequenced (filter, edge) changes of about 60 bytes, with
hysteresis at T/2 on coarse nodes and a grace period of 30 s before an edge withdraws a departed
client's filter. Under D3 it is almost silent; with a floor at the namespace and placement by
namespace, the grace period absorbs every reconnect.

**D6. Commands to devices reach every edge** under uniform placement. That is the accepted cost
of D3: one lane message per edge per command, dropped by the edges that do not hold the device.
Whether to avoid it by placing connections by namespace is R7's question; the router supports
either through the floor.

## Amendments to R3

To apply to R3, section Router, once this report is accepted:

1. In the first bullet, "a trie node with more than T distinct children (256 by default) is
   replaced by `prefix/#`" becomes: the edge registers the shape cover of its local interest
   (R6, D2) with T = 64; coarsening may start at the first level, and a deployment that places
   connections by namespace raises the floor (R6, D3).
2. A new bullet: the route view carries a shared group as one entry per (group, filter, edge),
   with the number of connected members on that edge. The publishing edge picks an edge in
   proportion to those numbers and that edge picks the member (R1, O1).
3. The **Spike** (R6) bullet and the R6 line of "Open questions for the spikes" are answered
   here.

## Risks and what was not measured

- **Command fan-out.** D3 multiplies every command addressed to a device by the number of edges.
  At one command per device a day it is noise; a rollout at 10,000 commands a second is 10^6
  lane messages a second across 100 edges. Pacing belongs to the platform, and a rate limit on
  admin publishes into device namespaces belongs to R8 or R9.
- **The population is synthetic.** That a floor at the namespace leaves most devices with their
  own entry depends on how many devices sit in namespaces smaller than the number of edges: the
  Zipf model here puts 50% to 81% there. A fleet of a few very large namespaces would favour the
  namespace floor. The counters of D4 measure it in production.
- **The shape heuristic compares level positions, not structure.** Filters of different shapes
  under one node can share a position whose values exceed T and be widened together. The
  counters of D4 show it when it happens.
- **Not measured:** the cost of keeping the shape cover up to date change by change (the spike
  recomputed it once a minute); matching while another thread applies route-view changes;
  scaling across cores; Linux and its allocator (the counts here are requested bytes, allocator
  overhead excluded); real topic and subscription distributions; durable interest held by log
  partitions, which R4 owns.
- **Laptop numbers.** All times come from a shared laptop under memory pressure; matching at
  10^6 filters is bound by cache misses, so a server's memory system decides its latency more
  than its clock.
