# R7. QUIC in a cluster

Status: draft, in two parts. **S4** (idle connection cost) is measured and written below.
**S3** (QUIC behind Google Cloud's passthrough UDP load balancer) runs later against a real
load balancer; its section holds the questions and placeholders until then. R3 leaves R7 the
question of keeping migrated connections on their edge behind the load balancer;
`docs/spec/mqtt-over-quic.md` (section 5) defers to it how connection IDs encode the serving
node; ADR 0002 asks it to settle migration and 0-RTT across a cluster before the design is.

## S3: QUIC behind a passthrough UDP load balancer

To be measured. The questions S3 answers:

1. **Affinity.** Does the load balancer keep every packet of a connection on one edge, under
   its default five-tuple hashing and with client IP affinity?
2. **Rebinding and migration.** When a client's address or port changes (NAT rebinding, a
   network switch), where do its packets land, and how often does the connection survive?
   What must a connection ID carry so that a packet reaching the wrong edge can be forwarded
   to the right one?
3. **Preferred address.** Can an edge offer clients its own address (RFC 9000 section 9.6)
   through the load balancer, and do clients move to it?
4. **0-RTT across edges.** How often is 0-RTT accepted when a resumed client lands on a
   different edge, with tickets shared across edges against a cache per edge? S4 below finds
   that rustls accepts 0-RTT only from its stateful session cache, which bears on this.
5. **UDP reachability.** What share of clients cannot reach the cluster over UDP at all? That
   decides whether the TLS/TCP listener the transport seam reserves (ADR 0002) is built before
   2.0.0.

Method, results and decisions: pending S3.

## S4: idle connection cost

### Question

What does an idle client connection cost an edge, in memory and CPU, on the stack the transport
will use: quinn 0.11 with rustls 0.23 on aws-lc-rs, TLS 1.3 with a client certificate? How many
handshakes, full, resumed and 0-RTT, does a core complete each second? Which transport settings
lower the cost, and which operating-system limits come into play?

The numbers are laptop numbers: one Apple M2 Pro (6 performance and 4 efficiency cores, 16 GiB),
macOS 26.5, Rust 1.99.0, with server and client on the same machine over loopback, while other
work kept about 4 GiB wired and the memory compressor busy. Memory per connection comes from a
counting allocator in the server process and does not depend on the machine; CPU and rates do.

### Answers

- **Memory.** 37.8 KiB of heap per idle connection with quinn's defaults, 46 to 54 KiB of
  physical footprint, the same from 10^3 to 5×10^4 connections. Limiting the streams a client
  may open brings it to 28.6 KiB (35.6 KiB of footprint).
- **CPU.** An idle connection costs CPU only at its keepalive: 44 to 114 µs each at 10^4
  connections, 55 µs at 5×10^4. Budget 0.4 of a core per 10^5 idle connections.
- **Handshakes.** One core completes 2,867 full handshakes a second with client certificates
  and 3,986 resumed ones with stateless tickets. 0-RTT, which rustls allows only from its
  stateful session cache, managed 2,983 with 70,000 sessions stored and 3,818 with 4,095: the
  cache costs more the more it holds.
- **Settings.** Only the stream limits matter for memory (a fifth less); windows, datagrams and
  the ACK frequency extension do not. MTU discovery costs 1.2 KiB and setup CPU.
- **Operating system.** 8 MiB socket buffers without root, no datagram dropped for full
  buffers; file descriptors are not a limit. Memory was this machine's limit.

### Method

`spikes/s4-quinn` is one binary that runs as a server process or as a client process, and as an
orchestrator that starts both and writes JSON.

- **Versions.** quinn 0.11.12 (quinn-proto 0.11.19, quinn-udp 0.5.16), rustls 0.23.45,
  rustls-webpki 0.103.15, aws-lc-rs 1.18.1 (aws-lc-sys 0.45.0), rcgen 0.14.10, tokio 1.53.1,
  release build with thin LTO. quinn and rustls run without their ring defaults, and `main`
  installs the aws-lc-rs provider before any TLS configuration exists.
- **TLS.** TLS 1.3 only, ALPN `mqtt`. The server requires a client certificate and verifies it
  against one CA with rustls's WebPKI verifier (but see finding F4 on the clientAuth extended key
  usage). Keys and certificates are ECDSA P-256, made by rcgen for every run;
  client CNs look like device CNs. The key share is rustls's default, X25519MLKEM768 first,
  except where a row says X25519.
- **Server.** Either one endpoint on a tokio runtime with ten worker threads, or one endpoint per
  core: ten endpoints on ten ports, each on its own thread with a single-threaded runtime. Each
  connection is served as the broker will serve it: the client's first bidirectional stream is
  the control stream, CONNECT gets CONNACK and PINGREQ gets PINGRESP. QUIC idle timeout 60 s,
  longer than 1.5 times the 30 s keepalive (spec section 6). Every second the server prints its
  live heap and allocation count (a counting global allocator), its physical footprint and
  resident size (`proc_pid_rusage`) and its CPU time (`getrusage`).
- **Client.** N connections over 32 UDP sockets, at most 512 handshakes at a time. Each opens the
  control stream, sends CONNECT and reads CONNACK, then sends one PINGREQ at a random point of the
  first 30 s so that keepalives spread evenly (quinn restarts its keep-alive timer on every packet
  it receives). After that it is kept alive every 30 s either by QUIC PING frames (quinn's
  `keep_alive_interval`) or by PINGREQ on the control stream, and is otherwise silent.
- **Idle measurement.** The server's CPU with no connections is measured for 10 s first. Memory
  per connection is read 35 s after the last connection is up, when every connection has had its
  first PINGREQ, relative to that baseline. CPU is read over the following 60 s (30 s for the
  transport settings) less the baseline rate, and divided by the keepalives due in that window.
- **Handshakes.** The server runs on one thread, asked onto a performance core. The client runs
  128 lanes on nine threads; each lane connects, exchanges CONNECT and CONNACK, closes, and
  connects again, under a server name of its own so that its tickets stay apart. After 3 s of
  warm-up, 20 s are measured from the server's own handshake counter and CPU time. The server
  issues two tickets per handshake, rustls's default. *Full*: the client keeps no tickets.
  *Resumed*: every handshake after a lane's first resumes from that lane's ticket, with the
  server using stateless tickets or a session cache of 100,000 entries. *0-RTT*: as resumed,
  with the session cache, and CONNECT sent in 0-RTT; once more with a cache of 4,096 entries.
  The server counts the sessions its cache holds by following the cache's own rules (a stored
  ticket adds one, a resumption that finds its session removes one, an insertion into a full
  cache evicts one), and records what the cache allocates when it is made.
- **Operating system.** Each UDP socket's buffers are set to 8 MiB, `kern.ipc.maxsockbuf`, the
  most macOS allows without root (the default receive buffer here is
  `net.inet.udp.recvspace` = 786,896 bytes). Datagrams dropped for full socket buffers are read
  from `netstat -s -p udp` before and after each run. File descriptors are not a limit: an
  endpoint is one UDP socket whatever its connection count.

`spikes/s4-quinn/run.sh` reruns everything and writes the files under `bench/results/s4-*.json`
that this section quotes.

### Results

#### Memory per idle connection

| Connections | Endpoints | Keepalive | Settings | Heap per connection | Allocations per connection | Physical footprint per connection | Client footprint per connection | Live after the idle window |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | one endpoint | QUIC PING | default | 37.9 KiB | 58 | 53.5 KiB | 54.0 KiB | 1,000 |
| 10,000 | one endpoint | QUIC PING | default | 37.8 KiB | 58 | 47.4 KiB | 46.9 KiB | 10,000 |
| 50,000 | one endpoint | QUIC PING | default | 37.8 KiB | 58 | 46.4 KiB | 45.8 KiB | 50,000 |
| 10,000 | one per core | QUIC PING | default | 37.8 KiB | 58 | 47.1 KiB | 46.9 KiB | 10,000 |
| 10,000 | one endpoint | PINGREQ | default | 37.8 KiB | 58 | 47.2 KiB | 46.9 KiB | 10,000 |
| 50,000 | one endpoint | QUIC PING | lean | 28.6 KiB | 58 | 35.6 KiB | 35.0 KiB | 50,000 |

An idle connection costs the server 37.8 KiB of heap in 58 allocations with quinn's defaults,
whatever the number of connections and whether one endpoint or one per core serves them. The
physical footprint adds what the allocator rounds and fragments, and counts the pages macOS
compresses: 47 KiB a connection at 10^4, 54 KiB at 10^3 where fixed costs weigh more. The
client's side of a connection costs the same. Resident size is left out: under this machine's
memory pressure macOS compressed idle connection pages, and resident size fell below the heap.
At 5×10^4 connections, the most this machine held beside its other work, the figures held:
37.8 KiB of heap and 46.4 KiB of footprint a connection with quinn's defaults, 28.6 KiB and
35.6 KiB with the lean settings, and every connection still alive after the idle window.

#### CPU while idle

| Connections | Endpoints | Keepalive | Settings | Keepalives a second | CPU, cores | CPU per keepalive | Setup, connections a second | Server CPU per setup handshake |
| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | one endpoint | QUIC PING | default | 33 | 0.0019 | 58 µs | 1,949 | 805 µs |
| 10,000 | one endpoint | QUIC PING | default | 333 | 0.0145 | 44 µs | 3,289 | 804 µs |
| 50,000 | one endpoint | QUIC PING | default | 1,667 | 0.0921 | 55 µs | 2,925 | 814 µs |
| 10,000 | one per core | QUIC PING | default | 333 | 0.0297 | 89 µs | 3,913 | 885 µs |
| 10,000 | one endpoint | PINGREQ | default | 333 | 0.0380 | 114 µs | 2,455 | 1,034 µs |
| 10,000 | one endpoint | QUIC PING | streams | 333 | 0.0261 | 78 µs | 2,477 | 1,022 µs |
| 10,000 | one endpoint | QUIC PING | windows | 333 | 0.0364 | 109 µs | 2,473 | 1,042 µs |
| 10,000 | one endpoint | QUIC PING | no-datagrams | 333 | 0.0277 | 83 µs | 1,978 | 988 µs |
| 10,000 | one endpoint | QUIC PING | no-mtu-discovery | 333 | 0.0277 | 83 µs | 2,799 | 677 µs |
| 10,000 | one endpoint | QUIC PING | ack-frequency | 333 | 0.0211 | 63 µs | 2,470 | 990 µs |
| 10,000 | one endpoint | QUIC PING | lean | 333 | 0.0187 | 56 µs | 3,284 | 486 µs |
| 50,000 | one endpoint | QUIC PING | lean | 1,667 | 0.0922 | 55 µs | 4,313 | 528 µs |

An idle connection costs CPU only when its keepalive arrives. At 10^4 connections, 333
keepalives a second took 0.015 to 0.038 of a core, 44 to 114 µs each. Settings that cannot
change the work done per keepalive (windows, datagrams) moved that figure by up to 2.5 times,
so the differences between rows, PINGREQ against QUIC PING or one endpoint against one per core
included, are within this machine's run-to-run spread and are not resolved here. At 5×10^4 the
spread closes: 1,667 keepalives a second took 0.092 of a core with either setting, 55 µs each.
For sizing, take 120 µs a keepalive: 10^5 idle connections on one edge with a 30 s keepalive
cost 0.4 of a core. macOS inflates it: there quinn-udp makes one system call per datagram
unless its `fast-apple-datapath` feature (private Apple interfaces) is on, where on Linux it
batches with `recvmmsg` and uses GSO and GRO.

Setting connections up 512 at a time cost the server 490 to 1,040 µs of CPU per connection,
with ten server threads competing for the ten cores with the client. That is two to three times
what a dedicated server thread spends per handshake in the next table, and an upper bound.

#### Transport settings

At 10^4 connections, one setting changed at a time:

| Settings | What changes | Heap per connection | Change | Physical footprint per connection |
| --- | --- | ---: | ---: | ---: |
| default | quinn's defaults: 100 bidirectional and 100 unidirectional streams the peer may open, 1.25 MB stream window, unlimited connection window, datagrams on, MTU discovery on | 37.8 KiB | | 47.4 KiB |
| streams | at most 4 bidirectional streams the peer may open, no unidirectional ones | 29.8 KiB | -8.0 KiB (-21%) | 37.8 KiB |
| windows | 64 KiB per stream, 256 KiB per connection in both directions | 37.8 KiB | none | 47.3 KiB |
| no-datagrams | datagrams off (the spec does not use them) | 37.8 KiB | none | 47.3 KiB |
| no-mtu-discovery | MTU discovery off, staying at 1,200 bytes | 36.6 KiB | -1.2 KiB (-3%) | 45.8 KiB |
| ack-frequency | the ACK frequency extension on | 37.8 KiB | none | 47.3 KiB |
| lean | streams, windows, no datagrams and no MTU discovery together | 28.6 KiB | -9.2 KiB (-24%) | 36.5 KiB |

Only the number of streams the peer may open changes memory much. quinn creates state for every
one of them when a connection starts (`StreamsState::new` in quinn-proto inserts an entry for
each stream id the peer may use), 100 bidirectional and 100 unidirectional by default: about 40
bytes for each allowed stream. MQTT over QUIC needs the control stream and a few data streams,
all bidirectional and opened by the client (spec section 2), so 4 and 0 save 8.0 KiB, a fifth.
Windows and the datagram buffer are limits, not allocations, and cost an idle connection
nothing. MTU discovery costs 1.2 KiB and, during setup, the probes sent after each handshake: the
runs without it spent 486 and 677 µs of server CPU per connection setting up, against 800 to
1,040 with it. The ACK frequency extension changes nothing for an idle connection.

#### Handshakes

| Handshake | Key share | Server resumption | Handshakes a second, one server thread | Server CPU | CPU per handshake | Resumed | 0-RTT accepted | Sessions held at the end |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| full | X25519MLKEM768 | stateless tickets | 2,867 | 0.99 cores | 347 µs | 0.0% |  |  |
| full | X25519 | stateless tickets | 3,051 | 0.99 cores | 326 µs | 0.0% |  |  |
| resumed | X25519MLKEM768 | stateless tickets | 3,986 | 1.00 cores | 250 µs | 100.0% |  |  |
| resumed | X25519 | stateless tickets | 4,305 | 0.98 cores | 228 µs | 100.0% |  |  |
| resumed | X25519MLKEM768 | session cache of 100,000 | 3,008 | 0.98 cores | 326 µs | 100.0% |  | 70,633 |
| 0-RTT | X25519MLKEM768 | session cache of 100,000 | 2,983 | 0.97 cores | 326 µs | 100.0% | 59,684 of 59,684 | 70,427 |
| 0-RTT | X25519MLKEM768 | session cache of 4,096 | 3,818 | 0.99 cores | 258 µs | 100.0% | 76,385 of 76,385 | 4,095 |

One server thread completes 2,867 full handshakes a second with a client certificate (347 µs
each) and 3,986 resumed ones with stateless tickets (250 µs). Each figure includes issuing two
tickets. The post-quantum key share costs 6% of a full handshake and 10% of a resumed one. With
rustls's session cache, resumed and 0-RTT handshakes cost the same, and what they cost depends
on how many sessions the cache holds: 326 µs each with 70,000 sessions stored, 258 µs with
4,095, close to stateless resumption. rustls takes a resumed session out of its eviction order
by a linear search (`LimitedCache::remove`), which here cost about 1 ns for every session held,
so a cache of 10^6 sessions would add about 1 ms to every resumption. A stored session takes 933
bytes of heap, and the cache allocates 88 bytes more for every session it may hold when it is
made (8.8 MB for 100,000); a client that keeps both of its tickets takes two sessions. 0-RTT was
accepted every time it was offered: it saves the client a round trip, not the server work.
Latency is not reported: on loopback, with 128 lanes queued on one server thread, it measures
the queue. The rows with the session cache were measured again after a fix to how stored
sessions are counted, and the 4,096 row was added then (the file's `reruns` entry); the other
rows are from the first run.

#### Operating-system limits

- **Socket buffers.** Every socket asked for 8 MiB and got it: `kern.ipc.maxsockbuf` allows 8 MiB
  without root on this machine, against a default receive buffer of 768 KiB. With that, no
  datagram was dropped for a full socket buffer in any run but one, which counted 92 during
  setup (the counter is system wide). On Linux the caps are `net.core.rmem_max` and
  `net.core.wmem_max`, far lower by default; an edge's deployment has to raise them. Not
  measured here.
- **File descriptors** are not a limit: an endpoint is one UDP socket whatever its connection
  count, so 50,000 connections used one descriptor on the server and 32 on the client.
- **Memory** was this machine's limit. At 5×10^4 connections with quinn's defaults each process
  held about 2.4 GB, and with the machine's other work free memory fell to 35%; larger runs
  were not tried.

### Findings

- **F1. Stream state is allocated up front.** See Transport settings: 8 KiB of every
  connection's 38 KiB is state for streams the client will never open.
- **F2. 0-RTT needs the stateful session cache on rustls.** While its tickets are stateless
  (`ServerConfig::ticketer` enabled), a rustls server neither offers nor accepts early data
  ("early_data with stateless resumption is not allowed"): a ticket that is not stored cannot be
  made single use, and replay protection depends on that. A session cache is per process unless
  something shares it, so a client that resumes on another edge gets 1-RTT resumption at best.
  Sharing ticket keys across edges, the obvious way to resume anywhere, gives resumption across
  edges but not 0-RTT: that needs a single-use store shared by the edges, or clients that come
  back to the edge that served them. S3 measures how often they do. rustls's own cache would
  not serve at an edge's scale anyway: each resumption searches it linearly (see Handshakes).
- **F3. The post-quantum key share is cheap enough to keep:** 6% of a full handshake.
- **F4. rustls's WebPKI client verifier accepts a client certificate that has no extended key
  usage at all.** It refuses one whose extended key usage lacks clientAuth
  (`KeyUsage::client_auth` in rustls-webpki 0.103 is "required if present"), but spec section 3
  and R2 rule 3 require the clientAuth usage to be there. `openqtt-transport` needs a check of
  its own on the verified certificate.
- **F5. The gate will see ring when quinn arrives.** quinn-proto 0.11.19 depends on ring for
  `wasm32-unknown-unknown` only; for aarch64-apple-darwin, x86_64-unknown-linux-gnu and
  aarch64-unknown-linux-gnu the tree has no ring. But `make layers` reads `cargo tree --target
  all` and `deny.toml` checks every target (`[graph] targets = []`), so both will refuse ring
  as soon as a workspace crate depends on quinn.

### Decisions

- **D1. Budget 40 KiB per idle client connection for the transport** on an edge, with the
  settings of D2 (36.5 KiB of footprint measured), before the broker's own session state. That
  is 26,000 connections per GiB, or 10^5 connections in 3.8 GiB.
- **D2. The client listener's transport settings:** no unidirectional streams; at most 8
  bidirectional streams per connection, the control stream and seven data streams, at about 40
  bytes each (4 were measured); datagrams off (spec section 1 does not use them); stream and
  connection windows bounded by the 1 MiB maximum packet size (R1, O5), which costs nothing
  while idle; MTU discovery on, for the larger packets it finds on real paths.
- **D3. Keepalive is MQTT's** (spec section 6, R2 rule 24: 30 s), and an edge budgets 120 µs of
  CPU per keepalive on this class of hardware until Linux numbers replace it: 0.4 of a core per
  10^5 idle connections.
- **D4. Handshake capacity** for planning: 350 µs of a core per full handshake and 250 µs per
  resumed one. 10^5 clients of a lost edge reconnecting with full handshakes cost 35
  core-seconds across the edges that take them.
- **D5. Resumption uses stateless tickets** (the fastest, nothing stored per client, and
  shareable across edges). 0-RTT stays off until S3 has measured where resuming clients land:
  on rustls it needs a session store (F2), at 933 bytes per stored session plus 88 per slot in
  rustls's cache, whose search per resumption grows with what it holds, so 0-RTT at an edge's
  scale needs a store of OpenQTT's own with constant-time removal.
- **D6. Keep rustls's default key share,** X25519MLKEM768 first (F3).
- **D7. One endpoint per core stays the layout.** Memory and idle CPU are the same either way
  within this machine's spread, and it set 10^4 connections up at 3,913 a second against 3,289
  for one endpoint on a ten-thread runtime, a difference the spread could also explain. Nothing
  here argues for changing it; how packets reach the right endpoint is S3's question.
- **D8. Two changes go with the first crate that takes quinn,** `openqtt-transport`: the
  explicit clientAuth check of F4, and limiting `deny.toml` and `make layers` to the shipped
  targets so that quinn-proto's ring for wasm (F5) does not fail the gate.

### Risks and what was not measured

- **macOS on a laptop.** Linux, the production platform, batches datagrams and parks threads
  differently; its CPU figures will be lower and are not measured here. Memory per connection
  should carry over, allocator overhead aside.
- **Loopback.** No loss, reordering, NAT or load balancer, and a round trip of about 0.1 ms. S3
  covers the load balancer; connections that migrate, lose packets or update keys are not
  measured.
- **One machine for both ends.** Client and server competed for cores and memory, which limited
  the connection count and spread the CPU figures.
- **Transport only.** The broker's own state per connection (session, buffers, authorization
  cache, its entries in the local index) comes on top of these figures.
- **Idle only.** Connections that carry traffic cost more: buffers grow with in-flight data, up
  to the windows of D2.

