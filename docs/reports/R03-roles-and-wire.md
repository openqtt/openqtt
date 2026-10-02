# R3. Roles, the message path and the wire

Status: draft. Decisions marked **Spike** wait for a measured result (R4, R6, R7).

## Roles

One binary, `openqtt`, runs one or more roles. Each role is its own set of pods in
Kubernetes and scales on what loads it. `--role all` runs every role in one process
for development and small deployments; it uses the same code paths over loopback.

| Role | Holds | Scales with | Kubernetes shape |
| --- | --- | --- | --- |
| edge | QUIC listeners, TLS, MQTT sessions of connected clients, a local subscription index, cached authorization rules | connections | Deployment |
| router | The interest index: which edges and log partitions want which topics | subscriptions | Deployment (StatefulSet if shard ownership is by ordinal) |
| log | Durable state in Raft partitions: retained messages, persistent sessions, offline queues, QoS 2 state, ownership of client identifiers; plus the meta group | bytes and throughput | StatefulSet with volumes |
| admin | REST API and CLI backend | operators | Deployment, never exposed outside the cluster |

## The hot path

A message crosses at most edge, then log, then edge. Authorization is decided inside
the edge from rules cached there.

1. The publishing edge authorizes, mounts the topic, and matches it against its
   merged route view (section Router).
2. Matches are grouped by destination: subscribers on this edge are delivered
   directly; other edges get the message on an edge-to-edge lane; log partitions
   holding durable interest get an append; a destination of kind `External` goes to
   an extension forwarder (the extension seam).
3. PUBACK (or PUBREC) is sent when:
   - the retained write has committed, if the retain flag is set;
   - every partition holding durable interest has committed the message;
   - live non-durable destinations have it queued on their lane.
   Committed means fsynced by a majority of the partition's replicas.
4. A denied publish gets reason code 0x87.

## Membership and placement

- Seeds come from headless Service DNS (`OPENQTT_SEEDS` outside Kubernetes). No
  Kubernetes API access and no gossip.
- Log pods with ordinals 0, 1 and 2 form the **meta group**, a Raft group holding
  membership, placement, users, bans and API keys.
- Every pod registers with the meta group and receives `(NodeId, Epoch)`. The epoch
  increases on every restart. Pods renew a lease every second; it expires after six.
- **A pod that cannot renew fences itself.** An edge sends its clients DISCONNECT
  0x9C and closes; a log replica steps down.
- The meta leader runs placement:
  - log: `P` partitions (256 by default, fixed when the cluster is created), each a
    Raft group, replication factor 1 or 3, replicas spread across zones;
  - router: 64 virtual shards assigned to live routers by rendezvous hashing.
- The placement table is versioned. Every pod watches it. Every request to a log
  partition carries `(NodeId, Epoch)`; a stale epoch is refused.
- A client identifier belongs to partition `partition_of(client_id)`: xxh3-64 with a
  fixed seed, modulo `P`. A golden test pins the function.

## The log

- openraft, one group per partition. Replication factor 1 first; moving to 3 is a
  membership change (add learners, promote).
- Storage engine behind a `KvEngine` trait. **Spike** (R4): fjall, redb or RocksDB;
  shared fsync between the Raft log and the state machine; multi-Raft or
  primary-backup if per-group overhead is too high.
- Keys sort byte-wise. Values are protobuf.

| Key | Contents |
| --- | --- |
| `own/{cid}` | Owner node, epoch and connection generation, will, session expiry. Written for every client: it is the authority for takeover |
| `sess/{cid}` | Subscriptions, next packet identifier, limits. Only when session expiry is above 0 |
| `msg/{seq}` | A message body, stored once per partition, collected below the lowest session cursor |
| `q/{cid}/{seq}` | Offline and pending queue entries |
| `infl/{cid}/{seq}` | Outbound in-flight state (sent, or received PUBREC) |
| `rel/{cid}/{pid}` | Inbound QoS 2 awaiting PUBREL: the receipt, committed with the message. A publication whose receipt the partition already holds is answered as accepted and not routed again. Deleted on PUBREL, on a refusal of that identifier, and with the session |
| `ret/{topic}` | Retained message and expiry, in the partition of the topic's first K literal levels (K fixed when the cluster is created) |
| `exp/…`, `will/…`, `byedge/…` | Timers and cleanup after an edge's epoch dies |

Retained messages are partitioned by a topic prefix so that a subscription with K
literal leading levels reads one partition. Filters with fewer literal levels query
every partition in parallel with bounded concurrency.

## Router

- **Edges register coarse interest.** Each edge keeps an exact local index and sends
  the router only changes to a cover set: a trie node with more than T distinct
  children (256 by default) is replaced by `prefix/#`. A delivery that matches no
  local subscriber is dropped and counted; the counter tunes T.
- **Durable subscriptions belong to log partitions, not edges**, so persistent
  sessions moving between edges never change routes.
- Each router shard owns filters by hash and keeps an arena trie (nodes in a
  vector, interned levels, separate `+` and `#` slots). Destination sets are small
  sorted vectors, promoted to roaring bitmaps above 64 entries.
- Every edge subscribes to every shard's route view (snapshot, then sequenced
  changes) and merges them into one local trie, so publish lookups never leave the
  edge. When a router dies, edges keep serving their last view.
- **Spike** (R6): memory per filter, match latency at a million filters, and the
  false-positive rate on device-shaped topics.

## Sessions

- **Connect.** The edge authenticates, then sends `Claim` to the partition leader,
  which commits `own` with the next connection generation. If another edge owns the
  client, the leader tells it `StepDown`; that edge sends DISCONNECT 0x8E, flushes
  acknowledgements to the log and closes. For session expiry above 0 the leader
  returns the session snapshot. CONNACK is sent only after the commit.
- **Sessions with expiry 0** live only in the edge. Nothing moves on takeover.
- **Wills** are stored in `own`. When an edge's epoch dies, the log publishes the
  wills of its clients, honouring will delay.
- **Drain.** On shutdown an edge stops being ready, then over its grace period sends
  each client DISCONNECT 0x9C, with jitter, and closes. 0x9D is reserved for
  permanent moves.
- **Resume elsewhere.** The same `Claim`; the log then streams queued messages to
  the new edge within the client's Receive Maximum.
- **QoS 2 from a client.** Every QoS 2 publication carries the client's Packet
  Identifier, and the partition commits `rel/{cid}/{pid}` with the message, so
  `rel` decides whether a PUBLISH is new, not the edge's memory. A connection that
  ends while such a commit is out hands its identifier over as reserved: the
  client publishes it again on its next connection, the edge publishes it again
  under the same receipt, and the partition routes it only if the first commit
  never happened. Either way the message is delivered once. The session machine
  releases the receipt when PUBREL arrives, or when it refuses a reserved
  identifier, since the client may then reuse it for a new message.

## The wire between roles

- QUIC (quinn) with mutual TLS. Certificates name the role and pod
  (`spiffe://openqtt/<cluster>/<role>/<pod>`); every service checks the caller's
  role.
- Per peer pair: one control stream (Hello, heartbeats). Long-lived unidirectional
  lanes carry batched flows: edge to edge (chosen by hash of the publisher, which
  keeps per-publisher order), edge to log (per partition), log to edge (per
  partition, credit-based). Requests open a fresh bidirectional stream.
- Frames are a varint length and an envelope `{type: u32, corr: u64, body}`. Bodies
  are protobuf (prost), with generated code committed. Payloads are carried as
  bytes without copying.
- **Versioning.** Each side sends `Hello{min, max}` and the link uses the lower
  maximum. Release N interoperates with N-1. Field tags are never reused; new
  fields are optional; a new message is sent only to a peer that advertised it;
  removal takes two releases. Message types 0x8000 to 0xFFFF are reserved for
  extensions. Stored entries carry a format version readable by N-1. Upgrade order:
  log, router, edge, admin. CI runs the previous release against the current one.

## The extension seam

`openqtt-ext` holds traits only. Every type is `#[non_exhaustive]`, new trait methods
come with defaults, the crate exports `API_VERSION`, and cargo-semver-checks guards
it.

- `Authenticator` (asynchronous) and `Authorizer` (synchronous, over cached rules).
- `SessionEvents` for connect, disconnect, subscribe and similar events.
- `InterestSource` and `Forwarder`, for destinations outside the cluster. A
  forwarder's acknowledgement counts toward the durability rule in the hot path.
- `LogConsumer`, to read committed partition streams with a durable cursor.
- `RedirectPolicy`, which chooses the Server Reference in 0x9C and 0x9D.

## When one pod is lost

| Role | Clients | Messages |
| --- | --- | --- |
| edge | Connections drop; clients reconnect, resuming TLS (0-RTT where allowed). The edge's routes are purged about six seconds after its lease lapses | QoS 0 and in-flight deliveries to sessions with expiry 0 are lost, as MQTT allows. Unacknowledged publishes are resent by clients with DUP. Persistent sessions lose nothing; wills fire |
| router | No effect | Edges keep their route view; new subscriptions on that shard are not visible to other edges for a few seconds |
| log, replication 3 | Connects and publishes on partitions it led pause for one to two seconds | Nothing committed is lost |
| log, replication 1 | Partitions it held are unavailable until it returns | Nothing committed is lost if its volume returns |
| admin | REST and CLI unavailable (run two) | None |

## Open questions for the spikes

- R4: engine, fsync sharing, per-group cost, claims per second (target 50,000 per
  cluster for reconnect storms), bytes per idle session at 10^8.
- R6: the coarsening threshold T and router memory.
- R7: keeping migrated connections on their edge behind the load balancer.
