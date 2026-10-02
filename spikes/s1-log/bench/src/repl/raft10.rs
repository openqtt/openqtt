//! openraft 0.10.0-alpha.36, the same cluster as `raft.rs` on the next release line.
//!
//! The difference that matters here: 0.9's core awaits every log flush before it does anything
//! else (`append_to_log` waits on the callback), and appends one client write at a time. 0.10
//! submits the append and moves on ("do not wait for the response"), batches queued client
//! writes into one append, and has a separate heartbeat path. Everything else mirrors `raft.rs`:
//! in-memory entries, durability handed to the node's shared `Disk`, metadata-only snapshots.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{Result, anyhow};
use futures_util::{Stream, StreamExt as _};
use openraft10::errors::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft10::network::{RPCOption, RaftNetworkFactory, v2::RaftNetworkV2};
use openraft10::base::{BoxFuture, BoxStream};
use openraft10::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, StreamAppendResult,
    VoteRequest, VoteResponse,
};
use openraft10::storage::{EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftStateMachine};
use openraft10::type_config::alias::{
    LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
};
use openraft10::{
    BasicNode, Config, EntryPayload, OptionalSend, Raft, RaftLogReader, RaftSnapshotBuilder,
    SnapshotPolicy,
};

use super::net::{DelayLine, Disk};
use super::raft::{Cmd, Timing};

openraft10::declare_raft_types!(
    pub C10:
        D = Cmd,
        R = u64,
);

type E = <C10 as openraft10::RaftTypeConfig>::Entry;
type Snap = Vec<u8>;

#[derive(Default)]
struct LogInner {
    vote: Option<VoteOf<C10>>,
    committed: Option<LogIdOf<C10>>,
    last_purged: Option<LogIdOf<C10>>,
    log: BTreeMap<u64, E>,
}

#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Mutex<LogInner>>,
    disk: Disk,
}

impl RaftLogReader<C10> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<E>, io::Error> {
        let g = self.inner.lock().expect("not poisoned");
        Ok(g.log.range(range).map(|(_, e)| e.clone()).collect())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<C10>>, io::Error> {
        Ok(self.inner.lock().expect("not poisoned").vote.clone())
    }
}

impl RaftLogStorage<C10> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<C10>, io::Error> {
        let g = self.inner.lock().expect("not poisoned");
        let last = g
            .log
            .iter()
            .next_back()
            .map(|(_, e)| e.log_id.clone())
            .or_else(|| g.last_purged.clone());
        Ok(LogState {
            last_purged_log_id: g.last_purged.clone(),
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf<C10>) -> Result<(), io::Error> {
        self.inner.lock().expect("not poisoned").vote = Some(vote.clone());
        self.disk.barrier().await;
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<C10>>) -> Result<(), io::Error> {
        self.inner.lock().expect("not poisoned").committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<C10>>, io::Error> {
        Ok(self.inner.lock().expect("not poisoned").committed.clone())
    }

    async fn append<I>(&mut self, entries: I, callback: IOFlushed<C10>) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = E> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut g = self.inner.lock().expect("not poisoned");
            for e in entries {
                g.log.insert(e.log_id.index(), e);
            }
        }
        self.disk.submit(move || callback.io_completed(Ok(())));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<C10>>) -> Result<(), io::Error> {
        let mut g = self.inner.lock().expect("not poisoned");
        let from = last_log_id.map(|l| l.index() + 1).unwrap_or(0);
        let _ = g.log.split_off(&from);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<C10>) -> Result<(), io::Error> {
        let mut g = self.inner.lock().expect("not poisoned");
        let keep = g.log.split_off(&(log_id.index() + 1));
        g.log = keep;
        g.last_purged = Some(log_id);
        Ok(())
    }
}

#[derive(Default)]
struct SmInner {
    last_applied: Option<LogIdOf<C10>>,
    membership: StoredMembershipOf<C10>,
    own: HashMap<Vec<u8>, Vec<u8>>,
    applied: u64,
    snapshot: Option<SnapshotMetaOf<C10>>,
}

#[derive(Clone, Default)]
pub struct Sm {
    inner: Arc<Mutex<SmInner>>,
}

impl RaftSnapshotBuilder<C10> for Sm {
    type SnapshotData = Snap;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<C10, Snap>, io::Error> {
        let mut g = self.inner.lock().expect("not poisoned");
        let meta = SnapshotMetaOf::<C10> {
            last_log_id: g.last_applied.clone(),
            last_membership: g.membership.clone(),
        };
        g.snapshot = Some(meta.clone());
        Ok(SnapshotOf::<C10, Snap> {
            meta,
            snapshot: Vec::new(),
        })
    }
}

impl RaftStateMachine<C10> for Sm {
    type SnapshotData = Snap;
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<C10>>, StoredMembershipOf<C10>), io::Error> {
        let g = self.inner.lock().expect("not poisoned");
        Ok((g.last_applied.clone(), g.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<C10>, io::Error>> + Unpin + OptionalSend,
    {
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let resp = {
                let mut g = self.inner.lock().expect("not poisoned");
                g.last_applied = Some(entry.log_id.clone());
                match entry.payload {
                    EntryPayload::Blank => 0,
                    EntryPayload::Normal(c) => {
                        g.own.insert(c.cid, c.val);
                        g.applied += 1;
                        g.applied
                    }
                    EntryPayload::Membership(m) => {
                        g.membership = StoredMembershipOf::<C10>::new(Some(entry.log_id.clone()), m);
                        0
                    }
                }
            };
            if let Some(r) = responder {
                r.send(resp);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(&mut self, meta: &SnapshotMetaOf<C10>, _snapshot: Snap) -> Result<(), io::Error> {
        let mut g = self.inner.lock().expect("not poisoned");
        g.last_applied = meta.last_log_id.clone();
        g.membership = meta.last_membership.clone();
        g.snapshot = Some(meta.clone());
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<C10, Snap>>, io::Error> {
        let g = self.inner.lock().expect("not poisoned");
        Ok(g.snapshot.clone().map(|meta| SnapshotOf::<C10, Snap> {
            meta,
            snapshot: Vec::new(),
        }))
    }
}

type R10 = Raft<C10, Sm>;

pub struct Router {
    rafts: RwLock<HashMap<(u32, u64), R10>>,
    line: DelayLine,
    delay: Duration,
    pub rpcs: AtomicU64,
    /// Send each append and wait for its answer before the next, as openraft's default does.
    sequential: bool,
}

impl Router {
    fn get(&self, group: u32, node: u64) -> Option<R10> {
        self.rafts.read().expect("not poisoned").get(&(group, node)).cloned()
    }
}

#[derive(Clone)]
pub struct Net {
    router: Arc<Router>,
    group: u32,
}

impl RaftNetworkFactory<C10> for Net {
    type Network = Conn;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Conn {
        Conn {
            router: self.router.clone(),
            group: self.group,
            target,
        }
    }
}

pub struct Conn {
    router: Arc<Router>,
    group: u32,
    target: u64,
}

impl Conn {
    fn raft(&self) -> Result<R10, Unreachable<C10>> {
        self.router
            .get(self.group, self.target)
            .ok_or_else(|| Unreachable::new(&io::Error::other("no such raft")))
    }
}

/// Stamps each item with when it is due as it arrives, then yields it no sooner, in order. Every
/// item waits the same delay, so order is kept and items in flight overlap, like a TCP stream or
/// a QUIC stream does.
fn delayed<T: Send + 'static>(
    input: impl Stream<Item = T> + Send + 'static,
    line: DelayLine,
    delay: Duration,
    sent: Option<Arc<Router>>,
) -> impl Stream<Item = T> + Send + 'static {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<(std::time::Instant, T)>();
    tokio::spawn(async move {
        futures_util::pin_mut!(input);
        while let Some(x) = input.next().await {
            if let Some(r) = &sent {
                r.rpcs.fetch_add(1, Ordering::Relaxed);
            }
            if tx.send((std::time::Instant::now() + delay, x)).is_err() {
                return;
            }
        }
    });
    futures_util::stream::unfold(rx, move |mut rx| {
        let line = line.clone();
        async move {
            let (at, x) = rx.recv().await?;
            line.sleep_until(at).await;
            Some((x, rx))
        }
    })
}

impl RaftNetworkV2<C10> for Conn {
    type SnapshotData = Snap;

    /// Pipelined: requests go out as the leader produces them, the follower takes them through
    /// its own pipelined `stream_append`, and the answers come back in order, each one hop later.
    /// openraft's default sends one request and waits for its answer before the next.
    fn stream_append<'s, S>(
        &'s mut self,
        input: S,
        _option: RPCOption,
    ) -> BoxFuture<'s, Result<BoxStream<'s, Result<StreamAppendResult<C10>, RPCError<C10>>>, RPCError<C10>>>
    where
        S: Stream<Item = AppendEntriesRequest<C10>> + OptionalSend + Unpin + 'static,
    {
        if self.router.sequential {
            return openraft10::network::stream_append_sequential(self, input, _option);
        }
        Box::pin(async move {
            let raft = self.raft()?;
            let line = self.router.line.clone();
            let delay = self.router.delay;
            let there = delayed(input, line.clone(), delay, Some(self.router.clone()));
            let answers = raft.stream_append(there);
            let back = delayed(answers, line, delay, None)
                .map(|r| r.map_err(|e| RPCError::Unreachable(Unreachable::new(&e))));
            let out: BoxStream<'s, Result<StreamAppendResult<C10>, RPCError<C10>>> = Box::pin(back);
            Ok(out)
        })
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C10>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<C10>, RPCError<C10>> {
        self.router.rpcs.fetch_add(1, Ordering::Relaxed);
        self.router.line.sleep(self.router.delay).await;
        let r = self.raft()?.append_entries(rpc).await;
        self.router.line.sleep(self.router.delay).await;
        r.map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }

    async fn vote(&mut self, rpc: VoteRequest<C10>, _option: RPCOption) -> Result<VoteResponse<C10>, RPCError<C10>> {
        self.router.rpcs.fetch_add(1, Ordering::Relaxed);
        self.router.line.sleep(self.router.delay).await;
        let r = self.raft()?.vote(rpc).await;
        self.router.line.sleep(self.router.delay).await;
        r.map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<C10>,
        snapshot: SnapshotOf<C10, Snap>,
        _cancel: impl std::future::Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<C10>, StreamingError<C10>> {
        self.router.rpcs.fetch_add(1, Ordering::Relaxed);
        self.router.line.sleep(self.router.delay).await;
        let raft = self.raft().map_err(StreamingError::Unreachable)?;
        let r = raft.install_full_snapshot(vote, snapshot).await;
        self.router.line.sleep(self.router.delay).await;
        r.map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))
    }
}

pub struct Cluster {
    pub router: Arc<Router>,
    pub groups: u32,
    pub disks: Vec<Disk>,
}

impl Cluster {
    pub async fn start(
        groups: u32,
        line: DelayLine,
        delay: Duration,
        flush: Duration,
        timing: &Timing,
        sequential: bool,
    ) -> Result<Self> {
        let config = Arc::new(
            Config {
                cluster_name: "s1".into(),
                heartbeat_interval: timing.heartbeat_ms,
                election_timeout_min: timing.election_min_ms,
                election_timeout_max: timing.election_max_ms,
                snapshot_policy: SnapshotPolicy::LogsSinceLast(100_000),
                max_in_snapshot_log_to_keep: 10_000,
                ..Default::default()
            }
            .validate()?,
        );
        let router = Arc::new(Router {
            rafts: RwLock::new(HashMap::new()),
            line: line.clone(),
            delay,
            rpcs: AtomicU64::new(0),
            sequential,
        });
        let disks: Vec<Disk> = (0..3).map(|_| Disk::new(line.clone(), flush)).collect();
        for g in 0..groups {
            for n in 0..3u64 {
                let store = LogStore {
                    inner: Arc::default(),
                    disk: disks[n as usize].clone(),
                };
                let raft = Raft::new(
                    n,
                    config.clone(),
                    Net {
                        router: router.clone(),
                        group: g,
                    },
                    store,
                    Sm::default(),
                )
                .await?;
                router.rafts.write().expect("not poisoned").insert((g, n), raft);
            }
        }
        let members: BTreeMap<u64, BasicNode> =
            (0..3u64).map(|n| (n, BasicNode::default())).collect();
        for g in 0..groups {
            let leader = u64::from(g % 3);
            let raft = router.get(g, leader).ok_or_else(|| anyhow!("missing raft"))?;
            raft.initialize(members.clone()).await?;
        }
        for g in 0..groups {
            let leader = u64::from(g % 3);
            let raft = router.get(g, leader).ok_or_else(|| anyhow!("missing raft"))?;
            raft.wait(Some(Duration::from_secs(60)))
                .current_leader(leader, "leader elected")
                .await?;
        }
        // An elected leader may still refuse writes until its first entry commits; start the
        // clock only once every group has taken one.
        for g in 0..groups {
            let raft = router.get(g, u64::from(g % 3)).ok_or_else(|| anyhow!("missing raft"))?;
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while raft
                .client_write(Cmd { cid: b"warm".to_vec(), val: Vec::new() })
                .await
                .is_err()
            {
                if std::time::Instant::now() > deadline {
                    return Err(anyhow!("group {g} never accepted a write"));
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        Ok(Cluster { router, groups, disks })
    }

    fn leader(&self, group: u32) -> R10 {
        self.router.get(group, u64::from(group % 3)).expect("raft exists")
    }

    pub async fn write(&self, group: u32, cmd: Cmd) -> Result<()> {
        self.leader(group).client_write(cmd).await?;
        Ok(())
    }

    pub async fn misplaced_leaders(&self) -> u32 {
        let mut n = 0;
        for g in 0..self.groups {
            if self.leader(g).current_leader().await != Some(u64::from(g % 3)) {
                n += 1;
            }
        }
        n
    }

    pub async fn shutdown(&self) {
        let rafts: Vec<R10> = self
            .router
            .rafts
            .write()
            .expect("not poisoned")
            .drain()
            .map(|(_, r)| r)
            .collect();
        for r in rafts {
            let _ = r.shutdown().await;
        }
    }
}
