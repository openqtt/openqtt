//! openraft 0.9, one group per partition, three replicas in one process.
//!
//! The log store keeps entries in memory and hands their durability to the node's `Disk`, so
//! every group on a node shares one group commit; `append` returns at once and openraft is told
//! through the `LogFlushed` callback when the batch is durable, which is how a real engine-backed
//! store would do it. The state machine applies claims to a map. Snapshots carry metadata only:
//! nothing here fails over, they exist so openraft can purge the in-memory log.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use anyhow::{Result, anyhow};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, OptionalSend, Raft, RaftLogReader,
    RaftSnapshotBuilder, SnapshotMeta, SnapshotPolicy, StorageError, StoredMembership, Vote,
};

use super::net::{DelayLine, Disk};

/// A claim: the client id and the `own` value to commit for it.
#[derive(Clone, Debug)]
pub struct Cmd {
    pub cid: Vec<u8>,
    pub val: Vec<u8>,
}

// openraft 0.10 wants application data it can print in its logs.
impl std::fmt::Display for Cmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "claim({} bytes)", self.cid.len())
    }
}

/// Several claims carried as one entry: an empty client id marks the batch, and `val` holds
/// length-prefixed (cid, val) pairs. Only the batching proposer below writes these.
fn encode_batch(cmds: &[Cmd]) -> Cmd {
    let mut val = Vec::new();
    for c in cmds {
        val.extend_from_slice(&(c.cid.len() as u16).to_be_bytes());
        val.extend_from_slice(&c.cid);
        val.extend_from_slice(&(c.val.len() as u16).to_be_bytes());
        val.extend_from_slice(&c.val);
    }
    Cmd { cid: Vec::new(), val }
}

fn decode_batch(val: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 2 <= val.len() {
        let n = usize::from(u16::from_be_bytes([val[i], val[i + 1]]));
        let cid = val[i + 2..i + 2 + n].to_vec();
        i += 2 + n;
        let m = usize::from(u16::from_be_bytes([val[i], val[i + 1]]));
        let v = val[i + 2..i + 2 + m].to_vec();
        i += 2 + m;
        out.push((cid, v));
    }
    out
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Cmd,
        R = u64,
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

type E = Entry<TypeConfig>;

#[derive(Default)]
struct LogInner {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    last_purged: Option<LogId<u64>>,
    log: BTreeMap<u64, E>,
}

#[derive(Clone)]
pub struct LogStore {
    inner: Arc<Mutex<LogInner>>,
    disk: Disk,
}

impl LogStore {
    fn new(disk: Disk) -> Self {
        LogStore {
            inner: Arc::default(),
            disk,
        }
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<E>, StorageError<u64>> {
        let g = self.inner.lock().expect("not poisoned");
        Ok(g.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let g = self.inner.lock().expect("not poisoned");
        let last = g
            .log
            .iter()
            .next_back()
            .map(|(_, e)| e.log_id)
            .or(g.last_purged);
        Ok(LogState {
            last_purged_log_id: g.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.lock().expect("not poisoned").vote = Some(*vote);
        // A vote must be durable before it is acted on.
        self.disk.barrier().await;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().expect("not poisoned").vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.inner.lock().expect("not poisoned").committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.inner.lock().expect("not poisoned").committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = E> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut g = self.inner.lock().expect("not poisoned");
            for e in entries {
                g.log.insert(e.log_id.index, e);
            }
        }
        self.disk.submit(move || callback.log_io_completed(Ok(())));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut g = self.inner.lock().expect("not poisoned");
        let _ = g.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut g = self.inner.lock().expect("not poisoned");
        g.last_purged = Some(log_id);
        let keep = g.log.split_off(&(log_id.index + 1));
        g.log = keep;
        Ok(())
    }
}

#[derive(Default)]
struct SmInner {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    own: HashMap<Vec<u8>, Vec<u8>>,
    applied: u64,
    snapshot: Option<SnapshotMeta<u64, BasicNode>>,
}

#[derive(Clone, Default)]
pub struct Sm {
    inner: Arc<Mutex<SmInner>>,
}

impl RaftSnapshotBuilder<TypeConfig> for Sm {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let mut g = self.inner.lock().expect("not poisoned");
        let meta = SnapshotMeta {
            last_log_id: g.last_applied,
            last_membership: g.membership.clone(),
            snapshot_id: format!("{}", g.applied),
        };
        g.snapshot = Some(meta.clone());
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Sm {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let g = self.inner.lock().expect("not poisoned");
        Ok((g.last_applied, g.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<u64>, StorageError<u64>>
    where
        I: IntoIterator<Item = E> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut g = self.inner.lock().expect("not poisoned");
        let mut res = Vec::new();
        for e in entries {
            g.last_applied = Some(e.log_id);
            match e.payload {
                EntryPayload::Blank => res.push(0),
                EntryPayload::Normal(c) => {
                    // A bounded map: the storm reuses client ids.
                    if c.cid.is_empty() {
                        for (cid, val) in decode_batch(&c.val) {
                            g.own.insert(cid, val);
                            g.applied += 1;
                        }
                    } else {
                        g.own.insert(c.cid, c.val);
                        g.applied += 1;
                    }
                    res.push(g.applied);
                }
                EntryPayload::Membership(m) => {
                    g.membership = StoredMembership::new(Some(e.log_id), m);
                    res.push(0);
                }
            }
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let mut g = self.inner.lock().expect("not poisoned");
        g.last_applied = meta.last_log_id;
        g.membership = meta.last_membership.clone();
        g.snapshot = Some(meta.clone());
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let g = self.inner.lock().expect("not poisoned");
        Ok(g.snapshot.clone().map(|meta| Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(Vec::new())),
        }))
    }
}

/// Every Raft instance in the process, by (group, node), and the simulated network between them.
pub struct Router {
    rafts: RwLock<HashMap<(u32, u64), Raft<TypeConfig>>>,
    line: DelayLine,
    delay: Duration,
    pub rpcs: AtomicU64,
}

impl Router {
    fn get(&self, group: u32, node: u64) -> Option<Raft<TypeConfig>> {
        self.rafts.read().expect("not poisoned").get(&(group, node)).cloned()
    }

    async fn hop(&self) {
        self.line.sleep(self.delay).await;
    }
}

#[derive(Clone)]
pub struct Net {
    router: Arc<Router>,
    group: u32,
}

impl RaftNetworkFactory<TypeConfig> for Net {
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

fn unreachable<E: std::error::Error>() -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::other("no such raft")))
}

impl RaftNetwork<TypeConfig> for Conn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.router.rpcs.fetch_add(1, Ordering::Relaxed);
        self.router.hop().await;
        let raft = self.router.get(self.group, self.target).ok_or_else(unreachable)?;
        let r = raft.append_entries(rpc).await;
        self.router.hop().await;
        r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.router.rpcs.fetch_add(1, Ordering::Relaxed);
        self.router.hop().await;
        let raft = self.router.get(self.group, self.target).ok_or_else(unreachable)?;
        let r = raft.install_snapshot(rpc).await;
        self.router.hop().await;
        r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.router.rpcs.fetch_add(1, Ordering::Relaxed);
        self.router.hop().await;
        let raft = self.router.get(self.group, self.target).ok_or_else(unreachable)?;
        let r = raft.vote(rpc).await;
        self.router.hop().await;
        r.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

pub struct Cluster {
    pub router: Arc<Router>,
    pub groups: u32,
    pub disks: Vec<Disk>,
    /// With batching, one proposer per group turns whatever claims are waiting into one entry,
    /// at most two entries in flight, because 0.9 appends one client write per flush.
    proposers: Option<Vec<tokio::sync::mpsc::UnboundedSender<(Cmd, tokio::sync::oneshot::Sender<bool>)>>>,
}

pub struct Timing {
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
}

impl Cluster {
    /// Three nodes, `groups` groups, group g led by node g % 3.
    pub async fn start(
        groups: u32,
        line: DelayLine,
        delay: Duration,
        flush: Duration,
        timing: &Timing,
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
        });
        let disks: Vec<Disk> = (0..3).map(|_| Disk::new(line.clone(), flush)).collect();
        for g in 0..groups {
            for n in 0..3u64 {
                let raft = Raft::new(
                    n,
                    config.clone(),
                    Net {
                        router: router.clone(),
                        group: g,
                    },
                    LogStore::new(disks[n as usize].clone()),
                    Sm::default(),
                )
                .await?;
                router.rafts.write().expect("not poisoned").insert((g, n), raft);
            }
        }
        let members: BTreeSet<u64> = [0, 1, 2].into();
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
        Ok(Cluster {
            router,
            groups,
            disks,
            proposers: None,
        })
    }

    /// Puts a batching proposer in front of every group's leader.
    pub fn with_batching(mut self) -> Self {
        let mut txs = Vec::new();
        for g in 0..self.groups {
            let (tx, mut rx) =
                tokio::sync::mpsc::unbounded_channel::<(Cmd, tokio::sync::oneshot::Sender<bool>)>();
            txs.push(tx);
            let raft = self.leader(g);
            tokio::spawn(async move {
                let slots = Arc::new(tokio::sync::Semaphore::new(2));
                loop {
                    let Ok(permit) = slots.clone().acquire_owned().await else { return };
                    let Some(first) = rx.recv().await else { return };
                    let mut batch = vec![first];
                    while batch.len() < 1024 {
                        match rx.try_recv() {
                            Ok(x) => batch.push(x),
                            Err(_) => break,
                        }
                    }
                    let raft = raft.clone();
                    tokio::spawn(async move {
                        let cmds: Vec<Cmd> = batch.iter().map(|(c, _)| c.clone()).collect();
                        let ok = raft.client_write(encode_batch(&cmds)).await.is_ok();
                        for (_, tx) in batch {
                            let _ = tx.send(ok);
                        }
                        drop(permit);
                    });
                }
            });
        }
        self.proposers = Some(txs);
        self
    }

    pub fn leader(&self, group: u32) -> Raft<TypeConfig> {
        self.router
            .get(group, u64::from(group % 3))
            .expect("raft exists")
    }

    pub async fn write(&self, group: u32, cmd: Cmd) -> Result<()> {
        if let Some(p) = &self.proposers {
            let (tx, rx) = tokio::sync::oneshot::channel();
            p[group as usize]
                .send((cmd, tx))
                .map_err(|_| anyhow!("proposer gone"))?;
            return if rx.await.unwrap_or(false) {
                Ok(())
            } else {
                Err(anyhow!("batched write failed"))
            };
        }
        self.leader(group).client_write(cmd).await?;
        Ok(())
    }

    /// Groups whose leader is not the one placed there (an election happened).
    pub fn misplaced_leaders(&self) -> u32 {
        (0..self.groups)
            .filter(|&g| {
                self.leader(g).metrics().borrow().current_leader != Some(u64::from(g % 3))
            })
            .count() as u32
    }

    pub async fn shutdown(&self) {
        let rafts: Vec<Raft<TypeConfig>> = self
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
