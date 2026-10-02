//! The simpler alternative: primary-backup per partition, fenced by an epoch the meta group
//! hands out.
//!
//! The primary of a partition appends an entry to its own disk and sends it to both backups;
//! the entry commits when the primary's copy is durable and one backup has acknowledged its own
//! durable copy (two of three, the same quorum as Raft with three replicas). Every message
//! carries the partition's epoch and a backup refuses an older one, so a deposed primary cannot
//! commit. There are no per-partition timers: liveness is the node's lease with the meta group,
//! one renewal per node per second whatever the number of partitions. Messages between two nodes
//! are coalesced, all partitions together, the way a per-peer lane would carry them.
//!
//! What is not here, and is the price of the scheme: choosing a new primary. The meta group must
//! fence the old primary's epoch, ask both backups for their last durable index, and promote the
//! one holding every committed entry. That protocol is what Raft's election already is.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::net::{DelayLine, Disk};
use super::raft::Cmd;

enum Msg {
    Append(Vec<(u32, u64, u64, Cmd)>),
    Ack(Vec<(u32, u64)>),
    Lease,
}

#[derive(Default)]
struct Group {
    epoch: u64,
    /// Primary side.
    next: u64,
    durable: u64,
    acked: [u64; 3],
    committed: u64,
    waiters: VecDeque<(u64, Cmd, oneshot::Sender<u64>)>,
    /// Backup side: what is durable here, as a contiguous prefix plus out-of-order extras.
    upto: u64,
    extra: BTreeSet<u64>,
    state: HashMap<Vec<u8>, Vec<u8>>,
    refused: u64,
}

pub struct Node {
    id: usize,
    groups: Vec<Mutex<Group>>,
    disk: Disk,
    out: OnceLock<Vec<Option<mpsc::UnboundedSender<Msg>>>>,
    pub messages: AtomicU64,
}

fn primary(g: u32) -> usize {
    (g % 3) as usize
}

impl Node {
    fn send(&self, to: usize, m: Msg) {
        if let Some(Some(tx)) = self.out.get().map(|o| &o[to]) {
            let _ = tx.send(m);
        }
    }

    fn backups(g: u32) -> [usize; 2] {
        let p = primary(g);
        [(p + 1) % 3, (p + 2) % 3]
    }

    /// Commits whatever is durable here and acknowledged by one backup, and answers its writers.
    fn try_commit(&self, grp: &mut Group, me: usize) {
        let backups_max = (0..3)
            .filter(|&b| b != me)
            .map(|b| grp.acked[b])
            .max()
            .unwrap_or(0);
        let commit = grp.durable.min(backups_max);
        if commit <= grp.committed {
            return;
        }
        grp.committed = commit;
        while grp.waiters.front().is_some_and(|(i, ..)| *i <= commit) {
            let (i, cmd, tx) = grp.waiters.pop_front().expect("peeked");
            grp.state.insert(cmd.cid, cmd.val);
            let _ = tx.send(i);
        }
    }

    fn handle(self: &Arc<Self>, from: usize, m: Msg) {
        self.messages.fetch_add(1, Ordering::Relaxed);
        match m {
            Msg::Append(entries) => {
                let mut accepted = Vec::with_capacity(entries.len());
                for (g, epoch, idx, _cmd) in entries {
                    let mut grp = self.groups[g as usize].lock().expect("not poisoned");
                    if epoch < grp.epoch {
                        grp.refused += 1;
                        continue;
                    }
                    grp.epoch = epoch;
                    accepted.push((g, idx));
                }
                // One durable write for the whole message, then one acknowledgement message.
                let me = self.clone();
                self.disk.submit(move || {
                    let mut acks: HashMap<u32, u64> = HashMap::new();
                    for (g, idx) in accepted {
                        let mut grp = me.groups[g as usize].lock().expect("not poisoned");
                        grp.extra.insert(idx);
                        while grp.extra.first() == Some(&(grp.upto + 1)) {
                            grp.extra.pop_first();
                            grp.upto += 1;
                        }
                        acks.insert(g, grp.upto);
                    }
                    me.send(from, Msg::Ack(acks.into_iter().collect()));
                });
            }
            Msg::Ack(acks) => {
                for (g, upto) in acks {
                    let mut grp = self.groups[g as usize].lock().expect("not poisoned");
                    if upto > grp.acked[from] {
                        grp.acked[from] = upto;
                        self.try_commit(&mut grp, self.id);
                    }
                }
            }
            Msg::Lease => {}
        }
    }

    /// A write on the primary of group `g`; resolves when committed and applied.
    pub fn write(self: &Arc<Self>, g: u32, cmd: Cmd) -> oneshot::Receiver<u64> {
        let (tx, rx) = oneshot::channel();
        let (idx, epoch) = {
            let mut grp = self.groups[g as usize].lock().expect("not poisoned");
            grp.next += 1;
            let idx = grp.next;
            grp.waiters.push_back((idx, cmd.clone(), tx));
            (idx, grp.epoch)
        };
        let me = self.clone();
        self.disk.submit(move || {
            let mut grp = me.groups[g as usize].lock().expect("not poisoned");
            // The disk completes in submission order, so this is a contiguous prefix.
            grp.durable = grp.durable.max(idx);
            me.try_commit(&mut grp, me.id);
        });
        for b in Self::backups(g) {
            self.send(b, Msg::Append(vec![(g, epoch, idx, cmd.clone())]));
        }
        rx
    }
}

pub struct Cluster {
    pub nodes: Vec<Arc<Node>>,
    pub disks: Vec<Disk>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl Cluster {
    pub fn start(groups: u32, line: DelayLine, delay: Duration, flush: Duration) -> Self {
        let disks: Vec<Disk> = (0..3).map(|_| Disk::new(line.clone(), flush)).collect();
        let nodes: Vec<Arc<Node>> = (0..3)
            .map(|id| {
                Arc::new(Node {
                    id,
                    groups: (0..groups)
                        .map(|_| {
                            Mutex::new(Group {
                                epoch: 1,
                                ..Group::default()
                            })
                        })
                        .collect(),
                    disk: disks[id].clone(),
                    out: OnceLock::new(),
                    messages: AtomicU64::new(0),
                })
            })
            .collect();
        let mut tasks = Vec::new();
        // One lane per ordered pair of nodes. A lane takes everything queued for its peer and
        // delivers it as one message after the one-way delay; messages in flight overlap.
        for a in 0..3 {
            let mut out = Vec::new();
            for b in 0..3 {
                if a == b {
                    out.push(None);
                    continue;
                }
                let (tx, mut rx) = mpsc::unbounded_channel::<Msg>();
                out.push(Some(tx));
                let to = nodes[b].clone();
                let line = line.clone();
                tasks.push(tokio::spawn(async move {
                    while let Some(first) = rx.recv().await {
                        let mut appends = Vec::new();
                        let mut acks = Vec::new();
                        let mut lease = false;
                        let mut take = |m: Msg| match m {
                            Msg::Append(mut e) => appends.append(&mut e),
                            Msg::Ack(mut k) => acks.append(&mut k),
                            Msg::Lease => lease = true,
                        };
                        take(first);
                        while let Ok(m) = rx.try_recv() {
                            take(m);
                        }
                        let sleep = line.sleep(delay);
                        let to = to.clone();
                        tokio::spawn(async move {
                            sleep.await;
                            if !appends.is_empty() {
                                to.handle(a, Msg::Append(appends));
                            }
                            if !acks.is_empty() {
                                to.handle(a, Msg::Ack(acks));
                            }
                            if lease {
                                to.handle(a, Msg::Lease);
                            }
                        });
                    }
                }));
            }
            let _ = nodes[a].out.set(out);
        }
        // Node leases with the meta group (node 0 here): one renewal a second per node.
        for n in 1..3 {
            let node = nodes[n].clone();
            tasks.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tick.tick().await;
                    node.send(0, Msg::Lease);
                }
            }));
        }
        Cluster {
            nodes,
            disks,
            tasks: Mutex::new(tasks),
        }
    }

    /// Stops the lanes and leases; the nodes hold each other through their lanes otherwise.
    pub fn shutdown(&self) {
        for t in self.tasks.lock().expect("not poisoned").drain(..) {
            t.abort();
        }
    }

    pub async fn write(&self, g: u32, cmd: Cmd) -> anyhow::Result<()> {
        self.nodes[primary(g)].write(g, cmd).await?;
        Ok(())
    }

    pub fn messages(&self) -> u64 {
        self.nodes.iter().map(|n| n.messages.load(Ordering::Relaxed)).sum()
    }

    pub fn refused(&self) -> u64 {
        self.nodes
            .iter()
            .flat_map(|n| n.groups.iter())
            .map(|g| g.lock().expect("not poisoned").refused)
            .sum()
    }
}
