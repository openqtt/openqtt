//! The interest index of R3: an arena trie of MQTT topic filters.
//!
//! - Nodes live in one vector and refer to each other by 32-bit index.
//! - Each level string is interned once; a node stores the id of the level that leads to it.
//! - Literal children are found through one table for the whole trie, keyed by
//!   `(parent, level)`. The table stores only the child's index and compares against the
//!   parent and level the child node already holds, so an edge costs 4 bytes plus control.
//! - `+` and `#` are separate slots on a node, never table entries.
//! - A filter ending at a node has a terminal: its destinations and its shared groups.
//!
//! Matching follows MQTT 5.0 section 4.7: `#` also matches the parent level, `+` matches one
//! level (an empty one too), and a topic starting with `$` is never matched by a filter whose
//! first level is a wildcard [MQTT-4.7.2-1].

use std::hash::BuildHasher;

use foldhash::fast::FixedState;
use hashbrown::HashTable;

use crate::destset::DestSet;

pub const NIL: u32 = u32::MAX;
/// The level of a node reached through a `+` slot.
const PLUS: u32 = u32::MAX - 1;
const ROOT: u32 = 0;

#[derive(Clone, Copy)]
struct Node {
    parent: u32,
    /// Interned level leading here, `PLUS` under a `+` slot, `NIL` for the root or a free node.
    level: u32,
    plus: u32,
    /// Terminal of the filter ending exactly here.
    exact: u32,
    /// Terminal of the filter ending here with `/#`.
    hash: u32,
    /// Literal children; the `+` slot is not counted.
    kids: u32,
}

impl Node {
    const fn new(parent: u32, level: u32) -> Self {
        Node {
            parent,
            level,
            plus: NIL,
            exact: NIL,
            hash: NIL,
            kids: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.kids == 0 && self.plus == NIL && self.exact == NIL && self.hash == NIL
    }
}

/// One shared subscription group on a filter.
pub struct Group {
    pub name: u32,
    pub members: DestSet,
}

/// What hangs off a filter: plain destinations and shared groups.
#[derive(Default)]
pub struct Term {
    pub dests: DestSet,
    #[expect(
        clippy::box_collection,
        reason = "most filters have no group, and None costs 8 bytes in every terminal where a Vec costs 24"
    )]
    pub groups: Option<Box<Vec<Group>>>,
}

impl Term {
    fn is_empty(&self) -> bool {
        self.dests.is_empty() && self.groups.as_ref().is_none_or(|g| g.is_empty())
    }
}

struct Span {
    off: u32,
    len: u32,
    refs: u32,
}

/// Level strings, each stored once in one byte arena, with reference counts so that a level
/// no node uses any more is freed.
pub struct Interner {
    arena: Vec<u8>,
    spans: Vec<Span>,
    table: HashTable<u32>,
    free: Vec<u32>,
    garbage: usize,
    hasher: FixedState,
}

fn span_bytes<'a>(arena: &'a [u8], s: &Span) -> &'a [u8] {
    let off = s.off as usize;
    &arena[off..off + s.len as usize]
}

impl Interner {
    fn new(hasher: FixedState) -> Self {
        Interner {
            arena: Vec::new(),
            spans: Vec::new(),
            table: HashTable::new(),
            free: Vec::new(),
            garbage: 0,
            hasher,
        }
    }

    pub fn get(&self, s: &str) -> Option<u32> {
        let h = self.hasher.hash_one(s.as_bytes());
        let (arena, spans) = (&self.arena, &self.spans);
        self.table
            .find(h, |&id| {
                span_bytes(arena, &spans[id as usize]) == s.as_bytes()
            })
            .copied()
    }

    pub fn resolve(&self, id: u32) -> &str {
        let b = span_bytes(&self.arena, &self.spans[id as usize]);
        std::str::from_utf8(b).unwrap_or("")
    }

    /// The id of `s`, adding a reference.
    fn intern(&mut self, s: &str) -> u32 {
        let h = self.hasher.hash_one(s.as_bytes());
        let Interner {
            arena,
            spans,
            table,
            free,
            hasher,
            ..
        } = self;
        if let Some(&id) = table.find(h, |&id| {
            span_bytes(arena, &spans[id as usize]) == s.as_bytes()
        }) {
            spans[id as usize].refs += 1;
            return id;
        }
        let span = Span {
            off: u32::try_from(arena.len()).expect("level arena under 4 GiB"),
            len: u32::try_from(s.len()).expect("level under 4 GiB"),
            refs: 1,
        };
        arena.extend_from_slice(s.as_bytes());
        let id = match free.pop() {
            Some(id) => {
                spans[id as usize] = span;
                id
            }
            None => {
                spans.push(span);
                u32::try_from(spans.len() - 1).expect("under 2^32 levels")
            }
        };
        let (arena, spans) = (&*arena, &*spans);
        table.insert_unique(h, id, |&x| {
            hasher.hash_one(span_bytes(arena, &spans[x as usize]))
        });
        id
    }

    fn release(&mut self, id: u32) {
        let span = &mut self.spans[id as usize];
        span.refs -= 1;
        if span.refs > 0 {
            return;
        }
        let h = self
            .hasher
            .hash_one(span_bytes(&self.arena, &self.spans[id as usize]));
        if let Ok(e) = self.table.find_entry(h, |&x| x == id) {
            e.remove();
        }
        let span = &mut self.spans[id as usize];
        self.garbage += span.len as usize;
        span.len = 0;
        self.free.push(id);
        if self.garbage > (1 << 20) && self.garbage * 2 > self.arena.len() {
            self.compact();
        }
    }

    /// Copies the live levels into a fresh arena, dropping the bytes of freed ones.
    fn compact(&mut self) {
        let mut arena = Vec::with_capacity(self.arena.len() - self.garbage);
        for s in &mut self.spans {
            if s.refs == 0 {
                s.off = 0;
                continue;
            }
            let off = arena.len();
            arena.extend_from_slice(span_bytes(&self.arena, s));
            s.off = u32::try_from(off).expect("level arena under 4 GiB");
        }
        self.arena = arena;
        self.garbage = 0;
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn shrink(&mut self) {
        self.compact();
        self.arena.shrink_to_fit();
        self.spans.shrink_to_fit();
        self.free.shrink_to_fit();
        let (arena, spans, hasher) = (&self.arena, &self.spans, &self.hasher);
        self.table
            .shrink_to_fit(|&x| hasher.hash_one(span_bytes(arena, &spans[x as usize])));
    }
}

/// Reusable buffers for matching, one per thread.
#[derive(Default)]
pub struct Scratch {
    levels: Vec<u32>,
    stack: Vec<(u32, u32)>,
}

/// Bytes held by each part of a trie, from capacities. The allocator count is the total that
/// counts; this says where it goes.
#[derive(Debug, Default, serde::Serialize)]
pub struct Parts {
    pub nodes: usize,
    pub child_table: usize,
    pub terms: usize,
    pub dest_vectors: usize,
    pub dest_bitmaps: usize,
    pub groups: usize,
    pub level_arena: usize,
    pub level_spans: usize,
    pub level_table: usize,
    pub free_lists: usize,
}

impl Parts {
    pub fn total(&self) -> usize {
        self.nodes
            + self.child_table
            + self.terms
            + self.dest_vectors
            + self.dest_bitmaps
            + self.groups
            + self.level_arena
            + self.level_spans
            + self.level_table
            + self.free_lists
    }
}

/// Where a filter ends: the node and whether it ends in `#`.
#[derive(Clone, Copy)]
struct End {
    node: u32,
    hash: bool,
}

pub struct Trie {
    nodes: Vec<Node>,
    free_nodes: Vec<u32>,
    children: HashTable<u32>,
    terms: Vec<Term>,
    free_terms: Vec<u32>,
    pub interner: Interner,
    hasher: FixedState,
    /// Filters with at least one destination or group.
    filters: usize,
    /// (filter, destination) pairs, members of shared groups included.
    entries: usize,
}

/// Splits `$share/<group>/<filter>` into its group and filter.
pub fn split_shared(filter: &str) -> (Option<&str>, &str) {
    if let Some(rest) = filter.strip_prefix("$share/")
        && let Some((g, f)) = rest.split_once('/')
    {
        return (Some(g), f);
    }
    (None, filter)
}

impl Default for Trie {
    fn default() -> Self {
        Self::new()
    }
}

impl Trie {
    pub fn new() -> Self {
        let hasher = FixedState::with_seed(0x6f70_656e_7174_7432);
        Trie {
            nodes: vec![Node::new(NIL, NIL)],
            free_nodes: Vec::new(),
            children: HashTable::new(),
            terms: Vec::new(),
            free_terms: Vec::new(),
            interner: Interner::new(hasher.clone()),
            hasher,
            filters: 0,
            entries: 0,
        }
    }

    pub fn filters(&self) -> usize {
        self.filters
    }

    pub fn entries(&self) -> usize {
        self.entries
    }

    /// Nodes in use, the root included.
    pub fn nodes(&self) -> usize {
        self.nodes.len() - self.free_nodes.len()
    }

    fn edge_hash(hasher: &FixedState, parent: u32, level: u32) -> u64 {
        hasher.hash_one((u64::from(parent) << 32) | u64::from(level))
    }

    fn child(&self, parent: u32, level: u32) -> Option<u32> {
        let h = Self::edge_hash(&self.hasher, parent, level);
        let nodes = &self.nodes;
        self.children
            .find(h, |&c| {
                let n = &nodes[c as usize];
                n.parent == parent && n.level == level
            })
            .copied()
    }

    fn alloc_node(&mut self, n: Node) -> u32 {
        match self.free_nodes.pop() {
            Some(id) => {
                self.nodes[id as usize] = n;
                id
            }
            None => {
                self.nodes.push(n);
                u32::try_from(self.nodes.len() - 1).expect("under 2^32 nodes")
            }
        }
    }

    fn literal_child_or_create(&mut self, parent: u32, s: &str) -> u32 {
        if let Some(l) = self.interner.get(s)
            && let Some(c) = self.child(parent, l)
        {
            return c;
        }
        let level = self.interner.intern(s);
        let id = self.alloc_node(Node::new(parent, level));
        let h = Self::edge_hash(&self.hasher, parent, level);
        let Trie {
            nodes,
            children,
            hasher,
            ..
        } = self;
        let nodes = &*nodes;
        children.insert_unique(h, id, |&c| {
            let n = &nodes[c as usize];
            Self::edge_hash(hasher, n.parent, n.level)
        });
        self.nodes[parent as usize].kids += 1;
        id
    }

    fn plus_child_or_create(&mut self, parent: u32) -> u32 {
        let p = self.nodes[parent as usize].plus;
        if p != NIL {
            return p;
        }
        let id = self.alloc_node(Node::new(parent, PLUS));
        self.nodes[parent as usize].plus = id;
        id
    }

    /// Walks a filter, creating nodes when `create`; None if a node is missing.
    fn walk(&mut self, filter: &str, create: bool) -> Option<End> {
        let mut n = ROOT;
        for l in filter.split('/') {
            match l {
                "#" => {
                    return Some(End {
                        node: n,
                        hash: true,
                    });
                }
                "+" => {
                    n = if create {
                        self.plus_child_or_create(n)
                    } else {
                        let p = self.nodes[n as usize].plus;
                        if p == NIL {
                            return None;
                        }
                        p
                    };
                }
                _ => {
                    n = if create {
                        self.literal_child_or_create(n, l)
                    } else {
                        self.child(n, self.interner.get(l)?)?
                    };
                }
            }
        }
        Some(End {
            node: n,
            hash: false,
        })
    }

    fn term_slot(&mut self, end: End) -> &mut u32 {
        let node = &mut self.nodes[end.node as usize];
        if end.hash {
            &mut node.hash
        } else {
            &mut node.exact
        }
    }

    /// Adds `dest` to `filter` (or to its shared group); false if it was already there.
    pub fn insert(&mut self, filter: &str, dest: u32) -> bool {
        let (group, f) = split_shared(filter);
        let Some(end) = self.walk(f, true) else {
            return false;
        };
        let mut t = *self.term_slot(end);
        if t == NIL {
            t = match self.free_terms.pop() {
                Some(t) => t,
                None => {
                    self.terms.push(Term::default());
                    u32::try_from(self.terms.len() - 1).expect("under 2^32 terminals")
                }
            };
            *self.term_slot(end) = t;
            self.filters += 1;
        }
        let added = match group {
            None => self.terms[t as usize].dests.insert(dest),
            Some(g) => {
                let gid = self.interner.get(g);
                let term = &mut self.terms[t as usize];
                let groups = term.groups.get_or_insert_with(Default::default);
                match gid.and_then(|gid| groups.iter_mut().find(|x| x.name == gid)) {
                    Some(grp) => grp.members.insert(dest),
                    None => {
                        let name = self.interner.intern(g);
                        let groups = self.terms[t as usize]
                            .groups
                            .get_or_insert_with(Default::default);
                        let mut members = DestSet::default();
                        members.insert(dest);
                        groups.push(Group { name, members });
                        true
                    }
                }
            }
        };
        if added {
            self.entries += 1;
        }
        added
    }

    /// Removes `dest` from `filter`, pruning nodes nothing needs any more.
    pub fn remove(&mut self, filter: &str, dest: u32) -> bool {
        let (group, f) = split_shared(filter);
        let Some(end) = self.walk(f, false) else {
            return false;
        };
        let t = *self.term_slot(end);
        if t == NIL {
            return false;
        }
        let removed = match group {
            None => self.terms[t as usize].dests.remove(dest),
            Some(g) => {
                let Some(gid) = self.interner.get(g) else {
                    return false;
                };
                let term = &mut self.terms[t as usize];
                let Some(groups) = term.groups.as_mut() else {
                    return false;
                };
                let Some(i) = groups.iter().position(|x| x.name == gid) else {
                    return false;
                };
                let gone = groups[i].members.remove(dest);
                if groups[i].members.is_empty() {
                    groups.swap_remove(i);
                    if groups.is_empty() {
                        term.groups = None;
                    }
                    self.interner.release(gid);
                }
                gone
            }
        };
        if !removed {
            return false;
        }
        self.entries -= 1;
        if self.terms[t as usize].is_empty() {
            self.terms[t as usize] = Term::default();
            self.free_terms.push(t);
            *self.term_slot(end) = NIL;
            self.filters -= 1;
            self.prune(end.node);
        }
        true
    }

    fn prune(&mut self, mut n: u32) {
        while n != ROOT && self.nodes[n as usize].is_empty() {
            let Node { parent, level, .. } = self.nodes[n as usize];
            if level == PLUS {
                self.nodes[parent as usize].plus = NIL;
            } else {
                let h = Self::edge_hash(&self.hasher, parent, level);
                if let Ok(e) = self.children.find_entry(h, |&c| c == n) {
                    e.remove();
                }
                self.nodes[parent as usize].kids -= 1;
                self.interner.release(level);
            }
            self.nodes[n as usize] = Node::new(NIL, NIL);
            self.free_nodes.push(n);
            n = parent;
        }
    }

    /// Calls `f` with the terminal of every filter that matches `topic`.
    pub fn matches<'a>(&'a self, topic: &str, s: &mut Scratch, mut f: impl FnMut(&'a Term)) {
        s.levels.clear();
        for l in topic.split('/') {
            s.levels.push(self.interner.get(l).unwrap_or(NIL));
        }
        let dollar = topic.as_bytes().first() == Some(&b'$');
        let depth = s.levels.len();
        s.stack.clear();
        s.stack.push((ROOT, 0));
        while let Some((n, d)) = s.stack.pop() {
            let node = &self.nodes[n as usize];
            let d = d as usize;
            let wild = !(d == 0 && dollar);
            if node.hash != NIL && wild {
                f(&self.terms[node.hash as usize]);
            }
            if d == depth {
                if node.exact != NIL {
                    f(&self.terms[node.exact as usize]);
                }
                continue;
            }
            let next = u32::try_from(d + 1).unwrap_or(u32::MAX);
            if node.plus != NIL && wild {
                s.stack.push((node.plus, next));
            }
            let l = s.levels[d];
            if l != NIL
                && let Some(c) = self.child(n, l)
            {
                s.stack.push((c, next));
            }
        }
    }

    /// The destinations of `topic`, ascending and without duplicates: every plain destination
    /// of every matching filter, plus one member chosen by `pick` from each shared group.
    /// Returns the number of matching filters.
    pub fn collect(&self, topic: &str, pick: u64, s: &mut Scratch, out: &mut Vec<u32>) -> usize {
        out.clear();
        let mut hits = 0;
        // Each destination set and each group's pick is sorted and unique on its own; several
        // of them together are neither, whether they come from one filter or from many.
        let mut sets = 0;
        self.matches(topic, s, |t| {
            hits += 1;
            if !t.dests.is_empty() {
                sets += 1;
                t.dests.extend_into(out);
            }
            if let Some(groups) = &t.groups {
                for g in groups.iter() {
                    if let Some(m) = g.members.pick(pick) {
                        sets += 1;
                        out.push(m);
                    }
                }
            }
        });
        if sets > 1 {
            out.sort_unstable();
            out.dedup();
        }
        hits
    }

    /// Rebuilds the filter string of a node's path, for walking the trie.
    pub fn path(&self, mut n: u32, out: &mut String) {
        let mut levels = Vec::new();
        while n != ROOT {
            let node = &self.nodes[n as usize];
            levels.push(if node.level == PLUS {
                "+"
            } else {
                self.interner.resolve(node.level)
            });
            n = node.parent;
        }
        out.clear();
        for (i, l) in levels.iter().rev().enumerate() {
            if i > 0 {
                out.push('/');
            }
            out.push_str(l);
        }
    }

    /// Every (filter, destination) pair, for snapshots and tests.
    pub fn for_each_entry(&self, mut f: impl FnMut(&str, u32)) {
        let mut path = String::new();
        let mut filter = String::new();
        for (i, node) in self.nodes.iter().enumerate() {
            if node.level == NIL && i != ROOT as usize {
                continue;
            }
            for (slot, hash) in [(node.exact, false), (node.hash, true)] {
                if slot == NIL {
                    continue;
                }
                self.path(u32::try_from(i).unwrap_or(NIL), &mut path);
                let mut v = Vec::new();
                self.terms[slot as usize].dests.extend_into(&mut v);
                filter.clear();
                filter.push_str(&path);
                if hash {
                    if !filter.is_empty() {
                        filter.push('/');
                    }
                    filter.push('#');
                }
                for d in v {
                    f(&filter, d);
                }
            }
        }
    }

    pub fn shrink(&mut self) {
        self.nodes.shrink_to_fit();
        self.free_nodes.shrink_to_fit();
        self.terms.shrink_to_fit();
        self.free_terms.shrink_to_fit();
        let (nodes, hasher) = (&self.nodes, &self.hasher);
        self.children.shrink_to_fit(|&c| {
            let n = &nodes[c as usize];
            Self::edge_hash(hasher, n.parent, n.level)
        });
        self.interner.shrink();
    }

    pub fn parts(&self) -> Parts {
        let mut p = Parts {
            nodes: self.nodes.capacity() * size_of::<Node>(),
            child_table: self.children.allocation_size(),
            terms: self.terms.capacity() * size_of::<Term>(),
            level_arena: self.interner.arena.capacity(),
            level_spans: self.interner.spans.capacity() * size_of::<Span>(),
            level_table: self.interner.table.allocation_size(),
            free_lists: (self.free_nodes.capacity()
                + self.free_terms.capacity()
                + self.interner.free.capacity())
                * 4,
            ..Parts::default()
        };
        for t in &self.terms {
            add_set(&mut p, &t.dests);
            if let Some(g) = &t.groups {
                p.groups += size_of::<Vec<Group>>() + g.capacity() * size_of::<Group>();
                for x in g.iter() {
                    add_set(&mut p, &x.members);
                }
            }
        }
        p
    }

    /// Literal children of each node in use, with its depth, for the coarsening analysis.
    pub fn fanout_histogram(&self) -> Vec<(u32, u32)> {
        let mut out = Vec::new();
        for (i, n) in self.nodes.iter().enumerate() {
            if n.level == NIL && i != ROOT as usize {
                continue;
            }
            if n.kids > 0 {
                let mut d = 0;
                let mut x = u32::try_from(i).unwrap_or(ROOT);
                while x != ROOT {
                    d += 1;
                    x = self.nodes[x as usize].parent;
                }
                out.push((d, n.kids));
            }
        }
        out
    }
}

fn add_set(p: &mut Parts, s: &DestSet) {
    match s {
        DestSet::Inline { .. } => {}
        DestSet::Small(v) => p.dest_vectors += v.capacity() * 4,
        DestSet::Big(b) => {
            // A roaring bitmap is a vector of containers; serialized size is a close lower
            // bound for what the containers hold.
            p.dest_bitmaps += size_of::<roaring::RoaringBitmap>() + b.serialized_size();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hits(t: &Trie, topic: &str) -> Vec<u32> {
        let mut s = Scratch::default();
        let mut out = Vec::new();
        t.collect(topic, 0, &mut s, &mut out);
        out
    }

    #[test]
    fn matches_like_the_specification() {
        let mut t = Trie::new();
        let filters = [
            "sport/tennis/player1/#",
            "sport/tennis/+",
            "sport/#",
            "#",
            "+/+",
            "/+",
            "+",
            "$SYS/#",
            "$SYS/monitor/+",
            "+/monitor/Clients",
            "sport/tennis/player1",
        ];
        for (i, f) in filters.iter().enumerate() {
            assert!(t.insert(f, u32::try_from(i).unwrap()));
        }
        // sport/tennis/player1/# matches the parent level too.
        assert_eq!(hits(&t, "sport/tennis/player1"), vec![0, 1, 2, 3, 10]);
        assert_eq!(hits(&t, "sport/tennis/player1/ranking"), vec![0, 2, 3]);
        assert_eq!(hits(&t, "sport"), vec![2, 3, 6]);
        assert_eq!(hits(&t, "sport/"), vec![2, 3, 4]);
        assert_eq!(hits(&t, "/finance"), vec![3, 4, 5]);
        // Wildcards at the first level never match a $ topic.
        assert_eq!(hits(&t, "$SYS/monitor/Clients"), vec![7, 8]);
        assert_eq!(hits(&t, "$SYS"), vec![7]);
    }

    #[test]
    fn removal_prunes_everything_back_to_the_root() {
        let mut t = Trie::new();
        let filters = [
            "a/b/c",
            "a/+/c/#",
            "a/b",
            "$share/g/a/b/#",
            "$share/h/a/b/#",
            "#",
        ];
        for f in filters {
            assert!(t.insert(f, 7));
            assert!(t.insert(f, 9));
        }
        assert_eq!(t.entries(), 12);
        for f in filters {
            assert!(t.remove(f, 7));
            assert!(!t.remove(f, 7));
            assert!(t.remove(f, 9));
        }
        assert_eq!(t.entries(), 0);
        assert_eq!(t.filters(), 0);
        assert_eq!(t.nodes(), 1);
        assert_eq!(t.interner.len(), 0);
    }

    #[test]
    fn shared_groups_deliver_to_one_member_each() {
        let mut t = Trie::new();
        for m in 0..10 {
            t.insert("$share/g/ingest/+/+/+/telemetry", 100 + m);
        }
        t.insert("ingest/acme/production/pump-3/telemetry", 1);
        let got = hits(&t, "ingest/acme/production/pump-3/telemetry");
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], 1);
        assert!((100..110).contains(&got[1]));
    }

    #[test]
    fn destinations_from_one_filter_are_sorted_and_unique() {
        let mut t = Trie::new();
        t.insert("$share/g/a", 7);
        t.insert("$share/h/a", 7);
        assert_eq!(hits(&t, "a"), vec![7]);
        t.insert("a", 9);
        t.insert("$share/k/a", 3);
        assert_eq!(hits(&t, "a"), vec![3, 7, 9]);
    }

    #[test]
    fn entries_round_trip() {
        let mut t = Trie::new();
        t.insert("a/+/c/#", 3);
        t.insert("#", 4);
        t.insert("a", 5);
        let mut seen = Vec::new();
        t.for_each_entry(|f, d| seen.push((f.to_string(), d)));
        seen.sort();
        assert_eq!(
            seen,
            vec![
                ("#".to_string(), 4),
                ("a".to_string(), 5),
                ("a/+/c/#".to_string(), 3)
            ]
        );
    }
}
