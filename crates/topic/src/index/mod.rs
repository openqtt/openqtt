//! The topic index of reports R3 and R6: an arena trie of topic filters and the destinations
//! that want each one.
//!
//! - Nodes live in one vector and refer to each other by 32-bit index.
//! - Each level string is interned once; a node stores the id of the level leading to it.
//! - Literal children are found through one hash table for the whole trie, keyed by parent and
//!   level. The table stores only the child's index and compares against the parent and level
//!   the child node already holds, so an edge of the trie costs four bytes and a control byte.
//! - `+` and `#` are slots on a node, never table entries.
//! - A filter ending at a node has a terminal: its destinations, and its shared groups with
//!   their members.
//!
//! Spike S2 measured this design at 138 to 150 bytes per filter once compacted, and a match at
//! 10^6 filters in 0.75 to 1.9 µs at the median and 4 to 7 µs at p99 (R6, D1). This version
//! takes one of the savings R6 left to measure: a terminal of 24 bytes rather than 40, its
//! destination set holding three ids inline in 16 bytes.

mod destset;
mod interner;

use std::fmt;
use std::hash::BuildHasher;
use std::marker::PhantomData;
use std::mem::size_of;

use foldhash::fast::RandomState;
use hashbrown::HashTable;
use roaring::RoaringBitmap;

use self::destset::DestSet;
use self::interner::Interner;
use crate::filter::SHARE_PREFIX;
use crate::{Error, TopicFilter, TopicName};

/// No node, terminal or level.
const NIL: u32 = u32::MAX;
/// The level of a node reached through a `+` slot.
const PLUS: u32 = u32::MAX - 1;
/// The largest id the index hands out to a node, a terminal or a level.
const MAX_ID: u32 = u32::MAX - 2;
const ROOT: u32 = 0;
/// When several sets contribute more destinations than this in all, they are merged by a
/// bitmap union rather than by sorting, which R6 measured as faster from about 1,000 (D1).
const UNION_ABOVE: usize = 1_000;

/// A destination the index can hold: anything with a 32-bit id.
///
/// Destination sets hold ids, so that a large one can be a roaring bitmap (R3). A caller with
/// richer destinations, such as an edge or a log partition in a route view, numbers them. Equal
/// ids are the same destination, and destinations come back in ascending order of id.
pub trait Destination: Copy + 'static {
    /// The id the index stores.
    fn to_id(self) -> u32;

    /// The destination stored as `id`. The index passes only ids it was given.
    fn from_id(id: u32) -> Self;
}

impl Destination for u32 {
    fn to_id(self) -> u32 {
        self
    }

    fn from_id(id: u32) -> Self {
        id
    }
}

#[derive(Clone, Copy, Debug)]
struct Node {
    parent: u32,
    /// The interned level leading here, [`PLUS`] under a `+` slot, [`NIL`] for the root and
    /// for a free node.
    level: u32,
    plus: u32,
    /// The terminal of the filter ending exactly here.
    exact: u32,
    /// The terminal of the filter ending here with `/#`, or of `#` itself at the root.
    hash: u32,
    /// Literal children; the `+` slot is not counted.
    kids: u32,
}

impl Node {
    const FREE: Self = Self::new(NIL, NIL);

    const fn new(parent: u32, level: u32) -> Self {
        Self {
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

/// What hangs off a filter. It is never empty while a node points at it.
#[derive(Clone, Debug, Default)]
struct Term {
    dests: DestSet,
    #[expect(
        clippy::box_collection,
        reason = "most filters have no shared group, and None costs 8 bytes where a Vec costs 24"
    )]
    groups: Option<Box<Vec<Group>>>,
}

impl Term {
    fn is_empty(&self) -> bool {
        self.dests.is_empty() && self.groups.is_none()
    }

    fn group(&self, name: u32) -> Option<&Group> {
        self.groups.as_ref()?.iter().find(|g| g.name == name)
    }
}

/// One shared group on a filter. It is never empty: the last member out removes it.
#[derive(Clone, Debug)]
struct Group {
    /// The interned ShareName.
    name: u32,
    members: DestSet,
}

/// Where a filter ends: the node, and whether it ends there with `/#`.
#[derive(Clone, Copy)]
struct End {
    node: u32,
    hash: bool,
}

fn edge_hash(state: &RandomState, parent: u32, level: u32) -> u64 {
    state.hash_one((u64::from(parent) << 32) | u64::from(level))
}

/// An index from topic filters to destinations: which destinations want a topic.
///
/// A filter is any [`TopicFilter`]. A plain filter holds a set of destinations; a shared
/// subscription holds, on its pattern, one group per ShareName, and a match delivers to one
/// member of each group. Matching follows section 4.7, including that a filter starting with a
/// wildcard does not match a name starting with `$` ([MQTT-4.7.2-1]).
///
/// Destinations are ids ([`Destination`]). A set of them is a few inline ids, a sorted vector
/// from 4, and a roaring bitmap above 64 (R3), back to a vector at 32. When more than one set
/// contributes to a match, the result is deduplicated, since two filters, or two groups, can
/// name the same destination (R6, section 4).
///
/// One writer at a time; any number of readers between writes, each with its own [`Scratch`].
#[derive(Clone)]
pub struct TopicIndex<V> {
    nodes: Vec<Node>,
    free_nodes: Vec<u32>,
    /// Literal children, by parent and level.
    children: HashTable<u32>,
    terms: Vec<Term>,
    free_terms: Vec<u32>,
    levels: Interner,
    state: RandomState,
    /// Filters with at least one destination or group member.
    filters: usize,
    /// (filter, destination) pairs, members of shared groups included.
    entries: usize,
    destination: PhantomData<fn(V) -> V>,
}

impl<V: Destination> Default for TopicIndex<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> fmt::Debug for TopicIndex<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TopicIndex")
            .field("filters", &self.filters)
            .field("entries", &self.entries)
            .field("nodes", &(self.nodes.len() - self.free_nodes.len()))
            .field("levels", &self.levels.len())
            .finish_non_exhaustive()
    }
}

impl<V: Destination> TopicIndex<V> {
    /// An empty index, its hash tables seeded at random.
    pub fn new() -> Self {
        let state = RandomState::default();
        Self {
            nodes: vec![Node::new(NIL, NIL)],
            free_nodes: Vec::new(),
            children: HashTable::new(),
            terms: Vec::new(),
            free_terms: Vec::new(),
            levels: Interner::new(state.clone()),
            state,
            filters: 0,
            entries: 0,
            destination: PhantomData,
        }
    }

    /// Filters with at least one destination or shared group member. A shared subscription
    /// counts as its pattern: `$share/a/t`, `$share/b/t` and `t` are one filter.
    pub fn filters(&self) -> usize {
        self.filters
    }

    /// (filter, destination) pairs, members of shared groups included.
    pub fn entries(&self) -> usize {
        self.entries
    }

    /// Whether the index holds nothing.
    pub fn is_empty(&self) -> bool {
        self.entries == 0
    }

    /// Nodes in use, the root included.
    pub fn nodes(&self) -> usize {
        self.nodes.len() - self.free_nodes.len()
    }

    /// Distinct level strings held, ShareNames included.
    pub fn levels(&self) -> usize {
        self.levels.len()
    }

    /// Adds `dest` to `filter`, or to its shared group; `Ok(false)` if it was there already.
    ///
    /// # Errors
    ///
    /// [`Error::IndexFull`] when the index has run out of ids or of room for level text, and
    /// then it is as it was.
    pub fn insert(&mut self, filter: &TopicFilter, dest: V) -> Result<bool, Error> {
        let end = self.create_path(filter.pattern())?;
        let mut term = self.slot(end);
        if term == NIL {
            term = match self.alloc_term() {
                Ok(term) => term,
                Err(error) => {
                    self.prune(end.node);
                    return Err(error);
                }
            };
            self.set_slot(end, term);
            self.filters += 1;
        }
        let id = dest.to_id();
        let added = match filter.share_name() {
            None => self.terms[term as usize].dests.insert(id),
            Some(share_name) => match self.insert_member(term, share_name, id) {
                Ok(added) => added,
                Err(error) => {
                    self.free_if_empty(end, term);
                    return Err(error);
                }
            },
        };
        if added {
            self.entries += 1;
        }
        Ok(added)
    }

    /// Removes `dest` from `filter`, or from its shared group, and every node nothing needs any
    /// more; false if it was not there.
    pub fn remove(&mut self, filter: &TopicFilter, dest: V) -> bool {
        let Some(end) = self.find_path(filter.pattern()) else {
            return false;
        };
        let term = self.slot(end);
        if term == NIL {
            return false;
        }
        let id = dest.to_id();
        let removed = match filter.share_name() {
            None => self.terms[term as usize].dests.remove(id),
            Some(share_name) => self.remove_member(term, share_name, id),
        };
        if !removed {
            return false;
        }
        self.entries -= 1;
        self.free_if_empty(end, term);
        true
    }

    /// Whether `filter`, or its shared group, holds `dest`.
    pub fn contains(&self, filter: &TopicFilter, dest: V) -> bool {
        let Some(term) = self
            .find_path(filter.pattern())
            .map(|end| self.slot(end))
            .filter(|&term| term != NIL)
        else {
            return false;
        };
        let term = &self.terms[term as usize];
        let id = dest.to_id();
        match filter.share_name() {
            None => term.dests.contains(id),
            Some(share_name) => self
                .levels
                .get(share_name)
                .and_then(|name| term.group(name))
                .is_some_and(|group| group.members.contains(id)),
        }
    }

    /// Puts into `out` every destination that wants `name`, ascending and each once: every
    /// destination of every matching filter, and one member of each matching shared group,
    /// chosen by `pick`. Returns how many filters matched.
    ///
    /// `pick` is called once for each matching group, and returns the index of the member to
    /// deliver to, in ascending order of id; it is taken modulo the group's size. Round robin
    /// (report R1, O1) is a counter the caller keeps.
    ///
    /// `out` is cleared first, and both it and `scratch` keep their memory between calls, so a
    /// match allocates nothing once they have grown.
    pub fn collect(
        &self,
        name: &TopicName,
        scratch: &mut Scratch,
        mut pick: impl FnMut(&SharedGroup<'_, V>) -> usize,
        out: &mut Vec<V>,
    ) -> usize {
        out.clear();
        let Scratch {
            levels,
            stack,
            terms,
            union,
        } = scratch;
        terms.clear();
        self.walk(name.as_str(), levels, stack, |term| terms.push(term));

        // Each set is sorted and unique on its own; two or more together are neither.
        let mut sets = 0;
        let mut total = 0;
        for &term in terms.iter() {
            let term = &self.terms[term as usize];
            if !term.dests.is_empty() {
                sets += 1;
                total += term.dests.len();
            }
            if let Some(groups) = &term.groups {
                sets += groups.len();
                total += groups.len();
            }
        }
        if sets > 1 && total > UNION_ABOVE {
            union.clear();
            for &term in terms.iter() {
                let term = &self.terms[term as usize];
                match term.dests.as_bitmap() {
                    Some(bitmap) => *union |= bitmap,
                    None => union.extend(term.dests.iter()),
                }
                for group in term.groups.iter().flat_map(|groups| groups.iter()) {
                    if let Some(member) = self.pick_member(group, &mut pick) {
                        union.insert(member);
                    }
                }
            }
            out.extend(union.iter().map(V::from_id));
        } else {
            for &term in terms.iter() {
                let term = &self.terms[term as usize];
                out.extend(term.dests.iter().map(V::from_id));
                for group in term.groups.iter().flat_map(|groups| groups.iter()) {
                    if let Some(member) = self.pick_member(group, &mut pick) {
                        out.push(V::from_id(member));
                    }
                }
            }
            if sets > 1 {
                out.sort_unstable_by_key(|dest| dest.to_id());
                out.dedup_by_key(|dest| dest.to_id());
            }
        }
        terms.len()
    }

    /// Calls `found` with every filter that matches `name`, in no particular order, and
    /// returns how many there were. Nothing is deduplicated: this is for a caller that needs
    /// more than [`collect`](Self::collect) gives, such as counts per filter.
    pub fn for_each_match(
        &self,
        name: &TopicName,
        scratch: &mut Scratch,
        mut found: impl FnMut(Matched<'_, V>),
    ) -> usize {
        let mut matched = 0;
        self.walk(
            name.as_str(),
            &mut scratch.levels,
            &mut scratch.stack,
            |term| {
                matched += 1;
                found(Matched {
                    term: &self.terms[term as usize],
                    levels: &self.levels,
                    destination: PhantomData,
                });
            },
        );
        matched
    }

    /// Calls `entry` with every (filter, destination) pair the index holds, members of shared
    /// groups as their `$share/{ShareName}/` filter, in no particular order. For snapshots
    /// and tests: it rebuilds each filter's text.
    pub fn for_each_entry(&self, mut entry: impl FnMut(&TopicFilter, V)) {
        let mut pattern = String::new();
        let mut text = String::new();
        for (node, n) in self.nodes.iter().enumerate() {
            if node != ROOT as usize && n.level == NIL {
                continue;
            }
            for (term, hash) in [(n.exact, false), (n.hash, true)] {
                if term == NIL {
                    continue;
                }
                let node = u32::try_from(node).unwrap_or(NIL);
                self.pattern_of(End { node, hash }, &mut pattern);
                let term = &self.terms[term as usize];
                if !term.dests.is_empty() {
                    let filter = TopicFilter::from_checked(&pattern, 0);
                    for id in term.dests.iter() {
                        entry(&filter, V::from_id(id));
                    }
                }
                for group in term.groups.iter().flat_map(|groups| groups.iter()) {
                    text.clear();
                    text.push_str(SHARE_PREFIX);
                    text.push_str(self.levels.resolve(group.name));
                    text.push('/');
                    let start = text.len();
                    text.push_str(&pattern);
                    let filter = TopicFilter::from_checked(&text, start);
                    for id in group.members.iter() {
                        entry(&filter, V::from_id(id));
                    }
                }
            }
        }
    }

    /// Gives back the memory that removals left behind: renumbers nodes, terminals and levels
    /// to drop the free slots between them, packs the level text, and shrinks every vector and
    /// table to fit. It visits the whole index once, so it is for after a large removal, not
    /// after every change.
    pub fn compact(&mut self) {
        let level_map = self.levels.renumber();

        let mut term_map = vec![NIL; self.terms.len()];
        let mut terms = Vec::with_capacity(self.terms.len() - self.free_terms.len());
        for (old, mut term) in std::mem::take(&mut self.terms).into_iter().enumerate() {
            if term.is_empty() {
                continue;
            }
            for group in term.groups.iter_mut().flat_map(|groups| groups.iter_mut()) {
                group.name = level_map[group.name as usize];
                if let DestSet::Small(ids) = &mut group.members {
                    ids.shrink_to_fit();
                }
            }
            if let Some(groups) = &mut term.groups {
                groups.shrink_to_fit();
            }
            if let DestSet::Small(ids) = &mut term.dests {
                ids.shrink_to_fit();
            }
            // Fewer than there were, so they fit.
            term_map[old] = u32::try_from(terms.len()).unwrap_or(NIL);
            terms.push(term);
        }

        let mut node_map = vec![NIL; self.nodes.len()];
        let mut live = 0u32;
        for (old, node) in self.nodes.iter().enumerate() {
            if old == ROOT as usize || node.level != NIL {
                node_map[old] = live;
                live += 1;
            }
        }
        let map = |map: &[u32], id: u32| if id == NIL { NIL } else { map[id as usize] };
        let nodes: Vec<Node> = self
            .nodes
            .iter()
            .enumerate()
            .filter(|&(old, node)| old == ROOT as usize || node.level != NIL)
            .map(|(_, node)| Node {
                parent: map(&node_map, node.parent),
                level: match node.level {
                    PLUS | NIL => node.level,
                    level => level_map[level as usize],
                },
                plus: map(&node_map, node.plus),
                exact: map(&term_map, node.exact),
                hash: map(&term_map, node.hash),
                kids: node.kids,
            })
            .collect();

        let mut children = HashTable::with_capacity(nodes.len().saturating_sub(1));
        for (id, node) in nodes.iter().enumerate().skip(1) {
            if node.level == PLUS {
                continue;
            }
            let id = u32::try_from(id).unwrap_or(NIL);
            children.insert_unique(edge_hash(&self.state, node.parent, node.level), id, |&c| {
                let n = &nodes[c as usize];
                edge_hash(&self.state, n.parent, n.level)
            });
        }

        self.nodes = nodes;
        self.terms = terms;
        self.children = children;
        self.free_nodes = Vec::new();
        self.free_terms = Vec::new();
    }

    /// The heap bytes the index holds, by part. It visits every terminal, so it is for
    /// metrics taken now and then, not for every change.
    pub fn memory(&self) -> Memory {
        let (level_text, level_spans, level_table, level_free) = self.levels.memory();
        let mut memory = Memory {
            nodes: self.nodes.capacity() * size_of::<Node>(),
            children: self.children.allocation_size(),
            terminals: self.terms.capacity() * size_of::<Term>(),
            destinations: 0,
            groups: 0,
            level_text,
            level_spans,
            level_table,
            free_lists: (self.free_nodes.capacity() + self.free_terms.capacity())
                * size_of::<u32>()
                + level_free,
        };
        for term in &self.terms {
            memory.destinations += term.dests.heap_bytes();
            if let Some(groups) = &term.groups {
                memory.groups += size_of::<Vec<Group>>() + groups.capacity() * size_of::<Group>();
                for group in groups.iter() {
                    memory.destinations += group.members.heap_bytes();
                }
            }
        }
        memory
    }

    // Matching.

    /// Calls `found` with the terminal of every filter that matches `name`. Each node is
    /// reached by one path, so each terminal comes once.
    fn walk(
        &self,
        name: &str,
        levels: &mut Vec<u32>,
        stack: &mut Vec<(u32, usize)>,
        mut found: impl FnMut(u32),
    ) {
        levels.clear();
        levels.extend(
            name.split('/')
                .map(|level| self.levels.get(level).unwrap_or(NIL)),
        );
        let depth = levels.len();
        let dollar = name.starts_with('$');
        stack.clear();
        stack.push((ROOT, 0));
        while let Some((n, d)) = stack.pop() {
            let node = &self.nodes[n as usize];
            // A wildcard at the first level does not match a name starting with `$`
            // [MQTT-4.7.2-1].
            let wild = d > 0 || !dollar;
            // `#` matches the parent level too, so its terminal fires whatever is left.
            if wild && node.hash != NIL {
                found(node.hash);
            }
            if d == depth {
                if node.exact != NIL {
                    found(node.exact);
                }
                continue;
            }
            if wild && node.plus != NIL {
                stack.push((node.plus, d + 1));
            }
            let level = levels[d];
            if level != NIL
                && let Some(child) = self.child(n, level)
            {
                stack.push((child, d + 1));
            }
        }
    }

    fn pick_member(
        &self,
        group: &Group,
        pick: &mut impl FnMut(&SharedGroup<'_, V>) -> usize,
    ) -> Option<u32> {
        let len = group.members.len();
        if len == 0 {
            return None;
        }
        let index = pick(&SharedGroup {
            name: self.levels.resolve(group.name),
            members: &group.members,
            destination: PhantomData,
        });
        group.members.get(index % len)
    }

    // Paths.

    fn child(&self, parent: u32, level: u32) -> Option<u32> {
        let nodes = &self.nodes;
        self.children
            .find(edge_hash(&self.state, parent, level), |&c| {
                let n = &nodes[c as usize];
                n.parent == parent && n.level == level
            })
            .copied()
    }

    /// The end of a checked pattern, if its nodes exist.
    fn find_path(&self, pattern: &str) -> Option<End> {
        let mut node = ROOT;
        for level in pattern.split('/') {
            node = match level {
                "#" => return Some(End { node, hash: true }),
                "+" => Some(self.nodes[node as usize].plus).filter(|&plus| plus != NIL)?,
                _ => self.child(node, self.levels.get(level)?)?,
            };
        }
        Some(End { node, hash: false })
    }

    /// The end of a checked pattern, creating the nodes it needs. On an error, what it created
    /// is pruned again.
    fn create_path(&mut self, pattern: &str) -> Result<End, Error> {
        let mut node = ROOT;
        for level in pattern.split('/') {
            let next = match level {
                "#" => return Ok(End { node, hash: true }),
                "+" => self.plus_child(node),
                _ => self.literal_child(node, level),
            };
            node = match next {
                Ok(next) => next,
                Err(error) => {
                    self.prune(node);
                    return Err(error);
                }
            };
        }
        Ok(End { node, hash: false })
    }

    fn plus_child(&mut self, parent: u32) -> Result<u32, Error> {
        let plus = self.nodes[parent as usize].plus;
        if plus != NIL {
            return Ok(plus);
        }
        let plus = self.alloc_node(Node::new(parent, PLUS))?;
        self.nodes[parent as usize].plus = plus;
        Ok(plus)
    }

    fn literal_child(&mut self, parent: u32, text: &str) -> Result<u32, Error> {
        if let Some(level) = self.levels.get(text)
            && let Some(child) = self.child(parent, level)
        {
            return Ok(child);
        }
        let level = self.levels.intern(text)?;
        let child = match self.alloc_node(Node::new(parent, level)) {
            Ok(child) => child,
            Err(error) => {
                self.levels.release(level);
                return Err(error);
            }
        };
        let (nodes, state) = (&self.nodes, &self.state);
        self.children
            .insert_unique(edge_hash(state, parent, level), child, |&c| {
                let n = &nodes[c as usize];
                edge_hash(state, n.parent, n.level)
            });
        self.nodes[parent as usize].kids += 1;
        Ok(child)
    }

    fn alloc_node(&mut self, node: Node) -> Result<u32, Error> {
        if let Some(id) = self.free_nodes.pop() {
            self.nodes[id as usize] = node;
            return Ok(id);
        }
        let id = next_id(self.nodes.len())?;
        self.nodes.push(node);
        Ok(id)
    }

    /// Frees nodes from `node` up while nothing needs them.
    fn prune(&mut self, mut node: u32) {
        while node != ROOT && self.nodes[node as usize].is_empty() {
            let Node { parent, level, .. } = self.nodes[node as usize];
            if level == PLUS {
                self.nodes[parent as usize].plus = NIL;
            } else {
                let hash = edge_hash(&self.state, parent, level);
                if let Ok(entry) = self.children.find_entry(hash, |&c| c == node) {
                    entry.remove();
                }
                self.nodes[parent as usize].kids -= 1;
                self.levels.release(level);
            }
            self.nodes[node as usize] = Node::FREE;
            self.free_nodes.push(node);
            node = parent;
        }
    }

    // Terminals.

    fn slot(&self, end: End) -> u32 {
        let node = &self.nodes[end.node as usize];
        if end.hash { node.hash } else { node.exact }
    }

    fn set_slot(&mut self, end: End, term: u32) {
        let node = &mut self.nodes[end.node as usize];
        if end.hash {
            node.hash = term;
        } else {
            node.exact = term;
        }
    }

    fn alloc_term(&mut self) -> Result<u32, Error> {
        if let Some(id) = self.free_terms.pop() {
            return Ok(id);
        }
        let id = next_id(self.terms.len())?;
        self.terms.push(Term::default());
        Ok(id)
    }

    /// Frees a terminal that holds nothing any more, and the nodes only it needed.
    fn free_if_empty(&mut self, end: End, term: u32) {
        if !self.terms[term as usize].is_empty() {
            return;
        }
        self.terms[term as usize] = Term::default();
        self.free_terms.push(term);
        self.set_slot(end, NIL);
        self.filters -= 1;
        self.prune(end.node);
    }

    fn insert_member(&mut self, term: u32, share_name: &str, id: u32) -> Result<bool, Error> {
        if let Some(name) = self.levels.get(share_name)
            && let Some(group) = self.terms[term as usize]
                .groups
                .as_mut()
                .and_then(|groups| groups.iter_mut().find(|g| g.name == name))
        {
            return Ok(group.members.insert(id));
        }
        let name = self.levels.intern(share_name)?;
        let mut members = DestSet::default();
        members.insert(id);
        self.terms[term as usize]
            .groups
            .get_or_insert_with(Box::default)
            .push(Group { name, members });
        Ok(true)
    }

    fn remove_member(&mut self, term: u32, share_name: &str, id: u32) -> bool {
        let Some(name) = self.levels.get(share_name) else {
            return false;
        };
        let term = &mut self.terms[term as usize];
        let Some(groups) = term.groups.as_mut() else {
            return false;
        };
        let Some(at) = groups.iter().position(|g| g.name == name) else {
            return false;
        };
        if !groups[at].members.remove(id) {
            return false;
        }
        if groups[at].members.is_empty() {
            groups.swap_remove(at);
            if groups.is_empty() {
                term.groups = None;
            }
            self.levels.release(name);
        }
        true
    }

    /// Writes the pattern of the filter ending at `end` into `out`.
    fn pattern_of(&self, end: End, out: &mut String) {
        let mut levels = Vec::new();
        let mut node = end.node;
        while node != ROOT {
            let n = &self.nodes[node as usize];
            levels.push(if n.level == PLUS {
                "+"
            } else {
                self.levels.resolve(n.level)
            });
            node = n.parent;
        }
        out.clear();
        for (i, level) in levels.iter().rev().enumerate() {
            if i > 0 {
                out.push('/');
            }
            out.push_str(level);
        }
        if end.hash {
            if !levels.is_empty() {
                out.push('/');
            }
            out.push('#');
        }
    }
}

/// The id after the last of `len`, unless the index has run out of them.
fn next_id(len: usize) -> Result<u32, Error> {
    u32::try_from(len)
        .ok()
        .filter(|&id| id <= MAX_ID)
        .ok_or(Error::IndexFull)
}

/// Reusable buffers for matching. Keep one per thread and pass it to every call.
#[derive(Debug, Default)]
pub struct Scratch {
    levels: Vec<u32>,
    stack: Vec<(u32, usize)>,
    terms: Vec<u32>,
    union: RoaringBitmap,
}

impl Scratch {
    /// Empty buffers, which grow to what the largest match needs and stay that size.
    pub fn new() -> Self {
        Self::default()
    }
}

/// One shared group on a matching filter, as [`TopicIndex::collect`] asks to pick from it.
pub struct SharedGroup<'a, V> {
    name: &'a str,
    members: &'a DestSet,
    destination: PhantomData<fn(V) -> V>,
}

impl<'a, V: Destination> SharedGroup<'a, V> {
    /// The ShareName.
    pub fn name(&self) -> &'a str {
        self.name
    }

    /// How many members the group has; never 0.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Whether the group has no member, which a group in the index never is.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// The member at `index`, in ascending order of id.
    pub fn member(&self, index: usize) -> Option<V> {
        self.members.get(index).map(V::from_id)
    }

    /// The members, in ascending order of id.
    pub fn members(&self) -> impl Iterator<Item = V> + 'a {
        self.members.iter().map(V::from_id)
    }
}

/// One filter that matched, as [`TopicIndex::for_each_match`] reports it.
pub struct Matched<'a, V> {
    term: &'a Term,
    levels: &'a Interner,
    destination: PhantomData<fn(V) -> V>,
}

impl<'a, V: Destination> Matched<'a, V> {
    /// The filter's own destinations, in ascending order of id.
    pub fn destinations(&self) -> impl Iterator<Item = V> + 'a {
        self.term.dests.iter().map(V::from_id)
    }

    /// The shared groups on the filter's pattern.
    pub fn groups(&self) -> impl Iterator<Item = SharedGroup<'a, V>> + 'a {
        let levels = self.levels;
        self.term
            .groups
            .iter()
            .flat_map(|groups| groups.iter())
            .map(move |group| SharedGroup {
                name: levels.resolve(group.name),
                members: &group.members,
                destination: PhantomData,
            })
    }
}

/// The heap bytes a [`TopicIndex`] holds, by part, from the capacities of its vectors and
/// tables. Allocator overhead is not in it, and a roaring bitmap's share is an estimate from
/// its containers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Memory {
    /// The node vector, 24 bytes a node.
    pub nodes: usize,
    /// The table of literal children.
    pub children: usize,
    /// The terminal vector, 24 bytes a terminal.
    pub terminals: usize,
    /// Destination sets too large to be held inline: vectors and bitmaps.
    pub destinations: usize,
    /// The group vectors of shared subscriptions.
    pub groups: usize,
    /// The text of the levels.
    pub level_text: usize,
    /// Where each level's text is, and its reference count.
    pub level_spans: usize,
    /// The table that finds a level by its text.
    pub level_table: usize,
    /// The lists of freed nodes, terminals and levels.
    pub free_lists: usize,
}

impl Memory {
    /// Every part together.
    pub fn total(&self) -> usize {
        self.nodes
            + self.children
            + self.terminals
            + self.destinations
            + self.groups
            + self.level_text
            + self.level_spans
            + self.level_table
            + self.free_lists
    }
}

#[cfg(test)]
mod tests;
