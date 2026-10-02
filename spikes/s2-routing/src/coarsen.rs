//! Interest coarsening at the edge (R3, section Router).
//!
//! An edge keeps an exact index of its local subscriptions and registers with the router only a
//! cover set: filters that match at least every topic its subscribers want. A node with more
//! than T distinct children is replaced by a coarser filter. Two forms are measured:
//!
//! - `#`-cover, as R3 writes it: the node becomes `prefix/#`.
//! - `+`-cover: the node's children are merged under `prefix/+`, keeping what follows them,
//!   so `ingest/o/n/<device>/commands/#` for many devices becomes `ingest/o/n/+/commands/#`
//!   rather than `ingest/o/n/#`, which would also attract every device's telemetry.
//!
//! A floor keeps nodes shallower than a given depth from ever being coarsened. Without one, the
//! production shape collapses: an edge serves devices of more than T organisations, so the node
//! `ingest` itself has more than T children.

use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum Policy {
    #[serde(rename = "#")]
    Hash,
    #[serde(rename = "+")]
    Plus,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct Rule {
    pub t: usize,
    pub policy: Policy,
    /// Nodes at a depth below this are never coarsened; the root is depth 0, `ingest` is 1.
    pub floor: usize,
    /// A coarse node stays coarse until its children drop to T/2, so that one device moving
    /// does not flip it back and forth.
    pub hysteresis: bool,
}

/// One edge's exact interest, as a tree of level strings.
#[derive(Default)]
pub struct Tree {
    kids: HashMap<Box<str>, Tree>,
    plus: Option<Box<Tree>>,
    exact: bool,
    hash: bool,
}

impl Tree {
    pub fn insert(&mut self, filter: &str) {
        let mut n = self;
        for l in filter.split('/') {
            match l {
                "#" => {
                    n.hash = true;
                    return;
                }
                "+" => n = n.plus.get_or_insert_with(Default::default),
                _ => {
                    if !n.kids.contains_key(l) {
                        n.kids.insert(l.into(), Tree::default());
                    }
                    n = n.kids.get_mut(l).expect("just inserted");
                }
            }
        }
        n.exact = true;
    }

    fn merge_from(&mut self, other: &Tree) {
        self.exact |= other.exact;
        self.hash |= other.hash;
        for (k, v) in &other.kids {
            if !self.kids.contains_key(k) {
                self.kids.insert(k.clone(), Tree::default());
            }
            if let Some(dst) = self.kids.get_mut(k) {
                dst.merge_from(v);
            }
        }
        if let Some(p) = &other.plus {
            self.plus.get_or_insert_with(Default::default).merge_from(p);
        }
    }

    fn distinct(&self) -> usize {
        self.kids.len() + usize::from(self.plus.is_some())
    }
}

/// A cover set: the filters an edge registers, and which of them are coarse.
#[derive(Default)]
pub struct Cover {
    pub filters: Vec<String>,
    /// Indexes into `filters` of the entries a coarsening produced.
    pub coarse: Vec<usize>,
    /// Paths of the nodes coarsened, kept for hysteresis.
    pub coarse_nodes: HashSet<String>,
}

pub fn cover(tree: &Tree, rule: Rule, before: Option<&HashSet<String>>) -> Cover {
    let mut c = Cover::default();
    let mut path = String::new();
    walk(tree, &mut path, 0, rule, before, &mut c, false);
    c
}

fn join(path: &str, level: &str) -> String {
    if path.is_empty() {
        level.to_string()
    } else {
        format!("{path}/{level}")
    }
}

fn push(c: &mut Cover, f: String, coarse: bool) {
    if coarse {
        c.coarse.push(c.filters.len());
    }
    c.filters.push(f);
}

fn walk(
    n: &Tree,
    path: &mut String,
    depth: usize,
    rule: Rule,
    before: Option<&HashSet<String>>,
    c: &mut Cover,
    merged: bool,
) {
    let d = n.distinct();
    let was = rule.hysteresis && before.is_some_and(|b| b.contains(path.as_str()));
    let coarse = depth >= rule.floor && depth > 0 && (d > rule.t || (was && d > rule.t / 2));
    if n.hash {
        // `prefix/#` already covers everything below.
        push(c, join(path, "#"), merged);
        return;
    }
    if coarse {
        c.coarse_nodes.insert(path.clone());
        match rule.policy {
            Policy::Hash => {
                push(c, join(path, "#"), true);
                return;
            }
            Policy::Plus => {
                if n.exact {
                    push(c, path.clone(), merged);
                }
                let mut m = Tree::default();
                for k in n.kids.values() {
                    m.merge_from(k);
                }
                if let Some(p) = &n.plus {
                    m.merge_from(p);
                }
                let len = path.len();
                if !path.is_empty() {
                    path.push('/');
                }
                path.push('+');
                walk(&m, path, depth + 1, rule, before, c, true);
                path.truncate(len);
                return;
            }
        }
    }
    if n.exact {
        push(c, path.clone(), merged);
    }
    let len = path.len();
    for (k, v) in &n.kids {
        if !path.is_empty() {
            path.push('/');
        }
        path.push_str(k);
        walk(v, path, depth + 1, rule, before, c, merged);
        path.truncate(len);
    }
    if let Some(p) = &n.plus {
        if !path.is_empty() {
            path.push('/');
        }
        path.push('+');
        walk(p, path, depth + 1, rule, before, c, merged);
        path.truncate(len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(t: usize, policy: Policy, floor: usize) -> Rule {
        Rule {
            t,
            policy,
            floor,
            hysteresis: false,
        }
    }

    fn sorted(c: &Cover) -> Vec<String> {
        let mut v = c.filters.clone();
        v.sort();
        v
    }

    fn devices(n: usize) -> Tree {
        let mut t = Tree::default();
        for i in 0..n {
            t.insert(&format!("ingest/acme/production/d{i}/commands/#"));
        }
        t.insert("ingest/acme/staging/d0/commands/#");
        t
    }

    #[test]
    fn below_the_threshold_nothing_changes() {
        let c = cover(&devices(3), rule(4, Policy::Hash, 3), None);
        assert_eq!(c.filters.len(), 4);
        assert!(c.coarse.is_empty());
    }

    #[test]
    fn hash_cover_replaces_the_node() {
        let c = cover(&devices(5), rule(4, Policy::Hash, 3), None);
        assert_eq!(
            sorted(&c),
            vec![
                "ingest/acme/production/#",
                "ingest/acme/staging/d0/commands/#"
            ]
        );
        assert_eq!(c.coarse.len(), 1);
    }

    #[test]
    fn plus_cover_keeps_the_suffix() {
        let c = cover(&devices(5), rule(4, Policy::Plus, 3), None);
        assert_eq!(
            sorted(&c),
            vec![
                "ingest/acme/production/+/commands/#",
                "ingest/acme/staging/d0/commands/#"
            ]
        );
    }

    #[test]
    fn without_a_floor_the_top_collapses() {
        let mut t = Tree::default();
        for org in 0..10 {
            t.insert(&format!("ingest/org{org}/production/d/commands/#"));
        }
        let c = cover(&t, rule(4, Policy::Hash, 0), None);
        assert_eq!(sorted(&c), vec!["ingest/#"]);
        let c = cover(&t, rule(4, Policy::Plus, 0), None);
        assert_eq!(sorted(&c), vec!["ingest/+/production/d/commands/#"]);
        let c = cover(&t, rule(4, Policy::Hash, 3), None);
        assert_eq!(c.filters.len(), 10);
    }

    #[test]
    fn hysteresis_keeps_a_node_coarse_down_to_half() {
        let r = Rule {
            hysteresis: true,
            ..rule(4, Policy::Plus, 3)
        };
        let first = cover(&devices(5), r, None);
        assert_eq!(first.coarse.len(), 1);
        let second = cover(&devices(3), r, Some(&first.coarse_nodes));
        assert_eq!(second.coarse.len(), 1);
        let third = cover(&devices(2), r, Some(&second.coarse_nodes));
        assert!(third.coarse.is_empty());
    }
}
