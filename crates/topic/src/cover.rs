//! The shape cover an edge registers with the router (report R6, D2 and D3).
//!
//! An edge keeps an exact index of its clients' subscriptions and tells the router only a cover
//! of them: filters that together match at least every topic its subscribers want, so that the
//! route view every edge holds stays small. Below a node of the edge's filter tree with more
//! than T distinct children, a level position with more than T distinct values becomes `+`, and
//! a resulting shape is registered only when more than T of the edge's filters share it; every
//! other filter is registered as it is. Many devices' `<org>/<ns>/<device>/commands/#` become
//! one `ingest/+/production/+/commands/#`, while one consumer's `ingest/acme/+/+/telemetry`
//! stays as it is.
//!
//! Replacing such a node by `prefix/#`, as R3 first wrote it, draws every device's telemetry to
//! every edge, and merging its children under `+` widens a consumer of one organisation into a
//! consumer of all of them; spike S2 measured both (R6, section 5). T is 64, and coarsening may
//! start at the first level (R6, D3). A deployment that places connections by namespace raises
//! the floor below which nothing is coarsened.
//!
//! This is the cover computed afresh from a set of filters, as the spike measured it. Keeping
//! it up to date change by change, with hysteresis at T/2 (R6, D4), is the edge's to build on
//! it.

use std::collections::{HashMap, HashSet};

use foldhash::fast::RandomState;

use crate::TopicFilter;

/// How an edge coarsens its interest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CoverRule {
    /// T: a node is coarsened when it has more than this many distinct children, a level
    /// position below it becomes `+` when it has more than this many distinct values, and a
    /// shape is registered when more than this many filters share it.
    pub threshold: usize,
    /// Nodes at a depth below this are never coarsened, where the root is at depth 0 and a
    /// first level at depth 1. The root is never coarsened whatever the floor, since a `+` at
    /// the first level would stop matching the topics that begin with `$` ([MQTT-4.7.2-1]) and
    /// the cover would lose them.
    pub floor: usize,
}

impl CoverRule {
    /// T = 64 from the first level (R6, D3).
    pub const DEFAULT: Self = Self {
        threshold: 64,
        floor: 1,
    };
}

impl Default for CoverRule {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// One filter of a cover.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoverEntry {
    /// The filter to register.
    pub filter: TopicFilter,
    /// Whether coarsening made it, so that it may match topics no subscriber of the edge wants.
    /// The edge counts deliveries that match only such entries, which is what tunes T (R6, D4).
    pub coarse: bool,
}

/// The shape cover of `filters` under `rule`, sorted by filter and each filter once.
///
/// A shared subscription is registered as it is, never coarsened, since the route view counts
/// its connected members per edge (R6, amendment 2 to R3).
pub fn shape_cover<'a>(
    filters: impl IntoIterator<Item = &'a TopicFilter>,
    rule: CoverRule,
) -> Vec<CoverEntry> {
    let mut tree = Tree::new();
    let mut out = Vec::new();
    for filter in filters {
        if filter.is_shared() {
            out.push((filter.as_str().to_owned(), false));
        } else {
            tree.insert(filter.pattern());
        }
    }
    walk(&tree, rule, &mut out);

    let mut entries: Vec<CoverEntry> = Vec::with_capacity(out.len());
    out.sort();
    for (text, coarse) in out {
        if let Some(last) = entries.last_mut()
            && last.filter.as_str() == text
        {
            last.coarse |= coarse;
            continue;
        }
        // Every text is one of the edge's filters, or a shape that was checked when it was
        // made, so none is left out here.
        if let Ok(filter) = TopicFilter::new(&text) {
            entries.push(CoverEntry { filter, coarse });
        }
    }
    entries
}

/// The root of a [`Tree`].
const ROOT: usize = 0;

/// One edge's filters, as a tree of levels. The nodes live in one vector and name each other
/// by index, so that neither walking the tree nor dropping it recurses: a filter can be 65,536
/// levels deep, which would overflow any stack a recursion used.
struct Tree<'a> {
    nodes: Vec<Node<'a>>,
}

#[derive(Default)]
struct Node<'a> {
    kids: HashMap<&'a str, usize, RandomState>,
    plus: Option<usize>,
    /// A filter ends here.
    exact: bool,
    /// A filter ends here with `/#`.
    hash: bool,
}

impl<'a> Tree<'a> {
    fn new() -> Self {
        Self {
            nodes: vec![Node::default()],
        }
    }

    fn insert(&mut self, pattern: &'a str) {
        let mut node = ROOT;
        for level in pattern.split('/') {
            if level == "#" {
                self.nodes[node].hash = true;
                return;
            }
            let next = self.nodes.len();
            let child = if level == "+" {
                *self.nodes[node].plus.get_or_insert(next)
            } else {
                *self.nodes[node].kids.entry(level).or_insert(next)
            };
            if child == next {
                self.nodes.push(Node::default());
            }
            node = child;
        }
        self.nodes[node].exact = true;
    }

    /// The children of `node`, each with the level leading to it.
    fn children(&self, node: usize) -> impl Iterator<Item = (&'a str, usize)> + '_ {
        let n = &self.nodes[node];
        n.kids
            .iter()
            .map(|(&level, &kid)| (level, kid))
            .chain(n.plus.map(|plus| ("+", plus)))
    }
}

/// `rest` below the node at `path`. The path of the root and the path of an empty first level
/// are both empty, so the depth tells them apart.
fn join(path: &str, depth: usize, rest: &str) -> String {
    if depth == 0 {
        rest.to_owned()
    } else {
        format!("{path}/{rest}")
    }
}

/// What the walk does next: enter a node, or leave one and take its level off the path.
enum Step<'a> {
    Enter {
        node: usize,
        depth: usize,
        level: &'a str,
    },
    Leave {
        len: usize,
    },
}

/// Walks the tree depth first with a stack of its own, keeping the path to the current node.
fn walk<'a>(tree: &Tree<'a>, rule: CoverRule, out: &mut Vec<(String, bool)>) {
    let mut path = String::new();
    let mut steps = vec![Step::Enter {
        node: ROOT,
        depth: 0,
        level: "",
    }];
    while let Some(step) = steps.pop() {
        let (node, depth, level) = match step {
            Step::Leave { len } => {
                path.truncate(len);
                continue;
            }
            Step::Enter { node, depth, level } => (node, depth, level),
        };
        steps.push(Step::Leave { len: path.len() });
        if depth > 1 {
            path.push('/');
        }
        path.push_str(level);
        let n = &tree.nodes[node];
        if n.hash {
            // `prefix/#` matches the node itself and everything below it, except that `#`
            // alone does not match the topics beginning with `$` [MQTT-4.7.2-1]: below the
            // root, the filters under a `$` level still need covering.
            out.push((join(&path, depth, "#"), false));
            if depth == 0 {
                for (level, kid) in tree.children(node).filter(|(l, _)| l.starts_with('$')) {
                    steps.push(Step::Enter {
                        node: kid,
                        depth: 1,
                        level,
                    });
                }
            }
            continue;
        }
        if n.exact {
            out.push((path.clone(), false));
        }
        let distinct = n.kids.len() + usize::from(n.plus.is_some());
        if depth >= rule.floor.max(1) && distinct > rule.threshold {
            shapes(tree, node, &path, depth, rule.threshold, out);
            continue;
        }
        for (level, kid) in tree.children(node) {
            steps.push(Step::Enter {
                node: kid,
                depth: depth + 1,
                level,
            });
        }
    }
}

/// Every filter below `node`, as levels relative to it, found with a stack of its own.
fn below<'a>(tree: &Tree<'a>, node: usize) -> Vec<Vec<&'a str>> {
    enum Visit<'a> {
        Enter { node: usize, level: &'a str },
        Leave,
    }
    let mut out = Vec::new();
    let mut levels = Vec::new();
    let mut steps: Vec<Visit<'a>> = tree
        .children(node)
        .map(|(level, node)| Visit::Enter { node, level })
        .collect();
    while let Some(step) = steps.pop() {
        let Visit::Enter { node, level } = step else {
            levels.pop();
            continue;
        };
        levels.push(level);
        steps.push(Visit::Leave);
        let n = &tree.nodes[node];
        if n.hash {
            // `#` covers the node and everything further down.
            levels.push("#");
            out.push(levels.clone());
            levels.pop();
            continue;
        }
        if n.exact {
            out.push(levels.clone());
        }
        for (level, kid) in tree.children(node) {
            steps.push(Visit::Enter { node: kid, level });
        }
    }
    out
}

/// The shapes of the filters below a coarsened node at `path`.
fn shapes(
    tree: &Tree<'_>,
    node: usize,
    path: &str,
    depth: usize,
    threshold: usize,
    out: &mut Vec<(String, bool)>,
) {
    let filters = below(tree, node);
    let positions = filters.iter().map(Vec::len).max().unwrap_or(0);
    // The distinct literal values at each position, counted through one set.
    let mut seen: HashSet<(usize, &str), RandomState> = HashSet::default();
    let mut distinct = vec![0usize; positions];
    for filter in &filters {
        for (position, &level) in filter.iter().enumerate() {
            if level != "+" && level != "#" && seen.insert((position, level)) {
                distinct[position] += 1;
            }
        }
    }
    let wild: Vec<bool> = distinct.iter().map(|&n| n > threshold).collect();
    let mut groups: HashMap<Vec<&str>, Vec<usize>, RandomState> = HashMap::default();
    for (i, filter) in filters.iter().enumerate() {
        let shape = filter
            .iter()
            .zip(&wild)
            .map(|(level, &wild)| if wild && *level != "#" { "+" } else { level })
            .collect();
        groups.entry(shape).or_default().push(i);
    }
    for (shape, members) in groups {
        let text = join(path, depth, &shape.join("/"));
        // A `+` in place of an empty level adds a byte, which could take the longest filters
        // past 65,535; those stay exact.
        if members.len() > threshold && TopicFilter::new(&text).is_ok() {
            out.push((text, true));
        } else {
            for i in members {
                out.push((join(path, depth, &filters[i].join("/")), false));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filters(texts: &[String]) -> Vec<TopicFilter> {
        texts.iter().map(|t| TopicFilter::new(t).unwrap()).collect()
    }

    fn cover(texts: &[String], threshold: usize, floor: usize) -> Vec<(String, bool)> {
        shape_cover(&filters(texts), CoverRule { threshold, floor })
            .into_iter()
            .map(|e| (e.filter.as_str().to_owned(), e.coarse))
            .collect()
    }

    fn devices(orgs: usize, per_org: usize) -> Vec<String> {
        let mut out = Vec::new();
        for org in 0..orgs {
            for d in 0..per_org {
                out.push(format!("ingest/org{org}/production/d{org}x{d}/commands/#"));
            }
        }
        out
    }

    #[test]
    fn below_the_threshold_nothing_changes() {
        let texts = devices(2, 2);
        let got = cover(&texts, 4, 1);
        let mut expected: Vec<(String, bool)> = texts.into_iter().map(|t| (t, false)).collect();
        expected.sort();
        assert_eq!(got, expected);
    }

    #[test]
    fn device_filters_collapse_into_their_shape() {
        let got = cover(&devices(10, 3), 4, 1);
        assert_eq!(
            got,
            [(String::from("ingest/+/production/+/commands/#"), true)]
        );
    }

    #[test]
    fn a_rare_filter_stays_exact() {
        let mut texts = devices(10, 3);
        texts.push(String::from("ingest/org3/+/+/telemetry"));
        assert_eq!(
            cover(&texts, 4, 1),
            [
                (String::from("ingest/+/production/+/commands/#"), true),
                (String::from("ingest/org3/+/+/telemetry"), false),
            ]
        );
    }

    #[test]
    fn the_floor_keeps_upper_levels_exact() {
        // With the floor at the namespace, only a namespace with more than T devices is
        // coarsened, and its organisation stays named.
        let mut texts = devices(1, 6);
        texts.extend(devices(3, 2).into_iter().skip(2));
        let got = cover(&texts, 4, 3);
        assert_eq!(
            got[0],
            (String::from("ingest/org0/production/+/commands/#"), true)
        );
        assert_eq!(got.len(), 5);
        assert!(got[1..].iter().all(|(_, coarse)| !coarse));
    }

    #[test]
    fn a_multi_level_wildcard_covers_what_is_below_it() {
        let mut texts = devices(1, 3);
        texts.push(String::from("ingest/org0/#"));
        texts.push(String::from("ingest/org0"));
        assert_eq!(
            cover(&texts, 4, 1),
            [(String::from("ingest/org0/#"), false)]
        );
        assert_eq!(
            cover(&[String::from("#"), String::from("a/b")], 0, 1),
            [(String::from("#"), false)]
        );
    }

    #[test]
    fn an_empty_first_level_is_kept() {
        let texts = [String::from("/#"), String::from("/"), String::from("//a")];
        assert_eq!(cover(&texts, 4, 1), [(String::from("/#"), false)]);
        let texts = [String::from("/a"), String::from("/b"), String::from("/c")];
        assert_eq!(cover(&texts, 2, 1), [(String::from("/+"), true)]);
    }

    #[test]
    fn a_multi_level_wildcard_alone_leaves_the_dollar_topics_to_cover() {
        // `#` does not match `$SYS/load` [MQTT-4.7.2-1], so `$SYS/+` must stay.
        let texts = [
            String::from("#"),
            String::from("$SYS/+"),
            String::from("a/b"),
            String::from("+/x"),
        ];
        assert_eq!(
            cover(&texts, 4, 1),
            [(String::from("#"), false), (String::from("$SYS/+"), false)]
        );
    }

    #[test]
    fn the_first_level_is_never_coarsened() {
        // More than T first levels, `$SYS` among them: a `+` there would stop matching
        // `$SYS/x` [MQTT-4.7.2-1], so the root keeps its children exact.
        let mut texts: Vec<String> = (0..10).map(|i| format!("t{i}/x")).collect();
        texts.push(String::from("$SYS/x"));
        for floor in [0, 1] {
            let got = cover(&texts, 2, floor);
            assert_eq!(got.len(), 11, "floor {floor}");
            assert!(got.iter().all(|(f, coarse)| !coarse && !f.starts_with('+')));
        }
    }

    #[test]
    fn shared_subscriptions_are_registered_as_they_are() {
        let mut texts = devices(10, 3);
        texts.push(String::from("$share/g/ingest/org1/+/+/telemetry"));
        texts.push(String::from("$share/g/ingest/org1/+/+/telemetry"));
        let got = cover(&texts, 4, 1);
        assert_eq!(
            got,
            [
                (String::from("$share/g/ingest/org1/+/+/telemetry"), false),
                (String::from("ingest/+/production/+/commands/#"), true),
            ]
        );
    }

    #[test]
    fn the_default_rule_is_r6s() {
        assert_eq!(
            CoverRule::default(),
            CoverRule {
                threshold: 64,
                floor: 1
            }
        );
    }
}
