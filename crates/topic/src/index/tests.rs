use std::collections::{BTreeMap, BTreeSet};

use super::*;

fn filter(text: &str) -> TopicFilter {
    TopicFilter::new(text).unwrap()
}

fn name(text: &str) -> TopicName {
    TopicName::new(text).unwrap()
}

fn index(entries: &[(&str, u32)]) -> TopicIndex<u32> {
    let mut index = TopicIndex::new();
    for &(text, dest) in entries {
        assert!(index.insert(&filter(text), dest).unwrap(), "{text} {dest}");
    }
    index
}

/// What `collect` gives, picking the first member of every group.
fn hits(index: &TopicIndex<u32>, topic: &str) -> Vec<u32> {
    let mut out = Vec::new();
    index.collect(&name(topic), &mut Scratch::new(), |_| 0, &mut out);
    out
}

/// Every (filter, destination) pair, sorted.
fn entries(index: &TopicIndex<u32>) -> Vec<(String, u32)> {
    let mut all = Vec::new();
    index.for_each_entry(|f, d| all.push((f.as_str().to_owned(), d)));
    all.sort();
    all
}

#[test]
fn the_layout_r6_measured_with_a_smaller_terminal() {
    assert_eq!(size_of::<Node>(), 24);
    assert_eq!(size_of::<Term>(), 24);
    assert_eq!(size_of::<Group>(), 24);
}

#[test]
fn section_4_7_matches_like_the_specification() {
    let index = index(&[
        ("sport/tennis/player1/#", 0),
        ("sport/tennis/+", 1),
        ("sport/#", 2),
        ("#", 3),
        ("+/+", 4),
        ("/+", 5),
        ("+", 6),
        ("$SYS/#", 7),
        ("$SYS/monitor/+", 8),
        ("+/monitor/Clients", 9),
        ("sport/tennis/player1", 10),
    ]);
    // `#` matches the parent level too.
    assert_eq!(hits(&index, "sport/tennis/player1"), [0, 1, 2, 3, 10]);
    assert_eq!(hits(&index, "sport/tennis/player1/ranking"), [0, 2, 3]);
    assert_eq!(hits(&index, "sport"), [2, 3, 6]);
    assert_eq!(hits(&index, "sport/"), [2, 3, 4]);
    assert_eq!(hits(&index, "/finance"), [3, 4, 5]);
    assert_eq!(hits(&index, "/"), [3, 4, 5]);
    // No wildcard at the first level matches a `$` topic [MQTT-4.7.2-1].
    assert_eq!(hits(&index, "$SYS/monitor/Clients"), [7, 8]);
    assert_eq!(hits(&index, "$SYS"), [7]);
    assert_eq!(hits(&index, "$other"), Vec::<u32>::new());
    // A level no filter has is matched by wildcards alone.
    assert_eq!(hits(&index, "weather"), [3, 6]);
}

#[test]
fn a_filter_holds_each_destination_once() {
    let mut index = TopicIndex::new();
    let f = filter("a/+/c");
    assert!(index.insert(&f, 7).unwrap());
    assert!(!index.insert(&f, 7).unwrap());
    assert!(index.insert(&f, 3).unwrap());
    assert!(index.contains(&f, 7) && index.contains(&f, 3));
    assert!(!index.contains(&f, 4));
    assert!(!index.contains(&filter("a/+"), 7));
    assert!(!index.contains(&filter("$share/g/a/+/c"), 7));
    assert_eq!((index.filters(), index.entries()), (1, 2));
    assert!(!index.remove(&f, 4));
    assert!(!index.remove(&filter("a/b/c"), 7));
    assert!(index.remove(&f, 7));
    assert!(!index.remove(&f, 7));
    assert_eq!(hits(&index, "a/b/c"), [3]);
}

#[test]
fn removal_prunes_everything_back_to_the_root() {
    let filters = [
        "a/b/c",
        "a/+/c/#",
        "a/b",
        "$share/g/a/b/#",
        "$share/h/a/b/#",
        "#",
        "/",
        "+/+/+",
    ];
    let mut index = TopicIndex::new();
    for text in filters {
        assert!(index.insert(&filter(text), 7).unwrap());
        assert!(index.insert(&filter(text), 9).unwrap());
    }
    assert_eq!(index.entries(), 16);
    // `$share/g/a/b/#` and `$share/h/a/b/#` share the pattern `a/b/#`.
    assert_eq!(index.filters(), 7);
    for text in filters {
        assert!(index.remove(&filter(text), 7));
        assert!(!index.remove(&filter(text), 7));
        assert!(index.remove(&filter(text), 9));
    }
    assert!(index.is_empty());
    assert_eq!((index.entries(), index.filters()), (0, 0));
    assert_eq!(index.nodes(), 1);
    assert_eq!(index.levels(), 0);
}

#[test]
fn a_shared_group_delivers_to_one_member() {
    let mut index = TopicIndex::new();
    for member in 0..10 {
        index
            .insert(&filter("$share/g/ingest/+/+/+/telemetry"), 100 + member)
            .unwrap();
    }
    index
        .insert(&filter("ingest/acme/production/pump-3/telemetry"), 1)
        .unwrap();
    let topic = name("ingest/acme/production/pump-3/telemetry");
    let mut out = Vec::new();
    let mut scratch = Scratch::new();
    for turn in 0..25 {
        let matched = index.collect(
            &topic,
            &mut scratch,
            |group| {
                assert_eq!(group.name(), "g");
                assert_eq!(group.len(), 10);
                assert!(!group.is_empty());
                assert_eq!(group.member(0), Some(100));
                assert_eq!(group.members().count(), 10);
                turn
            },
            &mut out,
        );
        assert_eq!(matched, 2);
        // The pick is taken modulo the group's size.
        assert_eq!(out, [1, 100 + u32::try_from(turn % 10).unwrap()]);
    }
}

#[test]
fn every_contributing_set_is_deduplicated() {
    // The review finding of spike S2: two groups on one filter may pick the same subscriber,
    // which must then be delivered to once.
    let index = index(&[("$share/g/a", 7), ("$share/h/a", 7)]);
    assert_eq!(hits(&index, "a"), [7]);
    // A pick that is also a plain destination of the filter.
    let index = index_with(&index, &[("a", 7), ("a", 9), ("$share/k/a", 3)]);
    assert_eq!(hits(&index, "a"), [3, 7, 9]);
    // Plain destinations of several filters.
    let index = self::index(&[("a/b", 1), ("a/b", 2), ("a/+", 2), ("#", 1), ("+/b", 3)]);
    assert_eq!(hits(&index, "a/b"), [1, 2, 3]);
    // One set alone is already sorted and unique.
    let index = self::index(&[("x", 5), ("x", 1), ("x", 3)]);
    assert_eq!(hits(&index, "x"), [1, 3, 5]);
}

fn index_with(base: &TopicIndex<u32>, more: &[(&str, u32)]) -> TopicIndex<u32> {
    let mut index = base.clone();
    for &(text, dest) in more {
        index.insert(&filter(text), dest).unwrap();
    }
    index
}

#[test]
fn large_overlapping_sets_merge_by_bitmap_union() {
    // Three filters, 1,500 destinations in all, overlapping: past UNION_ABOVE.
    let mut index = TopicIndex::new();
    let mut expected = BTreeSet::new();
    for (text, ids) in [
        ("t/+", (0..800u32).step_by(1)),
        ("t/#", (400..1_200u32).step_by(1)),
        ("#", (0..1_800u32).step_by(9)),
    ] {
        for id in ids {
            index.insert(&filter(text), id).unwrap();
            expected.insert(id);
        }
    }
    index.insert(&filter("$share/g/t/x"), 5_000).unwrap();
    index.insert(&filter("$share/g/t/x"), 5_001).unwrap();
    expected.insert(5_001);
    let mut out = Vec::new();
    let matched = index.collect(&name("t/x"), &mut Scratch::new(), |_| 1, &mut out);
    assert_eq!(matched, 4);
    assert_eq!(out, expected.into_iter().collect::<Vec<_>>());
}

#[test]
fn for_each_match_reports_every_matching_filter() {
    let index = index(&[
        ("a/#", 1),
        ("a/b", 2),
        ("a/b", 3),
        ("$share/g/a/b", 4),
        ("$share/g/a/b", 5),
        ("$share/h/+/b", 6),
    ]);
    let mut seen = Vec::new();
    let matched = index.for_each_match(&name("a/b"), &mut Scratch::new(), |m| {
        let dests: Vec<u32> = m.destinations().collect();
        let groups: Vec<(String, Vec<u32>)> = m
            .groups()
            .map(|g| (g.name().to_owned(), g.members().collect()))
            .collect();
        seen.push((dests, groups));
    });
    assert_eq!(matched, 3);
    seen.sort();
    assert_eq!(
        seen,
        [
            (vec![], vec![(String::from("h"), vec![6])]),
            (vec![1], vec![]),
            (vec![2, 3], vec![(String::from("g"), vec![4, 5])]),
        ]
    );
}

#[test]
fn entries_come_back_as_they_went_in() {
    let all = [
        ("a/+/c/#", 3),
        ("#", 4),
        ("a", 5),
        ("/", 6),
        ("+", 7),
        ("$share/g/a/+/c/#", 8),
        ("$share/$queue/#", 9),
        ("$share/g//", 10),
        ("$queue/x", 11),
    ];
    let index = index(&all);
    let mut expected: Vec<(String, u32)> = all.iter().map(|&(f, d)| (f.to_owned(), d)).collect();
    expected.sort();
    assert_eq!(entries(&index), expected);
    // Every rebuilt filter is what parsing its text gives.
    index.for_each_entry(|f, _| assert_eq!(&TopicFilter::new(f.as_str()).unwrap(), f));
}

#[test]
fn compaction_keeps_every_entry_and_drops_the_holes() {
    let mut index = TopicIndex::new();
    let filters: Vec<TopicFilter> = (0..2_000)
        .map(|i| filter(&format!("ingest/org-{}/ns/{i:016x}/commands/#", i % 7)))
        .collect();
    for (i, f) in filters.iter().enumerate() {
        index.insert(f, u32::try_from(i % 100).unwrap()).unwrap();
    }
    index
        .insert(&filter("$share/workers/ingest/+/ns/+/telemetry"), 42)
        .unwrap();
    index
        .insert(&filter("ingest/+/ns/+/telemetry"), 43)
        .unwrap();
    // Remove nine filters in ten.
    for (i, f) in filters.iter().enumerate() {
        if i % 10 != 0 {
            assert!(index.remove(f, u32::try_from(i % 100).unwrap()));
        }
    }
    let before = entries(&index);
    let memory = index.memory().total();
    let nodes = index.nodes();
    index.compact();
    assert_eq!(entries(&index), before);
    assert_eq!(index.nodes(), nodes);
    assert_eq!(index.nodes.len(), nodes);
    assert!(index.free_nodes.is_empty() && index.free_terms.is_empty());
    assert!(index.memory().total() < memory / 2);
    assert_eq!(index.memory().free_lists, 0);
    // It still matches, and still changes.
    assert_eq!(
        hits(&index, &format!("ingest/org-0/ns/{:016x}/commands/x", 70)),
        [70]
    );
    assert_eq!(hits(&index, "ingest/org-3/ns/abc/telemetry"), [42, 43]);
    assert!(index.insert(&filters[1], 1).unwrap());
    assert_eq!(
        hits(&index, &format!("ingest/org-1/ns/{:016x}/commands", 1)),
        [1]
    );
    for (i, f) in filters.iter().enumerate() {
        if i % 10 == 0 || i == 1 {
            assert!(index.remove(f, u32::try_from(i % 100).unwrap()));
        }
    }
    assert!(index.remove(&filter("$share/workers/ingest/+/ns/+/telemetry"), 42));
    assert!(index.remove(&filter("ingest/+/ns/+/telemetry"), 43));
    assert!(index.is_empty());
    assert_eq!((index.nodes(), index.levels()), (1, 0));
    index.compact();
    assert_eq!(index.nodes.len(), 1);
}

#[test]
fn memory_is_counted_by_part() {
    let mut index = TopicIndex::new();
    let empty = index.memory();
    assert_eq!(empty.nodes, size_of::<Node>());
    for i in 0..100u32 {
        index
            .insert(&filter(&format!("d/{i:04}/commands/#")), i)
            .unwrap();
    }
    for member in 0..70u32 {
        index.insert(&filter("$share/g/d/+/x"), member).unwrap();
    }
    let memory = index.memory();
    assert!(memory.nodes >= 201 * size_of::<Node>());
    assert!(memory.terminals >= 101 * size_of::<Term>());
    assert!(memory.level_text >= 100 * 4 + "commands".len());
    // A group of 70 is a bitmap, outside the terminal.
    assert!(memory.destinations > 0 && memory.groups > 0);
    assert_eq!(
        memory.total(),
        memory.nodes
            + memory.children
            + memory.terminals
            + memory.destinations
            + memory.groups
            + memory.level_text
            + memory.level_spans
            + memory.level_table
            + memory.free_lists
    );
}

#[test]
fn a_model_of_inserts_and_removes() {
    // A deterministic walk through inserts and removes, checked against a plain model after
    // every step. The property tests do the same with random filters.
    let texts = [
        "a",
        "a/b",
        "a/+",
        "a/#",
        "+/b",
        "#",
        "$share/g/a/b",
        "$share/h/a/#",
        "$x/#",
        "/",
        "+",
    ];
    let mut index = TopicIndex::new();
    let mut model: BTreeMap<(usize, u32), ()> = BTreeMap::new();
    let mut state = 0x2545_f491_4f6c_dd1du64;
    for _ in 0..3_000 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let which = usize::try_from(state % 11).unwrap();
        let dest = u32::try_from((state >> 8) % 5).unwrap();
        let f = filter(texts[which]);
        if state & (1 << 40) == 0 {
            let added = index.insert(&f, dest).unwrap();
            assert_eq!(added, model.insert((which, dest), ()).is_none());
        } else {
            let removed = index.remove(&f, dest);
            assert_eq!(removed, model.remove(&(which, dest)).is_some());
        }
        assert_eq!(index.entries(), model.len());
    }
    let mut expected: Vec<(String, u32)> = model
        .keys()
        .map(|&(which, dest)| (texts[which].to_owned(), dest))
        .collect();
    expected.sort();
    assert_eq!(entries(&index), expected);
}
