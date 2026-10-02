//! Property tests over the public API, each against a reference written from the text of the
//! specification rather than from the crate:
//!
//! - names and filters are accepted exactly when sections 4.7 and 4.8 allow them;
//! - `TopicFilter::matches` agrees with a naive matcher on random names and filters;
//! - the index finds exactly the destinations the naive matcher does, each once;
//! - random inserts and removes never lose or duplicate a destination, compaction changes
//!   nothing, and removing everything leaves an empty index;
//! - mounting keeps matching, stripping undoes it, and a mounted filter's text parses back to
//!   the same filter;
//! - a shape cover matches every name its filters match, and is the filters themselves when
//!   nothing is over the threshold.
//!
//! The strategies draw levels from a small alphabet, so that random filters and names match
//! each other often and the index's paths are shared.

use std::collections::{BTreeMap, BTreeSet};

use openqtt_topic::{
    CoverRule, Mountpoint, Scratch, TopicFilter, TopicIndex, TopicName, shape_cover,
};
use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::select;

/// The levels names are built from: empty, `$`, non-ASCII and spaces included.
const LEVELS: &[&str] = &["a", "b", "c", "", "$x", "$SYS", "\u{e9}", "a b"];

fn level() -> impl Strategy<Value = &'static str> {
    select(LEVELS)
}

fn name_text() -> impl Strategy<Value = String> {
    vec(level(), 1..5)
        .prop_map(|levels| levels.join("/"))
        .prop_filter("a name has at least one character", |text| !text.is_empty())
}

fn name() -> impl Strategy<Value = TopicName> {
    name_text().prop_map(|text| TopicName::new(&text).expect("a valid name"))
}

/// A pattern: levels or `+`, and maybe `#` at the end.
fn pattern_text() -> impl Strategy<Value = String> {
    let level = prop_oneof![4 => level(), 1 => Just("+")];
    (vec(level, 0..5), any::<bool>()).prop_filter_map(
        "a pattern has at least one character",
        |(mut levels, hash)| {
            if hash {
                levels.push("#");
            }
            let text = levels.join("/");
            (!text.is_empty()).then_some(text)
        },
    )
}

/// A filter, shared in one case in four.
fn filter_text() -> impl Strategy<Value = String> {
    let share = prop_oneof![3 => Just(None), 1 => select(&["g", "h"][..]).prop_map(Some)];
    (share, pattern_text()).prop_map(|(share, pattern)| match share {
        Some(group) => format!("$share/{group}/{pattern}"),
        None => pattern,
    })
}

fn filter() -> impl Strategy<Value = TopicFilter> {
    filter_text().prop_map(|text| TopicFilter::new(&text).expect("a valid filter"))
}

/// Strings dense in what the rules are about, for the validation properties.
fn raw_text() -> impl Strategy<Value = String> {
    let token = select(&["a", "/", "+", "#", "$", "$share/", "\0", "g", "\u{e9}"][..]);
    vec(token, 0..8).prop_map(|tokens| tokens.concat())
}

/// Matching as section 4.7 states it, level by level.
fn reference_matches(filter: &str, name: &str) -> bool {
    fn levels(filter: &[&str], name: &[&str]) -> bool {
        match filter.split_first() {
            None => name.is_empty(),
            // `#` matches the parent and any number of child levels.
            Some((&"#", _)) => true,
            Some((&"+", rest)) => name.split_first().is_some_and(|(_, n)| levels(rest, n)),
            Some((level, rest)) => name
                .split_first()
                .is_some_and(|(first, n)| first == level && levels(rest, n)),
        }
    }
    // A filter starting with a wildcard does not match a name starting with `$`.
    if name.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }
    let filter: Vec<&str> = filter.split('/').collect();
    let name: Vec<&str> = name.split('/').collect();
    levels(&filter, &name)
}

/// The pattern of a filter as section 4.8 reads it.
fn reference_pattern(filter: &str) -> &str {
    match filter.strip_prefix("$share/") {
        Some(rest) => rest.split_once('/').map_or(rest, |(_, pattern)| pattern),
        None => filter,
    }
}

fn reference_valid_name(text: &str) -> bool {
    !text.is_empty() && text.len() <= 65_535 && !text.contains(['\0', '+', '#'])
}

fn reference_valid_filter(text: &str) -> bool {
    if text.is_empty() || text.len() > 65_535 || text.contains('\0') {
        return false;
    }
    let pattern = match text.strip_prefix("$share/") {
        None => text,
        Some(rest) => match rest.split_once('/') {
            Some((group, pattern))
                if !group.is_empty() && !group.contains(['+', '#']) && !pattern.is_empty() =>
            {
                pattern
            }
            _ => return false,
        },
    };
    let levels: Vec<&str> = pattern.split('/').collect();
    levels.iter().enumerate().all(|(i, level)| match *level {
        "#" => i + 1 == levels.len(),
        "+" => true,
        other => !other.contains(['+', '#']),
    })
}

/// What the index must give for `name`: the destinations of every matching plain filter, and
/// the smallest member of every matching group, since the pick below always says 0. Also the
/// number of distinct matching patterns.
fn reference_collect(entries: &BTreeSet<(String, u32)>, name: &str) -> (Vec<u32>, usize) {
    let mut dests = BTreeSet::new();
    let mut groups: BTreeMap<(&str, &str), u32> = BTreeMap::new();
    let mut patterns = BTreeSet::new();
    for (filter, dest) in entries {
        let pattern = reference_pattern(filter);
        if !reference_matches(pattern, name) {
            continue;
        }
        patterns.insert(pattern);
        if filter.starts_with("$share/") {
            let group = groups.entry((filter.as_str(), pattern)).or_insert(*dest);
            *group = (*group).min(*dest);
        } else {
            dests.insert(*dest);
        }
    }
    dests.extend(groups.into_values());
    (dests.into_iter().collect(), patterns.len())
}

fn collect(index: &TopicIndex<u32>, name: &TopicName) -> (Vec<u32>, usize) {
    let mut out = Vec::new();
    let matched = index.collect(name, &mut Scratch::new(), |_| 0, &mut out);
    (out, matched)
}

fn entries(index: &TopicIndex<u32>) -> BTreeSet<(String, u32)> {
    let mut all = BTreeSet::new();
    index.for_each_entry(|filter, dest| {
        assert!(
            all.insert((filter.as_str().to_owned(), dest)),
            "{filter} {dest} twice"
        );
    });
    all
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    fn names_are_accepted_as_section_4_7_says(text in raw_text()) {
        prop_assert_eq!(TopicName::new(&text).is_ok(), reference_valid_name(&text), "{:?}", text);
    }

    #[test]
    fn filters_are_accepted_as_sections_4_7_and_4_8_say(text in raw_text()) {
        let filter = TopicFilter::new(&text);
        prop_assert_eq!(filter.is_ok(), reference_valid_filter(&text), "{:?}", text);
        if let Ok(filter) = filter {
            prop_assert_eq!(filter.pattern(), reference_pattern(&text));
        }
    }

    #[test]
    fn matching_agrees_with_the_reference(filter in filter(), name in name()) {
        prop_assert_eq!(
            filter.matches(&name),
            reference_matches(filter.pattern(), name.as_str()),
            "{} {}", filter, name
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn the_index_finds_what_the_reference_finds(
        entries in vec((filter(), 0..8u32), 0..24),
        names in vec(name(), 1..16),
    ) {
        let mut index = TopicIndex::new();
        let mut model = BTreeSet::new();
        for (filter, dest) in &entries {
            let added = index.insert(filter, *dest).expect("room in the index");
            prop_assert_eq!(added, model.insert((filter.as_str().to_owned(), *dest)));
        }
        for name in &names {
            let expected = reference_collect(&model, name.as_str());
            prop_assert_eq!(collect(&index, name), expected, "{}", name);
        }
    }

    #[test]
    fn inserts_and_removes_never_lose_or_duplicate_a_destination(
        pool in vec(filter(), 1..10),
        ops in vec((any::<bool>(), any::<prop::sample::Index>(), 0..6u32), 1..200),
        names in vec(name(), 1..8),
    ) {
        let mut index = TopicIndex::new();
        let mut model = BTreeSet::new();
        for (insert, which, dest) in ops {
            let filter = which.get(&pool);
            let key = (filter.as_str().to_owned(), dest);
            if insert {
                let added = index.insert(filter, dest).expect("room in the index");
                prop_assert_eq!(added, model.insert(key));
            } else {
                prop_assert_eq!(index.remove(filter, dest), model.remove(&key));
            }
            prop_assert_eq!(index.entries(), model.len());
            let held = model.contains(&(filter.as_str().to_owned(), dest));
            prop_assert_eq!(index.contains(filter, dest), held);
        }
        let patterns: BTreeSet<&str> = model.iter().map(|(f, _)| reference_pattern(f)).collect();
        prop_assert_eq!(index.filters(), patterns.len());
        prop_assert_eq!(entries(&index), model.clone());
        for name in &names {
            prop_assert_eq!(collect(&index, name), reference_collect(&model, name.as_str()));
        }

        // Compaction changes what is held in no way.
        index.compact();
        prop_assert_eq!(entries(&index), model.clone());
        for name in &names {
            prop_assert_eq!(collect(&index, name), reference_collect(&model, name.as_str()));
        }

        // Taking everything out leaves the root and nothing else.
        for (text, dest) in &model {
            let filter = TopicFilter::new(text).expect("a valid filter");
            prop_assert!(index.remove(&filter, *dest));
        }
        prop_assert!(index.is_empty());
        prop_assert_eq!((index.filters(), index.nodes(), index.levels()), (0, 1, 0));
    }

    #[test]
    fn mounting_keeps_matching_and_stripping_undoes_it(
        username in vec(select(&["acme", "production", "pump-3", "x"][..]), 1..4),
        filter in filter(),
        name in name(),
    ) {
        let username = username.join("/");
        let mount = Mountpoint::parse("ingest/${username}/")
            .expect("a valid mountpoint")
            .resolve(Some(&username), "client")
            .expect("a usable username");
        let mounted_filter = mount.mount_filter(&filter).expect("short enough");
        let mounted_name = mount.mount_name(&name).expect("short enough");
        prop_assert_eq!(mounted_filter.share_name(), filter.share_name());
        prop_assert_eq!(mount.strip(&mounted_name), Some(name.clone()));
        if filter.matches(&name) {
            prop_assert!(mounted_filter.matches(&mounted_name));
        }
        if mounted_filter.matches(&mounted_name) && !filter.matches(&name) {
            // Only the `$` rule of the first level, which the mount moves inward.
            prop_assert!(name.starts_with_dollar());
            prop_assert!(filter.pattern().starts_with(['+', '#']));
        }
    }

    #[test]
    fn a_mounted_filter_means_what_its_text_says(
        parts in vec(
            select(&["ingest/", "$share/", "$share", "$", "g/", "share/", "${username}",
                "${clientid}"][..]),
            1..5,
        ),
        username in select(&["acme", "share/g", "/g", "x/y"][..]),
        client_id in select(&["c", "e/g", "share"][..]),
        filter in filter(),
    ) {
        // Whatever mounts resolve, a mounted filter's text parses back to the same filter, so
        // the text can stand for it in a route view or a cover.
        let template = format!("{}/", parts.concat());
        let Ok(mountpoint) = Mountpoint::parse(&template) else {
            return Ok(());
        };
        let Ok(mount) = mountpoint.resolve(Some(username), client_id) else {
            return Ok(());
        };
        let mounted = mount.mount_filter(&filter).expect("short enough");
        let reparsed = TopicFilter::new(mounted.as_str()).expect("a mounted filter is valid");
        prop_assert_eq!(reparsed.share_name(), filter.share_name());
        prop_assert_eq!(reparsed.pattern(), mounted.pattern());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn a_shape_cover_matches_everything_its_filters_match(
        filters in vec(filter(), 0..40),
        names in vec(name(), 1..16),
        threshold in 0..4usize,
        floor in 0..4usize,
    ) {
        let cover = shape_cover(&filters, CoverRule { threshold, floor });
        let texts: Vec<&str> = cover.iter().map(|e| e.filter.as_str()).collect();
        let mut sorted = texts.clone();
        sorted.sort_unstable();
        sorted.dedup();
        prop_assert_eq!(&sorted, &texts, "sorted, each filter once");
        for name in &names {
            for filter in filters.iter().filter(|f| !f.is_shared() && f.matches(name)) {
                prop_assert!(
                    cover.iter().any(|e| !e.filter.is_shared() && e.filter.matches(name)),
                    "{} matches {} and nothing in {:?}", filter, name, texts
                );
            }
        }
        for filter in filters.iter().filter(|f| f.is_shared()) {
            prop_assert!(cover.iter().any(|e| e.filter == *filter && !e.coarse));
        }
        // An entry that is not coarse is one of the edge's own filters.
        for entry in cover.iter().filter(|e| !e.coarse) {
            prop_assert!(filters.contains(&entry.filter), "{} is not the edge's", entry.filter);
        }
    }

    #[test]
    fn under_a_high_threshold_the_cover_is_the_filters(filters in vec(filter(), 0..40)) {
        // Forty filters cannot give a node more than 64 children, so nothing is coarsened, and
        // a filter is left out only when a `#` filter of the edge covers it.
        let cover = shape_cover(&filters, CoverRule::DEFAULT);
        prop_assert!(cover.iter().all(|e| !e.coarse));
        for entry in &cover {
            prop_assert!(filters.contains(&entry.filter));
        }
        for filter in &filters {
            let kept = cover.iter().any(|e| e.filter == *filter);
            let covered = !filter.is_shared()
                && cover.iter().any(|e| {
                    !e.filter.is_shared() && below_hash(e.filter.pattern(), filter.pattern())
                });
            prop_assert!(kept || covered, "{} is missing from the cover", filter);
        }
    }
}

/// Whether `pattern` is covered by `hash`, a filter ending in `#`: equal to its parent level or
/// below it, and for `#` alone, not a `$` topic's.
fn below_hash(hash: &str, pattern: &str) -> bool {
    match hash.strip_suffix("/#") {
        Some(parent) => pattern == parent || pattern.starts_with(&format!("{parent}/")),
        None => hash == "#" && !pattern.starts_with('$'),
    }
}
