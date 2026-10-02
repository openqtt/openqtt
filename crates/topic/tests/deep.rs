//! Every public entry point at the deepest input the types accept, each on a thread with a
//! 256 KiB stack, so that recursion in proportion to the number of levels fails here, loudly,
//! rather than in a broker that a client hands such a topic.
//!
//! The deepest Topic Name or Topic Filter is 65,535 separators: 65,536 empty levels, which
//! section 4.7 allows. Wildcards make one level of every two bytes.

use std::thread;

use openqtt_topic::{CoverRule, Mountpoint, Scratch, TopicFilter, TopicIndex, TopicName};

/// A small stack: deep recursion overflows it long before 65,536 levels.
const STACK: usize = 256 * 1024;

/// Runs `test` on a thread with a small stack. Overflowing it aborts the test process.
fn on_a_small_stack(test: impl FnOnce() + Send + 'static) {
    thread::Builder::new()
        .stack_size(STACK)
        .spawn(test)
        .expect("a thread")
        .join()
        .expect("the test passes");
}

/// 65,536 empty levels.
fn separators() -> String {
    "/".repeat(65_535)
}

/// 32,768 levels: 32,767 `+` and a final `#`.
fn wildcards() -> String {
    format!("{}#", "+/".repeat(32_767))
}

/// 32,768 levels of `a`.
fn letters() -> String {
    format!("{}a", "a/".repeat(32_767))
}

fn name(text: &str) -> TopicName {
    TopicName::new(text).expect("a valid name")
}

fn filter(text: &str) -> TopicFilter {
    TopicFilter::new(text).expect("a valid filter")
}

#[test]
fn names_and_filters_at_65536_levels() {
    on_a_small_stack(|| {
        let deepest = name(&separators());
        assert_eq!(deepest.level_count(), 65_536);
        assert_eq!(deepest.levels().count(), 65_536);
        let letters = name(&letters());
        assert_eq!(letters.level_count(), 32_768);

        let empty = filter(&separators());
        assert_eq!(empty.level_count(), 65_536);
        assert!(empty.matches(&deepest));
        assert!(!empty.matches(&letters));
        let wild = filter(&wildcards());
        assert!(wild.has_wildcard());
        assert!(wild.matches(&letters));
        assert!(wild.matches(&deepest));
        let shared = filter(&format!("$share/g/{}", "/".repeat(65_526)));
        assert_eq!(shared.share_name(), Some("g"));
        assert_eq!(shared.level_count(), 65_527);
        assert_eq!(
            format!("{}a", separators()).parse::<TopicName>(),
            Err(openqtt_topic::Error::TooLong { len: 65_536 })
        );
    });
}

#[test]
fn mountpoints_at_65536_levels() {
    on_a_small_stack(|| {
        let deep = Mountpoint::parse(&"/".repeat(60_000)).expect("a valid mountpoint");
        let mount = deep.resolve(None, "client").expect("a usable mount");
        let mounted = mount.mount_name(&name("x")).expect("short enough");
        assert_eq!(mounted.level_count(), 60_001);
        assert_eq!(mount.strip(&mounted), Some(name("x")));

        let short = Mountpoint::parse("${clientid}/")
            .expect("a valid mountpoint")
            .resolve(None, "c")
            .expect("a usable mount");
        // The mount takes two of the 65,535 bytes, so the deepest mounted filter has 65,535
        // levels.
        let deepest = short
            .mount_filter(&filter(&"/".repeat(65_533)))
            .expect("short enough");
        assert_eq!(deepest.level_count(), 65_535);
        let wild = short
            .mount_filter(&filter(&format!("{}#", "+/".repeat(32_766))))
            .expect("short enough");
        assert!(wild.matches(&short.mount_name(&name(&letters()[2..])).expect("fits")));
        let stripped = short.strip(&name(&format!("c/{}", "/".repeat(65_532))));
        assert_eq!(stripped.map(|n| n.level_count()), Some(65_533));
    });
}

#[test]
fn the_index_at_65536_levels() {
    on_a_small_stack(|| {
        let deepest = filter(&separators());
        let wild = filter(&wildcards());
        let shared = filter(&format!("$share/g/{}", "/".repeat(65_526)));
        let mut index = TopicIndex::new();
        assert!(index.insert(&deepest, 1).expect("room"));
        assert!(index.insert(&wild, 2).expect("room"));
        assert!(index.insert(&shared, 3).expect("room"));
        assert!(index.contains(&deepest, 1) && index.contains(&wild, 2));
        assert!(index.contains(&shared, 3));

        let mut scratch = Scratch::new();
        let mut out = Vec::new();
        assert_eq!(
            index.collect(&name(&separators()), &mut scratch, |_| 0, &mut out),
            2
        );
        assert_eq!(out, [1, 2]);
        assert_eq!(
            index.collect(&name(&letters()), &mut scratch, |_| 0, &mut out),
            1
        );
        assert_eq!(out, [2]);
        let deep_shared = name(&"/".repeat(65_526));
        assert_eq!(
            index.collect(&deep_shared, &mut scratch, |_| 0, &mut out),
            2
        );
        assert_eq!(out, [2, 3]);
        let mut matched = 0;
        index.for_each_match(&name(&separators()), &mut scratch, |_| matched += 1);
        assert_eq!(matched, 2);

        let mut entries = Vec::new();
        index.for_each_entry(|f, d| entries.push((f.clone(), d)));
        entries.sort_by_key(|&(_, d)| d);
        assert_eq!(
            entries,
            [(deepest.clone(), 1), (wild.clone(), 2), (shared.clone(), 3)]
        );
        assert!(index.memory().nodes > 65_536);

        let copy = index.clone();
        index.compact();
        assert_eq!(index.entries(), 3);
        assert!(index.remove(&deepest, 1));
        assert!(index.remove(&wild, 2));
        assert!(index.remove(&shared, 3));
        assert_eq!((index.entries(), index.nodes(), index.levels()), (0, 1, 0));
        // Dropping an index that holds the deepest filters walks nothing either.
        drop(copy);
    });
}

#[test]
fn the_shape_cover_at_65536_levels() {
    on_a_small_stack(|| {
        let deep = "/".repeat(65_000);
        let filters = vec![
            // A chain 65,536 levels deep, walked to its end.
            filter(&separators()),
            filter(&wildcards()),
            // A shallow node with more children than T and a deep subtree under each, so
            // that shapes are taken from filters 65,000 levels long.
            filter(&format!("x/a0{deep}")),
            filter(&format!("x/a1{deep}")),
            filter(&format!("x/a2{deep}")),
            filter(&format!("$share/g/{}", "/".repeat(65_526))),
        ];
        let cover = openqtt_topic::shape_cover(
            &filters,
            CoverRule {
                threshold: 2,
                floor: 1,
            },
        );
        let texts: Vec<&str> = cover.iter().map(|e| e.filter.as_str()).collect();
        assert!(texts.contains(&separators().as_str()));
        assert!(texts.contains(&wildcards().as_str()));
        let shape = format!("x/+{deep}");
        assert!(cover.iter().any(|e| e.coarse && e.filter.as_str() == shape));
        assert_eq!(cover.len(), 4);
    });
}
