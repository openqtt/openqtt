//! The deepest input the types accept, on a thread with a 256 KiB stack, so that recursion in
//! proportion to the number of levels fails here, loudly, rather than in a broker that a client
//! hands such a topic.
//!
//! The deepest Topic Name or Topic Filter is 65,535 separators: 65,536 empty levels, which
//! section 4.7 allows. Wildcards make one level of every two bytes.

use std::thread;

use openqtt_topic::{CoverRule, TopicFilter};

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

fn filter(text: &str) -> TopicFilter {
    TopicFilter::new(text).expect("a valid filter")
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
