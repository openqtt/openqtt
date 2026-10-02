//! The index at the scale report R6 measured: a million filters shaped like the production
//! namespace, built and matched the way spike S2 did (`spikes/s2-routing`).
//!
//! Ignored by default, since it takes about half a minute and means something only in release
//! mode:
//!
//! ```text
//! cargo test --release -p openqtt-topic --test scale -- --ignored --nocapture
//! ```
//!
//! The bar is the spike's (R6, D1): about 150 bytes per filter once compacted, and a match well
//! under 10 µs at p99. Memory is counted from capacities, as `TopicIndex::memory` does, so it
//! is the same on every machine and is asserted; times are this machine's and are reported.
//!
//! The workloads are the spike's: (a) one command filter per device,
//! `ingest/<org>/<ns>/<device>/commands/#`, with organisations Zipf distributed; (b) a mix of
//! device, namespace, organisation and global filters; (c) workload (a) with 1,000 shared
//! groups over the hundred largest organisations.

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use openqtt_topic::{Scratch, TopicFilter, TopicIndex, TopicName};

/// Filters in each workload.
const FILTERS: u64 = 1_000_000;
/// Topics matched for each stream.
const SAMPLES: u64 = 1_000_000;
/// Edges, the destinations of device filters.
const EDGES: u64 = 100;
/// Compacted bytes per filter the index must stay under: the spike's 150, and room for the
/// hash tables' load factor at a different count.
const BAR_BYTES: f64 = 160.0;

// Deterministic values, as the spike derives them, so that every run sees the same filters.

fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn hash2(a: u64, salt: u64) -> u64 {
    mix(a ^ mix(salt))
}

#[expect(
    clippy::cast_precision_loss,
    reason = "53 random bits make a uniform f64 exactly"
)]
fn unit(u: u64) -> f64 {
    (u >> 11) as f64 / (1u64 << 53) as f64
}

/// A Zipf distribution over ranks 0..n, sampled by inverting its distribution function.
struct Zipf(Vec<f64>);

impl Zipf {
    #[expect(clippy::cast_precision_loss, reason = "ranks are far below 2^53")]
    fn new(n: usize) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0;
        for k in 1..=n {
            acc += 1.0 / k as f64;
            cdf.push(acc);
        }
        for c in &mut cdf {
            *c /= acc;
        }
        Self(cdf)
    }

    fn rank(&self, u: f64) -> u64 {
        let rank = self.0.partition_point(|&c| c <= u).min(self.0.len() - 1);
        u64::try_from(rank).expect("a rank fits in u64")
    }
}

const NAMESPACES: [&str; 4] = ["production", "staging", "field", "lab"];

/// The device population: organisations Zipf distributed over n/100 of them, each with one to
/// four namespaces, again Zipf distributed; device ids are 16 hexadecimal characters.
struct Shape {
    devices: u64,
    orgs: u64,
    org_zipf: Zipf,
    ns_zipf: Vec<Zipf>,
}

#[derive(Clone, Copy)]
struct Device {
    idx: u64,
    org: u64,
    ns: u64,
}

impl Shape {
    fn new(devices: u64) -> Self {
        let orgs = (devices / 100).max(10);
        Self {
            devices,
            orgs,
            org_zipf: Zipf::new(usize::try_from(orgs).expect("orgs fit in usize")),
            ns_zipf: (1..=NAMESPACES.len()).map(Zipf::new).collect(),
        }
    }

    fn namespaces_of(org: u64) -> usize {
        1 + usize::try_from(hash2(org, 0x6e73) % 4).expect("below 4")
    }

    fn device(&self, idx: u64) -> Device {
        let org = self.org_zipf.rank(unit(hash2(idx, 0x6f72)));
        let ns = self.ns_zipf[Self::namespaces_of(org) - 1].rank(unit(hash2(idx, 0x6e61)));
        Device { idx, org, ns }
    }

    fn ns_prefix(org: u64, ns: u64, out: &mut String) {
        let ns = NAMESPACES[usize::try_from(ns).expect("below 4")];
        write!(out, "ingest/org-{org:05}/{ns}").expect("writing to a String");
    }

    fn device_prefix(d: Device, out: &mut String) {
        out.clear();
        Self::ns_prefix(d.org, d.ns, out);
        write!(out, "/{:016x}", hash2(d.idx, 0x6964)).expect("writing to a String");
    }

    fn topic(d: Device, suffix: &str) -> TopicName {
        let mut text = String::new();
        Self::device_prefix(d, &mut text);
        text.push_str(suffix);
        TopicName::new(&text).expect("a valid name")
    }

    /// Filter `i` of workload (a), and its edge.
    fn command_filter(&self, i: u64) -> (TopicFilter, u32) {
        let mut text = String::new();
        Self::device_prefix(self.device(i), &mut text);
        text.push_str("/commands/#");
        let edge = u32::try_from(hash2(i, 0x6564) % EDGES).expect("below 100");
        (TopicFilter::new(&text).expect("a valid filter"), edge)
    }

    /// Filter `i` of workload (b), and its subscriber: mostly device filters, and wildcard
    /// filters of backend subscribers, each picking an organisation uniformly.
    fn mixed_filter(&self, i: u64) -> (TopicFilter, u32) {
        let d = self.device(i);
        let r = hash2(i, 0x6d78) % 100_000;
        let org = hash2(i, 0x6f67) % self.orgs;
        let ns = hash2(i, 0x6e67) % u64::try_from(Self::namespaces_of(org)).expect("small");
        let mut text = String::new();
        match r {
            0..90_000 => {
                Self::device_prefix(d, &mut text);
                text.push_str("/commands/#");
            }
            90_000..95_000 => {
                Self::device_prefix(d, &mut text);
                text.push_str("/config");
            }
            95_000..96_500 => {
                Self::ns_prefix(org, ns, &mut text);
                text.push_str("/+/telemetry");
            }
            96_500..98_000 => {
                Self::ns_prefix(org, ns, &mut text);
                text.push_str("/+/events/#");
            }
            98_000..98_990 => write!(text, "ingest/org-{org:05}/+/+/status").expect("String"),
            98_990..99_980 => write!(text, "ingest/org-{org:05}/#").expect("String"),
            99_980..99_995 if r.is_multiple_of(2) => text.push_str("ingest/+/+/+/alerts/+"),
            99_980..99_995 => text.push_str("+/+/+/+/telemetry"),
            _ if r.is_multiple_of(2) => text.push_str("$SYS/brokers/+/clients/#"),
            _ => text.push('#'),
        }
        let subscriber = u32::try_from(hash2(i, 0x7375) % (1 << 20)).expect("below 2^20");
        (TopicFilter::new(&text).expect("a valid filter"), subscriber)
    }

    /// Topic `j` of the mixed publish stream: mostly device telemetry.
    fn mixed_topic(&self, j: u64) -> TopicName {
        let d = self.device(hash2(j, 0x7470) % self.devices);
        let r = hash2(j, 0x746b) % 100;
        if r >= 95 {
            let text = format!("$SYS/brokers/edge-{}/clients/{j:x}/connected", r % 4);
            return TopicName::new(&text).expect("a valid name");
        }
        let suffix = match r {
            0..50 => "/telemetry",
            50..70 => "/commands/firmware",
            70..80 => "/config",
            80..90 => "/status",
            _ => "/events/boot",
        };
        Self::topic(d, suffix)
    }
}

/// Latencies of single calls, in nanoseconds.
struct Latencies(Vec<u64>);

impl Latencies {
    fn quantile(&self, q: f64) -> Duration {
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "an index into a million samples, from a quantile in [0, 1]"
        )]
        let at = ((self.0.len() - 1) as f64 * q).round() as usize;
        Duration::from_nanos(self.0[at])
    }

    fn mean(&self) -> Duration {
        let total: u64 = self.0.iter().sum();
        Duration::from_nanos(total / u64::try_from(self.0.len()).expect("fits"))
    }
}

struct Timing {
    filters_per_match: f64,
    destinations_per_match: f64,
    collect: Latencies,
    walk: Latencies,
}

#[expect(clippy::cast_precision_loss, reason = "counts far below 2^53")]
fn time_topics(index: &TopicIndex<u32>, topics: &[TopicName]) -> Timing {
    let mut scratch = Scratch::new();
    let mut out = Vec::new();
    let mut turn = 0usize;
    // Warm the caches and the branch predictors on another order.
    for topic in topics.iter().rev().take(200_000) {
        index.collect(topic, &mut scratch, |_| 0, &mut out);
    }
    let mut collect = Vec::with_capacity(topics.len());
    let (mut filters, mut destinations) = (0usize, 0usize);
    for topic in topics {
        let start = Instant::now();
        filters += index.collect(
            topic,
            &mut scratch,
            |_| {
                turn += 1;
                turn
            },
            &mut out,
        );
        collect.push(nanos(start.elapsed()));
        destinations += out.len();
    }
    // The walk alone: which filters match, without gathering their destinations.
    let mut walk = Vec::with_capacity(topics.len());
    for topic in topics {
        let start = Instant::now();
        index.for_each_match(topic, &mut scratch, |_| {});
        walk.push(nanos(start.elapsed()));
    }
    collect.sort_unstable();
    walk.sort_unstable();
    Timing {
        filters_per_match: filters as f64 / topics.len() as f64,
        destinations_per_match: destinations as f64 / topics.len() as f64,
        collect: Latencies(collect),
        walk: Latencies(walk),
    }
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

#[expect(clippy::cast_precision_loss, reason = "rates and ratios for a report")]
fn per_second(count: usize, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64()
}

#[expect(clippy::cast_precision_loss, reason = "rates and ratios for a report")]
fn ratio(bytes: usize, filters: usize) -> f64 {
    bytes as f64 / filters as f64
}

/// A deterministic shuffle of a tenth of 0..n.
fn shuffled_tenth(n: u64, seed: u64) -> Vec<u64> {
    let mut v: Vec<u64> = (0..n)
        .filter(|&i| hash2(i, seed).is_multiple_of(10))
        .collect();
    let mut state = seed;
    for i in (1..v.len()).rev() {
        state = mix(state);
        let j = usize::try_from(state % u64::try_from(i + 1).expect("fits")).expect("fits");
        v.swap(i, j);
    }
    v
}

/// Shared groups of workload (c): `$share/grp<g>/ingest/<org>/+/+/telemetry` over the hundred
/// largest organisations, two to 64 members each, every hundredth group with a thousand.
fn add_groups(index: &mut TopicIndex<u32>) -> u64 {
    let mut members = 0;
    for g in 0..1_000u64 {
        let text = format!("$share/grp{g}/ingest/org-{:05}/+/+/telemetry", g % 100);
        let filter = TopicFilter::new(&text).expect("a valid filter");
        let count = if g % 100 == 99 {
            1_000
        } else {
            2 + hash2(g, 0x6d62) % 63
        };
        for k in 0..count {
            let member = u32::try_from(hash2(g * 10_000 + k, 0x6d6d) % (1 << 20)).expect("fits");
            index.insert(&filter, member).expect("room in the index");
        }
        members += count;
    }
    members
}

#[test]
#[ignore = "a measurement at a million filters: run in release mode, by hand"]
#[expect(clippy::print_stderr, reason = "the test reports its measurements")]
#[expect(clippy::too_many_lines, reason = "one measurement, read top to bottom")]
fn a_million_filters() {
    if cfg!(debug_assertions) {
        eprintln!("note: built without optimizations; the times mean little");
    }
    let shape = Shape::new(FILTERS);
    let n = usize::try_from(FILTERS).expect("fits");
    let mut report = String::new();

    // (a) Device filters: memory, inserts and removals.
    let filters: Vec<(TopicFilter, u32)> = (0..FILTERS).map(|i| shape.command_filter(i)).collect();
    let mut index = TopicIndex::new();
    let start = Instant::now();
    for (filter, edge) in &filters {
        index.insert(filter, *edge).expect("room in the index");
    }
    let inserted = start.elapsed();
    assert_eq!(index.filters(), n);
    let built = index.memory();
    let tenth = shuffled_tenth(FILTERS, 0x726d);
    let start = Instant::now();
    for &i in &tenth {
        let (filter, edge) = &filters[usize::try_from(i).expect("fits")];
        assert!(index.remove(filter, *edge));
    }
    let removed = start.elapsed();
    let start = Instant::now();
    for &i in &tenth {
        let (filter, edge) = &filters[usize::try_from(i).expect("fits")];
        index.insert(filter, *edge).expect("room in the index");
    }
    let reinserted = start.elapsed();
    let start = Instant::now();
    index.compact();
    let compacting = start.elapsed();
    let compacted = index.memory();
    let per_filter = ratio(compacted.total(), n);
    writeln!(
        report,
        "(a) {n} device filters, {} nodes, {} levels",
        index.nodes(),
        index.levels()
    )
    .expect("String");
    writeln!(
        report,
        "    bytes per filter: {:.1} as built, {per_filter:.1} compacted (bar {BAR_BYTES})",
        ratio(built.total(), n)
    )
    .expect("String");
    writeln!(
        report,
        "    compacted, per filter: nodes {:.1}, children {:.1}, terminals {:.1}, level text \
         {:.1}, level spans {:.1}, level table {:.1}",
        ratio(compacted.nodes, n),
        ratio(compacted.children, n),
        ratio(compacted.terminals, n),
        ratio(compacted.level_text, n),
        ratio(compacted.level_spans, n),
        ratio(compacted.level_table, n),
    )
    .expect("String");
    writeln!(
        report,
        "    inserts {:.2} M/s, removals of a tenth in random order {:.2} M/s, reinserts \
         {:.2} M/s, compaction {compacting:.2?}",
        per_second(n, inserted) / 1e6,
        per_second(tenth.len(), removed) / 1e6,
        per_second(tenth.len(), reinserted) / 1e6,
    )
    .expect("String");

    // (a) Matching: a command to a subscribed device, and telemetry nothing wants.
    let device = |j: u64| shape.device(hash2(j, 0x7464) % FILTERS);
    let commands: Vec<TopicName> = (0..SAMPLES)
        .map(|j| Shape::topic(device(j), "/commands/firmware"))
        .collect();
    let telemetry: Vec<TopicName> = (0..SAMPLES)
        .map(|j| Shape::topic(device(j), "/telemetry"))
        .collect();
    let mut rows = vec![
        (
            "(a) command to a subscribed device",
            time_topics(&index, &commands),
        ),
        ("(a) telemetry, no match", time_topics(&index, &telemetry)),
    ];
    drop(commands);

    // (c) Workload (a) and 1,000 shared groups; telemetry of the organisations with groups.
    let members = add_groups(&mut index);
    let grouped: Vec<TopicName> = (0..)
        .map(device)
        .filter(|d| d.org < 100)
        .take(usize::try_from(SAMPLES).expect("fits"))
        .map(|d| Shape::topic(d, "/telemetry"))
        .collect();
    rows.push((
        "(c) telemetry into shared groups",
        time_topics(&index, &grouped),
    ));
    drop((index, filters, telemetry, grouped));

    // (b) The mixed workload.
    let mut index = TopicIndex::new();
    for i in 0..FILTERS {
        let (filter, subscriber) = shape.mixed_filter(i);
        index
            .insert(&filter, subscriber)
            .expect("room in the index");
    }
    index.compact();
    let mixed_per_filter = ratio(index.memory().total(), index.filters());
    writeln!(
        report,
        "(b) {} mixed filters: {mixed_per_filter:.1} bytes per filter compacted",
        index.filters()
    )
    .expect("String");
    let mixed: Vec<TopicName> = (0..SAMPLES).map(|j| shape.mixed_topic(j)).collect();
    rows.push(("(b) mixed publish stream", time_topics(&index, &mixed)));

    writeln!(
        report,
        "(c) adds {members} members of 1,000 groups to (a)\n\nmatches, {SAMPLES} topics a \
         stream, one call timed at a time:"
    )
    .expect("String");
    writeln!(
        report,
        "    {:<38} {:>7} {:>7} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "stream", "filters", "dests", "p50", "p99", "p99.9", "mean", "walk p50", "walk p99"
    )
    .expect("String");
    for (name, t) in &rows {
        writeln!(
            report,
            "    {name:<38} {:>7.2} {:>7.1} {:>9.2?} {:>9.2?} {:>9.2?} {:>9.2?} {:>9.2?} {:>9.2?}",
            t.filters_per_match,
            t.destinations_per_match,
            t.collect.quantile(0.5),
            t.collect.quantile(0.99),
            t.collect.quantile(0.999),
            t.collect.mean(),
            t.walk.quantile(0.5),
            t.walk.quantile(0.99),
        )
        .expect("String");
    }
    eprintln!("\n{report}");

    assert!(
        per_filter < BAR_BYTES,
        "{per_filter:.1} bytes per device filter, over the bar of {BAR_BYTES}"
    );
    assert!(
        mixed_per_filter < BAR_BYTES,
        "{mixed_per_filter:.1} bytes per mixed filter, over the bar of {BAR_BYTES}"
    );
}
