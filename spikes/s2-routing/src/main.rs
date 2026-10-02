//! Spike S2: routing at scale. Each subcommand runs one measurement and writes JSON.
//!
//! ```text
//! s2 memory  --n 100000,1000000 --workload a|b|c --out FILE
//! s2 match   --n 1000000 --samples 1000000 --out FILE
//! s2 fanout  --out FILE
//! s2 coarsen --n 1000000 --edges 100 --samples 200000 --out FILE
//! s2 churn   --n 1000000 --edges 100 --minutes 5 --out FILE
//! ```
//!
//! Report R6 (docs/reports/R06-routing.md) explains the method and quotes the results.

mod alloc;
mod coarsen;
mod destset;
mod rng;
mod trie;
mod workload;

use std::collections::{HashMap, HashSet};
use std::process::Command;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::coarsen::{Policy, Rule, Tree, cover};
use crate::rng::{Rng, hash2};
use crate::trie::{Scratch, Trie};
use crate::workload::{MIXED, Shape, mixed_filter, mixed_topic};

#[global_allocator]
static GLOBAL: alloc::Counting = alloc::Counting;

/// Edges in the router's view, for the destinations of device filters.
const EDGES: u64 = 100;

struct Args(HashMap<String, String>);

impl Args {
    fn parse() -> (String, Args) {
        let mut it = std::env::args().skip(1);
        let cmd = it.next().unwrap_or_default();
        let mut m = HashMap::new();
        while let Some(k) = it.next() {
            let v = it.next().unwrap_or_default();
            m.insert(k.trim_start_matches("--").to_string(), v);
        }
        (cmd, Args(m))
    }

    fn list(&self, k: &str, default: &[u64]) -> Vec<u64> {
        match self.0.get(k) {
            Some(v) => v.split(',').filter_map(|x| x.parse().ok()).collect(),
            None => default.to_vec(),
        }
    }

    fn num(&self, k: &str, default: u64) -> u64 {
        self.0
            .get(k)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    fn text(&self, k: &str, default: &str) -> String {
        self.0
            .get(k)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }
}

fn sh(cmd: &str, args: &[&str]) -> String {
    Command::new(cmd)
        .args(args)
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

fn machine() -> Value {
    json!({
        "cpu": sh("sysctl", &["-n", "machdep.cpu.brand_string"]),
        "cores": sh("sysctl", &["-n", "hw.ncpu"]),
        "memory_bytes": sh("sysctl", &["-n", "hw.memsize"]),
        "os": format!("macOS {}", sh("sw_vers", &["-productVersion"])),
        "rustc": sh("rustc", &["-V"]),
        "date": sh("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"]),
        "note": "laptop numbers: absolute values are for this machine only",
    })
}

fn main() {
    let (cmd, a) = Args::parse();
    let results = match cmd.as_str() {
        "memory" => memory(&a),
        "match" => matching(&a),
        "fanout" => fanout(&a),
        "coarsen" => coarsening(&a),
        "churn" => churn(&a),
        _ => {
            eprintln!("usage: s2 memory|match|fanout|coarsen|churn [--key value]...");
            std::process::exit(2);
        }
    };
    let doc = json!({
        "spike": "S2",
        "experiment": cmd,
        "machine": machine(),
        "args": a.0,
        "results": results,
    });
    let text = serde_json::to_string_pretty(&doc).unwrap_or_default();
    let out = a.text("out", "");
    if out.is_empty() {
        println!("{text}");
    } else {
        std::fs::write(&out, text + "\n").expect("write results");
        eprintln!("wrote {out}");
    }
}

fn per(x: usize, n: usize) -> f64 {
    if n == 0 { 0.0 } else { x as f64 / n as f64 }
}

fn rate(n: u64, d: Duration) -> f64 {
    n as f64 / d.as_secs_f64()
}

/// A filter of workload `w` and its destination: (a) device commands to an edge id, (b) the
/// mixed workload to a subscriber id.
fn filter(shape: &Shape, w: &str, i: u64, f: &mut String) -> u32 {
    match w {
        "b" => {
            mixed_filter(shape, i, f);
            u32::try_from(hash2(i, 0x7375) % (1 << 20)).unwrap_or(0)
        }
        _ => {
            Shape::command_filter(shape.device(i), f);
            u32::try_from(hash2(i, 0x6564) % EDGES).unwrap_or(0)
        }
    }
}

/// Shared groups of workload (c): `$share/grp<g>/ingest/<org>/+/+/telemetry` over the hundred
/// largest organisations, two to 64 members each, every hundredth group with a thousand.
fn add_groups(t: &mut Trie, groups: u64) -> u64 {
    let mut f = String::new();
    let mut members = 0;
    for g in 0..groups {
        f.clear();
        f.push_str(&format!("$share/grp{g}/ingest/"));
        Shape::org_name(u32::try_from(g % 100).unwrap_or(0), &mut f);
        f.push_str("/+/+/telemetry");
        let m = if g % 100 == 99 {
            1000
        } else {
            2 + hash2(g, 0x6d62) % 63
        };
        for k in 0..m {
            t.insert(
                &f,
                u32::try_from(hash2(g * 10_000 + k, 0x6d6d) % (1 << 20)).unwrap_or(0),
            );
        }
        members += m;
    }
    members
}

fn shuffled_subset(n: u64, keep_one_in: u64, seed: u64) -> Vec<u64> {
    let mut v: Vec<u64> = (0..n)
        .filter(|&i| hash2(i, seed).is_multiple_of(keep_one_in))
        .collect();
    let mut r = Rng::new(seed);
    for i in (1..v.len()).rev() {
        let j = usize::try_from(r.below(i as u64 + 1)).unwrap_or(0);
        v.swap(i, j);
    }
    v
}

// --- memory, insert and remove ------------------------------------------------------------

fn memory(a: &Args) -> Value {
    let w = a.text("workload", "a");
    let groups = a.num("groups", 1000);
    let mut rows = Vec::new();
    for n in a.list("n", &[100_000, 1_000_000]) {
        let shape = Shape::new(n);
        let mut f = String::new();

        // Generating the strings alone, so that insert rates can be read net of it.
        let t = Instant::now();
        let mut chars = 0usize;
        for i in 0..n {
            filter(&shape, &w, i, &mut f);
            chars += f.len();
        }
        let gen_time = t.elapsed();

        let h0 = alloc::now();
        let mut trie = Trie::new();
        let t = Instant::now();
        for i in 0..n {
            let d = filter(&shape, &w, i, &mut f);
            trie.insert(&f, d);
        }
        let insert_time = t.elapsed();
        let group_members = if w == "c" {
            add_groups(&mut trie, groups)
        } else {
            0
        };
        let h1 = alloc::now();
        let parts = trie.parts();
        let filters = trie.filters();
        let entries = trie.entries();
        let nodes = trie.nodes();
        let levels = trie.interner.len();

        let mut depth_max: HashMap<u32, u32> = HashMap::new();
        for (d, k) in trie.fanout_histogram() {
            let e = depth_max.entry(d).or_default();
            *e = (*e).max(k);
        }
        let mut depth_max: Vec<_> = depth_max.into_iter().collect();
        depth_max.sort_unstable();

        // A tenth of the filters, in random order: removed, then inserted again.
        let some = shuffled_subset(n, 10, 0x726d);
        let t = Instant::now();
        let mut removed = 0u64;
        for &i in &some {
            let d = filter(&shape, &w, i, &mut f);
            removed += u64::from(trie.remove(&f, d));
        }
        let remove_time = t.elapsed();
        let t = Instant::now();
        for &i in &some {
            let d = filter(&shape, &w, i, &mut f);
            trie.insert(&f, d);
        }
        let reinsert_time = t.elapsed();
        let h_reinsert = alloc::now();

        trie.shrink();
        let h2 = alloc::now();
        let parts_shrunk = trie.parts();

        let t = Instant::now();
        for i in 0..n {
            let d = filter(&shape, &w, i, &mut f);
            trie.remove(&f, d);
        }
        let remove_all_time = t.elapsed();
        let left = trie.entries();
        drop(trie);
        let h3 = alloc::now();

        let bytes = h1.bytes - h0.bytes;
        let row = json!({
            "workload": w,
            "n": n,
            "filters": filters,
            "entries": entries,
            "group_members": group_members,
            "nodes": nodes,
            "levels": levels,
            "mean_filter_chars": per(chars, usize::try_from(n).unwrap_or(1)),
            "heap_bytes": bytes,
            "heap_bytes_per_filter": per(bytes, filters),
            "heap_bytes_per_entry": per(bytes, entries),
            "allocations_per_filter": per(h1.allocs - h0.allocs, filters),
            "parts": parts,
            "parts_total": parts.total(),
            "heap_bytes_after_churn": h_reinsert.bytes - h0.bytes,
            "heap_bytes_shrunk": h2.bytes - h0.bytes,
            "heap_bytes_per_filter_shrunk": per(h2.bytes - h0.bytes, filters),
            "parts_shrunk": parts_shrunk,
            "generate_seconds": gen_time.as_secs_f64(),
            "insert_seconds": insert_time.as_secs_f64(),
            "insert_per_second": rate(n, insert_time),
            "insert_per_second_net_of_generation": rate(n, insert_time.saturating_sub(gen_time)),
            "remove_tenth_random_order_per_second": rate(removed, remove_time),
            "reinsert_tenth_per_second": rate(removed, reinsert_time),
            "remove_all_per_second": rate(n, remove_all_time),
            "entries_left_after_remove_all": left,
            "heap_bytes_left_after_drop": h3.bytes.saturating_sub(h0.bytes),
            "max_children_by_depth": depth_max,
            "sizes": {
                "node": 24,
                "term": size_of::<trie::Term>(),
                "destset": size_of::<destset::DestSet>(),
            },
        });
        eprintln!("{row}");
        rows.push(row);
    }
    json!({ "rows": rows, "mixed_kinds": MIXED })
}

// --- match latency -------------------------------------------------------------------------

#[derive(Default)]
struct Lat(Vec<u32>);

impl Lat {
    fn summary(&mut self) -> Value {
        let v = &mut self.0;
        v.sort_unstable();
        let q = |p: f64| -> u32 {
            if v.is_empty() {
                return 0;
            }
            let i = ((v.len() as f64 - 1.0) * p).round() as usize;
            v[i.min(v.len() - 1)]
        };
        let mean = v.iter().map(|&x| u64::from(x)).sum::<u64>() as f64 / v.len().max(1) as f64;
        json!({
            "samples": v.len(),
            "p50_ns": q(0.5), "p90_ns": q(0.9), "p99_ns": q(0.99), "p999_ns": q(0.999),
            "max_ns": v.last().copied().unwrap_or(0), "mean_ns": mean,
        })
    }
}

fn timer_resolution() -> Value {
    let mut lat = Lat::default();
    let mut nonzero = u32::MAX;
    for _ in 0..1_000_000 {
        let t = Instant::now();
        let d = t.elapsed().as_nanos();
        let d = u32::try_from(d).unwrap_or(u32::MAX);
        if d > 0 {
            nonzero = nonzero.min(d);
        }
        lat.0.push(d);
    }
    json!({ "empty_interval": lat.summary(), "smallest_nonzero_ns": nonzero })
}

fn time_topics(trie: &Trie, topics: &[String]) -> Value {
    let mut s = Scratch::default();
    let mut out = Vec::new();
    // Warm the caches and the branch predictors on a different order.
    for t in topics.iter().rev().take(200_000) {
        trie.collect(t, 0, &mut s, &mut out);
    }
    let mut lat = Lat::default();
    let mut hits = 0usize;
    let mut dests = 0usize;
    for (j, t) in topics.iter().enumerate() {
        let start = Instant::now();
        let h = trie.collect(t, j as u64, &mut s, &mut out);
        let d = start.elapsed();
        lat.0.push(u32::try_from(d.as_nanos()).unwrap_or(u32::MAX));
        hits += h;
        dests += out.len();
    }
    let start = Instant::now();
    for (j, t) in topics.iter().enumerate() {
        trie.collect(t, j as u64, &mut s, &mut out);
    }
    let bulk = start.elapsed();
    // The trie walk alone: which filters match, without gathering their destinations.
    let mut only = Lat::default();
    let mut found = 0usize;
    for t in topics {
        let start = Instant::now();
        let mut k = 0usize;
        trie.matches(t, &mut s, |_| k += 1);
        only.0
            .push(u32::try_from(start.elapsed().as_nanos()).unwrap_or(u32::MAX));
        found += k;
    }
    json!({
        "per_call": lat.summary(),
        "match_only_per_call": only.summary(),
        "match_only_filters_found": found,
        "bulk_mean_ns": bulk.as_nanos() as f64 / topics.len() as f64,
        "mean_matching_filters": per(hits, topics.len()),
        "mean_destinations": per(dests, topics.len()),
    })
}

fn matching(a: &Args) -> Value {
    let n = a.num("n", 1_000_000);
    let samples = a.num("samples", 1_000_000);
    let groups = a.num("groups", 1000);
    let shape = Shape::new(n);
    let mut f = String::new();
    let mut rows = Vec::new();
    for w in ["a", "b", "c"] {
        let mut trie = Trie::new();
        for i in 0..n {
            let d = filter(&shape, w, i, &mut f);
            trie.insert(&f, d);
        }
        if w == "c" {
            add_groups(&mut trie, groups);
        }
        let mut streams: Vec<(&str, Vec<String>)> = Vec::new();
        let device = |j: u64| shape.device(hash2(j, 0x7464) % n);
        match w {
            "a" => {
                streams.push((
                    "command to a subscribed device (one match)",
                    (0..samples)
                        .map(|j| {
                            Shape::command_topic(device(j), &mut f);
                            f.clone()
                        })
                        .collect(),
                ));
                streams.push((
                    "telemetry (no match)",
                    (0..samples)
                        .map(|j| {
                            Shape::telemetry_topic(device(j), &mut f);
                            f.clone()
                        })
                        .collect(),
                ));
            }
            "b" => streams.push((
                "mixed publish stream",
                (0..samples)
                    .map(|j| {
                        mixed_topic(&shape, j, &mut f);
                        f.clone()
                    })
                    .collect(),
            )),
            _ => {
                // Telemetry of devices in the hundred organisations that have groups.
                let mut picked = Vec::new();
                let mut j = 0u64;
                while picked.len() < usize::try_from(samples).unwrap_or(0) {
                    let d = device(j);
                    j += 1;
                    if d.org < 100 {
                        Shape::telemetry_topic(d, &mut f);
                        picked.push(f.clone());
                    }
                }
                streams.push(("telemetry into shared groups", picked));
            }
        }
        for (name, topics) in streams {
            let r = time_topics(&trie, &topics);
            let row = json!({ "workload": w, "n": n, "filters": trie.filters(),
                "stream": name, "result": r });
            eprintln!("{row}");
            rows.push(row);
        }
    }
    json!({ "rows": rows, "timer": timer_resolution() })
}

// --- fan-out -------------------------------------------------------------------------------

fn fanout(a: &Args) -> Value {
    let background = a.num("background", 100_000);
    let shape = Shape::new(background);
    let mut f = String::new();
    let mut trie = Trie::new();
    for i in 0..background {
        let d = filter(&shape, "a", i, &mut f);
        trie.insert(&f, d);
    }
    let topic = "ingest/org-00000/production/00000000000000aa/telemetry";
    let one = "ingest/org-00000/production/+/telemetry";
    let others = ["ingest/org-00000/#", "ingest/+/+/+/telemetry"];
    let mut rows = Vec::new();
    let mut s = Scratch::default();
    let mut out = Vec::new();
    for n in a.list("dests", &[1, 10, 100, 1000, 10_000]) {
        let dest = |k: u64| u32::try_from(hash2(k, 0x666f) % (1 << 20)).unwrap_or(0);
        for k in 0..n {
            trie.insert(one, dest(k));
        }
        for overlap in [false, true] {
            if overlap {
                // Two more filters match: one shares half the destinations, one a quarter.
                for k in 0..n / 2 {
                    trie.insert(others[0], dest(k));
                }
                for k in n / 4..n / 2 {
                    trie.insert(others[1], dest(k));
                }
                for k in n..n + n / 4 {
                    trie.insert(others[1], dest(k));
                }
            }
            let iters = (2_000_000 / n.max(1)).clamp(200, 2_000_000);
            let mut sum = 0u64;
            let start = Instant::now();
            for j in 0..iters {
                trie.collect(topic, j, &mut s, &mut out);
                for &d in &out {
                    sum = sum.wrapping_add(u64::from(d));
                }
            }
            let sorted = start.elapsed();
            let got = out.len();

            // The same union through a bitmap, the alternative for large sets.
            let start = Instant::now();
            let mut acc = roaring::RoaringBitmap::new();
            for _ in 0..iters {
                acc.clear();
                trie.matches(topic, &mut s, |t| {
                    if let Some(b) = t.dests.as_bitmap() {
                        acc |= b;
                    } else {
                        let mut v = Vec::new();
                        t.dests.extend_into(&mut v);
                        acc.extend(v);
                    }
                });
                for d in &acc {
                    sum = sum.wrapping_add(u64::from(d));
                }
            }
            let bitmap = start.elapsed();
            let ns = |d: Duration| d.as_nanos() as f64 / iters as f64;
            let row = json!({
                "destinations_on_main_filter": n,
                "matching_filters": if overlap { 3 } else { 1 },
                "distinct_destinations": got,
                "sort_dedup_ns_per_publish": ns(sorted),
                "sort_dedup_ns_per_destination": ns(sorted) / got.max(1) as f64,
                "bitmap_union_ns_per_publish": ns(bitmap),
                "bitmap_union_ns_per_destination": ns(bitmap) / acc.len().max(1) as f64,
                "checksum": sum % 1000,
            });
            eprintln!("{row}");
            rows.push(row);
            if overlap {
                for k in 0..n / 2 {
                    trie.remove(others[0], dest(k));
                }
                for k in (n / 4..n / 2).chain(n..n + n / 4) {
                    trie.remove(others[1], dest(k));
                }
            }
        }
        for k in 0..n {
            trie.remove(one, dest(k));
        }
    }
    json!({ "background_filters": background, "topic": topic, "rows": rows })
}

// --- placement ------------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize)]
enum Placement {
    /// Each connection lands on a uniformly random edge, as behind an L4 load balancer.
    Uniform,
    /// Devices of one namespace share an edge, chosen by rendezvous hashing of the namespace;
    /// a namespace larger than an edge's mean load spreads over just enough edges.
    Prefix,
}

struct Placer {
    kind: Placement,
    edges: u32,
    ns_edges: HashMap<(u32, u32), Vec<u32>>,
}

impl Placer {
    fn new(kind: Placement, shape: &Shape, edges: u32) -> Self {
        let mut ns_edges = HashMap::new();
        if kind == Placement::Prefix {
            let mut counts: HashMap<(u32, u32), u64> = HashMap::new();
            for i in 0..shape.devices {
                let d = shape.device(i);
                *counts.entry((d.org, d.ns)).or_default() += 1;
            }
            // Largest namespaces first, each onto the least loaded of its first few rendezvous
            // choices, so that whole namespaces still balance the edges.
            let avg = shape.devices.div_ceil(u64::from(edges));
            let mut counts: Vec<((u32, u32), u64)> = counts.into_iter().collect();
            counts.sort_unstable_by(|x, y| y.1.cmp(&x.1).then(x.0.cmp(&y.0)));
            let mut load = vec![0u64; edges as usize];
            for ((org, ns), c) in counts {
                let k = usize::try_from(c.div_ceil(avg).clamp(1, u64::from(edges))).unwrap_or(1);
                let key = (u64::from(org) << 8) | u64::from(ns);
                let mut ranked: Vec<(u64, u32)> = (0..edges)
                    .map(|e| (hash2(key, 0x7276 ^ (u64::from(e) << 32)), e))
                    .collect();
                ranked.sort_unstable_by(|x, y| y.cmp(x));
                let mut choice: Vec<u32> = ranked[..(2 * k + 2).min(ranked.len())]
                    .iter()
                    .map(|x| x.1)
                    .collect();
                choice.sort_by_key(|&e| load[e as usize]);
                choice.truncate(k);
                for &e in &choice {
                    load[e as usize] += c / k as u64;
                }
                ns_edges.insert((org, ns), choice);
            }
        }
        Placer {
            kind,
            edges,
            ns_edges,
        }
    }

    /// The edge a device's connection reaches; `epoch` changes on every reconnect.
    fn home(&self, d: workload::Device, epoch: u64) -> u32 {
        match self.kind {
            Placement::Uniform => {
                u32::try_from(hash2(d.idx ^ (epoch << 40), 0x7570) % u64::from(self.edges))
                    .unwrap_or(0)
            }
            Placement::Prefix => {
                let v = &self.ns_edges[&(d.org, d.ns)];
                v[usize::try_from(hash2(d.idx, 0x6b) % v.len() as u64).unwrap_or(0)]
            }
        }
    }
}

fn load_stats(members: &[Vec<u32>]) -> Value {
    let loads: Vec<usize> = members.iter().map(Vec::len).collect();
    let total: usize = loads.iter().sum();
    let max = loads.iter().copied().max().unwrap_or(0);
    let min = loads.iter().copied().min().unwrap_or(0);
    let mean = per(total, loads.len());
    json!({ "mean": mean, "max": max, "min": min, "max_over_mean": max as f64 / mean })
}

/// The consumer of all telemetry, on edge 0.
const BACKEND: &str = "ingest/+/+/+/telemetry";
/// Narrow consumers, each of one large organisation's telemetry.
const NARROW: u32 = 20;

/// The edge of narrow consumer `k`, which consumes organisation `k`'s telemetry.
fn narrow_edge(k: u32, edges: u32) -> u32 {
    u32::try_from(hash2(u64::from(k), 0x636f) % u64::from(edges)).unwrap_or(0)
}

fn narrow_filter(k: u32) -> String {
    let mut f = String::from("ingest/");
    Shape::org_name(k, &mut f);
    f.push_str("/+/+/telemetry");
    f
}

/// Builds edge `e`'s exact interest: its devices' command filters, on edge 0 the consumer of
/// all telemetry, and the narrow consumers placed on this edge, which share it with devices as
/// they would behind a load balancer.
fn edge_tree(shape: &Shape, members: &[u32], e: usize, edges: u32, f: &mut String) -> Tree {
    let mut tree = Tree::default();
    for &i in members {
        Shape::command_filter(shape.device(u64::from(i)), f);
        tree.insert(f);
    }
    if e == 0 {
        tree.insert(BACKEND);
    }
    for k in 0..NARROW {
        if narrow_edge(k, edges) as usize == e {
            tree.insert(&narrow_filter(k));
        }
    }
    tree
}

// --- coarsening ----------------------------------------------------------------------------

#[derive(Default)]
struct RuleAcc {
    entries: usize,
    coarse: usize,
    max_per_edge: usize,
    view: Trie,
}

fn coarsening(a: &Args) -> Value {
    let edges = u32::try_from(a.num("edges", 100)).unwrap_or(100);
    let samples = a.num("samples", 200_000);
    let ts = a.list("t", &[1, 4, 16, 64, 256, 1024]);
    let floors = a.list("floor", &[1, 2, 3]);
    let full_view = a.num("full-view", 1) == 1;
    let mut rows = Vec::new();
    let mut f = String::new();
    for n in a.list("n", &[1_000_000]) {
        let shape = Shape::new(n);
        for kind in [Placement::Uniform, Placement::Prefix] {
            let t0 = Instant::now();
            let placer = Placer::new(kind, &shape, edges);
            let mut members = vec![Vec::new(); edges as usize];
            for i in 0..n {
                let e = placer.home(shape.device(i), 0) as usize;
                members[e].push(u32::try_from(i).expect("under 2^32 devices"));
            }
            let mut rules = Vec::new();
            for &floor in &floors {
                for policy in [Policy::Hash, Policy::Plus, Policy::Shape] {
                    for &t in &ts {
                        rules.push(Rule {
                            t: usize::try_from(t).unwrap_or(1),
                            policy,
                            floor: usize::try_from(floor).unwrap_or(1),
                            hysteresis: false,
                        });
                    }
                }
            }
            let mut acc: Vec<RuleAcc> = rules.iter().map(|_| RuleAcc::default()).collect();
            for (e, m) in members.iter().enumerate() {
                let tree = edge_tree(&shape, m, e, edges, &mut f);
                for (r, rule) in rules.iter().enumerate() {
                    let c = cover(&tree, *rule, None);
                    let x = &mut acc[r];
                    x.entries += c.filters.len();
                    x.coarse += c.coarse.len();
                    x.max_per_edge = x.max_per_edge.max(c.filters.len());
                    for &ci in &c.coarse {
                        x.view.insert(&c.filters[ci], u32::try_from(e).unwrap_or(0));
                    }
                }
            }
            let mut s = Scratch::default();
            let mut out = Vec::new();
            for (rule, x) in rules.iter().zip(&acc) {
                // Commands come from the platform through the admin API (R2 rule 21), not from
                // an edge, so every edge that gets one counts. Telemetry comes from the device's
                // own edge, which delivers locally without a lane, so that edge does not count.
                // Exact entries only reach an edge that wants the message, so the coarse
                // entries alone decide the false positives.
                let (mut cmd_fp, mut cmd_del, mut tel_fp, mut tel_del) = (0u64, 0u64, 0u64, 0u64);
                let mut wants = Vec::new();
                for j in 0..samples {
                    let d = shape.device(hash2(j, 0x5350) % n);
                    let home = placer.home(d, 0);
                    Shape::command_topic(d, &mut f);
                    x.view.collect(&f, 0, &mut s, &mut out);
                    let fp = out.iter().filter(|&&e| e != home).count() as u64;
                    cmd_fp += fp;
                    cmd_del += fp + 1;
                    wants.clear();
                    wants.push(0);
                    if d.org < NARROW {
                        wants.push(narrow_edge(d.org, edges));
                    }
                    wants.retain(|&e| e != home);
                    wants.sort_unstable();
                    wants.dedup();
                    Shape::telemetry_topic(d, &mut f);
                    x.view.collect(&f, 0, &mut s, &mut out);
                    let fp = out
                        .iter()
                        .filter(|&&e| e != home && !wants.contains(&e))
                        .count() as u64;
                    tel_fp += fp;
                    tel_del += fp + wants.len() as u64;
                }
                let mut examples: Vec<String> = Vec::new();
                x.view.for_each_entry(|flt, _| {
                    if examples.len() < 3 && !examples.iter().any(|x| x == flt) {
                        examples.push(flt.to_string());
                    }
                });
                rows.push(json!({
                    "n": n, "edges": edges, "placement": kind, "rule": rule,
                    "route_entries": x.entries,
                    "coarse_entries": x.coarse,
                    "exact_entries": x.entries - x.coarse,
                    "route_entries_per_device": per(x.entries, usize::try_from(n).unwrap_or(1)),
                    "max_entries_per_edge": x.max_per_edge,
                    "command_false_positive_rate": cmd_fp as f64 / cmd_del.max(1) as f64,
                    "command_remote_deliveries_per_message": cmd_del as f64 / samples as f64,
                    "telemetry_false_positive_rate": tel_fp as f64 / tel_del.max(1) as f64,
                    "telemetry_remote_deliveries_per_message": tel_del as f64 / samples as f64,
                    "example_coarse_filters": examples,
                }));
            }
            let row = json!({ "n": n, "placement": kind, "organisations": shape.orgs,
                "load": load_stats(&members), "seconds": t0.elapsed().as_secs_f64() });
            eprintln!("{row}");
            rows.push(row);

            // The merged route view every edge holds, built in full for two rules, to measure
            // bytes per route entry rather than assume the device-filter figure.
            if full_view {
                for rule in [
                    Rule {
                        t: 64,
                        policy: Policy::Plus,
                        floor: 3,
                        hysteresis: false,
                    },
                    Rule {
                        t: 64,
                        policy: Policy::Shape,
                        floor: 1,
                        hysteresis: false,
                    },
                ] {
                    let h0 = alloc::now();
                    let mut view = Trie::new();
                    for (e, m) in members.iter().enumerate() {
                        let tree = edge_tree(&shape, m, e, edges, &mut f);
                        let c = cover(&tree, rule, None);
                        for flt in &c.filters {
                            view.insert(flt, u32::try_from(e).unwrap_or(0));
                        }
                    }
                    let h1 = alloc::now();
                    let row = json!({ "n": n, "placement": kind, "full_view_rule": rule,
                        "entries": view.entries(), "filters": view.filters(),
                        "heap_bytes": h1.bytes - h0.bytes,
                        "heap_bytes_per_entry": per(h1.bytes - h0.bytes, view.entries()) });
                    eprintln!("{row}");
                    rows.push(row);
                }
            }
        }
    }
    let narrow: Vec<Value> = (0..NARROW)
        .map(|k| json!({ "filter": narrow_filter(k), "edge_of_100": narrow_edge(k, 100) }))
        .collect();
    json!({ "consumer_of_all_telemetry_on_edge_0": BACKEND, "narrow_consumers": narrow,
        "rows": rows })
}

// --- churn ---------------------------------------------------------------------------------

fn varint_len(v: u64) -> usize {
    let bits = 64 - (v | 1).leading_zeros() as usize;
    bits.div_ceil(7)
}

/// A route-view change on the wire: operation, sequence number, edge, then the filter.
fn record_len(seq: u64, edge: u32, filter: &str) -> usize {
    1 + varint_len(seq)
        + varint_len(u64::from(edge))
        + varint_len(filter.len() as u64)
        + filter.len()
}

/// A route-view record: added or withdrawn, by which edge, which filter.
type Record = (bool, u32, String);

/// One namespace on one edge. With a floor at the namespace level the cover of an edge is the
/// union of its namespaces' covers, so it can be kept up to date one namespace at a time.
#[derive(Default)]
struct Cell {
    members: Vec<u32>,
    coarse: bool,
}

/// The cover of every edge under a floor-3 rule, maintained change by change.
struct Covers<'a> {
    shape: &'a Shape,
    rule: Rule,
    cells: Vec<HashMap<(u32, u32), Cell>>,
    transitions: usize,
}

impl Covers<'_> {
    fn exact(&self, i: u32) -> String {
        let mut s = String::new();
        Shape::command_filter(self.shape.device(u64::from(i)), &mut s);
        s
    }

    fn coarse(&self, key: (u32, u32)) -> String {
        let mut s = String::new();
        Shape::ns_prefix(key.0, key.1, &mut s);
        s.push_str(match self.rule.policy {
            Policy::Plus | Policy::Shape => "/+/commands/#",
            Policy::Hash => "/#",
        });
        s
    }

    fn rule_stays_coarse(&self, count: usize) -> bool {
        count > self.rule.t || (self.rule.hysteresis && count > self.rule.t / 2)
    }

    fn add(&mut self, i: u32, e: u32, out: &mut Vec<Record>) {
        let d = self.shape.device(u64::from(i));
        let key = (d.org, d.ns);
        let cell = self.cells[e as usize].entry(key).or_default();
        cell.members.push(i);
        if cell.coarse {
            return;
        }
        if cell.members.len() > self.rule.t {
            let members = cell.members.clone();
            cell.coarse = true;
            self.transitions += 1;
            for &m in &members[..members.len() - 1] {
                out.push((false, e, self.exact(m)));
            }
            out.push((true, e, self.coarse(key)));
        } else {
            out.push((true, e, self.exact(i)));
        }
    }

    fn remove(&mut self, i: u32, e: u32, out: &mut Vec<Record>) {
        let d = self.shape.device(u64::from(i));
        let key = (d.org, d.ns);
        let Some(cell) = self.cells[e as usize].get_mut(&key) else {
            return;
        };
        let Some(p) = cell.members.iter().position(|&m| m == i) else {
            return;
        };
        cell.members.swap_remove(p);
        let left = cell.members.len();
        let coarse = cell.coarse;
        let stays = self.rule_stays_coarse(left);
        let Some(cell) = self.cells[e as usize].get_mut(&key) else {
            return;
        };
        if coarse {
            if left == 0 {
                self.cells[e as usize].remove(&key);
                out.push((false, e, self.coarse(key)));
            } else if !stays {
                let members = cell.members.clone();
                cell.coarse = false;
                self.transitions += 1;
                out.push((false, e, self.coarse(key)));
                for m in members {
                    out.push((true, e, self.exact(m)));
                }
            }
        } else {
            if left == 0 {
                self.cells[e as usize].remove(&key);
            }
            out.push((false, e, self.exact(i)));
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Event {
    Disconnect,
    Reconnect,
    /// The grace period of a departed device's entry ends; carries the token it was set with.
    Expire(u32, u32),
}

const NOWHERE: u32 = u32::MAX;

/// Event-driven churn for floor-3 rules: every change is a record the moment it happens.
fn churn_events(
    shape: &Shape,
    placer: &Placer,
    rule: Rule,
    minutes: u64,
    per_mille: u64,
    grace_ms: u64,
) -> Value {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    let n = shape.devices;
    let nu = usize::try_from(n).unwrap_or(0);
    let edges = placer.edges;
    let mut covers = Covers {
        shape,
        rule,
        cells: (0..edges).map(|_| HashMap::new()).collect(),
        transitions: 0,
    };
    let mut home = vec![NOWHERE; nu];
    let mut held = vec![NOWHERE; nu];
    let mut token = vec![0u32; nu];
    let mut epoch = vec![0u64; nu];
    let mut snapshot = Vec::new();
    for i in 0..n {
        let e = placer.home(shape.device(i), 0);
        let iu = usize::try_from(i).unwrap_or(0);
        home[iu] = e;
        covers.add(u32::try_from(i).unwrap_or(0), e, &mut snapshot);
    }
    // Withdrawals cancel additions made earlier during the load; keep the final state.
    let mut state: HashSet<(u32, String)> = HashSet::new();
    for (add, e, f) in snapshot {
        if add {
            state.insert((e, f));
        } else {
            state.remove(&(e, f));
        }
    }
    covers.transitions = 0;
    let mut seq = 1_000_000u64;
    let snapshot_bytes: usize = state
        .iter()
        .map(|(e, f)| {
            seq += 1;
            record_len(seq, *e, f)
        })
        .sum();
    let h0 = alloc::now();
    let mut view = Trie::new();
    for (e, f) in &state {
        view.insert(f, *e);
    }
    let view_bytes = alloc::now().bytes - h0.bytes;
    let view_entries = view.entries();
    drop(state);

    let mut queue: BinaryHeap<Reverse<(u64, Event, u32)>> = BinaryHeap::new();
    for m in 0..minutes {
        let mut seen = HashSet::new();
        for j in 0..n * per_mille / 1000 {
            let i = hash2((m + 1) * n + j, 0x6368) % n;
            if seen.insert(i) {
                let at = m * 60_000 + hash2(i ^ (m << 40), 0x6174) % 60_000;
                let i = u32::try_from(i).unwrap_or(0);
                queue.push(Reverse((at, Event::Disconnect, i)));
                queue.push(Reverse((at + 5_000, Event::Reconnect, i)));
            }
        }
    }
    let mut per_minute: Vec<Vec<Record>> = (0..minutes).map(|_| Vec::new()).collect();
    let mut per_second: HashMap<u64, usize> = HashMap::new();
    let mut out = Vec::new();
    while let Some(Reverse((at, ev, i))) = queue.pop() {
        let iu = i as usize;
        out.clear();
        match ev {
            Event::Disconnect => {
                let e = home[iu];
                if e == NOWHERE {
                    continue;
                }
                home[iu] = NOWHERE;
                if grace_ms == 0 {
                    covers.remove(i, e, &mut out);
                } else {
                    held[iu] = e;
                    token[iu] += 1;
                    queue.push(Reverse((at + grace_ms, Event::Expire(e, token[iu]), i)));
                }
            }
            Event::Reconnect => {
                if home[iu] != NOWHERE {
                    continue;
                }
                epoch[iu] += 1;
                let e = placer.home(shape.device(u64::from(i)), epoch[iu]);
                home[iu] = e;
                if held[iu] == e {
                    // Back on the edge that still holds its entry: nothing changes.
                    held[iu] = NOWHERE;
                    token[iu] += 1;
                } else {
                    covers.add(i, e, &mut out);
                }
            }
            Event::Expire(e, t) => {
                if token[iu] == t && held[iu] == e {
                    held[iu] = NOWHERE;
                    covers.remove(i, e, &mut out);
                }
            }
        }
        let minute = usize::try_from(at / 60_000).unwrap_or(0);
        if minute < per_minute.len() {
            *per_second.entry(at / 1000).or_default() += out.len();
            per_minute[minute].append(&mut out);
        }
    }
    let mut rows = Vec::new();
    for (m, records) in per_minute.iter().enumerate() {
        let mut bytes = 0usize;
        for (_, e, f) in records {
            seq += 1;
            bytes += record_len(seq, *e, f);
        }
        let start = Instant::now();
        for (add, e, f) in records {
            if *add {
                view.insert(f, *e);
            } else {
                view.remove(f, *e);
            }
        }
        let apply = start.elapsed();
        let m64 = m as u64;
        let peak = (m64 * 60..(m64 + 1) * 60)
            .map(|s| per_second.get(&s).copied().unwrap_or(0))
            .max()
            .unwrap_or(0);
        rows.push(json!({
            "minute": m + 1,
            "records": records.len(),
            "adds": records.iter().filter(|r| r.0).count(),
            "bytes": bytes,
            "peak_records_in_one_second": peak,
            "apply_seconds": apply.as_secs_f64(),
            "apply_ns_per_record": apply.as_nanos() as f64 / records.len().max(1) as f64,
        }));
    }
    // The first minute starts with every device connected and none yet back, so it is the
    // least representative; the mean is over the rest.
    let steady: Vec<&Value> = rows.iter().skip(1).collect();
    let mean = |k: &str| {
        steady.iter().filter_map(|m| m[k].as_f64()).sum::<f64>() / steady.len().max(1) as f64
    };
    json!({
        "method": "events",
        "grace_seconds": grace_ms / 1000,
        "view_entries": view_entries,
        "view_heap_bytes": view_bytes,
        "snapshot_bytes": snapshot_bytes,
        "mean_records_per_minute": mean("records"),
        "mean_bytes_per_minute": mean("bytes"),
        "mean_peak_records_in_one_second": mean("peak_records_in_one_second"),
        "mean_apply_seconds_per_minute": mean("apply_seconds"),
        "mean_apply_ns_per_record": mean("apply_ns_per_record"),
        "coarse_transitions": covers.transitions,
        "minutes": rows,
    })
}

/// Churn for any rule, by recomputing every edge's cover once a minute: the net change a
/// stream batched per minute would carry. Used for the floor-1 rule, whose cover is not a union
/// of independent namespaces.
fn churn_recompute(
    shape: &Shape,
    placer: &Placer,
    rule: Rule,
    minutes: u64,
    per_mille: u64,
) -> Value {
    let n = shape.devices;
    let nu = usize::try_from(n).unwrap_or(0);
    let edges = placer.edges as usize;
    let mut f = String::new();
    let mut home = vec![0u32; nu];
    let mut members = vec![Vec::new(); edges];
    let mut pos = vec![0u32; nu];
    for i in 0..n {
        let e = placer.home(shape.device(i), 0);
        let iu = usize::try_from(i).unwrap_or(0);
        home[iu] = e;
        pos[iu] = u32::try_from(members[e as usize].len()).unwrap_or(0);
        members[e as usize].push(u32::try_from(i).unwrap_or(0));
    }
    let h0 = alloc::now();
    let mut view = Trie::new();
    let mut states: Vec<HashSet<String>> = Vec::new();
    for (e, m) in members.iter().enumerate() {
        let c = cover(&edge_tree(shape, m, e, placer.edges, &mut f), rule, None);
        for flt in &c.filters {
            view.insert(flt, u32::try_from(e).unwrap_or(0));
        }
        states.push(c.filters.into_iter().collect());
    }
    let view_bytes = alloc::now().bytes - h0.bytes;
    let view_entries = view.entries();
    let mut seq = 1_000_000u64;
    let mut rows = Vec::new();
    for minute in 1..=minutes {
        let mut moved = HashSet::new();
        for j in 0..n * per_mille / 1000 {
            let i = hash2(minute * n + j, 0x6368) % n;
            if !moved.insert(i) {
                continue;
            }
            let iu = usize::try_from(i).unwrap_or(0);
            let old = home[iu] as usize;
            let p = pos[iu] as usize;
            let last = *members[old].last().expect("member present");
            members[old].swap_remove(p);
            if last != u32::try_from(i).unwrap_or(0) {
                pos[last as usize] = u32::try_from(p).unwrap_or(0);
            }
            let new = placer.home(shape.device(i), minute);
            home[iu] = new;
            pos[iu] = u32::try_from(members[new as usize].len()).unwrap_or(0);
            members[new as usize].push(u32::try_from(i).unwrap_or(0));
        }
        let mut records: Vec<Record> = Vec::new();
        let mut bytes = 0usize;
        for (e, m) in members.iter().enumerate() {
            let c = cover(&edge_tree(shape, m, e, placer.edges, &mut f), rule, None);
            let next: HashSet<String> = c.filters.into_iter().collect();
            let e32 = u32::try_from(e).unwrap_or(0);
            for x in states[e].difference(&next) {
                seq += 1;
                bytes += record_len(seq, e32, x);
                records.push((false, e32, x.clone()));
            }
            for x in next.difference(&states[e]) {
                seq += 1;
                bytes += record_len(seq, e32, x);
                records.push((true, e32, x.clone()));
            }
            states[e] = next;
        }
        let start = Instant::now();
        for (add, e, x) in &records {
            if *add {
                view.insert(x, *e);
            } else {
                view.remove(x, *e);
            }
        }
        let apply = start.elapsed();
        rows.push(json!({
            "minute": minute, "records": records.len(), "bytes": bytes,
            "apply_seconds": apply.as_secs_f64(),
        }));
    }
    let mean =
        |k: &str| rows.iter().filter_map(|m| m[k].as_f64()).sum::<f64>() / rows.len().max(1) as f64;
    json!({
        "method": "recompute each minute",
        "view_entries": view_entries,
        "view_heap_bytes": view_bytes,
        "mean_records_per_minute": mean("records"),
        "mean_bytes_per_minute": mean("bytes"),
        "mean_apply_seconds_per_minute": mean("apply_seconds"),
        "minutes": rows,
    })
}

fn churn(a: &Args) -> Value {
    let n = a.num("n", 1_000_000);
    let edges = u32::try_from(a.num("edges", 100)).unwrap_or(100);
    let minutes = a.num("minutes", 6);
    let per_mille = a.num("churn-per-mille", 10);
    let shape = Shape::new(n);
    let mut rows = Vec::new();
    for kind in [Placement::Uniform, Placement::Prefix] {
        let placer = Placer::new(kind, &shape, edges);
        for t in a.list("t", &[16, 64, 256]) {
            for hysteresis in [false, true] {
                for grace in a.list("grace", &[0, 30]) {
                    let rule = Rule {
                        t: usize::try_from(t).unwrap_or(1),
                        policy: Policy::Plus,
                        floor: 3,
                        hysteresis,
                    };
                    let r = churn_events(&shape, &placer, rule, minutes, per_mille, grace * 1000);
                    let row = json!({ "n": n, "edges": edges, "placement": kind, "rule": rule,
                        "churn_per_minute": per_mille as f64 / 1000.0, "result": r });
                    eprintln!(
                        "{}",
                        json!({ "placement": kind, "rule": rule, "grace": grace,
                        "records": row["result"]["mean_records_per_minute"],
                        "bytes": row["result"]["mean_bytes_per_minute"],
                        "apply_s": row["result"]["mean_apply_seconds_per_minute"] })
                    );
                    rows.push(row);
                }
            }
        }
        for t in [16, 64] {
            let rule = Rule {
                t,
                policy: Policy::Shape,
                floor: 1,
                hysteresis: false,
            };
            let r = churn_recompute(&shape, &placer, rule, minutes, per_mille);
            let row = json!({ "n": n, "edges": edges, "placement": kind, "rule": rule,
                "churn_per_minute": per_mille as f64 / 1000.0, "result": r });
            eprintln!(
                "{}",
                json!({ "placement": kind, "rule": rule,
                "records": row["result"]["mean_records_per_minute"],
                "bytes": row["result"]["mean_bytes_per_minute"] })
            );
            rows.push(row);
        }
    }
    json!({ "rows": rows })
}
