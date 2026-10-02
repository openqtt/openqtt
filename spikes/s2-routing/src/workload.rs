//! Filters and topics shaped like the production namespace, `ingest/<org>/<ns>/<device>/...`
//! (R2: the mountpoint is `ingest/${username}/` and a device's username is its certificate CN).
//!
//! Every value derives from an index through a fixed hash, so a filter can be regenerated for
//! removal without being stored, and every run sees the same population.
//!
//! The population has a long tail: organisations are Zipf distributed (exponent 1) over
//! `n / 100` organisations, so the largest holds about a tenth of all devices and the smallest
//! a handful; each organisation has one to four namespaces, again Zipf distributed.

use crate::rng::{Zipf, hash2, unit};

const NAMESPACES: [&str; 4] = ["production", "staging", "field", "lab"];

#[derive(Clone, Copy, Debug)]
pub struct Device {
    pub idx: u64,
    pub org: u32,
    pub ns: u32,
}

pub struct Shape {
    pub devices: u64,
    pub orgs: usize,
    org_zipf: Zipf,
    ns_zipf: Vec<Zipf>,
}

impl Shape {
    pub fn new(devices: u64) -> Self {
        let orgs = usize::try_from((devices / 100).max(10)).unwrap_or(usize::MAX);
        Shape {
            devices,
            orgs,
            org_zipf: Zipf::new(orgs, 1.0),
            ns_zipf: (1..=NAMESPACES.len()).map(|k| Zipf::new(k, 1.0)).collect(),
        }
    }

    pub fn namespaces_of(org: u32) -> usize {
        1 + usize::try_from(hash2(u64::from(org), 0x6e73) % NAMESPACES.len() as u64).unwrap_or(0)
    }

    pub fn device(&self, idx: u64) -> Device {
        let org = self.org_zipf.rank(unit(hash2(idx, 0x6f72)));
        let k = Self::namespaces_of(u32::try_from(org).unwrap_or(0));
        let ns = self.ns_zipf[k - 1].rank(unit(hash2(idx, 0x6e61)));
        Device {
            idx,
            org: u32::try_from(org).unwrap_or(0),
            ns: u32::try_from(ns).unwrap_or(0),
        }
    }

    pub fn org_name(org: u32, out: &mut String) {
        use std::fmt::Write;
        let _ = write!(out, "org-{org:05}");
    }

    pub fn ns_name(ns: u32) -> &'static str {
        NAMESPACES[ns as usize % NAMESPACES.len()]
    }

    /// `ingest/<org>/<ns>`.
    pub fn ns_prefix(org: u32, ns: u32, out: &mut String) {
        out.push_str("ingest/");
        Self::org_name(org, out);
        out.push('/');
        out.push_str(Self::ns_name(ns));
    }

    /// `ingest/<org>/<ns>/<device>`, the device id being 16 hexadecimal characters.
    pub fn device_prefix(d: Device, out: &mut String) {
        use std::fmt::Write;
        out.clear();
        Self::ns_prefix(d.org, d.ns, out);
        let _ = write!(out, "/{:016x}", hash2(d.idx, 0x6964));
    }

    /// What a device subscribes, mounted: its own commands.
    pub fn command_filter(d: Device, out: &mut String) {
        Self::device_prefix(d, out);
        out.push_str("/commands/#");
    }

    pub fn command_topic(d: Device, out: &mut String) {
        Self::device_prefix(d, out);
        out.push_str("/commands/firmware");
    }

    pub fn telemetry_topic(d: Device, out: &mut String) {
        Self::device_prefix(d, out);
        out.push_str("/telemetry");
    }
}

/// The kinds of filter in the mixed workload, with their share per 100,000.
pub const MIXED: [(&str, u64); 8] = [
    ("device commands, ingest/o/n/d/commands/#", 90_000),
    ("device exact, ingest/o/n/d/config", 5_000),
    ("namespace, ingest/o/n/+/telemetry", 1_500),
    ("namespace, ingest/o/n/+/events/#", 1_500),
    ("organisation, ingest/o/+/+/status", 990),
    ("organisation, ingest/o/#", 990),
    ("global, ingest/+/+/+/alerts/+ and +/+/+/+/telemetry", 15),
    ("system and everything, $SYS/brokers/+/clients/# and #", 5),
];

/// Filter `i` of the mixed workload. Device filters belong to device `i`. Wildcard filters are
/// backend subscribers: each picks an organisation uniformly, so a large organisation does not
/// collect thousands of identical subscriptions, and global ones are rare.
pub fn mixed_filter(shape: &Shape, i: u64, out: &mut String) {
    let d = shape.device(i);
    let r = hash2(i, 0x6d78) % 100_000;
    let org = u32::try_from(hash2(i, 0x6f67) % shape.orgs as u64).unwrap_or(0);
    let ns = u32::try_from(hash2(i, 0x6e67) % Shape::namespaces_of(org) as u64).unwrap_or(0);
    out.clear();
    match r {
        0..90_000 => Shape::command_filter(d, out),
        90_000..95_000 => {
            Shape::device_prefix(d, out);
            out.push_str("/config");
        }
        95_000..96_500 => {
            Shape::ns_prefix(org, ns, out);
            out.push_str("/+/telemetry");
        }
        96_500..98_000 => {
            Shape::ns_prefix(org, ns, out);
            out.push_str("/+/events/#");
        }
        98_000..98_990 => {
            out.push_str("ingest/");
            Shape::org_name(org, out);
            out.push_str("/+/+/status");
        }
        98_990..99_980 => {
            out.push_str("ingest/");
            Shape::org_name(org, out);
            out.push_str("/#");
        }
        99_980..99_995 => out.push_str(if r.is_multiple_of(2) {
            "ingest/+/+/+/alerts/+"
        } else {
            "+/+/+/+/telemetry"
        }),
        _ => out.push_str(if r.is_multiple_of(2) {
            "$SYS/brokers/+/clients/#"
        } else {
            "#"
        }),
    }
}

/// Topic `j` of the mixed publish stream: mostly device telemetry.
pub fn mixed_topic(shape: &Shape, j: u64, out: &mut String) {
    let d = shape.device(hash2(j, 0x7470) % shape.devices);
    let r = hash2(j, 0x746b) % 100;
    out.clear();
    if r >= 95 {
        use std::fmt::Write;
        let _ = write!(out, "$SYS/brokers/edge-{}/clients/{:x}/connected", r % 4, j);
        return;
    }
    Shape::device_prefix(d, out);
    out.push_str(match r {
        0..50 => "/telemetry",
        50..70 => "/commands/firmware",
        70..80 => "/config",
        80..90 => "/status",
        _ => "/events/boot",
    });
}
