//! What every experiment shares: a seeded generator, the session key layout, latency
//! histograms, process resource usage, directory sizes and the JSON line each run emits.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use serde_json::{Value, json};

/// SplitMix64. Seeded, so a later phase can regenerate the client ids an earlier one wrote.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    pub fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }

    pub fn bytes(&mut self, n: usize) -> Vec<u8> {
        let mut v = vec![0; n];
        self.fill(&mut v);
        v
    }
}

const ID_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";

/// Client id number `i`: 16 to 64 characters of [a-z0-9], the same bytes every time it is asked
/// for, so the claim storm can address sessions the loader wrote without keeping a list.
pub fn client_id(i: u64) -> Vec<u8> {
    let mut r = Rng::new(i.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0x5EED);
    let len = 16 + r.below(49) as usize;
    (0..len)
        .map(|_| ID_ALPHABET[r.below(36) as usize])
        .collect()
}

/// Partitions in a cluster, R3's default.
pub const PARTITIONS: u64 = 256;

/// The partition a client id belongs to. FNV-1a stands in for xxh3 here; only the spread matters.
pub fn partition(cid: &[u8]) -> u16 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in cid {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    (h % PARTITIONS) as u16
}

/// Keys sort byte-wise and start with the partition, so one node's engine holds every partition
/// it replicates and each partition's keys stay contiguous: `{p:u16}{tag}{cid}...`.
pub fn key(p: u16, tag: u8, rest: &[&[u8]]) -> Vec<u8> {
    let mut k = Vec::with_capacity(3 + rest.iter().map(|r| r.len()).sum::<usize>());
    k.extend_from_slice(&p.to_be_bytes());
    k.push(tag);
    for r in rest {
        k.extend_from_slice(r);
    }
    k
}

pub const TAG_OWN: u8 = b'o';
pub const TAG_SESS: u8 = b's';
pub const TAG_QUEUE: u8 = b'q';
pub const TAG_MSG: u8 = b'm';

/// A Raft log key: group, then index, both big-endian so a group's entries sort in order.
pub fn log_key(group: u32, index: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(12);
    k.extend_from_slice(&group.to_be_bytes());
    k.extend_from_slice(&index.to_be_bytes());
    k
}

/// An `own/{cid}` value as protobuf would lay it out: owner node, epoch, connection generation,
/// session expiry, no will. 32 bytes, mostly small varints, so it compresses like the real thing.
pub fn own_value(node: u32, epoch: u64, conn_gen: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(32);
    v.push(0x08);
    v.extend_from_slice(&node.to_le_bytes());
    v.push(0x10);
    v.extend_from_slice(&epoch.to_le_bytes());
    v.push(0x18);
    v.extend_from_slice(&conn_gen.to_le_bytes());
    v.push(0x20);
    v.extend_from_slice(&3600u32.to_le_bytes());
    v.resize(32, 0);
    v
}

pub fn own_conn_gen(v: &[u8]) -> u64 {
    v.get(15..23)
        .map(|b| u64::from_le_bytes(b.try_into().expect("8 bytes")))
        .unwrap_or(0)
}

/// A small `sess/{cid}` value: two subscriptions of a device (its command topic and a broadcast
/// topic), the next packet id and the receive limits. 96 bytes.
pub fn sess_value(cid: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(96);
    v.extend_from_slice(b"\x0a\x16commands/");
    v.extend_from_slice(&cid[..cid.len().min(12)]);
    v.extend_from_slice(b"/#\x10\x01\x0a\x10fleet/broadcast/#\x10\x01\x18\x2a\x20\x80\x80\x04");
    v.resize(96, 0);
    v
}

/// Latency in microseconds, 1 us to 10 minutes at three significant digits.
pub struct Lat(Histogram<u64>);

impl Default for Lat {
    fn default() -> Self {
        Lat(Histogram::new_with_bounds(1, 600_000_000, 3).expect("valid bounds"))
    }
}

impl Lat {
    pub fn record(&mut self, d: Duration) {
        let us = (d.as_micros() as u64).clamp(1, 600_000_000);
        self.0.record(us).expect("in bounds");
    }

    pub fn add(&mut self, other: &Lat) {
        self.0.add(&other.0).expect("same bounds");
    }

    pub fn len(&self) -> u64 {
        self.0.len()
    }

    pub fn reset(&mut self) {
        self.0.reset();
    }

    pub fn summary(&self) -> Value {
        if self.0.is_empty() {
            return json!(null);
        }
        let h = &self.0;
        json!({
            "n": h.len(),
            "mean": (h.mean() * 10.0).round() / 10.0,
            "p50": h.value_at_quantile(0.50),
            "p90": h.value_at_quantile(0.90),
            "p99": h.value_at_quantile(0.99),
            "p999": h.value_at_quantile(0.999),
            "max": h.max(),
        })
    }

    pub fn quantile(&self, q: f64) -> u64 {
        self.0.value_at_quantile(q)
    }
}

/// Process-wide resource usage at one instant.
#[derive(Clone, Copy, Debug, Default)]
pub struct Usage {
    /// User plus system CPU seconds since the process started.
    pub cpu_s: f64,
    /// Resident set size, bytes.
    pub rss: u64,
    /// macOS physical footprint (what Activity Monitor shows: resident plus compressed), bytes.
    pub footprint: u64,
    /// Bytes this process caused to be written to storage, as the kernel accounts them.
    pub disk_written: u64,
    pub disk_read: u64,
}

pub fn usage() -> Usage {
    let mut u = Usage::default();
    // SAFETY: getrusage fills a plain struct we own.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) == 0 {
            let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
            u.cpu_s = tv(ru.ru_utime) + tv(ru.ru_stime);
        }
    }
    platform_usage(&mut u);
    u
}

#[cfg(target_os = "macos")]
fn platform_usage(u: &mut Usage) {
    // SAFETY: proc_pid_rusage fills a rusage_info_v2 we own; the cast is the C API's own idiom.
    unsafe {
        let mut info: libc::rusage_info_v2 = std::mem::zeroed();
        let ptr = (&mut info as *mut libc::rusage_info_v2).cast::<libc::rusage_info_t>();
        if libc::proc_pid_rusage(libc::getpid(), libc::RUSAGE_INFO_V2, ptr) == 0 {
            u.rss = info.ri_resident_size;
            u.footprint = info.ri_phys_footprint;
            u.disk_written = info.ri_diskio_byteswritten;
            u.disk_read = info.ri_diskio_bytesread;
        }
    }
}

#[cfg(not(target_os = "macos"))]
fn platform_usage(u: &mut Usage) {
    if let Ok(s) = fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(kb) = line.strip_prefix("VmRSS:") {
                u.rss = kb
                    .trim()
                    .trim_end_matches(" kB")
                    .trim()
                    .parse::<u64>()
                    .unwrap_or(0)
                    * 1024;
                u.footprint = u.rss;
            }
        }
    }
    if let Ok(s) = fs::read_to_string("/proc/self/io") {
        for line in s.lines() {
            if let Some(v) = line.strip_prefix("write_bytes:") {
                u.disk_written = v.trim().parse().unwrap_or(0);
            }
            if let Some(v) = line.strip_prefix("read_bytes:") {
                u.disk_read = v.trim().parse().unwrap_or(0);
            }
        }
    }
}

/// CPU seconds the calling thread has used.
pub fn thread_cpu_s() -> f64 {
    // SAFETY: clock_gettime fills a timespec we own.
    unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts);
        ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9
    }
}

/// Bytes under `dir`: as the files' lengths say, and as allocated on disk.
pub fn dir_size(dir: &Path) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt as _;
    let mut apparent = 0;
    let mut allocated = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = fs::read_dir(&d) else { continue };
        for e in rd.flatten() {
            let Ok(m) = e.metadata() else { continue };
            if m.is_dir() {
                stack.push(e.path());
            } else {
                apparent += m.len();
                allocated += m.blocks() * 512;
            }
        }
    }
    (apparent, allocated)
}

pub fn load_avg() -> [f64; 3] {
    let mut l = [0f64; 3];
    // SAFETY: getloadavg writes at most 3 doubles into our array.
    unsafe {
        libc::getloadavg(l.as_mut_ptr(), 3);
    }
    l.map(|x| (x * 100.0).round() / 100.0)
}

/// Removes and recreates a data directory.
pub fn fresh_dir(base: &Path, name: &str) -> PathBuf {
    let d = base.join(name);
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).expect("create data dir");
    d
}

/// Appends one run's record to the JSON lines file and echoes it.
/// The revision of the measurement code, written into every record. Records without one are
/// revision 1. Revision 2 counts window throughput by completion and keeps every window
/// request's latency through the drain, gives primary-backup's backups the same storage and
/// apply as Raft's followers, stops the fsync append mode wrapping, and makes a churn drain wait
/// for its queue to be durable before scanning it.
pub const REV: u64 = 2;

pub fn emit(out: &Path, mut record: Value) {
    if let Value::Object(m) = &mut record {
        m.insert("rev".into(), json!(REV));
        m.insert("load_avg".into(), json!(load_avg()));
        m.insert(
            "at".into(),
            json!(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
            ),
        );
    }
    let line = serde_json::to_string(&record).expect("serializable");
    println!("{line}");
    if let Some(parent) = out.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out)
        .expect("open results file");
    writeln!(f, "{line}").expect("write results");
}

/// Sleeps until `t`: the OS for most of it, then a short spin, so open-loop generators keep
/// their schedule to within a few microseconds.
pub fn sleep_until(t: Instant) {
    loop {
        let now = Instant::now();
        if now >= t {
            return;
        }
        let left = t - now;
        if left > Duration::from_micros(300) {
            std::thread::sleep(left - Duration::from_micros(200));
        } else {
            std::hint::spin_loop();
        }
    }
}

pub fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}
