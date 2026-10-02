//! Spike S4: the cost of idle QUIC connections on quinn 0.11, rustls 0.23 and aws-lc-rs, with
//! TLS 1.3 and a client certificate.
//!
//! ```text
//! s4 idle      --conns 1000,10000 --endpoints 1 --profiles default,lean --out FILE
//! s4 handshake --lanes 128 --duration 20 [--cases full-pq,0rtt-cache,...] --out FILE
//! s4 server ... and s4 client ...   the two processes the experiments start
//! ```
//!
//! Server and client are separate processes, so the server's memory and CPU are its own. Report
//! R7 (docs/reports/R07-quic.md) explains the method and quotes the results.

mod alloc;
mod client;
mod config;
mod os;
mod pki;
mod server;

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

#[global_allocator]
static GLOBAL: alloc::Counting = alloc::Counting;

pub struct Args(HashMap<String, String>);

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

    pub fn num(&self, k: &str, default: u64) -> u64 {
        self.0
            .get(k)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    pub fn text(&self, k: &str, default: &str) -> String {
        self.0
            .get(k)
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }

    fn list(&self, k: &str, default: &str) -> Vec<String> {
        self.text(k, default)
            .split(',')
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect()
    }
}

fn main() {
    // One crypto provider for the process, installed before any TLS configuration exists.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let (cmd, a) = Args::parse();
    let result = match cmd.as_str() {
        "server" => server::run(&a).map(|()| Value::Null),
        "client" => client::run(&a).map(|()| Value::Null),
        "pki" => {
            pki::generate(&PathBuf::from(a.text("pki", "target/pki")), 256).map(|()| Value::Null)
        }
        "idle" => idle(&a),
        "handshake" => handshake(&a),
        _ => Err("usage: s4 idle|handshake|server|client|pki [--key value]...".into()),
    };
    match result {
        Ok(Value::Null) => {}
        Ok(results) => {
            let doc = json!({
                "spike": "S4", "experiment": cmd, "machine": machine(), "args": a.0,
                "results": results,
            });
            let text = serde_json::to_string_pretty(&doc).unwrap_or_default();
            match a.0.get("out") {
                Some(out) => {
                    std::fs::write(out, text + "\n").expect("write results");
                    eprintln!("wrote {out}");
                }
                None => println!("{text}"),
            }
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

fn machine() -> Value {
    let sh = |c: &str, args: &[&str]| {
        Command::new(c)
            .args(args)
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default()
    };
    json!({
        "cpu": os::sysctl("machdep.cpu.brand_string"),
        "cores": os::sysctl("hw.ncpu"),
        "performance_cores": os::sysctl("hw.perflevel0.physicalcpu"),
        "memory_bytes": os::sysctl("hw.memsize"),
        "os": format!("macOS {}", sh("sw_vers", &["-productVersion"])),
        "rustc": sh("rustc", &["-V"]),
        "kern.ipc.maxsockbuf": os::sysctl("kern.ipc.maxsockbuf"),
        "net.inet.udp.recvspace": os::sysctl("net.inet.udp.recvspace"),
        "net.inet.udp.maxdgram": os::sysctl("net.inet.udp.maxdgram"),
        "kern.maxfilesperproc": os::sysctl("kern.maxfilesperproc"),
        "date": sh("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"]),
        "note": "laptop numbers: server and client share one machine; absolute values are for this machine only",
    })
}

/// A child process of this binary whose stdout is JSON lines.
struct Child {
    proc: std::process::Child,
    latest: Arc<Mutex<Option<Value>>>,
    marks: Arc<Mutex<Vec<Value>>>,
}

impl Child {
    fn spawn(args: &[String]) -> Result<Child, pki::Error> {
        let mut proc = Command::new(std::env::current_exe()?)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let out = proc.stdout.take().ok_or("no stdout")?;
        let latest = Arc::new(Mutex::new(None));
        let marks = Arc::new(Mutex::new(Vec::new()));
        let (l, m) = (latest.clone(), marks.clone());
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines().map_while(Result::ok) {
                let Ok(v) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                // Lines without "t" are one-off reports (listening, ready, summary).
                if v.get("t").is_some() {
                    *l.lock().expect("lock") = Some(v);
                } else {
                    m.lock().expect("lock").push(v);
                }
            }
        });
        Ok(Child {
            proc,
            latest,
            marks,
        })
    }

    fn latest(&self) -> Value {
        self.latest
            .lock()
            .expect("lock")
            .clone()
            .unwrap_or(Value::Null)
    }

    fn wait_mark(&self, key: &str, timeout: Duration) -> Option<Value> {
        let end = Instant::now() + timeout;
        while Instant::now() < end {
            if let Some(v) = self
                .marks
                .lock()
                .expect("lock")
                .iter()
                .find(|v| v.get(key).is_some())
            {
                return Some(v.clone());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        None
    }

    /// Waits until a statistics line newer than `after` arrives, so a reading is never stale.
    fn fresh(&self, after: f64) -> Value {
        for _ in 0..400 {
            let v = self.latest();
            if v["t"].as_f64().unwrap_or(0.0) > after {
                return v;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        self.latest()
    }

    fn kill(mut self) {
        let _ = self.proc.kill();
        let _ = self.proc.wait();
    }
}

fn f(v: &Value, k: &str) -> f64 {
    v[k].as_f64().unwrap_or(0.0)
}

fn cpu(v: &Value) -> f64 {
    f(v, "user_s") + f(v, "sys_s")
}

fn pki_dir(a: &Args) -> Result<PathBuf, pki::Error> {
    let dir = PathBuf::from(a.text("pki", "target/pki"));
    if !dir.join("clients.key").exists() {
        pki::generate(&dir, 256)?;
    }
    Ok(dir)
}

/// Opens N idle connections and measures what they cost the server.
fn idle(a: &Args) -> Result<Value, pki::Error> {
    let pki = pki_dir(a)?;
    let endpoints = a.num("endpoints", 1);
    let threads = a.num("threads", 0);
    let keepalive = a.text("keepalive", "quic");
    let interval = a.num("interval", 30);
    let window = a.num("window", 60);
    let mut port = a.num("port", 15000);
    let mut rows = Vec::new();
    for profile in a.list("profiles", "default") {
        for n in a.list("conns", "1000,10000") {
            let n: u64 = n.parse()?;
            let common = |extra: &[&str]| -> Vec<String> {
                let mut v: Vec<String> = extra.iter().map(|s| (*s).to_string()).collect();
                v.extend([
                    "--pki".into(),
                    pki.display().to_string(),
                    "--port".into(),
                    port.to_string(),
                    "--endpoints".into(),
                    endpoints.to_string(),
                    "--profile".into(),
                    profile.clone(),
                ]);
                v
            };
            let server = Child::spawn(&common(&["server", "--threads", &threads.to_string()]))?;
            let listening = server
                .wait_mark("listening", Duration::from_secs(20))
                .ok_or("server did not start")?;
            std::thread::sleep(Duration::from_secs(2));
            // The server's own cost with no connections: statistics thread and idle runtime.
            let pre = server.fresh(0.0);
            std::thread::sleep(Duration::from_secs(10));
            let s0 = server.fresh(f(&pre, "t"));
            let baseline_cores = (cpu(&s0) - cpu(&pre)) / (f(&s0, "t") - f(&pre, "t"));
            let drops0 = os::udp_full_buffer_drops();
            let started = Instant::now();
            let client = Child::spawn(&common(&[
                "client",
                "--mode",
                "idle",
                "--conns",
                &n.to_string(),
                "--keepalive",
                &keepalive,
                "--interval",
                &interval.to_string(),
                "--sockets",
                &a.num("sockets", 32).to_string(),
                "--concurrency",
                &a.num("concurrency", 512).to_string(),
            ]))?;
            let ready = client.wait_mark("ready", Duration::from_secs(1800));
            let setup_seconds = started.elapsed().as_secs_f64();
            // The first statistics line after the client saw every connection up, so the setup
            // CPU covers every handshake.
            let at_ready = server.latest();
            let s_ready = server.fresh(f(&at_ready, "t"));
            // Every connection sends its first PINGREQ within one interval of being ready.
            std::thread::sleep(Duration::from_secs(interval + 5));
            let s1 = server.fresh(f(&s_ready, "t"));
            std::thread::sleep(Duration::from_secs(window));
            let s2 = server.fresh(f(&s1, "t"));
            let c2 = client.latest();
            let drops1 = os::udp_full_buffer_drops();
            client.kill();
            server.kill();
            let live = f(&s1, "live").max(1.0);
            let per = |v: &Value, k: &str| (f(v, k) - f(&s0, k)) / live;
            let dt = f(&s2, "t") - f(&s1, "t");
            let busy = cpu(&s2) - cpu(&s1);
            let keepalives = live * dt / interval as f64;
            let row = json!({
                "conns": n, "endpoints": endpoints, "server_threads": threads,
                "profile": profile, "keepalive": keepalive, "interval_s": interval,
                "listening": listening, "ready": ready,
                "setup_seconds": setup_seconds,
                "setup_per_second": n as f64 / setup_seconds,
                "setup_server_cpu_s": cpu(&s_ready) - cpu(&s0),
                "live_at_measure": f(&s1, "live"), "live_at_end": f(&s2, "live"),
                "client_at_end": c2,
                "baseline": s0,
                "heap_per_conn": per(&s1, "heap"),
                "allocations_per_conn": per(&s1, "allocs"),
                "footprint_per_conn": per(&s1, "footprint"),
                "resident_per_conn": per(&s1, "resident"),
                "heap_per_conn_at_end": per(&s2, "heap"),
                "footprint_per_conn_at_end": per(&s2, "footprint"),
                "idle_window_s": dt,
                "baseline_cpu_cores": baseline_cores,
                "idle_cpu_cores": busy / dt,
                "idle_cpu_cores_net": busy / dt - baseline_cores,
                "idle_cpu_us_per_keepalive": (busy - baseline_cores * dt) * 1e6 / keepalives,
                "pings_seen_by_server": f(&s2, "pings") - f(&s1, "pings"),
                "udp_full_buffer_drops": match (drops0, drops1) {
                    (Some(x), Some(y)) => json!(y.saturating_sub(x)),
                    _ => Value::Null,
                },
            });
            eprintln!(
                "{}",
                json!({ "conns": n, "profile": profile, "endpoints": endpoints,
                "heap_per_conn": row["heap_per_conn"], "footprint_per_conn": row["footprint_per_conn"],
                "idle_cpu_cores_net": row["idle_cpu_cores_net"],
                "us_per_keepalive": row["idle_cpu_us_per_keepalive"],
                "setup_per_second": row["setup_per_second"],
                "live": row["live_at_end"], "drops": row["udp_full_buffer_drops"] })
            );
            rows.push(row);
            port += 20;
        }
    }
    Ok(json!({ "rows": rows }))
}

/// Handshakes per second against one server thread, full, resumed and 0-RTT.
fn handshake(a: &Args) -> Result<Value, pki::Error> {
    let pki = pki_dir(a)?;
    let lanes = a.num("lanes", 128);
    let warmup = a.num("warmup", 3);
    let duration = a.num("duration", 20);
    let mut rows = Vec::new();
    // (name, client handshake, post-quantum key share, server session cache entries; 0 means
    // stateless tickets). The small cache keeps few sessions, to show what a full one costs
    // per resumption: rustls finds a taken session's place in its eviction order by scanning.
    let cases: [(&str, &str, u64, u64); 7] = [
        ("full-pq", "full", 1, 0),
        ("full-x25519", "full", 0, 0),
        ("resumed-pq", "resumed", 1, 0),
        ("resumed-x25519", "resumed", 0, 0),
        ("resumed-cache", "resumed", 1, 100_000),
        ("0rtt-cache", "0rtt", 1, 100_000),
        ("0rtt-small-cache", "0rtt", 1, 4_096),
    ];
    let only = a.list(
        "cases",
        "full-pq,full-x25519,resumed-pq,resumed-x25519,resumed-cache,0rtt-cache,0rtt-small-cache",
    );
    for (port, (name, kind, pq, sessions)) in (a.num("port", 16000)..).zip(cases) {
        if !only.iter().any(|c| c == name) {
            continue;
        }
        let server = Child::spawn(&[
            "server".into(),
            "--pki".into(),
            pki.display().to_string(),
            "--port".into(),
            port.to_string(),
            "--threads".into(),
            "1".into(),
            "--pq".into(),
            pq.to_string(),
            "--sessions".into(),
            sessions.to_string(),
            "--stats-ms".into(),
            "100".into(),
        ])?;
        let listening = server
            .wait_mark("listening", Duration::from_secs(20))
            .ok_or("server did not start")?;
        std::thread::sleep(Duration::from_secs(2));
        let base = server.fresh(0.0);
        let client = Child::spawn(&[
            "client".into(),
            "--mode".into(),
            "handshake".into(),
            "--pki".into(),
            pki.display().to_string(),
            "--port".into(),
            port.to_string(),
            "--handshake".into(),
            kind.into(),
            "--pq".into(),
            pq.to_string(),
            "--lanes".into(),
            lanes.to_string(),
            "--warmup".into(),
            warmup.to_string(),
            "--duration".into(),
            duration.to_string(),
            "--threads".into(),
            a.num("client-threads", 9).to_string(),
        ])?;
        std::thread::sleep(Duration::from_secs(warmup));
        let s1 = server.fresh(f(&base, "t"));
        std::thread::sleep(Duration::from_secs(duration));
        let s2 = server.fresh(f(&s1, "t"));
        let summary = client
            .wait_mark("summary", Duration::from_secs(60))
            .unwrap_or(Value::Null);
        client.kill();
        // Let draining connections expire; what the server then still holds is its sessions.
        std::thread::sleep(Duration::from_secs(5));
        let s3 = server.fresh(f(&s2, "t"));
        server.kill();
        let dt = f(&s2, "t") - f(&s1, "t");
        let hs = f(&s2, "handshakes") - f(&s1, "handshakes");
        let busy = cpu(&s2) - cpu(&s1);
        // Sessions the cache holds after the run, counted as CountingStore follows the cache.
        let stored = f(&s3, "live_sessions");
        let row = json!({
            "case": name, "handshake": kind, "pq": pq == 1,
            "server_resumption": if sessions > 0 { "stateful cache" } else { "stateless tickets" },
            "cache_capacity": sessions,
            "cache_preallocated": listening["cache_preallocated"],
            "server_threads": 1, "client": summary,
            "server_handshakes_per_second": hs / dt,
            "server_cpu_cores": busy / dt,
            "server_cpu_us_per_handshake": busy * 1e6 / hs.max(1.0),
            "handshakes_per_server_core_second": hs / busy.max(1e-9),
            "resumed_fraction": (f(&s2, "resumed") - f(&s1, "resumed")) / hs.max(1.0),
            "live_after_drain": f(&s3, "live"),
            "baseline_heap": f(&base, "heap"),
            "heap_after_drain": f(&s3, "heap") - f(&base, "heap"),
            "sessions_ever_stored": f(&s3, "stored_sessions"),
            "sessions_evicted": f(&s3, "evicted_sessions"),
            "stored_sessions": stored,
            "heap_per_stored_session": if stored > 0.0 {
                json!((f(&s3, "heap") - f(&base, "heap")) / stored)
            } else {
                Value::Null
            },
        });
        eprintln!(
            "{}",
            json!({ "handshake": kind, "pq": pq, "sessions": sessions,
            "per_second": row["server_handshakes_per_second"], "cpu": row["server_cpu_cores"],
            "per_core": row["handshakes_per_server_core_second"],
            "resumed": row["resumed_fraction"], "client": row["client"] })
        );
        rows.push(row);
    }
    Ok(json!({ "lanes": lanes, "rows": rows }))
}
