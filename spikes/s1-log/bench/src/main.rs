//! Spike S1 (report R4): which storage engine and which replication scheme the OpenQTT log role
//! is built on. Each subcommand runs one experiment and appends one JSON object per measured run
//! to `--out`. `run.sh` runs them all; `collect.py` folds the lines into bench/results.
//!
//! Absolute numbers belong to the machine that produced them; what carries over is the
//! comparison between candidates and the shape of each curve.

mod commit;
mod engine;
mod exp;
mod repl;
mod util;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};

use engine::Kind;
use exp::Load;

#[derive(Parser)]
struct Cli {
    /// Where databases are created. Wiped per run.
    #[arg(long, global = true, default_value = "/tmp/s1-data")]
    data: PathBuf,
    /// JSON lines file each run appends one record to.
    #[arg(long, global = true, default_value = "out/results.jsonl")]
    out: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// The durability floor: one write plus one sync, by primitive.
    Fsync {
        #[arg(long, value_delimiter = ',', default_value = "std-sync-data,fsync,fullfsync,barrier")]
        prims: Vec<exp::fsync::Primitive>,
        #[arg(long, value_delimiter = ',', default_value = "128,4096,65536,1048576")]
        sizes: Vec<usize>,
        #[arg(long, value_delimiter = ',', default_value = "1")]
        threads: Vec<usize>,
        #[arg(long, value_delimiter = ',', default_value = "append,prealloc")]
        modes: Vec<exp::fsync::Mode>,
        #[arg(long, default_value_t = 5.0)]
        secs: f64,
    },
    /// Experiment 1: group commit latency and throughput.
    Write {
        #[arg(long, value_delimiter = ',', default_value = "fjall,redb,rocksdb")]
        engines: Vec<Kind>,
        #[arg(long, value_delimiter = ',', default_value = "128,1024")]
        sizes: Vec<usize>,
        #[arg(long, value_delimiter = ',', default_value = "0,1000,2000")]
        windows_us: Vec<u64>,
        #[arg(long, value_delimiter = ',', default_value = "c1,c64,c1024,o10000,o50000")]
        loads: Vec<String>,
        #[arg(long, default_value_t = 2.0)]
        warmup: f64,
        #[arg(long, default_value_t = 8.0)]
        secs: f64,
    },
    /// Experiment 1, second half: one fsync for the log and the state machine, or two.
    Shared {
        #[arg(long, value_delimiter = ',', default_value = "fjall,redb,rocksdb")]
        engines: Vec<Kind>,
        #[arg(long, value_delimiter = ',', default_value = "c256,o20000")]
        loads: Vec<String>,
        #[arg(long, default_value_t = 2.0)]
        warmup: f64,
        #[arg(long, default_value_t = 8.0)]
        secs: f64,
    },
    /// Experiment 2: the claim storm.
    Claims {
        #[arg(long, value_delimiter = ',', default_value = "fjall,redb,rocksdb")]
        engines: Vec<Kind>,
        /// Sessions that exist before the storm.
        #[arg(long, default_value_t = 1_000_000)]
        sessions: u64,
        #[arg(long, value_delimiter = ',', default_value = "10000,25000,50000,100000,200000")]
        rates: Vec<u64>,
        /// Apply threads, standing in for partition state machines.
        #[arg(long, default_value_t = 4)]
        workers: usize,
        #[arg(long, default_value_t = 256)]
        cache_mb: u64,
        #[arg(long, default_value_t = 2.0)]
        warmup: f64,
        #[arg(long, default_value_t = 8.0)]
        secs: f64,
    },
    /// Experiment 3: footprint per idle session. Phase `load`, then `idle` in a new process.
    Footprint {
        #[arg(long)]
        engine: Kind,
        #[arg(long)]
        sessions: u64,
        #[arg(long)]
        phase: String,
        #[arg(long, default_value_t = 64)]
        cache_mb: u64,
        #[arg(long, default_value_t = 100_000)]
        reads: u64,
    },
    /// Experiment 4: offline queues filling and draining.
    Churn {
        #[arg(long)]
        engine: Kind,
        #[arg(long, default_value_t = 600)]
        secs: u64,
        #[arg(long, default_value_t = 10_000)]
        rate: u64,
        #[arg(long, default_value_t = 50_000)]
        sessions: u64,
        #[arg(long, default_value_t = 256)]
        body: usize,
        #[arg(long, default_value_t = 5)]
        min_off_s: u64,
        #[arg(long, default_value_t = 60)]
        max_off_s: u64,
    },
    /// Experiment 5: recovery. Phases `load`, `open`, `crash-writer`, `replay`, `remove`.
    Recovery {
        #[arg(long)]
        engine: Kind,
        #[arg(long)]
        phase: String,
        #[arg(long, default_value_t = 10)]
        gib: u64,
        #[arg(long, default_value = "clean")]
        label: String,
        #[arg(long)]
        quick_repair: bool,
        #[arg(long, default_value_t = 1_000_000)]
        entries: u64,
    },
    /// Experiment 6: commit latency and throughput of Raft against primary-backup.
    Repl {
        #[arg(long, value_delimiter = ',', default_value = "raft,pb")]
        schemes: Vec<repl::Scheme>,
        #[arg(long, value_delimiter = ',', default_value = "1,16,128")]
        groups: Vec<u32>,
        /// One-way delay per hop.
        #[arg(long, value_delimiter = ',', default_value = "1000,2000")]
        delays_us: Vec<u64>,
        /// Modelled flush time of one node's group commit.
        #[arg(long, value_delimiter = ',', default_value = "0,1000")]
        flush_us: Vec<u64>,
        /// o<writes per second> or c<writes outstanding>, across all groups.
        #[arg(long, value_delimiter = ',', default_value = "o1000,o20000")]
        loads: Vec<String>,
        #[arg(long, default_value_t = 50)]
        heartbeat_ms: u64,
        #[arg(long, default_value_t = 150)]
        election_min_ms: u64,
        #[arg(long, default_value_t = 300)]
        election_max_ms: u64,
        #[arg(long, default_value_t = 2.0)]
        warmup: f64,
        #[arg(long, default_value_t = 8.0)]
        secs: f64,
    },
    /// Experiment 6: CPU an idle cluster spends on its groups.
    Idle {
        #[arg(long, value_delimiter = ',', default_value = "raft,pb")]
        schemes: Vec<repl::Scheme>,
        #[arg(long, value_delimiter = ',', default_value = "128,256")]
        groups: Vec<u32>,
        /// heartbeat:election_min:election_max in ms, several separated by commas.
        #[arg(long, value_delimiter = ',', default_value = "50:150:300,250:1000:2000")]
        timings: Vec<String>,
        #[arg(long, default_value_t = 0)]
        delay_us: u64,
        #[arg(long, default_value_t = 20.0)]
        secs: f64,
    },
    /// What one small loopback TCP round trip costs in CPU.
    Netcost {
        #[arg(long, default_value_t = 5.0)]
        secs: f64,
        #[arg(long, default_value_t = 128)]
        size: usize,
    },
}

fn secs(s: f64) -> Duration {
    Duration::from_secs_f64(s)
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    std::fs::create_dir_all(&cli.data)?;
    match cli.cmd {
        Cmd::Fsync { prims, sizes, threads, modes, secs } => {
            exp::fsync::run(&cli.data, &cli.out, &prims, &sizes, &threads, &modes, secs)
        }
        Cmd::Write { engines, sizes, windows_us, loads, warmup, secs: s } => {
            let loads: Vec<Load> = loads.iter().map(|l| Load::parse(l)).collect();
            exp::write::run(&cli.data, &cli.out, &engines, &sizes, &windows_us, &loads, secs(warmup), secs(s))
        }
        Cmd::Shared { engines, loads, warmup, secs: s } => {
            let loads: Vec<Load> = loads.iter().map(|l| Load::parse(l)).collect();
            exp::write::shared(&cli.data, &cli.out, &engines, &loads, secs(warmup), secs(s))
        }
        Cmd::Claims { engines, sessions, rates, workers, cache_mb, warmup, secs: s } => {
            exp::claims::run(
                &cli.data, &cli.out, &engines, sessions, &rates, workers, secs(warmup), secs(s),
                cache_mb,
            )
        }
        Cmd::Footprint { engine, sessions, phase, cache_mb, reads } => match phase.as_str() {
            "load" => exp::footprint::load(&cli.data, &cli.out, engine, sessions, cache_mb),
            "idle" => exp::footprint::idle(&cli.data, &cli.out, engine, sessions, cache_mb, reads),
            "remove" => {
                exp::footprint::remove(&cli.data, engine, sessions);
                Ok(())
            }
            other => anyhow::bail!("unknown phase {other}"),
        },
        Cmd::Churn { engine, secs: s, rate, sessions, body, min_off_s, max_off_s } => {
            exp::churn::run(&cli.data, &cli.out, engine, s, rate, sessions, body, min_off_s, max_off_s)
        }
        Cmd::Recovery { engine, phase, gib, label, quick_repair, entries } => match phase.as_str() {
            "load" => exp::recovery::load(&cli.data, &cli.out, engine, gib, quick_repair),
            "open" => exp::recovery::open(&cli.data, &cli.out, engine, &label, quick_repair),
            "crash-writer" => exp::recovery::crash_writer(&cli.data, engine, quick_repair),
            "replay" => exp::recovery::replay(&cli.data, &cli.out, engine, entries),
            "remove" => {
                exp::recovery::remove(&cli.data, engine);
                Ok(())
            }
            other => anyhow::bail!("unknown phase {other}"),
        },
        Cmd::Repl {
            schemes, groups, delays_us, flush_us, loads, heartbeat_ms, election_min_ms,
            election_max_ms, warmup, secs: s,
        } => {
            let args = repl::LatencyArgs {
                schemes,
                groups,
                delays_us,
                flush_us,
                loads: loads.iter().map(|l| repl::parse_offered(l)).collect(),
                warmup: secs(warmup),
                measure: secs(s),
                timing: repl::raft::Timing {
                    heartbeat_ms,
                    election_min_ms,
                    election_max_ms,
                },
            };
            repl::latency(&cli.out, &args)
        }
        Cmd::Idle { schemes, groups, timings, delay_us, secs: s } => {
            let timings: Vec<(u64, u64, u64)> = timings
                .iter()
                .map(|t| {
                    let v: Vec<u64> = t.split(':').map(|x| x.parse().expect("ms")).collect();
                    (v[0], v[1], v[2])
                })
                .collect();
            repl::idle(&cli.out, &schemes, &groups, &timings, delay_us, s)
        }
        Cmd::Netcost { secs: s, size } => repl::netcost(&cli.out, s, size),
    }
}
