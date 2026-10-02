//! Spike S1 (report R4): which storage engine and which replication scheme the OpenQTT log role
//! is built on. Each subcommand runs one experiment and appends one JSON object per measured run
//! to `--out`. `run.sh` runs them all; `collect.py` folds the lines into bench/results.
//!
//! Absolute numbers belong to the machine that produced them; what carries over is the
//! comparison between candidates and the shape of each curve.

mod commit;
mod engine;
mod exp;
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
    }
}
