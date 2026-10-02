//! The floor under every engine: what one durable write costs on this machine, by primitive.
//!
//! On macOS, `fsync()` only hands data to the drive; `F_FULLFSYNC` also flushes the drive's
//! cache and is the only one that survives power loss. Rust's `sync_data` and `sync_all` use
//! `F_FULLFSYNC` there. On Linux, `fdatasync` flushes the device cache and is the real thing.

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt as _;
use std::os::unix::io::AsRawFd as _;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Result;
use serde_json::json;

use crate::util::{Lat, Rng, emit, fresh_dir};

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Primitive {
    /// `File::sync_data` from std: F_FULLFSYNC on macOS, fdatasync on Linux.
    StdSyncData,
    /// `fsync(2)`: on macOS, not a cache flush.
    Fsync,
    /// `fcntl(F_FULLFSYNC)`, macOS only.
    Fullfsync,
    /// `fcntl(F_BARRIERFSYNC)`, macOS only: orders writes without waiting for the flush.
    Barrier,
}

fn sync(f: &std::fs::File, p: Primitive) -> std::io::Result<()> {
    match p {
        Primitive::StdSyncData => f.sync_data(),
        Primitive::Fsync => {
            // SAFETY: fsync on a descriptor we own.
            let r = unsafe { libc::fsync(f.as_raw_fd()) };
            if r == 0 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
        }
        #[cfg(target_os = "macos")]
        Primitive::Fullfsync | Primitive::Barrier => {
            let cmd = if p == Primitive::Fullfsync {
                libc::F_FULLFSYNC
            } else {
                libc::F_BARRIERFSYNC
            };
            // SAFETY: fcntl on a descriptor we own.
            let r = unsafe { libc::fcntl(f.as_raw_fd(), cmd) };
            if r != -1 { Ok(()) } else { Err(std::io::Error::last_os_error()) }
        }
        #[cfg(not(target_os = "macos"))]
        Primitive::Fullfsync | Primitive::Barrier => f.sync_data(),
    }
}

/// One thread appending `size` bytes and syncing, `secs` long; or `threads` of them, each on its
/// own file, to see whether the device serialises their flushes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Mode {
    /// Every write extends the file, so every sync also persists a new file size.
    Append,
    /// Writes go to a region written and synced beforehand, so the size never changes.
    Prealloc,
}

#[allow(clippy::too_many_arguments)]
pub fn run(
    data: &Path,
    out: &Path,
    prims: &[Primitive],
    sizes: &[usize],
    threads: &[usize],
    modes: &[Mode],
    secs: f64,
) -> Result<()> {
    let dir = fresh_dir(data, "fsync");
    for &mode in modes {
    for &p in prims {
        for &size in sizes {
            for &t in threads {
                let deadline = Instant::now() + Duration::from_secs_f64(secs);
                let hs: Vec<_> = (0..t)
                    .map(|i| {
                        let path = dir.join(format!("f{i}"));
                        std::thread::spawn(move || -> std::io::Result<(Lat, u64)> {
                            let f = OpenOptions::new()
                                .create(true)
                                .truncate(true)
                                .read(true)
                                .write(true)
                                .open(&path)?;
                            let mut rng = Rng::new(i as u64);
                            let buf = rng.bytes(size);
                            if mode == Mode::Prealloc {
                                let zeros = vec![0u8; 1 << 20];
                                for k in 0..80u64 {
                                    f.write_all_at(&zeros, k << 20)?;
                                }
                                f.sync_all()?;
                            }
                            let mut lat = Lat::default();
                            let mut off = 0u64;
                            let mut n = 0;
                            while Instant::now() < deadline {
                                let t0 = Instant::now();
                                f.write_all_at(&buf, off)?;
                                sync(&f, p)?;
                                lat.record(t0.elapsed());
                                off += size as u64;
                                n += 1;
                                // Keep files small: rewind every 64 MiB.
                                if off > 64 << 20 {
                                    off = 0;
                                }
                            }
                            Ok((lat, n))
                        })
                    })
                    .collect();
                let mut lat = Lat::default();
                let mut n = 0;
                for h in hs {
                    let (l, k) = h.join().expect("thread")?;
                    lat.add(&l);
                    n += k;
                }
                emit(
                    out,
                    json!({
                        "exp": "fsync",
                        "primitive": format!("{p:?}"),
                        "mode": format!("{mode:?}"),
                        "size": size,
                        "threads": t,
                        "secs": secs,
                        "syncs": n,
                        "per_s": (n as f64 / secs).round(),
                        "lat_us": lat.summary(),
                    }),
                );
            }
        }
    }
    }
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
