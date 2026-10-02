//! What the operating system says about this process and the UDP stack: CPU time, memory
//! footprint, thread priority and dropped datagrams. macOS only, like the spike.

use std::net::SocketAddr;
use std::process::Command;

/// CPU seconds (user, system) this process has used.
pub fn cpu() -> (f64, f64) {
    // SAFETY: getrusage writes into the zeroed struct we own and reads nothing else.
    let ru = unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_SELF, &mut ru);
        ru
    };
    let tv = |t: libc::timeval| t.tv_sec as f64 + f64::from(t.tv_usec) / 1e6;
    (tv(ru.ru_utime), tv(ru.ru_stime))
}

/// (physical footprint, resident size) in bytes. The footprint counts compressed and swapped
/// pages too, so it does not shrink when macOS compresses an idle process; the resident size
/// does.
pub fn memory() -> (u64, u64) {
    // SAFETY: proc_pid_rusage fills the zeroed rusage_info_v2 we pass for our own pid.
    unsafe {
        let mut info: libc::rusage_info_v2 = std::mem::zeroed();
        let rc = libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V2,
            (&raw mut info).cast::<libc::rusage_info_t>(),
        );
        if rc != 0 {
            return (0, 0);
        }
        (info.ri_phys_footprint, info.ri_resident_size)
    }
}

/// Asks the scheduler for performance cores: this M2 Pro has six of them and four efficiency
/// cores, and a measurement thread left on an efficiency core runs at a fraction of the speed.
pub fn prefer_performance_cores() {
    // SAFETY: sets the calling thread's own QoS class; no memory is involved.
    unsafe {
        libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE, 0);
    }
}

/// UDP datagrams the kernel dropped because a socket buffer was full, system wide.
pub fn udp_full_buffer_drops() -> Option<u64> {
    let out = Command::new("netstat")
        .args(["-s", "-p", "udp"])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    text.lines()
        .find(|l| l.contains("dropped due to full socket buffers"))
        .and_then(|l| l.split_whitespace().next())
        .and_then(|n| n.parse().ok())
}

/// A UDP socket with the buffers raised as far as the kernel allows without root.
pub fn udp_socket(
    addr: SocketAddr,
    buffer: usize,
) -> std::io::Result<(std::net::UdpSocket, usize, usize)> {
    use socket2::{Domain, Protocol, Socket, Type};
    let s = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    // kern.ipc.maxsockbuf caps both; a refusal leaves the default, which is reported.
    let _ = s.set_recv_buffer_size(buffer);
    let _ = s.set_send_buffer_size(buffer);
    s.bind(&addr.into())?;
    let (r, w) = (s.recv_buffer_size()?, s.send_buffer_size()?);
    Ok((s.into(), r, w))
}

pub fn sysctl(name: &str) -> String {
    Command::new("sysctl")
        .args(["-n", name])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}
