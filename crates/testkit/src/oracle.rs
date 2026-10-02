//! The oracle of the differential harness: OpenQTT 1.x, which is EMQX 5.8.9, run in Docker
//! with its QUIC listener on.

use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use openqtt_codec::{Disconnect, Packet};

use crate::packets::connect;
use crate::raw::{RawConnection, Target};
use crate::{Error, TestPki};

/// OpenQTT 1.0.1 as released, pinned by the digest of its multi-architecture index so every
/// run plays against the same broker.
pub const ORACLE_IMAGE: &str = "ghcr.io/openqtt/openqtt:1.0.1@sha256:c0c9348f3f67679cf8b76676bdc74a4a48586f3caeaa2ff6c39815f02c4522b8";

/// The QUIC listener's port inside the container, EMQX's default.
const QUIC_PORT: u16 = 14567;

/// Where the oracle's certificate and key are copied in the container.
const CERT_DIR: &str = "/opt/openqtt/etc/certs/differential";

/// A container the oracle runs in, removed when this is dropped.
#[derive(Debug)]
struct Container {
    name: String,
    files: PathBuf,
}

impl Drop for Container {
    fn drop(&mut self) {
        // Best effort: a container left behind is named for its process and visible in
        // `docker ps -a`.
        drop(docker(&["rm", "-f", &self.name]));
        drop(fs::remove_dir_all(&self.files));
    }
}

/// OpenQTT 1.x in a container of its own, with the QUIC listener on a loopback UDP port and a
/// server certificate from a throwaway CA, which [`target`](Self::target) trusts.
#[derive(Debug)]
pub struct Oracle {
    container: Container,
    target: Target,
}

/// Runs `docker` with `args`, and returns its standard output.
fn docker(args: &[&str]) -> Result<String, Error> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .map_err(|error| Error::Docker(format!("cannot run docker: {error}")))?;
    if !output.status.success() {
        return Err(Error::Docker(format!(
            "docker {} failed: {}",
            args.first().copied().unwrap_or_default(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

impl Oracle {
    /// Starts `image`, usually [`ORACLE_IMAGE`], pulling it first if Docker does not have it.
    /// The broker is not ready to serve when this returns; see [`ready`](Self::ready).
    ///
    /// # Errors
    ///
    /// [`Error::Docker`] when Docker cannot create, configure or start the container.
    pub fn start(image: &str) -> Result<Self, Error> {
        let pki = TestPki::new("OpenQTT differential CA")?;
        let server = pki.server(&["localhost", "127.0.0.1"])?;
        let name = format!(
            "openqtt-oracle-{}-{:x}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros()
        );
        let files = std::env::temp_dir().join(&name);
        fs::create_dir_all(&files)?;
        fs::write(files.join("server.pem"), &server.certificate_pem)?;
        fs::write(files.join("server.key"), &server.key_pem)?;
        let container = Container {
            name: name.clone(),
            files: files.clone(),
        };

        let publish = format!("127.0.0.1::{QUIC_PORT}/udp");
        let certfile = format!(
            "OPENQTT_LISTENERS__QUIC__DEFAULT__SSL_OPTIONS__CERTFILE={CERT_DIR}/server.pem"
        );
        let keyfile =
            format!("OPENQTT_LISTENERS__QUIC__DEFAULT__SSL_OPTIONS__KEYFILE={CERT_DIR}/server.key");
        let bind = format!("OPENQTT_LISTENERS__QUIC__DEFAULT__BIND=0.0.0.0:{QUIC_PORT}");
        docker(&[
            "create",
            "--name",
            &name,
            "--publish",
            &publish,
            "--env",
            "OPENQTT_LISTENERS__QUIC__DEFAULT__ENABLE=true",
            "--env",
            &bind,
            "--env",
            &certfile,
            "--env",
            &keyfile,
            image,
        ])?;
        let source = format!("{}/.", files.display());
        docker(&["cp", &source, &format!("{name}:{CERT_DIR}")])?;
        docker(&["start", &name])?;

        let port = docker(&["port", &name, &format!("{QUIC_PORT}/udp")])?;
        let addr: SocketAddr = port
            .lines()
            .find_map(|line| line.trim().parse().ok())
            .ok_or_else(|| Error::Docker(format!("no UDP port published: {port:?}")))?;
        Ok(Self {
            container,
            target: Target::new(addr, "localhost", vec![pki.ca_certificate()]),
        })
    }

    /// Waits until the broker answers a CONNECT over QUIC.
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`] when it does not within `timeout`, with the end of its log.
    pub async fn ready(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if let Ok(mut connection) = RawConnection::connect(&self.target).await
                && connection.send(connect("oracle-ready")).await.is_ok()
                && matches!(
                    connection.recv_packet(Duration::from_secs(2)).await,
                    Some(Packet::ConnAck(_))
                )
            {
                drop(connection.send(Disconnect::default()).await);
                drop(connection.closed(Duration::from_secs(2)).await);
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(Error::Timeout(format!(
            "the oracle did not answer CONNECT within {timeout:?}; its log ends:\n{}",
            self.logs()
        )))
    }

    /// Where the oracle listens, and the root its certificate chains to.
    pub fn target(&self) -> &Target {
        &self.target
    }

    /// The container's name.
    pub fn name(&self) -> &str {
        &self.container.name
    }

    /// The last lines of the broker's log, for a failure message.
    pub fn logs(&self) -> String {
        let output = Command::new("docker")
            .args(["logs", "--tail", "40", &self.container.name])
            .output();
        match output {
            Ok(output) => format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ),
            Err(error) => format!("(no log: {error})"),
        }
    }
}
