//! Scenarios: a sequence of client actions and expectations, played against any broker
//! address by a [`Runner`], which records a trace per client.
//!
//! ```no_run
//! # async fn run(target: openqtt_testkit::Target) {
//! use openqtt_testkit::{Runner, Scenario, packets};
//! use openqtt_testkit::codec::{PacketType, QoS};
//!
//! let scenario = Scenario::new("qos1_publish", "A QoS 1 PUBLISH gets a PUBACK")
//!     .statements(&["MQTT-3.3.4-1"])
//!     .connect("pub", packets::connect("{ns}-pub"))
//!     .send("pub", packets::publish("{ns}/t", QoS::AtLeastOnce, 1, "hello"))
//!     .expect_type("pub", PacketType::PubAck)
//!     .disconnect("pub");
//! let outcome = Runner::new(target).run(&scenario).await;
//! assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
//! println!("{:#}", outcome.trace());
//! # }
//! ```
//!
//! Strings in the packets a scenario sends may hold `{ns}`, which the runner replaces with a
//! namespace of its own for each run: topics, filters and Client Identifiers then never meet
//! those of another run on the same broker, and the normalized trace writes `{ns}` back.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openqtt_codec::{Packet, PacketType};
use serde_json::Value;

use crate::raw::{RawConnection, Recorded, Target};
use crate::trace::{ClientLog, normalized_trace, raw_trace};

/// How long a step waits for what it expects, unless it says otherwise.
pub const DEFAULT_WAIT: Duration = Duration::from_secs(3);

/// How long [`Scenario::disconnect`] waits for the server to close the connection.
pub const CLOSE_WAIT: Duration = Duration::from_secs(2);

/// What [`Scenario::expect`] checks a record against.
pub type Check = Arc<dyn Fn(&Recorded) -> bool + Send + Sync>;

/// One step of a scenario. Each names the client it acts for; a client is a sequence of
/// connections, the last one current.
#[derive(Clone)]
#[non_exhaustive]
pub enum Step {
    /// Opens a new QUIC connection and control stream for the client.
    Open {
        /// The client.
        client: String,
    },
    /// Sends a packet, after replacing `{ns}` in its strings.
    Send {
        /// The client.
        client: String,
        /// The packet.
        packet: Packet,
    },
    /// Sends bytes as they are.
    SendBytes {
        /// The client.
        client: String,
        /// The bytes.
        bytes: Vec<u8>,
    },
    /// Waits until `count` records have arrived from the server, the connection closed, or
    /// `timeout` passed. It records and expects nothing: the trace shows what came.
    Receive {
        /// The client.
        client: String,
        /// How many records to wait for.
        count: usize,
        /// The longest wait.
        timeout: Duration,
    },
    /// Takes the next record from the server and checks it; a record that fails the check, or
    /// none in time, is a failure of the run.
    Expect {
        /// The client.
        client: String,
        /// What is expected, for the failure message.
        what: String,
        /// The check.
        check: Check,
        /// The longest wait.
        timeout: Duration,
    },
    /// Waits until the server ends the control stream or the connection closes, or `timeout`
    /// passed.
    AwaitEnd {
        /// The client.
        client: String,
        /// The longest wait.
        timeout: Duration,
    },
    /// Waits until the connection closes, or `timeout` passed.
    AwaitClose {
        /// The client.
        client: String,
        /// The longest wait.
        timeout: Duration,
    },
    /// Acknowledges every PUBLISH and PUBREL the client received since its last
    /// acknowledgement, whatever Packet Identifiers the server chose, refusing with 0x80 when
    /// `refuse` is set ([`RawConnection::acknowledge`]).
    Acknowledge {
        /// The client.
        client: String,
        /// Whether PUBACK and PUBREC refuse the messages.
        refuse: bool,
    },
    /// Sends the client's last packet again, with DUP set on a PUBLISH
    /// ([`RawConnection::repeat`]).
    Repeat {
        /// The client.
        client: String,
    },
    /// Lets time pass, for what may still arrive and what must not.
    Wait(Duration),
    /// Closes the QUIC connection with an application error code and no DISCONNECT: an
    /// abnormal close.
    Close {
        /// The client.
        client: String,
        /// The QUIC application error code.
        code: u32,
    },
}

impl fmt::Debug for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open { client } => write!(f, "open {client}"),
            Self::Send { client, packet } => write!(f, "{client} sends {}", packet.packet_type()),
            Self::SendBytes { client, bytes } => write!(f, "{client} sends {} bytes", bytes.len()),
            Self::Receive { client, count, .. } => write!(f, "{client} receives {count}"),
            Self::Expect { client, what, .. } => write!(f, "{client} expects {what}"),
            Self::AwaitEnd { client, .. } => write!(f, "{client} awaits the end of the stream"),
            Self::AwaitClose { client, .. } => write!(f, "{client} awaits the close"),
            Self::Acknowledge { client, refuse } => {
                let how = if *refuse { "refuses" } else { "acknowledges" };
                write!(f, "{client} {how} what it received")
            }
            Self::Repeat { client } => write!(f, "{client} repeats its last packet"),
            Self::Wait(duration) => write!(f, "wait {duration:?}"),
            Self::Close { client, code } => write!(f, "{client} closes with {code}"),
        }
    }
}

/// A named sequence of steps, with the R1 statements and decisions it bears on.
#[derive(Debug, Clone)]
pub struct Scenario {
    /// A short name, unique in a catalogue: the trace's file name.
    pub name: String,
    /// What the scenario shows, in a sentence.
    pub summary: String,
    /// The statements of report R1 it exercises, as `MQTT-x.y.z-n`.
    pub statements: Vec<String>,
    /// The decisions of report R1 (D1 to D32) whose difference from EMQX it shows.
    pub divergences: Vec<String>,
    /// The steps, in order.
    pub steps: Vec<Step>,
}

impl Scenario {
    /// A scenario with no steps yet.
    pub fn new(name: &str, summary: &str) -> Self {
        Self {
            name: name.to_owned(),
            summary: summary.to_owned(),
            statements: Vec::new(),
            divergences: Vec::new(),
            steps: Vec::new(),
        }
    }

    /// The R1 statements the scenario exercises.
    #[must_use]
    pub fn statements(mut self, ids: &[&str]) -> Self {
        self.statements
            .extend(ids.iter().map(|id| (*id).to_owned()));
        self
    }

    /// The R1 decisions the scenario shows.
    #[must_use]
    pub fn divergences(mut self, ids: &[&str]) -> Self {
        self.divergences
            .extend(ids.iter().map(|id| (*id).to_owned()));
        self
    }

    /// Adds a step.
    #[must_use]
    pub fn step(mut self, step: Step) -> Self {
        self.steps.push(step);
        self
    }

    /// Opens a connection for `client`.
    #[must_use]
    pub fn open(self, client: &str) -> Self {
        self.step(Step::Open {
            client: client.to_owned(),
        })
    }

    /// `client` sends a packet.
    #[must_use]
    pub fn send(self, client: &str, packet: impl Into<Packet>) -> Self {
        self.step(Step::Send {
            client: client.to_owned(),
            packet: packet.into(),
        })
    }

    /// `client` sends bytes as they are.
    #[must_use]
    pub fn send_bytes(self, client: &str, bytes: &[u8]) -> Self {
        self.step(Step::SendBytes {
            client: client.to_owned(),
            bytes: bytes.to_vec(),
        })
    }

    /// `client` waits for `count` records, [`DEFAULT_WAIT`] at most.
    #[must_use]
    pub fn receive(self, client: &str, count: usize) -> Self {
        self.receive_within(client, count, DEFAULT_WAIT)
    }

    /// `client` waits for `count` records, `timeout` at most.
    #[must_use]
    pub fn receive_within(self, client: &str, count: usize, timeout: Duration) -> Self {
        self.step(Step::Receive {
            client: client.to_owned(),
            count,
            timeout,
        })
    }

    /// The next record `client` gets must pass `check`.
    #[must_use]
    pub fn expect(
        self,
        client: &str,
        what: &str,
        check: impl Fn(&Recorded) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.step(Step::Expect {
            client: client.to_owned(),
            what: what.to_owned(),
            check: Arc::new(check),
            timeout: DEFAULT_WAIT,
        })
    }

    /// The next record `client` gets must be a packet that passes `check`.
    #[must_use]
    pub fn expect_packet(
        self,
        client: &str,
        what: &str,
        check: impl Fn(&Packet) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.expect(client, what, move |record| {
            record.received().is_some_and(&check)
        })
    }

    /// The next record `client` gets must be a packet of this type.
    #[must_use]
    pub fn expect_type(self, client: &str, packet_type: PacketType) -> Self {
        self.expect_packet(client, packet_type.name(), move |packet| {
            packet.packet_type() == packet_type
        })
    }

    /// `client` waits for the server to end the control stream or close the connection,
    /// `timeout` at most.
    #[must_use]
    pub fn await_end(self, client: &str, timeout: Duration) -> Self {
        self.step(Step::AwaitEnd {
            client: client.to_owned(),
            timeout,
        })
    }

    /// `client` waits for its connection to close, `timeout` at most.
    #[must_use]
    pub fn await_close(self, client: &str, timeout: Duration) -> Self {
        self.step(Step::AwaitClose {
            client: client.to_owned(),
            timeout,
        })
    }

    /// `client` acknowledges every PUBLISH and PUBREL received since its last acknowledgement.
    #[must_use]
    pub fn ack(self, client: &str) -> Self {
        self.step(Step::Acknowledge {
            client: client.to_owned(),
            refuse: false,
        })
    }

    /// `client` answers every PUBLISH received since its last acknowledgement with PUBACK or
    /// PUBREC 0x80, and every PUBREL with PUBCOMP.
    #[must_use]
    pub fn ack_refusing(self, client: &str) -> Self {
        self.step(Step::Acknowledge {
            client: client.to_owned(),
            refuse: true,
        })
    }

    /// `client` sends its last packet again.
    #[must_use]
    pub fn repeat(self, client: &str) -> Self {
        self.step(Step::Repeat {
            client: client.to_owned(),
        })
    }

    /// Time passes.
    #[must_use]
    pub fn wait(self, duration: Duration) -> Self {
        self.step(Step::Wait(duration))
    }

    /// `client` closes its QUIC connection with `code` and no DISCONNECT, and waits until the
    /// close is recorded.
    #[must_use]
    pub fn close(self, client: &str, code: u32) -> Self {
        self.step(Step::Close {
            client: client.to_owned(),
            code,
        })
        .await_close(client, CLOSE_WAIT)
    }

    /// Opens a connection for `client`, sends `connect`, and expects a CONNACK, whatever its
    /// reason code.
    #[must_use]
    pub fn connect(self, client: &str, connect: openqtt_codec::Connect) -> Self {
        self.open(client)
            .send(client, connect)
            .expect_type(client, PacketType::ConnAck)
    }

    /// `client` ends the connection cleanly: DISCONNECT 0x00, then, once the server has ended
    /// the control stream or [`CLOSE_WAIT`] passed, a QUIC close with code 0, since the sender of
    /// DISCONNECT closes the connection (docs/spec/mqtt-over-quic.md, section 7).
    #[must_use]
    pub fn disconnect(self, client: &str) -> Self {
        self.send(client, openqtt_codec::Disconnect::default())
            .await_end(client, CLOSE_WAIT)
            .close(client, 0)
    }
}

/// What a run of a scenario left: the records of every client, and the expectations it
/// failed.
#[derive(Debug)]
pub struct Outcome {
    /// The scenario's name.
    pub scenario: String,
    /// The namespace the run replaced `{ns}` with.
    pub namespace: String,
    /// Each client's connections and their records.
    pub clients: BTreeMap<String, ClientLog>,
    /// The expectations that failed, and the steps that could not run, in order.
    pub failures: Vec<String>,
}

impl Outcome {
    /// The normalized trace, for comparing runs.
    pub fn trace(&self) -> Value {
        normalized_trace(&self.scenario, &self.namespace, &self.clients)
    }

    /// The raw trace, with time and nothing normalized.
    pub fn raw_trace(&self) -> Value {
        raw_trace(&self.scenario, &self.clients)
    }
}

/// Plays scenarios against one broker.
#[derive(Debug)]
pub struct Runner {
    target: Target,
    runs: AtomicU64,
    seed: u64,
}

impl Runner {
    /// A runner for the broker `target` reaches.
    pub fn new(target: Target) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let seed = u64::try_from(now.as_nanos() % u128::from(u64::MAX)).unwrap_or(0)
            ^ u64::from(std::process::id()).rotate_left(32);
        Self {
            target,
            runs: AtomicU64::new(0),
            seed,
        }
    }

    /// The broker this runner plays against.
    pub fn target(&self) -> &Target {
        &self.target
    }

    /// A namespace no other run of this process, or of a recent one, uses: letters and
    /// digits only, so it fits a topic level and a Client Identifier.
    fn namespace(&self) -> String {
        let run = self.runs.fetch_add(1, Ordering::Relaxed);
        let value = self
            .seed
            .wrapping_add(run.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        format!("oq{value:016x}")
    }

    /// Plays every step of `scenario` in order, then closes every connection.
    pub async fn run(&self, scenario: &Scenario) -> Outcome {
        let namespace = self.namespace();
        let mut connections: BTreeMap<String, Vec<RawConnection>> = BTreeMap::new();
        let mut failures = Vec::new();
        for (index, step) in scenario.steps.iter().enumerate() {
            let number = index + 1;
            if let Err(failure) = self.play(step, &namespace, &mut connections).await {
                failures.push(format!("step {number} ({step:?}): {failure}"));
            }
        }
        let clients = connections
            .iter()
            .map(|(name, history)| {
                let log = history.iter().map(RawConnection::records).collect();
                (name.clone(), log)
            })
            .collect();
        Outcome {
            scenario: scenario.name.clone(),
            namespace,
            clients,
            failures,
        }
    }

    async fn play(
        &self,
        step: &Step,
        namespace: &str,
        connections: &mut BTreeMap<String, Vec<RawConnection>>,
    ) -> Result<(), String> {
        match step {
            Step::Open { client } => {
                let connection = RawConnection::connect(&self.target)
                    .await
                    .map_err(|error| format!("cannot connect: {error}"))?;
                connections
                    .entry(client.clone())
                    .or_default()
                    .push(connection);
            }
            Step::Send { client, packet } => {
                let mut packet = packet.clone();
                substitute(&mut packet, namespace);
                current(connections, client)?
                    .send(packet)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Step::SendBytes { client, bytes } => {
                current(connections, client)?
                    .send_bytes(bytes)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Step::Receive {
                client,
                count,
                timeout,
            } => {
                let connection = current(connections, client)?;
                let deadline = tokio::time::Instant::now() + *timeout;
                for _ in 0..*count {
                    let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                    match connection.recv(left).await {
                        Some(record) if matches!(record.event, Recorded::Closed(_)) => break,
                        Some(_) => {}
                        None => break,
                    }
                }
            }
            Step::Expect {
                client,
                what,
                check,
                timeout,
            } => {
                let connection = current(connections, client)?;
                match connection.recv(*timeout).await {
                    Some(record) if check(&record.event) => {}
                    Some(record) => {
                        return Err(format!("expected {what}, got {}", describe(&record.event)));
                    }
                    None => return Err(format!("expected {what}, got nothing in {timeout:?}")),
                }
            }
            Step::AwaitEnd { client, timeout } => {
                drop(current(connections, client)?.ended(*timeout).await);
            }
            Step::AwaitClose { client, timeout } => {
                // A connection still open is what the trace then shows; it is not a failure.
                drop(current(connections, client)?.closed(*timeout).await);
            }
            Step::Acknowledge { client, refuse } => {
                current(connections, client)?
                    .acknowledge(*refuse)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Step::Repeat { client } => {
                current(connections, client)?
                    .repeat()
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Step::Wait(duration) => tokio::time::sleep(*duration).await,
            Step::Close { client, code } => current(connections, client)?.close(*code),
        }
        Ok(())
    }
}

/// The current connection of `client`.
fn current<'a>(
    connections: &'a mut BTreeMap<String, Vec<RawConnection>>,
    client: &str,
) -> Result<&'a mut RawConnection, String> {
    connections
        .get_mut(client)
        .and_then(|history| history.last_mut())
        .ok_or_else(|| format!("{client} has no connection"))
}

/// A record, briefly, for a failure message.
fn describe(record: &Recorded) -> String {
    match record {
        Recorded::Received(packet) => format!("{packet:?}"),
        Recorded::Malformed { bytes, error } => {
            format!("malformed bytes {} ({error})", crate::trace::hex(bytes))
        }
        Recorded::StreamFinished => "the end of the stream".into(),
        Recorded::StreamReset(code) => format!("a stream reset with {code}"),
        Recorded::Closed(close) => format!("the close of the connection ({close:?})"),
        Recorded::Sent(_) | Recorded::SentBytes(_) => "something sent".into(),
    }
}

/// Replaces `{ns}` with `namespace` in every string of a packet that names a topic, a filter
/// or a client.
pub fn substitute(packet: &mut Packet, namespace: &str) {
    let fill = |text: &mut String| {
        if text.contains("{ns}") {
            *text = text.replace("{ns}", namespace);
        }
    };
    match packet {
        Packet::Connect(connect) => {
            fill(&mut connect.client_id);
            if let Some(username) = &mut connect.username {
                fill(username);
            }
            if let Some(will) = &mut connect.will {
                fill(&mut will.topic);
                if let Some(topic) = &mut will.properties.response_topic {
                    fill(topic);
                }
            }
        }
        Packet::Publish(publish) => {
            fill(&mut publish.topic);
            if let Some(topic) = &mut publish.properties.response_topic {
                fill(topic);
            }
        }
        Packet::Subscribe(subscribe) => {
            for subscription in &mut subscribe.subscriptions {
                fill(&mut subscription.filter);
            }
        }
        Packet::Unsubscribe(unsubscribe) => {
            for filter in &mut unsubscribe.filters {
                fill(filter);
            }
        }
        _ => {}
    }
}
