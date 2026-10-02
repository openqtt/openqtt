//! A small broker around one machine, so that a test reads as a script: the client sends
//! packets, the clock moves, and the test reads what the client received and what the machine
//! asked of the edge.
//!
//! The harness plays the edge. It answers every request at once unless a test turns that off
//! for a kind of request: authentication succeeds, authorization follows a rule, the claim
//! finds the session the test stored, commits succeed, and a PUBLISH reaches the session's own
//! subscriptions and the retained store, as a one-session broker would. Answers go in the order
//! the requests came, after the effects of the input that asked for them, as an edge's would.
//! Timers fire when the clock passes them.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use bytes::Bytes;
use openqtt_codec::{
    ConnAck, Connect, ConnectProperties, Packet, PacketId, PubAck, PubComp, PubRec, PubRel,
    Publish, QoS, Subscribe, SubscribeProperties, Subscription, SubscriptionOptions, Unsubscribe,
    UnsubscribeProperties,
};
use openqtt_core::{Message, SubOpts, Timestamp, TopicFilter, TopicName};

use crate::{
    Action, AuthResult, Authentication, Authorization, Claim, ClaimResult, CloseCode, Config,
    Counter, Decision, Delivery, Effect, Input, Peer, Publication, PublishOutcome, Release,
    Session, SessionState, StreamId, Timer, WillOrder,
};

/// When the scripts start: 2026-10-01T00:00:00Z.
pub(crate) const START: u64 = 1_790_000_000;

/// A moment `seconds` after [`START`].
pub(crate) fn at(seconds: u64) -> Timestamp {
    Timestamp::from_unix_nanos((START + seconds) * 1_000_000_000)
}

/// What the harness answers by itself.
#[derive(Debug, Clone)]
pub(crate) struct Auto {
    /// Answer authentication with success.
    pub(crate) authenticate: bool,
    /// Answer authorization with this rule, or leave it to the test.
    pub(crate) authorize: Option<fn(&Action) -> Decision>,
    /// Answer the claim, with the session stored in [`Harness::stored`].
    pub(crate) claim: bool,
    /// Answer commits: accepted, matched when a subscription of the session matched.
    pub(crate) commit: bool,
    /// Deliver what the session publishes to its own subscriptions, and keep retained messages.
    pub(crate) loopback: bool,
    /// Answer retained reads from the retained store.
    pub(crate) retained: bool,
}

impl Default for Auto {
    fn default() -> Self {
        Self {
            authenticate: true,
            authorize: Some(|_| Decision::Allow),
            claim: true,
            commit: true,
            loopback: true,
            retained: true,
        }
    }
}

/// One machine and the edge around it.
pub(crate) struct Harness {
    pub(crate) session: Session,
    pub(crate) now: Timestamp,
    pub(crate) auto: Auto,
    /// Every effect, in order.
    pub(crate) log: Vec<Effect>,
    /// Packets the client received and the test has not read, with their streams.
    pub(crate) received: VecDeque<(StreamId, Packet)>,
    /// Requests the harness did not answer.
    pub(crate) authentications: Vec<Authentication>,
    pub(crate) authorizations: Vec<Authorization>,
    pub(crate) claims: Vec<Claim>,
    pub(crate) commits: Vec<Publication>,
    pub(crate) retained_reads: Vec<TopicFilter>,
    /// The session a claim finds, for a resumed session.
    pub(crate) stored: Option<SessionState>,
    /// The session's interest, by mounted filter.
    pub(crate) interest: BTreeMap<TopicFilter, SubOpts>,
    /// The retained messages, by topic.
    pub(crate) retained: BTreeMap<TopicName, Message>,
    /// Timers set and not fired or cancelled.
    pub(crate) timers: BTreeMap<Timer, Timestamp>,
    /// Whether the machine asked the transport to stop reading.
    pub(crate) paused: bool,
    /// Answers waiting to go in.
    answers: VecDeque<Input>,
}

impl Harness {
    /// A machine for a new connection with this configuration, at [`START`].
    pub(crate) fn with(config: Config) -> Self {
        Self::with_peer(config, Peer::new(7))
    }

    /// A machine for a new connection from this peer.
    pub(crate) fn with_peer(config: Config, peer: Peer) -> Self {
        let now = at(0);
        let (session, effects) = Session::new(config, peer, now);
        let mut harness = Self {
            session,
            now,
            auto: Auto::default(),
            log: Vec::new(),
            received: VecDeque::new(),
            authentications: Vec::new(),
            authorizations: Vec::new(),
            claims: Vec::new(),
            commits: Vec::new(),
            retained_reads: Vec::new(),
            stored: None,
            interest: BTreeMap::new(),
            retained: BTreeMap::new(),
            timers: BTreeMap::new(),
            paused: false,
            answers: VecDeque::new(),
        };
        harness.absorb(effects.into_vec());
        harness
    }

    /// A machine with R1's defaults.
    pub(crate) fn new() -> Self {
        Self::with(Config::default())
    }

    /// A machine with R1's defaults, connected as `client-1` with a Keep Alive of 30 seconds.
    pub(crate) fn connected() -> Self {
        let mut harness = Self::new();
        harness.connect(connect("client-1"));
        harness
    }

    /// A machine with `config`, connected as `client-1`.
    pub(crate) fn connected_with(config: Config) -> Self {
        let mut harness = Self::with(config);
        harness.connect(connect("client-1"));
        harness
    }

    /// Sends `connect` and returns the CONNACK, which must come.
    pub(crate) fn connect(&mut self, connect: Connect) -> ConnAck {
        let packets = self.send(connect);
        match packets.as_slice() {
            [Packet::ConnAck(connack)] => (**connack).clone(),
            other => panic!("expected one CONNACK, got {other:?}"),
        }
    }

    /// Feeds one input, then the answers it led to, and returns the packets the client
    /// received meanwhile.
    pub(crate) fn input(&mut self, input: Input) -> Vec<Packet> {
        self.feed(input);
        while let Some(answer) = self.answers.pop_front() {
            self.feed(answer);
        }
        self.take()
    }

    /// The client sends `packet` on the control stream.
    pub(crate) fn send(&mut self, packet: impl Into<Packet>) -> Vec<Packet> {
        self.input(Input::packet(packet))
    }

    /// The client sends `packet` on `stream`, and the test reads what it received with the
    /// streams it came on.
    pub(crate) fn send_on(
        &mut self,
        stream: StreamId,
        packet: impl Into<Packet>,
    ) -> Vec<(StreamId, Packet)> {
        self.feed(Input::Packet {
            stream,
            packet: packet.into(),
            early: false,
        });
        while let Some(answer) = self.answers.pop_front() {
            self.feed(answer);
        }
        self.received.drain(..).collect()
    }

    /// Feeds one input, then the answers it led to, and returns the packets the client
    /// received meanwhile with the streams they came on.
    pub(crate) fn input_on(&mut self, input: Input) -> Vec<(StreamId, Packet)> {
        self.feed(input);
        while let Some(answer) = self.answers.pop_front() {
            self.feed(answer);
        }
        self.received.drain(..).collect()
    }

    /// The packets the client received and the test has not read.
    pub(crate) fn take(&mut self) -> Vec<Packet> {
        self.received.drain(..).map(|(_, packet)| packet).collect()
    }

    /// Moves the clock by `by`, firing the timers it passes in order, and returns what the
    /// client received.
    pub(crate) fn advance(&mut self, by: Duration) -> Vec<Packet> {
        let until = self.now.saturating_add(by);
        loop {
            let due = self
                .timers
                .iter()
                .filter(|(_, at)| **at <= until)
                .min_by_key(|(_, at)| **at)
                .map(|(timer, at)| (*timer, *at));
            let Some((timer, when)) = due else {
                break;
            };
            self.timers.remove(&timer);
            self.now = self.now.max(when);
            self.feed(Input::Timer(timer));
            while let Some(answer) = self.answers.pop_front() {
                self.feed(answer);
            }
        }
        self.now = until;
        self.take()
    }

    /// Effects logged from `mark` on.
    pub(crate) fn since(&self, mark: usize) -> &[Effect] {
        &self.log[mark..]
    }

    /// Whether the machine closed the connection, and with what code.
    pub(crate) fn closed(&self) -> Option<CloseCode> {
        self.log.iter().find_map(|effect| match effect {
            Effect::Close(code) => Some(*code),
            _ => None,
        })
    }

    /// Every release, in order.
    pub(crate) fn releases(&self) -> Vec<&Release> {
        self.log
            .iter()
            .filter_map(|effect| match effect {
                Effect::Release(release) => Some(release),
                _ => None,
            })
            .collect()
    }

    /// Every will order, in order.
    pub(crate) fn wills(&self) -> Vec<&WillOrder> {
        self.log
            .iter()
            .filter_map(|effect| match effect {
                Effect::Will(order) => Some(order),
                _ => None,
            })
            .collect()
    }

    /// Every message published into the broker, in order.
    pub(crate) fn published(&self) -> Vec<&Message> {
        self.log
            .iter()
            .filter_map(|effect| match effect {
                Effect::Publish(publication) => Some(&publication.message),
                _ => None,
            })
            .collect()
    }

    /// How often `counter` was counted.
    pub(crate) fn count(&self, counter: Counter) -> usize {
        self.log
            .iter()
            .filter(|effect| **effect == Effect::Count(counter))
            .count()
    }

    /// Feeds one input and takes in its effects.
    fn feed(&mut self, input: Input) {
        let effects = self.session.handle(input, self.now);
        self.absorb(effects.into_vec());
    }

    /// Takes in effects: records them, plays the client's side of the transport, and queues
    /// the answers the harness gives by itself.
    fn absorb(&mut self, effects: Vec<Effect>) {
        for effect in effects {
            self.log.push(effect.clone());
            match effect {
                Effect::Send { stream, packet } => self.received.push_back((stream, packet)),
                Effect::Authenticate(request) => {
                    if self.auto.authenticate {
                        self.answers
                            .push_back(Input::Authenticated(AuthResult::Success { data: None }));
                    } else {
                        self.authentications.push(request);
                    }
                }
                Effect::Authorize(request) => match self.auto.authorize {
                    Some(rule) => self.answers.push_back(Input::Authorized {
                        request: request.request,
                        decisions: request.actions.iter().map(rule).collect(),
                    }),
                    None => self.authorizations.push(request),
                },
                Effect::Claim(claim) => {
                    if self.auto.claim {
                        let session = if claim.clean_start {
                            None
                        } else {
                            self.stored.take()
                        };
                        self.answers
                            .push_back(Input::Claimed(ClaimResult::Claimed { session }));
                    } else {
                        self.claims.push(claim);
                    }
                }
                Effect::Publish(publication) => self.route(publication),
                Effect::Subscribe(interest) => {
                    if interest.send_retained {
                        if self.auto.retained {
                            let messages = self
                                .retained
                                .values()
                                .filter(|message| interest.filter.matches(&message.topic))
                                .cloned()
                                .collect();
                            self.answers.push_back(Input::Retained {
                                filter: interest.filter.clone(),
                                messages,
                            });
                        } else {
                            self.retained_reads.push(interest.filter.clone());
                        }
                    }
                    self.interest.insert(interest.filter, interest.options);
                }
                Effect::Unsubscribe(filter) => {
                    self.interest.remove(&filter);
                }
                Effect::SetTimer { timer, at } => {
                    self.timers.insert(timer, at);
                }
                Effect::CancelTimer(timer) => {
                    self.timers.remove(&timer);
                }
                Effect::PauseReading => self.paused = true,
                Effect::ResumeReading => self.paused = false,
                Effect::SendRefusal(_)
                | Effect::FinishStream(_)
                | Effect::Close(_)
                | Effect::Release(_)
                | Effect::Will(_)
                | Effect::Count(_) => {}
            }
        }
    }

    /// Plays the broker for a publication: the retained store, the session's own
    /// subscriptions, and the commit.
    fn route(&mut self, publication: Publication) {
        if !self.auto.loopback {
            if publication.token.is_some() {
                self.commits.push(publication);
            }
            return;
        }
        let message = publication.message.clone();
        if message.retain {
            if message.payload.is_empty() {
                self.retained.remove(&message.topic);
            } else {
                self.retained.insert(message.topic.clone(), message.clone());
            }
        }
        let filters: Vec<TopicFilter> = self
            .interest
            .keys()
            .filter(|filter| filter.matches(&message.topic))
            .cloned()
            .collect();
        let matched = !filters.is_empty();
        if matched {
            self.answers.push_back(Input::Deliver(Delivery {
                message,
                subscriptions: filters,
            }));
        }
        match publication.token {
            Some(token) if self.auto.commit => self.answers.push_back(Input::Committed {
                token,
                outcome: PublishOutcome::Accepted { matched },
            }),
            Some(_) => self.commits.push(publication),
            None => {}
        }
    }
}

/// A CONNECT for `client_id` with Clean Start and a Keep Alive of 30 seconds.
pub(crate) fn connect(client_id: &str) -> Connect {
    Connect {
        clean_start: true,
        keep_alive: 30,
        client_id: client_id.to_owned(),
        ..Connect::default()
    }
}

/// A CONNECT for `client_id` changed by `change`.
pub(crate) fn connect_with(client_id: &str, change: impl FnOnce(&mut Connect)) -> Connect {
    let mut connect = connect(client_id);
    change(&mut connect);
    connect
}

/// CONNECT properties with this Session Expiry Interval.
pub(crate) fn expiry(seconds: u32) -> ConnectProperties {
    ConnectProperties {
        session_expiry_interval: Some(seconds),
        ..ConnectProperties::default()
    }
}

/// A Packet Identifier.
pub(crate) fn id(value: u16) -> PacketId {
    PacketId::new(value).expect("a test uses non-zero identifiers")
}

/// A PUBLISH from the client.
pub(crate) fn publish(topic: &str, qos: QoS, packet_id: Option<u16>, payload: &str) -> Publish {
    Publish {
        qos,
        topic: topic.to_owned(),
        packet_id: packet_id.map(id),
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        ..Publish::default()
    }
}

/// A QoS 0 PUBLISH.
pub(crate) fn publish0(topic: &str, payload: &str) -> Publish {
    publish(topic, QoS::AtMostOnce, None, payload)
}

/// A QoS 1 PUBLISH.
pub(crate) fn publish1(topic: &str, packet_id: u16, payload: &str) -> Publish {
    publish(topic, QoS::AtLeastOnce, Some(packet_id), payload)
}

/// A QoS 2 PUBLISH.
pub(crate) fn publish2(topic: &str, packet_id: u16, payload: &str) -> Publish {
    publish(topic, QoS::ExactlyOnce, Some(packet_id), payload)
}

/// A retained PUBLISH.
pub(crate) fn retained(topic: &str, qos: QoS, packet_id: Option<u16>, payload: &str) -> Publish {
    Publish {
        retain: true,
        ..publish(topic, qos, packet_id, payload)
    }
}

/// Subscription Options at this Maximum QoS.
pub(crate) fn options(qos: QoS) -> SubscriptionOptions {
    SubscriptionOptions {
        maximum_qos: qos,
        ..SubscriptionOptions::default()
    }
}

/// A SUBSCRIBE of each filter with its options.
pub(crate) fn subscribe(packet_id: u16, filters: &[(&str, SubscriptionOptions)]) -> Subscribe {
    Subscribe {
        packet_id: id(packet_id),
        properties: SubscribeProperties::default(),
        subscriptions: filters
            .iter()
            .map(|(filter, options)| Subscription {
                filter: (*filter).to_owned(),
                options: *options,
            })
            .collect(),
    }
}

/// A SUBSCRIBE of one filter at `qos`.
pub(crate) fn subscribe1(packet_id: u16, filter: &str, qos: QoS) -> Subscribe {
    subscribe(packet_id, &[(filter, options(qos))])
}

/// An UNSUBSCRIBE of each filter.
pub(crate) fn unsubscribe(packet_id: u16, filters: &[&str]) -> Unsubscribe {
    Unsubscribe {
        packet_id: id(packet_id),
        properties: UnsubscribeProperties::default(),
        filters: filters.iter().map(|filter| (*filter).to_owned()).collect(),
    }
}

/// A PUBACK with Success.
pub(crate) fn puback(packet_id: u16) -> PubAck {
    PubAck::new(id(packet_id))
}

/// A PUBREC with Success.
pub(crate) fn pubrec(packet_id: u16) -> PubRec {
    PubRec::new(id(packet_id))
}

/// A PUBREL with Success.
pub(crate) fn pubrel(packet_id: u16) -> PubRel {
    PubRel::new(id(packet_id))
}

/// A PUBCOMP with Success.
pub(crate) fn pubcomp(packet_id: u16) -> PubComp {
    PubComp::new(id(packet_id))
}

/// A Topic Name.
pub(crate) fn name(text: &str) -> TopicName {
    TopicName::new(text).expect("a test uses valid names")
}

/// A Topic Filter.
pub(crate) fn filter(text: &str) -> TopicFilter {
    TopicFilter::new(text).expect("a test uses valid filters")
}

/// A message from another client, as the broker delivers it.
pub(crate) fn message(topic: &str, qos: openqtt_core::QoS, payload: &str) -> Message {
    let mut message = Message::new(name(topic), Bytes::copy_from_slice(payload.as_bytes()));
    message.qos = qos;
    message.publisher = Some(openqtt_core::ClientId::new("other").expect("a valid identifier"));
    message
}

/// The PUBLISH packets among `packets`.
pub(crate) fn publishes(packets: &[Packet]) -> Vec<&Publish> {
    packets
        .iter()
        .filter_map(|packet| match packet {
            Packet::Publish(publish) => Some(publish),
            _ => None,
        })
        .collect()
}

/// The payloads of the PUBLISH packets among `packets`, as text.
pub(crate) fn payloads(packets: &[Packet]) -> Vec<String> {
    publishes(packets)
        .iter()
        .map(|publish| String::from_utf8_lossy(&publish.payload).into_owned())
        .collect()
}

/// Moves the clock by whole seconds.
pub(crate) fn seconds(value: u64) -> Duration {
    Duration::from_secs(value)
}
