//! Messages from the client: PUBLISH at each QoS, its authorization and commit, the
//! acknowledgements in arrival order, and PUBREL.

use openqtt_codec::{
    AckProperties, DisconnectReasonCode, PacketId, PubAck, PubComp, PubCompReasonCode, PubRec,
    PubRel, Publish, QoS,
};
use openqtt_core::{Message, Timestamp, TopicName};

use super::connect::deadline;
use super::{
    AckCode, Arrival, Authorizing, Commit, Inbound, InboundState, Phase, Reply, ReplyKind,
    ReplyState, Session,
};
use crate::convert::{core_format, core_qos};
use crate::phrase::phrase;
use crate::{
    Action, Authorization, Counter, Decision, Effect, Effects, Publication, PublishOutcome,
    PublishToken, RequestId, StreamId,
};

impl Session {
    /// A PUBLISH from the client.
    pub(super) fn publish(
        &mut self,
        stream: StreamId,
        mut publish: Publish,
        arrival: Arrival,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        if let Some(alias) = publish.properties.topic_alias.take() {
            if stream != StreamId::Control {
                // Topic Aliases travel on the control stream only (docs/spec/mqtt-over-quic.md,
                // section 2.3; report R1, O6).
                return self.protocol_error(now, fx);
            }
            if alias.get() > self.config.topic_alias_maximum {
                // [MQTT-3.2.2-17], [MQTT-3.3.2-9]: above the maximum the server announced.
                return self.close_with(DisconnectReasonCode::TopicAliasInvalid, None, now, fx);
            }
            if publish.topic.is_empty() {
                match self.aliases_in.get(&alias.get()) {
                    Some(topic) => publish.topic.clone_from(topic),
                    // An alias that maps to nothing (section 3.3.2.3.4).
                    None => return self.protocol_error(now, fx),
                }
            } else {
                // [MQTT-3.3.2-12]: every alias up to the maximum, set or replaced.
                self.aliases_in.insert(alias.get(), publish.topic.clone());
            }
        }
        if publish.qos > self.config.maximum_qos {
            // [MQTT-3.2.2-11]
            return self.close_with(DisconnectReasonCode::QosNotSupported, None, now, fx);
        }
        if publish.retain && !self.config.retain_available {
            // [MQTT-3.2.2-14]
            return self.close_with(DisconnectReasonCode::RetainNotSupported, None, now, fx);
        }
        if let Some(packet_id) = publish.packet_id {
            // A repeat before PUBREL: PUBREC again, and no second delivery ([MQTT-4.3.3-10],
            // report R1 D8). A reserved identifier is published again instead, under its
            // receipt, which the log keeps from the first commit if there was one.
            let repeat = match self.inbound.get(&packet_id.get()) {
                Some(inbound) if publish.qos == QoS::ExactlyOnce => match inbound.state {
                    InboundState::Committing(token) => Some(ReplyState::Waiting(token)),
                    InboundState::AwaitingRelease => Some(ReplyState::Ready(AckCode::Success)),
                    InboundState::Reserved => None,
                },
                _ => None,
            };
            if let Some(state) = repeat {
                self.push_reply(stream, packet_id, ReplyKind::PubRec, state, false);
                return self.flush(stream, now, fx);
            }
            // Held to Receive Maximum when it arrived ([MQTT-3.3.4-7]); it takes its slot now.
            self.inbound_in_flight = self.inbound_in_flight.saturating_add(1);
        }
        // Topic syntax refuses this PUBLISH alone (report R1, O25 and D32).
        let Ok(topic) = TopicName::new(&publish.topic) else {
            return self.refuse_publish(stream, &publish, AckCode::TopicNameInvalid, now, fx);
        };
        if topic.level_count() > self.config.maximum_topic_levels {
            // Report R1, O16; R2 rule 8: limits apply before mounting.
            return self.refuse_publish(stream, &publish, AckCode::TopicNameInvalid, now, fx);
        }
        let response_topic = match publish.properties.response_topic.as_deref() {
            Some(text) => match TopicName::new(text) {
                Ok(topic) => Some(topic),
                // [MQTT-3.3.2-14]
                Err(_) => {
                    return self.refuse_publish(
                        stream,
                        &publish,
                        AckCode::TopicNameInvalid,
                        now,
                        fx,
                    );
                }
            },
            None => None,
        };
        // Authorization sees the client's own topic (report R2, rule 7).
        let request = self.request();
        fx.push(Effect::Authorize(Authorization {
            request: RequestId(request),
            actions: vec![Action::Publish {
                topic: topic.clone(),
                qos: core_qos(publish.qos),
                retain: publish.retain,
            }],
        }));
        self.authorizing = Some(Authorizing::Publish {
            request,
            stream,
            received_at: arrival.at,
            publish: Box::new(publish),
            topic,
            response_topic,
        });
    }

    /// The authorizer answered for the packet waiting on it.
    pub(super) fn authorized_packet(
        &mut self,
        request: u64,
        decisions: &[Decision],
        now: Timestamp,
        fx: &mut Effects,
    ) {
        let matches = match &self.authorizing {
            Some(
                Authorizing::Publish { request: r, .. } | Authorizing::Subscribe { request: r, .. },
            ) => *r == request,
            None => false,
        };
        if !matches {
            return;
        }
        match self.authorizing.take() {
            Some(Authorizing::Publish {
                stream,
                received_at,
                publish,
                topic,
                response_topic,
                ..
            }) => {
                let allowed = decisions.first() == Some(&Decision::Allow);
                let route = Route {
                    stream,
                    received_at,
                    topic: &topic,
                    response_topic,
                    allowed,
                };
                self.route(route, *publish, now, fx);
            }
            Some(Authorizing::Subscribe {
                stream,
                packet_id,
                id,
                plan,
                ..
            }) => self.answer_subscribe(stream, packet_id, id, plan, decisions, now, fx),
            None => {}
        }
    }

    /// Publishes an authorized PUBLISH into the broker, or refuses it.
    fn route(&mut self, route: Route<'_>, publish: Publish, now: Timestamp, fx: &mut Effects) {
        let Route {
            stream,
            received_at,
            topic,
            response_topic,
            allowed,
        } = route;
        if !allowed {
            // Report R1, D2 and O14; report R2, rule 12.
            return self.refuse_publish(stream, &publish, AckCode::NotAuthorized, now, fx);
        }
        let Some(client) = &self.client else {
            return;
        };
        // Mounted after authorization (report R2, rules 6 and 7).
        let mounted = match &client.mount {
            Some(mount) => match mount.mount_name(topic) {
                Ok(mounted) => mounted,
                Err(_) => {
                    return self.refuse_publish(
                        stream,
                        &publish,
                        AckCode::TopicNameInvalid,
                        now,
                        fx,
                    );
                }
            },
            None => topic.clone(),
        };
        // The receipt the log commits with a QoS 2 message, `rel/{cid}/{pid}` (report R3).
        let receipt = publish
            .packet_id
            .filter(|_| publish.qos == QoS::ExactlyOnce);
        let properties = publish.properties;
        let mut message = Message::new(mounted, publish.payload);
        message.qos = core_qos(publish.qos);
        message.retain = publish.retain;
        // So that No Local holds the message back from this client wherever it is delivered
        // ([MQTT-3.8.3-3]).
        message.publisher = Some(client.id.clone());
        // [MQTT-3.3.2-4], [MQTT-3.3.2-15] to [MQTT-3.3.2-17], [MQTT-3.3.2-20]: every property
        // a subscriber receives, unaltered. DUP and the Topic Alias belong to this hop
        // ([MQTT-3.3.1-3]).
        message.payload_format = properties.payload_format_indicator.map(core_format);
        // Report R1, O8: the deadline is the moment of receipt plus the interval.
        message.expiry = deadline(received_at, properties.message_expiry_interval);
        message.content_type = properties.content_type;
        message.response_topic = response_topic;
        message.correlation_data = properties.correlation_data;
        message.user_properties = properties.user_properties;
        let token = match publish.packet_id {
            None => None,
            Some(packet_id) => {
                let token = self.token();
                let kind = if publish.qos == QoS::ExactlyOnce {
                    self.inbound.insert(
                        packet_id.get(),
                        Inbound {
                            state: InboundState::Committing(token),
                            stream,
                            counted: true,
                        },
                    );
                    ReplyKind::PubRec
                } else {
                    ReplyKind::PubAck
                };
                self.commits.insert(
                    token,
                    Commit {
                        stream,
                        packet_id,
                        qos: publish.qos,
                        retain: publish.retain,
                    },
                );
                // [MQTT-4.3.2-4], [MQTT-4.3.3-8]: acknowledged once durable (report R1, D26).
                self.push_reply(stream, packet_id, kind, ReplyState::Waiting(token), true);
                Some(PublishToken(token))
            }
        };
        fx.push(Effect::Publish(Publication {
            message,
            token,
            receipt,
        }));
    }

    /// Refuses one PUBLISH with `code` and keeps the connection: in its PUBACK or PUBREC, or
    /// at QoS 0 by dropping it and counting.
    fn refuse_publish(
        &mut self,
        stream: StreamId,
        publish: &Publish,
        code: AckCode,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        let Some(packet_id) = publish.packet_id else {
            let counter = if code == AckCode::NotAuthorized {
                Counter::PublishDenied
            } else {
                Counter::PublishTopicInvalid
            };
            return fx.push(Effect::Count(counter));
        };
        let kind = if publish.qos == QoS::ExactlyOnce {
            // The refusal ends the exchange and frees the identifier for the client
            // ([MQTT-4.3.3-9]), so whatever the log holds under it goes too: nothing from this
            // PUBLISH, but a receipt a cut-off commit of a reserved identifier may have left,
            // which would swallow the next message sent with it.
            let reserved = self
                .inbound
                .get(&packet_id.get())
                .is_some_and(|inbound| inbound.state == InboundState::Reserved);
            if reserved {
                self.inbound.remove(&packet_id.get());
            }
            fx.push(Effect::ReleaseReceipt(packet_id));
            ReplyKind::PubRec
        } else {
            ReplyKind::PubAck
        };
        self.push_reply(stream, packet_id, kind, ReplyState::Ready(code), true);
        self.flush(stream, now, fx);
    }

    /// What became of a publication.
    pub(super) fn committed(
        &mut self,
        token: u64,
        outcome: PublishOutcome,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        if self.phase == Phase::Closed {
            return;
        }
        let Some(commit) = self.commits.remove(&token) else {
            return;
        };
        let code = match outcome {
            // Report R1, O26: 0x10 only for a message neither retained nor matched.
            PublishOutcome::Accepted { matched } if matched || commit.retain => AckCode::Success,
            PublishOutcome::Accepted { .. } => AckCode::NoMatchingSubscribers,
            PublishOutcome::QuotaExceeded => AckCode::QuotaExceeded,
            PublishOutcome::Failed => AckCode::UnspecifiedError,
        };
        if commit.qos == QoS::ExactlyOnce {
            let id = commit.packet_id.get();
            if let Some(inbound) = self.inbound.get_mut(&id)
                && inbound.state == InboundState::Committing(token)
            {
                if code.is_error() {
                    // [MQTT-4.3.3-9]: the identifier is free again. The failed commit wrote
                    // nothing, but a repeat of a reserved identifier may find the log still
                    // holding the receipt of the first, cut-off commit: it goes with the
                    // exchange.
                    self.inbound.remove(&id);
                    fx.push(Effect::ReleaseReceipt(commit.packet_id));
                } else {
                    inbound.state = InboundState::AwaitingRelease;
                }
            }
        }
        // The PUBLISH and every repeat of it wait on this commit, and a repeat may have come on
        // another stream. The PUBLISH's own stream is answered first.
        let mut streams = vec![commit.stream];
        for (stream, replies) in &mut self.replies {
            for reply in replies
                .iter_mut()
                .filter(|reply| reply.state == ReplyState::Waiting(token))
            {
                reply.state = ReplyState::Ready(code);
                if !streams.contains(stream) {
                    streams.push(*stream);
                }
            }
        }
        for stream in streams {
            self.flush(stream, now, fx);
        }
    }

    /// Adds an acknowledgement to those owed on `stream`.
    fn push_reply(
        &mut self,
        stream: StreamId,
        packet_id: PacketId,
        kind: ReplyKind,
        state: ReplyState,
        counted: bool,
    ) {
        self.replies.entry(stream).or_default().push_back(Reply {
            packet_id,
            kind,
            state,
            counted,
        });
    }

    /// Sends the acknowledgements owed on `stream` that are ready, in the order the packets
    /// they answer arrived: one whose commit finished early waits for those before it (report
    /// R1, O15).
    pub(super) fn flush(&mut self, stream: StreamId, now: Timestamp, fx: &mut Effects) {
        while let Some(replies) = self.replies.get_mut(&stream) {
            let code = match replies.front() {
                None => {
                    self.replies.remove(&stream);
                    break;
                }
                Some(Reply {
                    state: ReplyState::Waiting(_),
                    ..
                }) => return,
                Some(Reply {
                    state: ReplyState::Ready(code),
                    ..
                }) => *code,
            };
            let Some(reply) = replies.pop_front() else {
                break;
            };
            // A PUBACK, or a PUBREC refusing the message, ends the exchange and frees its slot
            // of the server's Receive Maximum (section 4.9).
            if reply.counted && (reply.kind == ReplyKind::PubAck || code.is_error()) {
                self.inbound_in_flight = self.inbound_in_flight.saturating_sub(1);
            }
            let properties = AckProperties {
                reason_string: phrase(code.puback().value()),
                ..AckProperties::default()
            };
            // [MQTT-2.2.1-5], [MQTT-3.3.4-1]
            match reply.kind {
                ReplyKind::PubAck => self.send(
                    stream,
                    PubAck {
                        packet_id: reply.packet_id,
                        reason_code: code.puback(),
                        properties,
                    },
                    now,
                    fx,
                ),
                ReplyKind::PubRec => self.send(
                    stream,
                    PubRec {
                        packet_id: reply.packet_id,
                        reason_code: code.pubrec(),
                        properties,
                    },
                    now,
                    fx,
                ),
            }
            if self.phase == Phase::Closed {
                return;
            }
        }
        self.finish_stream_if_done(stream, fx);
    }

    /// The client's PUBREL ([MQTT-4.3.3-11]).
    pub(super) fn pubrel(
        &mut self,
        stream: StreamId,
        pubrel: &PubRel,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        let id = pubrel.packet_id;
        // A reserved identifier cannot have had its PUBREC, but a client that releases it says
        // it is done with it, and the receipt goes so as not to swallow its next message.
        let released = self.inbound.get(&id.get()).is_some_and(|inbound| {
            matches!(
                inbound.state,
                InboundState::AwaitingRelease | InboundState::Reserved
            )
        });
        let reason_code = if released {
            if let Some(inbound) = self.inbound.remove(&id.get())
                && inbound.counted
            {
                self.inbound_in_flight = self.inbound_in_flight.saturating_sub(1);
            }
            // [MQTT-4.3.3-12]: the identifier is free for a new message, and the log lets go
            // of the receipt it kept under it.
            fx.push(Effect::ReleaseReceipt(id));
            PubCompReasonCode::Success
        } else {
            PubCompReasonCode::PacketIdentifierNotFound
        };
        let properties = AckProperties {
            reason_string: phrase(reason_code.value()),
            ..AckProperties::default()
        };
        self.send(
            stream,
            PubComp {
                packet_id: id,
                reason_code,
                properties,
            },
            now,
            fx,
        );
        self.finish_stream_if_done(stream, fx);
    }
}

/// What the checks before authorization found out about a PUBLISH.
struct Route<'a> {
    /// The stream it came on.
    stream: StreamId,
    /// When it arrived.
    received_at: Timestamp,
    /// Its Topic Name, checked and not mounted.
    topic: &'a TopicName,
    /// Its Response Topic, checked.
    response_topic: Option<TopicName>,
    /// Whether the authorizer allowed it.
    allowed: bool,
}
