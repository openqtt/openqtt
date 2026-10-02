//! Messages to the client: deliveries and retained messages, the queue, Packet Identifiers,
//! Receive Maximum, Maximum Packet Size and Topic Aliases, and the client's acknowledgements.

use std::num::{NonZeroU16, NonZeroU32};

use openqtt_codec::{
    DisconnectReasonCode, PacketId, PubAck, PubComp, PubRec, PubRel, PubRelReasonCode, Publish,
    PublishProperties, QoS as CodecQoS,
};
use openqtt_core::{Message, QoS, Timestamp, TopicFilter, TopicName};

use super::{Ending, Outbound, Phase, Queued, Session, Stage, Subscription};
use crate::convert::{codec_format, codec_qos, codec_subscription_id};
use crate::{Counter, Delivery, Effect, Effects, StreamId};

/// Stands in for the Packet Identifier while a PUBLISH is measured: the field is two bytes
/// whichever it is.
const MEASURE_ID: PacketId = PacketId::MIN;

impl Session {
    /// A message from the broker.
    pub(super) fn deliver(&mut self, delivery: Delivery, now: Timestamp, fx: &mut Effects) {
        // Before a claim the connection has no session to deliver to, and after the end it has
        // no client.
        if self.phase == Phase::Closed
            || self.client.is_none()
            || self.claim != super::ClaimState::Held
        {
            return;
        }
        if self.names_awaiting(&delivery) {
            // Report R1, O2: a live message waits for the retained ones of a new subscription.
            self.held.push_back(delivery);
            return self.check_queue(now, fx);
        }
        if delivery.message.is_expired(now) {
            // [MQTT-3.3.2-5], report R1 O8.
            return fx.push(Effect::Count(Counter::DeliveryExpired));
        }
        match self.resolve(&delivery) {
            Ok(queued) => self.enqueue(queued, now, fx),
            Err(counter) => fx.push(Effect::Count(counter)),
        }
    }

    /// Whether a delivery names a subscription whose retained messages have not been sent.
    fn names_awaiting(&self, delivery: &Delivery) -> bool {
        delivery.subscriptions.iter().any(|filter| {
            self.subscriptions
                .get(filter)
                .is_some_and(|subscription| subscription.retained_reads > 0)
        })
    }

    /// How a delivery goes out: one copy for the session (report R1, O11), at the highest QoS
    /// of the subscriptions that take it, with all their Subscription Identifiers, on the
    /// stream of the one with the highest QoS, the oldest among equals.
    pub(super) fn resolve(&self, delivery: &Delivery) -> Result<Queued, Counter> {
        let Some(client) = &self.client else {
            return Err(Counter::DeliveryUnmatched);
        };
        let message = &delivery.message;
        // The topic as the client sees it; a topic outside its namespace is not delivered
        // (report R2, rule 6).
        let topic = match &client.mount {
            Some(mount) => mount
                .strip(&message.topic)
                .ok_or(Counter::DeliveryOutsideNamespace)?,
            None => message.topic.clone(),
        };
        let own = message.publisher.as_ref() == Some(&client.id);
        let mut chosen: Option<&Subscription> = None;
        let mut retain_as_published = false;
        let mut ids = Vec::new();
        for filter in &delivery.subscriptions {
            let Some(subscription) = self.subscriptions.get(filter) else {
                // Unsubscribed: no new message is added for it ([MQTT-3.10.4-2]).
                continue;
            };
            // The client's own filter must match the topic it would see ([MQTT-3.3.2-3],
            // [MQTT-4.7.2-1]): its `#`, mounted, takes in the mount's `$SYS/x`, which its own
            // `#` does not (report R2, rule 6).
            if !subscription.filter.matches(&topic) {
                continue;
            }
            // [MQTT-3.8.3-3]: never to the publisher's own connection.
            if own && subscription.options.no_local {
                continue;
            }
            retain_as_published |= subscription.options.retain_as_published;
            if let Some(id) = subscription.options.id {
                ids.push(id);
            }
            let better = chosen.is_none_or(|best| {
                (
                    subscription.options.qos,
                    std::cmp::Reverse(subscription.order),
                ) > (best.options.qos, std::cmp::Reverse(best.order))
            });
            if better {
                chosen = Some(subscription);
            }
        }
        let Some(chosen) = chosen else {
            return Err(Counter::DeliveryUnmatched);
        };
        ids.sort_unstable();
        ids.dedup();
        Ok(Queued {
            message: message.clone(),
            topic,
            // [MQTT-3.8.4-8]: the lower of the published and the granted QoS.
            qos: message.qos.min(chosen.options.qos),
            // [MQTT-3.3.1-12], [MQTT-3.3.1-13]
            retain: message.retain && retain_as_published,
            subscription_ids: ids,
            stream: chosen.stream,
        })
    }

    /// Queues a message for the client and sends what it can.
    fn enqueue(&mut self, queued: Queued, now: Timestamp, fx: &mut Effects) {
        self.queue.push_back(queued);
        self.pump(now, fx);
        self.check_queue(now, fx);
    }

    /// Ends the session when more messages wait than it may hold (report R1, O12;
    /// [MQTT-4.1.0-1]): DISCONNECT 0x97 first, so the client learns of the loss from Session
    /// Present 0 when it comes back.
    fn check_queue(&mut self, now: Timestamp, fx: &mut Effects) {
        if self.queued() <= self.config.maximum_queued_messages {
            return;
        }
        match self.phase {
            Phase::Connected => self.close_ending(
                DisconnectReasonCode::QuotaExceeded,
                None,
                Ending::SessionDiscarded,
                now,
                fx,
            ),
            Phase::Closed => {}
            _ => self.refuse_ending(
                openqtt_codec::ConnectReasonCode::QuotaExceeded,
                None,
                Ending::SessionDiscarded,
                now,
                fx,
            ),
        }
    }

    /// Queues the held deliveries that no longer wait for a retained read, in order.
    pub(super) fn release_held(&mut self, now: Timestamp, fx: &mut Effects) {
        while let Some(front) = self.held.front() {
            if self.names_awaiting(front) {
                break;
            }
            let Some(delivery) = self.held.pop_front() else {
                break;
            };
            if delivery.message.is_expired(now) {
                fx.push(Effect::Count(Counter::DeliveryExpired));
                continue;
            }
            match self.resolve(&delivery) {
                Ok(queued) => self.queue.push_back(queued),
                Err(counter) => fx.push(Effect::Count(counter)),
            }
        }
    }

    /// The retained messages for a new subscription: sent with RETAIN 1, after the SUBACK and
    /// before any live message for the subscription (report R1, O2; [MQTT-3.3.1-9]).
    pub(super) fn retained(
        &mut self,
        filter: &TopicFilter,
        messages: Vec<Message>,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        if self.phase == Phase::Closed {
            return;
        }
        let subscription = match self.subscriptions.get_mut(filter) {
            Some(subscription) if subscription.retained_reads > 0 => {
                subscription.retained_reads -= 1;
                Some(subscription.clone())
            }
            _ => None,
        };
        if let Some(subscription) = subscription {
            for message in messages {
                if message.is_expired(now) {
                    fx.push(Effect::Count(Counter::DeliveryExpired));
                    continue;
                }
                match self.resolve_retained(&subscription, message) {
                    Ok(queued) => self.queue.push_back(queued),
                    Err(counter) => fx.push(Effect::Count(counter)),
                }
            }
        }
        self.release_held(now, fx);
        self.pump(now, fx);
        self.check_queue(now, fx);
    }

    /// How a retained message goes out for the subscription that asked for it.
    fn resolve_retained(
        &self,
        subscription: &Subscription,
        message: Message,
    ) -> Result<Queued, Counter> {
        let Some(client) = &self.client else {
            return Err(Counter::DeliveryUnmatched);
        };
        let topic = match &client.mount {
            Some(mount) => mount
                .strip(&message.topic)
                .ok_or(Counter::DeliveryOutsideNamespace)?,
            None => message.topic.clone(),
        };
        // As for a live message: the client's own filter, and No Local (report R2, rule 6;
        // [MQTT-3.8.3-3]).
        if !subscription.filter.matches(&topic)
            || (subscription.options.no_local && message.publisher.as_ref() == Some(&client.id))
        {
            return Err(Counter::DeliveryUnmatched);
        }
        Ok(Queued {
            qos: message.qos.min(subscription.options.qos),
            // Sent because a subscription was made: RETAIN 1, whatever Retain As Published
            // says ([MQTT-3.3.1-12]).
            retain: true,
            subscription_ids: subscription.options.id.into_iter().collect(),
            stream: subscription.stream,
            topic,
            message,
        })
    }

    /// Sends what it can, in order: first what a resumed session had in flight, then the
    /// queue, as long as the client's Receive Maximum allows ([MQTT-3.3.4-9], [MQTT-4.9.0-2]).
    pub(super) fn pump(&mut self, now: Timestamp, fx: &mut Effects) {
        loop {
            if self.phase != Phase::Connected {
                return;
            }
            if let Some(index) = self.outbound.iter().position(|outbound| !outbound.sent) {
                if !self.resend(index, now, fx) {
                    return;
                }
                continue;
            }
            let Some(front) = self.queue.front() else {
                return;
            };
            if front.message.is_expired(now) {
                // [MQTT-3.3.2-5]: a copy whose delivery has not started is deleted.
                self.queue.pop_front();
                fx.push(Effect::Count(Counter::DeliveryExpired));
                continue;
            }
            if front.qos != QoS::AtMostOnce && !self.window_open() {
                // [MQTT-4.9.0-2]: nothing more at QoS 1 or 2 until a slot frees; the rest
                // waits behind it, in order.
                return;
            }
            let Some(queued) = self.queue.pop_front() else {
                return;
            };
            self.send_delivery(queued, now, fx);
        }
    }

    /// Whether another QoS 1 or 2 PUBLISH may go out: a slot of the client's Receive Maximum,
    /// capped by the server's own (report R1, O3), and a free Packet Identifier.
    fn window_open(&self) -> bool {
        let receive_maximum = self
            .client
            .as_ref()
            .map_or(0, |client| client.receive_maximum);
        let window = receive_maximum.min(self.config.receive_maximum.get());
        self.outbound_in_window < usize::from(window) && self.ids.len() < usize::from(u16::MAX)
    }

    /// Sends again a message a resumed session had in flight, with its Packet Identifier
    /// ([MQTT-4.4.0-1]) on the control stream (docs/spec/mqtt-over-quic.md, section 2.4).
    /// Returns false when it has to wait for a slot.
    fn resend(&mut self, index: usize, now: Timestamp, fx: &mut Effects) -> bool {
        let Some(outbound) = self.outbound.get(index) else {
            return false;
        };
        let packet_id = outbound.packet_id;
        match (outbound.stage, outbound.publish.clone()) {
            (Stage::Acknowledgement | Stage::Receipt, Some(mut publish)) => {
                if !self.window_open_for_resend() {
                    return false;
                }
                // [MQTT-3.3.1-1]: DUP marks it as sent before.
                publish.dup = true;
                if !self.fits(&publish) {
                    // The new connection's Maximum Packet Size is smaller: discarded as if
                    // sent ([MQTT-3.1.2-25]).
                    self.complete(index);
                    fx.push(Effect::Count(Counter::DeliveryTooLarge));
                    return true;
                }
                if let Some(outbound) = self.outbound.get_mut(index) {
                    outbound.sent = true;
                    outbound.counted = true;
                    outbound.stream = StreamId::Control;
                }
                self.outbound_in_window += 1;
                self.send(StreamId::Control, publish, now, fx);
            }
            _ => {
                // A PUBREL takes no slot of its own (section 4.9).
                if let Some(outbound) = self.outbound.get_mut(index) {
                    outbound.sent = true;
                    outbound.stream = StreamId::Control;
                }
                self.send(StreamId::Control, PubRel::new(packet_id), now, fx);
            }
        }
        true
    }

    /// Whether a slot of the client's Receive Maximum is free for a resent PUBLISH, which
    /// already holds its Packet Identifier.
    fn window_open_for_resend(&self) -> bool {
        let receive_maximum = self
            .client
            .as_ref()
            .map_or(0, |client| client.receive_maximum);
        let window = receive_maximum.min(self.config.receive_maximum.get());
        self.outbound_in_window < usize::from(window)
    }

    /// Whether a PUBLISH fits the client's Maximum Packet Size ([MQTT-3.1.2-24]); a packet
    /// exactly at the limit does (report R1, D6).
    fn fits(&self, publish: &Publish) -> bool {
        publish.encoded_len().is_ok_and(|size| {
            u64::try_from(size).is_ok_and(|size| size <= u64::from(self.limits.maximum_packet_size))
        })
    }

    /// Sends one queued message.
    fn send_delivery(&mut self, queued: Queued, now: Timestamp, fx: &mut Effects) {
        let stream = self.delivery_stream(queued.stream);
        let message = &queued.message;
        let qos = codec_qos(queued.qos);
        let mut publish = Publish {
            // [MQTT-3.3.1-3], [MQTT-4.3.1-1], [MQTT-4.3.2-2], [MQTT-4.3.3-2]: a first send.
            dup: false,
            qos,
            retain: queued.retain,
            topic: queued.topic.as_str().to_owned(),
            packet_id: (qos != CodecQoS::AtMostOnce).then_some(MEASURE_ID),
            properties: PublishProperties {
                payload_format_indicator: message.payload_format.map(codec_format),
                // [MQTT-3.3.2-6]: the time left, rounded up, never 0 (report R1, O8).
                message_expiry_interval: message
                    .expiry
                    .and_then(|deadline| deadline.interval_at(now))
                    .map(NonZeroU32::get),
                topic_alias: None,
                response_topic: message
                    .response_topic
                    .as_ref()
                    .map(|topic| topic.as_str().to_owned()),
                correlation_data: message.correlation_data.clone(),
                user_properties: message.user_properties.clone(),
                // [MQTT-3.3.4-3], [MQTT-3.3.4-4]
                subscription_identifiers: queued
                    .subscription_ids
                    .iter()
                    .map(|&id| codec_subscription_id(id))
                    .collect(),
                content_type: message.content_type.clone(),
            },
            payload: message.payload.clone(),
        };
        let stored = publish.clone();
        let new_alias = self.apply_alias(stream, &queued.topic, &mut publish);
        if !self.fits(&publish) {
            // [MQTT-3.1.2-25], report R1 D6: discarded as if sent, before it takes a slot or an
            // identifier.
            return fx.push(Effect::Count(Counter::DeliveryTooLarge));
        }
        if let Some(alias) = new_alias {
            self.aliases_out.insert(queued.topic.clone(), alias);
        }
        if qos != CodecQoS::AtMostOnce {
            let Some(packet_id) = self.allocate() else {
                return;
            };
            publish.packet_id = Some(packet_id);
            let mut stored = stored;
            stored.packet_id = Some(packet_id);
            self.outbound.push_back(Outbound {
                packet_id,
                stage: if qos == CodecQoS::ExactlyOnce {
                    Stage::Receipt
                } else {
                    Stage::Acknowledgement
                },
                publish: Some(stored),
                stream,
                counted: true,
                sent: true,
            });
            self.outbound_in_window += 1;
        }
        self.send(stream, publish, now, fx);
    }

    /// Puts a Topic Alias on a PUBLISH for the control stream, and returns one assigned for
    /// the first time, to record once the packet goes ([MQTT-3.1.2-26], [MQTT-3.1.2-27],
    /// [MQTT-3.3.2-11]; report R1, O6).
    fn apply_alias(
        &self,
        stream: StreamId,
        topic: &TopicName,
        publish: &mut Publish,
    ) -> Option<NonZeroU16> {
        if stream != StreamId::Control {
            return None;
        }
        let client_maximum = self
            .client
            .as_ref()
            .map_or(0, |client| client.topic_alias_maximum);
        let maximum = client_maximum.min(self.config.topic_alias_maximum);
        if let Some(&alias) = self.aliases_out.get(topic) {
            publish.topic.clear();
            publish.properties.topic_alias = Some(alias);
            return None;
        }
        // Assigned on a topic's first use, from 1, never above the maximum and never remapped.
        let next = u16::try_from(self.aliases_out.len() + 1).ok()?;
        if next > maximum {
            return None;
        }
        // [MQTT-3.3.2-8]: never 0.
        let alias = NonZeroU16::new(next)?;
        publish.properties.topic_alias = Some(alias);
        Some(alias)
    }

    /// The stream a delivery goes on: its subscription's, unless the client ended it, when it
    /// is the control stream (docs/spec/mqtt-over-quic.md, section 2.4).
    fn delivery_stream(&self, stream: StreamId) -> StreamId {
        match stream {
            StreamId::Data(id) if self.streams.contains_key(&id) => StreamId::Control,
            stream => stream,
        }
    }

    /// The next Packet Identifier not in flight ([MQTT-2.2.1-4], [MQTT-4.3.2-1],
    /// [MQTT-4.3.3-1]; report R1, D7).
    fn allocate(&mut self) -> Option<PacketId> {
        if self.ids.len() >= usize::from(u16::MAX) {
            return None;
        }
        let mut candidate = self.next_id;
        loop {
            if let Some(packet_id) = PacketId::new(candidate)
                && self.ids.insert(candidate)
            {
                self.next_id = candidate.wrapping_add(1);
                return Some(packet_id);
            }
            candidate = candidate.wrapping_add(1);
        }
    }

    /// The position of the message in flight with `packet_id`.
    fn position(&self, packet_id: PacketId) -> Option<usize> {
        self.outbound
            .iter()
            .position(|outbound| outbound.packet_id == packet_id)
    }

    /// Ends the exchange of the message at `index`: its slot and its identifier are free.
    fn complete(&mut self, index: usize) {
        if let Some(outbound) = self.outbound.remove(index) {
            if outbound.counted {
                self.outbound_in_window = self.outbound_in_window.saturating_sub(1);
            }
            self.ids.remove(&outbound.packet_id.get());
        }
    }

    /// The client's PUBACK ([MQTT-4.3.2-3]).
    pub(super) fn puback(&mut self, ack: &PubAck, now: Timestamp, fx: &mut Effects) {
        match self.position(ack.packet_id) {
            Some(index)
                if self
                    .outbound
                    .get(index)
                    .is_some_and(|outbound| outbound.stage == Stage::Acknowledgement) =>
            {
                // A failure code acknowledges too, and the message is not sent again
                // ([MQTT-4.4.0-2]).
                self.complete(index);
                self.pump(now, fx);
            }
            _ => fx.push(Effect::Count(Counter::UnknownAcknowledgement)),
        }
    }

    /// The client's PUBREC ([MQTT-4.3.3-3]).
    pub(super) fn pubrec(
        &mut self,
        stream: StreamId,
        rec: &PubRec,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        let packet_id = rec.packet_id;
        let Some(index) = self.position(packet_id) else {
            // Not in flight: PUBREL 0x92, which section 3.6.2.1 has for this.
            return self.send(
                stream,
                PubRel {
                    reason_code: PubRelReasonCode::PacketIdentifierNotFound,
                    ..PubRel::new(packet_id)
                },
                now,
                fx,
            );
        };
        let stage = self.outbound.get(index).map(|outbound| outbound.stage);
        match stage {
            Some(Stage::Receipt) if rec.reason_code.is_error() => {
                // A failure PUBREC ends the exchange ([MQTT-4.3.3-4], [MQTT-4.4.0-2]; report
                // R1, D9).
                self.complete(index);
                self.pump(now, fx);
            }
            Some(Stage::Receipt) => {
                if let Some(outbound) = self.outbound.get_mut(index) {
                    outbound.stage = Stage::Completion;
                    // [MQTT-4.3.3-6]: never sent again once PUBREL is out.
                    outbound.publish = None;
                }
                // [MQTT-4.3.3-4], [MQTT-4.3.3-5]
                self.send(stream, PubRel::new(packet_id), now, fx);
            }
            // A repeated PUBREC gets PUBREL 0x00 again (report R1, D10; [MQTT-3.6.2-1]).
            Some(Stage::Completion) => self.send(stream, PubRel::new(packet_id), now, fx),
            _ => fx.push(Effect::Count(Counter::UnknownAcknowledgement)),
        }
    }

    /// The client's PUBCOMP.
    pub(super) fn pubcomp(&mut self, comp: &PubComp, now: Timestamp, fx: &mut Effects) {
        match self.position(comp.packet_id) {
            Some(index)
                if self
                    .outbound
                    .get(index)
                    .is_some_and(|outbound| outbound.stage == Stage::Completion) =>
            {
                self.complete(index);
                self.pump(now, fx);
            }
            _ => fx.push(Effect::Count(Counter::UnknownAcknowledgement)),
        }
    }
}
