//! SUBSCRIBE and UNSUBSCRIBE.

use openqtt_codec::{
    AckProperties, PacketId, RetainHandling, SubAck, SubAckReasonCode, Subscribe, Subscription,
    SubscriptionOptions, UnsubAck, UnsubAckReasonCode, Unsubscribe,
};
use openqtt_core::{SubOpts, SubscriptionId, Timestamp, TopicFilter};

use super::{Authorizing, Planned, Session, Subscription as Held};
use crate::convert::{codec_qos, core_qos, core_retain_handling, core_subscription_id, granted};
use crate::phrase::phrase;
use crate::{
    Action, Authorization, Decision, Effect, Effects, Interest, RequestId, RetainedRead, StreamId,
};

impl Session {
    /// A SUBSCRIBE: each filter is handled as if it came alone, and one SUBACK answers them
    /// all ([MQTT-3.8.4-5]).
    pub(super) fn subscribe(
        &mut self,
        stream: StreamId,
        subscribe: Subscribe,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        // No Local on a Shared Subscription is a Protocol Error, which ends the connection
        // before any filter is acted on ([MQTT-3.8.3-4]).
        let shared_no_local = subscribe.subscriptions.iter().any(|subscription| {
            subscription.options.no_local
                && TopicFilter::new(&subscription.filter).is_ok_and(|filter| filter.is_shared())
        });
        if shared_no_local {
            return self.protocol_error(now, fx);
        }
        let identifier = subscribe.properties.subscription_identifier;
        let id = identifier.and_then(core_subscription_id);
        let plan: Vec<Planned> = subscribe
            .subscriptions
            .into_iter()
            .map(|subscription| self.plan(subscription, identifier.is_some()))
            .collect();
        let actions: Vec<Action> = plan
            .iter()
            .filter_map(|planned| match planned {
                Planned::Pending { filter, options } => Some(Action::Subscribe {
                    filter: filter.clone(),
                    qos: core_qos(options.maximum_qos.min(self.config.maximum_qos)),
                }),
                Planned::Refused(_) => None,
            })
            .collect();
        if actions.is_empty() {
            return self.answer_subscribe(stream, subscribe.packet_id, id, plan, &[], now, fx);
        }
        // Authorization sees the client's own filters (report R2, rules 7 and 11).
        let request = self.request();
        fx.push(Effect::Authorize(Authorization {
            request: RequestId(request),
            actions,
        }));
        self.authorizing = Some(Authorizing::Subscribe {
            request,
            stream,
            packet_id: subscribe.packet_id,
            id,
            plan,
        });
    }

    /// Checks one filter of a SUBSCRIBE. A filter that breaks sections 4.7 or 4.8 is refused
    /// alone, with 0x8F (report R1, O25 and D32).
    fn plan(&self, subscription: Subscription, identified: bool) -> Planned {
        let config = &self.config;
        if identified && !config.subscription_identifiers_available {
            return Planned::Refused(SubAckReasonCode::SubscriptionIdentifiersNotSupported);
        }
        let Ok(filter) = TopicFilter::new(&subscription.filter) else {
            return Planned::Refused(SubAckReasonCode::TopicFilterInvalid);
        };
        if filter.is_shared() && !config.shared_subscription_available {
            return Planned::Refused(SubAckReasonCode::SharedSubscriptionsNotSupported);
        }
        if filter.has_wildcard() && !config.wildcard_subscription_available {
            return Planned::Refused(SubAckReasonCode::WildcardSubscriptionsNotSupported);
        }
        if filter.level_count() > config.maximum_topic_levels {
            // Report R1, O16: limits apply before mounting.
            return Planned::Refused(SubAckReasonCode::TopicFilterInvalid);
        }
        Planned::Pending {
            filter,
            options: subscription.options,
        }
    }

    /// Makes the subscriptions a SUBSCRIBE was allowed, and answers it.
    #[expect(
        clippy::too_many_arguments,
        reason = "the parts of one SUBSCRIBE, kept while it waited for its authorization"
    )]
    pub(super) fn answer_subscribe(
        &mut self,
        stream: StreamId,
        packet_id: PacketId,
        id: Option<SubscriptionId>,
        plan: Vec<Planned>,
        decisions: &[Decision],
        now: Timestamp,
        fx: &mut Effects,
    ) {
        let mut decisions = decisions.iter();
        // [MQTT-3.8.4-6], [MQTT-3.9.3-1]: one code per filter, in order.
        let reason_codes: Vec<SubAckReasonCode> = plan
            .into_iter()
            .map(|planned| match planned {
                Planned::Refused(code) => code,
                // Report R1, D2; report R2, rule 12.
                Planned::Pending { .. } if decisions.next() != Some(&Decision::Allow) => {
                    SubAckReasonCode::NotAuthorized
                }
                Planned::Pending { filter, options } => {
                    self.add_subscription(stream, filter, options, id, fx)
                }
            })
            .collect();
        let reason_string = reason_codes
            .iter()
            .find(|code| code.is_error())
            .and_then(|code| phrase(code.value()));
        // [MQTT-3.8.4-1], [MQTT-3.8.4-2]: before any retained message (report R1, O2).
        self.send(
            stream,
            SubAck {
                packet_id,
                properties: AckProperties {
                    reason_string,
                    ..AckProperties::default()
                },
                reason_codes,
            },
            now,
            fx,
        );
        // A replacement that asks for no retained messages ends the wait of the deliveries
        // held for the subscription it replaced.
        self.release_held(now, fx);
        self.pump(now, fx);
        self.finish_stream_if_done(stream, fx);
    }

    /// Makes or replaces one subscription, and returns its SUBACK code.
    fn add_subscription(
        &mut self,
        stream: StreamId,
        filter: TopicFilter,
        options: SubscriptionOptions,
        id: Option<SubscriptionId>,
        fx: &mut Effects,
    ) -> SubAckReasonCode {
        let mounted = match self
            .client
            .as_ref()
            .and_then(|client| client.mount.as_ref())
        {
            Some(mount) => match mount.mount_filter(&filter) {
                Ok(mounted) => mounted,
                Err(_) => return SubAckReasonCode::TopicFilterInvalid,
            },
            None => filter.clone(),
        };
        // [MQTT-3.2.2-10]: any Requested QoS is accepted, and granted up to the server's
        // Maximum QoS.
        let qos = options.maximum_qos.min(self.config.maximum_qos);
        let existing = self.subscriptions.get(&mounted);
        if existing.is_none() && self.subscriptions.len() >= self.config.maximum_subscriptions {
            // Report R1, O16.
            return SubAckReasonCode::QuotaExceeded;
        }
        // [MQTT-3.3.1-9] to [MQTT-3.3.1-11]: retained messages as Retain Handling says, and
        // none for a Shared Subscription (section 4.8.2).
        let send_retained = !filter.is_shared()
            && match options.retain_handling {
                RetainHandling::SendAtSubscribe => true,
                RetainHandling::SendIfNew => existing.is_none(),
                RetainHandling::DoNotSend => false,
            };
        // A read the subscription it replaces waited for is stale from now on.
        if let Some(stale) = existing.and_then(|held| held.awaiting_read) {
            self.reads.remove(&stale);
        }
        let awaiting_read = send_retained.then(|| {
            let read = self.next_read;
            self.next_read += 1;
            self.reads.insert(read, mounted.clone());
            read
        });
        let mut granted_options = SubOpts::new(core_qos(qos));
        granted_options.no_local = options.no_local;
        granted_options.retain_as_published = options.retain_as_published;
        granted_options.retain_handling = core_retain_handling(options.retain_handling);
        granted_options.id = id;
        let order = self.next_subscription;
        self.next_subscription += 1;
        // [MQTT-3.8.4-3]: an identical filter replaces the subscription, in place, so no
        // message is lost to the replacement ([MQTT-3.8.4-4]).
        self.subscriptions.insert(
            mounted.clone(),
            Held {
                filter,
                options: granted_options,
                stream,
                order,
                awaiting_read,
            },
        );
        fx.push(Effect::Subscribe(Interest {
            filter: mounted,
            options: granted_options,
            retained: awaiting_read.map(RetainedRead),
        }));
        granted(codec_qos(granted_options.qos))
    }

    /// An UNSUBSCRIBE: each filter is handled as if it came alone, and one UNSUBACK answers
    /// them all ([MQTT-3.10.4-6]).
    pub(super) fn unsubscribe(
        &mut self,
        stream: StreamId,
        unsubscribe: &Unsubscribe,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        let mount = self.client.as_ref().and_then(|client| client.mount.clone());
        // [MQTT-3.11.3-1]: one code per filter, in order.
        let reason_codes: Vec<UnsubAckReasonCode> = unsubscribe
            .filters
            .iter()
            .map(|text| {
                // A filter that breaks section 4.7 or 4.8 is refused alone (report R1, D32).
                let Ok(filter) = TopicFilter::new(text) else {
                    return UnsubAckReasonCode::TopicFilterInvalid;
                };
                let mounted = match &mount {
                    Some(mount) => match mount.mount_filter(&filter) {
                        Ok(mounted) => mounted,
                        Err(_) => return UnsubAckReasonCode::TopicFilterInvalid,
                    },
                    None => filter,
                };
                // [MQTT-3.10.4-1]: compared byte for byte, and deleted on an exact match.
                // Deliveries already started complete ([MQTT-3.10.4-3]).
                if let Some(removed) = self.subscriptions.remove(&mounted) {
                    // An answer to the read it waited for is stale.
                    if let Some(stale) = removed.awaiting_read {
                        self.reads.remove(&stale);
                    }
                    fx.push(Effect::Unsubscribe(mounted));
                    UnsubAckReasonCode::Success
                } else {
                    // [MQTT-3.10.4-5]
                    UnsubAckReasonCode::NoSubscriptionExisted
                }
            })
            .collect();
        let reason_string = reason_codes
            .iter()
            .find(|code| code.is_error())
            .and_then(|code| phrase(code.value()));
        // [MQTT-3.10.4-4], [MQTT-3.10.4-5]
        self.send(
            stream,
            UnsubAck {
                packet_id: unsubscribe.packet_id,
                properties: AckProperties {
                    reason_string,
                    ..AckProperties::default()
                },
                reason_codes,
            },
            now,
            fx,
        );
        // Deliveries held for a subscription that is gone wait no longer.
        self.release_held(now, fx);
        self.pump(now, fx);
        self.finish_stream_if_done(stream, fx);
    }
}
