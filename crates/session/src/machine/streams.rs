//! Data streams the client ends (docs/spec/mqtt-over-quic.md, section 2.4).

use openqtt_codec::{Packet, QoS};
use openqtt_core::Timestamp;

use super::{Authorizing, Phase, Received, Session};
use crate::{Effect, Effects, StreamEnd, StreamId};

impl Session {
    /// The client ended a data stream.
    pub(super) fn stream_ended(
        &mut self,
        id: u64,
        end: StreamEnd,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        if self.phase == Phase::Closed {
            return;
        }
        let stream = StreamId::Data(id);
        let state = self.streams.entry(id).or_default();
        match end {
            StreamEnd::ClientFinished => state.client_finished = true,
            StreamEnd::ServerStopped => state.server_stopped = true,
        }
        let (client_finished, server_stopped) = (state.client_finished, state.server_stopped);
        // An exchange on the stream that needs a packet from the client: a QoS 1 or 2 PUBLISH
        // the server sent and the client has not acknowledged in full, or a QoS 2 PUBLISH the
        // client sent whose PUBREL has not come, including one still being authorized or not
        // processed yet. It is never moved to another stream ([MQTT-4.4.0-1]), so the
        // connection closes and the session recovers on the next.
        let waiting_on_client = self
            .outbound
            .iter()
            .any(|outbound| outbound.stream == stream)
            || self
                .inbound
                .values()
                .any(|inbound| inbound.stream == stream)
            || self.unprocessed_qos2(stream);
        if client_finished && waiting_on_client {
            return self.protocol_error(now, fx);
        }
        // An acknowledgement the server owes on a stream it can no longer send on.
        if server_stopped && self.owes(stream) {
            return self.protocol_error(now, fx);
        }
        // Subscriptions made on the stream deliver on the control stream from now on.
        for subscription in self.subscriptions.values_mut() {
            if subscription.stream == stream {
                subscription.stream = StreamId::Control;
            }
        }
        self.finish_stream_if_done(stream, fx);
    }

    /// Whether a QoS 2 PUBLISH from the client on `stream` is being authorized or waits to be
    /// processed: its exchange will need a PUBREL on the stream.
    fn unprocessed_qos2(&self, stream: StreamId) -> bool {
        let authorizing = matches!(
            &self.authorizing,
            Some(Authorizing::Publish { stream: on, publish, .. })
                if *on == stream && publish.qos == QoS::ExactlyOnce
        );
        authorizing
            || self.inbox.iter().any(|received| {
                matches!(
                    received,
                    Received::Packet {
                        stream: on,
                        packet: Packet::Publish(publish),
                        ..
                    } if *on == stream && publish.qos == QoS::ExactlyOnce
                )
            })
    }

    /// Whether the server owes the client an acknowledgement on `stream`: one waiting for a
    /// commit or for the ones before it, one for the packet being authorized, or one for a
    /// packet not processed yet.
    fn owes(&self, stream: StreamId) -> bool {
        self.replies
            .get(&stream)
            .is_some_and(|replies| !replies.is_empty())
            || self
                .authorizing
                .as_ref()
                .is_some_and(|authorizing| authorizing.stream() == stream)
            || self.inbox.iter().any(|received| {
                received.stream() == stream
                    && matches!(
                        received,
                        Received::Packet {
                            packet: Packet::Publish(_)
                                | Packet::PubRel(_)
                                | Packet::PubRec(_)
                                | Packet::Subscribe(_)
                                | Packet::Unsubscribe(_),
                            ..
                        }
                    )
                    && !matches!(
                        received,
                        Received::Packet {
                            packet: Packet::Publish(publish),
                            ..
                        } if publish.qos == QoS::AtMostOnce
                    )
            })
    }

    /// Finishes the server's side of a data stream the client finished, once nothing more is
    /// owed on it.
    pub(super) fn finish_stream_if_done(&mut self, stream: StreamId, fx: &mut Effects) {
        let StreamId::Data(id) = stream else {
            return;
        };
        if self.phase == Phase::Closed || self.owes(stream) {
            return;
        }
        if let Some(state) = self.streams.get_mut(&id)
            && state.client_finished
            && !state.server_stopped
            && !state.finished
        {
            state.finished = true;
            fx.push(Effect::FinishStream(id));
        }
    }
}
