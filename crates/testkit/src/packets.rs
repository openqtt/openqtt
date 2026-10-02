//! Short constructors for the packets scenarios send, so a scenario reads as the exchange it
//! plays. Anything they do not cover is a struct literal of `openqtt-codec`.

use bytes::Bytes;
use openqtt_codec::{
    Connect, PacketId, PubAck, PubComp, PubRec, PubRel, Publish, QoS, Subscribe,
    SubscribeProperties, Subscription, SubscriptionOptions, Unsubscribe, UnsubscribeProperties,
};

/// A Packet Identifier.
///
/// # Panics
///
/// For 0, which is never a Packet Identifier: a mistake in the test, not in a broker.
pub fn id(value: u16) -> PacketId {
    PacketId::new(value).expect("a Packet Identifier is never 0")
}

/// CONNECT as `client_id`, with Clean Start 1, Keep Alive 60 and no properties.
pub fn connect(client_id: &str) -> Connect {
    Connect {
        clean_start: true,
        keep_alive: 60,
        client_id: client_id.to_owned(),
        ..Connect::default()
    }
}

/// PUBLISH of `payload` to `topic`. `packet_id` is used at QoS 1 and 2 and ignored at QoS 0.
pub fn publish(topic: &str, qos: QoS, packet_id: u16, payload: &str) -> Publish {
    Publish {
        qos,
        topic: topic.to_owned(),
        packet_id: (qos != QoS::AtMostOnce).then(|| id(packet_id)),
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        ..Publish::default()
    }
}

/// SUBSCRIBE to one filter at a Maximum QoS, with the other options at their defaults.
pub fn subscribe(packet_id: u16, filter: &str, qos: QoS) -> Subscribe {
    subscribe_with(
        packet_id,
        filter,
        SubscriptionOptions {
            maximum_qos: qos,
            ..SubscriptionOptions::default()
        },
    )
}

/// SUBSCRIBE to one filter with these options.
pub fn subscribe_with(packet_id: u16, filter: &str, options: SubscriptionOptions) -> Subscribe {
    Subscribe {
        packet_id: id(packet_id),
        properties: SubscribeProperties::default(),
        subscriptions: vec![Subscription {
            filter: filter.to_owned(),
            options,
        }],
    }
}

/// UNSUBSCRIBE from these filters.
pub fn unsubscribe(packet_id: u16, filters: &[&str]) -> Unsubscribe {
    Unsubscribe {
        packet_id: id(packet_id),
        properties: UnsubscribeProperties::default(),
        filters: filters.iter().map(|filter| (*filter).to_owned()).collect(),
    }
}

/// PUBACK, Success.
pub fn puback(packet_id: u16) -> PubAck {
    PubAck::new(id(packet_id))
}

/// PUBREC, Success.
pub fn pubrec(packet_id: u16) -> PubRec {
    PubRec::new(id(packet_id))
}

/// PUBREL, Success.
pub fn pubrel(packet_id: u16) -> PubRel {
    PubRel::new(id(packet_id))
}

/// PUBCOMP, Success.
pub fn pubcomp(packet_id: u16) -> PubComp {
    PubComp::new(id(packet_id))
}
