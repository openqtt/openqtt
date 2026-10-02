//! The metrics OpenQTT records, with names drafted for report R9.
//!
//! [`Metrics`] holds one handle for each instrument in [`INSTRUMENTS`], made once from a
//! [`Meter`]. Recording is a method call whose attributes are typed, so a role can neither
//! invent a name nor misspell a value, and an attribute never carries a client identifier or a
//! topic: those are unbounded, and every distinct value would be a series of its own.
//!
//! Names follow OpenTelemetry's conventions: lowercase, dotted, the unit kept apart. A
//! Prometheus exporter writes `openqtt.packets.received` as `openqtt_packets_received_total` and
//! the attribute `packet.type` as the label `packet_type`.

use opentelemetry::metrics::{Counter, Meter, UpDownCounter};
use opentelemetry::{InstrumentationScope, Key, KeyValue, Value, global};

/// The instrumentation scope of every OpenQTT instrument.
pub const SCOPE: &str = "openqtt";

/// The meter of OpenQTT's scope, from the provider the binary installed.
///
/// An instrument made before the binary installs its provider records nothing for as long as it
/// lives, so a role makes its [`Metrics`] after the binary has started observability.
pub fn meter() -> Meter {
    global::meter_with_scope(
        InstrumentationScope::builder(SCOPE)
            .with_version(env!("CARGO_PKG_VERSION"))
            .build(),
    )
}

/// What kind of instrument a metric is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A sum that only rises.
    Counter,
    /// A sum that rises and falls: how many of something there are now.
    UpDownCounter,
}

/// One instrument, as report R9 will list it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Instrument {
    /// The name.
    pub name: &'static str,
    /// The kind.
    pub kind: Kind,
    /// The unit, in UCUM as OpenTelemetry writes it: `By` for bytes, `{packet}` for a count of
    /// packets.
    pub unit: &'static str,
    /// The attributes every value carries.
    pub attributes: &'static [&'static str],
    /// What it counts.
    pub description: &'static str,
}

/// The attribute keys, each of which takes the words of one type below.
pub mod attribute {
    /// A [`PacketType`](super::PacketType).
    pub const PACKET_TYPE: &str = "packet.type";
    /// A [`Qos`](super::Qos).
    pub const QOS: &str = "qos";
    /// A [`DropReason`](super::DropReason).
    pub const REASON: &str = "reason";
    /// A [`Decision`](super::Decision).
    pub const RESULT: &str = "result";
    /// An [`Action`](super::Action).
    pub const ACTION: &str = "action";
}

/// Client connections open now.
pub const CONNECTIONS: Instrument = Instrument {
    name: "openqtt.connections",
    kind: Kind::UpDownCounter,
    unit: "{connection}",
    attributes: &[],
    description: "Client connections open on this edge.",
};

/// MQTT packets read from clients.
pub const PACKETS_RECEIVED: Instrument = Instrument {
    name: "openqtt.packets.received",
    kind: Kind::Counter,
    unit: "{packet}",
    attributes: &[attribute::PACKET_TYPE],
    description: "MQTT packets received from clients, by type.",
};

/// MQTT packets written to clients.
pub const PACKETS_SENT: Instrument = Instrument {
    name: "openqtt.packets.sent",
    kind: Kind::Counter,
    unit: "{packet}",
    attributes: &[attribute::PACKET_TYPE],
    description: "MQTT packets sent to clients, by type.",
};

/// Bytes of MQTT packets read from clients.
pub const BYTES_RECEIVED: Instrument = Instrument {
    name: "openqtt.bytes.received",
    kind: Kind::Counter,
    unit: "By",
    attributes: &[],
    description: "Bytes of MQTT packets received from clients.",
};

/// Bytes of MQTT packets written to clients.
pub const BYTES_SENT: Instrument = Instrument {
    name: "openqtt.bytes.sent",
    kind: Kind::Counter,
    unit: "By",
    attributes: &[],
    description: "Bytes of MQTT packets sent to clients.",
};

/// Messages taken from clients for delivery.
pub const MESSAGES_RECEIVED: Instrument = Instrument {
    name: "openqtt.messages.received",
    kind: Kind::Counter,
    unit: "{message}",
    attributes: &[attribute::QOS],
    description: "Messages accepted from clients for delivery, authorized and valid, by QoS.",
};

/// Messages delivered to clients.
pub const MESSAGES_SENT: Instrument = Instrument {
    name: "openqtt.messages.sent",
    kind: Kind::Counter,
    unit: "{message}",
    attributes: &[attribute::QOS],
    description: "Messages delivered to clients, by QoS, not counting retransmissions.",
};

/// Messages, or copies of one, that reached no client.
pub const DELIVERIES_DROPPED: Instrument = Instrument {
    name: "openqtt.deliveries.dropped",
    kind: Kind::Counter,
    unit: "{delivery}",
    attributes: &[attribute::REASON],
    description: "Messages, or copies of one, that reached no client, by reason.",
};

/// Authentication decisions.
pub const AUTHENTICATIONS: Instrument = Instrument {
    name: "openqtt.authentications",
    kind: Kind::Counter,
    unit: "{decision}",
    attributes: &[attribute::RESULT],
    description: "Authentication decisions on connecting clients, by result.",
};

/// Authorization decisions.
pub const AUTHORIZATIONS: Instrument = Instrument {
    name: "openqtt.authorizations",
    kind: Kind::Counter,
    unit: "{decision}",
    attributes: &[attribute::ACTION, attribute::RESULT],
    description: "Authorization decisions on publishes and subscriptions, by action and result.",
};

/// Sessions taken over by a second connection with the same client identifier.
pub const SESSION_TAKEOVERS: Instrument = Instrument {
    name: "openqtt.session.takeovers",
    kind: Kind::Counter,
    unit: "{takeover}",
    attributes: &[],
    description: "Sessions taken over by a new connection with the same client identifier.",
};

/// Every instrument [`Metrics`] makes.
pub const INSTRUMENTS: [Instrument; 11] = [
    CONNECTIONS,
    PACKETS_RECEIVED,
    PACKETS_SENT,
    BYTES_RECEIVED,
    BYTES_SENT,
    MESSAGES_RECEIVED,
    MESSAGES_SENT,
    DELIVERIES_DROPPED,
    AUTHENTICATIONS,
    AUTHORIZATIONS,
    SESSION_TAKEOVERS,
];

/// Declares the words one attribute takes.
macro_rules! words {
    (
        $(#[$meta:meta])*
        pub enum $name:ident: $key:path {
            $(
                $(#[doc = $doc:literal])*
                $variant:ident = $text:literal,
            )+
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name {
            $(
                $(#[doc = $doc])*
                $variant,
            )+
        }

        impl $name {
            /// Every value.
            pub const ALL: &'static [Self] = &[$(Self::$variant,)+];

            /// The value as the attribute carries it.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text,)+
                }
            }

            /// The attribute, with a static key and value, so recording allocates nothing.
            fn attribute(self) -> KeyValue {
                KeyValue::new(Key::from_static_str($key), Value::from(self.as_str()))
            }
        }
    };
}

words! {
    /// The type of an MQTT packet.
    pub enum PacketType: attribute::PACKET_TYPE {
        /// CONNECT.
        Connect = "connect",
        /// CONNACK.
        Connack = "connack",
        /// PUBLISH.
        Publish = "publish",
        /// PUBACK.
        Puback = "puback",
        /// PUBREC.
        Pubrec = "pubrec",
        /// PUBREL.
        Pubrel = "pubrel",
        /// PUBCOMP.
        Pubcomp = "pubcomp",
        /// SUBSCRIBE.
        Subscribe = "subscribe",
        /// SUBACK.
        Suback = "suback",
        /// UNSUBSCRIBE.
        Unsubscribe = "unsubscribe",
        /// UNSUBACK.
        Unsuback = "unsuback",
        /// PINGREQ.
        Pingreq = "pingreq",
        /// PINGRESP.
        Pingresp = "pingresp",
        /// DISCONNECT.
        Disconnect = "disconnect",
        /// AUTH.
        Auth = "auth",
    }
}

words! {
    /// The quality of service of a message.
    pub enum Qos: attribute::QOS {
        /// At most once.
        AtMostOnce = "0",
        /// At least once.
        AtLeastOnce = "1",
        /// Exactly once.
        ExactlyOnce = "2",
    }
}

words! {
    /// Why a message, or a copy of one, reached no client.
    pub enum DropReason: attribute::REASON {
        /// It reached an edge none of whose clients wanted it, through coarse interest (R3, R6
        /// D4). How often this happens tunes the interest threshold.
        NoSubscriber = "no_subscriber",
        /// Its Message Expiry Interval passed before it was sent (R1, O8).
        Expired = "expired",
        /// It was larger than the client's Maximum Packet Size ([MQTT-3.1.2-25]).
        TooLarge = "too_large",
        /// It was QoS 0, for a session with no connection (R1, O13).
        OfflineQos0 = "offline_qos0",
        /// The session's queue was full, which ends the session (R1, O12).
        QueueFull = "queue_full",
        /// It was a QoS 0 publish that authorization denied (R1, O14).
        Denied = "denied",
        /// It was a QoS 0 publish with a topic MQTT or the limits refuse (R1, O16 and O25).
        InvalidTopic = "invalid_topic",
    }
}

words! {
    /// The result of an authentication or authorization decision.
    pub enum Decision: attribute::RESULT {
        /// Allowed.
        Allow = "allow",
        /// Refused.
        Deny = "deny",
        /// Not decided, because what decides it failed: the client is refused.
        Error = "error",
    }
}

words! {
    /// What a client asked to be authorized for.
    pub enum Action: attribute::ACTION {
        /// A publish.
        Publish = "publish",
        /// A subscription.
        Subscribe = "subscribe",
    }
}

/// A handle on every instrument in [`INSTRUMENTS`]. Made once, from [`meter`] in the broker or
/// from a test's own meter, and cheap to clone into each task.
#[derive(Debug, Clone)]
pub struct Metrics {
    connections: UpDownCounter<i64>,
    packets_received: Counter<u64>,
    packets_sent: Counter<u64>,
    bytes_received: Counter<u64>,
    bytes_sent: Counter<u64>,
    messages_received: Counter<u64>,
    messages_sent: Counter<u64>,
    deliveries_dropped: Counter<u64>,
    authentications: Counter<u64>,
    authorizations: Counter<u64>,
    session_takeovers: Counter<u64>,
}

impl Metrics {
    /// Makes every instrument on `meter`.
    pub fn new(meter: &Meter) -> Self {
        let counter = |instrument: Instrument| {
            meter
                .u64_counter(instrument.name)
                .with_unit(instrument.unit)
                .with_description(instrument.description)
                .build()
        };
        Self {
            connections: meter
                .i64_up_down_counter(CONNECTIONS.name)
                .with_unit(CONNECTIONS.unit)
                .with_description(CONNECTIONS.description)
                .build(),
            packets_received: counter(PACKETS_RECEIVED),
            packets_sent: counter(PACKETS_SENT),
            bytes_received: counter(BYTES_RECEIVED),
            bytes_sent: counter(BYTES_SENT),
            messages_received: counter(MESSAGES_RECEIVED),
            messages_sent: counter(MESSAGES_SENT),
            deliveries_dropped: counter(DELIVERIES_DROPPED),
            authentications: counter(AUTHENTICATIONS),
            authorizations: counter(AUTHORIZATIONS),
            session_takeovers: counter(SESSION_TAKEOVERS),
        }
    }

    /// A client connection opened.
    pub fn connection_opened(&self) {
        self.connections.add(1, &[]);
    }

    /// A client connection closed, however it ended.
    pub fn connection_closed(&self) {
        self.connections.add(-1, &[]);
    }

    /// A packet of `bytes` bytes was read from a client.
    pub fn packet_received(&self, packet: PacketType, bytes: u64) {
        self.packets_received.add(1, &[packet.attribute()]);
        self.bytes_received.add(bytes, &[]);
    }

    /// A packet of `bytes` bytes was written to a client.
    pub fn packet_sent(&self, packet: PacketType, bytes: u64) {
        self.packets_sent.add(1, &[packet.attribute()]);
        self.bytes_sent.add(bytes, &[]);
    }

    /// A message from a client was accepted for delivery.
    pub fn message_received(&self, qos: Qos) {
        self.messages_received.add(1, &[qos.attribute()]);
    }

    /// A message was delivered to a client.
    pub fn message_sent(&self, qos: Qos) {
        self.messages_sent.add(1, &[qos.attribute()]);
    }

    /// A message, or a copy of one, reached no client.
    pub fn delivery_dropped(&self, reason: DropReason) {
        self.deliveries_dropped.add(1, &[reason.attribute()]);
    }

    /// A connecting client's authentication was decided.
    pub fn authenticated(&self, result: Decision) {
        self.authentications.add(1, &[result.attribute()]);
    }

    /// A publish or a subscription was authorized, or not.
    pub fn authorized(&self, action: Action, result: Decision) {
        self.authorizations
            .add(1, &[action.attribute(), result.attribute()]);
    }

    /// A session was taken over by a new connection, and the old one told DISCONNECT 0x8E.
    pub fn session_taken_over(&self) {
        self.session_takeovers.add(1, &[]);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

    use super::*;

    /// One value of a recorded sum: its attributes as `key=value`, sorted.
    type Point = (Vec<String>, i64);

    /// The values an instrument should hold: each one's attributes, and the value.
    type Expected = &'static [(&'static [&'static str], i64)];

    /// Records through a meter of an in-memory SDK, then reads back every sum by name.
    fn record(work: impl FnOnce(&Metrics)) -> BTreeMap<String, (String, String, Vec<Point>)> {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let meter = provider.meter_with_scope(InstrumentationScope::builder(SCOPE).build());
        work(&Metrics::new(&meter));
        provider.force_flush().unwrap();

        let mut sums = BTreeMap::new();
        for resource in exporter.get_finished_metrics().unwrap() {
            for scope in resource.scope_metrics() {
                assert_eq!(scope.scope().name(), SCOPE);
                for metric in scope.metrics() {
                    let points: Vec<Point> = match metric.data() {
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                            .data_points()
                            .map(|point| {
                                (
                                    labels(point.attributes()),
                                    i64::try_from(point.value()).unwrap(),
                                )
                            })
                            .collect(),
                        AggregatedMetrics::I64(MetricData::Sum(sum)) => sum
                            .data_points()
                            .map(|point| (labels(point.attributes()), point.value()))
                            .collect(),
                        other => panic!("{} is not a sum: {other:?}", metric.name()),
                    };
                    sums.insert(
                        metric.name().to_owned(),
                        (
                            metric.unit().to_owned(),
                            metric.description().to_owned(),
                            points,
                        ),
                    );
                }
            }
        }
        sums
    }

    fn labels<'a>(attributes: impl Iterator<Item = &'a KeyValue>) -> Vec<String> {
        let mut labels: Vec<String> = attributes
            .map(|attribute| format!("{}={}", attribute.key, attribute.value))
            .collect();
        labels.sort();
        labels
    }

    #[test]
    fn every_instrument_records_under_its_name_unit_and_attributes() {
        let sums = record(|metrics| {
            metrics.connection_opened();
            metrics.connection_opened();
            metrics.connection_closed();
            metrics.packet_received(PacketType::Connect, 40);
            metrics.packet_received(PacketType::Publish, 100);
            metrics.packet_sent(PacketType::Connack, 9);
            metrics.message_received(Qos::AtLeastOnce);
            metrics.message_sent(Qos::AtMostOnce);
            metrics.delivery_dropped(DropReason::NoSubscriber);
            metrics.authenticated(Decision::Allow);
            metrics.authorized(Action::Publish, Decision::Deny);
            metrics.session_taken_over();
        });

        let expected: [(Instrument, Expected); 11] = [
            (CONNECTIONS, &[(&[], 1)]),
            (
                PACKETS_RECEIVED,
                &[(&["packet.type=connect"], 1), (&["packet.type=publish"], 1)],
            ),
            (PACKETS_SENT, &[(&["packet.type=connack"], 1)]),
            (BYTES_RECEIVED, &[(&[], 140)]),
            (BYTES_SENT, &[(&[], 9)]),
            (MESSAGES_RECEIVED, &[(&["qos=1"], 1)]),
            (MESSAGES_SENT, &[(&["qos=0"], 1)]),
            (DELIVERIES_DROPPED, &[(&["reason=no_subscriber"], 1)]),
            (AUTHENTICATIONS, &[(&["result=allow"], 1)]),
            (AUTHORIZATIONS, &[(&["action=publish", "result=deny"], 1)]),
            (SESSION_TAKEOVERS, &[(&[], 1)]),
        ];
        assert_eq!(sums.len(), INSTRUMENTS.len());
        for (instrument, points) in expected {
            let (unit, description, mut recorded) = sums[instrument.name].clone();
            assert_eq!(unit, instrument.unit, "{}", instrument.name);
            assert_eq!(description, instrument.description, "{}", instrument.name);
            recorded.sort();
            let points: Vec<Point> = points
                .iter()
                .map(|(labels, value)| (labels.iter().map(ToString::to_string).collect(), *value))
                .collect();
            assert_eq!(recorded, points, "{}", instrument.name);
            for (labels, _) in &recorded {
                let keys: BTreeSet<&str> = labels
                    .iter()
                    .map(|label| label.split_once('=').unwrap().0)
                    .collect();
                assert_eq!(
                    keys,
                    instrument
                        .attributes
                        .iter()
                        .copied()
                        .collect::<BTreeSet<&str>>(),
                    "{}",
                    instrument.name
                );
            }
        }
    }

    #[test]
    fn names_follow_the_conventions_and_none_repeats() {
        let mut names = BTreeSet::new();
        for instrument in INSTRUMENTS {
            let name = instrument.name;
            assert!(name.starts_with("openqtt."), "{name}");
            assert!(
                name.bytes()
                    .all(|b| b.is_ascii_lowercase() || b == b'.' || b == b'_'),
                "{name}"
            );
            assert!(
                !instrument.unit.is_empty() && !instrument.description.is_empty(),
                "{name}"
            );
            assert!(names.insert(name), "{name} twice");
        }
    }

    #[test]
    fn attribute_values_are_lowercase_words() {
        let words: Vec<&str> = PacketType::ALL
            .iter()
            .map(|value| value.as_str())
            .chain(Qos::ALL.iter().map(|value| value.as_str()))
            .chain(DropReason::ALL.iter().map(|value| value.as_str()))
            .chain(Decision::ALL.iter().map(|value| value.as_str()))
            .chain(Action::ALL.iter().map(|value| value.as_str()))
            .collect();
        for word in words {
            assert!(
                word.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                "{word}"
            );
        }
        assert_eq!(PacketType::ALL.len(), 15, "every MQTT 5 packet type");
    }

    #[test]
    fn without_a_provider_recording_does_nothing_and_cannot_fail() {
        let metrics = Metrics::new(&meter());
        metrics.connection_opened();
        metrics.packet_received(PacketType::Publish, 1);
        metrics.authorized(Action::Subscribe, Decision::Error);
    }
}
