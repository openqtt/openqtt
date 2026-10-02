//! The Application Message as it travels between roles, and when it expires.

use std::fmt;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use openqtt_topic::TopicName;

use crate::ClientId;

/// Quality of Service, the delivery guarantee of a message (sections 3.3.1.2 and 4.3). Ordered,
/// so the lesser of two is their minimum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
#[repr(u8)]
pub enum QoS {
    /// QoS 0: at most once.
    #[default]
    AtMostOnce = 0,
    /// QoS 1: at least once.
    AtLeastOnce = 1,
    /// QoS 2: exactly once.
    ExactlyOnce = 2,
}

impl QoS {
    /// The QoS with this value, or `None` for 3 and above, which are not QoS levels.
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::AtMostOnce),
            1 => Some(Self::AtLeastOnce),
            2 => Some(Self::ExactlyOnce),
            _ => None,
        }
    }

    /// The value, 0 to 2.
    pub const fn value(self) -> u8 {
        self as u8
    }
}

impl fmt::Display for QoS {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QoS {}", self.value())
    }
}

/// The Payload Format Indicator of a message (section 3.3.2.3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum PayloadFormat {
    /// 0: unspecified bytes, the same as no indicator.
    #[default]
    Unspecified = 0,
    /// 1: UTF-8 Encoded Character Data.
    Utf8 = 1,
}

impl PayloadFormat {
    /// The format with this value, or `None` for anything but 0 and 1.
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Utf8),
            _ => None,
        }
    }

    /// The value, 0 or 1.
    pub const fn value(self) -> u8 {
        self as u8
    }
}

/// A moment on the server's clock, to the nanosecond: nanoseconds since the Unix epoch.
///
/// A message crosses processes, from edge to edge and through the log, so the moments it
/// carries are wall-clock time rather than a process's monotonic clock: the edge that receives
/// a message reads the time once, and whichever pod delivers it compares against its own clock.
/// Nanoseconds in a `u64` reach into the year 2554.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Timestamp(u64);

impl Timestamp {
    /// The Unix epoch, 1970-01-01T00:00:00Z.
    pub const UNIX_EPOCH: Self = Self(0);

    /// The moment `nanos` nanoseconds after the Unix epoch.
    pub const fn from_unix_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Nanoseconds since the Unix epoch.
    pub const fn as_unix_nanos(self) -> u64 {
        self.0
    }

    /// `time` as a timestamp, or `None` before the Unix epoch or past the year 2554.
    pub fn from_system_time(time: SystemTime) -> Option<Self> {
        let since = time.duration_since(SystemTime::UNIX_EPOCH).ok()?;
        u64::try_from(since.as_nanos()).ok().map(Self)
    }

    /// The moment `duration` later, or the last representable one.
    pub fn saturating_add(self, duration: Duration) -> Self {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        Self(self.0.saturating_add(nanos))
    }

    /// The time from `earlier` to this moment, or zero if `earlier` is later.
    pub fn saturating_duration_since(self, earlier: Self) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}

/// When a message expires: the moment it was received plus its Message Expiry Interval, on the
/// server's clock at full resolution (report R1, O8).
///
/// From its deadline on a copy is expired, and an expired copy whose delivery has not started
/// is deleted ([MQTT-3.3.2-5]). So an interval of 1 s is expired 1 s after receipt, and an
/// interval of 0 at receipt: such a message reaches no subscriber and is never queued, and a
/// retained one replaces the topic's retained message and expires with it, leaving the topic
/// with none. The PUBLISH is still acknowledged with 0x00. A copy that is sent carries the time
/// left in whole seconds, rounded up, so never 0 ([MQTT-3.3.2-6]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Deadline {
    at: Timestamp,
    /// The interval as received, which no copy is sent with more of.
    interval: u32,
}

impl Deadline {
    /// The deadline of a message received at `received` with a Message Expiry Interval of
    /// `interval` seconds.
    pub fn after(received: Timestamp, interval: u32) -> Self {
        Self {
            at: received.saturating_add(Duration::from_secs(u64::from(interval))),
            interval,
        }
    }

    /// The moment the message expires.
    pub const fn at(self) -> Timestamp {
        self.at
    }

    /// The Message Expiry Interval it was received with, in seconds.
    pub const fn interval(self) -> u32 {
        self.interval
    }

    /// Whether a copy has expired at `now`: from the deadline itself on.
    pub fn is_expired(self, now: Timestamp) -> bool {
        now >= self.at
    }

    /// The Message Expiry Interval to send with a copy at `now`: the time left, rounded up to
    /// whole seconds, and never more than the interval received, even when the delivering pod's
    /// clock is behind the receiving one's. `None` once the copy has expired, when it is not
    /// sent at all.
    pub fn interval_at(self, now: Timestamp) -> Option<NonZeroU32> {
        if self.is_expired(now) {
            return None;
        }
        let left = self.at.as_unix_nanos() - now.as_unix_nanos();
        let seconds = u32::try_from(left.div_ceil(1_000_000_000)).unwrap_or(u32::MAX);
        NonZeroU32::new(seconds.min(self.interval))
    }
}

/// An Application Message as it travels past the edge that received it: to subscribers on that
/// edge, to other edges, and to log partitions (report R3).
///
/// It holds what every subscriber must receive unaltered, and what the broker needs to deliver
/// it. What belongs to one hop is not here: the Packet Identifier and DUP, a Topic Alias, which
/// belongs to its connection ([MQTT-3.3.2-7]), and the Subscription Identifiers, which depend
/// on the subscriptions a delivery matched ([MQTT-3.3.4-3]) and go with each delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Message {
    /// The Topic Name, mounted: as the rest of the cluster sees it (report R2, rule 6).
    pub topic: TopicName,
    /// The payload. A clone shares the bytes, and a decoded one shares the buffer it arrived in.
    pub payload: Bytes,
    /// The QoS it was published with, which no delivery exceeds ([MQTT-3.8.4-8]).
    pub qos: QoS,
    /// RETAIN as published. A delivery keeps it only for a subscription with Retain As
    /// Published, or when it is sent because a subscription was made ([MQTT-3.3.1-12],
    /// [MQTT-3.3.1-13]).
    pub retain: bool,
    /// The Client Identifier of the client that published it, a will included, so that No
    /// Local can hold its own messages back from it wherever they are delivered
    /// ([MQTT-3.8.3-3]). `None` for a message the platform published through the admin API (R2,
    /// rule 21).
    pub publisher: Option<ClientId>,
    /// The Payload Format Indicator, if one was sent ([MQTT-3.3.2-4]).
    pub payload_format: Option<PayloadFormat>,
    /// When it expires, if a Message Expiry Interval was sent.
    pub expiry: Option<Deadline>,
    /// The Content Type ([MQTT-3.3.2-20]).
    pub content_type: Option<String>,
    /// The Response Topic, a Topic Name ([MQTT-3.3.2-14]) that is never mounted or stripped
    /// ([MQTT-3.3.2-15]).
    pub response_topic: Option<TopicName>,
    /// The Correlation Data ([MQTT-3.3.2-16]).
    pub correlation_data: Option<Bytes>,
    /// User Properties, name and value, in the order received ([MQTT-3.3.2-17],
    /// [MQTT-3.3.2-18]).
    pub user_properties: Vec<(String, String)>,
}

impl Message {
    /// A message at QoS 0, not retained, with no publisher and no properties: set the rest
    /// field by field.
    pub fn new(topic: TopicName, payload: Bytes) -> Self {
        Self {
            topic,
            payload,
            qos: QoS::AtMostOnce,
            retain: false,
            publisher: None,
            payload_format: None,
            expiry: None,
            content_type: None,
            response_topic: None,
            correlation_data: None,
            user_properties: Vec::new(),
        }
    }

    /// Whether the message has expired at `now`; one without a Message Expiry Interval never
    /// does.
    pub fn is_expired(&self, now: Timestamp) -> bool {
        self.expiry.is_some_and(|deadline| deadline.is_expired(now))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: u64 = 1_000_000_000;

    fn at(nanos: u64) -> Timestamp {
        Timestamp::from_unix_nanos(nanos)
    }

    fn seconds(value: u32) -> Option<NonZeroU32> {
        NonZeroU32::new(value)
    }

    #[test]
    fn qos_values_and_order() {
        assert_eq!(QoS::from_u8(0), Some(QoS::AtMostOnce));
        assert_eq!(QoS::from_u8(1), Some(QoS::AtLeastOnce));
        assert_eq!(QoS::from_u8(2), Some(QoS::ExactlyOnce));
        assert_eq!(QoS::from_u8(3), None);
        assert_eq!(QoS::ExactlyOnce.min(QoS::AtLeastOnce), QoS::AtLeastOnce);
        assert_eq!(QoS::AtLeastOnce.value(), 1);
        assert_eq!(QoS::ExactlyOnce.to_string(), "QoS 2");
        assert_eq!(PayloadFormat::from_u8(1), Some(PayloadFormat::Utf8));
        assert_eq!(PayloadFormat::from_u8(2), None);
        assert_eq!(PayloadFormat::Utf8.value(), 1);
    }

    #[test]
    fn r1_o8_a_copy_expires_at_its_deadline() {
        let received = at(1_000 * SECOND + 123);
        let one = Deadline::after(received, 1);
        assert_eq!(one.at(), at(1_001 * SECOND + 123));
        assert_eq!(one.interval(), 1);
        assert!(!one.is_expired(received));
        assert!(!one.is_expired(at(1_001 * SECOND + 122)));
        // An interval of 1 s is expired 1 s after receipt, to the nanosecond.
        assert!(one.is_expired(at(1_001 * SECOND + 123)));
        assert!(one.is_expired(at(2_000 * SECOND)));
    }

    #[test]
    fn r1_o8_an_interval_of_zero_expires_at_receipt() {
        let received = at(5 * SECOND);
        let zero = Deadline::after(received, 0);
        assert!(zero.is_expired(received));
        assert_eq!(zero.interval_at(received), None);
        let mut message = Message::new(TopicName::new("t").unwrap(), Bytes::new());
        assert!(!message.is_expired(received));
        message.expiry = Some(zero);
        assert!(message.is_expired(received));
    }

    #[test]
    fn r1_o8_the_interval_sent_is_the_time_left_rounded_up() {
        let received = at(10 * SECOND);
        let deadline = Deadline::after(received, 10);
        assert_eq!(deadline.interval_at(received), seconds(10));
        // A nanosecond later the time left is 9.999999999 s, which rounds up to 10.
        assert_eq!(deadline.interval_at(at(10 * SECOND + 1)), seconds(10));
        assert_eq!(deadline.interval_at(at(11 * SECOND)), seconds(9));
        assert_eq!(deadline.interval_at(at(19 * SECOND + 1)), seconds(1));
        // Never 0: the last nanosecond still sends 1, and the deadline sends nothing.
        assert_eq!(deadline.interval_at(at(20 * SECOND - 1)), seconds(1));
        assert_eq!(deadline.interval_at(at(20 * SECOND)), None);
    }

    #[test]
    fn r1_o8_a_clock_behind_never_sends_more_than_was_received() {
        let deadline = Deadline::after(at(10 * SECOND), 10);
        // The delivering pod's clock is 5 ms behind the receiving one's.
        assert_eq!(
            deadline.interval_at(at(10 * SECOND - 5_000_000)),
            seconds(10)
        );
        assert_eq!(deadline.interval_at(Timestamp::UNIX_EPOCH), seconds(10));
    }

    #[test]
    fn the_longest_interval_fits() {
        let received = at(1_800_000_000 * SECOND);
        let deadline = Deadline::after(received, u32::MAX);
        assert_eq!(deadline.interval_at(received), seconds(u32::MAX));
        assert!(!deadline.is_expired(received));
        // Past the year 2554 the deadline stops at the last moment there is.
        let late = Deadline::after(at(u64::MAX - SECOND), 2);
        assert_eq!(late.at(), at(u64::MAX));
    }

    #[test]
    fn timestamps_from_the_system_clock() {
        assert_eq!(
            Timestamp::from_system_time(SystemTime::UNIX_EPOCH),
            Some(Timestamp::UNIX_EPOCH)
        );
        let later = SystemTime::UNIX_EPOCH + Duration::new(1_759_400_000, 7);
        assert_eq!(
            Timestamp::from_system_time(later),
            Some(at(1_759_400_000 * SECOND + 7))
        );
        let before = SystemTime::UNIX_EPOCH - Duration::from_secs(1);
        assert_eq!(Timestamp::from_system_time(before), None);
        let a = at(SECOND);
        assert_eq!(
            a.saturating_add(Duration::from_millis(1)),
            at(SECOND + 1_000_000)
        );
        assert_eq!(a.saturating_add(Duration::MAX), at(u64::MAX));
        assert_eq!(
            at(3 * SECOND).saturating_duration_since(a),
            Duration::from_secs(2)
        );
        assert_eq!(a.saturating_duration_since(at(3 * SECOND)), Duration::ZERO);
    }

    #[test]
    fn a_new_message_has_no_properties() {
        let topic = TopicName::new("ingest/acme/production/pump-3/telemetry").unwrap();
        let payload = Bytes::from_static(b"21.5");
        let mut message = Message::new(topic.clone(), payload.clone());
        assert_eq!(message.topic, topic);
        assert_eq!(message.payload, payload);
        assert_eq!((message.qos, message.retain), (QoS::AtMostOnce, false));
        assert_eq!(message.publisher, None);
        assert_eq!(message.expiry, None);
        assert!(message.user_properties.is_empty());
        // A clone shares the payload rather than copying it.
        message.qos = QoS::AtLeastOnce;
        let copy = message.clone();
        assert_eq!(copy.payload.as_ptr(), message.payload.as_ptr());
        assert_eq!(copy, message);
    }
}
