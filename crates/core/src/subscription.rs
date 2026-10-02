//! What a subscription asks for.

use std::fmt;
use std::num::NonZeroU32;

use crate::QoS;

/// The options of one subscription (section 3.8.3.1), as a session keeps them and a log
/// partition holding a durable subscription sees them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub struct SubOpts {
    /// Maximum QoS: the most a delivery to this subscription gets, the message's own QoS
    /// capping it ([MQTT-3.8.4-8]).
    pub qos: QoS,
    /// No Local: the client's own messages are not delivered to it ([MQTT-3.8.3-3]), which a
    /// shared subscription may not ask for ([MQTT-3.8.3-4]).
    pub no_local: bool,
    /// Retain As Published: deliveries keep the RETAIN flag they were published with
    /// ([MQTT-3.3.1-13]).
    pub retain_as_published: bool,
    /// When retained messages are sent for the subscription.
    pub retain_handling: RetainHandling,
    /// The Subscription Identifier of the SUBSCRIBE that made it, which a delivery for it
    /// carries ([MQTT-3.3.4-3]).
    pub id: Option<SubscriptionId>,
}

impl SubOpts {
    /// Options with this Maximum QoS and the rest at their defaults: retained messages sent at
    /// subscription, and no flags or identifier.
    pub fn new(qos: QoS) -> Self {
        Self {
            qos,
            ..Self::default()
        }
    }
}

/// The Retain Handling option of a subscription (section 3.8.3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum RetainHandling {
    /// 0: send the retained messages when the subscription is made ([MQTT-3.3.1-9]).
    #[default]
    SendAtSubscribe = 0,
    /// 1: send them only if the subscription did not exist ([MQTT-3.3.1-10]).
    SendIfNew = 1,
    /// 2: do not send them ([MQTT-3.3.1-11]).
    DoNotSend = 2,
}

impl RetainHandling {
    /// The Retain Handling with this value, or `None` for 3 and above.
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::SendAtSubscribe),
            1 => Some(Self::SendIfNew),
            2 => Some(Self::DoNotSend),
            _ => None,
        }
    }

    /// The value, 0 to 2.
    pub const fn value(self) -> u8 {
        self as u8
    }
}

/// A Subscription Identifier, 1 to 268,435,455: what a Variable Byte Integer holds, less 0
/// (section 3.8.2.1.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SubscriptionId(NonZeroU32);

impl SubscriptionId {
    /// The largest identifier.
    pub const MAX: u32 = 268_435_455;

    /// The identifier `value`, or `None` for 0 and anything above [`Self::MAX`].
    pub const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(id) if value <= Self::MAX => Some(Self(id)),
            _ => None,
        }
    }

    /// The value.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_default_to_what_a_bare_subscribe_asks_for() {
        let options = SubOpts::new(QoS::AtLeastOnce);
        assert_eq!(options.qos, QoS::AtLeastOnce);
        assert!(!options.no_local && !options.retain_as_published);
        assert_eq!(options.retain_handling, RetainHandling::SendAtSubscribe);
        assert_eq!(options.id, None);
        assert_eq!(SubOpts::default().qos, QoS::AtMostOnce);
    }

    #[test]
    fn retain_handling_values() {
        for value in 0..3 {
            assert_eq!(RetainHandling::from_u8(value).unwrap().value(), value);
        }
        assert_eq!(RetainHandling::from_u8(3), None);
    }

    #[test]
    fn a_subscription_identifier_is_1_to_268435455() {
        assert_eq!(SubscriptionId::new(0), None);
        assert_eq!(SubscriptionId::new(1).map(SubscriptionId::get), Some(1));
        assert_eq!(
            SubscriptionId::new(268_435_455).map(SubscriptionId::get),
            Some(SubscriptionId::MAX)
        );
        assert_eq!(SubscriptionId::new(268_435_456), None);
        assert_eq!(SubscriptionId::new(u32::MAX), None);
        assert_eq!(SubscriptionId::new(42).unwrap().to_string(), "42");
    }
}
