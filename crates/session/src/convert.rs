//! Between the codec's types and their twins in openqtt-core.
//!
//! The router and the log depend on core and must not depend on the codec (`make layers`), so
//! core has its own QoS, Payload Format Indicator, Retain Handling and Subscription Identifier.
//! The session sees both and converts here.

use std::num::NonZeroU32;

use openqtt_codec as codec;
use openqtt_core as core;

/// The core QoS of a codec QoS.
pub(crate) const fn core_qos(qos: codec::QoS) -> core::QoS {
    match qos {
        codec::QoS::AtMostOnce => core::QoS::AtMostOnce,
        codec::QoS::AtLeastOnce => core::QoS::AtLeastOnce,
        codec::QoS::ExactlyOnce => core::QoS::ExactlyOnce,
    }
}

/// The codec QoS of a core QoS.
pub(crate) const fn codec_qos(qos: core::QoS) -> codec::QoS {
    match qos {
        core::QoS::AtMostOnce => codec::QoS::AtMostOnce,
        core::QoS::AtLeastOnce => codec::QoS::AtLeastOnce,
        core::QoS::ExactlyOnce => codec::QoS::ExactlyOnce,
    }
}

/// The core Payload Format Indicator of a codec one.
pub(crate) const fn core_format(format: codec::PayloadFormat) -> core::PayloadFormat {
    match format {
        codec::PayloadFormat::Unspecified => core::PayloadFormat::Unspecified,
        codec::PayloadFormat::Utf8 => core::PayloadFormat::Utf8,
    }
}

/// The codec Payload Format Indicator of a core one.
pub(crate) const fn codec_format(format: core::PayloadFormat) -> codec::PayloadFormat {
    match format {
        core::PayloadFormat::Unspecified => codec::PayloadFormat::Unspecified,
        core::PayloadFormat::Utf8 => codec::PayloadFormat::Utf8,
    }
}

/// The core Retain Handling of a codec one.
pub(crate) const fn core_retain_handling(handling: codec::RetainHandling) -> core::RetainHandling {
    match handling {
        codec::RetainHandling::SendAtSubscribe => core::RetainHandling::SendAtSubscribe,
        codec::RetainHandling::SendIfNew => core::RetainHandling::SendIfNew,
        codec::RetainHandling::DoNotSend => core::RetainHandling::DoNotSend,
    }
}

/// The core Subscription Identifier of a codec one. The codec refuses a value above the
/// largest when it decodes, so `None` never comes from a decoded packet.
pub(crate) const fn core_subscription_id(id: NonZeroU32) -> Option<core::SubscriptionId> {
    core::SubscriptionId::new(id.get())
}

/// The codec Subscription Identifier of a core one.
pub(crate) const fn codec_subscription_id(id: core::SubscriptionId) -> NonZeroU32 {
    match NonZeroU32::new(id.get()) {
        Some(id) => id,
        // A core identifier is never 0.
        None => NonZeroU32::MIN,
    }
}

/// The SUBACK reason code granting `qos`.
pub(crate) const fn granted(qos: codec::QoS) -> codec::SubAckReasonCode {
    match qos {
        codec::QoS::AtMostOnce => codec::SubAckReasonCode::GrantedQos0,
        codec::QoS::AtLeastOnce => codec::SubAckReasonCode::GrantedQos1,
        codec::QoS::ExactlyOnce => codec::SubAckReasonCode::GrantedQos2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversions_round_trip() {
        for qos in [
            codec::QoS::AtMostOnce,
            codec::QoS::AtLeastOnce,
            codec::QoS::ExactlyOnce,
        ] {
            assert_eq!(codec_qos(core_qos(qos)), qos);
            assert_eq!(core_qos(qos).value(), qos.value());
            assert_eq!(granted(qos).value(), qos.value());
        }
        for format in [
            codec::PayloadFormat::Unspecified,
            codec::PayloadFormat::Utf8,
        ] {
            assert_eq!(codec_format(core_format(format)), format);
        }
        for handling in [
            codec::RetainHandling::SendAtSubscribe,
            codec::RetainHandling::SendIfNew,
            codec::RetainHandling::DoNotSend,
        ] {
            assert_eq!(core_retain_handling(handling).value(), handling.value());
        }
        let largest = NonZeroU32::new(268_435_455).unwrap();
        let id = core_subscription_id(largest).unwrap();
        assert_eq!(codec_subscription_id(id), largest);
        assert_eq!(
            core_subscription_id(NonZeroU32::new(268_435_456).unwrap()),
            None
        );
    }
}
