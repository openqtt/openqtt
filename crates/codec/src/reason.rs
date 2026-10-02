//! Reason codes: the common set of section 2.4 (Table 2-6) and the table each packet draws
//! from (Tables 3-1 and 3-4 to 3-11).
//!
//! A packet's reason code is typed by its own table, so a PUBACK cannot carry 0x8E (Session
//! taken over) and a decoder refuses one that does. [`ReasonCode`] is the common set, for code
//! that handles reason codes from any packet.

use crate::{PacketType, Sender};

/// Defines a reason code enum over `u8` with its constants and conversions.
macro_rules! reason_code_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $( $(#[$variant_meta:meta])* $variant:ident = $value:literal, )+
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[repr(u8)]
        pub enum $name {
            $( $(#[$variant_meta])* $variant = $value, )+
        }

        impl $name {
            /// Every code, in order of value.
            pub const ALL: &'static [Self] = &[$( Self::$variant ),+];

            /// The code with this value, or `None` when the value is not one of them.
            pub const fn from_u8(value: u8) -> Option<Self> {
                match value {
                    $( $value => Some(Self::$variant), )+
                    _ => None,
                }
            }

            /// The value on the wire.
            pub const fn value(self) -> u8 {
                self as u8
            }

            /// Whether the code reports a failure: 0x80 or above (section 2.4).
            pub const fn is_error(self) -> bool {
                self.value() >= 0x80
            }
        }

        impl From<$name> for u8 {
            fn from(code: $name) -> Self {
                code.value()
            }
        }
    };
}

/// Converts a packet's reason code into the common set it is drawn from.
macro_rules! into_reason_code {
    ($($name:ident),+) => {
        $(
            impl From<$name> for ReasonCode {
                fn from(code: $name) -> Self {
                    // Every code of every packet's table is in Table 2-6, which the tests
                    // check, so the fallback is never taken.
                    match ReasonCode::from_u8(code.value()) {
                        Some(common) => common,
                        None => ReasonCode::UnspecifiedError,
                    }
                }
            }
        )+
    };
}

reason_code_enum! {
    /// Every reason code, with the packets each may appear in (section 2.4, Table 2-6).
    ///
    /// 0x00 has three names in the specification: Success, Normal disconnection in DISCONNECT
    /// and Granted QoS 0 in SUBACK. Here it is [`ReasonCode::Success`].
    pub enum ReasonCode {
        /// 0x00 Success; Normal disconnection in DISCONNECT, Granted QoS 0 in SUBACK.
        Success = 0x00,
        /// 0x01 Granted QoS 1.
        GrantedQos1 = 0x01,
        /// 0x02 Granted QoS 2.
        GrantedQos2 = 0x02,
        /// 0x04 Disconnect with Will Message.
        DisconnectWithWillMessage = 0x04,
        /// 0x10 No matching subscribers.
        NoMatchingSubscribers = 0x10,
        /// 0x11 No subscription existed.
        NoSubscriptionExisted = 0x11,
        /// 0x18 Continue authentication.
        ContinueAuthentication = 0x18,
        /// 0x19 Re-authenticate.
        ReAuthenticate = 0x19,
        /// 0x80 Unspecified error.
        UnspecifiedError = 0x80,
        /// 0x81 Malformed Packet.
        MalformedPacket = 0x81,
        /// 0x82 Protocol Error.
        ProtocolError = 0x82,
        /// 0x83 Implementation specific error.
        ImplementationSpecificError = 0x83,
        /// 0x84 Unsupported Protocol Version.
        UnsupportedProtocolVersion = 0x84,
        /// 0x85 Client Identifier not valid.
        ClientIdentifierNotValid = 0x85,
        /// 0x86 Bad User Name or Password.
        BadUserNameOrPassword = 0x86,
        /// 0x87 Not authorized.
        NotAuthorized = 0x87,
        /// 0x88 Server unavailable.
        ServerUnavailable = 0x88,
        /// 0x89 Server busy.
        ServerBusy = 0x89,
        /// 0x8A Banned.
        Banned = 0x8A,
        /// 0x8B Server shutting down.
        ServerShuttingDown = 0x8B,
        /// 0x8C Bad authentication method.
        BadAuthenticationMethod = 0x8C,
        /// 0x8D Keep Alive timeout.
        KeepAliveTimeout = 0x8D,
        /// 0x8E Session taken over.
        SessionTakenOver = 0x8E,
        /// 0x8F Topic Filter invalid.
        TopicFilterInvalid = 0x8F,
        /// 0x90 Topic Name invalid.
        TopicNameInvalid = 0x90,
        /// 0x91 Packet Identifier in use.
        PacketIdentifierInUse = 0x91,
        /// 0x92 Packet Identifier not found.
        PacketIdentifierNotFound = 0x92,
        /// 0x93 Receive Maximum exceeded.
        ReceiveMaximumExceeded = 0x93,
        /// 0x94 Topic Alias invalid.
        TopicAliasInvalid = 0x94,
        /// 0x95 Packet too large.
        PacketTooLarge = 0x95,
        /// 0x96 Message rate too high.
        MessageRateTooHigh = 0x96,
        /// 0x97 Quota exceeded.
        QuotaExceeded = 0x97,
        /// 0x98 Administrative action.
        AdministrativeAction = 0x98,
        /// 0x99 Payload format invalid.
        PayloadFormatInvalid = 0x99,
        /// 0x9A Retain not supported.
        RetainNotSupported = 0x9A,
        /// 0x9B QoS not supported.
        QosNotSupported = 0x9B,
        /// 0x9C Use another server.
        UseAnotherServer = 0x9C,
        /// 0x9D Server moved.
        ServerMoved = 0x9D,
        /// 0x9E Shared Subscriptions not supported.
        SharedSubscriptionsNotSupported = 0x9E,
        /// 0x9F Connection rate exceeded.
        ConnectionRateExceeded = 0x9F,
        /// 0xA0 Maximum connect time.
        MaximumConnectTime = 0xA0,
        /// 0xA1 Subscription Identifiers not supported.
        SubscriptionIdentifiersNotSupported = 0xA1,
        /// 0xA2 Wildcard Subscriptions not supported.
        WildcardSubscriptionsNotSupported = 0xA2,
    }
}

/// One bit per packet type, for the Packets column of Table 2-6.
const fn bit(packet_type: PacketType) -> u16 {
    1 << packet_type.value()
}

const CONNACK: u16 = bit(PacketType::ConnAck);
const PUBACK: u16 = bit(PacketType::PubAck);
const PUBREC: u16 = bit(PacketType::PubRec);
const PUBREL: u16 = bit(PacketType::PubRel);
const PUBCOMP: u16 = bit(PacketType::PubComp);
const SUBACK: u16 = bit(PacketType::SubAck);
const UNSUBACK: u16 = bit(PacketType::UnsubAck);
const DISCONNECT: u16 = bit(PacketType::Disconnect);
const AUTH: u16 = bit(PacketType::Auth);

impl ReasonCode {
    /// Whether Table 2-6 lists this code for `packet_type`.
    ///
    /// Table 2-6 lists 0x8C (Bad authentication method) for DISCONNECT, which Table 3-10, the
    /// DISCONNECT table, leaves out. The codec follows Table 2-6 and accepts it, since a failed
    /// re-authentication (section 4.12.1) ends with a DISCONNECT and this is its reason.
    pub const fn is_valid_in(self, packet_type: PacketType) -> bool {
        let packets = match self {
            Self::Success => {
                CONNACK | PUBACK | PUBREC | PUBREL | PUBCOMP | SUBACK | UNSUBACK | DISCONNECT | AUTH
            }
            Self::GrantedQos1 | Self::GrantedQos2 => SUBACK,
            Self::DisconnectWithWillMessage
            | Self::ServerShuttingDown
            | Self::KeepAliveTimeout
            | Self::SessionTakenOver
            | Self::ReceiveMaximumExceeded
            | Self::TopicAliasInvalid
            | Self::MessageRateTooHigh
            | Self::AdministrativeAction
            | Self::MaximumConnectTime => DISCONNECT,
            Self::NoMatchingSubscribers => PUBACK | PUBREC,
            Self::NoSubscriptionExisted => UNSUBACK,
            Self::ContinueAuthentication | Self::ReAuthenticate => AUTH,
            Self::UnspecifiedError | Self::ImplementationSpecificError | Self::NotAuthorized => {
                CONNACK | PUBACK | PUBREC | SUBACK | UNSUBACK | DISCONNECT
            }
            Self::MalformedPacket
            | Self::ProtocolError
            | Self::ServerBusy
            | Self::BadAuthenticationMethod
            | Self::PacketTooLarge
            | Self::RetainNotSupported
            | Self::QosNotSupported
            | Self::UseAnotherServer
            | Self::ServerMoved
            | Self::ConnectionRateExceeded => CONNACK | DISCONNECT,
            Self::UnsupportedProtocolVersion
            | Self::ClientIdentifierNotValid
            | Self::BadUserNameOrPassword
            | Self::ServerUnavailable
            | Self::Banned => CONNACK,
            Self::TopicFilterInvalid => SUBACK | UNSUBACK | DISCONNECT,
            Self::TopicNameInvalid | Self::PayloadFormatInvalid => {
                CONNACK | PUBACK | PUBREC | DISCONNECT
            }
            Self::PacketIdentifierInUse => PUBACK | PUBREC | SUBACK | UNSUBACK,
            Self::PacketIdentifierNotFound => PUBREL | PUBCOMP,
            Self::QuotaExceeded => CONNACK | PUBACK | PUBREC | SUBACK | DISCONNECT,
            Self::SharedSubscriptionsNotSupported
            | Self::SubscriptionIdentifiersNotSupported
            | Self::WildcardSubscriptionsNotSupported => SUBACK | DISCONNECT,
        };
        packets & bit(packet_type) != 0
    }
}

reason_code_enum! {
    /// The Connect Reason Code of a CONNACK (section 3.2.2.2, Table 3-1). A server uses one of
    /// these ([MQTT-3.2.2-8]), and a code of 0x80 or above closes the connection
    /// ([MQTT-3.2.2-7]).
    pub enum ConnectReasonCode {
        /// 0x00 Success: the connection is accepted.
        Success = 0x00,
        /// 0x80 Unspecified error: the server does not wish to reveal the reason, or none of
        /// the other codes apply.
        UnspecifiedError = 0x80,
        /// 0x81 Malformed Packet: data within the CONNECT could not be correctly parsed.
        MalformedPacket = 0x81,
        /// 0x82 Protocol Error: data in the CONNECT does not conform to the specification.
        ProtocolError = 0x82,
        /// 0x83 Implementation specific error: the CONNECT is valid but not accepted by this
        /// server.
        ImplementationSpecificError = 0x83,
        /// 0x84 Unsupported Protocol Version: the server does not support the version of MQTT
        /// the client requested.
        UnsupportedProtocolVersion = 0x84,
        /// 0x85 Client Identifier not valid: a valid string the server does not allow.
        ClientIdentifierNotValid = 0x85,
        /// 0x86 Bad User Name or Password.
        BadUserNameOrPassword = 0x86,
        /// 0x87 Not authorized: the client may not connect.
        NotAuthorized = 0x87,
        /// 0x88 Server unavailable.
        ServerUnavailable = 0x88,
        /// 0x89 Server busy: try again later.
        ServerBusy = 0x89,
        /// 0x8A Banned by administrative action.
        Banned = 0x8A,
        /// 0x8C Bad authentication method: not supported, or not the one in use.
        BadAuthenticationMethod = 0x8C,
        /// 0x90 Topic Name invalid: the Will Topic is not malformed but not accepted.
        TopicNameInvalid = 0x90,
        /// 0x95 Packet too large: the CONNECT exceeded the maximum permissible size.
        PacketTooLarge = 0x95,
        /// 0x97 Quota exceeded: an implementation or administrative limit.
        QuotaExceeded = 0x97,
        /// 0x99 Payload format invalid: the Will Payload does not match its Payload Format
        /// Indicator.
        PayloadFormatInvalid = 0x99,
        /// 0x9A Retain not supported, and Will Retain was set.
        RetainNotSupported = 0x9A,
        /// 0x9B QoS not supported: the server does not support the Will QoS.
        QosNotSupported = 0x9B,
        /// 0x9C Use another server, temporarily.
        UseAnotherServer = 0x9C,
        /// 0x9D Server moved: use another server permanently.
        ServerMoved = 0x9D,
        /// 0x9F Connection rate exceeded.
        ConnectionRateExceeded = 0x9F,
    }
}

reason_code_enum! {
    /// The PUBACK Reason Code (section 3.4.2.1, Table 3-4); its sender uses one of these
    /// ([MQTT-3.4.2-1]).
    pub enum PubAckReasonCode {
        /// 0x00 Success: the message is accepted and publication of the QoS 1 message proceeds.
        Success = 0x00,
        /// 0x10 No matching subscribers: accepted, but nobody subscribes. Sent only by the
        /// server.
        NoMatchingSubscribers = 0x10,
        /// 0x80 Unspecified error: the publish is not accepted.
        UnspecifiedError = 0x80,
        /// 0x83 Implementation specific error: the PUBLISH is valid but not accepted.
        ImplementationSpecificError = 0x83,
        /// 0x87 Not authorized: the PUBLISH is not authorized.
        NotAuthorized = 0x87,
        /// 0x90 Topic Name invalid: not malformed, but not accepted.
        TopicNameInvalid = 0x90,
        /// 0x91 Packet Identifier in use.
        PacketIdentifierInUse = 0x91,
        /// 0x97 Quota exceeded.
        QuotaExceeded = 0x97,
        /// 0x99 Payload format invalid: the payload does not match its Payload Format
        /// Indicator.
        PayloadFormatInvalid = 0x99,
    }
}

reason_code_enum! {
    /// The PUBREC Reason Code (section 3.5.2.1, Table 3-5); its sender uses one of these
    /// ([MQTT-3.5.2-1]). The same values as PUBACK.
    pub enum PubRecReasonCode {
        /// 0x00 Success: the message is accepted and publication of the QoS 2 message proceeds.
        Success = 0x00,
        /// 0x10 No matching subscribers: accepted, but nobody subscribes. Sent only by the
        /// server.
        NoMatchingSubscribers = 0x10,
        /// 0x80 Unspecified error: the publish is not accepted.
        UnspecifiedError = 0x80,
        /// 0x83 Implementation specific error: the PUBLISH is valid but not accepted.
        ImplementationSpecificError = 0x83,
        /// 0x87 Not authorized: the PUBLISH is not authorized.
        NotAuthorized = 0x87,
        /// 0x90 Topic Name invalid: not malformed, but not accepted.
        TopicNameInvalid = 0x90,
        /// 0x91 Packet Identifier in use.
        PacketIdentifierInUse = 0x91,
        /// 0x97 Quota exceeded.
        QuotaExceeded = 0x97,
        /// 0x99 Payload format invalid: the payload does not match its Payload Format
        /// Indicator.
        PayloadFormatInvalid = 0x99,
    }
}

reason_code_enum! {
    /// The PUBREL Reason Code (section 3.6.2.1, Table 3-6); its sender uses one of these
    /// ([MQTT-3.6.2-1]).
    pub enum PubRelReasonCode {
        /// 0x00 Success: message released.
        Success = 0x00,
        /// 0x92 Packet Identifier not found: not an error during recovery, otherwise a
        /// mismatch of session state.
        PacketIdentifierNotFound = 0x92,
    }
}

reason_code_enum! {
    /// The PUBCOMP Reason Code (section 3.7.2.1, Table 3-7); its sender uses one of these
    /// ([MQTT-3.7.2-1]).
    pub enum PubCompReasonCode {
        /// 0x00 Success: Packet Identifier released, publication of the QoS 2 message complete.
        Success = 0x00,
        /// 0x92 Packet Identifier not found: not an error during recovery, otherwise a
        /// mismatch of session state.
        PacketIdentifierNotFound = 0x92,
    }
}

reason_code_enum! {
    /// A Subscribe Reason Code, one per Topic Filter of the SUBSCRIBE (section 3.9.3, Table
    /// 3-8); the server uses one of these for each ([MQTT-3.9.3-2]).
    pub enum SubAckReasonCode {
        /// 0x00 Granted QoS 0: accepted, delivered at QoS 0 at most.
        GrantedQos0 = 0x00,
        /// 0x01 Granted QoS 1: accepted, delivered at QoS 1 at most.
        GrantedQos1 = 0x01,
        /// 0x02 Granted QoS 2: accepted, delivered at any QoS.
        GrantedQos2 = 0x02,
        /// 0x80 Unspecified error: the subscription is not accepted.
        UnspecifiedError = 0x80,
        /// 0x83 Implementation specific error: the SUBSCRIBE is valid but not accepted.
        ImplementationSpecificError = 0x83,
        /// 0x87 Not authorized to make this subscription.
        NotAuthorized = 0x87,
        /// 0x8F Topic Filter invalid: correctly formed but not allowed for this client.
        TopicFilterInvalid = 0x8F,
        /// 0x91 Packet Identifier in use.
        PacketIdentifierInUse = 0x91,
        /// 0x97 Quota exceeded.
        QuotaExceeded = 0x97,
        /// 0x9E Shared Subscriptions not supported for this client.
        SharedSubscriptionsNotSupported = 0x9E,
        /// 0xA1 Subscription Identifiers not supported.
        SubscriptionIdentifiersNotSupported = 0xA1,
        /// 0xA2 Wildcard Subscriptions not supported.
        WildcardSubscriptionsNotSupported = 0xA2,
    }
}

reason_code_enum! {
    /// An Unsubscribe Reason Code, one per Topic Filter of the UNSUBSCRIBE (section 3.11.3,
    /// Table 3-9); the server uses one of these for each ([MQTT-3.11.3-2]).
    pub enum UnsubAckReasonCode {
        /// 0x00 Success: the subscription is deleted.
        Success = 0x00,
        /// 0x11 No subscription existed with that Topic Filter.
        NoSubscriptionExisted = 0x11,
        /// 0x80 Unspecified error: the unsubscribe could not be completed.
        UnspecifiedError = 0x80,
        /// 0x83 Implementation specific error: the UNSUBSCRIBE is valid but not accepted.
        ImplementationSpecificError = 0x83,
        /// 0x87 Not authorized to unsubscribe.
        NotAuthorized = 0x87,
        /// 0x8F Topic Filter invalid: correctly formed but not allowed for this client.
        TopicFilterInvalid = 0x8F,
        /// 0x91 Packet Identifier in use.
        PacketIdentifierInUse = 0x91,
    }
}

reason_code_enum! {
    /// The Disconnect Reason Code (section 3.14.2.1, Table 3-10); its sender uses one of these
    /// ([MQTT-3.14.2-1]).
    ///
    /// It includes 0x8C (Bad authentication method), which Table 2-6 lists for DISCONNECT and
    /// Table 3-10 leaves out; see [`ReasonCode::is_valid_in`].
    pub enum DisconnectReasonCode {
        /// 0x00 Normal disconnection: close normally and do not send the Will Message.
        NormalDisconnection = 0x00,
        /// 0x04 Disconnect with Will Message: the client wants its Will Message published.
        DisconnectWithWillMessage = 0x04,
        /// 0x80 Unspecified error.
        UnspecifiedError = 0x80,
        /// 0x81 Malformed Packet: the received packet does not conform to the specification.
        MalformedPacket = 0x81,
        /// 0x82 Protocol Error: an unexpected or out of order packet was received.
        ProtocolError = 0x82,
        /// 0x83 Implementation specific error: valid, but this implementation cannot process
        /// it.
        ImplementationSpecificError = 0x83,
        /// 0x87 Not authorized.
        NotAuthorized = 0x87,
        /// 0x89 Server busy.
        ServerBusy = 0x89,
        /// 0x8B Server shutting down.
        ServerShuttingDown = 0x8B,
        /// 0x8C Bad authentication method.
        BadAuthenticationMethod = 0x8C,
        /// 0x8D Keep Alive timeout: nothing received for 1.5 times the Keep Alive.
        KeepAliveTimeout = 0x8D,
        /// 0x8E Session taken over by another connection with the same Client Identifier.
        SessionTakenOver = 0x8E,
        /// 0x8F Topic Filter invalid: correctly formed but not accepted.
        TopicFilterInvalid = 0x8F,
        /// 0x90 Topic Name invalid: correctly formed but not accepted.
        TopicNameInvalid = 0x90,
        /// 0x93 Receive Maximum exceeded.
        ReceiveMaximumExceeded = 0x93,
        /// 0x94 Topic Alias invalid.
        TopicAliasInvalid = 0x94,
        /// 0x95 Packet too large.
        PacketTooLarge = 0x95,
        /// 0x96 Message rate too high.
        MessageRateTooHigh = 0x96,
        /// 0x97 Quota exceeded.
        QuotaExceeded = 0x97,
        /// 0x98 Administrative action.
        AdministrativeAction = 0x98,
        /// 0x99 Payload format invalid.
        PayloadFormatInvalid = 0x99,
        /// 0x9A Retain not supported.
        RetainNotSupported = 0x9A,
        /// 0x9B QoS not supported: above the Maximum QoS in the CONNACK.
        QosNotSupported = 0x9B,
        /// 0x9C Use another server, temporarily.
        UseAnotherServer = 0x9C,
        /// 0x9D Server moved: use another server permanently.
        ServerMoved = 0x9D,
        /// 0x9E Shared Subscriptions not supported.
        SharedSubscriptionsNotSupported = 0x9E,
        /// 0x9F Connection rate exceeded.
        ConnectionRateExceeded = 0x9F,
        /// 0xA0 Maximum connect time exceeded.
        MaximumConnectTime = 0xA0,
        /// 0xA1 Subscription Identifiers not supported.
        SubscriptionIdentifiersNotSupported = 0xA1,
        /// 0xA2 Wildcard Subscriptions not supported.
        WildcardSubscriptionsNotSupported = 0xA2,
    }
}

reason_code_enum! {
    /// The Authenticate Reason Code of an AUTH (section 3.15.2.1, Table 3-11); its sender uses
    /// one of these ([MQTT-3.15.2-1]).
    pub enum AuthReasonCode {
        /// 0x00 Success: authentication is successful. Sent by the server.
        Success = 0x00,
        /// 0x18 Continue authentication with another step. Sent by either end.
        ContinueAuthentication = 0x18,
        /// 0x19 Re-authenticate: start a re-authentication. Sent by the client.
        ReAuthenticate = 0x19,
    }
}

into_reason_code!(
    ConnectReasonCode,
    PubAckReasonCode,
    PubRecReasonCode,
    PubRelReasonCode,
    PubCompReasonCode,
    SubAckReasonCode,
    UnsubAckReasonCode,
    DisconnectReasonCode,
    AuthReasonCode
);

impl PubAckReasonCode {
    /// Whether `sender` may use this code: 0x10 (No matching subscribers) is sent only by the
    /// server (Table 3-4).
    pub const fn is_sent_by(self, sender: Sender) -> bool {
        !matches!(self, Self::NoMatchingSubscribers) || matches!(sender, Sender::Server)
    }
}

impl PubRecReasonCode {
    /// Whether `sender` may use this code: 0x10 (No matching subscribers) is sent only by the
    /// server (Table 3-5).
    pub const fn is_sent_by(self, sender: Sender) -> bool {
        !matches!(self, Self::NoMatchingSubscribers) || matches!(sender, Sender::Server)
    }
}

impl DisconnectReasonCode {
    /// Whether `sender` may use this code, by the Sent by column of Table 3-10. 0x8C, which the
    /// table leaves out, may come from either end.
    pub const fn is_sent_by(self, sender: Sender) -> bool {
        match self {
            Self::DisconnectWithWillMessage => matches!(sender, Sender::Client),
            Self::NotAuthorized
            | Self::ServerBusy
            | Self::ServerShuttingDown
            | Self::KeepAliveTimeout
            | Self::SessionTakenOver
            | Self::TopicFilterInvalid
            | Self::RetainNotSupported
            | Self::QosNotSupported
            | Self::UseAnotherServer
            | Self::ServerMoved
            | Self::SharedSubscriptionsNotSupported
            | Self::ConnectionRateExceeded
            | Self::MaximumConnectTime
            | Self::SubscriptionIdentifiersNotSupported
            | Self::WildcardSubscriptionsNotSupported => matches!(sender, Sender::Server),
            Self::NormalDisconnection
            | Self::UnspecifiedError
            | Self::MalformedPacket
            | Self::ProtocolError
            | Self::ImplementationSpecificError
            | Self::BadAuthenticationMethod
            | Self::TopicNameInvalid
            | Self::ReceiveMaximumExceeded
            | Self::TopicAliasInvalid
            | Self::PacketTooLarge
            | Self::MessageRateTooHigh
            | Self::QuotaExceeded
            | Self::AdministrativeAction
            | Self::PayloadFormatInvalid => true,
        }
    }
}

impl AuthReasonCode {
    /// Whether `sender` may use this code, by the Sent by column of Table 3-11.
    pub const fn is_sent_by(self, sender: Sender) -> bool {
        match self {
            Self::Success => matches!(sender, Sender::Server),
            Self::ContinueAuthentication => true,
            Self::ReAuthenticate => matches!(sender, Sender::Client),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The values of a per-packet table, as the codec types them.
    fn values(packet_type: PacketType) -> Vec<u8> {
        let all: Vec<u8> = (0..=u8::MAX).collect();
        let decode: fn(u8) -> bool = match packet_type {
            PacketType::ConnAck => |v| ConnectReasonCode::from_u8(v).is_some(),
            PacketType::PubAck => |v| PubAckReasonCode::from_u8(v).is_some(),
            PacketType::PubRec => |v| PubRecReasonCode::from_u8(v).is_some(),
            PacketType::PubRel => |v| PubRelReasonCode::from_u8(v).is_some(),
            PacketType::PubComp => |v| PubCompReasonCode::from_u8(v).is_some(),
            PacketType::SubAck => |v| SubAckReasonCode::from_u8(v).is_some(),
            PacketType::UnsubAck => |v| UnsubAckReasonCode::from_u8(v).is_some(),
            PacketType::Disconnect => |v| DisconnectReasonCode::from_u8(v).is_some(),
            PacketType::Auth => |v| AuthReasonCode::from_u8(v).is_some(),
            _ => |_| false,
        };
        all.into_iter().filter(|&v| decode(v)).collect()
    }

    #[test]
    fn table_2_6_has_43_codes_and_round_trips() {
        assert_eq!(ReasonCode::ALL.len(), 43);
        for value in 0..=u8::MAX {
            if let Some(code) = ReasonCode::from_u8(value) {
                assert_eq!(code.value(), value);
                assert_eq!(code.is_error(), value >= 0x80);
            }
        }
        assert_eq!(ReasonCode::from_u8(0x03), None);
        assert_eq!(ReasonCode::from_u8(0xA3), None);
    }

    #[test]
    fn every_packet_table_matches_the_packets_column_of_table_2_6() {
        for packet_type in PacketType::ALL {
            let from_table_2_6: Vec<u8> = (0..=u8::MAX)
                .filter(|&v| ReasonCode::from_u8(v).is_some_and(|c| c.is_valid_in(packet_type)))
                .collect();
            assert_eq!(values(packet_type), from_table_2_6, "{packet_type}");
        }
    }

    #[test]
    fn per_packet_tables_as_the_specification_prints_them() {
        let table_3_1 = [
            0x00, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8C, 0x90,
            0x95, 0x97, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9F,
        ];
        let table_3_4_and_3_5 = [0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99];
        let table_3_6_and_3_7 = [0x00, 0x92];
        let table_3_8 = [
            0x00, 0x01, 0x02, 0x80, 0x83, 0x87, 0x8F, 0x91, 0x97, 0x9E, 0xA1, 0xA2,
        ];
        let table_3_9 = [0x00, 0x11, 0x80, 0x83, 0x87, 0x8F, 0x91];
        let table_3_10 = [
            0x00, 0x04, 0x80, 0x81, 0x82, 0x83, 0x87, 0x89, 0x8B, 0x8D, 0x8E, 0x8F, 0x90, 0x93,
            0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1,
            0xA2,
        ];
        let table_3_11 = [0x00, 0x18, 0x19];

        assert_eq!(values(PacketType::ConnAck), table_3_1);
        assert_eq!(values(PacketType::PubAck), table_3_4_and_3_5);
        assert_eq!(values(PacketType::PubRec), table_3_4_and_3_5);
        assert_eq!(values(PacketType::PubRel), table_3_6_and_3_7);
        assert_eq!(values(PacketType::PubComp), table_3_6_and_3_7);
        assert_eq!(values(PacketType::SubAck), table_3_8);
        assert_eq!(values(PacketType::UnsubAck), table_3_9);
        assert_eq!(values(PacketType::Auth), table_3_11);

        // Table 3-10 omits 0x8C, which Table 2-6 lists for DISCONNECT; the codec accepts it.
        let mut disconnect = table_3_10.to_vec();
        disconnect.push(0x8C);
        disconnect.sort_unstable();
        assert_eq!(values(PacketType::Disconnect), disconnect);
    }

    #[test]
    fn per_packet_codes_convert_to_the_common_set() {
        for &code in DisconnectReasonCode::ALL {
            assert_eq!(ReasonCode::from(code).value(), code.value());
        }
        for &code in ConnectReasonCode::ALL {
            assert_eq!(ReasonCode::from(code).value(), code.value());
        }
        for &code in SubAckReasonCode::ALL {
            assert_eq!(ReasonCode::from(code).value(), code.value());
        }
        for &code in UnsubAckReasonCode::ALL {
            assert_eq!(ReasonCode::from(code).value(), code.value());
        }
        assert_eq!(
            ReasonCode::from(PubAckReasonCode::NoMatchingSubscribers),
            ReasonCode::NoMatchingSubscribers
        );
        assert_eq!(
            ReasonCode::from(PubRecReasonCode::PayloadFormatInvalid),
            ReasonCode::PayloadFormatInvalid
        );
        assert_eq!(
            ReasonCode::from(PubRelReasonCode::PacketIdentifierNotFound),
            ReasonCode::PacketIdentifierNotFound
        );
        assert_eq!(
            ReasonCode::from(PubCompReasonCode::Success),
            ReasonCode::Success
        );
        assert_eq!(
            ReasonCode::from(AuthReasonCode::ReAuthenticate),
            ReasonCode::ReAuthenticate
        );
    }

    #[test]
    fn table_3_10_sent_by() {
        let client_only = [0x04];
        let server_only = [
            0x87, 0x89, 0x8B, 0x8D, 0x8E, 0x8F, 0x9A, 0x9B, 0x9C, 0x9D, 0x9E, 0x9F, 0xA0, 0xA1,
            0xA2,
        ];
        for &code in DisconnectReasonCode::ALL {
            let value = code.value();
            assert_eq!(
                code.is_sent_by(Sender::Client),
                !server_only.contains(&value),
                "{code:?}"
            );
            assert_eq!(
                code.is_sent_by(Sender::Server),
                !client_only.contains(&value),
                "{code:?}"
            );
        }
    }

    #[test]
    fn tables_3_4_3_5_and_3_11_sent_by() {
        assert!(!PubAckReasonCode::NoMatchingSubscribers.is_sent_by(Sender::Client));
        assert!(PubAckReasonCode::NoMatchingSubscribers.is_sent_by(Sender::Server));
        assert!(!PubRecReasonCode::NoMatchingSubscribers.is_sent_by(Sender::Client));
        assert!(PubRecReasonCode::NotAuthorized.is_sent_by(Sender::Client));
        assert!(!AuthReasonCode::Success.is_sent_by(Sender::Client));
        assert!(AuthReasonCode::Success.is_sent_by(Sender::Server));
        assert!(AuthReasonCode::ContinueAuthentication.is_sent_by(Sender::Client));
        assert!(AuthReasonCode::ContinueAuthentication.is_sent_by(Sender::Server));
        assert!(AuthReasonCode::ReAuthenticate.is_sent_by(Sender::Client));
        assert!(!AuthReasonCode::ReAuthenticate.is_sent_by(Sender::Server));
    }
}
