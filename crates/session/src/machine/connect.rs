//! From the first packet to CONNACK: CONNECT, authentication, the Will Message, the claim and
//! the resumed session; and re-authentication once connected.

use std::num::{NonZeroU16, NonZeroU32};

use openqtt_codec::{
    Auth, AuthProperties, AuthReasonCode, ConnAck, ConnAckProperties, Connect, ConnectReasonCode,
    DisconnectReasonCode, MAX_PACKET_SIZE, Packet, PacketType, ProtocolRefusal, Will,
};
use openqtt_core::{ClientId, Deadline, Message, Timestamp, TopicName};
use openqtt_topic::{Error as TopicError, Mount, Placeholder};

use super::{
    ClaimState, Client, Ending, Inbound, InboundState, Limits, MAX_DRAWS, Outbound, Phase, Queued,
    Reauth, Received, Session, Stage, Subscription, WillDraft, draw,
};
use crate::convert::{core_format, core_qos};
use crate::state::StoredOutbound;
use crate::{
    Action, AuthResult, AuthStep, Authentication, Authorization, Claim, ClaimResult, CloseCode,
    Decision, Effect, Effects, Identity, RequestId, SessionState, StreamId, Timer, WillMessage,
};

impl Session {
    /// The first thing the client sent, which must be a CONNECT ([MQTT-3.1.0-1]).
    pub(super) fn first(&mut self, received: Received, now: Timestamp, fx: &mut Effects) {
        match received {
            Received::Packet {
                stream: StreamId::Control,
                packet: Packet::Connect(connect),
                early,
                ..
            } => self.connect(*connect, early, now, fx),
            // [MQTT-3.1.0-1]: anything else is closed on without a reply.
            Received::Packet { .. } => {
                self.finish(Ending::NotAccepted, CloseCode::ProtocolError, fx)
            }
            Received::Error {
                error, packet_type, ..
            } => self.unreadable_connect(&error, packet_type, now, fx),
        }
    }

    /// A first packet that did not decode.
    fn unreadable_connect(
        &mut self,
        error: &openqtt_codec::Error,
        packet_type: Option<PacketType>,
        now: Timestamp,
        fx: &mut Effects,
    ) {
        if let openqtt_codec::Error::UnsupportedProtocol { name, level } = error {
            // Report R1, D1: the bytes the client's own version reads, or none for another
            // protocol ([MQTT-3.1.2-1], [MQTT-3.1.2-2]).
            let refusal = ProtocolRefusal::for_connect(name, *level);
            let code = if refusal == ProtocolRefusal::Close {
                CloseCode::ProtocolError
            } else {
                fx.push(Effect::SendRefusal(refusal));
                CloseCode::NoError
            };
            return self.finish(Ending::NotAccepted, code, fx);
        }
        let connect = packet_type == Some(PacketType::Connect)
            || matches!(error, openqtt_codec::Error::InvalidConnectFlags { .. });
        if connect {
            // [MQTT-3.1.4-1], report R1 D4: 0x81 for a malformed CONNECT, 0x82 for a protocol
            // error, 0x95 for one too large.
            self.connect_read = true;
            self.refuse(error.connack_reason_code(), now, fx);
        } else {
            // [MQTT-3.1.0-1]
            self.finish(Ending::NotAccepted, CloseCode::ProtocolError, fx);
        }
    }

    /// The CONNECT: settles the connection and asks for authentication.
    fn connect(&mut self, connect: Connect, early: bool, now: Timestamp, fx: &mut Effects) {
        self.connect_read = true;
        self.connect_early = early;
        let properties = &connect.properties;
        self.limits = Limits {
            maximum_packet_size: properties
                .maximum_packet_size
                .map_or(MAX_PACKET_SIZE, NonZeroU32::get),
            problem_information: properties.request_problem_information.unwrap_or(true),
        };
        let (id, assigned, announce_id, username, password) = match self.config.identity {
            Identity::Certificate => {
                // Report R2 rule 4, R1 D20: the CN names the session whatever the CONNECT says.
                let Some(cn) = self.certificate_cn.clone() else {
                    return self.refuse(ConnectReasonCode::NotAuthorized, now, fx);
                };
                let Ok(id) = ClientId::new(&cn) else {
                    return self.refuse(ConnectReasonCode::ClientIdentifierNotValid, now, fx);
                };
                (id, false, true, Some(cn), None)
            }
            // [MQTT-3.1.3-6], report R1 O9 and O22: assigned with either Clean Start.
            Identity::Credentials if connect.client_id.is_empty() => (
                self.draw_client_id(),
                true,
                true,
                connect.username.clone(),
                connect.password.clone(),
            ),
            // [MQTT-3.1.3-5], report R1 O10: up to 256 bytes of UTF-8 without U+0000.
            Identity::Credentials => match ClientId::new(&connect.client_id) {
                Ok(id) => (
                    id,
                    false,
                    false,
                    connect.username.clone(),
                    connect.password.clone(),
                ),
                // [MQTT-3.1.3-8]
                Err(_) => return self.refuse(ConnectReasonCode::ClientIdentifierNotValid, now, fx),
            },
        };
        let will = match connect.will.clone().map(|will| self.will_draft(will)) {
            Some(Ok(draft)) => Some(draft),
            Some(Err(code)) => return self.refuse(code, now, fx),
            None => None,
        };
        let keep_alive = self.config.keep_alive.apply(connect.keep_alive);
        let requested_expiry = properties.session_expiry_interval;
        let client = Client {
            id: id.clone(),
            assigned,
            announce_id,
            username: username.clone(),
            mount: None,
            clean_start: connect.clean_start,
            session_present: false,
            keep_alive,
            // [MQTT-3.2.2-22]: only when the client's value is not the one in use (R1, O4).
            server_keep_alive: (keep_alive != connect.keep_alive).then_some(keep_alive),
            requested_expiry,
            expiry: requested_expiry
                .unwrap_or(0)
                .min(self.config.session_expiry_maximum),
            receive_maximum: properties.receive_maximum.map_or(u16::MAX, NonZeroU16::get),
            topic_alias_maximum: properties.topic_alias_maximum.unwrap_or(0),
            method: properties.authentication_method.clone(),
            auth_data: None,
            will,
            will_message: None,
        };
        let request = Authentication {
            step: AuthStep::Connect,
            client_id: id,
            username,
            password,
            certificate_cn: self.certificate_cn.clone(),
            method: properties.authentication_method.clone(),
            data: properties.authentication_data.clone(),
        };
        self.client = Some(client);
        self.phase = Phase::Authenticating;
        fx.push(Effect::Authenticate(request));
    }

    /// Checks a Will Message, the CONNACK code refusing it if it cannot be used.
    fn will_draft(&self, will: Will) -> Result<WillDraft, ConnectReasonCode> {
        if will.qos > self.config.maximum_qos {
            // [MQTT-3.2.2-12]
            return Err(ConnectReasonCode::QosNotSupported);
        }
        if will.retain && !self.config.retain_available {
            // [MQTT-3.2.2-13]
            return Err(ConnectReasonCode::RetainNotSupported);
        }
        // A Will Topic that breaks section 4.7 is not malformed, but not accepted (report R1,
        // O25), and neither is one over the level limit (O16).
        let topic = TopicName::new(&will.topic).map_err(|_| ConnectReasonCode::TopicNameInvalid)?;
        if topic.level_count() > self.config.maximum_topic_levels {
            return Err(ConnectReasonCode::TopicNameInvalid);
        }
        let response_topic = match &will.properties.response_topic {
            Some(text) => {
                Some(TopicName::new(text).map_err(|_| ConnectReasonCode::TopicNameInvalid)?)
            }
            None => None,
        };
        Ok(WillDraft {
            topic,
            response_topic,
            will,
        })
    }

    /// The identifier for a client that sent an empty one (report R1, O9).
    fn draw_client_id(&mut self) -> ClientId {
        let id = ClientId::assigned(draw(self.random, self.draws));
        self.draws += 1;
        id
    }

    /// The authenticator answered.
    pub(super) fn authenticated(&mut self, result: AuthResult, now: Timestamp, fx: &mut Effects) {
        match self.phase {
            Phase::Authenticating => self.connect_authenticated(result, now, fx),
            Phase::Connected if self.reauth == Reauth::Authenticating => {
                self.reauthenticated(result, now, fx);
            }
            _ => {}
        }
    }

    /// The authenticator answered for the CONNECT, or for a step of enhanced authentication.
    fn connect_authenticated(&mut self, result: AuthResult, now: Timestamp, fx: &mut Effects) {
        match result {
            // [MQTT-3.1.4-2], [MQTT-4.12.0-4]
            AuthResult::Failure(code) => self.refuse(code, now, fx),
            AuthResult::Continue { data } => {
                let Some(method) = self
                    .client
                    .as_ref()
                    .and_then(|client| client.method.clone())
                else {
                    // Without an Authentication Method there is no AUTH to send
                    // ([MQTT-4.12.0-6]).
                    return self.refuse(ConnectReasonCode::NotAuthorized, now, fx);
                };
                self.phase = Phase::Challenged;
                // [MQTT-4.12.0-2], [MQTT-4.12.0-5]
                let auth = Auth {
                    reason_code: AuthReasonCode::ContinueAuthentication,
                    properties: AuthProperties {
                        authentication_method: Some(method),
                        authentication_data: data,
                        ..AuthProperties::default()
                    },
                };
                self.send(StreamId::Control, auth, now, fx);
            }
            AuthResult::Success { data } => {
                if let Some(client) = self.client.as_mut() {
                    client.auth_data = data;
                }
                self.admit(now, fx);
            }
        }
    }

    /// What the client sent while enhanced authentication waits for its next AUTH. Only AUTH
    /// and DISCONNECT may come ([MQTT-3.1.2-30]).
    pub(super) fn challenged(&mut self, received: Received, now: Timestamp, fx: &mut Effects) {
        let packet = match received {
            Received::Error { error, .. } => {
                return self.refuse(error.connack_reason_code(), now, fx);
            }
            Received::Packet {
                stream: StreamId::Control,
                packet,
                ..
            } => packet,
            // Not on a data stream, which waits for CONNACK anyway.
            Received::Packet { .. } => {
                return self.refuse(ConnectReasonCode::ProtocolError, now, fx);
            }
        };
        match packet {
            Packet::Auth(auth) => {
                let Some(client) = self.client.as_ref() else {
                    return;
                };
                // [MQTT-4.12.0-3], [MQTT-4.12.0-5]
                if auth.reason_code != AuthReasonCode::ContinueAuthentication
                    || auth.properties.authentication_method != client.method
                {
                    return self.refuse(ConnectReasonCode::ProtocolError, now, fx);
                }
                let request = Authentication {
                    step: AuthStep::Continue,
                    client_id: client.id.clone(),
                    username: client.username.clone(),
                    password: None,
                    certificate_cn: self.certificate_cn.clone(),
                    method: client.method.clone(),
                    data: auth.properties.authentication_data,
                };
                self.phase = Phase::Authenticating;
                fx.push(Effect::Authenticate(request));
            }
            // The client gives up before CONNACK.
            Packet::Disconnect(_) => self.finish(Ending::NotAccepted, CloseCode::NoError, fx),
            // [MQTT-3.1.2-30], report R1 D4: a CONNACK, never a DISCONNECT, before acceptance.
            _ => self.refuse(ConnectReasonCode::ProtocolError, now, fx),
        }
    }

    /// The client is authenticated: resolve its mountpoint, then have its will authorized, then
    /// claim.
    fn admit(&mut self, now: Timestamp, fx: &mut Effects) {
        let mount = match self.resolve_mount() {
            Ok(mount) => mount,
            Err(code) => return self.refuse(code, now, fx),
        };
        let Some(client) = self.client.as_mut() else {
            return;
        };
        client.mount = mount;
        let Some(draft) = &client.will else {
            return self.claim(now, fx);
        };
        // The Will Message is published for the client, perhaps long after the connection
        // ends, so it is authorized now, against its own topic (report R2, rule 7).
        let action = Action::Publish {
            topic: draft.topic.clone(),
            qos: core_qos(draft.will.qos),
            retain: draft.will.retain,
        };
        let request = self.request();
        self.phase = Phase::AuthorizingWill(request);
        fx.push(Effect::Authorize(Authorization {
            request: RequestId(request),
            actions: vec![action],
        }));
    }

    /// The mount for the connection (report R2, rule 6), or the CONNACK code refusing a
    /// connection that cannot have one.
    fn resolve_mount(&self) -> Result<Option<Mount>, ConnectReasonCode> {
        let (Some(mountpoint), Some(client)) = (&self.config.mountpoint, &self.client) else {
            return Ok(None);
        };
        mountpoint
            .resolve(client.username.as_deref(), client.id.as_str())
            .map(Some)
            .map_err(|error| match error {
                TopicError::MissingPlaceholderValue {
                    placeholder: Placeholder::Username,
                }
                | TopicError::InvalidPlaceholderValue {
                    placeholder: Placeholder::Username,
                } => ConnectReasonCode::BadUserNameOrPassword,
                TopicError::InvalidPlaceholderValue {
                    placeholder: Placeholder::ClientId,
                } => ConnectReasonCode::ClientIdentifierNotValid,
                _ => ConnectReasonCode::NotAuthorized,
            })
    }

    /// The authorizer answered.
    pub(super) fn authorized(
        &mut self,
        request: u64,
        decisions: &[Decision],
        now: Timestamp,
        fx: &mut Effects,
    ) {
        let allowed = decisions.first() == Some(&Decision::Allow);
        match self.phase {
            Phase::AuthorizingWill(expected) if expected == request => {
                if allowed {
                    self.claim(now, fx);
                } else {
                    self.refuse(ConnectReasonCode::NotAuthorized, now, fx);
                }
            }
            Phase::Connected => self.authorized_packet(request, decisions, now, fx),
            _ => {}
        }
    }

    /// Claims the Client Identifier, with the will mounted for the session.
    fn claim(&mut self, now: Timestamp, fx: &mut Effects) {
        let Some(client) = self.client.as_ref() else {
            return;
        };
        let mounted = match &client.will {
            Some(draft) => will_message(draft, client.mount.as_ref(), &client.id).map(Some),
            None => Ok(None),
        };
        let will_message = match mounted {
            Ok(will_message) => will_message,
            Err(code) => return self.refuse(code, now, fx),
        };
        let Some(client) = self.client.as_mut() else {
            return;
        };
        client.will_message = will_message;
        let claim = Claim {
            client_id: client.id.clone(),
            assigned: client.assigned,
            clean_start: client.clean_start,
            session_expiry: client.expiry,
            will: client.will_message.clone(),
        };
        self.phase = Phase::Claiming;
        self.claim = ClaimState::Outstanding;
        fx.push(Effect::Claim(claim));
    }

    /// The log answered the claim.
    pub(super) fn claimed(&mut self, result: ClaimResult, now: Timestamp, fx: &mut Effects) {
        if self.claim != ClaimState::Outstanding {
            return;
        }
        match result {
            ClaimResult::Claimed { session } => {
                self.claim = ClaimState::Held;
                let clean_start = self.client.as_ref().is_none_or(|client| client.clean_start);
                // [MQTT-3.2.2-2]: Clean Start 1 never resumes.
                let present = match session {
                    Some(state) if !clean_start => self.restore(state),
                    _ => false,
                };
                if let Some(client) = self.client.as_mut() {
                    client.session_present = present;
                }
                match self.phase {
                    Phase::Claiming if self.handshake_complete => self.accept(now, fx),
                    Phase::Claiming => self.phase = Phase::Accepted,
                    // The connection ended while the claim was outstanding.
                    Phase::Closed => {
                        let ending = self.late_ending.take().unwrap_or(Ending::NotAccepted);
                        self.release(ending, fx);
                    }
                    _ => {}
                }
            }
            ClaimResult::Taken => {
                self.claim = ClaimState::None;
                if self.phase != Phase::Claiming {
                    return;
                }
                let assigned = self.client.as_ref().is_some_and(|client| client.assigned);
                if !assigned {
                    // Only an identifier the server assigned is refused for being in use; any
                    // other takes the session over.
                    return self.refuse(ConnectReasonCode::ClientIdentifierNotValid, now, fx);
                }
                if self.draws >= MAX_DRAWS {
                    return self.refuse(ConnectReasonCode::ServerUnavailable, now, fx);
                }
                // Report R1, O9: draw another and claim again.
                let id = self.draw_client_id();
                if let Some(client) = self.client.as_mut() {
                    client.id = id;
                }
                match self.resolve_mount() {
                    Ok(mount) => {
                        if let Some(client) = self.client.as_mut() {
                            client.mount = mount;
                        }
                        self.claim(now, fx);
                    }
                    Err(code) => self.refuse(code, now, fx),
                }
            }
            ClaimResult::Refused(code) => {
                self.claim = ClaimState::None;
                if self.phase == Phase::Claiming {
                    self.refuse(code, now, fx);
                }
            }
        }
    }

    /// Takes in the session a claim resumed, and says whether it is resumed: a state for
    /// another identifier or another namespace is not.
    fn restore(&mut self, state: SessionState) -> bool {
        let Some(client) = &self.client else {
            return false;
        };
        let mount = client.mount.as_ref().map(Mount::as_str);
        if state.client_id != client.id || state.mount.as_deref() != mount {
            return false;
        }
        for stored in state.subscriptions {
            let order = self.next_subscription;
            self.next_subscription += 1;
            // On a new connection every subscription delivers on the control stream
            // (docs/spec/mqtt-over-quic.md, section 2.4).
            self.subscriptions.insert(
                stored.mounted,
                Subscription {
                    filter: stored.filter,
                    options: stored.options,
                    stream: StreamId::Control,
                    order,
                    retained_reads: 0,
                },
            );
        }
        for stored in state.outbound {
            let Some(packet_id) = stored.packet_id() else {
                continue;
            };
            if !self.ids.insert(packet_id.get()) {
                continue;
            }
            let (stage, publish) = match stored {
                StoredOutbound::Publish(publish)
                    if publish.qos == openqtt_codec::QoS::ExactlyOnce =>
                {
                    (Stage::Receipt, Some(*publish))
                }
                StoredOutbound::Publish(publish) => (Stage::Acknowledgement, Some(*publish)),
                StoredOutbound::Release(_) => (Stage::Completion, None),
            };
            self.outbound.push_back(Outbound {
                packet_id,
                stage,
                publish,
                stream: StreamId::Control,
                counted: false,
                sent: false,
            });
        }
        for packet_id in state.awaiting_release {
            self.inbound.insert(
                packet_id.get(),
                Inbound {
                    state: InboundState::AwaitingRelease,
                    stream: StreamId::Control,
                    // A new connection starts with a full quota (section 4.9).
                    counted: false,
                },
            );
        }
        for stored in state.queue {
            self.queue.push_back(Queued {
                message: stored.message,
                topic: stored.topic,
                qos: stored.qos,
                retain: stored.retain,
                subscription_ids: stored.subscription_ids,
                stream: StreamId::Control,
            });
        }
        self.next_id = state.next_packet_id.max(1);
        true
    }

    /// Sends CONNACK 0x00 ([MQTT-3.1.4-5], [MQTT-3.2.0-1]) and opens the connection.
    pub(super) fn accept(&mut self, now: Timestamp, fx: &mut Effects) {
        let Some(client) = &self.client else {
            return;
        };
        let config = &self.config;
        let used_expiry = client.expiry;
        let properties = ConnAckProperties {
            // Report R1, O7: whenever the value used differs from the one asked for.
            session_expiry_interval: (client.requested_expiry.unwrap_or(0) != used_expiry)
                .then_some(used_expiry),
            receive_maximum: Some(config.receive_maximum),
            // Report R1, O19: only what differs from the absent value.
            maximum_qos: (config.maximum_qos != openqtt_codec::QoS::ExactlyOnce)
                .then_some(config.maximum_qos),
            retain_available: (!config.retain_available).then_some(false),
            maximum_packet_size: Some(config.maximum_packet_size),
            // [MQTT-3.1.3-7], [MQTT-3.2.2-16], report R1 D20.
            assigned_client_identifier: client.announce_id.then(|| client.id.as_str().to_owned()),
            topic_alias_maximum: (config.topic_alias_maximum > 0)
                .then_some(config.topic_alias_maximum),
            wildcard_subscription_available: (!config.wildcard_subscription_available)
                .then_some(false),
            subscription_identifier_available: (!config.subscription_identifiers_available)
                .then_some(false),
            shared_subscription_available: (!config.shared_subscription_available).then_some(false),
            // [MQTT-3.2.2-22]
            server_keep_alive: client.server_keep_alive,
            // [MQTT-4.12.0-5], [MQTT-4.12.0-6]
            authentication_method: client.method.clone(),
            authentication_data: client
                .method
                .as_ref()
                .and_then(|_| client.auth_data.clone()),
            // Report R1, O18: never Response Information ([MQTT-3.1.2-28]).
            ..ConnAckProperties::default()
        };
        let connack = ConnAck {
            // [MQTT-3.2.2-2], [MQTT-3.2.2-3]
            session_present: client.session_present,
            reason_code: ConnectReasonCode::Success,
            properties,
        };
        // Only the Reason String and User Properties may be left out to fit ([MQTT-3.2.2-19],
        // [MQTT-3.2.2-20]), and this CONNACK has neither. A client whose Maximum Packet Size
        // cannot hold the limits it must be told, or the identifier it was assigned, cannot be
        // served: it is refused with a CONNACK it can read.
        let mut measured = Packet::from(connack.clone());
        if measured
            .fit_within(self.limits.maximum_packet_size)
            .is_err()
        {
            return self.refuse(ConnectReasonCode::PacketTooLarge, now, fx);
        }
        self.phase = Phase::Connected;
        self.send(StreamId::Control, connack, now, fx);
        if self.connect_timer {
            self.connect_timer = false;
            fx.push(Effect::CancelTimer(Timer::Connect));
        }
        // Keep Alive runs from CONNACK: the client could send nothing it was waiting for
        // before it.
        self.last_activity = self.last_activity.max(now);
        self.keep_alive_timer = true;
        fx.push(Effect::SetTimer {
            timer: Timer::KeepAlive,
            at: self.keep_alive_deadline(),
        });
        // A resumed session sends again what it had in flight, then what waited ([MQTT-4.4.0-1]).
        self.pump(now, fx);
    }

    /// The handshake of a 0-RTT connection completed (docs/spec/mqtt-over-quic.md, section 4).
    pub(super) fn handshake_completed(&mut self, accepted: bool, now: Timestamp, fx: &mut Effects) {
        if self.handshake_complete {
            return;
        }
        self.handshake_complete = true;
        if !accepted {
            // What came in rejected early data is not acted on; the client sends it again.
            self.inbox.retain(|received| !received.is_early());
            if self.connect_early && self.phase != Phase::Closed {
                return self.finish(Ending::NotAccepted, CloseCode::ProtocolError, fx);
            }
        }
        if self.phase == Phase::Accepted {
            self.accept(now, fx);
        }
    }

    /// The client's AUTH once connected: a re-authentication (section 4.12.1).
    pub(super) fn reauthenticate(&mut self, auth: &Auth, now: Timestamp, fx: &mut Effects) {
        let Some(client) = &self.client else {
            return;
        };
        let Some(method) = client.method.clone() else {
            // [MQTT-4.12.0-7]: no AUTH without an Authentication Method in CONNECT.
            return self.protocol_error(now, fx);
        };
        if auth.properties.authentication_method.as_deref() != Some(method.as_str()) {
            // [MQTT-4.12.1-1]: the method the connection was authenticated with.
            return self.close_with(DisconnectReasonCode::BadAuthenticationMethod, None, now, fx);
        }
        let step = match (self.reauth, auth.reason_code) {
            (Reauth::Idle, AuthReasonCode::ReAuthenticate) => AuthStep::Reauthenticate,
            (Reauth::Challenged, AuthReasonCode::ContinueAuthentication) => AuthStep::Continue,
            _ => return self.protocol_error(now, fx),
        };
        let request = Authentication {
            step,
            client_id: client.id.clone(),
            username: client.username.clone(),
            password: None,
            certificate_cn: self.certificate_cn.clone(),
            method: Some(method),
            data: auth.properties.authentication_data.clone(),
        };
        // Other packets keep flowing meanwhile, under the authentication already done.
        self.reauth = Reauth::Authenticating;
        fx.push(Effect::Authenticate(request));
    }

    /// The authenticator answered a step of re-authentication.
    fn reauthenticated(&mut self, result: AuthResult, now: Timestamp, fx: &mut Effects) {
        let method = self
            .client
            .as_ref()
            .and_then(|client| client.method.clone());
        let (reason_code, data) = match result {
            AuthResult::Success { data } => {
                self.reauth = Reauth::Idle;
                (AuthReasonCode::Success, data)
            }
            AuthResult::Continue { data } => {
                self.reauth = Reauth::Challenged;
                (AuthReasonCode::ContinueAuthentication, data)
            }
            AuthResult::Failure(code) => {
                // [MQTT-4.12.1-2]: a DISCONNECT with the code where DISCONNECT has it, then
                // close.
                let code = DisconnectReasonCode::from_u8(code.value())
                    .filter(|code| code.is_error())
                    .unwrap_or(DisconnectReasonCode::NotAuthorized);
                return self.close_with(code, None, now, fx);
            }
        };
        let auth = Auth {
            reason_code,
            properties: AuthProperties {
                authentication_method: method,
                authentication_data: data,
                ..AuthProperties::default()
            },
        };
        self.send(StreamId::Control, auth, now, fx);
    }
}

/// The Will Message as the claim stores it: its topic mounted ([MQTT-3.1.2-14],
/// [MQTT-3.1.2-15] for its RETAIN flag).
fn will_message(
    draft: &WillDraft,
    mount: Option<&Mount>,
    client_id: &ClientId,
) -> Result<WillMessage, ConnectReasonCode> {
    let topic = match mount {
        Some(mount) => mount
            .mount_name(&draft.topic)
            .map_err(|_| ConnectReasonCode::TopicNameInvalid)?,
        None => draft.topic.clone(),
    };
    let will = &draft.will;
    let properties = &will.properties;
    let mut message = Message::new(topic, will.payload.clone());
    message.qos = core_qos(will.qos);
    message.retain = will.retain;
    message.publisher = Some(client_id.clone());
    message.payload_format = properties.payload_format_indicator.map(core_format);
    message.content_type.clone_from(&properties.content_type);
    message.response_topic.clone_from(&draft.response_topic);
    message
        .correlation_data
        .clone_from(&properties.correlation_data);
    // [MQTT-3.1.3-10]: in the order received.
    message
        .user_properties
        .clone_from(&properties.user_properties);
    Ok(WillMessage {
        message,
        expiry_interval: properties.message_expiry_interval,
        delay: properties.will_delay_interval.unwrap_or(0),
    })
}

/// The deadline of a message received at `now` with this Message Expiry Interval.
pub(super) fn deadline(now: Timestamp, interval: Option<u32>) -> Option<Deadline> {
    interval.map(|interval| Deadline::after(now, interval))
}
