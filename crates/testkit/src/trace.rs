//! Traces: what each client of a scenario sent and received, as JSON, raw with timestamps or
//! normalized so that two runs, or two brokers, can be compared line by line.
//!
//! Normalizing replaces what a run or a broker may choose freely and what carries no meaning
//! a test compares:
//!
//! - the run's namespace, wherever it appears in a string, by `{ns}`;
//! - Assigned Client Identifiers by `<assigned-1>`, `<assigned-2>`, in order of appearance;
//! - Packet Identifiers by the exchange they belong to: `c1`, `c2` for exchanges the client
//!   started, `s1`, `s2` for those the server started, numbered in order;
//! - Topic Aliases the server chose by `a1`, `a2`, in order of first use;
//! - Reason Strings the server sent by `<reason string>`, keeping only that one was sent;
//! - User Properties the server put on its own packets by `<server user properties>`; those
//!   on a PUBLISH came from its publisher and are kept;
//! - time, which a normalized trace leaves out;
//! - how a connection ended on the wire. A broker may finish the control stream, reset it or
//!   close the QUIC connection, in one instant or seconds apart, so which a client sees first
//!   is a race of the transport. A normalized trace keeps one record per connection, `ended`,
//!   saying who ended it first: the server, by any of the three, or the client, by closing
//!   the connection. The raw trace keeps every one, with its codes.
//!
//! Both kinds of trace write binary data longer than [`SHOWN_BYTES`] as its length and a
//! hash, so a large payload neither hides the rest of a trace nor goes unnoticed if it
//! changes.

use std::collections::{BTreeMap, HashMap};

use openqtt_codec::{
    AckProperties, Auth, ConnAck, Connect, Disconnect, Packet, PacketId, Publish,
    PublishProperties, Subscribe, Unsubscribe, Will,
};
use serde_json::{Map, Value, json};

use crate::raw::{Close, Record, Recorded};

/// What one client of a scenario did: its connections, in order, each with its records.
pub type ClientLog = Vec<Vec<Record>>;

/// How many bytes of binary data a trace writes out in full.
pub const SHOWN_BYTES: usize = 128;

/// Bytes as lowercase hexadecimal, or, past [`SHOWN_BYTES`], as their length and hash.
pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    if bytes.len() > SHOWN_BYTES {
        return abbreviated(bytes);
    }
    bytes.iter().fold(String::new(), |mut out, byte| {
        // Writing to a String cannot fail.
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// Binary data as a string when it is UTF-8, else as `hex:` and its hexadecimal; past
/// [`SHOWN_BYTES`], as its length and hash.
fn binary(bytes: &[u8]) -> Value {
    if bytes.len() > SHOWN_BYTES {
        return Value::from(abbreviated(bytes));
    }
    match std::str::from_utf8(bytes) {
        Ok(text) => Value::from(text),
        Err(_) => Value::from(format!("hex:{}", hex(bytes))),
    }
}

/// Long data as `<N bytes, fnv1a64 H>`: its 64-bit FNV-1a hash tells two payloads apart
/// without writing either out.
fn abbreviated(bytes: &[u8]) -> String {
    let hash = bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    format!("<{} bytes, fnv1a64 {hash:016x}>", bytes.len())
}

/// A reason code as `0x87 NotAuthorized`.
fn code(value: u8, name: impl std::fmt::Debug) -> Value {
    Value::from(format!("0x{value:02X} {name:?}"))
}

/// Inserts `value` under `key` when it is present.
fn put<T: Into<Value>>(map: &mut Map<String, Value>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        map.insert(key.to_owned(), value.into());
    }
}

fn user_properties(map: &mut Map<String, Value>, properties: &[(String, String)]) {
    if !properties.is_empty() {
        let pairs: Vec<Value> = properties
            .iter()
            .map(|(name, value)| json!([name, value]))
            .collect();
        map.insert("user_properties".into(), Value::Array(pairs));
    }
}

/// A packet as JSON: its type and every field and property it carries, absent properties
/// left out.
pub fn packet_json(packet: &Packet) -> Value {
    let mut map = Map::new();
    map.insert("type".into(), Value::from(packet.packet_type().name()));
    match packet {
        Packet::Connect(connect) => connect_json(&mut map, connect),
        Packet::ConnAck(connack) => connack_json(&mut map, connack),
        Packet::Publish(publish) => publish_json(&mut map, publish),
        Packet::PubAck(ack) => {
            ack_json(
                &mut map,
                ack.packet_id,
                code(ack.reason_code.value(), ack.reason_code),
                &ack.properties,
            );
        }
        Packet::PubRec(ack) => {
            ack_json(
                &mut map,
                ack.packet_id,
                code(ack.reason_code.value(), ack.reason_code),
                &ack.properties,
            );
        }
        Packet::PubRel(ack) => {
            ack_json(
                &mut map,
                ack.packet_id,
                code(ack.reason_code.value(), ack.reason_code),
                &ack.properties,
            );
        }
        Packet::PubComp(ack) => {
            ack_json(
                &mut map,
                ack.packet_id,
                code(ack.reason_code.value(), ack.reason_code),
                &ack.properties,
            );
        }
        Packet::Subscribe(subscribe) => subscribe_json(&mut map, subscribe),
        Packet::SubAck(suback) => {
            map.insert("packet_id".into(), suback.packet_id.get().into());
            let codes: Vec<Value> = suback
                .reason_codes
                .iter()
                .map(|reason| code(reason.value(), reason))
                .collect();
            map.insert("reason_codes".into(), Value::Array(codes));
            ack_properties(&mut map, &suback.properties);
        }
        Packet::Unsubscribe(unsubscribe) => unsubscribe_json(&mut map, unsubscribe),
        Packet::UnsubAck(unsuback) => {
            map.insert("packet_id".into(), unsuback.packet_id.get().into());
            let codes: Vec<Value> = unsuback
                .reason_codes
                .iter()
                .map(|reason| code(reason.value(), reason))
                .collect();
            map.insert("reason_codes".into(), Value::Array(codes));
            ack_properties(&mut map, &unsuback.properties);
        }
        Packet::PingReq | Packet::PingResp => {}
        Packet::Disconnect(disconnect) => disconnect_json(&mut map, disconnect),
        Packet::Auth(auth) => auth_json(&mut map, auth),
    }
    Value::Object(map)
}

fn connect_json(map: &mut Map<String, Value>, connect: &Connect) {
    map.insert("clean_start".into(), connect.clean_start.into());
    map.insert("keep_alive".into(), connect.keep_alive.into());
    map.insert("client_id".into(), connect.client_id.clone().into());
    let p = &connect.properties;
    let mut properties = Map::new();
    put(
        &mut properties,
        "session_expiry_interval",
        p.session_expiry_interval,
    );
    put(
        &mut properties,
        "receive_maximum",
        p.receive_maximum.map(|v| v.get()),
    );
    put(
        &mut properties,
        "maximum_packet_size",
        p.maximum_packet_size.map(|v| v.get()),
    );
    put(
        &mut properties,
        "topic_alias_maximum",
        p.topic_alias_maximum,
    );
    put(
        &mut properties,
        "request_response_information",
        p.request_response_information,
    );
    put(
        &mut properties,
        "request_problem_information",
        p.request_problem_information,
    );
    user_properties(&mut properties, &p.user_properties);
    put(
        &mut properties,
        "authentication_method",
        p.authentication_method.clone(),
    );
    put(
        &mut properties,
        "authentication_data",
        p.authentication_data.as_deref().map(binary),
    );
    if !properties.is_empty() {
        map.insert("properties".into(), Value::Object(properties));
    }
    if let Some(will) = &connect.will {
        map.insert("will".into(), will_json(will));
    }
    put(map, "username", connect.username.clone());
    put(map, "password", connect.password.as_deref().map(binary));
}

fn will_json(will: &Will) -> Value {
    let mut map = Map::new();
    map.insert("qos".into(), will.qos.value().into());
    map.insert("retain".into(), will.retain.into());
    map.insert("topic".into(), will.topic.clone().into());
    map.insert("payload".into(), binary(&will.payload));
    let p = &will.properties;
    let mut properties = Map::new();
    put(
        &mut properties,
        "will_delay_interval",
        p.will_delay_interval,
    );
    put(
        &mut properties,
        "payload_format_indicator",
        p.payload_format_indicator.map(|v| v.value()),
    );
    put(
        &mut properties,
        "message_expiry_interval",
        p.message_expiry_interval,
    );
    put(&mut properties, "content_type", p.content_type.clone());
    put(&mut properties, "response_topic", p.response_topic.clone());
    put(
        &mut properties,
        "correlation_data",
        p.correlation_data.as_deref().map(binary),
    );
    user_properties(&mut properties, &p.user_properties);
    if !properties.is_empty() {
        map.insert("properties".into(), Value::Object(properties));
    }
    Value::Object(map)
}

fn connack_json(map: &mut Map<String, Value>, connack: &ConnAck) {
    map.insert("session_present".into(), connack.session_present.into());
    map.insert(
        "reason_code".into(),
        code(connack.reason_code.value(), connack.reason_code),
    );
    let p = &connack.properties;
    let mut properties = Map::new();
    put(
        &mut properties,
        "session_expiry_interval",
        p.session_expiry_interval,
    );
    put(
        &mut properties,
        "receive_maximum",
        p.receive_maximum.map(|v| v.get()),
    );
    put(
        &mut properties,
        "maximum_qos",
        p.maximum_qos.map(|v| v.value()),
    );
    put(&mut properties, "retain_available", p.retain_available);
    put(
        &mut properties,
        "maximum_packet_size",
        p.maximum_packet_size.map(|v| v.get()),
    );
    put(
        &mut properties,
        "assigned_client_identifier",
        p.assigned_client_identifier.clone(),
    );
    put(
        &mut properties,
        "topic_alias_maximum",
        p.topic_alias_maximum,
    );
    put(&mut properties, "reason_string", p.reason_string.clone());
    user_properties(&mut properties, &p.user_properties);
    put(
        &mut properties,
        "wildcard_subscription_available",
        p.wildcard_subscription_available,
    );
    put(
        &mut properties,
        "subscription_identifier_available",
        p.subscription_identifier_available,
    );
    put(
        &mut properties,
        "shared_subscription_available",
        p.shared_subscription_available,
    );
    put(&mut properties, "server_keep_alive", p.server_keep_alive);
    put(
        &mut properties,
        "response_information",
        p.response_information.clone(),
    );
    put(
        &mut properties,
        "server_reference",
        p.server_reference.clone(),
    );
    put(
        &mut properties,
        "authentication_method",
        p.authentication_method.clone(),
    );
    put(
        &mut properties,
        "authentication_data",
        p.authentication_data.as_deref().map(binary),
    );
    if !properties.is_empty() {
        map.insert("properties".into(), Value::Object(properties));
    }
}

fn publish_json(map: &mut Map<String, Value>, publish: &Publish) {
    map.insert("dup".into(), publish.dup.into());
    map.insert("qos".into(), publish.qos.value().into());
    map.insert("retain".into(), publish.retain.into());
    map.insert("topic".into(), publish.topic.clone().into());
    put(map, "packet_id", publish.packet_id.map(PacketId::get));
    let properties = publish_properties(&publish.properties);
    if !properties.is_empty() {
        map.insert("properties".into(), Value::Object(properties));
    }
    map.insert("payload".into(), binary(&publish.payload));
}

fn publish_properties(p: &PublishProperties) -> Map<String, Value> {
    let mut properties = Map::new();
    put(
        &mut properties,
        "payload_format_indicator",
        p.payload_format_indicator.map(|v| v.value()),
    );
    put(
        &mut properties,
        "message_expiry_interval",
        p.message_expiry_interval,
    );
    put(
        &mut properties,
        "topic_alias",
        p.topic_alias.map(|v| v.get()),
    );
    put(&mut properties, "response_topic", p.response_topic.clone());
    put(
        &mut properties,
        "correlation_data",
        p.correlation_data.as_deref().map(binary),
    );
    user_properties(&mut properties, &p.user_properties);
    if !p.subscription_identifiers.is_empty() {
        let ids: Vec<Value> = p
            .subscription_identifiers
            .iter()
            .map(|id| Value::from(id.get()))
            .collect();
        properties.insert("subscription_identifiers".into(), Value::Array(ids));
    }
    put(&mut properties, "content_type", p.content_type.clone());
    properties
}

fn ack_json(
    map: &mut Map<String, Value>,
    packet_id: PacketId,
    reason_code: Value,
    properties: &AckProperties,
) {
    map.insert("packet_id".into(), packet_id.get().into());
    map.insert("reason_code".into(), reason_code);
    ack_properties(map, properties);
}

fn ack_properties(map: &mut Map<String, Value>, p: &AckProperties) {
    let mut properties = Map::new();
    put(&mut properties, "reason_string", p.reason_string.clone());
    user_properties(&mut properties, &p.user_properties);
    if !properties.is_empty() {
        map.insert("properties".into(), Value::Object(properties));
    }
}

fn subscribe_json(map: &mut Map<String, Value>, subscribe: &Subscribe) {
    map.insert("packet_id".into(), subscribe.packet_id.get().into());
    let mut properties = Map::new();
    put(
        &mut properties,
        "subscription_identifier",
        subscribe
            .properties
            .subscription_identifier
            .map(|v| v.get()),
    );
    user_properties(&mut properties, &subscribe.properties.user_properties);
    if !properties.is_empty() {
        map.insert("properties".into(), Value::Object(properties));
    }
    let subscriptions: Vec<Value> = subscribe
        .subscriptions
        .iter()
        .map(|subscription| {
            let options = subscription.options;
            json!({
                "filter": subscription.filter,
                "maximum_qos": options.maximum_qos.value(),
                "no_local": options.no_local,
                "retain_as_published": options.retain_as_published,
                "retain_handling": options.retain_handling.value(),
            })
        })
        .collect();
    map.insert("subscriptions".into(), Value::Array(subscriptions));
}

fn unsubscribe_json(map: &mut Map<String, Value>, unsubscribe: &Unsubscribe) {
    map.insert("packet_id".into(), unsubscribe.packet_id.get().into());
    let mut properties = Map::new();
    user_properties(&mut properties, &unsubscribe.properties.user_properties);
    if !properties.is_empty() {
        map.insert("properties".into(), Value::Object(properties));
    }
    map.insert("filters".into(), json!(unsubscribe.filters));
}

fn disconnect_json(map: &mut Map<String, Value>, disconnect: &Disconnect) {
    map.insert(
        "reason_code".into(),
        code(disconnect.reason_code.value(), disconnect.reason_code),
    );
    let p = &disconnect.properties;
    let mut properties = Map::new();
    put(
        &mut properties,
        "session_expiry_interval",
        p.session_expiry_interval,
    );
    put(&mut properties, "reason_string", p.reason_string.clone());
    user_properties(&mut properties, &p.user_properties);
    put(
        &mut properties,
        "server_reference",
        p.server_reference.clone(),
    );
    if !properties.is_empty() {
        map.insert("properties".into(), Value::Object(properties));
    }
}

fn auth_json(map: &mut Map<String, Value>, auth: &Auth) {
    map.insert(
        "reason_code".into(),
        code(auth.reason_code.value(), auth.reason_code),
    );
    let p = &auth.properties;
    let mut properties = Map::new();
    put(
        &mut properties,
        "authentication_method",
        p.authentication_method.clone(),
    );
    put(
        &mut properties,
        "authentication_data",
        p.authentication_data.as_deref().map(binary),
    );
    put(&mut properties, "reason_string", p.reason_string.clone());
    user_properties(&mut properties, &p.user_properties);
    if !properties.is_empty() {
        map.insert("properties".into(), Value::Object(properties));
    }
}

/// How a connection closed, as JSON.
fn close_json(close: &Close) -> Value {
    match close {
        Close::Application { code, reason } => {
            json!({ "by": "peer", "application_code": code, "reason": binary(reason) })
        }
        Close::Transport { code, reason } => {
            json!({ "by": "peer", "transport_code": code, "reason": reason })
        }
        Close::TimedOut => json!({ "by": "idle timeout" }),
        Close::Reset => json!({ "by": "peer", "reset": true }),
        Close::Locally => json!({ "by": "this end" }),
        Close::Other(other) => json!({ "other": other }),
    }
}

/// One record as JSON, without normalizing.
fn record_json(record: &Recorded) -> Value {
    match record {
        Recorded::Sent(packet) => json!({ "sent": packet_json(packet) }),
        Recorded::SentBytes(bytes) => json!({ "sent_bytes": hex(bytes) }),
        Recorded::Received(packet) => json!({ "received": packet_json(packet) }),
        Recorded::Malformed { bytes, error } => {
            json!({ "received_malformed": { "bytes": hex(bytes), "error": error } })
        }
        Recorded::StreamFinished => json!({ "stream": "finished" }),
        Recorded::StreamReset(code) => json!({ "stream": { "reset": code } }),
        Recorded::Closed(close) => json!({ "closed": close_json(close) }),
    }
}

/// Every record of every client, with its time in milliseconds since its connection opened,
/// nothing normalized: for reading what happened.
pub fn raw_trace(scenario: &str, clients: &BTreeMap<String, ClientLog>) -> Value {
    let mut out = Map::new();
    for (name, connections) in clients {
        let mut entries = Vec::new();
        for (index, records) in connections.iter().enumerate() {
            entries.push(json!({ "connection": index + 1 }));
            for record in records {
                let mut entry = record_json(&record.event);
                if let Value::Object(map) = &mut entry {
                    let millis = u64::try_from(record.at.as_millis()).unwrap_or(u64::MAX);
                    map.insert("at_ms".into(), millis.into());
                }
                entries.push(entry);
            }
        }
        out.insert(name.clone(), Value::Array(entries));
    }
    json!({ "scenario": scenario, "clients": out })
}

/// Every record of every client, normalized as the module says, so two runs of a scenario
/// compare equal when the broker behaved the same.
pub fn normalized_trace(
    scenario: &str,
    namespace: &str,
    clients: &BTreeMap<String, ClientLog>,
) -> Value {
    let mut assigned = Numbering::new("<assigned-", ">");
    let mut out = Map::new();
    for (name, connections) in clients {
        let mut normalizer = Normalizer::default();
        let mut entries = Vec::new();
        for (index, records) in connections.iter().enumerate() {
            entries.push(json!({ "connection": index + 1 }));
            normalizer.new_connection();
            let mut ended = false;
            for record in records {
                if let Some(by) = ended_by(&record.event) {
                    if !ended {
                        entries.push(json!({ "ended": by }));
                        ended = true;
                    }
                    continue;
                }
                entries.push(normalizer.record(&record.event, &mut assigned));
            }
        }
        out.insert(name.clone(), Value::Array(entries));
    }
    let mut trace = json!({ "scenario": scenario, "clients": out });
    if !namespace.is_empty() {
        replace_strings(&mut trace, namespace, "{ns}");
    }
    trace
}

/// Who ended a connection, when this record ends it: the peer, by finishing or resetting the
/// control stream or closing the connection; this end, by closing it; or the idle timeout.
fn ended_by(record: &Recorded) -> Option<&'static str> {
    match record {
        Recorded::StreamFinished | Recorded::StreamReset(_) => Some("by the server"),
        Recorded::Closed(Close::Locally) => Some("by the client"),
        Recorded::Closed(Close::TimedOut) => Some("by the idle timeout"),
        Recorded::Closed(Close::Other(_)) => Some("by the transport"),
        Recorded::Closed(_) => Some("by the server"),
        _ => None,
    }
}

/// Replaces `from` by `to` in every string of a JSON value.
fn replace_strings(value: &mut Value, from: &str, to: &str) {
    match value {
        Value::String(text) if text.contains(from) => *text = text.replace(from, to),
        Value::Array(items) => {
            for item in items {
                replace_strings(item, from, to);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                replace_strings(item, from, to);
            }
        }
        _ => {}
    }
}

/// Numbers distinct values in order of first appearance.
#[derive(Debug)]
struct Numbering {
    prefix: &'static str,
    suffix: &'static str,
    seen: HashMap<String, usize>,
}

impl Numbering {
    fn new(prefix: &'static str, suffix: &'static str) -> Self {
        Self {
            prefix,
            suffix,
            seen: HashMap::new(),
        }
    }

    fn name(&mut self, value: &str) -> String {
        let next = self.seen.len() + 1;
        let number = *self.seen.entry(value.to_owned()).or_insert(next);
        format!("{}{number}{}", self.prefix, self.suffix)
    }
}

/// Packet Identifiers of one side's exchanges: each new exchange takes the next number, and
/// the packets that answer it take the same one.
#[derive(Debug)]
struct Exchanges {
    prefix: char,
    started: usize,
    live: HashMap<u16, usize>,
}

impl Exchanges {
    fn new(prefix: char) -> Self {
        Self {
            prefix,
            started: 0,
            live: HashMap::new(),
        }
    }

    /// An exchange starts, unless one with this identifier is still open, as for a resend.
    fn start(&mut self, id: PacketId) -> Value {
        let number = match self.live.get(&id.get()) {
            Some(number) => *number,
            None => {
                self.started += 1;
                self.live.insert(id.get(), self.started);
                self.started
            }
        };
        Value::from(format!("{}{number}", self.prefix))
    }

    /// A packet of an exchange; `end` closes it.
    fn answer(&mut self, id: PacketId, end: bool) -> Value {
        let value = match self.live.get(&id.get()) {
            Some(number) => format!("{}{number}", self.prefix),
            None => format!("{}?{}", self.prefix, id.get()),
        };
        if end {
            self.live.remove(&id.get());
        }
        Value::from(value)
    }
}

/// The normalizing state of one client.
#[derive(Debug)]
struct Normalizer {
    client: Exchanges,
    server: Exchanges,
    aliases: Numbering,
}

impl Default for Normalizer {
    fn default() -> Self {
        Self {
            client: Exchanges::new('c'),
            server: Exchanges::new('s'),
            aliases: Numbering::new("a", ""),
        }
    }
}

impl Normalizer {
    /// Topic Aliases do not carry over to another connection ([MQTT-3.3.2-7]).
    fn new_connection(&mut self) {
        self.aliases = Numbering::new("a", "");
    }

    fn record(&mut self, record: &Recorded, assigned: &mut Numbering) -> Value {
        match record {
            Recorded::Sent(packet) => {
                let mut json = packet_json(packet);
                if let Some(id) = self.sent_id(packet) {
                    set(&mut json, "packet_id", id);
                }
                json!({ "sent": json })
            }
            Recorded::Received(packet) => {
                let mut json = packet_json(packet);
                if let Some(id) = self.received_id(packet) {
                    set(&mut json, "packet_id", id);
                }
                self.received_properties(packet, &mut json, assigned);
                json!({ "received": json })
            }
            other => record_json(other),
        }
    }

    /// The normalized identifier of a packet this end sent.
    fn sent_id(&mut self, packet: &Packet) -> Option<Value> {
        Some(match packet {
            Packet::Publish(publish) => self.client.start(publish.packet_id?),
            Packet::Subscribe(subscribe) => self.client.start(subscribe.packet_id),
            Packet::Unsubscribe(unsubscribe) => self.client.start(unsubscribe.packet_id),
            Packet::PubRel(pubrel) => self.client.answer(pubrel.packet_id, false),
            Packet::PubAck(puback) => self.server.answer(puback.packet_id, true),
            Packet::PubRec(pubrec) => self
                .server
                .answer(pubrec.packet_id, pubrec.reason_code.is_error()),
            Packet::PubComp(pubcomp) => self.server.answer(pubcomp.packet_id, true),
            _ => return None,
        })
    }

    /// The normalized identifier of a packet the peer sent.
    fn received_id(&mut self, packet: &Packet) -> Option<Value> {
        Some(match packet {
            Packet::Publish(publish) => self.server.start(publish.packet_id?),
            Packet::PubRel(pubrel) => self.server.answer(pubrel.packet_id, false),
            Packet::PubAck(puback) => self.client.answer(puback.packet_id, true),
            Packet::PubRec(pubrec) => self
                .client
                .answer(pubrec.packet_id, pubrec.reason_code.is_error()),
            Packet::PubComp(pubcomp) => self.client.answer(pubcomp.packet_id, true),
            Packet::SubAck(suback) => self.client.answer(suback.packet_id, true),
            Packet::UnsubAck(unsuback) => self.client.answer(unsuback.packet_id, true),
            _ => return None,
        })
    }

    /// Replaces the properties of a received packet that the server chooses freely.
    fn received_properties(&mut self, packet: &Packet, json: &mut Value, assigned: &mut Numbering) {
        let Some(properties) = json.get_mut("properties").and_then(Value::as_object_mut) else {
            return;
        };
        if properties.contains_key("reason_string") {
            properties.insert("reason_string".into(), "<reason string>".into());
        }
        match packet {
            Packet::Publish(publish) => {
                if let Some(alias) = publish.properties.topic_alias {
                    let name = self.aliases.name(&alias.get().to_string());
                    properties.insert("topic_alias".into(), name.into());
                }
            }
            _ => {
                if properties.contains_key("user_properties") {
                    properties.insert("user_properties".into(), "<server user properties>".into());
                }
            }
        }
        if let Some(Value::String(id)) = properties.get("assigned_client_identifier") {
            let name = assigned.name(id);
            properties.insert("assigned_client_identifier".into(), name.into());
        }
    }
}

/// Sets a field of a JSON object.
fn set(json: &mut Value, key: &str, value: Value) {
    if let Value::Object(map) = json {
        map.insert(key.to_owned(), value);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;
    use std::time::Duration;

    use bytes::Bytes;
    use openqtt_codec::{
        ConnAckProperties, PubAck, PubComp, PubRec, PubRel, QoS, SubAck, SubAckReasonCode,
    };

    use super::*;

    fn id(value: u16) -> PacketId {
        PacketId::new(value).unwrap()
    }

    fn records(events: Vec<Recorded>) -> Vec<Record> {
        events
            .into_iter()
            .enumerate()
            .map(|(index, event)| Record {
                at: Duration::from_millis(u64::try_from(index).unwrap() * 7),
                event,
            })
            .collect()
    }

    fn one_client(events: Vec<Recorded>) -> BTreeMap<String, ClientLog> {
        BTreeMap::from([("a".to_owned(), vec![records(events)])])
    }

    #[test]
    fn identifiers_are_numbered_by_exchange() {
        let delivery = |packet_id: u16, dup: bool| {
            Recorded::Received(Packet::Publish(Publish {
                dup,
                qos: QoS::ExactlyOnce,
                topic: "t".into(),
                packet_id: Some(id(packet_id)),
                ..Publish::default()
            }))
        };
        // One broker numbers its deliveries 1, 2; another 7, 1. Both normalize the same.
        let run = |first: u16, second: u16| {
            normalized_trace(
                "s",
                "",
                &one_client(vec![
                    delivery(first, false),
                    Recorded::Sent(Packet::PubRec(PubRec::new(id(first)))),
                    delivery(first, true),
                    Recorded::Received(Packet::PubRel(PubRel::new(id(first)))),
                    Recorded::Sent(Packet::PubComp(PubComp::new(id(first)))),
                    delivery(second, false),
                ]),
            )
        };
        let one = run(1, 2);
        assert_eq!(one, run(7, 1));
        let entries = one["clients"]["a"].as_array().unwrap();
        let ids: Vec<&str> = entries[1..]
            .iter()
            .map(|entry| {
                let packet = entry.get("received").or(entry.get("sent")).unwrap();
                packet["packet_id"].as_str().unwrap()
            })
            .collect();
        // The repeat with DUP belongs to the first exchange; after PUBCOMP a new one starts.
        assert_eq!(ids, ["s1", "s1", "s1", "s1", "s1", "s2"]);
    }

    #[test]
    fn client_exchanges_and_their_answers_share_a_number() {
        let trace = normalized_trace(
            "s",
            "",
            &one_client(vec![
                Recorded::Sent(Packet::Publish(Publish {
                    qos: QoS::AtLeastOnce,
                    topic: "t".into(),
                    packet_id: Some(id(40)),
                    ..Publish::default()
                })),
                Recorded::Received(Packet::PubAck(PubAck::new(id(40)))),
                Recorded::Received(Packet::SubAck(SubAck {
                    packet_id: id(41),
                    properties: AckProperties::default(),
                    reason_codes: vec![SubAckReasonCode::GrantedQos0],
                })),
            ]),
        );
        let entries = trace["clients"]["a"].as_array().unwrap();
        assert_eq!(entries[1]["sent"]["packet_id"], "c1");
        assert_eq!(entries[2]["received"]["packet_id"], "c1");
        // A SUBACK for an exchange that never started keeps its identifier, marked.
        assert_eq!(entries[3]["received"]["packet_id"], "c?41");
    }

    #[test]
    fn what_the_server_chooses_freely_is_replaced() {
        let connack = ConnAck {
            properties: ConnAckProperties {
                assigned_client_identifier: Some("auto-x7Fz".into()),
                reason_string: Some("welcome".into()),
                user_properties: vec![("node".into(), "emqx@172.17.0.2".into())],
                receive_maximum: NonZeroU16::new(32),
                ..ConnAckProperties::default()
            },
            ..ConnAck::default()
        };
        let aliased = |topic: &str| {
            Recorded::Received(Packet::Publish(Publish {
                topic: topic.into(),
                properties: PublishProperties {
                    topic_alias: NonZeroU16::new(9),
                    user_properties: vec![("from".into(), "publisher".into())],
                    ..PublishProperties::default()
                },
                payload: Bytes::from_static(b"x"),
                ..Publish::default()
            }))
        };
        let trace = normalized_trace(
            "s",
            "ns42",
            &one_client(vec![
                Recorded::Received(Packet::from(connack)),
                aliased("ns42/t"),
                aliased(""),
            ]),
        );
        let entries = trace["clients"]["a"].as_array().unwrap();
        let connack = &entries[1]["received"]["properties"];
        assert_eq!(connack["assigned_client_identifier"], "<assigned-1>");
        assert_eq!(connack["reason_string"], "<reason string>");
        assert_eq!(connack["user_properties"], "<server user properties>");
        assert_eq!(connack["receive_maximum"], 32);
        let first = &entries[2]["received"];
        assert_eq!(first["topic"], "{ns}/t");
        assert_eq!(first["properties"]["topic_alias"], "a1");
        // A publisher's User Properties are part of the message, and kept.
        assert_eq!(
            first["properties"]["user_properties"],
            json!([["from", "publisher"]])
        );
        assert_eq!(entries[3]["received"]["properties"]["topic_alias"], "a1");
    }

    #[test]
    fn long_data_is_abbreviated_and_a_connection_ends_once() {
        let large = Bytes::from(vec![b'x'; SHOWN_BYTES + 1]);
        let trace = normalized_trace(
            "s",
            "",
            &one_client(vec![
                Recorded::SentBytes(large.clone()),
                Recorded::Received(Packet::Publish(Publish {
                    topic: "t".into(),
                    payload: large,
                    ..Publish::default()
                })),
                Recorded::StreamFinished,
                Recorded::StreamReset(0),
                Recorded::Closed(Close::Application {
                    code: 1,
                    reason: Bytes::from_static(b"bye"),
                }),
            ]),
        );
        let entries = trace["clients"]["a"].as_array().unwrap();
        let shown = format!("<{} bytes, fnv1a64 ", SHOWN_BYTES + 1);
        assert!(
            entries[1]["sent_bytes"]
                .as_str()
                .unwrap()
                .starts_with(&shown)
        );
        assert_eq!(entries[1]["sent_bytes"], entries[2]["received"]["payload"]);
        // The end of the stream, its reset and the close are one record: who ended it first.
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[3], json!({ "ended": "by the server" }));
    }

    #[test]
    fn who_ended_a_connection_first_is_kept() {
        let trace = normalized_trace(
            "s",
            "",
            &BTreeMap::from([(
                "a".to_owned(),
                vec![
                    records(vec![Recorded::Closed(Close::Locally)]),
                    records(vec![Recorded::Closed(Close::Application {
                        code: 0,
                        reason: Bytes::new(),
                    })]),
                    records(vec![Recorded::Closed(Close::TimedOut)]),
                ],
            )]),
        );
        let entries = trace["clients"]["a"].as_array().unwrap();
        assert_eq!(entries[1], json!({ "ended": "by the client" }));
        assert_eq!(entries[3], json!({ "ended": "by the server" }));
        assert_eq!(entries[5], json!({ "ended": "by the idle timeout" }));
    }

    #[test]
    fn a_raw_trace_keeps_time_and_bytes() {
        let trace = raw_trace(
            "s",
            &one_client(vec![
                Recorded::SentBytes(Bytes::from_static(&[0x10, 0x00])),
                Recorded::Malformed {
                    bytes: Bytes::from_static(&[0x20, 0x02, 0x00, 0x00]),
                    error: "short".into(),
                },
                Recorded::Closed(Close::Application {
                    code: 0,
                    reason: Bytes::new(),
                }),
            ]),
        );
        let entries = trace["clients"]["a"].as_array().unwrap();
        assert_eq!(entries[0], json!({ "connection": 1 }));
        assert_eq!(entries[1], json!({ "sent_bytes": "1000", "at_ms": 0 }));
        assert_eq!(entries[2]["received_malformed"]["bytes"], "20020000");
        assert_eq!(entries[3]["at_ms"], 14);
        assert_eq!(entries[3]["closed"]["application_code"], 0);
    }
}
