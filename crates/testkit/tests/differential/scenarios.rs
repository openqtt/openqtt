//! The starter catalogue of the differential harness: scenarios from report R1, played against
//! OpenQTT 1.x today and against OpenQTT 2.0 once it runs. Each names the R1 statements it
//! exercises and the decisions (D1 to D32) whose difference from EMQX its trace shows.
//!
//! Steps wait for exactly what OpenQTT 1.x sends, so a run against it does not idle; where 2.0
//! sends less, its run waits out [`DEFAULT_WAIT`](openqtt_testkit::DEFAULT_WAIT) instead, and
//! its trace shows the difference.

use std::num::{NonZeroU16, NonZeroU32};
use std::time::Duration;

use bytes::Bytes;
use openqtt_testkit::Scenario;
use openqtt_testkit::codec::{
    Connect, ConnectProperties, Packet, Publish, PublishProperties, QoS, RetainHandling, Subscribe,
    SubscribeProperties, Subscription, SubscriptionId, SubscriptionOptions, Will,
};
use openqtt_testkit::packets::{
    connect, id, publish, pubrel, subscribe, subscribe_with, unsubscribe,
};

/// A pause for what may still arrive, or must not.
const SETTLE: Duration = Duration::from_millis(500);

/// How long a scenario waits for the server to end a connection it should end.
const END: Duration = Duration::from_secs(1);

/// How long a scenario waits for OpenQTT 1.x to close a connection after its own DISCONNECT,
/// which it does about three seconds later.
const SERVER_CLOSE: Duration = Duration::from_secs(5);

/// Every scenario, in the order the harness plays them.
pub fn catalogue() -> Vec<Scenario> {
    vec![
        connect_and_connack(),
        assigned_client_identifier(),
        first_packet_and_second_connect(),
        unsupported_protocol(),
        malformed_packets(),
        qos1_publish(),
        qos2_publish(),
        qos2_repeated_publish(),
        qos2_refused_by_the_subscriber(),
        subscribe_and_unsubscribe(),
        invalid_topic_filters(),
        invalid_topic_names(),
        retained_after_subscribe(),
        retained_cleared_by_zero_bytes(),
        keep_alive_timeout(),
        takeover(),
        client_maximum_packet_size(),
        server_maximum_packet_size(),
        topic_aliases(),
        subscription_identifiers(),
        no_local(),
        retain_as_published(),
        session_resumed(),
        will_message(),
    ]
}

/// A CONNECT as [`connect`] makes it, changed by `change`.
fn connect_with(client_id: &str, change: impl FnOnce(&mut Connect)) -> Connect {
    let mut packet = connect(client_id);
    change(&mut packet);
    packet
}

/// Subscription options at `qos`, changed by `change`.
fn options(qos: QoS, change: impl FnOnce(&mut SubscriptionOptions)) -> SubscriptionOptions {
    let mut options = SubscriptionOptions {
        maximum_qos: qos,
        ..SubscriptionOptions::default()
    };
    change(&mut options);
    options
}

/// A retained PUBLISH.
fn retained(topic: &str, qos: QoS, packet_id: u16, payload: &str) -> Publish {
    Publish {
        retain: true,
        ..publish(topic, qos, packet_id, payload)
    }
}

fn connect_and_connack() -> Scenario {
    let full = Connect {
        clean_start: true,
        keep_alive: 30,
        client_id: "{ns}-full".into(),
        properties: ConnectProperties {
            session_expiry_interval: Some(0),
            receive_maximum: NonZeroU16::new(10),
            maximum_packet_size: NonZeroU32::new(65_536),
            topic_alias_maximum: Some(4),
            request_response_information: Some(true),
            request_problem_information: Some(true),
            user_properties: vec![("scenario".into(), "connect".into())],
            ..ConnectProperties::default()
        },
        username: Some("{ns}-user".into()),
        password: Some(Bytes::from_static(b"secret")),
        ..Connect::default()
    };
    Scenario::new(
        "connect_and_connack",
        "CONNECT with every property a client sets, the capabilities CONNACK announces, \
         PINGREQ, and the CONNACKs for a Keep Alive of 0 or 5 and a Session Expiry of never",
    )
    .statements(&[
        "MQTT-3.1.4-5",
        "MQTT-3.2.0-1",
        "MQTT-3.2.2-2",
        "MQTT-3.2.2-9",
        "MQTT-3.2.2-22",
        "MQTT-3.1.2-28",
        "MQTT-3.12.4-1",
    ])
    .divergences(&["D13", "D18", "D22", "D23", "D28"])
    .connect("full", full)
    .send("full", Packet::PingReq)
    .receive("full", 1)
    .disconnect("full")
    .connect(
        "forever",
        connect_with("{ns}-forever", |c| {
            c.keep_alive = 0;
            c.properties.session_expiry_interval = Some(u32::MAX);
        }),
    )
    .disconnect("forever")
    .connect("brief", connect_with("{ns}-brief", |c| c.keep_alive = 5))
    .disconnect("brief")
}

fn assigned_client_identifier() -> Scenario {
    Scenario::new(
        "assigned_client_identifier",
        "A zero-length Client Identifier with Clean Start 1 gets one assigned; with Clean \
         Start 0 OpenQTT 1.x refuses it",
    )
    .statements(&[
        "MQTT-3.1.3-6",
        "MQTT-3.1.3-7",
        "MQTT-3.1.3-8",
        "MQTT-3.2.2-16",
    ])
    .divergences(&["D19", "D25"])
    .connect("fresh", connect(""))
    .disconnect("fresh")
    .connect("resume", connect_with("", |c| c.clean_start = false))
    .await_end("resume", END)
    .close("resume", 0)
}

fn first_packet_and_second_connect() -> Scenario {
    Scenario::new(
        "first_packet_and_second_connect",
        "A first packet other than CONNECT closes the connection without a reply; a second \
         CONNECT is a Protocol Error",
    )
    .statements(&["MQTT-3.1.0-1", "MQTT-3.1.0-2"])
    .open("ping_first")
    .send("ping_first", Packet::PingReq)
    .await_end("ping_first", END)
    .close("ping_first", 0)
    .connect("twice", connect("{ns}-twice"))
    .send("twice", connect("{ns}-twice"))
    .receive("twice", 1)
    .await_end("twice", END)
    .close("twice", 0)
}

/// MQTT 3.1.1's CONNECT: name `MQTT`, level 4, Clean Session, Keep Alive 60, `d1-v311`.
const CONNECT_V311: &[u8] = &[
    0x10, 0x13, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x07, b'd', b'1',
    b'-', b'v', b'3', b'1', b'1',
];

/// MQTT 3.1's CONNECT: name `MQIsdp`, level 3, `d1-v31`.
const CONNECT_V31: &[u8] = &[
    0x10, 0x14, 0x00, 0x06, b'M', b'Q', b'I', b's', b'd', b'p', 0x03, 0x02, 0x00, 0x3C, 0x00, 0x06,
    b'd', b'1', b'-', b'v', b'3', b'1',
];

/// A CONNECT at level 6 in the 3.1.1 layout, `d1-v6`.
const CONNECT_V6: &[u8] = &[
    0x10, 0x11, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x06, 0x02, 0x00, 0x3C, 0x00, 0x05, b'd', b'1',
    b'-', b'v', b'6',
];

/// An MQTT 5 CONNECT with the bridge bit on its level, 0x85, `d1-brdg5`.
const CONNECT_BRIDGE_V5: &[u8] = &[
    0x10, 0x15, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x85, 0x02, 0x00, 0x3C, 0x00, 0x00, 0x08, b'd',
    b'1', b'-', b'b', b'r', b'd', b'g', b'5',
];

/// An MQTT 5 CONNECT naming another protocol, `MQTX`, `d1-mqtx`.
const CONNECT_OTHER_PROTOCOL: &[u8] = &[
    0x10, 0x14, 0x00, 0x04, b'M', b'Q', b'T', b'X', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00, 0x07, b'd',
    b'1', b'-', b'm', b'q', b't', b'x',
];

fn unsupported_protocol() -> Scenario {
    let mut scenario = Scenario::new(
        "unsupported_protocol",
        "CONNECT from MQTT 3.1 and 3.1.1, at level 6, at level 0x85 and naming another \
         protocol: what each gets before the connection closes",
    )
    .statements(&["MQTT-3.1.2-1", "MQTT-3.1.2-2"])
    .divergences(&["D1"]);
    for (client, bytes) in [
        ("v311", CONNECT_V311),
        ("v31", CONNECT_V31),
        ("v6", CONNECT_V6),
        ("bridge_v5", CONNECT_BRIDGE_V5),
        ("other_protocol", CONNECT_OTHER_PROTOCOL),
    ] {
        scenario = scenario
            .open(client)
            .send_bytes(client, bytes)
            .receive(client, 1)
            .await_end(client, END)
            .close(client, 0);
    }
    scenario
}

/// Reserved packet type 0.
const RESERVED_TYPE: &[u8] = &[0x00, 0x00];

/// PUBLISH with both QoS bits set, Packet Identifier 1, to `oq/d3/q3`.
const PUBLISH_QOS_3: &[u8] = &[
    0x36, 0x0E, 0x00, 0x08, b'o', b'q', b'/', b'd', b'3', b'/', b'q', b'3', 0x00, 0x01, 0x00, b'x',
];

/// SUBSCRIBE with fixed-header flags 0000 instead of 0010, to `oq/d3/sb` at QoS 1.
const SUBSCRIBE_FLAGS_0: &[u8] = &[
    0x80, 0x0E, 0x00, 0x01, 0x00, 0x00, 0x08, b'o', b'q', b'/', b'd', b'3', b'/', b's', b'b', 0x01,
];

/// PUBLISH at QoS 1 with Packet Identifier 0, to `oq/d3/id`.
const PUBLISH_ID_0: &[u8] = &[
    0x32, 0x0E, 0x00, 0x08, b'o', b'q', b'/', b'd', b'3', b'/', b'i', b'd', 0x00, 0x00, 0x00, b'x',
];

/// PUBLISH at QoS 0 with DUP set, to `oq/d3/dp`.
const PUBLISH_QOS_0_DUP: &[u8] = &[
    0x38, 0x0C, 0x00, 0x08, b'o', b'q', b'/', b'd', b'3', b'/', b'd', b'p', 0x00, b'x',
];

fn malformed_packets() -> Scenario {
    let mut scenario = Scenario::new(
        "malformed_packets",
        "Packets the codec of OpenQTT 2.0 refuses as malformed or a protocol error, each on a \
         connection of its own: what OpenQTT 1.x does with them",
    )
    .statements(&[
        "MQTT-4.13.1-1",
        "MQTT-2.1.3-1",
        "MQTT-3.3.1-4",
        "MQTT-3.8.1-1",
        "MQTT-2.2.1-3",
        "MQTT-3.3.1-2",
    ])
    .divergences(&["D3", "D4"]);
    for (client, bytes) in [
        ("reserved_type", RESERVED_TYPE),
        ("qos_3", PUBLISH_QOS_3),
        ("subscribe_flags", SUBSCRIBE_FLAGS_0),
        ("packet_id_0", PUBLISH_ID_0),
        ("qos_0_dup", PUBLISH_QOS_0_DUP),
    ] {
        scenario = scenario
            .connect(client, connect(&format!("{{ns}}-{client}")))
            .send_bytes(client, bytes)
            .receive(client, 1)
            .await_end(client, END)
            .close(client, 0);
    }
    scenario
}

fn qos1_publish() -> Scenario {
    Scenario::new(
        "qos1_publish",
        "QoS 1: PUBACK to the publisher, delivery at QoS 1 and PUBACK from the subscriber, and \
         the PUBACK of a message nobody subscribes to",
    )
    .statements(&[
        "MQTT-3.3.4-1",
        "MQTT-4.3.2-2",
        "MQTT-4.3.2-4",
        "MQTT-2.2.1-5",
        "MQTT-3.3.2-3",
        "MQTT-4.5.0-1",
    ])
    .connect("sub", connect("{ns}-sub"))
    .send("sub", subscribe(1, "{ns}/q1", QoS::AtLeastOnce))
    .receive("sub", 1)
    .connect("pub", connect("{ns}-pub"))
    .send("pub", publish("{ns}/q1", QoS::AtLeastOnce, 1, "one"))
    .receive("pub", 1)
    .receive("sub", 1)
    .ack("sub")
    .send("pub", publish("{ns}/nobody", QoS::AtLeastOnce, 2, "two"))
    .receive("pub", 1)
    .wait(SETTLE)
    .disconnect("pub")
    .disconnect("sub")
}

fn qos2_publish() -> Scenario {
    Scenario::new(
        "qos2_publish",
        "QoS 2 both ways: PUBREC, PUBREL and PUBCOMP for the publisher and for the subscriber",
    )
    .statements(&[
        "MQTT-3.3.4-1",
        "MQTT-4.3.3-2",
        "MQTT-4.3.3-3",
        "MQTT-4.3.3-5",
        "MQTT-4.3.3-8",
        "MQTT-4.3.3-11",
        "MQTT-2.2.1-5",
    ])
    .connect("sub", connect("{ns}-sub"))
    .send("sub", subscribe(1, "{ns}/q2", QoS::ExactlyOnce))
    .receive("sub", 1)
    .connect("pub", connect("{ns}-pub"))
    .send("pub", publish("{ns}/q2", QoS::ExactlyOnce, 1, "two"))
    .receive("pub", 1)
    .send("pub", pubrel(1))
    .receive("pub", 1)
    .receive("sub", 1)
    .ack("sub")
    .receive("sub", 1)
    .ack("sub")
    .wait(SETTLE)
    .disconnect("pub")
    .disconnect("sub")
}

fn qos2_repeated_publish() -> Scenario {
    Scenario::new(
        "qos2_repeated_publish",
        "A QoS 2 PUBLISH repeated before its PUBREL: the PUBREC it gets, and whether the \
         subscriber receives it twice",
    )
    .statements(&["MQTT-4.3.3-9", "MQTT-4.3.3-10"])
    .divergences(&["D8"])
    .connect("sub", connect("{ns}-sub"))
    .send("sub", subscribe(1, "{ns}/again", QoS::ExactlyOnce))
    .receive("sub", 1)
    .connect("pub", connect("{ns}-pub"))
    .send("pub", publish("{ns}/again", QoS::ExactlyOnce, 7, "once"))
    .receive("pub", 1)
    .repeat("pub")
    .receive("pub", 1)
    .send("pub", pubrel(7))
    .receive("pub", 1)
    .receive("sub", 1)
    .ack("sub")
    .receive("sub", 1)
    .ack("sub")
    .wait(SETTLE)
    .disconnect("pub")
    .disconnect("sub")
}

fn qos2_refused_by_the_subscriber() -> Scenario {
    Scenario::new(
        "qos2_refused_by_the_subscriber",
        "A subscriber answers a QoS 2 delivery with PUBREC 0x80, then repeats it: whether the \
         server still sends PUBREL, and with which code",
    )
    .statements(&["MQTT-4.3.3-4", "MQTT-4.4.0-2", "MQTT-3.6.2-1"])
    .divergences(&["D9", "D10"])
    .connect("sub", connect("{ns}-sub"))
    .send("sub", subscribe(1, "{ns}/refused", QoS::ExactlyOnce))
    .receive("sub", 1)
    .connect("pub", connect("{ns}-pub"))
    .send("pub", publish("{ns}/refused", QoS::ExactlyOnce, 1, "no"))
    .receive("pub", 1)
    .send("pub", pubrel(1))
    .receive("pub", 1)
    .receive("sub", 1)
    .ack_refusing("sub")
    .receive("sub", 1)
    .repeat("sub")
    .receive("sub", 1)
    .ack("sub")
    .wait(SETTLE)
    .disconnect("pub")
    .disconnect("sub")
}

fn subscribe_and_unsubscribe() -> Scenario {
    let three = Subscribe {
        packet_id: id(1),
        properties: SubscribeProperties::default(),
        subscriptions: [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce]
            .into_iter()
            .map(|qos| Subscription {
                filter: format!("{{ns}}/s/{}", qos.value()),
                options: options(qos, |_| {}),
            })
            .collect(),
    };
    Scenario::new(
        "subscribe_and_unsubscribe",
        "One SUBACK code per filter in order, a subscription replaced, and UNSUBACK for a \
         filter that existed and one that did not",
    )
    .statements(&[
        "MQTT-3.8.4-1",
        "MQTT-3.8.4-2",
        "MQTT-3.8.4-3",
        "MQTT-3.8.4-5",
        "MQTT-3.8.4-6",
        "MQTT-3.9.3-1",
        "MQTT-3.10.4-4",
        "MQTT-3.10.4-5",
        "MQTT-3.10.4-6",
        "MQTT-3.11.3-1",
    ])
    .connect("sub", connect("{ns}-sub"))
    .send("sub", three)
    .receive("sub", 1)
    .send("sub", subscribe(2, "{ns}/s/1", QoS::AtMostOnce))
    .receive("sub", 1)
    .send("sub", unsubscribe(3, &["{ns}/s/0", "{ns}/s/none"]))
    .receive("sub", 1)
    .disconnect("sub")
}

fn invalid_topic_filters() -> Scenario {
    let mut scenario = Scenario::new(
        "invalid_topic_filters",
        "SUBSCRIBE with a filter that breaks section 4.7 or 4.8, beside a valid one: the whole \
         SUBSCRIBE refused, or the one filter",
    )
    .statements(&[
        "MQTT-4.7.1-1",
        "MQTT-4.7.1-2",
        "MQTT-4.7.3-1",
        "MQTT-4.8.2-1",
        "MQTT-4.8.2-2",
    ])
    .divergences(&["D32"]);
    for (client, filter) in [
        ("hash_inside", "{ns}/#/x"),
        ("plus_in_level", "{ns}/a+"),
        ("share_without_filter", "$share/{ns}"),
        ("empty", ""),
    ] {
        let subscribe = Subscribe {
            packet_id: id(1),
            properties: SubscribeProperties::default(),
            subscriptions: [("{ns}/ok", QoS::AtLeastOnce), (filter, QoS::AtLeastOnce)]
                .into_iter()
                .map(|(filter, qos)| Subscription {
                    filter: filter.into(),
                    options: options(qos, |_| {}),
                })
                .collect(),
        };
        scenario = scenario
            .connect(client, connect(&format!("{{ns}}-{client}")))
            .send(client, subscribe)
            .receive(client, 1)
            .await_end(client, END)
            .close(client, 0);
    }
    scenario
}

fn invalid_topic_names() -> Scenario {
    let response = Publish {
        properties: PublishProperties {
            response_topic: Some("{ns}/reply/+".into()),
            ..PublishProperties::default()
        },
        ..publish("{ns}/request", QoS::AtLeastOnce, 1, "x")
    };
    Scenario::new(
        "invalid_topic_names",
        "A QoS 1 PUBLISH whose Topic Name or Response Topic holds a wildcard: DISCONNECT, or \
         PUBACK 0x90 and the connection kept",
    )
    .statements(&["MQTT-3.3.2-2", "MQTT-3.3.2-14", "MQTT-4.7.0-1"])
    .divergences(&["D32"])
    .connect("topic", connect("{ns}-topic"))
    .send("topic", publish("{ns}/w/+", QoS::AtLeastOnce, 1, "x"))
    .receive("topic", 1)
    .await_end("topic", END)
    .close("topic", 0)
    .connect("response", connect("{ns}-response"))
    .send("response", response)
    .receive("response", 1)
    .await_end("response", END)
    .close("response", 0)
}

fn retained_after_subscribe() -> Scenario {
    let filter = "{ns}/ret/+";
    Scenario::new(
        "retained_after_subscribe",
        "A retained message after SUBACK, with RETAIN set, under each Retain Handling, and again \
         when a subscription is replaced",
    )
    .statements(&[
        "MQTT-3.3.1-5",
        "MQTT-3.3.1-9",
        "MQTT-3.3.1-10",
        "MQTT-3.3.1-11",
        "MQTT-3.8.4-4",
    ])
    .divergences(&["D15"])
    .connect("pub", connect("{ns}-pub"))
    .send("pub", retained("{ns}/ret/a", QoS::AtLeastOnce, 1, "kept"))
    .receive("pub", 1)
    .connect("sub", connect("{ns}-sub"))
    .send("sub", subscribe(1, filter, QoS::AtLeastOnce))
    .receive("sub", 2)
    .ack("sub")
    .connect("never", connect("{ns}-never"))
    .send(
        "never",
        subscribe_with(
            1,
            filter,
            options(QoS::AtLeastOnce, |o| {
                o.retain_handling = RetainHandling::DoNotSend;
            }),
        ),
    )
    .receive("never", 1)
    .connect("if_new", connect("{ns}-if_new"))
    .send(
        "if_new",
        subscribe_with(
            1,
            filter,
            options(QoS::AtLeastOnce, |o| {
                o.retain_handling = RetainHandling::SendIfNew;
            }),
        ),
    )
    .receive("if_new", 2)
    .ack("if_new")
    .send(
        "if_new",
        subscribe_with(
            2,
            filter,
            options(QoS::AtLeastOnce, |o| {
                o.retain_handling = RetainHandling::SendIfNew;
            }),
        ),
    )
    .receive("if_new", 1)
    .send("sub", subscribe(2, filter, QoS::AtLeastOnce))
    .receive("sub", 2)
    .ack("sub")
    .wait(SETTLE)
    .disconnect("pub")
    .disconnect("sub")
    .disconnect("never")
    .disconnect("if_new")
}

fn retained_cleared_by_zero_bytes() -> Scenario {
    Scenario::new(
        "retained_cleared_by_zero_bytes",
        "A zero-byte retained PUBLISH reaches the subscribers as usual and removes the retained \
         message, so a later subscriber gets none",
    )
    .statements(&["MQTT-3.3.1-6", "MQTT-3.3.1-7"])
    .divergences(&["D15"])
    .connect("pub", connect("{ns}-pub"))
    .send("pub", retained("{ns}/clear", QoS::AtLeastOnce, 1, "x"))
    .receive("pub", 1)
    .connect("sub", connect("{ns}-sub"))
    .send("sub", subscribe(1, "{ns}/clear", QoS::AtLeastOnce))
    .receive("sub", 2)
    .ack("sub")
    .send("pub", retained("{ns}/clear", QoS::AtLeastOnce, 2, ""))
    .receive("pub", 1)
    .receive("sub", 1)
    .ack("sub")
    .connect("late", connect("{ns}-late"))
    .send("late", subscribe(1, "{ns}/clear", QoS::AtLeastOnce))
    .receive("late", 1)
    .wait(SETTLE)
    .disconnect("pub")
    .disconnect("sub")
    .disconnect("late")
}

fn keep_alive_timeout() -> Scenario {
    Scenario::new(
        "keep_alive_timeout",
        "A client silent past 1.5 times a Keep Alive of 2 seconds gets DISCONNECT 0x8D",
    )
    .statements(&["MQTT-3.1.2-22"])
    .divergences(&["D5"])
    .connect("idle", connect_with("{ns}-idle", |c| c.keep_alive = 2))
    .receive_within("idle", 1, Duration::from_secs(6))
    .await_end("idle", END)
    .close("idle", 0)
}

fn takeover() -> Scenario {
    Scenario::new(
        "takeover",
        "A second connection with the same Client Identifier takes over: DISCONNECT 0x8E to the \
         first",
    )
    .statements(&["MQTT-3.1.4-3", "MQTT-3.2.2-2"])
    .connect("first", connect("{ns}-same"))
    .connect("second", connect("{ns}-same"))
    .receive("first", 1)
    .await_close("first", SERVER_CLOSE)
    .disconnect("second")
}

fn client_maximum_packet_size() -> Scenario {
    // A PUBLISH delivered on `{ns}/size/N` is 2 bytes of fixed header, 28 of Topic Name, 1 of
    // Property Length and the payload: 31 bytes plus the payload, the namespace being 18
    // characters. So 32, 33 and 34 bytes of payload make packets of 63, 64 and 65 bytes.
    //
    // After discarding a packet too large for a client, OpenQTT 1.x can hold back whatever it
    // sends that client next, a PINGRESP included, for a few seconds or until the client's
    // DISCONNECT, so nothing may follow a discard on the same connection: each packet goes to a
    // subscriber of its own, which only waits to see whether it arrives.
    let mut scenario = Scenario::new(
        "client_maximum_packet_size",
        "Subscribers with a Maximum Packet Size of 64 are sent packets of 63, exactly 64 and 65 \
         bytes, and the last must be discarded",
    )
    .statements(&["MQTT-3.1.2-24", "MQTT-3.1.2-25"])
    .divergences(&["D6"]);
    let sizes = [("fits", 63, 32), ("at_limit", 64, 33), ("over", 65, 34)];
    for (client, size, _) in sizes {
        scenario = scenario
            .connect(
                client,
                connect_with(&format!("{{ns}}-{client}"), |c| {
                    c.properties.maximum_packet_size = NonZeroU32::new(64);
                }),
            )
            .send(
                client,
                subscribe(1, &format!("{{ns}}/size/{size}"), QoS::AtMostOnce),
            )
            .receive(client, 1);
    }
    scenario = scenario.connect("pub", connect("{ns}-pub"));
    for (_, size, payload) in sizes {
        scenario = scenario.send(
            "pub",
            publish(
                &format!("{{ns}}/size/{size}"),
                QoS::AtMostOnce,
                0,
                &"x".repeat(payload),
            ),
        );
    }
    scenario = scenario.receive("fits", 1).wait(SETTLE).disconnect("pub");
    for (client, _, _) in sizes {
        scenario = scenario.disconnect(client);
    }
    scenario
}

fn server_maximum_packet_size() -> Scenario {
    // A QoS 1 PUBLISH to `{ns}/huge` has 28 bytes of variable header: 25 of Topic Name, 2 of
    // Packet Identifier and 1 of Property Length. A Remaining Length of 1,048,573 then makes a
    // packet of 1,048,577 bytes, one over 1 MiB; 1,048,577 makes it 1,048,581.
    let payload = |remaining_length: usize| "x".repeat(remaining_length - 28);
    Scenario::new(
        "server_maximum_packet_size",
        "PUBLISH just over 1 MiB in all but not in Remaining Length, then over 1 MiB in \
         Remaining Length too: which limit the server applies to what clients send",
    )
    .statements(&["MQTT-3.2.2-15"])
    .divergences(&["D23"])
    .connect("whole", connect("{ns}-whole"))
    .send(
        "whole",
        publish("{ns}/huge", QoS::AtLeastOnce, 1, &payload(1_048_573)),
    )
    .receive("whole", 1)
    .await_end("whole", END)
    .close("whole", 0)
    .connect("remaining", connect("{ns}-remaining"))
    .send(
        "remaining",
        publish("{ns}/huge", QoS::AtLeastOnce, 1, &payload(1_048_577)),
    )
    .receive("remaining", 1)
    .await_end("remaining", END)
    .close("remaining", 0)
}

fn topic_aliases() -> Scenario {
    let aliased = |topic: &str, alias: u16, payload: &str| Publish {
        properties: PublishProperties {
            topic_alias: NonZeroU16::new(alias),
            ..PublishProperties::default()
        },
        ..publish(topic, QoS::AtMostOnce, 0, payload)
    };
    Scenario::new(
        "topic_aliases",
        "Aliases from the publisher, the server's alias assignment to a subscriber with a Topic \
         Alias Maximum of 2, and none to a subscriber without one",
    )
    .statements(&[
        "MQTT-3.3.2-12",
        "MQTT-3.1.2-26",
        "MQTT-3.1.2-27",
        "MQTT-3.3.2-11",
    ])
    .divergences(&["D22"])
    .connect(
        "aliases",
        connect_with("{ns}-aliases", |c| {
            c.properties.topic_alias_maximum = Some(2);
        }),
    )
    .send("aliases", subscribe(1, "{ns}/alias/+", QoS::AtMostOnce))
    .receive("aliases", 1)
    .connect("plain", connect("{ns}-plain"))
    .send("plain", subscribe(1, "{ns}/alias/+", QoS::AtMostOnce))
    .receive("plain", 1)
    .connect("pub", connect("{ns}-pub"))
    .send("pub", aliased("{ns}/alias/a", 1, "1"))
    .send("pub", aliased("", 1, "2"))
    .send("pub", publish("{ns}/alias/b", QoS::AtMostOnce, 0, "3"))
    .send("pub", publish("{ns}/alias/c", QoS::AtMostOnce, 0, "4"))
    .send("pub", publish("{ns}/alias/a", QoS::AtMostOnce, 0, "5"))
    .receive("aliases", 5)
    .receive("plain", 5)
    .wait(SETTLE)
    .disconnect("pub")
    .disconnect("aliases")
    .disconnect("plain")
}

fn subscription_identifiers() -> Scenario {
    let with_identifier = |packet_id: u16, identifier: u32, filter: &str| Subscribe {
        packet_id: id(packet_id),
        properties: SubscribeProperties {
            subscription_identifier: SubscriptionId::new(identifier),
            ..SubscribeProperties::default()
        },
        subscriptions: vec![Subscription {
            filter: filter.into(),
            options: options(QoS::AtMostOnce, |_| {}),
        }],
    };
    Scenario::new(
        "subscription_identifiers",
        "Two overlapping subscriptions with identifiers 7 and 9: the copies delivered and the \
         identifiers on each; and the largest identifier there is",
    )
    .statements(&[
        "MQTT-3.3.4-2",
        "MQTT-3.3.4-3",
        "MQTT-3.3.4-4",
        "MQTT-3.3.4-5",
    ])
    .divergences(&["D12", "D14"])
    .connect("sub", connect("{ns}-sub"))
    .send("sub", with_identifier(1, 7, "{ns}/sid/+"))
    .receive("sub", 1)
    .send("sub", with_identifier(2, 9, "{ns}/sid/#"))
    .receive("sub", 1)
    .connect("pub", connect("{ns}-pub"))
    .send("pub", publish("{ns}/sid/x", QoS::AtMostOnce, 0, "both"))
    .receive("sub", 2)
    .wait(SETTLE)
    .connect("largest", connect("{ns}-largest"))
    .send(
        "largest",
        with_identifier(1, 268_435_455, "{ns}/sid/largest"),
    )
    .receive("largest", 1)
    .await_end("largest", END)
    .close("largest", 0)
    .disconnect("pub")
    .disconnect("sub")
}

fn no_local() -> Scenario {
    Scenario::new(
        "no_local",
        "No Local: a client's own message does not come back to it, and still reaches others",
    )
    .statements(&["MQTT-3.8.3-3"])
    .connect("me", connect("{ns}-me"))
    .send(
        "me",
        subscribe_with(
            1,
            "{ns}/nl",
            options(QoS::AtLeastOnce, |o| o.no_local = true),
        ),
    )
    .receive("me", 1)
    .connect("other", connect("{ns}-other"))
    .send("other", subscribe(1, "{ns}/nl", QoS::AtLeastOnce))
    .receive("other", 1)
    .send("me", publish("{ns}/nl", QoS::AtLeastOnce, 1, "mine"))
    .receive("me", 1)
    .receive("other", 1)
    .ack("other")
    .wait(SETTLE)
    .disconnect("me")
    .disconnect("other")
}

fn retain_as_published() -> Scenario {
    Scenario::new(
        "retain_as_published",
        "A live retained message reaches a Retain As Published subscription with RETAIN 1 and \
         another with RETAIN 0",
    )
    .statements(&["MQTT-3.3.1-12", "MQTT-3.3.1-13"])
    .connect("as_published", connect("{ns}-as_published"))
    .send(
        "as_published",
        subscribe_with(
            1,
            "{ns}/rap",
            options(QoS::AtLeastOnce, |o| o.retain_as_published = true),
        ),
    )
    .receive("as_published", 1)
    .connect("cleared", connect("{ns}-cleared"))
    .send("cleared", subscribe(1, "{ns}/rap", QoS::AtLeastOnce))
    .receive("cleared", 1)
    .connect("pub", connect("{ns}-pub"))
    .send("pub", retained("{ns}/rap", QoS::AtLeastOnce, 1, "live"))
    .receive("pub", 1)
    .receive("as_published", 1)
    .ack("as_published")
    .receive("cleared", 1)
    .ack("cleared")
    .wait(SETTLE)
    .disconnect("pub")
    .disconnect("as_published")
    .disconnect("cleared")
}

fn session_resumed() -> Scenario {
    let persistent = connect_with("{ns}-persistent", |c| {
        c.properties.session_expiry_interval = Some(300);
    });
    let resumed = Connect {
        clean_start: false,
        ..persistent.clone()
    };
    Scenario::new(
        "session_resumed",
        "A session with an expiry resumes with Session Present 1 and the message queued while it \
         was away; Clean Start 0 without a session gets Session Present 0",
    )
    .statements(&[
        "MQTT-3.1.2-5",
        "MQTT-3.1.2-6",
        "MQTT-3.1.2-23",
        "MQTT-3.2.2-3",
        "MQTT-4.5.0-1",
    ])
    .connect("device", persistent)
    .send("device", subscribe(1, "{ns}/cmd", QoS::AtLeastOnce))
    .receive("device", 1)
    .disconnect("device")
    .connect("pub", connect("{ns}-pub"))
    .send(
        "pub",
        publish("{ns}/cmd", QoS::AtLeastOnce, 1, "while away"),
    )
    .receive("pub", 1)
    .connect("device", resumed)
    .receive("device", 1)
    .ack("device")
    .wait(SETTLE)
    .disconnect("device")
    .connect(
        "fresh",
        connect_with("{ns}-fresh", |c| c.clean_start = false),
    )
    .disconnect("fresh")
    .disconnect("pub")
}

fn will_message() -> Scenario {
    let will = |payload: &str| Will {
        qos: QoS::AtLeastOnce,
        topic: "{ns}/will".into(),
        payload: Bytes::copy_from_slice(payload.as_bytes()),
        ..Will::default()
    };
    Scenario::new(
        "will_message",
        "The Will Message of a connection closed without DISCONNECT is published, not retained; \
         that of one ended with DISCONNECT 0x00 is not",
    )
    .statements(&[
        "MQTT-3.1.2-7",
        "MQTT-3.1.2-8",
        "MQTT-3.1.2-14",
        "MQTT-3.14.4-3",
    ])
    .connect("watch", connect("{ns}-watch"))
    .send("watch", subscribe(1, "{ns}/will", QoS::AtLeastOnce))
    .receive("watch", 1)
    .connect(
        "clean",
        connect_with("{ns}-clean", |c| c.will = Some(will("must not arrive"))),
    )
    .disconnect("clean")
    .connect(
        "abrupt",
        connect_with("{ns}-abrupt", |c| c.will = Some(will("gone"))),
    )
    .close("abrupt", 0)
    .receive("watch", 1)
    .ack("watch")
    .wait(SETTLE)
    .disconnect("watch")
}
