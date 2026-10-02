//! Authentication: the request at CONNECT, enhanced authentication as an extension point, and
//! re-authentication.

use bytes::Bytes;
use openqtt_codec::{
    Auth, AuthProperties, AuthReasonCode, ConnAck, Connect, ConnectReasonCode,
    DisconnectReasonCode, Packet,
};

use super::harness::{Harness, connect, connect_with, publish0, publish1};
use crate::{AuthResult, AuthStep, CloseCode, Effect, Input};

const METHOD: &str = "SCRAM-SHA-256";

fn data(text: &'static str) -> Option<Bytes> {
    Some(Bytes::from_static(text.as_bytes()))
}

fn auth(reason_code: AuthReasonCode, method: &str, payload: &'static str) -> Auth {
    Auth {
        reason_code,
        properties: AuthProperties {
            authentication_method: Some(method.to_owned()),
            authentication_data: data(payload),
            ..AuthProperties::default()
        },
    }
}

fn enhanced() -> Connect {
    connect_with("client-1", |connect| {
        connect.properties.authentication_method = Some(METHOD.into());
        connect.properties.authentication_data = data("client-first");
    })
}

/// A session waiting for its authenticator.
fn authenticating(connect: Connect) -> Harness {
    let mut harness = Harness::new();
    harness.auto.authenticate = false;
    harness.send(connect);
    harness
}

fn connack(packets: &[Packet]) -> &ConnAck {
    match packets {
        [Packet::ConnAck(connack), ..] => connack,
        other => panic!("expected CONNACK first, got {other:?}"),
    }
}

#[test]
fn the_authenticator_sees_the_connects_credentials() {
    let mut harness = authenticating(connect_with("client-1", |connect| {
        connect.username = Some("pump".into());
        connect.password = Some(Bytes::from_static(b"hunter2"));
    }));
    let request = harness.authentications.remove(0);
    assert_eq!(request.step, AuthStep::Connect);
    assert_eq!(request.client_id.as_str(), "client-1");
    assert_eq!(request.username.as_deref(), Some("pump"));
    assert_eq!(request.password, Some(Bytes::from_static(b"hunter2")));
    assert_eq!((&request.method, &request.data), (&None, &None));
    // A credential never reaches a log through `{:?}`.
    let shown = format!("{:?}", Effect::Authenticate(request.clone()));
    assert!(!shown.contains("hunter2"), "{shown}");
    assert!(shown.contains("<redacted>"), "{shown}");
}

#[test]
fn authentication_holds_back_what_the_client_sent_after_its_connect() {
    let mut harness = authenticating(connect("client-1"));
    assert!(harness.send(Packet::PingReq).is_empty());
    assert!(harness.send(publish1("t", 1, "x")).is_empty());
    assert!(harness.published().is_empty());
    let packets = harness.input(Input::Authenticated(AuthResult::Success { data: None }));
    // CONNACK, then the PINGRESP and the PUBACK, in the order the packets came.
    assert!(matches!(packets[0], Packet::ConnAck(_)));
    assert_eq!(packets[1], Packet::PingResp);
    assert!(matches!(packets[2], Packet::PubAck(_)));
}

#[test]
fn mqtt_4_12_0_2_another_step_is_an_auth_with_reason_code_0x18() {
    // covers: MQTT-4.12.0-5
    let mut harness = authenticating(enhanced());
    let request = harness.authentications.remove(0);
    assert_eq!(request.method.as_deref(), Some(METHOD));
    assert_eq!(request.data, data("client-first"));
    let packets = harness.input(Input::Authenticated(AuthResult::Continue {
        data: data("server-first"),
    }));
    assert_eq!(
        packets,
        [Packet::Auth(auth(
            AuthReasonCode::ContinueAuthentication,
            METHOD,
            "server-first"
        ))]
    );
    // The client answers with 0x18 and the same method, and the authenticator sees it.
    assert!(
        harness
            .send(auth(
                AuthReasonCode::ContinueAuthentication,
                METHOD,
                "client-final"
            ))
            .is_empty()
    );
    let request = harness.authentications.remove(0);
    assert_eq!(request.step, AuthStep::Continue);
    assert_eq!(request.data, data("client-final"));
    // A successful CONNACK carries the method too, with the last data.
    let packets = harness.input(Input::Authenticated(AuthResult::Success {
        data: data("server-final"),
    }));
    let connack = connack(&packets);
    assert_eq!(connack.reason_code, ConnectReasonCode::Success);
    assert_eq!(
        connack.properties.authentication_method.as_deref(),
        Some(METHOD)
    );
    assert_eq!(connack.properties.authentication_data, data("server-final"));
}

#[test]
fn mqtt_4_12_0_4_the_server_may_refuse_at_any_step() {
    // covers: MQTT-4.12.0-1
    let mut harness = authenticating(enhanced());
    harness.input(Input::Authenticated(AuthResult::Continue { data: None }));
    harness.send(auth(
        AuthReasonCode::ContinueAuthentication,
        METHOD,
        "wrong",
    ));
    let packets = harness.input(Input::Authenticated(AuthResult::Failure(
        ConnectReasonCode::NotAuthorized,
    )));
    assert_eq!(
        connack(&packets).reason_code,
        ConnectReasonCode::NotAuthorized
    );
    assert_eq!(harness.closed(), Some(CloseCode::NoError));

    // A method no authenticator claims gets 0x8C.
    let mut harness = authenticating(enhanced());
    let packets = harness.input(Input::Authenticated(AuthResult::Failure(
        ConnectReasonCode::BadAuthenticationMethod,
    )));
    assert_eq!(
        connack(&packets).reason_code,
        ConnectReasonCode::BadAuthenticationMethod
    );
    // A code that is not a failure is taken as 0x80.
    let mut harness = authenticating(connect("c"));
    let packets = harness.input(Input::Authenticated(AuthResult::Failure(
        ConnectReasonCode::Success,
    )));
    assert_eq!(
        connack(&packets).reason_code,
        ConnectReasonCode::UnspecifiedError
    );
}

#[test]
fn mqtt_4_12_0_3_an_auth_from_the_client_carries_0x18_and_the_same_method() {
    for wrong in [
        auth(AuthReasonCode::ContinueAuthentication, "PLAIN", "x"),
        auth(AuthReasonCode::ReAuthenticate, METHOD, "x"),
    ] {
        let mut harness = authenticating(enhanced());
        harness.input(Input::Authenticated(AuthResult::Continue { data: None }));
        let packets = harness.send(wrong);
        assert_eq!(
            connack(&packets).reason_code,
            ConnectReasonCode::ProtocolError
        );
    }
}

#[test]
fn mqtt_3_1_2_30_only_auth_or_disconnect_while_authentication_goes_on() {
    let mut harness = authenticating(enhanced());
    harness.input(Input::Authenticated(AuthResult::Continue { data: None }));
    let packets = harness.send(publish0("t", "x"));
    // Report R1, D4: a CONNACK, never a DISCONNECT, before acceptance.
    assert_eq!(
        connack(&packets).reason_code,
        ConnectReasonCode::ProtocolError
    );

    // A DISCONNECT ends it without a reply.
    let mut harness = authenticating(enhanced());
    harness.input(Input::Authenticated(AuthResult::Continue { data: None }));
    assert!(
        harness
            .send(openqtt_codec::Disconnect::default())
            .is_empty()
    );
    assert_eq!(harness.closed(), Some(CloseCode::NoError));
}

#[test]
fn mqtt_4_12_0_6_without_a_method_there_is_no_auth_and_no_method_in_connack() {
    let mut harness = authenticating(connect("client-1"));
    // An authenticator asking for another step gets a refusal instead: there is no AUTH to
    // send.
    let packets = harness.input(Input::Authenticated(AuthResult::Continue { data: None }));
    assert_eq!(
        connack(&packets).reason_code,
        ConnectReasonCode::NotAuthorized
    );
    assert!(!harness.log.iter().any(|effect| matches!(
        effect,
        Effect::Send {
            packet: Packet::Auth(_),
            ..
        }
    )));
    // Data from a successful authenticator does not go into the CONNACK either.
    let mut harness = authenticating(connect("client-1"));
    let packets = harness.input(Input::Authenticated(AuthResult::Success {
        data: data("x"),
    }));
    let connack = connack(&packets);
    assert_eq!(connack.properties.authentication_method, None);
    assert_eq!(connack.properties.authentication_data, None);
}

#[test]
fn mqtt_4_12_0_7_an_auth_without_a_method_in_the_connect_is_a_protocol_error() {
    let mut harness = Harness::connected();
    let packets = harness.send(auth(AuthReasonCode::ReAuthenticate, METHOD, "x"));
    let [Packet::Disconnect(disconnect)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(disconnect.reason_code, DisconnectReasonCode::ProtocolError);
}

/// A session authenticated with [`METHOD`], whose authenticator the test answers.
fn reauthenticating() -> Harness {
    let mut harness = Harness::new();
    harness.connect(enhanced());
    harness.auto.authenticate = false;
    harness
}

#[test]
fn mqtt_4_12_1_1_reauthentication_with_another_method_gets_disconnect_0x8c() {
    let mut harness = reauthenticating();
    let packets = harness.send(auth(AuthReasonCode::ReAuthenticate, "PLAIN", "x"));
    let [Packet::Disconnect(disconnect)] = packets.as_slice() else {
        panic!("{packets:?}");
    };
    assert_eq!(
        disconnect.reason_code,
        DisconnectReasonCode::BadAuthenticationMethod
    );
}

#[test]
fn reauthentication_runs_beside_the_other_packets() {
    let mut harness = reauthenticating();
    assert!(
        harness
            .send(auth(AuthReasonCode::ReAuthenticate, METHOD, "again"))
            .is_empty()
    );
    let request = harness.authentications.remove(0);
    assert_eq!(
        (request.step, request.data),
        (AuthStep::Reauthenticate, data("again"))
    );
    // Meanwhile the connection goes on, under the authentication it has.
    assert_eq!(harness.send(Packet::PingReq), [Packet::PingResp]);
    assert!(matches!(
        harness.send(publish1("t", 1, "x"))[..],
        [Packet::PubAck(_)]
    ));
    let packets = harness.input(Input::Authenticated(AuthResult::Continue {
        data: data("challenge"),
    }));
    assert_eq!(
        packets,
        [Packet::Auth(auth(
            AuthReasonCode::ContinueAuthentication,
            METHOD,
            "challenge"
        ))]
    );
    harness.send(auth(
        AuthReasonCode::ContinueAuthentication,
        METHOD,
        "response",
    ));
    assert_eq!(harness.authentications.remove(0).step, AuthStep::Continue);
    let packets = harness.input(Input::Authenticated(AuthResult::Success {
        data: data("ok"),
    }));
    assert_eq!(
        packets,
        [Packet::Auth(auth(AuthReasonCode::Success, METHOD, "ok"))]
    );
    // A 0x18 out of turn is a Protocol Error.
    let packets = harness.send(auth(AuthReasonCode::ContinueAuthentication, METHOD, "x"));
    assert!(
        matches!(&packets[..], [Packet::Disconnect(disconnect)] if disconnect.reason_code == DisconnectReasonCode::ProtocolError)
    );
}

#[test]
fn mqtt_4_12_1_2_a_failed_reauthentication_sends_disconnect_and_closes() {
    for (code, sent) in [
        (
            ConnectReasonCode::NotAuthorized,
            DisconnectReasonCode::NotAuthorized,
        ),
        (
            ConnectReasonCode::BadAuthenticationMethod,
            DisconnectReasonCode::BadAuthenticationMethod,
        ),
        // DISCONNECT has no 0x86: it says 0x87.
        (
            ConnectReasonCode::BadUserNameOrPassword,
            DisconnectReasonCode::NotAuthorized,
        ),
    ] {
        let mut harness = reauthenticating();
        harness.send(auth(AuthReasonCode::ReAuthenticate, METHOD, "again"));
        let packets = harness.input(Input::Authenticated(AuthResult::Failure(code)));
        let [Packet::Disconnect(disconnect)] = packets.as_slice() else {
            panic!("{packets:?}");
        };
        assert_eq!(disconnect.reason_code, sent, "{code:?}");
        assert!(harness.session.is_closed());
    }
}

#[test]
fn a_connect_timer_refuses_a_client_that_stops_authenticating() {
    let mut harness = authenticating(enhanced());
    harness.input(Input::Authenticated(AuthResult::Continue { data: None }));
    let packets = harness.advance(super::harness::seconds(10));
    assert_eq!(
        connack(&packets).reason_code,
        ConnectReasonCode::NotAuthorized
    );
}
