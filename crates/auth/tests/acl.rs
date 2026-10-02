//! The ACL format as `docs/spec/acl.md` defines it: one test per construct, and one per rule of
//! report R2 that authorization keeps (rules 9 to 16).

use std::sync::Arc;

use openqtt_auth::Error;
use openqtt_auth::acl::{Acl, AclAuthorizer};
use openqtt_core::{ClientId, QoS, TopicFilter, TopicName, Username};
use openqtt_ext::{Action, Authorizer, ClientInfo, Permission, Principal};

const ALLOW: Permission = Permission::Allow;
const DENY: Permission = Permission::Deny;

fn acl(text: &str) -> Arc<Acl> {
    match Acl::from_toml(text) {
        Ok(acl) => Arc::new(acl),
        Err(error) => panic!("{error}"),
    }
}

fn refused(text: &str) -> String {
    match Acl::from_toml(text) {
        Err(error @ (Error::Acl { .. } | Error::AclSyntax { .. })) => error.to_string(),
        other => panic!("not refused: {other:?}"),
    }
}

/// A client named `name`, as its user name and its client identifier, from 192.0.2.10.
fn client(name: &str) -> ClientInfo {
    named(Some(name), name)
}

fn named(username: Option<&str>, client_id: &str) -> ClientInfo {
    ClientInfo::new(
        ClientId::new(client_id).expect("valid in a test"),
        Principal::new(username.map(|name| Username::new(name).expect("valid in a test"))),
    )
    .with_address("192.0.2.10:50000".parse().expect("valid in a test"))
}

fn from(client: ClientInfo, address: &str) -> ClientInfo {
    client.with_address(address.parse().expect("valid in a test"))
}

/// The decision for a publish, both unbound and bound, which must agree.
fn publish(acl: &Arc<Acl>, client: &ClientInfo, topic: &str, qos: u8, retain: bool) -> Permission {
    let topic = TopicName::new(topic).expect("valid in a test");
    let action = Action::publish(&topic, QoS::from_u8(qos).expect("valid in a test"), retain);
    decide(acl, client, &action)
}

fn subscribe(acl: &Arc<Acl>, client: &ClientInfo, filter: &str, qos: u8) -> Permission {
    let filter = TopicFilter::new(filter).expect("valid in a test");
    let action = Action::subscribe(&filter, QoS::from_u8(qos).expect("valid in a test"));
    decide(acl, client, &action)
}

fn receive(acl: &Arc<Acl>, client: &ClientInfo, topic: &str) -> Permission {
    let topic = TopicName::new(topic).expect("valid in a test");
    let action = Action::receive(&topic, QoS::AtLeastOnce, false);
    decide(acl, client, &action)
}

fn decide(acl: &Arc<Acl>, client: &ClientInfo, action: &Action<'_>) -> Permission {
    let unbound = acl.decide(client, action);
    let bound = acl.bind(client).decide(action);
    assert_eq!(unbound, bound, "bound and unbound disagree on {action:?}");
    unbound
}

// --- R2 rules 9 to 16 ---------------------------------------------------------------------

#[test]
fn r2_rule_9_first_match_wins_and_no_match_denies() {
    let rules = acl(r##"
[[rule]]
permission = "deny"
action = "publish"
topics = ["a/secret"]

[[rule]]
permission = "allow"
action = "publish"
topics = ["a/#"]

[[rule]]
permission = "deny"
action = "publish"
topics = ["a/open"]
"##);
    let device = client("pump-3");
    assert_eq!(publish(&rules, &device, "a/secret", 1, false), DENY);
    // The allow comes first, so the later deny never decides.
    assert_eq!(publish(&rules, &device, "a/open", 1, false), ALLOW);
    // Nothing matches: denied.
    assert_eq!(publish(&rules, &device, "b/x", 1, false), DENY);
    assert_eq!(subscribe(&rules, &device, "a/#", 1), DENY);
    // An empty file, as when `auth.acl_file` is unset, denies everything.
    let none = acl("");
    assert_eq!(publish(&none, &device, "a/x", 0, false), DENY);
    assert_eq!(subscribe(&none, &device, "#", 0), DENY);
    assert_eq!(Acl::empty().len(), 0);
}

#[test]
fn r2_rule_10_rules_match_names_addresses_retain_and_wildcards() {
    let rules = acl(r##"
[[rule]]
permission = "allow"
username = "svc:platform"
address = ["10.0.0.0/8", "fd00::/8"]
action = "all"
topics = ["#"]

[[rule]]
permission = "allow"
username = { regex = "dev-[0-9]+" }
action = "publish"
retain = false
topics = ["telemetry/+/value", "$SYS/heartbeat"]
"##);
    let platform = from(client("svc:platform"), "10.4.5.6:1000");
    assert_eq!(
        publish(&rules, &platform, "ingest/x/commands/y", 1, true),
        ALLOW
    );
    let outside = from(client("svc:platform"), "203.0.113.9:1000");
    assert_eq!(
        publish(&rules, &outside, "ingest/x/commands/y", 1, true),
        DENY
    );
    let device = client("dev-17");
    assert_eq!(
        publish(&rules, &device, "telemetry/t/value", 1, false),
        ALLOW
    );
    assert_eq!(publish(&rules, &device, "telemetry/t/value", 1, true), DENY);
    assert_eq!(
        publish(&rules, &device, "telemetry/t/other", 1, false),
        DENY
    );
    // The regular expression matches the whole name.
    assert_eq!(
        publish(&rules, &client("dev-17x"), "telemetry/t/value", 1, false),
        DENY
    );
    // `#` never matches a `$`-topic, and a rule that names one does.
    assert_eq!(publish(&rules, &platform, "$SYS/heartbeat", 1, false), DENY);
    assert_eq!(publish(&rules, &device, "$SYS/heartbeat", 0, false), ALLOW);
}

#[test]
fn r2_rule_11_a_subscription_is_checked_as_a_topic_name() {
    let rules = acl(r##"
[[rule]]
permission = "allow"
action = "subscribe"
topics = ["ingest/acme/+/+/+"]
"##);
    let watcher = client("watcher");
    assert_eq!(subscribe(&rules, &watcher, "ingest/acme/+/+/+", 1), ALLOW);
    assert_eq!(subscribe(&rules, &watcher, "ingest/acme/a/b/c", 1), ALLOW);
    for broader in [
        "ingest/acme/#",
        "ingest/+/+/+/+",
        "ingest/acme/+/+/#",
        "ingest/acme/+/+",
        "#",
    ] {
        assert_eq!(subscribe(&rules, &watcher, broader, 1), DENY, "{broader}");
    }
    // A deny's `+` still refuses a `#` beneath it, as in 1.x: the subscription receives what the
    // deny names, so no later allow lets it through.
    let narrowed = acl(r##"
[[rule]]
permission = "deny"
action = "subscribe"
topics = ["ingest/acme/+"]

[[rule]]
permission = "allow"
action = "subscribe"
topics = ["ingest/#"]
"##);
    assert_eq!(subscribe(&narrowed, &watcher, "ingest/acme/#", 1), DENY);
    assert_eq!(subscribe(&narrowed, &watcher, "ingest/acme/a/b", 1), ALLOW);
    // A shared subscription is checked on its filter; the group grants nothing.
    assert_eq!(
        subscribe(&rules, &watcher, "$share/g/ingest/acme/a/b/c", 1),
        ALLOW
    );
    assert_eq!(
        subscribe(&rules, &watcher, "$share/g/ingest/acme/#", 1),
        DENY
    );
}

#[test]
fn r2_rule_12_a_refusal_is_a_deny_the_session_can_report() {
    // The ACL answers Deny; the session sends 0x87 for it (R2 rule 12). Here: a refusal is
    // never silent, it is a decision of its own.
    let rules = acl(r##"
[[rule]]
permission = "deny"
action = "publish"
topics = ["x"]
"##);
    assert_eq!(publish(&rules, &client("d"), "x", 1, false), DENY);
}

/// The device pattern of R2: services by a reserved prefix, devices by their CN under the
/// mountpoint `ingest/${username}/`, whose rules see their own relative topics (rule 7).
const DEVICES: &str = r##"
# Services, named by the reserved prefix the user list keeps for them (R2 rule 15), and only
# from inside the cluster: the address narrows the name and grants nothing alone (rule 14).
[[rule]]
permission = "allow"
username = { prefix = "svc:" }
address = "10.0.0.0/8"
action = "all"
topics = ["#"]

# A device never sends commands, retained or not (rule 13).
[[rule]]
permission = "deny"
action = "publish"
topics = ["commands/#"]

# A device never sets RETAIN (rule 16).
[[rule]]
permission = "deny"
action = "publish"
retain = true
topics = [{ all = true }]

[[rule]]
permission = "allow"
action = "publish"
topics = ["telemetry/#", "events/#"]

[[rule]]
permission = "allow"
action = "subscribe"
topics = ["commands/#"]
"##;

#[test]
fn r2_rule_13_a_device_receives_commands_and_never_sends_them() {
    let rules = acl(DEVICES);
    let device = client("acme/production/pump-3");
    assert_eq!(subscribe(&rules, &device, "commands/#", 1), ALLOW);
    assert_eq!(receive(&rules, &device, "commands/firmware"), ALLOW);
    for retain in [false, true] {
        for qos in 0..=2 {
            assert_eq!(
                publish(&rules, &device, "commands/firmware", qos, retain),
                DENY,
                "qos {qos} retain {retain}"
            );
        }
    }
    assert_eq!(
        publish(&rules, &device, "telemetry/temperature", 1, false),
        ALLOW
    );
}

#[test]
fn r2_rule_14_the_client_address_grants_nothing_by_itself() {
    let rules = acl(DEVICES);
    // A device from loopback or from inside the cluster gets what it gets from anywhere.
    for address in ["127.0.0.1:1", "[::1]:1", "10.1.2.3:1", "203.0.113.5:1"] {
        let device = from(client("pump-3"), address);
        assert_eq!(
            publish(&rules, &device, "commands/x", 1, false),
            DENY,
            "{address}"
        );
        assert_eq!(subscribe(&rules, &device, "#", 0), DENY, "{address}");
        assert_eq!(publish(&rules, &device, "telemetry/t", 1, false), ALLOW);
    }
    // The format refuses an allow rule that names an address and no one.
    let error = refused(
        r##"
[[rule]]
permission = "allow"
address = "127.0.0.1"
action = "all"
topics = ["#"]
"##,
    );
    assert!(
        error.contains("rule 1 (line 2): an allow rule cannot match on the client's address alone"),
        "{error}"
    );
    // A deny may name an address alone: it takes rights away, it grants none.
    acl(r##"
[[rule]]
permission = "deny"
address = "203.0.113.0/24"
action = "all"
topics = [{ all = true }]
"##);
}

#[test]
fn r2_rule_15_service_rights_cannot_be_claimed_by_naming() {
    let rules = acl(DEVICES);
    let service = from(client("svc:platform"), "10.0.0.7:1");
    assert_eq!(
        publish(&rules, &service, "ingest/pump-3/commands/firmware", 1, true),
        ALLOW
    );
    // Names that look alike are not the prefix.
    for name in ["SVC:platform", "svc-platform", "platform", "x-svc:platform"] {
        let lookalike = from(client(name), "10.0.0.7:1");
        assert_eq!(
            publish(
                &rules,
                &lookalike,
                "ingest/pump-3/commands/firmware",
                1,
                false
            ),
            DENY,
            "{name}"
        );
    }
    // The client identifier is the client's own choice and claims nothing.
    let chosen = from(named(Some("pump-3"), "svc:platform"), "10.0.0.7:1");
    assert_eq!(publish(&rules, &chosen, "ingest/x", 1, false), DENY);
    // That nobody else holds the prefix is the authenticators' part: see the password list,
    // the certificate identity and Anonymous.
}

#[test]
fn r2_rule_16_a_device_cannot_set_retain() {
    let rules = acl(DEVICES);
    let device = client("pump-3");
    assert_eq!(publish(&rules, &device, "telemetry/t", 1, true), DENY);
    assert_eq!(publish(&rules, &device, "telemetry/t", 1, false), ALLOW);
    assert_eq!(publish(&rules, &device, "events/boot", 0, true), DENY);
}

// --- Constructs ---------------------------------------------------------------------------

#[test]
fn a_user_name_matches_exactly_by_prefix_or_by_pattern() {
    let rules = acl(r##"
[[rule]]
permission = "allow"
username = ["alice", { prefix = "team-" }, { regex = "bot[0-9]{2}" }]
action = "publish"
topics = ["x"]
"##);
    for (name, allowed) in [
        ("alice", ALLOW),
        ("Alice", DENY),
        ("alice2", DENY),
        ("team-a", ALLOW),
        ("team", DENY),
        ("bot42", ALLOW),
        ("bot420", DENY),
        ("xbot42", DENY),
    ] {
        assert_eq!(
            publish(&rules, &client(name), "x", 0, false),
            allowed,
            "{name}"
        );
    }
    // A client without a user name matches no rule that names one.
    assert_eq!(publish(&rules, &named(None, "alice"), "x", 0, false), DENY);
}

#[test]
fn a_client_identifier_matches_like_a_user_name() {
    let rules = acl(r##"
[[rule]]
permission = "allow"
client_id = { regex = "sensor-[a-f0-9]+" }
action = "publish"
topics = ["x"]
"##);
    assert_eq!(
        publish(&rules, &named(Some("u"), "sensor-0af"), "x", 0, false),
        ALLOW
    );
    assert_eq!(
        publish(&rules, &named(Some("u"), "sensor-0ag"), "x", 0, false),
        DENY
    );
}

#[test]
fn an_address_matches_by_network_in_both_families() {
    let rules = acl(r##"
[[rule]]
permission = "allow"
username = "u"
address = ["10.1.0.0/16", "2001:db8::/32", "192.0.2.7"]
action = "publish"
topics = ["x"]
"##);
    for (address, allowed) in [
        ("10.1.200.3:1", ALLOW),
        ("10.2.0.1:1", DENY),
        ("[2001:db8:1::5]:1", ALLOW),
        ("[2001:db9::5]:1", DENY),
        ("192.0.2.7:1", ALLOW),
        ("192.0.2.8:1", DENY),
        // An IPv4 client on an IPv6 socket.
        ("[::ffff:10.1.0.9]:1", ALLOW),
    ] {
        assert_eq!(
            publish(&rules, &from(client("u"), address), "x", 0, false),
            allowed,
            "{address}"
        );
    }
    let nowhere = ClientInfo::new(
        ClientId::new("u").unwrap(),
        Principal::new(Some(Username::new("u").unwrap())),
    );
    assert_eq!(publish(&rules, &nowhere, "x", 0, false), DENY);
}

#[test]
fn attributes_match_the_principal() {
    let rules = acl(r##"
[[rule]]
permission = "allow"
attributes = { org = "acme", tier = { regex = "gold|silver" } }
action = "publish"
topics = ["x"]
"##);
    let with = |org: &str, tier: Option<&str>| {
        let mut principal =
            Principal::new(Some(Username::new("u").unwrap())).with_attribute("org", org);
        if let Some(tier) = tier {
            principal = principal.with_attribute("tier", tier);
        }
        ClientInfo::new(ClientId::new("u").unwrap(), principal)
    };
    assert_eq!(
        publish(&rules, &with("acme", Some("gold")), "x", 0, false),
        ALLOW
    );
    assert_eq!(
        publish(&rules, &with("acme", Some("bronze")), "x", 0, false),
        DENY
    );
    assert_eq!(publish(&rules, &with("acme", None), "x", 0, false), DENY);
    assert_eq!(
        publish(&rules, &with("globex", Some("gold")), "x", 0, false),
        DENY
    );
}

#[test]
fn qos_and_retain_narrow_a_publish() {
    let rules = acl(r##"
[[rule]]
permission = "allow"
action = "publish"
qos = [0, 1]
retain = true
topics = ["x"]

[[rule]]
permission = "allow"
action = "subscribe"
qos = 2
topics = ["x"]
"##);
    let c = client("c");
    assert_eq!(publish(&rules, &c, "x", 1, true), ALLOW);
    assert_eq!(publish(&rules, &c, "x", 2, true), DENY);
    assert_eq!(publish(&rules, &c, "x", 1, false), DENY);
    assert_eq!(subscribe(&rules, &c, "x", 2), ALLOW);
    assert_eq!(subscribe(&rules, &c, "x", 1), DENY);
    let error = refused(
        r##"
[[rule]]
permission = "allow"
action = "subscribe"
retain = true
topics = ["x"]
"##,
    );
    assert!(error.contains("`retain` applies to publishes"), "{error}");
}

#[test]
fn all_covers_publish_and_subscribe_and_retain_only_the_publish() {
    let rules = acl(r##"
[[rule]]
permission = "allow"
action = "all"
retain = false
topics = ["x/#"]
"##);
    let c = client("c");
    assert_eq!(publish(&rules, &c, "x/1", 1, false), ALLOW);
    assert_eq!(publish(&rules, &c, "x/1", 1, true), DENY);
    assert_eq!(subscribe(&rules, &c, "x/#", 1), ALLOW);
}

#[test]
fn placeholders_take_the_clients_own_values() {
    let rules = acl(r##"
[[rule]]
permission = "allow"
action = "all"
topics = ["users/${username}/#", "${clientid}/status"]
"##);
    let cn = named(Some("acme/pump-3"), "c-17");
    assert_eq!(publish(&rules, &cn, "users/acme/pump-3/t", 1, false), ALLOW);
    assert_eq!(publish(&rules, &cn, "users/acme/pump-4/t", 1, false), DENY);
    assert_eq!(subscribe(&rules, &cn, "users/acme/pump-3/#", 1), ALLOW);
    assert_eq!(publish(&rules, &cn, "c-17/status", 1, false), ALLOW);
    assert_eq!(publish(&rules, &cn, "c-18/status", 1, false), DENY);
    // A value that would widen the rule matches nothing.
    assert_eq!(
        publish(&rules, &named(Some("+"), "x"), "users/a/t", 0, false),
        DENY
    );
    assert_eq!(
        subscribe(&rules, &named(Some("#"), "x"), "users/#", 0),
        DENY
    );
    assert_eq!(
        publish(&rules, &named(Some("u"), "$SYS"), "$SYS/status", 0, false),
        DENY
    );
    // A client without a user name has no `${username}`.
    assert_eq!(
        publish(&rules, &named(None, "x"), "users/x/t", 0, false),
        DENY
    );
    let error = refused(
        r##"
[[rule]]
permission = "allow"
action = "all"
topics = ["${cert_common_name}/#"]
"##,
    );
    assert!(
        error.contains("a topic may use ${username} and ${clientid}"),
        "{error}"
    );
}

#[test]
fn eq_compares_text_and_all_takes_dollar_topics_too() {
    let rules = acl(r##"
[[rule]]
permission = "deny"
action = "subscribe"
topics = [{ eq = "#" }, { eq = "+/#" }]

[[rule]]
permission = "allow"
action = "subscribe"
topics = ["#"]

[[rule]]
permission = "allow"
username = "admin"
action = "publish"
topics = [{ all = true }]
"##);
    let c = client("c");
    assert_eq!(subscribe(&rules, &c, "#", 0), DENY);
    assert_eq!(subscribe(&rules, &c, "+/#", 0), DENY);
    assert_eq!(subscribe(&rules, &c, "a/#", 0), ALLOW);
    let admin = client("admin");
    assert_eq!(publish(&rules, &admin, "$SYS/x", 0, false), ALLOW);
    assert_eq!(publish(&rules, &admin, "any/thing", 0, false), ALLOW);
}

#[test]
fn a_delivery_is_allowed_when_its_topic_could_be_subscribed() {
    let rules = acl(r##"
[[rule]]
permission = "deny"
action = "subscribe"
topics = ["a/secret"]

[[rule]]
permission = "allow"
action = "subscribe"
topics = ["a/#"]
"##);
    let c = client("c");
    // The subscription is allowed; the one topic under it the rules refuse is not delivered.
    assert_eq!(subscribe(&rules, &c, "a/#", 1), ALLOW);
    assert_eq!(receive(&rules, &c, "a/open"), ALLOW);
    assert_eq!(receive(&rules, &c, "a/secret"), DENY);
}

#[test]
fn long_first_levels_are_told_apart() {
    let long = "x".repeat(300);
    let rules = acl(&format!(
        r##"
[[rule]]
permission = "allow"
action = "all"
topics = ["abcdefgh1/x", "{long}a/x", {{ eq = "abcdefgh3/#" }}]
"##
    ));
    let c = client("c");
    assert_eq!(publish(&rules, &c, "abcdefgh1/x", 0, false), ALLOW);
    assert_eq!(publish(&rules, &c, "abcdefgh2/x", 0, false), DENY);
    assert_eq!(publish(&rules, &c, &format!("{long}a/x"), 0, false), ALLOW);
    assert_eq!(publish(&rules, &c, &format!("{long}b/x"), 0, false), DENY);
    assert_eq!(subscribe(&rules, &c, "abcdefgh3/#", 0), ALLOW);
    assert_eq!(subscribe(&rules, &c, "abcdefgh4/#", 0), DENY);
}

#[test]
fn problems_are_all_reported_with_their_rule_and_line() {
    let error = refused(
        r##"version = 1

[[rule]]
permission = "allow"
username = { regex = "(unclosed" }
action = "publish"
topics = ["a/#/b", { eq = "" }]

[[rule]]
permission = "deny"
username = []
action = "all"
qos = []
topics = ["$share/g/t"]
"##,
    );
    let lines: Vec<&str> = error.lines().collect();
    assert_eq!(lines.len(), 6, "{error}");
    assert!(
        lines[0].starts_with(
            "rule 1 (line 3): `username` has a regular expression that does not compile"
        ),
        "{error}"
    );
    assert_eq!(
        lines[1],
        "rule 1 (line 3): the topic \"a/#/b\" is not a topic filter"
    );
    assert_eq!(
        lines[2],
        "rule 1 (line 3): `{ eq = \"\" }` is not a topic or a filter"
    );
    assert_eq!(
        lines[3],
        "rule 2 (line 9): `qos` lists no level, so the rule matches nothing"
    );
    assert_eq!(
        lines[4],
        "rule 2 (line 9): `username` lists nothing, so the rule matches nobody"
    );
    assert!(
        lines[5].starts_with("rule 2 (line 9): the topic \"$share/g/t\" is a shared subscription"),
        "{error}"
    );
}

#[test]
fn the_authorizer_takes_new_rules_while_clients_are_bound() {
    let authorizer = Arc::new(AclAuthorizer::new(
        Acl::from_toml(
            r##"
[[rule]]
permission = "allow"
action = "publish"
topics = ["old"]
"##,
        )
        .unwrap(),
    ));
    fn publish_to(topic: &TopicName) -> Action<'_> {
        Action::publish(topic, QoS::AtLeastOnce, false)
    }
    let bound = Arc::clone(&authorizer).bind(client("c"));
    let old = TopicName::new("old").unwrap();
    let new = TopicName::new("new").unwrap();
    assert_eq!(bound.authorize(&publish_to(&old)), ALLOW);
    assert_eq!(bound.authorize(&publish_to(&new)), DENY);
    authorizer.replace(
        Acl::from_toml(
            r##"
[[rule]]
permission = "allow"
action = "publish"
topics = ["new"]
"##,
        )
        .unwrap(),
    );
    assert_eq!(bound.authorize(&publish_to(&old)), DENY);
    assert_eq!(bound.authorize(&publish_to(&new)), ALLOW);
    assert_eq!(authorizer.authorize(&client("c"), &publish_to(&new)), ALLOW);
    assert_eq!(authorizer.acl().len(), 1);
}
