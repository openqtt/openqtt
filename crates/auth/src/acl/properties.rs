//! The compiled engine, unbound and bound, decides every case as the reference interpreter
//! does, over random rule sets, clients and actions drawn from small alphabets so that rules
//! match often.

use std::sync::Arc;

use openqtt_core::{ClientId, QoS, TopicFilter, TopicName, Username};
use openqtt_ext::{Action, ClientInfo, Principal};
use proptest::prelude::*;

use super::naive;
use super::spec::{ActionKind, Cidr, Decision, NameMatch, QosSet, RuleSpec, TopicSpec, Who};
use super::{Acl, render};

const NAMES: [&str; 9] = [
    "u1",
    "u2",
    "svc:x",
    "dev-1",
    "dev-22",
    "$u",
    "#",
    "a/b",
    "${clientid}",
];
const PATTERNS: [&str; 5] = ["dev-[0-9]+", "u.", "svc:.*", "a|b", "(?i)DEV-1"];
const PREFIXES: [&str; 4] = ["svc:", "dev-", "u", "$"];
const NETWORKS: [&str; 6] = [
    "10.0.0.0/8",
    "10.1.0.0/16",
    "192.0.2.7/32",
    "fd00::/8",
    "0.0.0.0/0",
    "::/0",
];
const ADDRESSES: [&str; 6] = [
    "10.1.2.3",
    "10.2.0.1",
    "192.0.2.7",
    "fd00::1",
    "::ffff:10.1.2.3",
    "203.0.113.5",
];
const LEVELS: [&str; 6] = ["a", "b", "t", "$SYS", "", "${username}"];
const NAMES_OF_TOPICS: [&str; 10] = [
    "a", "a/b", "a/b/c", "$SYS/x", "u1/t", "dev-1/t", "b/a", "a//b", "svc:x/t", "a/b/t",
];
const FILTERS: [&str; 12] = [
    "a/#",
    "a/+",
    "#",
    "+/t",
    "$SYS/#",
    "u1/#",
    "a/b",
    "$share/g/a/#",
    "+",
    "a/+/#",
    "+/+/t",
    "dev-1/#",
];

fn qos() -> impl Strategy<Value = QoS> {
    prop_oneof![
        Just(QoS::AtMostOnce),
        Just(QoS::AtLeastOnce),
        Just(QoS::ExactlyOnce)
    ]
}

fn matcher() -> impl Strategy<Value = NameMatch> {
    prop_oneof![
        prop::sample::select(&NAMES[..]).prop_map(|n| NameMatch::Exact(n.to_owned())),
        prop::sample::select(&PREFIXES[..]).prop_map(|p| NameMatch::Prefix(p.to_owned())),
        prop::sample::select(&PATTERNS[..]).prop_map(|p| NameMatch::Regex(p.to_owned())),
    ]
}

fn matchers() -> impl Strategy<Value = Option<Vec<NameMatch>>> {
    prop::option::weighted(0.25, prop::collection::vec(matcher(), 1..4))
}

fn who() -> impl Strategy<Value = Who> {
    (
        matchers(),
        matchers(),
        prop::option::weighted(
            0.2,
            prop::collection::vec(
                prop::sample::select(&NETWORKS[..]).prop_map(|n| Cidr::parse(n).unwrap()),
                1..3,
            ),
        ),
        prop::option::weighted(
            0.15,
            prop::collection::vec(
                prop::sample::select(&["acme", "globex"][..])
                    .prop_map(|v| NameMatch::Exact(v.to_owned())),
                1..2,
            ),
        ),
    )
        .prop_map(|(username, client_id, address, org)| Who {
            username,
            client_id,
            address,
            attributes: org.map(|org| ("org".to_owned(), org)).into_iter().collect(),
        })
}

/// A valid filter: literal levels and placeholders, `+` anywhere, `#` only at the end.
fn filter() -> impl Strategy<Value = String> {
    (
        prop::collection::vec(
            prop_oneof![
                4 => prop::sample::select(&LEVELS[..]).prop_map(str::to_owned),
                2 => Just("+".to_owned()),
                1 => Just("x${clientid}".to_owned()),
            ],
            1..4,
        ),
        any::<bool>(),
    )
        .prop_map(|(mut levels, hash)| {
            if hash {
                levels.push("#".to_owned());
            }
            levels.join("/")
        })
        .prop_filter("a filter is not empty", |filter| !filter.is_empty())
}

/// Filters likely to match the names and filters the requests use.
const COMMON: [&str; 12] = [
    "a/#",
    "a/+",
    "#",
    "+/t",
    "${username}/t",
    "${username}/#",
    "$SYS/#",
    "a/b",
    "+/+",
    "a/b/+",
    "+",
    "${clientid}/#",
];

fn topic() -> impl Strategy<Value = TopicSpec> {
    prop_oneof![
        3 => filter().prop_map(TopicSpec::Filter),
        3 => prop::sample::select(&COMMON[..]).prop_map(|f| TopicSpec::Filter(f.to_owned())),
        1 => prop::sample::select(&FILTERS[..])
            .prop_filter("not shared", |f| !f.starts_with("$share/"))
            .prop_map(|f| TopicSpec::Exact(f.to_owned())),
        1 => Just(TopicSpec::All),
    ]
}

fn rule() -> impl Strategy<Value = RuleSpec> {
    (
        prop_oneof![Just(Decision::Allow), Just(Decision::Deny)],
        who(),
        prop_oneof![
            Just(ActionKind::Publish),
            Just(ActionKind::Subscribe),
            Just(ActionKind::All)
        ],
        prop::option::weighted(0.3, prop::collection::vec(qos(), 1..3)),
        prop::option::of(any::<bool>()),
        prop::collection::vec(topic(), 1..4),
    )
        .prop_map(|(decision, mut who, action, levels, retain, topics)| {
            // What the engine refuses to load: an allow rule on an address alone (R2 rule 14),
            // and `retain` on a rule that cannot publish.
            if decision == Decision::Allow && who.address.is_some() && !who.names_identity() {
                who.username = Some(vec![NameMatch::Prefix("u".to_owned())]);
            }
            let qos = levels.map_or(QosSet::ALL, |levels| {
                levels.into_iter().fold(QosSet::EMPTY, QosSet::with)
            });
            RuleSpec {
                decision,
                who,
                action,
                qos,
                retain: retain.filter(|_| action.publishes()),
                topics,
            }
        })
}

fn client() -> impl Strategy<Value = ClientInfo> {
    (
        prop::option::weighted(0.85, prop::sample::select(&NAMES[..])),
        prop::sample::select(&NAMES[..]),
        prop::option::weighted(0.9, prop::sample::select(&ADDRESSES[..])),
        prop::option::of(prop::sample::select(&["acme", "globex", "initech"][..])),
    )
        .prop_map(|(username, client_id, address, org)| {
            let mut principal = Principal::new(username.map(|name| Username::new(name).unwrap()));
            if let Some(org) = org {
                principal = principal.with_attribute("org", org);
            }
            let mut client = ClientInfo::new(ClientId::new(client_id).unwrap(), principal);
            if let Some(address) = address {
                let ip: std::net::IpAddr = address.parse().unwrap();
                client = client.with_address(std::net::SocketAddr::new(ip, 4000));
            }
            client
        })
}

/// An action, by kind (0 publish, 1 subscribe, 2 receive), as indices into the tables above.
fn request() -> impl Strategy<Value = (u8, usize, QoS, bool)> {
    (
        0u8..3,
        0usize..FILTERS.len().max(NAMES_OF_TOPICS.len()),
        qos(),
        any::<bool>(),
    )
}

fn check(rules: &[RuleSpec], client: &ClientInfo, request: (u8, usize, QoS, bool)) {
    let (kind, index, qos, retain) = request;
    let name = TopicName::new(NAMES_OF_TOPICS[index % NAMES_OF_TOPICS.len()]).unwrap();
    let filter = TopicFilter::new(FILTERS[index % FILTERS.len()]).unwrap();
    let action = match kind {
        0 => Action::publish(&name, qos, retain),
        1 => Action::subscribe(&filter, qos),
        _ => Action::receive(&name, qos, retain),
    };
    let expected = naive::decide(rules, client, &action);
    let acl = Arc::new(Acl::compile(rules).unwrap());
    assert_eq!(acl.decide(client, &action), expected, "unbound: {action:?}");
    assert_eq!(
        acl.bind(client).decide(&action),
        expected,
        "bound: {action:?}"
    );
}

/// The property means something only if the rules it draws decide both ways often.
#[test]
fn the_cases_drawn_decide_both_ways() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    let mut runner = TestRunner::deterministic();
    let case = (prop::collection::vec(rule(), 0..10), client(), request());
    let (mut allowed, mut denied) = (0u32, 0u32);
    for _ in 0..2_000 {
        let (rules, client, (kind, index, qos, retain)) =
            case.new_tree(&mut runner).unwrap().current();
        let name = TopicName::new(NAMES_OF_TOPICS[index % NAMES_OF_TOPICS.len()]).unwrap();
        let filter = TopicFilter::new(FILTERS[index % FILTERS.len()]).unwrap();
        let action = match kind {
            0 => Action::publish(&name, qos, retain),
            1 => Action::subscribe(&filter, qos),
            _ => Action::receive(&name, qos, retain),
        };
        match naive::decide(&rules, &client, &action) {
            openqtt_ext::Permission::Allow => allowed += 1,
            _ => denied += 1,
        }
    }
    assert!(
        allowed >= 200 && denied >= 200,
        "allowed {allowed}, denied {denied}"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn the_engine_decides_as_the_reference_does(
        rules in prop::collection::vec(rule(), 0..10),
        client in client(),
        requests in prop::collection::vec(request(), 1..8),
    ) {
        for request in requests {
            check(&rules, &client, request);
        }
    }

    #[test]
    fn rules_read_back_from_the_toml_they_render_to(rules in prop::collection::vec(rule(), 0..6)) {
        let text = render::to_toml(&rules);
        prop_assert_eq!(super::parse_rules(&text).unwrap(), rules);
    }
}
