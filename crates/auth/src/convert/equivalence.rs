//! A converted file decides as the 1.x file did (R2 rule 30): random 1.x rules, written out as
//! Erlang terms, read back and converted, against a reference that reads the terms as 1.x does.
//!
//! The reference follows the meaning EMQX gives each construct ([emqx_authz_rule.erl
//! L290-L437][rule], [emqx_topic.erl L79-L116][topic]), restated here rather than ported: first
//! match wins, a user name pattern is searched for, not matched whole, an `eq` topic compares
//! text, and a topic is matched word by word, where a subscription's own `+` and `#` are words
//! too. It can apply the refinements report R2 and `docs/spec/acl.md` make on purpose, and the
//! converted file must then decide every case as it does:
//!
//! - an allow rule's `+` does not allow a subscription's `#` (R2 rule 11); a deny rule's still
//!   refuses it;
//! - a placeholder whose value is empty, holds `+`, `#` or U+0000, or would begin the filter
//!   with `$`, matches nothing.
//!
//! [rule]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx_auth/src/emqx_authz/emqx_authz_rule.erl#L290-L437
//! [topic]: https://github.com/openqtt/OpenQTT/blob/emqx-v5.8.9/apps/emqx/src/emqx_topic.erl#L79-L116

use std::net::IpAddr;
use std::sync::Arc;

use openqtt_core::{ClientId, QoS, TopicFilter, TopicName, Username};
use openqtt_ext::{Action, ClientInfo, Permission, Principal};
use proptest::prelude::*;
use regex::Regex;

use super::convert_acl;
use super::erlang::{self, Term};
use crate::acl::Acl;
use crate::acl::contract::Contract;
use crate::acl::spec::Cidr;

fn atom(name: &str) -> Term {
    Term::Atom(name.to_owned())
}

fn string(text: &str) -> Term {
    Term::Str(text.to_owned())
}

fn tuple(items: Vec<Term>) -> Term {
    Term::Tuple(items)
}

/// A request, as the reference reads it.
struct Asked {
    publish: bool,
    qos: u8,
    retain: bool,
    /// The topic name, or the subscription's filter.
    topic: String,
}

/// The decision 1.x makes, with no `authorization.no_match` to fall back on: no match denies.
fn decide_1x(rules: &[Term], client: &ClientInfo, asked: &Asked, refined: bool) -> Permission {
    for rule in rules {
        let Term::Tuple(items) = rule else {
            continue;
        };
        let decided = match items.as_slice() {
            [permission, Term::Atom(all)] if all == "all" => Some(permission),
            [permission, who, action, topics] => {
                let allow = matches!(permission, Term::Atom(word) if word == "allow");
                (action_1x(action, asked)
                    && who_1x(who, client)
                    && topics_1x(topics, client, asked, refined, allow))
                .then_some(permission)
            }
            _ => None,
        };
        if let Some(permission) = decided {
            return match permission {
                Term::Atom(word) if word == "allow" => Permission::Allow,
                _ => Permission::Deny,
            };
        }
    }
    Permission::Deny
}

fn action_1x(action: &Term, asked: &Asked) -> bool {
    let (kind, options) = match action {
        Term::Atom(kind) => (kind.as_str(), &[][..]),
        Term::Tuple(items) => match items.as_slice() {
            [Term::Atom(kind), Term::List(options)] => (kind.as_str(), options.as_slice()),
            _ => return false,
        },
        _ => return false,
    };
    let kind_ok = match kind {
        "publish" => asked.publish,
        "subscribe" => !asked.publish,
        _ => true,
    };
    let mut levels: Vec<u8> = Vec::new();
    let mut retain = None;
    for option in options {
        if let Term::Tuple(pair) = option {
            match pair.as_slice() {
                [Term::Atom(key), value] if key == "qos" => match value {
                    Term::Int(level) => levels.push(u8::try_from(*level).unwrap()),
                    Term::List(values) => levels.extend(values.iter().map(|value| match value {
                        Term::Int(level) => u8::try_from(*level).unwrap(),
                        _ => 9,
                    })),
                    _ => {}
                },
                [Term::Atom(key), Term::Atom(value)] if key == "retain" => {
                    retain.get_or_insert(value.clone());
                }
                _ => {}
            }
        }
    }
    let qos_ok = options.iter().all(|option| {
        !matches!(option, Term::Tuple(pair) if matches!(pair.first(), Some(Term::Atom(key)) if key == "qos"))
    }) || levels.contains(&asked.qos);
    // RETAIN is a condition on publishes only.
    let retain_ok = !asked.publish
        || match retain.as_deref() {
            Some("true") => asked.retain,
            Some("false") => !asked.retain,
            _ => true,
        };
    kind_ok && qos_ok && retain_ok
}

fn who_1x(who: &Term, client: &ClientInfo) -> bool {
    match who {
        Term::Atom(all) => all == "all",
        Term::Tuple(items) => match items.as_slice() {
            [Term::Atom(kind), value] if kind == "username" || kind == "user" => client
                .username()
                .is_some_and(|name| name_1x(value, name.as_str())),
            [Term::Atom(kind), value] if kind == "clientid" || kind == "client" => {
                name_1x(value, client.client_id.as_str())
            }
            [Term::Atom(kind), value] if kind == "ipaddr" => {
                address_1x(std::slice::from_ref(value), client)
            }
            [Term::Atom(kind), Term::List(values)] if kind == "ipaddrs" => {
                address_1x(values, client)
            }
            [Term::Atom(kind), Term::Str(name), value] if kind == "client_attr" => client
                .principal
                .attributes
                .get(name)
                .is_some_and(|attribute| name_1x(value, attribute)),
            [Term::Atom(kind), Term::List(conditions)] if kind == "and" => {
                conditions.iter().all(|condition| who_1x(condition, client))
            }
            [Term::Atom(kind), Term::List(conditions)] if kind == "or" => {
                conditions.iter().any(|condition| who_1x(condition, client))
            }
            _ => false,
        },
        _ => false,
    }
}

/// A name: equal text, or a pattern searched for anywhere in it, as `re:run` does.
fn name_1x(condition: &Term, name: &str) -> bool {
    match condition {
        Term::Str(exact) => name == exact,
        Term::Tuple(items) => match items.as_slice() {
            [Term::Atom(re), Term::Str(pattern)] if re == "re" => {
                Regex::new(pattern).unwrap().is_match(name)
            }
            _ => false,
        },
        _ => false,
    }
}

fn address_1x(networks: &[Term], client: &ClientInfo) -> bool {
    let Some(address) = client.address else {
        return false;
    };
    let ip = match address.ip() {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        v4 => v4,
    };
    networks.iter().any(|network| match network {
        // Bits past the prefix are cleared, as 1.x reads a network.
        Term::Str(text) => match Cidr::parse(text) {
            Ok(network) | Err(crate::acl::CidrError::HostBits(network)) => network.contains(ip),
            Err(_) => false,
        },
        _ => false,
    })
}

fn topics_1x(
    topics: &Term,
    client: &ClientInfo,
    asked: &Asked,
    refined: bool,
    allow: bool,
) -> bool {
    let entries = match topics {
        Term::Atom(all) => return all == "all",
        Term::List(entries) => entries,
        _ => return false,
    };
    entries.iter().any(|entry| match entry {
        Term::Tuple(pair) => match pair.as_slice() {
            [Term::Atom(eq), Term::Str(text)] if eq == "eq" => asked.topic == *text,
            _ => false,
        },
        Term::Str(text) => match text.strip_prefix("eq ") {
            Some(exact) => asked.topic == exact,
            None => render_1x(text, client, refined)
                .is_some_and(|filter| words_match(&asked.topic, &filter, refined && allow)),
        },
        _ => false,
    })
}

/// The filter with the client's values put in, `None` when a value is missing, or, refined,
/// unusable.
fn render_1x(template: &str, client: &ClientInfo, refined: bool) -> Option<String> {
    let mut out = String::new();
    let mut rest = template;
    loop {
        let Some(open) = rest.find("${") else {
            out.push_str(rest);
            return Some(out);
        };
        out.push_str(&rest[..open]);
        let inside = &rest[open + 2..];
        let close = inside.find('}')?;
        let value = match &inside[..close] {
            "username" => client.username()?.as_str().to_owned(),
            "clientid" => client.client_id.as_str().to_owned(),
            _ => return None,
        };
        let unusable = value.is_empty()
            || value.contains(['+', '#', '\0'])
            || (out.is_empty() && value.starts_with('$'));
        if refined && unusable {
            return None;
        }
        out.push_str(&value);
        rest = &inside[close + 1..];
    }
}

/// A topic or a subscription, word by word, against a filter: the filter's `+` takes one word
/// and its `#` the rest, and a topic beginning with `$` is never matched by a filter beginning
/// with a wildcard. Refined, for an allow rule, the filter's `+` does not take a `#`.
fn words_match(topic: &str, filter: &str, refined: bool) -> bool {
    let topic: Vec<&str> = topic.split('/').collect();
    let filter: Vec<&str> = filter.split('/').collect();
    if topic[0].starts_with('$') && (filter[0] == "+" || filter[0] == "#") {
        return false;
    }
    let mut i = 0;
    loop {
        match (topic.get(i), filter.get(i)) {
            (None, None) => return true,
            (_, Some(&"#")) if filter.len() == i + 1 => return true,
            (Some(word), Some(&"+")) => {
                if refined && *word == "#" {
                    return false;
                }
            }
            (Some(word), Some(level)) if word == level => {}
            _ => return false,
        }
        i += 1;
    }
}

// --- What is drawn ------------------------------------------------------------------------

const NAMES: [&str; 7] = ["u1", "svc:x", "dev-1", "a/b", "$u", "+", "#"];
const PATTERNS: [&str; 6] = ["^u1$", "u", "^svc:", "dev-[0-9]+$", "a|b", "^dev-1$"];
const NETWORKS: [&str; 4] = ["10.0.0.0/8", "10.1.2.3/16", "192.0.2.7", "fd00::/8"];
const FILTERS: [&str; 12] = [
    "a/#",
    "a/+",
    "#",
    "+/t",
    "${username}/t",
    "${clientid}/#",
    "$SYS/#",
    "a/b",
    "+",
    "u1/+",
    "x${username}/#",
    "a/${clientid}",
];
const TOPICS: [&str; 10] = [
    "a", "a/b", "a/t", "u1/t", "dev-1/x", "$SYS/x", "b/t", "a/b/c", "xu1/t", "a/u1",
];
const SUBSCRIPTIONS: [&str; 10] = [
    "a/#",
    "a/+",
    "#",
    "+/t",
    "u1/#",
    "$SYS/#",
    "a/b",
    "+",
    "a/+/c",
    "$share/g/a/#",
];

fn name_condition() -> impl Strategy<Value = Term> {
    prop_oneof![
        prop::sample::select(&NAMES[..]).prop_map(string),
        prop::sample::select(&PATTERNS[..])
            .prop_map(|pattern| tuple(vec![atom("re"), string(pattern)])),
    ]
}

fn simple_who() -> impl Strategy<Value = Term> {
    prop_oneof![
        name_condition().prop_map(|name| tuple(vec![atom("username"), name])),
        name_condition().prop_map(|name| tuple(vec![atom("clientid"), name])),
        prop::sample::select(&["acme", "globex"][..]).prop_map(|org| tuple(vec![
            atom("client_attr"),
            string("org"),
            string(org)
        ])),
    ]
}

/// Clients: an address never stands alone in an allow rule, which 2.0 refuses (R2 rule 14),
/// and an 'and' never names one kind of condition twice, which cannot be converted.
fn who(allow: bool) -> impl Strategy<Value = Term> {
    let address = prop::sample::select(&NETWORKS[..])
        .prop_map(|network| tuple(vec![atom("ipaddr"), string(network)]));
    let both = (
        name_condition().prop_map(|name| tuple(vec![atom("username"), name])),
        address.clone(),
    )
        .prop_map(|(name, address)| tuple(vec![atom("and"), Term::List(vec![name, address])]));
    let either = prop::collection::vec(simple_who(), 1..3)
        .prop_map(|conditions| tuple(vec![atom("or"), Term::List(conditions)]));
    if allow {
        prop_oneof![
            2 => Just(atom("all")),
            4 => simple_who(),
            1 => both,
            1 => either,
        ]
        .boxed()
    } else {
        prop_oneof![
            2 => Just(atom("all")),
            4 => simple_who(),
            1 => both,
            1 => either,
            1 => address,
            1 => prop::collection::vec(prop::sample::select(&NETWORKS[..]).prop_map(string), 0..3)
                .prop_map(|networks| tuple(vec![atom("ipaddrs"), Term::List(networks)])),
        ]
        .boxed()
    }
}

fn action() -> impl Strategy<Value = Term> {
    let kind = prop::sample::select(&["publish", "subscribe", "all"][..]);
    prop_oneof![
        kind.clone().prop_map(atom),
        (
            kind,
            prop::option::of(prop::collection::vec(0i64..3, 1..3)),
            prop::option::of(prop::sample::select(&["true", "false", "all"][..])),
        )
            .prop_map(|(kind, levels, retain)| {
                let mut options = Vec::new();
                if let Some(levels) = levels {
                    options.push(tuple(vec![
                        atom("qos"),
                        Term::List(levels.into_iter().map(Term::Int).collect()),
                    ]));
                }
                // 1.x pays `retain` no mind on a subscribe rule, and the converter says so.
                if let Some(retain) = retain.filter(|_| kind != "subscribe") {
                    options.push(tuple(vec![atom("retain"), atom(retain)]));
                }
                tuple(vec![atom(kind), Term::List(options)])
            }),
    ]
}

fn topics() -> impl Strategy<Value = Term> {
    prop_oneof![
        1 => Just(atom("all")),
        6 => prop::collection::vec(
            prop_oneof![
                4 => prop::sample::select(&FILTERS[..]).prop_map(string),
                1 => prop::sample::select(&FILTERS[..])
                    .prop_filter("no placeholder", |filter| !filter.contains("${"))
                    .prop_map(|filter| tuple(vec![atom("eq"), string(filter)])),
                1 => prop::sample::select(&FILTERS[..])
                    .prop_filter("no placeholder", |filter| !filter.contains("${"))
                    .prop_map(|filter| string(&format!("eq {filter}"))),
            ],
            1..3,
        )
        .prop_map(Term::List),
    ]
}

fn rule() -> impl Strategy<Value = Term> {
    prop::bool::ANY.prop_flat_map(|allow| {
        let permission = atom(if allow { "allow" } else { "deny" });
        prop_oneof![
            1 => Just(tuple(vec![permission.clone(), atom("all")])),
            8 => (who(allow), action(), topics()).prop_map(move |(who, action, topics)| {
                tuple(vec![permission.clone(), who, action, topics])
            }),
        ]
    })
}

fn client() -> impl Strategy<Value = ClientInfo> {
    (
        prop::option::weighted(0.85, prop::sample::select(&NAMES[..])),
        prop::sample::select(&NAMES[..]),
        prop::option::weighted(
            0.9,
            prop::sample::select(
                &[
                    "10.1.2.3",
                    "10.9.0.1",
                    "192.0.2.7",
                    "fd00::1",
                    "203.0.113.5",
                ][..],
            ),
        ),
        prop::option::of(prop::sample::select(&["acme", "globex"][..])),
    )
        .prop_map(|(username, client_id, address, org)| {
            let mut principal = Principal::new(username.map(|name| Username::new(name).unwrap()));
            if let Some(org) = org {
                principal = principal.with_attribute("org", org);
            }
            let mut client = ClientInfo::new(ClientId::new(client_id).unwrap(), principal);
            if let Some(address) = address {
                let ip: IpAddr = address.parse().unwrap();
                client = client.with_address(std::net::SocketAddr::new(ip, 1));
            }
            client
        })
}

fn source(rules: &[Term]) -> String {
    rules.iter().map(|rule| format!("{rule}.\n")).collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1_000))]

    #[test]
    fn a_converted_file_decides_as_the_1x_file(
        rules in prop::collection::vec(rule(), 0..8),
        clients in prop::collection::vec(client(), 1..4),
    ) {
        let text = source(&rules);
        // What is written reads back as it was.
        let read: Vec<Term> = erlang::parse(&text).unwrap().into_iter().map(|form| form.term).collect();
        prop_assert_eq!(&read, &rules);
        let converted = convert_acl(&text, &Contract::default()).unwrap();
        let acl = Arc::new(Acl::from_toml(&converted.text).unwrap());
        for client in &clients {
            let bound = acl.bind(client);
            for publish in [true, false] {
                let candidates = if publish { &TOPICS[..] } else { &SUBSCRIPTIONS[..] };
                for topic in candidates {
                    for (qos, retain) in [(0u8, false), (1, true), (2, false)] {
                        let level = QoS::from_u8(qos).unwrap();
                        let name;
                        let filter;
                        let (action, asked_topic) = if publish {
                            name = TopicName::new(topic).unwrap();
                            (Action::publish(&name, level, retain), topic.to_string())
                        } else {
                            filter = TopicFilter::new(topic).unwrap();
                            (Action::subscribe(&filter, level), filter.pattern().to_owned())
                        };
                        let asked = Asked { publish, qos, retain, topic: asked_topic };
                        let expected = decide_1x(&rules, client, &asked, true);
                        prop_assert_eq!(
                            bound.decide(&action),
                            expected,
                            "{:?} by {:?}\n{}\n{}",
                            action,
                            client,
                            text,
                            converted.text
                        );
                    }
                }
            }
        }
    }
}

/// The property means something only if the files drawn decide both ways often.
#[test]
fn the_files_drawn_decide_both_ways() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;

    let mut runner = TestRunner::deterministic();
    let case = (prop::collection::vec(rule(), 0..8), client());
    let (mut allowed, mut denied) = (0u32, 0u32);
    for _ in 0..500 {
        let (rules, client) = case.new_tree(&mut runner).unwrap().current();
        for (publish, topics) in [(true, &TOPICS[..]), (false, &SUBSCRIPTIONS[..])] {
            for topic in topics {
                let asked = Asked {
                    publish,
                    qos: 1,
                    retain: false,
                    topic: topic.trim_start_matches("$share/g/").to_owned(),
                };
                match decide_1x(&rules, &client, &asked, true) {
                    Permission::Allow => allowed += 1,
                    _ => denied += 1,
                }
            }
        }
    }
    let total = allowed + denied;
    assert!(
        allowed * 10 >= total && denied * 10 >= total,
        "allowed {allowed}, denied {denied}"
    );
}

#[test]
fn the_refinements_are_the_only_differences_and_they_occur() {
    // Each refinement changes a decision of 1.x, and only in the direction of refusing.
    let rules = erlang::parse(
        "{allow, all, subscribe, [\"a/+\", \"${username}/t\"]}.\n{allow, all, publish, [\"${clientid}/x\"]}.\n",
    )
    .unwrap()
    .into_iter()
    .map(|form| form.term)
    .collect::<Vec<_>>();
    let client = |name: &str| {
        ClientInfo::new(
            ClientId::new(name).unwrap(),
            Principal::new(Some(Username::new(name).unwrap())),
        )
    };
    let subscribe = |topic: &str| Asked {
        publish: false,
        qos: 0,
        retain: false,
        topic: topic.to_owned(),
    };
    // The subscription `a/#` under the rule `a/+` (R2 rule 11).
    assert_eq!(
        decide_1x(&rules, &client("u"), &subscribe("a/#"), false),
        Permission::Allow
    );
    assert_eq!(
        decide_1x(&rules, &client("u"), &subscribe("a/#"), true),
        Permission::Deny
    );
    // A user named `+` turning `${username}/t` into `+/t`.
    assert_eq!(
        decide_1x(&rules, &client("+"), &subscribe("z/t"), false),
        Permission::Allow
    );
    assert_eq!(
        decide_1x(&rules, &client("+"), &subscribe("z/t"), true),
        Permission::Deny
    );
    // A client identifier beginning with `$` reaching a `$`-topic.
    let publish = Asked {
        publish: true,
        qos: 0,
        retain: false,
        topic: "$SYS/x".to_owned(),
    };
    assert_eq!(
        decide_1x(&rules, &client("$SYS"), &publish, false),
        Permission::Allow
    );
    assert_eq!(
        decide_1x(&rules, &client("$SYS"), &publish, true),
        Permission::Deny
    );
}
